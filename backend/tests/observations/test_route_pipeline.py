"""私有 PG + 人工 MRT：文件重叠执行、固定快照、串行等价与实际强制中断。"""
import functools
import gzip
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import psycopg2
import pyarrow.parquet as pq
import pytest

from data_pipeline.bgp.replay.quality_overlay import CanonicalReplay
from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
from data_pipeline.bgp.archive.file_reader import CheckpointBinding, CheckpointReader, selected_prefix
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.ordered_reader import _adapt, binding_from_reader
from data_pipeline.bgp.replay.snapshot_contract import decode, digest
from data_pipeline.bgp.replay.archive_input import build_mapping
from data_pipeline.bgp.replay.file_pipeline import run_pipeline
from data_pipeline.bgp.state.checkpoint import StateStore
from data_pipeline.bgp.replay.route_replay import ReplayPlan
from tests.observations.test_observation_checkpoint import dsn, fixture, wait_marker
from tests.observations.test_observation_native import native
from tests.observations.test_observation_native_update import run_native, update_fixture
from tests.observations.test_observation_mrt import update, mrt


def replace_update(manifest,index,raw):
    from data_pipeline.bgp.input.mrt_reader import source_identity
    entry=manifest['inputs'][index];path=Path(entry['path'])
    path.write_bytes(gzip.compress(raw,mtime=0))
    entry.update(sha256=hashlib.sha256(path.read_bytes()).hexdigest(),size=path.stat().st_size)
    entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
    manifest['update_sources']=[e['source_id'] for e in manifest['inputs'] if e['role']=='update']


def read_file(receipt):
    return [(r['table'],decode(r['payload'])) for r in pq.read_table(receipt['path']).to_pylist()]


def assert_serial(dsn,root,receipt):
    """对照同一封存观察的原有完整端点映射及串行 CanonicalReplay。"""
    run_id=receipt['run_id'];current=selected_prefix(dsn,run_id);manifest=current['plan']['manifest']
    selected=(manifest['baseline_source'],*manifest['update_sources'])
    reader=ObservationReader(dsn,run_id,receipt['observation_snapshot'],selected,profile='observation')
    class Rows:
        def __init__(self):self.dsn=dsn;self.rows=[]
        def flush(self):pass
        def append(self,table,row):self.rows.append((table,row))
    rows=Rows();endpoints=build_mapping(rows,selected[0],selected[1:],reader=reader)
    frozen=binding_from_reader(reader)
    binding=CheckpointBinding(receipt['binding_id'],manifest['collector'],run_id,reader.snapshot,frozen.sources)
    canonical=CanonicalReplay(ReplayPlan(manifest['collector'],selected[0],selected[1:],endpoints),binding,selected)
    expected=list(rows.rows)
    for item in _adapt(reader.stream(),binding,selected):expected.extend(canonical.apply(item))
    expected.extend(canonical.export())
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT payload FROM route_file.files WHERE run_id=%s ORDER BY ordinal',(run_id,))
        files=[r[0] for r in c.fetchall()]
    actual=[]
    for f in files:actual.extend(read_file(f['file']))
    actual.extend(read_file(receipt['final_file']))
    def normalized(items):
        result=[]
        for table,row in items:
            if table=='source_coverage':
                row=dict(row);row['raw']={k:v for k,v in row['raw'].items() if k!='snapshot'}
            result.append((table,digest(row)))
        return sorted(result)
    assert normalized(actual)==normalized(expected)
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT binding FROM route_file.runs WHERE run_id=%s',(run_id,));config=c.fetchone()[0]
    restored=CanonicalReplay(canonical.replay.plan,binding,selected,compact=config["profile"]=="route-batch-candidate/v1")
    store=StateStore(dsn,run_id,root/'route-files',config)
    try:store.restore(restored)
    finally:store.close()
    for attr in ('current','last_known','legacy_by_prefix','legacy_paths','legacy_origins','seen_vps','cursor'):
        assert getattr(restored.replay,attr)==getattr(canonical.replay,attr),attr
    assert restored.positions==canonical.positions
    assert restored.index.gaps==canonical.index.gaps
    assert restored.counts==canonical.counts
    return actual


@pytest.mark.parametrize('bad',[False,True])
def test_native_endpoint_inventory_matches_actual_columns(tmp_path,native,bad):
    raw=update_fixture()
    if bad:raw+=update(attrs=b'\x40\x02\xff')+update()
    path,rows=run_native(tmp_path,native,raw,policy='isolate-payload/v1')
    inv=native.endpoints(path,'fixture',hashlib.sha256(path.read_bytes()).hexdigest(),tmp_path/'endpoints.json',policy='isolate-payload/v1')
    expected={}
    for row in rows['messages']:
        if row['peer_ip'] is None or row['local_message']:continue
        key=tuple(row[k] for k in ('peer_ip','peer_asn','local_ip','local_asn','interface'))
        old=expected.get(key);mid=row['message_id']
        expected[key]=(min(old[0],mid),old[1]+1) if old else (mid,1)
    assert inv['records']==len(rows['messages'])
    assert sorted(tuple(row) for row in inv['endpoints'])==sorted((*key,*value) for key,value in expected.items())


def await_baseline_consumer(label,ordinal,*,dsn,run_id,marker):
    if (label,ordinal)!=('file_mid',2):return
    # UPDATE 已产生真实落盘分段，但完整文件尚未取得 checkpoint。
    Path(marker+'.started').write_text('UPDATE 分段已经入库')
    deadline=time.monotonic()+45
    while time.monotonic()<deadline:
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:
            c.execute('SELECT cursor FROM route_file.runs WHERE run_id=%s',(run_id,));position=c.fetchone()[0]
            c.execute('SELECT count(*) FROM observation_m2.checkpoints WHERE run_id=%s',(run_id,));count=c.fetchone()[0]
        if position>=1:
            assert count==2
            Path(marker).write_text('基线消费已提交，UPDATE 文件仍在处理中')
            return
        time.sleep(.05)
    raise AssertionError('基线消费等待了整个 M2 封存')


def test_files_overlap_and_match_serial_with_late_ambiguous_endpoint(tmp_path,native,dsn):
    manifest=fixture(tmp_path/'input',bad=True)
    # 在最后一份 UPDATE 中出现另一个本地端点，必须从基线起就判为歧义。
    entry=manifest['inputs'][-1];path=Path(entry['path'])
    extra=bytearray(update());extra[28:32]=b'\xc0\0\x02\x02'
    replace_update(manifest,-1,gzip.decompress(path.read_bytes())+extra)
    root=tmp_path/'run';marker=tmp_path/'overlap'
    hook=functools.partial(await_baseline_consumer,dsn=dsn,run_id='overlap',marker=str(marker))
    def consumer_hook(label,ordinal):
        if (label,ordinal)!=('before_state_commit',1):return
        deadline=time.monotonic()+45
        while not Path(str(marker)+'.started').exists():
            if time.monotonic()>deadline:raise AssertionError('M2 未与基线路由事务重叠执行')
            time.sleep(.05)
    result=run_pipeline(manifest,dsn,root,native_build=native.build,run_id='overlap',policy='isolate-payload/v1',
                        batch_rows=3,min_free_bytes=0,producer_hook=hook,hook=consumer_hook)
    assert marker.exists() and result['qualification']=='route_candidate_complete'
    actual=assert_serial(dsn,root,result)
    assert any(t=='baseline_mappings' and r['status']=='ambiguous_local_endpoints' for t,r in actual)
    assert any(t=='scope_gap' for t,r in actual)
    assert run_pipeline(manifest,dsn,root,native_build=native.build,policy='isolate-payload/v1',batch_rows=3,min_free_bytes=0)==result


def test_checkpoint_gate_and_fixed_snapshot(tmp_path,dsn):
    manifest=fixture(tmp_path/'input');root=tmp_path/'m2';saved=[]
    def pause(label,ordinal,owner):
        if (label,ordinal)==('before_checkpoint',1):
            with pytest.raises(ValueError,match='尚未选择'):CheckpointReader(dsn,'gate',1,'0'*64)
        if (label,ordinal)==('after_checkpoint',1):
            cp=selected_prefix(dsn,'gate')['checkpoints'][1]
            saved.append(CheckpointReader(dsn,'gate',1,cp['digest']))
            with pytest.raises(ValueError,match='尚未封存'):
                ObservationReader(dsn,'gate',cp['snapshot'],[manifest['baseline_source']],profile='observation')
            raise RuntimeError('暂停')
    with pytest.raises(RuntimeError,match='暂停'):
        produce_checkpointed(manifest,dsn,root,run_id='gate',batch_rows=3,min_free_bytes=0,hook=pause)
    first=list(saved[0].stream())
    produce_checkpointed(manifest,dsn,root,run_id='gate',batch_rows=3,min_free_bytes=0)
    assert list(saved[0].stream())==first


@pytest.mark.parametrize('label',['files_ready','before_state_commit','after_state_commit'])
def test_actual_kill_resume_no_loss_or_replay(tmp_path,native,dsn,label):
    manifest=fixture(tmp_path/'input',bad=True);root=tmp_path/'run';marker=tmp_path/'marker'
    config=dict(manifest=manifest,dsn=dsn,output=str(root),native_build=str(native.build),batch_rows=3,
                min_free_bytes=0,policy='isolate-payload/v1',stop=label,ordinal=2,marker=str(marker))
    path=tmp_path/'config.json';path.write_text(json.dumps(config));log=tmp_path/'worker.log'
    with log.open('w') as out:
        process=subprocess.Popen([sys.executable,str(Path(__file__).with_name('route_worker.py')),str(path)],stdout=out,stderr=subprocess.STDOUT)
    try:
        wait_marker(process,marker)
        run_id=json.loads((root/'pipeline-run.json').read_text())['run_id']
        before=selected_prefix(dsn,run_id)
        os.kill(process.pid,signal.SIGKILL);assert process.wait(timeout=10)!=0
        # 子进程的父进程退出保护释放 M2 锁后重启同一身份。
        deadline=time.monotonic()+10
        while True:
            with psycopg2.connect(dsn) as pg,pg.cursor() as c:
                from data_pipeline.bgp.archive.checkpoint import Owner
                key=int(hashlib.sha256(run_id.encode()).hexdigest()[:15],16)
                c.execute('SELECT pg_try_advisory_lock(%s)',(key,));free=c.fetchone()[0]
                if free:c.execute('SELECT pg_advisory_unlock(%s)',(key,))
            if free:break
            if time.monotonic()>deadline:pytest.fail('父进程退出后 M2 所有权未释放')
            time.sleep(.05)
        for k in ('stop','ordinal','marker'):config.pop(k)
        result=run_pipeline(**config)
        after=selected_prefix(dsn,run_id)
        assert after['checkpoints'][:len(before['checkpoints'])]==before['checkpoints']
        assert_serial(dsn,root,result)
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:
            c.execute('SELECT count(*),count(DISTINCT ordinal) FROM route_file.files WHERE run_id=%s',(run_id,))
            assert c.fetchone()==(4,4)
        events=[json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()]
        # 提交后的确认丢失，恢复不得再次消费该文件。
        assert sum(e['label']=='consume_start' and e['ordinal']==2 for e in events)==(1 if label=='after_state_commit' else 2)
    except BaseException:
        print(log.read_text());raise
    finally:
        if process.poll() is None:process.kill();process.wait()


def test_empty_update_advances_complete_file_without_changing_routes(tmp_path,native,dsn):
    manifest=fixture(tmp_path/'input');replace_update(manifest,-1,b'')
    root=tmp_path/'run'
    result=run_pipeline(manifest,dsn,root,native_build=native.build,batch_rows=3,min_free_bytes=0,max_pending_files=1)
    actual=assert_serial(dsn,root,result)
    tail=[r for t,r in actual if t=='source_coverage' and r['source_id']==manifest['update_sources'][-1]]
    assert len(tail)==1 and tail[0]['raw']['messages']==0 and tail[0]['execution']=='complete'


@pytest.mark.parametrize('damage',['state','metadata','file','missing_row'])
def test_resume_rejects_persisted_corruption(tmp_path,native,dsn,damage):
    manifest=fixture(tmp_path/'input');root=tmp_path/'run'
    config=dict(manifest=manifest,dsn=dsn,output=root,native_build=native.build,batch_rows=3,min_free_bytes=0)
    result=run_pipeline(**config);run_id=result['run_id']
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        if damage=='state':
            c.execute("UPDATE route_file.rows SET digest=%s WHERE run_id=%s AND kind='route'",('0'*64,run_id))
        elif damage=='metadata':
            c.execute("UPDATE route_file.runs SET metadata='null' WHERE run_id=%s",(run_id,))
        elif damage=='missing_row':
            c.execute("DELETE FROM route_file.rows WHERE run_id=%s AND kind='route'",(run_id,))
        else:Path(result['final_file']['path']).write_bytes(b'broken')
    with pytest.raises(ValueError):run_pipeline(**config)


def test_actual_endpoint_audit_blocks_incorrect_prepass(tmp_path,native,dsn,monkeypatch):
    from data_pipeline.bgp.input.native_parser import NativeRuntime
    original=NativeRuntime.endpoints
    def wrong(self,*args,**kwargs):
        value=original(self,*args,**kwargs)
        value['endpoints'][0][-1]+=1
        return value
    monkeypatch.setattr(NativeRuntime,'endpoints',wrong)
    manifest=fixture(tmp_path/'input');root=tmp_path/'run'
    with pytest.raises(ValueError,match='端点与预扫'):
        run_pipeline(manifest,dsn,root,native_build=native.build,batch_rows=3,min_free_bytes=0)
    run_id=json.loads((root/'pipeline-run.json').read_text())['run_id']
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT cursor,receipt FROM route_file.runs WHERE run_id=%s',(run_id,))
        assert c.fetchone()==(1,None)


def test_second_consumer_cannot_take_ownership(tmp_path,dsn):
    owner=StateStore(dsn,'exclusive',tmp_path/'state',{'binding_id':'fixture'})
    try:
        with pytest.raises(ValueError,match='已有运行所有者'):
            StateStore(dsn,'exclusive',tmp_path/'state',{'binding_id':'fixture'})
    finally:owner.close()


def test_cli_spawn_native_contract_and_sealed_canonical(tmp_path,native,dsn):
    import struct
    from data_pipeline.bgp.ordered_reader import ordered, decode_interpretation
    from data_pipeline.bgp.replay.route_snapshot import produce_projection
    manifest=fixture(tmp_path/'input',bad=True)
    endpoint=struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\0\x02\x01'*2
    refresh=mrt(endpoint+b'\xff'*16+struct.pack('!HB',23,5)+b'\0\x01\0\x01',4)
    path=Path(manifest['inputs'][-1]['path'])
    replace_update(manifest,-1,gzip.decompress(path.read_bytes())+refresh)
    config=tmp_path/'config.json';config.write_text(json.dumps(dict(dsn=dsn,native_build=str(native.build),
        read_policy='isolate-payload/v1',limits=dict(batch_rows=3,min_free_bytes=0))))
    path=tmp_path/'manifest.json';path.write_text(json.dumps(manifest));root=tmp_path/'cli'
    script=Path(__file__).resolve().parents[3]/'scripts/observations.py'
    completed=subprocess.run([sys.executable,str(script),'route-file-pipeline','--config',str(config),
        '--manifest',str(path),'--output',str(root)],check=True,capture_output=True,text=True,timeout=60)
    result=json.loads(completed.stdout)
    assert_serial(dsn,root,result)
    reader=ObservationReader(dsn,result['run_id'],result['observation_snapshot'],
        [manifest['baseline_source'],*manifest['update_sources']],profile='observation')
    items=list(ordered(reader))
    gaps=[i for i in items if getattr(i,'gap',None)]
    assert any(i.gap.reason_code=='bgpdump_route_refresh_not_implemented' for i in gaps)
    for item in gaps:
        raw=item.raw['interpretation']
        with pytest.raises(ValueError,match='未知解释版本'):decode_interpretation(raw)
        value=json.loads(raw);value['reason_code']='fabricated_reason'
        with pytest.raises(ValueError):decode_interpretation(json.dumps(value),native_allowed=True)
    # 原有完整封存 canonical 入口也验证新诊断版本；不调用 publication。
    projection=produce_projection(reader,tmp_path/'sealed-projection',batch_rows=3,min_free_bytes=0)
    assert projection
