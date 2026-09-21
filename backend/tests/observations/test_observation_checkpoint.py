"""私有PG、人工多文件及实际OS终止恢复；不得绑定生产DSN。"""
import copy
from dataclasses import asdict
import gzip
import hashlib
import json
import os
from pathlib import Path
import signal
import struct
import subprocess
import sys
import time
import uuid

import psycopg2
from psycopg2 import sql
import pytest
from data_pipeline.bgp.archive.checkpoint import produce_checkpointed, Owner, sha, plan_for
from data_pipeline.bgp.archive.selection import Selection
from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch, SourceEnd
from data_pipeline.bgp.archive.store import scan, query, scan_observations
from tests.observations.test_observation_two_phase import fixture_manifest
from tests.observations.test_observation_mrt import update, mrt, attr


@pytest.fixture
def dsn():
    base=os.environ.get('DOMEYE_M2_TEST_DSN')
    if not base:pytest.skip('需显式绑定自有人工PG')
    name='m2_test_'+uuid.uuid4().hex
    pg=psycopg2.connect(base);pg.autocommit=True
    with pg.cursor() as c:c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    pg.close()
    yield base+' dbname='+name
    pg=psycopg2.connect(base);pg.autocommit=True
    with pg.cursor() as c:
        c.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s',(name,))
        c.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(name)))
    pg.close()


def fixture(root,bad=False):
    root.mkdir();m=fixture_manifest(root)
    ref=root/'r.json';ref.write_text('{"a":1,"a":2}')
    m['references']=[dict(path=str(ref),sha256=hashlib.sha256(ref.read_bytes()).hexdigest())]
    for i,e in enumerate(m['inputs'][1:]):
        endpoint=struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\0\x02\x01'*2
        raw=update()+update(ann=b'',attrs=b'')+update(subtype=7)+mrt(endpoint+struct.pack('!HH',6,1),5)
        raw+=update(ann=struct.pack('!I',42)+b'\x18\xc0\0\x02',subtype=9)
        raw+=mrt(struct.pack('!I',123456)+update()[12:],4,17)
        if bad and i==0:raw+=update(attrs=b'\xf0\x23\x04\0\x04\x2f\x66')+update()
        path=Path(e['path']);path.write_bytes(gzip.compress(raw,mtime=0))
        e['sha256']=hashlib.sha256(path.read_bytes()).hexdigest();e['size']=path.stat().st_size
        from data_pipeline.bgp.input.mrt_reader import source_identity
        e['source_id']=source_identity('rrc25',e['origin_uri'],e['sha256'])
    m['update_sources']=[e['source_id'] for e in m['inputs'][1:]]
    return m


def run(m,dsn,out,**kw):return produce_checkpointed(m,dsn,out,batch_rows=2,min_free_bytes=0,**kw)


def selected_rows(dsn,seal):
    b=Selection(dsn,seal['run_id'],seal['snapshot']);d=b.connect();result={}
    try:
        for name in b.columns:
            rows=d.execute('SELECT * FROM '+b.table(name)).fetchall()
            # interpretation source_path可相同；此fixture共用输入原件。
            result[name]=sorted((sha(row) for row in rows))
        return result
    finally:d.close()


def worker(config,path):
    path.write_text(json.dumps(config));log=path.with_suffix('.log').open('w')
    p=subprocess.Popen([sys.executable,str(Path(__file__).with_name('m2_worker.py')),str(path)],stdout=log,stderr=subprocess.STDOUT)
    log.close();return p


def wait_marker(p,path):
    deadline=time.monotonic()+30
    while not path.exists():
        if p.poll() is not None:pytest.fail('worker在故障点前退出: '+str(path))
        if time.monotonic()>deadline:p.kill();p.wait();pytest.fail('人工故障点未到达')
        time.sleep(.02)


@pytest.mark.parametrize('policy,bad',[('strict/v1',False),('isolate-payload/v1',True)])
def test_complete_stream_all_tables_and_legacy_gate(tmp_path,dsn,policy,bad):
    m=fixture(tmp_path/'input',bad);seal=run(m,dsn,tmp_path/'run',policy=policy)
    reader=ObservationReader(dsn,seal['run_id'],seal['snapshot'],[e['source_id'] for e in m['inputs']],profile='observation')
    stream=list(reader.stream())
    assert len([s for s in stream if isinstance(s,SourceEnd)])==3
    messages=[r for b in stream if isinstance(b,MessageBatch) for r in b.messages]
    assert sum(json.loads(r['interpretation'])['status']=='rejected' for r in messages)==int(bad)
    assert len(list(reader.reference_batches(m['references'][0]['sha256'])))>0
    assert query(dsn,seal['run_id'],'messages',profile='observation')
    assert list(scan(dsn,seal['run_id'],'paths',profile='observation'))
    assert list(scan_observations(dsn,seal['run_id'],profile='observation'))
    with pytest.raises((ValueError,psycopg2.errors.UndefinedTable)):
        ObservationReader(dsn,seal['run_id'],seal['snapshot'],[m['inputs'][0]['source_id']])
    with pytest.raises(ValueError):reader.bound_table('changes')
    rows=selected_rows(dsn,seal);assert set(rows)==set(reader.selection.columns)
    assert seal['business']=='not_run'


@pytest.mark.parametrize('label,ordinal', [('file_mid',2),('before_checkpoint',2),('after_checkpoint',2),('before_seal',4),('seal_pre_commit',4),('seal_post_commit',4)])
def test_actual_kill_resume_equals_continuous(tmp_path,dsn,label,ordinal):
    m=fixture(tmp_path/'input',True)
    # 同一catalog的唯一数据根：不同run也使用相同绑定根。
    data=tmp_path/'data'
    baseline=run(m,dsn,tmp_path/'baseline',policy='isolate-payload/v1',catalog_data_path=data)
    config=dict(manifest=m,dsn=dsn,output=str(tmp_path/'resume'),policy='isolate-payload/v1',stop=label,ordinal=ordinal,marker=str(tmp_path/'marker'))
    # worker也绑定同一catalog根
    config['catalog_data_path']=str(data)
    p=worker(config,tmp_path/'config.json');wait_marker(p,Path(config['marker']))
    os.kill(p.pid,signal.SIGKILL);assert p.wait()!=0
    config.pop('stop');p=worker(config,tmp_path/'config.json');assert p.wait(timeout=30)==0, (tmp_path/'config.log').read_text()
    rid=json.loads((tmp_path/'resume/run.json').read_text())['run_id']
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute('SELECT seal FROM observation_m2.runs WHERE run_id=%s',(rid,));resumed=c.fetchone()[0]
    assert selected_rows(dsn,baseline)==selected_rows(dsn,resumed)
    assert [(c['counts'],{t:(v['count'],v['digest']) for t,v in c['tables'].items()}) for c in baseline['checkpoints']]==[(c['counts'],{t:(v['count'],v['digest']) for t,v in c['tables'].items()}) for c in resumed['checkpoints']]
    events=[json.loads(l) for l in (tmp_path/'resume/events.jsonl').read_text().splitlines()]
    parsed=[e['ordinal'] for e in events if e['label']=='parse_start']
    for i in range(ordinal if label in ('file_mid','before_checkpoint') else min(ordinal+1,4)):
        assert parsed.count(i)==1
    if label in ('file_mid','before_checkpoint'):assert parsed.count(ordinal)==2
    # seal仅增加path_owner，既有消息/元素未再写一份。
    selected=Selection(dsn,rid,resumed['snapshot']);d=selected.connect()
    assert d.execute(f'SELECT count(DISTINCT attempt) FROM lake.{selected.schema}.messages').fetchone()[0]<=4
    d.close()
    (tmp_path/'对照回执.json').write_text(json.dumps(dict(boundary=label,ordinal=ordinal,killed_pid=json.loads(Path(config['marker']).read_text())['pid'],baseline_run=baseline['run_id'],resumed_run=rid,tables=selected_rows(dsn,resumed),parse_calls=parsed,checkpoint_counts=[c['counts'] for c in resumed['checkpoints']],equal=True),ensure_ascii=False,indent=2))


def test_late_selected_attempt_rows_invisible(tmp_path,dsn):
    m=fixture(tmp_path/'input');injected=[]
    def hook(label,ordinal,owner):
        if label!='before_seal':return
        with owner.pg.cursor() as c:c.execute('SELECT payload FROM observation_m2.checkpoints WHERE run_id=%s AND ordinal=2',(owner.run_id,));cp=c.fetchone()[0]
        from data_pipeline.bgp.archive.store import connect_duckdb, literal
        d=connect_duckdb();d.execute('LOAD ducklake');d.execute('LOAD postgres');d.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake')
        table='lake.m2_'+owner.run_id+'.messages'
        d.execute(f'INSERT INTO {table} SELECT * FROM {table} WHERE attempt=?',[cp['attempt']]);d.close();injected.append(True)
    seal=run(m,dsn,tmp_path/'run',hook=hook);b=Selection(dsn,seal['run_id'],seal['snapshot']);d=b.connect()
    physical=d.execute(f'SELECT count(*) FROM lake.{b.schema}.messages').fetchone()[0]
    logical=d.execute('SELECT count(*) FROM '+b.table('messages')).fetchone()[0]
    assert injected and physical>logical and logical==sum(c['counts']['messages'] for c in seal['checkpoints']);d.close()


def test_alias_and_plan_drift(tmp_path,dsn):
    m=fixture(tmp_path/'input');out=tmp_path/'run';run(m,dsn,out)
    alias=tmp_path/'alias.json';alias.write_bytes(Path(m['references'][0]['path']).read_bytes())
    same=copy.deepcopy(m);same['references'][0]['path']=str(alias);assert run(same,dsn,out)['qualification']=='observation_sealed'
    csv=tmp_path/'alias.csv';csv.write_bytes(alias.read_bytes());wrong=copy.deepcopy(m);wrong['references'][0]['path']=str(csv)
    with pytest.raises(ValueError,match='漂移'):run(wrong,dsn,out)
    for change in ('order','role','sha'):
        altered=copy.deepcopy(m)
        if change=='order':altered['update_sources'].reverse()
        elif change=='role':altered['inputs'][1]['role']='snapshot'
        else:altered['inputs'][1]['sha256']='0'*64
        with pytest.raises(ValueError):run(altered,dsn,out)


def test_resources_and_strict_bad_source(tmp_path,dsn):
    m=fixture(tmp_path/'input',True)
    with pytest.raises(ValueError):run(m,dsn,tmp_path/'bad')
    with pytest.raises(RuntimeError,match='RSS'):run(m,dsn,tmp_path/'rss',max_rss_bytes=1)
    with pytest.raises(RuntimeError,match='磁盘'):produce_checkpointed(m,dsn,tmp_path/'disk',min_free_bytes=10**30)


@pytest.mark.parametrize('operation',['checkpoint','seal','failed'])
def test_lost_lock_alive_worker_cannot_commit(tmp_path,dsn,operation):
    rid=uuid.uuid4().hex;marker=tmp_path/'owner';config=tmp_path/'owner.json'
    config.write_text(json.dumps(dict(dsn=dsn,run_id=rid,marker=str(marker),operation=operation)))
    p=subprocess.Popen([sys.executable,str(Path(__file__).with_name('m2_owner_worker.py')),str(config)])
    try:
        wait_marker(p,marker)
        pid=json.loads(marker.read_text())['pg_pid']
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:
            # PG终止默认只发送信号；等待会话退出后才验证新所有者取锁。
            c.execute('SELECT pg_terminate_backend(%s,5000)',(pid,));assert c.fetchone()[0]
        assert p.poll() is None
        b=Owner(dsn,rid)
        with b.transaction() as c:c.execute("UPDATE observation_m2.runs SET state='taken_over' WHERE run_id=%s",(rid,))
        Path(str(marker)+'.release').touch();assert p.wait(timeout=10)==0
        assert Path(str(marker)+'.rejected').exists()
        with b.transaction() as c:
            c.execute('SELECT state FROM observation_m2.runs WHERE run_id=%s',(rid,));assert c.fetchone()[0]=='taken_over'
            c.execute('SELECT count(*) FROM observation_m2.checkpoints WHERE run_id=%s',(rid,));assert c.fetchone()[0]==0
        b.close()
    finally:
        if p.poll() is None:p.kill();p.wait()


def test_late_update_is_rejected_before_seal(tmp_path,dsn):
    m=fixture(tmp_path/'input')
    def hook(label,ordinal,owner):
        if label!='before_seal':return
        from data_pipeline.bgp.archive.store import connect_duckdb, literal
        d=connect_duckdb();d.execute('LOAD ducklake');d.execute('LOAD postgres');d.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake')
        d.execute(f'UPDATE lake.m2_{owner.run_id}.messages SET epoch=999');d.close()
    with pytest.raises(ValueError,match='删除/更新|不再可见'):run(m,dsn,tmp_path/'run',hook=hook)


def test_file_corruption_and_format_aliases(tmp_path,dsn):
    m=fixture(tmp_path/'input');seal=run(m,dsn,tmp_path/'run')
    f=seal['checkpoints'][0]['files'][0]['path'];p=Path(f);raw=p.read_bytes();p.write_bytes(raw+b'corruption')
    with pytest.raises(ValueError,match='文件漂移'):run(m,dsn,tmp_path/'run')
    from data_pipeline.bgp.input.reference_reader import resolved_interpretation
    from openpyxl import Workbook
    a=tmp_path/'a.xlsx';b=tmp_path/'b.csv';book=Workbook();book.active.append([1,'001']);book.save(a);book.close();b.write_bytes(a.read_bytes())
    assert resolved_interpretation(a)==resolved_interpretation(b)
    alias=copy.deepcopy(m);r=m['references'][0];csv=tmp_path/'other.csv';csv.write_bytes(Path(r['path']).read_bytes());alias['references'].append(dict(path=str(csv),sha256=r['sha256']))
    with pytest.raises(ValueError,match='别名'):plan_for(alias,'strict/v1')


@pytest.mark.parametrize('drift',['code','schema','policy','data_path'])
def test_resume_version_drift_rejected(tmp_path,dsn,monkeypatch,drift):
    import data_pipeline.bgp.archive.checkpoint as module
    m=fixture(tmp_path/'input');out=tmp_path/'run';run(m,dsn,out)
    kw={}
    if drift=='code':monkeypatch.setattr(module,'code_identity',lambda:{'different':'code'})
    elif drift=='schema':monkeypatch.setattr(module,'VERSION','future-version')
    elif drift=='policy':kw['policy']='isolate-payload/v1'
    else:kw['catalog_data_path']=tmp_path/'different'
    with pytest.raises(ValueError,match='漂移'):run(m,dsn,out,**kw)


@pytest.mark.parametrize('damage',['path_conflict','orphan'])
def test_persisted_integrity_not_just_counts(tmp_path,dsn,damage):
    from data_pipeline.bgp.archive.checkpoint import AttemptStore, Guard, setup
    from data_pipeline.bgp.input.mrt_reader import read_source
    m=fixture(tmp_path/'input');root=tmp_path/'attempt';root.mkdir();owner=Owner(dsn,uuid.uuid4().hex);setup(owner)
    store=AttemptStore(dsn,'fixture',str(tmp_path/'data'),root,owner,Guard(root,tmp_path/'data',0,4*1024**3),100)
    e=m['inputs'][1]
    message=next(read_source(e['path'],e['sha256'],source_id=e['source_id']))
    try:
        store.message(message)
        if damage=='path_conflict':
            changed=dict(message.paths[0]);changed['as_path_text']='changed';store.append('paths',changed)
            with pytest.raises(ValueError,match='不同typed'):store.flush()
        else:
            store.pending['elements'][0]['path_key']='missing'
            with pytest.raises(ValueError,match='孤立'):store.checkpoint_data(dict(messages=1,elements=len(message.elements)))
    finally:store.close();owner.close()


def test_single_writer_and_checkpoint_selection_drift(tmp_path,dsn):
    a=Owner(dsn,'locked')
    try:
        with pytest.raises(ValueError,match='所有者'):Owner(dsn,'locked')
    finally:a.close()
    m=fixture(tmp_path/'input');seal=run(m,dsn,tmp_path/'run')
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE observation_m2.checkpoints SET payload=jsonb_set(payload,'{counts,messages}','999') WHERE run_id=%s AND ordinal=1",(seal['run_id'],))
    with pytest.raises(ValueError,match='漂移'):Selection(dsn,seal['run_id'],seal['snapshot'])


def test_reference_rows_and_input_alias_once(tmp_path,dsn):
    m=fixture(tmp_path/'input')
    csv=tmp_path/'r.csv';csv.write_text('key,value\n001,2\n\n \n001,2\n')
    m['references'].append(dict(path=str(csv),sha256=hashlib.sha256(csv.read_bytes()).hexdigest()))
    m['inputs'].append(dict(m['inputs'][1]))
    seal=run(m,dsn,tmp_path/'run');b=Selection(dsn,seal['run_id'],seal['snapshot']);d=b.connect()
    refs=d.execute('SELECT location,raw_row,effective_time_state,csv_record_kind FROM '+b.table('references')+' ORDER BY source_id,row').fetchall()
    assert sum(r[0]=='csv' for r in refs)==5
    assert all(r[2]=='unknown' for r in refs)
    assert any(r[3]=='blank_line' for r in refs) and any(r[3]=='whitespace_line' for r in refs)
    assert len(seal['checkpoints'])==5;d.close()


def test_internal_mapping_replay_use_fixed_selection(tmp_path,dsn):
    from types import SimpleNamespace
    from data_pipeline.bgp.replay.archive_input import build_mapping, messages
    m=fixture(tmp_path/'input',True);seal=run(m,dsn,tmp_path/'run',policy='isolate-payload/v1')
    reader=ObservationReader(dsn,seal['run_id'],seal['snapshot'],[e['source_id'] for e in m['inputs']],profile='observation')
    db=reader.connect();saved=[]
    sink=SimpleNamespace(dsn=dsn,db=db,schema=reader.schema,root=tmp_path,memory_limit='1GB',flush=lambda:None,append=lambda *r:saved.append(r))
    try:
        build_mapping(sink,m['baseline_source'],m['update_sources'],reader=reader)
        assert saved
        restored=list(messages(sink,m['baseline_source'],m['update_sources'],seal['snapshot'],reader=reader))
        assert len(restored)==sum(c['counts']['messages'] for c in seal['checkpoints'])
        rejected=[r for r in restored if r.reason]
        assert len(rejected)==1 and rejected[0].interpretation.status=='rejected' and not rejected[0].elements
    finally:db.close()


def test_explicit_checkpoint_cli(tmp_path,dsn):
    m=fixture(tmp_path/'input');manifest=tmp_path/'manifest.json';manifest.write_text(json.dumps(m))
    config=tmp_path/'config.json';config.write_text(json.dumps(dict(dsn=dsn,limits=dict(min_free_bytes=0,batch_rows=2))))
    entry=Path(__file__).resolve().parents[3]/'scripts/observations.py'
    completed=subprocess.run([sys.executable,str(entry),'checkpoint','--manifest',str(manifest),'--config',str(config),'--output',str(tmp_path/'out')],capture_output=True,text=True)
    assert completed.returncode==0,completed.stderr
    result=json.loads(completed.stdout)
    assert result['qualification']=='observation_sealed' and result['business']=='not_run'


def test_seal_writes_no_observation_rows(tmp_path,dsn):
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    from data_pipeline.bgp.archive.checkpoint import OBS_TABLES
    m=fixture(tmp_path/'input');snapshots={}
    def hook(label,ordinal,owner):
        if label not in ('before_seal','seal_post_commit'):return
        db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres');db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
        snapshots[label]={name:db.execute(f'SELECT count(*) FROM lake.m2_{owner.run_id}."{name}"').fetchone()[0] for name in OBS_TABLES}
        db.close()
    run(m,dsn,tmp_path/'run',hook=hook)
    assert snapshots['before_seal']==snapshots['seal_post_commit']
    (tmp_path/'封存无正文重写.json').write_text(json.dumps(snapshots,ensure_ascii=False,indent=2))


def test_cross_file_path_typed_conflict_rejected(tmp_path,dsn,monkeypatch):
    import data_pipeline.bgp.archive.checkpoint as module
    m=fixture(tmp_path/'input');real=module.read_source
    def corrupt(path,*args,**kwargs):
        for message in real(path,*args,**kwargs):
            if str(path)==m['inputs'][2]['path']:
                for p in message.paths:p['as_path_text']='injected conflicting interpretation'
            yield message
    monkeypatch.setattr(module,'read_source',corrupt)
    with pytest.raises(ValueError,match='同path_key不同typed'):run(m,dsn,tmp_path/'run')
