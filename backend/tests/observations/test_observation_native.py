"""原生 RIB：bgpdump 文本、原始证据与项目归属规则分别核对。"""
from dataclasses import asdict
import gzip
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys

import pyarrow as pa
import pytest

from data_pipeline.bgp.archive.checkpoint import OBS_TABLES, sha
from data_pipeline.bgp.input.mrt_reader import read_source
from data_pipeline.bgp.input.native_parser import NativeRuntime, DISPLAY_FIELDS
from tests.observations.test_observation_mrt import mrt, attr
from tests.observations.test_observation_checkpoint import dsn, selected_rows


@pytest.fixture
def native():
    location = os.environ.get('DOMEYE_NATIVE_BUILD')
    if not location:
        pytest.skip('需显式绑定本项目的原生候选构建')
    return NativeRuntime(Path(location))


def raw_fixture():
    peer = b'\0\0\0\x19\0\x05rrc25\0\x02'
    peer += b'\x02' + b'\xc0\0\x02\x01' * 2 + struct.pack('!I', 64497)
    peer += b'\x03' + b'\xc0\0\x02\x02' + bytes.fromhex('20010db8000000000000000000000001') + struct.pack('!I', 65536)
    rows = [mrt(peer, 1, 13)]
    attrs = [attr(), attr(64512), b'\x40\x02\x06\x01\x01' + struct.pack('!I', 64496),
             attr() + b'\xc0\x11\x06\x02\x01' + struct.pack('!I', 64496),
             b'\x40\x02\x06\x03\x01' + struct.pack('!I', 64496), b'', attr(23456),
             attr(0), attr(4294967295), attr(4200000000), attr()]
    for i, a in enumerate(attrs):
        prefix = bytes([25, 192, 0, i, 255]) if i % 2 == 0 else bytes.fromhex('2120010db8ff')
        body = struct.pack('!I', i) + prefix + struct.pack('!H', 2)
        body += b''.join(struct.pack('!HIH', j, 90 + j, len(a)) + a for j in range(2))
        rows.append(mrt(body, 2 if i % 2 == 0 else 4, 13))
    return b''.join(rows)


def expected_rows(path, source_id='fixture'):
    result = {name: [] for name in OBS_TABLES}
    paths = {}
    for m in read_source(path, hashlib.sha256(path.read_bytes()).hexdigest(), source_id=source_id):
        row = {name: getattr(m, name, None) for name, _ in OBS_TABLES['messages']}
        row.update(message_id=m.message_id, bgp_id_present=False, interpretation=json.dumps(asdict(m.interpretation), ensure_ascii=False))
        result['messages'].append(row)
        result['peers'].extend({k: p.get(k) for k, _ in OBS_TABLES['peers']} for p in m.peers)
        for e in m.elements:
            p=e['peer']
            row={**e, 'event_id':m.message_id+':'+str(e['ordinal']), 'message_id':m.message_id,
                 'peer_ip':p['ip'], 'peer_asn':p['asn'], 'bgp_id':p['bgp_id'], 'bgp_id_present':True,
                 'peer_table_record':p['table_record'], 'peer_index':p['index']}
            result['elements'].append({k:row.get(k) for k,_ in OBS_TABLES['elements']})
        paths.update((p['path_key'], p) for p in m.paths)
    result['paths']=list(paths.values())
    return result


def evidence_rows(table,rows):
    return [{k:v for k,v in row.items() if k not in DISPLAY_FIELDS.get(table,set())} for row in rows]


def test_native_evidence_fields_and_path_rules(tmp_path, native):
    path=tmp_path/'fixture.gz';path.write_bytes(gzip.compress(raw_fixture(),mtime=0))
    expected=expected_rows(path)
    output=tmp_path/'native'
    native.parse(path, 'fixture', hashlib.sha256(path.read_bytes()).hexdigest(), output, batch_rows=3)
    actual=native.rows(output)
    for table in OBS_TABLES:
        if table=='messages':
            for rows in (expected[table],actual[table]):
                for row in rows:row['interpretation']=json.loads(row['interpretation'])
        assert sorted(map(sha,evidence_rows(table,actual[table])))==sorted(map(sha,evidence_rows(table,expected[table]))), table


def test_native_compressed_segments_roundtrip(tmp_path,native):
    # 重复的完整 MRT 记录可压缩，但不能丢失观察或改变列式内容。
    raw=raw_fixture()*100
    path=tmp_path/'repeated.gz';path.write_bytes(gzip.compress(raw,mtime=0))
    output=tmp_path/'native'
    result=native.parse(path,'fixture',hashlib.sha256(path.read_bytes()).hexdigest(),output,batch_rows=1000)
    assert result['elements']==2200
    compressed=uncompressed=0
    for p in output.glob('segment-*/*.arrow'):
        with pa.memory_map(str(p),'r') as f:table=pa.ipc.open_file(f).read_all()
        sink=pa.BufferOutputStream()
        with pa.ipc.new_file(sink,table.schema) as writer:writer.write_table(table)
        buffer=sink.getvalue()
        assert pa.ipc.open_file(buffer).read_all().equals(table,check_metadata=True)
        compressed+=p.stat().st_size;uncompressed+=len(buffer)
    assert compressed<uncompressed/2
    import sqlite3
    with sqlite3.connect('file:'+str(output/'paths.sqlite')+'?mode=ro&immutable=1',uri=True) as db:
        assert db.execute('PRAGMA quick_check').fetchone()==('ok',)
        assert db.execute('SELECT count(*) FROM paths').fetchone()[0]==len(native.rows(output)['paths'])
    wal=output/'paths.sqlite-wal'
    assert not wal.exists() or wal.stat().st_size==0


def test_native_resume_partially_published_group(tmp_path,native):
    path=tmp_path/'fixture.gz';path.write_bytes(gzip.compress(raw_fixture(),mtime=0))
    digest=hashlib.sha256(path.read_bytes()).hexdigest();output=tmp_path/'native'
    native.parse(path,'fixture',digest,output,batch_rows=3)
    expected=native.rows(output)
    # 模拟字典事务已提交，但只公布了组内第一段时进程退出。
    (output/'complete.json').unlink();(output/'native-eof.json').unlink()
    markers=sorted(output.glob('segment-*/committed.json'));assert len(markers)>3
    for marker in markers[1:]:marker.unlink()
    native.parse(path,'fixture',digest,output,batch_rows=3)
    assert native.rows(output)==expected


def test_native_text_matches_upstream_cli(tmp_path,native):
    from ipaddress import ip_network
    path=tmp_path/'fixture.gz';path.write_bytes(gzip.compress(raw_fixture(),mtime=0))
    source=hashlib.sha256(path.read_bytes()).hexdigest();native.parse(path,'fixture',source,tmp_path/'native',batch_rows=3)
    rows=native.rows(tmp_path/'native');paths={p['path_key']:p for p in rows['paths']}
    actual=[(e['peer_ip'],str(e['peer_asn']),e['prefix'],paths[e['path_key']]['as_path_text']) for e in rows['elements']]
    cli=subprocess.run([str(native.build/'bgpdump-reference'),'-m',str(path)],capture_output=True,text=True,check=True,timeout=30)
    expected=[tuple(line.split('|')[3:7]) for line in cli.stdout.splitlines() if line.startswith('TABLE_DUMP2|')]
    def evidence(rows):return [(a,b,ip_network(c,strict=False),d) for a,b,c,d in rows]
    assert len(expected)==22 and evidence(actual)==evidence(expected)


def test_native_never_consumes_marker_without_file_hashes(tmp_path,native,monkeypatch):
    path=tmp_path/'fixture.gz';path.write_bytes(gzip.compress(raw_fixture(),mtime=0));delayed=[];seen=[]
    original=NativeRuntime.seal_segments
    def delay_one(root):
        original(root)
        markers=sorted(root.glob('segment-*/committed.json'))
        if markers and not delayed:
            marker=markers[-1];body=json.loads(marker.read_text());body.pop('files')
            marker.write_text(json.dumps(body));delayed.append(marker)
    monkeypatch.setattr(NativeRuntime,'seal_segments',staticmethod(delay_one))
    def consume(directory,segment):
        assert 'files' in segment
        native.verify_segment(directory,segment);seen.append(segment['segment'])
    result=native.parse(path,'fixture',hashlib.sha256(path.read_bytes()).hexdigest(),tmp_path/'out',batch_rows=3,consume=consume)
    assert delayed and seen==list(range(result['segments']))


@pytest.mark.parametrize('ordinals,valid',[
    ([0,1,2],True),([0,0,2],False),([0,2],False),([1,2],False),([-1,0],False),([0,None],False),
])
def test_native_partitioned_ordinal_check(ordinals,valid):
    import duckdb
    from data_pipeline.bgp.archive.native_writer import NativeStore
    store=object.__new__(NativeStore);store.db=duckdb.connect();store.guard=lambda:None
    try:
        store.db.execute('CREATE TABLE elements(message_id VARCHAR,ordinal BIGINT)')
        store.db.executemany('INSERT INTO elements VALUES (?,?)',[('first',x) for x in ordinals]+[('second',0),(None,0)])
        # NULL message_id 的引用资格由后续 FK 检查负责；其序号组仍必须被检查。
        if valid:store.check_element_ordinals('elements')
        else:
            with pytest.raises(ValueError,match='元素序号损坏'):store.check_element_ordinals('elements')
    finally:store.db.close()


@pytest.mark.parametrize('repeat',[1,3000])
def test_native_digest_matches_existing_typed_encoding(native,repeat):
    rows=[{'文字':'中文\n"\\\x00', '整数':-42, '大整数':4294967295, '空':None, '原字节':b'\0\xff', '布尔':True},
          {'文字':'', '整数':0, '大整数':None, '空':None, '原字节':b'', '布尔':False}]
    rows=rows*repeat;table=pa.Table.from_pylist(rows)
    expected=hashlib.sha256(b''.join(bytes.fromhex(sha(row)) for row in rows)).hexdigest()
    assert native.digest(table.to_reader())==(len(rows),expected)


@pytest.mark.parametrize('mode',[1,2,3,4])
def test_native_narrow_sort_preserves_v1_order(native,mode):
    rows=[dict(record=r,message_id='same_source:'+str(r),ordinal=ordinal,table_record=r,index=ordinal,
               path_key=hashlib.sha256(str((r,ordinal)).encode()).hexdigest(),payload=b'\xff\0',unknown=None)
          for r in (2,1,10,100,11,1000000,999999,0) for ordinal in (1,0)]
    keys={1:('record',),2:('message_id','ordinal'),3:('path_key',),4:('table_record','index')}[mode]
    expected=hashlib.sha256(b''.join(bytes.fromhex(sha(r)) for r in sorted(rows,key=lambda r:tuple(r[k] for k in keys)))).hexdigest()
    hashed=native.hash_rows(pa.Table.from_pylist(rows).to_reader(),mode).read_all().sort_by([('k1','ascending'),('k2','ascending')])
    assert native.combine(hashed.select(['digest']).to_reader())==(len(rows),expected)


@pytest.mark.parametrize('address,text',[('00000000000000000000ffffc0000201','::ffff:192.0.2.1/128'),('000000000000000000000000c0000201','::192.0.2.1/128')])
def test_native_mapped_ipv6_retains_bgpdump_text(tmp_path,native,address,text):
    raw=raw_fixture();peer=raw[:12+struct.unpack_from('!I',raw,8)[0]];attrs=attr()
    body=struct.pack('!I',0)+b'\x80'+bytes.fromhex(address)+struct.pack('!HHIH',1,0,90,len(attrs))+attrs
    path=tmp_path/'mapped.gz';path.write_bytes(gzip.compress(peer+mrt(body,4,13),mtime=0))
    expected=expected_rows(path)
    native.parse(path,'fixture',hashlib.sha256(path.read_bytes()).hexdigest(),tmp_path/'out')
    actual=native.rows(tmp_path/'out')['elements']
    assert actual[0]['prefix']==text
    assert evidence_rows('elements',actual)==evidence_rows('elements',expected['elements'])
    from data_pipeline.bgp.archive.native_writer import NativeAudit, arrow_file
    audit=NativeAudit(tmp_path/'out')
    for segment in sorted((tmp_path/'out').glob('segment-*')):
        for f in sorted(segment.glob('*.arrow'),key=lambda p:(p.stem!='messages',p.name)):audit.check(f.stem,arrow_file(f))
    assert audit.complete()['all_element_raw_prefixes']==1


@pytest.mark.parametrize('damage',['tail','body','gzip','attribute','update'])
def test_native_rejects_damage_without_complete(tmp_path,native,damage):
    raw=raw_fixture()
    if damage=='tail':raw+=b'\0\x01'
    elif damage=='body':raw=raw[:-1]
    elif damage=='attribute':raw=raw.replace(attr(),b'\x40\x02\x09'+attr()[3:])
    elif damage=='update':raw+=mrt(b'\0'*20)
    packed=gzip.compress(raw,mtime=0)
    if damage=='gzip':packed=packed[:-4]
    path=tmp_path/'bad.gz';path.write_bytes(packed)
    with pytest.raises((ValueError,RuntimeError,EOFError)):
        native.parse(path,'fixture',hashlib.sha256(packed).hexdigest(),tmp_path/'out',batch_rows=3)
    assert not (tmp_path/'out'/'complete.json').exists()


def manifest_for(root):
    from tests.observations.test_observation_two_phase import fixture_manifest
    from data_pipeline.bgp.input.mrt_reader import source_identity
    root.mkdir();manifest=fixture_manifest(root);entry=manifest['inputs'][0]
    path=Path(entry['path']);path.write_bytes(gzip.compress(raw_fixture(),mtime=0))
    entry['sha256']=hashlib.sha256(path.read_bytes()).hexdigest();entry['size']=path.stat().st_size
    entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
    manifest.update(inputs=[entry],baseline_source=entry['source_id'],update_sources=[])
    return manifest


def test_native_m2_preserves_evidence_and_seals_actual_rows(tmp_path,native,dsn):
    from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
    from data_pipeline.bgp.archive.native_writer import produce_native_rib
    manifest=manifest_for(tmp_path/'input');data=tmp_path/'lake'
    old=produce_checkpointed(manifest,dsn,tmp_path/'old',catalog_data_path=data,min_free_bytes=0,batch_rows=3)
    new=produce_native_rib(manifest,dsn,tmp_path/'new',build=native.build,catalog_data_path=data,min_free_bytes=0,batch_rows=3,deadline_seconds=None)
    assert new['qualification']=='observation_sealed' and new['business']=='not_run'
    from data_pipeline.bgp.archive.selection import Selection
    from data_pipeline.bgp.archive.checkpoint import KEYS
    selections=[]
    for seal in (old,new):
        selection=Selection(dsn,seal['run_id'],seal['snapshot']);db=selection.connect();rows={}
        try:
            for table in OBS_TABLES:
                actual=db.execute('SELECT * FROM '+selection.table(table)+' ORDER BY '+KEYS[table]).fetch_arrow_table()
                rows[table]=sorted(map(sha,evidence_rows(table,actual.to_pylist())))
                if seal is new:
                    assert native.digest(actual.to_reader())==(seal['checkpoints'][0]['tables'][table]['count'],seal['checkpoints'][0]['tables'][table]['digest'])
        finally:db.close()
        selections.append(rows)
    assert selections[0]==selections[1]
    for table in OBS_TABLES:
        assert old['checkpoints'][0]['tables'][table]['count']==new['checkpoints'][0]['tables'][table]['count']
    assert old['checkpoints'][0]['context']==new['checkpoints'][0]['context']


@pytest.mark.parametrize('damage',[False,True])
def test_native_checkpoint_only_resume_verifies_persisted_files(tmp_path,native,dsn,monkeypatch,damage):
    from data_pipeline.bgp.archive.native_writer import produce_native_rib
    config=dict(manifest=manifest_for(tmp_path/'input'),dsn=dsn,output=tmp_path/'out',build=native.build,
                catalog_data_path=tmp_path/'lake',min_free_bytes=0,batch_rows=3)
    def stop(label,ordinal,owner):
        if label=='before_checkpoint':raise RuntimeError('模拟最终校验中断')
    with pytest.raises(RuntimeError,match='模拟最终校验中断'):produce_native_rib(**config,hook=stop)
    def no_parse(*args,**kwargs):pytest.fail('已有完整核对回执，应仅恢复 checkpoint')
    monkeypatch.setattr(NativeRuntime,'parse',no_parse)
    if damage:
        file=next((tmp_path/'lake/native').rglob('*.parquet'));content=file.read_bytes();file.write_bytes(content[:-1]+bytes([content[-1]^1]))
        with pytest.raises(ValueError,match='已登记分段文件漂移'):produce_native_rib(**config)
    else:assert produce_native_rib(**config)['qualification']=='observation_sealed'


@pytest.mark.parametrize('stop',['segment_files_ready','after_segment','before_checkpoint','after_checkpoint','seal_pre_commit','seal_post_commit'])
def test_native_actual_process_kill_resume(tmp_path,native,dsn,stop):
    from data_pipeline.bgp.archive.native_writer import produce_native_rib
    manifest=manifest_for(tmp_path/'input');data=tmp_path/'lake'
    baseline=produce_native_rib(manifest,dsn,tmp_path/'baseline',build=native.build,catalog_data_path=data,min_free_bytes=0,batch_rows=3)
    config=dict(manifest=manifest,dsn=dsn,output=str(tmp_path/'resume'),build=str(native.build),catalog_data_path=str(data),min_free_bytes=0,batch_rows=3)
    path=tmp_path/'config.json';path.write_text(json.dumps(config))
    worker=tmp_path/'worker.py'
    worker.write_text('''import json,os,signal,sys
from pathlib import Path
from data_pipeline.bgp.archive.native_writer import produce_native_rib
config=json.loads(Path(sys.argv[1]).read_text())
def hook(label,ordinal,owner):
    if label==sys.argv[2]: os.killpg(os.getpgrp(),signal.SIGKILL)
produce_native_rib(**config,hook=hook)
''')
    env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parents[2])}
    # 与真实服务的 KillMode=control-group 一致，同时终止解析子进程。
    child=subprocess.run([sys.executable,str(worker),str(path),stop],env=env,capture_output=True,timeout=90,start_new_session=True)
    assert child.returncode==-9,child.stderr.decode()
    resumed=produce_native_rib(**config)
    assert selected_rows(dsn,baseline)==selected_rows(dsn,resumed)
    assert resumed['checkpoints'][0]['counts']==baseline['checkpoints'][0]['counts']
    assert resumed['checkpoints'][0]['tables']==baseline['checkpoints'][0]['tables']
