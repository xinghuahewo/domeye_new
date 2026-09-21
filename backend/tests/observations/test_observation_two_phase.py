"""独立PG验证来源/基线/回执门禁及不重解析的二阶段回放。"""
import gzip
import hashlib
import json
import os
from pathlib import Path
import struct
import uuid
import pytest
import psycopg2
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.bgp.replay.run_from_files import produce, normalized_inputs
from data_pipeline.bgp.archive.store import Store, scan, query
from tests.observations.test_observation_mrt import mrt, attr, update


def fixture_manifest(root):
    table=b'\0\0\0\x19\0\x05rrc25\0\x01\x02'+b'\xc0\0\x02\x01'*2+struct.pack('!I',64497)
    attrs=attr()
    rib=struct.pack('!I',0)+b'\x18\xc0\0\x02'+struct.pack('!HHIH',1,0,90,len(attrs))+attrs
    inputs=[]
    for name,data,role in [('rib',mrt(table,1,13)+mrt(rib,2,13),'baseline'),('a',update(attrs=attr(64498)),'update'),('b',update(attrs=attr(64498)),'update')]:
        path=root/(name+'.gz');path.write_bytes(gzip.compress(data,mtime=0))
        sha=hashlib.sha256(path.read_bytes()).hexdigest();uri='fixture://rrc25/'+name
        inputs.append({'path':str(path),'sha256':sha,'source_id':source_identity('rrc25',uri,sha),'origin_uri':uri,'size':path.stat().st_size,'role':role})
    return {'schema_version':'observation-run/v1','collector':'rrc25','window_start':'1970-01-01T00:00:00Z',
        'window_end_exclusive':'1970-01-02T00:00:00Z','inputs':inputs,'baseline_source':inputs[0]['source_id'],
        'update_sources':[i['source_id'] for i in inputs[1:]]}


@pytest.fixture
def dsn():
    base=os.environ.get('DOMEYE_STAGE1_TEST_DSN')
    if not base:pytest.skip('未绑定隔离PG')
    connection=psycopg2.connect(base);connection.autocommit=True
    name='two_phase_'+uuid.uuid4().hex
    with connection.cursor() as c:c.execute('CREATE DATABASE '+name)
    connection.close()
    return base+' dbname='+name


def test_two_sources_same_bytes_cache_alias_and_no_reparse(tmp_path,dsn,monkeypatch):
    import importlib
    producer=importlib.import_module('data_pipeline.bgp.replay.run_from_files')
    manifest=fixture_manifest(tmp_path)
    assert manifest['inputs'][1]['sha256']==manifest['inputs'][2]['sha256']
    assert manifest['inputs'][1]['source_id']!=manifest['inputs'][2]['source_id']
    original=producer.read_source;calls=[]
    def counted(*args,**kwargs):
        calls.append(kwargs['source_id'])
        yield from original(*args,**kwargs)
    monkeypatch.setattr(producer,'read_source',counted)
    report=produce(manifest,dsn,tmp_path/'run',min_free_bytes=0)
    assert calls==[i['source_id'] for i in manifest['inputs']]
    changes=[r for batch in scan(dsn,report['run_id'],'changes') for r in batch.to_pylist()]
    assert len(changes)==3
    assert changes[1]['before_path']==changes[0]['after_path']
    assert changes[1]['before_event']==changes[0]['event_id']
    assert changes[1]['event_id']!=changes[2]['event_id']
    assert changes[1]['after_path']==changes[2]['after_path']
    assert report['active_objects']==1
    assert query(dsn,report['run_id'],'baseline_mappings')[0][3]=='calculation_mapping'
    saved=json.loads((tmp_path/'run'/'execution.json').read_text())
    assert saved['state']=='ready' and report['state']=='complete'
    # 缓存路径变更不改变原始身份；重复别名不增加观察来源。
    alias={**manifest['inputs'][1],'path':str(tmp_path/'another-cache.gz')}
    changed={**manifest,'inputs':[manifest['inputs'][0],alias,*manifest['inputs'][1:]]}
    assert len(normalized_inputs(changed))==3
    assert normalized_inputs(changed)[1]['source_id']==manifest['inputs'][1]['source_id']


def test_snapshot_cannot_impersonate_baseline(tmp_path,dsn):
    manifest=fixture_manifest(tmp_path);manifest['inputs'][0]['role']='snapshot'
    with pytest.raises(ValueError,match='baseline角色'):
        produce(manifest,dsn,tmp_path/'rejected',min_free_bytes=0)
    assert not (tmp_path/'rejected').exists()


def test_receipt_write_failure_never_publishes(tmp_path,dsn,monkeypatch):
    manifest=fixture_manifest(tmp_path)
    def fail(*args):raise OSError('fixture receipt disk error')
    monkeypatch.setattr(Store,'write_receipt',fail)
    with pytest.raises(OSError):produce(manifest,dsn,tmp_path/'failed',min_free_bytes=0)
    failure=json.loads((tmp_path/'failed'/'failure.json').read_text())
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT state FROM domeye.runs WHERE run_id=%s',(failure['run_id'],))
        assert c.fetchone()[0]==failure['state']=='failed'
    with pytest.raises(ValueError):query(dsn,failure['run_id'],'elements')


def test_ambiguous_endpoints_not_silently_chosen(tmp_path,dsn):
    manifest=fixture_manifest(tmp_path)
    # 第二来源保持远端、换本地端点：全输入出现两种真实原字段组合。
    entry=manifest['inputs'][2];path=Path(entry['path'])
    raw=gzip.decompress(path.read_bytes())
    raw=raw[:28]+b'\xc0\0\x02\x03'+raw[32:]
    path.write_bytes(gzip.compress(raw,mtime=0))
    entry['sha256']=hashlib.sha256(path.read_bytes()).hexdigest();entry['size']=path.stat().st_size
    entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
    manifest['update_sources']=[i['source_id'] for i in manifest['inputs'][1:]]
    report=produce(manifest,dsn,tmp_path/'ambiguous',min_free_bytes=0)
    mapping=query(dsn,report['run_id'],'baseline_mappings')[0]
    assert mapping[3]=='ambiguous_local_endpoints'
    changes=[r for b in scan(dsn,report['run_id'],'changes') for r in b.to_pylist()]
    assert changes[1]['before_presence']=='unknown' and changes[2]['before_presence']=='unknown'
    assert len({r['object_key'] for r in changes})==3


def test_confirmed_commit_error_does_not_write_failed(tmp_path,dsn,monkeypatch):
    manifest=fixture_manifest(tmp_path)
    original=Store.finish
    def committed_then_error(self,receipt=None):
        original(self,receipt)
        raise OSError('fixture lost final confirmation')
    monkeypatch.setattr(Store,'finish',committed_then_error)
    result=produce(manifest,dsn,tmp_path/'confirmed',min_free_bytes=0)
    assert result['state']=='complete'
    assert not (tmp_path/'confirmed'/'failure.json').exists()
    assert query(dsn,result['run_id'],'elements')


def test_large_reference_parquet_lossless(tmp_path,dsn):
    value='x'*(16*1024**2+1)
    raw=b'\xef\xbb\xbfh,v\r\n"'+value.encode()+b'",z\r\n \t\n'
    path=tmp_path/'large.csv';path.write_bytes(raw)
    manifest=fixture_manifest(tmp_path)
    manifest['references']=[{'path':str(path),'sha256':hashlib.sha256(raw).hexdigest()}]
    report=produce(manifest,dsn,tmp_path/'large-run',min_free_bytes=0)
    saved=[r for batch in scan(dsn,report['run_id'],'references') for r in batch.to_pylist()]
    saved.sort(key=lambda r:r['row'])
    assert report['state']=='complete' and len(saved)==3
    assert json.loads(saved[1]['raw_row'])==[value,'z']
    for r in saved:
        fragment=raw[r['raw_byte_offset']:r['raw_byte_offset']+r['raw_byte_length']]
        assert hashlib.sha256(fragment).hexdigest()==r['raw_record_sha256']
        assert r['rule']=='reference-rows/v2'
    assert saved[2]['csv_record_kind']=='whitespace_line'


def test_malformed_reference_cannot_complete(tmp_path,dsn):
    path=tmp_path/'bad.csv';path.write_bytes(b'h,v\n"bad')
    manifest=fixture_manifest(tmp_path)
    manifest['references']=[{'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}]
    with pytest.raises(ValueError,match='CSV参考解析失败'):
        produce(manifest,dsn,tmp_path/'bad-run',min_free_bytes=0)
    failure=json.loads((tmp_path/'bad-run'/'failure.json').read_text())
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT state,snapshot FROM domeye.runs WHERE run_id=%s',(failure['run_id'],))
        assert c.fetchone()==('failed',None)
    with pytest.raises(ValueError):query(dsn,failure['run_id'],'references')


def test_shared_catalog_separate_runs_preserve_old_snapshot(tmp_path,dsn):
    first=tmp_path/'first';second=tmp_path/'second';first.mkdir();second.mkdir()
    one=fixture_manifest(first);two=fixture_manifest(second)
    for entry in two['inputs']:
        entry['origin_uri']=entry['origin_uri'].replace('fixture://','fixture://second/')
        entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
    two['baseline_source']=two['inputs'][0]['source_id']
    two['update_sources']=[e['source_id'] for e in two['inputs'][1:]]
    for folder,manifest,value in [(first,one,'first'),(second,two,'second')]:
        p=folder/'ref.csv';p.write_text('name\n'+value+'\n')
        manifest['references']=[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}]
    # 第一run兼容旧默认；第二run显式复用这个实际catalog根。
    a=produce(one,dsn,first/'run',min_free_bytes=0)
    catalog=first/'run'/'parquet'
    receipt=(first/'run'/'execution.json').read_bytes()
    files={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in catalog.rglob('*.parquet')}
    old=query(dsn,a['run_id'],'elements')
    import subprocess
    import sys
    config=second/'config.json';manifest_file=second/'manifest.json'
    config.write_text(json.dumps({'dsn':dsn,'catalog_data_path':str(catalog),'limits':{'min_free_bytes':0}}))
    manifest_file.write_text(json.dumps(two))
    cli=Path(__file__).resolve().parents[3]/'scripts/observations.py'
    completed=subprocess.run([sys.executable,str(cli),'produce','--config',str(config),
        '--manifest',str(manifest_file),'--output',str(second/'run')],check=True,capture_output=True,text=True)
    b=json.loads(completed.stdout)
    assert a['snapshot']<b['snapshot'] and a['run_id']!=b['run_id']
    assert b['catalog_data_path']==str(catalog.resolve())
    assert (first/'run'/'execution.json').read_bytes()==receipt
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==sha for p,sha in files.items())
    assert query(dsn,a['run_id'],'elements')==old
    for result,manifest in [(a,one),(b,two)]:
        refs=[r for batch in scan(dsn,result['run_id'],'references') for r in batch.to_pylist()]
        assert {r['source_id'] for r in refs}=={manifest['references'][0]['sha256']}
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:
            c.execute('SELECT source_id FROM domeye.inputs WHERE run_id=%s',(result['run_id'],))
            assert {r[0] for r in c.fetchall()}=={e['source_id'] for e in manifest['inputs']}
            c.execute('SELECT count(*) FROM domeye.current_routes WHERE run_id=%s',(result['run_id'],))
            assert c.fetchone()[0]==result['active_objects']==1
    with pytest.raises(Exception,match='[Dd]ata.path|DATA_PATH|data path'):
        produce(two,dsn,tmp_path/'mismatch',catalog_data_path=tmp_path/'wrong',min_free_bytes=0)
    with pytest.raises(FileExistsError):
        produce(two,dsn,second/'run',catalog_data_path=catalog,min_free_bytes=0)
    assert query(dsn,a['run_id'],'elements')==old
    assert (first/'run'/'execution.json').read_bytes()==receipt
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT snapshot FROM domeye.runs WHERE run_id=%s',(a['run_id'],))
        assert c.fetchone()[0]==a['snapshot']
        c.execute('SELECT count(*) FROM domeye.runs')
        assert c.fetchone()[0]==2
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==sha for p,sha in files.items())
