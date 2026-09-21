"""只在显式Resource隔离PG运行手工二进制fixture；无DSN则跳过。"""
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
import struct
import uuid

import psycopg2
import pytest

from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.analysis.resources import RibContext
from functools import partial
from data_pipeline.analysis.resources.produce import produce_resources as _produce_resources
produce_resources=partial(_produce_resources,fixture_only=True)
from data_pipeline.analysis.resources.store import ResourceStore, scan_resource as _scan_resource
scan_resource=partial(_scan_resource,allow_fixture=True)
from tests.observations.test_observation_mrt import mrt


def fixture_inputs(root, *, count=2, ribs=3, distinct_ribs=False):
    entries=[]
    for rib_index in range(ribs):
        epoch=100+rib_index*28800
        table=b'\0\0\0\x19\0\x05rrc25\0\x01\x02'+b'\xc0\0\x02\x01'*2+struct.pack('!I',64497)
        raw=bytearray(mrt(table,1,13,epoch))
        for index in range(count):
            # 两条AS_PATH相同但MED不同；默认count=2用于严格反例。
            tail=100+index//2+(rib_index*count//2 if distinct_ribs else 0)
            attrs=b'\x40\x02\x0a\x02\x02'+struct.pack('!II',9808,tail)+b'\x80\x04\x04'+struct.pack('!I',index)
            prefix=(0x0a0000+index+(rib_index*count if distinct_ribs else 0)).to_bytes(3,'big')
            body=struct.pack('!I',index)+b'\x18'+prefix+struct.pack('!HHIH',1,0,epoch-1,len(attrs))+attrs
            raw.extend(mrt(body,2,13,epoch))
        path=root/f'rib-{rib_index}.gz'
        path.write_bytes(gzip.compress(bytes(raw),mtime=0))
        sha=hashlib.sha256(path.read_bytes()).hexdigest()
        uri=f'fixture://rrc25/rib-{rib_index}'
        entries.append({'path':str(path),'sha256':sha,'origin_uri':uri,'source_id':source_identity('rrc25',uri,sha),
            'size':path.stat().st_size,'role':'baseline' if rib_index==0 else 'snapshot'})
    refs=[]
    csv=root/'as.csv';csv.write_text('asn,as_name,global_rank,unused\n64497,first,12.5,retained\n64497,second,99,also retained\n')
    country=root/'country.json';country.write_text(json.dumps({str(i):{'country_cn':'甲','unused':['keep']} for i in (9808,*range(100,100+count//2*(ribs if distinct_ribs else 1)))}))
    for path in (csv,country):refs.append({'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest={'schema_version':'observation-run/v1','collector':'rrc25',
        'window_start':'1970-01-01T00:00:00Z','window_end_exclusive':'1970-02-01T00:00:00Z',
        'baseline_source':entries[0]['source_id'],'update_sources':[],'inputs':entries,'references':refs}
    contexts=[RibContext(e['source_id'],'rrc25',datetime.fromtimestamp(100+i*28800,timezone.utc),
        refs[0]['sha256']+'+'+refs[1]['sha256'],'MRT-first-element-epoch',f'fixture-rib-{i}',
        'fixture_filename_independent','mrt-segments-ecbc03f',content_sha256=e['sha256'],origin_uri=e['origin_uri']) for i,e in enumerate(entries)]
    return manifest,contexts


def test_real_resource_lake_pg_and_gates(tmp_path, monkeypatch):
    dsn=os.environ.get('DOMEYE_RESOURCE_TEST_DSN')
    if not dsn:pytest.skip('未绑定Resource独立PG')
    admin=psycopg2.connect(dsn);admin.autocommit=True
    database='resource_'+uuid.uuid4().hex
    with admin.cursor() as c:c.execute('CREATE DATABASE '+database)
    dsn=dsn+' dbname='+database
    manifest,contexts=fixture_inputs(tmp_path)
    upstream=produce(manifest,dsn,tmp_path/'observation-run',min_free_bytes=0)
    report=produce_resources(dsn,upstream['run_id'],contexts,tmp_path/'resource-run',
        csv_source=manifest['references'][0]['sha256'],country_source=manifest['references'][1]['sha256'],
        topology_enabled=True,min_free_bytes=0)
    def rows(table):
        return [row for batch in scan_resource(dsn,report['run_id'],table,batch_size=2) for row in batch.to_pylist()]
    assert len(rows('sources'))==3
    assert {r['source_id'] for r in rows('sources')}=={c.source_id for c in contexts}
    assert {r['content_sha256'] for r in rows('sources')}=={c.content_sha256 for c in contexts}
    assert all(c.source_id!=c.content_sha256 for c in contexts)
    assert json.loads((tmp_path/'resource-run/execution.json').read_text())['state']=='ready'
    with pytest.raises(ValueError,match='fixture读取'):
        list(_scan_resource(dsn,report['run_id'],'metrics'))
    metrics=rows('metrics')
    globals=[r for r in metrics if r['bucket']=='global']
    assert len(globals)==3
    assert all(r['path_count']==1 and r['ipv4_prefix_count']==2 for r in globals)
    assert len(rows('rendered_paths'))==1
    assert len(rows('decision_refs'))==6
    assert len(rows('topology_edges'))==3
    assert len(rows('normal_bands'))==36
    assert all(r['as_name']=='first' and r['as_rank']==12 for r in metrics if r['dimension']=='peer_asn')
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT rib_count,initial_state_basis FROM domeye.resource_work_meta WHERE run_id=%s',(report['run_id'],))
        assert c.fetchone()==(3,'cold_start')
        c.execute('SELECT count(*) FROM domeye.resource_work_metrics WHERE run_id=%s',(report['run_id'],))
        assert c.fetchone()[0]==9
    # 写完成回执失败，PG必须保持failed，不能提前成为complete。
    from pathlib import Path
    original_open=Path.open
    def fail_receipt(path, *args, **kwargs):
        if path.name=='execution.json' and path.parent.name=='receipt-fail':
            raise OSError('fixture receipt write failure')
        return original_open(path,*args,**kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Path,'open',fail_receipt)
        with pytest.raises(OSError):
            produce_resources(dsn,upstream['run_id'],contexts,tmp_path/'receipt-fail',
                csv_source=manifest['references'][0]['sha256'],country_source=manifest['references'][1]['sha256'],
                topology_enabled=True,min_free_bytes=0)
    failure=json.loads((tmp_path/'receipt-fail/failure.json').read_text())
    with pytest.raises(ValueError):list(scan_resource(dsn,failure['run_id'],'metrics'))
    original_finish=ResourceStore.finish
    def committed_then_error(store, *args, **kwargs):
        original_finish(store,*args,**kwargs)
        raise OSError('fixture lost commit confirmation')
    with monkeypatch.context() as patch:
        patch.setattr(ResourceStore,'finish',committed_then_error)
        confirmed=produce_resources(dsn,upstream['run_id'],contexts,tmp_path/'confirmed',
            csv_source=manifest['references'][0]['sha256'],country_source=manifest['references'][1]['sha256'],
            topology_enabled=True,min_free_bytes=0)
        assert confirmed['state']=='complete'
        assert not (tmp_path/'confirmed/failure.json').exists()
    candidate=ResourceStore(dsn,tmp_path/'candidate',upstream['run_id'],[contexts[0].source_id],fixture_only=True)
    try:
        with pytest.raises(ValueError):list(scan_resource(dsn,candidate.run_id,'metrics'))
        with pytest.raises(ValueError):candidate.finish()
        candidate.fail('fixture: missing result')
        with pytest.raises(ValueError):candidate.state(None)
        with pytest.raises(ValueError):list(scan_resource(dsn,candidate.run_id,'metrics'))
        assert len(rows('metrics'))==9
    finally:
        candidate.close();admin.close()


def test_same_content_origins_cache_alias_and_no_resource_reparse(tmp_path, monkeypatch):
    from dataclasses import replace
    import importlib
    dsn=os.environ.get('DOMEYE_RESOURCE_TEST_DSN')
    if not dsn:pytest.skip('未绑定Resource独立PG')
    admin=psycopg2.connect(dsn);admin.autocommit=True
    database='identity_'+uuid.uuid4().hex
    with admin.cursor() as c:c.execute('CREATE DATABASE '+database)
    admin.close()
    dsn=dsn+' dbname='+database
    manifest,contexts=fixture_inputs(tmp_path,ribs=1)
    first=manifest['inputs'][0]
    uri='fixture://rrc25/another-original'
    second={**first,'role':'snapshot','origin_uri':uri,'source_id':source_identity('rrc25',uri,first['sha256'])}
    # 不存在的缓存别名放在实际原件之后；应按同source归一，不能再次访问路径。
    alias={**first,'path':str(tmp_path/'unused-cache-alias.gz')}
    manifest['inputs']=[first,alias,second]
    producer=importlib.import_module('data_pipeline.bgp.replay.run_from_files')
    original_read=producer.read_source
    calls=[]
    def counted(*args,**kwargs):
        calls.append(kwargs['source_id'])
        yield from original_read(*args,**kwargs)
    monkeypatch.setattr(producer,'read_source',counted)
    upstream=produce(manifest,dsn,tmp_path/'upstream',min_free_bytes=0)
    assert calls==[first['source_id'],second['source_id']]
    other=replace(contexts[0],source_id=second['source_id'],origin_uri=uri)
    outputs=[]
    for index,context in enumerate((contexts[0],other)):
        result=produce_resources(dsn,upstream['run_id'],[context],tmp_path/f'resource-{index}',
            csv_source=manifest['references'][0]['sha256'],country_source=manifest['references'][1]['sha256'],min_free_bytes=0)
        rows=[r for batch in scan_resource(dsn,result['run_id'],'sources') for r in batch.to_pylist()]
        assert rows[0]['source_id']==context.source_id
        assert rows[0]['content_sha256']==first['sha256']
        assert rows[0]['origin_uri']==context.origin_uri
        outputs.append(result['dataset_id'])
    assert outputs[0]!=outputs[1]
    assert calls==[first['source_id'],second['source_id']]  # Resource不再读原件。
    wrong=replace(other,source_id=first['sha256'])
    with pytest.raises(ValueError,match='非RIB或未知来源'):
        produce_resources(dsn,upstream['run_id'],[wrong],tmp_path/'sha-as-source',
            csv_source=manifest['references'][0]['sha256'],country_source=manifest['references'][1]['sha256'],min_free_bytes=0)
    wrong=replace(other,content_sha256='0'*64)
    with pytest.raises(ValueError,match='内容版本'):
        produce_resources(dsn,upstream['run_id'],[wrong],tmp_path/'wrong-content',
            csv_source=manifest['references'][0]['sha256'],country_source=manifest['references'][1]['sha256'],min_free_bytes=0)


def test_dependency_change_during_run_is_not_published(tmp_path, monkeypatch):
    import importlib
    from tests.resources.test_resource_identity import copied_identity_root
    from data_pipeline.analysis.resources.identity import code_identity
    dsn=os.environ.get('DOMEYE_RESOURCE_TEST_DSN')
    if not dsn:pytest.skip('未绑定Resource独立PG')
    admin=psycopg2.connect(dsn);admin.autocommit=True
    database='dependency_'+uuid.uuid4().hex
    with admin.cursor() as c:c.execute('CREATE DATABASE '+database)
    admin.close()
    dsn=dsn+' dbname='+database
    manifest,contexts=fixture_inputs(tmp_path,ribs=1)
    upstream=produce(manifest,dsn,tmp_path/'upstream',min_free_bytes=0)
    root=copied_identity_root(tmp_path)
    resource_store=importlib.import_module('data_pipeline.analysis.resources.store')
    resource_producer=importlib.import_module('data_pipeline.analysis.resources.produce')
    monkeypatch.setattr(resource_store,'execution_identity',lambda **kwargs:{**code_identity(root),'execution_mode':'synthetic-fixture-api'})
    monkeypatch.setattr(resource_producer,'execution_identity',lambda **kwargs:{**code_identity(root),'execution_mode':'synthetic-fixture-api'})
    original_state=ResourceStore.state
    def change_dependency(store,state):
        original_state(store,state)
        helper=root/'backend/utils/prefix_quantity.py'
        helper.write_text(helper.read_text().replace('return len(unique_c_segments_ints)', 'return 2 * len(unique_c_segments_ints)'))
    monkeypatch.setattr(ResourceStore,'state',change_dependency)
    with pytest.raises(ValueError,match='计算依赖'):
        produce_resources(dsn,upstream['run_id'],contexts,tmp_path/'changed-dependency',
            csv_source=manifest['references'][0]['sha256'],country_source=manifest['references'][1]['sha256'],min_free_bytes=0)
    failure=json.loads((tmp_path/'changed-dependency/failure.json').read_text())
    assert failure['state']=='failed'
    with pytest.raises(ValueError):list(scan_resource(dsn,failure['run_id'],'metrics'))
    assert not (tmp_path/'changed-dependency/execution.json').exists()


def test_csv_lexical_projection_through_saved_reference_batches(tmp_path):
    from dataclasses import replace
    from pathlib import Path
    from data_pipeline.bgp.archive.store import scan
    dsn=os.environ.get('DOMEYE_RESOURCE_TEST_DSN')
    if not dsn:pytest.skip('未绑定Resource独立PG')
    admin=psycopg2.connect(dsn);admin.autocommit=True
    database='csv_'+uuid.uuid4().hex
    with admin.cursor() as c:c.execute('CREATE DATABASE '+database)
    admin.close()
    dsn=dsn+' dbname='+database
    manifest,contexts=fixture_inputs(tmp_path,ribs=1)
    csv=Path(manifest['references'][0]['path'])
    csv.write_text('asn,as_name,global_rank\n\n   \n64497,first,12.5\n"   "\n,,\n64497,second,99\n')
    sha=hashlib.sha256(csv.read_bytes()).hexdigest()
    manifest['references'][0]['sha256']=sha
    country=manifest['references'][1]['sha256']
    contexts=[replace(c,reference_id=sha+'+'+country) for c in contexts]
    upstream=produce(manifest,dsn,tmp_path/'upstream',min_free_bytes=0)
    saved=[r for b in scan(dsn,upstream['run_id'],'references') for r in b.to_pylist() if r['source_id']==sha]
    saved.sort(key=lambda r:r['row'])
    assert [r['csv_record_kind'] for r in saved]==['record','blank_line','whitespace_line','record','record','record','record']
    assert json.loads(saved[2]['raw_row'])==json.loads(saved[4]['raw_row'])==['   ']
    report=produce_resources(dsn,upstream['run_id'],contexts,tmp_path/'resource',csv_source=sha,country_source=country,min_free_bytes=0)
    metrics=[r for b in scan_resource(dsn,report['run_id'],'metrics') for r in b.to_pylist()]
    vp=next(r for r in metrics if r['dimension']=='peer_asn')
    assert (vp['as_name'],vp['as_rank'],vp['reference_row_ref'])==('first',12,f'{sha}:csv:3')
    receipt=json.loads((tmp_path/'resource/execution.json').read_text())
    assert receipt['code_identity']['files']['backend/utils/prefix_quantity.py']['group']=='quantity_calculation'
    assert (tmp_path/'resource/code-identity.json').exists()


@pytest.mark.parametrize('changed_helper',[False,True])
def test_fresh_frozen_entry_real_pg(tmp_path,changed_helper):
    """独立解释器消费同一合成观察；完成身份绑定实际只读快照。"""
    from dataclasses import asdict
    import subprocess
    import sys
    from data_pipeline.analysis.resources.identity import ROOT, code_identity
    dsn=os.environ.get('DOMEYE_RESOURCE_TEST_DSN')
    if not dsn:pytest.skip('未绑定Resource独立PG')
    admin=psycopg2.connect(dsn);admin.autocommit=True
    database='resource_'+uuid.uuid4().hex
    with admin.cursor() as c:c.execute('CREATE DATABASE '+database)
    admin.close()
    dsn=dsn+' dbname='+database
    manifest,contexts=fixture_inputs(tmp_path,ribs=1)
    upstream=produce(manifest,dsn,tmp_path/'observations',min_free_bytes=0)
    from tests.resources.test_resource_bindings import bind_request, json_request
    request,_=bind_request(dsn,upstream,contexts,manifest,tmp_path,fresh=True)
    request=json_request(request)
    request['output']=str(tmp_path/'frozen-resource')
    launch_root=ROOT
    if changed_helper:
        from tests.resources.test_resource_identity import copied_identity_root
        launch_root=copied_identity_root(tmp_path)
    path=tmp_path/'request.json';path.write_text(json.dumps(request))
    if changed_helper:
        # 同一个真实warm进程先导入旧helper，再改源，再调用正式入口启动新进程。
        warm='''
import sys,subprocess
from pathlib import Path
sys.path.insert(0,sys.argv[1]+'/backend')
from data_pipeline.analysis.resources.compute import calculate_c_segments_count
from data_pipeline.analysis.resources.produce import produce_resources
assert calculate_c_segments_count({'192.0.2.0/24'})==1
helper=Path(sys.argv[1])/'backend/utils/prefix_quantity.py'
helper.write_text(helper.read_text().replace('return len(unique_c_segments_ints)','return 2 * len(unique_c_segments_ints)'))
assert calculate_c_segments_count({'192.0.2.0/24'})==1
try:
    produce_resources('invalid','unused',[],'unused',csv_source='unused',country_source='unused')
except ValueError as error:
    assert 'resource-frozen-run.py' in str(error)
else:
    raise AssertionError('旧进程被错误准入')
subprocess.run([sys.executable,sys.argv[1]+'/scripts/pipeline/resource-frozen-run.py',sys.argv[2]],check=True)
'''
        subprocess.run([sys.executable,'-B','-c',warm,str(launch_root),str(path)],check=True,capture_output=True,text=True)
    else:
        from tests.resources.test_resource_identity import copied_identity_root
        archive=copied_identity_root(tmp_path)
        directories=[archive,*[p for p in archive.rglob('*') if p.is_dir()]]
        for file in archive.rglob('*'):
            if file.is_file():file.chmod(0o444)
        for directory in directories:directory.chmod(0o555)
        try:
            subprocess.run([sys.executable,str(launch_root/'scripts/pipeline/resource-frozen-run.py'),str(path),
                '--snapshot',str(archive)],check=True,capture_output=True,text=True)
        finally:
            for directory in directories:directory.chmod(0o755)
    receipt=json.loads((tmp_path/'frozen-resource/execution.json').read_text())
    assert receipt['code_identity']['execution_mode']=='frozen-fresh-process'
    assert receipt['code_identity']['execution_binding']['pid']!=os.getpid()
    assert receipt['project_module_sources']['utils.prefix_quantity']=='backend/utils/prefix_quantity.py'
    assert receipt['code_identity']['files']==code_identity(launch_root)['files']
    rows=[r for batch in _scan_resource(dsn,receipt['run_id'],'metrics') for r in batch.to_pylist()]
    global_row=next(r for r in rows if r['bucket']=='global')
    assert global_row['ipv4_prefix_count']==2
    assert global_row['ipv4_address_count']==(1024 if changed_helper else 512)
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT state,code_digest FROM domeye.resource_runs WHERE run_id=%s',(receipt['run_id'],))
        assert c.fetchone()==('complete',receipt['code_digest'])
