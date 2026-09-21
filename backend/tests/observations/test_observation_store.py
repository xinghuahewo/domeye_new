"""真实隔离PostgreSQL与DuckLake集成；无DSN时明确跳过，不冒称通过。"""
import os
import hashlib
import gzip
import uuid
from pathlib import Path
import pytest
import psycopg2
from data_pipeline.bgp.input.mrt_reader import read_source, source_identity
from data_pipeline.bgp.replay.route_replay import Replay, ReplayPlan
from data_pipeline.bgp.archive.store import Store, query
from tests.observations.test_observation_mrt import update


def test_real_lake_pg(tmp_path):
    dsn=os.environ.get('DOMEYE_STAGE1_TEST_DSN')
    if not dsn:pytest.skip('未绑定本任务隔离PG')
    # 每个测试独立数据库；不连接其他业务表。
    database='test_'+uuid.uuid4().hex
    admin=psycopg2.connect(dsn);admin.autocommit=True
    with admin.cursor() as c:c.execute('CREATE DATABASE '+database)
    test_dsn=dsn+' dbname='+database
    source=tmp_path/'input.gz';source.write_bytes(gzip.compress(update()+update(),mtime=0))
    sha=hashlib.sha256(source.read_bytes()).hexdigest()
    store=Store(test_dsn,tmp_path/'run',batch_rows=50000)
    run=store.run_id
    replay=Replay(ReplayPlan('rrc25','none',(sha,)))
    store.bind({'inputs':[{'sha256':sha}]},replay.plan.version)
    with store.pg,store.pg.cursor() as c:
        c.execute('INSERT INTO domeye.inputs VALUES (%s,%s,%s,%s,%s)',(run,sha,str(source),source.stat().st_size,'validated'))
    try:
        with pytest.raises(ValueError):query(test_dsn,run,'elements')
        for m in read_source(source,sha,source_id=sha):
            store.message(m)
            for change in replay.consume(m):store.change(change)
        store.source_complete(sha,2,2)
        store.save_current(replay)
        snapshot=store.finish()
        assert snapshot>0
        assert len(query(test_dsn,run,'messages'))==2
        assert len(query(test_dsn,run,'elements'))==2
        assert len(query(test_dsn,run,'paths'))==1
        assert len(query(test_dsn,run,'changes'))==2
        from data_pipeline.bgp.archive.store import scan_observations
        batches=list(scan_observations(test_dsn,run,batch_size=1))
        assert len(batches)==2
        assert batches[0].to_pylist()[0]['as_path_text']=='64497 64496'
        assert list((tmp_path/'run'/'parquet').rglob('*.parquet'))
        with store.pg.cursor() as c:
            c.execute('SELECT count(*) FROM domeye.current_routes WHERE run_id=%s',(run,))
            assert c.fetchone()[0]==1
        store.fail('fixture失败')
        assert len(query(test_dsn,run,'messages'))==2  # 完成后不可改写状态
    finally:
        store.close();admin.close()


def test_public_produce_and_corrupt_failure(tmp_path):
    from data_pipeline.bgp.replay.run_from_files import produce
    from tests.observations.test_observation_mrt import mrt, attr
    import struct
    dsn=os.environ.get('DOMEYE_STAGE1_TEST_DSN')
    if not dsn:pytest.skip('未绑定本任务隔离PG')
    admin=psycopg2.connect(dsn);admin.autocommit=True
    database='test_'+uuid.uuid4().hex
    with admin.cursor() as c:c.execute('CREATE DATABASE '+database)
    test_dsn=dsn+' dbname='+database
    table=b'\0\0\0\x19\0\x05rrc25\0\x01\x02'+b'\xc0\0\x02\x01'*2+struct.pack('!I',64497)
    attrs=attr()
    rib=struct.pack('!I',0)+b'\x18\xc0\0\x02'+struct.pack('!HHIH',1,0,90,len(attrs))+attrs
    entries=[]
    for name,data,role in [('rib',mrt(table,1,13)+mrt(rib,2,13),'baseline'),
        ('a',update(attrs=attr(64498)),'update'),('b',update(ann=b'',withdrawn=b'\x18\xc0\0\x02',attrs=b''),'update')]:
        p=tmp_path/(name+'.gz');p.write_bytes(gzip.compress(data,mtime=0))
        sha=hashlib.sha256(p.read_bytes()).hexdigest()
        uri='fixture://rrc25/'+name
        entries.append({'path':str(p),'sha256':sha,'source_id':source_identity('rrc25',uri,sha),'origin_uri':uri,'size':p.stat().st_size,'role':role})
    manifest={'schema_version':'observation-run/v1','window_start':'1970-01-01T00:00:00Z','window_end_exclusive':'1970-01-02T00:00:00Z','collector':'rrc25','inputs':entries,'baseline_source':entries[0]['source_id'],
      'update_sources':[e['source_id'] for e in entries[1:]],'baseline_endpoints':[['192.0.2.1',64497,'192.0.2.2',12654,0]]}
    report=produce(manifest,test_dsn,tmp_path/'complete',min_free_bytes=0)
    assert report['active_objects']==1
    assert len(query(test_dsn,report['run_id'],'changes'))==3
    # 全新catalog及坏gzip；不能复用或覆盖已发布运行。
    failed_db='test_'+uuid.uuid4().hex
    with admin.cursor() as c:c.execute('CREATE DATABASE '+failed_db)
    bad=Path(entries[-1]['path']);bad.write_bytes(bad.read_bytes()[:-3])
    entries[-1].update(size=bad.stat().st_size,sha256=hashlib.sha256(bad.read_bytes()).hexdigest())
    entries[-1]['source_id']=source_identity('rrc25',entries[-1]['origin_uri'],entries[-1]['sha256'])
    manifest['update_sources']=[e['source_id'] for e in entries[1:]]
    with pytest.raises(EOFError):produce(manifest,dsn+' dbname='+failed_db,tmp_path/'failed',min_free_bytes=0)
    import json
    failure=json.loads((tmp_path/'failed'/'failure.json').read_text())
    with pytest.raises(ValueError):query(dsn+' dbname='+failed_db,failure['run_id'],'elements')
    admin.close()


def test_gate_missing_input_or_export(tmp_path):
    dsn=os.environ.get('DOMEYE_STAGE1_TEST_DSN')
    if not dsn:pytest.skip('未绑定本任务隔离PG')
    database='test_'+uuid.uuid4().hex
    admin=psycopg2.connect(dsn);admin.autocommit=True
    with admin.cursor() as c:c.execute('CREATE DATABASE '+database)
    store=Store(dsn+' dbname='+database,tmp_path/'gate')
    try:
        store.bind({'inputs':[{'sha256':'missing'}]},'fixture')
        with pytest.raises(ValueError,match='未导出'):store.finish()
        store.save_current(Replay(ReplayPlan('rrc25','none',())))
        with pytest.raises(ValueError,match='固定输入未全部完成'):store.finish()
        store.fail('fixture门禁')
        with pytest.raises(ValueError):query(dsn+' dbname='+database,store.run_id,'messages')
    finally:store.close();admin.close()


def test_cli_readonly_gate_rejects_writable_fixture(tmp_path):
    import json
    import subprocess
    import sys
    root=Path(__file__).resolve().parents[3]
    raw=tmp_path/'fixture.gz';raw.write_bytes(b'fixture')
    manifest=tmp_path/'manifest.json';manifest.write_text(json.dumps({'inputs':[{'path':str(raw)}]}))
    config=tmp_path/'config.json';config.write_text(json.dumps({'dsn':'unused','require_readonly_inputs':True,'preflight_receipt':str(tmp_path/'receipt.json')}))
    result=subprocess.run([sys.executable,str(root/'scripts/observations.py'),'produce','--config',str(config),'--manifest',str(manifest),'--output',str(tmp_path/'output')],capture_output=True,text=True)
    assert result.returncode!=0 and '只读挂载门禁拒绝' in result.stderr
    assert not (tmp_path/'output').exists() and not (tmp_path/'receipt.json').exists()
