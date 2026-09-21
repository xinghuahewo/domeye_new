"""P1 有限增量：自有人工畸形 CP、必需键、目录映射与主错清理。"""
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import uuid

import psycopg2
import pytest

from data_pipeline.bgp.archive import admission as p
from data_pipeline.bgp.archive import validation as v
from data_pipeline.bgp.archive.checkpoint import AttemptStore, produce_checkpointed
from tests.observations.test_observation_checkpoint import fixture


@pytest.fixture
def repair_dsn(tmp_path):
    base=os.environ.get('DOMEYE_M2_TEST_DSN')
    if not base:pytest.skip('需要本任务显式人工 PG')
    name='p1_repair_'+uuid.uuid4().hex
    pg=psycopg2.connect(base);pg.autocommit=True
    try:
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    finally:pg.close()
    dsn=base+' dbname='+name
    (tmp_path/'自有数据库.json').write_text(json.dumps(dict(dsn=dsn)))
    # 故意保留本任务坏登记供有限复核，不把它修成健康；联合交付停止整个自有 PG。
    return dsn


@pytest.mark.parametrize('fault',['null_event','wrong_peer','both'])
def test_actual_bad_cp_rejected_without_repair(tmp_path,repair_dsn,monkeypatch,fault):
    original=AttemptStore.append
    changed=[]
    def append(self,table,row):
        if table=='elements' and row['action']=='rib_snapshot':
            row=dict(row)
            if fault in ('null_event','both'):row['event_id']=None
            if fault in ('wrong_peer','both'):row['peer_index']=9999
            changed.append(row)
        return original(self,table,row)
    monkeypatch.setattr(AttemptStore,'append',append)
    m=fixture(tmp_path/'input')
    seal=produce_checkpointed(m,repair_dsn,tmp_path/'run',batch_rows=1000,min_free_bytes=0)
    assert changed
    root=tmp_path/'run'
    files={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in root.rglob('*') if f.is_file()}
    rt=p.Runtime(repair_dsn,(tmp_path.resolve(),),tmp_path.resolve(),fixture_only=True)
    b=p.inspect_binding(rt,seal['run_id'],seal['snapshot'],[x['source_id'] for x in m['inputs']])
    with pytest.raises(ValueError,match='完整CP/FK') as error:p.admit(rt,b,guard=lambda:None)
    assert files=={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in root.rglob('*') if f.is_file()}
    with psycopg2.connect(repair_dsn) as pg,pg.cursor() as c:
        c.execute("SELECT to_regclass('observation_publication.m2_admissions')")
        if c.fetchone()[0]:
            c.execute('SELECT count(*) FROM observation_publication.m2_admissions');assert c.fetchone()[0]==0
    (tmp_path/'实际拒绝.json').write_text(json.dumps(dict(fault=fault,changed=changed,seal=seal,error=str(error.value),files=files),ensure_ascii=False,default=lambda x:x.hex() if isinstance(x,bytes) else str(x)))

    if fault=='both':
        # 真实绑定目录注入，非原生 schema 演进；原 Parquet 字节不改。
        with psycopg2.connect(repair_dsn) as pg,pg.cursor() as c:
            c.execute("SELECT f.data_file_id FROM ducklake_data_file f JOIN ducklake_table t USING(table_id) JOIN ducklake_schema s USING(schema_id) WHERE s.schema_name=%s AND t.table_name='elements' AND f.begin_snapshot<=%s AND (f.end_snapshot IS NULL OR f.end_snapshot>%s) LIMIT 1",(seal['schema'],seal['snapshot'],seal['snapshot']))
            file_id=c.fetchone()[0]
            c.execute('UPDATE ducklake_data_file SET mapping_id=999 WHERE data_file_id=%s',(file_id,))
        with pytest.raises(ValueError,match='不支持 DuckLake column mapping') as mapping_error:
            p.admit(rt,b,guard=lambda:None)
        (tmp_path/'目录映射拒绝.json').write_text(json.dumps(dict(kind='真实绑定目录注入，非原生结构演进',data_file_id=file_id,error=str(mapping_error.value)),ensure_ascii=False))


def test_current_primary_and_all_cleanup_attempts(monkeypatch,tmp_path):
    # 精确 current 控制流；真实连接同路径探针由联合原 M2 验证另行执行。
    primary=ValueError('元数据主错');secondary=OSError('close 次错');attempts=[]
    class DB:
        def close(self):attempts.append('duckdb');raise secondary
    monkeypatch.setattr(p,'admission_shape',lambda a:None)
    monkeypatch.setattr(p,'untyped',lambda a:{'owner':'m2','run_id':'x','snapshot':0})
    monkeypatch.setattr(p,'_physical',lambda *a:{'root':'fixture'})
    monkeypatch.setattr(p,'_rules',lambda *args:{})
    monkeypatch.setattr(p,'_anchor',lambda *a:None)
    monkeypatch.setattr(p,'_binding',lambda rt,b:b)
    monkeypatch.setattr(p,'Selection',lambda *a:SimpleNamespace(connect=lambda:DB(),checkpoints=[]))
    def fail(*a):raise primary
    monkeypatch.setattr(p,'_ducklake_root',fail)
    with pytest.raises(ValueError) as caught:
        p.verify_current(p.Runtime('fixture',(tmp_path,),tmp_path,fixture_only=True),dict(owner_binding='',physical={'root':'fixture'},validator={},dependencies=[]),guard=lambda:None)
    assert caught.value is primary and caught.value.cleanup_errors==(secondary,) and attempts==['duckdb']
    assert any(frame.name=='fail' for frame in __import__('traceback').extract_tb(primary.__traceback__))
    attempts.clear()
    def close(name):
        def run():attempts.append(name);raise OSError(name)
        return run
    with pytest.raises(ValueError) as caught:
        with v._closing([close('one'),close('two')]):raise primary
    assert caught.value is primary and attempts==['two','one']
    assert [str(e) for e in primary.cleanup_errors]==['close 次错','two','one']


def test_all_eight_tables_required_keys_and_scientific_nulls():
    import duckdb
    from data_pipeline.bgp.archive.checkpoint import OBS_TABLES
    db=duckdb.connect()
    rows={name:[] for name in OBS_TABLES}
    rows['messages']=[dict(message_id='s:'+str(i),source_id='s',content_sha256='h',record=i,kind=k,interpretation='{"status":"decoded"}') for i,k in enumerate(('peer_index_table','rib','update'))]
    rows['elements']=[dict(event_id='s:'+str(i)+':0',message_id='s:'+str(i),ordinal=0,path_key='p',action=action,peer_table_record=table,peer_index=index,peer_ip=ip,peer_asn=asn,bgp_id=bgp,bgp_id_present=present) for i,action,table,index,ip,asn,bgp,present in [(1,'rib_snapshot',0,0,'ip',1,'bgp',True),(2,'withdraw',None,None,None,None,None,False)]]
    rows['paths']=[dict(path_key='p',asn_width=4,attributes_raw=b'',attributed_origin_asn=None)]
    rows['peers']=[dict(source_id='s',table_record=0,index=0,ip='ip',asn=1,bgp_id='bgp',bgp_id_present=True)]
    rows['eor']=[dict(message_id='s:2',afi=1,safi=1)]
    rows['associations']=[dict(message_id='s:2',peer_ref=None,reference_message='s:0')]
    rows['quality']=[dict(source_id='s',message_id=None,code='diagnostic',detail=None)]
    rows['references']=[dict(source_id='s',row=0,location='json',raw_row='null')]
    for table,columns in OBS_TABLES.items():
        db.execute('CREATE TABLE "'+table+'" ('+','.join('"'+n+'" '+t for n,t in columns)+')')
        for row in rows[table]:db.execute('INSERT INTO "'+table+'" VALUES ('+','.join('?' for _ in columns)+')',[row.get(n) for n,_ in columns])
    selection=SimpleNamespace(table=lambda name,source_id=None:'"'+name+'"')
    runtime=SimpleNamespace(event=lambda *a,**k:None)
    cp=dict(source_id='s',source_sha='h',counts=dict(decoded=3,rejected=0,unsupported=0))
    check=lambda:v._fk(runtime,db,selection,cp,lambda:None)
    check()  # Unknown origin、UPDATE 无 RIB 索引、未绑定 association 与 source quality NULL 均合法。
    required={'messages':['message_id','source_id','content_sha256','record','kind'],
        'elements':['event_id','message_id','ordinal','path_key','action'],
        'paths':['path_key','asn_width','attributes_raw'],'peers':['source_id','table_record','index'],
        'eor':['message_id','afi','safi'],'associations':['message_id'],
        'quality':['source_id','code'],'references':['source_id','row','location']}
    try:
        for table,keys in required.items():
            for key in keys:
                db.execute('BEGIN')
                try:
                    db.execute('UPDATE "'+table+'" SET "'+key+'"=NULL WHERE rowid=0')
                    with pytest.raises(ValueError,match='完整CP/FK'):check()
                finally:db.execute('ROLLBACK')
        for key,value in [('peer_index','99'),('peer_table_record','99'),('peer_ip',"'wrong'"),('peer_asn','999'),('bgp_id',"'wrong'"),('bgp_id_present','FALSE')]:
            db.execute('BEGIN')
            try:
                db.execute('UPDATE elements SET '+key+'='+value+' WHERE rowid=0')
                with pytest.raises(ValueError,match='完整CP/FK'):check()
            finally:db.execute('ROLLBACK')
        check()
    finally:db.close()
