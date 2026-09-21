"""仅复用 8233 既有人工 M2，不解析或重产任何原始 CP。"""
import json
from pathlib import Path
import hashlib
import pytest
import psycopg2
from data_pipeline.bgp.archive import admission as p
from data_pipeline.bgp.archive import validation as v
from data_pipeline.bgp.archive.value_codec import typed, untyped
from data_pipeline.bgp.archive.selection import Selection
import os
if os.environ.get("DOMEYE_M2_P1_INTEGRATION") != "8233":
    pytest.skip("仅显式授权的 8233 既有人工制品",allow_module_level=True)
from tests.observations.m2_p1_integration_support import *


def test_01_actual_admissions_and_full_eight_tables():
    before=files(ROOT);save('原目录SHA前.json',before)
    metadata=original_metadata();save('原登记前.json',metadata)
    pg=psycopg2.connect(DSN)
    try:
        with pg.cursor() as c:
            c.execute("SELECT to_regclass('observation_publication.m2_admissions')");assert c.fetchone()[0] is None
    finally:pg.close()
    binding=p.inspect_binding(RT,CONFIG['input_run'],CONFIG['input_snapshot'],CONFIG['ordered_sources'])
    save('原绑定.json',binding)
    with measure('首次M2准入'):a=p.admit(RT,binding,guard=guard)
    save('M2_Admission.json',a);RT.dependency_admissions=(a,)
    rb=p.reference_binding(RT,a,binding['reference_sources'][0]['source_id'])
    with measure('首次reference准入'):ref=p.admit(RT,rb,guard=guard)
    save('reference_Admission.json',ref)
    assert binding['selected_sources'][0]['upstream_rank']==0
    assert binding['reference_sources'][0]['checkpoint_ordinal']==0
    assert binding['seal']['checkpoints'][1]['ordinal']==1
    assert rb['selected_sources']==[] and ref['dependencies']==[a['admission_id']]
    for owner,b in [(a,binding),(ref,rb)]:
        with measure(owner['owner']+'复用'):
            assert p.admit(RT,b,guard=guard)==owner
            assert not any(e['kind'] in ('entity_hash','admit_body_batch','parquet_row_group') for e in EVENTS)
        with measure(owner['owner']+'current'):
            p.verify_current(RT,owner,guard=guard)
            assert not any(e['kind'] in ('entity_hash','admit_body_batch','parquet_row_group') for e in EVENTS)
    selection=Selection(DSN,CONFIG['input_run'],CONFIG['input_snapshot']);db=selection.connect()
    comparisons={}
    try:
        for owner in (a,ref):
            for table in (tuple(p.OBS_TABLES) if owner is a else ('references',)):
                req=request(owner,table);sources=untyped(req['scope_typed'])['source_ids'];expected=[]
                for source in sources:
                    cur=db.execute('SELECT * FROM '+selection.table(table,source)+' ORDER BY '+v.ORDERS[table])
                    assert [(c[0],str(c[1])) for c in cur.description]==p.OBS_TABLES[table]
                    expected.extend(dict(zip([n for n,t in p.OBS_TABLES[table]],r)) for r in cur.fetchall())
                pair=[]
                for batch in (1,17):
                    label=owner['owner']+'_'+table+'_'+str(batch)
                    with measure(label) as cost:
                        rows,receipt,first=read(owner,request(owner,table,batch));cost['first_batch']=first
                    assert typed(rows)==typed(expected)
                    save(label+'.receipt.json',receipt);(OUT/(label+'.typed.json')).write_text(typed(rows));pair.append(receipt['typed_digest'])
                assert pair[0]==pair[1]
                comparisons[owner['owner']+'.'+table]=dict(rows=len(expected),digest=pair[0])
    finally:db.close()
    for sources in (CONFIG['ordered_sources'][-1:],[]):
        with measure('子选择' if sources else '空选择') as cost:
            rows,receipt,first=read(a,request(a,'messages',1,sources));cost['first_batch']=first
        assert {r['source_id'] for r in rows}==set(sources)
        assert untyped(receipt['coverage_ref'])['business_absence']=='Unknown'
        save(('子选择' if sources else '空选择')+'.receipt.json',receipt)
    save('全文对照.json',comparisons)
    assert files(ROOT)==before and original_metadata()==metadata
    save('原目录SHA后.json',files(ROOT));save('原登记后.json',original_metadata())


def test_02_lock_current_early_close_and_primary(monkeypatch):
    a,ref=load();target=p._own_target(a)
    def update():
        pg=psycopg2.connect(DSN)
        try:
            with pg.cursor() as c:
                c.execute("SET LOCAL lock_timeout='100ms'")
                c.execute('UPDATE observation_publication.m2_admissions SET state=state WHERE record_key=%s',(target['key'],))
        finally:pg.rollback();pg.close()
    with measure('单锁跨reference早停'):
        with p.hold_lock(RT,a,target,guard=guard):
            assert sum('FOR SHARE' in e.get('sql','') for e in EVENTS)==1
            with pytest.raises(psycopg2.errors.LockNotAvailable):update()
            p.verify_current(RT,ref,guard=guard)
            with p.open_reader(RT,ref,request(ref,'references',1),guard=guard) as session:next(session)
            assert session.receipt is None
            marker=len(EVENTS)
        assert len(EVENTS)==marker
        update()
    assert not list(OUT.glob('m2-p1-*'))
    original=Selection.connect;primary=ValueError('实际 current 元数据主错');secondary=OSError('实际连接释放后 close 次错');closed=[]
    class Proxy:
        def __init__(self,db):self.db=db
        def __getattr__(self,n):return getattr(self.db,n)
        def close(self):self.db.close();closed.append(True);raise secondary
    monkeypatch.setattr(Selection,'connect',lambda s:Proxy(original(s)))
    def fail(*args):raise primary
    monkeypatch.setattr(p,'_ducklake_root',fail)
    with pytest.raises(ValueError) as caught:p.verify_current(RT,a,guard=guard)
    assert caught.value is primary and primary.cleanup_errors==(secondary,) and closed==[True]
    assert any(x.name=='fail' for x in __import__('traceback').extract_tb(primary.__traceback__))
    save('清理与跨界.json',dict(primary=str(primary),secondary=[str(e) for e in primary.cleanup_errors],actual_db_closed=True,early_receipt=session.receipt,lock_timeout='100ms 阻塞；释放后更新成功'))


def test_03_actual_rows_fk_and_fixed_mapping(monkeypatch):
    a,ref=load();binding=untyped(a['owner_binding']);s=Selection(DSN,CONFIG['input_run'],CONFIG['input_snapshot'])
    import duckdb
    db=duckdb.connect();lake=s.connect()
    cp=next(cp for cp in s.checkpoints if cp['tables']['peers']['count'])
    try:
        for name,cols in p.OBS_TABLES.items():
            table=lake.execute('SELECT * FROM '+s.table(name,cp['source_id'])).fetch_arrow_table()
            db.register('incoming',table);db.execute('CREATE TABLE "'+name+'" AS SELECT * FROM incoming');db.unregister('incoming')
    finally:lake.close()
    from types import SimpleNamespace
    local=SimpleNamespace(table=lambda name,source=None:'"'+name+'"')
    errors={}
    try:
        v._fk(RT,db,local,cp,lambda:None)
        for label,sql in [('必需event_id','UPDATE elements SET event_id=NULL WHERE action=\'rib_snapshot\''),('RIB错误Peer','UPDATE elements SET peer_index=9999 WHERE action=\'rib_snapshot\'')]:
            db.execute('BEGIN')
            try:
                db.execute(sql)
                with pytest.raises(ValueError,match='完整CP/FK') as caught:v._fk(RT,db,local,cp,lambda:None)
                errors[label]=str(caught.value)
            finally:db.execute('ROLLBACK')
        v._fk(RT,db,local,cp,lambda:None)
    finally:db.close()
    pg=psycopg2.connect(DSN)
    try:
        with pg.cursor() as c:
            c.execute('SELECT f.data_file_id,f.mapping_id FROM ducklake_data_file f JOIN ducklake_table t USING(table_id) JOIN ducklake_schema s USING(schema_id) WHERE s.schema_name=%s AND t.table_name=\'elements\' AND f.begin_snapshot<=%s AND (f.end_snapshot IS NULL OR f.end_snapshot>%s) LIMIT 1',(s.schema,s.snapshot,s.snapshot))
            file_id,old=c.fetchone();assert old is None
            c.execute('UPDATE ducklake_data_file SET mapping_id=999 WHERE data_file_id=%s',(file_id,))
        pg.commit()
        try:
            for name,call in [('current',lambda:p.verify_current(RT,a,guard=guard)),('admit',lambda:p.admit(RT,binding,guard=guard))]:
                with pytest.raises(ValueError,match='column mapping') as caught:call()
                errors['固定映射'+name]=str(caught.value)
        finally:
            with pg.cursor() as c:c.execute('UPDATE ducklake_data_file SET mapping_id=%s WHERE data_file_id=%s',(old,file_id))
            pg.commit()
    finally:pg.close()
    p.verify_current(RT,a,guard=guard)
    save('有限反例.json',dict(errors=errors,scope='原CP全文复制至私有内存DuckDB后受控SQL；真实PG目录mapping注入并恢复，非原生schema演进',data_file_id=file_id))


def test_04_actual_consumer_enumeration_and_small_exits():
    import subprocess
    import sys
    from collections import Counter
    summary={}
    for augment in (False,True):
        expected,inputs=consumer_probe(0,1,augment);seams=actual_seams(expected)
        for seed,batch in ((7,17),(2,1)):
            rows,other=consumer_probe(seed,batch,augment)
            assert {k:Counter(typed(r) for r in rs) for k,rs in inputs.items()}=={k:Counter(typed(r) for r in rs) for k,rs in other.items()}
            assert typed(rows)==typed(expected) and typed(actual_seams(rows))==typed(seams)
        if augment:
            assert sum(len(v['quality']) for v in expected.values())>=4
            assert any(p['bgp_id'] is None for v in expected.values() for m in v['message'] for p in m['peers'])
        if not augment:
            from dataclasses import asdict
            from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch
            direct={source:{'boundary':[],'quality':[],'message':[],'element':[]} for source in CONFIG['ordered_sources']}
            reader=ObservationReader(DSN,CONFIG['input_run'],CONFIG['input_snapshot'],CONFIG['ordered_sources'],profile='observation',batch_rows=17)
            for item in reader.stream():
                target=direct[item.source_id]
                if isinstance(item,MessageBatch):
                    target['quality'].extend(item.source_quality);target['message'].extend(item.messages);target['element'].extend(item.elements)
                else:target['boundary'].append(asdict(item))
            assert typed(direct)==typed(expected)
        label='原CP' if not augment else '副本增强'
        (OUT/(label+'consumer.typed.json')).write_text(typed(expected));(OUT/(label+'出口.typed.json')).write_text(typed(seams))
        code="import sys;sys.path[:0]=sys.argv[1:3];from tests.observations.m2_p1_integration_support import *;r,_=consumer_probe(3,17,sys.argv[3]=='True');print(typed([r,actual_seams(r)]))"
        proc=subprocess.run([sys.executable,'-I','-B','-c',code,str(Path(__file__).resolve().parents[2]),str(Path(__file__).resolve().parent),str(augment)],capture_output=True,text=True)
        (OUT/(label+'新进程.log')).write_text(proc.stderr)
        assert proc.returncode==0,proc.stderr
        assert proc.stdout.strip()==typed([expected,seams])
        summary[label]=dict(sources=len(expected),messages=sum(len(v['message']) for v in expected.values()),elements=sum(len(v['element']) for v in expected.values()),source_quality=sum(len(v['quality']) for v in expected.values()),digest=hashlib.sha256(typed(expected).encode()).hexdigest(),exit_digest=hashlib.sha256(typed(seams).encode()).hexdigest())
    save('consumer对照.json',summary)
    assert files(ROOT)==json.loads((OUT/'原目录SHA前.json').read_text())
    # JSON roundtrip 仅把 SQL tuple 的外壳转换为列表，不改变正文类型。
    assert json.loads(json.dumps(original_metadata()))==json.loads((OUT/'原登记前.json').read_text())
