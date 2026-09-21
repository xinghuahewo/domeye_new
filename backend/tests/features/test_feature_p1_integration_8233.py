"""8233 原 Feature M3 公共P1有限集成；没有科学生产。"""
import os
import json
import time
import hashlib
from pathlib import Path
from collections import Counter
from contextlib import contextmanager,closing
from dataclasses import replace
import pytest
import psycopg2
if os.environ.get('DOMEYE_FEATURE_P1_INTEGRATION')!='8233':pytest.skip('仅显式自有原人工Feature集成',allow_module_level=True)
from data_pipeline.analysis.features import publication as p, publication_io as io
from data_pipeline.bgp.archive import admission as m
from data_pipeline.bgp.archive.value_codec import typed, untyped
from data_pipeline.analysis.features.store import read_table
from data_pipeline.analysis.features.qualification import PROFILE
from tests.features.test_feature_publication import request
from tests.country.country_query_test_support import PgLogProbe
OUT=Path('/tmp/domeye-feature-p1-integration-8233').resolve();SCRATCH=OUT/'scratch'
ROOT=Path('/tmp/domeye-feature-m3-integration-8233/feature-m30').resolve()
ORIGINAL=json.loads((ROOT/'本进程公开读取.json').read_text());B=ORIGINAL['binding'];Q=json.loads((ROOT/'绑定请求.json').read_text());DSN=Q['dsn']
EVENTS=[]
def save(name,value):(OUT/name).write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str))
def hashes(root):return {str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in root.rglob('*') if f.is_file()}
def anchors():
    with closing(psycopg2.connect(DSN)) as pg,pg.cursor() as c:
        result={}
        for table in ['feature.runs','feature.qualified_results','feature.source_commits','observation_m2.runs','observation_m2.checkpoints']:
            c.execute('SELECT to_jsonb(t) FROM '+table+' t');result[table]=sorted((r[0] for r in c.fetchall()),key=lambda r:json.dumps(r,sort_keys=True))
        for table in ['feature.publication_admissions','observation_publication.m2_admissions','observation_publication.reference_admissions']:
            c.execute('SELECT to_regclass(%s)',(table,));exists=c.fetchone()[0]
            if exists:c.execute('SELECT record_key,to_jsonb(t) FROM '+table+' t');result[table]=dict(c.fetchall())
            else:result[table]={}
    return result
@contextmanager
def measure(name,pg=True):
    probe=PgLogProbe(DSN,OUT/'pg.log') if pg else None;offset=probe.start() if probe else None
    EVENTS.clear();start=time.monotonic();cost={}
    try:yield cost
    finally:
        cost['wall_seconds']=time.monotonic()-start
        if probe:
            snippet,count=probe.finish(offset);(OUT/(name+'.pg.log')).write_text(snippet);cost['pg_statements']=count
        else:cost['pg_statements']=None
        cost.update(event_counts=dict(Counter(e['kind'] for e in EVENTS)),parquet_bytes=sum(e.get('bytes',0) for e in EVENTS if e['kind']=='parquet_read'),entity_hash_bytes=sum(e.get('bytes',0) for e in EVENTS if e['kind']=='entity_hash'),decoded_rows=sum(e.get('rows',0) for e in EVENTS if e['kind']=='decoded_batch'),temporary_sample_max=max([e.get('temporary_bytes',0) for e in EVENTS if e['kind']=='resource_sample'] or [0]),process_lifetime_rss_max=max([e.get('rss_high_water_bytes',0) for e in EVENTS if e['kind']=='resource_sample'] or [0]) or None)
        save(name+'.cost.json',cost);save(name+'.events.json',EVENTS)
def collect(rt,a,req,cost=None):
    rows=[];start=time.monotonic()
    with p.open_reader(rt,a,req,guard=lambda:None) as s:
        for chunk in s:
            if cost is not None and 'first_batch' not in cost:cost['first_batch']=dict(wall_seconds=time.monotonic()-start,events=list(EVENTS))
            assert chunk['bytes']==len(chunk['rows_typed'].encode()) and chunk['rows']<=req['batch_rows']
            rows.extend(untyped(chunk['rows_typed']))
        assert s.receipt is None
    assert s.receipt['execution']=='complete';return rows,s.receipt
@pytest.fixture(scope='module')
def context():
    before=anchors();save('原锚前.json',before);save('原目录SHA.json',hashes(ROOT))
    with measure('上游fixture构造',False):u=m.Runtime(DSN,(ROOT,OUT),SCRATCH,fixture_only=True,audit_sink=EVENTS.append)
    seal=B['specification']['observation_seals'][0];sources=[s['source_id'] for s in B['specification']['ordered_bindings'][0]['sources']]
    assert seal['run_id']=='d21c4cf77200477fb5648bc32e39e146' and seal['run_id']!='3884fdb6cdb7408d92f78a85db70a6af'
    with measure('原M2inspect'):binding=m.inspect_binding(u,seal['run_id'],seal['snapshot'],sources)
    with measure('原M2首次admit'):ma=m.admit(u,binding,guard=lambda:None)
    u.dependency_admissions=(ma,)
    with measure('原reference首次admit'):ra=m.admit(u,m.reference_binding(u,ma,B['specification']['reference_binding']['source_sha256']),guard=lambda:None)
    for a in (ma,ra):m.verify_current(u,a,guard=lambda:None)
    save('上游当前依赖.json',[ma,ra])
    with measure('Feature构造',False):rt=p.Runtime(DSN,ROOT/'formal/output',(ROOT,OUT),SCRATCH,(ma,ra),{a['admission_id']:u for a in (ma,ra)},fixture_only=True,audit_sink=EVENTS.append)
    with measure('Feature首次完整admit'):a=p.admit(rt,B,guard=lambda:None)
    assert sum(e['kind']=='feature_full_audit' for e in EVENTS)==1
    save('FeatureAdmission.json',a)
    yield rt,a
    after=anchors();save('原锚后.json',after)
    for table,old in before.items():
        if isinstance(old,list):assert after[table]==old
        else:
            for k,v in old.items():assert after[table][k]==v
    assert hashes(ROOT)==json.loads((OUT/'原目录SHA.json').read_text()) and not list(SCRATCH.iterdir())

def test_01_full_original_and_public_reads(context):
    rt,a=context
    with measure('Feature复用'):assert p.admit(rt,B,guard=lambda:None)==a
    assert not any(e['kind'] in ('feature_full_audit','entity_hash','parquet_row_group') for e in EVENTS)
    with measure('Featurecurrent'):p.verify_current(rt,a,guard=lambda:None)
    assert not any(e['kind'] in ('feature_full_audit','entity_hash','parquet_row_group') for e in EVENTS)
    old=json.loads((ROOT/'全部主体.json').read_text());raw={}
    for t in io.TABLES:
        raw[t]=[r for b in read_table(DSN,B['run_id'],B['snapshot'],t,profile=PROFILE) for r in b.to_pylist()]
        assert Counter(typed(r) for r in raw[t])==Counter(typed(r) for r in old[t])
    save('原12表全typed.json',{k:typed(v) for k,v in raw.items()})
    for view in ('windows','coverage'):
        pair=[]
        for batch in (1,17):
            label=view+str(batch)
            with measure(label) as cost:rows,receipt=collect(rt,a,request(view,batch=batch),cost)
            assert Counter(typed(r) for r in rows)==Counter(typed(r) for r in ORIGINAL[view])
            save(label+'.typed.json',typed(rows));save(label+'.receipt.json',receipt);pair.append((rows,receipt['typed_digest']))
        assert pair[0]==pair[1]
    with measure('initial零窗口仍有coverage'):
        w,wr=collect(rt,a,request('windows','initial'));c,cr=collect(rt,a,request('coverage','initial'))
    assert w==[] and len(c)==12
    save('initial.json',dict(windows=w,coverage=c,receipts=[wr,cr]))

def test_02_stage30_single_lock(context):
    rt,a=context;t=p._own(a);assert t['stage']==30
    def update(blocked):
        with closing(psycopg2.connect(DSN)) as pg,pg.cursor() as c:
            c.execute("SET LOCAL lock_timeout='50ms'")
            if blocked:
                with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute('UPDATE '+p.REGISTRY+' SET state=state WHERE record_key=%s',(t['key'],))
            else:c.execute('UPDATE '+p.REGISTRY+' SET state=state WHERE record_key=%s',(t['key'],))
            pg.rollback()
    with measure('stage30实际单锁'):
        with p.hold_lock(rt,a,t,guard=lambda:None):
            assert sum('FOR SHARE' in e.get('sql','') for e in EVENTS)==1;update(True);count=len(EVENTS)
        assert len(EVENTS)==count;update(False)
    save('stage30单锁.json',dict(target=t,blocked=True,after_release=True,no_tail_audit=True))

@pytest.mark.parametrize('field',['max_temp_bytes','max_rss_bytes'])
def test_03_budget_actual_paths(context,field):
    base,a=context;result=[]
    for value in (float('nan'),float('inf')):
        with pytest.raises(ValueError,match=field):replace(base,**{field:value})
        for when in ('first','tail'):
            rt=replace(base);session=None
            with pytest.raises(ValueError,match=field) as held:
                with p.open_reader(rt,a,request('coverage'),guard=lambda:None) as session:
                    if when=='first':next(session);setattr(rt,field,value);list(session)
                    else:list(session);setattr(rt,field,value)
            assert session.receipt is None
            result.append(dict(value=str(value),when=when,error=str(held.value),receipt=None))
            assert not list(SCRATCH.iterdir())
    rt=replace(base);out=[]
    with p.open_reader(rt,a,request('coverage'),guard=lambda:None) as session:
        out.extend(untyped(next(session)['rows_typed']));setattr(rt,field,float(2*1024**3))
        for chunk in session:out.extend(untyped(chunk['rows_typed']))
    assert Counter(map(typed,out))==Counter(map(typed,ORIGINAL['coverage'])) and session.receipt['execution']=='complete'
    result.append(dict(finite_adjustment=float(2*1024**3),receipt=session.receipt))
    rt=replace(base)
    with pytest.raises(ValueError,match='临时盘/RSS保护') as held:
        with p.open_reader(rt,a,request('coverage'),guard=lambda:None) as session:next(session);setattr(rt,field,1);list(session)
    assert session.receipt is None;result.append(dict(finite_limit=1,error=str(held.value),receipt=None))
    save(field+'预算使用点.json',result)

def test_04_real_close_error(context,monkeypatch):
    rt,a=context;connect=io.connect_duckdb;closed=[]
    class DB:
        def __init__(self,*args):self.db=connect(*args)
        def __getattr__(self,name):return getattr(self.db,name)
        def close(self):self.db.close();closed.append(True);raise OSError('集成真实DuckDB关闭后错误')
    monkeypatch.setattr(io,'connect_duckdb',DB)
    with pytest.raises(OSError,match='真实DuckDB关闭后错误') as held:
        with p.open_reader(rt,a,request(),guard=lambda:None) as session:list(session)
    assert closed==[True] and session.receipt is None and held.value.__traceback__ is not None and not list(SCRATCH.iterdir())
    save('真实关闭无回执.json',dict(real_closed=True,error=str(held.value),receipt=None,traceback_retained=True,scratch_empty=True))

def test_05_original_eight_and_q1(monkeypatch):
    from tests.features.test_feature_m3_integration import test_original_strict_feature_and_q1_without_reproduction as check
    monkeypatch.setenv('DOMEYE_FEATURE_OLD_ROOT','/tmp/domeye-m2-repaired-integration-8233/test_resource_feature_share_ob0')
    check(OUT)
    old=json.loads(Path('/tmp/domeye-feature-m3-integration-8233/test_original_strict_feature_a0/旧Feature与Q1.json').read_text())
    new=json.loads((OUT/'旧Feature与Q1.json').read_text());assert old==new
