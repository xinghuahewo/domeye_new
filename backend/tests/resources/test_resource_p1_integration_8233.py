"""8233 原 Resource 人工制品的有限公共 P1 集成；不生产科学结果。"""
import hashlib
import json
import os
import pickle
import resource
import sys
import time
from collections import Counter
from contextlib import closing,contextmanager
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import psycopg2
from psycopg2.extras import Json
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

if os.environ.get('DOMEYE_RESOURCE_P1_INTEGRATION')!='8233':
    pytest.skip('仅显式自有原人工 Resource 集成',allow_module_level=True)
from data_pipeline.analysis.resources import publication as p, publication_validation as v
from data_pipeline.analysis.resources.publication_codec import typed, untyped, CODEC
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader
from data_pipeline.analysis.resources.qualification import ALL_TABLES, table_inventory
from data_pipeline.analysis.resources.identity import identity_digest
from data_pipeline.bgp.archive import admission as up
from tests.country.country_query_test_support import PgLogProbe

OUT=Path('/tmp/domeye-resource-p1-resumed-8233').resolve()
ROOT=Path('/tmp/domeye-resource-m3-integration-8233').resolve()
SCRATCH=OUT/'scratch';Q=json.loads((ROOT/'case.json').read_text());DSN=Q['request']['dsn']
EVENTS=[]
def save(name,value):(OUT/name).write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str))
def hashes():return {str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in ROOT.rglob('*') if f.is_file()}
def anchors():
    result={}
    with closing(psycopg2.connect(DSN)) as pg,pg.cursor() as c:
        for t in ('domeye.resource_runs','domeye.resource_references','observation_m2.runs','observation_m2.checkpoints'):
            c.execute('SELECT to_jsonb(t) FROM '+t+' t');result[t]=sorted([r[0] for r in c.fetchall()],key=lambda x:json.dumps(x,sort_keys=True))
        for t in (p.TABLE,*up.TABLES.values()):
            c.execute('SELECT to_regclass(%s)',(t,));exists=c.fetchone()[0]
            if exists:c.execute('SELECT record_key,to_jsonb(t) FROM '+t+' t');result[t]=dict(c.fetchall())
            else:result[t]={}
    return result
@contextmanager
def measure(name):
    probe=PgLogProbe(DSN,OUT/'pg.log');offset=probe.start();EVENTS.clear();start=time.monotonic();cost={}
    try:yield cost
    finally:
        cost['wall_seconds']=time.monotonic()-start;snippet,count=probe.finish(offset)
        (OUT/(name+'.pg.log')).write_text(snippet)
        cost.update(pg_statements=count,event_counts=dict(Counter(e['kind'] for e in EVENTS)),
            selected_batch_rows=sum(e.get('rows',0) for e in EVENTS if e['kind']=='selected_batch'),
            selected_batch_arrow_bytes=sum(e.get('decoded_bytes',0) for e in EVENTS if e['kind']=='selected_batch'),
            process_lifetime_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),
            rss_scope='本 Python 进程生命周期累计高水位，不含 PG，不是阶段独占峰值',pg_peak_memory=None,physical_disk_read_bytes=None)
        save(name+'.cost.json',cost);save(name+'.events.json',EVENTS)
def req(view='metrics',batch=7):return dict(view=view,scope_typed=typed({'scope':'all'}),codec_version=CODEC,batch_rows=batch,batch_bytes=1024**2)
def norm(row):
    # 只允许顶层 qualification 的未承诺顺序；其他嵌套列表完全保留。
    return typed({**row,'qualification':sorted(row['qualification'],key=typed)}) if 'qualification' in row else typed(row)
def collect(rt,a,request,cost):
    rows=[];start=time.monotonic()
    with p.open_reader(rt,a,request,guard=lambda:None) as s:
        for batch in s:
            if 'first_batch_seconds' not in cost:cost['first_batch_seconds']=time.monotonic()-start
            assert batch['bytes']==len(batch['rows_typed'].encode()) and batch['rows']<=request['batch_rows']
            rows.extend(untyped(batch['rows_typed']))
        assert s.receipt is None
    assert s.receipt['rows']==len(rows) and s.receipt['execution']=='complete'
    return rows,s.receipt

@pytest.fixture(scope='module')
def context():
    before=anchors();save('原锚前.json',before)
    assert before[p.TABLE]=={}  # 本任务没有旧 Resource P1，不伪造升级历史。
    r=Q['report'];assert (r['run_id'],r['snapshot'])==('c4b0419f11fc4d6ab7f4abb714ae1078',67)
    reader=ResourceObservationReader(DSN,r['run_id'],r['snapshot'],r['dataset_id'])
    with measure('原Reader完整binding'):b=reader.inputs()
    original=pickle.loads((ROOT/'新进程全正文.pickle').read_bytes())
    assert typed(b)==typed(original['inputs']);save('原binding.json',b)
    u=up.Runtime(DSN,(ROOT,OUT),SCRATCH,fixture_only=True,audit_sink=EVENTS.append)
    # 原声明仍取自原件，没有为路径试验移动输入或修改旧 plan。
    csv_path=Q['request']['csv_binding']['source_id']
    assert any(e['source_id']==csv_path for e in Q['seal']['checkpoints'])
    constructor=p.Runtime(DSN,(ROOT,OUT),SCRATCH,u,(),fixture_only=True)
    assert not any(hasattr(constructor,k) for k in ('_root_bindings','_path_bindings','_path_identity'))
    assert constructor.path(ROOT/'inputs/as.csv')==ROOT/'inputs/as.csv'
    save('共享Runtime原构造.json',dict(resource_constructed=True,no_m2_private_state=True,canonical_path=str(constructor.path(ROOT/'inputs/as.csv'))))
    deps=[]
    for i,(binding_id,full) in enumerate(b['binding']['observation_runs'].items()):
        seal=full['seal'];sids=[s['context']['source_id'] for s in b['binding']['sources'] if b['binding']['observation_inputs'][s['context']['source_id']]['binding_id']==binding_id]
        with measure('原M2inspect'+str(i)):ub=up.inspect_binding(u,seal['run_id'],seal['snapshot'],sids)
        with measure('原M2admit'+str(i)):deps.append(up.admit(u,ub,guard=lambda:None))
        source=next(e for e in ub['plan']['inputs'] if e['sha256']==csv_path)
        assert source['path'].startswith('/tmp/') and str(u.path(source['path'])).startswith('/private/tmp/')
        entity=next(e for e in deps[-1]['entities'] if e['source_path']==source['path'])
        assert entity['path']==str(Path(source['path']).resolve()) and len(entity)==8
        save('原tmp已直接准入.json',dict(source=source,entity=entity,admission_id=deps[-1]['admission_id']))
    u.dependency_admissions=tuple(deps);csv=b['binding']['csv_reference']
    ma=next(d for d in deps if up.untyped(d['owner_binding'])['run_id']==csv['run_id'])
    with measure('原CSVadmit'):ref=up.admit(u,up.reference_binding(u,ma,csv['source_id']),guard=lambda:None)
    deps.append(ref);save('上游当前依赖.json',deps)
    rt=p.Runtime(DSN,(ROOT,OUT),SCRATCH,u,tuple(deps),fixture_only=True,audit_sink=EVENTS.append)
    with measure('Resource首次完整admit'):a=p.admit(rt,b,guard=lambda:None)
    assert sum(e['kind']=='inventory_scan' for e in EVENTS)==1
    assert sum(e['kind']=='science_compare' for e in EVENTS)==10
    done=next(e for e in EVENTS if e['kind']=='admit_complete')
    assert done['algorithm_replays']==1 and done['legacy_relation_audits']==1
    assert all(len(e)==7 and 'source_path' not in e for e in a['entities'])
    save('首次审计.json',done);save('首次Admission.json',a)
    yield rt,a,b,original
    after=anchors();save('原锚后.json',after)
    for t,old in before.items():
        if isinstance(old,list):assert after[t]==old
        else:
            for k,value in old.items():assert after[t][k]==value
    assert hashes()==json.loads((OUT/'原目录SHA.json').read_text())
    assert not list(SCRATCH.iterdir())

def test_01_original_16_tables_and_four_public_views(context):
    rt,a,b,original=context
    with measure('Resource复用'):assert p.admit(rt,b,guard=lambda:None)==a
    assert not any(e['kind'] in ('inventory_scan','science_compare','entity_hash') for e in EVENTS)
    with measure('Resourcecurrent'):p.verify_current(rt,a,guard=lambda:None)
    assert not any(e['kind'] in ('inventory_scan','science_compare','entity_hash') for e in EVENTS)
    with measure('原16表逐值核验'):
        with p._lake(rt) as db:
            rows={t:db.execute('SELECT * FROM '+v.relation(b,t)+' ORDER BY ALL').fetch_arrow_table().to_pylist() for t in ALL_TABLES}
    for t in ALL_TABLES:
        assert Counter(map(typed,rows[t]))==Counter(map(typed,original['tables'][t])),t
    save('原16表全typed.json',{t:typed(r) for t,r in rows.items()})
    save('原16表计数.json',{t:len(r) for t,r in rows.items()})
    for view in ('metrics','normal_bands','topology_status','coverage'):
        expected=rows['coverage'] if view=='coverage' else original['values'][view]
        with measure(view+'完整读取') as cost:out,receipt=collect(rt,a,req(view),cost)
        assert Counter(map(norm,out))==Counter(map(norm,expected)),view
        assert not any(e['kind'] in ('inventory_scan','science_compare','entity_hash') for e in EVENTS)
        save(view+'.typed.json',typed(out));save(view+'.receipt.json',receipt)

def test_02_three_actual_single_target_locks(context):
    rt,a,_,_=context;results=[]
    for target in a['lock_targets']:
        ns=target['namespace']
        if ns not in p.OWN_NAMES:continue
        table,key={'resource.run':('domeye.resource_runs','run_id'),'resource.reference':('domeye.resource_references','reference_id'),'resource.admission':(p.TABLE,'record_key')}[ns]
        def update(blocked):
            with closing(psycopg2.connect(DSN)) as pg,pg.cursor() as c:
                c.execute("SET LOCAL lock_timeout='50ms'")
                if blocked:
                    with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute(f'UPDATE {table} SET state=state WHERE {key}=%s',(target['key'],))
                else:c.execute(f'UPDATE {table} SET state=state WHERE {key}=%s',(target['key'],))
                pg.rollback()
        with measure(ns+'单锁'):
            with p.hold_lock(rt,a,target,guard=lambda:None):
                assert sum('FOR SHARE' in e.get('sql','') for e in EVENTS)==1
                assert not any(e['kind'] in ('current_complete','inventory_scan','science_compare') for e in EVENTS)
                update(True);count=len(EVENTS)
            assert len(EVENTS)==count;update(False)
        results.append(dict(target=target,blocked=True,released=True,share_locks=1,no_hidden_current_or_tail_audit=True))
    assert len(results)==3;save('三个真实单锁.json',results)

def test_03_early_stop_and_tail_revocation(context):
    rt,a,b,_=context
    with measure('首批早停') as cost:
        with p.open_reader(rt,a,req(batch=1),guard=lambda:None) as s:batch=next(s);assert batch['rows']==1
        assert s.receipt is None
    def state(value):
        with closing(psycopg2.connect(DSN)) as pg,pg,pg.cursor() as c:c.execute('UPDATE domeye.resource_runs SET state=%s WHERE run_id=%s',(value,b['run_id']))
    try:
        with measure('完整读取尾撤销'):
            with pytest.raises(ValueError) as caught:
                with p.open_reader(rt,a,req(),guard=lambda:None) as s:list(s);state('failed')
            assert s.receipt is None
    finally:state('complete')
    save('早停尾撤销.json',dict(early_rows=1,early_receipt=None,tail_receipt=None,tail_error=str(caught.value),original_state_restored=True))

def test_04_real_duckdb_close_error(context,monkeypatch):
    rt,a,_,_=context;connect=p.connect_duckdb;closed=[]
    class DB:
        def __init__(self):self.db=connect()
        def __getattr__(self,name):return getattr(self.db,name)
        def close(self):self.db.close();closed.append(True);raise OSError('本任务真实 DuckDB 关闭后错误')
    results=[]
    for primary in (None,RuntimeError('调用方原错误')):
        with measure('真实close-'+('primary' if primary else 'normal')):
            with pytest.raises(OSError if primary is None else RuntimeError) as caught:
                with p.open_reader(rt,a,req(),guard=lambda:None) as s:
                    # 首 current 已完成，仅包装本次正文连接；实际 close 后再抛错。
                    with monkeypatch.context() as patch:
                        patch.setattr(p,'connect_duckdb',DB)
                        if primary is None:list(s)
                        else:next(s);raise primary
            assert s.receipt is None
            if primary is not None:assert caught.value is primary and len(primary.cleanup_errors)==1
        results.append(dict(primary_preserved=primary is not None,error=str(caught.value),receipt=None))
    assert closed==[True,True] and not list(SCRATCH.iterdir());save('真实关闭无回执.json',dict(actual_closes=len(closed),results=results))

def test_05_finite_budget_on_actual_reader(context):
    rt,a,_,_=context;results=[]
    for invalid in (True,float('nan'),float('inf')):
        with pytest.raises(ValueError):replace(rt,max_seconds=invalid)
        x=replace(rt)
        with measure('实际预算-'+str(invalid)):
            with pytest.raises(ValueError) as caught:
                with p.open_reader(x,a,req(batch=1),guard=lambda:None) as s:next(s);x.max_seconds=invalid;list(s)
            assert s.receipt is None
        results.append(dict(value=str(invalid),error=str(caught.value),receipt=None))
    x=replace(rt)
    with measure('实际累计行预算'):
        with pytest.raises(ValueError) as caught:
            with p.open_reader(x,a,req(batch=1),guard=lambda:None) as s:next(s);x.max_rows=1;list(s)
        assert s.receipt is None
    results.append(dict(max_rows=1,error=str(caught.value),receipt=None))
    x=replace(rt);rows=[]
    with measure('有限秒数调整'):
        with p.open_reader(x,a,req(batch=1),guard=lambda:None) as s:
            rows+=untyped(next(s)['rows_typed']);x.max_seconds=600.5
            for batch in s:rows+=untyped(batch['rows_typed'])
        assert s.receipt['execution']=='complete'
    assert rows==untyped(json.loads((OUT/'metrics.typed.json').read_text()))
    save('有限预算.json',dict(failures=results,finite_seconds=600.5,receipt=s.receipt))

def test_06_one_resigned_bad_science_then_new_entity_admission(context):
    rt,a,b,_=context;old=deepcopy(b)
    with p._lake(rt) as db:desc=p._descriptor(rt,db,b)
    path=Path(desc['resource_'+b['run_id']+'.normal_bands']['paths'][0])
    execution=Path(b['receipt']['storage_layout']['local_output'])/'execution.json'
    raw=path.read_bytes();execution_raw=execution.read_bytes();before=anchors()
    with closing(psycopg2.connect(DSN)) as pg,pg.cursor() as c:
        c.execute('SELECT data_file_id,file_size_bytes,footer_size,record_count FROM public.ducklake_data_file WHERE path=%s OR path=%s',(str(path),path.name));meta=c.fetchone();assert meta
    try:
        body=pq.read_table(path);rows=body.to_pylist();original_value=rows[0]['mean'];rows[0]['mean']=12345.0
        pq.write_table(pa.Table.from_pylist(rows,schema=body.schema),path)
        with closing(psycopg2.connect(DSN)) as pg,pg,pg.cursor() as c:
            c.execute('UPDATE public.ducklake_data_file SET file_size_bytes=%s,footer_size=%s,record_count=%s WHERE data_file_id=%s',(path.stat().st_size,pq.ParquetFile(path).metadata.serialized_size,len(rows),meta[0]))
        receipt=deepcopy(old['receipt'])
        with p._lake(rt) as db:receipt['inventory']=table_inventory(db,lambda t:v.relation(old,t))
        dataset=identity_digest(dict(run_id=old['run_id'],snapshot=old['snapshot'],receipt=receipt))
        with closing(psycopg2.connect(DSN)) as pg,pg,pg.cursor() as c:c.execute('UPDATE domeye.resource_runs SET dataset_id=%s,observation_receipt=%s WHERE run_id=%s',(dataset,Json(receipt),old['run_id']))
        updated=json.loads(execution_raw);updated.update(dataset_id=dataset,inventory=receipt['inventory']);execution.write_text(json.dumps(updated))
        with measure('normal均值坏件自签拒绝'):
            with pytest.raises(ValueError,match='科学全值/多重性') as caught:p.admit(rt,{**old,'dataset_id':dataset,'receipt':receipt},guard=lambda:None)
        assert anchors()[p.TABLE]==before[p.TABLE]
        save('代表坏件.json',dict(path=str(path),original_mean=original_value,mutated_mean=12345.0,resigned_dataset=dataset,error=str(caught.value),accepted_unchanged=True))
    finally:
        path.write_bytes(raw);execution.write_bytes(execution_raw)
        with closing(psycopg2.connect(DSN)) as pg,pg,pg.cursor() as c:
            c.execute('UPDATE public.ducklake_data_file SET file_size_bytes=%s,footer_size=%s,record_count=%s WHERE data_file_id=%s',(meta[1],meta[2],meta[3],meta[0]))
            c.execute('UPDATE domeye.resource_runs SET dataset_id=%s,observation_receipt=%s WHERE run_id=%s',(old['dataset_id'],Json(old['receipt']),old['run_id']))
    assert hashes()==json.loads((OUT/'原目录SHA.json').read_text())
    with measure('恢复后原Admission实体漂移拒绝'):
        with pytest.raises(ValueError) as caught:p.verify_current(rt,a,guard=lambda:None)
    with measure('恢复原字节后新实体admit'):fresh=p.admit(rt,old,guard=lambda:None)
    assert fresh['admission_id']!=a['admission_id'] and fresh['owner_binding']==a['owner_binding']
    p.verify_current(rt,fresh,guard=lambda:None)
    after=anchors();assert all(after[p.TABLE][k]==value for k,value in before[p.TABLE].items())
    assert typed(ResourceObservationReader(DSN,b['run_id'],b['snapshot'],b['dataset_id']).inputs())==typed(old)
    save('恢复后新Admission.json',fresh)
    save('实体变化边界.json',dict(old_admission=a['admission_id'],new_admission=fresh['admission_id'],old_error=str(caught.value),old_registration_preserved=True,original_bytes_binding_receipt_unchanged=True))

def test_07_original_resource_and_q1(monkeypatch):
    from tests.resources.test_resource_m3_integration import test_original_resource_q1_without_reimport as check
    monkeypatch.setenv('DOMEYE_RESOURCE_OLD_ROOT','/tmp/domeye-m2-repaired-integration-8233/test_resource_feature_share_ob0')
    monkeypatch.setenv('DOMEYE_RESOURCE_OWN_ROOT',str(OUT))
    with measure('旧Resource与Q1'):check()
