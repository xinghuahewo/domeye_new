"""Resource P1 自有隔离PG：原件兼容、科学坏件和流终结边界。"""
from pathlib import Path
from types import SimpleNamespace
import json
import os
import time
import uuid
import hashlib
import math
import psycopg2
import pytest
from data_pipeline.analysis.resources import publication as p
from data_pipeline.analysis.resources.publication_codec import CODEC, typed, untyped
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader
from data_pipeline.bgp.archive import admission as up
from tests.resources.test_resource_observation import prepare, launch
from tests.resources.test_resource_bindings import json_request


@pytest.fixture(scope='module')
def case(tmp_path_factory):
    base=os.environ.get('DOMEYE_RESOURCE_TEST_DSN')
    if not base:pytest.skip('需显式本任务人工PG')
    reuse=os.environ.get('DOMEYE_RESOURCE_P1_CASE')
    if reuse:
        root=Path(reuse);request=json.loads((root/'request.json').read_text());dsn=request['dsn']
        report=json.loads((root/'resource/execution.json').read_text())
        seal=report['binding_manifest']['observation_runs'][next(iter(report['binding_manifest']['observation_runs']))]['seal']
    else:
        root=tmp_path_factory.mktemp('resource_p1');name='resource_p1_'+uuid.uuid4().hex
        pg=psycopg2.connect(base);pg.autocommit=True
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
        pg.close();dsn=base+' dbname='+name
        request,seal,measure=prepare(root,dsn,ribs=9,uncertain_outlier=True)
        report=launch(json_request(request),root/'request.json')
    b=ResourceObservationReader(dsn,report['run_id'],report['snapshot'],report['dataset_id']).inputs()
    logs=[];u=up.Runtime(dsn,(root,),root,fixture_only=True,audit_sink=logs.append)
    ub=up.inspect_binding(u,seal['run_id'],seal['snapshot'],[s['context']['source_id'] for s in b['binding']['sources']])
    m=up.admit(u,ub,guard=lambda:None);u.dependency_admissions=(m,)
    ref=up.admit(u,up.reference_binding(u,m,request['csv_binding']['source_id']),guard=lambda:None)
    rt=p.Runtime(dsn,(root,),root,u,(m,ref),fixture_only=True,audit_sink=logs.append)
    (root/'case.json').write_text(json.dumps(dict(dsn=dsn,report=report,binding=b,m2=m,reference=ref),ensure_ascii=False))
    return SimpleNamespace(root=root,rt=rt,b=b,logs=logs,report=report)


def req(view='metrics',batch=7):return dict(view=view,scope_typed=typed({'scope':'all'}),codec_version=CODEC,batch_rows=batch,batch_bytes=1024**2)


def test_admit_reuse_current_and_full_values(case):
    c=case;start=time.monotonic();a=p.admit(c.rt,c.b,guard=lambda:None);first=time.monotonic()-start;c.a=a
    (c.root/'admission.json').write_text(json.dumps(a,ensure_ascii=False));c.logs.clear()
    start=time.monotonic();assert p.admit(c.rt,c.b,guard=lambda:None)==a;reuse=time.monotonic()-start
    start=time.monotonic();p.verify_current(c.rt,a,guard=lambda:None);current=time.monotonic()-start
    assert not any(e['kind'] in ('inventory_scan','science_compare','entity_hash') for e in c.logs)
    costs=[]
    for view in ('metrics','normal_bands','topology_status','coverage'):
        start=time.monotonic();out=[];first_batch=None
        with p.open_reader(c.rt,a,req(view),guard=lambda:None) as s:
            for batch in s:
                if first_batch is None:first_batch=time.monotonic()-start
                assert batch['bytes']==len(batch['rows_typed'].encode());out.extend(untyped(batch['rows_typed']))
            assert s.receipt is None
        assert s.receipt and s.receipt['rows']==len(out)
        reader=ResourceObservationReader(c.rt.dsn,c.b['run_id'],c.b['snapshot'],c.b['dataset_id'])
        old=[r for batch in reader.coverage(scope='all') for r in batch.to_pylist()] if view=='coverage' else list(reader.values(target=view,scope='all'))
        # 原接口资格列表不承诺顺序；科学行/资格的多重性完全保留。
        def norm(row):
            if 'qualification' in row:row={**row,'qualification':sorted(row['qualification'],key=typed)}
            return typed(row)
        assert sorted(map(norm,out))==sorted(map(norm,old))
        costs.append(dict(view=view,rows=len(out),first_batch_seconds=first_batch,full_seconds=time.monotonic()-start,receipt=s.receipt))
    (c.root/'cost.json').write_text(json.dumps(dict(first_admit_seconds=first,reuse_seconds=reuse,current_seconds=current,reads=costs,events=c.logs),ensure_ascii=False))


@pytest.mark.parametrize('value',[True,False,float('nan'),float('inf'),-float('inf'),0,-1])
def test_budget_rejects_at_construction_and_use(case,value):
    c=case
    for name in ('max_rows','max_bytes','max_rss_bytes','max_seconds','memory_bytes','max_temp_bytes','lock_timeout_ms'):
        args=dict(dsn=c.rt.dsn,allowed_roots=(c.root,),scratch_root=c.root,upstream_runtime=c.rt.upstream_runtime,dependency_admissions=c.rt.dependency_admissions,fixture_only=True)
        with pytest.raises(ValueError):p.Runtime(**args,**{name:value})
        original=getattr(c.rt,name);setattr(c.rt,name,value)
        try:
            with pytest.raises(ValueError):p.verify_current(c.rt,c.a,guard=lambda:None)
        finally:setattr(c.rt,name,original)


def test_early_stop_revocation_and_receipt(case):
    c=case;a=c.a
    with p.open_reader(c.rt,a,req(),guard=lambda:None) as s:next(s)
    assert s.receipt is None
    target=next(t for t in a['lock_targets'] if t['namespace']=='resource.run')
    def change(state):
        with psycopg2.connect(c.rt.dsn) as pg,pg.cursor() as cur:cur.execute('UPDATE domeye.resource_runs SET state=%s WHERE run_id=%s',(state,target['key']))
    try:
        with pytest.raises(ValueError):
            with p.open_reader(c.rt,a,req(),guard=lambda:None) as s:
                list(s);change('failed')
        assert s.receipt is None
    finally:change('complete')


def test_single_target_locks(case):
    c=case
    for t in c.a['lock_targets']:
        if t['namespace'] not in p.OWN_NAMES:continue
        table,key=('domeye.resource_runs','run_id') if t['namespace']=='resource.run' else ('domeye.resource_references','reference_id') if t['namespace']=='resource.reference' else (p.TABLE,'record_key')
        with p.hold_lock(c.rt,c.a,t,guard=lambda:None):
            pg=psycopg2.connect(c.rt.dsn)
            try:
                with pg.cursor() as cur:
                    cur.execute("SET lock_timeout='100ms'")
                    with pytest.raises(psycopg2.errors.LockNotAvailable):cur.execute(f'UPDATE {table} SET state=state WHERE {key}=%s',(t['key'],))
            finally:pg.rollback();pg.close()
        with psycopg2.connect(c.rt.dsn) as pg,pg.cursor() as cur:cur.execute(f'UPDATE {table} SET state=state WHERE {key}=%s',(t['key'],))


def test_close_failures_keep_primary_and_no_receipt(case,monkeypatch):
    from data_pipeline.analysis.resources import publication_validation as v
    original=v.selected_rows
    class BrokenClose:
        def __init__(self,*args):self.it=original(*args)
        def __iter__(self):return self
        def __next__(self):return next(self.it)
        def close(self):self.it.close();raise OSError('fixture close failure after actual close')
    monkeypatch.setattr(v,'selected_rows',BrokenClose)
    with pytest.raises(OSError):
        with p.open_reader(case.rt,case.a,req(),guard=lambda:None) as session:list(session)
    assert session.receipt is None
    primary=RuntimeError('caller primary')
    with pytest.raises(RuntimeError) as caught:
        with p.open_reader(case.rt,case.a,req(),guard=lambda:None) as session:
            next(session);raise primary
    assert caught.value is primary and len(primary.cleanup_errors)==1 and session.receipt is None
    first=OSError('rollback');second=OSError('close')
    def fail(e):raise e
    with pytest.raises(OSError) as caught:p._close((lambda:fail(first),lambda:fail(second)))
    assert caught.value is first and first.cleanup_errors==(second,)


@pytest.mark.parametrize('view',['metrics','coverage'])
def test_nonfinite_request_and_accumulation(case,view):
    for invalid in (True,False,0,-1,math.nan,math.inf):
        for field in ('batch_rows','batch_bytes'):
            request={**req(view),field:invalid}
            with pytest.raises(ValueError):
                with p.open_reader(case.rt,case.a,request,guard=lambda:None):pass
    with pytest.raises(ValueError):
        with p.open_reader(case.rt,case.a,{**req(view),'batch_bytes':1},guard=lambda:None) as s:list(s)
    assert s.receipt is None


@pytest.mark.parametrize('table,column,value',[('normal_bands','mean',12345.0),('topology_edges','weight',3),('metrics','as_name','错误但同计数'),('rendered_paths','duplicate',None)])
def test_actual_bad_science_with_resigned_legacy_receipt_rejects(case,table,column,value):
    """实际Parquet坏件，旧自hash全部重新签齐；新admit仍由固定原输入拒绝。"""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from copy import deepcopy
    from psycopg2.extras import Json
    from data_pipeline.analysis.resources.qualification import table_inventory
    from data_pipeline.analysis.resources.identity import identity_digest
    from data_pipeline.analysis.resources.publication_validation import relation
    c=case;old=deepcopy(c.b)
    with p._lake(c.rt) as db:paths=p._descriptor(c.rt,db,old)['resource_'+old['run_id']+'.'+table]['paths']
    path=Path(paths[0]);raw=path.read_bytes();execution=c.root/'resource/execution.json';execution_raw=execution.read_bytes()
    with psycopg2.connect(c.rt.dsn) as pg,pg.cursor() as cur:
        cur.execute('SELECT count(*) FROM '+p.TABLE+" WHERE state='accepted'");count=cur.fetchone()[0]
    with psycopg2.connect(c.rt.dsn) as pg,pg.cursor() as cur:
        cur.execute("SELECT data_file_id,file_size_bytes,footer_size,record_count FROM public.ducklake_data_file WHERE path=%s OR path=%s",(str(path),path.name));file_meta=cur.fetchone()
    try:
        body=pq.read_table(path);rows=body.to_pylist()
        if column=='duplicate':rows.append(dict(rows[0]))
        else:rows[0][column]=value
        pq.write_table(pa.Table.from_pylist(rows,schema=body.schema),path)
        with psycopg2.connect(c.rt.dsn) as pg,pg.cursor() as cur:
            cur.execute('UPDATE public.ducklake_data_file SET file_size_bytes=%s,footer_size=%s,record_count=%s WHERE data_file_id=%s',(path.stat().st_size,pq.ParquetFile(path).metadata.serialized_size,len(rows),file_meta[0]))
        receipt=deepcopy(old['receipt'])
        with p._lake(c.rt) as db:receipt['inventory']=table_inventory(db,lambda t:relation(old,t))
        dataset=identity_digest(dict(run_id=old['run_id'],snapshot=old['snapshot'],receipt=receipt))
        with psycopg2.connect(c.rt.dsn) as pg,pg.cursor() as cur:cur.execute('UPDATE domeye.resource_runs SET dataset_id=%s,observation_receipt=%s WHERE run_id=%s',(dataset,Json(receipt),old['run_id']))
        updated=json.loads(execution_raw);updated.update(dataset_id=dataset,inventory=receipt['inventory']);execution.write_text(json.dumps(updated))
        b={**old,'dataset_id':dataset,'receipt':receipt}
        with pytest.raises(ValueError,match='科学全值/多重性') as caught:p.admit(c.rt,b,guard=lambda:None)
        with psycopg2.connect(c.rt.dsn) as pg,pg.cursor() as cur:
            cur.execute('SELECT count(*) FROM '+p.TABLE+" WHERE state='accepted'");assert cur.fetchone()[0]==count
        (c.root/('bad-'+table+'.json')).write_text(json.dumps(dict(table=table,column=column,rejected=True,error=str(caught.value),accepted_unchanged=True),ensure_ascii=False))
    finally:
        path.write_bytes(raw);execution.write_bytes(execution_raw)
        with psycopg2.connect(c.rt.dsn) as pg,pg.cursor() as cur:cur.execute('UPDATE public.ducklake_data_file SET file_size_bytes=%s,footer_size=%s,record_count=%s WHERE data_file_id=%s',(file_meta[1],file_meta[2],file_meta[3],file_meta[0]))
        with psycopg2.connect(c.rt.dsn) as pg,pg.cursor() as cur:cur.execute('UPDATE domeye.resource_runs SET dataset_id=%s,observation_receipt=%s WHERE run_id=%s',(old['dataset_id'],Json(old['receipt']),old['run_id']))


def test_missing_anchor_wrong_oid_and_missing_target(case):
    from copy import deepcopy
    a=p.admit(case.rt,case.b,guard=lambda:None)
    for mutate in ('key','oid','target'):
        bad=deepcopy(a)
        if mutate=='key':next(t for t in bad['lock_targets'] if t['namespace']=='resource.admission')['key']=str(uuid.uuid4())
        elif mutate=='oid':bad['physical']['database_oid']+=1
        else:bad['lock_targets']=[t for t in bad['lock_targets'] if t['namespace']!='resource.run']
        bad['admission_id']=p.digest({k:v for k,v in bad.items() if k!='admission_id'})
        with pytest.raises(ValueError):p.verify_current(case.rt,bad,guard=lambda:None)
    with pytest.raises(ValueError):
        with p.hold_lock(case.rt,a,{**a['lock_targets'][-1],'key':'missing'},guard=lambda:None):pass
