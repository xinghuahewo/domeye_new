"""Feature/Canonical/Detection 的同一固定HEAD公共Runtime接合；只复用原人工制品。"""
import os
import json
import hashlib
import time
import resource
import sys
from copy import deepcopy
from pathlib import Path
from contextlib import contextmanager,closing
from collections import Counter
from dataclasses import asdict,replace
import psycopg2
from psycopg2.extensions import parse_dsn,make_dsn
import pytest
if os.environ.get('DOMEYE_PUBLIC_RUNTIME_INTEGRATION')!='8233':pytest.skip('仅本任务原人工制品',allow_module_level=True)
from data_pipeline.bgp.archive import admission as up; from data_pipeline.bgp.replay import snapshot_admission as cp, snapshot_access as ci
from data_pipeline.bgp.replay.snapshot_contract import ProjectionBinding, encode, TABLES as CTABLES
from data_pipeline.analysis.features import publication as fp, publication_io as fi
from data_pipeline.analysis.detection import publication as dp, publication_io as di
from tests.country.country_query_test_support import PgLogProbe

OUT=Path('/tmp/domeye-public-runtime-integration-8233').resolve()
FROOT=Path('/tmp/domeye-feature-m3-integration-8233/feature-m30').resolve()
MROOT=Path('/tmp/domeye-detection-m3-integration-8233/detection-m30').resolve()
CROOT=Path('/tmp/domeye-canonical-m3b-integration-8233').resolve()
DROOT=Path('/tmp/domeye-detection-lake-integration-8233/gap').resolve()
PGROOT=Path('/tmp/domeye-integration-detection-8233').resolve()
ROOTS=(FROOT,MROOT,CROOT,DROOT,PGROOT,OUT)
F=json.loads((FROOT/'本进程公开读取.json').read_text());FB=F['binding']
DQ=json.loads((DROOT/'request.json').read_text());CQ=json.loads((CROOT/'reader.json').read_text())
def dsn(text):return make_dsn(**{**parse_dsn(text),'host':str(PGROOT/'socket')})
FDSN=dsn(json.loads((FROOT/'绑定请求.json').read_text())['dsn']);MDSN=dsn(DQ['observation_dsn']);DDSN=dsn(DQ['detection_dsn'])
EVENTS=[];CACHE={}
class BoundProbe(PgLogProbe):
    def finish(self,offset):
        token=self.marker();text=self.wait_marker(offset,token);lines=text.splitlines(keepends=True)
        marker_index=next(i for i,line in enumerate(lines) if token in line);begin=marker_index
        while begin>0 and 'statement: BEGIN' not in lines[begin]:begin-=1
        snippet=''.join(lines[:begin]);count=sum('statement:' in line or 'execute ' in line for line in snippet.splitlines())
        # 仍由首尾marker证明日志实际绑定；前置拒绝合法产生0条业务SQL。
        return snippet,count
def save(name,value):(OUT/name).write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str))
def scratch(name):
    p=OUT/'scratch'/name;p.mkdir(exist_ok=True);return p
@contextmanager
def measured(name,connection):
    probe=BoundProbe(connection,OUT/'pg.log');offset=probe.start();EVENTS.clear();start=time.monotonic();cost={}
    try:yield cost
    finally:
        cost['wall_seconds']=time.monotonic()-start;log,n=probe.finish(offset);(OUT/(name+'.pg.log')).write_text(log)
        cost.update(pg_statements=n,event_counts=dict(Counter(e['kind'] for e in EVENTS)),process_lifetime_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),pg_peak_memory=None,physical_disk_read_bytes=None)
        save(name+'.cost.json',cost);save(name+'.events.json',EVENTS)
def snapshot():
    result={}
    for label,connection in [('feature',FDSN),('m2canonical',MDSN),('detection',DDSN)]:
        tables={}
        with closing(psycopg2.connect(connection)) as pg,pg.cursor() as c:
            for table in ('observation_m2.runs','observation_m2.checkpoints','observation_publication.m2_admissions','observation_publication.reference_admissions','feature.runs','feature.qualified_results','feature.source_commits',fp.REGISTRY,'m3_projection.runs',cp.REGISTRY,'detection.runs',dp.REGISTRY):
                c.execute('SELECT to_regclass(%s)',(table,));exists=c.fetchone()[0]
                if exists:
                    c.execute('SELECT to_jsonb(t) FROM '+table+' t');tables[table]=sorted([r[0] for r in c.fetchall()],key=lambda x:json.dumps(x,sort_keys=True))
                else:tables[table]=[]
        result[label]=tables
    return result
@pytest.fixture(scope='module',autouse=True)
def protection():
    prefix=os.environ.get('DOMEYE_PUBLIC_RUNTIME_REMAINING','')
    save(prefix+'原登记前.json',snapshot())
    yield
    before=json.loads((OUT/(prefix+'原登记前.json')).read_text());after=snapshot();save(prefix+'原登记后.json',after)
    for label,tables in before.items():
        for table,rows in tables.items():
            if 'admissions' in table:assert not (Counter(map(up.typed,rows))-Counter(map(up.typed,after[label][table])))
            else:assert after[label][table]==rows
    assert all(not list(p.iterdir()) for p in (OUT/'scratch').iterdir())

def dependencies(label,connection,run,snapshot,sids,refs,mode):
    key=(connection,run,snapshot,tuple(sids),tuple(refs),mode)
    if key in CACHE:return CACHE[key]
    if os.environ.get('DOMEYE_PUBLIC_RUNTIME_REMAINING'):
        path=OUT/(label+'-'+mode+'-dependencies.json')
        # 首轮缓存已按完整run/来源/参考选择复用Canonical的同一组真实依赖。
        if label=='Detection' and not path.exists():path=OUT/('Canonical-'+mode+'-dependencies.json')
        deps=json.loads(path.read_text());b=up.untyped(deps[0]['owner_binding'])
        assert (b['run_id'],b['snapshot'],b['ordered_source_ids'])==(run,snapshot,sids)
        assert [up.untyped(d['owner_binding'])['source_id'] for d in deps[1:]]==refs
        params=dict(dsn=connection,allowed_roots=ROOTS,scratch_root=scratch(label+'-up-'+mode),dependency_admissions=(deps[0],),audit_sink=EVENTS.append)
        if mode=='fixture':params['fixture_only']=True
        else:params.update(execution_profile='real-candidate/v1',input_manifest=b['plan']['manifest'],expected_m2_binding=b,memory_limit='256MB',max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,min_free_bytes=128*1024**2,lock_timeout_ms=2000)
        return up.Runtime(**params),tuple(deps)
    basic=up.Runtime(connection,ROOTS,scratch(label+'-up-'+mode),fixture_only=True,audit_sink=EVENTS.append)
    with measured(label+'-'+mode+'-M2inspect',connection):b=up.inspect_binding(basic,run,snapshot,sids)
    if mode=='fixture':u=basic
    else:u=up.Runtime(connection,ROOTS,basic.scratch_root,execution_profile='real-candidate/v1',input_manifest=b['plan']['manifest'],expected_m2_binding=b,memory_limit='256MB',max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,min_free_bytes=128*1024**2,lock_timeout_ms=2000,audit_sink=EVENTS.append)
    with measured(label+'-'+mode+'-M2admit',connection):ma=up.admit(u,b,guard=lambda:None)
    u.dependency_admissions=(ma,);deps=[ma]
    with measured(label+'-'+mode+'-references',connection):
        for source in refs:deps.append(up.admit(u,up.reference_binding(u,ma,source),guard=lambda:None))
    save(label+'-'+mode+'-dependencies.json',deps)
    CACHE[key]=(u,tuple(deps));return u,tuple(deps)
def consume(module,rt,a,request,label):
    values=[]
    with measured(label,rt.dsn) as cost:
        start=time.monotonic()
        with module.open_reader(rt,a,request,guard=lambda:None) as s:
            for chunk in s:
                cost.setdefault('first_batch_seconds',time.monotonic()-start)
                assert chunk['bytes']==len(chunk['rows_typed'].encode())
                values.extend((ci.untyped if module is cp else module.untyped)(chunk['rows_typed']))
            assert s.receipt is None
        assert s.receipt['execution']=='complete' and s.receipt['rows']==len(values)
    codec=encode if module is cp else module.typed
    save(label+'.typed.json',codec(values));save(label+'.receipt.json',s.receipt);return values
def frequest(view):return dict(view=view,scope_typed=up.typed(dict(mode='all',window_role='all')),codec_version=fi.CODEC,batch_rows=17,batch_bytes=4*1024**2)
def crequest(view):return dict(view=view,scope_typed=encode(dict(source_ids=None)),codec_version=cp.CODEC,batch_rows=100,batch_bytes=4*1024**2)
def drequest(view,pos=None):return dict(view=view,scope_typed=dp.typed(dict(start=0,stop=None,key=None,at_position=pos)),codec_version=dp.CODEC,batch_rows=100,batch_bytes=4*1024**2)
def cross_and_tail(module,runtimes,admissions,request,label,binding):
    for target,source in [('fixture','real'),('real','fixture')]:
        with measured(label+'-cross-'+target,runtimes[target].dsn):
            with pytest.raises(ValueError):module.verify_current(runtimes[target],admissions[source],guard=lambda:None)
    rt=runtimes['real'];wrong=deepcopy(binding)
    if module is fp:wrong['snapshot']+=1
    else:wrong['binding']['snapshot']+=1
    with pytest.raises(ValueError):rt.check_binding(wrong)
    original=rt.execution_profile
    try:
        with measured(label+'-tail-mode',rt.dsn):
            with pytest.raises(ValueError):
                with module.open_reader(rt,admissions['real'],request,guard=lambda:None) as s:list(s);rt.execution_profile='unknown'
            assert s.receipt is None
    finally:rt.execution_profile=original
    save(label+'-有限拒绝.json',dict(cross_modes=2,wrong_binding=True,tail_mode_drift=True,receipt=None))

def test_feature_modes_and_original_values():
    runtimes={};admissions={};seal=FB['specification']['observation_seals'][0]
    sids=[s['source_id'] for s in FB['specification']['ordered_bindings'][0]['sources']]
    for mode in ('fixture','real'):
        u,deps=dependencies('Feature',FDSN,seal['run_id'],seal['snapshot'],sids,[FB['specification']['reference_binding']['source_sha256']],mode)
        params=dict(dsn=FDSN,output_root=FROOT/'formal/output',allowed_roots=ROOTS,scratch_root=scratch('Feature-'+mode),dependency_admissions=deps,dependency_runtimes={a['admission_id']:u for a in deps},audit_sink=EVENTS.append)
        if mode=='fixture':params['fixture_only']=True
        else:params.update(execution_profile='real-candidate/v1',expected_feature_binding=FB,memory_limit='256MB',max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,min_free_bytes=128*1024**2,lock_timeout_ms=2000)
        rt=fp.Runtime(**params);runtimes[mode]=rt
        with measured('Feature-'+mode+'-admit',FDSN):a=fp.admit(rt,FB,guard=lambda:None)
        admissions[mode]=a;save('Feature-'+mode+'-Admission.json',a)
        with measured('Feature-'+mode+'-current-reuse',FDSN):fp.verify_current(rt,a,guard=lambda:None);assert fp.admit(rt,FB,guard=lambda:None)==a
        assert not any(e['kind']=='feature_full_audit' for e in EVENTS)
        for view in ('coverage',) if mode=='fixture' else ('windows','coverage'):
            values=consume(fp,rt,a,frequest(view),'Feature-'+mode+'-'+view)
            assert Counter(map(up.typed,values))==Counter(map(up.typed,F[view]))
    from data_pipeline.analysis.features.store import read_table
    from data_pipeline.analysis.features.qualification import PROFILE
    expected=json.loads(Path('/tmp/domeye-feature-p1-integration-8233/原12表全typed.json').read_text());raw={}
    with measured('Feature-real-原12表',FDSN):
        for table in fi.TABLES:
            raw[table]=[r for b in read_table(FDSN,FB['run_id'],FB['snapshot'],table,profile=PROFILE) for r in b.to_pylist()]
            assert Counter(map(up.typed,raw[table]))==Counter(map(up.typed,up.untyped(expected[table])))
    save('Feature-real-原12表.json',{k:up.typed(v) for k,v in raw.items()})
    cross_and_tail(fp,runtimes,admissions,frequest('coverage'),'Feature',FB)

def test_canonical_modes_and_default_original():
    desc=json.loads((CROOT/'公有descriptor与audit.json').read_text())['descriptor'];b=dict(binding=CQ['binding'],descriptor=desc)
    plan=desc['plan'];ib=plan['input_binding'];runtimes={};admissions={}
    expected=ci.untyped((CROOT/'新进程完整13表.typed.json').read_text())
    for mode in ('fixture','real'):
        u,deps=dependencies('Canonical',MDSN,ib['observation_run'],ib['seal_snapshot'],plan['selected_sources'],[r['source_id'] for r in plan['references']],mode)
        params=dict(dsn=MDSN,allowed_roots=ROOTS,scratch_root=scratch('Canonical-'+mode),dependency_admissions=deps,dependency_runtimes={a['admission_id']:u for a in deps},audit_sink=EVENTS.append)
        if mode=='fixture':params['fixture_only']=True
        else:params.update(execution_profile='real-candidate/v1',input_manifest=plan['input_manifest'],expected_canonical_binding=b,output_root=Path(desc['control_root']),memory_bytes=256*1024**2,max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,lock_timeout_ms=2000,max_batch_rows=10000,max_batch_bytes=4*1024**2,max_total_rows=1000000,max_total_bytes=512*1024**2,min_free_bytes=128*1024**2)
        rt=cp.Runtime(**params);runtimes[mode]=rt
        with measured('Canonical-'+mode+'-inspect',MDSN):assert cp.inspect_binding(rt,ProjectionBinding(**CQ['binding']),guard=lambda:None)==b
        with measured('Canonical-'+mode+'-admit',MDSN):a=cp.admit(rt,b,guard=lambda:None)
        admissions[mode]=a;save('Canonical-'+mode+'-Admission.json',a)
        with measured('Canonical-'+mode+'-current-reuse',MDSN):cp.verify_current(rt,a,guard=lambda:None);assert cp.admit(rt,b,guard=lambda:None)==a
        for table in ('source_coverage',) if mode=='fixture' else CTABLES:
            values=consume(cp,rt,a,crequest(table),'Canonical-'+mode+'-'+table)
            assert encode(values)==encode(expected[table]),table
    cross_and_tail(cp,runtimes,admissions,crequest('source_coverage'),'Canonical',b)

def test_detection_shared_helpers_and_original_gap():
    u,deps=dependencies('Detection',MDSN,DQ['input_run'],DQ['input_snapshot'],DQ['ordered_sources'],[r['source_id'] for r in DQ['references']],'fixture')
    rt=dp.Runtime(DDSN,DROOT/'output/business',ROOTS,scratch('Detection'),deps,{a['admission_id']:u for a in deps},fixture_only=True,audit_sink=EVENTS.append)
    ready=json.loads((DROOT/'output/business/ready.json').read_text())
    resume=os.environ.get('DOMEYE_PUBLIC_RUNTIME_REMAINING')=='Detection完成'
    if resume:
        a=json.loads((OUT/'Detection-Admission.json').read_text());b=dp.untyped(a['owner_binding'])
        assert (b['run_id'],b['snapshot'])==(ready['run_id'],ready['snapshot'])
    else:
        with measured('Detection-inspect',DDSN):b=dp.inspect_binding(rt,ready['run_id'],ready['snapshot'])
        with measured('Detection-admit',DDSN):a=dp.admit(rt,b,guard=lambda:None)
        save('Detection-Admission.json',a)
        with measured('Detection-current',DDSN):dp.verify_current(rt,a,guard=lambda:None)
    expected=json.loads(Path('/tmp/domeye-detection-lake-integration-8233/gap新湖实际全值.json').read_text())
    from datetime import datetime
    from data_pipeline.analysis.detection.lake_integrity import Inventory
    inventory=Inventory()
    for table in di.TABLES:
        # 原保存JSON使用default=str；仅按已冻结SQL schema恢复真实时间类型。
        for row in expected[table]:
            for name,kind in di.TABLES[table]:
                if kind=='TIMESTAMPTZ' and row[name] is not None:row[name]=datetime.fromisoformat(row[name])
        if resume and table=='records':values=dp.untyped(json.loads((OUT/'Detection-records.typed.json').read_text()))
        else:values=consume(dp,rt,a,drequest(table),'Detection-'+table)
        assert dp.typed(values)==dp.typed(expected[table])
        for row in values:inventory.add(table,row)
    assert inventory.result()==json.loads((DROOT/'fresh-reader.json').read_text())
    save('Detection-原完整typed交叉核对.json',inventory.result())
    pos=b['identity']['qualification_as_of_position']
    values=consume(dp,rt,a,drequest('qualified_revisions',pos),'Detection-qualified')
    from data_pipeline.analysis.detection.qualified_reader import read_qualified_revisions
    original=list(read_qualified_revisions(DDSN,b['run_id'],b['snapshot'],expected_binding_id=b['identity']['input_binding_id'],at_position=pos))
    assert dp.typed(values)==dp.typed(original)
    with pytest.raises(ValueError):dp.inspect_binding(rt,b['run_id'],b['snapshot']+1)
    original_limit=rt.max_rss_bytes
    try:
        with measured('Detection-tail-budget',DDSN):
            with pytest.raises(ValueError):
                with dp.open_reader(rt,a,drequest('records'),guard=lambda:None) as s:list(s);rt.max_rss_bytes=1
            assert s.receipt is None
    finally:rt.max_rss_bytes=original_limit
    save('Detection-有限拒绝.json',dict(wrong_snapshot=True,tail_budget=True,receipt=None,shared_path_private_state_absent=not hasattr(rt,'_root_bindings')))

@pytest.mark.parametrize('label',['Feature','Canonical'])
def test_remaining_cross_boundaries(label):
    if not os.environ.get('DOMEYE_PUBLIC_RUNTIME_REMAINING'):pytest.skip('只补已完成正向链之后的边界')
    runtimes={};admissions={}
    for mode in ('fixture','real'):
        a=json.loads((OUT/(label+'-'+mode+'-Admission.json')).read_text());admissions[mode]=a
        deps=json.loads((OUT/(label+'-'+mode+'-dependencies.json')).read_text());mb=up.untyped(deps[0]['owner_binding'])
        connection=FDSN if label=='Feature' else MDSN
        u,deps=dependencies(label,connection,mb['run_id'],mb['snapshot'],mb['ordered_source_ids'],[up.untyped(d['owner_binding'])['source_id'] for d in deps[1:]],mode)
        params=dict(dsn=connection,allowed_roots=ROOTS,scratch_root=scratch(label+'-'+mode),dependency_admissions=deps,dependency_runtimes={d['admission_id']:u for d in deps},audit_sink=EVENTS.append)
        if label=='Feature':
            b=fp.untyped(a['owner_binding']);params['output_root']=FROOT/'formal/output'
            if mode=='fixture':params['fixture_only']=True
            else:params.update(execution_profile='real-candidate/v1',expected_feature_binding=b,memory_limit='256MB',max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,min_free_bytes=128*1024**2,lock_timeout_ms=2000)
            module=fp;request=frequest('coverage')
        else:
            b=ci.untyped(a['owner_binding'])
            if mode=='fixture':params['fixture_only']=True
            else:params.update(execution_profile='real-candidate/v1',input_manifest=b['descriptor']['plan']['input_manifest'],expected_canonical_binding=b,output_root=Path(b['descriptor']['control_root']),memory_bytes=256*1024**2,max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,lock_timeout_ms=2000,max_batch_rows=10000,max_batch_bytes=4*1024**2,max_total_rows=1000000,max_total_bytes=512*1024**2,min_free_bytes=128*1024**2)
            module=cp;request=crequest('source_coverage')
        runtimes[mode]=module.Runtime(**params)
    cross_and_tail(module,runtimes,admissions,request,label,b)
