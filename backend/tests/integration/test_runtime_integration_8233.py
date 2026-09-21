"""复用8233原健康M2：Runtime真实候选控制路径的人工集成。"""
import os
import json
import copy
import time
import hashlib
from pathlib import Path
from contextlib import contextmanager,closing
from collections import Counter
from types import SimpleNamespace
import pytest
import psycopg2
if os.environ.get('DOMEYE_RUNTIME_INTEGRATION')!='8233':pytest.skip('仅显式自有人工Runtime集成',allow_module_level=True)
from data_pipeline.bgp.archive import admission as p
from data_pipeline.bgp.archive.value_codec import typed, untyped
from tests.observations.test_observation_publication_real import req, test_actual_scope_rejection_and_internal_resources as scope_checks
from tests.country.country_query_test_support import PgLogProbe
OUT=Path('/tmp/domeye-runtime-integration-8233').resolve()
OLD=Path('/tmp/domeye-m2-p1-integration-8233').resolve()
ROOT=Path('/tmp/domeye-detection-m3-integration-8233/detection-m30').resolve()
SCRATCH=OUT/'scratch'
Q=json.loads((ROOT/'request.json').read_text());DSN=Q['observation_dsn']
B=json.loads((OLD/'原绑定.json').read_text())
EVENTS=[]
PARAMS=dict(dsn=DSN,allowed_roots=(ROOT,OUT),scratch_root=SCRATCH,audit_sink=EVENTS.append,memory_limit='256MB',max_temp_bytes=512*1024**2,lock_timeout_ms=2000,max_rss_bytes=4*1024**3,min_free_bytes=128*1024**2)
def save(name,value):(OUT/name).write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str))
def files():return {str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in ROOT.rglob('*') if f.is_file()}
def runtime(real=True):return p.Runtime(**PARAMS,**(dict(execution_profile='real-candidate/v1',input_manifest=B['plan']['manifest'],expected_m2_binding=B) if real else dict(fixture_only=True)))
def metadata():
    with closing(psycopg2.connect(DSN)) as pg,pg.cursor() as c:
        c.execute('SELECT row_to_json(r) FROM observation_m2.runs r WHERE run_id=%s',(Q['input_run'],));run=c.fetchone()[0]
        c.execute('SELECT ordinal,payload FROM observation_m2.checkpoints WHERE run_id=%s ORDER BY ordinal',(Q['input_run'],));cp=c.fetchall()
        admissions={}
        for table in ('m2_admissions','reference_admissions'):
            c.execute('SELECT record_key,admission FROM observation_publication.'+table+' ORDER BY record_key');admissions[table]=dict(c.fetchall())
    return dict(run=run,checkpoints=cp,admissions=admissions)
@contextmanager
def measure(name,pg=True):
    probe=PgLogProbe(DSN,OUT/'pg.log') if pg else None
    offset=probe.start() if probe else None;EVENTS.clear();start=time.monotonic();info={}
    try:yield info
    finally:
        info['wall_seconds']=time.monotonic()-start
        if probe:
            log,count=probe.finish(offset);(OUT/(name+'.pg.log')).write_text(log);info['pg_statements']=count
        else:info['pg_statements']=None
        info.update(event_counts=dict(Counter(e['kind'] for e in EVENTS)),parquet_bytes=sum(e.get('bytes',0) for e in EVENTS if e['kind']=='parquet_read'),decoded_rows=sum(e.get('rows',0) for e in EVENTS if e['kind']=='decoded_batch'),temporary_disk_observed_max=max([e.get('bytes',0) for e in EVENTS if e['kind']=='temporary_disk'] or [0]))
        save(name+'.cost.json',info);save(name+'.events.json',EVENTS)
def read(rt,a,table,batch=1,first=None):
    rows=[];start=time.monotonic()
    with p.open_reader(rt,a,req(a,table,batch),guard=lambda:None) as session:
        for chunk in session:
            if first is not None and not first:first.update(seconds=time.monotonic()-start,events=list(EVENTS))
            rows.extend(untyped(chunk['rows_typed']))
        assert session.receipt is None
    assert session.receipt['execution']=='complete'
    return rows,session.receipt
@pytest.fixture(scope='module')
def case():
    before=metadata()
    if not (OUT/'原登记前.json').exists():save('原登记前.json',before);save('原文件SHA.json',files())
    with measure('real构造含参考解释' if not (OUT/'real构造含参考解释.cost.json').exists() else '复核构造含参考解释',pg=False) as cost:
        rt=runtime();cost['resource_usage']=copy.deepcopy(rt.resource_usage)
    synthetic=runtime(False)
    params={**PARAMS,'execution_profile':'real-candidate/v1','input_manifest':B['plan']['manifest'],'expected_m2_binding':B}
    result=SimpleNamespace(rt=rt,synthetic=synthetic,params=params,binding=B,root=ROOT,scratch=SCRATCH,out=OUT,before=before)
    yield result
    after=metadata();save('最终登记.json',after)
    assert after['run']==before['run'] and after['checkpoints']==before['checkpoints']
    for table,records in before['admissions'].items():
        for k,v in records.items():assert after['admissions'][table][k]==v
    assert files()==json.loads((OUT/'原文件SHA.json').read_text()) and not list(SCRATCH.iterdir())

def test_01_mode_manifest_binding(case):
    from jsonschema import ValidationError
    for change in [dict(execution_profile=None),dict(execution_profile='unknown'),dict(fixture_only=True)]:
        with pytest.raises(ValueError):p.Runtime(**{**case.params,**change})
    for name in ('memory_limit','max_temp_bytes','lock_timeout_ms','max_rss_bytes','min_free_bytes'):
        with pytest.raises(ValueError,match='显式资源'):p.Runtime(**{**case.params,name:None})
    errors={}
    for name in ('collector','uri','order','reference'):
        manifest=copy.deepcopy(case.params['input_manifest'])
        if name=='collector':manifest['collector']='foreign'
        elif name=='uri':manifest['inputs'][0]['origin_uri']+='-wrong'
        elif name=='order':manifest['inputs']=list(reversed(manifest['inputs']))
        else:manifest['references'][0]['sha256']='f'*64
        assert manifest!=case.params['input_manifest']
        with pytest.raises((ValueError,ValidationError)) as caught:p.Runtime(**{**case.params,'input_manifest':manifest})
        errors[name]=str(caught.value)
    scope_checks(case)
    save('实际manifest冲突.json',errors)
    assert p.inspect_binding(case.rt,B['run_id'],B['snapshot'],B['ordered_source_ids'])==B
    save('模式与原绑定.json',dict(binding=B,mode_manifest_binding_checks=True))

@pytest.fixture(scope='module')
def accepted(case):
    old=[json.loads((OLD/name).read_text()) for name in ('M2_Admission.json','reference_Admission.json')]
    for a in old:
        with pytest.raises(ValueError,match='源码版本'):p.verify_current(case.synthetic,a,guard=lambda:None)
    result={}
    for mode,rt in [('fixture',case.synthetic),('real',case.rt)]:
        with measure(mode+'首次M2准入'):a=p.admit(rt,B,guard=lambda:None)
        rt.dependency_admissions=(a,)
        rb=p.reference_binding(rt,a,B['reference_sources'][0]['source_id'])
        with measure(mode+'首次reference准入'):r=p.admit(rt,rb,guard=lambda:None)
        for label,admission in [('m2',a),('reference',r)]:
            save(mode+'_'+label+'_Admission.json',admission)
            with measure(mode+'_'+label+'复用'):assert p.admit(rt,untyped(admission['owner_binding']),guard=lambda:None)==admission
            with measure(mode+'_'+label+'current'):p.verify_current(rt,admission,guard=lambda:None)
        result[mode]=(rt,a,r)
    assert result['fixture'][1]['validator']['rules_digest']!=result['real'][1]['validator']['rules_digest']
    assert len({p._own_target(a)['key'] for _,a,r in result.values() for a in (a,r)})==4
    return result

def test_02_current_modes_and_full_original(case,accepted):
    for rt,a,r in accepted.values():
        assert untyped(a['owner_binding'])==B and untyped(r['owner_binding'])['selected_sources']==[]
        assert B['selected_sources'][0]['upstream_rank']==0 and B['reference_sources'][0]['checkpoint_ordinal']==0
    rt,a,r=accepted['real'];other,fa,fr=accepted['fixture']
    for target,admission in [(rt,fa),(other,a)]:
        with pytest.raises(ValueError,match='模式'):p.verify_current(target,admission,guard=lambda:None)
        with pytest.raises(ValueError,match='模式'):
            with p.hold_lock(target,admission,p._own_target(admission),guard=lambda:None):pass
        with pytest.raises(ValueError,match='模式'):
            with p.open_reader(target,admission,req(admission),guard=lambda:None):pass
    with pytest.raises(ValueError,match='模式'):p.reference_binding(rt,fa,B['reference_sources'][0]['source_id'])
    summary={}
    for admission in (a,r):
        for table in (tuple(p.OBS_TABLES) if admission['owner']=='m2' else ('references',)):
            label=admission['owner']+'_'+table;first={}
            with measure(label+'全读') as cost:
                rows,receipt=read(rt,admission,table,first=first);cost['first_batch']=first;cost['resource_usage']=copy.deepcopy(rt.resource_usage)
            expected=(OLD/(label+'_1.typed.json')).read_text();assert typed(rows)==expected
            (OUT/(label+'.typed.json')).write_text(typed(rows));save(label+'.receipt.json',receipt)
            summary[label]=dict(rows=len(rows),typed_digest=receipt['typed_digest'])
    save('原全八表与reference同序.json',summary)

def test_03_actual_single_locks(case,accepted):
    rt,a,r=accepted['real'];evidence=[]
    for admission in (a,r):
        target=p._own_target(admission);table=p.TABLES[admission['owner']+'.admission']
        def update(expect_failure):
            with closing(psycopg2.connect(DSN)) as pg,pg.cursor() as c:
                c.execute("SET LOCAL lock_timeout='50ms'")
                if expect_failure:
                    with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute('UPDATE '+table+' SET state=state WHERE record_key=%s',(target['key'],))
                else:c.execute('UPDATE '+table+' SET state=state WHERE record_key=%s',(target['key'],))
                pg.rollback()
        with measure(admission['owner']+'实际单锁'):
            with p.hold_lock(rt,admission,target,guard=lambda:None):
                assert sum('FOR SHARE' in e.get('sql','') for e in EVENTS)==1;update(True)
            update(False)
        evidence.append(dict(target=target,blocked=True,after_release=True))
    save('两实际单锁.json',evidence)

@pytest.mark.parametrize('name',['max_rss_bytes','min_free_bytes','max_temp_bytes'])
def test_04_first_batch_nan_and_finite(case,accepted,name):
    rt,a,r=accepted['real'];original=getattr(rt,name);evidence=[]
    for admission in (a,r):
        rows=[];session=None;start=time.monotonic()
        try:
            with pytest.raises(ValueError,match='有限正值') as caught:
                with p.open_reader(rt,admission,req(admission,'references' if admission is r else 'messages',1),guard=lambda:None) as session:
                    rows.extend(untyped(next(session)['rows_typed']));setattr(rt,name,float('nan'))
                    for chunk in session:rows.extend(untyped(chunk['rows_typed']))
            assert session.receipt is None
            evidence.append(dict(owner=admission['owner'],rows_delivered=len(rows),error=str(caught.value),receipt=None,wall_seconds=time.monotonic()-start))
        finally:setattr(rt,name,original)
        assert not list(SCRATCH.iterdir())
    bad={'max_rss_bytes':1,'min_free_bytes':2**63-1,'max_temp_bytes':1}[name]
    session=None
    try:
        with pytest.raises(ValueError) as caught:
            with p.open_reader(rt,a,req(a,batch=1),guard=lambda:None) as session:
                next(session);setattr(rt,name,bad);list(session)
        assert session.receipt is None
        evidence.append(dict(finite_excess=bad,error=str(caught.value),receipt=None))
    finally:setattr(rt,name,original)
    try:
        setattr(rt,name,original+1)
        rows,receipt=read(rt,a,'messages');assert typed(rows)==(OLD/'m2_messages_1.typed.json').read_text()
        evidence.append(dict(finite_valid=original+1,receipt=receipt))
    finally:setattr(rt,name,original)
    save(name+'使用点.json',evidence)

def test_05_tail_cleanup_no_receipt(case,accepted):
    rt,a,r=accepted['real'];original=rt.max_temp_bytes;tail=False
    def guard():
        if tail:rt.max_temp_bytes=float('nan')
    try:
        with pytest.raises(ValueError,match='有限正值') as caught:
            with p.open_reader(rt,a,req(a),guard=guard) as session:list(session);tail=True
        assert session.receipt is None
    finally:rt.max_temp_bytes=original
    assert not list(SCRATCH.iterdir())
    p.verify_current(rt,a,guard=lambda:None);p.verify_current(rt,r,guard=lambda:None)
    save('尾guard拒绝与清理.json',dict(error=str(caught.value),receipt=None,scratch_empty=True,current_after_restore=True))
