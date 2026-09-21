"""真实候选Runtime配置验证；仅复用自有人工冻结制品，不生产真实数据。"""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import time
from types import SimpleNamespace
import pytest

from data_pipeline.analysis.resources import publication as p
from data_pipeline.analysis.resources.publication_codec import typed, untyped, CODEC
from data_pipeline.bgp.archive import admission as up
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader


@pytest.fixture(scope='module')
def case():
    name=os.environ.get('DOMEYE_RESOURCE_REAL_CASE')
    out_name=os.environ.get('DOMEYE_RESOURCE_REAL_EVIDENCE')
    if not name or not out_name:pytest.skip('须显式自有健康制品与证据目录，不自动生产')
    root=Path(name);out=Path(out_name);scratch=out/'scratch'
    saved=json.loads((root/'case.json').read_text());dsn=saved['dsn'];report=saved['report'];events=[]
    reader=ResourceObservationReader(dsn,report['run_id'],report['snapshot'],report['dataset_id'])
    b=dict(binding=reader.manifest,receipt=reader.receipt,run_id=reader.run_id,snapshot=reader.snapshot,dataset_id=reader.dataset_id)
    hashes={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in root.rglob('*') if f.is_file()}
    fx=up.Runtime(dsn,(root,out),scratch,fixture_only=True,audit_sink=events.append)
    fixture_deps=[];real_deps=[];real_runtimes={};by_run={}
    for binding_id,full in b['binding']['observation_runs'].items():
        ids=[s['context']['source_id'] for s in b['binding']['sources'] if b['binding']['observation_inputs'][s['context']['source_id']]['binding_id']==binding_id]
        ub=up.inspect_binding(fx,full['binding']['observation_run'],full['binding']['seal_snapshot'],ids)
        fixture_deps.append(up.admit(fx,ub,guard=lambda:None))
        real=up.Runtime(dsn,(root,out),scratch,execution_profile='real-candidate/v1',input_manifest=ub['plan']['manifest'],expected_m2_binding=ub,
            memory_limit='256MB',max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,min_free_bytes=128*1024**2,lock_timeout_ms=2000)
        dep=up.admit(real,ub,guard=lambda:None);real.dependency_admissions=(dep,)
        real_deps.append(dep);real_runtimes[dep['admission_id']]=real;by_run[ub['run_id']]=(real,dep)
    fx.dependency_admissions=tuple(fixture_deps)
    csv=b['binding']['csv_reference'];fm=next(d for d in fixture_deps if up.untyped(d['owner_binding'])['run_id']==csv['run_id'])
    fixture_deps.append(up.admit(fx,up.reference_binding(fx,fm,csv['source_id']),guard=lambda:None))
    real,dep=by_run[csv['run_id']];reference=up.admit(real,up.reference_binding(real,dep,csv['source_id']),guard=lambda:None)
    real_deps.append(reference);real_runtimes[reference['admission_id']]=real
    params=dict(dsn=dsn,allowed_roots=(root,out),scratch_root=scratch,upstream_runtime=None,dependency_admissions=tuple(real_deps),
        dependency_runtimes=real_runtimes,execution_profile='real-candidate/v1',expected_resource_binding=b,output_root=root/'resource',
        max_rows=1000000,max_bytes=256*1024**2,max_rss_bytes=2*1024**3,memory_bytes=256*1024**2,max_temp_bytes=512*1024**2,
        min_free_bytes=128*1024**2,lock_timeout_ms=2000,audit_sink=events.append)
    fixture=p.Runtime(dsn,(root,out),scratch,fx,tuple(fixture_deps),fixture_only=True,audit_sink=events.append)
    return SimpleNamespace(root=root,out=out,scratch=scratch,b=b,params=params,fixture=fixture,hashes=hashes,events=events)


def runtime(case,**changes):return p.Runtime(**{**case.params,**changes})

def request(view='metrics'):return dict(view=view,scope_typed=typed({'scope':'all'}),codec_version=CODEC,batch_rows=7,batch_bytes=1048576)


def test_explicit_mode_scope_budgets_and_no_processing_deadline(case,monkeypatch):
    for changes in ({'execution_profile':None},{'fixture_only':True},{'expected_resource_binding':None},{'dependency_runtimes':{}},{'output_root':case.root},{'scratch_root':case.root/'resource'}):
        with pytest.raises(ValueError):runtime(case,**changes)
    for key in ('max_rows','max_bytes','max_rss_bytes','memory_bytes','max_temp_bytes','min_free_bytes','lock_timeout_ms'):
        for value in (None,True,float('nan'),float('inf'),0):
            with pytest.raises(ValueError):runtime(case,**{key:value})
    for value in (300,1.5,True,float('inf')):
        with pytest.raises(ValueError,match='总时长'):runtime(case,max_seconds=value)
    rt=runtime(case);assert rt.max_seconds is None
    guard=rt.budget(lambda:None)
    with monkeypatch.context() as patch:
        patch.setattr(p.time,'monotonic',lambda:10**15);guard()
    assert p.inspect_binding(rt,case.b['run_id'],case.b['snapshot'],case.b['dataset_id'])==case.b
    with pytest.raises(ValueError,match='固定'):p.inspect_binding(rt,case.b['run_id'],case.b['snapshot']+1,case.b['dataset_id'])
    rt.expected_resource_binding['snapshot']+=1
    with pytest.raises(ValueError,match='漂移'):rt.check_binding(case.b)


@pytest.fixture(scope='module')
def admitted(case):
    rt=runtime(case);times={}
    def measure(name,fn):
        begin=time.monotonic();result=fn();times[name]=time.monotonic()-begin;return result
    fixture=measure('fixture_admit',lambda:p.admit(case.fixture,case.b,guard=lambda:None))
    real=measure('real_admit',lambda:p.admit(rt,case.b,guard=lambda:None))
    case.events.clear()
    assert measure('real_reuse',lambda:p.admit(rt,case.b,guard=lambda:None))==real
    measure('real_current',lambda:p.verify_current(rt,real,guard=lambda:None))
    assert not any(e['kind'] in ('inventory_scan','science_compare','entity_hash') for e in case.events)
    (case.out/'Admission.json').write_text(json.dumps(real,ensure_ascii=False))
    (case.out/'依赖Admission.json').write_text(json.dumps(list(rt.dependency_admissions),ensure_ascii=False))
    return rt,fixture,real,times


def test_modes_and_complete_bindings_are_not_interchangeable(case,admitted):
    rt,fx,a,_=admitted
    assert a['admission_id']!=fx['admission_id'] and a['validator']['rules_digest']!=fx['validator']['rules_digest']
    for target,wrong in [(rt,fx),(case.fixture,a)]:
        with pytest.raises(ValueError):p.verify_current(target,wrong,guard=lambda:None)
        lock=next(t for t in wrong['lock_targets'] if t['namespace']=='resource.run')
        with pytest.raises(ValueError):
            with p.hold_lock(target,wrong,lock,guard=lambda:None):pass
    wrong=runtime(case);key=next(iter(wrong.dependency_runtimes));old=wrong.dependency_runtimes[key]
    try:
        wrong.dependency_runtimes[key]=case.fixture.upstream_runtime
        with pytest.raises(ValueError,match='模式'):p.verify_current(wrong,a,guard=lambda:None)
    finally:wrong.dependency_runtimes[key]=old
    b=deepcopy(case.b);b['binding']['sources'][0]['purpose']='unknown'
    with pytest.raises(ValueError):p.admit(rt,b,guard=lambda:None)


def test_actual_resources_and_scratch_identity(case,admitted):
    _,_,a,_=admitted
    for field,value in [('max_rss_bytes',1),('min_free_bytes',2**63-1),('max_rows',True)]:
        rt=runtime(case);setattr(rt,field,value)
        with pytest.raises(ValueError):p.verify_current(rt,a,guard=lambda:None)
    rt=runtime(case);before=case.scratch.with_name('scratch-before')
    case.scratch.rename(before);case.scratch.mkdir()
    try:
        with pytest.raises(ValueError,match='身份漂移'):p.verify_current(rt,a,guard=lambda:None)
    finally:case.scratch.rmdir();before.rename(case.scratch)
    rt=runtime(case);case.scratch.rename(before);case.scratch.symlink_to(case.root/'resource',target_is_directory=True)
    try:
        with pytest.raises(ValueError):p.verify_current(rt,a,guard=lambda:None)
    finally:case.scratch.unlink();before.rename(case.scratch)
    layout=case.b['receipt']['storage_layout']
    with pytest.raises(ValueError,match='隔离'):
        runtime(case,scratch_root=Path(layout['catalog_data_path'])/layout['schema_path'])


def test_original_views_and_files_unchanged(case,admitted):
    rt,fx,a,times=admitted;reads=[]
    old=ResourceObservationReader(rt.dsn,case.b['run_id'],case.b['snapshot'],case.b['dataset_id'])
    for view in ('metrics','normal_bands','topology_status','coverage'):
        begin=time.monotonic();first=None;actual=[]
        with p.open_reader(rt,a,request(view),guard=lambda:None) as session:
            for batch in session:
                if first is None:first=time.monotonic()-begin
                actual.extend(untyped(batch['rows_typed']))
        seconds=time.monotonic()-begin
        expected=[r for b in old.coverage(scope='all') for r in b.to_pylist()] if view=='coverage' else list(old.values(target=view,scope='all'))
        def normalize(r):return typed({**r,'qualification':sorted(r['qualification'],key=typed)}) if 'qualification' in r else typed(r)
        assert sorted(map(normalize,actual))==sorted(map(normalize,expected))
        assert session.receipt and session.receipt['rows']==len(actual)
        reads.append(dict(view=view,rows=len(actual),seconds=seconds,first_batch_seconds=first,receipt=session.receipt))
    assert case.hashes=={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in case.root.rglob('*') if f.is_file()}
    assert not list(case.scratch.iterdir())
    (case.out/'验证.json').write_text(json.dumps(dict(times=times,reads=reads,usage=rt.resource_usage,files_unchanged=len(case.hashes),run_id=case.b['run_id'],actual_total_data_processing_deadline=None),ensure_ascii=False))
