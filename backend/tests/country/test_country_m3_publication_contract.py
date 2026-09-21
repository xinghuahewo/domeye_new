"""公开接口的有限作用域、预算及早停合同；不伪造实际准入。"""
from dataclasses import replace
from types import SimpleNamespace
import inspect
import pytest

from data_pipeline.analysis.country_events.result_admission import Runtime
from data_pipeline.analysis.country_events.qualified_reader import ResultLimits, request_scope, open_qualified_country_result


def descriptor():
    return SimpleNamespace(window_us=(10,20),logical_run_id='logical',input_binding_id='input',
                           binding=SimpleNamespace(result_id='result',read_model_id='read'))


@pytest.mark.parametrize('kw',[
    {'window_us':(10.0,20)}, {'window_us':(True,20)}, {'window_us':(10,21)},
    {'window_us':(10,20),'revision':1}, {'window_us':(10,20),'after_sequence':True},
    {'window_us':(10,20),'dimension':'invented'},
])
def test_scope_rejects_implicit_conversion_or_unbound_selection(kw):
    with pytest.raises(ValueError): request_scope(descriptor(),'events',**kw)


def test_no_public_lock_bypass_or_private_source_verifier():
    signature=inspect.signature(open_qualified_country_result)
    assert '_locks_held' not in signature.parameters
    assert all(p.kind!=p.VAR_KEYWORD for p in signature.parameters.values())


def test_runtime_requires_artificial_permission_and_finite_budget(tmp_path):
    args=dict(dsn=f'host={tmp_path} dbname=fixture',output_root=tmp_path,allowed_roots=(tmp_path,),scratch_root=tmp_path,
              dependency_admissions=(),dependency_runtimes={})
    with pytest.raises(ValueError,match='fixture'): Runtime(**args)
    with pytest.raises(ValueError,match='country_invalid_limit'):
        Runtime(**args,fixture_only=True,limits=replace(ResultLimits(),max_references=True))
    a=Runtime(**args,fixture_only=True); b=Runtime(**args,fixture_only=True)
    a._read_stats['rows']=3
    assert b._read_stats['rows']==0


def test_shared_reader_peaks_are_max_and_work_is_additive():
    from collections import Counter
    from data_pipeline.analysis.country_events.qualified_reader import ResultReader
    shared=Counter(rows=7,references=11,bytes=101);closed=[]
    original=dict(shared)
    for rss,working,cache,reads,io in ((100,40,20,2,50),(80,60,10,3,70)):
        reader=ResultReader.__new__(ResultReader)
        reader.stats=shared;reader.db=SimpleNamespace(close=lambda:closed.append('db'))
        reader.access=SimpleNamespace(stats=Counter(peak_cache_bytes=cache,row_groups=reads,bytes=io),close=lambda:closed.append('access'))
        reader.budget=SimpleNamespace(stats=Counter(process_peak_rss_bytes=rss,peak_working_bytes=working,checks=1))
        reader.close();snapshot=dict(shared);reader.close();assert dict(shared)==snapshot
    assert shared['budget_process_peak_rss_bytes']==100
    assert shared['budget_peak_working_bytes']==60
    assert shared['physical_peak_cache_bytes']==20
    assert shared['physical_row_groups']==5 and shared['physical_bytes']==120
    assert shared['budget_checks']==2
    assert all(shared[k]==v for k,v in original.items())
    assert closed==['access','db','access','db']
