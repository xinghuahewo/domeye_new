"""S1四项独立复核反例；公开入口、真实临时SQLite、人工输入。"""
from dataclasses import replace
from fractions import Fraction
import importlib
import pytest

from data_pipeline.analysis.country_trends import compute_trends, TrendCompletion, FixtureBatch
from data_pipeline.analysis.country_trends.contract import Projection, ReferenceInput
from data_pipeline.analysis.country_events import event_aggregation as c2
from data_pipeline.analysis.country_events.compute import MetricPoint
from tests.country_trend.test_country_trend_s1 import fixture, stream, run, select, EVENT, reference_case


class ClosingStream:
    def __init__(self, inner, error=None):
        self.inner=iter(inner);self.error=error;self.closed=False
    def __iter__(self):return self
    def __next__(self):return next(self.inner)
    def close(self):
        self.closed=True
        close=getattr(self.inner,'close',None)
        if close:close()
        if self.error:raise self.error


def test_normal_input_close_failure_has_no_completion(tmp_path):
    request,items=fixture(())
    source=ClosingStream(stream(request,items),RuntimeError('input-close'))
    seen=[]
    with pytest.raises(RuntimeError,match='input-close'):
        for item in compute_trends(request,source,temporary_parent=tmp_path):seen.append(item)
    assert source.closed and not list(tmp_path.iterdir())
    assert not any(isinstance(x,TrendCompletion) for x in seen)


@pytest.mark.parametrize('value,denominator',[(8,20),(1,3),(2,5)])
def test_integer_scientific_ratio(value,denominator):
    request,items=fixture()
    def changed(integer):
        output=[]
        for r in items:
            if isinstance(r,c2.C2Row) and isinstance(r.value,MetricPoint) and r.value.metric=='invisible_direction_count':
                v=value if integer else Fraction(value)
                r=replace(r,value=replace(r.value,value=v,known_lower_bound=v,denominator=denominator,ratio=Fraction(value,denominator),state='calculated'))
            output.append(r)
        return output
    left,_=run(request,changed(True));right,_=run(request,changed(False))
    a=[r.values for r in left if r.kind in ('point','peak','profile') and r.key[0]=='invisible_direction_count']
    b=[r.values for r in right if r.kind in ('point','peak','profile') and r.key[0]=='invisible_direction_count']
    assert a==b


def test_missing_projection_cohort_not_comparable():
    request,items,p=reference_case()
    # 原接口没有cohort字段；修复接口显式缺失亦不能通过。
    if hasattr(p,'cohort_id'):p=replace(p,cohort_id=None)
    reference=ReferenceInput(EVENT,('fixture','reference'),'US',(p,replace(p,country='CA')))
    rows,_=run(replace(request,references=(reference,)),items)
    assert select(rows,'reference_context').get('state')=='insufficient_data'
    assert not any(r.kind=='reference_cdf' for r in rows)


def test_points_and_phases_have_graph_navigation():
    rows,_=run(*fixture())
    promised=[r for r in rows if r.kind in ('point','phase')]
    evidence={(r.get('source_kind'),r.get('source_key')) for r in rows if r.kind=='evidence'}
    assert all((r.kind,r.key) in evidence for r in promised)


def failing_sqlite_close(monkeypatch):
    module=importlib.import_module('data_pipeline.analysis.country_trends.compute')
    connect=module.sqlite3.connect
    calls=[]
    class Connection:
        def __init__(self,real):self.real=real
        def __getattr__(self,name):return getattr(self.real,name)
        def close(self):
            self.real.close()
            calls.append('sqlite-closed')
            raise RuntimeError('sqlite-close')
    monkeypatch.setattr(module.sqlite3,'connect',lambda *args,**kwargs:Connection(connect(*args,**kwargs)))
    return calls


def test_normal_sqlite_close_failure_has_no_completion(tmp_path,monkeypatch):
    calls=failing_sqlite_close(monkeypatch)
    request,items=fixture(())
    source=ClosingStream(stream(request,items))
    seen=[]
    with pytest.raises(RuntimeError,match='sqlite-close') as caught:
        for item in compute_trends(request,source,temporary_parent=tmp_path):seen.append(item)
    assert calls==['sqlite-closed'] and source.closed
    assert len(caught.value.cleanup_errors)==1
    assert not any(isinstance(x,TrendCompletion) for x in seen) and not list(tmp_path.iterdir())


def test_primary_and_both_close_failures_retained(tmp_path,monkeypatch):
    calls=failing_sqlite_close(monkeypatch)
    request,_=fixture(())
    source=ClosingStream([object()],RuntimeError('input-close'))
    with pytest.raises(ValueError,match='trend_fixture_batch') as caught:
        list(compute_trends(request,source,temporary_parent=tmp_path))
    assert calls==['sqlite-closed'] and source.closed and not list(tmp_path.iterdir())
    assert [str(e) for e in caught.value.cleanup_errors]==['sqlite-close','input-close']
    import traceback
    assert 'trend_fixture_batch' in ''.join(traceback.format_exception(type(caught.value),caught.value,caught.value.__traceback__))


def test_temporary_cleanup_failure_still_closes_input(tmp_path,monkeypatch):
    module=importlib.import_module('data_pipeline.analysis.country_trends.compute')
    factory=module.tempfile.TemporaryDirectory
    class Directory:
        def __init__(self,*args,**kwargs):self.real=factory(*args,**kwargs);self.name=self.real.name
        def cleanup(self):self.real.cleanup();raise RuntimeError('directory-cleanup')
    monkeypatch.setattr(module.tempfile,'TemporaryDirectory',Directory)
    request,items=fixture(())
    source=ClosingStream(stream(request,items))
    seen=[]
    with pytest.raises(RuntimeError,match='directory-cleanup'):
        for item in compute_trends(request,source,temporary_parent=tmp_path):seen.append(item)
    assert source.closed and not list(tmp_path.iterdir())
    assert not any(isinstance(x,TrendCompletion) for x in seen)


def test_early_close_reports_cleanup_failure(tmp_path):
    request,items=fixture()
    source=ClosingStream(stream(request,items),RuntimeError('input-close'))
    generator=compute_trends(request,source,temporary_parent=tmp_path)
    assert not isinstance(next(generator),TrendCompletion)
    with pytest.raises(RuntimeError,match='input-close') as caught:generator.close()
    assert isinstance(caught.value.interrupted_by,GeneratorExit)
    assert source.closed and not list(tmp_path.iterdir())


def test_entry_failure_acquires_no_resources(tmp_path,monkeypatch):
    module=importlib.import_module('data_pipeline.analysis.country_trends.compute')
    request,items=fixture()
    calls=[]
    monkeypatch.setattr(module.sqlite3,'connect',lambda *args,**kwargs:calls.append(True))
    source=ClosingStream(stream(request,items))
    with pytest.raises(ValueError,match='fixture_only'):
        list(compute_trends(replace(request,data_kind='real'),source,temporary_parent=tmp_path))
    assert calls==[] and not source.closed and not list(tmp_path.iterdir())
    # 未调用iter(stream)，所有权尚未接管；调用者自己的已有流仍由调用者关闭。
    source.close()


def test_normal_completion_follows_all_cleanup(tmp_path):
    request,items=fixture(())
    source=ClosingStream(stream(request,items))
    tails=[]
    for item in compute_trends(request,source,temporary_parent=tmp_path):
        if isinstance(item,TrendCompletion):
            assert source.closed and not list(tmp_path.iterdir())
            tails.append(item)
    assert len(tails)==1


@pytest.mark.parametrize('denominator',[None,0])
def test_null_and_zero_denominator_remain_no_ratio(denominator):
    request,items=fixture()
    changed=[]
    for r in items:
        if isinstance(r,c2.C2Row) and isinstance(r.value,MetricPoint) and r.value.metric=='invisible_direction_count':
            r=replace(r,value=replace(r.value,value=0,known_lower_bound=0,denominator=denominator,ratio=None,state='calculated_zero'))
        changed.append(r)
    rows,_=run(request,changed)
    assert all(r.get('raw').ratio is None for r in rows if r.kind=='metric' and r.key[0]=='invisible_direction_count')


def positive_reference():
    from data_pipeline.analysis.country_trends.contract import REFERENCE_DEFINITION, ActivityWindow
    request,items,p=reference_case()
    p=replace(p,cohort_id='fixture-cohort',definition_binding=REFERENCE_DEFINITION)
    other=replace(p,country='CA',cohort_id='fixture-cohort-CA',source_ref='fixture-CA')
    ref=ReferenceInput(EVENT,('fixture','reference'),'US',(p,other))
    activity=ActivityWindow(EVENT,'ordinary','announ_num','accepted_announce_elements',1_000_000,2_000_000,Fraction(7),'complete','fixture-activity')
    return replace(request,feature_binding=('fixture','feature'),activities=(activity,),references=(ref,)),items


def test_reference_distinct_cohorts_same_definition_positive():
    request,items=positive_reference()
    rows,_=run(request,items)
    assert select(rows,'reference_context').get('state')=='complete'
    assert select(rows,'reference_cdf',('decline_pp',)).get('percentile')==100
    assert {r.get('cohort_id') for r in rows if r.kind=='reference_country'}=={'fixture-cohort','fixture-cohort-CA'}
    assert any(r.kind=='reference_shape' for r in rows) and any(r.kind=='reference_common' for r in rows)


@pytest.mark.parametrize('field,value,error',[('cohort_id','wrong-cohort','target_cohort'),('definition_binding',('wrong-definition',),'target_definition')])
def test_reference_target_explicit_binding_mismatch(field,value,error):
    request,items=positive_reference()
    ref=request.references[0]
    ref=replace(ref,projections=(replace(ref.projections[0],**{field:value}),ref.projections[1]))
    with pytest.raises(ValueError,match=error):run(replace(request,references=(ref,)),items)


@pytest.mark.parametrize('field,value,reason',[('cohort_id',None,'cohort_unavailable'),('definition_binding',(),'definition_unavailable'),('definition_binding',('wrong',),'definition_mismatch')])
def test_other_reference_binding_exclusion(field,value,reason):
    request,items=positive_reference()
    ref=request.references[0]
    ref=replace(ref,projections=(ref.projections[0],replace(ref.projections[1],**{field:value})))
    rows,_=run(replace(request,references=(ref,)),items)
    assert select(rows,'reference_exclusion',('CA',)).get('reason')==reason
    assert select(rows,'reference_context').get('state')=='insufficient_data'


def test_graph_promised_scientific_types_and_sources_resolve():
    request,items=positive_reference()
    rows,_=run(request,items)
    science={(r.kind,r.event,r.key):r for r in rows if r.kind not in ('evidence','evidence_source','claim','unknown','limitation','edge')}
    evidence={r.get('source_locator'):r for r in rows if r.kind=='evidence'}
    kinds=('point','phase','activity_relation','reference_cdf')
    assert all(any(r.kind==kind for r in science.values()) for kind in kinds)
    for locator,scientific in science.items():
        if scientific.kind in kinds:
            node=evidence[locator]
            assert node.get('values')==scientific.values and node.get('conditions')
            links=[r for r in rows if r.kind=='evidence_source' and r.key[0]==node.key[0]]
            assert links and all(r.get('source_locator') in science for r in links)
    # fastest的两种中性差值也都有对应Evidence。
    for key in ('minimum_adjacent_change','maximum_adjacent_change'):
        assert ('point',EVENT,('visible_direction_count',key)) in evidence
    # migration缺数值证据：有Unknown和Evidence，但没有数值Claim。
    locator=('reference_cdf',EVENT,('migration_ratio',))
    node=evidence[locator]
    assert any(r.kind=='unknown' and dict(r.values).get('evidence_id')==node.key[0] for r in rows)
    assert not any(r.kind=='claim' and r.get('evidence_id')==node.key[0] for r in rows)


def test_previous_input_version_not_silently_upgraded():
    request,items=fixture()
    with pytest.raises(ValueError,match='input_version'):
        run(replace(request,schema_version='country-trend-fixture/v1'),items)


def test_graph_fanout_stops_at_shared_budget(monkeypatch,tmp_path):
    from data_pipeline.analysis.country_trends.contract import Limits, row
    module=importlib.import_module('data_pipeline.analysis.country_trends.compute')
    request,items=fixture((),families=False)
    produced=[]
    def expanded(source):
        for i in range(1000):
            produced.append(i)
            yield row('evidence',source.event,(i,),source_kind=source.kind)
    monkeypatch.setattr(module,'graph_rows',expanded)
    with pytest.raises(ValueError,match='row_budget'):
        list(compute_trends(request,stream(request,items),limits=Limits(max_rows=len(items)+6),temporary_parent=tmp_path))
    assert len(produced)<10 and not list(tmp_path.iterdir())
