"""固定72类型到S3的有限文件接线；不构造成功AD，不启动PG。"""
from dataclasses import asdict, replace
import sqlite3
import pytest

from tests.country.test_country_m3_schema import coverage, qualification, REF
from data_pipeline.analysis.country_events import event_aggregation as c2, qualified_schema as m3_schema, snapshot_schema as original
from data_pipeline.analysis.country_events.models import Incident, Time, Cursor
from data_pipeline.analysis.country_events.saved_input import CountryRevision
from data_pipeline.analysis.country_trends.country_source import initialize, save_source, save_event, validate_events
from data_pipeline.analysis.country_trends.compile_country_results import compile_country
from data_pipeline.analysis.country_trends.stream_files import write_body, read_body
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
from data_pipeline.analysis.country_trends.stream_budget import StreamBudget as SharedBudget, connect
from data_pipeline.analysis.country_trends.runtime import ProductionRuntime, Runtime
from data_pipeline.analysis.country_trends.stream_store import prepare_trend_m3
from data_pipeline.analysis.country_trends.result_admission import request_scope, CODEC
from data_pipeline.analysis.country_trends import stream_schema as s3_schema
from data_pipeline.analysis.country_trends.qualified_models import DIMENSIONS


def test_fixed_country_typed_history_to_s3_files(tmp_path):
    budget = SharedBudget(S2Limits(), tmp_path, lambda: None)
    db = connect(tmp_path/'source.sqlite', budget); initialize(db)
    try:
        incident = 'fixture-carry'
        historical = [CountryRevision(incident, r, 'US', Cursor(0, 0, r), Time(80+r), Time(80), None,
                       {'fixture_revision': r, 'raw_optional': None}) for r in (1, 2)]
        originals = [c2.C2Row(incident, r.revision, c2.InputEvidence('event_revision', f'{incident}/revision/{r.revision}', r)) for r in historical]
        status = c2.EventStatus(Incident(incident, 2, 'US', Time(80)), tuple(f'{incident}/revision/{r}' for r in (1,2)),
                                None, None, 'unavailable', ('fixture_missing_baseline',))
        originals.append(c2.C2Row(incident, 2, status))
        wrapped = []
        for seq, value in enumerate(originals):
            table, flat = m3_schema.row_encode(seq, value); raw = dict(table=table, row=flat)
            wrapped.append(raw); save_source(db, raw, budget)
        for seq, dimension in enumerate(DIMENSIONS, len(wrapped)):
            value = replace(coverage(), event_count=1, dimension=dimension, coverage_id='')
            table, flat = m3_schema.row_encode(seq, c2.C2Row(None,None,value))
            save_source(db, dict(table=table,row=flat), budget)
        envelope = dict(raw=wrapped[2], main=None, lifecycle={'selection':'carry_in','earlier_history':'Unknown','original_end':None},
                        original_revisions=tuple(wrapped[:2]), qualifications=())
        save_event(db, envelope, budget)
        validate_events(db, budget)
        write_body(compile_country(db, 'fixture-compiled', (100,200), budget), tmp_path/'body', budget=budget)
        rows = list(read_body(tmp_path/'body'))
        event, = [r for r in rows if r.kind=='event']
        assert event.event == (incident,2) and event.get('main') is None
        assert event.get('original_revisions') == tuple(wrapped[:2])
        assert event.get('raw').incident.end is None
        source = [r for r in rows if r.kind=='raw_source']
        assert [r.get('source_sequence') for r in source] == [0,1,2]
        assert [original.decode(r.get('raw_typed'))[1] for r in source] == [1,2,2]
        db.execute('UPDATE country_events SET payload=?',
                   (original.encode(dict(envelope,original_revisions=(wrapped[2],))),))
        with pytest.raises(ValueError,match='original_revision_type'): validate_events(db,budget)
    finally: db.close()


def test_qualified_numeric_compile_with_unknown_other_dimensions(tmp_path):
    from tests.country_trend.test_country_trend_s1 import fixture
    from data_pipeline.analysis.country_trends.event_inputs import EventSources
    from data_pipeline.analysis.country_events.compute import MetricPoint
    from data_pipeline.analysis.country_events.route_contract import CountryQualifiedValue
    from data_pipeline.analysis.country_trends.contract import ActivityWindow, Projection, ReferenceInput, REFERENCE_DEFINITION
    _, originals = fixture((20, 10), 20, families=False)
    window = (0, 2_000_000)
    event = ('fixture-event', 1)
    indexed = tuple((seq, v) for seq, v in enumerate(originals) if type(v) is c2.C2Row)
    source = EventSources('fixture-logical-run', 'fixture-binding', window, event, indexed, (), ())
    all_rows = list(originals)
    for key, value in source.originals.items():
        if key != source.population_ref and not (type(value) is MetricPoint and value.metric == 'visible_direction_count'):
            continue
        dimensions = ('fixed_denominator',) if key == source.population_ref else ('fixed_denominator', 'point_presence')
        for dimension in dimensions:
            q = qualification(**asdict(source.targets[key]), target_kind='metric', dimension=dimension, coverage='complete')
            all_rows.append(c2.C2Row(*event, q))
            if key != source.population_ref and dimension == 'fixed_denominator': continue
            v = CountryQualifiedValue('fixture-logical-run','fixture-binding',key,dimension,
                value.direction_count if key == source.population_ref else value.value,
                None,False,'endpoint_direction_count',source.population_ref,20,(REF,),
                (q.qualification_id,),window,'complete')
            all_rows.append(c2.C2Row(*event, v))
    for dimension in DIMENSIONS:
        all_rows.append(c2.C2Row(None,None,replace(coverage(),window_us=window,event_count=1,dimension=dimension,coverage_id='')))
    budget = SharedBudget(S2Limits(),tmp_path,lambda:None)
    db = connect(tmp_path/'source.sqlite', budget); initialize(db)
    try:
        wrapped = []
        for seq, value in enumerate(all_rows):
            table, flat = m3_schema.row_encode(seq,value)
            raw = dict(table=table,row=flat); save_source(db,raw,budget); wrapped.append(raw)
        save_event(db,dict(raw=wrapped[0],main=None,lifecycle={'earlier_history':'Unknown'},
                           original_revisions=(),qualifications=()),budget)
        validate_events(db,budget)
        # 这里只测试纯上下文编译及文件FK，不伪造Feature或参考的正式读取成功。
        activity = ActivityWindow(event,'ordinary','announ_num','accepted_announce_elements',
                                  1_000_000,2_000_000,3,'complete','fixture-activity')
        projections = tuple(Projection(code,'fixed_endpoint_direction',20,(1_000_000,2_000_000),
            values,'fixture-reference-'+code,cohort_id='fixture-cohort',definition_binding=REFERENCE_DEFINITION)
            for code, values in (('US',(20,10)),('GB',(20,18))))
        reference = ReferenceInput(event,('fixture-pure-reference',),'US',projections)
        context_sources = (('fixture-activity','feature',s3_schema.encode(activity)), *(
            (p.source_ref,'reference',s3_schema.encode(p)) for p in projections))
        write_body(compile_country(db,'fixture-qualified-files',window,budget,activities=(activity,),
            references=(reference,),context_sources=context_sources),tmp_path/'body',budget=budget)
        rows = list(read_body(tmp_path/'body'))
        points = [r for r in rows if r.kind=='qualified_metric' and r.key[0]=='visible_direction_count']
        assert [r.get('value') for r in points] == [20,10]
        assert any(r.kind=='qualified_metric' and r.get('value') is None for r in rows)
        assert any(r.kind=='fact' and r.key[-1]=='loss_magnitude' and r.get('value')==10 for r in rows)
        assert any(r.kind=='activity_relation' for r in rows)
        assert any(r.kind=='reference_cdf' and r.get('percentile')==100 for r in rows)
        assert next(r for r in rows if r.kind=='event').get('main') is None
    finally: db.close()


def runtime_args(tmp_path):
    root = tmp_path.resolve(); (root/'out').mkdir(); (root/'scratch').mkdir()
    return dict(dsn=f'host={root} dbname=fixture', output_root=root/'out', scratch_root=root/'scratch',
                allowed_roots=(root,), dependency_admissions=(), dependency_runtimes={}, country_window_us=(100,200))


def test_production_rejects_missing_actual_ad_before_creating_candidate(tmp_path):
    args = runtime_args(tmp_path)
    runtime = ProductionRuntime(**args, fixture_only=True)
    with pytest.raises(ValueError, match='actual_dependencies'): prepare_trend_m3(runtime)
    assert list(args['output_root'].iterdir()) == []
    with pytest.raises(ValueError): Runtime(**args, fixture_only=True, expected_trend_binding={})


def test_real_requires_explicit_budgets_and_runtime_cannot_change_window(tmp_path):
    args = runtime_args(tmp_path)
    with pytest.raises(ValueError, match='missing_budget'):
        ProductionRuntime(**args, execution_profile='real-candidate/v1')
    runtime = ProductionRuntime(**args, fixture_only=True)
    runtime.country_window_us = (0,200)
    with pytest.raises(ValueError, match='scope_drift'): runtime.resource_guard()


def test_p1_finite_view_scope_without_admission_or_io():
    scope = dict(event=('fixture',2), key_typed=None, after_sequence=-1, stop_sequence=None)
    request = dict(view='raw_source',scope_typed=s3_schema.encode(scope),codec_version=CODEC,batch_rows=2,batch_bytes=4096)
    assert request_scope(request,S2Limits()) == scope
    for bad in (dict(request,view='all'),dict(request,batch_rows=True),
                dict(request,scope_typed=s3_schema.encode(dict(scope,event=('fixture',True))))):
        with pytest.raises(ValueError): request_scope(bad,S2Limits())


def test_disk_event_stream_exceeds_resident_count_limit(tmp_path):
    cancelled=[False]
    failure=RuntimeError('fixture_cancel')
    def guard():
        if cancelled[0]:raise failure
    budget=SharedBudget(replace(S2Limits(),max_events=1),tmp_path,guard)
    db=connect(tmp_path/'events.sqlite',budget);initialize(db)
    try:
        envelopes=[]
        # 标识字典序与原序相反，插入事件表的顺序也与原序相反。
        for seq,incident in enumerate(('z-first','a-second')):
            status=c2.EventStatus(Incident(incident,1,'US',Time(80)),(),None,None,
                                  'unavailable',('fixture_missing_baseline',))
            table,flat=m3_schema.row_encode(seq,c2.C2Row(incident,1,status))
            wrapped=dict(table=table,row=flat);save_source(db,wrapped,budget)
            envelopes.append(dict(raw=wrapped,main=None,lifecycle={},original_revisions=(),qualifications=()))
        for envelope in reversed(envelopes):save_event(db,envelope,budget)
        for seq,dimension in enumerate(DIMENSIONS,2):
            value=replace(coverage(),event_count=2,dimension=dimension,coverage_id='')
            table,flat=m3_schema.row_encode(seq,c2.C2Row(None,None,value))
            save_source(db,dict(table=table,row=flat),budget)
        validate_events(db,budget)
        budget.limits=replace(budget.limits,max_events=2)
        expected=list(compile_country(db,'fixture-two',(100,200),budget))
        budget.limits=replace(budget.limits,max_events=1)
        write_body(compile_country(db,'fixture-two',(100,200),budget),tmp_path/'body',budget=budget)
        actual=list(read_body(tmp_path/'body'))
        assert actual==expected
        assert [r.event for r in actual if r.kind=='event']==[('z-first',1),('a-second',1)]
        availability,=[r for r in actual if r.kind=='result_availability']
        assert availability.get('event_count')==2
        assert all(r.get('raw')['event_count']==2 for r in actual if r.kind=='country_coverage')
        stream=compile_country(db,'fixture-two',(100,200),budget)
        for item in stream:
            if item.kind=='event':break
        cancelled[0]=True
        with pytest.raises(RuntimeError) as caught:next(stream)
        assert caught.value is failure
        cancelled[0]=False
        original_limits=budget.limits
        budget.limits=replace(budget.limits,max_event_bytes=1)
        with pytest.raises(ValueError,match='trend_country_event_memory_budget'):
            list(compile_country(db,'fixture-two',(100,200),budget))
        budget.limits=original_limits
        db.execute('UPDATE country_events SET sequence=99 WHERE incident=?',('z-first',))
        with pytest.raises(ValueError,match='trend_country_selected_event_sequence'):validate_events(db,budget)
    finally:db.close()
