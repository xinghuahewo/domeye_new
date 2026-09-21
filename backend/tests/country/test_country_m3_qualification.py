"""纯逐维规则fixture；不构造或宣称任何实际Admission。"""
from dataclasses import replace
from types import SimpleNamespace
from fractions import Fraction
import json
import pytest

from data_pipeline.analysis.country_events.event_aggregation import C2Row, C2Completion, EventStatus, CohortMember
from data_pipeline.analysis.country_events.compute import MetricPoint
from data_pipeline.analysis.country_events.models import Baseline, Time
from data_pipeline.analysis.country_events.route_contract import CountryQualification, CountryQualifiedValue, CountryCoverage
from data_pipeline.analysis.country_events.result_qualification import qualification_rows
from tests.country.test_country_enhancement_cohort import binding, incident, route


def fixture(*, gap=None, sample=26000000, value=Fraction(1), empty=False):
    event = replace(incident(), onset=Time(20)); member = route()
    baseline = Baseline('fixture-baseline', binding(), member.observed_at, member.cursor, 'fixture-mapping')
    head = EventStatus(event, (f'{event.incident_id}/revision/1',), baseline, 'fixture-cohort', 'available', (), 1, 1)
    scientific = [C2Row(event.incident_id, 1, head), C2Row(event.incident_id, 1, CohortMember('fixture-cohort', member)),
                  C2Row(event.incident_id, 1, MetricPoint('fixture-cohort', sample, 'visible_direction_count',
                         'endpoint_direction_count', value, Fraction(0), 1, None, 'unknown' if value is None else 'known'))]
    if empty:scientific=[]
    scientific.append(C2Completion('saved_M3', None, int(not empty), 0, {'output_rows': len(scientific)}, (15000000,31000000)))
    qual = dict(dimension_coverage=dict(origin_anchor='complete', candidate_lifecycle='complete'))
    final = dict(dimension_coverage=dict(origin_anchor='unknown' if gap else 'complete', candidate_lifecycle='unknown' if gap else 'complete'))
    result = dict(raw=dict(incident_id=event.incident_id, revision=1), qualification=final,
                  lifecycle=dict(anchor_coverage='unknown' if gap else 'complete_within_declared_cold_start',
                                 selection='possible_unknown' if gap else 'started_in_result'))
    gap_rows = [] if gap is None else [dict(raw=dict(scope=dict(kind='received_endpoint'), raw_time=dict(epoch=gap, microsecond=0)))]
    tables = {('canonical','source_coverage'): [{'fixture': True}], ('canonical','changes'): [], ('canonical','scope_gap'): gap_rows,
              ('detection','result_revisions'): [] if empty else [result],
              ('detection','m3_entries'): [] if empty else [dict(kind='event_qualification', incident_id=event.incident_id, revision=1, ordinal=0, payload_json=json.dumps(qual))],
              ('detection','result_coverage'): [dict(raw=dict(payload_json=json.dumps(dict(dimension_coverage=dict(event_absence='unknown' if gap else 'complete')))))]}
    # 这里只提供纯函数所需的引用标签，没有owner_binding、物理库或admit替身。
    src=SimpleNamespace(binding=SimpleNamespace(input_binding_id='fixture-input',collector='rrc25'),
                        inputs=SimpleNamespace(owners={'canonical':['fixture-c'],'detection':['fixture-d']},
                                               admissions={'fixture-d':{'owner':'detection'}}),
                        receipts={('fixture-d','result_coverage'):{'receipt':{'request_digest':'fixture-receipt'}}},
                        windows={'result_window':dict(window_start='1970-01-01T00:00:15Z',window_end_exclusive='1970-01-01T00:00:31Z')},
                        verify_saved=lambda:None,get=lambda owner,view:tables[owner,view])
    return src, scientific


def derive(**kw):
    source, raw = fixture(**kw)
    return qualification_rows(source, 'fixture-logical-run', raw, max_rows=1000, max_bytes=1024**2,
                              max_references_per_row=128, guard=lambda:None)


def values(rows, dimension):
    return [r.value for r in rows if isinstance(r.value,CountryQualifiedValue) and r.value.dimension==dimension]


def test_later_gap_preserves_preproved_denominator_but_not_visibility():
    rows=derive(gap=23,value=None)
    fixed=values(rows,'fixed_denominator')[0]; point=values(rows,'point_presence')[0]
    assert fixed.value==1 and fixed.denominator==1 and fixed.coverage=='complete'
    assert point.value is None and point.denominator==1 and point.coverage=='unknown'
    assert point.known_lower_bound is None and point.lower_bound_proven is False
    assert next(r.value for r in rows if isinstance(r.value,CountryQualification) and r.value.dimension=='event_anchor').coverage=='unknown'


def test_current_recovery_cannot_restore_history_or_continuity():
    rows=derive(gap=23,sample=31000000,value=Fraction(1))
    assert values(rows,'point_presence')[0].value==1
    q=[r.value for r in rows if isinstance(r.value,CountryQualification)]
    assert next(v for v in q if v.dimension=='path_history_completeness').coverage=='partial'
    assert next(v for v in q if v.dimension=='window_continuity').coverage=='unknown'


def test_gap_before_freeze_cannot_use_raw_denominator_as_proof():
    rows=derive(gap=18)
    assert values(rows,'fixed_denominator')[0].value is None
    assert values(rows,'point_presence')[0].value is None
    assert values(rows,'point_presence')[0].denominator is None


def test_zero_event_has_twelve_independent_coverages_no_fake_incident():
    rows=derive(gap=23,empty=True)
    assert len(rows)==12 and all(isinstance(r.value,CountryCoverage) for r in rows)
    assert all(r.incident_id is None and r.value.event_count==0 and r.value.execution=='complete' for r in rows)
    assert next(r.value for r in rows if r.value.dimension=='event_enumeration').coverage=='unknown'


def test_original_rows_are_unchanged_and_reference_budget_precedes_output():
    source,raw=fixture()
    from data_pipeline.analysis.country_events.snapshot_schema import row_encode
    before=[row_encode(i,r) for i,r in enumerate(raw)]
    with pytest.raises(ValueError,match='qualification_refs'):
        qualification_rows(source,'fixture',raw,max_rows=1000,max_bytes=1024**2,max_references_per_row=1,guard=lambda:None)
    assert [row_encode(i,r) for i,r in enumerate(raw)]==before


def test_path_current_needs_actual_object_recovery_and_keeps_history_partial():
    from tests.country.test_country_m3_temporal import fixture as temporal_fixture
    from data_pipeline.analysis.country_events.compute import PathSample
    history,messages=temporal_fixture()
    source,raw=fixture()
    original_get=source.get
    source.get=lambda owner,view: history.rows[view] if owner=='canonical' and view in history.rows else original_get(owner,view)
    source.history=history;source.messages=messages
    source.windows['result_window']['window_end_exclusive']='1970-01-01T00:01:44Z'
    change=history.rows['changes'][-1]
    sample=PathSample(cohort_id='fixture-cohort',sample_us=102000001,prefix='198.51.0.0/24',
        affected_asn=1,downstream_asn=2,affected_position=0,downstream_position=1,
        path_ref=change['raw']['calculation_after']['path_key'],observation_ref=change['event_id'],
        observed_at=Time(102,0),peer_ref='fixture-peer',mapping_ref='fixture-mapping',
        state_refs=(),relationship_ref=None,path_basis='fixture')
    event=raw[0]
    raw.insert(-1,C2Row(event.incident_id,event.revision,sample))
    raw[-1]=replace(raw[-1],costs={'output_rows':len(raw)-1})
    def q_at(value):
        raw[-2]=C2Row(event.incident_id,event.revision,value)
        rows=qualification_rows(source,'fixture',raw,max_rows=1000,max_bytes=1024**2,
            max_references_per_row=128,guard=lambda:None)
        return {r.value.dimension:r.value for r in rows if isinstance(r.value,CountryQualification)
                and r.value.raw_target_ref[0]=='path_sample'}
    q=q_at(sample)
    assert q['path_current'].coverage=='complete' and q['path_current'].recovery_witnesses
    assert q['path_history_completeness'].coverage=='partial'
    q=q_at(replace(sample,sample_us=101950000))
    assert q['path_current'].coverage=='unknown' and not q['path_current'].recovery_witnesses
