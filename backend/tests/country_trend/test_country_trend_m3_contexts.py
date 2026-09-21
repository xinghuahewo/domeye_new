"""人工逐对象资格；不连接 Country 候选、不授予正式准入。"""
from dataclasses import asdict

from tests.country_trend.test_country_trend_s1 import fixture
from data_pipeline.analysis.country_events import event_aggregation as c2
from data_pipeline.analysis.country_events.compute import AsnPoint, PrefixPoint, PathSummary
from data_pipeline.analysis.country_trends.event_inputs import EventSources
from data_pipeline.analysis.country_trends.qualified_contexts import asn_context, family_context, path_context


def sources(damage=None, *, window=(0, 3_000_000)):
    _, rows = fixture((20, 12, 18), 20)
    originals = tuple((seq, v) for seq, v in enumerate(rows)
                      if type(v) is c2.C2Row and v.incident_id is not None)
    event = originals[0][1].incident_id, originals[0][1].revision
    args = ('fixture-context', 'fixture-context-input', window, event, originals)
    base = EventSources(*args, (), ())
    qs = []
    for key, raw in base.originals.items():
        dimensions = (('cohort_membership', 'fixed_denominator') if type(raw) is c2.EventStatus else
                      ('cohort_membership', 'origin_attribution') if type(raw) is c2.CohortMember else
                      ('point_presence',) if type(raw) in (AsnPoint, PrefixPoint) else ())
        for dimension in dimensions:
            q = dict(asdict(base.targets[key]), qualification_id='fixture:'+str(key)+dimension,
                     dimension=dimension, coverage='complete')
            if damage:
                damage(raw, q)
            qs.append(q)
    dq = next(q for q in qs if q['raw_target_ref'] == base.population_ref and q['dimension'] == 'fixed_denominator')
    dv = dict(logical_run_id=args[0], input_binding_id=args[1], raw_target_ref=base.population_ref,
              window_us=args[2], dimension='fixed_denominator', population_ref=base.population_ref,
              unit='endpoint_direction_count', value=20, denominator=20,
              known_lower_bound=None, lower_bound_proven=False, coverage='complete',
              qualification_refs=(dq['qualification_id'],), qualified_value_id='fixture-dv',
              raw_basis_refs=(('fixture-source', 'fixture-raw'),))
    return EventSources(*args, qs, (dv,))


def test_asn_origin_gap_preserves_other_family_local_points():
    def damage(raw, q):
        if type(raw) is c2.CohortMember and raw.route.endpoint.afi == 1 and q['dimension'] == 'origin_attribution':
            q['coverage'] = 'unknown'
    source = sources(damage)
    rows = list(asn_context(source, (1_000_000, 2_000_000, 3_000_000)))
    points = [r for r in rows if r.kind == 'qualified_asn_point']
    assert points and all(r.get('raw') is not None for r in points)
    assert all(r.get('main') is not None for r in points if r.key[1] == 2)
    assert all(r.get('main') is None for r in points if r.key[1] in (0, 1))
    assert not any(r.kind == 'asn_priority' for r in rows)
    assert next(r for r in rows if r.kind == 'asn_context').get('state') == 'unavailable'


def test_family_point_qualification_does_not_blank_other_family():
    def damage(raw, q):
        if type(raw) is PrefixPoint and raw.prefix == '10.0.0.0/24' and raw.sample_us == 2_000_000:
            q['coverage'] = 'unknown'
    rows = list(family_context(sources(damage), (1_000_000, 2_000_000, 3_000_000)))
    points = {r.key: dict(r.values) for r in rows if r.kind == 'family_point'}
    assert points[1, 2_000_000]['value'] is None
    assert points[2, 2_000_000]['value'] == 6
    assert points[1, 1_000_000]['value'] == 10
    assert points[2, 2_000_000]['denominator'] == 10
    assert points[2, 2_000_000]['qualification_reasons'] == ()
    assert next(r for r in rows if r.kind == 'family_context').get('state') == 'unavailable'


def test_complete_asn_keeps_sampling_statistics_without_session_claim():
    rows = list(asn_context(sources(), (1_000_000, 2_000_000, 3_000_000)))
    assert next(r for r in rows if r.kind == 'asn_context').get('state') == 'complete'
    assert any(r.kind == 'asn_summary' for r in rows)
    assert any(r.kind == 'asn_priority' for r in rows)
    assert all(not r.get('continuous_duration_claimed') for r in rows if r.kind != 'qualified_asn_point')


def test_current_path_cannot_fill_history_or_continuity():
    source = sources()
    original = PathSummary(source.status.cohort_id, 64500, 64501, 1, 1, 1, 1, 1,
                           1_000_000, 1_000_000, (('fixture-path', 'fixture-prefix', 1_000_000),), 1, 256, 0)
    originals = ((0, c2.C2Row(*source.event, source.status)), (1, c2.C2Row(*source.event, original)))
    args = (source.logical_run_id, source.input_binding_id, source.window_us, source.event, originals)
    base = EventSources(*args, (), ())
    target = base.targets['path_summary', 1]
    qs = [dict(asdict(target), qualification_id='fixture-'+dimension, dimension=dimension, coverage=state)
          for dimension, state in (('path_current', 'complete'), ('path_history_completeness', 'partial'))]
    rows = list(path_context(EventSources(*args, qs, ())))
    assert len(rows) == 1
    assert rows[0].get('raw') is original and rows[0].get('current') is original
    assert rows[0].get('history') is None and rows[0].get('continuity') == 'unknown'
    assert rows[0].get('history_reasons') == ('path_history_completeness:partial',)
