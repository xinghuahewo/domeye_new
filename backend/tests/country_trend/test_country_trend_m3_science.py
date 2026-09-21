"""显式人工纯计算；没有 Country Admission、PG 或真实数据。"""
from dataclasses import asdict, replace
from fractions import Fraction as F

import pytest

from data_pipeline.analysis.country_trends.qualified_models import Target
from data_pipeline.analysis.country_trends.qualified_calculation import Qualifications, series_rows


TARGET = Target('fixture-trend-m3', 'fixture-input', ('metric_point', 20),
                'fixture-event', 2, 'fixture-cohort', None, None,
                'visible_direction_count', 120, (110, 140))
POPULATION = ('event_status', 3)
DENOMINATOR_TARGET = replace(TARGET, raw_target_ref=POPULATION, sample_us=None, metric=None)


def selected(value=20, *, coverage='complete', lower=None, dimension='point_presence', target=TARGET, denominator=20):
    qualification = dict(asdict(target), qualification_id='fixture-q', dimension=dimension,
                         coverage=coverage)
    proof = dict(logical_run_id=target.logical_run_id, input_binding_id=target.input_binding_id,
                 raw_target_ref=target.raw_target_ref, window_us=target.window_us,
                 dimension=dimension, population_ref=POPULATION, unit='endpoint_direction_count',
                 value=value if coverage == 'complete' else None, known_lower_bound=lower,
                 lower_bound_proven=lower is not None, denominator=denominator,
                 coverage=coverage, qualification_refs=('fixture-q',),
                 raw_basis_refs=(('fixture-source', 'fixture-ref'),), qualified_value_id='fixture-qv')
    return qualification, proof


def consume(q, proof, raw=20, target=TARGET):
    return Qualifications((q,)).select(target, raw, proof, dimension=proof['dimension'],
                                       population_ref=POPULATION, unit='endpoint_direction_count')


@pytest.mark.parametrize('field,value', [
    ('logical_run_id', 'wrong'), ('input_binding_id', 'wrong'),
    ('raw_target_ref', ('metric_point', 21)), ('incident_id', 'wrong'),
    ('revision', 1), ('revision', F(2)), ('cohort_id', 'wrong'), ('entity_key', 'wrong'),
    ('afi', 1), ('metric', 'wrong'), ('sample_us', 130), ('window_us', (100, 140)),
])
def test_qualification_cannot_cross_original_scope(field, value):
    q, proof = selected()
    q[field] = value
    with pytest.raises(ValueError, match='target_mismatch'):
        consume(q, proof)


def test_gap_keeps_raw_and_proven_lower_bound_without_exact_analysis():
    q, proof = selected(20, coverage='unknown', lower=F(1, 2))
    point = consume(q, proof)
    assert (point.raw, point.value, point.known_lower_bound, point.state) == (20, None, F(1, 2), 'unknown')
    d = consume(*selected(20, dimension='fixed_denominator', target=DENOMINATOR_TARGET), target=DENOMINATOR_TARGET)
    rows = list(series_rows(('fixture-event', 2), 'visible_direction_count', (120,), (point,),
                            unit='endpoint_direction_count', denominator=d))
    assert not any(r.kind in ('fact', 'atomic', 'phase') for r in rows)
    assert any(r.kind == 'unknown' and r.get('code') == 'incomplete_qualified_samples' for r in rows)
    proof['lower_bound_proven'] = False
    with pytest.raises(ValueError, match='inconsistent'):
        consume(q, proof)


def test_qualified_samples_keep_old_exact_science_without_continuity_claim():
    points = tuple(consume(*selected(v, target=replace(TARGET, sample_us=t, raw_target_ref=('metric_point', t))),
                           raw=v, target=replace(TARGET, sample_us=t, raw_target_ref=('metric_point', t)))
                   for t, v in zip((120, 130, 140), (20, 12, 18)))
    d = consume(*selected(20, dimension='fixed_denominator', target=DENOMINATOR_TARGET), target=DENOMINATOR_TARGET)
    rows = list(series_rows(('fixture-event', 2), 'visible_direction_count', (120, 130, 140), points,
                            unit='endpoint_direction_count', denominator=d))
    facts = {r.key[-1]: r.get('value') for r in rows if r.kind == 'fact'}
    assert facts['loss_magnitude'] == 8
    assert facts['extreme_to_end_rebound'] == 6
    assert facts['end_residual_from_start'] == 2
    assert facts['window_rebound_ratio'] == F(3, 4)
    assert facts['fixed_cohort_visibility_gap_integral'] == 10
    peak = next(r for r in rows if r.kind == 'peak')
    assert peak.get('known_peak') == 20 and peak.get('exact_peak') is None
    assert all(not r.get('continuous_duration_claimed') for r in rows if r.kind in ('phase', 'fact', 'threshold'))


@pytest.mark.parametrize('denominator,code', [(None, 'denominator_unknown'), (0, 'zero_denominator')])
def test_zero_and_unknown_denominator_differ(denominator, code):
    q, proof = selected(denominator, dimension='fixed_denominator', denominator=denominator, target=DENOMINATOR_TARGET)
    d = consume(q, proof, raw=denominator, target=DENOMINATOR_TARGET)
    point = consume(*selected(0, denominator=denominator), raw=0)
    assert point.state == 'observed_zero'
    rows = list(series_rows(('fixture-event', 2), 'visible_direction_count', (120,), (point,),
                            unit='endpoint_direction_count', denominator=d))
    assert not any(r.kind == 'fact' for r in rows)
    assert any(r.kind == 'unknown' and r.get('code') == code for r in rows)


def test_only_required_dimensions_and_raw_fraction_survive():
    q, proof = selected(F(1, 2))
    history = dict(q, qualification_id='fixture-path-history', dimension='path_history_completeness', coverage='unknown')
    index = Qualifications((q, history))
    point = index.select(TARGET, F(1, 2), proof, dimension='point_presence',
                         population_ref=POPULATION, unit='endpoint_direction_count')
    assert point.value == point.raw == F(1, 2)
    assert index.check(TARGET, ('fixture-path-history',), ('path_history_completeness',)) == ('path_history_completeness:unknown',)
    assert index.check(TARGET, (), ('window_continuity',)) == ('missing_qualification:window_continuity',)


def test_missing_or_disconnected_or_self_promoted_proof_rejected():
    q, proof = selected()
    index = Qualifications((q,))
    missing = index.select(TARGET, 20, None, dimension='point_presence', population_ref=POPULATION, unit='endpoint_direction_count')
    assert missing.raw == 20 and missing.value is None
    for change in ({'qualification_refs': ('missing',)}, {'population_ref': ('event_status', 999)}, {'value': 19}):
        with pytest.raises(ValueError):
            consume(q, dict(proof, **change))
    q['coverage'] = 'unknown'
    with pytest.raises(ValueError, match='complete_without_qualification'):
        consume(q, proof)


def test_correctly_selected_point_cannot_be_reused_at_another_sample():
    point = consume(*selected())
    d = consume(*selected(20, dimension='fixed_denominator', target=DENOMINATOR_TARGET), target=DENOMINATOR_TARGET)
    with pytest.raises(ValueError, match='series_target_mismatch'):
        list(series_rows(('fixture-event', 2), 'visible_direction_count', (130,), (point,),
                         unit='endpoint_direction_count', denominator=d))


def test_original_rows_survive_while_per_point_denominator_remains_unknown():
    from data_pipeline.analysis.country_events import event_aggregation as c2
    from data_pipeline.analysis.country_events.compute import MetricPoint
    from data_pipeline.analysis.country_events.models import Incident, Time
    from data_pipeline.analysis.country_trends.event_inputs import EventSources
    status = c2.EventStatus(Incident('fixture-event', 2, 'US', Time(80)), (), None,
                            'fixture-cohort', 'available', (), 20, 1)
    point = MetricPoint('fixture-cohort', 120, 'visible_direction_count',
                        'endpoint_direction_count', F(20), F(20), 20, F(1), 'calculated')
    history = replace(point, sample_us=100)
    q, value = selected(F(20))
    dq, dv = selected(20, dimension='fixed_denominator', target=DENOMINATOR_TARGET)
    dq['qualification_id'] = 'fixture-d'
    dv['qualification_refs'] = ('fixture-d',)
    originals = tuple((seq, c2.C2Row('fixture-event', 2, v))
                      for seq, v in ((3, status), (19, history), (20, point)))
    sources = EventSources('fixture-trend-m3', 'fixture-input', (110, 140),
                           ('fixture-event', 2), originals, (q, dq), (value, dv))
    selected_point = sources.metric(('metric_point', 20))
    assert selected_point.value == 20 and selected_point.denominator is None
    assert selected_point.denominator_reasons == ('missing_qualification:fixed_denominator',)
    rows = list(series_rows(('fixture-event', 2), 'visible_direction_count', (120,), (selected_point,),
                            unit='endpoint_direction_count', denominator=sources.denominator()))
    assert next(r for r in rows if r.kind == 'qualified_metric').get('value') == 20
    assert next(r for r in rows if r.kind == 'profile').get('denominator') is None
    assert sources.originals['metric_point', 19] is history
    assert sources.metric(('metric_point', 19)).value is None
    assert point.value == F(20) and point.denominator == 20


def test_full_original_grid_is_preserved_and_only_result_samples_enter_science():
    from tests.country_trend.test_country_trend_s1 import fixture
    from data_pipeline.analysis.country_events import event_aggregation as c2
    from data_pipeline.analysis.country_events.compute import MetricPoint
    from data_pipeline.analysis.country_trends.contract import Limits
    from data_pipeline.analysis.country_trends.qualified_models import metric_dimension
    from data_pipeline.analysis.country_trends.event_inputs import EventSources
    from data_pipeline.analysis.country_trends.qualified_calculation import event_series
    _, originals = fixture((20, 12, 18), 20, families=False)
    originals = tuple((seq, v) for seq, v in enumerate(originals)
                      if type(v) is c2.C2Row and v.incident_id is not None)
    event = originals[0][1].incident_id, originals[0][1].revision
    window = (1_000_000, 3_000_000)
    empty = EventSources('fixture-grid', 'fixture-grid-input', window, event, originals, (), ())
    qs, values = [], []
    for key, raw in empty.originals.items():
        if key == empty.population_ref:
            dimensions = ('fixed_denominator',)
        elif type(raw) is MetricPoint and raw.sample_us > window[0]:
            dimensions = ('fixed_denominator', metric_dimension(raw.metric))
        else:
            continue
        for dimension in dimensions:
            identity = 'fixture-q-' + str(key) + dimension
            qs.append(dict(asdict(empty.targets[key]), qualification_id=identity,
                           dimension=dimension, coverage='complete'))
            if type(raw) is MetricPoint and dimension == 'fixed_denominator':
                continue
            values.append(dict(logical_run_id='fixture-grid', input_binding_id='fixture-grid-input',
                raw_target_ref=key, window_us=window, dimension=dimension,
                population_ref=empty.population_ref,
                unit=raw.unit if type(raw) is MetricPoint else 'endpoint_direction_count',
                value=raw.value if type(raw) is MetricPoint else raw.direction_count,
                denominator=raw.denominator if type(raw) is MetricPoint else raw.direction_count,
                known_lower_bound=None, lower_bound_proven=False,
                qualification_refs=(identity,), coverage='complete',
                raw_basis_refs=(('fixture-source', 'fixture-raw'),), qualified_value_id=identity+'-v'))
    sources = EventSources('fixture-grid', 'fixture-grid-input', window, event, originals, qs, values)
    rows = list(event_series(sources, limits=Limits()))
    scope = next(r for r in rows if r.kind == 'analysis_scope')
    assert scope.get('calculation_samples') == (1_000_000, 2_000_000, 3_000_000)
    assert scope.get('result_samples') == (2_000_000, 3_000_000)
    assert scope.get('original_incident').end is None
    assert len([r for r in rows if r.kind == 'metric']) == 48
    assert len([r for r in rows if r.kind == 'qualified_metric']) == 32
    assert {r.key[1] for r in rows if r.kind == 'qualified_metric'} == {2_000_000, 3_000_000}
    assert all(r.get('exact_peak') is None for r in rows if r.kind == 'peak')
    analysis = next(r for r in rows if r.kind == 'analysis')
    assert analysis.get('state') == 'complete'
