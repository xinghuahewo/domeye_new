"""越窗完整资格不得生成主值；人工复现独立审查的路径 P2。"""
from dataclasses import asdict
import pytest
from data_pipeline.analysis.country_events import event_aggregation as c2
from data_pipeline.analysis.country_events.compute import PathSample
from data_pipeline.analysis.country_events.models import Incident, Time
from data_pipeline.analysis.country_trends.event_inputs import EventSources
from data_pipeline.analysis.country_trends.qualified_contexts import path_context, asn_context, family_context
from data_pipeline.analysis.country_trends.evidence_graph import evidence_rows
from tests.country_trend.test_country_trend_m3_contexts import sources


@pytest.mark.parametrize('sample', (100, 110, 120, 140, 150))
def test_path_sample_result_window_keeps_raw_but_limits_claim(sample):
    status = c2.EventStatus(Incident('fixture-path', 1, 'US', Time(80)), (), None,
                            'cohort', 'available', (), 1, 1)
    raw = PathSample('cohort', sample, '10.0.0.0/24', 64500, 64501, 0, 1,
                     'path', 'observation', Time(sample), 'peer', 'mapping', ('state',), None, 'observed')
    originals = ((0, c2.C2Row('fixture-path', 1, status)), (1, c2.C2Row('fixture-path', 1, raw)))
    args = ('fixture-run', 'fixture-binding', (110, 140), ('fixture-path', 1), originals)
    base = EventSources(*args, (), ())
    target = base.targets['path_sample', 1]
    qs = [dict(asdict(target), qualification_id='q-'+dimension, dimension=dimension, coverage=coverage)
          for dimension, coverage in (('path_current', 'complete'), ('path_history_completeness', 'unknown'))]
    source = EventSources(*args, qs, ())
    result, = path_context(source)
    applicable = 110 < sample <= 140
    assert tuple(source.originals) == (('event_status', 0), ('path_sample', 1))
    assert source.originals['path_sample', 1] is raw and result.get('raw') is raw
    assert result.get('current') is (raw if applicable else None)
    assert result.get('history') is None
    assert sum(r.kind == 'claim' for r in evidence_rows(result)) == int(applicable)
    if not applicable:
        assert 'sample_outside_result_window' in result.get('current_reasons')


@pytest.mark.parametrize('context', ('asn', 'family'))
def test_explicit_sample_list_cannot_bypass_result_window(context):
    source = sources(window=(1_000_000, 2_000_000))
    original_keys = tuple(source.originals)
    samples = (1_000_000, 2_000_000, 3_000_000)
    rows = list(asn_context(source, samples) if context == 'asn' else family_context(source, samples))
    points = [r for r in rows if r.kind == ('qualified_asn_point' if context == 'asn' else 'family_point')]
    assert points
    for result in points:
        sample = result.key[-1]
        value = result.get('main' if context == 'asn' else 'value')
        assert (value is not None) == (sample == 2_000_000)
        if sample != 2_000_000:
            assert not any(r.kind == 'claim' for r in evidence_rows(result))
    assert tuple(source.originals) == original_keys
