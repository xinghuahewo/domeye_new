"""S3 typed字段准备验证；不是实际湖生产或发布验收。"""
from dataclasses import replace
from fractions import Fraction as F
import pytest

from tests.country_trend.test_country_trend_m3_contexts import sources
from tests.country_trend.test_country_trend_m3_science import selected, consume, DENOMINATOR_TARGET, TARGET
from data_pipeline.analysis.country_trends.qualified_calculation import series_rows
from data_pipeline.analysis.country_trends.qualified_contexts import asn_context, family_context
from data_pipeline.analysis.country_trends import stream_schema as new, snapshot_schema as old
from data_pipeline.analysis.country_trends.contract import row
from data_pipeline.analysis.country_trends.evidence_graph import evidence_rows


def test_new_science_and_context_actual_rows_roundtrip_without_changing_v1():
    source = sources()
    samples = (1_000_000, 2_000_000, 3_000_000)
    points = tuple(consume(*selected(v, target=replace(TARGET, sample_us=t)),
                           raw=v, target=replace(TARGET, sample_us=t))
                   for t, v in zip((120, 130, 140), (20, 12, 18)))
    d = consume(*selected(20, dimension='fixed_denominator', target=DENOMINATOR_TARGET), target=DENOMINATOR_TARGET)
    rows = [*asn_context(source, samples), *family_context(source, samples),
            *series_rows(('fixture-event', 2), 'visible_direction_count', (120, 130, 140),
                         points, unit='endpoint_direction_count', denominator=d)]
    rows.extend(item for scientific in tuple(rows) for item in evidence_rows(scientific))
    for sequence, r in enumerate(rows):
        r = replace(r, result_id='fixture-s3-result')
        assert new.restore(r.kind, new.flatten(sequence, r)) == r
    assert len(old.TABLES) == 44 and set(old.TABLES).issubset(new.TABLES)
    assert old.VERSION == 'country-trend-store/v1' and new.VERSION != old.VERSION
    original = replace(row('point', ('event', 1), ('metric', 'start'), sample_us=1,
                           value=F(1, 2), unit='unit'), result_id='fixture-old-s2')
    assert old.restore('point', old.flatten(0, original)) == original
    with pytest.raises(ValueError, match='row_fields'):
        new.flatten(0, original)


def test_new_typed_rejects_resigned_unknown_fields_and_continuity():
    r = replace(row('qualified_metric', ('event', 1), ('metric', 120), raw=F(1, 2),
                    value=None, known_lower_bound=None, state='unknown',
                    reasons=('missing_qualification:point_presence',),
                    qualified_value_id=None, raw_basis_refs=(), unit='unit'), result_id='fixture-s3')
    flat = new.flatten(0, r)
    assert new.restore(r.kind, flat).get('raw') == F(1, 2)
    with pytest.raises(ValueError, match='row_fields'):
        new.flatten(0, replace(r, values=(*r.values, ('invented', 0))))
    altered = dict(flat, value=new.encode(0))
    with pytest.raises(ValueError, match='identity'):
        new.restore(r.kind, altered)
    profile = replace(row('profile', ('event', 1), ('metric',), state='complete', unit='unit',
                          denominator=20, sample_count=2, qualification_refs=(), qualification_reasons=(),
                          sample_basis='qualified_samples', continuous_duration_claimed=True), result_id='fixture-s3')
    with pytest.raises(ValueError, match='unproven_continuity'):
        new.flatten(0, profile)


def test_graph_keeps_unknown_and_local_zero_claim_with_exact_qualification_edges():
    missing = row('qualified_metric', ('event', 1), ('metric', 120), refs=('q-point',),
                  raw=20, value=None, known_lower_bound=F(1, 2), state='unknown',
                  reasons=('point_presence:partial',), qualified_value_id='qv-point',
                  raw_basis_refs=(('fixture-source', 'fixture-raw'),), unit='unit')
    graph = list(evidence_rows(missing))
    assert not any(r.kind == 'claim' for r in graph)
    assert any(r.kind == 'unknown' for r in graph)
    assert [r.get('qualification_id') for r in graph if r.kind == 'evidence_qualification'] == ['q-point']
    zero = replace(missing, values=tuple((k, 0 if k in ('raw', 'value') else None if k == 'known_lower_bound'
                                         else 'observed_zero' if k == 'state' else () if k == 'reasons' else v)
                                        for k, v in missing.values))
    assert any(r.kind == 'claim' for r in evidence_rows(zero))
