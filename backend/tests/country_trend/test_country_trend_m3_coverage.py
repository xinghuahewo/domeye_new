"""人工独立覆盖：没有生产C3/C4，没有Admission。"""
from dataclasses import replace
import pytest
from data_pipeline.analysis.country_trends.qualified_models import DIMENSIONS
from data_pipeline.analysis.country_trends.result_coverage import coverage_rows
from data_pipeline.analysis.country_trends.stream_schema import flatten, restore


def records(count, enumeration):
    return [dict(logical_run_id='fixture-coverage', input_binding_id='fixture-input',
        module='country', source_id=None, window_us=(110, 140), dimension=dimension,
        start_position=None, end_position=None, event_count=count, execution='complete',
        coverage=enumeration if dimension == 'event_enumeration' else 'unknown',
        qualification_count=0, qualification_digest='0'*64, gap_count=0, gap_digest='0'*64,
        unassigned_scope_refs=(), completion_receipt_refs=(('fixture', 'fixture-read-receipt'),),
        rule_version='country-qualification/v1', coverage_id='fixture-'+dimension)
        for dimension in DIMENSIONS]


@pytest.mark.parametrize('count,enumeration,state', [(0, 'complete', 'admitted_empty'),
    (0, 'unknown', 'unknown_empty'), (2, 'complete', 'observed_events')])
def test_execution_does_not_erase_independent_coverage(count, enumeration, state):
    raw = records(count, enumeration)
    rows = list(coverage_rows('fixture-coverage', 'fixture-input', (110, 140), raw, count))
    actual = [r.get('raw') for r in rows if r.kind == 'country_coverage']
    assert actual == raw
    assert all(r.event == () for r in rows)
    assert next(r for r in rows if r.kind == 'result_availability').get('state') == state
    for sequence, r in enumerate(rows):
        r = replace(r, result_id='fixture-result')
        assert restore(r.kind, flatten(sequence, r)) == r


def test_missing_or_wrong_coverage_cannot_make_an_empty_success():
    raw = records(0, 'complete')
    for damaged in (raw[:-1], [*raw, raw[0]], [dict(v, event_count=1) for v in raw],
                    [dict(v, input_binding_id='other') for v in raw]):
        with pytest.raises(ValueError):
            list(coverage_rows('fixture-coverage', 'fixture-input', (110, 140), damaged, 0))
