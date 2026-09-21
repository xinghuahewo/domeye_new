"""有限人工文件检查；无PG、Country候选、Binding或Admission。"""
from dataclasses import replace
from fractions import Fraction as F
import pytest
import pyarrow.parquet as pq

from data_pipeline.analysis.country_events.compute import MetricPoint
from data_pipeline.analysis.country_events import snapshot_schema as c3
from data_pipeline.analysis.country_trends.contract import row
from data_pipeline.analysis.country_trends import stream_schema as schema
from data_pipeline.analysis.country_trends.stream_files import write_body, read_body
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
from data_pipeline.analysis.country_trends.evidence_graph import evidence_rows
from data_pipeline.analysis.country_trends.result_coverage import coverage_rows
from tests.country_trend.test_country_trend_m3_coverage import records


def fixture():
    event = ('fixture-file-event', 2)
    metric = MetricPoint('fixture-cohort', 120, 'visible_direction_count',
                         'endpoint_direction_count', F(1, 2), F(1, 2), 2, F(1, 4), 'calculated')
    point = row('qualified_metric', event, (metric.metric, 120), refs=('fixture-q',),
                raw=F(1, 2), value=None, known_lower_bound=None, state='unknown',
                reasons=('point_presence:unknown',), qualified_value_id='fixture-qv',
                raw_basis_refs=(('fixture-source', 'fixture-original'),), unit=metric.unit)
    values = [point,
        row('qualified_value_source', event, ('fixture-qv',), raw_typed=schema.encode({'fixture': 'qv'}),
            qualified_value_id='fixture-qv', raw_target_ref=('metric_point', 7), dimension='point_presence'),
        row('raw_source', event, (7,), source_sequence=7, source_table='metric_point',
            raw_typed=c3.encode((*event, metric))),
        row('country_qualification_source', event, ('fixture-q',), raw_typed=schema.encode({'fixture': 'q'}),
            qualification_id='fixture-q', raw_target_ref=('metric_point', 7), dimension='point_presence'),
        *evidence_rows(point)]
    return tuple(replace(r, result_id='fixture-files') for r in values)


def test_actual_parquet_roundtrip_preserves_original_order_and_zero_tables(tmp_path):
    rows = fixture()
    root = tmp_path / 'body'
    proof = write_body(iter(rows), root, limits=replace(S2Limits(), max_batch_rows=2))
    assert tuple(read_body(root)) == rows
    assert proof['rows'] == len(rows)
    assert len(list(root.glob('*.parquet'))) == len(schema.TABLES)
    assert pq.read_table(root / 'phase.parquet').num_rows == 0
    assert tuple(read_body(root))[2].get('source_sequence') == 7
    assert not list(root.glob('s3-sort-*'))
    assert not (root / 'ready.json').exists() and not (root / 'manifest.json').exists()


def test_empty_body_and_unknown_empty_coverage_are_distinct(tmp_path):
    empty = write_body(iter(()), tmp_path / 'empty')
    assert empty['rows'] == 0 and list(read_body(tmp_path / 'empty')) == []
    rows = tuple(replace(r, result_id='fixture-unknown-empty') for r in
                 coverage_rows('fixture-coverage', 'fixture-input', (110, 140), records(0, 'unknown'), 0))
    write_body(iter(rows), tmp_path / 'unknown')
    restored = list(read_body(tmp_path / 'unknown'))
    assert next(r for r in restored if r.kind == 'result_availability').get('state') == 'unknown_empty'
    assert len([r for r in restored if r.kind == 'country_coverage']) == 12


@pytest.mark.parametrize('damage', ('missing_qualification', 'wrong_original_value', 'cross_event'))
def test_storage_source_fk_rejects_resigned_bad_rows(tmp_path, damage):
    rows = list(fixture())
    if damage == 'missing_qualification':
        rows = [r for r in rows if r.kind != 'country_qualification_source']
    elif damage == 'wrong_original_value':
        rows[0] = replace(rows[0], values=tuple((k, F(3, 2) if k == 'raw' else v) for k, v in rows[0].values))
    else:
        rows[1] = replace(rows[1], event=('other-fixture-event', 2))
    with pytest.raises(ValueError, match='fk'):
        write_body(iter(rows), tmp_path / damage)
    assert not list((tmp_path / damage).glob('s3-sort-*'))


def test_early_stop_and_small_row_budget_cleanup(tmp_path):
    root = tmp_path / 'body'
    write_body(iter(fixture()), root)
    stream = read_body(root)
    next(stream)
    stream.close()
    assert not list(root.glob('s3-sort-*'))
    with pytest.raises(ValueError, match='row_budget'):
        write_body(iter(fixture()), tmp_path / 'limited', limits=replace(S2Limits(), max_row_bytes=1))
    assert not list((tmp_path / 'limited').glob('s3-sort-*'))


def test_reader_rejects_rewritten_sequence_even_with_new_row_digest(tmp_path):
    root = tmp_path / 'body'
    write_body(iter(fixture()), root)
    path = root / 'raw_source.parquet'
    table = pq.read_table(path)
    flat, = table.to_pylist()
    flat['sequence'] = 1000
    import pyarrow as pa
    pq.write_table(pa.Table.from_pylist([flat], schema=table.schema), path)
    with pytest.raises(ValueError, match='part_entity_changed'):
        list(read_body(root))
    assert not list(root.glob('s3-sort-*'))


def test_input_primary_error_survives_close_failure(tmp_path):
    class BrokenInput:
        def __iter__(self): return self
        def __next__(self): raise RuntimeError('fixture-primary')
        def close(self): raise OSError('fixture-close')
    with pytest.raises(RuntimeError, match='fixture-primary') as caught:
        write_body(BrokenInput(), tmp_path / 'broken')
    assert any(str(e) == 'fixture-close' for e in caught.value.cleanup_errors)
    assert not list((tmp_path / 'broken').glob('s3-sort-*'))


def test_unknown_empty_cannot_be_resigned_as_admitted(tmp_path):
    rows = tuple(replace(r, result_id='fixture-unknown-empty',
                         values=tuple((k, 'admitted_empty' if k == 'state' else v) for k, v in r.values))
                 if r.kind == 'result_availability' else replace(r, result_id='fixture-unknown-empty')
                 for r in coverage_rows('fixture-coverage', 'fixture-input', (110, 140), records(0, 'unknown'), 0))
    with pytest.raises(ValueError, match='empty_coverage_fk'):
        write_body(iter(rows), tmp_path / 'false-empty')
