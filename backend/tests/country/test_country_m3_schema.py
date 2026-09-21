"""仅人工类型合同验证；fixture引用不冒充实际Admission或端到端资格。"""
from dataclasses import asdict, replace
from fractions import Fraction
import json

import pytest

from data_pipeline.analysis.country_events import snapshot_schema as old
from data_pipeline.analysis.country_events import qualified_schema as schema
from data_pipeline.analysis.country_events.event_aggregation import C2Row, InputEvidence
from data_pipeline.analysis.country_events.route_contract import CountryQualification, CountryCoverage, CountryQualifiedValue, DIMENSIONS
from data_pipeline.bgp.record_types import GapScope, ScopeKind


REF = ('canonical', 'a' * 64, 'source_coverage', 'fixture-source:2')


def qualification(**changes):
    scope = json.loads(json.dumps(asdict(GapScope(ScopeKind.COLLECTOR_CHAIN, 'rrc25', 'fixture-binding', None, None))))
    values = dict(logical_run_id='fixture-logical-run', input_binding_id='fixture-binding',
                  target_kind='module', raw_target_ref=None, incident_id=None, revision=None,
                  cohort_id=None, entity_key=None, afi=None, metric=None, dimension='event_enumeration',
                  dependency_scope=old.encode(scope), effective_start_position=(0, 0, 0, 0),
                  effective_end_position=None, sample_us=None, window_us=(100, 200), coverage='unknown',
                  gap_refs=(), upstream_qualification_refs=(), evidence_refs=(REF,), recovery_witnesses=())
    return CountryQualification(**(values | changes))


def coverage():
    return CountryCoverage('fixture-logical-run', 'fixture-binding', 'country', None, (100, 200),
                           'event_enumeration', (0, 0, 0, 0), None, 0, 'complete', 'unknown',
                           1, 'b' * 64, 1, 'c' * 64, (REF,), (REF,))


def qualified():
    return CountryQualifiedValue('fixture-logical-run', 'fixture-binding', ('metric_point', 2),
                                  'point_presence', None, 2, True, 'route_directions', ('cohort_member', 1),
                                  4, (REF,), (qualification().qualification_id,), (100, 200), 'unknown')


def test_exact_21_plus_3_and_original_row_bytes_unchanged():
    assert len(old.TABLES) == 21 and len(schema.TABLES) == 24 and len(DIMENSIONS) == 12
    for table in old.TABLES:
        assert schema.SCHEMAS[table] == old.SCHEMAS[table]
    row = C2Row(None, None, InputEvidence('fixture', 'original', {'raw': b'\xff', 'unknown': None}))
    assert schema.row_encode(0, row) == old.row_encode(0, row)
    table, physical = schema.row_encode(0, row)
    assert schema.row_decode(table, physical) == row


@pytest.mark.parametrize('value', [qualification(), coverage(), qualified()])
def test_new_rows_roundtrip_and_detect_same_count_tampering(value):
    item = C2Row(None, None, value)
    table, physical = schema.row_encode(7, item)
    assert schema.row_decode(table, physical) == item
    with pytest.raises(ValueError):
        schema.row_decode(table, dict(physical, dimension='window_peak'))
    with pytest.raises(ValueError):
        old.row_encode(7, item)


def test_unknown_visibility_keeps_proven_lower_bound_and_fixed_d_separate():
    value = qualified()
    assert value.value is None and value.known_lower_bound == 2 and value.denominator == 4
    with pytest.raises(ValueError, match='主值必须'):
        replace(value, value=0, qualified_value_id='')
    with pytest.raises(ValueError, match='未证明下界'):
        replace(value, known_lower_bound=0, lower_bound_proven=False, qualified_value_id='')
    missing = replace(value, known_lower_bound=None, lower_bound_proven=False, denominator=None, qualified_value_id='')
    table, physical = schema.row_encode(8, C2Row(None, None, missing))
    assert schema.row_decode(table, physical).value == missing
    exact = replace(value, known_lower_bound=Fraction(2, 3), qualified_value_id='')
    table, physical = schema.row_encode(9, C2Row(None, None, exact))
    assert schema.row_decode(table, physical).value == exact


def test_zero_event_coverage_has_no_fabricated_incident():
    value = coverage()
    assert value.event_count == 0 and value.coverage == 'unknown'
    with pytest.raises(ValueError, match='不能伪造事件'):
        schema.row_encode(0, C2Row('made-up', 1, value))


def test_sample_boundary_keeps_original_c2_right_endpoint_semantics():
    assert qualification(sample_us=200).sample_us == 200
    with pytest.raises(ValueError, match='网格窗口'):
        qualification(sample_us=100)


def test_identity_binds_dimension_position_scope_and_original_evidence():
    value = qualification()
    for changes in (dict(dimension='event_anchor'), dict(effective_start_position=(2, 0, 0, 0)),
                    dict(evidence_refs=(('canonical', 'd' * 64, 'source_coverage', 'other-source'),))):
        with pytest.raises(ValueError, match='身份冲突'):
            replace(value, **changes)
        assert replace(value, qualification_id='', **changes).qualification_id != value.qualification_id
    with pytest.raises(ValueError):
        qualification(dimension='arbitrary-expression')
    with pytest.raises(ValueError):
        qualification(effective_start_position=(2, 0, 0, 3))
    with pytest.raises(ValueError):
        qualification(evidence_refs=(('detection', 'a' * 64, 'state_entries', 'private-state'),))


def test_real_parquet_preserves_unknown_and_large_exact_lower_bound(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    value = replace(qualified(), known_lower_bound=2**80, denominator=2**81, qualified_value_id='')
    table, row = schema.row_encode(3, C2Row(None, None, value))
    kinds = {'VARCHAR': pa.string(), 'BIGINT': pa.int64(), 'BOOLEAN': pa.bool_(), 'BLOB': pa.binary()}
    physical_schema = pa.schema([(name, kinds[kind]) for name, kind, _ in schema.SCHEMAS[table]])
    path = tmp_path/'qualified.parquet'
    pq.write_table(pa.Table.from_pylist([row], schema=physical_schema), path)
    actual, = pq.read_table(path).to_pylist()
    restored = schema.row_decode(table, actual).value
    assert restored == value and restored.value is None and restored.known_lower_bound == 2**80
