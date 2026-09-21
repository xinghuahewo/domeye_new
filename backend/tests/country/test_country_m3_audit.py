"""M3本地关系和预算反例；不替代实际C3/C4及上游准入验收。"""
from dataclasses import replace
import pytest

from tests.country.test_country_m3_schema import qualification, coverage
from data_pipeline.analysis.country_events.event_aggregation import C2Row, InputEvidence
from data_pipeline.analysis.country_events.route_reference_audit import M3RowAudit


def audit(**limits):
    return M3RowAudit('fixture-logical-run', 'fixture-binding', {((100, 200), 'event_enumeration')},
                      **(dict(max_rows=100, max_bytes=1024**2, max_intervals=50,
                              max_references_per_row=20, max_total_references=100,
                              max_overlap_steps=100, max_dimensions_per_target=12,
                              guard=lambda: None) | limits))


def test_zero_event_coverage_is_independent_and_required():
    a = audit()
    a.append(0, C2Row(None, None, qualification()))
    a.append(1, C2Row(None, None, coverage()))
    assert a.finish()['rows'] == 2
    empty = audit()
    with pytest.raises(ValueError, match='覆盖缺项'):
        empty.finish()


def test_forward_raw_target_fk_and_cross_incident_are_checked():
    a = audit()
    q = qualification(target_kind='metric', raw_target_ref=('input_evidence', 2))
    a.append(0, C2Row(None, None, q))
    a.append(1, C2Row(None, None, coverage()))
    with pytest.raises(ValueError, match='目标缺失'):
        a.finish()
    a.append(2, C2Row(None, None, InputEvidence('fixture', 'raw', {})))
    a.finish()
    bad = audit()
    bad.append(0, C2Row(None, None, q))
    bad.append(1, C2Row(None, None, coverage()))
    bad.append(2, C2Row('another-event', 1, InputEvidence('fixture', 'raw', {})))
    with pytest.raises(ValueError, match='事件绑定冲突'):
        bad.finish()


def test_overlap_and_dimension_budget_cannot_reset_per_row():
    a = audit(max_dimensions_per_target=1)
    a.append(0, C2Row(None, None, qualification()))
    with pytest.raises(ValueError, match='dimensions'):
        a.append(1, C2Row(None, None, qualification(dimension='event_anchor')))
    with pytest.raises(ValueError):
        a.finish()
    overlap = audit()
    overlap.append(0, C2Row(None, None, qualification()))
    with pytest.raises(ValueError, match='区间重叠'):
        overlap.append(1, C2Row(None, None, qualification(effective_start_position=(2, 1, 0, 0))))


def test_reference_budget_and_noncontinuous_sequence_fail_closed():
    a = audit(max_total_references=2)
    a.append(0, C2Row(None, None, qualification()))
    a.append(1, C2Row(None, None, coverage()))
    assert a.finish()['references']>2
    b = audit()
    with pytest.raises(ValueError, match='行序'):
        b.append(1, C2Row(None, None, qualification()))


def test_actual_event_count_cannot_be_replaced_by_arbitrary_zero_or_success():
    a = audit()
    a.append(0, C2Row(None, None, qualification()))
    a.append(1, C2Row(None, None, replace(coverage(), event_count=1, coverage_id='')))
    with pytest.raises(ValueError, match='事件数'):
        a.finish()
