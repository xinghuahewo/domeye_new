"""用真实CanonicalReplay人工行验证样本左极限、ET Gap和对象当前恢复。"""
import pytest
from tests.observations.test_observation_ordered import message
from tests.country.test_country_m3_history import history
from tests.observations.test_canonical_projection import key
from data_pipeline.analysis.country_events.route_time_index import state_before


def fixture(direction='received',regression=False):
    a=message('s',0,elements=2,microsecond=0);a[0]['epoch']=100
    gap=message('s',1,status='rejected',direction=direction,microsecond=900000)
    gap[0].update(epoch=101 if not regression else 102,microsecond=None)
    b=message('s',2,elements=1,microsecond=0);b[0]['epoch']=102 if not regression else 101
    rows=[a,gap,b]
    for m,elements in rows:
        for element in elements:element['epoch']=m['epoch']
    h=history(rows)
    return h,{r[0]['message_id']:r[0] for r in rows}


def test_trusted_et_gap_raw_none_left_limit_and_exact_object_recovery():
    h,m=fixture()
    assert m['s:1']['microsecond'] is None
    assert h.rows['scope_gap'][0]['raw']['raw_time']['microsecond']==900000
    assert state_before(h,m,key(),101900000)['presence']=='present'
    assert state_before(h,m,key(),101900001)['presence']=='unknown'
    assert state_before(h,m,key(),102000000)['presence']=='unknown'
    restored=state_before(h,m,key(),102000001)
    assert restored['presence']=='present' and restored['source_ref']['table']=='changes'
    assert restored['continuity']=='partial' and restored['gap_refs']
    assert state_before(h,m,key('198.51.1.0/24'),102000001)['presence']=='unknown'
    assert state_before(h,m,key(),101500000)['presence']=='present'


def test_local_gap_is_not_a_received_time_invalidation():
    h,m=fixture(direction='local')
    result=state_before(h,m,key(),101950000)
    assert result['presence']=='present' and result['gap_refs']==()


def test_regression_uses_source_order_and_reports_unlocatable_boundary():
    h,m=fixture(regression=True)
    result=state_before(h,m,key(),101500000)
    assert result['presence']=='unknown' and result['time_boundary']=='not_locatable'
    assert result['source_ref'] is None and result['gap_refs']==()


def test_shared_index_budget_and_wrong_collector_do_not_silently_succeed():
    h,m=fixture()
    state_before(h,m,key(),102000001)
    n=h.stats['temporal_rows']
    state_before(h,m,key(),102000002)
    assert h.stats['temporal_rows']==n
    h.max_rows=h.stats['rows']+n
    with pytest.raises(ValueError,match='time_index_rows'):
        state_before(h,m,key('198.51.1.0/24'),102000001)
    with pytest.raises(ValueError,match='Collector'):
        state_before(h,m,('other',*key()[1:]),102000001)
