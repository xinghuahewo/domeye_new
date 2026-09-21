"""S2见证事件/样本/cohort定点合同；不修改S1默认规则。"""
from dataclasses import replace
import pytest
from tests.country_trend.test_country_trend_s1 import fixture
from data_pipeline.analysis.country_events import event_aggregation as c2
from data_pipeline.analysis.country_trends.compute import MetricPoint
from data_pipeline.analysis.country_trends.snapshot_qualification import validate_unknown_witnesses


def case():
    _,items=fixture((100,None,80))
    rows=[r for r in items if isinstance(r,c2.C2Row) and r.incident_id is not None]
    event=(rows[0].incident_id,rows[0].revision);sample=2_000_000
    values=[replace(r.value,denominator=None) if isinstance(r.value,MetricPoint) and r.value.metric=='visible_direction_count' and r.value.sample_us==sample else r.value for r in rows]
    values.append(c2.BoundaryUnavailable(event[0],sample,sample,(1,2)))
    return event,values


def test_legal_unknown_is_not_rewritten():
    event,values=case();before=repr(values)
    validate_unknown_witnesses(event,values)
    assert repr(values)==before


@pytest.mark.parametrize('damage',['inner_event','boundary_time','sample','cohort','known_value','outer_revision'])
def test_finite_witness_identity(damage):
    event,values=case()
    if damage=='outer_revision':event=(event[0],event[1]+1)
    elif damage in ('inner_event','boundary_time','sample'):
        values[-1]=replace(values[-1],**({'incident_id':'WRONG-EVENT'} if damage=='inner_event' else {'boundary_us':9} if damage=='boundary_time' else {'sample_us':9}))
    else:
        values=[replace(v,**({'cohort_id':'other'} if damage=='cohort' else {'value':0})) if isinstance(v,MetricPoint) and v.metric=='visible_direction_count' and v.denominator is None else v for v in values]
    with pytest.raises(ValueError,match='trend_witness_'):validate_unknown_witnesses(event,values)
