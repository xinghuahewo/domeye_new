"""实际Country峰值QV引用原样本下界的最小合同反例；无PG或成功AD。"""
from dataclasses import asdict
from fractions import Fraction as F
import pytest

from data_pipeline.analysis.country_events import event_aggregation as c2
from data_pipeline.analysis.country_events.compute import MetricPoint, Peak
from data_pipeline.analysis.country_events.models import Incident, Time
from data_pipeline.analysis.country_trends.event_inputs import EventSources


def sources(change=None, *, complete=False):
    event=('fixture-peak',2);window=(110,140)
    status=c2.EventStatus(Incident(*event,'US',Time(80)),(),None,'fixture-cohort','available',(),20,1)
    point=MetricPoint('fixture-cohort',120,'visible_direction_count','endpoint_direction_count',F(3) if complete else None,F(3),20,F(3,20) if complete else None,'calculated' if complete else 'unknown')
    peak=Peak('fixture-cohort','visible_direction_count','endpoint_direction_count',F(3),120,1,1,0 if complete else 1)
    originals=tuple((seq,c2.C2Row(*event,value)) for seq,value in ((3,status),(20,point),(21,peak)))
    args=('fixture-run','fixture-input',window,event,originals)
    base=EventSources(*args,(),())
    point_ref=('metric_point',20);peak_ref=('peak',21)
    qs=tuple(dict(asdict(base.targets[key]),qualification_id=qid,dimension=dimension,coverage='complete' if complete else 'unknown')
        for key,qid,dimension in ((point_ref,'q-point','point_presence'),(peak_ref,'q-peak','window_peak')))
    def value(key,dimension,refs):
        return dict(logical_run_id=args[0],input_binding_id=args[1],raw_target_ref=key,dimension=dimension,
            value=F(3) if complete else None,known_lower_bound=F(3),lower_bound_proven=True,unit='endpoint_direction_count',
            population_ref=('event_status',3),denominator=None,raw_basis_refs=(('canonical','fixture-ad-id','changes','fixture-row'),),
            qualification_refs=refs,window_us=window,coverage='complete' if complete else 'unknown',qualified_value_id='value-'+dimension)
    pv=value(point_ref,'point_presence',('q-point',));pk=value(peak_ref,'window_peak',('q-peak','q-point'))
    if change:change(pv,pk)
    return EventSources(*args,qs,(pv,pk)),pk


@pytest.mark.parametrize('complete',[False,True])
def test_peak_keeps_original_exact_or_unknown_value_and_all_refs(complete):
    source,original=sources(complete=complete)
    selected=source.peak('visible_direction_count')
    assert selected.raw==3 and selected.value==(3 if complete else None) and selected.known_lower_bound==3
    assert selected.state==('qualified' if complete else 'unknown')
    assert ('window_peak:unknown' in selected.reasons) is not complete
    assert selected.qualification_refs==('q-peak','q-point')
    assert source.values[(('peak',21),'window_peak')]==original


@pytest.mark.parametrize('field,value',[
    ('known_lower_bound',F(2)),
    ('population_ref',('event_status',999)),
    ('raw_basis_refs',(('canonical','fixture-ad-id','changes','wrong-row'),)),
])
def test_peak_rejects_disconnected_sample_lower_support(field,value):
    source,_=sources(lambda point,peak:point.update({field:value}))
    with pytest.raises(ValueError):source.peak('visible_direction_count')
