"""S2有限typed合同反例；纯核对照不授予正式输入资格。"""
from dataclasses import replace
from fractions import Fraction
import pytest
from data_pipeline.analysis.country_trends.snapshot_schema import flatten, restore
from data_pipeline.analysis.country_trends.contract import row
from tests.country_trend.test_country_trend_s1 import fixture, run
from tests.country_trend.test_country_trend_s1_repair import positive_reference


def test_s2_all_science_fields_roundtrip():
    for request,items in (fixture(),fixture(()),fixture((None,None)),positive_reference()):
        rows,_=run(request,items)
        for i,r in enumerate(rows):
            assert repr(restore(r.kind,flatten(i,r)))==repr(r)


@pytest.mark.parametrize('value',[None,0,Fraction(0),Fraction(2**128-1,2**80)])
def test_s2_raw_types(value):
    r=replace(row('point',('a',7),('metric','first'),sample_us=1,value=value,unit='x'),result_id='id')
    assert type(dict(restore('point',flatten(0,r)).values)['value']) is type(value)


@pytest.mark.parametrize('damage',['unknown_kind','extra_field','missing_field','hash','revision'])
def test_s2_reject_structural_damage(damage):
    r=replace(row('point',('a',7),('metric','first'),sample_us=1,value=Fraction(1,3),unit='x'),result_id='id')
    if damage=='unknown_kind':
        with pytest.raises(ValueError):flatten(0,replace(r,kind='arbitrary_table'))
    elif damage=='extra_field':
        with pytest.raises(ValueError):flatten(0,replace(r,values=(*r.values,('extra',0))))
    elif damage=='missing_field':
        with pytest.raises(ValueError):flatten(0,replace(r,values=r.values[:-1]))
    else:
        f=flatten(0,r);f['row_sha256' if damage=='hash' else 'revision']='0' if damage=='hash' else '9'
        with pytest.raises(ValueError):restore(r.kind,f)


def test_strict_unknown_denominator_adaptation_is_finite():
    from data_pipeline.analysis.country_trends.compute import event_rows
    from data_pipeline.analysis.country_trends.contract import Limits
    from data_pipeline.analysis.country_events import event_aggregation as c2
    from data_pipeline.analysis.country_events.compute import MetricPoint
    request,items=fixture((100,None,80))
    event=('fixture-incident',1)
    entries=[r for r in items if isinstance(r,c2.C2Row) and r.incident_id is not None]
    event=(entries[0].incident_id,entries[0].revision)
    values=[r.value for r in entries]
    sample=2_000_000
    changed=[replace(v,denominator=None,ratio=None) if isinstance(v,MetricPoint) and v.metric=='visible_direction_count' and v.sample_us==sample else v for v in values]
    changed.append(c2.BoundaryUnavailable(event[0],sample,sample,(1,2)))
    formal=lambda vv:list(event_rows(event,vv,request,Limits(),denominator_rule='strict_c3_unknown_denominator/v1'))
    rows=formal(changed)
    point=next(r for r in rows if r.kind=='metric' and r.key==('visible_direction_count',sample))
    assert point.get('raw').denominator is None and point.get('raw').value is None
    assert next(r for r in rows if r.kind=='profile' and r.key==('visible_direction_count',)).get('denominator') is None
    with pytest.raises(ValueError,match='denominator_drift'):list(event_rows(event,changed,request,Limits()))
    with pytest.raises(ValueError,match='unproven'):formal(changed[:-1])
    for damage in ('different_nonempty','known_missing','cohort'):
        bad=[]
        for v in changed:
            if isinstance(v,MetricPoint) and v.metric=='visible_direction_count' and v.sample_us==sample:
                if damage=='different_nonempty':v=replace(v,denominator=101)
                elif damage=='known_missing':v=replace(v,value=Fraction(0),state='calculated_zero')
                else:v=replace(v,cohort_id='other')
            bad.append(v)
        with pytest.raises(ValueError):formal(bad)


def test_unknown_enum_and_sequence_rejected():
    r=replace(row('profile',('a',1),('x',),state='invented',unit='x',denominator=None,sample_count=0),result_id='id')
    with pytest.raises(ValueError,match='enum'):flatten(0,r)
    r=replace(r,values=tuple((k,'no_samples' if k=='state' else v) for k,v in r.values))
    with pytest.raises(ValueError,match='key'):flatten(-1,r)


def test_formal_entry_has_no_fixture_bypass(tmp_path):
    from data_pipeline.analysis.country_trends.snapshot_store import prepare_trend
    with pytest.raises(ValueError,match='冻结新进程'):prepare_trend(None,None,tmp_path/'not-created')
    assert not (tmp_path/'not-created').exists()
