"""真实CanonicalReplay完整映射的E1/E2、双族和ADDPATH零槽见证。"""
from dataclasses import replace
from fractions import Fraction
import json
from tests.country.test_country_m3_qualification import fixture, values
from tests.country.test_country_m3_history import history
from tests.observations.test_observation_ordered import message
from data_pipeline.analysis.country_events.models import Route, Endpoint, Time, Cursor, Binding
from data_pipeline.analysis.country_events.event_aggregation import C2Row, CohortMember
from data_pipeline.analysis.country_events.compute import MetricPoint, Peak
from data_pipeline.analysis.country_events.result_qualification import qualification_rows
from data_pipeline.analysis.country_events.route_contract import CountryQualifiedValue


def witnessed(*,only_e2=False,unmapped=False,folded=False,gap_epoch=23):
    groups=[]
    for n,(epoch,peer,count,status) in enumerate(((10,'192.0.2.1',2,'decoded'),(11,'192.0.2.3',2,'decoded'),(gap_epoch,'192.0.2.1',0,'rejected'),(25,'192.0.2.1',1,'decoded'))):
        m,es=message('s',n,elements=count,status=status,microsecond=0,path_id=0,path_present=True)
        m['epoch']=epoch
        interp=json.loads(m['interpretation']);interp['header']['peer_ip']=peer;m['interpretation']=json.dumps(interp)
        if status=='decoded':m['peer_ip']=peer
        for i,e in enumerate(es):
            e.update(epoch=epoch,peer_ip=peer)
            if i==1:e.update(afi=2,prefix='2001:db8::/32',raw_prefix=b'\x20\x20\x01\x0d\xb8')
        groups.append((m,es))
    h=history(groups,max_gap_candidates=10000);source,raw=fixture()
    source.history=h;source.messages={m['message_id']:m for m,_ in groups};source.binding=h.binding
    oldget=source.get
    def get(owner,view):
        if owner=='canonical' and view in h.rows:return h.rows[view]
        rows=oldget(owner,view)
        if folded and owner=='detection' and view=='m3_entries':
            rows=[dict(r,payload_json=json.dumps({'dimension_coverage':{'origin_anchor':'unknown','candidate_lifecycle':'unknown'}})) for r in rows]
        return rows
    source.get=get
    routes=[]
    for row in h.rows['changes'][:4]:
        scope=row['raw']['scope'];pos=row['position'];p=pos['message']
        if only_e2 and scope[1]!='192.0.2.3':continue
        routes.append(Route(row['object_key'],Endpoint(*scope[:8]),scope[8],scope[10],'present',row['raw']['calculation_after']['path_key'],row['raw']['calculation_after']['origin'],row['event_id'],Time(10+p['record'],0),Cursor(0,p['record'],pos['ordinal']),'canonical-update-mapping',row['message_id'],mapping_state='unmapped' if unmapped else 'bound'))
    baseline=replace(raw[0].value.baseline,at=Time(11,0),cursor=Cursor(0,1,1),binding=Binding('fixture','1',('s',),'ref','decoder','state','rrc25'))
    head=replace(raw[0].value,baseline=baseline,direction_count=len(routes),fixed_prefix_count=2)
    out=[C2Row(raw[0].incident_id,1,head),*(C2Row(raw[0].incident_id,1,CohortMember(head.cohort_id,r)) for r in routes)]
    for sample in (21000000,24000000,26000000):
        value=Fraction(len(routes)) if sample==21000000 or only_e2 else None
        out.append(C2Row(raw[0].incident_id,1,MetricPoint(head.cohort_id,sample,'visible_direction_count','endpoint_direction_count',value,Fraction(999),len(routes),None,'known' if value is not None else 'unknown')))
    out.append(C2Row(raw[0].incident_id,1,Peak(head.cohort_id,'visible_direction_count','endpoint_direction_count',Fraction(len(routes)),21000000,1,1,2)))
    out.append(replace(raw[-1],costs={'output_rows':len(out)}))
    return source,out


def derive(**kwargs):
    source,raw=witnessed(**kwargs)
    return qualification_rows(source,'witnessed',raw,max_rows=10000,max_bytes=4*1024**2,max_references_per_row=128,guard=lambda:None)


def test_fixed_four_directions_gap_subset_two_then_exact_zero_slot_recovery_three():
    rows=derive()
    assert values(rows,'fixed_denominator')[0].value==4
    points=[v for v in values(rows,'point_presence') if v.raw_target_ref[0]=='metric_point']
    assert [v.value for v in points]==[4,None,None]
    assert [v.known_lower_bound for v in points]==[4,2,3]
    assert all(v.lower_bound_proven and v.denominator==4 for v in points)
    peak=next(v for v in values(rows,'window_peak') if v.raw_target_ref[0]=='peak')
    assert peak.value is None and peak.known_lower_bound==4 and peak.lower_bound_proven
    assert len(peak.qualification_refs)==2
    rows.close()


def test_bound_disjoint_e2_keeps_denominator_and_points_but_unknown_mapping_does_not():
    rows=derive(only_e2=True,gap_epoch=18)
    assert values(rows,'fixed_denominator')[0].value==2
    assert [v.value for v in values(rows,'point_presence')]==[2,2,2]
    rows.close()
    rows=derive(only_e2=True,unmapped=True,gap_epoch=18)
    assert values(rows,'fixed_denominator')[0].value is None
    assert all(v.known_lower_bound is None for v in values(rows,'point_presence'))
    rows.close()


def test_canonical_disjoint_cannot_override_detection_folded_anchor_unknown():
    rows=derive(only_e2=True,folded=True,gap_epoch=18)
    assert values(rows,'fixed_denominator')[0].value is None
    assert all(v.value is None and v.known_lower_bound is None for v in values(rows,'point_presence'))
    rows.close()


def test_fixed_disjoint_does_not_prove_potential_new_member_metric():
    source,raw=witnessed(only_e2=True)
    event=raw[0];raw.insert(-1,C2Row(event.incident_id,1,MetricPoint(event.value.cohort_id,24000000,'new_visible_ipv4_prefix_count','prefix_count',Fraction(0),Fraction(0),None,None,'known')))
    raw[-1]=replace(raw[-1],costs={'output_rows':len(raw)-1})
    rows=qualification_rows(source,'potential',raw,max_rows=10000,max_bytes=4*1024**2,max_references_per_row=128,guard=lambda:None)
    try:
        value=values(rows,'new_member_enumeration')[0]
        assert value.value is None and value.coverage=='unknown'
        assert values(rows,'fixed_denominator')[0].value==2
    finally:rows.close()
