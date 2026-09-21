"""C2正式保存链与单独typed稀有情况，前者不以注入事件冒充Detection。"""
from dataclasses import replace
from collections import Counter
import os
import pytest
from data_pipeline.analysis.country_events.event_aggregation import run_saved, run_typed, C2Row, C2Completion, EventStatus, CohortMember, BoundaryUnavailable
from data_pipeline.analysis.country_events.compute import Change, Grid, MetricPoint, PrefixPoint, ObservationFact, compute_country_enhancement, PathSummary
from data_pipeline.analysis.country_events.models import Cursor, Time
from data_pipeline.analysis.country_events.incremental_types import FirstQualifiedRef, DirectionPoint, AsnMetricPeak
from tests.country.test_country_enhancement_cohort import binding, incident, route, refs
from tests.country.test_country_enhancement_compute import paths, change
from tests.country.test_country_saved_input import build_pipeline, adapter, multiple_country_mrt


def run_case(tmp_path,events=None,changes=None,seeds=None,samples=(21,26,31),**kw):
    initial=seeds or [route()]
    baseline_changes=[Change(r.cursor,r.observed_at,binding().ordered_sources[0],r.observation_ref,r) for r in initial]
    return list(run_typed(binding(),Grid(15000000,31000000,tuple(round(t*1000000) for t in samples)),events or [incident()],baseline_changes+(changes or []),paths(),refs(),scratch_root=tmp_path,**kw))


def payload(rows,kind,incident_id=None):
    return [r.value for r in rows if isinstance(r,C2Row) and isinstance(r.value,kind) and (incident_id is None or r.incident_id==incident_id)]


def test_single_baseline_core_full_rows_match(tmp_path):
    changes=[change(0,23,presence='absent',path_ref=None),change(1,28)]
    rows=run_case(tmp_path,changes=changes)
    header=payload(rows,EventStatus)[0]
    from data_pipeline.analysis.country_events.cohort import freeze_cohorts
    cohort=freeze_cohorts((incident(),),header.baseline,[route()],refs()[:1])[0]
    old=[r for b in compute_country_enhancement((cohort,),binding(),Grid(15000000,31000000,(21000000,26000000,31000000)),changes,paths()) for r in b.rows]
    old_types={type(r) for r in old}
    new=[r.value for r in rows if isinstance(r,C2Row) and type(r.value) in old_types]
    assert Counter(map(repr,new))==Counter(map(repr,old))
    assert [r.state for r in payload(rows,PrefixPoint)]==['normal','complete','normal']
    assert payload(rows,DirectionPoint)
    assert payload(rows,AsnMetricPeak)
    assert rows[-1].costs['global_units_scanned']==3


def test_different_event_baselines(tmp_path):
    event2=replace(incident(),incident_id='second',onset=Time(27,0),detected_at=Time(27,0))
    rows=run_case(tmp_path,events=[incident(),event2],changes=[change(0,23,presence='absent',path_ref=None),change(1,28)])
    heads=payload(rows,EventStatus)
    assert len({h.baseline.cursor for h in heads})==2
    assert [h.fixed_prefix_count for h in heads]==[1,0]
    assert rows[-1].costs['global_units_scanned']==3


def test_formal_saved_two_country_onsets(tmp_path):
    base=os.environ.get('DOMEYE_COUNTRY_C1_TEST_DSN')
    if not base:pytest.skip('需要自有人工PG')
    prepared=build_pipeline(tmp_path,base,multiple_country_mrt(tmp_path),rich=True,multicountry=True)
    c1=adapter(prepared)
    rows=list(run_saved(c1,Grid(100000000,120000000,(106000000,107000000,112000000,113000000,120000000)),scratch_root=tmp_path))
    headers=payload(rows,EventStatus)
    assert {h.incident.country for h in headers}=={'ZZ','YY'}
    assert len({h.baseline.cursor for h in headers})==2
    assert all(h.direction_count==1 and h.fixed_prefix_count==1 for h in headers)
    for h in headers:
        target=107000000 if h.incident.country=='ZZ' else 113000000
        assert next(p for p in payload(rows,PrefixPoint,h.incident.incident_id) if p.sample_us==target).state=='complete'
    assert rows[-1].input_kind=='saved_C1'
    assert rows[-1].event_count==2


def test_revision_selection_and_complete_empty(tmp_path):
    rows=run_case(tmp_path,events=[incident(),replace(incident(),revision=2)])
    h=payload(rows,EventStatus)[0]
    assert h.incident.revision==2 and len(h.revision_refs)==2
    rows=list(run_typed(binding(),Grid(15000000,31000000,(21000000,)),[],[],paths(),refs(),scratch_root=tmp_path))
    assert rows[-1].event_count==0 and rows[-1].costs['global_units_scanned']==0


def test_missing_baseline_scoped_and_pre_detection(tmp_path):
    tooearly=replace(incident(),incident_id='tooearly',onset=Time(9))
    unknown=replace(incident(),incident_id='unknown-onset',onset=None)
    rows=run_case(tmp_path,events=[tooearly,unknown])
    hs={h.incident.incident_id:h for h in payload(rows,EventStatus)}
    assert hs['tooearly'].state=='unavailable'
    assert hs['unknown-onset'].state=='available' and hs['unknown-onset'].incident.onset is None


def test_second_precision_only_affected_sample_unknown(tmp_path):
    changes=[change(0,Time(23),presence='absent',path_ref=None)]
    rows=run_case(tmp_path,changes=changes,samples=(23,23.5,24))
    points={r.sample_us:r.state for r in payload(rows,PrefixPoint)}
    assert points=={23000000:'normal',23500000:'unknown',24000000:'complete'}
    peak=next(r for r in payload(rows,AsnMetricPeak) if r.afi==0 and r.metric=='complete_prefix_count')
    assert peak.known_peak==1 and peak.exact_peak is None and peak.unknown_slots==1
    from data_pipeline.analysis.country_events.compute import Completion
    assert payload(rows,Completion)[0].sample_count==3


def test_microsecond_and_right_open(tmp_path):
    rows=run_case(tmp_path,changes=[change(0,Time(23,500000),presence='absent',path_ref=None),change(1,Time(31,0))],samples=(23.5,23.500001,31))
    assert [r.state for r in payload(rows,PrefixPoint)]==['normal','complete','complete']
    assert not any(r.reference=='update-1' for r in payload(rows,ObservationFact))


def test_rollback_only_related_country_affected(tmp_path):
    us=route('us',prefix='203.0.113.0/24',origin=64498,cursor=Cursor(0,2,0))
    rows=run_case(tmp_path,seeds=[route(),us],events=[incident(),incident('US')],changes=[change(0,27,presence='absent',path_ref=None),change(1,23)],samples=(21,25,28))
    assert {p.sample_us:p.state for p in payload(rows,PrefixPoint,'incident-IR')}=={21000000:'normal',25000000:'unknown',28000000:'normal'}
    assert all(p.state=='normal' for p in payload(rows,PrefixPoint,'incident-US'))


def test_state_group_and_original_a_local(tmp_path):
    a=route();b=route('b',endpoint=replace(a.endpoint,local_ip='192.0.2.9'),cursor=Cursor(0,2,0))
    invalid=Change(Cursor(1,0,0),Time(23,0),'u1','state-0',invalidated_objects=(a.object_id,b.object_id))
    local=change(1,24,local_message=True)
    rows=run_case(tmp_path,seeds=[a,b],changes=[invalid,local],samples=(23,24,26))
    assert [r.state for r in payload(rows,PrefixPoint)]==['normal','unknown','unknown']
    assert len([r for r in payload(rows,DirectionPoint) if r.sample_us==24000000])==2
    assert payload(rows,ObservationFact)[0].route.fact_presence==a.fact_presence
    assert any(r.route.local_message for r in payload(rows,ObservationFact))


def test_global_gap_persists_into_later_event(tmp_path):
    gap=Change(Cursor(1,0,0),Time(23,0),'u1','gap',gap=True)
    later=replace(incident(),incident_id='later',onset=Time(25,0))
    rows=run_case(tmp_path,events=[incident(),later],changes=[gap],samples=(26,31))
    assert all(p.state=='unknown' for p in payload(rows,PrefixPoint))
    assert all(m.value is None for m in payload(rows,MetricPoint) if m.metric.startswith('new_'))


def test_new_country_other_unknown_withdraw_keeps_membership(tmp_path):
    new=route('new',prefix='203.0.113.0/24')
    changes=[change(0,21,new),change(1,23,new,origin=64498),change(2,25,new,origin=None),change(3,27,new,presence='absent',path_ref=None)]
    rows=run_case(tmp_path,changes=changes,samples=(22,24,26,28))
    def vals(name):return [m.value for m in payload(rows,MetricPoint) if m.metric==name]
    assert vals('new_visible_ipv4_prefix_count')==[1,1,1,0]
    assert vals('new_cumulative_ipv4_prefix_count')==[1,1,1,1]
    first=payload(rows,FirstQualifiedRef)
    assert len(first)==1 and first[0].first_qualified_ref=='update-0'
    assert payload(rows,EventStatus)[0].fixed_prefix_count==1


@pytest.mark.parametrize('batch_rows',[1,2,256])
def test_all_typed_rows_and_nested_order_batch_invariant(tmp_path,batch_rows):
    changes=[change(0,23,presence='absent',path_ref=None),change(1,28)]
    events=[replace(incident(),incident_id=f'e{i}') for i in range(4)]
    actual=run_case(tmp_path,events=events,changes=changes,batch_rows=batch_rows)
    expected=run_case(tmp_path,events=events,changes=changes,batch_rows=256)
    assert Counter(repr(r) for r in actual if isinstance(r,C2Row))==Counter(repr(r) for r in expected if isinstance(r,C2Row))
    one=run_case(tmp_path,events=events[:1],changes=changes)
    assert actual[-1].costs['global_units_scanned']==one[-1].costs['global_units_scanned']==3
    assert actual[-1].costs['staging_sql_writes']-one[-1].costs['staging_sql_writes']==3


@pytest.mark.parametrize('limits',[{'max_row_bytes':1},{'max_disk_bytes':1},{'max_rss_bytes':1},{'max_active':1}])
def test_resource_limits_cleanup(tmp_path,limits):
    with pytest.raises(ValueError,match='resource_limit'):
        run_case(tmp_path,events=[incident(),replace(incident(),incident_id='second')],changes=[change(0,23)],**limits)
    assert not list(tmp_path.glob('country-c2-*'))


def test_incomplete_or_bad_tail_no_completion(tmp_path):
    with pytest.raises(ValueError,match='typed_input_not_complete'):
        run_case(tmp_path,complete=False)
    with pytest.raises(ValueError,match='C2_source_cursor_mismatch'):
        run_case(tmp_path,changes=[replace(change(0,40),source_id='wrong')])


def test_closed_events_release_active_resources(tmp_path):
    first=replace(incident(),end=Time(22,0))
    second=replace(incident(),incident_id='second',onset=Time(25,0))
    rows=run_case(tmp_path,events=[first,second],changes=[change(0,23),change(1,28)],max_active=1,max_total_members=1)
    assert len(payload(rows,EventStatus))==2


def test_moas_addpath_dual_af_overlap_set_and_exact_fraction(tmp_path):
    from fractions import Fraction
    from data_pipeline.analysis.country_events.compute import Path, Segment
    a=route()
    seeds=[a,route('addpath',path_id=7,origin=64498,cursor=Cursor(0,2,0)),
           route('endpoint',endpoint=replace(a.endpoint,local_ip='192.0.2.8'),cursor=Cursor(0,3,0)),
           route('overlap',prefix='198.51.100.0/25',cursor=Cursor(0,4,0)),
           route('v6',endpoint=replace(a.endpoint,afi=2),prefix='2001:db8::/49',cursor=Cursor(0,5,0)),
           route('set',path_id=8,path_ref='set-path',origin=None,cursor=Cursor(0,6,0))]
    changes=[Change(r.cursor,r.observed_at,'rib',r.observation_ref,r) for r in seeds]
    pp=(*paths(),Path('set-path',(Segment('set',(64497,64498)),),b'raw-set'))
    rows=list(run_typed(binding(),Grid(15000000,31000000,(21000000,)),[incident()],changes,pp,refs(),scratch_root=tmp_path))
    head=payload(rows,EventStatus)[0]
    assert head.direction_count==4 and head.fixed_prefix_count==3
    metrics={m.metric:m for m in payload(rows,MetricPoint)}
    assert metrics['fixed_visible_ipv4_address_count'].value==256
    assert metrics['fixed_visible_ipv6_slash48_equivalent'].value==Fraction(1,2)
    assert {m.route.origin for m in payload(rows,CohortMember)}=={64497,64498,None}
    from data_pipeline.analysis.country_events.compute import PathSample
    assert any(r.route.path_ref=='set-path' and r.route.origin is None for r in payload(rows,ObservationFact))
    from data_pipeline.analysis.country_events.cohort import freeze_cohorts
    cohort=freeze_cohorts((incident(),),head.baseline,sorted(seeds,key=lambda r:(r.prefix,r.object_id)),refs())[0]
    old=[r for b in compute_country_enhancement((cohort,),binding(),Grid(15000000,31000000,(21000000,)),(),pp) for r in b.rows]
    kinds={type(r) for r in old}
    assert Counter(map(repr,old))==Counter(repr(r.value) for r in rows if isinstance(r,C2Row) and type(r.value) in kinds)


def test_multi_event_matches_individual_all_typed_rows(tmp_path):
    events=[incident(),replace(incident(),incident_id='later',onset=Time(27,0))]
    changes=[change(0,23,presence='absent',path_ref=None),change(1,28)]
    multi=run_case(tmp_path,events=events,changes=changes)
    for event in events:
        single=run_case(tmp_path,events=[event],changes=changes)
        assert Counter(repr(r) for r in multi if isinstance(r,C2Row) and r.incident_id==event.incident_id)==Counter(repr(r) for r in single if isinstance(r,C2Row) and r.incident_id==event.incident_id)


from tests.country.test_country_c1_repair import review_chain


def test_formal_state_local_precision_tail_and_batches(review_chain):
    import json
    grid=Grid(100000000,125000000,(107000000,113000000,120000000,121000000,121500000,124000000,125000000))
    import time
    begin=time.monotonic()
    rows=list(run_saved(adapter(review_chain),grid,scratch_root=review_chain[0]))
    inclusive256=time.monotonic()-begin
    begin=time.monotonic()
    other=list(run_saved(adapter(review_chain,batch_rows=1),grid,scratch_root=review_chain[0],batch_rows=1))
    inclusive1=time.monotonic()-begin
    assert Counter(repr(r) for r in rows if isinstance(r,C2Row))==Counter(repr(r) for r in other if isinstance(r,C2Row))
    assert rows[-1].costs['global_units_scanned']==27
    assert rows[-1].costs['staged_invalidations']==9
    assert any(f.route.local_message for f in payload(rows,ObservationFact))
    assert rows[-1].input_completion.counts['changes']==25
    (review_chain[0]/'c2-costs.json').write_text(json.dumps({'batch256':rows[-1].costs,'batch1':other[-1].costs,'including_C1_constructor_seconds':{'batch256':inclusive256,'batch1':inclusive1}},indent=2))


def test_event_count_scaling_includes_stage_and_output(tmp_path):
    import json
    cases={}
    for count in (1,8,32):
        events=[replace(incident(),incident_id=f'e{i}') for i in range(count)]
        changes=[change(i,21+i%10,presence='present') for i in range(60)]
        rows=run_case(tmp_path,events=events,changes=changes)
        cases[count]=rows[-1].costs
    assert {c['global_units_scanned'] for c in cases.values()}=={61}
    assert cases[32]['output_rows']>cases[1]['output_rows']
    assert cases[32]['related_changes_sent']==32*cases[1]['related_changes_sent']
    (tmp_path/'scaling-costs.json').write_text(json.dumps(cases,indent=2))


def test_formal_consumer_never_decodes_or_replays_again(review_chain,monkeypatch):
    import data_pipeline.bgp.replay.route_replay as state
    import data_pipeline.bgp.input.mrt_reader as mrt
    def forbidden(*args,**kwargs):raise AssertionError('C2不能重放或再解码MRT')
    monkeypatch.setattr(state.Replay,'consume',forbidden)
    # 确切MRT迭代入口；保存路径字节仍由C2解释，未触碰原MRT。
    monkeypatch.setattr(mrt,'read_source',forbidden)
    rows=list(run_saved(adapter(review_chain),Grid(100000000,125000000,(113000000,125000000)),scratch_root=review_chain[0]))
    assert isinstance(rows[-1],C2Completion)


def test_all_unknown_asn_peaks_do_not_forge_zero(tmp_path):
    rows=run_case(tmp_path,changes=[change(0,Time(23),presence='absent',path_ref=None)],samples=(23.5,))
    assert payload(rows,AsnMetricPeak)
    assert all(p.known_peak is None and p.exact_peak is None and p.unknown_slots==1 for p in payload(rows,AsnMetricPeak))


def test_path_peak_quality_marks_unlocatable_slot(tmp_path):
    from data_pipeline.analysis.country_events.incremental_types import PathPeakQuality
    rows=run_case(tmp_path,changes=[change(0,Time(23),presence='absent',path_ref=None)],samples=(21,23.5,26))
    quality=payload(rows,PathPeakQuality)
    assert quality and all(p.known_slots==2 and p.unknown_slots==1 and p.exact_prefix_peak is None for p in quality)


def test_formal_complete_empty(tmp_path):
    base=os.environ.get('DOMEYE_COUNTRY_C1_TEST_DSN')
    if not base:pytest.skip('需要自有人工PG')
    from tests.observations.test_observation_consumer import observation_fixture
    prepared=build_pipeline(tmp_path,base,observation_fixture(tmp_path),rich=False)
    rows=list(run_saved(adapter(prepared),Grid(100000000,125000000,(113000000,125000000)),scratch_root=tmp_path))
    assert rows[-1].input_completion.enumeration=='complete_empty'
    assert rows[-1].event_count==0 and not payload(rows,EventStatus)


def test_ambiguous_mapping_never_claims_available(tmp_path):
    rows=run_case(tmp_path,seeds=[route(mapping_state='ambiguous')])
    head=payload(rows,EventStatus)[0]
    assert head.state=='partial' and head.direction_count is None
    assert 'ambiguous_baseline_mapping' in head.reasons
