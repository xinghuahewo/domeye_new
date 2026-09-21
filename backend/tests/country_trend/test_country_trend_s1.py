"""C5 S1：同一公开入口，手写预期；只用人工typed行，不开PG。"""
from dataclasses import replace
from fractions import Fraction as F
import pytest

from data_pipeline.analysis.country_trends import compute_trends, TrendInput, FixtureBinding, FixtureBatch, FixtureEnd, Limits, TrendCompletion, ActivityWindow, ReferenceInput, Projection
from data_pipeline.analysis.country_trends.contract import TRACKS, REFERENCE_DEFINITION
from data_pipeline.analysis.country_events import event_aggregation as c2
from data_pipeline.analysis.country_events.compute import MetricPoint, PrefixPoint, AsnPoint, Completion, Quality
from data_pipeline.analysis.country_events.models import Incident, Time, Route, Endpoint, Cursor

EVENT=('fixture-event',1)
BINDING=FixtureBinding(('fixture-db',1,'fixture-catalog','fixture-country','country_fixture',1,'country-component/v1','fixture-sha','/fixture/country'),('fixture-read',),('fixture-c1',))


def fixture(values=(100,80,90), denominator=100, *, matrix=True, families=True):
    """两prefix共享一个ASN；每族denominator/2个不同端点；无真实资格。"""
    request=TrendInput(BINDING)
    samples=tuple((i+1)*1_000_000 for i in range(len(values)))
    incident=Incident(EVENT[0],1,'US',Time(0))
    members=[]
    if families:
        for afi,prefix in ((1,'10.0.0.0/24'),(2,'2001:db8::/48')):
            count=denominator//2 if afi==1 else denominator-denominator//2
            for n in range(count):
                ep=Endpoint('fixture',f'192.0.2.{n+1}',64501,'192.0.2.254',64502,0,afi,1)
                members.append(c2.CohortMember('fixture-cohort',Route(f'{afi}-{n}',ep,prefix,None,'present','path',64500,'obs',Time(0),Cursor(0,0,n),'mapping','peer')))
    rows=[c2.EventStatus(incident,(),None,'fixture-cohort','available',(),denominator,2 if families else 0),*members]
    for i,t in enumerate(samples):
        value=None if values[i] is None else F(values[i])
        for name in (*TRACKS,'visible_direction_count'):
            unit=dict(c2.METRICS)[name]
            v=value if name=='visible_direction_count' else F(0)
            d=denominator if unit in ('prefix_count','endpoint_direction_count','asn_count') and not name.startswith('new_') else None
            rows.append(MetricPoint('fixture-cohort',t,name,unit,v,F(0) if v is None else v,d,v/d if v is not None and d else None,'unknown' if v is None else 'calculated_zero' if v==0 else 'calculated'))
        if families:
            parts=(None,None) if value is None else (value//2,value-value//2)
            for afi,prefix,present in ((1,'10.0.0.0/24',parts[0]),(2,'2001:db8::/48',parts[1])):
                d=denominator//2 if afi==1 else denominator-denominator//2
                # 非整数方向不在双族fixture中伪造；专门精度例修改资源轨。
                if present is not None and present.denominator!=1:
                    raise ValueError('fixture integer directions required')
                present=int(present) if present is not None else 0
                unknown=d if value is None else 0
                rows.append(PrefixPoint('fixture-cohort',t,prefix,'unknown' if unknown else 'normal' if present==d else 'complete' if present==0 else 'partial',present,d-present-unknown,unknown,d,('obs',)))
            if matrix:
                state='unknown' if value is None else 'normal' if value==denominator else 'route_interrupted' if value==0 else 'affected'
                for afi,prefixes in ((0,('10.0.0.0/24','2001:db8::/48')),(1,('10.0.0.0/24',)),(2,('2001:db8::/48',))):
                    n=len(prefixes)
                    rows.append(AsnPoint('fixture-cohort',t,64500,afi,state,n if state=='normal' else 0,n if state=='affected' else 0,n if state=='route_interrupted' else 0,n if state=='unknown' else 0,prefixes))
    rows.append(Completion('fixture-cohort',len(samples),samples[-1] if samples else 0,False,'open',(0,max(1,len(samples))*1_000_000),(samples[0],samples[-1]) if samples else None))
    stream_rows=[c2.C2Row(*EVENT,v) for v in rows]
    stream_rows.append(c2.C2Completion('fixture',None,1,0,{},()))
    return request,stream_rows


def stream(request,rows,batch_size=31):
    for i in range(0,len(rows),batch_size):
        yield FixtureBatch(tuple(rows[i:i+batch_size]))
    yield FixtureEnd(request.country,len(rows))


def run(request,rows,**kwargs):
    result=list(compute_trends(request,stream(request,rows),**kwargs))
    assert isinstance(result[-1],TrendCompletion)
    assert all(r.result_id==result[-1].result_id for r in result[:-1])
    return result[:-1],result[-1]


def select(rows,kind,key=None):
    result=[r for r in rows if r.kind==kind and (key is None or r.key==key)]
    assert len(result)==1,(kind,key,len(result))
    return result[0]


@pytest.mark.parametrize('values,denominator,pattern',[
    ((9,0,9),9,'small_denominator'),
    ((100,99,100),100,'plateau'),
    ((100,90,100,90,100,90,100,90,100),100,'oscillation'),
    ((100,90,80),100,'multi_wave'),
    ((100,80,90),100,'single_wave_partial_rebound'),
    ((100,80,100),100,'single_wave_return_to_window_start'),
    ((90,70,100),100,'single_wave_above_window_start'),
    ((100,80),100,'mixed'),
    ((100,97,94),100,'unmatched'),
    ((10,8,10),10,'single_wave_return_to_window_start'),
])
def test_pattern_branches(values,denominator,pattern):
    rows,_=run(*fixture(values,denominator))
    assert select(rows,'analysis',('visible_direction_count',)).get('pattern')==pattern


def test_ledger_thresholds_points_and_units():
    rows,_=run(*fixture())
    expected={'start_to_extreme_change':-20,'loss_magnitude':20,'extreme_to_end_rebound':10,'end_residual_from_start':10,'window_rebound_ratio':F(1,2),'fixed_cohort_visibility_gap_integral':30,'window_start_visibility_gap_integral':30}
    for name,value in expected.items():
        fact=select(rows,'fact',('visible_direction_count',name))
        assert fact.get('value')==value
        assert fact.get('formula') and fact.get('operands')
    assert select(rows,'fact',('visible_direction_count','fixed_cohort_visibility_gap_integral')).get('unit')=='endpoint_direction_sample'
    assert [select(rows,'threshold',('visible_direction_count',t)).get('count') for t in (F(95,100),F(9,10),F(4,5))]==[2,1,0]
    assert select(rows,'point',('visible_direction_count','minimum')).get('sample_us')==2_000_000
    assert select(rows,'family_context').get('state')=='complete'
    assert select(rows,'family_context').get('maximum_divergence')==0


@pytest.mark.parametrize('values,states',[
    ((100,98,90,98,100),('stable','decline','abrupt_drop','abrupt_rise','rise')),
    ((80,80,80),('low_plateau',)*3),
    ((9,8,9),('stable','decline','rise')),
])
def test_atomic_exact_boundaries(values,states):
    rows,_=run(*fixture(values,9 if values[0]==9 else 100))
    assert tuple(r.get('state') for r in rows if r.kind=='atomic')==states


@pytest.mark.parametrize('values',[(100,100,100),(90,95,100)])
def test_zero_loss(values):
    rows,_=run(*fixture(values))
    assert select(rows,'fact',('visible_direction_count','window_rebound_ratio')).get('value') is None


@pytest.mark.parametrize('values,state',[((), 'no_samples'),((100,), 'complete'),((None,None),'unavailable'),((100,None,90),'degraded'),((None,100),'degraded')])
def test_empty_single_unknown(values,state):
    rows,_=run(*fixture(values))
    assert select(rows,'profile',('visible_direction_count',)).get('state')==state
    assert not any(r.kind in ('phase','fact','analysis') for r in rows)
    if None in values:
        assert select(rows,'family_context').get('state')=='unavailable'
        assert select(rows,'asn_summary',(64500,0)).get('longest_runs')[-1][1] <= 1


def test_exact_resource_fraction_and_raw_basis():
    request,items=fixture()
    target=next(i for i,r in enumerate(items) if isinstance(r,c2.C2Row) and isinstance(r.value,MetricPoint) and r.value.metric=='fixed_visible_ipv6_slash48_equivalent')
    value=F(1,2**80)
    original=replace(items[target].value,value=value,known_lower_bound=value,state='calculated')
    items[target]=replace(items[target],value=original)
    rows,_=run(request,items)
    assert select(rows,'metric',(original.metric,original.sample_us)).get('raw')==original
    assert not any(r.kind=='fact' and r.key[0]==original.metric for r in rows)


def test_matrix_missing_preserves_metrics():
    rows,_=run(*fixture(matrix=False))
    assert select(rows,'asn_context').get('state')=='unavailable'
    assert len([r for r in rows if r.kind=='metric' and r.key[0] in TRACKS])==45


def test_activity_exact_interval_and_reference_positive():
    request,items=fixture()
    activity=ActivityWindow(EVENT,'ordinary','announ_num','accepted_announce_elements',1_000_000,2_000_000,F(7),'complete','fixture-aw')
    target=Projection('US','fixed_endpoint_direction',100,(1_000_000,2_000_000,3_000_000),(F(100),F(80),F(90)),'fixture-us',asn_count=2,persistent_asn_count=1,cohort_id='fixture-cohort',definition_binding=REFERENCE_DEFINITION)
    other=replace(target,country='CA',cohort_id='fixture-ca-cohort',values=(F(100),F(90),F(100)),source_ref='fixture-ca',persistent_asn_count=0)
    excluded=replace(other,country='__UNKNOWN__')
    reference=ReferenceInput(EVENT,('fixture','reference-fixed'),'US',(target,other,excluded))
    request=replace(request,feature_binding=('fixture','feature-fixed'),activities=(activity,),references=(reference,))
    rows,_=run(request,items)
    assert select(rows,'activity_window').get('anchor_us')==2_000_000
    assert select(rows,'activity_relation',('ordinary','announ_num','minimum')).get('relation')=='same_slot'
    assert select(rows,'reference_context').get('state')=='complete'
    assert select(rows,'reference_cdf',('decline_pp',)).get('percentile')==100
    assert select(rows,'reference_cdf',('migration_ratio',)).get('percentile')==100
    assert select(rows,'reference_exclusion',('__UNKNOWN__',)).get('reason')=='unknown_bucket'
    assert select(rows,'reference_common',(2_000_000,)).get('share')==1


@pytest.mark.parametrize('mutation,error',[
    ('duplicate_metric','duplicate_metric'),('duplicate_prefix','duplicate_prefix'),
    ('duplicate_asn','duplicate_asn'),('completion','completion_grid'),
    ('member','duplicate_member'),('missing_metric','metric_matrix'),
])
def test_malformed_input(mutation,error):
    request,items=fixture()
    cls={'duplicate_metric':MetricPoint,'duplicate_prefix':PrefixPoint,'duplicate_asn':AsnPoint,'member':c2.CohortMember}.get(mutation)
    if cls:items.insert(0,next(r for r in items if isinstance(r,c2.C2Row) and isinstance(r.value,cls)))
    elif mutation=='completion':
        i=next(i for i,r in enumerate(items) if isinstance(r,c2.C2Row) and isinstance(r.value,Completion))
        items[i]=replace(items[i],value=replace(items[i].value,sample_count=99))
    else:items.pop(next(i for i,r in enumerate(items) if isinstance(r,c2.C2Row) and isinstance(r.value,MetricPoint) and r.value.metric==TRACKS[0]))
    with pytest.raises(ValueError,match=error):run(request,items)


def test_m3_and_real_identity_rejected():
    request,items=fixture()
    for request in (replace(request,data_kind='real'),replace(request,country=replace(BINDING,interpretation='partial')),replace(request,country=replace(BINDING,qualification='qualification_id-filled'))):
        with pytest.raises(ValueError,match='fixture_only|m3_not_supported'):run(request,items)


def test_no_end_no_normal_completion(tmp_path):
    request,items=fixture()
    with pytest.raises(ValueError,match='input_not_complete'):
        list(compute_trends(request,iter([FixtureBatch(tuple(items[:100]))]),temporary_parent=tmp_path))
    assert list(tmp_path.iterdir())==[]


def test_early_close_and_budget_cleanup(tmp_path):
    request,items=fixture()
    generator=compute_trends(request,stream(request,items),temporary_parent=tmp_path)
    next(generator);generator.close()
    assert list(tmp_path.iterdir())==[]
    for limits in (Limits(max_rows=2),Limits(max_event_bytes=1),Limits(max_disk_bytes=1),Limits(max_samples=1),Limits(max_rss_bytes=1)):
        with pytest.raises(ValueError,match='budget|limit'):
            list(compute_trends(request,stream(request,items),temporary_parent=tmp_path,limits=limits))
        assert list(tmp_path.iterdir())==[]


def test_empty_event_set():
    request=TrendInput(BINDING)
    rows,receipt=run(request,[c2.C2Completion('fixture',None,0,0,{},())])
    assert rows==[] and receipt.events==0 and receipt.output_rows==0


def test_identity_changes_with_physical_input_binding():
    request,items=fixture()
    _,a=run(request,items)
    moved=replace(request,country=replace(BINDING,component=(*BINDING.component[:-1],'/fixture/moved')))
    _,b=run(moved,items)
    assert a.result_id!=b.result_id
    _,c=run(request,items)
    assert a.result_id==c.result_id and a.output_sha256==c.output_sha256


def mutate(items,cls,fn):
    return [replace(r,value=fn(r.value)) if isinstance(r,c2.C2Row) and isinstance(r.value,cls) else r for r in items]


@pytest.mark.parametrize('case',['missing_prefix','unknown_expected','mapping_unknown','global_gap'])
def test_family_qualification_never_upgrades_integer_present(case):
    request,items=fixture()
    if case=='missing_prefix':items.pop(next(i for i,r in enumerate(items) if isinstance(r,c2.C2Row) and isinstance(r.value,PrefixPoint)))
    elif case=='unknown_expected':items=mutate(items,PrefixPoint,lambda p:replace(p,expected=None))
    elif case=='mapping_unknown':items=mutate(items,c2.CohortMember,lambda m:replace(m,route=replace(m.route,mapping_state='ambiguous')))
    else:items.insert(0,c2.C2Row(*EVENT,Quality('fixture-cohort','source_gap','fixture-global-gap')))
    rows,_=run(request,items)
    assert select(rows,'family_context').get('state')=='unavailable'
    assert all(r.get('value') is None for r in rows if r.kind=='family_point' and (case!='missing_prefix' or r.key[1]==1_000_000))


def test_family_sum_mismatch_rejected():
    request,items=fixture()
    items=mutate(items,PrefixPoint,lambda p:replace(p,present=p.present-1,absent=p.absent+1) if p.prefix.startswith('10.') else p)
    with pytest.raises(ValueError,match='family_total_mismatch'):run(request,items)


def test_asn_partition_unknown_and_different_time_peaks():
    request,items=fixture((100,80))
    # 固定cohort包含四个原origin，每个均有两族成员。
    items=mutate(items,c2.CohortMember,lambda m:replace(m,route=replace(m.route,origin=64500+(int(m.route.object_id.split('-')[1])%4))))
    items=[r for r in items if not (isinstance(r,c2.C2Row) and isinstance(r.value,AsnPoint))]
    for t in (1_000_000,2_000_000):
        for n,state in enumerate(('normal','affected','route_interrupted','unknown')):
            for afi,prefixes in ((0,('10.0.0.0/24','2001:db8::/48')),(1,('10.0.0.0/24',)),(2,('2001:db8::/48',))):
                count=len(prefixes)
                items.insert(0,c2.C2Row(*EVENT,AsnPoint('fixture-cohort',t,64500+n,afi,state,count if state=='normal' else 0,count if state=='affected' else 0,count if state=='route_interrupted' else 0,count if state=='unknown' else 0,prefixes)))
    items=mutate(items,MetricPoint,lambda p:replace(p,value=None,known_lower_bound=F(1),ratio=None,state='unknown') if p.metric in ('affected_asn_count','route_interrupted_asn_count') else p)
    rows,_=run(request,items)
    population=select(rows,'asn_population',(0,1_000_000))
    assert population.get('counts')==( ('normal',1),('affected',1),('route_interrupted',1),('unknown',1))
    for metric in ('affected_asn_count','route_interrupted_asn_count'):
        raw=select(rows,'metric',(metric,1_000_000)).get('raw')
        assert raw.value is None and raw.known_lower_bound==1
    # 三ASN、两类独立峰异时。移除第四origin，所有三族矩阵同步。
    items=[r for r in items if not (isinstance(r,c2.C2Row) and (isinstance(r.value,c2.CohortMember) and r.value.route.origin==64503 or isinstance(r.value,AsnPoint) and r.value.asn==64503))]
    items=mutate(items,AsnPoint,lambda p:replace(p,state='affected' if p.sample_us==1_000_000 else 'route_interrupted',normal=0,partial=len(p.prefix_refs) if p.sample_us==1_000_000 else 0,complete=len(p.prefix_refs) if p.sample_us==2_000_000 else 0,unknown=0))
    def peak_metric(p):
        if p.metric not in ('affected_asn_count','route_interrupted_asn_count'):return p
        value=F(3 if (p.metric=='affected_asn_count')==(p.sample_us==1_000_000) else 0)
        return replace(p,value=value,known_lower_bound=value,denominator=3,ratio=value/3,state='calculated' if value else 'calculated_zero')
    items=mutate(items,MetricPoint,peak_metric)
    rows,_=run(request,items)
    assert [select(rows,'peak',(m,)).get('exact_peak') for m in ('affected_asn_count','route_interrupted_asn_count')]==[3,3]
    assert [select(rows,'peak',(m,)).get('first_us') for m in ('affected_asn_count','route_interrupted_asn_count')]==[1_000_000,2_000_000]
    assert all(select(rows,'asn_population',(0,t)).get('total')==3 for t in (1_000_000,2_000_000))


@pytest.mark.parametrize('bounds,aligned',[( (1_000_000,2_000_000),True),((0,1_000_000),False),((1_000_001,2_000_000),False),((1_000_000,3_000_000),False)])
def test_activity_half_open_boundaries(bounds,aligned):
    request,items=fixture()
    w=ActivityWindow(EVENT,'ir','withdraw_num','accepted_withdraw_elements',*bounds,F(0),'complete','fixture-w')
    request=replace(request,feature_binding=('fixture','feature'),activities=(w,))
    rows,_=run(request,items)
    result=select(rows,'activity_window')
    assert result.get('raw')==w
    assert result.get('state')==('aligned' if aligned else 'unavailable')
    assert result.get('anchor_us')==(2_000_000 if aligned else None)


def test_duplicate_activity_and_invalid_population():
    request,items=fixture()
    w=ActivityWindow(EVENT,'ordinary','announ_num','accepted_announce_elements',1_000_000,2_000_000,F(3),'complete','fixture-a')
    for windows,error in (((w,w),'duplicate_activity'),((replace(w,population='update_message'),),'activity_population')):
        with pytest.raises(ValueError,match=error):run(replace(request,feature_binding=('fixture',),activities=windows),items)


def reference_case():
    request,items=fixture()
    p=Projection('US','fixed_endpoint_direction',100,(1_000_000,2_000_000,3_000_000),(F(100),F(80),F(90)),'fixture-US',cohort_id='fixture-cohort',definition_binding=REFERENCE_DEFINITION)
    return request,items,p


@pytest.mark.parametrize('change,reason',[
    ({'country':'__UNKNOWN__'},'unknown_bucket'),({'country':'bad'},'invalid_country'),
    ({'population':'legacy_prefix_vp'},'population_mismatch'),({'denominator':0},'invalid_denominator'),
    ({'denominator':9},'small_denominator'),({'samples':(1,2,3)},'grid_mismatch'),
    ({'quality':'unknown'},'quality_incomplete'),({'source_ref':''},'source_unavailable')])
def test_reference_exclusion_branches(change,reason):
    request,items,p=reference_case()
    other=replace(p,country='CA',**{k:v for k,v in change.items() if k!='country'})
    if 'country' in change:other=replace(other,country=change['country'])
    reference=ReferenceInput(EVENT,('fixture','reference'),'US',(p,other))
    rows,_=run(replace(request,references=(reference,)),items)
    assert select(rows,'reference_exclusion',(other.country,)).get('reason')==reason
    assert select(rows,'reference_context').get('state')=='insufficient_data'


@pytest.mark.parametrize('change',[{'population':'legacy_prefix_vp'},{'denominator':101},{'samples':(1,2,3)},{'values':(F(100),F(81),F(90))}])
def test_reference_target_mismatch(change):
    request,items,p=reference_case()
    ref=ReferenceInput(EVENT,('fixture','reference'),'US',(replace(p,**change),replace(p,country='CA')))
    with pytest.raises(ValueError,match='target_mismatch'):run(replace(request,references=(ref,)),items)


def test_reference_cdf_ties():
    request,items,p=reference_case()
    ref=ReferenceInput(EVENT,('fixture','reference'),'US',(p,replace(p,country='CA')))
    rows,_=run(replace(request,references=(ref,)),items)
    assert select(rows,'reference_cdf',('decline_pp',)).get('percentile')==100
    assert select(rows,'reference_cdf',('migration_ratio',)).get('percentile') is None


def test_graph_edges_and_bounded_claims():
    rows,_=run(*fixture())
    nodes={r.key[0] for r in rows if r.kind in ('claim','evidence','unknown','limitation')}
    for edge in (r for r in rows if r.kind=='edge'):
        assert edge.key[0] in nodes and edge.key[2] in nodes
        assert edge.key[1] in ('supported_by','limited_by','unknown_about')
    for claim in (r for r in rows if r.kind=='claim'):
        assert claim.get('conclusion_level')=='bounded_control_plane'
        assert {r.key[1] for r in rows if r.kind=='edge' and r.key[0]==claim.key[0]}=={'supported_by','limited_by','unknown_about'}


def test_failure_after_output_has_no_completion_and_closes(tmp_path):
    request,items=fixture()
    emitted=[]
    iterator=compute_trends(request,stream(request,items),temporary_parent=tmp_path,limits=Limits(max_rows=len(items)+10))
    with pytest.raises(ValueError,match='row_budget'):
        for item in iterator:emitted.append(item)
    assert emitted and not any(isinstance(r,TrendCompletion) for r in emitted)
    assert list(tmp_path.iterdir())==[]


def test_unavailable_cohort():
    request,items=fixture()
    status=next(r for r in items if isinstance(r,c2.C2Row) and isinstance(r.value,c2.EventStatus))
    status=replace(status,value=replace(status.value,cohort_id=None,state='unavailable',reasons=('missing_baseline',)))
    rows,_=run(request,[status,c2.C2Completion('fixture',None,1,1,{},())])
    assert select(rows,'availability').get('state')=='missing_baseline'
    assert not any(r.kind=='metric' for r in rows)


def test_exact_threshold_not_rounded_before_comparison():
    # 95%-epsilon在展示六位处相等，但科学阈值仍严格低于95%。
    request,items=fixture((100,95,100))
    epsilon=F(1,10**9)
    items=mutate(items,MetricPoint,lambda p:replace(p,value=F(95)-epsilon,known_lower_bound=F(95)-epsilon,ratio=(F(95)-epsilon)/100) if p.metric=='visible_direction_count' and p.sample_us==2_000_000 else p)
    # 无效族人口显式Unknown，不把非整数方向伪装成原prefix求和。
    items=mutate(items,PrefixPoint,lambda p:replace(p,expected=None))
    rows,_=run(request,items)
    assert select(rows,'threshold',('visible_direction_count',F(95,100))).get('count')==1
    fact=select(rows,'fact',('visible_direction_count','loss_magnitude'))
    assert fact.get('value')==5+epsilon
    assert fact.get('rounded_value')==5


def test_plateau_total_variation_boundary_and_tie_first():
    rows,_=run(*fixture((100,98,100,98,100,98)))
    assert select(rows,'analysis',('visible_direction_count',)).get('pattern')=='plateau'
    assert select(rows,'point',('visible_direction_count','minimum')).get('sample_us')==2_000_000
    rows,_=run(*fixture((100,98,100,98,100,98,100)))
    assert select(rows,'analysis',('visible_direction_count',)).get('pattern')=='unmatched'


def test_unknown_breaks_persistence_and_phase():
    rows,_=run(*fixture((80,None,80)))
    summary=select(rows,'asn_summary',(64500,0))
    assert summary.get('persistent_not_at_start') is None
    assert dict(summary.get('longest_runs'))['observed_non_normal']==1
    assert not any(r.kind=='phase' for r in rows)


def test_wrong_fixture_end_or_trailing_input_rejected():
    request,items=fixture()
    for ending in ([FixtureEnd(BINDING,len(items)+1)], [FixtureEnd(BINDING,len(items)),FixtureBatch(())]):
        batches=[FixtureBatch(tuple(items[i:i+31])) for i in range(0,len(items),31)]
        with pytest.raises(ValueError,match='input_not_complete|after_input_end'):
            list(compute_trends(request,iter(batches+ending)))


def test_shared_guard_failure_closes_input(tmp_path):
    request,items=fixture()
    closed=[]
    def source():
        try:yield from stream(request,items)
        finally:closed.append(True)
    count=0
    def guard():
        nonlocal count
        count+=1
        if count==10:raise RuntimeError('fixture-stop')
    with pytest.raises(RuntimeError,match='fixture-stop'):
        list(compute_trends(request,source(),guard=guard,temporary_parent=tmp_path))
    assert closed==[True] and not list(tmp_path.iterdir())


def test_reference_duplicate_country_rejected():
    request,items,p=reference_case()
    ref=ReferenceInput(EVENT,('fixture','reference'),'US',(p,p))
    with pytest.raises(ValueError,match='duplicate_country'):run(replace(request,references=(ref,)),items)


def test_original_pure_formula_comparison():
    # 固定新项目旧纯函数，独立对照原合法整数画像；不调用旧服务或外部环境。
    from services.country_outage_trend_profile import compile_trend_profile_v1,analyze_trend_profile_v1
    old_input={
        'schema_version':'country_outage_trend_profile_input_v1',
        'snapshot':{'event_type':'country_outage','event_reference':'country_outage/0/US/1/fixture','incident_id':'fixture-event','country_code':'US','collector_id':'rrc25','collector_count':1,'publication_id':'fixture-old','revision':1,'data_through':'2026-01-01T00:02:00Z','is_final':False,'window_start_utc':'2026-01-01T00:00:00Z','window_end_utc':'2026-01-01T00:02:00Z','timezone':'UTC'},
        'metric':{'metric_id':'visible_prefix_vp_count','label':'旧人工方向对应值','unit':'count','statistical_population':'fixed_prefix_vp','denominator':{'value':100,'unit':'count','statistical_population':'fixed_prefix_vp'}},
        'time_grid':{'slot_seconds':60,'expected_slot_count':3},'baseline':{'type':'fixed_cohort'},
        'slots':[{'index':i,'observed_at_utc':f'2026-01-01T00:0{i}:00Z','state':'observed','value':value,'source_ref':f'fixture:{i}'} for i,value in enumerate((100,80,90))],
    }
    old=analyze_trend_profile_v1(compile_trend_profile_v1(old_input))
    rows,_=run(*fixture())
    assert old['analysis']['pattern']['label']==select(rows,'analysis',('visible_direction_count',)).get('pattern')
    for fact in old['analysis']['derived_facts']:
        assert select(rows,'fact',('visible_direction_count',fact['metric'])).get('rounded_value')==fact['value']
    assert old['metric']['statistical_population']=='fixed_prefix_vp'
    assert select(rows,'fact',('visible_direction_count','fixed_cohort_visibility_gap_integral')).get('unit')!='prefix_vp_slot'


def test_activity_window_role_preserved_not_inferred():
    request,items=fixture()
    w=ActivityWindow(EVENT,'ordinary','announ_num','accepted_announce_elements',1_000_000,2_000_000,F(1),'complete','fixture-a',7,'fixture-source-7','comparison')
    rows,_=run(replace(request,feature_binding=('fixture',),activities=(w,)),items)
    assert select(rows,'activity_window').get('raw')==w
    assert select(rows,'activity_window').get('state')=='unavailable'


def test_input_batch_shape_does_not_change_identity():
    request,items=fixture()
    a=list(compute_trends(request,stream(request,items,1)))[-1]
    b=list(compute_trends(request,stream(request,items,256)))[-1]
    assert a.result_id==b.result_id and a.output_sha256==b.output_sha256


def test_cleanup_does_not_mask_primary_failure():
    request,_=fixture()
    class BrokenSource:
        def __iter__(self):return self
        def __next__(self):raise RuntimeError('primary-fixture-failure')
        def close(self):raise ValueError('secondary-close-failure')
    with pytest.raises(RuntimeError,match='primary-fixture-failure'):
        list(compute_trends(request,BrokenSource()))


def test_global_c3_evidence_preserved_without_fake_event():
    from data_pipeline.analysis.country_events.compute import ObservationFact
    from data_pipeline.analysis.country_events.snapshot_schema import decode
    request,items=fixture()
    route=next(r.value.route for r in items if isinstance(r,c2.C2Row) and isinstance(r.value,c2.CohortMember))
    originals=(c2.InputEvidence('fixture','fixture-global',{'unknown':None}),ObservationFact('obs',route,False))
    items=[c2.C2Row(None,None,v) for v in originals]+items
    rows,receipt=run(request,items)
    actual=[decode(r.get('raw_typed'))[2] for r in rows if r.kind=='source_evidence' and r.event==()]
    assert actual==list(originals) and receipt.events==1
