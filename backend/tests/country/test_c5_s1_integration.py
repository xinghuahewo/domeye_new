"""集成复用独立86行手写人工小例；自身目录执行，不访问原复核输出。"""
import os
import json
from pathlib import Path
from dataclasses import replace
from fractions import Fraction as F
from data_pipeline.analysis.country_trends import *
from data_pipeline.analysis.country_trends.contract import TRACKS, REFERENCE_DEFINITION, packed
from data_pipeline.analysis.country_events import event_aggregation as c2
from data_pipeline.analysis.country_events.compute import MetricPoint, PrefixPoint, AsnPoint, Completion
from data_pipeline.analysis.country_events.models import Incident, Time, Route, Endpoint, Cursor
E=('independent-c5',1);C='independent-cohort'
b=FixtureBinding(('own-system',123,'own-catalog','fixture-independent','country_own',7,'country-component/v1','own-sha','/fixture/own'),('read',7),('c1',9))
request=TrendInput(b);values=[];samples=(1000000,2000000,3000000)
values.append(c2.EventStatus(Incident(E[0],1,'ZZ',Time(0)),(),None,C,'available',(),20,2))
prefixes=((1,'10.7.0.0/24'),(2,'2001:db8:7::/49'))
for afi,prefix in prefixes:
 for i in range(10):
  ep=Endpoint('own',f'192.0.2.{i+1}',65001,'192.0.2.254',65002,0,afi,1)
  values.append(c2.CohortMember(C,Route(f'object-{afi}-{i}',ep,prefix,None,'present','own-path',65000,'own-observation',Time(0),Cursor(0,0,i),'mapping','peer')))
expected_raw={}
for i,t in enumerate(samples):
 present=(20,12,18)[i]
 for name in (*TRACKS,'visible_direction_count'):
  unit=dict(c2.METRICS)[name];d=None
  v=F(0)
  if name=='visible_direction_count':v=F(present);d=20
  elif name=='invisible_direction_count':v=F(20-present);d=20
  elif name=='interrupted_prefix_count':v=F(0 if i==0 else 2);d=2
  elif name=='completely_interrupted_prefix_count':d=2
  elif name=='affected_asn_count':v=F(i!=0);d=1
  elif name=='route_interrupted_asn_count':d=1
  elif name=='fixed_visible_ipv4_address_count':v=F(256)
  elif name=='fixed_visible_ipv6_slash48_equivalent':v=F(1,2)
  p=MetricPoint(C,t,name,unit,v,v,d,v/d if d else None,'calculated_zero' if v==0 else 'calculated')
  values.append(p);expected_raw[name,t]=p
 for afi,prefix in prefixes:
  values.append(PrefixPoint(C,t,prefix,'normal' if i==0 else 'partial',present//2,10-present//2,0,10,('own-observation',)))
 for afi,refs in ((0,tuple(p for _,p in prefixes)),(1,(prefixes[0][1],)),(2,(prefixes[1][1],))):
  n=len(refs);values.append(AsnPoint(C,t,65000,afi,'normal' if i==0 else 'affected',n if i==0 else 0,n if i else 0,0,0,refs))
values.append(Completion(C,3,3000000,False,'open',(0,3000000),(1000000,3000000)))
source=[c2.C2Row(*E,v) for v in values]+[c2.C2Completion('fixture',None,1,0,{},())]

def test_fixed_86_rows_science_and_graph(tmp_path):
    outdir=Path(os.environ.get("C5_S1_INTEGRATION_OUTPUT", str(tmp_path.parent/"evidence")));outdir.mkdir(parents=True,exist_ok=True)
    def execute(req,batchsize):
     def stream():
      for i in range(0,len(source),batchsize):yield FixtureBatch(tuple(source[i:i+batchsize]))
      yield FixtureEnd(req.country,len(source))
     return list(compute_trends(req,stream(),temporary_parent=tmp_path))
    out=execute(request,7);tail=out[-1];assert type(tail)is TrendCompletion
    def pick(kind,key=()):
     rows=[r for r in out[:-1] if r.kind==kind and r.key==key];assert len(rows)==1;return rows[0]
    assert {(r.key):r.get('raw') for r in out[:-1] if r.kind=='metric'}==expected_raw
    assert pick('analysis',('visible_direction_count',)).get('pattern')=='single_wave_partial_rebound'
    for name,value in [('loss_magnitude',F(8)),('extreme_to_end_rebound',F(6)),('end_residual_from_start',F(2)),('window_rebound_ratio',F(3,4)),('fixed_cohort_visibility_gap_integral',F(10)),('window_start_visibility_gap_integral',F(10))]:
     assert pick('fact',('visible_direction_count',name)).get('value')==value
    assert pick('family_context').get('maximum_divergence')==0
    for afi in (0,1,2):
     assert pick('asn_population',(afi,1000000)).get('total')==1
     assert dict(pick('asn_population',(afi,2000000)).get('counts'))=={'normal':0,'affected':1,'route_interrupted':0,'unknown':0}
    batched=execute(request,256)
    assert batched[-1].result_id==tail.result_id and batched[-1].output_sha256==tail.output_sha256
    changed=replace(request,country=replace(b,component=(*b.component[:-1],'/fixture/moved')))
    assert execute(changed,7)[-1].result_id!=tail.result_id
    result={'input_rows':tail.input_rows,'output_rows':tail.output_rows,'fraction_half_raw_preserved':True,'ledger_exact':True,'three_afi_separate':True,'batch_identity_stable':True,'physical_identity_changed':True,'input_sha256':tail.input_sha256,'output_sha256':tail.output_sha256}
    (outdir/'账本与身份.json').write_text(json.dumps(result,indent=2));print(result)

    def run(req,rows):
     def stream():
      for i in range(0,len(rows),7):yield FixtureBatch(tuple(rows[i:i+7]))
      yield FixtureEnd(req.country,len(rows))
     return list(compute_trends(req,stream(),temporary_parent=tmp_path))
    original=next(x for x in source if type(x)is c2.C2Row and type(x.value)is MetricPoint and x.value.metric=='invisible_direction_count' and x.value.sample_us==2000000)
    changed=replace(original,value=replace(original.value,value=8,known_lower_bound=8))
    out=run(request,[changed if x is original else x for x in source])
    raw=next(x.get('raw') for x in out[:-1] if x.kind=='metric' and x.key==('invisible_direction_count',2000000))
    assert type(raw.value)is int and raw.ratio==F(2,5)
    sciences=lambda rs:[(x.kind,x.event,x.key,x.values) for x in rs[:-1] if x.kind in ('profile','point','peak','fact')]
    assert sciences(out)==sciences(run(request,source))
    p=Projection('ZZ','fixed_endpoint_direction',20,samples,(F(20),F(12),F(18)),'source:ZZ')
    other=replace(p,country='US',values=(F(20),F(16),F(18)),source_ref='source:US')
    ref=ReferenceInput(E,('fixture','reference'),'ZZ',(p,other))
    req=replace(request,references=(ref,))
    missing=run(req,source)
    assert next(x for x in missing[:-1] if x.kind=='reference_context').get('state')=='insufficient_data'
    p=replace(p,cohort_id=C,definition_binding=REFERENCE_DEFINITION)
    other=replace(other,cohort_id='US-other-cohort',definition_binding=REFERENCE_DEFINITION)
    ref=replace(ref,projections=(p,other))
    activity=ActivityWindow(E,'ordinary','announ_num','accepted_announce_elements',1000000,2000000,F(4),'complete','source:activity',11,'own-source','result')
    req=replace(req,references=(ref,),feature_binding=('fixture','feature'),activities=(activity,))
    out=run(req,source)
    assert next(x for x in out[:-1] if x.kind=='reference_context').get('state')=='complete'
    science={(x.kind,x.event,x.key):x for x in out[:-1] if x.kind not in ('evidence','evidence_source','claim','unknown','limitation','edge')}
    evidence={x.get('source_locator'):x for x in out[:-1] if x.kind=='evidence'}
    for locator,x in science.items():
     if x.kind in ('point','phase','activity_relation','reference_cdf'):
      assert evidence[locator].get('values')==x.values
    for x in out[:-1]:
     if x.kind=='evidence_source':assert x.get('source_locator') in science
    unknown_cdf=evidence['reference_cdf',E,('migration_ratio',)]
    assert not any(x.kind=='claim' and x.get('evidence_id')==unknown_cdf.key[0] for x in out[:-1])
    assert any(x.kind=='unknown' and dict(x.values).get('evidence_id')==unknown_cdf.key[0] for x in out[:-1])
    for wrong in (replace(p,cohort_id='wrong'),replace(p,definition_binding=('wrong',))):
     try:run(replace(req,references=(replace(ref,projections=(wrong,other)),)),source);raise AssertionError('wrong target accepted')
     except ValueError as e:assert 'target_' in str(e)
    try:run(replace(req,schema_version='country-trend-fixture/v1'),source);raise AssertionError('v1 accepted')
    except ValueError as e:assert str(e)=='trend_input_version'
    tail=out[-1]
    result={'int_raw_preserved_and_exact_equivalent':True,'missing_cohort_excluded':True,'different_country_cohorts_complete':True,'all_evidence_sources_resolve':True,'unknown_no_numeric_claim':True,'wrong_target_rejected':True,'v1_rejected':True,'input_rows':tail.input_rows,'output_rows':tail.output_rows,'elapsed_seconds':tail.elapsed_seconds,'process_peak_rss_bytes':tail.process_peak_rss_bytes,'temporary_peak_bytes':tail.temporary_peak_bytes,'input_sha256':tail.input_sha256,'output_sha256':tail.output_sha256}
    (outdir/'图与上下文.json').write_text(json.dumps(result,indent=2));print(result)


    assert len(TRACKS)==15
    assert len([x for x in out[:-1] if x.kind=='metric' and x.key[0] in TRACKS])==45
    assert all(x.result_id==tail.result_id for x in out[:-1])
    assert not list(tmp_path.iterdir())
    assert next(x for x in out[:-1] if x.kind=='reference_cdf' and x.key==('decline_pp',)).get('percentile')==100
    assert next(x for x in out[:-1] if x.kind=='reference_shape').get('share')==1
    assert [x.get('share') for x in out[:-1] if x.kind=='reference_common']==[1,0]
    (outdir/'全部输出typed.json').write_bytes(packed(tuple(out[:-1])))
    from data_pipeline.analysis.country_events.snapshot_schema import encode
    (outdir/'全部输入typed.json').write_text(encode(tuple((x.incident_id,x.revision,x.value) if type(x) is c2.C2Row else x for x in source)))
