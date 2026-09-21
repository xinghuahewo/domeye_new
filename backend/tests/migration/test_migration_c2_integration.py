"""C2非空附加链：复用已接受独立手写MRT方案，全部原件和PG由本测试新建。"""
import struct,gzip,hashlib,json,time
from pathlib import Path
from collections import Counter
from data_pipeline.bgp.input.mrt_reader import source_identity
from tests.country.test_country_saved_input import build_pipeline, adapter
from tests.observations.test_observation_consumer import feature_dsn
from data_pipeline.analysis.country_events.event_aggregation import run_saved, C2Row, C2Completion, EventStatus
from data_pipeline.analysis.country_events.compute import Grid, PrefixPoint, ObservationFact

def test_formal_two_country_c2_integration(tmp_path,feature_dsn):
 root=tmp_path
 def mrt(payload,t,kind,sub):return struct.pack('!IHHI',t,kind,sub,len(payload))+payload
 def endpoint(p):return struct.pack('!IIHH',100+p,999,0,1)+bytes([192,0,2,p+1,192,0,2,254])
 def path(p,origin):return bytes([0x40,2,10,2,2])+struct.pack('!II',100+p,origin)
 def bgp(w=b'',a=b'',attrs=b''):
  body=struct.pack('!H',len(w))+w+struct.pack('!H',len(attrs))+attrs+a
  return b'\xff'*16+struct.pack('!HB',19+len(body),2)+body
 peers=struct.pack('!IH',25,5)+b'rrc25'+struct.pack('!H',3)
 for p in range(3):peers+=b'\x02'+bytes([192,0,2,p+1])*2+struct.pack('!I',100+p)
 rib=mrt(peers,90,13,1);withdraws=b''
 # 原件预期：ZZ起源1/2，YY起源3/4；106/112前最后方向均为Peer102。
 for n in range(4):
  nlri=bytes([24,172,16,n]);body=struct.pack('!I',n)+nlri+struct.pack('!H',3)
  for p in range(3):
   attrs=path(p,n+1);body+=struct.pack('!HIH',p,90,len(attrs))+attrs
   withdraws+=mrt(endpoint(p)+bgp(w=nlri),101+3*n+p,16,4)
  rib+=mrt(body,90,13,2)
 extra=mrt(endpoint(0)+bgp(a=bytes([24,172,20,0]),attrs=path(0,1)),120,16,4)
 for p in (0,1):extra+=mrt(endpoint(p)+struct.pack('!HH',6,1),121,16,5)
 extra+=mrt(endpoint(0)+bgp(a=bytes([24,172,20,0]),attrs=path(0,1)),122,16,7)
 extra+=mrt(struct.pack('!I',123456)+endpoint(0)+bgp(),123,17,4)+mrt(endpoint(0)+bgp(),122,16,4)
 inputs=[]
 for name,raw,role in [('rib',rib,'baseline'),('withdraws',withdraws,'update'),('extra',extra,'update'),('empty',b'','update')]:
  p=root/(name+'.gz');p.write_bytes(gzip.compress(raw,mtime=0));sha=hashlib.sha256(p.read_bytes()).hexdigest();uri='fixture://independent-c2/'+name
  inputs.append(dict(path=str(p),sha256=sha,size=p.stat().st_size,origin_uri=uri,source_id=source_identity('rrc25',uri,sha),role=role))
 manifest=dict(schema_version='observation-run/v1',collector='rrc25',window_start='1970-01-01T00:00:00Z',window_end_exclusive='1970-01-02T00:00:00Z',inputs=inputs,baseline_source=inputs[0]['source_id'],update_sources=[x['source_id'] for x in inputs[1:]])
 (root/'manifest-source.json').write_text(json.dumps(manifest))
 prepared=build_pipeline(root,feature_dsn,manifest,rich=True,multicountry=True)
 grid=Grid(100000000,125000000,(107000000,113000000,120000000,121000000,121500000,124000000,125000000));runs=[]
 for batch in (1,2):
  t=time.monotonic();rows=list(run_saved(adapter(prepared,batch_rows=batch),grid,scratch_root=root,batch_rows=batch));elapsed=time.monotonic()-t
  end=rows[-1];assert isinstance(end,C2Completion)
  assert end.input_completion.counts['changes']==25 and end.input_completion.counts['invalidations']==9
  assert end.costs['global_units_scanned']==27
  assert len(rows)==411 and end.costs['output_rows']==410
  assert end.costs['active_member_reservations']==end.costs['released_active_members']==7
  assert end.reference_interpretation['selected_rows']==8
  assert not list(root.glob('country-c2-*')) and not list(root.glob('country-c1-*'))
  assert end.reference_interpretation
  (root/'c2-full-rows.txt').write_text('\n'.join(repr(r) for r in rows))
  heads=[x.value for x in rows if isinstance(x,C2Row) and isinstance(x.value,EventStatus)]
  assert len(heads)==2
  expected={'ZZ':(106,'172.16.1.0/24',107000000),'YY':(112,'172.16.3.0/24',113000000)}
  for h in heads:
   onset,prefix,sample=expected[h.incident.country];assert h.incident.onset.epoch==onset
   assert h.fixed_prefix_count==h.direction_count==1
   selected=next(x[2] for x in h.reference_selections if x[0]==(2 if h.incident.country=='ZZ' else 4))
   assert selected['row']==(2 if h.incident.country=='ZZ' else 4)
   from data_pipeline.analysis.country_events.event_aggregation import CohortMember
   members=[x.value.route for x in rows if isinstance(x,C2Row) and x.incident_id==h.incident.incident_id and isinstance(x.value,CohortMember)]
   present=[x for x in members if x.presence=='present'];assert len(present)==1 and present[0].prefix==prefix and present[0].endpoint.remote_asn==102
   point=next(x.value for x in rows if isinstance(x,C2Row) and x.incident_id==h.incident.incident_id and isinstance(x.value,PrefixPoint) and x.value.sample_us==sample);assert point.state=='complete'
  facts=[x.value for x in rows if isinstance(x,C2Row) and isinstance(x.value,ObservationFact)]
  assert sum(x.route.local_message for x in facts)==1
  runs.append((Counter(repr(x) for x in rows if isinstance(x,C2Row)),dict(batch=batch,elapsed_including_C1=elapsed,costs=end.costs,rows=len(rows))))
  assert runs[-1][0]==runs[0][0]
 (root/'independent-evidence.json').write_text(json.dumps([x[1] for x in runs],indent=2));print(json.dumps([dict(batch=x[1]['batch'],rows=x[1]['rows'],elapsed=x[1]['elapsed_including_C1'],units=x[1]['costs']['global_units_scanned']) for x in runs]))
