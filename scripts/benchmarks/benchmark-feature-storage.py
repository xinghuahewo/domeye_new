#!/usr/bin/env python3
"""仅新建隔离数据库和合成MRT，度量保存观察→双Feature→持久化→读取。"""
import argparse
import csv
import gzip
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
import time
import uuid
import psycopg2

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'backend'),str(ROOT/'backend/tests')]
from tests.observations.test_observation_mrt import mrt, attr, update
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.analysis.features.calculation import FileWindow
from data_pipeline.analysis.features.projection import SourceBinding, FeaturePlan
from data_pipeline.analysis.features.reference import USECOLS
from data_pipeline.analysis.features.run import run_fixture
from data_pipeline.analysis.features.store import read_table
from datetime import datetime,timezone

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--output',type=Path,required=True)
p.add_argument('--prefixes',type=int,default=1024)
p.add_argument('--files',type=int,default=8)
a=p.parse_args()
base=os.environ['DOMEYE_FEATURE_TEST_DSN']
if not 1<=a.prefixes<=65536 or not 1<=a.files<=100:raise ValueError('人工规模无效')
a.output.mkdir(parents=True,exist_ok=False)
pg=psycopg2.connect(base);pg.autocommit=True
name='feature_bench_'+uuid.uuid4().hex
with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
pg.close();dsn=base+' dbname='+name
rawprefix=lambda n:b'\x18\x0a'+bytes([n//256,n%256])
peer_table=b'\0\0\0\x19\0\x05rrc25'+struct.pack('!H',3)
for n in range(3):peer_table+=b'\x02'+bytes([192,0,2,n+1])*2+struct.pack('!I',64497+n)
raw=mrt(peer_table,1,13,epoch=100)
attrs=attr()
for n in range(a.prefixes):
    rib=struct.pack('!I',n)+rawprefix(n)+struct.pack('!H',3)
    for vp in range(3):rib+=struct.pack('!HIH',vp,90,len(attrs))+attrs
    raw+=mrt(rib,2,13,epoch=100)
inputs=[]
def save(label,data,role):
    path=a.output/(label+'.gz');path.write_bytes(gzip.compress(data,mtime=0))
    sha=hashlib.sha256(path.read_bytes()).hexdigest();uri='fixture://rrc25/'+label
    inputs.append(dict(path=str(path),sha256=sha,source_id=source_identity('rrc25',uri,sha),origin_uri=uri,size=path.stat().st_size,role=role))
save('rib',raw,'baseline')
for i in range(a.files):
    data=b''
    for n in range(min(32,a.prefixes)):
        prefix=rawprefix((i*32+n)%a.prefixes)
        message=update(ann=prefix,attrs=attr(64498 if i%2==0 else 64496)) if n%3 else update(ann=b'',withdrawn=prefix)
        data+=struct.pack('!I',(i+2)*100)+message[4:]
    save('update'+str(i),data,'update')
ref=a.output/'reference.csv'
with ref.open('w',newline='') as f:
    writer=csv.DictWriter(f,fieldnames=USECOLS);writer.writeheader()
    for asn,country,code in [('64496','伊朗','IR'),('64498','美国','US')]:writer.writerow(dict(asn=asn,as_country_cn=country,as_country=code))
sha=hashlib.sha256(ref.read_bytes()).hexdigest()
manifest=dict(schema_version='observation-run/v1',collector='rrc25',window_start='1970-01-01T00:00:00Z',window_end_exclusive='1970-01-02T00:00:00Z',inputs=inputs,baseline_source=inputs[0]['source_id'],update_sources=[s['source_id'] for s in inputs[1:]],references=[dict(path=str(ref),sha256=sha)])
t=time.monotonic();upstream=produce(manifest,dsn,a.output/'observations',min_free_bytes=0)
production=time.monotonic()-t
reader=ObservationReader(dsn,upstream['run_id'],upstream['snapshot'],[s['source_id'] for s in inputs])
version=reader.run_id+':'+str(reader.snapshot);dt=lambda sec:datetime.fromtimestamp(sec,timezone.utc)
sources=tuple(SourceBinding(s.source_id,s.content_sha256,s.role,FileWindow(version,s.source_id,dt((i+1)*100),dt((i+2)*100),dt((i+1)*100),'unknown'),s.expected_elements) for i,s in enumerate(reader.starts))
t=time.monotonic();report=run_fixture(reader,FeaturePlan(version,'rrc25',sources),sha,ref,a.output/'feature')
calculation=time.monotonic()-t
t=time.monotonic();queried=sum(b.num_rows for b in read_table(dsn,report['run_id'],report['snapshot'],'windows',allow_fixture=True))
summary=dict(prefixes=a.prefixes,vps=3,update_files=a.files,upstream_seconds=production,feature_seconds=calculation,query_seconds=time.monotonic()-t,queried_windows=queried,**report)
(a.output/'benchmark.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
print(json.dumps({k:summary[k] for k in ['prefixes','vps','update_files','upstream_seconds','feature_seconds','query_seconds','queried_windows','peak_rss_bytes','actual_rows']},ensure_ascii=False))
