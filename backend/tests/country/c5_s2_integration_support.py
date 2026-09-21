"""8233 自有人工 S2 绑定与固定新进程；不发布、不选择他人数据库。"""
from contextlib import closing, contextmanager
from dataclasses import asdict, replace
from pathlib import Path
import hashlib
import json
import subprocess
import sys
import time
import uuid
import psycopg2
from tests.country_trend.country_trend_s2_support import runtime
from tests.country.test_country_saved_input import adapter
from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_schema as c3
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch, ReadReceipt
from data_pipeline.analysis.country_events.selection_contract import CountryRuntime, contract_value, contract_json
from data_pipeline.analysis.country_events.selection_index import inspect_country

OUT = Path('/tmp/domeye-c5-s2-integration-8233').resolve()
OLD = Path('/tmp/domeye-c3-integration-8233/c1-review-repair0').resolve()
BASE = 'host=/tmp/domeye-integration-detection-8233/socket port=55483 dbname=postgres'
REPO = Path(__file__).resolve().parents[3]


def load(path): return json.loads(Path(path).read_text())
def save(path, value): Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2))
def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def new_db(label):
    with closing(psycopg2.connect(BASE)) as pg:
        pg.autocommit = True
        with pg.cursor() as cur:
            name = 'c5s2_8233_' + label + '_' + uuid.uuid4().hex[:10]
            cur.execute('CREATE DATABASE ' + name)
    return psycopg2.extensions.make_dsn(BASE, dbname=name)


def request(c1, dsn, descriptor, private_root, output):
    return dict(country_descriptor=contract_json(descriptor),
                roles=dict(observation_dsn=c1.reader.dsn, detection_dsn=c1.detection.dsn, country_dsn=dsn, output_dsn=dsn),
                c1=dict(observation_run=c1.reader.run_id, observation_snapshot=c1.reader.snapshot, sources=list(c1.sources),
                        detection_run=c1.detection.run_id, detection_snapshot=c1.detection.snapshot,
                        production_receipt=asdict(c1.production_receipt), timezone='Asia/Shanghai'),
                private_root=str(private_root), output=str(output), receipt=str(OUT / (output.name + '-receipt.json')))


def original_request():
    q = load(OLD / 'request.json'); bound = load(OLD / 'binding.json')
    c1 = adapter((OLD, [q['observation_dsn'], q['detection_dsn']], q['ordered_sources'], bound['observation'], bound['detection']))
    joint = load('/tmp/domeye-c4-unified-tests-8233/c4-evidence0/joint.json')
    read, proof = contract_value(joint['read']), contract_value(joint['proof'])
    @contextmanager
    def qualify(*args, **kwargs): c1._check(); yield; c1._check()
    dsn = load(OLD / 'integration-reader-request.json')['dsn']
    descriptor = inspect_country(read, proof, runtime=CountryRuntime(dsn, c1, qualify))
    assert proof.source_rows == 411
    return request(c1, dsn, descriptor, OLD, OLD / 'c5-s2-integrated-0e2ca12')


def source_rows(q, label):
    rt = runtime(q); descriptor = contract_value(q['country_descriptor']); rows = []; receipt = None
    start = time.monotonic()
    with closing(ComponentReader(rt.country.component_dsn, descriptor.read_binding.component, c1=rt.country.c1).stream()) as stream:
        for value in stream:
            if isinstance(value, ReadReceipt): receipt = value
            else:
                assert isinstance(value, ReadBatch)
                rows.extend(value.rows)
    assert receipt and receipt.full_body_validated and receipt.rows == len(rows)
    typed = [c3.encode(r if isinstance(r, c2.C2Completion) else (r.incident_id, r.revision, r.value)) for r in rows]
    save(OUT / (label + '-source.json'), typed)
    save(OUT / (label + '-source-cost.json'), dict(wall_seconds=time.monotonic()-start, rows=len(rows),
                                                typed_bytes=sum(len(v.encode()) for v in typed), receipt=asdict(receipt)))
    return rows


def frozen(q, label, *, failure=False):
    path = OUT / (label + '-request.json'); save(path, q)
    start = time.monotonic()
    run = subprocess.run([sys.executable, str(REPO / 'scripts/pipeline/country-trend-frozen-run.py'), str(path)],
                         capture_output=True, text=True, cwd=REPO / 'backend')
    (OUT / (label + '-frozen.log')).write_text(run.stdout + run.stderr)
    save(OUT / (label + '-frozen-cost.json'), dict(wall_seconds=time.monotonic()-start, returncode=run.returncode))
    if failure:
        assert run.returncode != 0 and 'trend_witness_identity_or_sample' in run.stderr
        assert not Path(q['receipt']).exists() and not (Path(q['output']) / 'ready.json').exists()
        with closing(psycopg2.connect(q['roles']['output_dsn'])) as pg, pg.cursor() as c:
            c.execute('SELECT state,binding,proof FROM country_trends.components WHERE root=%s', (q['output'],))
            assert c.fetchone() == ('failed', None, None)
    else:
        assert run.returncode == 0, run.stderr
    return run


def make_country(label):
    """复用既有已审人工形态设计，在自己的路径/库独立生成一次。"""
    import gzip, struct
    from tests.country.test_country_saved_input import build_pipeline, multiple_country_mrt
    from tests.observations.test_observation_mrt import mrt
    from data_pipeline.bgp.input.mrt_reader import source_identity
    from data_pipeline.analysis.country_events.snapshot_store import produce_component, database_identity
    from data_pipeline.analysis.country_events.compute import Grid
    from data_pipeline.analysis.country_events.selection_contract import ReferenceBinding, digest
    from data_pipeline.analysis.country_events.selection_index import prepare_country_index
    root = OUT / (label + '-source'); root.mkdir()
    if label == 'revision':
        manifest = multiple_country_mrt(root); entry = manifest['inputs'][1]
        path = Path(entry['path']); original = gzip.decompress(path.read_bytes()); raw = b''; offset = 0
        while offset < len(original):
            length = struct.unpack('!I', original[offset+8:offset+12])[0] + 12
            if struct.unpack('!I', original[offset:offset+4])[0] <= 106: raw += original[offset:offset+length]
            offset += length
        for i in range(3):
            attrs = bytes([64,1,1,0,64,2,10,2,2]) + struct.pack('!II',100+i,1) + bytes([64,3,4,192,0,2,1])
            payload = b'\0\0' + struct.pack('!H',len(attrs)) + attrs + bytes([24,10,0,0])
            bgp = b'\xff'*16 + struct.pack('!HB',len(payload)+19,2) + payload
            endpoint = struct.pack('!IIHH',100+i,999,0,1) + bytes([192,0,2,i+1,192,0,2,254])
            raw += mrt(endpoint+bgp,epoch=115+i)
        path.write_bytes(gzip.compress(raw,mtime=0)); entry.update(sha256=sha(path),size=path.stat().st_size)
        entry['source_id'] = source_identity('rrc25',entry['origin_uri'],entry['sha256'])
        manifest['update_sources'] = [entry['source_id']]
        grid = Grid(99000000,140000000,(110000000,114000000))
    else:
        assert label == 'wide'
        table = b'\0\0\0\x19\0\x05rrc25' + struct.pack('!H',3)
        for i in range(3):
            ip=bytes([192,0,2,i+1]); table += b'\x02'+ip+ip+struct.pack('!I',100+i)
        baseline=mrt(table,1,13); updates=b''
        for n,origin in enumerate((1,3)):
            nlris=b''
            for j in range(20):
                nlri=bytes([24,10,n,j]); nlris+=nlri
                body=struct.pack('!I',n*20+j)+nlri+struct.pack('!H',3)
                for i in range(3):
                    attrs=bytes([64,2,10,2,2])+struct.pack('!II',100+i,origin)
                    body+=struct.pack('!HIH',i,90,len(attrs))+attrs
                baseline+=mrt(body,2,13)
            for i in range(3):
                payload=struct.pack('!H',len(nlris))+nlris+b'\0\0'
                bgp=b'\xff'*16+struct.pack('!HB',len(payload)+19,2)+payload
                endpoint=struct.pack('!IIHH',100+i,999,0,1)+bytes([192,0,2,i+1,192,0,2,254])
                updates+=mrt(endpoint+bgp,epoch=101+n*10+i)
        inputs=[]
        for name,raw,role in (('baseline',baseline,'baseline'),('updates',updates,'update')):
            path=root/(name+'.gz');path.write_bytes(gzip.compress(raw,mtime=0));uri='fixture://8233/c5-s2/'+name
            inputs.append(dict(path=str(path),sha256=sha(path),source_id=source_identity('rrc25',uri,sha(path)),origin_uri=uri,size=path.stat().st_size,role=role))
        manifest=dict(schema_version='observation-run/v1',collector='rrc25',window_start='1970-01-01T00:00:00Z',window_end_exclusive='1970-01-02T00:00:00Z',inputs=inputs,baseline_source=inputs[0]['source_id'],update_sources=[inputs[1]['source_id']])
        grid=Grid(100000000,140000000,(104000000,110000000,114000000,120000000,130000000))
    start=time.monotonic();prepared=build_pipeline(root,BASE,manifest,rich=True,multicountry=label=='wide'); c1=adapter(prepared)
    dsn=new_db(label);binding=produce_component(c1,grid,dsn,root/'c3')
    @contextmanager
    def qualify(*a,**kw): c1._check();yield;c1._check()
    country=CountryRuntime(dsn,c1,qualify);ref=c1.identity['reference_sources']['as_info']
    reference=ReferenceBinding(**database_identity(c1.reader.dsn),run_id=c1.reader.run_id,snapshot=c1.reader.snapshot,manifest_sha256=digest(c1.reader.manifest),source_id=ref['source_id'],content_sha256=ref['source_id'],expected_rows=ref['rows'])
    read,proof=prepare_country_index(binding,reference_binding=reference,runtime=country,output=root/'c4')
    descriptor=inspect_country(read,proof,runtime=country)
    q=request(c1,dsn,descriptor,OUT,OUT/(label+'-trend'))
    save(OUT/(label+'-source-prepare.json'),dict(wall_seconds=time.monotonic()-start,c3_rows=proof.source_rows,manifest=manifest))
    save(OUT/(label+'-request.json'),q)
    return q


def add_contexts(q, rows):
    import csv,gzip,struct
    from datetime import datetime,timezone
    from tests.observations.test_observation_two_phase import fixture_manifest
    from data_pipeline.bgp.replay.run_from_files import produce
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.bgp.input.mrt_reader import source_identity
    from data_pipeline.analysis.features.calculation import FileWindow
    from data_pipeline.analysis.features.projection import SourceBinding
    from data_pipeline.analysis.features.reference import USECOLS
    from data_pipeline.analysis.country_events.compute import MetricPoint
    from data_pipeline.analysis.country_trends.contract import Projection, ReferenceInput, REFERENCE_DEFINITION
    from data_pipeline.analysis.country_trends.snapshot_inputs import register_reference, REFERENCE_PROFILE
    from data_pipeline.analysis.country_trends.snapshot_schema import encode
    root=OUT/'feature-source';start=time.monotonic()
    if root.exists():
        path=root/'request.json';feature=load(path);dsn=feature['dsn']
    else:
        root.mkdir();dsn=new_db('feature');manifest=fixture_manifest(root)
        for entry,epoch in zip(manifest['inputs'][1:],(115,125)):
            path=Path(entry['path']);raw=gzip.decompress(path.read_bytes());path.write_bytes(gzip.compress(struct.pack('!I',epoch)+raw[4:],mtime=0))
            entry.update(sha256=sha(path),size=path.stat().st_size);entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
        manifest['update_sources']=[e['source_id'] for e in manifest['inputs'][1:]]
        ref=root/'reference.csv'
        with ref.open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=USECOLS);writer.writeheader()
            for asn in ('64496','64498'):writer.writerow(dict(asn=asn,as_country_cn='测试国乙',as_country='YY'))
        manifest['references']=[dict(path=str(ref),sha256=sha(ref))]
        start=time.monotonic();seal=produce(manifest,dsn,root/'observation',min_free_bytes=512*1024**2)
        reader=ObservationReader(dsn,seal['run_id'],seal['snapshot'],[e['source_id'] for e in manifest['inputs']]);version=f'{reader.run_id}:{reader.snapshot}'
        dt=lambda t:datetime.fromtimestamp(t,timezone.utc)
        sources=[SourceBinding(s.source_id,s.content_sha256,s.role,FileWindow(version,s.source_id,dt(a),dt(b),dt(a),'complete'),s.expected_elements,(version+':'+s.source_id+':message-scope',),'complete') for s,(a,b) in zip(reader.starts,((100,114),(114,120),(120,140)))]
        feature=dict(dsn=dsn,observation_run=reader.run_id,observation_snapshot=reader.snapshot,collector='rrc25',sources=[asdict(s) for s in sources],reference_sha=sha(ref),reference_path=str(ref),output=str(root/'feature'))
        path=root/'request.json';path.write_text(json.dumps(feature,default=lambda v:v.isoformat()))
    run=subprocess.run([sys.executable,str(REPO/'scripts/pipeline/feature-frozen-run.py'),str(path)],capture_output=True,text=True,cwd=REPO/'backend')
    (root/'frozen.log').write_text(run.stdout+run.stderr);assert run.returncode==0,run.stderr
    save(OUT/'feature-prepare-cost.json',dict(wall_seconds=time.monotonic()-start))
    statuses={};metrics={}
    for r in rows:
        if not isinstance(r,c2.C2Row):continue
        event=(r.incident_id,r.revision)
        if isinstance(r.value,c2.EventStatus):statuses[event]=r.value
        if isinstance(r.value,MetricPoint) and r.value.metric=='visible_direction_count':metrics.setdefault(event,[]).append(r.value)
    refs=[]
    for i,(event,status) in enumerate(sorted(statuses.items())):
        points=sorted(metrics[event],key=lambda p:p.sample_us)
        p=Projection(status.incident.country,'fixed_endpoint_direction',points[0].denominator,tuple(p.sample_us for p in points),tuple(p.value for p in points),f'row:{i}:0',asn_count=2,persistent_asn_count=1,cohort_id=status.cohort_id,definition_binding=REFERENCE_DEFINITION)
        assert p.denominator==20
        refs.append(ReferenceInput(event,(),status.incident.country,(p,replace(p,country='CA',cohort_id='independent-ca-'+str(i),source_ref=f'row:{i}:1',values=tuple(20 for _ in points)))))
    refpath=OUT/'reference-original.json'
    save(refpath,dict(profile=REFERENCE_PROFILE,version='8233-artificial/v1',origin_uri='fixture://8233/independent-projections',historical_applicability='unknown',references=encode(tuple(refs))))
    q['reference_binding']=register_reference(q['roles']['output_dsn'],refpath,OUT)
    q['roles'].update(reference_dsn=q['roles']['output_dsn'],feature_dsn=dsn)
    q['feature_receipt']=str(root/'feature/execution.json')
    q['feature_selections']=[[list(e),s.incident.country,'ordinary'] for e,s in sorted(statuses.items())]
    return q
