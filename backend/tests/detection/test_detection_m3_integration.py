"""8233人工Detection M3v2冻结联合与原typed/v1兼容，禁止真实输入。"""
from contextlib import closing
from collections import Counter
from dataclasses import asdict
from pathlib import Path
import gzip
import hashlib
import json
import os
import pickle
import subprocess
import sys
import time
import uuid
import psycopg2
from psycopg2.extensions import make_dsn
import pytest
from tests.detection.test_detection_pipeline import six_class_observation_fixture, saved_reference_files, KINDS
from tests.detection.test_detection_m3 import test_local_a_w_hand_calculation_preserves_raw_state, test_empty_unknown_scope_has_coverage
from tests.observations.test_observation_mrt import update
from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch
from data_pipeline.analysis.detection.models import DetectionScope, FileBoundary
from data_pipeline.analysis.detection.store import read_stored_rows, read_records, connect_duckdb, literal
from data_pipeline.analysis.detection.qualified_reader import read_binding, read_coverage, read_qualified_revisions, read_qualified_records, read_qualified_state_entries


def save(path,data):path.write_text(json.dumps(data,ensure_ascii=False,indent=2,default=str))
def digest(data):return hashlib.sha256(json.dumps(data,sort_keys=True,default=str).encode()).hexdigest()


@pytest.fixture(scope='module')
def integrated(tmp_path_factory):
    base=os.environ.get('DOMEYE_DETECTION_TEST_DSN')
    if not base:pytest.skip('需显式自有PG')
    if reuse := os.environ.get('DOMEYE_DETECTION_INTEGRATION_REUSE'):
        root=Path(reuse);request=json.loads((root/'request.json').read_text())
        return root,[request['observation_dsn'],request['detection_dsn']],request,json.loads((root/'formal/result.json').read_text())
    root=tmp_path_factory.mktemp('detection-m3');source_root=root/'input';source_root.mkdir()
    dsns=[]
    with closing(psycopg2.connect(base)) as pg:
        pg.autocommit=True
        with pg.cursor() as c:
            for label in ('input','output'):
                name='det_m3_8233_'+label+'_'+uuid.uuid4().hex[:10];c.execute('CREATE DATABASE '+name);dsns.append(make_dsn(base,dbname=name))
    manifest=six_class_observation_fixture(source_root)
    entry=manifest['inputs'][-1];p=Path(entry['path']);p.write_bytes(gzip.compress(gzip.decompress(p.read_bytes())+update(subtype=7,attrs=b'\xf0\x23\x04\x00\x04\x2f\x66'),mtime=0))
    entry.update(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),size=p.stat().st_size)
    entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256']);manifest['update_sources']=[entry['source_id']]
    refs=saved_reference_files(source_root,rich=True);manifest['references']=[{'path':x['path'],'sha256':x['sha256']} for x in refs]
    start=time.monotonic();seal=produce_checkpointed(manifest,dsns[0],root/'m2',min_free_bytes=0,policy='isolate-payload/v1');m2wall=time.monotonic()-start
    ids=[x['source_id'] for x in manifest['inputs']];version=f"{seal['run_id']}:{seal['snapshot']}"
    scope=DetectionScope('integration-8233','r','rrc25',version,'1970-01-01T00:00:00Z','1970-01-02T00:00:00Z')
    request={'input_profile':'observation','observation_dsn':dsns[0],'detection_dsn':dsns[1],'input_run':seal['run_id'],'input_snapshot':seal['snapshot'],'ordered_sources':ids,'scope':asdict(scope),'references':refs,'reference_version':'artificial-v1','boundaries':{x:asdict(FileBoundary(x,version,scope.window_start,{k:k+'_197001' for k in KINDS})) for x in ids[1:]},'output':str(root/'formal'),'min_free_bytes':0}
    save(root/'request.json',request);save(root/'manifest.json',manifest)
    upstream={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for parent in (source_root,root/'m2') for p in parent.rglob('*') if p.is_file()}
    start=time.monotonic();run=subprocess.run([sys.executable,str(Path(__file__).resolve().parents[3]/'scripts/pipeline/detection-frozen-run.py'),str(root/'request.json')],capture_output=True,text=True);wall=time.monotonic()-start
    (root/'formal.stdout').write_text(run.stdout);(root/'formal.stderr').write_text(run.stderr);assert run.returncode==0,run.stderr
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==sha for p,sha in upstream.items())
    result=json.loads((root/'formal/result.json').read_text());save(root/'生产成本.json',{'m2_wall':m2wall,'formal_wall':wall,'compressed_mrt_bytes':sum(x['size'] for x in manifest['inputs']),'reference_bytes':sum(Path(x['path']).stat().st_size for x in refs),'checkpoints':[x['counts'] for x in seal['checkpoints']],'upstream_files_unchanged':len(upstream)})
    return root,dsns,request,result


def test_complete_raw_coverage_qualified_fresh_process(integrated):
    root,dsns,request,result=integrated;run,snap=result['run_id'],result['snapshot'];dsn=dsns[1]
    binding=read_binding(dsn,run,snap);bid=binding['identity']['input_binding_id'];at=binding['identity']['qualification_as_of_position']
    raw={t:list(read_stored_rows(dsn,run,snap,t)) for t in ('records','state_entries','m3_entries')}
    coverage=list(read_coverage(dsn,run,snap,expected_binding_id=bid));assert coverage==raw['m3_entries']
    qualified=list(read_qualified_records(dsn,run,snap,expected_binding_id=bid,at_position=at));states=list(read_qualified_state_entries(dsn,run,snap,expected_binding_id=bid));revisions=list(read_qualified_revisions(dsn,run,snap,expected_binding_id=bid,at_position=at))
    assert [r['raw'] for r in qualified]==raw['records'] and [r['raw'] for r in states]==raw['state_entries']
    assert all(r['main'] is None for r in states)
    for r in revisions:
        if r['main'] is None:assert set(r['qualification']['dimension_coverage'].values())=={'unknown'} and len(r['qualification']['dimension_coverage'])==6
    gap=json.loads(next(r['payload_json'] for r in coverage if r['kind']=='scope_gap'));pos=gap['effective_position'];before=(pos[0],pos[1]-1,1,1000000)
    early=list(read_qualified_revisions(dsn,run,snap,expected_binding_id=bid,at_position=before));known={(x['raw']['incident_id'],x['raw']['revision']) for x in early if x['main'] is not None}
    assert any((x['raw']['incident_id'],x['raw']['revision']) in known and x['main'] is None for x in revisions)
    closed={ (x['incident_id'],x['revision']) for x in raw['records'] if x['record_kind']=='business_revision' and json.loads(x['legacy_json']).get('e_time') }
    assert closed
    assert not any((x['incident_id'],x['revision']) in closed and tuple(json.loads(x['payload_json'])['effective_position'])>=tuple(pos) for x in coverage if x['kind']=='event_qualification')
    reader=ObservationReader(dsns[0],request['input_run'],request['input_snapshot'],request['ordered_sources'],profile='observation')
    messages=[m for b in reader.stream() if isinstance(b,MessageBatch) for m in b.messages]
    from data_pipeline.analysis.detection._results import plain
    saved=[r['legacy'] for r in read_records(dsn,run,snap) if r['kind']=='source_message'];assert saved==plain(messages)
    expected={'binding':binding,'raw':raw,'coverage':coverage,'qualified':qualified,'states':states,'revisions':revisions};save(root/'完整正文与资格.json',expected)
    code='''import json,sys,time,resource,pickle
sys.path.insert(0,sys.argv[1])
from data_pipeline.analysis.detection.qualified_reader import *
from data_pipeline.analysis.detection.store import read_stored_rows
q=json.load(open(sys.argv[2]));r=json.load(open(sys.argv[3]));d=q['detection_dsn'];run=r['run_id'];s=r['snapshot'];start=time.monotonic()
b=read_binding(d,run,s);bid=b['identity']['input_binding_id'];at=b['identity']['qualification_as_of_position']
o=dict(binding=b,raw={t:list(read_stored_rows(d,run,s,t)) for t in ('records','state_entries','m3_entries')},coverage=list(read_coverage(d,run,s,expected_binding_id=bid)),qualified=list(read_qualified_records(d,run,s,expected_binding_id=bid,at_position=at)),states=list(read_qualified_state_entries(d,run,s,expected_binding_id=bid)),revisions=list(read_qualified_revisions(d,run,s,expected_binding_id=bid,at_position=at)))
pickle.dump(o,open(sys.argv[4],'wb'))
print(json.dumps(dict(wall=time.monotonic()-start,peak_rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024))))
'''
    child=subprocess.run([sys.executable,'-I','-c',code,str(Path(__file__).resolve().parents[2]),str(root/'request.json'),str(root/'formal/result.json'),str(root/'reader-output.pickle')],capture_output=True,text=True)
    (root/'reader.stdout').write_text(child.stdout);(root/'reader.stderr').write_text(child.stderr);assert child.returncode==0,child.stderr
    actual=json.loads(child.stdout);assert pickle.loads((root/'reader-output.pickle').read_bytes())==expected
    save(root/'完整摘要与读者成本.json',{'counts':{t:len(v) for t,v in raw.items()},'digests':{t:digest(v) for t,v in raw.items()},'reader_wall':actual['wall'],'reader_process_peak_rss':actual['peak_rss'],'source_message_rows':len(messages),'ended_revisions_not_requalified':len(closed),'pre_gap_known':len(known),'qualified_revisions':len(revisions),'unknown_revisions':sum(r['main'] is None for r in revisions)})


def test_same_count_retarget_and_unknown_schema_rejected(integrated):
    root,dsns,_,result=integrated;dsn=dsns[1];run,snap=result['run_id'],result['snapshot'];binding=read_binding(dsn,run,snap);identity=binding['identity'];bid=identity['input_binding_id']
    original=list(read_coverage(dsn,run,snap,expected_binding_id=bid));counts=Counter((r['incident_id'],r['revision']) for r in original if r['kind']=='event_qualification')
    row=next(r for r in original if r['kind']=='event_qualification' and counts[r['incident_id'],r['revision']]>1);other=next(r for r in original if r['kind']=='event_qualification' and (r['incident_id'],r['revision'])!=(row['incident_id'],row['revision']))
    db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres');db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake')
    with closing(psycopg2.connect(dsn)) as pg:
        pg.autocommit=True
        try:
            db.execute(f'UPDATE lake.det_{run}.m3_entries SET incident_id=?,revision=? WHERE ordinal=?',[other['incident_id'],other['revision'],row['ordinal']]);new=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
            with pg.cursor() as c:
                c.execute('UPDATE detection.m3_entries SET incident_id=%s,revision=%s WHERE run_id=%s AND ordinal=%s',(other['incident_id'],other['revision'],run,row['ordinal']));c.execute('UPDATE detection.runs SET snapshot=%s WHERE run_id=%s',(new,run))
            changed=[dict(r,incident_id=other['incident_id'],revision=other['revision']) if r['ordinal']==row['ordinal'] else r for r in original]
            assert {(r['incident_id'],r['revision']) for r in changed if r['kind']=='event_qualification'}==set(counts)
            with pytest.raises(ValueError,match='目标|身份') as caught:list(read_coverage(dsn,run,new,expected_binding_id=bid))
            error=str(caught.value)
        finally:
            db.execute(f'UPDATE lake.det_{run}.m3_entries SET incident_id=?,revision=? WHERE ordinal=?',[row['incident_id'],row['revision'],row['ordinal']]);db.close()
            with pg.cursor() as c:
                c.execute('UPDATE detection.m3_entries SET incident_id=%s,revision=%s WHERE run_id=%s AND ordinal=%s',(row['incident_id'],row['revision'],run,row['ordinal']));c.execute('UPDATE detection.runs SET snapshot=%s WHERE run_id=%s',(snap,run))
        try:
            with pg.cursor() as c:c.execute('UPDATE detection.runs SET identity=%s WHERE run_id=%s',(json.dumps(dict(identity,store_schema_version='wrong/v999')),run))
            with pytest.raises(ValueError,match='版本'):read_binding(dsn,run,snap)
            with pytest.raises(ValueError,match='版本'):list(read_coverage(dsn,run,snap,expected_binding_id=bid))
        finally:
            with pg.cursor() as c:c.execute('UPDATE detection.runs SET identity=%s WHERE run_id=%s',(json.dumps(identity),run))
    assert list(read_coverage(dsn,run,snap,expected_binding_id=bid))==original
    save(root/'真实双侧错绑及版本拒绝.json',{'ordinal':row['ordinal'],'rows_unchanged':len(original),'event_set_unchanged':True,'old_entry_id_and_payload_retained':True,'error':error,'schema_rejected':True,'restored_original_snapshot':snap})


def test_old_typed_v1_q2_original_binding(tmp_path):
    location=os.environ.get('DOMEYE_DETECTION_OLD_ROOT')
    if not location:pytest.skip('需绑定原人工制品')
    root=Path(location);q=json.loads((root/'detection-request.json').read_text());saved=json.loads((root/'q2-joint-evidence.json').read_text());e=json.loads((root/'three-module-evidence.json').read_text())['detection'];dsn=q['detection_dsn']
    before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
    raw=list(read_stored_rows(dsn,e['run_id'],e['snapshot'],'records'));states=list(read_stored_rows(dsn,e['run_id'],e['snapshot'],'state_entries'))
    assert len(raw)==saved['typed_records']
    from data_pipeline.results import Publication, Token
    pub=Publication(q['observation_dsn'],root,detection={'observation_dsn':q['observation_dsn'],'output_dsn':dsn})
    def heads():
        with closing(psycopg2.connect(q['observation_dsn'])) as pg,pg.cursor() as c:c.execute('SELECT * FROM publication_q1.head ORDER BY selector');return c.fetchall()
    from tests.publication.test_publication_q2 import all_pages
    from data_pipeline.results.manifest_io import encode
    initial=heads()
    assert Counter(encode(r) for r in all_pages(pub,Token(**saved['token']),'records'))==Counter(encode(r) for r in raw)
    reply=pub.query_detection(Token(**saved['token']),'events',page_size=100);assert reply==saved['events'];assert heads()==initial
    with pytest.raises(ValueError,match='版本'):read_binding(dsn,e['run_id'],e['snapshot'])
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in before.items())
    save(tmp_path/'原typed-v1与Q2.json',{'run':e['run_id'],'snapshot':e['snapshot'],'token':saved['token'],'records':len(raw),'state_entries':len(states),'raw_digest':digest(raw),'state_digest':digest(states),'full_q2_reply_equal':True,'heads_unchanged':initial,'unchanged_files':len(before)})


def test_baseline_only_has_coverage_without_event(integrated):
    root,dsns,request,_=integrated
    name='det_m3_8233_empty_'+uuid.uuid4().hex[:10]
    with closing(psycopg2.connect(dsns[1])) as pg:
        pg.autocommit=True
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    empty_dsn=make_dsn(dsns[1],dbname=name)
    request=dict(request,detection_dsn=empty_dsn,ordered_sources=request['ordered_sources'][:1],boundaries={},output=str(root/'baseline-only-recheck'))
    save(root/'baseline-only-request.json',request)
    start=time.monotonic()
    child=subprocess.run([sys.executable,str(Path(__file__).resolve().parents[3]/'scripts/pipeline/detection-frozen-run.py'),str(root/'baseline-only-request.json')],capture_output=True,text=True)
    (root/'baseline-only.stdout').write_text(child.stdout);(root/'baseline-only.stderr').write_text(child.stderr)
    assert child.returncode==0,child.stderr
    result=json.loads((root/'baseline-only-recheck/result.json').read_text());run,snap=result['run_id'],result['snapshot']
    binding=read_binding(empty_dsn,run,snap);bid=binding['identity']['input_binding_id']
    coverage=list(read_coverage(empty_dsn,run,snap,expected_binding_id=bid))
    assert Counter(r['kind'] for r in coverage)=={'source_coverage':1}
    assert list(read_qualified_revisions(empty_dsn,run,snap,expected_binding_id=bid,at_position=binding['identity']['qualification_as_of_position']))==[]
    raw=list(read_stored_rows(empty_dsn,run,snap,'records'))
    assert not any(r['record_kind']=='business_revision' for r in raw)
    save(root/'无事件完整coverage.json',dict(result=result,coverage=coverage,raw=raw,wall=time.monotonic()-start))
