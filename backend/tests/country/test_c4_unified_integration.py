"""统一集成：仅显式绑定本任务已有人工制品，原件不重导。"""
from dataclasses import replace
from pathlib import Path
import hashlib
import json
import os
import time
import resource
import uuid
import psycopg2
import pytest
from data_pipeline.results import Publication, Token
from data_pipeline.analysis.country_events import selection_contract as c
from data_pipeline.analysis.country_events.selection_index import prepare_country_index, inspect_country, iter_country_events
from data_pipeline.analysis.country_events.selection_reader import open_qualified_country
from data_pipeline.analysis.country_events.snapshot_store import ComponentBinding, database_identity
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch
from data_pipeline.analysis.country_events.saved_input import CountrySavedInput, DetectionBinding
from data_pipeline.analysis.country_events.saved_contract import ProductionReceiptBinding
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.analysis.country_events import event_aggregation as c2
from tests.country.test_country_saved_input import adapter
from tests.publication.test_publication_q2 import all_pages, SELECTOR as Q2
from tests.country.test_country_publication import next_country_head, test_real_country_old_p_after_new_head_and_all_alias_targets, test_admission_lock_covers_control_commit_and_rollback, test_real_admission_revoked_during_page


def load(p): return json.loads(Path(p).read_text())
def hashes(root): return {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
def write(p,obj): p.write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str))


def reference(c1):
    s=c1.identity['reference_sources']['as_info']
    return c.ReferenceBinding(**database_identity(c1.reader.dsn),run_id=c1.reader.run_id,snapshot=c1.reader.snapshot,
        manifest_sha256=c.digest(c1.reader.manifest),source_id=s['source_id'],content_sha256=s['source_id'],expected_rows=s['rows'])


def setup_country(pub,dsn,c1,binding,out):
    pub.country={'runtime':c.CountryRuntime(dsn,c1=c1),'observation_dsn':c1.reader.dsn,
                 'detection_dsn':c1.detection.dsn,'production_receipt':c1.production_receipt}
    pub.country['runtime']=replace(pub.country['runtime'],verify_qualification=pub.verify_country_qualification)
    started=time.monotonic()
    read,proof=prepare_country_index(binding,reference_binding=reference(c1),runtime=pub.country['runtime'],output=out)
    desc=inspect_country(read,proof,runtime=pub.country['runtime'])
    token=pub.prepare_country(read,proof,reference_binding=read.reference,profile=c.PROFILE)
    pub.publish(token,expected_generation=0,selector=c.SELECTOR)
    selections=[]
    for e in iter_country_events(desc):
        if type(e) is c.QueryReadReceipt:continue
        selections.append(pub.select_country(token,result_id=read.result_id,incident_id=e.incident.incident_id,revision=e.incident.revision,cohort_id=e.cohort_id))
    return (pub,token,read,proof,selections),time.monotonic()-started


@pytest.fixture(scope='module')
def joint(tmp_path_factory):
    path=os.environ.get('DOMEYE_C4_EXISTING_C3')
    if not path:pytest.skip('需显式绑定既有人工C3')
    root=Path(path);before=hashes(root)
    req=load(root/'request.json');saved=load(root/'binding.json');bound=load(root/'integration-reader-request.json')
    prepared=(root,[req['observation_dsn'],req['detection_dsn']],req['ordered_sources'],saved['observation'],saved['detection'])
    c1=adapter(prepared);binding=ComponentBinding(**bound['bindings'][0]);dsn=bound['dsn']
    pub=Publication(dsn,root);pub.initialize();pub.migrate_profiles()
    evidence=root/('c4-evidence-'+uuid.uuid4().hex);evidence.mkdir()
    os.environ['DOMEYE_C4_PUBLICATION_EVIDENCE']=str(evidence)
    reuse=os.environ.get('DOMEYE_C4_REUSE_JOINT')
    if reuse:
        prior=load(reuse);value=(pub,Token(**prior['token']),c.contract_value(prior['read']),c.contract_value(prior['proof']),[c.contract_value(s) for s in prior['selections']]);wall=None
        pub.country={'runtime':c.CountryRuntime(dsn,c1=c1,verify_qualification=pub.verify_country_qualification),'observation_dsn':c1.reader.dsn,'detection_dsn':c1.detection.dsn,'production_receipt':c1.production_receipt}
    else:
        value,wall=setup_country(pub,dsn,c1,binding,root/('c4-unified-'+uuid.uuid4().hex))
    _,token,read,proof,selections=value
    assert proof.source_rows==proof.readback_rows==411 and len(selections)==2
    assert all(Path(p).is_file() and hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in before.items())
    write(evidence/'joint.json',{'token':token.__dict__,'read':c.contract_json(read),'proof':c.contract_json(proof),'selections':[c.contract_json(s) for s in selections],
        'original_files_unchanged':before,'prepare_publish_select_wall_seconds':wall,'process_lifetime_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'rss_scope':'whole_process_lifetime_macos_includes_DuckDB_not_PG'})
    return value


def test_nonempty_full_typed_pages_and_source_identity(joint):
    p,token,read,proof,selections=joint
    parts=list(ComponentReader(p.country['runtime'].component_dsn,read.component,c1=p.country['runtime'].c1).stream())
    original=[r for part in parts if isinstance(part,ReadBatch) for r in part.rows]
    assert len(original)==411
    seen=set();counts={}
    for s in selections:
        with open_qualified_country(s,proof,runtime=p.country['runtime'],verify_qualification=p.verify_country_qualification) as q:
            for view in c.VIEWS:
                if view.startswith('resolve_'):continue
                a=list(q.iter_query(c.QueryRequest(view),page_size=37));b=list(q.iter_query(c.QueryRequest(view),page_size=100))
                assert a==b and type(a[-1]) is c.QueryReadReceipt
                assert json.loads(a[-1].scope_json)['publication_id']==token.publication_id
                counts[s.incident_id+':'+view]=len(a)-1
                for row in a[:-1]:
                    if 'source_sequence' not in row:continue
                    i=row['source_sequence'];expected=original[i]
                    assert c.typed_encode(row['value'])==c.typed_encode(expected.value if isinstance(expected,c2.C2Row) else expected)
                    seen.add(i)
    expected_event={i for i,r in enumerate(original) if isinstance(r,c2.C2Row) and r.incident_id is not None}
    assert expected_event<=seen
    write(Path(os.environ['DOMEYE_C4_PUBLICATION_EVIDENCE'])/'全页原值.json',{'counts':counts,'distinct_source_rows':len(seen),'event_source_rows':len(expected_event),'source_rows':411,'full_typed_values_equal':True})


def publication_responses(p,root):
    q1=load(root/'q1-evidence.json');q2=load(root/'q2-joint-evidence.json');out={}
    for n in ('first','second'):
        t=Token(**q1[n])
        for kind,mode in [('resource',''),('feature','ordinary'),('feature','ir')]:
            first=p.query(t,kind,mode=mode,page_size=7)
            out[n+kind+mode]=[p.query(t,kind,mode=mode,page=i,page_size=7) for i in range(1,(first['total']+6)//7+1)]
    t=Token(**q2['token'])
    for view in ('events','records','decisions','leak_outputs'):
        kw={'scan_budget':1000000} if view=='records' else {}
        out[view]=p.query_detection(t,view,page_size=100,**kw)
    for event in out['events']['items']:
        key=event['incident_id'];kw={'component':out['events']['component'],'incident':key}
        out[key+'revisions']=p.query_detection(t,'revisions',page_size=100,**kw)
        out[key+'detail']=p.query_detection(t,'detail',revision=event['revision'],**kw)
    return out


def test_original_v2_to_v3_without_reimport(tmp_path):
    root=Path(os.environ['DOMEYE_C4_OLD_Q2']);request=load(root/'detection-request.json');before_files=hashes(root)
    p=Publication(request['observation_dsn'],root,detection={'observation_dsn':request['observation_dsn'],'output_dsn':request['detection_dsn']})
    def version():
        with psycopg2.connect(p.dsn) as db,db.cursor() as cur:
            cur.execute('SELECT version FROM publication_q1.schema_version');return cur.fetchone()[0]
    initial_version=version();assert initial_version in (2,3)
    before=publication_responses(p,root);heads=(p.discover(),p.discover(Q2))
    p.migrate_profiles()
    assert version()==3 and (p.discover(),p.discover(Q2))==heads
    assert publication_responses(p,root)==before and hashes(root)==before_files
    write(tmp_path/'原v2到v3.json',{'old_version':initial_version,'new_version':3,'heads':heads,'response_sha256':hashlib.sha256(json.dumps(before,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'files_unchanged':before_files,'response_groups':len(before)})


def test_new_v3_q1_q2_and_same_publication_empty_c4(tmp_path):
    root=Path(os.environ['DOMEYE_C4_M2_JOINT']);req=load(root/'detection-request.json');b=load(root/'country-binding.json')
    p=Publication(req['observation_dsn'],root,detection={'observation_dsn':req['observation_dsn'],'output_dsn':req['detection_dsn']})
    p.migrate_profiles();old1,g1=p.discover();old2,g2=p.discover(Q2)
    old=publication_responses(p,root)
    new1=p.prepare(root/'resource-later/execution.json',root/'feature/execution.json');p.publish(new1,expected_generation=g1)
    new2=p.prepare_detection(root/'detection/business/ready.json');p.publish(new2,expected_generation=g2,selector=Q2)
    assert p.discover()==(new1,g1+1) and p.discover(Q2)==(new2,g2+1)
    assert publication_responses(p,root)==old
    assert len(all_pages(p,new2,'records'))==26 and p.query_detection(new2,'events')['total']==2
    assert p.query(new1,'feature',mode='ordinary')['total']==15
    reader=ObservationReader(req['observation_dsn'],b['observation_run'],b['observation_snapshot'],b['sources'])
    c1=CountrySavedInput(reader,DetectionBinding(**b['detection']),legacy_timezone='Asia/Shanghai',production_receipt=ProductionReceiptBinding(**b['production_receipt']),scratch_root=str(tmp_path))
    binding=ComponentBinding(**load(root/'c3-empty-evidence.json')['binding'])
    value,wall=setup_country(p,req['observation_dsn'],c1,binding,root/('c4-empty-'+uuid.uuid4().hex))
    _,token,read,proof,selections=value
    assert selections==[] and proof.source_rows==7
    desc=inspect_country(read,proof,runtime=p.country['runtime']);items=list(iter_country_events(desc))
    assert len(items)==1 and type(items[0]) is c.QueryReadReceipt and items[0].rows==0
    assert p.discover(c.SELECTOR)==(token,1) and p.discover()==(new1,g1+1) and p.discover(Q2)==(new2,g2+1)
    write(tmp_path/'同P合法空及v3新发布.json',{'q1':new1.__dict__,'q2':new2.__dict__,'country':token.__dict__,'read':c.contract_json(read),'proof':c.contract_json(proof),'event_count':0,'prepare_publish_select_wall_seconds':wall})


def test_real_p_cold_warm_costs(joint,monkeypatch):
    from collections import Counter
    from tests.country.country_query_test_support import PgLogProbe
    import data_pipeline.results as pub_module
    from data_pipeline.results import country_binding
    from data_pipeline.analysis.country_events import selection_index as query_index, selection_reader as query_module
    p,token,read,proof,selections=joint
    log=PgLogProbe(p.dsn,os.environ['DOMEYE_C4_PG_LOG']);calls=Counter()
    for module in (pub_module,country_binding,query_index):
        original=module.file_hash
        def measured(path,*args,_original=original,**kwargs):
            calls['sha256_calls']+=1;calls['sha256_input_bytes']+=Path(path).stat().st_size
            return _original(path,*args,**kwargs)
        monkeypatch.setattr(module,'file_hash',measured)
    original_check=query_module.check_entities
    def checked(entities):
        calls['query_entity_stat_checks']+=len(entities)
        return original_check(entities)
    monkeypatch.setattr(query_module,'check_entities',checked)
    root=Path(os.environ['DOMEYE_C4_PUBLICATION_EVIDENCE']);results={}
    offset=log.start();start=time.monotonic()
    with open_qualified_country(selections[0],proof,runtime=p.country['runtime'],verify_qualification=p.verify_country_qualification) as q:
        cold=q.query(c.QueryRequest('series15'),limit=2);wall=time.monotonic()-start
        snippet,sql=log.finish(offset);(root/'冷页PG.log').write_text(snippet)
        results['cold_open_page']={'wall_seconds':wall,'pg_statements':sql,'file_checks':dict(calls),'access':dict(q.access.stats)}
        calls.clear();prior=Counter(q.access.stats);offset=log.start();start=time.monotonic()
        warm=q.query(c.QueryRequest('series15'),limit=2);wall=time.monotonic()-start
        assert warm==cold
        snippet,sql=log.finish(offset);(root/'暖页PG.log').write_text(snippet)
        results['warm_same_page']={'wall_seconds':wall,'pg_statements':sql,'file_checks':dict(calls),'access_delta':dict(Counter(q.access.stats)-prior)}
    results['scope']='真实人工P；冷为新查询对象，非清空OS缓存；sha为三个实际helper调用输入字节，非物理磁盘读取；stat计数只覆盖查询器实体检查；PG全实例无其他并发测试'
    write(root/'冷暖成本.json',results)


@pytest.mark.parametrize('target',['observation','publication'])
def test_real_upstream_or_p_tail_revocation(joint,monkeypatch,target):
    from tests.country.test_country_publication import test_real_country_query_tail_revocation as probe
    probe(joint,target,monkeypatch)


def test_fixed_alias_all_pages_and_component_originals(joint):
    p,old,read,proof,selections=joint
    saved=load(os.environ['DOMEYE_C4_ALIAS_RECEIPT']);token=Token(**saved['token'])
    alias={'source':'legacy-fixture','table':'event_metric','ref_kind':'metric','id':'same'}
    pages=[p.resolve_alias(token,alias=alias,page=i,page_size=1) for i in (1,2,3)]
    assert all(page['total']==3 and page['resolution']=='ambiguous_reference' for page in pages)
    values=[row for page in pages for row in page['items']]
    assert len(values)==3 and all(row['original_position']==[3,3,1] for row in values)
    assert {row['incident_id'] for row in values}=={s.incident_id for s in selections}
    assert sorted(row['incident_id'] for row in values)==sorted([s.incident_id for s in selections]+[selections[0].incident_id])
    parts=list(ComponentReader(p.country['runtime'].component_dsn,read.component,c1=p.country['runtime'].c1).stream())
    rows=[r for part in parts if isinstance(part,ReadBatch) for r in part.rows]
    expected={i for i,r in enumerate(rows) if not isinstance(r,c2.C2Row) or r.incident_id is None}
    with open_qualified_country(selections[0],proof,runtime=p.country['runtime'],verify_qualification=p.verify_country_qualification) as q:
        output=list(q.iter_query(c.QueryRequest('audit',c.canonical({'kind':'component_scope'})),page_size=2))
    assert {r['source_sequence'] for r in output[:-1]}==expected
    for r in output[:-1]:
        original=rows[r['source_sequence']]
        assert c.typed_encode(r['value'])==c.typed_encode(original.value if isinstance(original,c2.C2Row) else original)
    write(Path(os.environ['DOMEYE_C4_PUBLICATION_EVIDENCE'])/'别名全页与组件原值.json',{'alias_pages':pages,'component_source_rows':len(expected),'all_component_values_equal':True})
