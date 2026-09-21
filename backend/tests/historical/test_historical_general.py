"""H5人工完整General图；原精确节点oracle与旧有限查询对照分开。"""
from contextlib import closing
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime,timedelta,timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid
import pytest
from data_pipeline.history.country_events import History, GeneralToken, PGIdentity, freeze_collection
from data_pipeline.history.country_events.model import TABLES, row_bytes
from data_pipeline.history.event_collection.model import Binding, Root, CollectionToken
from data_pipeline.history.database_import import Token
from data_pipeline.history.database_import.freeze import private_pg
from services.country_outage_general_read_model import CountryOutageGeneralReadModelRuntime
from tests.web import test_country_outage_general_read_model as original
from tests.historical.test_historical_rib import Measure
from tests.historical.test_historical_collection import oracle_nodes
REPO=Path(__file__).resolve().parents[3]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,v):p.write_bytes(row_bytes(v))
def decode(v):return GeneralToken(**{**v,'collection':CollectionToken(**{**v['collection'],'children':tuple(Token(**x) for x in v['collection']['children'])})})
def table(s,name,cap=None):
    output=[]
    with closing(s.bulk(name,batch_rows=cap)) as stream:
        for batch in stream:output.extend(batch['rows'])
    return output

def fixture(root,n=65,events=2,mutate=None):
    root.mkdir();temp=root.parent/('template-'+uuid.uuid4().hex);temp.mkdir()
    src=original.general_store.__wrapped__(temp);template=json.loads((src/'manifest.json').read_text());e=template['events'][0]
    objects={k:json.loads(gzip.decompress((src/e[k]['path']).read_bytes())) for k in ('overview','series')}
    lists={k:[json.loads(line) for line in gzip.decompress((src/e[k]['path']).read_bytes()).splitlines()] for k in ('affected_as','path_downstreams')}
    shutil.rmtree(temp);manifest=deepcopy(template);manifest['events']=[];expected=[]
    for i in range(events):
        event=deepcopy(e);event.update(incident_id=f'incident-{i}',publication_id=f'publication-{i}',event_read_model_id=f'read-model-{i}',legacy_reference=f'country_outage/2026-02-27 09:12:32/IR/{i+1}/r',cohort_id=f'cohort-{i}',event_metric_id=f'metric-{i}',event_as_path_id=f'path-{i}')
        values={k:deepcopy(v) for k,v in objects.items()};rows={k:[deepcopy(v[j%2]) for j in range(n)] for k,v in lists.items()}
        for obj in [*values.values(),*rows['affected_as'],*rows['path_downstreams']]:
            for key in ('incident_id','publication_id','event_read_model_id','legacy_reference','cohort_id','event_metric_id','event_as_path_id'):
                if key in obj:obj[key]=event[key]
        end=(datetime(2026,2,27,tzinfo=timezone.utc)+timedelta(minutes=n*5)).isoformat().replace('+00:00','Z')
        event.update(window_start_utc='2026-02-27T00:00:00Z',window_end_utc=end,data_through=end,state_point_count=n,affected_as_count=n,path_downstream_relation_count=n,path_sample_count=n*2)
        for value in values.values():value.update(window_start_utc=event['window_start_utc'],window_end_utc=end)
        overview=values['overview'];overview.update(data_through=end,state_point_count=n,affected_as_count=n,path_downstream_relation_count=n,route_interrupted_as_count=(n+1)//2,concurrent_path_downstream_relation_count=(n+1)//2)
        overview['cohort']['cohort_id']=event['cohort_id'];overview['cohort']['fixed_prefix_count']=n;overview['cohort']['fixed_asn_count']=n
        # 源缺失quality常量；便利输出只能另列legacy_derived。
        for key in ('observation_state','quality_state','missing_slot_count'):overview.pop(key,None)
        overview['unknown_extra']={'huge':12345678901234567890123456789012345678901234567890,'negative_zero':-0.0,'null':None,'empty':[]}
        series=values['series'];series.update(point_count=n,timestamps=[(datetime(2026,2,27,tzinfo=timezone.utc)+timedelta(minutes=(j+1)*5)).isoformat().replace('+00:00','Z') for j in range(n)])
        names=['interrupted_prefix_count','completely_interrupted_prefix_count','affected_asn_count','route_interrupted_asn_count','ipv4_resource','extra_unknown_track']
        series['tracks']={name:[None if j==3 else -0.0 if j==4 and k==5 else j+k for j in range(n)] for k,name in enumerate(names)}
        series['track_definitions']={name:{'label':name,'unit':'count','definition':'人工完整轨道'} for name in names}
        series['track_definitions']['definition_only']={'unit':'Unknown','definition':'源有定义但无轨道'}
        overview['final_values']={name:series['tracks'][name][-1] for name in names}
        overview['peaks']={name:{'value':max(v for v in series['tracks'][name] if v is not None),'state_point_utc':end} for name in names}
        for j,row in enumerate(rows['affected_as']):row.update(rank=j//2+1,asn=64500+j//2,as_name='测试-AS-'+str(j),event_classification='affected' if j%2 else 'route_interrupted')
        for j,row in enumerate(rows['path_downstreams']):
            row.update(affected_asn=64500+j//2,downstream_asn=64600+j//2)
            sample=row['path_samples'][0];sample['independent_peer_asns']=[64496,64496,64497];row['path_samples']=[sample,deepcopy(sample)]
        if mutate:mutate(i,event,values,rows)
        for key in values:
            values[key]['content_sha256']=original.content_sha(values[key]);event[key]=original.write_object(root,f'events/{i}/{key}.json.gz',values[key])
        for key in rows:event[key]=original.write_rows(root,f'events/{i}/{key}.jsonl.gz',rows[key])
        event['content_sha256']=original.content_sha(event);manifest['events'].append(event);expected.append({'event':event,**values,**rows})
    manifest['event_count']=events
    for key in ('state_point_count','affected_as_count','path_downstream_relation_count','path_sample_count'):manifest[key]=sum(e[key] for e in manifest['events'])
    manifest['content_sha256']=original.content_sha(manifest);raw=original.canonical(manifest)+b'\n';(root/'manifest.json').write_bytes(raw);(root/'COMPLETE.json').write_bytes(raw)
    binding=Binding((Root('general-read-model/v1',str(root/'manifest.json'),(root/'manifest.json').as_uri(),'fixture-H5',sha(root/'manifest.json')),),(str(root),))
    return binding,expected

@pytest.fixture(scope='module')
def lake():
    if not os.environ.get('Q3_PRIVATE_ROOT'):pytest.skip('须自己的明确私有PG')
    root=Path(os.environ['Q3_PRIVATE_ROOT']);out=root/'q3-c4'/('acceptance-'+uuid.uuid4().hex);out.mkdir(parents=True)
    base=f'host={root/"socket"} port=28763';name='q3c4_'+uuid.uuid4().hex[:12]
    with closing(private_pg(base+' dbname=postgres',root)) as pg:
        pg.autocommit=True
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    dsn=base+' dbname='+name
    with closing(private_pg(dsn,root)) as pg:identity=PGIdentity.read(pg)
    h=History(dsn,root,target_identity=identity);binding,expected=fixture(out/'source')
    costs={};log=root/'q3-c4/pg.log'
    with Measure(log,out/'freeze.pg.log') as m:manifest=freeze_collection(binding,out/'freeze')
    costs['freeze']=m.result
    with Measure(log,out/'import.pg.log') as m:collection=h.import_collection(manifest)
    costs['import']=m.result
    with Measure(log,out/'project.pg.log') as m:token=h.project_general(collection)
    costs['project']=m.result
    save(out/'binding.json',{'dsn':dsn,'root':str(root),'identity':asdict(identity),'token':asdict(token)});save(out/'phases.json',costs)
    print('Q3C4_EVIDENCE='+str(out));return h,token,out,expected

def test_full_typed_and_exact_original_graph(lake):
    h,t,out,expected=lake
    with h.general(t) as s:
        allrows={name:table(s,name) for name in TABLES}
        docs=allrows['documents'];raws={}
        for doc in docs:
            cursor=0;parts=[]
            while True:
                result=s.document(doc['document_id'],offset=cursor);parts.append(result['raw']);cursor=result['next_offset']
                if cursor is None:break
            raw=b''.join(parts);raws[str(doc['document_id'])]=raw.hex()
        for eid,source in enumerate(expected):
            got=s.overview(eid);assert got['source']==source['overview']
            assert 'quality_state' not in got['source'] and got['legacy_derived']['quality_state']=='complete'
            assert got['source']['event_end_at_utc'] is None and got['source']['is_final_in_data_range'] is False
            asrows=[r for r in allrows['affected_as'] if r['event_id']==eid]
            assert [(r['row_ordinal'],r['asn'],r['rank'],r['event_classification']) for r in asrows]==[(i,r['asn'],r['rank'],r['event_classification']) for i,r in enumerate(source['affected_as'])]
            points=[r for r in allrows['series_points'] if r['event_id']==eid]
            tracks=list(sorted(source['series']['tracks']))
            assert len(points)==len(tracks)*len(source['series']['timestamps'])
            for r in points:
                track=tracks[r['track_ordinal']];v=source['series']['tracks'][track][r['point_index']]
                assert r['timestamp_text']==source['series']['timestamps'][r['point_index']]
                assert r['kind']==('null' if v is None else 'number')
                if v is not None:assert r['number_lexeme']==json.dumps(v)
            relations=[r for r in allrows['path_relations'] if r['event_id']==eid]
            assert [(r['relation_ordinal'],r['affected_asn'],r['downstream_asn']) for r in relations]==[(i,v['affected_asn'],v['downstream_asn']) for i,v in enumerate(source['path_downstreams'])]
            samples=[r for r in allrows['path_samples'] if r['event_id']==eid]
            assert [(r['relation_ordinal'],r['sample_ordinal'],r['as_path_id'],r['as_path_canonical']) for r in samples]==[(i,j,v['as_path_id'],v['as_path_canonical']) for i,row in enumerate(source['path_downstreams']) for j,v in enumerate(row['path_samples'])]
            peers=[r for r in allrows['sample_peer_members'] if r['event_id']==eid]
            assert [(r['relation_ordinal'],r['sample_ordinal'],r['member_ordinal'],r['asn']) for r in peers]==[(i,j,k,v) for i,row in enumerate(source['path_downstreams']) for j,sample in enumerate(row['path_samples']) for k,v in enumerate(sample['independent_peer_asns'])]
    assert s.receipt;save(out/'ordered-tables.json',allrows);save(out/'raw-documents.json',raws);save(out/'complete.json',s.receipt)
    with h.collection(t.collection) as cs:
        from urllib.parse import urlparse
        files={r['file_id']:r for batch in cs.bulk('files') for r in batch['rows']}
        for doc in docs:
            file=files[doc['file_id']];sourcepath=Path(urlparse(file['uri']).path)
            compressed=sourcepath.read_bytes();assert hashlib.sha256(compressed).hexdigest()==file['raw_sha']
            originalraw=gzip.decompress(compressed) if file['members']==1 else compressed
            assert bytes.fromhex(raws[str(doc['document_id'])])==originalraw[doc['byte_start']:doc['byte_end']]
        for field in allrows['scalar_fields']:
            oracle=oracle_nodes(bytes.fromhex(raws[str(field['document_id'])]))[field['node_ordinal']]
            assert (field['kind'],field['member_ordinal'],field['name'])==(oracle[4],oracle[2],oracle[3] or '')
            assert field['presence']=='present'
            assert (field['number_lexeme'] if field['kind']=='number' else field['text_value'] if field['kind']=='string' else bool(field['bool_value']) if field['kind']=='bool' else None)==oracle[6]
        for row in allrows['overview_metrics']:
            source=expected[row['event_id']]['overview'][row['section']]
            assert row['metric']==list(sorted(source))[row['metric_ordinal']]
        for row in allrows['general_events']:
            e=expected[row['event_id']]['event']
            for key in ('incident_id','publication_id','revision','event_read_model_id','country_code','cohort_id','event_metric_id','event_as_path_id','lifecycle_state'):assert row[key]==e[key]
        for ref in allrows['identity_references']:
            assert ref['resolution'] in ('matched','Unknown')
            if ref['resolution']=='matched':
                assert ref['actual']==ref['expected']
                assert ref['target_store'] is not None if ref['relation_kind']=='store_declaration_candidate' else ref['target_event']==ref['event_id']
        with closing(cs.bulk('nodes')) as stream:
            nodes=(row for batch in stream for row in batch['rows']);current=iter(nodes);r=next(current,None)
            for doc in docs:
                oracle=oracle_nodes(bytes.fromhex(raws[str(doc['document_id'])]))
                for v in oracle:
                    assert r and r['document_id']==doc['document_id']
                    actual=(r['node_ordinal'],r['parent_ordinal'],r['member_ordinal'],r['key'],r['kind'],r['child_count'],r['number_lexeme'] if r['kind']=='number' else r['text_value'] if r['kind']=='string' else r['bool_value'] if r['kind']=='bool' else None)
                    assert actual==v;r=next(current,None)
            assert r is None
    assert cs.receipt;save(out/'exact-structure-complete.json',cs.receipt)

@pytest.mark.parametrize('limit',[1,17,60])
def test_legacy_all_pages_filters_and_hash_scope(lake,limit):
    h,t,out,expected=lake;runtime=CountryOutageGeneralReadModelRuntime(out/'source');receipts=[]
    with h.general(t) as s:
        initial=s.budget.counts['hash_calls']
        for eid,source in enumerate(expected):
            event=source['event'];reference=s.resolve(reference=event['legacy_reference']);assert reference['resolution']=='matched'
            for classification,search,sort in [('all','','default'),('all','','asn_asc'),('affected','AS64502','asn_asc'),('route_interrupted','测试','default')]:
                rows=[];cursor=None
                while True:
                    result=s.affected_asns(eid,classification=classification,search=search,sort=sort,limit=limit,cursor=cursor);rows+=result['items'];cursor=result['next_cursor']
                    if cursor is None:break
                old=[];page=1
                while True:
                    result=runtime.affected_asns(event['incident_id'],event['publication_id'],page=page,page_size=limit,classification=classification,query=search,sort=sort);old+=result['items']
                    if page==result['page_count']:break
                    page+=1
                assert rows==old
            for scope,asn,search in [('all',None,''),('concurrent',64502,''),('all',None,'测试')]:
                rows=[];cursor=None
                while True:
                    result=s.path_downstreams(eid,scope=scope,affected_asn=asn,search=search,limit=limit,cursor=cursor);rows+=result['items'];cursor=result['next_cursor']
                    if cursor is None:break
                old=[];page=1
                while True:
                    result=runtime.path_downstreams(event['incident_id'],event['publication_id'],page=page,page_size=limit,scope=scope,affected_asn=asn,query=search);old+=result['items']
                    if page==result['page_count']:break
                    page+=1
                assert rows==old
            tracks=s.query('track_definitions',event_id=eid,limit=60)['rows']
            for track in tracks:
                if track['presence']=='definition_without_track':continue
                points=[];cursor=None
                while True:
                    page=s.query('series_points',event_id=eid,track_ordinal=track['track_ordinal'],limit=limit,cursor=cursor);points+=page['rows'];cursor=page['next_cursor']
                    if cursor is None:break
                assert len(points)==source['series']['point_count']
                assert [v['timestamp_text'] for v in points]==source['series']['timestamps']
        assert s.budget.counts['hash_calls']==initial
    assert s.receipt;save(out/f'pages-{limit}.json',s.receipt)

@pytest.mark.parametrize('fault',['overview_count','revision','cross_incident','unknown_end','content','complete','track_length','sample_count'])
def test_finite_admission_negatives(lake,fault):
    h,_,out,_=lake;case=out/('negative-'+fault);case.mkdir()
    def mutate(i,event,values,rows):
        if i:return
        if fault=='overview_count':values['overview']['affected_as_count']+=1
        elif fault=='revision':values['overview']['revision']=2
        elif fault=='cross_incident':values['series']['incident_id']='incident-1'
        elif fault=='unknown_end':values['overview']['event_end_at_utc']=event['window_end_utc']
        elif fault=='track_length':values['series']['tracks']['ipv4_resource'].pop()
        elif fault=='sample_count':rows['path_downstreams'][0]['path_samples'].pop()
    binding,_=fixture(case/'source',n=3,mutate=mutate)
    if fault=='complete':(case/'source/COMPLETE.json').write_text('{}')
    if fault=='content':
        m=json.loads((case/'source/manifest.json').read_text());m['content_sha256']='0'*64;raw=original.canonical(m);(case/'source/manifest.json').write_bytes(raw);(case/'source/COMPLETE.json').write_bytes(raw)
        binding=Binding((Root('general-read-model/v1',str(case/'source/manifest.json'),(case/'source/manifest.json').as_uri(),'fixture',sha(case/'source/manifest.json')),),(str(case/'source'),))
    token=None;before=set(h.data_root.iterdir())
    if fault in ('revision','cross_incident'):
        collection=h.import_collection(freeze_collection(binding,case/'freeze'));token=h.project_general(collection)
        with h.general(token) as s:
            events=table(s,'general_events');assert events[0]['admission']=='identity_conflict' and events[1]['admission']=='available'
            refs=table(s,'identity_references');assert any(r['event_id']==0 and r['resolution']=='missing' for r in refs)
            assert s.overview(1)['source']['event_end_at_utc'] is None
        assert s.receipt
        with pytest.raises(ValueError,match='隔离'):
            with h.general(token) as failed:failed.overview(0)
        assert failed.receipt is None
        save(case/'isolation.json',{'events':events,'references':refs,'receipt':s.receipt});return
    with pytest.raises(ValueError) as error:
        collection=h.import_collection(freeze_collection(binding,case/'freeze'));token=h.project_general(collection)
    assert token is None
    failed_paths=[str(p) for p in set(h.data_root.iterdir())-before if (p/'FAILED.json').exists()]
    for p in failed_paths:assert not (Path(p)/'ready.json').exists()
    save(case/'rejection.json',{'error':str(error.value),'failed_candidates':failed_paths,'token_returned':False})

def test_cross_root_complete_candidates_and_unknown(lake):
    h,_,out,_=lake;case=out/'identity-ambiguity';case.mkdir()
    left,_=fixture(case/'a',n=3);right,_=fixture(case/'b',n=3)
    binding=Binding(left.roots+right.roots,left.allowed_roots+right.allowed_roots)
    collection=h.import_collection(freeze_collection(binding,case/'freeze'));token=h.project_general(collection)
    with h.general(token) as s:
        candidates=[];cursor=None
        while True:
            page=s.resolve(incident_id='incident-0',limit=1,cursor=cursor);assert page['resolution']=='ambiguous';candidates+=page['candidates'];cursor=page['next_cursor']
            if cursor is None:break
        assert len(candidates)==2 and all(r['admission']=='identity_conflict' for r in candidates)
        assert s.resolve(incident_id='missing')['resolution']=='missing'
        assert s.resolve(reference='invalid')['resolution']=='Unknown'
        assert s.resolve(incident_id='incident-0',publication_id='other')['resolution']=='missing'
        refs=table(s,'identity_references');assert any(r['resolution']=='ambiguous' for r in refs) and any(r['resolution']=='Unknown' for r in refs)
    assert s.receipt
    with pytest.raises(ValueError,match='隔离'):
        with h.general(token) as fail:fail.query('affected_as',event_id=0)
    assert fail.receipt is None;save(case/'identity-proof.json',{'token':asdict(token),'candidates':candidates,'references':refs,'receipt':s.receipt})

@pytest.mark.parametrize('fault',['budget','early','revoke','close'])
def test_terminal_and_budget(lake,monkeypatch,fault):
    h,t,out,_=lake;s=h.general(t)
    if fault=='early':
        with s:
            with closing(s.bulk('series_points',batch_rows=1)) as stream:next(stream)
        assert s.receipt is None
    elif fault=='budget':
        with pytest.raises(ValueError,match='预算漂移'):
            with s:
                s.query('affected_as',event_id=0,limit=60)
                original_limit=h.collection_limits.metadata_bytes
                object.__setattr__(h.collection_limits,'metadata_bytes',original_limit-1)
                s.query('general_events')
        object.__setattr__(h.collection_limits,'metadata_bytes',original_limit)
        assert s.receipt is None
    elif fault=='revoke':
        original_qualify=h._qualify_general
        def revoke(*a,**kw):
            result=original_qualify(*a,**kw)
            if kw.get('lock'):raise ValueError('人工尾部资格撤销')
            return result
        monkeypatch.setattr(h,'_qualify_general',revoke)
        with pytest.raises(ValueError,match='撤销'):
            with s:table(s,'general_events')
        assert s.receipt is None
    else:
        from tests.historical.test_historical_core_cleanup import ClosingDuck, closed
        with pytest.raises(RuntimeError):
            with s:
                table(s,'general_events');db=s.db=ClosingDuck(s.db,[])
        closed(db);assert s.receipt is None
    assert not any(hasattr(s,k) for k in ('metadata','references','rebuild','events','trend'))
    save(out/('terminal-'+fault+'.json'),{'receipt':s.receipt,'failed':s.failed,'cleanup_errors':s.cleanup_errors})

def test_query_scope_and_cursor_binding(lake):
    h,t,out,_=lake
    with h.general(t) as s:
        with pytest.raises(ValueError):s.query('affected_as',event_id=0,scope='invented')
        first=s.query('affected_as',event_id=0,limit=17)
        with pytest.raises(ValueError):s.query('affected_as',event_id=1,limit=17,cursor=first['next_cursor'])
    assert s.receipt is None and not s.failed
    with h.general(t) as s:
        cursor=first['next_cursor']
        while cursor:
            page=s.query('affected_as',event_id=0,limit=17,cursor=cursor);cursor=page['next_cursor']
    assert s.receipt and all(p['scope']=='query_suffix' for p in s.receipt['query_scopes'].values())
    save(out/'suffix.json',s.receipt)

def test_original_c1_h1_h2_tokens_same_targets(lake):
    h,_,out,_=lake
    from tests.historical import test_historical_rib as rib
    rib.test_old_collection_and_h1_original_tokens_unchanged((h,None,out,None))
    old=h.root/'q3-c3-repair/acceptance-865ded30b3ad42ebb08d86eff8699c06/new-healthy'
    binding=json.loads((old/'binding.json').read_text());token=rib.decode_token(binding['token']);reader=rib.History(binding['dsn'],h.root,target_identity=PGIdentity(**binding['identity']))
    before=sha(reader.data_root/token.profile_id/'ready.json');expected=json.loads((old/'ordered-tables.json').read_text());references=json.loads((old/'reference-proof.json').read_text())
    with reader.rib(token) as s:
        for name in rib.TABLES:assert json.loads(rib.row_bytes(rib.table(s,name)))==expected[name]
        for ref in expected['comparison_refs']:
            key=':'.join(str(ref[k]) for k in ('document_id','object_ordinal','side','ref_ordinal'))
            assert json.loads(rib.row_bytes(s.reference(ref['document_id'],ref['object_ordinal'],ref['side'],ref['ref_ordinal'])))==references[key]
    assert s.receipt and sha(reader.data_root/token.profile_id/'ready.json')==before
    save(out/'original-H2-compatibility.json',{'original_binding':str(old/'binding.json'),'same_ready_sha256':before,'reimported':False,'reprojected':False,'receipt':s.receipt})

def test_removed_source_new_process_full_values(lake):
    h,t,out,_=lake
    (out/'source').rename(out/'source-removed');(out/'freeze').rename(out/'freeze-removed')
    code='''from pathlib import Path
import json,sys
from tests.historical.test_historical_general import History, PGIdentity, decode, TABLES, row_bytes, table, save
p=Path(sys.argv[1]);b=json.loads((p/'binding.json').read_text());h=History(b['dsn'],b['root'],target_identity=PGIdentity(**b['identity']));t=decode(b['token'])
expected=json.loads((p/'ordered-tables.json').read_text());raws=json.loads((p/'raw-documents.json').read_text());assert not (p/'source').exists() and not (p/'freeze').exists()
business={'overview_metrics','track_definitions','series_points','affected_as','path_relations','path_samples','sample_peer_members'}
with h.general(t) as s:
    initial=s.budget.counts['hash_calls']
    for name in TABLES:
        assert json.loads(row_bytes(table(s,name)))==expected[name]
        groups=sorted({r['event_id'] for r in expected[name]}) if name in business else [None]
        for eid in groups:
            got=[];cursor=None
            while True:
                result=s.query(name,event_id=eid,limit=60,cursor=cursor);got+=result['rows'];cursor=result['next_cursor']
                if cursor is None:break
            want=[r for r in expected[name] if eid is None or r['event_id']==eid]
            assert json.loads(row_bytes(got))==want
    assert s.budget.counts['hash_calls']==initial
    for doc in expected['documents']:
        cursor=0;got=bytearray()
        while True:
            result=s.document(doc['document_id'],offset=cursor);got.extend(result['raw']);cursor=result['next_offset']
            if cursor is None:break
        assert got.hex()==raws[str(doc['document_id'])]
assert s.receipt
save(p/'new-process-result.json',{'all_tables_all_fields_equal':True,'all_pages_equal':True,'all_original_document_bytes_equal':True,'all_reference_rows_equal':True,'source_and_freeze_removed':True,'receipt':s.receipt})
'''
    (out/'new-process.py').write_text(code)
    result=subprocess.run([sys.executable,str(out/'new-process.py'),str(out)],cwd=REPO,env={**os.environ,'PYTHONPATH':str(REPO/'backend')},capture_output=True,text=True)
    (out/'new-process.log').write_text(result.stdout+result.stderr);assert result.returncode==0,result.stdout+result.stderr
    proof=json.loads((out/'new-process-result.json').read_text());assert proof['all_tables_all_fields_equal'] and proof['receipt']

@pytest.mark.parametrize('n,events',[(17,2),(1025,2),(1,81)])
def test_artificial_shapes_and_costs(lake,n,events):
    # 已完整基础之后才做形态成本；对照逐行摘要，不list所有领域行。
    h,_,out,_=lake;case=out/f'shape-{n}-{events}';case.mkdir();binding,expected=fixture(case/'source',n=n,events=events)
    costs={};log=h.root/'q3-c4/pg.log';reuse=os.environ.get('Q3_H5_REUSE_SHAPES')
    if reuse:
        old=Path(reuse);previous=old/f'shape-{n}-{events}';report=json.loads((previous/'shape-report.json').read_text());identity=json.loads((old/'binding.json').read_text())
        assert sha(previous/'source/manifest.json')==sha(case/'source/manifest.json')
        h=History(identity['dsn'],h.root,target_identity=PGIdentity(**identity['identity']))
        collection=decode(report['token']).collection;manifest=previous/'freeze/manifest.json'
        costs.update({key:{**report['costs'][key],'reused_unchanged_C1_from':str(previous/'shape-report.json')} for key in ('freeze','import')})
    else:
        with Measure(log,case/'freeze.pg.log') as m:manifest=freeze_collection(binding,case/'freeze')
        costs['freeze']=m.result
        with Measure(log,case/'import.pg.log') as m:collection=h.import_collection(manifest)
        costs['import']=m.result
    with Measure(log,case/'project.pg.log') as m:token=h.project_general(collection)
    costs['project']=m.result
    digests=None
    for cap in (1,17,1000):
        actual={}
        with Measure(log,case/f'bulk-{cap}.pg.log') as m:
            with h.general(token) as s:
                for name in TABLES:
                    count=0;digest=hashlib.sha256()
                    with closing(s.bulk(name,batch_rows=cap)) as stream:
                        for batch in stream:
                            for row in batch['rows']:
                                digest.update(row_bytes(row)+b'\n');count+=1
                                if name=='series_points':
                                    source=expected[row['event_id']]['series'];track=sorted(source['tracks'])[row['track_ordinal']];v=source['tracks'][track][row['point_index']]
                                    assert row['timestamp_text']==source['timestamps'][row['point_index']]
                                    assert row['number_lexeme']==(json.dumps(v) if v is not None else None)
                                elif name=='affected_as':
                                    source=expected[row['event_id']]['affected_as'][row['row_ordinal']]
                                    assert (row['asn'],row['rank'],row['event_classification'])==(source['asn'],source['rank'],source['event_classification'])
                                elif name=='path_samples':
                                    source=expected[row['event_id']]['path_downstreams'][row['relation_ordinal']]['path_samples'][row['sample_ordinal']]
                                    for k in ('prefix','address_family','as_path_id','as_path_canonical'):assert row[k]==source[k]
                                elif name=='sample_peer_members':assert row['asn']==expected[row['event_id']]['path_downstreams'][row['relation_ordinal']]['path_samples'][row['sample_ordinal']]['independent_peer_asns'][row['member_ordinal']]
                    actual[name]={'rows':count,'sha256':digest.hexdigest()}
            assert s.receipt
        costs['bulk-'+str(cap)]={**m.result,'receipt_resources':s.receipt['resources']}
        if digests is None:digests=actual
        else:assert actual==digests
    assert digests['general_events']['rows']==events and digests['affected_as']['rows']==events*n and digests['series_points']['rows']==events*n*6 and digests['path_samples']['rows']==events*n*2
    bulk_costs=[costs['bulk-'+str(cap)] for cap in (1,17,1000)]
    assert len({v['all_python_sha256_calls'] for v in bulk_costs})==1 and len({v['all_python_sha256_bytes'] for v in bulk_costs})==1
    assert len({v['pg_logged_statements'] for v in bulk_costs})==1
    domain=json.loads((h.data_root/token.profile_id/'ready.json').read_text());assert domain['input_spool_resources']['candidate_disk_bound_bytes']>=domain['input_spool_resources']['spool_physical_bytes']>0;structure=json.loads((h.data_root/collection.collection_id/'ready.json').read_text());frozen=json.loads(manifest.read_text())
    save(case/'shape-report.json',{'artificial_events':events,'event_files':events*4,'rows_each_list':n,'token':asdict(token),'table_digests':digests,'costs':costs,'freeze_resources':frozen.get('resources'),'collection_resources':structure.get('resources'),'profile_resources':domain['resources'],'input_spool_resources':domain['input_spool_resources'],'binding':{'dsn':h.dsn,'identity':asdict(h.target_identity)},'scope':'自造形态，不是真实81事件；bulk不同批对照完整摘要；profile检查逐行，不list全档；SHA为全Python，PG为实际独占日志'})

def test_root_upstream_conflicts_keep_all_candidates(lake):
    h,_,out,_=lake;case=out/'upstream-conflict';case.mkdir();left,_=fixture(case/'a',n=3)
    def change(i,event,values,rows):
        # 两根事件身份各不相同，只有相同上游dataset声明的manifest SHA冲突。
        renames={event['incident_id']:f'b-incident-{i}',event['publication_id']:f'b-publication-{i}',event['event_read_model_id']:f'b-read-model-{i}',event['legacy_reference']:f'country_outage/2026-02-27 09:12:32/IR/{i+101}/r'}
        for obj in [event,*values.values(),*rows['affected_as'],*rows['path_downstreams']]:
            for key,value in list(obj.items()):
                if isinstance(value,str) and value in renames:obj[key]=renames[value]
    right,_=fixture(case/'b',n=3,mutate=change)
    m=json.loads((case/'b/manifest.json').read_text());m['dataset_id']='different-root-dataset';m['source_event_metric_manifest_sha256']='f'*64;m['content_sha256']=original.content_sha(m)
    raw=original.canonical(m);(case/'b/manifest.json').write_bytes(raw);(case/'b/COMPLETE.json').write_bytes(raw)
    rr=Root('general-read-model/v1',str(case/'b/manifest.json'),(case/'b/manifest.json').as_uri(),'fixture',sha(case/'b/manifest.json'))
    binding=Binding(left.roots+(rr,),left.allowed_roots+right.allowed_roots)
    token=h.project_general(h.import_collection(freeze_collection(binding,case/'freeze')))
    with h.general(token) as s:
        events=table(s,'general_events');refs=table(s,'identity_references')
        assert len(events)==4 and all(e['admission']=='identity_conflict' for e in events)
        matches=[r for r in refs if r['relation_kind']=='store_declaration_candidate' and r['name']=='source_event_metric_dataset_id']
        assert len(matches)==8 and all(r['resolution']=='ambiguous' for r in matches) and len({r['target_store'] for r in matches})==2
        assert s.resolve(incident_id='incident-0')['candidates'][0]['admission']=='identity_conflict'
    assert s.receipt;save(case/'conflict-proof.json',{'token':asdict(token),'events':events,'references':matches,'receipt':s.receipt})

def test_cumulative_scan_bound_is_enforced(lake):
    from dataclasses import replace
    h,t,out,_=lake
    # 预算在新会话创建前固定；不是测试中途改预算造成的漂移反例。
    limited=History(h.dsn,h.root,target_identity=h.target_identity,limits=replace(h.limits,max_total_rows=100000))
    s=limited.general(t);calls=0;cursor=None
    with pytest.raises(ValueError,match='query_scan_rows_upper_bound'):
        with s:
            while True:
                page=s.query('scalar_fields',limit=60,cursor=cursor);cursor=page['next_cursor'];calls+=1
    assert calls>0 and s.receipt is None and s.failed
    save(out/'scan-bound.json',{'calls_before_failure':calls,'resources':s.budget.counts,'receipt':s.receipt,'scope':'按固定表全量行数/逻辑字节保守累计，不冒充引擎实际扫描量'})

def test_actual_query_plan_scan_observation(lake):
    h,t,out,_=lake
    with h.general(t) as s:
        # 只用于这次人工成本审计；生产Interface不开放任意SQL。
        result=s.db.execute('EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM '+s.table('affected_as')+' WHERE event_id=0 AND asn>64502 ORDER BY asn,row_ordinal LIMIT 17').fetchone()
        plan=json.loads(result[1]);save(out/'actual-query-plan.json',plan)
        def scans(node):
            return ([node] if node.get('operator_type')=='TABLE_SCAN' else [])+[scan for child in node.get('children',[]) for scan in scans(child)]
        operators=scans(plan);assert len(operators)==1
        assert sum(op['operator_rows_scanned'] for op in operators)<=next(t['rows'] for t in s.ready['tables'] if t['name']=='affected_as')
        s.query('general_events')
    assert s.receipt
