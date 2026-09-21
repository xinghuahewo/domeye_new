"""8233 Resource M3：自有多 RIB observation、冻结生产及有限公开读全量对账。"""
from collections import Counter
from contextlib import contextmanager,closing
from dataclasses import asdict
from datetime import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import pickle
import resource
import subprocess
import sys
import time
import uuid

import psycopg2
from psycopg2.extensions import make_dsn
import pyarrow as pa
import pytest
from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch
from data_pipeline.analysis.resources.observation import ObservationResourceStore, produce_observation_resources
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader
from data_pipeline.analysis.resources import observation_reader as public_module
from data_pipeline.analysis.resources.qualification import ALL_TABLES, normalized
from data_pipeline.analysis.resources.store import ResourceStore, TABLES, TYPES
from data_pipeline.analysis.resources.bindings import SourceBinding
from data_pipeline.analysis.resources.compute import RibContext
from tests.resources.test_resource_observation import prepare, launch
from tests.resources.test_resource_bindings import json_request


def save(p,x):p.write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def encode(x):return json.dumps(normalized(x),sort_keys=True,ensure_ascii=False,separators=(',',':'))
def same(a,b):return Counter(map(encode,a))==Counter(map(encode,b))
def unpack(q):
    q=dict(q);q['sources']=[SourceBinding(s['run_id'],s['snapshot'],s['purpose'],RibContext(**{**s['context'],'snapshot_time':datetime.fromisoformat(s['context']['snapshot_time'])})) for s in q['sources']]
    return q


@contextmanager
def measured():
    count=dict(inventory_calls=0,inventory_rows=0,inventory_logical_bytes=0,relation_validations=0)
    inventory=public_module.table_inventory;validate=public_module.validate_relations;start=time.monotonic()
    def inv(*a,**kw):
        count['inventory_calls']+=1;r=inventory(*a,**kw);count['inventory_rows']+=sum(x['rows'] for x in r.values());count['inventory_logical_bytes']+=sum(x['logical_bytes'] for x in r.values());return r
    def val(*a,**kw):count['relation_validations']+=1;return validate(*a,**kw)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(public_module,'table_inventory',inv);patch.setattr(public_module,'validate_relations',val)
        yield count
    count.update(wall=time.monotonic()-start,process_lifetime_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),rss_scope='当前进程生命周期累计，不含PG，不是阶段独占峰值')


def child_read(path):
    root=Path(path);q=json.loads((root/'case.json').read_text());r=q['report'];reader=ResourceObservationReader(q['request']['dsn'],r['run_id'],r['snapshot'],r['dataset_id']);out={};cost={}
    with measured() as c:out['inputs']=reader.inputs()
    cost['inputs']=c
    with measured() as c:out['values']={target:list(reader.values(target=target,scope='all')) for target in ('metrics','normal_bands','topology_status')}
    cost['values_all_three']=c
    with measured() as c:out['tables']={table:[row for batch in reader.scan(table,scope='all') for row in batch.to_pylist()] for table in ALL_TABLES}
    cost['all_16_tables']=c
    assert [cost[k]['inventory_calls'] for k in cost]==[2,6,32]
    assert [cost[k]['relation_validations'] for k in cost]==[1,3,16]
    (root/'新进程全正文.pickle').write_bytes(pickle.dumps(out));save(root/'公开读取实测.json',cost)


@pytest.fixture(scope='module')
def integrated():
    base=os.environ.get('DOMEYE_RESOURCE_TEST_DSN');location=os.environ.get('DOMEYE_RESOURCE_OWN_ROOT')
    if not base or not location:pytest.skip('需本任务自有PG/证据根')
    assert psycopg2.extensions.parse_dsn(base)['host']=='/tmp/domeye-integration-detection-8233/socket'
    root=Path(location)
    retained={}
    for directory in ('/tmp/domeye-detection-m3-integration-8233/detection-m30','/tmp/domeye-feature-m3-integration-8233/feature-m30','/tmp/domeye-canonical-m3b-integration-8233/canonical'):
        retained.update({str(p.resolve()):sha(p) for p in Path(directory).rglob('*') if p.is_file()})
    if os.environ.get('DOMEYE_RESOURCE_REUSE'):
        case=json.loads((root/'case.json').read_text());request=unpack(case['request']);seal=case['seal'];report=case['report'];measure=case['measure']
    else:
        name='resource_m3_8233_'+uuid.uuid4().hex[:12]
        with closing(psycopg2.connect(base)) as pg:
            pg.autocommit=True
            with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
        dsn=make_dsn(base,dbname=name)
        request,seal,measure=prepare(root,dsn,ribs=10,uncertain_outlier=True,bad=True)
        m2=ObservationReader(dsn,seal['run_id'],seal['snapshot'],[request['sources'][0].context.source_id],profile='observation')
        originals={f['path']:sha(f['path']) for cp in m2.selection.checkpoints for f in cp['files']}
        save(root/'M2生产前.json',dict(seal=seal,files=originals))
        report=launch(json_request(request),root/'resource-request.json')
        assert all(sha(p)==h for p,h in originals.items()) and m2.selection.seal==seal
        save(root/'case.json',dict(request=json_request(request),seal=seal,report=report,measure=measure))
    dsn=request['dsn'];source=request['sources'][0]
    def state():
        with closing(psycopg2.connect(dsn)) as pg,pg.cursor() as c:
            c.execute('SELECT to_jsonb(r) FROM observation_m2.runs r WHERE run_id=%s',(seal['run_id'],));run=c.fetchone()[0]
            c.execute('SELECT to_jsonb(c) FROM observation_m2.checkpoints c WHERE run_id=%s ORDER BY ordinal',(seal['run_id'],));return dict(run=run,checkpoints=c.fetchall())
    initial=state();reader=ObservationReader(dsn,seal['run_id'],seal['snapshot'],[source.context.source_id],profile='observation')
    m2files={f['path']:sha(f['path']) for cp in reader.selection.checkpoints for f in cp['files']}
    save(root/'原制品核对前.json',dict(retained=retained,m2files=m2files,m2_metadata=initial))
    yield root,request,seal,report
    assert state()==initial and reader.selection.seal==seal
    assert all(sha(p)==h for p,h in {**retained,**m2files}.items())
    save(root/'原制品核对后.json',dict(original_files=len(retained),m2_files=len(m2files),all_sha_equal=True,m2_registration_and_checkpoints_equal=True,m2_business=seal['business']))


def test_frozen_full_16_tables_science_and_hand_values(integrated):
    root,request,seal,report=integrated;dsn=request['dsn']
    identity=report['code_identity'];assert identity['execution_mode']=='frozen-fresh-process'
    assert all(sha(p)==v['sha256'] for p,v in identity['files'].items())
    assert report['project_module_sources']['data_pipeline.analysis.resources.validation']=='backend/data_pipeline/analysis/resources/validation.py'
    # 一次底层读取保存完整typed期望；公开入口另在新进程全部核验。
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres');db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
    try:expected={t:db.execute(f'SELECT * FROM lake.resource_{report["run_id"]}.{t} AT (VERSION => {report["snapshot"]})').fetch_arrow_table().to_pylist() for t in ALL_TABLES}
    finally:db.close()
    (root/'底层完整16表.pickle').write_bytes(pickle.dumps(expected))
    code='import sys;sys.path[:0]=sys.argv[1:3];from tests.resources.test_resource_m3_integration import child_read;child_read(sys.argv[3])'
    start=time.monotonic();child=subprocess.run([sys.executable,'-I','-c',code,str(Path(__file__).resolve().parents[2]),str(Path(__file__).resolve().parent),str(root)],capture_output=True,text=True)
    child_wall=time.monotonic()-start
    (root/'reader.stdout').write_text(child.stdout);(root/'reader.stderr').write_text(child.stderr);assert child.returncode==0,child.stderr
    actual=pickle.loads((root/'新进程全正文.pickle').read_bytes());assert all(same(expected[t],actual['tables'][t]) for t in ALL_TABLES)
    assert actual['inputs']['receipt']['inventory']==report['inventory']
    rows=actual['tables'];values=actual['values'];sources=request['sources'];sids=[s.context.source_id for s in sources]
    assert sorted(r['source_rank'] for r in rows['input_receipts'])==[0,1,3,4,5,6,7,8,9,10]
    assert len(rows['peer_dependencies'])==10 and rows['observation_quality']==[]
    assert sum(cp['counts']['rejected'] for cp in seal['checkpoints'])==1
    assert all(r['gaps']==0 for r in rows['input_receipts'])
    metrics=[v for v in values['metrics'] if v['raw']['dimension']=='first_path_asn' and v['raw']['bucket']=='global']
    assert len(metrics)==10
    for v in metrics:
        if v['raw']['source_id']==sids[-2]:
            assert v['raw']['ipv4_prefix_count']==0 and v['raw']['is_outlier'] and v['main']['ipv4_prefix_count'] is None
        else:assert v['raw']['ipv4_prefix_count']==2 and v['raw']['ipv4_address_count']==512 and v['raw']['path_count']==1 and v['main']['ipv4_prefix_count']==2
    last=next(v for v in metrics if v['raw']['source_id']==sids[-1]);assert last['main']['is_outlier'] is None
    normals=[v for v in values['normal_bands'] if v['raw']['dimension']=='first_path_asn' and v['raw']['bucket']=='global' and v['raw']['metric']=='ipv4_prefix_count']
    for v in normals:
        rank=sids.index(v['raw']['source_id']);assert v['raw']['mean']==2 and v['raw']['population_std']==0 and v['raw']['upper_bound']==2 and v['raw']['lower_bound']==1
        assert (v['main']['upper_bound'] is not None)==(rank in (6,7))
    assert all(r['sample_source']!=sids[-2] for r in rows['normal_samples'] if r['source_id']==sids[-1])
    assert any(r['source_id']==sids[-1] and r['kind']=='normal_history' and r['dependency_id']==sids[-2] for r in rows['qualification_dependencies'])
    compared=compare_old_science(root,request,report,rows)
    save(root/'全量及手算摘要.json',dict(report=report,counts={t:len(v) for t,v in rows.items()},reader_whole_process_wall=child_wall,old_scientific_rows=compared,nonempty_prefix_count=2,normal_qualified_ranks=[6,7],empty_filtered_rank=8,last_count_qualified_normal_unknown=True,quality_rows=0,quality_scope='所选RIB自然无quality；未据此宣称其他UPDATE无Gap'))


def compare_old_science(root,request,report,rows):
    from itertools import chain
    from data_pipeline.analysis.resources.adapter import reference_projection
    from data_pipeline.analysis.resources.references import read_reference
    path=root/'52f原compute.py';assert sha(path)==sha(Path(__file__).resolve().parents[2]/'data_pipeline/analysis/resources/compute.py')
    spec=importlib.util.spec_from_file_location('resource_integration_52f',path);old=importlib.util.module_from_spec(spec);sys.modules[spec.name]=old;spec.loader.exec_module(old)
    csv=request['csv_binding'];dsn=request['dsn'];reader=ObservationReader(dsn,csv['run_id'],csv['snapshot'],[csv['anchor_source_id']],profile='observation')
    countries,_=read_reference(dsn,**request['country_binding']);country=request['country_binding']['dataset_id']
    reference=[dict(source_id=country,row=i,location=k,raw_row=json.dumps({'__object_pairs__':[['country_cn',v[0]]]})) for i,(k,v) in enumerate(countries.items())]
    refs=reference_projection(chain(reader.reference_batches(csv['source_id']),[pa.Table.from_pylist(reference)]),csv_source=csv['source_id'],country_source=country)
    output={t:[] for t in TABLES};sink=object.__new__(ResourceStore);sink.run_id=report['run_id'];sink.member_sizes={};sink.tables=TABLES;sink.upstream_run=report['upstream_run'];sink.upstream_snapshot=report['upstream_snapshot'];sink.binding_manifest=report['binding_manifest'];sink.source_bindings={s.context.source_id:asdict(s) for s in request['sources']}
    sink.append=lambda t,row:output[t].append({k:row.get(k) for k,_ in TABLES[t]})
    computer=old.ResourceComputer(refs,topology_enabled=True)
    for s in request['sources']:
        r=ObservationReader(dsn,s.run_id,s.snapshot,[s.context.source_id],profile='observation');elements=[e for b in r.stream() if isinstance(b,MessageBatch) for e in b.elements]
        result=computer.compute(s.context,[old.RibElement(e['source_id'],e['message_id'],e['ordinal'],e['prefix'],e['as_path_text'],str(e['peer_asn']),e['peer_ip'],e['bgp_id'] if e['bgp_id_present'] else None,e['path_key'],e['afi'],e['safi'],e['action'],e['attributed_origin_asn']) for e in elements],decision_sink=sink.decision,membership_sink=sink.members)
        epochs=[e['epoch'] for e in elements];sink.result(result,[min(epochs) if epochs else None,max(epochs) if epochs else None,len(elements)])
    output['rendered_paths']=list({r['path_digest']:r for r in output['rendered_paths']}.values())
    for t in TABLES:
        cast=pa.Table.from_pylist(output[t],schema=pa.schema([(n,TYPES[ty]) for n,ty in TABLES[t]])).to_pylist()
        assert same(cast,rows[t]),t
    with closing(psycopg2.connect(dsn)) as pg,pg.cursor() as c:
        c.execute('SELECT rib_count,last_source,initial_state_basis FROM domeye.resource_work_meta WHERE run_id=%s',(report['run_id'],));assert c.fetchone()==(computer.state.rib_count,computer.state.last_context.source_id,computer.state.initial_state_basis)
    return sum(map(len,output.values()))


@pytest.mark.parametrize('case',['peer','future'])
def test_actual_finish_rejects_same_count_dependencies(integrated,monkeypatch,case):
    root,request,seal,report=integrated;sources=request['sources'];original=ObservationResourceStore.finish;details={}
    def damage(store,record):
        store.flush()
        if case=='peer':
            table=f'lake.{store.schema}.peer_dependencies';before=store.db.execute('SELECT count(*) FROM '+table).fetchone()[0]
            store.db.execute('UPDATE '+table+' SET peer_index=peer_index+999,peer_asn=999999')
            assert store.db.execute('SELECT count(*) FROM '+table).fetchone()[0]==before;details['peer_rows']=before
        else:
            target,old,new=sources[7],sources[6],sources[9];table=f'lake.{store.schema}.normal_samples'
            before=store.db.execute('SELECT count(*) FROM '+table).fetchone()[0]
            store.db.execute('UPDATE '+table+' SET sample_source=?,sample_time=? WHERE source_id=? AND sample_source=?',[new.context.source_id,new.context.snapshot_time,target.context.source_id,old.context.source_id])
            assert store.db.execute('SELECT count(*) FROM '+table).fetchone()[0]==before
            samples=store.db.execute("SELECT sample_source,sample_time FROM "+table+" WHERE source_id=? AND dimension='first_path_asn' AND bucket='global' AND metric='ipv4_prefix_count'",[target.context.source_id]).fetchall()
            assert len(samples)==7 and sum(t<target.context.snapshot_time for _,t in samples)==6 and any(t>target.context.snapshot_time for _,t in samples)
            details.update(target=target.context.source_id,future=new.context.source_id,samples=samples,prior_remaining=6,normal_rows=before)
        return original(store,record)
    monkeypatch.setattr(ObservationResourceStore,'finish',damage)
    args={k:v for k,v in request.items() if k!='operation'};start=time.monotonic()
    with pytest.raises(ValueError,match='Peer依赖位置或原属性' if case=='peer' else '不能引用未来RIB') as caught:
        produce_observation_resources(**{**args,'output':str(root/('failed-'+case)),'fixture_only':True})
    failure=json.loads((root/('failed-'+case)/'failure.json').read_text());assert failure['state']=='failed'
    with closing(psycopg2.connect(request['dsn'])) as pg,pg.cursor() as c:
        c.execute('SELECT state,dataset_id FROM domeye.resource_runs WHERE run_id=%s',(failure['run_id'],));assert c.fetchone()==('failed',None)
    save(root/(case+'反例.json'),dict(details=details,error=str(caught.value),failure=failure,wall=time.monotonic()-start,mode='显式人工fixture API完成前注入，非正式冻结入口攻击'))


def test_inputs_revoked_before_return_closes(integrated,monkeypatch):
    root,request,seal,report=integrated;dsn=request['dsn'];r=ResourceObservationReader(dsn,report['run_id'],report['snapshot'],report['dataset_id']);connect=r._connect;closed=[]
    def state(value):
        with closing(psycopg2.connect(dsn)) as pg,pg,pg.cursor() as c:c.execute('UPDATE observation_m2.runs SET state=%s WHERE run_id=%s',(value,seal['run_id']))
    class Tracked:
        def __init__(self,db):self.db=db
        def __getattr__(self,k):return getattr(self.db,k)
        def close(self):closed.append(1);self.db.close()
    def revoked():
        db=connect();state('failed');return Tracked(db)
    monkeypatch.setattr(r,'_connect',revoked)
    try:
        with measured() as costs:
            with pytest.raises(ValueError,match='观察尚未封存') as caught:r.inputs()
    finally:state('observation_sealed')
    assert closed==[1]
    save(root/'inputs尾撤销.json',dict(error=str(caught.value),closed=closed,state_restored=True,costs=costs))


def test_original_resource_q1_without_reimport():
    location=os.environ.get('DOMEYE_RESOURCE_OLD_ROOT');output=os.environ.get('DOMEYE_RESOURCE_OWN_ROOT')
    if not location or not output:pytest.skip('需本任务原Resource/Q1')
    root=Path(location);out=Path(output);request=json.loads((root/'resource-first.json').read_text());dsn=request['dsn']
    report=json.loads((root/'resource-first/execution.json').read_text());q1=json.loads((root/'q1-evidence.json').read_text())
    before={str(p):sha(p) for p in root.rglob('*') if p.is_file()}
    from data_pipeline.analysis.resources.store import scan_resource
    from data_pipeline.results import Publication, Token
    def metadata():
        with closing(psycopg2.connect(dsn)) as pg,pg.cursor() as c:
            c.execute('SELECT * FROM publication_q1.head ORDER BY selector');heads=c.fetchall()
            c.execute('SELECT to_jsonb(r) FROM domeye.resource_runs r WHERE run_id=%s',(report['run_id'],));return dict(heads=heads,resource=c.fetchone()[0])
    initial=metadata();start=time.monotonic()
    raw={t:[r for b in scan_resource(dsn,report['run_id'],t,scope='all') for r in b.to_pylist()] for t in TABLES}
    assert {t:len(v) for t,v in raw.items()}==report['actual_rows']
    pub=Publication(dsn,root);pages=0
    for saved in q1['old_pages']:
        if saved['kind']!='resource':continue
        for page in saved['pages']:
            assert pub.query(Token(**q1['first']),'resource',page=page['page'],page_size=page['page_size'])==page;pages+=1
    assert pages==2 and metadata()==initial
    assert all(sha(p)==h for p,h in before.items())
    save(out/'旧Resource与Q1.json',dict(run=report['run_id'],snapshot=report['snapshot'],dataset_id=report['dataset_id'],token=q1['first'],full_original_pages_equal=pages,counts={t:len(v) for t,v in raw.items()},digests={t:hashlib.sha256(encode(sorted(v,key=encode)).encode()).hexdigest() for t,v in raw.items()},original_files=len(before),all_sha_equal=True,registration_head_equal=True,wall=time.monotonic()-start))
