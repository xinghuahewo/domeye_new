"""独立人工三库：正式Detection链→共享发布/固定查询；不连接真实库。"""
import json
import os
from pathlib import Path
import uuid
from collections import Counter
import psycopg2
from psycopg2 import sql
import pytest
from data_pipeline.results import Publication, Token
from data_pipeline.results.profiles import Q2, SELECTORS
from data_pipeline.results.manifest_io import encode
from data_pipeline.analysis.detection.store import read_stored_rows
from tests.detection.test_detection_pipeline import test_frozen_child_consumes_saved_reference_rows_and_publishes_bound_identity as generate

SELECTOR='fixture:detection:q2'


@pytest.fixture(scope='module')
def setup():
    config=os.environ.get('DOMEYE_Q2_BINDING')
    if not config:pytest.skip('仅显式本任务人工PG')
    config=json.loads(Path(config).read_text());root=Path(config['output']);root.mkdir(exist_ok=True)
    saved=root/'private.json'
    if saved.exists():return json.loads(saved.read_text())
    old=config['legacy'];params=psycopg2.extensions.parse_dsn(old['dsn']);name='q2_control_'+uuid.uuid4().hex
    pg=psycopg2.connect(old['dsn']);pg.autocommit=True
    with pg.cursor() as c:c.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(sql.Identifier(name),sql.Identifier(params['dbname'])))
    pg.close();params['dbname']=name;control=psycopg2.extensions.make_dsn(**params)
    with psycopg2.connect(control) as pg,pg.cursor() as c:c.execute('DROP SCHEMA publication_q1 CASCADE')
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv('DOMEYE_DETECTION_TEST_DSN',old['dsn']);generate(root,patch)
    req=json.loads((root/'request.json').read_text());data={'dsn':control,'root':str(root.parent.parent),'detection':{'output_dsn':req['detection_dsn'],'observation_dsn':req['observation_dsn']},'ready':str(root/'frozen-output/business/ready.json'),'legacy':old}
    p=Publication(control,data['root'],detection=data['detection']);p.initialize()
    t=p.prepare(Path(old['root'])/'resource-first/execution.json',Path(old['root'])/'feature/output/execution.json');p.publish(t,expected_generation=0)
    data['q1_token']=t.__dict__;before=p.query(t,'feature',mode='ordinary');p.migrate_profiles();p.migrate_profiles();assert p.query(t,'feature',mode='ordinary')==before
    saved.write_text(json.dumps(data));saved.chmod(0o600)
    return data


@pytest.fixture
def p(setup):
    pub=Publication(setup['dsn'],setup['root'],detection=setup['detection'])
    pub.migrate_profiles()
    with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:c.execute('DELETE FROM publication_q1.head WHERE selector=%s',(SELECTOR,))
    return pub


def publish(p,setup):
    t=p.prepare_detection(setup['ready']);p.publish(t,expected_generation=0,selector=SELECTOR);return t


def all_pages(p,t,view,**kw):
    if view=='records':kw.setdefault('scan_budget',1000000)
    first=p.query_detection(t,view,page_size=100,**kw)
    return [r for n in range(1,(first['total']+99)//100+1) for r in p.query_detection(t,view,page=n,page_size=100,**kw)['items']]


def test_formal_full_typed_events_details_and_q1_migration(p,setup):
    t=publish(p,setup);ready=json.loads(Path(setup['ready']).read_text());original=list(read_stored_rows(setup['detection']['output_dsn'],ready['run_id'],ready['snapshot'],'records'))
    assert Counter(encode(r) for r in all_pages(p,t,'records'))==Counter(encode(r) for r in original)
    events=p.query_detection(t,'events',page_size=100);assert {r['event_kind'] for r in events['items']}=={'prefix_outage','as_outage','country_outage','hijack','sub_hijack','leak','moas'}
    assert events['total']<sum(r['record_kind']=='business_revision' for r in original)
    for event in events['items']:
        detail=p.query_detection(t,'detail',component=events['component'],incident=event['incident_id'],revision=1)
        assert detail['total']==1 and detail['items'][0]['revision']==1
        revisions=p.query_detection(t,'revisions',component=events['component'],incident=event['incident_id'])
        assert revisions['total']==event['revision']
    assert all_pages(p,t,'decisions')
    assert p.query(Token(**setup['q1_token']),'feature',mode='ordinary')['total']==8
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        c.execute('SELECT selector,profile FROM publication_q1.profiles');assert dict(c.fetchall())==SELECTORS
        c.execute('SELECT version FROM publication_q1.schema_version');assert c.fetchall()==[(3,)]
        with pytest.raises(psycopg2.IntegrityError):c.execute("INSERT INTO publication_q1.head VALUES ('real:any','x','x',1)")
    Path(setup['ready']).parents[2].joinpath('positive.json').write_text(encode({'token':t.__dict__,'events':events,'records':len(original)}))


@pytest.fixture(scope='module')
def variants(setup):
    import copy,gzip,hashlib,subprocess,sys
    from data_pipeline.bgp.replay.run_from_files import produce
    from data_pipeline.bgp.input.mrt_reader import source_identity
    root=Path(setup['ready']).parents[2];result={}
    base=json.loads((root/'request.json').read_text())
    for variant in ('empty','second','amplified','mixed'):
        path=root/(variant+'-binding.json')
        if path.exists():result[variant]=json.loads(path.read_text());continue
        req=copy.deepcopy(base);name='q2_'+variant+'_'+uuid.uuid4().hex
        pg=psycopg2.connect(setup['dsn']);pg.autocommit=True
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name+" ENCODING 'UTF8' TEMPLATE template0")
        pg.close();params=psycopg2.extensions.parse_dsn(setup['dsn']);params['dbname']=name;req['detection_dsn']=psycopg2.extensions.make_dsn(**params)
        folder=root/variant;folder.mkdir(exist_ok=True)
        if variant in ('empty','amplified','mixed'):
            name='q2_observation_'+uuid.uuid4().hex;pg=psycopg2.connect(setup['dsn']);pg.autocommit=True
            with pg.cursor() as c:c.execute('CREATE DATABASE '+name+" ENCODING 'UTF8' TEMPLATE template0")
            pg.close();params['dbname']=name;req['observation_dsn']=psycopg2.extensions.make_dsn(**params)
            with psycopg2.connect(base['observation_dsn']) as pg,pg.cursor() as c:
                c.execute('SELECT manifest FROM domeye.run_specs WHERE run_id=%s',(base['input_run'],));manifest=c.fetchone()[0]
            manifest.pop('code_identity',None);manifest.pop('dataset_id',None)
            for entry in manifest['inputs']:
                if entry['role']=='update':
                    raw=folder/(variant+'.gz');content=b''
                    if variant=='amplified':
                        import struct
                        from tests.observations.test_observation_mrt import update
                        eor=update(ann=b'',withdrawn=b'',attrs=b'');eor=struct.pack('!I',200)+eor[4:]
                        content=gzip.decompress(Path(entry['path']).read_bytes())+eor*1000
                    if variant=='mixed':
                        import struct
                        from tests.observations.test_observation_mrt import update, mrt
                        endpoint=struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\x00\x02\x01\xc0\x00\x02\x02'
                        extra=[update(subtype=7),mrt(endpoint+struct.pack('!HH',6,1),subtype=5),update(ann=b'',attrs=b'')]
                        content=gzip.decompress(Path(entry['path']).read_bytes())+b''.join(struct.pack('!I',200)+r[4:] for r in extra)
                    raw.write_bytes(gzip.compress(content,mtime=0));entry.update(path=str(raw),sha256=hashlib.sha256(raw.read_bytes()).hexdigest(),size=raw.stat().st_size,origin_uri='fixture://rrc25/q2-'+variant)
                    entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
            manifest['update_sources']=[e['source_id'] for e in manifest['inputs'] if e['role']=='update']
            upstream=produce(manifest,req['observation_dsn'],folder/'observations',min_free_bytes=0)
            req['input_run']=upstream['run_id'];req['input_snapshot']=upstream['snapshot'];req['ordered_sources']=[e['source_id'] for e in manifest['inputs']]
            req['scope']['input_version']=f"{upstream['run_id']}:{upstream['snapshot']}"
            boundary=next(iter(base['boundaries'].values()))
            req['boundaries']={s:{**boundary,'file_id':s,'source_version':req['scope']['input_version']} for s in req['ordered_sources'][1:]}
        req['scope']['run_id']='q2-'+variant;req['output']=str(folder/'frozen')
        request=folder/'request.json';request.write_text(json.dumps(req));request.chmod(0o600)
        run=subprocess.run([sys.executable,str(Path(__file__).resolve().parents[3]/'scripts/pipeline/detection-frozen-run.py'),str(request)],capture_output=True,text=True)
        (folder/'stdout.log').write_text(run.stdout);(folder/'stderr.log').write_text(run.stderr);assert run.returncode==0,run.stderr
        result[variant]={'output_dsn':req['detection_dsn'],'observation_dsn':req['observation_dsn'],'ready':str(folder/'frozen/business/ready.json')}
        path.write_text(json.dumps(result[variant]));path.chmod(0o600)
    return result


def test_empty_source_proves_no_events_and_fixed_old_pages(p,setup,variants):
    old=publish(p,setup);before=p.query_detection(old,'records',page_size=2,scan_budget=1000000)
    other=variants['empty'];empty=Publication(p.dsn,p.root,detection={k:other[k] for k in ('output_dsn','observation_dsn')})
    token=empty.prepare_detection(other['ready']);empty.publish(token,expected_generation=1,selector=SELECTOR)
    result=empty.query_detection(token,'events');assert result['total']==0 and result['availability']=='known_empty'
    audit=empty.query_detection(token,'records',scan_budget=1000000)
    assert audit['total']>0 and audit['verified_source_messages']['sources'][-1]['messages']==0
    assert p.query_detection(old,'records',page_size=2,scan_budget=1000000)==before
    with pytest.raises(ValueError,match='身份'):p.query_detection(token,'events')


def test_cross_component_same_business_keys_and_fixed_detail(p,setup,variants):
    old=publish(p,setup);events=p.query_detection(old,'events',page_size=100)
    other=variants['second'];second=Publication(p.dsn,p.root,detection={k:other[k] for k in ('output_dsn','observation_dsn')})
    token=second.prepare_detection(other['ready']);second.publish(token,expected_generation=1,selector=SELECTOR)
    new=second.query_detection(token,'events',page_size=100)
    assert new['component']!=events['component'] and new['total']==events['total']
    assert json.loads(Path(other['ready']).read_text())['snapshot']==json.loads(Path(setup['ready']).read_text())['snapshot']
    assert Counter((r['event_kind'],r['subject_key']) for r in new['items'])==Counter((r['event_kind'],r['subject_key']) for r in events['items'])
    item=events['items'][0]
    assert p.query_detection(old,'detail',component=events['component'],incident=item['incident_id'],revision=1)['total']==1
    with pytest.raises(ValueError,match='component'):second.query_detection(token,'detail',component=events['component'],incident=item['incident_id'],revision=1)


def test_alias_ambiguity_missing_and_leak_kinds(p,setup):
    from data_pipeline.results.detection_queries import aliases
    t=publish(p,setup);revisions=all_pages(p,t,'records',record_kind='business_revision')
    ref=aliases(next(r for r in revisions if r['event_kind']=='prefix_outage'))[0]
    broad={k:ref[k] for k in ('source','table','ref_kind','id')}
    result=p.query_detection(t,'alias',alias=broad,page_size=1)
    assert result['resolution']=='ambiguous_reference' and len(result['candidate_incidents'])>1
    assert p.query_detection(t,'alias',alias=ref)['resolution']=='resolved'
    assert p.query_detection(t,'alias',alias={**ref,'id':'absent'})['resolution']=='not_found'
    leak=next(r for r in revisions if r['event_kind']=='leak');refs=aliases(leak)
    assert {r['ref_kind'] for r in refs}=={'leak_phenomenon','leak_event'}
    assert all(p.query_detection(t,'alias',alias=r)['resolution']=='resolved' for r in refs)
    events=p.query_detection(t,'events',event_kind='leak')
    assert events['leak_phenomenon_instances']==2 and events['leak_legacy_event_output_instances']==2
    assert p.query_detection(t,'leak_outputs')['total']==2
    assert all(json.loads(r['attributes_json'])['end_state']=='not_recorded' for r in events['items'])


@pytest.mark.parametrize('target',['source','reference','output'])
@pytest.mark.parametrize('phase',['prepare','publish','query'])
def test_qualification_revocation_and_old_head(p,setup,target,phase):
    ready=json.loads(Path(setup['ready']).read_text());old=publish(p,setup)
    token=p.prepare_detection(setup['ready']) if phase=='publish' else old
    cfg=setup['detection'];dsn=cfg['output_dsn' if target=='output' else 'observation_dsn']
    table={'source':'domeye.inputs','reference':'domeye.reference_inputs','output':'detection.runs'}[target]
    run=ready['run_id'] if target=='output' else ready['identity']['input_run']
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT DISTINCT state FROM '+table+' WHERE run_id=%s',(run,));states=c.fetchall();assert len(states)==1;state=states[0][0]
        c.execute('UPDATE '+table+" SET state='failed' WHERE run_id=%s",(run,))
    try:
        with pytest.raises(ValueError):
            if phase=='prepare':p.prepare_detection(setup['ready'])
            elif phase=='publish':p.publish(token,expected_generation=1,selector=SELECTOR)
            else:p.query_detection(old,'events')
        assert p.discover(SELECTOR)==(old,1)
    finally:
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute('UPDATE '+table+' SET state=%s WHERE run_id=%s',(state,run))
    assert p.query_detection(old,'events')['total']==12


def test_cross_database_share_locks_are_real(p,setup):
    from data_pipeline.results import detection_binding
    b=detection_binding.inspect(p,setup['ready'])
    with detection_binding.verify(p,b,lock=True):
        pg=psycopg2.connect(setup['detection']['observation_dsn'])
        try:
            with pg.cursor() as c:
                c.execute("SET lock_timeout='100ms'")
                with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute("UPDATE domeye.runs SET state='failed' WHERE run_id=%s",(b['report']['identity']['input_run'],))
        finally:pg.close()


@pytest.mark.parametrize('change',['count','candidate','snapshot','ready'])
def test_bad_component_cannot_prepare(p,setup,change):
    ready=json.loads(Path(setup['ready']).read_text());run=ready['run_id'];dsn=setup['detection']['output_dsn']
    if change in ('count','ready'):
        copy={**ready,'records':ready['records']+1} if change=='count' else {**ready,'state':'complete'}
        path=Path(setup['ready']).parent/('bad-'+change+'.json');path.write_text(json.dumps(copy))
        with pytest.raises(ValueError):p.prepare_detection(path)
    else:
        field='state' if change=='candidate' else 'snapshot';old='complete' if change=='candidate' else ready['snapshot'];new='candidate' if change=='candidate' else old+1
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute('UPDATE detection.runs SET '+field+'=%s WHERE run_id=%s',(new,run))
        try:
            with pytest.raises(ValueError):p.prepare_detection(setup['ready'])
        finally:
            with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute('UPDATE detection.runs SET '+field+'=%s WHERE run_id=%s',(old,run))


def test_requested_empty_page_integrity_and_alias_fk(p,setup):
    t=publish(p,setup)
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute("UPDATE publication_q1.rows SET mode='other' WHERE build_id=%s AND kind='detection' AND mode='business_revision'",(t.build_id,))
    with pytest.raises(ValueError,match='计数'):p.query_detection(t,'events',page=999)
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        with pytest.raises(psycopg2.IntegrityError):c.execute('DELETE FROM publication_q1.rows WHERE build_id=%s',(t.build_id,))


@pytest.mark.parametrize('fault',['sequence','revision','source_end','baseline'])
def test_complete_record_relationships_reject_faults(p,setup,monkeypatch,fault):
    from data_pipeline.results import detection_binding
    original=detection_binding.rows
    def broken(pub,b,table):
        changed=False
        for row in original(pub,b,table):
            row=dict(row)
            if table=='records' and not changed:
                if fault=='sequence':row['sequence']+=1;changed=True
                elif fault=='revision' and row['record_kind']=='business_revision':
                    row['revision']+=1;attrs=json.loads(row['attributes_json']);attrs['revision']=row['revision'];row['attributes_json']=encode(attrs);changed=True
                elif fault=='baseline' and row['record_kind']=='input_completion':
                    legacy=json.loads(row['legacy_json']);legacy['baseline_count']+=1;row['legacy_json']=encode(legacy);changed=True
                elif fault=='source_end' and row['record_kind']=='source_end':
                    legacy=json.loads(row['legacy_json']);legacy['messages']+=1;row['legacy_json']=encode(legacy);changed=True
            yield row
    monkeypatch.setattr(detection_binding,'rows',broken)
    with pytest.raises(ValueError):p.prepare_detection(setup['ready'])
    with pytest.raises(ValueError,match='没有'):p.discover(SELECTOR)


def test_missing_state_and_byte_budget_reject(p,setup):
    from psycopg2.extras import Json
    from dataclasses import replace
    ready=json.loads(Path(setup['ready']).read_text());dsn=setup['detection']['output_dsn']
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('DELETE FROM detection.state_entries WHERE run_id=%s AND ordinal=0 RETURNING *',(ready['run_id'],));saved=list(c.fetchone())
    try:
        with pytest.raises(ValueError,match='行数'):p.prepare_detection(setup['ready'])
    finally:
        saved[4]=Json(saved[4]);saved[6]=Json(saved[6])
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute('INSERT INTO detection.state_entries VALUES (%s,%s,%s,%s,%s,%s,%s)',saved)
    limited=Publication(p.dsn,p.root,detection=p.detection,limits=replace(p.limits,max_row_bytes=32))
    with pytest.raises(ValueError):limited.prepare_detection(setup['ready'])
    limited=Publication(p.dsn,p.root,detection=p.detection,limits=replace(p.limits,max_rows=ready['records']+ready['state_entries']-1))
    with pytest.raises(ValueError):limited.prepare_detection(setup['ready'])


def test_candidate_invisible_and_before_visible_failure(p,setup,monkeypatch):
    old=publish(p,setup);candidate=p.prepare_detection(setup['ready'])
    with pytest.raises(ValueError,match='候选'):p.query_detection(candidate,'events')
    def fail(name):
        if name=='before_visible':raise RuntimeError('人工提交前失败')
    monkeypatch.setattr(p,'_checkpoint',fail)
    with pytest.raises(RuntimeError):p.publish(candidate,expected_generation=1,selector=SELECTOR)
    assert p.discover(SELECTOR)==(old,1) and p.query_detection(old,'events')['total']==12


def test_contract_fixtures_cross_month_roles_phenomenon_and_history_occurrences():
    # 独立内存Engine/typed合同fixture，不宣称这是正式冻结PG制品。
    from tests.detection.test_detection_computation import engine, send, leak_refs
    from data_pipeline.results.detection_queries import aliases, leak_link, resolve_targets
    from data_pipeline.analysis.detection._results import plain
    e=engine(reference=leak_refs(False));send(e,1,path='5 2 3 4')
    revision=next(r for r in e.output.rows if r['kind']=='business_revision' and r['event_kind']=='leak')
    assert not any(r['kind']=='leak_event_record' for r in e.output.rows)
    raw={'record_kind':'business_revision','event_kind':'leak','attributes_json':encode(plain({k:v for k,v in revision.items() if k not in ('legacy','evidence')})),'legacy_json':encode(plain(revision['legacy']))}
    assert leak_link(raw)[3] is not None
    refs=aliases(raw);assert refs[0]['ref_kind']=='leak_phenomenon'
    attrs=json.loads(raw['attributes_json']);attrs['legacy_ref']['current_legacy_table']='leak_phenomenon_202603';raw['attributes_json']=encode(attrs)
    assert aliases(raw)[0]['table']=='leak_phenomenon_202602' and aliases(raw)[0]['current_table']=='leak_phenomenon_202603'
    # 历史同值行必须由carrier给不同原行身份；查询解析不按相同payload吞并。
    identical_values=[{'legacy':None,'array':[]},{'legacy':None,'array':[]}]
    identities=['freeze/schema/table/ordinal:1','freeze/schema/table/ordinal:2']
    assert identical_values[0]==identical_values[1] and resolve_targets(identities)['resolution']=='ambiguous_reference'
    assert len(resolve_targets(identities)['candidate_incidents'])==2
    # off-pair规范角色和分类仍由已接受规则决定，原JSON保留未锚定原值。
    from data_pipeline.analysis.detection.roles import interpret_roles
    from data_pipeline.analysis.detection.classification import interpret_classification
    row={'kind':'business_revision','event_kind':'hijack','legacy':{'ori_as':'','moas_as1':'1','moas_as2':'2','is_hijack':True}}
    assert interpret_roles('hijack',row['legacy'])['attacker_asn'] is None
    assert interpret_classification(row)['classified_hijack'] is None
    assert interpret_roles('sub_hijack',{})['asn_roles']=='legacy_set_valued_roles'


def test_migration_retains_old_tokens_and_cannot_widen_profiles(p,setup):
    from psycopg2.extras import Json
    p.initialize();p.migrate_profiles()
    assert p.query(Token(**setup['q1_token']),'feature',mode='ordinary')['total']==8
    legacy=setup['legacy'];old=Publication(legacy['dsn'],legacy['root'])
    assert old.query(Token(**legacy['token']),'feature',mode='ordinary')['total']==8
    # 目录数据库约束和Python门禁均保持固定profile，不接受删减必需能力。
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        with pytest.raises(psycopg2.IntegrityError):c.execute('UPDATE publication_q1.profiles SET profile=%s WHERE selector=%s',(Json({**Q2,'required':[]}),SELECTOR))
    t=p.prepare_detection(setup['ready'])
    with pytest.raises(ValueError,match='profile'):p.publish(t,expected_generation=0)


def test_wrong_database_and_missing_ready(p,setup):
    wrong=Publication(p.dsn,p.root,detection={'output_dsn':setup['detection']['observation_dsn'],'observation_dsn':setup['detection']['output_dsn']})
    with pytest.raises((ValueError,psycopg2.Error)):wrong.prepare_detection(setup['ready'])
    with pytest.raises(FileNotFoundError):p.prepare_detection(Path(setup['ready']).parent/'missing-ready.json')



def test_audit_scaling_does_not_duplicate_source_messages_in_control_pg(p,setup,variants,monkeypatch):
    from time import perf_counter
    evidence={}
    for name,cfg,ready in [('base',setup['detection'],setup['ready']),('amplified',{k:variants['amplified'][k] for k in ('output_dsn','observation_dsn')},variants['amplified']['ready'])]:
        from data_pipeline.results import detection_queries
        pub=Publication(p.dsn,p.root,detection=cfg);writes=[];alias_writes=[];original=pub._insert;original_alias=detection_queries.execute_values
        with monkeypatch.context() as patch:
            def insert(c,rows):writes.append(len(rows));return original(c,rows)
            def alias_insert(c,statement,rows,**kwargs):alias_writes.append(len(rows));return original_alias(c,statement,rows,**kwargs)
            patch.setattr(pub,'_insert',insert);patch.setattr(detection_queries,'execute_values',alias_insert)
            started=perf_counter();token=pub.prepare_detection(ready);elapsed=perf_counter()-started
        with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
            c.execute('SELECT manifest FROM publication_q1.builds WHERE build_id=%s',(token.build_id,));m=c.fetchone()[0]
            c.execute("SELECT count(*) FROM publication_q1.rows WHERE build_id=%s AND kind='detection' AND mode='source_message'",(token.build_id,));assert c.fetchone()[0]==0
        pub.publish(token,expected_generation=0 if name=='base' else 1,selector=SELECTOR)
        audit_started=perf_counter();audit=pub.query_detection(token,'records',page_size=2,scan_budget=1000000);audit_seconds=perf_counter()-audit_started
        evidence[name]={'audit_seconds':audit_seconds,'message_proof':audit['verified_source_messages'],'source_messages':m['raw_record_counts']['source_message'],'all_lake_records':m['table_counts']['records'],
                        'pg_records':m['counts']['detection'],'pg_record_kinds':m['record_counts'],'record_insert_calls':len(writes),
                        'alias_rows':m['aliases']['count'],'alias_insert_calls':len(alias_writes),'prepare_seconds':elapsed,'token':token.__dict__}
    assert evidence['amplified']['source_messages']==evidence['base']['source_messages']+1000
    assert evidence['amplified']['pg_records']==evidence['base']['pg_records']
    assert evidence['amplified']['pg_record_kinds']==evidence['base']['pg_record_kinds']
    assert evidence['amplified']['record_insert_calls']==evidence['base']['record_insert_calls']
    folder=Path(setup['root'])/'q2-repair';folder.mkdir(exist_ok=True)
    (folder/'scaling.json').write_text(encode(evidence))


def test_raw_audit_is_explicit_and_business_queries_do_not_scan_lake(p,setup,monkeypatch):
    from data_pipeline.results import detection_binding
    t=publish(p,setup)
    with pytest.raises(ValueError,match='scan_budget'):p.query_detection(t,'records')
    with monkeypatch.context() as patch:
        patch.setattr(detection_binding,'rows',lambda *a,**k:pytest.fail('业务查询不应扫描湖表'))
        assert p.query_detection(t,'events')['total']==12
        assert p.query_detection(t,'decisions')['total']>0
        assert p.query_detection(t,'leak_outputs')['total']==2
    audit=p.query_detection(t,'records',page_size=2,scan_budget=1000000)
    assert len(audit['items'])==2 and audit['scanned_rows']==audit['total']


@pytest.mark.parametrize('fault',['replace','duplicate','source','record','payload','local','state','eor'])
def test_message_coverage_actual_pg_and_lake(p,setup,variants,fault):
    """真实副本同步PG/Parquet及目录；总records、结束自报和ready均保持。"""
    import shutil
    import pyarrow as pa
    import pyarrow.parquet as pq
    from psycopg2.extras import Json
    from data_pipeline.results import resource_feature_bindings as bindings
    from data_pipeline.analysis.detection.store import COLUMNS
    old=publish(p,setup)
    cfg=variants['mixed'] if fault in ('local','state','eor') else {**setup['detection'],'ready':setup['ready']}
    target=Path(setup['root'])/'q2-repair'/('fault-'+fault+'-'+uuid.uuid4().hex)
    shutil.copytree(Path(cfg['ready']).parent,target)
    params=psycopg2.extensions.parse_dsn(cfg['output_dsn']);name='q2_fault_'+uuid.uuid4().hex
    admin=psycopg2.connect(p.dsn);admin.autocommit=True
    with admin.cursor() as c:c.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(sql.Identifier(name),sql.Identifier(params['dbname'])))
    admin.close();params['dbname']=name;dsn=psycopg2.extensions.make_dsn(**params)
    ready=json.loads((target/'ready.json').read_text());run=ready['run_id']
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE public.ducklake_metadata SET value=%s WHERE key='data_path'",(str(target/'parquet')+'/',))
        cat=bindings.catalog(c,'public','det_'+run,ready['snapshot'])
    rows=list(read_stored_rows(dsn,run,ready['snapshot'],'records'))
    messages=[r for r in rows if r['record_kind']=='source_message'];victim=messages[0]
    if fault in ('local','state','eor'):
        victim=next(r for r in messages if (json.loads(r['legacy_json'])['local_message'] if fault=='local' else json.loads(r['legacy_json'])['kind']=='state_change' if fault=='state' else json.loads(r['legacy_json'])['eor']))
    donor=next(r for r in rows if r['record_kind']=='reference_parse') if fault in ('replace','local','state','eor') else messages[1] if fault=='duplicate' else victim
    replacement={**donor,'sequence':victim['sequence']}
    if fault in ('source','record','payload'):
        payload=json.loads(replacement['legacy_json'])
        if fault=='source':payload['source_id']='wrong-source'
        elif fault=='record':payload['record']+=1
        else:payload['raw_digest']='0'*64
        replacement['legacy_json']=encode(payload)
    for f in cat['files']:
        if f['table']!='records':continue
        path=Path(f['path']);table=pq.read_table(path);values=table.to_pylist()
        if not any(v['sequence']==victim['sequence'] for v in values):continue
        path.chmod(0o600);pq.write_table(pa.Table.from_pylist([replacement if v['sequence']==victim['sequence'] else v for v in values],schema=table.schema),path)
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:
            c.execute('UPDATE public.ducklake_data_file SET file_size_bytes=%s,footer_size=%s WHERE path=%s',(path.stat().st_size,int.from_bytes(path.read_bytes()[-8:-4],'little'),path.name));assert c.rowcount==1
    names=[n for n,_ in COLUMNS if n!='sequence']
    values=[Json(json.loads(replacement[n])) if n.endswith('_json') else replacement[n] for n in names]
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute(sql.SQL('UPDATE detection.records SET {} WHERE run_id=%s AND sequence=%s').format(sql.SQL(',').join(sql.SQL('{}=%s').format(sql.Identifier(n)) for n in names)),[*values,run,victim['sequence']])
    bad=Publication(p.dsn,p.root,detection={'observation_dsn':cfg['observation_dsn'],'output_dsn':dsn})
    private=target/'private.json';private.write_text(json.dumps({'output_dsn':dsn,'observation_dsn':cfg['observation_dsn'],'ready':str(target/'ready.json')}));private.chmod(0o600)
    proof={'fault':fault,'records':len(rows),'upstream_messages':len(messages),'saved_messages':len(messages)-(fault in ('replace','local','state','eor')),'rejected':False}
    try:
        with pytest.raises(ValueError,match='消息'):
            bad.prepare_detection(target/'ready.json')
        proof['rejected']=True
        assert p.discover(SELECTOR)==(old,1)
    finally:(target/'反例回执.json').write_text(json.dumps(proof,ensure_ascii=False))



def test_mixed_messages_and_unproved_q2_tokens(p,setup,variants):
    cfg=variants['mixed'];pub=Publication(p.dsn,p.root,detection={k:cfg[k] for k in ('output_dsn','observation_dsn')})
    token=pub.prepare_detection(cfg['ready']);pub.publish(token,expected_generation=0,selector=SELECTOR)
    result=pub.query_detection(token,'records',record_kind='source_message',page_size=100,scan_budget=1000000)
    messages=[json.loads(r['legacy_json']) for r in result['items']]
    assert any(m['local_message'] for m in messages) and any(m['kind']=='state_change' for m in messages) and any(m['eor'] for m in messages)
    assert result['total']==result['verified_source_messages']['total']==20
    # 构造与未接受旧Q2相同的缺证明清单，不依赖任何历史回执文件。
    from psycopg2.extras import Json
    from data_pipeline.results.manifest_io import digest, write_sealed
    old=p.prepare_detection(setup['ready'])
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        c.execute('SELECT manifest,manifest_path FROM publication_q1.builds WHERE build_id=%s',(old.build_id,));m,path=c.fetchone()
        del m['components'][0]['source_messages']
        sealed=write_sealed(Path(path).with_name('unproved.json'),m);old=Token('q1_'+digest(m),old.build_id,old.profile_digest)
        c.execute('UPDATE publication_q1.builds SET manifest=%s,manifest_path=%s,manifest_sha=%s,publication_id=%s WHERE build_id=%s',(Json(m),sealed['path'],sealed['sha256'],old.publication_id,old.build_id))
    with pytest.raises(ValueError,match='消息覆盖证明'):p.publish(old,expected_generation=1,selector=SELECTOR)
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute("UPDATE publication_q1.builds SET state='published' WHERE build_id=%s",(old.build_id,))
    with pytest.raises(ValueError,match='消息覆盖证明'):p.query_detection(old,'events')


@pytest.mark.parametrize('phase',['prepare','audit'])
@pytest.mark.parametrize('target',['source','output'])
def test_message_proof_tail_qualification(p,setup,monkeypatch,phase,target):
    from data_pipeline.results import detection_binding
    old=publish(p,setup);ready=json.loads(Path(setup['ready']).read_text())
    dsn=setup['detection']['output_dsn' if target=='output' else 'observation_dsn']
    table='detection.runs' if target=='output' else 'domeye.inputs'
    run=ready['run_id'] if target=='output' else ready['identity']['input_run']
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT DISTINCT state FROM '+table+' WHERE run_id=%s',(run,));states=c.fetchall();assert len(states)==1;state=states[0][0]
    original=detection_binding.rows
    def revoke(pub,b,table_name):
        yield from original(pub,b,table_name)
        if table_name=='records':
            with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute("UPDATE "+table+" SET state='failed' WHERE run_id=%s",(run,))
    try:
        with monkeypatch.context() as patch:
            patch.setattr(detection_binding,'rows',revoke)
            with pytest.raises(ValueError):
                if phase=='prepare':p.prepare_detection(setup['ready'])
                else:p.query_detection(old,'records',scan_budget=1000000)
        assert p.discover(SELECTOR)==(old,1)
    finally:
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute('UPDATE '+table+' SET state=%s WHERE run_id=%s',(state,run))
    assert p.query_detection(old,'events')['total']==12
