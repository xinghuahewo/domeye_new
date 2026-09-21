"""统一国家发布定向测试；真实旧Token兼容与明确fixture边界分开。"""
import json
import os
from pathlib import Path
import uuid

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
import pytest
from data_pipeline.results import Publication, Token, profiles
from data_pipeline.analysis.country_events import selection_contract as contract


@pytest.fixture(scope='module',params=['v1','v2'])
def migrated(request):
    path=os.environ.get('DOMEYE_C4_PUBLICATION_BINDING')
    if not path:pytest.skip('必须显式绑定本任务人工旧Token配置')
    cfg=json.loads(Path(path).read_text());source=cfg['legacy'] if request.param=='v1' else cfg
    # 授权自有原库显式迁移；clone改变OID，不能冒充旧Token原身份。
    p=Publication(source['dsn'],source['root'],detection=cfg['detection'])
    q1=Token(**(source['token'] if request.param=='v1' else cfg['q1_token']))
    before=p.query(q1,'feature',mode='ordinary');head=p.discover()
    q2=p.discover('fixture:detection:q2') if request.param=='v2' else None
    oldq2=p.query_detection(q2[0],'events') if q2 else None
    p.migrate_profiles();p.migrate_profiles()
    assert p.query(q1,'feature',mode='ordinary')==before and p.discover()==head
    if q2:assert p.discover('fixture:detection:q2')==q2 and p.query_detection(q2[0],'events')==oldq2
    return p,request.param


def test_explicit_v3_preserves_actual_old_profiles_tokens_heads(migrated):
    p,version=migrated
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        c.execute('SELECT version FROM publication_q1.schema_version');assert c.fetchall()==[(3,)]
        c.execute('SELECT selector,profile FROM publication_q1.profiles');assert dict(c.fetchall())==profiles.SELECTORS
    evidence=Path(os.environ.get('DOMEYE_C4_PUBLICATION_EVIDENCE',str(Path(p.root)/'c4-publication')));evidence.mkdir(exist_ok=True)
    (evidence/('migration-'+version+'.json')).write_text(json.dumps({'旧版本':version,'新版本':3,'旧Token及head完整响应保持':True},ensure_ascii=False))


def test_country_profile_cannot_widen_and_queries_do_not_migrate(migrated,monkeypatch):
    p,_=migrated
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        with pytest.raises(psycopg2.IntegrityError):
            c.execute('UPDATE publication_q1.profiles SET profile=%s WHERE selector=%s',(Json({**profiles.COUNTRY,'required':[]}),contract.SELECTOR))
    monkeypatch.setattr(p,'migrate_profiles',lambda:pytest.fail('查询不能触发迁移'))
    try:p.discover(contract.SELECTOR)
    except ValueError as error:assert '没有' in str(error)


@pytest.fixture(scope='module')
def source_only():
    """真实C3来源资格单测；描述占位不代表C4索引或publication已完成。"""
    from data_pipeline.analysis.country_events.snapshot_store import ComponentBinding
    from data_pipeline.analysis.country_events.saved_contract import ProductionReceiptBinding
    from tests.country.test_country_saved_input import adapter
    from data_pipeline.results import country_binding
    cfgpath=os.environ.get('DOMEYE_C4_PUBLICATION_BINDING')
    if not cfgpath:pytest.skip('必须绑定本任务人工库')
    cfg=json.loads(Path(cfgpath).read_text());root=Path(cfg['root'])/'c4-publication/formal'
    if not (root/'private.json').exists():pytest.skip('本任务正式C3尚未生成')
    own=json.loads((root/'private.json').read_text());prepared_root=Path(own['prepared_root']);request=json.loads((prepared_root/'request.json').read_text());saved=json.loads((prepared_root/'binding.json').read_text())
    prepared=(prepared_root,[request['observation_dsn'],request['detection_dsn']],request['ordered_sources'],saved['observation'],saved['detection'])
    c1=adapter(prepared);b=ComponentBinding(**own['binding']);m=json.loads((Path(b.root)/'manifest.json').read_text())
    upstream=contract.typed_decode(m['upstream']);obs=upstream['observation'];ref=c1.identity['reference_sources']['as_info']
    reference=contract.ReferenceBinding(obs['system_id'],obs['database_oid'],obs['run_id'],obs['snapshot'],contract.digest(obs['manifest']),ref['source_id'],ref['source_id'],ref['rows'])
    read=contract.CountryReadBinding(b,contract.result_id(b),'fixture-not-a-read-model',b.root,b.manifest_sha256,reference)
    proof=contract.CountryAdmissionProof(read.result_id,read.read_model_id,b.manifest_sha256,'fixture-only','fixture-only',0,0,0,0,0)
    descriptor=contract.CountryAdmissionDescriptor(read,proof,m['upstream'],m['parameters'],'fixture-only',(),contract.canonical(m['tables']),contract.canonical(m['event_index']))
    p=Publication(cfg['dsn'],cfg['root'],country={'runtime':contract.CountryRuntime(own['component_dsn'],c1=c1),'observation_dsn':request['observation_dsn'],'detection_dsn':request['detection_dsn'],'production_receipt':c1.production_receipt})
    return p,descriptor,country_binding


def test_real_source_qualification_without_c1_rescan(source_only,monkeypatch):
    from contextlib import ExitStack
    p,descriptor,binding=source_only
    monkeypatch.setattr(p.country['runtime'].c1,'_check',lambda:pytest.fail('轻量资格不能调用C1._check'))
    with ExitStack() as stack:
        actual=binding.current(p,descriptor,stack)
        assert actual['component']['state']=='complete'
    # 占位描述没有真实C4冻结实体，不能进入准入证明。
    with pytest.raises(ValueError,match='实体缺失'):binding.capture(p,descriptor)


@pytest.mark.parametrize('target',['component','observation','detection','reference'])
def test_real_source_locks_and_revocation(source_only,target):
    from contextlib import ExitStack
    p,descriptor,binding=source_only;upstream=contract.typed_decode(descriptor.upstream_typed)
    table={'component':'country_components.components','observation':'domeye.runs','detection':'detection.runs','reference':'domeye.reference_inputs'}[target]
    key='component_id' if target=='component' else 'run_id'
    run=descriptor.read_binding.component.component_id if target=='component' else upstream['detection' if target=='detection' else 'observation']['run_id']
    dsn=p.country['runtime'].component_dsn if target=='component' else p.country['detection_dsn' if target=='detection' else 'observation_dsn']
    with ExitStack() as stack:
        binding.current(p,descriptor,stack,lock=True)
        db=psycopg2.connect(dsn)
        try:
            with db.cursor() as c:
                c.execute("SET lock_timeout='100ms'")
                with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute('UPDATE '+table+" SET state='failed' WHERE "+key+'=%s',(run,))
        finally:db.close()
    with psycopg2.connect(dsn) as db,db.cursor() as c:
        c.execute('SELECT DISTINCT state FROM '+table+' WHERE '+key+'=%s',(run,));original=c.fetchall();assert len(original)==1
        c.execute('UPDATE '+table+" SET state='failed' WHERE "+key+'=%s',(run,))
    try:
        with ExitStack() as stack,pytest.raises(ValueError):binding.current(p,descriptor,stack)
    finally:
        with psycopg2.connect(dsn) as db,db.cursor() as c:c.execute('UPDATE '+table+' SET state=%s WHERE '+key+'=%s',(original[0][0],run))


@pytest.mark.parametrize('fault',['database','reference','snapshot','source_order'])
def test_actual_country_source_mismatch_rejected(source_only,fault):
    from contextlib import ExitStack
    from dataclasses import replace
    p,descriptor,binding=source_only
    cfg=dict(p.country)
    if fault=='database':cfg['observation_dsn']=cfg['detection_dsn']
    elif fault=='reference':descriptor=replace(descriptor,read_binding=replace(descriptor.read_binding,reference=replace(descriptor.read_binding.reference,content_sha256='0'*64)))
    else:
        upstream=contract.typed_decode(descriptor.upstream_typed)
        if fault=='snapshot':upstream['observation']['snapshot']+=1
        else:upstream['detection']['identity']['selected_sources']=list(reversed(upstream['detection']['identity']['selected_sources']))
        descriptor=replace(descriptor,upstream_typed=contract.typed_encode(upstream))
    wrong=Publication(p.dsn,p.root,country=cfg)
    with ExitStack() as stack,pytest.raises(ValueError):binding.current(wrong,descriptor,stack)


def test_actual_source_freeze_and_catalog_without_body_counts(source_only):
    p,descriptor,binding=source_only
    files,catalogs=binding.source_files(p,descriptor)
    assert files and {c['role'] for c in catalogs}=={'observation','detection'}
    assert all(Path(f['path']).is_file() and len(f['sha256'])==64 for f in files)
    cfg=p.country
    for entry in catalogs:
        with psycopg2.connect(cfg[entry['role']+'_dsn']) as pg,pg.cursor() as c:
            assert binding.catalog_identity(c,entry['schema'],entry['snapshot'])==entry['identity']


def test_typed_selection_is_not_publication_authority(source_only):
    p,descriptor,_=source_only
    q1,_=p.discover()
    forged=contract.CountrySelection(q1.publication_id,q1.build_id,q1.profile_digest,'country:fake',descriptor,'fake',1,None,'fake')
    with pytest.raises(ValueError,match='非国家'):
        with p.verify_country_qualification(descriptor,selection=forged):pass
    with pytest.raises(ValueError,match='非国家'):
        p.select_country(q1,result_id=descriptor.read_binding.result_id,incident_id='fake',revision=1,cohort_id=None)


@pytest.fixture(scope='module')
def joint(source_only):
    from dataclasses import replace
    p,_,_=source_only
    root=Path(os.environ.get('DOMEYE_C4_PUBLICATION_EVIDENCE',str(Path(p.root)/'c4-publication')));saved=root/'joint.json'
    if not saved.exists():pytest.skip('需要真实joint_prepare，不以fixture代替')
    data=json.loads(saved.read_text());p.country['runtime']=replace(p.country['runtime'],verify_qualification=p.verify_country_qualification)
    return p,Token(**data['token']),contract.contract_value(data['read']),contract.contract_value(data['proof']),[contract.contract_value(s) for s in data['selections']]


def test_real_publication_all_country_pages_and_cursor(joint):
    from data_pipeline.analysis.country_events.selection_reader import open_qualified_country
    p,token,read,proof,selections=joint
    for selection in selections:
        with open_qualified_country(selection,proof,runtime=p.country['runtime'],verify_qualification=p.verify_country_qualification) as q:
            for view in contract.VIEWS:
                if view in ('resolve_source','resolve_alias'):continue
                request=contract.QueryRequest(view)
                first=list(q.iter_query(request,page_size=100));second=list(q.iter_query(request,page_size=37))
                assert first==second and type(first[-1]) is contract.QueryReadReceipt
                assert json.loads(first[-1].scope_json)['publication_id']==token.publication_id
            page=q.query(contract.QueryRequest('series15'),limit=1);assert page.next_cursor
            with pytest.raises(ValueError,match='cursor'):q.query(contract.QueryRequest('audit'),cursor=page.next_cursor,limit=1)
    assert selections and all(s.publication_id==token.publication_id for s in selections)


@pytest.mark.parametrize('field',['build_id','profile_digest','component_key','revision','cohort_id'])
def test_real_country_selection_cross_binding_rejected(joint,field):
    from dataclasses import replace
    p,token,read,proof,selections=joint;s=selections[0]
    bad=replace(s,**{field:999 if field=='revision' else 'wrong'})
    with pytest.raises(ValueError):
        with p.verify_country_qualification(s.descriptor,selection=bad):pass


def test_real_country_proof_counter_not_authority(joint):
    from dataclasses import replace
    p,token,read,proof,_=joint;before=p.discover(contract.SELECTOR)
    with pytest.raises(ValueError):p.prepare_country(read,replace(proof,stream_calls=99),reference_binding=read.reference,profile=contract.PROFILE)
    assert p.discover(contract.SELECTOR)==before


def test_real_country_publish_locks_to_before_visible_and_rollback(joint,monkeypatch):
    p,old,read,proof,selections=joint;before=p.discover(contract.SELECTOR)
    candidate=p.prepare_country(read,proof,reference_binding=read.reference,profile=contract.PROFILE)
    with pytest.raises(ValueError):p.select_country(candidate,result_id=read.result_id,incident_id=selections[0].incident_id,revision=selections[0].revision,cohort_id=selections[0].cohort_id)
    upstream=contract.typed_decode(selections[0].descriptor.upstream_typed)
    def checkpoint(name):
        if name!='before_visible':return
        for role,table,field,key in [('component','country_components.components','component_id',read.component.component_id),('observation','domeye.runs','run_id',upstream['observation']['run_id']),('detection','detection.runs','run_id',upstream['detection']['run_id']),('observation','domeye.reference_inputs','run_id',upstream['observation']['run_id'])]:
            dsn=p.country['runtime'].component_dsn if role=='component' else p.country[role+'_dsn'];db=psycopg2.connect(dsn)
            try:
                with db.cursor() as c:
                    c.execute("SET lock_timeout='100ms'")
                    with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute('UPDATE '+table+" SET state='failed' WHERE "+field+'=%s',(key,))
            finally:db.close()
        raise RuntimeError('提交前人工失败')
    monkeypatch.setattr(p,'_checkpoint',checkpoint)
    with pytest.raises(RuntimeError):p.publish(candidate,expected_generation=before[1],selector=contract.SELECTOR)
    assert p.discover(contract.SELECTOR)==before


@pytest.fixture(scope='module')
def next_country_head(joint):
    from dataclasses import replace
    from data_pipeline.analysis.country_events import selection_index as query_index
    p,old,read,proof,selections=joint
    aliases=[]
    for s in selections:
        aliases.append({'source':'legacy-fixture','table':'event_metric','ref_kind':'metric','id':'same','incident_id':s.incident_id,'revision':s.revision,'cohort_id':s.cohort_id,'original_position':[3,3,1]})
    aliases.append(dict(aliases[0]));aliases.append({**aliases[0],'id':'unique'})
    p.country['runtime']=replace(p.country['runtime'],aliases_typed=contract.typed_encode(tuple(aliases)),aliases_source_json=contract.canonical({'publication':'legacy-fixture','version':'1'}))
    root=Path(os.environ.get('DOMEYE_C4_PUBLICATION_EVIDENCE',str(Path(p.root)/'c4-publication')));target=root/('alias-index-'+uuid.uuid4().hex)
    new_read,new_proof=query_index.prepare_country_index(read.component,reference_binding=read.reference,runtime=p.country['runtime'],output=target,limits=contract.QueryLimits())
    token=p.prepare_country(new_read,new_proof,reference_binding=new_read.reference,profile=contract.PROFILE)
    _,generation=p.discover(contract.SELECTOR);p.publish(token,expected_generation=generation,selector=contract.SELECTOR)
    (root/'joint-alias.json').write_text(json.dumps({'token':token.__dict__,'read':contract.contract_json(new_read),'proof':contract.contract_json(new_proof)},ensure_ascii=False))
    return p,token,new_read,new_proof,selections


def test_real_country_old_p_after_new_head_and_all_alias_targets(joint,next_country_head):
    from data_pipeline.analysis.country_events.selection_reader import open_qualified_country
    p,new,read,proof,_=next_country_head;_,old,old_read,old_proof,selections=joint
    assert p.discover(contract.SELECTOR)[0]==new and old!=new
    for s in selections:
        selected=p.select_country(old,result_id=old_read.result_id,incident_id=s.incident_id,revision=s.revision,cohort_id=s.cohort_id)
        with open_qualified_country(selected,old_proof,runtime=p.country['runtime'],verify_qualification=p.verify_country_qualification) as q:
            assert q.query(contract.QueryRequest('series15'),limit=1).total>1
    alias={'source':'legacy-fixture','table':'event_metric','ref_kind':'metric','id':'same'}
    result=p.resolve_alias(new,alias=alias,page_size=1)
    assert result['resolution']=='ambiguous_reference' and len(result['candidates'])==2 and result['total']==3 and len(result['items'])==1
    assert result['items'][0]['original_position']==[3,3,1]
    assert p.resolve_alias(new,alias={**alias,'id':'unique'})['resolution']=='resolved'
    assert p.resolve_alias(new,alias={**alias,'id':'absent'})['resolution']=='not_found'
    assert p.resolve_alias(old,alias=alias)['resolution']=='not_available'


@pytest.mark.parametrize('target',['component','observation','detection','reference','publication'])
def test_real_country_query_tail_revocation(joint,target,monkeypatch):
    from data_pipeline.analysis.country_events.selection_reader import open_qualified_country
    p,token,read,proof,selections=joint;s=selections[0];upstream=contract.typed_decode(s.descriptor.upstream_typed)
    table={'component':'country_components.components','observation':'domeye.runs','detection':'detection.runs','reference':'domeye.reference_inputs','publication':'publication_q1.builds'}[target]
    key='component_id' if target=='component' else 'build_id' if target=='publication' else 'run_id'
    value=read.component.component_id if target=='component' else token.build_id if target=='publication' else upstream['detection' if target=='detection' else 'observation']['run_id']
    dsn=p.country['runtime'].component_dsn if target=='component' else p.dsn if target=='publication' else p.country['detection_dsn' if target=='detection' else 'observation_dsn']
    with psycopg2.connect(dsn) as db,db.cursor() as c:
        c.execute('SELECT DISTINCT state FROM '+table+' WHERE '+key+'=%s',(value,));states=c.fetchall();assert len(states)==1;state=states[0][0]
    before=p.discover(contract.SELECTOR)
    try:
        with open_qualified_country(s,proof,runtime=p.country['runtime'],verify_qualification=p.verify_country_qualification) as q:
            original=q.access.read
            def revoked(locators):
                result=original(locators)
                with psycopg2.connect(dsn) as db,db.cursor() as c:c.execute('UPDATE '+table+" SET state='failed' WHERE "+key+'=%s',(value,))
                return result
            monkeypatch.setattr(q.access,'read',revoked)
            with pytest.raises(ValueError):q.query(contract.QueryRequest('series15'),limit=2)
    finally:
        with psycopg2.connect(dsn) as db,db.cursor() as c:c.execute('UPDATE '+table+' SET state=%s WHERE '+key+'=%s',(state,value))
    assert p.discover(contract.SELECTOR)==before


@pytest.mark.parametrize('target',['component','publication'])
def test_real_alias_nested_qualification_revocation(joint,target,monkeypatch):
    from data_pipeline.analysis.country_events.selection_reader import open_qualified_country
    p,_,_,_,selections=joint
    saved=json.loads((Path(os.environ.get('DOMEYE_C4_PUBLICATION_EVIDENCE',str(Path(p.root)/'c4-publication')))/'joint-alias.json').read_text());token=Token(**saved['token']);read=contract.contract_value(saved['read']);proof=contract.contract_value(saved['proof']);s=selections[0]
    selected=p.select_country(token,result_id=read.result_id,incident_id=s.incident_id,revision=s.revision,cohort_id=s.cohort_id)
    dsn=p.dsn if target=='publication' else p.country['runtime'].component_dsn
    table='publication_q1.builds' if target=='publication' else 'country_components.components'
    key='build_id' if target=='publication' else 'component_id';value=token.build_id if target=='publication' else read.component.component_id
    try:
        with open_qualified_country(selected,proof,runtime=p.country['runtime'],verify_qualification=p.verify_country_qualification) as q:
            original=q.alias
            def revoked(*args):
                result=original(*args)
                with psycopg2.connect(dsn) as db,db.cursor() as c:c.execute('UPDATE '+table+" SET state='failed' WHERE "+key+'=%s',(value,))
                return result
            monkeypatch.setattr(q,'alias',revoked)
            with pytest.raises(ValueError):q.query(contract.QueryRequest('resolve_alias',contract.canonical({'id':'same'})),limit=1)
    finally:
        with psycopg2.connect(dsn) as db,db.cursor() as c:c.execute('UPDATE '+table+' SET state=%s WHERE '+key+'=%s',('published' if target=='publication' else 'complete',value))


@pytest.mark.parametrize('alias',[False,True])
def test_real_admission_revoked_during_page(joint,next_country_head,monkeypatch,alias):
    from data_pipeline.analysis.country_events.selection_reader import open_qualified_country
    p,token,read,proof,selections=next_country_head if alias else joint
    s=selections[0]
    selected=p.select_country(token,result_id=read.result_id,incident_id=s.incident_id,revision=s.revision,cohort_id=s.cohort_id)
    def state(value):
        with psycopg2.connect(p.country['runtime'].component_dsn) as db,db.cursor() as c:
            c.execute('UPDATE country_components.read_admissions SET state=%s WHERE read_model_id=%s',(value,read.read_model_id));assert c.rowcount==1
    try:
        with open_qualified_country(selected,proof,runtime=p.country['runtime'],verify_qualification=p.verify_country_qualification) as q:
            owner=q if alias else q.access;name='alias' if alias else 'read';original=getattr(owner,name)
            def revoke(*args):
                result=original(*args);state('revoked');return result
            monkeypatch.setattr(owner,name,revoke)
            with pytest.raises(ValueError,match='C4_admission_mismatch'):
                q.query(contract.QueryRequest('resolve_alias',contract.canonical({'id':'same'})) if alias else contract.QueryRequest('series15'),limit=1)
    finally:state('accepted')


def test_original_unanchored_c4_is_rejected(joint):
    p,_,_,_,_=joint
    saved=json.loads((Path(p.root)/'c4-publication/joint.json').read_text())
    read=contract.contract_value(saved['read']);proof=contract.contract_value(saved['proof'])
    with pytest.raises(ValueError,match='C4_admission_missing'):
        p.prepare_country(read,proof,reference_binding=read.reference,profile=contract.PROFILE)


@pytest.mark.parametrize('fail',[False,True])
def test_admission_lock_covers_control_commit_and_rollback(joint,monkeypatch,fail):
    from contextlib import contextmanager
    from data_pipeline.results import country_binding
    p,_,read,proof,_=joint;before=p.discover(contract.SELECTOR)
    candidate=p.prepare_country(read,proof,reference_binding=read.reference,profile=contract.PROFILE)
    original=country_binding.verify_country_admission;observed=[]
    def blocked():
        db=psycopg2.connect(p.country['runtime'].component_dsn)
        try:
            with db.cursor() as c:
                c.execute("SET lock_timeout='100ms'")
                with pytest.raises(psycopg2.errors.LockNotAvailable):
                    c.execute("UPDATE country_components.read_admissions SET state='revoked' WHERE read_model_id=%s",(read.read_model_id,))
        finally:db.close()
    @contextmanager
    def checked(*args,**kwargs):
        with original(*args,**kwargs):
            try:yield
            finally:
                if kwargs.get('lock'):
                    # 外层控制事务已退出，锚上下文仍未退出：从另一连接核可见状态和锁。
                    blocked();observed.append(p.discover(contract.SELECTOR))
    def checkpoint(name):
        if name=='before_visible':
            blocked()
            if fail:raise RuntimeError('锚锁内人工回滚')
    monkeypatch.setattr(country_binding,'verify_country_admission',checked)
    monkeypatch.setattr(p,'_checkpoint',checkpoint)
    if fail:
        with pytest.raises(RuntimeError,match='锚锁内人工回滚'):p.publish(candidate,expected_generation=before[1],selector=contract.SELECTOR)
    else:p.publish(candidate,expected_generation=before[1],selector=contract.SELECTOR)
    expected=before if fail else (candidate,before[1]+1)
    assert observed==[expected] and p.discover(contract.SELECTOR)==expected
    # 退出后锁确已释放；仅验证可更新，回滚保持锚有效。
    db=psycopg2.connect(p.country['runtime'].component_dsn)
    try:
        with db.cursor() as c:
            c.execute("SET lock_timeout='100ms'")
            c.execute("UPDATE country_components.read_admissions SET state='revoked' WHERE read_model_id=%s",(read.read_model_id,));assert c.rowcount==1
    finally:db.rollback();db.close()
