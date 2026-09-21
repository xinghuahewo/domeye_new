"""重试预检只读，材料损坏在任何连接/目录准备前拒绝。"""
import hashlib
import json
import pytest
import gzip
import io
from pathlib import Path


@pytest.fixture
def materials(tmp_path,monkeypatch):
    """完整原人工材料副本；只替换文件系统边界，不改 AD 或领域验证。"""
    bundle=json.loads(gzip.decompress((Path(__file__).parents[1]/'fixtures/publication_v9_materials.json.gz').read_bytes()))
    original_open=Path.open;original_is_dir=Path.is_dir
    def open_fixture(path,*args,**kwargs):
        if str(path) in bundle['files']:
            value=bundle['files'][str(path)]
            return io.BytesIO(value.encode()) if args and 'b' in args[0] else io.StringIO(value)
        return original_open(path,*args,**kwargs)
    monkeypatch.setattr(Path,'open',open_fixture)
    monkeypatch.setattr(Path,'is_dir',lambda p:True if str(p).startswith('/Users/botongwu/.codex/outputs/') else original_is_dir(p))
    def save(name,value):
        path=tmp_path/name;path.write_text(json.dumps(value,ensure_ascii=False))
        return dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    request=bundle['request'];config=bundle['config']
    values=request['components']+[d['admission'] for d in request['dependencies']]
    repo=Path(__file__).resolve().parents[3]
    paths=[p for n in ('bgp','analysis/resources','analysis/detection','analysis/country_events','analysis/country_trends') for p in (repo/'backend/data_pipeline'/n).rglob('*.py')]
    paths+=list((repo/'backend/data_pipeline/analysis/features').glob('*.py'))+[repo/'backend/pyproject.toml',repo/'backend/uv.lock',repo/'frontend/package-lock.json']
    spec=dict(contract='publication-retry/v1',enabled=False,request=save('request.json',request),config=save('config.json',config),
        admissions=[save(a['admission_id']+'.json',a) for a in values],
        owner_code=save('codes.json',{str(p.relative_to(repo)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}),
        physical={a['admission_id']:dict(a['physical']) for a in values},
        publication_root=config['tail']['publication']['private_root'],publication_physical=['7685154057349199474',16388],
        attempt_root=str(tmp_path.parent/(tmp_path.name+'-attempt')),
        expected_generation=0,selector='artificial:m3:combined-country-trend',action='prepare')
    import psycopg2
    monkeypatch.setattr(psycopg2,'connect',lambda *a,**k:pytest.fail('材料预检不得连接 PG'))
    return spec,bundle


def test_complete_original_graph_preflight_is_read_only(materials):
    from data_pipeline.results.retry.driver import preflight
    spec,_=materials;before={p['path']:Path(p['path']).read_bytes() for p in spec['admissions']}
    checked=preflight(spec)
    assert checked['summary']==dict(state='materials_checked',admissions=20,requests=72,live_current='Unknown')
    assert not Path(spec['attempt_root']).exists()
    assert all(Path(p).read_bytes()==v for p,v in before.items())


@pytest.mark.parametrize('fault',['dependency','binding','physical','root','overlap'])
def test_invalid_saved_graph_or_write_scope_is_rejected(materials,fault):
    from data_pipeline.results.retry.driver import preflight
    spec,bundle=materials
    if fault=='dependency':spec['admissions'].pop()
    elif fault=='binding':
        p=spec['admissions'][0];value=json.loads(Path(p['path']).read_text());value['owner_binding']+=' '
        Path(p['path']).write_text(json.dumps(value));p['sha256']=hashlib.sha256(Path(p['path']).read_bytes()).hexdigest()
    elif fault=='physical':spec['physical'][bundle['request']['components'][0]['admission_id']]['database_oid']=999
    elif fault=='root':spec['publication_root']+='/wrong'
    elif fault=='overlap':spec['attempt_root']=bundle['config']['run_root']+'/new-attempt'
    with pytest.raises(ValueError):preflight(spec)


def test_original_publish_generation_conflict_never_writes_head(materials,tmp_path,monkeypatch):
    from data_pipeline.results import Publication, Token
    from data_pipeline.results.manifest_io import digest, write_sealed
    from data_pipeline.results import published_reader as combined
    import psycopg2
    spec,bundle=materials;q=bundle['request'];build='fixture-build'
    manifest={k:q[k] for k in ('contract','profile','components','dependencies','edges','role_graph','artificial_input')}
    manifest.update(build_id=build,code_sha256='0'*64,
        schema_sha256=combined.schema_sha(q['profile'],derived_reference=True),
        qualification_summary=[dict(component_key=a['owner'],storage_state='complete',coverage_typed='fixture-only',codec_version=a['binding_codec']) for a in q['components']],
        composition_validation=dict(validator_version=q['contract'],edges_checked=len(q['edges']),typed_digest='0'*64))
    file=write_sealed(tmp_path/'manifest.json',manifest)
    token=Token('q1_'+digest(manifest),build,digest(q['profile']))
    class PG:
        def __init__(self):self.last='';self.writes=[];self.rollbacks=0
        def __enter__(self):return self
        def __exit__(self,kind,*rest):
            if kind:self.rollbacks+=1
        def cursor(self):return self
        def execute(self,sql,args=None):
            self.last=sql
            if sql.startswith(('INSERT','UPDATE','DELETE')):self.writes.append(sql)
        def fetchone(self):
            if 'SELECT identity,root' in self.last:return (dict(system_identifier='1',database_oid='2'),str(tmp_path))
            if 'system_identifier::text FROM' in self.last:return ('1',)
            if 'oid::text' in self.last:return ('2',)
            if 'listen_addresses' in self.last:return ('',)
            if 'state,manifest,' in self.last:return ('ready',manifest,file['path'],file['sha256'],token.publication_id)
            if 'SELECT publication_id,build_id,generation' in self.last:return ('old','old-build',3)
            if 'SELECT state FROM' in self.last:return ('ready',)
            raise AssertionError(self.last)
    pg=PG();monkeypatch.setattr(psycopg2,'connect',lambda *a,**k:pg)
    publication=Publication('fixture',tmp_path)
    for _ in range(2):
        with pytest.raises(ValueError,match='generation冲突'):
            publication.publish(token,expected_generation=0,selector=spec['selector'])
    assert pg.writes==[] and pg.rollbacks>=2


def test_directory_preparation_preserves_old_files_and_is_exclusive(materials):
    from data_pipeline.results.retry.driver import preflight, prepare_directories
    spec,_=materials
    before=Path(spec['request']['path']).read_bytes()
    checked=preflight(spec);prepare_directories(checked)
    attempt=Path(spec['attempt_root'])
    assert len(list((attempt/'scratch').iterdir()))==20
    assert Path(spec['request']['path']).read_bytes()==before
    assert preflight(spec,prepared=True)['summary']['requests']==72
    with pytest.raises(ValueError,match='已存在'):prepare_directories(checked)


def test_constructs_all_original_runtimes_without_pg_admit_or_producer(materials,tmp_path,monkeypatch):
    from data_pipeline.results.retry.driver import preflight, prepare_directories
    from data_pipeline.results.retry.runtime import construct
    from data_pipeline.results.component_readers import api, Admissions
    from data_pipeline.jobs import result_admission as migration_admission, migration as migration_driver
    spec,_=materials;checked=prepare_directories(preflight(spec))
    def forbidden(*a,**k):pytest.fail('装载不得调用 admit/producer')
    monkeypatch.setattr(migration_admission,'admit_current',forbidden)
    monkeypatch.setattr(migration_driver,'produce_request',forbidden)
    for owner in {a['owner'] for a in checked['admissions']}:monkeypatch.setattr(api(owner),'admit',forbidden)
    original_stat=Path.stat;original_resolve=Path.resolve
    backing=tmp_path.stat()
    def saved(path):return str(path).startswith('/Users/botongwu/.codex/outputs/')
    monkeypatch.setattr(Path,'stat',lambda p,*a,**k:backing if saved(p) else original_stat(p,*a,**k))
    monkeypatch.setattr(Path,'resolve',lambda p,*a,**k:p if saved(p) else original_resolve(p,*a,**k))
    runtimes=construct(checked['config'],checked['admissions'],spec['attempt_root'])
    graph=Admissions(checked['admissions'],runtimes,guard=lambda:None)
    assert len(runtimes)==20 and len(graph.lock_targets)==49
    for admission in checked['admissions']:
        # 原规则真实计算；构造器成功不能代替此项代码/环境/完整绑定比较。
        actual=api(admission['owner'])._rules(runtimes[admission['admission_id']])
        assert actual=={k:v for k,v in admission['validator'].items() if k!='validation_digest'}
    trend=next(a for a in checked['admissions'] if a['owner']=='trend')
    assert str(runtimes[trend['admission_id']].output_root)==trend['physical']['root']
    from data_pipeline.results.component_streams import requests
    class BeforePG(RuntimeError):pass
    import psycopg2
    connected=[]
    def stop(dsn,*a,**k):
        connected.append(dsn)
        raise BeforePG('到达原入口第一次 PG 连接')
    monkeypatch.setattr(psycopg2,'connect',stop)
    config=checked['config']
    assert config['detection']['detection_dsn']!=config['observation_dsn']
    detection=next(a for a in checked['admissions'] if a['owner']=='detection')
    detection_runtime=runtimes[detection['admission_id']]
    binding=api('detection').untyped(detection['owner_binding'])
    with pytest.raises(BeforePG):
        api('detection').inspect_binding(detection_runtime,binding['run_id'],binding['snapshot'])
    assert connected.pop()==config['detection']['detection_dsn']
    from data_pipeline.results.component_readers import LOCK_OWNERS
    for admission in checked['admissions']:
        owner=admission['owner']
        expected=(config['tail'][owner]['dsn'] if owner in ('country','trend') else
                  config['detection']['detection_dsn'] if owner=='detection' else config['observation_dsn'])
        original_runtime=runtimes[admission['admission_id']]
        assert original_runtime.dsn==expected
        target=next(t for t in admission['lock_targets'] if LOCK_OWNERS[t['namespace']][0]==owner)
        with pytest.raises(BeforePG):
            with api(owner).hold_lock(original_runtime,admission,target,guard=lambda:None):
                pytest.fail('不得取得真实锁')
        assert connected.pop()==expected
    assert all(r.dsn==config['observation_dsn'] for r in detection_runtime.dependency_runtimes.values())
    runtime=runtimes[trend['admission_id']]
    rows,size=graph.envelope(trend['admission_id'],batch_rows=256,max_row_bytes=4194304)
    request=next(q for q in requests('trend',batch_rows=rows,batch_bytes=size,admission=trend) if q['view']=='context_source')
    with pytest.raises(BeforePG):
        with api('trend').open_reader(runtime,trend,request,guard=lambda:None):pytest.fail('不运行 PG')
    assert connected==[config['tail']['trend']['dsn']]


@pytest.mark.parametrize('fault',['oid','root'])
def test_live_control_boundary_rejects_wrong_identity_without_writing(tmp_path,monkeypatch,fault):
    from data_pipeline.results.retry.driver import verify_control
    from data_pipeline.results import Publication
    import psycopg2
    class PG:
        last='';closed=False
        def set_session(self,**kwargs):assert kwargs==dict(readonly=True)
        def close(self):self.closed=True
        def cursor(self):return self
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def execute(self,sql,args=None):
            assert sql.startswith('SELECT');self.last=sql
        def fetchone(self):
            if 'SELECT system_identifier::text,(' in self.last:return ('1',99 if fault=='oid' else 2)
            if 'SELECT identity,root' in self.last:return (dict(system_identifier='1',database_oid='2'),str(tmp_path/'wrong'))
            if 'system_identifier::text FROM' in self.last:return ('1',)
            if 'oid::text' in self.last:return ('2',)
            if 'listen_addresses' in self.last:return ('',)
            raise AssertionError(self.last)
    pg=PG();monkeypatch.setattr(psycopg2,'connect',lambda *a,**k:pg)
    with pytest.raises(ValueError,match='物理库|根目录'):
        verify_control(Publication('fixture',tmp_path),['1',2])
    assert pg.closed


def test_failed_attempt_revalidation_keeps_all_old_materials(materials):
    from data_pipeline.results.retry.driver import preflight, prepare_directories, execute
    spec,_=materials;checked=prepare_directories(preflight(spec))
    paths=[p['path'] for p in spec['admissions']]+[spec['request']['path'],spec['config']['path']]
    before={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in paths}
    # 本次 prepared 配置变更必须在任何 Runtime/live 步骤前拒绝。
    checked['spec']['enabled']=True
    with pytest.raises(ValueError,match='配置漂移'):execute(checked)
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==sha for p,sha in before.items())
    assert not (Path(spec['attempt_root'])/'仅一次执行.json').exists()


def test_bad_material_sha_never_creates_attempt_or_connects(tmp_path,monkeypatch):
    from data_pipeline.results.retry.driver import preflight
    import psycopg2
    monkeypatch.setattr(psycopg2,'connect',lambda *a,**k:pytest.fail('预检不得连接 PG'))
    source=tmp_path/'request.json';source.write_text('{}')
    attempt=tmp_path/'attempt'
    config=dict(request=dict(path=str(source),sha256='0'*64),attempt_root=str(attempt))
    with pytest.raises(ValueError,match='SHA'):
        preflight(config)
    assert not attempt.exists() and source.read_text()=='{}'
