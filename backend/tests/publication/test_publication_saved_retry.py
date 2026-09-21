"""仅修复既有profile迁移与保存Token续发；不连接真实PG。"""
from contextlib import contextmanager
from dataclasses import asdict
from types import SimpleNamespace
import pytest
from data_pipeline.results import Token, profiles
from data_pipeline.results.retry import driver as retry
from tests.publication.test_publication_retry import materials

@pytest.mark.parametrize('state,head,expected',[('ready',None,'ready'),('published',('p','b',1),'published'),('ready',('other','other',1),'error'),('published',None,'error'),('mismatch',None,'error')])
def test_saved_classification_is_read_only_and_bound(monkeypatch,state,head,expected):
    token=Token('p','b','profile');request=dict(contract='fixture',profile={},components=[],dependencies=[],edges=[],role_graph={},artificial_input={})
    class PG:
        def set_session(self,**kw):assert kw==dict(readonly=True,isolation_level='REPEATABLE READ')
        def close(self):pass
        def cursor(self):return self
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def execute(self,sql,args=None):assert sql.startswith('SELECT');self.sql=sql
        def fetchone(self):return (state,) if 'SELECT state' in self.sql else head
    monkeypatch.setattr(retry.psycopg2,'connect',lambda *a,**k:PG())
    publication=SimpleNamespace(dsn='fixture',_owner=lambda c:None,_manifest=lambda c,t,s:dict(request,profile={'wrong':True}) if state=='mismatch' else request)
    if expected=='error':
        with pytest.raises(ValueError):retry.classify_saved(publication,token,request,dict(selector='fixture',expected_generation=0))
    else:assert retry.classify_saved(publication,token,request,dict(selector='fixture',expected_generation=0))['state']==expected

@pytest.mark.parametrize('action,classification,migrate,fail_schema',[
    ('publish_saved','ready',True,False),('publish_saved','published',True,False),
    ('publish',None,False,True),('publish',None,True,False)])
def test_action_order_no_repeat_prepare(tmp_path,monkeypatch,action,classification,migrate,fail_schema):
    calls=[];token=Token('p','b','profile')
    @contextmanager
    def phase(*a,**k):yield {}
    monkeypatch.setattr(retry,'stage',phase)
    monkeypatch.setattr(retry,'classify_saved',lambda *a:(calls.append('classify') or dict(state=classification,generation=1 if classification=='published' else None)))
    def schema(*a):
        calls.append('schema')
        if fail_schema:raise ValueError('profile未迁移')
    monkeypatch.setattr(retry,'verify_profile',schema)
    monkeypatch.setattr(retry,'finish_publication',lambda *a:calls.append('publish'))
    monkeypatch.setattr(retry,'confirm_discovery',lambda *a:calls.append('discover'))
    class Publication:
        guard=staticmethod(lambda:None)
        def migrate_profiles(self):calls.append('migrate')
        def prepare_combined(self,*a,**k):calls.append('prepare');return token
    spec=dict(action=action,migrate_profiles=migrate,selector='fixture',expected_generation=0)
    checked=dict(spec=spec,request={},saved_token=asdict(token))
    if fail_schema:
        with pytest.raises(ValueError):retry.perform_action(Publication(),checked,{},tmp_path,{})
        assert calls==['schema']
    else:
        retry.perform_action(Publication(),checked,dict(max_rows=1,max_bytes=1),tmp_path,{})
        assert calls==(['classify','migrate','schema','publish'] if classification=='ready' else
                       ['classify','schema','discover'] if classification=='published' else ['migrate','schema','prepare','publish'])

@pytest.mark.parametrize('legacy',[True,False])
def test_actual_profile_check_detects_legacy_without_writes(monkeypatch,legacy):
    selector='artificial:m3:combined-country-trend'
    class PG:
        def set_session(self,**kw):assert kw['readonly'] is True
        def close(self):pass
        def cursor(self):return self
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def execute(self,sql,args=None):assert sql.startswith('SELECT');self.sql=sql
        def fetchone(self):
            if 'to_regclass' in self.sql:return ('schema_version','profiles')
            return (profiles.SELECTORS[selector],)
        def fetchall(self):return [(3,)] if 'SELECT version' in self.sql else [('head_selector_check' if legacy else 'head_profile_fk',)]
    monkeypatch.setattr(retry.psycopg2,'connect',lambda *a,**k:PG())
    publication=SimpleNamespace(dsn='fixture',_owner=lambda c:None)
    if legacy:
        with pytest.raises(ValueError,match='旧约束'):retry.verify_profile(publication,selector)
    else:retry.verify_profile(publication,selector)


def test_default_cli_only_preflights(tmp_path,monkeypatch):
    import runpy,sys,json
    from pathlib import Path
    module=runpy.run_path(str(Path(__file__).resolve().parents[3]/'scripts/pipeline/publication-retry.py'))
    main=module['main'];config=tmp_path/'spec.json';config.write_text('{}')
    calls=[]
    monkeypatch.setattr(retry,'preflight',lambda spec:(calls.append('preflight') or dict(admissions=[],summary={})))
    def forbidden(*a,**k):pytest.fail('默认入口不得连接或执行')
    for name in ['execute','prepare_directories','inspect_saved']:monkeypatch.setattr(retry,name,forbidden)
    monkeypatch.setattr(retry.psycopg2,'connect',forbidden)
    monkeypatch.setattr(sys,'argv',['publication-retry.py',str(config)])
    main()
    assert calls==['preflight'] and list(tmp_path.iterdir())==[config]


def test_saved_token_preflight_checks_sha_and_does_not_create_attempt(materials,tmp_path):
    import hashlib,json
    spec,bundle=materials
    token=Token('p','b',retry.digest(bundle['request']['profile']))
    path=tmp_path/'prepare.json';path.write_text(json.dumps(asdict(token)))
    spec.update(action='publish_saved',migrate_profiles=True,prepared_token=dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    checked=retry.preflight(spec)
    assert checked['saved_token']==asdict(token)
    from pathlib import Path
    assert not Path(spec['attempt_root']).exists()
    path.write_text('{}')
    with pytest.raises(ValueError,match='SHA'):retry.preflight(spec)


def test_saved_same_token_discover_error_keeps_confirmation(tmp_path,monkeypatch):
    token=Token('p','b','profile')
    monkeypatch.setattr(retry,'classify_saved',lambda *a:dict(state='published',generation=1))
    monkeypatch.setattr(retry,'verify_profile',lambda *a:None)
    class Publication:
        guard=staticmethod(lambda:None)
        def migrate_profiles(self):pytest.fail('同Token已发布不得迁移或重发')
        def prepare_combined(self,*a,**k):pytest.fail('不得重prepare')
        def publish(self,*a,**k):pytest.fail('不得重publish')
        def discover(self,*a):raise OSError('discover unavailable')
    status={}
    with pytest.raises(OSError,match='discover unavailable'):
        retry.perform_action(Publication(),dict(spec=dict(action='publish_saved',migrate_profiles=True,selector='fixture'),request={},saved_token=asdict(token)),{},tmp_path,status)
    assert status['publication_state']=='confirmed' and status['last_completed_state']=='published'
    assert status['published_token']==asdict(token)
