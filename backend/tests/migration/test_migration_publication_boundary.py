"""真实观测目录与原发布动作接缝；PG/owner用边界fixture，不代表业务验收。"""
from pathlib import Path
from types import SimpleNamespace as NS
from contextlib import contextmanager
import json
import pytest
from data_pipeline.jobs import downstream as tail
from data_pipeline.results import Token


@pytest.mark.parametrize('fail',[False,True])
@pytest.mark.parametrize('resumed',[False,True])
def test_existing_private_root_reaches_original_prepare_and_preserves_error(tmp_path,monkeypatch,fail,resumed):
    from data_pipeline import results as publication_q1
    from data_pipeline.results.retry import driver as publication_retry
    from data_pipeline.results import fixture_metadata as artificial_input, component_streams as combined_streams
    private=tmp_path/'publication';private.mkdir()
    request={'components':[{'owner':x} for x in ['country','detection','feature','resource','trend']]}
    monkeypatch.setattr(tail,'combined_request',lambda *a:(request,{}))
    monkeypatch.setattr(combined_streams,'requests',lambda owner,**k:range(68 if owner=='country' else 1))
    monkeypatch.setattr(artificial_input,'ArtificialInput',lambda **k:object())
    monkeypatch.setattr(publication_retry,'verify_profile',lambda *a:None)
    calls=[];token=Token('fixture','fixture','fixture')
    class P:
        def __init__(self,dsn,private_root,**k):assert Path(private_root)==private
        def guard(self):pass
        def initialize(self):calls.append('initialize')
        def migrate_profiles(self):calls.append('migrate')
        def prepare_combined(self,value,**kwargs):
            assert value is request and callable(kwargs['progress']);calls.append('prepare')
            if fail:raise ValueError('原prepare失败')
            return token
        def publish(self,value,**kwargs):assert value==token;calls.append('publish');return token
        def discover(self,selector):calls.append('discover');return token,1
    monkeypatch.setattr(publication_q1,'Publication',P)
    config=dict(manifest='fixture',mapping='fixture',input={},country_reference={},expected_publication_requests=72,
                tail={'publication':dict(dsn='fixture',private_root=str(private),limits={},max_rows=100,max_bytes=100,expected_generation=0)})
    if resumed:config['failed_publication']={'build_id':'prior'}
    setup=[] if resumed else ['initialize','migrate']
    if fail:
        with pytest.raises(ValueError,match='原prepare失败'):tail.publish_pairs(config,[],tmp_path,lambda:None,{})
        assert calls==setup+['prepare']
        assert not (tmp_path/'publication-observation/原prepare返回.json').exists()
    else:
        result=tail.publish_pairs(config,[],tmp_path,lambda:None,{})
        assert result['state']=='artificial_combined_published' and result['generation']==1
        assert calls==setup+['prepare','publish','discover']
        assert json.loads((tmp_path/'publication-observation/原discover返回.json').read_text())['generation']==1
    assert list(private.iterdir())==[]
    with pytest.raises(FileExistsError):tail.publish_pairs(config,[],tmp_path,lambda:None,{})


@pytest.mark.parametrize('actual,schema',[(('same',17730),None),(('same',17729),'publication_q1')])
def test_pristine_check_rejects_identity_or_existing_schema(tmp_path,monkeypatch,actual,schema):
    import psycopg2
    class Cursor:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def execute(self,sql):self.value=(schema,) if 'to_regnamespace' in sql else actual
        def fetchone(self):return self.value
    class PG:
        def set_session(self,**kw):assert kw['readonly'] is True
        def cursor(self):return Cursor()
        def close(self):pass
    monkeypatch.setattr(psycopg2,'connect',lambda *a:PG())
    with pytest.raises(ValueError):tail.require_pristine_publication(NS(dsn='fixture',root=tmp_path),['same',17729])


@pytest.mark.parametrize('change',[None,'identity','owner','candidate','token','head','extra_build','directory'])
def test_failed_publication_classifies_without_writing(tmp_path,monkeypatch,change):
    import psycopg2
    from data_pipeline.results.retry import driver as publication_retry
    monkeypatch.setattr(publication_retry,'verify_profile',lambda *a:None)
    (tmp_path/'q1-builds/prior').mkdir(parents=True)
    if change=='directory':(tmp_path/'q1-builds/other').mkdir()
    queries=[]
    class Cursor:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def execute(self,sql):
            assert sql.startswith('SELECT ');queries.append(sql);self.sql=sql
        def fetchone(self):return ('same',17730 if change=='identity' else 17729)
        def fetchall(self):
            if 'publication_q1.head' in self.sql:return [('s','prior','token',1)] if change=='head' else []
            row=('prior','candidate' if change=='candidate' else 'failed','digest',None,None,None,'token' if change=='token' else None)
            return [row,row] if change=='extra_build' else [row]
    class PG:
        def set_session(self,**kw):assert kw==dict(readonly=True,isolation_level='REPEATABLE READ')
        def cursor(self):return Cursor()
        def close(self):pass
    def owner(c):
        if change=='owner':raise ValueError('owner漂移')
    monkeypatch.setattr(psycopg2,'connect',lambda *a:PG())
    p=NS(dsn='fixture',root=tmp_path,_owner=owner)
    failure=dict(build_id='prior',input_digest='digest')
    if change:
        with pytest.raises(ValueError):tail.require_failed_publication(p,['same',17729],failure)
    else:
        state=tail.require_failed_publication(p,['same',17729],failure)
        assert state['state']=='known_failed_without_token' and state['history_preserved']
    assert (tmp_path/'q1-builds/prior').is_dir()
