"""人工再准入接线的副作用边界；不连接PG、不运行科学producer。"""
from contextlib import contextmanager
from types import SimpleNamespace as NS
from unittest.mock import Mock
import json
import subprocess
import sys
from pathlib import Path
import pytest
from data_pipeline.jobs import recheck_results as m


def test_disabled_execute_stops_before_materials(monkeypatch):
    preflight=Mock(side_effect=AssertionError('不得读取/连接'))
    monkeypatch.setattr(m,'preflight',preflight)
    with pytest.raises(ValueError,match='禁用'):m.execute({'enabled':False})
    preflight.assert_not_called()


def fixture_owners(monkeypatch,fail=None):
    owners=['m2','reference','detection','canonical','resource','feature']
    values=[dict(owner=o,admission_id=o,owner_binding={'original':o}) for o in owners]
    rt={o:object() for o in owners};calls=[]
    monkeypatch.setattr(m,'construct',lambda *args:rt)
    def current(r,a,guard):
        calls.append(('current',a['owner']))
        if a['owner']==fail:raise ValueError('原资格失效')
    monkeypatch.setattr(m,'api',lambda owner:NS(verify_current=current))
    monkeypatch.setattr(m,'codec',lambda owner:NS(untyped=lambda value:value))
    def admit(module,r,b,output,guard):
        owner=b['original'];calls.append(('admit',owner))
        return dict(owner=owner,admission_id=owner+'-new')
    monkeypatch.setattr(m,'admit_current',admit)
    @contextmanager
    def stage(*args):yield {}
    monkeypatch.setattr(m,'stage',stage)
    return values,rt,calls


def test_reuse_and_new_admission_keep_original_science_binding(tmp_path,monkeypatch):
    values,rt,calls=fixture_owners(monkeypatch)
    pairs=m.qualify({},values,tmp_path,lambda:None)
    assert calls==[('current',o) for o in ['m2','reference','detection']]+[('admit',o) for o in ['canonical','resource','feature']]
    assert [a['admission_id'] for r,a in pairs]==['m2','reference','detection','canonical-new','resource-new','feature-new']
    assert all(r is rt[a['owner']] for r,a in pairs)
    assert values[-1]['owner_binding']=={'original':'feature'}


def test_stale_reused_dependency_stops_before_any_new_admission(tmp_path,monkeypatch):
    values,rt,calls=fixture_owners(monkeypatch,fail='detection')
    with pytest.raises(ValueError,match='失效'):m.qualify({},values,tmp_path,lambda:None)
    assert not any(kind=='admit' for kind,owner in calls)
    assert not (tmp_path/'资格映射-detection.json').exists()


def test_disabled_cli_never_imports_business_or_creates_directories(tmp_path):
    script=Path(__file__).resolve().parents[3]/'scripts/pipeline/migration-requalify.py'
    spec=tmp_path/'spec.json';spec.write_text(json.dumps({'enabled':False}))
    result=subprocess.run([sys.executable,'-B',str(script),str(spec),'--execute'],capture_output=True,text=True,timeout=10)
    assert result.returncode!=0 and '禁用安排不能执行' in result.stderr
    assert list(tmp_path.iterdir())==[spec]


def test_preflight_rejects_old_target_database_without_writes(tmp_path,monkeypatch):
    original=dict(input={'profile':'artificial/v1','source_root':str(tmp_path/'raw')},
        manifest='fixture',mapping='fixture',run_root=str(tmp_path/'old'),
        observation_dsn='host=/private/tmp/fixture port=29568 user=fixture dbname=obs',
        detection={'detection_dsn':'host=/private/tmp/fixture port=29568 user=fixture dbname=det'},
        tail={n:dict(dsn=f'host=/private/tmp/fixture port=29568 user=fixture dbname={n}_old') for n in ('country','trend','reference','publication')})
    values=[dict(owner=o,admission_id=o,dependencies=[]) for o in sorted(m.REUSE|m.READMIT)]
    request=dict(profile={'data_kind':'artificial'},components=values,dependencies=[])
    material={'config':original,'request':request,**{a['owner']:a for a in values}}
    monkeypatch.setattr(m,'material',lambda value:material[value])
    monkeypatch.setattr(m.subprocess,'check_output',lambda args,**kw:'fixed' if args[1]=='rev-parse' else '')
    from data_pipeline.jobs.input_plan import FixedInputs
    monkeypatch.setattr(FixedInputs,'load',lambda *a:None)
    spec=dict(contract='artificial-requalify/v1',execution_code='fixed',source_config='config',source_request='request',
        admissions=[a['owner'] for a in values],tail=original['tail'],attempt_root=str(tmp_path/'new'),
        source_databases={'obs':16384,'det':16385},system_identifier='7685154057349199474',
        new_databases={n:n+'_old' for n in ('country','trend','publication')})
    with pytest.raises(ValueError,match='不得写入原尾段库'):m.preflight(spec)
    assert list(tmp_path.iterdir())==[]


def test_resume_qualifies_all_saved_owners_without_readmission(tmp_path,monkeypatch):
    values,rt,calls=fixture_owners(monkeypatch)
    values.append(dict(owner='country',admission_id='country',owner_binding={}))
    rt['country']=object()
    pairs=m.qualify({'reuse_all_admissions':True},values,tmp_path,lambda:None)
    assert calls==[('current',a['owner']) for a in values]
    assert [a for r,a in pairs]==values


def test_resume_readmits_only_changed_resource(tmp_path,monkeypatch):
    values,rt,calls=fixture_owners(monkeypatch)
    for owner in ('country','trend'):
        values.append(dict(owner=owner,admission_id=owner,owner_binding={}))
        rt[owner]=object()
    pairs=m.qualify({'reuse_all_admissions':True,'readmit_owners':['resource']},values,tmp_path,lambda:None)
    assert calls==[('admit' if a['owner']=='resource' else 'current',a['owner']) for a in values]
    assert [a for r,a in pairs if a['owner']!='resource']==[a for a in values if a['owner']!='resource']
    assert next(a['admission_id'] for r,a in pairs if a['owner']=='resource')=='resource-new'


def test_resume_tail_skips_country_and_context_before_original_trend(tmp_path,monkeypatch):
    from data_pipeline.jobs import downstream as tail, country_events as migration_country, trend_reference as migration_trend_reference
    from data_pipeline.analysis.country_trends import runtime as s3_runtime, stream_store as s3_store
    forbidden=Mock(side_effect=AssertionError('不应重做Country/上下文'))
    monkeypatch.setattr(migration_country,'produce_country',forbidden)
    monkeypatch.setattr(migration_trend_reference,'prepare_context',forbidden)
    monkeypatch.setattr(s3_runtime,'ProductionRuntime',lambda **options:options)
    class ReachedTrend(Exception):pass
    def trend(options,guard):
        assert options['feature_selections']==(('event','ordinary'),)
        assert options['reference_binding']=={'original':'reference'}
        assert [a['owner'] for a in options['dependency_admissions']]==['country','feature']
        raise ReachedTrend()
    monkeypatch.setattr(s3_store,'prepare_trend_m3',trend)
    config=dict(tail=dict(country={},reference={},trend=dict(dsn='fixture',window_us=(1,2),runtime=dict(output_root=str(tmp_path/'trend-output'),scratch_root=str(tmp_path/'scratch'),limits={})),publication=dict(private_root=str(tmp_path/'publication'))))
    pairs=[(object(),dict(owner=o,admission_id=o)) for o in ['country','feature']]
    with pytest.raises(ReachedTrend):tail.run_tail(NS(input_profile='artificial/v1'),config,pairs,tmp_path,lambda:None,resume_context=((('event','ordinary'),),{'binding':{'original':'reference'}}))
    forbidden.assert_not_called()
    assert not (tmp_path/'country').exists() and not (tmp_path/'trend-context').exists()
