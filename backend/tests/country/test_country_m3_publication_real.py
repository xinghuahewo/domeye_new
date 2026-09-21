"""纯Runtime两模式边界；本文件不创建或伪造任何实际Admission。"""
from dataclasses import replace
from types import SimpleNamespace
import pytest
from data_pipeline.analysis.country_events.result_admission import Runtime
from data_pipeline.analysis.country_events.qualified_reader import ResultLimits
from data_pipeline.analysis.country_events.snapshot_store import ComponentBinding
from data_pipeline.analysis.country_events.selection_contract import CountryReadBinding, CountryAdmissionProof, contract_json


def params(tmp_path):
    for name in ('output','component','scratch'):(tmp_path/name).mkdir()
    c=ComponentBinding('1',1,'catalog','c'*32,'country_'+'c'*32,1,'country-component-m3/v1','a'*64,str(tmp_path/'component'))
    r=CountryReadBinding(c,'result','read',str(tmp_path/'output'),'b'*64,None,'country-query-m3/v1')
    p=CountryAdmissionProof('result','read','a'*64,'source-receipt', 'd'*64,0,0,1,1,0)
    b=dict(read_binding=contract_json(r),proof=contract_json(p))
    return dict(dsn=f'host={tmp_path} dbname=fixture',output_root=tmp_path/'output',allowed_roots=(tmp_path,),
                scratch_root=tmp_path/'scratch',dependency_admissions=(),dependency_runtimes={},
                execution_profile='real-candidate/v1',expected_country_binding=b,limits=ResultLimits(),
                max_source_rows=1000,max_source_bytes=1024**2,lock_timeout_ms=100,
                min_free_bytes=1,max_temp_bytes=1024**2,memory_bytes=64*1024**2)


def test_real_mode_requires_every_explicit_resource(tmp_path):
    args=params(tmp_path)
    for name in ('limits','max_source_rows','max_source_bytes','lock_timeout_ms','min_free_bytes','max_temp_bytes','memory_bytes'):
        with pytest.raises(ValueError,match='显式资源'):
            Runtime(**dict(args,**{name:None}))
    runtime=Runtime(**args)
    runtime.check_binding(args['expected_country_binding'])
    with pytest.raises(ValueError,match='绑定越界'):
        runtime.check_binding(dict(args['expected_country_binding'],proof='different'))


@pytest.mark.parametrize('field,value',[('dsn','host=/invalid dbname=fixture'),('max_source_rows',999),
    ('execution_profile','synthetic-fixture/v1'),('fixture_only',True)])
def test_runtime_drift_rejected_before_io(tmp_path,field,value):
    runtime=Runtime(**params(tmp_path));setattr(runtime,field,value)
    with pytest.raises(ValueError,match='漂移'):runtime.resource_guard()


def test_scratch_and_modes_cannot_mix(tmp_path):
    args=params(tmp_path)
    with pytest.raises(ValueError,match='混用'):Runtime(**dict(args,fixture_only=True))
    with pytest.raises(ValueError,match='未隔离'):Runtime(**dict(args,scratch_root=args['output_root']))
    runtime=Runtime(**args)
    with pytest.raises(ValueError,match='未隔离'):runtime.separate_scratch(runtime.scratch_root)
    runtime.scratch_root.rename(tmp_path/'old-scratch');(tmp_path/'scratch').mkdir()
    with pytest.raises(ValueError,match='目录身份'):runtime.resource_guard()


def test_cross_mode_dependency_is_rejected_without_admission_verifier(tmp_path):
    args=params(tmp_path)
    # 仅在owner Admission核验之前测试模式拒绝，标签不是合格Admission。
    args.update(dependency_admissions=({'admission_id':'invalid-test-label'},),
                dependency_runtimes={'invalid-test-label':SimpleNamespace(fixture_only=True,execution_profile=None)})
    runtime=Runtime(**args)
    with pytest.raises(ValueError,match='依赖模式'):runtime.check_dependencies()


def test_single_target_lock_rejects_other_mode_before_lock_connection(tmp_path,monkeypatch):
    from data_pipeline.analysis.country_events import result_admission as p
    args=params(tmp_path);real=p.Runtime(**args)
    fixture=p.Runtime(dsn=args['dsn'],output_root=args['output_root'],allowed_roots=args['allowed_roots'],scratch_root=args['scratch_root'],dependency_admissions=(),dependency_runtimes={},fixture_only=True,min_free_bytes=1)
    # 只隔离规则分支；无实际Admission、锚或PG接合成功声明。
    admission={'validator':dict(p._rules(real),validation_digest='fixture')}
    monkeypatch.setattr(p,'_shape',lambda _:None);monkeypatch.setattr(p,'_anchor',lambda *_:{})
    monkeypatch.setattr(p.upstream.psycopg2,'connect',lambda *_:pytest.fail('模式拒绝前不得连接锁会话'))
    with pytest.raises(ValueError,match='模式'):
        with p.hold_lock(fixture,admission,{},guard=lambda:None):pytest.fail('不得进入锁体')
