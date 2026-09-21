"""driver生产父根到准入组件根：真实Runtime/严格_parts，PG前停止，不造成功AD。"""
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace as NS
import hashlib
import json

import pytest

from data_pipeline.jobs import downstream as tail
from data_pipeline.analysis.country_trends import runtime as s3_runtime, stream_store as s3_store, stream_schema as s3_schema
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
from data_pipeline.analysis.country_events import snapshot_schema as component_schema
from data_pipeline.analysis.country_events.selection_contract import CountryAdmissionProof, contract_json


class BeforePG(RuntimeError):
    pass


def options_and_binding(tmp_path):
    root = tmp_path.resolve()
    for name in ('trend', 'scratch'): (root/name).mkdir()
    deps = ({'owner': 'country', 'admission_id': 'fixture-country-not-ad'},
            {'owner': 'feature', 'admission_id': 'fixture-feature-not-ad'})
    options = dict(dsn=f'host={root} dbname=fixture_not_connected', output_root=str(root/'trend'),
                   scratch_root=str(root/'scratch'), allowed_roots=(root,),
                   dependency_admissions=deps, dependency_runtimes={a['admission_id']: object() for a in deps},
                   country_window_us=(100, 200), execution_profile='real-candidate/v1',
                   limits=S2Limits(), memory_bytes=256*1024**2, max_temp_bytes=512*1024**2,
                   min_free_bytes=128*1024**2, lock_timeout_ms=2000,
                   reference_binding={'fixture': 'reference'}, feature_selections=({'event': ('fixture', 2)},))
    return options


def produced_binding(runtime):
    component = 'a'*32
    root = runtime.output_root/component
    root.mkdir()
    identity = dict(system_id='123456', database_oid=123, catalog_id='b'*32)
    inputs = dict(root=str(root), identity=identity, execution_profile=runtime._profile,
                  window_us=runtime.country_window_us, dependencies=runtime.dependency_admissions,
                  reference_binding=runtime.reference_binding, feature_selections=runtime.feature_selections)
    binding = dict(**identity, component_id=component,
                   result_id='trend_m3_'+hashlib.sha256(s3_schema.encode(inputs).encode()).hexdigest(),
                   schema_name='trend_m3_'+component, snapshot=1, root=str(root), manifest_sha256='c'*64,
                   schema_version=s3_schema.VERSION, profile=s3_schema.PROFILE)
    return dict(binding=binding, proof={'inputs_typed': s3_schema.encode(inputs)})


def test_driver_keeps_production_parent_and_passes_component_to_real_admission(tmp_path, monkeypatch):
    from data_pipeline.jobs import country_events as migration_country, trend_reference as migration_trend_reference
    from data_pipeline.bgp.archive import admission as publication
    root = tmp_path.resolve()
    # run_tail自己创建这些目录；使用原真实Runtime，数据生产边界仅返回fixture绑定。
    config = dict(tail=dict(
        country=dict(c2_scratch_root=str(root/'c2'), capture={'scratch_root': str(root/'capture')},
                     runtime={'scratch_root': str(root/'country-scratch')}),
        trend=dict(dsn=f'host={root} dbname=fixture_not_connected', window_us=(100, 200),
                   runtime=dict(output_root=str(root/'trend'), scratch_root=str(root/'scratch'),
                       allowed_roots=(root,), limits=asdict(S2Limits()), memory_bytes=256*1024**2,
                       max_temp_bytes=512*1024**2, min_free_bytes=128*1024**2, lock_timeout_ms=2000)),
        reference={'derived_root': str(root/'derived')}, publication={'private_root': str(root/'publication')}))
    original = deepcopy(config)
    country_runtime, feature_runtime = object(), object()
    proof = CountryAdmissionProof('fixture', 'fixture', '0'*64, 'fixture', '0'*64, 0, 0, 0, 0, 0)
    country = dict(owner='country', admission_id='fixture-country-not-ad',
                   owner_binding=component_schema.encode({'proof': contract_json(proof)}))
    feature = dict(owner='feature', admission_id='fixture-feature-not-ad')
    pairs = [(object(), dict(owner='detection', admission_id='fixture-detection-not-ad', dependencies=[])), (feature_runtime, feature)]
    selections = ({'event': ('fixture', 2)},)
    reference = {'binding': {'fixture': 'reference'}}
    monkeypatch.setattr(migration_country, 'produce_country', lambda *a, **k: (country_runtime, country))
    monkeypatch.setattr(migration_trend_reference, 'prepare_context', lambda *a, **k: (selections, reference))
    captured = {}
    def produce(runtime, *, guard):
        assert type(runtime) is s3_runtime.ProductionRuntime
        assert runtime.output_root == root/'trend'
        captured['produced'] = produced_binding(runtime)
        return captured['produced']
    monkeypatch.setattr(s3_store, 'prepare_trend_m3', produce)
    def before_pg(runtime, *a, **k):
        captured['reader'] = runtime
        raise BeforePG('fixture stops before external PG, no AD')
    # 本测试停在PG前；新解释器绑定顺序由CLI fixture单独验证。
    from data_pipeline.common import process_resources as s3_process_resources
    monkeypatch.setattr(s3_process_resources, 'sqlite_temp', lambda: root/'scratch')
    monkeypatch.setattr(publication, '_pg', before_pg)
    with pytest.raises(BeforePG):
        tail.run_tail(NS(input_profile='artificial/v1'), config, pairs, root/'invocation', lambda: None)
    runtime = captured['reader']
    assert type(runtime) is s3_runtime.Runtime
    assert runtime.output_root == root/'trend'/('a'*32)
    assert runtime.expected_trend_binding == captured['produced']
    assert runtime.dependency_admissions == (country, feature)
    assert runtime.dependency_runtimes == {country['admission_id']: country_runtime, feature['admission_id']: feature_runtime}
    assert runtime.country_window_us == (100, 200)
    assert runtime.reference_binding == reference['binding'] and runtime.feature_selections == selections
    assert config == original
    assert json.loads((root/'invocation/trend/原Trend返回.json').read_text()) == captured['produced']
    assert not (root/'invocation/trend-admit/admission.json').exists()


@pytest.mark.parametrize('root_kind', ['parent', 'sibling'])
def test_real_runtime_still_rejects_other_roots(tmp_path, root_kind):
    options = options_and_binding(tmp_path)
    produced = produced_binding(s3_runtime.ProductionRuntime(**options))
    wrong = Path(options['output_root'])
    if root_kind == 'sibling':
        wrong = wrong/('d'*32)
        wrong.mkdir()
    with pytest.raises(ValueError, match='^trend_p1_binding_scope$'):
        s3_runtime.Runtime(**dict(options, output_root=wrong), expected_trend_binding=produced)


@pytest.mark.parametrize('key,value', [
    ('country_window_us', (101, 200)),
    ('reference_binding', {'fixture': 'different-reference'}),
    ('feature_selections', ({'event': ('other-event', 3)},)),
    ('dependency_admissions', ({'owner': 'country', 'admission_id': 'different-not-ad'},)),
])
def test_component_root_does_not_bypass_complete_input_contract(tmp_path, key, value):
    options = options_and_binding(tmp_path)
    produced = produced_binding(s3_runtime.ProductionRuntime(**options))
    reader_options = dict(options, output_root=produced['binding']['root'])
    reader_options[key] = value
    with pytest.raises(ValueError, match='^trend_p1_complete_input_binding$'):
        s3_runtime.Runtime(**reader_options, expected_trend_binding=produced)


@pytest.mark.parametrize('key,value,reason', [
    ('component_id', 'bad-id', 'trend_p1_binding_scope'),
    ('manifest_sha256', 'bad-hash', 'trend_p1_binding_scope'),
    ('result_id', 'trend_m3_'+'0'*64, 'trend_p1_result_identity'),
    ('system_id', '999999', 'trend_p1_complete_input_binding'),
])
def test_component_root_does_not_bypass_original_binding_identity(tmp_path, key, value, reason):
    options = options_and_binding(tmp_path)
    produced = produced_binding(s3_runtime.ProductionRuntime(**options))
    produced['binding'][key] = value
    with pytest.raises(ValueError, match='^'+reason+'$'):
        s3_runtime.Runtime(**dict(options, output_root=produced['binding']['root']),
                           expected_trend_binding=produced)
