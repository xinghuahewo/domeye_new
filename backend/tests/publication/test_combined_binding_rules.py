"""纯字段规则，不创建Admission、不连接PG或执行科学计算。"""
import json
from copy import deepcopy
from types import SimpleNamespace
import pytest
from data_pipeline.results.component_roles import selection
from data_pipeline.results.manifest_contract import PROFILES, CONTRACT, validate_structure
from data_pipeline.results.published_reader import validate_mode


def test_resource_rib_time_order_keeps_nonmonotonic_global_ranks():
    from data_pipeline.results.component_roles import known_role, codec
    # 有限原字段形态fixture；不是实际Admission或12时点运行证据。
    sources=[dict(source_id='baseline',role='baseline'),dict(source_id='earlier',role='snapshot'),dict(source_id='update',role='update')]
    selected=[dict(source_id=s['source_id'],upstream_rank=i,calculation_role=s['role']) for i,s in enumerate(sources)]
    mb=dict(input_binding=json.dumps(dict(sources=sources)),input_binding_id='one-full-input',
            ordered_source_ids=[s['source_id'] for s in sources],selected_sources=selected,
            plan=dict(manifest=dict(collector='rrc25')))
    binding=dict(sources=[dict(context=dict(source_id='earlier',snapshot_time='1970-01-01T00:00:00+00:00')),
                          dict(context=dict(source_id='baseline',snapshot_time='1970-01-01T01:00:00+00:00'))],
                 observation_inputs={s:dict(binding_id='one-full-input') for s in ('earlier','baseline','update')},
                 result_window=['1970-01-01T01:00:00+00:00','1970-01-01T02:00:00+00:00'])
    parent=dict(owner='m2',owner_binding=codec('m2').typed(mb),binding_codec='m2-publication-typed/v1')
    def role(value):
        return known_role('resource',dict(owner='resource',owner_binding=codec('resource').typed(dict(binding=value))), 'm2-key',parent)
    actual=role(binding)
    assert [s['upstream_rank'] for s in actual['selected_sources']]==[1,0]
    assert actual['input_binding_typed']==parent['owner_binding']
    with pytest.raises(ValueError,match='原序漂移'): selection(mb,['earlier','baseline'])
    duplicate=deepcopy(binding);duplicate['sources'].append(duplicate['sources'][0])
    with pytest.raises(ValueError,match='重复'):role(duplicate)
    update=deepcopy(binding);update['sources'][0]['context']['source_id']='update'
    with pytest.raises(ValueError,match='RIB'):role(update)


def test_global_rank_preserved_for_sparse_rib_subset():
    # 原sources含未被本消费方选用的中间来源；CP ordinal不在此域。
    sources = [dict(source_id='rib1',role='baseline'),dict(source_id='update',role='update'),dict(source_id='rib2',role='snapshot')]
    selected = [dict(source_id=s['source_id'],upstream_rank=i,calculation_role=s['role']) for i,s in enumerate(sources)]
    b = dict(input_binding=json.dumps(dict(sources=sources)),ordered_source_ids=['rib1','rib2'],selected_sources=[selected[0],selected[2]])
    assert selection(b,['rib2']) == [selected[2]]
    bad = deepcopy(b); bad['selected_sources'][1]['upstream_rank'] = 1
    with pytest.raises(ValueError,match='global rank'): selection(bad,['rib2'])
    with pytest.raises(ValueError,match='原序漂移'): selection(b,['rib2','rib1'])


def test_declared_revision_must_match_actual_field_before_other_validation():
    # 不构造可准入对象；此字段级负例在任何owner/数据库访问之前失败。
    owners = sorted(('resource','feature','detection','country'))
    q = dict(contract=CONTRACT,profile=deepcopy(PROFILES['fixture-m3-combined-base/v1']),
             dependency_revisions={k:'a'*40 for k in ('canonical','resource','feature','detection','country','trend')},
             components=[dict(owner=k,admission_id=k,owner_revision='b'*40) for k in owners],
             dependencies=[],edges=[],role_graph=[])
    with pytest.raises(ValueError,match='owner_revision'): validate_structure(q)


@pytest.mark.parametrize('profile,fixture,execution',[
    ('real-m3-combined-base/v1',True,None),
    ('fixture-m3-combined-base/v1',False,'real-candidate/v1'),
    ('real-m3-combined-base/v1',False,None),
])
def test_profile_rename_cannot_bypass_runtime_mode(profile,fixture,execution):
    with pytest.raises(ValueError,match='模式'):
        validate_mode(PROFILES[profile],[dict(admission_id='not-an-admission',owner='resource')],
                      {'not-an-admission':SimpleNamespace(fixture_only=fixture,execution_profile=execution)})
