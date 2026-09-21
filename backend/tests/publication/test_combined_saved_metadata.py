"""已接受历史人工元数据原值核对；没有current、PG或新准入。"""
import hashlib
import json
import os
from pathlib import Path
import pytest
from data_pipeline.results.component_readers import Admissions, lock_key
from data_pipeline.results.component_roles import known_role, selection, codec, verify_known_roles

if os.environ.get('DOMEYE_COMBINED_SAVED_METADATA') != '8233':
    pytest.skip('仅读明确授权的8233历史小元数据',allow_module_level=True)

WINDOW = Path('/tmp/domeye-resource-window-integration-8233')
PUBLIC = Path('/tmp/domeye-public-runtime-integration-8233')
FILES = {
    'resource':(WINDOW/'Resource-Admission.json',WINDOW/'Resource-real依赖.json'),
    'detection':(WINDOW/'Detection窗口-Admission.json',WINDOW/'Detection原real依赖.json'),
    'feature':(PUBLIC/'Feature-real-Admission.json',PUBLIC/'Feature-real-dependencies.json'),
    'canonical':(PUBLIC/'Canonical-real-Admission.json',PUBLIC/'Canonical-real-dependencies.json'),
}
REPORT = {'scope':'历史元数据；未核current，不是同P正例','sources':{},'edges':[]}


def load(owner):
    values=[]
    for path in FILES[owner]:
        raw=path.read_bytes(); REPORT['sources'][str(path)]=hashlib.sha256(raw).hexdigest()
        values.append(json.loads(raw))
    return values


@pytest.fixture(scope='module',autouse=True)
def save_report():
    yield
    output=os.environ.get('DOMEYE_COMBINED_METADATA_REPORT')
    if output: Path(output).write_text(json.dumps(REPORT,ensure_ascii=False,indent=2))


def test_original_resource_reference_stage_and_order():
    admission,deps=load('resource'); values=[admission,*deps]
    # 只构造元数据锁计划；object不会充当实际verify_current Runtime。
    graph=Admissions(values,{a['admission_id']:object() for a in values},guard=lambda:None)
    target=next(t for t in admission['lock_targets'] if t['namespace']=='resource.reference')
    assert target['stage']==10 and target in graph.lock_targets
    assert graph.lock_targets==sorted(graph.lock_targets,key=lock_key)
    assert len(graph.lock_targets)==len({lock_key(t) for a in values for t in a['lock_targets']})
    REPORT['resource_reference_lock']=target


@pytest.mark.parametrize('owner',FILES)
def test_original_binding_roles(owner):
    admission,deps=load(owner); by_id={d['admission_id']:d for d in deps}
    for consumer in [admission,*deps]:
        cc=codec(consumer['owner']); cb=cc.untyped(consumer['owner_binding'])
        if consumer['owner']=='m2':
            assert consumer['dependencies']==[]
            assert selection(cb,cb['ordered_source_ids'])==cb['selected_sources']
        for dep_id in consumer['dependencies']:
            dep=by_id[dep_id]; dc=codec(dep['owner']); db=dc.untyped(dep['owner_binding'])
            role=known_role(consumer['owner'],consumer,dep_id,dep)
            assert role['input_binding_typed']==dep['owner_binding']
            assert role['codec_version']==dep['binding_codec']
            window=dc.untyped(role['window_typed'])
            if dep['owner']=='reference':
                assert role['selected_sources']==[] and role['window_role']=='reference'
                assert dc.untyped(role['reference_role']) is not None
            else:
                raw=json.loads(db['input_binding'])
                for item in role['selected_sources']:
                    source=raw['sources'][item['upstream_rank']]
                    assert (item['source_id'],item['calculation_role'])==(source['source_id'],source['role'])
            REPORT['edges'].append(dict(group=owner,consumer=consumer['owner'],consumer_admission=consumer['admission_id'],
                dependency=dep_id,dependency_owner=dep['owner'],role=role['role'],selected_sources=role['selected_sources'],
                binding_fields=sorted(cb),dependency_binding_fields=sorted(db),window_fields=sorted(window),
                role_sha256=hashlib.sha256(json.dumps(role,sort_keys=True,ensure_ascii=False).encode()).hexdigest()))


def test_unrelated_actual_windows_cannot_form_one_publication():
    components=[]; dependencies={}; roles=[]
    for owner in FILES:
        admission,deps=load(owner)
        if owner=='canonical':dependencies['canonical:'+admission['admission_id']]=dict(dependency_key='canonical:'+admission['admission_id'],owner=owner,admission=admission)
        else:components.append(admission)
        nodes={d['admission_id']:d for d in deps}
        for dep in deps:
            key=dep['owner']+':'+dep['admission_id'];dependencies[key]=dict(dependency_key=key,owner=dep['owner'],admission=dep)
        for consumer in [admission,*deps]:
            key=consumer['owner'] if consumer['owner'] in ('resource','feature','detection') else consumer['owner']+':'+consumer['admission_id']
            for aid in consumer['dependencies']:
                dep=nodes[aid];roles.append(known_role(key,consumer,dep['owner']+':'+aid,dep))
    with pytest.raises(ValueError,match='结果窗口不相容') as error:
        verify_known_roles(dict(components=components,dependencies=list(dependencies.values()),role_graph=roles))
    REPORT['cross_window_rejection']=str(error.value)
