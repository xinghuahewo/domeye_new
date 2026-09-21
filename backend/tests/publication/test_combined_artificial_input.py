"""人工profile有限元数据验证；不连接PG、不调用current、不构造科学正例。"""
import hashlib
import json
import os
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import pytest
from data_pipeline.results.fixture_metadata import ArtificialInput
from data_pipeline.results.component_roles import codec
from data_pipeline.results.manifest_contract import PROFILES
from data_pipeline.results.published_reader import validate_mode, schema_sha, SCHEMA_SHA


@pytest.fixture
def original(tmp_path):
    location=os.environ.get('DOMEYE_COMBINED_THREE_RIB_METADATA')
    if not location: pytest.skip('须显式提供原三时点元数据')
    root=Path(location)
    resource=json.loads((root/'ResourceAdmission.json').read_text())
    parents=json.loads((root/'M2参考Admission.json').read_text())
    manifest_path=root/'原manifest.json'
    manifest=json.loads(manifest_path.read_bytes())
    b=codec('resource').untyped(resource['owner_binding'])
    ids=[s['source_id'] for s in manifest['inputs']]
    def row(source, sequence):
        rank=ids.index(source)
        return dict(source_id=source,joint_mrt_rank_zero_based=rank,
                    selection_sequence_zero_based=sequence,joint_role=manifest['inputs'][rank]['role'])
    rows=[]
    for i,s in enumerate(b['binding']['sources']):
        rows.append(row(s['context']['source_id'],i)|dict(nominal_time_utc=s['context']['snapshot_time']))
    calc=[row(s,i) for i,s in enumerate([manifest['baseline_source'],*manifest['update_sources']])]
    mapping=dict(consumers={k:dict(selected_sources=deepcopy(rows if k=='Resource' else calc))
                            for k in ('Resource','Feature','canonical','Detection')})
    mapping_path=tmp_path/'mapping.json';mapping_path.write_text(json.dumps(mapping))
    receipt=json.loads((root/'reference/execution.json').read_bytes())
    input=dict(profile='artificial/v1',description='三时点原AD静态字段检查，映射为有限测试fixture',
               source_root=str(root/'inputs'),manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
               mapping_sha256=hashlib.sha256(mapping_path.read_bytes()).hexdigest(),
               counts=dict(mrt_sources=3,reference_sources=2,resource_sources=3,calculation_sources=1))
    kwargs=dict(manifest=manifest_path,mapping=mapping_path,input=input,
                country_reference=dict(path=receipt['cache_path'],origin_uri=receipt['origin_uri'],content_sha256=receipt['content_sha256']))
    return kwargs,[resource,*parents]


def test_actual_metadata_and_artificial_runtime_shape(original):
    kwargs,ads=original
    fixed=ArtificialInput(**kwargs);declared=fixed.verify(ads)
    runtimes={a['admission_id']:SimpleNamespace(fixture_only=False,execution_profile='real-candidate/v1',
                expected_m2_binding=codec('m2').untyped(a['owner_binding']) if a['owner']=='m2' else None) for a in ads}
    validate_mode(PROFILES['artificial-m3-combined-base/v1'],ads,runtimes,artificial_input=fixed,declared=declared)
    with pytest.raises(ValueError,match='人工来源不得'): validate_mode(PROFILES['real-m3-combined-base/v1'],ads,runtimes)
    runtimes[ads[0]['admission_id']].fixture_only=True
    with pytest.raises(ValueError,match='模式'): validate_mode(PROFILES['artificial-m3-combined-base/v1'],ads,runtimes,artificial_input=fixed,declared=declared)


@pytest.mark.parametrize('change',['sha','root','count','binding','config_mutation','country_path','country_sha'])
def test_reject_input_drift(original,tmp_path,change):
    kwargs,ads=original;kwargs=deepcopy(kwargs);ads=deepcopy(ads)
    if change=='sha':kwargs['input']['manifest_sha256']='0'*64
    if change=='root':kwargs['input']['source_root']=str(tmp_path)
    if change=='count':kwargs['input']['counts']['mrt_sources']=33
    if change=='country_path':kwargs['country_reference']['path']=str(tmp_path/'country.json')
    if change=='country_sha':kwargs['country_reference']['content_sha256']='0'*64
    if change=='binding':
        a=next(a for a in ads if a['owner']=='m2');b=codec('m2').untyped(a['owner_binding'])
        b['plan']['manifest']['collector']='wrong';a['owner_binding']=codec('m2').typed(b)
    with pytest.raises(ValueError):
        fixed=ArtificialInput(**kwargs)
        if change=='config_mutation':fixed.input['counts']['mrt_sources']=33
        fixed.verify(ads)


def test_profile_capabilities_and_old_schema_unchanged():
    for suffix in ('base','country-trend'):
        fixture=PROFILES[f'fixture-m3-combined-{suffix}/v1'];artificial=PROFILES[f'artificial-m3-combined-{suffix}/v1']
        assert fixture['required']==artificial['required']
        assert schema_sha(fixture)==SCHEMA_SHA
        assert schema_sha(artificial)!=SCHEMA_SHA


@pytest.mark.parametrize('change',['mixed_uri','oversize','rank','time'])
def test_reject_metadata_with_matching_file_sha(original,tmp_path,change):
    kwargs,ads=original;kwargs=deepcopy(kwargs)
    key='manifest' if change in ('mixed_uri','oversize') else 'mapping'
    raw=Path(kwargs[key]).read_bytes();value=json.loads(raw)
    if change=='mixed_uri':value['inputs'][0]['origin_uri']='https://example.invalid/real'
    if change=='rank':value['consumers']['Resource']['selected_sources'][0]['joint_mrt_rank_zero_based']=0
    if change=='time':value['consumers']['Resource']['selected_sources'][0]['nominal_time_utc']='1970-01-01T00:00:00+00:00'
    raw=b' '* (8*1024**2+1) if change=='oversize' else json.dumps(value).encode()
    path=tmp_path/'changed.json';path.write_bytes(raw);kwargs[key]=path
    kwargs['input'][key+'_sha256']=hashlib.sha256(raw).hexdigest()
    with pytest.raises(ValueError):ArtificialInput(**kwargs).verify(ads)
