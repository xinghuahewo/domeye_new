"""人工参考路径/来源合同fixture；不构造AD或科学成功回执。"""
import hashlib
import json
import pytest
from data_pipeline.results.fixture_metadata import ArtificialInput


@pytest.mark.parametrize('fault',[None,'source_bytes','country','binding','overlap','root_replaced'])
def test_derived_reference_outside_frozen_raw_root(tmp_path,fault):
    raw=tmp_path/'raw';raw.mkdir();derived=tmp_path/'derived';derived.mkdir()
    source=raw/'independent.json';source.write_text(json.dumps({'origin_uri':'fixture://independent'}))
    binding=dict(path=str(derived/'reference.json'),sha256='1'*64,origin_uri='fixture://derived')
    contract=dict(source_path=str(source),source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                  derived_root=str(derived),country_admission_id='country-original',binding=binding)
    args=dict(manifest=source,mapping=source,input=dict(profile='artificial/v1',description='仅路径合同fixture',
              source_root=str(raw),manifest_sha256='0'*64,mapping_sha256='0'*64,
              counts=dict(mrt_sources=1,reference_sources=1,resource_sources=1,calculation_sources=1)),country_reference={})
    if fault=='overlap':contract['derived_root']=str(raw)
    fixed=ArtificialInput(**args,trend_reference=contract)
    country='country-original'
    if fault=='source_bytes':source.write_text('{}')
    if fault=='country':country='changed-country'
    if fault=='binding':binding={**binding,'sha256':'2'*64}
    if fault=='root_replaced':derived.rename(tmp_path/'old-derived');derived.mkdir()
    if fault is None:
        result=fixed.verify_derived_reference(binding,country)
        assert result['binding']==binding and result['source_sha256']==contract['source_sha256']
        assert result['source_origin_uri']=='fixture://independent'
    else:
        with pytest.raises(ValueError):fixed.verify_derived_reference(binding,country)


def test_schema_extension_keeps_original_artificial_identity():
    from data_pipeline.results.published_reader import schema_sha, ARTIFICIAL_SCHEMA, digest
    from data_pipeline.results.manifest_contract import PROFILES
    profile=PROFILES['artificial-m3-combined-country-trend/v1']
    assert schema_sha(profile)==digest(ARTIFICIAL_SCHEMA)
    assert schema_sha(profile,derived_reference=True)!=schema_sha(profile)
