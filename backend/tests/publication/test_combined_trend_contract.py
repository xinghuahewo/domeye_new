"""S3请求与拒绝合同；没有实际Trend AD则不构造角色成功正例。"""
from dataclasses import asdict
import hashlib
import pytest
from data_pipeline.results.component_readers import api, LOCK_OWNERS
from data_pipeline.results.component_streams import requests
from data_pipeline.results.trend_metadata import metadata
from data_pipeline.analysis.country_trends import stream_schema as schema
from data_pipeline.analysis.country_trends.stream_store import Binding
from data_pipeline.analysis.country_trends.result_admission import request_scope
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits


def test_all_s3_views_and_independent_availability_request():
    plan=requests('trend',batch_rows=1,batch_bytes=4096)
    assert len(plan)==54 and tuple(q['view'] for q in plan)==schema.TABLES
    assert sum(q['view']=='result_availability' for q in plan)==1
    for q in plan:
        assert q['codec_version']=='country-trend-typed/v2'
        assert request_scope(q,S2Limits())==dict(event=None,key_typed=None,after_sequence=-1,stop_sequence=None)
    assert all(LOCK_OWNERS[k]==('trend',60) for k in ('trend.component','trend.admission','trend.reference'))
    assert api('trend').__name__.endswith('result_admission')


def test_old_s2_codec_not_accepted():
    with pytest.raises(ValueError,match='S3 codec'):metadata(dict(binding_codec='country-trend-typed/v1'))


@pytest.mark.parametrize('fault',['no_country','feature_without_dependency','resource_reference','missing_reference_entity','old_schema'])
def test_incomplete_or_wrong_input_rejected(fault):
    # 拒绝专用字段fixture，无Admission摘要、可信锚或成功状态。
    inputs=dict(root='/fixture',identity=dict(system_id='1',database_oid=1,catalog_id='c'),
                window_us=(0,10),dependencies=() if fault=='no_country' else (dict(owner='country',admission_id='country'),),
                feature_selections=((("event",1),'ordinary'),) if fault=='feature_without_dependency' else (),reference_binding=None)
    if fault in ('resource_reference','missing_reference_entity'):
        inputs['reference_binding']=dict(profile='resource-reference/v2' if fault=='resource_reference' else 'country-trend-reference-artificial/v1',
            historical_applicability='unknown',system_id='1',database_oid=1,path='/fixture/reference',sha256='0'*64)
    binding=asdict(Binding(system_id='1',database_oid=1,catalog_id='c',component_id='x',result_id='trend_m3_'+hashlib.sha256(schema.encode(inputs).encode()).hexdigest(),
            schema_name='s',snapshot=1,root='/fixture',manifest_sha256='0'*64))
    if fault=='old_schema':binding['schema_version']='country-trend-store/v1'
    fixture=dict(binding_codec=schema.CODEC,owner_binding=schema.encode(dict(binding=binding,proof=dict(inputs_typed=schema.encode(inputs)))),
                 dependencies=sorted(a['admission_id'] for a in inputs['dependencies']),entities=[],lock_targets=[])
    with pytest.raises(ValueError):metadata(fixture)
