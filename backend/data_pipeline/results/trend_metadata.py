"""S3原完整输入角色接合；自有参考不伪装成M2参考Admission。"""
from dataclasses import fields
import hashlib
from data_pipeline.results.manifest_io import require
from data_pipeline.results.component_roles import codec, selection
from data_pipeline.results.country_metadata import metadata as country_metadata
from data_pipeline.analysis.country_trends import stream_schema as schema
from data_pipeline.analysis.country_trends.stream_store import Binding
from data_pipeline.analysis.country_trends.snapshot_inputs import REFERENCE_PROFILE
from data_pipeline.analysis.country_events.admission_sources import same_admission_value


def metadata(admission):
    require(admission['binding_codec']==schema.CODEC, 'Trend必须使用原S3 codec')
    value=schema.decode(admission['owner_binding'])
    require(set(value)=={'binding','proof'}, 'Trend原完整Binding缺失')
    binding=value['binding']
    require(set(binding)=={f.name for f in fields(Binding)}
            and binding['schema_version']==schema.VERSION and binding['profile']==schema.PROFILE, 'Trend不能使用旧S2绑定')
    text=value['proof']['inputs_typed']
    require(type(text) is str and len(text.encode())<=8*1024**2, 'Trend原输入元数据超限')
    inputs=schema.decode(text)
    require(binding['result_id']=='trend_m3_'+hashlib.sha256(schema.encode(inputs).encode()).hexdigest()
            and inputs['root']==binding['root']
            and inputs['identity']=={k:binding[k] for k in ('system_id','database_oid','catalog_id')}, 'Trend原结果/物理身份不符')
    deps=inputs['dependencies']
    require(sorted(a['admission_id'] for a in deps)==admission['dependencies']
            and len({a['admission_id'] for a in deps})==len(deps), 'Trend原依赖集合不符')
    require(sum(a['owner']=='country' for a in deps)==1 and sum(a['owner']=='feature' for a in deps)<=1
            and all(a['owner'] in ('country','feature') for a in deps), 'Trend依赖只能为Country与可选Feature')
    window=inputs['window_us']
    require(type(window) is tuple and len(window)==2 and all(type(t) is int for t in window) and window[0]<window[1], 'Trend原结果微秒窗不符')
    selections=inputs['feature_selections']
    require(type(selections) is tuple and len(set(selections))==len(selections), 'Trend Feature选择重复或类型错误')
    require(not selections or any(a['owner']=='feature' for a in deps), 'Trend Feature选择缺原依赖')
    for event,mode in selections:
        require(type(event) is tuple and len(event)==2 and type(event[0]) is str and bool(event[0])
                and type(event[1]) is int and event[1]>0 and mode in ('ordinary','ir'), 'Trend Feature原事件/模式不符')
    reference=inputs['reference_binding']
    if reference is not None:
        require(reference['profile']==REFERENCE_PROFILE and reference['historical_applicability']=='unknown'
                and (reference['system_id'],reference['database_oid'])==(binding['system_id'],binding['database_oid']), 'Trend自有参考绑定不符')
        require(any(e['path']==reference['path'] and e['sha256']==reference['sha256'] for e in admission['entities']), 'Trend自有参考原实体缺失')
        require(any(t['namespace']=='trend.reference' and t['stage']==60 and t['key']==reference['sha256'] for t in admission['lock_targets']), 'Trend自有参考锁缺失')
    else:require(not any(t['namespace']=='trend.reference' for t in admission['lock_targets']), 'Trend未绑定参考却声明参考锁')
    return inputs


def role(consumer_key,consumer,dependency_key,dependency):
    inputs=metadata(consumer)
    parents=[a for a in inputs['dependencies'] if a['admission_id']==dependency['admission_id']]
    require(len(parents)==1 and same_admission_value(parents[0],dependency), 'Trend原完整依赖Admission值不符')
    country=next(a for a in inputs['dependencies'] if a['owner']=='country')
    upstream,params,mb=country_metadata(country)
    require(inputs['window_us']==tuple(params['result_window_us']), 'Trend/Country原结果窗不符')
    ids=mb['ordered_source_ids'];windows=dict(country_window_us=inputs['window_us'],
        country_windows=upstream['windows'],feature_selections=inputs['feature_selections'],
        trend_reference_binding=inputs['reference_binding'])
    if dependency['owner']=='feature':
        spec=codec('feature').untyped(dependency['owner_binding'])['specification']
        ids=[s['source_id'] for s in spec['source_bindings']]
        windows['feature_specification']=spec
    dc=codec(dependency['owner'])
    return dict(consumer_key=consumer_key,dependency_key=dependency_key,role='trend:'+dependency['owner'],
        collector=mb['plan']['manifest']['collector'],input_binding_typed=dependency['owner_binding'],
        codec_version=dependency['binding_codec'],selected_sources=selection(mb,ids),
        window_role='result',window_typed=dc.typed(windows),reference_role=None)
