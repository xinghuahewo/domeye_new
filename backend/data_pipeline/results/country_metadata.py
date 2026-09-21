"""Country正式C3原元数据角色接合；不读取科学正文、不签发资格。"""
from pathlib import Path
import hashlib
import json
from data_pipeline.results.manifest_io import require
from data_pipeline.results.component_roles import codec, selection
from data_pipeline.analysis.country_events.selection_contract import contract_value
from data_pipeline.analysis.country_events.snapshot_schema import decode
from data_pipeline.analysis.country_events.admission_sources import same_admission_value


def metadata(admission):
    value=codec('country').untyped(admission['owner_binding'])
    read=contract_value(value['read_binding']);proof=contract_value(value['proof'])
    require(proof.component_manifest_sha256==read.component.manifest_sha256
            and proof.result_id==read.result_id and proof.read_model_id==read.read_model_id, 'Country原C3/C4证明串绑')
    path=Path(read.component.root)/'manifest.json'
    entities=[e for e in admission['entities'] if e['path']==str(path)]
    require(len(entities)==1 and entities[0]['sha256']==read.component.manifest_sha256, 'Country原manifest实体缺失')
    with path.open('rb') as handle: raw=handle.read(8*1024**2+1)
    require(len(raw)<=8*1024**2 and hashlib.sha256(raw).hexdigest()==read.component.manifest_sha256, 'Country原manifest大小或SHA不符')
    manifest=json.loads(raw);upstream=decode(manifest['upstream']);params=decode(manifest['parameters'])
    require(upstream['profile']=='country-m3-source/v1', 'Country非正式M3来源')
    deps=upstream['admissions']
    require(sorted(a['admission_id'] for a in deps)==admission['dependencies'], 'Country原完整依赖集合不符')
    owners={owner:[a for a in deps if a['owner']==owner] for owner in ('m2','reference','canonical','detection')}
    require(sum(map(len,owners.values()))==len(deps) and all(len(owners[k])==1 for k in ('m2','canonical','detection')), 'Country原四类依赖不符')
    mb=codec('m2').untyped(owners['m2'][0]['owner_binding'])
    require(upstream['input_binding_id']==mb['input_binding_id'], 'Country原InputBinding不符')
    mid=owners['m2'][0]['admission_id']
    refs=owners['reference'];expected={mid,*[a['admission_id'] for a in refs]}
    for owner in ('canonical','detection'):
        require(set(owners[owner][0]['dependencies'])==expected, 'Country C/D原依赖闭合不符')
    actual_refs=[]
    for a in refs:
        rb=codec('reference').untyped(a['owner_binding'])
        require(rb['m2_admission_id']==mid and rb['m2_binding']==mb, 'Country参考原M2串绑')
        actual_refs.append(rb['source_id'])
    require(len(actual_refs)==len(set(actual_refs)) and set(actual_refs)=={r['source_id'] for r in mb['reference_sources']}, 'Country原参考集合缺失')
    cb=codec('canonical').untyped(owners['canonical'][0]['owner_binding'])
    db=codec('detection').untyped(owners['detection'][0]['owner_binding'])
    require(cb['descriptor']['plan']['input_binding']==json.loads(mb['input_binding'])==db['identity']['input_binding']
            and cb['descriptor']['plan']['selected_sources']==mb['ordered_source_ids']==db['identity']['selected_sources'], 'Country原全输入或消费序不符')
    from data_pipeline.analysis.detection.result_window import identity_windows, instant
    windows=identity_windows(db['identity'],db['scope'])
    require(upstream['windows']==windows, 'Country原Detection窗口不符')
    result=windows['result_window']
    if type(result) is dict: result=[result['window_start'],result['window_end_exclusive']]
    from datetime import datetime,timezone
    epoch=datetime(1970,1,1,tzinfo=timezone.utc)
    def micros(t):
        delta=instant(t)-epoch
        return (delta.days*86400+delta.seconds)*1000000+delta.microseconds
    require(tuple(params['result_window_us'])==tuple(micros(t) for t in result), 'Country结果窗与Detection不符')
    return upstream,params,mb


def role(consumer_key, consumer, dependency_key, dependency):
    upstream,params,mb=metadata(consumer)
    matches=[a for a in upstream['admissions'] if a['admission_id']==dependency['admission_id']]
    require(len(matches)==1 and same_admission_value(matches[0],dependency), 'Country原依赖Admission值不符')
    owner=dependency['owner'];dc=codec(owner);reference=owner=='reference'
    selected=[] if reference else selection(mb,mb['ordered_source_ids'])
    return dict(consumer_key=consumer_key,dependency_key=dependency_key,role='country:'+owner,
        collector=mb['plan']['manifest']['collector'],input_binding_typed=dependency['owner_binding'],
        codec_version=dependency['binding_codec'],selected_sources=selected,
        window_role='reference' if reference else 'result',
        window_typed=dc.typed(dict(windows=upstream['windows'],parameters=params)),
        reference_role=dc.typed(dc.untyped(dependency['owner_binding'])) if reference else None)


def window(admission):
    return tuple(metadata(admission)[1]['result_window_us'])
