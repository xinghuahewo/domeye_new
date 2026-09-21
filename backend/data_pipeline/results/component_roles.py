"""已固定owner的原Binding到有限角色；不读库、不重新解释科学行。"""
import json
from data_pipeline.results.manifest_io import require


def codec(owner):
    if owner == 'trend':
        from types import SimpleNamespace
        from data_pipeline.analysis.country_trends.stream_schema import encode, decode
        return SimpleNamespace(typed=encode,untyped=decode)
    if owner == 'country':
        from types import SimpleNamespace
        from data_pipeline.analysis.country_events.snapshot_schema import encode, decode
        return SimpleNamespace(typed=encode,untyped=decode)
    if owner == 'canonical':
        from types import SimpleNamespace
        from data_pipeline.bgp.replay.snapshot_contract import encode
        from data_pipeline.bgp.replay.snapshot_access import untyped
        return SimpleNamespace(typed=encode,untyped=untyped)
    if owner in ('m2','reference'):
        from data_pipeline.bgp.archive import value_codec as value
    elif owner == 'feature':
        from data_pipeline.bgp.archive import value_codec as value
    elif owner == 'resource':
        from data_pipeline.analysis.resources import publication_codec as value
    elif owner == 'detection':
        from data_pipeline.analysis.detection import publication_codec as value
    else:
        raise ValueError('角色绑定尚缺正式owner codec：'+owner)
    return value


def selection(m2, source_ids):
    """使用原ordered完整sources的global rank，不能用CP ordinal或局部枚举。"""
    original = json.loads(m2['input_binding'])
    by_id = {s['source_id']:(rank,s['role']) for rank,s in enumerate(original['sources'])}
    require(len(by_id) == len(original['sources']), '原来源身份重复')
    require(len(set(source_ids)) == len(source_ids), '消费来源重复')
    require([s for s in m2['ordered_source_ids'] if s in source_ids] == list(source_ids), '消费来源不在实际M2选择或原序漂移')
    full = [dict(source_id=s,upstream_rank=by_id[s][0],calculation_role=by_id[s][1]) for s in m2['ordered_source_ids']]
    require(full == m2['selected_sources'], 'M2原global rank/calculation_role不符')
    return [s for s in full if s['source_id'] in source_ids]


def resource_selection(m2, source_ids):
    """独立RIB按Resource原时点顺序消费；完整M2身份/global rank不改变。"""
    original = selection(m2,m2['ordered_source_ids'])
    by_id = {s['source_id']:s for s in original}
    require(len(set(source_ids)) == len(source_ids), 'Resource消费来源重复')
    require(all(s in by_id for s in source_ids), 'Resource来源不在实际M2选择')
    require(all(by_id[s]['calculation_role'] in ('baseline','snapshot') for s in source_ids), 'Resource只接受独立RIB来源')
    return [by_id[s] for s in source_ids]


def known_role(consumer_key, consumer, dependency_key, dependency):
    """每个正式依赖一条完整选择；role名称在此有限绑定，非任意用途文本。"""
    if consumer['owner']=='trend':
        from data_pipeline.results.trend_metadata import role
        return role(consumer_key,consumer,dependency_key,dependency)
    if consumer['owner']=='country':
        from data_pipeline.results.country_metadata import role
        return role(consumer_key,consumer,dependency_key,dependency)
    owner = consumer['owner']; dc = codec(dependency['owner'])
    cb = codec(owner).untyped(consumer['owner_binding'])
    db = dc.untyped(dependency['owner_binding'])
    reference = dependency['owner'] == 'reference'
    require(dependency['owner'] in ('m2','reference'), '该已固定owner不接受此种依赖')
    mb = db['m2_binding'] if reference else db
    ids = []; purpose = None; window_role = 'result'
    if owner == 'reference':
        require(not reference and cb['m2_admission_id'] == dependency['admission_id'] and cb['m2_binding'] == db, '原参考父M2不一致')
        windows = {'reference':cb}; window_role = 'reference'
    elif owner == 'canonical':
        plan = cb['descriptor']['plan']
        require(plan['input_binding'] == json.loads(mb['input_binding']) and plan['input_manifest'] == mb['plan']['manifest'], 'canonical原M2输入不一致')
        if reference:
            matches = [r for r in plan['references'] if r['source_id'] == db['source_id']]
            require(len(matches) == 1, 'canonical参考用途缺失或重复'); purpose = matches[0]
        else: ids = plan['selected_sources']
        from data_pipeline.bgp.replay.calculation_window import window_from_plan
        windows = {'calculation_window':window_from_plan(plan),'input_manifest':plan['input_manifest']}
        window_role = 'warmup'
    elif owner == 'detection':
        identity = cb['identity']
        require(identity['input_binding'] == json.loads(mb['input_binding']), 'Detection原M2输入不一致')
        from data_pipeline.analysis.detection.result_window import identity_windows, windows as checked_windows
        windows = identity_windows(identity,cb['scope'])
        checked_windows(cb['scope'],identity['result_window'],mb['plan']['manifest'])
        if reference:
            purpose = {k:v for k,v in identity['reference_sources'].items() if v['source_id'] == db['source_id']}
            require(bool(purpose), 'Detection参考用途缺失')
        else: ids = identity['selected_sources']
    elif owner == 'feature':
        spec = cb['specification']
        require(any(s['run_id'] == mb['run_id'] and s['snapshot'] == mb['snapshot'] and s['seal'] == mb['seal'] for s in spec['observation_seals']), 'Feature实际M2封存不匹配')
        if reference:
            purpose = spec['reference_binding']
            require(purpose['source_sha256'] == db['source_id'] and purpose['checkpoint']['ordinal'] == db['checkpoint_ordinal'], 'Feature参考用途错配')
        else:
            ids = [s['source_id'] for s in spec['source_bindings'] if s['run_id'] == mb['run_id'] and s['snapshot'] == mb['snapshot']]
        windows = {k:spec[k] for k in ('initial_rib_time','calculation_window','comparison_window','result_window','source_bindings')}
    elif owner == 'resource':
        binding = cb['binding']
        if reference:
            purpose = {'csv_reference':binding['csv_reference'],'csv_observation':binding['csv_observation']}
            require(purpose['csv_reference']['source_id'] == db['source_id'] and purpose['csv_observation']['checkpoint']['ordinal'] == db['checkpoint_ordinal'], 'Resource参考用途错配')
        else:
            ids = [s['context']['source_id'] for s in binding['sources']
                   if binding['observation_inputs'][s['context']['source_id']]['binding_id'] == mb['input_binding_id']]
        windows = {k:binding[k] for k in ('result_window','sources')}
    else:
        raise ValueError('角色语义尚缺正式owner适配：'+owner)
    if reference:
        require(db['selected_sources'] == [], '参考不得伪造MRT rank')
        window_role = 'reference'
    selected = resource_selection(mb,ids) if owner == 'resource' else selection(mb,ids)
    if not reference and not ids: window_role = 'reference'
    return dict(consumer_key=consumer_key,dependency_key=dependency_key,
                role=owner+(':reference' if reference else ':observation' if ids else ':reference-carrier'),
                collector=mb['plan']['manifest']['collector'],input_binding_typed=dependency['owner_binding'],
                codec_version=dependency['binding_codec'],selected_sources=selected,
                window_role=window_role,window_typed=dc.typed(windows),
                reference_role=dc.typed(purpose) if reference else None)


def verify_known_roles(request):
    nodes = {a['owner']:a for a in request['components']}
    nodes.update({d['dependency_key']:d['admission'] for d in request['dependencies']})
    pending = []
    for role in request['role_graph']:
        consumer = nodes[role['consumer_key']]
        expected = known_role(role['consumer_key'],consumer,role['dependency_key'],nodes[role['dependency_key']])
        require(role == expected, '角色的实际来源/原rank/窗口/参考用途不符')
    bindings = {a['owner']:codec(a['owner']).untyped(a['owner_binding'])
                for a in request['components'] if a['owner'] in ('resource','feature','detection')}
    if set(bindings) == {'resource','feature','detection'}:
        from data_pipeline.analysis.detection.result_window import instant, identity_windows
        def bounds(value):
            if type(value) is dict:
                value = [value['window_start'],value['window_end_exclusive']]
            require(type(value) is list and len(value) == 2, '跨主体窗口缺失')
            start,end = map(instant,value)
            require(start < end, '跨主体窗口非空半开区间错误')
            return start,end
        d = bindings['detection']; dw = identity_windows(d['identity'],d['scope'])
        result = bounds(dw['result_window']); calculation = bounds(dw['calculation_window'])
        f = bindings['feature']['specification']; r = bindings['resource']['binding']
        require(bounds(f['result_window']) == result == bounds(r['result_window']), 'R/F/D结果窗口不相容')
        require(bounds(f['calculation_window']) == calculation, 'Feature/Detection计算窗不相容')
        comparison = bounds(f['comparison_window'])
        require(calculation[0] <= comparison[0] < comparison[1] == result[0], '比较窗与结果窗接合不符')
        for admission in nodes.values():
            if admission['owner'] != 'canonical': continue
            plan = codec('canonical').untyped(admission['owner_binding'])['descriptor']['plan']
            from data_pipeline.bgp.replay.calculation_window import window_from_plan
            require(bounds(window_from_plan(plan)) == calculation
                    and plan['input_binding'] == d['identity']['input_binding'], 'Canonical/D计算窗或完整输入链不一致')
    return pending
