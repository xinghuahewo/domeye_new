"""从完整原对象见证取得范围相交与确定子集；不使用last-known/raw累计作证明。"""
from fractions import Fraction
from data_pipeline.analysis.country_events.route_time_index import state_before
from data_pipeline.bgp.replay.quality_overlay import ScopeIndex, position_key


def route_scope(route,changes):
    if route.mapping_state!='bound' or route.local_message:return None
    e=route.endpoint
    scope=(e.collector,e.remote_ip,e.remote_asn,e.local_ip,e.local_asn,e.interface,e.afi,e.safi,
           route.prefix,route.path_id is not None,route.path_id)
    original=changes.get(route.observation_ref)
    if (original is None or tuple(original['raw']['scope'])!=scope
            or e.local_ip.startswith('unmapped_rib:')):return None
    return scope


def relevant_gaps(source,routes,changes,gaps,at,limit,guard):
    """精确原映射可排除不相交；无法定位时间时保留该对象全部潜在相交Gap。"""
    known=[]
    for route in routes:
        scope=route_scope(route,changes)
        if scope is None:return None  # 调用者保持保守全范围，不以字符串不同豁免。
        known.append(scope)
    if not known or not hasattr(source,'history') or not hasattr(source,'messages'):return None
    allowed=set()
    for scope in known:
        guard();state=state_before(source.history,source.messages,scope,at)
        if state['time_boundary']=='located':allowed.update(state['gap_refs'])
        else:
            for gap,_ in gaps:
                index=ScopeIndex();index.add(gap)
                if index.matching(scope):allowed.add(gap['gap_id'])
        if len(allowed)>limit:raise ValueError('resource_limit:M3_qualification_refs')
    refs=[]
    for gap,ref in gaps:
        if gap['gap_id'] in allowed:refs.append(ref)
    return tuple(refs)


def visible_directions(source,routes,changes,at,ref,guard,limit):
    """一个方向至少一个当时已证present对象；ADDPATH多对象不重复计方向。"""
    if not hasattr(source,'history') or not hasattr(source,'messages'):return None,()
    directions=set();witnesses=[]
    for route in routes:
        guard();scope=route_scope(route,changes)
        if scope is None:continue
        state=state_before(source.history,source.messages,scope,at)
        witness=state['source_ref']
        if (state['time_boundary']=='located' and state['presence']=='present'
                and state['origin'] is not None and witness is not None and witness['table']=='changes'):
            directions.add(route.direction)
            value=ref('canonical','changes',witness['ordinal'])
            if value not in witnesses:witnesses.append(value)
            if len(witnesses)>limit:raise ValueError('resource_limit:M3_qualification_refs')
    # 未提供一个已证正对象时，不能把未知补成0下界。
    return (Fraction(len(directions)),tuple(witnesses)) if directions else (None,())
