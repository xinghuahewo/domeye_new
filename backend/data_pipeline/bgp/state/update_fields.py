"""UPDATE 必要列直接计算；完整输入仅在同步证据输出边界展开。"""

from data_pipeline.analysis.features.calculation import Diagnostic, count_accepted, observation_filter_fields, prefix_filter
from data_pipeline.common.prefix_networks import network_facts
from data_pipeline.bgp.state.path_dictionary import vp_text
from data_pipeline.bgp.replay.route_replay import legacy_origin


# 资格处理只读取 action；复用不可变用法的两份上下文，不为每条拼装观察字典。
_QUALIFICATION_ACTION = {'announce': {'action': 'announce'}, 'withdraw': {'action': 'withdraw'}}


def prepare_update_batch(loop, batch):
    """本批新路径转换为共享编号；旧路径随记录直接在内存中读取。"""
    path_ids = {key: loop.pool.ident(path['as_path_text']) for key, path in batch.paths.items()}
    # 路径内容不复制；编号只属于内部共享文本字典，不是 MRT ADD-PATH 编号。
    batch.business_paths = {key: loop.pool.by_id(ident) for key, ident in path_ids.items()}
    batch.business_features = tuple(
        (mode, routes, loop.feature_quality[mode], loop.work[mode],
         lambda country, asn, mode=mode: loop.count_changed(mode, country, asn))
        for mode, routes in loop.features.items())


def observe_feature_fields(routes, action, raw_prefix, vp, text):
    """FeatureRoutes.observe 的 UPDATE 字段入口；保持原起源残留和撤回规则。"""
    prefix, _ = prefix_filter(raw_prefix)
    paths = routes.prefix_dict.get(prefix, {}) if prefix is not None else {}
    old_path = paths.get(vp, '')
    flag = 'A' if action == 'announce' else 'W'
    origin = legacy_origin(text)
    skip = observation_filter_fields(routes.mode, flag, raw_prefix, text, routes.reference)
    origin = origin if origin is not None else ''
    projection_skip = (not skip and action == 'announce' and routes.mode == 'ir'
                       and origin and routes.reference.code(origin) != 'IR')
    if not skip and not projection_skip:
        if hasattr(routes, 'mark_changed'):
            routes.mark_changed(prefix)
        if action == 'withdraw':
            if vp in paths:
                removed = legacy_origin(paths.pop(vp))
                removed = removed if removed is not None else ''
                remaining = {legacy_origin(path) if legacy_origin(path) is not None else ''
                             for path in paths.values()}
                if removed not in remaining:
                    routes.prefix_as.get(prefix, set()).discard(removed)
                    routes.as_prefix.get(removed, set()).discard(prefix)
                    if not routes.as_prefix.get(removed):
                        routes.as_prefix.pop(removed, None)
                if not paths:
                    routes.prefix_dict.pop(prefix, None)
                if not routes.prefix_as.get(prefix):
                    routes.prefix_as.pop(prefix, None)
        else:
            routes.vp_set.add(vp)
            routes.prefix_dict.setdefault(prefix, {})[vp] = text
            routes.prefix_as.setdefault(prefix, set()).add(origin)
            routes.as_prefix.setdefault(origin, set()).add(prefix)
        routes.refresh_tree(prefix)
        if hasattr(routes, 'finish_changed'):
            routes.finish_changed(prefix)
    return old_path, skip


def apply_update_fields(loop, batch, index, boundary):
    """规范状态已刚推进当前行；同步完成普通业务后才允许推进下一行。"""
    if (loop.boundary is not boundary or loop.current != boundary.raw['source_id']
            or batch.binding_ref != loop.binding['binding_id']):
        raise ValueError('直接业务列与消息边界不符')
    if loop._prepared_boundary is not boundary:
        loop._prepare_update_message(boundary)
    columns = batch.columns
    action = columns['action'][index]
    if action not in _QUALIFICATION_ACTION:
        raise ValueError('直接业务列不是 UPDATE')
    if batch.business_paths is None:
        raise ValueError('UPDATE 新旧路径尚未按批准备')
    ordinal = columns['ordinal'][index]
    raw_prefix = columns['prefix'][index]
    vp = vp_text(columns['peer_asn'][index])
    path = batch.business_paths[columns['path_key'][index]]
    loop.sequence += 1
    flag = 'A' if action == 'announce' else 'W'
    for mode, routes, quality, work, changed in batch.business_features:
        old_path, reason = routes.observe_fields(action, raw_prefix, vp, path)
        quality.element(_QUALIFICATION_ACTION[action])
        if reason:
            diagnostic = Diagnostic('update', f'{boundary.raw["message_id"]}:{ordinal}',
                                    reason, raw_prefix, prefix_filter(raw_prefix)[0])
            loop.sink('feature_diagnostic', dict(mode=mode, raw=vars(diagnostic).copy()))
        else:
            work.t = loop._feature_stamp
            count_accepted(work, loop.reference, flag, path, old_path, on_change=changed)
    if not loop.detection_enabled:return
    loop.detection_quality.position = (boundary.position.source_rank, boundary.position.record, 1, ordinal)
    loop.engine.consume_batch_fields(batch, index, boundary, loop._detection_stamp,
                                     loop._detection_context)
    if loop.engine.status in ('failed', 'partial'):
        raise ValueError('既有 Detection 规则计算失败，候选回滚')
