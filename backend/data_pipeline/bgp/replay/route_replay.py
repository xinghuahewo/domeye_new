"""确定性计算回放与有限事实；计算继承不表示实际Session连续。"""
from dataclasses import dataclass
from functools import cached_property, lru_cache
import hashlib
import json

RULE = 'calculation-replay/v1'
LIMITATIONS = ('cutover_assumed', 'source_order_declared', 'session_continuity_unknown')


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def source_peer(source, peer):
    return identity([source, peer.get('table_record'), peer.get('index')])


def associate(message, reference, candidates):
    """仅显式参考的同原始时间值、同精度、唯一端点正例。"""
    if (message.epoch, message.microsecond) != (reference.epoch, reference.microsecond):
        return None, 'time_or_precision_mismatch'
    matching = [p for p in candidates if (p['ip'], p['asn']) == (message.peer['ip'], message.peer['asn'])]
    if len(matching) != 1:
        return None, 'ambiguous_or_missing_candidate'
    p = matching[0]
    if message.peer.get('bgp_id_present') and message.peer['bgp_id'] != p['bgp_id']:
        return None, 'bgp_id_conflict'
    return source_peer(reference.source_id, p), 'bound_same_time_unique_endpoint/v1'


def legacy_origin(text):
    parts = text.split()
    if not parts:
        return None
    for part in reversed(parts):
        if '_' in part:
            continue
        if '{' in part:
            return part
        try:
            n = int(part)
        except ValueError:
            return part
        if not (64512 <= n <= 65535 or 4200000000 <= n <= 4294967294):
            return part
    return parts[-1]


@dataclass(frozen=True)
class ReplayPlan:
    collector: str
    baseline_source: str
    update_sources: tuple
    baseline_endpoints: tuple = ()

    @cached_property
    def version(self):
        return identity([RULE, self.collector, self.baseline_source, self.update_sources, self.baseline_endpoints, LIMITATIONS])


class _RouteValue:
    """借助实例共享字段键，返回的仍是独立、可变的普通dict。

    不保留包装实例；只共享字段名布局，不共享路由值。失效增加字段时仍
    使用普通dict复制，避免改变此前已返回的change或last_known。
    """
    def __init__(self, presence, path_key, origin, event_id, epoch):
        self.presence = presence
        self.path_key = path_key
        self.origin = origin
        self.event_id = event_id
        self.epoch = epoch


def _same(value):
    return value


class Replay:
    """内存有界由外层资源保护负责；一次独立run，不实现恢复。"""
    def __init__(self, plan, *, compact=False, detailed=True, legacy_views=True):
        self.plan = plan
        self.detailed = detailed
        self.legacy_views = legacy_views
        if detailed and not legacy_views:raise ValueError("详细审计需要兼容视图")
        self._source_ranks = {s: i for i, s in enumerate((plan.baseline_source, *plan.update_sources))}
        endpoints = {}
        for endpoint in plan.baseline_endpoints:
            endpoints.setdefault(tuple(endpoint[:2]), []).append(tuple(endpoint[2:]))
        self._baseline_endpoints = {key: values[0] for key, values in endpoints.items() if len(values) == 1}
        # 仅复用相等的不可变值；淘汰缓存引用不删除任何路由或历史事实。
        # 每个Replay独立持有，避免全局intern永久保留长尾输入。
        self._value = lru_cache(maxsize=8192, typed=True)(_same)
        self._prefix = lru_cache(maxsize=128)(_same)
        self.current = {}
        self.last_known = {}
        self.memory = None
        if compact:
            from data_pipeline.bgp.state.routes import PrefixState
            self.memory = PrefixState(plan)
            self.current, self.last_known = self.memory.current, self.memory.last_known
        self.cursor = (-1, -1, -1)
        self.legacy_by_prefix = {}
        self.legacy_paths = {}
        if compact:
            from data_pipeline.bgp.state.routes import FlatPaths
            self.legacy_paths = FlatPaths(self.legacy_by_prefix)
        self.legacy_origins = {}
        self.seen_vps = set()
        self.legacy_baseline_epoch = None

    def object_key(self, message, e):
        """复用原计算对象键；不改变身份、缓存上限或映射规则。"""
        peer = e['peer']
        # 这是计算端点键，明确不是跨文件稳定Peer/Session身份。
        local = (peer.get('local_ip'), peer.get('local_asn'), peer.get('interface'))
        if e['action'] == 'rib_snapshot':
            local = self._baseline_endpoints.get((peer['ip'], peer['asn']),
                ('unmapped_rib:' + message.source_id, None, None))
        prefix = self._prefix(e['prefix'])
        key = (self.plan.collector, self._value(peer['ip']), self._value(peer['asn']),
               *(self._value(v) for v in local), e['afi'], e['safi'],
               prefix, e['path_id_present'], e['path_id'])
        return key

    def consume(self, message, *, key_hint=None, position_hint=None):
        if message.source_id not in self._source_ranks:
            raise ValueError('未绑定的回放来源')
        source_rank = self._source_ranks[message.source_id]
        if (source_rank, message.record) < self.cursor[:2]:
            raise ValueError('回放输入倒序；只支持相邻重复元素幂等')
        if message.kind == 'state_change' and message.new_state != 6:
            yield from self.invalidate(message.message_id, 'session_disconnect', message.peer, message.epoch)
        if message.peer.get('local_message'):
            return  # 发往Peer的消息保留原始观察，但不推进收到的路由投影。
        paths = {p['path_key']: p for p in message.paths}
        for e in message.elements:
            eid = f'{message.message_id}:{e["ordinal"]}'
            cursor = (source_rank, message.record, e['ordinal'])
            if cursor <= self.cursor:
                continue
            self.cursor = cursor
            peer = e['peer']
            key = key_hint if key_hint is not None else self.object_key(message, e)
            prefix = key[8]
            before = self.current.get(key) if self.detailed else None
            path = paths[e['path_key']]
            path_key = None if e['action'] == 'withdraw' else self._value(e['path_key'])
            origin_asn = None if e['action'] == 'withdraw' else self._value(path['attributed_origin_asn'])
            if self.memory is None or self.detailed:
                after = vars(_RouteValue('absent' if e['action'] == 'withdraw' else 'present',
                                        path_key, origin_asn, eid, message.epoch))
            known = self.last_known.get(key) if self.detailed else None
            if self.memory is not None:
                self.memory.observe(key,source_rank,message.record,e['ordinal'],message.epoch,path_key,origin_asn,
                                    position=position_hint)
            else:
                self.current[key] = after
                self.last_known[key] = after
            if not self.legacy_views:
                yield {'scope':key,'source_rank':source_rank}
                continue
            vp = self._value(str(peer['asn']))
            legacy_key = (prefix, vp)
            legacy_before = self.legacy_paths.get(legacy_key)
            skip = None
            legacy_added=legacy_removed=None
            origin = self._value(legacy_origin(path['as_path_text']))
            if e['action']=='announce' and origin is None:origin=''
            if e['action']=='rib_snapshot' and not e['prefix'].endswith('/0') and self.legacy_baseline_epoch is None:
                self.legacy_baseline_epoch=message.epoch
            # 兼容过滤只作用旧口径，不丢原始观察或规范对象。
            if e['action']=='rib_snapshot' and e['prefix'].endswith('/0'):
                skip = 'legacy_default_route'
            elif e['action']=='rib_snapshot' and not origin:
                skip = 'legacy_empty_origin'
            if not skip:
                if e['action'] == 'withdraw':
                    removed = self.legacy_paths.pop(legacy_key, None)
                    self.legacy_by_prefix.setdefault(e['prefix'], {}).pop(vp, None)
                    if removed is not None:
                        old_origin = legacy_origin(removed)
                        remaining = [legacy_origin(v) for v in self.legacy_by_prefix.get(e['prefix'], {}).values()]
                        if old_origin not in remaining:
                            if old_origin in self.legacy_origins.setdefault(prefix,set()):legacy_removed=old_origin
                            self.legacy_origins[e['prefix']].discard(old_origin)
                else:
                    self.seen_vps.add(vp)
                    self.legacy_paths[legacy_key] = self._value(path['as_path_text'])
                    self.legacy_by_prefix.setdefault(prefix, {})[vp] = self.legacy_paths[legacy_key]
                    # 旧A残留保留于独立投影，规范current已替换旧值。
                    if origin not in self.legacy_origins.setdefault(prefix,set()):legacy_added=origin
                    self.legacy_origins[e['prefix']].add(origin)
            if not self.detailed:
                yield {'scope':key,'source_rank':source_rank}
                continue
            yield {'source_rank':source_rank,'record':message.record,'ordinal':e['ordinal'],'event_id': eid, 'object_key': identity(key), 'scope': key,
                   'calculation_before': before, 'calculation_after': after,
                   'baseline_ref':self.plan.baseline_source,'fact_before_presence': 'unknown', 'fact_after_presence': after['presence'],
                   'last_known_before': known, 'session_id': None,
                   'fragment_id': identity([message.source_id, peer]),
                   'legacy_before': legacy_before, 'legacy_after': self.legacy_paths.get(legacy_key),
                   'legacy_origin_added':legacy_added,'legacy_origin_removed':legacy_removed,
                   'legacy_origin_count':len(self.legacy_origins.get(e['prefix'],set())),
                   'legacy_skip': skip, 'limitations': LIMITATIONS, 'rule_version': self.plan.version}

    def invalidate(self, source, reason, peer, epoch=None):
        """失效不生成路由元素或withdraw，历史last_known保留。"""
        current=self.memory.current_for_peer(peer) if self.memory is not None else self.current.items()
        for key, value in current:
            if key[1:6] != (peer.get('ip'), peer.get('asn'), peer.get('local_ip'), peer.get('local_asn'), peer.get('interface')):
                continue
            self.current[key] = {**value, 'presence': 'unknown', 'reason': reason, 'invalidated_by': source}
            yield {'record_type':'invalidation','object_key':identity(key),'source':source,'reason':reason,
                   'epoch':epoch,'scope':key,'last_known_event':self.last_known[key]['event_id'],
                   'last_known_presence':self.last_known[key]['presence'],
                   'last_known_path':self.last_known[key]['path_key']}
