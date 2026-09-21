"""M3 范围 Gap 到既有保存态归约的接合；不重放 action，不生成伪 STATE。"""
from copy import deepcopy
from dataclasses import dataclass

from data_pipeline.analysis.country_events.compute import Change
from data_pipeline.analysis.country_events.models import Binding, Cursor, Time
from data_pipeline.analysis.country_events.aggregation_staging import Stage, load
from data_pipeline.bgp.replay.quality_overlay import ScopeIndex, position_key


@dataclass(frozen=True)
class ScopedGap(Change):
    original_gap: object = None

    def __post_init__(self):
        super().__post_init__()
        if (not self.gap or self.after is not None or type(self.original_gap) is not dict
                or self.original_gap['gap_id'] != self.reference):
            raise ValueError('M3 范围 Gap 必须保留原身份与正文')

    @property
    def global_invalidation(self):
        # 未见对象没有 current 行，不等于全局 Gap；其资格由独立 scope 保留。
        return False


class M3Stage(Stage):
    """复用旧 C2 原21类科学输出，新增的范围证据留在独立输入索引。

    旧物理 Binding 列保存完整原全局来源序，selected_binding 单独固定实际
    选择。未选来源没有输入行，也不能通过 add_change 注入计算。
    """
    def __init__(self, path, binding, guard, limits, *, baseline_source,
                 history, messages, max_gap_candidates):
        if baseline_source not in binding.ordered_sources:
            raise ValueError('M3 基线角色未在实际选择中')
        if type(max_gap_candidates) is not int or max_gap_candidates <= 0:
            raise ValueError('M3 Gap 候选预算无效')
        # 不扩写旧21表的Binding物理列；完整选择和M3输入身份必须另随证据封存。
        complete = Binding(binding.run_id, binding.snapshot_id, binding.global_sources,
                           binding.reference_version, binding.decoder_version,
                           binding.state_rule_version, binding.collector)
        super().__init__(path, complete, guard, limits)
        self.selected_binding = binding
        self.baseline_rank = binding.rank_of(baseline_source)
        self.history, self.messages = history, messages
        self.max_gap_candidates = max_gap_candidates

    def add_change(self, change, phase=1):
        if self.selected_binding.source_at(change.cursor.source_rank) != change.source_id:
            raise ValueError('M3 未选原来源不得进入 C2 归约')
        super().add_change(change, phase)

    def finish(self):
        if not self.history.sealed:
            raise ValueError('M3 原范围历史没有完整输入')
        # 每个 Gap 先扣全部对象候选；含未来和未匹配对象，不能按匹配后数量计费。
        for row in self.history.rows['scope_gap']:
            self.guard()
            raw = row['raw']
            candidates = len(self.history.scopes)
            self.history.check_budget()
            self.history.stats['gap_candidates'] += candidates
            self.counts['gap_scope_candidates'] += candidates
            index = ScopeIndex(); index.add(raw)
            pos = position_key(raw['position'])
            message = self.messages[row['message_id']]
            if (self.selected_binding.source_at(pos[0]) != row['source_id']
                    or message['source_id'] != row['source_id'] or message['record'] != pos[1]):
                raise ValueError('M3 Gap 原消息和原全局位置不符')
            affected = []
            for obj, scope in self.history.scopes.items():
                self.guard()
                # 尚未发生的对象不能被提前实例化；未见范围仍保留原 Gap 全文。
                if self.history.objects[obj][0][0] >= pos:
                    continue
                if index.matching(scope):
                    if len(affected) >= self.limits['max_objects']:
                        raise ValueError('resource_limit:M3_C2_gap_objects')
                    affected.append(obj)
            # RawTime 已由上游 ordered 合同决定；不能从可能为None的payload猜ET精度。
            at = raw['raw_time']
            change = ScopedGap(Cursor(pos[0], pos[1], 0), Time(at['epoch'], at['microsecond']),
                               row['source_id'], raw['gap_id'], invalidated_objects=tuple(sorted(affected)),
                               gap=True, original_gap=deepcopy(raw))
            self.add_change(change, phase=0)
            self.db.execute('INSERT INTO evidence VALUES (?,?,?)',
                            ('m3_scope_gap', raw['gap_id'], self.checked(row)))
        super().finish()
        # 基线角色来自实际 M2 选择，不能假定原rank为0。
        self.initial_end = self.db.execute('SELECT max(i) FROM units WHERE rank=?',
                                           (self.baseline_rank,)).fetchone()[0]

    def impact(self, low, high, country, prefixes, max_span, *, include_potential=False):
        if super().impact(low, high, country, prefixes, max_span):
            return True
        # 初始集合保留潜在成员风险；已固定样本只判原prefix/完整映射对象的相交。
        for (payload,) in self.db.execute('SELECT payload FROM units WHERE i>? AND i<=?', (low, high)):
            self.guard()
            change = load(payload)
            if isinstance(change,ScopedGap) and change.original_gap['scope']['kind']!='local_observation':
                if include_potential:return True
                index=ScopeIndex();index.add(change.original_gap);found=False
                for prefix in sorted(prefixes):
                    for (stored,) in self.db.execute('SELECT payload FROM current WHERE prefix=?',(prefix,)):
                        self.guard();self.history.check_budget()
                        self.history.stats['gap_candidates']+=1;self.counts['gap_scope_candidates']+=1
                        route=load(stored);e=route.endpoint
                        scope=(e.collector,e.remote_ip,e.remote_asn,e.local_ip,e.local_asn,e.interface,
                               e.afi,e.safi,route.prefix,route.path_id is not None,route.path_id)
                        if (route.mapping_state!='bound' or route.local_message
                                or e.local_ip.startswith('unmapped_rib:')
                                or self.history.scopes.get(route.object_id)!=scope):return True
                        found=True
                        if index.matching(scope):return True
                if not found:return True
        return False
