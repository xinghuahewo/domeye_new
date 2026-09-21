"""按 Canonical 原处理位置查询历史后态；范围 Gap 独立于合法变化保存。"""
from bisect import bisect_right
from collections import Counter
from copy import deepcopy

from data_pipeline.bgp.replay.quality_overlay import ScopeIndex, position_key
from data_pipeline.bgp.replay.snapshot_contract import encode
from data_pipeline.bgp.replay.snapshot_validation import validate_row
from data_pipeline.bgp.replay.route_replay import identity
from data_pipeline.bgp.record_types import MessagePosition, ElementPosition


class CanonicalHistory:
    """有界的单次消费索引；调用者须完整读取并核验公开读回执后才可查询。

    此类不签发 Admission，不推断事件 anchor，也不把 RIB 时间认作 cutover。
    position 是 (原全局 rank, record, phase, ordinal)，不是原正文内部 Replay rank。
    """
    tables = ('changes', 'invalidations', 'scope_gap', 'qualification_change')

    def __init__(self, binding, *, max_rows, max_bytes, max_gap_candidates, guard, store=None, original_rows=None):
        for limit in (max_rows, max_bytes, max_gap_candidates):
            if type(limit) is not int or limit <= 0:
                raise ValueError('M3 历史预算必须是正整数')
        self.binding, self.guard = binding, guard
        self.max_rows, self.max_bytes = max_rows, max_bytes
        self.max_gap_candidates = max_gap_candidates
        self.stats = Counter()
        self.rows = {table: [] for table in self.tables}
        self.objects, self.scopes, self.index = {}, {}, ScopeIndex()
        self.store=store;self.row_counts=Counter()
        if store is not None:
            from data_pipeline.analysis.country_events.history_staging import Objects, Gaps
            self.rows=dict(original_rows)
            self.objects=Objects(store);self.scopes=store.new_map();self.index=Gaps(store,self.rows['scope_gap'])
        self.sealed = False

    def check_budget(self):
        self.guard()
        if (any(type(v) is not int or v <= 0 for v in
                (self.max_rows, self.max_bytes, self.max_gap_candidates))
                or self.store is None and (self.stats['rows'] + self.stats['temporal_rows'] > self.max_rows
                or self.stats['bytes'] + self.stats['temporal_bytes'] > self.max_bytes)):
            raise ValueError('resource_limit:M3_history_budget')

    @staticmethod
    def original_position(position):
        """沿用上游位置类型校验；不能让原消息位置被错误的 phase/ordinal 替代。"""
        if type(position) is not dict:
            raise ValueError('M3 原处理位置结构无效')
        if set(position) == {'message', 'ordinal'}:
            message = position['message']
            if type(message) is not dict or set(message) != {'source_rank', 'record'}:
                raise ValueError('M3 原消息位置结构无效')
            return ElementPosition(MessagePosition(**message), position['ordinal']).sort_key
        if set(position) == {'source_rank', 'record'}:
            return MessagePosition(**position).sort_key
        raise ValueError('M3 原处理位置字段不符')

    def append(self, table, row):
        self.check_budget()
        if self.sealed or table not in self.rows:
            raise ValueError('M3 历史输入已关闭或表不受支持')
        validate_row(table, row)
        size = len(encode(row).encode())
        if self.store is None and (self.stats['rows'] + 1 > self.max_rows or self.stats['bytes'] + size > self.max_bytes):
            raise ValueError('resource_limit:M3_history_input')
        position = row['raw']['position'] if table == 'scope_gap' else row['position']
        order = self.original_position(position)
        if self.binding.source_at(order[0]) != row['source_id']:
            raise ValueError('M3 Canonical 原全局位置与来源冲突')
        if row['message_id'] != f"{row['source_id']}:{order[1]}":
            raise ValueError('M3 原消息引用与处理位置冲突')
        if table == 'changes' and (order[2] != 1 or order[3] != row['raw']['ordinal']):
            raise ValueError('M3 合法变化必须保留原元素位置')
        if table in ('scope_gap', 'invalidations') and order[2] != 0:
            raise ValueError('M3 Gap/STATE 必须保留原消息位置')
        frozen = row if self.store is not None else deepcopy(row)
        ordinal = self.row_counts[table] if self.store is not None else len(self.rows[table])
        if table == 'scope_gap':
            if frozen['raw']['binding_ref'] != self.binding.input_binding_id:
                raise ValueError('M3 Gap 原绑定冲突')
            self.index.add(frozen['raw'])
        elif table in ('changes', 'invalidations'):
            object_id, scope = frozen['object_key'], frozen['raw']['scope']
            if object_id in self.scopes and self.scopes[object_id] != scope:
                raise ValueError('M3 对象原身份变化')
            self.scopes[object_id] = scope
            entries = self.objects.setdefault(object_id, [])
            entries.append((order, table, ordinal))
        if self.store is None:self.rows[table].append(frozen)
        self.row_counts[table]+=1
        self.stats.update(rows=1, bytes=size)

    def seal(self):
        """仅结束本地索引输入；上游 Admission/回执由公开读取接合层验证。"""
        self.check_budget()
        if self.sealed:
            raise ValueError('M3 历史输入重复关闭')
        if self.store is not None:
            if any(self.row_counts[t]!=len(self.rows[t]) for t in self.tables):raise ValueError('M3 落盘历史输入不完整')
            self.store.flush();self.sealed=True;return
        for entries in self.objects.values():
            self.guard()
            entries.sort()
            if any(a[0] == b[0] for a, b in zip(entries, entries[1:])):
                raise ValueError('M3 对象同处理位置重复')
        self.sealed = True

    def validate_scope(self, scope):
        if (type(scope) is not tuple or len(scope) != 11 or scope[0] != self.binding.collector
                or scope[6] not in (1, 2) or type(scope[9]) is not bool
                or (scope[9] and type(scope[10]) is not int) or (not scope[9] and scope[10] is not None)):
            raise ValueError('M3 查询对象必须保留原 Collector/AFI/path 槽范围')

    def at(self, scope, position):
        self.check_budget()
        if not self.sealed:
            raise ValueError('M3 历史输入尚未完整枚举')
        self.validate_scope(scope)
        if (type(position) is not tuple or len(position) != 4
                or any(type(v) is not int or v < 0 for v in position)
                or position[2] not in (0, 1) or (position[2] == 0 and position[3] != 0)
                or self.binding.source_at(position[0]) is None):
            raise ValueError('M3 查询位置必须使用已选原全局来源')
        # 保守累计全部Gap候选工作量；不限制已执行的查询次数。
        cost = len(self.index.gaps)
        self.stats.update(queries=1, gap_candidates=cost)
        matches=[];working_bytes=0
        for g,overlap in self.index.matching(scope):
            if position_key(g['position'])<=position:
                if self.store is not None:
                    working_bytes+=len(encode(g).encode())
                    if working_bytes>self.store.max_row_bytes:raise ValueError('resource_limit:M3_gap_working_bytes')
                matches.append((g,overlap))
        entries = self.objects.get(identity(scope), ())
        if hasattr(entries,'before'):selected=entries.before(position)
        else:
            offset = bisect_right(entries, position, key=lambda entry: entry[0]) - 1
            selected = entries[offset] if offset >= 0 else None
        active = [(g, overlap) for g, overlap in matches
                  if selected is None or position_key(g['position']) > selected[0]]
        source_row = self.rows[selected[1]][selected[2]] if selected else None
        value = source_row['raw']['calculation_after'] if selected and selected[1] == 'changes' else None
        known = value is not None and value['presence'] in ('present', 'absent') and not active
        # 无观察和 STATE 失效都保持 Unknown；last_known 从不充当当前态。
        return dict(object_key=identity(scope), position=position,
                    presence=value['presence'] if known else 'unknown',
                    path_key=value.get('path_key') if known else None,
                    origin=value.get('origin') if known else None,
                    source_ref=None if selected is None else dict(table=selected[1], ordinal=selected[2]),
                    original=deepcopy(source_row),
                    gap_refs=tuple(g['gap_id'] for g, _ in matches),
                    active_gap_refs=tuple(g['gap_id'] for g, _ in active),
                    overlap='possible_overlap' if any(o == 'possible_overlap' for _, o in active)
                            else 'definite' if active else 'none',
                    continuity='partial' if matches else 'unknown_session_continuity')
