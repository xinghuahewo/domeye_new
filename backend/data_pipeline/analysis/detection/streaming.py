"""完整记录同步交付；自身不累计审计记录，单条超限显式失败。"""

from dataclasses import asdict, dataclass, field
from functools import cached_property
from typing import Iterable, Mapping
import json
import sys

from data_pipeline.analysis.detection._results import Results, plain
from data_pipeline.analysis.detection.engine import DetectionEngine
from data_pipeline.analysis.detection.models import DetectionInput, DetectionSeed
from data_pipeline.analysis.detection.adapter import adapt_element
from data_pipeline.common.prefix_networks import network_facts
from data_pipeline.bgp.state.path_dictionary import vp_text
from copy import deepcopy


class _DeferredEvidence(Mapping):
    """只供拥有输入的同步计算者使用；首次身份计算或输出时按原 asdict 冻结。"""

    def __init__(self, value):
        self.value = value

    @cached_property
    def snapshot(self):
        return asdict(self.value)

    def __getitem__(self, key):
        return self.snapshot[key]

    def __iter__(self):
        return iter(self.snapshot)

    def __len__(self):
        return len(self.snapshot)

    def __deepcopy__(self, memo):
        return deepcopy(self.snapshot, memo)


class _RowEvidence(_DeferredEvidence):
    """真实输出才构造原输入证据；同步消费期间原行和消息上下文均不可变。"""
    def __init__(self,row,context):self.row,self.context=row,context

    @cached_property
    def snapshot(self):return asdict(adapt_element(self.row,**self.context))


class _BatchEvidence(_DeferredEvidence):
    """只在当前同步规则调用内持有列批；输出/身份首次需要时冻结原适配结果。"""

    def __init__(self, batch, index, boundary, context):
        self.batch, self.index, self.boundary, self.context = batch, index, boundary, context

    @cached_property
    def snapshot(self):
        if self.batch is None:
            raise RuntimeError('同步计算结束后不能再引用未冻结的批次证据')
        return asdict(adapt_element(self.batch.row(self.index, self.boundary), **self.context))

    def release(self):
        self.batch = self.boundary = self.context = None


@dataclass(frozen=True)
class EmitReceipt:
    start: int
    end: int


@dataclass(frozen=True)
class StreamingSeed:
    observations: Iterable[DetectionInput]
    baseline_ref: str
    expected_count: int
    legacy_ids: Mapping = field(default_factory=dict)
    initialization: str = "explicit_rib_cold_start"


class BoundedResults(Results):
    """sink 必须同步消费或自行提供有界缓冲；异常禁止继续发出。"""

    OMITTED_AUDIT = frozenset(("compatibility_transition", "rule_decision", "reference_lookup", "reference_parse", "filtered"))

    def __init__(self, scope, references, sink, *, max_record_bytes=8 * 1024 * 1024, audit=True):
        super().__init__(scope, references)
        if max_record_bytes <= 0:
            raise ValueError("单条记录字节上限必须为正")
        self.sink = sink
        self.audit = audit
        self.max_record_bytes = max_record_bytes
        self.emitted = 0
        self.failed = False

    def append(self,kind,payload,**attributes):
        if not self.audit and kind in self.OMITTED_AUDIT:return None
        return super().append(kind,payload,**attributes)

    @property
    def position(self):
        return self.emitted

    def since(self, start):
        return EmitReceipt(start, self.emitted)

    def _save(self, row):
        if self.failed:
            raise RuntimeError("输出已经失败，禁止继续")
        try:
            # 逐片计量，避免为超大记录再分配完整序列化字符串。
            size = 0
            encoder = json.JSONEncoder(ensure_ascii=False, allow_nan=False)
            for chunk in encoder.iterencode(plain(row)):
                size += len(chunk.encode("utf-8"))
                if size > self.max_record_bytes:
                    raise ValueError("Detection 单条输出超过字节上限；不截断")
            self.sink(row)
            self.emitted += 1
        except Exception:
            self.failed = True
            raise


class _EventRecords(dict):
    """读出可变记录即标记候选；嵌套写入仍操作原记录，无复制或代理。"""

    def __init__(self, values, touch):
        super().__init__(values)
        self.touch = touch
        self.order = {key: i for i, key in enumerate(self)}
        self.next_order = len(self)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.touch(key)
        return value

    def __setitem__(self, key, value):
        if key not in self:
            self.order[key] = self.next_order
            self.next_order += 1
        super().__setitem__(key, value)
        self.touch(key)

    def get(self, key, default=None):
        return self[key] if key in self else default

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
        return self[key]

    def items(self):
        for key in self:
            yield key, self[key]

    def values(self):
        for key in self:
            yield self[key]

    def __deepcopy__(self, memo):
        return deepcopy(dict(dict.items(self)), memo)


class _EventObjects(dict):
    def __init__(self, values):
        super().__init__()
        self.dirty = set()
        self.order = {}
        self.next_order = 0
        for key, value in values.items():
            self[key] = value

    def __setitem__(self, key, value):
        if key not in self:
            self.order[key] = self.next_order
            self.next_order += 1
        records = _EventRecords(value, lambda ident: self.dirty.add((key, ident)))
        super().__setitem__(key, records)
        self.dirty.update((key, ident) for ident in records)

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
        return self[key]

    def drain(self):
        keys = [
            (obj, ident)
            for obj, ident in self.dirty
            if obj in self and ident in self[obj]
        ]
        keys.sort(key=lambda pair: (self.order[pair[0]], self[pair[0]].order[pair[1]]))
        self.dirty.clear()
        for obj, ident in keys:
            yield obj, ident, dict.__getitem__(self[obj], ident)

    def __deepcopy__(self, memo):
        return deepcopy(dict(self), memo)


class StreamingDetectionEngine(DetectionEngine):
    """算法共用原入口，修订只检查被取用/替换的记录；保持旧遍历顺序。"""

    def __init__(
        self, seed, references, scope, *, sink, max_record_bytes=8 * 1024 * 1024, projection=None, audit=True,
        defer_input_evidence=False,
    ):
        self.defer_input_evidence = defer_input_evidence
        super().__init__(
            seed,
            references,
            scope,
            projection=projection,
            results_factory=lambda s, r: BoundedResults(
                s, r, sink, max_record_bytes=max_record_bytes, audit=audit
            ),
        )
        self._event_indexes = []
        for kind, module, attr in [
            ("prefix_outage", self.outage, "prefix_outage_event"),
            ("as_outage", self.outage, "as_outage_event"),
            ("country_outage", self.outage, "country_outage_event"),
            ("moas", self.hijack, "moas_event_dict"),
            ("sub_hijack", self.subhijack, "sub_hijack_dict"),
        ]:
            index = _EventObjects(getattr(module, attr))
            setattr(module, attr, index)
            self._event_indexes.append((kind, index))

    @property
    def last_observation(self):
        position = getattr(self, '_last_observation_position', None)
        if position is not None:
            return f'{position[0]}:{position[1]}'
        return getattr(self, '_last_observation', None)

    @last_observation.setter
    def last_observation(self, value):
        self._last_observation = value
        self._last_observation_position = None

    def _input_context(self, item):
        if not self.defer_input_evidence or self.output.audit:
            return super()._input_context(item)
        # BusinessLoop 同步拥有适配后的输入；检测期间不修改它，下一条会替换上下文。
        # before/after 仍由原算法逐条取得；只是没有输出时免做整份证据的递归转换。
        return {
            "observation": _DeferredEvidence(item),
            "file": _DeferredEvidence(self.file_boundary),
            "time_semantics": "legacy_trigger_call; not_validated_regular_slot",
        }

    def consume_fields(self,row,t,context):
        """已通过消息与列合同的顺序计算入口；规则与事件正文仍由原实现生成。"""
        if self.output.audit or not self.defer_input_evidence:
            return self.consume(adapt_element(row,**context))
        if self.status in ('failed','partial') or self.file_boundary is None:
            raise ValueError('需有效文件边界且计算未失败')
        start=self.output.position;self._rule_stage='input'
        self.output.context={'observation':_RowEvidence(row,context),'file':_DeferredEvidence(self.file_boundary),
            'time_semantics':'legacy_trigger_call; not_validated_regular_slot'}
        try:
            if row['action'] not in ('announce','withdraw') or type(row['local_message']) is not bool:
                raise ValueError('直接 UPDATE 动作或方向无效')
            if row['peer_asn'] is None or not row['peer_ip']:raise ValueError('真实 Peer 端点缺失')
            ident=row['event_id']
            if ident in self.seen_observations:raise ValueError('重复观察引用；计算不得重复累计')
            self.seen_observations.add(ident)
            action='A' if row['action']=='announce' else 'W'
            prefix=network_facts(row['prefix'],strict=False).text
            reason='default_route' if prefix in ('0.0.0.0/0','::/0') else (
                'announcement_as_set' if action=='A' and '{' in row['as_path_text'] else None)
            if reason:
                self.output.append('filtered',{'reason':reason})
                return self.output.since(start)
            self._run_rules(t,action,prefix,vp_text(row['peer_asn']),row['as_path_text'])
            self.last_observation=ident
        except Exception as error:
            self.status='partial' if self.processed or self.output.position>start else 'failed'
            self.output.append('failed',dict(stage=self._rule_stage,exception=type(error).__name__,reason=str(error),publication_eligible=False))
        return self.output.since(start)

    def consume_batch_fields(self, batch, index, boundary, t, context):
        """必要列和共享消息上下文直接执行原规则，事件证据按需一次冻结。"""
        if self.output.audit or not self.defer_input_evidence:
            return self.consume(adapt_element(batch.row(index, boundary), **context))
        if self.status in ('failed', 'partial') or self.file_boundary is None:
            raise ValueError('需有效文件边界且计算未失败')
        start = self.output.position
        self._rule_stage = 'input'
        evidence = _BatchEvidence(batch, index, boundary, context)
        self.output.context = {
            'observation': evidence, 'file': _DeferredEvidence(self.file_boundary),
            'time_semantics': 'legacy_trigger_call; not_validated_regular_slot',
        }
        try:
            columns = batch.columns
            action = columns['action'][index]
            if action not in ('announce', 'withdraw') or type(boundary.raw['local_message']) is not bool:
                raise ValueError('直接 UPDATE 动作或方向无效')
            peer_asn = columns['peer_asn'][index]
            if peer_asn is None or not columns['peer_ip'][index]:
                raise ValueError('真实 Peer 端点缺失')
            ordinal = columns['ordinal'][index]
            if hasattr(self.seen_observations, 'advance'):
                self.seen_observations.advance(boundary.raw['source_id'], boundary.raw['record'], ordinal)
            else:
                ident = f'{boundary.raw["message_id"]}:{ordinal}'
                if ident in self.seen_observations:
                    raise ValueError('重复观察引用；计算不得重复累计')
                self.seen_observations.add(ident)
            flag = 'A' if action == 'announce' else 'W'
            prefix = network_facts(columns['prefix'][index], strict=False).text
            path = batch.business_paths[columns['path_key'][index]]
            reason = 'default_route' if prefix in ('0.0.0.0/0', '::/0') else (
                'announcement_as_set' if flag == 'A' and '{' in path else None)
            if reason:
                self.output.append('filtered', {'reason': reason})
                return self.output.since(start)
            self._run_rules(t, flag, prefix, vp_text(peer_asn), path)
            self._last_observation_position = (boundary.raw['message_id'], ordinal)
        except Exception as error:
            self.status = 'partial' if self.processed or self.output.position > start else 'failed'
            self.output.append('failed', dict(stage=self._rule_stage, exception=type(error).__name__,
                                              reason=str(error), publication_eligible=False))
        finally:
            # Results 在 append/事件身份生成时同步冻结；活动事件不拥有批次输入。
            # 无输出的观察不会为了释放批次而补做一次完整适配。
            evidence.release()
            self.output.context = {}
        return self.output.since(start)

    def _revisions(self):
        for kind, index in self._event_indexes:
            for obj, ident, record in index.drain():
                self.output.event_revision(kind, obj, ident, record)

    def _copy_seed(self, seed):
        if not isinstance(seed, StreamingSeed):
            return super()._copy_seed(seed)
        if not isinstance(seed.expected_count, int) or seed.expected_count < 0:
            raise ValueError("基线声明计数必须为非负整数")
        return DetectionSeed(
            (), seed.baseline_ref, deepcopy(seed.legacy_ids), seed.initialization
        )

    def _copy_references(self, references):
        # 避免引入含文件写入的参考构建模块；仅消费显式移交接口。
        if hasattr(references, "take_info"):
            return references.metadata()
        return super()._copy_references(references)

    def _reference_info(self, references):
        if hasattr(references, "take_info"):
            return references.take_info()
        return super()._reference_info(references)

    def _seed_observations(self, seed):
        count = 0
        for item in seed.observations:
            count += 1
            if isinstance(seed, StreamingSeed) and count > seed.expected_count:
                raise ValueError("基线超过声明计数")
            yield item
        if isinstance(seed, StreamingSeed) and count != seed.expected_count:
            raise ValueError("基线提前结束，计数不符")
        self.baseline_count = count
