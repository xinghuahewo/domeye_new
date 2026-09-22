"""39578fe 的 Resource 兼容投影 v1；标准 origin 由观察层独立提供。"""
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import ipaddress
from typing import Iterable

import numpy as np
from utils.prefix_quantity import calculate_c_segments_count, calculate_v6_48_segments_count

RULE = 'resource-39578fe-v1'
METRIC_UNITS = {
    'ipv4_prefix_count': 'distinct_ipv4_prefix',
    'ipv6_prefix_count': 'covered_ipv6_48_block',
    'ipv6_48_count': 'covered_ipv6_48_block',
    'ipv4_address_count': 'covered_ipv4_24_block_times_256',
    'vp_count': 'distinct_first_path_asn',
    'private_as_count': 'distinct_legacy_private_tail_asn',
    'public_as_count': 'distinct_legacy_public_tail_asn',
    'path_count': 'distinct_rendered_path',
}
ITEMS = ('ipv4_prefix_count', 'ipv6_prefix_count', 'private_as_count', 'path_count', 'public_as_count')


def resource_path_parts(path: str):
    """既有 Resource 文本规则；供逐元素计算与独立 RIB 集合统计共用。"""
    tokens = path.split(' ')
    if '{' in path:
        return tokens[0], None, None, 'as_set_after_prefix'
    try:
        tail = int(tokens[-1])
    except ValueError:
        return tokens[0], None, None, 'invalid_tail_after_prefix'
    return tokens[0], tokens[-1], 64512 <= tail <= 65535 or tail > 4294967295, 'accepted'


@dataclass(frozen=True)
class RibContext:
    source_id: str
    collector: str
    snapshot_time: datetime
    reference_id: str
    time_basis: str
    legacy_time_text: str | None = None
    legacy_time_basis: str = 'unknown'
    path_rendering_version: str = 'fixture-text-v1'
    content_sha256: str | None = None
    origin_uri: str | None = None


@dataclass(frozen=True)
class RibElement:
    source_id: str
    message_id: str
    ordinal: int
    prefix: str
    path: str
    peer_asn: str
    peer_ip: str | None = None
    peer_bgp_id: str | None = None
    attributes_ref: str | None = None
    afi: int = 1
    safi: int = 1
    action: str = 'rib_snapshot'
    attributed_origin: int | None = None


@dataclass(frozen=True)
class ReferenceAs:
    as_name: str | None = ''
    global_rank: str | float | int | None = None
    country_cn: str | None = None
    raw_row_ref: str | None = None


@dataclass(frozen=True)
class NormalSample:
    time: datetime
    source_id: str
    value: int


@dataclass
class NormalBand:
    list_len: int = 0
    upper_bound: int | None = None
    lower_bound: int | None = None
    samples: tuple[NormalSample, ...] = ()
    mean: float | None = None
    population_std: float | None = None


@dataclass
class ResourceRow:
    bucket: str
    dimension: str
    time: datetime
    ipv4_prefix: set[str] = field(default_factory=set)
    ipv6_prefix: set[str] = field(default_factory=set)
    vp_set: set[str] = field(default_factory=set)
    private_as: set[str] = field(default_factory=set)
    public_as: set[str] = field(default_factory=set)
    path: set[str] = field(default_factory=set)
    ipv4_prefix_count: int = 0
    ipv6_prefix_count: int = 0  # 旧字段单位为 /48 覆盖块
    ipv4_address_count: int = 0  # /24 覆盖块乘 256
    ipv6_48_count: int = 0
    vp_count: int = 0
    private_as_count: int = 0
    public_as_count: int = 0
    path_count: int = 0
    is_outlier: bool = False
    as_name: str | None = None
    as_rank: int | None = None
    reference_row_ref: str | None = None
    source_id: str = ''
    reference_id: str = ''
    legacy_prefix_insert_ready: bool | None = None
    membership_ref: str | None = None


@dataclass(frozen=True)
class Decision:
    message_id: str
    ordinal: int
    resource_status: str
    observation: RibElement


@dataclass(frozen=True)
class Topology:
    country_cn: str | None
    build_time: datetime
    status: str
    edges: tuple[tuple[int, int], ...]
    nodes: tuple[int, ...]
    legacy_replace_edges: bool
    legacy_update_snapshot: bool
    weight: int = 1
    link_color: str = '#000'
    node_color: str = '#777'


@dataclass
class ResourceState:
    rule_version: str = RULE
    rib_count: int = 0
    initial_state_basis: str = 'cold_start'
    last_context: RibContext | None = None
    normal_range: dict[str, dict[str, NormalBand]] = field(default_factory=dict)
    vp_normal_range: dict[str, dict[str, NormalBand]] = field(default_factory=dict)
    currently_abnormal: dict[str, bool] = field(default_factory=dict)
    prefix_count_dict: dict[str, dict[datetime, ResourceRow]] = field(default_factory=dict)
    vp_prefix_count_dict: dict[str, dict[datetime, ResourceRow]] = field(default_factory=dict)


@dataclass(frozen=True)
class ResourceResult:
    context: RibContext
    rule_version: str
    rows: tuple[ResourceRow, ...]
    vp_rows: tuple[ResourceRow, ...]
    topology: tuple[Topology, ...]
    decisions: tuple[Decision, ...]
    state: ResourceState
    decisions_externalized: bool = False


class ResourceComputer:
    """输入按单 RIB 完整调用；时点严格递增。导出是深拷贝，不承诺恢复。"""

    def __init__(self, references: dict[str, ReferenceAs], *, topology_enabled: bool = False):
        self.references = deepcopy(references)
        self.topology_enabled = topology_enabled
        self.state = ResourceState()
        self._last_time = None
        for bucket in ('9808', '4837', '4134', 'global'):
            self.state.normal_range[bucket] = {item: NormalBand() for item in ITEMS}

    def export_state(self) -> ResourceState:
        return deepcopy(self.state)

    def compute(self, context: RibContext, elements: Iterable[RibElement], *, decision_sink=None, membership_sink=None) -> ResourceResult:
        if (decision_sink is None) != (membership_sink is None):
            raise ValueError('流式模式必须同时保存决策和成员，不能丢中间信息')
        if context.snapshot_time.tzinfo is None:
            raise ValueError('snapshot_time 必须含时区')
        if self.state.last_context and context.collector != self.state.last_context.collector:
            raise ValueError('同一工作态不能跨 Collector')
        if self._last_time is not None and context.snapshot_time <= self._last_time:
            raise ValueError('每份 RIB 时点必须严格递增，不支持重复累加')
        # 失败不提交部分工作态。
        candidate = deepcopy(self.state)
        candidate.rib_count += 1
        rows = {'global': ResourceRow('global', 'first_path_asn', context.snapshot_time)}
        vps = {}
        decisions = []
        country_edges: dict[str, set[tuple[int, int]]] = {}
        missing_country = False
        for element in elements:
            if element.source_id != context.source_id:
                raise ValueError('元素来源与 RIB 不一致')
            if self.topology_enabled:
                missing_country |= self._topology_path(element.path, country_edges)
            status = 'accepted'
            if element.action == 'STATE' or element.prefix in ('0.0.0.0/0', '::/0'):
                status = 'default_or_state'
            else:
                first, tail, private, path_status = resource_path_parts(element.path)
                targets = [rows['global']]
                if first in ('9808', '4837', '4134'):
                    targets.append(rows.setdefault(first, ResourceRow(first, 'first_path_asn', context.snapshot_time)))
                vp = vps.setdefault(element.peer_asn, ResourceRow(element.peer_asn, 'peer_asn', context.snapshot_time))
                try:
                    version = ipaddress.ip_network(element.prefix).version
                except ValueError:
                    status = 'invalid_prefix'
                else:
                    for row in targets + [vp]:
                        getattr(row, 'ipv4_prefix' if version == 4 else 'ipv6_prefix').add(element.prefix)
                    for row in targets:
                        row.vp_set.add(first)
                    status = path_status
                    if status == 'accepted':
                        for row in targets:
                            row.path.add(element.path)
                            getattr(row, 'private_as' if private else 'public_as').add(tail)
            decision = Decision(element.message_id, element.ordinal, status, element)
            if decision_sink is None:
                decisions.append(decision)
            else:
                decision_sink(decision)
        for row in (*rows.values(), *vps.values()):
            row.source_id, row.reference_id = context.source_id, context.reference_id
            row.ipv4_prefix_count = len(row.ipv4_prefix)
            row.ipv6_prefix_count = row.ipv6_48_count = calculate_v6_48_segments_count(row.ipv6_prefix)
            row.ipv4_address_count = calculate_c_segments_count(row.ipv4_prefix) * 256
            row.vp_count = len(row.vp_set)
            row.private_as_count = len(row.private_as)
            row.public_as_count = len(row.public_as)
            row.path_count = len(row.path)
            is_vp = row.dimension == 'peer_asn'
            store = candidate.vp_prefix_count_dict if is_vp else candidate.prefix_count_dict
            ranges = candidate.vp_normal_range if is_vp else candidate.normal_range
            items = ITEMS[:2] if is_vp else ITEMS
            history = store.setdefault(row.bucket, {})
            history[row.time] = row
            bands = ranges.setdefault(row.bucket, {item: NormalBand() for item in items})
            for item in items:
                selected = [NormalSample(time, old.source_id, getattr(old, item)) for time, old in history.items() if time != row.time and not old.is_outlier]
                samples = [sample.value for sample in selected]
                if len(samples) > 5 or candidate.rib_count < 7:
                    if len(samples) <= 5:
                        samples.append(getattr(row, item))
                        selected.append(NormalSample(row.time, row.source_id, getattr(row, item)))
                    band = bands[item]
                    if len(samples) >= band.list_len:
                        mean, std = np.mean(samples), np.std(samples)
                        band.list_len = len(samples)
                        band.samples = tuple(selected)
                        band.mean, band.population_std = float(mean), float(std)
                        band.upper_bound = int((mean + 3 * std) * 1.2)
                        band.lower_bound = int((mean - 3 * std) * 0.8)
                band = bands[item]
                if band.upper_bound is not None and band.lower_bound is not None:
                    row.is_outlier |= not band.lower_bound <= getattr(row, item) <= band.upper_bound
            if not is_vp:
                row.legacy_prefix_insert_ready = all(b.upper_bound is not None and b.lower_bound is not None for b in bands.values())
                candidate.currently_abnormal[row.bucket] = row.is_outlier
            else:
                ref = self.references.get(row.bucket, ReferenceAs())
                row.as_name, row.reference_row_ref = ref.as_name, ref.raw_row_ref
                try:
                    row.as_rank = int(float(ref.global_rank)) if ref.global_rank not in ('', None) else None
                except (ValueError, TypeError, OverflowError):
                    row.as_rank = None
            for old_time in list(history):
                if old_time < row.time - timedelta(days=3):
                    del history[old_time]
        topologies = []
        if self.topology_enabled:
            countries = {ref.country_cn for ref in self.references.values() if ref.country_cn} | country_edges.keys()
            for country in sorted(countries):
                edges = tuple(sorted(country_edges.get(country, ())))
                nodes = tuple(sorted({asn for edge in edges for asn in edge}))
                status = 'graph_too_large' if len(edges) > 50000 else 'computed' if edges else 'no_edges'
                topologies.append(Topology(country, context.snapshot_time, status, edges, nodes, bool(edges), bool(edges) and len(edges) <= 50000))
            if missing_country or not countries:
                topologies.append(Topology(None, context.snapshot_time, 'missing_reference', (), (), False, False))
        else:
            topologies.append(Topology(None, context.snapshot_time, 'not_calculated', (), (), False, False))
        if membership_sink is not None:
            for row in (*rows.values(), *vps.values()):
                reference = membership_sink(row)
                if not reference:
                    raise ValueError('成员未返回外置引用')
                row.membership_ref = reference
                for name in ('ipv4_prefix', 'ipv6_prefix', 'vp_set', 'private_as', 'public_as', 'path'):
                    setattr(row, name, set())
        candidate.last_context = context
        self.state = candidate
        self._last_time = context.snapshot_time
        return ResourceResult(context, RULE, tuple(deepcopy(list(rows.values()))), tuple(deepcopy(list(vps.values()))), tuple(topologies), tuple(decisions), self.export_state(), decision_sink is not None)

    def _topology_path(self, path, country_edges):
        if not path or '{' in path or len(path.split(' ')) < 2:
            return False
        prev = None
        missing = False
        for token in path.split(' '):
            try:
                asn = int(token)
            except ValueError:
                continue
            if '_' in token or 64512 <= asn <= 65535 or 4200000000 <= asn <= 4294967294:
                continue
            if prev is not None and prev != token:
                c1 = self.references.get(prev, ReferenceAs()).country_cn
                c2 = self.references.get(token, ReferenceAs()).country_cn
                missing |= not c1 or not c2
                if c1 and c1 == c2 and int(prev) != asn:
                    country_edges.setdefault(c1, set()).add(tuple(sorted((int(prev), asn))))
            prev = token
        return bool(missing)
