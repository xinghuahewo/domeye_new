"""S2：消费已保存后态和失效；一条流服务同基线的一组cohort。"""
from dataclasses import dataclass, replace
from fractions import Fraction
from ipaddress import ip_network, collapse_addresses
from typing import Callable, Iterable, Optional, Tuple, Union
from data_pipeline.analysis.country_events.cohort import Cohort
from data_pipeline.analysis.country_events.incremental_types import InputRequest, DirectionPoint, FirstQualifiedRef, PathPeakQuality
from data_pipeline.analysis.country_events.models import ASSUMPTIONS, RULE_VERSION, Binding, Cursor, Endpoint, Route, Time


@dataclass(frozen=True)
class Change:
    cursor: Cursor
    at: Time
    source_id: str
    reference: str
    after: Optional[Route] = None
    invalidated_objects: Tuple[str, ...] = ()
    gap: bool = False
    local: bool = False

    def __post_init__(self):
        if not self.reference or not self.source_id:
            raise ValueError('变化必须有原始引用')
        if self.after is not None and (self.after.cursor != self.cursor or self.after.observed_at != self.at or self.after.observation_ref != self.reference):
            raise ValueError('后态和变化的时间/游标冲突')
        if self.after is not None and (self.gap or self.invalidated_objects):
            raise ValueError('变化与失效应为独立输入行')

    @property
    def global_invalidation(self):
        return self.gap and not self.invalidated_objects


@dataclass(frozen=True)
class Segment:
    kind: str
    asns: Tuple[int, ...]

    def __post_init__(self):
        if self.kind not in ('sequence', 'set', 'confed_sequence', 'confed_set') or not self.asns or any(type(a) is not int or not 1 <= a <= 4294967295 for a in self.asns):
            raise ValueError('路径segment无效')


@dataclass(frozen=True)
class Path:
    path_ref: str
    segments: Tuple[Segment, ...]
    raw_attributes: bytes
    as4_segments: Tuple[Segment, ...] = ()
    relationship_ref: Optional[str] = None


@dataclass(frozen=True)
class Grid:
    input_start_us: int
    input_end_us: int
    samples_us: Tuple[int, ...]

    def __post_init__(self):
        if any(type(t) is not int for t in (self.input_start_us, self.input_end_us, *self.samples_us)):
            raise ValueError('窗口和采样必须为整数微秒坐标')
        if self.input_start_us >= self.input_end_us or not self.samples_us:
            raise ValueError('半开窗口和采样网格不能为空')
        if any(a >= b for a, b in zip(self.samples_us, self.samples_us[1:])):
            raise ValueError('采样网格必须严格递增')
        if self.samples_us[0] <= self.input_start_us or self.samples_us[-1] > self.input_end_us:
            raise ValueError('采样必须在输入范围内，右端只表示左极限')


@dataclass(frozen=True)
class PrefixPoint:
    cohort_id: str
    sample_us: int
    prefix: str
    state: str
    present: int
    absent: int
    unknown: int
    expected: Optional[int]
    evidence_refs: Tuple[str, ...]
    fact_state: str = 'unknown'


@dataclass(frozen=True)
class AsnPoint:
    cohort_id: str
    sample_us: int
    asn: int
    afi: int
    state: str
    normal: int
    partial: int
    complete: int
    unknown: int
    prefix_refs: Tuple[str, ...]


@dataclass(frozen=True)
class MetricPoint:
    cohort_id: str
    sample_us: int
    metric: str
    unit: str
    value: Optional[Fraction]
    known_lower_bound: Fraction
    denominator: Optional[int]
    ratio: Optional[Fraction]
    state: str
    fact_value: Optional[Fraction] = None
    fact_state: str = 'unknown'
    assumptions: Tuple[str, ...] = ASSUMPTIONS


@dataclass(frozen=True)
class NewPrefixPoint:
    cohort_id: str
    sample_us: int
    prefix: str
    first_observed_us: int
    first_observation_ref: str
    visible: bool
    state: str
    origin_refs: Tuple[Tuple[Optional[int], str], ...]


@dataclass(frozen=True)
class ObservationFact:
    reference: str
    route: Route
    local: bool
    semantics: str = 'at_original_observation_only_not_continuous_state'


@dataclass(frozen=True)
class NewDirectionPoint:
    cohort_id: str
    sample_us: int
    prefix: str
    endpoint: Endpoint
    mapping_state: str
    observation_ref: str
    observed_at: Time
    semantics: str = 'outside_fixed_denominator_not_new_peer_claim'


@dataclass(frozen=True)
class PathSample:
    cohort_id: str
    sample_us: int
    prefix: str
    affected_asn: int
    downstream_asn: int
    affected_position: int
    downstream_position: int
    path_ref: str
    observation_ref: str
    observed_at: Time
    peer_ref: str
    mapping_ref: str
    state_refs: Tuple[str, ...]
    relationship_ref: Optional[str]
    path_basis: str
    semantics: str = 'ordered_path_association_not_dependency_or_cause'


@dataclass(frozen=True)
class PathSummary:
    cohort_id: str
    affected_asn: int
    downstream_asn: int
    observed_path_count: int
    route_observation_count: int
    independent_direction_count: int
    associated_prefix_count: int
    concurrent_sample_count: int
    first_concurrent_us: Optional[int]
    last_concurrent_us: Optional[int]
    sample_refs: Tuple[Tuple[str, str, int], ...]
    peak_concurrent_prefix_count: int
    peak_concurrent_ipv4_addresses: Fraction
    peak_concurrent_ipv6_slash48: Fraction


@dataclass(frozen=True)
class WindowClass:
    cohort_id: str
    asn: int
    afi: int
    classification: str
    observed_slots: int
    unknown_slots: int
    included_reason: str


@dataclass(frozen=True)
class Peak:
    cohort_id: str
    metric: str
    unit: str
    value: Fraction
    first_sample_us: int
    occurrence_count: int
    observed_slots: int
    unknown_slots: int


@dataclass(frozen=True)
class Quality:
    cohort_id: str
    code: str
    reference: str


@dataclass(frozen=True)
class Completion:
    cohort_id: str
    sample_count: int
    data_through_us: int
    left_censored: bool
    end_state: str
    input_window: Tuple[int, int]
    sample_window: Optional[Tuple[int, int]]


Row = Union[PrefixPoint, AsnPoint, MetricPoint, NewPrefixPoint, PathSample, PathSummary,
            WindowClass, Peak, Quality, Completion, ObservationFact, NewDirectionPoint, DirectionPoint, FirstQualifiedRef, PathPeakQuality]


@dataclass(frozen=True)
class Batch:
    binding: Binding
    rule_version: str
    rows: Tuple[Row, ...]


def resource(prefixes, afi):
    nets = [ip_network(p) for p in set(prefixes) if (ip_network(p).version == 4) == (afi == 1)]
    size = sum(n.num_addresses for n in collapse_addresses(nets))
    return Fraction(size, 1 if afi == 1 else 1 << 80)


def _classify(states):
    if not states or 'unknown' in states:
        return 'unknown'
    if all(s == 'normal' for s in states):
        return 'normal'
    if all(s == 'complete' for s in states):
        return 'route_interrupted'
    return 'affected'


def _reference_scope(function):
    # 引用域随生成器启动而分配，完成、close、异常均只清理本调用。
    from functools import wraps
    @wraps(function)
    def scoped(*args, _change_refs_factory=None, **kwargs):
        refs = None if _change_refs_factory is None else _change_refs_factory()
        primary = None
        try:
            yield from function(*args, _change_refs=refs, **kwargs)
        except BaseException as error:
            primary = error
            raise
        finally:
            if refs is not None:
                try: refs.close()
                except BaseException as error:
                    if primary is None: raise
                    primary.cleanup_errors = (*getattr(primary, 'cleanup_errors', ()), error)
    return scoped


@_reference_scope
def compute_country_enhancement(cohorts: Tuple[Cohort, ...], binding: Binding, grid: Grid,
                                changes: Iterable[Change], paths: Iterable[Path], *,
                                batch_rows=256, batch_bytes=4*1024**2, max_objects=100000, max_relations=100000, max_changes=1000000,
                                guard: Callable[[], None] = lambda: None,
                                _incremental=False, _shared_paths=None, _shared_refs=None, _context_routes=(), _initial_global_gap=False,
                                _before_current_insert: Callable[[], None] = lambda: None, _change_refs=None):
    """结果只在Completion之后算完成；异常后调用方必须丢弃本次候选输出。

    同一调用绑定同一基线与固定输入，多个国家共享当前对象索引。不同基线组须
    显式分别调用；不支持把各时点快照拼成同一全局当前状态。
    """
    guard()
    if not cohorts or batch_rows < 1 or batch_bytes < 1 or max_objects < 1 or max_relations < 1 or max_changes < 1:
        raise ValueError('cohort及资源上限无效')
    if any(c.rule_version != RULE_VERSION for c in cohorts):
        raise ValueError('不支持输入cohort规则版本')
    if len({c.cohort_id for c in cohorts}) != len(cohorts):
        raise ValueError('重复cohort')
    baselines = {c.baseline for c in cohorts if c.baseline is not None}
    if len(baselines) > 1 or any(b.binding != binding for b in baselines):
        raise ValueError('本次计算必须绑定同一基线及版本')
    baseline = next(iter(baselines), None)
    if baseline is not None and grid.samples_us and baseline.at.upper_us >= grid.samples_us[0]:
        raise ValueError('首个采样必须晚于基线原时间区间')
    path_index = {} if _shared_paths is None else _shared_paths
    for path in paths:
        guard()
        if not path.path_ref or path.path_ref in path_index or len(path_index) >= max_objects:
            raise ValueError('路径引用重复或超限')
        path_index[path.path_ref] = path
    def validate_path(route):
        if (route.presence == 'present' and route.path_ref is None) or (route.path_ref is not None and route.path_ref not in path_index):
            raise ValueError('适用路由路径缺字典引用')

    current = {}
    baseline_facts = {}
    object_keys = {}
    path_evidence = {}
    path_evidence_by_prefix = {}
    invalidation_refs = {}
    global_gap = _initial_global_gap
    for cohort in cohorts:
        for route in (*cohort.routes, *cohort.excluded_unknown, *_context_routes):
            validate_path(route)
            if route.observation_ref in baseline_facts and baseline_facts[route.observation_ref] != route:
                raise ValueError('基线观察引用内容冲突')
            baseline_facts[route.observation_ref] = route
            route_key = (route.endpoint, route.prefix, route.path_id)
            if route.object_id in object_keys and object_keys[route.object_id] != route_key:
                raise ValueError('同对象ID不能改变路由键')
            object_keys[route.object_id] = route_key
            if route.local_message:
                continue
            if route.object_id in current and current[route.object_id] != route:
                raise ValueError('同对象的基线状态冲突')
            if route.object_id not in current:
                _before_current_insert()
            current[route.object_id] = route
            if route.presence == 'present':
                path_evidence[route.observation_ref] = (route, 'baseline')
                path_evidence_by_prefix.setdefault(route.prefix, {})[route.observation_ref] = (route, 'baseline')
    route_key_ids = {(r.endpoint, r.prefix, r.path_id): r.object_id for r in current.values()}
    if len(route_key_ids) != len(current):
        raise ValueError('基线对象键重复')
    if len(baseline_facts) > max_objects or len(object_keys) > max_objects or len(current) > max_objects:
        raise ValueError('基线状态超过对象上限')
    # 原对象键只存一次，固定方向/前缀成员索引不保存整份历史。
    fixed = {c.cohort_id: {r.direction for r in c.routes if r.presence == 'present' and r.mapping_state == 'bound'} for c in cohorts}
    ambiguous_prefixes = {c.cohort_id: {r.prefix for r in c.routes if r.mapping_state != 'bound' or r.presence == 'unknown'} for c in cohorts}
    expected_by_prefix = {}
    for cid, directions in fixed.items():
        expected_by_prefix[cid] = {}
        for direction in directions:
            expected_by_prefix[cid].setdefault(direction[1], []).append(direction)
    refs = {c.cohort_id: ({r.asn: r for r in c.references} if _shared_refs is None else _shared_refs) for c in cohorts}
    new_seen = {c.cohort_id: {} for c in cohorts}
    new_directions = {c.cohort_id: {} for c in cohorts}
    window = {}; peaks = {}; relations = {}; sample_counts = {}; sample_ranges = {}
    pending = []
    retained_evidence_count = 0
    pending_bytes = 0

    def emit(row):
        nonlocal pending_bytes
        guard()
        size = len(repr(row).encode('utf-8'))
        if size > batch_bytes:
            raise ValueError('单条输出超过批次字节上限')
        result = []
        if pending and (len(pending) >= batch_rows or pending_bytes + size > batch_bytes):
            result.append(Batch(binding, RULE_VERSION, tuple(pending)))
            pending.clear(); pending_bytes = 0
        pending.append(row); pending_bytes += size
        return tuple(result)

    for route in sorted(baseline_facts.values(), key=lambda r: (r.object_id, r.observation_ref)):
        yield from emit(ObservationFact(route.observation_ref, route, route.local_message))

    iterator = iter(changes)
    last_cursor = baseline.cursor if baseline else None
    last_time = baseline.at.lower_us if baseline else None
    changes_read = 0
    change_refs = set() if _change_refs is None else _change_refs

    def next_change():
        nonlocal last_cursor, last_time, changes_read
        if _incremental:
            change = yield InputRequest()
            if change is None:
                return None
        else:
            try:
                change = next(iterator)
            except StopIteration:
                return None
        guard()
        changes_read += 1
        if change.reference in change_refs:
            raise ValueError('变化原始引用重复')
        change_refs.add(change.reference)
        if _change_refs is None and changes_read > max_changes:
            raise ValueError('输入变化超过显式上限')
        if change.after is not None and change.after.endpoint.collector != binding.collector:
            raise ValueError('变化Collector与绑定版本冲突')
        if binding.source_at(change.cursor.source_rank) != change.source_id:
            raise ValueError('变化来源与绑定清单冲突')
        if last_cursor is not None and change.cursor <= last_cursor:
            raise ValueError('变化游标倒序或重复')
        if not _incremental and last_time is not None and change.at.lower_us < last_time:
            raise ValueError('原时间回退，不能用重排伪造采样状态')
        last_cursor, last_time = change.cursor, change.at.lower_us
        return change

    def consume_change(change):
        nonlocal global_gap
        if change.after is not None:
            route = change.after
            validate_path(route)
            route_key = (route.endpoint, route.prefix, route.path_id)
            if route.object_id in object_keys and object_keys[route.object_id] != route_key:
                raise ValueError('同对象ID不能改变路由键')
            if route.object_id not in object_keys and len(object_keys) >= max_objects:
                raise ValueError('输入对象身份超过显式上限')
            object_keys[route.object_id] = route_key
            yield from emit(ObservationFact(change.reference, change.after, change.local or change.after.local_message))
        if not change.local and not (change.after is not None and change.after.local_message):
            if change.after is not None:
                route = change.after
                prior = current.get(route.object_id)
                if prior is not None and (prior.endpoint, prior.prefix, prior.path_id) != (route.endpoint, route.prefix, route.path_id):
                    raise ValueError('同对象ID不能改变路由键')
                route_key = (route.endpoint, route.prefix, route.path_id)
                if prior is None and route_key in route_key_ids:
                    raise ValueError('同路由键重复对象身份')
                if route.object_id not in current and len(current) >= max_objects:
                    raise ValueError('动态状态超过对象上限')
                if route.object_id not in current:
                    _before_current_insert()
                current[route.object_id] = route
                route_key_ids[route_key] = route.object_id
                invalidation_refs.pop(route.object_id, None)
                if route.presence == 'present':
                    if route.observation_ref not in path_evidence and len(path_evidence) >= max_objects:
                        raise ValueError('路径观察证据超过显式上限')
                    path_evidence[route.observation_ref] = (route, 'observed_history')
                    path_evidence_by_prefix.setdefault(route.prefix, {})[route.observation_ref] = (route, 'observed_history')
                for c in cohorts:
                    ref = refs[c.cohort_id].get(route.origin)
                    if route.prefix in c.prefixes and route.direction not in fixed[c.cohort_id]:
                        new_directions[c.cohort_id].setdefault(route.direction, route)
                    if route.presence == 'present' and ref is not None and ref.state == 'known' and ref.country == c.incident.country and route.prefix not in c.prefixes:
                        if _incremental and route.prefix not in new_seen[c.cohort_id]:
                            yield from emit(FirstQualifiedRef(c.cohort_id,route.prefix,change.reference,change.at.lower_us,((route.origin,route.observation_ref),),route.cursor,ref.source_ref))
                        new_seen[c.cohort_id].setdefault(route.prefix, (change.at.lower_us, change.reference))
            else:
                if change.global_invalidation:
                    global_gap = True
                targets = tuple(current) if change.global_invalidation else change.invalidated_objects
                for object_id in targets:
                    if object_id in current:
                        invalidation_refs[object_id] = change.reference
                        current[object_id] = replace(current[object_id], presence='unknown', fact_presence='unknown')
                for c in cohorts:
                    yield from emit(Quality(c.cohort_id, 'source_gap' if change.gap else 'scoped_invalidation', change.reference))

    change = yield from next_change()
    for sample in grid.samples_us:
        guard()
        while change is not None and change.at.lower_us < sample:
            if change.at.upper_us >= sample:
                raise ValueError('原时间精度不能确定采样左极限，拒绝猜测')
            yield from consume_change(change)
            change = yield from next_change()
        # 只构造本采样的索引，不缓存全部时点矩阵。
        by_direction = {}; by_prefix = {}
        for route in current.values():
            guard()
            by_direction.setdefault(route.direction, []).append(route)
            by_prefix.setdefault(route.prefix, []).append(route)
        for c in cohorts:
            anchor = c.incident.onset or c.incident.detected_at
            if sample <= anchor.lower_us or (c.incident.end is not None and sample > c.incident.end.lower_us):
                continue
            cid = c.cohort_id
            sample_counts[cid] = sample_counts.get(cid, 0) + 1
            sample_ranges.setdefault(cid, [sample, sample])[1] = sample
            if c.baseline is None:
                yield from emit(Quality(cid, 'baseline_unavailable', c.incident.incident_id))
                continue
            for direction, first_route in sorted(new_directions[cid].items(), key=repr):
                yield from emit(NewDirectionPoint(cid, sample, first_route.prefix, first_route.endpoint, first_route.mapping_state, first_route.observation_ref, first_route.observed_at))
            points = {}
            for prefix in c.prefixes:
                guard()
                counts = {'present': 0, 'absent': 0, 'unknown': 0}
                evidence = []
                for direction in sorted(expected_by_prefix[cid].get(prefix, ()), key=repr):
                    routes = by_direction.get(direction, [])
                    states = {r.presence for r in routes if r.mapping_state == 'bound'}
                    state = 'present' if 'present' in states else ('absent' if states == {'absent'} else 'unknown')
                    counts[state] += 1
                    if _incremental:
                        yield from emit(DirectionPoint(cid,sample,prefix,direction[0], 'unknown' if global_gap else state,
                                                       tuple(sorted(r.object_id for r in routes)),
                                                       tuple(sorted({r.observation_ref for r in routes} | {invalidation_refs[r.object_id] for r in routes if r.object_id in invalidation_refs}))))
                    evidence.extend(r.observation_ref for r in routes)
                    evidence.extend(invalidation_refs[r.object_id] for r in routes if r.object_id in invalidation_refs)
                ambiguous = prefix in ambiguous_prefixes[cid]
                total = sum(counts.values())
                state = 'unknown'
                if not global_gap and not ambiguous and total and not counts['unknown']:
                    state = 'normal' if counts['present'] == total else ('complete' if counts['absent'] == total else 'partial')
                point = PrefixPoint(cid, sample, prefix, state, counts['present'], counts['absent'], counts['unknown'],
                                    None if ambiguous else total, tuple(sorted(set(evidence))))
                points[prefix] = point
                yield from emit(point)
            asn_prefixes = {}
            for prefix, origin, _ in c.origins:
                if origin is not None:
                    afi = 1 if ip_network(prefix).version == 4 else 2
                    asn_prefixes.setdefault((origin, afi), set()).add(prefix)
                    asn_prefixes.setdefault((origin, 0), set()).add(prefix)
            asn_points = []
            for (asn, afi), prefixes in sorted(asn_prefixes.items()):
                states = [points[p].state for p in sorted(prefixes)]
                state = _classify(states)
                row = AsnPoint(cid, sample, asn, afi, state, states.count('normal'), states.count('partial'),
                               states.count('complete'), states.count('unknown'), tuple(sorted(prefixes)))
                asn_points.append(row); yield from emit(row)
                ledger = window.setdefault((cid, asn, afi), {'normal': 0, 'affected': 0, 'route_interrupted': 0, 'unknown': 0})
                ledger[state] += 1
            metrics = []
            prefix_denominator = None if 'baseline_visibility_unknown' in c.reasons else len(points)
            for name, state in (('normal_prefix_count', 'normal'), ('partially_interrupted_prefix_count', 'partial'),
                                ('completely_interrupted_prefix_count', 'complete')):
                known = sum(p.state == state for p in points.values())
                value = None if any(p.state == 'unknown' for p in points.values()) else Fraction(known)
                metrics.append((name, 'prefix_count', value, Fraction(known), prefix_denominator))
            known_interrupted = sum(p.state in ('partial', 'complete') for p in points.values())
            metrics.append(('interrupted_prefix_count', 'prefix_count', None if any(p.state == 'unknown' for p in points.values()) else Fraction(known_interrupted), Fraction(known_interrupted), prefix_denominator))
            known = sum(p.absent for p in points.values())
            value = None if global_gap or c.direction_count is None or any(p.unknown for p in points.values()) else Fraction(known)
            metrics.append(('invisible_direction_count', 'endpoint_direction_count', value, Fraction(known), c.direction_count))
            present_count = sum(p.present for p in points.values())
            metrics.append(('visible_direction_count', 'endpoint_direction_count', None if global_gap or c.direction_count is None or any(p.unknown for p in points.values()) else Fraction(present_count), Fraction(present_count), c.direction_count))
            for label in ('normal', 'affected', 'route_interrupted'):
                known = sum(a.state == label and a.afi == 0 for a in asn_points)
                value = None if any(a.state == 'unknown' and a.afi == 0 for a in asn_points) else Fraction(known)
                metrics.append((label+'_asn_count', 'asn_count', value, Fraction(known), None if prefix_denominator is None else sum(a.afi == 0 for a in asn_points)))
            for afi, unit in ((1, 'ipv4_address_count'), (2, 'ipv6_slash48_equivalent')):
                visible = [p for p, point in points.items() if point.present > 0]
                lower = resource(visible, afi)
                uncertain = any(p.present == 0 and (p.unknown or p.expected is None) for p in points.values() if (ip_network(p.prefix).version == 4) == (afi == 1))
                family_ambiguous = any((ip_network(p).version == 4) == (afi == 1) for p in ambiguous_prefixes[cid])
                value = None if uncertain or global_gap or family_ambiguous else lower
                metrics.append(('fixed_visible_'+unit, unit, value, lower, None))
                new_prefixes = new_seen[cid]
                new_visible = []
                for prefix, (first_at, first_ref) in sorted(new_prefixes.items()):
                    if (ip_network(prefix).version == 4) != (afi == 1):
                        continue
                    rows = by_prefix.get(prefix, [])
                    visible_now = any(r.presence == 'present' and r.mapping_state == 'bound' for r in rows)
                    unknown_now = any(r.presence == 'unknown' or r.mapping_state != 'bound' for r in rows)
                    new_state = 'present' if visible_now else ('unknown' if unknown_now else 'absent')
                    if visible_now: new_visible.append(prefix)
                    yield from emit(NewPrefixPoint(cid, sample, prefix, first_at, first_ref, visible_now, new_state,
                                                   tuple(sorted(((r.origin, r.observation_ref) for r in rows), key=repr))))
                family_new = [p for p in new_prefixes if (ip_network(p).version == 4) == (afi == 1)]
                unresolved = any(not any(r.presence == 'present' and r.mapping_state == 'bound' for r in by_prefix.get(p, [])) and
                                 any(r.presence == 'unknown' or r.mapping_state != 'bound' for r in by_prefix.get(p, [])) for p in family_new)
                lower_new = resource(new_visible, afi)
                metrics.extend([(f'new_visible_{unit}', unit, None if unresolved or global_gap else lower_new, lower_new, None),
                                (f'new_cumulative_{unit}', unit, None if global_gap else resource(family_new, afi), resource(family_new, afi), None),
                                (f'new_visible_ipv{4 if afi == 1 else 6}_prefix_count', 'prefix_count', None if unresolved or global_gap else Fraction(len(new_visible)), Fraction(len(new_visible)), None),
                                (f'new_cumulative_ipv{4 if afi == 1 else 6}_prefix_count', 'prefix_count', None if global_gap else Fraction(len(family_new)), Fraction(len(family_new)), None)])
            for name, unit, value, lower, denominator in metrics:
                ratio = value / denominator if value is not None and denominator else None
                row = MetricPoint(cid, sample, name, unit, value, lower, denominator, ratio,
                                  'unknown' if value is None else ('calculated_zero' if value == 0 else 'calculated'))
                yield from emit(row)
                ledger = peaks.setdefault((cid, name, unit), [None, None, 0, 0, 0])
                if value is None: ledger[4] += 1
                else:
                    ledger[3] += 1
                    if ledger[0] is None or value > ledger[0]: ledger[0:3] = [value, sample, 1]
                    elif value == ledger[0]: ledger[2] += 1
            # 历史路径提供关联证据；样本basis不得冒充当前可见路径。
            affected = {a.asn for a in asn_points if a.afi == 0 and a.state in ('affected', 'route_interrupted')}
            for prefix, point in sorted(points.items()):
                for route, basis in sorted(path_evidence_by_prefix.get(prefix, {}).values(), key=lambda item: (item[0].cursor, item[0].observation_ref)):
                    guard()
                    if route.prefix != prefix or route.mapping_state != 'bound' or route.direction not in fixed[cid]:
                        continue
                    if route.path_ref not in path_index:
                        raise ValueError('当前路径缺字典引用')
                    path = path_index[route.path_ref]
                    positions = []; offset = 0
                    # 不越过AS_SET/confed推断位置关联；完整原件留在输入Path。
                    for segment in path.segments:
                        if segment.kind == 'sequence':
                            positions.extend((offset + j, asn) for j, asn in enumerate(segment.asns))
                        else:
                            positions = []
                            break
                        offset += len(segment.asns)
                    for index, (pos, asn) in enumerate(positions):
                        if (asn, route.endpoint.afi) not in asn_prefixes:
                            continue
                        for down_pos, downstream in positions[index+1:]:
                            if downstream == asn: continue
                            key = (cid, asn, downstream)
                            if key not in relations and len(relations) >= max_relations:
                                raise ValueError('路径关系超过显式上限')
                            rel = relations.setdefault(key, {'paths': set(), 'observations': set(), 'directions': set(), 'prefixes': set(), 'concurrent': set(), 'samples': {}, 'sample_prefixes': set(), 'sample': sample, 'peak_prefix': 0, 'peak_v4': Fraction(0), 'peak_v6': Fraction(0), 'known_slots':0, 'unknown_slots':0})
                            if rel['sample'] != sample:
                                rel['sample_prefixes'].clear(); rel['sample'] = sample
                            retained_evidence_count += int(path.path_ref not in rel['paths']) + int(route.observation_ref not in rel['observations']) + int(route.direction not in rel['directions']) + int(prefix not in rel['prefixes']) + int(asn in affected and point.state in ('partial', 'complete') and sample not in rel['concurrent'])
                            if retained_evidence_count > max_objects:
                                raise ValueError('路径关系证据集合超过显式上限')
                            rel['paths'].add(path.path_ref); rel['observations'].add(route.observation_ref)
                            rel['directions'].add(route.direction); rel['prefixes'].add(prefix)
                            if asn in affected and point.state in ('partial', 'complete'):
                                rel['concurrent'].add(sample); rel['sample_prefixes'].add(prefix)
                            sample_key = (path.path_ref, route.observation_ref, pos, down_pos)
                            if sample_key not in rel['samples'] and len(rel['samples']) < 3:
                                rel['samples'][sample_key] = sample
                                yield from emit(PathSample(cid, sample, prefix, asn, downstream, pos, down_pos, path.path_ref,
                                                           route.observation_ref, route.observed_at, route.peer_ref, route.mapping_ref,
                                                           point.evidence_refs, path.relationship_ref, basis))
            for key, rel in relations.items():
                if key[0] == cid and rel['sample'] == sample:
                    unresolved = global_gap or any(points[p].state == 'unknown' for p in rel['prefixes'])
                    rel['unknown_slots' if unresolved else 'known_slots'] += 1
                    rel['peak_prefix'] = max(rel['peak_prefix'], len(rel['sample_prefixes']))
                    rel['peak_v4'] = max(rel['peak_v4'], resource(rel['sample_prefixes'], 1))
                    rel['peak_v6'] = max(rel['peak_v6'], resource(rel['sample_prefixes'], 2))
    # 验证整个声明输入范围，右端消息不进入state(t-)；尾部倒序同样使本次失败。
    while change is not None:
        if change.at.lower_us < grid.input_end_us:
            if not _incremental and change.at.upper_us >= grid.input_end_us:
                raise ValueError('原时间精度不能确定输入右端，拒绝猜测')
            yield from consume_change(change)
        change = yield from next_change()
    for (cid, asn, afi), counts in sorted(window.items()):
        label = 'route_interrupted' if counts['route_interrupted'] else ('affected' if counts['affected'] else ('normal' if counts['normal'] else 'unknown'))
        yield from emit(WindowClass(cid, asn, afi, label, sum(counts.values())-counts['unknown'], counts['unknown'], 'ever_'+label))
    for (cid, name, unit), (value, first, count, observed, unknown) in sorted(peaks.items()):
        if value is not None:
            yield from emit(Peak(cid, name, unit, value, first, count, observed, unknown))
    for (cid, asn, downstream), rel in sorted(relations.items()):
        concurrent = rel['concurrent']
        yield from emit(PathSummary(cid, asn, downstream, len(rel['paths']), len(rel['observations']), len(rel['directions']),
                                    len(rel['prefixes']), len(concurrent), min(concurrent) if concurrent else None,
                                    max(concurrent) if concurrent else None,
                                    tuple((k[0], k[1], v) for k, v in rel['samples'].items()), rel['peak_prefix'], rel['peak_v4'], rel['peak_v6']))
        if _incremental:
            yield from emit(PathPeakQuality(cid,asn,downstream,rel['known_slots'],rel['unknown_slots'],
                                           None if rel['unknown_slots'] else rel['peak_prefix'],
                                           None if rel['unknown_slots'] else rel['peak_v4'],
                                           None if rel['unknown_slots'] else rel['peak_v6']))
    for c in cohorts:
        sample_range = sample_ranges.get(c.cohort_id)
        yield from emit(Completion(c.cohort_id, sample_counts.get(c.cohort_id, 0), sample_range[1] if sample_range else grid.input_start_us,
                                   c.incident.carry_in_state == 'unknown', 'known' if c.incident.end else 'unknown',
                                   (grid.input_start_us, grid.input_end_us), tuple(sample_range) if sample_range else None))
    if pending:
        yield Batch(binding, RULE_VERSION, tuple(pending))
