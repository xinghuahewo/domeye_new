"""S1：一次分组基线迭代冻结选定国家集合，不将动态成员加入分母。"""
from dataclasses import asdict, dataclass
from hashlib import sha256
from itertools import groupby
import json
from typing import Callable, Iterable, Optional, Tuple
from data_pipeline.analysis.country_events.models import Baseline, Incident, Reference, Route, RULE_VERSION


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def select_baseline(incident: Incident, candidates: Iterable[Baseline]) -> Optional[Baseline]:
    """有cursor按cursor选前态；只有时间则严格早于已知精度区间。"""
    candidates = tuple(candidates)
    if len({b.binding for b in candidates}) > 1:
        raise ValueError('基线候选不可混版')
    if incident.onset_cursor is not None and candidates and candidates[0].binding.source_at(incident.onset_cursor.source_rank) is None:
        raise ValueError('起始游标未绑定来源')
    boundary = incident.onset or incident.detected_at
    if incident.onset_cursor is not None:
        allowed = [b for b in candidates if b.cursor < incident.onset_cursor and b.at.lower_us <= boundary.upper_us]
        key = lambda b: b.cursor
    else:
        allowed = [b for b in candidates if b.at.upper_us < boundary.lower_us]
        key = lambda b: (b.at.lower_us, b.cursor)
    if not allowed:
        return None
    winner = max(allowed, key=key)
    if sum(key(b) == key(winner) for b in allowed) > 1:
        raise ValueError('基线候选同位置歧义')
    return winner


@dataclass(frozen=True)
class Cohort:
    cohort_id: str
    incident: Incident
    baseline: Optional[Baseline]
    baseline_kind: str
    routes: Tuple[Route, ...]
    prefixes: Tuple[str, ...]
    origins: Tuple[Tuple[str, Optional[int], str], ...]
    references: Tuple[Reference, ...]
    excluded_unknown: Tuple[Route, ...]
    direction_count: Optional[int]
    known_direction_count: int
    state: str
    reasons: Tuple[str, ...]
    rule_version: str = RULE_VERSION


def freeze_cohorts(incidents: Tuple[Incident, ...], baseline: Optional[Baseline],
                   routes: Iterable[Route], references: Iterable[Reference], *,
                   max_routes=100000, guard: Callable[[], None] = lambda: None):
    """输入按prefix严格分组。多事件共享一次输入扫描；仅保留其成员和未归属证据。"""
    guard()
    if max_routes < 1 or len({i.incident_id for i in incidents}) != len(incidents):
        raise ValueError('限额或重复事件无效')
    refs = {}
    for ref in references:
        guard()
        if ref.asn in refs:
            raise ValueError('国家参考必须先显式解决重复/冲突')
        if len(refs) >= max_routes:
            raise ValueError('参考超过显式对象限额')
        refs[ref.asn] = ref
    if baseline is None:
        return tuple(Cohort(digest({'incident': asdict(i), 'references': [asdict(refs[a]) for a in sorted(refs)], 'rule': RULE_VERSION, 'state': 'baseline_unavailable'}), i, None, 'unavailable', (), (), (), tuple(refs[a] for a in sorted(refs)),
                            (), None, 0, 'baseline_unavailable', ('baseline_unavailable',)) for i in incidents)
    for i in incidents:
        if select_baseline(i, [baseline]) is None:
            raise ValueError('基线不在事件已知边界之前；不得回退较晚基线')
    selected = {i.incident_id: [] for i in incidents}
    unknown = []
    seen_objects = set()
    seen_keys = set()
    previous = None
    count = 0
    for prefix, group in groupby(routes, key=lambda r: r.prefix):
        guard()
        if previous is not None and prefix <= previous:
            raise ValueError('基线必须按prefix分组排序')
        previous = prefix
        block = []
        for route in group:
            guard(); count += 1
            if count > max_routes:
                raise ValueError('基线输入超过显式对象限额')
            if route.endpoint.collector != baseline.binding.collector:
                raise ValueError('路由Collector与基线冲突')
            if route.observed_at.upper_us > baseline.at.upper_us:
                raise ValueError('基线含晚于声明时点的观察')
            if route.object_id in seen_objects or route.cursor > baseline.cursor:
                raise ValueError('重复路由或基线含未来游标')
            route_key = (route.endpoint, route.prefix, route.path_id)
            if route_key in seen_keys and not route.local_message:
                raise ValueError('同路由对象键不能有重复身份')
            seen_objects.add(route.object_id)
            if route.local_message:
                unknown.append(route)
                continue
            seen_keys.add(route_key)
            block.append(route)
        countries = {refs[r.origin].country for r in block if r.presence in ('present', 'unknown') and r.origin in refs and refs[r.origin].state == 'known'}
        if not countries:
            unknown.extend(r for r in block if r.origin is None or r.origin not in refs or refs[r.origin].state != 'known')
        for incident in incidents:
            if incident.country in countries:
                selected[incident.incident_id].extend(block)
    results = []
    for i in incidents:
        rows = tuple(sorted(selected[i.incident_id], key=lambda r: r.object_id))
        directions = {r.direction for r in rows if r.presence == 'present' and r.mapping_state == 'bound'}
        ambiguous = any(r.mapping_state != 'bound' or r.presence == 'unknown' for r in rows)
        reasons = tuple(reason for condition, reason in ((any(r.mapping_state != 'bound' for r in rows), 'ambiguous_baseline_mapping'), (any(r.presence == 'unknown' for r in rows), 'baseline_visibility_unknown')) if condition)
        prefixes = tuple(sorted({r.prefix for r in rows}))
        origins = tuple((r.prefix, r.origin, r.observation_ref) for r in rows)
        identity = {'incident': asdict(i), 'baseline': asdict(baseline), 'routes': [asdict(r) for r in rows],
                    'references': [asdict(refs[a]) for a in sorted(refs)],
                    'excluded_unknown': [asdict(r) for r in unknown], 'rule': RULE_VERSION}
        results.append(Cohort(digest(identity), i, baseline, 'pre_onset' if i.onset else 'pre_detection',
                              rows, prefixes, origins, tuple(refs[a] for a in sorted(refs)), tuple(unknown),
                              None if ambiguous else len(directions), len(directions),
                              'qualified' if ambiguous else ('empty' if not rows else 'available'), reasons))
    return tuple(results)
