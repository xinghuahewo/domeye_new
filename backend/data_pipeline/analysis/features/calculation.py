"""普通与 IR Feature 纯计算。输入为显式兼容投影，不解析 MRT 或推进 RouteState。"""

from __future__ import annotations

from copy import deepcopy
from collections.abc import Mapping, Set
from dataclasses import asdict, dataclass, field
from datetime import datetime
from functools import lru_cache
from ipaddress import ip_network
from typing import Callable, Iterable, Literal
from zoneinfo import ZoneInfo

from utils.prefix_quantity import calculate_c_segments_count, calculate_v6_48_segments_count
from data_pipeline.common.prefix_networks import network_facts

Mode = Literal["ordinary", "ir"]
RULES = {"ordinary": "legacy-feature-39578fe/v1", "ir": "legacy-feature-ir-39578fe/v1"}
UNITS = {"v4Prefix_num": "ipv4_24_union_blocks", "v6Prefix_num": "ipv6_48_union_blocks",
         "v4IP_num": "ipv4_24_union_blocks_x256", "announ_num": "accepted_announce_elements",
         "withdraw_num": "accepted_withdraw_elements"}


@dataclass(frozen=True)
class ReferenceRow:
    source_ref: str
    asn: str
    country_name: str | None
    country_code: str | None


@dataclass(frozen=True)
class Reference:
    version: str
    country_names: dict[str, str | None]
    country_codes: dict[str, str | None]
    big_countries: dict[str, str]
    source_rows: tuple[ReferenceRow, ...] = ()

    @classmethod
    def from_rows(cls, version: str, rows: tuple[ReferenceRow, ...], big_countries: dict[str, str]):
        # BGPInfo.load_info drop_duplicates(asn, keep='first')；保留全部输入行供审计。
        names, codes = {}, {}
        for row in rows:
            if row.asn not in names:
                names[row.asn], codes[row.asn] = row.country_name, row.country_code
        return cls(version, names, codes, dict(big_countries), rows)

    def country(self, asn: str) -> str:
        value = self.country_names.get(asn.split("_")[0])
        return "未知" if value in (None, "", "未知") else value

    def code(self, asn: str) -> str | None:
        return self.country_codes.get(asn.split("_")[0])


@dataclass(frozen=True)
class InputGap:
    left_source: str
    right_source: str
    start: datetime
    end: datetime
    basis: str = "declared_input_bounds_not_session_continuity"


@dataclass(frozen=True)
class Projection:
    """兼容状态的完整可导出值；残留索引按原样保存，不纠正为规范事实。"""

    version: str
    rule_id: str
    prefix_dict: Mapping[str, Mapping[str, str]]
    prefix_as: Mapping[str, Set[str]]
    as_prefix: Mapping[str, Set[str]]
    seen_vps: frozenset[str]
    rib_file: str
    rib_time: str
    country_filter: str | None
    coverage: Literal["complete", "partial", "unknown"] = "unknown"
    input_gaps: tuple[InputGap, ...] = ()


@dataclass(frozen=True)
class Observation:
    source_ref: str
    sequence: int
    timestamp: datetime
    flag: str
    raw_prefix: str
    vp: str
    new_path: str | None
    old_vp_path: str | None
    before_origins: frozenset[str]
    after_origins: frozenset[str]
    projection_rule_id: str
    projection_skip_reason: str | None


@dataclass(frozen=True)
class FileWindow:
    input_version: str
    file_ref: str
    start: datetime
    end: datetime
    file_time: datetime
    coverage: Literal["complete", "partial", "unknown"]
    limitations: tuple[str, ...] = ()

    def __post_init__(self):
        if self.coverage not in ("complete", "partial", "unknown") or not self.input_version or not self.file_ref:
            raise ValueError("窗口身份或覆盖状态无效")
        for value in (self.start, self.end, self.file_time):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("窗口和文件时点必须带时区")
        if self.start >= self.end:
            raise ValueError("窗口必须是非空半开区间")
        if self.file_time.microsecond:
            raise ValueError("旧输出 timestamp(0) 要求显式秒精度，不能隐式舍入")


@dataclass
class Values:
    v4Prefix_num: int = 0
    v6Prefix_num: int = 0
    v4IP_num: int = 0
    announ_num: int = 0
    withdraw_num: int = 0


@dataclass
class AsnValues(Values):
    is_change: bool = False


@dataclass(frozen=True)
class LegacyCheckpointMetadata:
    """旧元数据的保留位置；本模块不打开 snapshot_path，也不由此恢复。"""

    version: str | int | None = None
    status: str | None = None
    created_at: str | None = None
    snapshot_path: str | None = None
    last_completed_update_file: str | None = None
    rib_file: str | None = None
    rib_time: str | None = None
    snapshot_format: str | None = None
    feature_name: str | None = None


@dataclass
class WorkState:
    mode: Mode
    execution_id: str
    reference_version: str
    projection: Projection
    feature_dict: dict[str, dict[str, AsnValues]] = field(default_factory=dict)
    feature_collect_dict: Values = field(default_factory=Values)
    t: str = ""
    last_completed_update_file: str | None = None
    last_window_end: datetime | None = None
    completed_files: int = 0
    status: str = "initialized"
    legacy_checkpoint_metadata: LegacyCheckpointMetadata | None = None

    def export(self) -> dict:
        """仅导出，不实现 checkpoint 加载、恢复或文件写入。"""
        def plain(value):
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, Set):
                return sorted(value)
            if isinstance(value, Mapping):
                return {k: plain(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return [plain(v) for v in value]
            return value
        return plain(asdict(self))


@dataclass(frozen=True)
class Diagnostic:
    scope: str
    identifier: str
    reason: str
    raw_prefix: str = ""
    normalized_prefix: str | None = None
    sample_prefixes: tuple[str, ...] = ()


@dataclass(frozen=True)
class FeatureRow:
    scope: Literal["asn", "country", "collect"]
    subject: str
    country: str
    values: Values
    raw_prefixes: frozenset[str]
    ipv4_prefixes: frozenset[str]
    ipv6_prefixes: frozenset[str]
    legacy_table: str | None
    row_presence: Literal["written", "ir_asn_write_disabled"]
    resource_status: Literal["observed_zero", "observed_nonzero", "unknown"]

    def legacy_payload(self, window: FileWindow, source: str) -> dict:
        payload = {"t": window.file_time.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None).isoformat(" "),
                   "source": source, "country": self.country, **asdict(self.values)}
        if self.scope == "asn":
            payload["asn"] = self.subject
        return payload


@dataclass(frozen=True)
class Result:
    rule_id: str
    window: FileWindow
    source: str
    rows: tuple[FeatureRow, ...]
    sparse_asns: tuple[tuple[str, str], ...]
    diagnostics: tuple[Diagnostic, ...]
    end_state: WorkState
    next_state: WorkState
    quality: Literal["complete", "partial", "unknown"]
    calculation_parameters: dict
    projection_audit: tuple[dict, ...] = ()


@dataclass(frozen=True)
class ConsumedWindow:
    """单写入者收尾结果；只冻结窗口计数，不复制整份继续计算状态。"""
    sparse_asns: tuple[tuple[str, str], ...]
    diagnostics: tuple[Diagnostic, ...]
    end_counts: Values
    next_state: WorkState
    quality: Literal["complete", "partial", "unknown"]


class FeatureCalculationError(ValueError):
    """失败不得发布；calculate_file 保留原态，consume_file 调用方必须回滚文件候选。"""

    quality = "failed"


def feature_origin(path: str | None) -> str | None:
    # 保留 Feature 自身的规则，特意不复用 BGPRib/规范 attributed_origin。
    if path in (None, ""):
        return None
    tokens = path.split(" ")
    if "{" in tokens[-1]:
        return None
    for token in reversed(tokens):
        value = int(token)  # 空 token/联盟段等旧异常必须使运行质量可见。
        if 64512 <= value <= 65535 or value > 4294967295:
            continue
        return token
    return None


def prefix_filter(raw: str) -> tuple[str | None, str | None]:
    try:
        text = str(raw).strip()
    except ValueError:
        return None, "invalid_prefix"
    # 同一元素会被投影和计数重复解释；只缓存不可变结果，不缓存路由状态。
    # 长的非法原值仍按原规则解释，但不让其占据共享缓存。
    return _cached_prefix_filter(text) if len(text) <= 128 else _prefix_filter(text)


def _prefix_filter(text: str) -> tuple[str | None, str | None]:
    try:
        network = network_facts(text, strict=False)
    except ValueError:
        return None, "invalid_prefix"
    if network.version == 4 and network.prefixlen < 8:
        return network.text, "oversized_ipv4_prefix"
    if network.version == 6 and network.prefixlen < 16:
        return network.text, "oversized_ipv6_prefix"
    # 旧调用没有传 prefix，声明的两个手工排除实际不生效。
    return network.text, None


_cached_prefix_filter = lru_cache(maxsize=4096)(_prefix_filter)


def observation_filter(mode: Mode, item: Observation, reference: Reference) -> str | None:
    return observation_filter_fields(mode, item.flag, item.raw_prefix, item.new_path, reference)


def observation_filter_fields(mode, flag, raw_prefix, new_path, reference):
    """投影可先用本条必要字段过滤，随后只构造一次带紧邻旧值的观察。"""
    if flag == "STATE":
        return "state_record"
    _, reason = prefix_filter(raw_prefix)
    if reason:
        return reason
    if mode == "ir" and flag == "A":
        origin = feature_origin(new_path)
        if not origin or reference.code(origin) != "IR":
            return "non_ir_announcement"
    return None


def initialize(mode: Mode, projection: Projection, reference: Reference, execution_id: str) -> WorkState:
    _check_projection(mode, projection)
    if not execution_id or not reference.version:
        raise ValueError("缺少执行或参考身份")
    state = WorkState(mode, execution_id, reference.version, deepcopy(projection))
    for asn in projection.as_prefix:
        state.feature_dict.setdefault(reference.country(asn), {})[asn] = AsnValues()
    return state


def _check_projection(mode: Mode, projection: Projection):
    if projection.rule_id != RULES[mode] or not projection.version:
        raise FeatureCalculationError("Feature 投影身份不匹配")
    if projection.country_filter != ("IR" if mode == "ir" else None):
        raise FeatureCalculationError("Feature 基线筛选规则不匹配")
    if projection.coverage not in ("complete", "partial", "unknown"):
        raise FeatureCalculationError("投影覆盖状态无效")


@lru_cache(maxsize=4096)
def _feature_time(timestamp, fold):
    # fold 必须参与键，避免同一时区重复小时的两个不同瞬间被 datetime 相等合并。
    return timestamp.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")


def count_observation(work, reference, window, item, *, on_change=None):
    """共用逐条计数规则；调用方先提供该元素紧邻的旧路径和过滤结果。"""
    if item.timestamp.tzinfo is None or not window.start <= item.timestamp < window.end:
        raise ValueError("观察不属于声明窗口或无时区")
    if item.projection_rule_id != RULES[work.mode]:
        raise ValueError("观察投影规则不匹配")
    reason = observation_filter(work.mode, item, reference)
    if reason != item.projection_skip_reason:
        raise ValueError("投影推进前过滤与 Feature 规则不一致")
    if reason:
        return Diagnostic("update", item.source_ref, reason, item.raw_prefix, prefix_filter(item.raw_prefix)[0])
    work.t = (_feature_time(item.timestamp, item.timestamp.fold) if type(item.timestamp) is datetime
              else item.timestamp.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S"))
    if item.flag not in ("A", "W"):
        return Diagnostic("update", item.source_ref, "unsupported_flag_no_count", item.raw_prefix)
    count_accepted(work,reference,item.flag,item.new_path,item.old_vp_path,on_change=on_change)


def count_accepted(work, reference, flag, new_path, old_path, *, on_change=None):
    """内部顺序循环入口：时间、动作和原过滤已检查，直接累计同一业务字段。"""
    key = "announ_num" if flag == "A" else "withdraw_num"
    setattr(work.feature_collect_dict, key, getattr(work.feature_collect_dict, key) + 1)
    origin = feature_origin(new_path if flag == "A" else old_path)
    if origin:
        country = reference.country(origin)
        try:
            values = work.feature_dict[country]
        except KeyError:
            values = work.feature_dict[country] = {}
        try:
            value = values[origin]
        except KeyError:
            value = values[origin] = AsnValues()
        setattr(value, key, getattr(value, key) + 1)
        value.is_change = True
        if on_change is not None:
            on_change(country, origin)


def calculate_file(state: WorkState, reference: Reference, window: FileWindow,
                   observations: Iterable[Observation], final_projection: Projection | Callable[[], Projection],
                   *, source: str = "r", monthly: bool = True,
                   country_table: str = "feature_country", other_table: str = "feature_other",
                   row_sink=None, retain_rows: bool = True) -> Result:
    """接收同版有序投影输入与末态，返回清零前/后状态；不改变调用方对象。"""
    return _calculate_file(state,reference,window,observations,final_projection,source=source,monthly=monthly,
        country_table=country_table,other_table=other_table,row_sink=row_sink,retain_rows=retain_rows)


def consume_file(state, reference, window, final_projection, *, row_sink, coverage=None):
    """内部单写入者直接收尾；失败后调用方必须丢弃候选并从完整检查点恢复。"""
    return _calculate_file(state,reference,window,(),final_projection,row_sink=row_sink,retain_rows=False,
                           consume=True,coverage=coverage)


def _calculate_file(state, reference, window, observations, final_projection, *, source="r", monthly=True,
                    country_table="feature_country", other_table="feature_other", row_sink=None,
                    retain_rows=True, consume=False, coverage=None):
    _check_projection(state.mode, state.projection)
    if reference.version != state.reference_version:
        raise FeatureCalculationError("参考版本发生变化")
    if state.last_window_end is not None and window.start < state.last_window_end:
        raise FeatureCalculationError("窗口重叠或回退")
    if window.file_ref == state.last_completed_update_file:
        raise FeatureCalculationError("重复文件不能重复累计")
    work = state if consume else deepcopy(state)
    diagnostics = []
    previous_sequence = -1
    for item in observations:
        try:
            if item.sequence <= previous_sequence or not item.source_ref:
                raise ValueError("观察来源内顺序/定位无效")
            previous_sequence = item.sequence
            diagnostic = count_observation(work, reference, window, item)
            if diagnostic is not None: diagnostics.append(diagnostic)
        except (ValueError, TypeError) as exc:
            raise FeatureCalculationError(f"{item.source_ref}: {exc}") from exc
    final_projection = final_projection() if callable(final_projection) else final_projection
    _check_projection(state.mode, final_projection)
    if coverage is not None:
        if not consume or state.mode != 'ordinary':
            raise FeatureCalculationError('增量覆盖仅用于普通 Feature 单写入者')
        coverage.validate_projection(final_projection)
    quality = "unknown" if "unknown" in (window.coverage, final_projection.coverage) else (
        "partial" if "partial" in (window.coverage, final_projection.coverage) else "complete")
    work.projection = deepcopy(final_projection)
    rows = []
    def emit(row):
        if row_sink is not None:
            row_sink(row)
        if retain_rows:
            rows.append(row)
    sparse = []
    collect_raw, collect_v4, collect_v6 = set(), set(), set()
    for country, asns in work.feature_dict.items():
        country_raw, country_v4, country_v6 = set(), set(), set()
        announcements = withdrawals = 0
        for asn, value in asns.items():
            if coverage is None:
                raw = final_projection.as_prefix.get(asn, frozenset())
                v4, v6 = set(), set()
                logged_skips = set()
                for prefix in sorted(raw):
                    normalized, reason = prefix_filter(prefix)
                    if reason:
                        warning_key = (reason, prefix if normalized is None else normalized)
                        if warning_key not in logged_skips:
                            diagnostics.append(Diagnostic("aggregate", f"{country}:{asn}", reason, prefix, normalized))
                            logged_skips.add(warning_key)
                        continue
                    (v4 if ip_network(normalized).version == 4 else v6).add(normalized)
                country_raw.update(raw)
                country_v4.update(v4)
                country_v6.update(v6)
            else:
                scope = coverage.ensure_asn(asn, country)
                raw, v4, v6 = scope.raw, scope.v4, scope.v6
                diagnostics.extend(scope.diagnostics(country, asn))
            if value.is_change:
                values = (_values(v4, v6, value.announ_num, value.withdraw_num) if coverage is None
                          else scope.values(value.announ_num, value.withdraw_num))
                value.v4Prefix_num, value.v6Prefix_num, value.v4IP_num = values.v4Prefix_num, values.v6Prefix_num, values.v4IP_num
                base = f"feature_{reference.big_countries[country]}" if reference.big_countries.get(country) else other_table
                table = base + (window.file_time.astimezone(ZoneInfo("Asia/Shanghai")).strftime("_%Y%m") if monthly else "")
                emit(_row("asn", asn, country if country != "未知" else " ", values, raw, v4, v6,
                                 table if state.mode == "ordinary" else None,
                                 "written" if state.mode == "ordinary" else "ir_asn_write_disabled", quality))
                diagnostics.extend(_warnings("asn", f"{country}:{asn}", values, v4,
                    sample_provider=None if coverage is None else scope.warning_sample))
                announcements += value.announ_num
                withdrawals += value.withdraw_num
            else:
                sparse.append((country, asn))
        if coverage is None:
            collect_raw.update(country_raw)
            collect_v4.update(country_v4)
            collect_v6.update(country_v6)
            values = _values(country_v4, country_v6, announcements, withdrawals)
        else:
            scope = coverage.countries[country]
            country_raw, country_v4, country_v6 = scope.raw, scope.v4, scope.v6
            values = scope.values(announcements, withdrawals)
        emit(_row("country", country, country, values, country_raw, country_v4, country_v6,
                         country_table, "written", quality))
        diagnostics.extend(_warnings("country", country, values, country_v4,
            sample_provider=None if coverage is None else scope.warning_sample))
    if coverage is None:
        values = _values(collect_v4, collect_v6, work.feature_collect_dict.announ_num, work.feature_collect_dict.withdraw_num)
    else:
        scope = coverage.collect
        collect_raw, collect_v4, collect_v6 = scope.raw, scope.v4, scope.v6
        values = scope.values(work.feature_collect_dict.announ_num, work.feature_collect_dict.withdraw_num)
    emit(_row("collect", "collect", "collect", values, collect_raw, collect_v4, collect_v6,
                     country_table, "written", quality))
    diagnostics.extend(_warnings("collect", "collect", values, collect_v4,
        sample_provider=None if coverage is None else scope.warning_sample))
    work.feature_collect_dict = deepcopy(values)
    if consume:end_counts=deepcopy(values)
    else:end_state = deepcopy(work)
    for asns in work.feature_dict.values():
        for value in asns.values():
            if value.is_change:
                value.is_change = False
                value.announ_num = value.withdraw_num = 0
    work.feature_collect_dict.announ_num = work.feature_collect_dict.withdraw_num = 0
    work.last_completed_update_file = window.file_ref
    work.last_window_end = window.end
    work.completed_files += 1
    work.status = "calculated_" + quality
    if consume:return ConsumedWindow(tuple(sparse),tuple(diagnostics),end_counts,work,quality)
    return Result(RULES[state.mode], window, source, tuple(rows), tuple(sparse), tuple(diagnostics), end_state, work, quality,
                  {"monthly": monthly, "country_table": country_table, "other_table": other_table,
                   "source": source, "timezone": "Asia/Shanghai", "units": dict(UNITS),
                   "big_countries": dict(reference.big_countries), "reference_version": reference.version,
                   "initial_projection_version": state.projection.version,
                   "final_projection_version": final_projection.version})


def _values(v4, v6, announcements, withdrawals):
    count = calculate_c_segments_count(v4)
    return Values(count, calculate_v6_48_segments_count(v6), count * 256, announcements, withdrawals)


def _row(scope, subject, country, values, raw, v4, v6, table, presence, coverage):
    status = "unknown" if coverage != "complete" else ("observed_nonzero" if v4 or v6 else "observed_zero")
    return FeatureRow(scope, subject, country, values, frozenset(raw), frozenset(v4), frozenset(v6), table, presence, status)


def _warnings(scope, identifier, values, prefixes, *, sample_provider=None):
    reasons = []
    if values.v4Prefix_num >= 2**24:
        reasons.append("v4prefix_num_reached_full_ipv4_c_segments")
    if values.v4IP_num >= 2**32:
        reasons.append("v4ip_num_reached_full_ipv4_space")
    elif values.v4IP_num >= 10**9:
        reasons.append("v4ip_num_exceeds_suspicious_threshold")
    sample = (sample_provider() if sample_provider is not None else tuple(sorted(prefixes)[:10])) if reasons else ()
    return [Diagnostic(scope, identifier, reason, sample_prefixes=sample) for reason in reasons]
