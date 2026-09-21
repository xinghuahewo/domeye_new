"""已核验类型的历史异常记录有界转换；不连接数据库，也不接入 Web 请求。

公开入口是 convert_anomaly_record、serialize_record、deserialize_record。
调用方提供源行、来源定位和 config/data-profile.json 的内容；本模块没有 IO。
这里只解释 prefix_outage、as_outage、leak、hijack、sub_hijack、country_outage 检测记录，不是 MRT RouteEvent，
也不证明检测正确性或正式数据准入。

来源须提供 instance、对应月份的 table、representation、带时区的 read_at，
以及 read_scope（source、start、end_exclusive）。representation=source_rows
表示调用方提供源业务字段；已加工详情不能反推原值。content_version、
collector_id、detector_version、evidence_refs 仅保留调用方给定的证据，
不能从实例名、source 类别或旧发布名推定。缺失证据仍为未知。

旧引用与可选旧 ID 原样保留；来源实例只限定命名空间，不是新异常 ID。
record 保存选定原字段、解释、定位、数据档和限制；read_at 是独立读取回执。
content_version 是上述 record 的规范编码摘要，绑定模型及映射规则版本，
不包含读取回执。摘要不证明内容已保存或来源可信，也不是数字签名。
源值支持 JSON 对象／列表及 datetime、timedelta、有限 Decimal；对象键须为
字符串，不得含保留键 $anomaly_scalar。原生 datetime 不提供原始文本精度，
精度保持未知；字符串的小数位数另行保留，不补造超出微秒的时点。
无时区的源时间按项目数据档解释；有明确时区的源值按实际时刻转换，
不能仅因其偏移不同于项目时区就认定冲突。

序列化只提供可复读字节，调用方负责实际留存。首次留存不是事件最初版本；
未保存的历史不能复原，本模块不提供生产存储、发布或历史查询服务。
"""

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import ipaddress
import json
import re
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


_SCHEMA_VERSION = "anomaly-record/v1"
_MAPPING_VERSION = "legacy-anomaly-mapping/v1"
_SOURCE_FIELDS = (
    "instance", "table", "representation", "read_at", "read_scope",
    "content_version", "collector_id", "detector_version", "evidence_refs",
)
_SCALAR_TAG = "$anomaly_scalar"


def _validate_value_encoding(value: Any) -> None:
    if isinstance(value, Mapping):
        if _SCALAR_TAG in value or any(not isinstance(key, str) for key in value):
            raise ValueError("源对象键必须为文本且不使用序列化保留键")
        for item in value.values():
            _validate_value_encoding(item)
    elif isinstance(value, list):
        for item in value:
            _validate_value_encoding(item)
    elif isinstance(value, tuple):
        raise ValueError("源数组须用列表表达，不能将元组静默改成列表")


def _encode_scalar(value: Any) -> dict[str, Any]:
    if isinstance(value, datetime):
        kind, encoded = "datetime", value.isoformat()
    elif isinstance(value, timedelta):
        kind, encoded = "timedelta_us", (value.days * 86400 + value.seconds) * 1000000 + value.microseconds
    elif isinstance(value, Decimal) and value.is_finite():
        kind, encoded = "decimal", str(value)
    else:
        raise ValueError("异常记录包含不支持的源值类型")
    return {_SCALAR_TAG: kind, "value": encoded}


def _decode_scalar(value: dict[str, Any]) -> Any:
    if _SCALAR_TAG not in value:
        return value
    if set(value) != {_SCALAR_TAG, "value"}:
        raise ValueError("异常记录源值编码无效")
    kind, encoded = value[_SCALAR_TAG], value["value"]
    if kind == "datetime" and isinstance(encoded, str):
        return datetime.fromisoformat(encoded)
    if kind == "timedelta_us" and type(encoded) is int:
        return timedelta(microseconds=encoded)
    if kind == "decimal" and isinstance(encoded, str):
        decoded = Decimal(encoded)
        if decoded.is_finite():
            return decoded
    raise ValueError("异常记录源值编码不支持")


def _canonical(value: Any) -> bytes:
    _validate_value_encoding(value)
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
        default=_encode_scalar,
    ).encode("utf-8")


def _content_version(record: Mapping[str, Any]) -> str:
    return "anr_v1_" + hashlib.sha256(_canonical(record)).hexdigest()


def _validate_result(result: Any) -> None:
    if (
        not isinstance(result, dict)
        or set(result) != {"record", "read_at", "content_version"}
        or not isinstance(result["record"], dict)
        or result["record"].get("schema_version") != _SCHEMA_VERSION
        or result["record"].get("mapping_version") not in {_MAPPING_VERSION, "legacy-anomaly-mapping/v2"}
    ):
        raise ValueError("异常记录格式不支持")
    if _content_version(result["record"]) != result["content_version"]:
        raise ValueError("异常记录内容版本不匹配")


def serialize_record(result: Mapping[str, Any]) -> bytes:
    """将结果编码为独立字节；不负责文件写入或生产发布。"""
    _validate_result(result)
    return _canonical(result)


def deserialize_record(payload: bytes) -> dict[str, Any]:
    """读取已保存的结果字节，不连接可变的原数据库。"""
    try:
        result = json.loads(payload, object_hook=_decode_scalar)
    except (InvalidOperation, OverflowError, TypeError) as error:
        raise ValueError("异常记录源值编码无效") from error
    _validate_result(result)
    return result


@dataclass(frozen=True)
class _EventKind:
    table: str
    key_fields: tuple[str, ...]
    object_field: str
    object_kind: str
    id_field: str
    detail_fields: tuple[str, ...]
    limitations: tuple[str, ...]
    lifecycle: bool = True
    level_field: str = "outage_level"
    level_description: str = "outage_level_descr"


_PATH_FIELDS = ("pre_vp_paths", "eve_vp_paths", "next_vp_paths")
_KINDS = {
    "prefix_outage": _EventKind(
        "prefix_outage", ("source", "prefix", "outage_id", "asn"),
        "prefix", "prefix", "outage_id", ("asn",) + _PATH_FIELDS,
        ("recorded_asn_not_responsibility", "path_times_not_inferred_from_event_time"),
    ),
    "as_outage": _EventKind(
        "as_outage", ("source", "asn", "outage_id"), "asn", "asn", "outage_id",
        ("outage_prefixes", "max_outage_prefix_num", "total_prefix_num", "max_outage_prefix_ratio") + _PATH_FIELDS,
        ("affected_population_not_established", "stored_ratio_not_authoritative", "path_times_not_inferred_from_event_time"),
    ),
    "leak": _EventKind(
        "leak_event", ("source", "prefix", "leak_event_id"), "prefix", "prefix", "leak_event_id",
        ("prefix_ori_as", "leak_by", "leak_to", "as_path"),
        ("leak_lifecycle_not_supported", "recorded_roles_not_causal"),
        False, "leak_level", "leak_level_info",
    ),
    "hijack": _EventKind(
        "hijack", ("source", "prefix", "hijack_eventid"), "prefix", "prefix", "hijack_eventid",
        (
            "hijacked_as", "hijacked_as_name", "hijacked_as_org", "hijacked_as_country",
            "hijacked_as_descr", "hijacked_as_admin", "hijacker_as", "hijacker_as_name",
            "hijacker_as_org", "hijacker_as_country", "hijacker_as_descr", "hijacker_as_admin",
            "end_as", "is_hijack", "filter_reason",
        ) + _PATH_FIELDS,
        ("recorded_roles_not_causal", "detector_flag_not_independent_confirmation",
         "path_times_not_inferred_from_event_time"),
        True, "hijack_level", "hijack_level_info",
    ),
    "sub_hijack": _EventKind(
        "sub_hijack", ("source", "prefix", "sub_hijack_eventid"),
        "prefix", "prefix", "sub_hijack_eventid",
        (
            "hijacked_prefix", "hijacked_as", "hijacked_as_name", "hijacked_as_org",
            "hijacked_as_country", "hijacked_as_descr", "hijacked_as_admin",
            "hijacker_as", "hijacker_as_name", "hijacker_as_org", "hijacker_as_country",
            "hijacker_as_descr", "hijacker_as_admin", "is_sub_hijack", "filter_reason",
        ),
        ("recorded_roles_not_causal", "detector_flag_not_independent_confirmation",
         "sub_hijack_path_evidence_not_available"),
        True, "sub_hijack_level", "level_info",
    ),
    "country_outage": _EventKind(
        "country_outage", ("source", "country", "outage_id"),
        "country", "country", "outage_id",
        ("country_chinese_name", "max_outage_as_ratio", "max_outage_as_num",
         "total_as_num", "outage_ases"),
        ("affected_population_not_established", "stored_ratio_not_authoritative",
         "description_time_not_event_start", "country_record_not_national_connectivity"),
    ),
}


def _field(row: Mapping[str, Any], name: str) -> dict[str, Any]:
    value = row.get(name)
    return {"state": "unknown" if value in (None, "") else "recorded", "value": value}


def _object_value(value: Any, kind: str) -> str:
    if kind == "prefix":
        if not isinstance(value, str):
            raise ValueError("前缀必须为 CIDR 文本")
        return str(ipaddress.ip_network(value, strict=True))
    if kind == "country":
        # 仅校验本轮源国家代码格式；不引入国家名映射或推定观测范围。
        if not isinstance(value, str) or not re.fullmatch(r"[A-Z]{2}", value):
            raise ValueError("国家代码须为两位大写字母")
        return value
    text = str(value)
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", text) or not 0 <= int(text) <= 4294967295:
        raise ValueError("ASN 不可解析")
    return str(int(text))


def is_unresolved_as_set_text(value: Any) -> bool:
    """只验证旧ASN字段的有界集合文本形式，不生成成员身份或单ASN解释。"""
    return (isinstance(value, str) and len(value) <= 4096
            and re.fullmatch(r'\{[0-9]{1,10}(?:, ?[0-9]{1,10})*\}', value) is not None
            and all(int(token) <= 4294967295 for token in re.findall(r'[0-9]+', value)))


def _time_field(value: Any, zone: str) -> dict[str, Any]:
    unknown = {"state": "unknown", "value": None, "precision": None}
    if value in (None, ""):
        return unknown
    precision, digits = "unknown", None
    if isinstance(value, str):
        match = re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"(?:\.([0-9]{1,6}))?(?:Z|[+-][0-9]{2}:[0-9]{2})?", value,
        )
        if not match:
            return unknown
        digits = len(match[1]) if match[1] else None
        precision = "fractional_second" if digits else "second"
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError):
        return unknown
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(zone))
    normalized = parsed.astimezone(timezone.utc).isoformat(
        timespec="microseconds" if parsed.microsecond else "seconds",
    ).replace("+00:00", "Z")
    result = {"state": "recorded", "value": normalized, "precision": precision}
    if digits:
        result["fractional_digits"] = digits
    return result


def _aware_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("来源上下文必须提供带时区时间")
    # PostgreSQL 回执可有 1—6 位小数；只为 Python 3.10 解析补位，不回写源值。
    encoded = re.sub(r"(\.[0-9]{1,6})(?=[+-][0-9]{2}:[0-9]{2}$)",
                     lambda match: match[1].ljust(7, "0"), value.replace("Z", "+00:00"))
    parsed = datetime.fromisoformat(encoded)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("来源上下文时间缺少时区")
    return parsed


def _validate_context(source: Mapping[str, Any], profile: Mapping[str, Any]) -> None:
    if not isinstance(source, Mapping) or not isinstance(profile, Mapping):
        raise ValueError("来源和数据档必须为字段对象")
    for name in ("instance", "table", "representation"):
        if not isinstance(source.get(name), str) or not source[name].strip():
            raise ValueError(f"来源缺少 {name}")
    _aware_time(source.get("read_at"))
    try:
        zone = ZoneInfo(profile["timezone"])
    except (KeyError, TypeError, ValueError, ZoneInfoNotFoundError) as error:
        raise ValueError("数据档时区不可用") from error
    scope = source.get("read_scope")
    if not isinstance(scope, Mapping) or not isinstance(scope.get("source"), str):
        raise ValueError("读取范围不完整")
    if not _aware_time(scope.get("start")) < _aware_time(scope.get("end_exclusive")):
        raise ValueError("读取范围起止无效")
    start = _aware_time(profile.get("window_start"))
    end = _aware_time(profile.get("window_end_exclusive"))
    snapshot = _aware_time(profile.get("snapshot_time"))
    if not start <= snapshot < end:
        raise ValueError("数据档窗口或快照无效")
    for value in (start, end, snapshot):
        if value.utcoffset() != value.astimezone(zone).utcoffset():
            raise ValueError("数据档时间与时区冲突")


def _scope_reasons(source: Mapping[str, Any], profile: Mapping[str, Any], start: str, source_code: str) -> list[str]:
    reasons = []
    if source.get("representation") != "source_rows":
        reasons.append("source_values_not_available")
    scope = source["read_scope"]
    if scope.get("source") != source_code:
        reasons.append("source_scope_conflict")
    observed = datetime.fromisoformat(start).replace(tzinfo=ZoneInfo(profile["timezone"]))
    read_start = _aware_time(scope["start"])
    read_end = _aware_time(scope["end_exclusive"])
    if not read_start <= observed < read_end:
        reasons.append("outside_read_scope")
    profile_start = _aware_time(profile["window_start"])
    profile_end = _aware_time(profile["window_end_exclusive"])
    if not profile_start <= observed < profile_end:
        reasons.append("outside_data_profile")
    return reasons


def convert_anomaly_record(
    legacy_reference: str,
    candidates: Sequence[Mapping[str, Any]],
    *,
    source: Mapping[str, Any],
    data_profile: Mapping[str, Any],
    existing_id: str | None = None,
    preserve_unresolved_as_set: bool = False,
) -> dict[str, Any]:
    """转换完整候选集，不查询数据库、不生成或修改旧异常 ID。

    legacy_reference 为 ``类型/本地起始时间/对象/编号/source``，时间格式为
    YYYY-MM-DD HH:MM:SS，前缀斜线在旧引用中以连字符编码。
    candidates 须由调用方按该引用完整提供；模块不能证明查询是否漏行。
    无候选返回 missing；一条完整匹配返回 matched；多条返回 ambiguous；
    来源、主键或时间范围冲突返回 conflict。未确认关联时不选取原行或详情，
    仅保留已有引用、候选定位与限制；候选行数不是异常数量。
    缺失上下文、非法引用或不支持的值类型抛出 ValueError。
    字段 state=recorded 只表示来源有此值（包括明确空数组），不是业务判定。
    """
    _validate_context(source, data_profile)
    if (
        not isinstance(legacy_reference, str)
        or not isinstance(candidates, Sequence)
        or isinstance(candidates, (str, bytes))
        or any(not isinstance(row, Mapping) for row in candidates)
    ):
        raise ValueError("异常引用或候选源行结构无效")
    kind, start, target, number, source_code = legacy_reference.split("/")
    if (
        kind not in _KINDS or not target or not source_code
        or not re.fullmatch(r"[0-9]+", number)
        or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", start)
    ):
        raise ValueError("异常引用格式或类型不支持")
    definition = _KINDS[kind]
    if type(preserve_unresolved_as_set) is not bool:
        raise ValueError('身份保留开关须为布尔值')
    unresolved = preserve_unresolved_as_set and kind == 'as_outage' and is_unresolved_as_set_text(target)
    target_value = target if unresolved else _object_value(target.replace("-", "/"), definition.object_kind)
    reasons = _scope_reasons(source, data_profile, start, source_code)
    key_names = definition.key_fields
    locators = [{
        "source_instance": source["instance"],
        "source_table": source["table"],
        "key": {name: row[name] for name in key_names if name in row},
    } for row in candidates]
    if source["table"] != f"{definition.table}_{start[:7].replace('-', '')}":
        reasons.append("source_table_conflict")
    for row in candidates:
        try:
            row_target = row.get(definition.object_field) if unresolved else _object_value(row.get(definition.object_field), definition.object_kind)
        except ValueError:
            row_target = None
        if (
            any(row.get(name) in (None, "") for name in key_names)
            or row.get("source") != source_code
            or row_target != target_value
            or str(row.get(definition.id_field)) != number
            or _time_field(row.get("s_time"), data_profile["timezone"])["value"] != _time_field(start, data_profile["timezone"])["value"]
        ):
            reasons.append("reference_conflict")
    association = (
        "conflict" if reasons else
        "missing" if not candidates else
        "ambiguous" if len(candidates) > 1 else "matched"
    )
    record = {
        "schema_version": _SCHEMA_VERSION,
        "mapping_version": "legacy-anomaly-mapping/v2" if unresolved else _MAPPING_VERSION,
        "identity": {
            "source_instance": source["instance"],
            "legacy_reference": legacy_reference,
            "legacy_id": existing_id,
        },
        "source": deepcopy({name: source.get(name) for name in _SOURCE_FIELDS}),
        "data_profile": deepcopy(dict(data_profile)),
        "association": {
            "state": association,
            "candidate_count": len(candidates),
            "locators": deepcopy(locators),
            "reasons": sorted(set(reasons)),
        },
        "raw_fields": None,
        "common": None,
        "details": None,
        "limitations": [
            "causal_conclusion_not_supported", "formal_admission_not_established",
            *definition.limitations,
        ],
    }
    for field, reason in (
        ("collector_id", "collector_unknown"),
        ("content_version", "source_content_version_unknown"),
        ("detector_version", "detector_version_unknown"),
    ):
        if source.get(field) in (None, ""):
            record["limitations"].append(reason)
    read_at = record["source"].pop("read_at")
    if unresolved:
        record['limitations'].append('as_set_object_identity_unresolved')
    if association == "matched":
        retained_fields = set(definition.key_fields + definition.detail_fields + (
            "s_time", "e_time", "duration", definition.level_field,
            definition.level_description, "event_info",
        ))
        row = deepcopy({name: value for name, value in candidates[0].items() if name in retained_fields})
        record["raw_fields"] = row
        record["common"] = {
            "kind": kind,
            "object": {"kind": 'unresolved_asn' if unresolved else definition.object_kind, "value": target_value},
            "start_time": _time_field(row.get("s_time"), data_profile["timezone"]),
            "end_time": _time_field(row.get("e_time"), data_profile["timezone"]) if definition.lifecycle else {"state": "unavailable", "value": None},
            "duration": _field(row, "duration") if definition.lifecycle else {"state": "unavailable", "value": None},
            "level": _field(row, definition.level_field),
            "level_description": _field(row, definition.level_description),
            "summary": _field(row, "event_info"),
        }
        record["details"] = {name: _field(row, name) for name in definition.detail_fields}
        if definition.lifecycle and row.get("e_time") not in (None, ""):
            end = record["common"]["end_time"]
            start_time = record["common"]["start_time"]
            if end["state"] == "recorded" and start_time["state"] == "recorded":
                if datetime.fromisoformat(end["value"].replace("Z", "+00:00")) < datetime.fromisoformat(start_time["value"].replace("Z", "+00:00")):
                    end.update(state="unknown", value=None)
            if end["state"] == "unknown":
                record["limitations"].append("end_time_unconfirmed")
    return {"record": record, "read_at": read_at, "content_version": _content_version(record)}
