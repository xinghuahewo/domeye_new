"""计算输出记录；没有数据库连接与通知发送实现。"""

import ast
from collections.abc import Mapping
from data_pipeline.bgp.state.path_dictionary import PathText
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime
from functools import wraps
from hashlib import sha256
import json
import math
from itertools import islice


def plain(value):
    """集合和非字符串键保留类型，避免 int/string 合并及集合顺序污染身份。"""
    if isinstance(value,PathText):return value.text
    if isinstance(value, bytes):
        return {"$bytes": value.hex()}
    if isinstance(value, float) and not math.isfinite(value):
        return {"$float": str(value)}
    if isinstance(value, (set, frozenset)):
        return {
            "$set": sorted(
                (plain(v) for v in value), key=lambda x: json.dumps(x, sort_keys=True)
            )
        }
    if isinstance(value, Mapping):
        if any(not isinstance(k, str) for k in value):
            return {
                "$map": sorted(
                    ([plain(k), plain(v)] for k, v in value.items()),
                    key=lambda x: json.dumps(x[0], sort_keys=True),
                )
            }
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    return value


def stable(value):
    return sha256(
        json.dumps(
            plain(value),
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def stable_stream(value):
    """按 stable 的同一字节合同计算大配置摘要，不复制整套参考及 JSON 正文。"""
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, allow_nan=False,
                               separators=(",", ":"))
    scalar = encoder.encode

    class FullChunk(Exception):
        pass

    def bounded_plain(item, budget):
        # 最多规范化 4096 个节点，再交给 C JSON 编码器；超过预算便沿原容器
        # 分流。完整参考表只排序键，不生成其值的规范化副本或完整 JSON。
        budget[0] -= 1
        if budget[0] < 0:
            raise FullChunk
        if isinstance(item, PathText):
            return item.text
        if isinstance(item, bytes):
            return {"$bytes": item.hex()}
        if isinstance(item, float) and not math.isfinite(item):
            return {"$float": str(item)}
        if isinstance(item, (set, frozenset)):
            return {"$set": sorted((bounded_plain(v, budget) for v in item),
                                   key=lambda x: json.dumps(x, sort_keys=True))}
        if isinstance(item, Mapping):
            if any(not isinstance(k, str) for k in item):
                return {"$map": sorted(
                    ([bounded_plain(k, budget), bounded_plain(v, budget)]
                     for k, v in item.items()),
                    key=lambda x: json.dumps(x[0], sort_keys=True))}
            return {k: bounded_plain(v, budget) for k, v in item.items()}
        if isinstance(item, (list, tuple)):
            return [bounded_plain(v, budget) for v in item]
        if isinstance(item, datetime):
            return {"$datetime": item.isoformat()}
        return item

    def sort_key(item):
        # 与 plain 的集合、非字符串键排序完全相同，保留相同排序键的原顺序。
        return json.dumps(plain(item), sort_keys=True)

    def groups(items):
        iterator = iter(items)
        while block := list(islice(iterator, 128)):
            yield block

    def sequence(items):
        yield "["
        first = True
        for block in groups(items):
            if not first:
                yield ","
            first = False
            try:
                normalized = bounded_plain(block, [4096])
            except FullChunk:
                for offset, item in enumerate(block):
                    if offset:
                        yield ","
                    yield from chunks(item)
            else:
                yield scalar(normalized)[1:-1]
        yield "]"

    def chunks(item):
        try:
            small = bounded_plain(item, [4096])
        except FullChunk:
            pass
        else:
            yield scalar(small)
            return
        if isinstance(item, PathText):
            yield scalar(item.text)
        elif isinstance(item, bytes):
            yield '{"$bytes":'
            yield scalar(item.hex())
            yield "}"
        elif isinstance(item, float) and not math.isfinite(item):
            yield '{"$float":'
            yield scalar(str(item))
            yield "}"
        elif isinstance(item, (set, frozenset)):
            yield '{"$set":'
            yield from sequence(sorted(item, key=sort_key))
            yield "}"
        elif isinstance(item, Mapping):
            if any(not isinstance(key, str) for key in item):
                yield '{"$map":'
                yield from sequence((key, item[key]) for key in sorted(item, key=sort_key))
                yield "}"

            else:
                yield "{"
                first = True
                for block in groups(sorted(item)):
                    if not first:
                        yield ","
                    first = False
                    try:
                        normalized = bounded_plain({key: item[key] for key in block}, [4096])
                    except FullChunk:
                        for offset, key in enumerate(block):
                            if offset:
                                yield ","
                            yield scalar(key)
                            yield ":"
                            yield from chunks(item[key])
                    else:
                        yield scalar(normalized)[1:-1]
                yield "}"
        elif isinstance(item, (list, tuple)):
            yield from sequence(item)
        elif isinstance(item, datetime):
            yield '{"$datetime":'
            yield scalar(item.isoformat())
            yield "}"
        else:
            yield scalar(item)

    result = sha256()
    pending = []
    size = 0
    for chunk in chunks(value):
        pending.append(chunk)
        size += len(chunk)
        if size >= 256 * 1024:
            result.update("".join(pending).encode())
            pending.clear()
            size = 0
    if pending:
        result.update("".join(pending).encode())
    return result.hexdigest()


def rule(location):
    def decorate(fn):
        @wraps(fn)
        def wrapped(self, *args, **kwargs):
            result = fn(self, *args, **kwargs)
            self.output.append(
                "rule_decision",
                {
                    "rule": location,
                    "inputs": {"args": args, "kwargs": kwargs},
                    "result": result,
                    "reference_rows_ref": self.output.references.version + "/row_refs",
                },
            )
            return result

        return wrapped

    return decorate


class Results:
    def __init__(self, scope, references):
        self.scope, self.references = scope, references
        self.source = scope.source
        self.thresholds = {
            "PREFIX_OUTAGE_THRESHOLD": 1.0,
            "PREFIX_RESTORE_THRESHOLD": 0.4,
            "AS_OUTAGE_THRESHOLD": 0.2,
            "AS_RESTORE_THRESHOLD": 0.85,
            "COUNTRY_OUTAGE_THRESHOLD": 0.03,
            "COUNTRY_RESTORE_THRESHOLD": 0.98,
        }
        self.rows, self.errors, self.projection_gaps = [], [], []
        self.context = {"baseline": True}
        self.identities, self.revisions, self.last_records = {}, {}, {}
        self.start_tables = {}

    @property
    def position(self):
        return len(self.rows)

    def since(self, start):
        return deepcopy(self.rows[start:])

    def _save(self, row):
        self.rows.append(row)

    def append(self, kind, payload, **attributes):
        row = {
            "kind": kind,
            "scope": asdict(self.scope),
            "reference_version": self.references.version,
            "reference_historical_applicability": self.references.historical_applicability,
            "evidence": deepcopy(self.context),
            "legacy": deepcopy(payload),
        }
        row.update(attributes)
        self._save(row)
        return row

    def parse_domains(self, raw, location):
        try:
            value = ast.literal_eval(raw) if isinstance(raw, str) else raw
            if not isinstance(value, (list, tuple, set)) or not all(
                isinstance(x, str) for x in value
            ):
                raise ValueError("域名必须为字符串集合")
        except (
            ValueError,
            SyntaxError,
            TypeError,
            MemoryError,
            RecursionError,
        ) as error:
            failure = {"location": location, "raw": raw, "reason": str(error)}
            self.errors.append(failure)
            self.append("reference_parse_failed", failure)
            raise ValueError(f"参考文本解析失败：{location}") from error
        self.append(
            "reference_parse", {"location": location, "raw": raw, "parsed": value}
        )
        return value

    def triplet(self, mapping, first, second, third):
        row = mapping.get(first, {}).get(second, {}).get(third)
        value = row["stability"] if row is not None else 0
        self.append(
            "reference_lookup",
            {
                "rule": "BGPLeak.py:290/get_other_info.py:19",
                "key": [first, second, third],
                "reference_row": row,
                "measurement_state": "available" if row is not None else "missing",
                "legacy_effective_value": value,
                "default_policy": "missing_returns_zero_39578fe",
            },
        )
        return value

    def capture(self, operation, **values):
        # 原数据库写入点改为显式、完整的值输出；不复制整张运行字典。
        selectors = {
            "prefix_outage_event": ("prefix", "prefix_outage_id"),
            "as_outage_event": ("origin", "as_outage_id"),
            "country_outage_event": ("country", "country_outage_id"),
            "moas_event_dict": ("prefix", "moas_id"),
            "sub_hijack_dict": ("prefix", "sub_hijack_id"),
            "phenomenon_dict": ("prefix", "phenomenon_id"),
        }
        for field, (obj, ident) in selectors.items():
            if field in values:
                values[field] = deepcopy(values[field][values[obj]][values[ident]])
        attributes = {}
        if operation == "send_outage_alert":
            attributes["delivery_state"] = "not_sent"
        if operation == "event_start" and "attacked_as" not in values:
            gap = {
                "operation": operation,
                "missing_fields": ["attacked_as"],
                "reason": "旧国家调用未传入必需实参；仅捕获计算意图，未调用旧数据库函数",
            }
            attributes["projection_quality"] = "partial"
            attributes["missing_reasons"] = gap
            self.projection_gaps.append(deepcopy(gap))
        else:
            attributes["projection_quality"] = "captured_not_persisted"
        self.append(operation, values, **attributes)

    def retire(self, kind, obj, legacy_id, record):
        self.event_revision(kind, obj, legacy_id, record)

    def event_revision(self, kind, obj, legacy_id, record):
        if kind == "moas" and record.get("is_hijack"):
            self.event_revision("hijack", obj, record["legacy_hijack_id"], record)
        # 开始时表引用保留，跨月结束不会被当前文件表覆盖。
        onset = record.get("s_time")
        key = stable([kind, obj, legacy_id, onset])
        current_table = (
            record.get("moas_table")
            if kind == "moas"
            else record.get("leak_phenomenon_table")
            if kind == "leak"
            else record.get(
                "table", record.get("hijack_table", record.get("sub_hijack_table"))
            )
        )
        self.start_tables.setdefault(key, current_table)
        if key not in self.identities:
            self.identities[key] = (
                "det_"
                + stable(
                    [
                        self.scope.run_id,
                        self.source,
                        kind,
                        obj,
                        onset,
                        self.context,
                        len(self.identities),
                    ]
                )[:32]
            )
        identity = self.identities[key]
        digest = stable(record)
        if self.last_records.get(identity) == digest:
            return
        self.last_records[identity] = digest
        self.revisions[identity] = self.revisions.get(identity, 0) + 1
        self.append(
            "business_revision",
            record,
            incident_id=identity,
            revision=self.revisions[identity],
            event_kind=kind,
            object=obj,
            legacy_ref={
                "source": self.source,
                "id": legacy_id,
                "onset": onset,
                "object": obj,
                "table": self.start_tables[key],
                "current_legacy_table": current_table,
            },
            conclusion="指定观察范围内的启发式异常候选",
            end_state="not_recorded"
            if kind == "leak"
            else ("recorded" if record.get("e_time") else "unknown"),
        )
