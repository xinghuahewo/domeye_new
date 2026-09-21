"""消费已保存整行；只在显式新建的隔离目录暂存表格，不访问原资料。"""

import ast
import csv
from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Iterable
from types import SimpleNamespace
from data_pipeline.analysis.detection.models import ReferenceBundle

import pandas as pd
from openpyxl import Workbook
from openpyxl.cell.cell import ERROR_CODES


@dataclass(frozen=True)
class ReferenceSource:
    role: str
    source_id: str
    expected_rows: int
    rows: Iterable[dict]


class ReferenceView:
    version_rule = "detection-reference-39578fe/v2"
    csv_keys = {
        "as_info": "asn",
        "important_as_dict": "aut-num",
        "prefix_info": "prefix",
        "triplet_info": "first_as",
    }
    json_roles = {
        "as_prefix_dict",
        "as_rel_dict",
        "important_domain_dict",
        "private_as_dict",
    }
    historical_applicability = "Unknown"

    def __init__(self, version, snapshot_ref, root, *, guard=lambda: None):
        if not version or not snapshot_ref:
            raise ValueError("参考版本与固定观察快照必须明确")
        self.version, self.snapshot_ref = version, snapshot_ref
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=False)
        self.guard = guard
        self._mappings, self._selections, self.sources = {}, {}, {}
        self._taken = False
        self._streamed_roles = set()

    def mapping(self, role):
        if self._taken:
            raise ValueError("参考解释已移交计算，禁止再取可变映射")
        if role in self._streamed_roles:
            raise ValueError("参考解释已通过sink移交")
        return self._mappings[role]

    def selection(self, role, key):
        if role in self._streamed_roles:
            raise ValueError("参考选择已通过sink移交")
        return self._selections[role][key]

    def add(self, source, *, selected_sink=None):
        """as_info可逐选中行移交sink；解释/键/定位完全复用普通Detection路径。"""
        if selected_sink is not None and source.role != "as_info":
            raise ValueError("选择sink仅支持as_info")
        if self._taken:
            raise ValueError("参考解释已移交计算")
        if source.role in self.json_roles:
            return self._add_json(source)
        if source.role in ("country", "important_prefix_v4", "important_prefix_v6"):
            return self._add_excel(source)
        if source.role not in self.csv_keys or source.role in self.sources:
            raise ValueError("参考用途不支持或重复")
        if source.expected_rows < 0:
            raise ValueError("参考计数非法")
        path = self.root / (source.role + ".csv")
        locators = []
        count = 0
        with path.open("x", encoding="utf-8", newline="") as stream:
            # 仅逻辑字段已保存；全引号避免原“带引号空白记录”被重写成可跳过空行。
            writer = csv.writer(stream, quoting=csv.QUOTE_ALL)
            for row in source.rows:
                self.guard()
                if (
                    row["source_id"] != source.source_id
                    or row["rule"] != "reference-rows/v2"
                ):
                    raise ValueError("参考来源或保存版本不符")
                if row["row"] != count or row["location"] != "csv":
                    raise ValueError("参考行顺序或格式不符")
                count += 1
                if row.get("csv_record_kind") in ("blank_line", "whitespace_line"):
                    continue
                cells = json.loads(row["raw_row"])
                if not isinstance(cells, list) or any(
                    not isinstance(x, str) for x in cells
                ):
                    raise ValueError("CSV保存字段必须是原字符串列表")
                writer.writerow(cells)
                locators.append(
                    {
                        "snapshot_ref": self.snapshot_ref,
                        **{k: v for k, v in row.items() if k != "raw_row"},
                    }
                )
        if count != source.expected_rows:
            raise ValueError("参考来源计数不符")
        self.guard()

        frame = pd.read_csv(
            path, keep_default_na=source.role == "important_as_dict", low_memory=False
        )
        if len(frame) != len(locators) - 1:
            raise ValueError("解释行数与保存行定位不符")
        locators = locators[1:]
        mapping, selected = {}, {}
        if source.role == "triplet_info":
            # 与旧 apply(axis=1) 相同的行类型提升；不强制规范化三键。
            for ordinal, row in frame.iterrows():
                key = tuple(str(row[k]) for k in ("first_as", "second_as", "third_as"))
                record = row.to_dict()
                mapping.setdefault(key[0], {}).setdefault(key[1], {})[key[2]] = record
                selected[key] = locators[ordinal]
        else:
            key_field = self.csv_keys[source.role]
            frame = frame.drop_duplicates(subset=[key_field], keep="first")
            for ordinal, record in zip(frame.index, frame.to_dict(orient="records")):
                key = record.pop(key_field)
                if source.role == "as_info":
                    key = str(key)
                    for field in (
                        "import_as",
                        "export_as",
                        "v4Peer",
                        "v6Peer",
                        "sibling_as",
                    ):
                        if field not in record:
                            continue
                        raw = record[field]
                        value = ast.literal_eval(raw) if raw else []
                        if not isinstance(value, (list, tuple, set)):
                            raise ValueError(f"参考关系不是集合：{key}/{field}")
                        record[field] = value
                self.guard()
                if selected_sink is None:
                    mapping[key] = record
                    selected[key] = locators[ordinal]
                else:
                    selected_sink(key, record, locators[ordinal])
        self._mappings[source.role], self._selections[source.role] = mapping, selected
        if selected_sink is not None:
            self._streamed_roles.add(source.role)
        self.sources[source.role] = {
            "source_id": source.source_id,
            "rows": count,
            "snapshot_ref": self.snapshot_ref,
        }

    def _add_excel(self, source):
        if source.role in self.sources or source.expected_rows < 0:
            raise ValueError("参考用途重复或计数非法")
        if (
            source.role == "important_prefix_v6"
            and "important_prefix_v4" not in self.sources
        ):
            raise ValueError("重要前缀必须先v4再v6")
        records, locators, sheet = [], [], None
        for row in source.rows:
            self.guard()
            if (
                row["source_id"] != source.source_id
                or row["rule"] != "reference-rows/v2"
                or row["row"] != len(records)
            ):
                raise ValueError("参考来源/版本/行順序不符")
            if sheet is not None and row["location"] != sheet:
                raise ValueError("多工作表尚缺显式首表次序合同，不猜旧默认表")
            sheet = row["location"]
            cells = json.loads(row["raw_row"])
            if any(cell["type"] in ("f", "d") for cell in cells):
                raise ValueError("保存公式/日期尚无完整旧读取类型合同，不能冒充已解释")
            for cell in cells:
                kind, value = cell["type"], cell["value"]
                supported = (
                    kind == "s"
                    and isinstance(value, str)
                    and len(value) <= 32767
                    or kind == "n"
                    and (
                        value is None
                        or type(value) in (int, float)
                        and math.isfinite(value)
                    )
                    or kind == "b"
                    and type(value) is bool
                    or kind == "e"
                    and value in ERROR_CODES
                    or kind == "inlineStr"
                    and value is None
                )
                if not supported:
                    raise ValueError(f"保存Excel类型/值超出支持合同：{kind}")
            records.append(cells)
            locators.append(
                {
                    "snapshot_ref": self.snapshot_ref,
                    **{k: v for k, v in row.items() if k != "raw_row"},
                }
            )
        if len(records) != source.expected_rows or not records:
            raise ValueError("参考来源计数不符或缺少表头")
        # 用项目锁定pandas的Excel读取保持旧数值/空值推断，不把纯数字文本固定成str。
        book = Workbook()
        for row_number, record in enumerate(records, 1):
            for column, saved in enumerate(record, 1):
                cell = book.active.cell(row_number, column, saved["value"])
                # value赋值会推断公式/错误；必须用已保存类型覆盖推断。
                cell.data_type = saved["type"]
                if saved["type"] == "inlineStr":
                    # openpyxl把原空字符串读为inlineStr/None；用s/空串重写同一空inline节点。
                    cell.value = ""
                    cell.data_type = "s"
        path = self.root / (source.role + ".xlsx")
        book.save(path)
        book.close()
        frame = pd.read_excel(path, keep_default_na=source.role != "country")
        if source.role == "country":
            frame = frame.fillna("")
            mapping, selected = {}, {}
            for ordinal, row in frame.iterrows():
                value = row.to_dict()
                key = value.pop("two_letter_code")
                mapping[key], selected[key] = value, locators[ordinal + 1]
            self._mappings["country"], self._selections["country"] = mapping, selected
        else:
            if source.role == "important_prefix_v4":
                self._prefix_frame, self._prefix_locations = frame, locators[1:]
            else:
                frame = pd.concat([self._prefix_frame, frame], ignore_index=True)
                locators = [None] + self._prefix_locations + locators[1:]
                del self._prefix_frame, self._prefix_locations
            frame = frame.drop_duplicates(subset=["prefix"], keep="first")
            mapping, selected = {}, {}
            for ordinal, row in zip(frame.index, frame.to_dict(orient="records")):
                key = row.pop("prefix")
                mapping[key], selected[key] = row, locators[ordinal + 1]
            self._mappings["important_prefix_dict"] = mapping
            self._selections["important_prefix_dict"] = selected
        self.sources[source.role] = {
            "source_id": source.source_id,
            "rows": len(records),
            "snapshot_ref": self.snapshot_ref,
        }

    def metadata(self):
        required = (
            set(self.csv_keys)
            | self.json_roles
            | {"country", "important_prefix_v4", "important_prefix_v6"}
        )
        if set(self.sources) != required:
            raise ValueError("必须显式提供全部11类参考来源")
        return ReferenceBundle(
            self.version,
            dict(self.sources),
            {"selection_rule": self.version_rule, "snapshot_ref": self.snapshot_ref},
            {},
        )

    def take_info(self):
        if self._streamed_roles:
            raise ValueError("参考解释已通过sink移交")
        self.metadata()
        if self._taken:
            raise ValueError("参考解释只能移交一次")
        self._taken = True
        return SimpleNamespace(**self._mappings)
        self.guard()

    def _add_json(self, source):
        if source.role in self.sources or source.expected_rows < 0:
            raise ValueError("参考用途重复或计数非法")
        mapping, selected, count = {}, {}, 0
        for row in source.rows:
            self.guard()
            if (
                row["source_id"] != source.source_id
                or row["rule"] != "reference-rows/v2"
                or row["row"] != count
            ):
                raise ValueError("参考来源/版本/行顺序不符")
            key = row["location"]
            mapping[key] = _json_value(json.loads(row["raw_row"]))
            selected[key] = {
                "snapshot_ref": self.snapshot_ref,
                **{k: v for k, v in row.items() if k != "raw_row"},
            }
            count += 1
        if count != source.expected_rows:
            raise ValueError("参考来源计数不符")
        self._mappings[source.role], self._selections[source.role] = mapping, selected
        self.sources[source.role] = {
            "source_id": source.source_id,
            "rows": count,
            "snapshot_ref": self.snapshot_ref,
        }


def _json_value(value):
    if isinstance(value, dict):
        if set(value) != {"__object_pairs__"}:
            raise ValueError("保存JSON对象缺少重复键顺序证据")
        return {key: _json_value(item) for key, item in value["__object_pairs__"]}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value
