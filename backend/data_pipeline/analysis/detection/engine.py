"""六类兼容纯计算入口；只消费显式观察和参考，不提供发布或恢复。"""

from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
from data_pipeline.common.prefix_networks import network_facts
from functools import lru_cache
from types import SimpleNamespace
import ast

from data_pipeline.analysis.detection.models import DetectionInput, DetectionScope, DetectionSeed, FileBoundary, ReferenceBundle
from data_pipeline.analysis.detection.projection import DetectionProjection, legacy_origin
from data_pipeline.analysis.detection._results import Results, plain
from data_pipeline.analysis.detection._hijack import BGPHijack
from data_pipeline.analysis.detection._subprefix_hijack import BGPSubHijack
from data_pipeline.analysis.detection._leak import BGPLeak
from data_pipeline.analysis.detection._outage import BGPOutage


def _local(utc):
    # 一条消息的多个元素共享时点；只缓存有界的原始字符串，错误仍走原校验。
    if type(utc) is str and len(utc) <= 128:
        return _cached_local(utc)
    return _local_value(utc)


@lru_cache(maxsize=4096)
def _cached_local(utc):
    return _local_value(utc)


def _local_value(utc):
    value = datetime.fromisoformat(utc.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("观察时点必须含时区")
    return value.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


class DetectionEngine:
    """consume 顺序不可调换；失败后只可导出 partial，不接受继续计算。"""

    def __init__(
        self,
        seed: DetectionSeed,
        references: ReferenceBundle,
        scope: DetectionScope,
        *,
        results_factory=Results,
        projection=None,
    ):
        for value in (
            scope.run_id,
            scope.source,
            scope.collector_id,
            scope.input_version,
            references.version,
            seed.baseline_ref,
        ):
            if not value:
                raise ValueError("运行、来源、参考与基线身份必须显式提供")
        start = datetime.fromisoformat(scope.window_start.replace("Z", "+00:00"))
        end = datetime.fromisoformat(scope.window_end.replace("Z", "+00:00"))
        if start.tzinfo is None or end.tzinfo is None or start >= end:
            raise ValueError("计算窗口必须是含时区的非空半开区间")
        self.window = (start, end)
        self.scope, self.seed, self.references = (
            scope,
            self._copy_seed(seed),
            self._copy_references(references),
        )
        self.output = results_factory(scope, self.references)
        self.projection = DetectionProjection() if projection is None else projection
        self.status, self.processed, self.file_boundary = "ready", 0, None
        self.last_observation = None
        self.seen_observations = set()
        self.info = self._reference_info(references)
        required = (
            "as_info",
            "prefix_info",
            "country",
            "important_as_dict",
            "important_prefix_dict",
            "as_prefix_dict",
            "as_rel_dict",
            "important_domain_dict",
            "private_as_dict",
            "triplet_info",
        )
        if any(not hasattr(self.info, k) for k in required):
            raise ValueError("参考必须显式提供全部解释映射；允许 fixture 显式空映射")
        # BGPInfo 的五类列表文本安全解释；其余整行和来源仍保留。
        for key, row in self.info.as_info.items():
            for field in ("import_as", "export_as", "v4Peer", "v6Peer", "sibling_as"):
                if field in row and isinstance(row[field], str):
                    raw = row[field]
                    try:
                        parsed = ast.literal_eval(raw) if raw else []
                        if not isinstance(parsed, (list, tuple, set)):
                            raise ValueError("参考关系字段必须为集合")
                    except (ValueError, SyntaxError) as error:
                        raise ValueError(
                            f"参考解析失败 as_info/{key}/{field}: {raw!r}: {error}"
                        ) from error
                    row[field] = parsed
        self.output.context = {
            "baseline_ref": seed.baseline_ref,
            "initialization": seed.initialization,
        }
        for item in self._seed_observations(seed):
            self._check_identity(item)
            if item.action != "RIB":
                raise ValueError("基线只允许显式 RIB 观察")
            prefix = network_facts(item.prefix, strict=False).text
            if prefix in ("0.0.0.0/0", "::/0"):
                continue
            if not self.projection.t:
                self.projection.t = _local(item.observed_at)
            # RIB 含 AS_SET 的旧投影原样保留；A 才排除含{。
            if legacy_origin(item.raw_path) != "":
                self.projection.apply("RIB", prefix, item.legacy_vp, item.raw_path)
        self.outage = BGPOutage(self.output, self.info, self.projection)
        self.hijack = BGPHijack(self.output, self.info, self.projection)
        self.subhijack = BGPSubHijack(self.output, self.info, self.projection)
        self.leak = BGPLeak(self.output, self.info, self.projection)
        for name, module, attr in [
            ("prefix", self.outage, "prefix_init_id"),
            ("as", self.outage, "as_init_id"),
            ("country", self.outage, "country_init_id"),
            ("moas", self.hijack, "moas_init_id"),
            ("hijack", self.hijack, "hijack_init_id"),
            ("sub_hijack", self.subhijack, "sub_hijack_init_id"),
            ("leak_phenomenon", self.leak, "phenomenon_init_id"),
            ("leak", self.leak, "event_init_id"),
        ]:
            setattr(module, attr, deepcopy(dict(seed.legacy_ids.get(name, {}))))
        self.outage.init_outage(self.projection.prefix_dict)
        self.hijack.init_hijack(self.projection.prefix_dict)
        self.subhijack.init_sub_hijack(self.projection.prefix_dict)
        self.leak.init_leak()

    def _copy_seed(self, seed):
        return deepcopy(seed)

    def _copy_references(self, references):
        return deepcopy(references)

    def _reference_info(self, references):
        return SimpleNamespace(**deepcopy(dict(references.mappings)))

    def _seed_observations(self, seed):
        return iter(seed.observations)

    def _check_identity(self, item):
        if not all(
            (
                item.observation_id,
                item.source_id,
                item.source_version,
                item.peer_ref,
                item.legacy_vp,
                item.raw_record_ref,
            )
        ):
            raise ValueError("观察、来源、原记录、Peer 和 legacy VP 引用不能为空")
        if item.collector_id != self.scope.collector_id:
            raise ValueError("观察 collector 与计算范围冲突")
        _local(item.observed_at)

    def begin_file(self, boundary: FileBoundary):
        if self.status in ("failed", "partial"):
            raise ValueError("失败计算不可继续")
        if self.file_boundary is not None:
            raise ValueError("必须先结束上一文件")
        if any(
            not isinstance(value, str) or not value.strip()
            for value in (boundary.file_id, boundary.source_version)
        ):
            raise ValueError("文件身份和来源版本必须是非空字符串")
        if not isinstance(boundary.observed_at, str):
            raise ValueError("文件时点必须是含时区的时间字符串")
        try:
            instant = datetime.fromisoformat(
                boundary.observed_at.replace("Z", "+00:00")
            )
        except ValueError as error:
            raise ValueError("文件时点格式非法") from error
        if instant.tzinfo is None:
            raise ValueError("文件时点必须含时区")
        if not self.window[0] <= instant < self.window[1]:
            raise ValueError("文件时点在声明计算窗口之外")
        required = (
            "prefix_outage",
            "as_outage",
            "country_outage",
            "event",
            "moas",
            "hijack",
            "sub_hijack",
            "leak_phenomenon",
            "leak",
        )
        if any(not boundary.legacy_tables.get(k) for k in required):
            raise ValueError("各 legacy 月表引用必须显式提供")
        self.file_boundary = deepcopy(boundary)
        t = boundary.legacy_tables
        self.outage.set_table(
            t["prefix_outage"], t["as_outage"], t["country_outage"], t["event"]
        )
        self.hijack.set_table(t["moas"], t["hijack"], t["event"])
        self.subhijack.set_table(t["sub_hijack"], t["event"])
        self.leak.set_table(t["leak_phenomenon"], t["leak"], t["event"])

    def _revisions(self):
        for kind, module, attr in [
            ("prefix_outage", self.outage, "prefix_outage_event"),
            ("as_outage", self.outage, "as_outage_event"),
            ("country_outage", self.outage, "country_outage_event"),
            ("moas", self.hijack, "moas_event_dict"),
            ("sub_hijack", self.subhijack, "sub_hijack_dict"),
        ]:
            for obj, records in getattr(module, attr).items():
                for ident, record in records.items():
                    self.output.event_revision(kind, obj, ident, record)

    def _input_context(self, item):
        return {
            "observation": asdict(item),
            "file": asdict(self.file_boundary),
            "time_semantics": "legacy_trigger_call; not_validated_regular_slot",
        }

    def _run_rules(self, t, action, prefix, vp, path):
        """通用输入与直接列批共用同一有序业务规则。"""
        old = self.projection.at(prefix)
        self._rule_stage = "compatibility_projection"
        self.projection.apply(action, prefix, vp, path)
        new = self.projection.at(prefix)
        self.output.context["compatibility_before"] = old
        self.output.context["compatibility_after"] = new
        self.output.context["projection_version"] = self.projection.version
        self.output.append(
            "compatibility_transition", {"prefix": prefix, "old": old, "new": new}
        )
        self._rule_stage = "hijack"
        self.hijack.hijack_detect(
            t, prefix, old["origins"], new["origins"], new["vp_paths"]
        )
        self._rule_stage = "subhijack"
        self.subhijack.sub_hijack_detect(t, prefix)
        self._rule_stage = "leak"
        self.leak.leak_detect(t, action, prefix, vp, path)
        self._rule_stage = "outage"
        self.outage.outage_detect(
            t,
            action,
            prefix,
            vp,
            path,
            old["origins"],
            old["vp_paths"],
            new["origins"],
            new["vp_paths"],
        )
        if self.output.errors:
            raise ValueError("参考解释失败，不能按成功继续")
        self._revisions()
        self.processed += 1
        self.status = "computed"

    def consume(self, item: DetectionInput):
        if self.status in ("failed", "partial") or self.file_boundary is None:
            raise ValueError("需有效文件边界且计算未失败")
        start = self.output.position
        self._rule_stage = "input"
        self.output.context = self._input_context(item)
        try:
            self._check_identity(item)
            if item.action not in ("A", "W", "STATE"):
                raise ValueError("UPDATE 只支持 A/W/STATE")
            instant = datetime.fromisoformat(item.observed_at.replace("Z", "+00:00"))
            if not self.window[0] <= instant < self.window[1]:
                raise ValueError("UPDATE 在声明计算窗口之外")
            if item.source_version != self.file_boundary.source_version:
                raise ValueError("文件与观察来源版本不一致")
            if item.observation_id in self.seen_observations:
                raise ValueError("重复观察引用；计算不得重复累计")
            self.seen_observations.add(item.observation_id)
            if item.action == "STATE":
                self.output.append("filtered", {"reason": "detection_state"})
                return self.output.since(start)
            prefix = network_facts(item.prefix, strict=False).text
            reason = (
                "default_route"
                if prefix in ("0.0.0.0/0", "::/0")
                else (
                    "announcement_as_set"
                    if item.action == "A" and "{" in item.raw_path
                    else None
                )
            )
            if reason:
                self.output.append("filtered", {"reason": reason})
                return self.output.since(start)
            t = _local(item.observed_at)
            self._run_rules(t,item.action,prefix,item.legacy_vp,item.raw_path)
            self.last_observation = item.observation_id
        except Exception as error:
            self.status = (
                "partial"
                if self.processed or self.output.position > start
                else "failed"
            )
            self.output.append(
                "failed",
                {
                    "stage": self._rule_stage,
                    "exception": type(error).__name__,
                    "reason": str(error),
                    "publication_eligible": False,
                },
            )
        return self.output.since(start)

    def finish_file(self):
        if self.file_boundary is None:
            raise ValueError("没有打开的文件")
        start = self.output.position
        self.output.context = {
            "file": asdict(self.file_boundary),
            "last_observation_id": self.last_observation,
            "phase": "file_tail",
        }
        if self.status not in ("failed", "partial"):
            try:
                self.outage.update_outage_event_table()
                self._revisions()
                self.output.append(
                    "file_finished",
                    {"status": "computed", "publication_eligible": False},
                )
            except Exception as error:
                self.status = "partial"
                self.output.append(
                    "failed",
                    {
                        "stage": "outage_file_tail",
                        "exception": type(error).__name__,
                        "reason": str(error),
                        "publication_eligible": False,
                    },
                )
        else:
            self.output.append(
                "file_finished", {"status": self.status, "publication_eligible": False}
            )
        self.file_boundary = None
        return self.output.since(start)

    def export_state(self):
        modules = {}
        for name in ("outage", "hijack", "subhijack", "leak"):
            modules[name] = {
                k: deepcopy(v)
                for k, v in vars(getattr(self, name)).items()
                if k not in ("output", "bgp_info", "bgp_rib")
            }
        return plain(
            {
                "schema_version": "detection-business-state/v1",
                "scope": asdict(self.scope),
                "status": self.status,
                "publication_eligible": False,
                "processed": self.processed,
                "baseline": asdict(self.seed),
                "references": asdict(self.references),
                "thresholds": self.output.thresholds,
                "compatibility_projection": self.projection.export(),
                "modules": modules,
                "rows": self.output.rows,
                "identities": self.output.identities,
                "legacy_start_tables": self.output.start_tables,
                "revisions": self.output.revisions,
                "quality_errors": self.output.errors,
                "legacy_projection_quality": "partial"
                if self.output.projection_gaps
                else "captured_not_persisted",
                "projection_gaps": self.output.projection_gaps,
                "open_file": asdict(self.file_boundary) if self.file_boundary else None,
                "last_observation": self.last_observation,
                "seen_observations": self.seen_observations,
            }
        )
