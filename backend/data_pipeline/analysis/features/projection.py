"""Feature 私有兼容投影：消费已保存观察，不调用 MRT 解析或规范 Replay。"""
from collections.abc import Mapping, Set
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from types import MappingProxyType
from zoneinfo import ZoneInfo
from zlib import crc32

from data_pipeline.analysis.features.calculation import RULES, FileWindow, InputGap, Observation, Projection, calculate_file, initialize, observation_filter, prefix_filter
from data_pipeline.bgp.input.path_decoding import DECODER_VERSION, DIRECTION_RULE
from data_pipeline.bgp.replay.route_replay import identity, legacy_origin

PROJECTION_RULE = 'feature-private-projection/v2'


class FrozenIndex(Mapping):
    """分片不可变索引；每文件只复制被写入的桶，旧版本永久可读。"""
    BUCKETS = 128

    @classmethod
    def bucket_number(cls, key):
        return crc32(key.encode('utf-8')) % cls.BUCKETS

    def __init__(self, buckets=None):
        self._buckets = buckets or tuple(MappingProxyType({}) for _ in range(self.BUCKETS))

    def __getitem__(self, key):
        return self._buckets[self.bucket_number(key)][key]

    def __iter__(self):
        for bucket in self._buckets:
            yield from bucket

    def __len__(self):
        return sum(map(len, self._buckets))

    def __deepcopy__(self, memo):
        return self

    def edit(self):
        return IndexEdit(self)


class IndexEdit(Mapping):
    def __init__(self, base):
        self.base, self.changed = base, {}
        self.copied_entries = 0
        self.sealed = False

    def _bucket(self, key, write=False):
        if write and self.sealed:
            raise RuntimeError('已冻结编辑器不可继续写入')
        number = self.base.bucket_number(key)
        if write and number not in self.changed:
            self.changed[number] = dict(self.base._buckets[number])
            self.copied_entries += len(self.changed[number])
        return self.changed.get(number, self.base._buckets[number])

    def __getitem__(self, key):
        return self._bucket(key)[key]

    def __setitem__(self, key, value):
        self._bucket(key, True)[key] = value

    def discard(self, key):
        if key in self:
            self._bucket(key, True).pop(key)

    def __iter__(self):
        for number, bucket in enumerate(self.base._buckets):
            yield from self.changed.get(number, bucket)

    def __len__(self):
        return sum(1 for _ in self)

    def freeze(self):
        self.sealed = True
        return FrozenIndex(tuple(MappingProxyType(self.changed[n]) if n in self.changed else b
                                 for n, b in enumerate(self.base._buckets)))


class FrozenMembers(Set):
    def __init__(self, index=None):
        self.index = index if index is not None else FrozenIndex()

    def __contains__(self, key):
        return key in self.index

    def __iter__(self):
        return iter(self.index)

    def __len__(self):
        return len(self.index)

    def __deepcopy__(self, memo):
        return self

    def edit(self):
        return self.index.edit()


@dataclass(frozen=True)
class SourceBinding:
    source_id: str
    content_sha256: str
    role: str
    window: FileWindow
    expected_elements: int
    message_quality_refs: tuple[str, ...] = ()
    message_quality_state: str = "unknown"


@dataclass(frozen=True)
class FeaturePlan:
    observation_version: str
    collector: str
    sources: tuple[SourceBinding, ...]
    limitations: tuple[str, ...] = ("session_continuity_unknown", "cutover_assumed", "fixed_path_rendering")

    def __post_init__(self):
        if not self.observation_version or not self.collector or not self.sources:
            raise ValueError("缺少观察版本/来源计划")
        if self.sources[0].role != "baseline" or any(s.role != "update" for s in self.sources[1:]):
            raise ValueError("必须先基线后UPDATE")
        if len({s.source_id for s in self.sources}) != len(self.sources):
            raise ValueError("来源身份重复")
        for source in self.sources:
            if source.window.input_version != self.observation_version or source.expected_elements < 0:
                raise ValueError("来源版本/计数不匹配")
            if source.message_quality_state not in ('complete', 'partial', 'unknown'):
                raise ValueError('消息scope质量无效')
            if source.message_quality_state == 'complete' and not source.message_quality_refs:
                raise ValueError('完整消息scope必须有同版证据引用')
        for left, right in zip(self.sources[1:], self.sources[2:]):
            if right.window.start < left.window.end:
                raise ValueError("UPDATE窗口重叠或倒序")
        if len(self.sources) > 1 and self.sources[0].window.start > self.sources[1].window.start:
            raise ValueError('基线晚于首个UPDATE窗口')


class FeatureAdapter:
    """每个实例拥有一个独立普通/IR投影；失败不提交工作态或投影。"""
    def __init__(self, mode, reference, plan, execution_id, *, audit_sink=None, row_sink=None, retain_outputs=True):
        self.mode, self.reference, self.plan, self.execution_id = mode, reference, plan, execution_id
        self.state = None
        self.cursor = 0
        self.last_audit = ()
        self.copy_metrics = {}
        self.audit_sink, self.row_sink, self.retain_outputs = audit_sink, row_sink, retain_outputs

    def consume_source(self, binding, batches):
        if self.cursor >= len(self.plan.sources) or binding != self.plan.sources[self.cursor]:
            raise ValueError("来源不符合冻结顺序")
        old = self.state.projection if self.state else Projection(
            identity([self.plan.observation_version, PROJECTION_RULE, RULES[self.mode], self.reference.version, "empty"]),
            RULES[self.mode], FrozenIndex(), FrozenIndex(), FrozenIndex(), frozenset(),
            binding.source_id, "", "IR" if self.mode == "ir" else None, "unknown")
        paths, origins, members = old.prefix_dict.edit(), old.prefix_as.edit(), old.as_prefix.edit()
        member_edits = {}
        seen = set(old.seen_vps)
        audit = []
        rib_time = old.rib_time
        count, last = 0, (-1, -1)
        changed_path_members = 0
        input_gaps = old.input_gaps
        if binding.role != 'baseline':
            previous_source = self.plan.sources[self.cursor - 1]
            previous_end = self.state.last_window_end or previous_source.window.end
            if binding.window.start > previous_end:
                input_gaps += (InputGap(previous_source.source_id, binding.source_id,
                                       previous_end, binding.window.start),)

        def membership(asn):
            if asn not in member_edits:
                member_edits[asn] = members.get(asn, FrozenMembers()).edit()
            return member_edits[asn]

        def consume_rows():
            nonlocal count, last, rib_time, changed_path_members
            for batch in batches:
                for row in batch.to_pylist():
                    count += 1
                    if row["source_id"] != binding.source_id or row["content_sha256"] != binding.content_sha256:
                        raise ValueError("观察来源/内容身份不匹配")
                    position = (row["record"], row["ordinal"])
                    if position <= last:
                        raise ValueError("观察重复或倒序")
                    last = position
                    timestamp = datetime.fromtimestamp(row["epoch"], timezone.utc).replace(microsecond=row.get("microsecond") or 0)
                    if not binding.window.start <= timestamp < binding.window.end:
                        raise ValueError("观察超出绑定时间")
                    if binding.role == 'baseline' and len(self.plan.sources) > 1 and timestamp > self.plan.sources[1].window.start:
                        raise ValueError('基线实际记录晚于UPDATE切点')
                    action = row["action"]
                    if (binding.role == "baseline") != (action == "rib_snapshot"):
                        raise ValueError("观察action与来源role冲突")
                    if action not in ("rib_snapshot", "announce", "withdraw"):
                        raise ValueError("非路由元素")
                    prefix, _ = prefix_filter(row["prefix"])
                    vp, text = str(row["peer_asn"]), row["as_path_text"]
                    before_paths = paths.get(prefix, {}) if prefix is not None else {}
                    old_path = before_paths.get(vp, "")
                    before_origins = origins.get(prefix, frozenset()) if prefix is not None else frozenset()
                    item = Observation(row["event_id"], count - 1, timestamp,
                        "A" if action == "announce" else "W", row["prefix"], vp, text, old_path,
                        before_origins, before_origins, RULES[self.mode], None)
                    skip = None
                    projection_skip = None
                    origin = legacy_origin(text)
                    if action == "rib_snapshot":
                        if prefix is None or prefix.endswith("/0"):
                            skip = "legacy_invalid_or_default_route"
                        else:
                            if not rib_time:
                                rib_time = timestamp.astimezone(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                            if not origin:
                                skip = "legacy_empty_origin"
                            elif self.mode == "ir" and self.reference.code(origin) != "IR":
                                skip = "non_ir_rib"
                    else:
                        skip = observation_filter(self.mode, item, self.reference)
                        origin = origin if origin is not None else ""
                        if not skip and action == "announce" and self.mode == "ir" and origin and self.reference.code(origin) != "IR":
                            projection_skip = "non_ir_bgprib_origin"
                    if not skip and not projection_skip:
                        updated_paths = dict(before_paths)
                        changed_path_members += len(before_paths)
                        updated_origins = set(before_origins)
                        if action == "withdraw":
                            if vp in updated_paths:
                                removed = legacy_origin(updated_paths.pop(vp))
                                removed = removed if removed is not None else ""
                                remaining = {legacy_origin(v) if legacy_origin(v) is not None else "" for v in updated_paths.values()}
                                if removed not in remaining:
                                    updated_origins.discard(removed)
                                    membership(removed).discard(prefix)
                        else:
                            seen.add(vp)
                            updated_paths[vp] = text
                            updated_origins.add(origin)
                            membership(origin)[prefix] = True
                        if updated_paths:
                            paths[prefix] = MappingProxyType(updated_paths)
                        else:
                            paths.discard(prefix)
                        if updated_origins:
                            origins[prefix] = frozenset(updated_origins)
                        else:
                            origins.discard(prefix)
                    audit_row = {"event_id": row["event_id"], "source_id": binding.source_id,
                        "observation_version": self.plan.observation_version, "reference_version": self.reference.version,
                        "action": action, "raw_prefix": row['prefix'], "prefix": prefix, "vp": vp,
                        "before_path_exists": vp in before_paths,
                        "after_path_exists": prefix is not None and vp in paths.get(prefix, {}),
                        "after_path": paths.get(prefix, {}).get(vp) if prefix is not None else None,
                        "local_message": row.get('local_message'), "direction_rule": row.get('direction_rule', 'synthetic_explicit_vp'),
                        "path_key": row["path_key"], "skip": skip, "projection_skip": projection_skip,
                        "before_path": old_path, "before_origins": before_origins,
                        "after_origins": origins.get(prefix, frozenset()) if prefix is not None else frozenset()}
                    if self.audit_sink is not None:
                        self.audit_sink(audit_row)
                    if self.retain_outputs:
                        audit.append(audit_row)
                    if action != "rib_snapshot":
                        yield replace(item, projection_skip_reason=skip,
                                      after_origins=origins.get(prefix, frozenset()) if prefix is not None else frozenset())
            if count != binding.expected_elements:
                raise ValueError("观察数量与冻结来源回执不匹配")

        def finish_projection():
            for asn, edit in member_edits.items():
                value = FrozenMembers(edit.freeze())
                if value:
                    members[asn] = value
                else:
                    members.discard(asn)
            version = identity([old.version, self.plan.observation_version, self.plan.collector,
                binding.source_id, binding.content_sha256, PROJECTION_RULE, RULES[self.mode], self.reference.version,
                DECODER_VERSION, DIRECTION_RULE, count, binding.window.start.isoformat(), binding.window.end.isoformat(), binding.window.file_time.isoformat(),
                binding.window.coverage, binding.window.limitations, binding.message_quality_refs,
                binding.message_quality_state, self.plan.limitations])
            self.copy_metrics = {"copied_index_entries": sum(e.copied_entries for e in (paths, origins, members, *member_edits.values())),
                                 "edited_asns": len(member_edits), "observations": count,
                                 "changed_prefix_path_members_copied": changed_path_members,
                                 "full_route_snapshot_deepcopies": 0}
            coverage = binding.window.coverage
            if binding.message_quality_state == 'unknown':
                coverage = 'unknown'
            elif binding.message_quality_state == 'partial' and coverage == 'complete':
                coverage = 'partial'
            if binding.role != 'baseline':
                if 'unknown' in (old.coverage, coverage):
                    coverage = 'unknown'
                elif 'partial' in (old.coverage, coverage) or input_gaps:
                    coverage = 'partial'
            return Projection(version, RULES[self.mode], paths.freeze(), origins.freeze(), members.freeze(),
                              frozenset(seen), old.rib_file, rib_time, old.country_filter, coverage, input_gaps)

        if binding.role == "baseline":
            tuple(consume_rows())
            candidate = initialize(self.mode, finish_projection(), self.reference, self.execution_id)
            result = None
        else:
            result = calculate_file(self.state, self.reference,
                replace(binding.window, limitations=binding.window.limitations + self.plan.limitations),
                consume_rows(), finish_projection, row_sink=self.row_sink, retain_rows=self.retain_outputs)
            result = replace(result, projection_audit=tuple(audit), calculation_parameters={
                **result.calculation_parameters, 'observation_version': self.plan.observation_version,
                'collector': self.plan.collector, 'source_id': binding.source_id,
                'content_sha256': binding.content_sha256, 'message_quality_refs': binding.message_quality_refs,
                'message_quality_state': binding.message_quality_state,
                'input_gaps': tuple({'left_source': gap.left_source, 'right_source': gap.right_source,
                    'start': gap.start.isoformat(), 'end': gap.end.isoformat(), 'basis': gap.basis}
                    for gap in input_gaps)})
            candidate = result.next_state
        self.state, self.last_audit = candidate, tuple(audit)
        self.cursor += 1
        return result
