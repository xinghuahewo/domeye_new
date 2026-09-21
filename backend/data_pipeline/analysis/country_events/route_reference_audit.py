"""C3 新增行的有限本地关系审计；实际上游证明由公开输入层另行闭合。"""
from collections import Counter
from dataclasses import asdict

from data_pipeline.analysis.country_events import qualified_schema as schema
from data_pipeline.analysis.country_events.snapshot_schema import encode
from data_pipeline.analysis.country_events.event_aggregation import C2Row, EventStatus
from data_pipeline.analysis.country_events.route_contract import CountryQualification, CountryCoverage, CountryQualifiedValue


class M3RowAudit:
    """一遍收集原目标和新资格；总预算跨对象共享，尾部再核前向引用。

    通过只证明本地typed关系，不是完整C3 Admission；不能替代上游原证据核对、
    Coverage枚举摘要、完整源回执、组件持久化回读或C4验收。
    """
    def __init__(self, logical_run_id, input_binding_id, required_coverage, *,
                 max_rows, max_bytes, max_intervals, max_references_per_row,
                 max_total_references, max_overlap_steps, max_dimensions_per_target, guard, store=None):
        self.logical_run_id, self.input_binding_id = logical_run_id, input_binding_id
        self.required_coverage = frozenset(required_coverage)
        self.limits = dict(rows=max_rows, bytes=max_bytes, intervals=max_intervals,
                          references_per_row=max_references_per_row, references=max_total_references,
                          overlap_steps=max_overlap_steps, dimensions=max_dimensions_per_target)
        self.guard = guard
        self.stats = Counter()
        self.resident_rows=self.resident_bytes=0
        self.targets, self.qualifications, self.coverages = {}, {}, {}
        self.store=store
        self.values,self.intervals,self.dimensions=[],{},{}
        if store is not None:
            from data_pipeline.analysis.country_events.calculation_staging import ScientificRows, QualificationMap
            self.targets=store.new_map();self.qualifications=QualificationMap(store)
            self.values=ScientificRows(store);self.intervals=store.new_groups();self.dimensions=store.new_map()
        self.events = set()
        self.closed = False
        self.failed = False
        self.check()

    def check(self):
        self.guard()
        if any(type(n) is not int or n <= 0 for n in self.limits.values()):
            raise ValueError('M3 本地审计预算无效')
        for key in (() if self.store is not None else ('rows', 'bytes', 'intervals')):
            if self.stats[key] > self.limits[key]:
                raise ValueError('resource_limit:M3_audit_' + key)

    def charge(self, key, amount):
        self.check()
        if self.store is None and key in ('rows','bytes','intervals') and self.stats[key] + amount > self.limits[key]:
            raise ValueError('resource_limit:M3_audit_' + key)
        self.stats[key] += amount

    def retain_metadata(self, value):
        # events/coverage仍为驻留集合；只给其实际占用计费，不限制磁盘历史。
        self.resident_rows+=1;self.resident_bytes+=len(encode(value).encode())
        if self.resident_rows>self.limits['rows'] or self.resident_bytes>self.limits['bytes']:
            raise ValueError('resource_limit:M3_audit_resident_metadata')

    def append(self, sequence, item):
        if self.failed:
            raise ValueError('M3 本地审计已失败')
        try:
            return self._append(sequence, item)
        except BaseException:
            self.failed = True
            raise

    def _append(self, sequence, item):
        self.check()
        if self.closed:
            raise ValueError('M3 本地审计已关闭')
        if type(sequence) is not int or sequence != self.stats['rows']:
            raise ValueError('M3 全局typed行序必须连续且唯一')
        value = item.value if isinstance(item, C2Row) else item
        if isinstance(value, CountryQualification):
            refs = sum(len(v) for v in (value.gap_refs, value.upstream_qualification_refs,
                                       value.evidence_refs, value.recovery_witnesses))
        elif isinstance(value, CountryCoverage):
            refs = len(value.unassigned_scope_refs) + len(value.completion_receipt_refs)
        elif isinstance(value, CountryQualifiedValue):
            refs = len(value.qualification_refs) + len(value.raw_basis_refs)
        else:
            refs = 0
        if refs > self.limits['references_per_row']:
            raise ValueError('resource_limit:M3_audit_references_per_row')
        self.charge('references', refs)
        table, row = schema.row_encode(sequence, item)
        self.charge('rows', 1)
        self.charge('bytes', len(encode(row).encode()))
        if table not in schema.NEW_TABLES:
            if (table, sequence) in self.targets:
                raise ValueError('M3 原typed目标重复')
            self.targets[table, sequence] = (row['_incident_id'], row['_revision'])
            if isinstance(value, EventStatus):
                if value.incident.incident_id not in self.events:self.retain_metadata(value.incident.incident_id)
                self.events.add(value.incident.incident_id)
            return
        if (value.logical_run_id, value.input_binding_id) != (self.logical_run_id, self.input_binding_id):
            raise ValueError('M3 新行跨逻辑run或原绑定')
        if isinstance(value, CountryQualification):
            if value.qualification_id in self.qualifications:
                raise ValueError('M3 资格重复')
            self.charge('intervals', 1)
            target_key = (value.target_kind, value.raw_target_ref, value.incident_id, value.revision,
                          value.cohort_id, value.entity_key, value.afi, value.metric, value.sample_us)
            dimensions=set(self.dimensions.get(target_key,()))
            if value.dimension not in dimensions and len(dimensions) >= self.limits['dimensions']:
                raise ValueError('resource_limit:M3_audit_dimensions')
            dimensions.add(value.dimension);self.dimensions[target_key]=tuple(sorted(dimensions))
            key = (*target_key, value.dimension, value.dependency_scope)
            candidates = self.intervals.get(key, ())
            self.charge('overlap_steps', len(candidates))
            for prior in candidates:
                if self.store is not None:prior=CountryQualification(**prior['value'])
                self.guard()
                time_overlap = max(prior.window_us[0], value.window_us[0]) < min(prior.window_us[1], value.window_us[1])
                pstart, pend = prior.effective_start_position, prior.effective_end_position
                start, end = value.effective_start_position, value.effective_end_position
                ordered_overlap = (pend is None or start is None or start < pend) and (end is None or pstart is None or pstart < end)
                if time_overlap and ordered_overlap:
                    raise ValueError('M3 同目标维度及原依赖范围的资格区间重叠')
            if self.store is None:self.intervals.setdefault(key,[]).append(value)
            else:self.intervals[key].append(dict(sequence=sequence,value=asdict(value)))
            self.qualifications[value.qualification_id] = value
        elif isinstance(value, CountryCoverage):
            key = (value.window_us, value.dimension, value.source_id)
            if key in self.coverages:
                raise ValueError('M3 模块覆盖重复')
            self.retain_metadata(asdict(value))
            self.coverages[key] = value
        else:
            if self.store is None:self.values.append((value,row['_incident_id'],row['_revision']))
            else:self.values.append(sequence,item)

    def finish(self):
        self.check()
        if self.closed or self.failed:
            raise ValueError('M3 本地审计重复结束')
        def target(ref, outer=None):
            if ref not in self.targets or outer is not None and self.targets[ref] != outer:
                raise ValueError('M3 原typed目标缺失或事件绑定冲突')
        for q in self.qualifications.values():
            self.guard()
            if q.raw_target_ref is not None:
                target(q.raw_target_ref, (q.incident_id, str(q.revision) if q.revision is not None else None))
        for value,incident,revision in self.value_rows():
            self.guard()
            target(value.raw_target_ref, (incident, revision))
            target(value.population_ref)
            if any(ref not in self.qualifications for ref in value.qualification_refs):
                raise ValueError('M3 主值资格引用缺失')
            own = [self.qualifications[ref] for ref in value.qualification_refs
                   if self.qualifications[ref].dimension == value.dimension
                   and self.qualifications[ref].raw_target_ref == value.raw_target_ref]
            if not own or value.coverage == 'complete' and any(q.coverage != 'complete' for q in own):
                raise ValueError('M3 主值没有同目标维度的充分资格')
        present = {(w, d) for w, d, source in self.coverages if source is None}
        if present != self.required_coverage:
            raise ValueError('M3 必需模块覆盖缺项或多项')
        for coverage in self.coverages.values():
            if coverage.source_id is None and coverage.execution == 'complete' and coverage.event_count != len(self.events):
                raise ValueError('M3 覆盖事件数与实际原事件枚举不符')
        self.closed = True
        return dict(self.stats)

    def close(self):
        if self.store is not None:
            from data_pipeline.analysis.country_events.snapshot_store import cleanup
            cleanup(tuple(item.close for item in (self.targets,self.qualifications,self.values,self.intervals,self.dimensions)))

    def value_rows(self):
        if self.store is None:yield from self.values
        else:
            for r in self.values:yield r.value,r.incident_id,str(r.revision) if r.revision is not None else None
