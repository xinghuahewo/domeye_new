"""显式M3的24表Writer/Reader，复用原物理写入、固定快照回读和登记流程。"""
from collections import Counter
from dataclasses import asdict
import hashlib

from data_pipeline.analysis.country_events import snapshot_store as component, snapshot_reader as component_reader, snapshot_audit as component_audit, qualified_schema as m3_schema
from data_pipeline.analysis.country_events.snapshot_schema import encode, decode
from data_pipeline.analysis.country_events.route_reference_audit import M3RowAudit
from data_pipeline.analysis.country_events.route_contract import DIMENSIONS
from data_pipeline.analysis.country_events.event_aggregation import C2Row
from data_pipeline.analysis.country_events import snapshot_schema as component_schema

AUDIT_LIMITS = dict(max_rows=10000000, max_bytes=512*1024**2, max_intervals=1000000,
                    max_references_per_row=128, max_total_references=10000000,
                    max_overlap_steps=1000000, max_dimensions_per_target=12)


def upstream(source):
    source.check(); source.inputs.verify(); source.verify_saved()
    return dict(profile='country-m3-source/v1', input_binding_id=source.binding.input_binding_id,
                admissions=tuple(source.inputs.admissions[k] for k in sorted(source.inputs.admissions)),
                reads=tuple(source.receipts[k] for k in sorted(source.receipts)),
                legacy_timezone=source.timezone, windows=source.windows)


def digest_ids(ids):
    hasher=hashlib.sha256();hasher.update(b'["tuple",[')
    for i,value in enumerate(ids):
        if i:hasher.update(b',')
        hasher.update(encode(value).encode())
    hasher.update(b']]');return hasher.hexdigest()


def validate_new(db, source, logical_run_id, window, limits, guard):
    audit = M3RowAudit(logical_run_id, source.binding.input_binding_id,
                       {(window, dimension) for dimension in DIMENSIONS}, guard=guard,store=source.store, **limits)
    added_count=0
    for seq, table, payload in db.execute('SELECT sequence,table_name,payload FROM rows ORDER BY sequence'):
        item = m3_schema.row_decode(table, decode(payload))
        audit.append(seq, item)
        if table in m3_schema.NEW_TABLES:added_count+=1
        elif added_count:raise ValueError('M3 C3原科学行必须保持完整前缀')
    audit.finish()

    def resolve(ref, receipt=False):
        owner, aid, view, key = ref
        saved = source.receipts.get((aid, view))
        if saved is None or source.inputs.admissions[aid]['owner'] != owner:
            raise ValueError('M3 C3原证据没有实际完整来源绑定')
        if receipt:
            if key != 'receipt:' + saved['receipt']['request_digest']:
                raise ValueError('M3 C3完整回执引用不符')
            return saved['receipt']
        if not key.isdecimal() or str(int(key)) != key or int(key) >= len(source.rows[aid, view]):
            raise ValueError('M3 C3原证据行定位无效')
        return source.rows[aid, view][int(key)]

    for q in audit.qualifications.values():
        guard()
        for group in (q.evidence_refs, q.gap_refs, q.upstream_qualification_refs, q.recovery_witnesses):
            for ref in group:resolve(ref)
        for ref in q.gap_refs:
            row = resolve(ref)
            if not (ref[0:1] == ('canonical',) and ref[2] == 'scope_gap'
                    or ref[0] == 'detection' and ref[2] == 'm3_entries' and row['kind'] == 'scope_gap'):
                raise ValueError('M3 C3Gap引用不是原独立Gap')
        for ref in q.recovery_witnesses:
            row = resolve(ref)
            if (ref[0] != 'canonical' or ref[2] != 'changes'
                    or row['raw']['calculation_after']['presence'] not in ('present', 'absent')):
                raise ValueError('M3 C3恢复见证不是原精确对象后态')
    for value, _, _ in audit.value_rows():
        for ref in value.raw_basis_refs:guard();resolve(ref)
    for coverage in audit.coverages.values():
        guard()
        count=0;gaps=set(coverage.unassigned_scope_refs)
        def ids():
            nonlocal count
            for q in audit.qualifications.values():
                guard()
                if q.dimension==coverage.dimension and q.window_us==coverage.window_us:
                    count+=1;gaps.update(q.gap_refs)
                    if len(gaps)>limits['max_references_per_row']:raise ValueError('resource_limit:M3_qualification_refs')
                    yield q.qualification_id
        actual_digest=digest_ids(ids());gaps=sorted(gaps)
        if (coverage.qualification_count,coverage.qualification_digest)!=(count,actual_digest):
            raise ValueError('M3 C3资格覆盖行数或摘要不符')
        if (coverage.gap_count, coverage.gap_digest) != (len(gaps), digest_ids(gaps)):
            raise ValueError('M3 C3Gap覆盖行数或摘要不符')
        for ref in coverage.unassigned_scope_refs:resolve(ref)
        for ref in coverage.completion_receipt_refs:resolve(ref, receipt=True)
    # 同一已验证原输入与原科学目标重新推导有限资格，不能仅凭存在的引用
    # 把Unknown伪签为complete。这里不重新运行Detection或改写原科学输出。
    from data_pipeline.analysis.country_events.result_qualification import qualification_rows
    def originals():
        for table,payload in db.execute('SELECT table_name,payload FROM rows ORDER BY sequence'):
            if table not in m3_schema.NEW_TABLES:yield m3_schema.row_decode(table,decode(payload))
    expected = qualification_rows(source, logical_run_id, originals(),
                                   max_rows=min(limits['max_rows'], source.max_rows),
                                   max_bytes=min(limits['max_bytes'], source.max_bytes),
                                   max_references_per_row=limits['max_references_per_row'], guard=guard)
    from itertools import zip_longest
    def added():
        for table,payload in db.execute('SELECT table_name,payload FROM rows ORDER BY sequence'):
            if table in m3_schema.NEW_TABLES:yield m3_schema.row_decode(table,decode(payload))
    primary=None
    try:
        for actual,wanted in zip_longest(added(),expected):
            guard()
            if actual is None or wanted is None or encode(asdict(actual))!=encode(asdict(wanted)):
                raise ValueError('M3 C3逐维资格或主值不符合完整原依赖')
    except BaseException as error:primary=error;raise
    finally:component.cleanup((expected.close,),primary)
    audit.close()
    return dict(audit.stats)


class M3ComponentWriter(component.ComponentWriter):
    tables, schemas, schema_version = m3_schema.TABLES, m3_schema.SCHEMAS, m3_schema.SCHEMA_VERSION
    kind = 'country-m3-typed'
    encode_row, decode_row = staticmethod(m3_schema.row_encode), staticmethod(m3_schema.row_decode)

    def __init__(self, dsn, root, *, source, logical_run_id, window_us, audit_limits=None, parameters=None, **limits):
        self.source = source
        selected = {**AUDIT_LIMITS, **(audit_limits or {})}
        if set(selected) != set(AUDIT_LIMITS):raise ValueError('M3 C3审计预算字段不受支持')
        params = dict(parameters or {}, logical_run_id=logical_run_id, result_window_us=tuple(window_us), m3_audit_limits=selected)
        super().__init__(dsn, root, parameters=params, **limits)

    def guard(self):
        super().guard(); self.source.check()

    def upstream(self):
        return upstream(self.source)

    def check_append(self, item):
        is_new = isinstance(item, C2Row) and type(item.value) in m3_schema.BY_CLASS
        if is_new != (self.completion is not None):
            raise ValueError('M3 C3先保留完整原C2，再追加三个资格表；不得改原行序')

    def check_completion_count(self):
        raw_rows = sum(self.counts[t] for t in component_schema.TABLES)
        if self.completion.costs.get('output_rows', 0) != raw_rows - 1:
            raise ValueError('M3 C3原C2完成计数不符')

    def validate(self):
        if self.completion.input_kind != 'saved_M3':
            raise ValueError('M3 C3不能以typed fixture或旧C1冒充完整M3输入')
        grid = self.parameters.get('grid', {})
        if (grid.get('input_start_us'), grid.get('input_end_us')) != self.completion.scope:
            raise ValueError('M3 C3实际计算窗不符')
        availability = component_audit.validate(self.spool, self.completion, None, self.guard, self.stats,
                                               m3_source=self.source, row_decoder=self.decode_row)
        self.stats.update({'m3_' + k: v for k, v in validate_new(self.spool, self.source,
                          self.parameters['logical_run_id'], self.parameters['result_window_us'],
                          self.parameters['m3_audit_limits'], self.guard).items()})
        return availability


class M3ComponentReader(component_reader.ComponentReader):
    tables, schemas, schema_version = m3_schema.TABLES, m3_schema.SCHEMAS, m3_schema.SCHEMA_VERSION
    kind = 'country-m3-typed'
    decode_row = staticmethod(m3_schema.row_decode)

    def __init__(self, dsn, binding, *, source, **limits):
        self.source = source
        runtime=getattr(source,'query_runtime',None)
        if runtime is not None:
            self.lake_connection=runtime.lake_connect
            self.temporary_root=runtime.scratch_root
        super().__init__(dsn, binding, **limits)

    def upstream(self):
        return upstream(self.source)

    def validate(self, index, completion):
        if completion.input_kind != 'saved_M3':
            raise ValueError('M3 C3读取缺完整M3输入证明')
        params = decode(self.manifest['parameters'])
        component_audit.validate(index, completion, None, self.guard, Counter(),
                                 m3_source=self.source, row_decoder=self.decode_row)
        validate_new(index, self.source, params['logical_run_id'], params['result_window_us'],
                     params['m3_audit_limits'], self.guard)
