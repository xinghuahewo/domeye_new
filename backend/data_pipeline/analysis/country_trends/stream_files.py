"""S3内部文件段：有限Parquet表、磁盘排序和存储引用核对；不登记或准入。"""
from contextlib import contextmanager
from pathlib import Path
import hashlib
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from data_pipeline.analysis.country_events import snapshot_schema as c3
from data_pipeline.analysis.country_events.snapshot_store import cleanup
from data_pipeline.analysis.country_trends import stream_schema as schema
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
from data_pipeline.analysis.country_trends.stream_budget import StreamBudget, connect
from data_pipeline.analysis.country_trends import part_manifest as parts
from data_pipeline.analysis.country_trends import parquet_metadata as parquet
from data_pipeline.common import process_resources as resources


@contextmanager
def _index(root, budget):
    temporary = tempfile.TemporaryDirectory(prefix='s3-sort-', dir=root)
    db = None
    try:
        resources.add_root(temporary.name)
        budget.scratch_roots.add(Path(temporary.name))
        db = connect(Path(temporary.name) / 'rows.sqlite', budget)
        db.executescript('''CREATE TABLE rows(sequence INTEGER PRIMARY KEY,kind TEXT,payload TEXT,locator TEXT UNIQUE,result_id TEXT);
          CREATE INDEX by_kind ON rows(kind,sequence);
          CREATE INDEX by_result ON rows(result_id);
          CREATE TABLE definitions(role TEXT,key TEXT,locator TEXT,PRIMARY KEY(role,key));
          CREATE TABLE links(role TEXT,key TEXT,source TEXT);''')
        yield db
    finally:
        cleanup((lambda: db.close() if db is not None else None, temporary.cleanup, lambda: resources.remove_root(temporary.name),
                 lambda: budget.scratch_roots.discard(Path(temporary.name))), sys.exc_info()[1])


def _arrow(kind):
    return pa.schema([(name, pa.int64() if typ == 'BIGINT' else pa.string())
                      for name, typ in schema.SCHEMAS[kind]])


def _append(db, sequence, r, budget, *, phase='output'):
    flat = schema.flatten(sequence, r)
    payload = schema.encode(flat)
    budget.charge(payload.encode())
    budget.stats[phase + '_rows'] += 1
    budget.stats[phase + '_bytes'] += len(payload.encode())
    if r.kind == 'context_source' and len(payload.encode()) > budget.limits.max_context_bytes:
        raise ValueError('trend_feature_public_source_budget')
    if len(payload.encode()) > budget.limits.max_batch_bytes:
        raise ValueError('trend_s3_file_row_bytes')
    locator = schema.encode((r.kind, r.event, r.key))
    db.execute('INSERT INTO rows VALUES (?,?,?,?,?)', (sequence, r.kind, payload, locator, r.result_id))
    def define(role, key):
        db.execute('INSERT INTO definitions VALUES (?,?,?)', (role, schema.encode(key), locator))
    def link(role, key):
        budget.refs(1)
        db.execute('INSERT INTO links VALUES (?,?,?)', (role, schema.encode(key), locator))
    define('locator', (r.kind, r.event, r.key))
    v = dict(r.values)
    if r.kind == 'raw_source':
        original = c3.decode(v['raw_typed'])
        if v['source_table'] == 'c2_completion':
            if type(original) is not c3.TABLES['c2_completion'] or r.event:
                raise ValueError('trend_s3_raw_completion')
        else:
            incident, revision, value = original
            if (r.event != (() if incident is None else (incident, revision))
                    or c3.BY_CLASS.get(type(value)) != v['source_table']):
                raise ValueError('trend_s3_raw_source_identity')
        if type(v['source_sequence']) is not int or v['source_sequence'] < 0:
            raise ValueError('trend_s3_raw_sequence')
        define('raw', (v['source_table'], v['source_sequence']))
        define('raw_sequence', v['source_sequence'])
        if v['source_table'] == 'metric_point':
            define('raw_metric', (r.event, (value.metric, value.sample_us)))
    if r.kind in ('country_qualification_source', 'qualified_value_source'):
        field = 'qualification_id' if r.kind == 'country_qualification_source' else 'qualified_value_id'
        define(field, v[field])
        if v['raw_target_ref'] is not None:
            link('raw', v['raw_target_ref'])
    if r.kind == 'coverage_source':
        define('coverage_source_id', v['coverage_id'])
    if r.kind == 'country_coverage':
        define('coverage_id', v['raw']['coverage_id'])
    if r.kind == 'result_availability':
        link('coverage_id', v['coverage_id'])
    if r.kind in ('qualified_asn_point', 'qualified_path'):
        link('raw', v['raw_target_ref'])
    if r.kind == 'qualified_metric' and v['qualified_value_id'] is not None:
        link('qualified_value_id', v['qualified_value_id'])
    if r.kind in ('metric', 'qualified_metric'):
        link('raw_metric', (r.event, r.key))
    if r.kind == 'context_source':
        define('context', v['source_ref'])
    for qid in v.get('qualification_refs', ()):
        link('qualification_id', qid)
    if r.kind == 'evidence_qualification':
        link('qualification_id', v['qualification_id'])
        link('locator', ('evidence', r.event, (r.key[0],)))
    if r.kind in ('qualified_metric', 'qualified_asn_point', 'qualified_path'):
        for qid in r.refs:
            link('qualification_id', qid)
    else:
        for ref in r.refs:
            if not isinstance(ref, str): raise ValueError('trend_s3_reference_type')
            if ref.startswith('metric:'):
                _, metric, sample = ref.split(':')
                link('locator', ('metric', r.event, (metric, int(sample))))
            elif ref.startswith('profile:'):
                link('locator', ('profile', r.event, (ref.split(':', 1)[1],)))
            else:
                link('context', ref)
    if 'source_locator' in v:
        link('locator', v['source_locator'])
    if 'evidence_id' in v:
        link('locator', ('evidence', r.event, (v['evidence_id'],)))
    if r.kind == 'evidence_source':
        link('locator', ('evidence', r.event, (r.key[0],)))
    if r.kind == 'edge':
        source, relation, target = r.key
        kinds = {'supported_by': 'evidence', 'limited_by': 'limitation', 'unknown_about': 'unknown'}
        if relation not in kinds:
            raise ValueError('trend_s3_edge_relation')
        link('locator', ('claim', r.event, (source,)))
        link('locator', (kinds[relation], r.event, (target,)))


def _validate(db, budget):
    budget.check()
    count, first, last = db.execute('SELECT count(*),min(sequence),max(sequence) FROM rows').fetchone()
    if db.execute('SELECT count(DISTINCT result_id) FROM rows INDEXED BY by_result').fetchone()[0] > 1:
        raise ValueError('trend_s3_mixed_result')
    if count and (first, last) != (0, count - 1):
        raise ValueError('trend_s3_sequence_gap')
    missing = db.execute('''SELECT l.role,l.key FROM links l LEFT JOIN definitions d
        ON l.role=d.role AND l.key=d.key WHERE d.key IS NULL LIMIT 1''').fetchone()
    if missing:
        raise ValueError('trend_s3_storage_fk:' + missing[0])
    for role, source_kind, source_payload, target_kind, target_payload in db.execute('''
        SELECT l.role,s.kind,s.payload,t.kind,t.payload FROM links l
        JOIN definitions d ON d.role=l.role AND d.key=l.key
        JOIN rows s ON s.locator=l.source JOIN rows t ON t.locator=d.locator'''):
        budget.check()
        source = schema.restore(source_kind, schema.decode(source_payload))
        target = schema.restore(target_kind, schema.decode(target_payload))
        if source.event != target.event and not (role == 'context' and target.event == ()):
            raise ValueError('trend_s3_cross_event_fk')
        sv, tv = dict(source.values), dict(target.values)
        if role == 'raw_metric':
            original = c3.decode(tv['raw_typed'])[2]
            expected = original.value if source_kind == 'qualified_metric' else original
            if schema.encode(sv['raw']) != schema.encode(expected):
                raise ValueError('trend_s3_raw_metric_fk')
        if role == 'coverage_id':
            raw = tv['raw']
            state = ('admitted_empty' if raw['coverage'] == 'complete' else 'unknown_empty') if raw['event_count'] == 0 else 'observed_events'
            if (raw['dimension'] != 'event_enumeration'
                    or (sv['execution'], sv['event_count'], sv['enumeration_coverage'], sv['state'], sv['completion_receipt_refs'])
                    != (raw['execution'], raw['event_count'], raw['coverage'], state, raw['completion_receipt_refs'])):
                raise ValueError('trend_s3_empty_coverage_fk')
        if role == 'raw' and source_kind in ('qualified_asn_point', 'qualified_path'):
            original = c3.decode(tv['raw_typed'])[2]
            if schema.encode(sv['raw']) != schema.encode(original):
                raise ValueError('trend_s3_raw_value_fk')
        if role == 'qualified_value_id' and source_kind == 'qualified_metric':
            hit = db.execute('''SELECT r.payload FROM definitions d JOIN rows r ON r.locator=d.locator
                WHERE d.role='raw' AND d.key=?''', (schema.encode(tv['raw_target_ref']),)).fetchone()
            original_row = schema.restore('raw_source', schema.decode(hit[0]))
            original = c3.decode(original_row.get('raw_typed'))[2]
            if (tv['raw_target_ref'][0] != 'metric_point'
                    or (original.metric, original.sample_us) != source.key
                    or schema.encode(original.value) != schema.encode(sv['raw'])):
                raise ValueError('trend_s3_raw_metric_fk')
        if role == 'locator' and source_kind == 'evidence':
            if (sv['source_kind'] != target.kind or sv['source_key'] != target.key
                    or schema.encode((sv['values'], sv['source_refs'])) != schema.encode((target.values, target.refs))):
                raise ValueError('trend_s3_evidence_source_value')


def _batches(db, kind, budget):
    batch, size = [], 0
    for payload, in db.execute('SELECT payload FROM rows WHERE kind=? ORDER BY sequence', (kind,)):
        budget.check()
        n = len(payload.encode())
        if batch and (len(batch) == budget.limits.max_batch_rows or size + n > budget.limits.max_batch_bytes):
            yield batch
            batch, size = [], 0
        batch.append(schema.decode(payload)); size += n
    if batch:
        yield batch


def _budget(root, limits, guard):
    if limits.max_batch_rows > 256:
        raise ValueError('trend_s3_batch_limit')
    return StreamBudget(limits, root, guard)


def read_body(directory, *, limits=S2Limits(), guard=lambda: None, budget=None, temporary_parent=None):
    """内部全量回读：落盘排序后按原输出sequence逐行；没有成功Receipt。

    完整存储FK检查后才开始输出。早停仍清理排序文件；最终P1另负责
    输入当前性、实体绑定、完整来源审计与关闭后的回执。
    """
    resources.sqlite_temp()
    if budget is not None and not isinstance(budget, StreamBudget):
        raise ValueError('trend_s3_requires_stream_budget')
    root = Path(directory)
    budget = budget or _budget(root, limits, guard)
    summary = parts.read_summary(root, limits, budget.check)
    with _index(Path(temporary_parent) if temporary_parent is not None else root, budget) as db:
        for kind in schema.TABLES:
            stream = parts.iter_table(root, kind, limits, budget.check, summary=summary)
            try:
                for flat in stream:
                    _append(db, flat['sequence'], schema.restore(kind, flat), budget, phase='readback')
            finally: cleanup((stream.close,), sys.exc_info()[1])
        _validate(db, budget)
        count = 0; h = hashlib.sha256()
        for kind, payload in db.execute('SELECT kind,payload FROM rows ORDER BY sequence'):
            budget.check()
            item = schema.restore(kind, schema.decode(payload))
            h.update(bytes.fromhex(schema.row_hash(item))); count += 1
            yield item
        if (count, h.hexdigest()) != (summary['rows'], summary['sha256']):
            raise ValueError('trend_s3_body_digest')


def write_body(rows, directory, *, limits=S2Limits(), guard=lambda: None, budget=None):
    """写完整有限表（包括零行表），独立回读核对后返回内部计数。

    不创建manifest/ready、PG目录、正式Binding或Admission；失败文件保留。
    """
    resources.sqlite_temp()
    if budget is not None and not isinstance(budget, StreamBudget):
        raise ValueError('trend_s3_requires_stream_budget')
    root = Path(directory)
    root.mkdir(parents=False, exist_ok=False)
    budget = budget or _budget(root, limits, guard)
    iterator = iter(rows)
    hashes = {k: hashlib.sha256() for k in schema.TABLES}
    counts = dict.fromkeys(schema.TABLES, 0)
    expected = hashlib.sha256()
    try:
        with _index(root, budget) as db:
            result_id = None
            for sequence, r in enumerate(iterator):
                if result_id is None: result_id = r.result_id
                if r.result_id != result_id: raise ValueError('trend_s3_mixed_result')
                _append(db, sequence, r, budget)
                digest = bytes.fromhex(schema.row_hash(r))
                hashes[r.kind].update(digest); expected.update(digest); counts[r.kind] += 1
            _validate(db, budget)
            tables = {}
            for kind in schema.TABLES:
                mh = hashlib.sha256(); count = 0
                manifest_path = root/parts.manifest_name(kind)
                resources.track(manifest_path)
                with manifest_path.open('xb') as manifest:
                    batches = iter(_batches(db, kind, budget))
                    try:
                        count = _write_parts(root, kind, batches, manifest, mh, budget)
                    finally: cleanup((batches.close,), sys.exc_info()[1])
                resources.freeze(manifest_path)
                tables[kind] = dict(rows=counts[kind], sha256=hashes[kind].hexdigest(),
                                    parts=count, parts_sha256=mh.hexdigest())
            summary = dict(format=parts.FORMAT, rows=sum(counts.values()),
                           sha256=expected.hexdigest(), tables=tables)
            encoded = parts.encode(summary)
            if len(encoded) > limits.max_context_bytes:
                raise ValueError('trend_s3_parts_summary_item')
            resources.track(root/parts.SUMMARY)
            (root/parts.SUMMARY).write_bytes(encoded)
            resources.freeze(root/parts.SUMMARY)
    finally:
        cleanup((getattr(iterator, 'close', lambda: None),), sys.exc_info()[1])
    actual = hashlib.sha256(); actual_counts = dict.fromkeys(schema.TABLES, 0)
    stream = read_body(root, limits=limits, budget=budget)
    try:
        for r in stream:
            actual.update(bytes.fromhex(schema.row_hash(r))); actual_counts[r.kind] += 1
    finally:
        cleanup((stream.close,), sys.exc_info()[1])
    if actual.digest() != expected.digest() or actual_counts != counts:
        raise ValueError('trend_s3_file_readback')
    budget.finish() if isinstance(budget, StreamBudget) else budget.check()
    return dict(summary, charged_rows=budget.rows, charged_bytes=budget.byte_count,
                processing=dict(budget.stats))


def _write_parts(root, kind, batches, manifest, manifest_hash, budget):
    """当前批写成row-group；只留当前文件的常量计数与摘要，不缓存整片。"""
    writer = None; ordinal = 0
    groups = count = 0; first = last = None; h = hashlib.sha256()
    path = None
    def finish():
        nonlocal writer, ordinal
        closing = writer; writer = None
        cleanup((closing.close,), None)
        resources.freeze(path)
        file = parquet.open_parquet(path, budget.limits, budget.check)
        cleanup((file.close,), None)
        summary = dict(rows=count, first=first, last=last, sha256=h.hexdigest())
        entry = parts.describe(path, kind, ordinal, summary, budget.check)
        line = parts.encode(entry) + b'\n'
        if len(line) > min(budget.limits.max_row_bytes, budget.limits.max_context_bytes):
            raise ValueError('trend_s3_part_manifest_item')
        manifest.write(line); manifest_hash.update(line); ordinal += 1
    try:
        for batch in batches:
            if writer is None:
                path = root/parts.part_name(kind, ordinal)
                resources.track(path)
                writer = pq.ParquetWriter(path, _arrow(kind), compression='zstd', write_statistics=False)
                groups = count = 0; first = last = None; h = hashlib.sha256()
            writer.write_table(pa.Table.from_pylist(batch, schema=_arrow(kind)))
            for flat in batch:
                if first is None: first = flat['sequence']
                last = flat['sequence']; count += 1
                h.update(bytes.fromhex(schema.row_hash(schema.restore(kind, flat))))
            groups += 1
            budget.check()
            if groups >= parquet.MAX_ROW_GROUPS or path.stat().st_size >= parquet.TARGET_FILE_BYTES:
                finish()
        if writer is None and ordinal == 0:
            path = root/parts.part_name(kind, 0)
            resources.track(path)
            writer = pq.ParquetWriter(path, _arrow(kind), compression='zstd', write_statistics=False)
        if writer is not None: finish()
        return ordinal
    finally:
        cleanup((lambda: writer.close() if writer is not None else None,), sys.exc_info()[1])
