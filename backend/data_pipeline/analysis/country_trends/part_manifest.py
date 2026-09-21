"""S3物理分片清单v1：固定表汇总、逐项清单与有序完整读取。"""
from pathlib import Path
import hashlib
import json
import sys

from data_pipeline.analysis.country_events.snapshot_store import cleanup
from data_pipeline.results.manifest_io import file_hash
from data_pipeline.analysis.country_trends import stream_schema as schema
from data_pipeline.analysis.country_trends.parquet_metadata import open_parquet

FORMAT = 'country-trend-parts/v1'
SUMMARY = 'index.json'


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def part_name(kind, ordinal):
    return kind + ('' if ordinal == 0 else '.part.' + str(ordinal).zfill(20)) + '.parquet'


def manifest_name(kind):
    return kind + '.parts.jsonl'


def _plain(path):
    path = Path(path)
    if any(p.is_symlink() for p in (path, *path.parents)) or not path.is_file():
        raise ValueError('trend_s3_missing_or_symlink_table')
    return path


def entity(path):
    path = _plain(path); st = path.stat()
    return dict(path=str(path.resolve()), device=st.st_dev, inode=st.st_ino, size=st.st_size,
                mtime_ns=st.st_mtime_ns, ctime_ns=st.st_ctime_ns)


def describe(path, kind, ordinal, summary, guard):
    before = entity(path)
    checksum = file_hash(path, guard)
    if entity(path) != before:
        raise ValueError('trend_s3_part_changed')
    return dict(version=FORMAT, table=kind, ordinal=ordinal, path=part_name(kind, ordinal),
                rows=summary['rows'], first=summary['first'], last=summary['last'],
                sha256=summary['sha256'], entity=dict(before, sha256=checksum))


def read_summary(directory, limits, guard, expected=None):
    guard(); path = _plain(Path(directory)/SUMMARY)
    # 只有固定54表的小汇总；变长分片条目从不装入它。
    with path.open('rb') as stream:
        raw = stream.read(limits.max_context_bytes + 1)
    if len(raw) > limits.max_context_bytes:
        raise ValueError('trend_s3_parts_summary_item')
    value = json.loads(raw)
    if (set(value) != {'format','rows','sha256','tables'} or value['format'] != FORMAT
            or set(value['tables']) != set(schema.TABLES)):
        raise ValueError('trend_s3_parts_summary')
    if expected is not None and value != {k:expected[k] for k in value}:
        raise ValueError('trend_s3_parts_binding')
    return value


def iter_parts(directory, kind, limits, guard, *, summary=None, hash_body=False):
    """清单逐行有界读取；只有完整耗尽才核尾计数与摘要。"""
    root = Path(directory)
    summary = summary or read_summary(root, limits, guard)
    wanted = summary['tables'][kind]
    h = hashlib.sha256(); count = rows = 0; previous = -1
    maximum = min(limits.max_row_bytes, limits.max_context_bytes)
    with _plain(root/manifest_name(kind)).open('rb') as stream:
        while True:
            guard(); line = stream.readline(maximum + 1)
            if not line: break
            if len(line) > maximum or not line.endswith(b'\n'):
                raise ValueError('trend_s3_part_manifest_item')
            entry = json.loads(line)
            if (set(entry) != {'version','table','ordinal','path','rows','first','last','sha256','entity'}
                    or entry['version'] != FORMAT or entry['table'] != kind
                    or type(entry['ordinal']) is not int or entry['ordinal'] != count
                    or entry['path'] != part_name(kind, count)
                    or type(entry['rows']) is not int or entry['rows'] < 0):
                raise ValueError('trend_s3_part_order')
            if entry['rows']:
                if (type(entry['first']) is not int or type(entry['last']) is not int
                        or entry['first'] <= previous or entry['last'] < entry['first']):
                    raise ValueError('trend_s3_part_sequence')
                previous = entry['last']
            elif entry['first'] is not None or entry['last'] is not None or count or wanted['rows']:
                raise ValueError('trend_s3_empty_part')
            path = root/entry['path']; saved = entry['entity']
            if entity(path) != {k:v for k,v in saved.items() if k != 'sha256'}:
                raise ValueError('trend_s3_part_entity_changed')
            if hash_body and file_hash(path, guard) != saved['sha256']:
                raise ValueError('trend_s3_part_hash')
            h.update(line); count += 1; rows += entry['rows']
            yield entry
    if (count, rows, h.hexdigest()) != (wanted['parts'], wanted['rows'], wanted['parts_sha256']):
        raise ValueError('trend_s3_part_manifest_tail')


def iter_table(directory, kind, limits, guard, *, summary=None):
    """逐片单批解码；物理顺序和逻辑表摘要均在耗尽后核验。"""
    from data_pipeline.analysis.country_trends.stream_files import _arrow
    root = Path(directory); summary = summary or read_summary(root, limits, guard)
    total = hashlib.sha256(); rows = 0; previous = -1
    parts = iter_parts(root, kind, limits, guard, summary=summary)
    try:
        for entry in parts:
            file = open_parquet(root/entry['path'], limits, guard)
            count = 0; first = last = None; h = hashlib.sha256()
            try:
                if file.schema_arrow != _arrow(kind): raise ValueError('trend_s3_file_columns')
                if any(file.metadata.row_group(i).total_byte_size > limits.max_batch_bytes
                       for i in range(file.metadata.num_row_groups)):
                    raise ValueError('trend_s3_row_group_bytes')
                for batch in file.iter_batches(batch_size=limits.max_batch_rows):
                    for flat in batch.to_pylist():
                        seq = flat['sequence']
                        if type(seq) is not int or seq <= previous:
                            raise ValueError('trend_s3_part_sequence')
                        previous = seq
                        if first is None: first = seq
                        last = seq
                        value = bytes.fromhex(schema.row_hash(schema.restore(kind, flat)))
                        h.update(value); total.update(value); count += 1; rows += 1
                        yield flat
            finally: cleanup((file.close,), sys.exc_info()[1])
            if (count, first, last, h.hexdigest()) != (entry['rows'],entry['first'],entry['last'],entry['sha256']):
                raise ValueError('trend_s3_part_content')
    finally: cleanup((parts.close,), sys.exc_info()[1])
    if (rows, total.hexdigest()) != (summary['tables'][kind]['rows'],summary['tables'][kind]['sha256']):
        raise ValueError('trend_s3_table_digest')


def current_parts(runtime, owner_binding, guard):
    """轻量current只扫清单与stat，不扫描Parquet科学正文。"""
    root = runtime.output_root/'data'
    summary = read_summary(root, runtime.limits, guard, owner_binding['proof']['body'])
    for kind in schema.TABLES:
        for entry in iter_parts(root, kind, runtime.limits, guard, summary=summary):
            runtime.path(root/entry['path'])  # 实体stat已由iter_parts核对；这里只复用访问范围门禁。
