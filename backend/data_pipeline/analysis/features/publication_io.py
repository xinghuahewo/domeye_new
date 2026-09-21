"""Feature P1 私有实体/临时读取工具；不建立通用目录或连接权限。"""
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import resource
import sys
import tempfile

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

from data_pipeline.bgp.archive.store import connect_duckdb, literal, TYPES
from data_pipeline.bgp.archive.value_codec import typed, digest
from data_pipeline.bgp.archive.validation import _closing, CountedFile
from data_pipeline.analysis.features.store import TABLES as SCIENCE
from data_pipeline.analysis.features.qualification import TABLES as QUALIFIED

TABLES = {**SCIENCE, **QUALIFIED}
CODEC = 'm2-checkpoint-typed/v1'


def query(runtime, db, sql, args=None):
    runtime.resource_guard()
    runtime.event('feature_sql', sql=sql)
    runtime.resource_guard()
    db.execute(sql, args) if args is not None else db.execute(sql)
    runtime.resource_guard()
    return db


@contextmanager
def pg(runtime, *, write=False):
    runtime.resource_guard()
    with _closing([]) as cleanup:
        connection = psycopg2.connect(runtime.dsn)
        cleanup.extend([connection.close, connection.rollback])
        connection.set_session(readonly=not write)
        yield connection
        if write:
            connection.commit()


def entity(runtime, path, guard, *, full=False):
    guard()
    p = runtime.path(path)
    stat = p.stat()
    if not p.is_file():
        raise ValueError('Feature实体必须为普通文件')
    result = dict(path=str(p), device=stat.st_dev, inode=stat.st_ino, size=stat.st_size,
                  mtime_ns=stat.st_mtime_ns, ctime_ns=stat.st_ctime_ns)
    if full:
        h = hashlib.sha256()
        with p.open('rb') as file:
            for chunk in iter(lambda: file.read(1024**2), b''):
                guard(); h.update(chunk); runtime.event('entity_hash', bytes=len(chunk))
            os.fsync(file.fileno())
        if result != entity(runtime, p, guard):
            raise ValueError('Feature实体hash期间漂移')
        result['sha256'] = h.hexdigest()
    return result


def entities_current(runtime, entities, guard):
    for e in entities:
        if entity(runtime, e['path'], guard) != {k: v for k, v in e.items() if k != 'sha256'}:
            raise ValueError('Feature固定实体漂移')


def budget(runtime, root, guard):
    guard()
    runtime.validate_budgets()
    size = sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)
    runtime.event('resource_sample', temporary_bytes=size, rss_high_water_bytes=rss)
    runtime.validate_budgets()
    if size > runtime.max_temp_bytes or rss > runtime.max_rss_bytes:
        raise ValueError('Feature审计临时盘/RSS保护')


@contextmanager
def stage(runtime, guard):
    with _closing([]) as cleanup:
        runtime.resource_guard()
        temporary = tempfile.TemporaryDirectory(prefix='feature-p1-', dir=runtime.scratch_root)
        cleanup.append(temporary.cleanup)
        runtime.resource_guard()
        root = Path(temporary.name)
        if not runtime.fixture_only:
            runtime.path(root)
        db = connect_duckdb(str(root / 'audit.duckdb'))
        cleanup.append(db.close)
        query(runtime, db, 'SET memory_limit=' + literal(runtime.memory_limit))
        query(runtime, db, 'SET temp_directory=' + literal(root / 'spill'))
        yield db, root
        budget(runtime, root, guard)


def schema(table):
    return pa.schema([(n, {**TYPES, 'BIGINT[]': pa.list_(pa.int64())}[t]) for n, t in TABLES[table]])


def create(runtime, db, name, table):
    query(runtime, db, f'CREATE TABLE {name} (' + ','.join('"' + n + '" ' + t for n, t in TABLES[table]) + ')')


def insert(runtime, db, name, table, rows):
    db.register('incoming', pa.Table.from_pylist(rows, schema=schema(table)))
    try:
        query(runtime, db, f'INSERT INTO {name} SELECT * FROM incoming')
    finally:
        db.unregister('incoming')


def load(runtime, db, root, table, files, guard):
    create(runtime, db, 'actual_' + table, table)
    for item in files:
        if item['table'] != table:
            continue
        with _closing([]) as cleanup:
            file = CountedFile(runtime.path(item['path']), runtime); cleanup.append(file.close)
            parquet = pq.ParquetFile(file); cleanup.append(parquet.close)
            if not parquet.schema_arrow.equals(schema(table), check_metadata=False):
                raise ValueError('Feature Parquet完整typed schema不符：' + table)
            if parquet.metadata.num_rows != item['rows']:
                raise ValueError('Feature目录与实际文件行数不符')
            for group in range(parquet.num_row_groups):
                runtime.event('parquet_row_group', table=table, path=item['path'], group=group,
                              rows=parquet.metadata.row_group(group).num_rows)
                for batch in parquet.iter_batches(batch_size=512, row_groups=[group]):
                    budget(runtime, root, guard)
                    runtime.event('decoded_batch', rows=batch.num_rows, bytes=batch.nbytes)
                    insert(runtime, db, 'actual_' + table, table, batch.to_pylist())


def rows(runtime, db, sql, guard):
    with _closing([]) as cleanup:
        reader = query(runtime, db, sql).fetch_record_batch(512); cleanup.append(reader.close)
        for batch in reader:
            guard()
            yield from batch.to_pylist()


def row_digest(values):
    h = hashlib.sha256(); count = 0
    for value in values:
        h.update(bytes.fromhex(digest(typed(value)))); count += 1
    return count, h.hexdigest()
