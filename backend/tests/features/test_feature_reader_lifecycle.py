"""对显式绑定的自有人工制品检查连接生命周期；不生产或重算制品。"""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import uuid

import psycopg2
from psycopg2.extensions import make_dsn
import pytest

from data_pipeline.analysis.features import qualified_read as reader, store as feature_store
from data_pipeline.analysis.features.qualification import PROFILE


@pytest.fixture
def original():
    path = os.environ.get('DOMEYE_FEATURE_READER_CONTEXT')
    if not path:
        pytest.skip('需要明确绑定自有人工制品的修复前摘要')
    return json.loads(Path(path).read_text())


@pytest.fixture
def probe(original):
    app = 'feature_reader_lifecycle_' + uuid.uuid4().hex
    dsn = make_dsn(original['dsn'], application_name=app)

    def sessions():
        # 独立观察连接，每次重新读取统计，不释放被测异常或触发GC。
        with closing(psycopg2.connect(original['dsn'])) as pg, pg, pg.cursor() as c:
            c.execute('SELECT pid,state FROM pg_stat_activity WHERE application_name=%s', (app,))
            return c.fetchall()

    return dsn, sessions


@pytest.mark.parametrize('damage', ['digest', 'missing_anchor'])
def test_committed_anchor_rejection_closes_with_traceback_retained(original, probe, damage):
    dsn, sessions = probe
    binding = original['binding']
    run = binding['run_id']
    with closing(psycopg2.connect(original['dsn'])) as pg, pg, pg.cursor() as c:
        c.execute('SELECT snapshot,receipt FROM feature.qualified_results WHERE run_id=%s', (run,))
        snapshot, receipt = c.fetchone()
        if damage == 'digest':
            c.execute('UPDATE feature.qualified_results SET receipt=%s WHERE run_id=%s',
                      (json.dumps({**receipt, 'specification_digest': 'invalid'}), run))
        else:
            c.execute('DELETE FROM feature.qualified_results WHERE run_id=%s', (run,))
    held = []
    try:
        try:
            reader.inspect_binding(dsn, run, binding['snapshot'])
        except ValueError as exc:
            held.append(exc)
        assert len(held) == 1
        assert '完成锚' in str(held[0])
        assert held[0].__traceback__ is not None
        assert sessions() == []
    finally:
        with closing(psycopg2.connect(original['dsn'])) as pg, pg, pg.cursor() as c:
            if damage == 'missing_anchor':
                c.execute('INSERT INTO feature.qualified_results(run_id,snapshot,receipt) VALUES (%s,%s,%s)',
                          (run, snapshot, json.dumps(receipt)))
            else:
                c.execute('UPDATE feature.qualified_results SET receipt=%s WHERE run_id=%s',
                          (json.dumps(receipt), run))


def test_missing_run_rejection_closes_with_traceback_retained(original, probe):
    dsn, sessions = probe
    with pytest.raises(ValueError, match='固定结果不存在') as held:
        reader.inspect_binding(dsn, uuid.uuid4().hex, original['binding']['snapshot'])
    assert held.value.__traceback__ is not None
    assert sessions() == []


def digest_rows(rows):
    return hashlib.sha256(json.dumps(sorted(json.dumps(r, sort_keys=True) for r in rows)).encode()).hexdigest()


def test_original_artifact_binding_and_all_outputs_unchanged(original, probe):
    dsn, sessions = probe
    binding = original['binding']
    assert reader.inspect_binding(dsn, binding['run_id'], binding['snapshot']) == binding
    for table, digest in original['raw_digests'].items():
        rows = (row for batch in feature_store.read_table(
            dsn, binding['run_id'], binding['snapshot'], table, profile=PROFILE) for row in batch.to_pylist())
        assert digest_rows(rows) == digest, table
    assert digest_rows(reader.read_windows(dsn, binding)) == original['public_digest']
    assert digest_rows(reader.read_coverage(dsn, binding)) == original['coverage_digest']
    assert sessions() == []
    for path, digest in original['files'].items():
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest, path


@pytest.mark.parametrize('stop', ['early_close', 'guard'])
def test_duckdb_and_pg_close_on_interruption(original, probe, monkeypatch, stop):
    dsn, sessions = probe
    opened = []
    connect = reader.connect_duckdb

    class TrackedDatabase:
        def __init__(self):
            self.db = connect()
            self.closed = False
            opened.append(self)

        def __getattr__(self, name):
            return getattr(self.db, name)

        def execute(self, *args, **kwargs):
            self.db.execute(*args, **kwargs)
            return self

        def close(self):
            self.db.close()
            self.closed = True

    monkeypatch.setattr(reader, 'connect_duckdb', TrackedDatabase)
    monkeypatch.setattr(feature_store, 'connect_duckdb', TrackedDatabase)
    interrupted = False

    def guard():
        if interrupted:
            raise RuntimeError('人工中断')

    stream = reader.read_windows(dsn, original['binding'], guard=guard)
    next(stream)
    assert any(not db.closed for db in opened)
    if stop == 'early_close':
        stream.close()
    else:
        interrupted = True
        with pytest.raises(RuntimeError, match='人工中断') as held:
            list(stream)
        assert held.value.__traceback__ is not None
    assert opened and all(db.closed for db in opened)
    assert sessions() == []
