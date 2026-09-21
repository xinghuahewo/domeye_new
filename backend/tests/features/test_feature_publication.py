"""仅显式自有人工原制品：不重新生产M2或Feature，不使用他人PG。"""
from contextlib import contextmanager, closing
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
import uuid

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_pipeline.analysis.features import publication as p, publication_io as io
from data_pipeline.bgp.archive import admission as m
from data_pipeline.bgp.archive.value_codec import typed, untyped, digest
from data_pipeline.analysis.features.qualified_read import read_windows, read_coverage


@pytest.fixture(scope='module')
def context():
    path = os.environ.get('DOMEYE_FEATURE_READER_CONTEXT')
    if not path: pytest.skip('须显式绑定自有人工原制品')
    c = json.loads(Path(path).read_text()); root = Path(path).resolve().parent.parent
    deps = json.loads((root / 'p1/dependencies.json').read_text())
    u = m.Runtime(c['dsn'], (root,), root / 'p1', fixture_only=True,
                  dependency_admissions=tuple(a for a in deps if a['owner'] == 'm2'))
    events = []
    runtime = p.Runtime(c['dsn'], Path(c['root']) / 'output', (root,), root / 'p1', tuple(deps),
                        {a['admission_id']: u for a in deps}, fixture_only=True, audit_sink=events.append)
    a = p.admit(runtime, c['binding'], guard=lambda: None)
    return c, runtime, a, events


def request(view='windows', role='all', batch=1):
    return dict(view=view, scope_typed=typed(dict(mode='all', window_role=role)), codec_version=io.CODEC,
                batch_rows=batch, batch_bytes=128*1024)


def collect(runtime, admission, req):
    start = time.monotonic(); output = []; first = None
    with p.open_reader(runtime, admission, req, guard=lambda: None) as session:
        for batch in session:
            if first is None: first = time.monotonic() - start
            assert batch['bytes'] == len(batch['rows_typed'].encode())
            decoded = untyped(batch['rows_typed']); assert batch['rows'] == len(decoded)
            output.extend(decoded)
        assert session.receipt is None
    assert session.receipt and session.receipt['rows'] == len(output)
    return output, session.receipt, dict(first_seconds=first, total_seconds=time.monotonic()-start)


def multiset(rows): return sorted(typed(row) for row in rows)


def test_admit_reuse_current_and_full_values(context):
    c, runtime, a, events = context
    events.clear()
    assert p.admit(runtime, c['binding'], guard=lambda: None) == a
    p.verify_current(runtime, a, guard=lambda: None)
    assert not any(e['kind'] in ('feature_full_audit', 'entity_hash', 'parquet_row_group', 'decoded_batch') for e in events)
    result = {}
    for view, old_reader in [('windows', read_windows), ('coverage', read_coverage)]:
        original = list(old_reader(c['dsn'], c['binding']))
        rows1, receipt1, cost1 = collect(runtime, a, request(view, batch=1))
        rows2, receipt2, cost2 = collect(runtime, a, request(view, batch=17))
        assert rows1 == rows2
        assert multiset(rows1) == multiset(original)
        assert receipt1['typed_digest'] == receipt2['typed_digest']
        result[view] = dict(rows=len(rows1), first=cost1, second=cost2, typed_digest=receipt1['typed_digest'])
    assert not any(e['kind'] == 'feature_full_audit' for e in events)
    # initial没有科学窗口，但六维coverage存在。
    windows, wr, _ = collect(runtime, a, request('windows', 'initial'))
    coverage, cr, _ = collect(runtime, a, request('coverage', 'initial'))
    assert windows == [] and len(coverage) == 12 and wr['rows'] == 0 and cr['rows'] == 12
    (runtime.scratch_root / 'reader-cost.json').write_text(json.dumps(result, ensure_ascii=False, indent=2))
    (runtime.scratch_root / 'reader-events.json').write_text(json.dumps(events, ensure_ascii=False, indent=2))
    for path, expected in c['files'].items(): assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == expected


def test_self_signed_missing_key_rejected(context):
    _, runtime, a, _ = context
    fake = deepcopy(a)
    next(t for t in fake['lock_targets'] if t['namespace'] == 'feature.admission')['key'] = str(uuid.uuid4())
    fake['admission_id'] = digest({k: v for k, v in fake.items() if k != 'admission_id'})
    with pytest.raises(ValueError, match='可信登记'): p.verify_current(runtime, fake, guard=lambda: None)


@contextmanager
def mutate(runtime, query, args, restore_query, restore_args):
    with closing(psycopg2.connect(runtime.dsn)) as pg, pg, pg.cursor() as cursor: cursor.execute(query, args)
    try: yield
    finally:
        with closing(psycopg2.connect(runtime.dsn)) as pg, pg, pg.cursor() as cursor: cursor.execute(restore_query, restore_args)


@pytest.mark.parametrize('namespace', p.NAMESPACES)
def test_single_target_real_lock(context, namespace):
    _, runtime, a, events = context
    target = next(t for t in a['lock_targets'] if t['namespace'] == namespace)
    table, key = {'feature.run': ('feature.runs', 'run_id'),
                  'feature.qualification': ('feature.qualified_results', 'run_id'),
                  'feature.admission': (p.REGISTRY, 'record_key')}[namespace]
    sql = f'UPDATE {table} SET {key}={key} WHERE {key}=%s'
    with p.hold_lock(runtime, a, target, guard=lambda: None):
        with closing(psycopg2.connect(runtime.dsn)) as pg, pg, pg.cursor() as c:
            c.execute("SET LOCAL lock_timeout='100ms'")
            with pytest.raises(psycopg2.errors.LockNotAvailable): c.execute(sql, (target['key'],))
        events.clear()
    assert events == []  # 退出只释放，无尾audit或其他SQL。
    with closing(psycopg2.connect(runtime.dsn)) as pg, pg, pg.cursor() as c: c.execute(sql, (target['key'],))


@pytest.mark.parametrize('stop', ['early', 'guard', 'close'])
def test_no_receipt_on_incomplete_or_cleanup(context, monkeypatch, stop):
    _, runtime, a, _ = context
    if stop == 'close':
        connect = io.connect_duckdb
        class Database:
            def __init__(self, *args): self.db = connect(*args)
            def __getattr__(self, name): return getattr(self.db, name)
            def close(self): self.db.close(); raise OSError('真实DuckDB已关闭后的注入错误')
        monkeypatch.setattr(io, 'connect_duckdb', Database)
    interrupted = False
    def guard():
        if interrupted: raise RuntimeError('人工guard')
    if stop == 'early':
        with p.open_reader(runtime, a, request(), guard=guard) as session: next(session)
    else:
        with pytest.raises((RuntimeError, OSError)) as held:
            with p.open_reader(runtime, a, request(), guard=guard) as session:
                next(session)
                if stop == 'guard': interrupted = True
                list(session)
        assert held.value.__traceback__ is not None
    assert session.receipt is None


def test_actual_upstream_revocation_during_read(context):
    _, runtime, a, _ = context
    dep = next(x for x in runtime.dependency_admissions if x['owner'] == 'reference')
    key = next(t['key'] for t in dep['lock_targets'] if t['namespace'] == 'reference.admission')
    with p.open_reader(runtime, a, request(), guard=lambda: None) as session:
        next(session)
        # 保持撤销跨过尾current；context退出产生真实拒绝，由外层捕获。
    assert session.receipt is None
    with mutate(runtime, "UPDATE observation_publication.reference_admissions SET state='revoked' WHERE record_key=%s", (key,),
                "UPDATE observation_publication.reference_admissions SET state='accepted' WHERE record_key=%s", (key,)):
        with pytest.raises(ValueError, match='撤销'):
            p.verify_current(runtime, a, guard=lambda: None)
    with pytest.raises(ValueError, match='撤销'):
        with mutate(runtime, "UPDATE observation_publication.reference_admissions SET state='accepted' WHERE record_key=%s", (key,),
                    "UPDATE observation_publication.reference_admissions SET state='accepted' WHERE record_key=%s", (key,)):
            with p.open_reader(runtime, a, request(), guard=lambda: None) as session:
                next(session)
                with closing(psycopg2.connect(runtime.dsn)) as pg, pg, pg.cursor() as c:
                    c.execute("UPDATE observation_publication.reference_admissions SET state='revoked' WHERE record_key=%s", (key,))
                list(session)
    assert session.receipt is None
    p.verify_current(runtime, a, guard=lambda: None)


@pytest.mark.parametrize('damage', ['windows', 'qualifications', 'reference_rows', 'null_key', 'duplicate'])
def test_real_bad_body_cannot_admit(context, tmp_path, damage):
    c, runtime, a, _ = context
    # 改私有副本，原Parquet/ready不写。仅临时将自有catalog根指向副本，随后恢复。
    clone = runtime.scratch_root / ('bad-' + uuid.uuid4().hex)
    shutil.copytree(runtime.output_root, clone)
    try:
        target_table = 'windows' if damage in ('null_key', 'duplicate') else damage
        target = next((clone / 'parquet').rglob(target_table + '/*.parquet'))
        table = pq.read_table(target); values = table.to_pylist()
        if damage == 'windows': values[0]['announ_num'] += 1
        elif damage == 'qualifications': values[0]['qualification_id'] = 'self-signed'
        elif damage == 'reference_rows': values[0]['rule'] = 'invented-reference-rule'
        elif damage == 'null_key': values[0]['source_id'] = None
        else:
            donor = next(x for x in (clone/'parquet').rglob('windows/*.parquet') if x != target)
            values[0] = pq.read_table(donor).to_pylist()[0]
        pq.write_table(pa.Table.from_pylist(values, schema=table.schema), target)
        bad = p.Runtime(runtime.dsn, clone, runtime.allowed_roots, runtime.scratch_root,
                        runtime.dependency_admissions, runtime.dependency_runtimes, fixture_only=True)
        catalog = 'fl_' + c['binding']['run_id']
        sql = f"UPDATE {catalog}.ducklake_metadata SET value=%s WHERE key='data_path'"
        with closing(psycopg2.connect(runtime.dsn)) as pg, pg, pg.cursor() as cursor:
            cursor.execute(f'SELECT count(*) FROM {p.REGISTRY}'); before = cursor.fetchone()[0]
        with mutate(runtime, sql, (str(clone/'parquet')+'/',), sql, (str(runtime.output_root/'parquet')+'/',)):
            with pytest.raises(ValueError, match='完整科学/资格关系不符'):
                p.admit(bad, c['binding'], guard=lambda: None)
        with closing(psycopg2.connect(runtime.dsn)) as pg, pg, pg.cursor() as cursor:
            cursor.execute(f'SELECT count(*) FROM {p.REGISTRY}'); assert cursor.fetchone()[0] == before
    finally: shutil.rmtree(clone)
    p.verify_current(runtime, a, guard=lambda: None)


def test_same_run_other_database_oid_rejected(context):
    c, runtime, a, _ = context
    from psycopg2.extensions import make_dsn, parse_dsn
    name = 'feature_p1_wrong_oid_' + uuid.uuid4().hex
    control = make_dsn(runtime.dsn, dbname='postgres')
    with closing(psycopg2.connect(control)) as pg:
        pg.autocommit = True
        with pg.cursor() as cursor:
            cursor.execute('CREATE DATABASE ' + name + ' TEMPLATE ' + parse_dsn(runtime.dsn)['dbname'])
    try:
        clone = p.Runtime(make_dsn(runtime.dsn, dbname=name), runtime.output_root, runtime.allowed_roots,
                          runtime.scratch_root, runtime.dependency_admissions, runtime.dependency_runtimes, fixture_only=True)
        with pytest.raises(ValueError, match='数据库身份'):
            p.verify_current(clone, a, guard=lambda: None)
    finally:
        with closing(psycopg2.connect(control)) as pg:
            pg.autocommit = True
            with pg.cursor() as cursor: cursor.execute('DROP DATABASE ' + name)


def test_inner_row_stream_close_error_prevents_receipt(context, monkeypatch):
    _, runtime, a, _ = context
    original = io.rows
    closed = []
    class Rows:
        def __init__(self, source): self.source = source
        def __iter__(self): return self
        def __next__(self): return next(self.source)
        def close(self):
            self.source.close(); closed.append(True)
            raise OSError('真实行流关闭后失败')
    def tracked(*args):
        source = original(*args)
        return Rows(source) if 'FROM actual_windows' in args[2] else source
    monkeypatch.setattr(io, 'rows', tracked)
    with pytest.raises(OSError, match='行流关闭'):
        with p.open_reader(runtime, a, request(), guard=lambda: None) as session: list(session)
    assert closed and session.receipt is None
