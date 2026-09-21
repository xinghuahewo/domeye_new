"""公共原typed流：资源、资格、完整行限制及正式人工链对照。"""
import json
import pytest
import psycopg2
from psycopg2.extras import RealDictCursor
from data_pipeline.analysis.detection import store


def check_formal_stream(dsn, run_id, snapshot, monkeypatch):
    """由实际MRT+11参考的正式冻结链调用，复用一次生产。"""
    stored = {}
    for table, order in (("records", "sequence"), ("state_entries", "ordinal")):
        rows = list(store.read_stored_rows(dsn, run_id, snapshot, table, batch_rows=3))
        stored[table] = rows
        db = store.connect_duckdb()
        try:
            db.execute("LOAD ducklake")
            db.execute("LOAD postgres")
            db.execute("ATTACH " + store.literal("ducklake:postgres:" + dsn) + " AS lake")
            result = db.execute(f"SELECT * FROM lake.det_{run_id}.{table} AT (VERSION => {snapshot}) ORDER BY {order}")
            names = [c[0] for c in result.description]
            direct = [dict(zip(names, row)) for row in result.fetchall()]
            assert rows == direct  # 包括JSON文本、全部列、原顺序和重复值。
            if table == "records":
                db.execute(f"CREATE TABLE lake.det_{run_id}.later_catalog_version (value INTEGER)")
                assert db.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0] > snapshot
        finally:
            db.close()
        assert list(store.read_stored_rows(dsn, run_id, snapshot, table)) == rows
        pg = psycopg2.connect(dsn)
        try:
            with pg.cursor(cursor_factory=RealDictCursor) as c:
                c.execute(f"SELECT * FROM detection.{table} WHERE run_id=%s ORDER BY {order}", (run_id,))
                pgrows = c.fetchall()
            for row in pgrows:
                del row["run_id"]
            expected = [{k: json.loads(v) if k.endswith('_json') else v for k, v in row.items()} for row in rows]
            assert pgrows == expected  # PG JSONB比较语义；原文本以湖列为准。
        finally:
            pg.close()
    rows = stored["records"]
    assert list(store.read_records(dsn, run_id, snapshot)) == [
        {**json.loads(r['attributes_json']), 'legacy': json.loads(r['legacy_json']), 'evidence': json.loads(r['evidence_json'])} for r in rows]
    assert [r['sequence'] for r in store.read_revisions(dsn, run_id, snapshot)] == [r['sequence'] for r in rows if r['record_kind'] == 'business_revision']
    assert [r['sequence'] for r in store.read_decisions(dsn, run_id, snapshot)] == [r['sequence'] for r in rows if r['record_kind'] == 'rule_decision']
    # 整份重建仍由原消费者验证；同时确认它实际消费的流就是公开全字段流。
    original = store.reconstruct_state(dsn, run_id, snapshot)
    with monkeypatch.context() as m:
        m.setattr(store, '_read_table', lambda *a, **kw: iter(stored['state_entries']))
        assert store.reconstruct_state(dsn, run_id, snapshot) == original
    for table in ('records', 'state_entries'):
        with pytest.raises(ValueError, match='字节限额'):
            list(store.read_stored_rows(dsn, run_id, snapshot, table, max_row_bytes=1))
    with pytest.raises(ValueError, match='不可消费'):
        list(store.read_stored_rows(dsn, run_id, snapshot + 999, 'records'))
    pg = psycopg2.connect(dsn)
    pg.autocommit = True
    try:
        with pg.cursor() as c:
            c.execute('SELECT state,schema_name,snapshot,identity,scope FROM detection.runs WHERE run_id=%s', (run_id,))
            saved = c.fetchone()
        for column, value in [('state','candidate'), ('state','failed'), ('schema_name','wrong'), ('snapshot',snapshot+1), ('identity', '{}'), ('scope','{}')]:
            stream = store.read_stored_rows(dsn, run_id, snapshot, 'records')
            next(stream)
            try:
                with pg.cursor() as c:
                    c.execute(f'UPDATE detection.runs SET {column}=%s WHERE run_id=%s', (value,run_id))
                with pytest.raises(ValueError):
                    list(stream)
                if column != 'scope':
                    with pytest.raises(ValueError):
                        list(store.read_stored_rows(dsn, run_id, snapshot, 'records'))
            finally:
                stream.close()
                with pg.cursor() as c:
                    c.execute('UPDATE detection.runs SET state=%s,schema_name=%s,snapshot=%s,identity=%s,scope=%s WHERE run_id=%s', (*saved[:3],json.dumps(saved[3]),json.dumps(saved[4]),run_id))
    finally:
        pg.close()
    return {table: len(rows) for table, rows in stored.items()}


@pytest.mark.parametrize('table', ['other', 'records;DROP TABLE x', '', None])
def test_table_rejected_without_connection(table):
    with pytest.raises(ValueError, match='无效'):
        list(store.read_stored_rows('unused', 'abc', 1, table))


def test_guard_and_early_close_release_resources(monkeypatch):
    import pyarrow as pa
    connections, databases = [], []
    class PG:
        def __init__(self): self.closed = False; connections.append(self)
        def set_session(self, **kwargs): pass
        def cursor(self): return self
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args): pass
        def fetchone(self): return ('complete','det_abc',1,{'execution_mode':'frozen-fresh-process'}, {})
        def close(self): self.closed = True
    class DB:
        def __init__(self): self.closed = False; databases.append(self)
        def execute(self, *args): return self
        def fetch_record_batch(self, n):
            yield pa.RecordBatch.from_pylist([{'sequence':1,'legacy_json':'重复'}, {'sequence':2,'legacy_json':'重复'}])
        def close(self): self.closed = True
    monkeypatch.setattr(store.psycopg2,'connect',lambda dsn: PG())
    monkeypatch.setattr(store,'connect_duckdb',DB)
    stream = store.read_stored_rows('unused','abc',1,'records')
    assert next(stream)['sequence'] == 1
    stream.close()
    assert all(c.closed for c in connections + databases)
    assert len(connections) == 1  # 早停没有伪造末尾资格复验。
    calls = 0
    def guard():
        nonlocal calls
        calls += 1
        if calls == 5: raise RuntimeError('资源保护')
    with pytest.raises(RuntimeError, match='资源保护'):
        list(store.read_stored_rows('unused','abc',1,'records',guard=guard))
    assert all(c.closed for c in connections + databases)
    assert [r['sequence'] for r in store.read_stored_rows('unused','abc',1,'records')] == [1,2]
    large = '中' * (3 * 1024**2)
    def large_batch(self, n):
        yield pa.RecordBatch.from_pylist([{'attributes_json': '{}', 'legacy_json': json.dumps(large, ensure_ascii=False), 'evidence_json': '{}'}])
    monkeypatch.setattr(DB, 'fetch_record_batch', large_batch)
    with pytest.raises(ValueError, match='字节限额'):
        list(store.read_stored_rows('unused','abc',1,'records'))
    assert list(store.read_records('unused','abc',1))[0]['legacy'] == large
    assert len(list(store.read_stored_rows('unused','abc',1,'records',max_row_bytes=12*1024**2))) == 1


def test_old_reader_does_not_inherit_new_row_budget(monkeypatch):
    """既有调用不施加新上限，公开typed入口仍默认8MiB。"""
    budgets = []
    def stream(*args, max_row_bytes=None, **kwargs):
        budgets.append(max_row_bytes)
        yield {'attributes_json': '{}', 'legacy_json': '{}', 'evidence_json': '{}'}
    monkeypatch.setattr(store, '_stream_stored_rows', stream)
    assert list(store.read_records('unused', 'abc', 1)) == [{'legacy': {}, 'evidence': {}}]
    list(store.read_stored_rows('unused', 'abc', 1, 'records'))
    assert budgets == [None, 8 * 1024**2]
