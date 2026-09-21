"""H1 owner生命周期定向复验：只用自己的固定Collection重新投影，不重导。"""
from contextlib import closing
import json
import os
from pathlib import Path
import traceback
import uuid

import duckdb
import pytest

from tests.historical.test_historical_core import History, PGIdentity, decode_token, save, sha
from data_pipeline.history.event_index.model import TABLES


@pytest.fixture(scope='module')
def lake():
    if not os.environ.get('Q3_PRIVATE_ROOT'):
        pytest.skip('须显式本任务私有PG')
    root = Path(os.environ['Q3_PRIVATE_ROOT'])
    original = root / 'q3-c2/acceptance-513d7b18ff7542cda5dc7087a2ef9841'
    binding = json.loads((original / 'binding.json').read_text())
    assert Path(binding['root']) == root
    h = History(binding['dsn'], root, target_identity=PGIdentity(**binding['identity']))
    old = decode_token(binding['core'])
    ready = root / 'history' / old.profile_id / 'ready.json'
    before = sha(ready.read_bytes())
    with pytest.raises(ValueError, match='规则/来源绑定变化'):
        with h.core(old):
            pass
    out = root / 'q3-c2-cleanup' / ('acceptance-' + uuid.uuid4().hex)
    out.mkdir(parents=True)
    new = h.project_core(old.collection, root_id=old.root_id)
    assert before == sha(ready.read_bytes())
    from dataclasses import asdict
    save(out / 'binding.json', {'dsn': h.dsn, 'root': str(root), 'identity': asdict(h.target_identity), 'token': asdict(new),
                               'old_token': asdict(old), 'old_ready_sha_unchanged': before,
                               'compatibility': '不支持跨规则旧CoreToken；从原Collection重新project，未重新冻结/导入历史'})
    print('Q3C2_CLEANUP_EVIDENCE=' + str(out))
    return h, new, out, original


class ClosingDuck:
    def __init__(self, db, events, *, fail=True, sql_error=False, on_close=None):
        self.actual, self.events, self.fail, self.sql_error, self.on_close = db, events, fail, sql_error, on_close
        self.primary = None
    def __getattr__(self, key):
        return getattr(self.actual, key)
    def execute(self, *args, **kwargs):
        if self.sql_error:
            try:
                return self.actual.execute('SELECT * FROM missing_h1_cleanup_fixture_table')
            except BaseException as error:
                self.primary = error
                raise
        return self.actual.execute(*args, **kwargs)
    def close(self):
        self.actual.close()
        self.events.append('DuckDB actually closed')
        if self.on_close:
            self.on_close()
        if self.fail:
            raise RuntimeError('fixture error AFTER real DuckDB close')


class ClosingPG:
    def __init__(self, pg, events, fail):
        self.actual, self.events, self.fail = pg, events, fail
    def __getattr__(self, key):
        return getattr(self.actual, key)
    def close(self):
        self.actual.close()
        self.events.append('PostgreSQL actually closed')
        if self.fail:
            raise RuntimeError('fixture error AFTER real PostgreSQL close')


def closed(db):
    with pytest.raises(duckdb.ConnectionException):
        db.actual.execute('SELECT 1')


def proof(out, name, s, events, error=None):
    save(out / (name + '.json'), {'events': events, 'failed': s.failed, 'receipt': s.receipt,
                                'cleanup_errors': s.cleanup_errors, 'raised_type': type(error).__name__ if error else None,
                                'traceback': ''.join(traceback.format_exception(error)) if error else None})


def test_real_duck_close_error_cannot_sign_complete(lake):
    h, token, out, _ = lake
    events = []
    with pytest.raises(RuntimeError, match='AFTER real DuckDB') as error:
        with h.core(token) as s:
            s.detail('2026-03-01', 0)
            db = s.db = ClosingDuck(s.db, events)
    assert s.failed and s.receipt is None and s.db is None
    closed(db)
    proof(out, 'duck-close-failure', s, events, error.value)


def test_real_sql_error_keeps_primary_with_close_error(lake):
    h, token, out, _ = lake
    events = []
    with h.core(token) as s:
        db = s.db = ClosingDuck(s.db, events, sql_error=True)
        with pytest.raises(duckdb.CatalogException) as error:
            s.detail('2026-03-01', 0)
        assert error.value is db.primary
        assert error.value.cleanup_errors[0]['resource'] == 'DuckDB'
        assert s.failed and s.db is None
    assert s.receipt is None
    closed(db)
    proof(out, 'real-sql-plus-close', s, events, error.value)


@pytest.mark.parametrize('final_failure', [False, True])
def test_pg_close_and_final_validation_cleanup(lake, monkeypatch, final_failure):
    h, token, out, _ = lake
    events, connections = [], []
    original_connect = h._connect
    original_qualify = h._qualify_core
    primary = ValueError('fixture final qualification failure')
    def connect():
        pg = ClosingPG(original_connect(), events, True)
        connections.append(pg)
        return pg
    def qualify(*a, **kw):
        result = original_qualify(*a, **kw)
        if kw.get('lock') and final_failure:
            raise primary
        return result
    with pytest.raises(ValueError if final_failure else RuntimeError) as error:
        with h.core(token) as s:
            s.detail('2026-03-01', 0)
            db = s.db = ClosingDuck(s.db, events, fail=final_failure)
            monkeypatch.setattr(h, '_connect', connect)
            monkeypatch.setattr(h, '_qualify_core', qualify)
    assert s.failed and s.receipt is None and s.db is None
    assert connections and all(p.actual.closed for p in connections)
    closed(db)
    if final_failure:
        assert error.value is primary
        assert [e['resource'] for e in primary.cleanup_errors] == ['DuckDB', 'qualification PostgreSQL']
    proof(out, 'final-plus-cleanup' if final_failure else 'pg-close-failure', s, events, error.value)


def test_success_receipt_after_all_owners_and_parameter_correction(lake, monkeypatch):
    h, token, out, _ = lake
    events, connections = [], []
    original_connect = h._connect
    def connect():
        pg = ClosingPG(original_connect(), events, False)
        connections.append(pg)
        return pg
    with h.core(token) as s:
        with pytest.raises(ValueError):
            s.query('2026-03-01', limit=0)
        assert not s.failed
        assert s.detail('2026-03-01', 0)['state'] == 'available'
        def under_lock():
            assert s.receipt is None and connections[-1].actual.closed == 0
            # 真正资格事务仍在；此查询不依赖已关闭DuckDB。
            with connections[-1].actual.cursor() as c:
                c.execute("SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() AND mode='RowShareLock'")
                assert c.fetchone()[0] > 0
            events.append('qualification share locks still held')
        db = s.db = ClosingDuck(s.db, events, fail=False, on_close=under_lock)
        monkeypatch.setattr(h, '_connect', connect)
    assert s.receipt['qualification'] == 'complete' and not s.failed
    assert events == ['DuckDB actually closed', 'qualification share locks still held', 'PostgreSQL actually closed']
    assert connections[-1].actual.closed and s.db is None
    closed(db)
    proof(out, 'normal-owner-order', s, events)


def test_early_stop_generator_exit_records_cleanup(lake):
    h, token, out, _ = lake
    events = []
    with h.core(token) as s:
        stream = s.bulk('core_records', batch_rows=1)
        next(stream)
        db = s.db = ClosingDuck(s.db, events)
        stream.close()
    assert s.failed and s.receipt is None and s.cleanup_errors and s.db is None
    closed(db)
    proof(out, 'early-stop-close', s, events)


def test_reproject_original_collection_full_fields_no_reimport(lake):
    h, token, out, original = lake
    expected = json.loads((original / 'ordered-domain.json').read_text())
    documents = json.loads((original / 'expected.json').read_text())
    with h.core(token) as s:
        for name in TABLES:
            actual = [r for b in s.bulk(name, batch_rows=17) for r in b['rows']]
            assert actual == expected[name]
        for i, item in enumerate(documents['2026-03-01']['items']):
            detail = s.detail('2026-03-01', i)
            assert detail['item'] == item and detail['record'] == documents['2026-03-01']['payloads'][i]
    assert s.receipt
    save(out / 'reprojection-full-fields.json', {'receipt': s.receipt, 'whole_ordered_rows': sum(map(len, expected.values())),
                                             'details': len(documents['2026-03-01']['items']), 'source_reimported': False})


@pytest.mark.parametrize('operation', ['detail_failed_day', 'detail_missing', 'rebuild'])
def test_legal_operation_terminal_registration(lake, operation):
    h, token, out, _ = lake
    with h.core(token) as s:
        if operation == 'rebuild':
            response = s.rebuild()
            key = 'rebuild:' + response['schema']
            with closing(h._connect()) as pg, pg.cursor() as c:
                c.execute('SELECT count(*) FROM ' + response['schema'] + '.records')
                assert c.fetchone()[0] == response['rows'] == 65
            expected = {'operation': 'rebuild', 'schema': response['schema'], 'token': response['token'],
                        'scope': 'core_query_index_only', 'status': 'committed', 'rows': 65}
        else:
            day, occurrence = ('2026-03-03', 0) if operation == 'detail_failed_day' else ('2026-03-01', 999999)
            response = s.detail(day, occurrence)
            key = 'detail:' + day + ':' + str(occurrence)
            if operation == 'detail_failed_day':
                assert response['state'] == 'validation_failed'
                assert all(response[k] is None for k in ('overview', 'trend', 'events'))
                resolution = 'unknown'
            else:
                assert response['state'] == 'not_retained' and response['record'] is None
                assert response['resolution'] == 'missing'
                resolution = 'missing'
            expected = {'operation': 'detail', 'day': day, 'occurrence': occurrence, 'status': 'completed',
                        'state': response['state'], 'resolution': resolution}
        assert s.receipt is None and response['qualification'] == 'provisional' and not s.failed
    assert s.receipt['qualification'] == 'complete' and not s.failed
    assert key in s.receipt['completed_operations']
    assert s.receipt['operation_scopes'][key] == expected
    save(out / (operation + '-after.json'), {'response': response, 'receipt': s.receipt})
