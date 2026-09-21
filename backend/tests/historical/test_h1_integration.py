"""H1 授权链人工集成：仅显式本任务私有库，原制品不重导。"""
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import uuid

import pytest
import psycopg2

from tests.historical import test_historical_core as fixture_tools
from tests.historical.test_historical_core import History, PGIdentity, decode_token, oracle_wire, oracle_selection, pages, save
from tests.historical.test_historical_core_cleanup import ClosingDuck, ClosingPG, closed, proof
from data_pipeline.history.event_index.model import TABLES, rule_sha
from data_pipeline.history.event_collection import CollectionToken, freeze_collection
from data_pipeline.history.database_import import Token
from data_pipeline.history.database_import.freeze import private_pg
from tests.country.country_query_test_support import PgLogProbe

RULE = '10f545190d3460bd8c351d82e7dc4fedb841c179481c5e0769fc758a54829e07'
DAY = '2026-03-01'
OLD = 'q3-c1/integration-3cad2ad5257b40198ed8eebee7247551'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class Measure:
    """实际独占 PG 日志；Python 哈希包括规则、原件、行摘要及游标。"""
    def __init__(self, h, out):
        self.h, self.out = h, out
        self.probe = PgLogProbe(h.dsn, h.root / 'h1-integration-pg.log')

    def run(self, label, action):
        counts = {'calls': 0, 'bytes': 0}
        actual = hashlib.sha256
        class Hash:
            def __init__(self, data=b'', **kw):
                counts['calls'] += 1
                counts['bytes'] += len(data)
                self.h = actual(data, **kw)
            def update(self, data):
                counts['bytes'] += len(data)
                self.h.update(data)
            def __getattr__(self, key):
                return getattr(self.h, key)
        offset = self.probe.start()
        start = time.monotonic()
        try:
            hashlib.sha256 = Hash
            result = action()
        finally:
            hashlib.sha256 = actual
        elapsed = time.monotonic() - start
        log, sql = self.probe.finish(offset)
        (self.out / (label + '.pg.log')).write_text(log)
        save(self.out / (label + '.cost.json'), {
            'wall_seconds': elapsed, 'pg_statements': sql, 'python_sha256': counts,
            'result': asdict(result) if hasattr(result, '__dataclass_fields__') else result,
            'scope': '本次独占PG实际日志；全Python SHA非OS总IO；RSS见回执且阶段独立峰值Unknown',
        })
        return result


@pytest.fixture(scope='module')
def lake():
    location = os.environ.get('Q3_PRIVATE_ROOT')
    if not location:
        pytest.skip('须明确绑定本任务私有PG')
    root = Path(location).resolve()
    assert root == Path('/tmp/domeye-integration-q3-8233').resolve()
    reuse = os.environ.get('H1_INTEGRATION_REUSE')
    if reuse:
        out = Path(reuse).resolve()
        assert out.parent == root / 'h1-integration-8233'
        binding = json.loads((out / 'binding.json').read_text())
        h = History(binding['dsn'], root, target_identity=PGIdentity(**binding['identity']))
        token = decode_token(binding['core'])
        assert token.rule_sha256 == rule_sha() == RULE
        return h, token, out, json.loads((out / 'expected.json').read_text())
    out = root / 'h1-integration-8233' / ('acceptance-' + uuid.uuid4().hex)
    out.mkdir(parents=True)
    name = 'h1_8233_' + uuid.uuid4().hex[:10]
    base = f'host={root / "socket"} port=28763'
    with closing(private_pg(base + ' dbname=postgres', root)) as pg:
        pg.autocommit = True
        with pg.cursor() as c:
            c.execute('CREATE DATABASE ' + name)
    dsn = base + ' dbname=' + name
    with closing(private_pg(dsn, root)) as pg:
        identity = PGIdentity.read(pg)
    h = History(dsn, root, target_identity=identity)
    assert rule_sha() == RULE
    original = fixture_tools.convert_anomaly_record
    def with_scalars(ref, rows, **kwargs):
        rows[0]['pre_vp_paths'].update(
            negative_zero=Decimal('-0.000'), negative_microsecond=timedelta(microseconds=-1),
            offset_microsecond=datetime(2026, 3, 1, 5, 30, 0, 123456,
                                        tzinfo=timezone(timedelta(hours=5, minutes=30))))
        return original(ref, rows, **kwargs)
    fixture_tools.convert_anomaly_record = with_scalars
    try:
        binding, expected = fixture_tools.fixture(out / 'source', count=19)
    finally:
        fixture_tools.convert_anomaly_record = original
    # 原始 SQLite 文本是独立 oracle；增加普通 extra 的重键、负零、超安全整数。
    with closing(sqlite3.connect(out / 'source' / (DAY + '.sqlite3'))) as db, db:
        for ordinal, raw in db.execute('SELECT rowid,item FROM records').fetchall():
            raw = raw[:-1] + ',"extra":{"a":1,"a":1.0,"z":-0,"big":9007199254740993}}'
            db.execute('UPDATE records SET item=? WHERE rowid=?', (raw, ordinal))
        cols = [d[0] for d in db.execute('SELECT * FROM records').description]
        source_rows = [dict(zip(cols, row)) for row in db.execute('SELECT * FROM records ORDER BY rowid')]
    binding = fixture_tools.rebind(out / 'source', binding)
    expected['raw_items'] = [oracle_wire(r['item']) for r in source_rows]
    expected['root'] = oracle_wire((out / 'source/manifest.json').read_bytes())
    expected['sql_columns'] = [{k: v for k, v in r.items() if k not in ('payload', 'item')} for r in source_rows]
    save(out / 'expected.json', expected)
    started = time.monotonic()
    manifest = freeze_collection(binding, out / 'freeze')
    save(out / 'freeze.cost.json', {'wall_seconds': time.monotonic() - started,
                                   'resources': json.loads(manifest.read_text())['resources']})
    measurement = Measure(h, out)
    collection = measurement.run('import', lambda: h.import_collection(manifest))
    token = measurement.run('project', lambda: h.project_core(collection, root_id=0))
    save(out / 'binding.json', dict(dsn=dsn, root=str(root), identity=asdict(identity), core=asdict(token)))
    for ident, label in ((collection.collection_id, 'collection'), (token.profile_id, 'core')):
        ready = json.loads((h.data_root / ident / 'ready.json').read_text())
        save(out / (label + '.ready.json'), ready)
    # 封存后读取必须不依赖原 source/freeze 地址；移动保留原证据。
    (out / 'source').rename(out / 'source-removed')
    (out / 'freeze').rename(out / 'freeze-removed')
    print('H1_INTEGRATION_EVIDENCE=' + str(out))
    return h, token, out, expected


def verify_scalars(tables, expected):
    seen = set()
    for scalar in tables['core_scalars']:
        node = expected[DAY]['payloads'][scalar['occurrence']]
        for step in json.loads(scalar['member_path']):
            if type(step) is int:
                node = node['items'][step]
            else:
                key, index = step
                assert node['members'][index][0] == key
                node = node['members'][index][1]
        members = dict(node['members'])
        kind = scalar['scalar_kind']
        assert members['$anomaly_scalar']['value'] == kind
        value = members['value'].get('lexeme', members['value'].get('value'))
        assert value == scalar['original_text']
        assert scalar['document_id'] == tables['core_records'][scalar['occurrence']]['payload_document']
        seen.add((kind, value))
        if kind in ('decimal', 'timedelta_us'):
            number = Decimal(value)
            sign, digits, exponent = number.as_tuple()
            assert scalar['sign'] == (-1 if sign else 1)
            assert scalar['coefficient_digits'] == ''.join(map(str, digits))
            assert scalar['exponent10'] == exponent
            assert bool(scalar['negative_zero']) == bool(sign and number == 0)
            # 旧便利列只收原十进制指数非负的整数；-0.000 的身份由精确列保留。
            convenient = int(number) if exponent >= 0 and len(digits) + exponent <= 19 and -(2**63) <= number < 2**63 else None
            assert scalar['integer_value'] == convenient
        else:
            dt = datetime.fromisoformat(value).astimezone(timezone.utc)
            delta = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)
            assert scalar['utc_microseconds'] == (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
            assert scalar['utc_time'] == dt.isoformat()
    assert {('decimal', '-0.000'), ('timedelta_us', '-1'),
            ('datetime', '2026-03-01T05:30:00.123456+05:30')} <= seen


def read_all(h, token, out, expected):
    with h.core(token) as s:
        assert s.metadata()['root_document'] == expected['root']
        tables = {name: [r for b in s.bulk(name, batch_rows=7) for r in b['rows']] for name in TABLES}
        for i, row in enumerate(tables['core_records']):
            assert row['occurrence'] == i
            assert {k: row[k] for k in expected['sql_columns'][i]} == expected['sql_columns'][i]
            detail = s.detail(DAY, i)
            assert detail['record'] == expected[DAY]['payloads'][i]
            assert detail['item_exact'] == expected['raw_items'][i]
            assert detail['item'] == expected[DAY]['items'][i]
            assert detail['provenance'] == expected[DAY]['provenance']
        verify_scalars(tables, expected)
        for link in tables['core_links']:
            assert link['document_id'] == tables['core_records'][link['occurrence']]['payload_document']
            assert link['resolution'] == ('unresolved_external' if link['role'] == 'H3_original_detail' else 'resolved')
    assert s.receipt['qualification'] == 'complete'
    return tables, s.receipt


def test_full_fields_and_fresh_process(lake):
    h, token, out, expected = lake
    def action():
        tables, receipt = read_all(h, token, out, expected)
        save(out / 'ordered-domain.json', tables)
        return {'counts': {k: len(v) for k, v in tables.items()}, 'receipt': receipt}
    Measure(h, out).run('full-read', action)
    script = '''
import sys,json
from pathlib import Path
from tests.historical.test_h1_integration import *
from data_pipeline.overview.index import DailyIndex
p=Path(sys.argv[1]);b=json.loads((p/'binding.json').read_text());e=json.loads((p/'expected.json').read_text())
h=History(b['dsn'],b['root'],target_identity=PGIdentity(**b['identity']));t=decode_token(b['core'])
assert not (p/'source').exists() and not (p/'freeze').exists()
DailyIndex._open=lambda *a: (_ for _ in ()).throw(AssertionError('禁止回访旧库'))
rows,receipt=read_all(h,t,p,e)
assert rows==json.loads((p/'ordered-domain.json').read_text())
query_and_rebuild(h,t,p,e,'child-')
save(p/'new-process.json',{'receipt':receipt,'counts':{k:len(v) for k,v in rows.items()},'source_freeze_absent':True})
'''
    result = subprocess.run([sys.executable, '-c', script, str(out)], capture_output=True, text=True,
                            cwd=Path(__file__).resolve().parents[2])
    with (out / 'new-process.log').open('a') as log:
        log.write(result.stdout + result.stderr)
    assert result.returncode == 0, result.stderr


def query_and_rebuild(h, token, out, expected, prefix=''):
    items = expected[DAY]['items']
    def queries():
        requests = 0
        with h.core(token) as s:
            for query in ({}, {'family': 'ipv4'}, {'family': 'ipv6'}, {'family': 'unknown'},
                          {'sort': 'time'}, {'level': 'unknown'}, {'q': '{64496, 64497}'},
                          {'kind': 'sub_hijack', 'q': '192.0.2.0/24'}, {'kind': 'country_outage', 'q': '伊朗'},
                          {'kind': 'as_outage', 'hour': 10, 'level': 'low'}):
                result = list(pages(s, limit=3, **query)); requests += len(result)
                assert [r['occurrence'] for p in result for r in p['events']['items']] == oracle_selection(items, **query)
                family = query.get('family', 'all')
                population = [items[i] for i in oracle_selection(items, family=family)]
                assert result[0]['overview']['record_count'] == len(population)
                for hour, bucket in enumerate(result[0]['trend']['buckets']):
                    assert bucket['value'] == len({v['object'] for v in population if v['kind'] == 'prefix_outage' and int(v['reference'].split('/')[1][11:13]) == hour})
            for ref, resolution, expected_ids in ((items[0]['reference'], 'ambiguous', [0, 1]),
                                                  (items[-1]['reference'], 'matched', [18]), ('missing', 'missing', [])):
                cursor, ids = None, []
                while True:
                    page = s.references(DAY, ref, limit=1, cursor=cursor)
                    assert page['resolution'] == resolution
                    ids.extend(v['occurrence'] for v in page['events']['items'])
                    cursor = page['next_cursor']
                    if cursor is None:
                        break
                assert ids == expected_ids
            empty = s.query('2026-03-02')
            assert empty['state'] == 'available' and empty['overview']['record_count'] == 0
            assert all(b['value'] == 0 for b in empty['trend']['buckets'])
            missing = s.query('2026-03-05')
            assert missing['state'] == 'not_retained' and all(missing[k] is None for k in ('overview', 'trend', 'events'))
            for day in ('2026-03-03', '2026-03-04', '2026-03-10', '2026-03-20'):
                for value in (s.query(day), s.detail(day, 0), s.references(day, 'missing')):
                    assert value['state'] == 'validation_failed'
                    assert all(value[k] is None for k in ('overview', 'trend', 'events'))
                    assert value['diagnostic'] == oracle_wire(json.dumps(expected[day]))
            missing_detail = s.detail(DAY, 999)
            assert missing_detail['state'] == 'not_retained' and missing_detail['resolution'] == 'missing'
        assert s.receipt['qualification'] == 'complete'
        for day in ('2026-03-03', '2026-03-04', '2026-03-10', '2026-03-20'):
            scope = s.receipt['operation_scopes']['detail:' + day + ':0']
            assert scope['state'] == 'validation_failed' and scope['resolution'] == 'unknown'
        assert s.receipt['operation_scopes']['detail:' + DAY + ':999']['resolution'] == 'missing'
        return {'page_requests': requests, 'receipt': s.receipt}
    Measure(h, out).run(prefix + 'pages', queries)
    def rebuild():
        with h.core(token) as s:
            result = s.rebuild()
            assert s.receipt is None and result['qualification'] == 'provisional'
            with closing(h._connect()) as pg, pg.cursor() as c:
                c.execute('SELECT * FROM ' + result['schema'] + '.records ORDER BY occurrence')
                columns = [d[0] for d in c.description]
                assert columns == ['day', 'occurrence', 'reference', 'kind', 'family', 'level', 'hour', 'severity', 'start_time', 'search', 'item_document', 'payload_document']
                rows = [dict(zip(columns, row)) for row in c.fetchall()]
                domain = json.loads((out / 'ordered-domain.json').read_text())['core_records']
                assert rows == [{k: row[k] for k in columns} for row in domain]
        scope = s.receipt['operation_scopes']['rebuild:' + result['schema']]
        assert scope['scope'] == 'core_query_index_only' and scope['status'] == 'committed' and scope['rows'] == 19
        assert scope['token'] == asdict(token)
        return {'receipt': s.receipt, 'rebuild': result, 'second_pg_all_columns_before_exit': rows}
    Measure(h, out).run(prefix + 'rebuild', rebuild)


@pytest.mark.parametrize('sql_error', [False, True])
def test_native_cleanup_failure(lake, sql_error):
    import duckdb
    h, token, out, _ = lake
    events = []
    with pytest.raises(duckdb.CatalogException if sql_error else RuntimeError) as error:
        with h.core(token) as s:
            if not sql_error:
                s.detail(DAY, 0)
            wrapped = s.db = ClosingDuck(s.db, events, sql_error=sql_error)
            if sql_error:
                s.detail(DAY, 0)
    assert s.failed and s.receipt is None and s.db is None
    closed(wrapped)
    if sql_error:
        assert error.value is wrapped.primary
        assert error.value.cleanup_errors[0]['resource'] == 'DuckDB'
    proof(out, 'sql-plus-close' if sql_error else 'close-failure', s, events, error.value)


def test_actual_shared_lock_release_order(lake, monkeypatch):
    h, token, out, _ = lake
    connect = h._connect
    events, connections = [], []
    def tracked():
        pg = ClosingPG(connect(), events, False)
        connections.append(pg)
        return pg
    with h.core(token) as s:
        s.detail(DAY, 0)
        def contender():
            assert s.receipt is None and connections[-1].actual.closed == 0
            with closing(connect()) as pg, pg.cursor() as c:
                c.execute("SET lock_timeout='100ms'")
                with pytest.raises(psycopg2.errors.LockNotAvailable):
                    c.execute('UPDATE history_q3.core_profiles SET state=state WHERE profile_id=%s', (token.profile_id,))
                pg.rollback()
            events.append('second PG update blocked by qualification shared lock')
        wrapped = s.db = ClosingDuck(s.db, events, fail=False, on_close=contender)
        monkeypatch.setattr(h, '_connect', tracked)
    assert events == ['DuckDB actually closed', 'second PG update blocked by qualification shared lock', 'PostgreSQL actually closed']
    assert s.receipt['qualification'] == 'complete' and connections[-1].actual.closed
    closed(wrapped)
    proof(out, 'shared-lock-order', s, events)


def test_original_collection_and_old_tokens_readonly(lake, monkeypatch):
    from data_pipeline.history.event_collection.model import DEFINITIONS
    from tests.historical.test_q3c1_integration import test_five_original_q3a_q3b_tokens_without_reimport
    h, _, out, _ = lake
    original = h.root / OLD
    binding = json.loads((original / 'binding.json').read_text())
    values = dict(binding['token'])
    values['children'] = tuple(Token(**v) for v in values['children'])
    token = CollectionToken(**values)
    prior = History(binding['dsn'], h.root, target_identity=PGIdentity(**binding['identity']))
    paths = [p for p in (h.data_root / token.collection_id).rglob('*') if p.is_file()]
    paths += [p for p in original.rglob('*') if p.is_file()]
    before = {str(p): sha(p) for p in paths}
    with prior.collection(token) as session:
        rows = {name: [r for b in session.bulk(name, batch_rows=17) for r in b['rows']] for name in DEFINITIONS}
    # C.1 独立原始全行快照使用其原编码规则（含 Decimal/bytes）。
    from tests.historical.test_historical_collection import encoded
    assert encoded(rows) == (original / 'ordered-typed.json').read_bytes()
    assert session.receipt['qualification'] == 'complete'
    assert all(sha(p) == value for p, value in before.items())
    save(out / 'original-c1.json', {'token': asdict(token), 'unchanged_files': before, 'receipt': session.receipt})
    test_five_original_q3a_q3b_tokens_without_reimport((h, token, out), monkeypatch)
