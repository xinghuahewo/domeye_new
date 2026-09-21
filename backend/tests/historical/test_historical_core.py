"""Q3-C.2仅自造H1输入与显式本任务PG；全页/多重集/词法证据可独立复算。"""
from contextlib import closing
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import uuid

import pytest

from data_pipeline.common.event_records import convert_anomaly_record, serialize_record
from data_pipeline.overview.input import overview_item, overview_search_text, overview_level_filter
from data_pipeline.overview.index import DailyIndex
from data_pipeline.history.event_collection import Binding, Root, freeze_collection, CollectionToken
from data_pipeline.history.event_index import History, CoreToken
from data_pipeline.history.event_index.model import TABLES, code_identity
from data_pipeline.history.event_index.exact import loads, native, wire
from data_pipeline.history.database_import import Token, Limits
from data_pipeline.history.database_import.binding import PGIdentity
from data_pipeline.history.database_import.freeze import private_pg

PROFILE = json.loads((Path(__file__).resolve().parents[3] / 'config/data-profile.json').read_text())
KINDS = ['prefix_outage', 'as_outage', 'leak', 'hijack', 'sub_hijack', 'country_outage']
SOURCE = dict(instance='fixture-only/core-h1', code='r', collector_id='rrc25', collector_basis='user_confirmation',
              coverage='unknown', detector_version=None, confirmed_on='2026-09-13')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def oracle_wire(raw):
    """独立标准库pairs/数字词法oracle，不调用产品exact/Parser构造期望。"""
    class Pairs(list):
        pass
    class Numeric(str):
        pass
    value = json.loads(raw, object_pairs_hook=Pairs, parse_int=Numeric, parse_float=Numeric)
    def encode(v):
        if isinstance(v, Pairs):
            return {'kind': 'object', 'members': [[k, encode(x)] for k, x in v]}
        if isinstance(v, Numeric):
            return {'kind': 'number', 'lexeme': str(v)}
        if isinstance(v, list):
            return {'kind': 'array', 'items': [encode(x) for x in v]}
        return {'kind': 'null' if v is None else 'bool' if type(v) is bool else 'string', 'value': v}
    return encode(value)


def oracle_selection(items, family='all', kind='all', level='all', hour=None, q='', sort='severity'):
    selected = []
    for i, v in enumerate(items):
        family_ok = family == 'all' or v['address_family'] == family or family in ('ipv4', 'ipv6') and v['address_family'] == 'mixed'
        level_value = 'conflict' if 'level_conflict' in v else v['level'] or 'unknown'
        search = ' '.join([v['object'], v['record_number'], *['AS' + a for a in v['asns']],
                           *([v['parent_prefix']] if 'parent_prefix' in v else []), *([v['country_name']] if v.get('country_name') else [])]).lower()
        if (family_ok and (kind == 'all' or kind == v['kind']) and (level == 'all' or level == level_value)
                and (hour is None or hour == int(v['reference'].split('/')[1][11:13]))
                and (not q or q == v['reference'] or q.lower() in search)):
            selected.append(i)
    selected.sort(key=lambda i: (items[i]['reference'], i))
    selected.sort(key=lambda i: items[i]['start_time'], reverse=True)
    if sort == 'severity':
        selected.sort(key=lambda i: {'high': 0, 'middle': 1, 'low': 2}.get(items[i]['level'], 3))
    return selected


def window(day):
    start = datetime.fromisoformat(day).replace(tzinfo=timezone(timedelta(hours=8)))
    return {'source': 'r', 'start': start.isoformat(), 'end_exclusive': (start + timedelta(days=1)).isoformat()}


def make_record(day, index, duplicate):
    n = index // 2 if duplicate else index
    variant = n % 9
    kind = ['prefix_outage', 'prefix_outage', 'as_outage', 'as_outage', 'as_outage', 'leak', 'hijack', 'sub_hijack', 'country_outage'][variant]
    hour = 8 + n % 3
    start = f'{day} {hour:02}:00:00'
    aware = datetime.fromisoformat(start).replace(tzinfo=timezone(timedelta(hours=8))).astimezone(timezone(timedelta(hours=5, minutes=30)))
    row = {'source': 'r', 's_time': aware, 'e_time': aware + timedelta(seconds=5), 'duration': timedelta(seconds=5),
           'event_info': None, 'pre_vp_paths': {'precise': Decimal('12345678901234567890.123456789012345678900'), 'offset': aware}}
    level = None if variant == 8 else ['high', 'middle', 'low'][n % 3]
    if kind == 'prefix_outage':
        target = '192.0.2.0/24' if variant == 0 else '2001:db8::/32'
        row.update(prefix=target, outage_id=str(n), asn='64496', outage_level=level)
    elif kind == 'as_outage':
        target = '{64496, 64497}' if variant == 4 else '64496'
        row.update(asn=target, outage_id=str(n), outage_level=level,
                   outage_prefixes=['192.0.2.0/24', '2001:db8::/32'] if variant == 2 else None)
    elif kind == 'leak':
        target = '198.51.100.0/24'
        row.update(prefix=target, leak_event_id=str(n), leak_level=level, as_path='{64496,64497} 64498', leak_by='64496')
    elif kind == 'hijack':
        target = '203.0.113.0/24'
        row.update(prefix=target, hijack_eventid=str(n), hijack_level=level, hijacked_as='64496', hijacker_as='64497')
    elif kind == 'sub_hijack':
        target = '192.0.2.0/25'
        row.update(prefix=target, sub_hijack_eventid=str(n), sub_hijack_level=level, hijacked_prefix='192.0.2.0/24', hijacked_as="['64496']", hijacker_as="['64497']")
    else:
        target = 'IR'
        row.update(country=target, outage_id=str(n), outage_level=level, country_chinese_name='伊朗', outage_ases=['64496', '64497'])
    table = ('leak_event' if kind == 'leak' else kind) + '_' + day[:7].replace('-', '')
    ref = f'{kind}/{start}/{target.replace("/", "-")}/{n}/r'
    source = dict(instance=SOURCE['instance'], table=table, representation='source_rows', read_at='2026-09-13T01:00:00+00:00',
                  read_scope=window(day), collector_id='rrc25', content_version=None, detector_version=None,
                  evidence_refs=[{'file': 'evidence.txt', 'sha256': sha(b'artificial H1 evidence\n')}])
    return convert_anomaly_record(ref, [row], source=source, data_profile=PROFILE, preserve_unresolved_as_set=variant == 4)


def fixture(root, count=65, duplicate=True):
    root.mkdir()
    (root / 'evidence.txt').write_bytes(b'artificial H1 evidence\n')
    manifest = dict(schema_version='core-overview-index/v2', data_profile=PROFILE, source=SOURCE, kinds=KINDS,
                    interpretation_version='recorded-anomaly-overview/v3', days={}, diagnostics={})
    expected = {}
    for day, number, rule in [('2026-03-01', count, 'recorded-anomaly-overview/v3'), ('2026-03-02', 0, 'recorded-anomaly-overview/v2')]:
        records = [make_record(day, i, duplicate) for i in range(number)]
        payloads = [serialize_record(r) for r in records]
        jsonl = b'\n'.join(payloads) + (b'\n' if payloads else b'')
        name = day + '-records.jsonl'
        (root / name).write_bytes(jsonl)
        p = dict(schema_version='core-overview-input/v1', data_profile=PROFILE, source=SOURCE, kinds=KINDS,
                 interpretation_version=rule, window=window(day), records={'file': name, 'sha256': sha(jsonl), 'count': number}, level_conflicts={})
        provenance = json.dumps(p, ensure_ascii=False, sort_keys=True).encode()
        path = root / (day + '.sqlite3')
        items = []
        with closing(sqlite3.connect(path)) as db, db:
            # 无PK允许同ref不同occurrence；不能用旧fetchone作为唯一oracle。
            db.execute('CREATE TABLE records(reference TEXT,kind TEXT,object TEXT,start_time TEXT,hour INTEGER,family TEXT,level TEXT,severity INTEGER,search TEXT,item TEXT,payload BLOB)')
            db.execute('CREATE TABLE provenance(manifest BLOB)')
            db.execute('INSERT INTO provenance VALUES (?)', (provenance,))
            for i, r in enumerate(records):
                item = overview_item(r)
                items.append(item)
                db.execute('INSERT INTO records VALUES (?,?,?,?,?,?,?,?,?,?,?)', (item['reference'], item['kind'], item['object'], item['start_time'],
                           int(item['reference'].split('/')[1][11:13]), item['address_family'], overview_level_filter(item),
                           {'high': 0, 'middle': 1, 'low': 2}.get(item['level'], 3), overview_search_text(item), json.dumps(item), payloads[i]))
        manifest['days'][day] = dict(file=path.name, sha256=sha(path.read_bytes()), count=number, input_version='overview_v1_' + sha(provenance), window=window(day), interpretation_version=rule)
        expected[day] = {'items': items, 'payloads': [oracle_wire(p) for p in payloads], 'provenance': oracle_wire(provenance)}
    for i, day in enumerate(('2026-03-03', '2026-03-04', '2026-03-10', '2026-03-20')):
        reading = i == 0
        evidence = dict(format='failed-day-read/v1' if reading else 'complete-day-audit/v1', query_window=window(day),
                        source_data_sha256=str(i + 1) * 64, query_sha256='a' * 64, selection_sha256='b' * 64,
                        read_at='2026-09-13T01:00:00+00:00', finished_at=None if reading else '2026-09-13T01:01:00+00:00',
                        receipt_sha256=None if reading else 'c' * 64, audit_sha256=None if reading else 'd' * 64,
                        failure_sha256='e' * 64 if reading else None)
        reason = dict(kind='all' if reading else ['as_outage', 'country_outage', 'hijack'][i - 1],
                      code='source_read_timeout' if reading else ['source_identity_unresolved', 'source_population_mismatch', 'start_time_conflict'][i - 1],
                      count=None if reading else i, evidence=evidence)
        diagnostic = dict(schema_version='core-overview-diagnostic/v2', stage='source_read' if reading else 'source_field_validation',
                          source=SOURCE, data_profile=PROFILE, window=window(day), compiler_sha256='f' * 64, reasons=[reason])
        path = root / (day + '.diagnostic.json')
        save(path, diagnostic)
        manifest['diagnostics'][day] = {'file': path.name, 'sha256': sha(path.read_bytes()), 'window': window(day)}
        expected[day] = diagnostic
    manifest['window'] = {'source': 'r', 'start': window('2026-03-01')['start'], 'end_exclusive': window('2026-03-02')['end_exclusive']}
    save(root / 'manifest.json', manifest)
    binding = Binding((Root('core-index/v1', str(root / 'manifest.json'), (root / 'manifest.json').as_uri(), 'fixture-H1', sha((root / 'manifest.json').read_bytes())),), (str(root),))
    return binding, expected


@pytest.fixture(scope='module')
def lake():
    location = os.environ.get('Q3_PRIVATE_ROOT')
    if not location:
        pytest.skip('仅显式本任务人工PG')
    root = Path(location)
    out = root / 'q3-c2' / ('acceptance-' + uuid.uuid4().hex)
    out.mkdir(parents=True)
    base = f'host={root / "socket"} port=28763'
    name = 'q3c2_' + uuid.uuid4().hex[:12]
    with closing(private_pg(base + ' dbname=postgres', root)) as pg:
        pg.autocommit = True
        with pg.cursor() as c:
            c.execute('CREATE DATABASE ' + name)
    dsn = base + ' dbname=' + name
    with closing(private_pg(dsn, root)) as pg:
        identity = PGIdentity.read(pg)
    h = History(dsn, root, target_identity=identity)
    binding, expected = fixture(out / 'source')
    manifest = freeze_collection(binding, out / 'freeze')
    token = h.import_collection(manifest)
    core = h.project_core(token, root_id=0)
    save(out / 'binding.json', dict(dsn=dsn, root=str(root), identity=asdict(identity), collection=asdict(token), core=asdict(core)))
    save(out / 'expected.json', expected)
    print('Q3C2_EVIDENCE=' + str(out))
    return h, core, out, expected


def pages(session, day='2026-03-01', **kw):
    cursor = None
    while True:
        result = session.query(day, cursor=cursor, **kw)
        yield result
        cursor = result.get('next_cursor')
        if cursor is None:
            break


def test_complete_ordered_fields_and_exact_details(lake):
    h, token, out, expected = lake
    items = expected['2026-03-01']['items']
    order = sorted(range(len(items)), key=lambda i: (items[i]['reference'], i))
    order.sort(key=lambda i: items[i]['start_time'], reverse=True)
    order.sort(key=lambda i: {'high': 0, 'middle': 1, 'low': 2}.get(items[i]['level'], 3))
    with h.core(token) as s:
        actual = [r for p in pages(s, limit=17) for r in p['events']['items']]
        assert [r['occurrence'] for r in actual] == order
        assert [r['item'] for r in actual] == [items[i] for i in order]
        for i in range(len(items)):
            detail = s.detail('2026-03-01', i)
            assert detail['item'] == items[i]
            assert detail['record'] == expected['2026-03-01']['payloads'][i]
            assert detail['provenance'] == expected['2026-03-01']['provenance']
        tables = {name: [r for b in s.bulk(name, batch_rows=17) for r in b['rows']] for name in TABLES}
    assert s.receipt
    save(out / 'whole-session.json', s.receipt)
    save(out / 'ordered-domain.json', tables)
    assert not (h.data_root / token.profile_id / 'projection.sqlite').exists()
    assert len(tables['core_records']) == 65
    assert {r['original_text'] for r in tables['core_scalars'] if r['scalar_kind'] == 'decimal'} == {'12345678901234567890.123456789012345678900'}
    for scalar in tables['core_scalars']:
        record = tables['core_records'][scalar['occurrence']]
        assert scalar['document_id'] == record['payload_document']
        node = expected['2026-03-01']['payloads'][scalar['occurrence']]
        for step in json.loads(scalar['member_path']):
            if type(step) is int:
                node = node['items'][step]
            else:
                key, ordinal = step
                assert node['members'][ordinal][0] == key
                node = node['members'][ordinal][1]
        members = dict(node['members'])
        assert members['$anomaly_scalar']['value'] == scalar['scalar_kind']
        assert members['value'].get('lexeme', members['value'].get('value')) == scalar['original_text']
        if scalar['scalar_kind'] == 'decimal':
            assert scalar['coefficient_digits'] == '12345678901234567890123456789012345678900'
            assert scalar['exponent10'] == -21 and scalar['sign'] == 1 and scalar['integer_value'] is None
        elif scalar['scalar_kind'] == 'timedelta_us':
            assert scalar['integer_value'] == 5000000
        else:
            dt = datetime.fromisoformat(scalar['original_text']).astimezone(timezone.utc)
            delta = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)
            assert scalar['utc_microseconds'] == (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
    for link in tables['core_links']:
        assert link['document_id'] == tables['core_records'][link['occurrence']]['payload_document']
        assert link['resolution'] == ('unresolved_external' if link['role'] == 'H3_original_detail' else 'resolved')


@pytest.mark.parametrize('limit', [1, 17, 60])
def test_pages_filters_trend_and_ambiguity(lake, limit):
    h, token, _, expected = lake
    items = expected['2026-03-01']['items']
    with h.core(token) as s:
        for family in ('all', 'ipv4', 'ipv6', 'unknown'):
            all_pages = list(pages(s, family=family, limit=limit))
            selected = [v for v in items if family == 'all' or v['address_family'] == family or family in ('ipv4', 'ipv6') and v['address_family'] == 'mixed']
            actual = [v['item'] for p in all_pages for v in p['events']['items']]
            assert actual == [items[i] for i in oracle_selection(items, family=family)]
            first = all_pages[0]
            assert first['overview']['record_count'] == len(selected)
            for hour, bucket in enumerate(first['trend']['buckets']):
                expected_prefixes = {v['object'] for v in selected if v['kind'] == 'prefix_outage' and int(v['reference'].split('/')[1][11:13]) == hour}
                assert bucket['value'] == len(expected_prefixes)
            restricted = list(pages(s, family=family, kind='as_outage', level='high', hour=10, q='AS64496', limit=limit))
            assert [v['item'] for p in restricted for v in p['events']['items']] == [items[i] for i in oracle_selection(items, family=family, kind='as_outage', level='high', hour=10, q='AS64496')]
            assert all(p['trend'] == first['trend'] for p in restricted)
            assert all(p['overview'] == first['overview'] for p in restricted)
        cursor = None
        candidates = []
        while True:
            p = s.references('2026-03-01', items[0]['reference'], limit=limit, cursor=cursor)
            assert p['resolution'] == 'ambiguous'
            candidates.extend(p['events']['items'])
            cursor = p['next_cursor']
            if cursor is None:
                break
        assert [v['occurrence'] for v in candidates] == [0, 1]
        assert s.references('2026-03-01', 'missing')['resolution'] == 'missing'
    assert s.receipt


def test_day_gates_and_admitted_empty(lake):
    h, token, _, expected = lake
    with h.core(token) as s:
        empty = s.query('2026-03-02')
        assert empty['state'] == 'available' and empty['overview']['record_count'] == 0
        assert empty['metadata']['root_interpretation'].endswith('/v3') and empty['metadata']['input_interpretation'].endswith('/v2')
        assert all(b['value'] == 0 for b in empty['trend']['buckets'])
        for day in ('2026-03-03', '2026-03-04', '2026-03-10', '2026-03-20'):
            for result in (s.query(day), s.detail(day, 0), s.references(day, 'anything')):
                assert result['state'] == 'validation_failed'
                assert all(result[k] is None for k in ('overview', 'trend', 'events'))
                assert result['diagnostic'] == oracle_wire(json.dumps(expected[day]).encode())
        assert s.query('2026-03-05')['state'] == 'not_retained'
    assert s.receipt


def decode_token(value):
    value = dict(value)
    collection = dict(value['collection'])
    collection['children'] = tuple(Token(**v) for v in collection['children'])
    value['collection'] = CollectionToken(**collection)
    return CoreToken(**value)


def test_actual_new_process_after_source_and_freeze_removed(lake):
    h, token, out, expected = lake
    (out / 'source').rename(out / 'source-removed')
    (out / 'freeze').rename(out / 'freeze-removed')
    code = '''
from pathlib import Path
import json,sys
from contextlib import closing
from tests.historical.test_historical_core import History, PGIdentity, decode_token, pages, save, oracle_selection
from data_pipeline.history.event_index.model import TABLES
from data_pipeline.overview.index import DailyIndex
p=Path(sys.argv[1]); b=json.loads((p/'binding.json').read_text()); expected=json.loads((p/'expected.json').read_text())
h=History(b['dsn'],b['root'],target_identity=PGIdentity(**b['identity'])); token=decode_token(b['core'])
DailyIndex._open=lambda *a: (_ for _ in ()).throw(AssertionError('不能临时回访旧DailyIndex'))
with h.core(token) as s:
    tables={name:[r for b in s.bulk(name,batch_rows=17) for r in b['rows']] for name in TABLES}
    assert tables==json.loads((p/'ordered-domain.json').read_text())
    actual=[r for page in pages(s,limit=17) for r in page['events']['items']]
    assert len(actual)==len(expected['2026-03-01']['items'])
    for item in actual:
        i=item['occurrence']; detail=s.detail('2026-03-01',i)
        assert detail['record']==expected['2026-03-01']['payloads'][i]
        assert detail['item']==expected['2026-03-01']['items'][i]
    items=expected['2026-03-01']['items']
    for query in ({'family':'ipv6','sort':'time'},{'level':'unknown'},{'kind':'as_outage','q':'AS64496'},{'hour':10}):
        result=[r for page in pages(s,limit=17,**query) for r in page['events']['items']]
        assert [r['occurrence'] for r in result]==oracle_selection(items,**query)
        assert [r['item'] for r in result]==[items[i] for i in oracle_selection(items,**query)]
    assert s.query('2026-03-02')['overview']['record_count']==0
    assert s.references('2026-03-01',items[0]['reference'])['resolution']=='ambiguous'
    assert s.references('2026-03-01',items[-1]['reference'])['resolution']=='matched'
    assert s.references('2026-03-01','missing')['resolution']=='missing'
    for day in ('2026-03-03','2026-03-04','2026-03-10','2026-03-20'):
        assert s.detail(day,0)['state']=='validation_failed'
    rebuilt=s.rebuild()
assert s.receipt
with closing(h._connect()) as pg,pg.cursor() as c:
    c.execute('SELECT * FROM '+rebuilt['schema']+'.records ORDER BY occurrence')
    actual=c.fetchall(); columns=[d[0] for d in c.description]
    assert [dict(zip(columns,r)) for r in actual]==[{k:r[k] for k in columns} for r in tables['core_records']]
    c.execute('SELECT column_name FROM information_schema.columns WHERE table_schema=%s',(rebuilt['schema'],))
    assert not {'nodes','payload','item','raw_fields'} & {v[0] for v in c.fetchall()}
save(p/'new-process.json',{'receipt':s.receipt,'rebuild':rebuilt,'all_ordered_rows':sum(len(v) for v in tables.values()),'original_sources_absent':True})
'''
    run = subprocess.run([sys.executable, '-c', code, str(out)], capture_output=True, text=True)
    (out / 'new-process.log').write_text(run.stdout + run.stderr)
    assert run.returncode == 0, run.stderr


def test_invalid_parameters_cursor_and_early_stop(lake):
    h, token, _, _ = lake
    with h.core(token) as s:
        for action in (lambda: s.query('bad-date'), lambda: s.query('2026-03-01', limit=0),
                       lambda: s.query('2026-03-01', hour=True), lambda: s.detail('2026-03-01', -1)):
            with pytest.raises(ValueError):
                action()
            assert not s.failed
        first = s.query('2026-03-01', limit=1)
        for values in ({'family': 'ipv6'}, {'sort': 'time'}, {'q': 'AS64496'}):
            with pytest.raises(ValueError, match='游标'):
                s.query('2026-03-01', limit=1, cursor=first['next_cursor'], **values)
        assert s.detail('2026-03-01', 999999)['state'] == 'not_retained'
        assert not s.failed
    assert s.receipt is None  # 未耗尽首个合法分页，其他成功操作不能覆盖它。
    with h.core(token) as s:
        stream = s.bulk('core_records', batch_rows=1)
        next(stream)
        stream.close()
    assert s.receipt is None and s.db is None


@pytest.mark.parametrize('operation', ['query', 'detail', 'bulk', 'rebuild'])
def test_current_row_budget_and_caught_error(lake, operation):
    h, token, _, _ = lake
    reader = History(h.dsn, h.root, target_identity=h.target_identity, limits=replace(h.limits, max_row_bytes=64))
    with reader.core(token) as s:
        with pytest.raises(ValueError):
            if operation == 'query':
                s.query('2026-03-01')
            elif operation == 'detail':
                s.detail('2026-03-01', 0)
            elif operation == 'rebuild':
                s.rebuild()
            else:
                with closing(s.bulk('core_records')) as stream:
                    next(stream)
        assert s.failed and s.db is None
    assert s.receipt is None


def test_actual_document_truncation_then_restoration(lake):
    h, token, out, _ = lake
    row = json.loads((out / 'ordered-domain.json').read_text())['core_records'][0]
    path = h.data_root / token.collection.collection_id / 'source' / row['payload_path']
    original = path.read_bytes()
    with h.core(token) as s:
        try:
            path.write_bytes(b'')
            with pytest.raises(ValueError, match='截断'):
                s.detail('2026-03-01', 0)
        finally:
            path.write_bytes(original)
        assert s.failed and s.db is None
    assert s.receipt is None


def test_whole_response_budget_includes_metadata_and_trend(lake):
    from data_pipeline.history.event_collection.store import row_bytes
    h, token, _, _ = lake
    with h.core(token) as s:
        page = s.query('2026-03-01', limit=1)
    maximum = len(row_bytes(page)) - 1
    reader = History(h.dsn, h.root, target_identity=h.target_identity,
                     limits=replace(h.limits, max_row_bytes=maximum, batch_bytes=maximum))
    with reader.core(token) as s:
        with pytest.raises(ValueError, match='完整返回页'):
            s.query('2026-03-01', limit=1)
        assert s.failed and s.db is None
    assert s.receipt is None


@pytest.mark.parametrize('stage', ['before', 'yield', 'final'])
def test_budget_drift(lake, stage):
    h, token, _, _ = lake
    original = h.limits
    s = h.core(token)
    try:
        with pytest.raises(ValueError, match='预算漂移'):
            if stage == 'before':
                h.limits = replace(original, max_row_bytes=original.max_row_bytes - 1)
            with s:
                if stage == 'yield':
                    with closing(s.bulk('core_records', batch_rows=1)) as stream:
                        next(stream)
                        h.limits = replace(original, max_row_bytes=original.max_row_bytes - 1)
                        next(stream)
                else:
                    h.limits = replace(original, max_row_bytes=original.max_row_bytes - 1)
    finally:
        h.limits = original
    assert s.receipt is None and s.db is None


@pytest.mark.parametrize('target', ['profile', 'collection', 'child', 'ready', 'rule', 'source'])
def test_qualification_revocation_before_and_at_exit(lake, monkeypatch, target):
    h, token, out, _ = lake
    original = None
    path = None
    relation = None
    value = None
    if target in ('profile', 'collection', 'child'):
        relation, key, value = {'profile': ('core_profiles', 'profile_id', token.profile_id),
                                'collection': ('collections', 'collection_id', token.collection.collection_id),
                                'child': ('imports', 'import_id', token.collection.children[0].import_id)}[target]
    elif target == 'ready':
        path = h.data_root / token.profile_id / 'ready.json'
    elif target == 'source':
        raw = json.loads((out / 'ordered-domain.json').read_text())['core_records'][0]
        path = h.data_root / token.collection.collection_id / 'source' / raw['item_path']
    if path:
        original = path.read_bytes()
    from data_pipeline.history.event_index import store
    actual_code = store.code_identity
    def change(broken):
        if relation:
            with closing(h._connect()) as pg, pg, pg.cursor() as c:
                c.execute('UPDATE history_q3.' + relation + ' SET state=%s WHERE ' + key + '=%s', ('failed' if broken else 'complete', value))
        elif path:
            path.write_bytes(original + b' ' if broken else original)
        else:
            monkeypatch.setattr(store, 'code_identity', (lambda: {'changed': 'fixture'}) if broken else actual_code)
    try:
        change(True)
        with pytest.raises(ValueError):
            with h.core(token):
                pass
        change(False)
        with pytest.raises(ValueError):
            with h.core(token) as s:
                s.detail('2026-03-01', 0)
                change(True)
        assert s.receipt is None and s.db is None
    finally:
        change(False)


def rebind(root, binding):
    path = root / 'manifest.json'
    m = json.loads(path.read_text())
    for day, entry in m['days'].items():
        entry['sha256'] = sha((root / entry['file']).read_bytes())
    save(path, m)
    return replace(binding, roots=(replace(binding.roots[0], sha256=sha(path.read_bytes())),))


@pytest.mark.parametrize('change', ['column', 'item', 'provenance', 'tag', 'common', 'duplicate_semantic', 'unsupported_root'])
def test_conflicts_keep_structure_but_reject_domain(lake, tmp_path, change):
    h, _, _, _ = lake
    root = tmp_path / 'source'
    binding, _ = fixture(root, count=3)
    path = root / '2026-03-01.sqlite3'
    with closing(sqlite3.connect(path)) as db, db:
        if change == 'column':
            db.execute("UPDATE records SET family='unknown' WHERE rowid=1")
        elif change == 'item':
            raw = db.execute('SELECT item FROM records WHERE rowid=1').fetchone()[0]
            value = json.loads(raw)
            value['object'] = '203.0.113.0/24'
            db.execute('UPDATE records SET item=? WHERE rowid=1', (json.dumps(value),))
        elif change == 'provenance':
            raw = json.loads(db.execute('SELECT manifest FROM provenance').fetchone()[0])
            raw['interpretation_version'] = 'recorded-anomaly-overview/v2'
            db.execute('UPDATE provenance SET manifest=?', (json.dumps(raw).encode(),))
        elif change == 'duplicate_semantic':
            raw = db.execute('SELECT item FROM records WHERE rowid=1').fetchone()[0]
            db.execute('UPDATE records SET item=? WHERE rowid=1', (raw[:-1] + ',"object":"x"}',))
        elif change in ('tag', 'common'):
            payload = json.loads(db.execute('SELECT payload FROM records WHERE rowid=1').fetchone()[0])
            if change == 'tag':
                payload['record']['raw_fields']['duration']['extra'] = 'conflict'
            else:
                payload['record']['common']['object']['value'] = '203.0.113.0/24'
            # 明确更新原content身份及补验JSONL，让反例到达真正tag/共同字段核验。
            payload['content_version'] = 'anr_v1_' + sha(json.dumps(payload['record'], ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode())
            raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
            db.execute('UPDATE records SET payload=? WHERE rowid=1', (raw,))
            f = root / '2026-03-01-records.jsonl'
            lines = f.read_bytes().splitlines()
            lines[0] = raw
            f.write_bytes(b'\n'.join(lines) + b'\n')
            p = json.loads(db.execute('SELECT manifest FROM provenance').fetchone()[0])
            p['records']['sha256'] = sha(f.read_bytes())
            p_raw = json.dumps(p).encode()
            db.execute('UPDATE provenance SET manifest=?', (p_raw,))
            m = json.loads((root / 'manifest.json').read_text())
            m['days']['2026-03-01']['input_version'] = 'overview_v1_' + sha(p_raw)
            save(root / 'manifest.json', m)
    if change == 'unsupported_root':
        m = json.loads((root / 'manifest.json').read_text())
        m['scale'] = {'file': 'unadapted'}
        save(root / 'manifest.json', m)
    binding = rebind(root, binding)
    if change == 'unsupported_root':
        with pytest.raises(ValueError):
            freeze_collection(binding, tmp_path / 'freeze')
        return
    manifest = freeze_collection(binding, tmp_path / 'freeze')
    collection = h.import_collection(manifest)
    with pytest.raises((ValueError, TypeError)):
        h.project_core(collection, root_id=0)
    # 领域失败没有改变原集合结构audit资格。
    with h.collection(collection) as s:
        for _ in s.bulk('files'):
            pass
    assert s.receipt


def test_ordinary_item_duplicate_retained(lake, tmp_path):
    h, _, _, _ = lake
    root = tmp_path / 'source'
    binding, _ = fixture(root, count=1)
    with closing(sqlite3.connect(root / '2026-03-01.sqlite3')) as db, db:
        raw = db.execute('SELECT item FROM records').fetchone()[0]
        db.execute('UPDATE records SET item=?', (raw[:-1] + ',"extra":{"source_note":9007199254740993,"source_note":-0}}',))
    collection = h.import_collection(freeze_collection(rebind(root, binding), tmp_path / 'freeze'))
    token = h.project_core(collection, root_id=0)
    with h.core(token) as s:
        item = s.detail('2026-03-01', 0)['item_exact']
        extra = next(v for k, v in item['members'] if k == 'extra')
        assert extra['members'] == [['source_note', {'kind': 'number', 'lexeme': '9007199254740993'}], ['source_note', {'kind': 'number', 'lexeme': '-0'}]]
    assert s.receipt


def test_query_full_comparison_with_current_dailyindex(lake, tmp_path):
    h, _, _, _ = lake
    root = tmp_path / 'source'
    binding, _ = fixture(root, count=17, duplicate=False)
    index = DailyIndex(root / 'manifest.json', (root / 'manifest.json').read_bytes())
    token = h.project_core(h.import_collection(freeze_collection(binding, tmp_path / 'freeze')), root_id=0)
    with h.core(token) as s:
        for values in ({}, {'family': 'ipv4'}, {'family': 'ipv6', 'kind': 'as_outage'}, {'family': 'unknown'},
                       {'level': 'high', 'hour': 8}, {'level': 'unknown'}, {'q': 'AS64496'}, {'sort': 'time'}, {'kind': 'country_outage', 'q': '伊朗'},
                       {'kind': 'sub_hijack', 'q': '192.0.2.0/24'}, {'q': '{64496, 64497}'}):
            q = dict(date='2026-03-01', start=window('2026-03-01')['start'], family='all', kind='all', level='all', hour=None, q='', sort='severity', page=1, page_size=60)
            q.update(values)
            old = index.query({'query': q, 'metadata': {}})
            actual = list(pages(s, **values))[0]
            assert actual['overview'] == old['overview']
            assert actual['trend'] == old['trend']
            assert actual['excluded_unknown_family'] == old['query']['excluded_unknown_family']
            assert actual['events']['total'] == old['events']['total']
            assert actual['events']['distinct_prefixes'] == old['events']['distinct_prefixes']
            assert [r['item'] for r in actual['events']['items']] == old['events']['items']
    assert s.receipt


def test_level_conflict_keeps_original_detail(lake, tmp_path):
    h, _, _, _ = lake
    root = tmp_path / 'source'
    binding, _ = fixture(root, count=7, duplicate=False)
    path = root / '2026-03-01.sqlite3'
    with closing(sqlite3.connect(path)) as db, db:
        original = db.execute("SELECT rowid,item,payload FROM records WHERE kind='as_outage' ORDER BY rowid LIMIT 1").fetchone()
        ordinal, item, payload = original
        item = json.loads(item)
        original_level = item['level']
        conflict = {'event_table': 'event_table_202603', 'event_level': 'high' if original_level != 'high' else 'low',
                    'detail_level': original_level, 'source_input_sha256': sha(b'artificial H1 evidence\n')}
        item['level'] = None
        item['level_conflict'] = conflict
        db.execute("UPDATE records SET item=?,level='conflict',severity=3 WHERE rowid=?", (json.dumps(item), ordinal))
        p = json.loads(db.execute('SELECT manifest FROM provenance').fetchone()[0])
        p['level_conflicts'] = {item['reference']: conflict}
        raw = json.dumps(p).encode()
        db.execute('UPDATE provenance SET manifest=?', (raw,))
    m = json.loads((root / 'manifest.json').read_text())
    m['days']['2026-03-01']['input_version'] = 'overview_v1_' + sha(raw)
    save(root / 'manifest.json', m)
    token = h.project_core(h.import_collection(freeze_collection(rebind(root, binding), tmp_path / 'freeze')), root_id=0)
    with h.core(token) as s:
        page = s.query('2026-03-01', level='conflict')
        assert page['events']['total'] == 1
        row = page['events']['items'][0]
        assert row['item'] == item
        assert s.detail('2026-03-01', row['occurrence'])['record'] == oracle_wire(payload)
    assert s.receipt


def test_cursor_suffix_scope_and_output_mutation(lake):
    from copy import deepcopy
    h, token, _, _ = lake
    with h.core(token) as s:
        first = s.query('2026-03-01', limit=17)
        saved = deepcopy(first['next_cursor'])
        first['overview']['record_count'] = -100
        first['metadata']['source']['code'] = 'bad'
        first['next_cursor']['after'][-1] = 999999
        with pytest.raises(ValueError, match='游标继续'):
            s.query('2026-03-01', limit=17, cursor=first['next_cursor'])
        second = s.query('2026-03-01', limit=17, cursor=saved)
        assert second['overview']['record_count'] == 65 and second['metadata']['source']['code'] == 'r'
    assert s.receipt is None
    with h.core(token) as s:
        cursor = saved
        while cursor:
            result = s.query('2026-03-01', limit=17, cursor=cursor)
            cursor = result['next_cursor']
        assert result['query_scope']['scope'] == 'query_suffix'
        assert s.metadata()['original_index_version'].startswith('overview_index_v2_')
    assert s.receipt and next(iter(s.receipt['query_scopes'].values()))['scope'] == 'query_suffix'


def test_project_aggregate_rows_and_disk_budget(lake):
    from data_pipeline.history.event_collection import CollectionLimits
    h, token, _, _ = lake
    # C.1输入350定位行可读，H1展开642+领域行不能重置500行额度。
    small = History(h.dsn, h.root, target_identity=h.target_identity, limits=replace(h.limits, max_total_rows=500))
    with pytest.raises(ValueError, match='profile_rows'):
        small.project_core(token.collection, root_id=0)
    small = History(h.dsn, h.root, target_identity=h.target_identity, collection_limits=replace(CollectionLimits(), temporary_bytes=65536))
    with pytest.raises(ValueError):
        small.project_core(token.collection, root_id=0)


def test_span_total_bytes_checked_before_open(tmp_path, monkeypatch):
    from data_pipeline.history.event_collection.model import Budget
    from data_pipeline.history.event_index.project import read_span
    path = tmp_path / 'fixture'
    path.write_bytes(b'1234567890')
    budget = Budget(tmp_path, replace(Limits(), max_total_bytes=9))
    actual = Path.open
    calls = []
    def opened(self, *a, **kw):
        if self == path:
            calls.append(True)
        return actual(self, *a, **kw)
    monkeypatch.setattr(Path, 'open', opened)
    with pytest.raises(ValueError, match='profile_source_bytes'):
        read_span(tmp_path, 'fixture', 0, 10, budget)
    assert calls == []


def test_measured_native_costs(lake, monkeypatch):
    if os.environ.get('Q3C2_COST') != '1':
        pytest.skip('实际独占PG日志测量需显式启用')
    import time
    h, token, out, _ = lake
    log = h.root / 'q3-c2-pg.log'
    phases = []
    counter = {'python_sha256_calls': 0, 'python_sha256_bytes': 0}
    actual = hashlib.sha256
    class Hash:
        def __init__(self, data=b'', **kw):
            counter['python_sha256_calls'] += 1
            counter['python_sha256_bytes'] += len(data)
            self.h = actual(data, **kw)
        def update(self, data):
            counter['python_sha256_bytes'] += len(data)
            self.h.update(data)
        def __getattr__(self, k):
            return getattr(self.h, k)
    monkeypatch.setattr(hashlib, 'sha256', Hash)
    def measured(label, action):
        offset = log.stat().st_size
        before = dict(counter)
        start = time.monotonic()
        result = action()
        elapsed = time.monotonic() - start
        with log.open('rb') as stream:
            stream.seek(offset)
            raw = stream.read()
        (out / (label + '.pg.log')).write_bytes(raw)
        phase = {'phase': label, 'wall_seconds': elapsed,
                 'actual_private_pg_statements': sum('statement:' in line or 'execute <unnamed>:' in line for line in raw.decode().splitlines()),
                 **{k: counter[k] - before[k] for k in counter},
                 'result': asdict(result) if isinstance(result, (CollectionToken, CoreToken)) else result}
        phases.append(phase)
        return result
    binding, _ = fixture(out / 'cost-source', count=65)
    manifest = measured('freeze', lambda: str(freeze_collection(binding, out / 'cost-freeze')))
    collection = measured('import', lambda: h.import_collection(manifest))
    token = measured('project', lambda: h.project_core(collection, root_id=0))
    for cap in (1, 17, 1000):
        def read():
            with h.core(token) as s:
                for table in TABLES:
                    for _ in s.bulk(table, batch_rows=cap):
                        pass
            return s.receipt
        measured('bulk-' + str(cap), read)
    assert len({p['actual_private_pg_statements'] for p in phases[3:6]}) == 1
    assert len({p['python_sha256_calls'] for p in phases[3:6]}) == 1
    assert len({p['python_sha256_bytes'] for p in phases[3:6]}) == 1
    for cap in (1, 17, 60):
        def read():
            with h.core(token) as s:
                count = sum(1 for _ in pages(s, limit=cap))
            return {'receipt': s.receipt, 'page_requests': count}
        measured('pages-' + str(cap), read)
    # 集合显式全资格SHA固定；每页另外有小游标身份SHA，不能混称全闭包SHA。
    assert len({p['result']['receipt']['resources']['hash_calls'] for p in phases[6:]}) == 1
    ready = json.loads((h.data_root / token.profile_id / 'ready.json').read_text())
    original = json.loads((h.data_root / token.collection.collection_id / 'source-manifest.json').read_text())
    imported = json.loads((h.data_root / token.collection.collection_id / 'ready.json').read_text())
    save(out / '实际成本.json', {'fixture_records': 65, 'nonempty_days': 1, 'admitted_empty_days': 1, 'failed_days': 4,
                              'token': asdict(token), 'phases': phases, 'projection_resources': ready['resources'],
                              'freeze_resources': original['resources'], 'collection_import_resources': imported['resources'],
                              'collection_parquet_bytes': sum(f['bytes'] for f in imported['files']),
                              'retained_bytes': sum(f['bytes'] for f in imported['artifacts']),
                              'parquet_bytes': sum(f['bytes'] for f in ready['files']),
                              'scope': '仅Python SHA256/显式读块/本任务独占PG实际日志；RSS为进程生命周期且不含PG；页游标哈希另含在全Python计数'})
