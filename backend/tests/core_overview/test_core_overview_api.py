"""从公开只读 HTTP 入口验收 C 首页；输入仅为临时合成记录。"""

import hashlib
import json
from pathlib import Path
import subprocess
import sqlite3
import sys

import pytest

from data_pipeline.common.event_records import convert_anomaly_record, serialize_record


PROFILE = json.loads((Path(__file__).resolve().parents[3] / 'config/data-profile.json').read_text())
URL = '/api/v1/core-overview'


@pytest.mark.parametrize('indexed', [False, True])
def test_level_conflict_preserves_record_and_has_separate_filter(client, retained, tmp_path, monkeypatch, indexed):
    path, manifest, records = retained
    before = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    original = records[4]
    record = original['record']
    source = {**record['source'], 'read_at': original['read_at'],
              'evidence_refs': [{'sha256': 'a' * 64, 'kind': 'synthetic_source_extract'}]}
    records[4] = convert_anomaly_record(record['identity']['legacy_reference'],
        [record['raw_fields']], source=source, data_profile=PROFILE)
    payload = b''.join(serialize_record(row) + b'\n' for row in records)
    (path.parent / 'records.jsonl').write_bytes(payload)
    conflict = {'event_table': 'event_table_202602', 'event_level': 'low',
                'detail_level': 'high', 'source_input_sha256': 'a' * 64}
    manifest.update(interpretation_version='recorded-anomaly-overview/v2',
                    level_conflicts={record['identity']['legacy_reference']: conflict})
    manifest['records']['sha256'] = hashlib.sha256(payload).hexdigest()
    path.write_text(json.dumps(manifest))
    if indexed:
        output = tmp_path / 'conflict-index'
        run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                              '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
        assert run.returncode == 0, run.stderr
        monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    response = client.get(URL, query_string={'date': '2026-02-27', 'level': 'conflict'})
    assert response.status_code == 200
    actual = response.get_json()
    assert actual['events']['total'] == 1
    item = actual['events']['items'][0]
    assert item['level'] is None and item['level_conflict'] == conflict
    assert actual['overview'] == before['overview']
    assert actual['trend'] == before['trend']
    detail = client.get(URL + '/record', query_string={'ref': item['reference'], 'version': actual['version']}).get_json()
    assert detail['item'] == item
    assert detail['record'] == json.loads(serialize_record(records[4]))
    assert detail['record']['record']['raw_fields']['outage_level'] == 'high'
    assert client.get(URL, query_string={'date': '2026-02-27', 'level': 'high'}).get_json()['events']['total'] == 2
    assert client.get(URL, query_string={'date': '2026-02-27', 'level': 'unknown'}).get_json()['events']['total'] == 0
    ordered = client.get(URL, query_string={'date': '2026-02-27'}).get_json()['events']['items']
    assert ordered[-1] == item
    # 回到公开原包入口：不接受未绑定证据、错误月表、伪冲突或借注解放宽时间。
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(path))
    for bad in ({**conflict, 'source_input_sha256': 'b' * 64},
                {**conflict, 'event_table': 'event_table_202603'},
                {**conflict, 'event_level': 'high'},
                {**conflict, 'detail_level': 'middle'},
                {**conflict, 'event_level': 'unrecognised'}):
        manifest['level_conflicts'][item['reference']] = bad
        path.write_text(json.dumps(manifest))
        assert client.get(URL, query_string={'date': '2026-02-27'}).status_code == 503
    manifest['level_conflicts'] = {item['reference']: conflict}
    manifest['interpretation_version'] = 'recorded-anomaly-overview/v1'
    path.write_text(json.dumps(manifest))
    assert client.get(URL, query_string={'date': '2026-02-27'}).status_code == 503
    manifest['interpretation_version'] = 'recorded-anomaly-overview/v2'
    broken = convert_anomaly_record(item['reference'],
        [{**record['raw_fields'], 'e_time': '2026-02-26 23:00:00'}], source=source, data_profile=PROFILE)
    payload = serialize_record(broken) + b'\n'
    (path.parent / 'records.jsonl').write_bytes(payload)
    manifest['records'].update(count=1, sha256=hashlib.sha256(payload).hexdigest())
    path.write_text(json.dumps(manifest))
    assert client.get(URL, query_string={'date': '2026-02-27'}).status_code == 503


@pytest.mark.parametrize('end,duration', [
    ('2026-02-26 00:05:00', '-1 days'),
    (None, '-2 days +23:08:33'),
    ('2026-02-27 00:06:00', '00:00:59'),
    ('2026-02-27 00:06:00', '1 mon'),
])
def test_invalid_source_time_is_not_admitted_as_unknown(client, retained, tmp_path, end, duration):
    path, manifest, records = retained
    original = records[0]
    record = original['record']
    broken = convert_anomaly_record(record['identity']['legacy_reference'],
        [{**record['raw_fields'], 'e_time': end, 'duration': duration}],
        source={**record['source'], 'read_at': original['read_at']}, data_profile=PROFILE)
    payload = serialize_record(broken) + b'\n'
    (path.parent / 'records.jsonl').write_bytes(payload)
    manifest['records'].update(count=1, sha256=hashlib.sha256(payload).hexdigest())
    path.write_text(json.dumps(manifest))
    response = client.get(URL, query_string={'date': '2026-02-27'})
    assert response.status_code == 503
    assert response.get_json()['state'] == 'unavailable'
    output = tmp_path / 'invalid-index'
    command = [sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
               '--input', str(path), '--output', str(output)]
    assert subprocess.run(command, capture_output=True, timeout=10).returncode != 0
    assert not (output / 'manifest.json').exists()


def test_offline_daily_index_serves_same_population_and_original_detail(client, retained, tmp_path, monkeypatch):
    path, _, _ = retained
    expected = client.get(URL, query_string={'date': '2026-02-27', 'page_size': '2'}).get_json()
    root = Path(__file__).resolve().parents[3]
    output = tmp_path / 'indexed'
    command = [sys.executable, str(root / 'scripts/core_overview/index-core-overview-inputs.py'),
               '--input', str(path), '--output', str(output)]
    run = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    actual = client.get(URL, query_string={'date': '2026-02-27', 'page_size': '2'}).get_json()
    assert actual['state'] == 'available'
    for field in ('overview', 'trend', 'events', 'query'):
        assert actual[field] == expected[field]
    assert actual['version'] != expected['version']
    assert actual['metadata']['available_dates'] == ['2026-02-27']
    item = actual['events']['items'][0]
    detail = client.get(URL + '/record', query_string={'ref': item['reference'], 'version': actual['version']})
    assert detail.status_code == 200
    assert detail.get_json()['item'] == item
    assert detail.get_json()['record']['content_version'] == item['content_version']
    assert client.get(URL).get_json()['state'] == 'window_not_retained'
    assert subprocess.run(command, capture_output=True, timeout=10).returncode != 0


def test_large_day_is_compiled_offline_and_web_only_reads_the_index(client, retained, tmp_path, monkeypatch):
    path, manifest, records = retained
    original = records[0]
    record = original['record']
    source = {**record['source'], 'read_at': original['read_at']}
    payload_path = path.parent / 'records.jsonl'
    digest = hashlib.sha256()
    with payload_path.open('wb') as stream:
        for number in range(65):
            reference = record['identity']['legacy_reference'].split('/')
            reference[3] = str(number)
            result = convert_anomaly_record('/'.join(reference),
                [{**record['raw_fields'], 'outage_id': number, 'event_info': 'x' * (512 * 1024)}],
                source=source, data_profile=PROFILE)
            encoded = serialize_record(result) + b'\n'
            stream.write(encoded)
            digest.update(encoded)
    assert payload_path.stat().st_size > 64 * 1024 * 1024
    manifest['records'].update(count=65, sha256=digest.hexdigest())
    path.write_text(json.dumps(manifest))
    # Web原包读取仍拒绝超出既有64MiB边界；更大输入仅由显式离线命令消费。
    assert client.get(URL, query_string={'date':'2026-02-27'}).status_code == 503
    output = tmp_path / 'large-index'
    command = [sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
               '--input', str(path), '--output', str(output)]
    run = subprocess.run(command, capture_output=True, text=True, timeout=60)
    assert run.returncode == 0, run.stderr
    payload_path.rename(path.parent / 'not-on-web-path.jsonl')
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    response = client.get(URL, query_string={'date':'2026-02-27','page_size':100})
    assert response.status_code == 200
    data = response.get_json()
    assert data['events']['total'] == len(data['events']['items']) == 65
    item = data['events']['items'][0]
    detail = client.get(URL + '/record', query_string={'ref':item['reference'],'version':data['version']})
    assert detail.status_code == 200
    assert detail.get_json()['record']['record']['raw_fields']['event_info'] == 'x' * (512 * 1024)


@pytest.mark.parametrize('failure', ['digest', 'count', 'missing_conflict_reference', 'last_record_duplicate', 'too_large'])
def test_streamed_input_failure_never_leaves_a_published_catalog(retained, tmp_path, failure):
    path, manifest, records = retained
    payload = path.parent / 'records.jsonl'
    if failure == 'digest':
        manifest['records']['sha256'] = '0' * 64
    elif failure == 'count':
        manifest['records']['count'] += 1
    elif failure == 'missing_conflict_reference':
        manifest.update(interpretation_version='recorded-anomaly-overview/v2', level_conflicts={'missing': {}})
    elif failure == 'last_record_duplicate':
        with payload.open('ab') as stream:
            stream.write(serialize_record(records[0]) + b'\n')
        manifest['records'].update(count=len(records) + 1, sha256=hashlib.sha256(payload.read_bytes()).hexdigest())
    else:
        with payload.open('r+b') as stream:
            stream.truncate(2 * 1024 * 1024 * 1024 + 1)  # 临时稀疏fixture，不读取2GiB正文。
    path.write_text(json.dumps(manifest))
    output = tmp_path / 'rejected-index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode != 0
    assert not (output / 'manifest.json').exists()


def test_daily_index_pins_non_contiguous_days_without_filling_gaps(client, retained, tmp_path, monkeypatch):
    path, manifest, records = retained
    target = tmp_path / 'march'
    target.mkdir()
    scope = {'source': 'r', 'start': '2026-03-31T00:00:00+08:00', 'end_exclusive': '2026-04-01T00:00:00+08:00'}
    original = records[0]['record']
    raw = {**original['raw_fields'], 's_time': '2026-03-31 00:05:00'}
    ref = original['identity']['legacy_reference'].replace('2026-02-27', '2026-03-31')
    source = {**original['source'], 'table': 'prefix_outage_202603', 'read_scope': scope, 'read_at': records[0]['read_at']}
    result = convert_anomaly_record(ref, [raw], source=source, data_profile=PROFILE)
    payload = serialize_record(result) + b'\n'
    (target / 'records.jsonl').write_bytes(payload)
    second = {**manifest, 'window': scope, 'records': {'file': 'records.jsonl', 'sha256': hashlib.sha256(payload).hexdigest(), 'count': 1}}
    (target / 'manifest.json').write_text(json.dumps(second))
    output = tmp_path / 'index'
    command = [sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
               '--input', str(path), '--input', str(target / 'manifest.json'), '--output', str(output)]
    run = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    default = client.get(URL).get_json()
    assert default['query']['date'] == '2026-03-31'
    assert default['events']['total'] == 1
    assert default['metadata']['available_dates'] == ['2026-02-27', '2026-03-31']
    missing = client.get(URL, query_string={'date': '2026-03-01', 'version': default['version']}).get_json()
    assert missing['state'] == 'window_not_retained'
    assert missing['overview'] is missing['trend'] is missing['events'] is None
    empty = client.get(URL, query_string={'date': '2026-03-31', 'q': 'not-a-match'}).get_json()
    assert empty['state'] == 'available' and empty['events']['total'] == 0
    assert empty['overview']['record_count'] == 1
    older = client.get(URL, query_string={'date': '2026-02-27', 'version': default['version']}).get_json()
    assert older['events']['total'] == 7
    assert older['version'] == default['version']
    # 一日损坏不能伪装为空；也不能把其余独立日一起掩盖。
    broken = output / '2026-03-31.sqlite3'
    broken.chmod(0o600)
    broken.write_bytes(b'corrupt')
    failure = client.get(URL)
    assert failure.status_code == 503
    assert failure.get_json()['state'] == 'unavailable'
    assert failure.get_json()['metadata']['available_dates'] == ['2026-02-27', '2026-03-31']
    assert failure.get_json()['overview'] is failure.get_json()['events'] is None
    assert client.get(URL, query_string={'date': '2026-02-27'}).get_json()['events']['total'] == 7


def test_daily_index_rejects_catalog_count_not_matching_retained_provenance(client, retained, tmp_path, monkeypatch):
    path, _, _ = retained
    output = tmp_path / 'index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    target = output / 'manifest.json'
    catalog = json.loads(target.read_bytes())
    catalog['days']['2026-02-27']['count'] = 100
    target.chmod(0o600)
    target.write_text(json.dumps(catalog))
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(target))
    assert client.get(URL, query_string={'date': '2026-02-27'}).status_code == 503


@pytest.mark.parametrize('query', [
    {'family': 'ipv4'}, {'family': 'ipv6'}, {'family': 'unknown'},
    {'kind': 'prefix_outage', 'hour': '0'}, {'q': 'AS64512', 'level': 'low', 'page_size': '1', 'page': '2'},
    {'sort': 'time'}, {'q': '%'}, {'page': '100'}, {'kind': 'as_outage'},
])
def test_daily_index_preserves_existing_filter_sort_and_bucket_semantics(client, retained, tmp_path, monkeypatch, query):
    path, _, _ = retained
    parameters = {'date': '2026-02-27', **query}
    expected = client.get(URL, query_string=parameters).get_json()
    output = tmp_path / 'index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    actual = client.get(URL, query_string=parameters).get_json()
    for field in ('query', 'events', 'trend', 'overview'):
        assert actual[field] == expected[field]


def test_index_refuses_wal_content_outside_the_bound_file(client, retained, tmp_path, monkeypatch):
    path, _, _ = retained
    output = tmp_path / 'index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    database = output / '2026-02-27.sqlite3'
    database.chmod(0o600)
    connection = sqlite3.connect(database)
    try:
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        catalog_path = output / 'manifest.json'
        catalog = json.loads(catalog_path.read_bytes())
        catalog['days']['2026-02-27']['sha256'] = hashlib.sha256(database.read_bytes()).hexdigest()
        catalog_path.chmod(0o600)
        catalog_path.write_text(json.dumps(catalog))
        monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(catalog_path))
        connection.execute("UPDATE records SET level='unknown'")
        connection.commit()
        response = client.get(URL, query_string={'date': '2026-02-27'})
        assert response.status_code == 503
        assert response.get_json()['events'] is None
    finally:
        connection.close()


def test_business_day_identity_does_not_depend_on_window_offset_spelling(client, retained, tmp_path, monkeypatch):
    path, manifest, records = retained
    scope = {'source': 'r', 'start': '2026-02-26T16:00:00+00:00', 'end_exclusive': '2026-02-27T16:00:00+00:00'}
    originals = []
    for result in records:
        record = result['record']
        source = {**record['source'], 'table': record['association']['locators'][0]['source_table'],
                  'read_scope': scope, 'read_at': result['read_at']}
        originals.append(convert_anomaly_record(record['identity']['legacy_reference'], [record['raw_fields']], source=source, data_profile=PROFILE))
    payload = b'\n'.join(serialize_record(result) for result in originals) + b'\n'
    (path.parent / 'records.jsonl').write_bytes(payload)
    manifest.update(window=scope, records={'file': 'records.jsonl', 'sha256': hashlib.sha256(payload).hexdigest(), 'count': 7})
    path.write_text(json.dumps(manifest))
    old = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    assert old['metadata']['available_dates'] == ['2026-02-27']
    output = tmp_path / 'index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    new = client.get(URL, query_string={'date': '2026-02-27'})
    assert new.status_code == 200
    assert new.get_json()['events'] == old['events']


@pytest.fixture()
def retained(monkeypatch, tmp_path):
    scope = {'source': 'r', 'start': '2026-02-27T00:00:00+08:00', 'end_exclusive': '2026-02-28T00:00:00+08:00'}
    rows = [
        ('prefix_outage', '00:05:00', '192.0.2.0/24', 12, 'low'),
        ('prefix_outage', '00:10:00', '192.0.2.0/24', 13, 'low'),
        ('prefix_outage', '01:05:00', '192.0.2.0/24', 14, 'middle'),
        ('prefix_outage', '00:20:00', '2001:db8::/48', 2, 'high'),
        ('as_outage', '00:30:00', '64512', 1, 'high'),
        ('as_outage', '01:10:00', '64513', 1, 'low'),
        ('leak', '00:40:00', '198.51.100.0/24', 1, 'high'),
    ]
    records = []
    for kind, clock, target, number, level in rows:
        start = f'2026-02-27 {clock}'
        ref = f'{kind}/{start}/{target.replace("/", "-")}/{number}/r'
        leak = kind == 'leak'
        row = {'source': 'r', 's_time': start, 'e_time': None, 'duration': '00:00:01',
               'leak_level' if leak else 'outage_level': level,
               'leak_event_id' if leak else 'outage_id': number}
        if kind == 'as_outage':
            row.update(asn=target, outage_prefixes=['192.0.2.0/24', '2001:db8::/48'] if target == '64512' else None)
        else:
            row.update(prefix=target, asn='64512')
        source = {'instance': 'fixture/bgp_project', 'table': f'{"leak_event" if leak else kind}_202602',
                  'representation': 'source_rows', 'read_at': '2026-09-10T12:00:00Z', 'read_scope': scope}
        records.append(convert_anomaly_record(ref, [row], source=source, data_profile=PROFILE))
    data = b'\n'.join(serialize_record(record) for record in records) + b'\n'
    (tmp_path / 'records.jsonl').write_bytes(data)
    manifest = {
        'schema_version': 'core-overview-input/v1',
        'interpretation_version': 'recorded-anomaly-overview/v1',
        'data_profile': PROFILE,
        'source': {'instance': 'fixture/bgp_project', 'code': 'r', 'collector_id': 'rrc25',
                   'collector_basis': 'user_confirmation', 'confirmed_on': '2026-09-10',
                   'coverage': 'unknown', 'detector_version': None},
        'window': scope,
        'kinds': ['prefix_outage', 'as_outage', 'leak'],
        'records': {'file': 'records.jsonl', 'sha256': hashlib.sha256(data).hexdigest(), 'count': len(records)},
        'evidence': {'fixture': True},
    }
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(manifest))
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(path))
    return path, manifest, records


@pytest.fixture()
def retained_with_hijack(retained):
    path, manifest, records = retained
    row = {'source': 'r', 'prefix': '203.0.113.0/24', 'hijack_eventid': 8,
           's_time': '2026-02-27 00:45:00', 'e_time': None, 'duration': None,
           'hijacked_as': '64520', 'hijacker_as': '64521', 'hijack_level': 'middle',
           'is_hijack': True, 'filter_reason': 'possible hijack', 'end_as': None,
           'pre_vp_paths': {'2026-02-26 23:50:00': ['64500 64520']},
           'eve_vp_paths': {}, 'next_vp_paths': None}
    source = {'instance': manifest['source']['instance'], 'table': 'hijack_202602',
              'representation': 'source_rows', 'read_at': '2026-09-11T00:00:00Z',
              'read_scope': manifest['window']}
    result = convert_anomaly_record('hijack/2026-02-27 00:45:00/203.0.113.0-24/8/r',
                                    [row], source=source, data_profile=PROFILE)
    payload = b''.join(serialize_record(record) + b'\n' for record in [*records, result])
    (path.parent / 'records.jsonl').write_bytes(payload)
    manifest['kinds'].append('hijack')
    manifest['records'].update(count=8, sha256=hashlib.sha256(payload).hexdigest())
    path.write_text(json.dumps(manifest))
    return path, manifest, [*records, result]


@pytest.fixture()
def retained_with_sub_hijack(retained_with_hijack):
    path, manifest, records = retained_with_hijack
    row = {'source': 'r', 'prefix': '192.0.2.0/25', 'sub_hijack_eventid': 9,
           'hijacked_prefix': '192.0.2.0/24', 's_time': '2026-02-27 00:50:00',
           'e_time': None, 'duration': None, 'sub_hijack_level': 'middle',
           'hijacked_as': "['64530']", 'hijacker_as': "['64531']",
           'is_sub_hijack': True, 'filter_reason': 'possible hijack', 'level_info': '合成等级说明'}
    source = {'instance': manifest['source']['instance'], 'table': 'sub_hijack_202602',
              'representation': 'source_rows', 'read_at': '2026-09-11T03:00:00Z',
              'read_scope': manifest['window']}
    result = convert_anomaly_record('sub_hijack/2026-02-27 00:50:00/192.0.2.0-25/9/r',
                                    [row], source=source, data_profile=PROFILE)
    payload = b''.join(serialize_record(record) + b'\n' for record in [*records, result])
    (path.parent / 'records.jsonl').write_bytes(payload)
    manifest['kinds'].append('sub_hijack')
    manifest['records'].update(count=9, sha256=hashlib.sha256(payload).hexdigest())
    path.write_text(json.dumps(manifest))
    return path, manifest, [*records, result]


@pytest.fixture()
def retained_with_country_outage(retained_with_sub_hijack):
    path, manifest, records = retained_with_sub_hijack
    row = {'source': 'r', 'country': 'ZZ', 'outage_id': 7,
           'country_chinese_name': '合成国家', 's_time': '2026-02-27 00:55:00',
           'e_time': None, 'duration': None, 'outage_level': 'middle',
           'outage_level_descr': '合成等级说明', 'max_outage_as_ratio': 0.3,
           'max_outage_as_num': 2, 'total_as_num': 10, 'outage_ases': ['64540', '64541'],
           'event_info': '北京时间 2026-02-28 08:00:00 的说明，不作为事件起点'}
    source = {'instance': manifest['source']['instance'], 'table': 'country_outage_202602',
              'representation': 'source_rows', 'read_at': '2026-09-11T05:00:00Z',
              'read_scope': manifest['window']}
    result = convert_anomaly_record('country_outage/2026-02-27 00:55:00/ZZ/7/r',
                                   [row], source=source, data_profile=PROFILE)
    payload = b''.join(serialize_record(record) + b'\n' for record in [*records, result])
    (path.parent / 'records.jsonl').write_bytes(payload)
    manifest['kinds'].append('country_outage')
    manifest['records'].update(count=10, sha256=hashlib.sha256(payload).hexdigest())
    path.write_text(json.dumps(manifest))
    return path, manifest, [*records, result]


def test_country_http_preserves_source_name_asns_and_same_version_detail(client, retained_with_country_outage):
    _, _, records = retained_with_country_outage
    response = client.get(URL, query_string={'date': '2026-02-27', 'kind': 'country_outage', 'q': 'AS64541'})
    assert response.status_code == 200
    data = response.get_json()
    assert data['metadata']['kinds'] == ['prefix_outage', 'as_outage', 'leak', 'hijack', 'sub_hijack', 'country_outage']
    assert data['overview']['record_count'] == 10
    assert [bucket['value'] for bucket in data['trend']['buckets']] == [2, 1] + [0] * 22
    assert data['events']['total'] == 1
    item = data['events']['items'][0]
    assert item['object'] == 'ZZ' and item['country_name'] == '合成国家'
    assert item['asns'] == ['64540', '64541'] and item['address_family'] == 'unknown'
    assert item['start_time'] == '2026-02-26T16:55:00Z'
    assert item['end_time']['state'] == 'unknown'
    detail = client.get(URL + '/record', query_string={'ref': item['reference'], 'version': data['version']})
    assert detail.status_code == 200
    assert detail.get_json()['record'] == json.loads(serialize_record(records[-1]))
    assert len(detail.get_json()['record']['record']['raw_fields']) == 14


def test_country_name_and_recorded_asn_search_survive_daily_index(
    client, retained_with_country_outage, tmp_path, monkeypatch,
):
    path, _, _ = retained_with_country_outage
    query = {'date': '2026-02-27', 'kind': 'country_outage', 'q': '合成国家'}
    response = client.get(URL, query_string=query)
    assert response.status_code == 200 and response.get_json()['events']['total'] == 1
    before = response.get_json()
    output = tmp_path / 'six-kind-index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    indexed = client.get(URL, query_string=query)
    assert indexed.status_code == 200
    for field in ('overview', 'trend', 'events', 'query'):
        assert indexed.get_json()[field] == before[field]
    for term in ('ZZ', 'AS64540', 'AS64541'):
        assert client.get(URL, query_string={**query, 'q': term}).get_json()['events']['total'] == 1
    for family in ('ipv4', 'ipv6'):
        data = client.get(URL, query_string={**query, 'family': family}).get_json()
        assert data['events']['total'] == 0
        assert data['query']['excluded_unknown_family'] == 2
    assert client.get(URL, query_string={**query, 'family': 'unknown'}).get_json()['events']['total'] == 1


@pytest.mark.parametrize('change', [
    {'e_time': '2026-02-27 00:54:59'},
    {'duration': '-00:00:01'},
    {'e_time': '2026-02-27 00:56:00', 'duration': '00:00:59'},
    {'duration': 'invalid'},
])
def test_country_time_conflicts_cannot_be_hidden_by_other_kind_filter(
    client, retained_with_country_outage, tmp_path, change,
):
    path, manifest, records = retained_with_country_outage
    original = records[-1]
    record = original['record']
    changed = convert_anomaly_record(record['identity']['legacy_reference'],
        [{**record['raw_fields'], **change}],
        source={**record['source'], 'read_at': original['read_at']}, data_profile=PROFILE)
    payload = b''.join(serialize_record(item) + b'\n' for item in [*records[:-1], changed])
    (path.parent / 'records.jsonl').write_bytes(payload)
    manifest['records']['sha256'] = hashlib.sha256(payload).hexdigest()
    path.write_text(json.dumps(manifest))
    assert client.get(URL, query_string={'date': '2026-02-27', 'kind': 'prefix_outage'}).status_code == 503
    output = tmp_path / 'invalid-six-kind-index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode != 0 and not (output / 'manifest.json').exists()


def test_sub_hijack_http_retains_parent_roles_and_same_version_detail(client, retained_with_sub_hijack):
    _, _, records = retained_with_sub_hijack
    response = client.get(URL, query_string={'date': '2026-02-27', 'kind': 'sub_hijack', 'q': 'AS64531'})
    assert response.status_code == 200
    data = response.get_json()
    assert data['metadata']['kinds'] == ['prefix_outage', 'as_outage', 'leak', 'hijack', 'sub_hijack']
    assert data['overview']['record_count'] == 9
    assert [bucket['value'] for bucket in data['trend']['buckets']] == [2, 1] + [0] * 22
    assert data['events']['total'] == 1
    item = data['events']['items'][0]
    assert item['object'] == '192.0.2.0/25' and item['parent_prefix'] == '192.0.2.0/24'
    assert item['asns'] == ['64530', '64531'] and item['address_family'] == 'ipv4'
    assert item['end_time']['state'] == 'unknown'
    detail = client.get(URL + '/record', query_string={'ref': item['reference'], 'version': data['version']})
    assert detail.status_code == 200
    assert detail.get_json()['record'] == json.loads(serialize_record(records[-1]))
    assert not any('path' in field for field in detail.get_json()['record']['record']['details'])


def test_sub_hijack_parent_prefix_search_is_preserved_by_daily_index(
    client, retained_with_sub_hijack, tmp_path, monkeypatch,
):
    path, _, _ = retained_with_sub_hijack
    query = {'date': '2026-02-27', 'kind': 'sub_hijack', 'q': '192.0.2.0/24'}
    response = client.get(URL, query_string=query)
    assert response.status_code == 200 and response.get_json()['events']['total'] == 1
    before = response.get_json()
    output = tmp_path / 'five-kind-index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    indexed = client.get(URL, query_string=query)
    assert indexed.status_code == 200
    for field in ('overview', 'trend', 'events', 'query'):
        assert indexed.get_json()[field] == before[field]
    assert client.get(URL, query_string={**query, 'q': 'AS64530'}).get_json()['events']['total'] == 1
    assert client.get(URL, query_string={**query, 'family': 'ipv6'}).get_json()['events']['total'] == 0


@pytest.mark.parametrize('change', [
    {'e_time': '2026-02-27 00:49:59'},
    {'duration': '-00:00:01'},
    {'e_time': '2026-02-27 00:51:00', 'duration': '00:00:59'},
    {'duration': 'invalid'},
    {'hijacked_prefix': '198.51.100.0/24'},
    {'hijacked_prefix': '192.0.2.0/25'},
    {'hijacked_prefix': '2001:db8::/32'},
])
def test_sub_hijack_time_or_parent_conflict_cannot_enter_http_or_index(
    client, retained_with_sub_hijack, tmp_path, change,
):
    path, manifest, records = retained_with_sub_hijack
    original = records[-1]
    record = original['record']
    changed = convert_anomaly_record(record['identity']['legacy_reference'],
        [{**record['raw_fields'], **change}],
        source={**record['source'], 'read_at': original['read_at']}, data_profile=PROFILE)
    payload = b''.join(serialize_record(item) + b'\n' for item in [*records[:-1], changed])
    (path.parent / 'records.jsonl').write_bytes(payload)
    manifest['records']['sha256'] = hashlib.sha256(payload).hexdigest()
    path.write_text(json.dumps(manifest))
    # 整日失败，不允许筛选其他已接入类型绕过新类型中的源字段问题。
    response = client.get(URL, query_string={'date': '2026-02-27', 'kind': 'prefix_outage'})
    assert response.status_code == 503
    output = tmp_path / 'invalid-five-kind-index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode != 0 and not (output / 'manifest.json').exists()


def test_four_kind_input_lists_hijack_and_preserves_same_version_detail(client, retained_with_hijack):
    _, _, records = retained_with_hijack
    response = client.get(URL, query_string={'date': '2026-02-27', 'kind': 'hijack', 'q': 'AS64521'})
    assert response.status_code == 200
    data = response.get_json()
    assert data['metadata']['kinds'] == ['prefix_outage', 'as_outage', 'leak', 'hijack']
    assert data['overview']['record_count'] == 8
    assert [bucket['value'] for bucket in data['trend']['buckets']] == [2, 1] + [0] * 22
    assert data['events']['total'] == 1
    item = data['events']['items'][0]
    assert item['kind'] == 'hijack' and item['object'] == '203.0.113.0/24'
    assert item['asns'] == ['64520', '64521'] and item['address_family'] == 'ipv4'
    assert item['end_time']['state'] == 'unknown'
    detail = client.get(URL + '/record', query_string={'ref': item['reference'], 'version': data['version']})
    assert detail.status_code == 200
    assert detail.get_json()['record'] == json.loads(serialize_record(records[-1]))
    assert detail.get_json()['record']['record']['common']['duration']['state'] == 'unknown'


def test_offline_index_serves_four_kind_input_without_changing_population(
    client, retained_with_hijack, tmp_path, monkeypatch,
):
    path, _, records = retained_with_hijack
    query = {'date': '2026-02-27', 'kind': 'hijack', 'q': 'AS64520'}
    expected = client.get(URL, query_string=query).get_json()
    output = tmp_path / 'four-kind-index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    response = client.get(URL, query_string=query)
    assert response.status_code == 200
    actual = response.get_json()
    for field in ('overview', 'trend', 'events', 'query'):
        assert actual[field] == expected[field]
    assert actual['events']['total'] == 1
    detail = client.get(URL + '/record', query_string={
        'ref': records[-1]['record']['identity']['legacy_reference'], 'version': actual['version']})
    assert detail.status_code == 200
    assert detail.get_json()['record'] == json.loads(serialize_record(records[-1]))


@pytest.mark.parametrize('end,duration', [
    ('2026-02-26 23:59:00', None), (None, '-00:00:01'),
    ('2026-02-27 00:46:00', '00:00:59'), ('invalid', None),
])
def test_hijack_source_time_conflict_blocks_http_and_offline_index(
    client, retained_with_hijack, tmp_path, end, duration,
):
    path, manifest, records = retained_with_hijack
    original = records[-1]
    record = original['record']
    invalid = convert_anomaly_record(record['identity']['legacy_reference'],
        [{**record['raw_fields'], 'e_time': end, 'duration': duration}],
        source={**record['source'], 'read_at': original['read_at']}, data_profile=PROFILE)
    payload = b''.join(serialize_record(value) + b'\n' for value in [*records[:-1], invalid])
    (path.parent / 'records.jsonl').write_bytes(payload)
    manifest['records']['sha256'] = hashlib.sha256(payload).hexdigest()
    path.write_text(json.dumps(manifest))
    response = client.get(URL, query_string={'date': '2026-02-27'})
    assert response.status_code == 503
    output = tmp_path / 'invalid-four-kind-index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/core_overview/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode != 0 and not (output / 'manifest.json').exists()


@pytest.mark.parametrize('end,duration', [
    (None, None), ('2026-03-02 00:45:00', '3 days 00:00:00'),
])
def test_hijack_end_is_preserved_as_recorded_or_unknown_not_live_status(
    client, retained_with_hijack, end, duration,
):
    path, manifest, records = retained_with_hijack
    original = records[-1]
    record = original['record']
    value = convert_anomaly_record(record['identity']['legacy_reference'],
        [{**record['raw_fields'], 'e_time': end, 'duration': duration}],
        source={**record['source'], 'read_at': original['read_at']}, data_profile=PROFILE)
    payload = b''.join(serialize_record(item) + b'\n' for item in [*records[:-1], value])
    (path.parent / 'records.jsonl').write_bytes(payload)
    manifest['records']['sha256'] = hashlib.sha256(payload).hexdigest()
    path.write_text(json.dumps(manifest))
    response = client.get(URL, query_string={'date': '2026-02-27', 'kind': 'hijack'})
    assert response.status_code == 200
    item = response.get_json()['events']['items'][0]
    assert item['end_time']['state'] == ('recorded' if end else 'unknown')
    assert item['end_time']['value'] == ('2026-03-01T16:45:00Z' if end else None)


def test_four_kind_input_cannot_hide_hijack_under_a_three_kind_declaration(client, retained_with_hijack):
    path, manifest, _ = retained_with_hijack
    manifest['kinds'].remove('hijack')
    path.write_text(json.dumps(manifest))
    assert client.get(URL, query_string={'date': '2026-02-27'}).status_code == 503


@pytest.mark.parametrize('family,query,total', [('ipv4', 'AS64520', 1), ('ipv6', '', 0), ('all', 'AS64521', 1)])
def test_hijack_search_and_address_family_are_scoped_to_retained_records(
    client, retained_with_hijack, family, query, total,
):
    response = client.get(URL, query_string={'date': '2026-02-27', 'kind': 'hijack', 'family': family, 'q': query})
    assert response.status_code == 200
    assert response.get_json()['events']['total'] == total


def test_home_lists_retained_records_and_preserves_unknowns(client, retained):
    response = client.get(URL, query_string={'date': '2026-02-27'})
    assert response.status_code == 200
    data = response.get_json()
    assert data['state'] == 'available'
    assert data['metadata']['source']['collector_id'] == 'rrc25'
    assert data['metadata']['source']['coverage'] == 'unknown'
    assert data['metadata']['source']['detector_version'] is None
    assert data['overview'] == {'record_count': 7, 'visible_prefixes': None, 'visible_origin_ases': None}
    assert data['events']['total'] == 7
    assert data['events']['items'][0]['kind'] == 'leak'
    assert data['events']['items'][0]['end_time'] == {'state': 'unavailable', 'value': None}


def test_default_snapshot_day_is_unavailable_not_silently_replaced(client, retained):
    data = client.get(URL).get_json()
    assert data['state'] == 'window_not_retained'
    assert data['query']['date'] == '2026-03-31'
    assert data['overview'] is None
    assert data['trend'] is None
    assert data['events'] is None
    assert data['metadata']['retained_window']['start'] == '2026-02-27T00:00:00+08:00'


def test_hourly_distinct_prefixes_reconcile_with_list_not_row_count(client, retained):
    data = client.get(URL, query_string={'date': '2026-02-27', 'hour': '0', 'kind': 'prefix_outage'}).get_json()
    assert data['trend']['metric'] == 'recorded_prefix_outage_starts_distinct'
    assert data['trend']['bucket_seconds'] == 3600
    assert [point['value'] for point in data['trend']['buckets']] == [2, 1] + [0] * 22
    assert data['events']['total'] == 3
    assert data['events']['distinct_prefixes'] == 2
    assert data['overview']['record_count'] == 7
    assert {item['reference'].split('/')[3] for item in data['events']['items']} == {'2', '12', '13'}


@pytest.mark.parametrize('family,total,prefix_count,excluded', [('all', 7, 2, 0), ('ipv4', 5, 1, 1), ('ipv6', 2, 1, 1), ('unknown', 1, 0, 0)])
def test_family_membership_keeps_mixed_and_unknown_explicit(client, retained, family, total, prefix_count, excluded):
    data = client.get(URL, query_string={'date': '2026-02-27', 'family': family}).get_json()
    assert data['events']['total'] == total
    assert data['trend']['buckets'][0]['value'] == prefix_count
    assert data['query']['excluded_unknown_family'] == excluded
    if family in ('ipv4', 'ipv6'):
        as_event = next(item for item in data['events']['items'] if item['kind'] == 'as_outage')
        assert as_event['address_family'] == 'mixed'
    if family == 'unknown':
        assert data['events']['items'][0]['address_family'] == 'unknown'


def test_search_level_sort_and_pagination_share_one_filtered_population(client, retained):
    query = {'date': '2026-02-27', 'q': 'AS64512', 'kind': 'prefix_outage', 'level': 'low', 'page_size': '1'}
    first = client.get(URL, query_string=query).get_json()
    second = client.get(URL, query_string={**query, 'page': '2', 'version': first['version']}).get_json()
    assert first['events']['total'] == second['events']['total'] == 2
    assert first['events']['distinct_prefixes'] == 1
    assert first['events']['items'][0]['record_number'] == '13'
    assert second['events']['items'][0]['record_number'] == '12'
    assert first['events']['page_count'] == 2
    assert first['overview']['record_count'] == 7
    by_time = client.get(URL, query_string={'date': '2026-02-27', 'sort': 'time'}).get_json()
    assert by_time['events']['items'][0]['object'] == '64513'


def test_detail_keeps_original_key_and_pinned_record_version(client, retained):
    listing = client.get(URL, query_string={'date': '2026-02-27', 'kind': 'prefix_outage'}).get_json()
    item = listing['events']['items'][0]
    response = client.get(URL + '/record', query_string={'ref': item['reference'], 'version': listing['version']})
    assert response.status_code == 200
    detail = response.get_json()
    assert detail['item'] == item
    assert detail['version'] == listing['version']
    assert detail['record']['content_version'] == item['content_version']
    assert detail['record']['record']['association']['locators'][0]['key']['asn'] == '64512'
    assert detail['record']['record']['source']['collector_id'] is None
    assert detail['metadata']['source']['collector_basis'] == 'user_confirmation'
    assert client.get(URL + '/record', query_string={'ref': item['reference'], 'version': 'other'}).status_code == 409
    assert client.get(URL, query_string={'date': '2026-02-27', 'version': 'other'}).status_code == 409
    assert client.get(URL + '/record', query_string={'ref': item['reference']}).status_code == 400


@pytest.mark.parametrize('change', ['source', 'collector', 'coverage', 'kinds'])
def test_unbound_source_declarations_fail_closed(client, retained, change):
    path, manifest, _ = retained
    if change == 'source':
        manifest['source']['code'] = 'other'
    elif change == 'collector':
        manifest['source']['collector_id'] = 'rrc00'
    elif change == 'coverage':
        manifest['source']['coverage'] = 'complete'
    else:
        manifest['kinds'].append('country_outage')
    path.write_text(json.dumps(manifest))
    response = client.get(URL, query_string={'date': '2026-02-27'})
    assert response.status_code == 503
    assert response.get_json()['state'] == 'unavailable'


@pytest.mark.parametrize('query', [
    {'date': '2026-02-30'}, {'date': '2026-04-01'}, {'family': 'v7'},
    {'kind': 'country_outage'}, {'hour': '24'}, {'level': 'critical'},
    {'sort': 'random'}, {'page': '0'}, {'page_size': '101'}, {'q': 'a' * 121},
    {'collector': 'rrc00'}, [('date', '2026-02-27'), ('date', '2026-03-31')],
])
def test_invalid_or_silently_ignored_filters_are_rejected(client, retained, query):
    assert client.get(URL, query_string=query).status_code == 400


def test_integrity_failure_is_not_empty_success_or_cached_success(client, retained, monkeypatch):
    def no_database(*args, **kwargs):
        raise AssertionError('首页留存查询不得访问数据库')
    monkeypatch.setattr('psycopg2.connect', no_database)
    assert client.get(URL, query_string={'date': '2026-02-27'}).status_code == 200
    path, manifest, _ = retained
    (path.parent / manifest['records']['file']).write_bytes(b'corrupt')
    response = client.get(URL, query_string={'date': '2026-02-27'})
    assert response.status_code == 503
    assert 'events' not in response.get_json()


def test_response_shapes_match_generated_type_source_contract(client, retained):
    import jsonschema
    contract = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_text())
    listing = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    missing = client.get(URL).get_json()
    detail = client.get(URL + '/record', query_string={'ref': listing['events']['items'][0]['reference'], 'version': listing['version']}).get_json()
    for payload, name in [(listing, 'CoreOverviewPayload'), (missing, 'CoreOverviewPayload'), (detail, 'CoreOverviewDetail')]:
        jsonschema.Draft202012Validator({**contract, '$ref': f'#/components/schemas/{name}'}).validate(payload)


@pytest.mark.parametrize('failure', [None, 'checksum', 'read_only'])
def test_offline_retainer_preserves_bytes_and_refuses_invalid_input_or_overwrite(retained, tmp_path, failure):
    path, manifest, records = retained
    audit_dir = tmp_path / 'audit'
    audit_dir.mkdir()
    root = Path(__file__).resolve().parents[3]
    files = {'day.jsonl': b'{}\n', 'extract.sql': b'BEGIN READ ONLY; ROLLBACK;\n',
             'day-records.jsonl': (path.parent / 'records.jsonl').read_bytes(),
             'anomaly_records_source.py': (root / 'backend/data_pipeline/common/event_records.py').read_bytes(),
             'data-profile.json': json.dumps(PROFILE).encode()}
    for name, payload in files.items():
        (audit_dir / name).write_bytes(payload)
    hashes = {name: hashlib.sha256(payload).hexdigest() for name, payload in files.items()}
    audit = {'converted_file_sha256': hashes['day-records.jsonl'],
             'mapping_code_sha256': hashes['anomaly_records_source.py'],
             'data_profile_sha256': hashes['data-profile.json'], 'errors': [], 'roundtrip_equal': True,
             'hourly_prefix_reconciliation': {'mismatches': [], 'checked': True}, 'records': len(records)}
    receipt = {'payload_sha256': hashes['day.jsonl'], 'query_sha256': hashes['extract.sql'],
               'event_count': len(records), 'source_instance': manifest['source']['instance'],
               'context': {'source': 'r', 'window_start': manifest['window']['start'],
                           'window_end_exclusive': manifest['window']['end_exclusive'],
                           'read_at': '2026-09-10T12:00:00Z', 'read_only': 'off' if failure == 'read_only' else 'on'},
               'receipt': {'read_only': 'on'}}
    (audit_dir / 'day-audit.json').write_text(json.dumps(audit))
    (audit_dir / 'day-receipt.json').write_text(json.dumps(receipt))
    if failure == 'checksum':
        (audit_dir / 'day-records.jsonl').write_bytes(b'corrupt')
    output = tmp_path / 'consumption'
    command = [sys.executable, str(root / 'scripts/core_overview/retain-core-overview-input.py'),
               '--audit-dir', str(audit_dir), '--output', str(output)]
    run = subprocess.run(command, capture_output=True, text=True, timeout=10)
    if failure:
        assert run.returncode != 0
        assert not output.exists()
        return
    assert run.returncode == 0, run.stderr
    assert (output / 'records.jsonl').read_bytes() == files['day-records.jsonl']
    result = json.loads(run.stdout)
    manifest_bytes = (output / 'manifest.json').read_bytes()
    assert result['version'] == 'overview_v1_' + hashlib.sha256(manifest_bytes).hexdigest()
    assert (output / 'retainer.py').read_bytes() == (root / 'scripts/core_overview/retain-core-overview-input.py').read_bytes()
    assert subprocess.run(command, capture_output=True, timeout=10).returncode != 0
    assert (output / 'manifest.json').read_bytes() == manifest_bytes
