"""成功空日须经离线留存和索引后，才能在公开 HTTP 中返回零。"""

import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from data_pipeline.anomaly_records import convert_anomaly_record, serialize_record
from test_core_overview_api import retained  # 复用公开消费包的合成fixture。


ROOT = Path(__file__).resolve().parents[3]
PROFILE = json.loads((ROOT / 'config/data-profile.json').read_text())
URL = '/api/v1/core-overview'


def empty_audit(directory):
    directory.mkdir()
    context = {'kind': 'context', 'database': 'bgp_project', 'source': 'r',
               'window_start': '2026-02-01T00:00:00+08:00',
               'window_end_exclusive': '2026-02-02T00:00:00+08:00',
               'read_at': '2026-09-10T15:00:00+00:00', 'read_only': 'on',
               'isolation': 'repeatable read', 'per_kind': 0}
    end = {'kind': 'receipt', 'finished_at': '2026-09-10T15:00:01+00:00', 'read_only': 'on'}
    raw = [context, {'kind': 'event_counts', 'counts': {'prefix_outage': 0, 'as_outage': 0, 'leak': 0}},
           {'kind': 'prefix_buckets', 'items': []}, end]
    files = {'day.jsonl': b''.join((json.dumps(row) + '\n').encode() for row in raw),
             'day-records.jsonl': b'', 'extract.sql': b'-- fixture only\n',
             'anomaly_records_source.py': (ROOT / 'backend/data_pipeline/anomaly_records.py').read_bytes(),
             'data-profile.json': json.dumps(PROFILE).encode()}
    for name, payload in files.items():
        (directory / name).write_bytes(payload)
    hashes = {name: hashlib.sha256(payload).hexdigest() for name, payload in files.items()}
    audit = {'input_sha256': hashes['day.jsonl'], 'converted_file_sha256': hashes['day-records.jsonl'],
             'mapping_code_sha256': hashes['anomaly_records_source.py'],
             'data_profile_sha256': hashes['data-profile.json'], 'errors': [], 'roundtrip_equal': True,
             'hourly_prefix_reconciliation': {'mismatches': [], 'checked': True,
                 'prefix_rows': 0, 'sum_bucket_distinct': 0, 'window_distinct': 0}, 'records': 0}
    receipt = {'payload_sha256': hashes['day.jsonl'], 'query_sha256': hashes['extract.sql'],
               'event_count': 0, 'source_instance': 'fixture/bgp_project',
               'context': context, 'receipt': end}
    (directory / 'day-audit.json').write_text(json.dumps(audit))
    (directory / 'day-receipt.json').write_text(json.dumps(receipt))
    return raw, audit, receipt


def retain(directory, output):
    return subprocess.run([sys.executable, str(ROOT / 'scripts/retain-core-overview-input.py'),
                           '--audit-dir', str(directory), '--output', str(output)],
                          capture_output=True, text=True, timeout=10)


@pytest.mark.parametrize('duration,accepted', [('-00:00:01', False), ('00:00:01', True)])
def test_nonempty_retention_checks_original_time_before_creating_output(tmp_path, retained, duration, accepted):
    _, manifest, records = retained
    original = records[0]
    record = original['record']
    result = convert_anomaly_record(record['identity']['legacy_reference'],
        [{**record['raw_fields'], 'duration': duration}],
        source={**record['source'], 'read_at': original['read_at']}, data_profile=PROFILE)
    directory = tmp_path / 'nonempty-audit'
    raw, audit, receipt = empty_audit(directory)
    raw[0].update(window_start=manifest['window']['start'], window_end_exclusive=manifest['window']['end_exclusive'])
    raw[1]['counts']['prefix_outage'] = 1
    raw[2]['items'] = [{'bucket': '2026-02-27T00:00:00', 'family': 4, 'records': 1, 'distinct_prefixes': 1}]
    raw.insert(1, {'kind': 'event', 'event': {**result['record']['raw_fields'],
        'detail_url': record['identity']['legacy_reference'], 'event_type': '前缀中断'},
        'candidates': [result['record']['raw_fields']]})
    audit['hourly_prefix_reconciliation'].update(prefix_rows=1, sum_bucket_distinct=1, window_distinct=1)
    payload = serialize_record(result) + b'\n'
    (directory / 'day-records.jsonl').write_bytes(payload)
    original_bytes = b''.join((json.dumps(row) + '\n').encode() for row in raw)
    (directory / 'day.jsonl').write_bytes(original_bytes)
    audit.update(records=1, converted_file_sha256=hashlib.sha256(payload).hexdigest(), input_sha256=hashlib.sha256(original_bytes).hexdigest())
    receipt.update(event_count=1, payload_sha256=audit['input_sha256'])
    (directory / 'day-audit.json').write_text(json.dumps(audit))
    (directory / 'day-receipt.json').write_text(json.dumps(receipt))
    output = tmp_path / 'nonempty-retained'
    run = retain(directory, output)
    assert (run.returncode == 0) is accepted, run.stderr
    assert output.exists() is accepted


def test_verified_empty_day_survives_retention_indexing_and_http(client, tmp_path, monkeypatch):
    audit_dir = tmp_path / 'audit'
    empty_audit(audit_dir)
    retained = tmp_path / 'retained'
    run = retain(audit_dir, retained)
    assert run.returncode == 0, run.stderr
    evidence = json.loads((retained / 'manifest.json').read_text())['evidence']['empty_result']
    assert evidence['basis'] == 'complete-day-zero-source-counts/v1'
    assert evidence['source_counts'] == {'prefix_outage': 0, 'as_outage': 0, 'leak': 0}
    assert evidence['audit_sha256'] == hashlib.sha256((audit_dir / 'day-audit.json').read_bytes()).hexdigest()
    assert evidence['receipt_sha256'] == hashlib.sha256((audit_dir / 'day-receipt.json').read_bytes()).hexdigest()
    indexed = tmp_path / 'indexed'
    run = subprocess.run([sys.executable, str(ROOT / 'scripts/index-core-overview-inputs.py'),
                          '--input', str(retained / 'manifest.json'), '--output', str(indexed)],
                         capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(indexed / 'manifest.json'))
    response = client.get(URL, query_string={'date': '2026-02-01'})
    assert response.status_code == 200
    result = response.get_json()
    assert result['state'] == 'available'
    assert result['metadata']['available_dates'] == ['2026-02-01']
    assert result['metadata']['source']['coverage'] == 'unknown'
    assert result['overview'] == {'record_count': 0, 'visible_prefixes': None, 'visible_origin_ases': None}
    assert result['events'] == {'total': 0, 'items': [], 'distinct_prefixes': 0, 'page': 1, 'page_size': 10, 'page_count': 0}
    assert [bucket['value'] for bucket in result['trend']['buckets']] == [0] * 24
    missing = client.get(URL).get_json()
    assert missing['query']['date'] == '2026-03-31'
    assert missing['state'] == 'window_not_retained'
    assert missing['overview'] is missing['events'] is missing['trend'] is None
    assert retain(audit_dir, retained).returncode != 0


@pytest.mark.parametrize('failure', [
    'missing_receipt', 'nonempty_source', 'sample', 'wrong_context', 'bucket_rows',
    'wrong_audit', 'boolean_count', 'no_timezone', 'backward_receipt', 'partial_day',
    'prefix_rows', 'sum_bucket_distinct', 'window_distinct', 'missing_source', 'blank_source',
])
def test_empty_retention_rejects_unproven_zero_without_creating_output(tmp_path, failure):
    audit_dir = tmp_path / 'audit'
    raw, audit, receipt = empty_audit(audit_dir)
    if failure == 'missing_receipt':
        raw.pop()
    elif failure == 'nonempty_source':
        raw[1]['counts']['leak'] = 1
    elif failure == 'sample':
        raw[0]['per_kind'] = 10
    elif failure == 'wrong_context':
        receipt['context'] = {**receipt['context'], 'source': 'other'}
    elif failure == 'bucket_rows':
        raw[2]['items'] = [{'bucket': '2026-02-01T01:00:00', 'records': 1}]
    elif failure == 'wrong_audit':
        audit['input_sha256'] = '0' * 64
    elif failure == 'boolean_count':
        raw[1]['counts']['leak'] = False
    elif failure == 'no_timezone':
        raw[0]['read_at'] = '2026-09-10T15:00:00'
    elif failure == 'backward_receipt':
        raw[-1]['finished_at'] = '2026-09-09T15:00:00+00:00'
    elif failure == 'partial_day':
        raw[0]['window_start'] = '2026-02-01T12:00:00+08:00'
    elif failure in {'prefix_rows', 'sum_bucket_distinct', 'window_distinct'}:
        audit['hourly_prefix_reconciliation'][failure] = 1
    elif failure == 'missing_source':
        receipt['source_instance'] = None
    elif failure == 'blank_source':
        receipt['source_instance'] = '  '
    payload = b''.join((json.dumps(row) + '\n').encode() for row in raw)
    (audit_dir / 'day.jsonl').write_bytes(payload)
    receipt['payload_sha256'] = hashlib.sha256(payload).hexdigest()
    if failure != 'wrong_audit':
        audit['input_sha256'] = receipt['payload_sha256']
    (audit_dir / 'day-audit.json').write_text(json.dumps(audit))
    (audit_dir / 'day-receipt.json').write_text(json.dumps(receipt))
    output = tmp_path / 'retained'
    assert retain(audit_dir, output).returncode != 0
    assert not output.exists()


def test_empty_retention_accepts_postgresql_variable_fraction_precision(tmp_path):
    audit_dir = tmp_path / 'audit'
    raw, audit, receipt = empty_audit(audit_dir)
    raw[0]['read_at'] = '2026-09-10T15:00:00.72471+00:00'
    raw[-1]['finished_at'] = '2026-09-10T15:00:01.12+00:00'
    payload = b''.join((json.dumps(row) + '\n').encode() for row in raw)
    (audit_dir / 'day.jsonl').write_bytes(payload)
    audit['input_sha256'] = receipt['payload_sha256'] = hashlib.sha256(payload).hexdigest()
    (audit_dir / 'day-audit.json').write_text(json.dumps(audit))
    (audit_dir / 'day-receipt.json').write_text(json.dumps(receipt))
    assert retain(audit_dir, tmp_path / 'retained').returncode == 0
