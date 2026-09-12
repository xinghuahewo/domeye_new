"""失败日诊断经离线命令进入只读HTTP；全部来源数据是临时合成fixture。"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from test_core_overview_api import retained, retained_with_hijack

ROOT = Path(__file__).resolve().parents[3]
URL = '/api/v1/core-overview'


def diagnostic_selection(directory, manifest):
    directory.mkdir()
    context = {'kind': 'context', 'month': '202603', 'anomaly_kind': 'as_outage',
        'source': 'r', 'database': 'bgp_project', 'start': '2026-03-31', 'end_exclusive': '2026-04-01',
        'business_timezone': 'Asia/Shanghai', 'read_at': '2026-09-10T15:00:00.12345+00:00',
        'read_only': 'on', 'isolation': 'repeatable read'}
    ending = {'kind': 'receipt', 'read_only': 'on', 'finished_at': '2026-09-10T15:00:01+00:00'}
    row = {'day': '2026-03-31', 'kind': 'as_outage', 'records': 3, 'distinct_refs': 3,
        'missing_candidates': 0, 'multiple_candidates': 0, 'start_conflicts': 0, 'reference_conflicts': 0,
        'end_conflicts': 0, 'duration_conflicts': 0, 'level_not_recognized': 0,
        'level_conflicts': 2, 'invalid_time_order': 0}
    data = b''.join((json.dumps(item) + '\n').encode() for item in [context, {'kind':'quality_by_day', 'items':[row]}, ending])
    query = (ROOT / 'backend/data_pipeline/diagnostic_queries/quality_by_day.sql').read_bytes()
    parameters = {'month': '202603', 'anomaly_kind': 'as_outage', 'start': '2026-03-31', 'end': '2026-04-01',
        'event_table': 'event_table_202603', 'prefix_table': 'prefix_outage_202603',
        'as_table': 'as_outage_202603', 'leak_table': 'leak_event_202603'}
    receipt = {'source_instance': manifest['source']['instance'], 'context': context, 'receipt': ending,
        'input_sha256': hashlib.sha256(data).hexdigest(), 'query_sha256': hashlib.sha256(query).hexdigest(),
        'bytes': len(data), 'parameters': parameters, 'elapsed_seconds': 1}
    files = {'data': ('source.jsonl', data), 'receipt': ('receipt.json', json.dumps(receipt).encode()),
        'query': ('query.sql', query)}
    selection = {'schema_version': 'core-overview-diagnostic-selection/v1', 'format': 'quality-by-day/v1',
        'source': manifest['source'], 'data_profile': manifest['data_profile'], 'files': {}}
    for key, (filename, payload) in files.items():
        (directory / filename).write_bytes(payload)
        selection['files'][key] = {'file': filename, 'sha256': hashlib.sha256(payload).hexdigest()}
    path = directory / 'selection.json'
    path.write_text(json.dumps(selection))
    return path


def index(source, output, *diagnostics):
    command = [sys.executable, str(ROOT / 'scripts/index-core-overview-inputs.py'), '--input', str(source), '--output', str(output)]
    for path in diagnostics:
        command.extend(['--diagnostic', str(path)])
    return subprocess.run(command, capture_output=True, text=True, timeout=10)


def hijack_selection(directory, declaration):
    """完整32日协议的合成证据，仅03-04含一条双侧结束冲突；不读取真实数据。"""
    directory.mkdir()
    days = [f'2026-02-{day:02d}' for day in range(1, 29)] + ['2026-03-01', '2026-03-02', '2026-03-04', '2026-03-05']
    reference = 'hijack/2026-03-04 00:00:00/203.0.113.0-24/1/r'
    event = {'source': 'r', 'detail_url': reference, 'event_type': '前缀劫持', 'affected_prefix': '203.0.113.0/24',
             's_time': '2026-03-04T00:00:00', 'e_time': '2026-03-04T00:01:00', 'duration': '00:01:00', 'level': 'low'}
    detail = {'source': 'r', 'prefix': '203.0.113.0/24', 'hijack_eventid': 1,
              's_time': event['s_time'], 'e_time': '2026-03-04T00:02:00', 'duration': '00:02:00', 'hijack_level': 'low'}
    counts = [{'kind': 'day_counts', 'day': day, 'event_table': 'event_table_' + day[:7].replace('-', ''),
               'detail_table': 'hijack_' + day[:7].replace('-', ''), 'kind_mismatch': 0,
               **{key: int(day == '2026-03-04') for key in ('event_rows', 'unique_refs', 'event_type_rows', 'refkind_rows',
                   'candidate_rows', 'unique_candidate_keys', 'detail_day_rows', 'detail_day_unique_keys')}} for day in days]
    begin = {'kind': 'tx_begin', 'utc': '2026-09-11T02:00:00+00:00', 'database': 'bgp_project',
             'read_only': 'on', 'isolation': 'repeatable read', 'timezone': 'UTC',
             'statement_timeout': '15s', 'lock_timeout': '2s', 'snapshot': 'fixture-snapshot'}
    ending = {'kind': 'tx_end', 'utc': '2026-09-11T02:00:01.12345+00:00', 'read_only': 'on',
              'isolation': 'repeatable read', 'snapshot': 'fixture-snapshot', 'application_name': 'fixture-hijack-read'}
    rows = [begin, {'kind': 'columns', 'rows': []}, {'kind': 'constraints', 'rows': []}, {'kind': 'indexes', 'rows': []},
            {'kind': 'scope', 'source': declaration['source'], 'data_profile': declaration['data_profile'], 'days': days},
            *counts,
            {'kind': 'source_record', 'day': '2026-03-04', 'event_table': 'event_table_202603', 'detail_table': 'hijack_202603',
             'event': event, 'candidate_count': 1, 'candidates': [detail]},
            {'kind': 'day_detail_record', 'day': '2026-03-04', 'detail_table': 'hijack_202603', 'detail': detail},
            {'kind': 'pair_check', 'day': '2026-03-04', 'detail_url': reference, 'end_diff': True, 'duration_diff': True,
             'level_diff': False, 'event_time_duration_diff': False, 'detail_time_duration_diff': False}, ending, {'kind': 'rollback_ack'}]
    data = b''.join((json.dumps(row) + '\n').encode() for row in rows)
    query = (ROOT / 'backend/data_pipeline/diagnostic_queries/hijack_selected_days.sql').read_bytes()
    receipt = {'source_instance': declaration['source']['instance'], 'parameters': {}, 'complete': True,
               'execution': {'exit_code': 0, 'failure': None, 'child_reaped': True}, 'rollback_ack': True,
               'result_bytes': len(data), 'result_lines': len(rows), 'result_sha256': hashlib.sha256(data).hexdigest(),
               'sql_bytes': len(query), 'sql_sha256': hashlib.sha256(query).hexdigest(), 'stderr_bytes': 0,
               'tx_begin': begin, 'tx_end': ending, 'application_name': ending['application_name']}
    selected = {'schema_version': 'core-overview-diagnostic-selection/v1', 'format': 'hijack-selected-days/v1',
                'source': declaration['source'], 'data_profile': declaration['data_profile'], 'files': {}}
    for key, filename, payload in [('data', 'source.jsonl', data), ('receipt', 'receipt.json', json.dumps(receipt).encode()), ('query', 'query.sql', query)]:
        (directory / filename).write_bytes(payload)
        selected['files'][key] = {'file': filename, 'sha256': hashlib.sha256(payload).hexdigest()}
    path = directory / 'selection.json'
    path.write_text(json.dumps(selected))
    return path


def test_hijack_time_disagreement_becomes_failed_day_not_partial_success(
    client, retained_with_hijack, tmp_path, monkeypatch,
):
    source, declaration, _ = retained_with_hijack
    selection = hijack_selection(tmp_path / 'hijack-evidence', declaration)
    output = tmp_path / 'four-type-with-failure'
    run = index(source, output, selection)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    for kind in ('all', 'hijack', 'prefix_outage'):
        response = client.get(URL, query_string={'date': '2026-03-04', 'kind': kind})
        assert response.status_code == 503
        body = response.get_json()
        assert body['overview'] is body['events'] is body['trend'] is None
        assert [(r['kind'], r['code'], r['count']) for r in body['diagnostic']['reasons']] == [('hijack', 'time_fields_conflict', 1)]
        evidence = body['diagnostic']['reasons'][0]['evidence']
        assert len(evidence['queried_dates']) == 32
        assert '2026-03-03' not in evidence['queried_dates'] and '2026-03-04' in evidence['queried_dates']
        import jsonschema
        contract = json.loads((ROOT / 'contracts/openapi.json').read_text())
        jsonschema.Draft202012Validator({**contract, '$ref': '#/components/schemas/CoreOverviewPayload'}).validate(body)


def rewrite_selection(path, change):
    """同时更新fixture原文和摘要，以测试语义校验而不是只触发摘要错误。"""
    selection = json.loads(path.read_text())
    rows = [json.loads(line) for line in (path.parent / 'source.jsonl').read_bytes().splitlines()]
    receipt = json.loads((path.parent / 'receipt.json').read_bytes())
    change(selection, rows, receipt)
    payload = b''.join((json.dumps(row) + '\n').encode() for row in rows)
    receipt.update(context=rows[0], receipt=rows[-2] if rows[-1]['kind'] == 'rollback_ack' else rows[-1])
    receipt['output_sha256' if selection['format'] == 'prefix-quality/v1' else 'input_sha256'] = hashlib.sha256(payload).hexdigest()
    receipt['bytes'] = {'stdout': len(payload), 'stderr': 0} if selection['format'] == 'prefix-quality/v1' else len(payload)
    if selection['format'] == 'hijack-selected-days/v1':
        receipt.update(tx_begin=rows[0], tx_end=rows[-2], result_bytes=len(payload),
                       result_lines=len(rows), result_sha256=hashlib.sha256(payload).hexdigest())
    (path.parent / 'source.jsonl').write_bytes(payload)
    (path.parent / 'receipt.json').write_text(json.dumps(receipt))
    for entry in selection['files'].values():
        entry['sha256'] = hashlib.sha256((path.parent / entry['file']).read_bytes()).hexdigest()
    path.write_text(json.dumps(selection))


@pytest.mark.parametrize('problem', ['partial_calendar', 'candidates', 'boolean_candidate', 'boolean_count',
    'missing_direct', 'pair_disagrees', 'wrong_source', 'read_failed', 'wrong_snapshot',
    'false_string', 'wrong_count_table'])
def test_hijack_diagnostic_refuses_unverified_evidence(retained_with_hijack, tmp_path, problem):
    source, declaration, _ = retained_with_hijack
    selection = hijack_selection(tmp_path / 'evidence', declaration)
    def change(selected, rows, receipt):
        original = next(row for row in rows if row['kind'] == 'source_record')
        if problem == 'partial_calendar': rows.pop(5)
        elif problem == 'candidates': original['candidates'].append(dict(original['candidates'][0]))
        elif problem == 'boolean_candidate': original['candidate_count'] = True
        elif problem == 'boolean_count': rows[5]['kind_mismatch'] = False
        elif problem == 'missing_direct': rows.remove(next(row for row in rows if row['kind'] == 'day_detail_record'))
        elif problem == 'pair_disagrees': next(row for row in rows if row['kind'] == 'pair_check')['end_diff'] = False
        elif problem == 'wrong_source': receipt['source_instance'] = 'unverified'
        elif problem == 'read_failed': receipt['execution']['exit_code'] = 1
        elif problem == 'wrong_snapshot': rows[-2]['snapshot'] = 'different'
        elif problem == 'false_string': receipt['complete'] = 'false'
        elif problem == 'wrong_count_table': rows[5]['detail_table'] = 'hijack_202603'
    rewrite_selection(selection, change)
    output = tmp_path / 'index'
    run = index(source, output, selection)
    assert run.returncode != 0, run.stdout
    assert not (output / 'manifest.json').exists()


def test_hijack_agreeing_tables_with_wrong_duration_still_fail(client, retained_with_hijack, tmp_path, monkeypatch):
    source, declaration, _ = retained_with_hijack
    selection = hijack_selection(tmp_path / 'evidence', declaration)
    def change(selected, rows, receipt):
        original = next(row for row in rows if row['kind'] == 'source_record')
        original['event'].update(e_time='2026-03-04T00:02:00', duration='00:01:00')
        original['candidates'][0]['duration'] = '00:01:00'
        next(row for row in rows if row['kind'] == 'day_detail_record')['detail']['duration'] = '00:01:00'
        next(row for row in rows if row['kind'] == 'pair_check').update(end_diff=False, duration_diff=False)
    rewrite_selection(selection, change)
    output = tmp_path / 'index'
    run = index(source, output, selection)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    response = client.get(URL, query_string={'date': '2026-03-04'})
    assert response.status_code == 503
    assert response.get_json()['diagnostic']['reasons'][0]['code'] == 'time_fields_conflict'


def test_hijack_uninterpretable_duration_is_not_a_proven_conflict(retained_with_hijack, tmp_path):
    source, declaration, _ = retained_with_hijack
    selection = hijack_selection(tmp_path / 'evidence', declaration)
    def change(selected, rows, receipt):
        original = next(row for row in rows if row['kind'] == 'source_record')
        original['event'].update(e_time=None, duration='1 mon')
        original['candidates'][0].update(e_time=None, duration='1 mon')
        next(row for row in rows if row['kind'] == 'day_detail_record')['detail'].update(e_time=None, duration='1 mon')
        next(row for row in rows if row['kind'] == 'pair_check').update(end_diff=False, duration_diff=False)
    rewrite_selection(selection, change)
    output = tmp_path / 'index'
    run = index(source, output, selection)
    assert run.returncode != 0
    assert not (output / 'manifest.json').exists()


def test_two_types_of_time_failure_merge_without_admitting_that_day(client, retained, tmp_path, monkeypatch):
    source, manifest, _ = retained
    as_selection = diagnostic_selection(tmp_path / 'as-diagnostic', manifest)
    prefix_selection = diagnostic_selection(tmp_path / 'prefix-diagnostic', manifest)
    def as_time(selection, rows, receipt):
        rows[0].update(start='2026-03-03', end_exclusive='2026-03-04')
        rows[1]['items'][0].update(day='2026-03-03', level_conflicts=0, invalid_time_order=3)
        receipt['parameters'].update(start='2026-03-03', end='2026-03-04')
    rewrite_selection(as_selection, as_time)
    query = (ROOT / 'backend/data_pipeline/diagnostic_queries/prefix_quality.sql').read_bytes()
    (prefix_selection.parent / 'query.sql').write_bytes(query)
    def prefix_time(selection, rows, receipt):
        selection['format'] = 'prefix-quality/v1'
        rows[0].update(start='2026-03-03', end_exclusive='2026-03-04')
        data = {**rows[1]['items'][0], 'window_start': '2026-03-03', 'window_end_exclusive': '2026-03-04',
            'records': 12, 'distinct_refs': 12, 'level_conflicts': 0, 'invalid_time_order': 9,
            'unique_candidates': 12, 'matched_candidate_rows': 12, 'max_candidates': 1,
            'null_refs': 0, 'invalid_reference_ids': 0, 'event_level_not_recognized': 0,
            'candidate_missing_start_rows': 0, 'candidate_invalid_level_rows': 0, 'detail_duration_arithmetic_conflicts': 0}
        rows[1] = {'kind': 'prefix_quality', 'data': data}
        rows.append({'kind': 'rollback_ack', 'status': 'ROLLBACK 后客户端到达确认行'})
        receipt.update(state='success', returncode=0, error=None, rollback_ack=rows[-1],
            parameters={'start': '2026-03-03', 'end': '2026-03-04', 'explain': 'false'},
            query_sha256=hashlib.sha256(query).hexdigest())
    rewrite_selection(prefix_selection, prefix_time)
    output = tmp_path / 'index'
    run = index(source, output, as_selection, prefix_selection)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    response = client.get(URL, query_string={'date': '2026-03-03', 'family':'ipv6', 'kind':'leak', 'hour':'23', 'q':'no-match'})
    assert response.status_code == 503
    body = response.get_json()
    assert body['events'] is body['overview'] is body['trend'] is None
    assert [(r['kind'], r['code'], r['count']) for r in body['diagnostic']['reasons']] == [
        ('as_outage', 'invalid_time_order', 3), ('prefix_outage', 'invalid_time_order', 9)]
    assert body['metadata']['diagnostic_dates'] == ['2026-03-03']


def test_source_failure_has_evidence_but_no_metrics_or_record_access(client, retained, tmp_path, monkeypatch):
    path, manifest, records = retained
    selection = diagnostic_selection(tmp_path / 'diagnostic', manifest)
    output = tmp_path / 'index'
    run = index(path, output, selection)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    response = client.get(URL)
    assert response.status_code == 503
    body = response.get_json()
    assert body['state'] == 'unavailable' and body['query']['date'] == '2026-03-31'
    import jsonschema
    contract = json.loads((ROOT / 'contracts/openapi.json').read_bytes())
    jsonschema.Draft202012Validator({**contract, '$ref': '#/components/schemas/CoreOverviewPayload'}).validate(body)
    assert body['overview'] is body['trend'] is body['events'] is None
    assert body['metadata']['available_dates'] == ['2026-02-27']
    assert body['metadata']['diagnostic_dates'] == ['2026-03-31']
    assert body['diagnostic']['stage'] == 'source_field_precheck'
    reason = body['diagnostic']['reasons'][0]
    assert (reason['code'], reason['kind'], reason['count']) == ('level_conflict', 'as_outage', 2)
    assert reason['evidence']['read_at'] == '2026-09-10T15:00:00.12345+00:00'
    assert body['diagnostic']['version'].startswith('overview_diagnostic_v1_')
    assert body['version'].startswith('overview_index_v2_')
    good = client.get(URL, query_string={'date': '2026-02-27', 'version': body['version']}).get_json()
    assert good['overview']['record_count'] == 7
    assert client.get(URL, query_string={'date': '2026-03-30'}).get_json()['state'] == 'window_not_retained'
    ref = records[4]['record']['identity']['legacy_reference'].replace('2026-02-27', '2026-03-31')
    assert client.get(URL + '/record', query_string={'ref': ref, 'version': body['version']}).status_code == 503
    assert client.get(URL, query_string={'version': 'overview_index_v1_' + '0' * 64}).status_code == 409
    assert client.get(URL + '/record', query_string={'ref': ref, 'version': 'overview_index_v1_' + '0' * 64}).status_code == 409


@pytest.mark.parametrize('problem', ['missing', 'checksum', 'escape', 'oversize', 'format'])
def test_broken_diagnostic_does_not_hide_good_days_or_turn_into_missing(client, retained, tmp_path, monkeypatch, problem):
    source, manifest, _ = retained
    selection = diagnostic_selection(tmp_path / 'evidence', manifest)
    output = tmp_path / 'index'
    assert index(source, output, selection).returncode == 0
    path = output / '2026-03-31.diagnostic.json'
    path.chmod(0o600)
    if problem == 'missing':
        path.rename(output / 'kept-original.json')
    elif problem == 'escape':
        outside = tmp_path / 'outside.json'
        path.rename(outside)
        path.symlink_to(outside)
    else:
        content = json.loads(path.read_bytes())
        if problem == 'format':
            content['schema_version'] = 'unsupported'
        path.write_bytes(b'x' * 65537 if problem == 'oversize' else json.dumps(content).encode())
        if problem != 'checksum':
            catalog_path = output / 'manifest.json'
            catalog = json.loads(catalog_path.read_bytes())
            catalog['diagnostics']['2026-03-31']['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            catalog_path.chmod(0o600)
            catalog_path.write_text(json.dumps(catalog))
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    response = client.get(URL)
    body = response.get_json()
    assert response.status_code == 503 and '诊断证据不可验证' in body['message']
    assert body['overview'] is body['events'] is body['trend'] is None and 'diagnostic' not in body
    assert body['metadata']['available_dates'] == ['2026-02-27']
    assert body['metadata']['diagnostic_dates'] == ['2026-03-31']
    assert client.get(URL, query_string={'date':'2026-02-27'}).get_json()['overview']['record_count'] == 7


@pytest.mark.parametrize('problem', ['empty', 'boolean_count', 'too_many', 'source', 'profile', 'query', 'receipt_time',
    'database', 'partial_calendar', 'month', 'duplicate_refs', 'boolean_refs', 'candidates', 'row_day', 'duplicate_selection'])
def test_diagnostic_compilation_rejects_inconsistent_evidence(retained, tmp_path, problem):
    source, manifest, _ = retained
    selection = diagnostic_selection(tmp_path / 'evidence', manifest)
    def change(selected, rows, receipt):
        row = rows[1]['items'][0]
        if problem == 'empty': row['level_conflicts'] = 0
        elif problem == 'boolean_count': row['level_conflicts'] = True
        elif problem == 'too_many': row['level_conflicts'] = 4
        elif problem == 'source': receipt['source_instance'] = 'other-instance'
        elif problem == 'profile': selected['data_profile']['timezone'] = 'UTC'
        elif problem == 'query': receipt['parameters']['as_table'] = 'as_outage_202602'
        elif problem == 'receipt_time': rows[-1]['finished_at'] = '2026-09-10T14:00:00Z'
        elif problem == 'database': rows[0]['database'] = 'other_database'
        elif problem == 'partial_calendar': rows[1]['items'] = []
        elif problem == 'month': rows[0]['month'] = '202602'
        elif problem == 'duplicate_refs': row['distinct_refs'] = 2
        elif problem == 'boolean_refs': row.update(records=1, distinct_refs=True, level_conflicts=1)
        elif problem == 'candidates': row['multiple_candidates'] = 1
        elif problem == 'row_day': row['day'] = '2026-03-30'
    rewrite_selection(selection, change)
    output = tmp_path / 'index'
    diagnostics = [selection, selection] if problem == 'duplicate_selection' else [selection]
    run = index(source, output, *diagnostics)
    assert run.returncode != 0, run.stdout
    assert not (output / 'manifest.json').exists()


def test_consumption_and_failure_cannot_select_the_same_day(retained, tmp_path):
    source, manifest, _ = retained
    selection = diagnostic_selection(tmp_path / 'evidence', manifest)
    def change(selected, rows, receipt):
        rows[0].update(month='202602', start='2026-02-27', end_exclusive='2026-02-28')
        rows[1]['items'][0]['day'] = '2026-02-27'
        receipt['parameters'] = {key: value.replace('202603', '202602') for key, value in receipt['parameters'].items()}
        receipt['parameters'].update(start='2026-02-27', end='2026-02-28')
    rewrite_selection(selection, change)
    output = tmp_path / 'index'
    run = index(source, output, selection)
    assert run.returncode != 0 and '同时选择' in run.stderr
    assert not (output / 'manifest.json').exists()
