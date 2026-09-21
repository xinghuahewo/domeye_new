"""只读 HTTP 的完整日校验／读取失败边界；全部证据为临时合成数据。"""
import hashlib
import json

import pytest

from tests.core_overview.test_core_overview_api import retained
from tests.core_overview.test_core_overview_diagnostics import ROOT, URL, index


def selected_diagnostic(tmp_path, retained, monkeypatch, *, read_failure=False):
    source, declaration, _ = retained
    output = tmp_path / 'index'
    run = index(source, output)
    assert run.returncode == 0, run.stderr
    path = output / 'manifest.json'
    manifest = json.loads(path.read_bytes())
    window = {'source': 'r', 'start': '2026-03-27T00:00:00+08:00', 'end_exclusive': '2026-03-28T00:00:00+08:00'}
    evidence = {'format': 'failed-day-read/v1' if read_failure else 'complete-day-audit/v1',
        'source_data_sha256': 'a' * 64, 'query_sha256': 'b' * 64, 'selection_sha256': 'c' * 64,
        'receipt_sha256': None if read_failure else 'd' * 64,
        'audit_sha256': None if read_failure else 'e' * 64,
        'failure_sha256': 'f' * 64 if read_failure else None,
        'query_window': window, 'read_at': '2026-09-11T02:00:00+00:00',
        'finished_at': None if read_failure else '2026-09-11T02:00:01+00:00'}
    diagnostic = {'schema_version': 'core-overview-diagnostic/v2',
        'stage': 'source_read' if read_failure else 'source_field_validation',
        'source': declaration['source'], 'data_profile': declaration['data_profile'], 'window': window,
        'compiler_sha256': '0' * 64, 'reasons': [{
            'code': 'source_read_timeout' if read_failure else 'source_identity_unresolved',
            'kind': 'all' if read_failure else 'as_outage', 'count': None if read_failure else 2,
            'evidence': evidence}]}
    manifest.update(schema_version='core-overview-index/v2', diagnostics={})
    def save():
        payload = json.dumps(diagnostic).encode()
        file = output / '2026-03-27.diagnostic.json'
        file.write_bytes(payload)
        manifest['diagnostics']['2026-03-27'] = {'file': file.name, 'sha256': hashlib.sha256(payload).hexdigest(), 'window': window}
        path.chmod(0o600)
        path.write_text(json.dumps(manifest))
    save()
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(path))
    return diagnostic, save


@pytest.mark.parametrize('read_failure', [False, True])
def test_failed_day_retains_exact_failure_stage_and_null_metrics(client, retained, tmp_path, monkeypatch, read_failure):
    diagnostic, _ = selected_diagnostic(tmp_path, retained, monkeypatch, read_failure=read_failure)
    for kind in ('all', 'prefix_outage', 'as_outage'):
        response = client.get(URL, query_string={'date': '2026-03-27', 'kind': kind})
        assert response.status_code == 503
        body = response.get_json()
        assert body['state'] == 'unavailable'
        assert body['overview'] is body['events'] is body['trend'] is None
        assert body['diagnostic']['reasons'] == diagnostic['reasons']
        assert body['diagnostic']['stage'] == diagnostic['stage']
        assert body['diagnostic']['version'].startswith('overview_diagnostic_v2_')
    ref = 'as_outage/2026-03-27 00:00:00/64512/1/r'
    detail = client.get(URL + '/record', query_string={'ref': ref, 'version': body['version']})
    assert detail.status_code == 503
    assert client.get(URL, query_string={'date': '2026-02-27'}).status_code == 200
    import jsonschema
    contract = json.loads((ROOT / 'contracts/openapi.json').read_text())
    jsonschema.Draft202012Validator({'$ref': '#/components/schemas/CoreOverviewPayload', 'components': contract['components']}).validate(body)


@pytest.mark.parametrize('mutation', ['count', 'stage', 'format', 'receipt', 'finished', 'kind', 'window', 'old_schema'])
def test_inconsistent_read_failure_cannot_be_presented_as_verified(client, retained, tmp_path, monkeypatch, mutation):
    diagnostic, save = selected_diagnostic(tmp_path, retained, monkeypatch, read_failure=True)
    reason = diagnostic['reasons'][0]
    if mutation == 'count': reason['count'] = 0
    if mutation == 'stage': diagnostic['stage'] = 'source_field_validation'
    if mutation == 'format': reason['evidence']['format'] = 'complete-day-audit/v1'
    if mutation == 'receipt': reason['evidence']['receipt_sha256'] = 'd' * 64
    if mutation == 'finished': reason['evidence']['finished_at'] = '2026-09-11T02:01:30+00:00'
    if mutation == 'kind': reason['kind'] = 'as_outage'
    if mutation == 'window': reason['evidence']['query_window'] = {**diagnostic['window'], 'source': 'other'}
    if mutation == 'old_schema': diagnostic['schema_version'] = 'core-overview-diagnostic/v1'
    save()
    body = client.get(URL, query_string={'date': '2026-03-27'}).get_json()
    assert body['state'] == 'unavailable'
    assert body.get('diagnostic') is None
    assert body['overview'] is body['events'] is body['trend'] is None


@pytest.mark.parametrize('count', [0, True, None, -1])
def test_source_failure_requires_a_positive_integer_count(client, retained, tmp_path, monkeypatch, count):
    diagnostic, save = selected_diagnostic(tmp_path, retained, monkeypatch)
    diagnostic['reasons'][0]['count'] = count
    save()
    body = client.get(URL, query_string={'date': '2026-03-27'}).get_json()
    assert body.get('diagnostic') is None
