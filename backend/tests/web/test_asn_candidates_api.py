"""用内存关系查询验证候选分页、文件区间纳入和单 AS 读取；不访问真实库。"""
import json
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import psycopg2
import pytest
from jsonschema import Draft202012Validator

from config.database import LazyConnection
from data_pipeline.results import delivery_read
from database import asn_workbench
from services import asn_candidates, asn_service
from utils import asn_reference

URL = '/api/v1/features/ases/candidates'
QUERY = {'start_time': '2026-02-24 08:00:00', 'end_time': '2026-02-24 08:10:00'}


@pytest.fixture
def source(monkeypatch):
    database = duckdb.connect(':memory:')
    database.execute('CREATE SCHEMA result_delivery')
    database.execute('CREATE TABLE result_delivery.files (ordinal INTEGER, window_start TIMESTAMPTZ, window_end TIMESTAMPTZ)')
    database.execute('CREATE TABLE result_delivery.features (ordinal INTEGER, t TIMESTAMP, scope VARCHAR, subject VARCHAR, country VARCHAR, announ_num BIGINT, withdraw_num BIGINT, v4prefix_num BIGINT, v6prefix_num BIGINT, v4ip_num BIGINT)')
    for ordinal, left, right in [(0, '08:00:00', '08:00:00'), (1, '08:00:00', '08:05:00'), (2, '08:05:00', '08:10:00'), (3, '08:10:00', '08:15:00')]:
        database.execute('INSERT INTO result_delivery.files VALUES (?,?,?)', [ordinal, '2026-02-24T'+left+'+08:00', '2026-02-24T'+right+'+08:00'])
    for ordinal, label, asn, country, announce, withdraw in [
        (0, '08:00:00', '64599', '伊朗', 9999, 0),
        (1, '08:00:00', '64500', '伊朗', 5, 0), (2, '08:05:00', '64500', '伊朗', 7, 2),
        (3, '08:10:00', '64500', '伊朗', 10000, 0),
        (1, '08:00:00', '64501', '伊朗', None, 3),
        (1, '08:00:00', '64502', '美国', 100, 0),
        (1, '08:00:00', '64503', None, 0, 0),
    ]:
        database.execute('INSERT INTO result_delivery.features VALUES (?,?,?,?,?,?,?,?,?,?)', [ordinal, '2026-02-24 '+label, 'asn', asn, country, announce, withdraw, 1, None, 256])
    state = {'queries': [], 'fail': False, 'database': database}
    class Connection:
        closed = 1
        def rollback(self): pass
        def cursor(self, **kwargs):
            class Cursor:
                def __enter__(self): return self
                def __exit__(self, *args): pass
                def close(self): pass
                def execute(self, sql, params=()):
                    assert sql.lstrip().startswith(('SELECT', 'WITH'))
                    if state['fail']: raise psycopg2.OperationalError('合成只读故障')
                    state['queries'].append((sql, params))
                    # 仅适配 PostgreSQL 的 JSON/数组函数名，原查询的 JOIN、筛选、分组与排序照常执行。
                    sql = sql.replace('%s', '?').replace('jsonb_agg', 'json_group_array').replace('to_jsonb', 'to_json').replace('::jsonb', '::json')
                    sql = sql.replace("array_remove(array_agg(DISTINCT NULLIF(x.country, '') ORDER BY NULLIF(x.country, '')), NULL)", "list_filter(array_agg(DISTINCT NULLIF(x.country, '') ORDER BY NULLIF(x.country, '')), c -> c IS NOT NULL)")
                    self.result = database.execute(sql, params)
                def fetchone(self):
                    total, raw = self.result.fetchone()
                    rows = json.loads(raw)
                    for row in rows:
                        row['latest_observation'] = datetime.fromisoformat(row['latest_observation'] + ':00' if row['latest_observation'].endswith('+00') else row['latest_observation']).isoformat()
                    return total, rows
                def fetchall(self):
                    rows = self.result.fetchall()
                    if kwargs.get('cursor_factory'):
                        keys = [cell[0] for cell in self.result.description]
                        return [dict(zip(keys, row)) for row in rows]
                    return rows
            return Cursor()
    state['connection'] = Connection()
    meta = {'state': 'available', 'version': 'delivery_fixture', 'binding': {'collector': 'rrc25'},
            'start': '2026-02-24T08:00:00+08:00', 'end_exclusive': '2026-02-24T08:15:00+08:00',
            'intervals': [{'start': '2026-02-24T08:00:00+08:00', 'end_exclusive': '2026-02-24T08:15:00+08:00'}]}
    monkeypatch.setenv('DOMEYE_RESULT_DELIVERY', 'true')
    monkeypatch.setattr(delivery_read, 'status', lambda *a, **k: meta)
    monkeypatch.setattr(delivery_read, 'available_countries', lambda *a, **k: ['伊朗', '美国'])
    monkeypatch.setattr(LazyConnection, 'get_connection', lambda self: state['connection'])
    monkeypatch.setattr(asn_service.data_loader, 'as_info', {'64500': {'as_name': '示例 AS', 'org_name': '示例组织'}})
    yield state
    database.close()


def validate(payload):
    spec = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_text())
    Draft202012Validator({'$ref': '#/components/schemas/AsCandidatesPayload', 'components': spec['components']}).validate(payload)


def test_country_filter_full_files_nulls_and_pagination(source, client):
    response = client.get(URL, query_string={**QUERY, 'country': '伊朗', 'page_size': 1})
    assert response.status_code == 200
    payload = response.get_json(); validate(payload)
    assert payload['total'] == payload['page_count'] == 2
    row = payload['items'][0]
    assert row['asn'] == '64500' and row['announce'] == 12 and row['withdraw'] == 2
    assert row['sample_count'] == 2 and row['latest_observation'] == '2026-02-24T08:10:00+08:00'
    assert row['as_name'] == '示例 AS' and row['anomaly_count'] is None
    assert payload['metadata']['version'] == response.headers['X-Domeye-Result-Version']
    second = client.get(URL, query_string={**QUERY, 'country': '伊朗', 'page_size': 1, 'page': 2}).get_json()
    assert second['items'][0]['asn'] == '64501'
    assert second['items'][0]['announce'] is second['items'][0]['update_total'] is None
    beyond = client.get(URL, query_string={**QUERY, 'country': '伊朗', 'page': 99}).get_json()
    assert beyond['total'] == 2 and beyond['items'] == []


def test_prefix_search_numeric_sort_zero_and_unknown_country(source, client):
    payload = client.get(URL, query_string={**QUERY, 'q': 'AS6450', 'sort': 'asn', 'order': 'desc'}).get_json()
    validate(payload)
    assert [row['asn'] for row in payload['items']] == ['64503', '64502', '64501', '64500']
    assert payload['items'][0]['update_total'] == 0 and payload['items'][0]['withdraw_rate'] is None
    assert payload['items'][0]['country'] is None and payload['items'][0]['countries'] == []
    assert client.get(URL, query_string={**QUERY, 'country': '伊'}).status_code == 400
    empty = client.get(URL, query_string={**QUERY, 'q': '99999'}).get_json()
    assert empty['state'] == 'available' and empty['items'] == []


def test_second_precision_excludes_partial_files_in_candidates_and_details(source, client):
    request = {**QUERY, 'start_time': '2026-02-24 08:00:01'}
    payload = client.get(URL, query_string=request).get_json()
    assert payload['total'] == 1 and payload['items'][0]['sample_count'] == 1
    assert payload['items'][0]['update_total'] == 9
    rows = asn_workbench.get_as_feature_aggregates(source['connection'], {'wrong_static_country_table': ['64500']},
        datetime(2026,2,24,8,0,1), datetime(2026,2,24,8,0,1), datetime(2026,2,24,8,10))
    assert rows[0]['sample_count'] == 1 and rows[0]['announce'] == 7 and rows[0]['withdraw'] == 2
    assert rows[0]['ipv6_prefixes'] is None
    series = asn_workbench.get_as_feature_series(source['connection'], 'wrong_static_country_table', '64500',
        datetime(2026,2,24,8,0,1), datetime(2026,2,24,8,10))
    assert len(series) == 1 and series[0]['time'] == datetime(2026,2,24,8,5)


def test_no_coverage_no_read_and_failures_are_explicit(source, client, monkeypatch):
    payload = client.get(URL, query_string={**QUERY, 'start_time': '2026-02-25 08:00:00', 'end_time': '2026-02-25 09:00:00'}).get_json()
    validate(payload)
    assert payload['state'] == 'window_not_observed' and payload['total'] == 0
    assert not source['queries']
    assert client.get(URL, query_string={**QUERY, 'version': 'old'}).status_code == 409
    source['fail'] = True
    assert client.get(URL, query_string=QUERY).status_code == 503
    monkeypatch.delenv('DOMEYE_RESULT_DELIVERY')
    assert client.get(URL, query_string=QUERY).status_code == 503


@pytest.mark.parametrize('extra', [{'q':'name'}, {'q':"1%';SELECT"}, {'sort':'country'}, {'order':'random'},
    {'page_size':'51'}, {'page':'0'}, {'version':''}, {'country':['伊朗','美国']}, {'extra':'x'},
    {'end_time':'2026-04-15 00:00:00'}, {'start_time':'2026-02-24 08:10:00'}])
def test_bad_query_never_reads_candidate_rows(source, client, extra):
    assert client.get(URL, query_string={**QUERY, **extra}).status_code == 400
    assert not source['queries']


@pytest.mark.parametrize('endpoint', ['overview', 'events'])
def test_as_profile_outside_delivery_is_unknown_not_zero(source, client, endpoint):
    response = client.get('/api/v1/features/ases/' + endpoint, query_string={
        'asn': '64500', 'start_time': '2026-02-25 08:00:00', 'end_time': '2026-02-26 08:00:00'})
    assert response.status_code == 503
    assert response.get_json() == {'status': False, 'msg': '所选区间没有已交付观测，数量未知'}
    assert not source['queries']


def test_selected_asn_multiday_reads_own_samples_without_static_pool(source, client, monkeypatch):
    monkeypatch.setattr(asn_service.data_loader, 'ensure_core_data_loaded', lambda: pytest.fail('单AS无需加载静态重点池'))
    monkeypatch.setattr(asn_service, 'get_as_event_counts', lambda **kwargs: [])
    query = {'asn': 'AS64501', 'start_time': '2026-02-23 00:00:00', 'end_time': '2026-02-26 00:00:00'}
    response = client.get('/api/v1/features/ases/overview', query_string=query)
    assert response.status_code == 200
    payload = response.get_json()
    assert payload['scope_kind'] == 'selected_asn' and payload['scope_size'] == 1
    assert payload['delivery_coverage']['state'] == 'partial'
    assert payload['delivery_coverage']['version'] == response.headers['X-Domeye-Result-Version']
    assert '未交付' in payload['scope_note']
    profile = payload['selected_asn']
    assert profile['asn'] == '64501' and profile['sample_count'] == 1
    assert profile['announce'] is profile['update_total'] is profile['withdraw_rate'] is None
    assert profile['previous_update_total'] is profile['update_change_rate'] is None
    assert profile['sparkline'][0]['announce'] is None
    assert profile['series'][0]['announce'] is None
    assert payload['withdraw_rate_rankings'] == []
    spec = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_text())
    Draft202012Validator({'$ref':'#/components/schemas/AsOverview','components':spec['components']}).validate(payload)


@pytest.mark.parametrize('endpoint', ['overview', 'events'])
def test_as_profile_maximum_window_and_invalid_asn(source, client, endpoint):
    for extra in ({'end_time':'2026-03-18 00:00:01'}, {'asn':'4294967296'}, {'asn':'AS零'}, {'asn':['64500','64501']}):
        response = client.get('/api/v1/features/ases/' + endpoint, query_string={
            'asn': '64500', 'start_time': '2026-02-01 00:00:00', 'end_time': '2026-03-18 00:00:00', **extra})
        assert response.status_code == 400
    assert not source['queries']


def test_as_recent_events_keep_full_multiday_window_and_partial_coverage(source, client, monkeypatch):
    calls = []
    monkeypatch.setattr(asn_service, 'get_as_exact_event_rows', lambda **kwargs: (calls.append(kwargs) or [], 0))
    response = client.get('/api/v1/features/ases/events', query_string={
        'asn':'AS64500', 'start_time':'2026-02-23 00:00:00','end_time':'2026-02-26 00:00:00'})
    assert response.status_code == 200
    payload = response.get_json()
    assert payload['delivery_coverage']['state'] == 'partial' and '未交付' in payload['scope_note']
    assert calls[0]['asn'] == '64500' and calls[0]['end_time'] - calls[0]['start_time'] == timedelta(days=3)
    assert payload['delivery_coverage']['version'] == response.headers['X-Domeye-Result-Version']
    spec = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_text())
    Draft202012Validator({'$ref':'#/components/schemas/AsExactEventPage','components':spec['components']}).validate(payload)



def test_latest_missing_resource_is_not_replaced_by_older_known_value(source):
    source['database'].execute("UPDATE result_delivery.features SET v6prefix_num=3 WHERE ordinal=1 AND subject='64500'")
    rows = asn_workbench.get_as_feature_aggregates(source['connection'], {'feature_wrong': ['64500']},
        datetime(2026,2,24,8), datetime(2026,2,24,8), datetime(2026,2,24,8,10))
    assert rows[0]['ipv6_prefixes'] is None


def test_candidate_and_selected_asn_share_identity_without_changing_sample_country(source, client, monkeypatch, tmp_path):
    identity_file = tmp_path / 'identity.csv'
    identity_file.write_text('asn,as_name,as_country_cn,org_name,org_name_cn,type\n'
                             '64500,共享名称,静态参考国,English,共享组织,Transit\n', encoding='utf-8')
    monkeypatch.setattr(asn_reference, 'AS_INFO_FILE', str(identity_file))
    monkeypatch.setattr(asn_reference.data_loader, 'as_info', {})
    monkeypatch.setattr(asn_service.data_loader, 'ensure_core_data_loaded', lambda: pytest.fail('不得加载大型核心资料'))
    monkeypatch.setattr(asn_service, 'get_as_event_counts', lambda **kwargs: [])
    candidate = client.get(URL, query_string={**QUERY, 'country':'伊朗', 'q':'64500'}).get_json()
    profile = client.get('/api/v1/features/ases/overview', query_string={**QUERY, 'asn':'64500'}).get_json()
    assert candidate['items'][0]['country'] == '伊朗'
    for key in ('as_name', 'org_name'):
        assert candidate['items'][0][key] == profile['selected_asn'][key]
    assert profile['selected_asn']['country'] == '静态参考国'
    assert profile['selected_asn']['as_type'] == 'Transit'
    assert '历史适用性未知' in profile['scope_note']
    assert any('历史适用性未知' in note for note in candidate['metadata']['limitations'])
    assert asn_reference._read_identities.cache_info().misses == 1
