"""通过公开查询验证退役、缺失值、窗口和合同，不读取真实制品。"""

from contextlib import ExitStack
from datetime import datetime, timedelta
import json
from pathlib import Path
from unittest.mock import patch

import duckdb
import jsonschema
import pandas as pd
import pytest

from database import asn_workbench, country_workbench
from services import asn_service, country_service


RETIRED = (
    '/api/v1/p0/status', '/api/v1/p0/metrics/bgp_update_record_count', '/api/v1/p0/quality',
    '/api/v1/dashboard/counts/total', '/api/v1/dashboard/counts/type', '/api/v1/dashboard/overview',
)
WINDOW = {'start_time': '2026-02-04 00:00:00', 'end_time': '2026-02-04 00:10:00'}


@pytest.fixture(autouse=True)
def clear_query_caches(monkeypatch):
    monkeypatch.setattr(asn_service, '_ASN_CACHE', {})
    monkeypatch.setattr(country_service, '_COUNTRY_CACHE', {})


@pytest.mark.parametrize('enforced', [False, True])
@pytest.mark.parametrize('path', RETIRED)
def test_retired_endpoints_do_not_read_data(client, monkeypatch, tmp_path, path, enforced):
    monkeypatch.setenv('P0_DATA_RELEASE_DIR', str(tmp_path / 'retired'))
    monkeypatch.setenv('P0_DATA_PRODUCTION_ACTIVE', 'true')
    monkeypatch.setenv('DOMEYE_ENFORCE_DATA_WINDOW', str(enforced).lower())
    if enforced:
        monkeypatch.setenv('DOMEYE_DATA_WINDOW_START', '2026-02-01 00:00:00')
        monkeypatch.setenv('DOMEYE_DATA_WINDOW_END_EXCLUSIVE', '2026-04-01 00:00:00')
        monkeypatch.setenv('DOMEYE_DATA_SNAPSHOT_TIME', '2026-03-31 23:59:59')
    with patch('psycopg2.connect', side_effect=AssertionError('退役入口不得连接数据库')), \
         patch('utils.data_loader.ensure_core_data_loaded', side_effect=AssertionError('不得加载旧数据')):
        # 无参数或无效旧参数都应先表达路由已不存在，而非沿旧查询继续处理。
        assert client.get(path).status_code == 404
        assert client.get(path, query_string={'start_time': 'invalid'}).status_code == 404


def profile_request(client, family, rows):
    service = 'services.country_service' if family == 'country' else 'services.asn_service'
    selected = {'country': '测试国'} if family == 'country' else {'asn': '64500'}
    with ExitStack() as stack:
        if family == 'asn':
            stack.enter_context(patch(service + '.data_loader.ensure_core_data_loaded'))
            stack.enter_context(patch.object(asn_service.data_loader, 'ases_1000', pd.DataFrame([{'asn': '64500'}])))
            stack.enter_context(patch.object(asn_service.data_loader, 'as_info', {}))
            stack.enter_context(patch.object(asn_service.data_loader, 'important_as_dict', {}))
        prefix = 'get_country' if family == 'country' else 'get_as'
        for suffix, value in [('feature_aggregates', rows), ('event_counts', []), ('sparklines', []), ('feature_series', [])]:
            stack.enter_context(patch(service + '.' + prefix + '_' + suffix, return_value=value))
        endpoint = 'countries' if family == 'country' else 'ases'
        response = client.get('/api/v1/features/' + endpoint + '/overview', query_string={**WINDOW, **selected})
    assert response.status_code == 200
    payload = response.get_json()
    contract = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_text())
    schema = 'CountryOverview' if family == 'country' else 'AsOverview'
    jsonschema.Draft202012Validator({'$ref': '#/components/schemas/' + schema, 'components': contract['components']}).validate(payload)
    return payload, payload['selected_country' if family == 'country' else 'selected_asn']


@pytest.mark.parametrize('family', ['country', 'asn'])
def test_no_samples_are_unknown_instead_of_zero(client, family):
    payload, profile = profile_request(client, family, [])
    assert payload['window_boundary'] == '[start,end)'
    for field in ('announce', 'withdraw', 'update_total', 'withdraw_rate', 'previous_update_total', 'update_change_rate', 'resource_change', 'peak_updates'):
        assert profile[field] is None, field
    assert profile['sample_count'] == profile['previous_sample_count'] == 0
    assert payload['update_rankings'] == payload['withdraw_rate_rankings'] == []
    if family == 'asn':
        assert profile['volatility'] is None
        assert payload['volatility_rankings'] == []


@pytest.mark.parametrize('family', ['country', 'asn'])
def test_observed_zero_stays_zero_but_ratio_is_not_applicable(client, family):
    row = {'country': '测试国', 'asn': '64500', 'sample_count': 2, 'previous_sample_count': 0,
           'announce': 0, 'withdraw': 0, 'previous_announce': 0, 'previous_withdraw': 0,
           'peak_updates': 0, 'update_average': 0.0, 'update_stddev': 0.0}
    payload, profile = profile_request(client, family, [row])
    assert profile['update_total'] == profile['announce'] == profile['withdraw'] == 0
    assert profile['withdraw_rate'] is None
    assert profile['previous_update_total'] is None
    assert payload['withdraw_rate_rankings'] == []
    if family == 'asn':
        assert profile['volatility'] is None


@pytest.mark.parametrize('family', ['country', 'asn'])
def test_comparable_observed_samples_preserve_percent_and_change(client, family):
    row = {'country': '测试国', 'asn': '64500', 'sample_count': 2, 'previous_sample_count': 1,
           'announce': 5, 'withdraw': 1, 'previous_announce': 8, 'previous_withdraw': 2,
           'peak_updates': 4, 'update_average': 3.0, 'update_stddev': 1.0}
    _, profile = profile_request(client, family, [row])
    assert profile['update_total'] == 6
    assert profile['previous_update_total'] == 10
    assert profile['withdraw_rate'] == 16.7
    assert profile['update_change_rate'] == -40.0
    if family == 'asn':
        assert profile['volatility'] == 33.3


@pytest.mark.parametrize('endpoint,service', [
    ('overview', 'get_asn_workbench'), ('events', 'get_asn_recent_events'),
])
@pytest.mark.parametrize('extra', [
    [('event_window', 'True')], [('event_window', '1')],
    [('event_window', 'true'), ('event_window', 'false')],
    [('event_reference', 'country_outage/example')],
    [('event_window', 'true'), ('event_reference', 'one'), ('event_reference', 'two')],
])
def test_event_mode_rejects_ambiguous_parameters_before_query(client, endpoint, service, extra):
    with patch('web.api.features.api.' + service) as query:
        response = client.get('/api/v1/features/ases/' + endpoint, query_string=[*WINDOW.items(), ('asn', '64500'), *extra])
    assert response.status_code == 400
    assert response.get_json()['status'] is False
    query.assert_not_called()


@pytest.mark.parametrize('path,service', [
    ('countries/overview', 'get_country_workbench'),
    ('ases/overview', 'get_asn_workbench'), ('ases/events', 'get_asn_recent_events'),
])
def test_half_open_query_accepts_exclusive_data_window_end(client, monkeypatch, path, service):
    monkeypatch.setenv('DOMEYE_ENFORCE_DATA_WINDOW', 'true')
    monkeypatch.setenv('DOMEYE_DATA_WINDOW_START', '2026-02-01 00:00:00')
    monkeypatch.setenv('DOMEYE_DATA_WINDOW_END_EXCLUSIVE', '2026-04-01 00:00:00')
    monkeypatch.setenv('DOMEYE_DATA_SNAPSHOT_TIME', '2026-03-31 23:59:59')
    with patch('web.api.features.api.' + service, return_value={'ok': True}) as query:
        response = client.get('/api/v1/features/' + path, query_string={
            'start_time': '2026-03-31 00:00:00', 'end_time': '2026-04-01 00:00:00', 'asn': '64500',
        })
    assert response.status_code == 200
    query.assert_called_once()


class QueryConnection:
    """用内存 SQL 执行国家聚合，不把 PostgreSQL 语句断言当业务验证。"""
    closed = 1

    def __init__(self):
        self.database = duckdb.connect(':memory:')

    def cursor(self, **kwargs):
        connection = self

        class Cursor:
            def execute(self, sql, parameters):
                self.result = connection.database.execute(sql.replace('%s', '?'), parameters)

            def fetchall(self):
                keys = [item[0] for item in self.result.description]
                return [dict(zip(keys, row)) for row in self.result.fetchall()]

            def close(self):
                pass

        return Cursor()

    def rollback(self):
        pass


def test_country_sql_excludes_end_retains_missing_previous_and_first_tie(monkeypatch):
    connection = QueryConnection()
    table = country_workbench.FEATURE_COUNTRY_TABLE
    start = datetime(2026, 2, 4)
    connection.database.execute(f'CREATE TABLE {table} (t TIMESTAMP, country VARCHAR, announ_num BIGINT, withdraw_num BIGINT, v4prefix_num BIGINT, v6prefix_num BIGINT, v4ip_num BIGINT, source VARCHAR)')
    connection.database.executemany(f'INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?)', [
        (start, '测试国', 8, 2, 10, 2, 2560, country_workbench.SOURCE),
        (start + timedelta(minutes=5), '测试国', 8, 2, 12, 2, 3072, country_workbench.SOURCE),
        (start + timedelta(minutes=10), '测试国', 1000, 0, 1000, 2, 256000, country_workbench.SOURCE),
    ])
    monkeypatch.setattr(country_workbench, 'if_table_exist', lambda *args: True)
    try:
        rows = country_workbench.get_country_feature_aggregates(connection, start-timedelta(minutes=10), start, start+timedelta(minutes=10))
        assert len(rows) == 1
        row = rows[0]
        assert row['announce'] == 16 and row['withdraw'] == 4
        assert row['sample_count'] == 2 and row['previous_sample_count'] == 0
        assert row['previous_announce'] is row['previous_withdraw'] is None
        assert row['peak_time'] == start
        assert row['ipv4_prefixes'] == 12
    finally:
        connection.database.close()


def test_asn_empty_previous_window_is_not_zero(monkeypatch):
    start = datetime(2026, 2, 4)
    frame = pd.DataFrame([{'asn': '64500', 't': start, 'announce': 0, 'withdraw': 0,
                           'v4Prefix_num': 2, 'v6Prefix_num': 1, 'v4IP_num': 512}])
    monkeypatch.setattr(asn_workbench, 'select_as_list_feature_db', lambda *args: frame)
    rows = asn_workbench.get_as_feature_aggregates(None, {'feature_other': ['64500']}, start-timedelta(minutes=10), start, start+timedelta(minutes=10))
    assert rows[0]['previous_announce'] is rows[0]['previous_withdraw'] is None
    assert rows[0]['previous_sample_count'] == 0
    assert rows[0]['announce'] == rows[0]['withdraw'] == 0
