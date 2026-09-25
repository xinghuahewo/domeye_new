"""中断时序的业务边界：未处理时点不能从空结束时间推算。"""
from copy import deepcopy

import pytest

from data_pipeline.results import delivery_read
from services import features_service


@pytest.fixture
def completed_outages(monkeypatch):
    meta = {
        'state': 'available', 'version': 'delivery_test',
        'start': '2026-02-27T08:00:00+08:00',
        'end_exclusive': '2026-03-01T19:20:00+08:00',
        'intervals': [{'start': '2026-02-27T08:00:00+08:00',
                       'end_exclusive': '2026-03-01T19:20:00+08:00'}],
        'binding': {'collector': 'rrc25'},
    }
    monkeypatch.setenv('DOMEYE_RESULT_DELIVERY', 'true')
    monkeypatch.setattr(delivery_read, 'status', lambda *args, **kwargs: deepcopy(meta))
    monkeypatch.setattr(delivery_read, 'available_countries', lambda *args, **kwargs: ['测试地区'])
    return meta


def test_open_outage_does_not_extend_past_processed_data(completed_outages, client):
    response = client.get('/api/v1/features/outages/country-as', query_string={
        'country': '测试地区', 'start_time': '2026-03-01 21:00:00',
        'end_time': '2026-03-01 22:00:00',
    })
    assert response.status_code == 200
    body = response.get_json()
    points = body if isinstance(body, list) else body['data']
    assert all(point['outage_count'] is None for point in points)
    assert len(points) == 20


@pytest.fixture
def outage_rows(monkeypatch):
    from config.database import LazyConnection
    from datetime import datetime
    import psycopg2

    state = {'rows': [], 'queries': [], 'fail': False}

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, query, params):
            assert query.startswith('SELECT DISTINCT data->>')
            assert 'FROM result_delivery.events' in query
            assert '2026' not in query and 'event_list' not in query
            if state['fail']:
                raise psycopg2.OperationalError('fixture 读取失败')
            state['queries'].append((query, params))
            self.query, self.params = query, params
        def fetchall(self):
            field, kind, source, end, start, *selectors = self.params
            rows = []
            for row in state['rows']:
                if row.get('kind', 'as_outage') != kind: continue
                if "data->>'country'=%s" in self.query and row.get('country', '测试地区') != selectors[0]: continue
                if "data->>'asn'=%s" in self.query and row.get('asn') != selectors[-1]: continue
                started = datetime.fromisoformat(row['s_time'])
                ended = datetime.fromisoformat(row['e_time']) if row.get('e_time') else None
                if started < end and (ended is None or ended > start):
                    rows.append((row[field], started, ended, 'e_time' in row))
            return rows

    class Connection:
        cursor = Cursor

    monkeypatch.setattr(LazyConnection, 'get_connection', lambda self: Connection())
    monkeypatch.setattr(features_service.data_loader, '_core_data_loaded', True)
    monkeypatch.setattr(features_service.data_loader, 'prefix_info', {'192.0.2.0/24': {}, '198.51.100.0/24': {}})
    return state


def get_curve(client, path='country-as', start='2026-03-01 19:15:00', end='2026-03-01 19:27:00', **params):
    if path.startswith('country-'): params.setdefault('country', '测试地区')
    return client.get('/api/v1/features/outages/' + path, query_string={
        'start_time': start, 'end_time': end, **params,
    })


def validate(payload):
    import json
    from pathlib import Path
    from jsonschema import Draft202012Validator, FormatChecker
    contract = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_text())
    Draft202012Validator({'$ref': '#/components/schemas/OutageSeriesPayload',
                         'components': contract['components']}, format_checker=FormatChecker()).validate(payload)


@pytest.mark.parametrize('path,kind,field,unit,params', [
    ('country-as', 'as_outage', 'asn', 'asn', {}),
    ('global-as', 'as_outage', 'asn', 'asn', {}),
    ('country-prefix', 'prefix_outage', 'prefix', 'prefix', {}),
    ('global-prefix', 'prefix_outage', 'prefix', 'prefix', {}),
    ('as-prefix', 'prefix_outage', 'prefix', 'prefix', {'asn': 'AS64501'}),
])
def test_five_series_share_coverage_distinct_counts_and_previous_month_events(
    completed_outages, outage_rows, client, path, kind, field, unit, params,
):
    identifier = '64501' if field == 'asn' else '192.0.2.0/24'
    outage_rows['rows'] = [
        {'kind': kind, 'asn': '64501', field: identifier, 's_time': '2026-02-28 09:00:00', 'e_time': None},
        {'kind': kind, 'asn': '64501', field: identifier, 's_time': '2026-03-01 19:14:00', 'e_time': '2026-03-01 19:18:00'},
        {'kind': kind, 'asn': '64501', field: identifier, 's_time': '2026-03-01 19:27:00', 'e_time': None},
    ]
    response = get_curve(client, path, **params)
    assert response.status_code == 200
    body = response.get_json(); validate(body)
    assert [point['outage_count'] for point in body['data']] == [1, 1, None, None]
    assert [point['observation_state'] for point in body['data']] == ['observed'] * 2 + ['not_observed'] * 2
    assert body['metadata']['coverage']['state'] == 'partial'
    assert body['metadata']['unit'] == unit
    assert body['metadata']['version'] == response.headers['X-Domeye-Result-Version']
    assert body['query']['end_exclusive'] == '2026-03-01T19:27:00+08:00'
    assert len(outage_rows['queries']) == 1


@pytest.mark.parametrize('path,kind,field,identifier', [
    ('country-as', 'as_outage', 'asn', '64501'),
    ('country-prefix', 'prefix_outage', 'prefix', '192.0.2.0/24'),
])
def test_recorded_end_is_exclusive_and_covered_empty_is_zero(
    completed_outages, outage_rows, client, path, kind, field, identifier,
):
    outage_rows['rows'] = [{'kind': kind, field: identifier, 's_time': '2026-02-28 10:00:00', 'e_time': '2026-03-01 19:18:00'}]
    body = get_curve(client, path).get_json()
    assert [point['outage_count'] for point in body['data']] == [1, 0, None, None]
    outage_rows['rows'] = []
    body = get_curve(client, path).get_json()
    assert [point['outage_count'] for point in body['data']] == [0, 0, None, None]


def test_gap_stays_unknown_and_old_open_state_is_not_carried_over(completed_outages, outage_rows, client):
    completed_outages['intervals'] = [
        {'start': completed_outages['start'], 'end_exclusive': '2026-03-01T19:03:00+08:00'},
        {'start': '2026-03-01T19:06:00+08:00', 'end_exclusive': completed_outages['end_exclusive']},
    ]
    outage_rows['rows'] = [{'asn': '64501', 's_time': '2026-02-28 10:00:00', 'e_time': '2026-03-01 19:12:00'}]
    body = get_curve(client, start='2026-03-01 19:00:00', end='2026-03-01 19:15:00').get_json()
    validate(body)
    assert [point['outage_count'] for point in body['data']] == [1, None, None, None, 0]
    assert [point['observation_state'] for point in body['data']] == ['observed', 'not_observed', 'unknown', 'unknown', 'observed']
    assert len(body['metadata']['coverage']['intervals']) == 2


def test_prefix_population_filter_and_end_precedence_are_preserved(completed_outages, outage_rows, client):
    outage_rows['rows'] = [
        {'kind': 'prefix_outage', 'prefix': prefix, 's_time': '2026-03-01 19:00:00', 'e_time': end}
        for prefix, end in [('192.0.2.0/24', None), ('192.0.2.0/24', '2026-03-01 19:18:00'), ('192.0.2.0/25', None)]
    ]
    body = get_curve(client, 'country-prefix').get_json()
    assert [point['outage_count'] for point in body['data']] == [1, 0, None, None]
    assert body['metadata']['population'] == 'coarse_routing_prefixes'


def test_failure_is_not_an_empty_success(completed_outages, outage_rows, client):
    outage_rows['fail'] = True
    response = get_curve(client)
    assert response.status_code == 503
    assert 'data' not in response.get_json()


def test_missing_end_field_does_not_mean_ongoing(completed_outages, outage_rows, client):
    outage_rows['rows'] = [{'asn': '64501', 's_time': '2026-02-28 10:00:00'}]
    assert get_curve(client).status_code == 503


def test_prefix_filter_unavailable_does_not_mean_zero(completed_outages, outage_rows, client, monkeypatch):
    monkeypatch.setattr(features_service.data_loader, 'prefix_info', {})
    assert get_curve(client, 'country-prefix').status_code == 503


def test_unreadable_coverage_does_not_mean_unobserved(completed_outages, outage_rows, client):
    completed_outages['state'] = 'unavailable'
    assert get_curve(client).status_code == 503
    assert not outage_rows['queries']


def test_version_conflict_is_reported_before_reading_events(completed_outages, outage_rows, client):
    assert get_curve(client, version='previous').status_code == 409
    assert not outage_rows['queries']


@pytest.mark.parametrize('start,end', [
    ('2026-03-01 19:00:00', '2026-03-01 19:00:00'),
    ('2026-02-28 18:00:00', '2026-03-01 19:00:00'),
])
def test_invalid_window_is_rejected(completed_outages, client, start, end):
    assert get_curve(client, start=start, end=end).status_code == 400


def test_coverage_before_request_uses_same_bound_status(completed_outages, outage_rows, client, monkeypatch):
    calls = []
    def status(*args, **kwargs):
        calls.append(True)
        return deepcopy(completed_outages)
    monkeypatch.setattr(delivery_read, 'status', status)
    assert get_curve(client).status_code == 200
    assert len(calls) == 1


@pytest.mark.parametrize('params', [
    [('country', '测试地区'), ('country', '另一地区')],
    [('country', '')], [('country', '测试地区'), ('unexpected', '1')],
])
def test_invalid_selectors_cannot_fall_through_to_global(completed_outages, client, params):
    response = client.get('/api/v1/features/outages/country-as', query_string=params + [
        ('start_time', '2026-03-01 19:00:00'), ('end_time', '2026-03-01 19:15:00'),
    ])
    assert response.status_code == 400


@pytest.mark.parametrize('path', ['country-as', 'country-prefix'])
@pytest.mark.parametrize('country', ['XX', 'Example', '测试地', 'collect'])
def test_unknown_country_is_not_a_covered_zero_series(completed_outages, outage_rows, client, path, country):
    response = get_curve(client, path, country=country)
    assert response.status_code == 400
    assert 'data' not in response.get_json()
    assert 'metadata.countries' in response.get_json()['msg']
    assert not outage_rows['queries']


@pytest.mark.parametrize('path', ['country-as', 'country-prefix'])
def test_country_catalog_failure_cannot_produce_zero(completed_outages, outage_rows, client, monkeypatch, path):
    import psycopg2
    def unavailable(*args, **kwargs):
        raise psycopg2.OperationalError('合成国家目录读取失败')
    monkeypatch.setattr(delivery_read, 'available_countries', unavailable)
    assert get_curve(client, path).status_code == 503
    assert not outage_rows['queries']


def test_prefix_http_reads_membership_without_loading_unrelated_assets(completed_outages, outage_rows, client, monkeypatch, tmp_path):
    from utils import data_loader
    path = tmp_path / 'prefixes.csv'
    path.write_text('prefix,name\n192.0.2.0/24,示例\n192.0.2.0/24,重复\n198.51.100.0/24,另一前缀\n')
    monkeypatch.setattr(data_loader, 'PREFIX_INFO_FILE', str(path))
    monkeypatch.setattr(data_loader, '_core_data_loaded', False)
    def unrelated():
        raise AssertionError('前缀成员查询不需要 AS、域名或国家资料')
    monkeypatch.setattr(data_loader, 'ensure_core_data_loaded', unrelated)
    outage_rows['rows'] = [
        {'kind': 'prefix_outage', 'prefix': prefix, 's_time': '2026-03-01 19:00:00', 'e_time': None}
        for prefix in ['192.0.2.0/24', '192.0.2.0/25']
    ]
    response = get_curve(client, 'country-prefix')
    assert response.status_code == 200
    assert [p['outage_count'] for p in response.get_json()['data']] == [1, 1, None, None]


def test_prefix_queries_reuse_membership_without_rescanning_the_country_independent_catalog(
    completed_outages, outage_rows, client, monkeypatch,
):
    from collections.abc import Set

    class Membership(Set):
        scans = 0
        values = frozenset(['192.0.2.0/24', '198.51.100.0/24'])
        def __contains__(self, value): return value in self.values
        def __len__(self): return len(self.values)
        def __iter__(self):
            self.scans += 1
            return iter(self.values)

    members = Membership()
    monkeypatch.setattr(features_service.data_loader, 'coarse_routing_prefixes', lambda: members)
    outage_rows['rows'] = [
        {'kind': 'prefix_outage', 'prefix': prefix, 's_time': '2026-03-01 19:00:00', 'e_time': None}
        for prefix in ['192.0.2.0/24', '192.0.2.0/25']
    ]
    for _ in range(2):
        response = get_curve(client, 'country-prefix')
        assert response.status_code == 200
        assert [p['outage_count'] for p in response.get_json()['data']] == [1, 1, None, None]
    assert members.scans == 0
