"""独立 RIB 的 HTTP 主值、时间边界及 Core 规模来源回归。"""
import copy
from datetime import datetime

import psycopg2
import pytest

from data_pipeline.results import delivery_read
from services.resource_service import METRIC_UNITS
from tests.web.test_delivered_queries import delivery, event, validate


WINDOW = {'start_time': '2026-02-24 08:00:00', 'end_time': '2026-02-24 11:35:00'}


@pytest.fixture
def rib(delivery, monkeypatch):
    row = {
        'snapshot_id': 'rib_statistics_v1_' + 'a' * 64, 'observed_at': '2026-02-24T00:00:00+00:00',
        'collector_id': 'rrc25', 'source_id': 'fixture-rib', 'source_sha256': 'b' * 64,
        'canonical': {'rule': 'fixture-canonical', 'by_family': {
            'all': {'visible_prefixes': 21, 'visible_origin_ases': 2},
            'ipv4': {'visible_prefixes': 20, 'visible_origin_ases': 2},
            'ipv6': {'visible_prefixes': 1, 'visible_origin_ases': 1},
        }},
        'resource': {'rule': 'fixture-resource', 'metrics': {
            name: {'main': 89, 'raw': 89, 'qualification': 'qualified', 'reason': 'fixture', 'unit': unit}
            for name, unit in METRIC_UNITS.items()
        }},
        'limitations': ['独立时点，不代表连续状态'],
    }
    state = {'rows': [row], 'calls': []}

    def read(start, end):
        state['calls'].append((start, end))
        return copy.deepcopy([item for item in state['rows'] if start <= datetime.fromisoformat(item['observed_at']) < end])

    monkeypatch.setattr(delivery_read, 'read_rib_statistics', read)
    return state


def test_resource_points_preserve_qualified_zero_unknown_and_units(rib, client):
    cells = rib['rows'][0]['resource']['metrics']
    cells['ipv4_prefix_count'].update(main=0, raw=0)
    cells['ipv6_48_count'].update(main=None, raw=123, qualification='unknown', reason='测试缺失')
    response = client.get('/api/v1/resources', query_string=WINDOW)
    assert response.status_code == 200
    payload = response.get_json()
    assert payload['query']['window_boundary'] == '[start,end)'
    assert len(payload['points']) == 1
    metrics = payload['points'][0]['metrics']
    assert metrics['ipv4_prefix_count']['main'] == 0
    assert metrics['ipv6_48_count']['main'] is None
    assert metrics['ipv4_prefix_count']['unit'] == 'distinct_ipv4_prefix'
    assert metrics['ipv6_48_count']['unit'] == 'covered_ipv6_48_block'
    assert all('raw' not in cell for cell in metrics.values())
    assert payload['points'][0]['metadata']['collector_id'] == 'rrc25'
    validate(payload, 'ResourceStatisticsPayload')


def test_resource_window_is_half_open_and_accepts_explicit_timezone(rib, client):
    before = copy.deepcopy(rib['rows'][0])
    before.update(snapshot_id='rib_statistics_v1_' + 'c' * 64, observed_at='2026-02-23T23:59:59+00:00')
    boundary = copy.deepcopy(before)
    boundary.update(observed_at='2026-02-24T03:35:00+00:00')
    rib['rows'] += [before, boundary]
    query = {'start_time': '2026-02-24T00:00:00Z', 'end_time': '2026-02-24T03:35:00Z'}
    payload = client.get('/api/v1/resources', query_string=query).get_json()
    assert len(payload['points']) == 1
    start, end = rib['calls'][0]
    assert start.isoformat() == '2026-02-24T08:00:00+08:00'
    assert end.isoformat() == '2026-02-24T11:35:00+08:00'
    payload = client.get('/api/v1/resources', query_string={**WINDOW, 'start_time': '2026-02-24 09:00:00'}).get_json()
    assert payload['state'] == 'not_calculated'
    assert payload['points'] == []
    validate(payload, 'ResourceStatisticsPayload')


@pytest.mark.parametrize('query', [
    {}, {'start_time': '2026-02-24 08:00:00'},
    {**WINDOW, 'metric': 'sql'}, {**WINDOW, 'end_time': WINDOW['start_time']},
    {**WINDOW, 'start_time': '2026-02-24'},
    {'start_time': '2026-01-31 23:00:00', 'end_time': '2026-02-01 00:00:00'},
    [('start_time', WINDOW['start_time']), ('start_time', WINDOW['start_time']), ('end_time', WINDOW['end_time'])],
])
def test_resource_invalid_parameters_do_not_query_projection(rib, client, query):
    assert client.get('/api/v1/resources', query_string=query).status_code == 400
    assert rib['calls'] == []


def test_resource_no_binding_and_broken_projection_are_distinct(rib, client, monkeypatch):
    monkeypatch.delenv('DOMEYE_RESULT_DELIVERY')
    assert client.get('/api/v1/resources', query_string=WINDOW).get_json()['state'] == 'not_configured'
    assert rib['calls'] == []
    monkeypatch.setenv('DOMEYE_RESULT_DELIVERY', 'true')
    rib['rows'][0]['resource']['metrics']['public_as_count']['qualification'] = 'unknown'
    response = client.get('/api/v1/resources', query_string=WINDOW)
    assert response.status_code == 503
    assert response.get_json()['state'] == 'unavailable'


def test_core_scale_uses_canonical_family_and_never_resource_tail_asns(rib, client):
    for family, prefixes, origins in [('all', 21, 2), ('ipv4', 20, 2), ('ipv6', 1, 1)]:
        payload = client.get('/api/v1/core-overview', query_string={
            'date': '2026-02-24', 'family': family, 'kind': 'leak', 'level': 'high',
        }).get_json()
        assert payload['overview']['visible_prefixes'] == prefixes
        assert payload['overview']['visible_origin_ases'] == origins
        assert payload['metadata']['rib_statistics']['metrics']['visible_origin_ases'] != 89
        validate(payload, 'CoreOverviewPayload')
    payload = client.get('/api/v1/core-overview', query_string={'date': '2026-02-24', 'family': 'unknown'}).get_json()
    assert payload['metadata']['rib_statistics']['state'] == 'not_applicable'
    assert payload['overview']['visible_prefixes'] is None


@pytest.mark.parametrize('change', [
    {'collector_id': 'rrc00'}, {'observed_at': '2026-02-24T03:35:00+00:00'},
    {'observed_at': '2026-02-23T00:00:00+00:00'},
])
def test_core_does_not_borrow_another_collector_or_time(rib, client, change):
    rib['rows'][0].update(change)
    payload = client.get('/api/v1/core-overview', query_string={'date': '2026-02-24'}).get_json()
    assert payload['metadata']['rib_statistics']['state'] == 'not_calculated'
    assert payload['overview']['visible_prefixes'] is None


def test_resource_read_failure_does_not_erase_delivered_events(rib, delivery, client, monkeypatch):
    def fail(*args):
        raise psycopg2.OperationalError('内部连接详情不得泄露')

    delivery['items'] = [event(1)]
    monkeypatch.setattr(delivery_read, 'read_rib_statistics', fail)
    payload = client.get('/api/v1/core-overview', query_string={'date': '2026-02-24'}).get_json()
    assert payload['state'] == 'available'
    assert payload['events']['total'] == 1
    assert payload['metadata']['rib_statistics']['state'] == 'unavailable'
    assert payload['overview']['visible_origin_ases'] is None
    response = client.get('/api/v1/resources', query_string=WINDOW)
    assert response.status_code == 503
    assert '内部连接详情' not in response.get_data(as_text=True)


def test_latest_rib_point_is_explicit_and_independent_of_list_hour(rib, client):
    later = copy.deepcopy(rib['rows'][0])
    later.update(snapshot_id='rib_statistics_v1_' + 'c' * 64, observed_at='2026-02-24T01:00:00+00:00')
    later['canonical']['by_family']['all']['visible_prefixes'] = 25
    rib['rows'].append(later)
    payload = client.get('/api/v1/core-overview', query_string={'date': '2026-02-24', 'hour': '7'}).get_json()
    assert payload['events'] is None
    assert payload['metadata']['rib_statistics']['snapshot_id'] == later['snapshot_id']
    assert payload['metadata']['rib_statistics']['metrics']['visible_prefixes'] == 25


def test_resource_cross_day_window_reads_existing_points_only(rib, client):
    response = client.get('/api/v1/resources', query_string={**WINDOW, 'end_time': '2026-03-01 00:00:00'})
    assert response.status_code == 200
    payload = response.get_json(); validate(payload, 'ResourceStatisticsPayload')
    assert len(payload['points']) == 1
    assert payload['query']['end_exclusive'] == '2026-03-01T00:00:00+08:00'
