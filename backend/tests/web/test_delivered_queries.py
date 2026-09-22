"""完成文件的公开 HTTP 查询；合成记录验证窗口与数量的业务含义。"""
import copy
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from data_pipeline.results import delivery_read


URL = '/api/v1/core-overview'


def event(number, kind='prefix_outage', at='2026-02-24T08:10:00+08:00', family='ipv4'):
    return {
        'reference': f'{kind}/2026-02-24 08:10:00/192.0.2.0-24/{number}/r',
        'content_version': f'fixture-{number}', 'kind': kind, 'object': '192.0.2.0/24',
        'start_time': at, 'end_time': {'state': 'unknown', 'value': None},
        'level': 'low', 'address_family': family, 'asns': [], 'record_number': str(number),
    }


def interval(start='2026-02-24T08:00:00+08:00', end='2026-02-24T11:35:00+08:00'):
    return {'start': start, 'end_exclusive': end}


@pytest.fixture
def delivery(monkeypatch):
    meta = {
        'state': 'available', 'version': 'delivery_fixture', 'files': 44, 'updates': 43,
        **interval(), 'intervals': [interval()], 'coverage': 'partial_window',
        'archive': 'paused_by_user', 'binding': {'source_run': 'fixture', 'collector': 'rrc25'},
        'rejected': 0, 'unsupported': 0, 'limitations': ['合成测试数据'],
    }
    state = {'meta': meta, 'items': [], 'errors': 0, 'queries': []}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, query, params):
            state['queries'].append((query, params))
            assert query.startswith('SELECT ')
            self.query = query

        def fetchall(self):
            return [(copy.deepcopy(item),) for item in state['items']]

        def fetchone(self):
            return (state['errors'],)

    class Connection:
        cursor = Cursor

    monkeypatch.setenv('DOMEYE_RESULT_DELIVERY', 'true')
    monkeypatch.setattr(delivery_read, 'status', lambda: copy.deepcopy(meta))
    monkeypatch.setattr(delivery_read, 'conn_11', Connection())
    monkeypatch.setattr(delivery_read, 'read_rib_statistics', lambda start, end: None)
    return state


def validate(payload, name):
    contract = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_text())
    Draft202012Validator({
        '$ref': f'#/components/schemas/{name}', 'components': contract['components'],
    }).validate(payload)


def test_six_event_counts_are_distinct_from_unique_prefixes_and_list_filters(delivery, client):
    delivery['items'] = [event(1), event(2)] + [
        event(i + 3, kind=kind) for i, kind in enumerate(delivery_read.KINDS[1:])
    ] + [event(20, at='2026-02-24T11:35:00+08:00')]
    response = client.get(URL, query_string={'date': '2026-02-24', 'kind': 'leak', 'q': 'missing'})
    assert response.status_code == 200
    payload = response.get_json()
    assert payload['events']['total'] == 0
    assert payload['overview']['record_count'] == 7
    assert payload['trend']['buckets'][0]['value'] == 1
    trends = payload['event_trends']
    assert trends['state'] == 'available'
    assert {item['kind']: item['total'] for item in trends['series']} == {
        kind: 2 if kind == 'prefix_outage' else 1 for kind in delivery_read.KINDS
    }
    buckets = trends['series'][0]['buckets']
    assert [item['value'] for item in buckets] == [2, 0, 0, 0]
    assert buckets[-1]['end_exclusive'] == '2026-02-24T11:35:00+08:00'
    assert len(buckets) == 4
    assert response.headers['X-Domeye-Result-Version'] == 'delivery_fixture'
    validate(payload, 'CoreOverviewPayload')


def test_unknown_hours_and_undelivered_gaps_do_not_become_zero(client, delivery):
    delivery['meta']['intervals'] = [
        interval('2026-02-24T08:05:00+08:00', '2026-02-24T08:15:00+08:00'),
        interval('2026-02-24T10:50:00+08:00', '2026-02-24T11:35:00+08:00'),
    ]
    delivery['items'] = [event(1), event(2, at='2026-02-24T09:00:00+08:00')]
    payload = client.get(URL, query_string={'date': '2026-02-24'}).get_json()
    buckets = payload['event_trends']['series'][0]['buckets']
    assert [(item['start'][11:16], item['end_exclusive'][11:16], item['value']) for item in buckets] == [
        ('08:05', '08:15', 1), ('10:50', '11:00', 0), ('11:00', '11:35', 0),
    ]
    assert payload['overview']['record_count'] == 1
    for query, params in delivery['queries']:
        assert ' OR ' in query
        assert params == ('2026-02-24 08:05:00', '2026-02-24 08:15:00',
                          '2026-02-24 10:50:00', '2026-02-24 11:35:00')
    delivery['queries'].clear()
    payload = client.get(URL, query_string={'date': '2026-02-24', 'hour': '9'}).get_json()
    assert payload['state'] == 'window_not_retained'
    assert payload['events'] is None
    assert not delivery['queries']


def test_family_selection_applies_to_all_six_series_without_adding_mixed_twice(client, delivery):
    delivery['items'] = [event(i, family=family) for i, family in enumerate(['ipv4', 'ipv6', 'mixed', 'unknown'])]
    for family, count in [('all', 4), ('ipv4', 2), ('ipv6', 2), ('unknown', 1)]:
        payload = client.get(URL, query_string={'date': '2026-02-24', 'family': family}).get_json()
        assert payload['event_trends']['series'][0]['total'] == count
        assert payload['overview']['record_count'] == count
    assert payload['query']['excluded_unknown_family'] == 0


def test_projection_failure_is_not_a_successful_zero_trend(client, delivery):
    delivery['errors'] = 1
    payload = client.get(URL, query_string={'date': '2026-02-24'}).get_json()
    assert payload['metadata']['projection_unavailable_records'] == 1
    assert payload['event_trends']['state'] == 'unavailable'
    assert payload['event_trends']['series'] == []
    validate(payload, 'CoreOverviewPayload')


def test_date_directory_and_midnight_boundaries_use_actual_intervals(client, delivery):
    delivery['meta'].update(start='2026-02-24T23:50:00+08:00', end_exclusive='2026-02-28T00:00:00+08:00')
    delivery['meta']['intervals'] = [
        interval('2026-02-24T23:50:00+08:00', '2026-02-25T00:05:00+08:00'),
        interval('2026-02-27T23:55:00+08:00', '2026-02-28T00:00:00+08:00'),
    ]
    delivery['items'] = [event(1, at='2026-02-25T00:00:00+08:00')]
    payload = client.get(URL, query_string={'date': '2026-02-25'}).get_json()
    assert payload['metadata']['available_dates'] == ['2026-02-24', '2026-02-25', '2026-02-27']
    assert payload['event_trends']['series'][0]['buckets'] == [
        {'start': '2026-02-25T00:00:00+08:00', 'end_exclusive': '2026-02-25T00:05:00+08:00', 'value': 1},
    ]
    assert client.get(URL, query_string={'date': '2026-02-26'}).get_json()['state'] == 'window_not_retained'


def test_single_rib_point_does_not_establish_activity_coverage():
    at = datetime.fromisoformat('2026-02-24T08:00:00+08:00')
    assert delivery_read._merge_intervals([(at, at)]) == []


@pytest.mark.parametrize('source_start,source_end,day,expected', [
    ('2026-02-24T00:00:00+00:00', '2026-02-24T03:35:00+00:00', '2026-02-24', [
        ('2026-02-24T08:00:00+08:00', '2026-02-24T09:00:00+08:00'),
        ('2026-02-24T09:00:00+08:00', '2026-02-24T10:00:00+08:00'),
        ('2026-02-24T10:00:00+08:00', '2026-02-24T11:00:00+08:00'),
        ('2026-02-24T11:00:00+08:00', '2026-02-24T11:35:00+08:00'),
    ]),
    ('2026-02-24T15:55:00+00:00', '2026-02-24T16:05:00+00:00', '2026-02-25', [
        ('2026-02-25T00:00:00+08:00', '2026-02-25T00:05:00+08:00'),
    ]),
])
def test_utc_delivery_intervals_return_business_timezone_buckets(delivery, client, source_start, source_end, day, expected):
    delivery['meta'].update(start=source_start, end_exclusive=source_end,
                            intervals=[interval(source_start, source_end)])
    first = (datetime.fromisoformat(expected[0][0]) + timedelta(seconds=1)).isoformat()
    delivery['items'] = [event(1, at=first), event(2, at=source_end)]
    payload = client.get(URL, query_string={'date': day}).get_json()
    groups = [payload['trend']['buckets'], *[series['buckets'] for series in payload['event_trends']['series']]]
    for buckets in groups:
        assert [(item['start'], item['end_exclusive']) for item in buckets] == expected
    assert payload['overview']['record_count'] == 1  # 排除源窗口右端点。
    assert payload['event_trends']['series'][0]['total'] == 1
    assert payload['metadata']['result_delivery']['intervals'] == [interval(source_start, source_end)]


def test_result_version_tracks_rib_delivery_and_preserves_old_database_identity():
    at = datetime.fromisoformat('2026-02-24T08:00:00+08:00')
    state = {'versions': None}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, query):
            assert query.startswith('SELECT ')
            self.query = query

        def fetchone(self):
            if 'SELECT body' in self.query:
                return ({'collector': 'rrc25', 'source_run': 'fixture'},)
            return (None if state['versions'] is None else 'result_delivery.rib_statistics',)

        def fetchall(self):
            if 'SELECT snapshot_id' in self.query:
                return [(value,) for value in state['versions']]
            return [(0, 'fixture', at, at, {'rejected': 0, 'unsupported': 0})]

    class Connection:
        cursor = Cursor

    conn = Connection()
    old = delivery_read.status(conn)
    assert old['intervals'] == []
    state['versions'] = []
    assert delivery_read.status(conn)['version'] == old['version']
    state['versions'] = ['rib_statistics_v1_fixture']
    new = delivery_read.status(conn)['version']
    assert new != old['version']
    assert delivery_read.status(conn)['version'] == new


def test_rib_read_uses_bounded_sql_and_never_initializes_old_database(monkeypatch):
    state = {'exists': False, 'queries': []}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, query, params=None):
            assert query.startswith('SELECT ')
            state['queries'].append((query, params))

        def fetchone(self):
            return ('result_delivery.rib_statistics' if state['exists'] else None,)

        def fetchall(self):
            return [({'snapshot_id': 'fixture'},)]

    class Connection:
        cursor = Cursor

    monkeypatch.setattr(delivery_read, 'conn_11', Connection())
    start = datetime.fromisoformat('2026-02-24T08:00:00+08:00')
    end = datetime.fromisoformat('2026-02-24T11:35:00+08:00')
    assert delivery_read.read_rib_statistics(start, end) is None
    assert len(state['queries']) == 1
    state['exists'] = True
    assert delivery_read.read_rib_statistics(start, end) == [{'snapshot_id': 'fixture'}]
    query, params = state['queries'][-1]
    assert 'observed_at >= %s AND observed_at < %s ORDER BY observed_at, snapshot_id' in query
    assert params == (start, end)
