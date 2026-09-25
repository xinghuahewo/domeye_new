"""经过实际时序入口核对统计值、时点与缺口；不读取真实数据库。"""
from datetime import datetime
import json
from pathlib import Path

from jsonschema import Draft202012Validator
import pytest

from tests.web.test_country_feature_series_api import source, row, QUERY, URL


def validate(payload, name):
    contract = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_text())
    Draft202012Validator({'$ref': '#/components/schemas/' + name,
                         'components': contract['components']}).validate(payload)


def test_feature_summary_keeps_real_points_and_unknown_activity(source, client):
    source['rows'] = [row(0, announce=8, withdraw=0, ipv4_addresses=256),
                      row(5, announce=2, withdraw=0, ipv4_addresses=512),
                      row(15, announce=4, withdraw=None, ipv4_addresses=None)]
    raw = client.get(URL, query_string=QUERY).get_json()
    source['queries'].clear()
    response = client.get(URL, query_string={**QUERY, 'summary_seconds': 600})
    assert response.status_code == 200
    body = response.get_json()
    assert len(source['queries']) == 1
    assert {key: value for key, value in body.items() if key != 'summary'} == raw
    first, gap, empty = body['summary']['buckets']
    ip = first['metrics']['ipv4_addresses']
    assert ip['first'] == {'value': 256, 'at': '2026-03-01T19:05:00+08:00'}
    assert ip['last'] == {'value': 512, 'at': '2026-03-01T19:10:00+08:00'}
    assert ip['first_to_last'] == {'from': ip['first'], 'to': ip['last'], 'delta': 256, 'percent_of_first': 100}
    assert ip['known_sample_mean'] == 384
    assert 'total' not in ip
    assert first['metrics']['announce']['total'] == 10
    assert first['metrics']['withdraw']['total'] == 0
    assert gap['coverage']['state'] == 'partial'
    assert gap['metrics']['announce']['known_window_sum'] == 4
    assert gap['metrics']['announce']['total'] is None
    assert gap['metrics']['withdraw']['known_window_sum'] is None
    assert gap['metrics']['ipv4_addresses']['first']['value'] is None
    assert gap['metrics']['ipv4_addresses']['first_to_last'] is None
    assert empty['metrics']['announce']['total'] is None
    assert empty['metrics']['ipv4_addresses']['last'] is None
    assert empty['end_exclusive'] == QUERY['end_time'].replace(' ', 'T') + '+08:00'
    validate(body, 'CountryFeatureSeriesPayload')


def test_unaligned_windows_exclude_cross_boundary_activity_but_keep_in_window_states(source, client):
    source['meta']['intervals'] = [{'start': source['meta']['start'],
                                  'end_exclusive': source['meta']['end_exclusive']}]
    source['rows'] = [row(0, announce=40), row(5, announce=12, withdraw=1),
                      row(10, announce=6, withdraw=0)]
    body = client.get(URL, query_string={**QUERY, 'start_time': '2026-03-01 19:01:00',
                         'end_time': '2026-03-01 19:12:00', 'summary_seconds': 660}).get_json()
    bucket = body['summary']['buckets'][0]
    activity = bucket['metrics']['announce']
    assert bucket['coverage']['state'] == 'complete'
    assert activity['source_intervals'] == [{'start': '2026-03-01T19:05:00+08:00',
                                            'end_exclusive': '2026-03-01T19:10:00+08:00'}]
    assert activity['known_window_sum'] == 12
    assert activity['total'] is None
    resource = bucket['metrics']['ipv4_addresses']
    assert resource['first']['at'] == '2026-03-01T19:05:00+08:00'
    assert resource['last']['at'] == '2026-03-01T19:10:00+08:00'
    assert len(body['data']['activity']) == 1
    assert [p['at'] for p in body['data']['resources']] == [resource['first']['at'], resource['last']['at']]


def test_complete_query_files_crossing_bucket_edges_are_not_prorated(source, client):
    source['meta']['intervals'] = [{'start': source['meta']['start'], 'end_exclusive': source['meta']['end_exclusive']}]
    source['rows'] = [row(0, announce=20), row(5, announce=40), row(10, announce=80)]
    body = client.get(URL, query_string={**QUERY, 'start_time': '2026-03-01 19:02:00',
                         'end_time': '2026-03-01 19:12:00', 'summary_seconds': 300}).get_json()
    assert [p['announce'] for p in body['data']['activity']] == [40]
    first, second = body['summary']['buckets']
    assert first['metrics']['ipv4_addresses']['last']['at'] == '2026-03-01T19:05:00+08:00'
    assert second['metrics']['ipv4_addresses']['last']['at'] == '2026-03-01T19:10:00+08:00'
    for bucket in (first, second):
        assert bucket['metrics']['announce']['known_window_sum'] is None
        assert bucket['metrics']['announce']['total'] is None


def test_short_window_can_have_a_closing_resource_without_activity(source, client):
    source['rows'] = [row(0, announce=20)]
    body = client.get(URL, query_string={**QUERY, 'start_time': '2026-03-01 19:03:00',
                         'end_time': '2026-03-01 19:05:00', 'summary_seconds': 120}).get_json()
    assert body['data']['activity'] == []
    resource = body['summary']['buckets'][0]['metrics']['ipv4_addresses']
    assert resource['first'] == {'at': '2026-03-01T19:05:00+08:00', 'value': 2048}
    assert resource['first_to_last'] is None
    assert body['summary']['buckets'][0]['metrics']['announce']['total'] is None


def test_missing_last_resource_is_not_replaced_by_last_known(source, client):
    source['rows'] = [row(0), row(5, ipv4_addresses=None)]
    body = client.get(URL, query_string={**QUERY, 'summary_seconds': 600}).get_json()
    metric = body['summary']['buckets'][0]['metrics']['ipv4_addresses']
    assert metric['last'] == {'value': None, 'at': '2026-03-01T19:10:00+08:00'}
    assert metric['known_sample_count'] == 1
    assert metric['known_sample_mean'] == 2048
    assert metric['first_to_last'] is None


@pytest.mark.parametrize('value', ['', '0', '-1', '60.5', 'x', '86401', ['600', '900']])
def test_summary_parameter_rejected_before_read(source, client, value):
    assert client.get(URL, query_string={**QUERY, 'summary_seconds': value}).status_code == 400
    assert not source['queries']


def test_overlapping_activity_is_not_double_counted_and_zero_baseline_has_no_percent(source, client):
    source['rows'] = [row(0, announce=3, ipv4_addresses=0,
                          source_end=datetime.fromisoformat('2026-03-01T19:07:00+08:00')),
                      row(5, announce=5, ipv4_addresses=256)]
    body = client.get(URL, query_string={**QUERY, 'summary_seconds': 600}).get_json()
    metrics = body['summary']['buckets'][0]['metrics']
    assert metrics['announce']['overlapping_windows'] is True
    assert metrics['announce']['known_window_sum'] is None
    assert metrics['announce']['total'] is None
    assert metrics['ipv4_addresses']['first_to_last']['delta'] == 256
    assert metrics['ipv4_addresses']['first_to_last']['percent_of_first'] is None


def test_equal_resource_times_cannot_describe_a_temporal_change(source, client):
    source['rows'] = [row(0, ipv4_addresses=0,
                          source_end=datetime.fromisoformat('2026-03-01T19:10:00+08:00')),
                      row(5, ipv4_addresses=256)]
    body = client.get(URL, query_string={**QUERY, 'summary_seconds': 600}).get_json()
    assert body['summary']['buckets'][0]['metrics']['ipv4_addresses']['first_to_last'] is None


def test_outage_summary_uses_existing_observation_states_and_point_times(source, client, monkeypatch):
    from data_pipeline.results import delivery_read
    monkeypatch.setattr(delivery_read, 'read_outage_intervals', lambda *a, **kw: [
        {'asn': '64501', 's_time': datetime.fromisoformat('2026-03-01T19:00:00+08:00'), 'e_time': None},
        {'asn': '64502', 's_time': datetime.fromisoformat('2026-03-01T19:06:00+08:00'), 'e_time': None},
    ])
    response = client.get('/api/v1/features/outages/country-as',
                          query_string={**QUERY, 'summary_seconds': 600})
    assert response.status_code == 200
    body = response.get_json()
    first, gap, _ = body['summary']['buckets']
    metric = first['metrics']['outage_count']
    assert metric['unit'] == 'asn'
    assert metric['first']['at'] == '2026-03-01T19:00:00+08:00'
    assert metric['last']['at'] == '2026-03-01T19:09:00+08:00'
    assert metric['first_to_last']['delta'] == 1
    assert metric['known_sample_mean'] == 1.5
    assert metric['known_sample_maximum'] == {'value': 2, 'at': '2026-03-01T19:06:00+08:00'}
    assert gap['metrics']['outage_count']['known_sample_count'] == 0
    assert gap['metrics']['outage_count']['known_sample_mean'] is None
    assert gap['metrics']['outage_count']['first_to_last'] is None
    validate(body, 'OutageSeriesPayload')


def test_read_failure_and_version_conflict_never_get_statistics(source, client):
    source['fail'] = True
    response = client.get(URL, query_string={**QUERY, 'summary_seconds': 600})
    assert response.status_code == 503 and 'summary' not in response.get_json()
    response = client.get(URL, query_string={**QUERY, 'summary_seconds': 600, 'version': 'old'})
    assert response.status_code == 409 and 'summary' not in response.get_json()
