"""公共比较入口保留测量范围和总体；使用合成数据经过真实读取与统计路径。"""
import json
from pathlib import Path

import psycopg2
import pytest

from data_pipeline.results import delivery_read
from services import features_service
from tests.web.test_country_feature_series_api import source, row
from tests.web.test_series_statistics_api import validate

URL = '/api/v1/features/countries/comparison'
QUERY = {'country': '测试地区', 'reference_start_time': '2026-03-01 19:02:00',
         'reference_end_time': '2026-03-01 19:12:00',
         'start_time': '2026-03-01 19:15:00', 'end_time': '2026-03-01 19:25:00'}


@pytest.fixture
def series(source, monkeypatch):
    source['meta']['end_exclusive'] = '2026-03-01T19:30:00+08:00'
    source['meta']['intervals'] = [{'start': source['meta']['start'], 'end_exclusive': source['meta']['end_exclusive']}]
    source['rows'] = [row(0, announce=10, withdraw=0), row(5, announce=20, withdraw=2, ipv4_addresses=256),
                      row(10, announce=30, withdraw=3), row(15, announce=40, withdraw=4),
                      row(20, announce=50, withdraw=5, ipv4_addresses=512)]
    monkeypatch.setattr(delivery_read, 'read_outage_intervals', lambda *args, **kwargs: [])
    monkeypatch.setattr(features_service.data_loader, 'coarse_routing_prefixes', lambda: ['192.0.2.0/24'])
    return source


def test_partial_activity_does_not_become_a_full_window_change(series, client):
    response = client.get(URL, query_string=QUERY)
    assert response.status_code == 200
    body = response.get_json()
    values = body['metrics']
    activity = values['withdraw']
    assert activity['reference']['value'] is None
    assert activity['reference']['known_window_sum'] == 2
    assert activity['current']['value'] == 9
    assert activity['comparison'] == {'state': 'not_comparable', 'reasons': ['reference_total_unknown'],
                                       'delta': None, 'percent_of_reference': None}
    assert values['announce']['reference']['known_window_sum'] == 20
    resource = values['ipv4_addresses']
    assert resource['reference'] == {'value': 256, 'at': '2026-03-01T19:10:00+08:00'}
    assert resource['current'] == {'value': 512, 'at': '2026-03-01T19:25:00+08:00'}
    assert resource['comparison']['delta'] == 256
    assert values['as_outage']['statistic'] == 'last_sample'
    assert values['as_outage']['population'] == 'detected_asns'
    assert values['prefix_outage']['population'] == 'coarse_routing_prefixes'
    assert body['metadata']['recovery_assessment'] == 'not_assessed'
    validate(body, 'CountryComparisonPayload')


@pytest.mark.parametrize('start,end,first', [('19:02:00', '19:12:00', '19:05:00'),
                                           ('19:00:00', '19:12:00', '19:00:00')])
def test_complete_delivery_and_selected_activity_intervals_have_distinct_contracts(series, client, start, end, first):
    body = client.get(URL, query_string={**QUERY,
        'reference_start_time': '2026-03-01 ' + start,
        'reference_end_time': '2026-03-01 ' + end}).get_json()
    assert body['query']['reference']['coverage']['state'] == 'complete'
    activity = body['metrics']['announce']
    assert activity['reference']['source_intervals'] == [{
        'start': '2026-03-01T' + first + '+08:00',
        'end_exclusive': '2026-03-01T19:10:00+08:00'}]
    assert activity['reference']['value'] is None
    assert activity['comparison']['state'] == 'not_comparable'
    assert activity['comparison']['delta'] is None
    validate(body, 'CountryComparisonPayload')
    schemas = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_text())['components']['schemas']
    for name in ['ComparisonWindowReading', 'SeriesWindowStatistics']:
        ref = schemas[name]['properties']['source_intervals']['items']['$ref']
        assert ref == '#/components/schemas/ActivitySourceInterval'


def test_equal_complete_windows_return_both_activity_changes_and_zero_reference(series, client):
    body = client.get(URL, query_string={**QUERY, 'reference_start_time': '2026-03-01 19:00:00',
                                        'reference_end_time': '2026-03-01 19:10:00'}).get_json()
    values = body['metrics']
    assert values['announce']['comparison']['delta'] == 60
    assert values['announce']['comparison']['percent_of_reference'] == 200
    assert values['withdraw']['comparison']['delta'] == 7
    assert values['withdraw']['comparison']['percent_of_reference'] == 350
    assert values['as_outage']['comparison'] == {'state': 'comparable', 'reasons': [],
                                                 'delta': 0, 'percent_of_reference': None}


def test_unequal_complete_activity_windows_are_not_compared_as_equal_periods(series, client):
    body = client.get(URL, query_string={**QUERY, 'reference_start_time': '2026-03-01 19:00:00',
                                        'reference_end_time': '2026-03-01 19:05:00'}).get_json()
    assert body['metrics']['announce']['reference']['value'] == 10
    assert body['metrics']['announce']['comparison']['reasons'] == ['window_duration_mismatch']
    assert body['metrics']['announce']['comparison']['delta'] is None


def test_unknown_last_point_is_not_replaced_with_an_earlier_value(series, client):
    series['rows'][-1]['ipv4_addresses'] = None
    body = client.get(URL, query_string=QUERY).get_json()
    assert body['metrics']['ipv4_addresses']['current']['value'] is None
    assert body['metrics']['ipv4_addresses']['comparison']['reasons'] == ['current_value_unknown']


@pytest.mark.parametrize('change', [{'version': ''}, {'country': ''}, {'reference_start_time': ''},
    {'reference_end_time': '2026-03-03 19:00:00'}, {'reference_start_time': ['x', 'y']}, {'other': 'x'}])
def test_invalid_comparison_is_rejected_before_data_reads(series, client, change):
    assert client.get(URL, query_string={**QUERY, **change}).status_code == 400
    assert not series['queries']


def test_read_failure_and_version_change_cannot_produce_comparisons(series, client, monkeypatch):
    assert client.get(URL, query_string={**QUERY, 'version': 'old'}).status_code == 409
    def fail(*args, **kwargs):
        raise psycopg2.OperationalError('合成中断读取失败')
    monkeypatch.setattr(delivery_read, 'read_outage_intervals', fail)
    response = client.get(URL, query_string=QUERY)
    assert response.status_code == 503
    assert 'metrics' not in response.get_json()


def test_reference_window_obeys_the_same_profile_boundaries(series, client, monkeypatch):
    monkeypatch.setenv('DOMEYE_ENFORCE_DATA_WINDOW', 'true')
    monkeypatch.setenv('DOMEYE_DATA_WINDOW_START', '2026-03-01 00:00:00')
    monkeypatch.setenv('DOMEYE_DATA_WINDOW_END_EXCLUSIVE', '2026-04-01 00:00:00')
    monkeypatch.setenv('DOMEYE_DATA_SNAPSHOT_TIME', '2026-03-31 23:59:59')
    response = client.get(URL, query_string={**QUERY, 'reference_start_time': '2026-02-28 19:00:00',
                                            'reference_end_time': '2026-02-28 20:00:00'})
    assert response.status_code == 400
    assert not series['queries']
