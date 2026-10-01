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
    state = {'meta': meta, 'items': [], 'records': {}, 'errors': 0, 'queries': [], 'countries': {}}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, query, params=()):
            state['queries'].append((query, params))
            assert query.startswith(('SELECT ', 'WITH RECURSIVE subjects'))
            self.query = query
            self.params = params

        def fetchall(self):
            if self.query.startswith('WITH RECURSIVE subjects'):
                return [('中国',), ('伊朗',), ('美国',)]
            if self.query.startswith("SELECT DISTINCT data->'attacked_country'"):
                return [(value,) for value in state['countries'].values()]
            if self.query.startswith('SELECT reference,'):
                return list(state['countries'].items())
            if self.query.startswith("SELECT core_item->>"):
                fields = ('reference', 'kind', 'object', 'start_time', 'address_family', 'level')
                rows = []
                for item in state['items']:
                    if 'reference = ANY' in self.query and item['reference'] not in self.params[-1]:
                        continue
                    row = tuple(item[name] for name in fields) + ('level_conflict' in item,)
                    if "core_item->>'record_number'" in self.query:
                        row += (item['record_number'], item['asns'], item.get('parent_prefix'), item.get('country_name'))
                    rows.append(copy.deepcopy(row))
                return rows
            if 'reference = ANY' in self.query:
                rows = []
                for item in state['items']:
                    if item['reference'] not in self.params[0]:
                        continue
                    row = (copy.deepcopy(item),)
                    if 'jsonb_build_object' in self.query:
                        data = state['records'].get(item['reference'], {}).get('data', {})
                        row += ('e_time' in data and data['e_time'] is None, copy.deepcopy(data))
                    rows.append(row)
                return rows
            raise AssertionError(self.query)

        def fetchone(self):
            if self.query.startswith('SELECT incident_id,data,context,core_item,core_error'):
                row = state['records'].get(self.params[0])
                return copy.deepcopy((row['incident_id'], row['data'], row['context'], row['item'], None)) if row else None
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
        if 'reference = ANY' in query:
            assert params == ([delivery['items'][0]['reference']],)
            continue
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


def add_record(delivery, kind='as_outage', **changes):
    """通过真实交付转换生成缓存行，HTTP 同时读取对应的原始事件字段。"""
    from data_pipeline.overview.input import overview_item

    data = {
        'source': 'r', 's_time': '2026-02-24 08:10:00', 'e_time': None,
        'outage_id': 1, 'outage_level': 'high', 'outage_level_descr': '合成规则',
        'asn': '64501', 'prefix': '192.0.2.0/24', 'country': 'ZZ',
        'country_chinese_name': '测试地区', 'outage_ases': ['64501', '64502'],
        'max_outage_as_num': 2, 'total_as_num': 12, 'max_outage_as_ratio': 2 / 12,
    }
    data.update(changes)
    target = {'as_outage': data['asn'], 'prefix_outage': data['prefix'].replace('/', '-'),
              'country_outage': data['country']}[kind]
    ref = f"{kind}/{data['s_time']}/{target}/{data['outage_id']}/r"
    context = {
        'scope': {'run_id': 'synthetic-run', 'source': 'r', 'collector_id': 'synthetic-collector',
                  'window_start': '2026-02-24T00:00:00Z', 'window_end': '2026-02-25T00:00:00Z',
                  'computation_version': 'synthetic-detector/v2'},
        'delivered_at': '2026-09-01T00:00:00Z', 'result_locator': {'ordinal': 1},
    }
    result = delivery_read.normalized_record(ref, data, context, 'synthetic-event')
    item = overview_item(result)
    delivery['items'].append(item)
    row = {'incident_id': 'synthetic-event', 'data': data, 'context': context, 'item': item}
    delivery['records'][ref] = row
    return ref, row


def structured_country():
    peak_at = '2026-02-24T01:20:00Z'
    return {
        'schema_version': 'country-outage-incident/v2',
        'algorithm_version': 'country_outage_live_event_model_v2',
        'incident_id': 'incident_fixture', 'cohort_id': 'cohort_fixture',
        'country_code': 'ZZ', 'collector_id': 'legacy_live', 'source': 'r',
        'onset_at': '2026-02-24T00:07:00Z', 'detected_at': '2026-02-24T00:10:00Z',
        'peak_at': peak_at, 'peak_snapshot_id': 'snapshot_fixture',
        'observation_end_at': '2026-02-24T03:32:00Z',
        'duration_state': 'lower_bound', 'recovery_state': 'unknown',
        'trough_at': None, 'trough_snapshot_id': None,
        'partial_recovery_at': None, 'full_recovery_at': None,
        'milestones': {'peak': {'at': peak_at, 'snapshot_id': 'snapshot_fixture',
                               'metric': 'affected_asn_ratio', 'metric_value': 2 / 12,
                               'time_precision': 'five_minute_runtime_observation'}},
    }


@pytest.mark.parametrize('kind', ['as_outage', 'prefix_outage'])
def test_open_outage_is_ongoing_at_delivered_cutoff_in_list_and_detail(delivery, client, kind):
    ref, row = add_record(delivery, kind)
    before = copy.deepcopy(row['item'])
    listed = client.get(URL, query_string={'date': '2026-02-24'}).get_json()
    detailed = client.get(URL + '/record', query_string={'ref': ref, 'version': 'delivery_fixture'}).get_json()
    lifecycle = detailed['item']['lifecycle']
    assert lifecycle == {
        'state': 'ongoing', 'basis': 'detector_end_time',
        'data_end_exclusive': '2026-02-24T03:35:00Z', 'observed_at': None,
    }
    assert listed['events']['items'][0] == detailed['item']
    assert detailed['item']['end_time'] == before['end_time']
    assert detailed['item']['end_time']['state'] == 'unknown'
    assert detailed['item']['end_time']['value'] is None
    assert detailed['item']['content_version'] == detailed['record']['content_version'] == before['content_version']
    assert row['item'] == before  # 不改写已交付的旧缓存或内容身份。
    validate(listed, 'CoreOverviewPayload')
    validate(detailed, 'CoreOverviewDetail')


def test_country_peak_and_membership_come_from_structured_data_without_summary_parsing(delivery, client):
    incident = structured_country()
    ref, row = add_record(delivery, 'country_outage', structured_incident=incident,
                          peak_snapshot_id='snapshot_fixture', structured_v2=True,
                          event_info='不包含峰值时间的任意摘要')
    listed = client.get(URL, query_string={'date': '2026-02-24'}).get_json()
    detail = client.get(URL + '/record', query_string={'ref': ref, 'version': 'delivery_fixture'}).get_json()
    item = detail['item']
    assert item == listed['events']['items'][0]
    assert item['country_incident']['peak_at'] == incident['peak_at']
    assert item['country_incident']['asn_membership'] == {
        'basis': 'peak_snapshot', 'count': 2, 'total': 12, 'ratio': 2 / 12,
    }
    assert item['asns'] == ['64501', '64502']
    assert item['country_incident']['recovery_state'] == 'unknown'
    assert item['lifecycle']['state'] == 'ongoing'
    assert item['lifecycle']['observed_at'] == incident['observation_end_at']
    assert item['lifecycle']['data_end_exclusive'] != row['context']['scope']['window_end']
    assert item['content_version'] == detail['record']['content_version']
    validate(detail, 'CoreOverviewDetail')


@pytest.mark.parametrize('value,state', [(None, 'ongoing'), ('', 'unknown'),
                                        ('2026-02-24 09:00:00', 'ended')])
def test_null_is_not_conflated_with_bad_or_recorded_end_time(delivery, client, value, state):
    ref, _ = add_record(delivery, e_time=value)
    response = client.get(URL + '/record', query_string={'ref': ref, 'version': 'delivery_fixture'})
    assert response.status_code == 200
    assert response.get_json()['item']['lifecycle']['state'] == state


def test_missing_end_time_and_delivery_gaps_do_not_assert_ongoing(delivery, client):
    ref, row = add_record(delivery)
    del row['data']['e_time']
    response = client.get(URL + '/record', query_string={'ref': ref, 'version': 'delivery_fixture'})
    assert response.get_json()['item']['lifecycle']['state'] == 'unknown'
    row['data']['e_time'] = None
    delivery['meta']['intervals'] = [interval(end='2026-02-24T08:15:00+08:00'),
                                   interval(start='2026-02-24T10:00:00+08:00')]
    response = client.get(URL + '/record', query_string={'ref': ref, 'version': 'delivery_fixture'})
    assert response.get_json()['item']['lifecycle']['state'] == 'unknown'


@pytest.mark.parametrize('change', [
    {'peak_snapshot_id': 'another-snapshot'}, {'max_outage_as_num': 3},
    {'total_as_num': 0}, {'max_outage_as_ratio': .5},
    {'structured_incident': {'schema_version': 'country-outage-incident/v2'}},
])
def test_inconsistent_structured_peak_fails_explicitly(delivery, client, change):
    fields = {'structured_incident': structured_country(), 'peak_snapshot_id': 'snapshot_fixture', 'structured_v2': True}
    fields.update(change)
    ref, _ = add_record(delivery, 'country_outage', **fields)
    response = client.get(URL + '/record', query_string={'ref': ref, 'version': 'delivery_fixture'})
    assert response.status_code == 503
    assert response.get_json()['state'] == 'unavailable'
    assert client.get(URL, query_string={'date': '2026-02-24'}).status_code == 503


def test_unstructured_country_keeps_unknown_and_stale_version_stays_409(delivery, client):
    ref, _ = add_record(delivery, 'country_outage')
    response = client.get(URL + '/record', query_string={'ref': ref, 'version': 'delivery_fixture'})
    assert response.get_json()['item']['lifecycle']['state'] == 'unknown'
    assert 'country_incident' not in response.get_json()['item']
    assert client.get(URL + '/record', query_string={'ref': ref, 'version': 'older'}).status_code == 409


def test_country_resolution_reads_delivery_without_enhanced_directory(delivery, client, monkeypatch, tmp_path):
    monkeypatch.setenv('DOMEYE_COUNTRY_OUTAGE_GENERAL_READ_MODEL', str(tmp_path / 'missing'))
    # 即使旧增强读取器不可用，也不应进入它；完成文件模式明确选择现有数据库。
    from web.api.v2 import country_outages
    def unexpected(*args, **kwargs):
        raise AssertionError('完成文件模式不应调用增强目录或旧事实入口')
    monkeypatch.setattr(country_outages, '_data_layer_call', unexpected)
    monkeypatch.setattr(country_outages, 'resolve_country_outage', unexpected)
    ref, _ = add_record(delivery, 'country_outage', structured_incident=structured_country(),
                        peak_snapshot_id='snapshot_fixture', structured_v2=True)
    response = client.get('/api/v2/events/resolve', query_string={'ref': ref.replace(' ', '+')})
    assert response.status_code == 200
    payload = response.get_json()
    validate(payload, 'DeliveredCountryOutageResolution')
    event = payload['event']
    core = client.get(URL + '/record', query_string={'ref': ref, 'version': event['version']}).get_json()
    assert event['item'] == core['item']
    assert event['record']['content_version'] == core['record']['content_version']
    assert response.headers['X-Domeye-Result-Version'] == event['version']
    assert event['item']['lifecycle']['state'] == 'ongoing'
    assert event['item']['country_incident']['asn_membership']['basis'] == 'peak_snapshot'
    assert not {'publication_id', 'revision', 'capabilities'} & payload.keys()


@pytest.mark.parametrize('params,code', [
    ({}, 400), ({'ref': 'bad'}, 400),
    ({'ref': 'as_outage/2026-02-24 08:10:00/64501/1/r'}, 400),
    ({'ref': 'country_outage/2026-02-24 08:10:00/ZZ/1/r'}, 404),
    ({'ref': 'country_outage/2026-02-24 08:10:00/ZZ/1/r', 'version': 'older'}, 409),
    ({'ref': 'country_outage/2026-02-24 08:10:00/ZZ/1/r', 'version': ''}, 400),
    ({'ref': 'country_outage/2026-02-24 08:10:00/ZZ/1/r', 'extra': 'x'}, 400),
    ({'ref': ['country_outage/2026-02-24 08:10:00/ZZ/1/r', 'bad']}, 400),
])
def test_country_resolution_preserves_query_errors(delivery, client, params, code):
    response = client.get('/api/v2/events/resolve', query_string=params)
    assert response.status_code == code


def test_country_resolution_failure_is_not_a_legacy_fallback(delivery, client, monkeypatch):
    from web.api.v2 import country_outages
    def unexpected(*args, **kwargs):
        raise AssertionError('数据失败不能切换旧来源')
    monkeypatch.setattr(country_outages, '_general_read_call', unexpected)
    ref, _ = add_record(delivery, 'country_outage', structured_v2=True, structured_incident={})
    response = client.get('/api/v2/events/resolve', query_string={'ref': ref})
    assert response.status_code == 503
    assert response.get_json()['observation_state'] == 'unavailable'


def test_country_recorded_full_recovery_ends_the_detector_event(delivery, client):
    incident = structured_country()
    incident.update(recovery_state='fully_recovered', duration_state='exact',
                    full_recovery_at='2026-02-24T03:00:00Z')
    ref, _ = add_record(delivery, 'country_outage', structured_incident=incident,
                        peak_snapshot_id='snapshot_fixture', structured_v2=True,
                        e_time='2026-02-24 11:00:00')
    response = client.get(URL + '/record', query_string={'ref': ref, 'version': 'delivery_fixture'})
    assert response.status_code == 200
    assert response.get_json()['item']['lifecycle']['state'] == 'ended'
    validate(response.get_json(), 'CoreOverviewDetail')


def test_leak_and_unselected_country_records_do_not_gain_outage_semantics(delivery, client):
    ref, _ = add_record(delivery, 'country_outage', structured_v2=True, structured_incident={})
    delivery['items'].append(event(2, kind='leak'))
    response = client.get(URL, query_string={'date': '2026-02-24', 'kind': 'leak'})
    assert response.status_code == 200
    item = response.get_json()['events']['items'][0]
    assert item['lifecycle']['state'] == 'unavailable'
    assert 'country_incident' not in item
    assert all(ref not in params[0] for sql, params in delivery['queries'] if 'reference = ANY' in sql)


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


def test_cross_day_window_clips_buckets_preserves_gaps_and_excludes_right_end(delivery, client):
    delivery['meta']['intervals'] = [interval('2026-02-24T23:50:00+08:00', '2026-02-25T00:10:00+08:00'),
                                     interval('2026-02-25T02:00:00+08:00', '2026-02-25T03:00:00+08:00')]
    delivery['items'] = [event(1, at='2026-02-24T23:55:00+08:00'), event(2, at='2026-02-25T00:05:00+08:00'),
                         event(3, at='2026-02-25T01:00:00+08:00'), event(4, at='2026-02-25T03:00:00+08:00')]
    response = client.get(URL, query_string={'start_time': '2026-02-24 23:55:00', 'end_time': '2026-02-25 03:00:00'})
    assert response.status_code == 200
    payload = response.get_json(); validate(payload, 'CoreOverviewPayload')
    assert payload['overview']['record_count'] == 2
    assert payload['metadata']['query_coverage']['state'] == 'partial'
    buckets = payload['event_trends']['series'][0]['buckets']
    assert len(buckets) == 3
    assert buckets[0]['start'] == '2026-02-24T23:55:00+08:00'
    assert [b['value'] for b in buckets] == [1, 1, 0]
    assert payload['event_trends']['filter_scope'] == 'window_country_and_family'


@pytest.mark.parametrize('kind', delivery_read.KINDS)
def test_region_exact_membership_controls_totals_cards_and_list(delivery, client, kind):
    delivery['items'] = [event(number, kind=kind) for number in range(1, 5)]
    delivery['countries'] = {item['reference']: country for item, country in zip(delivery['items'], ['中国', ['中国', '美国'], '中国台湾省', None])}
    params = {'start_time': '2026-02-24 08:00:00', 'end_time': '2026-02-24 11:35:00', 'country': '中国'}
    payload = client.get(URL, query_string=params).get_json()
    validate(payload, 'CoreOverviewPayload')
    assert payload['overview']['record_count'] == payload['events']['total'] == 2
    assert {series['kind']: series['total'] for series in payload['event_trends']['series']} == {
        event_kind: 2 if event_kind == kind else 0 for event_kind in delivery_read.KINDS
    }
    assert payload['metadata']['rib_statistics']['state'] == 'not_applicable'
    assert payload['overview']['visible_prefixes'] is None
    assert payload['metadata']['query_coverage']['state'] == 'complete'
    empty = client.get(URL, query_string={**params, 'country': '伊朗'}).get_json()
    assert empty['overview']['record_count'] == 0
    outside = client.get(URL, query_string={**params, 'start_time': '2026-02-25 08:00:00', 'end_time': '2026-02-25 09:00:00'}).get_json()
    assert outside['overview'] is None and outside['metadata']['query_coverage']['state'] == 'none'
    assert client.get(URL, query_string={**params, 'country': '并不存在的地区'}).status_code == 400


def test_long_window_uses_daily_buckets_without_filling_unknown_days(delivery, client):
    delivery['items'] = [event(1)]
    payload = client.get(URL, query_string={'start_time': '2026-02-01 00:00:00', 'end_time': '2026-03-01 00:00:00'}).get_json()
    validate(payload, 'CoreOverviewPayload')
    assert payload['event_trends']['bucket_seconds'] == 86400
    assert len(payload['event_trends']['series'][0]['buckets']) == 1
    assert payload['event_trends']['series'][0]['total'] == 1


@pytest.mark.parametrize('params', [
    {'start_time': '2026-02-24 08:00:00'},
    {'start_time': '2026-02-24 09:00:00', 'end_time': '2026-02-24 08:00:00'},
    {'start_time': '2026-02-30 08:00:00', 'end_time': '2026-03-01 08:00:00'},
    {'start_time': '2026-01-31 08:00:00', 'end_time': '2026-02-24 08:00:00'},
    {'date': '2026-02-24', 'start_time': '2026-02-24 08:00:00', 'end_time': '2026-02-24 09:00:00'},
    {'hour': '8', 'start_time': '2026-02-24 08:00:00', 'end_time': '2026-02-24 09:00:00'},
])
def test_invalid_range_is_rejected_before_event_query(delivery, client, params):
    assert client.get(URL, query_string=params).status_code == 400
    assert not delivery['queries']


def test_country_range_keeps_structured_lifecycle_and_record_version(delivery, client):
    ref, _ = add_record(delivery, 'country_outage', structured_incident=structured_country(),
                        peak_snapshot_id='snapshot_fixture', structured_v2=True)
    delivery['countries'][ref] = "['伊朗', '测试国']"
    params = {'start_time': '2026-02-24 08:00:00', 'end_time': '2026-02-24 11:35:00',
              'country': '测试国', 'version': 'delivery_fixture'}
    response = client.get(URL, query_string=params)
    assert response.status_code == 200
    listed = response.get_json(); validate(listed, 'CoreOverviewPayload')
    detail = client.get(URL + '/record', query_string={'ref': ref, 'version': listed['version']}).get_json()
    item = listed['events']['items'][0]
    assert item == detail['item']
    assert item['lifecycle']['state'] == 'ongoing'
    assert item['country_incident']['asn_membership']['basis'] == 'peak_snapshot'
    assert listed['metadata']['countries'] == ['中国', '伊朗', '测试国', '美国']
    assert client.get(URL, query_string={**params, 'version': 'older'}).status_code == 409


def test_six_hour_buckets_keep_timezone_boundary_and_partial_coverage(delivery, client):
    delivery['meta']['intervals'] = [interval('2026-02-24T05:55:00+08:00', '2026-02-24T06:05:00+08:00')]
    delivery['items'] = [event(1, at='2026-02-23T21:59:00Z'),
                         event(2, at='2026-02-23T22:00:00Z'),
                         event(3, at='2026-02-23T22:05:00Z')]
    response = client.get(URL, query_string={'start_time': '2026-02-23T00:00:00Z',
                                            'end_time': '2026-02-26T00:00:00Z'})
    assert response.status_code == 200
    payload = response.get_json(); validate(payload, 'CoreOverviewPayload')
    assert payload['event_trends']['bucket_seconds'] == 21600
    buckets = payload['event_trends']['series'][0]['buckets']
    assert [(bucket['start'], bucket['end_exclusive'], bucket['value']) for bucket in buckets] == [
        ('2026-02-24T05:55:00+08:00', '2026-02-24T06:00:00+08:00', 1),
        ('2026-02-24T06:00:00+08:00', '2026-02-24T06:05:00+08:00', 1),
    ]
    assert payload['overview']['record_count'] == 2
    assert payload['metadata']['query_coverage']['state'] == 'partial'


def test_country_directory_read_failure_is_not_empty_success(delivery, client, monkeypatch):
    import psycopg2
    def fail(_conn):
        raise psycopg2.OperationalError('synthetic unavailable country directory')
    monkeypatch.setattr(delivery_read, 'available_countries', fail)
    response = client.get(URL, query_string={'date': '2026-02-24', 'country': '伊朗'})
    assert response.status_code == 503
    payload = response.get_json()
    assert payload['state'] == 'unavailable'
    assert 'overview' not in payload and 'events' not in payload
