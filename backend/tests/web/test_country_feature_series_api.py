"""单国曲线不借用全国家总览，并保留版本、空值和实际覆盖。"""
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path

import psycopg2
import pytest
from jsonschema import Draft202012Validator
from data_pipeline.results import delivery_read

URL = '/api/v1/features/countries/series'
QUERY = {'country': '测试地区', 'start_time': '2026-03-01 19:00:00', 'end_time': '2026-03-01 19:25:00'}

@pytest.fixture
def source(monkeypatch):
    meta = {'state': 'available', 'version': 'delivery_test', 'binding': {'collector': 'rrc25'},
            'start': '2026-03-01T19:00:00+08:00', 'end_exclusive': '2026-03-01T19:20:00+08:00',
            'intervals': [{'start': '2026-03-01T19:00:00+08:00', 'end_exclusive': '2026-03-01T19:10:00+08:00'},
                          {'start': '2026-03-01T19:15:00+08:00', 'end_exclusive': '2026-03-01T19:20:00+08:00'}]}
    state = {'meta': meta, 'queries': [], 'rows': [], 'fail': False}
    monkeypatch.setenv('DOMEYE_RESULT_DELIVERY', 'true')
    monkeypatch.setattr(delivery_read, 'status', lambda *a, **k: deepcopy(meta))
    from config.database import LazyConnection
    class Cursor:
        def execute(self, query, params):
            if state['fail']: raise psycopg2.OperationalError('合成数据库故障')
            assert query.strip().startswith('SELECT') and 'GROUP BY' not in query
            assert 'country = %s' in query and 't < %s' in query
            state['queries'].append((query, params))
        def fetchall(self): return deepcopy(state['rows'])
        def close(self): pass
    class Connection:
        closed = 0
        def get_transaction_status(self): return psycopg2.extensions.TRANSACTION_STATUS_INTRANS
        def cursor(self, **kwargs): return Cursor()
        def rollback(self): pass
    monkeypatch.setattr(LazyConnection, 'get_connection', lambda self: Connection())
    return state


def row(minute, **values):
    return {**dict(time=datetime(2026, 3, 1, 19, minute), announce=0, withdraw=None,
                   ipv4_prefixes=8, ipv6_prefixes=2, ipv4_addresses=2048), **values}


def test_single_country_series_reuses_bounded_read_and_preserves_null(source, client):
    source['rows'] = [row(0), row(5), row(10), row(15), row(20)]
    response = client.get(URL, query_string=QUERY)
    assert response.status_code == 200
    payload = response.get_json()
    assert len(source['queries']) == 1
    assert source['queries'][0][1] == ('r', '测试地区', datetime(2026,3,1,19), datetime(2026,3,1,19,20))
    assert [p['time'] for p in payload['data']] == ['2026-03-01T19:00:00+08:00', '2026-03-01T19:05:00+08:00', '2026-03-01T19:15:00+08:00']
    assert all(p['announce']==0 and p['withdraw'] is None for p in payload['data'])
    assert payload['metadata']['version'] == response.headers['X-Domeye-Result-Version']
    assert payload['metadata']['units']['ipv4_prefixes'] == 'ipv4_24_equivalent'
    assert payload['metadata']['units']['ipv4_addresses'] == 'ipv4_address'
    contract = json.loads((Path(__file__).resolve().parents[3]/'contracts/openapi.json').read_text())
    Draft202012Validator({'$ref':'#/components/schemas/CountryFeatureSeriesPayload','components':contract['components']}).validate(payload)


def test_empty_window_does_not_create_zero_samples(source, client):
    response = client.get(URL, query_string={**QUERY, 'start_time':'2026-03-01 20:00:00','end_time':'2026-03-01 21:00:00'})
    assert response.status_code == 200
    assert response.get_json()['data'] == []
    assert response.get_json()['metadata']['coverage']['state'] == 'none'
    assert not source['queries']


def test_read_failure_and_duplicate_samples_fail_explicitly(source, client):
    source['fail']=True
    assert client.get(URL, query_string=QUERY).status_code == 503
    source['fail']=False; source['rows']=[row(0),row(0)]
    assert client.get(URL, query_string=QUERY).status_code == 503
    source['rows']=[row(0,announce=-1)]
    assert client.get(URL, query_string=QUERY).status_code == 503


@pytest.mark.parametrize('change,code', [({'country':''},400), ({'country':'collect'},400), ({'version':'old'},409),
    ({'version':''},400), ({'extra':'x'},400), ({'country':['甲','乙']},400),
    ({'end_time':'2026-03-03 19:00:00'},400), ({'end_time':'2026-03-01 19:00:00'},400)])
def test_query_boundaries_fail_before_series_read(source, client, change, code):
    assert client.get(URL,query_string={**QUERY,**change}).status_code==code
    assert not source['queries']


@pytest.mark.parametrize('path,selector', [
    ('countries/series', {'country':'测试地区'}), ('outages/country-as', {'country':'测试地区'}),
    ('outages/country-prefix', {'country':'测试地区'}), ('outages/as-prefix', {'asn':'64501'}),
    ('outages/global-as', {}), ('outages/global-prefix', {}),
])
def test_half_open_series_may_end_at_profile_exclusive_boundary(source, client, monkeypatch, path, selector):
    monkeypatch.setenv('DOMEYE_ENFORCE_DATA_WINDOW', 'true')
    monkeypatch.setenv('DOMEYE_DATA_WINDOW_START', '2026-02-01 00:00:00')
    monkeypatch.setenv('DOMEYE_DATA_WINDOW_END_EXCLUSIVE', '2026-04-01 00:00:00')
    monkeypatch.setenv('DOMEYE_DATA_SNAPSHOT_TIME', '2026-03-31 23:59:59')
    response=client.get('/api/v1/features/'+path,query_string={**selector,
        'start_time':'2026-03-31 23:00:00','end_time':'2026-04-01 00:00:00'})
    assert response.status_code==200
    assert response.get_json()['metadata']['coverage']['state']=='none'
    assert not source['queries']
