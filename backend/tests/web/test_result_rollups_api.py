"""统一入口继续读取既有汇总；请求不启动生产，不将读取失败变为零。"""
import psycopg2
import pytest

from services import result_rollup_service

QUERY = {'start_time': '2026-03-01 00:00:00', 'end_time': '2026-03-02 00:00:00'}


def test_unconfigured_rollups_remain_explicit(client, monkeypatch):
    monkeypatch.delenv('DOMEYE_RESULT_DELIVERY', raising=False)
    response = client.get('/api/v1/result-rollups', query_string=QUERY)
    assert response.status_code == 200
    assert response.get_json()['state'] == 'not_configured'
    assert response.get_json()['summary'] is None


@pytest.mark.parametrize('change', [{'other': 'x'}, {'grain': 'week'}, {'start_time': ['x', 'y']}])
def test_invalid_rollups_are_rejected(client, change):
    assert client.get('/api/v1/result-rollups', query_string={**QUERY, **change}).status_code == 400


def test_rollup_read_failure_is_unavailable(client, monkeypatch):
    class Unavailable:
        def cursor(self):
            raise psycopg2.OperationalError('合成读取失败')
    monkeypatch.setenv('DOMEYE_RESULT_DELIVERY', 'true')
    monkeypatch.setattr(result_rollup_service, 'conn_11', Unavailable())
    response = client.get('/api/v1/result-rollups', query_string=QUERY)
    assert response.status_code == 503
    assert response.get_json()['state'] == 'unavailable'
