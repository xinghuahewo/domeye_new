"""真实HTTP应用链，数据库驱动为隔离fixture；不访问真实实例。"""
import pytest
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, get_ident
from psycopg2.extensions import TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_INTRANS


QUERY = {'country': 'all', 'date': '2026-03-24_2026-03-31', 'sort_mode': 'start_timeB'}


class FixtureConnection:
    failure = None
    missing_tables = ()
    barrier = None

    def __init__(self):
        self.closed = 0
        self.transaction_status = TRANSACTION_STATUS_IDLE
        self.threads = set()

    def cursor(self, **kwargs):
        return FixtureCursor(self)

    def get_transaction_status(self):
        return self.transaction_status

    def rollback(self):
        self.transaction_status = TRANSACTION_STATUS_IDLE

    def close(self):
        self.closed = 1
        self.transaction_status = TRANSACTION_STATUS_IDLE


class FixtureCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def execute(self, sql, params=None):
        if get_ident() not in self.connection.threads:
            self.connection.threads.add(get_ident())
            if self.connection.barrier is not None:
                self.connection.barrier.wait(timeout=3)
        self.connection.transaction_status = TRANSACTION_STATUS_INTRANS
        if self.connection.failure == 'list' and 'SELECT * FROM (' in sql:
            raise psycopg2.OperationalError('fixture数据库读取失败')
        if self.connection.failure == 'count' and 'SUM(cnt)' in sql:
            raise psycopg2.OperationalError('fixture数据库计数失败')
        if self.connection.failure == 'catalog' and 'pg_class' in sql:
            raise psycopg2.OperationalError('fixture数据库目录读取失败')
        if 'pg_class' in sql:
            self.rows = [(0 if params[0] in self.connection.missing_tables else 1,)]
        elif 'SUM(cnt)' in sql:
            self.rows = [(0,)]
        else:
            self.rows = []

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0]

    def close(self):
        pass


@pytest.fixture
def database_driver(monkeypatch):
    from config.database import close_all_connections
    close_all_connections()
    connections = []

    def connect(**kwargs):
        connection = FixtureConnection()
        connections.append(connection)
        return connection

    monkeypatch.setattr(psycopg2, 'connect', connect)
    yield connections
    close_all_connections()
    for connection in connections:
        connection.close()


def test_event_query_releases_database_resources(client, database_driver):
    response = client.get('/api/v1/events', query_string=QUERY)

    assert response.status_code == 200
    assert response.get_json() == {'data': [], 'record_count': '0', 'total_page': 0}
    assert database_driver and all(connection.closed for connection in database_driver)


@pytest.mark.parametrize('failure', ['list', 'count', 'catalog'])
def test_event_read_failure_is_not_a_successful_empty_list(client, database_driver, monkeypatch, failure):
    monkeypatch.setattr(FixtureConnection, 'failure', failure)

    response = client.get('/api/v1/events', query_string=QUERY)

    assert response.status_code == 503
    assert response.get_json()['status'] is False
    assert 'data' not in response.get_json() and 'record_count' not in response.get_json()
    assert 'fixture数据库读取失败' not in response.get_data(as_text=True)
    assert all(connection.closed for connection in database_driver)


def test_parallel_event_queries_do_not_share_database_transactions(app, database_driver, monkeypatch):
    monkeypatch.setattr(FixtureConnection, 'barrier', Barrier(2))

    def read():
        return app.test_client().get('/api/v1/events', query_string=QUERY)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(read) for _ in range(2)]
        responses = [future.result(timeout=5) for future in futures]

    assert all(response.status_code == 200 for response in responses)
    assert all(response.get_json() == {'data': [], 'record_count': '0', 'total_page': 0} for response in responses)
    assert all(connection.closed and len(connection.threads) == 1 for connection in database_driver)


def test_failed_connection_can_be_followed_by_a_fresh_successful_request(client, database_driver, monkeypatch):
    connect = psycopg2.connect

    def unavailable(**kwargs):
        raise psycopg2.OperationalError('fixture连接失败，不能暴露原始错误')

    monkeypatch.setattr(psycopg2, 'connect', unavailable)
    failed = client.get('/api/v1/events', query_string=QUERY)
    assert failed.status_code == 503
    assert failed.get_json()['status'] is False
    assert 'record_count' not in failed.get_json()
    assert 'fixture' not in failed.get_data(as_text=True)

    monkeypatch.setattr(psycopg2, 'connect', connect)
    recovered = client.get('/api/v1/events', query_string=QUERY)
    assert recovered.status_code == 200
    assert recovered.get_json() == {'data': [], 'record_count': '0', 'total_page': 0}
    assert all(connection.closed for connection in database_driver)


@pytest.mark.parametrize('dates', ['2026-02-01_2026-02-28', '2026-02-01_2026-03-31'])
def test_missing_month_is_not_silently_empty_or_partial(client, database_driver, monkeypatch, dates):
    monkeypatch.setattr(FixtureConnection, 'missing_tables', ('event_table_202602',))

    response = client.get('/api/v1/events', query_string={**QUERY, 'date': dates})

    assert response.status_code == 503
    assert response.get_json()['status'] is False
    assert 'data' not in response.get_json() and 'record_count' not in response.get_json()
    assert all(connection.closed for connection in database_driver)
