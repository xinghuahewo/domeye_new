"""共享原生连接的线程预算；无需 PG 或真实数据。"""
import pytest

from data_pipeline.bgp.archive import store


@pytest.mark.parametrize('value,expected', [(None, 2), ('1', 1), ('3', 3)])
def test_threads_applied_to_live_connections(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv('DOMEYE_DUCKDB_THREADS', raising=False)
    else:
        monkeypatch.setenv('DOMEYE_DUCKDB_THREADS', value)
    connections = []
    try:
        for _ in range(3):
            db = store.connect_duckdb()
            connections.append(db)
            assert db.execute("SELECT current_setting('threads')").fetchone() == (expected,)
    finally:
        for db in connections:
            db.close()


@pytest.mark.parametrize('value', ['', '0', '-1', '+2', '2.0', 'true', ' 2', '2 ', '02', '２'])
def test_invalid_threads_rejected_before_native_connect(monkeypatch, value):
    monkeypatch.setenv('DOMEYE_DUCKDB_THREADS', value)
    def unexpected_connect(*args, **kwargs):
        pytest.fail('非法预算不应创建原生连接')
    monkeypatch.setattr(store.duckdb, 'connect', unexpected_connect)
    with pytest.raises(ValueError, match='DOMEYE_DUCKDB_THREADS'):
        store.connect_duckdb()


def test_budget_is_constructor_config(monkeypatch):
    monkeypatch.setenv('DOMEYE_DUCKDB_THREADS', '2')
    original = store.duckdb.connect
    calls = []
    def capture(*args, **kwargs):
        calls.append(kwargs.get('config'))
        return original(*args, **kwargs)
    monkeypatch.setattr(store.duckdb, 'connect', capture)
    db = store.connect_duckdb()
    db.close()
    assert calls == [{'threads': 2}]
