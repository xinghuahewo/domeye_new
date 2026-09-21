import builtins
import sys
from datetime import datetime, timezone

import pytest


EXPECTED_ROUTES = {
    '/api/v1/rib-snapshots',
    '/api/v1/rib-snapshots/<version>',
    '/api/v1/rib-snapshots/<version>/observations',
    '/api/v1/rib-snapshots/<version>/asns/<asn>',
    '/api/v1/core-overview',
    '/api/v1/core-overview/record',
    '/api/v1/healthz',
    '/api/v1/p0/status',
    '/api/v1/p0/metrics/<metric_name>',
    '/api/v1/p0/quality',
    '/api/v1/events',
    '/api/v1/events/top',
    '/api/v1/events/evidence-bundle/<event_type>/<start_time>/<problem>/<int:event_id>/<source>',
    '/api/v1/events/story/<event_type>/<start_time>/<problem>/<int:event_id>/<source>',
    '/api/v1/events/observations/<event_type>/<start_time>/<problem>/<int:event_id>/<source>',
    '/api/v1/<event_type>/<start_time>/<problem>/<int:event_id>/<source>',
    '/api/v1/features/top',
    '/api/v1/features/countries',
    '/api/v1/features/countries/overview',
    '/api/v1/features/ases',
    '/api/v1/features/ases/overview',
    '/api/v1/features/ases/events',
    '/api/v1/features/outages/country-as',
    '/api/v1/features/outages/country-prefix',
    '/api/v1/features/outages/as-prefix',
    '/api/v1/features/outages/global-as',
    '/api/v1/features/outages/global-prefix',
    '/api/v1/dashboard/counts/total',
    '/api/v1/dashboard/counts/type',
    '/api/v1/dashboard/overview',
    '/api/v2/events/resolve',
    '/api/v2/country-outages/<incident_id>/overview',
    '/api/v2/country-outages/<incident_id>/series',
    '/api/v2/country-outages/<incident_id>/asns',
    '/api/v2/country-outages/<incident_id>/audit',
    '/api/v2/country-outages/<incident_id>/trend',
    '/api/v2/country-outages/<incident_id>/path-downstreams',
}


def test_route_whitelist(app):
    routes = {
        str(rule)
        for rule in app.url_map.iter_rules()
        if not str(rule).startswith('/static/')
    }
    assert routes == EXPECTED_ROUTES


def test_removed_platform_routes_return_404(client):
    for path in (
        '/api/v1/login',
        '/api/v1/events/state',
        '/api/v1/events/judge',
        '/api/v1/events/notify',
        '/api/v1/reports/word-export',
        '/api/v1/geodata/boundaries',
        '/api/v1/node-status',
        '/api/v1/data-query/tasks',
    ):
        assert client.get(path).status_code == 404


def test_health_does_not_require_database_or_assets(client, assert_contract):
    before = datetime.now(timezone.utc)
    response = client.get('/api/v1/healthz')
    after = datetime.now(timezone.utc)

    assert response.status_code == 200
    payload = response.get_json()
    assert_contract(payload, {'status': str, 'service': str, 'time': str})
    assert payload['status'] == 'ok'
    assert payload['service'] == 'domeye-core'
    assert payload['time'].endswith('Z')

    health_time = datetime.fromisoformat(payload['time'].replace('Z', '+00:00'))
    assert health_time.tzinfo == timezone.utc
    assert before <= health_time <= after


def test_removed_services_are_not_imported(app):
    assert 'services.auth_service' not in sys.modules
    assert 'services.data_query_service' not in sys.modules
    assert 'core.visualize.outage_topo' not in sys.modules
    assert 'web.api.reports.api' not in sys.modules


def test_local_env_loader_can_be_disabled(monkeypatch):
    from run import load_local_env

    monkeypatch.setenv('DOMEYE_CORE_SKIP_LOCAL_ENV', 'true')

    def unexpected_env_lookup(_):
        raise AssertionError('禁用本地环境加载后不应读取 .env')

    monkeypatch.setattr('run.os.path.exists', unexpected_env_lookup)
    load_local_env()


@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize("path", [
    "/api/v2/country-outage/chat/conversations",
    "/api/v2/country-outage/chat/conversations/test",
    "/api/v2/country-outage/chat/conversations/test/resume",
    "/api/v2/country-outage/chat/conversations/test/turns",
    "/api/v2/country-outage/chat/conversations/test/turns/test/trace",
    "/api/v2/country-outage/chat/conversations/test/turns/test/cancel",
    "/api/v2/country-outage/reports",
    "/api/v2/country-outage/investigations",
])
def test_removed_agent_routes_never_dispatch(client, monkeypatch, method, path):
    def unexpected_request(*args, **kwargs):
        raise AssertionError("退役接口不得访问模型或 Sidecar")

    monkeypatch.setattr("requests.sessions.Session.request", unexpected_request)
    assert getattr(client, method)(path).status_code == 404


def test_only_read_only_methods_are_registered(app):
    for rule in app.url_map.iter_rules():
        if str(rule).startswith("/api/"):
            assert rule.methods <= {"GET", "HEAD", "OPTIONS"}


def test_data_window_guard_remains_global(app):
    from web.data_window_guard import enforce_request_data_window

    assert enforce_request_data_window in app.before_request_funcs[None]


def test_startup_does_not_import_or_run_initializers(monkeypatch):
    from run import create_app

    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "init_db" or name == "core" or name.startswith("core."):
            raise AssertionError("只读 Web 入口不得加载初始化或检测器")
        return original_import(name, *args, **kwargs)

    def unexpected_io(*args, **kwargs):
        raise AssertionError("启动不得连接数据库或预加载运行数据")

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr("psycopg2.connect", unexpected_io)
    monkeypatch.setattr("utils.data_loader.init_global_data", unexpected_io)
    monkeypatch.setattr("run.AUTO_INIT_DB", True, raising=False)
    monkeypatch.setattr("run.LOAD_CORE_DATA_ON_STARTUP", True, raising=False)
    monkeypatch.setattr("run.init_runtime_directories", unexpected_io, raising=False)
    monkeypatch.setenv("AUTO_INIT_DB", "true")
    monkeypatch.setenv("LOAD_CORE_DATA_ON_STARTUP", "true")
    app = create_app("default")

    assert app.test_client().get("/api/v1/healthz").status_code == 200


def test_data_requests_do_not_accept_agent_identity_headers(app):
    with app.test_request_context(
        "/api/v1/healthz",
        headers={"X-User-Id": "forged", "X-Authorization-Scope": "admin"},
    ):
        from flask import request

        assert app.preprocess_request() is None
        assert "domeye.authenticated_user_id" not in request.environ
        assert "domeye.authorization_scope" not in request.environ


def test_cross_origin_access_does_not_advertise_write_methods(monkeypatch):
    from run import create_app

    monkeypatch.setenv("CORS_ORIGINS", "http://localhost:3100")
    response = create_app("testing").test_client().options(
        "/api/v1/healthz",
        headers={
            "Origin": "http://localhost:3100",
            "Access-Control-Request-Method": "POST",
        },
    )
    assert "POST" not in response.headers.get("Access-Control-Allow-Methods", "")
