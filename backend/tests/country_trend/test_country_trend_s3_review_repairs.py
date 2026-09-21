"""三个独立审查反例：实际纯表达式/入口前缀、本地DuckDB失败关闭；无成功AD。"""
import ast
import hashlib
import inspect
from types import SimpleNamespace

import duckdb
import pytest

from data_pipeline.bgp.archive.value_codec import typed, digest
from data_pipeline.analysis.country_trends import feature_source as s3_feature, result_admission as public, runtime as s3_runtime
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits


def test_feature_public_digest_matches_unchanged_upstream_formula():
    # 执行捕获器中实际的h.update表达式，不伪造Feature reader或回执。
    tree = ast.parse(inspect.getsource(s3_feature.capture_feature))
    updates = [node for node in ast.walk(tree) if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute)
        and isinstance(node.value.func.value, ast.Name) and node.value.func.value.id == 'h'
        and node.value.func.attr == 'update']
    body = compile(ast.fix_missing_locations(ast.Module(body=updates, type_ignores=[])), '<capture-hash>', 'exec')
    actual, expected = hashlib.sha256(), hashlib.sha256()
    for row in ({'raw':{'country':'US'},'values':{'announ_num':1}},
                {'raw':{'country':'US'},'values':{'announ_num':None}}):
        text = typed(row)
        exec(body, {'h':actual,'payload':text.encode(),'text':text,'digest':digest})
        expected.update(bytes.fromhex(digest(typed(row))))
    assert actual.hexdigest() == expected.hexdigest()


def test_read_request_snapshot_cannot_be_rebound_by_caller():
    # 仅执行实际open_reader首次current之前的纯前缀；不跨入PG/current或模拟成功AD。
    function = ast.parse(inspect.getsource(public.open_reader)).body[0]
    prefix = []
    for statement in function.body:
        if isinstance(statement, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'guard' for t in statement.targets):
            break
        prefix.append(statement)
    body = compile(ast.fix_missing_locations(ast.Module(body=prefix, type_ignores=[])), '<read-request>', 'exec')
    scope = dict(event=('event-A',1),key_typed=None,after_sequence=-1,stop_sequence=None)
    request = dict(view='raw_source',scope_typed=public.schema.encode(scope),codec_version=public.CODEC,batch_rows=2,batch_bytes=4096)
    initial = digest(request)
    namespace = dict(public.__dict__,request=request,runtime=SimpleNamespace(limits=S2Limits()))
    exec(body, namespace)
    request.update(view='metric',scope_typed=public.schema.encode(dict(scope,event=('event-B',1))),batch_rows=1)
    assert namespace['scope'] == scope
    assert digest(namespace['request']) == initial
    assert namespace['request']['view'] == 'raw_source'


def test_lake_configuration_primary_survives_actual_duckdb_close_error(tmp_path, monkeypatch):
    root = tmp_path.resolve(); (root/'out').mkdir(); (root/'scratch').mkdir()
    runtime = s3_runtime.ProductionRuntime(dsn=f'host={root} dbname=fixture',output_root=root/'out',
        scratch_root=root/'scratch',allowed_roots=(root,),dependency_admissions=(),dependency_runtimes={},
        country_window_us=(100,200),fixture_only=True)
    db = duckdb.connect(); closed = []
    primary = RuntimeError('fixture-configure-primary')
    secondary = OSError('fixture-close-secondary')
    class Connection:
        def execute(self, *args, **kwargs): raise primary
        def close(self):
            db.close(); closed.append(True); raise secondary
    monkeypatch.setattr(s3_runtime,'lake_connect',lambda *args,**kwargs:Connection())
    with pytest.raises(RuntimeError) as caught: runtime.lake()
    assert caught.value is primary and closed == [True]
    assert caught.value.cleanup_errors == (secondary,)
    with pytest.raises(duckdb.ConnectionException): db.execute('SELECT 1')
