"""组合请求到原 Detection reader 的纯 fixture 接合；在首次 PG 连接前停止。"""
from copy import deepcopy

import pytest

from data_pipeline.analysis.detection import publication as detection
from data_pipeline.results.component_streams import requests


class BeforePG(RuntimeError):
    """到达外部连接边界，不代表 current、正文读取或准入通过。"""


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    output = tmp_path / 'output'; output.mkdir()
    scratch = tmp_path / 'scratch'; scratch.mkdir()
    runtime = detection.Runtime('fixture-unused', output, (tmp_path,), scratch,
                                (), {}, fixture_only=True)
    binding = dict(run_id='fixture', snapshot=1, scope=dict(
        window_start='1970-01-01T00:05:00Z', window_end='1970-01-01T00:15:00Z'),
        identity=dict(qualification_as_of_position=[6, 1, 2, 0],
                      result_window=dict(window_start='1970-01-01T00:10:00Z',
                                         window_end_exclusive='1970-01-01T00:15:00Z'),
                      result_window_rule='detection-result-window/v1', window_coverage={}))
    physical = dict(system_identifier='1', database_oid=1, catalog='fixture',
                    schema='det_fixture', root=str(output), snapshot=1)
    targets = [dict(stage=30, system_identifier='1', database_oid=1,
                    namespace=namespace, key=key) for namespace, key in (
        ('detection.run', 'fixture'),
        ('detection.admission', '00000000-0000-0000-0000-000000000001'))]
    admission = dict(contract='component-publication-admission/v1', owner='detection',
        owner_revision='0'*40, owner_binding=detection.typed(binding),
        binding_codec=detection.CODEC, physical=physical,
        validator=dict(version='fixture', code_sha256='0'*64, typed_schema='fixture',
                       typed_codec=detection.CODEC, rules_digest='0'*64,
                       validation_digest='0'*64),
        inventory_digest='0'*64, entities=[], dependencies=[], lock_targets=targets)
    admission['admission_id'] = detection.digest(admission)

    def stop_before_pg(*args, **kwargs):
        raise BeforePG('明确 fixture：首次外部 PG 连接前终止')

    monkeypatch.setattr(detection.upstream.psycopg2, 'connect', stop_before_pg)
    return runtime, admission


def test_qualified_request_reaches_original_reader_pg_boundary(fixture):
    runtime, admission = fixture
    plan = requests('detection', batch_rows=7, batch_bytes=4096, admission=admission)
    request = next(q for q in plan if q['view'] == 'qualified_revisions')
    with pytest.raises(BeforePG, match='首次外部 PG'):
        with detection.open_reader(runtime, admission, request, guard=lambda: None):
            pytest.fail('fixture 不得取得成功 reader')


def test_all_eight_views_keep_their_position_and_full_range(fixture):
    runtime, admission = fixture
    before = deepcopy(admission)
    plan = requests('detection', batch_rows=7, batch_bytes=4096, admission=admission)
    assert tuple(q['view'] for q in plan) == (
        'records', 'state_entries', 'm3_entries', 'revisions', 'decisions',
        'qualified_revisions', 'result_revisions', 'result_coverage')
    for request in plan:
        assert detection.untyped(request['scope_typed']) == dict(
            start=0, stop=None, key=None,
            at_position=[6, 1, 2, 0] if request['view'] == 'qualified_revisions' else None)
        assert (request['batch_rows'], request['batch_bytes']) == (7, 4096)
        with pytest.raises(BeforePG):
            with detection.open_reader(runtime, admission, request, guard=lambda: None):
                pytest.fail('不得伪造正文或 current 成功')
    assert admission == before


def test_missing_admission_is_not_replaced_with_a_default():
    with pytest.raises(ValueError, match='缺原Admission'):
        requests('detection', batch_rows=7, batch_bytes=4096)


@pytest.mark.parametrize('position', [None, [], [6, 1, 2], [6, 1, 2, 0, 0],
                                     [6, 1, -1, 0], [6, 1, True, 0], [6, 1, 2.0, 0]])
def test_invalid_admission_position_still_rejected_by_original_reader(fixture, position):
    runtime, admission = fixture
    binding = detection.untyped(admission['owner_binding'])
    binding['identity']['qualification_as_of_position'] = position
    admission['owner_binding'] = detection.typed(binding)
    plan = requests('detection', batch_rows=7, batch_bytes=4096, admission=admission)
    request = next(q for q in plan if q['view'] == 'qualified_revisions')
    with pytest.raises(ValueError, match='必须明确有效四元处理位置'):
        with detection.open_reader(runtime, admission, request, guard=lambda: None):
            pytest.fail('无效位置不得到达读取')


def test_request_beyond_original_admission_is_rejected(fixture):
    runtime, admission = fixture
    request = next(q for q in requests('detection', batch_rows=7, batch_bytes=4096,
                                     admission=admission) if q['view'] == 'qualified_revisions')
    scope = detection.untyped(request['scope_typed']); scope['at_position'] = [6, 1, 2, 1]
    request['scope_typed'] = detection.typed(scope)
    with pytest.raises(ValueError, match='必须明确有效四元处理位置'):
        with detection.open_reader(runtime, admission, request, guard=lambda: None):
            pytest.fail('越界不得读取')


def test_other_views_still_refuse_position_clipping(fixture):
    runtime, admission = fixture
    for request in requests('detection', batch_rows=7, batch_bytes=4096, admission=admission):
        if request['view'] == 'qualified_revisions':
            continue
        scope = detection.untyped(request['scope_typed']); scope['at_position'] = [6, 1, 2, 0]
        request['scope_typed'] = detection.typed(scope)
        with pytest.raises(ValueError, match='不接受处理位置裁剪'):
            with detection.open_reader(runtime, admission, request, guard=lambda: None):
                pytest.fail('其他 view 不得裁剪')


def test_original_current_and_budget_checks_remain_reachable(fixture):
    runtime, admission = fixture
    request = next(q for q in requests('detection', batch_rows=7, batch_bytes=4096,
                                     admission=admission) if q['view'] == 'qualified_revisions')
    bad = deepcopy(request); bad['batch_rows'] = 0
    with pytest.raises(ValueError, match='有限批预算'):
        with detection.open_reader(runtime, admission, bad, guard=lambda: None):
            pytest.fail('预算不得绕过')
    admission['inventory_digest'] = '1'*64
    with pytest.raises(ValueError, match='Admission摘要不符'):
        with detection.open_reader(runtime, admission, request, guard=lambda: None):
            pytest.fail('current 原摘要校验不得绕过')
