"""真实候选入口的人工控制路径验收；复用原M3，不代表真实全天结果。"""
from copy import deepcopy
from dataclasses import replace
import json
import os
from pathlib import Path
import time

import pytest

from data_pipeline.analysis.features import publication as p
from data_pipeline.bgp.archive import admission as m
from data_pipeline.bgp.archive.value_codec import typed, untyped
from data_pipeline.bgp.replay.route_replay import identity
from data_pipeline.analysis.features.qualified_read import read_windows, read_coverage


@pytest.fixture(scope='module')
def case():
    path = os.environ.get('DOMEYE_FEATURE_READER_CONTEXT')
    if not path: pytest.skip('需要显式绑定自有人工原M3')
    c = json.loads(Path(path).read_text()); root = Path(path).resolve().parent.parent
    out = root / 'feature-real-runtime'; deps = json.loads((out/'dependencies.json').read_text())
    return c, root, out, deps


def runtime(case, mode='real', **changes):
    c, root, out, groups = case; deps = groups[mode]['admissions']
    mb = untyped(deps[0]['owner_binding'])
    params = dict(dsn=c['dsn'], allowed_roots=(root,), scratch_root=out/'upstream-scratch', dependency_admissions=(deps[0],))
    if mode == 'synthetic': params['fixture_only'] = True
    else:
        params.update(execution_profile='real-candidate/v1', input_manifest=mb['plan']['manifest'], expected_m2_binding=mb,
                      memory_limit='256MB', max_temp_bytes=512*1024**2, max_rss_bytes=2*1024**3,
                      min_free_bytes=128*1024**2, lock_timeout_ms=2000)
    upstream = m.Runtime(**params)
    params = dict(dsn=c['dsn'], output_root=Path(c['root'])/'output', allowed_roots=(root,), scratch_root=out/'feature-scratch',
                  dependency_admissions=tuple(deps), dependency_runtimes={a['admission_id']: upstream for a in deps})
    if mode == 'synthetic': params['fixture_only'] = True
    else:
        params.update(execution_profile='real-candidate/v1', expected_feature_binding=c['binding'],
                      memory_limit='256MB', max_temp_bytes=512*1024**2, max_rss_bytes=2*1024**3,
                      min_free_bytes=128*1024**2, lock_timeout_ms=2000)
    return p.Runtime(**{**params, **changes})


@pytest.fixture(scope='module')
def admissions(case):
    c, _, out, _ = case; result = {}; costs = {}
    for mode in ('synthetic', 'real'):
        rt = runtime(case, mode); events = []; rt.audit_sink = events.append; start = time.monotonic()
        a = p.admit(rt, c['binding'], guard=lambda: None)
        result[mode] = a; costs[mode] = dict(seconds=time.monotonic()-start, usage=rt.resource_usage,
            full_audits=sum(e['kind']=='feature_full_audit' for e in events), parquet_groups=sum(e['kind']=='parquet_row_group' for e in events))
        (out/(mode+'-events.json')).write_text(json.dumps(events, ensure_ascii=False, indent=2))
    (out/'feature-admissions.json').write_text(json.dumps(result, ensure_ascii=False, indent=2))
    (out/'admit-cost.json').write_text(json.dumps(costs, ensure_ascii=False, indent=2))
    return result


def test_configuration_explicit_and_default_fixture(case):
    rt = runtime(case, 'synthetic')
    assert rt.fixture_only and rt.memory_limit == '256MB' and rt.min_free_bytes == 0
    for changes in ({'execution_profile':None}, {'execution_profile':'unknown'}, {'fixture_only':True},
                    {'expected_feature_binding':None}, {'dsn':''}, {'allowed_roots':()}):
        with pytest.raises(ValueError): runtime(case, **changes)
    for name in ('memory_limit', 'max_temp_bytes', 'max_rss_bytes', 'min_free_bytes', 'lock_timeout_ms'):
        with pytest.raises(ValueError, match='显式资源'): runtime(case, **{name:None})
    for name, value in [('max_temp_bytes', float('nan')), ('max_rss_bytes', float('inf')), ('memory_limit','unlimited'), ('min_free_bytes',0), ('lock_timeout_ms',True)]:
        with pytest.raises(ValueError): runtime(case, **{name:value})
    with pytest.raises(ValueError, match='未隔离'): runtime(case, scratch_root=rt.output_root)


def test_expected_binding_and_mode_mutation(case, admissions):
    c, _, _, _ = case
    wrong = deepcopy(c['binding']); wrong['snapshot'] += 1
    wrong['binding_id'] = identity({k:v for k,v in wrong.items() if k!='binding_id'})
    rt = runtime(case, expected_feature_binding=wrong)
    with pytest.raises(ValueError, match='固定范围'): p.admit(rt, c['binding'], guard=lambda:None)
    rt = runtime(case); rt.execution_profile = 'synthetic-fixture/v1'
    with pytest.raises(ValueError, match='模式漂移'): p.verify_current(rt, admissions['real'], guard=lambda:None)
    rt = runtime(case); rt.scratch_root = rt.output_root
    with pytest.raises(ValueError, match='固定范围漂移'): p.verify_current(rt, admissions['real'], guard=lambda:None)
    rt = runtime(case); rt.expected_feature_binding['specification']['result_window'] = None
    with pytest.raises(ValueError, match='固定范围漂移'): p.verify_current(rt, admissions['real'], guard=lambda:None)
    with pytest.raises(ValueError, match='validator'):
        p.verify_current(runtime(case, 'synthetic'), admissions['real'], guard=lambda:None)
    with pytest.raises(ValueError, match='validator'):
        p.verify_current(runtime(case), admissions['synthetic'], guard=lambda:None)


def test_real_upstream_mode_required(case):
    c, _, _, _ = case; artificial = runtime(case, 'synthetic')
    rt = runtime(case, dependency_admissions=artificial.dependency_admissions, dependency_runtimes=artificial.dependency_runtimes)
    with pytest.raises(ValueError, match='上游P1执行模式'): p.admit(rt, c['binding'], guard=lambda:None)


def test_real_full_values_current_and_reuse(case, admissions):
    c, _, out, _ = case; rt = runtime(case); a = admissions['real']; events = []; rt.audit_sink = events.append
    assert p.admit(rt, c['binding'], guard=lambda:None) == a
    p.verify_current(rt, a, guard=lambda:None)
    assert not any(e['kind'] in ('feature_full_audit','parquet_row_group','entity_hash') for e in events)
    results = {}
    for view, original in [('windows',read_windows),('coverage',read_coverage)]:
        req = dict(view=view, scope_typed=typed(dict(mode='all',window_role='all')), codec_version='m2-checkpoint-typed/v1',batch_rows=2,batch_bytes=128*1024)
        rows = []; start = time.monotonic(); first = None
        with p.open_reader(rt, a, req, guard=lambda:None) as session:
            for batch in session:
                if first is None: first=time.monotonic()-start
                rows.extend(untyped(batch['rows_typed']))
            assert session.receipt is None
        assert session.receipt is not None
        assert sorted(typed(r) for r in rows) == sorted(typed(r) for r in original(c['dsn'],c['binding']))
        results[view] = dict(seconds=time.monotonic()-start,first_seconds=first,rows=len(rows),receipt=session.receipt)
    assert admissions['synthetic']['admission_id'] != a['admission_id']
    assert admissions['synthetic']['owner_binding'] == a['owner_binding']
    assert admissions['synthetic']['inventory_digest'] == a['inventory_digest']
    assert admissions['synthetic']['validator']['rules_digest'] != a['validator']['rules_digest']
    (out/'read-cost.json').write_text(json.dumps(results,ensure_ascii=False,indent=2))
    (out/'read-events.json').write_text(json.dumps(events,ensure_ascii=False,indent=2))


def test_tail_mode_drift_no_receipt(case, admissions):
    rt=runtime(case);req=dict(view='coverage',scope_typed=typed(dict(mode='all',window_role='all')),codec_version='m2-checkpoint-typed/v1',batch_rows=1,batch_bytes=128*1024)
    with pytest.raises(ValueError,match='模式漂移'):
        with p.open_reader(rt,admissions['real'],req,guard=lambda:None) as session:
            list(session);rt.execution_profile='unknown'
    assert session.receipt is None


def test_real_resource_adjustment(case, admissions):
    rt=runtime(case);rt.max_rss_bytes=1
    with pytest.raises(ValueError,match='RSS/最低空闲盘'):p.verify_current(rt,admissions['real'],guard=lambda:None)
    rt=runtime(case);rt.min_free_bytes=10**30
    with pytest.raises(ValueError,match='RSS/最低空闲盘'):p.verify_current(rt,admissions['real'],guard=lambda:None)
