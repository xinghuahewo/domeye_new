"""Detection真实候选增量：仅显式绑定本任务原人工制品，不生产。"""
import os
import json
import hashlib
from pathlib import Path
from collections import Counter
from copy import deepcopy
import pytest

if os.environ.get('DOMEYE_DETECTION_REAL_INTEGRATION') != '8233':
    pytest.skip('仅本任务有限人工接合', allow_module_level=True)
import tests.integration.test_public_runtime_integration_8233 as t
from data_pipeline.analysis.detection.lake_integrity import Inventory

OLD = t.OUT
t.OUT = Path('/tmp/domeye-detection-real-integration-8233').resolve()
t.ROOTS = tuple(p for p in t.ROOTS if p != OLD) + (t.OUT,)

@pytest.fixture(scope='module', autouse=True)
def preserve():
    before = t.snapshot()
    assert before == json.loads((OLD/'最终登记.json').read_text())
    t.save('原登记前.json', before)
    yield
    after = t.snapshot()
    t.save('原登记后.json', after)
    for label, tables in before.items():
        for table, rows in tables.items():
            if label == 'detection' and table == t.dp.REGISTRY:
                assert not (Counter(map(t.up.typed, rows)) - Counter(map(t.up.typed, after[label][table])))
                assert len(after[label][table]) == len(rows) + 1
            else:
                assert rows == after[label][table]
    checks = json.loads((t.OUT/'历史保护前.json').read_text())
    for check in checks:
        q = json.loads(Path(check['source']).read_text())
        files = q.get('files', q)
        assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == sha for p, sha in files.items())
    t.save('历史保护后.json', checks)
    assert all(not list(p.iterdir()) for p in (t.OUT/'scratch').iterdir())


def upstream(mode):
    deps = json.loads((OLD/('Canonical-'+mode+'-dependencies.json')).read_text())
    b = t.up.untyped(deps[0]['owner_binding'])
    assert (b['run_id'], b['snapshot'], b['ordered_source_ids']) == (t.DQ['input_run'], t.DQ['input_snapshot'], t.DQ['ordered_sources'])
    assert [t.up.untyped(a['owner_binding'])['source_id'] for a in deps[1:]] == [r['source_id'] for r in t.DQ['references']]
    params = dict(dsn=t.MDSN, allowed_roots=t.ROOTS, scratch_root=t.scratch('上游-'+mode), dependency_admissions=(deps[0],), audit_sink=t.EVENTS.append)
    if mode == 'fixture':
        params['fixture_only'] = True
    else:
        params.update(execution_profile='real-candidate/v1', input_manifest=b['plan']['manifest'], expected_m2_binding=b, memory_limit='256MB', max_temp_bytes=512*1024**2, max_rss_bytes=2*1024**3, min_free_bytes=128*1024**2, lock_timeout_ms=2000)
    u = t.up.Runtime(**params)
    with t.measured('上游-'+mode+'-current', t.MDSN):
        for a in deps:
            t.up.verify_current(u, a, guard=lambda: None)
    t.save('上游-'+mode+'-原Admission.json', deps)
    return u, tuple(deps)


def test_detection_real_existing_gap():
    old_a = json.loads((OLD/'Detection-Admission.json').read_text())
    b = t.dp.untyped(old_a['owner_binding'])
    assert (b['run_id'], b['snapshot']) == ('0774d9ed031d4fb7b25b6ad8a8faff06', 7)
    runtimes = {}
    for mode in ('fixture', 'real'):
        u, deps = upstream(mode)
        params = dict(dsn=t.DDSN, output_root=t.DROOT/'output/business', allowed_roots=t.ROOTS, scratch_root=t.scratch('Detection-'+mode), dependency_admissions=deps, dependency_runtimes={a['admission_id']: u for a in deps}, audit_sink=t.EVENTS.append)
        if mode == 'fixture':
            params['fixture_only'] = True
        else:
            params.update(execution_profile='real-candidate/v1', expected_detection_binding=b, memory_bytes=256*1024**2, max_temp_bytes=512*1024**2, max_rss_bytes=2*1024**3, min_free_bytes=128*1024**2, lock_timeout_ms=2000)
        runtimes[mode] = t.dp.Runtime(**params)
    with t.measured('fixture-旧Admission-current', t.DDSN):
        with pytest.raises(ValueError, match='验证器身份已变化') as exc:
            t.dp.verify_current(runtimes['fixture'], old_a, guard=lambda: None)
    t.save('fixture-旧Admission-current.json', dict(result='旧验证器身份失效，原登记保留，不追加fixture准入', error=str(exc.value)))
    rt = runtimes['real']
    with t.measured('real-inspect', t.DDSN):
        assert t.dp.inspect_binding(rt, b['run_id'], b['snapshot']) == b
    with t.measured('real-admit', t.DDSN):
        a = t.dp.admit(rt, b, guard=lambda: None)
    t.save('real-Admission.json', a)
    with t.measured('real-current-reuse', t.DDSN):
        t.dp.verify_current(rt, a, guard=lambda: None)
        assert t.dp.admit(rt, b, guard=lambda: None) == a
    assert not any(e['kind'] in ('full_table_scan', 'body_query', 'entity_hash', 'admit_body_batch') for e in t.EVENTS)
    inv = Inventory()
    for table in t.di.TABLES:
        rows = t.consume(t.dp, rt, a, t.drequest(table), 'real-'+table)
        expected = t.dp.untyped(json.loads((OLD/('Detection-'+table+'.typed.json')).read_text()))
        assert t.dp.typed(rows) == t.dp.typed(expected)
        for row in rows:
            inv.add(table, row)
    assert inv.result() == json.loads((t.DROOT/'fresh-reader.json').read_text())
    t.save('原独立typed核验.json', inv.result())
    t.save('正向资源使用.json', deepcopy(rt.resource_usage))
    with t.measured('跨模式-current拒绝', t.DDSN):
        with pytest.raises(ValueError, match='验证器身份已变化'):
            t.dp.verify_current(runtimes['fixture'], a, guard=lambda: None)
    budget = rt.max_rss_bytes
    try:
        with t.measured('real-尾资源拒绝', t.DDSN):
            with pytest.raises(ValueError, match='RSS'):
                with t.dp.open_reader(rt, a, t.drequest('m3_entries'), guard=lambda: None) as reader:
                    list(reader)
                    rt.max_rss_bytes = 1
            assert reader.receipt is None
    finally:
        rt.max_rss_bytes = budget
    t.save('有限拒绝.json', dict(cross_mode_current=True, tail_resource=True, receipt=None))
