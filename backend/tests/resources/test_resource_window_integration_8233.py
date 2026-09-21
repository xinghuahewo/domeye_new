"""已接受Resource真实Runtime与Detection窗口的有限组合；只用自有人工输入。"""
import os
import json
import hashlib
import subprocess
import sys
import uuid
from pathlib import Path
from copy import deepcopy
from collections import Counter
from contextlib import closing
import pytest
import psycopg2
from psycopg2.extensions import make_dsn, parse_dsn

if os.environ.get('DOMEYE_RESOURCE_WINDOW_INTEGRATION') != '8233':
    pytest.skip('只运行明确授权的有限人工组合', allow_module_level=True)
import tests.integration.test_public_runtime_integration_8233 as t
import tests.resources.test_resource_p1_integration_8233 as r
from data_pipeline.analysis.detection.store import read_stored_rows
from data_pipeline.analysis.detection.lake_integrity import Inventory

OUT = Path('/tmp/domeye-resource-window-integration-8233').resolve()
OLD = t.OUT
RPREV = r.OUT
t.OUT = OUT
t.ROOTS = tuple(p for p in t.ROOTS if p != OLD) + (r.ROOT, OUT)
r.OUT = OUT
r.EVENTS = t.EVENTS
r.DSN = t.dsn(r.DSN)

@pytest.fixture(scope='module', autouse=True)
def preservation():
    prefix = os.environ.get('DOMEYE_WINDOW_FINISH', '')
    d_before = t.snapshot(); r_before = r.anchors()
    t.save(prefix+'原Detection及其他登记前.json', d_before); t.save(prefix+'原Resource登记前.json', r_before)
    yield
    d_after = t.snapshot(); r_after = r.anchors()
    t.save(prefix+'原Detection及其他登记后.json', d_after); t.save(prefix+'原Resource登记后.json', r_after)
    assert d_before == d_after
    for table, rows in r_before.items():
        if isinstance(rows, list): assert rows == r_after[table]
        else: assert all(r_after[table][k] == v for k, v in rows.items())
    checks = json.loads((OUT/'历史保护前.json').read_text())
    for c in checks:
        q = json.loads(Path(c['source']).read_text()); files = q.get('files', q)
        assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == sha for p, sha in files.items())
    t.save(prefix+'历史保护后.json', checks)
    assert all(not list(p.iterdir()) for p in (OUT/'scratch').iterdir())


def test_resource_real_and_lock_gate():
    b = json.loads((RPREV/'原binding.json').read_text())
    old_deps = json.loads((RPREV/'上游当前依赖.json').read_text())
    assert (b['run_id'], b['snapshot']) == ('c4b0419f11fc4d6ab7f4abb714ae1078', 67)
    assert all(s['context']['origin_uri'].startswith('fixture://') for s in b['binding']['sources'])
    deps = []; mapping = {}; upstream = None
    for old in old_deps:
        if old['owner'] != 'm2': continue
        ub = t.up.untyped(old['owner_binding'])
        upstream = t.up.Runtime(r.DSN, t.ROOTS, t.scratch('Resource上游'), execution_profile='real-candidate/v1', input_manifest=ub['plan']['manifest'], expected_m2_binding=ub, memory_limit='256MB', max_temp_bytes=512*1024**2, max_rss_bytes=2*1024**3, min_free_bytes=128*1024**2, lock_timeout_ms=2000, audit_sink=t.EVENTS.append)
        with t.measured('Resource上游必要real准入', r.DSN):
            assert t.up.inspect_binding(upstream, ub['run_id'], ub['snapshot'], ub['ordered_source_ids']) == ub
            ma = t.up.admit(upstream, ub, guard=lambda: None)
        deps.append(ma); upstream.dependency_admissions = (ma,); mapping[ma['admission_id']] = upstream
    csv = b['binding']['csv_reference']
    with t.measured('ResourceCSV必要real准入', r.DSN):
        ref = t.up.admit(upstream, t.up.reference_binding(upstream, ma, csv['source_id']), guard=lambda: None)
    deps.append(ref); mapping[ref['admission_id']] = upstream
    t.save('Resource-real依赖.json', deps)
    params = dict(dsn=r.DSN, allowed_roots=t.ROOTS, scratch_root=t.scratch('Resource'), upstream_runtime=None, dependency_admissions=tuple(deps), dependency_runtimes=mapping, execution_profile='real-candidate/v1', expected_resource_binding=b, output_root=r.ROOT/'resource', max_rows=1000000, max_bytes=256*1024**2, max_rss_bytes=2*1024**3, memory_bytes=256*1024**2, max_temp_bytes=512*1024**2, min_free_bytes=128*1024**2, lock_timeout_ms=2000, audit_sink=t.EVENTS.append)
    rt = r.p.Runtime(**params)
    assert rt.max_seconds is None
    with t.measured('Resource-inspect', r.DSN):
        assert r.p.inspect_binding(rt, b['run_id'], b['snapshot'], b['dataset_id']) == b
    with t.measured('Resource-admit', r.DSN): a = r.p.admit(rt, b, guard=lambda: None)
    t.save('Resource-Admission.json', a)
    with t.measured('Resource-current-reuse', r.DSN):
        r.p.verify_current(rt, a, guard=lambda: None)
        assert r.p.admit(rt, b, guard=lambda: None) == a
    assert not any(e['kind'] in ('science_compare','inventory_scan','entity_hash') for e in t.EVENTS)
    for view in ('metrics','normal_bands','topology_status','coverage'):
        with t.measured('Resource-'+view, r.DSN) as cost: rows, receipt = r.collect(rt, a, r.req(view), cost)
        expected = r.untyped(json.loads((RPREV/(view+'.typed.json')).read_text()))
        assert Counter(map(r.norm, rows)) == Counter(map(r.norm, expected))
        t.save('Resource-'+view+'.typed.json', r.typed(rows)); t.save('Resource-'+view+'.receipt.json', receipt)
    t.save('Resource正向资源.json', deepcopy(rt.resource_usage))
    lock = next(x for x in a['lock_targets'] if x['namespace'] == 'resource.run')
    def update(blocked):
        with closing(psycopg2.connect(r.DSN)) as pg, pg.cursor() as c:
            c.execute("SET LOCAL lock_timeout='50ms'")
            if blocked:
                with pytest.raises(psycopg2.errors.LockNotAvailable):
                    c.execute('UPDATE domeye.resource_runs SET state=state WHERE run_id=%s', (lock['key'],))
            else: c.execute('UPDATE domeye.resource_runs SET state=state WHERE run_id=%s', (lock['key'],))
            pg.rollback()
    wrong = deepcopy(b); wrong['binding']['sources'][0]['purpose'] = 'unknown'
    wrong_rt = r.p.Runtime(**dict(params, expected_resource_binding=wrong))
    with t.measured('Resource锁入口错范围', r.DSN):
        with pytest.raises(ValueError, match='绑定超出真实Resource固定范围'):
            with r.p.hold_lock(wrong_rt, a, lock, guard=lambda: None):
                pytest.fail('错误范围不应进入锁体')
        assert not any('FOR SHARE' in e.get('sql','') for e in t.EVENTS)
        update(False)
    original_free = rt.min_free_bytes
    try:
        with t.measured('Resource锁退出资源', r.DSN):
            with pytest.raises(ValueError, match='最低空闲盘'):
                with r.p.hold_lock(rt, a, lock, guard=lambda: None):
                    update(True); rt.min_free_bytes = 2**63-1
            rt.min_free_bytes = original_free
            update(False)
            assert sum('FOR SHARE' in e.get('sql','') for e in t.EVENTS) == 1
            assert not any(e['kind'] in ('current_complete','inventory_scan','science_compare','entity_hash') for e in t.EVENTS)
    finally: rt.min_free_bytes = original_free
    t.save('Resource锁门禁.json', dict(wrong_entry_rejected=True, body_not_entered=True, actual_update_blocked=True, resource_exit_rejected=True, lock_released=True, share_locks=1, all_probe_updates_rolled_back=True))


def test_detection_window_with_real_runtime():
    deps = json.loads((OLD/'Canonical-real-dependencies.json').read_text())
    ub = t.up.untyped(deps[0]['owner_binding'])
    assert (ub['run_id'], ub['snapshot']) == ('3884fdb6cdb7408d92f78a85db70a6af', 26)
    assert ub['ordered_source_ids'] == t.DQ['ordered_sources']
    assert [t.up.untyped(x['owner_binding'])['source_id'] for x in deps[1:]] == [x['source_id'] for x in t.DQ['references']]
    u = t.up.Runtime(t.MDSN, t.ROOTS, t.scratch('Detection上游'), execution_profile='real-candidate/v1', input_manifest=ub['plan']['manifest'], expected_m2_binding=ub, dependency_admissions=(deps[0],), memory_limit='256MB', max_temp_bytes=512*1024**2, max_rss_bytes=2*1024**3, min_free_bytes=128*1024**2, lock_timeout_ms=2000, audit_sink=t.EVENTS.append)
    with t.measured('Detection上游原real-current', t.MDSN):
        for dep in deps: t.up.verify_current(u, dep, guard=lambda: None)
    t.save('Detection原real依赖.json', deps)
    directory = OUT/'window'
    if not directory.exists():
        directory.mkdir()
        database_name = 'det_window_8233_' + uuid.uuid4().hex[:12]
        with closing(psycopg2.connect(make_dsn(**{**parse_dsn(t.MDSN), 'dbname':'postgres'}))) as pg:
            pg.autocommit = True
            with pg.cursor() as c: c.execute('CREATE DATABASE '+database_name+" ENCODING 'UTF8' TEMPLATE template0")
        q = deepcopy(t.DQ)
        q.update(observation_dsn=t.MDSN, detection_dsn=make_dsn(**{**parse_dsn(t.MDSN),'dbname':database_name}), output=str(directory/'output'), min_free_bytes=128*1024**2, max_rss_bytes=2*1024**3)
        q['scope'].update(window_start='1970-01-01T00:01:40Z', window_end='1970-01-01T00:01:54Z')
        for value in q['boundaries'].values(): value['observed_at'] = q['scope']['window_start']
        q['result_window'] = dict(window_start='1970-01-01T00:01:50Z', window_end_exclusive='1970-01-01T00:01:54Z')
        (directory/'request.json').write_text(json.dumps(q, ensure_ascii=False, indent=2))
        with t.measured('Detection窗口唯一冻结生产', q['detection_dsn']):
            with (directory/'process.log').open('w') as log:
                done = subprocess.run([sys.executable, 'scripts/pipeline/detection-frozen-run.py', str(directory/'request.json')], stdout=log, stderr=subprocess.STDOUT)
            assert done.returncode == 0, (directory/'process.log').read_text()
    q = json.loads((directory/'request.json').read_text())
    ready = json.loads((directory/'output/business/ready.json').read_text())
    b = {k:ready[k] for k in ('run_id','snapshot','identity','scope')}
    t.save('Detection新完整binding.json', b)
    rt = t.dp.Runtime(q['detection_dsn'], directory/'output/business', t.ROOTS, t.scratch('Detection'), tuple(deps), {x['admission_id']:u for x in deps}, execution_profile='real-candidate/v1', expected_detection_binding=b, memory_bytes=256*1024**2, max_temp_bytes=512*1024**2, max_rss_bytes=2*1024**3, min_free_bytes=128*1024**2, lock_timeout_ms=2000, audit_sink=t.EVENTS.append)
    with t.measured('Detection窗口-inspect', rt.dsn): assert t.dp.inspect_binding(rt,b['run_id'],b['snapshot']) == b
    with t.measured('Detection窗口-admit', rt.dsn): a = t.dp.admit(rt,b,guard=lambda:None)
    t.save('Detection窗口-Admission.json', a)
    with t.measured('Detection窗口-current-reuse', rt.dsn):
        t.dp.verify_current(rt,a,guard=lambda:None)
        assert t.dp.admit(rt,b,guard=lambda:None) == a
    assert not any(e['kind'] in ('full_table_scan','body_query','entity_hash') for e in t.EVENTS)
    original = {}; inv = Inventory()
    for table in t.di.TABLES:
        with t.measured('Detection窗口原Reader-'+table, rt.dsn): original[table] = list(read_stored_rows(rt.dsn,b['run_id'],b['snapshot'],table))
        actual = t.consume(t.dp, rt, a, t.drequest(table), 'Detection窗口-'+table)
        assert t.dp.typed(actual) == t.dp.typed(original[table])
        for row in actual: inv.add(table,row)
    t.save('Detection窗口原三表.json', {k:t.dp.typed(v) for k,v in original.items()})
    t.save('Detection窗口inventory.json', inv.result())
    selected = t.consume(t.dp,rt,a,t.drequest('result_revisions'),'Detection窗口-result_revisions')
    coverage = t.consume(t.dp,rt,a,t.drequest('result_coverage'),'Detection窗口-result_coverage')
    ids = {x['raw']['incident_id'] for x in selected}
    assert ids and [x['raw'] for x in selected] == [x for x in original['records'] if x['record_kind']=='business_revision' and x['incident_id'] in ids]
    assert [x['raw'] for x in coverage] == [x for x in original['m3_entries'] if x['kind']=='source_coverage']
    assert any(x['main'] is None and x['qualification']['coverage']=='unknown' for x in selected)
    assert any(json.loads(x['raw']['payload_json'])['coverage']=='unknown' for x in coverage)
    assert any(x['kind']=='scope_gap' for x in original['m3_entries'])
    finish_window(q,b,rt,a,original,selected,coverage)


def finish_window(q,b,rt,a,original,selected,coverage):
    ub = t.up.untyped(rt.dependency_admissions[0]['owner_binding'])
    ids = {x['raw']['incident_id'] for x in selected}
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.bgp.ordered_reader import MessageBoundary, ordered
    from data_pipeline.analysis.detection._results import plain
    reader = ObservationReader(t.MDSN,q['input_run'],q['input_snapshot'],q['ordered_sources'],profile='observation')
    from dataclasses import asdict
    messages = [item for item in ordered(reader) if isinstance(item, MessageBoundary)]
    raw = [json.loads(x['legacy_json']) for x in original['records'] if x['record_kind']=='source_message']
    assert t.dp.typed(raw) == t.dp.typed([plain(item.raw) for item in messages])
    ranks = b['identity']['window_coverage']['selected_source_ranks']
    assert [x['upstream_rank'] for x in ranks] == [x['upstream_rank'] for x in ub['selected_sources']]
    t.save('Detection窗口消息边界.json', [plain(asdict(x)) for x in messages])
    assert any(x['lifecycle']['start_utc'] is not None and x['lifecycle']['start_utc'] < '1970-01-01T00:01:50+00:00' for x in selected)
    t.save('Detection窗口语义.json', dict(selected_revisions=len(selected), selected_events=len(ids), full_pre_window_chain=True, raw_messages=len(raw), source_ranks=ranks, gap_preserved=True, unknown_preserved=True, coverage=b['identity']['window_coverage']))
    t.save('Detection窗口尾检查前Runtime状态.json',deepcopy(rt.resource_usage))
    original_rss = rt.max_rss_bytes
    try:
        with t.measured('Detection窗口尾资源失败',rt.dsn):
            with pytest.raises(ValueError,match='RSS'):
                with t.dp.open_reader(rt,a,t.drequest('result_revisions'),guard=lambda:None) as s:
                    list(s);rt.max_rss_bytes=1
            assert s.receipt is None
    finally: rt.max_rss_bytes=original_rss
    t.save('Detection窗口尾失败.json',dict(receipt=None,resource_rejected=True))


def test_finish_existing_window():
    if not os.environ.get('DOMEYE_WINDOW_FINISH'):
        pytest.skip('仅补已完成窗口的剩余边界')
    q=json.loads((OUT/'window/request.json').read_text())
    b=json.loads((OUT/'Detection新完整binding.json').read_text())
    a=json.loads((OUT/'Detection窗口-Admission.json').read_text())
    deps=json.loads((OUT/'Detection原real依赖.json').read_text())
    ub=t.up.untyped(deps[0]['owner_binding'])
    u=t.up.Runtime(t.MDSN,t.ROOTS,t.scratch('Detection上游'),execution_profile='real-candidate/v1',input_manifest=ub['plan']['manifest'],expected_m2_binding=ub,dependency_admissions=(deps[0],),memory_limit='256MB',max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,min_free_bytes=128*1024**2,lock_timeout_ms=2000,audit_sink=t.EVENTS.append)
    rt=t.dp.Runtime(q['detection_dsn'],OUT/'window/output/business',t.ROOTS,t.scratch('Detection'),tuple(deps),{x['admission_id']:u for x in deps},execution_profile='real-candidate/v1',expected_detection_binding=b,memory_bytes=256*1024**2,max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,min_free_bytes=128*1024**2,lock_timeout_ms=2000,audit_sink=t.EVENTS.append)
    original={k:t.dp.untyped(v) for k,v in json.loads((OUT/'Detection窗口原三表.json').read_text()).items()}
    selected=t.dp.untyped(json.loads((OUT/'Detection窗口-result_revisions.typed.json').read_text()))
    coverage=t.dp.untyped(json.loads((OUT/'Detection窗口-result_coverage.typed.json').read_text()))
    finish_window(q,b,rt,a,original,selected,coverage)
