"""共享构造线程预算的实际M2只读接合；只重新准入使用到的原人工绑定。"""
import os
import json
import hashlib
import inspect
from pathlib import Path
from dataclasses import asdict
from collections import Counter
from contextlib import closing
import pytest
import psycopg2

if os.environ.get('DOMEYE_THREADS_INTEGRATION') != '8233':
    pytest.skip('仅明确授权的8233原人工M2', allow_module_level=True)
import tests.integration.test_public_runtime_integration_8233 as t
import tests.resources.test_resource_p1_integration_8233 as r
from data_pipeline.bgp.archive import admission as p, store
from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch
from data_pipeline.bgp.ordered_reader import ordered, binding_from_reader, MessageBoundary
from data_pipeline.bgp.record_types import metadata_json
from data_pipeline.analysis.detection._results import plain

OLD = t.OUT
OUT = Path('/tmp/domeye-duckdb-threads-integration-8233').resolve()
t.OUT = OUT
t.ROOTS = tuple(x for x in t.ROOTS if x != OLD) + (OUT,)
RAW = Path('/tmp/domeye-m2-p1-integration-8233')
PREVIOUS = Path('/tmp/domeye-runtime-integration-8233')
WINDOW = Path('/tmp/domeye-resource-window-integration-8233')
PHASE = 'setup'


def window_registry():
    q = json.loads((WINDOW/'window/request.json').read_text())
    with closing(psycopg2.connect(q['detection_dsn'])) as pg, pg.cursor() as c:
        c.execute('SELECT to_jsonb(t) FROM detection.runs t'); runs = [x[0] for x in c.fetchall()]
        c.execute('SELECT to_jsonb(t) FROM detection.publication_admissions t'); admissions = [x[0] for x in c.fetchall()]
    return dict(runs=runs, admissions=admissions)


@pytest.fixture(scope='module', autouse=True)
def preserved_and_observed():
    assert os.environ['DOMEYE_DUCKDB_THREADS'] == '2'
    before = t.snapshot(); resource_before = r.anchors(); window_before = window_registry()
    assert window_before == json.loads((WINDOW/'新Detection登记.json').read_text())
    t.save('原登记前.json', before); t.save('原Resource登记前.json', resource_before); t.save('原窗口登记前.json', window_before)
    native = store.duckdb.connect; connections = []
    def capture(*args, **kwargs):
        # 调用真实native构造器，不替换连接、查询、资源保护或返回类型。
        db = native(*args, **kwargs)
        try:
            actual = db.execute("SELECT current_setting('threads')").fetchone()[0]
            callers = [Path(frame.filename).name+':'+frame.function for frame in inspect.stack()[1:7]]
            connections.append(dict(phase=PHASE, path=str(args[0]) if args else ':memory:', constructor_config=kwargs.get('config'), actual_threads=actual, callers=callers))
            assert kwargs.get('config') == {'threads':2} and actual == 2
            return db
        except BaseException:
            db.close(); raise
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(store.duckdb, 'connect', capture)
        yield connections
    after = t.snapshot()
    t.save('原登记后.json', after); t.save('实际共享构造线程.json', connections)
    for label, tables in before.items():
        for table, rows in tables.items():
            if label == 'm2canonical' and table in ('observation_publication.m2_admissions','observation_publication.reference_admissions'):
                assert len(after[label][table]) == len(rows)+1
                assert not (Counter(map(p.typed,rows))-Counter(map(p.typed,after[label][table])))
            else: assert rows == after[label][table]
    assert r.anchors() == resource_before and window_registry() == window_before
    assert r.hashes() == json.loads(Path('/tmp/domeye-resource-close-integration-8233/原目录SHA.json').read_text())
    original = json.loads((PREVIOUS/'原文件SHA.json').read_text())
    assert all(hashlib.sha256(Path(path).read_bytes()).hexdigest()==sha for path,sha in original.items())
    checks = json.loads((OUT/'历史保护前.json').read_text())
    for c in checks:
        q = json.loads(Path(c['source']).read_text()); files = q.get('files',q)
        assert all(hashlib.sha256(Path(path).read_bytes()).hexdigest()==sha for path,sha in files.items())
    t.save('历史保护后.json',checks)
    assert all(not list(path.iterdir()) for path in (OUT/'scratch').iterdir())


def request(a, view):
    b = p.untyped(a['owner_binding'])
    if b['owner']=='reference': sources=[b['source_id']]
    elif view=='references': sources=[x['source_id'] for x in b['reference_sources']]
    else: sources=b['ordered_source_ids']
    return dict(view=view,scope_typed=p.typed(dict(source_ids=sources)),codec_version=p.CODEC,batch_rows=17,batch_bytes=4*1024**2)


def test_existing_m2_public_chain_and_threads(preserved_and_observed):
    global PHASE
    old_deps = json.loads((OLD/'Canonical-real-dependencies.json').read_text())
    old_m2 = old_deps[0]; b = p.untyped(old_m2['owner_binding'])
    assert (b['run_id'],b['snapshot']) == ('3884fdb6cdb7408d92f78a85db70a6af',26)
    assert b == json.loads((RAW/'原绑定.json').read_text())
    sid = b['reference_sources'][0]['source_id']
    old_ref = next(x for x in old_deps[1:] if p.untyped(x['owner_binding'])['source_id']==sid)
    PHASE = 'Runtime构造'
    rt = p.Runtime(t.MDSN,t.ROOTS,t.scratch('M2'),execution_profile='real-candidate/v1',input_manifest=b['plan']['manifest'],expected_m2_binding=b,dependency_admissions=(old_m2,),memory_limit='256MB',max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,min_free_bytes=128*1024**2,lock_timeout_ms=2000,audit_sink=t.EVENTS.append)
    PHASE = '旧规则拒绝'
    errors = {}
    for a in (old_m2,old_ref):
        with t.measured(a['owner']+'旧规则current',rt.dsn):
            with pytest.raises(ValueError,match='源码版本') as exc: p.verify_current(rt,a,guard=lambda:None)
        errors[a['admission_id']] = str(exc.value)
    t.save('旧规则拒绝.json',errors)
    PHASE = 'M2准入'
    with t.measured('M2-inspect-admit',rt.dsn):
        assert p.inspect_binding(rt,b['run_id'],b['snapshot'],b['ordered_source_ids']) == b
        a = p.admit(rt,b,guard=lambda:None)
    t.save('新M2-Admission.json',a); rt.dependency_admissions=(a,)
    PHASE = '参考准入'
    with t.measured('参考-binding-admit',rt.dsn):
        rb = p.reference_binding(rt,a,sid)
        ref = p.admit(rt,rb,guard=lambda:None)
    t.save('新参考-Admission.json',ref)
    for item in (a,ref):
        PHASE = item['owner']+'current-reuse'
        with t.measured(PHASE,rt.dsn):
            p.verify_current(rt,item,guard=lambda:None)
            assert p.admit(rt,p.untyped(item['owner_binding']),guard=lambda:None)==item
        assert not any(e['kind'] in ('entity_hash','parquet_read','decoded_batch') for e in t.EVENTS)
    summary = {}
    for item in (a,ref):
        for table in (tuple(p.OBS_TABLES) if item is a else ('references',)):
            PHASE = item['owner']+'-'+table+'公开读取'
            rows = t.consume(p,rt,item,request(item,table),PHASE)
            expected = (PREVIOUS/(item['owner']+'_'+table+'.typed.json')).read_text()
            assert p.typed(rows) == expected
            summary[PHASE] = len(rows)
    PHASE = '原Consumer有序流'
    reader = ObservationReader(rt.dsn,b['run_id'],b['snapshot'],b['ordered_source_ids'],profile='observation',batch_rows=17,guard=rt.resource_guard)
    direct={source:{'boundary':[],'quality':[],'message':[],'element':[]} for source in reader.sources}
    with t.measured(PHASE,rt.dsn):
        for item in reader.stream():
            target=direct[item.source_id]
            if isinstance(item,MessageBatch):
                target['quality'].extend(item.source_quality);target['message'].extend(item.messages);target['element'].extend(item.elements)
            else: target['boundary'].append(asdict(item))
    assert p.typed(direct) == (RAW/'原CPconsumer.typed.json').read_text()
    t.save('原Consumer流.typed.json',p.typed(direct))
    PHASE = '原Ordered逻辑顺序'
    with t.measured(PHASE,rt.dsn):
        assert metadata_json(binding_from_reader(reader)) == b['input_binding']
        items = list(ordered(reader))
    boundaries = [plain(asdict(x)) for x in items if isinstance(x,MessageBoundary)]
    assert boundaries == json.loads((WINDOW/'Detection窗口消息边界.json').read_text())
    t.save('原Ordered完整流.typed.json',p.typed([asdict(x) for x in items]))
    t.save('原逻辑顺序身份.json',dict(input_binding=b['input_binding'],input_binding_id=b['input_binding_id'],ordered_source_ids=b['ordered_source_ids'],selected_sources=b['selected_sources'],message_boundaries=len(boundaries),ordered_items=len(items)))
    observed=preserved_and_observed
    assert observed and all(c['actual_threads']==2 for c in observed)
    assert any('publication_validation.py:_stage' in c['callers'] and '公开读取' in c['phase'] for c in observed)
    assert any('consumer.py:connect' in c['callers'] and c['phase']=='原Ordered逻辑顺序' for c in observed)
    t.save('读取行数.json',summary)
    t.save('资源预算.json',dict(duckdb_threads=2,memory_limit=rt.memory_limit,max_temp_bytes=rt.max_temp_bytes,max_rss_bytes=rt.max_rss_bytes,min_free_bytes=rt.min_free_bytes,lock_timeout_ms=rt.lock_timeout_ms,usage=rt.resource_usage,processing_deadline=None))
