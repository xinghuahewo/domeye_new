"""real-candidate 控制路径的合成验收：复用显式自有 sealed M2，不生产真实数据。"""
import copy
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import psycopg2
import pytest
from jsonschema import ValidationError

from data_pipeline.bgp.archive import admission as p
from data_pipeline.bgp.archive import validation as v
from data_pipeline.bgp.archive.value_codec import typed, untyped, CODEC


@pytest.fixture(scope='module')
def case():
    config=os.environ.get('DOMEYE_P1_REAL_CASE_JSON')
    if not config:pytest.skip('需显式自有人工sealed M2配置，不自动生成输入')
    doc=json.loads(Path(config).read_text())
    previous=[json.loads(Path(path).read_text()) for path in doc['old_admissions']]
    binding=untyped(previous[0]['owner_binding'])
    root=Path(doc['source']);scratch=Path(doc['scratch']);out=Path(doc['evidence'])
    params=dict(dsn=doc['dsn'],allowed_roots=(root,scratch),scratch_root=scratch,
                execution_profile='real-candidate/v1',input_manifest=binding['plan']['manifest'],expected_m2_binding=binding,
                memory_limit='256MB',max_temp_bytes=512*1024**2,lock_timeout_ms=2000,max_rss_bytes=4*1024**3,min_free_bytes=128*1024**2)
    return SimpleNamespace(params=params,binding=binding,root=root,scratch=scratch,out=out,previous=previous)


def runtime(case,**changes):return p.Runtime(**{**case.params,**changes})


def req(a,table='messages',batch=17):
    b=untyped(a['owner_binding'])
    sources=(b['ordered_source_ids'] if table!='references' else [s['source_id'] for s in b['reference_sources']]) if a['owner']=='m2' else [b['source_id']]
    return dict(view=table,scope_typed=typed(dict(source_ids=sources)),codec_version=CODEC,batch_rows=batch,batch_bytes=1024**2)


def test_modes_explicit_budgets_manifest_and_isolation(case):
    with pytest.raises(ValueError):runtime(case,execution_profile=None)
    with pytest.raises(ValueError):runtime(case,execution_profile='unknown')
    with pytest.raises(ValueError):runtime(case,fixture_only=True)
    for name in ('memory_limit','max_temp_bytes','lock_timeout_ms','max_rss_bytes','min_free_bytes'):
        with pytest.raises(ValueError,match='显式资源'):runtime(case,**{name:None})
    for name,value in [('memory_limit','unlimited'),('max_temp_bytes',True),('lock_timeout_ms',0),('max_rss_bytes',-1),('min_free_bytes',0)]:
        with pytest.raises(ValueError,match='有限正值'):runtime(case,**{name:value})
    for change in ('collector','uri','order','reference'):
        manifest=copy.deepcopy(case.params['input_manifest'])
        if change=='collector':manifest['collector']='foreign'
        elif change=='uri':manifest['inputs'][0]['origin_uri']+='-wrong'
        elif change=='order':manifest['inputs'][1:]=reversed(manifest['inputs'][1:])
        else:manifest['references'][0]['sha256']='f'*64
        with pytest.raises((ValueError,ValidationError)):runtime(case,input_manifest=manifest)
    with pytest.raises(ValueError,match='未隔离'):
        runtime(case,scratch_root=Path(case.params['input_manifest']['inputs'][0]['path']).parent)
    with pytest.raises(ValueError,match='允许根'):
        runtime(case,allowed_roots=(case.scratch,))
    alias=copy.deepcopy(case.params['input_manifest']);alias['inputs'].append(copy.deepcopy(alias['inputs'][0]))
    rt=runtime(case,input_manifest=alias)  # 原规则的同源别名不创造来源。
    assert p.inspect_binding(rt,case.binding['run_id'],case.binding['snapshot'],case.binding['ordered_source_ids'])==case.binding
    rt.input_manifest['collector']='drift'
    with pytest.raises(ValueError,match='漂移'):rt.check_binding(case.binding)


def test_actual_scope_rejection_and_internal_resources(case):
    rt=runtime(case);b=case.binding
    for run,snapshot,sources in [(b['run_id'],b['snapshot']+1,b['ordered_source_ids']),('f'*32,b['snapshot'],b['ordered_source_ids']),(b['run_id'],b['snapshot'],list(reversed(b['ordered_source_ids'])))]:
        with pytest.raises(ValueError,match='固定M2'):p.inspect_binding(rt,run,snapshot,sources)
    wrong=copy.deepcopy(b);wrong['snapshot']+=1
    with pytest.raises(ValueError):p.admit(rt,wrong,guard=lambda:None)
    # 声明本身不是可信凭据：Runtime接受完整候选值后仍须实际Selection核对。
    claimed=runtime(case,expected_m2_binding=wrong)
    with pytest.raises(ValueError):p.inspect_binding(claimed,wrong['run_id'],wrong['snapshot'],wrong['ordered_source_ids'])
    rt.max_rss_bytes=1
    with pytest.raises(ValueError,match='RSS'):p.inspect_binding(rt,b['run_id'],b['snapshot'],b['ordered_source_ids'])
    with pytest.raises(ValueError,match='RSS'):p.admit(rt,b,guard=lambda:None)
    rt.max_rss_bytes=case.params['max_rss_bytes'];rt.min_free_bytes=2**63-1
    with pytest.raises(ValueError,match='空闲盘'):p.admit(rt,b,guard=lambda:None)


@pytest.fixture(scope='module')
def accepted(case):
    rt=runtime(case)
    synthetic=p.Runtime(case.params['dsn'],case.params['allowed_roots'],case.scratch,fixture_only=True)
    a_fixture=p.admit(synthetic,case.binding,guard=lambda:None)
    a=p.admit(rt,case.binding,guard=lambda:None);rt.dependency_admissions=(a,)
    reference=p.reference_binding(rt,a,case.binding['reference_sources'][0]['source_id'])
    r=p.admit(rt,reference,guard=lambda:None)
    for label,value in [('人工模式Admission',a_fixture),('真实候选模式合成Admission',a),('真实候选模式合成reference',r)]:
        (case.out/(label+'.json')).write_text(json.dumps(value,ensure_ascii=False,indent=2))
    return rt,synthetic,a,r,a_fixture


def test_mode_identity_reuse_eight_views_originals(case,accepted,monkeypatch):
    rt,synthetic,a,r,fixture_a=accepted
    files={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in case.root.rglob('*') if f.is_file()}
    assert a['admission_id']!=fixture_a['admission_id'] and p._own_target(a)['key']!=p._own_target(fixture_a)['key']
    assert a['validator']['rules_digest']!=fixture_a['validator']['rules_digest']
    for target,admission in [(rt,fixture_a),(synthetic,a)]:
        with pytest.raises(ValueError,match='模式'):p.verify_current(target,admission,guard=lambda:None)
        with pytest.raises(ValueError,match='模式'):
            with p.hold_lock(target,admission,p._own_target(admission),guard=lambda:None):pass
        with pytest.raises(ValueError,match='模式'):
            with p.open_reader(target,admission,req(admission),guard=lambda:None):pass
    with pytest.raises(ValueError,match='模式'):p.reference_binding(rt,fixture_a,case.binding['reference_sources'][0]['source_id'])
    wrong_rt=runtime(case,dependency_admissions=(fixture_a,))
    with pytest.raises(ValueError):p.admit(wrong_rt,untyped(r['owner_binding']),guard=lambda:None)
    # 无关HEAD不是current身份：复用不重新取登记提交，也不能使旧owner_revision失效。
    with monkeypatch.context() as patch:
        patch.setattr(p.subprocess,'check_output',lambda *a,**kw:(_ for _ in ()).throw(AssertionError('不应查询HEAD')))
        p.verify_current(rt,a,guard=lambda:None)
        assert p.admit(rt,case.binding,guard=lambda:None)==a
        assert p.admit(rt,untyped(r['owner_binding']),guard=lambda:None)==r
    selection=p.Selection(rt.dsn,case.binding['run_id'],case.binding['snapshot']);db=selection.connect()
    evidence={};receipts={}
    try:
        for admission in (a,r):
            for table in (tuple(p.OBS_TABLES) if admission['owner']=='m2' else ('references',)):
                expected=[]
                for source in untyped(req(admission,table)['scope_typed'])['source_ids']:
                    result=db.execute('SELECT * FROM '+selection.table(table,source)+' ORDER BY '+v.ORDERS[table])
                    names=[d[0] for d in result.description];expected.extend(dict(zip(names,row)) for row in result.fetchall())
                for batch in (1,17):
                    rows=[]
                    with p.open_reader(rt,admission,req(admission,table,batch),guard=lambda:None) as session:
                        for chunk in session:rows.extend(untyped(chunk['rows_typed']))
                        assert session.receipt is None
                    assert typed(rows)==typed(expected) and session.receipt['execution']=='complete'
                    key=admission['owner']+'/'+table+'/'+str(batch);evidence[key]=typed(rows);receipts[key]=session.receipt
                assert receipts[admission['owner']+'/'+table+'/1']['typed_digest']==receipts[admission['owner']+'/'+table+'/17']['typed_digest']
    finally:db.close()
    with psycopg2.connect(rt.dsn) as pg,pg.cursor() as cursor:
        for old in case.previous:
            cursor.execute('SELECT admission FROM '+p.TABLES[old['owner']+'.admission']+' WHERE record_key=%s',(p._own_target(old)['key'],))
            assert cursor.fetchone()[0]==old
            with pytest.raises(ValueError,match='源码版本'):p.verify_current(rt,old,guard=lambda:None)
    assert files=={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in case.root.rglob('*') if f.is_file()}
    assert case.binding['selected_sources'][0]['upstream_rank']==0 and case.binding['reference_sources'][0]['checkpoint_ordinal']==0
    assert untyped(r['owner_binding'])['selected_sources']==[]
    (case.out/'完整八表typed.json').write_text(json.dumps(evidence,ensure_ascii=False))
    (case.out/'完整ReadReceipt.json').write_text(json.dumps(receipts,ensure_ascii=False,indent=2))
    (case.out/'原文件与资源检查.json').write_text(json.dumps(dict(files=files,resource_usage=rt.resource_usage,old_records_unchanged=True),ensure_ascii=False,indent=2))


def test_real_lifecycle_guard_and_representative_lock(case,accepted):
    rt,_,a,r,_=accepted
    with p.open_reader(rt,a,req(a,batch=1),guard=lambda:None) as early:next(early)
    assert early.receipt is None
    fail=False
    def tail_guard():
        if fail:raise ValueError('调用方尾保护')
    with pytest.raises(ValueError,match='调用方尾保护'):
        with p.open_reader(rt,a,req(a),guard=tail_guard) as tail:
            list(tail);fail=True
    assert tail.receipt is None
    original=rt.max_temp_bytes;rt.max_temp_bytes=1
    try:
        with pytest.raises(ValueError,match='临时磁盘'):
            with p.open_reader(rt,a,req(a),guard=lambda:None) as limited:list(limited)
        assert limited.receipt is None
    finally:rt.max_temp_bytes=original
    original=rt.max_rss_bytes
    try:
        with pytest.raises(ValueError,match='RSS'):
            with p.open_reader(rt,a,req(a,batch=1),guard=lambda:None) as limited:
                next(limited);rt.max_rss_bytes=1;next(limited)
        assert limited.receipt is None
    finally:rt.max_rss_bytes=original
    assert not list(case.scratch.iterdir())
    for admission in (a,r):
        table=p.TABLES[admission['owner']+'.admission'];key=p._own_target(admission)['key']
        with p.hold_lock(rt,admission,p._own_target(admission),guard=lambda:None):
            other=psycopg2.connect(rt.dsn)
            try:
                with other.cursor() as c:
                    c.execute("SET lock_timeout='50ms'")
                    with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute('UPDATE '+table+' SET state=state WHERE record_key=%s',(key,))
            finally:other.rollback();other.close()
        with psycopg2.connect(rt.dsn) as pg,pg.cursor() as c:c.execute('UPDATE '+table+' SET state=state WHERE record_key=%s',(key,))
    p.verify_current(rt,a,guard=lambda:None);p.verify_current(rt,r,guard=lambda:None)
