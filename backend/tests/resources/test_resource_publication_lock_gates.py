"""两个真实PG单锁门禁反例与一次完整读取；复用原人工健康制品。"""
from copy import deepcopy
import hashlib
import json
import time
import psycopg2
import pytest
from tests.resources.test_resource_publication_real import case, runtime, request
from data_pipeline.analysis.resources import publication as p
from data_pipeline.analysis.resources.publication_codec import typed, untyped
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader


@pytest.fixture(scope='module')
def accepted(case):
    rt=runtime(case);start=time.monotonic();a=p.admit(rt,case.b,guard=lambda:None)
    (case.out/'准入.json').write_text(json.dumps(dict(admission=a,seconds=time.monotonic()-start),ensure_ascii=False))
    return a


def update_blocked(c):
    pg=psycopg2.connect(c.params['dsn'])
    try:
        with pg.cursor() as cur:
            cur.execute("SET lock_timeout='50ms'")
            try:cur.execute('UPDATE domeye.resource_runs SET state=state WHERE run_id=%s',(c.b['run_id'],))
            except psycopg2.errors.LockNotAvailable:return True
            return False
    finally:pg.rollback();pg.close()


def target(a):return next(t for t in a['lock_targets'] if t['namespace']=='resource.run')


def test_wrong_complete_scope_cannot_acquire_original_lock(case,accepted):
    wrong=deepcopy(case.b);wrong['binding']['sources'][0]['purpose']='unknown'
    rt=runtime(case,expected_resource_binding=wrong)
    with pytest.raises(ValueError):p.verify_current(rt,accepted,guard=lambda:None)
    entered=False;blocked=None;error=None;start=time.monotonic();case.events.clear()
    try:
        with p.hold_lock(rt,accepted,target(accepted),guard=lambda:None):
            entered=True;blocked=update_blocked(case)
    except ValueError as exc:error=str(exc)
    (case.out/'错范围锁.json').write_text(json.dumps(dict(entered=entered,actual_update_blocked=blocked,error=error,seconds=time.monotonic()-start),ensure_ascii=False))
    assert not update_blocked(case)
    assert not entered and error is not None


def test_normal_exit_rechecks_resource_and_releases(case,accepted):
    rt=runtime(case);error=None;case.events.clear();start=time.monotonic()
    try:
        with p.hold_lock(rt,accepted,target(accepted),guard=lambda:None):
            assert update_blocked(case)
            rt.min_free_bytes=2**63-1
    except ValueError as exc:error=str(exc)
    with pytest.raises(ValueError):rt.resource_guard()
    assert not update_blocked(case)
    locks=[e for e in case.events if e['kind']=='pg_sql' and 'FOR SHARE' in e['sql']]
    assert len(locks)==1 and 'resource_runs' in locks[0]['sql']
    assert not any(e['kind'] in ('inventory_scan','science_compare','current_complete','entity_hash') for e in case.events)
    (case.out/'退出资源锁.json').write_text(json.dumps(dict(error=error,actual_lock_count=len(locks),released=True,seconds=time.monotonic()-start),ensure_ascii=False))
    assert error is not None


def test_normal_complete_metrics_preserve_values_and_binding(case,accepted):
    rt=runtime(case);begin=time.monotonic()
    original=p.inspect_binding(rt,case.b['run_id'],case.b['snapshot'],case.b['dataset_id'])
    with p.open_reader(rt,accepted,request(),guard=lambda:None) as session:
        rows=[r for batch in session for r in untyped(batch['rows_typed'])]
    old=ResourceObservationReader(rt.dsn,case.b['run_id'],case.b['snapshot'],case.b['dataset_id'])
    expected=list(old.values(target='metrics',scope='all'))
    def norm(r):return typed({**r,'qualification':sorted(r['qualification'],key=typed)})
    assert sorted(map(norm,rows))==sorted(map(norm,expected))
    assert session.receipt and session.receipt['rows']==len(rows)
    assert p.inspect_binding(rt,case.b['run_id'],case.b['snapshot'],case.b['dataset_id'])==original
    assert case.hashes=={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in case.root.rglob('*') if f.is_file()}
    assert not list(case.scratch.iterdir())
    (case.out/'正常读取.json').write_text(json.dumps(dict(rows=len(rows),seconds=time.monotonic()-begin,receipt=session.receipt,files_unchanged=len(case.hashes)),ensure_ascii=False))
