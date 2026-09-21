"""单点登记事实fixture：真实current/hold/lease控制流，无PG或Reader。"""
from contextlib import contextmanager
from copy import deepcopy
from collections import Counter
from types import SimpleNamespace
import pytest
from psycopg2.extensions import TRANSACTION_STATUS_INTRANS
from data_pipeline.analysis.country_trends import result_admission as owner
from data_pipeline.common import admission_locks as locks
from data_pipeline.results import component_readers as combined_access


@pytest.fixture
def env(monkeypatch):
    counts = Counter()
    audit = {'catalog_digest': owner.digest(owner.schema.encode({}))}
    physical = {'system_identifier': 'fixture-system', 'database_oid': 42}
    target = dict(stage=60, **physical, namespace='trend.admission', key='fixture-key')
    ad = dict(owner='trend', admission_id='fixture-ad', physical=physical,
              owner_binding=owner.schema.encode({'binding': {}}), entities=[], dependencies=[],
              validator={'validation_digest': owner.digest(audit)},
              lock_targets=[dict(target, namespace='trend.component'), target])
    def mark(name):
        def call(*a, **kw): counts[name] += 1
        return call
    rt = SimpleNamespace(dsn='host=/fixture dbname=fixture', lock_timeout_ms=2000, dependency_admissions=[],
        guarded=lambda guard: guard, dependencies=mark('dependencies'))
    class Cursor:
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def execute(self, sql, args=None):
            self.sql=sql
            if 'SELECT state,admission,audit' in sql:
                counts['locked_select' if 'FOR SHARE' in sql else 'anchor_select'] += 1
        def fetchone(self):
            if 'pg_control_system' in self.sql: return ('fixture-system',42)
            return ('accepted', deepcopy(ad), deepcopy(audit))
    class PG:
        closed=False;autocommit=False;status=TRANSACTION_STATUS_INTRANS
        snapshot=('fixture-system',42,123,'u','u','r','public','off','read committed')
        def set_session(self, **kw): pass
        def get_transaction_status(self): return self.status
        def get_dsn_parameters(self): return {'host':'/fixture','dbname':'fixture'}
        def cursor(self): return Cursor()
        def rollback(self): self.status=0
        def close(self): self.closed=True
    pg=PG()
    def snapshot(conn): counts['lease_checks']+=1;return conn.snapshot
    monkeypatch.setattr(locks,'_snapshot',snapshot)
    monkeypatch.setattr(locks.psycopg2,'connect',lambda dsn: pg)
    @contextmanager
    def connect(runtime):
        counts['anchor_connections']+=1
        yield PG()
    monkeypatch.setattr(owner.pg_api,'_pg',connect)
    monkeypatch.setattr(owner,'_shape',mark('shape'))
    def parts(*a): counts['parts']+=1;return dict(system_id='fixture-system',database_oid=42)
    monkeypatch.setattr(owner,'_parts',parts)
    def rules(*a): counts['rules']+=1;return {}
    monkeypatch.setattr(owner,'_rules',rules)
    monkeypatch.setattr(owner.pg_api,'_entities_current',mark('entities'))
    monkeypatch.setattr(owner.parts,'current_parts',mark('part_entities'))
    def catalog(*a): counts['catalog']+=1;return {}
    monkeypatch.setattr(owner,'catalog',catalog)
    original_digest=owner.digest
    def digest(value):
        if isinstance(value,dict) and 'catalog_digest' in value: counts['audit_digest']+=1
        return original_digest(value)
    monkeypatch.setattr(owner,'digest',digest)
    return SimpleNamespace(rt=rt,ad=ad,target=target,pg=pg,audit=audit,counts=counts)


@contextmanager
def held(e):
    with locks.LockConnections() as scope:
        lease=scope.borrow(e.rt,e.target,guard=lambda:None)
        with owner.hold_lock(e.rt,e.ad,e.target,guard=lambda:None,lock_connection=lease) as fact:
            yield fact


def current(e,ad=None,rt=None):
    return owner.verify_current(rt or e.rt,ad or e.ad,guard=lambda:None)


def test_real_current_replaces_only_anchor(env):
    e=env
    with held(e) as fact:
        current(e)  # 原hold的_anchor和首次current分别保留
        assert e.counts['anchor_select']==2 and e.counts['locked_select']==1
        before=e.counts.copy()
        with owner.use_locked_admission(e.rt,e.ad,fact):
            for _ in range(3): current(e)
            value=owner._current_anchor(e.rt,e.ad);value['catalog_digest']='changed'
            assert owner._current_anchor(e.rt,e.ad)==e.audit
        for key in ('anchor_select','anchor_connections','audit_digest'):
            assert e.counts[key]==before[key]
        for key in ('shape','parts','rules','entities','part_entities','catalog','dependencies'):
            assert e.counts[key]-before[key]==3
        assert e.counts['lease_checks']>before['lease_checks']
        print({'阶段':'首次锁及原current', '计数':dict(before),
               '启用context后增量':dict(e.counts-before)})
        current(e)
        assert e.counts['anchor_select']==before['anchor_select']+1
    assert owner._anchor_context.get() is None and not owner._anchor_facts


@pytest.mark.parametrize('change',['ad','runtime','target','physical','forged'])
def test_bad_binding_fails_without_anchor_fallback(env,change):
    e=env
    with held(e) as fact:
        with owner.use_locked_admission(e.rt,e.ad,fact):
            a=deepcopy(e.ad);rt=e.rt
            if change=='ad': a['admission_id']='other'
            elif change=='runtime': rt=SimpleNamespace(**vars(e.rt))
            elif change=='target': a['lock_targets'][1]['key']='other'
            elif change=='physical': a['physical']['database_oid']=43
            else:
                token=owner._anchor_context.set(owner._AnchorFact())
            count=e.counts['anchor_select']
            try:
                with pytest.raises(ValueError): current(e,a,rt)
            finally:
                if change=='forged': owner._anchor_context.reset(token)
            assert e.counts['anchor_select']==count


@pytest.mark.parametrize('change',['rollback','commit','new-transaction','closed','parameters','physical-lease','scope-exit'])
def test_invalid_lease_never_falls_back(env,change):
    e=env
    with held(e) as fact:
        with owner.use_locked_admission(e.rt,e.ad,fact):
            if change in ('rollback','commit'): e.pg.status=0
            elif change=='new-transaction': e.pg.snapshot=(*e.pg.snapshot[:2],124,*e.pg.snapshot[3:])
            elif change=='closed': e.pg.closed=True
            elif change=='physical-lease': e.pg.snapshot=('other-system',*e.pg.snapshot[1:])
            elif change=='parameters': e.pg.get_dsn_parameters=lambda:{'dbname':'other'}
            else: owner._anchor_facts[fact][3].scope._active=False
            before=e.counts['anchor_select']
            with pytest.raises(ValueError): current(e)
            assert e.counts['anchor_select']==before


@pytest.mark.parametrize('check',['rules','entities'])
def test_unprotected_drift_still_fails(env,monkeypatch,check):
    e=env
    with held(e) as fact:
        with owner.use_locked_admission(e.rt,e.ad,fact):
            if check=='rules': monkeypatch.setattr(owner,'_rules',lambda rt:{'changed':True})
            else:
                def drift(*a): raise ValueError('file drift')
                monkeypatch.setattr(owner.pg_api,'_entities_current',drift)
            with pytest.raises(ValueError): current(e)


def test_audit_changed_after_entry_anchor_cannot_issue(env,monkeypatch):
    e=env;original=owner._anchor
    def anchor(*args):
        result=original(*args);e.audit['catalog_digest']='bad';return result
    monkeypatch.setattr(owner,'_anchor',anchor)
    with pytest.raises(ValueError,match='lock_anchor_audit'):
        with held(e): pass
    assert not owner._anchor_facts


def test_exception_exit_and_old_fact_reentry(env):
    e=env
    with held(e) as fact:
        with pytest.raises(RuntimeError):
            with owner.use_locked_admission(e.rt,e.ad,fact): raise RuntimeError('business')
        assert owner._anchor_context.get() is None
        current(e)
        with owner.use_locked_admission(e.rt,e.ad,fact):
            with pytest.raises(ValueError,match='reentry'):
                with owner.use_locked_admission(e.rt,e.ad,fact): pass
    with pytest.raises(ValueError,match='inactive'):
        with owner.use_locked_admission(e.rt,e.ad,fact): pass


def test_combination_activates_after_initial_current(env,monkeypatch):
    e=env
    graph=object.__new__(combined_access.Admissions)
    graph.guard=lambda:None;graph._locks=[('a',e.target)]
    graph._locking=False;graph._read_scope=None
    graph._by_id={'a':e.ad};graph._runtimes={'a':e.rt};graph._order=['a']
    monkeypatch.setattr(combined_access,'api',lambda name:owner)
    with graph.locked():
        assert e.counts['anchor_select']==2
        before=e.counts['anchor_select']
        graph.current();graph.current()
        assert e.counts['anchor_select']==before
    assert owner._anchor_context.get() is None and not owner._anchor_facts
    assert graph._read_scope is None and graph._locking is False


def test_hold_exit_revokes_even_if_context_still_set(env):
    e=env
    context=None
    try:
        with held(e) as fact:
            context=owner.use_locked_admission(e.rt,e.ad,fact)
            context.__enter__()
            current(e)
        before=e.counts['anchor_select']
        with pytest.raises(ValueError,match='inactive'): current(e)
        assert e.counts['anchor_select']==before
    finally:
        if context is not None: context.__exit__(None,None,None)
    assert owner._anchor_context.get() is None
