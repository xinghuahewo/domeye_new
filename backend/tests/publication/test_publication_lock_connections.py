"""锁连接生命周期的有限替身；不模拟owner current、AD准入或ready。"""
from types import SimpleNamespace
import pytest
from psycopg2.extensions import TRANSACTION_STATUS_INTRANS, TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_INERROR
from data_pipeline.common import admission_locks as locks


class Connection:
    def __init__(self,oid=1):
        self.closed=False;self.autocommit=False;self.status=TRANSACTION_STATUS_INTRANS
        self.snapshot=('instance',oid,99,'user','user','none','public','off','read committed')
        self.rollbacks=0;self.closes=0;self.fail_cleanup=False
    def set_session(self,**kwargs):self.autocommit=kwargs['autocommit']
    def get_transaction_status(self):return self.status
    def get_dsn_parameters(self):return {'dbname':str(self.snapshot[1]),'user':'user'}
    def cursor(self):return self
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def execute(self,*args):pass
    def fetchone(self):return self.snapshot
    def rollback(self):
        self.rollbacks+=1
        if self.fail_cleanup:raise RuntimeError('rollback failed')
    def close(self):
        self.closes+=1;self.closed=True
        if self.fail_cleanup:raise RuntimeError('close failed')


def target(oid=1,key='one'):
    return dict(system_identifier='instance',database_oid=oid,stage=10,namespace='m2.checkpoint',key=key)


def admission(t):return dict(physical={k:t[k] for k in ('system_identifier','database_oid')},lock_targets=[t])


@pytest.fixture
def factory(monkeypatch):
    made=[]
    def connect(dsn):
        c=Connection(int(locks.parse_dsn(dsn)['dbname']));made.append(c);return c
    monkeypatch.setattr(locks.psycopg2,'connect',connect)
    return made


def test_many_targets_keep_one_connection_per_database(factory):
    rt=SimpleNamespace(dsn='dbname=1 user=user')
    with locks.LockConnections() as scope:
        for i in range(601):
            t=target(key=str(i));lease=scope.borrow(rt,t,guard=lambda:None)
            pg=locks.validated_connection(rt,admission(t),t,lease)
            assert pg.rollbacks==pg.closes==0
        scope.borrow(SimpleNamespace(dsn='dbname=2 user=user'),target(2),guard=lambda:None)
        assert len(factory)==2
    assert all(c.rollbacks==c.closes==1 for c in factory)
    with pytest.raises(ValueError,match='有效scope'):locks.validated_connection(rt,admission(t),t,lease)


@pytest.mark.parametrize('change',['config','closed','autocommit','idle','failed','transaction','role','wrong_scope','foreign_scope','parameters','environment','target'])
def test_reject_invalid_borrow(factory,change,monkeypatch):
    rt=SimpleNamespace(dsn='dbname=1 user=user');t=target()
    with locks.LockConnections() as scope:
        lease=scope.borrow(rt,t,guard=lambda:None);pg=lease.connection
        if change=='config':rt.dsn='dbname=1 user=other'
        if change=='closed':pg.closed=True
        if change=='autocommit':pg.autocommit=True
        if change=='idle':pg.status=TRANSACTION_STATUS_IDLE
        if change=='failed':pg.status=TRANSACTION_STATUS_INERROR
        if change=='transaction':pg.snapshot=(*pg.snapshot[:2],100,*pg.snapshot[3:])
        if change=='role':pg.snapshot=(*pg.snapshot[:5],'changed',*pg.snapshot[6:])
        if change=='wrong_scope':lease=pg
        if change=='foreign_scope':
            from dataclasses import replace
            lease=replace(lease,scope=locks.LockConnections())
        if change=='parameters':pg.get_dsn_parameters=lambda:{'dbname':'changed'}
        if change=='environment':monkeypatch.setenv('PGAPPNAME','changed')
        if change=='target':t=target(2)
        with pytest.raises(ValueError):locks.validated_connection(rt,admission(t),t,lease)


def test_wrong_physical_closes_new_connection(factory):
    with locks.LockConnections() as scope:
        with pytest.raises(ValueError,match='物理库'):
            scope.borrow(SimpleNamespace(dsn='dbname=1 user=user'),target(2),guard=lambda:None)
    assert factory[0].rollbacks==factory[0].closes==1


def test_same_database_config_mismatch_does_not_open_another(factory):
    with locks.LockConnections() as scope:
        scope.borrow(SimpleNamespace(dsn='dbname=1 user=user'),target(),guard=lambda:None)
        with pytest.raises(ValueError,match='配置'):
            scope.borrow(SimpleNamespace(dsn='dbname=1 user=other'),target(),guard=lambda:None)
        assert len(factory)==1


def test_cleanup_preserves_primary_and_every_cleanup_error(factory):
    primary=RuntimeError('primary')
    with pytest.raises(RuntimeError) as caught:
        with locks.LockConnections() as scope:
            lease=scope.borrow(SimpleNamespace(dsn='dbname=1 user=user'),target(),guard=lambda:None)
            lease.connection.fail_cleanup=True
            raise primary
    assert caught.value is primary
    assert [str(e) for e in primary.cleanup_errors]==['rollback failed','close failed']
    assert factory[0].closes==1


def test_cleanup_without_primary_preserves_secondary(factory):
    with pytest.raises(RuntimeError,match='rollback failed') as caught:
        with locks.LockConnections() as scope:
            scope.borrow(SimpleNamespace(dsn='dbname=1 user=user'),target(),guard=lambda:None).connection.fail_cleanup=True
    assert str(caught.value.cleanup_errors[0])=='close failed'
