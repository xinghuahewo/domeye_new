"""组合发布专用锁事务的窄借用入口；不登记资格、不决定锁目标或排序。"""
from dataclasses import dataclass, field
import os
import psycopg2
from psycopg2.extensions import parse_dsn, TRANSACTION_STATUS_INTRANS


def require(ok, reason):
    if not ok: raise ValueError(reason)


def _configuration(runtime):
    # 只作内部严格相等比较，不输出连接参数或凭据。
    return (parse_dsn(runtime.dsn),{k:v for k,v in os.environ.items() if k.startswith('PG')})


def _snapshot(pg):
    with pg.cursor() as cur:
        cur.execute("SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()),"
                    "txid_current(),current_user,session_user,current_setting('role'),current_setting('search_path'),"
                    "current_setting('transaction_read_only'),current_setting('transaction_isolation') FROM pg_control_system()")
        return cur.fetchone()


@dataclass(frozen=True,repr=False)
class _Lease:
    scope: object
    connection: object
    physical: tuple
    configuration: object = field(repr=False)
    parameters: object = field(repr=False)
    snapshot: tuple = field(repr=False)


class LockConnections:
    """由组合最外层持有；每物理库一事务，最后才统一rollback/close。"""
    def __init__(self):
        self._active=False;self._leases={}

    def __enter__(self):
        require(not self._active and not self._leases, '锁连接scope不可重复进入')
        self._active=True
        return self

    def borrow(self,runtime,target,*,guard):
        require(self._active, '锁连接scope未生效')
        guard()
        physical=(target['system_identifier'],target['database_oid'])
        configuration=_configuration(runtime)
        if physical in self._leases:
            lease=self._leases[physical]
            require(lease.configuration==configuration, '同库锁连接配置不一致')
            _validate(runtime,lease)
            return lease
        pg=psycopg2.connect(runtime.dsn)
        try:
            pg.set_session(readonly=False,autocommit=False)
            snapshot=_snapshot(pg)
            require(snapshot[:2]==physical, '锁连接实际物理库不符')
            require(snapshot[-2:]==('off','read committed'), '锁连接事务会话约束不符')
            lease=_Lease(self,pg,physical,configuration,pg.get_dsn_parameters(),snapshot)
            self._leases[physical]=lease
            _validate(runtime,lease)
            return lease
        except BaseException as primary:
            # 未交给scope之前也必须保留首异常并回收实际连接。
            self._leases.pop(physical,None)
            _release(pg,primary)
            raise

    def __exit__(self,kind,primary,tb):
        errors=[]
        self._active=False
        for lease in reversed(tuple(self._leases.values())):
            for action in (lease.connection.rollback,lease.connection.close):
                try:action()
                except BaseException as error:errors.append(error)
        self._leases.clear()
        if errors:
            if primary is not None:primary.cleanup_errors=(*getattr(primary,'cleanup_errors',()),*errors)
            else:
                errors[0].cleanup_errors=(*getattr(errors[0],'cleanup_errors',()),*errors[1:])
                raise errors[0]
        return False


def _release(pg,primary):
    errors=[]
    for action in (pg.rollback,pg.close):
        try:action()
        except BaseException as error:errors.append(error)
    if errors:primary.cleanup_errors=(*getattr(primary,'cleanup_errors',()),*errors)


def _state(lease):
    pg=lease.connection
    require(not pg.closed and not pg.autocommit, '借用锁连接关闭或autocommit')
    require(pg.get_transaction_status()==TRANSACTION_STATUS_INTRANS, '借用锁事务状态改变')
    require(pg.get_dsn_parameters()==lease.parameters, '借用锁连接有效参数改变')
    require(_snapshot(pg)==lease.snapshot, '借用锁事务或会话身份改变')


def _validate(runtime,lease):
    require(type(lease) is _Lease and type(lease.scope) is LockConnections, '非本次scope借用锁连接')
    scope=lease.scope
    require(scope._active and scope._leases.get(lease.physical) is lease, '借用锁连接不归有效scope')
    require(_configuration(runtime)==lease.configuration, '借用锁连接配置不一致')
    _state(lease)


def validated_connection(runtime,admission,lock_target,lease):
    """仅返回活scope原事务；owner仍执行自身全部校验/timeout/单目标SQL。

    调用者不得提交、回滚或关闭借用连接；scope统一清理。默认独立hold_lock
    不经过此入口，其原连接生命周期保持不变。
    """
    _validate(runtime,lease)
    physical=(lock_target['system_identifier'],lock_target['database_oid'])
    require(physical==lease.physical==(admission['physical']['system_identifier'],admission['physical']['database_oid'])
            and lock_target in admission['lock_targets'], '借用锁连接目标或owner物理库不符')
    return lease.connection


def code_sha():
    from pathlib import Path
    import hashlib
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
