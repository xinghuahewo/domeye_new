"""有限公共组件接合：只调用owner公开资格、单锁和有界读取，不生产或补签。"""
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from dataclasses import dataclass
from data_pipeline.results.manifest_io import require
from data_pipeline.bgp.archive.value_codec import digest

# 已接受公共入口；未知owner失败关闭。
def api(owner):
    if owner in ('m2', 'reference'):
        from data_pipeline.bgp.archive import admission as publication
    elif owner == 'canonical':
        from data_pipeline.bgp.replay import snapshot_admission as publication
    elif owner == 'feature':
        from data_pipeline.analysis.features import publication as publication
    elif owner == 'resource':
        from data_pipeline.analysis.resources import publication
    elif owner == 'detection':
        from data_pipeline.analysis.detection import publication
    elif owner == 'country':
        from data_pipeline.analysis.country_events import result_admission as publication
    elif owner == 'trend':
        from data_pipeline.analysis.country_trends import result_admission as publication
    else:
        raise ValueError('组合尚无该owner正式公共接缝：' + str(owner))
    return publication


LOCK_OWNERS = {
    'trend.component': ('trend',60), 'trend.admission': ('trend',60), 'trend.reference': ('trend',60),
    'country.component': ('country',40), 'country.read_model': ('country',50),
    'country.admission': ('country',50),
    'm2.run': ('m2', 10), 'm2.checkpoint': ('m2', 10),
    'm2.admission': ('m2', 10), 'reference.admission': ('reference', 10),
    'canonical.run': ('canonical', 20), 'canonical.admission': ('canonical', 20),
    'feature.run': ('feature', 30), 'feature.qualification': ('feature', 30),
    'feature.admission': ('feature', 30), 'resource.run': ('resource', 30),
    'resource.reference': ('resource', 10), 'resource.admission': ('resource', 30),
    'detection.run': ('detection', 30), 'detection.admission': ('detection', 30),
}


def lock_key(target):
    require(set(target) == {'stage', 'system_identifier', 'database_oid', 'namespace', 'key'}, '组合锁字段错误')
    require(type(target['stage']) is int and type(target['database_oid']) is int, '组合锁数值类型错误')
    require(all(type(target[x]) is str for x in ('system_identifier', 'namespace', 'key')), '组合锁身份类型错误')
    return (target['stage'], target['system_identifier'], target['database_oid'], target['namespace'], target['key'].encode())


class Admissions:
    """仅一个明确闭合的实际Admission图；不构成可发布profile或published授权。"""
    def __init__(self, admissions, runtimes, *, guard):
        self.guard = guard
        self._locking = False
        self._read_scope = None
        self._values = deepcopy(list(admissions))
        self._by_id = {a['admission_id']: a for a in self._values}
        require(len(self._by_id) == len(self._values) and bool(self._values), '组合Admission重复或为空')
        require(set(runtimes) == set(self._by_id), '组合Runtime必须逐实际Admission显式绑定')
        self._runtimes = dict(runtimes)
        for a in self._values:
            api(a['owner'])
            require(a['admission_id'] == digest({k: v for k, v in a.items() if k != 'admission_id'}), '组合Admission摘要错误')
            require(a['dependencies'] == sorted(set(a['dependencies'])), '组合依赖必须唯一有序')
            require(set(a['dependencies']) <= set(self._by_id), '组合缺少实际依赖Admission')
            # owner Runtime里使用的对象不能与组合图的同名ID形成两份真相。
            for dep in getattr(self._runtimes[a['admission_id']], 'dependency_admissions', ()):
                require(dep['admission_id'] in self._by_id and dep == self._by_id[dep['admission_id']], 'Runtime依赖不属于固定组合图')
        self._order = []
        visiting = set()
        def visit(key):
            require(key not in visiting, '组合依赖环')
            if key in self._order: return
            visiting.add(key)
            for dep in self._by_id[key]['dependencies']: visit(dep)
            visiting.remove(key); self._order.append(key)
        for key in sorted(self._by_id): visit(key)
        self._locks = self._lock_plan()

    def _lock_plan(self):
        targets = {}
        for a in self._values:
            for t in a['lock_targets']:
                key = lock_key(t)
                require(t['namespace'] in LOCK_OWNERS, '组合未知锁namespace')
                owner, stage = LOCK_OWNERS[t['namespace']]
                require(t['stage'] == stage, '组合锁stage与owner不符')
                candidates = [x for x in self._values if x['owner'] == owner and t in x['lock_targets']
                              and x['physical']['system_identifier'] == t['system_identifier']
                              and x['physical']['database_oid'] == t['database_oid']]
                require(bool(candidates), '继承锁没有实际原owner Admission')
                # 同一来源可在多个子选择出现；每个owner.verify_current另核自己的完整绑定。
                chosen = min(candidates, key=lambda x: x['admission_id'])
                targets[key] = (chosen['admission_id'], deepcopy(t))
        return [targets[k] for k in sorted(targets)]

    @property
    def lock_targets(self): return [deepcopy(t) for _, t in self._locks]

    def envelope(self, admission_id, *, batch_rows, max_row_bytes):
        from data_pipeline.results.stream_policy import envelope
        require(admission_id in self._by_id, '请求不属于固定组合图')
        return envelope(self._by_id[admission_id]['owner'],self._runtimes[admission_id],
                        batch_rows=batch_rows,max_row_bytes=max_row_bytes)

    def current(self):
        for key in self._order:
            self.guard()
            a = self._by_id[key]
            api(a['owner']).verify_current(self._runtimes[key], deepcopy(a), guard=self.guard)

    @contextmanager
    def locked(self):
        """全部单目标锁到齐后统一current；退出只释放，不在control提交后验业务。"""
        from data_pipeline.common.admission_locks import LockConnections
        require(not self._locking, '组合锁会话不得嵌套或重入')
        self._locking = True
        try:
            with LockConnections() as connections, ExitStack() as stack:
                trend_facts = []
                for key, target in self._locks:
                    self.guard(); a = self._by_id[key]
                    lease=connections.borrow(self._runtimes[key],target,guard=self.guard)
                    fact = stack.enter_context(api(a['owner']).hold_lock(self._runtimes[key], deepcopy(a), deepcopy(target), guard=self.guard,lock_connection=lease))
                    if a['owner'] == 'trend' and target['namespace'] == 'trend.admission':
                        trend_facts.append((key, fact))
                self.current()
                # Trend事实与P读取scope均在原锁内；P scope先撤销。
                with ExitStack() as verified:
                    for key, fact in trend_facts:
                        verified.enter_context(api('trend').use_locked_admission(
                            self._runtimes[key], deepcopy(self._by_id[key]), fact))
                    self._read_scope = object()
                    try: yield
                    finally: self._read_scope = None
        finally:
            self._read_scope = None
            self._locking = False

    @contextmanager
    def read(self, admission_id, request, *, max_rows, max_bytes):
        """组件级逐批全流；累计行/编码字节仅计量，无P授权含义。

        max_rows/max_bytes 保留正整数调用兼容，不再限制流的累计量；
        原请求批封装、owner校验、资源guard及完整回执仍生效。
        """
        require(admission_id in self._by_id, '读取不属于固定组合图')
        require(type(max_rows) is int and max_rows > 0 and type(max_bytes) is int and max_bytes > 0, '组合读取兼容参数非法')
        a = self._by_id[admission_id]; req = deepcopy(request)
        scope = self._read_scope
        if scope is None: self.current()
        result = ReadSession()
        def check_scope():
            if scope is not None:
                require(self._read_scope is scope, '组合读取锁会话已失效')
        try:
            with api(a['owner']).open_reader(self._runtimes[admission_id], deepcopy(a), req, guard=self.guard) as source:
                def batches():
                    iterator = iter(source)
                    while True:
                        check_scope()
                        try: batch = next(iterator)
                        except StopIteration: break
                        self.guard()
                        require(set(batch) == {'rows_typed', 'codec_version', 'rows', 'bytes'}, 'owner批字段错误')
                        require(type(batch['rows']) is int and batch['rows'] >= 0 and type(batch['bytes']) is int, 'owner批计量类型错误')
                        require(batch['codec_version'] == req['codec_version'] and batch['bytes'] == len(batch['rows_typed'].encode()), 'owner批codec/字节错误')
                        require(batch['rows']<=req['batch_rows'] and 0<=batch['bytes']<=req['batch_bytes'], 'owner批超过原声明请求')
                        result.rows += batch['rows']; result.bytes += batch['bytes']
                        yield batch
                    result.exhausted = True
                result.iterator = batches()
                try: yield result
                finally: result.iterator.close()
            if result.exhausted:
                receipt = source.receipt
                require(receipt is not None and receipt['contract'] == 'component-publication-read/v1'
                        and receipt['admission_id'] == admission_id and receipt['request_digest'] == digest(req)
                        and receipt['rows'] == result.rows and receipt['execution'] == 'complete', 'owner完整读取回执不符')
                check_scope()
                if scope is None: self.current()
                result.receipt = deepcopy(receipt)
        except BaseException:
            self._read_scope = None
            raise
        finally:
            if not result.exhausted: self._read_scope = None



@dataclass
class ReadSession:
    receipt: object = None
    rows: int = 0
    bytes: int = 0
    exhausted: bool = False
    iterator: object = None

    def __iter__(self): return self
    def __next__(self): return next(self.iterator)
