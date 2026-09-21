"""国家 M3 四类实际公开上游的固定绑定和有限读取，不读取 Detection 私有状态。"""
from contextlib import ExitStack, contextmanager
from copy import deepcopy
import json
import hashlib

from data_pipeline.bgp.archive import admission as m2
from data_pipeline.bgp.replay import snapshot_admission as canonical
from data_pipeline.bgp.replay.snapshot_contract import TABLES as CANONICAL_TABLES, encode, decode
from data_pipeline.bgp.replay.snapshot_validation import CODEC as CANONICAL_CODEC
from data_pipeline.analysis.detection import publication as detection
from data_pipeline.analysis.detection.publication_codec import typed as dtyped, untyped as duntyped, CODEC as DCODEC
from data_pipeline.bgp.archive.checkpoint import sha as m2_row_digest


def update_read_digest(hasher, owner, row):
    """沿用各 owner 的完整读摘要算法，不能用国家自己的编码替代。"""
    if owner in ('m2', 'reference'):
        hasher.update(bytes.fromhex(m2_row_digest(row)))
    elif owner == 'canonical':
        hasher.update(encode(row).encode() + b'\n')
    elif owner == 'detection':
        data = dtyped(row).encode()
        hasher.update(len(data).to_bytes(8, 'big'))
        hasher.update(data)
    else:
        raise ValueError('M3 原读摘要 owner 无效')


def check_saved_read(owner, admission_id, view, rows, saved, *, guard):
    """核验已取得的公开原行和回执的一致性；不签发 Admission 或替代当前核验。

    回执须由调用方保留实际 open_reader 的来源证据。此函数只检查本地保存
    是否截断、调序、改值或串绑，不把任意用户构造的回执认证成上游证明。
    """
    request, receipt = saved['request'], saved['receipt']
    if (saved['owner'] != owner or saved['view'] != view or request['view'] != view
            or receipt['admission_id'] != admission_id
            or receipt['request_digest'] != m2.digest(request)
            or receipt['execution'] != 'complete'):
        raise ValueError('M3 保存原读回执串绑或未完成')
    hasher = hashlib.sha256()
    count = 0
    for row in rows:
        guard()
        update_read_digest(hasher, owner, row)
        count += 1
    if receipt['rows'] != count or receipt['typed_digest'] != hasher.hexdigest():
        raise ValueError('M3 保存原行与公开完整摘要不符')


class M3Inputs:
    """Admission 只能来自各 owner 的实际 admit；此层仅核验和消费。

    admissions/runtimes 按真实 admission_id 绑定；M2、Canonical、Detection 各一个，
    reference 是该 M2 的全部实际参考。全部读累计计量、批内限额，并保持固定锁到消费结束。
    max_rows/max_bytes保留旧配置兼容，不再充当流式总量停止条件。
    """
    def __init__(self, admissions, runtimes, *, max_rows, max_bytes, batch_rows,
                 batch_bytes, guard):
        if any(type(v) is not int or v <= 0 for v in (max_rows, max_bytes, batch_rows, batch_bytes)) or batch_rows > 10000:
            raise ValueError('M3 上游读取预算无效')
        self.admissions = {a['admission_id']: deepcopy(a) for a in admissions}
        if len(self.admissions) != len(admissions) or set(runtimes) != set(self.admissions):
            raise ValueError('M3 原 Admission/Runtime 必须唯一且逐项对应')
        self.runtimes, self.guard = dict(runtimes), guard
        self.max_rows, self.max_bytes = max_rows, max_bytes
        self.batch_rows, self.batch_bytes = batch_rows, batch_bytes
        self.rows = self.bytes = 0
        self.receipts = []
        self.open_reads = 0
        self.active = False
        self.owners = {}
        for aid, admission in self.admissions.items():
            owner = admission['owner']
            if owner not in ('m2', 'reference', 'canonical', 'detection'):
                raise ValueError('M3 上游 owner 超出四类固定范围')
            self.owners.setdefault(owner, []).append(aid)
        if any(len(self.owners.get(owner, ())) != 1 for owner in ('m2', 'canonical', 'detection')):
            raise ValueError('M3 必须显式绑定一个 M2、Canonical 和 Detection')

    @staticmethod
    def module(owner):
        return {'m2': m2, 'reference': m2, 'canonical': canonical, 'detection': detection}[owner]

    def verify(self):
        self.check_budget()
        for aid, a in self.admissions.items():
            self.guard()
            self.module(a['owner']).verify_current(self.runtimes[aid], a, guard=self.guard)
        mid = self.owners['m2'][0]
        mb = m2.untyped(self.admissions[mid]['owner_binding'])
        refs = self.owners.get('reference', [])
        expected = {mid, *refs}
        for owner in ('canonical', 'detection'):
            a = self.admissions[self.owners[owner][0]]
            if set(a['dependencies']) != expected:
                raise ValueError('M3 Canonical/Detection 未绑定同一完整 M2 与参考 Admission')
        actual_refs = []
        for rid in refs:
            raw = m2.untyped(self.admissions[rid]['owner_binding'])
            if raw['m2_admission_id'] != mid or raw['m2_binding'] != mb:
                raise ValueError('M3 参考与实际 M2 绑定不符')
            actual_refs.append(raw['source_id'])
        if len(set(actual_refs)) != len(actual_refs) or set(actual_refs) != {r['source_id'] for r in mb['reference_sources']}:
            raise ValueError('M3 完整参考集合缺项或重复')
        cb = decode(self.admissions[self.owners['canonical'][0]]['owner_binding'])
        db = duntyped(self.admissions[self.owners['detection'][0]]['owner_binding'])
        original = json.loads(mb['input_binding'])
        if (cb['descriptor']['plan']['input_binding'] != original
                or db['identity']['input_binding'] != original
                or cb['descriptor']['plan']['selected_sources'] != mb['ordered_source_ids']
                or db['identity']['selected_sources'] != mb['ordered_source_ids']):
            raise ValueError('M3 四类上游原始来源序/绑定不一致')
        self.m2_binding, self.canonical_binding, self.detection_binding = mb, cb, db

    def check_budget(self):
        self.guard()
        if (any(type(v) is not int or v <= 0 for v in
                (self.max_rows, self.max_bytes, self.batch_rows, self.batch_bytes))
                or self.batch_rows > 10000):
            raise ValueError('resource_limit:M3_shared_input')

    @contextmanager
    def locked(self):
        if self.active:
            raise ValueError('M3 输入不能嵌套使用')
        self.verify()
        targets = {}
        for aid, a in self.admissions.items():
            for t in a['lock_targets']:
                namespace = t['namespace'].split('.')[0]
                if namespace != a['owner'] and not (a['owner'] == 'm2' and namespace == 'm2'):
                    continue
                key = (t['stage'], t['system_identifier'], t['database_oid'], t['namespace'], t['key'])
                targets.setdefault(key, (aid, t))
        from data_pipeline.common.admission_locks import LockConnections
        with LockConnections() as connections, ExitStack() as stack:
            for key in sorted(targets):
                aid, target = targets[key]
                a = self.admissions[aid]
                lease=connections.borrow(self.runtimes[aid],target,guard=self.guard)
                stack.enter_context(self.module(a['owner']).hold_lock(
                    self.runtimes[aid], a, target, guard=self.guard,lock_connection=lease))
            self.verify()
            self.active = True
            try:
                yield self
                if self.open_reads:
                    raise ValueError('M3 上游读取尚未耗尽关闭')
                self.verify()
            finally:
                self.active = False

    def read(self, aid, view):
        """完整原表或已接受的 D 结果视图；旧 D 不补造窗口证明。"""
        if not self.active or aid not in self.admissions:
            raise ValueError('M3 读取必须持有固定上游锁')
        a = self.admissions[aid]
        owner = a['owner']
        if owner == 'canonical':
            if view not in CANONICAL_TABLES:
                raise ValueError('M3 Canonical 必须使用原 13 表视图')
            codec, pack, unpack = CANONICAL_CODEC, encode, decode
            scope = dict(source_ids=None)
        elif owner == 'detection':
            if view not in ('records', 'm3_entries', 'result_revisions', 'result_coverage'):
                raise ValueError('M3 仅读取 Detection 原修订/决策与独立资格覆盖')
            if view.startswith('result_') and self.detection_binding['identity'].get('result_window_rule') != 'detection-result-window/v1':
                raise ValueError('M3 D缺少实际结果窗生产证明，旧D不能补签')
            codec, pack, unpack = DCODEC, dtyped, duntyped
            scope = dict(start=0, stop=None, key=None, at_position=None)
        else:
            raw = m2.untyped(a['owner_binding'])
            codec, pack, unpack = m2.CODEC, m2.typed, m2.untyped
            sources = [raw['source_id']] if owner == 'reference' else raw['ordered_source_ids']
            if owner == 'reference' and view != 'references':
                raise ValueError('M3 参考只能读取实际参考原行')
            if owner == 'm2' and view == 'references':
                raise ValueError('M3 参考必须通过明确参考 Admission 读取')
            scope = dict(source_ids=sources)
        request = dict(view=view, scope_typed=pack(scope), codec_version=codec,
                       batch_rows=self.batch_rows, batch_bytes=self.batch_bytes)
        count = 0
        hasher = hashlib.sha256()
        with self.read_session(owner, aid, a, request) as session:
            for batch in session:
                self.guard()
                if not self.active or batch['codec_version'] != codec:
                    raise ValueError('M3 读取锁/原 codec 不符')
                if len(batch['rows_typed'].encode())>self.batch_bytes:
                    raise ValueError('resource_limit:M3_input_batch')
                rows = unpack(batch['rows_typed'])
                if len(rows) != batch['rows'] or len(batch['rows_typed'].encode()) != batch['bytes']:
                    raise ValueError('M3 公开批计数不符')
                if len(rows)>self.batch_rows:
                    raise ValueError('resource_limit:M3_input_batch')
                for row in rows:
                    self.check_budget()
                    if not self.active:
                        raise ValueError('M3 上游锁已释放')
                    size = len(pack(row).encode())
                    self.rows += 1
                    self.bytes += size
                    count += 1
                    update_read_digest(hasher, owner, row)
                    yield row
        receipt = session.receipt
        if (not receipt or receipt['execution'] != 'complete' or receipt['rows'] != count
                or receipt['admission_id'] != aid or receipt['request_digest'] != m2.digest(request)
                or receipt['typed_digest'] != hasher.hexdigest()):
            raise ValueError('M3 上游完整公开读回执缺失或不符')
        self.receipts.append(dict(owner=owner, view=view, request=request, receipt=deepcopy(receipt)))

    @contextmanager
    def read_session(self, owner, aid, admission, request):
        self.check_budget()
        self.open_reads += 1
        try:
            with self.module(owner).open_reader(self.runtimes[aid], admission, request, guard=self.guard) as session:
                yield session
        finally:
            self.open_reads -= 1
