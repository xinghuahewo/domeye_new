"""canonical P0：原完整绑定的离线准入、轻量 current、单目标锁和有界原值流。"""
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
import hashlib
import json
import os
import platform
import subprocess
import time
import uuid

from psycopg2.extras import Json

from data_pipeline.bgp.archive import admission as upstream
from data_pipeline.bgp.replay import snapshot_access as io
from data_pipeline.bgp.replay.snapshot_access import Runtime
from data_pipeline.bgp.replay.snapshot_contract import TABLES, COLUMNS, PROFILE, ProjectionBinding, encode, digest as typed_digest
from data_pipeline.bgp.replay.snapshot_validation import CODEC, RULES, INPUT_RULES, validate_row
from data_pipeline.bgp.replay.snapshot_store import ProjectionReader, relation
from data_pipeline.bgp.archive.value_codec import CONTRACT, fields, digest, canonical, ADMISSION_FIELDS, PHYSICAL_FIELDS, VALIDATOR_FIELDS, ENTITY_FIELDS, LOCK_FIELDS, REQUEST_FIELDS

ROOT = Path(__file__).resolve().parents[4]
REGISTRY = 'canonical_publication.admissions'
SOURCE_VIEWS = ('changes','invalidations','scope_gap','qualification_change','source_coverage','source_quality','reference_binding')
# 验收代码与历史 plan.code 分开；修改这些实现需要新验收，不重写旧科学身份。
CODE_FILES = ('common/admission_locks.py','bgp/replay/snapshot_admission.py','bgp/replay/snapshot_access.py','bgp/replay/snapshot_contract.py',
              'bgp/replay/snapshot_store.py','bgp/replay/snapshot_validation.py','bgp/replay/calculation_window.py','bgp/replay/quality_overlay.py','bgp/replay/archive_input.py',
              'bgp/ordered_reader.py','bgp/record_types.py','bgp/replay/route_replay.py','bgp/archive/message_reader.py','bgp/archive/selection.py','bgp/archive/store.py',
              'bgp/archive/checkpoint.py','bgp/input/mrt_reader.py','bgp/input/mrt_types.py','bgp/archive/admission.py','bgp/archive/value_codec.py','bgp/archive/validation.py')


def _rules(runtime=None):
    code = {name:hashlib.sha256((Path(__file__).parents[2]/name).read_bytes()).hexdigest() for name in CODE_FILES}
    code['environment'] = dict(python=platform.python_version(), packages={n:version(n) for n in ('duckdb','pyarrow','psycopg2-binary')})
    from data_pipeline.common.admission_locks import code_sha as lock_code_sha
    code['lock_connection_code']=lock_code_sha()
    return dict(version='canonical-publication/v1', code_sha256=digest(code), typed_schema=PROFILE,
                typed_codec=CODEC, rules_digest=digest(dict(execution_profile=runtime._profile if runtime else 'synthetic-fixture/v1',
                    fixed_scope_digest=runtime._scope_digest if runtime else None,science=RULES,input=INPUT_RULES,tables=TABLES,
                    columns=COLUMNS,audit='ProjectionReader.audit/13-tables-one-Replay',custody='immutable-stat/v1',
                    ducklake='0.3',extensions={'ducklake':'3f1b372','postgres_scanner':'b9fce43'})))


def _revision():
    revision = subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip()
    for name in CODE_FILES:
        relative = 'backend/data_pipeline/' + name
        try:
            committed = subprocess.check_output(['git','show',revision+':'+relative], cwd=ROOT, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as exc:
            raise ValueError('准入必须使用包含实际验证实现的固定提交') from exc
        if committed != (ROOT/relative).read_bytes():
            raise ValueError('准入必须使用包含实际验证实现的固定提交')
    return revision


def _target_key(t):
    return tuple(t[k] for k in ('stage','system_identifier','database_oid','namespace','key'))


def _own(a):
    found = [t for t in a['lock_targets'] if t['namespace'] == 'canonical.admission']
    if len(found) != 1: raise ValueError('canonical 可信登记 key 目标缺失或重复')
    key = io.untyped(found[0]['key'])
    if type(key) is not str or str(uuid.UUID(key)) != key: raise ValueError('可信记录 key 必须为独立 UUID')
    return found[0]


def _shape(a):
    fields(a, ADMISSION_FIELDS); fields(a['physical'], PHYSICAL_FIELDS); fields(a['validator'], VALIDATOR_FIELDS)
    if a['contract'] != CONTRACT or a['owner'] != 'canonical' or a['binding_codec'] != CODEC:
        raise ValueError('canonical Admission 合同不符')
    if a['admission_id'] != digest({k:v for k,v in a.items() if k != 'admission_id'}):
        raise ValueError('Admission 摘要错误')
    def sha(value, size=64):
        return type(value) is str and len(value) == size and all(c in '0123456789abcdef' for c in value)
    if not sha(a['owner_revision'],40) or not sha(a['inventory_digest']) or any(not sha(a['validator'][k]) for k in ('code_sha256','rules_digest','validation_digest')):
        raise ValueError('验收源码/摘要字段无效')
    p = a['physical']
    if (type(p['database_oid']) is not int or p['database_oid'] <= 0 or type(p['snapshot']) is not int or p['snapshot'] < 0
            or p['catalog'] != 'lake' or any(type(p[k]) is not str or not p[k] for k in ('system_identifier','schema','root'))):
        raise ValueError('实际物理字段无效')
    if type(a['dependencies']) is not list or a['dependencies'] != sorted(set(a['dependencies'])) or any(not sha(x) for x in a['dependencies']):
        raise ValueError('依赖集合必须为唯一排序 ID')
    for e in a['entities']:
        fields(e, ENTITY_FIELDS)
        if not sha(e['sha256']) or any(type(e[k]) is not int or e[k] < 0 for k in ('device','inode','size','mtime_ns','ctime_ns')):
            raise ValueError('实体字段无效')
    if [e['path'] for e in a['entities']] != sorted({e['path'] for e in a['entities']}):
        raise ValueError('实体必须唯一排序')
    for t in a['lock_targets']:
        fields(t, LOCK_FIELDS)
        if (type(t['stage']) is not int or t['stage'] not in (10,20) or type(t['key']) is not str
                or t['system_identifier'] != p['system_identifier'] or type(t['database_oid']) is not int or t['database_oid'] != p['database_oid']
                or t['namespace'] not in ('m2.run','m2.checkpoint','m2.admission','reference.admission','canonical.run','canonical.admission')
                or (t['stage'] == 20) != t['namespace'].startswith('canonical.')):
            raise ValueError('有限锁目标无效')
    if a['lock_targets'] != sorted(a['lock_targets'],key=_target_key) or len({_target_key(t) for t in a['lock_targets']}) != len(a['lock_targets']):
        raise ValueError('锁目标必须唯一全序')
    _own(a); io.binding(io.untyped(a['owner_binding']))


def inspect_binding(runtime, binding, *, guard):
    """便捷取得原完整 descriptor；只读，尚不是 Admission，不提升旧回执资格。"""
    runtime.check(); guard()
    if not isinstance(binding, ProjectionBinding): raise ValueError('必须显式 ProjectionBinding')
    from dataclasses import asdict
    if not runtime.fixture_only and asdict(binding) != runtime.expected_canonical_binding['binding']:
        raise ValueError('inspect 超出真实候选固定 ProjectionBinding')
    # 先从有限 PG 锚定位并检查允许根，再允许旧 descriptor 读取原控制回执。
    with io.pg(runtime) as pg, pg.cursor() as c:
        row = io.sql(runtime,c,'SELECT seal FROM m3_projection.runs WHERE run_id=%s',(binding.run_id,)).fetchone()
    if row is None: raise ValueError('canonical 原登记缺失')
    runtime.path(row[0]['control_root'])
    for f in row[0]['metrics']['parquet_files']: runtime.path(f['path'])
    reader = ProjectionReader(runtime.dsn,binding,guard=guard)
    result = dict(binding=asdict(binding),descriptor=reader.descriptor())
    io.metadata(runtime,result)
    return result


def _dependencies(runtime, b, guard):
    runtime.check_binding(b)
    deps = {a['admission_id']:a for a in runtime.dependency_admissions}
    if len(deps) != len(runtime.dependency_admissions) or set(runtime.dependency_runtimes) != set(deps):
        raise ValueError('必须逐项显式提供真实依赖 Admission 和 Runtime')
    raw = {}
    for key, a in deps.items():
        upstream.admission_shape(a)
        dep_runtime = runtime.dependency_runtimes[key]; io.dependency_check(dep_runtime,runtime)
        def checked():
            guard(); runtime.check(); io.dependency_check(dep_runtime,runtime)
        upstream.verify_current(dep_runtime,a,guard=checked)
        raw[key] = upstream.untyped(a['owner_binding'])
    m2 = [a for a in deps.values() if a['owner'] == 'm2']
    refs = [a for a in deps.values() if a['owner'] == 'reference']
    plan = b['descriptor']['plan']; database = b['descriptor']['database']
    if len(m2) != 1 or len(refs) != len(plan['references']) or len(deps) != 1 + len(refs):
        raise ValueError('canonical 必须精确绑定一个 M2 及全部实际参考选择')
    ma = m2[0]; mb = raw[ma['admission_id']]
    if (json.loads(mb['input_binding']) != plan['input_binding'] or mb['ordered_source_ids'] != plan['selected_sources']
            or mb['plan']['manifest'] != plan['input_manifest'] or ma['physical']['system_identifier'] != database['system_identifier']
            or ma['physical']['database_oid'] != database['database_oid']):
        raise ValueError('实际 M2 完整绑定、原 rank 或物理依赖不符')
    expected = {r['source_id']:r for r in plan['references']}
    if len(expected) != len(refs): raise ValueError('原参考绑定重复')
    for a in refs:
        r = raw[a['admission_id']]
        if r != upstream.reference_binding(runtime.dependency_runtimes[a['admission_id']], ma, r['source_id']):
            raise ValueError('参考必须为同一 M2 原事实完整选择')
        if r['source_id'] not in expected or r['selected_sources'] != []:
            raise ValueError('参考选择缺失或误用 MRT rank')
        cp = mb['seal']['checkpoints'][r['checkpoint_ordinal']]; want = expected[r['source_id']]
        if (cp['source_id'] != r['source_id'] or any(want[k] != cp[k] for k in ('source_sha','format'))
                or want['checkpoint_digest'] != cp['digest'] or want['rows'] != cp['counts']['references']):
            raise ValueError('原参考 CP、计数或解释身份不符')
    return deps


def _targets(physical, b, deps, key):
    targets = {_target_key(t):t for a in deps.values() for t in a['lock_targets']}
    for ns, value in [('canonical.run',b['binding']['run_id']),('canonical.admission',key)]:
        t = dict(stage=20,system_identifier=physical['system_identifier'],database_oid=physical['database_oid'],namespace=ns,key=encode(value))
        targets[_target_key(t)] = t
    return [targets[k] for k in sorted(targets)]


def _anchor(runtime, a):
    with io.pg(runtime) as pg, pg.cursor() as c:
        row = io.sql(runtime,c,f'SELECT state,admission,audit FROM {REGISTRY} WHERE record_key=%s',(io.untyped(_own(a)['key']),)).fetchone()
    if row is None or row[:2] != ('accepted',a): raise ValueError('可信准入缺失、撤销或整份绑定不符')
    audit = row[2]
    if digest(audit['inventory']) != a['inventory_digest'] or digest(audit['validation']) != a['validator']['validation_digest']:
        raise ValueError('可信验证正文不符')
    return audit


def verify_current(runtime, admission, *, guard):
    guard(); runtime.check(); _shape(admission)
    if {k:v for k,v in admission['validator'].items() if k != 'validation_digest'} != _rules(runtime):
        raise ValueError('当前验证规则或实现变化，必须新准入')
    b = io.untyped(admission['owner_binding']); audit = _anchor(runtime,admission)
    physical, metadata = io.metadata(runtime,b)
    if physical != admission['physical'] or digest(metadata) != audit['validation']['metadata_digest']:
        raise ValueError('实际 PG、目录、固定快照漂移')
    deps = _dependencies(runtime,b,guard)
    if sorted(deps) != admission['dependencies'] or _targets(physical,b,deps,io.untyped(_own(admission)['key'])) != admission['lock_targets']:
        raise ValueError('依赖或有限锁目标缺失/漂移')
    if io.entities(runtime,b['descriptor'],guard) != admission['entities']:
        raise ValueError('完整输出及控制实体漂移')
    guard(); runtime.check(); runtime.event('canonical_current', entities=len(admission['entities']))


def _locked(runtime, c, a, b, t):
    key = io.untyped(t['key'])
    if t['namespace'] == 'canonical.run':
        row = io.sql(runtime,c,'SELECT state,profile,snapshot,seal,control_sha,plan,schema_name FROM m3_projection.runs WHERE run_id=%s FOR SHARE',(key,)).fetchone()
        d = b['descriptor']
        if (key != b['binding']['run_id'] or row is None or row[:3] != ('complete',PROFILE,b['binding']['snapshot'])
                or row[3]['digest'] != b['binding']['seal_digest'] or row[4:] != (d['manifest_sha'],d['plan'],d['schema'])
                or row[3]['digest'] != typed_digest({k:v for k,v in row[3].items() if k != 'digest'})):
            raise ValueError('锁后原 canonical 登记不符')
    elif t['namespace'] == 'canonical.admission':
        row = io.sql(runtime,c,f'SELECT state,admission FROM {REGISTRY} WHERE record_key=%s FOR SHARE',(key,)).fetchone()
        if row != ('accepted',a): raise ValueError('锁后可信 canonical 登记不符')
    else: raise ValueError('canonical 只认识自身 stage 20 单目标')


@contextmanager
def hold_lock(runtime, admission, lock_target, *, guard, lock_connection=None):
    from data_pipeline.common.admission_locks import validated_connection
    guard(); runtime.check(); _shape(admission)
    if lock_target not in admission['lock_targets'] or lock_target['stage'] != 20 or lock_target['namespace'] not in ('canonical.run','canonical.admission'):
        raise ValueError('不是本 owner 的可信单目标')
    if {k:v for k,v in admission['validator'].items() if k != 'validation_digest'} != _rules(runtime):
        raise ValueError('持锁验证实现、模式或固定范围不符')
    _anchor(runtime,admission); b = io.untyped(admission['owner_binding'])
    if io.metadata(runtime,b)[0] != admission['physical']: raise ValueError('持锁物理身份不符')
    from contextlib import nullcontext
    connection=io.pg(runtime,lock=True) if lock_connection is None else nullcontext(validated_connection(runtime,admission,lock_target,lock_connection))
    with connection as pg, pg.cursor() as c:
        io.sql(runtime,c,"SELECT set_config('lock_timeout',%s,true)",(str(runtime.lock_timeout_ms)+'ms',))
        _locked(runtime,c,admission,b,lock_target)
        guard(); runtime.check()
        yield None
    # 仅 rollback/close，绝不尾 audit/current 或隐藏依赖锁。


def _validate(runtime, b, guard):
    """唯一完整入口：原 audit 单次覆盖 13 表、FK、真实 M2 推导及单次 Replay。"""
    started = time.monotonic(); before = io.entities(runtime,b['descriptor'],guard)
    with io.scratch(runtime,guard) as (scratch, checked):
        reader = ProjectionReader(runtime.dsn,ProjectionBinding(**b['binding']),guard=checked)
        original = reader.input_reader.connect
        def connect():
            db = original()
            try:
                io.configure(db,runtime,scratch)
                io.check_extensions(db)
            except BaseException as primary:
                try: db.close()
                except BaseException as secondary:
                    primary.cleanup_errors = (*getattr(primary, 'cleanup_errors', ()), secondary)
                raise
            return db
        reader.input_reader.connect = connect
        runtime.event('canonical_full_audit_started', tables=13)
        result = reader.audit()
        if result['descriptor'] != b['descriptor'] or result['audit'] != 'complete':
            raise ValueError('原完整 audit 未覆盖实际绑定')
        checked()
    # 正文 hash 已在原 audit 核对；前后 stat 相等后 fsync，不额外全量 hash。
    io.entities_current(runtime,before,guard)
    for e in before:
        with runtime.path(e['path']).open('rb') as f: os.fsync(f.fileno())
    io.entities_current(runtime,before,guard)
    inventory = dict(tables=result['tables'],columns=[(n,'BIGINT' if n == 'seq' else 'VARCHAR') for n in COLUMNS],
                     codec=CODEC,scientific_relations='13-table-exact-M2-Replay-and-FK/v1')
    # 规范 JSON 的回读使 tuple/list 差异不进入 PG audit。
    inventory = json.loads(canonical(inventory))
    cost = dict(full_audits=1,replays=1,tables=13,rows=sum(t['rows'] for t in result['tables'].values()),
                wall_seconds=time.monotonic()-started)
    runtime.event('canonical_full_audit_complete', **cost)
    return inventory,before,cost


def admit(runtime, owner_binding, *, guard):
    guard(); runtime.check(); revision = _revision()
    b = io.binding(io.untyped(encode(owner_binding)))
    physical, metadata = io.metadata(runtime,b); deps = _dependencies(runtime,b,guard); rules = _rules(runtime)
    search = digest(dict(binding=encode(b),physical=physical,rules=rules,dependencies=sorted(deps)))
    with io.pg(runtime,write=True) as pg, pg.cursor() as c:
        io.sql(runtime,c,'CREATE SCHEMA IF NOT EXISTS canonical_publication')
        io.sql(runtime,c,f'''CREATE TABLE IF NOT EXISTS {REGISTRY}(record_key UUID PRIMARY KEY,
            search TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('accepted','revoked')),
            admission JSONB NOT NULL,audit JSONB NOT NULL)''')
        io.sql(runtime,c,f"CREATE UNIQUE INDEX IF NOT EXISTS canonical_admission_reuse ON {REGISTRY} ((audit->>'reuse_key')) WHERE state='accepted'")
        candidates = io.sql(runtime,c,f"SELECT admission FROM {REGISTRY} WHERE search=%s AND state='accepted'",(search,)).fetchall()
    for (a,) in candidates:
        try: verify_current(runtime,a,guard=guard)
        except ValueError: continue
        runtime.event('canonical_admit_reused',admission_id=a['admission_id']); return a
    key = str(uuid.uuid4())
    inventory, entities, cost = _validate(runtime,b,guard)
    validation = dict(metadata_digest=digest(metadata),inventory_digest=digest(inventory),scientific_relations='original-audit-13-tables-one-Replay/v1')
    a = dict(contract=CONTRACT,owner='canonical',owner_revision=revision,owner_binding=encode(b),binding_codec=CODEC,
             physical=physical,validator={**rules,'validation_digest':digest(validation)},inventory_digest=digest(inventory),
             entities=entities,dependencies=sorted(deps),lock_targets=_targets(physical,b,deps,key))
    a['admission_id'] = digest(a); _shape(a)
    reuse = digest(dict(search=search,entities=entities,inventory=inventory,validation=validation))
    from data_pipeline.common.admission_locks import LockConnections
    with LockConnections() as connections, ExitStack() as locks:
        for t in a['lock_targets']:
            if t['stage'] != 10: continue
            dep = next(d for d in deps.values() if t in d['lock_targets'])
            dr = runtime.dependency_runtimes[dep['admission_id']]
            io.dependency_check(dr,runtime)
            def locked_guard():
                guard(); runtime.check(); io.dependency_check(dr,runtime)
            lease=connections.borrow(dr,t,guard=locked_guard)
            locks.enter_context(upstream.hold_lock(dr,dep,t,guard=locked_guard,lock_connection=lease))
        with io.pg(runtime,write=True) as pg, pg.cursor() as c:
            io.sql(runtime,c,"SELECT set_config('lock_timeout',%s,true)",(str(runtime.lock_timeout_ms)+'ms',))
            _locked(runtime,c,a,b,next(t for t in a['lock_targets'] if t['namespace']=='canonical.run'))
            _dependencies(runtime,b,guard)
            if io.metadata(runtime,b) != (physical,metadata) or _rules(runtime) != rules:
                raise ValueError('准入期间实际物理、目录或验证规则漂移')
            io.entities_current(runtime,entities,guard)
            io.sql(runtime,c,f"INSERT INTO {REGISTRY} VALUES (%s,%s,'accepted',%s,%s) ON CONFLICT DO NOTHING",
                   (key,search,Json(a),Json(dict(inventory=inventory,validation=validation,cost=cost,reuse_key=reuse))))
            stored = io.sql(runtime,c,f"SELECT admission FROM {REGISTRY} WHERE audit->>'reuse_key'=%s AND state='accepted'",(reuse,)).fetchone()
            if stored is None: raise ValueError('可信准入登记未完成')
            a = stored[0]
    runtime.event('canonical_admit_complete',admission_id=a['admission_id'],cost=cost)
    return a


@dataclass
class ReadSession:
    iterator: object = None
    receipt: object = None
    exhausted: bool = False
    failed: bool = False
    rows: int = 0
    bytes: int = 0
    hasher: object = field(default_factory=hashlib.sha256)

    def __iter__(self): return self

    def __next__(self):
        try: return next(self.iterator)
        except StopIteration: raise
        except BaseException:
            self.failed = True
            raise

    def close(self):
        if not self.exhausted: self.failed = True
        try: self.iterator.close()
        except BaseException:
            self.failed = True
            raise


def _request(runtime, a, request):
    runtime.check(); fields(request,REQUEST_FIELDS)
    request = json.loads(canonical(request))
    if request['codec_version'] != CODEC or request['view'] not in TABLES: raise ValueError('未知 canonical view/codec')
    for name in ('batch_rows','batch_bytes'):
        io.positive(request[name],name)
        if request[name] > getattr(runtime,'max_'+name): raise ValueError('请求批预算超过 runtime 上限')
    scope = io.untyped(request['scope_typed']); fields(scope,{'source_ids'})
    sources = scope['source_ids']; plan = io.untyped(a['owner_binding'])['descriptor']['plan']
    allowed = [r['source_id'] for r in plan['references']] if request['view']=='reference_binding' else plan['selected_sources']
    if sources is not None and (request['view'] not in SOURCE_VIEWS or type(sources) is not list
            or any(type(s) is not str for s in sources) or len(set(sources)) != len(sources)
            or [s for s in allowed if s in sources] != sources):
        raise ValueError('请求只能使用绑定原序来源子集；无来源列的表仅接受 null')
    return request,scope


def _selected_rows(runtime, a, request, scope, guard):
    d = io.untyped(a['owner_binding'])['descriptor']; table = request['view']
    with io.scratch(runtime,guard) as (scratch,checked), io.data_connection(runtime,scratch) as db:
        sources = scope['source_ids']; where = ''; args = []
        if sources is not None:
            where = ' WHERE source_id IN ('+','.join('?' for _ in sources)+')' if sources else ' WHERE FALSE'
            args = sources
        query = f'SELECT * FROM {relation(d["schema"],a["physical"]["snapshot"],table)}'+where+' ORDER BY seq'
        runtime.event('canonical_data_sql',sql=query)
        cursor = db.execute(query,args); count = 0; last = -1; hashed = hashlib.sha256()
        while True:
            checked()
            row = cursor.fetchone()
            if row is None: break
            physical = dict(zip(COLUMNS,row)); text = physical['payload']
            if type(text) is not str or len(text.encode()) > request['batch_bytes']:
                raise ValueError('单行原正文超过批预算')
            if type(physical['seq']) is not int or physical['seq'] <= last or (sources is None and physical['seq'] != count):
                raise ValueError('原 seq 缺失、重复或顺序变化')
            last = physical['seq']; count += 1
            hashed.update(encode(tuple(row)).encode()+b'\n')
            payload = io.untyped(text); validate_row(table,payload,physical=physical)
            runtime.event('canonical_selected_row',bytes=len(text.encode()),table=table)
            yield payload
        if sources is None and d['tables'][table] != dict(rows=count,sha256=hashed.hexdigest()):
            raise ValueError('所选整表行数或原序 typed 摘要不符')
        checked()


@contextmanager
def open_reader(runtime, admission, request, *, guard):
    request,scope = _request(runtime,admission,request)
    # 持有独立规范副本，外部变更不改变在读请求或固定身份。
    admission = json.loads(canonical(admission))
    verify_current(runtime,admission,guard=guard)
    session = ReadSession(); source = _selected_rows(runtime,admission,request,scope,guard)
    def limits():
        _request(runtime,admission,request)
    def batches():
        pending = []
        def output():
            limits()
            verify_current(runtime,admission,guard=guard)
            limits()
            text = encode(pending)
            return dict(rows_typed=text,codec_version=CODEC,rows=len(pending),bytes=len(text.encode()))
        for row in source:
            guard(); _request(runtime,admission,request)
            single = encode([row]).encode(); row_bytes = len(encode(row).encode())
            if len(single) > request['batch_bytes']: raise ValueError('单行编码超过批字节预算')
            candidate = encode([*pending,row]).encode()
            if pending and (len(pending) >= request['batch_rows'] or len(candidate) > request['batch_bytes']):
                yield output(); pending = []
                guard(); limits()
            # 累计行数与原行编码字节仅计量；硬上限约束实际批封装。
            pending.append(row); session.rows += 1; session.bytes += row_bytes
            session.hasher.update(encode(row).encode()+b'\n')
        if pending: yield output()
        guard(); limits()
        session.exhausted = True
    session.iterator = batches()
    with io.cleanup([source.close,session.close]):
        yield session
    if session.exhausted and not session.failed:
        limits()
        verify_current(runtime,admission,guard=guard)
        limits()
        d = io.untyped(admission['owner_binding'])['descriptor']
        coverage = {t:d['tables'][t] for t in ('source_coverage','scope_gap','qualification_change','current_routes')}
        session.receipt = dict(contract='component-publication-read/v1',admission_id=admission['admission_id'],
            request_digest=digest(request),rows=session.rows,typed_digest=session.hasher.hexdigest(),execution='complete',
            coverage_ref=encode(dict(admission_id=admission['admission_id'],scope=scope,tables=coverage,
                                    schema=d['schema'],snapshot=admission['physical']['snapshot'],business_absence='Unknown')))
