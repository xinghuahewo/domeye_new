"""Detection P1：固定制品的准入、轻量 current、单目标锁和有界读。"""
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import subprocess
import uuid
from psycopg2.extras import Json
from data_pipeline.bgp.archive import admission as upstream
from data_pipeline.bgp.archive.value_codec import ADMISSION_FIELDS, PHYSICAL_FIELDS, VALIDATOR_FIELDS, ENTITY_FIELDS, LOCK_FIELDS
from data_pipeline.bgp.archive.validation import _finish_close
from data_pipeline.analysis.detection.qualification_contract import validate_identity, LAKE_PROFILE
from data_pipeline.analysis.detection.publication_codec import CODEC, typed, untyped, canonical, digest, fields, REQUEST_FIELDS
from data_pipeline.analysis.detection import publication_io as io

REGISTRY = 'detection.publication_admissions'
ROOT = Path(__file__).resolve().parents[4]


@dataclass
class Runtime:
    dsn: str
    output_root: Path
    allowed_roots: tuple
    scratch_root: Path
    dependency_admissions: tuple
    dependency_runtimes: dict
    fixture_only: bool = False
    audit_sink: object = None
    memory_bytes: int | None = None
    max_temp_bytes: int | None = None
    max_rss_bytes: int | None = None
    lock_timeout_ms: int | None = None
    execution_profile: str | None = None
    expected_detection_binding: dict | None = None
    min_free_bytes: int | None = None

    def __post_init__(self):
        if type(self.fixture_only) is not bool: raise ValueError('Detection人工模式必须为显式bool')
        if self.fixture_only:
            if self.execution_profile is not None or self.expected_detection_binding is not None:
                raise ValueError('Detection人工与真实候选参数不得混用')
            self._profile = 'synthetic-fixture/v1'
        elif self.execution_profile == 'real-candidate/v1':
            if type(self.expected_detection_binding) is not dict:
                raise ValueError('Detection真实候选必须提供完整绑定')
            self._profile = self.execution_profile
        else: raise ValueError('Detection必须显式选择人工或real-candidate/v1')
        self._mode = (self.fixture_only, self.execution_profile)
        defaults = dict(memory_bytes=256*1024**2, max_temp_bytes=512*1024**2,
                        max_rss_bytes=2*1024**3, lock_timeout_ms=2000, min_free_bytes=0)
        for name, value in defaults.items():
            if getattr(self, name) is None:
                if not self.fixture_only: raise ValueError('Detection真实候选缺显式资源参数：' + name)
                setattr(self, name, value)
        self.validate_budgets()
        roots = tuple(Path(p) for p in self.allowed_roots)
        self.allowed_roots = tuple(p.resolve(strict=True) for p in roots)
        if not self.allowed_roots or type(self.dsn) is not str or not self.dsn.strip():
            raise ValueError('Detection必须显式指定DSN及允许根')
        if not self.fixture_only:
            for root in roots: self.path(root)
        self.output_root = self.path(self.output_root); self.scratch_root = self.path(self.scratch_root)
        if not self.output_root.is_dir() or not self.scratch_root.is_dir():
            raise ValueError('Detection输出与scratch须为既有目录')
        if not self.fixture_only:
            self.expected_detection_binding = untyped(typed(self.expected_detection_binding))
            fields(self.expected_detection_binding, {'run_id', 'snapshot', 'identity', 'scope'})
            validate_identity(self.expected_detection_binding['identity'])
            self.separate_scratch(self.output_root)
            stat = self.scratch_root.stat()
            self._scratch_identity = (stat.st_dev, stat.st_ino)
        self._scope_digest = self.scope_digest()
        self.resource_usage = dict(checks=0, peak_rss_bytes=0, min_free_bytes=None,
                                   rss_scope='whole_process_lifetime_peak_excluding_pg')

    def scope_digest(self):
        return digest([self.dsn, [str(p) for p in self.allowed_roots], str(self.output_root),
                       str(self.scratch_root), self.expected_detection_binding])

    def validate_budgets(self):
        if type(self.fixture_only) is not bool or (self.fixture_only, self.execution_profile) != self._mode:
            raise ValueError('Detection Runtime模式漂移')
        for name in ('memory_bytes', 'max_temp_bytes', 'max_rss_bytes', 'lock_timeout_ms', 'min_free_bytes'):
            minimum = 0 if name == 'min_free_bytes' and self.fixture_only else 1
            if type(getattr(self, name)) is not int or getattr(self, name) < minimum:
                raise ValueError('Detection预算必须为有限整数：' + name)

    def separate_scratch(self, root):
        if self.fixture_only: return
        root = self.path(root)
        if root == self.scratch_root or root in self.scratch_root.parents or self.scratch_root in root.parents:
            raise ValueError('Detection scratch与原输入/制品目录未隔离')

    def validate_scope(self):
        if self.scope_digest() != self._scope_digest: raise ValueError('Detection Runtime固定范围漂移')
        if self.fixture_only: return
        try:
            scratch = self.path(self.scratch_root)
            self.separate_scratch(self.output_root)
            stat = scratch.stat()
        except (ValueError, OSError) as exc:
            raise ValueError('Detection scratch实际路径漂移') from exc
        if not scratch.is_dir() or (stat.st_dev, stat.st_ino) != self._scratch_identity:
            raise ValueError('Detection scratch目录身份漂移')

    def check_binding(self, binding):
        self.resource_guard()
        if not self.fixture_only and binding != self.expected_detection_binding:
            raise ValueError('绑定超出Detection真实候选完整范围')

    def resource_guard(self):
        # 共享PG/SQL helper协议沿用Detection自身保护，不依赖M2私态。
        io.check(self, lambda: None)

    path = upstream.Runtime.path
    event = upstream.Runtime.event


def inspect_binding(runtime, run_id, snapshot):
    runtime.validate_budgets()
    if type(run_id) is not str or not run_id.isascii() or not run_id.isalnum() or type(snapshot) is not int or snapshot < 0:
        raise ValueError('Detection固定身份非法')
    with upstream._pg(runtime) as pg, pg.cursor() as c:
        row = upstream._sql(runtime, c, 'SELECT state,schema_name,snapshot,identity,scope FROM detection.runs WHERE run_id=%s', (run_id,)).fetchone()
    if row is None or row[:3] != ('complete', 'det_' + run_id, snapshot): raise ValueError('Detection尚未完整完成或固定身份不符')
    validate_identity(row[3])
    result = dict(run_id=run_id, snapshot=snapshot, identity=row[3], scope=row[4])
    runtime.check_binding(result)
    return result


def _binding(runtime, b):
    fields(b, {'run_id', 'snapshot', 'identity', 'scope'})
    if inspect_binding(runtime, b['run_id'], b['snapshot']) != b: raise ValueError('Detection原绑定漂移')


def _rules(runtime=None):
    paths = sorted(Path(__file__).parent.glob('*.py'))
    code = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    code['upstream'] = upstream._rules()
    from data_pipeline.common.admission_locks import code_sha as lock_code_sha
    code['lock_connection_code']=lock_code_sha()
    return dict(version='detection-publication/v1', code_sha256=digest(code), typed_schema='detection-m3-three-tables/v2',
                typed_codec=CODEC, rules_digest=digest(dict(execution_profile=runtime._profile if runtime is not None else 'synthetic-fixture/v1', tables=io.TABLES, inventory='detection-lake-integrity/v2', custody='immutable-stat/v1')))


def _dependencies(runtime, b, guard):
    ids = [a['admission_id'] for a in runtime.dependency_admissions]
    if len(ids) != len(set(ids)) or set(runtime.dependency_runtimes) != set(ids): raise ValueError('依赖Runtime/Admission集合不符')
    m2 = []; refs = {}
    for a in runtime.dependency_admissions:
        io.check(runtime, guard)
        dependency = runtime.dependency_runtimes[a['admission_id']]
        if (dependency.fixture_only, dependency.execution_profile) != runtime._mode:
            raise ValueError('Detection依赖Runtime模式不符')
        upstream.verify_current(dependency, a, guard=guard)
        raw = upstream.untyped(a['owner_binding'])
        if a['owner'] == 'm2': m2.append((a, raw))
        elif a['owner'] == 'reference':
            if raw['source_id'] in refs: raise ValueError('重复参考来源')
            refs[raw['source_id']] = (a, raw)
        else: raise ValueError('Detection不接受未知上游owner')
    if len(m2) != 1: raise ValueError('Detection必须绑定一个真实M2 Admission')
    a, raw = m2[0]; identity = b['identity']
    if not runtime.fixture_only:
        for entry in raw['plan']['inputs']: runtime.separate_scratch(Path(entry['path']).parent)
        for cp in raw['seal']['checkpoints']:
            for entry in cp['files']: runtime.separate_scratch(Path(entry['path']).parent)
    if (raw['run_id'], raw['snapshot'], raw['ordered_source_ids'], raw['input_binding_id']) != (identity['input_run'], identity['input_snapshot'], identity['selected_sources'], identity['input_binding_id']):
        raise ValueError('Detection原M2选择/身份不符')
    # input_binding 是原 dataclass metadata 编码；重新从真实 Admission 解出完整值。
    from data_pipeline.bgp.ordered_reader import binding_from_reader
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from dataclasses import asdict
    actual = asdict(binding_from_reader(ObservationReader(runtime.dependency_runtimes[a['admission_id']].dsn, raw['run_id'], raw['snapshot'], raw['ordered_source_ids'], profile='observation')))
    actual = json.loads(canonical(actual))
    if actual != identity['input_binding']: raise ValueError('Detection完整M2原值不符')
    if 'result_window_rule' in identity:
        from data_pipeline.analysis.detection.result_window import windows, identity_windows
        identity_windows(identity,b['scope'])
        windows(b['scope'],identity['result_window'],raw['plan']['manifest'])
    sources = identity['reference_sources']; checkpoints = {}
    if set(refs) != {v['source_id'] for v in sources.values()}: raise ValueError('Detection真实参考Admission缺项或多项')
    for role, source in sources.items():
        ref_a, ref = refs[source['source_id']]
        if ref['m2_admission_id'] != a['admission_id'] or ref['m2_binding'] != raw: raise ValueError('参考绑定不是同一M2')
        cp = raw['seal']['checkpoints'][ref['checkpoint_ordinal']]
        if cp['source_id'] != source['source_id'] or source != dict(source_id=cp['source_id'], rows=cp['counts']['references'], snapshot_ref=f"{raw['run_id']}:{raw['snapshot']}"):
            raise ValueError('参考原值/CP不符')
        checkpoints[role] = cp
    interpretation = dict(selection_rule='detection-reference-39578fe/v2', snapshot_ref=f"{raw['run_id']}:{raw['snapshot']}")
    result = dict(input_binding=actual, reference_sources=sources, reference_checkpoints=checkpoints, reference_interpretation=interpretation)
    if any(identity[k] != v for k, v in result.items()): raise ValueError('原参考解释或checkpoint不符')
    return sorted(ids), result


def _target(physical, namespace, key):
    return dict(stage=30, system_identifier=physical['system_identifier'], database_oid=physical['database_oid'], namespace=namespace, key=key)


def _shape(a):
    fields(a, ADMISSION_FIELDS); fields(a['physical'], PHYSICAL_FIELDS); fields(a['validator'], VALIDATOR_FIELDS)
    if a['contract'] != 'component-publication-admission/v1' or a['owner'] != 'detection' or a['binding_codec'] != CODEC: raise ValueError('Detection合同不符')
    if a['admission_id'] != digest({k:v for k,v in a.items() if k != 'admission_id'}): raise ValueError('Admission摘要不符')
    def sha(x): return type(x) is str and len(x) == 64 and all(c in '0123456789abcdef' for c in x)
    if type(a['owner_revision']) is not str or len(a['owner_revision']) != 40 or any(c not in '0123456789abcdef' for c in a['owner_revision']): raise ValueError('验证器提交身份无效')
    if not sha(a['inventory_digest']) or any(not sha(a['validator'][k]) for k in ('code_sha256','rules_digest','validation_digest')): raise ValueError('Detection摘要类型无效')
    if type(a['dependencies']) is not list or a['dependencies'] != sorted(set(a['dependencies'])) or any(not sha(x) for x in a['dependencies']): raise ValueError('依赖ID必须唯一有序')
    if type(a['entities']) is not list or [e['path'] for e in a['entities']] != sorted(set(e['path'] for e in a['entities'])): raise ValueError('实体必须唯一有序')
    for e in a['entities']:
        fields(e, ENTITY_FIELDS)
        if not sha(e['sha256']) or any(type(e[k]) is not int or e[k]<0 for k in ('device','inode','size','mtime_ns','ctime_ns')): raise ValueError('实体字段无效')
    for t in a['lock_targets']: fields(t, LOCK_FIELDS)
    b = untyped(a['owner_binding']); targets = a['lock_targets']
    if len(targets) != 2 or targets[0] != _target(a['physical'], 'detection.run', b['run_id']) or targets[1] != _target(a['physical'], 'detection.admission', targets[1]['key']):
        raise ValueError('Detection必须保留两个真实stage30目标')
    uuid.UUID(targets[1]['key'])


def _anchor(runtime, a):
    with upstream._pg(runtime) as pg, pg.cursor() as c:
        row = upstream._sql(runtime, c, f'SELECT state,admission,audit FROM {REGISTRY} WHERE record_key=%s', (a['lock_targets'][1]['key'],)).fetchone()
    if row is None or row[:2] != ('accepted', a): raise ValueError('Detection可信完整登记不存在或已撤销')
    if not runtime.fixture_only and str(runtime.output_root / 'ready.json') not in {e['path'] for e in a['entities']}:
        raise ValueError('Detection登记完成回执超出显式输出范围')
    return row[2]


def verify_current(runtime, admission, *, guard):
    io.check(runtime, guard); _shape(admission)
    a = admission; b = untyped(a['owner_binding']); audit = _anchor(runtime, a)
    if digest(audit) != a['validator']['validation_digest']: raise ValueError('可信验收内容摘要不符')
    _binding(runtime, b)
    if {k:v for k,v in a['validator'].items() if k != 'validation_digest'} != _rules(runtime): raise ValueError('Detection新验证器身份已变化')
    physical, catalog = io.catalog(runtime, b, guard)
    if physical != a['physical'] or digest(catalog) != audit['catalog_digest']: raise ValueError('Detection物理目录漂移')
    deps, _ = _dependencies(runtime, b, guard)
    if deps != a['dependencies']: raise ValueError('Detection上游Admission改变')
    upstream._entities_current(runtime, a['entities'], guard)
    runtime.event('current_complete', full_table_scans=0)
    return a


def _locked(runtime, c, a, target):
    b = untyped(a['owner_binding'])
    if target['namespace'] == 'detection.run':
        row = upstream._sql(runtime, c, 'SELECT state,schema_name,snapshot,identity,scope FROM detection.runs WHERE run_id=%s FOR SHARE', (target['key'],)).fetchone()
        expected = ('complete', 'det_' + b['run_id'], b['snapshot'], b['identity'], b['scope'])
    else:
        row = upstream._sql(runtime, c, f'SELECT state,admission FROM {REGISTRY} WHERE record_key=%s FOR SHARE', (target['key'],)).fetchone()
        expected = ('accepted', a)
    if row != expected: raise ValueError('Detection锁后实际记录不符')


@contextmanager
def hold_lock(runtime, admission, lock_target, *, guard, lock_connection=None):
    from data_pipeline.common.admission_locks import validated_connection
    io.check(runtime, guard); _shape(admission); _anchor(runtime, admission)
    if lock_target not in admission['lock_targets']: raise ValueError('不是本Detection单目标锁')
    b = untyped(admission['owner_binding']); runtime.check_binding(b)
    if {k:v for k,v in admission['validator'].items() if k != 'validation_digest'} != _rules(runtime):
        raise ValueError('Detection锁规则/模式不符')
    physical, _ = io.catalog(runtime, b, guard)
    if physical != admission['physical']: raise ValueError('锁物理身份不符')
    # 普通事务允许 FOR SHARE；锁生命周期不执行提交。
    pg = upstream.psycopg2.connect(runtime.dsn) if lock_connection is None else validated_connection(runtime,admission,lock_target,lock_connection); primary = None
    try:
        with pg.cursor() as c:
            upstream._sql(runtime, c, "SELECT set_config('lock_timeout',%s,true)", (str(runtime.lock_timeout_ms) + 'ms',))
            _locked(runtime, c, admission, lock_target)
        yield
    except BaseException as exc: primary = exc; raise
    finally:
        if lock_connection is None:upstream._release(pg, primary)


def admit(runtime, owner_binding, *, guard):
    io.check(runtime, guard)
    b = untyped(typed(owner_binding)); _binding(runtime, b)
    deps, source_binding = _dependencies(runtime, b, guard)
    physical, catalog = io.catalog(runtime, b, guard); rules = _rules(runtime)
    reuse = digest(dict(binding=b, physical=physical, validator=rules, dependencies=deps))
    with upstream._pg(runtime, write=True) as pg, pg.cursor() as c:
        upstream._sql(runtime, c, f'''CREATE TABLE IF NOT EXISTS {REGISTRY} (
            record_key UUID PRIMARY KEY,reuse_key TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('accepted','revoked')),
            admission JSONB NOT NULL,audit JSONB NOT NULL)''')
        candidates = upstream._sql(runtime, c, f'SELECT admission FROM {REGISTRY} WHERE reuse_key=%s AND state=%s ORDER BY record_key', (reuse, 'accepted')).fetchall()
    for row in candidates:
        try: verify_current(runtime, row[0], guard=guard)
        except ValueError: continue
        runtime.event('admit_reused', full_table_scans=0)
        return row[0]
    key = str(uuid.uuid4())
    entities = [upstream._entity(runtime, f['path'], hash_body=True, guard=guard) for f in catalog['files']]
    ready_path = runtime.path(runtime.output_root / 'ready.json')
    entities.append(upstream._entity(runtime, ready_path, hash_body=True, guard=guard))
    ready = json.loads(ready_path.read_text())
    if any(ready[k] != b[k] for k in ('run_id','snapshot','identity','scope')) or ready['state'] != 'ready': raise ValueError('Detection原完成回执不符')
    from data_pipeline.analysis.detection.lake_integrity import validate_snapshot
    expected = b['identity'].get('storage_integrity', {}).get('inventory')
    if b['identity']['output_profile'] == LAKE_PROFILE and expected is None: raise ValueError('lake/v2缺原生产枚举')
    with io.connection(runtime, guard) as db:
        report = validate_snapshot(runtime.dsn, b['run_id'], b['snapshot'], b['identity'], expected,
            source_binding=source_binding, guard=lambda: io.check(runtime, guard), validated_connection=db,
            scope=b['scope'], compare_inventory=expected is not None, audit_sink=lambda **event: runtime.event(event.pop('kind'), **event))
    if ready['records'] != report['inventory']['records']['rows'] or ready['state_entries'] != report['inventory']['state_entries']['rows'] or b['identity']['qualification_counts'] != report['qualification_counts']:
        raise ValueError('Detection原完成计数不符')
    inventory_digest = digest(dict(inventory=report['inventory'], schemas=report['schemas'], version=report['version']))
    audit = dict(catalog_digest=digest(catalog), validation=report)
    revision = subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip()
    for path in [*sorted(Path(__file__).parent.glob('*.py')), ROOT/'backend/data_pipeline/common/admission_locks.py']:
        relative = str(path.relative_to(ROOT))
        try:
            committed = subprocess.check_output(['git','show',revision + ':' + relative], cwd=ROOT, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as exc:
            raise ValueError('Detection准入须来自包含实际验证器的固定提交') from exc
        if committed != path.read_bytes(): raise ValueError('Detection准入须来自包含实际验证器的固定提交')
    a = dict(contract='component-publication-admission/v1', owner='detection', owner_revision=revision, owner_binding=typed(b), binding_codec=CODEC,
        physical=physical, validator=dict(rules, validation_digest=digest(audit)), inventory_digest=inventory_digest,
        entities=sorted(entities,key=lambda e:e['path']), dependencies=deps,
        lock_targets=[_target(physical,'detection.run',b['run_id']),_target(physical,'detection.admission',key)])
    a['admission_id'] = digest(a)
    _binding(runtime,b); upstream._entities_current(runtime,a['entities'],guard)
    if _dependencies(runtime,b,guard)[0] != deps or io.catalog(runtime,b,guard) != (physical,catalog) or _rules(runtime) != rules: raise ValueError('准入末尾绑定漂移')
    with upstream._pg(runtime, write=True) as pg, pg.cursor() as c:
        upstream._sql(runtime,c,"SELECT set_config('lock_timeout',%s,true)",(str(runtime.lock_timeout_ms)+'ms',))
        # 同一原绑定的并发首次准入最终串行收口，不覆盖历史验收。
        upstream._sql(runtime,c,'SELECT pg_advisory_xact_lock(%s)',(int(reuse[:15],16),))
        existing = upstream._sql(runtime,c,f'SELECT admission FROM {REGISTRY} WHERE reuse_key=%s AND state=%s ORDER BY record_key',(reuse,'accepted')).fetchall()
        for row in existing:
            try: verify_current(runtime,row[0],guard=guard)
            except ValueError: continue
            return row[0]
        _locked(runtime,c,a,a['lock_targets'][0])
        upstream._sql(runtime,c,f'INSERT INTO {REGISTRY} VALUES (%s,%s,%s,%s,%s)',(key,reuse,'accepted',Json(a),Json(audit)))
    runtime.event('admit_complete', report=report)
    return a


@dataclass
class ReadSession:
    iterator: object = None
    receipt: object = None
    exhausted: bool = False
    rows: int = 0
    hasher: object = field(default_factory=hashlib.sha256)
    def __iter__(self): return self
    def __next__(self): return next(self.iterator)
    def close(self): self.iterator.close()


@contextmanager
def open_reader(runtime, admission, request, *, guard):
    fields(request, REQUEST_FIELDS); request = json.loads(canonical(request))
    if request['codec_version'] != CODEC or type(request['batch_rows']) is not int or not 1 <= request['batch_rows'] <= 10000 or type(request['batch_bytes']) is not int or request['batch_bytes'] < 1:
        raise ValueError('Detection读codec或有限批预算无效')
    if request['view'] not in (*io.TABLES, 'revisions', 'decisions', 'qualified_revisions', 'result_revisions', 'result_coverage'): raise ValueError('Detection未知view')
    scope = untyped(request['scope_typed']); fields(scope, {'start','stop','key','at_position'})
    if type(scope['start']) is not int or scope['start'] < 0 or (scope['stop'] is not None and (type(scope['stop']) is not int or scope['stop'] < scope['start'])) or (scope['key'] is not None and type(scope['key']) is not str): raise ValueError('Detection有限筛选非法')
    b = untyped(admission['owner_binding']); pos = scope['at_position']
    if request['view'] == 'qualified_revisions':
        if type(pos) is not list or len(pos) != 4 or any(type(x) is not int or x < 0 for x in pos) or pos > b['identity']['qualification_as_of_position']: raise ValueError('必须明确有效四元处理位置')
    elif pos is not None: raise ValueError('原表读取不接受处理位置裁剪')
    if request['view'].startswith('result_'):
        from data_pipeline.analysis.detection.result_window import identity_windows
        identity_windows(b['identity'],b['scope'])
        if request['view']=='result_coverage' and (scope['start']!=0 or scope['stop'] is not None or scope['key'] is not None): raise ValueError('结果Coverage必须完整所选来源')
    verify_current(runtime, admission, guard=guard)
    source = io.rows(runtime, b, request, scope, guard); session = ReadSession()
    def batches():
        pending = []
        for row in source:
            io.check(runtime, guard)
            if len(typed([row]).encode()) > request['batch_bytes']: raise ValueError('Detection单行超过字节预算')
            if pending and (len(pending) >= request['batch_rows'] or len(typed([*pending,row]).encode()) > request['batch_bytes']):
                text = typed(pending); yield dict(rows_typed=text,codec_version=CODEC,rows=len(pending),bytes=len(text.encode())); pending=[]
            pending.append(row); session.rows += 1
            encoded = typed(row).encode(); session.hasher.update(len(encoded).to_bytes(8,'big')); session.hasher.update(encoded)
        if pending:
            text = typed(pending); yield dict(rows_typed=text,codec_version=CODEC,rows=len(pending),bytes=len(text.encode()))
        session.exhausted = True
    session.iterator = batches(); primary = None
    try: yield session
    except BaseException as exc: primary = exc; raise
    finally:
        _finish_close([source.close, session.iterator.close], primary)
        if primary is None and session.exhausted:
            verify_current(runtime, admission, guard=guard)
            window_ref = {}
            if request['view'].startswith('result_'):
                window_ref = dict(result_window=b['identity']['result_window'], window_coverage=b['identity']['window_coverage'], selection_rule=b['identity']['result_window_rule'])
            session.receipt = dict(contract='component-publication-read/v1',admission_id=admission['admission_id'],request_digest=digest(request),rows=session.rows,typed_digest=session.hasher.hexdigest(),execution='complete',
                coverage_ref=typed(dict(admission_id=admission['admission_id'],view=request['view'],scope=scope,run_scope=b['scope'],qualification_table='m3_entries',qualification_counts=b['identity']['qualification_counts'],as_of_position=b['identity']['qualification_as_of_position'],business_absence='Unknown',**window_ref)))
