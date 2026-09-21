"""Feature M3 有限公共 P1；只登记本模块验收，不生产科学数据或组合 P。"""
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass, field
import hashlib
import json
import math
import re
import resource
import shutil
import sys
from pathlib import Path
import subprocess
import uuid

from psycopg2.extras import Json

from data_pipeline.bgp.archive import admission as upstream
from data_pipeline.bgp.archive.value_codec import typed, untyped, digest, fields, canonical, ADMISSION_FIELDS, PHYSICAL_FIELDS, VALIDATOR_FIELDS, ENTITY_FIELDS, LOCK_FIELDS, REQUEST_FIELDS
from data_pipeline.bgp.archive.validation import _closing
from data_pipeline.bgp.replay.route_replay import identity
from data_pipeline.analysis.features.qualification import PROFILE, SCHEMA_VERSION, RULE, DIMENSIONS
from data_pipeline.analysis.features import publication_io as io

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = 'component-publication-admission/v1'
NAMESPACES = ('feature.run', 'feature.qualification', 'feature.admission')
REGISTRY = 'feature.publication_admissions'


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
    memory_limit: str | None = None
    max_temp_bytes: int | float | None = None
    max_rss_bytes: int | float | None = None
    lock_timeout_ms: int | None = None
    execution_profile: str | None = None
    expected_feature_binding: dict | None = None
    min_free_bytes: int | None = None

    def __post_init__(self):
        if type(self.fixture_only) is not bool:
            raise ValueError('Feature人工模式须为显式bool')
        if self.fixture_only:
            if self.execution_profile is not None or self.expected_feature_binding is not None:
                raise ValueError('Feature人工与真实候选参数不得混用')
            self._profile = 'synthetic-fixture/v1'
        elif self.execution_profile == 'real-candidate/v1':
            if type(self.expected_feature_binding) is not dict:
                raise ValueError('Feature真实候选缺少完整expected_feature_binding')
            self._profile = self.execution_profile
        else:
            raise ValueError('Feature必须显式选择人工或real-candidate/v1')
        self._mode = (self.fixture_only, self.execution_profile)
        defaults = dict(memory_limit='256MB', max_temp_bytes=512*1024**2,
                        max_rss_bytes=1024**3, lock_timeout_ms=2000, min_free_bytes=0)
        for name, value in defaults.items():
            if getattr(self, name) is None:
                if not self.fixture_only:
                    raise ValueError('Feature真实候选缺少显式资源参数：' + name)
                setattr(self, name, value)
        self.validate_budgets()
        roots = tuple(Path(p) for p in self.allowed_roots)
        self.allowed_roots = tuple(p.resolve(strict=True) for p in roots)
        if not self.allowed_roots or type(self.dsn) is not str or not self.dsn.strip():
            raise ValueError('Feature必须显式指定DSN及允许根')
        if not self.fixture_only:
            for root in roots: self.path(root)
        self.output_root = self.path(self.output_root)
        self.scratch_root = self.path(self.scratch_root)
        if not self.output_root.is_dir() or not self.scratch_root.is_dir():
            raise ValueError('Feature输出与scratch须为既有目录')
        if not self.fixture_only:
            if (self.output_root == self.scratch_root or self.output_root in self.scratch_root.parents
                    or self.scratch_root in self.output_root.parents):
                raise ValueError('Feature scratch与原输出目录未隔离')
            self.expected_feature_binding = untyped(typed(self.expected_feature_binding))
            _binding(self.expected_feature_binding)
            self._scope_digest = digest([self.expected_feature_binding, str(self.output_root), str(self.scratch_root)])
            stat = self.scratch_root.stat()
            self._scratch_identity = (stat.st_dev, stat.st_ino)
        self.resource_usage = dict(checks=0, peak_rss_bytes=0, min_free_bytes=None,
                                   rss_scope='whole_process_lifetime_peak_excluding_pg')

    def validate_budgets(self):
        for name in ('max_temp_bytes', 'max_rss_bytes'):
            value = getattr(self, name)
            if (type(value) not in (int, float) or value < 1
                    or (type(value) is float and not math.isfinite(value))):
                raise ValueError('Feature预算须为至少1的有限数值：' + name)
        if (type(self.memory_limit) is not str or not re.fullmatch(r'[1-9][0-9]*(?:\.[0-9]+)?\s*(?:B|KB|MB|GB|TB)', self.memory_limit, re.I)
                or type(self.lock_timeout_ms) is not int or self.lock_timeout_ms < 1
                or type(self.min_free_bytes) is not int or self.min_free_bytes < (0 if self.fixture_only else 1)):
            raise ValueError('Feature内存/锁/空闲盘预算无效')

    def validate_scratch(self):
        if self.fixture_only:
            return
        try:
            scratch = self.path(self.scratch_root)
            output = self.path(self.output_root)
            stat = scratch.stat()
        except (OSError, ValueError) as exc:
            raise ValueError('Feature scratch实际路径/允许根漂移') from exc
        if (not scratch.is_dir() or (stat.st_dev, stat.st_ino) != self._scratch_identity
                or scratch == output or scratch in output.parents or output in scratch.parents):
            raise ValueError('Feature scratch目录身份/隔离范围漂移')

    def check_binding(self, binding):
        self.resource_guard()
        if not self.fixture_only and binding != self.expected_feature_binding:
            raise ValueError('绑定超出Feature真实候选固定范围')

    def resource_guard(self):
        self.validate_budgets()
        if (self.fixture_only, self.execution_profile) != self._mode:
            raise ValueError('Feature Runtime模式漂移')
        if self.fixture_only:
            return
        if digest([self.expected_feature_binding, str(self.output_root), str(self.scratch_root)]) != self._scope_digest:
            raise ValueError('Feature Runtime固定范围漂移')
        self.validate_scratch()
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)
        free = shutil.disk_usage(self.scratch_root).free
        usage = self.resource_usage
        usage['checks'] += 1
        usage['peak_rss_bytes'] = max(usage['peak_rss_bytes'], rss)
        usage['min_free_bytes'] = free if usage['min_free_bytes'] is None else min(usage['min_free_bytes'], free)
        if rss > self.max_rss_bytes or free < self.min_free_bytes:
            raise ValueError('Feature真实候选RSS/最低空闲盘保护')

    def guarded(self, caller):
        def check():
            self.resource_guard()
            caller()
            self.resource_guard()
        return check

    def path(self, path):
        p = Path(path); resolved = p.resolve(strict=True)
        if str(p) != str(resolved) or not any(resolved == r or r in resolved.parents for r in self.allowed_roots):
            raise ValueError('Feature实体超出允许根或路径非规范')
        if any(x.is_symlink() for x in (p, *p.parents)):
            raise ValueError('Feature禁止符号链接实体')
        return resolved

    def event(self, kind, **values):
        if self.audit_sink is not None:
            self.audit_sink(dict(kind=kind, **values))


def _rules(runtime=None):
    # 精确文件摘要，不以整个仓库HEAD强迫已有上游登记重新admit。
    paths = sorted((ROOT / 'backend/data_pipeline/analysis/features').glob('*.py'))
    code = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    from data_pipeline.analysis.features.identity import code_identity
    environment = code_identity()
    code.update(environment={k: environment[k] for k in ('python', 'packages')}, dependencies=environment['files'])
    from data_pipeline.common.admission_locks import code_sha as lock_code_sha
    code['lock_connection_code']=lock_code_sha()
    return dict(version='feature-publication/v1', code_sha256=digest(code),
                typed_schema=SCHEMA_VERSION, typed_codec=io.CODEC,
                rules_digest=digest(dict(execution_profile=runtime._profile if runtime is not None else 'synthetic-fixture/v1', tables=io.TABLES, audit='existing-calculation-exact-multiset/v1',
                                        qualification=RULE, custody='immutable-stat/v1')))


def _revision():
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    from data_pipeline.analysis.features.identity import code_identity
    for relative in code_identity()['files']:
        path = ROOT / relative
        committed = subprocess.check_output(['git', 'show', revision + ':' + relative], cwd=ROOT)
        if committed != path.read_bytes():
            raise ValueError('Feature准入必须来自包含实际实现的固定提交')
    return revision


def _shape(a):
    fields(a, ADMISSION_FIELDS); fields(a['physical'], PHYSICAL_FIELDS); fields(a['validator'], VALIDATOR_FIELDS)
    if a['contract'] != CONTRACT or a['owner'] != 'feature' or a['binding_codec'] != io.CODEC:
        raise ValueError('Feature Admission合同不符')
    for e in a['entities']: fields(e, ENTITY_FIELDS)
    for t in a['lock_targets']: fields(t, LOCK_FIELDS)
    if a['admission_id'] != digest({k: v for k, v in a.items() if k != 'admission_id'}):
        raise ValueError('Feature Admission摘要不符')


def _binding(b):
    fields(b, {'specification','reference_version','catalog_ref','completion_digest','profile','schema_version',
               'run_id','snapshot','specification_digest','qualification_digest','table_counts','input_seals','binding_id'})
    if type(b) is not dict or b.get('profile') != PROFILE or b.get('schema_version') != SCHEMA_VERSION:
        raise ValueError('P1首片仅M3，旧8表沿原入口')
    if b.get('binding_id') != identity({k: v for k, v in b.items() if k != 'binding_id'}):
        raise ValueError('Feature完整binding摘要不符')
    if type(b.get('run_id')) is not str or len(b['run_id']) != 32 or any(x not in '0123456789abcdef' for x in b['run_id']):
        raise ValueError('Feature run格式无效')
    if type(b.get('snapshot')) is not int or b['snapshot'] < 0:
        raise ValueError('Feature snapshot无效')
    return b


def _metadata(runtime, b):
    """仅PG登记/固定目录元数据；不连接业务表进行count或FK。"""
    _binding(b); runtime.check_binding(b); run = b['run_id']; snapshot = b['snapshot']; catalog = 'fl_' + run
    with io.pg(runtime) as pg, pg.cursor() as c:
        def sql(q, args=()): return io.query(runtime, c, q, args)
        physical_row = sql('SELECT system_identifier::text,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) FROM pg_control_system()').fetchone()
        row = sql('SELECT state,snapshot,schema_name,specification FROM feature.runs WHERE run_id=%s', (run,)).fetchone()
        if row != ('complete', snapshot, 'f_' + run, b['specification']):
            raise ValueError('Feature run撤销/错版/规格漂移')
        completion = sql('SELECT snapshot,receipt FROM feature.qualified_results WHERE run_id=%s', (run,)).fetchone()
        if completion is None or completion[0] != snapshot or identity(completion[1]) != b['completion_digest']:
            raise ValueError('Feature资格完成锚漂移')
        if completion[1]['specification_digest'] != identity(b['specification']):
            raise ValueError('Feature完成规格摘要不符')
        metadata = {}
        for table in ('ducklake_metadata', 'ducklake_schema', 'ducklake_table', 'ducklake_column',
                      'ducklake_data_file', 'ducklake_delete_file', 'ducklake_inlined_data_tables',
                      'ducklake_partition_info', 'ducklake_partition_column', 'ducklake_snapshot',
                      'ducklake_column_mapping', 'ducklake_name_mapping'):
            values = sql(f'SELECT to_jsonb(t) FROM {catalog}.{table} t').fetchall()
            metadata[table] = sorted((r[0] for r in values), key=canonical)
    physical = dict(system_identifier=physical_row[0], database_oid=physical_row[1], catalog=catalog,
                    schema='f_' + run, root=str(runtime.output_root / 'parquet'), snapshot=snapshot)
    if b['catalog_ref'] != {k: physical[k] for k in ('system_identifier', 'database_oid')}:
        raise ValueError('同run异实际数据库身份')
    runtime.path(physical['root'])
    options = metadata['ducklake_metadata']
    if not any(x['key'] == 'data_path' and x['value'] == physical['root'] + '/' for x in options):
        raise ValueError('实际Feature DuckLake根不符')
    if not any(x['key'] == 'version' and x['value'] == '0.3' for x in options):
        raise ValueError('不支持的DuckLake格式')
    if any(metadata[t] for t in ('ducklake_delete_file', 'ducklake_inlined_data_tables', 'ducklake_partition_info',
                                'ducklake_partition_column', 'ducklake_column_mapping', 'ducklake_name_mapping')):
        raise ValueError('本片不支持Feature删除/inline/分区/映射演进')
    visible = lambda x: x['begin_snapshot'] <= snapshot and (x['end_snapshot'] is None or x['end_snapshot'] > snapshot)
    schemas = [x for x in metadata['ducklake_schema'] if x['schema_name'] == physical['schema'] and visible(x)]
    if len(schemas) != 1 or schemas[0]['path'] != physical['schema'] + '/' or not schemas[0]['path_is_relative']:
        raise ValueError('Feature schema物理目录不符')
    tables = [x for x in metadata['ducklake_table'] if x['schema_id'] == schemas[0]['schema_id'] and visible(x)]
    if len(tables) != 12 or {x['table_name'] for x in tables} != set(io.TABLES):
        raise ValueError('缺少Feature必要表')
    files = []
    for table in tables:
        name = table['table_name']
        if table['path'] != name + '/' or not table['path_is_relative']:
            raise ValueError('Feature表物理路径不符')
        columns = sorted((x for x in metadata['ducklake_column'] if x['table_id'] == table['table_id'] and visible(x)
                          and x['parent_column'] is None), key=lambda x: x['column_order'])
        if [(x['column_name'], x['column_type']) for x in columns] != [(n, {'BIGINT':'int64','VARCHAR':'varchar','BOOLEAN':'boolean','VARCHAR[]':'list','BIGINT[]':'list'}[t]) for n, t in io.TABLES[name]]:
            raise ValueError('Feature目录typed schema不符：' + name)
        for file in metadata['ducklake_data_file']:
            if file['table_id'] != table['table_id'] or not visible(file): continue
            if (not file['path_is_relative'] or file['file_format'] != 'parquet' or file['mapping_id'] is not None
                    or file['partition_id'] is not None or file['encryption_key'] is not None or file['partial_file_info'] is not None):
                raise ValueError('不支持的Feature实体布局')
            path = runtime.path(Path(physical['root']) / schemas[0]['path'] / table['path'] / file['path'])
            files.append(dict(table=name, path=str(path), rows=file['record_count']))
    if len({x['path'] for x in files}) != len(files): raise ValueError('重复Feature实体路径')
    return physical, metadata, sorted(files, key=lambda x: x['path']), completion[1]


def _dependencies(runtime, b, guard):
    deps = {a['admission_id']: a for a in runtime.dependency_admissions}
    if len(deps) != len(runtime.dependency_admissions) or set(runtime.dependency_runtimes) != set(deps):
        raise ValueError('Feature依赖集合/Runtime映射不符')
    for key, a in deps.items():
        dep_runtime = runtime.dependency_runtimes[key]
        dep_profile = 'synthetic-fixture/v1' if dep_runtime.fixture_only else dep_runtime.execution_profile
        if dep_profile != runtime._profile:
            raise ValueError('Feature与上游P1执行模式不符')
        upstream.verify_current(runtime.dependency_runtimes[key], a, guard=guard)
    spec = b['specification']; m2 = {}; references = []
    for a in deps.values():
        raw = untyped(a['owner_binding'])
        if a['owner'] == 'm2':
            key = (raw['run_id'], raw['snapshot'])
            if key in m2: raise ValueError('Feature重复M2选择')
            m2[key] = (a, raw)
        elif a['owner'] == 'reference': references.append((a, raw))
        else: raise ValueError('Feature未知依赖owner')
    if set(m2) != {(x['run_id'], x['snapshot']) for x in spec['observation_seals']} or len(references) != 1:
        raise ValueError('Feature依赖缺失/多余')
    for seal in spec['observation_seals']:
        a, raw = m2[seal['run_id'], seal['snapshot']]
        if raw['seal'] != seal['seal']: raise ValueError('Feature实际M2封存边不符')
    for bound in spec['ordered_bindings']:
        matches = [raw for _, raw in m2.values() if json.loads(raw['input_binding']) == bound]
        if len(matches) != 1 or json.loads(matches[0]['input_binding']) != bound:
            raise ValueError('Feature原来源rank/ordered绑定不符')
    ra, ref = references[0]; expected = spec['reference_binding']
    key = (expected['run_id'], expected['snapshot'])
    if (ref['m2_admission_id'] != m2[key][0]['admission_id'] or ref['source_id'] != expected['source_sha256']
            or ref['checkpoint_ordinal'] != expected['checkpoint']['ordinal'] or ref['selected_sources'] != []):
        raise ValueError('Feature实际参考选择不符')
    reference_runtime = runtime.dependency_runtimes[ra['admission_id']]
    raw_path = reference_runtime.path(spec['reference_view']['raw_path'])
    if str(raw_path) != ref['m2_binding']['plan']['inputs'][ref['checkpoint_ordinal']]['path']:
        raise ValueError('Feature参考原件必须为实际依赖选定的授权实体')
    return deps


def _own(a):
    found = [x for x in a['lock_targets'] if x['namespace'] == 'feature.admission']
    if len(found) != 1: raise ValueError('Feature可信key目标缺失/重复')
    return found[0]


def _anchor(runtime, a):
    with io.pg(runtime) as pg, pg.cursor() as c:
        io.query(runtime, c, f'SELECT state,admission,audit FROM {REGISTRY} WHERE record_key=%s', (_own(a)['key'],))
        row = c.fetchone()
    if row is None or row[:2] != ('accepted', a):
        raise ValueError('Feature可信登记缺失/撤销/完整对象不符')
    if digest(row[2]['inventory']) != a['inventory_digest'] or a['validator']['validation_digest'] != digest(row[2]['validation']):
        raise ValueError('Feature可信验收记录内容不符')
    return row[2]


def verify_current(runtime, admission, *, guard):
    guard = runtime.guarded(guard)
    guard(); runtime.validate_budgets(); _shape(admission)
    if {k: v for k, v in admission['validator'].items() if k != 'validation_digest'} != _rules(runtime):
        raise ValueError('Feature实际validator规则变化，需新admit')
    b = _binding(untyped(admission['owner_binding'])); audit = _anchor(runtime, admission)
    physical, metadata, files, completion = _metadata(runtime, b)
    if physical != admission['physical'] or digest(metadata) != audit['validation']['metadata_digest']:
        raise ValueError('Feature物理/固定目录漂移')
    deps = _dependencies(runtime, b, guard)
    if sorted(deps) != admission['dependencies']: raise ValueError('Feature依赖Admission漂移')
    io.entities_current(runtime, admission['entities'], guard)
    runtime.event('feature_current', entities=len(admission['entities']))
    runtime.resource_guard()


def _target(physical, namespace, key):
    return dict(stage=30, system_identifier=physical['system_identifier'], database_oid=physical['database_oid'],
                namespace=namespace, key=key)


def _locked(runtime, c, b, a, target):
    if target['namespace'] == 'feature.run':
        io.query(runtime, c, 'SELECT state,snapshot,schema_name,specification FROM feature.runs WHERE run_id=%s FOR SHARE', (target['key'],))
        expected = ('complete', b['snapshot'], 'f_' + b['run_id'], b['specification'])
    elif target['namespace'] == 'feature.qualification':
        io.query(runtime, c, 'SELECT snapshot,receipt FROM feature.qualified_results WHERE run_id=%s FOR SHARE', (target['key'],))
        row = c.fetchone()
        if row is None or row[0] != b['snapshot'] or identity(row[1]) != b['completion_digest']:
            raise ValueError('Feature锁后完成锚不符')
        return
    elif target['namespace'] == 'feature.admission':
        io.query(runtime, c, f'SELECT state,admission FROM {REGISTRY} WHERE record_key=%s FOR SHARE', (target['key'],))
        expected = ('accepted', a)
    else: raise ValueError('Feature仅持有自己的stage30单目标')
    if c.fetchone() != expected: raise ValueError('Feature锁后登记不符')


@contextmanager
def hold_lock(runtime, admission, lock_target, *, guard, lock_connection=None):
    from data_pipeline.common.admission_locks import validated_connection
    guard = runtime.guarded(guard)
    guard(); _shape(admission); _anchor(runtime, admission)
    if {k: v for k, v in admission['validator'].items() if k != 'validation_digest'} != _rules(runtime):
        raise ValueError('Feature锁规则/执行模式不符')
    if lock_target not in admission['lock_targets'] or lock_target['stage'] != 30 or lock_target['namespace'] not in NAMESPACES:
        raise ValueError('Feature锁目标不属于本owner可信目标')
    b = untyped(admission['owner_binding'])
    if _metadata(runtime, b)[0] != admission['physical']: raise ValueError('Feature锁物理身份不符')
    # 普通事务允许FOR SHARE；退出rollback/close，不写状态、不尾audit。
    with _closing([]) as cleanup:
        import psycopg2
        pg = psycopg2.connect(runtime.dsn) if lock_connection is None else validated_connection(runtime,admission,lock_target,lock_connection)
        if lock_connection is None:cleanup.extend([pg.close, pg.rollback])
        with pg.cursor() as c:
            io.query(runtime, c, "SELECT set_config('lock_timeout',%s,true)", (str(runtime.lock_timeout_ms) + 'ms',))
            _locked(runtime, c, b, admission, lock_target)
        yield


def admit(runtime, owner_binding, *, guard):
    from data_pipeline.analysis.features.publication_validation import validate
    guard = runtime.guarded(guard)
    guard()
    runtime.validate_budgets()
    revision = _revision()
    b = _binding(untyped(typed(owner_binding))); deps = _dependencies(runtime, b, guard)
    physical, metadata, files, completion = _metadata(runtime, b); rules = _rules(runtime)
    search = digest(dict(binding=typed(b), physical=physical, rules=rules, dependencies=sorted(deps)))
    with io.pg(runtime, write=True) as pg, pg.cursor() as c:
        io.query(runtime, c, f'''CREATE TABLE IF NOT EXISTS {REGISTRY}(record_key UUID PRIMARY KEY,
            search TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('accepted','revoked')),
            admission JSONB NOT NULL,audit JSONB NOT NULL)''')
        io.query(runtime, c, f'SELECT admission FROM {REGISTRY} WHERE search=%s AND state=\'accepted\'', (search,))
        candidates = c.fetchall()
    for (candidate,) in candidates:
        try: verify_current(runtime, candidate, guard=guard)
        except ValueError: continue
        runtime.event('admit_reused', admission_id=candidate['admission_id']); return candidate
    key = str(uuid.uuid4())
    inventory, entities, cost = validate(runtime, b, files, completion, guard)
    validation = dict(metadata_digest=digest(metadata), inventory_digest=digest(inventory),
                      scientific_relations='existing-calculation-exact-multiset/v1')
    targets = {_target_key(t): t for dep in deps.values() for t in dep['lock_targets']}
    for ns, pk in [('feature.run', b['run_id']), ('feature.qualification', b['run_id']), ('feature.admission', key)]:
        t = _target(physical, ns, pk); targets[_target_key(t)] = t
    a = dict(contract=CONTRACT, owner='feature', owner_revision=revision,
             owner_binding=typed(b), binding_codec=io.CODEC, physical=physical,
             validator={**rules, 'validation_digest': digest(validation)}, inventory_digest=digest(inventory),
             entities=entities, dependencies=sorted(deps), lock_targets=[targets[k] for k in sorted(targets)])
    a['admission_id'] = digest(a)
    # 登记前显式按全序调用各owner单目标锁，无hold_lock内部嵌套。
    from data_pipeline.common.admission_locks import LockConnections
    with LockConnections() as connections, ExitStack() as locks:
        for t in a['lock_targets']:
            if t['stage'] == 30: continue
            owner = next(dep for dep in deps.values() if t in dep['lock_targets'])
            dr=runtime.dependency_runtimes[owner['admission_id']]
            lease=connections.borrow(dr,t,guard=guard)
            locks.enter_context(upstream.hold_lock(dr, owner, t, guard=guard,lock_connection=lease))
        with io.pg(runtime, write=True) as pg, pg.cursor() as c:
            io.query(runtime, c, "SELECT set_config('lock_timeout',%s,true)", (str(runtime.lock_timeout_ms) + 'ms',))
            for t in a['lock_targets']:
                if t['stage'] == 30 and t['namespace'] != 'feature.admission': _locked(runtime, c, b, a, t)
            _dependencies(runtime, b, guard)
            if _metadata(runtime, b)[:2] != (physical, metadata) or _rules(runtime) != rules:
                raise ValueError('Feature准入期间目录/规则漂移')
            io.entities_current(runtime, entities, guard)
            io.query(runtime, c, f'INSERT INTO {REGISTRY} VALUES (%s,%s,\'accepted\',%s,%s)',
                     (key, search, Json(a), Json(dict(inventory=inventory, validation=validation, cost=cost))))
    runtime.event('feature_admit_complete', cost=cost, admission_id=a['admission_id'])
    return a


def _target_key(t):
    return tuple(t[k] for k in ('stage', 'system_identifier', 'database_oid', 'namespace', 'key'))


@dataclass
class ReadSession:
    iterator: object = None
    receipt: object = None
    exhausted: bool = False
    rows: int = 0
    hasher: object = field(default_factory=hashlib.sha256)

    def __iter__(self): return self
    def __next__(self): return next(self.iterator)


@contextmanager
def open_reader(runtime, admission, request, *, guard):
    from data_pipeline.analysis.features.publication_validation import selected_rows
    guard = runtime.guarded(guard)
    fields(request, REQUEST_FIELDS); request = json.loads(canonical(request))
    if request['codec_version'] != io.CODEC or request['view'] not in ('windows', 'coverage'):
        raise ValueError('Feature有限view/codec不符')
    scope = untyped(request['scope_typed']); fields(scope, {'mode', 'window_role'})
    if scope['mode'] not in ('ordinary', 'ir', 'all') or scope['window_role'] not in ('initial', 'warmup', 'comparison', 'result', 'all'):
        raise ValueError('Feature选择范围无效')
    if type(request['batch_rows']) is not int or not 1 <= request['batch_rows'] <= 10000 or type(request['batch_bytes']) is not int or request['batch_bytes'] < 1:
        raise ValueError('Feature批预算无效')
    verify_current(runtime, admission, guard=guard)
    b = untyped(admission['owner_binding']); session = ReadSession()
    source = selected_rows(runtime, b, request['view'], scope, guard)
    def batches():
        pending = []
        for row in source:
            guard(); runtime.validate_budgets(); text = typed([row])
            if len(text.encode()) > request['batch_bytes']: raise ValueError('Feature单行超过批预算')
            candidate = typed([*pending, row])
            if pending and (len(pending) >= request['batch_rows'] or len(candidate.encode()) > request['batch_bytes']):
                text = typed(pending)
                yield dict(rows_typed=text, codec_version=io.CODEC, rows=len(pending), bytes=len(text.encode()))
                runtime.validate_budgets()
                pending = []
            pending.append(row); session.rows += 1
            session.hasher.update(bytes.fromhex(digest(typed(row))))
        if pending:
            text = typed(pending)
            yield dict(rows_typed=text, codec_version=io.CODEC, rows=len(pending), bytes=len(text.encode()))
        runtime.validate_budgets()
        session.exhausted = True
    session.iterator = batches()
    with _closing([source.close, session.iterator.close]):
        yield session
    if session.exhausted:
        runtime.validate_budgets()
        verify_current(runtime, admission, guard=guard)
        runtime.resource_guard()
        session.receipt = dict(contract='component-publication-read/v1', admission_id=admission['admission_id'],
            request_digest=digest(request), rows=session.rows, typed_digest=session.hasher.hexdigest(), execution='complete',
            coverage_ref=typed(dict(admission_id=admission['admission_id'], view='coverage', scope=scope,
                                   qualification_digest=b['qualification_digest'], absence='Unknown')))
