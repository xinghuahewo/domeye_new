"""人工Country M3正式准入：固定实际C3/C4、完整依赖和单目标40/50锁。"""
from contextlib import contextmanager, ExitStack
from collections import Counter
from dataclasses import dataclass, field, asdict, replace
from pathlib import Path
import hashlib
import json
import resource
import shutil
import subprocess
import sys
import uuid

from psycopg2.extras import Json
from data_pipeline.bgp.archive import admission as upstream
from data_pipeline.bgp.archive.value_codec import ADMISSION_FIELDS, PHYSICAL_FIELDS, VALIDATOR_FIELDS, ENTITY_FIELDS, LOCK_FIELDS, REQUEST_FIELDS, fields, digest
from data_pipeline.analysis.country_events.snapshot_schema import encode as typed, decode as untyped
from data_pipeline.analysis.country_events.selection_contract import contract_json, contract_value, FileEntity
from data_pipeline.analysis.country_events.qualified_index import hold_component, inspect_qualified_country_result, M3Descriptor
from data_pipeline.analysis.country_events.qualified_reader import CODEC, VIEWS, ResultLimits, ResultReader, request_scope
from data_pipeline.analysis.country_events.route_inputs import M3Inputs
from data_pipeline.analysis.country_events.admission_sources import load_source
from data_pipeline.analysis.country_events.selection_admission import _record
from data_pipeline.analysis.country_events.result_catalog import catalog
from data_pipeline.analysis.country_events.snapshot_store import cleanup

REGISTRY = 'country_components.publication_admissions'
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
    limits: ResultLimits | None = None
    max_source_rows: int | None = None
    max_source_bytes: int | None = None
    lock_timeout_ms: int | None = None
    min_free_bytes: int | None = None
    max_temp_bytes: int | None = None
    memory_bytes: int | None = None
    execution_profile: str | None = None
    expected_country_binding: dict | None = None
    _read_stats: object = field(default_factory=Counter, init=False, repr=False)

    def __post_init__(self):
        if type(self.fixture_only) is not bool: raise ValueError('Country人工模式必须为显式bool')
        if self.fixture_only:
            if self.execution_profile is not None or self.expected_country_binding is not None:
                raise ValueError('Country人工与真实候选参数不得混用')
            self._profile='synthetic-fixture/v1'
        elif self.execution_profile=='real-candidate/v1':
            if type(self.expected_country_binding) is not dict:
                raise ValueError('Country真实候选缺完整C3/C4结果绑定')
            self.expected_country_binding=untyped(typed(self.expected_country_binding))
            self._profile=self.execution_profile
        else: raise ValueError('Country必须显式选择fixture或real-candidate/v1')
        self._mode=(self.fixture_only,self.execution_profile)
        defaults=dict(limits=ResultLimits(),max_source_rows=1000000,max_source_bytes=256*1024**2,
                      lock_timeout_ms=2000,min_free_bytes=128*1024**2,max_temp_bytes=512*1024**2,memory_bytes=256*1024**2)
        for name,value in defaults.items():
            if getattr(self,name) is None:
                if not self.fixture_only: raise ValueError('Country真实候选缺显式资源参数:'+name)
                setattr(self,name,value)
        original_roots=tuple(Path(p) for p in self.allowed_roots)
        self.allowed_roots=tuple(p.resolve(strict=True) for p in original_roots)
        if not self.allowed_roots or type(self.dsn) is not str or not self.dsn.strip():
            raise ValueError('Country必须显式指定DSN和允许根')
        for path in original_roots:self.path(path)
        from psycopg2.extensions import parse_dsn
        parsed=parse_dsn(self.dsn)
        if not parsed.get('dbname') or not parsed.get('host') or parsed.get('service'):
            raise ValueError('Country DSN必须显式主机与数据库')
        if self.fixture_only:
            if not parsed['host'].startswith('/'):raise ValueError('Country fixture仅允许本地隔离socket')
            self.path(Path(parsed['host']))
        self.output_root=self.path(self.output_root);self.scratch_root=self.path(self.scratch_root)
        if not self.output_root.is_dir() or not self.scratch_root.is_dir():
            raise ValueError('Country输出与scratch必须为既有目录')
        self._directory_identities=tuple((str(p),p.stat().st_dev,p.stat().st_ino)
                                         for p in (*self.allowed_roots,self.output_root,self.scratch_root))
        self._scope_digest=self.scope_digest()
        self.separate_scratch(self.output_root)
        self.resource_guard()
        if not self.fixture_only:_parts(self,self.expected_country_binding)

    path = upstream.Runtime.path
    event = upstream.Runtime.event

    def scope_digest(self):
        return digest(dict(dsn=self.dsn,roots=[str(p) for p in self.allowed_roots],output=str(self.output_root),
            scratch=str(self.scratch_root),binding=self.expected_country_binding,
            dependencies=[a['admission_id'] for a in self.dependency_admissions],
            runtime_ids=sorted((k,id(v)) for k,v in self.dependency_runtimes.items()),
            limits=asdict(self.limits),max_source_rows=self.max_source_rows,max_source_bytes=self.max_source_bytes,
            lock_timeout_ms=self.lock_timeout_ms,min_free_bytes=self.min_free_bytes,max_temp_bytes=self.max_temp_bytes,memory_bytes=self.memory_bytes))

    def separate_scratch(self,root):
        if self.fixture_only:return
        root=self.path(root)
        if root==self.scratch_root or root in self.scratch_root.parents or self.scratch_root in root.parents:
            raise ValueError('Country scratch与原输入/制品未隔离')

    def check_binding(self,binding):
        self.resource_guard()
        if not self.fixture_only and binding!=self.expected_country_binding:
            raise ValueError('Country真实候选完整绑定越界')

    def check_dependencies(self):
        self.resource_guard()
        if set(self.dependency_runtimes)!={a['admission_id'] for a in self.dependency_admissions}:
            raise ValueError('Country依赖Runtime集合不符')
        for a in self.dependency_admissions:
            dependency=self.dependency_runtimes[a['admission_id']]
            if (dependency.fixture_only,getattr(dependency,'execution_profile',None))!=self._mode:
                raise ValueError('Country依赖模式不得混用')
            if not self.fixture_only:
                for entity in a['entities']:self.separate_scratch(Path(entity['path']).parent)

    def resource_guard(self):
        if type(self.fixture_only) is not bool or (self.fixture_only,self.execution_profile)!=self._mode:
            raise ValueError('Country Runtime模式漂移')
        for name in ('max_source_rows','max_source_bytes','lock_timeout_ms','min_free_bytes','max_temp_bytes','memory_bytes'):
            if type(getattr(self,name)) is not int or getattr(self,name)<1:
                raise ValueError('Country有限预算无效:'+name)
        if not isinstance(self.limits,ResultLimits):raise ValueError('Country结果预算类型无效')
        if any(type(v) is not int or v<1 for v in asdict(self.limits).values()):
            raise ValueError('Country结果预算必须为正整数')
        if self.scope_digest()!=self._scope_digest:raise ValueError('Country Runtime固定范围或资源漂移')
        for name,device,inode in self._directory_identities:
            path=self.path(Path(name));st=path.stat()
            if not path.is_dir() or (st.st_dev,st.st_ino)!=(device,inode):raise ValueError('Country目录身份漂移')
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        if rss>self.limits.max_rss_bytes:raise ValueError('resource_limit:Country_RSS')
        if any(shutil.disk_usage(p).free<self.min_free_bytes for p in (self.scratch_root,self.output_root)):
            raise ValueError('resource_limit:Country_free_disk')
        if sum(p.stat().st_size for p in self.scratch_root.rglob('*') if p.is_file())>self.max_temp_bytes:
            raise ValueError('resource_limit:Country_temp_disk')

    def lake_connect(self,dsn):
        self.resource_guard()
        if dsn!=self.dsn:raise ValueError('Country查询连接DSN越界')
        from data_pipeline.analysis.country_events.snapshot_store import lake_connect
        db=lake_connect(dsn,memory_bytes=self.memory_bytes,scratch_root=self.scratch_root,max_temp_bytes=self.max_temp_bytes)
        try:
            settings=db.execute("SELECT current_setting('memory_limit'),current_setting('temp_directory'),current_setting('max_temp_directory_size')").fetchone()
            self.event('country_query_connection',settings=settings)
            return db
        except BaseException:
            db.close();raise


def _parts(runtime, binding):
    runtime.check_binding(binding)
    fields(binding, {'read_binding', 'proof'})
    read = contract_value(binding['read_binding']); proof = contract_value(binding['proof'])
    if runtime.path(Path(read.root)) != runtime.output_root:
        raise ValueError('Country结果根串绑')
    runtime.path(Path(read.component.root))
    runtime.separate_scratch(Path(read.component.root))
    return read, proof


def inspect_binding(runtime, read_binding, proof):
    result = dict(read_binding=contract_json(read_binding), proof=contract_json(proof))
    with _context(runtime, result, lambda: None): pass
    return result


def _inputs(runtime, guard):
    runtime.check_dependencies()
    return M3Inputs(runtime.dependency_admissions, runtime.dependency_runtimes,
                    max_rows=runtime.max_source_rows, max_bytes=runtime.max_source_bytes,
                    batch_rows=runtime.limits.batch_rows, batch_bytes=runtime.limits.batch_bytes,
                    guard=lambda: (runtime.resource_guard(), guard()))


@contextmanager
def _context(runtime, binding, guard):
    runtime.resource_guard(); guard(); read, proof = _parts(runtime, binding)
    inputs = _inputs(runtime, guard)
    with inputs.locked():
        with hold_component(runtime.dsn, read.component):
            source = load_source(runtime, read.component, inputs)
            primary=None
            try:
                source.query_runtime = runtime
                runtime.event('country_index_full_validation',operation='首次准入或显式inspect')
                descriptor = inspect_qualified_country_result(runtime.dsn, read, proof, source=source, limits=runtime.limits)
                yield source, descriptor
                source.check(); guard()
            except BaseException as error:primary=error;raise
            finally:cleanup((source.close,),primary)


def _rules(runtime):
    paths = sorted(Path(__file__).parent.glob('*.py'))
    code = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    from data_pipeline.common.admission_locks import code_sha as lock_code_sha
    code['lock_connection_code']=lock_code_sha()
    return dict(version='country-publication-m3/v1', code_sha256=digest(code),
                typed_schema='country-component-m3/v1', typed_codec=CODEC,
                rules_digest=digest(dict(execution_profile=runtime._profile, views=VIEWS, source='retained-full-public-reads/v1', stages=[40,50])))


def _physical(read):
    b = read.component
    return dict(system_identifier=b.system_id, database_oid=b.database_oid, catalog=b.catalog_id,
                schema=b.schema_name, root=b.root, snapshot=b.snapshot)


def _target(physical, stage, namespace, key):
    return dict(stage=stage, system_identifier=physical['system_identifier'],
                database_oid=physical['database_oid'], namespace=namespace, key=key)


def _shape(a):
    fields(a, ADMISSION_FIELDS); fields(a['physical'], PHYSICAL_FIELDS); fields(a['validator'], VALIDATOR_FIELDS)
    if (a['contract'] != 'component-publication-admission/v1' or a['owner'] != 'country'
            or a['binding_codec'] != CODEC or a['admission_id'] != digest({k:v for k,v in a.items() if k != 'admission_id'})):
        raise ValueError('Country正式Admission合同不符')
    for entity in a['entities']: fields(entity, ENTITY_FIELDS)
    for target in a['lock_targets']: fields(target, LOCK_FIELDS)
    b = untyped(a['owner_binding']); read = contract_value(b['read_binding'])
    t = a['lock_targets']; p = a['physical']
    if (p != _physical(read) or len(t) != 3
            or t[0] != _target(p,40,'country.component',read.component.component_id)
            or t[1] != _target(p,50,'country.read_model',read.read_model_id)
            or t[2] != _target(p,50,'country.admission',t[2]['key'])):
        raise ValueError('Country阶段40/50单目标集合不符')
    uuid.UUID(t[2]['key'])
    if a['dependencies'] != sorted(set(a['dependencies'])): raise ValueError('Country依赖重复或无序')


def _anchor(runtime, a):
    with upstream._pg(runtime) as pg, pg.cursor() as cur:
        row = upstream._sql(runtime,cur,f'SELECT state,admission,audit FROM {REGISTRY} WHERE record_key=%s',
                            (a['lock_targets'][2]['key'],)).fetchone()
    if row is None or row[:2] != ('accepted',a): raise ValueError('Country实际正式登记缺失或撤销')
    if digest(row[2]) != a['validator']['validation_digest']: raise ValueError('Country实际验收摘要不符')
    return row[2]


def verify_current(runtime, admission, *, guard):
    runtime.resource_guard(); guard(); _shape(admission); audit=_anchor(runtime,admission)
    if {k:v for k,v in admission['validator'].items() if k != 'validation_digest'} != _rules(runtime):
        raise ValueError('Country当前验证器已变化')
    if sorted(a['admission_id'] for a in runtime.dependency_admissions) != admission['dependencies']:
        raise ValueError('Country当前完整依赖已变化')
    upstream._entities_current(runtime, admission['entities'], guard)
    read,proof=_parts(runtime,untyped(admission['owner_binding']))
    if digest(typed(catalog(runtime,read,proof,guard)))!=audit['catalog_digest']:
        raise ValueError('Country当前实际目录元数据漂移')
    _inputs(runtime,guard).verify()
    runtime.event('country_current_complete',body_scans=0,index_scans=0,source_loads=0,locks=0)
    return admission


def _locked(runtime, cur, a, target):
    b = untyped(a['owner_binding']); read, proof = _parts(runtime,b)
    actual = upstream._sql(runtime,cur,'SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()').fetchone()
    if actual != (read.component.system_id,read.component.database_oid): raise ValueError('Country锁物理身份不符')
    if target['namespace'] == 'country.component':
        row = upstream._sql(runtime,cur,'SELECT schema_name,state,snapshot,root,manifest_sha256 FROM country_components.components WHERE component_id=%s FOR SHARE',(target['key'],)).fetchone()
        c = read.component; expected = (c.schema_name,'complete',c.snapshot,c.root,c.manifest_sha256)
    elif target['namespace'] == 'country.read_model':
        row = upstream._sql(runtime,cur,'SELECT admission_version,component_id,binding_json,proof_json,manifest_sha256,validation_sha256,state FROM country_components.read_admissions WHERE read_model_id=%s FOR SHARE',(target['key'],)).fetchone()
        expected = _record(read,proof)
    else:
        row = upstream._sql(runtime,cur,f'SELECT state,admission FROM {REGISTRY} WHERE record_key=%s FOR SHARE',(target['key'],)).fetchone()
        expected = ('accepted',a)
    if row != expected: raise ValueError('Country单目标锁后原绑定不符')


@contextmanager
def hold_lock(runtime, admission, lock_target, *, guard, lock_connection=None):
    from data_pipeline.common.admission_locks import validated_connection
    runtime.resource_guard(); guard(); _shape(admission); _anchor(runtime,admission)
    if {k:v for k,v in admission['validator'].items() if k!='validation_digest'}!=_rules(runtime):
        raise ValueError('Country当前验证器或模式已变化')
    runtime.check_dependencies()
    if lock_target not in admission['lock_targets']: raise ValueError('Country锁目标越界')
    pg = upstream.psycopg2.connect(runtime.dsn) if lock_connection is None else validated_connection(runtime,admission,lock_target,lock_connection); primary = None
    try:
        with pg.cursor() as cur:
            upstream._sql(runtime,cur,"SELECT set_config('lock_timeout',%s,true)",(str(runtime.lock_timeout_ms)+'ms',))
            _locked(runtime,cur,admission,lock_target)
        yield
    except BaseException as error: primary=error; raise
    finally:
        if lock_connection is None:upstream._release(pg,primary)


def admit(runtime, owner_binding, *, guard):
    b = untyped(typed(owner_binding)); rules = _rules(runtime)
    revision = subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    for path in [*sorted(Path(__file__).parent.glob('*.py')), ROOT/'backend/data_pipeline/common/admission_locks.py']:
        try:
            committed = subprocess.check_output(['git','show',revision+':'+str(path.relative_to(ROOT))],cwd=ROOT, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as exc:
            raise ValueError('Country正式准入必须使用已固定完整提交') from exc
        if committed != path.read_bytes(): raise ValueError('Country正式准入必须使用已固定完整提交')
    deps = sorted(a['admission_id'] for a in runtime.dependency_admissions)
    reuse = digest(dict(binding=b,validator=rules,dependencies=deps))
    with upstream._pg(runtime,write=True) as pg, pg.cursor() as cur:
        upstream._sql(runtime,cur,f'''CREATE TABLE IF NOT EXISTS {REGISTRY} (
            record_key UUID PRIMARY KEY,reuse_key TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('accepted','revoked')),
            admission JSONB NOT NULL,audit JSONB NOT NULL)''')
        found = upstream._sql(runtime,cur,f'SELECT admission FROM {REGISTRY} WHERE reuse_key=%s AND state=%s',(reuse,'accepted')).fetchall()
    for row in found:
        try: return verify_current(runtime,row[0],guard=guard)
        except ValueError: continue
    with _context(runtime,b,guard) as (source,descriptor):
        read = descriptor.binding; p = _physical(read); key = str(uuid.uuid4())
        from data_pipeline.analysis.country_events.qualified_store import M3ComponentReader
        from data_pipeline.analysis.country_events.snapshot_reader import ReadReceipt
        complete=None
        runtime.event('country_c3_full_validation_start')
        reader=M3ComponentReader(runtime.dsn,read.component,source=source,guard=runtime.resource_guard)
        stream=reader.stream()
        try:
            for part in stream:
                if isinstance(part,ReadReceipt):complete=part
        finally:stream.close()
        if complete is None or not complete.full_body_validated or complete.rows!=descriptor.proof.source_rows:
            raise ValueError('Country首次准入缺当前完整C3资格审计')
        runtime.event('country_c3_full_validation_complete',rows=complete.rows)
        entities = [upstream._entity(runtime,e.path,hash_body=True,guard=guard) for e in descriptor.entities]
        audit = dict(proof=b['proof'],logical_run_id=descriptor.logical_run_id,input_binding_id=descriptor.input_binding_id,
                     window_us=list(descriptor.window_us),event_count=descriptor.event_count,
                     admitted_empty=descriptor.admitted_empty,complete_source_reads=len(source.receipts),
                     descriptor_entities=[asdict(e) for e in descriptor.entities],
                     full_c3_receipt=typed(asdict(complete)),
                     catalog_digest=digest(typed(catalog(runtime,read,descriptor.proof,guard))))
        a = dict(contract='component-publication-admission/v1',owner='country',owner_revision=revision,
            owner_binding=typed(b),binding_codec=CODEC,physical=p,validator=dict(rules,validation_digest=digest(audit)),
            inventory_digest=digest(dict(component=asdict(read.component),proof=b['proof'])),
            entities=sorted(entities,key=lambda e:e['path']),dependencies=deps,
            lock_targets=[_target(p,40,'country.component',read.component.component_id),
                          _target(p,50,'country.read_model',read.read_model_id),_target(p,50,'country.admission',key)])
        a['admission_id']=digest(a); _shape(a)
        upstream._entities_current(runtime,a['entities'],guard)
        if _rules(runtime)!=rules: raise ValueError('Country准入期间代码变化')
        with upstream._pg(runtime,write=True) as pg, pg.cursor() as cur:
            upstream._sql(runtime,cur,"SELECT set_config('lock_timeout',%s,true)",(str(runtime.lock_timeout_ms)+'ms',))
            upstream._sql(runtime,cur,'SELECT pg_advisory_xact_lock(%s)',(int(reuse[:15],16),))
            for target in a['lock_targets'][:2]: _locked(runtime,cur,a,target)
            upstream._sql(runtime,cur,f'INSERT INTO {REGISTRY} VALUES (%s,%s,%s,%s,%s)',(key,reuse,'accepted',Json(a),Json(audit)))
    runtime.event('admit_complete',audit=audit)
    return a


@dataclass
class ReadSession:
    iterator: object = None
    receipt: object = None
    exhausted: bool = False
    def __iter__(self): return self
    def __next__(self): return next(self.iterator)
    def close(self): self.iterator.close()


class _AdmittedReadGuard:
    """只在实际当前Admission核验后使用；正文读取不装载原科学输入。"""
    def __init__(self,runtime,guard):self.query_runtime=runtime;self.guard=guard
    def check(self):self.query_runtime.resource_guard();self.guard()


def _descriptor(runtime,admission):
    audit=_anchor(runtime,admission);read,proof=_parts(runtime,untyped(admission['owner_binding']))
    return M3Descriptor(read,proof,tuple(FileEntity(**e) for e in audit['descriptor_entities']),
        audit['logical_run_id'],audit['input_binding_id'],tuple(audit['window_us']),
        audit['event_count'],audit['admitted_empty'])


@contextmanager
def open_reader(runtime, admission, request, *, guard):
    fields(request,REQUEST_FIELDS)
    if (request['codec_version']!=CODEC or type(request['batch_rows']) is not int
            or not 1<=request['batch_rows']<=runtime.limits.batch_rows
            or type(request['batch_bytes']) is not int or not 1<=request['batch_bytes']<=runtime.limits.batch_bytes):
        raise ValueError('Country公开读codec或批预算无效')
    scope = untyped(request['scope_typed'])
    fields(scope,{'window_us','dimension','incident_id','revision','table','after_sequence','stop_sequence'})
    verify_current(runtime,admission,guard=guard);session=ReadSession()
    descriptor=_descriptor(runtime,admission)
    selected=request_scope(descriptor,request['view'],**scope)
    limits=replace(runtime.limits,batch_rows=request['batch_rows'],batch_bytes=request['batch_bytes'])
    reader=ResultReader(runtime.dsn,descriptor,_AdmittedReadGuard(runtime,guard),limits,runtime._read_stats)
    hasher=hashlib.sha256();count=0
    def batches():
        nonlocal count
        pending=[]
        for row in reader.rows(selected):
            runtime.resource_guard();guard();encoded=typed(row).encode()
            if len(typed((row,)).encode())>request['batch_bytes']:raise ValueError('resource_limit:Country_public_row')
            if pending and (len(pending)>=request['batch_rows'] or len(typed(tuple([*pending,row])).encode())>request['batch_bytes']):
                text=typed(tuple(pending));yield dict(rows_typed=text,codec_version=CODEC,rows=len(pending),bytes=len(text.encode()));pending=[]
            pending.append(row);count+=1;hasher.update(len(encoded).to_bytes(8,'big'));hasher.update(encoded)
        if pending:
            text=typed(tuple(pending));yield dict(rows_typed=text,codec_version=CODEC,rows=len(pending),bytes=len(text.encode()))
        session.exhausted=True
    session.iterator=batches();primary=None
    try:yield session
    except BaseException as error:primary=error;raise
    finally:cleanup((session.iterator.close,reader.close),primary)
    if session.exhausted:
        verify_current(runtime,admission,guard=guard)
        session.receipt=dict(contract='component-publication-read/v1',admission_id=admission['admission_id'],
            request_digest=digest(request),rows=count,typed_digest=hasher.hexdigest(),
            execution='complete',coverage_ref=typed(dict(view=request['view'],scope=scope,
                result_id=descriptor.binding.result_id,read_model_id=descriptor.binding.read_model_id,
                event_count=descriptor.event_count,admitted_empty=descriptor.admitted_empty,
                full_component_receipt=False,dimensions=12)))
        runtime.event('country_public_read_complete',view=request['view'],rows=count,
                      source_loads=0,index_full_scans=0,locks=0,costs=dict(reader.stats))
