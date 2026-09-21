"""仅M2原观察和参考事实的离线可信准入、当前核验、单目标锁与有界读取。"""
from contextlib import contextmanager
from dataclasses import dataclass,field
import hashlib
from importlib.metadata import version
import platform
import os
import json
import re
import resource
import shutil
import sys
from pathlib import Path
import subprocess
import uuid

import psycopg2
from psycopg2.extras import Json

from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.ordered_reader import binding_from_reader
from data_pipeline.bgp.record_types import metadata_json
from data_pipeline.bgp.archive.selection import Selection
from data_pipeline.bgp.archive.checkpoint import OBS_TABLES, sha
from data_pipeline.bgp.archive.value_codec import CODEC, CONTRACT, canonical, digest, typed, untyped, fields, admission_shape, REQUEST_FIELDS

TABLES={'m2.admission':'observation_publication.m2_admissions','reference.admission':'observation_publication.reference_admissions'}
BINDING_FIELDS={'owner','run_id','snapshot','ordered_source_ids','profile','plan','seal','input_binding','input_binding_id','selected_sources','reference_sources'}
REFERENCE_FIELDS={'owner','m2_admission_id','m2_binding','source_id','checkpoint_ordinal','fields','interpretation','selected_sources'}
ROOT=Path(__file__).resolve().parents[4]


@dataclass
class Runtime:
    dsn:str
    allowed_roots:tuple
    scratch_root:Path
    fixture_only:bool=False
    dependency_admissions:tuple=()
    audit_sink:object=None
    memory_limit:str|None=None
    max_temp_bytes:int|None=None
    lock_timeout_ms:int|None=None
    execution_profile:str|None=None
    input_manifest:dict|None=None
    expected_m2_binding:dict|None=None
    max_rss_bytes:int|None=None
    min_free_bytes:int|None=None

    def __post_init__(self):
        if type(self.fixture_only) is not bool:raise ValueError('人工模式必须为显式bool')
        if self.fixture_only:
            if self.execution_profile is not None or self.input_manifest is not None or self.expected_m2_binding is not None:
                raise ValueError('人工与真实候选参数不得混用')
            self._profile='synthetic-fixture/v1'
        elif self.execution_profile=='real-candidate/v1':
            if type(self.input_manifest) is not dict or type(self.expected_m2_binding) is not dict:
                raise ValueError('真实候选必须绑定原manifest与完整M2绑定')
            self._profile=self.execution_profile
        else:raise ValueError('必须显式选择人工或real-candidate/v1')
        self._mode=(self.fixture_only,self.execution_profile)
        defaults=dict(memory_limit='256MB',max_temp_bytes=512*1024**2,lock_timeout_ms=2000,
                      max_rss_bytes=4*1024**3,min_free_bytes=128*1024**2)
        for name,value in defaults.items():
            if getattr(self,name) is None:
                if not self.fixture_only:raise ValueError('真实候选缺少显式资源参数：'+name)
                setattr(self,name,value)
        self._checked_limits()
        roots=tuple(Path(p) for p in self.allowed_roots)
        if any(not p.is_absolute() for p in roots):raise ValueError('允许根必须为绝对路径')
        self.allowed_roots=tuple(p.resolve(strict=True) for p in roots)
        if not self.allowed_roots:raise ValueError('允许根不能为空')
        self._path_bindings={}
        self._root_bindings=tuple((p,self._path_identity(p)) for p in roots)
        for root in roots:self.path(root)
        if not self.fixture_only:
            self.path(self.scratch_root)
        self.scratch_root=Path(self.scratch_root).resolve(strict=True)
        self.path(self.scratch_root)
        if not self.scratch_root.is_dir():raise ValueError('scratch必须为既有隔离目录')
        self.resource_usage=dict(checks=0,peak_rss_bytes=0,min_free_bytes=None,
                                 rss_scope='whole_process_lifetime_peak_excluding_pg')
        if not self.fixture_only:
            # 复制调用方已选范围；这里不授予可信资格，后续仍走实际PG/Selection与完整验收。
            self.input_manifest=untyped(typed(self.input_manifest))
            self.expected_m2_binding=untyped(typed(self.expected_m2_binding))
            fields(self.expected_m2_binding,BINDING_FIELDS)
            if self.expected_m2_binding['owner']!='m2' or self.expected_m2_binding['profile']!='observation':
                raise ValueError('真实候选只接受固定M2 observation绑定')
            self._manifest_scope()
            self._scope_digest=digest([self.input_manifest,self.expected_m2_binding])

    def _checked_limits(self):
        # Runtime允许有限预算调整；每次使用读取并验证同一份值，NaN/bool等不能绕过比较。
        values={name:getattr(self,name) for name in ('memory_limit','max_temp_bytes','lock_timeout_ms','max_rss_bytes','min_free_bytes')}
        if (type(values['memory_limit']) is not str or not re.fullmatch(r'[1-9][0-9]*(?:\.[0-9]+)?\s*(?:B|KB|MB|GB|TB)',values['memory_limit'],re.I)
                or any(type(value) is not int or value<1 for name,value in values.items() if name!='memory_limit')):
            raise ValueError('资源参数必须为有限正值')
        return values

    def _manifest_scope(self):
        from datetime import datetime
        from jsonschema import Draft202012Validator,FormatChecker
        from data_pipeline.bgp.replay.run_from_files import normalized_inputs
        from data_pipeline.bgp.input.reference_reader import resolved_interpretation
        manifest=self.input_manifest;bound=self.expected_m2_binding
        schema=json.loads((ROOT/'contracts/data/observation-run.schema.json').read_text())
        Draft202012Validator(schema,format_checker=FormatChecker()).validate(manifest)
        if datetime.fromisoformat(manifest['window_start'].replace('Z','+00:00'))>=datetime.fromisoformat(manifest['window_end_exclusive'].replace('Z','+00:00')):
            raise ValueError('manifest时间窗无效')
        normalized={**manifest,'inputs':normalized_inputs(manifest)}
        if normalized!=bound['plan']['manifest']:raise ValueError('manifest与所选原计划不符')
        refs=[];seen={}
        for entry in [*manifest['inputs'],*manifest.get('references',[])]:
            self.resource_guard();self.path(entry['path'])
        for entry in manifest.get('references',[]):
            interpretation=resolved_interpretation(entry['path'])
            if entry['sha256'] in seen:
                if seen[entry['sha256']]!=interpretation:raise ValueError('参考别名解释冲突')
                continue
            seen[entry['sha256']]=interpretation
            refs.append({**entry,'format':interpretation,'role':'reference','source_id':entry['sha256']})
        if [*refs,*normalized['inputs']]!=bound['plan']['inputs']:
            raise ValueError('manifest来源/参考解释与原输入链不符')
        protected=[Path(e['path']).parent for e in bound['plan']['inputs']]
        protected.extend(Path(f['path']).parent for cp in bound['seal']['checkpoints'] for f in cp['files'])
        for root in protected:self._separate_scratch(root)

    def _separate_scratch(self,root):
        root=self.path(root)
        if root==self.scratch_root or root in self.scratch_root.parents or self.scratch_root in root.parents:
            raise ValueError('scratch与原件/固定制品目录未隔离')

    def check_binding(self,binding):
        self.resource_guard()
        if (self.fixture_only,self.execution_profile)!=self._mode:raise ValueError('Runtime模式漂移')
        if not self.fixture_only:
            if digest([self.input_manifest,self.expected_m2_binding])!=self._scope_digest:
                raise ValueError('Runtime固定范围漂移')
            if binding.get('m2_binding',binding)!=self.expected_m2_binding:
                raise ValueError('绑定超出真实候选固定M2范围')

    def resource_guard(self):
        limits=self._checked_limits()
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        free=shutil.disk_usage(self.scratch_root).free
        usage=self.resource_usage;usage['checks']+=1
        usage['peak_rss_bytes']=max(usage['peak_rss_bytes'],rss)
        usage['min_free_bytes']=free if usage['min_free_bytes'] is None else min(usage['min_free_bytes'],free)
        if rss>limits['max_rss_bytes']:raise ValueError('P1 RSS资源保护')
        if free<limits['min_free_bytes']:raise ValueError('P1 最低空闲盘保护')

    def guarded(self,caller):
        def check():self.resource_guard();caller();self._checked_limits()
        return check

    @staticmethod
    def _path_identity(path):
        resolved=path.resolve(strict=True);s=resolved.stat()
        return resolved,s.st_dev,s.st_ino

    def path(self,path):
        p=Path(path)
        # Resource/Detection直接借用本方法，原合同仅要求allowed_roots；不隐式增加构造成员。
        if not isinstance(self,Runtime):
            resolved=p.resolve(strict=True)
            if str(p)!=str(resolved) or not any(resolved==r or r in resolved.parents for r in self.allowed_roots):
                raise ValueError('实体超出显式允许根或非规范路径')
            if any(parent.is_symlink() for parent in (p,*p.parents)):raise ValueError('禁止符号链接实体')
            return resolved
        if not p.is_absolute():raise ValueError('实体必须为绝对路径')
        if any(self._path_identity(root)!=identity for root,identity in self._root_bindings):
            raise ValueError('允许根实体或路径指向漂移')
        identity=self._path_identity(p);resolved=identity[0]
        if not any(resolved==r or r in resolved.parents for r in self.allowed_roots):raise ValueError('实体超出显式允许根')
        # 原声明不改写；只固定本次Runtime的解析结果，跨Runtime由Admission实体证据复核。
        if self._path_bindings.setdefault(str(p),identity)!=identity:raise ValueError('实体或路径指向漂移')
        return resolved

    def event(self,kind,**values):
        if self.audit_sink is not None:self.audit_sink(dict(kind=kind,**values))


@contextmanager
def _pg(runtime,*,write=False):
    runtime.resource_guard()
    pg=psycopg2.connect(runtime.dsn);primary=None
    try:
        pg.set_session(readonly=not write)
        yield pg
        if write:pg.commit()
    except BaseException as exc:primary=exc;raise
    finally:_release(pg,primary)


def _release(pg,primary):
    errors=[]
    for action in (pg.rollback,pg.close):
        try:action()
        except BaseException as exc:errors.append(exc)
    if errors:
        if primary is not None:primary.cleanup_errors=(*getattr(primary,'cleanup_errors',()),*errors)
        else:raise errors[0]


def _sql(runtime,c,query,args=()):
    runtime.resource_guard()
    runtime.event('pg_sql',sql=query)
    c.execute(query,args)
    runtime.resource_guard()
    return c


def _physical(runtime,binding):
    runtime.check_binding(binding)
    b=binding['m2_binding'] if binding['owner']=='reference' else binding
    with _pg(runtime) as pg,pg.cursor() as c:
        row=_sql(runtime,c,'SELECT system_identifier::text,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) FROM pg_control_system()').fetchone()
        found=_sql(runtime,c,'SELECT schema_name,data_path FROM observation_m2.runs WHERE run_id=%s',(b['run_id'],)).fetchone()
    if found is None or found[0]!=b['seal']['schema'] or found[0]!='m2_'+b['run_id']:raise ValueError('实际M2登记缺失或schema不符')
    # 直接 Parquet 读取未实现 column mapping；仅检查绑定 schema 在固定快照可见的文件。
    with _pg(runtime) as pg,pg.cursor() as c:
        mapped=_sql(runtime,c,'''SELECT f.data_file_id FROM public.ducklake_data_file f
            JOIN public.ducklake_table t USING(table_id)
            JOIN public.ducklake_schema s USING(schema_id)
            WHERE s.schema_name=%s AND t.table_name=ANY(%s)
              AND s.begin_snapshot<=%s AND (s.end_snapshot IS NULL OR s.end_snapshot>%s)
              AND t.begin_snapshot<=%s AND (t.end_snapshot IS NULL OR t.end_snapshot>%s)
              AND f.begin_snapshot<=%s AND (f.end_snapshot IS NULL OR f.end_snapshot>%s)
              AND f.mapping_id IS NOT NULL LIMIT 1''',
            (found[0],list(OBS_TABLES)+['seal_selection','path_owner'],*([b['snapshot']]*6))).fetchone()
        if mapped:raise ValueError('固定快照不支持 DuckLake column mapping')
    root=str(runtime.path(found[1]))
    if not runtime.fixture_only:runtime._separate_scratch(root)
    return dict(system_identifier=row[0],database_oid=row[1],catalog='lake',schema=found[0],root=root,snapshot=b['snapshot'])


def inspect_binding(runtime,run_id,snapshot,ordered_source_ids):
    runtime.resource_guard()
    if not runtime.fixture_only:
        b=runtime.expected_m2_binding
        if (run_id,snapshot,list(ordered_source_ids))!=(b['run_id'],b['snapshot'],b['ordered_source_ids']):raise ValueError('inspect超出固定M2范围')
    reader=ObservationReader(runtime.dsn,run_id,snapshot,ordered_source_ids,profile='observation',guard=runtime.resource_guard)
    binding=binding_from_reader(reader);s=reader.selection
    refs=[dict(source_id=e['source_id'],checkpoint_ordinal=cp['ordinal'],interpretation=e['format'])
          for e,cp in zip(s.plan['inputs'],s.checkpoints) if e['role']=='reference']
    ranks={source.source_id:i for i,source in enumerate(binding.sources)}
    result=dict(owner='m2',run_id=run_id,snapshot=snapshot,ordered_source_ids=list(reader.sources),profile='observation',
        plan=s.plan,seal=s.seal,input_binding=metadata_json(binding),input_binding_id=binding.binding_id,
        selected_sources=[dict(source_id=source,upstream_rank=ranks[source],calculation_role=binding.sources[ranks[source]].role) for source in reader.sources],
        reference_sources=refs)
    runtime.check_binding(result)
    return result


def reference_binding(runtime,m2_admission,source_id):
    admission_shape(m2_admission)
    if m2_admission['owner']!='m2':raise ValueError('参考依赖必须为M2')
    b=untyped(m2_admission['owner_binding']);runtime.check_binding(b)
    if {k:v for k,v in m2_admission['validator'].items() if k!='validation_digest'}!=_rules(runtime):raise ValueError('参考依赖规则/模式不符')
    found=[x for x in b['reference_sources'] if x['source_id']==source_id]
    if len(found)!=1:raise ValueError('参考来源未被M2声明')
    return dict(owner='reference',m2_admission_id=m2_admission['admission_id'],m2_binding=b,source_id=source_id,
        checkpoint_ordinal=found[0]['checkpoint_ordinal'],fields=[n for n,_ in OBS_TABLES['references']],
        interpretation=found[0]['interpretation'],selected_sources=[])


def _binding(runtime,b):
    if type(b) is not dict or b.get('owner') not in ('m2','reference'):raise ValueError('只允许M2/reference绑定')
    runtime.check_binding(b)
    if b['owner']=='m2':
        fields(b,BINDING_FIELDS)
        actual=inspect_binding(runtime,b['run_id'],b['snapshot'],b['ordered_source_ids'])
    else:
        fields(b,REFERENCE_FIELDS)
        deps=[a for a in runtime.dependency_admissions if a['admission_id']==b['m2_admission_id']]
        if len(deps)!=1:raise ValueError('缺少显式M2 Admission依赖')
        actual=reference_binding(runtime,deps[0],b['source_id'])
    if actual!=b:raise ValueError('完整原绑定/来源rank/参考选择不符')
    return b['m2_binding'] if b['owner']=='reference' else b


def _rules(runtime=None):
    files=('bgp/archive/admission.py','bgp/archive/value_codec.py','bgp/archive/validation.py','bgp/archive/checkpoint.py','bgp/archive/selection.py','bgp/archive/message_reader.py','bgp/ordered_reader.py','bgp/record_types.py','bgp/archive/store.py','bgp/replay/run_from_files.py','bgp/input/mrt_reader.py','bgp/input/reference_reader.py')
    code={name:hashlib.sha256((Path(__file__).parents[2]/name).read_bytes()).hexdigest() for name in files}
    code['observation_manifest_schema']=hashlib.sha256((ROOT/'contracts/data/observation-run.schema.json').read_bytes()).hexdigest()
    code['runtime']=dict(python=platform.python_version(),packages={name:version(name) for name in ('duckdb','pyarrow','psycopg2-binary','jsonschema')})
    from data_pipeline.common.admission_locks import code_sha as lock_code_sha
    code['lock_connection_code']=lock_code_sha()
    return dict(version='m2-reference-admission/v1',code_sha256=digest(code),typed_schema='observation-checkpoint/v1',typed_codec=CODEC,
        rules_digest=digest(dict(execution_profile=runtime._profile if runtime is not None else 'synthetic-fixture/v1',tables=OBS_TABLES,fk='m2-complete-cp-fk/v1',entities='immutable-custody-source-path-stat/v2',ducklake_format='0.3',extensions={'ducklake':'3f1b372','postgres_scanner':'b9fce43'})))


def _ducklake_root(runtime,db,expected):
    query="SELECT option_name,value,scope FROM ducklake_options('lake') WHERE option_name IN ('data_path','version')"
    runtime.event('duckdb_metadata',sql=query)
    rows=db.execute(query).fetchall()
    versions=dict(db.execute('SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded').fetchall())
    if versions.get('ducklake')!='3f1b372' or versions.get('postgres_scanner')!='b9fce43':raise ValueError('读取扩展版本不符')
    paths=[value for name,value,scope in rows if name=='data_path' and scope=='GLOBAL']
    if len(paths)!=1 or str(runtime.path(paths[0]))!=expected or ('version','0.3','GLOBAL') not in rows:raise ValueError('实际DuckLake根/格式不符')


def _entity(runtime,path,*,hash_body=False,guard,preserve_source_path=False):
    guard();p=runtime.path(path);s=p.stat()
    if not preserve_source_path and str(p)!=str(path):raise ValueError('旧实体合同要求规范路径')
    if not p.is_file():raise ValueError('不是普通制品文件')
    result=dict(path=str(p),device=s.st_dev,inode=s.st_ino,size=s.st_size,mtime_ns=s.st_mtime_ns,ctime_ns=s.st_ctime_ns)
    if preserve_source_path:result['source_path']=str(path)
    if hash_body:
        h=hashlib.sha256()
        with p.open('rb') as f:
            for chunk in iter(lambda:f.read(1024**2),b''):
                guard();h.update(chunk);runtime.event('entity_hash',bytes=len(chunk))
            os.fsync(f.fileno())
        result['sha256']=h.hexdigest()
        if {k:v for k,v in result.items() if k!='sha256'}!=_entity(runtime,path,guard=guard,preserve_source_path=preserve_source_path):raise ValueError('hash期间实体漂移')
    return result


def _entities_current(runtime,entities,guard):
    for e in entities:
        if _entity(runtime,e.get('source_path',e['path']),guard=guard,preserve_source_path='source_path' in e)!={k:v for k,v in e.items() if k!='sha256'}:raise ValueError('固定实体发生漂移')


def _target(physical,namespace,key):
    return dict(stage=10,system_identifier=physical['system_identifier'],database_oid=physical['database_oid'],namespace=namespace,key=key)


def _source_targets(physical,b):
    return [_target(physical,'m2.run',b['run_id']),*[_target(physical,'m2.checkpoint',canonical([b['run_id'],cp['ordinal']])) for cp in b['seal']['checkpoints']]]


def _setup(runtime):
    with _pg(runtime,write=True) as pg,pg.cursor() as c:
        _sql(runtime,c,'CREATE SCHEMA IF NOT EXISTS observation_publication')
        for table in TABLES.values():
            _sql(runtime,c,f"CREATE TABLE IF NOT EXISTS {table}(record_key UUID PRIMARY KEY,reuse_key TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('accepted','revoked')),admission JSONB NOT NULL,audit JSONB NOT NULL)")
            _sql(runtime,c,f'CREATE UNIQUE INDEX IF NOT EXISTS {table.split(".")[1]}_reuse ON {table}(reuse_key) WHERE state=\'accepted\'')


def _own_target(a):
    targets=[t for t in a['lock_targets'] if t['namespace']==a['owner']+'.admission']
    if len(targets)!=1:raise ValueError('可信登记锁目标缺失或重复')
    return targets[0]


def _anchor(runtime,a):
    t=_own_target(a)
    try:
        with _pg(runtime) as pg,pg.cursor() as c:
            row=_sql(runtime,c,f'SELECT state,admission FROM {TABLES[t["namespace"]]} WHERE record_key=%s',(t['key'],)).fetchone()
    except psycopg2.errors.UndefinedTable:raise ValueError('缺少可信准入登记') from None
    if row!=('accepted',a):raise ValueError('可信准入已撤销、缺失或整份绑定不符')


def verify_current(runtime,admission,*,guard):
    guard=runtime.guarded(guard)
    guard();admission_shape(admission);b=untyped(admission['owner_binding'])
    physical=_physical(runtime,b)
    if physical!=admission['physical']:raise ValueError('实际PG物理身份/数据库OID不符')
    if {k:v for k,v in admission['validator'].items() if k!='validation_digest'}!=_rules(runtime):raise ValueError('验收规则/源码版本/模式不符')
    _anchor(runtime,admission)
    raw=_binding(runtime,b)
    if b['owner']=='reference':
        dep=next(a for a in runtime.dependency_admissions if a['admission_id']==b['m2_admission_id'])
        if admission['dependencies']!=[dep['admission_id']]:raise ValueError('参考依赖边不符')
        verify_current(runtime,dep,guard=guard)
    elif admission['dependencies']!=[]:raise ValueError('M2不得有依赖')
    selection=Selection(runtime.dsn,raw['run_id'],raw['snapshot'])
    from data_pipeline.bgp.archive.validation import _closing
    with _closing([]) as cleanup:
        db=selection.connect();cleanup.append(db.close)
        _ducklake_root(runtime,db,physical['root'])  # 仅固定选择与目录元数据。
    runtime.event('fixed_selection_check',rows=len(selection.checkpoints))
    _entities_current(runtime,admission['entities'],guard)


def admit(runtime,owner_binding,*,guard):
    from data_pipeline.bgp.archive.validation import validate
    owner_binding=untyped(typed(owner_binding))
    guard=runtime.guarded(guard)
    guard();raw=_binding(runtime,owner_binding);physical=_physical(runtime,owner_binding);rules=_rules(runtime)
    dependencies=[]
    if owner_binding['owner']=='reference':
        dep=next(a for a in runtime.dependency_admissions if a['admission_id']==owner_binding['m2_admission_id'])
        verify_current(runtime,dep,guard=guard);dependencies=[dep['admission_id']]
    _setup(runtime)
    search=digest(dict(binding=typed(owner_binding),physical=physical,rules=rules,dependencies=dependencies))
    table=TABLES[owner_binding['owner']+'.admission']
    with _pg(runtime) as pg,pg.cursor() as c:
        candidates=_sql(runtime,c,f'SELECT admission FROM {table} WHERE state=\'accepted\' AND audit->>\'search\'=%s',(search,)).fetchall()
    for (candidate,) in candidates:
        try:verify_current(runtime,candidate,guard=guard)
        except ValueError:continue
        runtime.event('admit_reused',admission_id=candidate['admission_id']);return candidate
    # key独立分配，不由最终摘要派生；失败不会登记accepted。
    key=str(uuid.uuid4())
    inventory,entities,cost=validate(runtime,owner_binding,guard=guard)
    targets=_source_targets(physical,raw)
    if dependencies:targets=list(dep['lock_targets'])
    targets.append(_target(physical,owner_binding['owner']+'.admission',key))
    targets=sorted(targets,key=lambda t:(t['stage'],t['system_identifier'],t['database_oid'],t['namespace'],t['key']))
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    a=dict(contract=CONTRACT,owner=owner_binding['owner'],owner_revision=revision,owner_binding=typed(owner_binding),binding_codec=CODEC,
        physical=physical,validator={**rules,'validation_digest':digest(inventory)},inventory_digest=digest(inventory),entities=entities,
        dependencies=dependencies,lock_targets=targets)
    a['admission_id']=digest(a)
    _binding(runtime,owner_binding);_entities_current(runtime,entities,guard)
    if _physical(runtime,owner_binding)!=physical or _rules(runtime)!=rules:raise ValueError('准入期间身份/规则漂移')
    reuse=digest(dict(search=search,entities=entities,inventory=inventory))
    with _pg(runtime,write=True) as pg,pg.cursor() as c:
        # 准入登记事务显式锁住全部原锚；公开hold_lock仍严格只取一个目标。
        _sql(runtime,c,"SELECT set_config('lock_timeout',%s,true)",(str(runtime._checked_limits()['lock_timeout_ms'])+'ms',))
        for target in targets:
            if target['namespace']==a['owner']+'.admission':continue
            _locked_row(runtime,c,a,target)
        _binding(runtime,owner_binding);_entities_current(runtime,entities,guard)
        if _rules(runtime)!=rules:raise ValueError('登记前规则版本漂移')
        _sql(runtime,c,f'INSERT INTO {table} VALUES (%s,%s,\'accepted\',%s,%s) ON CONFLICT DO NOTHING',
             (key,reuse,Json(a),Json(dict(search=search,inventory=inventory,cost=cost))))
        row=_sql(runtime,c,f'SELECT admission FROM {table} WHERE reuse_key=%s AND state=\'accepted\'',(reuse,)).fetchone()
        if row is None:raise ValueError('可信登记未成功')
        a=row[0]
    runtime.event('admit_complete',cost=cost,admission_id=a['admission_id'])
    return a

def _locked_row(runtime,c,admission,lock_target):
    b=untyped(admission['owner_binding']);raw=b.get('m2_binding',b)
    ns=lock_target['namespace'];key=lock_target['key']
    if ns=='m2.run':
        row=_sql(runtime,c,'SELECT state,seal FROM observation_m2.runs WHERE run_id=%s FOR SHARE',(key,)).fetchone()
        expected=('observation_sealed',raw['seal'])
    elif ns=='m2.checkpoint':
        import json
        run,ordinal=json.loads(key)
        row=_sql(runtime,c,'SELECT payload FROM observation_m2.checkpoints WHERE run_id=%s AND ordinal=%s FOR SHARE',(run,ordinal)).fetchone()
        expected=(raw['seal']['checkpoints'][ordinal],)
    elif ns in TABLES:
        row=_sql(runtime,c,f'SELECT state,admission FROM {TABLES[ns]} WHERE record_key=%s FOR SHARE',(key,)).fetchone()
        expected_admission=admission if ns==admission['owner']+'.admission' else next(a for a in runtime.dependency_admissions if _own_target(a)==lock_target)
        expected=('accepted',expected_admission)
    else:raise ValueError('未知有限锁namespace')
    if row!=expected:raise ValueError('锁后实际登记不符')



@contextmanager
def hold_lock(runtime,admission,lock_target,*,guard,lock_connection=None):
    from data_pipeline.common.admission_locks import validated_connection
    guard=runtime.guarded(guard)
    guard();admission_shape(admission)
    if {k:v for k,v in admission['validator'].items() if k!='validation_digest'}!=_rules(runtime):raise ValueError('锁规则/源码版本/模式不符')
    if lock_target not in admission['lock_targets']:raise ValueError('锁目标不属于可信Admission')
    b=untyped(admission['owner_binding']);raw=b.get('m2_binding',b)
    if _physical(runtime,b)!=admission['physical']:raise ValueError('锁物理身份不符')
    _anchor(runtime,admission)
    runtime.resource_guard()
    pg=psycopg2.connect(runtime.dsn) if lock_connection is None else validated_connection(runtime,admission,lock_target,lock_connection);primary=None
    try:
        with pg.cursor() as c:
            _sql(runtime,c,"SELECT set_config('lock_timeout',%s,true)",(str(runtime._checked_limits()['lock_timeout_ms'])+'ms',))
            _locked_row(runtime,c,admission,lock_target)
        yield
    except BaseException as exc:primary=exc;raise
    finally:
        # 不尾audit、不隐式取得其他stage10目标。
        if lock_connection is None:_release(pg,primary)


@dataclass
class ReadSession:
    iterator:object=None
    receipt:object=None
    exhausted:bool=False
    rows:int=0
    hasher:object=field(default_factory=hashlib.sha256)
    def __iter__(self):return self
    def __next__(self):return next(self.iterator)
    def close(self):self.iterator.close()


@contextmanager
def open_reader(runtime,admission,request,*,guard):
    from data_pipeline.bgp.archive.validation import read_rows
    guard=runtime.guarded(guard)
    guard()
    fields(request,REQUEST_FIELDS)
    import json
    request=json.loads(canonical(request))
    if request['codec_version']!=CODEC:raise ValueError('请求codec不符')
    if type(request['batch_rows']) is not int or not 1<=request['batch_rows']<=10000 or type(request['batch_bytes']) is not int or request['batch_bytes']<1:raise ValueError('批预算无效')
    scope=untyped(request['scope_typed']);fields(scope,{'source_ids'})
    verify_current(runtime,admission,guard=guard);b=untyped(admission['owner_binding'])
    allowed=b['ordered_source_ids'] if b['owner']=='m2' and request['view']!='references' else ([b['source_id']] if b['owner']=='reference' else [x['source_id'] for x in b['reference_sources']])
    sources=scope['source_ids']
    if type(sources) is not list or len(set(sources))!=len(sources) or [s for s in allowed if s in sources]!=sources:raise ValueError('请求来源不在绑定原序')
    views=tuple(OBS_TABLES) if b['owner']=='m2' else ('references',)
    if request['view'] not in views:raise ValueError('未知有限观察view')
    session=ReadSession();source=read_rows(runtime,b,request['view'],sources,request['batch_rows'],guard=guard)
    def batches():
        pending=[]
        for row in source:
            guard();single=typed([row]).encode()
            if len(single)>request['batch_bytes']:raise ValueError('单行超过批字节预算')
            candidate=typed([*pending,row]).encode()
            if pending and (len(pending)>=request['batch_rows'] or len(candidate)>request['batch_bytes']):
                text=typed(pending);yield dict(rows_typed=text,codec_version=CODEC,rows=len(pending),bytes=len(text.encode()));pending=[]
            pending.append(row);session.rows+=1;session.hasher.update(bytes.fromhex(sha(row)))
        if pending:
            text=typed(pending);yield dict(rows_typed=text,codec_version=CODEC,rows=len(pending),bytes=len(text.encode()))
        session.exhausted=True
    session.iterator=batches();primary=None
    try:
        yield session
    except BaseException as exc:primary=exc;raise
    finally:
        failures=[]
        for resource in (session.iterator,source):
            try:resource.close()
            except BaseException as exc:failures.append(exc)
        if failures:
            if primary is not None:primary.cleanup_errors=tuple(failures)
            else:raise failures[0]
        elif primary is None and session.exhausted:
            verify_current(runtime,admission,guard=guard)
            session.receipt=dict(contract='component-publication-read/v1',admission_id=admission['admission_id'],request_digest=digest(request),rows=session.rows,
                typed_digest=session.hasher.hexdigest(),execution='complete',coverage_ref=typed(dict(admission_id=admission['admission_id'],source_ids=sources,view=request['view'],
                    source_checkpoints=[{k:cp[k] for k in ('source_id','ordinal','counts','raw','parse','ingest')} for cp in b.get('m2_binding',b)['seal']['checkpoints'] if cp['source_id'] in sources],
                    observation_qualification='observation_sealed',business='not_run',business_absence='Unknown')))
