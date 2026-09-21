"""Resource 独立RIB的显式人工/真实候选P1；不发布业务head或改变科学结果。"""
from contextlib import contextmanager
from dataclasses import dataclass,field
import hashlib
from importlib.metadata import version
import math
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import time
import uuid

import psycopg2
from psycopg2.extras import Json
from data_pipeline.bgp.archive import admission as upstream
from data_pipeline.bgp.archive.value_codec import ADMISSION_FIELDS, PHYSICAL_FIELDS, VALIDATOR_FIELDS, ENTITY_FIELDS, LOCK_FIELDS, REQUEST_FIELDS
from data_pipeline.bgp.archive.store import connect_duckdb, literal
from data_pipeline.analysis.resources.publication_codec import CODEC, canonical, digest, fields, typed, untyped
from data_pipeline.analysis.resources.qualification import ALL_TABLES, PROFILE, VERSION
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader
from data_pipeline.analysis.resources.references import _binding as reference_binding

CONTRACT='component-publication-admission/v1'
TABLE='domeye.resource_publication_admissions'
BINDING_FIELDS={'binding','receipt','run_id','snapshot','dataset_id'}
OWN_NAMES={'resource.run','resource.admission','resource.reference'}


def positive(value,name,integer=True):
    if (type(value) is not int if integer else type(value) not in (int,float)) or (type(value) is float and not math.isfinite(value)) or value<=0:
        raise ValueError('必须为正有限预算：'+name)


@dataclass
class Runtime:
    dsn:str
    allowed_roots:tuple
    scratch_root:Path
    upstream_runtime:object
    dependency_admissions:tuple
    fixture_only:bool=False
    max_rows:int|None=None
    max_bytes:int|None=None
    max_rss_bytes:int|None=None
    max_seconds:float|None=None
    memory_bytes:int|None=None
    max_temp_bytes:int|None=None
    lock_timeout_ms:int|None=None
    audit_sink:object=None
    execution_profile:str|None=None
    expected_resource_binding:dict|None=None
    output_root:Path|None=None
    dependency_runtimes:dict|None=None
    min_free_bytes:int|None=None

    def __post_init__(self):
        if type(self.fixture_only) is not bool:raise ValueError('fixture_only须为显式bool')
        if self.fixture_only:
            if self.execution_profile is not None or self.expected_resource_binding is not None or self.output_root is not None or self.dependency_runtimes is not None:
                raise ValueError('人工与真实候选参数不得混用')
            self._profile='synthetic-fixture/v1'
        elif self.execution_profile=='real-candidate/v1':
            if type(self.expected_resource_binding) is not dict or self.output_root is None or type(self.dependency_runtimes) is not dict:
                raise ValueError('真实候选缺完整Resource绑定/输出范围/依赖Runtime')
            self._profile=self.execution_profile
            if self.max_seconds is not None:raise ValueError('真实批量处理不接受总时长截止')
        else:raise ValueError('须显式选择fixture或real-candidate/v1')
        self._mode=(self.fixture_only,self.execution_profile)
        defaults=dict(max_rows=1000000,max_bytes=256*1024**2,max_rss_bytes=2*1024**3,
                      memory_bytes=256*1024**2,max_temp_bytes=512*1024**2,lock_timeout_ms=2000,min_free_bytes=128*1024**2)
        for name,value in defaults.items():
            if getattr(self,name) is None:
                if not self.fixture_only:raise ValueError('真实候选缺显式资源预算：'+name)
                setattr(self,name,value)
        if self.fixture_only and self.max_seconds is None:self.max_seconds=300
        roots=tuple(Path(p) for p in self.allowed_roots)
        if not roots or any(not p.is_absolute() for p in roots):raise ValueError('须显式绝对允许根')
        self.allowed_roots=tuple(p.resolve(strict=True) for p in roots)
        self.scratch_root=self.path(self.scratch_root)
        if not self.scratch_root.is_dir():raise ValueError('scratch须为既有目录')
        self._scope_digest=None
        if not self.fixture_only:
            self.output_root=self.path(self.output_root)
            if not self.output_root.is_dir():raise ValueError('输出须为既有目录')
            self.expected_resource_binding=untyped(typed(self.expected_resource_binding))
            fields(self.expected_resource_binding,BINDING_FIELDS)
            if str(self.output_root)!=self.expected_resource_binding['receipt']['storage_layout']['local_output']:raise ValueError('输出范围与原绑定不符')
            self._binding_text=typed(self.expected_resource_binding)
            self._output_layout=untyped(typed(self.expected_resource_binding['receipt']['storage_layout']))
            self._scope_digest=self._scope()
            self._directories=tuple((p,self._directory_identity(p)) for p in (*self.allowed_roots,self.scratch_root,self.output_root))
            protected=set()
            for dep in self.dependency_admissions:
                upstream.admission_shape(dep)
                original=upstream.untyped(dep['owner_binding']);bound=original.get('m2_binding',original)
                protected.update(Path(e['path']).parent.resolve(strict=True) for e in bound['plan']['inputs'])
                protected.update(Path(f['path']).parent.resolve(strict=True) for cp in bound['seal']['checkpoints'] for f in cp['files'])
            self._protected=tuple(protected)
        self.resource_usage=dict(checks=0,peak_rss_bytes=0,min_free_bytes=None,rss_scope='whole_process_lifetime_peak_excluding_pg')
        self.validate()

    path=upstream.Runtime.path
    event=upstream.Runtime.event

    @staticmethod
    def _directory_identity(path):
        p=path.resolve(strict=True);stat=p.stat();return (str(p),stat.st_dev,stat.st_ino)

    def _scope(self):
        return digest(dict(output=str(self.output_root),scratch=str(self.scratch_root),
            roots=[str(p) for p in self.allowed_roots],dsn=self.dsn,dependencies=sorted(d['admission_id'] for d in self.dependency_admissions),
            runtimes=sorted(self.dependency_runtimes)))

    def _separate(self,root):
        root=Path(root).resolve(strict=True)
        if root==self.scratch_root or root in self.scratch_root.parents or self.scratch_root in root.parents:
            raise ValueError('scratch须与原件/固定输出目录隔离')

    def validate(self):
        if (self.fixture_only,self.execution_profile)!=self._mode:raise ValueError('Resource Runtime模式漂移')
        if type(self.dsn) is not str or not self.dsn.strip() or not self.allowed_roots:raise ValueError('缺显式DSN或允许根')
        for name in ('max_rows','max_bytes','max_rss_bytes','memory_bytes','max_temp_bytes','lock_timeout_ms','min_free_bytes'):
            positive(getattr(self,name),name)
        if self.fixture_only:positive(self.max_seconds,'max_seconds',False)
        elif self.max_seconds is not None:raise ValueError('真实批量处理不得设置总时长截止')
        if not self.fixture_only:
            if self._scope()!=self._scope_digest:raise ValueError('Resource固定范围漂移')
            for path,identity in self._directories:
                current=self.path(path)
                if not current.is_dir() or self._directory_identity(current)!=identity:raise ValueError('允许根/输出/scratch实际目录身份漂移')
            self._separate(self.output_root)
            layout=self._output_layout
            self._separate(Path(layout['catalog_data_path'])/layout['schema_path'] if layout['path_is_relative'] else layout['schema_path'])
            expected_ids={d['admission_id'] for d in self.dependency_admissions}
            if not expected_ids or set(self.dependency_runtimes)!=expected_ids:raise ValueError('须逐依赖提供真实Runtime')
        for dep in self.dependency_admissions:
            runtime=_upstream(self,dep)
            if runtime.dsn!=self.dsn or runtime.fixture_only!=self.fixture_only or runtime._profile!=self._profile:raise ValueError('依赖DSN/模式不符')
        if not self.fixture_only:
            for root in self._protected:
                if root==self.scratch_root or root in self.scratch_root.parents or self.scratch_root in root.parents:raise ValueError('scratch与原输入目录未隔离')

    def check_binding(self,binding):
        self.resource_guard()
        if not self.fixture_only:
            if typed(self.expected_resource_binding)!=self._binding_text:raise ValueError('Resource完整固定绑定漂移')
            if typed(binding)!=self._binding_text:raise ValueError('绑定超出真实Resource固定范围')

    def resource_guard(self):
        self.validate()
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        free=shutil.disk_usage(self.scratch_root).free
        usage=self.resource_usage;usage['checks']+=1;usage['peak_rss_bytes']=max(usage['peak_rss_bytes'],rss)
        usage['min_free_bytes']=free if usage['min_free_bytes'] is None else min(free,usage['min_free_bytes'])
        if rss>self.max_rss_bytes or free<self.min_free_bytes:raise ValueError('Resource RSS/最低空闲盘资源保护')

    def budget(self,guard):
        self.resource_guard();start=time.monotonic()
        def check():
            self.resource_guard();guard();self.resource_guard()
            if self.fixture_only and time.monotonic()-start>self.max_seconds:raise ValueError('fixture时间预算')
        check();return check


def _upstream(rt,dep):
    if rt.fixture_only:return rt.upstream_runtime
    try:return rt.dependency_runtimes[dep['admission_id']]
    except KeyError:raise ValueError('缺依赖Runtime') from None


def _close(actions,primary=None):
    # generator.close()会吞掉GeneratorExit；真实清理失败必须抛到外层，
    # 才能附着到调用方原主错，或在没有主错时传播。
    if isinstance(primary,GeneratorExit):primary=None
    errors=[]
    for action in actions:
        try:action()
        except BaseException as exc:errors.append(exc)
    if errors:
        if primary is None:
            primary=errors.pop(0);primary.cleanup_errors=(*getattr(primary,'cleanup_errors',()),*errors);raise primary
        primary.cleanup_errors=(*getattr(primary,'cleanup_errors',()),*errors)


@contextmanager
def _pg(rt,write=False):
    rt.resource_guard()
    pg=psycopg2.connect(rt.dsn);error=None
    try:
        pg.set_session(readonly=not write)
        yield pg
        if write:pg.commit()
    except BaseException as exc:error=exc;raise
    finally:_close((pg.rollback,pg.close),error)


def _sql(rt,c,sql,args=()):
    rt.resource_guard();rt.event('pg_sql',sql=sql);c.execute(sql,args);rt.resource_guard();return c


@contextmanager
def _lake(rt):
    import tempfile
    rt.resource_guard()
    if not rt.fixture_only:
        rt.check_binding(rt.expected_resource_binding);_country(rt,rt.expected_resource_binding)
    temp=tempfile.TemporaryDirectory(prefix='resource-p1-',dir=rt.scratch_root);db=None;error=None
    try:
        db=connect_duckdb();db.execute('SET memory_limit='+literal(str(rt.memory_bytes)+'B'))
        db.execute('SET threads=2');db.execute('SET temp_directory='+literal(temp.name))
        db.execute('SET max_temp_directory_size='+literal(str(rt.max_temp_bytes)+'B'))
        db.execute('LOAD ducklake');db.execute('LOAD postgres')
        db.execute('ATTACH '+literal('ducklake:postgres:'+rt.dsn)+' AS lake (READ_ONLY)')
        yield db
    except BaseException as exc:error=exc;raise
    finally:_close(([db.close] if db is not None else [])+[temp.cleanup],error)


def _rules(rt=None):
    # Producer 版本留在原 receipt；这里单独固定当前验收代码和依赖，不要求历史重产。
    root=Path(__file__).parent
    files=list(root.glob('*.py'))+[Path(upstream.__file__),Path(upstream.__file__).with_name('validation.py'),Path(upstream.__file__).parents[1]/'input/path_decoding.py']
    code={str(p.relative_to(root.parents[2])):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    code['packages']={name:version(name) for name in ('numpy','pandas','duckdb','pyarrow','psycopg2-binary')}
    from data_pipeline.common.admission_locks import code_sha as lock_code_sha
    code['lock_connection_code']=lock_code_sha()
    return dict(version='resource-publication-admission/v1',code_sha256=digest(code),typed_schema=PROFILE,typed_codec=CODEC,
        rules_digest=digest(dict(execution_profile=rt._profile if rt is not None else 'synthetic-fixture/v1',tables=ALL_TABLES,qualification=VERSION,science='fixed-observation-algorithm-audit/v1',upstream=upstream._rules(rt))))


def _shape(a):
    fields(a,ADMISSION_FIELDS);fields(a['physical'],PHYSICAL_FIELDS);fields(a['validator'],VALIDATOR_FIELDS)
    if a['contract']!=CONTRACT or a['owner']!='resource' or a['binding_codec']!=CODEC:raise ValueError('Resource Admission版本不符')
    if a['admission_id']!=digest({k:v for k,v in a.items() if k!='admission_id'}):raise ValueError('Admission摘要不符')
    if type(a['owner_revision']) is not str or len(a['owner_revision'])!=40:raise ValueError('完整owner提交无效')
    for k in ('database_oid','snapshot'):positive(a['physical'][k],k)
    if a['physical']['catalog']!='lake':raise ValueError('catalog无效')
    if a['dependencies']!=sorted(set(a['dependencies'])):raise ValueError('依赖顺序/多重性不符')
    if [e['path'] for e in a['entities']]!=sorted(set(e['path'] for e in a['entities'])):raise ValueError('实体顺序/多重性不符')
    for e in a['entities']:
        fields(e,ENTITY_FIELDS)
        for k in ('device','inode','size','mtime_ns','ctime_ns'):
            if type(e[k]) is not int or e[k]<0:raise ValueError('实体stat类型无效')
    for t in a['lock_targets']:
        fields(t,LOCK_FIELDS)
        if type(t['stage']) is not int or type(t['database_oid']) is not int:raise ValueError('锁类型无效')
        if t['namespace'] in OWN_NAMES:
            if t['stage']!=(10 if t['namespace']=='resource.reference' else 30):raise ValueError('锁stage不符')
        elif t['namespace'] not in ('m2.run','m2.checkpoint','m2.admission','reference.admission') or t['stage']!=10:raise ValueError('未知锁')
    if len(set(map(canonical,a['lock_targets'])))!=len(a['lock_targets']):raise ValueError('重复锁')
    fields(untyped(a['owner_binding']),BINDING_FIELDS)


def _binding(rt,b):
    fields(b,BINDING_FIELDS)
    if type(b['run_id']) is not str or not b['run_id'].isalnum():raise ValueError('run无效')
    positive(b['snapshot'],'snapshot')
    rt.check_binding(b)
    if rt.fixture_only and (any(not s['context']['origin_uri'].startswith('fixture://') for s in b['binding']['sources']) or not b['binding']['country_reference']['origin_uri'].startswith('fixture://')):raise ValueError('本片只接受人工fixture来源')
    r=ResourceObservationReader(rt.dsn,b['run_id'],b['snapshot'],b['dataset_id'],allow_fixture=rt.fixture_only)
    actual=dict(binding=r.manifest,receipt=r.receipt,run_id=r.run_id,snapshot=r.snapshot,dataset_id=r.dataset_id)
    if typed(actual)!=typed(b):raise ValueError('原Reader.inputs完整绑定不符')
    return r


def inspect_binding(runtime,run_id,snapshot,dataset_id):
    rt=runtime;rt.resource_guard()
    if not rt.fixture_only:
        expected=rt.expected_resource_binding
        if (run_id,snapshot,dataset_id)!=(expected['run_id'],expected['snapshot'],expected['dataset_id']):raise ValueError('inspect超出固定Resource范围')
    reader=ResourceObservationReader(rt.dsn,run_id,snapshot,dataset_id,allow_fixture=rt.fixture_only)
    result=dict(binding=reader.manifest,receipt=reader.receipt,run_id=reader.run_id,snapshot=reader.snapshot,dataset_id=reader.dataset_id)
    _binding(rt,result);return result


def _physical(rt,b):
    with _pg(rt) as pg,pg.cursor() as c:
        system,oid=_sql(rt,c,'SELECT system_identifier::text,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) FROM pg_control_system()').fetchone()
        root=_sql(rt,c,"SELECT value FROM public.ducklake_metadata WHERE key='data_path'").fetchone()[0]
    if root!=b['receipt']['storage_layout']['catalog_data_path']:raise ValueError('原Resource catalog根不符')
    return dict(system_identifier=system,database_oid=oid,catalog='lake',schema='resource_'+b['run_id'],root=str(rt.path(root)),snapshot=b['snapshot'])


def _dependencies(rt,b,guard):
    manifest=b['binding'];selected=[]
    source_ids=[s['context']['source_id'] for s in manifest['sources']]
    if len(set(source_ids))!=len(source_ids) or set(source_ids)!=set(manifest['observation_inputs']):raise ValueError('Resource来源重复或元数据枚举不符')
    for dep in rt.dependency_admissions:
        upstream.admission_shape(dep)
    def one(predicate):
        found=[d for d in rt.dependency_admissions if predicate(d,upstream.untyped(d['owner_binding']))]
        if len(found)!=1:raise ValueError('缺少或重复显式M2/reference Admission')
        dep=found[0];upstream.verify_current(_upstream(rt,dep),dep,guard=guard);selected.append(dep)
        return dep
    for binding_id,full in manifest['observation_runs'].items():
        dep=one(lambda d,x:d['owner']=='m2' and x['input_binding_id']==binding_id and x['seal']==full['seal'])
        original=upstream.untyped(dep['owner_binding'])
        needed=[s['context']['source_id'] for s in manifest['sources'] if manifest['observation_inputs'][s['context']['source_id']]['binding_id']==binding_id]
        if canonical(full['binding'])!=original['input_binding'] or canonical(full['manifest'])!=canonical(original['plan']['manifest']):raise ValueError('M2完整绑定字段不符')
        global_ids=[s['source_id'] for s in full['binding']['sources']]
        chosen=original['ordered_source_ids']
        if len(set(chosen))!=len(chosen) or [sid for sid in global_ids if sid in chosen]!=chosen:raise ValueError('M2选择不在原完整输入序')
        # Resource按时点消费，M2选择按完整输入序；只核包含关系，不互相重排。
        if not set(needed)<=set(chosen):raise ValueError('M2 Admission缺少所需RIB')
        checkpoints={cp['source_id']:cp for cp in full['seal']['checkpoints']}
        for sid in needed:
            meta=manifest['observation_inputs'][sid]
            if type(meta['source_rank']) is not int or meta['source_rank']!=global_ids.index(sid):raise ValueError('Resource原global rank不符')
            if sid not in checkpoints or meta['checkpoint']!=checkpoints[sid]:raise ValueError('Resource原checkpoint不符')
    csv=manifest['csv_reference'];meta=manifest['csv_observation']
    ref=one(lambda d,x:d['owner']=='reference' and x['source_id']==csv['source_id'] and x['m2_binding']['run_id']==csv['run_id'] and x['m2_binding']['snapshot']==csv['snapshot'] and x['checkpoint_ordinal']==meta['checkpoint']['ordinal'] and x['m2_binding']['seal']['checkpoints'][x['checkpoint_ordinal']]==meta['checkpoint'])
    one(lambda d,x:d['owner']=='m2' and d['admission_id']==upstream.untyped(ref['owner_binding'])['m2_admission_id'])
    return sorted({d['admission_id']:d for d in selected}.values(),key=lambda d:d['admission_id'])


def _country(rt,b):
    ref=b['binding']['country_reference']
    bound=reference_binding(rt.dsn,ref['reference_id'],ref['dataset_id'],rt.fixture_only)
    if not rt.fixture_only:
        rt.path(bound['raw_path']);rt._separate(Path(bound['raw_path']).parent);rt._separate(bound['output'])
        layout=bound['storage_layout'];rt._separate(Path(layout['catalog_data_path'])/layout['schema_path'] if layout['path_is_relative'] else layout['schema_path'])
    return bound


def _descriptor(rt,db,b):
    result={}
    from data_pipeline.analysis.resources.qualification import check_table_set
    check_table_set(rt.dsn,'resource_'+b['run_id'],b['snapshot'])
    country=_country(rt,b)
    for schema,snapshot,tables in [('resource_'+b['run_id'],b['snapshot'],ALL_TABLES),(country['lake_schema'],country['snapshot'],{'rows':None})]:
        layout=b['receipt']['storage_layout'] if schema=='resource_'+b['run_id'] else country['storage_layout']
        with _pg(rt) as pg,pg.cursor() as c:
            actual=_sql(rt,c,'SELECT path,path_is_relative FROM public.ducklake_schema WHERE schema_name=%s AND begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s)',(schema,snapshot,snapshot)).fetchall()
        if actual!=[(layout['schema_path'],layout['path_is_relative'])]:raise ValueError('原schema目录绑定不符')
        for table in tables:
            rt.event('descriptor_metadata',table=table)
            rows=db.execute('SELECT data_file,delete_file FROM ducklake_list_files(?,?,schema => ?,snapshot_version => ?)', ['lake',table,schema,snapshot]).fetchall()
            # 当前旧制品为只追加布局；拒绝无法固定实体语义的delete文件。
            if any(d is not None for _,d in rows):raise ValueError('不支持带delete文件的固定制品')
            paths=sorted(str(rt.path(p)) for p,_ in rows)
            with _pg(rt) as pg,pg.cursor() as c:
                files=_sql(rt,c,"""SELECT row_to_json(f) FROM public.ducklake_data_file f JOIN public.ducklake_table t USING(table_id)
                    JOIN public.ducklake_schema s USING(schema_id) WHERE s.schema_name=%s AND t.table_name=%s
                    AND s.begin_snapshot<=%s AND (s.end_snapshot IS NULL OR s.end_snapshot>%s)
                    AND t.begin_snapshot<=%s AND (t.end_snapshot IS NULL OR t.end_snapshot>%s)
                    AND f.begin_snapshot<=%s AND (f.end_snapshot IS NULL OR f.end_snapshot>%s) ORDER BY f.data_file_id""",(schema,table,*([snapshot]*6))).fetchall()
                columns=_sql(rt,c,"""SELECT row_to_json(c) FROM public.ducklake_column c JOIN public.ducklake_table t USING(table_id)
                    JOIN public.ducklake_schema s USING(schema_id) WHERE s.schema_name=%s AND t.table_name=%s
                    AND c.begin_snapshot<=%s AND (c.end_snapshot IS NULL OR c.end_snapshot>%s) ORDER BY c.column_id""",(schema,table,snapshot,snapshot)).fetchall()
            if any(r[0]['mapping_id'] is not None for r in files):raise ValueError('固定Resource不支持column mapping')
            result[schema+'.'+table]=dict(paths=paths,files=[r[0] for r in files],columns=[r[0] for r in columns])
    return result


def _target(p,ns,key):return dict(stage=10 if ns=='resource.reference' else 30,system_identifier=p['system_identifier'],database_oid=p['database_oid'],namespace=ns,key=key)


def _anchor(rt,a):
    targets=[t for t in a['lock_targets'] if t['namespace']=='resource.admission']
    if len(targets)!=1:raise ValueError('可信主键缺失')
    with _pg(rt) as pg,pg.cursor() as c:
        try:row=_sql(rt,c,'SELECT state,admission,audit FROM '+TABLE+' WHERE record_key=%s',(targets[0]['key'],)).fetchone()
        except psycopg2.errors.UndefinedTable:raise ValueError('缺少可信Admission') from None
    if row is None or row[:2]!=('accepted',a):raise ValueError('可信记录缺失/撤销/不符')
    audit=row[2]
    if digest(audit['inventory'])!=a['inventory_digest'] or a['validator']['validation_digest']!=digest(audit['inventory']):raise ValueError('审计登记不符')
    return audit


def verify_current(runtime,admission,*,guard):
    rt=runtime;guard=rt.budget(guard);guard();_shape(admission);b=untyped(admission['owner_binding'])
    if _physical(rt,b)!=admission['physical']:raise ValueError('实际PG/OID/root不符')
    if {k:v for k,v in admission['validator'].items() if k!='validation_digest'}!=_rules(rt):raise ValueError('validator规则发生变化')
    audit=_anchor(rt,admission);_binding(rt,b)
    deps=_dependencies(rt,b,guard)
    if [d['admission_id'] for d in deps]!=admission['dependencies']:raise ValueError('依赖绑定不符')
    if _country(rt,b)!=audit['inventory']['country']:raise ValueError('独立参考锚变化')
    with _lake(rt) as db:
        if _descriptor(rt,db,b)!=audit['inventory']['descriptor']:raise ValueError('目录文件选择发生变化')
    upstream._entities_current(rt,admission['entities'],guard);guard()
    rt.event('current_complete',full_table_scans=0,algorithm_replays=0)


def _locked(rt,c,a,target,audit):
    b=untyped(a['owner_binding']);ns=target['namespace'];key=target['key']
    if ns=='resource.admission':
        row=_sql(rt,c,'SELECT state,admission FROM '+TABLE+' WHERE record_key=%s FOR SHARE',(key,)).fetchone();expected=('accepted',a)
    elif ns=='resource.run':
        row=_sql(rt,c,'SELECT state,snapshot,dataset_id,binding_manifest,observation_receipt FROM domeye.resource_runs WHERE run_id=%s FOR SHARE',(key,)).fetchone()
        expected=('complete',b['snapshot'],b['dataset_id'],b['binding'],b['receipt'])
    elif ns=='resource.reference':
        row=_sql(rt,c,'SELECT state,dataset_id,manifest FROM domeye.resource_references WHERE reference_id=%s FOR SHARE',(key,)).fetchone()
        expected=('complete',b['binding']['country_reference']['dataset_id'],audit['inventory']['country'])
    else:raise ValueError('本owner只取得单个Resource登记锁，依赖锁由对应owner路由')
    if row!=expected:raise ValueError('锁后实际登记不符')


@contextmanager
def hold_lock(runtime,admission,lock_target,*,guard,lock_connection=None):
    from data_pipeline.common.admission_locks import validated_connection
    rt=runtime;guard=rt.budget(guard);_shape(admission)
    if lock_target not in admission['lock_targets'] or lock_target['namespace'] not in OWN_NAMES:raise ValueError('非本owner实际锁目标')
    b=untyped(admission['owner_binding'])
    rt.check_binding(b)
    if _physical(rt,b)!=admission['physical']:raise ValueError('锁物理身份不符')
    if {k:v for k,v in admission['validator'].items() if k!='validation_digest'}!=_rules(rt):raise ValueError('锁模式/规则不符')
    audit=_anchor(rt,admission);pg=psycopg2.connect(rt.dsn) if lock_connection is None else validated_connection(rt,admission,lock_target,lock_connection);error=None
    try:
        with pg.cursor() as c:
            _sql(rt,c,"SELECT set_config('lock_timeout',%s,true)",(str(rt.lock_timeout_ms)+'ms',));guard()
            _locked(rt,c,admission,lock_target,audit)
        yield
        guard();rt.check_binding(b)
    except BaseException as exc:error=exc;raise
    finally:
        if lock_connection is None:_close((pg.rollback,pg.close),error)


def admit(runtime,owner_binding,*,guard):
    from data_pipeline.analysis.resources.publication_validation import validate
    rt=runtime;guard=rt.budget(guard);b=untyped(typed(owner_binding));_binding(rt,b)
    p=_physical(rt,b);deps=_dependencies(rt,b,guard);rules=_rules(rt)
    search=digest(dict(binding=typed(b),physical=p,rules=rules,dependencies=[d['admission_id'] for d in deps]))
    with _pg(rt,True) as pg,pg.cursor() as c:
        _sql(rt,c,'CREATE TABLE IF NOT EXISTS '+TABLE+" (record_key UUID PRIMARY KEY,reuse_key TEXT UNIQUE NOT NULL,state TEXT NOT NULL CHECK(state IN ('accepted','revoked')),admission JSONB NOT NULL,audit JSONB NOT NULL)")
        candidates=_sql(rt,c,'SELECT admission FROM '+TABLE+" WHERE state='accepted' AND audit->>'search'=%s",(search,)).fetchall()
    for (candidate,) in candidates:
        try:verify_current(rt,candidate,guard=guard)
        except ValueError:continue
        rt.event('admit_reused',full_table_scans=0,algorithm_replays=0);return candidate
    key=str(uuid.uuid4());inventory,entities,cost=validate(rt,b,guard)
    targets={canonical(t):t for d in deps for t in d['lock_targets']}
    for ns,identity in [('resource.run',b['run_id']),('resource.admission',key),('resource.reference',b['binding']['country_reference']['reference_id'])]:
        t=_target(p,ns,identity);targets[canonical(t)]=t
    a=dict(contract=CONTRACT,owner='resource',owner_revision=subprocess.check_output(['git','rev-parse','HEAD'],text=True,cwd=Path(__file__).parents[4]).strip(),owner_binding=typed(b),binding_codec=CODEC,physical=p,
        validator={**rules,'validation_digest':digest(inventory)},inventory_digest=digest(inventory),entities=entities,dependencies=[d['admission_id'] for d in deps],
        lock_targets=sorted(targets.values(),key=lambda t:(t['stage'],t['system_identifier'],t['database_oid'],t['namespace'],t['key'])))
    a['admission_id']=digest(a);audit=dict(search=search,inventory=inventory,cost=cost)
    _binding(rt,b);_dependencies(rt,b,guard);upstream._entities_current(rt,entities,guard)
    if _physical(rt,b)!=p or _rules(rt)!=rules:raise ValueError('admit尾部身份变化')
    # 实际登记的短事务；依赖由其可信接口逐目标持锁，无新恢复/授权协议。
    from contextlib import ExitStack
    from data_pipeline.common.admission_locks import LockConnections
    with LockConnections() as connections, ExitStack() as stack:
        for target in a['lock_targets']:
            if target['namespace'] in OWN_NAMES:continue
            dep=next(d for d in deps if target in d['lock_targets'])
            dr=_upstream(rt,dep);lease=connections.borrow(dr,target,guard=guard)
            stack.enter_context(upstream.hold_lock(dr,dep,target,guard=guard,lock_connection=lease))
        with _pg(rt,True) as pg,pg.cursor() as c:
            _sql(rt,c,"SELECT set_config('lock_timeout',%s,true)",(str(rt.lock_timeout_ms)+'ms',))
            for target in a['lock_targets']:
                if target['namespace'] in ('resource.run','resource.reference'):_locked(rt,c,a,target,audit)
            _binding(rt,b);_dependencies(rt,b,guard);upstream._entities_current(rt,entities,guard)
            with _lake(rt) as db:
                if _descriptor(rt,db,b)!=inventory['descriptor']:raise ValueError('登记前目录变化')
            if _physical(rt,b)!=p or _country(rt,b)!=inventory['country'] or _rules(rt)!=rules:raise ValueError('登记前规则/实体身份变化')
            reuse=digest(dict(search=search,entities=entities,inventory=inventory))
            _sql(rt,c,'INSERT INTO '+TABLE+" VALUES (%s,%s,'accepted',%s,%s) ON CONFLICT(reuse_key) DO NOTHING",(key,reuse,Json(a),Json(audit)))
            row=_sql(rt,c,'SELECT state,admission FROM '+TABLE+' WHERE reuse_key=%s',(reuse,)).fetchone()
            if row is None or row[0]!='accepted':raise ValueError('已撤销记录不可复活')
            a=row[1]
    rt.event('admit_complete',**cost);return a


@dataclass
class ReadSession:
    iterator:object=None
    receipt:object=None
    exhausted:bool=False
    rows:int=0
    bytes:int=0
    hasher:object=field(default_factory=hashlib.sha256)
    def __iter__(self):return self
    def __next__(self):return next(self.iterator)
    def close(self):self.iterator.close()


@contextmanager
def open_reader(runtime,admission,request,*,guard):
    from data_pipeline.analysis.resources.publication_validation import selected_rows
    rt=runtime;guard=rt.budget(guard);request=untyped(typed(request));fields(request,REQUEST_FIELDS)
    if request['codec_version']!=CODEC or request['view'] not in ('metrics','normal_bands','topology_status','coverage'):raise ValueError('非有限Resource view/codec')
    for name in ('batch_rows','batch_bytes'):positive(request[name],name)
    if request['batch_rows']>rt.max_rows or request['batch_bytes']>rt.max_bytes:raise ValueError('批预算超过runtime')
    scope=untyped(request['scope_typed']);fields(scope,{'scope'})
    if scope['scope'] not in ('all','result'):raise ValueError('未知范围')
    verify_current(rt,admission,guard=guard);session=ReadSession();coverage=[]
    source=selected_rows(rt,untyped(admission['owner_binding']),request,coverage,guard)
    def batches():
        pending=[]
        for row in source:
            guard();data=(typed(row)+'\n').encode();single=typed([row]).encode()
            if len(single)>request['batch_bytes']:raise ValueError('单条编码超过来源请求封装')
            candidate=typed([*pending,row]).encode()
            if pending and (len(pending)>=request['batch_rows'] or len(candidate)>request['batch_bytes']):
                text=typed(pending);yield dict(rows_typed=text,codec_version=CODEC,rows=len(pending),bytes=len(text.encode()));pending=[]
            pending.append(row);session.rows+=1;session.bytes+=len(data);session.hasher.update(data)
            guard()
        if pending:
            text=typed(pending);yield dict(rows_typed=text,codec_version=CODEC,rows=len(pending),bytes=len(text.encode()))
        session.exhausted=True
    session.iterator=batches();error=None
    try:yield session
    except BaseException as exc:error=exc;raise
    finally:
        _close((session.iterator.close,source.close),error)
        if error is None and session.exhausted:
            verify_current(rt,admission,guard=guard)
            session.receipt=dict(contract='component-publication-read/v1',admission_id=admission['admission_id'],request_digest=digest(request),rows=session.rows,typed_digest=session.hasher.hexdigest(),execution='complete',coverage_ref=typed(coverage))
