"""canonical P0 的有限资源、目录及实体读取；不生产科学数据。"""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import os
import json
import re
import resource
import shutil
import tempfile

import psycopg2
from psycopg2.extensions import parse_dsn

from data_pipeline.bgp.archive import admission as upstream
from data_pipeline.bgp.replay.snapshot_contract import COLUMNS, TABLES, PROFILE, encode, decode, digest
from data_pipeline.bgp.replay.snapshot_validation import CODEC, validate_rules
from data_pipeline.bgp.archive.value_codec import fields, canonical, digest as scope_digest
from data_pipeline.bgp.archive.store import connect_duckdb, literal

# 区分未传预算与显式 None，保留人工模式原无效值拒绝。
_UNSET = object()

BUDGETS = ('memory_bytes', 'max_temp_bytes', 'max_rss_bytes', 'lock_timeout_ms',
           'max_batch_rows', 'max_batch_bytes', 'max_total_rows', 'max_total_bytes', 'min_free_bytes')


def positive(value, name):
    if type(value) is not int or value <= 0:
        raise ValueError('预算必须为正有限整数：' + name)


@dataclass
class Runtime:
    dsn: str
    allowed_roots: tuple
    scratch_root: Path
    dependency_admissions: tuple
    dependency_runtimes: dict
    fixture_only: bool = False
    audit_sink: object = None
    memory_bytes: int = _UNSET
    max_temp_bytes: int = _UNSET
    max_rss_bytes: int = _UNSET
    lock_timeout_ms: int = _UNSET
    max_batch_rows: int = _UNSET
    max_batch_bytes: int = _UNSET
    max_total_rows: int = _UNSET
    max_total_bytes: int = _UNSET
    min_free_bytes: int = _UNSET
    execution_profile: str | None = None
    input_manifest: dict | None = None
    expected_canonical_binding: dict | None = None
    output_root: Path | None = None

    def __post_init__(self):
        if type(self.fixture_only) is not bool:
            raise ValueError('必须显式选择人工或 real-candidate 模式')
        if self.fixture_only:
            if any(v is not None for v in (self.execution_profile,self.input_manifest,self.expected_canonical_binding,self.output_root)):
                raise ValueError('人工与真实候选固定参数不得混用')
            self._profile = 'synthetic-fixture/v1'
        elif self.execution_profile == 'real-candidate/v1':
            if type(self.input_manifest) is not dict or type(self.expected_canonical_binding) is not dict or self.output_root is None:
                raise ValueError('真实候选缺少原 manifest、完整 Canonical 绑定或输出根')
            self._profile = self.execution_profile
        else:
            raise ValueError('必须显式选择人工或 real-candidate/v1')
        self._mode = (self.fixture_only,self.execution_profile)
        defaults = dict(memory_bytes=256*1024**2,max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,
                        lock_timeout_ms=2000,max_batch_rows=10000,max_batch_bytes=4*1024**2,
                        max_total_rows=1000000,max_total_bytes=512*1024**2,min_free_bytes=1)
        for name,value in defaults.items():
            if getattr(self,name) is _UNSET:
                if not self.fixture_only: raise ValueError('真实候选缺少显式预算：'+name)
                setattr(self,name,value)
        roots = tuple(Path(p) for p in self.allowed_roots)
        self.allowed_roots = tuple(p.resolve(strict=True) for p in roots)
        if not self.fixture_only:
            for root in roots: self.path(root)
        self.scratch_root = self.path(self.scratch_root)
        if not self.scratch_root.is_dir(): raise ValueError('scratch 必须为既有隔离目录')
        self._scope_digest = None
        if not self.fixture_only:
            self.input_manifest = untyped(encode(self.input_manifest))
            self.expected_canonical_binding = binding(untyped(encode(self.expected_canonical_binding)))
            d = self.expected_canonical_binding['descriptor']
            validate_rules(d['plan'],profile=PROFILE,codec=d['codec'],tables=d['tables'])
            if self.input_manifest != d['plan']['input_manifest']:
                raise ValueError('真实候选原 manifest 与 Canonical 生产计划不符')
            self.output_root = self.path(self.output_root)
            if str(self.output_root) != d['control_root'] or not self.output_root.is_dir():
                raise ValueError('显式输出根必须精确绑定原控制目录')
            self._scope_digest = scope_digest([self.input_manifest,self.expected_canonical_binding])
            protected = [self.output_root, self.path(d['database']['data_root'].rstrip('/'))]
            protected.extend(self.path(e['path']).parent for e in [*self.input_manifest['inputs'],*self.input_manifest.get('references',[])])
            for root in protected:
                if root == self.scratch_root or root in self.scratch_root.parents or self.scratch_root in root.parents:
                    raise ValueError('scratch 与原输入/输出目录未隔离')
        self._configuration = self._runtime_configuration()
        self.check()

    def _runtime_configuration(self):
        # 连接与授权目录仅留在 Runtime，不写入 Admission 或 validator 摘要。
        return (self.dsn,tuple(map(str,self.allowed_roots)),str(self.scratch_root),str(self.output_root))

    def check_binding(self, value):
        self.check()
        if not self.fixture_only and value != self.expected_canonical_binding:
            raise ValueError('绑定超出真实候选固定 Canonical 范围')

    def path(self, path):
        p = Path(path); resolved = p.resolve(strict=True)
        if (str(p) != str(resolved) or not self.allowed_roots
                or not any(resolved == r or r in resolved.parents for r in self.allowed_roots)
                or any(x.is_symlink() for x in (p, *p.parents))):
            raise ValueError('canonical 实体必须位于显式允许根且为规范路径')
        return resolved

    def check(self):
        if type(self.fixture_only) is not bool or (self.fixture_only,self.execution_profile) != self._mode:
            raise ValueError('Canonical Runtime 模式漂移')
        if self._runtime_configuration() != self._configuration:
            raise ValueError('Canonical Runtime 连接或授权目录漂移')
        if not self.fixture_only and scope_digest([self.input_manifest,self.expected_canonical_binding]) != self._scope_digest:
            raise ValueError('Canonical Runtime 固定范围漂移')
        for name in BUDGETS: positive(getattr(self, name), name)
        self.path(self.scratch_root)
        if self.fixture_only: fixture_dsn(self, self.dsn)
        else:
            parsed=parse_dsn(self.dsn)
            if not (parsed.get('host') or parsed.get('hostaddr')) or not parsed.get('dbname') or parsed.get('service'):
                raise ValueError('真实候选必须显式 DSN 主机和数据库，不从 service 推导')
        disk_roots=[self.scratch_root,self.output_root]
        if not self.fixture_only: disk_roots.append(self.expected_canonical_binding['descriptor']['database']['data_root'].rstrip('/'))
        for root in disk_roots:
            if root is not None and shutil.disk_usage(self.path(root)).free < self.min_free_bytes:
                raise ValueError('Canonical 最低空闲盘保护')
        # ru_maxrss 是进程历史高水位，macOS 为字节，Linux 为 KiB；不是本操作增量。
        import sys
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)
        if peak > self.max_rss_bytes: raise ValueError('canonical 进程 RSS 高水位超过预算')

    def event(self, kind, **values):
        if self.audit_sink is not None: self.audit_sink(dict(kind=kind, **values))


def fixture_dsn(runtime, dsn):
    parsed = parse_dsn(dsn)
    host = parsed.get('host', '')
    if not host.startswith('/') or ',' in host or parsed.get('hostaddr') or parsed.get('service'):
        raise ValueError('本片 DSN 必须显式绑定允许根内的私有 Unix socket')
    runtime.path(host)


def dependency_check(runtime, owner_runtime=None):
    runtime.resource_guard()
    if owner_runtime is not None and (runtime.fixture_only,runtime.execution_profile) != owner_runtime._mode:
        raise ValueError('Canonical 与实际上游 Runtime 模式不符')
    for name in ('max_temp_bytes', 'lock_timeout_ms'):
        positive(getattr(runtime, name), '上游 ' + name)
    if not re.fullmatch(r'[1-9][0-9]*(?:B|KB|MB|GB)', runtime.memory_limit):
        raise ValueError('上游内存预算必须为正整数及有限字节单位')
    if runtime.fixture_only: fixture_dsn(runtime, runtime.dsn)
    runtime.path(runtime.scratch_root)


def untyped(text):
    if type(text) is not str: raise ValueError('必须提供原 codec 文本')
    value = decode(text)
    if encode(value) != text: raise ValueError('原 typed 文本必须为规范编码')
    return value


@contextmanager
def cleanup(actions):
    primary = None
    try: yield actions
    except BaseException as exc: primary = exc; raise
    finally:
        errors = []
        for action in reversed(actions):
            try: action()
            except BaseException as exc: errors.append(exc)
        if errors:
            if primary is not None and not isinstance(primary, GeneratorExit):
                primary.cleanup_errors = (*getattr(primary, 'cleanup_errors', ()), *errors)
            else:
                errors[0].cleanup_errors = (*getattr(errors[0], 'cleanup_errors', ()), *errors[1:])
                raise errors[0] from primary


@contextmanager
def pg(runtime, *, write=False, lock=False):
    runtime.check()
    with cleanup([]) as actions:
        conn = psycopg2.connect(runtime.dsn)
        actions.extend([conn.close, conn.rollback])
        conn.set_session(readonly=not (write or lock))
        yield conn
        if write: conn.commit()


def sql(runtime, cursor, query, args=()):
    runtime.check(); runtime.event('pg_sql', sql=query)
    cursor.execute(query, args)
    return cursor


def binding(value):
    fields(value, {'binding', 'descriptor'})
    b = value['binding']; fields(b, {'run_id', 'snapshot', 'seal_digest', 'profile'})
    if (b['profile'] != PROFILE or type(b['run_id']) is not str
            or re.fullmatch('[0-9a-f]{32}', b['run_id']) is None
            or type(b['snapshot']) is not int or b['snapshot'] < 0):
        raise ValueError('canonical 原固定绑定无效')
    fields(value['descriptor'], {'binding', 'database', 'schema', 'manifest_sha', 'control_root',
                                'control_entity', 'state', 'plan', 'codec', 'tables', 'files'})
    if value['descriptor']['binding'] != b: raise ValueError('原 binding/descriptor 不一致')
    return value


def metadata(runtime, b):
    """只取实际 PG 完成锚与固定快照目录；不读任何 Parquet 正文或控制文件正文。"""
    runtime.check_binding(b); binding(b); key = b['binding']; snapshot = key['snapshot']
    with pg(runtime) as conn, conn.cursor() as c:
        q = lambda query, args=(): sql(runtime, c, query, args)
        system, oid, name = q('SELECT system_identifier::text,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),current_database() FROM pg_control_system()').fetchone()
        options = q('SELECT key,value,scope FROM public.ducklake_metadata').fetchall()
        roots = [v for k, v, scope in options if k == 'data_path' and scope is None]
        if len(roots) != 1 or ('version', '0.3', None) not in options:
            raise ValueError('实际 DuckLake 根或格式不支持')
        root = str(runtime.path(roots[0].rstrip('/')))
        database = dict(system_identifier=system, database_oid=oid, database_name=name,
                        catalog='lake', catalog_backend='postgres', data_root=roots[0])
        row = q('SELECT state,profile,snapshot,seal,control_sha,plan,schema_name FROM m3_projection.runs WHERE run_id=%s', (key['run_id'],)).fetchone()
        if row is None or row[:3] != ('complete', PROFILE, snapshot):
            raise ValueError('原 canonical 完成登记缺失、撤销或错版')
        seal = row[3]
        validate_rules(seal.get('plan'), profile=seal.get('profile'), codec=seal.get('codec'), tables=seal.get('tables'))
        if (seal['digest'] != key['seal_digest'] or digest({k:v for k,v in seal.items() if k != 'digest'}) != seal['digest']
                or seal['run_id'] != key['run_id'] or seal['snapshot'] != snapshot
                or seal['schema'] != 'm3_' + key['run_id'] or row[6] != seal['schema']
                or row[5] != seal['plan'] or seal['database'] != database):
            raise ValueError('原 canonical 完整计划、封印或物理身份漂移')
        control = runtime.path(seal['control_root']); st = control.stat()
        if dict(device=st.st_dev, inode=st.st_ino) != seal['control_entity']:
            raise ValueError('原控制目录实体漂移')
        actual = dict(binding=key, database=database, schema=seal['schema'], manifest_sha=row[4],
                      control_root=str(control), control_entity=seal['control_entity'], state='complete',
                      plan=seal['plan'], codec=seal['codec'], tables=seal['tables'], files=seal['metrics']['parquet_files'])
        if actual != b['descriptor']: raise ValueError('完整原 descriptor 与实际登记不符')
        visible = lambda x: x['begin_snapshot'] <= snapshot and (x['end_snapshot'] is None or x['end_snapshot'] > snapshot)
        schemas = [r[0] for r in q('SELECT to_jsonb(s) FROM public.ducklake_schema s WHERE schema_name=%s', (seal['schema'],)).fetchall() if visible(r[0])]
        if len(schemas) != 1: raise ValueError('固定 schema 目录缺失或重复')
        schema = schemas[0]
        tables = [r[0] for r in q('SELECT to_jsonb(t) FROM public.ducklake_table t WHERE schema_id=%s', (schema['schema_id'],)).fetchall() if visible(r[0])]
        if len(tables) != len(TABLES) or {t['table_name'] for t in tables} != set(TABLES):
            raise ValueError('固定 13 表目录不完整')
        snapshots = q('SELECT to_jsonb(s) FROM public.ducklake_snapshot s WHERE snapshot_id=%s', (snapshot,)).fetchall()
        if len(snapshots) != 1: raise ValueError('实际固定快照登记缺失或重复')
        ids = [t['table_id'] for t in tables]
        meta = dict(schema=schemas, tables=sorted(tables, key=canonical), snapshot=snapshots[0][0])
        for table in ('ducklake_column', 'ducklake_data_file', 'ducklake_delete_file', 'ducklake_inlined_data_tables', 'ducklake_partition_info'):
            values = [r[0] for r in q(f'SELECT to_jsonb(t) FROM public.{table} t WHERE table_id=ANY(%s)', (ids,)).fetchall()]
            meta[table] = sorted([x for x in values if 'begin_snapshot' not in x or visible(x)], key=canonical)
    if any(meta[t] for t in ('ducklake_delete_file', 'ducklake_inlined_data_tables', 'ducklake_partition_info')):
        raise ValueError('本片不支持删除、inline 或分区布局')
    if schema['path'] != seal['schema'] + '/' or not schema['path_is_relative']:
        raise ValueError('schema 物理路径不符')
    files = []
    for table in tables:
        if table['path'] != table['table_name'] + '/' or not table['path_is_relative']:
            raise ValueError('表物理路径不符')
        columns = sorted((x for x in meta['ducklake_column'] if x['table_id'] == table['table_id']), key=lambda x:x['column_order'])
        if [(x['column_name'],x['column_type'],x['parent_column']) for x in columns] != [(n,'int64' if n == 'seq' else 'varchar',None) for n in COLUMNS]:
            raise ValueError('目录列类型或顺序不符')
        for f in meta['ducklake_data_file']:
            if f['table_id'] != table['table_id']: continue
            if (not f['path_is_relative'] or f['file_format'] != 'parquet' or any(f.get(k) is not None for k in ('mapping_id','partition_id','encryption_key','partial_file_info'))):
                raise ValueError('本片不支持该文件布局')
            path = runtime.path(Path(root) / schema['path'] / table['path'] / f['path'])
            files.append(dict(table=table['table_name'], path=str(path), bytes=f['file_size_bytes']))
    expected = [{k:f[k] for k in ('table','path','bytes')} for f in actual['files']]
    if sorted(files,key=canonical) != sorted(expected,key=canonical): raise ValueError('固定快照实际文件集合不符')
    physical = dict(system_identifier=system, database_oid=oid, catalog='lake', schema=seal['schema'], root=root, snapshot=snapshot)
    runtime.event('canonical_metadata', tables=len(tables), files=len(files))
    return physical, meta


def entities(runtime, descriptor, guard):
    known = {f['path']:f['sha256'] for f in descriptor['files']}
    if len(known) != len(descriptor['files']): raise ValueError('重复 canonical 输出实体')
    known[str(Path(descriptor['control_root']) / 'execution.json')] = descriptor['manifest_sha']
    return [{**upstream._entity(runtime, p, guard=guard), 'sha256':sha} for p, sha in sorted(known.items())]


def entities_current(runtime, values, guard):
    upstream._entities_current(runtime, values, guard)


@contextmanager
def scratch(runtime, guard):
    runtime.check()
    path = Path(tempfile.mkdtemp(prefix='canonical-p0-', dir=runtime.scratch_root))
    def remove():
        runtime.path(path)  # 不沿被重指向的 scratch/子目录删除其他目录。
        shutil.rmtree(path)
    with cleanup([remove]):
        runtime.path(path)
        def checked():
            guard(); runtime.check(); runtime.path(path)
            size = sum(p.stat().st_size for p in path.rglob('*') if p.is_file())
            if size > runtime.max_temp_bytes: raise ValueError('canonical 临时磁盘超过预算')
        yield path, checked


def configure(db, runtime, path):
    runtime.check(); runtime.path(path)
    db.execute('SET memory_limit=' + literal(str(runtime.memory_bytes) + 'B'))
    db.execute('SET max_temp_directory_size=' + literal(str(runtime.max_temp_bytes) + 'B'))
    db.execute('SET temp_directory=' + literal(path))
    db.execute('SET threads=1')


@contextmanager
def data_connection(runtime, path):
    with cleanup([]) as actions:
        db = connect_duckdb(); actions.append(db.close)
        configure(db, runtime, path)
        db.execute('LOAD ducklake'); db.execute('LOAD postgres')
        check_extensions(db)
        db.execute('ATTACH ' + literal('ducklake:postgres:' + runtime.dsn) + ' AS lake (READ_ONLY)')
        yield db


def check_extensions(db):
    versions = dict(db.execute('SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded').fetchall())
    if versions.get('ducklake') != '3f1b372' or versions.get('postgres_scanner') != 'b9fce43':
        raise ValueError('读取或验收扩展版本不符')
