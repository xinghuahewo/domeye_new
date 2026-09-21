"""Resource隔离离线存储；独立数据集/完成身份，复用已冻结观察读取。"""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import uuid

import duckdb
import pyarrow as pa
import psycopg2
from psycopg2.extras import execute_values, Json

from data_pipeline.bgp.archive.store import literal, connect_duckdb
from data_pipeline.analysis.resources.compute import RULE, METRIC_UNITS
from data_pipeline.analysis.resources.identity import execution_identity, identity_digest, project_module_sources

SCHEMA_VERSION = 'resource/v3'
METRICS = tuple(METRIC_UNITS)
TABLES = {
    'sources': [('source_id','VARCHAR'),('content_sha256','VARCHAR'),('origin_uri','VARCHAR'),('collector','VARCHAR'),('snapshot_time','TIMESTAMPTZ'),
        ('reference_id','VARCHAR'),('time_basis','VARCHAR'),('legacy_time_text','VARCHAR'),
        ('legacy_time_basis','VARCHAR'),('path_rendering_version','VARCHAR'),
        ('min_epoch','BIGINT'),('max_epoch','BIGINT'),('element_count','BIGINT'),
        ('upstream_run','VARCHAR'),('upstream_snapshot','BIGINT'),('purpose','VARCHAR'),('decoder_version','VARCHAR')],
    'metrics': [('source_id','VARCHAR'),('dimension','VARCHAR'),('bucket','VARCHAR'),('time','TIMESTAMPTZ')]
        + [(n,'BIGINT') for n in METRICS] + [('is_outlier','BOOLEAN'),('as_name','VARCHAR'),
          ('as_rank','BIGINT'),('reference_row_ref','VARCHAR'),('normal_state','VARCHAR'),('membership_ref','VARCHAR'),('ipv6_cidr_count','BIGINT')],
    'memberships': [('source_id','VARCHAR'),('dimension','VARCHAR'),('bucket','VARCHAR'),('kind','VARCHAR'),('member','VARCHAR')],
    'rendered_paths': [('path_digest','VARCHAR'),('path_text','VARCHAR')],
    'normal_bands': [('source_id','VARCHAR'),('dimension','VARCHAR'),('bucket','VARCHAR'),('metric','VARCHAR'),
        ('list_len','BIGINT'),('upper_bound','BIGINT'),('lower_bound','BIGINT'),('mean','DOUBLE'),('population_std','DOUBLE')],
    'normal_samples': [('source_id','VARCHAR'),('dimension','VARCHAR'),('bucket','VARCHAR'),('metric','VARCHAR'),
        ('sample_source','VARCHAR'),('sample_time','TIMESTAMPTZ'),('value','BIGINT')],
    'topology_edges': [('source_id','VARCHAR'),('country_cn','VARCHAR'),('a_asn','DECIMAL(38,0)'),('b_asn','DECIMAL(38,0)'),('weight','BIGINT'),('country_scope','VARCHAR')],
    'topology_status': [('source_id','VARCHAR'),('country_cn','VARCHAR'),('build_time','TIMESTAMPTZ'),
        ('status','VARCHAR'),('node_count','BIGINT'),('edge_count','BIGINT'),('legacy_replace_edges','BOOLEAN'),
        ('legacy_update_snapshot','BOOLEAN'),('link_color','VARCHAR'),('node_color','VARCHAR'),('country_scope','VARCHAR')],
    'decoding_differences': [('source_id','VARCHAR'),('event_id','VARCHAR'),('detail','VARCHAR')],
    'decision_refs': [('source_id','VARCHAR'),('event_id','VARCHAR'),('status','VARCHAR')],
}
TYPES = {'VARCHAR':pa.string(),'BIGINT':pa.int64(),'DECIMAL(38,0)':pa.decimal128(38,0),
         'TIMESTAMPTZ':pa.timestamp('us',tz='UTC'),'BOOLEAN':pa.bool_(),'DOUBLE':pa.float64()}


def completed_binding(dsn, run):
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute("SELECT schema_name,snapshot FROM domeye.runs WHERE run_id=%s AND state='complete'", (run,))
            row = c.fetchone()
    if row is None:
        raise ValueError('上游未完成')
    return row


class ResourceStore:
    tables = TABLES
    schema_version = SCHEMA_VERSION

    def upstream_binding(self, dsn, run):
        return completed_binding(dsn, run)

    def dataset_identifier(self, source_rows, topology_modes, snapshot):
        return hashlib.sha256(json.dumps([self.schema_version,RULE,self.code_digest,self.upstream_run,
            self.upstream_snapshot,self.binding_manifest,source_rows,topology_modes],default=str).encode()).hexdigest()

    def __init__(self, dsn, output, upstream_run, sources, *, batch_rows=10000, expected_code_identity=None, fixture_only=False,
                 binding_manifest=None, validate_upstreams=lambda:None):
        if not fixture_only and binding_manifest is None:
            raise ValueError('正式Resource存储必须绑定多源清单与结果窗口')
        self.fixture_only=fixture_only
        initial_identity=execution_identity(fixture_only=fixture_only)
        self.binding_manifest=binding_manifest
        self.validate_upstreams=validate_upstreams
        self.source_bindings={s["context"]["source_id"]:s for s in (binding_manifest or {}).get("sources",[])}
        self.dsn = dsn
        self.root = Path(output)
        self.root.mkdir(parents=True, exist_ok=False)
        self.upstream_run = upstream_run
        _, self.upstream_snapshot = self.upstream_binding(dsn, upstream_run)
        self.run_id = uuid.uuid4().hex
        self.schema = 'resource_' + self.run_id
        self.sources = tuple(sources)
        if not self.sources or len(set(self.sources)) != len(self.sources):
            raise ValueError('RIB来源为空或重复')
        self.pending = {name: [] for name in self.tables}
        self.batch_rows = batch_rows
        self.pending_count = 0
        self.db = connect_duckdb(str(self.root/'resource-staging.duckdb'))
        self.db.execute("SET memory_limit='512MB'")
        self.db.execute('SET threads=2')
        self.db.execute('LOAD ducklake')
        self.db.execute('LOAD postgres')
        self.db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake')
        self.db.execute('CREATE SCHEMA lake.'+self.schema)
        self.db.execute('CREATE TABLE path_keys (path_digest VARCHAR PRIMARY KEY)')
        for name, columns in self.tables.items():
            self.db.execute(f'CREATE TABLE lake.{self.schema}.{name} ('+', '.join('"'+n+'" '+t for n,t in columns)+')')
        self.pg = psycopg2.connect(dsn)
        with self.pg,self.pg.cursor() as c:
            c.execute("SELECT value FROM public.ducklake_metadata WHERE key='data_path'")
            data_path=c.fetchone()[0]
            c.execute('SELECT path,path_is_relative FROM public.ducklake_schema WHERE schema_name=%s AND end_snapshot IS NULL',(self.schema,))
            schema_path,is_relative=c.fetchone()
        self.storage_layout={'catalog_data_path':data_path,'schema_path':schema_path,'path_is_relative':is_relative,
                             'local_output':str(self.root.resolve())}
        self.saved_state = False
        self.member_sizes = {}
        self.finished = False
        self.code_identity = expected_code_identity or initial_identity
        if execution_identity(fixture_only=self.fixture_only)!=self.code_identity:
            raise ValueError('Resource依赖在参考投影期间变化')
        self.code_identity = {**self.code_identity, 'extensions': self._extensions()}
        self.code_digest = identity_digest(self.code_identity)
        with (self.root/'code-identity.json').open('x') as identity_file:
            json.dump(self.code_identity,identity_file,ensure_ascii=False,indent=2,sort_keys=True)
        with self.pg, self.pg.cursor() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.resource_runs (
                run_id TEXT PRIMARY KEY, schema_name TEXT, state TEXT, upstream_run TEXT,
                upstream_snapshot BIGINT, rule_version TEXT, schema_version TEXT, code_digest TEXT,
                snapshot BIGINT, dataset_id TEXT, reason TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.resource_work_meta (
                run_id TEXT PRIMARY KEY, rib_count BIGINT, last_source TEXT, initial_state_basis TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.resource_work_metrics (
                run_id TEXT, source_id TEXT, dimension TEXT, bucket TEXT, time TIMESTAMPTZ,
                ipv4_prefix_count BIGINT, ipv6_prefix_count BIGINT, private_as_count BIGINT,
                path_count BIGINT, public_as_count BIGINT, is_outlier BOOLEAN, membership_ref TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.resource_work_abnormal (
                run_id TEXT, bucket TEXT, currently_abnormal BOOLEAN)''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.resource_work_bands (
                run_id TEXT, dimension TEXT, bucket TEXT, metric TEXT, list_len BIGINT,
                upper_bound BIGINT, lower_bound BIGINT, mean DOUBLE PRECISION, population_std DOUBLE PRECISION)''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.resource_work_samples (
                run_id TEXT, dimension TEXT, bucket TEXT, metric TEXT, source_id TEXT, time TIMESTAMPTZ, value BIGINT)''')
            c.execute('ALTER TABLE domeye.resource_runs ADD COLUMN IF NOT EXISTS execution_mode TEXT')
            c.execute('ALTER TABLE domeye.resource_runs ADD COLUMN IF NOT EXISTS binding_manifest JSONB')
            c.execute('''INSERT INTO domeye.resource_runs
                (run_id,schema_name,state,upstream_run,upstream_snapshot,rule_version,schema_version,code_digest,execution_mode,binding_manifest)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (self.run_id,self.schema,'candidate',upstream_run,self.upstream_snapshot,RULE,self.schema_version,self.code_digest,
                 self.code_identity['execution_mode'],Json(binding_manifest)))

    def _extensions(self):
        return dict(self.db.execute("SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded ORDER BY extension_name").fetchall())

    def append(self, table, row):
        if self.finished:
            raise ValueError('已终结结果不可改写')
        self.pending[table].append(row)
        self.pending_count += 1
        if self.pending_count >= self.batch_rows:
            self.flush()

    def flush(self):
        # 本地去重索引在湖事务之外；首轮失败终结，不执行恢复/重试。
        if self.pending['rendered_paths']:
            self.db.register('path_candidates', pa.Table.from_pylist(self.pending['rendered_paths']))
            self.db.execute('CREATE TEMP TABLE unseen AS SELECT DISTINCT b.* FROM path_candidates b LEFT JOIN path_keys k USING(path_digest) WHERE k.path_digest IS NULL')
            self.db.execute('INSERT INTO path_keys SELECT path_digest FROM unseen')
            self.db.unregister('path_candidates')
        self.db.execute('BEGIN')
        try:
            for name, rows in self.pending.items():
                if not rows:
                    continue
                schema = pa.schema([(n,TYPES[t]) for n,t in self.tables[name]])
                self.db.register('resource_batch', pa.Table.from_pylist(rows, schema=schema))
                if name == 'rendered_paths':
                    self.db.execute(f'INSERT INTO lake.{self.schema}.{name} SELECT * FROM unseen')
                else:
                    self.db.execute(f'INSERT INTO lake.{self.schema}.{name} SELECT * FROM resource_batch')
                self.db.unregister('resource_batch')
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise
        if self.pending['rendered_paths']:
            self.db.execute('DROP TABLE unseen')
        for rows in self.pending.values():
            rows.clear()
        self.pending_count = 0

    def decision(self, decision):
        e = decision.observation
        self.append('decision_refs', {'source_id':e.source_id,'event_id':f'{e.message_id}:{e.ordinal}', 'status':decision.resource_status})

    def members(self, row):
        for kind in ('ipv4_prefix','ipv6_prefix','vp_set','private_as','public_as','path'):
            self.member_sizes[(row.source_id,row.dimension,row.bucket,kind)]=len(getattr(row,kind))
            for member in getattr(row, kind):
                if kind == 'path':
                    digest = hashlib.sha256(member.encode()).hexdigest()
                    self.append('rendered_paths', {'path_digest':digest,'path_text':member})
                    member = digest
                self.append('memberships', {'source_id':row.source_id,'dimension':row.dimension,
                    'bucket':row.bucket,'kind':kind,'member':member})
        return f'{self.run_id}/{row.source_id}/{row.dimension}/{row.bucket}'

    def result(self, result, observed_range):
        context = result.context
        bound=self.source_bindings.get(context.source_id,{})
        self.append('sources', {**asdict(context),'upstream_run':bound.get('run_id',self.upstream_run),
            'upstream_snapshot':bound.get('snapshot',self.upstream_snapshot),'purpose':bound.get('purpose','result'),
            'decoder_version':(self.binding_manifest or {}).get('decoder_version','legacy-fixture'),
            'min_epoch':observed_range[0],
            'max_epoch':observed_range[1],'element_count':observed_range[2]})
        for row in (*result.rows,*result.vp_rows):
            ranges = result.state.normal_range if row.dimension == 'first_path_asn' else result.state.vp_normal_range
            bands = ranges[row.bucket]
            values=asdict(row)
            if row.dimension=='peer_asn':
                for metric in METRICS:
                    if metric not in ('ipv4_prefix_count','ipv6_prefix_count','ipv6_48_count'):
                        values[metric]=None
            self.append('metrics', {**values,'ipv6_cidr_count':self.member_sizes[(row.source_id,row.dimension,row.bucket,'ipv6_prefix')],'normal_state':'available' if all(b.upper_bound is not None for b in bands.values()) else 'insufficient_normal_samples'})
            for metric, band in bands.items():
                key = {'source_id':context.source_id,'dimension':row.dimension,'bucket':row.bucket,'metric':metric}
                self.append('normal_bands', {**key,**asdict(band)})
                for sample in band.samples:
                    self.append('normal_samples', {**key,'sample_source':sample.source_id,'sample_time':sample.time,'value':sample.value})
        for graph in result.topology:
            self.append('topology_status', {**{n:getattr(graph,n,None) for n,_ in self.tables['topology_status']},'country_scope':'legacy_unknown' if graph.country_cn=='未知' else 'unknown' if graph.country_cn is None else 'reference_label','source_id':context.source_id,'node_count':len(graph.nodes),'edge_count':len(graph.edges)})
            for a,b in graph.edges:
                self.append('topology_edges', {'source_id':context.source_id,'country_cn':graph.country_cn,'a_asn':a,'b_asn':b,'weight':graph.weight,'country_scope':'legacy_unknown' if graph.country_cn=='未知' else 'unknown' if graph.country_cn is None else 'reference_label'})

    def state(self, state):
        if self.finished:
            raise ValueError('已终结工作态不可改写')
        with self.pg, self.pg.cursor() as c:
            for suffix in ('meta','metrics','abnormal','bands','samples'):
                c.execute('DELETE FROM domeye.resource_work_'+suffix+' WHERE run_id=%s',(self.run_id,))
            c.execute('INSERT INTO domeye.resource_work_meta VALUES (%s,%s,%s,%s)',
                (self.run_id,state.rib_count,state.last_context.source_id,state.initial_state_basis))
            for dimension,store in (('first_path_asn',state.prefix_count_dict),('peer_asn',state.vp_prefix_count_dict)):
                execute_values(c,'INSERT INTO domeye.resource_work_metrics VALUES %s',
                    ((self.run_id,row.source_id,dimension,row.bucket,row.time,row.ipv4_prefix_count,row.ipv6_prefix_count,
                      row.private_as_count,row.path_count,row.public_as_count,row.is_outlier,row.membership_ref)
                     for history in store.values() for row in history.values()),page_size=1000)
            execute_values(c,'INSERT INTO domeye.resource_work_abnormal VALUES %s',
                ((self.run_id,b,v) for b,v in state.currently_abnormal.items()))
            for dimension,ranges in (('first_path_asn',state.normal_range),('peer_asn',state.vp_normal_range)):
                execute_values(c,'INSERT INTO domeye.resource_work_bands VALUES %s',
                    ((self.run_id,dimension,b,m,v.list_len,v.upper_bound,v.lower_bound,v.mean,v.population_std)
                     for b,metrics in ranges.items() for m,v in metrics.items()),page_size=1000)
                execute_values(c,'INSERT INTO domeye.resource_work_samples VALUES %s',
                    ((self.run_id,dimension,b,m,s.source_id,s.time,s.value)
                     for b,metrics in ranges.items() for m,v in metrics.items() for s in v.samples),page_size=1000)
        self.saved_state = True

    def finish(self, report_details=None):
        if self.finished or not self.saved_state:
            raise ValueError('结果已终结或工作态未保存')
        self.flush()
        prefix = f'lake.{self.schema}'
        rows = self.db.execute(f'SELECT source_id FROM {prefix}.sources').fetchall()
        if len(rows)!=len(self.sources) or {r[0] for r in rows}!=set(self.sources):
            raise ValueError('RIB结果缺失或重复')
        for source in self.sources:
            expected = self.db.execute(f'SELECT element_count FROM {prefix}.sources WHERE source_id=?',[source]).fetchone()[0]
            count,distinct = self.db.execute(f'SELECT count(*),count(DISTINCT event_id) FROM {prefix}.decision_refs WHERE source_id=?',[source]).fetchone()
            globals_count = self.db.execute(f"SELECT count(*) FROM {prefix}.metrics WHERE source_id=? AND dimension='first_path_asn' AND bucket='global'",[source]).fetchone()[0]
            if count!=expected or distinct!=expected or globals_count!=1:
                raise ValueError('元素决策或global结果不完整')
        for kind,metric in (('ipv4_prefix','ipv4_prefix_count'),('ipv6_prefix','ipv6_cidr_count'),
                ('vp_set','vp_count'),('private_as','private_as_count'),('public_as','public_as_count'),('path','path_count')):
            mismatch=self.db.execute(f'''SELECT count(*) FROM {prefix}.metrics m LEFT JOIN
                (SELECT source_id,dimension,bucket,count(*) n,count(DISTINCT member) d FROM {prefix}.memberships
                 WHERE kind=? GROUP BY ALL) s USING(source_id,dimension,bucket)
                WHERE m.{metric} IS NOT NULL AND (m.{metric}!=coalesce(s.n,0) OR coalesce(s.n,0)!=coalesce(s.d,0))''',[kind]).fetchone()[0]
            if mismatch:
                raise ValueError('成员集合与计数不一致：'+kind)
        orphan=self.db.execute(f'''SELECT count(*) FROM {prefix}.memberships m LEFT JOIN
            {prefix}.rendered_paths p ON m.member=p.path_digest WHERE m.kind='path' AND p.path_digest IS NULL''').fetchone()[0]
        if orphan:
            raise ValueError('路径成员引用缺失')
        self.validate_upstreams()
        code_digest = identity_digest({**execution_identity(fixture_only=self.fixture_only), 'extensions': self._extensions()})
        if code_digest!=self.code_digest:
            raise ValueError('Resource计算依赖或运行环境在运行期间变化')
        source_rows=self.db.execute(f'SELECT * FROM {prefix}.sources ORDER BY snapshot_time').fetchall()
        topology_modes=self.db.execute(f'SELECT source_id,status FROM {prefix}.topology_status ORDER BY source_id,status').fetchall()
        snapshot = self.db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        dataset_id = self.dataset_identifier(source_rows, topology_modes, snapshot)
        counts={table:self.db.execute(f'SELECT count(*) FROM {prefix}.{table}').fetchone()[0] for table in self.tables}
        report={'run_id':self.run_id,'dataset_id':dataset_id,'snapshot':snapshot,'state':'complete',
                'storage_layout':self.storage_layout,'binding_manifest':self.binding_manifest,'schema_version':self.schema_version,'upstream_run':self.upstream_run,
                'upstream_snapshot':self.upstream_snapshot,'actual_rows':counts,'code_identity':self.code_identity,'code_digest':self.code_digest,**(report_details or {})}
        if not self.fixture_only:
            report['project_module_sources']=project_module_sources(self.code_identity)
        # 必需回执先持久化；只有随后PG完成登记成功才允许读取，孤立回执不可消费。
        with (self.root/'execution.json').open('x') as receipt:
            json.dump({**report,'state':'ready'},receipt,ensure_ascii=False,indent=2)
            receipt.flush()
            import os
            os.fsync(receipt.fileno())
        with self.pg,self.pg.cursor() as c:
            c.execute("UPDATE domeye.resource_runs SET state='complete',snapshot=%s,dataset_id=%s WHERE run_id=%s AND state='candidate'",(snapshot,dataset_id,self.run_id))
            if c.rowcount!=1:
                raise ValueError('运行不再是候选')
        self.finished=True
        return report

    def fail(self, reason):
        self.finished=True
        try:
            self.pg.rollback()
            with self.pg,self.pg.cursor() as c:
                c.execute("UPDATE domeye.resource_runs SET state='failed',reason=%s WHERE run_id=%s AND state='candidate'",(str(reason),self.run_id))
                c.execute('SELECT state FROM domeye.resource_runs WHERE run_id=%s',(self.run_id,))
                return c.fetchone()[0]
        except Exception:
            try:
                with psycopg2.connect(self.dsn) as pg,pg.cursor() as c:
                    c.execute('SELECT state FROM domeye.resource_runs WHERE run_id=%s',(self.run_id,))
                    return c.fetchone()[0]
            except Exception:
                return 'unknown'

    def close(self):
        self.db.close()
        self.pg.close()


def scan_resource(dsn, run, table, batch_size=10000, *, allow_fixture=False, scope='result'):
    if scope not in ('result','all'):raise ValueError('未知读取范围')
    if table not in TABLES or not 1<=batch_size<=100000:
        raise ValueError('数据集或批大小无效')
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute("SELECT schema_name,snapshot,execution_mode,schema_version FROM domeye.resource_runs WHERE run_id=%s AND state='complete'",(run,))
            row=c.fetchone()
    if row is None or row[0]!='resource_'+run or not run.isalnum():
        raise ValueError('Resource版本未完成或身份无效')
    if row[2]!='frozen-fresh-process' and not (allow_fixture and row[2]=='synthetic-fixture-api'):
        raise ValueError('Resource结果未经正式新进程入口；fixture读取须显式声明')
    if row[3]=='resource-observation/v1':raise ValueError('新profile须使用ResourceObservationReader读取原值与资格')
    db=connect_duckdb()
    try:
        db.execute('LOAD ducklake');db.execute('LOAD postgres')
        db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
        base=f'lake.{row[0]}';version=f'AT (VERSION => {int(row[1])})'
        query=f'SELECT t.* FROM {base}.{table} t {version}'
        if scope=='result':
            selected=f"SELECT source_id FROM {base}.sources {version} WHERE purpose='result'"
            if table=='rendered_paths':
                query+=f" WHERE path_digest IN (SELECT member FROM {base}.memberships {version} WHERE kind='path' AND source_id IN ({selected}))"
            else:query+=f' WHERE t.source_id IN ({selected})'
        yield from db.execute(query).fetch_record_batch(batch_size)
    finally:
        db.close()
