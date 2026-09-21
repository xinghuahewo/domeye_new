"""固定C3组件Reader。早停close无ReadReceipt；单页不冒充全正文审计。"""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
import resource
import sys
import tempfile

import psycopg2

from data_pipeline.analysis.country_events.snapshot_store import ComponentBinding, database_identity, upstream_binding, lake_connect, file_hash, LIMITS, KIND
from data_pipeline.analysis.country_events.snapshot_schema import SCHEMA_VERSION, SCHEMAS, TABLES, encode, decode, row_decode, packed
from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_audit as component_audit


@dataclass(frozen=True)
class ReadBatch:
    rows: tuple


@dataclass(frozen=True)
class ReadReceipt:
    binding: ComponentBinding
    mode: str
    rows: int
    scope: object
    full_body_validated: bool


@dataclass(frozen=True)
class Page:
    rows: tuple
    next_sequence: object
    receipt: ReadReceipt


class ComponentReader:
    tables, schemas, schema_version, kind = TABLES, SCHEMAS, SCHEMA_VERSION, KIND
    decode_row = staticmethod(row_decode)
    lake_connection = staticmethod(lake_connect)
    temporary_root = None

    def upstream(self):
        return upstream_binding(self.c1)

    def validate(self,index,completion):
        return component_audit.validate(index,completion,None,self.guard,CounterLike(),source_checks=False)

    def __init__(self,dsn,binding,*,c1=None,batch_rows=256,batch_bytes=4*1024**2,max_row_bytes=4*1024**2,max_rows=10000000,max_rss_bytes=2*1024**3,max_disk_bytes=8*1024**3,guard=lambda:None):
        self.dsn,self.binding,self.c1=dsn,binding,c1
        self.batch_rows,self.batch_bytes,self.max_row_bytes=batch_rows,batch_bytes,max_row_bytes
        if min(batch_rows,batch_bytes,max_row_bytes)<1 or batch_rows>10000 or type(max_rows) is not int or max_rows<1:raise ValueError('invalid_component_read_limit')
        self.external_guard=guard;self.root=Path(binding.root);self.workdir=None
        self.max_rows,self.max_rss_bytes,self.max_disk_bytes=max_rows,max_rss_bytes,max_disk_bytes
        self.manifest=self.check()

    def guard(self):
        self.external_guard()
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        size=sum(p.stat().st_size for root in (self.root,self.workdir) if root is not None for p in Path(root).rglob('*') if p.is_file())
        if rss>self.max_rss_bytes or size>self.max_disk_bytes:raise ValueError('resource_limit:C3_read_working_set')

    def check(self):
        self.guard();b=self.binding
        if b.schema_version!=self.schema_version or b.schema_name!='country_'+b.component_id or not b.component_id.isalnum():raise ValueError('C3_binding_schema_mismatch')
        if database_identity(self.dsn)!={'system_id':b.system_id,'database_oid':b.database_oid}:raise ValueError('C3_catalog_database_mismatch')
        if file_hash(self.root/'manifest.json',self.guard)!=b.manifest_sha256:raise ValueError('C3_manifest_digest_mismatch')
        manifest=json.loads((self.root/'manifest.json').read_text())
        expected=dict(system_id=b.system_id,database_oid=b.database_oid,catalog_id=b.catalog_id)
        if (manifest['kind'],manifest['schema_version'],manifest['identity'],manifest['component_id'],manifest['schema_name'],manifest['snapshot'],manifest['root'])!=(self.kind,self.schema_version,expected,b.component_id,b.schema_name,b.snapshot,b.root):raise ValueError('C3_manifest_binding_mismatch')
        if packed(manifest['schemas'])!=packed(self.schemas) or set(manifest['tables'])!=set(self.tables):raise ValueError('C3_table_set_mismatch')
        ready=json.loads((self.root/'ready.json').read_text())
        if ready!=dict(component_id=b.component_id,manifest_sha256=b.manifest_sha256):raise ValueError('C3_not_ready')
        with psycopg2.connect(self.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as cur:
                cur.execute('SELECT catalog_id FROM country_components.catalog WHERE singleton')
                if cur.fetchone()!=(b.catalog_id,):raise ValueError('C3_catalog_id_mismatch')
                cur.execute('SELECT schema_name,state,snapshot,root,manifest_sha256,scope,availability FROM country_components.components WHERE component_id=%s',(b.component_id,))
                if cur.fetchone()!=(b.schema_name,'complete',b.snapshot,b.root,b.manifest_sha256,encode(tuple(manifest['scope'])),manifest['availability']):raise ValueError('C3_not_complete')
            with pg.cursor(name='c3_read_index') as cur:
                cur.execute('SELECT incident_id,revision,country,state,cohort_id,sequence,row_hash FROM country_components.event_index WHERE component_id=%s ORDER BY sequence',(b.component_id,))
                # E索引也按批读取，Reader不缓存所有事件。
                index_digest=hashlib.sha256();count=0
                while rows:=cur.fetchmany(1):
                    for row in rows:
                        self.guard();payload=encode(tuple(row)).encode()
                        if len(payload)>self.batch_bytes:raise ValueError('resource_limit:C3_read_index')
                        index_digest.update(payload);count+=1
                if manifest.get('event_index')!=dict(rows=count,sha256=index_digest.hexdigest()):raise ValueError('C3_event_index_mismatch')
        if encode(self.upstream())!=manifest['upstream']:raise ValueError('C3_upstream_binding_mismatch')
        actual={str(p.relative_to(self.root)) for p in (self.root/'data').glob('*.parquet')}
        if actual!={f['path'] for f in manifest['files']}:raise ValueError('C3_file_set_mismatch')
        for file in manifest['files']:
            path=self.root/file['path']
            if path.resolve().parent!=self.root.resolve()/'data':raise ValueError('C3_file_scope_mismatch')
            stat=path.stat()
            if (stat.st_size,stat.st_mtime_ns)!=(file['size'],file['mtime_ns']):raise ValueError('C3_file_identity_mismatch')
        return manifest

    def connect(self):
        self.check();db=self.lake_connection(self.dsn);b=self.binding
        try:
            # 实际固定湖schema/列及count验证不复扫所有正文，COUNT由湖元数据/文件统计处理。
            for table in self.tables:
                linked=db.execute("SELECT data_file,data_file_size_bytes,delete_file FROM ducklake_list_files('lake',?,schema=>?,snapshot_version=>?)",[table,b.schema_name,b.snapshot]).fetchall()
                if any(d is not None or not Path(p).is_relative_to(self.root) for p,n,d in linked):raise ValueError('C3_lake_file_scope')
                if [[str(Path(p).relative_to(self.root)),n] for p,n,d in linked]!=self.manifest['lake_files'][table]:raise ValueError('C3_lake_file_binding_mismatch')
                cols=db.execute(f'DESCRIBE SELECT * FROM lake.{b.schema_name}.{table} AT (VERSION => {b.snapshot})').fetchall()
                if [(r[0],r[1]) for r in cols]!=[(n,t) for n,t,_ in self.schemas[table]]:raise ValueError('C3_actual_schema_mismatch')
                count=db.execute(f'SELECT count(*) FROM lake.{b.schema_name}.{table} AT (VERSION => {b.snapshot})').fetchone()[0]
                if count!=self.manifest['tables'][table]['rows']:raise ValueError('C3_actual_count_mismatch')
            return db
        except BaseException:db.close();raise

    def fetch_rows(self,table):
        width=max(1,self.manifest['tables'][table]['max_encoded_bytes'])
        return min(self.batch_rows,max(1,self.batch_bytes//width))

    def page(self,table,*,incident_id=None,after_sequence=-1,limit=256):
        if table not in self.tables or type(limit) is not int or not 1<=limit<=10000 or type(after_sequence) is not int:raise ValueError('C3_page_scope')
        if incident_id is not None:
            with psycopg2.connect(self.dsn) as pg:
                pg.set_session(readonly=True)
                with pg.cursor() as cur:
                    cur.execute('SELECT sequence FROM country_components.event_index WHERE component_id=%s AND incident_id=%s',(self.binding.component_id,incident_id))
                    if cur.fetchone() is None:raise ValueError('C3_unknown_incident')
        db=self.connect()
        try:
            b=self.binding;where='_sequence>?';args=[after_sequence]
            if incident_id is not None:where+=' AND _incident_id=?';args.append(incident_id)
            result=db.execute(f'SELECT * FROM lake.{b.schema_name}.{table} AT (VERSION => {b.snapshot}) WHERE {where} ORDER BY _sequence LIMIT {limit+1}',args)
            output=[];size=0;more=False;last=after_sequence
            for batch in result.fetch_record_batch(self.fetch_rows(table)):
                self.guard()
                for row in batch.to_pylist():
                    item=self.decode_row(table,row);n=len(encode(row).encode())
                    if n>self.max_row_bytes or n>self.batch_bytes:raise ValueError('resource_limit:C3_read_row')
                    if len(output)>=limit or size+n>self.batch_bytes:more=True;break
                    output.append(item);size+=n;last=row['_sequence']
                if more:break
            self.check()
            return Page(tuple(output),last if more else None,ReadReceipt(b,'page',len(output),(table,incident_id,after_sequence,last),False))
        finally:db.close()

    def stream(self):
        """全局sequence合并，独立摘要/关系复核；提前close只清理，无最终读取回执。"""
        db=self.connect();b=self.binding
        try:
            # 每种类型一个有界游标，通过磁盘暂存合并；不在Python保留整张表。
            with tempfile.TemporaryDirectory(prefix='country-c3-reader-',dir=self.temporary_root) as directory:
                self.workdir=directory
                index=sqlite3.connect(Path(directory)/'audit.sqlite')
                try:
                    index.execute('PRAGMA cache_size=-8192');index.execute('PRAGMA temp_store=FILE')
                    index.execute('CREATE TABLE rows(sequence INTEGER PRIMARY KEY,table_name TEXT,payload TEXT)')
                    component_audit.initialize(index)
                    completion=None;total=0
                    for table in self.tables:
                        digest=hashlib.sha256();count=0
                        result=db.execute(f'SELECT * FROM lake.{b.schema_name}.{table} AT (VERSION => {b.snapshot}) ORDER BY _sequence')
                        for batch in result.fetch_record_batch(self.fetch_rows(table)):
                            self.guard();inserts=[]
                            for row in batch.to_pylist():
                                payload=encode(row);size=len(payload.encode())
                                if size>self.max_row_bytes or size>self.batch_bytes:raise ValueError('resource_limit:C3_read_row')
                                item=self.decode_row(table,row);component_audit.index_output(index,item,row['_sequence'])
                                if isinstance(item,c2.C2Completion):completion=item
                                inserts.append((row['_sequence'],table,payload));digest.update(bytes.fromhex(row['_row_hash']));count+=1
                            index.executemany('INSERT INTO rows VALUES (?,?,?)',inserts)
                        if count!=self.manifest['tables'][table]['rows'] or digest.hexdigest()!=self.manifest['tables'][table]['sha256']:raise ValueError('C3_table_digest_mismatch')
                        total+=count
                    if total!=self.manifest['row_count'] or completion is None:raise ValueError('C3_stream_count_mismatch')
                    if hashlib.sha256(encode(completion).encode()).hexdigest()!=self.manifest['completion_sha256']:raise ValueError('C3_completion_mismatch')
                    # 封存已验证上游每条引用；读取不再全扫描上游，仍独立验证组件内部关系。
                    # source约束由check的固定资格与封存摘要承接，不能把page称为此全量验证。
                    self.validate(index,completion)
                    for file in self.manifest['files']:
                        if file_hash(self.root/file['path'],self.guard)!=file['sha256']:raise ValueError('C3_file_digest_mismatch')
                    output=[];size=0;expected=0
                    for sequence,table,payload in index.execute('SELECT sequence,table_name,payload FROM rows ORDER BY sequence'):
                        self.guard()
                        if sequence!=expected:raise ValueError('C3_sequence_gap')
                        expected+=1;n=len(payload.encode())
                        if output and (len(output)>=self.batch_rows or size+n>self.batch_bytes):yield ReadBatch(tuple(output));output=[];size=0
                        output.append(self.decode_row(table,decode(payload)));size+=n
                    if output:yield ReadBatch(tuple(output))
                    self.check()
                    yield ReadReceipt(b,'component',total,tuple(self.manifest['scope']),True)
                finally:index.close();self.workdir=None
        finally:db.close()


class CounterLike(dict):
    def __missing__(self,key):return 0
