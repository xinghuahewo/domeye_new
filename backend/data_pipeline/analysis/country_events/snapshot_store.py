"""C3不可变typed组件：湖表为正文，PG仅登记与选中事件索引，无head。"""
from collections import Counter
from dataclasses import asdict,dataclass
import hashlib
import json
import os
from pathlib import Path
import resource
import sqlite3
import sys
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values

from data_pipeline.bgp.archive.store import connect_duckdb, literal
from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_audit as component_audit
from data_pipeline.analysis.country_events.snapshot_schema import SCHEMA_VERSION, SCHEMAS, TABLES, encode, decode, row_encode, row_decode, packed

KIND='country-c2-typed'
LIMITS=dict(batch_rows=256,batch_bytes=4*1024**2,max_row_bytes=4*1024**2,max_rows=10000000,
            max_event_rows=1000000,max_rss_bytes=2*1024**3,max_disk_bytes=8*1024**3)


@dataclass(frozen=True)
class ComponentBinding:
    system_id: str
    database_oid: int
    catalog_id: str
    component_id: str
    schema_name: str
    snapshot: int
    schema_version: str
    manifest_sha256: str
    root: str


def database_identity(dsn):
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as cur:
            cur.execute('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')
            system,oid=cur.fetchone()
    return dict(system_id=system,database_oid=oid)


def upstream_binding(c1):
    if c1 is None:return None
    c1._check()
    return dict(observation=dict(database_identity(c1.reader.dsn),run_id=c1.reader.run_id,snapshot=c1.reader.snapshot,
                                 manifest=c1.reader.manifest),
                detection=dict(database_identity(c1.detection.dsn),run_id=c1.detection.run_id,snapshot=c1.detection.snapshot,identity=c1.identity),
                production_receipt_sha256=c1.production_receipt.sha256,
                legacy_timezone=c1.timezone,reference_selection_rule='detection-reference-39578fe/v2')


def file_hash(path,guard=lambda:None):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        while chunk:=stream.read(1024**2):guard();digest.update(chunk)
    return digest.hexdigest()


def fsync_tree(root):
    for p in root.rglob('*'):
        if p.is_file():
            with p.open('rb') as stream:os.fsync(stream.fileno())
    for directory in [*sorted((p for p in root.rglob('*') if p.is_dir()),reverse=True),root]:
        fd=os.open(directory,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)


def write_json(path,value):
    with path.open('x',encoding='utf-8') as stream:
        stream.write(packed(value));stream.flush();os.fsync(stream.fileno())


def arrow_schema(table,schemas=SCHEMAS):
    kinds={'VARCHAR':pa.string(),'BIGINT':pa.int64(),'BOOLEAN':pa.bool_(),'BLOB':pa.binary()}
    return pa.schema([(name,kinds[kind]) for name,kind,_ in schemas[table]])


def lake_connect(dsn,root=None,*,memory_bytes=None,scratch_root=None,max_temp_bytes=None):
    db=connect_duckdb()
    try:
        if any(v is not None for v in (memory_bytes,scratch_root,max_temp_bytes)):
            if (type(memory_bytes) is not int or memory_bytes<1 or type(max_temp_bytes) is not int
                    or max_temp_bytes<1 or scratch_root is None):
                raise ValueError('Country连接资源必须完整明确')
            db.execute('SET memory_limit='+literal(str(memory_bytes)+'B'))
            db.execute('SET max_temp_directory_size='+literal(str(max_temp_bytes)+'B'))
            db.execute('SET temp_directory='+literal(str(scratch_root)))
        db.execute('LOAD ducklake');db.execute('LOAD postgres')
    except BaseException:
        db.close();raise
    try:
        versions=dict(db.execute('SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded').fetchall())
        if versions.get('ducklake')!='3f1b372' or versions.get('postgres_scanner')!='b9fce43':
            raise ValueError('component_extension_version_mismatch')
        options='READ_ONLY'
        if root is not None:
            with psycopg2.connect(dsn) as pg,pg.cursor() as cur:
                cur.execute("SELECT to_regclass('public.ducklake_metadata') IS NOT NULL")
                initialized=cur.fetchone()[0]
            # 同库后续组件复用既有catalog根；各组件仅注册自己的绝对Parquet路径。
            options='DATA_INLINING_ROW_LIMIT 0'
            if not initialized:options='DATA_PATH '+literal(root/'data')+', '+options
        db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake ('+options+')')
        if root is not None:db.execute('SET temp_directory='+literal(root/'duck-temp'))
        return db
    except BaseException:
        db.close();raise


def code_binding():
    base=Path(__file__).resolve().parents[3]
    paths=sorted((base/'data_pipeline/analysis/country_events').glob('*.py'))
    paths += [base/'pyproject.toml',base/'uv.lock',base.parent/'frontend/package-lock.json']
    return {str(p.relative_to(base.parent)):file_hash(p) for p in paths}


def cleanup(actions,primary=None):
    """每项均尝试；主失败不被清理失败覆盖，额外错误供调用方检查。"""
    errors=[]
    for action in actions:
        try:action()
        except BaseException as error:errors.append(error)
    if errors:
        target=primary if primary is not None else errors[0]
        target.cleanup_errors=(*getattr(target,'cleanup_errors',()),*errors)
        if primary is None:raise target


def close_stream(stream):
    if stream is not None and hasattr(stream,'close'):stream.close()


class ComponentWriter:
    # 显式版本子类复用同一物理写入与回读；默认旧21表合同保持不变。
    tables, schemas, schema_version, kind = TABLES, SCHEMAS, SCHEMA_VERSION, KIND
    encode_row, decode_row = staticmethod(row_encode), staticmethod(row_decode)

    def upstream(self):
        return upstream_binding(self.c1)

    def validate(self):
        return component_audit.validate(self.spool,self.completion,self.c1,self.guard,self.stats)

    def check_append(self,item):
        if self.completion is not None:raise ValueError('C3_rows_after_completion')

    def check_completion_count(self):
        if self.completion.costs.get('output_rows',0)!=self.count-1:raise ValueError('C3_completion_row_count_mismatch')

    def __init__(self,dsn,root,*,c1=None,parameters=None,**limits):
        if set(limits)-set(LIMITS):raise ValueError('unknown_component_limit')
        self.limits={**LIMITS,**limits}
        if any(type(v) is not int or v<1 for v in self.limits.values()):raise ValueError('invalid_component_limit')
        self.root=Path(root).resolve();self.root.mkdir(parents=True,exist_ok=False)
        self.dsn,self.c1,self.parameters=dsn,c1,parameters or {}
        self.id=uuid.uuid4().hex;self.schema='country_'+self.id
        self.stats=Counter();self.started=time.monotonic();self.closed=False
        self.pg=None;self.lake=None;self.spool=None
        try:
            self.anchor=self.upstream()
            self.identity=database_identity(dsn)
            self.pg=psycopg2.connect(dsn)
            with self.pg,self.pg.cursor() as cur:
                self.sql(cur,'CREATE SCHEMA IF NOT EXISTS country_components')
                self.sql(cur,'CREATE TABLE IF NOT EXISTS country_components.catalog(singleton BOOLEAN PRIMARY KEY CHECK(singleton),catalog_id TEXT NOT NULL)')
                self.sql(cur,'INSERT INTO country_components.catalog VALUES (true,%s) ON CONFLICT DO NOTHING',(uuid.uuid4().hex,))
                self.sql(cur,'SELECT catalog_id FROM country_components.catalog WHERE singleton')
                self.identity['catalog_id']=cur.fetchone()[0]
                self.sql(cur,'''CREATE TABLE IF NOT EXISTS country_components.components(
                    component_id TEXT PRIMARY KEY,schema_name TEXT NOT NULL,state TEXT NOT NULL,
                    snapshot BIGINT,root TEXT NOT NULL,manifest_sha256 TEXT,scope TEXT,availability TEXT,reason TEXT)''')
                self.sql(cur,'''CREATE TABLE IF NOT EXISTS country_components.event_index(
                    component_id TEXT,incident_id TEXT,revision TEXT,country TEXT,state TEXT,cohort_id TEXT,
                    sequence BIGINT,row_hash TEXT,PRIMARY KEY(component_id,incident_id))''')
                self.sql(cur,'INSERT INTO country_components.components(component_id,schema_name,state,root) VALUES (%s,%s,%s,%s)',(self.id,self.schema,'candidate',str(self.root)))
            self.spool=sqlite3.connect(self.root/'working.sqlite')
            self.spool.execute('PRAGMA cache_size=-8192');self.spool.execute('PRAGMA temp_store=FILE')
            self.spool.executescript('''CREATE TABLE rows(sequence INTEGER PRIMARY KEY,table_name TEXT,incident TEXT,revision TEXT,payload TEXT,row_hash TEXT,byte_count INTEGER);
                CREATE INDEX row_table ON rows(table_name,sequence);CREATE INDEX row_incident ON rows(incident,sequence);''')
            component_audit.initialize(self.spool)
            self.counts=Counter();self.row_widths=Counter();self.digests={t:hashlib.sha256() for t in self.tables}
            self.count=0;self.completion=None;self.pending=[];self.pending_bytes=0
            self.guard()
        except BaseException as error:
            cleanup((self.close,),error);raise

    def sql(self,cur,query,args=None):
        self.stats['direct_pg_calls']+=1;cur.execute(query,args)

    def guard(self):
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        size=sum(p.stat().st_size for p in self.root.rglob('*') if p.is_file())
        if self.c1 is not None and self.c1.workdir is not None:
            source_work=Path(self.c1.workdir)
            if not source_work.is_relative_to(self.root):size+=sum(p.stat().st_size for p in source_work.rglob('*') if p.is_file())
        self.stats['peak_component_bytes']=max(self.stats['peak_component_bytes'],size)
        self.stats['process_peak_rss_bytes']=max(self.stats['process_peak_rss_bytes'],rss)
        if rss>self.limits['max_rss_bytes'] or size>self.limits['max_disk_bytes']:raise ValueError('resource_limit:C3_working_set')

    def append(self,item):
        self.guard()
        self.check_append(item)
        table,row=self.encode_row(self.count,item);payload=encode(row);size=len(payload.encode())
        if size>self.limits['max_row_bytes'] or size>self.limits['batch_bytes']:raise ValueError('resource_limit:C3_row_bytes')
        incident=row['_incident_id']
        if self.pending and (len(self.pending)>=self.limits['batch_rows'] or self.pending_bytes+size>self.limits['batch_bytes']):self.flush()
        self.pending.append((self.count,table,incident,row['_revision'],payload,row['_row_hash'],size));self.pending_bytes+=size
        component_audit.index_output(self.spool,item,self.count)
        self.counts[table]+=1;self.row_widths[table]=max(self.row_widths[table],size);self.digests[table].update(bytes.fromhex(row['_row_hash']));self.count+=1
        if isinstance(item,c2.C2Completion):self.completion=item

    def flush(self):
        if self.pending:
            self.spool.executemany('INSERT INTO rows VALUES (?,?,?,?,?,?,?)',self.pending);self.spool.commit()
            self.stats['spool_batches']+=1
            self.stats['peak_encoded_batch_bytes']=max(self.stats['peak_encoded_batch_bytes'],self.pending_bytes)
            self.stats['peak_batch_rows']=max(self.stats['peak_batch_rows'],len(self.pending))
            self.pending=[];self.pending_bytes=0
        self.guard()

    def batches(self,query,args=()):
        cur=self.spool.execute(query,args);batch=[];size=0
        for (payload,) in cur:
            self.guard();n=len(payload.encode())
            if batch and (len(batch)>=self.limits['batch_rows'] or size+n>self.limits['batch_bytes']):yield batch;batch=[];size=0
            batch.append(decode(payload));size+=n
        if batch:yield batch

    def seal(self):
        self.flush()
        if self.completion is None:raise ValueError('C3_missing_completion')
        self.check_completion_count()
        if self.c1 is not None:
            grid=self.parameters.get('grid',{})
            if (grid.get('input_start_us'),grid.get('input_end_us'))!=self.completion.scope:raise ValueError('C3_declared_scope_mismatch')
        availability=self.validate()
        self.stats['input_and_relation_seconds']=time.monotonic()-self.started
        self.lake=lake_connect(self.dsn,self.root)
        self.lake.execute('CREATE SCHEMA lake.'+self.schema)
        data=self.root/'data';data.mkdir(exist_ok=True)
        for table in self.tables:
            self.guard()
            cols=','.join('"'+n+'" '+t for n,t,_ in self.schemas[table])
            self.lake.execute(f'CREATE TABLE lake.{self.schema}.{table} ({cols})')
            path=data/(table+'.parquet')
            with pq.ParquetWriter(path,arrow_schema(table,self.schemas),compression='zstd') as writer:
                for batch in self.batches('SELECT payload FROM rows WHERE table_name=? ORDER BY sequence',(table,)):
                    writer.write_table(pa.Table.from_pylist(batch,schema=arrow_schema(table,self.schemas)));self.stats['parquet_row_groups']+=1
            if self.counts[table]:
                self.lake.execute('CALL ducklake_add_data_files(\'lake\','+literal(table)+','+literal(path)+',schema=>'+literal(self.schema)+')')
            self.stats['lake_table_operations']+=1
        self.snapshot=self.lake.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        self.stats['files_persisted']=len(self.tables)
        lake_files={}
        # 不以暂存计数冒充实际湖表：新连接、固定snapshot逐行复读所有列/摘要。
        verify=lake_connect(self.dsn)
        try:
            for table in self.tables:
                linked=verify.execute("SELECT data_file,data_file_size_bytes,delete_file FROM ducklake_list_files('lake',?,schema=>?,snapshot_version=>?)",[table,self.schema,self.snapshot]).fetchall()
                if any(d is not None or Path(p).resolve()!=data/(table+'.parquet') for p,n,d in linked):raise ValueError('C3_lake_file_scope')
                lake_files[table]=[[str(Path(p).relative_to(self.root)),n] for p,n,d in linked]
                result=verify.execute(f'SELECT * FROM lake.{self.schema}.{table} AT (VERSION => {self.snapshot}) ORDER BY _sequence')
                digest=hashlib.sha256();count=0
                for batch in result.fetch_record_batch(min(self.limits['batch_rows'],max(1,self.limits['batch_bytes']//max(1,self.row_widths[table])))):
                    self.guard()
                    for row in batch.to_pylist():self.decode_row(table,row);digest.update(bytes.fromhex(row['_row_hash']));count+=1
                if count!=self.counts[table] or digest.hexdigest()!=self.digests[table].hexdigest():raise ValueError('C3_lake_readback_mismatch:'+table)
        finally:verify.close()
        self.stats['lake_and_readback_seconds']=time.monotonic()-self.started-self.stats['input_and_relation_seconds']
        self.lake.close();self.lake=None
        self.spool.commit()
        # event_index只复制E个选中事件；保留物理typed字段和有界详情定位。
        with self.pg,self.pg.cursor() as cur:
            batch=[];size=0
            for incident,revision,cohort,state,country,sequence,details in self.spool.execute('SELECT e.incident,e.revision,e.cohort,e.state,e.country,e.sequence,r.row_hash FROM events e JOIN rows r ON r.sequence=e.sequence ORDER BY e.sequence'):
                row=(self.id,incident,revision,country,state,cohort,sequence,details)
                width=len(encode(row).encode())
                if width>self.limits['batch_bytes']:raise ValueError('resource_limit:C3_index_row')
                if batch and (len(batch)>=self.limits['batch_rows'] or size+width>self.limits['batch_bytes']):
                    execute_values(cur,'INSERT INTO country_components.event_index VALUES %s',batch,page_size=len(batch));self.stats['direct_pg_calls']+=1;batch=[];size=0
                batch.append(row);size+=width
            if batch:execute_values(cur,'INSERT INTO country_components.event_index VALUES %s',batch,page_size=len(batch));self.stats['direct_pg_calls']+=1
        event_digest=hashlib.sha256();event_rows=0
        for incident,revision,cohort,state,country,sequence,details in self.spool.execute('SELECT e.incident,e.revision,e.cohort,e.state,e.country,e.sequence,r.row_hash FROM events e JOIN rows r ON r.sequence=e.sequence ORDER BY e.sequence'):
            event_digest.update(encode((incident,revision,country,state,cohort,sequence,details)).encode());event_rows+=1
        event_index=dict(rows=event_rows,sha256=event_digest.hexdigest())
        with self.pg,self.pg.cursor(name='c3_seal_index') as cur:
            self.sql(cur,'SELECT incident_id,revision,country,state,cohort_id,sequence,row_hash FROM country_components.event_index WHERE component_id=%s ORDER BY sequence',(self.id,))
            actual=hashlib.sha256();n=0
            while records:=cur.fetchmany(1):
                for record in records:actual.update(encode(tuple(record)).encode());n+=1
            if dict(rows=n,sha256=actual.hexdigest())!=event_index:raise ValueError('C3_index_readback_mismatch')
        self.spool.close();self.spool=None
        (self.root/'working.sqlite').unlink()
        sync_started=time.monotonic();fsync_tree(self.root)
        self.stats['data_fsync_seconds']=time.monotonic()-sync_started
        files=[]
        for path in sorted(data.glob('*.parquet')):
            stat=path.stat();files.append(dict(path=str(path.relative_to(self.root)),size=stat.st_size,mtime_ns=stat.st_mtime_ns,sha256=file_hash(path,self.guard)))
        if self.upstream()!=self.anchor:raise ValueError('C3_upstream_binding_drift')
        self.stats['seal_seconds_before_manifest']=time.monotonic()-self.started
        manifest=dict(kind=self.kind,schema_version=self.schema_version,identity=self.identity,component_id=self.id,
            schema_name=self.schema,snapshot=self.snapshot,root=str(self.root),availability=availability,
            input_kind=self.completion.input_kind,scope=list(self.completion.scope),upstream=encode(self.anchor),
            parameters=encode(self.parameters),limits=self.limits,code_locks=code_binding(),schemas=self.schemas,
            tables={t:dict(rows=self.counts[t],sha256=self.digests[t].hexdigest(),max_encoded_bytes=self.row_widths[t]) for t in self.tables},
            row_count=self.count,files=files,lake_files=lake_files,event_index=event_index,completion_sha256=hashlib.sha256(encode(self.completion).encode()).hexdigest(),costs=dict(self.stats))
        write_json(self.root/'manifest.json',manifest)
        sha=file_hash(self.root/'manifest.json')
        write_json(self.root/'ready.json',dict(component_id=self.id,manifest_sha256=sha))
        fsync_tree(self.root)
        if self.upstream()!=self.anchor:raise ValueError('C3_upstream_binding_drift')
        self.guard()
        with self.pg,self.pg.cursor() as cur:
            self.sql(cur,"UPDATE country_components.components SET state='complete',snapshot=%s,manifest_sha256=%s,scope=%s,availability=%s WHERE component_id=%s AND state='candidate'",(self.snapshot,sha,encode(self.completion.scope),availability,self.id))
            if cur.rowcount!=1:raise ValueError('C3_registration_changed')
        return ComponentBinding(**self.identity,component_id=self.id,schema_name=self.schema,snapshot=self.snapshot,
                                schema_version=self.schema_version,manifest_sha256=sha,root=str(self.root))

    def fail(self,error):
        if self.pg is not None:
            self.pg.rollback()
            with self.pg,self.pg.cursor() as cur:self.sql(cur,"UPDATE country_components.components SET state='failed',reason=%s WHERE component_id=%s AND state!='complete'",(type(error).__name__+':'+str(error),self.id))

    def close(self):
        def release(attr):
            value=getattr(self,attr,None)
            try:
                if value is not None:value.close()
            finally:setattr(self,attr,None)
        try:cleanup(tuple(lambda attr=attr:release(attr) for attr in ('lake','spool','pg')))
        finally:self.closed=True


def persist_stream(stream,dsn,root,*,c1=None,parameters=None,**limits):
    writer=None
    try:
        writer=ComponentWriter(dsn,root,c1=c1,parameters=parameters,**limits)
        for item in stream:writer.append(item)
        finished,stream=stream,None
        close_stream(finished)
        return writer.seal()
    except BaseException as error:
        if writer is not None:cleanup((lambda:writer.fail(error),),error)
        raise
    finally:
        cleanup((lambda:close_stream(stream),lambda:writer.close() if writer is not None else None),sys.exc_info()[1])


def produce_component(c1,grid,dsn,root,*,c2_options=None,**limits):
    writer=stream=None
    original_guard=c1.external_guard
    try:
        options=dict(c2_options or {})
        for key in ('max_rss_bytes','max_disk_bytes'):
            options[key]=min(options.get(key,LIMITS[key]),limits.get(key,LIMITS[key]))
        # C2临时盘在同一结果树下，C3 guard持续计入合计预算。
        writer=ComponentWriter(dsn,root,c1=c1,parameters=dict(grid=asdict(grid),c2_options=options),**limits)
        def combined_guard():original_guard();writer.guard()
        c1.external_guard=combined_guard
        stream=c2.run_saved(c1,grid,scratch_root=writer.root,**options)
        for item in stream:writer.append(item)
        finished,stream=stream,None
        close_stream(finished)
        return writer.seal()
    except BaseException as error:
        if writer is not None:cleanup((lambda:writer.fail(error),),error)
        raise
    finally:
        cleanup((lambda:close_stream(stream),lambda:setattr(c1,'external_guard',original_guard),
                 lambda:writer.close() if writer is not None else None),sys.exc_info()[1])
