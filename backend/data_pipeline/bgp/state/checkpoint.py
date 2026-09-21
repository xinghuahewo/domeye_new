"""文件边界的路由候选工作态：PG 状态、游标、不可变结果引用同事务提交。"""
from contextlib import contextmanager, nullcontext
from dataclasses import asdict
import csv
import hashlib
import io
import os
import struct
import sys
from itertools import islice
from pathlib import Path
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2.extras import Json, execute_values

from data_pipeline.bgp.archive.checkpoint import Owner, file_sha
from data_pipeline.bgp.replay.snapshot_contract import encode, decode, digest

META_FIELDS=('ends','last_order','window_checked','first','last','counts')
REPLAY_FIELDS=('cursor','seen_vps','legacy_baseline_epoch')
BUSINESS_VERSION='business-state/v2'
MANIFEST_SCHEMA=pa.schema([('namespace',pa.string()),('key',pa.string()),('operation',pa.string()),
                           ('sequence',pa.int64()),('digest',pa.string())])
FILE_SCHEMA=pa.schema([('table',pa.string()),('payload',pa.string())])


class BusinessManifest:
    """键与摘要清单，不再复制任何业务正文。"""
    def __init__(self, root, guard):
        self.path=Path(root)/(uuid.uuid4().hex+'.business.parquet')
        self.writer=pq.ParquetWriter(self.path,MANIFEST_SCHEMA,compression='zstd')
        self.pending=[]; self.count=0; self.hashed=hashlib.sha256(); self.guard=guard

    def append(self, namespace, key, operation, sequence, expected):
        row=(namespace,key,operation,sequence,expected)
        self.hashed.update(bytes.fromhex(digest(row))); self.count+=1
        self.pending.append(dict(zip(MANIFEST_SCHEMA.names,row)))
        if len(self.pending)>=2048: self.flush()

    def flush(self):
        self.guard()
        if self.pending:
            self.writer.write_table(pa.Table.from_pylist(self.pending,schema=MANIFEST_SCHEMA)); self.pending=[]

    def finish(self):
        self.flush(); self.writer.close()
        with self.path.open('rb') as stream: os.fsync(stream.fileno())
        fd=os.open(self.path.parent,os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)
        receipt=dict(path=str(self.path),size=self.path.stat().st_size,sha256=file_sha(self.path),
                     rows=self.count,logical_digest=self.hashed.hexdigest())
        for _ in manifest_rows(receipt,self.guard): pass
        return receipt


def manifest_rows(receipt, guard=lambda:None):
    path=Path(receipt['path'])
    if path.stat().st_size!=receipt['size'] or file_sha(path)!=receipt['sha256']:
        raise ValueError('业务变化清单文件漂移')
    hashed=hashlib.sha256(); count=0; parquet=pq.ParquetFile(path)
    try:
        if parquet.schema_arrow!=MANIFEST_SCHEMA: raise ValueError('业务变化清单结构漂移')
        for batch in parquet.iter_batches(batch_size=2048):
            for row in batch.to_pylist():
                value=tuple(row[name] for name in MANIFEST_SCHEMA.names)
                if row['operation'] not in ('put','delete'): raise ValueError('未知业务变化操作')
                hashed.update(bytes.fromhex(digest(value))); count+=1
                yield value
            guard()
    finally: parquet.close()
    if (count,hashed.hexdigest())!=(receipt['rows'],receipt['logical_digest']):
        raise ValueError('业务变化清单摘要不符')


class ResultFile:
    def __init__(self,root,guard):
        self.path=Path(root)/(uuid.uuid4().hex+'.parquet');self.guard=guard
        self.writer=pq.ParquetWriter(self.path,FILE_SCHEMA,compression='zstd')
        self.pending=[];self.count=0;self.logical=hashlib.sha256();self.closed=False
        self.pending_bytes=0;self.max_row_bytes=0;self.peak_pending_bytes=0

    def append(self,table,row):
        payload=encode(row)
        self._append_payload(table,payload)

    def append_business(self,table,row):
        from data_pipeline.bgp.state.business import encode_result
        self._append_payload(table,encode_result(row))

    def _append_payload(self,table,payload):
        # 行数不能限制含完整前缀集合的窗口正文；按实际Python字符串占用反压。
        # 大单行保持完整，独立成批，不截断业务结果。
        size=sys.getsizeof(payload)+sys.getsizeof(table)
        if self.pending and self.pending_bytes+size>8*1024**2:self.flush()
        self.logical.update(bytes.fromhex(digest((table,payload))));self.count+=1
        self.pending.append(dict(table=table,payload=payload))
        self.pending_bytes+=size;self.max_row_bytes=max(self.max_row_bytes,size)
        self.peak_pending_bytes=max(self.peak_pending_bytes,self.pending_bytes)
        if len(self.pending)>=512 or self.pending_bytes>=8*1024**2:self.flush()

    def flush(self):
        self.guard()
        if self.pending:
            self.writer.write_table(pa.Table.from_pylist(self.pending,schema=FILE_SCHEMA))
            self.pending=[];self.pending_bytes=0

    def finish(self):
        self.flush();self.close()
        with self.path.open('rb') as f:os.fsync(f.fileno())
        fd=os.open(self.path.parent,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
        receipt=dict(path=str(self.path),size=self.path.stat().st_size,sha256=file_sha(self.path),
                     rows=self.count,logical_digest=self.logical.hexdigest(),
                     max_row_memory_bytes=self.max_row_bytes,peak_pending_memory_bytes=self.peak_pending_bytes)
        verify_file(receipt,self.guard)
        return receipt

    def close(self):
        if not self.closed:self.writer.close();self.closed=True


def verify_file(receipt,guard=lambda:None):
    path=Path(receipt['path'])
    if path.stat().st_size!=receipt['size'] or file_sha(path)!=receipt['sha256']:raise ValueError('路由结果文件漂移')
    h=hashlib.sha256();count=0
    parquet=pq.ParquetFile(path)
    try:
        if parquet.schema_arrow!=FILE_SCHEMA:raise ValueError('路由结果结构漂移')
        for batch in parquet.iter_batches(batch_size=512):
            for row in batch.to_pylist():
                decode(row['payload'])
                h.update(bytes.fromhex(digest((row['table'],row['payload']))));count+=1
            guard()
    finally:parquet.close()
    if (count,h.hexdigest())!=(receipt['rows'],receipt['logical_digest']):raise ValueError('路由结果读回摘要不符')


class DirtyRoutes(dict):
    def __init__(self,values,dirty):super().__init__(values);self.dirty=dirty
    def __setitem__(self,key,value):
        super().__setitem__(key,value)
        if self.dirty is not None:self.dirty.add(key)


class Delta:
    """计算时只记变化键；文件确认阶段才批量保存，不复制状态负载。"""
    def __init__(self,store,cursor,reducer):
        self.store=store;self.cursor=cursor;self.reducer=reducer;self.dirty=set();self.prefixes=set()
        self.file=ResultFile(store.root,store.guard)
        self.baseline=False
        if reducer is not None:
            r=reducer.replay
            if r.memory is not None:
                self.baseline=not r.memory.prefix_dict
                # 首次空状态直接在文件末遍历唯一字典，不再为全部 RIB 槽复制脏键集合。
                r.memory.dirty=None if self.baseline else self.dirty
            for obj,name in (() if r.memory is not None else ((r,'current'),(r,'last_known'),(reducer,'positions'))):
                value=getattr(obj,name)
                if isinstance(value,DirtyRoutes):value.dirty=self.dirty
                else:setattr(obj,name,DirtyRoutes(value,self.dirty))

    def append(self,table,row):
        if table=='changes' and self.reducer.replay.legacy_views:self.prefixes.add(row['raw']['scope'][8])
        # 原始观察已经完整归档；正常主链只持久化唯一工作态及质量/业务结果。
        if table!='changes' or self.store.audit_transitions:self.file.append(table,row)

    def append_business(self,table,row):
        # 业务输出始终保留；正文在本调用内冻结，缓冲中只有不可变字符串。
        self.file.append_business(table,row)

    def flush(self):
        with self.store.measure('route_state_write'):self._flush()

    def _flush(self):
        self.store.guard();rows=[]
        if self.reducer is None:return
        r=self.reducer.replay
        def save_rows():
            if not rows:return
            execute_values(self.cursor,'''INSERT INTO route_file.rows VALUES %s
                ON CONFLICT(run_id,kind,key) DO UPDATE SET payload=excluded.payload,digest=excluded.digest''',rows,page_size=512)
            rows.clear();self.store.guard()
        if r.memory is not None:
            keys=iter(r.memory.prefix_dict if self.baseline else self.dirty)
            while part:=list(islice(keys,8192)):
                # PG 二进制 COPY 保持原 BYTEA 和摘要合同，避免十六进制膨胀及文本反转义。
                data=io.BytesIO();data.write(b'PGCOPY\n\xff\r\n\x00'+struct.pack('>ii',0,0))
                for key in part:
                    value=r.memory.prefix_dict[key]
                    fields=(key,value,hashlib.sha256(key+value).digest())
                    if self.baseline:fields=(self.store.run_id.encode(),*fields)
                    data.write(struct.pack('>h',len(fields)))
                    for field in fields:data.write(struct.pack('>i',len(field)));data.write(field)
                data.write(struct.pack('>h',-1));data.seek(0)
                if self.baseline:
                    self.cursor.copy_expert('COPY route_file.packed_rows(run_id,key,payload,digest) FROM STDIN WITH (FORMAT BINARY)',data)
                else:
                    self.cursor.execute('TRUNCATE route_delta')
                    self.cursor.copy_expert('COPY route_delta(key,payload,digest) FROM STDIN WITH (FORMAT BINARY)',data)
                    self.cursor.execute('''INSERT INTO route_file.packed_rows
                        SELECT %s,key,payload,digest FROM route_delta
                        ON CONFLICT(run_id,key) DO UPDATE SET payload=excluded.payload,digest=excluded.digest''',(self.store.run_id,))
                self.store.guard()
        elif r.memory is None:
            for key in self.dirty:
                value=(key,r.current.get(key),r.last_known.get(key),self.reducer.positions.get(key))
                rows.append((self.store.run_id,'route',digest(key),encode(value),digest(value)))
                if len(rows)>=512:save_rows()
        for prefix in self.prefixes:
            value=(prefix,r.legacy_by_prefix.get(prefix),r.legacy_origins.get(prefix))
            if value[1:] == (None,None):continue
            rows.append((self.store.run_id,'prefix',digest(prefix),encode(value),digest(value)))
            if len(rows)>=512:save_rows()
        save_rows()
        self.dirty.clear();self.prefixes.clear()
        if self.baseline:
            self.baseline=False
            r.memory.dirty=self.dirty


def metadata(reducer):
    if reducer is None:return None
    if reducer.source is not None or reducer.message is not None:raise ValueError('只能在完整 SourceEnd 后提交状态')
    r=reducer.replay
    return dict(plan=asdict(r.plan),canonical={k:getattr(reducer,k) for k in META_FIELDS},
                replay={k:getattr(r,k) for k in REPLAY_FIELDS},compact=None if r.memory is None else r.memory.metadata(),gaps=list(reducer.index.gaps.values()),
                row_counts=dict(route=len(r.current),prefix=len(r.legacy_by_prefix.keys()|r.legacy_origins.keys())))


class StateStore:
    def __init__(self,dsn,run_id,root,binding,*,guard=lambda:None,metrics=None,audit_transitions=True):
        self.dsn=dsn;self.run_id=run_id;self.root=Path(root).resolve();self.root.mkdir(parents=True,exist_ok=True)
        self.guard=guard;self.metrics=metrics;self.audit_transitions=audit_transitions;self.owner=Owner(dsn,'route-file:'+run_id)
        try:
            with self.owner.transaction() as c:
                c.execute('CREATE SCHEMA IF NOT EXISTS route_file')
                c.execute('''CREATE TABLE IF NOT EXISTS route_file.runs(
                    run_id TEXT PRIMARY KEY,binding JSONB NOT NULL,cursor INTEGER NOT NULL,
                    metadata TEXT,receipt JSONB,root TEXT NOT NULL)''')
                c.execute('''CREATE TABLE IF NOT EXISTS route_file.rows(
                    run_id TEXT,kind TEXT,key TEXT,payload TEXT NOT NULL,digest TEXT NOT NULL,
                    PRIMARY KEY(run_id,kind,key))''')
                c.execute('''CREATE TABLE IF NOT EXISTS route_file.packed_rows(
                    run_id TEXT,key BYTEA,payload BYTEA NOT NULL,digest BYTEA NOT NULL,
                    PRIMARY KEY(run_id,key))''')
                c.execute('CREATE TEMP TABLE route_delta(key BYTEA,payload BYTEA,digest BYTEA)')
                c.execute('''CREATE TABLE IF NOT EXISTS route_file.business_rows(
                    run_id TEXT,namespace TEXT,key TEXT,sequence BIGINT,payload TEXT,digest TEXT,
                    PRIMARY KEY(run_id,namespace,key))''')
                c.execute('''CREATE TABLE IF NOT EXISTS route_file.business_rows_v2(
                    run_id TEXT,namespace TEXT,key TEXT,sequence BIGINT NOT NULL,payload TEXT NOT NULL,digest TEXT NOT NULL,
                    PRIMARY KEY(run_id,namespace,key))''')
                c.execute('''CREATE TABLE IF NOT EXISTS route_file.business_manifests_v2(
                    run_id TEXT,ordinal INTEGER,receipt JSONB NOT NULL,PRIMARY KEY(run_id,ordinal))''')
                c.execute('''CREATE TABLE IF NOT EXISTS route_file.files(
                    run_id TEXT,ordinal INTEGER,checkpoint_digest TEXT NOT NULL,payload JSONB NOT NULL,
                    PRIMARY KEY(run_id,ordinal))''')
                c.execute('SELECT binding,root FROM route_file.runs WHERE run_id=%s',(run_id,));old=c.fetchone()
                if old is None:
                    c.execute('INSERT INTO route_file.runs VALUES (%s,%s,-1,NULL,NULL,%s)',(run_id,Json(binding),str(self.root)))
                elif old!=(binding,str(self.root)):raise ValueError('路由运行计划、规则或输出位置漂移')
            self.binding=binding
            self.validate_files()
        except BaseException:self.owner.close();raise

    def measure(self,name):return self.metrics.measure(name) if self.metrics else nullcontext()

    def status(self):
        with self.owner.transaction() as c:
            c.execute('SELECT cursor,metadata,receipt FROM route_file.runs WHERE run_id=%s',(self.run_id,))
            position,meta,receipt=c.fetchone()
        return position,None if meta is None else decode(meta),receipt

    def files(self):
        with self.owner.transaction() as c:
            c.execute('SELECT ordinal,checkpoint_digest,payload FROM route_file.files WHERE run_id=%s ORDER BY ordinal',(self.run_id,))
            return c.fetchall()

    def validate_files(self):
        files=self.files();position,meta,receipt=self.status()
        if [f[0] for f in files]!=list(range(position+1)):raise ValueError('路由消费提交链不连续')
        if files and files[-1][2]['metadata_digest']!=digest(meta):raise ValueError('路由恢复元数据摘要不符')
        if meta is None:
            with self.owner.transaction() as c:
                c.execute('SELECT 1 FROM route_file.rows WHERE run_id=%s LIMIT 1',(self.run_id,))
                if c.fetchone():raise ValueError('没有恢复元数据却存在路由工作态')
                c.execute('SELECT 1 FROM route_file.packed_rows WHERE run_id=%s LIMIT 1',(self.run_id,))
                if c.fetchone():raise ValueError('没有恢复元数据却存在紧凑工作态')
        if meta is None:
            with self.owner.transaction() as c:
                for table in ('business_rows','business_rows_v2','business_manifests_v2'):
                    c.execute('SELECT 1 FROM route_file.'+table+' WHERE run_id=%s LIMIT 1',(self.run_id,))
                    if c.fetchone():raise ValueError('没有恢复元数据却存在业务工作态')
        for _,_,payload in files:verify_file(payload['file'],self.guard)
        if receipt is not None:verify_file(receipt['final_file'],self.guard)

    def restore(self,reducer):
        _,meta,_=self.status()
        if meta is None:return
        if meta['plan']!=asdict(reducer.replay.plan):raise ValueError('恢复路由计算计划漂移')
        r=reducer.replay;counts=dict(route=0,prefix=0)
        if meta.get('compact') is not None:
            if r.memory is None:raise ValueError('紧凑状态恢复方式不符')
            r.memory.restore_metadata(meta['compact'])
        else:
            r.current=DirtyRoutes({},None);r.last_known=DirtyRoutes({},None);reducer.positions=DirtyRoutes({},None)
        with self.owner.transaction():
            with self.owner.pg.cursor(name='route_restore_'+uuid.uuid4().hex) as c:
                c.itersize=512
                c.execute('SELECT kind,key,payload,digest FROM route_file.rows WHERE run_id=%s',(self.run_id,))
                for kind,key,payload,expected in c:
                    value=decode(payload)
                    if digest(value)!=expected or digest(value[0])!=key:raise ValueError('路由工作态摘要不符')
                    if kind=='route':
                        scope,current,known,position=value
                        r.current[scope]=current;r.last_known[scope]=known
                        if position is not None:reducer.positions[scope]=position
                    elif kind=='prefix':
                        prefix,paths,origins=value
                        if paths is not None:
                            r.legacy_by_prefix[prefix]=paths
                            for vp,path in paths.items():r.legacy_paths[prefix,vp]=path
                        if origins is not None:r.legacy_origins[prefix]=origins
                    else:raise ValueError('未知路由工作态表')
                    counts[kind]+=1
                    if sum(counts.values())%512==0:self.guard()
        if r.memory is not None:
            with self.owner.transaction():
                with self.owner.pg.cursor(name='packed_restore_'+uuid.uuid4().hex) as c:
                    c.itersize=8192
                    c.execute('SELECT key,payload,digest FROM route_file.packed_rows WHERE run_id=%s',(self.run_id,))
                    for key,value,expected in c:
                        key,value,expected=bytes(key),bytes(value),bytes(expected)
                        if hashlib.sha256(key+value).digest()!=expected:raise ValueError('紧凑工作态摘要不符')
                        r.memory.scope(key);r.memory.value(value,'current')
                        r.memory.load(key,value);counts['route']+=1
                        if counts['route']%8192==0:self.guard()
        if counts!=meta['row_counts']:raise ValueError('路由工作态缺行或多行')
        for key,value in meta['canonical'].items():setattr(reducer,key,value)
        for key,value in meta['replay'].items():setattr(r,key,value)
        for gap in meta['gaps']:reducer.index.add(gap)

    def save_business(self,c,business,ordinal):
        from data_pipeline.bgp.state.business import encode_business
        from data_pipeline.bgp.state.dirty_keys import install_changes, MISSING, encode_state
        with self.measure('business_change_tracking_install'):
            changes=install_changes(business,guard=self.guard)
        old=changes.receipt
        if old is not None and old.get('version')!=BUSINESS_VERSION:
            raise ValueError('旧业务恢复格式不能跨版本续跑')
        changes.prepare()
        namespaces=[] if old is None else list(old['namespaces'])
        next_sequence=0 if old is None else old['next_sequence']
        c.execute('CREATE TEMP TABLE business_pending(namespace TEXT,key TEXT,position BIGINT,operation TEXT,reinsert BOOLEAN,payload TEXT,digest TEXT,PRIMARY KEY(namespace,key)) ON COMMIT DROP')
        data=io.StringIO(); writer=csv.writer(data,lineterminator='\n'); buffered=0; position=0
        def flush():
            data.seek(0)
            c.copy_expert('COPY business_pending(namespace,key,position,operation,reinsert,payload,digest) FROM STDIN WITH (FORMAT CSV)',data)
            data.seek(0); data.truncate(0); self.guard()
        source=changes.baseline_rows() if old is None else ((n,k,changes.value(n,k)) for n,k in changes.dirty)
        for namespace,key,value in source:
            if namespace not in namespaces: namespaces.append(namespace)
            key_id=hashlib.sha256(encode_business(key).encode()).hexdigest()
            deleted=value is MISSING
            payload='' if deleted else encode_state((key,value))
            expected='' if deleted else digest((namespace,key_id,payload))
            writer.writerow((namespace,key_id,position,'delete' if deleted else 'put',
                             (namespace,key) in changes.reinserted,payload,expected))
            position+=1; buffered+=1
            if buffered>=512 or data.tell()>=1024*1024: flush(); buffered=0
        if buffered: flush()
        # 首次基线的位置已经按导出顺序编号，不做全表JOIN或窗口排序。
        if old is None:
            c.execute("""CREATE TEMP TABLE business_delta ON COMMIT DROP AS
                SELECT namespace,key,position,operation,payload,digest,NULL::bigint AS old_sequence,
                       NULL::text AS old_digest,position AS sequence FROM business_pending""")
        else:
            # 编号只为新增/删除后重建分配；普通更新保持旧插入顺序。
            c.execute("""CREATE TEMP TABLE business_delta ON COMMIT DROP AS
            SELECT namespace,key,position,operation,payload,digest,old_sequence,old_digest,
                CASE WHEN fresh THEN %s+SUM(CASE WHEN fresh THEN 1 ELSE 0 END) OVER(ORDER BY position)-1
                     ELSE old_sequence END AS sequence
            FROM (SELECT p.*,b.sequence AS old_sequence,b.digest AS old_digest,
                    (p.operation='put' AND (b.key IS NULL OR p.reinsert)) AS fresh
                  FROM business_pending p LEFT JOIN route_file.business_rows_v2 b
                    ON b.run_id=%s AND b.namespace=p.namespace AND b.key=p.key) d
            WHERE (operation='delete' AND old_sequence IS NOT NULL)
                OR (operation='put' AND (fresh OR digest IS DISTINCT FROM old_digest))""",(next_sequence,self.run_id))
        c.execute("SELECT COUNT(*) FILTER(WHERE operation='put' AND old_sequence IS NULL), COUNT(*) FILTER(WHERE operation='delete'),COALESCE(MAX(sequence)+1,%s) FROM business_delta",(next_sequence,))
        inserted,deleted,maximum=c.fetchone()
        total=(0 if old is None else old['rows'])+inserted-deleted
        next_sequence=max(next_sequence,int(maximum))
        manifest=BusinessManifest(self.root,self.guard)
        try:
            with self.owner.pg.cursor(name='business_manifest_'+uuid.uuid4().hex) as reader:
                reader.itersize=2048
                reader.execute('SELECT namespace,key,operation,sequence,digest FROM business_delta ORDER BY position')
                for namespace,key,operation,sequence,expected in reader:
                    manifest.append(namespace,key,operation,int(sequence),expected if operation=='put' else None)
            manifest_receipt=manifest.finish()
        finally: manifest.writer.close()
        c.execute("""INSERT INTO route_file.business_rows_v2
            SELECT %s,namespace,key,sequence,payload,digest FROM business_delta WHERE operation='put'
            ON CONFLICT(run_id,namespace,key) DO UPDATE SET sequence=excluded.sequence,payload=excluded.payload,digest=excluded.digest""",(self.run_id,))
        c.execute("""DELETE FROM route_file.business_rows_v2 b USING business_delta d
            WHERE b.run_id=%s AND b.namespace=d.namespace AND b.key=d.key AND d.operation='delete'""",(self.run_id,))
        receipt=dict(version=BUSINESS_VERSION,ordinal=ordinal,kind='baseline' if old is None else 'delta',
                     previous=None if old is None else old['chain_digest'],manifest=manifest_receipt,
                     rows=total,next_sequence=next_sequence,namespaces=namespaces,completed=business.completed,next_shared_id=changes._next_node)
        receipt['chain_digest']=digest(receipt)
        c.execute('INSERT INTO route_file.business_manifests_v2 VALUES (%s,%s,%s)',(self.run_id,ordinal,Json(receipt)))
        return receipt

    def restore_business(self,business):
        from data_pipeline.bgp.state.business import pack, encode_business
        from data_pipeline.bgp.state.dirty_keys import install_changes, unpack_state
        _,meta,_=self.status()
        if not meta or 'business' not in meta: raise ValueError('恢复游标没有业务计数和活动事件')
        receipt=meta['business']
        if receipt.get('version')!=BUSINESS_VERSION: raise ValueError('旧业务恢复格式不能跨版本续跑')
        # 恢复允许完整核验。临时期望表只存键/顺序/摘要，不复制业务正文到Python。
        with self.owner.transaction() as c:
            c.execute('CREATE TEMP TABLE business_expected(namespace TEXT,key TEXT,sequence BIGINT,digest TEXT,PRIMARY KEY(namespace,key)) ON COMMIT DROP')
            c.execute('SELECT ordinal,receipt FROM route_file.business_manifests_v2 WHERE run_id=%s ORDER BY ordinal',(self.run_id,))
            chain=c.fetchall(); previous=None
            if [ordinal for ordinal,_ in chain]!=list(range(receipt['ordinal']+1)):
                raise ValueError('业务变化清单链不连续')
            for ordinal,item in chain:
                body={k:v for k,v in item.items() if k!='chain_digest'}
                if item['version']!=BUSINESS_VERSION or item['ordinal']!=ordinal or item['previous']!=previous or digest(body)!=item['chain_digest']:
                    raise ValueError('业务变化清单摘要链不符')
                if item['kind']!=('baseline' if ordinal==0 else 'delta'): raise ValueError('业务基线位置无效')
                pending=[]
                def flush():
                    if not pending: return
                    puts=[(n,k,s,h) for n,k,op,s,h in pending if op=='put']
                    deletes=[(n,k) for n,k,op,s,h in pending if op=='delete']
                    if puts: execute_values(c,'INSERT INTO business_expected VALUES %s ON CONFLICT(namespace,key) DO UPDATE SET sequence=excluded.sequence,digest=excluded.digest',puts,page_size=2048)
                    if deletes: execute_values(c,'DELETE FROM business_expected e USING (VALUES %s) d(namespace,key) WHERE e.namespace=d.namespace AND e.key=d.key',deletes,page_size=2048)
                    pending.clear(); self.guard()
                for value in manifest_rows(item['manifest'],self.guard):
                    pending.append(value)
                    if len(pending)>=2048: flush()
                flush(); previous=item['chain_digest']
            if not chain or chain[-1][1]!=receipt: raise ValueError('业务清单与游标未绑定同一提交')
            c.execute('SELECT COUNT(*) FROM business_expected'); count=c.fetchone()[0]
            if count!=receipt['rows']: raise ValueError('业务清单对象数量不符')
            c.execute("""SELECT 1 FROM business_expected e FULL JOIN
                (SELECT namespace,key,sequence,digest FROM route_file.business_rows_v2 WHERE run_id=%s) b
                ON e.namespace=b.namespace AND e.key=b.key
                WHERE e.key IS NULL OR b.key IS NULL OR e.sequence<>b.sequence OR e.digest<>b.digest LIMIT 1""",(self.run_id,))
            if c.fetchone(): raise ValueError('业务正文集合与变化清单不符')
            shared={}
            def rows():
                restored=0
                with self.owner.pg.cursor(name='business_restore_'+uuid.uuid4().hex) as reader:
                    reader.itersize=512
                    reader.execute('SELECT namespace,key,sequence,payload,digest FROM route_file.business_rows_v2 WHERE run_id=%s ORDER BY array_position(%s::text[],namespace),CASE WHEN key=ANY(%s::text[]) THEN 0 ELSE 1 END,sequence',(self.run_id,receipt['namespaces'],[hashlib.sha256(encode_business(k).encode()).hexdigest() for k in ('$mapping','$value')]))
                    for namespace,key,sequence,payload,expected in reader:
                        if namespace not in receipt['namespaces'] or digest((namespace,key,payload))!=expected:
                            raise ValueError('业务工作态命名空间或正文摘要不符')
                        key_value,value=unpack_state(decode(payload),business.pool,shared)
                        if digest(pack(key_value))!=key: raise ValueError('业务工作态键摘要不符')
                        restored+=1; yield namespace,key_value,value
                        if restored%512==0: self.guard()
                if restored!=receipt['rows']: raise ValueError('业务工作态缺行或多行')
            with self.measure('business_state_restore'): business.restore(rows())
        if business.completed!=receipt['completed']: raise ValueError('业务文件游标不符')
        with self.measure('business_change_tracking_restore'):
            install_changes(business,receipt,shared,guard=self.guard)

    @contextmanager
    def commit_file(self,cp,reducer,*,check=lambda:None,hook=lambda *a:None,ordinal=None,business=None):
        # 直接流可在文件开始时开事务，但最终 checkpoint 只能在完整文件选择后取得。
        deferred=callable(cp)
        ordinal=ordinal if deferred else cp['ordinal']
        if type(ordinal) is not int or ordinal<0:raise ValueError('文件事务需要明确序号')
        delta=None; business_receipt=None
        changes=getattr(business,'changes',None) if business is not None else None
        if changes is not None: changes.begin()
        try:
            with self.owner.transaction() as c:
                c.execute('SELECT cursor,receipt FROM route_file.runs WHERE run_id=%s FOR UPDATE',(self.run_id,))
                if c.fetchone()!=(ordinal-1,None):raise ValueError('路由提交位置冲突')
                delta=Delta(self,c,reducer)
                yield delta
                cp=cp() if deferred else cp
                if cp is None or cp['ordinal']!=ordinal:raise ValueError('计算文件尚未取得完整 checkpoint')
                delta.flush()
                business_receipt=None
                if business is not None:
                    with self.measure('business_state_write'):business_receipt=self.save_business(c,business,ordinal)
                with self.measure('route_output_audit'):file=delta.file.finish()
                hook('files_ready',cp['ordinal'])
                check()
                meta=metadata(reducer)
                if business_receipt is not None:meta['business']=business_receipt
                payload=dict(file=file,source_id=cp['source_id'],metadata_digest=digest(meta))
                c.execute('INSERT INTO route_file.files VALUES (%s,%s,%s,%s)',(self.run_id,cp['ordinal'],cp['digest'],Json(payload)))
                c.execute('UPDATE route_file.runs SET cursor=%s,metadata=%s WHERE run_id=%s',
                          (cp['ordinal'],encode(meta),self.run_id))
                hook('before_state_commit',cp['ordinal'])
            if business_receipt is not None: business.changes.committed(business_receipt)
            hook('after_state_commit',cp['ordinal'])
        except BaseException:
            if business is not None and getattr(business,'changes',None) is not None: business.changes.failed_file()
            raise
        finally:
            if delta is not None:delta.file.close()

    def finish(self,reducer,seal,*,hook=lambda *a:None):
        old=self.status()[2]
        if old is not None:return old
        file=ResultFile(self.root,self.guard)
        try:
            if self.audit_transitions:
                for table,row in reducer.export():file.append(table,row)
            else:
                file.append('state_checkpoint',dict(metadata=self.status()[1],state_store='route_file.packed_rows'))
            receipt=dict(profile=self.binding['profile'],qualification='route_candidate_complete',
                run_id=self.run_id,binding_id=self.binding['binding_id'],observation_seal_digest=seal['digest'],
                observation_snapshot=seal['snapshot'],files=len(seal['checkpoints']),final_file=file.finish(),
                business='not_published',features='file_candidate_complete' if self.binding.get('business_config_digest') else 'not_run',
                detection='file_candidate_complete' if self.binding.get('business_config_digest') and self.binding.get('detection_enabled',True) else 'not_run')
            with self.owner.transaction() as c:
                c.execute('SELECT cursor,receipt FROM route_file.runs WHERE run_id=%s FOR UPDATE',(self.run_id,))
                if c.fetchone()!=(len(seal['checkpoints'])-1,None):raise ValueError('候选完成位置冲突')
                c.execute('SELECT checkpoint_digest FROM route_file.files WHERE run_id=%s ORDER BY ordinal',(self.run_id,))
                if [r[0] for r in c.fetchall()]!=[cp['digest'] for cp in seal['checkpoints']]:raise ValueError('候选与观察封存绑定不符')
                c.execute('UPDATE route_file.runs SET receipt=%s WHERE run_id=%s',(Json(receipt),self.run_id))
                hook('before_route_complete',len(seal['checkpoints']))
            hook('after_route_complete',len(seal['checkpoints']))
            return receipt
        finally:file.close()

    def close(self):self.owner.close()
