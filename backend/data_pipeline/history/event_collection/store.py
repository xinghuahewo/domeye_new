"""固定集合目录与DuckLake结构表；业务profile投影尚未实现。"""
from contextlib import closing, contextmanager
from dataclasses import asdict
from decimal import Decimal
import hashlib
import os
from pathlib import Path
import re
import uuid

import pyarrow as pa
from psycopg2 import sql

from data_pipeline.history.database_import import History as BaseHistory, Token
from data_pipeline.history.database_import.codec import canonical
from data_pipeline.history.database_import.freeze import Limits, read_json, write_json
from data_pipeline.history.event_collection.freeze import safe_path, verify_collection
from data_pipeline.history.event_collection.model import Budget, CollectionLimits, CollectionToken, DEFINITIONS, ORDER, SCHEMAS, code_identity, profile_hash
from data_pipeline.history.event_collection.spool import Spool
from data_pipeline.history.event_collection.children import remaining, manifest_preflight, directory_bytes, ChildHistory, checked_lake

TABLES=tuple(DEFINITIONS)


def normalized(name,row):
    return {k:(bool(row[k]) if t=='b' and row[k] is not None else Decimal(row[k]) if t=='d' and row[k] is not None else row[k]) for k,t in DEFINITIONS[name]}

def row_bytes(row):
    return canonical({k:format(v,'f') if isinstance(v,Decimal) else v for k,v in row.items()})

def checked_row(row,budget):
    budget.check(); data=row_bytes(row)
    if len(data)>budget.limits.max_row_bytes: raise ValueError('结构单行字节预算超限')
    return data


class History(BaseHistory):
    def __init__(self,*args,collection_limits=CollectionLimits(),**kwargs):
        super().__init__(*args,**kwargs); self.collection_limits=collection_limits

    def _budget(self):
        limits,cl=self.limits,self.collection_limits
        return Budget(self.root,limits,cl,lambda:self.limits==limits and self.collection_limits==cl)

    def import_collection(self,manifest_path):
        budget=self._budget(); budget.check()
        if self.target_identity is None: raise ValueError('集合须显式私有目标身份')
        package=Path(manifest_path).absolute().parent
        manifest=verify_collection(package,budget); manifest_sha=budget.hash(package/'manifest.json')
        cid=manifest['collection_id']
        if not re.fullmatch('[0-9a-f]{32}',cid): raise ValueError('集合身份无效')
        pg=self._connect(); db=None; registered=False; children=[]
        out=self.data_root/cid
        try:
            self._initialize(pg)
            with pg,pg.cursor() as c:
                c.execute('''CREATE TABLE IF NOT EXISTS history_q3.collections(
                    collection_id TEXT PRIMARY KEY,manifest_sha TEXT NOT NULL,profile_sha TEXT NOT NULL,
                    state TEXT NOT NULL,snapshot BIGINT,ready_sha TEXT,private_root TEXT NOT NULL,reason TEXT)''')
                c.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0))',('history-collection:'+cid,))
                if not c.fetchone()[0]: raise ValueError('集合已有写入者')
                c.execute('SELECT state,manifest_sha FROM history_q3.collections WHERE collection_id=%s',(cid,))
                if c.fetchone(): raise ValueError('集合已存在；不自动重导/重试')
                c.execute("INSERT INTO history_q3.collections VALUES (%s,%s,%s,'candidate',NULL,NULL,%s,NULL)",(cid,manifest_sha,manifest['profile_sha256'],str(self.root)))
            registered=True; out.mkdir(exist_ok=False)
            for child in manifest['children']:
                budget.check(); child_limits,disk=remaining(budget)
                child_manifest,child_rows,child_bytes=manifest_preflight(package/child,child_limits)
                child_history=ChildHistory(self,budget,child_limits,disk)
                child_token=child_history.import_package(package/child)
                budget.check(); children.append(child_token)
                budget.add('child_rows',child_rows,self.limits.max_total_rows)
                budget.add('child_data_bytes',child_bytes,self.limits.max_total_bytes)
                child_output=directory_bytes(self.data_root/child_token.import_id,budget)
                budget.add('child_output_bytes',child_output)
                budget.add('temporary_bytes',child_output,self.collection_limits.temporary_bytes)
            retained=out/'source'; retained.mkdir()
            artifacts=[]
            # 结构SQLite仅是冻结/导入暂存，不复制入完成载体；主体仅DuckLake/Parquet。
            for item in manifest['artifacts']:
                if item['path']=='structure.sqlite': continue
                source=safe_path(package/item['path'],(package,)); target=retained/item['path']; target.parent.mkdir(parents=True,exist_ok=True)
                with source.open('rb') as src,target.open('xb') as dst:
                    while True:
                        budget.check(); block=src.read(self.collection_limits.chunk_bytes); budget.add('read_blocks',1)
                        if not block: break
                        budget.add('temporary_bytes',len(block),self.collection_limits.temporary_bytes); dst.write(block)
                    dst.flush(); os.fsync(dst.fileno())
                if budget.hash(target)!=item['sha256']: raise ValueError('导入原/解码件复制不符')
                artifacts.append(item)
            write_json(out/'source-manifest.json',manifest)
            db=checked_lake(self._lake(cid,False),out,budget,self.collection_limits.temporary_bytes-budget.counts.get('child_output_bytes',0)-2*self.limits.max_metadata_bytes)
            db.execute('CREATE SCHEMA lake.history')
            counts=[]
            with closing(Spool(package/'structure.sqlite',budget)) as spool:
                for ti,name in enumerate(TABLES):
                    schema=SCHEMAS[name]; db.register('collection_schema',pa.Table.from_batches([],schema=schema))
                    db.execute(f'CREATE TABLE lake.history.t{ti} AS SELECT * FROM collection_schema'); db.unregister('collection_schema')
                    pending=[]; size=0; count=0; digest=hashlib.sha256()
                    def flush():
                        nonlocal size
                        if not pending: return
                        budget.check(); batch=pa.Table.from_pylist(pending,schema=schema)
                        if batch.nbytes>self.limits.batch_bytes: raise ValueError('Arrow写批字节超限')
                        db.register('collection_batch',batch)
                        try: db.execute(f'INSERT INTO lake.history.t{ti} SELECT * FROM collection_batch')
                        finally: db.unregister('collection_batch')
                        budget.add('arrow_batches',1); pending.clear(); size=0; budget.check()
                    for row in spool.rows(name):
                        row=normalized(name,row); data=row_bytes(row)
                        if len(data)>self.limits.max_row_bytes: raise ValueError('结构单行超限')
                        if pending and (len(pending)>=self.limits.batch_rows or size+len(data)>self.limits.batch_bytes): flush()
                        budget.add('typed_rows',1,self.limits.max_total_rows); budget.add('typed_bytes',len(data),self.limits.max_total_bytes)
                        pending.append(row); size+=len(data); digest.update(data+b'\n'); count+=1
                    flush(); counts.append({'name':name,'rows':count,'sha256':digest.hexdigest()})
            snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
            # 按完整有序typed流回读；不用原解析器作唯一测试oracle。
            for ti,name in enumerate(TABLES):
                h=hashlib.sha256(); count=0
                for row in self._collection_rows(db,ti,name,snapshot,budget):
                    h.update(row_bytes(row)+b'\n'); count+=1
                if (count,h.hexdigest())!=(counts[ti]['rows'],counts[ti]['sha256']): raise ValueError('typed回读不符')
            files=self._files(db,{'tables':counts},snapshot,out,budget.guard)
            verify_collection(package,budget)
            if budget.hash(package/'manifest.json')!=manifest_sha: raise ValueError('导入期间冻结清单变化')
            ready=dict(collection_id=cid,manifest_sha256=manifest_sha,profile_sha256=manifest['profile_sha256'],snapshot=snapshot,
                       children=[asdict(t) for t in children],tables=counts,files=files,artifacts=artifacts,
                       code_sha256=code_identity(),scope='structure_audit_only',integrity_state='complete',
                       resources=budget.report(),limits=asdict(self.limits),collection_limits=asdict(self.collection_limits))
            write_json(out/'ready.json',ready); ready_sha=budget.hash(out/'ready.json')
            # 固定所有child资格共享锁直到集合登记提交。
            with pg:
                for child in sorted(children,key=lambda t:t.import_id): self._component(child,pg=pg,lock=True)
                budget.check()
                if code_identity()!=ready['code_sha256']: raise ValueError('集合代码漂移')
                for item in artifacts:
                    if budget.hash(retained/item['path'])!=item['sha256']: raise ValueError('最终原件漂移')
                for item in files:
                    if budget.hash(item['path'])!=item['sha256']: raise ValueError('最终Parquet漂移')
                with pg.cursor() as c:
                    c.execute("UPDATE history_q3.collections SET state='complete',snapshot=%s,ready_sha=%s WHERE collection_id=%s AND state='candidate'",(snapshot,ready_sha,cid))
                    if c.rowcount!=1: raise ValueError('集合完成登记竞争')
            return CollectionToken(cid,manifest_sha,manifest['profile_sha256'],snapshot,ready_sha,tuple(children))
        except BaseException as error:
            if registered:
                pg.rollback()
                with pg,pg.cursor() as c:
                    c.execute("UPDATE history_q3.collections SET state='failed',reason=%s WHERE collection_id=%s AND state='candidate'",(str(error),cid))
            raise
        finally:
            if db: db.close()
            pg.close()

    def _collection_rows(self,db,ti,name,snapshot,budget,where='',params=(),cap=None):
        budget.check()
        # 同固定快照先确定最大行宽；批上限不决定是否再做全档哈希。
        width=db.execute(f'SELECT coalesce(max(octet_length(encode(to_json(t)))),0) FROM (SELECT * FROM lake.history.t{ti} AT (VERSION => {int(snapshot)})) t').fetchone()[0]
        requested=min(cap or self.limits.batch_rows,self.limits.batch_rows,max(1,self.limits.batch_bytes//max(1,8*width+256*len(SCHEMAS[name]))))
        if width>self.limits.max_row_bytes: requested=1  # 当前小行额度时不预取多行，逐行精确编码再判定。
        result=db.execute(f'SELECT * FROM lake.history.t{ti} AT (VERSION => {int(snapshot)}) '+where+' ORDER BY '+ORDER[name],params)
        reader=result.fetch_record_batch(requested)
        try:
            if reader.schema!=SCHEMAS[name]: raise ValueError('集合typed schema不符')
            while True:
                budget.check()
                try: batch=next(reader)
                except StopIteration: break
                budget.check(); budget.add('arrow_batches',1)
                if batch.nbytes>self.limits.batch_bytes: raise ValueError('Arrow读批字节超限')
                for row in batch.to_pylist():
                    budget.check(); data=checked_row(row,budget)
                    budget.add('typed_rows',1,self.limits.max_total_rows); budget.add('typed_bytes',len(data),self.limits.max_total_bytes)
                    yield row
        finally: reader.close()

    def _qualify_collection(self,token,budget,pg=None,lock=False):
        budget.check()
        if not isinstance(token,CollectionToken) or not re.fullmatch('[0-9a-f]{32}',token.collection_id): raise ValueError('缺固定CollectionToken')
        owned=pg is None
        if owned: pg=self._connect()
        try:
            with pg.cursor() as c:
                c.execute('SELECT state,manifest_sha,profile_sha,snapshot,ready_sha,private_root FROM history_q3.collections WHERE collection_id=%s'+(' FOR SHARE' if lock else ''),(token.collection_id,))
                if c.fetchone()!=('complete',token.manifest_sha256,token.profile_sha256,token.snapshot,token.ready_sha256,str(self.root)): raise ValueError('集合候选/错版/撤销')
                c.execute(sql.SQL('SELECT snapshot_id FROM {}.ducklake_snapshot WHERE snapshot_id=%s').format(sql.Identifier('hl_'+token.collection_id)),(token.snapshot,))
                if c.fetchone()!=(token.snapshot,): raise ValueError('集合固定快照不存在')
            budget.add('sql_calls',2)
            out=self.data_root/token.collection_id
            if budget.hash(out/'ready.json')!=token.ready_sha256 or budget.hash(out/'source-manifest.json')!=token.manifest_sha256: raise ValueError('集合固定清单变化')
            ready=read_json(out/'ready.json',self.collection_limits.metadata_bytes)
            if ready['profile_sha256']!=profile_hash() or ready['code_sha256']!=code_identity() or ready['children']!=[asdict(t) for t in token.children]: raise ValueError('集合规则/child绑定变化')
            for child in sorted(token.children,key=lambda t:t.import_id):
                budget.check(); self._component(child,pg=pg,lock=lock)
            total=0
            for item in ready['files']:
                path=safe_path(item['path'],(out/'parquet',)); total+=item['bytes']
                if total>self.limits.max_total_bytes or path.stat().st_size!=item['bytes'] or budget.hash(path)!=item['sha256']: raise ValueError('集合Parquet损坏/超限')
            total=0
            for item in ready['artifacts']:
                path=safe_path(out/'source'/item['path'],(out/'source',)); total+=item['bytes']
                if total>self.collection_limits.temporary_bytes or path.stat().st_size!=item['bytes'] or budget.hash(path)!=item['sha256']: raise ValueError('集合原/解码件损坏/超限')
            return ready
        finally:
            if owned: pg.close()

    def collection(self,token): return CollectionSession(self,token)


class CollectionSession:
    def __init__(self,history,token):
        self.h,self.token=history,token; self.budget=history._budget(); self.db=None; self.ready=None
        self.receipt=None; self.completed=set(); self.active=False; self.failed=False
    def __enter__(self):
        if self.failed or self.ready is not None: raise ValueError('结构会话不可重复进入')
        try:
            self.budget.check(); self.ready=self.h._qualify_collection(self.token,self.budget)
            self.db=self.h._lake(self.token.collection_id,True)
            return self
        except BaseException:
            self._fail(); raise
    def __exit__(self,kind,error,trace):
        try:
            if kind is None and not self.failed and not self.active:
                self.budget.check(); pg=self.h._connect()
                try:
                    with pg:
                        self.h._qualify_collection(self.token,self.budget,pg=pg,lock=True)
                        self.budget.check()
                        self.receipt={'qualification':'complete','scope':'structure_audit_session','datasets_exhausted':sorted(self.completed),
                                      'token':asdict(self.token),'resources':self.budget.report(),'business_admission':'not_implemented'}
                finally: pg.close()
        except BaseException:
            self._fail(); raise
        finally:
            if self.db: self.db.close(); self.db=None
    def _fail(self):
        self.failed=True; self.receipt=None
        db,self.db=self.db,None
        if db: db.close()
    def _check(self):
        try:
            self.budget.check()
            if self.db is None or self.failed: raise ValueError('结构会话已关闭/失败')
        except BaseException:
            self._fail(); raise
    @contextmanager
    def _reading(self):
        try:
            self._check(); yield; self._check()
        except BaseException:
            self._fail(); raise
    def bulk(self,dataset,*,batch_rows=None):
        self._check()
        if dataset not in TABLES or self.active: raise ValueError('未知结构表/已有活动流')
        cap=self.h.limits.batch_rows if batch_rows is None else batch_rows
        if type(cap) is not int or not 1<=cap<=self.h.limits.batch_rows: raise ValueError('输出批预算无效')
        self.active=True; digest=hashlib.sha256(); count=0
        try:
            pending=[]; size=0; ti=TABLES.index(dataset)
            with closing(self.h._collection_rows(self.db,ti,dataset,self.token.snapshot,self.budget,cap=cap)) as rows:
                for row in rows:
                    self._check(); data=row_bytes(row)
                    if pending and (len(pending)>=cap or size+len(data)>self.h.limits.batch_bytes):
                        yield {'qualification':'provisional','rows':pending}
                        self._check(); pending=[]; size=0
                    pending.append(row); size+=len(data); count+=1; digest.update(data+b'\n')
                if pending:
                    yield {'qualification':'provisional','rows':pending}; self._check()
            expected=self.ready['tables'][ti]
            if (count,digest.hexdigest())!=(expected['rows'],expected['sha256']): raise ValueError('结构全表有序摘要不符')
            self.completed.add(dataset)
        except BaseException:
            self._fail(); raise
        finally: self.active=False
    def nodes(self,document_id,*,after=-1,limit=60,parent=None,key=None,cursor=None):
        """仅固定文档结构定位；重键返回所有occurrence，无任意JSONPath。"""
        self._check()
        if self.active or type(document_id) is not int or not 0<=document_id<2**63 or type(after) is not int or not -1<=after<2**63 or type(limit) is not int or not 1<=limit<=min(60,self.h.limits.batch_rows): raise ValueError('结构分页参数无效')
        binding={'token':asdict(self.token),'document_id':document_id,'parent':parent,'key':key}
        if cursor is not None:
            if not isinstance(cursor,dict) or set(cursor)!={'binding','after'} or cursor['binding']!=binding or after!=-1 or type(cursor['after']) is not int or not 0<=cursor['after']<2**63: raise ValueError('结构分页游标错版/参数冲突')
            after=cursor['after']
        where='WHERE document_id=? AND node_ordinal>?'; params=[document_id,after]
        if parent is not None:
            if type(parent) is not int or not 0<=parent<2**63: raise ValueError('父节点无效')
            where+=' AND parent_ordinal=?'; params.append(parent)
        if key is not None:
            if parent is None or not isinstance(key,str) or len(key.encode('utf8'))>self.h.collection_limits.token_bytes: raise ValueError('成员定位无效')
            where+=' AND key=?'; params.append(key)
        # LIMIT包含在固定查询，不扫描其余页到Python；稳定node顺序。
        ti=TABLES.index('nodes'); query=f'SELECT * FROM lake.history.t{ti} AT (VERSION => {int(self.token.snapshot)}) {where} ORDER BY node_ordinal LIMIT ?'
        with self._reading():
            self.budget.check(); result=self.db.execute(query,params+[limit]); rows=[]; size=0
            while True:
                self._check(); values=result.fetchone()
                if values is None: break
                row=dict(zip(SCHEMAS['nodes'].names,values)); data=checked_row(row,self.budget); size+=len(data)
                if size>self.h.limits.batch_bytes: raise ValueError('结构页字节超限')
                self.budget.add('typed_rows',1,self.h.limits.max_total_rows); self.budget.add('typed_bytes',len(data),self.h.limits.max_total_bytes)
                rows.append(row)
            self._check()
            return {'qualification':'provisional','present':bool(rows),'rows':rows,'after':rows[-1]['node_ordinal'] if rows else after,
                    'binding':binding,'next_cursor':{'binding':binding,'after':rows[-1]['node_ordinal']} if len(rows)==limit else None}
    def span(self,document_id,node_ordinal):
        self._check()
        if type(node_ordinal) is not int or not 0<=node_ordinal<2**63: raise ValueError('节点参数无效')
        rows=self.nodes(document_id,after=node_ordinal-1,limit=1)['rows']
        if not rows or rows[0]['node_ordinal']!=node_ordinal: raise ValueError('节点未留存')
        with self._reading():
            node=rows[0]; size=node['byte_end']-node['byte_start']
            if size>self.h.limits.batch_bytes: raise ValueError('节点原span超有界返回预算')
            ti=TABLES.index('documents')
            doc=self.db.execute(f'SELECT entity_path FROM lake.history.t{ti} AT (VERSION => {int(self.token.snapshot)}) WHERE document_id=?',[document_id]).fetchone()
            root=self.h.data_root/self.token.collection_id/'source'; path=safe_path(root/doc[0],(root,))
            with path.open('rb') as f:
                self._check(); f.seek(node['byte_start']); data=f.read(size)
            if len(data)!=size: raise ValueError('节点原span截断')
            self._check(); return data
    def rebuild(self):
        """PG只重建文档定位索引；不复制nodes/JSON/业务主体。"""
        self._check()
        with self._reading():
            target='hcidx_'+uuid.uuid4().hex; pg=self.h._connect(); count=0
            try:
                with pg,pg.cursor() as c:
                    c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(target)))
                    c.execute(sql.SQL('CREATE TABLE {}.documents(document_id BIGINT PRIMARY KEY,file_id BIGINT,entity_path TEXT,byte_start BIGINT,byte_end BIGINT,node_count BIGINT)').format(sql.Identifier(target)))
                    with closing(self.bulk('documents')) as stream:
                        for batch in stream:
                            self._check(); values=[(r['document_id'],r['file_id'],r['entity_path'],r['byte_start'],r['byte_end'],r['node_count']) for r in batch['rows']]
                            encoded=[c.mogrify('(%s,%s,%s,%s,%s,%s)',v) for v in values]
                            prefix=sql.SQL('INSERT INTO {}.documents VALUES ').format(sql.Identifier(target)).as_string(pg).encode()
                            statement=prefix+b','.join(encoded)
                            if len(statement)>self.h.limits.batch_bytes: raise ValueError('定位索引PG批超限')
                            c.execute(statement); self.budget.add('sql_calls',1); count+=len(values)
                    self.h._qualify_collection(self.token,self.budget,pg=pg,lock=True); self._check()
                return {'schema':target,'rows':count,'scope':'document_location_index_only','nodes_copied_to_pg':0,'token':asdict(self.token)}
            finally: pg.close()
