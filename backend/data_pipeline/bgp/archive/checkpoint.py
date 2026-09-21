"""M2-B：完整文件恢复点。只有持锁PG会话可选择不可变观察尝试。"""
from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import sys
import time
import uuid

import pyarrow as pa
import psycopg2
from psycopg2.extras import Json

from data_pipeline.bgp.input.mrt_reader import read_source, ReadPolicy
from data_pipeline.bgp.input.reference_reader import rows as reference_rows, resolved_interpretation
from data_pipeline.bgp.replay.run_from_files import normalized_inputs, code_identity
from data_pipeline.bgp.replay.route_replay import associate
from data_pipeline.bgp.archive.store import TABLES, TYPES, connect_duckdb, literal

VERSION = 'observation-checkpoint/v1'
OBS_TABLES = {k: TABLES[k] for k in ('messages','elements','paths','peers','eor','associations','quality','references')}
OBS_TABLES['messages'] = OBS_TABLES['messages'] + [('interpretation','VARCHAR')]
KEYS = {'messages':'record','elements':'message_id,ordinal','paths':'path_key','peers':'table_record,"index"',
        'eor':'message_id,afi,safi','associations':'message_id,peer_ref',
        'quality':'source_id,message_id,code,detail','references':'row,location'}


def encoded(value):
    """类型/bytes/null有区别的稳定摘要编码，不将物理文件顺序当逻辑顺序。"""
    if isinstance(value, bytes): return ['bytes',value.hex()]
    if isinstance(value, dict): return ['dict',[[k,encoded(v)] for k,v in sorted(value.items())]]
    if isinstance(value,(tuple,list)): return ['list',[encoded(v) for v in value]]
    return [type(value).__name__,value]


def sha(value):
    return hashlib.sha256(json.dumps(encoded(value),ensure_ascii=False,separators=(',',':')).encode()).hexdigest()


def file_sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(1024**2),b''): h.update(chunk)
    return h.hexdigest()


def durable(path, value):
    path=Path(path)
    with path.open('x') as f:
        json.dump(value,f,ensure_ascii=False,indent=2);f.flush();os.fsync(f.fileno())
    fd=os.open(path.parent,os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


def event(root, label, **values):
    """小型运行审计；测试可等明确边界后实际终止进程。"""
    with (Path(root)/'events.jsonl').open('a') as f:
        f.write(json.dumps(dict(label=label,**values),ensure_ascii=False)+'\n');f.flush();os.fsync(f.fileno())


class Owner:
    def __init__(self,dsn,run_id):
        self.pg=psycopg2.connect(dsn);self.run_id=run_id;self.alive=True
        self.key=int(hashlib.sha256(run_id.encode()).hexdigest()[:15],16)
        with self.pg.cursor() as c:
            c.execute('SELECT pg_try_advisory_lock(%s)',(self.key,))
            if not c.fetchone()[0]: self.pg.close();self.alive=False;raise ValueError('已有运行所有者')
        self.pg.commit()

    def check(self):
        if not self.alive or self.pg.closed: self.alive=False;raise RuntimeError('invocation提交权已撤销')
        try:
            with self.pg.cursor() as c:
                c.execute("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND classid=%s AND objid=%s AND granted)",(self.key>>32,self.key&0xffffffff))
                if not c.fetchone()[0]: self.alive=False;raise RuntimeError('invocation失锁')
        except (psycopg2.OperationalError,psycopg2.InterfaceError):
            self.alive=False;raise

    @contextmanager
    def transaction(self):
        try:
            self.check()
            with self.pg.cursor() as c: yield c
            self.pg.commit()
        except BaseException as exc:
            if isinstance(exc,(psycopg2.OperationalError,psycopg2.InterfaceError)) or self.pg.closed: self.alive=False
            if not self.pg.closed: self.pg.rollback()
            raise

    def close(self):
        self.alive=False;self.pg.close()


def setup(owner):
    with owner.transaction() as c:
        c.execute('CREATE SCHEMA IF NOT EXISTS observation_m2')
        c.execute('''CREATE TABLE IF NOT EXISTS observation_m2.runs(
          run_id TEXT PRIMARY KEY,plan_id TEXT NOT NULL,plan JSONB NOT NULL,schema_name TEXT NOT NULL,
          data_path TEXT NOT NULL,state TEXT NOT NULL,seal JSONB,reason TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS observation_m2.checkpoints(
          run_id TEXT,ordinal INTEGER,payload JSONB NOT NULL,PRIMARY KEY(run_id,ordinal))''')


from data_pipeline.common.run_metrics import Metrics


class Guard:
    def __init__(self,root,data_path,min_free_bytes,max_rss_bytes):
        self.roots=(Path(root),Path(data_path));self.free=min_free_bytes;self.rss=max_rss_bytes
    def __call__(self):
        for p in self.roots:
            while not p.exists():p=p.parent
            if shutil.disk_usage(p).free<self.free: raise RuntimeError('磁盘资源保护')
        peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        if peak>self.rss: raise RuntimeError('RSS资源保护')


def plan_for(manifest,policy):
    from datetime import datetime
    from jsonschema import Draft202012Validator,FormatChecker
    schema=json.loads((Path(__file__).resolve().parents[4]/'contracts/data/observation-run.schema.json').read_text())
    Draft202012Validator(schema,format_checker=FormatChecker()).validate(manifest)
    if datetime.fromisoformat(manifest['window_start'].replace('Z','+00:00'))>=datetime.fromisoformat(manifest['window_end_exclusive'].replace('Z','+00:00')):raise ValueError('结果时间窗无效')
    inputs=normalized_inputs(manifest)
    refs=[]
    seen={}
    for r in manifest.get('references',[]):
        resolved=resolved_interpretation(r['path'])
        if r['sha256'] in seen:
            if seen[r['sha256']]!=resolved:raise ValueError('同SHA引用别名改变解释格式')
            continue
        seen[r['sha256']]=resolved
        refs.append({**r,'format':resolved,'role':'reference','source_id':r['sha256']})
    plan=dict(version=VERSION,policy=ReadPolicy(policy).value,manifest={**manifest,'inputs':inputs},
              inputs=[*refs,*inputs],code=code_identity())
    canonical=json.loads(json.dumps(plan))
    for e in canonical['inputs']:e.pop('path',None)
    for e in canonical['manifest']['inputs']:e.pop('path',None)
    canonical['manifest']['references']=[{k:v for k,v in e.items() if k!='path'} for e in refs]
    return plan,sha(canonical)


class AttemptStore:
    def __init__(self,dsn,schema,data_path,root,owner,guard,batch_rows=50000,memory_limit='1GB'):
        self.schema=schema;self.root=Path(root);self.owner=owner;self.guard=guard;self.batch_rows=batch_rows
        self.db=connect_duckdb(str(self.root/'staging.duckdb'))
        self.db.execute('SET memory_limit='+literal(memory_limit))
        self.db.execute('SET temp_directory='+literal(self.root/'temp'))
        self.db.execute('LOAD ducklake');self.db.execute('LOAD postgres')
        self.db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (DATA_PATH '+literal(data_path)+', DATA_INLINING_ROW_LIMIT 0)')
        versions=dict(self.db.execute('SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded').fetchall())
        if versions.get('ducklake')!='3f1b372' or versions.get('postgres_scanner')!='b9fce43':raise ValueError('扩展版本不符')
        self.db.execute(f'CREATE SCHEMA IF NOT EXISTS lake.{schema}')
        for name,cols in OBS_TABLES.items():
            self.db.execute(f'CREATE TABLE IF NOT EXISTS lake.{schema}."{name}" (attempt VARCHAR,'+','.join('"'+n+'" '+t for n,t in cols)+')')
        self.db.execute(f'CREATE TABLE IF NOT EXISTS lake.{schema}.path_owner(seal_id VARCHAR,path_key VARCHAR,attempt VARCHAR)')
        self.db.execute(f'CREATE TABLE IF NOT EXISTS lake.{schema}.seal_selection(seal_id VARCHAR,source_id VARCHAR,attempt VARCHAR,ordinal BIGINT,checkpoint_digest VARCHAR)')
        self.db.execute('CREATE TABLE path_keys(path_key VARCHAR PRIMARY KEY,digest VARCHAR)')
        self.path_count=0
        self.pending={k:[] for k in OBS_TABLES};self.attempt=uuid.uuid4().hex
        self.before_files=self.files();self.flushes=0

    def files(self,snapshot=None):
        result={}
        for name in OBS_TABLES:
            version='' if snapshot is None else ', snapshot_version => ?'
            args=['lake',name,self.schema]+([] if snapshot is None else [snapshot])
            for row in self.db.execute('SELECT data_file,data_file_size_bytes,delete_file FROM ducklake_list_files(?, ?, schema => ?'+version+')',args).fetchall():
                if row[2] is not None:raise ValueError('本阶段禁止删除/改写文件')
                result[row[0]]=(name,row[1])
        return result

    def append(self,table,row):
        self.pending[table].append({k:row.get(k) for k,_ in OBS_TABLES[table]})
        if sum(map(len,self.pending.values()))>=self.batch_rows:self.flush()

    def message(self,m):
        p=m.peer
        row={n:getattr(m,n,None) for n,_ in TABLES['messages']}
        row.update(message_id=m.message_id,peer_ip=p.get('ip'),peer_asn=p.get('asn'),bgp_id=p.get('bgp_id'),
            bgp_id_present=p.get('bgp_id_present',False),local_ip=p.get('local_ip'),local_asn=p.get('local_asn'),interface=p.get('interface'),endpoint_afi=p.get('endpoint_afi'),local_message=p.get('local_message'),
            interpretation=json.dumps(asdict(m.interpretation),ensure_ascii=False))
        self.append('messages',row)
        for p in m.peers:self.append('peers',p)
        for afi,safi in m.eor_families:self.append('eor',dict(message_id=m.message_id,afi=afi,safi=safi))
        for p in m.paths:self.append('paths',p)
        for e in m.elements:
            p=e['peer'];row={**e,'event_id':m.message_id+':'+str(e['ordinal']),'message_id':m.message_id,
                'peer_ip':p.get('ip'),'peer_asn':p.get('asn'),'bgp_id':p.get('bgp_id'),'bgp_id_present':p.get('bgp_id_present',False),
                'peer_table_record':p.get('table_record'),'peer_index':p.get('index')}
            self.append('elements',row)
        if m.reason:self.append('quality',dict(source_id=m.source_id,message_id=m.message_id,code=m.interpretation.reason_code,detail=m.reason))

    def new_paths(self,rows):
        """只查询本批key；内存随批次有界，不缓存整个文件的路径字典。"""
        candidates={}
        for row in rows:
            key=row['path_key'];digest=sha(row)
            if key in candidates and candidates[key][1]!=digest:raise ValueError('同path_key不同typed正文')
            if hashlib.sha256(bytes([row['asn_width']])+row['attributes_raw']).hexdigest()!=key:raise ValueError('path_key与原bytes不符')
            candidates[key]=(row,digest)
        keys=list(candidates)
        existing=[]
        # 小字典一次批量关联更快；扫描规模上限随本批key数限定，不随文件无限增长。
        if self.path_count<=256*len(keys):
            self.db.register('candidate_path_keys',pa.Table.from_pylist([{'path_key':key} for key in keys]))
            try:existing=self.db.execute('SELECT k.path_key,k.digest FROM path_keys k JOIN candidate_path_keys c USING(path_key)').fetchall()
            finally:self.db.unregister('candidate_path_keys')
        else:
            # 小块IN使主键索引可用，避免每批重新扫描整个累计字典。
            for start in range(0,len(keys),512):
                self.guard();chunk=keys[start:start+512]
                query='SELECT path_key,digest FROM path_keys WHERE path_key IN ('+','.join('?' for _ in chunk)+')'
                existing.extend(self.db.execute(query,chunk).fetchall())
        for key,digest in existing:
            if candidates[key][1]!=digest:raise ValueError('同path_key不同typed正文')
            del candidates[key]
        return [row for row,_ in candidates.values()],[{'path_key':key,'digest':digest} for key,(_,digest) in candidates.items()]

    def flush(self):
        if not any(self.pending.values()):return
        self.guard();self.owner.check()
        self.db.execute('BEGIN');new_hashes=[]
        try:
            for name,rows in self.pending.items():
                if not rows:continue
                if name=='paths':
                    rows,new_hashes=self.new_paths(rows)
                    if not rows:continue
                schema=pa.schema([('attempt',pa.string()),*[(n,TYPES[t]) for n,t in OBS_TABLES[name]]])
                self.db.register('batch',pa.Table.from_pylist([dict(attempt=self.attempt,**r) for r in rows],schema=schema))
                self.db.execute(f'INSERT INTO lake.{self.schema}."{name}" SELECT * FROM batch')
                self.db.unregister('batch')
            self.db.execute('COMMIT')
        except BaseException:self.db.execute('ROLLBACK');raise
        if new_hashes:
            self.db.register('path_hashes',pa.Table.from_pylist(new_hashes))
            self.db.execute('INSERT INTO path_keys SELECT path_key,digest FROM path_hashes')
            self.path_count+=len(new_hashes)
            self.db.unregister('path_hashes')
        for rows in self.pending.values():rows.clear()
        self.flushes+=1

    def relation(self,table,snapshot=None,max_rowid=None):
        v=f' AT (VERSION => {int(snapshot)})' if snapshot is not None else ''
        ceiling='' if max_rowid is None else f' AND rowid<={int(max_rowid)}'
        return f'(SELECT * EXCLUDE(attempt) FROM lake.{self.schema}."{table}"{v} WHERE attempt={literal(self.attempt)}{ceiling})'

    def typed_digest(self,reader):
        """默认实现保持原编码；显式原生候选可替换执行器。"""
        h=hashlib.sha256();count=0
        for batch in reader:
            for row in batch.to_pylist():h.update(bytes.fromhex(sha(row)));count+=1
            self.guard()
        return count,h.hexdigest()

    def table_digest(self,name,snapshot):
        query=f'SELECT * FROM {self.relation(name,snapshot)} ORDER BY {KEYS[name]}'
        return self.typed_digest(self.db.execute(query).fetch_record_batch(getattr(self,'digest_batch_rows',512)))

    def check_element_ordinals(self,relation):
        if self.db.execute(f'SELECT 1 FROM {relation} GROUP BY message_id HAVING min(ordinal)<>0 OR max(ordinal)+1<>count(*) OR count(DISTINCT ordinal)<>count(*) LIMIT 1').fetchone():raise ValueError('元素序号损坏')

    def checkpoint_data(self,expected):
        self.flush();snapshot=self.db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        tables={}
        for name,cols in OBS_TABLES.items():
            self.guard()
            count,digest=self.table_digest(name,snapshot)
            actual_count,ceiling=self.db.execute(f'SELECT count(*),coalesce(max(rowid),-1) FROM lake.{self.schema}."{name}" AT (VERSION => {int(snapshot)}) WHERE attempt=?',[self.attempt]).fetchone()
            if actual_count!=count:raise ValueError('摘要与固定快照实际行数不符')
            tables[name]=dict(count=count,digest=digest,max_rowid=ceiling)
        m=self.relation('messages',snapshot);e=self.relation('elements',snapshot);p=self.relation('paths',snapshot)
        records=self.db.execute(f'SELECT count(*),count(DISTINCT record),min(record),max(record) FROM {m}').fetchone()
        if records[0] and records!=(records[0],records[0],0,records[0]-1):raise ValueError('消息记录缺漏/重复')
        self.check_element_ordinals(e)
        if self.db.execute(f'SELECT 1 FROM {e} e LEFT JOIN {m} m USING(message_id) LEFT JOIN {p} p USING(path_key) WHERE m.message_id IS NULL OR p.path_key IS NULL LIMIT 1').fetchone():raise ValueError('孤立引用')
        for name in ('eor','quality','associations'):
            attached=self.relation(name,snapshot)
            if self.db.execute(f'SELECT 1 FROM {attached} a LEFT JOIN {m} m USING(message_id) WHERE a.message_id IS NOT NULL AND m.message_id IS NULL LIMIT 1').fetchone():raise ValueError('消息附属表孤立引用')
        actual_status=dict(self.db.execute(f"SELECT json_extract_string(interpretation,'$.status'),count(*) FROM {m} GROUP BY 1").fetchall())
        if any(actual_status.get(k,0)!=expected.get(k,0) for k in ('decoded','unsupported','rejected')):raise ValueError('持久化解释分类计数不符')
        if tables['messages']['count']!=expected.get('messages',0) or tables['elements']['count']!=expected.get('elements',0) or tables['references']['count']!=expected.get('references',0):raise ValueError('生产与保存计数不符')
        files=[]
        for path,(table,size) in self.files(snapshot).items():
            if path in self.before_files:continue
            # 文件真实内容必须仅属本attempt，不靠begin_snapshot猜归属。
            identities=self.db.execute('SELECT DISTINCT attempt FROM read_parquet(?)',[path]).fetchall()
            if (self.attempt,) not in identities:continue
            if identities!=[(self.attempt,)]:raise ValueError('数据文件跨attempt混合')
            files.append(dict(path=path,table=table,size=size,sha256=file_sha(path)))
        return dict(snapshot=snapshot,tables=tables,files=files,flushes=self.flushes,io=dict(data_file_hash_bytes=sum(f['size'] for f in files),persisted_audit_rows=sum(t['count'] for t in tables.values()),physical_file_count=len(files),scope='logical_rows_and_file_bytes_not_os_cache_misses'))

    def close(self):self.db.close()


def verify_checkpoint(cp):
    for f in cp['files']:
        if Path(f['path']).stat().st_size!=f['size'] or file_sha(f['path'])!=f['sha256']:raise ValueError('checkpoint数据文件漂移')
    body={k:v for k,v in cp.items() if k!='digest'}
    if sha(body)!=cp['digest']:raise ValueError('checkpoint正文摘要不符')


def produce_checkpointed(manifest,dsn,output,*,run_id=None,policy=ReadPolicy.STRICT,catalog_data_path=None,
                         batch_rows=50000,min_free_bytes=2*1024**3,max_rss_bytes=4*1024**3,duckdb_memory_limit='1GB',native_build=None,hook=lambda *a:None,observer=None):
    """新阶段显式入口；不生成业务complete，不运行Replay/M3。"""
    if not 1<=batch_rows<=1000000 or min_free_bytes<0 or max_rss_bytes<1:raise ValueError('资源参数无效')
    root=Path(output).resolve();root.mkdir(parents=True,exist_ok=True)
    data_path=str(Path(catalog_data_path or root/'data').resolve())
    guard=Guard(root,data_path,min_free_bytes,max_rss_bytes);guard()
    plan,plan_id=plan_for(manifest,policy)
    runtime=None
    if observer is not None and native_build is None:raise ValueError('直接批次消费需要原生解析器')
    if native_build is not None:
        from data_pipeline.bgp.input.native_parser import NativeRuntime, PARSER_AUTHORITY
        runtime=NativeRuntime(native_build)
        if batch_rows>200000:raise ValueError('原生批大小超过 200000')
        plan['native']=runtime.identity;plan['parser_authority']=PARSER_AUTHORITY;plan['native_batch_rows']=batch_rows
        plan_id=sha([plan_id,runtime.identity,PARSER_AUTHORITY,batch_rows])
        pa.set_cpu_count(2);pa.set_io_thread_count(2)
    identity_path=root/'run.json'
    if identity_path.exists():
        saved=json.loads(identity_path.read_text())
        if run_id is not None and run_id!=saved['run_id']:raise ValueError('运行身份冲突')
        run_id=saved['run_id']
    else:
        run_id=run_id or uuid.uuid4().hex
        durable(identity_path,dict(run_id=run_id))
    if not run_id.isalnum():raise ValueError('run_id非法')
    owner=Owner(dsn,run_id);invocation=uuid.uuid4().hex;metrics=Metrics(root/('samples-'+invocation+'.jsonl'),resource_metrics=runtime is not None);store=None
    phase='prepare';active_ordinal=None;source_eof=False;file_selected=False;counts=None
    event(root,'invocation',run_id=run_id,invocation=invocation,pid=os.getpid(),pg_pid=owner.pg.get_backend_pid())
    try:
        setup(owner);schema='m2_'+run_id
        with owner.transaction() as c:
            c.execute('SELECT plan_id,data_path,state,seal FROM observation_m2.runs WHERE run_id=%s',(run_id,));old=c.fetchone()
            if old:
                if old[:2]!=(plan_id,data_path):raise ValueError('计划/代码/格式/结构/DATA_PATH漂移')
                if old[2]=='observation_sealed':
                    from data_pipeline.bgp.archive.selection import Selection
                    selection=Selection(dsn,run_id,old[3]['snapshot'])
                    checked=selection.connect();checked.close()
                    for cp in old[3]['checkpoints']:verify_checkpoint(cp)
                    for entry in plan['inputs']:
                        if file_sha(entry['path'])!=entry['sha256']:raise ValueError('封存来源SHA漂移')
                    selection.check()
                    if observer is not None:
                        for cp in old[3]['checkpoints']:observer.checkpoint(cp)
                    return old[3]
            else:c.execute('INSERT INTO observation_m2.runs VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL)',(run_id,plan_id,Json(plan),schema,data_path,'running'))
            c.execute("UPDATE observation_m2.runs SET state='running',reason=NULL WHERE run_id=%s",(run_id,))
            c.execute('SELECT ordinal,payload FROM observation_m2.checkpoints WHERE run_id=%s ORDER BY ordinal',(run_id,));checkpoints=[p for _,p in c.fetchall()]
        if [p['ordinal'] for p in checkpoints]!=list(range(len(checkpoints))):raise ValueError('checkpoint链不连续')
        context=dict(reference=None,reference_peers=[],baseline_elements=0,prior_update_last=None)
        previous=None
        for ordinal,entry in enumerate(plan['inputs']):
            guard()
            if ordinal<len(checkpoints):
                cp=checkpoints[ordinal];verify_checkpoint(cp)
                if cp['previous']!=previous or cp['source_id']!=entry['source_id']:raise ValueError('checkpoint源序/前驱损坏')
                if file_sha(entry['path'])!=entry['sha256']:raise ValueError('恢复原件SHA漂移')
                context=json.loads(json.dumps(cp['context']));previous=cp['digest'];event(root,'skip',ordinal=ordinal,attempt=cp['attempt'])
                if observer is not None:observer.checkpoint(cp)
                continue
            active_ordinal=ordinal;source_eof=False;file_selected=False;phase='prepare_file'
            attempt_root=root/('attempt-'+uuid.uuid4().hex);attempt_root.mkdir()
            native_file=None
            if runtime is not None and entry['role']!='reference':
                from data_pipeline.bgp.input.native_ingest import NativeFile
                native_file=NativeFile(root,ordinal,entry,runtime,dsn,schema,data_path,owner,guard,batch_rows,duckdb_memory_limit,metrics,ReadPolicy(policy).value)
                store=native_file.store
            else:store=AttemptStore(dsn,schema,data_path,attempt_root,owner,guard,batch_rows,duckdb_memory_limit)
            counts=dict(messages=0,elements=0,references=0,decoded=0,rejected=0,unsupported=0)
            source_stat=Path(entry['path']).stat()
            if entry.get('size',source_stat.st_size)!=source_stat.st_size:raise ValueError('声明来源大小不符')
            start=time.monotonic();event(root,'parse_start',ordinal=ordinal,attempt=store.attempt)
            if native_file is not None:
                phase='native_pipeline'
                counts=native_file.ingest(context,hook,counts,observer=observer);source_eof=True
            else:
                prior=None;rib_epoch=None
                iterator=iter(reference_rows(entry['path'],entry['sha256'])) if entry['role']=='reference' else iter(read_source(entry['path'],entry['sha256'],source_id=entry['source_id'],policy=policy))
                while True:
                    guard()
                    phase='parse'
                    with metrics.measure('parse'):
                        try:item=next(iterator)
                        except StopIteration:source_eof=True;break
                    phase='observation_write'
                    with metrics.measure('observation_write'):
                        if entry['role']=='reference':store.append('references',item);counts['references']+=1
                        else:
                            m=item
                            if entry['role'] in ('baseline','snapshot'):
                                if m.kind not in ('rib','peer_index_table'):raise ValueError('RIB角色/载荷拒绝')
                                if rib_epoch is None:rib_epoch=m.epoch
                                if rib_epoch!=m.epoch:raise ValueError('RIB时间冲突')
                            elif m.mrt_type not in (16,17):raise ValueError('UPDATE角色冲突')
                            store.message(m);counts['messages']+=1;counts['elements']+=len(m.elements);counts[m.interpretation.status.value]+=1
                            if entry['role']=='baseline' and m.kind=='peer_index_table':
                                context['reference']=dict(source_id=m.source_id,record=m.record,message_id=m.message_id,epoch=m.epoch,microsecond=m.microsecond)
                                context['reference_peers']=m.peers
                            if entry['role']=='baseline':context['baseline_elements']+=len(m.elements)
                            if m.peer and context['reference']:
                                from types import SimpleNamespace
                                ref=SimpleNamespace(**context['reference'])
                                peer_ref,reason=associate(m,ref,context['reference_peers'])
                                store.append('associations',dict(message_id=m.message_id,peer_ref=peer_ref,reference_message=ref.message_id,rule='bound_same_time_unique_endpoint/v1',reason=reason))
                            # ET精度来自独立可信头，拒绝payload不抹掉头；未知精度保留None。
                            stamp=[m.epoch,m.interpretation.header.microsecond if m.mrt_type==17 else 0]
                            known=stamp[1] is not None
                            if entry['role']=='update' and counts['messages']==1 and context['prior_update_last'] is not None and known and context['prior_update_last'][1] is not None and stamp<=context['prior_update_last']:
                                store.append('quality',dict(source_id=m.source_id,message_id=m.message_id,code='cross_file_time_overlap',detail='保留声明源序；时间连续性未知'))
                            if prior is not None and known and prior[1] is not None and stamp<prior:store.append('quality',dict(source_id=m.source_id,message_id=m.message_id,code='timestamp_regression',detail='保留物理顺序；时间连续性未知'))
                            prior=stamp
                    hook('file_mid',ordinal,owner)
                if entry['role']=='update':context['prior_update_last']=prior
            if counts['messages']!=counts['decoded']+counts['unsupported']+counts['rejected']:raise ValueError('解析状态计数不符')
            with metrics.measure('observation_write'):store.flush()
            phase='checkpoint'
            with metrics.measure('checkpoint'):
                if code_identity()!=plan['code']:raise ValueError('处理期间代码/规则漂移')
                data=store.checkpoint_data(counts)
                if native_file is not None:native_file.verify_paths(data)
                if entry['role']=='reference' and resolved_interpretation(entry['path'])!=entry['format']:raise ValueError('读取期间引用解释格式漂移')
                cp=dict(**data,ordinal=ordinal,source_id=entry['source_id'],attempt=store.attempt,previous=previous,context=json.loads(json.dumps(context)),
                    counts=counts,raw='verified_source_eof',parse='partial' if counts['rejected'] or counts['unsupported'] else 'complete',ingest='complete',
                    source_sha=entry['sha256'],source_stat=dict(size=source_stat.st_size,mtime_ns=source_stat.st_mtime_ns,ino=source_stat.st_ino),format=entry.get('format'),plan_id=plan_id,elapsed_seconds=time.monotonic()-start,
                    metrics=metrics.receipt(),metrics_scope='invocation_to_prepared',resources=dict(min_free_bytes=min_free_bytes,max_rss_bytes=max_rss_bytes,batch_rows=batch_rows,duckdb_memory_limit=duckdb_memory_limit),input_bytes=Path(entry['path']).stat().st_size)
                cp['digest']=sha(cp);durable(attempt_root/'prepared.json',cp)
                store.close();store=None  # 所有该attempt写入连接关闭后才选择。
                event(root,'before_checkpoint',ordinal=ordinal);hook('before_checkpoint',ordinal,owner)
                with owner.transaction() as c:
                    c.execute('SELECT payload FROM observation_m2.checkpoints WHERE run_id=%s ORDER BY ordinal DESC LIMIT 1',(run_id,));last=c.fetchone()
                    if (last[0]['digest'] if last else None)!=previous:raise ValueError('checkpoint前驱漂移')
                    c.execute('INSERT INTO observation_m2.checkpoints VALUES (%s,%s,%s)',(run_id,ordinal,Json(cp)))
                file_selected=True
                event(root,'after_checkpoint',ordinal=ordinal);hook('after_checkpoint',ordinal,owner)
                if observer is not None:observer.checkpoint(cp)
                checkpoints.append(cp);previous=cp['digest']
        phase='seal'
        event(root,'before_seal',ordinal=len(checkpoints));hook('before_seal',len(checkpoints),owner)
        with metrics.measure('seal'):
            if code_identity()!=plan['code']:raise ValueError('封存前代码/规则漂移')
            if runtime is None:seal=seal_run(dsn,schema,data_path,root,owner,checkpoints,plan_id,guard,hook)
            else:
                from data_pipeline.bgp.input.native_ingest import seal_native
                seal=seal_native(dsn,schema,data_path,root,owner,checkpoints,plan_id,guard,hook,runtime,duckdb_memory_limit,metrics)
        durable(root/('metrics-'+invocation+'.json'),metrics.receipt())
        return seal
    except BaseException as exc:
        # 失锁旧进程不允许通过另一个连接补写failed。
        try:
            with owner.transaction() as c:c.execute("UPDATE observation_m2.runs SET state='failed',reason=%s WHERE run_id=%s AND state<>'observation_sealed'",(str(exc),run_id))
        except BaseException:pass
        durable(root/('failure-'+invocation+'.json'),dict(invocation=invocation,ordinal=active_ordinal,phase=phase,
            source_eof=source_eof,file_selected=file_selected,raw='complete' if source_eof else 'not_complete',
            parse='not_completed' if not source_eof else 'see_checkpoint',ingest='checkpointed' if file_selected else 'unqualified_partial_or_unknown',
            received_counts=counts,reason=str(exc),metrics=metrics.receipt()))
        event(root,'error',reason=str(exc),invocation=invocation,phase=phase)
        raise
    finally:
        if store:store.close()
        owner.close();metrics.close()


def seal_run(dsn,schema,data_path,root,owner,checkpoints,plan_id,guard,hook):
    db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres')
    db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (DATA_PATH '+literal(data_path)+')')
    seal_id=uuid.uuid4().hex
    try:
        visible_files=set()
        for name in OBS_TABLES:
            for path,deleted in db.execute('SELECT data_file,delete_file FROM ducklake_list_files(?,?,schema => ?)', ['lake',name,schema]).fetchall():
                if deleted is not None:raise ValueError('封存前存在删除/更新文件，禁止选择')
                visible_files.add(path)
        for cp in checkpoints:
            if any(f['path'] not in visible_files for f in cp['files']):raise ValueError('checkpoint文件不再可见，禁止选择')
        for cp in checkpoints:
            guard();verify_checkpoint(cp)
            # 精确checkpoint snapshot读取，仅扫描paths建立索引，不重写全部观察。
            result=db.execute(f'SELECT * EXCLUDE(attempt) FROM lake.{schema}.paths AT (VERSION => {cp["snapshot"]}) WHERE attempt=? ORDER BY path_key',[cp['attempt']])
            # 独立游标连接避免写owners中断流式结果。
            rows=result.fetch_record_batch(512)
            # 使用独立本地索引连接，内存受DuckDB限制，不缓存全量路径。
            if 'index' not in locals():index=connect_duckdb(str(Path(root)/('index-'+seal_id+'.duckdb')));index.execute('CREATE TABLE owners(path_key VARCHAR PRIMARY KEY,attempt VARCHAR,digest VARCHAR)')
            for batch in rows:
                guard()
                candidates=[dict(path_key=row['path_key'],attempt=cp['attempt'],digest=sha(row)) for row in batch.to_pylist()]
                index.register('candidates',pa.Table.from_pylist(candidates))
                if index.execute('SELECT 1 FROM candidates c JOIN owners o USING(path_key) WHERE c.digest<>o.digest LIMIT 1').fetchone():raise ValueError('同path_key不同typed正文')
                index.execute('INSERT INTO owners SELECT * FROM candidates ON CONFLICT DO NOTHING')
                index.unregister('candidates')
        owner.check()
        owner_count=0;owner_digest=hashlib.sha256()
        if 'index' in locals():
            for batch in index.execute('SELECT path_key,attempt FROM owners ORDER BY path_key').fetch_record_batch(512):
                for row in batch.to_pylist():owner_digest.update(bytes.fromhex(sha(row)));owner_count+=1
                db.register('owners_batch',batch);db.execute(f'INSERT INTO lake.{schema}.path_owner SELECT ?,* FROM owners_batch',[seal_id]);db.unregister('owners_batch')
        db.register('seal_batch',pa.Table.from_pylist([dict(seal_id=seal_id,source_id=cp['source_id'],attempt=cp['attempt'],ordinal=cp['ordinal'],checkpoint_digest=cp['digest']) for cp in checkpoints]))
        db.execute(f'INSERT INTO lake.{schema}.seal_selection SELECT * FROM seal_batch');db.unregister('seal_batch')
        snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        seal=dict(version=VERSION,run_id=owner.run_id,plan_id=plan_id,schema=schema,snapshot=snapshot,seal_id=seal_id,
                  checkpoints=checkpoints,io=dict(observation_rows_rewritten=0,path_owner_rows_written=owner_count,selection_rows_written=len(checkpoints),data_file_hash_bytes=sum(f['size'] for cp in checkpoints for f in cp['files']),scope='seal_verification_and_index_only'),path_owner_count=owner_count,path_owner_digest=owner_digest.hexdigest(),qualification='observation_sealed',business='not_run')
        seal['digest']=sha(seal);durable(Path(root)/('seal-'+seal_id+'.json'),seal)
        hook('seal_pre_commit',len(checkpoints),owner)
        with owner.transaction() as c:c.execute("UPDATE observation_m2.runs SET state='observation_sealed',seal=%s WHERE run_id=%s",(Json(seal),owner.run_id))
        hook('seal_post_commit',len(checkpoints),owner)
        return seal
    finally:
        if 'index' in locals():index.close()
        db.close()
