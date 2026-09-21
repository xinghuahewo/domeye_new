"""独立 M3 投影存储与固定封印 Reader；不调用旧 Store.finish。"""
from dataclasses import asdict
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import uuid
import psycopg2
from psycopg2.extras import Json
import pyarrow as pa

from data_pipeline.bgp.archive.store import connect_duckdb, literal
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.ordered_reader import binding_from_reader
from data_pipeline.bgp.replay.snapshot_contract import TABLES, COLUMNS, PROFILE, ProjectionBinding, encode, decode, digest
from data_pipeline.bgp.replay.quality_overlay import ScopeIndex
from data_pipeline.bgp.replay.route_replay import identity
from data_pipeline.bgp.replay.snapshot_validation import CODEC, validate_rules, validate_row, validate_contents


@contextmanager
def closing(resource):
    """关闭失败保留主异常；显式早停时仍暴露关闭失败。"""
    primary=None
    try:
        yield resource
    except BaseException as exc:
        primary=exc
        raise
    finally:
        try:resource.close()
        except BaseException as failure:
            if primary is not None and not isinstance(primary,GeneratorExit):
                primary.cleanup_errors=(*getattr(primary,'cleanup_errors',()),failure)
            else:
                failure.cleanup_errors=(failure,)
                raise failure from primary


def database_identity(dsn):
    pg=psycopg2.connect(dsn)
    try:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute('SELECT system_identifier::text FROM pg_control_system()');system=c.fetchone()[0]
            c.execute('SELECT oid,current_database() FROM pg_database WHERE datname=current_database()');oid,name=c.fetchone()
            c.execute("SELECT value FROM ducklake_metadata WHERE key='data_path' AND scope IS NULL");root=c.fetchone()[0]
        return dict(system_identifier=system,database_oid=oid,database_name=name,
                    catalog='lake',catalog_backend='postgres',data_root=root)
    finally:pg.close()


def file_sha(path,guard=lambda:None):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):
            guard();h.update(block)
    return h.hexdigest()


def relation(schema,snapshot,table):
    if table not in TABLES:raise ValueError('未知 M3 投影表')
    base=f'lake.{schema}."{table}"'+(f' AT (VERSION => {int(snapshot)})' if snapshot is not None else '')
    return '(SELECT * FROM '+base+')'


def _ledger(db,schema,snapshot,guard=lambda:None):
    result={}
    for table in TABLES:
        guard()
        count=0;hashed=hashlib.sha256()
        for batch in db.execute(f'SELECT * FROM {relation(schema,snapshot,table)} ORDER BY seq').fetch_record_batch(128):
            for row in batch.to_pylist():
                guard()
                if type(row['payload']) is not str or len(row['payload'].encode())>4*1024**2:raise ValueError('投影摘要单行字节资源上限')
                if row['seq']!=count:raise ValueError('投影表序号缺失或重复')
                hashed.update(encode(tuple(row[k] for k in COLUMNS)).encode()+b'\n');count+=1
        result[table]=dict(rows=count,sha256=hashed.hexdigest())
    return result


def _validate(db,schema,snapshot,input_reader,plan,guard=lambda:None):
    validate_rules(plan,profile=PROFILE,codec=CODEC,tables=TABLES)
    guard()
    t=lambda name:relation(schema,snapshot,name)
    names={r[0] for r in db.execute("SELECT table_name FROM information_schema.tables WHERE table_catalog='lake' AND table_schema=?",[schema]).fetchall()}
    if names!=set(TABLES):raise ValueError('独立投影表枚举不完整')
    selected=plan['selected_sources'];where=','.join(literal(s) for s in selected)
    rows=[decode(r[0]) for r in db.execute(f'SELECT payload FROM {t("source_coverage")} ORDER BY seq').fetchall()]
    if [r['source_id'] for r in rows]!=selected:raise ValueError('缺少完整来源覆盖')
    expected={s.source_id:s for s in binding_from_reader(input_reader).sources}
    for row in rows:
        source=expected[row['source_id']]
        if (row['raw']['messages'],row['raw']['elements'])!=(source.messages,source.elements):raise ValueError('来源覆盖原计数不符')
        if any(row['parse_counts'][k]!=getattr(source,k) for k in ('decoded','rejected','unsupported')):raise ValueError('来源解释覆盖计数不符')
        if set(row['derived_counts'])!=set(('changes','invalidations','scope_gap','qualification_change','source_quality')):raise ValueError('来源派生枚举缺失')
        for table,expected_count in row['derived_counts'].items():
            if table not in ('changes','invalidations','scope_gap','qualification_change','source_quality'):raise ValueError('来源派生枚举无效')
            if db.execute(f'SELECT count(*) FROM {t(table)} WHERE source_id=?',[source.source_id]).fetchone()[0]!=expected_count:raise ValueError('来源派生覆盖计数不符')
        gaps=db.execute(f'SELECT count(*) FROM {t("scope_gap")} WHERE source_id=?',[source.source_id]).fetchone()[0]
        if gaps!=source.rejected+source.unsupported or row['parse_counts']['gaps']!=gaps:raise ValueError('漏 Gap 或重复 Gap')
    messages=input_reader.bound_table('messages');elements=input_reader.bound_table('elements')
    expected_changes=db.execute(f'SELECT count(*) FROM {elements} e JOIN {messages} m USING(message_id) WHERE m.source_id IN ({where}) AND NOT coalesce(m.local_message,false)').fetchone()[0]
    actual=db.execute(f'SELECT count(*),count(DISTINCT event_id) FROM {t("changes")}').fetchone()
    if actual!=(expected_changes,expected_changes):raise ValueError('合法 received 元素与变化未一一对应')
    bad=db.execute(f'''SELECT count(*) FROM {t("changes")} c LEFT JOIN {elements} e USING(event_id)
        LEFT JOIN {messages} m ON e.message_id=m.message_id
        WHERE e.event_id IS NULL OR c.message_id!=e.message_id OR c.source_id!=m.source_id OR m.source_id NOT IN ({where})''').fetchone()[0]
    if bad:raise ValueError('变化原观察引用损坏')
    bad=db.execute(f'''SELECT count(*) FROM {t("scope_gap")} g LEFT JOIN {messages} m USING(message_id)
        WHERE m.message_id IS NULL OR g.source_id!=m.source_id OR m.source_id NOT IN ({where})
        OR json_extract_string(m.interpretation,'$.status')='decoded' ''').fetchone()[0]
    if bad:raise ValueError('Gap 原解释引用损坏')
    n,unique=db.execute(f'SELECT count(*),count(DISTINCT message_id) FROM {t("scope_gap")}').fetchone()
    if n!=unique:raise ValueError('同消息多次 Gap')
    bad=db.execute(f'''SELECT count(*) FROM {t("invalidations")} i LEFT JOIN {messages} m USING(message_id)
        WHERE m.kind!='state_change' OR m.message_id IS NULL''').fetchone()[0]
    if bad:raise ValueError('非真实 STATE 混入失效')
    objects=db.execute(f'SELECT count(DISTINCT object_key) FROM {t("changes")}').fetchone()[0]
    current=db.execute(f'SELECT count(*),count(DISTINCT object_key) FROM {t("current_routes")}').fetchone()
    if current!=(objects,objects):raise ValueError('当前对象导出不完整')
    missing=db.execute(f'''SELECT count(*) FROM {t("scope_gap")} g LEFT JOIN {t("qualification_change")} q
        ON g.gap_id=q.gap_id AND q.object_key IS NULL WHERE q.gap_id IS NULL''').fetchone()[0]
    if missing:raise ValueError('缺少零命中范围资格')
    refs=[decode(r[0]) for r in db.execute(f'SELECT payload FROM {t("reference_binding")} ORDER BY seq').fetchall()]
    if refs!=plan['references']:raise ValueError('参考绑定或完成回执不符')
    if db.execute(f'SELECT count(*) FROM {t("projection_metadata")}').fetchone()[0]!=1:raise ValueError('缺少原完整状态元数据')
    return validate_contents(db,schema,snapshot,input_reader,plan,guard)


class ProjectionWriter:
    def __init__(self,reader,root,plan,*,batch_rows=2048,memory_limit='1GB',guard=lambda:None):
        validate_rules(plan,profile=PROFILE,codec=CODEC,tables=TABLES)
        self.prepared=None
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=False)
        self.dsn=reader.dsn;self.input_reader=reader;self.plan=plan;self.guard=guard
        self.run_id=uuid.uuid4().hex;self.schema='m3_'+self.run_id
        self.hashes={t:hashlib.sha256() for t in TABLES}
        self.counts={t:0 for t in TABLES};self.pending={t:[] for t in TABLES};self.batch_rows=batch_rows
        self.payload_bytes=0;self.db=None;self.pg=None;self.snapshot=None
        try:
            self.pg=psycopg2.connect(self.dsn)
            with self.pg,self.pg.cursor() as c:
                c.execute('CREATE SCHEMA IF NOT EXISTS m3_projection')
                c.execute('''CREATE TABLE IF NOT EXISTS m3_projection.runs(run_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,profile TEXT NOT NULL,schema_name TEXT NOT NULL,
                    snapshot BIGINT,seal JSONB,reason TEXT,control_sha TEXT,plan JSONB)''')
                c.execute('INSERT INTO m3_projection.runs VALUES (%s,%s,%s,%s,NULL,NULL,NULL,NULL,%s)',
                          (self.run_id,'not_run',PROFILE,self.schema,Json(plan)))
            self.db=connect_duckdb(str(self.root/'staging.duckdb'))
            self.db.execute('SET memory_limit='+literal(memory_limit))
            self.db.execute('SET temp_directory='+literal(self.root/'temp'))
            self.db.execute('LOAD ducklake');self.db.execute('LOAD postgres')
            self.db.execute('ATTACH '+literal('ducklake:postgres:'+self.dsn)+' AS lake (DATA_INLINING_ROW_LIMIT 0)')
            self.db.execute('CREATE SCHEMA lake.'+self.schema)
            for name in TABLES:
                self.db.execute(f'CREATE TABLE lake.{self.schema}."{name}" (seq BIGINT,'+
                                ','.join(f'{c} VARCHAR' for c in COLUMNS[1:])+')')
            with self.pg,self.pg.cursor() as c:
                c.execute("UPDATE m3_projection.runs SET state='running' WHERE run_id=%s AND state='not_run'",(self.run_id,))
        except BaseException as exc:
            for action in (lambda:self.fail(exc),self.close):
                try:action()
                except BaseException as cleanup:exc.cleanup_errors=(*getattr(exc,'cleanup_errors',()),cleanup)
            raise

    def append(self,table,row):
        self.guard();validate_row(table,row);self.prepared=None
        if table not in TABLES:raise ValueError('未知 M3 投影表')
        payload=encode(row);seq=self.counts[table];self.counts[table]+=1
        self.payload_bytes+=len(payload.encode())
        physical=dict(seq=seq,**{k:row.get(k) for k in COLUMNS[1:-1]},payload=payload)
        self.hashes[table].update(encode(tuple(physical[k] for k in COLUMNS)).encode()+b'\n')
        self.pending[table].append(physical)
        if sum(map(len,self.pending.values()))>=self.batch_rows:self.flush()

    def flush(self):
        self.guard()
        for table,rows in self.pending.items():
            if not rows:continue
            arrow=pa.Table.from_pylist(rows,schema=pa.schema([('seq',pa.int64()),*[(k,pa.string()) for k in COLUMNS[1:]]]))
            self.db.register('projection_batch',arrow)
            try:self.db.execute(f'INSERT INTO lake.{self.schema}."{table}" SELECT * FROM projection_batch')
            finally:self.db.unregister('projection_batch')
            rows.clear()

    def prepare(self):
        self.flush();self.snapshot=self.db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        self.validation=_validate(self.db,self.schema,self.snapshot,self.input_reader,self.plan,self.guard)
        ledger=_ledger(self.db,self.schema,self.snapshot,self.guard)
        if ledger!={t:dict(rows=self.counts[t],sha256=self.hashes[t].hexdigest()) for t in TABLES}:raise ValueError('输出落盘正文或计数不符')
        self.prepared=(self.snapshot,encode(self.plan),encode(ledger))
        return ledger

    def files(self):
        rows=[]
        for table in TABLES:
            for path,size,deleted in self.db.execute('SELECT data_file,data_file_size_bytes,delete_file FROM ducklake_list_files(?,?,schema => ?,snapshot_version => ?)',
                    ['lake',table,self.schema,self.snapshot]).fetchall():
                if deleted is not None:raise ValueError('独立投影不应有删除文件')
                rows.append(dict(table=table,path=path,bytes=size,sha256=file_sha(path,self.guard)))
        return rows

    def finish(self,ledger,metrics):
        validate_rules(self.plan,profile=PROFILE,codec=CODEC,tables=TABLES)
        if self.prepared!=(self.snapshot,encode(self.plan),encode(ledger)):
            raise ValueError('投影未按固定计划和快照完成正文验证')
        self.input_reader.selection.check()
        self.input_reader.check_sources(self.input_reader.starts)
        db=self.input_reader.connect();db.close()
        root_stat=self.root.stat()
        seal=dict(profile=PROFILE,run_id=self.run_id,schema=self.schema,snapshot=self.snapshot,plan=self.plan,
                  tables=ledger,execution='complete',business='not_assessed',metrics=metrics,
                  database=database_identity(self.dsn),control_root=str(self.root.resolve()),
                  control_entity=dict(device=root_stat.st_dev,inode=root_stat.st_ino),codec=CODEC)
        seal['digest']=digest(seal)
        # 关闭输出写入连接后才提交权威 complete；失败不留下可读封印。
        self.db.close();self.db=None
        with (self.root/'execution.json').open('x') as f:
            json.dump(seal,f,ensure_ascii=False,indent=2);f.flush();os.fsync(f.fileno())
        with self.pg,self.pg.cursor() as c:
            c.execute("UPDATE m3_projection.runs SET state='complete',snapshot=%s,seal=%s,control_sha=%s WHERE run_id=%s AND state='running'",
                      (self.snapshot,Json(seal),file_sha(self.root/'execution.json'),self.run_id))
            if c.rowcount!=1:raise ValueError('投影完成状态冲突')
        return ProjectionBinding(self.run_id,self.snapshot,seal['digest'])

    def fail(self,reason,metrics=None):
        if self.pg is None:return
        self.pg.rollback()
        with self.pg,self.pg.cursor() as c:
            c.execute("UPDATE m3_projection.runs SET state='failed',reason=%s WHERE run_id=%s AND state IN ('not_run','running')",(json.dumps(str(reason),ensure_ascii=True),self.run_id))
            c.execute('SELECT state FROM m3_projection.runs WHERE run_id=%s',(self.run_id,));state=c.fetchone()
        if state==('failed',):
            with (self.root/'failure.json').open('x') as f:
                json.dump(dict(run_id=self.run_id,profile=PROFILE,execution='failed',plan=self.plan,reason=str(reason),metrics=metrics),f,ensure_ascii=False,indent=2)
                f.flush();os.fsync(f.fileno())

    def close(self):
        errors=[]
        for name in ('db','pg'):
            resource=getattr(self,name)
            if resource is None:continue
            setattr(self,name,None)
            try:resource.close()
            except BaseException as exc:errors.append(exc)
        if errors:
            errors[0].cleanup_errors=(*getattr(errors[0],'cleanup_errors',()),*errors[1:])
            raise errors[0]


class ProjectionReader:
    def __init__(self,dsn,binding,*,guard=lambda:None):
        if not isinstance(binding,ProjectionBinding) or binding.profile!=PROFILE:raise ValueError('必须显式绑定 M3 profile')
        if not binding.run_id.isalnum() or type(binding.snapshot) is not int or binding.snapshot<0:raise ValueError('投影绑定无效')
        self.dsn=dsn;self.binding=binding;self.guard=guard;self.seal=self._read_seal()
        self.schema=self.seal['schema']
        if self.schema!='m3_'+binding.run_id or set(self.seal['tables'])!=set(TABLES):raise ValueError('投影 schema/枚举不符')
        p=self.seal['plan'];b=p['input_binding']
        self.input_reader=ObservationReader(dsn,b['observation_run'],b['seal_snapshot'],p['selected_sources'],profile='observation')
        if json.loads(json.dumps(asdict(binding_from_reader(self.input_reader))))!=b:raise ValueError('M2 输入绑定漂移')

    def _read_seal(self):
        pg=psycopg2.connect(self.dsn)
        try:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute('SELECT state,profile,snapshot,seal,control_sha,plan FROM m3_projection.runs WHERE run_id=%s',(self.binding.run_id,));row=c.fetchone()
            if row is None or row[:3]!=('complete',PROFILE,self.binding.snapshot):raise ValueError('未完成、失败或错版投影不可读')
            seal=row[3]
            validate_rules(seal.get('plan'),profile=seal.get('profile'),codec=seal.get('codec'),tables=seal.get('tables',{}))
            if row[5]!=seal['plan']:raise ValueError('投影登记输入计划漂移')
            if seal['digest']!=self.binding.seal_digest or digest({k:v for k,v in seal.items() if k!='digest'})!=seal['digest']:raise ValueError('投影封印摘要不符')
            root=Path(seal['control_root']);stat=root.stat()
            if dict(device=stat.st_dev,inode=stat.st_ino)!=seal['control_entity']:raise ValueError('可信控制目录实体改变')
            content=(root/'execution.json').read_bytes()
            if hashlib.sha256(content).hexdigest()!=row[4] or json.loads(content)!=seal:raise ValueError('控制回执与 PG 完成登记不符')
            if database_identity(self.dsn)!=seal['database']:raise ValueError('PG 系统/数据库/catalog 实体不符')
            return seal
        finally:pg.close()

    def _connect(self):
        self.guard()
        if self._read_seal()!=self.seal:raise ValueError('投影固定选择漂移')
        return self.input_reader.connect()  # M2 固定物理封印资格一并校验。

    def scan(self,table,batch_rows=2048,max_row_bytes=4*1024**2):
        if table not in TABLES or not 1<=batch_rows<=10000 or max_row_bytes<1:raise ValueError('读取表/批大小无效')
        count=0;hashed=hashlib.sha256()
        with closing(self._connect()) as db:
            for batch in db.execute(f'SELECT * FROM {relation(self.schema,self.binding.snapshot,table)} ORDER BY seq').fetch_record_batch(min(batch_rows,128)):
                for row in batch.to_pylist():
                    self.guard()
                    if len(row['payload'].encode())>max_row_bytes:raise ValueError('投影单行字节资源上限')
                    if row['seq']!=count:raise ValueError('投影行序不符')
                    hashed.update(encode(tuple(row[k] for k in COLUMNS)).encode()+b'\n');count+=1
                    payload=decode(row['payload']);validate_row(table,payload,physical=row)
                    yield payload
            if self.seal['tables'][table]!=dict(rows=count,sha256=hashed.hexdigest()):raise ValueError('投影表内容与封印不符')
            _validate(db,self.schema,self.binding.snapshot,self.input_reader,self.seal['plan'],self.guard)
            self.input_reader.selection.check()
            if self._read_seal()!=self.seal:raise ValueError('读取尾投影状态漂移')
        return dict(table=table,rows=count,sha256=hashed.hexdigest(),execution='complete',binding=asdict(self.binding))

    def descriptor(self):
        with closing(self._connect()) as db:
            return dict(binding=asdict(self.binding),database=self.seal['database'],schema=self.schema,
                manifest_sha=file_sha(Path(self.seal['control_root'])/'execution.json',self.guard),
                control_root=self.seal['control_root'],control_entity=self.seal['control_entity'],
                state='complete',plan=self.seal['plan'],codec=self.seal['codec'],
                tables=self.seal['tables'],files=self.seal['metrics']['parquet_files'])

    def audit(self):
        with closing(self._connect()) as db:
            for f in self.seal['metrics']['parquet_files']:
                if Path(f['path']).stat().st_size!=f['bytes'] or file_sha(f['path'],self.guard)!=f['sha256']:raise ValueError('M3 Parquet 文件漂移')
            dependencies=set(self.seal['plan']['selected_sources'])|{r['source_id'] for r in self.seal['plan']['references']}
            for cp in self.input_reader.selection.checkpoints:
                if cp['source_id'] not in dependencies:continue
                for f in cp['files']:
                    if Path(f['path']).stat().st_size!=f['size'] or file_sha(f['path'],self.guard)!=f['sha256']:raise ValueError('绑定 M2 文件漂移')
            _validate(db,self.schema,self.binding.snapshot,self.input_reader,self.seal['plan'],self.guard)
            if _ledger(db,self.schema,self.binding.snapshot,self.guard)!=self.seal['tables']:raise ValueError('全表审计摘要不符')
            self.input_reader.selection.check()
            if self._read_seal()!=self.seal:raise ValueError('审计尾投影状态漂移')
            return dict(profile=PROFILE,run_id=self.binding.run_id,tables=self.seal['tables'],audit='complete',descriptor=self.descriptor())

    verify = audit

    @contextmanager
    def locked(self):
        """调用方持有的 PG 共享行锁；不声称锁住外部文件或替调用方提交事务。"""
        pg=psycopg2.connect(self.dsn)
        try:
            with pg.cursor() as c:
                c.execute('SELECT run_id FROM m3_projection.runs WHERE run_id=%s FOR SHARE',(self.binding.run_id,))
                rid=self.seal['plan']['input_binding']['observation_run']
                c.execute('SELECT run_id FROM observation_m2.runs WHERE run_id=%s FOR SHARE',(rid,))
                c.execute('SELECT ordinal FROM observation_m2.checkpoints WHERE run_id=%s FOR SHARE',(rid,))
                c.fetchall()
            yield self.verify()
            self.verify()
        finally:
            try:pg.rollback()
            finally:pg.close()

    def current(self,scope):
        key=tuple(scope)
        if len(key)!=11 or key[0]!=self.seal['plan']['input_binding']['collector'] or type(key[9]) is not bool or (key[9] and type(key[10]) is not int) or (not key[9] and key[10] is not None):raise ValueError('对象完整 typed scope 无效')
        # 当前行与 Gap 都经完整扫描摘要核验，有限版本不以索引命中代替完整性。
        found=None
        for row in self.scan('current_routes'):
            if row['object_key']==identity(key):found=row
        if found is not None:return found
        index=ScopeIndex()
        for row in self.scan('scope_gap'):index.add(row['raw'])
        q=index.qualification(key);q['current']='unknown'
        return dict(object_key=identity(key),scope=key,current=None,last_known=None,last_position=None,qualification=q)
