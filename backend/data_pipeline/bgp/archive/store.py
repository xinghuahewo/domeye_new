"""DuckLake历史与PG业务登记；仅显式隔离离线调用，无Web启动副作用。"""
from pathlib import Path
import os
import re
import time
import uuid
import duckdb
import pyarrow as pa
import psycopg2
from psycopg2.extras import execute_values

TABLES = {
 'baseline_mappings': [('baseline_source','VARCHAR'),('peer_ip','VARCHAR'),('peer_asn','BIGINT'),('status','VARCHAR'),('rule','VARCHAR'),('peer_refs','VARCHAR'),('endpoint_evidence','VARCHAR'),('input_sources','VARCHAR[]')],
 'projection_metadata': [('name','VARCHAR'),('value','VARCHAR')],
 'invalidations': [('object_key','VARCHAR'),('source','VARCHAR'),('reason','VARCHAR'),('epoch','BIGINT'),
   ('scope','VARCHAR[]'),('last_known_event','VARCHAR'),('last_known_presence','VARCHAR'),('last_known_path','VARCHAR')],
 'references': [('source_id','VARCHAR'),('row','BIGINT'),('location','VARCHAR'),('raw_row','VARCHAR'),('csv_record_kind','VARCHAR'),('csv_physical_start','BIGINT'),('csv_physical_end','BIGINT'),('raw_byte_offset','BIGINT'),('raw_byte_length','BIGINT'),('raw_record_sha256','VARCHAR'),('effective_time_state','VARCHAR'),('rule','VARCHAR')],
 'associations': [('message_id','VARCHAR'),('peer_ref','VARCHAR'),('reference_message','VARCHAR'),('rule','VARCHAR'),('reason','VARCHAR')],
 'quality': [('source_id','VARCHAR'),('message_id','VARCHAR'),('code','VARCHAR'),('detail','VARCHAR')],
 'legacy_state': [('prefix','VARCHAR'),('vp','VARCHAR'),('path_text','VARCHAR')],
 'legacy_origin_index': [('prefix','VARCHAR'),('origin','VARCHAR')],
 'legacy_seen_vps': [('vp','VARCHAR')],
 'messages': [('message_id','VARCHAR'),('source_id','VARCHAR'),('content_sha256','VARCHAR'),('record','BIGINT'),('offset','BIGINT'),
   ('length','BIGINT'),('epoch','BIGINT'),('microsecond','INTEGER'),('mrt_type','INTEGER'),
   ('mrt_subtype','INTEGER'),('raw_digest','VARCHAR'),('kind','VARCHAR'),('reason','VARCHAR'),
   ('peer_ip','VARCHAR'),('peer_asn','BIGINT'),('bgp_id','VARCHAR'),('bgp_id_present','BOOLEAN'),
   ('local_ip','VARCHAR'),('local_asn','BIGINT'),('interface','INTEGER'),('endpoint_afi','INTEGER'),('local_message','BOOLEAN'),
   ('old_state','INTEGER'),('new_state','INTEGER')],
 'elements': [('event_id','VARCHAR'),('message_id','VARCHAR'),('ordinal','BIGINT'),('action','VARCHAR'),
   ('afi','INTEGER'),('safi','INTEGER'),('prefix','VARCHAR'),('raw_prefix','BLOB'),
   ('path_id','BIGINT'),('path_id_present','BOOLEAN'),('originated_epoch','BIGINT'),
   ('peer_ip','VARCHAR'),('peer_asn','BIGINT'),('bgp_id','VARCHAR'),('bgp_id_present','BOOLEAN'),
   ('peer_table_record','BIGINT'),('peer_index','BIGINT'),('path_key','VARCHAR'),
   ('attributes_offset','BIGINT'),('attributes_length','BIGINT'),('attributes_digest','VARCHAR')],
 'paths': [('path_key','VARCHAR'),('attributes_raw','BLOB'),('attributes_digest','VARCHAR'),
   ('asn_width','INTEGER'),('as_path_raw','BLOB'),('as4_path_raw','BLOB'),('as_path_text','VARCHAR'),
   ('as4_path_text','VARCHAR'),('raw_origin_asn','BIGINT'),('attributed_origin_asn','BIGINT'),('reason','VARCHAR')],
 'peers': [('source_id','VARCHAR'),('table_record','BIGINT'),('index','BIGINT'),('bgp_id','VARCHAR'),
   ('ip','VARCHAR'),('asn','BIGINT'),('bgp_id_present','BOOLEAN')],
 'eor': [('message_id','VARCHAR'),('afi','INTEGER'),('safi','INTEGER')],
 'changes': [('source_rank','BIGINT'),('record','BIGINT'),('ordinal','BIGINT'),('event_id','VARCHAR'),('object_key','VARCHAR'),('scope','VARCHAR[]'),('before_event','VARCHAR'),('after_event','VARCHAR'),('after_epoch','BIGINT'),('before_presence','VARCHAR'),
   ('before_path','VARCHAR'),('after_presence','VARCHAR'),('after_path','VARCHAR'),('after_origin','BIGINT'),
   ('fact_before_presence','VARCHAR'),('fact_after_presence','VARCHAR'),('baseline_ref','VARCHAR'),('last_known_event','VARCHAR'),('legacy_before','VARCHAR'),
   ('legacy_after','VARCHAR'),('legacy_origin_added','VARCHAR'),('legacy_origin_removed','VARCHAR'),('legacy_origin_count','BIGINT'),('legacy_skip','VARCHAR'),
   ('fragment_id','VARCHAR'),('session_id','VARCHAR'),('rule_version','VARCHAR'),('limitations','VARCHAR[]')]
}
TYPES={'VARCHAR':pa.string(),'BIGINT':pa.int64(),'INTEGER':pa.int32(),'BOOLEAN':pa.bool_(),
       'BLOB':pa.binary(),'VARCHAR[]':pa.list_(pa.string())}


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def connect_duckdb(path=':memory:'):
    # 在原生构造时限制线程，避免多连接先按主机CPU数分配线程资源。
    threads=os.environ.get('DOMEYE_DUCKDB_THREADS','2')
    if re.fullmatch(r'[1-9][0-9]*',threads) is None:
        raise ValueError('DOMEYE_DUCKDB_THREADS 必须为十进制正整数')
    db=duckdb.connect(path,config={'threads':int(threads)})
    db.execute("SET memory_limit='1GB'")
    extension_dir=os.environ.get('DOMEYE_DUCKDB_EXTENSION_DIRECTORY')
    if extension_dir:db.execute('SET extension_directory='+literal(extension_dir))
    return db


class Store:
    def __init__(self, dsn, root, *, batch_rows=50000, memory_limit='1GB', catalog_data_path=None):
        self.dsn=dsn
        self.root=Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.catalog_data_path=Path(catalog_data_path).resolve() if catalog_data_path is not None else self.root/'parquet'
        self.run_id=uuid.uuid4().hex
        self.batch_rows=batch_rows
        self.memory_limit=memory_limit
        self.pending={k:[] for k in TABLES}
        self.counts={k:0 for k in TABLES}
        self.db=connect_duckdb(str(self.root/'staging.duckdb'))
        self.message_cursors={}
        self.exported_current=False
        self.expected_inputs=None
        self.expected_references=()
        self.flush_metrics=[]
        self.phase='observations'
        self.db.execute('SET memory_limit='+literal(memory_limit))
        self.db.execute('SET temp_directory='+literal(self.root/'temp'))
        self.db.execute('LOAD ducklake'); self.db.execute('LOAD postgres')
        try:
            # 已有catalog由DuckLake精确校验DATA_PATH；绝不使用OVERRIDE。
            self.db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (DATA_PATH '+literal(self.catalog_data_path)+', DATA_INLINING_ROW_LIMIT 0)')
        except Exception:
            self.db.close()
            raise
        loaded=dict(self.db.execute("SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded").fetchall())
        if loaded.get('ducklake')!='3f1b372' or loaded.get('postgres_scanner')!='b9fce43':
            raise ValueError('DuckLake/Postgres扩展版本与已验版本不符')
        self.schema='r_'+self.run_id
        self.db.execute('CREATE SCHEMA lake.'+self.schema)
        for name, columns in TABLES.items():
            self.db.execute(f'CREATE TABLE lake.{self.schema}.{name} ('+', '.join('"'+n+'" '+t for n,t in columns)+')')
        self.db.execute('CREATE TABLE path_keys (path_key VARCHAR PRIMARY KEY)')
        self.pg=psycopg2.connect(dsn)
        with self.pg, self.pg.cursor() as c:
            c.execute('CREATE SCHEMA IF NOT EXISTS domeye')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.runs (
                run_id TEXT PRIMARY KEY, schema_name TEXT NOT NULL, state TEXT NOT NULL,
                snapshot BIGINT, reason TEXT, created_at TIMESTAMPTZ DEFAULT now())''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.progress (
                run_id TEXT PRIMARY KEY,phase TEXT,message_cursor TEXT,element_cursor TEXT,change_cursor TEXT,
                submitted_rows BIGINT,updated_at TIMESTAMPTZ DEFAULT now())''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.run_specs (
                run_id TEXT PRIMARY KEY, manifest JSONB NOT NULL, rule_version TEXT NOT NULL,
                current_exported BOOLEAN NOT NULL DEFAULT false)''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.reference_inputs (
                run_id TEXT,source_id TEXT,path TEXT,state TEXT,row_count BIGINT,
                PRIMARY KEY(run_id,source_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.source_receipts (
                run_id TEXT,source_id TEXT,message_count BIGINT,element_count BIGINT,
                PRIMARY KEY(run_id,source_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.inputs (
                run_id TEXT, source_id TEXT, path TEXT, size BIGINT, state TEXT,
                PRIMARY KEY(run_id,source_id))''')
            c.execute('''CREATE TABLE IF NOT EXISTS domeye.current_routes (
                run_id TEXT, object_key TEXT, presence TEXT, path_key TEXT, origin BIGINT,
                event_id TEXT, epoch BIGINT, semantics TEXT, fact_presence TEXT, last_known_event TEXT, PRIMARY KEY(run_id,object_key))''')
            c.execute('INSERT INTO domeye.runs(run_id,schema_name,state) VALUES (%s,%s,%s)',
                      (self.run_id,self.schema,'candidate'))

    def bind(self,manifest,rule_version):
        import json
        self.expected_inputs=tuple(i.get('source_id',i['sha256']) for i in manifest['inputs'])
        self.expected_references=tuple(i['sha256'] for i in manifest.get('references',[]))
        with self.pg,self.pg.cursor() as c:
            c.execute('INSERT INTO domeye.run_specs(run_id,manifest,rule_version) VALUES (%s,%s,%s)',
                      (self.run_id,json.dumps(manifest),rule_version))

    def source_complete(self,source_id,message_count,element_count):
        with self.pg,self.pg.cursor() as c:
            c.execute('INSERT INTO domeye.source_receipts VALUES (%s,%s,%s,%s)',
                      (self.run_id,source_id,message_count,element_count))
            c.execute("UPDATE domeye.inputs SET state='validated' WHERE run_id=%s AND source_id=%s",(self.run_id,source_id))

    def append(self, table, row):
        self.pending[table].append(row)
        if sum(map(len,self.pending.values()))>=self.batch_rows:
            self.flush()

    def flush(self):
        started=time.monotonic()
        submitted=sum(map(len,self.pending.values()))
        # 运行失败不恢复，因此字典索引先提交不会影响已发布数据。
        # INSERT主键索引只处理本批键；不反复构建全历史anti-join。
        if self.pending['paths']:
            self.db.register('candidate_keys',pa.table({'path_key':list({r['path_key'] for r in self.pending['paths']})}))
            unseen=self.db.execute('INSERT INTO path_keys SELECT * FROM candidate_keys ON CONFLICT DO NOTHING RETURNING path_key').fetch_arrow_table()
            self.db.unregister('candidate_keys')
            self.db.register('unseen_keys',unseen)
        self.db.execute('BEGIN')
        try:
            for name, rows in self.pending.items():
                if not rows: continue
                schema=pa.schema([(n,TYPES[t]) for n,t in TABLES[name]])
                self.db.register('batch',pa.Table.from_pylist(rows,schema=schema))
                if name=='paths':
                    # 只将本批新字典值批量写入湖表。
                    self.db.execute(f'''INSERT INTO lake.{self.schema}.paths
                        SELECT DISTINCT ON (path_key) b.* FROM batch b JOIN unseen_keys USING(path_key)''')
                else:
                    self.db.execute(f'INSERT INTO lake.{self.schema}.{name} SELECT * FROM batch')
                self.db.unregister('batch')
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise
        if self.pending['paths']:self.db.unregister('unseen_keys')
        if submitted:
            message_cursor=self.pending['messages'][-1]['message_id'] if self.pending['messages'] else None
            element_cursor=self.pending['elements'][-1]['event_id'] if self.pending['elements'] else None
            change_cursor=self.pending['changes'][-1]['event_id'] if self.pending['changes'] else None
            with self.pg,self.pg.cursor() as c:
                c.execute('''INSERT INTO domeye.progress VALUES (%s,%s,%s,%s,%s,%s,now())
                    ON CONFLICT(run_id) DO UPDATE SET phase=excluded.phase,
                    message_cursor=coalesce(excluded.message_cursor,domeye.progress.message_cursor),
                    element_cursor=coalesce(excluded.element_cursor,domeye.progress.element_cursor),
                    change_cursor=coalesce(excluded.change_cursor,domeye.progress.change_cursor),
                    submitted_rows=domeye.progress.submitted_rows+excluded.submitted_rows,updated_at=now()''',
                    (self.run_id,self.phase,message_cursor,element_cursor,change_cursor,submitted))
        self.flush_metrics.append({'submitted_rows':submitted,'seconds':time.monotonic()-started})
        for name in self.pending:
            self.counts[name]+=len(self.pending[name]); self.pending[name].clear()

    def message(self,m):
        previous=self.message_cursors.get(m.source_id,-1)
        if m.record<previous:raise ValueError('消息顺序倒退')
        if m.record==previous:return False
        if m.record!=previous+1:raise ValueError('消息物理记录不连续')
        self.message_cursors[m.source_id]=m.record
        p=m.peer
        row={n:getattr(m,n,None) for n,_ in TABLES['messages']}
        row.update(message_id=m.message_id,peer_ip=p.get('ip'),peer_asn=p.get('asn'),
                   bgp_id=p.get('bgp_id'),bgp_id_present=p.get('bgp_id_present',False),
                   local_ip=p.get('local_ip'),local_asn=p.get('local_asn'),interface=p.get('interface'),endpoint_afi=p.get('endpoint_afi'),local_message=p.get('local_message'))
        self.append('messages',row)
        for p in m.peers:self.append('peers',p)
        for afi,safi in m.eor_families:self.append('eor',{'message_id':m.message_id,'afi':afi,'safi':safi})
        for p in m.paths:self.append('paths',p)
        for e in m.elements:
            p=e['peer']
            self.append('elements',{**e,'event_id':f'{m.message_id}:{e["ordinal"]}',
                'message_id':m.message_id,'peer_ip':p.get('ip'),'peer_asn':p.get('asn'),
                'bgp_id':p.get('bgp_id'),'bgp_id_present':p.get('bgp_id_present',False),
                'peer_table_record':p.get('table_record'),'peer_index':p.get('index')})

    def change(self,row):
        if row.get('record_type')=='invalidation':
            self.append('invalidations',{**row,'scope':[str(v) if v is not None else None for v in row['scope']]})
            return
        before=row['calculation_before'] or {};after=row['calculation_after'];known=row['last_known_before'] or {}
        self.append('changes',{**row,'scope':[str(v) if v is not None else None for v in row['scope']],
            'before_event':before.get('event_id'),'after_event':after['event_id'],'after_epoch':after['epoch'],'before_presence':before.get('presence','unknown'),
            'before_path':before.get('path_key'),'after_presence':after['presence'],
            'after_path':after['path_key'],'after_origin':after['origin'],
            'last_known_event':known.get('event_id')})

    def save_current(self,replay):
        self.append('projection_metadata',{'name':'legacy_baseline_epoch','value':str(replay.legacy_baseline_epoch) if replay.legacy_baseline_epoch is not None else None})
        self.append('projection_metadata',{'name':'legacy_country_filter','value':None})
        self.append('projection_metadata',{'name':'legacy_baseline_source','value':replay.plan.baseline_source})
        for (prefix,vp),path in replay.legacy_paths.items():
            self.append('legacy_state',{'prefix':prefix,'vp':vp,'path_text':path})
        for prefix,origins in replay.legacy_origins.items():
            for origin in origins:self.append('legacy_origin_index',{'prefix':prefix,'origin':origin})
        for vp in replay.seen_vps:self.append('legacy_seen_vps',{'vp':vp})
        from data_pipeline.bgp.replay.route_replay import identity
        with self.pg,self.pg.cursor() as c:
            execute_values(c,'INSERT INTO domeye.current_routes VALUES %s',
                ((self.run_id,identity(key),v['presence'],v['path_key'],v['origin'],v['event_id'],v['epoch'],'calculation_replay/v1','unknown',replay.last_known[key]['event_id'])
                 for key,v in replay.current.items()),page_size=10000)
        with self.pg,self.pg.cursor() as c:
            c.execute('UPDATE domeye.run_specs SET current_exported=true WHERE run_id=%s',(self.run_id,))
        self.exported_current=True

    def finish(self,receipt=None):
        with self.pg.cursor() as c:
            c.execute('SELECT state FROM domeye.runs WHERE run_id=%s',(self.run_id,))
            if c.fetchone()[0]!='candidate':raise ValueError('运行已终结，不允许再次完成')
        if self.expected_inputs is None:raise ValueError('未绑定固定输入与规则')
        if not self.exported_current:raise ValueError('未导出计算工作态')
        if self.expected_inputs is not None:
            with self.pg.cursor() as c:
                c.execute("SELECT source_id FROM domeye.inputs WHERE run_id=%s AND state='validated'",(self.run_id,))
                if {r[0] for r in c.fetchall()}!=set(self.expected_inputs):raise ValueError('固定输入未全部完成')
        with self.pg.cursor() as c:
            c.execute("SELECT source_id FROM domeye.reference_inputs WHERE run_id=%s AND state='validated'",(self.run_id,))
            if {r[0] for r in c.fetchall()}!=set(self.expected_references):raise ValueError('参考输入未全部完成')
        self.flush()
        prefix=f'lake.{self.schema}'
        rejected=self.db.execute(f'SELECT count(*) FROM {prefix}.messages WHERE reason IS NOT NULL').fetchone()[0]
        orphan=self.db.execute(f'''SELECT count(*) FROM {prefix}.elements e
            LEFT JOIN {prefix}.messages m USING(message_id) LEFT JOIN {prefix}.paths p USING(path_key)
            WHERE m.message_id IS NULL OR p.path_key IS NULL''').fetchone()[0]
        if rejected or orphan:
            raise ValueError(f'质量拒绝：拒绝消息={rejected},孤立引用={orphan}')
        if self.db.execute(f'SELECT count(*) FROM {prefix}.messages').fetchone()[0]==0:
            raise ValueError('空运行不能发布')
        with self.pg.cursor() as c:
            c.execute("SELECT count(*) FROM domeye.inputs WHERE run_id=%s AND state!='validated'",(self.run_id,))
            if c.fetchone()[0]:raise ValueError('输入尚未完成校验')
        self.actual_counts={name:self.db.execute(f'SELECT count(*) FROM {prefix}.{name}').fetchone()[0] for name in TABLES}
        with self.pg.cursor() as c:
            c.execute('SELECT source_id,message_count,element_count FROM domeye.source_receipts WHERE run_id=%s',(self.run_id,))
            receipts=c.fetchall()
        if {r[0] for r in receipts}!=set(self.expected_inputs):raise ValueError('缺少完整源回执')
        if sum(r[1] for r in receipts)!=self.actual_counts['messages'] or sum(r[2] for r in receipts)!=self.actual_counts['elements']:
            raise ValueError('源回执与湖表观察数量不符')
        if self.actual_counts['changes']==0:raise ValueError('缺少必需状态变化输出')
        objects=self.db.execute(f'SELECT count(DISTINCT object_key) FROM {prefix}.changes').fetchone()[0]
        with self.pg.cursor() as c:
            c.execute('SELECT count(*) FROM domeye.current_routes WHERE run_id=%s',(self.run_id,))
            if c.fetchone()[0]!=objects:raise ValueError('PG当前态与历史对象数量不符')
            c.execute('SELECT current_exported FROM domeye.run_specs WHERE run_id=%s',(self.run_id,))
            if c.fetchone()!= (True,):raise ValueError('PG未确认工作态导出')
        snapshot=self.db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        payload=receipt if receipt is not None else {}
        payload.update(run_id=self.run_id,snapshot=snapshot,state='ready',
                       catalog_data_path=str(self.catalog_data_path),run_output_path=str(self.root),
                       publication_authority='PostgreSQL domeye.runs',actual_rows=self.actual_counts,
                       flush_metrics=self.flush_metrics)
        self.write_receipt(payload)
        with self.pg,self.pg.cursor() as c:
            c.execute("UPDATE domeye.runs SET state='complete',snapshot=%s WHERE run_id=%s AND state='candidate'",(snapshot,self.run_id))
        return snapshot

    def write_receipt(self,payload):
        import json
        # 必需回执先落盘；ready仅表示可提交，PG是完成状态的唯一权威。
        with (self.root/'execution.json').open('x') as f:
            json.dump(payload,f,ensure_ascii=False,indent=2)
            f.flush()
            os.fsync(f.fileno())

    def fail(self,reason):
        # 提交响应不明时读取唯一权威；不把可能已完成的运行另写成failed。
        try:
            self.pg.rollback()
            with self.pg,self.pg.cursor() as c:
                c.execute("UPDATE domeye.runs SET state='failed',reason=%s WHERE run_id=%s AND state='candidate'",(str(reason),self.run_id))
                c.execute('SELECT state FROM domeye.runs WHERE run_id=%s',(self.run_id,))
                return c.fetchone()[0]
        except Exception:
            try:
                with psycopg2.connect(self.dsn) as pg,pg.cursor() as c:
                    c.execute('SELECT state FROM domeye.runs WHERE run_id=%s',(self.run_id,))
                    return c.fetchone()[0]
            except Exception:
                return 'unknown'

    def close(self):
        self.db.close();self.pg.close()


def bound_table(schema,snapshot,table):
    if table not in TABLES:raise ValueError('读取表不在合同中')
    return f'(SELECT * FROM lake.{schema}."{table}" AT (VERSION => {int(snapshot)}))'


def query(dsn,run_id,table,limit=100,*,profile='complete'):
    """仅已完成模块数据；不修改catalog、文件或业务选择。"""
    if table not in TABLES or not 1<=limit<=1000:raise ValueError('查询范围无效')
    if profile=='observation':
        from data_pipeline.bgp.archive.selection import for_run
        selected=for_run(dsn,run_id);db=selected.connect()
        try:return db.execute(f'SELECT * FROM {selected.table(table)} LIMIT {limit}').fetchall()
        finally:db.close()
    if profile!='complete':raise ValueError('未知消费资格')
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute("SELECT schema_name,snapshot FROM domeye.runs WHERE run_id=%s AND state='complete'",(run_id,))
            row=c.fetchone()
    if row is None:raise ValueError('候选、失败或未知运行不可消费')
    schema,snapshot=row
    if schema!='r_'+run_id or not run_id.isalnum():raise ValueError('登记身份无效')
    db=connect_duckdb()
    try:
        db.execute('LOAD ducklake');db.execute('LOAD postgres')
        db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
        return db.execute(f'SELECT * FROM {bound_table(schema,snapshot,table)} LIMIT {limit}').fetchall()
    finally:db.close()


def scan(dsn,run_id,table,batch_size=10000,*,profile='complete'):
    """后续离线模块有界消费同版全量，返回Arrow批；changes按声明计算序。"""
    if table not in TABLES or not 1<=batch_size<=100000:raise ValueError('扫描范围无效')
    if profile=='observation':
        from data_pipeline.bgp.archive.selection import for_run
        selected=for_run(dsn,run_id);db=selected.connect()
        try:yield from db.execute(f'SELECT * FROM {selected.table(table)}').fetch_record_batch(batch_size)
        finally:db.close()
        return
    if profile!='complete':raise ValueError('未知消费资格')
    pg=psycopg2.connect(dsn)
    try:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute("SELECT schema_name,snapshot FROM domeye.runs WHERE run_id=%s AND state='complete'",(run_id,))
            row=c.fetchone()
    finally:pg.close()
    if row is None:raise ValueError('候选、失败或未知运行不可消费')
    schema,snapshot=row
    if schema!='r_'+run_id or not run_id.isalnum():raise ValueError('登记身份无效')
    db=connect_duckdb()
    try:
        db.execute('LOAD ducklake');db.execute('LOAD postgres')
        db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
        order=' ORDER BY source_rank,record,ordinal' if table=='changes' else ''
        result=db.execute(f'SELECT * FROM {bound_table(schema,snapshot,table)}'+order)
        yield from result.fetch_record_batch(batch_size)
    finally:db.close()


def scan_observations(dsn,run_id,batch_size=10000,*,profile='complete'):
    """下游RIB/UPDATE共用的已完成观察Interface；保持来源内物理顺序。

    path文本是二进制段的固定渲染，原始真值为原字节；不声称原件存在文本空白。
    """
    if not 1<=batch_size<=100000:raise ValueError('批次大小无效')
    if profile=='observation':
        from data_pipeline.bgp.archive.selection import for_run
        from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch
        selected=for_run(dsn,run_id)
        reader=ObservationReader(dsn,run_id,selected.snapshot,[e['source_id'] for e in selected.manifest['inputs']],profile=profile,batch_rows=min(batch_size,10000))
        # 元素辅助接口明确不能代替消息/gap消费；原完整MessageBatch另由Reader提供。
        for item in reader.stream():
            if isinstance(item,MessageBatch) and item.elements:yield pa.RecordBatch.from_pylist(list(item.elements))
        return
    if profile!='complete':raise ValueError('未知消费资格')
    pg=psycopg2.connect(dsn)
    try:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute("SELECT schema_name,snapshot FROM domeye.runs WHERE run_id=%s AND state='complete'",(run_id,))
            row=c.fetchone()
    finally:pg.close()
    if row is None:raise ValueError('未完成数据不可消费')
    schema,snapshot=row
    if schema!='r_'+run_id or not run_id.isalnum():raise ValueError('登记身份无效')
    db=connect_duckdb()
    try:
        db.execute('LOAD ducklake');db.execute('LOAD postgres')
        db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
        result=db.execute(f'''SELECT e.*, m.source_id,m.content_sha256,m.record,m.offset,m.length,m.epoch,m.microsecond,
            m.mrt_type,m.mrt_subtype,m.local_ip,m.local_asn,m.interface,
            p.as_path_raw,p.as4_path_raw,p.as_path_text,p.as4_path_text,p.attributes_raw,
            p.raw_origin_asn,p.attributed_origin_asn,p.reason AS origin_reason
            FROM {bound_table(schema,snapshot,"elements")} e JOIN {bound_table(schema,snapshot,"messages")} m USING(message_id)
            JOIN {bound_table(schema,snapshot,"paths")} p USING(path_key) ORDER BY m.source_id,m.record,e.ordinal''')
        yield from result.fetch_record_batch(batch_size)
    finally:db.close()
