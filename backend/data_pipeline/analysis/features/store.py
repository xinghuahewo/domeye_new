"""Feature私有候选历史/工作态；ready先落盘，PG完成后才可读。无恢复/outbox。"""
from dataclasses import asdict
import json
import os
from pathlib import Path
import time
import uuid
import pyarrow as pa
import psycopg2
from psycopg2.extras import execute_values

from data_pipeline.analysis.features.calculation import prefix_filter, UNITS
from data_pipeline.bgp.archive.message_reader import byte_size
from data_pipeline.analysis.features.qualification import TABLES as QUALIFICATION_TABLES, PROFILE, SCHEMA_VERSION, DIMENSIONS, ORDER_KEYS, rows_digest
from data_pipeline.bgp.replay.route_replay import identity
from data_pipeline.bgp.archive.store import connect_duckdb, literal, TYPES

METRICS = [('v4Prefix_num','BIGINT'),('v6Prefix_num','BIGINT'),('v4IP_num','BIGINT'),
           ('announ_num','BIGINT'),('withdraw_num','BIGINT')]
DIAGNOSTICS_VERSION = 'feature-module-diagnostics/v1'
WARNING_METRICS = {
 'v4prefix_num_reached_full_ipv4_c_segments': ('v4Prefix_num', 2**24),
 'v4ip_num_reached_full_ipv4_space': ('v4IP_num', 2**32),
 'v4ip_num_exceeds_suspicious_threshold': ('v4IP_num', 10**9),
}
TABLES = {
 'module_diagnostics': [('dataset_version','VARCHAR'),('mode','VARCHAR'),('source_id','VARCHAR'),
   ('source_rank','BIGINT'),('sequence','BIGINT'),('start','VARCHAR'),('end','VARCHAR'),('file_time','VARCHAR'),
   ('scope','VARCHAR'),('identifier','VARCHAR'),('reason','VARCHAR'),('raw_prefix','VARCHAR'),
   ('normalized_prefix','VARCHAR'),('sample_prefixes','VARCHAR[]'),('metric','VARCHAR'),('threshold','BIGINT'),
   ('comparison','VARCHAR'),('value','BIGINT'),('unit','VARCHAR'),('rule_id','VARCHAR'),
   ('observation_version','VARCHAR'),('reference_version','VARCHAR'),('projection_version','VARCHAR')],
 'windows': [('mode','VARCHAR'),('source_id','VARCHAR'),('source_rank','BIGINT'),('start','VARCHAR'),('end','VARCHAR'),
   ('file_time','VARCHAR'),('legacy_source','VARCHAR'),('scope','VARCHAR'),('subject','VARCHAR'),('country','VARCHAR'),
   *METRICS,('row_presence','VARCHAR'),('resource_status','VARCHAR'),('legacy_table','VARCHAR'),('reference_version','VARCHAR')],
 'projection_revisions': [('mode','VARCHAR'),('source_id','VARCHAR'),('source_rank','BIGINT'),('sequence','BIGINT'),
   ('event_id','VARCHAR'),('action','VARCHAR'),('prefix','VARCHAR'),('raw_prefix','VARCHAR'),('vp','VARCHAR'),
   ('before_path','VARCHAR'),('after_path','VARCHAR'),('before_path_exists','BOOLEAN'),('after_path_exists','BOOLEAN'),
   ('before_origins','VARCHAR[]'),('after_origins','VARCHAR[]'),('skip','VARCHAR'),('projection_skip','VARCHAR'),
   ('path_key','VARCHAR'),('observation_version','VARCHAR'),('reference_version','VARCHAR'),('local_message','BOOLEAN'),('direction_rule','VARCHAR')],
 'resource_members': [('mode','VARCHAR'),('source_id','VARCHAR'),('scope','VARCHAR'),('subject','VARCHAR'),
   ('raw_prefix','VARCHAR'),('normalized_prefix','VARCHAR'),('skip_reason','VARCHAR')],
 'state_deltas': [('mode','VARCHAR'),('source_id','VARCHAR'),('source_rank','BIGINT'),('phase','VARCHAR'),
   ('scope','VARCHAR'),('country','VARCHAR'),('asn','VARCHAR'),*METRICS,('is_change','BOOLEAN'),
   ('previous_projection','VARCHAR'),('projection_version','VARCHAR')],
 'source_receipts': [('mode','VARCHAR'),('source_id','VARCHAR'),('source_rank','BIGINT'),('previous_projection','VARCHAR'),
   ('projection_version','VARCHAR'),('messages','BIGINT'),('elements','BIGINT'),('windows','BIGINT'),('state_messages','BIGINT'),('eor_records','BIGINT'),('local_messages','BIGINT'),('quality_records','BIGINT'),('quality','VARCHAR'),
   ('input_gaps','VARCHAR'),('reference_version','VARCHAR'),('rib_time','VARCHAR'),('last_t','VARCHAR')],
 'decoding_differences': [('source_id','VARCHAR'),('event_id','VARCHAR'),('record','BIGINT'),('ordinal','BIGINT'),('evidence','VARCHAR')],
 'reference_rows': [('source_id','VARCHAR'),('row','BIGINT'),('location','VARCHAR'),('raw_row','VARCHAR'),
   ('raw_byte_offset','BIGINT'),('raw_byte_length','BIGINT'),('raw_record_sha256','VARCHAR'),('csv_record_kind','VARCHAR'),
   ('csv_physical_start','BIGINT'),('csv_physical_end','BIGINT'),('rule','VARCHAR'),('effective_time_state','VARCHAR'),
   ('legacy_asn','VARCHAR'),('legacy_asn_type','VARCHAR'),('legacy_country','VARCHAR'),('legacy_country_code','VARCHAR'),('selected','BOOLEAN')],
}
for _table in ('windows','projection_revisions','resource_members','state_deltas','source_receipts','decoding_differences','module_diagnostics'):
    TABLES[_table].append(('window_role','VARCHAR'))
TABLES['source_receipts'] += [('diagnostics','BIGINT'),('diagnostics_state','VARCHAR'),('upstream_run_id','VARCHAR'),('upstream_snapshot','BIGINT'),
    ('source_role','VARCHAR'),('calculation_role','VARCHAR'),('origin_uri','VARCHAR'),('content_sha256','VARCHAR')]


class FeatureStore:
    def __init__(self, dsn, root, specification, *, batch_rows=1000, batch_bytes=4*1024**2, guard=lambda: None):
        if batch_rows < 1 or batch_bytes < 1: raise ValueError('批写阈值无效')
        self.dsn, self.root, self.specification = dsn, Path(root), specification
        self.root.mkdir(parents=True, exist_ok=False)
        self.run_id = uuid.uuid4().hex
        self.schema = 'f_'+self.run_id
        self.tables={**TABLES,**QUALIFICATION_TABLES} if specification.get('output_profile')==PROFILE else TABLES
        self.qualification_expected={}
        self.batch_rows, self.batch_bytes, self.guard = batch_rows, batch_bytes, guard
        self.pending = {t: [] for t in self.tables}; self.pending_bytes = self.pending_count = 0
        self.counts = {t: 0 for t in self.tables}; self.flush_metrics = []
        self.source_counts = {}; self.validated = False
        self.diagnostic_expected = {}; self.diagnostic_values = {}
        specification['diagnostics_dataset_version'] = DIAGNOSTICS_VERSION
        self.source_bindings = {s['source_id']:s for s in specification.get('source_bindings',())}
        self.db = connect_duckdb(str(self.root/'staging.duckdb'))
        self.db.execute('SET temp_directory='+literal(self.root/'temp'))
        self.db.execute('LOAD ducklake'); self.db.execute('LOAD postgres')
        loaded = dict(self.db.execute('SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded').fetchall())
        if loaded.get('ducklake') != '3f1b372' or loaded.get('postgres_scanner') != 'b9fce43':
            raise ValueError('未锁定的扩展版本')
        self.db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+" AS lake (METADATA_SCHEMA "+literal("fl_"+self.run_id)+", DATA_PATH "+literal(self.root/'parquet')+', DATA_INLINING_ROW_LIMIT 0)')
        self.db.execute('CREATE SCHEMA lake.'+self.schema)
        for name, columns in self.tables.items():
            self.db.execute(f'CREATE TABLE lake.{self.schema}.{name} ('+', '.join('"'+n+'" '+t for n,t in columns)+')')
        self.pg = psycopg2.connect(dsn)
        with self.pg, self.pg.cursor() as c:
            c.execute('CREATE SCHEMA IF NOT EXISTS feature')
            c.execute('''CREATE TABLE IF NOT EXISTS feature.runs(run_id TEXT PRIMARY KEY,state TEXT NOT NULL,
                schema_name TEXT NOT NULL,snapshot BIGINT,specification JSONB NOT NULL,reason TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS feature.states(run_id TEXT,mode TEXT,scope TEXT,country TEXT,asn TEXT,
                v4prefix_num BIGINT,v6prefix_num BIGINT,v4ip_num BIGINT,announ_num BIGINT,withdraw_num BIGINT,
                is_change BOOLEAN,projection_version TEXT,PRIMARY KEY(run_id,mode,scope,country,asn))''')
            c.execute('''CREATE TABLE IF NOT EXISTS feature.paths(run_id TEXT,mode TEXT,prefix TEXT,vp TEXT,path TEXT,
                PRIMARY KEY(run_id,mode,prefix,vp))''')
            c.execute('''CREATE TABLE IF NOT EXISTS feature.origins(run_id TEXT,mode TEXT,prefix TEXT,origin TEXT,
                PRIMARY KEY(run_id,mode,prefix,origin))''')
            c.execute('''CREATE TABLE IF NOT EXISTS feature.seen_vps(run_id TEXT,mode TEXT,vp TEXT,PRIMARY KEY(run_id,mode,vp))''')
            c.execute('''CREATE TABLE IF NOT EXISTS feature.source_commits(run_id TEXT,mode TEXT,source_id TEXT,
                previous_projection TEXT,projection_version TEXT,messages BIGINT,elements BIGINT,
                PRIMARY KEY(run_id,mode,source_id))''')
            if specification.get('output_profile')==PROFILE:
                c.execute('CREATE TABLE IF NOT EXISTS feature.qualified_results(run_id TEXT PRIMARY KEY,snapshot BIGINT NOT NULL,receipt JSONB NOT NULL)')
            c.execute('INSERT INTO feature.runs VALUES (%s,%s,%s,NULL,%s,NULL)',
                      (self.run_id,'candidate',self.schema,json.dumps(specification)))

    def append(self, table, row):
        self.guard()
        binding=self.source_bindings.get(row.get('source_id'))
        if binding is not None and ('window_role','VARCHAR') in self.tables[table]:
            row={**row,'window_role':binding['window_role']}
            if table=='source_receipts':
                row.update(upstream_run_id=binding['run_id'],upstream_snapshot=binding['snapshot'],
                           **{k:binding[k] for k in ('source_role','calculation_role','origin_uri','content_sha256')})
        size = byte_size(row)
        if self.pending_count and (self.pending_count >= self.batch_rows or self.pending_bytes+size > self.batch_bytes):
            self.flush()
        self.pending[table].append(row)
        self.pending_count += 1; self.pending_bytes += size
        if self.pending_count >= self.batch_rows or self.pending_bytes >= self.batch_bytes:
            self.flush()  # 单条大行独立写，不截断。

    def flush(self):
        if not self.pending_count: return
        self.guard(); started = time.monotonic()
        self.db.execute('BEGIN')
        try:
            for table, rows in self.pending.items():
                if not rows: continue
                schema = pa.schema([(n,({**TYPES,'BIGINT[]':pa.list_(pa.int64())})[t]) for n,t in self.tables[table]])
                self.db.register('feature_batch', pa.Table.from_pylist(rows, schema=schema))
                self.db.execute(f'INSERT INTO lake.{self.schema}.{table} SELECT * FROM feature_batch')
                self.db.unregister('feature_batch')
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK'); raise
        # 候选内部可分批持久化；任何失败不开放此run，不宣称跨存储原子。
        with self.pg, self.pg.cursor() as c:
            changed = {}
            seen = set()
            for r in self.pending['projection_revisions']:
                if r['prefix'] is not None and not r['skip'] and not r['projection_skip']:
                    changed[r['mode'],r['prefix'],r['vp']] = r
                    if r['action'] != 'withdraw':
                        seen.add((self.run_id,r['mode'],r['vp']))
            # 历史曾见不能由末工作态推导：同批A→W仍必须保留这个VP。
            if seen:
                execute_values(c,'INSERT INTO feature.seen_vps VALUES %s ON CONFLICT DO NOTHING', sorted(seen))
            for (mode,prefix,vp), r in changed.items():
                if r['after_path_exists']:
                    c.execute('INSERT INTO feature.paths VALUES (%s,%s,%s,%s,%s) ON CONFLICT(run_id,mode,prefix,vp) DO UPDATE SET path=excluded.path',
                              (self.run_id,mode,prefix,vp,r['after_path']))
                else:
                    c.execute('DELETE FROM feature.paths WHERE run_id=%s AND mode=%s AND prefix=%s AND vp=%s', (self.run_id,mode,prefix,vp))
            prefix_changes = {}
            for r in self.pending['projection_revisions']:
                if r['prefix'] is not None and not r['skip'] and not r['projection_skip']:
                    prefix_changes[r['mode'],r['prefix']] = r['after_origins']
            for (mode,prefix), origins in prefix_changes.items():
                c.execute('DELETE FROM feature.origins WHERE run_id=%s AND mode=%s AND prefix=%s', (self.run_id,mode,prefix))
                if origins:
                    execute_values(c,'INSERT INTO feature.origins VALUES %s', [(self.run_id,mode,prefix,o) for o in origins])
            latest = {}
            for r in self.pending['state_deltas']:
                latest[r['mode'],r['scope'],r['country'],r['asn']] = r
            if latest:
                execute_values(c,'''INSERT INTO feature.states VALUES %s ON CONFLICT(run_id,mode,scope,country,asn)
                    DO UPDATE SET v4prefix_num=excluded.v4prefix_num,v6prefix_num=excluded.v6prefix_num,v4ip_num=excluded.v4ip_num,
                    announ_num=excluded.announ_num,withdraw_num=excluded.withdraw_num,is_change=excluded.is_change,
                    projection_version=excluded.projection_version''',
                    [(self.run_id,r['mode'],r['scope'],r['country'],r['asn'],*(r[n] for n,_ in METRICS),r['is_change'],r['projection_version']) for r in latest.values()])
        self.flush_metrics.append({'rows':self.pending_count,'bytes':self.pending_bytes,'seconds':time.monotonic()-started})
        for table, rows in self.pending.items():
            self.counts[table] += len(rows); rows.clear()
        self.pending_count = self.pending_bytes = 0
        self.guard()

    def audit(self, mode, source_rank, row):
        key = (mode,row['source_id'])
        count = self.source_counts.setdefault(key, {'audit':0,'windows':0})
        self.append('projection_revisions', {**row,'mode':mode,'source_rank':source_rank,'sequence':count['audit'],
                    'before_origins':sorted(row['before_origins']),'after_origins':sorted(row['after_origins'])})
        count['audit'] += 1

    def window_row(self, mode, rank, binding, reference, row):
        self.source_counts.setdefault((mode,binding.source_id), {'audit':0,'windows':0})['windows'] += 1
        self.append('windows', {**asdict(row.values),'mode':mode,'source_id':binding.source_id,'source_rank':rank,
            'start':binding.window.start.isoformat(),'end':binding.window.end.isoformat(),'file_time':binding.window.file_time.isoformat(),
            'legacy_source':'r','scope':row.scope,'subject':row.subject,'country':row.country,'row_presence':row.row_presence,
            'resource_status':row.resource_status,'legacy_table':row.legacy_table,'reference_version':reference.version})
        if row.values.v4IP_num >= 10**9 or row.values.v4Prefix_num >= 2**24:
            identifier = f"{row.country if row.country != ' ' else '未知'}:{row.subject}" if row.scope == 'asn' else row.subject
            self.diagnostic_values[row.scope,identifier] = row.values
        for raw in row.raw_prefixes:
            normalized, reason = prefix_filter(raw)
            self.append('resource_members', {'mode':mode,'source_id':binding.source_id,'scope':row.scope,'subject':row.subject,
                'raw_prefix':raw,'normalized_prefix':normalized,'skip_reason':reason})

    def save_diagnostics(self, mode, rank, binding, result):
        diagnostics = result.diagnostics if result else ()
        self.diagnostic_expected[mode,binding.source_id] = len(diagnostics)
        for sequence, diagnostic in enumerate(diagnostics):
            metric, threshold = WARNING_METRICS.get(diagnostic.reason, (None,None))
            value = getattr(self.diagnostic_values[diagnostic.scope,diagnostic.identifier],metric) if metric else None
            self.append('module_diagnostics', {**asdict(diagnostic),
                'dataset_version':DIAGNOSTICS_VERSION,'mode':mode,'source_id':binding.source_id,
                'source_rank':rank,'sequence':sequence,'start':binding.window.start.isoformat(),
                'end':binding.window.end.isoformat(),'file_time':binding.window.file_time.isoformat(),
                'metric':metric,'threshold':threshold,'comparison':'>=' if metric else None,
                'value':value,'unit':UNITS[metric] if metric else None,'rule_id':result.rule_id,
                'observation_version':self.specification['observation_version'],
                'reference_version':result.end_state.reference_version,
                'projection_version':result.end_state.projection.version})
        self.diagnostic_values.clear()

    def save_phases(self, mode, rank, binding, old_state, end_state, next_state):
        previous = old_state.projection.version if old_state is not None else None
        phases = [('baseline',next_state)] if old_state is None else [('window_end',end_state),('next_window',next_state)]
        for phase, state in phases:
            for country, asns in state.feature_dict.items():
                for asn, value in asns.items():
                    prior = old_state.feature_dict.get(country,{}).get(asn) if old_state else None
                    dirty = end_state.feature_dict[country][asn].is_change if end_state else False
                    if prior == value and not dirty: continue
                    self.append('state_deltas', {'mode':mode,'source_id':binding.source_id,'source_rank':rank,'phase':phase,
                        'scope':'asn','country':country,'asn':asn,**asdict(value),
                        'previous_projection':previous,'projection_version':state.projection.version})
            self.append('state_deltas', {'mode':mode,'source_id':binding.source_id,'source_rank':rank,'phase':phase,
                'scope':'collect','country':'collect','asn':'',**asdict(state.feature_collect_dict),'is_change':False,
                'previous_projection':previous,'projection_version':state.projection.version})

    def source_complete(self, mode, rank, binding, source_end, old_state, state, expected_windows):
        counted = self.source_counts.get((mode,binding.source_id), {'audit':0,'windows':0})
        if counted != {'audit':binding.expected_elements,'windows':expected_windows}:
            raise ValueError('Feature审计/窗口必要输出不完整')
        expected_diagnostics = self.diagnostic_expected.get((mode,binding.source_id))
        if expected_diagnostics is None: raise ValueError('缺少诊断保存回执')
        self.flush()
        saved_diagnostics = self.db.execute(f'SELECT count(*) FROM lake.{self.schema}.module_diagnostics WHERE mode=? AND source_id=?', [mode,binding.source_id]).fetchone()[0]
        if saved_diagnostics != expected_diagnostics: raise ValueError('诊断必要输出不完整')
        if self.specification.get('output_profile')==PROFILE:self.check_qualification_source(mode,binding.source_id)
        row = self.source_receipt(mode,rank,binding,source_end,old_state,state,expected_windows)
        self.append('source_receipts', row)
        self.flush()
        with self.pg, self.pg.cursor() as c:
            c.execute('INSERT INTO feature.source_commits VALUES (%s,%s,%s,%s,%s,%s,%s)',
                      (self.run_id,mode,binding.source_id,row['previous_projection'],row['projection_version'],source_end.messages,source_end.elements))

    def source_receipt(self, mode, rank, binding, source_end, old_state, state, expected_windows):
        """仅构造原回执行；供生产及同规则只读审计复用。"""
        expected_diagnostics = self.diagnostic_expected[mode,binding.source_id]
        row = {'diagnostics':expected_diagnostics,'diagnostics_state':'saved' if rank else 'not_applicable',
            'mode':mode,'source_id':binding.source_id,'source_rank':rank,
            'previous_projection':old_state.projection.version if old_state else None,'projection_version':state.projection.version,
            'messages':source_end.messages,'elements':source_end.elements,'windows':expected_windows,
            'state_messages':source_end.state_messages,'eor_records':source_end.eor_records,
            'local_messages':source_end.local_messages,'quality_records':source_end.quality_records,'quality':state.projection.coverage,
            'input_gaps':json.dumps([dict(left_source=g.left_source,right_source=g.right_source,start=g.start.isoformat(),end=g.end.isoformat(),basis=g.basis) for g in state.projection.input_gaps]), 'reference_version':state.reference_version,
            'rib_time':state.projection.rib_time,'last_t':state.t}
        return row

    def check_qualification_source(self, mode, source):
        self.flush()
        expected=self.qualification_expected.get((mode,source))
        if expected is None:raise ValueError('缺少独立资格来源回执')
        def rows(table):
            return self.db.execute(f'SELECT * EXCLUDE(window_role) FROM lake.{self.schema}.{table} WHERE mode=? AND source_id=?',[mode,source]).fetch_arrow_table().to_pylist()
        receipts=rows('qualification_receipts')
        if receipts!=[expected]:raise ValueError('资格来源回执不一致')
        quals=rows('qualifications')
        quals.sort(key=lambda r:DIMENSIONS.index(r['dimension']))
        if len(quals)!=len(DIMENSIONS) or {q['dimension'] for q in quals}!=set(DIMENSIONS) or identity(quals)!=expected['qualification_digest']:
            raise ValueError('独立资格枚举或身份不完整')
        gaps=rows('input_gaps');quality=rows('source_qualities')
        if len(gaps)!=expected['gaps'] or len(quality)!=expected['qualities'] or len({g['gap_id'] for g in gaps})!=len(gaps):
            raise ValueError('Gap/质量证据计数不一致')
        for q in quals:
            if q['qualification_id']!=identity({k:v for k,v in q.items() if k!='qualification_id'}):raise ValueError('资格摘要漂移')
            for table,column,refs in [('input_gaps','gap_id',q['gap_refs']),('source_qualities','evidence_id',q['quality_refs'])]:
                found=self.db.execute(f'SELECT {column} FROM lake.{self.schema}.{table} WHERE mode=? AND source_rank<=?', [mode,q['source_rank']]).fetchall()
                if not set(refs)<={r[0] for r in found}:raise ValueError('资格引用缺失或引用未来证据')

    def validate(self, states):
        self.flush()
        with self.pg.cursor() as c:
            for mode, state in states.items():
                c.execute('SELECT prefix,vp,path FROM feature.paths WHERE run_id=%s AND mode=%s', (self.run_id,mode))
                count = 0
                while rows := c.fetchmany(1000):
                    self.guard()
                    for prefix,vp,path in rows:
                        if state.projection.prefix_dict.get(prefix,{}).get(vp) != path: raise ValueError('PG路径与投影不一致')
                        count += 1
                if count != sum(map(len,state.projection.prefix_dict.values())): raise ValueError('PG路径缺失')
                c.execute('SELECT prefix,origin FROM feature.origins WHERE run_id=%s AND mode=%s', (self.run_id,mode))
                count = 0
                while rows := c.fetchmany(1000):
                    self.guard()
                    for prefix,origin in rows:
                        if origin not in state.projection.prefix_as.get(prefix,()): raise ValueError('PG起源不一致')
                        count += 1
                if count != sum(map(len,state.projection.prefix_as.values())): raise ValueError('PG起源缺失')
                c.execute('SELECT vp FROM feature.seen_vps WHERE run_id=%s AND mode=%s', (self.run_id,mode))
                if {r[0] for r in c.fetchall()} != set(state.projection.seen_vps): raise ValueError('PG历史VP集合不一致')
                c.execute('SELECT scope,country,asn,v4prefix_num,v6prefix_num,v4ip_num,announ_num,withdraw_num,is_change FROM feature.states WHERE run_id=%s AND mode=%s', (self.run_id,mode))
                count = 0
                while rows := c.fetchmany(1000):
                    self.guard()
                    for scope,country,asn,*values in rows:
                        expected = state.feature_collect_dict if scope == 'collect' else state.feature_dict.get(country,{}).get(asn)
                        if expected is None or values != [*(getattr(expected,n) for n,_ in METRICS),getattr(expected,'is_change',False)]:
                            raise ValueError('PG清零后业务态不一致')
                        count += 1
                if count != 1+sum(map(len,state.feature_dict.values())): raise ValueError('PG业务态缺失')
        self.guard()
        self.validated = True

    def finish(self, extra, *, validate_inputs):
        if not self.validated: raise ValueError('未完成工作态对账')
        self.flush()
        expected = {(m,s) for m in ('ordinary','ir') for s in self.specification['source_ids']}
        with self.pg.cursor() as c:
            c.execute('SELECT mode,source_id FROM feature.source_commits WHERE run_id=%s', (self.run_id,))
            if set(c.fetchall()) != expected: raise ValueError('缺少必要来源完成提交')
        actual = {t:self.db.execute(f'SELECT count(*) FROM lake.{self.schema}.{t}').fetchone()[0] for t in self.tables}
        if actual['module_diagnostics'] != sum(self.diagnostic_expected.values()): raise ValueError('诊断最终计数不一致')
        if actual != self.counts or actual['source_receipts'] != len(expected): raise ValueError('历史必要输出计数不一致')
        if actual['reference_rows'] == 0 or actual['state_deltas'] == 0: raise ValueError('缺少参考/业务态历史')
        if self.specification.get('output_profile')==PROFILE:
            for mode,source in expected:self.check_qualification_source(mode,source)
        qualification_hashes={}
        if self.specification.get('output_profile')==PROFILE:
            for table,keys in ORDER_KEYS.items():
                result=self.db.execute(f'SELECT * FROM lake.{self.schema}.{table} ORDER BY '+','.join(keys))
                def checked_rows():
                    for batch in result.fetch_record_batch(1000):
                        self.guard()
                        yield from batch.to_pylist()
                qualification_hashes[table]=rows_digest(checked_rows())
        snapshot = self.db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        qualified_receipt={'specification_digest':identity(self.specification),'reference_version':extra.get('reference_version'),'actual_rows':actual,
                           'qualification_hashes':qualification_hashes,'qualification_receipts':list(self.qualification_expected.values())}
        if self.specification.get('output_profile')==PROFILE:extra={**extra,'qualification_completion':qualified_receipt}
        receipt = {**extra,'run_id':self.run_id,'snapshot':snapshot,'state':'ready','specification':self.specification,
                   'actual_rows':actual,'flush_metrics':self.flush_metrics,'publication_authority':'feature.runs'}
        # 所有私有源提交和输出检查之后、ready/complete之前复验真实消费的固定输入。
        validate_inputs()
        self.write_ready(receipt)
        with self.pg, self.pg.cursor() as c:
            if self.specification.get('output_profile')==PROFILE:
                c.execute('INSERT INTO feature.qualified_results VALUES (%s,%s,%s)',(self.run_id,snapshot,json.dumps(qualified_receipt)))
            c.execute("UPDATE feature.runs SET state='complete',snapshot=%s WHERE run_id=%s AND state='candidate'", (snapshot,self.run_id))
            if c.rowcount != 1: raise ValueError('运行状态不允许完成')
        return {**receipt,'state':'complete'}

    def write_ready(self, receipt):
        with (self.root/'execution.json').open('x') as f:
            json.dump(receipt,f,ensure_ascii=False,indent=2); f.flush(); os.fsync(f.fileno())
        fd = os.open(self.root,os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)

    def fail(self, reason):
        self.pg.rollback()
        with self.pg, self.pg.cursor() as c:
            c.execute("UPDATE feature.runs SET state='failed',reason=%s WHERE run_id=%s AND state='candidate'", (str(reason),self.run_id))

    def close(self):
        self.db.close(); self.pg.close()


def read_table(dsn, run_id, expected_snapshot, table, batch_rows=1000, *, allow_fixture=False, window_role=None, profile=None):
    tables={**TABLES,**QUALIFICATION_TABLES} if profile==PROFILE else TABLES
    if profile not in (None,PROFILE):raise ValueError('未知Feature输出profile')
    if table not in tables or not run_id.isalnum(): raise ValueError('读取范围无效')
    if window_role is not None and (window_role not in ('initial','warmup','comparison','result') or ('window_role','VARCHAR') not in tables[table]):
        raise ValueError('该表或窗口用途不可筛选')
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute('SELECT state,snapshot,schema_name,specification FROM feature.runs WHERE run_id=%s', (run_id,))
            row=c.fetchone()
            if row is None or row[:3] != ('complete',expected_snapshot,'f_'+run_id): raise ValueError('Feature候选/失败/错版不可读')
            if row[3].get('output_profile')!=profile:raise ValueError('Feature输出profile必须显式匹配，旧消费不可读取新资格结果')
            if not allow_fixture and row[3].get('code_identity',{}).get('execution_mode')!='frozen-fresh-process':
                raise ValueError('正式消费拒绝synthetic或未知执行身份')
    if table == 'module_diagnostics':
        version = row[3].get('diagnostics_dataset_version')
        if version is None: raise ValueError('module_diagnostics: not_saved（旧版本未保存诊断）')
        if version != DIAGNOSTICS_VERSION: raise ValueError('module_diagnostics: unsupported_version')
    db = connect_duckdb()
    try:
        db.execute('LOAD ducklake'); db.execute('LOAD postgres')
        db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+" AS lake (READ_ONLY, METADATA_SCHEMA "+literal("fl_"+run_id)+")")
        predicate=' WHERE window_role=?' if window_role is not None else ''
        result = db.execute(f'SELECT * FROM lake.f_{run_id}.{table} AT (VERSION => {int(expected_snapshot)})'+predicate,
                            [window_role] if window_role is not None else [])
        yield from result.fetch_record_batch(batch_rows)
    finally: db.close()


def reconstruct_phases(dsn, run_id, snapshot, mode, *, allow_fixture=False):
    """审计用历史重建，不作为恢复加载入口。"""
    rows = [r for b in read_table(dsn,run_id,snapshot,'state_deltas',allow_fixture=allow_fixture) for r in b.to_pylist() if r['mode']==mode]
    phase_order = {'baseline':0,'window_end':1,'next_window':2}
    rows.sort(key=lambda r:(r['source_rank'],phase_order[r['phase']]))
    state = {}; output = {}
    for row in rows:
        key = (row['scope'],row['country'],row['asn'])
        state[key] = tuple(row[n] for n,_ in METRICS)+(row['is_change'],)
        output[row['source_rank'],row['phase']] = dict(state)
    return output
