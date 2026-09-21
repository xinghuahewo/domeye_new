"""历史深模块：冻结包准入、固定版本行流与显式离线人工副本重建。

complete只指声明的人工封存范围；此模块不持有任何业务head。
"""
from dataclasses import asdict, dataclass
from contextlib import contextmanager, closing
import hashlib
import math
from pathlib import Path
import re
import struct
import uuid

import pyarrow as pa
from psycopg2 import sql

from data_pipeline.bgp.archive.store import connect_duckdb, literal
from data_pipeline.history.database_import.binding import PGIdentity
from data_pipeline.history.database_import.codec import RULE, SCHEMA, arrow_schema, canonical, typed, untyped, pg_kind
from data_pipeline.history.database_import.freeze import Limits, Guard, block_rows, digest, private_pg, read_json, sha_file, sync_directory, verify_package, write_json, pg_output, read_rows


@dataclass(frozen=True)
class Token:
    import_id: str
    source_freeze_id: str
    manifest_sha256: str
    snapshot: int
    ready_sha256: str


def _equal(left, right):
    if isinstance(left, float) and isinstance(right, float):
        return (math.isnan(left) and math.isnan(right)) or struct.pack('>d', left) == struct.pack('>d', right)
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_equal(left[k], right[k]) for k in left)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(_equal(a,b) for a,b in zip(left,right))
    return left == right


class History:
    def __init__(self, dsn, private_root, limits=Limits(), *, target_identity=None):
        self.dsn = dsn; self.root = Path(private_root).resolve(); self.limits = limits
        # 构造不写文件/catalog；所有写入只在import/rebuild。
        self.data_root = self.root/'history'
        if target_identity is not None and not isinstance(target_identity, PGIdentity): raise ValueError('目标绑定身份无效')
        self.target_identity = target_identity

    def _connect(self):
        pg = private_pg(self.dsn, self.root)
        try:
            if self.target_identity is not None and PGIdentity.read(pg) != self.target_identity: raise ValueError('私有目标绑定身份不符')
            pg.rollback()
            return pg
        except BaseException:
            pg.close(); raise

    def _check_source_target(self, manifest):
        if manifest['data_kind'] != 'retained-history': return
        if self.target_identity is None: raise ValueError('非fixture载体须显式绑定独立私有目标')
        if manifest['engine'] == 'postgres':
            source = manifest['source']
            if PGIdentity(source['instance_system_identifier'], source['database_oid']) == self.target_identity:
                raise ValueError('源不能作为archive/catalog/query-copy目标')

    def bulk(self, token, *, table_indices=None, purpose='audit', batch_rows=None):
        from data_pipeline.history.database_import.reader import BulkReader
        return BulkReader(self, token, table_indices=table_indices, purpose=purpose, batch_rows=batch_rows)

    def _initialize(self, pg):
        self.data_root.mkdir(exist_ok=True)
        with pg, pg.cursor() as c:
            c.execute('CREATE SCHEMA IF NOT EXISTS history_q3')
            c.execute('''CREATE TABLE IF NOT EXISTS history_q3.imports(
                source_freeze_id TEXT PRIMARY KEY, manifest_sha256 TEXT NOT NULL,
                import_id TEXT UNIQUE NOT NULL,state TEXT NOT NULL,snapshot BIGINT,
                ready_sha256 TEXT,private_root TEXT NOT NULL,reason TEXT)''')

    def _lake(self, import_id, readonly):
        if not re.fullmatch('[0-9a-f]{32}', import_id): raise ValueError('import身份无效')
        # DuckLake可能自行打开新的PG连接；每次attach前验证显式目标身份。
        if self.target_identity is not None: self._connect().close()
        db = connect_duckdb()
        try:
            db.execute("SET memory_limit='256MB'"); db.execute("SET max_temp_directory_size='0B'"); db.execute('SET threads=1'); db.execute("SET TimeZone='UTC'")
            db.execute('LOAD ducklake'); db.execute('LOAD postgres')
            versions = dict(db.execute("SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded").fetchmany(64))
            if versions.get('ducklake') != '3f1b372' or versions.get('postgres_scanner') != 'b9fce43': raise ValueError('未锁定DuckLake/PG扩展')
            options = 'READ_ONLY, ' if readonly else 'DATA_INLINING_ROW_LIMIT 0, DATA_PATH '+literal(self.data_root/import_id/'parquet')+', '
            db.execute('ATTACH '+literal('ducklake:postgres:'+self.dsn)+' AS lake ('+options+'METADATA_SCHEMA '+literal('hl_'+import_id)+')')
            return db
        except BaseException: db.close(); raise

    def import_package(self, manifest_path):
        """单写者；同冻结同内容完成后确认原token，失败/候选不自动重试。"""
        package = Path(manifest_path).resolve().parent
        m = verify_package(package, self.limits)
        self._check_source_target(m)
        source_sha = sha_file(package/'manifest.json'); freeze_id = m['source_freeze_id']
        pg = self._connect(); db = None; import_id = None; inserted = False
        guard = Guard(self.root, self.limits)
        try:
            self._initialize(pg)
            with pg, pg.cursor() as c:
                # Session锁覆盖文件/湖/登记。仅本冻结串行，不承担恢复平台职责。
                c.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0))', ('history-q3:'+freeze_id,))
                if not c.fetchone()[0]: raise ValueError('同冻结已有导入写入者')
                c.execute('SELECT manifest_sha256,import_id,state,snapshot,ready_sha256,private_root FROM history_q3.imports WHERE source_freeze_id=%s', (freeze_id,))
                old = c.fetchone()
                if old:
                    if old[0] != source_sha or old[5] != str(self.root): raise ValueError('同源身份不同内容/根目录拒绝')
                    if old[2] != 'complete': raise ValueError('已有候选或失败；不自动重试追加')
                    token = Token(old[1], freeze_id, source_sha, old[3], old[4]); self.component(token)
                    return token
                import_id = uuid.uuid4().hex
                c.execute('INSERT INTO history_q3.imports VALUES (%s,%s,%s,\'candidate\',NULL,NULL,%s,NULL)', (freeze_id,source_sha,import_id,str(self.root)))
            inserted = True
            out = self.data_root/import_id; out.mkdir(exist_ok=False)
            # 完整原元数据独立封存；不要求读取时再访问源SQLite/PG导出。
            write_json(out/'source-manifest.json', m)
            db = self._lake(import_id, False); db.execute('CREATE SCHEMA lake.history')
            for ti, table in enumerate(m['tables']):
                schema = arrow_schema(m['engine'], table)
                db.register('history_schema', pa.Table.from_batches([], schema=schema))
                db.execute(f'CREATE TABLE lake.history.t{ti} AS SELECT * FROM history_schema'); db.unregister('history_schema')
                pending = []; pending_bytes = 0; ordinal = 0
                def flush():
                    nonlocal pending_bytes
                    if not pending: return
                    guard.check(); batch = pa.Table.from_pylist(pending, schema=schema)
                    db.register('history_batch', batch)
                    try: db.execute(f'INSERT INTO lake.history.t{ti} SELECT * FROM history_batch')
                    finally: db.unregister('history_batch')
                    guard.max_batch_rows = max(guard.max_batch_rows, len(pending)); guard.max_batch_bytes = max(guard.max_batch_bytes, pending_bytes)
                    pending.clear(); pending_bytes = 0; guard.check()
                for bi, block in enumerate(table['blocks']):
                    for ri, raw in enumerate(block_rows(package, block, self.limits)):
                        data = canonical(raw); guard.row(data)
                        if pending and (len(pending) >= self.limits.batch_rows or pending_bytes+len(data) > self.limits.batch_bytes): flush()
                        row = {'_ordinal': ordinal, '_block': bi, '_row': ri, '_sha256': digest(raw)}
                        row.update({'c'+str(ci): typed(m['engine'], col, value) for ci,(col,value) in enumerate(zip(table['columns'], raw))})
                        pending.append(row); pending_bytes += len(data); ordinal += 1
                flush()
            snapshot = db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
            # 在同固定snapshot逐行重核typed值、完整raw摘要和occurrence序列。
            validation = self._validate(db, m, snapshot, guard)
            files = self._files(db, m, snapshot, out, guard)
            verify_package(package, self.limits)
            if sha_file(package/'manifest.json') != source_sha: raise ValueError('导入期间冻结清单变化')
            with pg.cursor() as c:
                c.execute('SELECT system_identifier::text FROM pg_control_system()'); system = c.fetchone()[0]
                c.execute('SELECT oid FROM pg_database WHERE datname=current_database()'); oid = c.fetchone()[0]
            ready = {'schema_version': SCHEMA, 'import_rule_version': RULE, 'import_id': import_id, 'source_freeze_id': freeze_id,
                     'source_manifest_sha256': source_sha, 'snapshot': snapshot, 'state': 'ready', 'data_kind': m['data_kind'],
                     'catalog_ref': {'system_identifier': system,'database_oid': oid,'metadata_schema': 'hl_'+import_id},
                     'schema_name': 'history', 'tables': validation, 'files': files, 'resources': guard.report(),
                     'limits': asdict(self.limits), 'source_manifest': 'source-manifest.json',
                     'availability': m['availability'], 'code_sha256': {p.name: sha_file(p) for p in Path(__file__).parent.glob('*.py')}}
            write_json(out/'ready.json', ready); sync_directory(out); sync_directory(self.data_root)
            ready_sha = sha_file(out/'ready.json')
            with pg, pg.cursor() as c:
                c.execute("UPDATE history_q3.imports SET state='complete',snapshot=%s,ready_sha256=%s WHERE source_freeze_id=%s AND state='candidate' AND manifest_sha256=%s", (snapshot,ready_sha,freeze_id,source_sha))
                if c.rowcount != 1: raise ValueError('完成登记竞争/身份不符')
            return Token(import_id, freeze_id, source_sha, snapshot, ready_sha)
        except BaseException as exc:
            if inserted:
                pg.rollback()
                with pg, pg.cursor() as c:
                    c.execute("UPDATE history_q3.imports SET state='failed',reason=%s WHERE import_id=%s AND state='candidate'", (str(exc), import_id))
            raise
        finally:
            if db: db.close()
            pg.close()

    def _validate(self, db, m, snapshot, guard):
        result = []
        for ti, table in enumerate(m['tables']):
            ordinal = 0; h = hashlib.sha256(); block_counts = [0]*len(table['blocks'])
            expected_bi = expected_ri = 0
            width = self._read_width(db,ti,snapshot,table)
            cursor = db.execute(f'SELECT * FROM lake.history.t{ti} AT (VERSION => {int(snapshot)}) ORDER BY _ordinal')
            with closing(self._batches(cursor,width,guard,expected_schema=arrow_schema(m['engine'],table))) as batches:
                for batch in batches:
                    guard.check()
                    for row in batch.to_pylist():
                        if row['_ordinal'] != ordinal or row['_block'] != expected_bi or row['_row'] != expected_ri: raise ValueError('occurrence身份/顺序不符')
                        raw = self._raw(m, table, row)
                        if digest(raw) != row['_sha256']: raise ValueError('历史原值行摘要不符')
                        data = canonical(raw); h.update(data+b'\n'); ordinal += 1
                        block_counts[expected_bi] += 1; expected_ri += 1
                        if expected_ri == table['blocks'][expected_bi]['rows']: expected_bi += 1; expected_ri = 0
            if ordinal != table['rows'] or h.hexdigest() != table['content_sha256'] or block_counts != [b['rows'] for b in table['blocks']]: raise ValueError('历史整表/块完整性不符')
            result.append({'table_index': ti,'schema': table['schema'],'name': table['name'],'rows': ordinal,'content_sha256': h.hexdigest(),'block_counts': block_counts})
        return result

    @staticmethod
    def _raw(m, table, row):
        raw = []
        for ci, col in enumerate(table['columns']):
            value = untyped(m['engine'], row['c'+str(ci)])
            if not _equal(typed(m['engine'], col, value), row['c'+str(ci)]): raise ValueError('typed主体与原值不符')
            raw.append(value)
        return raw

    def _read_width(self, db, ti, snapshot, table):
        # 同一固定snapshot的SQL聚合，仅返回一个宽度，不缓存整表或依赖新清单字段。
        maximum = db.execute(f"SELECT coalesce(max(octet_length(encode(to_json(t)))),0) FROM (SELECT * FROM lake.history.t{ti} AT (VERSION => {int(snapshot)})) t").fetchone()[0]
        # 当前原生标量/struct/list的保守Arrow payload界限，含NULL位图/offset开销。
        # 不是Python对象/RSS的分配前保证；实际Arrow nbytes与进程RSS另行检查。
        return 8*maximum+256*(len(table['columns'])+4)

    def _batches(self, result, width, guard, *, cap=None, expected_schema=None):
        requested = read_rows(self.limits,width,cap)
        guard.check(); reader = result.fetch_record_batch(requested)
        try:
            if expected_schema is not None and (reader.schema.names != expected_schema.names or any(a.type != b.type for a,b in zip(reader.schema,expected_schema))):
                raise ValueError('历史物理typed schema不符')
            for batch in reader:
                guard.check()
                if batch.num_rows > requested or batch.nbytes > max(self.limits.batch_bytes,width): raise ValueError('Arrow读取缓冲字节/行数超限')
                yield batch
                guard.check()
        finally:
            reader.close()

    def _files(self, db, m, snapshot, out, guard):
        files = []; total_bytes = 0
        for ti, table in enumerate(m['tables']):
            previous_files = len(files)
            cur = db.execute(f"SELECT * FROM ducklake_list_files('lake','t{ti}',schema=>'history',snapshot_version=>{int(snapshot)})")
            names = [d[0] for d in cur.description]
            while batch := cur.fetchmany(self.limits.batch_rows):
                for values in batch:
                    row = dict(zip(names, values))
                    for key in ('data_file', 'delete_file'):
                        if not row.get(key): continue
                        path = Path(row[key]).resolve()
                        if not path.is_relative_to(out/'parquet'): raise ValueError('catalog引用超出私有目录')
                        with path.open('rb') as f: __import__('os').fsync(f.fileno())
                        parent = path.parent
                        while parent != out:
                            sync_directory(parent); parent = parent.parent
                        sync_directory(out)
                        total_bytes += path.stat().st_size
                        if total_bytes > self.limits.max_total_bytes: raise ValueError('历史输出字节超限')
                        files.append({'path': str(path), 'bytes': path.stat().st_size, 'sha256': sha_file(path, guard)})
                        if len(canonical(files)) > self.limits.max_metadata_bytes: raise ValueError('文件核验回执预算超限')
            if table['rows'] and len(files) == previous_files: raise ValueError('非空历史缺少实际Parquet文件')
        return files

    def component(self, token):
        """固定组件资格；每次完整校验已绑定Parquet，代价受显式规模预算限制。"""
        return self._component(token)

    def _component(self, token, *, pg=None, lock=False):
        if not isinstance(token, Token) or not re.fullmatch('[0-9a-f]{32}', token.import_id): raise ValueError('必须传固定Token')
        owned = pg is None
        if owned: pg = self._connect()
        try:
            if owned: pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute('SELECT state,snapshot,manifest_sha256,ready_sha256,private_root FROM history_q3.imports WHERE import_id=%s AND source_freeze_id=%s'+(' FOR SHARE' if lock else ''), (token.import_id,token.source_freeze_id))
                row = c.fetchone()
                if row != ('complete',token.snapshot,token.manifest_sha256,token.ready_sha256,str(self.root)): raise ValueError('候选/失败/错版组件不可读')
                c.execute('SELECT system_identifier::text FROM pg_control_system()'); system = c.fetchone()[0]
                c.execute('SELECT oid FROM pg_database WHERE datname=current_database()'); oid = c.fetchone()[0]
                c.execute(sql.SQL('SELECT snapshot_id FROM {}.ducklake_snapshot WHERE snapshot_id=%s').format(sql.Identifier('hl_'+token.import_id)),(token.snapshot,))
                if c.fetchone() != (token.snapshot,): raise ValueError('固定catalog快照不存在')
        finally:
            if owned: pg.close()
        out = self.data_root/token.import_id; guard = Guard(out, self.limits)
        if sha_file(out/'ready.json', guard) != token.ready_sha256 or sha_file(out/'source-manifest.json', guard) != token.manifest_sha256: raise ValueError('固定清单SHA不符')
        ready = read_json(out/'ready.json', self.limits.max_metadata_bytes)
        if ready['catalog_ref'] != {'system_identifier':system,'database_oid':oid,'metadata_schema':'hl_'+token.import_id}: raise ValueError('catalog身份不符')
        if ready['snapshot'] != token.snapshot or ready['source_freeze_id'] != token.source_freeze_id: raise ValueError('固定ready版本不符')
        total_bytes = 0
        for f in ready['files']:
            path = Path(f['path']); total_bytes += f['bytes']
            if total_bytes > self.limits.max_total_bytes or not path.resolve().is_relative_to(out/'parquet') or path.is_symlink() or path.stat().st_size != f['bytes'] or sha_file(path, guard) != f['sha256']: raise ValueError('历史文件损坏/越界/预算超限')
        return {'component_key': 'historical:'+token.source_freeze_id, 'origin_kind': 'historical_import', 'qualification': 'complete',
                'business_availability': ready['availability'], 'token': asdict(token), 'binding': ready,
                'original': read_json(out/'source-manifest.json', self.limits.max_metadata_bytes)}

    def scan(self, token, table_index, *, after_ordinal=-1, limit=100, purpose='business'):
        """有界全字段行流；稳定occurrence分页；隔离源只允许显式离线audit读取。"""
        if type(limit) is not int or not 1 <= limit <= self.limits.batch_rows or type(after_ordinal) is not int or after_ordinal < -1: raise ValueError('读取预算/游标无效')
        if purpose not in ('business','audit'): raise ValueError('未知读取用途')
        component = self.component(token); m = component['original']
        if purpose == 'business' and m['availability']['status'] != 'available': raise ValueError('隔离来源禁止业务/详情旁路')
        if type(table_index) is not int or not 0 <= table_index < len(m['tables']): raise ValueError('未知原表')
        table = m['tables'][table_index]; db = self._lake(token.import_id, True); guard = Guard(self.root,self.limits)
        try:
            width = self._read_width(db,table_index,token.snapshot,table)
            result = db.execute(f'SELECT * FROM lake.history.t{table_index} AT (VERSION => {int(token.snapshot)}) WHERE _ordinal > ? ORDER BY _ordinal LIMIT ?', [after_ordinal,limit])
            size = 0; expected = after_ordinal+1; page = []
            with closing(self._batches(result,width,guard,cap=limit)) as batches:
                for batch in batches:
                    for row in batch.to_pylist():
                        raw = self._raw(m,table,row); data = canonical(raw); guard.row(data); size += len(data)+1
                        if size > self.limits.batch_bytes: raise ValueError('本页字节预算超限；减小limit')
                        if row['_ordinal'] != expected or digest(raw) != row['_sha256']: raise ValueError('固定行身份/摘要不符')
                        expected += 1
                        page.append({'occurrence': {'source_freeze_id': token.source_freeze_id,'schema':table['schema'],'table':table['name'],'block':row['_block'],'row':row['_row'],'ordinal':row['_ordinal']}, 'content_sha256': row['_sha256'], 'values': raw, 'typed': {col['name']: row['c'+str(ci)] for ci,col in enumerate(table['columns'])}})
            if expected != min(table['rows'],after_ordinal+1+limit) and after_ordinal < table['rows']: raise ValueError('固定页缺行')
            # 页已完整缓存且有界；最终锁内核验成功前一行也不交付。
            with self._delivery_qualification(token):
                yield from page
        finally: db.close()

    @contextmanager
    def _delivery_qualification(self, token):
        pg = self._connect()
        try:
            pg.set_session(isolation_level='READ COMMITTED',readonly=False)
            with pg:
                self._component(token,pg=pg,lock=True)
                yield
        finally: pg.close()

    def resolve_reference(self, token, reference):
        m = self.component(token)['original']
        if m['availability']['status'] != 'available': return {'status': 'unavailable','reason': m['availability']['reason'],'targets': []}
        matches = [r for r in m['references'] if r['original_ref'] == reference]
        if not matches: return {'status': 'not_retained', 'targets': []}
        targets = [target for r in matches for target in r.get('targets', [])]
        if any(r.get('status') == 'unknown' for r in matches): return {'status':'unknown','reason':[r['reason'] for r in matches if r.get('status')=='unknown'],'targets':targets}
        if any(r.get('status') != 'resolved' for r in matches): return {'status': 'ambiguous_reference' if targets or any(r.get('status') == 'ambiguous_reference' for r in matches) else 'not_retained', 'targets': targets}
        return {'status': 'resolved' if len(targets) == 1 else 'ambiguous_reference','targets': targets}

    def _stream(self, component, token, ti):
        m = component['original']; table = m['tables'][ti]
        db = self._lake(token.import_id, True); guard = Guard(self.root,self.limits)
        try:
            width = self._read_width(db,ti,token.snapshot,table)
            result = db.execute(f'SELECT * FROM lake.history.t{ti} AT (VERSION => {int(token.snapshot)}) ORDER BY _ordinal')
            ordinal = 0; bi = ri = 0; content = hashlib.sha256()
            with closing(self._batches(result,width,guard,expected_schema=arrow_schema(m['engine'],table))) as batches:
                for batch in batches:
                    for row in batch.to_pylist():
                        raw = self._raw(m,table,row); guard.row(canonical(raw))
                        if row['_ordinal'] != ordinal or row['_block'] != bi or row['_row'] != ri or digest(raw) != row['_sha256']: raise ValueError('副本来源行身份不符')
                        occurrence = {'source_freeze_id':token.source_freeze_id,'schema':table['schema'],'table':table['name'],'block':bi,'row':ri,'ordinal':ordinal}
                        ordinal += 1; ri += 1; content.update(canonical(raw)+b'\n')
                        if ri == table['blocks'][bi]['rows']: bi += 1; ri = 0
                        yield {'occurrence':occurrence,'content_sha256':row['_sha256'],'values':raw,'typed':{col['name']:row['c'+str(ci)] for ci,col in enumerate(table['columns'])}}
            if ordinal != table['rows'] or content.hexdigest() != table['content_sha256']: raise ValueError('副本来源缺行/整表摘要不符')
        finally: db.close()

    def rebuild(self, token):
        """只从固定历史构造同语义人工PG查询副本，无业务可见head。"""
        component = self.component(token); m = component['original']
        self._check_source_target(m)
        target = 'hqcopy_'+uuid.uuid4().hex; pg = self._connect()
        statistics = []; written_sql_bytes = 0; writer_guard = Guard(self.root,self.limits)
        try:
            pg.set_session(isolation_level='READ COMMITTED',readonly=False)
            with pg, pg.cursor() as c:
                for setting in ("SET LOCAL TimeZone='UTC'","SET LOCAL DateStyle='ISO,YMD'","SET LOCAL IntervalStyle='iso_8601'","SET LOCAL extra_float_digits=3","SET LOCAL bytea_output='hex'"):
                    c.execute(setting)
                c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(target)))
                c.execute(sql.SQL('CREATE TYPE {}.sqlite_cell AS (storage_class TEXT,integer_value BIGINT,real_bits BYTEA,raw_bytes BYTEA)').format(sql.Identifier(target)))
                for ti, table in enumerate(m['tables']):
                    definitions = []
                    for col in table['columns']:
                        datatype = sql.SQL('{}.sqlite_cell').format(sql.Identifier(target)) if m['engine'] == 'sqlite' else _pg_type(col)
                        definitions.append(sql.SQL('{} {}').format(sql.Identifier(col['name']), datatype))
                    relation = sql.SQL('{}.{}').format(sql.Identifier(target),sql.Identifier('t'+str(ti)))
                    c.execute(sql.SQL('CREATE TABLE {} ({})').format(relation,sql.SQL(',').join(definitions)))
                    count = 0; h = hashlib.sha256()
                    placeholders = [sql.SQL('%s::')+_pg_type(col) if m['engine']=='postgres' else
                        sql.SQL('ROW(%s,%s,%s,%s)::{}.sqlite_cell').format(sql.Identifier(target)) for col in table['columns']]
                    template = sql.SQL('({})').format(sql.SQL(',').join(placeholders))
                    prefix = sql.SQL('INSERT INTO {} VALUES ').format(relation).as_string(pg).encode('utf-8')
                    pending = []; statement_bytes = len(prefix)
                    stats = {'table_index':ti,'statements':0,'rows':0,'encoded_bytes':0,
                             'max_rows_per_statement':0,'max_statement_bytes':0,'max_encoded_row_bytes':0}
                    def flush():
                        nonlocal statement_bytes, written_sql_bytes
                        if not pending: return
                        writer_guard.check()
                        statement = prefix+b','.join(pending)
                        if len(statement) > self.limits.batch_bytes or written_sql_bytes+len(statement) > self.limits.max_total_bytes: raise ValueError('PG实际语句字节超限')
                        written_sql_bytes += len(statement)
                        c.execute(statement)
                        stats['statements'] += 1; stats['rows'] += len(pending); stats['encoded_bytes'] += len(statement)
                        stats['max_rows_per_statement'] = max(stats['max_rows_per_statement'],len(pending))
                        stats['max_statement_bytes'] = max(stats['max_statement_bytes'],len(statement))
                        pending.clear(); statement_bytes = len(prefix)
                    for row in self._stream(component, token, ti):
                        values = []
                        for ci,col in enumerate(table['columns']):
                            if m['engine'] == 'postgres': values.append(row['values'][ci])
                            else:
                                cell = row['typed'][col['name']]; values.extend([cell['storage_class'],cell['integer'],cell['real_bits'],cell['bytes']])
                        # mogrify只在客户端编码，不逐行访问PG；包含引号/bytea膨胀。
                        encoded = c.mogrify(template,values); writer_guard.row(encoded)
                        stats['max_encoded_row_bytes'] = max(stats['max_encoded_row_bytes'],len(encoded)+1)
                        if len(prefix)+len(encoded) > self.limits.batch_bytes: raise ValueError('PG单行连同语句前缀超预算')
                        addition = len(encoded)+(1 if pending else 0)
                        if pending and (len(pending) >= self.limits.batch_rows or statement_bytes+addition > self.limits.batch_bytes):
                            flush(); addition = len(encoded)
                        pending.append(encoded); statement_bytes += addition
                        count += 1; h.update(canonical(row['values'])+b'\n')
                    flush(); statistics.append(stats)
                    if count != table['rows'] or h.hexdigest() != table['content_sha256']: raise ValueError('副本重建内容不符')
                    self._verify_copy(pg, relation, m, table)
                # 同一READ COMMITTED事务最终取得完成登记共享锁并复验文件；锁持有至副本提交。
                self._component(token,pg=pg,lock=True)
            return {'schema': target,'token': asdict(token),'tables': [{'name':'t'+str(i),'original_schema':t['schema'],'original_table':t['name'],'rows':t['rows']} for i,t in enumerate(m['tables'])], 'qualification':'artificial_query_copy_only','write_statistics':statistics}
        finally: pg.close()

    def _verify_copy(self, pg, relation, m, table):
        expressions = []
        for col in table['columns']:
            name = sql.Identifier(col['name'])
            if m['engine'] == 'postgres': expressions.append(pg_output(col))
            else:
                expressions.extend(sql.SQL(template).format(name) for template in (
                    '({}).storage_class','({}).integer_value',"encode(({}).real_bits,'hex')","encode(({}).raw_bytes,'hex')"))
        h = hashlib.sha256(); count = 0; guard = Guard(self.root,self.limits)
        with pg.cursor() as probe:
            sizes = sql.SQL('+').join(sql.SQL('coalesce(octet_length(({})::text),0)').format(e) for e in expressions)
            probe.execute(sql.SQL('SELECT coalesce(max({}),0) FROM {}').format(sizes,relation))
            width = 6*probe.fetchone()[0]+5*len(expressions)+2
        fetch_rows = read_rows(self.limits,width)
        with pg.cursor(name='verify_copy_'+uuid.uuid4().hex) as cursor:
            cursor.execute(sql.SQL('SELECT {} FROM {} ORDER BY ctid').format(sql.SQL(',').join(expressions),relation))
            while batch := cursor.fetchmany(fetch_rows):
                for values in batch:
                    if m['engine'] == 'postgres': raw = list(values)
                    else:
                        raw = []
                        for i in range(0,len(values),4):
                            kind, integer, real, binary = values[i:i+4]
                            raw.append({'storage_class':kind,'value':str(integer) if kind=='integer' else real if kind=='real' else binary if kind in ('text','blob') else None})
                    data = canonical(raw); guard.row(data); h.update(data+b'\n'); count += 1
        if count != table['rows'] or h.hexdigest() != table['content_sha256']: raise ValueError('实际PG副本全字段回读不符')


def _pg_type(col):
    kind = pg_kind(col); modifier = col.get('typmod', -1); suffix = ''
    if kind == 'numeric' and modifier >= 4:
        precision = ((modifier-4) >> 16) & 65535; scale = (modifier-4) & 65535
        if not 1 <= precision <= 1000 or not 0 <= scale <= precision: raise ValueError('不支持numeric typmod')
        suffix = f'({precision},{scale})'
    elif kind in ('varchar','bpchar') and modifier >= 4: suffix = f'({modifier-4})'
    elif kind in ('timestamp','timestamptz') and modifier >= 0:
        if modifier > 6: raise ValueError('时间精度超限')
        suffix = f'({modifier})'
    return sql.SQL('pg_catalog.{}'+suffix+('[]' if col['array'] else '')).format(sql.Identifier(kind))
