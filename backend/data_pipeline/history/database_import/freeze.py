"""人工/已授权固定源的离线冻结；不访问环境默认连接或自动发现真实库。"""
from dataclasses import asdict, dataclass
import hashlib
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import resource
import shutil
import sqlite3
import struct
import sys
import uuid

import psycopg2
from psycopg2 import sql

from data_pipeline.history.database_import.codec import RULE, SCHEMA, canonical, typed, pg_kind
from data_pipeline.history.database_import.binding import PGIdentity, PGSource, SQLiteSource, source_label


@dataclass(frozen=True)
class Limits:
    batch_rows: int = 1000
    batch_bytes: int = 4*1024**2
    max_row_bytes: int = 4*1024**2
    max_total_rows: int = 10_000_000
    max_total_bytes: int = 10*1024**3
    max_metadata_bytes: int = 4*1024**2
    min_free_bytes: int = 256*1024**2
    max_rss_bytes: int = 2*1024**3
    max_tables: int = 100
    max_columns: int = 512
    def __post_init__(self):
        if any(type(v) is not int or v < 1 for v in asdict(self).values()): raise ValueError('资源阈值必须为正整数')
        if self.max_row_bytes > self.batch_bytes: raise ValueError('单行预算不得超过批字节预算')


def read_rows(limits, row_bound, cap=None):
    """按同快照实测上界规划缓冲；合法大行独占一批，不降低单行资格。"""
    if type(row_bound) is not int or row_bound < 1: raise ValueError('读取行宽界限无效')
    return min(limits.batch_rows, cap or limits.batch_rows, max(1,limits.batch_bytes//row_bound))


class Guard:
    def __init__(self, root, limits):
        self.root, self.limits = Path(root), limits
        self.rows = self.bytes = self.original_bytes = self.peak_rss = 0
        self.max_batch_rows = self.max_batch_bytes = 0
    def check(self):
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)
        self.peak_rss = max(self.peak_rss, rss)
        if rss > self.limits.max_rss_bytes: raise ValueError('RSS预算超限')
        if shutil.disk_usage(self.root).free < self.limits.min_free_bytes: raise ValueError('磁盘余量不足')
    def row(self, data):
        self.check()
        size = len(data)+1
        if size > self.limits.max_row_bytes: raise ValueError('单行字节预算超限')
        self.rows += 1; self.bytes += size
        if self.rows > self.limits.max_total_rows or self.bytes+self.original_bytes > self.limits.max_total_bytes: raise ValueError('总行/字节预算超限')
    def report(self):
        return {k: getattr(self, k) for k in ('rows', 'bytes', 'original_bytes', 'peak_rss', 'max_batch_rows', 'max_batch_bytes')}


def sha_file(path, guard=None):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(1024**2):
            if guard: guard.check()
            h.update(chunk)
    return h.hexdigest()


def digest(value): return hashlib.sha256(canonical(value)).hexdigest()


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


def write_json(path, value):
    path = Path(path)
    with path.with_suffix(path.suffix+'.pending').open('xb') as f:
        f.write(canonical(value)); f.flush(); os.fsync(f.fileno())
    os.replace(path.with_suffix(path.suffix+'.pending'), path)
    sync_directory(path.parent)


def read_json(path, limit):
    with open(path, 'rb') as f:
        data = f.read(limit+1)
        if len(data) > limit: raise ValueError('元数据字节超限')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ValueError('重复JSON字段')
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=pairs)


def quote(name): return '"'+name.replace('"', '""')+'"'


def private_pg(dsn, root):
    """Q3-A只接显式私有UTF8 socket实例；拒绝TCP/其他任务目录。"""
    conn = psycopg2.connect(dsn)
    try:
        host = conn.get_dsn_parameters().get('host', '')
        if not host.startswith('/') or not Path(host).resolve().is_relative_to(Path(root).resolve()): raise ValueError('必须使用本任务私有socket')
        with conn.cursor() as c:
            c.execute("SELECT current_setting('data_directory'),current_setting('listen_addresses'),current_setting('server_encoding'),inet_server_addr()")
            directory, listen, encoding, address = c.fetchone()
            if not Path(directory).resolve().is_relative_to(Path(root).resolve()) or listen or encoding != 'UTF8' or address is not None:
                raise ValueError('非本任务私有无TCP UTF8 PG')
        conn.rollback()
        return conn
    except BaseException:
        conn.close(); raise


def _new_package(destination, engine, source, limits, availability, refs, *, retained=False):
    root = Path(destination).resolve(); root.mkdir(parents=True, exist_ok=False)
    if availability is None: availability = {'status': 'available', 'reason': None}
    if availability.get('status') not in (('available', 'validation_failed', 'unavailable', 'unknown') if retained else ('available', 'validation_failed', 'unavailable')): raise ValueError('可用性无效')
    if availability['status'] != 'available' and not availability.get('reason'): raise ValueError('缺隔离原因')
    m = {'schema_version': SCHEMA, 'import_rule_version': RULE, 'source_freeze_id': uuid.uuid4().hex,
         'frozen_at': datetime.now(timezone.utc).isoformat(), 'engine': engine, 'data_kind': 'fixture', 'source': source, 'tables': [], 'originals': [],
         'availability': availability, 'references': refs or [], 'limits': asdict(limits)}
    return root, m, Guard(root, limits)


def _write_table(root, manifest, table, rows, expected, guard):
    if len(table['columns']) > guard.limits.max_columns: raise ValueError('列数超限')
    names = [c['name'] for c in table['columns']]
    if len(names) != len(set(names)): raise ValueError('重复原列名')
    index = len(manifest['tables']); blocks = []; pending = []; size = 0; count = 0; h = hashlib.sha256()
    def flush():
        nonlocal size
        if not pending: return
        name = f't{index}-b{len(blocks)}.jsonl'
        with (root/name).open('xb') as f:
            for line in pending: f.write(line+b'\n')
            f.flush(); os.fsync(f.fileno())
        blocks.append({'file': name, 'rows': len(pending), 'bytes': (root/name).stat().st_size, 'sha256': sha_file(root/name, guard)})
        guard.max_batch_rows = max(guard.max_batch_rows, len(pending)); guard.max_batch_bytes = max(guard.max_batch_bytes, size)
        pending.clear(); size = 0
    for row in rows:
        if len(row) != len(names): raise ValueError('缺字段与NULL不能混同')
        for col, raw in zip(table['columns'], row): typed(manifest['engine'], col, raw)
        encoded = canonical(row); guard.row(encoded)
        if pending and (len(pending) >= guard.limits.batch_rows or size+len(encoded)+1 > guard.limits.batch_bytes): flush()
        pending.append(encoded); size += len(encoded)+1; count += 1; h.update(encoded+b'\n')
    flush()
    if count != expected: raise ValueError('原表计数与冻结不符')
    manifest['tables'].append({**table, 'expected_rows': expected, 'rows': count, 'content_sha256': h.hexdigest(), 'blocks': blocks})


def _seal(root, m, guard):
    m['resources'] = guard.report()
    if len(canonical(m)) > guard.limits.max_metadata_bytes: raise ValueError('封存元数据超限')
    write_json(root/'manifest.json', m)
    # 先从封存文件独立检查全部块，再给冻结资格。
    verify_package(root, guard.limits, require_complete=False)
    write_json(root/'COMPLETE.json', {'manifest_sha256': sha_file(root/'manifest.json'), 'source_freeze_id': m['source_freeze_id']})
    return root/'manifest.json'


def freeze_sqlite(source, destination, *, source_version, limits=Limits(), availability=None, references=None, attachments=()):
    return _freeze_sqlite(source, destination, source_version=source_version, limits=limits, availability=availability, references=references, attachments=attachments)


def _freeze_sqlite(source, destination, *, source_version, limits, availability, references, attachments, binding=None, publication=None):
    source = Path(source).resolve()
    if Path(str(source)+'-wal').exists(): raise ValueError('v1仅接受关闭写入的独立SQLite原件，WAL原件须先独立封存')
    root, m, guard = _new_package(destination, 'sqlite', {'origin_uri': source.as_uri(), 'source_version': source_version}, limits, availability, references, retained=binding is not None)
    if binding:
        m.update(data_kind='retained-history', publication=publication)
        m['source'].update(origin_uri=binding.origin_uri, bound_sha256=binding.sha256, consistency='independent_closed_file')
    original = root/'original.sqlite'; before = sha_file(source, guard)
    if binding and before != binding.sha256: raise ValueError('SQLite源绑定SHA不符')
    with source.open('rb') as src, original.open('xb') as dst:
        while chunk := src.read(min(1024**2, limits.batch_bytes)):
            guard.check(); dst.write(chunk)
            if dst.tell() > limits.max_total_bytes: raise ValueError('原件字节超限')
        dst.flush(); os.fsync(dst.fileno())
    if before != sha_file(source, guard) or before != sha_file(original, guard) or Path(str(source)+'-wal').exists(): raise ValueError('SQLite复制期间原件变化')
    guard.original_bytes = original.stat().st_size
    m['originals'] = [{'file': original.name, 'bytes': original.stat().st_size, 'sha256': before,'origin_uri':m['source']['origin_uri'],'role':'sqlite_original','source_version':source_version}]
    _attachments(root,m,attachments,guard)
    db = sqlite3.connect(original.as_uri()+'?mode=ro', uri=True); db.text_factory = bytes
    try:
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        encoding = db.execute('PRAGMA encoding').fetchone()[0].decode()
        entries = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY rowid").fetchmany(limits.max_tables*8+1)
        if len(entries) > limits.max_tables*8: raise ValueError('SQLite schema对象超限')
        m['export'] = {'tool': RULE, 'tool_sha256': sha_file(__file__), 'python': sys.version, 'sqlite_version': sqlite3.sqlite_version, 'transaction': 'BEGIN/query_only', 'encoding': encoding,
                       'schema_objects': [[v.decode() if isinstance(v, bytes) else v for v in r] for r in entries]}
        for obj_type, name, _, ddl in entries:
            if obj_type != b'table' or name.startswith(b'sqlite_'): continue
            if len(m['tables']) >= limits.max_tables: raise ValueError('表数超限')
            name = name.decode(); ddl = ddl.decode()
            if 'VIRTUAL TABLE' in ddl.upper(): raise ValueError('未支持虚拟表')
            columns = []
            for cid, col, declared, notnull, default, pk, hidden in db.execute('PRAGMA table_xinfo('+quote(name)+')').fetchmany(limits.max_columns+1):
                if hidden: raise ValueError('v1不支持生成/隐藏列，不静默丢字段')
                declaration = declared.decode(); upper = declaration.upper()
                affinity = 'INTEGER' if 'INT' in upper else 'TEXT' if any(x in upper for x in ('CHAR','CLOB','TEXT')) else 'BLOB' if not upper or 'BLOB' in upper else 'REAL' if any(x in upper for x in ('REAL','FLOA','DOUB')) else 'NUMERIC'
                columns.append({'ordinal': cid, 'name': col.decode(), 'declared_type': declaration, 'affinity': affinity, 'not_null': bool(notnull), 'default': default.decode() if default is not None else None, 'pk_position': pk})
            table = {'schema': 'main', 'name': name, 'columns': columns, 'original_ddl': ddl,
                     'identity': {'kind': 'primary_key' if any(c['pk_position'] for c in columns) else 'no_primary_key', 'occurrence': 'freeze/table/block/row'}}
            expr = ','.join('typeof('+quote(c['name'])+'),'+quote(c['name']) for c in columns)
            size_expr = '+'.join('coalesce(length(cast('+quote(c['name'])+' AS BLOB)),0)' for c in columns)
            # 同一只读事务获得实际行宽；hex及cell结构计入保守读取上界。
            wire_width = db.execute('SELECT coalesce(max('+size_expr+'),0) FROM '+quote(name)).fetchone()[0]
            if wire_width > limits.max_row_bytes: raise ValueError('SQLite源行超字节预算')
            fetch_rows = read_rows(limits,2*wire_width+80*len(columns)+1)
            expected = db.execute('SELECT count(*) FROM '+quote(name)).fetchone()[0]
            cursor = db.execute('SELECT '+expr+' FROM '+quote(name))
            def rows():
                while batch := cursor.fetchmany(fetch_rows):
                    for r in batch:
                        output = []
                        for i in range(0, len(r), 2):
                            kind, value = r[i].decode(), r[i+1]
                            raw = str(value) if kind == 'integer' else struct.pack('>d', value).hex() if kind == 'real' else value.hex() if kind in ('text', 'blob') else None
                            output.append({'storage_class': kind, 'value': raw})
                        yield output
            _write_table(root, m, table, rows(), expected, guard)
        db.rollback()
    finally: db.close()
    return _seal(root, m, guard)


def freeze_postgres(dsn, private_root, tables, destination, *, source_version, limits=Limits(), availability=None, references=None, attachments=()):
    return _freeze_postgres(lambda: private_pg(dsn, private_root), tables, destination, source_version=source_version, limits=limits, availability=availability, references=references, attachments=attachments)


def _freeze_postgres(connect, tables, destination, *, source_version, limits, availability, references, attachments, binding=None, publication=None):
    if not tables or len(tables) > limits.max_tables or len(set(tables)) != len(tables): raise ValueError('原表集合为空/重复/超限')
    root, m, guard = _new_package(destination, 'postgres', {'source_version': source_version}, limits, availability, references, retained=binding is not None)
    _attachments(root,m,attachments,guard)
    if binding: m.update(data_kind='retained-history', publication=publication)
    db = connect()
    try:
        if not binding: db.set_session(readonly=True, isolation_level='REPEATABLE READ')
        with db.cursor() as c:
            for statement in ("SET LOCAL TimeZone='UTC'", "SET LOCAL DateStyle='ISO, YMD'", "SET LOCAL IntervalStyle='iso_8601'", "SET LOCAL extra_float_digits=3", "SET LOCAL bytea_output='hex'", "SET LOCAL client_encoding='UTF8'", "SET LOCAL search_path=pg_catalog"):
                c.execute(statement)
            c.execute('SELECT system_identifier::text FROM pg_control_system()'); system = c.fetchone()[0]
            c.execute('SELECT oid FROM pg_database WHERE datname=current_database()'); oid = c.fetchone()[0]
            c.execute('SELECT txid_current_snapshot()::text,pg_export_snapshot(),transaction_timestamp()::text,version()'); snapshot, token, instant, version = c.fetchone()
            m['source'].update(instance_system_identifier=system, database_oid=oid, origin_uri=binding.origin_uri if binding else f'fixture-pg://{system}/{oid}')
            if binding and PGIdentity(system, oid) != binding.identity: raise ValueError('PG源绑定身份漂移')
            m['export'] = {'tool': RULE, 'tool_sha256': sha_file(__file__), 'python': sys.version, 'driver': psycopg2.__version__, 'server': version, 'snapshot': snapshot, 'export_snapshot': token, 'transaction_time': instant,
                           'isolation': 'repeatable read', 'read_only': True, 'settings': {'TimeZone': 'UTC', 'DateStyle': 'ISO, YMD', 'IntervalStyle': 'iso_8601', 'extra_float_digits': '3', 'bytea_output': 'hex', 'client_encoding': 'UTF8'}}
            for schema, name in tables:
                guard.check()
                # 防止并发DDL使目录和行流不匹配；不阻断普通写事务，所有表同一快照。
                c.execute(sql.SQL('LOCK TABLE {}.{} IN ACCESS SHARE MODE').format(sql.Identifier(schema), sql.Identifier(name)))
                c.execute("SELECT c.oid,c.relkind,c.relrowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s", (schema, name))
                entry = c.fetchone()
                if not entry or entry[1] != 'r': raise ValueError('v1只冻结显式普通表；分区/视图不得静默展开')
                if binding and entry[2]: raise ValueError('源启用RLS，不能把角色可见子集封为完整表')
                relid = entry[0]
                if binding:
                    c.execute('SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent=%s OR inhrelid=%s)', (relid, relid))
                    if c.fetchone()[0]: raise ValueError('显式源表不支持继承/分区展开')
                c.execute('''SELECT a.attnum,a.attname,format_type(a.atttypid,a.atttypmod),a.atttypid,a.atttypmod,a.attnotnull,
                    pg_get_expr(d.adbin,d.adrelid),a.attndims,tn.nspname,t.typname,t.typelem,
                    en.nspname,et.typname,a.attidentity,a.attgenerated,a.attcollation
                    FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid JOIN pg_namespace tn ON tn.oid=t.typnamespace
                    LEFT JOIN pg_type et ON et.oid=t.typelem LEFT JOIN pg_namespace en ON en.oid=et.typnamespace
                    LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
                    WHERE a.attrelid=%s AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum''', (relid,))
                records = c.fetchmany(limits.max_columns+1)
                if len(records) > limits.max_columns: raise ValueError('原列数超限')
                if not records: raise ValueError('v1不支持零列表')
                columns = []
                for num, col, formatted, typid, typmod, nn, default, ndims, ns, typ, elem, ens, etyp, identity, generated, collation in records:
                    is_array = bool(elem) and typ == '_'+str(etyp)
                    column = {'ordinal': num, 'name': col, 'formatted_type': formatted, 'type_oid': typid, 'typmod': typmod,
                              'not_null': nn, 'default': default, 'declared_dimensions': ndims, 'type_schema': ens if is_array else ns,
                              'base_type': etyp if is_array else typ, 'array': is_array, 'original_type': typ,
                              'identity_generation': identity, 'generated': generated, 'collation_oid': collation}
                    if generated: raise ValueError('v1生成列须另立重建规则')
                    pg_kind(column); columns.append(column)
                c.execute('SELECT conname,contype,pg_get_constraintdef(oid),conkey,condeferrable,condeferred FROM pg_constraint WHERE conrelid=%s ORDER BY oid', (relid,))
                constraints = c.fetchmany(limits.max_columns+1)
                if len(constraints) > limits.max_columns: raise ValueError('约束元数据超限')
                table = {'schema': schema, 'name': name, 'columns': columns,
                         'constraints': [dict(zip(('name','kind','definition','columns','deferrable','initially_deferred'), r)) for r in constraints],
                         'identity': {'kind': 'primary_key' if any(r[1] == 'p' for r in constraints) else 'no_primary_key', 'occurrence': 'freeze/table/block/row', 'ctid': 'same_snapshot_order_only'}}
                relation = sql.SQL('{}.{}').format(sql.Identifier(schema), sql.Identifier(name))
                c.execute(sql.SQL('SELECT count(*) FROM {}').format(relation)); expected = c.fetchone()[0]
                if expected+guard.rows > limits.max_total_rows: raise ValueError('原表行数预算超限')
                fields = sql.SQL(',').join(pg_output(col) for col in columns)
                sizes = sql.SQL('+').join(sql.SQL('coalesce(octet_length({}),0)').format(pg_output(col)) for col in columns)
                c.execute(sql.SQL('SELECT coalesce(max({}),0) FROM {}').format(sizes,relation))
                wire_width = c.fetchone()[0]
                if wire_width > limits.max_row_bytes: raise ValueError('PG源行超字节预算')
                fetch_rows = read_rows(limits,6*wire_width+5*len(columns)+2)
                with db.cursor(name='freeze_'+uuid.uuid4().hex) as stream:
                    stream.itersize = fetch_rows
                    stream.execute(sql.SQL('SELECT {} FROM {} ORDER BY ctid').format(fields, relation))
                    def rows():
                        while batch := stream.fetchmany(stream.itersize):
                            yield from (list(r) for r in batch)
                    _write_table(root, m, table, rows(), expected, guard)
        db.rollback()
    finally: db.close()
    return _seal(root, m, guard)


def verify_package(root, limits=Limits(), *, require_complete=True):
    root = Path(root); guard = Guard(root, limits)
    m = read_json(root/'manifest.json', limits.max_metadata_bytes)
    if m['schema_version'] != SCHEMA or m['import_rule_version'] != RULE or m['engine'] not in ('sqlite','postgres') or m['data_kind'] not in ('fixture','retained-history'): raise ValueError('冻结合同/类型不支持')
    if m['data_kind'] == 'retained-history':
        _retained_metadata(m.get('publication'), m.get('availability'))
        source = m['source']
        source_label(source['origin_uri'], source['source_version'])
        if m['engine'] == 'postgres': PGIdentity(source['instance_system_identifier'], source['database_oid'])
        elif source.get('consistency') != 'independent_closed_file' or not any(o['sha256'] == source.get('bound_sha256') and o['role'] == 'sqlite_original' for o in m['originals']): raise ValueError('SQLite显式原件绑定不符')
    if not isinstance(m['source_freeze_id'], str) or len(m['source_freeze_id']) != 32: raise ValueError('冻结身份无效')
    if require_complete:
        complete = read_json(root/'COMPLETE.json', limits.max_metadata_bytes)
        if complete != {'manifest_sha256': sha_file(root/'manifest.json', guard), 'source_freeze_id': m['source_freeze_id']}: raise ValueError('冻结未complete或身份不同')
    if len(m['tables']) > limits.max_tables: raise ValueError('冻结表数超限')
    availability = m.get('availability', {})
    if availability.get('status') not in (('available','validation_failed','unavailable','unknown') if m['data_kind']=='retained-history' else ('available','validation_failed','unavailable')) or (availability['status'] != 'available' and not availability.get('reason')): raise ValueError('原可用性不完整')
    for reference in m.get('references', []):
        if reference.get('status') == 'unknown' and (m['data_kind'] != 'retained-history' or not reference.get('reason')): raise ValueError('Unknown ref缺原因/合同不支持')
        if reference.get('status') not in ('resolved','not_retained','ambiguous_reference','unknown') or not isinstance(reference.get('original_ref'),str): raise ValueError('原ref状态不完整')
        targets = reference.get('targets')
        if not isinstance(targets,list) or (reference['status'] == 'resolved' and len(targets) != 1) or (reference['status'] == 'not_retained' and targets): raise ValueError('原ref资格/目标不符')
        for target in targets:
            ti, ordinal = target.get('table_index'), target.get('ordinal')
            if type(ti) is not int or not 0 <= ti < len(m['tables']) or type(ordinal) is not int or not 0 <= ordinal < m['tables'][ti]['rows']: raise ValueError('原ref目标越界')
    names = [(t['schema'],t['name']) for t in m['tables']]
    if len(names) != len(set(names)): raise ValueError('原表重复')
    files = set()
    for original in m['originals']:
        guard.original_bytes += original['bytes']
        if guard.original_bytes > limits.max_total_bytes: raise ValueError('原件总字节超限')
        _verify_file(root, original, files, guard)
    for table in m['tables']:
        if len(table['columns']) > limits.max_columns: raise ValueError('列数超限')
        columns = table['columns']
        if len({col['name'] for col in columns}) != len(columns): raise ValueError('原列重复')
        h = hashlib.sha256(); count = 0
        for bi, block in enumerate(table['blocks']):
            _verify_file(root, block, files, guard)
            n = 0
            for row in block_rows(root, block, limits):
                if not isinstance(row, list) or len(row) != len(columns): raise ValueError('缺字段/结构不符')
                for col, raw in zip(columns, row): typed(m['engine'], col, raw)
                data = canonical(row); guard.row(data); h.update(data+b'\n'); n += 1
            if n != block['rows']: raise ValueError('块计数不符')
            count += n
        if count != table['rows'] or count != table['expected_rows'] or h.hexdigest() != table['content_sha256']: raise ValueError('整表计数/摘要不符')
    return m


def _verify_file(root, descriptor, files, guard):
    name = descriptor['file']
    if Path(name).name != name or name in files: raise ValueError('封存路径逃逸/重复')
    files.add(name); path = root/name
    if path.is_symlink() or path.stat().st_size != descriptor['bytes'] or sha_file(path, guard) != descriptor['sha256']: raise ValueError('封存块大小/SHA不符')


def block_rows(root, block, limits):
    with (Path(root)/block['file']).open('rb') as f:
        while line := f.readline(limits.max_row_bytes+2):
            if len(line) > limits.max_row_bytes+1 or not line.endswith(b'\n'): raise ValueError('行字节预算/块末行不完整')
            row = json.loads(line)
            if canonical(row)+b'\n' != line: raise ValueError('冻结行不是v1规范编码')
            yield row


def pg_output(col):
    # bpchar::text会裁掉数据库保留的填充空格；使用类型output保留原值。
    return sql.SQL('pg_catalog.bpcharout({})::text' if pg_kind(col)=='bpchar' and not col['array'] else '{}::text').format(sql.Identifier(col['name']))


def _attachments(root, manifest, attachments, guard):
    if len(attachments) > guard.limits.max_tables: raise ValueError('原附件数超限')
    for index, item in enumerate(attachments):
        source = Path(item['path']).resolve()
        if not item.get('role') or 'source_version' not in item: raise ValueError('附件缺原用途/版本')
        before = sha_file(source,guard); name = f'attachment-{index}.original'; target = root/name
        size = source.stat().st_size
        if guard.original_bytes+guard.bytes+size > guard.limits.max_total_bytes: raise ValueError('原附件字节超限')
        with source.open('rb') as src, target.open('xb') as dst:
            while chunk := src.read(min(1024**2,guard.limits.batch_bytes)):
                guard.check(); dst.write(chunk)
                if dst.tell() > size: raise ValueError('附件复制期间变大')
            dst.flush(); os.fsync(dst.fileno())
        if target.stat().st_size != size or sha_file(target,guard) != before or sha_file(source,guard) != before: raise ValueError('附件复制期间变化')
        guard.original_bytes += size
        manifest['originals'].append({'file':name,'bytes':size,'sha256':before,'origin_uri':source.as_uri(),'original_name':source.name,
            'source_version':item['source_version'],'role':item['role'],'consistency':'independent_immutable_copy_not_database_transaction'})


def _retained_metadata(publication, availability):
    if not isinstance(publication, dict) or not publication:
        raise ValueError('retained-history须保留显式原publication描述或Unknown原因')
    if publication.get('status') == 'unknown' and not publication.get('reason'): raise ValueError('原publication Unknown须保留原因')
    if not isinstance(availability, dict) or availability.get('status') not in ('available', 'validation_failed', 'unavailable', 'unknown'):
        raise ValueError('retained-history须显式原可用性，不能默认可用')
    if availability['status'] != 'available' and not availability.get('reason'): raise ValueError('缺原隔离/Unknown原因')


def freeze_postgres_source(source, tables, destination, *, target_identity, publication, availability, references=(), attachments=(), limits=Limits()):
    """预绑定只读源；目标身份仅用于隔离，真正写入时还须验证目标连接。"""
    if not isinstance(source, PGSource) or not isinstance(target_identity, PGIdentity): raise ValueError('须显式绑定源及目标身份')
    if source.identity == target_identity: raise ValueError('源与目标数据库相同')
    _retained_metadata(publication, availability)
    try:
        return _freeze_postgres(source.connect, tables, destination, source_version=source.source_version, limits=limits, availability=availability, references=references, attachments=attachments, binding=source, publication=publication)
    except psycopg2.Error:
        raise ValueError('PG源读取/权限/只读快照失败，未完成封存') from None


def freeze_sqlite_source(source, destination, *, binding, publication, availability, references=(), attachments=(), limits=Limits()):
    if not isinstance(binding, SQLiteSource): raise ValueError('须显式SQLite原件绑定')
    _retained_metadata(publication, availability)
    return _freeze_sqlite(source, destination, source_version=binding.source_version, limits=limits, availability=availability, references=references, attachments=attachments, binding=binding, publication=publication)
