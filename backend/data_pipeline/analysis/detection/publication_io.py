"""Detection P1 的实际目录、资源关闭及固定快照读取。"""
from contextlib import contextmanager
from pathlib import Path
import json
import resource
import shutil
import sys
import tempfile
from data_pipeline.bgp.archive import admission as upstream
from data_pipeline.bgp.archive.validation import _finish_close
from data_pipeline.analysis.detection.store import connect_duckdb, literal, COLUMNS
from data_pipeline.analysis.detection.lake_integrity import STATE_COLUMNS
from data_pipeline.analysis.detection.qualification_store import DDL
from data_pipeline.analysis.detection.publication_codec import typed

TABLES = {'records': COLUMNS, 'state_entries': STATE_COLUMNS,
          'm3_entries': tuple(tuple(x.strip().split()) for x in DDL.split(','))}


def check(runtime, guard):
    runtime.validate_budgets(); runtime.validate_scope(); guard()
    runtime.validate_budgets(); runtime.validate_scope()
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)
    free = shutil.disk_usage(runtime.scratch_root).free
    usage = runtime.resource_usage; usage['checks'] += 1
    usage['peak_rss_bytes'] = max(usage['peak_rss_bytes'], rss)
    usage['min_free_bytes'] = free if usage['min_free_bytes'] is None else min(usage['min_free_bytes'], free)
    if free < runtime.min_free_bytes: raise ValueError('Detection最低空闲盘保护')
    if rss > runtime.max_rss_bytes: raise ValueError('Detection P1进程RSS预算超限')
    if sum(p.stat().st_size for p in runtime.scratch_root.rglob('*') if p.is_file()) > runtime.max_temp_bytes:
        raise ValueError('Detection P1临时空间预算超限')


@contextmanager
def connection(runtime, guard):
    check(runtime, guard)
    temp = tempfile.TemporaryDirectory(dir=runtime.scratch_root, prefix='detection-p1-')
    db = None; primary = None
    try:
        db = connect_duckdb()
        db.execute('SET memory_limit=' + literal(str(runtime.memory_bytes) + 'B'))
        db.execute('SET max_temp_directory_size=' + literal(str(runtime.max_temp_bytes) + 'B'))
        db.execute('SET temp_directory=' + literal(temp.name))
        db.execute('LOAD ducklake'); db.execute('LOAD postgres')
        versions = dict(db.execute('SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded').fetchall())
        if versions.get('ducklake') != '3f1b372' or versions.get('postgres_scanner') != 'b9fce43':
            raise ValueError('Detection扩展版本不符')
        db.execute('ATTACH ' + literal('ducklake:postgres:' + runtime.dsn) + ' AS lake (READ_ONLY)')
        yield db
    except BaseException as exc: primary = exc; raise
    finally:
        actions = [temp.cleanup]
        if db is not None: actions.append(db.close)
        _finish_close(actions, None if isinstance(primary, GeneratorExit) else primary)


def catalog(runtime, binding, guard):
    """仅目录及表定义，不扫描正文。目录完整内容进入可信审计摘要。"""
    run, snapshot = binding['run_id'], binding['snapshot']
    with upstream._pg(runtime) as pg, pg.cursor() as c:
        physical = upstream._sql(runtime, c, 'SELECT system_identifier::text,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) FROM pg_control_system()').fetchone()
    # 固定快照的实际PG目录行也须绑定；不能仅凭文件名/大小漏掉column ID或映射变化。
    catalog_rows = {}
    with upstream._pg(runtime) as pg, pg.cursor() as c:
        visible = 'begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s)'
        def selected_rows(name, predicate, args):
            values = upstream._sql(runtime,c,f'SELECT to_jsonb(t) FROM public.{name} t WHERE {predicate} AND {visible}',(*args,snapshot,snapshot)).fetchall()
            # 已选行的未来结束对该固定快照等价于尚未结束；不可忽略可见性筛选。
            rows = [dict(r[0], end_snapshot=None) for r in values]
            return sorted(rows,key=lambda r:json.dumps(r,sort_keys=True))
        catalog_rows['ducklake_schema'] = selected_rows('ducklake_schema','schema_name=%s',('det_' + run,))
        schema_ids = [r['schema_id'] for r in catalog_rows['ducklake_schema']]
        catalog_rows['ducklake_table'] = selected_rows('ducklake_table','schema_id=ANY(%s) AND table_name=ANY(%s)',(schema_ids,list(TABLES)))
        table_ids = [r['table_id'] for r in catalog_rows['ducklake_table']]
        inline = upstream._sql(runtime,c,'''SELECT i.table_id FROM public.ducklake_inlined_data_tables i
            JOIN public.ducklake_snapshot s ON s.snapshot_id=%s
            WHERE i.table_id=ANY(%s) AND i.schema_version<=s.schema_version''',(snapshot,table_ids)).fetchall()
        if inline: raise ValueError('Detection P1仅接受Parquet正文，不接受PG inlining')
        for name in ('ducklake_column','ducklake_data_file'):
            catalog_rows[name] = selected_rows(name,'table_id=ANY(%s)',(table_ids,))
        file_ids = [r['data_file_id'] for r in catalog_rows['ducklake_data_file']]
        catalog_rows['ducklake_delete_file'] = selected_rows('ducklake_delete_file','table_id=ANY(%s) AND data_file_id=ANY(%s)',(table_ids,file_ids))
        # mapping 没有生命周期列，只能按可见文件引用判断；未来文件的映射不能影响历史版本。
        if any(r.get('mapping_id') is not None for r in catalog_rows['ducklake_data_file']): raise ValueError('Detection P1不接受column mapping制品')
    files = []; schemas = {}
    with connection(runtime, guard) as db:
        runtime.event('catalog_read')
        options = db.execute("SELECT option_name,value,scope FROM ducklake_options('lake') WHERE option_name IN ('data_path','version') AND scope='GLOBAL' ORDER BY option_name,scope").fetchall()
        roots = [r[1] for r in options if r[0] == 'data_path' and r[2] == 'GLOBAL']
        if len(roots) != 1 or not any(r[0] == 'version' and r[1] == '0.3' for r in options):
            raise ValueError('Detection湖目录版本/根不符')
        root = str(runtime.path(roots[0].rstrip('/')))
        for table, columns in TABLES.items():
            check(runtime, guard)
            actual = db.execute(f'DESCRIBE SELECT * FROM lake.det_{run}.{table} AT (VERSION => {snapshot})').fetchall()
            schema = [(r[0], r[1].replace('TIMESTAMP WITH TIME ZONE', 'TIMESTAMPTZ')) for r in actual]
            if schema != list(columns): raise ValueError('Detection完整schema不符')
            schemas[table] = schema
            for path, size, deleted in db.execute('SELECT data_file,data_file_size_bytes,delete_file FROM ducklake_list_files(?, ?, schema => ?, snapshot_version => ?)', ['lake', table, 'det_' + run, snapshot]).fetchall():
                if deleted is not None: raise ValueError('Detection固定制品不允许delete文件')
                runtime.separate_scratch(Path(path).parent)
                path = str(runtime.path(path))
                if not runtime.fixture_only and runtime.output_root not in Path(path).parents:
                    raise ValueError('Detection正文超出显式输出范围')
                if Path(path).stat().st_size != size: raise ValueError('目录实体大小不符')
                files.append(dict(table=table, path=path, size=size))
    if len({f['path'] for f in files}) != len(files): raise ValueError('Detection文件重复归属')
    return dict(system_identifier=physical[0], database_oid=physical[1], catalog='lake', schema='det_' + run, root=root, snapshot=snapshot), dict(catalog_rows=catalog_rows, options=options, schemas=schemas, files=sorted(files, key=lambda f: f['path']))


def rows(runtime, binding, request, scope, guard):
    view = request['view']
    if view in ('result_revisions','result_coverage'):
        yield from result_rows(runtime,binding,request,scope,guard)
        return
    table = {'revisions': 'records', 'decisions': 'records', 'qualified_revisions': 'records'}.get(view, view)
    order = 'sequence' if table == 'records' else 'ordinal'
    source = f"(SELECT * FROM lake.det_{binding['run_id']}.{table} AT (VERSION => {binding['snapshot']}))"
    where = [f'r.{order} >= ?']; args = [scope['start']]
    if scope['stop'] is not None: where.append(f'r.{order} < ?'); args.append(scope['stop'])
    if view in ('revisions', 'qualified_revisions'): where.append("r.record_kind='business_revision'")
    if view == 'decisions': where.append("r.record_kind='rule_decision'")
    filter_column = 'family' if table == 'state_entries' else 'incident_id'
    if scope['key'] is not None: where.append(f'r.{filter_column}=?'); args.append(scope['key'])
    with connection(runtime, guard) as db:
        if view == 'qualified_revisions':
            qsource = f"lake.det_{binding['run_id']}.m3_entries AT (VERSION => {binding['snapshot']})"
            position = ','.join(f"CAST(json_extract(payload_json,'$.effective_position[{i}]') AS BIGINT)" for i in range(4))
            query = f'''WITH q AS (SELECT *,row_number() OVER (PARTITION BY incident_id,revision ORDER BY ordinal DESC) AS n
                FROM {qsource} WHERE kind='event_qualification' AND [{position}] <= ?)
                SELECT r.*,q.entry_id AS qualification_id,q.payload_json AS qualification_json FROM {source} r
                JOIN q ON q.incident_id=r.incident_id AND q.revision=r.revision AND q.n=1
                WHERE {' AND '.join(where)} ORDER BY r.{order}'''
            args = [scope['at_position'], *args]
        else: query = f"SELECT r.* FROM {source} r WHERE {' AND '.join(where)} ORDER BY r.{order}"
        runtime.event('body_query', view=view, tables=2 if view == 'qualified_revisions' else 1)
        batches = db.execute(query, args).fetch_record_batch(request['batch_rows']); primary = None
        try:
            for batch in batches:
                check(runtime, guard)
                runtime.event('decoded_batch', rows=batch.num_rows, arrow_bytes=batch.nbytes)
                for row in batch.to_pylist():
                    check(runtime, guard)
                    if view == 'qualified_revisions':
                        q = json.loads(row.pop('qualification_json')); q['qualification_id'] = row.pop('qualification_id')
                        row = dict(raw=row, main=row if q['coverage'] == 'complete' else None, qualification=q, as_of_position=scope['at_position'])
                    yield row
        except BaseException as exc: primary = exc; raise
        finally: _finish_close([batches.close], None if isinstance(primary, GeneratorExit) else primary)


def result_rows(runtime,binding,request,scope,guard):
    from data_pipeline.analysis.detection.result_window import identity_windows
    from data_pipeline.analysis.detection.result_selection import query, selection
    window=identity_windows(binding['identity'],binding['scope'])
    if request['view']=='result_coverage':
        raw_request=dict(request,view='m3_entries')
        # 原资格全流中只选择来源Coverage；零事件也有真实来源，不造incident。
        source=rows(runtime,binding,raw_request,scope,guard);primary=None
        try:
            for row in source:
                if row['kind']=='source_coverage':
                    yield dict(raw=row,windows=window,window_coverage=binding['identity']['window_coverage'],
                               earlier_history='Unknown',business_absence='Unknown')
        except BaseException as exc:primary=exc;raise
        finally:_finish_close([source.close],None if isinstance(primary,GeneratorExit) else primary)
        return
    with connection(runtime,guard) as db:
        sql,args=query(binding,scope);runtime.event('body_query',view='result_revisions',tables=3)
        batches=db.execute(sql,args).fetch_record_batch(request['batch_rows']);primary=None
        try:
            for batch in batches:
                check(runtime,guard);runtime.event('decoded_batch',rows=batch.num_rows,arrow_bytes=batch.nbytes)
                for row in batch.to_pylist():
                    check(runtime,guard);value=selection(row,binding)
                    if value is not None:yield value
        except BaseException as exc:primary=exc;raise
        finally:_finish_close([batches.close],None if isinstance(primary,GeneratorExit) else primary)
