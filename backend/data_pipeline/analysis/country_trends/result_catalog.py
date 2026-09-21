"""Trend目录v2：服务端稳定全序游标，单项编码、增量摘要；不扫科学正文。"""
import hashlib
from contextlib import ExitStack
from data_pipeline.bgp.archive import admission as pg_api
from data_pipeline.results.manifest_io import encode as json_text
from data_pipeline.analysis.country_trends.stream_schema import TABLES, encode

FORMAT = 'country-trend-catalog-digest/v2'
ORDER = 'postgres-jsonb-text-C/v1'


def catalog(runtime, owner_binding, guard):
    b, proof = owner_binding['binding'], owner_binding['proof']
    streams = {}; count = size = serial = 0
    with pg_api._pg(runtime) as pg, ExitStack() as pending:
        def records(name, sql, args=()):
            nonlocal count, size, serial
            serial += 1
            # 命名游标避免客户端execute先收集整件结果。只搬运一个有界目录项。
            with pg.cursor(name='trend_catalog_' + str(serial), scrollable=False, withhold=False) as cur:
                pg_api._sql(runtime, cur, sql, args)
                h = hashlib.sha256(); n = byte_count = 0
                while True:
                    guard(); batch = cur.fetchmany(1)
                    if not batch: break
                    value, = batch[0]
                    payload = encode(value).encode()
                    if len(payload) > min(runtime.limits.max_row_bytes, runtime.limits.max_context_bytes):
                        raise ValueError('trend_catalog_item_budget')
                    h.update(len(payload).to_bytes(8,'big')); h.update(payload)
                    n += 1; byte_count += len(payload)
                    yield value
                streams[name] = dict(rows=n, bytes=byte_count, sha256=h.hexdigest())
                count += n; size += byte_count
        def query(name, sql, args=()):
            stream = records(name, sql, args)
            pending.callback(stream.close)
            return stream
        def one(name, sql, args=()):
            it = query(name, sql, args)
            try:
                first = next(it, None)
                if first is None or next(it, None) is not None:
                    raise ValueError('trend_catalog_singleton')
                return first
            finally: it.close()
        def selected(name, table, predicate, args):
            doc = "(to_jsonb(t)||jsonb_build_object('end_snapshot',NULL))"
            sql = f'SELECT {doc} FROM public.{table} t WHERE {predicate} AND begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s) ORDER BY {doc}::text COLLATE "C"'
            return query(name, sql, (*args,b['snapshot'],b['snapshot']))
        physical = one('physical', "SELECT jsonb_build_array(system_identifier::text,(SELECT oid::bigint FROM pg_database WHERE datname=current_database())) FROM pg_control_system()")
        if physical != [b['system_id'],b['database_oid']]: raise ValueError('trend_catalog_physical')
        if one('identity','SELECT to_jsonb(catalog_id) FROM country_trends.catalog WHERE singleton') != b['catalog_id']:
            raise ValueError('trend_catalog_identity')
        component = one('component', 'SELECT jsonb_build_array(state,root,schema_name,binding,proof) FROM country_trends.components WHERE component_id=%s', (b['component_id'],))
        if component != ['complete',b['root'],b['schema_name'],json_text(b),json_text(proof)]:
            raise ValueError('trend_catalog_control')
        for _ in query('metadata', "SELECT to_jsonb(t) FROM (SELECT key,value,scope,scope_id FROM public.ducklake_metadata WHERE key IN ('data_path','version')) t ORDER BY to_jsonb(t)::text COLLATE \"C\""): pass
        schema_id = None
        for item in selected('schema','ducklake_schema','schema_name=%s',(b['schema_name'],)):
            if schema_id is not None: raise ValueError('trend_catalog_schema')
            schema_id = item['schema_id']
        if schema_id is None: raise ValueError('trend_catalog_schema')
        ids = []; names = set()
        for item in selected('tables','ducklake_table','schema_id=%s',(schema_id,)):
            name = item['table_name']
            if name not in TABLES or name in names: raise ValueError('trend_catalog_tables')
            names.add(name); ids.append(item['table_id'])
        if names != set(TABLES): raise ValueError('trend_catalog_tables')
        for _ in query('inline','SELECT to_jsonb(i.table_id) FROM public.ducklake_inlined_data_tables i JOIN public.ducklake_snapshot s ON s.snapshot_id=%s WHERE i.table_id=ANY(%s) AND i.schema_version<=s.schema_version ORDER BY i.table_id',(b['snapshot'],ids)):
            raise ValueError('trend_catalog_inline')
        for name in ('ducklake_column','ducklake_data_file','ducklake_delete_file'):
            for _ in selected(name,name,'table_id=ANY(%s)',(ids,)):
                if name == 'ducklake_delete_file': raise ValueError('trend_catalog_delete')
        # 引用集合留在服务端；不把mapping_id收成Python全量列表。
        referenced = 'SELECT DISTINCT mapping_id FROM public.ducklake_data_file WHERE table_id=ANY(%s) AND begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s) AND mapping_id IS NOT NULL'
        args = (ids,b['snapshot'],b['snapshot'])
        for _ in query('missing_mapping',f'SELECT to_jsonb(r.mapping_id) FROM ({referenced}) r LEFT JOIN public.ducklake_column_mapping m USING(mapping_id) WHERE m.mapping_id IS NULL ORDER BY r.mapping_id',args):
            raise ValueError('trend_catalog_column_mapping')
        for item in query('column_mapping',f'SELECT to_jsonb(t) FROM public.ducklake_column_mapping t WHERE mapping_id IN ({referenced}) ORDER BY to_jsonb(t)::text COLLATE "C"',args):
            if item['table_id'] not in ids or item['type'] != 'map_by_name':
                raise ValueError('trend_catalog_column_mapping')
        for _ in query('name_mapping',f'SELECT to_jsonb(t) FROM public.ducklake_name_mapping t WHERE mapping_id IN ({referenced}) ORDER BY to_jsonb(t)::text COLLATE "C"',args): pass
    runtime.event('trend_catalog_metadata', rows=count, bytes=size, body_scans=0, locks=0)
    return dict(version=FORMAT, ordering=ORDER, encoding='country-trend-typed/v2-length64be',
                streams=streams, rows=count, bytes=size)
