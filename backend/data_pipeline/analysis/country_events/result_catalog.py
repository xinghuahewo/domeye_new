"""Country current只读实际PG目录；不取行锁、不读取业务正文或索引全文。"""
import json

from data_pipeline.bgp.archive import admission as upstream
from data_pipeline.analysis.country_events.qualified_schema import TABLES
from data_pipeline.analysis.country_events.selection_admission import _record
from data_pipeline.analysis.country_events.snapshot_schema import encode


def catalog(runtime, read, proof, guard):
    b=read.component; result={};count=size=0
    with upstream._pg(runtime) as pg,pg.cursor() as cur:
        def query(sql,args=()):
            nonlocal count,size
            guard();upstream._sql(runtime,cur,sql,args);rows=[]
            while batch:=cur.fetchmany(runtime.limits.batch_rows):
                for row in batch:
                    count+=1;size+=len(encode(row).encode())
                    if count>runtime.max_source_rows or size>runtime.max_source_bytes:
                        raise ValueError('resource_limit:Country_catalog_metadata')
                    rows.append(row)
            return rows
        physical=query('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')
        if physical!=[(b.system_id,b.database_oid)]:raise ValueError('Country当前物理库串绑')
        if query('SELECT catalog_id FROM country_components.catalog WHERE singleton')!=[(b.catalog_id,)]:
            raise ValueError('Country当前实际catalog身份不符')
        control=query('SELECT schema_name,state,snapshot,root,manifest_sha256,scope,availability FROM country_components.components WHERE component_id=%s',(b.component_id,))
        if len(control)!=1 or control[0][:5]!=(b.schema_name,'complete',b.snapshot,b.root,b.manifest_sha256):
            raise ValueError('Country当前实际组件绑定不符')
        result['component']=control
        if query('SELECT admission_version,component_id,binding_json,proof_json,manifest_sha256,validation_sha256,state FROM country_components.read_admissions WHERE read_model_id=%s',(read.read_model_id,))!=[_record(read,proof)]:
            raise ValueError('Country当前实际C4登记不符')
        result['metadata']=query("SELECT key,value,scope,scope_id FROM public.ducklake_metadata WHERE key IN ('data_path','version') ORDER BY key,scope,scope_id")
        def selected(table,predicate,args):
            rows=query(f'SELECT to_jsonb(t) FROM public.{table} t WHERE {predicate} AND begin_snapshot<=%s AND (end_snapshot IS NULL OR end_snapshot>%s)',(*args,b.snapshot,b.snapshot))
            return sorted((dict(r[0],end_snapshot=None) for r in rows),key=lambda r:json.dumps(r,sort_keys=True))
        result['schema']=selected('ducklake_schema','schema_name=%s',(b.schema_name,))
        schemas=[r['schema_id'] for r in result['schema']]
        result['tables']=selected('ducklake_table','schema_id=ANY(%s)',(schemas,))
        if len(schemas)!=1 or sorted(r['table_name'] for r in result['tables'])!=sorted(TABLES):
            raise ValueError('Country当前目录24表不完整')
        ids=[r['table_id'] for r in result['tables']]
        if query('SELECT i.table_id FROM public.ducklake_inlined_data_tables i JOIN public.ducklake_snapshot s ON s.snapshot_id=%s WHERE i.table_id=ANY(%s) AND i.schema_version<=s.schema_version',(b.snapshot,ids)):
            raise ValueError('Country当前目录不接受inline正文')
        for table in ('ducklake_column','ducklake_data_file','ducklake_delete_file'):
            result[table]=selected(table,'table_id=ANY(%s)',(ids,))
        if result['ducklake_delete_file']:
            raise ValueError('Country当前目录不接受delete')
        # 本项目Writer通过add_data_files登记原Parquet，产生实际map_by_name。
        # 首次全C3审计已验证逻辑列与原行；current固定完整映射目录而不扫正文。
        mapping_ids=sorted({r['mapping_id'] for r in result['ducklake_data_file'] if r.get('mapping_id') is not None})
        result['column_mapping']=query('SELECT to_jsonb(t) FROM public.ducklake_column_mapping t WHERE mapping_id=ANY(%s) ORDER BY mapping_id',(mapping_ids,))
        if (len(result['column_mapping'])!=len(mapping_ids)
                or any(r[0]['table_id'] not in ids or r[0]['type']!='map_by_name' for r in result['column_mapping'])):
            raise ValueError('Country当前目录column mapping串绑')
        result['name_mapping']=query('SELECT to_jsonb(t) FROM public.ducklake_name_mapping t WHERE mapping_id=ANY(%s) ORDER BY mapping_id,column_id',(mapping_ids,))
    runtime.event('country_catalog_metadata',rows=count,bytes=size,body_scans=0,locks=0)
    return result
