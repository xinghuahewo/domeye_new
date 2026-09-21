"""Q3-A附加人工检查：源删除/移开后逐表重建；与业务发布隔离。"""
from dataclasses import asdict
import hashlib
from pathlib import Path
from psycopg2 import sql
from tests.historical.test_historical_import_q3 import artificial, collect
from data_pipeline.history.database_import.freeze import private_pg, write_json


def test_deleted_source_full_history_rebuild(artificial):
    a=artificial;h=a['history']
    tokens=[a['pg_token'],a['sqlite_token']]
    components=[h.component(t) for t in tokens]
    before=[[collect(a,t,index) for index,table in enumerate(c['original']['tables'])] for t,c in zip(tokens,components)]
    frozen_hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for t in tokens for p in (a['root']/'history'/t.import_id).rglob('*') if p.is_file()}
    pg=private_pg(a['dsn'],a['root'])
    with pg,pg.cursor() as c:c.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(a['source_schema'])))
    pg.close()
    for p in [a['pg_manifest'].parent,a['sqlite_manifest'].parent,a['source']]:p.rename(p.with_name(p.name+'.removed'))
    copies=[h.rebuild(t) for t in tokens]
    # NaN不自等；完整repr保留原类型、数值表示、嵌套顺序和重复occurrence。
    after=[[collect(a,t,index) for index,table in enumerate(c['original']['tables'])] for t,c in zip(tokens,components)]
    assert repr(after)==repr(before)
    pg=private_pg(a['dsn'],a['root'])
    try:
        with pg.cursor() as c:
            for setting in ("SET TimeZone='UTC'","SET IntervalStyle='iso_8601'","SET DateStyle='ISO,YMD'","SET extra_float_digits=3","SET bytea_output='hex'"):c.execute(setting)
            for index,(table,rows) in enumerate(zip(components[0]['original']['tables'],before[0])):
                cols=sql.SQL(',').join(sql.SQL('{}::text').format(sql.Identifier(col['name'])) for col in table['columns'])
                c.execute(sql.SQL('SELECT {} FROM {}.{} ORDER BY ctid').format(cols,sql.Identifier(copies[0]['schema']),sql.Identifier('t'+str(index))))
                assert c.fetchall()==[tuple(r['values']) for r in rows]
            for index,(table,rows) in enumerate(zip(components[1]['original']['tables'],before[1])):
                cols=sql.SQL(',').join(sql.SQL('({}).storage_class,({}).integer_value,({}).real_bits,({}).raw_bytes').format(*[sql.Identifier(col['name'])]*4) for col in table['columns'])
                c.execute(sql.SQL('SELECT {} FROM {}.{} ORDER BY ctid').format(cols,sql.Identifier(copies[1]['schema']),sql.Identifier('t'+str(index))))
                actual=[tuple(bytes(v) if isinstance(v,memoryview) else v for v in r) for r in c.fetchall()]
                expected=[tuple(value for col in table['columns'] for value in (r['typed'][col['name']]['storage_class'],r['typed'][col['name']]['integer'],r['typed'][col['name']]['real_bits'],r['typed'][col['name']]['bytes'])) for r in rows]
                assert actual==expected
    finally:pg.close()
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==sha for p,sha in frozen_hashes.items())
    write_json(a['evidence']/'源删除后完整重建.json',{'tokens':[asdict(t) for t in tokens],'copies':copies,'table_rows':[[len(r) for r in tables] for tables in before],'original_pg_schema_deleted':True,'original_files_moved':True,'all_raw_typed_order_occurrence_unchanged':True})
