"""Q3-A：全部源为人工；集成检查必须显式绑定本任务私有UTF8 socket PG。"""
from collections import Counter
from dataclasses import asdict, replace
from decimal import Decimal
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import struct
import uuid

import pyarrow.parquet as pq
import pytest
from psycopg2 import sql

from data_pipeline.history.database_import import History, Limits, Token, freeze_postgres, freeze_sqlite, verify_package
from data_pipeline.history.database_import.codec import parse_array, scalar
from data_pipeline.history.database_import.freeze import private_pg, read_json, sha_file, write_json


def _source_sql(text):
    # SQL.format只替换schema；测试原值中的JSON/数组花括号保持字面含义。
    return sql.SQL(text.replace('{}','@SCHEMA@',1).replace('{','{{').replace('}','}}').replace('@SCHEMA@','{}'))


@pytest.mark.parametrize('raw,expected', [
    ('{}',([],[],[])),
    ('[0:1][3:4]={{"NULL",NULL},{"",null}}',([2,2],[0,3],['NULL',None,'','null'])),
    ('{"a,b","a\\"b","a\\\\b"}',([3],[1],['a,b','a"b','a\\b'])),
])
def test_independent_array_grammar(raw, expected):
    assert parse_array(raw) == expected


@pytest.mark.parametrize('kind,raw', [('int8','9223372036854775808'),('bool','yes'),('numeric','1e2'),('timestamp','2026-03-01 00:00:00.1234567'),('timestamptz','2026-03-01 00:00:00'),('bytea','hello'),('json','NaN')])
def test_explicit_type_rejections(kind,raw):
    with pytest.raises(ValueError): scalar(kind,raw)


def test_interval_independent_components():
    assert scalar('interval','P10M3DT-4H-5M-6.000007S') == {'months':10,'days':3,'micros':-14706000007}
    assert scalar('interval','P1M') != scalar('interval','P30D')


@pytest.fixture(scope='module')
def artificial():
    location = os.environ.get('Q3_PRIVATE_ROOT')
    if not location: pytest.skip('仅显式私有人工PG集成；不连接默认数据库')
    root = Path(location).resolve()
    dsn = f'host={root / "socket"} port=28763 dbname=postgres'
    evidence = root/('acceptance-'+uuid.uuid4().hex); evidence.mkdir()
    limits = Limits(batch_rows=2,batch_bytes=32768,max_row_bytes=8192)
    h = History(dsn,root,limits)
    pg = private_pg(dsn,root)
    source_schema = 'q3source_'+uuid.uuid4().hex
    huge = '12345678901234567890123456789012345678901234567890.1234567890123456789012345678901234567890'
    with pg,pg.cursor() as c:
        c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(source_schema)))
        c.execute(sql.SQL('''CREATE TABLE {}."Historic" (
            "ID" bigint, "A" text DEFAULT 'A', "a" text, bounded numeric(12,4), huge numeric(90,40), arbitrary numeric,
            span interval, local_time timestamp(6), instant timestamptz(6), day date,
            numbers bigint[], words text[], blob bytea, flag boolean, f double precision,
            j json, jb jsonb, empty_text text, sql_null text,
            state integer, judge boolean, notify boolean, event_info text, detail_url text,
            pre_vp_paths text, eve_vp_paths text, next_vp_paths text, original_ratio numeric)
            ''').format(sql.Identifier(source_schema)))
        c.execute(_source_sql('''INSERT INTO {}."Historic" VALUES
            (9007199254740993,'same','lower',12345678.1250,%s,9007199254740993,
             '10 mons 3 days -04:05:06.000007','2026-03-01 12:34:56.123456','2026-03-01 12:34:56.123456+08','2026-03-01',
             '[0:1][3:4]={{9007199254740993,NULL},{-2,0}}','[0:3]={"NULL",NULL,"","a,b"}',decode('00ff01','hex'),true,'Infinity',
             '  {"x": 1, "x": 2}  ','null','',NULL,2,false,true,'{"path":"{328405}"}','old:ambiguous',
             '["{328405}"]','null','[]',0.125)
            ''').format(sql.Identifier(source_schema)), (huge,))
        c.execute(sql.SQL('INSERT INTO {}."Historic" SELECT * FROM {}."Historic"').format(sql.Identifier(source_schema),sql.Identifier(source_schema)))
        c.execute(_source_sql('''INSERT INTO {}."Historic" ("ID",bounded,huge,arbitrary,span,local_time,instant,numbers,words,flag,f,j,jb,empty_text,sql_null)
            VALUES (0,'NaN','NaN','NaN','1 mon','2026-03-01 00:00:00.000001','2026-03-01 00:00:00.000001+00','{}','{}',false,'NaN','null','{"b":2,"a":1}','null','null'),
                   (-1,0,0,'Infinity','-1 mon -2 days 00:00:00.000001','infinity','-infinity','{NULL}','{NULL}',NULL,'-Infinity',NULL,NULL,NULL,'')
            ''').format(sql.Identifier(source_schema)))
        c.execute(sql.SQL('CREATE TABLE {}."Keyed" ("Key" INTEGER PRIMARY KEY, "Value" TEXT NOT NULL DEFAULT \'untouched\')').format(sql.Identifier(source_schema)))
        c.execute(sql.SQL('INSERT INTO {}."Keyed" VALUES (7,\'人工\')').format(sql.Identifier(source_schema)))
        c.execute(sql.SQL('CREATE TABLE {}."Empty" (only_col NUMERIC(8,2))').format(sql.Identifier(source_schema)))
        # 独立SQL预期：不用导入codec。明确设置规范输出，以所有字段的多重集合核验。
        for setting in ("SET TimeZone='UTC'","SET IntervalStyle='iso_8601'","SET DateStyle='ISO,YMD'","SET extra_float_digits=3","SET bytea_output='hex'"): c.execute(setting)
        c.execute(sql.SQL('SELECT * FROM {}."Historic" LIMIT 0').format(sql.Identifier(source_schema)))
        names = [d.name for d in c.description]
        c.execute(sql.SQL('SELECT {} FROM {}."Historic" ORDER BY ctid').format(sql.SQL(',').join(sql.SQL('{}::text').format(sql.Identifier(n)) for n in names),sql.Identifier(source_schema)))
        expected = c.fetchmany(10)
        c.execute(sql.SQL('SELECT extract(year FROM span)*12+extract(month FROM span),extract(day FROM span),extract(hour FROM span)*3600000000+extract(minute FROM span)*60000000+extract(second FROM span)*1000000,array_dims(numbers),array_lower(words,1) FROM {}."Historic" WHERE "ID"=9007199254740993 LIMIT 1').format(sql.Identifier(source_schema)))
        assert c.fetchone() == (Decimal(10),Decimal(3),Decimal(-14706000007),'[0:1][3:4]',0)
    pg.close()
    refs = [{'original_ref':'old:ambiguous','status':'ambiguous_reference','targets':[{'table_index':0,'ordinal':0},{'table_index':0,'ordinal':1}]},
            {'original_ref':'old:missing','status':'not_retained','targets':[]},
            {'original_ref':'old:keyed','status':'resolved','targets':[{'table_index':1,'ordinal':0}]}]
    pg_manifest = freeze_postgres(dsn,root,[(source_schema,'Historic'),(source_schema,'Keyed'),(source_schema,'Empty')],evidence/'pg-freeze',source_version='artificial-pg-v1',limits=limits,references=refs)
    pg_token = h.import_package(pg_manifest)
    source = evidence/'人工.sqlite'
    db = sqlite3.connect(source)
    db.execute('CREATE TABLE "Mixed" ("ID" INTEGER, "Value" NUMERIC, "Raw" TEXT DEFAULT \'null\')')
    source_values = [(1,9007199254740993,'null'),(2,1.25,''),(3,'not-json','[]'),(4,b'\x00\xff',None),(5,None,'null'),(1,9007199254740993,'null')]
    db.executemany('INSERT INTO "Mixed" VALUES (?,?,?)',source_values)
    db.execute('CREATE TABLE "Pk" (k INTEGER PRIMARY KEY, text_value TEXT NOT NULL DEFAULT \'默认\')');db.execute("INSERT INTO Pk VALUES (10,'值')")
    db.commit()
    # 独立SQLite原值与storage class预期，未调用编码器。
    expected_sqlite = db.execute('SELECT ID,typeof(Value),Value,typeof(Raw),Raw FROM Mixed ORDER BY rowid').fetchmany(20)
    db.close()
    sqlite_manifest = freeze_sqlite(source,evidence/'sqlite-freeze',source_version='artificial-sqlite-v1',limits=limits)
    sqlite_token = h.import_package(sqlite_manifest)
    state = dict(root=root,evidence=evidence,dsn=dsn,history=h,limits=limits,source_schema=source_schema,
                 pg_manifest=pg_manifest,pg_token=pg_token,sqlite_manifest=sqlite_manifest,sqlite_token=sqlite_token,
                 expected=expected,expected_sqlite=expected_sqlite,names=names,huge=huge,source=source)
    yield state
    write_json(evidence/'交付回执.json', {'pg_token':asdict(pg_token),'sqlite_token':asdict(sqlite_token),'source_schema':source_schema,
        'independent_pg_expected':[list(r) for r in expected],'independent_sqlite_expected':repr(expected_sqlite),
        'pg_component':h.component(pg_token),'sqlite_component':h.component(sqlite_token)})
    print('Q3_ARTIFICIAL_EVIDENCE='+str(evidence))


def collect(a,token,table=0,purpose='business'):
    h=a['history']; count=h.component(token)['original']['tables'][table]['rows']; result=[]
    for offset in range(0,count,2): result.extend(h.scan(token,table,after_ordinal=offset-1,limit=2,purpose=purpose))
    return result


def test_pg_all_fields_multiset_and_independent_typed_values(artificial):
    a=artificial; rows=collect(a,a['pg_token'])
    assert Counter(tuple(r['values']) for r in rows) == Counter(a['expected'])
    assert len(rows)==4 and rows[0]['content_sha256']==rows[1]['content_sha256']
    assert rows[0]['occurrence'] != rows[1]['occurrence']
    r=rows[0]['typed']
    assert r['ID']['value']==9007199254740993
    assert r['bounded']['value']=={'state':'finite','finite':Decimal('12345678.1250')}
    assert r['huge']['value']==a['huge']
    assert r['span']['value']=={'months':10,'days':3,'micros':-14706000007}
    assert r['local_time']['value']['finite']==dt.datetime(2026,3,1,12,34,56,123456)
    assert r['instant']['value']['finite']==dt.datetime(2026,3,1,4,34,56,123456,tzinfo=dt.timezone.utc)
    assert r['numbers']['value']=={'dimensions':[2,2],'lower_bounds':[0,3],'items':[9007199254740993,None,-2,0]}
    assert r['words']['value']=={'dimensions':[4],'lower_bounds':[0],'items':['NULL',None,'','a,b']}
    assert r['blob']['value']==b'\x00\xff\x01' and r['flag']['value'] is True
    assert r['j']['raw']=='  {"x": 1, "x": 2}  '
    assert r['jb']['raw']=='null' and r['sql_null'] is None and r['empty_text']['raw']==''
    assert r['eve_vp_paths']['value']=='null' and r['pre_vp_paths']['value']=='["{328405}"]'
    assert rows[2]['typed']['bounded']['value']=={'state':'NaN','finite':None}
    assert rows[2]['typed']['numbers']['value']=={'dimensions':[],'lower_bounds':[],'items':[]}
    assert rows[3]['typed']['local_time']['value']=={'state':'infinity','finite':None}


def test_sqlite_storage_classes_and_duplicates(artificial):
    a=artificial; rows=collect(a,a['sqlite_token']); expected=a['expected_sqlite']
    actual=[]
    for row in rows:
        cells=row['typed']; v=cells['Value']; text=cells['Raw']
        value=v['integer'] if v['storage_class']=='integer' else v['real'] if v['storage_class']=='real' else v['bytes'].decode() if v['storage_class']=='text' else v['bytes']
        raw=None if text['storage_class']=='null' else text['bytes'].decode()
        actual.append((cells['ID']['integer'],v['storage_class'],value,text['storage_class'],raw))
    assert Counter(actual)==Counter(expected)
    assert rows[1]['typed']['Value']['real_bits']==bytes.fromhex('3ff4000000000000')
    component=a['history'].component(a['sqlite_token']); table=component['original']['tables'][0]
    assert table['identity']['kind']=='no_primary_key' and len(table['blocks'])==3
    assert table['columns'][1]['affinity']=='NUMERIC' and table['columns'][1]['declared_type']=='NUMERIC'
    assert component['original']['tables'][1]['columns'][0]['pk_position']==1


def test_actual_parquet_native_types_and_metadata(artificial):
    a=artificial; c=a['history'].component(a['pg_token']); file=c['binding']['files'][0]['path']
    schema=pq.read_schema(file)
    assert 'int64' in str(schema.field('c0').type)
    assert 'decimal128(12, 4)' in str(schema.field('c3').type)
    assert 'timestamp[us]' in str(schema.field('c7').type)
    assert 'timestamp[us, tz=UTC]' in str(schema.field('c8').type)
    assert 'binary' in str(schema.field('c12').type)
    original=c['original']; cols=original['tables'][0]['columns']
    assert [col['name'] for col in cols]==a['names'] and cols[1]['default']=="'A'::text"
    assert cols[4]['formatted_type']=='numeric(90,40)' and cols[7]['typmod']==6
    assert original['tables'][1]['identity']['kind']=='primary_key'
    assert original['export']['read_only'] is True and original['export']['settings']['TimeZone']=='UTC'


def test_repeat_same_completed_freeze_does_not_append(artificial):
    a=artificial; h=a['history']; token=h.import_package(a['pg_manifest'])
    assert token==a['pg_token']; assert len(collect(a,token))==4


def test_fixed_old_version_after_new_batch(artificial):
    a=artificial; h=a['history']; before=collect(a,a['sqlite_token'])
    path=freeze_sqlite(a['source'],a['evidence']/'second-freeze',source_version='artificial-sqlite-v2',limits=a['limits'])
    new=h.import_package(path)
    assert new!=a['sqlite_token']; assert collect(a,a['sqlite_token'])==before


def test_refs_missing_ambiguous_and_resolved(artificial):
    a=artificial; h=a['history']; t=a['pg_token']
    assert h.resolve_reference(t,'old:ambiguous')['status']=='ambiguous_reference'
    assert h.resolve_reference(t,'old:missing')['status']=='not_retained'
    assert h.resolve_reference(t,'absent')['status']=='not_retained'
    assert h.resolve_reference(t,'old:keyed')['targets']==[{'table_index':1,'ordinal':0}]


def test_rebuild_without_original_files_or_source_reads(artificial,monkeypatch):
    a=artificial; h=a['history']; originals=[a['pg_manifest'].parent,a['sqlite_manifest'].parent,a['source']]
    for p in originals: p.rename(p.with_name(p.name+'.held'))
    try:
        import data_pipeline.history.database_import.freeze as freeze
        monkeypatch.setattr(freeze,'freeze_sqlite',lambda *x,**y:pytest.fail('重建不许读原件'))
        copies=[h.rebuild(a['pg_token']),h.rebuild(a['sqlite_token'])]
    finally:
        for p in originals: p.with_name(p.name+'.held').rename(p)
    pg=private_pg(a['dsn'],a['root'])
    try:
        with pg,pg.cursor() as c:
            for setting in ("SET TimeZone='UTC'","SET IntervalStyle='iso_8601'","SET DateStyle='ISO,YMD'","SET extra_float_digits=3","SET bytea_output='hex'"): c.execute(setting)
            c.execute(sql.SQL('SELECT {} FROM {}.t0').format(sql.SQL(',').join(sql.SQL('{}::text').format(sql.Identifier(n)) for n in a['names']),sql.Identifier(copies[0]['schema'])))
            assert Counter(c.fetchmany(10))==Counter(a['expected'])
            c.execute(sql.SQL('SELECT ("ID").integer_value,("Value").storage_class,("Value").integer_value,encode(("Value").real_bits,\'hex\'),encode(("Value").raw_bytes,\'hex\'),("Raw").storage_class,encode(("Raw").raw_bytes,\'hex\') FROM {}.t0').format(sql.Identifier(copies[1]['schema'])))
            expected=[(1,'integer',9007199254740993,None,None,'text','6e756c6c'),(2,'real',None,'3ff4000000000000',None,'text',''),(3,'text',None,None,'6e6f742d6a736f6e','text','5b5d'),(4,'blob',None,None,'00ff','null',None),(5,'null',None,None,None,'text','6e756c6c'),(1,'integer',9007199254740993,None,None,'text','6e756c6c')]
            assert Counter(c.fetchmany(10))==Counter(expected)
    finally: pg.close()
    write_json(a['evidence']/'重建副本.json',copies)


def resign(path,m):
    write_json(path/'manifest.json',m)
    write_json(path/'COMPLETE.json',{'manifest_sha256':sha_file(path/'manifest.json'),'source_freeze_id':m['source_freeze_id']})


@pytest.mark.parametrize('damage',['missing_block','bad_sha','wrong_type','missing_field','same_identity_different_content','incomplete'])
def test_broken_freezes_never_complete(artificial,damage):
    a=artificial; path=a['evidence']/('broken-'+damage); shutil.copytree(a['pg_manifest'].parent,path)
    m=read_json(path/'manifest.json',a['limits'].max_metadata_bytes)
    if damage not in ('same_identity_different_content',): m['source_freeze_id']=uuid.uuid4().hex
    if damage=='missing_block': (path/m['tables'][0]['blocks'][0]['file']).unlink()
    elif damage=='bad_sha': m['tables'][0]['blocks'][0]['sha256']='0'*64
    elif damage in ('wrong_type','missing_field'):
        b=m['tables'][0]['blocks'][0]; file=path/b['file']; lines=file.read_text().splitlines(); row=json.loads(lines[0])
        if damage=='wrong_type': row[0]='not-an-integer'
        else: row.pop()
        lines[0]=json.dumps(row); file.write_text('\n'.join(lines)+'\n'); b['sha256']=sha_file(file); b['bytes']=file.stat().st_size
    elif damage=='same_identity_different_content': m['source']['source_version']='different'
    resign(path,m)
    if damage=='incomplete': (path/'COMPLETE.json').unlink()
    with pytest.raises((ValueError,FileNotFoundError)): a['history'].import_package(path/'manifest.json')
    pg=private_pg(a['dsn'],a['root'])
    try:
        with pg.cursor() as c:
            c.execute('SELECT state,manifest_sha256 FROM history_q3.imports WHERE source_freeze_id=%s',(m['source_freeze_id'],)); result=c.fetchone()
            assert result is None or damage=='same_identity_different_content' and result==('complete',a['pg_token'].manifest_sha256)
    finally: pg.close()


def test_midway_failure_not_complete_or_readable(artificial,monkeypatch):
    a=artificial; path=freeze_sqlite(a['source'],a['evidence']/'midway-freeze',source_version='failure',limits=a['limits'])
    def fail(*args,**kwargs): raise RuntimeError('人工固定回读阶段故障')
    monkeypatch.setattr(a['history'],'_validate',fail)
    with pytest.raises(RuntimeError): a['history'].import_package(path)
    m=read_json(path,a['limits'].max_metadata_bytes); pg=private_pg(a['dsn'],a['root'])
    try:
        with pg.cursor() as c:
            c.execute('SELECT state,import_id FROM history_q3.imports WHERE source_freeze_id=%s',(m['source_freeze_id'],)); state,identity=c.fetchone()
            assert state=='failed'
            with pytest.raises(ValueError): a['history'].component(Token(identity,m['source_freeze_id'],sha_file(path),1,'0'*64))
    finally: pg.close()
    with pytest.raises(ValueError): a['history'].import_package(path)


def test_core_quarantine_no_detail_bypass(artificial):
    a=artificial; path=freeze_sqlite(a['source'],a['evidence']/'quarantine-freeze',source_version='人工隔离日2026-03-03',limits=a['limits'],availability={'status':'validation_failed','reason':'人工源校验失败','date':'2026-03-03'},references=[{'original_ref':'detail:1','status':'resolved','targets':[{'table_index':0,'ordinal':0}]}])
    token=a['history'].import_package(path)
    with pytest.raises(ValueError): list(a['history'].scan(token,0,limit=1))
    assert a['history'].resolve_reference(token,'detail:1')['status']=='unavailable'
    assert len(list(a['history'].scan(token,0,limit=1,purpose='audit')))==1


def test_resource_limits_and_no_total_deadline(artificial):
    a=artificial
    with pytest.raises(ValueError): freeze_sqlite(a['source'],a['evidence']/'row-limit',source_version='budget',limits=replace(a['limits'],max_total_rows=1))
    assert not (a['evidence']/'row-limit'/'COMPLETE.json').exists()
    with pytest.raises(ValueError): list(a['history'].scan(a['pg_token'],0,limit=3))
    assert 'max_seconds' not in asdict(a['limits'])


def test_one_snapshot_across_tables_during_concurrent_write(artificial,monkeypatch):
    a=artificial; ns='snapshot_'+uuid.uuid4().hex; db=private_pg(a['dsn'],a['root'])
    with db,db.cursor() as c:
        c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(ns)))
        for name in ('a','b'):
            c.execute(sql.SQL('CREATE TABLE {}.{} (v INTEGER)').format(sql.Identifier(ns),sql.Identifier(name)))
            c.execute(sql.SQL('INSERT INTO {}.{} VALUES (1)').format(sql.Identifier(ns),sql.Identifier(name)))
    db.close()
    import data_pipeline.history.database_import.freeze as f
    original=f._write_table
    def between(root,m,table,rows,expected,guard):
        original(root,m,table,rows,expected,guard)
        if table['name']=='a':
            writer=private_pg(a['dsn'],a['root'])
            with writer,writer.cursor() as c: c.execute(sql.SQL('INSERT INTO {}.b VALUES (2)').format(sql.Identifier(ns)))
            writer.close()
    monkeypatch.setattr(f,'_write_table',between)
    path=freeze_postgres(a['dsn'],a['root'],[(ns,'a'),(ns,'b')],a['evidence']/'snapshot-freeze',source_version='snapshot-test',limits=a['limits'])
    m=verify_package(path.parent,a['limits'])
    assert [t['rows'] for t in m['tables']]==[1,1]
    db=private_pg(a['dsn'],a['root'])
    with db.cursor() as c:
        c.execute(sql.SQL('SELECT count(*) FROM {}.b').format(sql.Identifier(ns)));assert c.fetchone()[0]==2
    db.close()


def test_native_float4_and_fixed_char_keep_real_values(artificial):
    a=artificial; ns='native_'+uuid.uuid4().hex; db=private_pg(a['dsn'],a['root'])
    with db,db.cursor() as c:
        c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(ns)))
        c.execute(sql.SQL('CREATE TABLE {}.values (i SMALLINT,j INTEGER,r REAL,d DOUBLE PRECISION,pad CHAR(5),v VARCHAR(6),flags BOOLEAN[],label NAME)').format(sql.Identifier(ns)))
        c.execute(sql.SQL("INSERT INTO {}.values VALUES (-32768,2147483647,0.1,'-0','x',' x ',ARRAY[true,false,NULL],'Alpha')").format(sql.Identifier(ns)))
    db.close()
    path=freeze_postgres(a['dsn'],a['root'],[(ns,'values')],a['evidence']/'native-freeze',source_version='native',limits=a['limits'])
    token=a['history'].import_package(path); row=list(a['history'].scan(token,0,limit=1))[0]
    assert row['typed']['r']['value']==0.10000000149011612
    assert row['typed']['label']['value']=='Alpha'
    assert row['typed']['pad']['raw']=='x    '
    assert struct.pack('>d',row['typed']['d']['value'])==bytes.fromhex('8000000000000000')
    assert row['typed']['flags']['value']['items']==[True,False,None]
    a['history'].rebuild(token)


def test_unknown_type_is_rejected_before_complete(artificial):
    a=artificial; ns='unknown_'+uuid.uuid4().hex; db=private_pg(a['dsn'],a['root'])
    with db,db.cursor() as c:
        c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(ns)))
        c.execute(sql.SQL('CREATE TABLE {}.unknown (address INET)').format(sql.Identifier(ns)))
    db.close()
    destination=a['evidence']/'unsupported-freeze'
    with pytest.raises(ValueError,match='未支持'): freeze_postgres(a['dsn'],a['root'],[(ns,'unknown')],destination,source_version='unsupported',limits=a['limits'])
    assert not (destination/'COMPLETE.json').exists()


def test_typed_projection_mismatch_prevents_complete(artificial,monkeypatch):
    a=artificial; directory=a['evidence']/'typed-broken-freeze'; shutil.copytree(a['pg_manifest'].parent,directory)
    m=read_json(directory/'manifest.json',a['limits'].max_metadata_bytes);m['source_freeze_id']=uuid.uuid4().hex;resign(directory,m);path=directory/'manifest.json'
    original=a['history']._validate
    def damage(db,m,snapshot,guard):
        db.execute("UPDATE lake.history.t0 SET c0=struct_pack(raw := c0.raw,value := 3::BIGINT) WHERE _ordinal=0")
        snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        return original(db,m,snapshot,guard)
    monkeypatch.setattr(a['history'],'_validate',damage)
    with pytest.raises(ValueError): a['history'].import_package(path)


def test_complete_parquet_damage_is_unavailable(artificial):
    a=artificial; file=Path(a['history'].component(a['pg_token'])['binding']['files'][0]['path']); content=file.read_bytes()
    try:
        file.write_bytes(content[:-1]+bytes([content[-1]^1]))
        with pytest.raises(ValueError): list(a['history'].scan(a['pg_token'],0,limit=1))
    finally: file.write_bytes(content)


def test_invalid_reference_target_rejected(artificial):
    a=artificial; path=a['evidence']/'bad-reference'; shutil.copytree(a['pg_manifest'].parent,path)
    m=read_json(path/'manifest.json',a['limits'].max_metadata_bytes);m['source_freeze_id']=uuid.uuid4().hex
    m['references'][0]['targets'][0]['ordinal']=999; resign(path,m)
    with pytest.raises(ValueError): a['history'].import_package(path/'manifest.json')


def test_original_diagnostics_attachment_retains_bytes(artificial):
    a=artificial; diagnostic=a['evidence']/'原诊断.json'
    raw=' { "date":"2026-03-03", "stage":"人工解析失败", "count":null }\n'.encode()
    diagnostic.write_bytes(raw)
    path=freeze_sqlite(a['source'],a['evidence']/'attachment-freeze',source_version='artificial-with-diagnostic',limits=a['limits'],attachments=[{'path':diagnostic,'role':'original_diagnostic','source_version':'人工诊断v1'}])
    m=verify_package(path.parent,a['limits']); original=m['originals'][1]
    assert (path.parent/original['file']).read_bytes()==raw
    assert original['role']=='original_diagnostic' and original['sha256']==hashlib.sha256(raw).hexdigest()
    token=a['history'].import_package(path)
    assert a['history'].component(token)['original']['originals'][1]==original


def _set_qualification(a,token,state):
    db=private_pg(a['dsn'],a['root'])
    try:
        with db,db.cursor() as c:
            c.execute('UPDATE history_q3.imports SET state=%s WHERE import_id=%s',(state,token.import_id))
    finally: db.close()


def test_repair_scan_current_row_and_page_budgets_zero_delivery(artificial):
    a=artificial; token=a['pg_token']; normal=list(a['history'].scan(token,0,limit=2))
    row_bytes=len(json.dumps(normal[0]['values'],ensure_ascii=True,sort_keys=True,separators=(',',':')).encode())+1
    receipts=[]
    for label,limits in [('row',replace(a['limits'],max_row_bytes=1)),
                         ('page',replace(a['limits'],max_row_bytes=row_bytes,batch_bytes=2*row_bytes-1))]:
        rows=[]
        with pytest.raises(ValueError):
            for row in History(a['dsn'],a['root'],limits).scan(token,0,limit=2): rows.append(row)
        assert rows==[]; receipts.append({'budget':label,'delivered':len(rows),'limits':asdict(limits)})
    assert len(list(History(a['dsn'],a['root'],replace(a['limits'],max_row_bytes=row_bytes)).scan(token,0,limit=1)))==1
    write_json(a['evidence']/'修复-分页预算.json',{'actual_raw_row_bytes':row_bytes,'negative':receipts,'normal_rows':len(normal)})


def test_repair_revoke_after_lake_open_delivers_nothing(artificial,monkeypatch):
    a=artificial; h=a['history']; token=a['pg_token']; original=h._lake; rows=[]
    def revoked(*args,**kwargs):
        db=original(*args,**kwargs);_set_qualification(a,token,'failed');return db
    try:
        monkeypatch.setattr(h,'_lake',revoked)
        with pytest.raises(ValueError):
            for row in h.scan(token,0,limit=2): rows.append(row)
        assert rows==[]
    finally: _set_qualification(a,token,'complete')
    write_json(a['evidence']/'修复-页出口撤销.json',{'revocation':'after_actual_lake_open','delivered_rows':len(rows)})


def test_repair_revoke_after_copy_verification_rolls_back(artificial,monkeypatch):
    a=artificial; h=a['history']; token=a['pg_token']; original=h._verify_copy; schemas=[]
    def revoked(pg,relation,m,table):
        original(pg,relation,m,table)
        schemas.append(relation.as_string(pg).split('.')[0].strip('"'))
        _set_qualification(a,token,'failed')
    try:
        monkeypatch.setattr(h,'_verify_copy',revoked)
        with pytest.raises(ValueError): h.rebuild(token)
        assert schemas
        db=private_pg(a['dsn'],a['root'])
        try:
            with db.cursor() as c:
                c.execute('SELECT count(*) FROM pg_namespace WHERE nspname=%s',(schemas[0],))
                assert c.fetchone()==(0,)
        finally: db.close()
    finally: _set_qualification(a,token,'complete')
    write_json(a['evidence']/'修复-副本出口撤销.json',{'revocation':'after_actual_verify_copy','schema':schemas[0],'committed_schema_count':0})


def test_repair_final_share_lock_blocks_registration_change(artificial):
    import psycopg2
    a=artificial; token=a['pg_token']; stream=a['history'].scan(token,0,limit=2);next(stream)
    db=private_pg(a['dsn'],a['root'])
    try:
        with pytest.raises(psycopg2.errors.LockNotAvailable):
            with db,db.cursor() as c:
                c.execute("SET LOCAL lock_timeout='50ms'")
                c.execute("UPDATE history_q3.imports SET state='failed' WHERE import_id=%s",(token.import_id,))
    finally: db.close();stream.close()
    _set_qualification(a,token,'failed')
    try:
        with pytest.raises(ValueError): list(a['history'].scan(token,0,limit=1))
    finally: _set_qualification(a,token,'complete')
    write_json(a['evidence']/'修复-交付行锁.json',{'during_delivery_sqlstate':'55P03','after_close_revocation':'succeeds','isolation':'READ COMMITTED'})


@pytest.mark.parametrize('kind,raw', [
    ('float4','0.1'),('float4','-0'),('float4','1e-45'),('float4','3.4028235e38'),('float4','-3.4028235e38'),
    ('float8','-0'),('float8','5e-324'),('float8','1.7976931348623157e308'),('float8','1.7976931348623158e308'),
    ('float4','NaN'),('float4','Infinity'),('float8','NaN'),('float8','-Infinity'),
])
def test_repair_float_boundaries_against_actual_pg(artificial,kind,raw):
    import math
    a=artificial; value=scalar(kind,raw); db=private_pg(a['dsn'],a['root'])
    try:
        with db.cursor() as c:
            c.execute(sql.SQL('SELECT pg_catalog.{}(%s::pg_catalog.{})').format(sql.Identifier(kind+'send'),sql.Identifier(kind)),(raw,))
            bits=bytes(c.fetchone()[0])
    finally: db.close()
    if not math.isnan(value): assert struct.pack('>f' if kind=='float4' else '>d',value)==bits
    else: assert math.isnan(struct.unpack('>f' if kind=='float4' else '>d',bits)[0])
    path=a['evidence']/'修复-PG浮点边界.json'
    prior=json.loads(path.read_text()) if path.exists() else []
    write_json(path,[*prior,{'kind':kind,'raw':raw,'source_pg_bits':bits.hex(),'typed_bits':struct.pack('>f' if kind=='float4' else '>d',value).hex()}])


@pytest.mark.parametrize('kind,raw',[('float4','1e39'),('float4','1e-50'),('float8','1e400'),('float8','1e-400')])
def test_repair_invalid_finite_float_resealed_package_rejected(artificial,kind,raw):
    import psycopg2
    a=artificial; db=private_pg(a['dsn'],a['root'])
    try:
        with pytest.raises(psycopg2.errors.NumericValueOutOfRange):
            with db,db.cursor() as c: c.execute(sql.SQL('SELECT %s::pg_catalog.{}').format(sql.Identifier(kind)),(raw,))
    finally: db.close()
    with pytest.raises(ValueError): scalar(kind,raw)
    directory=a['evidence']/('repaired-float-'+kind+'-'+raw);shutil.copytree(a['pg_manifest'].parent,directory)
    m=read_json(directory/'manifest.json',a['limits'].max_metadata_bytes);m['source_freeze_id']=uuid.uuid4().hex
    table=m['tables'][0]; ci=[col['name'] for col in table['columns']].index('f')
    if kind=='float4': table['columns'][ci].update(base_type='float4',original_type='float4',formatted_type='real',type_oid=700)
    whole=hashlib.sha256()
    for bi,block in enumerate(table['blocks']):
        file=directory/block['file'];rows=[json.loads(line) for line in file.read_text().splitlines()]
        if bi==0: rows[0][ci]=raw
        encoded=b''.join(json.dumps(row,ensure_ascii=True,sort_keys=True,separators=(',',':')).encode()+b'\n' for row in rows)
        file.write_bytes(encoded);block.update(bytes=len(encoded),sha256=sha_file(file));whole.update(encoded)
    table['content_sha256']=whole.hexdigest();resign(directory,m)
    with pytest.raises(ValueError,match='浮点'): a['history'].import_package(directory/'manifest.json')
    db=private_pg(a['dsn'],a['root'])
    try:
        with db.cursor() as c:
            c.execute('SELECT state FROM history_q3.imports WHERE source_freeze_id=%s',(m['source_freeze_id'],));assert c.fetchone() is None
    finally: db.close()
    write_json(directory/'类型拒绝回执.json',{'kind':kind,'raw':raw,'pg_cast_sqlstate':'22003','complete':False})


def test_repair_batched_insert_execution_count_and_encoding_expansion(artificial):
    import re
    a=artificial;ns='batch_'+uuid.uuid4().hex;db=private_pg(a['dsn'],a['root'])
    with db,db.cursor() as c:
        c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(ns)))
        c.execute(sql.SQL('CREATE TABLE {}.source (v BIGINT,t TEXT)').format(sql.Identifier(ns)))
        for i in range(9): c.execute(sql.SQL('INSERT INTO {}.source VALUES (%s,%s)').format(sql.Identifier(ns)),(i,"'"*60+'\\'*10))
    db.close()
    path=freeze_postgres(a['dsn'],a['root'],[(ns,'source')],a['evidence']/'repair-batch-freeze',source_version='人工批写',limits=a['limits'])
    token=a['history'].import_package(path);log=a['root']/'pg.log';observations=[]
    for name,limits in [('rows',replace(a['limits'],batch_rows=3)),('bytes',replace(a['limits'],batch_rows=100,batch_bytes=500,max_row_bytes=500))]:
        offset=log.stat().st_size
        h=History(a['dsn']+" options='-c log_statement=all'",a['root'],limits);copy=h.rebuild(token)
        with log.open('rb') as f: f.seek(offset);actual_log=f.read().decode()
        statements=re.findall(r'statement: (INSERT INTO "'+copy['schema']+r'"\."t0" VALUES [^\n]+)',actual_log)
        stats=copy['write_statistics'][0]
        assert len(statements)==stats['statements']<9
        assert stats['rows']==9 and stats['max_statement_bytes']<=limits.batch_bytes and stats['max_rows_per_statement']<=limits.batch_rows
        assert max(len(s.encode()) for s in statements)==stats['max_statement_bytes']
        if name=='rows': assert len(statements)==3
        raw_row_bytes=len(json.dumps(['1',"'"*60+'\\'*10],separators=(',',':')).encode())+1
        assert stats['max_encoded_row_bytes']>raw_row_bytes
        observations.append({'budget':name,'actual_log_insert_statements':len(statements),'stats':stats,'raw_row_bytes':raw_row_bytes,'limits':asdict(limits)})
        (a['evidence']/('修复-批写-'+name+'.log')).write_text(actual_log)
    # 先允许raw行，再拒绝实际SQL编码后的超大行；所有副本DDL/INSERT回滚。
    with pytest.raises(ValueError):
        History(a['dsn'],a['root'],replace(a['limits'],max_row_bytes=raw_row_bytes)).rebuild(token)
    write_json(a['evidence']/'修复-实际批写.json',observations)


@pytest.mark.parametrize('artifact',['ready.json','source-manifest.json','parquet'])
def test_repair_final_copy_file_check_rolls_back(artificial,monkeypatch,artifact):
    a=artificial;h=a['history'];token=a['pg_token'];component=h.component(token)
    file=Path(component['binding']['files'][0]['path']) if artifact=='parquet' else a['root']/'history'/token.import_id/artifact
    before=file.read_bytes();original=h._verify_copy;schemas=[]
    def damage(pg,relation,m,table):
        original(pg,relation,m,table);schemas.append(relation.as_string(pg).split('.')[0].strip('"'))
        if table['name']=='Empty': file.write_bytes(before+b' ')
    try:
        monkeypatch.setattr(h,'_verify_copy',damage)
        with pytest.raises(ValueError): h.rebuild(token)
        db=private_pg(a['dsn'],a['root'])
        try:
            with db.cursor() as c:
                c.execute('SELECT count(*) FROM pg_namespace WHERE nspname=%s',(schemas[0],));assert c.fetchone()==(0,)
        finally: db.close()
    finally: file.write_bytes(before)


def test_repair_final_page_file_check_delivers_nothing(artificial,monkeypatch):
    a=artificial;h=a['history'];token=a['pg_token'];file=a['root']/'history'/token.import_id/'ready.json'
    before=file.read_bytes();original=h._raw;rows=[]
    def damage(m,table,row):
        raw=original(m,table,row)
        if row['_ordinal']==1: file.write_bytes(before+b' ')
        return raw
    try:
        monkeypatch.setattr(h,'_raw',damage)
        with pytest.raises(ValueError):
            for row in h.scan(token,0,limit=2): rows.append(row)
        assert rows==[]
    finally: file.write_bytes(before)


def test_repair_prior_candidate_tokens_remain_readable(artificial):
    path=os.environ.get('Q3_PREVIOUS_RECEIPT')
    if not path: pytest.skip('需要同一私有实例中b1c55d2旧Token回执；不借用他人数据库')
    a=artificial;receipt=json.loads(Path(path).read_text());results=[]
    for component in receipt['资源与组件'].values():
        token=Token(**component['token']);h=a['history'];expected=h.component(token)['original']['tables'][0]['rows']
        assert len(collect(a,token))==expected
        copy=h.rebuild(token);assert copy['token']==asdict(token)
        results.append({'token':asdict(token),'rows':expected,'copy':copy})
    write_json(a['evidence']/'修复-b1旧Token.json',results)


def test_repair_source_root_isolation(artificial):
    a=artificial
    with pytest.raises(ValueError,match='私有socket'):
        History(a['dsn'],a['evidence'],a['limits']).component(a['pg_token'])


def test_repair_rebuild_final_lock_held_until_commit(artificial,monkeypatch):
    import psycopg2
    a=artificial;h=a['history'];token=a['pg_token'];original=h._component;observed=[]
    def check(token,*,pg=None,lock=False):
        result=original(token,pg=pg,lock=lock)
        if pg is not None and lock:
            with pg.cursor() as c:
                c.execute('SHOW transaction_isolation');assert c.fetchone()==('read committed',)
            revoker=private_pg(a['dsn'],a['root'])
            try:
                with pytest.raises(psycopg2.errors.LockNotAvailable):
                    with revoker,revoker.cursor() as c:
                        c.execute("SET LOCAL lock_timeout='50ms'")
                        c.execute("UPDATE history_q3.imports SET state='failed' WHERE import_id=%s",(token.import_id,))
                observed.append('55P03')
            finally: revoker.close()
        return result
    monkeypatch.setattr(h,'_component',check)
    copy=h.rebuild(token);assert observed==['55P03']
    _set_qualification(a,token,'failed')
    try:
        with pytest.raises(ValueError): h.component(token)
    finally: _set_qualification(a,token,'complete')
    write_json(a['evidence']/'修复-副本最终行锁.json',{'before_commit_revocation':observed,'after_commit_revocation':'succeeds','copy':copy})


class _ReadProbe:
    def __init__(self,db,records,stage): self.db,self.records,self.stage=db,records,stage;self.query=''
    def __getattr__(self,name): return getattr(self.db,name)
    def execute(self,query,*args,**kwargs): self.query=str(query);self.db.execute(query,*args,**kwargs);return self
    def fetch_record_batch(self,n):
        native=self.db.fetch_record_batch(n)
        record={'stage':self.stage[0],'requested':n,'rows':[],'bytes':[],'closed':False};self.records.append(record)
        class Reader:
            @property
            def schema(self): return native.schema
            def __iter__(self):
                for batch in native:
                    record['rows'].append(batch.num_rows);record['bytes'].append(batch.nbytes);yield batch
            def close(self): record['closed']=True;native.close()
        return Reader()


@pytest.mark.parametrize('n',[17,65,1025])
def test_read_batch_default_actual_protocol_and_full_values(artificial,monkeypatch,n):
    import re
    a=artificial;limits=Limits();dsn=a['dsn']+" options='-c log_statement=all'";ns='read_'+uuid.uuid4().hex
    db=private_pg(dsn,a['root'])
    with db,db.cursor() as c:
        c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(ns)))
        c.execute(sql.SQL('CREATE TABLE {}.source (i BIGINT,t TEXT,b BYTEA)').format(sql.Identifier(ns)))
        c.execute(sql.SQL("INSERT INTO {}.source SELECT mod(i,4),CASE WHEN mod(i,4)=0 THEN NULL WHEN mod(i,4)=1 THEN '' ELSE %s END,CASE WHEN mod(i,4)=0 THEN NULL ELSE decode('00ff','hex') END FROM generate_series(1,%s) i").format(sql.Identifier(ns)),("'"*48+'\\中',n))
        c.execute(sql.SQL('SELECT i::text,t,b::text FROM {}.source ORDER BY ctid').format(sql.Identifier(ns)));expected=c.fetchmany(n+1)
    db.close();log=a['root']/'pg.log';offset=log.stat().st_size
    path=freeze_postgres(dsn,a['root'],[(ns,'source')],a['evidence']/('read-default-'+str(n)),source_version='人工读取批次',limits=limits)
    with log.open('rb') as f:f.seek(offset);text=f.read().decode()
    source_fetch=re.findall(r'FETCH FORWARD (\d+) FROM "freeze_[0-9a-f]+"',text)
    assert len(source_fetch)==(n+999)//1000+1 and set(source_fetch)=={'1000'}
    h=History(dsn,a['root'],limits);native=h._lake;records=[];stage=['validate']
    monkeypatch.setattr(h,'_lake',lambda *args,**kw:_ReadProbe(native(*args,**kw),records,stage))
    token=h.import_package(path);offset=log.stat().st_size;stage[0]='stream';copy=h.rebuild(token)
    with log.open('rb') as f:f.seek(offset);text=f.read().decode()
    copy_fetch=re.findall(r'FETCH FORWARD (\d+) FROM "verify_copy_[0-9a-f]+"',text)
    assert len(copy_fetch)==(n+999)//1000+1 and set(copy_fetch)=={'1000'}
    stage[0]='scan';rows=[]
    for start in range(0,n,1000): rows.extend(h.scan(token,0,after_ordinal=start-1,limit=min(1000,n-start)))
    assert [tuple(row['values']) for row in rows]==expected
    assert [row['occurrence']['ordinal'] for row in rows]==list(range(n))
    for row,raw in zip(rows,expected):
        assert row['typed']['i']['value']==int(raw[0])
        assert (row['typed']['t']['value'] if row['typed']['t'] is not None else None)==raw[1]
        assert (row['typed']['b']['value'] if row['typed']['b'] is not None else None)==(None if raw[2] is None else bytes.fromhex(raw[2][2:]))
    db=private_pg(dsn,a['root'])
    with db.cursor() as c:
        c.execute(sql.SQL('SELECT i::text,t,b::text FROM {}.t0 ORDER BY ctid').format(sql.Identifier(copy['schema'])));assert c.fetchmany(n+1)==expected
    db.close()
    for entry in records:
        assert entry['requested']>1 and entry['closed'] and max(entry['bytes'],default=0)<=limits.batch_bytes
    for kind in ('validate','stream'): assert sum(len(r['rows']) for r in records if r['stage']==kind)<n
    write_json(a['evidence']/('读取批次-'+str(n)+'.json'),{'rows':n,'source_fetch':source_fetch,'copy_fetch':copy_fetch,'arrow':records,'token':asdict(token),'same_order_and_full_values':True})


def test_read_batch_large_row_and_mixed_rows_keep_default_qualification(artificial,monkeypatch):
    a=artificial;limits=Limits();ns='wide_'+uuid.uuid4().hex;db=private_pg(a['dsn'],a['root'])
    big='x'*(limits.max_row_bytes-128)
    with db,db.cursor() as c:
        c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(ns)))
        c.execute(sql.SQL('CREATE TABLE {}.source (t TEXT)').format(sql.Identifier(ns)))
        values=['small',None,'small',big,'',"'\\",None]
        for value in values:c.execute(sql.SQL('INSERT INTO {}.source VALUES (%s)').format(sql.Identifier(ns)),(value,))
    db.close()
    path=freeze_postgres(a['dsn'],a['root'],[(ns,'source')],a['evidence']/'wide-default-freeze',source_version='合法近4MiB单行',limits=limits)
    m=verify_package(path.parent,limits);assert all(b['bytes']<=limits.batch_bytes for b in m['tables'][0]['blocks'])
    h=History(a['dsn'],a['root'],limits);native=h._lake;records=[];stage=['large']
    monkeypatch.setattr(h,'_lake',lambda *args,**kw:_ReadProbe(native(*args,**kw),records,stage))
    token=h.import_package(path)
    reader_db=native(token.import_id,True)
    try: buffer_bound=h._read_width(reader_db,0,token.snapshot,m['tables'][0])
    finally: reader_db.close()
    assert list(h.scan(token,0,after_ordinal=2,limit=1))[0]['values']==[big]
    copy=h.rebuild(token);assert copy['write_statistics'][0]['max_statement_bytes']<=limits.batch_bytes
    db=private_pg(a['dsn'],a['root'])
    with db.cursor() as c:
        c.execute(sql.SQL('SELECT t FROM {}.t0 ORDER BY ctid').format(sql.Identifier(copy['schema'])));assert [r[0] for r in c.fetchmany(10)]==values
    db.close()
    tight=History(a['dsn'],a['root'],replace(limits,max_row_bytes=100))
    assert list(tight.scan(token,0,limit=1))[0]['values']==['small']
    delivered=[]
    with pytest.raises(ValueError):
        for row in tight.scan(token,0,after_ordinal=2,limit=1):delivered.append(row)
    assert not delivered
    write_json(a['evidence']/'读取批次-近单行界限.json',{'big_characters':len(big),'limits':asdict(limits),'token':asdict(token),'copy':copy,'tight_large_row_delivered':0,'arrow_buffer_bound':max(limits.batch_bytes,buffer_bound),'arrow':records})
    assert any(max(r['bytes'],default=0)>limits.batch_bytes for r in records)
    assert all(max(r['bytes'],default=0)<=max(limits.batch_bytes,buffer_bound) and r['closed'] for r in records)


def test_read_batch_empty_fault_and_close_release_reader(artificial,monkeypatch):
    a=artificial;h=History(a['dsn'],a['root']);records=[];stage=['empty'];native=h._lake
    monkeypatch.setattr(h,'_lake',lambda *args,**kw:_ReadProbe(native(*args,**kw),records,stage))
    assert list(h.scan(a['pg_token'],2,limit=10))==[] and records[-1]['closed'] and records[-1]['rows']==[]
    component=h.component(a['pg_token']);stage[0]='close';stream=h._stream(component,a['pg_token'],0);next(stream);stream.close()
    assert records[-1]['closed']
    stage[0]='failure'
    def fail(*args):raise ValueError('人工批内错误')
    monkeypatch.setattr(h,'_raw',fail)
    with pytest.raises(ValueError):list(h.scan(a['pg_token'],0,limit=2))
    assert records[-1]['closed']
    write_json(a['evidence']/'读取批次-close与故障.json',records)


def test_read_batch_4c_frozen_tokens_need_no_reimport(artificial):
    path=os.environ.get('Q3_4C_RECEIPT')
    if not path: pytest.skip('需要同一私有PG中固定4c旧Token回执')
    a=artificial;index=json.loads(Path(path).read_text());h=History(a['dsn'],a['root']);proof=[]
    for item in index['资源'].values():
        token=Token(**item['token']);component=h.component(token)
        assert token.import_id not in (a['pg_token'].import_id,a['sqlite_token'].import_id)
        for ti,table in enumerate(component['original']['tables']):
            digest=hashlib.sha256();count=0
            for start in range(0,table['rows'],1000):
                for row in h.scan(token,ti,after_ordinal=start-1,limit=min(1000,table['rows']-start)):
                    digest.update(json.dumps(row['values'],ensure_ascii=True,sort_keys=True,separators=(',',':')).encode()+b'\n');count+=1
            assert count==table['rows'] and digest.hexdigest()==table['content_sha256']
        copy=h.rebuild(token);assert copy['token']==asdict(token)
        proof.append({'token':asdict(token),'full_fields_digest_and_order':'equal','copy':copy})
    write_json(a['evidence']/'读取批次-4c旧Token.json',proof)


def test_read_batch_null_cells_overhead_counts_in_source_buffer(artificial):
    import re
    a=artificial;dsn=a['dsn']+" options='-c log_statement=all'";ns='nullwidth_'+uuid.uuid4().hex;cols=33
    db=private_pg(dsn,a['root'])
    with db,db.cursor() as c:
        c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(ns)))
        c.execute(sql.SQL('CREATE TABLE {}.source ({})').format(sql.Identifier(ns),sql.SQL(',').join(sql.SQL('{} TEXT').format(sql.Identifier('c'+str(i))) for i in range(cols))))
        for _ in range(17): c.execute(sql.SQL('INSERT INTO {}.source DEFAULT VALUES').format(sql.Identifier(ns)))
    db.close();limits=Limits(batch_rows=100,batch_bytes=1000,max_row_bytes=300);log=a['root']/'pg.log';offset=log.stat().st_size
    path=freeze_postgres(dsn,a['root'],[(ns,'source')],a['evidence']/'read-null-width',source_version='全NULL行宽',limits=limits)
    with log.open('rb') as f:f.seek(offset);text=f.read().decode()
    fetches=[int(n) for n in re.findall(r'FETCH FORWARD (\d+) FROM "freeze_[0-9a-f]+"',text)]
    raw_bytes=len(json.dumps([None]*cols,separators=(',',':')).encode())+1
    assert set(fetches)=={5} and all(n*raw_bytes<=limits.batch_bytes for n in fetches)
    assert verify_package(path.parent,limits)['tables'][0]['rows']==17
