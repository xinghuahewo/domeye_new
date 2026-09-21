"""Q3-B：仅显式绑定本任务人工多库；不读取真实源或环境默认DSN。"""
from collections import Counter
from contextlib import closing
from dataclasses import asdict, replace
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import time
import uuid

import psycopg2
from psycopg2 import sql
import pytest

from data_pipeline.history.database_import import History, Limits, PGIdentity, PGSource, SQLiteSource, Token, freeze_postgres_source, freeze_sqlite_source
from data_pipeline.history.database_import import reader as reader_module
from data_pipeline.history.database_import.codec import canonical
from data_pipeline.history.database_import.freeze import private_pg, sha_file, write_json


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str)+'\n')


@pytest.fixture(scope='module')
def bound():
    location = os.environ.get('Q3_PRIVATE_ROOT')
    if not location: pytest.skip('仅显式本任务私有人工PG')
    root = Path(location).resolve(); out = root/'q3-b'/('acceptance-'+uuid.uuid4().hex); out.mkdir(parents=True)
    suffix = uuid.uuid4().hex[:12]
    source_name, target_name, role = 'q3bs_'+suffix, 'q3bt_'+suffix, 'q3br_'+suffix
    base = f'host={root / "socket"} port=28763'
    admin_dsn = base+' dbname=postgres'
    pg = private_pg(admin_dsn, root); pg.autocommit = True
    with pg.cursor() as c:
        c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(source_name)))
        c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(target_name)))
        c.execute(sql.SQL('CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT').format(sql.Identifier(role)))
    pg.close()
    source_admin = base+' dbname='+source_name
    target_dsn = base+' dbname='+target_name
    source_pg = private_pg(source_admin, root)
    raw = ['9007199254740993', '123456789012345678901234567890.1234567890',
           '[0:1][3:4]={{9007199254740993,NULL},{-2,0}}',
           '[0:3]={"NULL",NULL,"","a,b"}', '\\x00ff01',
           '2026-03-01 04:34:56.123456+00', '  {"x": 1, "x": 2}  ', None, '尾\\行\n中文']
    columns = 'n bigint,precise numeric(40,10),arr bigint[],words text[],blob bytea,instant timestamptz(6),j json,missing text,txt text'
    casts = ['bigint','numeric(40,10)','bigint[]','text[]','bytea','timestamptz','json','text','text']
    with source_pg, source_pg.cursor() as c:
        for schema in ('left_side','right_side'):
            c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
            c.execute(sql.SQL('CREATE TABLE {}.same ('+columns+')').format(sql.Identifier(schema)))
        c.execute('CREATE TABLE left_side.empty (x bigint)')
        for row in (raw, raw, [None]*len(raw)):
            c.execute('INSERT INTO left_side.same VALUES ('+','.join('%s::'+t for t in casts)+')', row)
        c.execute('INSERT INTO right_side.same VALUES ('+','.join('%s::'+t for t in casts)+')', raw)
        c.execute(sql.SQL('GRANT USAGE ON SCHEMA left_side,right_side TO {}').format(sql.Identifier(role)))
        c.execute(sql.SQL('GRANT SELECT ON ALL TABLES IN SCHEMA left_side,right_side TO {}').format(sql.Identifier(role)))
        c.execute('REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC')
        c.execute(sql.SQL('GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {}').format(sql.Identifier(role)))
        c.execute('SELECT rolsuper,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=%s',(role,))
        assert c.fetchone() == (False,False,False)
        source_identity = PGIdentity.read(source_pg)
    source_pg.close()
    with closing(private_pg(target_dsn,root)) as target_pg: target_identity = PGIdentity.read(target_pg)
    source = PGSource(source_identity,'history-test://q3b/source','artificial-v1',source_admin+' user='+role)
    h = History(target_dsn,root,target_identity=target_identity)
    metadata = {'publication':{'status':'unknown','reason':'人工历史没有业务publication','source_freeze_id':'original-freeze-id','import_id':'original-import-id'},
                'availability':{'status':'available','reason':None},
                'references':[{'original_ref':'old:1','status':'resolved','targets':[{'table_index':0,'ordinal':1}]},{'original_ref':'old:unknown','status':'unknown','reason':'原关联不可判定','targets':[]}]}
    args = dict(source=source,tables=[('left_side','same'),('right_side','same'),('left_side','empty')],target_identity=target_identity,**metadata)
    manifest = freeze_postgres_source(destination=out/'pg-freeze',**args)
    token = h.import_package(manifest)
    assert h.import_package(manifest) == token
    save(out/'人工绑定与索引.json',{'source_identity':asdict(source_identity),'target_identity':asdict(target_identity),
         'source_role_capabilities':{'superuser':False,'createdb':False,'createrole':False},
         'manifest':str(manifest),'token':asdict(token),'expected_raw':[raw,raw,[None]*len(raw)],'target_database':target_name})
    print('Q3B_ARTIFICIAL_EVIDENCE='+str(out))
    return dict(root=root,out=out,source=source,source_admin=source_admin,target_dsn=target_dsn,admin_dsn=admin_dsn,
                source_name=source_name,role=role,identity=target_identity,h=h,args=args,manifest=manifest,token=token,raw=raw)


def test_multidatabase_typed_occurrences_and_receipt(bound):
    b = bound; h = b['h']; token = b['token']
    with h.bulk(token,batch_rows=1) as stream:
        batches = []
        for batch in stream:
            assert batch['qualification'] == 'provisional' and stream.receipt is None
            batches.append(batch)
        receipt = stream.receipt
    rows = [r for batch in batches if batch['table_index']==0 for r in batch['rows']]
    assert [r['values'] for r in rows] == [b['raw'],b['raw'],[None]*len(b['raw'])]
    assert Counter(canonical(r['values']) for r in rows)[canonical(b['raw'])] == 2
    assert [r['occurrence']['ordinal'] for r in rows] == [0,1,2]
    assert rows[0]['typed']['precise']['value'] == b['raw'][1]
    assert rows[0]['typed']['arr']['value']['lower_bounds'] == [0,3]
    assert rows[0]['typed']['blob']['value'] == bytes.fromhex('00ff01')
    assert receipt['scope'] == 'whole_archive' and [r['rows'] for r in receipt['tables']] == [3,1,0]
    checksum = receipt.pop('receipt_sha256')
    assert hashlib.sha256(canonical(receipt)).hexdigest() == checksum
    assert stream.receipt['receipt_sha256'] == checksum  # 外部修改不污染内部回执。
    assert h.component(token)['original']['publication'] == b['args']['publication']
    assert h.resolve_reference(token,'old:1')['status'] == 'resolved'
    assert h.resolve_reference(token,'old:unknown')['status'] == 'unknown'
    assert rows[0]['typed']['words']['value']['items']==['NULL',None,'','a,b']
    assert rows[0]['typed']['instant']['value']['finite'].microsecond==123456
    save(b['out']/'公开批流完整回执.json',stream.receipt)


@pytest.mark.parametrize('cap',[1,2,1000])
def test_full_qualification_constant_per_session(bound,monkeypatch,cap):
    h = bound['h']; calls = []; original = h._component
    def check(*args,**kwargs):
        calls.append(bool(kwargs.get('lock')))
        return original(*args,**kwargs)
    monkeypatch.setattr(h,'_component',check)
    with h.bulk(bound['token'],batch_rows=cap) as stream:
        batches = list(stream)
    assert calls == [False,True]
    assert sum(len(b['rows']) for b in batches) == 4
    assert stream.receipt['resources']['rows'] == 4


def test_subset_and_empty_scope(bound):
    with bound['h'].bulk(bound['token'],table_indices=[2]) as stream:
        assert list(stream) == []
    assert stream.receipt['scope']=='declared_table_subset' and stream.receipt['tables'][0]['rows']==0
    for bad in ([0,0],[1,0],[True],[],[-1]):
        with pytest.raises(ValueError): bound['h'].bulk(bound['token'],table_indices=bad)
    with pytest.raises(ValueError):
        with bound['h'].bulk(bound['token'],table_indices=[9]) as stream: list(stream)
    assert stream.receipt is None


def test_early_close_releases_real_duckdb_reader(bound,monkeypatch):
    h = bound['h']; closed = []; batches_closed=[]; original=h._lake; original_batches=h._batches
    class DB:
        def __init__(self,db): self.db=db
        def __getattr__(self,key): return getattr(self.db,key)
        def close(self): self.db.close(); closed.append(True)
    def batches(*args,**kwargs):
        try:
            with closing(original_batches(*args,**kwargs)) as iterator: yield from iterator
        finally: batches_closed.append(True)
    monkeypatch.setattr(h,'_lake',lambda *args:DB(original(*args)))
    monkeypatch.setattr(h,'_batches',batches)
    with h.bulk(bound['token'],batch_rows=1) as stream: next(stream)
    assert stream.state == 'incomplete' and stream.receipt is None
    assert closed == [True] and batches_closed == [True]
    stream = h.bulk(bound['token']); stream.close()
    assert stream.state == 'incomplete' and stream.receipt is None


def test_final_code_drift_no_receipt(bound,monkeypatch):
    actual=reader_module.code_identity; calls=[]
    def identity():
        value=actual(); calls.append(True)
        if len(calls)==2: value['reader.py']='changed'
        return value
    monkeypatch.setattr(reader_module,'code_identity',identity)
    with pytest.raises(ValueError,match='代码版本漂移'):
        with bound['h'].bulk(bound['token']) as stream: list(stream)
    assert stream.state=='failed' and stream.receipt is None


def test_final_actual_qualification_drift(bound):
    b=bound
    with closing(private_pg(b['target_dsn'],b['root'])) as pg:
        try:
            with pytest.raises(ValueError,match='候选/失败'):
                with b['h'].bulk(b['token'],batch_rows=1) as stream:
                    next(stream)
                    with pg,pg.cursor() as c: c.execute("UPDATE history_q3.imports SET state='failed' WHERE import_id=%s",(b['token'].import_id,))
                    list(stream)
            assert stream.receipt is None
        finally:
            with pg,pg.cursor() as c: c.execute("UPDATE history_q3.imports SET state='complete' WHERE import_id=%s",(b['token'].import_id,))


@pytest.mark.parametrize('filename',['ready.json','source-manifest.json','parquet'])
def test_final_actual_file_drift(bound,filename):
    b=bound; directory=b['h'].data_root/b['token'].import_id
    path=directory/filename if filename!='parquet' else next((directory/'parquet').rglob('*.parquet'))
    original=path.read_bytes()
    try:
        with pytest.raises(ValueError,match='SHA|损坏'):
            with b['h'].bulk(b['token'],batch_rows=1) as stream:
                delivered = 0
                while delivered < 4: delivered += len(next(stream)['rows'])
                path.write_bytes(original+b' ')
                list(stream)
        assert stream.receipt is None
    finally: path.write_bytes(original)


def test_totals_cross_tables_and_row_budget(bound):
    b=bound
    for limits in (replace(Limits(),max_total_rows=3),replace(Limits(),max_row_bytes=1)):
        h=History(b['target_dsn'],b['root'],limits,target_identity=b['identity'])
        with pytest.raises(ValueError):
            with h.bulk(b['token']) as stream: list(stream)
        assert stream.receipt is None


def test_output_byte_boundary_and_fixed_session_budget(bound):
    b=bound; limits=replace(Limits(),batch_bytes=300,max_row_bytes=300)
    h=History(b['target_dsn'],b['root'],limits,target_identity=b['identity'])
    with h.bulk(b['token']) as stream: batches=list(stream)
    assert [len(batch['rows']) for batch in batches]==[1,1,1,1]
    assert all(batch['raw_bytes']<=300 for batch in batches)
    h=History(b['target_dsn'],b['root'],target_identity=b['identity'])
    with pytest.raises(ValueError,match='预算漂移'):
        with h.bulk(b['token'],batch_rows=1) as stream:
            next(stream); h.limits=replace(h.limits,batch_rows=2000); list(stream)
    assert stream.receipt is None


def test_source_target_fail_before_catalog_write(bound):
    b=bound; args=b['args'].copy(); args['target_identity']=b['source'].identity
    bad=b['out']/'same-source-output'
    with pytest.raises(ValueError,match='相同'): freeze_postgres_source(destination=bad,**args)
    assert not bad.exists()
    for h in (History(b['source_admin'],b['root'],target_identity=b['identity']),
              History(b['source_admin'],b['root'],target_identity=b['source'].identity),
              History(b['target_dsn'],b['root'])):
        with pytest.raises(ValueError): h.import_package(b['manifest'])
    with closing(private_pg(b['source_admin'],b['root'])) as pg,pg.cursor() as c:
        c.execute("SELECT count(*) FROM pg_namespace WHERE nspname='history_q3' OR nspname LIKE 'hl_%' OR nspname LIKE 'hqcopy_%'")
        assert c.fetchone()==(0,)


def test_source_permission_identity_and_no_credentials(bound):
    b=bound
    for dsn in ('','dbname=unspecified','not-a-dsn'):
        with pytest.raises(ValueError): replace(b['source'],dsn=dsn)
    assert b['source'].dsn not in repr(b['source']) and b['role'] not in repr(b['source'])
    args=b['args'].copy(); args['source']=replace(b['source'],identity=PGIdentity(b['source'].identity.system_identifier,1))
    with pytest.raises(ValueError,match='身份不符'): freeze_postgres_source(destination=b['out']/'wrong-id',**args)
    with closing(private_pg(b['source_admin'],b['root'])) as pg:
        with pg,pg.cursor() as c:
            c.execute(sql.SQL('REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM {}').format(sql.Identifier(b['role'])))
        try:
            with pytest.raises(ValueError,match='pg_control_system') as error:
                freeze_postgres_source(destination=b['out']/'permission',**b['args'])
            assert b['role'] not in str(error.value) and b['source'].dsn not in str(error.value)
        finally:
            with pg,pg.cursor() as c: c.execute(sql.SQL('GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {}').format(sql.Identifier(b['role'])))
    for directory in ('wrong-id','permission'):
        assert not (b['out']/directory/'COMPLETE.json').exists()
    text=b['manifest'].read_text()
    assert b['source'].dsn not in text and b['role'] not in text and 'password=' not in text
    with closing(b['source'].connect()) as pg,pg.cursor() as c:
        with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction): c.execute('CREATE TABLE forbidden(x int)')


def test_sqlite_bound_closed_file_and_unknown(bound):
    b=bound; source=b['out']/'independent.sqlite'
    with sqlite3.connect(source) as db:
        db.execute('CREATE TABLE historic(value)')
        db.executemany('INSERT INTO historic VALUES (?)',[(None,),(b'\0\xff',),('中文',),('中文',),(9007199254740993,)])
    binding=SQLiteSource('history-test://q3b/sqlite','file-v1',sha_file(source),True)
    args=dict(binding=binding,publication={'status':'unknown','reason':'原发布未知'},availability={'status':'unknown','reason':'原可用性未知'})
    manifest=freeze_sqlite_source(source,b['out']/'sqlite-freeze',**args)
    token=b['h'].import_package(manifest); source.unlink()
    with b['h'].bulk(token) as stream: rows=[r for batch in stream for r in batch['rows']]
    assert len(rows)==5 and rows[2]['values']==rows[3]['values']
    assert stream.receipt['availability']['status']=='unknown'
    with pytest.raises(ValueError,match='隔离'):
        with b['h'].bulk(token,purpose='business') as rejected: list(rejected)
    rebuilt=b['h'].rebuild(token)
    assert rebuilt['tables'][0]['rows']==5
    save(b['out']/'SQLite独立文件回执.json',{'token':asdict(token),'receipt':stream.receipt,'rebuild':rebuilt,'source_removed':not source.exists()})


def test_source_database_removed_fresh_reader_and_rebuild(bound,monkeypatch):
    b=bound; clone='q3bgone_'+uuid.uuid4().hex[:12]
    with closing(private_pg(b['admin_dsn'],b['root'])) as admin:
        admin.autocommit=True
        with admin.cursor() as c: c.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(sql.Identifier(clone),sql.Identifier(b['source_name'])))
    clone_admin=b['source_admin'].replace('dbname='+b['source_name'],'dbname='+clone)
    with closing(private_pg(clone_admin,b['root'])) as pg: identity=PGIdentity.read(pg)
    source=replace(b['source'],identity=identity,dsn=clone_admin+' user='+b['role'])
    args={**b['args'],'source':source}
    manifest=freeze_postgres_source(destination=b['out']/'removed-source-freeze',**args)
    token=b['h'].import_package(manifest)
    with closing(private_pg(b['admin_dsn'],b['root'])) as admin:
        admin.autocommit=True
        with admin.cursor() as c:
            c.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(clone)))
            c.execute('SELECT oid FROM pg_database WHERE datname=%s',(clone,)); assert c.fetchone() is None
    # 冻结包也移走；新实例仅持目标DSN、身份和固定Token。
    shutil.move(str(manifest.parent),str(b['out']/'removed-source-package-retained'))
    monkeypatch.setattr(PGSource,'connect',lambda self:pytest.fail('消费阶段不得访问源'))
    h=History(b['target_dsn'],b['root'],target_identity=b['identity'])
    with h.bulk(token,batch_rows=2) as stream: batches=list(stream)
    assert [r['values'] for batch in batches if batch['table_index']==0 for r in batch['rows']]==[b['raw'],b['raw'],[None]*len(b['raw'])]
    copy=h.rebuild(token)
    with closing(private_pg(b['target_dsn'],b['root'])) as pg,pg.cursor() as c:
        for index,expected in enumerate(([b['raw'],b['raw'],[None]*len(b['raw'])],[b['raw']])):
            c.execute("SET TimeZone='UTC'")
            c.execute(sql.SQL('SELECT n::text,precise::text,arr::text,words::text,blob::text,instant::text,j::text,missing,txt FROM {}.{} ORDER BY ctid').format(sql.Identifier(copy['schema']),sql.Identifier('t'+str(index))))
            assert [list(r) for r in c.fetchall()]==expected
    save(b['out']/'PG源移除完整闭环.json',{'source_database_absent':True,'package_moved':True,'token':asdict(token),'receipt':stream.receipt,'rebuild':copy,'all_fields_order_duplicates_equal':True})


def test_source_snapshot_lock_and_concurrent_update(bound,monkeypatch):
    from data_pipeline.history.database_import import freeze as module
    b=bound; original=module._write_table; observed=[]
    def write(root,m,table,rows,expected,guard):
        result=original(root,m,table,rows,expected,guard)
        if table['schema']=='left_side' and table['name']=='same':
            with closing(private_pg(b['source_admin'],b['root'])) as pg:
                with pg,pg.cursor() as c:
                    c.execute('INSERT INTO right_side.same SELECT * FROM right_side.same')
                    c.execute("SELECT count(*) FROM pg_locks l JOIN pg_class t ON t.oid=l.relation JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='left_side' AND t.relname='same' AND mode='AccessShareLock' AND granted")
                    observed.append(c.fetchone()[0])
        return result
    monkeypatch.setattr(module,'_write_table',write)
    try:
        manifest=freeze_postgres_source(destination=b['out']/'same-snapshot',**b['args'])
        m=json.loads(manifest.read_text())
        assert m['tables'][1]['rows']==1 and observed[0]>=1
        assert m['export']['read_only'] is True and m['export']['isolation']=='repeatable read'
        save(b['out']/'PG同快照并发写验证.json',{'rows_after_concurrent_insert_in_frozen_second_table':1,'access_share_locks':observed,'export':m['export']})
    finally:
        with closing(private_pg(b['source_admin'],b['root'])) as pg:
            with pg,pg.cursor() as c: c.execute('DELETE FROM right_side.same WHERE ctid NOT IN (SELECT ctid FROM right_side.same ORDER BY ctid LIMIT 1)')


@pytest.mark.parametrize('case',['permission','unsupported','rls','inheritance','interrupted'])
def test_failed_source_never_complete(bound,monkeypatch,case):
    from data_pipeline.history.database_import import freeze as module
    b=bound; path=b['out']/('rejected-'+case); table='bad_'+case
    with closing(private_pg(b['source_admin'],b['root'])) as pg:
        with pg,pg.cursor() as c:
            c.execute(sql.SQL('CREATE TABLE left_side.{} (x '+('uuid' if case=='unsupported' else 'text')+')').format(sql.Identifier(table)))
            if case!='permission': c.execute(sql.SQL('GRANT SELECT ON left_side.{} TO {}').format(sql.Identifier(table),sql.Identifier(b['role'])))
            if case=='inheritance': c.execute(sql.SQL('CREATE TABLE left_side.child () INHERITS (left_side.{})').format(sql.Identifier(table)))
            if case=='rls': c.execute(sql.SQL('ALTER TABLE left_side.{} ENABLE ROW LEVEL SECURITY').format(sql.Identifier(table)))
    if case=='interrupted': monkeypatch.setattr(module,'_write_table',lambda *args:(_ for _ in ()).throw(KeyboardInterrupt()))
    args={**b['args'],'tables':[('left_side',table)]}
    with pytest.raises((ValueError,KeyboardInterrupt)): freeze_postgres_source(destination=path,**args)
    assert not (path/'COMPLETE.json').exists()


@pytest.mark.parametrize('case',['sha','wal','immutable'])
def test_sqlite_binding_rejects_changed_or_open_original(bound,case):
    b=bound; path=b['out']/('bad-'+case+'.sqlite')
    with sqlite3.connect(path) as db: db.execute('CREATE TABLE x(a)')
    sha=sha_file(path)
    if case=='wal': Path(str(path)+'-wal').write_bytes(b'open')
    args=dict(publication={'status':'unknown','reason':'人工未知'},availability={'status':'unknown','reason':'测试未知'})
    with pytest.raises(ValueError):
        binding=SQLiteSource('history-test://q3b/bad','v1','0'*64 if case=='sha' else sha,case!='immutable')
        freeze_sqlite_source(path,b['out']/('bad-freeze-'+case),binding=binding,**args)
    assert not (b['out']/('bad-freeze-'+case)/'COMPLETE.json').exists()


class ReadProbe:
    def __init__(self,db,records,queries): self.db,self.records,self.queries=db,records,queries
    def __getattr__(self,name): return getattr(self.db,name)
    def execute(self,query,*args,**kwargs):
        self.queries.append(str(query)); self.db.execute(query,*args,**kwargs); return self
    def fetch_record_batch(self,n):
        reader=self.db.fetch_record_batch(n)
        record={'requested':n,'actual_rows':[],'actual_bytes':[],'closed':False}; self.records.append(record)
        class Reader:
            @property
            def schema(self): return reader.schema
            def __iter__(self):
                for batch in reader:
                    record['actual_rows'].append(batch.num_rows); record['actual_bytes'].append(batch.nbytes); yield batch
            def close(self): reader.close(); record['closed']=True
        return Reader()


@pytest.mark.parametrize('n',[17,65,1025])
def test_actual_default_sql_arrow_and_constant_sha_cost(bound,monkeypatch,n):
    import data_pipeline.history.database_import.store as module
    b=bound; table='cost_'+str(n); out=b['out']/table; out.mkdir()
    with closing(private_pg(b['source_admin'],b['root'])) as pg:
        with pg,pg.cursor() as c:
            c.execute(sql.SQL('CREATE TABLE left_side.{} (i bigint,t text,b bytea)').format(sql.Identifier(table)))
            c.execute(sql.SQL("INSERT INTO left_side.{} SELECT mod(i,4),CASE WHEN mod(i,4)=0 THEN NULL WHEN mod(i,4)=1 THEN '' ELSE %s END,decode('00ff','hex') FROM generate_series(1,%s) i").format(sql.Identifier(table)),("'"*48+'\\中文',n))
            c.execute(sql.SQL('GRANT SELECT ON left_side.{} TO {}').format(sql.Identifier(table),sql.Identifier(b['role'])))
            # 仅人工角色设置真实协议日志；源冻结本身不执行该配置写入。
            c.execute(sql.SQL("ALTER ROLE {} SET log_statement='all'").format(sql.Identifier(b['role'])))
            c.execute(sql.SQL('SELECT i::text,t,b::text FROM left_side.{} ORDER BY ctid').format(sql.Identifier(table)))
            expected=[list(r) for r in c.fetchall()]
    log=b['root']/'pg.log'; offset=log.stat().st_size
    try: manifest=freeze_postgres_source(destination=out/'freeze',**{**b['args'],'tables':[('left_side',table)],'references':[]})
    finally:
        with closing(private_pg(b['source_admin'],b['root'])) as pg:
            with pg,pg.cursor() as c: c.execute(sql.SQL('ALTER ROLE {} RESET log_statement').format(sql.Identifier(b['role'])))
    with log.open('rb') as f: f.seek(offset); source_log=f.read().decode()
    # 只保留冻结连接的BEGIN至ROLLBACK，测试设置角色的SQL不进入源证据。
    source_log='\n'.join(line for line in source_log.splitlines() if b['role'] not in line)
    (out/'source.log').write_text(source_log)
    fetches=re.findall(r'FETCH FORWARD (\d+) FROM "freeze_[0-9a-f]+"',source_log)
    assert fetches==['1000']*((n+999)//1000+1)
    h=History(b['target_dsn']+" options='-c log_statement=all'",b['root'],target_identity=b['identity'])
    native=h._lake; arrows=[]; queries=[]
    monkeypatch.setattr(h,'_lake',lambda *a:ReadProbe(native(*a),arrows,queries))
    token=h.import_package(manifest)
    trials=[]; actual_sha=module.sha_file; actual_component=h._component
    for cap in (1,17,1000):
        sha_calls=[]; checks=[]; arrows.clear(); queries.clear()
        def sha(path,*a): sha_calls.append(str(path)); return actual_sha(path,*a)
        def component(*a,**kw): checks.append(bool(kw.get('lock'))); return actual_component(*a,**kw)
        monkeypatch.setattr(module,'sha_file',sha); monkeypatch.setattr(h,'_component',component)
        log_start=log.stat().st_size; started=time.perf_counter()
        with h.bulk(token,batch_rows=cap) as stream: rows=[r for batch in stream for r in batch['rows']]
        with log.open('rb') as file: file.seek(log_start); bulk_log=file.read().decode()
        (out/('bulk-'+str(cap)+'.log')).write_text(bulk_log)
        assert [r['values'] for r in rows]==expected
        assert [r['occurrence']['ordinal'] for r in rows]==list(range(n))
        for row,raw in zip(rows,expected):
            assert row['typed']['i']['value']==int(raw[0])
            assert (row['typed']['t']['value'] if row['typed']['t'] else None)==raw[1]
            assert row['typed']['b']['value']==b'\0\xff'
        assert checks==[False,True] and len(arrows)==1
        request=arrows[0]['requested']
        assert request>1 and arrows[0]['actual_rows']==([request]*(n//request)+([n%request] if n%request else []))
        assert arrows[0]['closed']
        trials.append({'cap':cap,'seconds':time.perf_counter()-started,'sha_calls':list(sha_calls),'qualification_calls':checks,'actual_pg_statements':len(re.findall('statement:|execute [^:]+:',bulk_log)),
                       'actual_arrow':json.loads(json.dumps(arrows)),'actual_duckdb_queries':list(queries),'receipt':stream.receipt})
        monkeypatch.setattr(module,'sha_file',actual_sha); monkeypatch.setattr(h,'_component',actual_component)
    assert len({len(r['sha_calls']) for r in trials})==1
    offset=log.stat().st_size; rebuilt=h.rebuild(token)
    with log.open('rb') as f: f.seek(offset); copy_log=f.read().decode()
    (out/'copy.log').write_text(copy_log)
    copy_fetch=re.findall(r'FETCH FORWARD (\d+) FROM "verify_copy_[0-9a-f]+"',copy_log)
    assert copy_fetch==fetches
    with closing(private_pg(b['target_dsn'],b['root'])) as pg,pg.cursor() as c:
        c.execute(sql.SQL('SELECT i::text,t,b::text FROM {}.t0 ORDER BY ctid').format(sql.Identifier(rebuilt['schema'])))
        assert [list(r) for r in c.fetchall()]==expected
    save(out/'实际成本与完整对账.json',{'rows':n,'source_fetches':fetches,'copy_fetches':copy_fetch,'trials':trials,'token':asdict(token),'rebuild':rebuilt,'full_raw_typed_occurrences_equal':True})


def test_actual_code_file_change_prevents_receipt(bound,monkeypatch):
    source=Path(reader_module.__file__).parent; target=bound['out']/'execution-code-copy'; target.mkdir()
    for path in source.glob('*.py'): shutil.copyfile(path,target/path.name)
    monkeypatch.setattr(reader_module,'__file__',str(target/'reader.py'))
    with pytest.raises(ValueError,match='代码版本漂移'):
        with bound['h'].bulk(bound['token'],batch_rows=1) as stream:
            next(stream)
            with (target/'reader.py').open('a') as file: file.write('\n# 人工代码漂移反例\n')
            list(stream)
    assert stream.receipt is None


def test_wide_row_null_precise_numeric_and_output_bytes(bound,monkeypatch):
    b=bound; table='wide'; size=4*1024**2-256
    with closing(private_pg(b['source_admin'],b['root'])) as pg:
        with pg,pg.cursor() as c:
            c.execute('CREATE TABLE left_side.wide (x text,d numeric(12,4),ts timestamp(6))')
            c.execute("INSERT INTO left_side.wide VALUES ('small',12.1250,'2026-03-01 12:34:56.123456'),(%s,12.1250,'2026-03-01 12:34:56.123456'),(NULL,NULL,NULL)",('w'*size,))
            c.execute(sql.SQL('GRANT SELECT ON left_side.wide TO {}').format(sql.Identifier(b['role'])))
    manifest=freeze_postgres_source(destination=b['out']/'wide-freeze',**{**b['args'],'tables':[('left_side',table)],'references':[]})
    token=b['h'].import_package(manifest); h=History(b['target_dsn'],b['root'],target_identity=b['identity'])
    native=h._lake; arrows=[];queries=[]
    monkeypatch.setattr(h,'_lake',lambda *a:ReadProbe(native(*a),arrows,queries))
    with h.bulk(token) as stream:
        batches=list(stream)
    rows=[r for batch in batches for r in batch['rows']]
    assert [len(r['values'][0]) if r['values'][0] is not None else None for r in rows]==[5,size,None]
    assert rows[0]['typed']['d']['value']['finite']==Decimal('12.1250')
    assert rows[0]['typed']['ts']['value']['finite'].microsecond==123456
    assert all(batch['raw_bytes']<=h.limits.batch_bytes for batch in batches)
    assert all(r['closed'] for r in arrows) and max(arrows[0]['actual_bytes'])>h.limits.batch_bytes
    save(b['out']/'宽行与实际资源.json',{'raw_text_bytes':size,'arrow':arrows,'receipt':stream.receipt})


def test_own_b1_4c_626_tokens_bulk_and_rebuild_without_import(bound,monkeypatch):
    b=bound; root=b['root']; h=History(b['admin_dsn'],root)
    indexes=[('b1','交付索引.json','资源与组件'),('4c','repair-b1c55d2/交付索引.json','资源')]
    tokens=[]
    for version,file,key in indexes:
        path=root/file
        if not path.exists(): pytest.skip('需要本任务旧人工Token证据，不借其他数据库')
        for item in json.loads(path.read_text())[key].values(): tokens.append((version,Token(**item['token'])))
    old_path=root/'acceptance-9984ffb3d02b4920bd4a093fcf25329b'/'交付回执.json'
    if not old_path.exists(): pytest.skip('需要本任务626人工Token证据')
    old=json.loads(old_path.read_text())
    tokens.extend(('626',Token(**old[key])) for key in ('pg_token','sqlite_token'))
    monkeypatch.setattr(h,'import_package',lambda *a:pytest.fail('旧Token不得重导'))
    proof=[]
    for version,token in tokens:
        component=h.component(token)
        with h.bulk(token,batch_rows=2) as stream: batches=list(stream)
        for ti,table in enumerate(component['original']['tables']):
            rows=[r for batch in batches if batch['table_index']==ti for r in batch['rows']]
            assert [r['occurrence']['ordinal'] for r in rows]==list(range(table['rows']))
            assert hashlib.sha256(b''.join(canonical(r['values'])+b'\n' for r in rows)).hexdigest()==table['content_sha256']
            # 已接受旧scan的raw/typed/occurrence与新批流完整相等。
            previous=[]
            for offset in range(0,table['rows'],1000): previous.extend(h.scan(token,ti,after_ordinal=offset-1,limit=min(1000,table['rows']-offset),purpose='audit'))
            from data_pipeline.history.database_import.store import _equal
            assert _equal(rows,previous)
        copy=h.rebuild(token)
        proof.append({'version':version,'token':asdict(token),'bulk_receipt':stream.receipt,'rebuild':copy,'old_scan_and_new_bulk_full_equal':True})
    assert len(proof)==6
    save(b['out']/'六个旧Token完整兼容.json',proof)
