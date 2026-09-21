"""P1 人工制品接合；真实PG必须由调用者显式提供自己获准的fixture清单。"""
import copy
import json
import os
from pathlib import Path
import time
from contextlib import contextmanager
import psycopg2
import pytest
from data_pipeline.analysis.detection import publication as d
from data_pipeline.analysis.detection import publication_io as io
from data_pipeline.analysis.detection.store import read_stored_rows, read_records
from data_pipeline.bgp.archive import admission as m


@pytest.fixture(scope='module')
def bundles():
    path = os.environ.get('DETECTION_P1_FIXTURE_CONFIG')
    if not path: pytest.skip('需要显式人工fixture清单')
    config = json.loads(Path(path).read_text())
    assert config['fixture_only'] is True
    result = []
    for item in config['fixtures']:
        q = json.loads(Path(item['request']).read_text())
        ready = json.loads((Path(item['output_root'])/'ready.json').read_text())
        events = []
        u = m.Runtime(q['observation_dsn'],tuple(config['allowed_roots']),Path(config['scratch_root']),fixture_only=True,audit_sink=events.append)
        binding = m.inspect_binding(u,q['input_run'],q['input_snapshot'],q['ordered_sources'])
        a = m.admit(u,binding,guard=lambda:None); u.dependency_admissions=(a,)
        deps = [a]+[m.admit(u,m.reference_binding(u,a,x['source_id']),guard=lambda:None) for x in q['references']]
        r = d.Runtime(q['detection_dsn'],Path(item['output_root']),tuple(config['allowed_roots']),Path(config['scratch_root']),tuple(deps),{x['admission_id']:u for x in deps},fixture_only=True,audit_sink=events.append)
        b = d.inspect_binding(r,ready['run_id'],ready['snapshot'])
        events.clear(); t=time.monotonic(); admitted=d.admit(r,b,guard=lambda:None)
        cost=dict(label=item['label'],admit_seconds=time.monotonic()-t,admit_events=copy.deepcopy(events))
        result.append((r,b,admitted,events,cost))
    yield result
    Path(config['evidence']).write_text(json.dumps([x[4] for x in result],ensure_ascii=False,indent=2))


def request(view='records', **scope):
    return dict(view=view,scope_typed=d.typed(dict(start=0,stop=None,key=None,at_position=None,**scope)),codec_version=d.CODEC,batch_rows=7,batch_bytes=4*1024**2)


def consume(r,a,req):
    with d.open_reader(r,a,req,guard=lambda:None) as s:
        rows=[]
        for batch in s:
            assert batch['bytes']==len(batch['rows_typed'].encode())<=req['batch_bytes']
            decoded=d.untyped(batch['rows_typed']);assert len(decoded)==batch['rows']<=req['batch_rows']
            rows.extend(decoded)
    assert s.receipt and s.receipt['rows']==len(rows)
    return rows,s.receipt


def test_whole_values_reuse_current_and_bounded_views(bundles):
    for r,b,a,events,cost in bundles:
        events.clear(); t=time.monotonic(); assert d.admit(r,b,guard=lambda:None)==a
        cost['reuse_seconds']=time.monotonic()-t; cost['reuse_events']=copy.deepcopy(events)
        assert not any(e['kind'] in ('full_table_scan','body_query','entity_hash') for e in events)
        events.clear(); t=time.monotonic();d.verify_current(r,a,guard=lambda:None)
        cost['current_seconds']=time.monotonic()-t;cost['current_events']=copy.deepcopy(events)
        assert not any(e['kind'] in ('full_table_scan','body_query','entity_hash') for e in events)
        cost['reads']={}
        for table in io.TABLES:
            events.clear();t=time.monotonic();first=None
            with d.open_reader(r,a,request(table),guard=lambda:None) as session:
                rows=[]
                for batch in session:
                    if first is None:first=time.monotonic()-t
                    rows.extend(d.untyped(batch['rows_typed']))
            assert session.receipt
            full_seconds=time.monotonic()-t
            original=list(read_stored_rows(r.dsn,b['run_id'],b['snapshot'],table))
            assert rows==original
            cost['reads'][table]=dict(first_batch_seconds=first,full_read_seconds=full_seconds,rows=len(rows),events=copy.deepcopy(events))
            assert sum(e['kind']=='body_query' for e in events)==1
            assert not any(e['kind'] in ('full_table_scan','entity_hash') for e in events)
        raw,_=consume(r,a,request())
        revisions,_=consume(r,a,request('revisions'))
        assert revisions==[x for x in raw if x['record_kind']=='business_revision']
        decisions,_=consume(r,a,request('decisions'))
        assert decisions==[x for x in raw if x['record_kind']=='rule_decision']
        req=request('qualified_revisions');req['scope_typed']=d.typed(dict(start=0,stop=None,key=None,at_position=b['identity']['qualification_as_of_position']))
        qualified,_=consume(r,a,req)
        from data_pipeline.analysis.detection.qualified_reader import read_qualified_revisions
        old=list(read_qualified_revisions(r.dsn,b['run_id'],b['snapshot'],expected_binding_id=b['identity']['input_binding_id'],at_position=b['identity']['qualification_as_of_position']))
        assert d.typed(qualified)==d.typed(old)
        req=request();req['scope_typed']=d.typed(dict(start=2,stop=5,key=None,at_position=None))
        assert consume(r,a,req)[0]==raw[2:5]
        # 原兼容入口仍可完整消费，未要求新的Admission。
        assert len(list(read_records(r.dsn,b['run_id'],b['snapshot'])))==len(raw)


def test_failure_close_and_no_receipt(bundles,monkeypatch):
    r,b,a,events,_=bundles[0]
    with d.open_reader(r,a,request(),guard=lambda:None) as s: next(s)
    assert s.receipt is None
    req=request();req['batch_bytes']=1
    with pytest.raises(ValueError):
        with d.open_reader(r,a,req,guard=lambda:None) as s:list(s)
    assert s.receipt is None
    original=io.rows
    def close_failure(*args):
        source=original(*args)
        try: yield from source
        finally:
            source.close();raise RuntimeError('人工真实读取资源关闭故障')
    monkeypatch.setattr(io,'rows',close_failure)
    with pytest.raises(RuntimeError,match='关闭故障'):
        with d.open_reader(r,a,request(),guard=lambda:None) as s:list(s)
    assert s.receipt is None
    with pytest.raises(LookupError) as caught:
        with d.open_reader(r,a,request(),guard=lambda:None) as s:
            next(s);raise LookupError('主异常')
    assert s.receipt is None and caught.value.cleanup_errors


def test_real_single_locks_and_tail_revocation(bundles):
    r,b,a,events,_=bundles[0]
    for target in a['lock_targets']:
        events.clear()
        with d.hold_lock(r,a,target,guard=lambda:None):
            pg=psycopg2.connect(r.dsn)
            try:
                with pg.cursor() as c:
                    c.execute("SET lock_timeout='100ms'")
                    query=('UPDATE detection.runs SET state=state WHERE run_id=%s' if target['namespace']=='detection.run' else 'UPDATE detection.publication_admissions SET state=state WHERE record_key=%s')
                    with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute(query,(target['key'],))
            finally:pg.rollback();pg.close()
        locks=[e for e in events if e['kind']=='pg_sql' and 'FOR SHARE' in e['sql']]
        assert len(locks)==1
    key=a['lock_targets'][1]['key']
    def state(value):
        pg=psycopg2.connect(r.dsn)
        try:
            with pg,pg.cursor() as c:c.execute('UPDATE detection.publication_admissions SET state=%s WHERE record_key=%s',(value,key))
        finally:pg.close()
    try:
        with pytest.raises(ValueError):
            with d.open_reader(r,a,request(),guard=lambda:None) as s:
                list(s);state('revoked')
        assert s.receipt is None
    finally:state('accepted')


@pytest.mark.parametrize('bad',[0,-1,True,float('nan'),float('inf'),float('-inf'),1.5])
def test_budget_construct_and_use(tmp_path,bad):
    kwargs=dict(dsn='unused',output_root=tmp_path,allowed_roots=(tmp_path,),scratch_root=tmp_path,dependency_admissions=(),dependency_runtimes={},fixture_only=True)
    for field in ('memory_bytes','max_temp_bytes','max_rss_bytes','lock_timeout_ms'):
        with pytest.raises(ValueError):d.Runtime(**kwargs,**{field:bad})
        r=d.Runtime(**kwargs);setattr(r,field,bad)
        with pytest.raises(ValueError):io.check(r,lambda:None)


def test_fake_and_missing_upstream(bundles):
    r,b,a,_,_=bundles[0]
    fake=copy.deepcopy(a);fake['inventory_digest']='0'*64;fake['admission_id']=d.digest({k:v for k,v in fake.items() if k!='admission_id'})
    with pytest.raises(ValueError):d.verify_current(r,fake,guard=lambda:None)
    missing=copy.copy(r);missing.dependency_admissions=r.dependency_admissions[:-1]
    with pytest.raises(ValueError):d.verify_current(missing,a,guard=lambda:None)
    drift=copy.deepcopy(b);drift['scope']['window_start']='1969-01-01T00:00:00Z'
    with pytest.raises(ValueError):d.admit(r,drift,guard=lambda:None)


def test_actual_database_close_failure(bundles,monkeypatch):
    r,b,a,_,_=bundles[0]
    connect=io.connect_duckdb
    class ClosingDB:
        def __init__(self):self.db=connect();self.body=False
        def execute(self,query,*args):
            if query.startswith('SELECT r.*'):self.body=True
            return self.db.execute(query,*args)
        def close(self):
            self.db.close()
            if self.body:raise RuntimeError('人工DuckDB关闭失败')
    monkeypatch.setattr(io,'connect_duckdb',ClosingDB)
    with pytest.raises(RuntimeError,match='DuckDB关闭失败'):
        with d.open_reader(r,a,request(),guard=lambda:None) as s:list(s)
    assert s.receipt is None
    with pytest.raises(LookupError) as caught:
        with d.open_reader(r,a,request(),guard=lambda:None) as s:
            next(s);raise LookupError('保留主异常')
    assert s.receipt is None and caught.value.cleanup_errors


@pytest.mark.parametrize('fault',['index','state','qualification'])
def test_first_admit_rechecks_scientific_body(bundles,monkeypatch,fault):
    import pyarrow as pa
    r,b,a,_,_=bundles[0]
    connect=io.connect_duckdb
    class MutatedDB:
        def __init__(self):self.db=connect();self.query=''
        def execute(self,query,*args):
            self.query=query;self.db.execute(query,*args);return self
        def fetchall(self):return self.db.fetchall()
        def fetch_record_batch(self,n):
            batches=self.db.fetch_record_batch(n);query=self.query
            class Batches:
                def __iter__(self):
                    for batch in batches:
                        rows=batch.to_pylist()
                        for row in rows:
                            if fault=='index' and '.records ' in query and row['record_kind']=='business_revision':row['attacker_asn']=999999
                            if fault=='state' and '.state_entries ' in query and row['family']=='output' and row['attribute']=='last_records':row['attribute']='wrong_last_records'
                            if fault=='qualification' and '.m3_entries ' in query and row['kind']=='event_qualification':row['incident_id']='wrong-scientific-key'
                        yield pa.RecordBatch.from_pylist(rows,schema=batch.schema)
                def close(self):batches.close()
            return Batches()
        def close(self):self.db.close()
    monkeypatch.setattr(io,'connect_duckdb',MutatedDB)
    # 只把自己的同绑定既有验收暂时撤销，让公共admit真实走完整验证；结束恢复。
    pg=psycopg2.connect(r.dsn)
    try:
        with pg,pg.cursor() as c:
            c.execute('SELECT record_key FROM detection.publication_admissions WHERE state=%s',('accepted',));keys=[str(x[0]) for x in c.fetchall()]
            c.execute("UPDATE detection.publication_admissions SET state='revoked' WHERE record_key=ANY(%s::uuid[])",(keys,))
        with pytest.raises(ValueError):d.admit(r,b,guard=lambda:None)
        with pg,pg.cursor() as c:
            c.execute("SELECT count(*) FROM detection.publication_admissions WHERE state='accepted'");assert c.fetchone()[0]==0
    finally:
        with pg,pg.cursor() as c:c.execute("UPDATE detection.publication_admissions SET state='accepted' WHERE record_key=ANY(%s::uuid[])",(keys,))
        pg.close()


def test_old_typed_api_and_reject_old_lake():
    path=os.environ.get('DETECTION_P1_FIXTURE_CONFIG')
    if not path:pytest.skip('需要显式人工fixture清单')
    config=json.loads(Path(path).read_text())
    for item in config.get('legacy',[]):
        q=json.loads(Path(item['request']).read_text());ready=json.loads((Path(item['output_root'])/'ready.json').read_text())
        r=d.Runtime(q['detection_dsn'],Path(item['output_root']),tuple(config['allowed_roots']),Path(config['scratch_root']),(),{},fixture_only=True)
        with pytest.raises(ValueError):d.inspect_binding(r,ready['run_id'],ready['snapshot'])
        if item['profile']=='typed/v1':
            rows=list(read_records(r.dsn,ready['run_id'],ready['snapshot']))
            assert len(rows)==ready['records']>0
