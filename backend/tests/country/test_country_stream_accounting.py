"""只验证现有落盘接缝；临时SQLite/模拟物理读，不访问PG。"""
from collections import Counter
from dataclasses import asdict,replace
from types import SimpleNamespace
import hashlib
import json
import sqlite3
import pytest
from data_pipeline.analysis.country_events.input_staging import SourceStore


def store(tmp_path):
    return SourceStore(tmp_path,max_rows=1,max_bytes=1,max_disk_bytes=8*1024**2,
                       max_row_bytes=1024**2,batch_rows=2,guard=lambda:None)


def test_disk_history_time_indexes_and_repeated_queries_ignore_totals(tmp_path):
    from tests.country.test_country_m3_temporal import fixture
    from tests.observations.test_canonical_projection import key
    from data_pipeline.analysis.country_events.route_history import CanonicalHistory
    from data_pipeline.analysis.country_events.route_time_index import state_before
    from data_pipeline.bgp.replay.snapshot_contract import encode, decode
    old,messages=fixture();s=store(tmp_path)
    try:
        for v in old.tables:s.add_view(('canonical',v),(decode(encode(r)) for r in old.rows[v]))
        new=CanonicalHistory(old.binding,max_rows=1,max_bytes=1,max_gap_candidates=1,guard=s.check,
                             store=s,original_rows={v:s['canonical',v] for v in old.tables})
        for v in old.tables:
            for row in s['canonical',v]:new.append(v,row)
        new.seal()
        for at in (101500000,101900000,102000000,102000001)*2:
            assert state_before(new,messages,key(),at)==state_before(old,messages,key(),at)
        assert new.stats['rows']>1 and new.stats['temporal_rows']>1 and new.stats['time_queries']>1
    finally:s.close()
    assert not list(tmp_path.iterdir())


def test_disk_detection_history_keeps_exact_chain_beyond_total(tmp_path):
    from tests.country.test_country_m3_results import fixture
    from data_pipeline.analysis.country_events.qualified_event_results import country_results
    args=fixture();expected=country_results(*args,max_rows=100,guard=lambda:None);s=store(tmp_path)
    try:
        for n,rows in enumerate(args[:4]):s.add_view(('d',n),rows)
        actual=country_results(*(s['d',n] for n in range(4)),args[4],max_rows=1,guard=s.check,store=s)
        assert tuple(actual)==expected
        with pytest.raises(ValueError,match='result_chain'):country_results(*args,max_rows=1,guard=lambda:None)
    finally:s.close()


def test_c2_staged_changes_and_bytes_are_counts(tmp_path):
    from data_pipeline.analysis.country_events.aggregation_staging import Stage, load
    from data_pipeline.analysis.country_events.compute import Change
    from data_pipeline.analysis.country_events.models import Binding, Cursor, Time
    s=Stage(tmp_path/'stage.sqlite',Binding('r','1',('s',),'ref','decoder','state','rrc25'),lambda:None,
            dict(max_row_bytes=4096,max_staged_rows=1,max_changes=1,max_objects=10))
    changes=[Change(Cursor(0,i,0),Time(i+1,0),'s',str(i)) for i in range(6)]
    try:
        for row in changes:s.add_change(row)
        assert s.counts['input_units']==6 and s.counts['staged_rows']>=6
        assert [load(row[0]) for row in s.db.execute('SELECT payload FROM unsorted_units ORDER BY record')]==changes
    finally:s.close()


def test_disk_qualification_and_audit_keep_full_typed_output(tmp_path):
    from tests.country.test_country_m3_qualification import fixture
    from data_pipeline.analysis.country_events.event_aggregation import C2Row, InputEvidence
    from data_pipeline.analysis.country_events.snapshot_schema import encode
    from data_pipeline.analysis.country_events.result_qualification import qualification_rows
    from data_pipeline.analysis.country_events.route_reference_audit import M3RowAudit
    from data_pipeline.analysis.country_events.qualified_store import AUDIT_LIMITS
    from data_pipeline.analysis.country_events.route_contract import DIMENSIONS
    source,raw=fixture();raw=raw[:-1]+[C2Row(None,None,InputEvidence('m3_original',str(i),{'raw':'x'*1024})) for i in range(100)]+raw[-1:]
    expected=qualification_rows(source,'logical',raw,max_rows=10000,max_bytes=4*1024**2,max_references_per_row=128,guard=lambda:None)
    s=store(tmp_path);source.store=s
    from data_pipeline.analysis.country_events.input_staging import Lookup
    s.add_view(('fixture','changes'),source.get('canonical','changes'))
    source.unique=lambda rows,key:Lookup(s['fixture','changes'],key)
    actual=None;a=None
    try:
        actual=qualification_rows(source,'logical',iter(raw),max_rows=20,max_bytes=65536,max_references_per_row=128,guard=s.check)
        assert [encode(asdict(r)) for r in actual]==[encode(asdict(r)) for r in expected]
        assert actual.costs['input_rows']>20 and actual.costs['logical_bytes']>65536
        a=M3RowAudit('logical',source.binding.input_binding_id,{((15000000,31000000),d) for d in DIMENSIONS},
            **(AUDIT_LIMITS|dict(max_rows=20,max_bytes=32768,max_intervals=1,max_total_references=1,max_overlap_steps=1)),guard=s.check,store=s)
        for seq,item in enumerate([*raw,*actual]):a.append(seq,item)
        stats=a.finish();assert stats['rows']>20 and stats['references']>1 and stats['intervals']>1
        with pytest.raises(ValueError,match='resident'):
            qualification_rows(source,'logical',iter(raw),max_rows=1,max_bytes=65536,max_references_per_row=128,guard=s.check)
    finally:
        if a:a.close()
        if actual:actual.close()
        expected.close();s.close()


def test_sql_steps_count_and_preserve_resource_or_cancel_primary(tmp_path):
    from data_pipeline.analysis.country_events.selection_index import counted_progress
    with sqlite3.connect(tmp_path/'sql.sqlite') as db:
        stats=Counter();cb,errors=counted_progress(stats,lambda:None,step=1);db.set_progress_handler(cb,1)
        assert db.execute('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100) SELECT sum(x) FROM n').fetchone()[0]==5050
        assert stats['index_steps']>1 and not errors
        for primary in (ValueError('resource_limit:fixture'),KeyboardInterrupt('cancel')):
            def reject():raise primary
            cb,errors=counted_progress(stats,reject,step=1);db.set_progress_handler(cb,1)
            with pytest.raises(sqlite3.OperationalError,match='interrupt'):db.execute('SELECT 2')
            assert errors==[primary]
        db.set_progress_handler(None,0)


def test_c3_append_flush_retains_order_and_digest_beyond_totals(tmp_path):
    from data_pipeline.analysis.country_events.snapshot_store import ComponentWriter, LIMITS
    from data_pipeline.analysis.country_events import snapshot_audit as component_audit
    from data_pipeline.analysis.country_events.event_aggregation import C2Row, InputEvidence
    from data_pipeline.analysis.country_events.snapshot_schema import encode, decode, row_encode
    writer=ComponentWriter.__new__(ComponentWriter)
    writer.limits=LIMITS|dict(max_rows=1,max_event_rows=1,batch_rows=2)
    writer.root=tmp_path;writer.c1=None;writer.stats=Counter();writer.counts=Counter();writer.row_widths=Counter()
    writer.digests={t:hashlib.sha256() for t in writer.tables};writer.count=0;writer.completion=None;writer.pending=[];writer.pending_bytes=0
    writer.spool=sqlite3.connect(tmp_path/'rows.sqlite')
    writer.spool.execute('CREATE TABLE rows(sequence INTEGER,table_name TEXT,incident TEXT,revision TEXT,payload TEXT,row_hash TEXT,byte_count INTEGER)')
    component_audit.initialize(writer.spool)
    rows=[C2Row('event',1,InputEvidence('fixture',str(i),{'raw':b'x'})) for i in range(7)]
    try:
        for item in rows:writer.append(item)
        writer.flush()
        saved=list(writer.spool.execute('SELECT sequence,payload FROM rows ORDER BY sequence'))
        assert [(seq,decode(payload)) for seq,payload in saved]==[(seq,row_encode(seq,item)[1]) for seq,item in enumerate(rows)]
        assert writer.stats['peak_batch_rows']<=2 and writer.count==7
        h=hashlib.sha256()
        for seq,item in enumerate(rows):h.update(bytes.fromhex(row_encode(seq,item)[1]['_row_hash']))
        assert writer.digests['input_evidence'].hexdigest()==h.hexdigest()
    finally:writer.spool.close()


def public_reader_fixture(tmp_path,monkeypatch):
    from data_pipeline.analysis.country_events import qualified_reader as qr, result_admission as p, qualified_schema as m3_schema
    from data_pipeline.analysis.country_events.event_aggregation import C2Row, InputEvidence
    root=tmp_path/'component';root.mkdir();(root/'manifest.json').write_text('{}')
    rows=[C2Row('event',1,InputEvidence('fixture',str(i),{'raw':b'x'})) for i in range(6)]
    with sqlite3.connect(tmp_path/'index.sqlite') as db:
        db.execute('CREATE TABLE rows(sequence INTEGER,table_name TEXT,row_group INTEGER,row_offset INTEGER,row_hash TEXT)')
        for seq,item in enumerate(rows):
            table,row=m3_schema.row_encode(seq,item);db.execute('INSERT INTO rows VALUES (?,?,?,?,?)',(seq,table,0,seq,row['_row_hash']))
    closed=[]
    class Access:
        def __init__(self,*args,**kw):self.stats=Counter()
        def read(self,locators):return {l[3]:rows[l[3]] for l in locators}
        def close(self):closed.append(True)
    monkeypatch.setattr(qr,'M3Access',Access)
    binding=SimpleNamespace(root=tmp_path,component=SimpleNamespace(root=root),result_id='result',read_model_id='read')
    descriptor=SimpleNamespace(binding=binding,window_us=(10,20),logical_run_id='logical',input_binding_id='input',entities=(),event_count=1,admitted_empty=False)
    runtime=SimpleNamespace(dsn=None,limits=qr.ResultLimits(max_rows=1,max_references=1,max_result_bytes=1,max_index_steps=1,batch_rows=2),_read_stats=Counter(),resource_guard=lambda:None,event=lambda *a,**kw:None)
    monkeypatch.setattr(p,'_descriptor',lambda *_:descriptor)
    monkeypatch.setattr(p,'verify_current',lambda *a,**kw:None)
    scope=dict(window_us=(10,20),dimension=None,incident_id=None,revision=None,table='input_evidence',after_sequence=-1,stop_sequence=None)
    request=dict(view='raw',scope_typed=p.typed(scope),codec_version=p.CODEC,batch_rows=2,batch_bytes=4096)
    return p,qr,runtime,descriptor,request,rows,closed


def test_public_queries_share_accounting_with_complete_receipts(tmp_path,monkeypatch):
    p,qr,runtime,descriptor,request,rows,closed=public_reader_fixture(tmp_path,monkeypatch)
    from data_pipeline.analysis.country_events import qualified_schema as m3_schema
    expected=[dict(table=m3_schema.row_encode(i,r)[0],row=m3_schema.row_encode(i,r)[1]) for i,r in enumerate(rows)]
    for _ in range(2):
        with p.open_reader(runtime,{'admission_id':'fixture'},request,guard=lambda:None) as session:
            actual=[r for batch in session for r in p.untyped(batch['rows_typed'])]
        assert actual==expected and session.receipt['rows']==6
        h=hashlib.sha256()
        for row in expected:
            data=p.typed(row).encode();h.update(len(data).to_bytes(8,'big'));h.update(data)
        assert h.hexdigest()==session.receipt['typed_digest']
    scope=p.untyped(request['scope_typed']);scope['table']='observation_fact'
    with p.open_reader(runtime,{'admission_id':'fixture'},dict(request,scope_typed=p.typed(scope)),guard=lambda:None) as session:
        assert list(session)==[]
    assert session.receipt['rows']==0 and runtime._read_stats['rows']==12 and runtime._read_stats['bytes']>1
    assert len(closed)==3


@pytest.mark.parametrize('failure',['early','tail'])
def test_public_query_failure_has_no_tail_receipt(tmp_path,monkeypatch,failure):
    p,qr,runtime,descriptor,request,rows,closed=public_reader_fixture(tmp_path,monkeypatch)
    if failure=='early':
        with p.open_reader(runtime,{'admission_id':'fixture'},request,guard=lambda:None) as session:next(session)
    else:
        calls=[]
        def current(*a,**kw):
            calls.append(True)
            if len(calls)==2:raise ValueError('tail failed')
        monkeypatch.setattr(p,'verify_current',current)
        with pytest.raises(ValueError,match='tail failed'):
            with p.open_reader(runtime,{'admission_id':'fixture'},request,guard=lambda:None) as session:list(session)
    assert session.receipt is None and closed==[True]


def test_reader_sql_callback_restores_cancel_primary(tmp_path,monkeypatch):
    p,qr,runtime,descriptor,request,rows,closed=public_reader_fixture(tmp_path,monkeypatch)
    source=SimpleNamespace(check=lambda:None)
    reader=qr.ResultReader(None,descriptor,source,runtime.limits)
    primary=KeyboardInterrupt('cancelled')
    def cancel():raise primary
    source.check=cancel
    def query(scope):
        yield from reader.db.execute('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n')
    monkeypatch.setattr(reader,'_rows',query)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:list(reader.rows({}))
        assert caught.value is primary and reader.stats['index_steps']>=1000
    finally:reader.close()


@pytest.mark.parametrize('limit',['max_disk_bytes','max_rss_bytes'])
def test_sql_callback_uses_actual_physical_budget(tmp_path,limit):
    from data_pipeline.analysis.country_events.selection_index import Budget, counted_progress
    from data_pipeline.analysis.country_events.selection_contract import QueryLimits
    (tmp_path/'occupied').write_bytes(b'data')
    with sqlite3.connect(tmp_path/'physical.sqlite') as db:
        budget=Budget(tmp_path,replace(QueryLimits(),**{limit:1}))
        cb,errors=counted_progress(Counter(),budget,step=1);db.set_progress_handler(cb,1)
        with pytest.raises(sqlite3.OperationalError,match='interrupt'):db.execute('SELECT 1')
        assert str(errors[0])=='resource_limit:C4_working_set'
        db.set_progress_handler(None,0)


def test_c4_alias_and_event_streams_keep_complete_digest(tmp_path):
    from data_pipeline.analysis.country_events.selection_index import iter_country_aliases, iter_country_events
    from data_pipeline.analysis.country_events.selection_contract import QueryLimits, QueryReadReceipt
    from data_pipeline.analysis.country_events.snapshot_schema import encode
    from tests.country.test_country_m3_qualification import fixture
    head=fixture()[1][0].value
    values=[{'id':str(i),'raw':b'x'} for i in range(6)]
    events=[replace(head,incident=replace(head.incident,incident_id=str(i))) for i in range(3)]
    component=tmp_path/'component';component.mkdir()
    descriptor=SimpleNamespace(entities=(),read_binding=SimpleNamespace(root=tmp_path,component=SimpleNamespace(root=component),read_model_id='fixture'),event_index_json=json.dumps(dict(rows=3)))
    with sqlite3.connect(tmp_path/'index.sqlite') as db:
        db.execute('CREATE TABLE meta(key TEXT,value TEXT)');db.execute('INSERT INTO meta VALUES (?,?)',('aliases_source','{"fixture":true}'))
        db.execute('CREATE TABLE aliases(source TEXT,table_name TEXT,ref_kind TEXT,id TEXT,ordinal INTEGER,payload TEXT)')
        db.executemany('INSERT INTO aliases VALUES (?,?,?,?,?,?)',[('s','t','r',str(i),i,encode(v)) for i,v in enumerate(values)])
        db.execute('CREATE TABLE events(event TEXT,revision TEXT,status TEXT)')
        db.executemany('INSERT INTO events VALUES (?,?,?)',[(e.incident.incident_id,str(e.incident.revision),encode(e)) for e in events])
    limits=replace(QueryLimits(),max_rows=1,max_index_steps=1)
    for fn,expected in ((iter_country_aliases,values),(iter_country_events,events)):
        out=list(fn(descriptor,limits=limits));assert out[:-1]==expected and isinstance(out[-1],QueryReadReceipt)
        assert out[-1].rows==len(expected)
        early=fn(descriptor,limits=limits);assert next(early)==expected[0];early.close()
        with pytest.raises(ValueError,match='working_set'):list(fn(descriptor,limits=replace(limits,max_rss_bytes=1)))
