"""显式授权的人工实际载体检查；普通测试不选数据库、不造资格。"""
from contextlib import contextmanager,closing
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import json,os,sqlite3
import pytest
from tests.country_trend.country_trend_s2_support import runtime, TrendBinding
from data_pipeline.analysis.country_trends.snapshot_reader import TrendReader, TrendReadReceipt, verify_trend
from data_pipeline.analysis.country_trends.snapshot_schema import encode, decode, row_hash
from data_pipeline.analysis.country_trends import snapshot_audit as audit
from data_pipeline.analysis.country_trends.compute import event_rows
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits, SharedBudget


@pytest.fixture(scope='module')
def saved():
    source=os.environ.get('DOMEYE_C5_S2_ARTIFICIAL_REQUEST')
    if not source:pytest.skip('仅显式绑定已授权人工实际载体')
    q=json.loads(Path(source).read_text());p=json.loads(Path(q['receipt']).read_text())
    rt=runtime(q);b=TrendBinding(**p['binding'])
    reader=TrendReader(b,p['proof'],runtime=rt)
    stream=list(reader.scan());assert isinstance(stream[-1],TrendReadReceipt)
    return SimpleNamespace(q=q,b=b,p=p['proof'],rt=rt,rows=stream[:-1],manifest=reader.manifest)


def test_original_full_stream_and_pure_science_oracle(saved):
    from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch, ReadReceipt
    from data_pipeline.analysis.country_events.selection_contract import contract_value
    from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_schema as c3
    descriptor=contract_value(saved.q['country_descriptor'])
    originals=[];events={}
    with closing(ComponentReader(saved.rt.country.component_dsn,descriptor.read_binding.component,c1=saved.rt.country.c1).stream()) as stream:
        for batch in stream:
            if isinstance(batch,ReadReceipt):assert batch.rows==len(originals);continue
            assert isinstance(batch,ReadBatch)
            for r in batch.rows:
                originals.append(c3.encode(r if isinstance(r,c2.C2Completion) else (r.incident_id,r.revision,r.value)))
                if isinstance(r,c2.C2Row) and r.incident_id is not None:events.setdefault((r.incident_id,r.revision),[]).append(r.value)
    assert originals==[r.get('raw_typed') for r in saved.rows if r.kind=='raw_source']
    activities,refs=decode(saved.manifest['input_identity']['contexts'])
    request=SimpleNamespace(activities=activities,references=refs)
    for event,values in events.items():
        # 同一未改科学核默认分支作精确有理数/顺序oracle，正式接合只增加资格元数据。
        expected=list(event_rows(event,values,request,S2Limits()))
        kinds={r.kind for r in expected}
        actual=[replace(r,result_id='') for r in saved.rows if r.event==event and r.kind in kinds]
        normalized=[]
        for r in actual:
            if r.kind=='event':r=replace(r,values=tuple((k,'fixture_calculation' if k=='basis' else v) for k,v in r.values))
            if r.kind=='activity_context' and saved.q.get('feature_receipt'):r=replace(r,values=(('state','not_provided'),))
            normalized.append(r)
        assert normalized==expected


def test_actual_nonempty_contexts(saved):
    if not saved.q.get('reference_binding'):pytest.skip('本候选未选参照')
    counts={k:sum(r.kind==k for r in saved.rows) for k in ('reference_cdf','reference_common','reference_shape','activity_relation','activity_window')}
    assert counts==dict(reference_cdf=6,reference_common=6,reference_shape=2,activity_relation=6,activity_window=4)
    for r in saved.rows:
        if r.kind=='reference_cdf':assert r.get('comparable_count')==2 and r.get('percentile') is not None
        if r.kind=='reference_common':assert r.get('declining_count')==0 and r.get('causal_claim') is False
        if r.kind=='activity_relation':assert r.get('causal_claim') is False


@pytest.mark.parametrize('size',[1,7,256])
def test_public_pages(saved,size):
    reader=TrendReader(saved.b,saved.p,runtime=saved.rt)
    rows=list(reader.iter_query('reference_cdf',page_size=size))
    assert isinstance(rows[-1],TrendReadReceipt) and rows[-1].full_body_validated is False
    assert rows[:-1]==[r for r in saved.rows if r.kind=='reference_cdf']
    assert reader.stats['full_scans']==0


def test_event_cursor_and_whitelist(saved):
    reader=TrendReader(saved.b,saved.p,runtime=saved.rt)
    page=reader.query('metric',limit=1)
    assert page.next_cursor
    with pytest.raises(ValueError,match='cursor'):reader.query('peak',cursor=page.next_cursor)
    with pytest.raises(ValueError,match='unknown_event'):reader.query('event',event=('wrong',1))
    with pytest.raises(ValueError,match='scope'):reader.query('select *')
    with pytest.raises(ValueError,match='scope'):reader.query('metric',limit=257)


@contextmanager
def control_change(dsn,statement,args,restore,restore_args):
    import psycopg2
    pg=psycopg2.connect(dsn)
    try:
        with pg,pg.cursor() as c:c.execute(statement,args)
        yield
    finally:
        with pg,pg.cursor() as c:c.execute(restore,restore_args)
        pg.close()


def test_control_revocation_and_tail(saved):
    reader=TrendReader(saved.b,saved.p,runtime=saved.rt)
    stream=reader.scan();first=next(stream);assert not isinstance(first,TrendReadReceipt)
    change="UPDATE country_trends.components SET state=%s WHERE component_id=%s"
    with control_change(saved.rt.component_dsn,change,('revoked',saved.b.component_id),change,('complete',saved.b.component_id)):
        with pytest.raises(ValueError,match='control_anchor'):reader.query('result')
        emitted=[]
        with pytest.raises(ValueError,match='control_anchor'):
            for r in stream:emitted.append(r)
        assert not any(isinstance(r,TrendReadReceipt) for r in emitted)
    with verify_trend(saved.b,saved.p,saved.rt,lock=True):pass


def test_reference_revocation(saved):
    ref=saved.q.get('reference_binding')
    if not ref:pytest.skip('无参照')
    reader=TrendReader(saved.b,saved.p,runtime=saved.rt)
    change='UPDATE country_trends.references SET state=%s WHERE reference_id=%s'
    with control_change(saved.rt.reference_dsn,change,('revoked',ref['sha256']),change,('complete',ref['sha256'])):
        with pytest.raises(ValueError,match='reference_registration'):reader.query('result')


def test_manifest_binding_and_proof_damage(saved):
    with pytest.raises(ValueError,match='manifest_hash'):
        TrendReader(replace(saved.b,manifest_sha256='0'*64),saved.p,runtime=saved.rt)
    with pytest.raises(ValueError,match='schema_proof'):
        TrendReader(saved.b,{**saved.p,'output_rows':saved.p['output_rows']+1},runtime=saved.rt)


@pytest.mark.parametrize('damage',['missing_target','cross_event','raw_value','result_count','raw_hash','graph_missing'])
def test_relational_damage_even_with_recomputed_row_hash(saved,damage):
    rows=list(saved.rows);events=[r.event for r in rows if r.kind=='event']
    if damage=='graph_missing':rows=[r for r in rows if not (r.kind=='evidence' and r.get('source_kind')=='phase')]
    else:
        for i,r in enumerate(rows):
            if damage in ('missing_target','cross_event') and r.kind=='evidence_source' and r.event==events[0]:
                target=r.get('source_locator')
                target=(target[0],events[1] if damage=='cross_event' else target[1],target[2] if damage=='cross_event' else ('missing',))
                rows[i]=replace(r,values=(('source_locator',target),));break
            if damage=='raw_value' and r.kind=='metric':
                raw=r.get('raw');rows[i]=replace(r,values=(('raw',replace(raw,value=999)),));break
            if damage in ('result_count','raw_hash') and r.kind=='result':
                rows[i]=replace(r,values=tuple((k,999 if k=='event_count' and damage=='result_count' else '0'*64 if k=='input_sha256' and damage=='raw_hash' else v) for k,v in r.values));break
    with sqlite3.connect(':memory:') as db:
        audit.initialize(db)
        with pytest.raises((ValueError,sqlite3.IntegrityError)):
            for r in rows:row_hash(r);audit.append(db,r)
            audit.validate(db,saved.p['events'],saved.p['input_rows'],lambda:None)


def test_early_close_and_cleanup_failure(saved,monkeypatch):
    from data_pipeline.analysis.country_trends import snapshot_reader as module
    reader=TrendReader(saved.b,saved.p,runtime=saved.rt)
    original=module.tempfile.TemporaryDirectory
    class FailingTemporary:
        def __init__(self,*a,**kw):self.value=original(*a,**kw);self.name=self.value.name
        def cleanup(self):self.value.cleanup();raise OSError('injected cleanup')
    monkeypatch.setattr(module.tempfile,'TemporaryDirectory',FailingTemporary)
    stream=reader.scan();assert not isinstance(next(stream),TrendReadReceipt)
    with pytest.raises(OSError,match='injected cleanup'):stream.close()
    assert not reader.budget.scratch_roots


def test_shared_read_budgets(saved):
    with pytest.raises(ValueError,match='row_budget'):
        reader=TrendReader(saved.b,saved.p,runtime=saved.rt,limits=S2Limits(max_rows=1))
        list(reader.scan())
    reader=TrendReader(saved.b,saved.p,runtime=saved.rt,limits=S2Limits(max_index_steps=1))
    with pytest.raises(ValueError,match='index_budget'):reader.query('metric',limit=1)


@pytest.mark.parametrize('fault',['row_budget','writer_close','audit_close','final_qualification_close'])
def test_candidate_failure_never_returns_admitted_receipt(saved,monkeypatch,fault):
    import uuid,psycopg2
    from data_pipeline.analysis.country_trends import snapshot_store as store, execution_identity as s2_identity, snapshot_reader as s2_reader
    from data_pipeline.analysis.country_events.selection_contract import contract_value
    # 仅异常路径注入；不构造任何可完成的正式入口/回执，不用于正例验收。
    monkeypatch.setattr(s2_identity,'execution_identity',lambda:{'negative_test':'must_fail'})
    output=Path(saved.b.root).parent/('negative-'+fault+'-'+uuid.uuid4().hex)
    if fault=='writer_close':
        connect=store.lake_connect
        class FailClose:
            def __init__(self,db):self.db=db
            def __getattr__(self,k):return getattr(self.db,k)
            def close(self):self.db.close();raise OSError('injected writer close')
        monkeypatch.setattr(store,'lake_connect',lambda *a,**k:FailClose(connect(*a,**k)))
    elif fault=='audit_close':
        original=s2_reader.audit_lake
        def bad(*a,**k):original(*a,**k);raise OSError('injected audit close')
        monkeypatch.setattr(s2_reader,'audit_lake',bad)
    elif fault=='final_qualification_close':
        original=store.verify_inputs
        @contextmanager
        def bad(*a,**k):
            with original(*a,**k):yield
            if k.get('lock'):raise OSError('injected final qualification close')
        monkeypatch.setattr(store,'verify_inputs',bad)
    expected={'row_budget':'trend_row_budget|resource_limit:C3_read','writer_close':'injected writer close','audit_close':'injected audit close','final_qualification_close':'injected final qualification close'}[fault]
    with pytest.raises((ValueError,OSError),match=expected):
        store.prepare_trend(contract_value(saved.q['country_descriptor']),saved.rt,output,limits=S2Limits(max_rows=1) if fault=='row_budget' else S2Limits())
    pg=psycopg2.connect(saved.rt.component_dsn)
    try:
        with pg.cursor() as c:
            c.execute('SELECT state,binding FROM country_trends.components WHERE root=%s',(str(output),))
            assert c.fetchone()==('failed',None)
    finally:pg.close()
    if fault!='final_qualification_close':assert not (output/'ready.json').exists()
