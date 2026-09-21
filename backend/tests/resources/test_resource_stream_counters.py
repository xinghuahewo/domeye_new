"""仅测试流式控制与资源保护；不连接PG，不证明实际准入。"""
import hashlib
from types import SimpleNamespace

import pytest

from data_pipeline.analysis.resources import publication as p
from data_pipeline.analysis.resources import publication_validation as v
from data_pipeline.analysis.resources.publication_codec import CODEC, typed, untyped


@pytest.fixture
def runtime(tmp_path):
    return p.Runtime('fixture-unused',(tmp_path,),tmp_path,None,(),fixture_only=True,
                     max_rows=2,max_bytes=512)


def setup_reader(monkeypatch, rows, *, error=None, close_error=None, tail_error=False):
    checks=[];closed=[]
    def current(*args,**kwargs):
        checks.append(True)
        if tail_error and len(checks)==2:raise ValueError('tail')
    def selected(*args):
        try:
            yield from rows
            if error:raise error
        finally:
            closed.append(True)
            if close_error:raise close_error
    monkeypatch.setattr(p,'verify_current',current)
    monkeypatch.setattr(v,'selected_rows',selected)
    return checks,closed


def request(batch_bytes=128):
    return dict(view='metrics',scope_typed=typed({'scope':'all'}),codec_version=CODEC,
                batch_rows=2,batch_bytes=batch_bytes)


def admission():return dict(admission_id='fixture',owner_binding=typed({}))


def test_stream_crosses_both_old_totals_preserves_order_digest(runtime,monkeypatch):
    rows=[{'n':i,'text':'x'*40} for i in range(12)]
    checks,closed=setup_reader(monkeypatch,rows)
    with p.open_reader(runtime,admission(),request(),guard=lambda:None) as s:
        batches=list(s)
        assert s.receipt is None
    assert [r for b in batches for r in untyped(b['rows_typed'])]==rows
    assert all(b['rows']<=2 and b['bytes']<=128 for b in batches)
    assert s.rows>runtime.max_rows and s.bytes>runtime.max_bytes
    assert s.receipt['rows']==12 and s.receipt['typed_digest']==hashlib.sha256(
        b''.join((typed(r)+'\n').encode() for r in rows)).hexdigest()
    assert s.receipt['execution']=='complete' and len(checks)==2 and closed==[True]


def test_large_singleton_and_exact_resident_boundary(runtime,monkeypatch):
    large={'text':'x'*200};runtime.max_bytes=len(typed([large]).encode())
    rows=[{'n':0},large,{'n':2}]
    setup_reader(monkeypatch,rows)
    with p.open_reader(runtime,admission(),request(runtime.max_bytes),guard=lambda:None) as s:batches=list(s)
    assert [untyped(b['rows_typed']) for b in batches]==[[rows[0]],[large],[rows[2]]]
    assert batches[1]['bytes']==runtime.max_bytes and s.receipt


@pytest.mark.parametrize('failure',['single','source','tail','close','early','caller'])
def test_failures_and_early_stop_have_no_complete_receipt(runtime,monkeypatch,failure):
    rows=[{'n':i} for i in range(6)]
    if failure=='single':rows=[{'text':'x'*runtime.max_bytes}]
    checks,closed=setup_reader(monkeypatch,rows,
        error=ValueError('source') if failure=='source' else None,
        close_error=ValueError('close') if failure=='close' else None,
        tail_error=failure=='tail')
    def consume():
        nonlocal session
        with p.open_reader(runtime,admission(),request(),guard=lambda:None) as session:
            if failure=='early':next(session)
            elif failure=='caller':next(session);raise ValueError('caller')
            else:list(session)
    session=None
    if failure=='early':consume()
    else:
        with pytest.raises(ValueError):consume()
    assert session.receipt is None and closed==[True]


def test_audit_cumulative_only_but_resident_cap_retained(runtime):
    calls=[];a=v.Account(runtime,lambda:calls.append(True))
    row={'x':'y'*100}
    for _ in range(20):a.add(row)
    assert a.rows==20 and a.bytes==20*len(typed(row).encode()) and len(calls)==20
    retained=v.ResidentAccount(runtime,lambda:None)
    retained.add(row);retained.add(row)
    with pytest.raises(ValueError,match='驻留集合'):retained.add(row)
    runtime.max_rows=100;runtime.max_bytes=1
    with pytest.raises(ValueError,match='驻留集合'):v.ResidentAccount(runtime,lambda:None).add(row)


@pytest.mark.parametrize('kind',['rss','disk','caller'])
def test_actual_resource_and_caller_guards_still_interrupt(runtime,monkeypatch,kind):
    setup_reader(monkeypatch,[{'n':i} for i in range(10)])
    calls=[]
    def guard():
        calls.append(True)
        if len(calls)==3:
            if kind=='rss':runtime.max_rss_bytes=1
            elif kind=='disk':monkeypatch.setattr(p.shutil,'disk_usage',lambda _:SimpleNamespace(free=0))
            else:raise ValueError('caller guard')
    with pytest.raises(ValueError):
        with p.open_reader(runtime,admission(),request(),guard=guard) as s:list(s)
    assert s.receipt is None


@pytest.mark.parametrize('over_envelope',[False,True])
def test_resource_to_existing_publication_envelope(runtime,monkeypatch,over_envelope):
    from contextlib import contextmanager
    from data_pipeline.results.component_readers import Admissions, digest
    from data_pipeline.results.stream_policy import consume
    row={'text':'x'*200}
    envelope_size=len(typed([row]).encode())-(1 if over_envelope else 0)
    setup_reader(monkeypatch,[row])
    original=p.open_reader;owner_sessions=[]
    @contextmanager
    def capture(*args,**kwargs):
        with original(*args,**kwargs) as owner:
            owner_sessions.append(owner)
            yield owner
    monkeypatch.setattr(p,'open_reader',capture)
    a=dict(owner='resource',owner_binding=typed({}),dependencies=[],lock_targets=[])
    a['admission_id']=digest(a)
    graph=Admissions([a],{a['admission_id']:runtime},guard=lambda:None)
    count,size=graph.envelope(a['admission_id'],batch_rows=2,max_row_bytes=envelope_size)
    req=request(size);req['batch_rows']=count
    consumed=[];stream=None
    def run():
        nonlocal stream
        with graph.read(a['admission_id'],req,max_rows=100,max_bytes=10000) as stream:
            for source_batch in stream:
                assert source_batch['bytes']<=size
                consumed.extend(consume('resource',source_batch,batch_rows=2,batch_bytes=100,envelope_bytes=size))
    if over_envelope:
        with pytest.raises(ValueError,match='来源请求封装'):run()
        assert stream.receipt is None and owner_sessions[0].receipt is None
    else:
        run()
        assert len(consumed)==1 and consumed[0]['batch']['rows']==1
        assert consumed[0]['batch']['bytes']>100
        assert untyped(consumed[0]['batch']['rows_typed'])==[row]
        assert stream.receipt==owner_sessions[0].receipt
        assert stream.receipt['request_digest']==digest(req)
        assert stream.receipt['execution']=='complete' and stream.receipt['rows']==1
