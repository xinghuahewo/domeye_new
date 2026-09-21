"""临时SQLite与模拟公开会话；不访问PG或构造真实Admission。"""
from contextlib import contextmanager
from types import SimpleNamespace
import hashlib
import json
import pytest
from data_pipeline.analysis.country_events import route_inputs as mi
from data_pipeline.analysis.country_events.route_source_reader import M3Source
from data_pipeline.analysis.country_events.input_staging import SourceStore
from data_pipeline.analysis.country_events.snapshot_schema import encode
from tests.country.test_country_m3_results import fixture as result_fixture


def fixture_inputs(monkeypatch, failure=None):
    admissions=[dict(owner=o,admission_id=o,lock_targets=[],owner_binding=mi.m2.typed(
        dict(source_id='ref') if o=='reference' else dict(ordered_source_ids=['s'])))
        for o in ('m2','reference','canonical','detection')]
    inputs=mi.M3Inputs(admissions,{a['admission_id']:object() for a in admissions},
        max_rows=1,max_bytes=1,batch_rows=2,batch_bytes=4096,guard=lambda:None)
    inputs.m2_binding=dict(run_id='fixture',snapshot=1,ordered_source_ids=['s'],input_binding_id='binding',
        selected_sources=[dict(source_id='s',calculation_role='baseline')],input_binding=json.dumps(dict(
            interpretation_version='decoder',collector='rrc25',sources=[dict(source_id='s')])))
    inputs.canonical_binding=dict(descriptor=dict(plan=dict(algorithm='state')))
    inputs.detection_binding=result_fixture()[-1]
    inputs.detection_binding['identity']['reference_version']='ref'
    calls=[];closed=[];verifications=[]
    monkeypatch.setattr(inputs,'verify',lambda:verifications.append(True))
    values=[dict(message_id=f's:{i}',raw=b'x',value=2**80,missing=None,ordered=[1,1]) for i in range(6)]
    @contextmanager
    def open_reader(runtime,admission,request,*,guard):
        owner=admission['owner'];view=request['view'];calls.append((owner,view))
        pack=mi.encode if owner=='canonical' else mi.dtyped if owner=='detection' else mi.m2.typed
        rows=values if (owner,view)==('m2','messages') else values[:3] if owner=='reference' else []
        session=SimpleNamespace(receipt=None)
        def batches():
            for start in range(0,len(rows),2):
                if failure=='source' and start==2:raise ValueError('source failed')
                text=pack(rows[start:start+2]);yield dict(codec_version=request['codec_version'],rows_typed=text,rows=len(rows[start:start+2]),bytes=len(text.encode()))
        try:
            yield Session(session,batches())
            if failure=='tail':raise ValueError('tail failed')
            h=hashlib.sha256()
            for row in rows:mi.update_read_digest(h,owner,row)
            session.receipt=dict(execution='complete',rows=len(rows),admission_id=owner,
                request_digest=mi.m2.digest(request),typed_digest=h.hexdigest())
        finally:closed.append((owner,view))
    class Session:
        def __init__(self,state,iterator):self.state=state;self.iterator=iterator
        def __iter__(self):return self.iterator
        @property
        def receipt(self):return self.state.receipt
    monkeypatch.setattr(inputs,'module',lambda owner:SimpleNamespace(open_reader=open_reader))
    return inputs,values,calls,closed,verifications


def test_input_counters_do_not_stop_stream(monkeypatch):
    inputs,*_=fixture_inputs(monkeypatch)
    inputs.rows=100;inputs.bytes=10000
    inputs.check_budget()


def test_spool_row_total_is_accounting(tmp_path):
    s=SourceStore(tmp_path,max_rows=1,max_bytes=1024**2,max_row_bytes=4096,batch_rows=2,guard=lambda:None)
    try:
        s.add_view(('x','v'),[{'i':0},{'i':1}]);assert s.count==2
    finally:s.close()


def test_full_disk_capture_small_legacy_totals_keeps_all_views_and_receipts(tmp_path,monkeypatch):
    inputs,values,calls,closed,checks=fixture_inputs(monkeypatch)
    with M3Source.capture(inputs,scratch_root=tmp_path,max_rows=1,max_bytes=1,max_disk_bytes=1024**2,
                          max_row_bytes=4096,max_gap_candidates=10) as source:
        assert list(source.get('m2','messages'))==values
        assert list(source.get('reference','references'))==values[:3]
        assert source.costs==dict(original_rows=9,original_bytes=sum(len(encode(r).encode()) for r in values+values[:3]))
        assert inputs.rows==source.store.count==9 and inputs.bytes>1
        assert len(calls)==len(inputs.receipts)==25 and calls==closed
        source.verify_saved();source.store.check_ordinals();root=source.store.root
        # 驻留副本仍有实际工作集保护，不能借流式策略放行deepcopy。
        with pytest.raises(ValueError,match='M3_source_rows'):
            M3Source(inputs,{k:list(v) for k,v in source.rows.items()},inputs.receipts,
                     max_rows=1,max_bytes=1,max_gap_candidates=10)
    assert not root.exists() and not inputs.active and inputs.open_reads==0 and len(checks)==3


@pytest.mark.parametrize('failure',['source','tail'])
def test_failed_capture_has_no_complete_receipt_and_cleans(tmp_path,monkeypatch,failure):
    inputs,_,_,closed,_=fixture_inputs(monkeypatch,failure)
    with pytest.raises(ValueError,match=failure+' failed'):
        with M3Source.capture(inputs,scratch_root=tmp_path,max_rows=1,max_bytes=1,max_disk_bytes=1024**2,
                              max_row_bytes=4096,max_gap_candidates=10):pass
    assert not inputs.receipts and inputs.open_reads==0 and not inputs.active
    assert closed and not list(tmp_path.iterdir())


@pytest.mark.parametrize('kind',['legacy_disk','explicit_disk','rss','row'])
def test_physical_and_single_row_guards_still_close(tmp_path,kind):
    kw=dict(max_rows=1,max_bytes=1024**2,max_row_bytes=4096,batch_rows=2,guard=lambda:None)
    if kind=='legacy_disk':kw['max_bytes']=1
    elif kind=='explicit_disk':kw['max_disk_bytes']=1
    elif kind=='rss':kw['max_rss_bytes']=1
    else:kw['max_row_bytes']=1
    s=SourceStore(tmp_path,**kw);root=s.root
    try:
        with pytest.raises(ValueError,match={'legacy_disk':'spool_disk','explicit_disk':'spool_disk','rss':'spool_RSS','row':'single_row'}[kind]):
            s.add_view(('x','v'),[{'value':1}])
    finally:s.close()
    assert not root.exists()


@pytest.mark.parametrize('field,value',[('batch_rows',1),('batch_bytes',1)])
def test_batch_guard_before_yield_and_receipt(tmp_path,monkeypatch,field,value):
    inputs,*_=fixture_inputs(monkeypatch);setattr(inputs,field,value)
    with pytest.raises(ValueError,match='M3_input_batch'):
        with M3Source.capture(inputs,scratch_root=tmp_path,max_rows=1,max_bytes=1,max_disk_bytes=1024**2,
                              max_row_bytes=4096,max_gap_candidates=10):pass
    assert not inputs.receipts and inputs.rows==0 and inputs.open_reads==0
    assert not list(tmp_path.iterdir())
