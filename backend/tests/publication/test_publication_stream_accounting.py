"""P累计计量的有限fixture；实际接合/聚批路径，不连接PG或读真实制品。"""
from contextlib import contextmanager
from types import SimpleNamespace
import pytest
from data_pipeline.results import component_readers as access
from data_pipeline.results.component_roles import codec
from data_pipeline.results.component_streams import read_required, requests


@pytest.fixture
def source(monkeypatch):
    c=codec('trend');a=dict(owner='trend',dependencies=[],lock_targets=[])
    a['admission_id']=access.digest(a)
    state=dict(fault=None,graph_checks=0,guard_checks=0)
    text=c.typed((dict(value='x'*300),dict(value='y'*300)))
    batch=dict(rows_typed=text,rows=2,bytes=len(text.encode()),codec_version='country-trend-typed/v2')
    @contextmanager
    def reader(runtime,admission,request,**kw):
        class Source:
            receipt=None
            def __iter__(self):
                for _ in range(3):
                    value=dict(batch)
                    if state['fault']=='source_envelope':value['rows']=request['batch_rows']+1
                    if state['fault']=='typed_count':value['rows']=1
                    yield value
        stream=Source();yield stream
        if state['fault']!='tail':
            stream.receipt=dict(contract='component-publication-read/v1',admission_id=a['admission_id'],
                request_digest=access.digest(request),rows=6,execution='complete')
    def current(*args,**kw):state['graph_checks']+=1
    def guard():state['guard_checks']+=1
    monkeypatch.setattr(access,'api',lambda owner:SimpleNamespace(verify_current=current,open_reader=reader))
    monkeypatch.setattr('psycopg2.connect',lambda *a,**k:pytest.fail('fixture不得连接PG'))
    runtime=SimpleNamespace(limits=SimpleNamespace(max_batch_rows=2,max_batch_bytes=4096))
    graph=access.Admissions([a],{a['admission_id']:runtime},guard=guard)
    return graph,a,state,batch


def test_single_reader_counts_past_both_legacy_limits(source):
    graph,a,state,batch=source
    req=requests('trend',batch_rows=2,batch_bytes=4096)[0]
    with graph.read(a['admission_id'],req,max_rows=1,max_bytes=1) as stream:
        assert list(stream)==[batch]*3
    assert (stream.rows,stream.bytes)==(6,3*batch['bytes'])
    assert stream.receipt['rows']==6 and state['guard_checks']>0


def test_multiview_totals_continue_and_keep_four_boundaries(source):
    graph,a,state,batch=source;events=[]
    graph.current() # candidate写前
    with graph.locked():
        with read_required(graph,[a],batch_rows=2,batch_bytes=1024,max_row_bytes=4096,
                           max_rows=1,max_bytes=16384,progress=events.append) as stream:
            chunks=list(stream)
        graph.current() # ready前
    assert len(stream.receipts)==54 and state['graph_checks']==4
    assert sum(chunk['batch']['rows'] for _,_,chunk in chunks)==324
    assert events[-1]['state']=='complete'
    assert (events[-1]['observed_rows'],events[-1]['observed_bytes'])==(324,162*batch['bytes'])
    assert events[-1]['observed_bytes']>16384
    summary=codec('trend').untyped(stream.qualification_summary[0]['coverage_typed'])
    assert summary['batches']==[batch['rows_typed']]*3


@pytest.mark.parametrize('fault,reason',[
    ('source_envelope','原声明请求'),('typed_count','行计量'),('tail','回执')])
def test_source_or_tail_failure_does_not_complete_group(source,fault,reason):
    graph,a,state,_=source;state['fault']=fault
    with pytest.raises(ValueError,match=reason):
        with read_required(graph,[a],batch_rows=2,batch_bytes=1024,max_row_bytes=4096,
                           max_rows=1,max_bytes=16384) as stream:
            list(stream)
    assert stream.receipts is None and stream.qualification_summary is None


def test_early_stop_has_no_group_success(source):
    graph,a,_,_=source
    with read_required(graph,[a],batch_rows=2,batch_bytes=1024,max_row_bytes=4096,
                       max_rows=1,max_bytes=16384) as stream:next(stream)
    assert stream.receipts is None and stream.qualification_summary is None


def test_resident_coverage_metadata_still_has_a_limit(source):
    graph,a,_,_=source
    with pytest.raises(ValueError,match='驻留资格原批'):
        with read_required(graph,[a],batch_rows=2,batch_bytes=1024,max_row_bytes=4096,
                           max_rows=1,max_bytes=1) as stream:list(stream)
    assert stream.receipts is None and stream.qualification_summary is None
