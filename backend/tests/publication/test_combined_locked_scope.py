"""P层会话去重；owner仅用fixture，不连接PG。"""
from contextlib import contextmanager
from types import SimpleNamespace
import pytest
from data_pipeline.results import component_readers as access
from data_pipeline.results.component_streams import read_required
from data_pipeline.results.component_roles import codec

@pytest.fixture
def graph(monkeypatch):
    calls=[]
    a=dict(owner='trend',dependencies=[],lock_targets=[])
    a['admission_id']=access.digest(a)
    @contextmanager
    def reader(runtime,admission,req,**kw):
        calls.append('owner_before')
        class Source:
            receipt=None
            def __iter__(self):
                text=codec('trend').typed((dict(value='fixture'),))
                yield dict(rows_typed=text,rows=1,bytes=len(text.encode()),codec_version=req['codec_version'])
        source=Source()
        try:
            yield source
            calls.append('owner_after')
            source.receipt=dict(contract='component-publication-read/v1',admission_id=a['admission_id'],request_digest=access.digest(req),rows=1,execution='complete')
        finally:calls.append('owner_closed')
    monkeypatch.setattr(access,'api',lambda owner:SimpleNamespace(verify_current=lambda *a,**k:calls.append('graph'),open_reader=reader))
    fixed=access.Admissions([a],{a['admission_id']:SimpleNamespace()},guard=lambda:None)
    return fixed,a,calls

def read_one(fixed,a):
    from data_pipeline.results.component_streams import requests
    req=requests('trend',batch_rows=2,batch_bytes=4096)[0]
    return fixed.read(a['admission_id'],req,max_rows=100,max_bytes=10000)

def test_multiview_keeps_four_boundaries_and_owner_checks(graph):
    fixed,a,calls=graph
    fixed.current() # 原candidate写前
    with fixed.locked():
        with read_required(fixed,[a],batch_rows=2,batch_bytes=4096,max_rows=100,max_bytes=100000) as streams:
            list(streams)
        fixed.current() # 原ready提交前
    assert calls.count('graph')==4
    assert calls.count('owner_before')==calls.count('owner_after')==54
    assert len(streams.receipts)==54
    with read_one(fixed,a) as result:list(result)
    assert calls.count('graph')==6 and result.receipt is not None

@pytest.mark.parametrize('failure',['early','exception'])
def test_failed_read_revokes_scope(graph,failure):
    fixed,a,calls=graph
    with fixed.locked():
        try:
            with read_one(fixed,a) as result:
                next(result)
                if failure=='exception':raise ValueError('fixture')
        except ValueError:pass
        assert result.receipt is None
        with read_one(fixed,a) as after:list(after)
        assert calls.count('graph')==3
    assert fixed._read_scope is None

def test_expired_reader_cannot_reuse_new_scope(graph):
    fixed,a,calls=graph
    with fixed.locked():
        cm=read_one(fixed,a);result=cm.__enter__()
    with fixed.locked():
        with pytest.raises(ValueError,match='会话已失效'):next(result)
        cm.__exit__(None,None,None)
    assert result.receipt is None

def test_nested_and_failed_lock_entry_do_not_leak(graph,monkeypatch):
    fixed,a,calls=graph
    with fixed.locked():
        with pytest.raises(ValueError,match='嵌套或重入'):
            with fixed.locked():pass
        with read_one(fixed,a) as result:list(result)
        assert calls.count('graph')==1
    def failed():raise ValueError('current failure')
    monkeypatch.setattr(fixed,'current',failed)
    with pytest.raises(ValueError,match='current failure'):
        with fixed.locked():pass
    assert fixed._read_scope is None and fixed._locking is False

def test_complete_tail_drift_refuses_receipts(graph,monkeypatch):
    fixed,a,calls=graph
    with fixed.locked():
        def drift():raise ValueError('tail drift')
        monkeypatch.setattr(fixed,'current',drift)
        with pytest.raises(ValueError,match='tail drift'):
            with read_required(fixed,[a],batch_rows=2,batch_bytes=4096,max_rows=100,max_bytes=100000) as streams:
                list(streams)
        assert streams.receipts is None
    assert fixed._read_scope is None
