"""消费批接口保留原 owner 编码；原 v9 人工行仅作 fixture，不代表准入。"""
import gzip
import hashlib
from pathlib import Path
from types import SimpleNamespace
import pytest

from data_pipeline.results.component_roles import codec
from data_pipeline.results.stream_policy import envelope, consume


def test_v9_legal_singleton_uses_declared_envelope_and_keeps_exact_bytes():
    text = gzip.decompress((Path(__file__).parents[1]/'fixtures/publication_v9_singleton.json.gz').read_bytes()).decode()
    assert len(text.encode()) == 2355029
    assert hashlib.sha256(text.encode()).hexdigest() == 'b1de93c3c3c548906d12c0ebcf1b350fdc560d5e5fbdef922a1104edefc1a565'
    runtime = SimpleNamespace(limits=SimpleNamespace(max_batch_rows=256,max_batch_bytes=4194304))
    rows, size = envelope('trend',runtime,batch_rows=256,max_row_bytes=4194304)
    assert (rows,size) == (256,4194304)
    batch = dict(rows_typed=text,codec_version='country-trend-typed/v2',rows=1,bytes=2355029)
    result = list(consume('trend',batch,batch_rows=256,batch_bytes=1048576,envelope_bytes=size))
    assert len(result)==1 and result[0]['batch']==batch
    assert result[0]['kind']=='consumption_batch' and result[0]['source_batch_sha256']==hashlib.sha256(text.encode()).hexdigest()


@pytest.mark.parametrize('owner',['resource','feature','detection','country','trend'])
def test_small_rows_group_without_changing_codec_fields_or_order(owner):
    c=codec(owner)
    seq=tuple if owner in ('country','trend') else list
    values=seq(dict(index=i,value=('中文',None,True,12)) for i in range(7))
    text=c.typed(values)
    original=dict(rows_typed=text,rows=7,bytes=len(text.encode()),codec_version='原codec')
    chunks=list(consume(owner,original,batch_rows=3,batch_bytes=100000,envelope_bytes=100000))
    assert [v['batch']['rows'] for v in chunks]==[3,3,1]
    restored=seq(row for chunk in chunks for row in c.untyped(chunk['batch']['rows_typed']))
    assert c.typed(restored)==text
    assert [v['source_row_start'] for v in chunks]==[0,3,6]
    assert all('receipt' not in v and 'request_digest' not in v for v in chunks)


def test_full_wrapper_limit_and_declared_source_limit():
    c=codec('trend');text=c.typed((dict(value='中文'),));size=len(text.encode())
    batch=dict(rows_typed=text,rows=1,bytes=size,codec_version='原codec')
    assert list(consume('trend',batch,batch_rows=2,batch_bytes=size-1,envelope_bytes=size))[0]['batch']==batch
    with pytest.raises(ValueError,match='声明封装'):
        list(consume('trend',batch,batch_rows=2,batch_bytes=size,envelope_bytes=size-1))


def test_graph_negotiates_each_original_runtime_capability():
    from data_pipeline.results.component_readers import Admissions
    from data_pipeline.bgp.archive.value_codec import digest
    values=[];runtimes={}
    for owner, runtime, expected in (
        ('trend',SimpleNamespace(limits=SimpleNamespace(max_batch_rows=128,max_batch_bytes=4194304)),(128,4194304)),
        ('country',SimpleNamespace(limits=SimpleNamespace(batch_rows=64,batch_bytes=2097152)),(64,2097152)),
        ('resource',SimpleNamespace(max_rows=100,max_bytes=3000000),(100,3000000)),
        ('feature',SimpleNamespace(),(256,4194304)),('detection',SimpleNamespace(),(256,4194304))):
        a=dict(owner=owner,dependencies=[],lock_targets=[]);a['admission_id']=digest(a)
        values.append(a);runtimes[a['admission_id']]=runtime
    graph=Admissions(values,runtimes,guard=lambda:None)
    assert [graph.envelope(a['admission_id'],batch_rows=256,max_row_bytes=4194304) for a in values]==[
        (128,4194304),(64,2097152),(100,3000000),(256,4194304),(256,4194304)]


def test_complete_stream_retains_original_receipt_and_early_stop_has_none():
    """公共流 fixture adapter；不模拟或宣称 owner current/PG 成功。"""
    from contextlib import contextmanager
    from data_pipeline.results.component_streams import read_required
    from data_pipeline.results.manifest_io import digest
    from data_pipeline.analysis.country_trends.stream_schema import CODEC
    c=codec('trend');seen=[]
    class Graph:
        guard=staticmethod(lambda:None)
        current=staticmethod(lambda:None)
        def envelope(self,*a,**k):return 256,4194304
        @contextmanager
        def read(self,aid,request,**limits):
            text=c.typed(tuple(dict(i=i,text='x'*32) for i in range(5)))
            batch=dict(rows_typed=text,rows=5,bytes=len(text.encode()),codec_version=CODEC)
            session=SimpleNamespace(receipt=None)
            class Source:
                receipt=None
                def __iter__(self):yield batch
            source=Source();seen.append((request,batch,source))
            yield source
            source.receipt=dict(contract='fixture-source-receipt',request_digest=digest(request),rows=5)
    graph=Graph();components=[dict(owner='trend',admission_id='fixture-stream-only')]
    options=dict(batch_rows=2,batch_bytes=200,max_row_bytes=4194304,max_rows=10000,max_bytes=1000000)
    with read_required(graph,components,**options) as stream:
        chunks=list(stream)
        assert stream.receipts is None
    assert len(stream.receipts)==54 and len(chunks)>54
    for item, (request,batch,source) in zip(stream.receipts,seen):
        assert item['receipt']==source.receipt and item['request']==request
        assert request['batch_bytes']==4194304
    coverage=c.untyped(stream.qualification_summary[0]['coverage_typed'])
    assert coverage['batches']==[seen[-1][1]['rows_typed']]
    with read_required(graph,components,**options) as stopped:next(stopped)
    assert stopped.receipts is None and stopped.qualification_summary is None


def test_actual_rss_and_free_disk_guards_still_fail(tmp_path):
    from data_pipeline.results.manifest_io import Limits
    import shutil
    with pytest.raises(ValueError,match='RSS保护'):
        Limits(max_rss_bytes=1).check(tmp_path)
    with pytest.raises(ValueError,match='磁盘保护'):
        Limits(min_free_bytes=shutil.disk_usage(tmp_path).total+1).check(tmp_path)
