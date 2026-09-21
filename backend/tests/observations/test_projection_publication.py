"""canonical P0 增量合同；真实私有 PG 验收另存交付回执。"""
import hashlib

import pytest

from data_pipeline.bgp.replay import snapshot_admission as public
from data_pipeline.bgp.replay import snapshot_access as io
from data_pipeline.bgp.replay.snapshot_contract import encode
from data_pipeline.bgp.replay.snapshot_validation import CODEC


def runtime(tmp_path, **kwargs):
    root = tmp_path.resolve()
    return public.Runtime(dsn=f'host={root} dbname=fixture',allowed_roots=(root,),scratch_root=root,
                          dependency_admissions=(),dependency_runtimes={},fixture_only=True,**kwargs)


@pytest.mark.parametrize('bad',[True,False,float('nan'),float('inf'),-float('inf'),0,-1,1.5,'2',None])
def test_all_budget_construction_and_actual_use(tmp_path,bad):
    for name in io.BUDGETS:
        with pytest.raises(ValueError): runtime(tmp_path,**{name:bad})
        rt = runtime(tmp_path); setattr(rt,name,bad)
        with pytest.raises(ValueError): rt.check()


def test_dependency_budget_actual_use(tmp_path):
    from data_pipeline.bgp.archive.admission import Runtime
    root = tmp_path.resolve()
    rt = Runtime(f'host={root} dbname=fixture',(root,),root,fixture_only=True)
    for name in ('max_temp_bytes','lock_timeout_ms'):
        old = getattr(rt,name)
        for bad in (True,float('nan'),float('inf'),1.1):
            setattr(rt,name,bad)
            with pytest.raises(ValueError): io.dependency_check(rt)
        setattr(rt,name,old)
    for bad in ('NaNMB','InfinityGB','0GB','1.5MB'):
        rt.memory_limit = bad
        with pytest.raises(ValueError): io.dependency_check(rt)


def test_codec_roundtrip_is_not_approximate_json():
    value = {'scope':(b'a',None,True,0), 'set':{2,3},'list':[0,False]}
    assert io.untyped(encode(value)) == value
    with pytest.raises(ValueError): io.untyped(' {"dict":[]}')
    with pytest.raises(ValueError): io.untyped('{"dict":[["x",1],["x",2]]}')
    with pytest.raises(ValueError): io.untyped('NaN')


def dummy_admission():
    return dict(owner_binding=encode(dict(descriptor=dict(plan=dict(selected_sources=['a','c'],references=[]),
        tables={t:{'rows':0,'sha256':'0'*64} for t in ('source_coverage','scope_gap','qualification_change','current_routes')},schema='m3_dummy'))),
        admission_id='a'*64,physical=dict(snapshot=1))


def request(**kwargs):
    return dict(view='changes',scope_typed=encode({'source_ids':None}),codec_version=CODEC,batch_rows=1,batch_bytes=4096,**kwargs)


def setup_read(monkeypatch,tmp_path,rows):
    rt = runtime(tmp_path); calls=[]
    monkeypatch.setattr(public,'verify_current',lambda *a,**k:calls.append('current'))
    def source(*a,**k): yield from rows
    monkeypatch.setattr(public,'_selected_rows',source)
    return rt,calls


def test_complete_only_after_normal_exit_and_cleanup(monkeypatch,tmp_path):
    rt,calls = setup_read(monkeypatch,tmp_path,[{'x':(1,b'v')},{'x':None}])
    with public.open_reader(rt,dummy_admission(),request(),guard=lambda:None) as session:
        batches = list(session)
        assert session.receipt is None
    assert session.receipt['rows'] == 2
    assert [io.untyped(b['rows_typed']) for b in batches] == [[{'x':(1,b'v')}],[{'x':None}]]
    assert all(b['bytes'] == len(b['rows_typed'].encode()) for b in batches)
    assert calls == ['current']*4  # 首、逐批两次、资源清理后的尾部。
    assert io.untyped(session.receipt['coverage_ref'])['business_absence']=='Unknown'


@pytest.mark.parametrize('mode',['early','caller_error','caught_iterator_error','close_error','source_error','tail_revoke'])
def test_failure_never_receipt(monkeypatch,tmp_path,mode):
    rt,calls = setup_read(monkeypatch,tmp_path,[{'x':1},{'x':2}])
    if mode == 'close_error':
        def source(*a,**k):
            try: yield {'x':1}
            finally: raise RuntimeError('close fail')
        monkeypatch.setattr(public,'_selected_rows',source)
    if mode == 'source_error':
        def source(*a,**k):
            yield {'x':1}
            yield {'x':2}
            raise RuntimeError('source fail')
        monkeypatch.setattr(public,'_selected_rows',source)
    if mode == 'tail_revoke':
        def current(*a,**k):
            calls.append('current')
            if len(calls)==4: raise ValueError('revoked')
        monkeypatch.setattr(public,'verify_current',current)
    session = None
    try:
        with public.open_reader(rt,dummy_admission(),request(),guard=lambda:None) as session:
            if mode=='early': next(session)
            elif mode=='caller_error': list(session); raise RuntimeError('caller')
            elif mode=='caught_iterator_error':
                next(session); rt.max_total_rows=True
                with pytest.raises(ValueError): next(session)
            else: list(session)
    except (RuntimeError,ValueError): pass
    assert session is not None and session.receipt is None


def test_request_rejects_invalid_limits_and_unknown_scope(tmp_path):
    rt = runtime(tmp_path); a = dummy_admission()
    for name in ('batch_rows','batch_bytes'):
        for bad in (True,float('nan'),float('inf'),0,-1,1.1):
            q=request();q[name]=bad
            with pytest.raises(ValueError): public._request(rt,a,q)
    for view,sources in [('changes',['c','a']),('changes',['b']),('current_routes',[])]:
        q=request();q.update(view=view,scope_typed=encode({'source_ids':sources}))
        with pytest.raises(ValueError): public._request(rt,a,q)


def test_cleanup_attempts_all_and_keeps_primary():
    actions=[]; error=ValueError('primary')
    def fail(): actions.append('fail');raise RuntimeError('cleanup')
    with pytest.raises(ValueError) as result:
        with io.cleanup([lambda:actions.append('last'),fail]): raise error
    assert result.value is error and actions==['fail','last']
    assert len(error.cleanup_errors)==1


def test_numeric_total_tightening_after_last_batch_is_metering(monkeypatch,tmp_path):
    rows = [{'x':'long original value'}]
    rt,calls = setup_read(monkeypatch,tmp_path,rows)
    with public.open_reader(rt,dummy_admission(),request(),guard=lambda:None) as session:
        batch = next(session)
        rt.max_total_bytes = 1
        with pytest.raises(StopIteration): next(session)
    assert io.untyped(batch['rows_typed']) == rows
    assert session.receipt['rows'] == 1
    assert session.bytes == len(encode(rows[0]).encode())


def test_tail_current_total_change_is_metering(monkeypatch,tmp_path):
    rt,calls = setup_read(monkeypatch,tmp_path,[{'x':1}])
    def current(*a,**k):
        calls.append('current')
        if len(calls)==3: rt.max_total_bytes=1
    monkeypatch.setattr(public,'verify_current',current)
    with public.open_reader(rt,dummy_admission(),request(),guard=lambda:None) as session:
        list(session)
    assert calls == ['current']*3
    assert session.receipt['rows'] == 1


@pytest.mark.parametrize('bound',['rows','bytes'])
def test_total_is_metering_across_hard_bounded_batches(monkeypatch,tmp_path,bound):
    rows = [{'x':value} for value in (None,0,False,b'raw',(1,'原值'),[2,3],{'k':'v'})]
    rt,calls = setup_read(monkeypatch,tmp_path,rows)
    rt.max_total_rows = rt.max_total_bytes = 1
    q = request()
    q.update(batch_rows=2 if bound=='rows' else 100,
             batch_bytes=4096 if bound=='rows' else max(len(encode([r]).encode()) for r in rows))
    with public.open_reader(rt,dummy_admission(),q,guard=lambda:None) as session:
        batches = list(session)
        assert session.exhausted and session.receipt is None
    assert len(batches)>1
    assert [r for b in batches for r in io.untyped(b['rows_typed'])] == rows
    assert all(b['rows']<=q['batch_rows'] and b['bytes']==len(b['rows_typed'].encode())<=q['batch_bytes'] for b in batches)
    assert session.rows == session.receipt['rows'] == len(rows)
    assert session.bytes == sum(len(encode(r).encode()) for r in rows)
    assert session.receipt['typed_digest'] == hashlib.sha256(b''.join(encode(r).encode()+b'\n' for r in rows)).hexdigest()


def test_single_row_wrapper_must_fit_batch(monkeypatch,tmp_path):
    row = {'x':'原值'}
    rt,calls = setup_read(monkeypatch,tmp_path,[row])
    rt.max_total_rows = rt.max_total_bytes = 1
    q = request(); q['batch_bytes'] = len(encode([row]).encode())-1
    assert len(encode(row).encode())<=q['batch_bytes']
    with pytest.raises(ValueError,match='单行编码超过批字节预算'):
        with public.open_reader(rt,dummy_admission(),q,guard=lambda:None) as session:
            list(session)
    assert session.receipt is None
