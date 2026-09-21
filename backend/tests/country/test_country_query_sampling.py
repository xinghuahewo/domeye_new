"""查询物理采样节流、即时取消和公开会话尾部检查；仅临时 fixture。"""
from collections import Counter
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from data_pipeline.analysis.country_events import selection_index as qi
from tests.country.test_country_stream_accounting import public_reader_fixture


def test_sampling_refreshes_time_roots_limits_and_rechecks_failures(tmp_path, monkeypatch):
    now=[0.0];scans=[]
    monkeypatch.setattr(qi.time,'monotonic',lambda:now[0])
    monkeypatch.setattr(qi.resource,'getrusage',lambda _:SimpleNamespace(ru_maxrss=0))
    original=Path.rglob
    def scan(path,pattern):
        scans.append(path)
        return original(path,pattern)
    monkeypatch.setattr(Path,'rglob',scan)
    root=tmp_path/'root';root.mkdir()
    budget=qi.Budget(root,qi.QueryLimits(max_disk_bytes=100),_sample=True)
    checks=[]
    def guard():checks.append(True);budget()
    progress,errors=qi.counted_progress(Counter(),guard)
    for _ in range(2000):assert progress()==0
    assert len(checks)==2000 and len(scans)==1 and not errors
    now[0]=0.049;budget();assert len(scans)==1
    now[0]=0.051;budget();assert len(scans)==2
    extra=tmp_path/'extra';extra.mkdir();budget.extra_roots=lambda:(extra,)
    budget();assert len(scans)==4
    budget.limits=replace(budget.limits,max_disk_bytes=99)
    budget();assert len(scans)==6
    (root/'growth').write_bytes(b'x'*100)
    budget()  # 同一采样间隔内允许延迟到下一次检查。
    now[0]=0.102
    for _ in range(2):
        with pytest.raises(ValueError,match='resource_limit:C4_working_set'):budget()
    assert len(scans)==10


def test_public_close_forces_sample_and_withholds_receipt(tmp_path,monkeypatch):
    p,qr,runtime,descriptor,request,rows,closed=public_reader_fixture(tmp_path,monkeypatch)
    monkeypatch.setattr(qi.time,'monotonic',lambda:0.0)
    runtime.limits=replace(runtime.limits,max_disk_bytes=65536)
    with pytest.raises(ValueError,match='resource_limit:C4_working_set'):
        with p.open_reader(runtime,{'admission_id':'fixture'},request,guard=lambda:None) as session:
            list(session)
            (tmp_path/'tail-growth').write_bytes(b'x'*65537)
    assert session.receipt is None and closed==[True]


def test_sampled_sql_callback_keeps_immediate_original_cancel(tmp_path,monkeypatch):
    p,qr,runtime,descriptor,request,rows,closed=public_reader_fixture(tmp_path,monkeypatch)
    monkeypatch.setattr(qi.time,'monotonic',lambda:0.0)
    source=SimpleNamespace(check=lambda:None)
    reader=qr.ResultReader(None,descriptor,source,runtime.limits)
    reader.check(force_sample=True)
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
    assert closed==[True]
