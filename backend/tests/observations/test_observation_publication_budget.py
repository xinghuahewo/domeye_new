"""可变P1预算必须在使用点保持有限正值；不冻结合法有限调整。"""
from pathlib import Path
from types import SimpleNamespace

import pytest

from data_pipeline.bgp.archive import admission as p
from data_pipeline.bgp.archive import validation as v


def test_live_budget_types_and_finite_adjustment(tmp_path):
    rt=p.Runtime('unused',(tmp_path,),tmp_path,fixture_only=True)
    for name in ('max_rss_bytes','min_free_bytes','max_temp_bytes','lock_timeout_ms'):
        original=getattr(rt,name)
        for bad in (float('nan'),float('inf'),-float('inf'),True,False,0,-1,'100',None):
            setattr(rt,name,bad)
            with pytest.raises(ValueError,match='有限正值'):rt.resource_guard()
        setattr(rt,name,original+1)
        assert rt._checked_limits()[name]==original+1
        setattr(rt,name,original)
    original=rt.memory_limit
    for bad in ('NaN','inf','0MB','-1GB',None,1):
        rt.memory_limit=bad
        with pytest.raises(ValueError,match='有限正值'):rt.resource_guard()
    rt.memory_limit=original
    rt.resource_guard()


def test_temp_budget_validated_after_caller_guard(tmp_path):
    rt=p.Runtime('unused',(tmp_path,),tmp_path,fixture_only=True)
    def mutate():rt.max_temp_bytes=float('nan')
    with pytest.raises(ValueError,match='有限正值'):rt.guarded(mutate)()
    rt.max_temp_bytes=512*1024**2
    with pytest.raises(ValueError,match='有限正值'):v._guard_temp(rt,tmp_path,rt.guarded(mutate))
    rt.max_temp_bytes=1
    (tmp_path/'sample').write_bytes(b'12')
    with pytest.raises(ValueError,match='临时磁盘'):v._guard_temp(rt,tmp_path,lambda:None)
    rt.max_temp_bytes=2
    v._guard_temp(rt,tmp_path,lambda:None)  # 上限相等允许；有限上调即时生效。


def test_resource_comparison_uses_validated_snapshot(tmp_path,monkeypatch):
    rt=p.Runtime('unused',(tmp_path,),tmp_path,fixture_only=True,max_rss_bytes=1)
    def usage():
        rt.max_rss_bytes=float('nan')
        return SimpleNamespace(ru_maxrss=10)
    monkeypatch.setattr(p.resource,'getrusage',lambda *a:usage())
    with pytest.raises(ValueError,match='RSS'):rt.resource_guard()
    with pytest.raises(ValueError,match='有限正值'):rt.resource_guard()
