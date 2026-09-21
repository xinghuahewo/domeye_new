"""Resource真实来源验证边界的UTC窗口兼容；Reader仅提供fixture元数据。"""
from datetime import datetime, timezone

import pytest

from data_pipeline.analysis.resources import bindings
from data_pipeline.analysis.resources.compute import RibContext
from data_pipeline.bgp.input.mrt_reader import source_identity


def sources_and_reader(monkeypatch):
    inputs=[];sources=[];calls=[]
    for epoch in (100,200,300,600):
        uri=f'fixture://resource-window/{epoch}';sha=f'{epoch:064x}'
        sid=source_identity('rrc25',uri,sha)
        inputs.append(dict(source_id=sid,role='snapshot',origin_uri=uri,sha256=sha))
        sources.append(bindings.SourceBinding('fixture-run',1,'result' if epoch==600 else 'warmup',
            RibContext(sid,'rrc25',datetime.fromtimestamp(epoch,timezone.utc),'fixture-reference',
                       'explicit-epoch',content_sha256=sha,origin_uri=uri)))
    class Reader:
        def __init__(self,dsn,run,snapshot,ids,**kwargs):
            calls.append(ids)
            self.manifest=dict(collector='rrc25',inputs=inputs)
    monkeypatch.setattr(bindings,'ObservationReader',Reader)
    return sources,calls


@pytest.mark.parametrize('window', [
    ('1970-01-01T00:10:00Z','1970-01-01T00:15:00Z'),
    ('1970-01-01T00:10:00+00:00','1970-01-01T00:15:00+00:00'),
    ('1970-01-01T08:10:00+08:00','1970-01-01T08:15:00+08:00'),
])
def test_validate_sources_keeps_actual_warmup_and_result_window(monkeypatch,window):
    sources,calls=sources_and_reader(monkeypatch)
    readers=bindings.validate_sources('fixture',sources,window,profile='observation')
    assert len(readers)==len(calls)==4
    assert [bindings.time_value(t).timestamp() for t in window]==[600,900]
    assert bindings.time_value(window[0]).utcoffset()==datetime.fromisoformat(window[0].replace('Z','+00:00')).utcoffset()


def test_validate_sources_keeps_half_open_bounds(monkeypatch):
    sources,calls=sources_and_reader(monkeypatch)
    # 600为result且在左端点通过；右端点600必须拒绝，不能归到窗口内。
    with pytest.raises(ValueError,match='用途与结果窗口'):
        bindings.validate_sources('fixture',sources,('1970-01-01T00:05:01Z','1970-01-01T00:10:00Z'))


@pytest.mark.parametrize('value',['1970-01-01T00:10:00','invalid-time'])
def test_time_value_does_not_accept_naive_or_invalid(value):
    with pytest.raises(ValueError):bindings.time_value(value)
