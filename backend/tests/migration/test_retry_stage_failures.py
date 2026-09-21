"""阶段入口/出口与观测写入失败均保留定位及原异常。"""
import json
import pytest
from data_pipeline.results.retry.observation import stage
from data_pipeline.jobs import downstream as migration_tail


@pytest.mark.parametrize('where',['entry','exit','start_write','final_write'])
def test_entire_stage_failure_is_annotated(tmp_path,monkeypatch,where):
    monkeypatch.setattr(migration_tail,'process_sample',lambda *a,**k:dict(parent_rss_bytes=100,children_rss_bytes=20,observed_pids=[1]))
    original=OSError('fixture '+where);calls=[]
    def guard():
        calls.append(1)
        if (where=='entry' and len(calls)==1) or (where=='exit' and len(calls)==2):raise original
    path_type=type(tmp_path);original_open=path_type.open
    def open_file(path,*a,**k):
        if (where=='start_write' and path.name=='阶段开始.json') or (where=='final_write' and path.name=='阶段观测.json'):
            raise original
        return original_open(path,*a,**k)
    monkeypatch.setattr(path_type,'open',open_file)
    with pytest.raises(OSError) as caught:
        with stage(tmp_path,'control','完成核验次数',guard) as metrics:
            metrics['processed_count']=1
    assert caught.value is original
    assert original.retry_stage=='control'
    assert original.retry_observation['stage']=='control'
    assert original.retry_observation['processed_unit']=='完成核验次数'
    assert original.retry_observation['state']=='failed'
    if where!='final_write':
        saved=json.loads((tmp_path/'stages/control/阶段观测.json').read_text())
        assert saved['stage']=='control' and saved['processed_unit']=='完成核验次数'


def test_inner_stage_and_cleanup_error_are_preserved(tmp_path,monkeypatch):
    original=ValueError('primary')
    path_type=type(tmp_path);original_open=path_type.open
    def open_file(path,*a,**k):
        if path.name=='阶段观测.json':raise OSError('cleanup')
        return original_open(path,*a,**k)
    monkeypatch.setattr(path_type,'open',open_file)
    with pytest.raises(ValueError) as caught:
        with stage(tmp_path,'outer','外层'):
            with stage(tmp_path,'inner','内层'):
                raise original
    assert caught.value is original and original.retry_stage=='inner'
    assert original.retry_observation['stage']=='inner'
    assert len(original.migration_cleanup_errors)==2


def test_failure_before_metrics_are_available_marks_unknown(tmp_path):
    (tmp_path/'stages').write_text('fixture obstruction')
    with pytest.raises(OSError) as caught:
        with stage(tmp_path,'runtime','原Runtime数量'):pytest.fail('不得进入业务体')
    assert caught.value.retry_stage=='runtime'
    assert caught.value.retry_observation['wall_seconds'] is None
    assert caught.value.retry_observation['rss_coverage']=='unavailable'
