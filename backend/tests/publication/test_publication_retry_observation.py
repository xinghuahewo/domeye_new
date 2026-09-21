"""仅计量与阶段状态 fixture，不把进度当资格或恢复点。"""
from contextlib import contextmanager,nullcontext
import json
import pytest
from tests.publication.test_publication_retry import materials
from data_pipeline.results.component_roles import codec
from data_pipeline.results.component_streams import read_required


@pytest.mark.parametrize('fail',[False,True])
def test_view_failure_retains_observed_amount_and_last_complete_boundary(tmp_path,fail):
    from data_pipeline.results.retry.observation import ViewProgress
    c=codec('trend')
    class Graph:
        guard=staticmethod(lambda:None)
        current=staticmethod(lambda:None)
        @contextmanager
        def read(self,aid,request,**limits):
            class Source:
                receipt=None
                def __iter__(self):
                    text=c.typed((dict(value='fixture'),))
                    yield dict(rows_typed=text,rows=1,bytes=len(text.encode()),codec_version=request['codec_version'])
                    if fail and request['view']=='activity_relation':raise ValueError('fixture-view-failure')
            source=Source();yield source
            source.receipt=dict(rows=1,execution='complete')
    observation=ViewProgress(tmp_path)
    with pytest.raises(ValueError,match='fixture-view-failure') if fail else nullcontext():
        with read_required(Graph(),[dict(owner='trend',admission_id='fixture')],batch_rows=1,
                           batch_bytes=4096,max_rows=10000,max_bytes=1000000,progress=observation) as stream:
            list(stream)
    latest=json.loads((tmp_path/'读取进度.json').read_text())
    assert latest['owner']=='trend' and latest['view_observed_rows']==1
    if fail:
        assert latest['state']=='failed' and latest['view']=='activity_relation'
        assert latest['observed_rows']==2 and latest['completed_views']==1
        assert latest['last_completed_view']['view']=='activity_context' and stream.receipts is None
    else:
        assert latest['state']=='complete' and latest['observed_rows']==54 and latest['completed_views']==54
        assert len(stream.receipts)==54
    assert latest['physical_input_bytes'] is None and latest['progress_only'] is True
    assert 'receipt' not in latest


@pytest.mark.parametrize('fault',['none','publish_receipt','discover','mismatch','publish_unknown'])
def test_publish_confirmation_survives_later_failure(tmp_path,monkeypatch,fault):
    from data_pipeline.results.retry.driver import finish_publication
    from data_pipeline.results import Token
    from data_pipeline.jobs import downstream as migration_tail
    monkeypatch.setattr(migration_tail,'process_sample',lambda *a,**k:dict(parent_rss_bytes=100,children_rss_bytes=20,observed_pids=[1]))
    token=Token('p','b','profile');calls=[]
    class Publication:
        guard=staticmethod(lambda:None)
        def publish(self,*a,**k):
            calls.append('publish')
            if fault=='publish_unknown':raise OSError('unknown commit')
            return token
        def discover(self,*a):
            calls.append('discover')
            if fault=='discover':raise OSError('discover unavailable')
            return (Token('wrong','wrong','profile') if fault=='mismatch' else token),7
    original_open=type(tmp_path).open
    if fault=='publish_receipt':
        def broken_open(path,*a,**k):
            if path.name=='原publish返回.json':raise OSError('receipt write failure')
            return original_open(path,*a,**k)
        monkeypatch.setattr(type(tmp_path),'open',broken_open)
    status=dict(state='prepared',last_completed_state='prepared',publication_state='not_attempted',generation=None)
    from data_pipeline.results.retry.observation import stage
    # 已完成上游制品 fixture；后续失败不能覆盖它或回退准备阶段。
    upstream=tmp_path/'已完成上游.json';upstream.write_text('已完成fixture')
    with stage(tmp_path,'prepare','原owner已交付行') as metrics:
        metrics['processed_count']=10
    if fault=='none':finish_publication(Publication(),token,dict(expected_generation=0,selector='fixture'),tmp_path,status)
    else:
        with pytest.raises((OSError,ValueError)):
            finish_publication(Publication(),token,dict(expected_generation=0,selector='fixture'),tmp_path,status)
    assert calls.count('publish')==1
    if fault=='publish_unknown':
        assert status['publication_state']=='outcome_unknown' and 'published_token' not in status
    else:
        assert status['publication_state']=='confirmed' and status['published_token']['publication_id']=='p'
        assert status['last_completed_state']==('discovered' if fault=='none' else 'published')
    assert status['generation']==(7 if fault=='none' else None)
    publication_metrics=json.loads((tmp_path/'stages/publish/阶段观测.json').read_text())
    assert publication_metrics['state']==('failed' if fault in ('publish_receipt','publish_unknown') else 'completed')
    assert publication_metrics['stage_process_group_peak_rss_bytes']==120
    assert publication_metrics['physical_input_bytes'] is None
    assert json.loads((tmp_path/'stages/prepare/阶段观测.json').read_text())['state']=='completed'
    assert upstream.read_text()=='已完成fixture'


def test_sampling_failure_preserves_primary_and_has_no_fake_rss(tmp_path,monkeypatch):
    from data_pipeline.results.retry.observation import stage
    from data_pipeline.jobs import downstream as migration_tail
    def unavailable(*a,**k):raise OSError('ps unavailable')
    monkeypatch.setattr(migration_tail,'process_sample',unavailable)
    with pytest.raises(ValueError,match='primary'):
        with stage(tmp_path,'fixture','完成工作单元') as metrics:
            metrics['processed_count']=2
            raise ValueError('primary')
    saved=json.loads((tmp_path/'stages/fixture/阶段观测.json').read_text())
    assert saved['sampling_failures']>=1 and saved['rss_coverage']=='unavailable'
    assert saved['stage_process_group_peak_rss_bytes'] is None and saved['state']=='failed'
    assert saved['error']['message']=='primary' and saved['physical_output_bytes'] is None


def test_progress_is_only_accounting_and_write_error_does_not_replace_work(tmp_path,monkeypatch):
    from data_pipeline.results.retry.observation import ViewProgress
    event=dict(state='complete',owner='fixture',view='fixture',observed_rows=10**12,observed_bytes=10**15,
               view_observed_rows=10**12,view_observed_bytes=10**15,completed_views=1,planned_views=1,last_completed_view=None)
    observer=ViewProgress(tmp_path);observer(event)
    assert json.loads((tmp_path/'读取进度.json').read_text())['observed_rows']==10**12
    def fail(*a,**k):raise OSError('disk observation failure')
    monkeypatch.setattr(type(tmp_path),'open',fail)
    observer(dict(event,state='failed',error=dict(message='original failure')))
    assert observer.latest['error']['message']=='original failure' and observer.errors[0]['count']==1


@pytest.mark.parametrize('valid',[True,False])
def test_preflight_validates_explicit_observation_contract(materials,valid):
    from data_pipeline.results.retry.driver import preflight
    spec,_=materials
    spec['observation']=dict(contract='publication-retry-observation/v1',stage_rss_sample_seconds=0.1,
        batch_progress_flush_seconds=1.0,physical_io='Unknown',progress_only=True)
    if valid:assert preflight(spec)['summary']['requests']==72
    else:
        spec['observation']['progress_only']=False
        with pytest.raises(ValueError,match='观测合同'):preflight(spec)
