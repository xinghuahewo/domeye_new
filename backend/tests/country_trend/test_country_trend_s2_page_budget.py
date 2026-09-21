"""分页引用预算定点复核：只读显式人工制品，不重产、不改原锚。"""
import pytest
from tests.country_trend.test_country_trend_s2_saved import saved
from data_pipeline.analysis.country_trends.snapshot_reader import TrendReader, TrendReadReceipt
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
from data_pipeline.analysis.country_trends.snapshot_schema import reference_count


def reader(saved,limit):
    return TrendReader(saved.b,saved.p,runtime=saved.rt,limits=S2Limits(max_references=limit))


@pytest.mark.parametrize('method',['query','iter_query','scan'])
def test_actual_edge_or_full_stream_over_budget_has_no_success(saved,method):
    r=reader(saved,1);emitted=[]
    with pytest.raises(ValueError,match='trend_reference_budget'):
        if method=='query':r.query('edge',limit=1)
        else:
            for item in (r.iter_query('edge',page_size=1) if method=='iter_query' else r.scan()):emitted.append(item)
    assert not any(isinstance(v,TrendReadReceipt) for v in emitted)
    assert r.budget.references>1 and not r.budget.scratch_roots


@pytest.mark.parametrize('kind',['edge','claim','evidence','metric','point','activity_window'])
def test_shared_rule_charges_actual_rows_including_lookahead(saved,kind):
    original=[v for v in saved.rows if v.kind==kind]
    assert len(original)>1
    r=reader(saved,100);before=r.budget.rows;page=r.query(kind,limit=1)
    assert page.items==(original[0],) and page.next_cursor
    assert r.budget.references==sum(reference_count(v) for v in original[:2])
    assert r.budget.rows-before==2 and not r.budget.scratch_roots


@pytest.mark.parametrize('event_filter',[False,True])
def test_enough_budget_filtered_key_preserves_body_and_receipt(saved,event_filter):
    edge=next(v for v in saved.rows if v.kind=='edge');r=reader(saved,2);before=r.budget.rows
    filters=dict(key=edge.key)
    if event_filter:filters['event']=edge.event
    values=list(r.iter_query('edge',page_size=1,**filters))
    assert values[:-1]==[edge] and isinstance(values[-1],TrendReadReceipt)
    assert values[-1].rows==1 and values[-1].full_body_validated is False
    assert r.budget.references==2 and r.budget.rows-before==1


def test_event_filter_and_cross_page_budget_does_not_reset(saved):
    edge=next(v for v in saved.rows if v.kind=='edge');r=reader(saved,6)
    first=r.query('edge',event=edge.event,limit=1)
    assert first.items==(edge,) and first.next_cursor and r.budget.references==4
    with pytest.raises(ValueError,match='trend_reference_budget'):
        r.query('edge',event=edge.event,cursor=first.next_cursor,limit=1)
    # 第二页首行重新读取、随后lookahead也读取；尝试量4+2+2，不按页重置。
    assert r.budget.references==8 and not r.budget.scratch_roots


def test_iter_query_partial_rows_never_get_success_receipt(saved):
    r=reader(saved,6);emitted=[]
    with pytest.raises(ValueError,match='trend_reference_budget'):
        for item in r.iter_query('edge',page_size=1):emitted.append(item)
    assert len(emitted)==1 and emitted[0].kind=='edge'
    assert not any(isinstance(v,TrendReadReceipt) for v in emitted)
    assert r.budget.references==8 and not r.budget.scratch_roots


@pytest.mark.parametrize('budget_failure',[False,True])
def test_close_failure_and_primary_failure_are_preserved(saved,monkeypatch,budget_failure):
    from data_pipeline.analysis.country_trends import snapshot_reader as module
    r=reader(saved,1 if budget_failure else 4);connect=module.lake_connect;closed=[]
    class BrokenClose:
        def __init__(self,db):self.db=db
        def __getattr__(self,k):return getattr(self.db,k)
        def close(self):
            self.db.close();closed.append(True);raise OSError('page close failed')
    monkeypatch.setattr(module,'lake_connect',lambda *a,**k:BrokenClose(connect(*a,**k)))
    with pytest.raises(ValueError if budget_failure else OSError,match='trend_reference_budget' if budget_failure else 'page close failed') as error:
        r.query('edge',limit=1)
    assert closed==[True] and not r.budget.scratch_roots
    if budget_failure:assert any(isinstance(e,OSError) for e in error.value.cleanup_errors)
