"""复用自有健康制品，探测真正内层 DuckDB 关闭链；不重产科学数据。"""
import pytest
from tests.resources.test_resource_publication import case, req
from data_pipeline.analysis.resources import publication as p
from data_pipeline.analysis.resources.publication_codec import typed, untyped
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader


@pytest.fixture(scope='module')
def admitted(case):
    return case,p.admit(case.rt,case.b,guard=lambda:None)


@pytest.mark.parametrize('mode',['caller','early','exhausted'])
def test_actual_inner_duckdb_close_failure(admitted,monkeypatch,mode):
    c,a=admitted;connect=p.connect_duckdb;closed=[];primary=RuntimeError('调用方原主错');origin=[]
    scratch_before=set(c.rt.scratch_root.iterdir())
    class DB:
        def __init__(self):self.db=connect()
        def __getattr__(self,k):return getattr(self.db,k)
        def close(self):
            self.db.close();closed.append(True)
            raise OSError('实际DuckDB关闭后注入次错')
    with pytest.raises(RuntimeError if mode=='caller' else OSError) as caught:
        with p.open_reader(c.rt,a,req(),guard=lambda:None) as session:
            # 首current已结束；替换真正selected_rows所用的内层连接。
            monkeypatch.setattr(p,'connect_duckdb',DB)
            if mode=='exhausted':list(session)
            else:
                next(session)
                if mode=='caller':
                    try:raise primary
                    except RuntimeError:
                        origin.append(primary.__traceback__)
                        raise
    monkeypatch.setattr(p,'connect_duckdb',connect)
    assert closed==[True] and session.receipt is None
    assert set(c.rt.scratch_root.iterdir())==scratch_before
    if mode=='caller':
        assert caught.value is primary
        cursor=primary.__traceback__;chain=[]
        while cursor:chain.append(cursor);cursor=cursor.tb_next
        assert origin[0] in chain
        assert len(primary.cleanup_errors)==1
        assert isinstance(primary.cleanup_errors[0],OSError)
        assert '实际DuckDB关闭' in str(primary.cleanup_errors[0])
    else:assert '实际DuckDB关闭' in str(caught.value)


def test_healthy_values_and_registration_unchanged(admitted):
    c,a=admitted;before=p._binding(c.rt,c.b).binding
    reader=ResourceObservationReader(c.rt.dsn,c.b['run_id'],c.b['snapshot'],c.b['dataset_id'])
    expected=list(reader.values(target='metrics',scope='all'))
    with p.open_reader(c.rt,a,req(),guard=lambda:None) as session:
        actual=[r for batch in session for r in untyped(batch['rows_typed'])]
    def normalized(row):return typed({**row,'qualification':sorted(row['qualification'],key=typed)})
    assert sorted(map(normalized,actual))==sorted(map(normalized,expected))
    assert session.receipt and session.receipt['rows']==len(expected)
    assert p._binding(c.rt,c.b).binding==before
