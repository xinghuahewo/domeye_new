"""原 Resource 的实际关闭链定向集成，不运行原生产或科学测试矩阵。"""
import os
import json
from pathlib import Path
from collections import Counter
import pytest
if os.environ.get('DOMEYE_RESOURCE_CLOSE_INTEGRATION')!='8233':
    pytest.skip('仅本任务已绑定原人工制品',allow_module_level=True)
import tests.resources.test_resource_p1_integration_8233 as base
from data_pipeline.analysis.resources import publication as p
from data_pipeline.bgp.archive import admission as up
from data_pipeline.analysis.resources.publication_codec import typed, untyped

OUT=Path('/tmp/domeye-resource-close-integration-8233').resolve()
OLD=Path('/tmp/domeye-resource-p1-resumed-8233').resolve()

@pytest.fixture(scope='module')
def admitted():
    # 所有新证据进入新目录，保留原失败日志与固定索引。
    base.OUT=OUT;base.SCRATCH=OUT/'scratch'
    before=base.anchors();base.save('原登记前.json',before)
    original=json.loads((OLD/'原锚后.json').read_text());assert before==original
    deps=json.loads((OLD/'上游当前依赖.json').read_text())
    b=json.loads((OLD/'原binding.json').read_text())
    u=up.Runtime(base.DSN,(base.ROOT,OUT),base.SCRATCH,fixture_only=True,dependency_admissions=tuple(deps),audit_sink=base.EVENTS.append)
    with base.measure('原M2参考当前核验'):
        for a in deps:up.verify_current(u,a,guard=lambda:None)
    rt=p.Runtime(base.DSN,(base.ROOT,OUT),base.SCRATCH,u,tuple(deps),fixture_only=True,audit_sink=base.EVENTS.append)
    old=json.loads((OLD/'恢复后新Admission.json').read_text())
    with base.measure('旧Resource规则边界'):
        with pytest.raises(ValueError,match='validator规则发生变化') as caught:p.verify_current(rt,old,guard=lambda:None)
    base.save('旧规则拒绝.json',dict(admission=old['admission_id'],error=str(caught.value)))
    with base.measure('必要Resource新准入'):a=p.admit(rt,b,guard=lambda:None)
    base.save('新Admission.json',a)
    assert a['admission_id']!=old['admission_id'] and a['owner_binding']==old['owner_binding']
    initial=base.anchors();base.save('必要准入后登记.json',initial)
    yield rt,a
    after=base.anchors();base.save('原登记后.json',after);assert after==initial
    for table,rows in before.items():
        if isinstance(rows,list):assert rows==after[table]
        else:assert all(after[table][k]==v for k,v in rows.items())
    assert base.hashes()==json.loads((OUT/'原目录SHA.json').read_text())
    assert not list(base.SCRATCH.iterdir())

@pytest.mark.parametrize('mode',['caller','early','exhausted'])
def test_actual_inner_close(admitted,monkeypatch,mode):
    rt,a=admitted;connect=p.connect_duckdb;closed=[];primary=RuntimeError('调用方原主错');secondary=OSError('实际DuckDB关闭后注入次错');origin=[];arrow=[]
    class Reader:
        def __init__(self,reader):self.reader=reader;self.count=0;arrow.append(self)
        def __iter__(self):return self
        def __next__(self):return next(self.reader)
        def close(self):self.reader.close();self.count+=1
    class DB:
        def __init__(self):self.db=connect()
        def __getattr__(self,key):return getattr(self.db,key)
        def execute(self,*args,**kwargs):self.db.execute(*args,**kwargs);return self
        def fetch_record_batch(self,*args,**kwargs):return Reader(self.db.fetch_record_batch(*args,**kwargs))
        def close(self):
            self.db.close();closed.append(True)
            with pytest.raises(Exception,match='closed'):self.db.execute('SELECT 1')
            raise secondary
    with base.measure('真实关闭-'+mode):
        with pytest.raises(RuntimeError if mode=='caller' else OSError) as caught:
            with p.open_reader(rt,a,base.req(),guard=lambda:None) as session:
                monkeypatch.setattr(p,'connect_duckdb',DB)
                if mode=='exhausted':list(session)
                else:
                    next(session)
                    if mode=='caller':
                        try:raise primary
                        except RuntimeError:origin.append(primary.__traceback__);raise
        monkeypatch.setattr(p,'connect_duckdb',connect)
        assert closed==[True] and session.receipt is None
        assert len(arrow)==3 and [r.count for r in arrow]==[1,1,1]
        if mode=='caller':
            assert caught.value is primary and primary.cleanup_errors==(secondary,)
            cursor=primary.__traceback__;trace=[]
            while cursor:trace.append(cursor);cursor=cursor.tb_next
            assert origin[0] in trace
        else:assert caught.value is secondary
        assert not any(e['kind'] in ('science_compare','inventory_scan') for e in base.EVENTS)
    assert not list(base.SCRATCH.iterdir())
    base.save('真实关闭-'+mode+'.json',dict(mode=mode,duckdb_closed_once=True,closed_connection_query_rejected=True,arrow_closes=[r.count for r in arrow],receipt=None,same_primary_and_traceback=mode=='caller',same_secondary=True,scratch_empty=True))

def test_healthy_metrics(admitted):
    rt,a=admitted
    with base.measure('正常metrics') as cost:rows,receipt=base.collect(rt,a,base.req(),cost)
    expected=untyped(json.loads((OLD/'metrics.typed.json').read_text()))
    assert Counter(map(base.norm,rows))==Counter(map(base.norm,expected))
    assert len(rows)==28 and receipt['execution']=='complete'
    assert not any(e['kind'] in ('science_compare','inventory_scan','entity_hash') for e in base.EVENTS)
    base.save('正常metrics.typed.json',typed(rows));base.save('正常metrics.receipt.json',receipt)
