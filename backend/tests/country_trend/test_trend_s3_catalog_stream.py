"""有序目录流的SQL适配器fixture；无真实PG或AD，不用空current桩。"""
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
import copy
import pytest
from data_pipeline.analysis.country_trends import result_catalog as catalog
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
from data_pipeline.analysis.country_trends.stream_schema import TABLES
from data_pipeline.results.manifest_io import encode as json_text


def fixture(monkeypatch, count=300, batch=1, mutation=False):
    b=dict(system_id='1',database_oid=2,catalog_id='fixture',root='/fixture',schema_name='fixture',component_id='fixture',snapshot=1)
    proof={'fixture':'unqualified'};binding=dict(binding=b,proof=proof);names=list(TABLES)
    table_ids=list(range(len(names)))
    docs=[dict(table_id=table_ids[0],data_file_id=i,path=str(i),mapping_id=1,begin_snapshot=1,end_snapshot=None) for i in range(count)]
    if mutation:docs[-1]['path']='changed'
    state=SimpleNamespace(fetches=0,largest=0,queries=[])
    class Cursor:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def execute(self,sql,args):
            state.queries.append(sql)
            if 'pg_control_system' in sql:values=[['1',2 if 'oid::bigint' in sql else '2']] # PG14: OID在JSONB内为字符串，bigint为数字
            elif 'country_trends.catalog' in sql:values=['fixture']
            elif 'country_trends.components' in sql:values=[['complete','/fixture','fixture',json_text(b),json_text(proof)]]
            elif 'ducklake_metadata' in sql:values=[dict(key='version',value='0.3',scope='GLOBAL',scope_id=None)]
            elif 'ducklake_schema t' in sql:values=[dict(schema_id=1,schema_name='fixture',end_snapshot=None)]
            elif 'ducklake_table t' in sql:values=[dict(table_id=i,table_name=n,schema_id=1,end_snapshot=None) for i,n in enumerate(names)]
            elif 'ducklake_inlined' in sql or 'ducklake_delete_file t' in sql or 'WHERE m.mapping_id IS NULL' in sql:values=[]
            elif 'ducklake_column_mapping t' in sql:values=[dict(mapping_id=1,table_id=0,type='map_by_name')]
            elif 'ducklake_name_mapping t' in sql:values=[dict(mapping_id=1,column_id=i,name=str(i)) for i in range(count)]
            elif 'ducklake_column t' in sql:values=[dict(table_id=i,column_id=0,end_snapshot=None) for i in table_ids]
            elif 'ducklake_data_file t' in sql:values=docs
            else:raise AssertionError(sql)
            self.values=iter(sorted(copy.deepcopy(values),key=lambda x:json_text(x)))
        def fetchmany(self,n):
            assert n==1  # 实际策略一次只搬一项，不把驱动端页当整件列表。
            state.fetches+=1;value=next(self.values,None)
            result=[] if value is None else [(value,)]
            state.largest=max(state.largest,len(result));return result
    class PG:
        def cursor(self,**kw):
            assert kw['name'].startswith('trend_catalog_') and kw['scrollable'] is False
            return Cursor()
    @contextmanager
    def pg(runtime):yield PG()
    monkeypatch.setattr(catalog.pg_api,'_pg',pg)
    rt=SimpleNamespace(limits=replace(S2Limits(),max_rows=1,max_total_bytes=1,max_batch_rows=batch,max_context_bytes=1024),
        resource_guard=lambda:None,event=lambda *a,**kw:None)
    return rt,binding,state


def test_catalog_grows_without_total_context_cap_and_deterministic_batch_config(monkeypatch):
    results=[]
    for batch in (1,2):
        rt,b,state=fixture(monkeypatch,batch=batch)
        result=catalog.catalog(rt,b,lambda:None)
        assert result['rows']>600 and result['bytes']>rt.limits.max_context_bytes and state.largest==1
        assert result['streams']['ducklake_data_file']['rows']==300
        assert 'ORDER BY' in next(q for q in state.queries if 'ducklake_data_file t' in q)
        results.append(result)
    assert results[0]==results[1]


def test_one_catalog_item_drift_changes_digest(monkeypatch):
    rt,b,_=fixture(monkeypatch);before=catalog.catalog(rt,b,lambda:None)
    rt,b,_=fixture(monkeypatch,mutation=True);after=catalog.catalog(rt,b,lambda:None)
    assert before['streams']['ducklake_data_file']['sha256']!=after['streams']['ducklake_data_file']['sha256']
    assert before['rows']==after['rows']


@pytest.mark.parametrize('field,value',[('system_id','9'),('database_oid',3)])
def test_catalog_rejects_real_physical_identity_difference(monkeypatch,field,value):
    rt,b,_=fixture(monkeypatch,count=1)
    b['binding'][field]=value
    with pytest.raises(ValueError,match='trend_catalog_physical'):
        catalog.catalog(rt,b,lambda:None)
