"""P1 独立复核两项P2的有限真实PG增量；复用既有制品。"""
import copy
import hashlib
from contextlib import contextmanager
import psycopg2
import pytest
from tests.detection.test_detection_publication import bundles, consume, request
from data_pipeline.analysis.detection import publication as d
from data_pipeline.analysis.detection import publication_io as io
from data_pipeline.analysis.detection.store import read_stored_rows


def test_lock_exit_is_only_rollback_close(bundles,monkeypatch):
    r,b,a,events,cost=bundles[0];connect=psycopg2.connect;traces=[]
    class Connection:
        def __init__(self,*args,**kwargs):self.pg=connect(*args,**kwargs);self.calls=[];traces.append(self.calls)
        def __getattr__(self,name):return getattr(self.pg,name)
        def set_session(self,*args,**kwargs):self.calls.append(['set_session',kwargs]);return self.pg.set_session(*args,**kwargs)
        def commit(self):self.calls.append('commit');return self.pg.commit()
        def rollback(self):self.calls.append('rollback');return self.pg.rollback()
        def close(self):self.calls.append('close');return self.pg.close()
    monkeypatch.setattr(psycopg2,'connect',Connection)
    results=[]
    for target in a['lock_targets']:
        with d.hold_lock(r,a,target,guard=lambda:None):
            trace=traces[-1]
            assert trace==[]  # 普通事务，没有只读set_session。
            pg=connect(r.dsn)
            try:
                with pg.cursor() as c:
                    c.execute("SET lock_timeout='100ms'")
                    query='UPDATE detection.runs SET state=state WHERE run_id=%s' if target['namespace']=='detection.run' else 'UPDATE detection.publication_admissions SET state=state WHERE record_key=%s'
                    with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute(query,(target['key'],))
            finally:pg.rollback();pg.close()
        assert trace==['rollback','close'];results.append(dict(target=target,exit=list(trace),update_blocked=True))
    cost['lock_repair']=results
    original=Connection.rollback
    def failure(self):original(self);raise RuntimeError('人工回滚清理错误')
    with pytest.raises(LookupError) as caught:
        with d.hold_lock(r,a,a['lock_targets'][0],guard=lambda:None):
            monkeypatch.setattr(Connection,'rollback',failure)
            raise LookupError('主异常')
    assert isinstance(caught.value.cleanup_errors[0],RuntimeError)
    assert traces[-1]==['rollback','close']


@contextmanager
def mutation(dsn,query,args,undo,undo_args):
    pg=psycopg2.connect(dsn);pg.autocommit=True
    try:
        with pg.cursor() as c:c.execute(query,args)
        yield
    finally:
        with pg.cursor() as c:c.execute(undo,undo_args)
        pg.close()


def test_fixed_snapshot_catalog_boundaries(bundles):
    r,b,a,events,cost=bundles[0];snap=b['snapshot'];_,before=io.catalog(r,b,lambda:None)
    file=next(x for x in before['catalog_rows']['ducklake_data_file'] if x['table_id']==next(t['table_id'] for t in before['catalog_rows']['ducklake_table'] if t['table_name']=='records'))
    table_id=file['table_id'];file_id=file['data_file_id']
    def whole():return list(read_stored_rows(r.dsn,b['run_id'],snap,'records'))
    original=whole();original_sha=hashlib.sha256(d.typed(original).encode()).hexdigest();results=[]
    # 独立报告实证的未来结束：真实PG修改，固定正文摘要及同份Admission current都必须不变。
    for name,key,value in [('ducklake_schema','schema_id',before['catalog_rows']['ducklake_schema'][0]['schema_id']),('ducklake_table','table_id',table_id),('ducklake_column','column_id',next(c['column_id'] for c in before['catalog_rows']['ducklake_column'] if c['table_id']==table_id)),('ducklake_data_file','data_file_id',file_id)]:
        predicate=key+'=%s'+(' AND table_id=%s' if name=='ducklake_column' else '')
        args=(value,table_id) if name=='ducklake_column' else (value,)
        with mutation(r.dsn,f'UPDATE public.{name} SET end_snapshot=%s WHERE {predicate}',(snap+1000,*args),f'UPDATE public.{name} SET end_snapshot=NULL WHERE {predicate}',args):
            d.verify_current(r,a,guard=lambda:None)
            assert io.catalog(r,b,lambda:None)[1]==before
            assert whole()==original
        results.append(dict(case=name+'未来结束',sha256=original_sha,current='pass'))
    # 未来追加文件带mapping：此前仅为静态风险，这里另行做真实目录探针，不假称有新科学输出。
    future_id=987654321
    with mutation(r.dsn,'''INSERT INTO public.ducklake_data_file
        (data_file_id,table_id,begin_snapshot,end_snapshot,file_order,path,path_is_relative,file_format,record_count,file_size_bytes,footer_size,row_id_start,mapping_id)
        SELECT %s,table_id,%s,NULL,file_order,path,path_is_relative,file_format,record_count,file_size_bytes,footer_size,row_id_start,%s
        FROM public.ducklake_data_file WHERE data_file_id=%s''',(future_id,snap+1000,987654321,file_id),
        'DELETE FROM public.ducklake_data_file WHERE data_file_id=%s',(future_id,)):
        d.verify_current(r,a,guard=lambda:None);assert whole()==original
        assert io.catalog(r,b,lambda:None)[1]==before
    results.append(dict(case='未来追加文件及mapping引用目录探针',sha256=original_sha,current='pass',scope='本次新增目录实证，未生产未来文件正文'))
    for field,new in [('end_snapshot',snap),('mapping_id',987654321),('file_size_bytes',file['file_size_bytes']+1)]:
        with mutation(r.dsn,f'UPDATE public.ducklake_data_file SET {field}=%s WHERE data_file_id=%s',(new,file_id),f'UPDATE public.ducklake_data_file SET {field}=%s WHERE data_file_id=%s',(file[field],file_id)):
            with pytest.raises(ValueError):d.verify_current(r,a,guard=lambda:None)
        d.verify_current(r,a,guard=lambda:None)
        results.append(dict(case='实际可见文件'+field+'变化',current='reject'))
    column=next(x for x in before['catalog_rows']['ducklake_column'] if x['table_id']==table_id and x['column_name']=='attacker_asn')
    with mutation(r.dsn,'UPDATE public.ducklake_column SET column_type=%s WHERE table_id=%s AND column_id=%s',('VARCHAR',table_id,column['column_id']),
                  'UPDATE public.ducklake_column SET column_type=%s WHERE table_id=%s AND column_id=%s',(column['column_type'],table_id,column['column_id'])):
        with pytest.raises(ValueError):d.verify_current(r,a,guard=lambda:None)
    results.append(dict(case='当前可见列类型变化',current='reject'))
    with mutation(r.dsn,'INSERT INTO public.ducklake_inlined_data_tables VALUES (%s,%s,%s)',(table_id,'detection_p1_inline_probe',0),
                  'DELETE FROM public.ducklake_inlined_data_tables WHERE table_id=%s AND table_name=%s',(table_id,'detection_p1_inline_probe')):
        with pytest.raises(ValueError,match='inlining'):d.verify_current(r,a,guard=lambda:None)
    results.append(dict(case='当前可见inlining声明',current='reject'))
    d.verify_current(r,a,guard=lambda:None);assert whole()==original
    cost['catalog_repair']=results


def test_full_typed_and_receipts_unchanged(bundles):
    for r,b,a,events,cost in bundles:
        for table in io.TABLES:
            events.clear();rows,receipt=consume(r,a,request(table))
            assert rows==list(read_stored_rows(r.dsn,b['run_id'],b['snapshot'],table))
            assert not any(e['kind'] in ('full_table_scan','entity_hash') for e in events)
            cost.setdefault('repair_reads',{})[table]=dict(rows=len(rows),receipt=receipt)
