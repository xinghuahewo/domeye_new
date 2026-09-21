"""三项独立复核的真实私有PG反例；不连接审查者或真实业务数据库。"""
from dataclasses import replace
from datetime import datetime
from data_pipeline.analysis.resources.bindings import SourceBinding
from data_pipeline.analysis.resources.compute import RibContext
import json
import os
from pathlib import Path
import time
import uuid

import psycopg2
import pytest

from data_pipeline.bgp.archive.checkpoint import AttemptStore
from data_pipeline.analysis.resources.observation import ObservationResourceStore, produce_observation_resources
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader
from tests.resources.test_resource_observation import prepare, launch
from tests.resources.test_resource_bindings import json_request


@pytest.fixture(scope='module')
def cases(tmp_path_factory):
    base=os.environ.get('DOMEYE_RESOURCE_TEST_DSN')
    if not base:pytest.skip('需本任务自有PG')
    root=tmp_path_factory.mktemp('repair');cases={}
    reuse=os.environ.get('DOMEYE_RESOURCE_REPAIR_CASE_ROOT')
    if reuse:
        for kind in ('regular','excluded'):
            saved=json.loads((Path(reuse)/kind/'case.json').read_text());request=saved['request']
            request['sources']=[SourceBinding(s['run_id'],s['snapshot'],s['purpose'],RibContext(**{**s['context'],'snapshot_time':datetime.fromisoformat(s['context']['snapshot_time'])})) for s in request['sources']]
            directory=root/kind;directory.mkdir();cases[kind]=(directory,request,saved['report'])
        return cases
    for kind in ('regular','excluded'):
        directory=root/kind;directory.mkdir();name='resource_repair_'+uuid.uuid4().hex
        pg=psycopg2.connect(base);pg.autocommit=True
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
        pg.close();dsn=base+' dbname='+name
        original=AttemptStore.message
        def with_quality(store,m):
            original(store,m)
            if m.record==0 and m.mrt_type==13:
                # 人工诊断经真实M2写入/checkpoint；不声称真实RIB解析自然产生该诊断。
                for mid in (None,m.message_id):
                    store.append('quality',dict(source_id=m.source_id,message_id=mid,code='fixture_audit',detail='原始诊断'))
        AttemptStore.message=with_quality
        try:request,seal,measure=prepare(directory,dsn,ribs=9,uncertain_outlier=kind=='excluded')
        finally:AttemptStore.message=original
        report=launch(json_request(request),directory/'healthy-request.json')
        (directory/'case.json').write_text(json.dumps(dict(request=json_request(request),report=report,measure=measure),ensure_ascii=False))
        cases[kind]=(directory,request,report)
    return cases


@pytest.mark.parametrize('case',['peer','quality','future','excluded'])
def test_finish_rejects_wrong_actual_dependency(cases,monkeypatch,case):
    directory,request,healthy=cases['excluded' if case=='excluded' else 'regular']
    original=ObservationResourceStore.finish
    def damage(store,details):
        store.flush()
        if case=='peer':store.db.execute(f'UPDATE lake.{store.schema}.peer_dependencies SET peer_index=peer_index+999,peer_asn=999999')
        elif case=='quality':store.db.execute(f"UPDATE lake.{store.schema}.observation_quality SET detail='错误但同计数'")
        else:
            sources=request['sources'];target=sources[-1 if case=='excluded' else -2]
            old=sources[0 if case=='excluded' else -3];new=sources[-2 if case=='excluded' else -1]
            extra=",value=0" if case=='excluded' else ''
            where=" AND dimension='first_path_asn' AND bucket='global'" if case=='excluded' else ''
            store.db.execute(f'UPDATE lake.{store.schema}.normal_samples SET sample_source=?,sample_time=?{extra} WHERE source_id=? AND sample_source=?{where}',
                [new.context.source_id,new.context.snapshot_time,target.context.source_id,old.context.source_id])
        return original(store,details)
    monkeypatch.setattr(ObservationResourceStore,'finish',damage)
    args={k:v for k,v in request.items() if k!='operation'};started=time.monotonic();result=None;error=None
    try:result=produce_observation_resources(**{**args,'output':str(directory/case),'fixture_only':True})
    except ValueError as exc:error=str(exc)
    evidence=dict(case=case,rejected=error is not None,error=error,report=result,wall_seconds=time.monotonic()-started)
    (directory/(case+'-result.json')).write_text(json.dumps(evidence,ensure_ascii=False))
    assert error is not None,'错误依赖被完成门禁接受：'+case
    failure=json.loads((directory/case/'failure.json').read_text());assert failure['state']=='failed'


@pytest.mark.parametrize('target',['m2','reference'])
def test_inputs_tail_revocation_rejects_and_closes(cases,monkeypatch,target):
    directory,request,report=cases['regular'];dsn=request['dsn']
    reader=ResourceObservationReader(dsn,report['run_id'],report['snapshot'],report['dataset_id'])
    connect=reader._connect;closed=[]
    table='observation_m2.runs' if target=='m2' else 'domeye.resource_references'
    key='run_id' if target=='m2' else 'reference_id'
    identity=request['sources'][0].run_id if target=='m2' else request['country_binding']['reference_id']
    restored='observation_sealed' if target=='m2' else 'complete'
    def state(value):
        with psycopg2.connect(dsn) as pg,pg.cursor() as cur:cur.execute(f'UPDATE {table} SET state=%s WHERE {key}=%s',(value,identity))
    class Tracked:
        def __init__(self,db):self.db=db
        def __getattr__(self,k):return getattr(self.db,k)
        def close(self):closed.append(True);self.db.close()
    def revoke():
        db=connect();state('failed');return Tracked(db)
    monkeypatch.setattr(reader,'_connect',revoke);error=None
    try:
        try:reader.inputs()
        except ValueError as exc:error=str(exc)
    finally:state(restored)
    (directory/(target+'-tail.json')).write_text(json.dumps(dict(rejected=error is not None,error=error,closed=closed),ensure_ascii=False))
    assert error is not None,'返回前依赖撤销仍成功：'+target
    assert closed==[True]


def test_original_normal_selection_retention_is_accepted():
    """原算法构造冷启动、异常排除、稀疏桶、3天清理和非减保留正例。"""
    from datetime import timezone,timedelta
    from data_pipeline.analysis.resources.compute import ResourceComputer, RibElement, RibContext, ITEMS
    from data_pipeline.analysis.resources.validation import validate_normal_history
    start=datetime(2026,1,1,tzinfo=timezone.utc);computer=ResourceComputer({})
    sources={};metrics=[];bands=[];normal={};results=[]
    for i in range(15):
        stamp=start+timedelta(hours=i*8+(168 if i>=10 else 0));sid='rib'+str(i)
        context=RibContext(sid,'rrc25',stamp,'fixture','fixture')
        count=20 if i==7 else 2;peer='64497' if i<10 else '64498'
        result=computer.compute(context,[RibElement(sid,sid+':'+str(j),0,f'10.0.{j}.0/24','9808 100',peer) for j in range(count)])
        results.append(result);sources[sid]=dict(source_id=sid,snapshot_time=stamp)
        for r in (*result.rows,*result.vp_rows):
            metrics.append(dict(source_id=sid,dimension=r.dimension,bucket=r.bucket,time=stamp,is_outlier=r.is_outlier,**{m:getattr(r,m) for m in ITEMS}))
            ranges=result.state.normal_range if r.dimension=='first_path_asn' else result.state.vp_normal_range
            for m,b in ranges[r.bucket].items():
                bands.append(dict(source_id=sid,dimension=r.dimension,bucket=r.bucket,metric=m,list_len=b.list_len))
                if b.samples:normal[(sid,r.dimension,r.bucket,m)]=[dict(sample_source=x.source_id,sample_time=x.time,value=x.value) for x in b.samples]
    assert results[7].rows[0].is_outlier
    assert normal[('rib0','first_path_asn','global','ipv4_prefix_count')][0]['sample_source']=='rib0'
    assert 'rib7' not in {r['sample_source'] for r in normal[('rib8','first_path_asn','global','ipv4_prefix_count')]}
    assert any(r['sample_time']<sources['rib14']['snapshot_time']-timedelta(days=3) for r in normal[('rib14','first_path_asn','global','ipv4_prefix_count')])
    validate_normal_history(sources,metrics,bands,normal,lambda:None)
