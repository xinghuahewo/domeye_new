"""Resource 私有 PG 人工 MRT：真实 M2→公共选择→独立结果→全新读取。"""
from dataclasses import asdict, replace
from datetime import timedelta
import gzip
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
import threading
import re
import uuid

import psycopg2
from psycopg2.extras import Json
import pytest
from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.analysis.resources.bindings import SourceBinding
from data_pipeline.analysis.resources.identity import ROOT, identity_digest
from data_pipeline.analysis.resources.references import read_reference
from data_pipeline.analysis.resources.observation import produce_observation_resources, ObservationResourceStore
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader
from data_pipeline.analysis.resources.qualification import ALL_TABLES
from tests.resources.test_resource_store import fixture_inputs
from tests.resources.test_resource_bindings import json_request


def launch(request,path):
    path.write_text(json.dumps(request,ensure_ascii=False));done=threading.Event();samples=[]
    def sample():
        while not done.is_set():
            samples.append(sum(p.stat().st_size for p in path.parent.rglob('*') if p.is_file()))
            done.wait(.1)
    thread=threading.Thread(target=sample);thread.start();start=time.monotonic()
    try:
        child=subprocess.run(['/usr/bin/time','-l',sys.executable,str(ROOT/'scripts/pipeline/resource-frozen-run.py'),str(path)],capture_output=True,text=True)
    finally:done.set();thread.join()
    rss=re.search(r'(\d+)\s+maximum resident set size',child.stderr)
    measurement=dict(wall_seconds=time.monotonic()-start,peak_rss_bytes=int(rss[1]) if rss else None,
        rss_scope='macOS time -l fresh launcher and child excludes PG',sampled_peak_disk_bytes=max(samples,default=0),
        disk_scope='test root including input/reference/catalog/output, excludes frozen code temp',sample_interval_seconds=.1,exit_code=child.returncode)
    path.with_suffix('.measurement.json').write_text(json.dumps(measurement,ensure_ascii=False,indent=2))
    if child.returncode:raise AssertionError(child.stderr)
    return json.loads(child.stdout)

from tests.observations.test_observation_mrt import update


@pytest.fixture
def own_dsn(tmp_path):
    base=os.environ.get('DOMEYE_RESOURCE_TEST_DSN')
    if not base:pytest.skip('需显式本任务私有PG')
    name='resource_m3_'+uuid.uuid4().hex
    pg=psycopg2.connect(base);pg.autocommit=True
    with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    pg.close();dsn=base+' dbname='+name
    (tmp_path/'database.json').write_text(json.dumps(dict(dsn=dsn,database=name)))
    return dsn


def input_set(root,*,bad=False,empty=False,ribs=8,uncertain_outlier=False):
    root.mkdir();m,contexts=fixture_inputs(root,ribs=ribs,count=2)
    # 最后snapshot可只有Peer表，无RIB元素；保留可解析EOF但不伪造空范围证明。
    if empty:
        e=m['inputs'][-1];p=Path(e['path']);raw=gzip.decompress(p.read_bytes());size=int.from_bytes(raw[8:12],'big')
        p.write_bytes(gzip.compress(b'' if empty=='all' else raw[:12+size],mtime=0));e.update(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),size=p.stat().st_size)
        e['source_id']=source_identity('rrc25',e['origin_uri'],e['sha256']);contexts[-1]=replace(contexts[-1],source_id=e['source_id'],content_sha256=e['sha256'])
    if uncertain_outlier:
        # 第8个点为空，旧算法以此前正常带将raw 0排除为异常；第9个点不能丢其历史依赖。
        e=m['inputs'][-2];p=Path(e['path']);raw=gzip.decompress(p.read_bytes())
        p.write_bytes(gzip.compress(raw[:12+int.from_bytes(raw[8:12],'big')],mtime=0))
        e.update(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),size=p.stat().st_size)
        e['source_id']=source_identity('rrc25',e['origin_uri'],e['sha256']);contexts[-2]=replace(contexts[-2],source_id=e['source_id'],content_sha256=e['sha256'])
    p=root/'update.gz';p.write_bytes(gzip.compress(update()+ (update(attrs=b'\xf0\x23\x04\0\x04\x2f\x66') if bad else b''),mtime=0))
    sha=hashlib.sha256(p.read_bytes()).hexdigest();uri='fixture://resource/update'
    e=dict(path=str(p),sha256=sha,size=p.stat().st_size,origin_uri=uri,role='update',source_id=source_identity('rrc25',uri,sha))
    # UPDATE夹在snapshot之间，来源rank不可按RIB子集重编号。
    m['inputs'].insert(2,e);m['update_sources']=[e['source_id']]
    alias={**m['inputs'][1],'path':str(root/'alias.gz')};Path(alias['path']).write_bytes(Path(m['inputs'][1]['path']).read_bytes())
    m['inputs'].append(alias)
    return m,contexts


def prepare(root,dsn,*,bad=False,empty=False,ribs=8,split=False,uncertain_outlier=False):
    m,contexts=input_set(root/'inputs',bad=bad,empty=empty,ribs=ribs,uncertain_outlier=uncertain_outlier)
    t=time.monotonic();seal=produce_checkpointed(m,dsn,root/'m2',policy='isolate-payload/v1' if bad else 'strict/v1',
        catalog_data_path=root/'catalog',min_free_bytes=0,batch_rows=3)
    second=None
    if split:
        entries=[e for e in m['inputs'] if e['source_id'] in {c.source_id for c in contexts[-2:]}]
        manifest={**m,'inputs':[{**e,'role':'baseline' if i==0 else 'snapshot'} for i,e in enumerate(entries)],'baseline_source':entries[0]['source_id'],'update_sources':[],'references':[]}
        second=produce_checkpointed(manifest,dsn,root/'m2-second',catalog_data_path=root/'catalog',min_free_bytes=0,batch_rows=3)
    m2_wall=time.monotonic()-t
    path=root/'supplement.json';path.write_text('{"9808":{"country_cn":"甲"},"100":{"country_cn":"甲"}}')
    ref=launch(dict(operation='register_reference',dsn=dsn,path=str(path),output=str(root/'reference'),origin_uri='fixture://reference',
        content_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),min_free_bytes=0),root/'reference-request.json')
    country=dict(reference_id=ref['reference_id'],dataset_id=ref['dataset_id'])
    identity=read_reference(dsn,**country)[1]
    csv=dict(run_id=seal['run_id'],snapshot=seal['snapshot'],anchor_source_id=contexts[0].source_id,source_id=m['references'][0]['sha256'])
    reference_id=identity_digest(dict(csv=csv,country=identity))
    sources=[SourceBinding(seal['run_id'],seal['snapshot'],'warmup' if i<len(contexts)-2 else 'result',replace(c,reference_id=reference_id)) for i,c in enumerate(contexts)]
    if second:sources=[replace(s,run_id=second['run_id'],snapshot=second['snapshot']) if s.purpose=='result' else s for s in sources]
    request=dict(operation='resource_observation',dsn=dsn,sources=sources,result_window=[contexts[-2].snapshot_time,contexts[-1].snapshot_time+timedelta(hours=8)],
        csv_binding=csv,country_binding=country,output=str(root/'resource'),min_free_bytes=0)
    return request,seal,dict(m2_wall_seconds=m2_wall,compressed_input_bytes=sum(Path(e['path']).stat().st_size for e in m['inputs'][:-1]),
        m2_messages=sum(c['counts']['messages'] for c in seal['checkpoints']),m2_elements=sum(c['counts']['elements'] for c in seal['checkpoints']),second_m2=second)


def read_all(dsn,report):
    reader=ResourceObservationReader(dsn,report['run_id'],report['snapshot'],report['dataset_id'])
    return {t:[row for b in reader.scan(t,scope='all') for row in b.to_pylist()] for t in ALL_TABLES}


def test_fresh_m2_resource_reader_and_old_typed_equivalence(tmp_path,own_dsn):
    request,seal,measure=prepare(tmp_path,own_dsn,split=True)
    t=time.monotonic();report=launch(json_request(request),tmp_path/'resource-request.json');measure['resource_fresh_wall_seconds']=time.monotonic()-t
    assert report['schema_version']=='resource-observation/v1'
    # 全新解释器经公开Reader读取，实际校验全部表；测试脚本属于测试，不冒生产冻入口。
    config=tmp_path/'read.json';config.write_text(json.dumps(dict(dsn=own_dsn,report=report)))
    script=tmp_path/'read.py';script.write_text('''import json,sys,time,resource
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader
c=json.load(open(sys.argv[1]));p=c['report'];t=time.monotonic()
r=ResourceObservationReader(c['dsn'],p['run_id'],p['snapshot'],p['dataset_id'])
v=list(r.values());assert v and all(x['main']['ipv4_prefix_count']==2 for x in v)
assert any(x['main']['is_outlier'] is not None for x in v)
print(json.dumps({'values':len(v),'wall_seconds':time.monotonic()-t,'peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}))
''')
    out=subprocess.run([sys.executable,str(script),str(config)],env={**os.environ,'PYTHONPATH':str(ROOT/'backend')},capture_output=True,text=True,check=True)
    measure['fresh_reader']=json.loads(out.stdout)
    rows=read_all(own_dsn,report)
    assert len(rows['sources'])==8 and len(rows['input_receipts'])==8
    assert sorted(r['source_rank'] for r in rows['input_receipts'])==[0,0,1,1,3,4,5,6]
    assert all(r['status']=='qualified' for r in rows['coverage'] if r['dimension']=='rib_point')
    public=ResourceObservationReader(own_dsn,report['run_id'],report['snapshot'],report['dataset_id'])
    assert all(v['main']['upper_bound'] is not None for v in public.values(target='normal_bands'))
    assert all(v['main']['edge_count']==1 for v in public.values(target='topology_status'))
    assert len(public.inputs()['binding']['observation_inputs'])==8

    assert any(q['status']=='unknown' for q in rows['qualifications'] if q['metric']=='is_outlier')
    # 同一M2 typed输入直接驱动原计算器，逐字段核对旧十表（仅运行ID引用重绑定）。
    from data_pipeline.analysis.resources.compute import ResourceComputer, RibElement
    from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch
    from data_pipeline.analysis.resources.adapter import reference_projection
    from itertools import chain
    import pyarrow as pa
    csv=request['csv_binding'];cr=ObservationReader(own_dsn,seal['run_id'],seal['snapshot'],[csv['anchor_source_id']],profile='observation')
    countries,identity=read_reference(own_dsn,**request['country_binding'])
    rr=[dict(source_id=request['country_binding']['dataset_id'],row=i,location=k,raw_row=json.dumps({'__object_pairs__':[['country_cn',v[0]]]})) for i,(k,v) in enumerate(countries.items())]
    refs=reference_projection(chain(cr.reference_batches(csv['source_id']),[pa.Table.from_pylist(rr)]),csv_source=csv['source_id'],country_source=request['country_binding']['dataset_id'])
    computer=ResourceComputer(refs,topology_enabled=True);compared=0
    from data_pipeline.analysis.resources.compute import METRIC_UNITS
    for binding in request['sources']:
        rid=binding.context.source_id;r=ObservationReader(own_dsn,binding.run_id,binding.snapshot,[rid],profile='observation')
        elements=[e for b in r.stream() if isinstance(b,MessageBatch) for e in b.elements]
        result=computer.compute(binding.context,[RibElement(e['source_id'],e['message_id'],e['ordinal'],e['prefix'],e['as_path_text'],str(e['peer_asn']),e['peer_ip'],e['bgp_id'] if e['bgp_id_present'] else None,e['path_key'],e['afi'],e['safi'],e['action'],e['attributed_origin_asn']) for e in elements])
        for raw in (*result.rows,*result.vp_rows):
            stored=next(x for x in rows['metrics'] if (x['source_id'],x['dimension'],x['bucket'])==(rid,raw.dimension,raw.bucket))
            for metric in (*METRIC_UNITS,'is_outlier','as_name','as_rank','reference_row_ref'):
                expected=getattr(raw,metric)
                if raw.dimension=='peer_asn' and metric in METRIC_UNITS and metric not in ('ipv4_prefix_count','ipv6_prefix_count','ipv6_48_count'):expected=None
                assert stored[metric]==expected;compared+=1
            for kind in ('ipv4_prefix','ipv6_prefix','vp_set','private_as','public_as','path'):
                actual={x['member'] for x in rows['memberships'] if (x['source_id'],x['dimension'],x['bucket'],x['kind'])==(rid,raw.dimension,raw.bucket,kind)}
                expected=getattr(raw,kind)
                if kind=='path':expected={hashlib.sha256(x.encode()).hexdigest() for x in expected}
                assert actual==expected;compared+=1
            ranges=result.state.normal_range if raw.dimension=='first_path_asn' else result.state.vp_normal_range
            for metric,band in ranges[raw.bucket].items():
                b=next(x for x in rows['normal_bands'] if (x['source_id'],x['dimension'],x['bucket'],x['metric'])==(rid,raw.dimension,raw.bucket,metric))
                for field in ('list_len','upper_bound','lower_bound','mean','population_std'):assert b[field]==getattr(band,field);compared+=1
                samples=[x for x in rows['normal_samples'] if (x['source_id'],x['dimension'],x['bucket'],x['metric'])==(rid,raw.dimension,raw.bucket,metric)]
                assert {(s['sample_source'],s['sample_time'],s['value']) for s in samples}=={(s.source_id,s.time,s.value) for s in band.samples}
        assert {(d.message_id+':'+str(d.ordinal),d.resource_status) for d in result.decisions}=={(d['event_id'],d['status']) for d in rows['decision_refs'] if d['source_id']==rid}
        assert next(x for x in rows['sources'] if x['source_id']==rid)['element_count']==len(elements)
        for graph in result.topology:
            status=next(x for x in rows['topology_status'] if x['source_id']==rid and x['country_cn']==graph.country_cn)
            for field in ('build_time','status','legacy_replace_edges','legacy_update_snapshot','link_color','node_color'):
                assert status[field]==getattr(graph,field);compared+=1
            assert (status['node_count'],status['edge_count'])==(len(graph.nodes),len(graph.edges))
            assert {(int(e['a_asn']),int(e['b_asn'])) for e in rows['topology_edges'] if e['source_id']==rid and e['country_cn']==graph.country_cn}==set(graph.edges)
    assert {p['path_text'] for p in rows['rendered_paths']}=={'9808 100'}
    assert rows['decoding_differences']==[]
    # 保留的最终工作态与原算法导出逐字段对照，不伪装为checkpoint恢复。
    with psycopg2.connect(own_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT rib_count,last_source,initial_state_basis FROM domeye.resource_work_meta WHERE run_id=%s',(report['run_id'],))
        assert c.fetchone()==(computer.state.rib_count,computer.state.last_context.source_id,computer.state.initial_state_basis)
        c.execute('SELECT bucket,currently_abnormal FROM domeye.resource_work_abnormal WHERE run_id=%s',(report['run_id'],))
        assert dict(c.fetchall())==computer.state.currently_abnormal
        c.execute('SELECT dimension,bucket,metric,list_len,upper_bound,lower_bound,mean,population_std FROM domeye.resource_work_bands WHERE run_id=%s',(report['run_id'],))
        expected={(dimension,b,m,v.list_len,v.upper_bound,v.lower_bound,v.mean,v.population_std) for dimension,ranges in [('first_path_asn',computer.state.normal_range),('peer_asn',computer.state.vp_normal_range)] for b,metrics in ranges.items() for m,v in metrics.items()}
        assert set(c.fetchall())==expected
        c.execute('SELECT dimension,bucket,metric,source_id,time,value FROM domeye.resource_work_samples WHERE run_id=%s',(report['run_id'],))
        expected={(dimension,b,m,x.source_id,x.time,x.value) for dimension,ranges in [('first_path_asn',computer.state.normal_range),('peer_asn',computer.state.vp_normal_range)] for b,metrics in ranges.items() for m,v in metrics.items() for x in v.samples}
        assert set(c.fetchall())==expected
    measure.update(compared_typed_fields=compared,report=report,parquet_bytes=sum(p.stat().st_size for p in (tmp_path/'catalog').rglob('*.parquet')),
        retained_temporary_disk_bytes=sum(p.stat().st_size for p in tmp_path.rglob('*') if p.is_file()),peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    (tmp_path/'validation.json').write_text(json.dumps(measure,ensure_ascii=False,indent=2))


def test_independent_rib_bad_update_empty_and_actual_negative_gates(tmp_path,own_dsn,monkeypatch):
    request,seal,measure=prepare(tmp_path,own_dsn,bad=True,empty=True,ribs=3)
    report=launch(json_request(request),tmp_path/'resource-request.json');rows=read_all(own_dsn,report)
    assert sum(cp['counts']['rejected'] for cp in seal['checkpoints'])==1
    assert all(r['gaps']==0 for r in rows['input_receipts'])
    sid=request['sources'][-1].context.source_id
    assert any(r['source_id']==sid and r['elements']==0 for r in rows['input_receipts'])
    values=list(ResourceObservationReader(own_dsn,report['run_id'],report['snapshot'],report['dataset_id']).values())
    assert any(v['raw']['source_id']==sid and v['raw']['ipv4_prefix_count']==0 and v['main']['ipv4_prefix_count'] is None for v in values)
    assert any(v['raw']['source_id']!=sid and v['main']['ipv4_prefix_count']==2 for v in values)
    for snapshot,dataset in [(report['snapshot']+1,report['dataset_id']),(report['snapshot'],'0'*64)]:
        with pytest.raises(ValueError):ResourceObservationReader(own_dsn,report['run_id'],snapshot,dataset)
    for state in ('candidate','failed'):
        with psycopg2.connect(own_dsn) as pg,pg.cursor() as c:c.execute('UPDATE domeye.resource_runs SET state=%s WHERE run_id=%s',(state,report['run_id']))
        with pytest.raises(ValueError):ResourceObservationReader(own_dsn,report['run_id'],report['snapshot'],report['dataset_id'])
    with psycopg2.connect(own_dsn) as pg,pg.cursor() as c:c.execute("UPDATE domeye.resource_runs SET state='complete' WHERE run_id=%s",(report['run_id'],))
    # 真实PG错误seal在生产前拒绝，恢复测试原metadata后才开展下一例。
    with psycopg2.connect(own_dsn) as pg,pg.cursor() as c:c.execute('UPDATE observation_m2.runs SET seal=%s WHERE run_id=%s',(Json({**seal,'digest':'0'*64}),seal['run_id']))
    args={k:v for k,v in request.items() if k!='operation'}
    with pytest.raises(ValueError):produce_observation_resources(**{**args,'output':str(tmp_path/'bad-seal'),'fixture_only':True})
    with pytest.raises(ValueError):list(ResourceObservationReader(own_dsn,report['run_id'],report['snapshot'],report['dataset_id']).values())
    with psycopg2.connect(own_dsn) as pg,pg.cursor() as c:c.execute('UPDATE observation_m2.runs SET seal=%s WHERE run_id=%s',(Json(seal),seal['run_id']))
    for index,sources in enumerate(([request['sources'][0],*request['sources']],list(reversed(request['sources'])),[replace(request['sources'][0],snapshot=seal['snapshot']+1),*request['sources'][1:]])):
        with pytest.raises(ValueError):produce_observation_resources(**{**args,'sources':sources,'output':str(tmp_path/f'bad-selection-{index}'),'fixture_only':True})
    original=ObservationResourceStore.finish
    for case in ('missing_peer','false_count','missing_normal','false_normal_value','unknown_table'):
        def damage(store,details,case=case):
            store.flush()
            if case=='missing_peer':store.db.execute(f'DELETE FROM lake.{store.schema}.peer_dependencies')
            elif case=='false_count':store.db.execute(f'UPDATE lake.{store.schema}.input_receipts SET elements=elements+1')
            elif case=='missing_normal':store.db.execute(f'UPDATE lake.{store.schema}.normal_samples SET sample_source=\'missing\'')
            elif case=='false_normal_value':store.db.execute(f'UPDATE lake.{store.schema}.normal_samples SET value=value+1')
            else:store.db.execute(f'CREATE TABLE lake.{store.schema}.unknown (x BIGINT)')
            return original(store,details)
        with monkeypatch.context() as p:
            p.setattr(ObservationResourceStore,'finish',damage)
            with pytest.raises(ValueError):produce_observation_resources(**{**args,'output':str(tmp_path/case),'fixture_only':True})
        failure=json.loads((tmp_path/case/'failure.json').read_text());assert failure['state']=='failed'
    import pyarrow as pa
    import pyarrow.parquet as pq
    directory=Path(report['storage_layout']['catalog_data_path'])/report['storage_layout']['schema_path']
    file=next(p for p in directory.rglob('*.parquet') if {'qualification_id','status'}<=set(pq.read_schema(p).names))
    before=file.read_bytes();table=pq.read_table(file)
    with psycopg2.connect(own_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT data_file_id,file_size_bytes,footer_size FROM public.ducklake_data_file WHERE path=%s',(file.name,));file_id,size,footer=c.fetchone()
    col=table.schema.get_field_index('status');pq.write_table(table.set_column(col,table.schema.field(col),pa.array(['invalid_status']*len(table))),file)
    after=file.read_bytes();delta=int.from_bytes(after[-8:-4],'little')-int.from_bytes(before[-8:-4],'little')
    with psycopg2.connect(own_dsn) as pg,pg.cursor() as c:c.execute('UPDATE public.ducklake_data_file SET file_size_bytes=%s,footer_size=%s WHERE data_file_id=%s',(len(after),footer+delta,file_id))
    with pytest.raises(ValueError,match='全表内容'):list(ResourceObservationReader(own_dsn,report['run_id'],report['snapshot'],report['dataset_id']).values())
    file.write_bytes(before)
    with psycopg2.connect(own_dsn) as pg,pg.cursor() as c:c.execute('UPDATE public.ducklake_data_file SET file_size_bytes=%s,footer_size=%s WHERE data_file_id=%s',(size,footer,file_id))
    (tmp_path/'validation.json').write_text(json.dumps(dict(report=report,measure=measure,negative_cases=['wrong_snapshot','wrong_dataset','candidate','failed','bad_seal','missing_peer','false_count','missing_normal','same_count_physical_qualification_tamper']),ensure_ascii=False,indent=2))


def test_damaged_rib_still_fails_m2(tmp_path,own_dsn):
    m,contexts=input_set(tmp_path/'inputs',ribs=2)
    entry=m['inputs'][0];p=Path(entry['path']);raw=gzip.decompress(p.read_bytes());p.write_bytes(gzip.compress(raw[:-1],mtime=0))
    entry.update(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),size=p.stat().st_size);entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256']);m['baseline_source']=entry['source_id']
    with pytest.raises((ValueError,EOFError)):
        produce_checkpointed(m,own_dsn,tmp_path/'bad-m2',policy='isolate-payload/v1',catalog_data_path=tmp_path/'catalog',min_free_bytes=0)
    with psycopg2.connect(own_dsn) as pg,pg.cursor() as c:
        c.execute("SELECT count(*) FROM observation_m2.runs WHERE state='observation_sealed'");assert c.fetchone()[0]==0


def test_zero_message_snapshot_keeps_independent_coverage(tmp_path,own_dsn):
    request,seal,measure=prepare(tmp_path,own_dsn,empty='all',ribs=2)
    report=launch(json_request(request),tmp_path/'resource-request.json')
    reader=ResourceObservationReader(own_dsn,report['run_id'],report['snapshot'],report['dataset_id'])
    receipts=[r for b in reader.scan('input_receipts') for r in b.to_pylist()]
    assert any(r['messages']==r['elements']==r['peers']==0 for r in receipts)
    assert len([r for b in reader.coverage() for r in b.to_pylist()])==8
    values=list(reader.values())
    assert any(v['raw']['ipv4_prefix_count']==0 and v['main']['ipv4_prefix_count'] is None for v in values)


def test_excluded_uncertain_predecessor_is_still_normal_dependency(tmp_path,own_dsn):
    request,seal,measure=prepare(tmp_path,own_dsn,ribs=9,uncertain_outlier=True)
    report=launch(json_request(request),tmp_path/'resource-request.json')
    public=ResourceObservationReader(own_dsn,report['run_id'],report['snapshot'],report['dataset_id'])
    raw=list(public.values());bad=request['sources'][-2].context.source_id;last=request['sources'][-1].context.source_id
    assert any(v['raw']['source_id']==bad and v['raw']['is_outlier'] and v['main']['ipv4_prefix_count'] is None for v in raw)
    assert any(v['raw']['source_id']==last and v['main']['ipv4_prefix_count']==2 and v['main']['is_outlier'] is None for v in raw)
    normal=list(public.values(target='normal_bands'))
    assert all(v['main']['upper_bound'] is None for v in normal)
    samples=[r for b in public.scan('normal_samples') for r in b.to_pylist()]
    assert all(r['sample_source']!=bad for r in samples if r['source_id']==last)
    deps=[r for b in public.scan('qualification_dependencies') for r in b.to_pylist()]
    assert any(r['source_id']==last and r['kind']=='normal_history' and r['dependency_id']==bad for r in deps)
