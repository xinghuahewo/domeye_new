"""自有新PG的3个RIB：M2 baseline最晚，Resource按时点消费，实际P1接合。"""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import time
import uuid
import psycopg2
import pytest
from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
from data_pipeline.bgp.archive import admission as up
from data_pipeline.analysis.resources import publication as p
from data_pipeline.analysis.resources.bindings import SourceBinding
from data_pipeline.analysis.resources.identity import identity_digest
from data_pipeline.analysis.resources.references import read_reference
from data_pipeline.analysis.resources.observation_reader import ResourceObservationReader
from data_pipeline.analysis.resources.publication_codec import typed, untyped, CODEC
from tests.resources.test_resource_store import fixture_inputs
from tests.resources.test_resource_observation import launch
from tests.resources.test_resource_bindings import json_request


def test_three_rib_different_orders(tmp_path):
    base=os.environ.get('DOMEYE_RESOURCE_ORDERS_DSN')
    if not base:pytest.skip('需显式本任务新隔离PG')
    name='resource_orders_'+uuid.uuid4().hex
    pg=psycopg2.connect(base);pg.autocommit=True
    with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    pg.close();dsn=base+' dbname='+name
    root=tmp_path;inputs=root/'inputs';inputs.mkdir();scratch=root/'scratch';scratch.mkdir()
    timings={}
    def measure(label,fn):
        start=time.monotonic();value=fn();timings[label]=time.monotonic()-start;return value
    manifest,contexts=fixture_inputs(inputs,ribs=3)
    manifest['inputs']=[manifest['inputs'][i] for i in (2,0,1)]
    for i,entry in enumerate(manifest['inputs']):entry['role']='baseline' if i==0 else 'snapshot'
    manifest['baseline_source']=manifest['inputs'][0]['source_id']
    seal=measure('M2人工生产',lambda:produce_checkpointed(manifest,dsn,root/'m2',catalog_data_path=root/'catalog',batch_rows=3,
        min_free_bytes=128*1024**2,max_rss_bytes=2*1024**3,duckdb_memory_limit='256MB'))
    path=inputs/'country.json'
    reference=measure('国家参考人工登记',lambda:launch(dict(operation='register_reference',dsn=dsn,path=str(path),output=str(root/'reference'),origin_uri='fixture://reference',
        content_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),min_free_bytes=128*1024**2,max_rss_bytes=2*1024**3),root/'reference-request.json'))
    country={'reference_id':reference['reference_id'],'dataset_id':reference['dataset_id']}
    country_identity=read_reference(dsn,**country)[1]
    csv=dict(run_id=seal['run_id'],snapshot=seal['snapshot'],anchor_source_id=contexts[0].source_id,source_id=manifest['references'][0]['sha256'])
    refid=identity_digest(dict(csv=csv,country=country_identity))
    sources=[SourceBinding(seal['run_id'],seal['snapshot'],'warmup' if i<2 else 'result',replace(c,reference_id=refid)) for i,c in enumerate(contexts)]
    raw_request=dict(operation='resource_observation',dsn=dsn,sources=sources,result_window=[contexts[-1].snapshot_time,contexts[-1].snapshot_time+timedelta(hours=8)],
        csv_binding=csv,country_binding=country,output=str(root/'resource'),min_free_bytes=128*1024**2,max_rss_bytes=2*1024**3)
    report=measure('Resource人工生产',lambda:launch(json_request(raw_request),root/'resource-request.json'))
    reader=ResourceObservationReader(dsn,report['run_id'],report['snapshot'],report['dataset_id'])
    binding=reader.inputs();before_files={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in (root/'catalog').rglob('*') if f.is_file()}
    fx=up.Runtime(dsn,(root,),scratch,fixture_only=True)
    global_ids=[e['source_id'] for e in manifest['inputs']]
    original=up.inspect_binding(fx,seal['run_id'],seal['snapshot'],global_ids)
    def dependencies(chosen):
        selected=up.inspect_binding(fx,seal['run_id'],seal['snapshot'],chosen)
        rt=up.Runtime(dsn,(root,),scratch,execution_profile='real-candidate/v1',input_manifest=manifest,expected_m2_binding=selected,
            memory_limit='256MB',max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,min_free_bytes=128*1024**2,lock_timeout_ms=2000)
        m=up.admit(rt,selected,guard=lambda:None);rt.dependency_admissions=(m,)
        ref=up.admit(rt,up.reference_binding(rt,m,csv['source_id']),guard=lambda:None)
        return rt,(m,ref)
    upstream,deps=measure('M2选择与参考准入',lambda:dependencies(global_ids))
    def resource_runtime(deps,upstream):return p.Runtime(dsn,(root,),scratch,None,deps,execution_profile='real-candidate/v1',expected_resource_binding=binding,output_root=root/'resource',
        dependency_runtimes={d['admission_id']:upstream for d in deps},max_rows=1000000,max_bytes=256*1024**2,max_rss_bytes=2*1024**3,memory_bytes=256*1024**2,
        max_temp_bytes=512*1024**2,min_free_bytes=128*1024**2,lock_timeout_ms=2000)
    rt=resource_runtime(deps,upstream)
    a=measure('Resource准入',lambda:p.admit(rt,binding,guard=lambda:None))
    measure('current',lambda:p.verify_current(rt,a,guard=lambda:None))
    assert p.admit(rt,binding,guard=lambda:None)==a
    needed=[s['context']['source_id'] for s in binding['binding']['sources']]
    ranks=[binding['binding']['observation_inputs'][sid]['source_rank'] for sid in needed]
    assert ranks==[1,2,0] and needed==[c.source_id for c in contexts]
    assert up.untyped(deps[0]['owner_binding'])['ordered_source_ids']==global_ids
    assert len({v['binding_id'] for v in binding['binding']['observation_inputs'].values()})==1
    receipts=[r for batch in reader.scan('input_receipts',scope='all') for r in batch.to_pylist()]
    assert {r['source_id']:r['source_rank'] for r in receipts}==dict(zip(needed,ranks))
    request=dict(view='metrics',scope_typed=typed({'scope':'all'}),codec_version=CODEC,batch_rows=3,batch_bytes=1048576)
    start=time.monotonic()
    with p.open_reader(rt,a,request,guard=lambda:None) as session:rows=[r for batch in session for r in untyped(batch['rows_typed'])]
    timings['完整metrics读取']=time.monotonic()-start
    assert session.receipt and session.receipt['rows']==len(rows)
    expected=list(reader.values(target='metrics',scope='all'))
    norm=lambda r:typed({**r,'qualification':sorted(r['qualification'],key=typed)})
    assert sorted(map(norm,rows))==sorted(map(norm,expected))
    errors={}
    for kind in ('rank','duplicate','seal','checkpoint'):
        bad=deepcopy(binding);m=bad['binding'];bid=next(iter(m['observation_runs']))
        if kind=='rank':m['observation_inputs'][needed[0]]['source_rank']=0
        elif kind=='duplicate':m['sources'].append(deepcopy(m['sources'][0]))
        elif kind=='seal':m['observation_runs'][bid]['seal']['digest']='0'*64
        else:m['observation_inputs'][needed[0]]['checkpoint']['ordinal']=999
        with pytest.raises(ValueError) as caught:p._dependencies(rt,bad,lambda:None)
        errors[kind]=str(caught.value)
    smaller,small_deps=dependencies(global_ids[:2]);small_rt=resource_runtime(small_deps,smaller)
    with pytest.raises(ValueError,match='缺少所需RIB') as caught:p.admit(small_rt,binding,guard=lambda:None)
    errors['missing_actual_selection']=str(caught.value)
    assert reader.inputs()==binding
    assert before_files=={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in (root/'catalog').rglob('*') if f.is_file()}
    assert not list(scratch.iterdir())
    for file,value in [('ResourceAdmission.json',a),('Resource完整Binding.json',binding),('M2参考Admission.json',list(deps)),('M2原完整Binding.json',original),('原manifest.json',manifest),
        ('验证回执.json',dict(timings=timings,resource_ranks=ranks,m2_source_order=global_ids,resource_source_order=needed,errors=errors,read=session.receipt,usage=rt.resource_usage,
            source_elements=sum(x['checkpoint']['counts']['elements'] for x in binding['binding']['observation_inputs'].values()),inventory=report['inventory'],catalog_files_unchanged=len(before_files),dsn=dsn))]:
        (root/file).write_text(json.dumps(value,ensure_ascii=False,indent=2))
