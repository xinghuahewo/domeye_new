"""自有人工MRT→M2封存→共用ordered→双Feature→冻结公开读取。"""
from dataclasses import asdict,fields
from datetime import datetime,timezone
import csv
import gzip
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys
import time
import resource
import psycopg2
import pytest
from tests.observations.test_observation_consumer import feature_dsn
from tests.observations.test_observation_two_phase import fixture_manifest
from tests.observations.test_observation_mrt import update, attr, mrt
from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.analysis.features.inputs import FeatureInputs, SourceView, ReferenceView
from data_pipeline.analysis.features.calculation import FileWindow
from data_pipeline.analysis.features.reference import USECOLS
from data_pipeline.analysis.features.run import produce_bound_features
from data_pipeline.analysis.features.qualification import PROFILE, DIMENSIONS
from data_pipeline.analysis.features.store import read_table, FeatureStore
from data_pipeline.analysis.features.qualified_read import inspect_binding, read_windows, read_coverage


def raw_update(epoch,**kw):
    raw=update(**kw)
    return struct.pack('!I',epoch)+raw[4:]


def prepare(root,dsn,*,bad=True,local=True,empty_second=False,base_epoch=100,snapshot=False,unknown_peer=False,first_raw=None):
    root.mkdir()
    manifest=fixture_manifest(root)
    baseline=manifest['inputs'][0];bp=Path(baseline['path']);raw=gzip.decompress(bp.read_bytes());offset=0;rebuilt=b''
    while offset<len(raw):
        length=struct.unpack('!I',raw[offset+8:offset+12])[0]
        rebuilt+=struct.pack('!I',base_epoch)+raw[offset+4:offset+12+length];offset+=12+length
    bp.write_bytes(gzip.compress(rebuilt,mtime=0));baseline['sha256']=hashlib.sha256(bp.read_bytes()).hexdigest();baseline['size']=bp.stat().st_size
    baseline['source_id']=source_identity('rrc25',baseline['origin_uri'],baseline['sha256']);manifest['baseline_source']=baseline['source_id']
    ref=root/'reference.csv'
    with ref.open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=USECOLS);writer.writeheader()
        for asn,country,code in [('64496','伊朗','IR'),('64498','美国','US')]:
            writer.writerow(dict(asn=asn,as_country_cn=country,as_country=code))
    sha=hashlib.sha256(ref.read_bytes()).hexdigest()
    manifest['references']=[dict(path=str(ref),sha256=sha)]
    first=(raw_update(base_epoch+100,subtype=7 if local else 4,attrs=b'\xf0\x23\x04\x00\x04\x2f\x66') if bad else raw_update(base_epoch+100,subtype=7 if local else 4,withdrawn=b'\x18\xc0\x00\x02',ann=b'',attrs=b''))
    if first_raw is not None:first=first_raw
    if unknown_peer:first=mrt(b'\x00',4,16,epoch=base_epoch+100)
    second=b'' if empty_second else (raw_update(base_epoch+200,ann=b'\x18\xc6\x33\x64',attrs=attr(64496))+raw_update(base_epoch+200,ann=b'\x18\xcb\x00\x71',attrs=attr(64498))+raw_update(base_epoch+200,ann=b'',withdrawn=b'\x18\xc0\x00\x02',attrs=b''))
    for entry,raw in zip(manifest['inputs'][1:],[first,second]):
        p=Path(entry['path']);p.write_bytes(gzip.compress(raw,mtime=0))
        entry.update(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),size=p.stat().st_size)
        entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
    manifest['update_sources']=[e['source_id'] for e in manifest['inputs'][1:]]
    if snapshot:
        entry={**baseline,'role':'snapshot','origin_uri':f'fixture://rrc25/snapshot-{base_epoch}'}
        entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
        manifest['inputs'].insert(1,entry)
    start=time.monotonic()
    rss_before=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
    seal=produce_checkpointed(manifest,dsn,root/'m2',policy='isolate-payload/v1',min_free_bytes=0,batch_rows=2,catalog_data_path=root.parent/'m2-catalog')
    dt=lambda sec:datetime.fromtimestamp(sec,timezone.utc)
    views=[]
    selected=[e for e in manifest['inputs'] if e['role']!='baseline'] if snapshot else manifest['inputs']
    for rank,entry in enumerate(selected):
        cp=next(cp for cp in seal['checkpoints'] if cp['source_id']==entry['source_id'])
        views.append(SourceView(seal['run_id'],seal['snapshot'],entry['source_id'],entry['origin_uri'],entry['sha256'],entry['role'],
                     'initial_rib' if not rank else 'update',sha,cp['counts']['messages'],cp['counts']['elements'],
                     FileWindow(seal['run_id']+':'+str(seal['snapshot']),entry['source_id'],dt(base_epoch+100*rank),dt(base_epoch+100*(rank+1)),dt(base_epoch+100*rank),'complete'),
                     (seal['digest'],),'complete','observation'))
    cp=next(cp for cp in seal['checkpoints'] if cp['source_id']==sha)
    reference=ReferenceView(seal['run_id'],seal['snapshot'],sha,str(ref),cp['counts']['references'],'observation')
    inputs=FeatureInputs(dsn,'rrc25',views,reference,result_window=(dt(base_epoch+200),dt(base_epoch+300)),comparison_window=(dt(base_epoch+100),dt(base_epoch+200)))
    (root/'upstream-cost.json').write_text(json.dumps(dict(wall_seconds=time.monotonic()-start,rss_high_water_before=rss_before,rss_high_water_after=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),memory_scope='pytest进程累计高水位，M2及绑定共享进程，不是M2独占峰值',compressed_bytes=sum(e['size'] for e in manifest['inputs']),counts=[cp['counts'] for cp in seal['checkpoints']])))
    return inputs,seal


def formal(root,inputs,**options):
    root.mkdir()
    request=dict(dsn=inputs.dsn,collector=inputs.collector,source_views=[{k:v for k,v in alias.items() if k in {f.name for f in fields(SourceView)}} for source in inputs.source_specs for alias in source['aliases']],reference_view=asdict(inputs.reference_view),
                 result_window=inputs.result_window,comparison_window=inputs.comparison_window,output=str(root/'output'),**options)
    p=root/'request.json';p.write_text(json.dumps(request,default=lambda v:v.isoformat()));p.chmod(0o600)
    start=time.monotonic()
    run=subprocess.run([sys.executable,str(Path(__file__).resolve().parents[3]/'scripts/pipeline/feature-frozen-run.py'),str(p)],capture_output=True,text=True)
    (root/'stdout.txt').write_text(run.stdout);(root/'stderr.txt').write_text(run.stderr)
    (root/'wall.json').write_text(json.dumps(dict(seconds=time.monotonic()-start)))
    assert run.returncode==0,run.stderr
    return json.loads(run.stdout)


def rows(dsn,report,table):
    return [r for b in read_table(dsn,report['run_id'],report['snapshot'],table,profile=PROFILE,allow_fixture=True) for r in b.to_pylist()]


def test_formal_local_gap_later_window_and_public_reader(tmp_path,feature_dsn):
    inputs,seal=prepare(tmp_path/'input',feature_dsn)
    report=formal(tmp_path/'formal',inputs,batch_rows=2)
    assert report['state']=='complete'
    assert len(rows(feature_dsn,report,'input_gaps'))==2
    assert all(g['direction']=='local' and g['peer_asn']==64497 for g in rows(feature_dsn,report,'input_gaps'))
    binding=inspect_binding(feature_dsn,report['run_id'],report['snapshot'])
    output=list(read_windows(feature_dsn,binding))
    collect={(r['raw']['mode'],r['raw']['source_rank']):r for r in output if r['raw']['scope']=='collect'}
    for mode in ('ordinary','ir'):
        assert collect[mode,1]['raw_values']['withdraw_num']==0
        assert collect[mode,1]['raw_values']['v4IP_num']==256
        assert all(v is None for v in collect[mode,1]['values'].values())
        later=collect[mode,2]
        assert later['values']['announ_num']==(2 if mode=='ordinary' else 1)
        assert later['values']['withdraw_num']==1
        assert later['values']['v4IP_num'] is None
        assert later['qualifications']['withdrawal_attribution']['coverage']=='partial'
        assert later['qualifications']['comparison']['coverage']=='partial'
    with pytest.raises(ValueError,match='profile'):
        list(read_table(feature_dsn,report['run_id'],report['snapshot'],'windows'))
    assert len(list(read_coverage(feature_dsn,binding)))==2*3*len(DIMENSIONS)
    assert seal['business']=='not_run'


def test_empty_inherited_gap_has_coverage_without_asn_rows(tmp_path,feature_dsn):
    inputs,_=prepare(tmp_path/'input',feature_dsn,empty_second=True)
    report=produce_bound_features(inputs,tmp_path/'feature',fixture_only=True,batch_rows=1)
    assert not [r for r in rows(feature_dsn,report,'windows') if r['scope']=='asn']
    qualifications=rows(feature_dsn,report,'qualifications')
    last=[r for r in qualifications if r['source_rank']==2]
    assert len(last)==2*len(DIMENSIONS)
    assert all(q['coverage']=='complete' for q in last if q['dimension']=='window_counts')
    assert all(q['coverage']=='partial' and q['gap_refs'] for q in last if q['dimension'] in ('resources','sparse','comparison'))


def test_missing_gap_cannot_complete(tmp_path,feature_dsn,monkeypatch):
    from data_pipeline.analysis.features.qualification import Qualification
    inputs,_=prepare(tmp_path/'input',feature_dsn)
    monkeypatch.setattr(Qualification,'boundary',lambda *args:None)
    with pytest.raises(ValueError,match='漏Gap'):
        produce_bound_features(inputs,tmp_path/'feature',fixture_only=True)
    assert not (tmp_path/'feature'/'execution.json').exists()


def test_no_gap_matches_accepted_runner_all_typed_science(tmp_path,feature_dsn):
    from types import SimpleNamespace
    inputs,_=prepare(tmp_path/'input',feature_dsn,bad=False)
    # 同一已封存人工输入、同一plan/ref，执行固定接受基线的原runner作独立对照。
    source=subprocess.check_output(['git','show','52f0a1fe74e7cc1ffbea5a05a83751145d775056:backend/data_pipeline/analysis/features/run.py'],text=True)
    namespace={'__name__':'data_pipeline.feature_run_baseline','__package__':'data_pipeline'}
    exec(compile(source,'accepted-feature-run.py','exec'),namespace)
    proxy=SimpleNamespace(**inputs.__dict__,validate=inputs.validate,_bounds=inputs._bounds,
                          stream=lambda:(item for reader in inputs.blocks for item in reader.stream()))
    old=namespace['produce_bound_features'](proxy,tmp_path/'baseline',fixture_only=True,batch_rows=2)
    new=formal(tmp_path/'new',inputs,batch_rows=3)
    from data_pipeline.analysis.features.store import TABLES
    for table in TABLES:
        expected=[r for b in read_table(feature_dsn,old['run_id'],old['snapshot'],table,allow_fixture=True) for r in b.to_pylist()]
        actual=rows(feature_dsn,new,table)
        assert sorted(map(lambda r:json.dumps(r,sort_keys=True),actual))==sorted(map(lambda r:json.dumps(r,sort_keys=True),expected)),table
    assert all(q['coverage']=='complete' for q in rows(feature_dsn,new,'qualifications') if q['source_rank'])


def test_gap_batch_equivalence_and_late_binding_revoke(tmp_path,feature_dsn):
    inputs,seal=prepare(tmp_path/'input',feature_dsn)
    reports=[formal(tmp_path/f'batch-{size}',inputs,batch_rows=size) for size in (1,50)]
    from data_pipeline.analysis.features.store import TABLES as SCIENCE
    from data_pipeline.analysis.features.qualification import TABLES as QUALIFIED
    for table in {*SCIENCE,*QUALIFIED}:
        contents=[sorted(json.dumps(r,sort_keys=True) for r in rows(feature_dsn,report,table)) for report in reports]
        assert contents[0]==contents[1],table
    binding=inspect_binding(feature_dsn,reports[0]['run_id'],reports[0]['snapshot'])
    # 真新解释器通过固定binding读取（无业务运行状态继承）。
    request=tmp_path/'binding.json';request.write_text(json.dumps(binding))
    code='''import json,sys,time,resource;start=time.monotonic();sys.path.insert(0,sys.argv[1]);from data_pipeline.analysis.features.qualified_read import read_windows;count=len(list(read_windows(sys.argv[2],json.load(open(sys.argv[3])))));print(json.dumps(dict(rows=count,wall_seconds=time.monotonic()-start,peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),scope='fresh reader process including imports')))'''
    read=subprocess.run([sys.executable,'-I','-c',code,str(Path(__file__).resolve().parents[2]),feature_dsn,str(request)],capture_output=True,text=True)
    (tmp_path/'reader.stdout').write_text(read.stdout);(tmp_path/'reader.stderr').write_text(read.stderr)
    assert read.returncode==0,read.stderr
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE observation_m2.runs SET state='failed' WHERE run_id=%s",(seal['run_id'],))
    with pytest.raises(ValueError,match='封存'):
        list(read_windows(feature_dsn,binding))


def test_missing_coverage_and_final_upstream_revoke_block_finish(tmp_path,feature_dsn,monkeypatch):
    inputs,seal=prepare(tmp_path/'input',feature_dsn)
    append=FeatureStore.append
    def omit(self,table,row):
        if table!='qualifications':append(self,table,row)
    monkeypatch.setattr(FeatureStore,'append',omit)
    with pytest.raises(ValueError,match='资格枚举'):
        produce_bound_features(inputs,tmp_path/'missing',fixture_only=True)
    monkeypatch.setattr(FeatureStore,'append',append)
    complete=FeatureStore.source_complete
    def revoke(self,mode,rank,*args):
        complete(self,mode,rank,*args)
        if mode=='ir' and rank==2:
            with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
                c.execute("UPDATE observation_m2.runs SET state='failed' WHERE run_id=%s",(seal['run_id'],))
    monkeypatch.setattr(FeatureStore,'source_complete',revoke)
    with pytest.raises(ValueError,match='封存'):
        produce_bound_features(inputs,tmp_path/'revoked',fixture_only=True)
    assert not (tmp_path/'revoked'/'execution.json').exists()


def test_snapshot_alias_multiview_and_gap_survives_switch(tmp_path,feature_dsn):
    from dataclasses import replace
    first,_=prepare(tmp_path/'p',feature_dsn,snapshot=True)
    second,_=prepare(tmp_path/'d',feature_dsn,bad=False,base_epoch=300,empty_second=True)
    # 第一视图原snapshot作私有RIB，重复同一绑定只消费一次；第二视图baseline不进入链。
    views=[first.views[0],first.views[0],*first.views[1:],*second.views[1:]]
    # 同原参考bytes身份，不受独立载体run影响。
    dt=lambda t:datetime.fromtimestamp(t,timezone.utc)
    inputs=FeatureInputs(feature_dsn,'rrc25',views,first.reference_view,result_window=(dt(400),dt(600)),comparison_window=(dt(200),dt(400)))
    report=formal(tmp_path/'formal',inputs,batch_rows=10)
    assert len(report['specification']['source_bindings'])==5
    assert len(report['specification']['source_bindings'][0]['aliases'])==2
    assert report['specification']['source_bindings'][0]['upstream_source_rank']==1
    bindings=report['specification']['source_bindings']
    assert bindings[3]['ordered_binding_ref']!=bindings[1]['ordered_binding_ref']
    assert bindings[3]['sequence']==3 and bindings[3]['upstream_source_rank']==1
    last=[q for q in rows(feature_dsn,report,'qualifications') if q['source_rank']==4]
    assert all(q['gap_refs'] and q['coverage']=='partial' for q in last if q['dimension']=='resources')
    assert all(q['coverage']=='complete' for q in last if q['dimension']=='window_counts')


def test_unknown_peer_covers_future_asns(tmp_path,feature_dsn):
    inputs,_=prepare(tmp_path/'input',feature_dsn,unknown_peer=True)
    report=produce_bound_features(inputs,tmp_path/'feature',fixture_only=True)
    gaps=rows(feature_dsn,report,'input_gaps')
    assert len(gaps)==2 and all(g['peer_asn'] is None and g['unknown_peer_covers_unseen_asns'] and g['all_future_origin_asns'] for g in gaps)
    assert all(g['scope_kind']=='collector_chain' for g in gaps)
    assert all(q['unknown_peer'] and q['gap_refs'] for q in rows(feature_dsn,report,'qualifications') if q['source_rank']==2 and q['dimension']=='sparse')


def test_source_end_early_stop_and_wrong_binding_rejected(tmp_path,feature_dsn,monkeypatch):
    from data_pipeline.bgp.record_types import SourceEnd
    inputs,_=prepare(tmp_path/'input',feature_dsn)
    original=inputs.stream
    def short():
        for item in original():
            if isinstance(item,SourceEnd):return
            yield item
    monkeypatch.setattr(inputs,'stream',short)
    with pytest.raises(ValueError,match='来源未结束'):
        produce_bound_features(inputs,tmp_path/'early',fixture_only=True)
    monkeypatch.setattr(inputs,'stream',original)
    inputs.source_specs[0]['upstream_source_rank']=99
    with pytest.raises(ValueError,match='映射冲突'):
        produce_bound_features(inputs,tmp_path/'wrong',fixture_only=True)


def test_same_asn_addpath_local_a_and_et_gap_keep_science(tmp_path,feature_dsn):
    a=raw_update(200,subtype=11,ann=struct.pack('!I',0)+b'\x18\xc6\x33\x64',attrs=attr(64496))
    bad=update(subtype=7,attrs=b'\xf0\x23\x04\0\x04\x2f\x66')
    gap=mrt(struct.pack('!I',654321)+bad[12:],7,17,epoch=200)
    b=raw_update(200,subtype=11,ann=struct.pack('!I',1)+b'\x18\xc6\x33\x64',attrs=attr(64498),peer=b'\xc0\x00\x02\x03')
    inputs,_=prepare(tmp_path/'input',feature_dsn,first_raw=a+gap+b,empty_second=True)
    report=formal(tmp_path/'formal',inputs,batch_rows=20)
    gaps=rows(feature_dsn,report,'input_gaps')
    assert all(g['microsecond']==654321 and g['record']==1 and g['peer_asn']==64497 for g in gaps)
    audits=rows(feature_dsn,report,'projection_revisions')
    ordinary=[r for r in audits if r['mode']=='ordinary' and r['source_rank']==1]
    assert [r['sequence'] for r in ordinary]==[0,1]
    assert ordinary[1]['before_path']==ordinary[0]['after_path']  # 同ASN/不同端点与ADDPATH仍折叠。
    assert ordinary[1]['vp']==ordinary[0]['vp']=='64497'
    output=list(read_windows(feature_dsn,inspect_binding(feature_dsn,report['run_id'],report['snapshot'])))
    collect={r['raw']['mode']:r for r in output if r['raw']['source_rank']==1 and r['raw']['scope']=='collect'}
    assert collect['ordinary']['raw_values']['announ_num']==2 and collect['ir']['raw_values']['announ_num']==1
    assert all(r['values']['announ_num'] is None and r['values']['v4IP_num'] is None for r in collect.values())


def test_corrupt_qualification_identity_and_completion_anchor_reject(tmp_path,feature_dsn,monkeypatch):
    inputs,_=prepare(tmp_path/'input',feature_dsn)
    append=FeatureStore.append
    def corrupt(self,table,row):
        if table=='qualifications':row={**row,'coverage':'complete'}
        append(self,table,row)
    monkeypatch.setattr(FeatureStore,'append',corrupt)
    with pytest.raises(ValueError,match='资格枚举|资格摘要'):
        produce_bound_features(inputs,tmp_path/'corrupt',fixture_only=True)
    monkeypatch.setattr(FeatureStore,'append',append)
    report=produce_bound_features(inputs,tmp_path/'valid',fixture_only=True)
    binding=inspect_binding(feature_dsn,report['run_id'],report['snapshot'],allow_fixture=True)
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE feature.qualified_results SET receipt=jsonb_set(receipt,'{specification_digest}','\"bad\"') WHERE run_id=%s",(report['run_id'],))
    with pytest.raises(ValueError,match='完成锚'):
        list(read_windows(feature_dsn,binding,allow_fixture=True))
