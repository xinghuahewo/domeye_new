"""同catalog两个完成观察：原snapshot作私有基线，P→D连续且参考不重复。"""
from dataclasses import asdict,replace
from datetime import datetime,timezone
import csv
import gzip
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys
import psycopg2
import pytest

from data_pipeline.analysis.features.calculation import FileWindow
from data_pipeline.analysis.features.inputs import FeatureInputs, SourceView, ReferenceView
from data_pipeline.analysis.features.reference import USECOLS
from data_pipeline.analysis.features.run import produce_bound_features
from data_pipeline.analysis.features.store import read_table, FeatureStore, reconstruct_phases
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.bgp.replay.run_from_files import produce
from tests.observations.test_observation_consumer import feature_dsn
from tests.observations.test_observation_mrt import mrt, attr, update


def dt(t):return datetime.fromtimestamp(t,timezone.utc)


def rib(t):
    table=b'\0\0\0\x19\0\x05rrc25\0\x01\x02'+b'\xc0\0\x02\x01'*2+struct.pack('!I',64497)
    attrs=attr()
    body=struct.pack('!I',0)+b'\x18\xc0\0\x02'+struct.pack('!HHIH',1,0,t,len(attrs))+attrs
    return mrt(table,1,13,epoch=t)+mrt(body,2,13,epoch=t)


def event(t,*,withdraw=False,vp=64497,origin=64498):
    raw=update(ann=b'' if withdraw else b'\x18\xc0\x00\x02',withdrawn=b'\x18\xc0\x00\x02' if withdraw else b'',attrs=b'' if withdraw else attr(origin))
    return struct.pack('!I',t)+raw[4:12]+struct.pack('!I',vp)+raw[16:]


def fixture_views(root,dsn,gap=False,with_alias=False):
    ref=root/'reference.csv'
    with ref.open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=USECOLS);writer.writeheader()
        for asn,country,code in [('64496','伊朗','IR'),('64498','美国','US')]:writer.writerow(dict(asn=asn,as_country_cn=country,as_country=code))
    sha=hashlib.sha256(ref.read_bytes()).hexdigest()
    reports=[];manifests=[];readers=[]
    for index,records in enumerate([
        [('base','baseline',rib(100)),('initial','snapshot',rib(100)),
         ('p','update',event(200)+event(200,vp=64500,origin=64496)+event(200,withdraw=True,vp=64500)),('p-empty','update',b'')],
        [('base','baseline',rib(400)),('d','update',event(450 if gap else 400,withdraw=True)),('d-empty','update',b'')],
    ]):
        if with_alias and index==1:records.insert(1,('aliasinitial','snapshot',rib(100)))
        directory=root/str(index);directory.mkdir();entries=[]
        for name,role,raw in records:
            path=directory/(name+'.gz');path.write_bytes(gzip.compress(raw,mtime=0))
            content=hashlib.sha256(path.read_bytes()).hexdigest();uri='fixture://rrc25/'+str(index)+'/'+name
            if name=='aliasinitial':uri='fixture://rrc25/0/initial'
            entries.append(dict(path=str(path),sha256=content,size=path.stat().st_size,origin_uri=uri,source_id=source_identity('rrc25',uri,content),role=role))
        manifest=dict(schema_version='observation-run/v1',collector='rrc25',window_start=dt(100).isoformat(),window_end_exclusive=dt(600).isoformat(),inputs=entries,baseline_source=entries[0]['source_id'],update_sources=[e['source_id'] for e in entries if e['role']=='update'],references=[dict(path=str(ref),sha256=sha)] if index==0 else [])
        report=produce(manifest,dsn,directory/'observations',min_free_bytes=0,catalog_data_path=root/'catalog')
        reports.append(report);manifests.append(manifest)
        readers.append(ObservationReader(dsn,report['run_id'],report['snapshot'],[e['source_id'] for e in entries]))
    def view(index,name,start,end,role):
        reader=readers[index]
        entry=next(e for e in manifests[index]['inputs'] if Path(e['path']).stem==name)
        receipt=next(s for s in reader.starts if s.source_id==entry['source_id'])
        return SourceView(reader.run_id,reader.snapshot,entry['source_id'],entry['origin_uri'],entry['sha256'],entry['role'],role,sha,
                          receipt.expected_messages,receipt.expected_elements,
                          FileWindow(reader.run_id+':'+str(reader.snapshot),entry['source_id'],dt(start),dt(end),dt(start),'unknown'))
    views=[view(0,'initial',100,200,'initial_rib'),view(0,'p',200,300,'update'),view(0,'p-empty',300,400,'update'),
           view(1,'d',450 if gap else 400,500,'update'),view(1,'d-empty',500,600,'update')]
    reference=ReferenceView(readers[0].run_id,readers[0].snapshot,sha,str(ref),3)
    return views,reference,readers,view


def bind(dsn,views,reference):
    return FeatureInputs(dsn,'rrc25',views,reference,result_window=(dt(400),dt(600)),comparison_window=(dt(200),dt(400)))


def rows(dsn,report,table,role=None):
    return [r for b in read_table(dsn,report['run_id'],report['snapshot'],table,allow_fixture=True,window_role=role) for r in b.to_pylist()]


def test_two_runs_snapshot_initial_no_reset_reference_reuse_alias_and_sparse(tmp_path,feature_dsn,monkeypatch):
    views,reference,readers,view=fixture_views(tmp_path,feature_dsn)
    # 后一观察run没有参考行；只从明确的前一完成视图复用。
    assert not readers[1].manifest['references']
    import data_pipeline.bgp.input.mrt_reader as parser
    monkeypatch.setattr(parser,'read_source',lambda *a,**k:pytest.fail('Feature不得重解析MRT'))
    inputs=bind(feature_dsn,[views[0],views[0],*views[1:]],reference)
    assert len(inputs.views)==5 and len(inputs.source_specs[0]['aliases'])==2
    report=produce_bound_features(inputs,tmp_path/'feature',fixture_only=True,batch_rows=2)
    assert report['specification']['observation_run'] is None
    assert report['actual_rows']['source_receipts']==10
    receipts=rows(feature_dsn,report,'source_receipts')
    assert {r['source_role'] for r in receipts if r['calculation_role']=='initial_rib'}=={'snapshot'}
    assert {r['upstream_run_id'] for r in receipts}=={r.run_id for r in readers}
    result=rows(feature_dsn,report,'windows','result');comparison=rows(feature_dsn,report,'windows','comparison')
    assert result and comparison and not {r['source_id'] for r in result}&{r['source_id'] for r in comparison}
    # P将旧VP路径改成美国；D必须按这条路径归属withdraw，不能从D的IR RIB重新初始化。
    us=next(r for r in result if r['mode']=='ordinary' and r['scope']=='asn' and r['subject']=='64498')
    assert us['withdraw_num']==1
    ir=next(r for r in result if r['mode']=='ir' and r['scope']=='asn' and r['subject']=='64496')
    assert ir['withdraw_num']==1 and ir['row_presence']=='ir_asn_write_disabled'
    assert all(r['scope']!='asn' for r in result if r['source_rank']==4)
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT mode,vp FROM feature.seen_vps WHERE run_id=%s ORDER BY mode,vp',(report['run_id'],))
        assert c.fetchall()==[('ir','64497'),('ir','64500'),('ordinary','64497'),('ordinary','64500')]
    phases=reconstruct_phases(feature_dsn,report['run_id'],report['snapshot'],'ordinary',allow_fixture=True)
    assert phases[3,'window_end']['asn','美国','64498'][4]==1
    assert phases[3,'next_window']['asn','美国','64498'][4]==0
    # 相同字节不同URI的另一个RIB不能因SHA相等而合并成一个初始源。
    another=view(0,'base',100,200,'initial_rib')
    assert another.content_sha256==views[0].content_sha256 and another.source_id!=views[0].source_id
    with pytest.raises(ValueError,match='首位'):bind(feature_dsn,[views[0],another,*views[1:]],reference)


def test_gap_is_preserved_across_views(tmp_path,feature_dsn):
    views,reference,_,_=fixture_views(tmp_path,feature_dsn,gap=True)
    report=produce_bound_features(bind(feature_dsn,views,reference),tmp_path/'feature',fixture_only=True)
    receipts=rows(feature_dsn,report,'source_receipts','result')
    assert all(json.loads(r['input_gaps']) for r in receipts)
    assert all(r['quality']=='unknown' for r in receipts)


@pytest.mark.parametrize('change',['collector','role','calculation_role','snapshot','uri','reference'])
def test_wrong_binding_rejected(tmp_path,feature_dsn,change):
    views,reference,_,_=fixture_views(tmp_path,feature_dsn)
    if change=='collector':
        with pytest.raises(ValueError):FeatureInputs(feature_dsn,'rrc00',views,reference)
        return
    modifications={'role':dict(source_role='baseline'),'calculation_role':dict(calculation_role='update'),
                   'snapshot':dict(snapshot=views[0].snapshot+1),'uri':dict(origin_uri='fixture://wrong'),
                   'reference':dict(reference_sha256='0'*64)}
    views[0]=replace(views[0],**modifications[change])
    with pytest.raises(ValueError):bind(feature_dsn,views,reference)


@pytest.mark.parametrize('target',['upstream','reference','source'])
def test_final_all_view_qualification(tmp_path,feature_dsn,monkeypatch,target):
    views,reference,readers,_=fixture_views(tmp_path,feature_dsn)
    inputs=bind(feature_dsn,views,reference)
    original=FeatureStore.source_complete
    def revoke(self,mode,rank,*args):
        result=original(self,mode,rank,*args)
        if mode=='ir' and rank==4:
            with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
                if target=='upstream':c.execute("UPDATE domeye.runs SET state='failed' WHERE run_id=%s",(readers[0].run_id,))
                elif target=='reference':c.execute("UPDATE domeye.reference_inputs SET state='failed' WHERE run_id=%s",(reference.run_id,))
                else:c.execute('UPDATE domeye.source_receipts SET message_count=message_count+1 WHERE run_id=%s AND source_id=%s',(views[0].run_id,views[0].source_id))
        return result
    monkeypatch.setattr(FeatureStore,'source_complete',revoke)
    with pytest.raises(ValueError):produce_bound_features(inputs,tmp_path/'feature',fixture_only=True)
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT state FROM feature.runs');assert c.fetchone()[0]=='failed'
    assert not (tmp_path/'feature'/'execution.json').exists()


def test_formal_multiple_view_cli(tmp_path,feature_dsn):
    views,reference,_,_=fixture_views(tmp_path,feature_dsn)
    request=dict(dsn=feature_dsn,collector='rrc25',source_views=[asdict(v) for v in views],reference_view=asdict(reference),
                 comparison_window=[dt(200),dt(400)],result_window=[dt(400),dt(600)],output=str(tmp_path/'feature'))
    path=tmp_path/'request.json';path.write_text(json.dumps(request,default=lambda t:t.isoformat()))
    result=subprocess.run([sys.executable,str(Path(__file__).resolve().parents[3]/'scripts/pipeline/feature-frozen-run.py'),str(path)],capture_output=True,text=True)
    (tmp_path/'stdout.txt').write_text(result.stdout);(tmp_path/'stderr.txt').write_text(result.stderr)
    assert result.returncode==0,result.stderr
    report=json.loads(result.stdout)
    assert report['specification']['code_identity']['execution_mode']=='frozen-fresh-process'
    assert sum(b.num_rows for b in read_table(feature_dsn,report['run_id'],report['snapshot'],'windows',window_role='result'))>0


@pytest.mark.parametrize('carrier',['rrc25','rrc00'])
def test_cross_run_same_source_alias_once_and_reference_third_view(tmp_path,feature_dsn,monkeypatch,carrier):
    views,reference,readers,view=fixture_views(tmp_path,feature_dsn,with_alias=True)
    alias=view(1,'aliasinitial',100,200,'initial_rib')
    assert alias.source_id==views[0].source_id and alias.run_id!=views[0].run_id
    # 第三完成视图只提供参考；不用把参考复制到第二个消费run。
    path=tmp_path/'reference-rib.gz';path.write_bytes(gzip.compress(rib(50),mtime=0))
    sha=hashlib.sha256(path.read_bytes()).hexdigest();uri='fixture://rrc25/reference-only'
    source=source_identity(carrier,uri,sha)
    manifest=dict(schema_version='observation-run/v1',collector=carrier,window_start=dt(0).isoformat(),window_end_exclusive=dt(100).isoformat(),
                  inputs=[dict(path=str(path),sha256=sha,size=path.stat().st_size,origin_uri=uri,source_id=source,role='baseline')],
                  baseline_source=source,update_sources=[],references=[dict(path=reference.raw_path,sha256=reference.source_sha256)])
    # 只在此局部人工载体构造允许rrc00；正式Stage1 schema/producer不修改。
    import copy,importlib
    producer=importlib.import_module('data_pipeline.bgp.replay.run_from_files')
    validator=producer.Draft202012Validator
    def fixture_validator(schema,**kwargs):
        schema=copy.deepcopy(schema);schema['properties']['collector']={'const':carrier}
        return validator(schema,**kwargs)
    with monkeypatch.context() as patch:
        if carrier!='rrc25':patch.setattr(producer,'Draft202012Validator',fixture_validator)
        upstream=produce(manifest,feature_dsn,tmp_path/'reference-only',min_free_bytes=0,catalog_data_path=tmp_path/'catalog')
    separate=replace(reference,run_id=upstream['run_id'],snapshot=upstream['snapshot'])
    inputs=bind(feature_dsn,[views[0],alias,*views[1:]],separate)
    report=produce_bound_features(inputs,tmp_path/'feature',fixture_only=True)
    assert report['actual_rows']['source_receipts']==10
    assert report['actual_rows']['reference_rows']==3
    assert report['specification']['reference_binding']['run_id']==upstream['run_id']
    assert report['specification']['reference_binding']['carrier_collector']==carrier
    assert report['specification']['collector']=='rrc25'
    assert source not in report['specification']['source_ids']
    assert inputs.reference_reader.manifest['collector']==carrier
    assert len(report['specification']['source_bindings'][0]['aliases'])==2
    if carrier=='rrc00':
        original=FeatureStore.source_complete
        def revoke_reference(self,mode,rank,*args):
            result=original(self,mode,rank,*args)
            if mode=='ir' and rank==4:
                with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
                    c.execute("UPDATE domeye.reference_inputs SET state='failed' WHERE run_id=%s",(separate.run_id,))
            return result
        with monkeypatch.context() as patch:
            patch.setattr(FeatureStore,'source_complete',revoke_reference)
            with pytest.raises(ValueError,match='参考资格'):
                produce_bound_features(inputs,tmp_path/'reference-revoked',fixture_only=True)
        assert not (tmp_path/'reference-revoked'/'execution.json').exists()
        with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
            c.execute('SELECT state FROM feature.runs ORDER BY state')
            assert c.fetchall()==[('complete',),('failed',)]
            c.execute("UPDATE domeye.reference_inputs SET state='validated' WHERE run_id=%s",(separate.run_id,))
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE domeye.inputs SET state='failed' WHERE run_id=%s AND source_id=%s",(alias.run_id,alias.source_id))
    with pytest.raises(ValueError):inputs.validate()
