"""人工MRT→保存观察→独立双Feature→私有PG/Parquet历史的真实链路。"""
import csv
import gzip
import hashlib
from datetime import datetime, timezone
import json
import struct
import pandas as pd
import psycopg2
import pytest
from data_pipeline.analysis.features.calculation import FileWindow
from data_pipeline.analysis.features.projection import FeaturePlan, SourceBinding
from data_pipeline.analysis.features.reference import USECOLS
from data_pipeline.analysis.features.run import run_fixture
from data_pipeline.analysis.features.store import read_table, reconstruct_phases, FeatureStore
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.bgp.input.mrt_reader import source_identity
from tests.observations.test_observation_consumer import feature_dsn
from tests.observations.test_observation_two_phase import fixture_manifest


def prepared(tmp_path,dsn,decode_edges=False,a_bytes=None,empty_last=False):
    manifest=fixture_manifest(tmp_path)
    for i,e in enumerate(manifest['inputs'][1:],2):
        p=tmp_path/(('a' if i==2 else 'b')+'.gz')
        raw=gzip.decompress(p.read_bytes()); raw=struct.pack('!I',i*100)+raw[4:]
        if decode_edges and i==2:
            from tests.observations.test_observation_mrt import update, attr
            local=update(ann=struct.pack('!I',42)+b'\x18\xc0\x00\x02',attrs=attr(64498),subtype=11)
            local=struct.pack('!I',200)+local[4:]
            et=update(ann=b'\x18\xc6\x33\x64',attrs=attr(64498))
            et=struct.pack('!IHHI',200,17,4,len(et)-8)+struct.pack('!I',123456)+et[12:]
            raw=local+et
        if a_bytes is not None and i==2:raw=a_bytes
        if empty_last and i==3:raw=b''
        p.write_bytes(gzip.compress(raw,mtime=0)); e['sha256']=hashlib.sha256(p.read_bytes()).hexdigest()
        e['size']=p.stat().st_size; e['source_id']=source_identity('rrc25',e['origin_uri'],e['sha256'])
    manifest['update_sources']=[e['source_id'] for e in manifest['inputs'][1:]]
    ref=tmp_path/'reference.csv'
    with ref.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=USECOLS);w.writeheader()
        for asn,name,code in [('064496','伊朗','IR'),('64498','美国','US'),('64498','伊朗','IR')]:
            w.writerow({'asn':asn,'as_country_cn':name,'as_country':code})
        f.write('\n')
    sha=hashlib.sha256(ref.read_bytes()).hexdigest()
    manifest['references']=[{'path':str(ref),'sha256':sha}]
    report=produce(manifest,dsn,tmp_path/'observations',min_free_bytes=0)
    reader=ObservationReader(dsn,report['run_id'],report['snapshot'],[e['source_id'] for e in manifest['inputs']],batch_rows=2)
    version=report['run_id']+':'+str(report['snapshot'])
    dt=lambda sec:datetime.fromtimestamp(sec,timezone.utc)
    sources=tuple(SourceBinding(s.source_id,s.content_sha256,s.role,FileWindow(version,s.source_id,dt((i+1)*100),dt((i+2)*100),dt((i+1)*100),'unknown'),s.expected_elements) for i,s in enumerate(reader.starts))
    return reader,FeaturePlan(version,'rrc25',sources),sha,ref


def table(dsn,report,name):
    return [r for b in read_table(dsn,report['run_id'],report['snapshot'],name,allow_fixture=True) for r in b.to_pylist()]


def test_private_two_modes_typed_phases_and_pandas_reference(tmp_path,feature_dsn):
    reader,plan,sha,ref=prepared(tmp_path,feature_dsn)
    report=run_fixture(reader,plan,sha,ref,tmp_path/'feature',batch_rows=8,batch_bytes=4096)
    assert report['state']=='complete'
    assert json.loads((tmp_path/'feature'/'execution.json').read_text())['state']=='ready'
    rows=table(feature_dsn,report,'reference_rows')
    selected=[r for r in rows if r['selected']]
    old=pd.read_csv(ref,keep_default_na=False,usecols=USECOLS).drop_duplicates(subset=['asn'],keep='first').set_index('asn')
    assert [r['legacy_asn'] for r in selected]==list(old.index.astype(str))==['64496','64498']
    assert any(r['csv_record_kind']=='blank_line' for r in rows)
    windows=table(feature_dsn,report,'windows')
    ordinary=[r for r in windows if r['mode']=='ordinary' and r['scope']=='collect']
    assert [r['announ_num'] for r in ordinary]==[1,1]
    assert all(r['resource_status']=='unknown' for r in windows)
    phases=reconstruct_phases(feature_dsn,report['run_id'],report['snapshot'],'ordinary',allow_fixture=True)
    assert phases[1,'window_end']['collect','collect',''][3]==1
    assert phases[1,'next_window']['collect','collect',''][3]==0
    assert phases[1,'window_end']['asn','美国','64498'][5] is True
    assert phases[1,'next_window']['asn','美国','64498'][5] is False
    assert report['actual_rows']['source_receipts']==6
    assert all(m['full_route_snapshot_deepcopies']==0 for m in report['source_metrics'])
    with pytest.raises(ValueError): list(read_table(feature_dsn,report['run_id'],report['snapshot']+1,'windows'))


def test_ready_failure_is_not_consumable(tmp_path,feature_dsn,monkeypatch):
    reader,plan,sha,ref=prepared(tmp_path,feature_dsn)
    def fail(*args): raise OSError('fixture ready failed')
    monkeypatch.setattr(FeatureStore,'write_ready',fail)
    with pytest.raises(OSError,match='fixture ready'):run_fixture(reader,plan,sha,ref,tmp_path/'feature')
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT run_id,state FROM feature.runs'); run,state=c.fetchone()
    assert state=='failed'
    with pytest.raises(ValueError):list(read_table(feature_dsn,run,0,'windows'))

@pytest.mark.parametrize('failure',['projection_revisions','state_deltas'])
def test_required_sink_failure_never_completes(tmp_path,feature_dsn,monkeypatch,failure):
    reader,plan,sha,ref=prepared(tmp_path,feature_dsn)
    original=FeatureStore.append
    def append(self,table,row):
        if table==failure and (table!='state_deltas' or row['phase']=='next_window'):
            raise OSError('fixture required output failed')
        return original(self,table,row)
    monkeypatch.setattr(FeatureStore,'append',append)
    with pytest.raises(OSError,match='required output'):run_fixture(reader,plan,sha,ref,tmp_path/'feature',batch_rows=2)
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT state FROM feature.runs');assert c.fetchone()[0]=='failed'
    assert not (tmp_path/'feature'/'execution.json').exists()


def test_same_database_new_private_catalog_and_large_rows(tmp_path,feature_dsn):
    reader,plan,sha,ref=prepared(tmp_path,feature_dsn)
    first=run_fixture(reader,plan,sha,ref,tmp_path/'first',batch_rows=2,batch_bytes=32)
    second=run_fixture(reader,plan,sha,ref,tmp_path/'second',batch_rows=4)
    assert first['run_id']!=second['run_id']
    assert all(m['rows']==1 for m in first['flush_metrics'] if m['bytes']>32)
    assert first['actual_rows']==second['actual_rows']
    assert len(table(feature_dsn,first,'windows'))==len(table(feature_dsn,second,'windows'))


def test_corrected_local_addpath_et_default_and_difference_history(tmp_path,feature_dsn):
    reader,plan,sha,ref=prepared(tmp_path,feature_dsn,decode_edges=True)
    report=run_fixture(reader,plan,sha,ref,tmp_path/'feature')
    differences=table(feature_dsn,report,'decoding_differences')
    assert len(differences)==2
    evidence=[json.loads(r['evidence']) for r in differences]
    assert any(r['old_fixed_vp']=='12654' and r['new_vp']=='64497' and r['old_fixed_path']=='42' for r in evidence)
    assert any(r['microsecond']==123456 and r['old_time_error'] for r in evidence)
    window=next(r for r in table(feature_dsn,report,'windows') if r['mode']=='ordinary' and r['scope']=='collect' and r['source_rank']==1)
    assert window['announ_num']==2
    revisions=[r for r in table(feature_dsn,report,'projection_revisions') if r['mode']=='ordinary' and r['source_rank']==1]
    assert {r['vp'] for r in revisions}=={'64497'}
    assert all(r['after_path']=='64497 64498' for r in revisions)


def test_frozen_fresh_process_formal_entry_and_default_fixture_rejection(tmp_path,feature_dsn):
    from dataclasses import asdict
    from pathlib import Path
    import subprocess
    import sys
    from data_pipeline.analysis.features.run import produce_features
    reader,plan,sha,ref=prepared(tmp_path,feature_dsn)
    with pytest.raises(ValueError,match='冻结新进程'):
        produce_features(reader,plan,sha,ref,tmp_path/'direct')
    assert not (tmp_path/'direct').exists()
    synthetic=run_fixture(reader,plan,sha,ref,tmp_path/'synthetic')
    with pytest.raises(ValueError,match='synthetic'):
        list(read_table(feature_dsn,synthetic['run_id'],synthetic['snapshot'],'windows'))
    request={'dsn':feature_dsn,'observation_run':reader.run_id,'observation_snapshot':reader.snapshot,
             'collector':plan.collector,'sources':[asdict(s) for s in plan.sources],
             'reference_sha':sha,'reference_path':str(ref),'output':str(tmp_path/'formal')}
    request_path=tmp_path/'request.json'
    request_path.write_text(json.dumps(request,default=lambda v:v.isoformat()))
    root=Path(__file__).resolve().parents[3]
    result=subprocess.run([sys.executable,str(root/'scripts/pipeline/feature-frozen-run.py'),str(request_path)],capture_output=True,text=True,check=True)
    report=json.loads(result.stdout)
    assert report['state']=='complete'
    identity=report['specification']['code_identity']
    assert identity['execution_mode']=='frozen-fresh-process'
    assert identity['execution_binding']['snapshot_root']!=str(root)
    assert 'data_pipeline.analysis.features.reference' in identity['execution_binding']['module_sources']
    assert 'backend/data_pipeline/bgp/input/path_decoding.py' in identity['files']
    assert sum(b.num_rows for b in read_table(feature_dsn,report['run_id'],report['snapshot'],'windows'))>0


def formal_call(directory,reader,plan,sha,ref,**options):
    from dataclasses import asdict
    from pathlib import Path
    import subprocess,sys
    directory.mkdir()
    request={'dsn':reader.dsn,'observation_run':reader.run_id,'observation_snapshot':reader.snapshot,
             'collector':plan.collector,'sources':[asdict(s) for s in plan.sources],
             'reference_sha':sha,'reference_path':str(ref),'output':str(directory/'output'),**options}
    path=directory/'request.json';path.write_text(json.dumps(request,default=lambda v:v.isoformat()));path.chmod(0o600)
    result=subprocess.run([sys.executable,str(Path(__file__).resolve().parents[3]/'scripts/pipeline/feature-frozen-run.py'),str(path)],capture_output=True,text=True)
    (directory/'stdout.txt').write_text(result.stdout);(directory/'stderr.txt').write_text(result.stderr)
    return result


def test_formal_new_vp_aw_batch_equivalence_and_empty_source(tmp_path,feature_dsn):
    from tests.observations.test_observation_mrt import update, attr
    def raw(withdraw):
        m=update(ann=b'' if withdraw else b'\x18\xc0\x00\x02',withdrawn=b'\x18\xc0\x00\x02' if withdraw else b'',attrs=b'' if withdraw else attr())
        return struct.pack('!I',200)+m[4:12]+struct.pack('!I',64500)+m[16:]
    reader,plan,sha,ref=prepared(tmp_path,feature_dsn,a_bytes=raw(False)+raw(True),empty_last=True)
    reports=[]
    for batch in (1,2,100):
        result=formal_call(tmp_path/('batch'+str(batch)),reader,plan,sha,ref,batch_rows=batch)
        assert result.returncode==0,result.stderr
        reports.append(json.loads(result.stdout))
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        for report in reports:
            c.execute('SELECT mode,vp FROM feature.seen_vps WHERE run_id=%s ORDER BY mode,vp',(report['run_id'],))
            assert c.fetchall()==[('ir','64497'),('ir','64500'),('ordinary','64497'),('ordinary','64500')]
    assert all(report['actual_rows']==reports[0]['actual_rows'] for report in reports)
    def values(report):
        return sorted((r['mode'],r['source_rank'],r['scope'],r['subject'],r['announ_num'],r['withdraw_num']) for r in table(feature_dsn,report,'windows'))
    assert all(values(report)==values(reports[0]) for report in reports)
    assert all(r['elements']==0 for r in table(feature_dsn,reports[0],'source_receipts') if r['source_rank']==2)


@pytest.mark.parametrize('case',['wrong_collector','upstream_revoked','reference_revoked','reference_count','source_revoked','source_count'])
def test_formal_input_identity_and_final_qualification(tmp_path,feature_dsn,case):
    from psycopg2 import sql
    reader,plan,sha,ref=prepared(tmp_path,feature_dsn)
    if case!='wrong_collector':
        with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
            c.execute('CREATE SCHEMA feature')
            c.execute('CREATE TABLE feature.source_commits(run_id TEXT,mode TEXT,source_id TEXT,previous_projection TEXT,projection_version TEXT,messages BIGINT,elements BIGINT,PRIMARY KEY(run_id,mode,source_id))')
            targets={
                'upstream_revoked':"UPDATE domeye.runs SET state='failed' WHERE run_id={run}",
                'reference_revoked':"UPDATE domeye.reference_inputs SET state='failed' WHERE run_id={run}",
                'reference_count':"UPDATE domeye.reference_inputs SET row_count=row_count+1 WHERE run_id={run}",
                'source_revoked':"UPDATE domeye.inputs SET state='failed' WHERE run_id={run} AND source_id={source}",
                'source_count':"UPDATE domeye.source_receipts SET element_count=element_count+1 WHERE run_id={run} AND source_id={source}",
            }
            target=sql.SQL(targets[case]).format(run=sql.Literal(reader.run_id),source=sql.Literal(reader.sources[0]))
            c.execute(sql.SQL("CREATE FUNCTION feature.revoke_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.mode='ir' AND NEW.source_id={last} THEN {target}; END IF; RETURN NEW; END $$").format(last=sql.Literal(reader.sources[-1]),target=target))
            c.execute('CREATE TRIGGER revoke_fixture AFTER INSERT ON feature.source_commits FOR EACH ROW EXECUTE FUNCTION feature.revoke_fixture()')
    result=formal_call(tmp_path/'formal',reader,plan,sha,ref,**({'collector':'rrc00'} if case=='wrong_collector' else {}))
    assert result.returncode!=0,'invalid qualification unexpectedly completed: '+result.stdout
    assert not (tmp_path/'formal'/'output'/'execution.json').exists()
    if case=='wrong_collector':assert not (tmp_path/'formal'/'output').exists()
    else:
        with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
            c.execute('SELECT run_id,state FROM feature.runs');run,state=c.fetchone()
        assert state=='failed'
        with pytest.raises(ValueError):list(read_table(feature_dsn,run,0,'windows'))
