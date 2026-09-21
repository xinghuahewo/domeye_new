"""人工固定M2 → 两种正式存储策略；完整值和有限失败路径。"""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
import time
import resource
from dataclasses import asdict
import psycopg2
import pytest
from data_pipeline.analysis.detection.qualification_contract import LAKE_PROFILE, entry_id
from data_pipeline.analysis.detection.store import read_stored_rows, reconstruct_state


def database(base):
    pg=psycopg2.connect(base);pg.autocommit=True
    name='det_lake_'+uuid.uuid4().hex
    try:
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name+" ENCODING 'UTF8' TEMPLATE template0")
    finally:pg.close()
    return base+' dbname='+name


@pytest.fixture(params=['gap','empty'])
def request_data(request,tmp_path):
    from tests.detection.test_detection_pipeline import six_class_observation_fixture, saved_reference_files
    from tests.detection.test_detection_computation import KINDS
    from data_pipeline.analysis.detection.models import DetectionScope, FileBoundary
    from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
    base=os.environ.get('DOMEYE_DETECTION_TEST_DSN')
    if not base:pytest.skip('未绑定本任务人工PG')
    manifest=six_class_observation_fixture(tmp_path)
    if request.param=='empty':
        manifest['inputs']=manifest['inputs'][:1];manifest['update_sources']=[]
    else:
        import gzip
        from tests.observations.test_observation_mrt import update
        from data_pipeline.bgp.input.mrt_reader import source_identity
        source=manifest['inputs'][-1];path=Path(source['path'])
        path.write_bytes(gzip.compress(gzip.decompress(path.read_bytes())+update(attrs=b'\xf0\x23\x04\x00\x04\x2f\x66',subtype=7),mtime=0))
        source['sha256']=hashlib.sha256(path.read_bytes()).hexdigest();source['size']=path.stat().st_size
        source['source_id']=source_identity('rrc25',source['origin_uri'],source['sha256'])
        manifest['update_sources']=[e['source_id'] for e in manifest['inputs'][1:]]
    refs=saved_reference_files(tmp_path,rich=True)
    manifest['references']=[{k:r[k] for k in ('path','sha256')} for r in refs]
    obs=database(base)
    result=produce_checkpointed(manifest,obs,tmp_path/'input',min_free_bytes=0,policy='isolate-payload/v1')
    version=f"{result['run_id']}:{result['snapshot']}"
    scope=DetectionScope('lake-fixture','r','rrc25',version,'1970-01-01T00:00:00Z','1970-01-02T00:00:00Z')
    return dict(observation_dsn=obs,input_run=result['run_id'],input_snapshot=result['snapshot'],
                ordered_sources=[r['source_id'] for r in manifest['inputs']],input_profile='observation',
                scope=asdict(scope),references=refs,reference_version='artificial-v1',
                boundaries={s:asdict(FileBoundary(s,version,scope.window_start,{k:k+'_197001' for k in KINDS})) for s in manifest['update_sources']},
                min_free_bytes=0),base,request.param


def pg_metrics(dsn):
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'0/0')::bigint")
        lsn=c.fetchone()[0]
        c.execute("SELECT n.nspname,sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('detection','public') AND c.relkind='r' GROUP BY n.nspname")
        return dict(wal_lsn=lsn,relation_bytes={k:int(v) for k,v in c.fetchall()})


def formal(request,base,path,profile):
    path.mkdir()
    q=dict(request,detection_dsn=database(base),output=str(path/'output'),output_profile=profile)
    f=path/'request.json';f.write_text(json.dumps(q))
    root=Path(__file__).resolve().parents[3]
    before=pg_metrics(q['detection_dsn'])
    started=time.monotonic()
    peak_files=0;peak_temp=0;samples=0
    with (path/'process.log').open('w') as log:
        process=subprocess.Popen([sys.executable,str(root/'scripts/pipeline/detection-frozen-run.py'),str(f)],stdout=log,stderr=subprocess.STDOUT)
        while process.poll() is None:
            sizes=[];temporary=[]
            for file in (path/'output').rglob('*'):
                try:
                    if file.is_file():
                        size=file.stat().st_size;sizes.append(size)
                        if file.suffix in ('.duckdb','.wal') or 'temp' in file.relative_to(path/'output').parts:temporary.append(size)
                except FileNotFoundError:pass
            peak_files=max(peak_files,sum(sizes));peak_temp=max(peak_temp,sum(temporary));samples+=1
            time.sleep(.05)
    producer_seconds=time.monotonic()-started
    after=pg_metrics(q['detection_dsn'])
    assert process.returncode==0,(path/'process.log').read_text()
    result=json.loads((path/'output/result.json').read_text())
    ready=json.loads((path/'output/business/ready.json').read_text())
    read_started=time.monotonic()
    rows={t:list(read_stored_rows(q['detection_dsn'],result['run_id'],result['snapshot'],t)) for t in ('records','state_entries','m3_entries')}
    read_seconds=time.monotonic()-read_started
    from data_pipeline.analysis.detection.lake_integrity import Inventory
    inv=Inventory()
    for table,values in rows.items():
        for row in values:inv.add(table,row)
    script=path/'fresh-reader.py'
    script.write_text('''import sys,json
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from data_pipeline.analysis.detection.store import read_stored_rows
from data_pipeline.analysis.detection.lake_integrity import Inventory
q=json.loads(Path(sys.argv[2]).read_text());r=json.loads(Path(sys.argv[3]).read_text());v=Inventory()
for t in ('records','state_entries','m3_entries'):
 for row in read_stored_rows(q['detection_dsn'],r['run_id'],r['snapshot'],t,batch_rows=3):v.add(t,row)
print(json.dumps(v.result()))
''')
    fresh=subprocess.run([sys.executable,'-I',str(script),str(root/'backend'),str(f),str(path/'output/result.json')],capture_output=True,text=True)
    assert fresh.returncode==0,fresh.stderr
    assert json.loads(fresh.stdout)==inv.result()
    (path/'fresh-reader.json').write_text(fresh.stdout)
    files=[f for f in (path/'output').rglob('*') if f.is_file()]
    metrics=dict(sampled_output_peak_bytes=peak_files,sampled_staging_temp_peak_bytes=peak_temp,samples=samples,
                 sample_scope='父pytest每0.05秒采样本输出目录；可能漏过短峰；不是累计写入量',producer_seconds=producer_seconds,read_seconds=read_seconds,pg_before=before,pg_after=after,
                 pg_wal_bytes=after['wal_lsn']-before['wal_lsn'],wal_scope='本任务专有PG集群，含目录与后台写入；非正文专属',
                 output_file_bytes=sum(f.stat().st_size for f in files),parquet_bytes=sum(f.stat().st_size for f in files if f.suffix=='.parquet'),
                 parquet_files=sum(f.suffix=='.parquet' for f in files),typed=inv.result(),
                 pg_body_rows=0 if profile==LAKE_PROFILE else sum(len(v) for v in rows.values()),
                 pg_body_typed_bytes=0 if profile==LAKE_PROFILE else sum(v['typed_bytes'] for v in inv.result().values()),
                 bytes_scope='完整typed行规范编码UTF8长度；非SQL协议或物理磁盘写入字节',
                 read_process_pid=os.getpid(),read_process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),
                 read_memory_scope='pytest进程启动至本次读取结束累计峰值；不含生产子进程/PG')
    (path/'metrics.json').write_text(json.dumps(metrics,ensure_ascii=False,indent=2))
    return q,result,ready,rows


def comparable_json(rows):
    from tests.detection.test_detection_pipeline import normalize_old_moas_pair
    result=copy.deepcopy(rows)
    for row in result:
        for key,value in row.items():
            if key.endswith('_json'):
                row[key]=json.dumps(normalize_old_moas_pair(json.loads(value)),ensure_ascii=False,separators=(',',':'))
    return result


def comparable_state(rows,records):
    from data_pipeline.analysis.detection._results import stable
    from tests.detection.test_detection_pipeline import normalize_old_moas_pair
    latest={r['incident_id']:json.loads(r['legacy_json']) for r in records if r['record_kind']=='business_revision'}
    rows=comparable_json(rows)
    for row in rows:
        if (row['family'],row['attribute'],row['container'])==('output','last_records','entry'):
            raw=latest[json.loads(row['key_json'])]
            assert json.loads(row['value_json'])==stable(raw)
            row['value_json']=json.dumps(stable(normalize_old_moas_pair(raw)))
    groups={}
    for row in rows:groups.setdefault((row['family'],row['attribute']),[]).append(row)
    for group in groups.values():
        if group[0]['container']=='set':
            values=sorted(r['value_json'] for r in group[1:])
            for row,value in zip(group[1:],values):row['value_json']=value
    return rows


def test_lake_matches_mirror_and_no_pg_body(request_data,tmp_path,monkeypatch):
    from data_pipeline.analysis.detection.qualified_reader import read_qualified_revisions, read_qualified_state_entries
    request,base,kind=request_data
    old=formal(request,base,tmp_path/'mirror','detection-m3/v2')
    new=formal(request,base,tmp_path/'lake',LAKE_PROFILE)
    a,b=old[3],new[3]
    assert comparable_json(a['records'])==comparable_json(b['records'])
    assert comparable_state(a['state_entries'],a['records'])==comparable_state(b['state_entries'],b['records'])
    # 唯一跨执行资格差异：component_run（正文/target_ref）与由此确定的entry_id。
    normalized=copy.deepcopy(b['m3_entries'])
    for row in normalized:
        p=json.loads(row['payload_json'])
        assert p['component_run']==p['target_ref']['component_run']==new[1]['run_id']
        p['component_run']=p['target_ref']['component_run']=old[1]['run_id']
        row['payload_json']=json.dumps(p,ensure_ascii=False,separators=(',',':'))
        row['entry_id']=entry_id(old[1]['run_id'],row)
    assert normalized==a['m3_entries']
    for item in (old,new):
        q,r,ready,rows=item
        with psycopg2.connect(q['detection_dsn']) as pg,pg.cursor() as c:
            for table in rows:
                c.execute('SELECT to_regclass(%s)',('detection.'+table,))
                relation=c.fetchone()[0]
                if item is new:assert relation is None
                else:
                    assert relation is not None
                    c.execute('SELECT count(*) FROM detection.'+table+' WHERE run_id=%s',(r['run_id'],))
                    assert c.fetchone()[0]==len(rows[table])
        identity=ready['identity'];bid=identity['input_binding_id'];pos=identity['qualification_as_of_position']
        revisions=list(read_qualified_revisions(q['detection_dsn'],r['run_id'],r['snapshot'],expected_binding_id=bid,at_position=pos))
        states=list(read_qualified_state_entries(q['detection_dsn'],r['run_id'],r['snapshot'],expected_binding_id=bid))
        assert bool(revisions)==(kind=='gap')
        if kind=='gap':assert any(x['main'] is None for x in revisions) and all(x['main'] is None for x in states)
        assert sum(x['kind']=='source_coverage' for x in rows['m3_entries'])==len(request['ordered_sources'])
    report=new[2]['identity']['storage_integrity']
    assert report['full_table_scans']==3 and report['public_admission']=='not_performed'
    from tests.detection.test_detection_pipeline import normalize_old_moas_pair
    restored=[]
    for item in (old,new):
        state=normalize_old_moas_pair(reconstruct_state(item[0]['detection_dsn'],item[1]['run_id'],item[1]['snapshot']))
        normalized_state=comparable_state(item[3]['state_entries'],item[3]['records'])
        state['output']['last_records']={json.loads(r['key_json']):json.loads(r['value_json']) for r in normalized_state if (r['family'],r['attribute'],r['container'])==('output','last_records','entry')}
        restored.append(state)
    assert restored[0]==restored[1]
    if kind=='gap':
        check_finish_faults(request,base,tmp_path,monkeypatch)
        wrong=dict(new[0],input_profile='complete',output=str(tmp_path/'wrong-input-profile'))
        path=tmp_path/'wrong-request.json';path.write_text(json.dumps(wrong))
        process=subprocess.run([sys.executable,str(Path(__file__).resolve().parents[3]/'scripts/pipeline/detection-frozen-run.py'),str(path)],capture_output=True,text=True)
        assert process.returncode!=0 and '要求observation输入' in process.stderr
        assert not (Path(wrong['output'])/'result.json').exists()
    (tmp_path/'comparison.json').write_text(json.dumps(dict(kind=kind,old=old[1],new=new[1],integrity=report),ensure_ascii=False,indent=2))


def check_finish_faults(request,base,tmp_path,monkeypatch):
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.analysis.detection.reference_view import ReferenceView, ReferenceSource
    from data_pipeline.analysis.detection.models import DetectionScope, FileBoundary
    from data_pipeline.analysis.detection.ordered_runner import run
    from data_pipeline.analysis.detection.qualification_store import M3Store
    from data_pipeline.analysis.detection.identity import identity_builder
    original=M3Store.finish
    results=[]
    for fault in ('body','fk','state','missing_state_family','wrong_role_index','missing_last_record_key'):
        target=tmp_path/fault;target.mkdir();dsn=database(base)
        reader=ObservationReader(request['observation_dsn'],request['input_run'],request['input_snapshot'],request['ordered_sources'],profile='observation')
        view=ReferenceView(request['reference_version'],f"{reader.run_id}:{reader.snapshot}",target/'refs')
        for source in request['references']:
            rows=(r for b in reader.reference_batches(source['source_id']) for r in b.to_pylist())
            view.add(ReferenceSource(source['role'],source['source_id'],source['expected_rows'],rows))
        captured={}
        changed=[0]
        def broken(store,**kwargs):
            store.flush();store.flush_qualification()
            table=f'lake.{store.schema}.'
            captured['run']=store.run_id
            if fault in ('missing_state_family','wrong_role_index','missing_last_record_key'):
                assert changed[0]>0
            elif fault=='body':
                store.db.execute(f"UPDATE {table}records SET legacy_json='{{}}' WHERE sequence=(SELECT min(sequence) FROM {table}records WHERE record_kind='business_revision')")
            elif fault=='state':
                store.db.execute(f"UPDATE {table}state_entries SET container='entry' WHERE ordinal=0")
            else:
                batch=store.db.execute(f"SELECT * FROM {table}m3_entries WHERE kind='event_qualification' ORDER BY ordinal LIMIT 1").fetchone()
                from data_pipeline.analysis.detection.qualification_contract import FIELDS
                row=dict(zip(FIELDS,batch));row['incident_id']='missing-scientific-target'
                p=json.loads(row['payload_json']);p['target_ref']['incident_id']=row['incident_id']
                row['payload_json']=json.dumps(p,ensure_ascii=False,separators=(',',':'));row['entry_id']=entry_id(store.run_id,row)
                store.db.execute(f'UPDATE {table}m3_entries SET incident_id=?,payload_json=?,entry_id=? WHERE ordinal=?',[row[k] for k in ('incident_id','payload_json','entry_id','ordinal')])
            captured['snapshot']=store.db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
            return original(store,**kwargs)
        with monkeypatch.context() as patch:
            from data_pipeline.analysis.detection import store
            if fault=='missing_state_family':
                write_state=store.DetectionStore._write_state
                def bad_state(self,rows):
                    changed[0]+=sum(r[1]=='output' for r in rows)
                    return write_state(self,[(r[0],'wrong_output',*r[2:]) if r[1]=='output' else r for r in rows])
                patch.setattr(store.DetectionStore,'_write_state',bad_state)
            if fault=='missing_last_record_key':
                write_state=store.DetectionStore._write_state
                def missing_key(self,rows):
                    modified=[]
                    for r in rows:
                        if changed[0]==0 and (r[1],r[2],r[4])==('output','last_records','entry'):
                            r=(r[0],r[1],'identities',*r[3:]);changed[0]+=1
                        modified.append(r)
                    return write_state(self,modified)
                patch.setattr(store.DetectionStore,'_write_state',missing_key)
            if fault=='wrong_role_index':
                roles=store.interpret_roles
                def bad_roles(*args):
                    value=roles(*args)
                    if value['attacker_asn'] is not None:
                        value['attacker_asn']=999999;changed[0]+=1
                    return value
                patch.setattr(store,'interpret_roles',bad_roles)
            patch.setattr(M3Store,'finish',broken)
            expected_error={'missing_state_family':'终态族/属性/容器','wrong_role_index':'科学typed索引','missing_last_record_key':'终态科学键集合'}.get(fault)
            with pytest.raises(ValueError,match=expected_error):
                run(reader,view,DetectionScope(**request['scope']),{k:FileBoundary(**v) for k,v in request['boundaries'].items()},dsn,target/'output',
                    root=Path(__file__).resolve().parents[3],identity_builder=identity_builder,fixture_only=True,min_free_bytes=0,output_profile=LAKE_PROFILE)
        assert not (target/'output/ready.json').exists()
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:
            c.execute('SELECT state FROM detection.runs WHERE run_id=%s',(captured['run'],));assert c.fetchone()[0]=='failed'
        with pytest.raises(ValueError):list(read_stored_rows(dsn,captured['run'],captured['snapshot'],'records',allow_synthetic=True))
        results.append(dict(fault=fault,**captured,state='failed',ready=False,writer_changed_rows=changed[0]))
    (tmp_path/'failures.json').write_text(json.dumps(results,indent=2))
