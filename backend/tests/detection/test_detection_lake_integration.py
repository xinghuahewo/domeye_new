"""8233 原健康 M2/ref → 两次正式 lake/v2；原镜像只读对照。"""
import copy
import hashlib
import json
import os
from pathlib import Path
from collections import Counter
import time
import pytest
import psycopg2
if os.environ.get('DOMEYE_DETECTION_LAKE_INTEGRATION')!='8233':pytest.skip('仅显式本任务人工湖单写集成',allow_module_level=True)
from tests.detection.test_detection_lake import formal, database, comparable_json, comparable_state
from data_pipeline.analysis.detection.store import read_stored_rows
from data_pipeline.analysis.detection.qualification_contract import LAKE_PROFILE, LAKE_SCHEMA, entry_id
from data_pipeline.analysis.detection.lake_integrity import Inventory, row_bytes
from tests.country.country_query_test_support import PgLogProbe

OUT=Path('/tmp/domeye-detection-lake-integration-8233').resolve()
OLD=Path('/tmp/domeye-detection-m3-integration-8233/detection-m30').resolve()
BASE='host=/tmp/domeye-integration-detection-8233/socket port=55483 dbname=postgres'
TABLES=('records','state_entries','m3_entries')

def save(name,v):(OUT/name).write_text(json.dumps(v,ensure_ascii=False,indent=2,default=str))
def hashes(root):return {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
def read_all(q,r):return {t:list(read_stored_rows(q['detection_dsn'],r['run_id'],r['snapshot'],t)) for t in TABLES}
def load_old(kind):
    q=json.loads((OLD/('request.json' if kind=='gap' else 'baseline-only-request.json')).read_text())
    result=json.loads((OLD/('formal' if kind=='gap' else 'baseline-only-recheck')/'result.json').read_text())
    return q,result,read_all(q,result)

def measured(label,dsn,action):
    probe=PgLogProbe(dsn,OUT/'pg.log');offset=probe.start();start=time.monotonic()
    result=action();elapsed=time.monotonic()-start;log,count=probe.finish(offset)
    (OUT/(label+'.pg.log')).write_text(log);save(label+'.cost.json',dict(wall_seconds=elapsed,pg_statements=count,scope='自身独占PG实日志；含子进程及隐式DuckLake，非单条SQL/物理IO'))
    return result

@pytest.mark.parametrize('kind',['gap','empty'])
def test_formal_lake_v2_vs_original_mirror(kind):
    if not (OUT/'原完整目录SHA.json').exists():save('原完整目录SHA.json',hashes(OLD))
    oldq,oldr,a=load_old(kind);q=copy.deepcopy(oldq)
    q['min_free_bytes']=128*1024**2
    path=OUT/kind
    if (path/'output/result.json').exists():
        nq=json.loads((path/'request.json').read_text());nr=json.loads((path/'output/result.json').read_text());ready=json.loads((path/'output/business/ready.json').read_text());rows=read_all(nq,nr)
    else:nq,nr,ready,rows=measured(kind+'正式生产及全读',q['observation_dsn'],lambda:formal(q,BASE,path,LAKE_PROFILE))
    save(kind+'原镜像实际全值.json',a);save(kind+'新湖实际全值.json',rows)
    if kind=='gap':assert json.loads(json.dumps(a,default=str))==json.loads((OLD/'完整正文与资格.json').read_text())['raw']
    else:assert json.loads(json.dumps(a['records'],default=str))==json.loads((OLD/'无事件完整coverage.json').read_text())['raw']
    differences={t:[dict(ordinal=i,fields=[k for k in x if x[k]!=y[k]]) for i,(x,y) in enumerate(zip(a[t],rows[t])) if x!=y] for t in TABLES}
    save(kind+'原始差异.json',differences)
    inventory=Inventory()
    for table in TABLES:
        for row in rows[table]:inventory.add(table,row)
    identity=ready['identity'];integrity=identity['storage_integrity']
    assert identity['output_profile']==LAKE_PROFILE and identity['store_schema_version']==LAKE_SCHEMA
    assert integrity['version']=='detection-lake-integrity/v2' and integrity['inventory']==inventory.result() and integrity['public_admission']=='not_performed'
    assert identity['qualification_rule']=='detection-gap-qualification/v2'
    repo=Path(__file__).resolve().parents[3]
    for f,info in identity['files'].items():assert hashlib.sha256((repo/f).read_bytes()).hexdigest()==info['sha256']
    assert identity['execution_mode']=='frozen-fresh-process'
    pg_checks={}
    for label,request,result,values in [('old',oldq,oldr,a),('new',nq,nr,rows)]:
        pg=psycopg2.connect(request['detection_dsn'])
        try:
            with pg.cursor() as c:
                pg_checks[label]={}
                for table in TABLES:
                    c.execute('SELECT to_regclass(%s)',('detection.'+table,));relation=c.fetchone()[0]
                    if label=='new':assert relation is None;pg_checks[label][table]=None
                    else:
                        c.execute('SELECT count(*) FROM detection.'+table+' WHERE run_id=%s',(result['run_id'],));count=c.fetchone()[0];assert count==len(values[table]);pg_checks[label][table]=count
        finally:pg.close()
    from data_pipeline.analysis.detection.qualified_reader import read_qualified_revisions, read_qualified_state_entries
    bid=identity['input_binding_id'];at=identity['qualification_as_of_position']
    revisions=list(read_qualified_revisions(nq['detection_dsn'],nr['run_id'],nr['snapshot'],expected_binding_id=bid,at_position=at));states=list(read_qualified_state_entries(nq['detection_dsn'],nr['run_id'],nr['snapshot'],expected_binding_id=bid))
    assert len(states)==len(rows['state_entries'])
    assert sum(r['kind']=='source_coverage' for r in rows['m3_entries'])==len(q['ordered_sources'])
    if kind=='gap':assert any(r['main'] is None for r in revisions) and all(r['main'] is None for r in states)
    else:assert not revisions and len(rows['m3_entries'])==1
    # 原镜像早于已接受的 consumer 排序修复；仅这两类元数据允许重排。
    old_records=copy.deepcopy(a['records']);new_records=copy.deepcopy(rows['records'])
    assert len(old_records)==len(new_records)
    metadata_checks=[]
    for old_row,new_row in zip(old_records,new_records):
        if old_row['record_kind']!='source_message':continue
        assert new_row['record_kind']=='source_message'
        old_payload=json.loads(old_row['legacy_json']);new_payload=json.loads(new_row['legacy_json'])
        for field,keys in [('peers',('table_record','peer_index','bgp_id','ip','asn','bgp_id_present')),('quality',('code','detail'))]:
            left=old_payload[field];right=new_payload[field]
            # JSON 类型及整条记录关联均保留，Counter 同时保留重复成员。
            encoded=lambda x:json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':'))
            assert Counter(map(encoded,left))==Counter(map(encoded,right))
            sort_key=lambda x:tuple((x[k] is None,x[k]) for k in keys)
            assert right==sorted(right,key=sort_key)
            metadata_checks.append(dict(sequence=old_row.get('sequence'),field=field,count=len(right),order_changed=left!=right))
            old_payload[field]=right
        old_row['legacy_json']=json.dumps(old_payload,ensure_ascii=False,separators=(',',':'))
    assert comparable_json(old_records)==comparable_json(new_records)
    save(kind+'元数据有限排序验收.json',metadata_checks)
    assert comparable_state(a['state_entries'],a['records'])==comparable_state(rows['state_entries'],rows['records'])
    normalized=copy.deepcopy(rows['m3_entries'])
    for row in normalized:
        payload=json.loads(row['payload_json']);assert payload['component_run']==payload['target_ref']['component_run']==nr['run_id']
        payload['component_run']=payload['target_ref']['component_run']=oldr['run_id']
        row['payload_json']=json.dumps(payload,ensure_ascii=False,separators=(',',':'));row['entry_id']=entry_id(oldr['run_id'],row)
    assert normalized==a['m3_entries']
    save(kind+'对照完成.json',dict(old=oldr,new=nr,rows={k:len(v) for k,v in rows.items()},pg_body=pg_checks,frozen_files=len(identity['files']),narrow_set_moas_equal=True,qualified_revisions=len(revisions),integrity=integrity))
    assert hashes(OLD)==json.loads((OUT/'原完整目录SHA.json').read_text())


@pytest.mark.parametrize('fault',['missing_last_record_key','wrong_role_index'])
def test_minimal_actual_writer_fault(fault,monkeypatch):
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.analysis.detection.reference_view import ReferenceView, ReferenceSource
    from data_pipeline.analysis.detection.models import DetectionScope, FileBoundary
    from data_pipeline.analysis.detection.ordered_runner import run
    from data_pipeline.analysis.detection.store import DetectionStore, connect_duckdb, literal
    from data_pipeline.analysis.detection.identity import identity_builder
    from data_pipeline.analysis.detection import store as module
    q=json.loads((OLD/'request.json').read_text());path=OUT/fault;path.mkdir();dsn=database(BASE)
    reader=ObservationReader(q['observation_dsn'],q['input_run'],q['input_snapshot'],q['ordered_sources'],profile='observation')
    refs=ReferenceView(q['reference_version'],f'{reader.run_id}:{reader.snapshot}',path/'refs')
    for source in q['references']:
        rows=(row for batch in reader.reference_batches(source['source_id']) for row in batch.to_pylist());refs.add(ReferenceSource(source['role'],source['source_id'],source['expected_rows'],rows))
    changed=[0]
    if fault=='missing_last_record_key':
        original=DetectionStore._write_state
        def broken(self,rows):
            values=[]
            for row in rows:
                if changed[0]==0 and (row[1],row[2],row[4])==('output','last_records','entry'):row=(row[0],row[1],'identities',*row[3:]);changed[0]+=1
                values.append(row)
            return original(self,values)
        monkeypatch.setattr(DetectionStore,'_write_state',broken);message='终态科学键集合'
    else:
        original=module.interpret_roles
        def broken(*args):
            value=original(*args)
            if value['attacker_asn'] is not None:value['attacker_asn']=999999;changed[0]+=1
            return value
        monkeypatch.setattr(module,'interpret_roles',broken);message='科学typed索引'
    with pytest.raises(ValueError,match=message) as caught:
        run(reader,refs,DetectionScope(**q['scope']),{k:FileBoundary(**v) for k,v in q['boundaries'].items()},dsn,path/'output',root=Path(__file__).resolve().parents[3],identity_builder=identity_builder,fixture_only=True,min_free_bytes=128*1024**2,output_profile=LAKE_PROFILE)
    assert changed[0]>0 and not (path/'output/ready.json').exists()
    pg=psycopg2.connect(dsn)
    try:
        with pg.cursor() as c:c.execute('SELECT run_id,state FROM detection.runs');runid,state=c.fetchone();assert state=='failed'
    finally:pg.close()
    db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres');db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
    try:
        snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        if fault=='wrong_role_index':assert db.execute(f'SELECT count(*) FROM lake.det_{runid}.records WHERE attacker_asn=999999').fetchone()[0]==changed[0]
        else:
            heads=db.execute(f'SELECT family,attribute,container FROM lake.det_{runid}.state_entries WHERE container<>\'entry\' ORDER BY ordinal').fetchall()
            from data_pipeline.analysis.detection.lake_contract import STATE_HEADERS
            assert heads==[(f,a,c) for (f,a),c in STATE_HEADERS.items()]
            counts=dict(db.execute(f'SELECT attribute,count(*) FROM lake.det_{runid}.state_entries WHERE family=\'output\' AND container=\'entry\' GROUP BY attribute').fetchall());assert counts['last_records']==counts['revisions']-1
    finally:db.close()
    with pytest.raises(ValueError):list(read_stored_rows(dsn,runid,snapshot,'records',allow_synthetic=True))
    save(fault+'.json',dict(dsn=dsn,run_id=runid,snapshot=snapshot,state=state,error=str(caught.value),changed_rows=changed[0],ready=False,kind='实际Writer synthetic故障，不是正式冻结科学运行'))


def test_original_typed_and_q2():
    from contextlib import closing
    from data_pipeline.results import Publication, Token
    from data_pipeline.results.manifest_io import encode
    from tests.publication.test_publication_q2 import all_pages
    from data_pipeline.analysis.detection.qualified_reader import read_binding
    from tests.detection.test_detection_m3_integration import digest
    root=Path('/tmp/domeye-m2-repaired-integration-8233/test_resource_feature_share_ob0')
    q=json.loads((root/'detection-request.json').read_text());saved=json.loads((root/'q2-joint-evidence.json').read_text());e=json.loads((root/'three-module-evidence.json').read_text())['detection'];before=hashes(root)
    raw=list(read_stored_rows(q['detection_dsn'],e['run_id'],e['snapshot'],'records'));states=list(read_stored_rows(q['detection_dsn'],e['run_id'],e['snapshot'],'state_entries'))
    original=json.loads(Path('/tmp/domeye-detection-m3-recheck-8233/test_old_typed_v1_q2_original_0/原typed-v1与Q2.json').read_text())
    assert (e['run_id'],e['snapshot'],digest(raw),digest(states))==(original['run'],original['snapshot'],original['raw_digest'],original['state_digest'])
    pub=Publication(q['observation_dsn'],root,detection={'observation_dsn':q['observation_dsn'],'output_dsn':q['detection_dsn']});token=Token(**saved['token'])
    def heads():
        with closing(psycopg2.connect(q['observation_dsn'])) as pg,pg.cursor() as c:c.execute('SELECT * FROM publication_q1.head ORDER BY selector');return c.fetchall()
    initial=heads()
    assert Counter(encode(r) for r in all_pages(pub,token,'records'))==Counter(encode(r) for r in raw)
    assert pub.query_detection(token,'events',page_size=100)==saved['events'] and heads()==initial
    with pytest.raises(ValueError,match='M3 profile不受支持'):read_binding(q['detection_dsn'],e['run_id'],e['snapshot'])
    assert hashes(root)==before
    save('原typed-v1与Q2.json',dict(run=e['run_id'],snapshot=e['snapshot'],token=saved['token'],records=len(raw),state_entries=len(states),raw_digest=digest(raw),state_digest=digest(states),full_q2_reply_equal=True,heads_unchanged=initial,unchanged_files=len(before)))
