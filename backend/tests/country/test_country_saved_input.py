"""C1仅私有socket PG：正式人工生产及冻结Detection后的固定保存态输入。"""
import json
import hashlib
from contextlib import contextmanager
import os
from dataclasses import asdict, replace
from pathlib import Path
import subprocess
import sys
import uuid

import psycopg2
import pytest

from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch
from data_pipeline.analysis.country_events.saved_input import CountrySavedInput, DetectionBinding, SavedBatch, CountryRevision, InputCompletion, OrderQuality, legacy_time
from tests.detection.test_detection_pipeline import saved_reference_files, six_class_observation_fixture
from tests.detection.test_detection_computation import KINDS
from data_pipeline.analysis.detection.models import DetectionScope, FileBoundary


@pytest.fixture(scope='module')
def prepared(tmp_path_factory):
    base = os.environ.get('DOMEYE_COUNTRY_C1_TEST_DSN')
    if not base:
        pytest.skip('必须显式绑定本任务私有socket PG')
    require_socket = psycopg2.extensions.parse_dsn(base)
    assert require_socket['host'].startswith('/')
    root = tmp_path_factory.mktemp('country-c1')
    return build_pipeline(root,base,six_class_observation_fixture(root),rich=True)


def build_pipeline(root,base,manifest,rich,multicountry=False,large_reference_bytes=0):
    dsns = []
    pg = psycopg2.connect(base)
    pg.autocommit = True
    with pg.cursor() as c:
        for _ in range(2):
            name = 'country_c1_'+uuid.uuid4().hex
            c.execute('CREATE DATABASE '+name)
            dsns.append(base+' dbname='+name)
    pg.close()
    references = saved_reference_files(root, rich=rich)
    if multicountry:
        import csv
        from openpyxl import load_workbook
        for r in references:
            path=Path(r['path'])
            if r['role']=='as_info':
                with path.open(newline='') as f: rows=list(csv.DictReader(f))
                for row in rows:
                    if row['asn'] in ('3','4'): row['as_country']='YY'
                with path.open('w',newline='') as f:
                    writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
            elif r['role']=='country':
                book=load_workbook(path);book.active.append(['YY','测试国乙']);book.save(path);r['expected_rows']+=1
            r['sha256']=r['source_id']=hashlib.sha256(path.read_bytes()).hexdigest()
    if large_reference_bytes:
        import csv
        r=next(r for r in references if r['role']=='important_as_dict')
        with Path(r['path']).open('a',newline='') as f:csv.writer(f).writerow(['9999999','X'*large_reference_bytes])
        r['expected_rows']+=1
        r['sha256']=r['source_id']=hashlib.sha256(Path(r['path']).read_bytes()).hexdigest()
    manifest['references'] = [{'path':r['path'],'sha256':r['sha256']} for r in references]
    observed = produce(manifest,dsns[0],root/'observation',min_free_bytes=0,batch_rows=4)
    sources = [manifest['baseline_source'],*manifest['update_sources']]
    version = f"{observed['run_id']}:{observed['snapshot']}"
    scope = DetectionScope('country-c1-fixture','r','rrc25',version,
                           manifest['window_start'],manifest['window_end_exclusive'])
    request = dict(observation_dsn=dsns[0], detection_dsn=dsns[1],input_run=observed['run_id'],
                   input_snapshot=observed['snapshot'],ordered_sources=sources,scope=asdict(scope),
                   references=references,reference_version='country-artificial-v1',
                   boundaries={s:asdict(FileBoundary(s,version,scope.window_start,{k:k+'_197001' for k in KINDS}))
                               for s in sources[1:]},output=str(root/'detection'),min_free_bytes=0)
    (root/'request.json').write_text(json.dumps(request))
    repo = Path(__file__).resolve().parents[3]
    child = subprocess.run([sys.executable,str(repo/'scripts/pipeline/detection-frozen-run.py'),str(root/'request.json')],
                           capture_output=True,text=True)
    (root/'child.stdout').write_text(child.stdout)
    (root/'child.stderr').write_text(child.stderr)
    assert child.returncode == 0, child.stderr
    detection = json.loads((root/'detection/result.json').read_text())
    receipt_sha=hashlib.sha256((root/'observation/execution.json').read_bytes()).hexdigest()
    (root/'binding.json').write_text(json.dumps(dict(observation=observed,detection=detection,receipt_sha256=receipt_sha)))
    return root, dsns, sources, observed, detection


def input_bindings(prepared):
    from data_pipeline.analysis.country_events.saved_contract import ProductionReceiptBinding
    root=prepared[0]
    return dict(production_receipt=ProductionReceiptBinding(str(root/'observation/execution.json'),json.loads((root/'binding.json').read_text())['receipt_sha256']),scratch_root=str(root))


def adapter(prepared, **kwargs):
    _,dsns,sources,o,d = prepared
    reader = ObservationReader(dsns[0],o['run_id'],o['snapshot'],sources,batch_rows=4)
    return CountrySavedInput(reader,DetectionBinding(dsns[1],d['run_id'],d['snapshot']),
                             legacy_timezone='Asia/Shanghai',**input_bindings(prepared),**kwargs)


def test_formal_saved_chain_preserves_fields_and_all_country_revisions(prepared,monkeypatch):
    from data_pipeline.bgp.replay.route_replay import Replay
    monkeypatch.setattr(Replay,'consume',lambda *a,**k:pytest.fail('adapter不得Replay'))
    rows = list(adapter(prepared).stream())
    completion = rows[-1]
    assert isinstance(completion,InputCompletion)
    assert completion.enumeration=='complete' and completion.baseline_initialized_elements==9
    events = [r for r in rows if isinstance(r,CountryRevision)]
    assert events and all(r.country=='ZZ' and r.onset_cursor is None for r in events)
    assert all(r.onset.microsecond is None and r.onset_link_state=='unknown' for r in events)
    assert len({r.trigger_cursor for r in events})>1
    changes = [r for b in rows if isinstance(b,SavedBatch) and b.table=='changes' for r in b.rows]
    assert len(changes)==22
    assert all(r.after_presence==r.original['after_presence'] and r.after_path==r.original['after_path'] for r in changes)
    assert any(r.after_presence=='absent' for r in changes)
    assert all(r.at.microsecond is None for r in changes)
    assert all(r.trigger_time.lower_us>=r.onset.lower_us for r in events)
    assert len([r for b in rows if isinstance(b,SavedBatch) and b.table=='references' for r in b.rows])>11
    for rowsize in (1,7):
        other=list(adapter(prepared,batch_rows=rowsize).stream())
        assert replace(other[-1],resource_limits=completion.resource_limits)==completion
    (prepared[0]/'adapter-evidence.json').write_text(json.dumps(asdict(completion),default=str))


def test_fixed_snapshot_and_source_order_rejected(prepared):
    _,dsns,sources,o,d=prepared
    with pytest.raises(ValueError):
        ObservationReader(dsns[0],o['run_id'],o['snapshot']+1,sources)
    reader=ObservationReader(dsns[0],o['run_id'],o['snapshot'],list(reversed(sources)))
    with pytest.raises(ValueError,match='source_order'):
        CountrySavedInput(reader,DetectionBinding(dsns[1],d['run_id'],d['snapshot']),legacy_timezone='Asia/Shanghai',**input_bindings(prepared))


@pytest.mark.parametrize('target',['observation','detection','reference'])
def test_qualification_drift_has_no_completion(prepared,target):
    a=adapter(prepared)
    _,dsns,_,o,d=prepared
    dsn=dsns[1] if target=='detection' else dsns[0]
    sql,params = {
        'observation':("UPDATE domeye.runs SET state=%s WHERE run_id=%s",(o['run_id'],)),
        'detection':("UPDATE detection.runs SET state=%s WHERE run_id=%s",(d['run_id'],)),
        'reference':("UPDATE domeye.reference_inputs SET state=%s WHERE run_id=%s",(o['run_id'],)),
    }[target]
    old='validated' if target=='reference' else 'complete'
    try:
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute(sql,('failed',*params))
        with pytest.raises(ValueError):list(a.stream())
    finally:
        with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute(sql,(old,*params))


def test_legacy_time_preserves_second_precision():
    at=legacy_time({'$datetime':'1970-01-01T08:01:41'},'Asia/Shanghai')
    assert at.epoch==101 and at.microsecond is None
    assert legacy_time(None,'Asia/Shanghai') is None
    fractional=legacy_time('1970-01-01 08:01:41.000001','Asia/Shanghai')
    assert fractional.epoch==101 and fractional.microsecond==1


def test_formal_empty_enumeration_local_state_et_and_rollback(tmp_path):
    base=os.environ.get('DOMEYE_COUNTRY_C1_TEST_DSN')
    if not base: pytest.skip('必须绑定私有PG')
    import gzip
    import struct
    import hashlib
    from tests.observations.test_observation_consumer import observation_fixture
    from tests.observations.test_observation_mrt import update, mrt
    from data_pipeline.bgp.input.mrt_reader import source_identity
    manifest=observation_fixture(tmp_path)
    entry=manifest['inputs'][1]
    raw=gzip.decompress(Path(entry['path']).read_bytes())
    # ET同一原秒内123456微秒。尾部更早但合法的普通消息保留源序。
    ordinary=update()
    header=struct.pack('!IHHI',101,17,4,len(ordinary)-12+4)
    et=header+struct.pack('!I',123456)+ordinary[12:]
    raw+=et+update(ann=b'',attrs=b'')
    Path(entry['path']).write_bytes(gzip.compress(raw,mtime=0))
    entry['sha256']=hashlib.sha256(Path(entry['path']).read_bytes()).hexdigest()
    entry['size']=Path(entry['path']).stat().st_size
    entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
    manifest['update_sources']=[e['source_id'] for e in manifest['inputs'][1:]]
    prepared=build_pipeline(tmp_path,base,manifest,rich=False)
    rows=list(adapter(prepared).stream())
    assert rows[-1].enumeration=='complete_empty'
    assert rows[-1].selected_revisions==()
    assert rows[-1].boundary_capability=='unsupported_order'
    assert any(isinstance(r,OrderQuality) for r in rows)
    messages=[m for b in rows if isinstance(b,MessageBatch) for m in b.messages]
    assert any(m['local_message'] for m in messages)
    assert any(m['microsecond']==123456 for m in messages)
    invalid=[r for b in rows if isinstance(b,SavedBatch) and b.table=='invalidations' for r in b.rows]
    assert invalid and all(r.phase=='message_before_elements' for r in invalid)
    assert all(r.at.microsecond is None for r in invalid)
    assert rows[-1].counts['source_messages']==8


@contextmanager
def mutate_saved(prepared, table, transform):
    """只改自有人工Parquet及容器元数据，固定回执、run、snapshot不改。"""
    import pyarrow.parquet as pq
    root,dsns,_,o,_=prepared
    backup=[]
    try:
        for path in (root/'observation/parquet'/('r_'+o['run_id'])/table).glob('*.parquet'):
            raw=path.read_bytes()
            with psycopg2.connect(dsns[0]) as pg,pg.cursor() as c:
                c.execute('SELECT data_file_id,footer_size,file_size_bytes,record_count FROM public.ducklake_data_file WHERE path=%s',(path.name,))
                fid,footer,size,count=c.fetchone()
            backup.append((path,raw,fid,footer,size,count))
            changed=transform(pq.read_table(path));pq.write_table(changed,path)
            updated=path.read_bytes()
            delta=int.from_bytes(updated[-8:-4],'little')-int.from_bytes(raw[-8:-4],'little')
            with psycopg2.connect(dsns[0]) as pg,pg.cursor() as c:
                c.execute('UPDATE public.ducklake_data_file SET footer_size=%s,file_size_bytes=%s,record_count=%s WHERE data_file_id=%s',(footer+delta,len(updated),changed.num_rows,fid))
        yield
    finally:
        for path,raw,fid,footer,size,count in backup:
            path.write_bytes(raw)
            with psycopg2.connect(dsns[0]) as pg,pg.cursor() as c:
                c.execute('UPDATE public.ducklake_data_file SET footer_size=%s,file_size_bytes=%s,record_count=%s WHERE data_file_id=%s',(footer,size,count,fid))


@pytest.mark.parametrize('table,field,value,error',[
    ('paths',None,None,'production_table_count_mismatch:paths'),
    ('changes','object_key','wrong','change_cross_object_reference:before_event'),
    ('changes','ordinal',99,'change_cursor_identity_mismatch'),
    ('changes','after_event','absent-reference','missing_reference:changes.after_event'),
    ('projection_metadata',None,None,'production_table_count_mismatch:projection_metadata'),
    ('baseline_mappings','peer_refs','[[0,999,999]]','mapping_peer_identity_mismatch'),
    ('references',None,None,'production_table_count_mismatch:references'),
])
def test_damaged_saved_tables_never_complete(prepared,table,field,value,error):
    import pyarrow as pa
    def damage(rows):
        if field is None:return rows.slice(0,0)
        values=rows[field].to_pylist()
        for i in range(len(values)):
            if table!='changes' or rows['source_rank'][i].as_py()>0:values[i]=value
        index=rows.schema.get_field_index(field)
        return rows.set_column(index,rows.schema.field(index),pa.array(values,type=rows.schema.field(index).type))
    with mutate_saved(prepared,table,damage):
        seen=[]
        with pytest.raises(ValueError,match=error):
            for row in adapter(prepared).stream():seen.append(row)
        assert not any(isinstance(r,InputCompletion) for r in seen)


def test_midstream_drift_rejected_before_completion(prepared):
    a=adapter(prepared); stream=a.stream(); seen=[next(stream)]
    _,dsns,_,_,d=prepared
    try:
        with psycopg2.connect(dsns[1]) as pg,pg.cursor() as c:
            c.execute("UPDATE detection.runs SET state='failed' WHERE run_id=%s",(d['run_id'],))
        with pytest.raises(ValueError):
            seen.extend(stream)
        assert not any(isinstance(r,InputCompletion) for r in seen)
    finally:
        stream.close()
        with psycopg2.connect(dsns[1]) as pg,pg.cursor() as c:
            c.execute("UPDATE detection.runs SET state='complete' WHERE run_id=%s",(d['run_id'],))


def test_multiple_country_events_keep_distinct_onsets_without_guessing_cursor(tmp_path):
    base=os.environ.get('DOMEYE_COUNTRY_C1_TEST_DSN')
    if not base: pytest.skip('必须绑定私有PG')
    prepared=build_pipeline(tmp_path,base,multiple_country_mrt(tmp_path),rich=True,multicountry=True)
    rows=list(adapter(prepared).stream())
    events=[r for r in rows if isinstance(r,CountryRevision)]
    assert {r.country for r in events}=={'ZZ','YY'}
    assert len({r.onset.epoch for r in events})>=2
    assert len(rows[-1].selected_revisions)>=2
    assert all(r.onset_cursor is None and r.onset_link_state=='unknown' for r in events)


@pytest.mark.parametrize('field,value,error',[
    ('epoch',999,'incident_observation_field_mismatch'),
    ('snapshot_ref','other:1','incident_observation_identity_mismatch'),
])
def test_incident_wrong_observation_fields_rejected(prepared,monkeypatch,field,value,error):
    from data_pipeline.analysis.country_events import saved_input
    original=saved_input.read_revisions
    def broken(*args,**kwargs):
        for row in original(*args,**kwargs):
            if row['event_kind']=='country_outage':
                row['evidence']['observation'][field]=value
            yield row
    monkeypatch.setattr(saved_input,'read_revisions',broken)
    with pytest.raises(ValueError,match=error):list(adapter(prepared).stream())


def multiple_country_mrt(root):
    import struct,gzip,hashlib
    from tests.observations.test_observation_mrt import mrt
    from data_pipeline.bgp.input.mrt_reader import source_identity
    table=b'\0\0\0\x19\0\x05rrc25'+struct.pack('!H',3)
    for i in range(3):
        ip=bytes([192,0,2,i+1]);table+=b'\x02'+ip+ip+struct.pack('!I',100+i)
    baseline=mrt(table,1,13);updates=b''
    for n,origin in enumerate((1,2,3,4)):
        nlri=bytes([24,10,n,0]);body=struct.pack('!I',n)+nlri+struct.pack('!H',3)
        for i in range(3):
            attrs=bytes([64,2,10,2,2])+struct.pack('!II',100+i,origin)
            body+=struct.pack('!HIH',i,90,len(attrs))+attrs
            payload=struct.pack('!H',len(nlri))+nlri+b'\0\0'
            bgp=b'\xff'*16+struct.pack('!HB',len(payload)+19,2)+payload
            endpoint=struct.pack('!IIHH',100+i,999,0,1)+bytes([192,0,2,i+1,192,0,2,254])
            updates+=mrt(endpoint+bgp,epoch=101+n*3+i)
        baseline+=mrt(body,2,13)
    inputs=[]
    for name,raw,role in [('baseline',baseline,'baseline'),('updates',updates,'update')]:
        path=root/(name+'.gz');path.write_bytes(gzip.compress(raw,mtime=0))
        sha=hashlib.sha256(path.read_bytes()).hexdigest();uri='fixture://rrc25/'+name
        inputs.append(dict(path=str(path),sha256=sha,source_id=source_identity('rrc25',uri,sha),origin_uri=uri,size=path.stat().st_size,role=role))
    return dict(schema_version='observation-run/v1',collector='rrc25',window_start='1970-01-01T00:00:00Z',
                window_end_exclusive='1970-01-02T00:00:00Z',inputs=inputs,baseline_source=inputs[0]['source_id'],update_sources=[inputs[1]['source_id']])
