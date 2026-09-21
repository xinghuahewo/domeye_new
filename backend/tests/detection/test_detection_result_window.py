"""人工计算→结果窗选择→P1回执；只运行显式自有PG，保留完整计算流。"""
import copy
import gzip
import hashlib
import json
import os
from pathlib import Path
import struct
import time
from dataclasses import asdict
import pytest
from data_pipeline.analysis.detection.result_window import windows, check_message
from data_pipeline.analysis.detection import publication as d
from data_pipeline.analysis.detection.store import read_stored_rows
from data_pipeline.bgp.archive import admission as m
from tests.detection.test_detection_lake import database, formal
from tests.detection.test_detection_pipeline import six_class_observation_fixture, saved_reference_files
from tests.detection.test_detection_computation import KINDS


@pytest.fixture(scope='module')
def cases():
    root_env=os.environ.get('DETECTION_RESULT_WINDOW_ROOT');base=os.environ.get('DOMEYE_DETECTION_TEST_DSN')
    config_path=os.environ.get('DETECTION_RESULT_WINDOW_REUSE_CONFIG')
    if not root_env or not base or not config_path:pytest.skip('需要明确人工PG和输出目录')
    config=json.loads(Path(config_path).read_text())
    root=Path(root_env);root.mkdir(exist_ok=True)
    if config.get('cross_day_request'):
        q=json.loads(Path(config['cross_day_request']).read_text())
        from data_pipeline.bgp.archive.message_reader import ObservationReader
        reader=ObservationReader(q['observation_dsn'],q['input_run'],q['input_snapshot'],q['ordered_sources'],profile='observation')
        manifest=reader.selection.plan['manifest'];produced=dict(run_id=q['input_run'],snapshot=q['input_snapshot'],reused=True)
        from data_pipeline.analysis.detection.models import DetectionScope
        scope=DetectionScope(**q['scope']);start=time.monotonic()
    else:
        source=root/'new-cross-day';source.mkdir()
        manifest=six_class_observation_fixture(source);manifest['window_end_exclusive']='1970-01-03T16:00:00Z'
        from data_pipeline.bgp.input.mrt_reader import source_identity
        for entry in manifest['inputs']:
            raw=bytearray(gzip.decompress(Path(entry['path']).read_bytes()));offset=0
            while offset<len(raw):
                epoch,kind,subtype,size=struct.unpack('!IHHI',raw[offset:offset+12])
                struct.pack_into('!I',raw,offset,60000 if entry['role']=='baseline' else epoch+144000-107)
                offset+=12+size
            Path(entry['path']).write_bytes(gzip.compress(raw,mtime=0));entry['sha256']=hashlib.sha256(Path(entry['path']).read_bytes()).hexdigest()
            entry['size']=Path(entry['path']).stat().st_size;entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
        extra=dict(manifest['inputs'][0],role='snapshot',origin_uri='fixture://rrc25/unused-rib')
        extra['source_id']=source_identity('rrc25',extra['origin_uri'],extra['sha256'])
        manifest['inputs'].insert(1,extra);manifest['baseline_source']=manifest['inputs'][0]['source_id'];manifest['update_sources']=[manifest['inputs'][2]['source_id']]
        refs=saved_reference_files(source,rich=True);manifest['references']=[{k:r[k] for k in ('path','sha256')} for r in refs]
        from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
        obs=database(base);start=time.monotonic();produced=produce_checkpointed(manifest,obs,source/'input',min_free_bytes=128*1024**2,policy='isolate-payload/v1')
        version=f"{produced['run_id']}:{produced['snapshot']}"
        from data_pipeline.analysis.detection.models import DetectionScope, FileBoundary
        scope=DetectionScope('cross-day-fixture','r','rrc25',version,'1970-01-01T16:00:00Z','1970-01-03T16:00:00Z')
        sid=manifest['inputs'][2]['source_id']
        q=dict(observation_dsn=obs,input_run=produced['run_id'],input_snapshot=produced['snapshot'],ordered_sources=[manifest['inputs'][0]['source_id'],sid],input_profile='observation',scope=asdict(scope),references=refs,reference_version='artificial-v1',min_free_bytes=128*1024**2,boundaries={sid:asdict(FileBoundary(sid,version,scope.window_start,{k:k+'_197001' for k in KINDS}))})
    definitions=[('A',q,dict(window_start='1970-01-02T16:00:00Z',window_end_exclusive=scope.window_end)),('B',q,dict(window_start='1970-01-02T16:00:03Z',window_end_exclusive=scope.window_end))]
    for label in ('C-gap','zero'):
        old=json.loads(Path(config['reuse_requests'][label]).read_text())
        definitions.append((label,old,dict(window_start='1970-01-01T00:01:50Z',window_end_exclusive=old['scope']['window_end'])))
    evidence=dict(new_m2_seconds=time.monotonic()-start,new_m2=produced,input_manifest=manifest,cases={})
    allowed=(root,*(Path(p) for p in config['allowed_roots']))
    scratch=root/'scratch';scratch.mkdir();result={}
    for label,original,window in definitions:
        request=dict(original,result_window=window)
        path=Path(config.get('reuse_outputs',{}).get(label,root/label))
        if label not in config.get('reuse_outputs',{}):formal(request,base,path,'detection-m3-lake/v2')
        actual=json.loads((path/'request.json').read_text());ready=json.loads((path/'output/business/ready.json').read_text());events=[]
        u=m.Runtime(actual['observation_dsn'],allowed,scratch,fixture_only=True)
        ma=m.admit(u,m.inspect_binding(u,actual['input_run'],actual['input_snapshot'],actual['ordered_sources']),guard=lambda:None);u.dependency_admissions=(ma,)
        deps=[ma]+[m.admit(u,m.reference_binding(u,ma,x['source_id']),guard=lambda:None) for x in actual['references']]
        runtime=d.Runtime(actual['detection_dsn'],path/'output/business',allowed,scratch,tuple(deps),{x['admission_id']:u for x in deps},fixture_only=True,audit_sink=events.append)
        binding=d.inspect_binding(runtime,ready['run_id'],ready['snapshot']);admission=d.admit(runtime,binding,guard=lambda:None)
        result[label]=(runtime,binding,admission)
        evidence['cases'][label]=dict(binding=binding,admission=admission,events=events)
    yield result,evidence
    (root/'evidence.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2))


def read(runtime,admission,view):
    req=dict(view=view,scope_typed=d.typed(dict(start=0,stop=None,key=None,at_position=None)),codec_version=d.CODEC,batch_rows=9,batch_bytes=4*1024**2)
    with d.open_reader(runtime,admission,req,guard=lambda:None) as session:
        rows=[row for batch in session for row in d.untyped(batch['rows_typed'])]
    assert session.receipt
    return rows,session.receipt


def test_actual_lifecycle_selection_and_whole_chain(cases):
    cases,evidence=cases
    for label,(r,b,a) in cases.items():
        original=list(read_stored_rows(r.dsn,b['run_id'],b['snapshot'],'records'))
        assert read(r,a,'records')[0]==original
        for table in ('state_entries','m3_entries'):
            assert read(r,a,table)[0]==list(read_stored_rows(r.dsn,b['run_id'],b['snapshot'],table))
        selected,receipt=read(r,a,'result_revisions');coverage,cov_receipt=read(r,a,'result_coverage')
        ids={x['raw']['incident_id'] for x in selected}
        assert [x['raw'] for x in selected]==[row for row in original if row['record_kind']=='business_revision' and row['incident_id'] in ids]
        assert len(coverage)==len(b['identity']['selected_sources'])
        evidence['cases'][label].update(selected_typed=d.typed(selected),receipt=receipt,coverage=coverage,coverage_receipt=cov_receipt)
        if label=='A':
            assert b['scope']['window_start']=='1970-01-01T16:00:00Z'
            assert any(x['lifecycle']['selection']=='carry_in' and x['lifecycle']['end_coverage']=='recorded' for x in selected)
            assert any(x['lifecycle']['selection']=='carry_in' and x['lifecycle']['active_in_final_saved_state'] and x['lifecycle']['revisions']==1 for x in selected)
            assert any(x['lifecycle']['selection']=='started_in_result' for x in selected)
            assert [x['upstream_rank'] for x in m.untyped(r.dependency_admissions[0]['owner_binding'])['selected_sources']]==[0,2]
        elif label=='B':
            ended={row['incident_id'] for row in original if row['record_kind']=='business_revision' and json.loads(row['legacy_json']).get('e_time')=='1970-01-03 00:00:01'}
            assert ended and not (ended&ids)
        elif label=='C-gap':
            assert any(x['main'] is None and x['qualification']['coverage']=='unknown' for x in selected)
            assert any(json.loads(x['raw']['payload_json'])['coverage']=='unknown' for x in coverage)
        elif label=='zero':assert not selected and coverage and receipt['rows']==0
    # A/B仅结果窗不同；计算全记录逐值相同，未按D裁剪原科学输出。
    ra,ba,aa=cases['A'];rb,bb,ab=cases['B']
    from tests.detection.test_detection_lake import comparable_json
    # 仅旧MOAS成对成员的既有跨冻结进程语义对照，不修改任何制品或普通列表。
    assert comparable_json(read(ra,aa,'records')[0])==comparable_json(read(rb,ab,'records')[0])


def test_invalid_windows_and_all_message_kinds():
    scope=dict(window_start='1970-01-01T16:00:00Z',window_end='1970-01-03T16:00:00Z')
    assert windows(scope)['result_window']['window_start']==scope['window_start']
    for bad in [dict(window_start=scope['window_end'],window_end_exclusive=scope['window_end']),dict(window_start='1970-01-01T00:00:00Z',window_end_exclusive=scope['window_end']),dict(window_start=scope['window_start'],window_end='x')]:
        with pytest.raises(ValueError):windows(scope,bad)
    for kind in ('update','state','gap','empty'):
        row=dict(epoch=57600,microsecond=None,mrt_type=16,message_id=kind)
        assert check_message(row,windows(scope)['calculation_window'])
        with pytest.raises(ValueError):check_message(dict(row,epoch=230400),windows(scope)['calculation_window'])
        with pytest.raises(ValueError):check_message(dict(row,epoch=57599),windows(scope)['calculation_window'])
    with pytest.raises(ValueError):check_message(dict(epoch=57600,microsecond=None,mrt_type=17),windows(scope)['calculation_window'])


def test_real_frozen_rejects_bad_window_source_and_gap(cases):
    import subprocess,sys
    cases,evidence=cases;root=Path(os.environ['DETECTION_RESULT_WINDOW_ROOT']);base=os.environ['DOMEYE_DETECTION_TEST_DSN']
    script=Path(__file__).resolve().parents[3]/'scripts/pipeline/detection-frozen-run.py'
    definitions=[]
    for name,label in [('wrong_result','A'),('wrong_source','A'),('raw_gap_outside','C-gap')]:
        r,b,a=cases[label];q=json.loads((r.output_root.parent.parent/'request.json').read_text())
        if name=='wrong_result':q['result_window']=dict(window_start='1970-01-01T00:00:00Z',window_end_exclusive=q['scope']['window_end'])
        elif name=='wrong_source':
            original=m.untyped(r.dependency_admissions[0]['owner_binding'])
            extra=next(x['source_id'] for x in json.loads(original['input_binding'])['sources'] if x['role']=='snapshot')
            q['ordered_sources'].insert(1,extra)
        else:
            q['scope']['window_start']='1970-01-01T00:01:41Z'
            for boundary in q['boundaries'].values():boundary['observed_at']=q['scope']['window_start']
        path=root/name;path.mkdir();q['output']=str(path/'output');q['detection_dsn']=database(base)
        req=path/'request.json';req.write_text(json.dumps(q));start=time.monotonic()
        done=subprocess.run([sys.executable,str(script),str(req)],capture_output=True,text=True)
        (path/'process.log').write_text(done.stdout+done.stderr)
        assert done.returncode!=0 and not (path/'output/business/ready.json').exists()
        assert ('RawTime' if name=='raw_gap_outside' else 'source rank' if name=='wrong_source' else '结果窗') in done.stderr
        definitions.append(dict(case=name,seconds=time.monotonic()-start,returncode=done.returncode,no_ready=True))
    evidence['rejected_production']=definitions


def test_default_window_actual_production(cases):
    import subprocess,sys
    cases,evidence=cases;old,b,a=cases['zero'];root=Path(os.environ['DETECTION_RESULT_WINDOW_ROOT'])/'default-result'
    root.mkdir();q=json.loads((old.output_root.parent.parent/'request.json').read_text());q.pop('result_window',None)
    q['output']=str(root/'output');q['detection_dsn']=database(os.environ['DOMEYE_DETECTION_TEST_DSN'])
    req=root/'request.json';req.write_text(json.dumps(q));script=Path(__file__).resolve().parents[3]/'scripts/pipeline/detection-frozen-run.py'
    start=time.monotonic();done=subprocess.run([sys.executable,str(script),str(req)],capture_output=True,text=True)
    (root/'process.log').write_text(done.stdout+done.stderr);assert done.returncode==0
    ready=json.loads((root/'output/business/ready.json').read_text())
    runtime=copy.copy(old);runtime.dsn=q['detection_dsn'];runtime.output_root=root/'output/business'
    binding=d.inspect_binding(runtime,ready['run_id'],ready['snapshot']);admission=d.admit(runtime,binding,guard=lambda:None)
    assert binding['identity']['result_window']==dict(window_start=binding['scope']['window_start'],window_end_exclusive=binding['scope']['window_end'])
    rows,receipt=read(runtime,admission,'result_revisions');coverage,cov_receipt=read(runtime,admission,'result_coverage')
    assert rows==[] and coverage
    evidence['default_production']=dict(seconds=time.monotonic()-start,binding=binding,admission=admission,receipt=receipt,coverage_receipt=cov_receipt)


def test_unknown_anchor_not_filtered_by_reported_late_start():
    from data_pipeline.analysis.detection.result_selection import selection
    b=dict(scope=dict(window_start='1970-01-01T00:00:00Z',window_end='1970-01-04T00:00:00Z'),identity=dict(result_window_rule='detection-result-window/v1',result_window=dict(window_start='1970-01-02T00:00:00Z',window_end_exclusive='1970-01-03T00:00:00Z'),window_coverage={},qualification_as_of_position=[1,1,1,1]))
    legacy=dict(s_time='1970-01-03 09:00:00',e_time=None)
    row=dict(event_kind='prefix_outage',selection_first=json.dumps(legacy),selection_last=json.dumps(legacy),selection_final_q=json.dumps(dict(coverage='unknown')),selection_state=None,selection_first_sequence=0,selection_last_sequence=0,selection_revisions=1,selection_state_ordinal=None,selection_final_qid='q',selection_q=json.dumps(dict(coverage='unknown')),selection_qid='q')
    selected=selection(row,b)
    assert selected['main'] is None and selected['lifecycle']['selection']=='possible_unknown'


def test_result_views_keep_close_failure_contract(cases,monkeypatch):
    from data_pipeline.analysis.detection import publication_io as io
    cases,_=cases;r,b,a=cases['A'];connect=io.connect_duckdb
    class ClosingDB:
        def __init__(self):self.db=connect();self.body=False
        def execute(self,query,*args):
            if query.startswith(('WITH revisions','SELECT r.*')):self.body=True
            return self.db.execute(query,*args)
        def close(self):
            self.db.close()
            if self.body:raise RuntimeError('人工结果视图数据库关闭故障')
    monkeypatch.setattr(io,'connect_duckdb',ClosingDB)
    for view in ('result_revisions','result_coverage'):
        req=dict(view=view,scope_typed=d.typed(dict(start=0,stop=None,key=None,at_position=None)),codec_version=d.CODEC,batch_rows=1,batch_bytes=4*1024**2)
        with pytest.raises(RuntimeError,match='关闭故障'):
            with d.open_reader(r,a,req,guard=lambda:None) as session:list(session)
        assert session.receipt is None
        with pytest.raises(LookupError) as caught:
            with d.open_reader(r,a,req,guard=lambda:None) as session:
                next(session);raise LookupError('调用方主错')
        assert session.receipt is None and caught.value.cleanup_errors
