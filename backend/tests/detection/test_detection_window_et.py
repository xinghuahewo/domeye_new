"""可信ET头与原NULL微秒并存：复用独立生成的人工M2，实际完成/准入。"""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import psycopg2
import pytest
from data_pipeline.analysis.detection import publication as d
from data_pipeline.analysis.detection.store import read_stored_rows
from data_pipeline.bgp.archive import admission as m
from tests.detection.test_detection_lake import database
from tests.detection.test_detection_result_window import read


def test_retained_et_gap_completes_and_admits():
    root_env=os.environ.get('DETECTION_WINDOW_ET_ROOT');base=os.environ.get('DOMEYE_DETECTION_TEST_DSN')
    if not root_env or not base:pytest.skip('必须明确自有人工ET制品及PG')
    root=Path(root_env);q=json.loads((root/'reused-request.json').read_text())
    before=json.loads((root/'before-evidence.json').read_text())
    original=[x['raw'] for x in before['boundaries']]
    et=[x for x in before['boundaries'] if x['raw']['mrt_type']==17 and x['raw']['microsecond'] is None]
    assert len(et)==2 and all(x['raw_time']['microsecond']==900000 for x in et)
    assert {x['gap']['parse_status'] for x in et}=={'rejected','unsupported'}
    script=Path(__file__).resolve().parents[3]/'scripts/pipeline/detection-frozen-run.py'
    case=root/'after';case.mkdir();request=dict(q,detection_dsn=database(base),output=str(case/'output'))
    req=case/'request.json';req.write_text(json.dumps(request));start=time.monotonic()
    done=subprocess.run([sys.executable,str(script),str(req)],capture_output=True,text=True)
    wall=time.monotonic()-start;(case/'process.log').write_text(done.stdout+done.stderr);assert done.returncode==0,done.stderr
    ready_path=case/'output/business/ready.json';ready_text=ready_path.read_text();ready=json.loads(ready_text)
    scratch=root/'scratch';u=m.Runtime(q['observation_dsn'],(root,),scratch,fixture_only=True)
    ma=m.admit(u,m.inspect_binding(u,q['input_run'],q['input_snapshot'],q['ordered_sources']),guard=lambda:None);u.dependency_admissions=(ma,)
    deps=[ma]+[m.admit(u,m.reference_binding(u,ma,x['source_id']),guard=lambda:None) for x in q['references']]
    events=[];runtime=d.Runtime(request['detection_dsn'],ready_path.parent,(root,),scratch,tuple(deps),{x['admission_id']:u for x in deps},fixture_only=True,audit_sink=events.append)
    start=time.monotonic();binding=d.inspect_binding(runtime,ready['run_id'],ready['snapshot']);admission=d.admit(runtime,binding,guard=lambda:None);admitwall=time.monotonic()-start
    evidence=dict(production_wall_seconds=wall,admit_wall_seconds=admitwall,binding=binding,admission=admission,dependencies=deps,reads={},events=events)
    for table in ('records','state_entries','m3_entries','result_revisions','result_coverage'):
        start=time.monotonic();rows,receipt=read(runtime,admission,table)
        evidence['reads'][table]=dict(seconds=time.monotonic()-start,rows_typed=d.typed(rows),receipt=receipt,rows=len(rows))
        if table in ('records','state_entries','m3_entries'):
            assert rows==list(read_stored_rows(runtime.dsn,binding['run_id'],binding['snapshot'],table))
        if table=='records':
            saved=[json.loads(row['legacy_json']) for row in rows if row['record_kind']=='source_message']
            assert saved==original
        if table=='m3_entries':
            gaps=[json.loads(row['payload_json']) for row in rows if row['kind']=='scope_gap']
            assert len(gaps)==2
        if table=='result_coverage':
            assert any(json.loads(row['raw']['payload_json'])['coverage']=='unknown' for row in rows)
    coverage=binding['identity']['window_coverage']
    assert coverage['update_messages']==coverage['result_messages']==5
    assert coverage['first_raw_time']=='1970-01-01T00:01:40.900000+00:00'
    assert coverage['last_raw_time']=='1970-01-01T00:01:44.900000+00:00'
    assert ready_path.read_text()==ready_text
    # 同一原M2，只有首个无元素ET Gap在计算窗外；不能补0或跳Gap。
    negative=root/'outside';negative.mkdir();bad=copy.deepcopy(q)
    bad['scope']['window_start']='1970-01-01T00:01:40.950000Z'
    bad['result_window']['window_start']='1970-01-01T00:01:41Z'
    for boundary in bad['boundaries'].values():boundary['observed_at']=bad['scope']['window_start']
    bad.update(detection_dsn=database(base),output=str(negative/'output'))
    bad_path=negative/'request.json';bad_path.write_text(json.dumps(bad));start=time.monotonic()
    failed=subprocess.run([sys.executable,str(script),str(bad_path)],capture_output=True,text=True)
    (negative/'process.log').write_text(failed.stdout+failed.stderr)
    assert failed.returncode!=0 and 'RawTime在计算窗之外' in failed.stderr and not (negative/'output/business/ready.json').exists()
    evidence['outside']=dict(seconds=time.monotonic()-start,no_ready=True,returncode=failed.returncode)
    evidence['pg']={}
    for label in ('before','after','outside'):
        request=json.loads((root/label/'request.json').read_text())
        with psycopg2.connect(request['detection_dsn']) as pg,pg.cursor() as c:
            c.execute('SELECT run_id,state,snapshot FROM detection.runs ORDER BY run_id');evidence['pg'][label]=c.fetchall()
    (root/'after-evidence.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2))
