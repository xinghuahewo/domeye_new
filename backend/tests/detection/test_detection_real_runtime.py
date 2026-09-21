"""复用既有人工M2/D，在实际PG核验真实候选Runtime；不生产科学制品。"""
import copy
import json
import os
from pathlib import Path
import shutil
import time

import pytest
from data_pipeline.bgp.archive import admission as upstream
from data_pipeline.analysis.detection import publication as d
from data_pipeline.analysis.detection.store import read_stored_rows


def test_fixture_and_real_candidate_public_chain():
    path = os.environ.get('DETECTION_REAL_RUNTIME_CONFIG')
    if not path: pytest.skip('必须明确自有人工制品配置')
    config = json.loads(Path(path).read_text()); q = json.loads(Path(config['request']).read_text())
    output = Path(config['output_root']); ready_text = (output/'ready.json').read_text()
    ready = json.loads(ready_text); allowed = tuple(Path(p) for p in config['allowed_roots'])
    scratch = Path(config['scratch_root']); uscratch = Path(config['upstream_scratch'])
    binding = {k: ready[k] for k in ('run_id','snapshot','identity','scope')}
    limits = dict(memory_bytes=256*1024**2,max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,
                  lock_timeout_ms=2000,min_free_bytes=128*1024**2)
    base = dict(dsn=q['detection_dsn'],output_root=output,allowed_roots=allowed,scratch_root=scratch)
    result = {}; runtimes = {}; admissions = {}; evidence = dict(cases=result,rejections=[])
    def reject(name, call, match=None):
        with pytest.raises(ValueError, match=match): call()
        evidence['rejections'].append(name)
    for real in (False, True):
        mode = 'real-candidate/v1' if real else 'synthetic-fixture/v1'; events=[]; started=time.monotonic()
        if not real:
            u = upstream.Runtime(q['observation_dsn'],allowed,uscratch,fixture_only=True)
            ub = upstream.inspect_binding(u,q['input_run'],q['input_snapshot'],q['ordered_sources'])
        else:
            u = upstream.Runtime(q['observation_dsn'],allowed,uscratch,
                execution_profile=mode,input_manifest=ub['plan']['manifest'],expected_m2_binding=ub,
                memory_limit='256MB',max_temp_bytes=512*1024**2,max_rss_bytes=2*1024**3,
                lock_timeout_ms=2000,min_free_bytes=128*1024**2)
            assert upstream.inspect_binding(u,q['input_run'],q['input_snapshot'],q['ordered_sources']) == ub
        ma = upstream.admit(u,ub,guard=lambda:None); u.dependency_admissions=(ma,)
        deps = [ma]+[upstream.admit(u,upstream.reference_binding(u,ma,x['source_id']),guard=lambda:None) for x in q['references']]
        upstream_wall = time.monotonic()-started
        kwargs = dict(base,dependency_admissions=tuple(deps),dependency_runtimes={a['admission_id']:u for a in deps},audit_sink=events.append)
        kwargs.update(dict(execution_profile=mode,expected_detection_binding=binding,**limits) if real else dict(fixture_only=True))
        r = d.Runtime(**kwargs); runtimes[mode]=r
        started=time.monotonic(); assert d.inspect_binding(r,binding['run_id'],binding['snapshot']) == binding
        a = d.admit(r,binding,guard=lambda:None); admission_wall=time.monotonic()-started
        admissions[mode]=a; d.verify_current(r,a,guard=lambda:None)
        case=dict(upstream_wall_seconds=upstream_wall,inspect_admit_wall_seconds=admission_wall,
                  binding=binding,admission=a,dependencies=deps,reads={},events=events)
        result[mode]=case
        for table in ('records','state_entries','m3_entries'):
            request=dict(view=table,scope_typed=d.typed(dict(start=0,stop=None,key=None,at_position=None)),
                         codec_version=d.CODEC,batch_rows=11,batch_bytes=4*1024**2)
            started=time.monotonic(); first=None; rows=[]; encoded_bytes=0
            with d.open_reader(r,a,request,guard=lambda:None) as session:
                for batch in session:
                    if first is None:first=time.monotonic()-started
                    rows.extend(d.untyped(batch['rows_typed'])); encoded_bytes+=batch['bytes']
            wall=time.monotonic()-started
            assert session.receipt and rows == list(read_stored_rows(r.dsn,binding['run_id'],binding['snapshot'],table))
            case['reads'][table]=dict(rows=len(rows),first_batch_seconds=first,wall_seconds=wall,
                                     encoded_bytes=encoded_bytes,rows_typed=d.typed(rows),receipt=session.receipt)
        for target in a['lock_targets']:
            with d.hold_lock(r,a,target,guard=lambda:None):pass
        case['resource_usage']=copy.deepcopy(r.resource_usage)
        if real:
            real_kwargs=kwargs
            wrong_output=scratch.parent/'wrong-output';wrong_output.mkdir(exist_ok=True)
            wrong=d.Runtime(**dict(kwargs,output_root=wrong_output))
            reject('fresh_wrong_output_current',lambda:d.verify_current(wrong,a,guard=lambda:None),'输出范围')
            reject('fresh_wrong_output_admit',lambda:d.admit(wrong,binding,guard=lambda:None),'输出范围')
            reject('scratch_overlaps_output',lambda:d.Runtime(**dict(kwargs,scratch_root=output)),'隔离')
            source_parent=Path(ub['plan']['inputs'][0]['path']).parent
            wrong=d.Runtime(**dict(kwargs,scratch_root=source_parent))
            reject('scratch_overlaps_input',lambda:d.admit(wrong,binding,guard=lambda:None),'隔离')
            for field in limits:
                missing=dict(kwargs);missing.pop(field)
                reject('missing_'+field,lambda: d.Runtime(**missing))
            for field in ('dsn','allowed_roots','output_root','scratch_root','expected_detection_binding'):
                altered=copy.copy(r)
                if field=='dsn': altered.dsn += ' application_name=drift'
                elif field=='allowed_roots': altered.allowed_roots=()
                elif field=='expected_detection_binding':
                    altered.expected_detection_binding=copy.deepcopy(binding)
                    altered.expected_detection_binding['scope']['window_end']='1970-01-03T00:00:00Z'
                else:setattr(altered,field,uscratch)
                reject('runtime_drift_'+field,lambda:d.inspect_binding(altered,binding['run_id'],binding['snapshot']))
            # 显式完整范围核验也覆盖未来窗口字段；不生产或准入一个伪造窗口制品。
            changed=copy.deepcopy(binding);changed['identity']['result_window']={'window_start':'1970-01-01T00:00:00Z','window_end_exclusive':'1970-01-02T00:00:00Z'}
            reject('window_binding_mismatch',lambda:r.check_binding(changed))
            altered=copy.copy(r);altered.fixture_only=True
            reject('mode_drift',lambda:d.inspect_binding(altered,binding['run_id'],binding['snapshot']))
            for field, value, match in [('max_rss_bytes',1,'RSS'),('min_free_bytes',shutil.disk_usage(scratch).free+1024**3,'空闲盘')]:
                altered=copy.copy(r);setattr(altered,field,value)
                reject('actual_'+field,lambda:d.inspect_binding(altered,binding['run_id'],binding['snapshot']),match)
            altered=copy.copy(r)
            with upstream._pg(altered) as pg,pg.cursor() as c:
                assert upstream._sql(altered,c,'SELECT 1').fetchone()==(1,)
                probe=scratch/'runtime-temp-probe';probe.write_bytes(b'xx');altered.max_temp_bytes=1
                try:reject('actual_sql_temp_guard',lambda:upstream._sql(altered,c,'SELECT 2'),'临时空间')
                finally:probe.unlink()
            # 完整读取后最后current的实际资源失败不得签Receipt。
            with pytest.raises(ValueError,match='RSS'):
                with d.open_reader(r,a,request,guard=lambda:None) as failed:
                    list(failed);r.max_rss_bytes=1
            assert failed.receipt is None;r.max_rss_bytes=limits['max_rss_bytes']
            evidence['rejections'].append('tail_resource_failure_no_receipt')
            # 真实scratch被换成符号链接或不同inode都必须在使用时拒绝，并原样复位。
            saved=scratch.with_name('scratch-held');scratch.rename(saved)
            try:
                scratch.symlink_to(uscratch,target_is_directory=True)
                try:reject('scratch_symlink_replacement',lambda:r.resource_guard(),'scratch')
                finally:scratch.unlink()
                scratch.mkdir()
                try:reject('scratch_inode_replacement',lambda:r.resource_guard(),'scratch')
                finally:scratch.rmdir()
            finally:saved.rename(scratch)
    fixture=runtimes['synthetic-fixture/v1']; real=runtimes['real-candidate/v1']
    bad=copy.copy(fixture);bad.fixture_only=1
    reject('non_bool_mode_at_use',lambda:bad.resource_guard(),'模式')
    for r, a in [(real,admissions['synthetic-fixture/v1']),(fixture,admissions['real-candidate/v1'])]:
        reject('cross_mode_current',lambda:d.verify_current(r,a,guard=lambda:None))
        def cross_lock():
            with d.hold_lock(r,a,a['lock_targets'][0],guard=lambda:None):pass
        reject('cross_mode_lock',cross_lock)
    bad=copy.copy(real);bad.dependency_admissions=fixture.dependency_admissions;bad.dependency_runtimes=fixture.dependency_runtimes
    reject('cross_mode_dependencies',lambda:d.admit(bad,binding,guard=lambda:None),'模式')
    mixed=dict(real_kwargs,fixture_only=True)
    reject('mixed_mode_constructor',lambda:d.Runtime(**mixed))
    for mode in result:
        assert result[mode]['reads']['records']['rows']==131
        for table, read in result[mode]['reads'].items():
            assert read['rows_typed']==result['synthetic-fixture/v1']['reads'][table]['rows_typed']
    assert (output/'ready.json').read_text()==ready_text
    evidence.update(original_ready_unchanged=True,no_scientific_production=True)
    Path(config['evidence']).write_text(json.dumps(evidence,ensure_ascii=False,indent=2))
