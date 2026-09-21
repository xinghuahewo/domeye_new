"""人工原生批次直接计算：顺序等价、文件事务以及归档成功后的独立恢复。"""
import json
from pathlib import Path
import pytest

from data_pipeline.bgp.pipeline import run_pipeline
from data_pipeline.bgp.archive.file_reader import selected_prefix
from data_pipeline.bgp.archive.message_reader import ObservationReader
from tests.observations.test_observation_checkpoint import dsn, fixture
from tests.observations.test_observation_native import native
from tests.observations.test_route_pipeline import assert_serial, replace_update


@pytest.mark.parametrize('batch_rows',[1,3,100000])
def test_direct_batches_match_canonical_without_full_file_reader(tmp_path,native,dsn,monkeypatch,batch_rows):
    manifest=fixture(tmp_path/'input',bad=True);root=tmp_path/'run';seen=[]
    original=ObservationReader.stream
    def forbidden(*args,**kwargs):raise AssertionError('直接主链路不得全文件 JOIN')
    monkeypatch.setattr(ObservationReader,'stream',forbidden)
    def hook(label,ordinal):
        if label=='batch_computed':
            run_id=json.loads((root/'pipeline-run.json').read_text())['run_id']
            selected=selected_prefix(dsn,run_id)
            assert len(selected['checkpoints'])==ordinal
            seen.append(ordinal)
    result=run_pipeline(manifest,dsn,root,native_build=native.build,batch_rows=batch_rows,
                        audit_transitions=True,min_free_bytes=0,policy='isolate-payload/v1',hook=hook)
    assert set(seen)=={1,2,3}
    monkeypatch.setattr(ObservationReader,'stream',original)
    assert_serial(dsn,root,result)
    assert run_pipeline(manifest,dsn,root,native_build=native.build,batch_rows=batch_rows,
                        audit_transitions=True,min_free_bytes=0,policy='isolate-payload/v1')==result


@pytest.mark.parametrize('stop',['batch_computed','files_ready','before_state_commit','after_state_commit'])
def test_direct_resume_does_not_skip_archived_batches(tmp_path,native,dsn,stop):
    manifest=fixture(tmp_path/'input',bad=True);root=tmp_path/'run'
    def hook(label,ordinal):
        if label==stop and ordinal==2:raise RuntimeError('人工中断')
    args=dict(manifest=manifest,dsn=dsn,output=root,native_build=native.build,batch_rows=3,
              audit_transitions=True,min_free_bytes=0,policy='isolate-payload/v1')
    with pytest.raises(RuntimeError,match='人工中断'):run_pipeline(**args,hook=hook)
    result=run_pipeline(**args)
    assert_serial(dsn,root,result)


def test_empty_update_direct_file_boundary(tmp_path,native,dsn):
    manifest=fixture(tmp_path/'input');replace_update(manifest,-1,b'')
    root=tmp_path/'run'
    result=run_pipeline(manifest,dsn,root,native_build=native.build,batch_rows=3,audit_transitions=True,min_free_bytes=0)
    assert_serial(dsn,root,result)


@pytest.mark.parametrize('stop',[None,'batch_computed','files_ready','before_state_commit','after_state_commit'])
def test_native_business_file_transaction(tmp_path,native,dsn,stop):
    from tests.observations.test_batch_business import native_business_fixture, assert_business_serial
    manifest,config=native_business_fixture(tmp_path/'input');root=tmp_path/'run'
    args=dict(manifest=manifest,dsn=dsn,output=root,native_build=native.build,batch_rows=2,
              min_free_bytes=0,business_config=config)
    if stop:
        def hook(label,ordinal):
            if label==stop and ordinal==2:raise RuntimeError('业务候选中断')
        with pytest.raises(RuntimeError,match='业务候选中断'):run_pipeline(**args,hook=hook)
    result=run_pipeline(**args)
    assert_business_serial(dsn,root,result,manifest,config)
    assert run_pipeline(**args)==result


@pytest.mark.parametrize('stop,producer',[('before_segment_archive',True),('after_segment',True),
    ('batch_computed',False),('files_ready',False),('before_state_commit',False),('after_state_commit',False)])
def test_sigkill_business_and_cursor_recovery(tmp_path,native,dsn,stop,producer):
    import os,signal,subprocess,sys
    from tests.observations.test_observation_checkpoint import wait_marker
    from tests.observations.test_batch_business import native_business_fixture, assert_business_serial
    manifest,config=native_business_fixture(tmp_path/'input');root=tmp_path/'run';marker=tmp_path/'marker'
    args=dict(manifest=manifest,dsn=dsn,output=str(root),native_build=str(native.build),batch_rows=2,
        min_free_bytes=0,business_config=config,direct_batches=True,stop=stop,ordinal=2,producer_stop=producer,marker=str(marker))
    settings=tmp_path/'worker.json';settings.write_text(json.dumps(args))
    log=tmp_path/'worker.log'
    with log.open('w') as f:p=subprocess.Popen([sys.executable,str(Path(__file__).with_name('route_worker.py')),str(settings)],stdout=f,stderr=subprocess.STDOUT,env=os.environ.copy())
    try:
        wait_marker(p,marker);os.kill(p.pid,signal.SIGKILL);assert p.wait(timeout=10)!=0
    finally:
        if p.poll() is None:p.kill();p.wait()
    args={k:v for k,v in args.items() if k not in ('direct_batches','stop','ordinal','producer_stop','marker')}
    result=run_pipeline(**args)
    assert_business_serial(dsn,root,result,manifest,config)
    assert run_pipeline(**args)==result


def test_archive_and_single_writer_overlap_with_bound(tmp_path,native,dsn):
    import threading
    manifest=fixture(tmp_path/'input');archiving=threading.Event();computed=threading.Event()
    def compute(label,ordinal):
        if label=='batch_computed' and ordinal==1:
            assert archiving.wait(15),'归档与计算没有重叠'
            computed.set()
    def archive(label,ordinal):
        if label=='before_segment_archive' and ordinal==1:
            archiving.set();assert computed.wait(15),'计算未在归档结束前完成'
    root=tmp_path/'run'
    run_pipeline(manifest,dsn,root,native_build=native.build,batch_rows=2,min_free_bytes=0,hook=compute,producer_hook=archive)
    metric=json.loads(next(root.glob('route-metrics-*.json')).read_text())
    assert metric['compute_queue_peak']==1 and archiving.is_set() and computed.is_set()


@pytest.mark.parametrize('batch_rows',[1,4,100000])
def test_direct_rich_evidence_peer_change_snapshot_and_clock_reversal(tmp_path,native,dsn,batch_rows):
    import gzip,hashlib,struct
    from data_pipeline.bgp.input.mrt_reader import source_identity
    from tests.observations.test_observation_native import raw_fixture
    from tests.observations.test_observation_native_update import update_fixture
    from tests.observations.test_observation_mrt import mrt, update, attr
    manifest=fixture(tmp_path/'input')
    # 两次 Peer 表及同序号再用；独立 Resource 快照不得推进当前态。
    changed=bytearray(raw_fixture());offset=12+4+2+5+2+1+4+4
    assert struct.unpack_from('!I',changed,offset)[0]==64497
    struct.pack_into('!I',changed,offset,64499)
    replace_update(manifest,0,raw_fixture()+changed)
    manifest['baseline_source']=manifest['inputs'][0]['source_id']
    baseline=manifest['inputs'][0]
    snapshot={**baseline,'origin_uri':'fixture://rrc25/independent-snapshot','role':'snapshot'}
    snapshot['source_id']=source_identity('rrc25',snapshot['origin_uri'],snapshot['sha256'])
    manifest['inputs'].insert(1,snapshot)
    earlier=mrt(update()[12:],4,16,99)
    as4=update(attrs=attr(23456)+b'\xc0\x11\x06\x02\x01'+struct.pack('!I',64496))
    replace_update(manifest,-2,update_fixture()+as4+earlier)
    replace_update(manifest,-1,update_fixture())
    root=tmp_path/'run'
    result=run_pipeline(manifest,dsn,root,native_build=native.build,batch_rows=batch_rows,
        audit_transitions=True,min_free_bytes=0,policy='isolate-payload/v1')
    rows=assert_serial(dsn,root,result)
    from data_pipeline.bgp.archive.message_reader import MessageBatch
    reader=ObservationReader(dsn,result['run_id'],result['observation_snapshot'],[manifest['baseline_source'],*manifest['update_sources']],profile='observation')
    qualities=[q['code'] for batch in reader.stream() if isinstance(batch,MessageBatch) for m in batch.messages for q in m['quality']]
    assert 'timestamp_regression' in qualities
    assert not any(t=='changes' and r.get('source_id')==snapshot['source_id'] for t,r in rows)


@pytest.mark.parametrize('damage',['business_payload','business_delete','compact_payload','compact_delete'])
def test_direct_persistent_state_damage_rejects_resume(tmp_path,native,dsn,damage):
    import psycopg2
    from tests.observations.test_batch_business import native_business_fixture
    manifest,config=native_business_fixture(tmp_path/'input');root=tmp_path/'run'
    args=dict(manifest=manifest,dsn=dsn,output=root,native_build=native.build,batch_rows=2,min_free_bytes=0,business_config=config)
    result=run_pipeline(**args)
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        table='business_rows_v2' if damage.startswith('business') else 'packed_rows'
        if damage.endswith('delete'):c.execute('DELETE FROM route_file.'+table+' WHERE ctid=(SELECT ctid FROM route_file.'+table+' WHERE run_id=%s LIMIT 1)',(result['run_id'],))
        else:c.execute('UPDATE route_file.'+table+" SET digest=%s WHERE ctid=(SELECT ctid FROM route_file."+table+' WHERE run_id=%s LIMIT 1)',('0'*64 if table=='business_rows_v2' else bytes(32),result['run_id']))
        assert c.rowcount==1, '必须确实损坏当前格式的一行工作态'
    with pytest.raises(ValueError,match='摘要|缺行|顺序|集合|清单'):run_pipeline(**args)


def test_default_cli_direct_business_contract(tmp_path,native,dsn):
    import subprocess,sys
    from tests.observations.test_batch_business import native_business_fixture, assert_business_serial
    manifest,business=native_business_fixture(tmp_path/'input')
    m=tmp_path/'manifest.json';b=tmp_path/'business.json';c=tmp_path/'config.json';root=tmp_path/'run'
    m.write_text(json.dumps(manifest));b.write_text(json.dumps(business))
    c.write_text(json.dumps(dict(dsn=dsn,native_build=str(native.build),business_config=str(b),limits=dict(batch_rows=2,min_free_bytes=0))))
    script=Path(__file__).resolve().parents[3]/'scripts/observations.py'
    run=subprocess.run([sys.executable,str(script),'route-pipeline','--config',str(c),'--manifest',str(m),'--output',str(root)],capture_output=True,text=True,timeout=60)
    assert run.returncode==0,run.stderr
    result=json.loads(run.stdout.strip().splitlines()[-1]);assert result['profile']=='route-batch-candidate/v1'
    assert_business_serial(dsn,root,result,manifest,business)
    assert not list(root.glob('hot-paths-*'))


def test_business_hash_runtime_drift_cannot_silently_resume(tmp_path,native,dsn):
    import os,subprocess,sys
    from tests.observations.test_batch_business import native_business_fixture
    manifest,config=native_business_fixture(tmp_path/'input');root=tmp_path/'run'
    args=dict(manifest=manifest,dsn=dsn,output=str(root),native_build=str(native.build),batch_rows=2,min_free_bytes=0,business_config=config)
    run_pipeline(**args)
    args['direct_batches']=True;settings=tmp_path/'worker.json';settings.write_text(json.dumps(args))
    changed=str((int(os.environ['PYTHONHASHSEED'])+1)%4294967296)
    result=subprocess.run([sys.executable,str(Path(__file__).with_name('route_worker.py')),str(settings)],capture_output=True,text=True,
        env={**os.environ,'PYTHONHASHSEED':changed},timeout=30)
    assert result.returncode!=0 and '运行计划、规则或输出位置漂移' in result.stderr
