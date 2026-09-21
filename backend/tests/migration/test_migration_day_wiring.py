"""纯元数据/受控失败验证；不生产观察或伪造成功Admission。"""
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from data_pipeline.jobs.input_plan import FixedInputs, require_available, build_requests
from data_pipeline.jobs.stage_runner import run_stage


def inputs():
    r = [1,2,3,4,5,6,0,7,8,297,586,587]
    fc = [i for i in range(588) if i not in set(r)-{0}]
    entries = [dict(source_id=str(i), role='baseline' if i == 0 else 'snapshot' if i in r else 'update') for i in range(588)]
    manifest = dict(inputs=entries, references=[dict(sha256=f'{i:064x}', path='/not-read/'+str(i)) for i in range(11)],
                    baseline_source='0', update_sources=list(map(str, fc[1:])))
    def selected(ranks):
        return [dict(selection_sequence_zero_based=n, joint_mrt_rank_zero_based=i,
                     source_id=str(i), joint_role=entries[i]['role']) for n,i in enumerate(ranks)]
    mapping = dict(consumers={name:dict(selected_sources=selected(r if name == 'Resource' else fc))
                             for name in ('Resource','Feature','canonical','Detection')})
    return manifest, mapping


def test_global_rank_is_not_resource_consumer_sequence():
    m, mapping = inputs()
    fixed = FixedInputs(m, mapping)
    assert fixed.sources('Resource')[6] == '0'
    assert fixed.sources('Feature')[0] == '0'
    mapping['consumers']['Resource']['selected_sources'][6]['joint_mrt_rank_zero_based'] = 6
    with pytest.raises(ValueError, match='rank'):
        FixedInputs(m, mapping)


def test_no_partial_launch_before_full_chain_preflight():
    launch = Mock()
    with pytest.raises(ValueError, match='Country'):
        require_available(['Country','Trend','fullP'])
        launch()
    launch.assert_not_called()


def test_disk_refusal_keeps_receipt_without_launch(tmp_path):
    launch = Mock()
    with pytest.raises(RuntimeError, match='磁盘'):
        run_stage(['never-start'], tmp_path/'stage', threads=2, max_rss_bytes=100,
                  min_free_bytes=100, disk_roots=[tmp_path], launch=launch,
                  disk_usage=lambda p: SimpleNamespace(free=99))
    launch.assert_not_called()
    receipt = json.loads((tmp_path/'stage/阶段观测.json').read_text())
    assert receipt['state'] == 'failed'
    assert receipt['parent_sample_peak_rss_bytes'] is None
    assert receipt['processed_count'] is None
    assert receipt['wall_seconds'] >= 0


def test_nonzero_child_is_not_admitted_and_environment_is_explicit(tmp_path, monkeypatch):
    import data_pipeline.jobs.stage_runner as module
    process = Mock(pid=123)
    process.poll.return_value = 7
    process.wait.return_value = 7
    launch = Mock(return_value=process)
    monkeypatch.setattr(module, 'stop_process_group', Mock(return_value=dict(state='group_exited',exit_code=7)))
    monkeypatch.setenv('PYTHONPATH', '/untrusted-old-project')
    with pytest.raises(RuntimeError, match='入口失败'):
        run_stage(['original-entry'], tmp_path/'stage', threads=2, max_rss_bytes=100,
                  min_free_bytes=100, disk_roots=[tmp_path], launch=launch,
                  disk_usage=lambda p: SimpleNamespace(free=1000))
    kwargs = launch.call_args.kwargs
    assert kwargs['env']['DOMEYE_DUCKDB_THREADS'] == '2'
    assert 'PYTHONPATH' not in kwargs['env']
    assert kwargs['start_new_session'] is True
    receipt = json.loads((tmp_path/'stage/阶段观测.json').read_text())
    assert receipt['exit_code'] == 7
    assert receipt['state'] == 'failed'
    assert receipt['admit_seconds'] is None
    # 同路径重试不能覆盖失败证据或假装跳过。
    with pytest.raises(FileExistsError):
        run_stage(['original-entry'], tmp_path/'stage', threads=2, max_rss_bytes=100,
                  min_free_bytes=100, disk_roots=[tmp_path], launch=launch)


def test_admit_failure_never_fabricates_admission(tmp_path):
    from data_pipeline.jobs.result_admission import admit_current
    # 故意模拟admit异常，不生成任何成功Admission替身。
    owner = SimpleNamespace(admit=Mock(side_effect=ValueError('原准入拒绝')), verify_current=Mock())
    with pytest.raises(ValueError, match='原准入拒绝'):
        admit_current(owner, object(), {}, tmp_path/'admit', lambda: None)
    owner.verify_current.assert_not_called()
    assert not (tmp_path/'admit/admission.json').exists()
    metrics = json.loads((tmp_path/'admit/准入观测.json').read_text())
    assert metrics['state'] == 'failed'
    assert metrics['admit_seconds'] >= 0
    assert metrics['current_seconds'] is None


@pytest.mark.parametrize("run_id,snapshot", [("1"*32,31),("2"*32,42)])
def test_request_fields_keep_receipt_identity_and_explicit_windows(tmp_path, monkeypatch,run_id,snapshot):
    from copy import deepcopy
    import data_pipeline.jobs.input_plan as module
    m, mapping = inputs()
    m['collector'] = 'fixture-collector'
    for entry in m['inputs']:
        entry.update(origin_uri='fixture://'+entry['source_id'], sha256='a'*64, path='/not-read/'+entry['source_id'])
    for row in mapping['consumers']['Feature']['selected_sources']:
        row['calculation_role'] = 'initial_rib' if row['source_id'] == '0' else 'update'
    mapping['windows'] = {
        'D_result': dict(start='2026-02-27T16:00:00Z', end_exclusive='2026-02-28T16:00:00Z'),
        'P_comparison': dict(start='2026-02-26T16:00:00Z', end_exclusive='2026-02-27T16:00:00Z'),
        'canonical_detection_computation': dict(start='2026-02-26T16:00:00Z', end_exclusive='2026-02-28T16:00:00Z')}
    fixed = FixedInputs(m, mapping)
    # 请求构造测试注入元数据，不生产checkpoint，不声称实际M2或Admission成功。
    seal = dict(run_id=run_id, snapshot=snapshot, checkpoints=[dict(source_id=s,
        counts=dict(messages=2, elements=3, references=4)) for s in [*fixed.entries, *fixed.references]])
    monkeypatch.setattr(module, 'sealed_receipt', lambda value, fixed: value)
    config = dict(threads=2, limits=dict(max_rss_bytes=1000, min_free_bytes=1000),
        observation_dsn='never-connect', output_root=str(tmp_path), csv_source_id=next(iter(fixed.references)),
        feature_sources={s:dict(window=dict(start='explicit-start',end='explicit-end',file_time='explicit-time',
            coverage='unknown'), message_quality_state='unknown', message_quality_refs=[]) for s in fixed.sources('Feature')},
        detection=dict(detection_dsn='never-connect-output', reference_version='explicit-version',
            scope=dict(run_id='logical-name',source='fixture',collector_id=m['collector'],
                window_start='2026-02-26T16:00:00Z', window_end='2026-02-28T16:00:00Z'),
            references=[dict(role='explicit-'+s,source_id=s) for s in fixed.references],
            boundaries={s:dict(file_id=s,source_version='original-template-label',
                observed_at='2026-02-27T17:00:00Z',legacy_tables={'event':'event_202602'}) for s in m['update_sources']}))
    before=deepcopy(config)
    result = build_requests(fixed, seal, config)
    assert result['feature']['source_views'][0]['run_id'] == seal['run_id']
    assert result['feature']['source_views'][0]['expected_elements'] == 3
    assert result['feature']['source_views'][0]['window']['coverage'] == 'unknown'
    assert result['feature']['reference_view']['expected_rows'] == 4
    assert result['canonical']['calculation_window']['window_start'] == '2026-02-26T16:00:00Z'
    assert result['detection']['result_window']['window_start'] == '2026-02-27T16:00:00Z'
    assert result['detection']['input_profile'] == 'observation'
    assert result['detection']['output_profile'] == 'detection-m3-lake/v2'
    assert result['detection']['scope']['input_version'] == f'{run_id}:{snapshot}'
    assert list(result['detection']['boundaries']) == m['update_sources']
    for sid,boundary in result['detection']['boundaries'].items():
        assert boundary['source_version'] == result['detection']['scope']['input_version']
        assert boundary == dict(before['detection']['boundaries'][sid],source_version=f'{run_id}:{snapshot}')
        assert boundary['file_id'] == sid
    assert config == before
    for problem in ('missing','wrong-key','wrong-file-id'):
        invalid=deepcopy(config);sid=m['update_sources'][0]
        if problem=='missing':del invalid['detection']['boundaries'][sid]
        elif problem=='wrong-key':invalid['detection']['boundaries']['wrong']=invalid['detection']['boundaries'].pop(sid)
        else:invalid['detection']['boundaries'][sid]['file_id']='wrong'
        with pytest.raises(ValueError,match='边界'):
            build_requests(fixed,seal,invalid)
    assert 'input_version' not in config['detection']['scope']
    config['detection']['input_run'] = 'forged'
    with pytest.raises(ValueError, match='不得覆盖'):
        build_requests(fixed, seal, config)


def test_rss_guard_stops_only_owned_process_group(tmp_path, monkeypatch):
    import data_pipeline.jobs.stage_runner as module
    process = Mock(pid=987654321)
    process.poll.return_value = None
    process.wait.return_value = -15
    cleanup = Mock(return_value=dict(state='group_exited',exit_code=-15))
    monkeypatch.setattr(module, 'stop_process_group', cleanup)
    with pytest.raises(RuntimeError, match='RSS'):
        run_stage(['not-actually-launched'], tmp_path/'stage', threads=2, max_rss_bytes=100,
            min_free_bytes=10, disk_roots=[tmp_path], launch=Mock(return_value=process),
            disk_usage=lambda p: SimpleNamespace(free=1000),
            sample=lambda pid: dict(parent_rss_bytes=70, children_rss_bytes=40))
    cleanup.assert_called_once_with(process, 5)
    receipt = json.loads((tmp_path/'stage/阶段观测.json').read_text())
    assert receipt['process_tree_sample_peak_rss_bytes'] == 110
    assert receipt['state'] == 'failed'


def test_disabled_full_driver_rejects_before_manifest_access(tmp_path):
    import subprocess
    import sys
    script = Path(__file__).resolve().parents[3]/'scripts/pipeline/migration-day-run.py'
    config = tmp_path/'disabled.json'
    config.write_text(json.dumps(dict(enabled=False, manifest=str(tmp_path/'missing'))))
    result = subprocess.run([sys.executable,str(script),'run-full','--config',str(config)],
        capture_output=True,text=True,timeout=5)
    assert result.returncode != 0 and '未显式启用' in result.stderr
    assert 'FileNotFoundError' not in result.stderr
    require_available(['Resource.admit'])  # 已合入被接受的双序修复。


def test_frozen_entry_shapes_match_original_public_signatures():
    # 导入只为签名核对；不构造Reader/Runtime或数据库连接。
    import inspect
    from data_pipeline.analysis.features.run import produce_bound_features
    from data_pipeline.bgp.replay.route_snapshot import produce_projection
    from data_pipeline.analysis.resources.observation import produce_observation_resources
    from data_pipeline.analysis.resources.references import register_reference
    inspect.signature(produce_bound_features).bind(object(), output='/not-created', max_rss_bytes=1)
    inspect.signature(produce_projection).bind(object(), output='/not-created',
        calculation_window={}, max_rss_bytes=1, min_free_bytes=1)
    inspect.signature(produce_observation_resources).bind(dsn='not-connected',sources=[],result_window=[],
        csv_binding={},country_binding={},output='/not-created',max_rss_bytes=1,min_free_bytes=1)
    inspect.signature(register_reference).bind(dsn='not-connected',path='/not-read',output='/not-created',
        origin_uri='fixture://metadata-only',content_sha256='a'*64,max_rss_bytes=1,min_free_bytes=1)


def test_checkpoint_ordinal_is_not_mrt_rank():
    from data_pipeline.jobs.input_plan import sealed_receipt
    from data_pipeline.bgp.archive.checkpoint import sha
    fixed = FixedInputs(*inputs())
    # 故意错误的元数据：把MRT放在reference之前；不调用生产器。
    value = dict(run_id='metadata-only', snapshot=1, qualification='observation_sealed',
        checkpoints=[dict(source_id=s, ordinal=i, ingest='complete', raw='verified_source_eof')
                     for i,s in enumerate([*fixed.entries, *fixed.references])])
    value['digest'] = sha(value)
    with pytest.raises(ValueError, match='固定来源原序'):
        sealed_receipt(value, fixed)


def test_sampling_failure_keeps_same_real_worker_alive(tmp_path):
    import os
    import sys
    from data_pipeline.jobs.stage_runner import process_sample
    calls = []
    def sample(pid):
        calls.append(pid)
        if len(calls) == 1:
            os.kill(pid, 0)
            raise OSError('受控的一次采样故障')
        return process_sample(pid)
    result = run_stage([sys.executable, '-c',
        'import os,time,json; time.sleep(0.4); print(json.dumps({"pid":os.getpid()}))'],
        tmp_path/'worker', threads=2, max_rss_bytes=256*1024**2, min_free_bytes=1,
        disk_roots=[tmp_path], sample=sample)
    assert result['state'] == 'process_exited_zero'
    assert result['sampling_failures'] == 1
    assert result['sample_count'] > 0
    assert result['rss_coverage'] == 'partial'
    assert len(set(calls)) == 1
    assert json.loads((tmp_path/'worker/stdout.log').read_text())['pid'] == calls[0] == result['pid']


def test_persistent_sampling_loss_still_checks_disk_and_exit(tmp_path):
    import sys
    failures = []
    disks = []
    def missing(pid):
        failures.append(pid)
        return dict(parent_rss_bytes=None, children_rss_bytes=0)
    def disk(path):
        disks.append(path)
        if len(disks) == 2:
            raise OSError('运行中一次磁盘采样故障')
        return SimpleNamespace(free=1000)
    result = run_stage([sys.executable, '-c', 'import time; time.sleep(0.25)'],
        tmp_path/'worker', threads=2, max_rss_bytes=256*1024**2, min_free_bytes=1,
        disk_roots=[tmp_path], sample=missing, disk_usage=disk)
    assert result['state'] == 'process_exited_zero'
    assert result['rss_coverage'] == 'unavailable'
    assert result['process_tree_sample_peak_rss_bytes'] is None
    assert result['sampling_failures'] == len(failures) >= 2
    assert result['disk_sampling_failures'] == 1
    assert len(disks) >= len(failures)+1
    assert len(set(failures)) == 1


def test_bounded_last_json_ignores_large_prior_log_and_rejects_large_receipt(tmp_path):
    from data_pipeline.jobs.migration import read_last_json
    path = tmp_path/'stdout.log'
    path.write_bytes(b'x'*1024**2 + b'\n{"actual": 17}\n')
    assert read_last_json(path, 32) == {'actual':17}
    path.write_bytes(b'log\n{"large":"'+b'x'*100+b'"}\n')
    with pytest.raises(ValueError, match='字节上限'):
        read_last_json(path, 32)


def artificial_metadata(tmp_path):
    import hashlib
    root = tmp_path/'inputs'
    entries = [dict(source_id=f'{i+100:064x}', sha256=f'{i+200:064x}', size=1,
        origin_uri=f'fixture://bounded-case/{i}', path=str(root/f'{i}.gz'),
        role=['baseline','snapshot','snapshot','update','update'][i]) for i in range(5)]
    manifest = dict(inputs=entries, references=[dict(sha256=f'{i+300:064x}',path=str(root/f'ref{i}.json')) for i in range(2)],
        baseline_source=entries[0]['source_id'],update_sources=[e['source_id'] for e in entries[3:]])
    def selected(ranks):
        return [dict(source_id=entries[i]['source_id'],selection_sequence_zero_based=n,
            joint_mrt_rank_zero_based=i,joint_role=entries[i]['role'],
            nominal_time_utc=f'2026-02-26T{n*8:02}:00:00Z') for n,i in enumerate(ranks)]
    mapping = dict(consumers={name:dict(selected_sources=selected([1,2,0] if name=='Resource' else [0,3,4]))
                             for name in ('Resource','Feature','canonical','Detection')})
    paths=[]
    for name,value in [('manifest',manifest),('mapping',mapping)]:
        path=tmp_path/(name+'.json');path.write_text(json.dumps(value));paths.append(path)
    profile=dict(profile='artificial/v1', description='仅元数据模式测试，未生成MRT或成功回执',
        source_root=str(root),counts=dict(mrt_sources=5,reference_sources=2,resource_sources=3,calculation_sources=3),
        manifest_sha256=hashlib.sha256(paths[0].read_bytes()).hexdigest(),mapping_sha256=hashlib.sha256(paths[1].read_bytes()).hexdigest())
    return paths, profile


def test_explicit_artificial_input_uses_strict_native_runtime_not_fixture_admission(tmp_path):
    from data_pipeline.jobs.result_admission import candidate_mode
    paths, profile = artificial_metadata(tmp_path)
    fixed = FixedInputs.load(*paths, input_config=profile)
    assert fixed.input_profile == 'artificial/v1'
    assert fixed.counts['mrt_sources'] == 5
    assert [r['joint_mrt_rank_zero_based'] for r in fixed.selection('Resource')] == [1,2,0]
    assert candidate_mode(fixed.input_profile) == dict(fixture_only=False,execution_profile='real-candidate/v1')
    with pytest.raises(ValueError, match='摘要'):
        FixedInputs.load(*paths)  # 不能用小输入冒充真实固定清单。
    profile['profile'] = 'rrc25-fixed/v1'
    with pytest.raises(ValueError, match='不接受'):
        FixedInputs.load(*paths, input_config=profile)


def test_artificial_input_refuses_undeclared_scale_and_real_uri(tmp_path):
    paths, profile = artificial_metadata(tmp_path)
    profile['counts']['mrt_sources'] = 588
    with pytest.raises(ValueError, match='小规模'):
        FixedInputs.load(*paths, input_config=profile)
    profile['counts']['mrt_sources'] = 5
    manifest = json.loads(paths[0].read_text());manifest['inputs'][0]['origin_uri'] = 'ssh://actual-source/rib'
    with pytest.raises(ValueError, match='fixture URI'):
        FixedInputs(manifest, json.loads(paths[1].read_text()), artificial=profile)


def test_cleanup_reaps_parent_and_terminates_term_ignoring_descendant(tmp_path):
    import os
    import signal
    import sys
    import time
    from data_pipeline.jobs.stage_runner import group_members
    pidfile = tmp_path/'child.pid'
    child = ('import os,signal,time;from pathlib import Path;'
        'signal.signal(signal.SIGTERM,signal.SIG_IGN);'
        f'Path({str(pidfile)!r}).write_text(str(os.getpid()));time.sleep(20)')
    parent = f'import subprocess,sys,time;subprocess.Popen([sys.executable,"-c",{child!r}]);time.sleep(20)'
    observed=[]
    def sample(pid):
        observed.append(pid)
        # 仅本有限测试的握手等待，非驱动处理超时策略。
        end=time.monotonic()+3
        while not pidfile.exists() and time.monotonic()<end:
            time.sleep(.01)
        assert pidfile.exists()
        return dict(parent_rss_bytes=2,children_rss_bytes=2)
    try:
        with pytest.raises(RuntimeError, match='RSS'):
            run_stage([sys.executable,'-c',parent], tmp_path/'stage', threads=2,max_rss_bytes=1,
                min_free_bytes=1,disk_roots=[tmp_path],sample=sample,termination_grace_seconds=.2)
        receipt=json.loads((tmp_path/'stage/阶段观测.json').read_text())
        assert receipt['cleanup']['state']=='group_exited'
        assert receipt['cleanup']['signals']==['SIGTERM','SIGKILL']
        assert not [p for p,state in group_members(observed[0]).items() if not state.startswith('Z')]
    finally:
        # 测试本身兜底清理，只针对本次实际观测到的自建组。
        if observed:
            try: os.killpg(observed[0],signal.SIGKILL)
            except ProcessLookupError: pass


def test_receipt_and_cleanup_errors_do_not_replace_primary_failure(tmp_path, monkeypatch):
    import data_pipeline.jobs.stage_runner as module
    process=Mock(pid=987654321)
    process.poll.return_value=7;process.wait.return_value=7
    monkeypatch.setattr(module,'stop_process_group',Mock(side_effect=OSError('清理观测故障')))
    monkeypatch.setattr(module,'save_new',Mock(side_effect=OSError('回执磁盘故障')))
    with pytest.raises(RuntimeError,match='原阶段入口失败') as caught:
        run_stage(['never-actually-started'],tmp_path/'stage',threads=2,max_rss_bytes=1,
            min_free_bytes=1,disk_roots=[tmp_path],launch=Mock(return_value=process))
    assert caught.value.migration_cleanup_errors[0]['message']=='清理观测故障'
    assert caught.value.migration_receipt_error['message']=='回执磁盘故障'


def test_admission_and_driver_final_writes_preserve_primary(tmp_path, monkeypatch):
    import data_pipeline.jobs.stage_runner as observation
    from data_pipeline.jobs.result_admission import admit_current
    from data_pipeline.jobs.migration import run_fixed
    monkeypatch.setattr(observation,'save_new',Mock(side_effect=OSError('ENOSPC')))
    owner = SimpleNamespace(admit=Mock(side_effect=ValueError('原admit错误')))
    with pytest.raises(ValueError,match='原admit错误') as caught:
        admit_current(owner,object(),{},tmp_path/'admit',lambda:None)
    assert caught.value.migration_cleanup_errors[0]['message']=='ENOSPC'
    fixed = SimpleNamespace(input_profile='artificial/v1',counts={},artificial={})
    with pytest.raises(ValueError,match='M2模式') as caught:
        run_fixed(fixed,{'enabled':True,'m2':{'mode':'invalid'}},tmp_path/'driver',lambda:None)
    assert caught.value.migration_cleanup_errors[0]['message']=='ENOSPC'
    with pytest.raises(OSError,match='ENOSPC'):
        observation.save_final(tmp_path/'successful-but-not-saved.json',{})
