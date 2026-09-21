"""尾段真实RSS观测；仅当前进程、人工子进程和临时目录。"""
import json
import os
import time
from data_pipeline.jobs.downstream import measured


def test_stage_has_own_samples(tmp_path):
    with measured(tmp_path/'country', lambda: None):
        payload = bytearray(32 * 1024 * 1024)
        time.sleep(.25)
        assert payload[0] == 0
    result = json.loads((tmp_path/'country/阶段观测.json').read_text())
    assert result['stage_process_group_peak_rss_bytes'] is not None
    assert result['driver_pid'] == os.getpid()
    assert result['started_at'] <= result['ended_at']


def test_real_child_and_same_sample_peak(tmp_path, monkeypatch):
    import subprocess
    import sys
    import threading
    import data_pipeline.jobs.downstream as tail
    original = tail.process_sample
    seen = threading.Event()
    child = subprocess.Popen([sys.executable, '-c',
        "import sys; data=bytearray(24*1024*1024); print('ready',flush=True); sys.stdin.read()"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == 'ready'
        def sample(pid, **kwargs):
            value = original(pid, **kwargs)
            if child.pid in value['observed_pids']:
                seen.set()
            return value
        monkeypatch.setattr(tail, 'process_sample', sample)
        with measured(tmp_path/'trend', lambda: None):
            memory = bytearray(32*1024*1024)
            assert seen.wait(5)
            time.sleep(.2)
            assert len(memory) > 0
        value = json.loads((tmp_path/'trend/阶段观测.json').read_text())
        peak = value['peak_samples']['stage_process_group_peak_rss_bytes']
        assert value['stage_process_group_peak_rss_bytes'] == peak['parent_rss_bytes'] + peak['children_rss_bytes']
        assert value['children_sample_peak_rss_bytes'] >= 24*1024*1024
        assert child.pid in peak['observed_pids']
        assert value['started_at'] <= peak['at'] <= value['ended_at']
        assert value['pg_sample_peak_rss_bytes'] is None
    finally:
        child.communicate(timeout=5)


def test_sampling_failure_keeps_unknown_and_primary(tmp_path, monkeypatch):
    import pytest
    import data_pipeline.jobs.downstream as tail
    import threading
    attempted = threading.Event()
    def unavailable(pid, **kwargs):
        attempted.set()
        raise OSError('人工采样失败')
    monkeypatch.setattr(tail, 'process_sample', unavailable)
    primary = ValueError('原业务异常')
    with pytest.raises(ValueError) as caught:
        with measured(tmp_path/'failed', lambda: None):
            assert attempted.wait(2)
            raise primary
    assert caught.value is primary
    value = json.loads((tmp_path/'failed/阶段观测.json').read_text())
    assert value['state'] == 'failed'
    assert value['sample_count'] == 0 and value['sampling_failures'] >= 1
    assert value['rss_coverage'] == 'unavailable'
    assert value['stage_process_group_peak_rss_bytes'] is None
    assert value['started_at'] <= value['ended_at']


def test_partial_sampling_is_explicit(tmp_path, monkeypatch):
    import threading
    import data_pipeline.jobs.downstream as tail
    original = tail.process_sample
    sampled = threading.Event()
    calls = []
    def sample(pid, **kwargs):
        calls.append(pid)
        if len(calls) == 1:
            raise OSError('人工首样本失败')
        value = original(pid, **kwargs)
        sampled.set()
        return value
    monkeypatch.setattr(tail, 'process_sample', sample)
    with measured(tmp_path/'partial', lambda: None):
        assert sampled.wait(5)
    value = json.loads((tmp_path/'partial/阶段观测.json').read_text())
    assert value['state'] == 'completed' and value['rss_coverage'] == 'partial'
    assert value['sample_count'] >= 1 and value['sampling_failures'] == 1


def test_monitor_start_failure_does_not_skip_business(tmp_path, monkeypatch):
    import data_pipeline.jobs.downstream as tail
    def fail_start(self):
        raise RuntimeError('cannot start new thread')
    monkeypatch.setattr(tail.threading.Thread, 'start', fail_start)
    entered = []
    with measured(tmp_path/'start-failed', lambda: None):
        entered.append(True)
    value = json.loads((tmp_path/'start-failed/阶段观测.json').read_text())
    assert entered and value['state'] == 'completed'
    assert value['rss_coverage'] == 'unavailable' and value['sample_count'] == 0


def test_blocked_sampler_cannot_delay_forever_or_mutate_receipt(tmp_path, monkeypatch):
    import threading
    import pytest
    import data_pipeline.jobs.downstream as tail
    entered, release, exited = threading.Event(), threading.Event(), threading.Event()
    def blocked(pid, **kwargs):
        entered.set()
        try:
            assert release.wait(5)
            return dict(parent_rss_bytes=1, children_rss_bytes=0, observed_pids=[pid])
        finally:
            exited.set()
    monkeypatch.setattr(tail, 'process_sample', blocked)
    primary = ValueError('原错')
    started = time.monotonic()
    try:
        with pytest.raises(ValueError) as caught:
            with measured(tmp_path/'blocked', lambda: None) as value:
                assert entered.wait(2)
                raise primary
        assert caught.value is primary and time.monotonic()-started < 3
        path = tmp_path/'blocked/阶段观测.json'
        before = path.read_bytes()
        snapshot = json.dumps(value, sort_keys=True)
        release.set()
        assert exited.wait(2)
        # 等受控监控线程退出，核落盘及同一结果对象均不再变化。
        for worker in threading.enumerate():
            if worker.name == 'migration-stage-rss':
                worker.join(timeout=2)
        assert before == path.read_bytes() and snapshot == json.dumps(value, sort_keys=True)
        assert value['last_sample_error']['type'] == 'TimeoutError'
    finally:
        release.set()


def test_real_observation_command_timeout_reaps_child(tmp_path, monkeypatch):
    import subprocess
    import sys
    import pytest
    import data_pipeline.jobs.stage_runner as observation
    original = subprocess.run
    pidfile = tmp_path/'pid'
    def sleeping_ps(argv, **kwargs):
        return original([sys.executable, '-c',
            'import os,time; from pathlib import Path; Path('+repr(str(pidfile))+').write_text(str(os.getpid())); time.sleep(5)'],
            **kwargs)
    monkeypatch.setattr(observation.subprocess, 'run', sleeping_ps)
    with pytest.raises(subprocess.TimeoutExpired):
        observation.process_sample(os.getpid(), timeout=.2)
    pid = int(pidfile.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
