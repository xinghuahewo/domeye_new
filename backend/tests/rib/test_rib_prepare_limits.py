"""公开离线 CLI 的时限配置；全部使用人工 MRT 与临时目录。"""
import json
import hashlib
import subprocess
import sys
import pytest

from tests.rib.test_rib_snapshots import CLI, command, prepare, source_fixture
from tests.rib.test_rib_batches import candidate


def test_prepare_accepts_explicit_two_hour_budget_without_changing_snapshot_identity(tmp_path):
    source = source_fixture(tmp_path)
    default = prepare(source, tmp_path / 'default')
    extended = prepare(source, tmp_path / 'extended', **{'max-seconds': 7200})
    assert default.returncode == 0, default.stderr
    assert extended.returncode == 0, extended.stderr
    assert json.loads(default.stdout)['version'] == json.loads(extended.stdout)['version']
    assert (tmp_path / 'default/manifest.json').read_bytes() == (tmp_path / 'extended/manifest.json').read_bytes()


def traced_alarm(tmp_path, arguments, injection=''):
    receipt = tmp_path / 'alarms.json'
    runner = '''import json,pathlib,runpy,signal,sys,time
alarms = []
original_alarm = signal.alarm
def alarm(seconds):
    alarms.append(seconds)
    return original_alarm(seconds)
signal.alarm = alarm
'''
    runner += injection + '\nsys.argv=' + repr([str(CLI), *map(str, arguments)]) + '\n'
    runner += 'try:\n    runpy.run_path(sys.argv[0],run_name="__main__")\nfinally:\n'
    runner += '    pathlib.Path(' + repr(str(receipt)) + ').write_text(json.dumps(alarms))\n'
    run = subprocess.run([sys.executable, '-c', runner], capture_output=True, text=True, timeout=10)
    return run, json.loads(receipt.read_text())


def prepare_arguments(source, output):
    return ['prepare', '--source', source, '--source-sha256', hashlib.sha256(source.read_bytes()).hexdigest(),
            '--collector', 'rrc25', '--observed-at', '2026-02-27T00:00:00Z', '--output', output]


def batch_arguments(tmp_path, name='batch'):
    manifest = tmp_path / (name + '.json')
    rows = [candidate(tmp_path / (name + '-first'), '2026-02-27T00:00:00Z'),
            candidate(tmp_path / (name + '-later'), '2026-02-27T08:00:00Z')]
    manifest.write_text(json.dumps({'schema_version': 'rib-batch-input/v1',
        'window_start': '2026-02-26T16:00:00Z', 'window_end_exclusive': '2026-02-27T16:00:00Z',
        'candidates': rows}))
    return ['batch', '--manifest', manifest, '--output', tmp_path / name, '--registry', tmp_path / 'registry']


@pytest.mark.parametrize('seconds', [None, 1, 1200, 1201, 7200])
def test_prepare_arms_the_requested_os_deadline_and_cancels_it_after_completion(tmp_path, seconds):
    source = source_fixture(tmp_path)
    args = prepare_arguments(source, tmp_path / 'candidate')
    if seconds is not None:
        args += ['--max-seconds', seconds]
    run, alarms = traced_alarm(tmp_path, args)
    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout)['state'] == 'prepared'
    assert alarms == [1200 if seconds is None else seconds, 0]


@pytest.mark.parametrize('seconds', [-1, 7201, '1.5', 'invalid'])
def test_prepare_rejects_out_of_range_or_noninteger_budgets_before_writing(tmp_path, seconds):
    source = source_fixture(tmp_path)
    output = tmp_path / 'rejected'
    run = prepare(source, output, **{'max-seconds': seconds})
    assert run.returncode != 0
    assert '时长须在0—7200秒' in run.stderr or 'invalid int value' in run.stderr
    assert not output.exists()


def test_prepare_real_timeout_interrupts_source_io_without_a_complete_candidate(tmp_path):
    source = source_fixture(tmp_path)
    output, completed = tmp_path / 'timed-out', tmp_path / 'io-completed'
    injection = f'''
original_open = pathlib.Path.open
def opened(path, *args, **kwargs):
    if str(path) == {str(source)!r}:
        time.sleep(3)
        pathlib.Path({str(completed)!r}).touch()
    return original_open(path, *args, **kwargs)
pathlib.Path.open = opened
'''
    before = source.read_bytes()
    run, alarms = traced_alarm(tmp_path, prepare_arguments(source, output) + ['--max-seconds', 1], injection)
    assert run.returncode == 1
    assert '离线处理超过时长资源上限' in run.stderr
    assert alarms == [1, 0]  # 实际SIGALRM终止I/O等待，不以机器速度阈值判定。
    assert not completed.exists()
    assert not (output / 'manifest.json').exists()
    assert source.read_bytes() == before


def test_prepare_zero_disables_deadline_and_completes_delayed_source_io(tmp_path):
    source = source_fixture(tmp_path)
    output, completed = tmp_path / 'unlimited', tmp_path / 'io-completed'
    default = tmp_path / 'default'
    assert prepare(source, default).returncode == 0
    injection = f'''
original_open = pathlib.Path.open
delayed = False
def opened(path, *args, **kwargs):
    global delayed
    if str(path) == {str(source)!r} and not delayed:
        delayed = True
        time.sleep(3)
        pathlib.Path({str(completed)!r}).touch()
    return original_open(path, *args, **kwargs)
pathlib.Path.open = opened
'''
    before = source.read_bytes()
    run, alarms = traced_alarm(tmp_path, prepare_arguments(source, output) + ['--max-seconds', 0], injection)
    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout)['state'] == 'prepared'
    assert alarms == [0, 0]
    assert completed.exists()
    assert source.read_bytes() == before
    assert (output / 'manifest.json').read_bytes() == (default / 'manifest.json').read_bytes()


def test_prepare_zero_keeps_projection_disk_limit_and_never_completes_partial_candidate(tmp_path):
    source = source_fixture(tmp_path, extra_prefixes=20000)
    output = tmp_path / 'over-disk-limit'
    before = source.read_bytes()
    run = prepare(source, output, **{'max-seconds': 0, 'max-database-mib': 1})
    assert run.returncode == 1
    assert not (output / 'manifest.json').exists()
    assert source.read_bytes() == before


@pytest.mark.parametrize('seconds', [0, 7200])
def test_register_keeps_fixed_default_and_rejects_a_time_budget_override(tmp_path, seconds):
    source = source_fixture(tmp_path)
    output, registry = tmp_path / 'candidate', tmp_path / 'registry'
    assert prepare(source, output).returncode == 0
    args = ['register', '--candidate', output, '--registry', registry]
    rejected = command(*args, '--max-seconds', seconds)
    assert rejected.returncode == 2 and 'unrecognized arguments' in rejected.stderr
    assert not registry.exists()
    run, alarms = traced_alarm(tmp_path, args)
    assert run.returncode == 0, run.stderr
    assert alarms == [1200, 0]


@pytest.mark.parametrize('seconds', [-1, 1201, 7200, '1.5', 'invalid'])
def test_batch_rejects_invalid_time_budgets_before_writing(tmp_path, seconds):
    output, registry = tmp_path / 'batch', tmp_path / 'registry'
    run = command('batch', '--manifest', tmp_path / 'unused.json', '--output', output,
                  '--registry', registry, '--max-seconds', seconds)
    assert run.returncode != 0
    assert '时长须在0—1200秒' in run.stderr or 'invalid int value' in run.stderr
    assert not output.exists() and not registry.exists()


def test_batch_zero_cancels_deadline_through_later_source_and_registration_wait(tmp_path):
    args = batch_arguments(tmp_path)
    completed = tmp_path / 'registration-wait-completed'
    injection = f'''
import fcntl
original_open, original_flock = pathlib.Path.open, fcntl.flock
delayed = False
def opened(path, *args, **kwargs):
    global delayed
    if str(path) == {str(tmp_path / 'batch-later/fixture.gz')!r} and not delayed:
        delayed = True
        time.sleep(2)
    return original_open(path, *args, **kwargs)
def flocked(fd, operation):
    if operation == fcntl.LOCK_EX:
        time.sleep(2)
        pathlib.Path({str(completed)!r}).touch()
    return original_flock(fd, operation)
pathlib.Path.open, fcntl.flock = opened, flocked
original_alarm(1)  # CLI必须原生取消已存在的闹钟。
'''
    run, alarms = traced_alarm(tmp_path, args + ['--max-seconds', 0], injection)
    assert run.returncode == 0, run.stderr
    report = json.loads(run.stdout)
    assert report['state'] == 'committed' and report['coverage'] == 'complete'
    assert [row['state'] for row in report['days'][0]['candidates']] == ['verified', 'verified']
    assert alarms == [0, 0]
    assert completed.exists()


@pytest.mark.parametrize('seconds', [None, 1, 1200])
def test_batch_keeps_requested_deadline_and_disk_defaults(tmp_path, seconds):
    args = batch_arguments(tmp_path)
    if seconds is not None:
        args += ['--max-seconds', seconds]
    run, alarms = traced_alarm(tmp_path, args)
    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout)['state'] == 'committed'
    assert alarms == [1200 if seconds is None else seconds, 0]
    execution = json.loads((tmp_path / 'batch/execution.json').read_bytes())
    assert execution['max_database_mib'] == 4096 and execution['max_batch_mib'] == 8192


@pytest.mark.parametrize('seconds', [None, 1])
def test_batch_default_and_positive_deadlines_really_interrupt_registration_wait(tmp_path, seconds):
    args = batch_arguments(tmp_path)
    if seconds is not None:
        args += ['--max-seconds', seconds]
    completed = tmp_path / 'wait-completed'
    injection = f'''
import fcntl
native_alarm = original_alarm
def accelerated_alarm(seconds):
    if seconds == 1200:
        signal.setitimer(signal.ITIMER_REAL, 0.5)  # 默认值已记录；缩短OS计时，仍交付真实SIGALRM。
        return 0
    return native_alarm(seconds)
original_alarm = accelerated_alarm
original_flock = fcntl.flock
def flocked(fd, operation):
    if operation == fcntl.LOCK_EX:
        time.sleep(3)
        pathlib.Path({str(completed)!r}).touch()
    return original_flock(fd, operation)
fcntl.flock = flocked
'''
    run, alarms = traced_alarm(tmp_path, args, injection)
    assert run.returncode == 1 and '离线处理超过时长资源上限' in run.stderr
    assert alarms == [1200 if seconds is None else seconds, 0]
    assert not completed.exists() and not (tmp_path / 'registry/registry.json').exists()
    assert json.loads((tmp_path / 'batch/failure.json').read_bytes())['state'] == 'aborted'
