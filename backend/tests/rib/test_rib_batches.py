"""固定清单批次验收：人工原始 MRT → 同一离线 CLI → 只读 HTTP。"""
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil
import gzip
import pytest
import subprocess
import sys
import struct
from tests.rib.test_rib_snapshots import CLI, ROOT

from tests.rib.test_rib_snapshots import command, source_fixture


def candidate(directory, stamp, extra=0):
    directory.mkdir(parents=True)
    source = source_fixture(directory, extra_prefixes=extra,
                            stamp=int(datetime.fromisoformat(stamp.replace('Z', '+00:00')).timestamp()))
    return {'path': str(source), 'sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
            'collector': 'rrc25', 'observed_at': stamp}


def daily_candidates(directory, start, days):
    first = datetime.fromisoformat(start.replace('Z', '+00:00'))
    return [candidate(directory / f'{day}-{hour}',
                      (first + timedelta(days=day, hours=hour)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                      extra=int(hour == 0))
            for day in range(days) for hour in (0, 8, 16)]


def batch(tmp_path, candidates, start, end, registry, name='run', injection=None, cli=CLI, **limits):
    manifest = tmp_path / (name + '.json')
    manifest.write_text(json.dumps({'schema_version': 'rib-batch-input/v1',
        'window_start': start, 'window_end_exclusive': end, 'candidates': candidates}))
    arguments = [str(cli), 'batch', '--manifest', str(manifest), '--registry', str(registry), '--output', str(tmp_path / name),
                 *(str(value) for key, value in limits.items() for value in ('--' + key.replace('_', '-'), value))]
    if injection:
        return subprocess.run([sys.executable, '-c', 'import runpy,sys\n' + injection + '\nsys.argv=' + repr(arguments)
                              + '\nrunpy.run_path(sys.argv[0],run_name="__main__")'], capture_output=True, text=True, timeout=20)
    return subprocess.run([sys.executable, *arguments], capture_output=True, text=True, timeout=20)


def test_two_cross_month_batches_use_one_entry_and_actual_business_days(tmp_path, monkeypatch, client):
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    first = daily_candidates(tmp_path / 'first-inputs', '2026-02-26T16:00:00Z', 2)
    run = batch(tmp_path, list(reversed(first)), '2026-02-26T16:00:00Z', '2026-02-28T16:00:00Z', registry,
                max_batch_mib=25600, max_seconds=0)
    assert run.returncode == 0, run.stderr
    first_report = json.loads(run.stdout)
    assert [day['date'] for day in first_report['days']] == ['2026-02-27', '2026-02-28']
    second = daily_candidates(tmp_path / 'second-inputs', '2026-02-28T16:00:00Z', 10)
    run = batch(tmp_path, list(reversed(second)), '2026-02-28T16:00:00Z', '2026-03-10T16:00:00Z', registry, 'second',
                max_batch_mib=91136, max_seconds=0)
    assert run.returncode == 0, run.stderr
    report = json.loads(run.stdout)
    import jsonschema
    jsonschema.validate(report, json.loads((ROOT / 'contracts/data/rib-batch-report.schema.json').read_bytes()))
    jsonschema.validate(json.loads((tmp_path / 'second.json').read_bytes()),
                        json.loads((ROOT / 'contracts/data/rib-batch-input.schema.json').read_bytes()))
    assert report['state'] == 'committed' and report['coverage'] == 'complete'
    assert len(report['days']) == 10
    assert report['days'][0]['date'] == '2026-03-01' and report['days'][-1]['date'] == '2026-03-10'
    for result, rows, name in [(first_report, first, 'run'), (report, second, 'second')]:
        assert all([row['state'] for row in day['candidates']] == ['verified'] * 3 for day in result['days'])
        assert all(day['version'] == day['candidates'][0]['version'] for day in result['days'])
        attempts = json.loads((tmp_path / name / 'execution.json').read_bytes())['attempts']
        assert [row['sha256'] for row in attempts] == [row['sha256'] for row in rows]
    found = client.get('/api/v1/rib-snapshots').json
    assert len(found['snapshots']) == 12
    for row in found['snapshots']:
        base = '/api/v1/rib-snapshots/' + row['version']
        assert client.get(base).json['metrics']['visible_prefixes'] == 6
        assert client.get(base + '/asns/13335').json['prefix_count'] == 3
        assert client.get(base + '/observations').json['total'] == 10


def test_aliases_overlap_and_retry_keep_identities_without_rolling_back_later_selection(tmp_path, monkeypatch, client):
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    old = candidate(tmp_path / 'old', '2026-02-27T00:00:00Z')
    later = candidate(tmp_path / 'later', '2026-02-27T08:00:00Z', 1)
    window = ('2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z')
    first = batch(tmp_path, [old], *window, registry, 'first')
    assert first.returncode == 0, first.stderr
    original = json.loads(first.stdout)
    alias = tmp_path / 'alias.gz'
    shutil.copyfile(old['path'], alias)
    repeat = batch(tmp_path, [{**old, 'path': str(alias)}, old, old], *window, registry, 'aliases')
    assert repeat.returncode == 0, repeat.stderr
    assert json.loads(repeat.stdout) == original
    changed = batch(tmp_path, [later], *window, registry, 'changed')
    assert changed.returncode == 0, changed.stderr
    latest = json.loads(changed.stdout)['days'][0]['version']
    assert latest != original['days'][0]['version']
    repeat = batch(tmp_path, [old], *window, registry, 'retry')
    assert repeat.returncode == 0, repeat.stderr
    assert client.get('/api/v1/rib-snapshots?date=2026-02-27').json['snapshots'][0]['version'] == latest
    # 新候选集合明确选择最早合格源，可以选回旧结果；结果身份独立于批次选择身份。
    selected_back = batch(tmp_path, [later, old], *window, registry, 'select-back')
    assert selected_back.returncode == 0, selected_back.stderr
    back = json.loads(selected_back.stdout)
    assert back['selection_id'] != original['selection_id']
    assert back['days'][0]['version'] == original['days'][0]['version']
    assert client.get('/api/v1/rib-snapshots?date=2026-02-27').json['snapshots'][0]['version'] == back['days'][0]['version']
    overlapping = batch(tmp_path, [old], window[0], '2026-02-28T16:00:00Z', registry, 'overlap')
    assert overlapping.returncode == 0, overlapping.stderr
    assert json.loads(overlapping.stdout)['days'][0]['version'] == back['days'][0]['version']
    assert client.get('/api/v1/rib-snapshots/' + back['days'][0]['version'] + '/observations').json['total'] == 9


def test_confirmed_bad_inputs_are_recorded_and_each_day_has_a_result(tmp_path, monkeypatch, client):
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    bad = candidate(tmp_path / 'bad', '2026-02-26T16:00:00Z')
    Path(bad['path']).write_bytes(b'bad gzip')
    bad['sha256'] = hashlib.sha256(b'bad gzip').hexdigest()
    valid = candidate(tmp_path / 'valid', '2026-02-27T08:00:00Z')
    broken = candidate(tmp_path / 'broken', '2026-02-28T00:00:00Z')
    Path(broken['path']).write_bytes(gzip.compress(b'bad mrt', mtime=0))
    broken['sha256'] = hashlib.sha256(Path(broken['path']).read_bytes()).hexdigest()
    missing = {**valid, 'path': str(tmp_path / 'missing.gz'), 'observed_at': '2026-03-02T00:00:00Z', 'sha256': '0' * 64}
    run = batch(tmp_path, [valid, missing, broken, bad], '2026-02-26T16:00:00Z', '2026-03-02T16:00:00Z', registry)
    assert run.returncode == 0, run.stderr
    report = json.loads(run.stdout)
    assert report['coverage'] == 'gaps'
    assert [day['state'] for day in report['days']] == ['available', 'validation_failed', 'missing_input', 'missing_input']
    assert report['days'][0]['candidates'][0]['rejection'] == {
        'code': 'invalid_gzip', 'stage': 'gzip', 'message': "Not a gzipped file (b'ba')"}
    assert report['days'][1]['candidates'][0]['rejection']['code'] == 'invalid_mrt'
    assert report['days'][3]['candidates'][0]['rejection']['code'] == 'missing_input'
    assert len(client.get('/api/v1/rib-snapshots').json['snapshots']) == 1


def test_http_distinguishes_latest_batch_gaps_from_published_defaults_and_uncalculated_days(tmp_path, monkeypatch, client):
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    good = candidate(tmp_path / 'good', '2026-02-27T00:00:00Z')
    first = batch(tmp_path, [good], '2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z', registry, 'first')
    assert first.returncode == 0, first.stderr
    version = json.loads(first.stdout)['days'][0]['version']
    broken = candidate(tmp_path / 'broken', '2026-03-01T00:00:00Z')
    broken['sha256'] = 'f' * 64
    run = batch(tmp_path, [broken], '2026-02-26T16:00:00Z', '2026-03-01T16:00:00Z', registry)
    assert run.returncode == 0, run.stderr
    report = json.loads(run.stdout)
    previous = client.get('/api/v1/rib-snapshots?date=2026-02-27').json
    assert previous['snapshots'][0]['version'] == version
    assert previous['days'] == [{'date': '2026-02-27', 'state': 'available', 'version': version,
                                 'last_batch': {'selection_id': report['selection_id'], 'state': 'missing_input'}}]
    for date, state in [('2026-02-28', 'missing_input'), ('2026-03-01', 'validation_failed'), ('2026-03-02', 'not_calculated')]:
        found = client.get('/api/v1/rib-snapshots?date=' + date)
        assert found.status_code == 200
        assert found.json['days'][0]['state'] == state and found.json['snapshots'] == []
    assert client.get('/api/v1/rib-snapshots/' + version + '/asns/174').json['prefix_count'] == 0
    (registry / 'versions' / version / 'snapshot.sqlite').write_bytes(b'broken')
    failure = client.get('/api/v1/rib-snapshots/' + version)
    assert failure.status_code == 503 and failure.json['state'] == 'validation_failed'
    assert client.get('/api/v1/rib-snapshots/rib_snapshot_v1_' + 'a' * 64).json['state'] == 'unknown_version'


@pytest.mark.parametrize('conflict', ['sha_time', 'sha_collector', 'actual_time', 'actual_collector'])
def test_declaration_conflict_rejects_entire_batch_even_after_good_candidate(conflict, tmp_path):
    good = candidate(tmp_path / 'good', '2026-02-27T00:00:00Z')
    other = candidate(tmp_path / 'other', '2026-02-28T00:00:00Z')
    if conflict == 'sha_time':
        other = {**good, 'observed_at': '2026-02-28T00:00:00Z'}
    elif conflict == 'sha_collector':
        other = {**good, 'collector': 'rrc00'}
    elif conflict == 'actual_time':
        other['observed_at'] = '2026-02-28T01:00:00Z'
    else:
        raw = gzip.decompress(Path(other['path']).read_bytes()).replace(b'rrc25', b'rrc00')
        Path(other['path']).write_bytes(gzip.compress(raw, mtime=0))
        other['sha256'] = hashlib.sha256(Path(other['path']).read_bytes()).hexdigest()
    registry = tmp_path / 'published'
    run = batch(tmp_path, [good, other], '2026-02-26T16:00:00Z', '2026-02-28T16:00:00Z', registry)
    assert run.returncode == 1
    assert not (registry / 'registry.json').exists()


@pytest.mark.parametrize('failure', ['PermissionError', 'OSError', 'ValueError', 'RuntimeError', 'TimeoutError', 'sqlite3.OperationalError'])
def test_second_day_execution_errors_preserve_all_previous_defaults(failure, tmp_path, monkeypatch, client):
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    old = candidate(tmp_path / 'old', '2026-02-27T00:00:00Z')
    window = ('2026-02-26T16:00:00Z', '2026-02-28T16:00:00Z')
    assert batch(tmp_path, [old], *window, registry, 'old-run').returncode == 0
    before = (registry / 'registry.json').read_bytes()
    first = candidate(tmp_path / 'new', '2026-02-27T01:00:00Z', 1)
    second = candidate(tmp_path / 'second', '2026-02-28T00:00:00Z')
    fallback = candidate(tmp_path / 'fallback', '2026-02-28T08:00:00Z')
    injected = f'''import pathlib,sqlite3
original_open = pathlib.Path.open
def opened(path, mode='r', *args, **kwargs):
    if str(path) == {second['path']!r} and mode == 'rb':
        raise {failure}('injected fatal failure')
    return original_open(path, mode, *args, **kwargs)
pathlib.Path.open = opened
'''
    run = batch(tmp_path, [first, second, fallback], *window, registry, injection=injected, max_seconds=0)
    assert run.returncode == 1
    assert 'injected fatal failure' in run.stderr
    assert (registry / 'registry.json').read_bytes() == before
    assert len(client.get('/api/v1/rib-snapshots').json['snapshots']) == 1
    assert not (tmp_path / 'run' / (fallback['sha256'] + '-0')).exists()
    report = json.loads((tmp_path / 'run/failure.json').read_bytes())
    assert report['state'] == 'aborted' and len(report['days']) == 2
    assert report['coverage'] == 'unknown'


@pytest.mark.parametrize('failure', ['copy', 'replace', 'input_changed_during_registration'])
def test_registration_failure_cannot_expose_part_of_a_batch(failure, tmp_path, monkeypatch, client):
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    old = candidate(tmp_path / 'old', '2026-02-27T00:00:00Z')
    window = ('2026-02-26T16:00:00Z', '2026-02-28T16:00:00Z')
    assert batch(tmp_path, [old], *window, registry, 'old-run').returncode == 0
    before = (registry / 'registry.json').read_bytes()
    old_url = '/api/v1/rib-snapshots/' + client.get('/api/v1/rib-snapshots').json['snapshots'][0]['version']
    reference = client.get(old_url + '/asns/13335').json
    first = candidate(tmp_path / 'new', '2026-02-27T01:00:00Z', 1)
    second = candidate(tmp_path / 'second', '2026-02-28T00:00:00Z')
    injected = f'''import os,shutil,pathlib
original_copy, original_replace = shutil.copyfile, os.replace
def copied(source, destination, *args, **kwargs):
    if {failure!r} == 'copy': raise OSError('injected copy failure')
    value = original_copy(source, destination, *args, **kwargs)
    if {failure!r} == 'input_changed_during_registration':
        pathlib.Path({first['path']!r}).write_bytes(b'changed while installing versions')
    return value
def replaced(source, destination):
    if str(destination).endswith('/registry.json') and {failure!r} == 'replace':
        raise OSError('injected index interruption')
    return original_replace(source, destination)
shutil.copyfile, os.replace = copied, replaced
'''
    run = batch(tmp_path, [first, second], *window, registry, injection=injected, max_seconds=0)
    assert run.returncode == 1
    assert (registry / 'registry.json').read_bytes() == before
    assert client.get(old_url + '/asns/13335').json == reference
    assert len(client.get('/api/v1/rib-snapshots').json['snapshots']) == 1


def test_alias_rejection_is_source_level_and_independent_of_path_order(tmp_path):
    wrong = candidate(tmp_path / 'wrong', '2026-02-27T00:00:00Z')
    wrong['sha256'] = 'f' * 64
    alias = {**wrong, 'path': str(tmp_path / 'z-missing.gz')}
    window = ('2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z')
    first = batch(tmp_path, [wrong, alias], *window, tmp_path / 'published', 'first')
    assert first.returncode == 0, first.stderr
    result = json.loads(first.stdout)
    assert result['days'][0]['state'] == 'validation_failed'
    moved = tmp_path / 'a-moved.gz'
    shutil.move(wrong['path'], moved)
    retry = batch(tmp_path, [{**wrong, 'path': str(moved)}, {**alias, 'path': str(tmp_path / '0-missing.gz')}],
                  *window, tmp_path / 'published', 'retry')
    assert retry.returncode == 0, retry.stderr
    assert json.loads(retry.stdout) == result
    attempts = json.loads((tmp_path / 'first/execution.json').read_bytes())['attempts']
    assert {row['rejection']['code'] for row in attempts} == {'sha256_mismatch', 'missing_input'}


def test_missing_candidate_appearing_during_execution_aborts_instead_of_publishing_later_source(tmp_path):
    first = candidate(tmp_path / 'first', '2026-02-27T00:00:00Z')
    backup = tmp_path / 'backup.gz'
    shutil.move(first['path'], backup)
    later = candidate(tmp_path / 'later', '2026-02-27T08:00:00Z')
    injected = f'''import pathlib
original_open = pathlib.Path.open
changed = False
def opened(path, mode='r', *args, **kwargs):
    global changed
    if str(path) == {later['path']!r} and mode == 'rb' and not changed:
        changed = True
        with original_open(pathlib.Path({str(backup)!r}), 'rb') as reader:
            with original_open(pathlib.Path({first['path']!r}), 'wb') as writer: writer.write(reader.read())
    return original_open(path, mode, *args, **kwargs)
pathlib.Path.open = opened
'''
    registry = tmp_path / 'published'
    run = batch(tmp_path, [first, later], '2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z', registry, injection=injected)
    assert run.returncode == 1
    assert not (registry / 'registry.json').exists()


def isolated_tool(tmp_path, settings):
    isolated = tmp_path / 'tool'
    for source in [CLI, ROOT / 'backend/data_pipeline/__init__.py', *list((ROOT / 'backend/data_pipeline').rglob('*.py'))]:
        destination = isolated / source.relative_to(ROOT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    (isolated / 'config').mkdir()
    (isolated / 'config/data-profile.json').write_text(json.dumps(settings))
    return isolated


def test_isolated_profile_parameterizes_window_and_keeps_half_open_snapshot_guards(tmp_path):
    settings = {'schema_version': 1, 'id': 'isolated-june', 'mode': 'fixed', 'timezone': 'Asia/Shanghai',
        'window_start': '2026-05-31T16:00:00Z', 'window_end_exclusive': '2026-06-02T16:00:00Z',
        'snapshot_time': '2026-06-02T15:59:59Z', 'api_profile': 'core'}
    isolated = isolated_tool(tmp_path, settings)
    source = candidate(tmp_path / 'june', '2026-05-31T16:00:00Z')
    registry = tmp_path / 'published'
    run = batch(tmp_path, [source], '2026-05-31T16:00:00Z', '2026-06-02T16:00:00Z', registry, cli=isolated / 'scripts/rib/rib-snapshot.py')
    assert run.returncode == 0, run.stderr
    assert [row['date'] for row in json.loads(run.stdout)['days']] == ['2026-06-01', '2026-06-02']
    # HTTP复用当前应用，仅从隔离树加载本项目的数据档和快照模块。
    script = f'''import sys,os,json
sys.path[:0] = [{str(isolated / 'backend')!r}, {str(ROOT / 'backend')!r}]
os.environ['DOMEYE_RIB_SNAPSHOT_REGISTRY'] = {str(registry)!r}
from run import create_app
client = create_app('testing').test_client()
responses = [client.get('/api/v1/rib-snapshots?date=' + day) for day in ['2026-06-01','2026-05-31','2026-06-03']]
print(json.dumps([{{'status': value.status_code, 'body': value.json}} for value in responses]))
'''
    response = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True, timeout=20)
    assert response.returncode == 0, response.stderr
    data = json.loads(response.stdout)
    assert [row['status'] for row in data] == [200, 400, 400]
    assert data[0]['body']['snapshots'][0]['date'] == '2026-06-01'
    end_source = candidate(tmp_path / 'end', '2026-06-02T16:00:00Z')
    rejected = batch(tmp_path, [end_source], '2026-05-31T16:00:00Z', '2026-06-02T16:00:00Z', registry, 'outside', cli=isolated / 'scripts/rib/rib-snapshot.py')
    assert rejected.returncode == 1
    settings['snapshot_time'] = '2026-06-02T00:00:00Z'
    (isolated / 'config/data-profile.json').write_text(json.dumps(settings))
    rejected = batch(tmp_path, [source], '2026-05-31T16:00:00Z', '2026-06-02T16:00:00Z', registry, 'snapshot-cap', cli=isolated / 'scripts/rib/rib-snapshot.py')
    assert rejected.returncode == 1


@pytest.mark.parametrize('limit', [{'max_batch_mib': 1}, {'max_batch_mib': 91137},
    {'max_database_mib': 0}, {'max_database_mib': 4097}])
def test_resource_limits_reject_instead_of_automatically_expanding(limit, tmp_path):
    registry = tmp_path / 'published'
    run = batch(tmp_path, [], '2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z', registry, **limit)
    assert run.returncode == 1
    assert not registry.exists() and not (tmp_path / 'run').exists()


@pytest.mark.parametrize('days, budget', [(2, 25600), (10, 91136)])
def test_fatal_later_candidate_after_final_day_selection_aborts_the_full_batch(days, budget, tmp_path, monkeypatch, client):
    start = '2026-02-28T16:00:00Z'
    end = (datetime.fromisoformat(start.replace('Z', '+00:00')) + timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')
    rows = daily_candidates(tmp_path / 'inputs', start, days)
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    assert batch(tmp_path, [rows[1]], start, end, registry, 'old').returncode == 0
    before = (registry / 'registry.json').read_bytes()
    previous = client.get('/api/v1/rib-snapshots').json
    injected = f'''import pathlib
original_open = pathlib.Path.open
def opened(path, mode='r', *args, **kwargs):
    if str(path) == {rows[-1]['path']!r} and mode == 'rb':
        raise OSError('injected final unselected source failure')
    return original_open(path, mode, *args, **kwargs)
pathlib.Path.open = opened
'''
    run = batch(tmp_path, list(reversed(rows)), start, end, registry, injection=injected,
                max_seconds=0, max_batch_mib=budget)
    assert run.returncode == 1 and 'injected final unselected source failure' in run.stderr
    assert (registry / 'registry.json').read_bytes() == before
    assert client.get('/api/v1/rib-snapshots').json == previous
    failure = json.loads((tmp_path / 'run/failure.json').read_bytes())
    assert failure['state'] == 'aborted' and failure['coverage'] == 'unknown'
    assert len(failure['days']) == days and all(day['state'] == 'available' for day in failure['days'])
    attempts = json.loads((tmp_path / 'run/execution.json').read_bytes())['attempts']
    assert [row['sha256'] for row in attempts] == [row['sha256'] for row in rows[:-1]]
    assert len(list((tmp_path / 'run').glob('*/manifest.json'))) == days


@pytest.mark.parametrize('days, budget, final_cap', [(2, 24576, 3584), (2, 25600, 4096),
    (10, 90112, 3584), (10, 91136, 4096)])
def test_four_gib_sources_and_metadata_are_charged_before_later_candidates(days, budget, final_cap, tmp_path):
    # 每个选中库4GiB，全部选中元数据合计512MiB；24/88GiB少512MiB单库预算，25/89GiB恰够。
    start = '2026-02-28T16:00:00Z'
    end = (datetime.fromisoformat(start.replace('Z', '+00:00')) + timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')
    rows = daily_candidates(tmp_path / 'inputs', start, days)
    metadata = {row['sha256'] + '-0': (1 if i // 3 < days - 1 else 513 - days) * 1024 ** 2
                for i, row in enumerate(rows)}
    receipt = tmp_path / 'caps.json'
    injected = f'''import pathlib,sqlite3,json,types
original_stat = pathlib.Path.stat
metadata = {metadata!r}
def sized(path, *args, **kwargs):
    value = original_stat(path, *args, **kwargs)
    size = value.st_size
    if path.name == 'snapshot.sqlite':
        size = 4294967296
    elif path.name == 'execution.json' and path.parent.name in metadata:
        size = metadata[path.parent.name] - original_stat(path.parent / 'manifest.json').st_size
    return types.SimpleNamespace(**{{key: size if key == 'st_size' else getattr(value, key)
                                   for key in dir(value) if key.startswith('st_')}})
pathlib.Path.stat = sized
caps = []
class SizedConnection(sqlite3.Connection):
    def execute(self, sql, *args, **kwargs):
        if sql.startswith('PRAGMA max_page_count='):
            cap = int(sql.split('=')[1]) // 256
            caps.append(cap)
            pathlib.Path({str(receipt)!r}).write_text(json.dumps(caps))
            if cap < 4096:
                raise sqlite3.OperationalError('injected 4 GiB projection exceeds cap')
        return super().execute(sql, *args, **kwargs)
original_connect = sqlite3.connect
def connected(*args, **kwargs):
    return original_connect(*args, factory=SizedConnection, **kwargs)
sqlite3.connect = connected
'''
    registry = tmp_path / 'published'
    run = batch(tmp_path, rows, start, end, registry, injection=injected, max_batch_mib=budget, max_seconds=0)
    caps = json.loads(receipt.read_bytes())
    assert caps[-1] == final_cap and all(cap == 4096 for cap in caps[:-1])
    if final_cap == 4096:
        assert run.returncode == 0, run.stderr
        report = json.loads(run.stdout)
        assert report['state'] == 'committed' and report['coverage'] == 'complete'
        assert len(caps) == days * 3
    else:
        assert run.returncode == 1 and 'injected 4 GiB projection exceeds cap' in run.stderr
        assert not (registry / 'registry.json').exists()
        assert json.loads((tmp_path / 'run/failure.json').read_bytes())['state'] == 'aborted'
    # 只在文件系统／SQLite边界插桩；实际fixture目录不产生GiB文件。
    assert sum(path.stat().st_size for path in tmp_path.rglob('*') if path.is_file()) < 2 * 1024 ** 2


def test_batch_metadata_overrun_preserves_previous_default_and_failed_candidate(tmp_path):
    rows = daily_candidates(tmp_path / 'inputs', '2026-02-26T16:00:00Z', 1)
    window = ('2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z')
    registry = tmp_path / 'published'
    assert batch(tmp_path, [rows[1]], *window, registry, 'old').returncode == 0
    before = (registry / 'registry.json').read_bytes()
    injected = f'''import pathlib,types
original_stat = pathlib.Path.stat
def sized(path, *args, **kwargs):
    value = original_stat(path, *args, **kwargs)
    if path.name == 'execution.json' and path.parent.name == {rows[0]['sha256'] + '-0'!r}:
        return types.SimpleNamespace(**{{key: 1048576 if key == 'st_size' else getattr(value, key)
                                       for key in dir(value) if key.startswith('st_')}})
    return value
pathlib.Path.stat = sized
'''
    run = batch(tmp_path, rows, *window, registry, injection=injected, max_batch_mib=2, max_seconds=0)
    assert run.returncode == 1 and '批次磁盘预算耗尽' in run.stderr
    assert (registry / 'registry.json').read_bytes() == before
    assert (tmp_path / 'run' / (rows[0]['sha256'] + '-0') / 'manifest.json').exists()
    assert json.loads((tmp_path / 'run/failure.json').read_bytes())['state'] == 'aborted'


def test_budget_parameters_change_execution_receipts_but_not_content_identities(tmp_path):
    source = candidate(tmp_path / 'source', '2026-02-27T00:00:00Z')
    window = ('2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z')
    registry = tmp_path / 'published'
    original = batch(tmp_path, [source], *window, registry, 'default')
    assert original.returncode == 0, original.stderr
    for name, limits in [('minimum', {'max_seconds': 1, 'max_database_mib': 1, 'max_batch_mib': 2}),
                         ('first', {'max_seconds': 0, 'max_database_mib': 4096, 'max_batch_mib': 25600}),
                         ('second', {'max_seconds': 0, 'max_database_mib': 4096, 'max_batch_mib': 91136})]:
        run = batch(tmp_path, [source], *window, registry, name, **limits)
        assert run.returncode == 0, run.stderr
        assert json.loads(run.stdout) == json.loads(original.stdout)
        execution = json.loads((tmp_path / name / 'execution.json').read_bytes())
        assert execution['max_database_mib'] == limits['max_database_mib']
        assert execution['max_batch_mib'] == limits['max_batch_mib']


def test_batch_timeout_and_second_day_disk_limit_leave_registry_unchanged(tmp_path):
    registry = tmp_path / 'published'
    first = candidate(tmp_path / 'first', '2026-02-27T00:00:00Z')
    second = candidate(tmp_path / 'second', '2026-02-28T00:00:00Z', 20000)
    window = ('2026-02-26T16:00:00Z', '2026-02-28T16:00:00Z')
    result = batch(tmp_path, [first, second], *window, registry, max_database_mib=1, max_seconds=0)
    assert result.returncode == 1
    assert not (registry / 'registry.json').exists()

    injection = f'''import pathlib,time
original_open = pathlib.Path.open
def opened(path, *args, **kwargs):
    if str(path) == {first['path']!r}: time.sleep(3)
    return original_open(path, *args, **kwargs)
pathlib.Path.open = opened
'''
    result = batch(tmp_path, [first], *window, registry, 'timeout', injection=injection, max_seconds=1)
    assert result.returncode == 1
    assert '资源上限' in result.stderr
    assert not (registry / 'registry.json').exists()


@pytest.mark.parametrize('changed_path, changes_snapshot', [
    ('backend/data_pipeline/bgp/snapshots/batch.py', False), ('scripts/rib/rib-snapshot.py', True)])
def test_implementation_bytes_change_only_the_identities_that_bind_them(changed_path, changes_snapshot, tmp_path):
    settings = json.loads((ROOT / 'config/data-profile.json').read_bytes())
    isolated = isolated_tool(tmp_path, settings)
    source = candidate(tmp_path / 'source', '2026-02-27T00:00:00Z')
    registry = tmp_path / 'published'
    window = ('2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z')
    before = batch(tmp_path, [source], *window, registry, 'before', cli=isolated / 'scripts/rib/rib-snapshot.py')
    assert before.returncode == 0, before.stderr
    with (isolated / changed_path).open('a') as stream:
        stream.write('\n# 隔离实现字节变化\n')
    after = batch(tmp_path, [source], *window, registry, 'after', cli=isolated / 'scripts/rib/rib-snapshot.py')
    assert after.returncode == 0, after.stderr
    old, new = json.loads(before.stdout), json.loads(after.stdout)
    assert old['selection_id'] != new['selection_id']
    if changes_snapshot:
        assert old['days'][0]['version'] != new['days'][0]['version']
    else:
        assert old['days'][0]['version'] == new['days'][0]['version']


@pytest.mark.parametrize('count, end', [(65, '2026-02-27T16:00:00Z'), (0, '2026-03-30T16:00:00Z')])
def test_candidate_and_day_count_are_bounded(count, end, tmp_path):
    source = candidate(tmp_path / 'source', '2026-02-27T00:00:00Z')
    registry = tmp_path / 'published'
    run = batch(tmp_path, [source] * count, '2026-02-26T16:00:00Z', end, registry)
    assert run.returncode == 1
    assert not registry.exists()


def test_verified_empty_family_is_available_zero_but_empty_raw_mrt_is_rejected(tmp_path, monkeypatch, client):
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    source = source_fixture(tmp_path, include_ipv6=False)
    row = {'path': str(source), 'sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
           'collector': 'rrc25', 'observed_at': '2026-02-27T00:00:00Z'}
    window = ('2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z')
    run = batch(tmp_path, [row], *window, registry)
    assert run.returncode == 0, run.stderr
    version = json.loads(run.stdout)['days'][0]['version']
    base = '/api/v1/rib-snapshots/' + version
    assert client.get(base + '?family=ipv6').json['metrics'] == {
        'visible_prefixes': 0, 'visible_origin_ases': 0, 'attributed_prefixes': 0,
        'unattributed_prefixes': 0, 'rib_entries': 0, 'unattributed_entries': 0}
    empty = client.get(base + '/asns/13335?family=ipv6').json
    assert empty['state'] == 'available' and empty['prefix_count'] == 0 and empty['items'] == []
    source.write_bytes(gzip.compress(b'', mtime=0))
    row['sha256'] = hashlib.sha256(source.read_bytes()).hexdigest()
    run = batch(tmp_path, [row], *window, registry, 'empty')
    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout)['days'][0]['state'] == 'validation_failed'
    assert client.get('/api/v1/rib-snapshots?date=2026-02-27').json['snapshots'][0]['version'] == version


def test_replacing_staged_candidate_with_another_valid_projection_aborts_whole_batch(tmp_path):
    from tests.rib.test_rib_snapshots import prepare
    source = candidate(tmp_path / 'source', '2026-02-27T00:00:00Z')
    alternate = candidate(tmp_path / 'alternate', '2026-02-27T00:00:00Z', 3)
    alternate_output = tmp_path / 'alternate-output'
    assert prepare(Path(alternate['path']), alternate_output).returncode == 0
    injected = f'''import shutil,pathlib
original_copy = shutil.copyfile
def copied(source, destination, *args, **kwargs):
    return original_copy(pathlib.Path({str(alternate_output)!r}) / pathlib.Path(source).name, destination, *args, **kwargs)
shutil.copyfile = copied
'''
    registry = tmp_path / 'published'
    run = batch(tmp_path, [source], '2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z', registry, injection=injected)
    assert run.returncode == 1
    assert not (registry / 'registry.json').exists()


def test_unsupported_mrt_scope_is_not_misreported_as_confirmed_corruption(tmp_path):
    unsupported = candidate(tmp_path / 'unsupported', '2026-02-27T00:00:00Z')
    raw = bytearray(gzip.decompress(Path(unsupported['path']).read_bytes()))
    first_rib_offset = 12 + struct.unpack_from('!I', raw, 8)[0]
    struct.pack_into('!H', raw, first_rib_offset + 6, 3)  # 多播范围未受本投影支持。
    Path(unsupported['path']).write_bytes(gzip.compress(raw, mtime=0))
    unsupported['sha256'] = hashlib.sha256(Path(unsupported['path']).read_bytes()).hexdigest()
    fallback = candidate(tmp_path / 'fallback', '2026-02-27T08:00:00Z')
    registry = tmp_path / 'published'
    run = batch(tmp_path, [unsupported, fallback], '2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z', registry)
    assert run.returncode == 1
    assert not registry.exists()


def binary_prefix_candidate(directory, stamp, prefix_length, prefix_bytes, as4=None):
    """单Peer、单前缀的原始二进制MRT，允许显式指定线上的Prefix字节。"""
    directory.mkdir()
    epoch = int(datetime.fromisoformat(stamp.replace('Z', '+00:00')).timestamp())
    def frame(subtype, body):
        return struct.pack('!IHHI', epoch, 13, subtype, len(body)) + body
    peer = b'\x02\xc0\x00\x02\x01\xc0\x00\x02\x01' + struct.pack('!I', 64496)
    raw = frame(1, b'\x00\x00\x00\x19\x00\x05rrc25\x00\x01' + peer)
    attributes = (b'\x40\x01\x01\x00' + b'\x40\x02\x06\x02\x01\x00\x00\x34\x17'
                  + b'\x40\x03\x04\xc0\x00\x02\x01')
    if as4 is not None:
        attributes += b'\xc0\x11' + bytes([len(as4)]) + as4
    body = struct.pack('!IB', 0, prefix_length) + prefix_bytes + b'\x00\x01'
    body += struct.pack('!HIH', 0, epoch - 1, len(attributes)) + attributes
    raw += frame(2, body)
    source = directory / 'fixture.gz'
    source.write_bytes(gzip.compress(raw, mtime=0))
    return {'path': str(source), 'sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
            'collector': 'rrc25', 'observed_at': stamp}


def test_nonzero_prefix_padding_is_valid_and_keeps_earliest_candidate_and_raw_location(tmp_path, monkeypatch, client):
    # RFC6396 §4.3.2：/25的末7位没有意义；c6 33 64 ff代表198.51.100.128/25。
    first = binary_prefix_candidate(tmp_path / 'padding', '2026-02-27T00:00:00Z', 25, b'\xc6\x33\x64\xff')
    later = candidate(tmp_path / 'later', '2026-02-27T08:00:00Z')
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    run = batch(tmp_path, [later, first], '2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z', registry)
    assert run.returncode == 0, run.stderr
    version = json.loads(run.stdout)['days'][0]['version']
    base = '/api/v1/rib-snapshots/' + version
    data = client.get(base + '/observations').json
    assert data['source']['sha256'] == first['sha256']
    assert data['total'] == 1
    row = data['items'][0]
    assert row['prefix'] == '198.51.100.128/25'
    assert (row['physical_record'], row['decoded_offset'], row['entry_index']) == (1, 38, 0)
    assert client.get(base + '/asns/13335').json['prefix_count'] == 1
    assert hashlib.sha256(Path(first['path']).read_bytes()).hexdigest() == first['sha256']


@pytest.mark.parametrize('as4', [b'', b'\x02\x01\x00\x00\x34', b'\x02\x02\x00\x00\x34\x17'])
def test_malformed_as4_path_is_rejected_before_selecting_later_candidate(as4, tmp_path, monkeypatch, client):
    # RFC6793 §6：空／不足6字节／段声明2个ASN却只有1个，均是明确的结构损坏。
    first = binary_prefix_candidate(tmp_path / 'bad-as4', '2026-02-27T00:00:00Z', 24, b'\xc6\x33\x64', as4)
    later = candidate(tmp_path / 'later', '2026-02-27T08:00:00Z')
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    run = batch(tmp_path, [later, first], '2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z', registry)
    assert run.returncode == 0, run.stderr
    day = json.loads(run.stdout)['days'][0]
    assert day['candidates'][0].get('rejection', {}).get('code') == 'invalid_mrt'
    data = client.get('/api/v1/rib-snapshots/' + day['version']).json
    assert data['source']['sha256'] == later['sha256']


def test_minimal_valid_as4_path_keeps_raw_evidence_without_reconstructing_origin(tmp_path, monkeypatch, client):
    first = binary_prefix_candidate(tmp_path / 'as4', '2026-02-27T00:00:00Z', 24, b'\xc6\x33\x64',
                                    b'\x02\x01\x00\x01\x00\x00')
    later = candidate(tmp_path / 'later', '2026-02-27T08:00:00Z')
    registry = tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    run = batch(tmp_path, [later, first], '2026-02-26T16:00:00Z', '2026-02-27T16:00:00Z', registry)
    assert run.returncode == 0, run.stderr
    version = json.loads(run.stdout)['days'][0]['version']
    base = '/api/v1/rib-snapshots/' + version
    data = client.get(base + '/observations').json
    assert data['source']['sha256'] == first['sha256']
    row = data['items'][0]
    assert row['as4_path_hex'] == '020100010000' and row['as_path_hex'] == '020100003417'
    assert row['raw_origin_asn'] == 13335 and row['attributed_origin_asn'] is None
    assert row['reason'] == 'as4_path_requires_separate_rule'
    assert client.get(base).json['metrics']['visible_origin_ases'] == 0
