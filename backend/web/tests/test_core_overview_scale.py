"""单RIB规模与异常消费兼容；只用临时合成文件，经公开HTTP验收。"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import shutil

import pytest

from test_core_overview_api import retained, PROFILE, URL


def put(path, value):
    payload = (json.dumps(value, ensure_ascii=False, sort_keys=True) + '\n').encode()
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


@pytest.fixture()
def scaled(retained, monkeypatch, tmp_path):
    path, _, _ = retained
    output = tmp_path / 'index'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/index-core-overview-inputs.py'),
                          '--input', str(path), '--output', str(output)], capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    manifest = json.loads((output / 'manifest.json').read_bytes())
    package = output / 'scale'
    package.mkdir()
    (package / 'evidence').mkdir()
    evidence = {}
    for name in ('candidate-0800-proof.json', 'candidate-0800-frames.json', 'candidate-0800-receipt.json',
                 'candidate-0800-executed.py', 'candidate_frame_census.py'):
        evidence[name] = {'file': 'evidence/' + name, 'sha256': put(package / 'evidence' / name, {'synthetic': True})}
    summary = {
        'schema_version': 'core-rib-scale/v1', 'interpretation_version': 'rib-prefix-union/v1',
        'data_profile': PROFILE, 'observed_at': '2026-02-27T08:00:00Z',
        'source': {'collector_id': 'rrc25', 'collector_bgp_id': '0.0.0.25', 'view_name': 'rrc25', 'coverage': 'unknown',
                   'path': '/synthetic/bview.gz', 'sha256': 'a' * 64, 'compressed_bytes': 100},
        'families': {
            'ipv4': {'visible_prefixes': 3, 'rib_entries': 5, 'peer_indices': [0, 1], 'prefix_set_sha256': 'b' * 64, 'visible_origin_ases': None},
            'ipv6': {'visible_prefixes': 2, 'rib_entries': 2, 'peer_indices': [1], 'prefix_set_sha256': 'c' * 64, 'visible_origin_ases': None},
            'all': {'visible_prefixes': 5, 'rib_entries': 7, 'peer_indices': [0, 1], 'visible_origin_ases': None}},
        'origin_metric_state': 'pending_definition', 'limits': ['单RIB，不是连续路由状态。'],
    }
    package_manifest = {'schema_version': 'core-rib-scale-manifest/v1',
                        'summary': {'file': 'summary.json', 'sha256': put(package / 'summary.json', summary)},
                        'source_local_path': '/synthetic/not-needed-by-web.gz', 'evidence': evidence, 'retainer_sha256': 'd' * 64}
    manifest['scale'] = {'file': 'scale/manifest.json', 'sha256': put(package / 'manifest.json', package_manifest)}
    (output / 'manifest.json').chmod(0o600)
    put(output / 'manifest.json', manifest)
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(output / 'manifest.json'))
    return output, manifest, summary


def test_snapshot_prefix_union_is_not_anomaly_count_or_rib_entry_count(client, scaled):
    response = client.get(URL, query_string={'date': '2026-02-27'})
    assert response.status_code == 200
    data = response.get_json()
    assert data['overview'] == {'record_count': 7, 'visible_prefixes': 5, 'visible_origin_ases': None}
    assert data['metadata']['scale']['observed_at'] == '2026-02-27T08:00:00Z'
    assert data['metadata']['scale']['available_dates'] == ['2026-02-27']
    assert data['metadata']['scale']['state'] == 'available'
    assert data['metadata']['scale']['version'] == 'rib_scale_v1_' + scaled[1]['scale']['sha256']
    import jsonschema
    contract = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_bytes())
    jsonschema.Draft202012Validator({'$ref': '#/components/schemas/CoreOverviewPayload', 'components': contract['components']}).validate(data)


@pytest.mark.parametrize('family,value,state', [('ipv4', 3, 'available'), ('ipv6', 2, 'available'), ('unknown', None, 'family_not_supported')])
def test_snapshot_follows_address_family_not_list_filters(client, scaled, family, value, state):
    data = client.get(URL, query_string={'date': '2026-02-27', 'family': family, 'q': 'no-match', 'hour': '5', 'level': 'high'}).get_json()
    assert data['events']['total'] == 0
    assert data['overview']['visible_prefixes'] == value
    assert data['metadata']['scale']['state'] == state


def replace_summary(scaled, change):
    output, manifest, summary = scaled
    change(summary)
    package = json.loads((output / 'scale/manifest.json').read_bytes())
    package['summary']['sha256'] = put(output / 'scale/summary.json', summary)
    manifest['scale']['sha256'] = put(output / 'scale/manifest.json', package)
    put(output / 'manifest.json', manifest)


def test_snapshot_is_not_carried_to_another_available_business_day(client, scaled):
    replace_summary(scaled, lambda value: value.update(observed_at='2026-03-31T08:00:00Z'))
    data = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    assert data['state'] == 'available' and data['overview']['record_count'] == 7
    assert data['overview']['visible_prefixes'] is None
    assert data['metadata']['scale']['state'] == 'date_not_retained'
    assert data['metadata']['scale']['available_dates'] == ['2026-03-31']


@pytest.mark.parametrize('damage', ['summary_hash', 'collector', 'time', 'family_sum', 'origin', 'boolean', 'peer', 'traversal', 'duplicate_json', 'symlink_loop'])
def test_bad_scale_keeps_anomalies_available_without_numeric_scale(client, scaled, damage):
    output, manifest, _ = scaled
    before = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    if damage == 'summary_hash':
        (output / 'scale/summary.json').write_bytes(b'corrupt')
    elif damage == 'symlink_loop':
        path = output / 'scale/summary.json'
        path.rename(output / 'scale/summary.original.json')
        path.symlink_to('summary.json')
    elif damage == 'traversal':
        manifest['scale']['file'] = '../scale/manifest.json'
        put(output / 'manifest.json', manifest)
    elif damage == 'duplicate_json':
        raw = (output / 'scale/manifest.json').read_bytes().replace(b'{', b'{"schema_version":"wrong",', 1)
        (output / 'scale/manifest.json').write_bytes(raw)
        manifest['scale']['sha256'] = hashlib.sha256(raw).hexdigest()
        put(output / 'manifest.json', manifest)
    else:
        changes = {
            'collector': lambda value: value['source'].update(collector_id='rrc00'),
            'time': lambda value: value.update(observed_at='2026-04-01T08:00:00Z'),
            'family_sum': lambda value: value['families']['all'].update(visible_prefixes=6),
            'origin': lambda value: value['families']['all'].update(visible_origin_ases=3),
            'boolean': lambda value: value['families']['ipv6'].update(visible_prefixes=True),
            'peer': lambda value: value['families']['ipv4'].update(peer_indices=[0, 0]),
        }
        replace_summary(scaled, changes[damage])
    response = client.get(URL, query_string={'date': '2026-02-27'})
    data = response.get_json()
    assert response.status_code == 200 and data['state'] == 'available'
    assert data['events'] == before['events'] and data['trend'] == before['trend']
    assert data['overview'] == {'record_count': 7, 'visible_prefixes': None, 'visible_origin_ases': None}
    assert data['metadata']['scale']['state'] == 'unavailable'


def test_scale_binding_does_not_bypass_failed_or_unretained_anomaly_days(client, scaled):
    output, _, _ = scaled
    missing = client.get(URL, query_string={'date': '2026-02-28'}).get_json()
    assert missing['state'] == 'window_not_retained' and missing['overview'] is None
    assert 'scale' not in missing['metadata']
    (output / '2026-02-27.sqlite3').chmod(0o600)
    (output / '2026-02-27.sqlite3').write_bytes(b'corrupt')
    failed = client.get(URL, query_string={'date': '2026-02-27'})
    assert failed.status_code == 503
    assert failed.get_json()['overview'] is None and failed.get_json()['events'] is None
    assert 'scale' not in failed.get_json()['metadata']


def test_removing_scale_binding_changes_version_not_anomaly_records(client, scaled):
    output, manifest, _ = scaled
    before = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    manifest.pop('scale')
    put(output / 'manifest.json', manifest)
    after = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    assert 'scale' not in after['metadata'] and after['overview']['visible_prefixes'] is None
    assert after['events'] == before['events'] and after['trend'] == before['trend']
    assert after['version'] != before['version']
    assert client.get(URL, query_string={'date': '2026-02-27', 'version': before['version']}).status_code == 409
    assert client.get(URL + '/record', query_string={'ref': before['events']['items'][0]['reference'], 'version': before['version']}).status_code == 409


def test_offline_binding_creates_new_copy_without_mutating_anomaly_input(client, scaled, monkeypatch, tmp_path):
    output, manifest, _ = scaled
    manifest.pop('scale')
    put(output / 'manifest.json', manifest)
    before = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    destination = tmp_path / 'bound'
    command = [sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/bind-core-overview-scale.py'),
               '--index', str(output / 'manifest.json'), '--scale', str(output / 'scale/manifest.json'), '--output', str(destination)]
    run = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stderr
    assert client.get(URL, query_string={'date': '2026-02-27'}).get_json() == before
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(destination / 'manifest.json'))
    after = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    assert after['overview']['visible_prefixes'] == 5
    assert after['events'] == before['events'] and after['trend'] == before['trend']
    assert after['version'] != before['version']
    assert subprocess.run(command, capture_output=True, text=True, timeout=10).returncode != 0


def test_request_reads_only_bound_manifest_and_summary_not_offline_audit_files(client, scaled):
    output, _, _ = scaled
    (output / 'scale/evidence').rename(output / 'scale/offline-only-evidence')
    data = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    assert data['overview']['visible_prefixes'] == 5
    assert data['metadata']['scale']['state'] == 'available'


def test_offline_binding_rejects_changed_audit_evidence(scaled, tmp_path):
    output, manifest, _ = scaled
    manifest.pop('scale')
    put(output / 'manifest.json', manifest)
    (output / 'scale/evidence/candidate-0800-proof.json').write_bytes(b'changed')
    destination = tmp_path / 'rejected'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/bind-core-overview-scale.py'),
                          '--index', str(output / 'manifest.json'), '--scale', str(output / 'scale/manifest.json'), '--output', str(destination)],
                         capture_output=True, text=True, timeout=10)
    assert run.returncode == 1 and '摘要不符' in run.stderr
    assert not (destination / 'manifest.json').exists()


@pytest.fixture()
def origin_scaled(scaled):
    output, manifest, summary = scaled
    folder = output / 'origin'
    folder.mkdir()
    families = {}
    for family, total in [('ipv4', 2), ('ipv6', 2), ('all', 3)]:
        metric = summary['families'][family]
        families[family] = {key: metric[key] for key in ('visible_prefixes', 'rib_entries')}
        families[family].update(visible_origin_ases=total, unattributed_entries=1 if family != 'ipv6' else 0)
        if family != 'all':
            families[family].update(prefix_set_sha256=metric['prefix_set_sha256'],
                                   peer_entry_counts={'0': 3, '1': 2} if family == 'ipv4' else {'1': 2})
    origin_summary = {'schema_version': 'core-rib-origin/v1', 'interpretation_version': 'rib-attributed-origin/private-skip-v1',
                      'source': {key: summary['source'][key] for key in ('collector_id', 'coverage', 'sha256', 'compressed_bytes')},
                      'data_profile': PROFILE, 'observed_at': summary['observed_at'], 'families': families,
                      'rules': {'skip_ranges_inclusive': [[64512, 65535], [4200000000, 4294967294]],
                                'reserved_legacy_exclusion': [65535], 'stop_special_asns': [0, 23456, 4294967295]},
                      'mrt_records': 6, 'decoded_bytes': 200, 'gzip_eof': True, 'distinct_paths': 5,
                      'limits': ['单RIB；原值另存，不跨歧义猜测。']}
    package = {'schema_version': 'core-rib-origin-manifest/v1',
               'summary': {'file': 'summary.json', 'sha256': put(folder / 'summary.json', origin_summary)},
               'producer_sha256': 'e' * 64, 'cli_sha256': 'f' * 64,
               'evidence': {name: {'file': name, 'sha256': 'a' * 64, 'bytes': 100}
                            for name in ('paths.sqlite', 'origins.json', 'peers.json')}}
    manifest['origin'] = {'file': 'origin/manifest.json', 'sha256': put(folder / 'manifest.json', package)}
    put(output / 'manifest.json', manifest)
    return output, manifest, summary, origin_summary


def test_http_origin_union_is_bound_to_same_prefix_snapshot_without_changing_anomalies(client, origin_scaled):
    data = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    assert data['overview'] == {'record_count': 7, 'visible_prefixes': 5, 'visible_origin_ases': 3}
    assert data['metadata']['scale']['origin_metric_state'] == 'available'
    assert data['metadata']['scale']['origin']['unattributed_entries'] == 1
    assert data['metadata']['scale']['origin']['version'].startswith('rib_origin_v1_')
    assert client.get(URL, query_string={'date': '2026-02-27', 'family': 'ipv4', 'q': 'no-match'}).get_json()['overview']['visible_origin_ases'] == 2
    import jsonschema
    contract = json.loads((Path(__file__).resolve().parents[3] / 'contracts/openapi.json').read_bytes())
    jsonschema.Draft202012Validator({'$ref': '#/components/schemas/CoreOverviewPayload', 'components': contract['components']}).validate(data)


@pytest.fixture()
def origin_offline(origin_scaled):
    import sqlite3
    output, manifest, _, origin_summary = origin_scaled
    folder = output / 'origin'
    with sqlite3.connect(folder / 'paths.sqlite') as database:
        database.execute('CREATE TABLE paths(attributed_origin_asn INTEGER, ipv4_count INTEGER, ipv6_count INTEGER)')
        database.executemany('INSERT INTO paths VALUES(?,?,?)', [(31027, 2, 1), (328405, 2, 0), (13979, 0, 1), (None, 1, 0)])
    put(folder / 'origins.json', {'ipv4': [31027, 328405], 'ipv6': [13979, 31027], 'all': [13979, 31027, 328405]})
    put(folder / 'peers.json', [{'index': 0}, {'index': 1}])
    package = json.loads((folder / 'manifest.json').read_bytes())
    for name, entry in package['evidence'].items():
        entry.update(sha256=hashlib.sha256((folder / name).read_bytes()).hexdigest(), bytes=(folder / name).stat().st_size)
    put(folder / 'manifest.json', package)
    manifest.pop('origin')
    put(output / 'manifest.json', manifest)
    return output, folder


def test_offline_origin_binding_preserves_parent_records_and_prefix_snapshot(client, origin_offline, monkeypatch, tmp_path):
    output, folder = origin_offline
    before = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    destination = tmp_path / 'origin-bound'
    command = [sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/bind-core-overview-origin.py'),
               '--index', str(output / 'manifest.json'), '--origin', str(folder / 'manifest.json'), '--output', str(destination)]
    run = subprocess.run(command, capture_output=True, text=True, timeout=15)
    assert run.returncode == 0, run.stderr
    assert client.get(URL, query_string={'date': '2026-02-27'}).get_json() == before
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', str(destination / 'manifest.json'))
    after = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    assert after['overview']['visible_origin_ases'] == 3
    assert after['overview']['visible_prefixes'] == before['overview']['visible_prefixes']
    assert after['metadata']['scale']['version'] == before['metadata']['scale']['version']
    assert after['events'] == before['events'] and after['trend'] == before['trend']
    assert after['version'] != before['version']
    assert client.get(URL, query_string={'date': '2026-02-27', 'version': before['version']}).status_code == 409


@pytest.mark.parametrize('damage', ['parent_symlink', 'private', 'reserved', 'boolean', 'negative_entries'])
def test_offline_origin_binding_rejects_escaped_evidence_or_invalid_members(origin_offline, tmp_path, damage):
    import sqlite3
    output, folder = origin_offline
    if damage == 'parent_symlink':
        outside = tmp_path / 'outside-evidence'
        (output / 'scale/evidence').rename(outside)
        (output / 'scale/evidence').symlink_to(outside, target_is_directory=True)
    else:
        with sqlite3.connect(folder / 'paths.sqlite') as database:
            if damage == 'negative_entries':
                database.executemany('INSERT INTO paths VALUES(?,?,?)', [(None, -1, 0), (None, 1, 0)])
            else:
                replacement = {'private': 64512, 'reserved': 23456, 'boolean': True}[damage]
                database.execute('UPDATE paths SET attributed_origin_asn=? WHERE attributed_origin_asn=31027', (int(replacement),))
                members = json.loads((folder / 'origins.json').read_bytes())
                for family in members:
                    members[family] = sorted(replacement if asn == 31027 else asn for asn in members[family])
                put(folder / 'origins.json', members)
        package = json.loads((folder / 'manifest.json').read_bytes())
        for name, entry in package['evidence'].items():
            entry.update(sha256=hashlib.sha256((folder / name).read_bytes()).hexdigest(), bytes=(folder / name).stat().st_size)
        put(folder / 'manifest.json', package)
    destination = tmp_path / 'rejected-origin'
    run = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[3] / 'scripts/bind-core-overview-origin.py'),
                          '--index', str(output / 'manifest.json'), '--origin', str(folder / 'manifest.json'), '--output', str(destination)],
                         capture_output=True, text=True, timeout=15)
    assert run.returncode == 1, (run.stdout, run.stderr)
    assert not (destination / 'manifest.json').exists()


def test_scale_binder_rejects_shared_code_change_during_copy(scaled, tmp_path):
    output, manifest, _ = scaled
    manifest.pop('scale')
    put(output / 'manifest.json', manifest)
    root = Path(__file__).resolve().parents[3]
    isolated = tmp_path / 'tool'
    (isolated / 'scripts').mkdir(parents=True)
    (isolated / 'config').mkdir()
    shutil.copyfile(root / 'config/data-profile.json', isolated / 'config/data-profile.json')
    shutil.copytree(root / 'backend/data_pipeline', isolated / 'backend/data_pipeline', ignore=shutil.ignore_patterns('__pycache__'))
    shutil.copyfile(root / 'scripts/bind-core-overview-scale.py', isolated / 'scripts/bind-core-overview-scale.py')
    target = isolated / 'backend/data_pipeline/consumption_files.py'
    runner = f'''
import pathlib,runpy,sys
original_open = pathlib.Path.open
changed = False
def mutate(path, mode='r', *args, **kwargs):
    global changed
    if not changed and str(path) == {str(output / 'scale/summary.json')!r} and mode == 'rb':
        changed = True
        with original_open(pathlib.Path({str(target)!r}), 'a') as writer:
            writer.write('\\n# 模拟共享复制代码变更\\n')
    return original_open(path, mode, *args, **kwargs)
pathlib.Path.open = mutate
sys.argv = {repr([str(isolated / 'scripts/bind-core-overview-scale.py'), '--index', str(output / 'manifest.json'), '--scale', str(output / 'scale/manifest.json'), '--output', str(tmp_path / 'changed-code')])}
runpy.run_path(sys.argv[0], run_name='__main__')
'''
    run = subprocess.run([sys.executable, '-c', runner], capture_output=True, text=True, timeout=15)
    assert run.returncode == 1, (run.stdout, run.stderr)
    assert '代码' in run.stderr
    assert not (tmp_path / 'changed-code/manifest.json').exists()


@pytest.mark.parametrize('damage', ['hash', 'source', 'time', 'union', 'unattributed', 'prefix', 'rules', 'symlink_loop'])
def test_bad_origin_isolated_from_existing_prefix_and_anomaly_data(client, origin_scaled, damage):
    output, manifest, _, summary = origin_scaled
    before = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    package = json.loads((output / 'origin/manifest.json').read_bytes())
    if damage == 'hash':
        (output / 'origin/summary.json').write_bytes(b'broken')
    elif damage == 'symlink_loop':
        (output / 'origin/summary.json').rename(output / 'origin/summary.original.json')
        (output / 'origin/summary.json').symlink_to('summary.json')
    else:
        if damage == 'source':
            summary['source']['sha256'] = '0' * 64
        elif damage == 'time':
            summary['observed_at'] = '2026-02-27T16:00:00Z'
        elif damage == 'union':
            summary['families']['all']['visible_origin_ases'] = 5
        elif damage == 'unattributed':
            summary['families']['ipv4']['unattributed_entries'] = 5
        elif damage == 'prefix':
            summary['families']['ipv4']['prefix_set_sha256'] = '0' * 64
        else:
            summary['rules']['skip_ranges_inclusive'] = []
        package['summary']['sha256'] = put(output / 'origin/summary.json', summary)
        manifest['origin']['sha256'] = put(output / 'origin/manifest.json', package)
        put(output / 'manifest.json', manifest)
    data = client.get(URL, query_string={'date': '2026-02-27'}).get_json()
    assert data['overview'] == {'record_count': 7, 'visible_prefixes': 5, 'visible_origin_ases': None}
    assert data['metadata']['scale']['origin_metric_state'] == 'unavailable'
    assert data['events'] == before['events'] and data['trend'] == before['trend']


@pytest.mark.parametrize('family,expected', [('ipv4', 2), ('ipv6', 2), ('unknown', None)])
def test_origin_address_family_and_unadmitted_dates_never_leak_counts(client, origin_scaled, family, expected):
    data = client.get(URL, query_string={'date': '2026-02-27', 'family': family, 'hour': '5', 'q': 'no-match'}).get_json()
    assert data['overview']['visible_origin_ases'] == expected
    missing = client.get(URL, query_string={'date': '2026-02-28'}).get_json()
    assert missing['overview'] is None and 'scale' not in missing['metadata']
    output = origin_scaled[0]
    (output / '2026-02-27.sqlite3').chmod(0o600)
    (output / '2026-02-27.sqlite3').write_bytes(b'broken')
    failed = client.get(URL, query_string={'date': '2026-02-27'})
    assert failed.status_code == 503 and failed.get_json()['overview'] is None
