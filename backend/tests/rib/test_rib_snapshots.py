"""共享快照纵向验收：人工 MRT → 显式离线命令 → 公开只读 HTTP。"""
import gzip
import hashlib
import ipaddress
import json
from pathlib import Path
import struct
import subprocess
import sys
import shutil
import sqlite3
import pytest

ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / 'scripts/rib/rib-snapshot.py'
STAMP = 1772150400  # 2026-02-27 00:00 UTC


def source_fixture(directory, extra_prefixes=0, stamp=STAMP, include_ipv6=True):
    def frame(subtype, body):
        return struct.pack('!IHHI', stamp, 13, subtype, len(body)) + body

    peers = b''.join(bytes([2]) + ipaddress.ip_address(f'192.0.2.{i + 1}').packed * 2
                     + struct.pack('!I', 64496 + i) for i in range(3))
    raw = frame(1, b'\x00\x00\x00\x19\x00\x05rrc25\x00\x03' + peers)
    # 手算：5 个前缀、2 个明确起源；3 个前缀至少有一个明确起源、2 个完全无明确起源。
    # 共9条观察，其中4条不可归属；多Peer和多起源不会抬高总体并集。
    rows = [
        ('198.51.100.0/24', [[(2, [174, 13335])], [(2, [13335])]]),
        ('203.0.113.0/24', [[(2, [13335, 64512])], [(2, [15169])], [(1, [13335, 15169])]]),
        ('2001:db8::/32', [[(2, [15169])], [(3, [64496])]]),
        ('192.0.2.0/24', [[(2, [64512])]]),
        ('192.0.3.0/24', [[(1, [13335])]]),
    ]
    rows.extend((f'2001:db8:{i + 1:x}::/48', [[(2, [13335])]]) for i in range(extra_prefixes))
    for seq, (prefix, paths) in enumerate(rows):
        network = ipaddress.ip_network(prefix)
        if network.version == 6 and not include_ipv6:
            continue
        entries = b''
        for peer, segments in enumerate(paths):
            path = b''.join(bytes([kind, len(asns)]) + struct.pack('!' + 'I' * len(asns), *asns)
                            for kind, asns in segments)
            attributes = b'\x40\x02' + bytes([len(path)]) + path
            entries += struct.pack('!HIH', peer, STAMP - 1, len(attributes)) + attributes
        body = struct.pack('!IB', seq, network.prefixlen) + network.network_address.packed[:(network.prefixlen + 7) // 8]
        raw += frame(2 if network.version == 4 else 4, body + struct.pack('!H', len(paths)) + entries)
    source = directory / 'fixture.gz'
    source.write_bytes(gzip.compress(raw, mtime=0))
    return source


def command(*args):
    return subprocess.run([sys.executable, str(CLI), *map(str, args)], capture_output=True, text=True, timeout=20)


def prepare(source, output, **overrides):
    options = {'source': source, 'source-sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
               'collector': 'rrc25', 'observed-at': '2026-02-27T00:00:00Z', 'output': output, **overrides}
    return command('prepare', *(value for key, value in options.items() for value in ('--' + key, value)))


def test_raw_rib_becomes_discoverable_only_after_explicit_registration(tmp_path, monkeypatch, client):
    source = source_fixture(tmp_path)
    output, registry = tmp_path / 'candidate', tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    run = prepare(source, output)
    assert run.returncode == 0, run.stderr
    version = json.loads(run.stdout)['version']
    assert client.get('/api/v1/rib-snapshots').status_code == 503
    assert not registry.exists()  # GET不能创建登记目录。
    run = command('register', '--candidate', output, '--registry', registry)
    assert run.returncode == 0, run.stderr
    found = client.get('/api/v1/rib-snapshots?date=2026-02-27')
    assert found.status_code == 200
    assert found.json['snapshots'][0]['version'] == version
    summary = client.get(f'/api/v1/rib-snapshots/{version}')
    assert summary.status_code == 200
    assert summary.json['metrics'] == {'visible_prefixes': 5, 'visible_origin_ases': 2,
                                      'attributed_prefixes': 3, 'unattributed_prefixes': 2,
                                      'rib_entries': 9, 'unattributed_entries': 4}
    assert summary.json['observed_at'] == '2026-02-27T00:00:00Z'
    assert summary.json['source']['collector_id'] == 'rrc25'
    assert summary.json['source']['coverage'] == 'unknown'


def publish_fixture(tmp_path, monkeypatch):
    source = source_fixture(tmp_path)
    output, registry = tmp_path / 'candidate', tmp_path / 'published'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    run = prepare(source, output)
    assert run.returncode == 0, run.stderr
    assert command('register', '--candidate', output, '--registry', registry).returncode == 0
    return source, output, registry, json.loads(run.stdout)['version']


def test_asn_reads_distinct_explicit_origins_from_the_same_snapshot(tmp_path, monkeypatch, client):
    _, _, _, version = publish_fixture(tmp_path, monkeypatch)
    base = f'/api/v1/rib-snapshots/{version}'
    response = client.get(base + '/asns/13335')
    assert response.status_code == 200
    data = response.json
    assert data['prefix_count'] == 2  # 多Peer只计1；单元素AS_SET不计作明确起源。
    assert data['asn'] == 13335 and data['family'] == 'all'
    overall = client.get(base).json
    assert all(data[key] == overall[key] for key in ('version', 'date', 'observed_at', 'source', 'origin_rule', 'limitations'))
    other = client.get(base + '/asns/15169').json
    assert other['prefix_count'] == 2  # 多起源前缀分别关联，不拆分或任意归给一方。
    assert overall['metrics']['attributed_prefixes'] == 3
    assert client.get(base + '/asns/13335?family=ipv6').json['prefix_count'] == 0
    assert client.get(base + '/asns/15169?family=ipv6').json['prefix_count'] == 1
    assert client.get(base + '/asns/174').json['prefix_count'] == 0  # 仅路径经过不算起源。


def test_asn_evidence_is_bounded_repeatable_and_traces_only_its_explicit_origins(tmp_path, monkeypatch, client):
    source = source_fixture(tmp_path, extra_prefixes=30)
    output, registry = tmp_path / 'candidate', tmp_path / 'published'
    run = prepare(source, output)
    assert run.returncode == 0, run.stderr
    assert command('register', '--candidate', output, '--registry', registry).returncode == 0
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    version = json.loads(run.stdout)['version']
    url = f'/api/v1/rib-snapshots/{version}/asns/13335'
    data = client.get(url).json
    assert data['prefix_count'] == 32
    assert data['sample_limit'] == 20 and data['sample_truncated'] is True
    assert len(data['items']) == 20
    assert data == client.get(url).json
    assert all(row['attributed_origin_asn'] == 13335 for row in data['items'])
    assert data['items'][0]['prefix'] == '198.51.100.0/24'
    assert (data['items'][0]['physical_record'], data['items'][0]['entry_index'], data['items'][0]['decoded_offset']) == (1, 0, 64)
    assert data['items'][1]['peer']['ip'] == '192.0.2.2'
    assert data['items'][2]['reason'] == 'private_as_skipped'
    empty = client.get(url.replace('13335', '64512')).json
    assert empty['state'] == 'available' and empty['prefix_count'] == 0
    assert empty['items'] == [] and empty['sample_truncated'] is False


def test_public_observations_preserve_prefix_origins_raw_peers_and_source_locations(tmp_path, monkeypatch, client):
    source, _, _, version = publish_fixture(tmp_path, monkeypatch)
    response = client.get(f'/api/v1/rib-snapshots/{version}/observations?page_size=100')
    assert response.status_code == 200
    data = response.json
    assert data['total'] == 9
    assert data['source']['sha256'] == hashlib.sha256(source.read_bytes()).hexdigest()
    first = data['items'][0]
    assert first['peer'] == {'index': 0, 'bgp_id': '192.0.2.1', 'ip': '192.0.2.1', 'asn': 64496}
    assert (first['physical_record'], first['decoded_offset'], first['entry_index']) == (1, 64, 0)
    assert first['originated_time_epoch'] == STAMP - 1
    mixed = [row for row in data['items'] if row['prefix'] == '203.0.113.0/24']
    assert [row['attributed_origin_asn'] for row in mixed] == [13335, 15169, None]
    assert mixed[0]['raw_origin_asn'] == 64512
    assert mixed[2]['reason'] == 'as_set_ambiguous'
    assert mixed[2]['as_path_hex'] == '01020000341700003b41'
    assert data['items'][-1]['reason'] == 'as_set_ambiguous'  # 单元素AS_SET也不猜测起源。
    ipv6 = client.get(f'/api/v1/rib-snapshots/{version}?family=ipv6').json
    assert ipv6['metrics'] == {'visible_prefixes': 1, 'visible_origin_ases': 1,
                               'attributed_prefixes': 1, 'unattributed_prefixes': 0,
                               'rib_entries': 2, 'unattributed_entries': 1}


def test_streamed_projection_hits_disk_cap_without_publishing_partial_result(tmp_path):
    source = source_fixture(tmp_path, extra_prefixes=20000)
    output = tmp_path / 'too-large'
    run = prepare(source, output, **{'max-database-mib': 1})
    assert run.returncode == 1
    assert '资源' in run.stderr or 'disk is full' in run.stderr
    assert not (output / 'manifest.json').exists()
    registry = tmp_path / 'published'
    assert command('register', '--candidate', output, '--registry', registry).returncode != 0
    assert not registry.exists()


def test_source_movement_and_repeated_preparation_keep_identity_and_observation_count(tmp_path, monkeypatch, client):
    source, _, registry, version = publish_fixture(tmp_path, monkeypatch)
    moved = tmp_path / 'renamed.gz'
    shutil.copyfile(source, moved)
    another = tmp_path / 'another-candidate'
    run = prepare(moved, another)
    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout)['version'] == version
    assert command('register', '--candidate', another, '--registry', registry).returncode == 0
    assert len(client.get('/api/v1/rib-snapshots').json['snapshots']) == 1
    assert client.get(f'/api/v1/rib-snapshots/{version}').json['metrics']['rib_entries'] == 9


@pytest.mark.parametrize('failure', ['sha', 'gzip', 'timestamp', 'collector'])
def test_bad_input_has_no_complete_candidate(failure, tmp_path):
    source = source_fixture(tmp_path)
    options = {}
    if failure == 'gzip':
        source.write_bytes(source.read_bytes()[:-8])
    if failure == 'sha':
        options['source-sha256'] = '0' * 64
    if failure == 'timestamp':
        options['observed-at'] = '2026-02-27T01:00:00Z'
    if failure == 'collector':
        options['collector'] = 'rrc00'
    output = tmp_path / failure
    assert prepare(source, output, **options).returncode == 1
    assert not (output / 'manifest.json').exists()


def test_corrupt_selected_snapshot_and_unknown_version_are_explicit_read_only_errors(tmp_path, monkeypatch, client):
    _, _, registry, version = publish_fixture(tmp_path, monkeypatch)
    unknown = client.get('/api/v1/rib-snapshots/rib_snapshot_v1_' + 'f' * 64)
    assert unknown.status_code == 404 and unknown.json['state'] == 'unknown_version'
    path = registry / 'versions' / version / 'snapshot.sqlite'
    path.write_bytes(b'broken')
    before = {str(p): p.read_bytes() for p in registry.rglob('*') if p.is_file()}
    assert client.get(f'/api/v1/rib-snapshots/{version}').status_code == 503
    assert client.get('/api/v1/rib-snapshots?date=2026-02-27').status_code == 503
    assert {str(p): p.read_bytes() for p in registry.rglob('*') if p.is_file()} == before


def test_public_http_responses_match_shared_snapshot_contract(tmp_path, monkeypatch, client):
    import jsonschema
    _, _, _, version = publish_fixture(tmp_path, monkeypatch)
    contract = json.loads((ROOT / 'contracts/openapi.json').read_bytes())
    for path, actual in [('/rib-snapshots', '/rib-snapshots?date=2026-02-27'),
                         ('/rib-snapshots/{version}', f'/rib-snapshots/{version}'),
                         ('/rib-snapshots/{version}/asns/{asn}', f'/rib-snapshots/{version}/asns/13335'),
                         ('/rib-snapshots/{version}/observations', f'/rib-snapshots/{version}/observations')]:
        response = client.get('/api/v1' + actual)
        schema = contract['paths'][path]['get']['responses']['200']['content']['application/json']['schema']
        jsonschema.Draft202012Validator({**schema, 'components': contract['components']}).validate(response.json)


def test_asn_version_failure_and_publication_change_never_return_zero_or_switch(tmp_path, monkeypatch, client):
    _, _, registry, version = publish_fixture(tmp_path, monkeypatch)
    old_url = f'/api/v1/rib-snapshots/{version}/asns/13335'
    before = client.get(old_url).json
    source = source_fixture(tmp_path, extra_prefixes=1)
    other = tmp_path / 'other'
    run = prepare(source, other)
    newer = json.loads(run.stdout)['version']
    assert command('register', '--candidate', other, '--registry', registry).returncode == 0
    assert client.get(old_url).json == before
    assert client.get(f'/api/v1/rib-snapshots/{newer}/asns/13335').json['prefix_count'] == 3
    assert client.get('/api/v1/rib-snapshots/rib_snapshot_v1_' + 'f' * 64 + '/asns/13335').status_code == 404
    (registry / 'versions' / version / 'snapshot.sqlite').write_bytes(b'broken')
    files = {str(p): p.read_bytes() for p in registry.rglob('*') if p.is_file()}
    failure = client.get(old_url)
    assert failure.status_code == 503 and 'prefix_count' not in failure.json
    assert {str(p): p.read_bytes() for p in registry.rglob('*') if p.is_file()} == files


def test_latest_selection_validates_only_selected_day_and_never_falls_back(tmp_path, monkeypatch, client):
    _, _, registry, old = publish_fixture(tmp_path, monkeypatch)
    source = source_fixture(tmp_path, stamp=STAMP + 86400)
    other = tmp_path / 'later-day'
    run = prepare(source, other, **{'observed-at': '2026-02-28T00:00:00Z'})
    assert run.returncode == 0, run.stderr
    newer = json.loads(run.stdout)['version']
    assert command('register', '--candidate', other, '--registry', registry).returncode == 0
    selected = client.get('/api/v1/rib-snapshots?latest=true')
    assert selected.status_code == 200
    assert selected.json['snapshots'] == [{'date': '2026-02-28', 'version': newer, 'observed_at': '2026-02-28T00:00:00Z'}]
    (registry / 'versions' / old / 'snapshot.sqlite').write_bytes(b'broken')
    assert client.get('/api/v1/rib-snapshots?latest=true').json == selected.json
    assert client.get('/api/v1/rib-snapshots?date=2026-02-27').status_code == 503
    assert client.get('/api/v1/rib-snapshots?date=2026-03-01').json['snapshots'] == []
    (registry / 'versions' / newer / 'snapshot.sqlite').write_bytes(b'broken')
    assert client.get('/api/v1/rib-snapshots?latest=true').status_code == 503
    assert client.get('/api/v1/rib-snapshots?latest=true&date=2026-02-27').status_code == 400


@pytest.mark.parametrize('suffix', ['0', '-1', 'AS13335', '4294967296', '1.1', '13335?family=bad',
                                  '13335?family=all&family=ipv4', '13335?page_size=100000'])
def test_asn_rejects_invalid_queries(suffix, client):
    assert client.get('/api/v1/rib-snapshots/rib_snapshot_v1_' + 'a' * 64 + '/asns/' + suffix).status_code == 400


def test_valid_result_in_wrong_version_directory_is_never_relabelled(tmp_path, monkeypatch, client):
    _, _, registry, version = publish_fixture(tmp_path, monkeypatch)
    source = source_fixture(tmp_path, extra_prefixes=1)
    other = tmp_path / 'other'
    assert prepare(source, other).returncode == 0
    shutil.copytree(other, registry / 'versions' / version, dirs_exist_ok=True)
    assert client.get(f'/api/v1/rib-snapshots/{version}').status_code == 503
    assert client.get('/api/v1/rib-snapshots?date=2026-02-27').status_code == 503


def test_registering_an_old_result_again_keeps_the_current_daily_default(tmp_path, monkeypatch, client):
    _, original, registry, version = publish_fixture(tmp_path, monkeypatch)
    source = source_fixture(tmp_path, extra_prefixes=1)
    other = tmp_path / 'other'
    run = prepare(source, other)
    newer = json.loads(run.stdout)['version']
    assert command('register', '--candidate', other, '--registry', registry).returncode == 0
    assert command('register', '--candidate', original, '--registry', registry).returncode == 0
    assert client.get('/api/v1/rib-snapshots?date=2026-02-27').json['snapshots'][0]['version'] == newer
    assert client.get(f'/api/v1/rib-snapshots/{version}').json['metrics']['visible_prefixes'] == 5


def test_registration_rejects_wrong_prefix_origin_projection_even_when_totals_match(tmp_path):
    source = source_fixture(tmp_path)
    output = tmp_path / 'candidate'
    assert prepare(source, output).returncode == 0
    database_path = output / 'snapshot.sqlite'
    with sqlite3.connect(database_path) as database:
        database.execute("UPDATE origins SET asn=15169 WHERE prefix='198.51.100.0/24'")
    manifest_path = output / 'manifest.json'
    manifest = json.loads(manifest_path.read_bytes())
    manifest['database']['sha256'] = hashlib.sha256(database_path.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest))
    registry = tmp_path / 'published'
    run = command('register', '--candidate', output, '--registry', registry)
    assert run.returncode == 1
    assert not registry.exists()


def test_summary_get_uses_offline_validation_seal_without_reading_whole_database(tmp_path, monkeypatch, client):
    _, _, _, version = publish_fixture(tmp_path, monkeypatch)
    original_open = Path.open

    def bounded_open(path, mode='r', *args, **kwargs):
        if path.name == 'snapshot.sqlite':
            raise AssertionError('HTTP总体查询不能全量复读SQLite文件')
        return original_open(path, mode, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', bounded_open)
    assert client.get(f'/api/v1/rib-snapshots/{version}').status_code == 200
    assert client.get('/api/v1/rib-snapshots?date=2026-02-27').status_code == 200


def test_interrupted_manifest_write_never_leaves_a_complete_candidate(tmp_path):
    source = source_fixture(tmp_path)
    output = tmp_path / 'interrupted'
    arguments = [str(CLI), 'prepare', '--source', str(source), '--source-sha256', hashlib.sha256(source.read_bytes()).hexdigest(),
                 '--collector', 'rrc25', '--observed-at', '2026-02-27T00:00:00Z', '--output', str(output)]
    runner = '''import pathlib, runpy, sys
original_open = pathlib.Path.open
class Interrupted:
    def __init__(self, stream): self.stream = stream
    def __enter__(self): return self
    def __exit__(self, *args): self.stream.close()
    def write(self, data):
        self.stream.write(data[:10])
        raise OSError('injected write interruption')
def opened(path, mode='r', *args, **kwargs):
    stream = original_open(path, mode, *args, **kwargs)
    return Interrupted(stream) if 'manifest' in path.name and ('w' in mode or 'x' in mode) else stream
pathlib.Path.open = opened
'''
    run = subprocess.run([sys.executable, '-c', runner + '\nsys.argv=' + repr(arguments)
                          + '\nrunpy.run_path(sys.argv[0],run_name="__main__")'], capture_output=True, text=True, timeout=20)
    assert run.returncode == 1
    assert 'injected write interruption' in run.stderr
    assert not (output / 'manifest.json').exists()


def test_changed_implementation_during_preparation_has_no_complete_candidate(tmp_path):
    source = source_fixture(tmp_path)
    isolated = tmp_path / 'tool'
    for name in ('scripts/rib/rib-snapshot.py', 'backend/data_pipeline/__init__.py',
                 'backend/data_pipeline/bgp/snapshots/origin.py', 'backend/data_pipeline/bgp/snapshots/snapshot.py',
                 'backend/data_pipeline/bgp/snapshots/snapshot_store.py', 'config/data-profile.json'):
        destination = isolated / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    output = tmp_path / 'candidate'
    arguments = [str(isolated / 'scripts/rib/rib-snapshot.py'), 'prepare', '--source', str(source),
                 '--source-sha256', hashlib.sha256(source.read_bytes()).hexdigest(), '--collector', 'rrc25',
                 '--observed-at', '2026-02-27T00:00:00Z', '--output', str(output)]
    runner = f'''import pathlib,runpy,sys
original_open = pathlib.Path.open
changed = False
def opened(path, mode='r', *args, **kwargs):
    global changed
    if not changed and str(path) == {str(source)!r} and mode == 'rb':
        changed = True
        with original_open(pathlib.Path({str(isolated / 'backend/data_pipeline/bgp/snapshots/origin.py')!r}), 'a') as stream:
            stream.write('\\n# 运行中修改\\n')
    return original_open(path, mode, *args, **kwargs)
pathlib.Path.open = opened
'''
    run = subprocess.run([sys.executable, '-c', runner + '\nsys.argv=' + repr(arguments)
                          + '\nrunpy.run_path(sys.argv[0],run_name="__main__")'], capture_output=True, text=True, timeout=20)
    assert run.returncode == 1
    assert '代码' in run.stderr
    assert not (output / 'manifest.json').exists()


def test_new_snapshot_configuration_does_not_change_existing_anomaly_http_results(tmp_path, monkeypatch, client):
    from tests.core_overview.test_core_overview_api import retained
    # 复用既有fixture生成器；被测行为仍从公开HTTP读取。
    anomaly_dir = tmp_path / 'anomalies'
    anomaly_dir.mkdir()
    retained.__wrapped__(monkeypatch, anomaly_dir)
    before = client.get('/api/v1/core-overview?date=2026-02-27').json
    _, _, registry, version = publish_fixture(tmp_path, monkeypatch)
    assert client.get('/api/v1/core-overview?date=2026-02-27').json == before
    database = registry / 'versions' / version / 'snapshot.sqlite'
    database.write_bytes(b'broken')
    assert client.get(f'/api/v1/rib-snapshots/{version}').status_code == 503
    assert client.get('/api/v1/core-overview?date=2026-02-27').json == before


@pytest.mark.parametrize('query', ['date=2026-04-01', 'date=2026-2-27', 'date=invalid', 'date=2026-02-27&date=2026-02-28', 'unexpected=1'])
def test_snapshot_discovery_rejects_invalid_scope(query, client):
    assert client.get('/api/v1/rib-snapshots?' + query).status_code == 400


def test_unconfigured_or_missing_registry_never_initializes_data_on_startup(tmp_path, monkeypatch):
    from run import create_app
    monkeypatch.delenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', raising=False)
    assert create_app().test_client().get('/api/v1/rib-snapshots').json['state'] == 'not_configured'
    missing = tmp_path / 'missing-registry'
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(missing))
    client = create_app().test_client()
    assert not missing.exists()
    assert client.get('/api/v1/rib-snapshots').status_code == 503
    assert not missing.exists()


def test_same_size_content_change_with_restored_mtime_invalidates_registered_file(tmp_path, monkeypatch, client):
    import os
    _, _, registry, version = publish_fixture(tmp_path, monkeypatch)
    path = registry / 'versions' / version / 'snapshot.sqlite'
    before = path.stat()
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert client.get(f'/api/v1/rib-snapshots/{version}').status_code == 503


def test_malformed_registered_timestamp_returns_declared_unavailable_response(tmp_path, monkeypatch, client):
    _, _, registry, version = publish_fixture(tmp_path, monkeypatch)
    path = registry / 'versions' / version / 'manifest.json'
    manifest = json.loads(path.read_bytes())
    manifest['summary']['observed_at'] = 17
    path.write_text(json.dumps(manifest))
    response = client.get(f'/api/v1/rib-snapshots/{version}')
    assert response.status_code == 503
    assert response.json['state'] == 'validation_failed'


def test_asn_query_deadline_returns_unavailable_without_partial_numbers(tmp_path, monkeypatch, client):
    from itertools import count
    import time
    source = source_fixture(tmp_path, extra_prefixes=10000)
    output, registry = tmp_path / 'candidate', tmp_path / 'published'
    run = prepare(source, output)
    assert run.returncode == 0, run.stderr
    assert command('register', '--candidate', output, '--registry', registry).returncode == 0
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    version = json.loads(run.stdout)['version']
    # 系统时钟边界模拟查询预算已耗尽；不依赖私有SQL形状或真实业务规模。
    ticks = count(0, 3)
    monkeypatch.setattr(time, 'monotonic', lambda: next(ticks))
    response = client.get(f'/api/v1/rib-snapshots/{version}/asns/13335')
    assert response.status_code == 503
    assert response.json['state'] == 'validation_failed' and 'prefix_count' not in response.json
