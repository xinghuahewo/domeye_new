"""单RIB消费包公开CLI；只使用临时合成输入，不查询真实数据。"""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / 'scripts/retain-rib-scale-input.py'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def evidence(tmp_path):
    source = tmp_path / 'source.gz'
    source.write_bytes(gzip.compress(b'synthetic-complete-rib', mtime=0))
    folder = tmp_path / 'evidence'
    folder.mkdir()
    for name in ('candidate-0800-executed.py', 'candidate_frame_census.py'):
        (folder / name).write_text('THIS IS EVIDENCE, NEVER EXECUTED\n')
    families = {
        '4': {'prefix_count': 2, 'prefix_set_sha256': 'a' * 64,
              'statistics': {'rib_records': 2, 'rib_entries': 3}, 'peer_entry_counts': {'0': 2, '1': 1}},
        '6': {'prefix_count': 1, 'prefix_set_sha256': 'b' * 64,
              'statistics': {'rib_records': 1, 'rib_entries': 2}, 'peer_entry_counts': {'0': 1, '2': 1}},
    }
    identity = {'bytes': source.stat().st_size, 'mtime_ns': 1, 'inode': 1, 'device': 1}
    audit = {'complete_stream': True, 'sample_only': False, 'gzip_read_to_eof': True,
             'view_name': 'rrc25', 'collector_bgp_id': '0.0.0.25', 'peers': [
                 {'index': 0, 'ip': '192.0.2.1', 'asn': 64496, 'bgp_id': '192.0.2.1', 'peer_type': 2},
                 {'index': 1, 'ip': '192.0.2.2', 'asn': 64497, 'bgp_id': '192.0.2.2', 'peer_type': 2},
                 {'index': 2, 'ip': '2001:db8::3', 'asn': 64498, 'bgp_id': '192.0.2.3', 'peer_type': 3}],
             'timestamp_counts': {'2026-03-31T08:00:00+00:00': 4}, 'mrt_records': 4,
             'decompressed_bytes': 22, 'compressed_sha256': digest(source), 'families': families,
             'source_path': '/home/bgpdata/data/ripe/rrc25/2026.03/bview.20260331.0800.gz',
             'identity_before': identity, 'identity_after': identity,
             'counts': {'ipv4_prefixes': 2, 'ipv6_prefixes': 1, 'combined_prefixes': 3}}
    frames = {'gzip_eof': True, 'source_sha256': digest(source), 'source': audit['source_path'],
              'mrt_epoch_counts': {'1774944000': 4}, 'mrt_records': 4, 'decoded_bytes': 22,
              'peer_table_count': 3, 'families': families,
              'script_sha256': digest(folder / 'candidate_frame_census.py')}
    # 独立字面预期：两条IPv4前缀与一条IPv6前缀，不把五条entry计作五个Prefix。
    def save(name, value):
        (folder / name).write_text(json.dumps(value))
    save('candidate-0800-proof.json', audit)
    save('candidate-0800-frames.json', frames)
    save('candidate-0800-receipt.json', {'exit_code': 0,
         'started_utc': '2026-09-11T08:00:00Z', 'finished_utc': '2026-09-11T08:01:00Z',
         'output_sha256': digest(folder / 'candidate-0800-proof.json'),
         'executed_code_sha256': digest(folder / 'candidate-0800-executed.py')})
    return folder, source


def command(evidence, output):
    folder, source = evidence
    return [sys.executable, str(CLI), '--evidence-dir', str(folder), '--source', str(source),
            '--audit-sha256', digest(folder / 'candidate-0800-proof.json'),
            '--frames-sha256', digest(folder / 'candidate-0800-frames.json'), '--output', str(output)]


def test_retains_actual_snapshot_prefix_union_without_inventing_origin_metric(evidence, tmp_path):
    output = tmp_path / 'consumption'
    run = subprocess.run(command(evidence, output), capture_output=True, text=True, timeout=15)
    assert run.returncode == 0, run.stderr
    manifest = json.loads((output / 'manifest.json').read_text())
    summary = json.loads((output / manifest['summary']['file']).read_text())
    assert summary['observed_at'] == '2026-03-31T08:00:00Z'
    assert summary['families']['ipv4']['visible_prefixes'] == 2
    assert summary['families']['ipv6']['visible_prefixes'] == 1
    assert summary['families']['all']['visible_prefixes'] == 3
    assert summary['families']['all']['rib_entries'] == 5
    assert summary['families']['all']['peer_indices'] == [0, 1, 2]
    assert summary['origin_metric_state'] == 'pending_definition'
    assert all(value['visible_origin_ases'] is None for value in summary['families'].values())
    assert summary['source']['coverage'] == 'unknown'
    assert digest(output / manifest['summary']['file']) == manifest['summary']['sha256']
    assert summary['data_profile']['snapshot_time'] == '2026-03-31T23:59:59+08:00'
    again = subprocess.run(command(evidence, output), capture_output=True, timeout=15)
    assert again.returncode != 0


def refresh_receipt(folder):
    path = folder / 'candidate-0800-receipt.json'
    receipt = json.loads(path.read_text())
    receipt['output_sha256'] = digest(folder / 'candidate-0800-proof.json')
    path.write_text(json.dumps(receipt))


def test_incomplete_gzip_cannot_be_promoted_even_when_report_hashes_match(evidence, tmp_path):
    folder, source = evidence
    source.write_bytes(source.read_bytes()[:-8])
    for name, field in [('candidate-0800-proof.json', 'compressed_sha256'), ('candidate-0800-frames.json', 'source_sha256')]:
        path = folder / name
        value = json.loads(path.read_text())
        value[field] = digest(source)
        if name == 'candidate-0800-proof.json':
            value['identity_before']['bytes'] = value['identity_after']['bytes'] = source.stat().st_size
        path.write_text(json.dumps(value))
    refresh_receipt(folder)
    output = tmp_path / 'invalid'
    run = subprocess.run(command(evidence, output), capture_output=True, text=True, timeout=15)
    assert run.returncode != 0
    assert not (output / 'manifest.json').exists()


@pytest.mark.parametrize('problem', ['duplicate_peer', 'noncanonical_peer', 'out_of_range_peer', 'negative_peer_count', 'bad_set_sha', 'boolean_receipt'])
def test_matching_reports_do_not_bypass_structural_census_rules(evidence, tmp_path, problem):
    folder, _ = evidence
    for name in ('candidate-0800-proof.json', 'candidate-0800-frames.json'):
        path = folder / name
        value = json.loads(path.read_text())
        family = value['families']['4']
        if problem == 'duplicate_peer':
            family['statistics']['duplicate_peer_entries_within_record'] = 1
        elif problem == 'noncanonical_peer':
            family['peer_entry_counts'] = {'00': 2, '1': 1}
        elif problem == 'out_of_range_peer':
            family['peer_entry_counts'] = {'3': 2, '1': 1}
        elif problem == 'negative_peer_count':
            family['peer_entry_counts'] = {'0': -1, '1': 4}
        elif problem == 'bad_set_sha':
            family['prefix_set_sha256'] = 'not-a-set-digest'
        path.write_text(json.dumps(value))
    refresh_receipt(folder)
    if problem == 'boolean_receipt':
        path = folder / 'candidate-0800-receipt.json'
        value = json.loads(path.read_text())
        value['exit_code'] = False
        path.write_text(json.dumps(value))
    output = tmp_path / 'invalid'
    run = subprocess.run(command(evidence, output), capture_output=True, text=True, timeout=15)
    assert run.returncode != 0
    assert not (output / 'manifest.json').exists()


def test_output_cannot_be_nested_in_evidence(evidence):
    folder, _ = evidence
    output = folder / 'new-output'
    before = {path.name: digest(path) for path in folder.iterdir()}
    run = subprocess.run(command(evidence, output), capture_output=True, text=True, timeout=15)
    assert run.returncode != 0
    assert not output.exists()
    assert {path.name: digest(path) for path in folder.iterdir()} == before


def test_duplicate_json_keys_cannot_hide_a_conflicting_collector(evidence, tmp_path):
    folder, _ = evidence
    path = folder / 'candidate-0800-proof.json'
    path.write_text(path.read_text().replace('"view_name": "rrc25"', '"view_name": "another", "view_name": "rrc25"'))
    refresh_receipt(folder)
    output = tmp_path / 'duplicate-key'
    run = subprocess.run(command(evidence, output), capture_output=True, text=True, timeout=15)
    assert run.returncode != 0
    assert not output.exists()
