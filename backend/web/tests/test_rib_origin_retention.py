"""起源规模公开离线命令；仅合成 MRT，不访问真实数据或旧应用。"""
import gzip
import hashlib
import ipaddress
import json
from pathlib import Path
import struct
import sqlite3
import shutil
import subprocess
import sys
import pytest

ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / 'scripts/retain-rib-origin-input.py'
STAMP = 1774944000  # 2026-03-31 08:00 UTC


def frame(subtype, body):
    return struct.pack('!IHHI', STAMP, 13, subtype, len(body)) + body


def rib_source(tmp_path, paths, as4_payload=None):
    peers = b''.join(bytes([2]) + ipaddress.ip_address(f'192.0.2.{i + 1}').packed * 2
                     + struct.pack('!I', 64496 + i) for i in range(len(paths)))
    raw = frame(1, b'\x00\x00\x00\x19\x00\x05rrc25' + struct.pack('!H', len(paths)) + peers)
    for seq, (subtype, prefix, selected) in enumerate([
        (2, '192.0.2.0/24', range(len(paths))), (4, '2001:db8::/32', [0, 1])
    ]):
        net = ipaddress.ip_network(prefix)
        entries = b''
        for peer in selected:
            path = b''.join(bytes([kind, len(asns)]) + struct.pack('!' + 'I' * len(asns), *asns)
                            for kind, asns in paths[peer])
            attr = b'\x50\x02' + struct.pack('!H', len(path)) + path
            if as4_payload is not None:
                attr += b'\xc0\x11' + bytes([len(as4_payload)]) + as4_payload
            entries += struct.pack('!HIH', peer, STAMP - 1, len(attr)) + attr
        body = struct.pack('!IB', seq, net.prefixlen) + net.network_address.packed[:(net.prefixlen + 7) // 8]
        raw += frame(subtype, body + struct.pack('!H', len(selected)) + entries)
    source = tmp_path / 'rib.gz'
    source.write_bytes(gzip.compress(raw, mtime=0))
    return source


def invoke(source, output):
    return subprocess.run([sys.executable, str(CLI), '--source', str(source), '--source-sha256',
                           hashlib.sha256(source.read_bytes()).hexdigest(), '--output', str(output)],
                          text=True, capture_output=True, timeout=15)


def test_cli_counts_attributed_origins_preserves_raw_sets_and_unions_families(tmp_path):
    source = rib_source(tmp_path, [
        [(2, [31027, 64512, 4200000001])],
        [(2, [31027, 328405])],
        [(2, [31027]), (1, [328405])],
        [(2, [31027]), (1, [36040, 211612]), (2, [64512])],
        [(2, [64512, 4200000001])],
        [(2, [31027]), (3, [64496])],
        [(1, [36040, 211612]), (2, [328405])],
    ])
    run = invoke(source, tmp_path / 'output')
    assert run.returncode == 0, run.stderr
    manifest = json.loads((tmp_path / 'output/manifest.json').read_text())
    summary = json.loads((tmp_path / 'output/summary.json').read_text())
    assert summary['observed_at'] == '2026-03-31T08:00:00Z'
    origins = json.loads((tmp_path / 'output/origins.json').read_text())
    assert origins['ipv4'] == [31027, 328405]
    assert origins['ipv6'] == [31027, 328405]
    assert summary['families']['all']['visible_origin_ases'] == 2  # 不把两族相加成4
    assert summary['families']['ipv4']['unattributed_entries'] == 4
    assert summary['families']['all']['rib_entries'] == 9
    with sqlite3.connect((tmp_path / 'output/paths.sqlite').as_uri() + '?mode=ro', uri=True) as database:
        database.row_factory = sqlite3.Row
        catalog = [dict(row) for row in database.execute('SELECT * FROM paths')]
    skipped = next(row for row in catalog if row['reason'] == 'private_as_skipped')
    assert skipped['raw_origin_asn'] == 4200000001
    assert skipped['attributed_origin_asn'] == 31027
    set_suffix = b'\x01\x01' + struct.pack('!I', 328405)
    assert any(row['path_key'].endswith(set_suffix) and row['attributed_origin_asn'] is None for row in catalog)
    assert manifest['summary']['sha256'] == hashlib.sha256((tmp_path / 'output/summary.json').read_bytes()).hexdigest()


def test_cli_reports_truncated_gzip_without_admitting_partial_origin_counts(tmp_path):
    source = rib_source(tmp_path, [[(2, [31027])], [(2, [328405])]])
    source.write_bytes(source.read_bytes()[:-8])
    output = tmp_path / 'truncated'
    run = invoke(source, output)
    assert run.returncode == 1
    assert '起源留存失败' in run.stderr
    assert not (output / 'manifest.json').exists()


@pytest.mark.parametrize('terminal,expected,reason', [
    (64511, 64511, 'explicit_terminal_asn'),
    (64512, 31027, 'private_as_skipped'),
    (65535, 31027, 'private_as_skipped'),
    (65536, 65536, 'explicit_terminal_asn'),
    (4200000000, 31027, 'private_as_skipped'),
    (4294967294, 31027, 'private_as_skipped'),
    (4294967295, None, 'special_asn_not_attributable'),
    (23456, None, 'special_asn_not_attributable'),
    (0, None, 'special_asn_not_attributable'),
])
def test_cli_preserves_raw_terminal_at_exclusion_and_reserved_boundaries(tmp_path, terminal, expected, reason):
    source = rib_source(tmp_path, [[(2, [31027, terminal])], [(2, [328405])]])
    output = tmp_path / 'bounded'
    run = invoke(source, output)
    assert run.returncode == 0, run.stderr
    with sqlite3.connect((output / 'paths.sqlite').as_uri() + '?mode=ro', uri=True) as database:
        row = database.execute('SELECT raw_origin_asn,attributed_origin_asn,reason FROM paths WHERE raw_origin_asn=?', (terminal,)).fetchone()
    assert row == (terminal, expected, reason)


def test_cli_rejects_existing_output_and_mismatched_source_sha(tmp_path):
    source = rib_source(tmp_path, [[(2, [31027])], [(2, [328405])]])
    output = tmp_path / 'occupied'
    output.mkdir()
    marker = output / 'keep.txt'
    marker.write_text('existing evidence')
    assert invoke(source, output).returncode == 1
    assert marker.read_text() == 'existing evidence'
    rejected = tmp_path / 'bad-sha'
    run = subprocess.run([sys.executable, str(CLI), '--source', str(source), '--source-sha256', '0' * 64,
                          '--output', str(rejected)], text=True, capture_output=True, timeout=15)
    assert run.returncode == 1
    assert not rejected.exists()


def test_cli_does_not_admit_truncated_as4_segments_as_merely_unknown(tmp_path):
    source = rib_source(tmp_path, [[(2, [31027])], [(2, [328405])]], as4_payload=b'\x02\x02\x00\x00\x79\x33')
    output = tmp_path / 'bad-as4'
    run = invoke(source, output)
    assert run.returncode == 1
    assert not (output / 'manifest.json').exists()


def instrumented(source, output, patch, cli=CLI):
    # 仅替换外部文件I/O，不调用或改写producer内部逻辑；入口仍为真实CLI。
    runner = ('import pathlib,runpy,sys,os\n' + patch + '\n'
              + 'sys.argv=' + repr([str(cli), '--source', str(source), '--source-sha256',
                                    hashlib.sha256(source.read_bytes()).hexdigest(), '--output', str(output)])
              + '\nrunpy.run_path(sys.argv[0],run_name="__main__")\n')
    return subprocess.run([sys.executable, '-c', runner], capture_output=True, text=True, timeout=15)


def test_cli_never_leaves_a_final_manifest_after_partial_manifest_write(tmp_path):
    source = rib_source(tmp_path, [[(2, [31027])], [(2, [328405])]])
    output = tmp_path / 'interrupted'
    patch = '''
original_open = pathlib.Path.open
class BrokenWrite:
    def __init__(self, stream): self.stream = stream
    def __enter__(self): return self
    def __exit__(self, *args): self.stream.close()
    def write(self, data):
        self.stream.write(data[:10])
        raise OSError('injected manifest write interruption')
def fail_manifest(path, mode='r', *args, **kwargs):
    stream = original_open(path, mode, *args, **kwargs)
    return BrokenWrite(stream) if 'manifest' in path.name and ('x' in mode or 'w' in mode) else stream
pathlib.Path.open = fail_manifest
'''
    run = instrumented(source, output, patch)
    assert run.returncode == 1
    assert not (output / 'manifest.json').exists()


def test_cli_does_not_treat_access_time_change_as_changed_source_content(tmp_path):
    source = rib_source(tmp_path, [[(2, [31027])], [(2, [328405])]])
    patch = f'''
original_stat = pathlib.Path.stat
counter = 0
def changed_atime(path, *args, **kwargs):
    global counter
    stat = original_stat(path, *args, **kwargs)
    if str(path) == {str(source)!r}:
        counter += 1
        values = list(stat)
        values[7] += counter
        return os.stat_result(values)
    return stat
pathlib.Path.stat = changed_atime
'''
    run = instrumented(source, tmp_path / 'atime', patch)
    assert run.returncode == 0, run.stderr


def test_cli_rejects_code_change_during_read_instead_of_binding_new_code_to_old_execution(tmp_path):
    source = rib_source(tmp_path, [[(2, [31027])], [(2, [328405])]])
    isolated = tmp_path / 'isolated-tool'
    # 保留包边界；否则从 backend 工作目录运行时，真实 regular package 会覆盖隔离目录的 namespace package。
    for name in ('scripts/retain-rib-origin-input.py', 'backend/data_pipeline/__init__.py',
                 'backend/data_pipeline/rib_origin.py', 'config/data-profile.json'):
        (isolated / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, isolated / name)
    producer = isolated / 'backend/data_pipeline/rib_origin.py'
    patch = f'''
original_open = pathlib.Path.open
changed = False
def mutate_during_source_read(path, mode='r', *args, **kwargs):
    global changed
    if not changed and str(path) == {str(source)!r} and mode == 'rb':
        changed = True
        with original_open(pathlib.Path({str(producer)!r}), 'a') as writer:
            writer.write('\\n# 模拟运行中代码变更\\n')
    return original_open(path, mode, *args, **kwargs)
pathlib.Path.open = mutate_during_source_read
'''
    output = tmp_path / 'code-changed'
    run = instrumented(source, output, patch, cli=isolated / 'scripts/retain-rib-origin-input.py')
    assert run.returncode == 1, (run.stdout, run.stderr)
    assert '代码' in run.stderr
    assert not (output / 'manifest.json').exists()
