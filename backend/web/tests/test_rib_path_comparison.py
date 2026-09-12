"""只经离线命令及其公开制品验收两端比较；合成MRT，不访问真实数据。"""
import gzip
import hashlib
import ipaddress
import json
from pathlib import Path
import struct
import subprocess
import sys
import shutil
import pytest

ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / 'scripts/compare-rib-paths.py'
LEFT = 1774886400  # UTC 03-30 16:00，北京03-31零点
RIGHT = 1774944000
PEERS = [('192.0.2.1', '192.0.2.1', 64496), ('192.0.2.2', '192.0.2.2', 64497)]


def path(*segments):
    return b''.join(bytes([kind, len(asns)]) + struct.pack('!' + 'I' * len(asns), *asns)
                    for kind, asns in segments)


def source(tmp_path, name, stamp, peers, rows, extra=b''):
    def frame(subtype, body):
        return struct.pack('!IHHI', stamp, 13, subtype, len(body)) + body
    table = b'\0\0\0\x19\0\x05rrc25' + struct.pack('!H', len(peers))
    for bgp, address, asn in peers:
        ip = ipaddress.ip_address(address)
        table += bytes([2 + (ip.version == 6)]) + ipaddress.ip_address(bgp).packed + ip.packed + struct.pack('!I', asn)
    raw = frame(1, table)
    for seq, (prefix, entries, addpath) in enumerate(rows):
        net = ipaddress.ip_network(prefix)
        body = struct.pack('!IB', seq, net.prefixlen) + net.network_address.packed[:(net.prefixlen + 7)//8]
        body += struct.pack('!H', len(entries))
        for peer, as_path, as4 in entries:
            attrs = b'' if as_path is None else b'\x50\x02' + struct.pack('!H', len(as_path)) + as_path
            if as4 is not None:
                attrs += b'\xd0\x11' + struct.pack('!H', len(as4)) + as4
            body += struct.pack('!HI', peer, stamp - 20)
            if addpath:
                body += struct.pack('!I', 7)
            body += struct.pack('!H', len(attrs)) + attrs
        raw += frame((2 if net.version == 4 else 4) + (6 if addpath else 0), body)
    file = tmp_path / name
    file.write_bytes(gzip.compress(raw + extra, mtime=0))
    return file


def invoke(left, right, output, extra=()):
    return subprocess.run([sys.executable, str(CLI), '--left', str(left), '--left-sha256',
        hashlib.sha256(left.read_bytes()).hexdigest(), '--right', str(right), '--right-sha256',
        hashlib.sha256(right.read_bytes()).hexdigest(), '--output', str(output), *extra],
        capture_output=True, text=True, timeout=20)


def read_result(output):
    summary = json.loads((output / 'summary.json').read_text())
    with gzip.open(output / 'comparisons.jsonl.gz', 'rt') as stream:
        rows = [json.loads(line) for line in stream]
    return summary, rows


def test_cli_pairs_raw_peers_not_indexes_and_preserves_prepend_and_private_as(tmp_path):
    left = source(tmp_path, 'l.gz', LEFT, PEERS, [
        ('192.0.2.0/24', [(0, path((2, [31027, 64512])), None), (1, path((2, [6939, 64496])), None)], False),
        ('2001:db8::/32', [(0, path((2, [31027, 64496])), None)], False)])
    right = source(tmp_path, 'r.gz', RIGHT, list(reversed(PEERS)), [
        ('2001:db8::/32', [(1, path((2, [31027, 64496, 64496])), None)], False),
        ('192.0.2.0/24', [(1, path((2, [31027]), (2, [64512])), None), (0, path((2, [6939, 64497])), None)], False)])
    output = tmp_path / 'result'
    run = invoke(left, right, output)
    assert run.returncode == 0, run.stderr
    summary, rows = read_result(output)
    assert summary['totals'] == {'same': 1, 'different': 2, 'left_only': 0, 'right_only': 0, 'not_comparable': 0}
    assert summary['comparable_pairs'] == 3
    assert summary['interval_change_count'] is None
    assert summary['session_continuity'] == 'unknown'
    assert [row['prefix'] for row in rows] == ['192.0.2.0/24', '2001:db8::/32']
    pair = rows[0]['objects'][0]
    assert pair == [0, 'same', [[0, 0]], [[0, 0]], []]
    assert rows[0]['left_frames'][0][0] == 1
    assert rows[0]['right_frames'][0][0] == 2
    assert (output / 'left.mrt').read_bytes() == gzip.decompress(left.read_bytes())
    assert (output / 'right.mrt').read_bytes() == gzip.decompress(right.read_bytes())


@pytest.mark.parametrize('case,expected_reason', [
    ('as_set', 'as_set'), ('confed_sequence', 'confed_sequence'), ('confed_set', 'confed_set'),
    ('as4', 'as4_path_present'), ('empty', 'empty_as_path'), ('missing', 'missing_as_path'),
    ('duplicate_entry', 'duplicate_entry'), ('duplicate_peer', 'ambiguous_peer_group'), ('addpath', 'add_path')])
def test_cli_retains_non_comparable_objects_without_inventing_zero_changes(tmp_path, case, expected_reason):
    peers = PEERS + [PEERS[0]] if case == 'duplicate_peer' else PEERS
    special = {'as_set': path((2, [31027]), (1, [64496, 64497])),
               'confed_sequence': path((2, [31027]), (3, [64496])),
               'confed_set': path((4, [64496]), (2, [31027])),
               'empty': b'', 'missing': None}.get(case, path((2, [31027, 64496])))
    entries = [(0, special, path((2, [31027])) if case == 'as4' else None)]
    if case == 'duplicate_entry':
        entries *= 2
    left = source(tmp_path, 'l.gz', LEFT, peers, [('192.0.2.0/24', entries, case == 'addpath')])
    right = source(tmp_path, 'r.gz', RIGHT, PEERS, [('192.0.2.0/24', [(0, path((2, [31027, 64496])), None)], False)])
    output = tmp_path / 'result'
    run = invoke(left, right, output)
    assert run.returncode == 0, run.stderr
    summary, rows = read_result(output)
    assert summary['totals']['not_comparable'] == 1
    assert summary['comparable_pairs'] == 0
    assert summary['different_fraction'] is None
    assert expected_reason in rows[0]['objects'][0][4]
    assert summary['reason_counts'][expected_reason] == 1
    assert (output / 'left.mrt').read_bytes() == gzip.decompress(left.read_bytes())


def test_cli_never_pairs_same_prefix_from_different_peer_or_merges_different_ips(tmp_path):
    peers = [PEERS[0], (PEERS[0][0], '2001:db8::1', PEERS[0][2])]
    entries = [(0, path((2, [64496])), None)]
    left = source(tmp_path, 'l.gz', LEFT, peers, [('192.0.2.0/24', entries, False)])
    right = source(tmp_path, 'r.gz', RIGHT, peers, [('192.0.2.0/24', [(1, path((2, [64496])), None)], False)])
    run = invoke(left, right, tmp_path / 'out')
    assert run.returncode == 0, run.stderr
    summary, _ = read_result(tmp_path / 'out')
    assert summary['totals'] == {'same': 0, 'different': 0, 'left_only': 1, 'right_only': 1, 'not_comparable': 0}
    assert summary['comparable_pairs'] == 0
    assert summary['different_fraction'] is None


def test_cli_preserves_duplicates_across_rib_frames_instead_of_selecting_one(tmp_path):
    row = ('192.0.2.0/24', [(0, path((2, [64496])), None)], False)
    left = source(tmp_path, 'l.gz', LEFT, PEERS, [row, row])
    right = source(tmp_path, 'r.gz', RIGHT, PEERS, [row])
    run = invoke(left, right, tmp_path / 'out')
    assert run.returncode == 0, run.stderr
    summary, rows = read_result(tmp_path / 'out')
    assert summary['totals']['not_comparable'] == 1
    assert rows[0]['objects'][0][2] == [[0, 0], [1, 0]]


@pytest.mark.parametrize('damage', ['gzip', 'mrt', 'path', 'as4', 'extra_peer_table', 'peer_index', 'time'])
def test_cli_never_admits_a_partial_or_ambiguous_file(tmp_path, damage):
    row = ('192.0.2.0/24', [(0, path((2, [64496])), None)], False)
    left = source(tmp_path, 'l.gz', LEFT, PEERS, [row])
    if damage in ('path', 'as4', 'peer_index'):
        broken = b'\x02\x02\0\0\xfc\x00'
        entries = [(99 if damage == 'peer_index' else 0,
                    broken if damage == 'path' else path((2, [64496])), broken if damage == 'as4' else None)]
        left = source(tmp_path, 'l.gz', LEFT, PEERS, [('192.0.2.0/24', entries, False)])
    elif damage == 'gzip':
        left.write_bytes(left.read_bytes()[:-8])
    elif damage == 'mrt':
        left.write_bytes(gzip.compress(gzip.decompress(left.read_bytes())[:-1], mtime=0))
    elif damage == 'extra_peer_table':
        raw = gzip.decompress(left.read_bytes())
        size = struct.unpack_from('!I', raw, 8)[0]
        left.write_bytes(gzip.compress(raw + raw[:12 + size], mtime=0))
    elif damage == 'time':
        left = source(tmp_path, 'l.gz', RIGHT, PEERS, [row])
    right = source(tmp_path, 'r.gz', RIGHT, PEERS, [row])
    output = tmp_path / 'out'
    run = invoke(left, right, output)
    assert run.returncode == 1
    assert not (output / 'manifest.json').exists()
    failure = json.loads((output / 'failure.json').read_text())
    assert failure['status'] == 'not_admitted'
    assert failure['source_sha256'][0] == hashlib.sha256(left.read_bytes()).hexdigest()


def test_cli_existing_inputs_are_unchanged_and_identical_runs_have_identical_versions(tmp_path):
    row = ('192.0.2.0/24', [(0, path((2, [64496])), None)], False)
    left = source(tmp_path, 'l.gz', LEFT, PEERS, [row])
    right = source(tmp_path, 'r.gz', RIGHT, PEERS, [row])
    before = (left.read_bytes(), right.read_bytes())
    first = invoke(left, right, tmp_path / 'one')
    second = invoke(left, right, tmp_path / 'two')
    assert first.returncode == second.returncode == 0
    assert json.loads(first.stdout)['version'] == json.loads(second.stdout)['version']
    assert invoke(left, right, tmp_path / 'one').returncode == 1
    assert before == (left.read_bytes(), right.read_bytes())
    manifest = json.loads((tmp_path / 'one/manifest.json').read_text())
    for name, binding in manifest['files'].items():
        assert hashlib.sha256((tmp_path / 'one' / name).read_bytes()).hexdigest() == binding['sha256']


@pytest.mark.parametrize('case', ['source_symlink', 'parent_symlink', 'bad_sha', 'manifest_write', 'code_change'])
def test_cli_rejects_unsafe_or_changed_bindings_without_final_manifest(tmp_path, case):
    row = ('192.0.2.0/24', [(0, path((2, [64496])), None)], False)
    left = source(tmp_path, 'l.gz', LEFT, PEERS, [row])
    right = source(tmp_path, 'r.gz', RIGHT, PEERS, [row])
    output = tmp_path / 'out'
    cli = CLI
    patch = ''
    if case == 'source_symlink':
        (tmp_path / 'linked.gz').symlink_to(left)
        left = tmp_path / 'linked.gz'
    elif case == 'parent_symlink':
        (tmp_path / 'linked').symlink_to(tmp_path, target_is_directory=True)
        output = tmp_path / 'linked/out'
    elif case == 'manifest_write':
        patch = '''
original_open = pathlib.Path.open
def blocked(path, mode='r', *args, **kwargs):
    if path.name == 'manifest.pending' and mode == 'xb':
        raise OSError('注入最终清单写入失败')
    return original_open(path, mode, *args, **kwargs)
pathlib.Path.open = blocked
'''
    elif case == 'code_change':
        isolated = tmp_path / 'isolated'
        for name in ('scripts/compare-rib-paths.py', 'backend/data_pipeline/__init__.py',
                     'backend/data_pipeline/rib_path_comparison.py', 'backend/data_pipeline/rib_origin.py', 'config/data-profile.json'):
            (isolated / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, isolated / name)
        cli = isolated / 'scripts/compare-rib-paths.py'
        changed = isolated / 'backend/data_pipeline/rib_path_comparison.py'
        patch = f'''
original_open = pathlib.Path.open
changed = False
def mutate(path, mode='r', *args, **kwargs):
    global changed
    if not changed and str(path) == {str(left)!r} and mode == 'rb':
        changed = True
        with original_open(pathlib.Path({str(changed)!r}), 'a') as writer:
            writer.write('\\n# 注入运行中代码变化\\n')
    return original_open(path, mode, *args, **kwargs)
pathlib.Path.open = mutate
'''
    args = [str(cli), '--left', str(left), '--left-sha256', '0'*64 if case == 'bad_sha' else hashlib.sha256(left.read_bytes()).hexdigest(),
            '--right', str(right), '--right-sha256', hashlib.sha256(right.read_bytes()).hexdigest(), '--output', str(output)]
    code = 'import pathlib,runpy,sys\n' + patch + '\nsys.argv=' + repr(args) + '\nrunpy.run_path(sys.argv[0],run_name="__main__")\n'
    run = subprocess.run([sys.executable, '-c', code], text=True, capture_output=True, timeout=20)
    assert run.returncode == 1, (run.stdout, run.stderr)
    assert not (output / 'manifest.json').exists()


def test_cli_does_not_turn_a_committed_manifest_into_failure_when_temp_cleanup_is_unavailable(tmp_path):
    row = ('192.0.2.0/24', [(0, path((2, [64496])), None)], False)
    left = source(tmp_path, 'l.gz', LEFT, PEERS, [row])
    right = source(tmp_path, 'r.gz', RIGHT, PEERS, [row])
    output = tmp_path / 'out'
    args = [str(CLI), '--left', str(left), '--left-sha256', hashlib.sha256(left.read_bytes()).hexdigest(),
            '--right', str(right), '--right-sha256', hashlib.sha256(right.read_bytes()).hexdigest(), '--output', str(output)]
    code = '''import pathlib,runpy,sys
def no_cleanup(*args, **kwargs):
    raise OSError('注入临时名称不可清理')
pathlib.Path.unlink = no_cleanup
sys.argv = ''' + repr(args) + '\nrunpy.run_path(sys.argv[0],run_name="__main__")\n'
    run = subprocess.run([sys.executable, '-c', code], text=True, capture_output=True, timeout=20)
    assert run.returncode == 0, run.stderr
    assert (output / 'manifest.json').exists()
    assert not (output / 'failure.json').exists()
