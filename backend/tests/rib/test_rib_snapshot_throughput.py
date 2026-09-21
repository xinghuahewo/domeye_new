"""#23：同一人工输入经公开 CLI／HTTP 保留事实，并消除逐观察重复工作。"""
from collections import Counter
import gzip
import hashlib
import ipaddress
import json
import io
import sqlite3
import struct
import subprocess
import sys
import pytest

from tests.rib.test_rib_snapshots import CLI, STAMP, command, prepare


def shared_prefix_source(directory):
    def frame(subtype, body):
        return struct.pack('!IHHI', STAMP, 13, subtype, len(body)) + body

    peers = [dict(index=i, bgp_id=f'192.0.2.{i + 1}',
                  ip=f'192.0.2.{i + 1}' if i < 5 else '2001:db8::6', asn=64496 + i)
             for i in range(6)]
    peer_bytes = b''.join(bytes([2 if i < 5 else 3]) + ipaddress.ip_address(p['bgp_id']).packed
                          + ipaddress.ip_address(p['ip']).packed + struct.pack('!I', p['asn'])
                          for i, p in enumerate(peers))
    raw = frame(1, b'\0\0\0\x19\0\x05rrc25\0\x06' + peer_bytes)
    # 原字节、末端、明确起源和原因均为手工预期，不调用生产器解释路径。
    a = ('020100003417', None, 13335, 13335, 'explicit_terminal_asn')
    b = ('020100003b41', None, 15169, 15169, 'explicit_terminal_asn')
    as_set = ('010100003417', None, None, None, 'as_set_ambiguous')
    missing = (None, None, None, None, 'missing_or_empty_path')
    empty = ('', None, None, None, 'missing_or_empty_path')
    confed = ('03010000fbf0', None, None, None, 'confederation_ambiguous')
    as4 = ('020100003417', '020100010000', 13335, None, 'as4_path_requires_separate_rule')
    private = ('0202000034170000fc00', None, 64512, 13335, 'private_as_skipped')
    # /25、/33 的非零尾随填充位；乱序 Peer 索引与不同 originated time。
    records = [
        (2, 25, 'c63364ff', '198.51.100.128/25', [(3, a), (0, b), (4, a), (1, as_set), (2, missing), (5, empty)]),
        (4, 33, '20010db8ff', '2001:db8:8000::/33', [(2, b), (0, a), (4, b), (3, confed), (1, as4), (5, private)]),
        (2, 24, 'cb0071', '203.0.113.0/24', [(5, a), (1, a)]),
        (4, 32, '20010db9', '2001:db9::/32', [(4, missing), (0, as_set)]),
    ]
    expected = []
    for physical, (subtype, bits, packed, prefix, observations) in enumerate(records, 1):
        body = struct.pack('!IB', physical - 1, bits) + bytes.fromhex(packed) + struct.pack('!H', len(observations))
        for entry, (peer, (path, as4_path, raw_origin, origin, reason)) in enumerate(observations):
            attributes = b''
            for code, value in ((2, path), (17, as4_path)):
                if value is not None:
                    payload = bytes.fromhex(value)
                    attributes += bytes([0x40 if code == 2 else 0xc0, code, len(payload)]) + payload
            originated = STAMP - 10 - entry
            body += struct.pack('!HIH', peer, originated, len(attributes)) + attributes
            expected.append(dict(family='ipv4' if subtype == 2 else 'ipv6', prefix=prefix, safi=1,
                                 physical_record=physical, decoded_offset=len(raw), entry_index=entry,
                                 originated_time_epoch=originated, peer=peers[peer], as_path_hex=path,
                                 as4_path_hex=as4_path, raw_origin_asn=raw_origin,
                                 attributed_origin_asn=origin, reason=reason))
        raw += frame(subtype, body)
    source = directory / 'shared-prefix.gz'
    source.write_bytes(gzip.compress(raw, mtime=0))
    return source, expected


def measured_prepare(source, output, receipt):
    # 只观察标准库网络格式化及 SQLite 边界；真实 CLI 仍执行完整 prepare。
    runner = '''
import ipaddress,json,pathlib,runpy,sqlite3,sys
counts = {'networks': 0, 'origins': 0, 'observations': 0}
original_network, original_connect = ipaddress.ip_network, sqlite3.connect
def network(*args, **kwargs):
    counts['networks'] += 1
    return original_network(*args, **kwargs)
def trace(sql):
    words = sql.upper().split()
    if words and words[0] == 'INSERT' and 'INTO' in words:
        table = words[words.index('INTO') + 1].lower()
        if table in ('origins', 'observations'):
            counts[table] += 1
def connect(*args, **kwargs):
    connection = original_connect(*args, **kwargs)
    connection.set_trace_callback(trace)
    return connection
ipaddress.ip_network, sqlite3.connect = network, connect
'''
    args = [str(CLI), 'prepare', '--source', str(source), '--source-sha256', hashlib.sha256(source.read_bytes()).hexdigest(),
            '--collector', 'rrc25', '--observed-at', '2026-02-27T00:00:00Z', '--output', str(output)]
    runner += '\nsys.argv=' + repr(args) + '\nrunpy.run_path(sys.argv[0],run_name="__main__")\n'
    runner += 'pathlib.Path(' + repr(str(receipt)) + ').write_text(json.dumps(counts))\n'
    run = subprocess.run([sys.executable, '-c', runner], capture_output=True, text=True, timeout=20)
    assert run.returncode == 0, run.stderr
    return json.loads(run.stdout)['version'], json.loads(receipt.read_text())


def assert_public_facts(client, version, source, expected):
    base = '/api/v1/rib-snapshots/' + version
    overall = client.get(base)
    assert overall.status_code == 200
    assert overall.json['metrics'] == dict(visible_prefixes=4, visible_origin_ases=2, attributed_prefixes=3,
                                           unattributed_prefixes=1, rib_entries=16, unattributed_entries=7)
    assert overall.json['source'] == dict(sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                                         collector_id='rrc25', coverage='unknown')
    audit = client.get(base + '/observations?page_size=100')
    assert audit.status_code == 200 and audit.json['total'] == 16
    assert audit.json['items'] == expected
    # 全部关联均可由 ASN 的完整小样本取回，含跨 Prefix 重复 ASN 与双栈多起源。
    for asn, prefixes in ((13335, {'198.51.100.128/25', '2001:db8:8000::/33', '203.0.113.0/24'}),
                          (15169, {'198.51.100.128/25', '2001:db8:8000::/33'}), (64512, set())):
        response = client.get(base + f'/asns/{asn}')
        assert response.status_code == 200
        assert response.json['prefix_count'] == len(prefixes)
        assert {row['prefix'] for row in response.json['items']} == prefixes
        assert response.json['sample_truncated'] is False
        assert Counter(json.dumps(row, sort_keys=True) for row in response.json['items']) == Counter(
            json.dumps(row, sort_keys=True) for row in expected if row['attributed_origin_asn'] == asn)


def test_prepare_formats_each_physical_prefix_once_with_all_observations_intact(tmp_path, monkeypatch, client):
    source, expected = shared_prefix_source(tmp_path)
    output, registry = tmp_path / 'candidate', tmp_path / 'registry'
    version, counts = measured_prepare(source, output, tmp_path / 'counts.json')
    run = command('register', '--candidate', output, '--registry', registry)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    assert_public_facts(client, version, source, expected)
    assert counts['observations'] == 16
    assert counts['networks'] == 4


def test_prepare_writes_each_prefix_origin_once_without_losing_unknowns_or_cross_prefix_origins(tmp_path, monkeypatch, client):
    source, expected = shared_prefix_source(tmp_path)
    output, registry = tmp_path / 'candidate', tmp_path / 'registry'
    version, counts = measured_prepare(source, output, tmp_path / 'counts.json')
    run = command('register', '--candidate', output, '--registry', registry)
    assert run.returncode == 0, run.stderr
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', str(registry))
    assert_public_facts(client, version, source, expected)
    assert counts['observations'] == 16
    assert counts['origins'] == 5  # 两个双起源Prefix＋一个单起源Prefix；全未知Prefix不写关联。


@pytest.mark.parametrize('retain_catalog', [True, False])
@pytest.mark.parametrize('consumer', ['none', 'callback', 'falsey_callback'])
def test_public_parser_keeps_optional_consumers_and_catalog_modes(tmp_path, monkeypatch, retain_catalog, consumer):
    from data_pipeline.bgp.snapshots.origin import parse_rib

    source, expected = shared_prefix_source(tmp_path)
    observed, networks = [], []
    original = ipaddress.ip_network

    def network(*args, **kwargs):
        networks.append(args)
        return original(*args, **kwargs)

    class FalseyConsumer:
        def __bool__(self):
            return False

        def __call__(self, row):
            observed.append(row)

    monkeypatch.setattr(ipaddress, 'ip_network', network)
    callback = None if consumer == 'none' else observed.append if consumer == 'callback' else FalseyConsumer()
    raw = gzip.decompress(source.read_bytes())
    with sqlite3.connect(':memory:') as database:
        epoch, prefixes, counts, peers, records, decoded = parse_rib(
            io.BytesIO(raw), database, observe=callback, retain_catalog=retain_catalog)
    assert (epoch, records, decoded) == (STAMP, 5, len(raw))
    assert {family: len(values) for family, values in prefixes.items()} == {'ipv4': 2, 'ipv6': 2}
    assert counts == {'ipv4': Counter({0: 1, 1: 2, 2: 1, 3: 1, 4: 1, 5: 2}),
                      'ipv6': Counter({0: 2, 1: 1, 2: 1, 3: 1, 4: 2, 5: 1})}
    assert peers[5] == dict(index=5, bgp_id='192.0.2.6', ip='2001:db8::6', asn=64501)
    assert observed == ([] if consumer == 'none' else expected)
    assert len(networks) == (0 if consumer == 'none' else 4)


@pytest.mark.parametrize('damage', ['zero_observations', 'duplicate_normalized_prefix', 'late_bad_path', 'unsupported'])
def test_shared_prefix_rejects_invalid_or_unsupported_records_without_admitting_a_candidate(tmp_path, damage):
    source, _ = shared_prefix_source(tmp_path)
    raw = bytearray(gzip.decompress(source.read_bytes()))
    first = 12 + struct.unpack_from('!I', raw, 8)[0]
    first_end = first + 12 + struct.unpack_from('!I', raw, first + 8)[0]
    if damage == 'zero_observations':
        # /25 的四字节前缀后为观察数；已有非空源不能掩盖后来的空记录。
        struct.pack_into('!H', raw, first + 12 + 9, 0)
    elif damage == 'duplicate_normalized_prefix':
        duplicate = bytearray(raw[first:first_end])
        duplicate[12 + 8] = 0x80  # 与原 ff 填充不同，但仍是同一 /25 网络。
        raw += duplicate
    elif damage == 'late_bad_path':
        # 前三条观察已成功，后续 AS_SET 声明两个 ASN 而只携带一个。
        at = raw.index(bytes.fromhex('010100003417'))
        raw[at + 1] = 2
    else:
        struct.pack_into('!H', raw, first + 6, 6)
    source.write_bytes(gzip.compress(raw, mtime=0))
    before = source.read_bytes()
    output, registry = tmp_path / 'rejected', tmp_path / 'registry'
    run = prepare(source, output)
    assert run.returncode == 1
    reason = {'zero_observations': '空 RIB Prefix', 'duplicate_normalized_prefix': '重复 Prefix 记录',
              'late_bad_path': 'AS_PATH 段类型或长度无效', 'unsupported': '仅支持 TABLE_DUMP_V2 单播'}[damage]
    assert reason in run.stderr
    assert not (output / 'manifest.json').exists()
    assert command('register', '--candidate', output, '--registry', registry).returncode == 1
    assert not registry.exists()
    assert source.read_bytes() == before
