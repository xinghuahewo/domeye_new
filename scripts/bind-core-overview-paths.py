#!/usr/bin/env python3
"""把两时点比较结果绑定为新的首页消费副本；只离线复读，不检测或覆盖旧输入。"""
import argparse
from collections import Counter
from contextlib import ExitStack
import gzip
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import signal
import struct
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))
from data_pipeline.consumption_files import copy_bound, read_manifest
from data_pipeline.core_overview_index import DailyIndex
from data_pipeline.core_overview_paths import RULE, STATUSES, read_comparison
from data_pipeline.core_overview_scale import read_scale, read_origin
from data_pipeline import rib_path_comparison as rib


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as reader:
        for chunk in iter(lambda: reader.read(1024 ** 2), b''):
            value.update(chunk)
    return value.hexdigest()


def plain(path):
    require(not any(p.is_symlink() for p in (path, *path.parents)), '不接受软链制品')


def save(path, value):
    data = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode()
    require(len(data) <= 65536, '消费摘要超限')
    with path.open('xb') as writer:
        writer.write(data)
    path.chmod(0o400)
    return hashlib.sha256(data).hexdigest()


def sample(row, item, groups, streams, inputs):
    group = groups[item[0]]
    value = {'family': 'ipv4' if row['afi'] == 1 else 'ipv6', 'prefix': row['prefix'],
             'peer': {key: group[key] for key in ('bgp_id', 'ip', 'asn')}}
    for side, refs, stream, source in zip(('left', 'right'), item[2:4], streams, inputs):
        require(len(refs) == 1 and len(group[side + '_indexes']) == 1, '样本对象不唯一')
        frame_index, entry_index = refs[0]
        record, offset, subtype = row[side + '_frames'][frame_index]
        stream.seek(offset)
        stamp, kind, observed_subtype, size = rib.HEADER.unpack(stream.read(12))
        require(kind == 13 and observed_subtype == subtype and stamp == source['epoch']
                and 0 < size <= 16 * 1024**2, '样本帧定位不符')
        body = stream.read(size)
        afi, prefix, _ = rib._prefix(body, subtype)
        require(len(body) == size and afi == row['afi']
                and str(ipaddress.ip_address(prefix[:-1])) + '/' + str(prefix[-1]) == row['prefix'], '样本前缀不符')
        entries = list(rib._entries(body, subtype, source['peers'], {}))
        peer, entry, (as_path, reasons) = entries[entry_index]
        require(entry == entry_index and peer == group[side + '_indexes'][0] and not reasons and as_path,
                '样本路径不可比')
        if len(as_path) > 256:
            return None
        value[side + '_path'] = list(as_path)
        value[side + '_reference'] = {'record': record, 'offset': offset, 'entry': entry, 'peer_index': peer}
    require(value['left_path'] != value['right_path'], '样本并非路径不同')
    return value


def retained_summary(folder, manifest, source_sha):
    summary = json.loads(read_manifest(folder / 'summary.json'))
    require(summary['schema_version'] == 'rib-path-comparison/v1' and summary['interpretation_version'] == RULE,
            '比较摘要版本不符')
    inputs = summary['inputs']
    require(len(inputs) == 2, '必须恰为两次观察')
    for side, source in zip(('left', 'right'), inputs):
        require(source['sha256'] == manifest['files'][side + '.gz']['sha256']
                and source['compressed_bytes'] == manifest['files'][side + '.gz']['bytes']
                and source['decoded_sha256'] == manifest['files'][side + '.mrt']['sha256']
                and source['decoded_bytes'] == manifest['files'][side + '.mrt']['bytes']
                and source['gzip_eof'] is True, '源RIB绑定不一致')
    groups = json.loads(read_manifest(folder / 'peer-groups.json'))
    require(groups == rib._groups(*inputs)[0], '原始Peer分组与两端表不符')
    counts = {afi: Counter({key: 0 for key in STATUSES}) for afi in (1, 2)}
    examples, sample_counts = [], Counter()
    prefix_rows = total_bytes = 0
    with ExitStack() as stack:
        streams = [stack.enter_context((folder / (side + '.mrt')).open('rb')) for side in ('left', 'right')]
        reader = stack.enter_context(gzip.open(folder / 'comparisons.jsonl.gz', 'rb'))
        while True:
            line = reader.readline(32 * 1024**2 + 1)
            if not line:
                break
            total_bytes += len(line)
            require(len(line) <= 32 * 1024**2 and total_bytes <= 32 * 1024**3, '比较正文超限')
            row = json.loads(line)
            afi = row['afi']
            require(afi in (1, 2) and row['safi'] == 1, '比较地址族不符')
            for item in row['objects']:
                group, status, left_refs, right_refs, reasons = item
                require(type(group) is int and 0 <= group < len(groups) and status in STATUSES, '比较对象无效')
                counts[afi][status] += 1
                if status == 'different' and sample_counts[afi] < 5:
                    require(not reasons, '可比较样本含歧义')
                    value = sample(row, item, groups, streams, inputs)
                    if value:
                        examples.append(value)
                        sample_counts[afi] += 1
            prefix_rows += 1
            require(prefix_rows <= 4_000_000, '比较行数超限')
    totals = {key: counts[1][key] + counts[2][key] for key in STATUSES}
    require(prefix_rows == summary['prefix_rows'] and totals == summary['totals']
            and all(counts[afi] == summary['families'][str(afi)] for afi in counts), '全量比较分类复读不等')
    require(sum(totals.values()) <= 160_000_000, '对象总数超限')
    return {key: summary[key] for key in ('data_profile', 'collector_id', 'coverage', 'session_continuity',
            'interval_change_count', 'interpretation_version', 'limits')} | {
        'schema_version': 'core-rib-path-summary/v1', 'comparison_version': 'rib_path_comparison_v1_' + source_sha,
        'left': {key: inputs[0][key] for key in ('observed_at', 'sha256')},
        'right': {key: inputs[1][key] for key in ('observed_at', 'sha256')},
        'families': {'ipv4': dict(counts[1]), 'ipv6': dict(counts[2]), 'all': totals}, 'examples': examples}


def copy_index_packages(index, manifest, output):
    source = index.path.parent
    for entry in [*index.days.values(), *index.diagnostics.values()]:
        path = source / entry['file']
        plain(path)
        require(path.parent == source and path.stat().st_size <= 4 * 1024**3, '日文件路径或大小不符')
        copy_bound(path, output / entry['file'], entry['sha256'])
    for name in ('scale', 'origin'):
        if name not in manifest:
            continue
        folder = source / name
        binding = manifest[name]
        require(binding['file'] == name + '/manifest.json', '旧消费包路径不符')
        package = json.loads(read_manifest(folder / 'manifest.json'))
        (output / name).mkdir(mode=0o700)
        for entry in [package['summary'], *package['evidence'].values()]:
            file = Path(entry['file'])
            require(not file.is_absolute() and '..' not in file.parts, '旧证据路径越界')
            path = folder / file
            plain(path)
            require(path.stat().st_size <= 4 * 1024**3, '旧证据超限')
            destination = output / name / file
            destination.parent.mkdir(parents=True, exist_ok=True)
            copy_bound(path, destination, entry['sha256'])
        copy_bound(folder / 'manifest.json', output / name / 'manifest.json', binding['sha256'])
    if 'scale' in manifest:
        prefix, _ = read_scale(output / 'manifest.json', manifest)
        if 'origin' in manifest:
            read_origin(output / 'manifest.json', manifest, prefix)


def bind(args):
    args.index, args.comparison, args.output = (path.absolute() for path in (args.index, args.comparison, args.output))
    for path in (args.index, args.comparison, args.output):
        plain(path)
    original, raw = read_manifest(args.index), read_manifest(args.comparison)
    index = DailyIndex(args.index, original)
    manifest, source = json.loads(original), json.loads(raw)
    require('path_comparison' not in manifest, '请从尚未绑定路径的版本追加')
    require(not args.output.exists() and not args.output.is_relative_to(args.index.parent)
            and not args.output.is_relative_to(args.comparison.parent), '必须使用独立新目录')
    require(source['schema_version'] == 'rib-path-comparison-manifest/v1' and source['interpretation_version'] == RULE
            and set(source['files']) == {'left.gz','right.gz','left.mrt','right.mrt','frames.sqlite',
                                         'peer-groups.json','comparisons.jsonl.gz','summary.json'}, '比较包结构不符')
    code_paths = [Path(__file__), ROOT/'backend/data_pipeline/core_overview_paths.py',
                  ROOT/'backend/data_pipeline/consumption_files.py', ROOT/'backend/data_pipeline/core_overview_index.py',
                  ROOT/'backend/data_pipeline/core_overview_scale.py', Path(rib.__file__), Path(rib.rib_origin.__file__)]
    code = {str(path.relative_to(ROOT)): digest(path) for path in code_paths}
    require(source['code']['producer'] == digest(Path(rib.__file__))
            and source['code']['rib_decoder'] == digest(Path(rib.rib_origin.__file__))
            and source['code']['cli'] == digest(ROOT/'scripts/compare-rib-paths.py'), '比较生产代码未绑定到当前可复读版本')
    fingerprints = {}
    for name, entry in source['files'].items():
        path = args.comparison.parent / name
        plain(path)
        before = path.stat()
        require(type(entry['bytes']) is int and 0 < entry['bytes'] <= 12 * 1024**3
                and before.st_size == entry['bytes'] and digest(path) == entry['sha256'], '源证据摘要或大小不符：' + name)
        fingerprints[name] = tuple(getattr(before, field) for field in rib.STAT_FIELDS)
    source_sha = hashlib.sha256(raw).hexdigest()
    summary = retained_summary(args.comparison.parent, source, source_sha)
    args.output.mkdir(mode=0o700)
    folder = args.output / 'paths'
    folder.mkdir(mode=0o700)
    package = {'schema_version': 'core-rib-path-package/v1', 'source_manifest_sha256': source_sha,
               'binding_code_sha256': code[str(Path(__file__).relative_to(ROOT))],
               'summary': {'file': 'summary.json', 'sha256': save(folder/'summary.json', summary)},
               'evidence': {'file': 'source-manifest.json', 'sha256': source_sha}, 'code_sha256': code,
               'verification': '八个源文件完整摘要与全部分类计数复读；路径值只对有界样本逐帧复读。'}
    copy_bound(args.comparison, folder/'source-manifest.json', source_sha)
    manifest['path_comparison'] = {'file': 'paths/manifest.json', 'sha256': save(folder/'manifest.json', package)}
    _, path_version, _ = read_comparison(args.output/'manifest.json', manifest)
    copy_index_packages(index, manifest, args.output)
    manifest['evidence'] = {**manifest.get('evidence', {}), 'path_binding': {'parent_version': index.version, 'code_sha256': code}}
    for name, fingerprint in fingerprints.items():
        require(tuple(getattr((args.comparison.parent/name).stat(), field) for field in rib.STAT_FIELDS) == fingerprint,
                '复读期间证据变化')
    require(read_manifest(args.index) == original and read_manifest(args.comparison) == raw
            and all(digest(path) == code[str(path.relative_to(ROOT))] for path in code_paths), '绑定期间清单或代码变化')
    pending = args.output/'manifest.pending'
    save(pending, manifest)
    candidate = DailyIndex(args.output/'manifest.json', read_manifest(pending))
    result = {'manifest': str(args.output/'manifest.json'), 'version': candidate.version,
              'parent_version': index.version, 'path_version': path_version, 'sample_count': len(summary['examples'])}
    os.link(pending, args.output/'manifest.json')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--index', type=Path, required=True)
    parser.add_argument('--comparison', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    def timeout(*_):
        raise TimeoutError('路径绑定超过300秒上限')
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(300)
    try:
        print(json.dumps(bind(args), ensure_ascii=False))
    except (OSError, EOFError, ValueError, KeyError, TypeError, IndexError, RuntimeError, struct.error) as error:
        parser.exit(1, '路径绑定失败：' + str(error) + '\n')
    finally:
        signal.alarm(0)


if __name__ == '__main__':
    main()
