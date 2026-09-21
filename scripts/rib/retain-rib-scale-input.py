#!/usr/bin/env python3
"""将明确选定的双路完整RIB核验留存为小型消费包；不是通用MRT解析器。

仅离线执行；不连接数据库、不执行证据目录中的程序、不改源文件。
前缀集合取自已选定的完整核验并交叉对账；本命令不重新解释AS_PATH。
"""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import signal


ROOT = Path(__file__).resolve().parents[2]
REPORTS = ('candidate-0800-proof.json', 'candidate-0800-frames.json', 'candidate-0800-receipt.json')
CODES = ('candidate-0800-executed.py', 'candidate_frame_census.py')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


def natural(value, maximum=80_000_000):
    return type(value) is int and 0 <= value <= maximum


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'JSON重复字段：' + key)
        result[key] = value
    return result


def source_digest(path):
    before = path.stat()
    require(path.is_file() and 0 < before.st_size <= 512 * 1024 * 1024, '源文件大小无效')
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    decoded = 0
    with gzip.open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            decoded += len(block)
            require(decoded <= 12 * 1024 ** 3, '源文件解压超过上限')
    require(before == path.stat(), '源文件在读取中变化')
    return digest.hexdigest(), before.st_size, decoded


def retain(args):
    folder, source, output = args.evidence_dir.resolve(), args.source.resolve(), args.output.resolve()
    require(not args.output.is_symlink() and not output.exists(), '不得覆盖已有输出或选择输出符号链接')
    require(not output.is_relative_to(folder) and not source.is_relative_to(output), '输出必须独立于证据目录和原文件')
    raw = {}
    for name in (*REPORTS, *CODES):
        path = folder / name
        require(path.is_file() and 0 < path.stat().st_size <= 8 * 1024 * 1024, '证据文件缺失或过大：' + name)
        with path.open('rb') as stream:
            raw[name] = stream.read(8 * 1024 * 1024 + 1)
        require(len(raw[name]) <= 8 * 1024 * 1024, '证据读取超过上限')
    require(sha(raw[REPORTS[0]]) == args.audit_sha256 and sha(raw[REPORTS[1]]) == args.frames_sha256, '指定核验摘要不匹配')
    audit, frames, receipt = [json.loads(raw[name], object_pairs_hook=unique_object) for name in REPORTS]
    require(audit['complete_stream'] is True and audit['sample_only'] is False
            and audit['gzip_read_to_eof'] is True and frames['gzip_eof'] is True, '必须是完整文件核验')
    require(type(receipt['exit_code']) is int and receipt['exit_code'] == 0 and receipt['output_sha256'] == args.audit_sha256
            and receipt['executed_code_sha256'] == sha(raw[CODES[0]])
            and frames['script_sha256'] == sha(raw[CODES[1]]), '执行回执或程序摘要不符')
    require(audit['view_name'] == 'rrc25' and audit['collector_bgp_id'] == '0.0.0.25', 'Collector身份不符')
    require(len(audit['timestamp_counts']) == len(frames['mrt_epoch_counts']) == 1, '不是单一文件时点')
    stamp = datetime.fromisoformat(next(iter(audit['timestamp_counts'])).replace('Z', '+00:00'))
    require(stamp.tzinfo is not None and stamp.utcoffset().total_seconds() == 0 and stamp.microsecond == 0, '文件时点必须是UTC整秒')
    profile = json.loads((ROOT / 'config/data-profile.json').read_bytes())
    require(datetime.fromisoformat(profile['window_start']) <= stamp < datetime.fromisoformat(profile['window_end_exclusive'])
            and stamp <= datetime.fromisoformat(profile['snapshot_time']), '文件时点不在允许的数据范围内')
    require(natural(audit['mrt_records'], 2_000_000) and natural(frames['mrt_records'], 2_000_000)
            and natural(audit['decompressed_bytes'], 12 * 1024 ** 3) and natural(frames['decoded_bytes'], 12 * 1024 ** 3)
            and frames['mrt_epoch_counts'] == {str(int(stamp.timestamp())): audit['mrt_records']}
            and list(audit['timestamp_counts'].values()) == [audit['mrt_records']]
            and audit['mrt_records'] == frames['mrt_records']
            and audit['decompressed_bytes'] == frames['decoded_bytes'], '双路时间或完整记录数不符')
    source_sha, source_bytes, decoded = source_digest(source)
    require(source_sha == audit['compressed_sha256'] == frames['source_sha256']
            and source_bytes == audit['identity_before']['bytes']
            and audit['identity_before'] == audit['identity_after']
            and audit['source_path'] == frames['source']
            and decoded == audit['decompressed_bytes'], '原文件与双路核验绑定不符')
    peers = audit['peers']
    require(isinstance(peers, list) and 0 < len(peers) <= 65535 and natural(frames['peer_table_count'], 65535)
            and len(peers) == frames['peer_table_count']
            and all(type(peer['index']) is int and peer['index'] == index for index, peer in enumerate(peers)), 'Peer表数量或位置不符')
    families = {}
    for key, label in (('4', 'ipv4'), ('6', 'ipv6')):
        left, right = audit['families'][key], frames['families'][key]
        for report in (left, right):
            require(natural(report['prefix_count'], 2_000_000)
                    and isinstance(report['prefix_set_sha256'], str) and re.fullmatch('[0-9a-f]{64}', report['prefix_set_sha256'])
                    and isinstance(report['peer_entry_counts'], dict), 'Prefix集合声明无效')
            require(all(isinstance(index, str) and re.fullmatch('0|[1-9][0-9]{0,4}', index)
                        and int(index) < len(peers) and natural(count) and count > 0
                        for index, count in report['peer_entry_counts'].items()), 'Peer位置或条目数无效')
            require(all(natural(report['statistics'][field]) for field in ('rib_records', 'rib_entries'))
                    and all(type(report['statistics'].get(field, 0)) is int and report['statistics'].get(field, 0) == 0
                            for field in ('duplicate_prefix_records', 'zero_entry_records', 'duplicate_peer_entries_within_record')), '结构异常不能准入')
        for field in ('prefix_count', 'prefix_set_sha256', 'peer_entry_counts'):
            require(left[field] == right[field], '双路前缀集合或Peer条目不符')
        for field in ('rib_records', 'rib_entries', 'duplicate_prefix_records', 'zero_entry_records', 'duplicate_peer_entries_within_record'):
            require(left['statistics'].get(field, 0) == right['statistics'].get(field, 0), '双路结构计数不符')
        require(left['prefix_count'] == left['statistics']['rib_records'], '存在重复或空前缀记录')
        require(sum(left['peer_entry_counts'].values()) == left['statistics']['rib_entries'], 'Peer条目总数不闭合')
        families[label] = {'visible_prefixes': left['prefix_count'], 'prefix_set_sha256': left['prefix_set_sha256'],
                           'rib_entries': left['statistics']['rib_entries'], 'peer_indices': sorted(map(int, left['peer_entry_counts'])),
                           'visible_origin_ases': None}
    families['all'] = {'visible_prefixes': families['ipv4']['visible_prefixes'] + families['ipv6']['visible_prefixes'],
                       'rib_entries': families['ipv4']['rib_entries'] + families['ipv6']['rib_entries'],
                       'peer_indices': sorted(set(families['ipv4']['peer_indices']) | set(families['ipv6']['peer_indices'])),
                       'visible_origin_ases': None}
    require(audit['mrt_records'] == families['all']['visible_prefixes'] + 1
            and audit['counts']['combined_prefixes'] == families['all']['visible_prefixes']
            and audit['counts']['ipv4_prefixes'] == families['ipv4']['visible_prefixes']
            and audit['counts']['ipv6_prefixes'] == families['ipv6']['visible_prefixes'], '汇总数量不闭合')
    summary = {'schema_version': 'core-rib-scale/v1', 'interpretation_version': 'rib-prefix-union/v1',
               'data_profile': profile, 'observed_at': stamp.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z'),
               'source': {'collector_id': 'rrc25', 'collector_bgp_id': audit['collector_bgp_id'], 'view_name': audit['view_name'],
                          'coverage': 'unknown', 'path': audit['source_path'], 'sha256': source_sha, 'compressed_bytes': source_bytes},
               'families': families, 'origin_metric_state': 'pending_definition',
               'limits': ['单RIB自身时点，不是配置快照时点或连续RouteState。',
                          '跨本文件Peer位置合并Prefix；位置不是稳定Peer Identity，条目数不是Prefix数。',
                          '起源AS口径尚待确认，暂不输出起源指标；不改变AS_SET及私用／保留原值。',
                          '两份完整报告交叉核验Prefix集合与逐Peer条目；本命令校验gzip EOF，不重新解释MRT或全量AS_PATH。']}
    payload = encoded(summary)
    require(len(payload) <= 65536, '消费摘要过大')
    manifest = {'schema_version': 'core-rib-scale-manifest/v1', 'summary': {'file': 'summary.json', 'sha256': sha(payload)},
                'source_local_path': str(source), 'evidence': {name: {'file': 'evidence/' + name, 'sha256': sha(value)} for name, value in raw.items()},
                'retainer_sha256': sha(Path(__file__).read_bytes())}
    manifest_payload = encoded(manifest)
    require(len(manifest_payload) <= 65536, '消费清单过大')
    output.mkdir(mode=0o700)
    (output / 'evidence').mkdir(mode=0o700)
    for name, value in raw.items():
        with (output / 'evidence' / name).open('xb') as stream:
            stream.write(value)
        (output / 'evidence' / name).chmod(0o400)
    with (output / 'summary.json').open('xb') as stream:
        stream.write(payload)
    (output / 'summary.json').chmod(0o400)
    with (output / 'manifest.json').open('xb') as stream:
        stream.write(manifest_payload)
    (output / 'manifest.json').chmod(0o400)
    return {'manifest': str(output / 'manifest.json'), 'version': 'rib_scale_v1_' + sha(manifest_payload),
            'observed_at': summary['observed_at'], 'visible_prefixes': families['all']['visible_prefixes'], 'visible_origin_ases': None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--audit-sha256', required=True)
    parser.add_argument('--frames-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    def timeout(*_):
        raise TimeoutError('留存核验超过120秒上限')
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(120)
    try:
        print(json.dumps(retain(args), ensure_ascii=False))
    except (OSError, EOFError, ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
        parser.exit(1, 'RIB消费留存失败：' + str(error) + '\n')
    finally:
        signal.alarm(0)


if __name__ == '__main__':
    main()
