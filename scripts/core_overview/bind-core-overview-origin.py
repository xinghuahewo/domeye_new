#!/usr/bin/env python3
"""给已绑定前缀规模的首页另建起源消费版本；不覆盖，不读取MRT或真实数据库。"""
import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'backend'))
from data_pipeline.common.verified_files import copy_bound, read_manifest
from data_pipeline.overview.index import DailyIndex
from data_pipeline.overview.scale import read_origin, read_scale


def require(value, message):
    if not value:
        raise ValueError(message)


def copy_package(source, destination, binding, maximum, package_root):
    require(binding['file'] == source.name
            and source.resolve() == package_root.resolve() / source.relative_to(package_root)
            and source.stat().st_size <= maximum
            and ('bytes' not in binding or binding['bytes'] == source.stat().st_size), '制品越界或超限')
    copy_bound(source, destination / source.name, binding['sha256'])


def verify_members(folder, summary):
    path = folder / 'origins.json'
    require(path.stat().st_size <= 8 * 1024 * 1024, 'ASN集合超限')
    members = json.loads(path.read_bytes())
    require(set(members) == {'ipv4', 'ipv6', 'all'}, 'ASN集合地址族不符')
    rules = summary['rules']
    for values in members.values():
        require(isinstance(values, list) and len(values) <= 4_000_000
                and all(type(asn) is int and 0 < asn < 4294967295
                        and asn not in rules['stop_special_asns']
                        and not any(start <= asn <= end for start, end in rules['skip_ranges_inclusive']) for asn in values), 'ASN集合包含非法或排除值')
        require(values == sorted(set(values)), 'ASN集合必须排序去重')
    with closing(sqlite3.connect((folder / 'paths.sqlite').as_uri() + '?mode=ro', uri=True)) as database:
        database.execute('PRAGMA query_only=ON')
        database.execute('PRAGMA trusted_schema=OFF')
        require(database.execute('PRAGMA integrity_check').fetchall() == [('ok',)], '原路径目录损坏')
        invalid = database.execute("SELECT 1 FROM paths WHERE typeof(ipv4_count)!='integer' OR typeof(ipv6_count)!='integer' "
                                   "OR ipv4_count<0 OR ipv6_count<0 OR ipv4_count+ipv6_count<=0 "
                                   "OR (attributed_origin_asn IS NOT NULL AND typeof(attributed_origin_asn)!='integer') LIMIT 1").fetchone()
        require(invalid is None, '路径目录条目类型或分母无效')
        for family in ('ipv4', 'ipv6'):
            column = family + '_count'
            rows = [row[0] for row in database.execute(f'SELECT DISTINCT attributed_origin_asn FROM paths WHERE {column}>0 AND attributed_origin_asn IS NOT NULL ORDER BY attributed_origin_asn')]
            require(rows == members[family] and len(rows) == summary['families'][family]['visible_origin_ases'], '起源集合与目录不符')
            total, unknown = database.execute(f'SELECT sum({column}),sum(CASE WHEN attributed_origin_asn IS NULL THEN {column} ELSE 0 END) FROM paths').fetchone()
            metric = summary['families'][family]
            require((total or 0) == metric['rib_entries'] and (unknown or 0) == metric['unattributed_entries'], '路径条目分母不符')
    require(members['all'] == sorted(set(members['ipv4']) | set(members['ipv6']))
            and len(members['all']) == summary['families']['all']['visible_origin_ases'], '双栈必须按集合并集')


def bind(args):
    code_paths = [Path(__file__), ROOT / 'backend/data_pipeline/common/verified_files.py',
                  ROOT / 'backend/data_pipeline/overview/scale.py', ROOT / 'backend/data_pipeline/overview/index.py']
    code_hashes = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths}
    original = read_manifest(args.index)
    origin_bytes = read_manifest(args.origin)
    index = DailyIndex(args.index, original)
    manifest = json.loads(original)
    require('scale' in manifest and 'origin' not in manifest, '仅对已有前缀且未绑定起源的版本追加')
    prefix, _ = read_scale(args.index, manifest)
    output = args.output.resolve()
    require(not args.output.is_symlink() and not output.exists()
            and not output.is_relative_to(args.index.parent.resolve())
            and not output.is_relative_to(args.origin.parent.resolve()), '输出必须是独立新目录')
    output.mkdir(mode=0o700)
    (output / 'origin').mkdir(mode=0o700)
    origin_manifest = json.loads(origin_bytes)
    require(origin_manifest['summary']['file'] == 'summary.json'
            and set(origin_manifest['evidence']) == {'origins.json', 'peers.json', 'paths.sqlite'}, '起源包结构不符')
    for name, entry in [('summary.json', origin_manifest['summary']), *origin_manifest['evidence'].items()]:
        require(entry['file'] == name, '起源包文件绑定不符')
        copy_package(args.origin.parent / name, output / 'origin', entry, 4 * 1024 ** 3 if name == 'paths.sqlite' else 8 * 1024 * 1024, args.origin.parent)
    origin_sha = hashlib.sha256(origin_bytes).hexdigest()
    copy_bound(args.origin, output / 'origin/manifest.json', origin_sha)
    manifest['origin'] = {'file': 'origin/manifest.json', 'sha256': origin_sha}
    origin_summary, origin_version = read_origin(output / 'manifest.json', manifest, prefix)
    verify_members(output / 'origin', origin_summary)
    (output / 'scale/evidence').mkdir(mode=0o700, parents=True)
    scale_folder = args.index.parent / 'scale'
    scale_manifest = json.loads(read_manifest(scale_folder / 'manifest.json'))
    copy_bound(scale_folder / 'manifest.json', output / 'scale/manifest.json', manifest['scale']['sha256'])
    copy_package(scale_folder / 'summary.json', output / 'scale', scale_manifest['summary'], 65536, args.index.parent)
    for name, entry in scale_manifest['evidence'].items():
        require(entry['file'] == 'evidence/' + name, '前缀证据路径不符')
        copy_package(scale_folder / 'evidence' / name, output / 'scale/evidence', {**entry, 'file': name}, 8 * 1024 * 1024, args.index.parent)
    for entry in [*index.days.values(), *index.diagnostics.values()]:
        copy_package(args.index.parent / entry['file'], output, entry, 4 * 1024 ** 3, args.index.parent)
    require(read_manifest(args.index) == original and read_manifest(args.origin) == origin_bytes, '绑定期间输入清单变化')
    require(all(hashlib.sha256(path.read_bytes()).hexdigest() == code_hashes[str(path.relative_to(ROOT))] for path in code_paths), '绑定期间执行代码变化')
    manifest['evidence'] = {**manifest.get('evidence', {}), 'origin_binding': {
        'parent_version': index.version, 'code_sha256': code_hashes}}
    payload = (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()
    require(len(payload) <= 65536, '新首页清单超限')
    candidate = DailyIndex(output / 'manifest.json', payload)
    pending = output / 'manifest.pending'
    with pending.open('xb') as writer:
        writer.write(payload)
        writer.flush()
        os.fsync(writer.fileno())
    pending.chmod(0o400)
    os.link(pending, output / 'manifest.json')
    pending.unlink()
    return {'manifest': str(output / 'manifest.json'), 'version': candidate.version,
            'parent_version': index.version, 'origin_version': origin_version,
            'families': {name: metric['visible_origin_ases'] for name, metric in origin_summary['families'].items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--index', type=Path, required=True)
    parser.add_argument('--origin', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    def timeout(*_):
        raise TimeoutError('起源绑定超过300秒上限')
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(300)
    try:
        print(json.dumps(bind(args), ensure_ascii=False))
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, sqlite3.Error) as error:
        parser.exit(1, '起源绑定失败：' + str(error) + '\n')
    finally:
        signal.alarm(0)


if __name__ == '__main__':
    main()
