"""两个已绑定RIB的离线AS_SEQUENCE端点对照；不生产连续状态或Web数据。"""
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timezone
import gzip
import hashlib
import ipaddress
from itertools import groupby
import json
import os
from pathlib import Path
import shutil
import sqlite3
import struct
import sys

from data_pipeline import rib_origin

RULE = 'rrc25-raw-peer-rib-endpoint/as-sequence-v1'
HEADER = struct.Struct('!IHHI')
U16 = struct.Struct('!H').unpack_from
STAT_FIELDS = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
STATUSES = ('same', 'different', 'left_only', 'right_only', 'not_comparable')


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _json(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode()


def _write(path, value):
    with path.open('xb') as writer:
        writer.write(_json(value))


def _plain(path):
    _require(not any(p.is_symlink() for p in (path, *path.parents)), '拒绝软链输入或输出路径')


def _prefix(body, subtype):
    _require(len(body) >= 7, 'RIB前缀帧截断')
    afi = 1 if subtype in (2, 8) else 2
    width = 4 if afi == 1 else 16
    bits = body[4]
    _require(bits <= width * 8, '非法Prefix长度')
    end = 5 + (bits + 7) // 8
    _require(end + 2 <= len(body), 'RIB前缀截断')
    packed = body[5:end].ljust(width, b'\0')
    _require(not bits % 8 or packed[(bits - 1)//8] & ((1 << (8 - bits % 8)) - 1) == 0,
             'Prefix含非零主机位')
    return afi, packed + bytes([bits]), end


def _index(source, spool, database, side):
    records = decoded = 0
    epoch = peers = None
    digest = hashlib.sha256()
    batch = []
    with gzip.open(source, 'rb') as stream, spool.open('xb') as writer:
        while True:
            header = stream.read(12)
            if not header:
                break
            _require(len(header) == 12, 'MRT头截断')
            stamp, kind, subtype, size = HEADER.unpack(header)
            _require(size <= 16 * 1024**2 and decoded + size + 12 <= 12 * 1024**3
                     and records < 2_000_000, 'MRT资源上限')
            body = stream.read(size)
            _require(len(body) == size, 'MRT帧截断')
            writer.write(header)
            writer.write(body)
            digest.update(header)
            digest.update(body)
            _require(kind == 13, '本规则只接受TABLE_DUMP_V2')
            _require(epoch is None or epoch == stamp, '单文件MRT时点不一致')
            epoch = stamp
            if subtype == 1:
                _require(records == 0 and peers is None, 'ambiguous_peer_table：多表作用域不准入')
                peers = rib_origin._peers(body)
            else:
                _require(peers is not None, '缺少首Peer表')
                _require(subtype in (2, 4, 8, 10), 'unsupported_rib_subtype：无法确定路由作用域')
                afi, prefix, _ = _prefix(body, subtype)
                batch.append((side, afi, prefix, records, decoded, size, subtype))
                if len(batch) == 10000:
                    database.executemany('INSERT INTO frames VALUES (?,?,?,?,?,?,?)', batch)
                    database.commit()
                    batch.clear()
                    print(json.dumps({'phase': 'index', 'side': side, 'records': records,
                                      'decoded_bytes': decoded}), file=sys.stderr, flush=True)
            decoded += 12 + size
            records += 1
    _require(peers is not None and records > 1, '无可解析RIB前缀帧')
    database.executemany('INSERT INTO frames VALUES (?,?,?,?,?,?,?)', batch)
    database.commit()
    return {'peers': peers, 'epoch': epoch, 'mrt_records': records, 'decoded_bytes': decoded,
            'decoded_sha256': digest.hexdigest(), 'gzip_eof': True}


def _path_value(path, as4, cache):
    key = (path, as4)
    if key in cache:
        return cache[key]
    if as4 is not None:
        rib_origin._segments(as4)
    segments = None if path is None else rib_origin._segments(path)
    reasons = []
    if path is None:
        reasons.append('missing_as_path')
    elif not segments:
        reasons.append('empty_as_path')
    else:
        names = {1: 'as_set', 3: 'confed_sequence', 4: 'confed_set'}
        reasons.extend(sorted({names[kind] for kind, _ in segments if kind != 2}))
    if as4 is not None:
        reasons.append('as4_path_present')
    value = (None if reasons else tuple(asn for _, asns in segments for asn in asns), tuple(reasons))
    if len(cache) >= 15000:
        cache.clear()
    # 很长的合法路径逐条解释，不让缓存乘以最大属性长度膨胀。
    if sum(len(raw) for raw in key if raw is not None) <= 512:
        cache[key] = value
    return value


def _entries(body, subtype, peers, cache):
    _, _, cursor = _prefix(body, subtype)
    count = U16(body, cursor)[0]
    cursor += 2
    for entry_index in range(count):
        addpath = subtype in (8, 10)
        width = 12 if addpath else 8
        _require(cursor + width <= len(body), 'RIB条目头截断')
        peer = U16(body, cursor)[0]
        size = U16(body, cursor + width - 2)[0]
        cursor += width
        _require(peer < len(peers), 'Peer表索引越界')
        end = cursor + size
        _require(end <= len(body), 'RIB属性截断')
        path = as4 = None
        seen = set()
        while cursor < end:
            _require(cursor + 3 <= end, '属性头截断')
            flags, code = body[cursor:cursor + 2]
            cursor += 2
            width = 2 if flags & 16 else 1
            _require(cursor + width <= end, '属性长度截断')
            length = U16(body, cursor)[0] if width == 2 else body[cursor]
            cursor += width
            _require(code not in seen and cursor + length <= end, '重复或截断属性')
            seen.add(code)
            if code == 2:
                path = body[cursor:cursor + length]
            elif code == 17:
                as4 = body[cursor:cursor + length]
            cursor += length
        canonical, reasons = _path_value(path, as4, cache)
        yield peer, entry_index, (canonical, reasons + (('add_path',) if addpath else ()))
    _require(cursor == len(body), 'RIB条目尾部多余字节')


def _groups(left, right):
    tables = []
    for source in (left, right):
        table = defaultdict(list)
        for peer in source['peers']:
            table[(peer['bgp_id'], peer['ip'], peer['asn'])].append(peer['index'])
        tables.append(table)
    groups = [{'id': n, 'bgp_id': key[0], 'ip': key[1], 'asn': key[2],
               'left_indexes': tables[0].get(key, []), 'right_indexes': tables[1].get(key, [])}
              for n, key in enumerate(sorted(tables[0].keys() | tables[1].keys()))]
    maps = [{index: group['id'] for group in groups for index in group[side + '_indexes']}
            for side in ('left', 'right')]
    return groups, maps


def _frame_groups(database, side):
    cursor = database.execute('SELECT afi,prefix,record,offset,size,subtype FROM frames WHERE side=? ORDER BY afi,prefix,record', (side,))
    for key, rows in groupby(cursor, lambda row: (row[0], row[1])):
        frames, total = [], 0
        for row in rows:
            total += row[4]
            _require(len(frames) < 1000 and total <= 32 * 1024**2, '同Prefix帧资源上限')
            frames.append(row)
        yield key, frames


def _collect(stream, frames, peer_map, peers, cache):
    result = defaultdict(list)
    count = 0
    for frame_index, (_, _, record, offset, size, subtype) in enumerate(frames):
        stream.seek(offset + 12)
        body = stream.read(size)
        _require(len(body) == size, '留存MRT复读截断')
        for peer, entry, value in _entries(body, subtype, peers, cache):
            count += 1
            _require(count <= 100000, '单端Prefix条目资源上限')
            result[peer_map[peer]].append(([frame_index, entry], value))
    return result


def _compare(database, output, left, right):
    groups, maps = _groups(left, right)
    _write(output / 'peer-groups.json', groups)
    iterators = [_frame_groups(database, side) for side in (0, 1)]
    current = [next(it, None) for it in iterators]
    counters = {afi: Counter({key: 0 for key in STATUSES}) for afi in (1, 2)}
    reason_counts = Counter()
    entry_counts = [Counter(), Counter()]
    cache = {}
    prefix_count = 0
    encoded_bytes = 0
    with (output / 'left.mrt').open('rb') as ls, (output / 'right.mrt').open('rb') as rs, \
            (output / 'comparisons.jsonl.gz').open('xb') as raw, \
            gzip.GzipFile(filename='', mode='wb', fileobj=raw, compresslevel=1, mtime=0) as writer:
        while any(item is not None for item in current):
            key = min(item[0] for item in current if item is not None)
            frames = [item[1] if item is not None and item[0] == key else [] for item in current]
            entries = [_collect(stream, rows, mapping, source['peers'], cache)
                       for stream, rows, mapping, source in zip((ls, rs), frames, maps, (left, right))]
            afi, prefix = key
            objects = []
            for side in (0, 1):
                entry_counts[side][afi] += sum(len(values) for values in entries[side].values())
                _require(sum(entry_counts[side].values()) <= 80_000_000, 'RIB条目总数上限')
            for group in sorted(entries[0].keys() | entries[1].keys()):
                lvalues, rvalues = (table.get(group, []) for table in entries)
                reasons = sorted({reason for _, (_, codes) in lvalues + rvalues for reason in codes})
                if len(lvalues) > 1 or len(rvalues) > 1:
                    reasons.append('duplicate_entry')
                ambiguous = any(len(groups[group][side + '_indexes']) > 1 for side in ('left', 'right'))
                if ambiguous:
                    reasons.append('ambiguous_peer_group')
                    status = 'not_comparable'
                elif not lvalues:
                    status = 'right_only'
                elif not rvalues:
                    status = 'left_only'
                else:
                    if reasons:
                        status = 'not_comparable'
                    else:
                        status = 'same' if lvalues[0][1][0] == rvalues[0][1][0] else 'different'
                counters[afi][status] += 1
                reason_counts.update(reasons)
                objects.append([group, status, [value[0] for value in lvalues], [value[0] for value in rvalues], reasons])
            row = {'afi': afi, 'safi': 1, 'prefix': str(ipaddress.ip_address(prefix[:-1])) + '/' + str(prefix[-1]),
                   'left_frames': [[r[2], r[3], r[5]] for r in frames[0]],
                   'right_frames': [[r[2], r[3], r[5]] for r in frames[1]], 'objects': objects}
            payload = _json(row)
            encoded_bytes += len(payload)
            _require(encoded_bytes <= 32 * 1024**3 and raw.tell() <= 4 * 1024**3, '比较输出资源上限')
            writer.write(payload)
            prefix_count += 1
            if prefix_count % 10000 == 0:
                print(json.dumps({'phase': 'compare', 'prefixes': prefix_count,
                                  'entries': [sum(c.values()) for c in entry_counts]}), file=sys.stderr, flush=True)
            for side in (0, 1):
                if frames[side]:
                    current[side] = next(iterators[side], None)
    total = {status: sum(c[status] for c in counters.values()) for status in STATUSES}
    comparable = total['same'] + total['different']
    return {'totals': total, 'families': {str(afi): dict(c) for afi, c in counters.items()},
            'reason_counts': dict(reason_counts), 'comparable_pairs': comparable,
            'different_fraction': total['different'] / comparable if comparable else None,
            'prefix_rows': prefix_count, 'source_entries': [dict(c) for c in entry_counts],
            'raw_peer_groups': len(groups), 'interval_change_count': None, 'session_continuity': 'unknown'}


def compare_ribs(left, right, left_sha, right_sha, output, profile):
    """公开离线生产接口；新目录最终manifest才表示本次完整成功。"""
    sources = [Path(left).absolute(), Path(right).absolute()]
    output = Path(output).absolute()
    for path in (*sources, output):
        _plain(path)
    _require(not output.exists(), '输出已存在，拒绝覆盖')
    _require(output.parent.is_dir(), '输出父目录不存在')
    code_paths = {'producer': Path(__file__), 'rib_decoder': Path(rib_origin.__file__), 'cli': Path(sys.argv[0])}
    code_hashes = {key: rib_origin._sha(path) for key, path in code_paths.items()}
    fingerprints = []
    for source, expected in zip(sources, (left_sha, right_sha)):
        _require(source.is_file() and source.stat().st_size <= 512 * 1024**2, '原文件非法或超限')
        before = source.stat()
        _require(rib_origin._sha(source) == expected, '原文件SHA不符')
        fingerprints.append(tuple(getattr(before, field) for field in STAT_FIELDS))
    output.mkdir(mode=0o700)
    try:
        inputs = []
        with closing(sqlite3.connect(output / 'frames.sqlite')) as database:
            database.execute('PRAGMA journal_mode=OFF')
            database.execute('PRAGMA synchronous=OFF')
            database.execute('PRAGMA cache_size=-32768')
            database.execute('PRAGMA temp_store=FILE')
            database.execute('PRAGMA max_page_count=524288')
            database.execute('CREATE TABLE frames(side INTEGER,afi INTEGER,prefix BLOB,record INTEGER,offset INTEGER,size INTEGER,subtype INTEGER)')
            for side, (name, source, expected) in enumerate(zip(('left', 'right'), sources, (left_sha, right_sha))):
                retained = output / (name + '.gz')
                with source.open('rb') as reader, retained.open('xb') as writer:
                    shutil.copyfileobj(reader, writer, 1024**2)
                _require(rib_origin._sha(retained) == expected, '留存副本SHA不符')
                result = _index(retained, output / (name + '.mrt'), database, side)
                result.update({'source_path': str(source), 'sha256': expected, 'compressed_bytes': retained.stat().st_size})
                stamp = datetime.fromtimestamp(result['epoch'], timezone.utc)
                _require(datetime.fromisoformat(profile['window_start']) <= stamp < datetime.fromisoformat(profile['window_end_exclusive'])
                         and stamp <= datetime.fromisoformat(profile['snapshot_time']), '时点不在数据配置范围')
                result['observed_at'] = stamp.isoformat().replace('+00:00', 'Z')
                inputs.append(result)
            _require(inputs[0]['epoch'] < inputs[1]['epoch'], '左时点须早于右时点')
            database.execute('CREATE INDEX frame_objects ON frames(side,afi,prefix,record)')
            database.commit()
            _require(database.execute('PRAGMA integrity_check').fetchone()[0] == 'ok', '定位索引损坏')
            summary = _compare(database, output, *inputs)
        summary.update({'schema_version': 'rib-path-comparison/v1', 'interpretation_version': RULE,
                        'data_profile': profile, 'collector_id': 'rrc25', 'coverage': 'unknown', 'inputs': inputs,
                        'denominator': '原始Peer属性组×AFI×SAFI×Prefix对象对；不是独立Prefix数',
                        'limits': ['仅两个RIB时点，不是区间变化次数、时刻、稳定性、异常或实际网络影响。',
                                   '原始Peer属性配对不是稳定身份或连续Session；单端缺项不等于网络消失。',
                                   '只比纯非空AS_SEQUENCE，保留prepend和私用值；其他属性变化不在范围内。'],
                        'references': 'comparisons每行objects=[group,status,left_refs,right_refs,reasons]；ref=[frame位置,entry序号]，零基。left/right_frames=[物理记录号,解压偏移,subtype]；原始路径及Originated Time在对应mrt，Peer表物理记录0。'})
        _write(output / 'summary.json', summary)
        for source, expected, fingerprint in zip(sources, (left_sha, right_sha), fingerprints):
            _require(rib_origin._sha(source) == expected and
                     tuple(getattr(source.stat(), field) for field in STAT_FIELDS) == fingerprint, '读取期间源文件变化')
        _require(all(rib_origin._sha(path) == code_hashes[key] for key, path in code_paths.items()), '读取期间代码变化')
        bindings = {path.name: {'sha256': rib_origin._sha(path), 'bytes': path.stat().st_size} for path in sorted(output.iterdir())}
        manifest = {'schema_version': 'rib-path-comparison-manifest/v1', 'interpretation_version': RULE,
                    'files': bindings, 'code': code_hashes, 'runtime': {'python': sys.version, 'sqlite': sqlite3.sqlite_version}}
        payload = _json(manifest)
        pending = output / 'manifest.pending'
        with pending.open('xb') as writer:
            writer.write(payload)
            writer.flush()
            os.fsync(writer.fileno())
        for path in output.iterdir():
            path.chmod(0o400)
        result = {'version': 'rib_path_comparison_v1_' + hashlib.sha256(payload).hexdigest(),
                  'manifest': str(output / 'manifest.json'), 'totals': summary['totals']}
        # 最终名称是唯一准入标志。保留同实体的pending名称，提交后不再做可能失败的清理。
        os.link(pending, output / 'manifest.json')
        return result
    except (OSError, EOFError, ValueError, sqlite3.Error, struct.error) as error:
        _write(output / 'failure.json', {'status': 'not_admitted', 'reason': str(error), 'sources': [str(s) for s in sources],
                                       'source_sha256': [left_sha, right_sha], 'code': code_hashes})
        raise
