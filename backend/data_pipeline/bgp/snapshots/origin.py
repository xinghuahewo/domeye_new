"""单文件 RRC25 TABLE_DUMP_V2 单播起源归属统计；仅供显式离线命令。

保留 AS_PATH 原字节与段，另算归属；不装载旧应用、不重建路由状态。
不支持的 MRT 类别拒绝整包；不能明确归属的路径保留原因，不猜测 ASN。
"""
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import gzip
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import struct
import sys
import sqlite3


RULE = 'rib-attributed-origin/private-skip-v1'
SKIP_RANGES = ((64512, 65535), (4200000000, 4294967294))
STOP_SPECIAL_ASNS = (0, 23456, 4294967295)
HEADER = struct.Struct('!IHHI')
ENTRY = struct.Struct('!HIH')
U16 = struct.Struct('!H').unpack_from
MAX_DECODED = 12 * 1024 ** 3


def require(condition, message):
    if not condition:
        raise ValueError(message)


class InputRejected(ValueError):
    """只用于已经确认的源输入拒绝；执行、资源或写入错误不属于此类。"""
    def __init__(self, message, code='invalid_mrt', stage='mrt'):
        super().__init__(message)
        self.code, self.stage = code, stage

    def detail(self):
        return {'code': self.code, 'stage': self.stage, 'message': str(self)}


class DeclarationConflict(ValueError):
    pass


def input_require(condition, message):
    if not condition:
        raise InputRejected(message)


def _sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _segments(path):
    segments = []
    cursor = 0
    while path is not None and cursor < len(path):
        input_require(cursor + 2 <= len(path), 'AS_PATH 段头截断')
        kind, count = path[cursor:cursor + 2]
        cursor += 2
        input_require(kind in (1, 2, 3, 4) and count > 0 and cursor + count * 4 <= len(path), 'AS_PATH 段类型或长度无效')
        values = struct.unpack_from('!' + 'I' * count, path, cursor)
        cursor += 4 * count
        segments.append([kind, list(values)])
    return segments


def _interpret(path, as4):
    segments = _segments(path)
    if as4 is not None:
        # 不采纳归属仍须验证段结构，不能用unknown绕过损坏输入。
        # RFC6793 §6：至少一个4字节ASN与2字节段头，属性长度必须为偶数。
        input_require(len(as4) >= 6 and len(as4) % 2 == 0, 'AS4_PATH 属性长度无效')
        _segments(as4)
    raw = segments[-1][1][-1] if segments and segments[-1][0] == 2 else None
    result = {'as_path_hex': path.hex() if path is not None else None,
              'as4_path_hex': as4.hex() if as4 is not None else None,
              'segments': segments, 'raw_origin_asn': raw, 'attributed_origin_asn': None}
    if as4 is not None:
        return {**result, 'reason': 'as4_path_requires_separate_rule'}
    if path is None or not segments:
        return {**result, 'reason': 'missing_or_empty_path'}
    skipped = False
    for kind, values in reversed(segments):
        if kind != 2:
            return {**result, 'reason': 'as_set_ambiguous' if kind == 1 else 'confederation_ambiguous'}
        for value in reversed(values):
            # 沿用既有归属排除区间，其中65535是保留值，不称作私用ASN。
            if any(start <= value <= end for start, end in SKIP_RANGES):
                skipped = True
                continue
            if value in STOP_SPECIAL_ASNS:
                return {**result, 'reason': 'special_asn_not_attributable'}
            return {**result, 'attributed_origin_asn': value,
                    'reason': 'private_as_skipped' if skipped else 'explicit_terminal_asn'}
    return {**result, 'reason': 'only_excluded_asns'}


def _peers(body):
    input_require(len(body) >= 8, 'Peer 表截断')
    name_len = U16(body, 4)[0]
    cursor = 6 + name_len
    input_require(cursor + 2 <= len(body), 'Peer 表名称截断')
    if body[:4] != b'\x00\x00\x00\x19' or body[6:cursor] != b'rrc25':
        raise DeclarationConflict('Collector 不匹配')
    count = U16(body, cursor)[0]
    cursor += 2
    peers = []
    for index in range(count):
        input_require(cursor < len(body), 'Peer 条目截断')
        flags = body[cursor]
        require(flags & ~3 == 0, 'Peer flags 不支持')
        width = 16 if flags & 1 else 4
        asn_width = 4 if flags & 2 else 2
        end = cursor + 5 + width + asn_width
        input_require(end <= len(body), 'Peer 条目长度无效')
        peers.append({'index': index, 'bgp_id': str(ipaddress.ip_address(body[cursor + 1:cursor + 5])),
                      'ip': str(ipaddress.ip_address(body[cursor + 5:cursor + 5 + width])),
                      'asn': int.from_bytes(body[cursor + 5 + width:end], 'big')})
        cursor = end
    input_require(cursor == len(body) and peers, 'Peer 表尾部或数量无效')
    return peers


def parse_rib(stream, database, observe=None, retain_catalog=True, expected_epoch=None):
    """流式核验单RIB；可在同次解析中接收逐条原始观察及起源解释。"""
    prefixes = {'ipv4': set(), 'ipv6': set()}
    peer_counts = {'ipv4': Counter(), 'ipv6': Counter()}
    catalog = {}
    catalog_bytes = 0
    if retain_catalog:
        database.execute('''CREATE TABLE paths (
        path_key BLOB, as4_key BLOB, raw_origin_asn INTEGER, attributed_origin_asn INTEGER,
        reason TEXT NOT NULL, ipv4_count INTEGER NOT NULL, ipv6_count INTEGER NOT NULL,
        physical_record INTEGER, decoded_offset INTEGER, entry_index INTEGER, peer_index INTEGER,
        originated_time_epoch INTEGER, family TEXT, PRIMARY KEY (path_key, as4_key)) WITHOUT ROWID''')
    def flush():
        nonlocal catalog_bytes
        if retain_catalog:
            database.executemany('''INSERT INTO paths VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(path_key,as4_key) DO UPDATE SET
            ipv4_count=ipv4_count+excluded.ipv4_count, ipv6_count=ipv6_count+excluded.ipv6_count''',
            ((keys[0], keys[1], row['raw_origin_asn'], row['attributed_origin_asn'], row['reason'],
              row['entry_counts']['ipv4'], row['entry_counts']['ipv6'], *row['first_occurrence'].values())
             for keys, row in catalog.items()))
        database.commit()
        require(database.execute('PRAGMA page_count').fetchone()[0] * 4096 <= 4 * 1024 ** 3, '路径目录超过4GiB')
        catalog.clear()
        catalog_bytes = 0
    peers = []
    epoch = None
    offset = records = entries = 0
    progress = 128 * 1024 * 1024
    while True:
        header = stream.read(12)
        if not header:
            break
        input_require(len(header) == 12, 'MRT 头截断')
        stamp, kind, subtype, size = HEADER.unpack(header)
        require(kind == 13 and subtype in (1, 2, 4), '仅支持 TABLE_DUMP_V2 单播')
        require(size <= 16 * 1024 * 1024 and offset + 12 + size <= MAX_DECODED and records < 2_000_000, 'MRT 读取超限')
        if expected_epoch is not None and stamp != expected_epoch:
            raise DeclarationConflict('声明时点与实际RIB不一致')
        epoch = stamp if epoch is None else epoch
        input_require(stamp == epoch, '不是单一快照时点')
        body = stream.read(size)
        input_require(len(body) == size, 'MRT 正文截断')
        if subtype == 1:
            input_require(records == 0, '重复或非首条 Peer 表')
            peers = _peers(body)
        else:
            input_require(peers and size >= 7, 'RIB 在 Peer 表前或条目截断')
            family, bits_max = ('ipv4', 32) if subtype == 2 else ('ipv6', 128)
            bits = body[4]
            width = (bits + 7) // 8
            input_require(bits <= bits_max and 7 + width <= size, 'Prefix 长度无效')
            raw_prefix = body[5:5 + width]
            # RFC6396 §4.3.2：尾随填充位无意义；仅规范化网络键，原源与定位不变。
            if bits % 8:
                raw_prefix = raw_prefix[:-1] + bytes([raw_prefix[-1] & (0xff << (8 - bits % 8))])
            key = bytes([bits]) + raw_prefix.ljust(bits_max // 8, b'\0')
            input_require(key not in prefixes[family], '重复 Prefix 记录')
            prefixes[family].add(key)
            count = U16(body, 5 + width)[0]
            input_require(count > 0, '空 RIB Prefix')
            cursor = 7 + width
            seen = set()
            prefix_text = None
            for entry_index in range(count):
                input_require(cursor + 8 <= size, 'RIB 条目头截断')
                peer, originated, length = ENTRY.unpack_from(body, cursor)
                cursor += 8
                end = cursor + length
                input_require(peer < len(peers) and peer not in seen and end <= size, 'Peer 重复、越界或属性截断')
                seen.add(peer)
                peer_counts[family][peer] += 1
                entries += 1
                require(entries <= 80_000_000, 'RIB 条目数超限')
                path = as4 = None
                codes = set()
                while cursor < end:
                    input_require(cursor + 3 <= end, '属性头截断')
                    flags, code = body[cursor:cursor + 2]
                    cursor += 2
                    input_require(code not in codes, '重复 BGP 属性')
                    codes.add(code)
                    if flags & 16:
                        input_require(cursor + 2 <= end, '扩展属性长度截断')
                        size_attr = U16(body, cursor)[0]
                        cursor += 2
                    else:
                        size_attr = body[cursor]
                        cursor += 1
                    input_require(cursor + size_attr <= end, '属性正文截断')
                    if code == 2:
                        path = body[cursor:cursor + size_attr]
                    elif code == 17:
                        as4 = body[cursor:cursor + size_attr]
                    cursor += size_attr
                # 前导0表示缺属性，前导1表示原属性字节，区分缺失与空值。
                identity = (b'\0' if path is None else b'\1' + path, b'\0' if as4 is None else b'\1' + as4)
                item = catalog.get(identity)
                if item is None:
                    if len(catalog) >= 30_000 or catalog_bytes >= 32 * 1024 * 1024:
                        flush()
                    item = _interpret(path, as4)
                    item['entry_counts'] = {'ipv4': 0, 'ipv6': 0}
                    item['first_occurrence'] = {'physical_record': records, 'decoded_offset': offset,
                                                'entry_index': entry_index, 'peer_index': peer,
                                                'originated_time_epoch': originated, 'family': family}
                    catalog[identity] = item
                    catalog_bytes += len(identity[0]) + len(identity[1])
                item['entry_counts'][family] += 1
                if observe is not None:
                    if prefix_text is None:
                        prefix_text = str(ipaddress.ip_network((ipaddress.ip_address(key[1:]), bits)))
                    observe({'family': family, 'prefix': prefix_text, 'safi': 1,
                        'peer': peers[peer], 'physical_record': records, 'decoded_offset': offset,
                        'entry_index': entry_index, 'originated_time_epoch': originated,
                        'as_path_hex': item['as_path_hex'], 'as4_path_hex': item['as4_path_hex'],
                        'raw_origin_asn': item['raw_origin_asn'],
                        'attributed_origin_asn': item['attributed_origin_asn'], 'reason': item['reason']})
            input_require(cursor == size, 'RIB 尾部多余字节')
        offset += 12 + size
        records += 1
        if offset >= progress:
            print(json.dumps({'decoded_bytes': offset, 'mrt_records': records, 'rib_entries': entries}), file=sys.stderr, flush=True)
            progress += 128 * 1024 * 1024
    input_require(peers and records > 1, '没有完整 RIB 内容')
    flush()
    return epoch, prefixes, peer_counts, peers, records, offset


def retain_origins(source, expected_sha, output, profile):
    """读一份显式指定的原文件，成功后另建绑定摘要／集合／原路径目录。"""
    code_paths = {'producer_sha256': Path(__file__), 'cli_sha256': Path(sys.argv[0])}
    code_hashes = {name: _sha(path) for name, path in code_paths.items()}
    require(not source.is_symlink() and source.is_file(), '源必须是普通文件')
    require(not output.exists() and not output.is_symlink() and not source.resolve().is_relative_to(output.resolve()), '输出必须独立且不存在')
    before = source.stat()
    require(0 < before.st_size <= 512 * 1024 * 1024 and _sha(source) == expected_sha, '原文件大小或摘要不匹配')
    output.mkdir(mode=0o700, parents=True)
    # 仅新建离线派生文件；失败目录没有manifest，不能消费。不触碰真实数据库。
    database = sqlite3.connect(output / 'paths.sqlite')
    database.execute('PRAGMA page_size=4096')
    database.execute('PRAGMA journal_mode=OFF')
    database.execute('PRAGMA synchronous=OFF')
    database.execute('PRAGMA cache_size=-32768')
    database.execute('PRAGMA temp_store=FILE')
    try:
        with gzip.open(source, 'rb') as stream:
            epoch, prefixes, peer_counts, peers, records, decoded = parse_rib(stream, database)
        require(database.execute('PRAGMA integrity_check').fetchall() == [('ok',)], '路径目录完整性失败')
    finally:
        database.close()
    require(_sha(source) == expected_sha, '读取期间原文件变化')
    after = source.stat()
    require(all(getattr(before, key) == getattr(after, key)
                for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')), '读取期间原文件变化')
    stamp = datetime.fromtimestamp(epoch, timezone.utc)
    require(datetime.fromisoformat(profile['window_start']) <= stamp < datetime.fromisoformat(profile['window_end_exclusive'])
            and stamp <= datetime.fromisoformat(profile['snapshot_time']), '快照不在配置范围')
    families, origins = {}, {}
    with closing(sqlite3.connect((output / 'paths.sqlite').resolve().as_uri() + '?mode=ro', uri=True)) as database:
        distinct_paths = database.execute('SELECT count(*) FROM paths').fetchone()[0]
        for family in ('ipv4', 'ipv6'):
            column = family + '_count'  # 仅来自上面固定字面列表，非用户SQL输入。
            origins[family] = [row[0] for row in database.execute(
                f'SELECT DISTINCT attributed_origin_asn FROM paths WHERE {column}>0 AND attributed_origin_asn IS NOT NULL ORDER BY attributed_origin_asn')]
            reasons = dict(database.execute(f'SELECT reason,sum({column}) FROM paths WHERE {column}>0 GROUP BY reason'))
            unknown = database.execute(f'SELECT coalesce(sum({column}),0) FROM paths WHERE attributed_origin_asn IS NULL').fetchone()[0]
            require(sum(reasons.values()) == sum(peer_counts[family].values()), '路径统计与RIB条目不闭合')
            families[family] = {'visible_origin_ases': len(origins[family]), 'unattributed_entries': unknown,
                                'visible_prefixes': len(prefixes[family]), 'rib_entries': sum(peer_counts[family].values()),
                                'prefix_set_sha256': hashlib.sha256(b''.join(sorted(prefixes[family]))).hexdigest(),
                                'peer_entry_counts': dict(sorted(peer_counts[family].items())), 'reasons': dict(sorted(reasons.items()))}
    origins['all'] = sorted(set(origins['ipv4']) | set(origins['ipv6']))
    families['all'] = {'visible_origin_ases': len(origins['all']),
                       **{key: sum(families[f][key] for f in ('ipv4', 'ipv6'))
                          for key in ('visible_prefixes', 'rib_entries', 'unattributed_entries')}}
    summary = {'schema_version': 'core-rib-origin/v1', 'interpretation_version': RULE,
               'source': {'sha256': expected_sha, 'compressed_bytes': before.st_size, 'collector_id': 'rrc25', 'coverage': 'unknown'},
               'observed_at': stamp.isoformat().replace('+00:00', 'Z'), 'data_profile': profile, 'families': families,
               'mrt_records': records, 'decoded_bytes': decoded, 'gzip_eof': True, 'distinct_paths': distinct_paths,
               'path_storage': 'paths.sqlite：path_key/as4_key首字节0为缺失、1为原始属性字节；其余为四字节ASN段编码。',
               'rules': {'skip_ranges_inclusive': SKIP_RANGES,
                         'reserved_legacy_exclusion': [65535], 'stop_special_asns': STOP_SPECIAL_ASNS,
                         'ambiguity': '不拆集合；向前遇集合或联盟段即停止；不跨段猜测；AS4_PATH另待规则'},
               'limits': ['单RIB文件自身时点；不是日末或连续状态。', '计数为按确认规则可明确归属的ASN并集；不完整代表所有网络。',
                          '私用区间沿用旧归属实现并额外排除保留值65535；原始路径与末端另存。']}
    def encode(value):
        return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode()
    payload = encode(summary)
    require(len(payload) <= 65536, '消费摘要超限')
    bindings = {}
    for name, value in [('summary.json', payload), ('origins.json', encode(origins)), ('peers.json', encode(peers))]:
        with (output / name).open('xb') as writer:
            writer.write(value)
        bindings[name] = {'file': name, 'sha256': hashlib.sha256(value).hexdigest(), 'bytes': len(value)}
    bindings['paths.sqlite'] = {'file': 'paths.sqlite', 'sha256': _sha(output / 'paths.sqlite'),
                                'bytes': (output / 'paths.sqlite').stat().st_size}
    require(all(_sha(path) == code_hashes[name] for name, path in code_paths.items()), '读取期间执行代码变化')
    manifest = {'schema_version': 'core-rib-origin-manifest/v1', 'summary': bindings.pop('summary.json'),
                'evidence': bindings, 'source_local_path': str(source.resolve()),
                **code_hashes, 'runtime': {'python': sys.version, 'sqlite': sqlite3.sqlite_version}}
    manifest_bytes = encode(manifest)
    pending = output / 'manifest.pending'
    with pending.open('xb') as writer:
        writer.write(manifest_bytes)
        writer.flush()
        os.fsync(writer.fileno())
    for path in output.iterdir():
        path.chmod(0o400)
    # 硬链接原子建立最终名称，且目标已存在时拒绝，不覆盖其他结果。
    os.link(pending, output / 'manifest.json')
    pending.unlink()  # 本次自行创建的清单临时名；最终同一实体已保留。
    return {'manifest': str((output / 'manifest.json').resolve()),
            'version': 'rib_origin_v1_' + hashlib.sha256(manifest_bytes).hexdigest(),
            'families': {f: metric['visible_origin_ases'] for f, metric in families.items()}}
