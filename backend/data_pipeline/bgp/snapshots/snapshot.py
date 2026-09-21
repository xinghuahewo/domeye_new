"""显式准备单RIB共享快照；路径仅用于定位，不参与结果身份。"""
from datetime import datetime, timezone
import gzip
import hashlib
import os
from pathlib import Path
import sqlite3
import struct
import sys
import uuid
import zlib
import stat

from data_pipeline.bgp.snapshots import origin as rib_origin
from data_pipeline.bgp.snapshots.snapshot_store import business_date, encode, logical_digest, metrics, require, sha_file, validate_candidate


class VerifiedGzip:
    def __init__(self, stream):
        self.stream = stream

    def read(self, size):
        try:
            return self.stream.read(size)
        except (gzip.BadGzipFile, EOFError, zlib.error) as error:
            raise rib_origin.InputRejected(str(error), 'invalid_gzip', 'gzip') from error


def implementation_paths():
    return (Path(__file__), Path(rib_origin.__file__), Path(__file__).with_name('snapshot_store.py'), Path(sys.argv[0]))


def implementation_binding():
    return {path.name: sha_file(path) for path in implementation_paths()}


def prepare(source, expected_sha, collector, observed_at, output, max_database_mib=4096):
    require(1 <= max_database_mib <= 4096, '查询投影资源上限须在1—4096 MiB')
    require(collector == 'rrc25', '仅支持RRC25')
    day = business_date(observed_at)
    code_paths = implementation_paths()
    implementation = implementation_binding()
    try:
        before = source.lstat()
    except FileNotFoundError as error:
        raise rib_origin.InputRejected('输入文件缺失', 'missing_input', 'source') from error
    require(stat.S_ISREG(before.st_mode), '输入必须是普通文件')
    require(before.st_size <= 512 * 1024 ** 2, '输入超过512MiB资源上限')
    if sha_file(source) != expected_sha:
        raise rib_origin.InputRejected('输入摘要不匹配', 'sha256_mismatch', 'source')
    require(not output.exists() and not output.is_symlink(), '候选输出必须不存在')
    output.mkdir(parents=True, mode=0o700)
    database_path = output / 'snapshot.sqlite'
    database = sqlite3.connect(database_path)
    try:
        database.executescript('''PRAGMA page_size=4096; PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF; PRAGMA cache_size=-32768; PRAGMA temp_store=FILE;
            CREATE TABLE observations (physical_record INTEGER, entry_index INTEGER, peer_index INTEGER NOT NULL,
                originated_time_epoch INTEGER NOT NULL, path_id INTEGER NOT NULL,
                PRIMARY KEY(physical_record,entry_index)) WITHOUT ROWID;
            CREATE TABLE prefixes (physical_record INTEGER PRIMARY KEY, family TEXT NOT NULL,
                prefix TEXT NOT NULL, decoded_offset INTEGER NOT NULL, UNIQUE(family,prefix));
            CREATE TABLE peers (peer_index INTEGER PRIMARY KEY, bgp_id TEXT, ip TEXT, asn INTEGER);
            CREATE TABLE path_dictionary (path_id INTEGER PRIMARY KEY, digest BLOB UNIQUE NOT NULL,
                path_key BLOB NOT NULL, as4_key BLOB NOT NULL, raw_origin_asn INTEGER,
                attributed_origin_asn INTEGER, reason TEXT NOT NULL);
            CREATE TABLE origins (family TEXT, prefix TEXT, asn INTEGER, PRIMARY KEY(family,prefix,asn)) WITHOUT ROWID;
            CREATE VIEW observation_facts AS SELECT o.*,p.family,p.prefix,p.decoded_offset,
                d.raw_origin_asn,d.attributed_origin_asn,d.reason,d.path_key,d.as4_key,
                peer.bgp_id,peer.ip,peer.asn AS peer_asn
                FROM observations o JOIN prefixes p USING(physical_record)
                JOIN path_dictionary d USING(path_id) JOIN peers peer USING(peer_index);
        ''')
        database.execute(f'PRAGMA max_page_count={max_database_mib * 256}')
        path_cache = {}
        cache_bytes = 0
        last_record = None
        seen_peers = set()
        record_origins = set()

        def observe(row):
            nonlocal last_record, cache_bytes
            keys = tuple(b'\0' if row[key] is None else b'\1' + bytes.fromhex(row[key])
                         for key in ('as_path_hex', 'as4_path_hex'))
            path_id = path_cache.get(keys)
            if path_id is None:
                digest = hashlib.sha256(struct.pack('!I', len(keys[0])) + keys[0] + keys[1]).digest()
                previous = database.execute('SELECT path_id,path_key,as4_key FROM path_dictionary WHERE digest=?', (digest,)).fetchone()
                if previous:
                    require(tuple(previous[1:]) == keys, '路径摘要相同但原文不同')
                    path_id = previous[0]
                else:
                    path_id = database.execute('''INSERT INTO path_dictionary
                        (digest,path_key,as4_key,raw_origin_asn,attributed_origin_asn,reason) VALUES (?,?,?,?,?,?)''',
                        (digest, *keys, row['raw_origin_asn'], row['attributed_origin_asn'], row['reason'])).lastrowid
                if len(path_cache) >= 30_000 or cache_bytes >= 32 * 1024 ** 2:
                    path_cache.clear()
                    cache_bytes = 0
                path_cache[keys] = path_id
                cache_bytes += sum(map(len, keys))
            if last_record != row['physical_record']:
                database.execute('INSERT INTO prefixes VALUES (?,?,?,?)',
                                 (row['physical_record'], row['family'], row['prefix'], row['decoded_offset']))
                last_record = row['physical_record']
                record_origins.clear()
            peer = row['peer']
            if peer['index'] not in seen_peers:
                database.execute('INSERT INTO peers VALUES (?,?,?,?)', (peer['index'], peer['bgp_id'], peer['ip'], peer['asn']))
                seen_peers.add(peer['index'])
            database.execute('INSERT INTO observations VALUES (?,?,?,?,?)', (
                row['physical_record'], row['entry_index'], peer['index'], row['originated_time_epoch'], path_id))
            origin = row['attributed_origin_asn']
            if origin is not None and origin not in record_origins:
                database.execute('INSERT OR IGNORE INTO origins VALUES (?,?,?)',
                                 (row['family'], row['prefix'], origin))
                record_origins.add(origin)

        try:
            with gzip.open(source, 'rb') as stream:
                epoch, _, _, _, records, decoded = rib_origin.parse_rib(VerifiedGzip(stream), database, observe,
                    retain_catalog=False, expected_epoch=int(datetime.fromisoformat(observed_at.replace('Z', '+00:00')).timestamp()))
        except rib_origin.InputRejected:
            require(sha_file(source) == expected_sha and all(getattr(source.stat(), key) == getattr(before, key)
                    for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')), '读取期间输入变化')
            require(all(sha_file(path) == implementation[path.name] for path in code_paths), '执行期间代码发生变化')
            raise
        database.execute('CREATE INDEX origins_by_asn ON origins(asn,family,prefix)')
        actual = datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace('+00:00', 'Z')
        require(actual == observed_at, '声明时点与实际RIB不一致')
        require(sha_file(source) == expected_sha and all(getattr(source.stat(), key) == getattr(before, key)
                for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')), '读取期间输入变化')
        identity = {'source_sha256': expected_sha, 'collector_id': collector, 'observed_at': actual,
                    'scope': ['TABLE_DUMP_V2/IPv4/unicast', 'TABLE_DUMP_V2/IPv6/unicast'],
                    'origin_rule': rib_origin.RULE, 'projection_rule': 'rib-prefix-origin/v1',
                    'implementation': implementation, 'reference_versions': {}}
        summary = {'identity': identity, 'observed_at': actual, 'date': day,
                   'source': {'sha256': expected_sha, 'collector_id': collector, 'coverage': 'unknown'},
                   'origin_rule': rib_origin.RULE, 'unit': 'distinct_prefix_and_origin_asn',
                   'families': {family: metrics(database, family) for family in ('all', 'ipv4', 'ipv6')},
                   'mrt_records': records, 'decoded_bytes': decoded, 'gzip_eof': True,
                   'limitations': ['单RIB时点，不是整日或连续RouteState；Session为Unknown。',
                                   '仅RRC25控制面观察，覆盖Unknown；不推断实际断网、用户影响或原因。',
                                   '多起源前缀保留每个明确归属，ASN前缀数之和不等于总体并集。']}
        logical_sha = logical_digest(database, summary)
        database.commit()
    finally:
        database.close()
    version = 'rib_snapshot_v1_' + hashlib.sha256(encode(identity)).hexdigest()
    manifest = {'schema_version': 'rib-snapshot-manifest/v1', 'version': version,
                'summary': summary, 'logical_sha256': logical_sha,
                'database': {'sha256': sha_file(database_path), 'bytes': database_path.stat().st_size}}
    (output / 'execution.json').write_bytes(encode({'execution_id': str(uuid.uuid4()),
        'source_local_path': str(source.resolve()), 'finished_at': datetime.now(timezone.utc).isoformat(),
        'version': version, 'max_database_mib': max_database_mib,
        'python': sys.version, 'sqlite': sqlite3.sqlite_version}))
    # 完整核验后原子建立完成标志；写入中断不会留下半写的manifest.json。
    pending = output / 'manifest.pending'
    with pending.open('xb') as writer:
        writer.write(encode(manifest))
        writer.flush()
        os.fsync(writer.fileno())
    validate_candidate(output, 'manifest.pending')
    require(all(sha_file(path) == implementation[path.name] for path in code_paths), '执行期间代码发生变化')
    os.link(pending, output / 'manifest.json')
    pending.unlink()
    return {'version': version, 'candidate': str(output.resolve()), 'state': 'prepared'}
