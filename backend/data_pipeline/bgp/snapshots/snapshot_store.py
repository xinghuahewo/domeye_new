"""单RIB共享制品的核验、显式登记及只读打开；不导入生产器或初始化业务库。"""
from contextlib import contextmanager
from datetime import datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[4]
VERSION_PATTERN = r'rib_snapshot_v1_[0-9a-f]{64}'
MAX_DATABASE_BYTES = 4 * 1024 ** 3
FAMILIES = ('all', 'ipv4', 'ipv6')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode()


def sha_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def profile():
    return json.loads((ROOT / 'config/data-profile.json').read_bytes())


def business_date(observed_at):
    require(isinstance(observed_at, str)
            and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', observed_at), '快照时点须为UTC秒级字符串')
    settings = profile()
    stamp = datetime.fromisoformat(observed_at.replace('Z', '+00:00'))
    require(stamp.tzinfo is not None and datetime.fromisoformat(settings['window_start'].replace('Z', '+00:00')) <= stamp
            < datetime.fromisoformat(settings['window_end_exclusive'].replace('Z', '+00:00'))
            and stamp <= datetime.fromisoformat(settings['snapshot_time'].replace('Z', '+00:00')), '快照不在数据档允许范围')
    return stamp.astimezone(ZoneInfo(settings['timezone'])).date().isoformat()


def validate_date(day):
    settings = profile()
    parsed = datetime.strptime(day, '%Y-%m-%d').date().isoformat()
    start = datetime.strptime(day, '%Y-%m-%d').replace(tzinfo=ZoneInfo(settings['timezone']))
    require(parsed == day and start + timedelta(days=1) > datetime.fromisoformat(settings['window_start'].replace('Z', '+00:00'))
            and start < datetime.fromisoformat(settings['window_end_exclusive'].replace('Z', '+00:00'))
            and start <= datetime.fromisoformat(settings['snapshot_time'].replace('Z', '+00:00')), '日期不在数据档允许范围')
    return day


def read_json(path, maximum=1024 * 1024):
    require(not path.is_symlink() and path.is_file() and path.stat().st_size <= maximum, '清单缺失或超过限制')
    value = json.loads(path.read_bytes())
    require(isinstance(value, dict), '清单须为JSON对象')
    return value


@contextmanager
def read_database(path):
    require(not path.is_symlink() and path.is_file(), '快照查询投影缺失')
    database = sqlite3.connect(path.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
    database.row_factory = sqlite3.Row
    try:
        database.execute('PRAGMA query_only=ON')
        yield database
    finally:
        database.close()


def metrics(database, family):
    where, args = ('', ()) if family == 'all' else (' WHERE family=?', (family,))
    prefixes = database.execute('SELECT count(*) FROM prefixes' + where, args).fetchone()[0]
    attributed = database.execute('SELECT count(*) FROM (SELECT DISTINCT family,prefix FROM origins' + where + ')', args).fetchone()[0]
    origins = database.execute('SELECT count(DISTINCT asn) FROM origins' + where, args).fetchone()[0]
    entries = database.execute('SELECT count(*),coalesce(sum(attributed_origin_asn IS NULL),0) FROM observation_facts' + where, args).fetchone()
    return {'visible_prefixes': prefixes, 'visible_origin_ases': origins, 'attributed_prefixes': attributed,
            'unattributed_prefixes': prefixes - attributed, 'rib_entries': entries[0], 'unattributed_entries': entries[1]}


def logical_digest(database, summary):
    digest = hashlib.sha256(encode(summary))
    # 规范化表逐行散列，不重复展开数千万条路径／Peer全文。
    for table, order in (('prefixes', 'physical_record'), ('peers', 'peer_index'),
                         ('path_dictionary', 'path_id'), ('observations', 'physical_record,entry_index'),
                         ('origins', 'family,prefix,asn')):
        digest.update(encode(table))
        for row in database.execute(f'SELECT * FROM {table} ORDER BY {order}'):
            digest.update(encode([value.hex() if isinstance(value, bytes) else value for value in row]))
    return digest.hexdigest()


def candidate_manifest(directory, manifest_name='manifest.json'):
    require(not directory.is_symlink(), '快照目录不能是符号链接')
    manifest = read_json(directory / manifest_name)
    require(manifest['schema_version'] == 'rib-snapshot-manifest/v1', '快照清单版本不支持')
    summary = manifest['summary']
    version = 'rib_snapshot_v1_' + hashlib.sha256(encode(summary['identity'])).hexdigest()
    require(manifest['version'] == version, '快照结果身份不一致')
    require(summary['date'] == business_date(summary['observed_at']), '快照日期不一致')
    identity = summary['identity']
    require(identity['observed_at'] == summary['observed_at']
            and identity['source_sha256'] == summary['source']['sha256']
            and identity['collector_id'] == summary['source']['collector_id'] == 'rrc25'
            and identity['origin_rule'] == summary['origin_rule'] == 'rib-attributed-origin/private-skip-v1'
            and identity['scope'] == ['TABLE_DUMP_V2/IPv4/unicast', 'TABLE_DUMP_V2/IPv6/unicast']
            and identity['projection_rule'] == 'rib-prefix-origin/v1'
            and summary['source']['coverage'] == 'unknown' and summary['gzip_eof'] is True,
            '快照正文与身份依据不一致')
    return manifest


def validate_candidate(directory, manifest_name='manifest.json'):
    manifest = candidate_manifest(directory, manifest_name)
    summary = manifest['summary']
    binding = manifest['database']
    path = directory / 'snapshot.sqlite'
    before = file_stamp(path)
    require(not path.is_symlink() and path.is_file() and 0 < path.stat().st_size <= MAX_DATABASE_BYTES
            and path.stat().st_size == binding['bytes'] and sha_file(path) == binding['sha256'], '快照查询投影摘要不匹配')
    with read_database(path) as database:
        require(database.execute('PRAGMA integrity_check').fetchone()[0] == 'ok', '快照查询投影损坏')
        for projection, observation in (
            ('SELECT family,prefix FROM prefixes', 'SELECT family,prefix FROM observation_facts'),
            ('SELECT family,prefix,asn FROM origins',
             'SELECT family,prefix,attributed_origin_asn FROM observation_facts WHERE attributed_origin_asn IS NOT NULL'),
        ):
            require(database.execute(f'SELECT * FROM ({projection} EXCEPT {observation}) LIMIT 1').fetchone() is None
                    and database.execute(f'SELECT * FROM ({observation} EXCEPT {projection}) LIMIT 1').fetchone() is None,
                    'Prefix起源投影与原始观察不一致')
        require(database.execute('SELECT count(*) FROM observations').fetchone()[0]
                == database.execute('SELECT count(*) FROM observation_facts').fetchone()[0], '观察关联缺项')
        require(all(metrics(database, family) == summary['families'][family] for family in FAMILIES), '快照统计不闭合')
        require(logical_digest(database, summary) == manifest['logical_sha256'], '快照逻辑正文冲突')
    require(file_stamp(path) == before, '核验期间查询投影变化')
    return manifest


def read_registry(root):
    require(not root.is_symlink(), '登记位置不能是符号链接')
    index = read_json(root / 'registry.json')
    require(index['schema_version'] == 'rib-snapshot-registry/v1', '登记版本不支持')
    require(isinstance(index['versions'], dict) and isinstance(index['dates'], dict), '登记结构无效')
    for version, item in index['versions'].items():
        require(re.fullmatch(VERSION_PATTERN, version) and validate_date(item['date']), '登记身份无效')
    for day, version in index['dates'].items():
        require(validate_date(day) and version in index['versions'] and index['versions'][version]['date'] == day, '默认快照登记不一致')
    batches = index.get('batches', {})
    require(isinstance(batches, dict) and isinstance(index.get('batch_dates', {}), dict), '批次登记结构无效')
    for batch_id, report in batches.items():
        require(batch_id == report['selection_id'] == 'rib_batch_v1_' + hashlib.sha256(encode(report['identity'])).hexdigest()
                and report['state'] == 'committed' and report['coverage'] in ('complete', 'gaps')
                and 1 <= len(report['days']) <= 31, '批次登记身份或状态无效')
        for row in report['days']:
            require(validate_date(row['date']) and row['state'] in ('available', 'missing_input', 'validation_failed'), '批次日期结果无效')
            if row['state'] == 'available':
                require(row['version'] in index['versions'] and index['versions'][row['version']]['date'] == row['date'], '批次结果版本不一致')
    for day, batch_id in index.get('batch_dates', {}).items():
        require(batch_id in batches and sum(row['date'] == day for row in batches[batch_id]['days']) == 1, '最近批次日期指向无效')
    return index


def file_stamp(path):
    require(not path.is_symlink() and path.is_file(), '已登记文件缺失或类型改变')
    stat = path.stat()
    return {key: getattr(stat, key) for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')}


def check_registered_database(root, index, version):
    require(file_stamp(root / 'versions' / version / 'snapshot.sqlite') == index['versions'][version]['database_stat'],
            '已登记查询文件发生变化，需离线重新核验')


def read_registered(root, index, version):
    entry = index['versions'][version]
    directory = root / 'versions' / version
    # manifest有固定大小上限；登记中的摘要和文件实体戳来自离线完整核验。
    manifest = candidate_manifest(directory)
    require(sha_file(directory / 'manifest.json') == entry['manifest_sha256'], '已登记清单发生变化')
    check_registered_database(root, index, version)
    require(manifest['version'] == version
            and all(manifest['summary'][key] == entry[key] for key in ('date', 'observed_at')),
            '登记版本、日期或时点与快照不一致')
    return manifest


def stage_version(candidate, root, index):
    """在持有登记锁时安装完整版本；仅更新内存索引，尚不可发现。"""
    manifest = validate_candidate(candidate)
    version, day = manifest['version'], manifest['summary']['date']
    if version in index['versions']:
        previous = read_registered(root, index, version)
        require(previous['logical_sha256'] == manifest['logical_sha256'], '相同结果身份出现不同逻辑正文')
        return version
    versions = root / 'versions'
    versions.mkdir(exist_ok=True)
    destination = versions / version
    if destination.exists():
        previous = validate_candidate(destination)
        require(previous['version'] == version and previous['logical_sha256'] == manifest['logical_sha256'],
                '相同结果身份出现不同逻辑正文')
    else:
        with tempfile.TemporaryDirectory(prefix='.register-', dir=root) as scratch:
            copied = Path(scratch) / 'result'
            copied.mkdir()
            for name in ('manifest.json', 'snapshot.sqlite', 'execution.json'):
                shutil.copyfile(candidate / name, copied / name)
            require(validate_candidate(copied) == manifest, '安装期间候选投影或清单变化')
            os.rename(copied, destination)
    for name in ('manifest.json', 'snapshot.sqlite', 'execution.json'):
        with (destination / name).open('rb') as reader:
            os.fsync(reader.fileno())
    index['versions'][version] = {'date': day, 'observed_at': manifest['summary']['observed_at'],
                                 'manifest_sha256': sha_file(destination / 'manifest.json'),
                                 'database_stat': file_stamp(destination / 'snapshot.sqlite')}
    return version


@contextmanager
def registry_transaction(root):
    require(not root.is_symlink(), '登记位置不能是符号链接')
    root.mkdir(parents=True, exist_ok=True)
    with (root / '.register.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        index = read_registry(root) if (root / 'registry.json').exists() else {
            'schema_version': 'rib-snapshot-registry/v1', 'versions': {}, 'dates': {}}
        yield index


def commit_registry(root, index):
    payload = encode(index)
    require(len(payload) <= 1024 * 1024, '登记索引超过1MiB上限')
    with tempfile.NamedTemporaryFile(prefix='.registry-', dir=root, delete=False) as writer:
        temporary = Path(writer.name)
        try:
            writer.write(payload)
            writer.flush()
            os.fsync(writer.fileno())
            os.replace(temporary, root / 'registry.json')
        finally:
            temporary.unlink(missing_ok=True)


def register(candidate, root):
    """普通重复登记保持现有默认；批次显式选择走独立提交。"""
    manifest = validate_candidate(candidate)
    version, day = manifest['version'], manifest['summary']['date']
    with registry_transaction(root) as index:
        existed = version in index['versions']
        stage_version(candidate, root, index)
        if not existed:
            index['dates'][day] = version
            commit_registry(root, index)
    return {'version': version, 'date': day, 'state': 'registered'}


def register_batch(candidates, root, report, verify_execution):
    """整批核验后一次提交；同一选择重试不能回滚后来默认。"""
    with registry_transaction(root) as index:
        previous = index.get('batches', {}).get(report['selection_id'])
        if previous:
            require(previous == report, '相同批次选择身份出现不同结果')
            for day in report['days']:
                if day['state'] == 'available':
                    read_registered(root, index, day['version'])
            verify_execution()
            return
        for day in report['days']:
            if day['state'] == 'available':
                version = stage_version(candidates[day['version']], root, index)
                index['dates'][day['date']] = version
        index.setdefault('batches', {})[report['selection_id']] = report
        for day in report['days']:
            index.setdefault('batch_dates', {})[day['date']] = report['selection_id']
            if day['state'] == 'available':
                read_registered(root, index, day['version'])
        verify_execution()
        commit_registry(root, index)
