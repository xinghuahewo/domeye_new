"""按日的离线消费索引。Web 只打开既有文件；生产入口只供离线命令调用。"""

from contextlib import closing, contextmanager
from datetime import datetime, timedelta
from functools import lru_cache
import hashlib
import json
import re
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from data_pipeline.common.event_records import serialize_record
from data_pipeline.overview.input import InputError, SUPPORTED_KIND_SETS, SUPPORTED_INTERPRETATIONS, stream_index_input, overview_item, overview_search_text, overview_level_filter
from data_pipeline.overview.diagnostics import compile_diagnostics, read_diagnostic, validate_diagnostic


def _digest(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(path):
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _single_file(path):
    # 仅允许离线闭合的单文件 SQLite。WAL/热日志不能成为未绑定输入。
    if any(Path(str(path) + suffix).exists() for suffix in ('-wal', '-shm', '-journal')):
        raise ValueError('索引存在未绑定的日志或共享内存文件')
    with path.open('rb') as stream:
        header = stream.read(20)
    if header[:16] != b'SQLite format 3\x00' or header[18:20] != b'\x01\x01':
        raise ValueError('仅允许闭合的非 WAL 单文件索引')


@lru_cache(maxsize=128)
def _verify(path, identity, expected):
    # 仅缓存同一文件实体的摘要检查；不是不可变快照或观测完整性的证明。
    if _digest(path) != expected or _identity(path) != identity:
        raise ValueError('按日索引摘要不一致或检查期间发生变化')


def build_index(inputs, output, diagnostics=()):
    """从已留存单日包建立新版本；不访问源库、不改写输入，不覆盖输出。"""
    output = Path(output)
    if output.exists():
        raise ValueError('输出目录已存在；请保留旧版本')
    manifest = None
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    for source_path in inputs:
        source_path = Path(source_path)
        declaration, records, input_version = stream_index_input(source_path)
        zone = ZoneInfo(declaration['data_profile']['timezone'])
        start = datetime.fromisoformat(declaration['window']['start']).astimezone(zone)
        end = datetime.fromisoformat(declaration['window']['end_exclusive']).astimezone(zone)
        if start.hour or start.minute or start.second or start.microsecond or end != start + timedelta(days=1):
            raise ValueError('索引输入必须是一个完整业务日的留存范围')
        if manifest is None:
            manifest = {key: declaration[key] for key in ('data_profile', 'source', 'kinds', 'interpretation_version')}
            manifest.update(schema_version='core-overview-index/v1', days={})
        if any(manifest[key] != declaration[key] for key in ('data_profile', 'source', 'kinds', 'interpretation_version')):
            raise ValueError('不同日期的来源或口径不兼容')
        day = start.date().isoformat()
        if day in manifest['days']:
            raise ValueError('同一业务日不能同时选择两个输入版本')
        path = output / f'{day}.sqlite3'
        with closing(records), closing(sqlite3.connect(path)) as connection, connection:
            connection.execute('PRAGMA journal_mode=DELETE')
            connection.execute('''CREATE TABLE records (
                reference TEXT PRIMARY KEY, kind TEXT NOT NULL, object TEXT NOT NULL,
                start_time TEXT NOT NULL, hour INTEGER NOT NULL, family TEXT NOT NULL,
                level TEXT NOT NULL, severity INTEGER NOT NULL, search TEXT NOT NULL,
                item TEXT NOT NULL, payload BLOB NOT NULL)''')
            record_count = 0
            for result in records:
                item = overview_item(result, declaration.get('level_conflicts'))
                search = overview_search_text(item)
                connection.execute('INSERT INTO records VALUES (?,?,?,?,?,?,?,?,?,?,?)', (
                    item['reference'], item['kind'], item['object'], item['start_time'],
                    datetime.fromisoformat(item['start_time'].replace('Z', '+00:00')).astimezone(zone).hour,
                    item['address_family'], overview_level_filter(item),
                    {'high': 0, 'middle': 1, 'low': 2}.get(item['level'], 3), search,
                    json.dumps(item, ensure_ascii=False, sort_keys=True), serialize_record(result)))
                record_count += 1
            connection.execute('CREATE INDEX records_severity ON records(severity, start_time DESC, reference)')
            connection.execute('CREATE INDEX records_time ON records(start_time DESC, reference)')
            connection.execute('CREATE INDEX records_family_kind_hour ON records(family, kind, hour, object)')
            connection.execute('CREATE TABLE provenance (manifest BLOB NOT NULL)')
            source_bytes = source_path.read_bytes()
            if 'overview_v1_' + hashlib.sha256(source_bytes).hexdigest() != input_version:
                raise ValueError('构建期间输入清单发生变化')
            connection.execute('INSERT INTO provenance VALUES (?)', (source_bytes,))
        path.chmod(0o400)
        manifest['days'][day] = {'file': path.name, 'sha256': _digest(path), 'count': record_count,
                                 'input_version': input_version,
                                 'window': {'source': declaration['source']['code'], 'start': start.isoformat(), 'end_exclusive': end.isoformat()}}
        if manifest['interpretation_version'] == 'recorded-anomaly-overview/v3':
            manifest['days'][day]['interpretation_version'] = declaration['interpretation_version']
    if not manifest:
        raise ValueError('至少指定一份已留存输入')
    failures = {}
    for selection in diagnostics:
        for day, diagnostic in compile_diagnostics(selection, manifest).items():
            if day in manifest['days']:
                raise ValueError('同一业务日不能同时选择消费输入和失败诊断')
            if day in failures:
                failures[day]['reasons'].extend(diagnostic['reasons'])
                validate_diagnostic(failures[day], manifest, diagnostic['window'])
            else:
                failures[day] = diagnostic
    if failures:
        manifest.update(schema_version='core-overview-index/v2', diagnostics={})
        for day, diagnostic in sorted(failures.items()):
            diagnostic['reasons'].sort(key=lambda item: (item['kind'], item['code']))
            payload = (json.dumps(diagnostic, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()
            if len(payload) > 65536:
                raise ValueError('单日诊断文件超限')
            filename = f'{day}.diagnostic.json'
            with (output / filename).open('xb') as stream:
                stream.write(payload)
            (output / filename).chmod(0o400)
            manifest['diagnostics'][day] = {'file': filename, 'sha256': hashlib.sha256(payload).hexdigest(), 'window': diagnostic['window']}
    days = sorted(manifest['days'])
    manifest['window'] = {'source': manifest['source']['code'],
                          'start': manifest['days'][days[0]]['window']['start'],
                          'end_exclusive': manifest['days'][days[-1]]['window']['end_exclusive']}
    manifest['evidence'] = {'indexer_sha256': _digest(Path(__file__)),
                            'window_semantics': '日期外包范围；仅 days 中列出的业务日已留存，不表示连续覆盖'}
    payload = (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()
    if len(payload) > 65536:
        raise ValueError('目录清单超限')
    with (output / 'manifest.json').open('xb') as stream:
        stream.write(payload)
    (output / 'manifest.json').chmod(0o400)
    return {'manifest': str((output / 'manifest.json').resolve()), 'days': days,
            'version': ('overview_index_v2_' if failures else 'overview_index_v1_') + hashlib.sha256(payload).hexdigest()}


class DailyIndex:
    def __init__(self, path, payload):
        self.path = path
        self.manifest = json.loads(payload)
        schema = self.manifest['schema_version']
        if schema not in {'core-overview-index/v1', 'core-overview-index/v2'}:
            raise ValueError('日期目录格式未支持')
        self.version = ('overview_index_v2_' if schema.endswith('/v2') else 'overview_index_v1_') + hashlib.sha256(payload).hexdigest()
        profile = json.loads((Path(__file__).resolve().parents[3] / 'config/data-profile.json').read_text())
        source = self.manifest['source']
        if (self.manifest['data_profile'] != profile
                or self.manifest['interpretation_version'] not in SUPPORTED_INTERPRETATIONS
                or set(self.manifest['kinds']) not in SUPPORTED_KIND_SETS
                or source['code'] != 'r' or source['collector_id'] != 'rrc25'
                or source['collector_basis'] != 'user_confirmation'
                or source['coverage'] != 'unknown' or source['detector_version'] is not None):
            raise ValueError('来源、数据档或消费口径不兼容')
        datetime.strptime(source['confirmed_on'], '%Y-%m-%d')
        self.days = self.manifest['days']
        self.diagnostics = self.manifest['diagnostics'] if schema.endswith('/v2') else {}
        if schema.endswith('/v1') and 'diagnostics' in self.manifest:
            raise ValueError('旧目录不能携带未绑定的诊断语义')
        if not isinstance(self.days, dict) or not 1 <= len(self.days) <= 366:
            raise ValueError('日期目录无效')
        if (not isinstance(self.diagnostics, dict) or len(self.days) + len(self.diagnostics) > 366
                or set(self.days) & set(self.diagnostics)):
            raise ValueError('失败诊断日期目录无效或与消费日重叠')
        for day, entry in self.diagnostics.items():
            start = datetime.strptime(day, '%Y-%m-%d').replace(tzinfo=ZoneInfo(profile['timezone']))
            end = start + timedelta(days=1)
            if (day != start.date().isoformat()
                    or not datetime.fromisoformat(profile['window_start']) <= start < end <= datetime.fromisoformat(profile['window_end_exclusive'])
                    or entry['file'] != f'{day}.diagnostic.json' or not re.fullmatch('[0-9a-f]{64}', entry['sha256'])
                    or entry['window'] != {'source': source['code'], 'start': start.isoformat(), 'end_exclusive': end.isoformat()}):
                raise ValueError('失败诊断日声明无效')
        for day, entry in self.days.items():
            start = datetime.strptime(day, '%Y-%m-%d').replace(tzinfo=ZoneInfo(profile['timezone']))
            end = start + timedelta(days=1)
            if (day != start.date().isoformat()
                    or not datetime.fromisoformat(profile['window_start']) <= start < end <= datetime.fromisoformat(profile['window_end_exclusive'])
                    or entry['window'] != {'source': source['code'], 'start': start.isoformat(), 'end_exclusive': end.isoformat()}
                    or entry['file'] != f'{day}.sqlite3' or type(entry['count']) is not int or entry['count'] < 0):
                raise ValueError('按日索引声明无效')
            if (self.manifest['interpretation_version'] == 'recorded-anomaly-overview/v3'
                    and entry.get('interpretation_version') not in SUPPORTED_INTERPRETATIONS):
                raise ValueError('新目录须逐日明确绑定原输入解释，不能重解释旧日文件')
        days = sorted(self.days)
        if self.manifest['window'] != {'source': source['code'], 'start': self.days[days[0]]['window']['start'],
                                      'end_exclusive': self.days[days[-1]]['window']['end_exclusive']}:
            raise ValueError('目录范围与已留存日期不一致')

    @contextmanager
    def _open(self, day):
        try:
            entry = self.days[day]
            path = self.path.parent / entry['file']
            if path.resolve().parent != self.path.parent.resolve():
                raise ValueError('索引文件越出清单目录')
            identity = _identity(path)
            _single_file(path)
            _verify(path, identity, entry['sha256'])
            connection = sqlite3.connect(path.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
            try:
                connection.execute('PRAGMA query_only=ON')
                original_bytes = connection.execute('SELECT manifest FROM provenance').fetchone()[0]
                original = json.loads(original_bytes)
                if ('overview_v1_' + hashlib.sha256(original_bytes).hexdigest() != entry['input_version']
                        or original['window']['source'] != entry['window']['source']
                        or any(datetime.fromisoformat(original['window'][key]) != datetime.fromisoformat(entry['window'][key]) for key in ('start', 'end_exclusive'))
                        or any(original[key] != self.manifest[key] for key in ('data_profile', 'source', 'kinds'))
                        or original['interpretation_version'] != self.day_interpretation(day)
                        or original['records']['count'] != entry['count']
                        or connection.execute('SELECT count(*) FROM records').fetchone()[0] != entry['count']):
                    raise ValueError('目录与留存输入来源或记录数不一致')
                # 对文件系统边界作前后检查，变化不能返回旧的成功结果。
                yield connection
                _single_file(path)
                if _identity(path) != identity:
                    raise ValueError('查询期间输入发生变化')
            finally:
                connection.close()
        except (OSError, RuntimeError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
            raise InputError('首页按日索引不可用或校验失败') from error

    def day_interpretation(self, day):
        if self.manifest['interpretation_version'] == 'recorded-anomaly-overview/v3':
            return self.days[day]['interpretation_version']
        return self.manifest['interpretation_version']

    def query(self, response):
        query = response['query']
        day, family = query['date'], query['family']
        self._check_diagnostic(day)
        if day not in self.days:
            return response
        with self._open(day) as connection:
            if self.manifest['interpretation_version'] == 'recorded-anomaly-overview/v3':
                response['metadata']['input_interpretation_version'] = self.day_interpretation(day)
            if family == 'all':
                clause, values = '1=1', []
            elif family in {'ipv4', 'ipv6'}:
                clause, values = 'family IN (?, ?)', [family, 'mixed']
            else:
                clause, values = 'family = ?', [family]
            count = connection.execute('SELECT count(*) FROM records WHERE ' + clause, values).fetchone()[0]
            excluded = connection.execute("SELECT count(*) FROM records WHERE family='unknown'").fetchone()[0] if family in {'ipv4', 'ipv6'} else 0
            query['excluded_unknown_family'] = excluded
            counts = dict(connection.execute('SELECT hour, count(DISTINCT object) FROM records WHERE ' + clause + " AND kind='prefix_outage' GROUP BY hour", values))
            start = datetime.fromisoformat(query['start'])
            buckets = [{'start': (start + timedelta(hours=hour)).isoformat(),
                        'end_exclusive': (start + timedelta(hours=hour + 1)).isoformat(),
                        'value': counts.get(hour, 0)} for hour in range(24)]
            for key, column, absent in [('kind', 'kind', 'all'), ('hour', 'hour', None), ('level', 'level', 'all')]:
                if query[key] != absent:
                    clause += f' AND {column} = ?'
                    values.append(query[key])
            if query['q']:
                clause += ' AND (reference = ? OR instr(search, ?) > 0)'
                values.extend([query['q'], query['q'].lower()])
            total, distinct = connection.execute('SELECT count(*), count(DISTINCT CASE WHEN kind=\'prefix_outage\' THEN object END) FROM records WHERE ' + clause, values).fetchone()
            order = ('severity, ' if query['sort'] == 'severity' else '') + 'start_time DESC, reference'
            page, size = query['page'], query['page_size']
            items = [json.loads(row[0]) for row in connection.execute('SELECT item FROM records WHERE ' + clause + ' ORDER BY ' + order + ' LIMIT ? OFFSET ?', [*values, size, (page - 1) * size])]
            response.update(state='available', overview={'record_count': count, 'visible_prefixes': None, 'visible_origin_ases': None},
                            trend={'metric': 'recorded_prefix_outage_starts_distinct', 'bucket_seconds': 3600, 'buckets': buckets},
                            events={'total': total, 'items': items, 'distinct_prefixes': distinct, 'page': page, 'page_size': size, 'page_count': (total + size - 1) // size})
        return response

    def _check_diagnostic(self, day):
        if day not in self.diagnostics:
            return
        try:
            entry = self.diagnostics[day]
            path = self.path.parent / entry['file']
            if path.resolve().parent != self.path.parent.resolve():
                raise ValueError('诊断文件越出清单目录')
            diagnostic = read_diagnostic(path, entry['sha256'], self.manifest, entry['window'])
        except (OSError, RuntimeError, ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
            raise InputError('已知失败日的诊断证据不可验证') from error
        message = ('选定日期源数据读取未完成' if diagnostic['stage'] == 'source_read'
                   else '选定日期的源记录校验失败')
        raise InputError(message + '；不能提供消费指标或异常详情', payload={'diagnostic': diagnostic})

    def detail(self, reference):
        parts = reference.split('/')
        day = parts[1][:10] if len(parts) == 5 else ''
        self._check_diagnostic(day)
        if day in self.days:
            with self._open(day) as connection:
                row = connection.execute('SELECT item,payload FROM records WHERE reference=?', (reference,)).fetchone()
                if row:
                    metadata = {key: self.manifest[key] for key in ('source', 'interpretation_version')}
                    if self.manifest['interpretation_version'] == 'recorded-anomaly-overview/v3':
                        metadata['input_interpretation_version'] = self.day_interpretation(day)
                    return {'state': 'available', 'version': self.version, 'item': json.loads(row[0]),
                            'metadata': metadata,
                            'record': json.loads(row[1])}
        raise InputError('该留存版本中没有此引用', 404)
