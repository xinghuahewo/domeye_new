"""固定CoreToken的离线查询；页暂定，日门禁及会话终端资格不可旁路。"""
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta
import hashlib
import json
import re
import uuid

from psycopg2 import sql

from data_pipeline.history.event_collection.store import checked_row, row_bytes
from data_pipeline.history.event_index.exact import loads, native, wire
from data_pipeline.history.event_index.model import TABLES, SCHEMAS, RULE
from data_pipeline.history.event_index.project import read_span
from data_pipeline.history.event_index.store import rows
from data_pipeline.history.event_index.cleanup import owner, release


class CoreSession:
    def __init__(self, history, token):
        self.h, self.token = history, token
        self.budget = history._budget()
        self.db = self.ready = self.receipt = None
        self.failed = self.active = False
        self.pending = set()
        self.completed = set()
        self.operation_scopes = {}
        self.cache = {}
        self.progress = {}
        self.cleanup_errors = []
        self.budget.cleanup_errors = self.cleanup_errors

    def __enter__(self):
        if self.ready is not None or self.failed:
            raise ValueError('H1会话不可重复进入')
        try:
            self.ready = self.h._qualify_core(self.token, self.budget)
            self.db = self.h._lake(self.token.profile_id, True)
            return self
        except BaseException as error:
            self._fail(error)
            raise

    def __exit__(self, kind, error, trace):
        primary = error
        pg = None
        receipt = None
        try:
            if kind is None and not self.failed and not self.active and not self.pending:
                # 只读资格事务保持共享锁，直到DuckDB释放及最终本地检查完成；
                # 最后关闭PG自动结束此只读事务，再使回执可见。
                pg = self.h._connect()
                self.h._qualify_core(self.token, self.budget, pg=pg, lock=True)
                self._check()
                receipt = {'qualification': 'complete', 'scope': 'H1_artificial_core_queries', 'profile': RULE,
                           'token': asdict(self.token), 'completed_operations': sorted(self.completed),
                           'query_scopes': self.progress, 'operation_scopes': self.operation_scopes,
                           'resources': self.budget.report(), 'H3': 'unresolved_external'}
                if len(row_bytes(receipt)) > self.h.collection_limits.metadata_bytes:
                    raise ValueError('H1终端回执元数据超限')
        except BaseException as caught:
            primary = caught
        finally:
            db, self.db = self.db, None
            primary = release(db, 'DuckDB', primary, self.cleanup_errors)
            if primary is None and receipt is not None:
                try:
                    # 不调用要求live DB的_check；仍拒绝关闭期间预算漂移。
                    self.budget.check()
                except BaseException as caught:
                    primary = caught
            primary = release(pg, 'qualification PostgreSQL', primary, self.cleanup_errors)
        if primary is not None or self.cleanup_errors:
            self.failed = True
            self.receipt = None
            if error is None and primary is not None:
                raise primary.with_traceback(primary.__traceback__)
        elif receipt is not None:
            self.receipt = receipt
        return False

    def _fail(self, primary=None):
        self.failed = True
        self.receipt = None
        db, self.db = self.db, None
        cleanup = release(db, 'DuckDB', primary, self.cleanup_errors)
        if primary is None and cleanup is not None:
            raise cleanup

    def _check(self):
        try:
            self.budget.check()
            if self.db is None or self.failed:
                raise ValueError('H1会话已关闭/失败')
        except BaseException as error:
            self._fail(error)
            raise

    @contextmanager
    def _reading(self):
        try:
            self._check()
            yield
            self._check()
        except BaseException as error:
            self._fail(error)
            raise

    def table(self, name):
        return f'lake.history.t{TABLES.index(name)} AT (VERSION => {int(self.token.snapshot)})'

    def _fetch(self, statement, params=(), *, maximum=60):
        self._check()
        result = self.db.execute(statement, params)
        names = [d[0] for d in result.description]
        output = []
        size = 0
        while True:
            self._check()
            values = result.fetchone()
            if values is None:
                break
            row = dict(zip(names, values))
            data = checked_row(row, self.budget)
            size += len(data)
            if len(output) >= maximum or size > self.h.limits.batch_bytes:
                raise ValueError('H1查询有界结果超限')
            self.budget.add('typed_rows', 1, self.h.limits.max_total_rows)
            self.budget.add('typed_bytes', len(data), self.h.limits.max_total_bytes)
            output.append(row)
        return output

    def _remember(self, key, value):
        if key not in self.cache and len(self.cache) >= self.h.collection_limits.edges:
            raise ValueError('H1查询缓存项预算超限')
        old = len(row_bytes(self.cache.get(key))) if key in self.cache else 0
        size = self.budget.counts.get('query_cache_bytes', 0) + len(row_bytes(value)) - old
        if size > self.h.collection_limits.metadata_bytes:
            raise ValueError('H1查询缓存字节预算超限')
        self.budget.counts['query_cache_bytes'] = size
        self.cache[key] = value

    def _complete(self, key, scope=None):
        if key not in self.completed and len(self.completed) >= self.h.collection_limits.edges:
            raise ValueError('H1完成操作scope数量超限')
        if scope is not None:
            self.operation_scopes[key] = scope
            if len(row_bytes(self.operation_scopes)) > self.h.collection_limits.metadata_bytes:
                raise ValueError('H1完成操作scope字节超限')
        self.completed.add(key)

    @staticmethod
    def _date(day):
        if not isinstance(day, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', day):
            raise ValueError('H1日期参数无效')
        datetime.strptime(day, '%Y-%m-%d')

    def _gate(self, day):
        key = 'day:' + day
        if key not in self.cache:
            result = self._fetch('SELECT * FROM ' + self.table('core_days') + ' WHERE day=?', [day], maximum=1)
            self._remember(key, result[0] if result else {'day': day, 'state': 'not_retained'})
        return self.cache[key]

    def _unavailable(self, day):
        response = {'qualification': 'provisional', 'state': day['state'], 'day': day['day'],
                    'overview': None, 'trend': None, 'events': None}
        if day['state'] == 'validation_failed':
            raw = self._document(day['entity_path'], day['byte_start'], day['byte_end'])
            response['diagnostic'] = wire(loads(raw))
        return self._bounded(response)

    def _bounded(self, response):
        if len(row_bytes(response)) > self.h.limits.batch_bytes:
            raise ValueError('H1完整返回页字节超限')
        self._check()
        return response

    def _document(self, path, start, end):
        root = self.h.data_root / self.token.collection.collection_id / 'source'
        return read_span(root, path, start, end, self.budget)

    def _item(self, row):
        raw = self._document(row['item_path'], row['item_start'], row['item_end'])
        item = loads(raw)
        known = ('reference', 'content_version', 'kind', 'object', 'start_time', 'end_time', 'level', 'address_family', 'asns', 'record_number')
        result = {k: native(item.get(k)) for k in known}
        for key in ('object_identity', 'level_conflict', 'parent_prefix', 'country_name'):
            v = item.get(key, None)
            # null与缺失的区别同时保存在完整item_exact中。
            if any(k == key for k, _ in item.members):
                result[key] = native(v)
        return {'occurrence': row['occurrence'], 'item': result, 'item_exact': wire(item),
                'source': {k: row[k] for k in ('file_id', 'source_ordinal', 'item_document', 'payload_document', 'item_storage', 'payload_storage')}}

    @staticmethod
    def _family(family):
        if family == 'all':
            return 'TRUE', []
        if family in ('ipv4', 'ipv6'):
            return 'family IN (?,\'mixed\')', [family]
        return 'family=?', [family]

    def _metrics(self, day, family):
        key = 'metrics:' + day['day'] + ':' + family
        if key not in self.cache:
            clause, params = self._family(family)
            statement = ('SELECT hour,count(*) FILTER (WHERE ' + clause + ') AS n,'
                         'count(DISTINCT object) FILTER (WHERE ' + clause + ' AND kind=\'prefix_outage\') AS prefixes,'
                         'count(*) FILTER (WHERE family=\'unknown\') AS unknown FROM ' + self.table('core_records') + ' WHERE day=? GROUP BY hour ORDER BY hour')
            result = self._fetch(statement, [*params, *params, day['day']], maximum=24)
            start = datetime.fromisoformat(day['window_start'])
            by_hour = {r['hour']: r['prefixes'] for r in result}
            self._remember(key, {'overview': {'record_count': sum(r['n'] for r in result), 'visible_prefixes': None, 'visible_origin_ases': None},
                                 'excluded_unknown_family': sum(r['unknown'] for r in result) if family in ('ipv4', 'ipv6') else 0,
                                 'trend': {'metric': 'recorded_prefix_outage_starts_distinct', 'bucket_seconds': 3600,
                                           'buckets': [{'start': (start + timedelta(hours=i)).isoformat(),
                                                        'end_exclusive': (start + timedelta(hours=i + 1)).isoformat(),
                                                        'value': by_hour.get(i, 0)} for i in range(24)]}})
        return self.cache[key]

    def query(self, day, *, family='all', kind='all', level='all', hour=None, q='', sort='severity', limit=60, cursor=None):
        return deepcopy(self._page(day, family=family, kind=kind, level=level, hour=hour, q=q, sort=sort, limit=limit, cursor=cursor))

    def metadata(self):
        self._check()
        if self.active:
            raise ValueError('H1已有活动流')
        with self._reading():
            root = self.ready['root']
            result = {'qualification': 'provisional', 'profile': RULE, 'token': asdict(self.token),
                      'original_index_version': root['original_index_version'],
                      'root_document': wire(loads(self._document(root['entity_path'], root['byte_start'], root['byte_end'])))}
            if len(row_bytes(result)) > self.h.limits.batch_bytes:
                raise ValueError('H1目录元数据返回超限')
            self._complete('metadata')
            return result

    def references(self, day, reference, *, limit=60, cursor=None):
        if not isinstance(reference, str) or not reference or len(reference.encode()) > self.h.collection_limits.token_bytes:
            raise ValueError('H1引用参数无效')
        return deepcopy(self._page(day, family='all', kind='all', level='all', hour=None, q='', sort='time', limit=limit, cursor=cursor, reference=reference))

    def _page(self, day, *, family, kind, level, hour, q, sort, limit, cursor, reference=None):
        self._check()
        self._date(day)
        kinds = self.ready['root']['declaration']['kinds']
        if (self.active or family not in ('all', 'ipv4', 'ipv6', 'unknown') or kind not in ['all', *kinds]
                or level not in ('all', 'high', 'middle', 'low', 'unknown', 'conflict')
                or (hour is not None and (type(hour) is not int or not 0 <= hour < 24))
                or not isinstance(q, str) or len(q.strip()) > 120 or sort not in ('severity', 'time')
                or type(limit) is not int or not 1 <= limit <= min(60, self.h.limits.batch_rows)):
            raise ValueError('H1查询参数无效')
        params_binding = dict(day=day, family=family, kind=kind, level=level, hour=hour, q=q.strip(), sort=sort, limit=limit, reference=reference)
        binding = json.loads(row_bytes({'token': asdict(self.token), 'profile': RULE, 'query': params_binding}))
        key = hashlib.sha256(row_bytes(binding)).hexdigest()
        after = None
        if cursor is not None:
            if not isinstance(cursor, dict) or set(cursor) != {'binding', 'after'} or cursor['binding'] != binding:
                raise ValueError('H1游标错版/查询scope不符')
            after = cursor['after']
            if (not isinstance(after, list) or len(after) != 4 or type(after[0]) is not int or not 0 <= after[0] <= 3
                    or not isinstance(after[1], str) or len(after[1]) > 64 or not isinstance(after[2], str)
                    or len(after[2].encode()) > self.h.collection_limits.token_bytes or type(after[3]) is not int or not 0 <= after[3] < 2**63):
                raise ValueError('H1游标完整排序键无效')
        previous = self.progress.get(key)
        if previous and not previous['finished'] and after != previous['next_after']:
            raise ValueError('H1分页必须沿已交付游标继续；不能跳过未读页')
        with self._reading():
            gate = self._gate(day)
            if gate['state'] != 'available':
                self._complete('gated:' + key)
                return self._bounded({**self._unavailable(gate), 'binding': binding, 'resolution': 'unknown' if reference is not None else None})
            clause, values = self._family(family)
            clause = 'day=? AND ' + clause
            values = [day, *values]
            for column, value, absent in [('kind', kind, 'all'), ('level', level, 'all'), ('hour', hour, None)]:
                if value != absent:
                    clause += ' AND ' + column + '=?'
                    values.append(value)
            if q.strip():
                clause += ' AND (reference=? OR contains(search,?))'
                values.extend([q.strip(), q.strip().lower()])
            if reference is not None:
                clause += ' AND reference=?'
                values.append(reference)
            count_key = 'count:' + key
            if count_key not in self.cache:
                stats = self._fetch('SELECT count(*) AS total,count(DISTINCT CASE WHEN kind=\'prefix_outage\' THEN object END) AS distinct_prefixes FROM ' + self.table('core_records') + ' WHERE ' + clause, values, maximum=1)[0]
                self._remember(count_key, stats)
            stats = self.cache[count_key]
            if after:
                severity, time, ref, ordinal = after
                rest = '(start_time<? OR (start_time=? AND (reference>? OR (reference=? AND occurrence>?))))'
                tail = [time, time, ref, ref, ordinal]
                if sort == 'severity':
                    rest = '(severity>? OR (severity=? AND ' + rest + '))'
                    tail = [severity, severity, *tail]
                clause += ' AND ' + rest
                values.extend(tail)
            order = ('severity,' if sort == 'severity' else '') + 'start_time DESC,reference,occurrence'
            records = self._fetch('SELECT * FROM ' + self.table('core_records') + ' WHERE ' + clause + ' ORDER BY ' + order + ' LIMIT ?', [*values, limit], maximum=limit)
            output = []
            size = 0
            for r in records:
                item = self._item(r)
                size += len(row_bytes(item))
                if size > self.h.limits.batch_bytes:
                    raise ValueError('H1 items返回页字节超限')
                output.append(item)
            next_cursor = None
            scope = previous if previous and not previous['finished'] else {
                'scope': 'full_query' if after is None else 'query_suffix', 'start_after': after,
                'day': day, 'query': params_binding}
            if len(records) == limit:
                last = records[-1]
                next_cursor = {'binding': binding, 'after': [last[k] for k in ('severity', 'start_time', 'reference', 'occurrence')]}
                self.pending.add(key)
            else:
                self.pending.discard(key)
                self._complete('query:' + key)
            scope.update(next_after=next_cursor['after'] if next_cursor else None, finished=next_cursor is None)
            if key not in self.progress and len(self.progress) >= self.h.collection_limits.edges:
                raise ValueError('H1游标scope预算超限')
            self.progress[key] = scope
            if len(row_bytes(self.progress)) > self.h.collection_limits.metadata_bytes:
                raise ValueError('H1游标scope字节超限')
            if len(self.pending) + len(self.completed) > self.h.collection_limits.edges:
                raise ValueError('H1会话查询scope数量超限')
            metrics = self._metrics(gate, family)
            return self._bounded({'qualification': 'provisional', 'state': 'available', 'day': day, 'scope': 'selected_core_day',
                    'binding': binding, 'metadata': {'input_version': gate['input_version'], 'root_interpretation': gate['root_interpretation'],
                                                   'input_interpretation': gate['input_interpretation'], 'root_document': gate['root_document'], 'provenance_document': gate['document_id'],
                                                   'source': self.ready['root']['declaration']['source'], 'data_profile': self.ready['root']['declaration']['data_profile'],
                                                   'window': {'source': self.ready['root']['declaration']['source']['code'], 'start': gate['window_start'], 'end_exclusive': gate['window_end']},
                                                   'original_index_version': self.ready['root']['original_index_version']},
                    **metrics, 'events': {**stats, 'items': output}, 'next_cursor': next_cursor, 'query_scope': dict(scope),
                    'resolution': ('missing' if stats['total'] == 0 else 'matched' if stats['total'] == 1 else 'ambiguous') if reference is not None else None,
                    'original_H3_resolution': 'unresolved_external'})

    def detail(self, day, occurrence):
        self._check()
        self._date(day)
        if self.active or type(occurrence) is not int or not 0 <= occurrence < 2**63:
            raise ValueError('H1详情occurrence参数无效')
        with self._reading():
            gate = self._gate(day)
            key = 'detail:' + day + ':' + str(occurrence)
            scope = {'operation': 'detail', 'day': day, 'occurrence': occurrence, 'status': 'completed'}
            if gate['state'] != 'available':
                response = self._unavailable(gate)
                self._complete(key, {**scope, 'state': gate['state'], 'resolution': 'unknown'})
                return response
            found = self._fetch('SELECT * FROM ' + self.table('core_records') + ' WHERE day=? AND occurrence=?', [day, occurrence], maximum=1)
            if not found:
                response = self._bounded({'qualification': 'provisional', 'state': 'not_retained', 'resolution': 'missing', 'record': None})
                self._complete(key, {**scope, 'state': 'not_retained', 'resolution': 'missing'})
                return response
            row = found[0]
            raw = self._document(row['payload_path'], row['payload_start'], row['payload_end'])
            response = {'qualification': 'provisional', 'state': 'available', **self._item(row), 'record': wire(loads(raw)),
                        'provenance': wire(loads(self._document(gate['entity_path'], gate['byte_start'], gate['byte_end']))),
                        'profile': RULE, 'H3': 'unresolved_external'}
            if len(row_bytes(response)) > self.h.limits.batch_bytes:
                raise ValueError('H1详情返回字节超限')
            self._complete(key, {**scope, 'state': 'available', 'resolution': 'matched'})
            return response

    def bulk(self, name, *, batch_rows=None):
        self._check()
        cap = self.h.limits.batch_rows if batch_rows is None else batch_rows
        if self.active or name not in TABLES or type(cap) is not int or not 1 <= cap <= self.h.limits.batch_rows:
            raise ValueError('H1批参数无效')
        self.active = True
        try:
            pending = []
            size = count = 0
            digest = hashlib.sha256()
            with owner(rows(self.db, self.token, name, self.budget, cap), 'typed row stream', self.cleanup_errors) as stream:
                while True:
                    self._check()
                    try:
                        row = next(stream)
                    except StopIteration:
                        break
                    data = row_bytes(row)
                    if pending and (len(pending) == cap or size + len(data) > self.h.limits.batch_bytes):
                        yield {'qualification': 'provisional', 'rows': pending, 'dataset': name}
                        self._check()
                        pending, size = [], 0
                    pending.append(row)
                    size += len(data)
                    count += 1
                    digest.update(data + b'\n')
                if pending:
                    yield {'qualification': 'provisional', 'rows': pending, 'dataset': name}
                    self._check()
            expected = next(t for t in self.ready['tables'] if t['name'] == name)
            if (count, digest.hexdigest()) != (expected['rows'], expected['sha256']):
                raise ValueError('H1完整typed摘要不符')
            self._complete('bulk:' + name)
        except BaseException as error:
            self._fail(error)
            raise
        finally:
            self.active = False

    def rebuild(self):
        self._check()
        if self.active:
            raise ValueError('H1已有活动流')
        with self._reading():
            namespace = 'hcq_' + uuid.uuid4().hex
            columns = ('day', 'occurrence', 'reference', 'kind', 'family', 'level', 'hour', 'severity', 'start_time', 'search', 'item_document', 'payload_document')
            count = written = calls = index_bytes = 0
            with owner(self.h._connect(), 'rebuild PostgreSQL', self.cleanup_errors) as pg, pg, pg.cursor() as c:
                c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(namespace)))
                declarations = ','.join('"' + k + '" ' + ('BIGINT' if k in ('occurrence', 'hour', 'severity', 'item_document', 'payload_document') else 'TEXT') for k in columns)
                c.execute(sql.SQL('CREATE TABLE {}.records (' + declarations + ')').format(sql.Identifier(namespace)))

                def disk():
                    nonlocal index_bytes
                    self._check()
                    c.execute('SELECT pg_total_relation_size(%s)', (namespace + '.records',))
                    index_bytes = c.fetchone()[0]
                    if index_bytes > self.h.collection_limits.temporary_bytes:
                        raise ValueError('H1派生PG索引磁盘超限')
                    self.budget.counts['query_index_bytes'] = index_bytes

                with owner(self.bulk('core_records'), 'rebuild record stream', self.cleanup_errors) as stream:
                    for batch in stream:
                        prefix = sql.SQL('INSERT INTO {}.records VALUES ').format(sql.Identifier(namespace)).as_string(pg).encode()
                        pending = []
                        size = len(prefix)
                        for row in batch['rows']:
                            value = c.mogrify('(' + ','.join('%s' for _ in columns) + ')', tuple(row[k] for k in columns))
                            if size + len(value) + 1 > self.h.limits.batch_bytes and pending:
                                self._check()
                                statement = prefix + b','.join(pending)
                                c.execute(statement)
                                calls += 1
                                written += len(statement)
                                self.budget.add('index_sql_bytes', len(statement), self.h.limits.max_total_bytes)
                                disk()
                                pending, size = [], len(prefix)
                            if size + len(value) + 1 > self.h.limits.batch_bytes:
                                raise ValueError('H1索引单行PG字节超限')
                            pending.append(value)
                            size += len(value) + 1
                            count += 1
                        if pending:
                            self._check()
                            statement = prefix + b','.join(pending)
                            c.execute(statement)
                            calls += 1
                            written += len(statement)
                            self.budget.add('index_sql_bytes', len(statement), self.h.limits.max_total_bytes)
                            disk()
                for name, fields in [('lookup', 'day,reference,occurrence'), ('ordering', 'day,severity,start_time DESC,reference,occurrence')]:
                    c.execute(sql.SQL('CREATE INDEX {} ON {}.records (' + fields + ')').format(sql.Identifier(name), sql.Identifier(namespace)))
                disk()
                self.h._qualify_core(self.token, self.budget, pg=pg, lock=True)
                self._check()
            self._complete('rebuild:' + namespace, {'operation': 'rebuild', 'schema': namespace,
                           'token': asdict(self.token), 'scope': 'core_query_index_only', 'status': 'committed', 'rows': count})
            return {'qualification': 'provisional', 'schema': namespace, 'rows': count, 'insert_calls': calls, 'sql_bytes': written, 'index_bytes': index_bytes, 'scope': 'core_query_index_only',
                    'json_nodes_copied': 0, 'token': asdict(self.token)}
