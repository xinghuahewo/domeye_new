"""完成结果的小型小时／日汇总。只重建脏小时，日结果合并小时结果。

来源文件事务只标记失效；汇总事务失败不影响基础查询。每次仅驻留一个对象
的一个小时（或一天）的采样，批量写入 256 行，不读取 RouteState 或 MRT。
"""
from __future__ import annotations

import hashlib
import itertools
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from psycopg2.extras import Json, execute_values

PROFILE = json.loads((Path(__file__).resolve().parents[3] / 'config/data-profile.json').read_text())
ZONE = ZoneInfo(PROFILE['timezone'])
RULE = 'completed-result-rollups/v1'
KINDS = ('prefix_outage', 'as_outage', 'country_outage', 'leak', 'hijack', 'sub_hijack')
FAMILIES = ('all', 'ipv4', 'ipv6', 'unknown')
DEFINITIONS = {
    'announce': {'unit': 'accepted_route_element', 'aggregation': 'sum_nonoverlapping_windows'},
    'withdraw': {'unit': 'accepted_route_element', 'aggregation': 'sum_nonoverlapping_windows'},
    'ipv4_blocks': {'unit': 'covered_ipv4_24_block', 'aggregation': 'last_sample_and_sample_peak'},
    'ipv6_blocks': {'unit': 'covered_ipv6_48_block', 'aggregation': 'last_sample_and_sample_peak'},
    'ipv4_addresses': {'unit': 'covered_ipv4_24_block_times_256', 'aggregation': 'last_sample_and_sample_peak'},
    'event_starts': {'unit': 'incident', 'aggregation': 'distinct_incident_by_start'},
}


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()


def hour(value):
    return value.astimezone(ZONE).replace(minute=0, second=0, microsecond=0)


def mark(cur, start, end=None):
    left = hour(start)
    end = end or start + timedelta(microseconds=1)
    while left < end:
        cur.execute('INSERT INTO result_delivery.rollup_dirty VALUES(%s) ON CONFLICT DO NOTHING', (left,))
        left += timedelta(hours=1)


def initialize(cur):
    cur.execute('''CREATE TABLE IF NOT EXISTS result_delivery.rollup_control
 (id integer PRIMARY KEY CHECK(id=1), rule text NOT NULL, timezone text NOT NULL);
CREATE TABLE IF NOT EXISTS result_delivery.rollup_dirty (start timestamptz PRIMARY KEY);
CREATE TABLE IF NOT EXISTS result_delivery.rollup_periods (
 grain text NOT NULL, start timestamptz NOT NULL, body jsonb NOT NULL, PRIMARY KEY(grain,start));
CREATE TABLE IF NOT EXISTS result_delivery.rollup_features (
 grain text NOT NULL, start timestamptz NOT NULL, scope text NOT NULL, subject text NOT NULL,
 body jsonb NOT NULL, PRIMARY KEY(grain,start,scope,subject));
CREATE INDEX IF NOT EXISTS rollup_feature_subject ON result_delivery.rollup_features(scope,subject,grain,start);
CREATE INDEX IF NOT EXISTS delivery_event_start ON result_delivery.events((data->>'s_time'));
''')
    cur.execute('SELECT rule,timezone FROM result_delivery.rollup_control WHERE id=1')
    old = cur.fetchone()
    if old:
        if old != (RULE, PROFILE['timezone']):
            raise ValueError('汇总口径或业务时区改变，须显式迁移')
        return
    # 既有交付库的首次接入只登记一次，不反复全表核验。
    cur.execute('SELECT window_start,window_end FROM result_delivery.files ORDER BY ordinal')
    for start, end in cur.fetchall():
        if end > start:
            mark(cur, start, end)
    cur.execute('INSERT INTO result_delivery.rollup_control VALUES(1,%s,%s)', (RULE, PROFILE['timezone']))


def intervals(values):
    result = []
    for left, right in sorted(values):
        if right <= left:
            continue
        if result and left <= result[-1][1]:
            result[-1] = (result[-1][0], max(right, result[-1][1]))
        else:
            result.append((left, right))
    return [{'start': a.isoformat(), 'end_exclusive': b.isoformat()} for a, b in result]


def coverage(parts, start, end):
    merged = intervals((datetime.fromisoformat(p['start']), datetime.fromisoformat(p['end_exclusive'])) for p in parts)
    seconds = sum((datetime.fromisoformat(p['end_exclusive']) - datetime.fromisoformat(p['start'])).total_seconds() for p in merged)
    return {'intervals': merged, 'observed_seconds': seconds,
            'state': 'complete' if seconds == (end-start).total_seconds() else 'partial'}


def sample(start, end, values):
    if any(v is not None and (type(v) is not int or v < 0) for v in values):
        raise ValueError('Feature 数量须为非负整数或未知')
    return {'sample_count': 1, 'intervals': intervals([(start, end)]),
            'activity': dict(zip(('announce', 'withdraw'), values[:2])),
            'resources': {name: {'last': value, 'last_at': end.isoformat(), 'peak': value,
                                 'peak_at': end.isoformat() if value is not None else None,
                                 'unknown_samples': int(value is None)}
                          for name, value in zip(('ipv4_blocks', 'ipv6_blocks', 'ipv4_addresses'), values[2:])}}


def combine(parts):
    """仅合并同一对象、不重叠时间的统计；不把缺失采样变为零。"""
    parts = list(parts)  # 一个对象至多一个小时的来源文件／一天的 24 个小时。
    if not parts:
        return None
    result = {'sample_count': sum(p['sample_count'] for p in parts),
              'intervals': intervals((datetime.fromisoformat(i['start']), datetime.fromisoformat(i['end_exclusive']))
                                     for p in parts for i in p['intervals']),
              'activity': {}, 'resources': {}}
    for name in ('announce', 'withdraw'):
        values = [p['activity'][name] for p in parts]
        result['activity'][name] = None if any(v is None for v in values) else sum(values)
    for name in ('ipv4_blocks', 'ipv6_blocks', 'ipv4_addresses'):
        cells = [p['resources'][name] for p in parts]
        last = max(cells, key=lambda c: datetime.fromisoformat(c['last_at']))
        known = [c for c in cells if c['peak'] is not None]
        peak = min(known, key=lambda c: (-c['peak'], datetime.fromisoformat(c['peak_at']))) if known else None
        result['resources'][name] = {'last': last['last'], 'last_at': last['last_at'],
                                     'peak': peak['peak'] if peak else None,
                                     'peak_at': peak['peak_at'] if peak else None,
                                     'unknown_samples': sum(c['unknown_samples'] for c in cells)}
    return result


def _write_features(conn, grain, start, sql, args, transform):
    digest = hashlib.sha256()
    count, batch = 0, []
    with conn.cursor(name='rollup_stream') as reader, conn.cursor() as writer:
        reader.itersize = 256
        reader.execute(sql, args)
        for (scope, subject), rows in itertools.groupby(reader, key=lambda r: r[:2]):
            body = combine(transform(row) for row in rows)
            digest.update(encoded([scope, subject, body]))
            batch.append((grain, start, scope, subject, Json(body)))
            count += 1
            if len(batch) == 256:
                execute_values(writer, 'INSERT INTO result_delivery.rollup_features VALUES %s', batch)
                batch.clear()
        if batch:
            execute_values(writer, 'INSERT INTO result_delivery.rollup_features VALUES %s', batch)
    return count, digest.hexdigest()


def _save_period(cur, grain, start, end, body, feature_receipt):
    body.update(rule=RULE, timezone=PROFILE['timezone'], start=start.isoformat(), end_exclusive=end.isoformat(),
                feature_subjects=feature_receipt[0], feature_digest=feature_receipt[1])
    body['version'] = 'rollup_' + hashlib.sha256(encoded(body)).hexdigest()
    cur.execute('INSERT INTO result_delivery.rollup_periods VALUES(%s,%s,%s) '
                'ON CONFLICT(grain,start) DO UPDATE SET body=excluded.body', (grain, start, Json(body)))
    return body


def _build_hour(conn, cur, start):
    end = start + timedelta(hours=1)
    cur.execute('SELECT ordinal,receipt_sha,window_start,window_end,counts FROM result_delivery.files '
                'WHERE window_end>%s AND window_start<%s AND window_end>window_start ORDER BY window_start', (start, end))
    files = cur.fetchall()
    previous = None
    for row in files:
        if row[2] < start or row[3] > end or (previous is not None and row[2] < previous):
            raise ValueError('来源窗口重叠或跨小时，不能分摊文件计数')
        previous = row[3]
    parts = intervals((r[2].astimezone(ZONE), r[3].astimezone(ZONE)) for r in files)
    cur.execute("DELETE FROM result_delivery.rollup_features WHERE grain='hour' AND start=%s", (start,))
    feature_receipt = _write_features(conn, 'hour', start,
        'SELECT f.scope,f.subject,s.window_start,s.window_end,f.announ_num,f.withdraw_num,'
        'f.v4prefix_num,f.v6prefix_num,f.v4ip_num FROM result_delivery.features f '
        'JOIN result_delivery.files s USING(ordinal) WHERE s.ordinal=ANY(%s) ORDER BY f.scope,f.subject,s.window_end',
        ([r[0] for r in files],), lambda r: sample(r[2].astimezone(ZONE), r[3].astimezone(ZONE), r[4:]))
    events = {family: {kind: 0 for kind in KINDS} for family in FAMILIES}
    event_digest = hashlib.sha256()
    errors = 0
    # 按已覆盖片段选择；当前表每个 incident_id 仅一行，修订不累计。
    for part in parts:
        left, right = (datetime.fromisoformat(part[k]).replace(tzinfo=None).isoformat(sep=' ')
                       for k in ('start', 'end_exclusive'))
        with conn.cursor(name='event_rollup_stream') as reader:
            reader.itersize = 256
            reader.execute("SELECT incident_id,revision,kind,core_item,context FROM result_delivery.events "
                           "WHERE data->>'s_time'>=%s AND data->>'s_time'<%s ORDER BY incident_id", (left, right))
            for ident, revision, kind, item, context in reader:
                event_digest.update(encoded([ident, revision, context.get('reference_version'), context.get('scope')]))
                if item is None:
                    errors += 1
                    continue
                for family in FAMILIES:
                    if family == 'all' or item['address_family'] == family or (family in ('ipv4','ipv6') and item['address_family'] == 'mixed'):
                        events[family][kind] += 1
    if errors or not parts:
        events = {family: {kind: None for kind in KINDS} for family in FAMILIES}
    body = {'coverage': coverage(parts, start, end), 'events': events,
            'quality': {'projection_errors': errors, 'rejected': sum(r[4]['rejected'] for r in files),
                        'unsupported': sum(r[4]['unsupported'] for r in files), 'historical_applicability': 'Unknown'},
            'sources': [{'ordinal': r[0], 'receipt_sha256': r[1]} for r in files], 'event_digest': event_digest.hexdigest()}
    return _save_period(cur, 'hour', start, end, body, feature_receipt)


def _build_day(conn, cur, start):
    end = start + timedelta(days=1)
    cur.execute("SELECT body FROM result_delivery.rollup_periods WHERE grain='hour' AND start>=%s AND start<%s ORDER BY start", (start,end))
    hours = [r[0] for r in cur.fetchall()]
    # 空覆盖的旧开始小时仍可保留版本，但不充当已观测事件零。
    observed = [p for p in hours if p['coverage']['intervals']]
    cur.execute("DELETE FROM result_delivery.rollup_features WHERE grain='day' AND start=%s", (start,))
    receipt = _write_features(conn, 'day', start,
        "SELECT scope,subject,body FROM result_delivery.rollup_features WHERE grain='hour' AND start>=%s AND start<%s ORDER BY scope,subject,start",
        (start,end), lambda r:r[2])
    body = {'coverage': coverage([i for p in hours for i in p['coverage']['intervals']], start,end),
            'events': {family: {kind: None if not observed or any(p['events'][family][kind] is None for p in observed)
                               else sum(p['events'][family][kind] for p in observed) for kind in KINDS} for family in FAMILIES},
            'quality': {key: sum(p['quality'][key] for p in hours) for key in ('projection_errors','rejected','unsupported')},
            'sources': [{'start': p['start'], 'version': p['version']} for p in hours]}
    body['quality']['historical_applicability'] = 'Unknown'
    return _save_period(cur, 'day', start,end,body,receipt)


def refresh(conn):
    """失败时保留脏标记；独立重试无需重交付。每个业务日一个原子事务。"""
    started = time.monotonic()
    with conn, conn.cursor() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(2026092244)')
        initialize(cur)
        cur.execute('SELECT start FROM result_delivery.rollup_dirty ORDER BY start')
        days = sorted({r[0].astimezone(ZONE).replace(hour=0) for r in cur.fetchall()})
    hours_count = 0
    for day in days:
        with conn, conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(2026092244)')
            cur.execute("SET LOCAL work_mem='16MB'; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL statement_timeout='120s'")
            cur.execute('SELECT start FROM result_delivery.rollup_dirty WHERE start>=%s AND start<%s ORDER BY start', (day,day+timedelta(days=1)))
            dirty = [r[0].astimezone(ZONE) for r in cur.fetchall()]
            for start in dirty:
                _build_hour(conn,cur,start)
            _build_day(conn,cur,day)
            cur.execute('DELETE FROM result_delivery.rollup_dirty WHERE start=ANY(%s)', (dirty,))
            hours_count += len(dirty)
    return {'state':'complete','hours_refreshed':hours_count,'days_refreshed':len(days),
            'seconds':round(time.monotonic()-started,3),'rule':RULE}
