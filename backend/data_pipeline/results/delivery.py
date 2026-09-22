"""完成文件的显式离线交付：历史正文复用 Parquet，PG 保存共享查询投影。

不解析 MRT，不恢复计算工作态；每个文件单事务，摘要相同的重复交付无副作用。
只允许一个来源批次，文件必须从 0 连续。旧批次失败状态和资格回执保持原样。
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
from psycopg2.extras import Json, execute_values
from data_pipeline.bgp.replay.snapshot_contract import decode

ZONE = ZoneInfo('Asia/Shanghai')
KINDS = ('prefix_outage', 'as_outage', 'hijack', 'sub_hijack', 'leak', 'country_outage')
METRICS = ('v4Prefix_num', 'v6Prefix_num', 'v4IP_num', 'announ_num', 'withdraw_num')


def plain(value):
    if isinstance(value, dict):
        if set(value) == {'$datetime'}:
            return value['$datetime']
        if set(value) == {'$set'}:
            return [plain(x) for x in value['$set']]
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list, set)):
        return [plain(x) for x in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    return value


def digest_file(path):
    with Path(path).open('rb') as stream:
        digest = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
        return digest.hexdigest()


def source_time(value):
    value = plain(value)
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return parsed.replace(tzinfo=ZONE) if parsed.tzinfo is None else parsed


def canonical_detail(row):
    """只映射字段名和可核实定位，不重算业务规则。原字段仍在历史正文。"""
    kind, legacy_ref = row['event_kind'], row['legacy_ref']
    if kind not in KINDS or (kind == 'leak' and not row['legacy'].get('is_leak')):
        return None
    value = plain(row['legacy'])
    object_field = {'as_outage': 'asn', 'country_outage': 'country'}.get(kind, 'prefix')
    value[object_field] = row['object']
    value['source'] = legacy_ref['source']
    if kind == 'country_outage' and isinstance(value.get('outage_ases'), list):
        value['outage_ases'] = [str(asn) if type(asn) is int and 0 <= asn <= 4294967295 else asn for asn in value['outage_ases']]
    id_field = {'hijack': 'hijack_eventid', 'sub_hijack': 'sub_hijack_eventid', 'leak': 'leak_event_id'}.get(kind, 'outage_id')
    ident = value.get('legacy_event_id') if kind == 'leak' else legacy_ref['id']
    if ident is None:
        raise ValueError('已判定事件缺少可查询编号')
    value[id_field] = ident
    if kind in ('hijack', 'sub_hijack'):
        value[kind + '_level'] = value.get(kind + '_level', value.get('level'))
        value[kind + '_level_info'] = value.get('level_info')
    if kind == 'leak':
        value['is_leak'] = int(value['is_leak'])
    for name in ('hijacked_as', 'hijacker_as'):
        if isinstance(value.get(name), list):
            value[name] = str(value[name])
    duration = value.get('duration')
    if isinstance(duration, str):
        parts = re.fullmatch(r'(\d+) days (\d+) hours (\d+) minutes (\d+) seconds', duration)
        if parts:
            days, hours, minutes, seconds = map(int, parts.groups())
            value['duration'] = str(timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds))
    start = source_time(value['s_time']).astimezone(ZONE).replace(tzinfo=None).isoformat(sep=' ')
    value['s_time'] = start
    if value.get('e_time'):
        value['e_time'] = source_time(value['e_time']).astimezone(ZONE).replace(tzinfo=None).isoformat(sep=' ')
    ref = f"{kind}/{start}/{str(row['object']).replace('/', '-')}/{ident}/{value['source']}"
    return ref, value


def initialize(conn, binding):
    with conn, conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(2026092244)")
        cur.execute("SELECT to_regclass('result_delivery.binding')")
        if cur.fetchone()[0] is None:
            cur.execute("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','v','m','p')")
            if cur.fetchone()[0]:
                raise ValueError('交付只能初始化独立空数据库，不能覆盖既有系统表')
        cur.execute('''CREATE SCHEMA IF NOT EXISTS result_delivery;
CREATE TABLE IF NOT EXISTS result_delivery.binding (id integer PRIMARY KEY CHECK(id=1), body jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS result_delivery.files (
 ordinal integer PRIMARY KEY, source_id text UNIQUE NOT NULL, receipt_sha text NOT NULL,
 receipt jsonb NOT NULL, window_start timestamptz NOT NULL, window_end timestamptz NOT NULL,
 counts jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS result_delivery.features (
 ordinal integer NOT NULL, t timestamp NOT NULL, scope text NOT NULL, subject text NOT NULL,
 country text, legacy_table text NOT NULL, v4prefix_num bigint, v6prefix_num bigint,
 v4ip_num bigint, announ_num bigint, withdraw_num bigint,
 PRIMARY KEY(ordinal,scope,subject));
CREATE INDEX IF NOT EXISTS feature_subject_time ON result_delivery.features(scope,subject,t);
CREATE TABLE IF NOT EXISTS result_delivery.revisions (
 incident_id text NOT NULL, revision integer NOT NULL, ordinal integer NOT NULL,
 row_number integer NOT NULL, kind text NOT NULL, reference text, PRIMARY KEY(incident_id,revision));
CREATE TABLE IF NOT EXISTS result_delivery.events (
 incident_id text PRIMARY KEY, revision integer NOT NULL, ordinal integer NOT NULL,
 row_number integer NOT NULL, kind text NOT NULL, reference text UNIQUE NOT NULL,
 data jsonb NOT NULL, context jsonb NOT NULL, core_item jsonb, core_error text);
CREATE INDEX IF NOT EXISTS event_kind ON result_delivery.events(kind);
CREATE TABLE IF NOT EXISTS result_delivery.event_list (
 reference text PRIMARY KEY, ordinal integer NOT NULL, data jsonb NOT NULL);
''')
        cur.execute('SELECT body FROM result_delivery.binding WHERE id=1')
        old = cur.fetchone()
        if old and {k:v for k,v in old[0].items() if k != 'projection_revision'} != binding:
            raise ValueError('数据库已绑定不同来源批次或交付版本')
        cur.execute('INSERT INTO result_delivery.binding VALUES(1,%s) ON CONFLICT DO NOTHING', (Json(binding),))


def import_file(conn, receipt_path, expected, *, window_start, window_end):
    path = Path(receipt_path)
    content = path.read_bytes()
    receipt = json.loads(content)
    ordinal, file = receipt['ordinal'], receipt['file']
    sha = hashlib.sha256(content).hexdigest()
    source = receipt['source_receipt']
    if (source.get('kind') != 'source_complete' or source['ordinal'] != ordinal
            or source['source_id'] != receipt['source_id'] or expected['source_id'] != receipt['source_id']
            or source['source_sha'] != expected['sha256']):
        raise ValueError('完成回执与输入身份不一致')
    body = Path(file['path'])
    before = body.stat()
    if before.st_size != file['size'] or digest_file(body) != file['sha256']:
        raise ValueError('结果正文大小或摘要不一致')
    counts = {'feature': 0, 'business_revision': 0, 'event_operation': 0, 'rows': 0,
              'rejected': source['counts']['rejected'], 'unsupported': source['counts']['unsupported']}
    with conn, conn.cursor() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(2026092244)')
        cur.execute('SELECT receipt_sha FROM result_delivery.files WHERE ordinal=%s', (ordinal,))
        old = cur.fetchone()
        if old:
            if old[0] != sha:
                raise ValueError('同一序号的完成回执发生改变')
            return {'ordinal': ordinal, 'status': 'already_delivered'}
        cur.execute('SELECT coalesce(max(ordinal),-1)+1 FROM result_delivery.files')
        if cur.fetchone()[0] != ordinal:
            raise ValueError('只允许按原顺序交付连续完成文件')
        features = []
        for batch in pq.ParquetFile(body).iter_batches(batch_size=4, columns=['table', 'payload']):
            for stored in batch.to_pylist():
                number = counts['rows']; counts['rows'] += 1
                if stored['table'] not in ('feature_result', 'detection_result'):
                    continue
                row = decode(stored['payload'])
                if stored['table'] == 'feature_result':
                    if row['mode'] != 'ordinary':
                        raise ValueError('此交付器只支持普通 Feature')
                    raw = row['raw']; window = row['window']
                    if row['source_id'] != receipt['source_id']:
                        raise ValueError('Feature 来源身份不一致')
                    t = source_time(window['file_time']).astimezone(ZONE).replace(tzinfo=None)
                    values = raw['values']
                    features.append((ordinal, t, raw['scope'], str(raw['subject']), raw.get('country'), raw['legacy_table'], *(values.get(k) for k in METRICS)))
                    counts['feature'] += 1
                    if len(features) >= 1000:
                        execute_values(cur, 'INSERT INTO result_delivery.features VALUES %s', features); features.clear()
                elif row['kind'] == 'business_revision':
                    mapped = canonical_detail(row)
                    ref, data = mapped if mapped else (None, None)
                    cur.execute('SELECT max(revision) FROM result_delivery.revisions WHERE incident_id=%s', (row['incident_id'],))
                    prior = cur.fetchone()[0]
                    if row['revision'] != (prior or 0) + 1:
                        raise ValueError(f"事件修订不连续: {row['incident_id']}")
                    cur.execute('INSERT INTO result_delivery.revisions VALUES(%s,%s,%s,%s,%s,%s)', (row['incident_id'], row['revision'], ordinal, number, row['event_kind'], ref))
                    if mapped:
                        context = {k: plain(row[k]) for k in ('scope', 'end_state', 'conclusion', 'reference_version', 'reference_historical_applicability')}
                        context['delivered_at'] = datetime.now(timezone.utc).isoformat()
                        context['result_locator'] = {'path':str(body),'sha256':file['sha256'],'row':number,'ordinal':ordinal,'revision':row['revision']}
                        from data_pipeline.results.delivery_read import normalized_record
                        from data_pipeline.overview.input import overview_item
                        core_item, core_error = None, None
                        try:
                            core_item = overview_item(normalized_record(ref, data, context, row['incident_id']))
                        except (ValueError, KeyError, TypeError) as error:
                            # 仅隔离可解释性投影；原业务事实和引用仍留存，质量数字对外可查。
                            core_error = str(error)
                        cur.execute('''INSERT INTO result_delivery.events VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(incident_id) DO UPDATE SET revision=excluded.revision,ordinal=excluded.ordinal,
row_number=excluded.row_number,kind=excluded.kind,reference=excluded.reference,data=excluded.data,context=excluded.context,core_item=excluded.core_item,core_error=excluded.core_error''',
                                    (row['incident_id'],row['revision'],ordinal,number,row['event_kind'],ref,Json(data),Json(context),Json(core_item) if core_item else None,core_error))
                    counts['business_revision'] += 1
                elif row['kind'] in ('event_start', 'event_end', 'event_as_outage_update', 'event_country_outage_update'):
                    data = plain(row['legacy']); ref = data['detail_url']
                    if row['kind'] != 'event_start':
                        cur.execute('SELECT 1 FROM result_delivery.event_list WHERE reference=%s', (ref,))
                        if not cur.fetchone():
                            raise ValueError('事件修订没有已交付的开始记录')
                    cur.execute('''INSERT INTO result_delivery.event_list VALUES(%s,%s,%s)
ON CONFLICT(reference) DO UPDATE SET ordinal=excluded.ordinal,
data=result_delivery.event_list.data || excluded.data''', (ref,ordinal,Json(data)))
                    counts['event_operation'] += 1
        if features:
            execute_values(cur, 'INSERT INTO result_delivery.features VALUES %s', features)
        identity = lambda stat: (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        if counts['rows'] != file['rows'] or identity(body.stat()) != identity(before):
            raise ValueError('结果行数或读取期间文件身份发生改变')
        cur.execute('INSERT INTO result_delivery.files VALUES(%s,%s,%s,%s,%s,%s,%s)',
                    (ordinal,receipt['source_id'],sha,Json(receipt),window_start,window_end,Json(counts)))
        _create_query_views(cur)
    return {'ordinal': ordinal, 'status': 'delivered', **counts}


def create_query_views(conn):
    """显式修复既有查询视图；日常交付在文件事务内完成。"""
    with conn, conn.cursor() as cur:
        _create_query_views(cur)


def _create_query_views(cur):
    """新投影的一组兼容视图；没有旧数据库回退，也不创建旧写入器。"""
    from psycopg2 import sql
    spec = json.loads(Path(__file__).with_name('delivery_views.json').read_text())
    cur.execute("SELECT DISTINCT to_char(window_start AT TIME ZONE 'Asia/Shanghai','YYYYMM') FROM result_delivery.files")
    months = [x[0] for x in cur.fetchall()]
    for month in months:
        for table, fields in spec.items():
            columns = []
            for field, typ in fields.items():
                if typ == 'jsonb':
                    expr = sql.SQL("data->{} AS {}").format(sql.Literal(field), sql.Identifier(field))
                else:
                    expr = sql.SQL("(data->>{})::{} AS {}").format(sql.Literal(field), sql.SQL(typ), sql.Identifier(field))
                columns.append(expr)
            relation = 'event_list' if table == 'event_table' else 'events'
            where = sql.SQL("replace(substr(data->>'s_time',1,7),'-','')={}").format(sql.Literal(month))
            if relation == 'events':
                where += sql.SQL(' AND kind={}').format(sql.Literal('leak' if table=='leak_event' else table))
            cur.execute(sql.SQL('CREATE OR REPLACE VIEW {} AS SELECT {} FROM result_delivery.{} WHERE {}').format(
                sql.Identifier(table+'_'+month), sql.SQL(',').join(columns), sql.Identifier(relation), where))
    cur.execute("CREATE OR REPLACE VIEW feature_country AS SELECT t,'r'::text AS source,subject AS country,v4prefix_num,v6prefix_num,v4ip_num,announ_num,withdraw_num FROM result_delivery.features WHERE scope IN ('country','collect')")
    cur.execute("SELECT DISTINCT legacy_table FROM result_delivery.features WHERE scope='asn'")
    tables = [x[0] for x in cur.fetchall()]
    # 基础表名和按月表名都来自同一个投影，适配现有 ASN 查询路由。
    for table in sorted(set(tables + [re.sub(r'_\d{6}$','',t) for t in tables])):
        if not re.fullmatch(r'feature_[A-Za-z0-9_]+', table):
            raise ValueError('非法 Feature 表定位')
        pattern = table if re.search(r'_\d{6}$',table) else table+'_%'
        cur.execute(sql.SQL("CREATE OR REPLACE VIEW {} AS SELECT t,'r'::text AS source,subject AS asn,country,v4prefix_num,v6prefix_num,v4ip_num,announ_num,withdraw_num FROM result_delivery.features WHERE scope='asn' AND legacy_table LIKE {}").format(sql.Identifier(table.lower()),sql.Literal(pattern)))
