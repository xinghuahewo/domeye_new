"""交付库的只读查询；不扫描历史 Parquet、不触发生产。"""
import hashlib
import json
from ast import literal_eval
from bisect import bisect_right
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from config.database import conn_11
from data_pipeline.common.event_records import convert_anomaly_record, serialize_record
from data_pipeline.overview.input import InputError, overview_search_text, overview_level_filter
from data_pipeline.results.event_view import COUNTRY_FIELDS, event_item

KINDS = ['prefix_outage','as_outage','hijack','sub_hijack','leak','country_outage']
PROFILE = json.loads((Path(__file__).resolve().parents[3] / 'config/data-profile.json').read_text())


def _merge_intervals(intervals):
    """合并已交付窗口；中间没有完成文件的时段不由首末时间推定覆盖。"""
    merged = []
    for start, end in sorted(intervals):
        if end <= start:
            continue  # 单 RIB 时点不构成事件活动时间覆盖。
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return merged


def _covered_intervals(meta, start, end):
    # PG 可返回 UTC；裁剪结果仍按项目业务时区输出，避免 max/min 继承源端点时区。
    zone = ZoneInfo(PROFILE['timezone'])
    start, end = start.astimezone(zone), end.astimezone(zone)
    return _merge_intervals(
        (max(start, datetime.fromisoformat(item['start']).astimezone(zone)),
         min(end, datetime.fromisoformat(item['end_exclusive']).astimezone(zone)))
        for item in meta['intervals']
    )


def _bucket_windows(intervals, start, end, seconds):
    cursor = start.replace(hour=(start.hour // (seconds // 3600)) * (seconds // 3600), minute=0, second=0, microsecond=0)
    result = []
    while cursor < end:
        boundary = cursor + timedelta(seconds=seconds)
        result.extend((max(left, cursor), min(right, boundary)) for left, right in intervals
                      if max(left, cursor) < min(right, boundary))
        cursor = boundary
    return result


def _countries(value):
    """复用来源明确记录的国家名称；不由 ASN、攻击方或搜索文本猜测归属。"""
    if isinstance(value, list):
        return {item.strip() for item in value if isinstance(item, str) and item.strip()}
    if not isinstance(value, str) or not value.strip():
        return set()
    text = value.strip()
    if text.startswith(('[', '(')):
        try:
            parsed = literal_eval(text)
            return _countries(list(parsed)) if isinstance(parsed, (list, tuple)) else set()
        except (ValueError, SyntaxError, RecursionError):
            return set()
    return {text}


def available_countries():
    with conn_11.cursor() as cur:
        cur.execute("SELECT DISTINCT subject FROM result_delivery.features WHERE scope='country'")
        countries = {row[0] for row in cur.fetchall() if row[0]}
        cur.execute("SELECT DISTINCT data->'attacked_country' FROM result_delivery.event_list")
        for row in cur.fetchall():
            countries.update(_countries(row[0]))
    return sorted(countries)


def _started_at(item):
    return datetime.fromisoformat(item['start_time'].replace('Z', '+00:00'))


def _bucket_items(items, windows):
    """只解析一次事件时间，归入有覆盖的半开桶；间隙与右端点仍排除。"""
    starts = [left for left, _ in windows]
    groups = [[] for _ in windows]
    if not windows:
        return groups
    for item in items:
        started_at = _started_at(item).astimezone(starts[0].tzinfo)
        index = bisect_right(starts, started_at) - 1
        if index >= 0 and started_at < windows[index][1]:
            groups[index].append(item)
    return groups


def _event_trends(groups, windows, projection_errors, seconds=3600, ranged=False):
    """计数当前事件事实，每次修订不会成为一次新事件；不解释为并发数。"""
    result = {
        'state': 'unavailable' if projection_errors else 'available',
        'metric': 'recorded_event_starts', 'filter_scope': 'window_country_and_family' if ranged else 'date_and_family',
        'bucket_seconds': seconds, 'series': [],
    }
    if projection_errors:
        result['message'] = '部分交付事件尚不能解释，六类趋势暂不可用；缺失不表示零。'
        return result
    counts = [Counter(item['kind'] for item in group) for group in groups]
    for kind in KINDS:
        buckets = [
            {'start': left.isoformat(), 'end_exclusive': right.isoformat(),
             'value': count[kind]}
            for (left, right), count in zip(windows, counts)
        ]
        result['series'].append({'kind': kind, 'total': sum(count[kind] for count in counts), 'buckets': buckets})
    return result


def _core_summaries(cur, where, values, search):
    """统计和分页只取标量；完整投影留到选定当前页后再读取。"""
    fields = ('reference', 'kind', 'object', 'start_time', 'address_family', 'level')
    columns = [f"core_item->>'{field}'" for field in fields]
    columns.append("core_item ? 'level_conflict'")
    if search:
        columns.extend(["core_item->>'record_number'", "core_item->'asns'",
                        "core_item->>'parent_prefix'", "core_item->>'country_name'"])
    cur.execute(f"SELECT {', '.join(columns)} FROM result_delivery.events WHERE {where} AND core_item IS NOT NULL", values)
    items = []
    for row in cur.fetchall():
        item = dict(zip(fields, row[:6]))
        if row[6]:
            item['level_conflict'] = True  # 只供既有筛选判断字段存在；不作为公开投影返回。
        if search:
            item.update(record_number=row[7], asns=row[8], country_name=row[10])
            if row[9] is not None:
                item['parent_prefix'] = row[9]
        items.append(item)
    return items


def _page_items(items, delivery):
    if not items:
        return []
    references = [item['reference'] for item in items]
    # 同一只读、可重复读事务，列表投影和统计继续绑定同一版本。
    country_fields = ', '.join(f"'{field}', data->'{field}'" for field in COUNTRY_FIELDS)
    with conn_11.cursor() as cur:
        # 只对当前页取所需字段；不传输大型路径正文，也不重新生成旧内容摘要。
        cur.execute("SELECT core_item, CASE WHEN kind IN ('as_outage','prefix_outage') "
                    "THEN data @> '{\"e_time\":null}'::jsonb ELSE false END, "
                    f"CASE WHEN kind='country_outage' THEN jsonb_build_object({country_fields}) ELSE NULL END "
                    'FROM result_delivery.events WHERE reference = ANY(%s) AND core_item IS NOT NULL', (references,))
        records = {row[0]['reference']: event_item(row[0], null_end_time=row[1],
                   country_fields=row[2], delivery=delivery) for row in cur.fetchall()}
    if len(records) != len(references) or set(records) != set(references):
        raise InputError('分页事件投影与统计不一致')
    return [records[reference] for reference in references]


def status(conn=conn_11):
    with conn.cursor() as cur:
        cur.execute('SELECT body FROM result_delivery.binding WHERE id=1')
        binding = cur.fetchone()
        if not binding:
            raise InputError('未找到结果交付绑定')
        cur.execute('SELECT ordinal,receipt_sha,window_start,window_end,counts FROM result_delivery.files ORDER BY ordinal')
        files = cur.fetchall()
        cur.execute("SELECT to_regclass('result_delivery.rib_statistics')")
        if cur.fetchone()[0] is not None:
            cur.execute('SELECT snapshot_id FROM result_delivery.rib_statistics ORDER BY snapshot_id')
            rib_versions = [row[0] for row in cur.fetchall()]
        else:
            rib_versions = []
    if not files:
        return {'state':'empty', 'files':0, 'binding':binding[0]}
    start = min(row[2] for row in files); end = max(row[3] for row in files)
    intervals = _merge_intervals((row[2], row[3]) for row in files)
    identity = [binding[0], [(r[0], r[1]) for r in files]]
    if rib_versions:
        identity.append({'rib_statistics': rib_versions})
    version = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return {'state':'available', 'version':'delivery_'+version, 'files':len(files),
            'updates':len(files)-1,'start':start.isoformat(),'end_exclusive':end.isoformat(),
            'intervals':[{'start':a.isoformat(),'end_exclusive':b.isoformat()} for a,b in intervals],
            'coverage':'partial_window', 'archive':'paused_by_user','binding':binding[0],
            'rejected':sum(r[4]['rejected'] for r in files), 'unsupported':sum(r[4]['unsupported'] for r in files),
            'limitations':['仅覆盖已交付文件；窗口外未知','历史参考适用性 Unknown','原批次未完成；不代表整窗验收','完整 Observation 归档由用户暂停']}


def read_rib_statistics(start, end):
    """只读离线生成的小型时点投影；旧库缺表不尝试初始化或补算。"""
    with conn_11.cursor() as cur:
        cur.execute("SELECT to_regclass('result_delivery.rib_statistics')")
        if cur.fetchone()[0] is None:
            return None
        cur.execute('SELECT body FROM result_delivery.rib_statistics WHERE observed_at >= %s AND observed_at < %s ORDER BY observed_at, snapshot_id', (start, end))
        return [row[0] for row in cur.fetchall()]


def read_outage_intervals(kind, start, end, *, country=None, asn=None, conn=conn_11):
    """按实际重叠读取当前检测事实；不受开始月份视图限制，不将读取失败转为空。"""
    if kind not in ('as_outage', 'prefix_outage'):
        raise ValueError('不支持的中断类型')
    field = 'asn' if kind == 'as_outage' else 'prefix'
    zone = ZoneInfo(PROFILE['timezone'])
    # 来源字段沿用检测器的业务本地时间，与兼容视图的 timestamp 类型一致。
    local_start, local_end = (value.astimezone(zone).replace(tzinfo=None) for value in (start, end))
    where = ["kind=%s", "data->>'source'=%s", "(data->>'s_time')::timestamp < %s",
             "((data->>'e_time')::timestamp > %s OR data->>'e_time' IS NULL)"]
    values = [field, kind, 'r', local_end, local_start]
    for name, value in [('country', country), ('asn', asn)]:
        if value is not None:
            where.append(f"data->>'{name}'=%s")
            values.append(value)
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT data->>%s, (data->>'s_time')::timestamp, "
                    "(data->>'e_time')::timestamp, data ? 'e_time' "
                    "FROM result_delivery.events WHERE " + ' AND '.join(where), values)
        rows = cur.fetchall()
    result = []
    for identifier, started, ended, has_end in rows:
        if not identifier or started is None or not has_end or (ended is not None and ended < started):
            raise InputError('中断记录的对象或起止时间不完整')
        result.append((identifier, started.replace(tzinfo=zone), ended.replace(tzinfo=zone) if ended else None))
    return result


def normalized_record(ref, data, context, incident_id):
    scope = context['scope']
    table = ('leak_event' if ref.startswith('leak/') else ref.split('/')[0])+'_'+ref.split('/')[1][:7].replace('-','')
    source = {'instance':scope['run_id'], 'table':table,'representation':'source_rows',
              'read_at':context['delivered_at'], 'read_scope':{'source':scope['source'],
              'start':scope['window_start'],'end_exclusive':scope['window_end']},
              'collector_id':scope['collector_id'],'detector_version':scope['computation_version'],
              'evidence_refs':[context['result_locator']]}
    return convert_anomaly_record(ref,[data],source=source,data_profile=PROFILE,
                                  existing_id=incident_id,preserve_unresolved_as_set=True)


class DeliveredIndex:
    def __init__(self):
        self.status = status()
        if self.status['state'] != 'available':
            raise InputError('尚无完成文件可查询')
        meta = self.status
        self.version = meta['version']
        self.manifest = {'data_profile':PROFILE,'kinds':KINDS,
                         'source':{'instance':meta['binding']['source_run'],'code':'r','collector_id':meta['binding']['collector'],
                                   'collector_basis':'input_manifest','confirmed_on':'Unknown','detector_version':None,'coverage':'partial_window'},
                         'window':{'start':meta['start'],'end_exclusive':meta['end_exclusive'],'source':'r'},
                         'interpretation_version':'completed-file-results/v2'}
        self.days = set()
        for interval in meta['intervals']:
            day = datetime.fromisoformat(interval['start']).astimezone(ZoneInfo(PROFILE['timezone'])).replace(hour=0,minute=0,second=0,microsecond=0)
            while day < datetime.fromisoformat(interval['end_exclusive']):
                self.days.add(day.date().isoformat()); day += timedelta(days=1)

    def query(self, response):
        meta = self.status; response['metadata']['result_delivery'] = meta
        start = datetime.fromisoformat(response['query']['start']); end = datetime.fromisoformat(response['query']['end_exclusive'])
        intervals = _covered_intervals(meta, start, end)
        country = response['query'].get('country', '')
        scoped = 'window_mode' in response['query']
        if scoped:
            response['metadata']['countries'] = available_countries()
            if country and country not in response['metadata']['countries']:
                raise InputError('当前结果源没有该国家或地区，请从地区列表选择', 400)
            response['metadata']['query_coverage'] = {
                'state': 'complete' if intervals == [(start, end)] else 'partial' if intervals else 'none',
                'intervals': [{'start': a.isoformat(), 'end_exclusive': b.isoformat()} for a, b in intervals],
                'country_basis': 'event_list.attacked_country',
            }
        if not intervals:
            return response
        requested_hour = response['query']['hour']
        if requested_hour is not None and not any(
            start + timedelta(hours=requested_hour) < b
            and start + timedelta(hours=requested_hour+1) > a for a,b in intervals
        ):
            return response
        query_times = tuple(
            value.astimezone(ZoneInfo(PROFILE['timezone'])).replace(tzinfo=None).isoformat(sep=' ')
            for interval in intervals for value in interval
        )
        window_predicate = ' OR '.join("(data->>'s_time'>=%s AND data->>'s_time'<%s)" for _ in intervals)
        # canonical_detail 将同一 start 写入 reference 和 data.s_time；生成 core_item
        # 前 normalized_record 再核对身份与时间。已准入行无需展开大型证据正文来筛选。
        event_window_predicate = ' OR '.join("(split_part(reference,'/',2)>=%s AND split_part(reference,'/',2)<%s)" for _ in intervals)
        with conn_11.cursor() as cur:
            scope_clause = ''
            values = query_times
            if country:
                cur.execute(f"SELECT reference, data->'attacked_country' FROM result_delivery.event_list WHERE ({window_predicate})", query_times)
                references = [row[0] for row in cur.fetchall() if country in _countries(row[1])]
                scope_clause = ' AND reference = ANY(%s)'
                values = (*query_times, references)
            items = _core_summaries(cur, f'({event_window_predicate}){scope_clause}', values, bool(response['query']['q']))
            cur.execute(f"SELECT count(*) FROM result_delivery.events WHERE ({window_predicate}){scope_clause} AND core_error IS NOT NULL",values)
            response['metadata']['projection_unavailable_records'] = cur.fetchone()[0]
        seconds = 3600 if end-start <= timedelta(days=2) else 21600 if end-start <= timedelta(days=7) else 86400
        windows = _bucket_windows(intervals, start, end, seconds)
        groups = _bucket_items(items, windows)
        q=response['query']; family=q['family']
        q['excluded_unknown_family']=sum(x['address_family']=='unknown' for group in groups for x in group) if family in ('ipv4','ipv6') else 0
        groups=[[x for x in group if family=='all' or x['address_family']==family or (family in ('ipv4','ipv6') and x['address_family']=='mixed')] for group in groups]
        items=[x for group in groups for x in group]
        total=len(items)
        zone=ZoneInfo(PROFILE['timezone'])
        hour=lambda x:datetime.fromisoformat(x['start_time'].replace('Z','+00:00')).astimezone(zone).hour
        buckets = [
            {'start':a.isoformat(),'end_exclusive':b.isoformat(),
             'value':len({x['object'] for x in group if x['kind']=='prefix_outage'})}
            for (a,b),group in zip(windows,groups)
        ]
        response['event_trends'] = _event_trends(
            groups, windows, response['metadata']['projection_unavailable_records'], seconds, scoped,
        )
        items=[x for x in items if (q['kind']=='all' or x['kind']==q['kind']) and (q['hour'] is None or hour(x)==q['hour']) and (q['level']=='all' or overview_level_filter(x)==q['level']) and (not q['q'] or q['q'].lower() in overview_search_text(x))]
        items.sort(key=lambda x:x['reference']);items.sort(key=lambda x:x['start_time'],reverse=True)
        if q['sort']=='severity':items.sort(key=lambda x:{'high':0,'middle':1,'low':2}.get(x['level'],3))
        page,size=q['page'],q['page_size']
        response.update(state='available',overview={'record_count':total,'visible_prefixes':None,'visible_origin_ases':None},
                        trend={'metric':'recorded_prefix_outage_starts_distinct','bucket_seconds':seconds,'buckets':buckets},
                        events={'total':len(items),'items':_page_items(items[(page-1)*size:page*size], meta),'distinct_prefixes':len({x['object'] for x in items if x['kind']=='prefix_outage'}),'page':page,'page_size':size,'page_count':(len(items)+size-1)//size})
        return response

    def detail(self, ref):
        with conn_11.cursor() as cur:
            cur.execute('SELECT incident_id,data,context,core_item,core_error FROM result_delivery.events WHERE reference=%s',(ref,))
            row=cur.fetchone()
        if row is None:raise InputError('该交付版本没有此事件引用',404)
        if row[4]:raise InputError('此事件投影尚不可用：'+row[4],503)
        result=normalized_record(ref,row[1],row[2],row[0])
        item = event_item(row[3], null_end_time='e_time' in row[1] and row[1]['e_time'] is None,
                          country_fields=row[1], delivery=self.status)
        return {'state':'available','version':self.version,'item':item,
                'metadata':{'source':self.manifest['source'],'interpretation_version':self.manifest['interpretation_version'],'result_delivery':self.status},
                'record':json.loads(serialize_record(result))}
