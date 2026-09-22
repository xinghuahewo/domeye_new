"""交付库的只读查询；不扫描历史 Parquet、不触发生产。"""
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from config.database import conn_11
from data_pipeline.common.event_records import convert_anomaly_record, serialize_record
from data_pipeline.overview.input import InputError, overview_search_text, overview_level_filter

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


def _hour_windows(intervals, day_start):
    return [
        (max(left, day_start + timedelta(hours=hour)),
         min(right, day_start + timedelta(hours=hour + 1)))
        for hour in range(24)
        for left, right in intervals
        if max(left, day_start + timedelta(hours=hour))
        < min(right, day_start + timedelta(hours=hour + 1))
    ]


def _started_at(item):
    return datetime.fromisoformat(item['start_time'].replace('Z', '+00:00'))


def _event_trends(items, windows, projection_errors):
    """计数当前事件事实，每次修订不会成为一次新事件；不解释为并发数。"""
    result = {
        'state': 'unavailable' if projection_errors else 'available',
        'metric': 'recorded_event_starts', 'filter_scope': 'date_and_family',
        'bucket_seconds': 3600, 'series': [],
    }
    if projection_errors:
        result['message'] = '部分交付事件尚不能解释，六类趋势暂不可用；缺失不表示零。'
        return result
    for kind in KINDS:
        selected = [item for item in items if item['kind'] == kind]
        buckets = [
            {'start': left.isoformat(), 'end_exclusive': right.isoformat(),
             'value': sum(left <= _started_at(item) < right for item in selected)}
            for left, right in windows
        ]
        result['series'].append({'kind': kind, 'total': len(selected), 'buckets': buckets})
    return result


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
                         'interpretation_version':'completed-file-results/v1'}
        self.days = set()
        for interval in meta['intervals']:
            day = datetime.fromisoformat(interval['start']).astimezone(ZoneInfo(PROFILE['timezone'])).replace(hour=0,minute=0,second=0,microsecond=0)
            while day < datetime.fromisoformat(interval['end_exclusive']):
                self.days.add(day.date().isoformat()); day += timedelta(days=1)

    def query(self, response):
        meta = self.status; response['metadata']['result_delivery'] = meta
        start = datetime.fromisoformat(response['query']['start']); end = datetime.fromisoformat(response['query']['end_exclusive'])
        intervals = _covered_intervals(meta, start, end)
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
        with conn_11.cursor() as cur:
            cur.execute(f"SELECT core_item FROM result_delivery.events WHERE ({window_predicate}) AND core_item IS NOT NULL",query_times)
            items = [r[0] for r in cur.fetchall()]
            cur.execute(f"SELECT count(*) FROM result_delivery.events WHERE ({window_predicate}) AND core_error IS NOT NULL",query_times)
            response['metadata']['projection_unavailable_records'] = cur.fetchone()[0]
        items = [item for item in items if any(a <= _started_at(item) < b for a,b in intervals)]
        q=response['query']; family=q['family']
        q['excluded_unknown_family']=sum(x['address_family']=='unknown' for x in items) if family in ('ipv4','ipv6') else 0
        items=[x for x in items if family=='all' or x['address_family']==family or (family in ('ipv4','ipv6') and x['address_family']=='mixed')]
        total=len(items)
        zone=ZoneInfo(PROFILE['timezone'])
        hour=lambda x:datetime.fromisoformat(x['start_time'].replace('Z','+00:00')).astimezone(zone).hour
        windows = _hour_windows(intervals, start)
        buckets = [
            {'start':a.isoformat(),'end_exclusive':b.isoformat(),
             'value':len({x['object'] for x in items if x['kind']=='prefix_outage' and a <= _started_at(x) < b})}
            for a,b in windows
        ]
        response['event_trends'] = _event_trends(
            items, windows, response['metadata']['projection_unavailable_records'],
        )
        items=[x for x in items if (q['kind']=='all' or x['kind']==q['kind']) and (q['hour'] is None or hour(x)==q['hour']) and (q['level']=='all' or overview_level_filter(x)==q['level']) and (not q['q'] or q['q'].lower() in overview_search_text(x))]
        items.sort(key=lambda x:x['reference']);items.sort(key=lambda x:x['start_time'],reverse=True)
        if q['sort']=='severity':items.sort(key=lambda x:{'high':0,'middle':1,'low':2}.get(x['level'],3))
        page,size=q['page'],q['page_size']
        response.update(state='available',overview={'record_count':total,'visible_prefixes':None,'visible_origin_ases':None},
                        trend={'metric':'recorded_prefix_outage_starts_distinct','bucket_seconds':3600,'buckets':buckets},
                        events={'total':len(items),'items':items[(page-1)*size:page*size],'distinct_prefixes':len({x['object'] for x in items if x['kind']=='prefix_outage'}),'page':page,'page_size':size,'page_count':(len(items)+size-1)//size})
        return response

    def detail(self, ref):
        with conn_11.cursor() as cur:
            cur.execute('SELECT incident_id,data,context,core_item,core_error FROM result_delivery.events WHERE reference=%s',(ref,))
            row=cur.fetchone()
        if row is None:raise InputError('该交付版本没有此事件引用',404)
        if row[4]:raise InputError('此事件投影尚不可用：'+row[4],503)
        result=normalized_record(ref,row[1],row[2],row[0])
        return {'state':'available','version':self.version,'item':row[3],
                'metadata':{'source':self.manifest['source'],'interpretation_version':self.manifest['interpretation_version'],'result_delivery':self.status},
                'record':json.loads(serialize_record(result))}
