"""交付库的只读查询；不扫描历史 Parquet、不触发生产。"""
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from config.database import conn_11
from data_pipeline.common.event_records import convert_anomaly_record, serialize_record
from data_pipeline.overview.input import InputError, overview_item, overview_search_text, overview_level_filter

KINDS = ['prefix_outage','as_outage','hijack','sub_hijack','leak','country_outage']
PROFILE = json.loads((Path(__file__).resolve().parents[3] / 'config/data-profile.json').read_text())


def status(conn=conn_11):
    with conn.cursor() as cur:
        cur.execute('SELECT body FROM result_delivery.binding WHERE id=1')
        binding = cur.fetchone()
        if not binding:
            raise InputError('未找到结果交付绑定')
        cur.execute('SELECT ordinal,receipt_sha,window_start,window_end,counts FROM result_delivery.files ORDER BY ordinal')
        files = cur.fetchall()
    if not files:
        return {'state':'empty', 'files':0, 'binding':binding[0]}
    start = min(row[2] for row in files); end = max(row[3] for row in files)
    version = hashlib.sha256(json.dumps([binding[0],[(r[0],r[1]) for r in files]],sort_keys=True).encode()).hexdigest()
    return {'state':'available', 'version':'delivery_'+version, 'files':len(files),
            'updates':len(files)-1,'start':start.isoformat(),'end_exclusive':end.isoformat(),
            'coverage':'partial_window', 'archive':'paused_by_user','binding':binding[0],
            'rejected':sum(r[4]['rejected'] for r in files), 'unsupported':sum(r[4]['unsupported'] for r in files),
            'limitations':['仅覆盖已交付文件；窗口外未知','历史参考适用性 Unknown','原批次未完成；不代表整窗验收','完整 Observation 归档由用户暂停']}


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
        day = datetime.fromisoformat(meta['start']).astimezone(ZoneInfo(PROFILE['timezone'])).replace(hour=0,minute=0,second=0,microsecond=0)
        while day < datetime.fromisoformat(meta['end_exclusive']):
            self.days.add(day.date().isoformat()); day += timedelta(days=1)

    def query(self, response):
        meta = self.status; response['metadata']['result_delivery'] = meta
        start = datetime.fromisoformat(response['query']['start']); end = datetime.fromisoformat(response['query']['end_exclusive'])
        left=max(start,datetime.fromisoformat(meta['start'])); right=min(end,datetime.fromisoformat(meta['end_exclusive']))
        if left >= right:
            return response
        requested_hour = response['query']['hour']
        if requested_hour is not None and (start+timedelta(hours=requested_hour+1) <= left or start+timedelta(hours=requested_hour) >= right):
            return response
        with conn_11.cursor() as cur:
            cur.execute("SELECT core_item FROM result_delivery.events WHERE data->>'s_time'>=%s AND data->>'s_time'<%s AND core_item IS NOT NULL",(left.astimezone(ZoneInfo(PROFILE['timezone'])).replace(tzinfo=None).isoformat(sep=' '),right.astimezone(ZoneInfo(PROFILE['timezone'])).replace(tzinfo=None).isoformat(sep=' ')))
            items = [r[0] for r in cur.fetchall()]
            cur.execute('SELECT count(*) FROM result_delivery.events WHERE core_error IS NOT NULL')
            response['metadata']['projection_unavailable_records'] = cur.fetchone()[0]
        q=response['query']; family=q['family']
        q['excluded_unknown_family']=sum(x['address_family']=='unknown' for x in items) if family in ('ipv4','ipv6') else 0
        items=[x for x in items if family=='all' or x['address_family']==family or (family in ('ipv4','ipv6') and x['address_family']=='mixed')]
        total=len(items)
        zone=ZoneInfo(PROFILE['timezone'])
        hour=lambda x:datetime.fromisoformat(x['start_time'].replace('Z','+00:00')).astimezone(zone).hour
        buckets=[]
        # 只返回实际覆盖的桶；部分小时明确截断，窗口外不补零。
        for n in range(24):
            a=max(start+timedelta(hours=n),left); b=min(start+timedelta(hours=n+1),right)
            if a<b:
                buckets.append({'start':a.isoformat(),'end_exclusive':b.isoformat(),'value':len({x['object'] for x in items if x['kind']=='prefix_outage' and hour(x)==n})})
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
