"""只读小时／日结果；长窗按可合并日结果查询，不现场扫描原 Feature。"""
import os
from datetime import datetime, timedelta

import psycopg2

from config.database import conn_11
from data_pipeline.results import delivery_read
from data_pipeline.results.rollups import PROFILE, ZONE, RULE, DEFINITIONS, KINDS, combine, coverage, hour
from services.resource_service import ResourceError, _time


def _query(params):
    if set(params) - {'start_time','end_time','grain','scope','subject','family'}:
        raise ResourceError('含有不支持的汇总查询参数',400)
    try:
        start, end = _time(params['start_time']), _time(params['end_time'])
        grain, scope, subject, family = params.get('grain','hour'), params.get('scope','collect'), params.get('subject',''), params.get('family','all')
        if grain not in ('hour','day') or scope not in ('collect','country','asn') or family not in ('all','ipv4','ipv6','unknown'):
            raise ValueError()
        if (scope == 'collect' and subject) or (scope != 'collect' and (not subject.strip() or len(subject)>128)):
            raise ValueError()
        if not datetime.fromisoformat(PROFILE['window_start']) <= start < end <= datetime.fromisoformat(PROFILE['window_end_exclusive']):
            raise ValueError()
        if end-start > timedelta(days=31):
            raise ValueError()
    except (KeyError, ValueError):
        raise ResourceError('需要有效起止时间（最多 31 天）、hour/day 粒度和单个对象；collect 不传 subject',400)
    return start,end,grain,scope,subject,family


def query_rollups(params):
    start,end,grain,scope,subject,family = _query(params)
    result = {'state':'not_configured','rule':RULE,
              'query':{'start':start.isoformat(),'end_exclusive':end.isoformat(),'grain':grain,
                       'scope':scope,'subject':subject,'family':family,'timezone':PROFILE['timezone'],'window_boundary':'[start,end)'},
              'definitions':DEFINITIONS,'points':[],'summary':None,
              'limitations':['仅累计实际采样；稀疏缺行不表示零','资源为末个采样值和采样峰值，不能跨对象加总',
                             '事件为采集范围内六类开始数，不是指定国家或 ASN 的关联事件数',
                             '结束未知不表示持续中或已恢复','国家增强与深层 Trend 未接入','历史参考适用性 Unknown']}
    if os.environ.get('DOMEYE_RESULT_DELIVERY') != 'true':
        return result
    floor = hour(start) if grain == 'hour' else start.replace(hour=0,minute=0,second=0,microsecond=0)
    try:
        with conn_11.cursor() as cur:
            cur.execute("SELECT to_regclass('result_delivery.rollup_periods')")
            if cur.fetchone()[0] is None:
                return {**result,'state':'not_calculated'}
            cur.execute('SELECT start FROM result_delivery.rollup_dirty WHERE start>=%s AND start<%s',
                        (floor, hour(end)+timedelta(hours=1) if grain=='hour' else end.replace(hour=0,minute=0,second=0,microsecond=0)+timedelta(days=1)))
            dirty = [r[0].astimezone(ZONE) for r in cur.fetchall()]
            if grain == 'day':
                dirty = [d.replace(hour=0) for d in dirty]
            cur.execute('SELECT start,body FROM result_delivery.rollup_periods WHERE grain=%s AND start>=%s AND start<%s ORDER BY start', (grain,floor,end))
            periods = cur.fetchall()
            if any(floor <= d < end for d in dirty):
                return {**result,'state':'retry_pending','message':'汇总待更新；基础 Feature、事件和 RIB 结果仍可查询'}
            if scope == 'collect':
                cur.execute('SELECT start,subject,body FROM result_delivery.rollup_features WHERE grain=%s AND start>=%s AND start<%s AND scope=%s ORDER BY start', (grain,floor,end,scope))
            else:
                cur.execute('SELECT start,subject,body FROM result_delivery.rollup_features WHERE grain=%s AND start>=%s AND start<%s AND scope=%s AND subject=%s ORDER BY start', (grain,floor,end,scope,subject))
            features = {}
            for point_start, name, body in cur.fetchall():
                if point_start in features:
                    raise ValueError('采集范围出现多个对象，不能跨对象累加')
                features[point_start] = (name,body)
        for point_start, period in periods:
            spans = period['coverage']['intervals']
            if not spans:
                continue
            if any(datetime.fromisoformat(i['start']) < start or datetime.fromisoformat(i['end_exclusive']) > end for i in spans):
                raise ResourceError('请求切入已汇总的观测片段，请扩大到完整桶或使用原始 Feature 时序',400)
            feature = features.get(point_start)
            result['points'].append({'start':period['start'],'end_exclusive':period['end_exclusive'],
                                     'version':period['version'],'coverage':period['coverage'],'quality':period['quality'],
                                     'sources':period['sources'],'subject':feature[0] if feature else subject,
                                     'feature':feature[1] if feature else None,'event_starts':period['events'][family]})
        points = result['points']
        if not points:
            return {**result,'state':'not_calculated'}
        parts = [p['feature'] for p in points if p['feature'] is not None]
        result.update(state='available',source=delivery_read.status(conn_11),
                      summary={'feature':combine(parts),
                               'coverage':coverage([i for p in points for i in p['coverage']['intervals']],start,end),
                               'event_starts':{kind:None if any(p['event_starts'][kind] is None for p in points)
                                               else sum(p['event_starts'][kind] for p in points) for kind in KINDS}})
        return result
    except (psycopg2.Error,ValueError,KeyError,TypeError) as error:
        raise ResourceError('汇总读取失败，不能解释为零；基础结果独立查询',503) from error
