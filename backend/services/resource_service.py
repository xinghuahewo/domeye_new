"""独立 RIB 统计的业务读取；不在 HTTP 中生产 Resource 或共享快照。"""
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2

from data_pipeline.results import delivery_read


PROFILE = delivery_read.PROFILE
ZONE = ZoneInfo(PROFILE['timezone'])
# 公开固定指标，与离线 Resource 单位一致；不导入计算器或提供任意指标查询。
METRIC_UNITS = {
    'ipv4_prefix_count': 'distinct_ipv4_prefix',
    'ipv6_prefix_count': 'covered_ipv6_48_block',
    'ipv6_48_count': 'covered_ipv6_48_block',
    'ipv4_address_count': 'covered_ipv4_24_block_times_256',
    'vp_count': 'distinct_first_path_asn',
    'private_as_count': 'distinct_legacy_private_tail_asn',
    'public_as_count': 'distinct_legacy_public_tail_asn',
    'path_count': 'distinct_rendered_path',
}


class ResourceError(Exception):
    def __init__(self, message, status=503):
        super().__init__(message)
        self.status = status


def _time(value):
    if not isinstance(value, str) or not re.fullmatch(
        r'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?', value,
    ):
        raise ValueError('时间格式错误')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return parsed.replace(tzinfo=ZONE) if parsed.tzinfo is None else parsed.astimezone(ZONE)


def _window(params):
    if set(params) != {'start_time', 'end_time'}:
        raise ResourceError('必须且只能提供 start_time、end_time', 400)
    try:
        start, end = _time(params['start_time']), _time(params['end_time'])
        if not start < end:
            raise ValueError('窗口须为正')
        if not datetime.fromisoformat(PROFILE['window_start']) <= start < end <= datetime.fromisoformat(PROFILE['window_end_exclusive']):
            raise ValueError('超出数据档')
    except ValueError as error:
        raise ResourceError('时间须精确到秒，窗口大于零且在项目数据范围内', 400) from error
    return start, end


def _count(value):
    if value is not None and (type(value) is not int or value < 0):
        raise ValueError('数量必须为非负整数或 null')
    return value


def _point(row):
    metrics = {}
    for name, unit in METRIC_UNITS.items():
        cell = row['resource']['metrics'][name]
        main = _count(cell['main'])
        if cell['unit'] != unit or cell['qualification'] not in {'qualified', 'unknown', 'not_applicable'}:
            raise ValueError('资源单位或适用性无效')
        if (cell['qualification'] == 'qualified') != (main is not None):
            raise ValueError('主值与适用性不一致')
        if not isinstance(cell['reason'], str):
            raise ValueError('缺少适用性原因')
        metrics[name] = {key: cell[key] for key in ('main', 'qualification', 'reason', 'unit')}
    return {
        'snapshot_id': row['snapshot_id'], 'observed_at': row['observed_at'], 'metrics': metrics,
        'metadata': {**{key: row[key] for key in ('collector_id', 'source_id', 'source_sha256', 'limitations')},
                     'rule': row['resource']['rule']},
    }


def query_resources(params):
    start, end = _window(params)
    response = {
        'state': 'not_configured',
        'query': {'start': start.isoformat(), 'end_exclusive': end.isoformat(),
                  'timezone': PROFILE['timezone'], 'scope': 'global', 'window_boundary': '[start,end)'},
        'points': [],
    }
    if os.environ.get('DOMEYE_RESULT_DELIVERY') != 'true':
        return {**response, 'message': '未配置完成文件结果交付'}
    try:
        rows = delivery_read.read_rib_statistics(start, end)
        if not rows:
            return {**response, 'state': 'not_calculated', 'message': '查询窗口内尚无独立 RIB 资源统计'}
        response.update(state='available', points=[_point(row) for row in rows])
        return response
    except (psycopg2.Error, ValueError, KeyError, TypeError) as error:
        raise ResourceError('RIB 资源统计暂不可用，不能解释为零') from error


def attach_rib_statistics(response):
    """Core 规模与事件独立可用，单时点统计不能伪装成连续规模。"""
    family = response['query']['family']
    metadata = {'state': 'not_calculated', 'family': family}
    response['metadata']['rib_statistics'] = metadata
    if response['query'].get('country'):
        metadata.update(state='not_applicable', message='地区 RIB 统计尚未生成')
        return response
    if family == 'unknown':
        metadata.update(state='not_applicable', message='该 RIB 统计不提供未知地址族规模')
        return response
    start, end = (datetime.fromisoformat(response['query'][key]) for key in ('start', 'end_exclusive'))
    try:
        rows = delivery_read.read_rib_statistics(start, end) or []
        intervals = delivery_read._covered_intervals(response['metadata']['result_delivery'], start, end)
        rows = [row for row in rows if row['collector_id'] == response['metadata']['source']['collector_id']
                and any(left <= _time(row['observed_at']) < right for left, right in intervals)]
        if not rows:
            metadata['message'] = '当前已交付窗口内没有同采集器的独立 RIB 规模'
            return response
        row = max(rows, key=lambda item: (_time(item['observed_at']), item['snapshot_id']))
        values = row['canonical']['by_family'][family]
        metrics = {name: _count(values[name]) for name in ('visible_prefixes', 'visible_origin_ases')}
        metadata.update(state='available', metrics=metrics, rule=row['canonical']['rule'],
                        **{key: row[key] for key in ('snapshot_id', 'observed_at', 'collector_id', 'source_id', 'source_sha256', 'limitations')})
        if response['overview'] is not None:
            response['overview'].update(metrics)
    except (psycopg2.Error, ValueError, KeyError, TypeError):
        metadata.update(state='unavailable', message='独立 RIB 规模暂不可用；事件结果仍可查询')
    return response
