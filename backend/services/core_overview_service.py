"""C 首页的留存异常只读查询；不检测、不生产、不连接源数据库。"""

import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from data_pipeline.common.event_records import serialize_record
from data_pipeline.overview.input import InputError as OverviewError, load_legacy_input, overview_item as _item, overview_search_text, overview_level_filter
from data_pipeline.overview.index import DailyIndex
from data_pipeline.results.delivery_read import DeliveredIndex
from data_pipeline.overview.scale import attach_scale
from data_pipeline.overview.paths import attach_comparison
from services.resource_service import attach_rib_statistics, _time


def _load():
    if os.environ.get('DOMEYE_RESULT_DELIVERY') == 'true':
        import psycopg2
        try:
            index = DeliveredIndex()
            return index.manifest, index, index.version
        except psycopg2.Error as error:
            raise OverviewError('结果交付库暂不可读') from error
    configured = os.environ.get('DOMEYE_CORE_OVERVIEW_MANIFEST')
    if not configured:
        raise OverviewError('未配置首页留存输入')
    try:
        path = Path(configured)
        if path.stat().st_size > 65536:
            raise ValueError('清单过大')
        payload = path.read_bytes()
        manifest = json.loads(payload)
        if manifest.get('schema_version') in {'core-overview-index/v1', 'core-overview-index/v2'}:
            index = DailyIndex(path, payload)
            return manifest, index, index.version
        return load_legacy_input(path)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
        raise OverviewError('首页留存输入不可用或校验失败') from error

def get_core_overview(params):
    if set(params) - {'date', 'start_time', 'end_time', 'country', 'family', 'kind', 'level', 'hour', 'q', 'sort', 'page', 'page_size', 'version'}:
        raise OverviewError('存在不支持的查询参数', 400)
    manifest, records, version = _load()
    if params.get('version') is not None and params['version'] != version:
        raise OverviewError('留存版本已变更；请重新选择窗口，不能混用列表与详情', 409)
    profile = manifest['data_profile']
    zone = ZoneInfo(profile['timezone'])
    day = params.get('date', datetime.fromisoformat(profile['snapshot_time']).astimezone(zone).date().isoformat())
    try:
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', day):
            raise ValueError()
        start = datetime.fromisoformat(day).replace(tzinfo=zone)
    except ValueError as error:
        raise OverviewError('日期须为有效的 YYYY-MM-DD', 400) from error
    end = start + timedelta(days=1)
    ranged = 'start_time' in params or 'end_time' in params
    country = params.get('country', '').strip()
    if len(country) > 80 or any(ord(char) < 32 for char in country):
        raise OverviewError('国家或地区名称无效', 400)
    if ranged:
        if 'date' in params or 'hour' in params:
            raise OverviewError('时间区间不能同时提供 date 或 hour', 400)
        try:
            start, end = _time(params.get('start_time')), _time(params.get('end_time'))
            day = start.date().isoformat()
        except ValueError as error:
            raise OverviewError('请提供完整有效的秒级起止时间', 400) from error
    if (ranged or country) and not isinstance(records, DeliveredIndex):
        raise OverviewError('当前留存源尚不支持区间与地区查询；需要完成文件结果源', 400)
    kind = params.get('kind', 'all')
    if kind not in ['all', *manifest['kinds']]:
        raise OverviewError('该异常类型尚未接入', 400)
    raw_hour = params.get('hour')
    if raw_hour is not None and raw_hour not in {str(hour) for hour in range(24)}:
        raise OverviewError('时段须为 0 至 23', 400)
    hour = int(raw_hour) if raw_hour is not None else None
    family = params.get('family', 'all')
    if family not in {'all', 'ipv4', 'ipv6', 'unknown'}:
        raise OverviewError('地址族筛选无效', 400)
    level = params.get('level', 'all')
    sort = params.get('sort', 'severity')
    query = params.get('q', '').strip()
    if level not in {'all', 'high', 'middle', 'low', 'unknown', 'conflict'} or sort not in {'severity', 'time'} or len(query) > 120:
        raise OverviewError('筛选或排序参数无效', 400)
    try:
        page = int(params.get('page', '1'))
        page_size = int(params.get('page_size', '10'))
        if not 1 <= page <= 1000000 or not 1 <= page_size <= 100:
            raise ValueError()
    except ValueError as error:
        raise OverviewError('页码须为正整数，每页 1 至 100 条', 400) from error
    if not datetime.fromisoformat(profile['window_start']) <= start < end <= datetime.fromisoformat(profile['window_end_exclusive']):
        raise OverviewError('起止时间须有序且位于项目数据窗口内', 400)
    retained_start = datetime.fromisoformat(manifest['window']['start']).astimezone(zone)
    retained_end = datetime.fromisoformat(manifest['window']['end_exclusive']).astimezone(zone)
    cursor = retained_start.replace(hour=0, minute=0, second=0, microsecond=0)
    legacy_dates = []
    while cursor + timedelta(days=1) <= retained_end:
        if cursor >= retained_start:
            legacy_dates.append(cursor.date().isoformat())
        cursor += timedelta(days=1)
    response = {
        'state': 'window_not_retained', 'version': version,
        'metadata': {'source': manifest['source'], 'data_profile': profile,
                     'retained_window': manifest['window'], 'kinds': manifest['kinds'],
                     'interpretation_version': manifest['interpretation_version'],
                     'available_dates': sorted(records.days) if isinstance(records, (DailyIndex, DeliveredIndex)) else legacy_dates},
        'query': {'date': day, 'start': start.isoformat(), 'end_exclusive': end.isoformat(), 'kind': kind, 'hour': hour, 'family': family, 'excluded_unknown_family': None,
                  'level': level, 'sort': sort, 'q': query, 'page': page, 'page_size': page_size},
        'overview': None, 'trend': None, 'events': None,
    }
    if ranged or country:
        response['query'].update(window_mode='range' if ranged else 'day', country=country)
    if isinstance(records, DeliveredIndex):
        return attach_rib_statistics(records.query(response))
    if isinstance(records, DailyIndex):
        if records.diagnostics:
            response['metadata']['diagnostic_dates'] = sorted(records.diagnostics)
        try:
            return attach_comparison(attach_scale(records.query(response), records.path, manifest), records.path, manifest)
        except OverviewError as error:
            # 日文件不可用不抹去已经校验的目录；绝不回传部分成功指标。
            failure = {**response, 'state': 'unavailable', 'message': str(error),
                       'overview': None, 'trend': None, 'events': None}
            if error.payload and 'diagnostic' in error.payload:
                failure['diagnostic'] = error.payload['diagnostic']
            raise OverviewError(str(error), error.status, payload=failure) from error
    if not datetime.fromisoformat(manifest['window']['start']) <= start < end <= datetime.fromisoformat(manifest['window']['end_exclusive']):
        return response
    items = [_item(result, manifest.get('level_conflicts')) for result in records if start <= datetime.fromisoformat(result['record']['common']['start_time']['value'].replace('Z', '+00:00')) < end]
    response['query']['excluded_unknown_family'] = sum(item['address_family'] == 'unknown' for item in items) if family in {'ipv4', 'ipv6'} else 0
    items = [item for item in items if family == 'all' or item['address_family'] == family or (family in {'ipv4', 'ipv6'} and item['address_family'] == 'mixed')]
    record_count = len(items)
    local_hour = lambda item: datetime.fromisoformat(item['start_time'].replace('Z', '+00:00')).astimezone(zone).hour
    buckets = []
    for index in range(24):
        prefixes = {item['object'] for item in items if item['kind'] == 'prefix_outage' and local_hour(item) == index}
        buckets.append({'start': (start + timedelta(hours=index)).isoformat(), 'end_exclusive': (start + timedelta(hours=index + 1)).isoformat(), 'value': len(prefixes)})
    items = [item for item in items if (kind == 'all' or item['kind'] == kind) and (hour is None or local_hour(item) == hour)]
    items = [item for item in items if (level == 'all' or overview_level_filter(item) == level)
             and (not query or query == item['reference'] or query.lower() in overview_search_text(item))]
    items.sort(key=lambda item: item['reference'])
    items.sort(key=lambda item: item['start_time'], reverse=True)
    if sort == 'severity':
        items.sort(key=lambda item: {'high': 0, 'middle': 1, 'low': 2}.get(item['level'], 3))
    response.update({
        'state': 'available',
        'version': version,
        'overview': {'record_count': record_count, 'visible_prefixes': None, 'visible_origin_ases': None},
        'trend': {'metric': 'recorded_prefix_outage_starts_distinct', 'bucket_seconds': 3600, 'buckets': buckets},
        'events': {'total': len(items), 'items': items[(page - 1) * page_size:page * page_size],
                   'distinct_prefixes': len({item['object'] for item in items if item['kind'] == 'prefix_outage'}),
                   'page': page, 'page_size': page_size, 'page_count': (len(items) + page_size - 1) // page_size},
    })
    return response


def get_core_overview_record(params):
    if set(params) - {'ref', 'version'}:
        raise OverviewError('存在不支持的详情参数', 400)
    if not params.get('version') or not params.get('ref'):
        raise OverviewError('详情必须携带原引用及列表版本', 400)
    manifest, records, version = _load()
    if params['version'] != version:
        raise OverviewError('详情版本与当前留存输入不一致', 409)
    if isinstance(records, (DailyIndex, DeliveredIndex)):
        return records.detail(params['ref'])
    for result in records:
        if result['record']['identity']['legacy_reference'] == params['ref']:
            return {'state': 'available', 'version': version, 'item': _item(result, manifest.get('level_conflicts')),
                    'metadata': {'source': manifest['source'], 'interpretation_version': manifest['interpretation_version']},
                    'record': json.loads(serialize_record(result))}
    raise OverviewError('该留存版本中没有此引用', 404)
