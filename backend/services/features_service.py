import datetime
import os
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from flask import g, has_request_context

from config.config import BIG_COUNTRY, FEATURE_COUNTRY_TABLE, FEATURE_OTHER_TABLE, SOURCE
from config.database import conn_11
from database.feature_asn import select_as_feature_db
from database.feature_country import select_country_feature_db
from data_pipeline.overview.input import InputError
from data_pipeline.results import delivery_read
from utils import data_loader
from utils.data_loader import as_info
from utils.get_as_info import get_as_country
from utils.get_event import (
    deal_features,
    deal_outage,
    get_as_features_list,
    get_country_feature_list,
)


def _parse_datetime_range(start_time, end_time):
    if not start_time or not end_time:
        return None, None, ({'status': False, 'msg': '开始时间和结束时间不能为空！'}, 400)

    try:
        start_time_dt = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_time_dt = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None, None, ({'status': False, 'msg': '时间格式错误，应为 YYYY-MM-DD HH:MM:SS'}, 400)

    if start_time_dt > end_time_dt:
        return None, None, ({'status': False, 'msg': '开始时间不能晚于结束时间！'}, 400)

    return start_time_dt, end_time_dt, None


def _parse_page_num(raw_value):
    if raw_value in [None, ''] or str(raw_value).startswith('0'):
        return 1
    if str(raw_value).isdigit():
        return int(raw_value)
    return 1


def _parse_page_size(raw_value):
    return int(raw_value) if str(raw_value) in ['5', '10', '20', '50'] else 5


def _get_query_type(query):
    trimmed_input = query.strip().lower()
    if trimmed_input.startswith('as') and trimmed_input[2:].isdigit():
        return 'as'
    if trimmed_input.isdigit():
        return 'as'
    if trimmed_input.startswith('rrc') and trimmed_input[3:].isdigit():
        return 'collector'
    if query in ['路由采集点', 'collector']:
        return 'collector'
    return 'country'


def get_top_feature_data(start_time, end_time, target, conn=conn_11):
    if not all([start_time, end_time, target]) or not str(target).strip():
        return {'status': False, 'msg': '缺少必需参数：start_time, end_time, target'}, 400
    _, _, error = _parse_datetime_range(start_time, end_time)
    if error:
        return error

    query_type = _get_query_type(target)

    try:
        if query_type == 'as':
            data_loader.ensure_core_data_loaded()
            target = ''.join(ch for ch in target if ch.isdigit())
            as_country = get_as_country(as_info, target)
            table_name = f"feature_{BIG_COUNTRY[as_country]}" if as_country in BIG_COUNTRY else FEATURE_OTHER_TABLE
            data = select_as_feature_db(
                conn=conn,
                target=target,
                source=SOURCE,
                start_time=start_time,
                end_time=end_time,
                table_name=table_name,
            )
        elif query_type == 'country':
            data = select_country_feature_db(
                conn=conn,
                target=target,
                source=SOURCE,
                start_time=start_time,
                end_time=end_time,
                table_name=FEATURE_COUNTRY_TABLE,
            )
        elif query_type == 'collector':
            data = select_country_feature_db(
                conn=conn,
                target='collect',
                source=SOURCE,
                start_time=start_time,
                end_time=end_time,
                table_name=FEATURE_COUNTRY_TABLE,
            )
        else:
            return {'status': False, 'msg': '未知的查询类型'}, 500
    except Exception:
        return {'status': False, 'msg': '数据查询失败'}, 500

    try:
        return deal_features(data)
    except Exception:
        return {'status': False, 'msg': '特征结果无法读取'}, 500


def get_country_feature_series(start_time, end_time, country='', page_num=None, page_size=None, conn=conn_11):
    _, _, error = _parse_datetime_range(start_time, end_time)
    if error:
        return error

    data_loader.ensure_core_data_loaded()
    return get_country_feature_list(
        conn=conn,
        start_time=start_time,
        end_time=end_time,
        country=country,
        all_country_list=data_loader.country_list,
        page_num=_parse_page_num(page_num),
        page_size=_parse_page_size(page_size),
    )

def get_as_feature_series(start_time, end_time, asn='', country='', page_num=None, page_size=None, conn=conn_11):
    _, _, error = _parse_datetime_range(start_time, end_time)
    if error:
        return error

    data_loader.ensure_core_data_loaded()
    if country:
        ases = data_loader.ases_1000[data_loader.ases_1000['as_country_cn'] == country]['asn'].tolist()
    else:
        ases = data_loader.ases_1000['asn'].tolist()

    if asn:
        ases = [asn]

    return get_as_features_list(
        conn=conn,
        as_info=as_info,
        start_time=start_time,
        end_time=end_time,
        ases=ases,
        page_num=_parse_page_num(page_num),
        page_size=_parse_page_size(page_size),
    )


def _get_outage_feature(kind, country, asn, start_time, end_time, conn, prefixes=None, version=None):
    start_time_dt, end_time_dt, error = _parse_datetime_range(start_time, end_time)
    if error:
        return error
    if start_time_dt >= end_time_dt:
        return {'status': False, 'msg': '开始时间必须早于结束时间'}, 400
    if end_time_dt - start_time_dt > datetime.timedelta(days=1):
        return {'status': False, 'msg': '中断时序单次最多查询 24 小时'}, 400
    if os.environ.get('DOMEYE_RESULT_DELIVERY') != 'true':
        return {'status': False, 'msg': '中断时序缺少可核对的检测语义与处理覆盖范围'}, 503
    zone = ZoneInfo(delivery_read.PROFILE['timezone'])
    start, end = start_time_dt.replace(tzinfo=zone), end_time_dt.replace(tzinfo=zone)
    field = 'asn' if kind == 'as_outage' else 'prefix'
    try:
        meta = getattr(g, 'result_delivery', None) if has_request_context() else None
        if meta is None:
            meta = delivery_read.status(conn)
        if meta['state'] != 'available':
            raise InputError('没有可读取的完成文件覆盖范围')
        if version is not None and version != meta['version']:
            return {'status': False, 'msg': '交付版本已变化，请按同一版本重新查询'}, 409
        if country is not None and (country == 'collect' or country not in delivery_read.available_countries(conn=conn)):
            return {'status': False, 'msg': '国家名称不在当前结果源中，请使用 /api/v1/core-overview 的 metadata.countries 名称'}, 400
        coverage = delivery_read._covered_intervals(meta, start, end)
        # 采样需要未裁剪的覆盖段，才能核对窗口前发生的事件是否跨越处理缺口。
        all_coverage = delivery_read._covered_intervals(
            meta, datetime.datetime.fromisoformat(meta['start']),
            datetime.datetime.fromisoformat(meta['end_exclusive']),
        )
        rows = []
        if coverage:
            rows = delivery_read.read_outage_intervals(kind, coverage[0][0], coverage[-1][1],
                                                       country=country, asn=asn, conn=conn)
            if field == 'prefix' and prefixes is None:
                prefixes = data_loader.coarse_routing_prefixes()
                if not prefixes:
                    raise InputError('粗路由筛选数据不可用')
        points = deal_outage(pd.DataFrame(rows, columns=[field, 's_time', 'e_time']),
                             type=field, start_time=start, end_time=end, prefixes=prefixes or [],
                             coverage=all_coverage, interval_minutes=3)
        return {
            'query': {'start': start.isoformat(), 'end_exclusive': end.isoformat(),
                      'timezone': delivery_read.PROFILE['timezone'], 'window_boundary': '[start,end)',
                      'country': country, 'asn': asn},
            'metadata': {
                'version': meta['version'], 'interpretation_version': 'outage-series/v2',
                'collector_id': meta['binding']['collector'],
                'data_start': meta['start'], 'data_end_exclusive': meta['end_exclusive'],
                'coverage': {'state': 'complete' if coverage == [(start, end)] else 'partial' if coverage else 'none',
                             'intervals': [{'start': a.isoformat(), 'end_exclusive': b.isoformat()} for a, b in coverage]},
                'metric': 'concurrent_outage_objects', 'unit': field, 'sample_seconds': 180,
                'population': 'detected_asns' if field == 'asn' else 'coarse_routing_prefixes',
            },
            'data': points,
        }
    except (psycopg2.Error, InputError, ValueError, KeyError, TypeError, OSError):
        return {'status': False, 'msg': '中断时序数据或覆盖范围不可读取，不能解释为零'}, 503


def get_as_outage_feature(country, start_time, end_time, conn=conn_11, version=None):
    return _get_outage_feature('as_outage', country, None, start_time, end_time, conn, version=version)


def get_prefix_outage_feature(country, asn, start_time, end_time, conn=conn_11, prefixes=None, version=None):
    return _get_outage_feature('prefix_outage', country, asn, start_time, end_time, conn, prefixes, version)
