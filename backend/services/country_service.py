"""国家特征与异常态势工作台服务。"""

from ast import literal_eval
from collections import Counter, defaultdict
from copy import deepcopy
import datetime
import os
import threading
import time
from zoneinfo import ZoneInfo

import psycopg2
from flask import g, has_request_context

from config.database import conn_11
from services.feature_statistics import activity_summary
from data_pipeline.overview.input import InputError
from data_pipeline.results import delivery_read
from database.country_workbench import (
    get_country_event_counts,
    get_country_feature_aggregates,
    get_country_feature_series,
    get_country_sparklines,
)


_COUNTRY_CACHE = {}
_COUNTRY_CACHE_LOCK = threading.Lock()
_COUNTRY_CACHE_TTL_SECONDS = 30
_COUNTRY_CACHE_MAX_ENTRIES = 32

COUNTRY_SERIES_UNITS = {
    'announce': 'accepted_route_element', 'withdraw': 'accepted_route_element',
    'ipv4_prefixes': 'ipv4_24_equivalent', 'ipv6_prefixes': 'ipv6_48_equivalent',
    'ipv4_addresses': 'ipv4_address',
}


def get_country_series(country, start_time, end_time, version=None, conn=conn_11):
    """单国图表直接复用已有五分钟查询；不做排名、前窗比较或异常聚合。"""
    start, end, error = _parse_range(start_time, end_time)
    if error:
        return error
    if not country or country == 'collect' or len(country) > 80 or any(ord(c) < 32 for c in country):
        return {'status': False, 'msg': '必须指定有效的国家名称'}, 400
    if os.environ.get('DOMEYE_RESULT_DELIVERY') != 'true':
        return {'status': False, 'msg': '单国时序缺少可核对的数据版本与覆盖'}, 503
    zone = ZoneInfo(delivery_read.PROFILE['timezone'])
    start, end = start.replace(tzinfo=zone), end.replace(tzinfo=zone)
    try:
        meta = getattr(g, 'result_delivery', None) if has_request_context() else None
        if meta is None:
            meta = delivery_read.status(conn)
        if meta['state'] != 'available':
            raise InputError('没有可读取的完成文件覆盖范围')
        if version is not None and version != meta['version']:
            return {'status': False, 'msg': '交付版本已变化，请按同一版本重新查询'}, 409
        if country not in delivery_read.available_countries(conn=conn):
            return {'status': False, 'msg': '国家名称不在当前结果源中，请使用 /api/v1/core-overview 的 metadata.countries 名称'}, 400
        coverage = delivery_read._covered_intervals(meta, start, end)
        rows = get_country_feature_series(conn, country, coverage[0][0].replace(tzinfo=None),
                                          coverage[-1][1].replace(tzinfo=None)) if coverage else []
        points, times = [], set()
        for row in rows:
            at = row['time'].replace(tzinfo=zone)
            if not any(left <= at < right for left, right in coverage):
                continue
            if at in times or any(row[key] is not None and (type(row[key]) is not int or row[key] < 0)
                                  for key in COUNTRY_SERIES_UNITS):
                raise InputError('国家时序含重复时点或无效数值')
            times.add(at)
            points.append({**_feature_point(row), 'time': at.isoformat()})
        return {
            'query': {'country': country, 'start': start.isoformat(), 'end_exclusive': end.isoformat(),
                      'timezone': str(zone), 'window_boundary': '[start,end)'},
            'metadata': {'version': meta['version'], 'collector_id': meta['binding']['collector'],
                         'interpretation_version': 'country-feature-series/v1', 'sample_seconds': 300,
                         'data_start': meta['start'], 'data_end_exclusive': meta['end_exclusive'],
                         'units': COUNTRY_SERIES_UNITS,
                         'coverage': {'state': 'complete' if coverage == [(start, end)] else 'partial' if coverage else 'none',
                                      'intervals': [{'start': a.isoformat(), 'end_exclusive': b.isoformat()} for a,b in coverage]}},
            'data': points,
        }
    except (psycopg2.Error, InputError, ValueError, KeyError, TypeError, AttributeError):
        return {'status': False, 'msg': '国家时序数据或覆盖范围不可读取，不能解释为零'}, 503


def _cached_country_result(key):
    if key is None:
        return None
    now = time.monotonic()
    with _COUNTRY_CACHE_LOCK:
        cached = _COUNTRY_CACHE.get(key)
        if cached is None:
            return None
        created_at, payload = cached
        if now - created_at > _COUNTRY_CACHE_TTL_SECONDS:
            _COUNTRY_CACHE.pop(key, None)
            return None
        return deepcopy(payload)


def _store_country_result(key, payload):
    if key is None:
        return payload
    now = time.monotonic()
    with _COUNTRY_CACHE_LOCK:
        expired = [
            cache_key
            for cache_key, (created_at, _) in _COUNTRY_CACHE.items()
            if now - created_at > _COUNTRY_CACHE_TTL_SECONDS
        ]
        for cache_key in expired:
            _COUNTRY_CACHE.pop(cache_key, None)
        if len(_COUNTRY_CACHE) >= _COUNTRY_CACHE_MAX_ENTRIES:
            oldest_key = min(_COUNTRY_CACHE, key=lambda item: _COUNTRY_CACHE[item][0])
            _COUNTRY_CACHE.pop(oldest_key, None)
        _COUNTRY_CACHE[key] = (now, deepcopy(payload))
    return payload


def _parse_range(start_time, end_time):
    try:
        start = datetime.datetime.strptime(start_time or '', '%Y-%m-%d %H:%M:%S')
        end = datetime.datetime.strptime(end_time or '', '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None, None, ({'status': False, 'msg': '时间格式错误，应为 YYYY-MM-DD HH:MM:SS'}, 400)
    if start >= end:
        return None, None, ({'status': False, 'msg': '开始时间必须早于结束时间'}, 400)
    if end - start > datetime.timedelta(days=1):
        return None, None, ({'status': False, 'msg': '国家工作台最多支持 24 小时窗口'}, 400)
    return start, end, None


def _parse_limit(raw_value):
    try:
        value = int(raw_value or 6)
    except (TypeError, ValueError):
        value = 6
    return min(12, max(3, value))


def _row_value(row, key):
    try:
        return row[key]
    except (KeyError, TypeError):
        return None


def _int_value(value, default=0):
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _nullable_int(value):
    return None if value is None else _int_value(value)


def _time_value(value):
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return str(value) if value else None


def _change_rate(current, previous):
    if previous == 0:
        return None
    return round((current - previous) / previous * 100, 1)


def _delta(current, baseline):
    if current is None or baseline is None:
        return None
    return current - baseline


def _country_values(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith(('[', '(')):
        try:
            return _country_values(literal_eval(text))
        except (ValueError, SyntaxError):
            pass
    return [text]


def _event_counters(rows):
    counts = Counter()
    high_counts = Counter()
    for row in rows:
        weight = _int_value(_row_value(row, 'event_count'))
        for country in set(_country_values(_row_value(row, 'attacked_country'))):
            counts[country] += weight
            if str(_row_value(row, 'level')).lower() == 'high':
                high_counts[country] += weight
    return counts, high_counts


def _feature_point(row):
    return {
        'time': _time_value(_row_value(row, 'time')),
        'announce': _nullable_int(_row_value(row, 'announce')),
        'withdraw': _nullable_int(_row_value(row, 'withdraw')),
        'ipv4_prefixes': _nullable_int(_row_value(row, 'ipv4_prefixes')),
        'ipv6_prefixes': _nullable_int(_row_value(row, 'ipv6_prefixes')),
        'ipv4_addresses': _nullable_int(_row_value(row, 'ipv4_addresses')),
    }


def _country_profile(row, anomaly_count=0, high_risk_count=0):
    country = str(_row_value(row, 'country') or '').strip()
    ipv4_prefixes = _nullable_int(_row_value(row, 'ipv4_prefixes'))
    ipv6_prefixes = _nullable_int(_row_value(row, 'ipv6_prefixes'))
    ipv4_addresses = _nullable_int(_row_value(row, 'ipv4_addresses'))
    baseline_ipv4_prefixes = _nullable_int(_row_value(row, 'baseline_ipv4_prefixes'))
    baseline_ipv6_prefixes = _nullable_int(_row_value(row, 'baseline_ipv6_prefixes'))
    baseline_ipv4_addresses = _nullable_int(_row_value(row, 'baseline_ipv4_addresses'))
    ipv4_prefix_change = _delta(ipv4_prefixes, baseline_ipv4_prefixes)
    ipv6_prefix_change = _delta(ipv6_prefixes, baseline_ipv6_prefixes)
    ipv4_address_change = _delta(ipv4_addresses, baseline_ipv4_addresses)
    resource_deltas = [
        abs(value)
        for value in (ipv4_prefix_change, ipv6_prefix_change)
        if value is not None
    ]
    resource_change_rates = [
        abs(value)
        for value in (
            _change_rate(ipv4_prefixes, baseline_ipv4_prefixes)
            if ipv4_prefixes is not None and baseline_ipv4_prefixes is not None
            else None,
            _change_rate(ipv6_prefixes, baseline_ipv6_prefixes)
            if ipv6_prefixes is not None and baseline_ipv6_prefixes is not None
            else None,
        )
        if value is not None
    ]
    return {
        'country': country,
        **activity_summary(row),
        'latest_observation': _time_value(_row_value(row, 'latest_observation')),
        'ipv4_prefixes': ipv4_prefixes,
        'ipv6_prefixes': ipv6_prefixes,
        'ipv4_addresses': ipv4_addresses,
        'ipv4_prefix_change': ipv4_prefix_change,
        'ipv6_prefix_change': ipv6_prefix_change,
        'ipv4_address_change': ipv4_address_change,
        'resource_change': max(resource_deltas) if resource_deltas else None,
        'resource_change_rate': max(resource_change_rates) if resource_change_rates else None,
        'peak_time': _time_value(_row_value(row, 'peak_time')),
        'anomaly_count': anomaly_count,
        'high_risk_count': high_risk_count,
        'sparkline': [],
        'series': [],
    }


def _ranking(profiles, key, limit, predicate=None):
    candidates = [
        profile
        for profile in profiles
        if profile[key] is not None and (predicate is None or predicate(profile))
    ]
    return sorted(
        candidates,
        key=lambda item: (-item[key], -item['high_risk_count'], item['country']),
    )[:limit]


def get_country_workbench(start_time, end_time, country='', limit=None, conn=conn_11):
    start, end, error = _parse_range(start_time, end_time)
    if error:
        return error
    ranking_limit = _parse_limit(limit)
    selected_name = str(country or '').strip()
    cache_key = (
        start.strftime('%Y-%m-%d %H:%M:%S'),
        end.strftime('%Y-%m-%d %H:%M:%S'),
        selected_name,
        ranking_limit,
    ) if conn is conn_11 else None
    cached = _cached_country_result(cache_key)
    if cached is not None:
        return cached
    previous_start = start - (end - start)
    feature_rows = get_country_feature_aggregates(
        conn=conn,
        previous_start=previous_start,
        current_start=start,
        end_time=end,
    )
    event_rows = get_country_event_counts(conn=conn, start_time=start, end_time=end)
    anomaly_counts, high_risk_counts = _event_counters(event_rows)
    profiles = {}
    for row in feature_rows:
        name = str(_row_value(row, 'country') or '').strip()
        if not name:
            continue
        profiles[name] = _country_profile(
            row,
            anomaly_count=anomaly_counts[name],
            high_risk_count=high_risk_counts[name],
        )
    for name, anomaly_count in anomaly_counts.items():
        if name not in profiles:
            profiles[name] = _country_profile(
                {'country': name},
                anomaly_count=anomaly_count,
                high_risk_count=high_risk_counts[name],
            )

    profile_values = list(profiles.values())
    data_profiles = [profile for profile in profile_values if profile['sample_count'] > 0]
    update_rankings = _ranking(data_profiles, 'update_total', ranking_limit)
    withdraw_rate_rankings = _ranking(
        data_profiles,
        'withdraw_rate',
        ranking_limit,
        predicate=lambda item: item['update_total'] > 0,
    )
    resource_change_rankings = _ranking(
        data_profiles,
        'resource_change_rate',
        ranking_limit,
        predicate=lambda item: item['resource_change_rate'] is not None,
    )
    anomaly_rankings = _ranking(
        profile_values,
        'anomaly_count',
        ranking_limit,
        predicate=lambda item: item['anomaly_count'] > 0,
    )
    selected = profiles.get(selected_name) if selected_name else None
    if selected_name and selected is None:
        selected = _country_profile({'country': selected_name})
        profiles[selected_name] = selected

    sparkline_countries = {
        profile['country']
        for ranking in (
            update_rankings,
            withdraw_rate_rankings,
            resource_change_rankings,
            anomaly_rankings,
        )
        for profile in ranking
    }
    if selected_name:
        sparkline_countries.add(selected_name)
    sparkline_map = defaultdict(list)
    for row in get_country_sparklines(
        conn=conn,
        countries=sorted(sparkline_countries),
        start_time=start,
        end_time=end,
    ):
        name = str(_row_value(row, 'country') or '').strip()
        if not name:
            continue
        sparkline_map[name].append({
            'time': _time_value(_row_value(row, 'bucket')),
            'announce': _int_value(_row_value(row, 'announce')),
            'withdraw': _int_value(_row_value(row, 'withdraw')),
        })
    for name, points in sparkline_map.items():
        if name in profiles:
            profiles[name]['sparkline'] = points
    if selected is not None:
        selected['series'] = [
            _feature_point(row)
            for row in get_country_feature_series(
                conn=conn,
                country=selected_name,
                start_time=start,
                end_time=end,
            )
        ]

    latest_observation = max(
        (profile['latest_observation'] for profile in data_profiles if profile['latest_observation']),
        default=None,
    )
    result = {
        'start_time': start.strftime('%Y-%m-%d %H:%M:%S'),
        'end_time': end.strftime('%Y-%m-%d %H:%M:%S'),
        'timezone': 'Asia/Shanghai',
        'window_boundary': '[start,end)',
        'latest_observation': latest_observation,
        'country_count': len(data_profiles),
        'countries_with_anomalies': sum(
            1 for profile in profile_values if profile['anomaly_count'] > 0
        ),
        'update_leader': update_rankings[0] if update_rankings else None,
        'withdraw_rate_leader': withdraw_rate_rankings[0] if withdraw_rate_rankings else None,
        'resource_change_leader': resource_change_rankings[0] if resource_change_rankings else None,
        'update_rankings': update_rankings,
        'withdraw_rate_rankings': withdraw_rate_rankings,
        'resource_change_rankings': resource_change_rankings,
        'anomaly_rankings': anomaly_rankings,
        'selected_country': selected,
    }
    return _store_country_result(cache_key, result)
