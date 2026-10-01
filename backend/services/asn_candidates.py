"""完成文件中有实际 Feature 样本的 AS 候选；查询不创建索引或生产数据。"""
import math
import os
import re
from datetime import timedelta

import psycopg2
from flask import g, has_request_context

from config.database import conn_11
from data_pipeline.overview.input import InputError
from data_pipeline.results import delivery_read
from services.resource_service import _time
from utils.asn_reference import get_asn_identity, REFERENCE_NOTE


def read_candidates(conn, start, end, country, query, sort, order, page, page_size):
    # 标识符只从白名单选取；country/q 等用户值均作为参数传入。
    sort_column = {'activity': 'update_total', 'latest': 'latest_observation', 'asn': 'asn::bigint'}[sort]
    direction = {'asc': 'ASC', 'desc': 'DESC'}[order]
    where = ["x.scope='asn'", 'f.window_start >= %s', 'f.window_end <= %s',
             'f.window_start < f.window_end']
    values = [start, end]
    if country:
        where.append('x.country = %s'); values.append(country)
    if query:
        where.append('x.subject LIKE %s'); values.append(query + '%')
    with conn.cursor() as cur:
        cur.execute(f"""WITH grouped AS MATERIALIZED (
            SELECT x.subject AS asn,
                   array_remove(array_agg(DISTINCT NULLIF(x.country, '') ORDER BY NULLIF(x.country, '')), NULL) AS countries,
                   COUNT(*) AS sample_count, MAX(f.window_end) AS latest_observation,
                   CASE WHEN COUNT(x.announ_num)=COUNT(*) THEN SUM(x.announ_num) END AS announce,
                   CASE WHEN COUNT(x.withdraw_num)=COUNT(*) THEN SUM(x.withdraw_num) END AS withdraw,
                   CASE WHEN COUNT(x.announ_num)=COUNT(*) AND COUNT(x.withdraw_num)=COUNT(*)
                        THEN SUM(x.announ_num)+SUM(x.withdraw_num) END AS update_total
            FROM result_delivery.features x JOIN result_delivery.files f USING (ordinal)
            WHERE {' AND '.join(where)} GROUP BY x.subject
        ), selected AS (
            SELECT * FROM grouped ORDER BY {sort_column} {direction} NULLS LAST, asn::bigint ASC
            LIMIT %s OFFSET %s
        ) SELECT (SELECT COUNT(*) FROM grouped),
                 COALESCE((SELECT jsonb_agg(to_jsonb(selected)) FROM selected), '[]'::jsonb)""",
                    (*values, page_size, (page - 1) * page_size))
        return cur.fetchone()


def get_asn_candidates(params, conn=conn_11):
    allowed = {'start_time', 'end_time', 'country', 'q', 'sort', 'order', 'page', 'page_size', 'version'}
    try:
        if set(params) - allowed:
            raise ValueError('存在不支持的参数')
        start, end = _time(params.get('start_time')), _time(params.get('end_time'))
        if not start < end <= start + timedelta(days=45):
            raise ValueError('AS 候选窗口必须大于零且最多 45 天')
        profile = delivery_read.PROFILE
        if not _time(profile['window_start']) <= start < end <= _time(profile['window_end_exclusive']):
            raise ValueError('查询超出项目数据窗口')
        country = params.get('country', '').strip()
        if len(country) > 80 or any(ord(char) < 32 for char in country) or country == 'collect':
            raise ValueError('国家名称无效')
        query = params.get('q', '').strip().upper()
        if query.startswith('AS'):
            query = query[2:]
        if query and not re.fullmatch(r'[0-9]{1,10}', query):
            raise ValueError('搜索只接受数字或 AS 加数字的 ASN 前缀')
        sort, order = params.get('sort', 'activity'), params.get('order', 'desc')
        if sort not in {'activity', 'latest', 'asn'} or order not in {'asc', 'desc'}:
            raise ValueError('排序参数无效')
        page, size = int(params.get('page', '1')), int(params.get('page_size', '20'))
        if not 1 <= page <= 1000000 or not 1 <= size <= 50:
            raise ValueError('页码须为正整数，每页 1 至 50 条')
        version = params.get('version')
        if version is not None and not version.strip():
            raise ValueError('版本不能为空')
    except (ValueError, TypeError) as error:
        return {'status': False, 'msg': str(error)}, 400
    if os.environ.get('DOMEYE_RESULT_DELIVERY') != 'true':
        return {'status': False, 'msg': 'AS 候选缺少已配置的完成文件结果源'}, 503
    try:
        meta = getattr(g, 'result_delivery', None) if has_request_context() else None
        if meta is None:
            meta = delivery_read.status(conn)
        if meta['state'] != 'available':
            raise InputError('完成文件结果源不可用')
        if version is not None and version != meta['version']:
            return {'status': False, 'msg': '交付版本已变化，请按同一版本重新查询'}, 409
        if country and country not in delivery_read.available_countries(conn):
            return {'status': False, 'msg': '国家名称不在当前结果源中，请使用首页地区目录'}, 400
        coverage = delivery_read._covered_intervals(meta, start, end)
        total, rows = read_candidates(conn, start, end, country, query, sort, order, page, size) if coverage else (0, [])
        items = []
        for row in rows:
            asn = row['asn']
            if not re.fullmatch(r'[0-9]{1,10}', asn) or not 0 < int(asn) <= 4294967295:
                raise ValueError('AS 编号无效')
            for key in ('sample_count', 'announce', 'withdraw', 'update_total'):
                value = row[key]
                if value is not None and (type(value) is not int or value < 0):
                    raise ValueError('AS 候选含无效数值')
            at = _time(row['latest_observation'])
            if not start < at <= end or row['sample_count'] == 0:
                raise ValueError('AS 候选样本范围无效')
            countries = row['countries']
            info = get_asn_identity(asn)
            items.append({**row, 'latest_observation': at.isoformat(),
                          'country': countries[0] if len(countries) == 1 else None,
                          'as_name': info.get('as_name') or None,
                          'org_name': info.get('org_name_cn') or info.get('org_name') or None,
                          'withdraw_rate': round(row['withdraw'] / row['update_total'] * 100, 2) if row['update_total'] else None,
                          'anomaly_count': None})
        return {
            'state': 'available' if coverage else 'window_not_observed',
            'query': {'start': start.isoformat(), 'end_exclusive': end.isoformat(), 'timezone': profile['timezone'],
                      'window_boundary': '[start,end)', 'country': country, 'q': query,
                      'sort': sort, 'order': order, 'page': page, 'page_size': size},
            'metadata': {'version': meta['version'], 'collector_id': meta['binding']['collector'],
                         'scope_kind': 'delivered_asn_feature_samples',
                         'country_basis': 'result_delivery.features.country',
                         'coverage': {'state': 'complete' if coverage == [(start, end)] else 'partial' if coverage else 'none',
                                      'intervals': [{'start': a.isoformat(), 'end_exclusive': b.isoformat()} for a,b in coverage]},
                         'limitations': ['候选仅含查询内完整已交付文件的 AS Feature 样本，不代表全国或全网 AS。',
                                         '宣告和撤回仅汇总这些文件样本；缺失指标为 null，文件覆盖不证明采集完整。',
                                         '筛选国家采用 Feature 样本中的参考归属；异常数未在此接口统计。',
                                         REFERENCE_NOTE]},
            'total': total, 'page_count': math.ceil(total / size), 'items': items,
        }
    except (psycopg2.Error, InputError, ValueError, TypeError, KeyError, AttributeError):
        return {'status': False, 'msg': 'AS 候选或覆盖读取失败，不能解释为无数据或零'}, 503
