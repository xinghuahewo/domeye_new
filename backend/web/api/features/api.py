from flask import request
from flask_restful import Resource
from services.asn_service import get_asn_recent_events, get_asn_workbench
from services.country_service import get_country_series, get_country_workbench
from services.series_statistics import summarize_series

from services.features_service import (
    get_as_feature_series,
    get_as_outage_feature,
    get_country_feature_series,
    get_prefix_outage_feature,
    get_top_feature_data,
)


def _event_window_options():
    """显式校验扩展模式，避免拼写错误被静默当成普通查询。"""
    values = request.args.getlist('event_window')
    references = request.args.getlist('event_reference')
    if len(values) > 1 or len(references) > 1:
        return None, ({'status': False, 'msg': '事件窗口参数不能重复'}, 400)
    value = values[0] if values else 'false'
    if value not in ('true', 'false'):
        return None, ({'status': False, 'msg': 'event_window 必须为 true 或 false'}, 400)
    enabled = value == 'true'
    reference = references[0].strip() if references else ''
    if reference and not enabled:
        return None, ({'status': False, 'msg': 'event_reference 只能用于事件窗口查询'}, 400)
    return {'event_window': enabled, 'event_reference': reference}, None


class TopFeatureResource(Resource):
    """
    获取置顶/关键目标的时序特征图表数据
    Endpoint: /api/v1/features/top
    """

    def get(self):
        return get_top_feature_data(
            start_time=request.args.get('start_time'),
            end_time=request.args.get('end_time'),
            target=request.args.get('target'),
        )

class CountryFeatureListResource(Resource):
    """
    获取国家时序特征列表
    Endpoint: /api/v1/features/countries
    """

    def get(self):
        return get_country_feature_series(
            start_time=request.args.get('start_time'),
            end_time=request.args.get('end_time'),
            country=request.args.get('country', ''),
            page_num=request.args.get('page_num'),
            page_size=request.args.get('page_size', 5),
        )


class CountryWorkbenchResource(Resource):
    """获取国家级报文、资源与异常聚合。"""

    def get(self):
        return get_country_workbench(
            start_time=request.args.get('start_time'),
            end_time=request.args.get('end_time'),
            country=request.args.get('country', ''),
            limit=request.args.get('limit'),
        )


class ASFeatureListResource(Resource):
    """
    获取AS时序特征列表
    Endpoint: /api/v1/features/ases
    """

    def get(self):
        return get_as_feature_series(
            start_time=request.args.get('start_time'),
            end_time=request.args.get('end_time'),
            asn=request.args.get('asn', ''),
            country=request.args.get('country', ''),
            page_num=request.args.get('page_num'),
            page_size=request.args.get('page_size', 5),
        )


class ASWorkbenchResource(Resource):
    """获取优先监测 ASN 的报文、资源、静态信息与异常聚合。"""

    def get(self):
        options, error = _event_window_options()
        if error:
            return error
        return get_asn_workbench(
            start_time=request.args.get('start_time'),
            end_time=request.args.get('end_time'),
            asn=request.args.get('asn', ''),
            limit=request.args.get('limit'),
            **options,
        )


class ASRecentEventsResource(Resource):
    """获取数字边界精确匹配的 ASN 最近事件。"""

    def get(self):
        options, error = _event_window_options()
        if error:
            return error
        return get_asn_recent_events(
            start_time=request.args.get('start_time'),
            end_time=request.args.get('end_time'),
            asn=request.args.get('asn', ''),
            page_size=request.args.get('page_size'),
            **options,
        )


class _FeatureSeriesResource(Resource):
    kind = 'as'
    selector = None

    def get(self):
        allowed = {'start_time', 'end_time', 'version', 'summary_seconds'} | ({self.selector} if self.selector else set())
        if set(request.args) - allowed or any(len(request.args.getlist(key)) != 1 for key in request.args):
            return {'status': False, 'msg': '时序参数重复或不受支持'}, 400
        selected = request.args.get(self.selector, '').strip() if self.selector else None
        if self.selector and not selected:
            return {'status': False, 'msg': f'必须指定 {self.selector}'}, 400
        if self.selector == 'asn':
            selected = selected.removeprefix('AS').removeprefix('as')
            if not selected.isascii() or not selected.isdigit() or not 0 < int(selected) <= 4294967295:
                return {'status': False, 'msg': 'ASN 必须是有效的 AS 编号'}, 400
            selected = str(int(selected))
        version = request.args.get('version')
        if version is not None and not version.strip():
            return {'status': False, 'msg': '版本不能为空'}, 400
        summary = request.args.get('summary_seconds')
        if summary is not None and (not summary.isascii() or not summary.isdigit() or not 60 <= int(summary) <= 86400):
            return {'status': False, 'msg': 'summary_seconds 必须为 60 至 86400 的整数秒'}, 400
        options = {
            'country': selected if self.selector == 'country' else None,
            'start_time': request.args.get('start_time'),
            'end_time': request.args.get('end_time'), 'version': version,
        }
        if self.kind == 'country_features':
            result = get_country_series(**options)
        elif self.kind == 'as':
            result = get_as_outage_feature(**options)
        else:
            result = get_prefix_outage_feature(**options, asn=selected if self.selector == 'asn' else None)
        if summary is not None and isinstance(result, dict) and 'data' in result:
            result = {**result, 'summary': summarize_series(result, int(summary))}
        return result


class CountryASOutageFeatureResource(_FeatureSeriesResource):
    selector = 'country'


class CountryPrefixOutageFeatureResource(_FeatureSeriesResource):
    kind = 'prefix'
    selector = 'country'


class ASPrefixOutageFeatureResource(_FeatureSeriesResource):
    kind = 'prefix'
    selector = 'asn'


class GlobalASOutageFeatureResource(_FeatureSeriesResource):
    pass


class GlobalPrefixOutageFeatureResource(_FeatureSeriesResource):
    kind = 'prefix'


class CountryFeatureSeriesResource(_FeatureSeriesResource):
    kind = 'country_features'
    selector = 'country'
