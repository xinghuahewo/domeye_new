"""在同次只读请求内比较两窗现有国家统计，不推定事件恢复。"""
from datetime import datetime

from services.country_service import _parse_range, get_country_series
from services.features_service import get_as_outage_feature, get_prefix_outage_feature
from services.series_statistics import summarize_series


def _window(query):
    return {key: query[key] for key in ('start', 'end_exclusive')}


def _duration(window):
    return int((datetime.fromisoformat(window['end_exclusive']) - datetime.fromisoformat(window['start'])).total_seconds())


def _comparison_metric(reference, current, definition, same_duration):
    activity = reference['kind'] == 'window_count'
    def reading(stats):
        if activity:
            return {'value': stats['total'], 'known_window_sum': stats['known_window_sum'],
                    'source_intervals': stats['source_intervals']}
        return stats['last'] or {'value': None, 'at': None}
    before, after = reading(reference), reading(current)
    reasons = []
    for side, value in [('reference', before['value']), ('current', after['value'])]:
        if value is None:
            reasons.append(side + ('_total_unknown' if activity else '_value_unknown'))
    if activity and not same_duration:
        reasons.append('window_duration_mismatch')
    delta = after['value'] - before['value'] if not reasons else None
    return {**definition, 'unit': reference['unit'], 'statistic': 'window_total' if activity else 'last_sample',
            'reference': before, 'current': after,
            'comparison': {'state': 'not_comparable' if reasons else 'comparable', 'reasons': reasons,
                           'delta': delta, 'percent_of_reference': 100 * delta / before['value']
                           if delta is not None and before['value'] else None}}


def get_country_comparison(country, start_time, end_time, reference_start_time, reference_end_time, version=None):
    """沿用三类已有读取和统计规则；两侧先校验，失败保留原错误。"""
    for start, end in [(reference_start_time, reference_end_time), (start_time, end_time)]:
        _, _, error = _parse_range(start, end)
        if error:
            return error
    windows, statistics = {}, {}
    bound_version, collector = version, None
    definitions = {}
    for side, start, end in [('reference', reference_start_time, reference_end_time), ('current', start_time, end_time)]:
        statistics[side] = {}
        readers = [('features', get_country_series, {}), ('as_outage', get_as_outage_feature, {}),
                   ('prefix_outage', get_prefix_outage_feature, {'asn': None})]
        for kind, reader, options in readers:
            payload = reader(country=country, start_time=start, end_time=end, version=bound_version, **options)
            if not isinstance(payload, dict) or 'data' not in payload:
                return payload
            meta = payload['metadata']
            if bound_version is None:
                bound_version = meta['version']
            collector = collector or meta['collector_id']
            if meta['version'] != bound_version or meta['collector_id'] != collector:
                return {'status': False, 'msg': '比较两侧的版本或观察点不一致'}, 409
            window = _window(payload['query'])
            windows[side] = {**window, 'seconds': _duration(window), 'coverage': meta['coverage']}
            summary = summarize_series(payload, _duration(window))['buckets'][0]['metrics']
            for key, stats in summary.items():
                name = key if kind == 'features' else kind
                definition = {'measurement': meta['measurement'][key], 'population': 'country_feature_records'} if kind == 'features' else {
                    'measurement': meta['metric'], 'population': meta['population']}
                if name in definitions and definitions[name] != {**definition, 'unit': stats['unit']}:
                    return {'status': False, 'msg': '比较两侧的指标定义、单位或总体不一致'}, 503
                definitions[name] = {**definition, 'unit': stats['unit']}
                statistics[side][name] = stats
    same_duration = windows['reference']['seconds'] == windows['current']['seconds']
    return {
        'query': {'country': country, 'reference': windows['reference'], 'current': windows['current'],
                  'timezone': payload['query']['timezone']},
        'metadata': {'interpretation_version': 'country-window-comparison/v1', 'version': bound_version,
                     'collector_id': collector, 'scope': 'country_statistics', 'recovery_assessment': 'not_assessed'},
        'metrics': {name: _comparison_metric(statistics['reference'][name], statistics['current'][name], definition, same_duration)
                    for name, definition in definitions.items()},
    }
