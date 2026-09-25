"""从已读取的时序生成带真实时点的统计；不访问数据库或推定恢复状态。"""
from datetime import datetime, timedelta


def _intervals(values):
    """合并覆盖段，并另行保留重叠事实，活动不能重复求和。"""
    merged, overlaps = [], False
    for left, right in sorted(values):
        if merged and left <= merged[-1][1]:
            overlaps |= left < merged[-1][1]
            merged[-1] = (merged[-1][0], max(right, merged[-1][1]))
        else:
            merged.append((left, right))
    return merged, overlaps


def _encode(intervals):
    return [{'start': left.isoformat(), 'end_exclusive': right.isoformat()} for left, right in intervals]


def _point_statistics(rows, metric, unit, time_field):
    points = [{'value': row[metric], 'at': row[time_field]} for row in rows]
    known = [point for point in points if point['value'] is not None]
    first, last = (points[0], points[-1]) if points else (None, None)
    change = None
    if (len(points) > 1 and first['value'] is not None and last['value'] is not None
            and datetime.fromisoformat(first['at']) < datetime.fromisoformat(last['at'])):
        delta = last['value'] - first['value']
        change = {'from': first, 'to': last, 'delta': delta,
                  'percent_of_first': 100 * delta / first['value'] if first['value'] else None}
    return {'kind': 'point', 'unit': unit, 'sample_count': len(points), 'known_sample_count': len(known),
            'first': first, 'last': last,
            'known_sample_minimum': min(known, key=lambda p: p['value']) if known else None,
            'known_sample_maximum': max(known, key=lambda p: p['value']) if known else None,
            'known_sample_mean': sum(p['value'] for p in known) / len(known) if known else None,
            'first_to_last': change}


def _window_statistics(rows, metric, unit, start, end, covered):
    windows, overlap = _intervals([(datetime.fromisoformat(row['start']),
                                   datetime.fromisoformat(row['end_exclusive'])) for row in rows])
    values = [row[metric] for row in rows if row[metric] is not None]
    observed = sum(values) if values and not overlap else None
    complete = covered and windows == [(start, end)] and len(values) == len(rows) and not overlap
    return {'kind': 'window_count', 'unit': unit, 'sample_count': len(rows), 'known_sample_count': len(values),
            'source_intervals': _encode(windows), 'overlapping_windows': overlap,
            'known_window_sum': observed, 'total': observed if complete else None}


def summarize_series(payload, seconds):
    """按实际测量时间分桶；活动仅纳入完整区间，资源纳入桶末状态点。"""
    query, meta = payload['query'], payload['metadata']
    start, end = datetime.fromisoformat(query['start']), datetime.fromisoformat(query['end_exclusive'])
    outage = meta['interpretation_version'] == 'outage-series/v2'
    metrics = {'outage_count': meta['unit']} if outage else meta['units']
    coverage = [(datetime.fromisoformat(part['start']), datetime.fromisoformat(part['end_exclusive']))
                for part in meta['coverage']['intervals']]
    point_field = 'time_slot' if outage else 'at'
    point_rows = payload['data'] if outage else payload['data']['resources']
    points = sorted([(datetime.fromisoformat(row[point_field]), row) for row in point_rows], key=lambda p: p[0])
    windows = [] if outage else [(datetime.fromisoformat(row['start']),
                                  datetime.fromisoformat(row['end_exclusive']), row)
                                 for row in payload['data']['activity']]
    buckets, left = [], start
    while left < end:
        right = min(left + timedelta(seconds=seconds), end)
        parts, _ = _intervals([(max(left, a), min(right, b)) for a, b in coverage if a < right and b > left])
        covered = parts == [(left, right)]
        samples = [row for at, row in points if (left <= at < right if outage else left < at <= right)]
        activity = [row for a, b, row in windows if left <= a < b <= right]
        stats = {}
        for metric, unit in metrics.items():
            if not outage and metric in ('announce', 'withdraw'):
                stats[metric] = _window_statistics(activity, metric, unit, left, right, covered)
            else:
                stats[metric] = _point_statistics(samples, metric, unit, point_field)
        buckets.append({'start': left.isoformat(), 'end_exclusive': right.isoformat(),
                        'coverage': {'state': 'complete' if covered else 'partial' if parts else 'none',
                                     'intervals': _encode(parts)}, 'metrics': stats})
        left = right
    return {'schema_version': 'series-statistics/v2', 'interval_seconds': seconds,
            'extrema_ties': 'earliest_sample', 'buckets': buckets}
