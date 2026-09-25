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
    """按查询起点分桶，保留原样本标签选择规则与各指标自身的时间语义。"""
    query, meta = payload['query'], payload['metadata']
    start, end = datetime.fromisoformat(query['start']), datetime.fromisoformat(query['end_exclusive'])
    outage = meta['interpretation_version'] == 'outage-series/v2'
    label = 'time_slot' if outage else 'source.label'
    metrics = {'outage_count': meta['unit']} if outage else meta['units']
    coverage = [(datetime.fromisoformat(part['start']), datetime.fromisoformat(part['end_exclusive']))
                for part in meta['coverage']['intervals']]
    def sample_time(row):
        return datetime.fromisoformat(row['time_slot'] if outage else row['source']['label'])

    rows = sorted(payload['data'], key=sample_time)
    selected = {}
    for row in rows:
        at = sample_time(row)
        if start <= at < end:
            selected.setdefault(int((at - start).total_seconds() // seconds), []).append(row)
    buckets, left, index = [], start, 0
    while left < end:
        right = min(left + timedelta(seconds=seconds), end)
        parts, _ = _intervals([(max(left, a), min(right, b)) for a, b in coverage if a < right and b > left])
        covered = parts == [(left, right)]
        samples = selected.get(index, [])
        stats = {}
        for metric, unit in metrics.items():
            if not outage and metric in ('announce', 'withdraw'):
                stats[metric] = _window_statistics([row['activity'] for row in samples],
                                                  metric, unit, left, right, covered)
            else:
                stats[metric] = _point_statistics(samples if outage else [row['resources'] for row in samples],
                                                 metric, unit, 'time_slot' if outage else 'at')
        buckets.append({'start': left.isoformat(), 'end_exclusive': right.isoformat(),
                        'coverage': {'state': 'complete' if covered else 'partial' if parts else 'none',
                                     'intervals': _encode(parts)}, 'metrics': stats})
        left, index = right, index + 1
    return {'schema_version': 'series-statistics/v1', 'interval_seconds': seconds,
            'selection_time': label, 'extrema_ties': 'earliest_sample', 'buckets': buckets}
