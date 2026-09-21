"""按原目标核资格后复用精确科学核；不读取数据库或签发 Admission。"""
from dataclasses import fields, replace

from data_pipeline.analysis.country_events.snapshot_schema import encode

from data_pipeline.analysis.country_trends.analysis import profile_rows
from data_pipeline.analysis.country_trends.contract import row
from data_pipeline.analysis.country_trends.qualified_models import DIMENSIONS, Selection, Target, numeric


class Qualifications:
    """有限资格连接：原行字段决定目标，引用的资格必须逐字段相符。

    records 是原 Country 公共行的无损字段映射；完整原行仍由输入层保存。
    此处只消费映射，不导入未冻结的 Country M3 候选或模拟正式准入。
    """
    def __init__(self, records):
        self.records = {}
        for source in records:
            record = dict(source)
            key = record['qualification_id']
            if not key or key in self.records or record['dimension'] not in DIMENSIONS:
                raise ValueError('trend_m3_duplicate_or_invalid_qualification')
            if record['coverage'] not in ('complete', 'partial', 'unknown', 'not_applicable'):
                raise ValueError('trend_m3_qualification_coverage')
            self.records[key] = record

    def check(self, target, references, dimensions):
        if type(target) is not Target or not dimensions or any(d not in DIMENSIONS for d in dimensions):
            raise ValueError('trend_m3_selection_scope')
        if len(set(references)) != len(references):
            raise ValueError('trend_m3_duplicate_qualification_reference')
        selected = []
        for key in references:
            if key not in self.records:
                raise ValueError('trend_m3_dangling_qualification')
            q = self.records[key]
            if any(encode(q[f.name]) != encode(getattr(target, f.name)) for f in fields(target)):
                raise ValueError('trend_m3_qualification_target_mismatch')
            selected.append(q)
        reasons = []
        # 原前史可保留在Target/原行索引；资格相等不代表适用于结果D。
        # 无sample的事件/窗口汇总不套用原C2样本左极限规则。
        if target.sample_us is not None and not target.window_us[0] < target.sample_us <= target.window_us[1]:
            reasons.append('sample_outside_result_window')
        for dimension in dimensions:
            matching = [q for q in selected if q['dimension'] == dimension]
            if not matching:
                reasons.append('missing_qualification:' + dimension)
            elif len(matching) != 1:
                raise ValueError('trend_m3_ambiguous_qualification')
            elif matching[0]['coverage'] != 'complete':
                reasons.append(dimension + ':' + matching[0]['coverage'])
        return tuple(reasons)

    def select(self, target, raw, qualified, *, dimension, population_ref, unit):
        numeric(raw)
        if qualified is None:
            return Selection(target, dimension, population_ref, raw, None, None, 'unknown',
                             ('missing_qualified_value:' + dimension,), (), (), None)
        qv = qualified
        if (any(encode(qv[k]) != encode(getattr(target, k)) for k in
                ('logical_run_id', 'input_binding_id', 'raw_target_ref', 'window_us'))
                or qv['dimension'] != dimension or encode(qv['population_ref']) != encode(population_ref)
                or qv['unit'] != unit):
            raise ValueError('trend_m3_qualified_value_target_mismatch')
        refs = tuple(qv['qualification_refs'])
        reasons = self.check(target, refs, (dimension,))
        value = numeric(qv['value'])
        lower = numeric(qv['known_lower_bound'])
        denominator = numeric(qv['denominator'])
        if (type(qv['lower_bound_proven']) is not bool
                or qv['lower_bound_proven'] != (lower is not None)
                or value is not None and (value != raw or lower is not None and lower > value)
                or qv['coverage'] not in ('complete', 'partial', 'unknown', 'not_applicable')
                or qv['coverage'] != 'complete' and value is not None
                or not qv['raw_basis_refs'] or not qv['qualified_value_id']):
            raise ValueError('trend_m3_qualified_value_inconsistent')
        if qv['coverage'] == 'complete' and reasons:
            raise ValueError('trend_m3_complete_without_qualification')
        if qv['coverage'] != 'complete':
            reasons += ('qualified_value:' + qv['coverage'],)
        if value is None:
            reasons += ('exact_value_unknown',)
        return Selection(target, dimension, population_ref, raw, value if not reasons else None, lower,
                         'unknown' if reasons else 'observed_zero' if value == 0 else 'qualified',
                         reasons, refs, tuple(qv['raw_basis_refs']), qv['qualified_value_id'], denominator)


def series_rows(event, name, samples, points, *, unit, denominator, peak=None):
    """只把已选主值送入原算法；下界永不充当精确点或分母。

    peak 为独立原窗口峰值选择。即使所有采样点可用，也不自动获得
    完整窗口峰值或 Session 连续性。原科学字段保留并追加适用性。
    """
    if (len(samples) != len(points) or tuple(sorted(set(samples))) != tuple(samples)
            or any(type(t) is not int for t in samples)
            or any(type(p) is not Selection for p in points)
            or type(denominator) is not Selection):
        raise ValueError('trend_m3_series_shape')
    values = [p.value for p in points]
    population = denominator.target
    if (denominator.dimension != 'fixed_denominator'
            or (population.incident_id, population.revision) != event
            or any((p.target.incident_id, p.target.revision) != event
                   or p.target.sample_us != t or p.target.metric != name
                   or p.population_ref != population.raw_target_ref
                   or any(getattr(p.target, k) != getattr(population, k) for k in
                          ('logical_run_id', 'input_binding_id', 'cohort_id', 'window_us'))
                   for t, p in zip(samples, points))):
        raise ValueError('trend_m3_series_target_mismatch')
    if peak is not None and (peak.dimension != 'window_peak'
            or peak.population_ref != population.raw_target_ref
            or (peak.target.incident_id, peak.target.revision) != event
            or peak.target.metric != name
            or any(getattr(peak.target, k) != getattr(population, k) for k in
                   ('logical_run_id', 'input_binding_id', 'cohort_id', 'window_us'))):
        raise ValueError('trend_m3_peak_target_mismatch')
    d = denominator.value
    if d is not None and type(d) is not int:
        raise ValueError('trend_m3_fixed_denominator_integer')
    if name == 'visible_direction_count' and any(p.denominator != d for p in points):
        # 同一原固定人口的合格分母必须在每个样本一致，不能仅沿用首槽。
        d = None
    refs = tuple(dict.fromkeys(r for p in (*points, denominator) for r in p.qualification_refs))
    reasons = tuple(dict.fromkeys(r for p in (*points, denominator) for r in (*p.reasons, *p.denominator_reasons)))
    if d is None and denominator.value is not None:
        reasons += ('point_denominator_unknown_or_mismatch',)
    for index, point in enumerate(points):
        yield row('qualified_metric', event, (name, samples[index]),
                  refs=point.qualification_refs, raw=point.raw, value=point.value,
                  known_lower_bound=point.known_lower_bound, state=point.state,
                  reasons=point.reasons, qualified_value_id=point.qualified_value_id,
                  raw_basis_refs=point.raw_basis_refs, unit=unit)
    for scientific in profile_rows(event, name, samples, values, unit, d):
        fields_out = dict(scientific.values)
        local_refs = refs
        local_reasons = reasons
        if scientific.kind == 'peak':
            exact = peak.value if peak is not None else None
            # 原窗口峰值与采样子集峰值各自保留，不把已知子集冒充完整窗口。
            fields_out['exact_peak'] = exact
            if peak is not None:
                local_refs += peak.qualification_refs
                local_reasons += peak.reasons
            else:
                local_reasons += ('missing_window_peak_qualification',)
        fields_out.update(qualification_refs=tuple(dict.fromkeys(local_refs)),
                          qualification_reasons=tuple(dict.fromkeys(local_reasons)),
                          sample_basis='qualified_samples', continuous_duration_claimed=False)
        yield replace(scientific, values=tuple(fields_out.items()))
    if any(v is None for v in values):
        yield row('unknown', event, (name, 'sample_analysis'), code='incomplete_qualified_samples',
                  reasons=reasons, qualification_refs=refs)
    if name == 'visible_direction_count' and (d is None or d == 0):
        yield row('unknown', event, (name, 'ratio'), code='denominator_unknown' if d is None else 'zero_denominator',
                  reasons=reasons, qualification_refs=refs)


def event_series(sources, *, limits):
    """原计算网格完整保留；仅结果 D 内合格样本参与本次趋势。

    原起止保留在 EventStatus 中，本函数不从最后样本构造事件结束。
    ASN/双族/路径及可选上下文由同版本的事件编译入口继续处理。
    """
    from data_pipeline.analysis.country_events.compute import MetricPoint, Completion
    from data_pipeline.analysis.country_events.event_aggregation import METRICS
    from data_pipeline.analysis.country_trends.contract import TRACKS
    completions = [v for v in sources.originals.values() if type(v) is Completion]
    if len(completions) != 1:
        raise ValueError('trend_m3_original_completion_count')
    completion = completions[0]
    metrics = {}
    for key, original in sources.originals.items():
        if type(original) is MetricPoint:
            natural = original.metric, original.sample_us
            if natural in metrics:
                raise ValueError('trend_m3_duplicate_metric')
            metrics[natural] = key
            yield row('metric', sources.event, natural, raw=original)
    grid = tuple(sorted(t for name, t in metrics if name == 'visible_direction_count'))
    if len(grid) > limits.max_samples:
        raise ValueError('trend_m3_sample_budget')
    if (completion.sample_count != len(grid)
            or completion.sample_window != ((grid[0], grid[-1]) if grid else None)
            or any(not completion.input_window[0] < t <= completion.input_window[1] for t in grid)):
        raise ValueError('trend_m3_original_grid')
    samples = tuple(t for t in grid if sources.window_us[0] < t <= sources.window_us[1])
    yield row('source_completion', sources.event, raw=completion)
    yield row('analysis_scope', sources.event, calculation_window_us=completion.input_window,
              calculation_samples=grid, result_window_us=sources.window_us,
              result_samples=samples, original_incident=sources.status.incident,
              earlier_history='raw_only_without_applicable_qualification')
    denominator = sources.denominator()
    for name in (*TRACKS, 'visible_direction_count'):
        if any((name, t) not in metrics for t in samples):
            raise ValueError('trend_m3_incomplete_original_metric_matrix')
        selected = tuple(sources.metric(metrics[name, t]) for t in samples)
        # 其他轨仅由其自身样本的合格原分母得到profile分母，不借事件方向D。
        if name == 'visible_direction_count':
            d = denominator
        else:
            ds = {p.denominator for p in selected}
            value = next(iter(ds)) if len(ds) == 1 and None not in ds else None
            d = replace(denominator, raw=None, value=value, denominator=value,
                state='unknown' if value is None else 'observed_zero' if value == 0 else 'qualified',
                reasons=tuple(dict.fromkeys(r for p in selected for r in p.denominator_reasons)),
                qualification_refs=tuple(dict.fromkeys(r for p in selected for r in p.qualification_refs)),
                qualified_value_id=None)
        yield from series_rows(sources.event, name, samples, selected,
                               unit=dict(METRICS)[name], denominator=d, peak=sources.peak(name))
