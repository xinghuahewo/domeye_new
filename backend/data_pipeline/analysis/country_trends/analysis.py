"""精确方向画像；旧阈值及优先级保留，人口和舍入规则显式换版。"""
from fractions import Fraction as F
from data_pipeline.analysis.country_trends.contract import row


def analyze(values, denominator):
    """仅接收完整、至少两点的固定可见方向量；结果均为精确数。"""
    if len(values) < 2 or denominator is None or denominator <= 0 or any(v is None for v in values):
        return None
    if any(v < 0 or v > denominator for v in values):
        raise ValueError('trend_direction_outside_denominator')
    normalized = [F(v, denominator) for v in values]
    deltas = [b-a for a, b in zip(values, values[1:])]
    pp = [(b-a)*100 for a, b in zip(normalized, normalized[1:])]
    directional = [d for d in pp if abs(d) >= 2]
    signs = [1 if d > 0 else -1 for d in directional]
    alternations = sum(a != b for a, b in zip(signs, signs[1:]))
    alternation_ratio = F(alternations, len(signs)-1) if len(signs) > 1 else F(0)
    abrupt = [i for i, d in enumerate(pp, 1) if d <= -8]
    minimum = min(range(len(values)), key=lambda i: (values[i], i))
    total = sum(map(abs, pp), F(0))
    span = (max(normalized)-min(normalized))*100
    small = denominator < 10
    if small:
        pattern = 'small_denominator'
    elif span <= 2 and total <= 10:
        pattern = 'plateau'
    elif len(directional) >= 8 and alternation_ratio >= F(4, 5) and total >= 50:
        pattern = 'oscillation'
    elif len(abrupt) >= 2:
        pattern = 'multi_wave'
    elif len(abrupt) == 1 and not any(d >= 2 for d in pp[:minimum]) and minimum < len(values)-1 and values[-1] > values[minimum]:
        pattern = ('single_wave_partial_rebound' if values[-1] < values[0] else
                   'single_wave_return_to_window_start' if values[-1] == values[0] else
                   'single_wave_above_window_start')
    else:
        pattern = 'mixed' if abrupt else 'unmatched'
    atoms = []
    for i, value in enumerate(values):
        d = deltas[i-1] if i else None
        dp = pp[i-1] if i else None
        state = 'stable'
        if i:
            if small:
                state = 'decline' if d < 0 else 'rise' if d > 0 else 'stable'
            elif dp <= -8: state = 'abrupt_drop'
            elif dp >= 8: state = 'abrupt_rise'
            elif dp <= -2: state = 'decline'
            elif dp >= 2: state = 'rise'
        if not small and state == 'stable' and normalized[i] <= F(4, 5):
            state = 'low_plateau'
        tags = ('high_volatility',) if pattern == 'oscillation' and state in ('rise', 'decline', 'abrupt_rise', 'abrupt_drop') else ()
        atoms.append((state, tags, d, dp))
    phases = []
    start = 0
    for i in range(1, len(atoms)+1):
        if i < len(atoms) and atoms[i][:2] == atoms[start][:2]:
            continue
        phases.append((start, i-1, *atoms[start][:2]))
        start = i
    start, end, low = values[0], values[-1], values[minimum]
    loss = start-low
    facts = (
        ('start_to_extreme_change', low-start, 'extreme-start', (low, start), 'endpoint_direction_count'),
        ('loss_magnitude', loss, 'start-extreme', (start, low), 'endpoint_direction_count'),
        ('extreme_to_end_rebound', end-low, 'end-extreme', (end, low), 'endpoint_direction_count'),
        ('end_residual_from_start', start-end, 'start-end', (start, end), 'endpoint_direction_count'),
        ('window_rebound_ratio', F(end-low, loss) if loss > 0 else None, '(end-extreme)/(start-extreme)', (end, low, start), 'ratio'),
        ('fixed_cohort_visibility_gap_integral', sum(max(denominator-v, 0) for v in values), 'sum(max(D-x,0))', (denominator,), 'endpoint_direction_sample'),
        ('window_start_visibility_gap_integral', sum(max(start-v, 0) for v in values), 'sum(max(start-x,0))', (start,), 'endpoint_direction_sample'),
    )
    return dict(pattern=pattern, minimum=minimum, atoms=atoms, phases=phases, facts=facts,
                features=(span, total, len(directional), alternations, alternation_ratio, tuple(abrupt)),
                thresholds=tuple((t, tuple(i for i, v in enumerate(normalized) if v < t)) for t in (F(95, 100), F(9, 10), F(4, 5))))


def profile_rows(event, name, samples, values, unit, denominator=None):
    known = [(i, v) for i, v in enumerate(values) if v is not None]
    complete = bool(samples) and len(known) == len(samples)
    yield row('profile', event, (name,), state='complete' if complete else 'no_samples' if not samples else 'unavailable' if not known else 'degraded', unit=unit, denominator=denominator, sample_count=len(samples))
    if not known:
        return
    peak = max(v for _, v in known)
    peak_i = next(i for i, v in known if v == peak)
    yield row('peak', event, (name,), known_peak=peak, exact_peak=peak if complete else None, first_us=samples[peak_i], occurrence_count=sum(v == peak for _, v in known), known_slots=len(known), unknown_slots=len(values)-len(known), unit=unit)
    if not complete:
        return
    low_i = min(range(len(values)), key=lambda i: (values[i], i))
    for kind, i in (('start', 0), ('end', len(values)-1), ('minimum', low_i)):
        yield row('point', event, (name, kind), refs=(f'metric:{name}:{samples[i]}',), sample_us=samples[i], value=values[i], unit=unit)
    if len(values) < 2:
        return
    deltas = [b-a for a, b in zip(values, values[1:])]
    for kind, i in (('minimum_adjacent_change', min(range(len(deltas)), key=lambda i: (deltas[i], i))), ('maximum_adjacent_change', max(range(len(deltas)), key=lambda i: (deltas[i], -i)))):
        yield row('point', event, (name, kind), refs=(f'metric:{name}:{samples[i]}', f'metric:{name}:{samples[i+1]}'), sample_us=samples[i+1], value=deltas[i], unit=unit)
    if name != 'visible_direction_count':
        return
    result = analyze(values, denominator)
    if result is None:
        yield row('analysis', event, (name,), state='unavailable', reason='denominator_unavailable')
        return
    yield row('analysis', event, (name,), state='complete', pattern=result['pattern'], features=result['features'])
    for i, (state, tags, delta, dp) in enumerate(result['atoms']):
        yield row('atomic', event, (name, i), sample_us=samples[i], state=state, tags=tags, delta=delta, delta_pp=dp)
    for i, (start, end, state, tags) in enumerate(result['phases']):
        yield row('phase', event, (name, i), start_us=samples[start], end_us=samples[end], state=state, tags=tags, start_value=values[start], end_value=values[end], unit=unit)
    for metric, value, formula, operands, fact_unit in result['facts']:
        yield row('fact', event, (name, metric), refs=(f'profile:{name}',), value=value, rounded_value=round(value,6) if value is not None else None, rounding='half_even_6_decimal_exact', formula=formula, operands=operands, unit=fact_unit)
    for threshold, indices in result['thresholds']:
        yield row('threshold', event, (name, threshold), count=len(indices), continuous_duration_claimed=False)
        for i in indices:
            yield row('threshold_sample', event, (name, threshold, i), sample_us=samples[i])
