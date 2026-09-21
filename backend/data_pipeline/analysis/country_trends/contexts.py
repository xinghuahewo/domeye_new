"""有限 ASN、双族、活动和独立参照上下文，不读取服务或全局配置。"""
from collections import Counter
from fractions import Fraction as F
from ipaddress import ip_network
from data_pipeline.analysis.country_trends.contract import STATES, REFERENCE_DEFINITION, row, exact
from data_pipeline.analysis.country_trends.analysis import analyze


def asn_rows(event, samples, members, points):
    roster = {}
    for member in members:
        r = member.route
        if r.origin is not None:
            for afi in (0, r.endpoint.afi):
                roster.setdefault((r.origin, afi), set()).add(r.prefix)
    indexed = {}
    for p in points:
        key = (p.asn, p.afi, p.sample_us)
        if key in indexed:
            raise ValueError('trend_duplicate_asn')
        if p.state not in STATES or p.afi not in (0, 1, 2) or p.sample_us not in samples or any(type(v) is not int or v<0 for v in (p.normal,p.partial,p.complete,p.unknown)):
            raise ValueError('trend_invalid_asn')
        total=p.normal+p.partial+p.complete+p.unknown
        expected_state='unknown' if not total or p.unknown else 'normal' if p.normal==total else 'route_interrupted' if p.complete==total else 'affected'
        if total!=len(p.prefix_refs) or p.state!=expected_state:
            raise ValueError('trend_asn_partition')
        indexed[key] = p
        yield row('asn_point', event, key, raw=p)
    if len(indexed) != len(roster)*len(samples) or any((asn,afi) not in roster for asn,afi,t in indexed):
        yield row('asn_context', event, state='unavailable', reason='incomplete_matrix')
        return
    if not samples:
        yield row('asn_context', event, state='no_samples')
        return
    yield row('asn_context', event, state='complete', asn_count=sum(afi == 0 for _, afi in roster))
    priorities=[]
    for asn, afi in sorted(roster):
        series = [indexed[asn, afi, t] for t in samples]
        if any(set(p.prefix_refs) != roster[asn, afi] or len(p.prefix_refs) != len(roster[asn, afi]) for p in series):
            raise ValueError('trend_asn_membership_mismatch')
        states = [p.state for p in series]
        counts = Counter(states)
        runs = {}
        for state in (*STATES, 'observed_non_normal'):
            longest = current = 0
            for value in states:
                match = value in ('affected', 'route_interrupted') if state == 'observed_non_normal' else value == state
                current = current + 1 if match else 0
                longest = max(longest, current)
            runs[state] = longest
        persistence = None if 'unknown' in states else states[0] != states[-1]
        priorities.append((asn,afi,len(roster[asn,afi]),runs['observed_non_normal']))
        yield row('asn_summary', event, (asn, afi), start=states[0], end=states[-1], persistent_not_at_start=persistence, state_counts=tuple((s, counts[s]) for s in STATES), longest_runs=tuple(runs.items()), scale_prefix_count=len(roster[asn, afi]))
        for i in range(1, len(samples)):
            if states[i] != states[i-1]:
                yield row('asn_transition', event, (asn, afi, i), from_state=states[i-1], to_state=states[i], from_us=samples[i-1], to_us=samples[i], continuous_recovery_claim=False)
    for view in ('scale','persistence'):
        ordered=sorted(priorities,key=lambda x:(x[1],-x[2],x[0]) if view=='scale' else (x[1],-x[3],-x[2],x[0]))
        for rank,(asn,afi,scale,persistence) in enumerate(ordered):
            yield row('asn_priority',event,(view,rank),asn=asn,afi=afi,scale_prefix_count=scale,longest_observed_non_normal=persistence,single_impact_score=None)
    # 全样本相邻转移矩阵比旧首末/阶段抽样更完整；各族分开，含Unknown。
    for afi in (0, 1, 2):
        population = [asn for asn, family in roster if family == afi]
        for i, t in enumerate(samples):
            counts = Counter(indexed[asn, afi, t].state for asn in population)
            yield row('asn_population', event, (afi, t), counts=tuple((s, counts[s]) for s in STATES), total=len(population))
            if i:
                cells = Counter((indexed[asn, afi, samples[i-1]].state, indexed[asn, afi, t].state) for asn in population)
                for before in STATES:
                    for after in STATES:
                        yield row('asn_transition_cell', event, (afi, i, before, after), count=cells[before, after])


def family_projection(event, samples, members, prefixes, status, main, qualities):
    directions = {}
    mapping_known = True
    for member in members:
        r = member.route
        directions.setdefault(r.prefix, set()).add(r.endpoint)
        mapping_known &= r.mapping_state == 'bound'
    if status.fixed_prefix_count is not None and status.fixed_prefix_count != len(directions):
        raise ValueError('trend_cohort_prefix_count')
    denominators = {afi: sum(len(ds) for p, ds in directions.items() if ip_network(p).version == version) for afi, version in ((1, 4), (2, 6))}
    total = sum(denominators.values())
    complete_members = status.direction_count is not None and status.direction_count == total
    indexed = {}
    for p in prefixes:
        key = (p.prefix, p.sample_us)
        if key in indexed:
            raise ValueError('trend_duplicate_prefix')
        if p.prefix not in directions or p.sample_us not in samples:
            raise ValueError('trend_prefix_outside_cohort')
        indexed[key] = p
    values = {1: [], 2: []}
    for t in samples:
        point = main[t]
        good = complete_members and mapping_known and not qualities and not status.reasons and point.value is not None and point.denominator == total
        sums = {1: 0, 2: 0}
        for prefix, ds in directions.items():
            p = indexed.get((prefix, t))
            if p is None:
                good = False
                continue
            if min(p.present, p.absent, p.unknown) < 0 or (p.expected is not None and p.present+p.absent+p.unknown != p.expected):
                raise ValueError('trend_prefix_partition')
            good &= p.expected is not None and p.expected == len(ds) and p.unknown == 0
            sums[1 if ip_network(prefix).version == 4 else 2] += p.present
        if good and sum(sums.values()) != point.value:
            raise ValueError('trend_family_total_mismatch')
        for afi in (1, 2):
            values[afi].append(F(sums[afi]) if good else None)
    return denominators, values


def family_rows(event, samples, denominators, values):
    for afi in (1, 2):
        for i, t in enumerate(samples):
            yield row('family_point', event, (afi, t), value=values[afi][i], denominator=denominators[afi], unit='endpoint_direction_count')
    results = {afi: analyze(values[afi], denominators[afi]) for afi in (1, 2)}
    if any(r is None for r in results.values()):
        yield row('family_context', event, state='unavailable', reason='family_population_or_quality')
        return
    divergence = [(F(values[1][i], denominators[1])-F(values[2][i], denominators[2]))*100 for i in range(len(samples))]
    best = max(range(len(samples)), key=lambda i: (abs(divergence[i]), -i))
    delta = results[2]['minimum']-results[1]['minimum']
    for i, t in enumerate(samples):
        yield row('family_divergence', event, (t,), ipv4_minus_ipv6_pp=divergence[i])
    ratio = F(max(denominators.values()), min(denominators.values()))
    yield row('family_context', event, state='complete', maximum_divergence=divergence[best], maximum_at=samples[best], extreme_delta_slots=delta, relation='same_slot' if delta == 0 else 'adjacent_slot' if abs(delta) == 1 else 'lagged_slot', denominator_ratio=ratio, asymmetry=ratio >= 10)


def activity_rows(event, samples, main_values, denominator, windows):
    seen = set()
    aligned = {}
    analysis = analyze(main_values, denominator)
    for w in windows:
        if w.mode not in ('ordinary', 'ir') or w.metric not in ('announ_num', 'withdraw_num') or w.population != {'announ_num':'accepted_announce_elements', 'withdraw_num':'accepted_withdraw_elements'}[w.metric]:
            raise ValueError('trend_activity_population')
        if type(w.start_us) is not int or type(w.end_us) is not int or w.start_us >= w.end_us or not w.source_ref or w.state not in ('complete', 'unknown'):
            raise ValueError('trend_activity_window')
        if type(w.source_rank) is not int or w.source_rank<0 or not w.source_id or w.window_role not in ('initial','warmup','comparison','result'):
            raise ValueError('trend_activity_source_role')
        exact(w.value)
        if w.value is not None and w.value < 0:
            raise ValueError('trend_activity_value')
        key = (w.mode, w.metric, w.start_us, w.end_us)
        if key in seen:
            raise ValueError('trend_duplicate_activity_window')
        seen.add(key)
        matches = [i for i in range(1, len(samples)) if (w.start_us, w.end_us) == (samples[i-1], samples[i])]
        i = matches[0] if matches else None
        good = i is not None and w.window_role == 'result' and w.state == 'complete' and w.value is not None and main_values[i] is not None and main_values[i-1] is not None
        yield row('activity_window', event, key, refs=(w.source_ref,), raw=w, state='aligned' if good else 'unavailable', anchor_us=samples[i] if good else None, reason=None if good else 'interval_or_quality_mismatch')
        if good:
            aligned.setdefault((w.mode, w.metric), []).append((i, w.value, key, w.source_ref))
    if not windows:
        yield row('activity_context', event, state='not_provided')
    if analysis:
        deltas = [b-a for a, b in zip(main_values, main_values[1:])]
        points = (('minimum', analysis['minimum']), ('minimum_adjacent_change', min(range(len(deltas)), key=lambda i:(deltas[i],i))+1), ('maximum_adjacent_change', max(range(len(deltas)), key=lambda i:(deltas[i],-i))+1))
        for key, candidates in sorted(aligned.items()):
            index, value, window_key, source_ref = max(candidates, key=lambda x:(x[1],-x[0]))
            for kind, point in points:
                offset = index-point
                yield row('activity_relation', event, (*key, kind), refs=(source_ref,), window_key=window_key, state_point_key=('visible_direction_count',kind), peak_interval_end=samples[index], peak_value=value, offset_slots=offset, relation='same_slot' if offset == 0 else 'adjacent_slot' if abs(offset) == 1 else 'lagged_slot', causal_claim=False)


def reference_rows(event, samples, main_values, denominator, reference, event_cohort_id):
    if reference is None:
        yield row('reference_context', event, state='not_provided')
        return
    if not reference.binding or reference.definition != 'fixed_endpoint_direction/v1':
        raise ValueError('trend_reference_binding_or_definition')
    codes = [p.country for p in reference.projections]
    if len(set(codes)) != len(codes):
        raise ValueError('trend_reference_duplicate_country')
    target = next((p for p in reference.projections if p.country == reference.target), None)
    if target is None or target.population != 'fixed_endpoint_direction' or target.samples != samples or target.values != tuple(main_values) or target.denominator != denominator:
        raise ValueError('trend_reference_target_mismatch')
    if target.cohort_id and target.cohort_id != event_cohort_id:
        raise ValueError('trend_reference_target_cohort')
    if target.definition_binding and target.definition_binding != REFERENCE_DEFINITION:
        raise ValueError('trend_reference_target_definition')
    comparable = []
    for p in reference.projections:
        reason = None
        if p.country == '__UNKNOWN__': reason = 'unknown_bucket'
        elif len(p.country) != 2 or not p.country.isascii() or not p.country.isalpha() or not p.country.isupper(): reason = 'invalid_country'
        elif p.population != 'fixed_endpoint_direction': reason = 'population_mismatch'
        elif not isinstance(p.cohort_id,str) or not p.cohort_id.strip(): reason = 'cohort_unavailable'
        elif not p.definition_binding: reason = 'definition_unavailable'
        elif p.definition_binding != REFERENCE_DEFINITION: reason = 'definition_mismatch'
        elif type(p.denominator) is not int or p.denominator <= 0: reason = 'invalid_denominator'
        elif p.denominator < 10: reason = 'small_denominator'
        elif p.samples != samples or len(p.values) != len(samples): reason = 'grid_mismatch'
        elif not p.source_ref: reason = 'source_unavailable'
        elif p.quality != 'complete' or any(v is None for v in p.values): reason = 'quality_incomplete'
        if reason:
            yield row('reference_exclusion', event, (p.country,), reason=reason)
            continue
        for value in p.values:
            exact(value)
        if any(v < 0 or v > p.denominator for v in p.values):
            raise ValueError('trend_reference_value_range')
        if len(samples) < 2:
            yield row('reference_exclusion', event, (p.country,), reason='insufficient_samples')
            continue
        ratios = [F(v,p.denominator) for v in p.values]
        deltas = [(b-a)*100 for a,b in zip(ratios,ratios[1:])]
        shape = []
        for delta in deltas:
            direction = 'DEGRADING' if delta <= -2 else 'RECOVERING' if delta >= 2 else 'STABLE'
            if not shape or shape[-1] != direction: shape.append(direction)
        migration = F(p.persistent_asn_count,p.asn_count) if type(p.asn_count) is int and p.asn_count > 0 and type(p.persistent_asn_count) is int and 0 <= p.persistent_asn_count <= p.asn_count else None
        item = (p.country,(ratios[0]-min(ratios))*100,sum(v < F(95,100) for v in ratios),migration,tuple(shape),tuple(deltas))
        comparable.append(item)
        yield row('reference_country', event, (p.country,), refs=(p.source_ref,), cohort_id=p.cohort_id, definition_binding=p.definition_binding, decline_pp=item[1], below95_samples=item[2], migration_ratio=migration, shape=item[4])
    target_metrics = next((p for p in comparable if p[0] == reference.target), None)
    if target_metrics is None or len(comparable) < 2:
        yield row('reference_context', event, state='insufficient_data', comparable_count=len(comparable))
        return
    yield row('reference_context', event, state='complete', comparable_count=len(comparable))
    for index, name in ((1,'decline_pp'),(2,'below95_samples'),(3,'migration_ratio')):
        values = [p[index] for p in comparable if p[index] is not None]
        target_value = target_metrics[index]
        cdf = F(sum(v <= target_value for v in values)*100,len(values)) if values and target_value is not None else None
        yield row('reference_cdf', event, (name,), target=target_value, percentile=cdf, comparable_count=len(values), contributors=tuple(p[0] for p in comparable if p[index] is not None))
    for shape, count in sorted(Counter(p[4] for p in comparable).items()):
        yield row('reference_shape', event, shape, count=count, share=F(count,len(comparable)))
    for i in range(1,len(samples)):
        count = sum(p[5][i-1] <= -2 for p in comparable)
        yield row('reference_common', event, (samples[i],), declining_count=count, share=F(count,len(comparable)), target_declined=target_metrics[5][i-1] <= -2, causal_claim=False)
