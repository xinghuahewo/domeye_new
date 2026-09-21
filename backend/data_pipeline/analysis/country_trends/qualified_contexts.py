"""原 ASN、双族和路径的有限资格消费；原对象从不被改写成合格。"""
from dataclasses import replace
from ipaddress import ip_network

from data_pipeline.analysis.country_events import event_aggregation as c2
from data_pipeline.analysis.country_events.compute import AsnPoint, PrefixPoint, PathSample, PathSummary, Quality, MetricPoint
from data_pipeline.analysis.country_trends.contract import row
from data_pipeline.analysis.country_trends.contexts import asn_rows, family_projection, family_rows


def _condition(item, refs, reasons):
    return replace(item, values=(*item.values,
        ('qualification_refs', tuple(dict.fromkeys(refs))),
        ('qualification_reasons', tuple(dict.fromkeys(reasons))),
        ('sample_basis', 'qualified_samples'), ('continuous_duration_claimed', False)))


def asn_context(sources, samples):
    """每个合格原点独立可读；完整矩阵/排名须固定人口与所有所用点可用。"""
    members = []
    points = []
    why, qrefs = sources.qualification(sources.population_ref, ('cohort_membership',))
    population_reasons, population_refs = why, qrefs
    refs, reasons = list(qrefs), list(why)
    member_proofs = []
    for key, original in sources.originals.items():
        if type(original) is c2.CohortMember:
            members.append(original)
            why, qrefs = sources.qualification(key, ('cohort_membership', 'origin_attribution'))
            refs.extend(qrefs); reasons.extend(why)
            member_proofs.append((original.route, why, qrefs))
    for key, original in sources.originals.items():
        if type(original) is AsnPoint and original.sample_us in samples:
            points.append(original)
            why, qrefs = sources.qualification(key, ('point_presence',))
            related = [(r, w, q) for r, w, q in member_proofs if r.prefix in original.prefix_refs
                       and original.afi in (0, r.endpoint.afi)]
            why = (*population_reasons, *why, *(w for _, ww, _ in related for w in ww))
            qrefs = (*population_refs, *qrefs, *(q for _, _, qq in related for q in qq))
            if set(original.prefix_refs) != {r.prefix for r, _, _ in related if r.origin == original.asn}:
                why += ('asn_prefix_population_missing',)
            refs.extend(qrefs); reasons.extend(why)
            yield row('qualified_asn_point', sources.event,
                      (original.asn, original.afi, original.sample_us), refs=qrefs,
                      raw=original, main=None if why else original,
                      reasons=why, raw_target_ref=key)
    if reasons:
        # 不将缺资格对象视为0人口，也不对未经证明的全矩阵计算排名。
        yield _condition(row('asn_context', sources.event, state='unavailable',
                             reason='asn_population_or_point_qualification'), refs, reasons)
        return
    for item in asn_rows(sources.event, samples, members, points):
        yield _condition(item, refs, ())


def family_context(sources, samples):
    members = [v for v in sources.originals.values() if type(v) is c2.CohortMember]
    prefixes = [v for v in sources.originals.values() if type(v) is PrefixPoint and v.sample_us in samples]
    main = {v.sample_us: v for v in sources.originals.values()
            if type(v) is MetricPoint and v.metric == 'visible_direction_count' and v.sample_us in samples}
    quality = [v for v in sources.originals.values() if type(v) is Quality]
    denominators, values = family_projection(sources.event, samples, members, prefixes,
                                            sources.status, main, quality)
    member_reasons = {1: [], 2: []}
    member_refs = {1: [], 2: []}
    point_reasons = {(afi, t): [] for afi in (1, 2) for t in samples}
    point_refs = {(afi, t): [] for afi in (1, 2) for t in samples}
    refs = []
    for key, original in sources.originals.items():
        if type(original) is c2.CohortMember:
            afi = original.route.endpoint.afi
            why, qrefs = sources.qualification(key, ('cohort_membership',))
            member_reasons[afi].extend(why); refs.extend(qrefs)
            member_refs[afi].extend(qrefs)
        elif type(original) is PrefixPoint and original.sample_us in samples:
            afi = 1 if ip_network(original.prefix).version == 4 else 2
            why, qrefs = sources.qualification(key, ('point_presence',))
            point_reasons[afi, original.sample_us].extend(why); refs.extend(qrefs)
            point_refs[afi, original.sample_us].extend(qrefs)
    d_reasons, d_refs = sources.qualification(sources.population_ref, ('fixed_denominator', 'cohort_membership'))
    denominator = sources.denominator()
    d_reasons = (*d_reasons, *denominator.reasons)
    d_refs = (*d_refs, *denominator.qualification_refs)
    refs.extend(d_refs)
    all_reasons = list(d_reasons)
    for afi in (1, 2):
        why = (*member_reasons[afi], *d_reasons)
        if why:
            denominators[afi] = None
        for index, t in enumerate(samples):
            reasons = (*why, *point_reasons[afi, t])
            if reasons:
                values[afi][index] = None
            all_reasons.extend(reasons)
    for item in family_rows(sources.event, samples, denominators, values):
        if item.kind == 'family_point':
            afi, t = item.key
            local_reasons = (*member_reasons[afi], *d_reasons, *point_reasons[afi, t])
            if item.get('value') is None and not local_reasons:
                local_reasons = ('original_family_value_unknown',)
            yield _condition(item, (*member_refs[afi], *d_refs, *point_refs[afi, t]), local_reasons)
        else:
            yield _condition(item, refs, all_reasons)


def path_context(sources):
    """当前与历史分别给出资格和原引用，current恢复不会推广为历史完整。"""
    for key, original in sources.originals.items():
        if type(original) not in (PathSample, PathSummary):
            continue
        current, cr = sources.qualification(key, ('path_current',))
        history, hr = sources.qualification(key, ('path_history_completeness',))
        yield row('qualified_path', sources.event, key, refs=tuple(dict.fromkeys((*cr, *hr))),
                  raw=original, raw_target_ref=key,
                  current=None if current else original, current_reasons=current,
                  history=None if history else original, history_reasons=history,
                  continuity='unknown', causal_claim=False)
