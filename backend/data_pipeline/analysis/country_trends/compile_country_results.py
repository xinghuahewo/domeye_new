"""从已捕获公开Country原流编译S3；保留原历史，不自行选择最大revision。"""
from dataclasses import asdict, replace
from itertools import chain

from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_schema as original, qualified_schema as m3_schema
from data_pipeline.analysis.country_trends.contract import row, digest
from data_pipeline.analysis.country_trends.contexts import activity_rows, reference_rows
from data_pipeline.analysis.country_trends.qualified_contexts import _condition
from data_pipeline.analysis.country_trends.event_inputs import EventSources
from data_pipeline.analysis.country_trends.qualified_calculation import event_series
from data_pipeline.analysis.country_trends.qualified_contexts import asn_context, family_context, path_context
from data_pipeline.analysis.country_trends.result_coverage import coverage_rows
from data_pipeline.analysis.country_trends.evidence_graph import evidence_rows
from data_pipeline.analysis.country_trends.country_source import decoded_rows


def compile_country(db, result_id, window_us, budget, *, activities=(), references=(),
                    context_sources=(), feature_admission=None, feature_selections=()):
    """原21表全流及新资格逐行保存，只有公开events选定的revision进入分析。"""
    def bound(item):
        budget.check()
        return replace(item, result_id=result_id)
    for source, role, raw in context_sources:
        yield bound(row('context_source', (), (source,), source_ref=source, role=role, raw_typed=raw))
    if feature_admission is not None:
        from data_pipeline.bgp.archive.value_codec import untyped
        from data_pipeline.analysis.country_trends.stream_schema import encode, flatten
        def feature_source(source,value):
            item=bound(row('context_source', (), (source,), source_ref=source, role='feature', raw_typed=encode(value)))
            # 此处仅构造单行；写出端以实际sequence核完整编码。
            size=len(encode(flatten(0,item)).encode())
            if size>min(budget.limits.max_context_bytes,budget.limits.max_row_bytes,budget.limits.max_batch_bytes):
                raise ValueError('trend_feature_public_source_budget')
            return item
        for view, ordinal, payload in db.execute('SELECT * FROM feature_input ORDER BY view,ordinal'):
            source = 'feature:' + feature_admission['admission_id'] + ':' + view + ':' + str(ordinal)
            yield feature_source(source,untyped(payload))
        from data_pipeline.analysis.country_trends.feature_identity import sources as identity_sources
        for source, value in identity_sources(db, feature_admission, budget):
            yield feature_source(source,value)
    coverages = []
    for sequence, item in decoded_rows(db):
        budget.check()
        table, wrapped = m3_schema.row_encode(sequence, item)
        event = () if type(item) is not c2.C2Row or item.incident_id is None else (item.incident_id, item.revision)
        if table in original.TABLES:
            raw = (item.incident_id, item.revision, item.value) if type(item) is c2.C2Row else item
            yield bound(row('raw_source', event, (sequence,), source_sequence=sequence,
                            source_table=table, raw_typed=original.encode(raw)))
            if not event and table in ('input_evidence', 'observation_fact'):
                yield bound(row('source_evidence', (), (sequence,), raw_typed=original.encode(raw)))
        else:
            value = item.value
            if table == 'country_coverage':
                if len(coverages) >= 12: raise ValueError('trend_country_coverage_count')
                coverages.append(asdict(value))
                yield bound(row('coverage_source', (), (value.coverage_id,),
                    raw_typed=original.encode(dict(table=table, row=wrapped)),
                    coverage_id=value.coverage_id, dimension=value.dimension))
            else:
                kind, field = ('country_qualification_source', 'qualification_id') if table == 'country_qualification' else ('qualified_value_source', 'qualified_value_id')
                identity = getattr(value, field)
                yield bound(row(kind, event, (identity,), raw_typed=original.encode(dict(table=table, row=wrapped)),
                    **{field: identity}, raw_target_ref=value.raw_target_ref, dimension=value.dimension))
    if len(coverages) != 12: raise ValueError('trend_country_independent_coverage')
    logical_run = coverages[0]['logical_run_id']; input_binding = coverages[0]['input_binding_id']
    event_count = db.execute('SELECT count(*) FROM country_events').fetchone()[0]
    # 事件已落盘并按原序逐个读取；总数仅用于覆盖核对，不作为驻留数量上限。
    for output in coverage_rows(logical_run, input_binding, window_us, coverages, event_count):
        yield bound(output)
    for incident, revision, sequence, payload in db.execute('SELECT * FROM country_events ORDER BY sequence'):
        event = incident, revision
        size = db.execute('SELECT coalesce(sum(bytes),0) FROM country_input WHERE incident=? AND revision=?', event).fetchone()[0]
        if size + len(payload.encode()) > budget.limits.max_event_bytes:
            raise ValueError('trend_country_event_memory_budget')
        originals, qs, qvs = [], [], []
        for seq, item in decoded_rows(db, event=event):
            if type(item.value) in original.OUTPUTS: originals.append((seq, item))
            elif type(item.value) is m3_schema.NEW_TABLES['country_qualification']: qs.append(asdict(item.value))
            elif type(item.value) is m3_schema.NEW_TABLES['country_qualified_value']: qvs.append(asdict(item.value))
        sources = EventSources(logical_run, input_binding, window_us, event, originals, qs, qvs)
        envelope = original.decode(payload)
        status = sources.status
        yield bound(row('event', event, state=status.state, reasons=status.reasons,
            cohort_id=status.cohort_id, basis='country_m3_calculation', raw=status,
            main=status if envelope['main'] is not None else None, lifecycle=envelope['lifecycle'],
            original_revisions=envelope['original_revisions']))
        if status.cohort_id is None:
            yield bound(row('availability', event, state='missing_baseline'))
            continue
        for value in sources.originals.values():
            name = type(value).__name__
            if name in ('InputEvidence', 'CohortMember', 'PrefixPoint'):
                yield bound(row('source_evidence', event, (name, digest(original.encode(value))), raw_typed=original.encode(value)))
            elif name == 'Quality': yield bound(row('quality', event, (value.code, value.reference), raw=value))
            elif name in ('WindowClass', 'AsnMetricPeak', 'Peak'):
                yield bound(row('source_window_peak', event, (name, digest(original.encode(value))), raw=value))
        samples = tuple(sorted(v.sample_us for v in sources.originals.values()
            if type(v).__name__ == 'MetricPoint' and v.metric == 'visible_direction_count'
            and window_us[0] < v.sample_us <= window_us[1]))
        for producer in (event_series(sources, limits=budget.limits), asn_context(sources, samples),
                         family_context(sources, samples), path_context(sources)):
            for scientific in producer:
                yield bound(scientific)
                for output in evidence_rows(scientific): yield bound(output)
        selected = [sources.metric(key) for key, v in sources.originals.items()
                    if type(v).__name__ == 'MetricPoint' and v.metric == 'visible_direction_count' and v.sample_us in samples]
        selected.sort(key=lambda p: p.target.sample_us)
        denominator = sources.denominator()
        d = denominator.value if all(p.denominator == denominator.value for p in selected) else None
        refs = tuple(q for p in (*selected, denominator) for q in p.qualification_refs)
        reasons = tuple(r for p in (*selected, denominator) for r in (*p.reasons, *p.denominator_reasons))
        windows = tuple(w for w in activities if w.event == event)
        activity = activity_rows(event, samples, [p.value for p in selected], d, windows)
        if not windows and any(selection[0] == event for selection in feature_selections):
            activity = (row('activity_context', event, state='unavailable', reason='no_exact_country_code_window'),)
        reference = [r for r in references if r.event == event]
        if len(reference) > 1: raise ValueError('trend_multiple_reference')
        if reference and (d is None or any(p.value is None for p in selected)):
            comparison = (row('reference_context', event, state='insufficient_data', reason='target_qualification_unknown'),)
        else:
            comparison = reference_rows(event, samples, [p.value for p in selected], d,
                                         reference[0] if reference else None, status.cohort_id)
        for output in chain(activity, comparison):
            output = _condition(output, refs, reasons)
            yield bound(output)
            for evidence in evidence_rows(output): yield bound(evidence)
