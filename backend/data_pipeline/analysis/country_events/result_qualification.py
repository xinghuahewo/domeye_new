"""国家原科学行的逐维资格；冻结集合、当前状态和历史完整性分别判断。"""
from collections import defaultdict
from dataclasses import asdict
from fractions import Fraction
import hashlib
import json

from data_pipeline.analysis.country_events import event_aggregation as c2, compute, incremental_types as inc, snapshot_schema as raw_schema
from data_pipeline.analysis.country_events.route_contract import DIMENSIONS, CountryQualification, CountryCoverage, CountryQualifiedValue
from data_pipeline.bgp.record_types import GapScope, ScopeKind
from data_pipeline.bgp.replay.quality_overlay import position_key
from data_pipeline.analysis.detection.result_window import instant


def digest(values):
    return hashlib.sha256(raw_schema.encode(tuple(values)).encode()).hexdigest()


def qualification_rows(source, logical_run_id, raw_rows, *, max_rows, max_bytes, max_references_per_row, guard):
    from data_pipeline.analysis.country_events.input_staging import SourceStore
    from data_pipeline.analysis.country_events.snapshot_store import cleanup
    store=getattr(source,'store',None);owned=store is None
    if owned:
        store=SourceStore(None,max_rows=max_rows,max_bytes=max_bytes,max_row_bytes=max_bytes,
                          batch_rows=1,guard=guard)
    try:
        result=_qualification_rows(source,logical_run_id,raw_rows,max_rows=max_rows,max_bytes=max_bytes,
            max_references_per_row=max_references_per_row,guard=guard,store=store)
        if owned:result.owner=store
        return result
    except BaseException as error:
        if owned:cleanup((store.close,),error)
        raise


def _qualification_rows(source,logical_run_id,raw_rows,*,max_rows,max_bytes,max_references_per_row,guard,store):
    """原行不重写；新增行序由调用方接在完整原C2之后。限定实际D结果窗。

    complete均限定已声明的冷启动和观察窗；session连续性没有证据时仍Unknown。
    固定D可使用Gap前的原首次revision资格，不能由最终恢复状态反推初始D。
    """
    source.verify_saved()
    if any(type(n) is not int or n <= 0 for n in (max_rows, max_bytes, max_references_per_row)):
        raise ValueError('M3 逐维计算预算无效')
    window = tuple(int(instant(source.windows['result_window'][k]).timestamp())*1000000
                   for k in ('window_start', 'window_end_exclusive'))
    from data_pipeline.analysis.country_events.calculation_staging import ScientificRows
    raw=ScientificRows(store);used_bytes=0;input_count=0
    for item in raw_rows:
        guard(); table, row = raw_schema.row_encode(input_count, item)
        used_bytes += len(raw_schema.encode(row).encode())
        if getattr(source,'store',None) is None and (input_count >= max_rows or used_bytes > max_bytes):
            raise ValueError('resource_limit:M3_qualification_input')
        # 全原输入证据仍逐行计费/保留在C3；它们不参与国家逐目标资格，勿复制入RAM。
        if not (isinstance(item,c2.C2Row) and isinstance(item.value,c2.InputEvidence) and item.value.kind=='m3_original'):
            raw.append(input_count,item)
        input_count+=1
    if not raw or not isinstance(raw[-1], c2.C2Completion) or raw[-1].input_kind != 'saved_M3':
        raise ValueError('M3 逐维计算缺完整原C2')
    # 仅为仍集中驻留的事件/成员保留原预算，不用全流累计量代替工作集。
    resident_count=resident_bytes=0
    def retain(value):
        nonlocal resident_count,resident_bytes
        resident_count+=1;resident_bytes+=len(raw_schema.encode(value).encode())
        if resident_count>max_rows or resident_bytes>max_bytes:
            raise ValueError('resource_limit:M3_qualification_resident')
        return value
    events = {}
    for seq, _, item in raw.items():
        if isinstance(item,c2.C2Row) and isinstance(item.value,c2.EventStatus):
            retain(asdict(item.value));events[item.incident_id]=(seq,item.value)
    if len(events) != raw[-1].event_count:
        raise ValueError('M3 逐维原事件枚举不符')
    members = defaultdict(list)
    for _, _, item in raw.items():
        if isinstance(item, c2.C2Row) and isinstance(item.value, c2.CohortMember):
            retain(asdict(item.value.route));members[item.incident_id].append(item.value.route)
    owners = source.inputs.owners
    def ref(owner, view, ordinal):return (owner, owners[owner][0], view, str(ordinal))
    canonical_refs = tuple(retain(ref('canonical', 'source_coverage', i)) for i, _ in enumerate(source.get('canonical', 'source_coverage')))
    detection_refs = tuple(retain(ref('detection', 'result_coverage', i)) for i, _ in enumerate(source.get('detection', 'result_coverage')))
    results=store.new_map()
    for i,r in enumerate(source.get('detection','result_revisions')):results[r['raw']['incident_id'],r['raw']['revision']]=(i,r)
    if getattr(source,'store',None) is None:
        changes={r['event_id']:ref('canonical','changes',i) for i,r in enumerate(source.get('canonical','changes'))}
        change_rows={r['event_id']:r for r in source.get('canonical','changes')}
    else:
        from data_pipeline.analysis.country_events.input_staging import References, GapReferences
        change_rows=source.unique(source.get('canonical','changes'),'event_id')
        changes=References(change_rows,'canonical',owners['canonical'][0],'changes')
    earliest=store.new_map()
    for i, row in enumerate(source.get('detection', 'm3_entries')):
        if row['kind'] == 'event_qualification':
            key = row['incident_id'], row['revision']
            if key not in earliest or row['ordinal'] < earliest[key][0]['ordinal']:
                earliest[key] = row, ref('detection', 'm3_entries', i)
    gaps = [(row['raw'], ref('canonical', 'scope_gap', i)) for i, row in enumerate(source.get('canonical', 'scope_gap'))] if getattr(source,'store',None) is None else GapReferences(source.get('canonical','scope_gap'),owners['canonical'][0])
    d_gaps = tuple(retain(ref('detection', 'm3_entries', i)) for i, row in enumerate(source.get('detection', 'm3_entries')) if row['kind'] == 'scope_gap')
    coverage_payloads = [retain(json.loads(r['raw']['payload_json'])) for r in source.get('detection', 'result_coverage')]
    enumeration = bool(coverage_payloads) and all(p['dimension_coverage']['event_absence'] == 'complete' for p in coverage_payloads)
    scope = asdict(GapScope(ScopeKind.COLLECTOR_CHAIN, source.binding.collector, source.binding.input_binding_id, None, None))
    scope['kind'] = scope['kind'].value
    for k in ('route_families', 'prefixes', 'path_slots'):scope[k] = scope[k].value
    scope = raw_schema.encode(scope)
    emitted=ScientificRows(store)
    def emit(value, incident=None, revision=None):
        nonlocal used_bytes
        guard()
        fields = ('gap_refs', 'upstream_qualification_refs', 'evidence_refs', 'recovery_witnesses',
                  'completion_receipt_refs', 'unassigned_scope_refs', 'raw_basis_refs', 'qualification_refs')
        if sum(len(getattr(value, name, ())) for name in fields) > max_references_per_row:
            raise ValueError('resource_limit:M3_qualification_refs')
        used_bytes += len(raw_schema.encode(asdict(value)).encode())
        emitted.append(input_count+len(emitted),c2.C2Row(incident,revision,value))
        return value
    def applicable_gaps(at,incident=None):
        if incident is not None:
            from data_pipeline.analysis.country_events.qualification_evidence import relevant_gaps
            bounded=relevant_gaps(source,members[incident],change_rows,gaps,at,max_references_per_row,guard)
            if bounded is not None:return bounded
        found=[]
        for g,r in gaps:
            guard()
            if g['scope']['kind']!='local_observation' and g['raw_time']['epoch']*1000000+(g['raw_time']['microsecond'] or 0)<at:
                if len(found)>=max_references_per_row:raise ValueError('resource_limit:M3_qualification_refs')
                found.append(r)
        return tuple(found)
    frozen = {}
    for incident, (seq, status) in events.items():
        guard()
        selected = results.get((incident, status.incident.revision))
        if selected is None:raise ValueError('M3 原国家事件不在实际D完整结果选择中')
        first = min((revision for event, revision in results if event == incident))
        first_q, first_ref = earliest[incident, first]
        initial = json.loads(first_q['payload_json'])
        anchor = status.incident.onset
        anchor_ok = (anchor is not None and initial['dimension_coverage']['origin_anchor'] == 'complete'
                     and initial['dimension_coverage']['candidate_lifecycle'] == 'complete'
                     and status.baseline is not None and status.baseline.at.upper_us < anchor.lower_us
                     and not applicable_gaps(anchor.lower_us,incident))
        # 不从Gap后的current恢复、原成员数或原available标签补证初始集合完整。
        fixed = bool(anchor_ok and status.direction_count is not None
                     and all(r.mapping_state == 'bound' and r.presence in ('present', 'absent')
                             and (r.presence != 'present' or r.origin is not None) for r in members[incident]))
        frozen[incident] = fixed, first_ref
    peaks={}
    for seq, table, item in raw.items():
        guard()
        if not isinstance(item, c2.C2Row):continue
        v = item.value; sample = getattr(v, 'sample_us', None)
        if sample is not None and not window[0] < sample <= window[1]:continue
        incident, revision = item.incident_id, item.revision
        if incident is None:continue  # 原全局观察事实保留；国家结果资格绑定实际选中事件。
        status_seq, status = events[incident]
        fixed, first_ref = frozen[incident]
        result_index, selected = results[incident, revision]
        proof, dq = selected['lifecycle'], selected['qualification']
        dimensions = ()
        if isinstance(v, c2.EventStatus):dimensions = DIMENSIONS
        elif isinstance(v, c2.CohortMember):dimensions = ('cohort_membership', 'origin_attribution')
        elif isinstance(v, compute.MetricPoint):dimensions = ('fixed_denominator', 'new_member_enumeration' if v.metric.startswith('new_') else 'origin_attribution' if 'asn' in v.metric else 'point_presence')
        elif isinstance(v, (compute.PrefixPoint, inc.DirectionPoint, compute.AsnPoint)):dimensions = ('point_presence',)
        elif isinstance(v, (compute.PathSample, compute.PathSummary)):dimensions = ('path_current', 'path_history_completeness')
        elif isinstance(v, (compute.NewPrefixPoint, compute.NewDirectionPoint, inc.FirstQualifiedRef)):dimensions = ('new_member_enumeration',)
        elif isinstance(v, (compute.Peak, inc.AsnMetricPeak, inc.PathPeakQuality)):dimensions = ('window_peak',)
        elif isinstance(v, compute.WindowClass):dimensions = ('window_continuity',)
        elif isinstance(v, c2.InputEvidence) and v.kind == 'event_revision':dimensions = ('event_anchor', 'event_lifecycle')
        if not dimensions:continue
        at = sample or window[1]
        relevant = applicable_gaps(at,incident)
        path_state = None
        if isinstance(v,compute.PathSample):
            from data_pipeline.analysis.country_events.route_time_index import state_before
            observed = change_rows.get(v.observation_ref)
            if observed is None:raise ValueError('M3 路径样本缺原Canonical观察')
            path_state = state_before(source.history,source.messages,observed['raw']['scope'],sample)
        bases = tuple(dict.fromkeys((ref('detection', 'result_revisions', result_index), first_ref, *canonical_refs,
                                     *(changes[r.observation_ref] for r in members[incident] if r.observation_ref in changes))))
        for dimension in dimensions:
            coverage = 'unknown'; qgaps = relevant; recovery=(); effective=None
            if dimension == 'event_enumeration':coverage = 'complete' if enumeration else 'unknown'; qgaps = d_gaps
            elif dimension == 'event_anchor':
                coverage = 'complete' if proof['anchor_coverage'] == 'complete_within_declared_cold_start' and dq['dimension_coverage']['origin_anchor'] == 'complete' else 'unknown'; qgaps = d_gaps
            elif dimension == 'event_lifecycle':
                coverage = 'complete' if proof['selection'] != 'possible_unknown' and dq['dimension_coverage']['candidate_lifecycle'] == 'complete' else 'unknown'; qgaps = d_gaps
            elif dimension in ('cohort_membership', 'fixed_denominator'):
                coverage = 'complete' if fixed else 'unknown'; qgaps = applicable_gaps(status.incident.onset.lower_us,incident) if status.incident.onset else relevant
            elif dimension == 'point_presence':
                state = v.route.presence if isinstance(v, c2.CohortMember) else getattr(v, 'state', 'unknown')
                known = state not in ('unknown', 'unavailable')
                if isinstance(v, compute.MetricPoint):known = v.value is not None
                if isinstance(v, c2.EventStatus):known = False  # 必须查询具体样本/对象，不能以available推定所有时刻。
                coverage = 'complete' if fixed and known else 'unknown'
            elif dimension == 'origin_attribution':
                known = all(r.presence != 'present' or r.origin is not None for r in members[incident])
                if isinstance(v, c2.CohortMember):known = v.route.origin is not None
                if isinstance(v, compute.MetricPoint):known = known and v.value is not None
                coverage = 'complete' if fixed and known else 'unknown'
            elif dimension == 'new_member_enumeration':
                qgaps=applicable_gaps(at)  # 潜在新成员不能按已冻结cohort排除。
                known = not isinstance(v, compute.MetricPoint) or v.value is not None
                coverage = 'complete' if fixed and not qgaps and known else 'unknown'
            elif dimension == 'path_current':
                # 历史PathSample本身不证明current；必须回到原对象的时序边界和后态。
                if path_state is not None and path_state['time_boundary']=='located':
                    effective=path_state['position']
                    qgaps=tuple(r for g,r in gaps if g['gap_id'] in path_state['gap_refs'])
                    known=path_state['presence']=='present' and path_state['path_key']==v.path_ref
                    coverage='complete' if fixed and known else 'not_applicable' if fixed and path_state['presence']=='absent' else 'unknown'
                    witness=path_state['source_ref']
                    if known and qgaps and witness is not None and witness['table']=='changes':
                        recovery=(ref('canonical','changes',witness['ordinal']),)
            elif dimension == 'path_history_completeness':
                coverage = 'complete' if fixed and not relevant else 'partial' if fixed else 'unknown'
                if path_state is not None and path_state['time_boundary']!='located':coverage='unknown'
            elif dimension == 'window_peak':
                if getattr(v,'metric','').startswith('new_'):qgaps=applicable_gaps(at)
                exact = getattr(v, 'unknown_slots', None) == 0
                if isinstance(v, inc.AsnMetricPeak):exact = v.exact_peak is not None
                coverage = 'complete' if fixed and not qgaps and exact else 'unknown'
            # 无session连续性证明时window_continuity保持Unknown，不以无Gap充当连续性。
            q = emit(CountryQualification(logical_run_id, source.binding.input_binding_id,
                     'revision' if isinstance(v, (c2.EventStatus, c2.InputEvidence)) else 'object' if isinstance(v, c2.CohortMember) else 'sample' if sample is not None else 'metric',
                     (table, seq), incident, revision, getattr(v, 'cohort_id', status.cohort_id),
                     getattr(getattr(v, 'route', None), 'object_id', getattr(v, 'prefix', None)),
                     getattr(v, 'afi', None) if getattr(v, 'afi', None) in (1, 2) else None,
                     getattr(v, 'metric', None), dimension, scope, effective, None, sample, window,
                     coverage, qgaps, (first_ref,), bases, recovery), incident, revision)
            if isinstance(v, compute.MetricPoint) and dimension != 'fixed_denominator':
                lower=None;witnesses=()
                if fixed and v.metric=='visible_direction_count':
                    from data_pipeline.analysis.country_events.qualification_evidence import visible_directions
                    lower,witnesses=visible_directions(source,members[incident],change_rows,sample,ref,guard,max_references_per_row)
                if lower is not None:
                    previous=peaks.get((incident,v.metric))
                    if previous is None or lower>previous[0]:peaks[incident,v.metric]=(lower,witnesses,q.qualification_id)
                value_bases=tuple(dict.fromkeys((*bases,*witnesses)))
                emit(CountryQualifiedValue(logical_run_id, source.binding.input_binding_id, (table, seq), dimension,
                     v.value if coverage == 'complete' else None, lower, lower is not None, v.unit,
                     ('event_status', status_seq), v.denominator if fixed else None, value_bases,
                     (q.qualification_id,), window, coverage), incident, revision)
            elif isinstance(v, c2.EventStatus) and dimension == 'fixed_denominator':
                emit(CountryQualifiedValue(logical_run_id, source.binding.input_binding_id, (table, seq), dimension,
                     v.direction_count if fixed else None, None, False, 'endpoint_direction_count',
                     (table, seq), v.direction_count if fixed else None, bases,
                     (q.qualification_id,), window, coverage), incident, revision)
            elif isinstance(v, (compute.Peak, inc.AsnMetricPeak)):
                value = v.value if isinstance(v, compute.Peak) else v.exact_peak
                proved=peaks.get((incident,v.metric)) if isinstance(v,compute.Peak) else None
                lower=proved[0] if proved is not None else None
                value_bases=tuple(dict.fromkeys((*bases,*(proved[1] if proved else ()))))
                qrefs=(q.qualification_id,proved[2]) if proved else (q.qualification_id,)
                emit(CountryQualifiedValue(logical_run_id, source.binding.input_binding_id, (table, seq), dimension,
                     value if coverage == 'complete' else None, lower, lower is not None, getattr(v, 'unit', v.metric),
                     ('event_status', status_seq), None, value_bases, qrefs, window, coverage), incident, revision)
    receipts = tuple(retain((source.inputs.admissions[aid]['owner'], aid, view, 'receipt:'+saved['receipt']['request_digest']))
                     for (aid, view), saved in sorted(source.receipts.items()))
    for dimension in DIMENSIONS:
        count=complete=0;all_q_gaps=set();ids=hashlib.sha256();ids.update(b'["tuple",[')
        for item in emitted:
            q=item.value
            if not isinstance(q,CountryQualification) or q.dimension!=dimension:continue
            if count:ids.update(b',')
            ids.update(raw_schema.encode(q.qualification_id).encode());count+=1;complete+=q.coverage=='complete'
            all_q_gaps.update(q.gap_refs)
            if len(all_q_gaps)>max_references_per_row:raise ValueError('resource_limit:M3_qualification_refs')
        ids.update(b']]')
        all_gaps = tuple(sorted(set(d_gaps if dimension in ('event_enumeration', 'event_anchor', 'event_lifecycle') else applicable_gaps(window[1])) | all_q_gaps))
        state = 'complete' if dimension == 'event_enumeration' and enumeration else 'complete' if count and complete==count else 'partial' if complete else 'unknown'
        emit(CountryCoverage(logical_run_id, source.binding.input_binding_id, 'country', None, window, dimension,
             None, None, len(events), 'complete', state,count,ids.hexdigest(),
             len(all_gaps), digest(all_gaps), all_gaps, receipts))
    source.verify_saved()
    results.close();earliest.close();raw.close();store.flush()
    emitted.costs=dict(input_rows=input_count,output_rows=len(emitted),logical_bytes=used_bytes,
                       resident_rows=resident_count,resident_bytes=resident_bytes)
    return emitted
