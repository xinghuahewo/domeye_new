"""S1：一次人工C3同形流→磁盘有界排序→纯趋势行；没有业务存储/PG。"""
from dataclasses import fields, replace
from fractions import Fraction
from hashlib import sha256
from itertools import chain
from pathlib import Path
import resource
import sqlite3
import sys
import tempfile
import time

from data_pipeline.analysis.country_events import event_aggregation as c2
from data_pipeline.analysis.country_events.compute import MetricPoint, PrefixPoint, AsnPoint, Completion, Quality, ObservationFact
from data_pipeline.analysis.country_events.snapshot_schema import encode, decode, OUTPUTS
from data_pipeline.analysis.country_trends.contract import TrendInput, FixtureBatch, FixtureEnd, Limits, TrendCompletion, TRACKS, VERSION, RULE, ACTIVITY_RULE, packed, digest, row, exact
from data_pipeline.analysis.country_trends.analysis import profile_rows
from data_pipeline.analysis.country_trends.contexts import asn_rows, family_projection, family_rows, activity_rows, reference_rows


def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)


class Budget:
    def __init__(self, limits, guard):
        if any(type(getattr(limits, f.name)) is not int or getattr(limits, f.name) <= 0 for f in fields(limits)):
            raise ValueError('trend_invalid_limits')
        self.limits, self.guard = limits, guard
        self.rows = 0
        self.disk_peak = 0
        self.directory = None

    def check(self):
        self.guard()
        if rss() > self.limits.max_rss_bytes:
            raise ValueError('trend_rss_limit')
        if self.directory:
            size = sum(p.stat().st_size for p in Path(self.directory).iterdir() if p.is_file())
            self.disk_peak = max(self.disk_peak, size)
            if size > self.limits.max_disk_bytes:
                raise ValueError('trend_disk_limit')

    def charge(self, payload):
        self.rows += 1
        if self.rows > self.limits.max_rows or len(payload) > self.limits.max_row_bytes:
            raise ValueError('trend_row_budget')
        self.check()


def validate_input(request, budget):
    if type(request) is not TrendInput or request.data_kind != 'fixture':
        raise ValueError('trend_fixture_only')
    if request.schema_version != VERSION:
        raise ValueError('trend_input_version')
    b = request.country
    if b.qualification != 'fixture_strict_no_m3' or b.interpretation != 'strict':
        raise ValueError('trend_m3_not_supported')
    if len(b.component) != 9 or not str(b.component[3]).startswith('fixture') or not b.read_binding or not b.c1_binding:
        raise ValueError('trend_fixture_binding')
    if request.rules != (RULE, ACTIVITY_RULE):
        raise ValueError('trend_rule_mismatch')
    if len(packed(request)) > budget.limits.max_context_bytes:
        raise ValueError('trend_context_budget')
    for context in (*request.activities, *(p for ref in request.references for p in ref.projections)):
        budget.charge(packed(context))
    if request.activities and not request.feature_binding:
        raise ValueError('trend_feature_not_bound')
    # 这些字符串是人工固定绑定证据，不冒充已核真实资格。
    if request.feature_binding and request.feature_binding[0] != 'fixture':
        raise ValueError('trend_feature_fixture_only')
    if any(not r.binding or r.binding[0] != 'fixture' for r in request.references):
        raise ValueError('trend_reference_fixture_only')


def metric_validate(point):
    unit = dict(c2.METRICS).get(point.metric)
    if unit is None or point.unit != unit:
        raise ValueError('trend_metric_definition')
    for value in (point.value, point.known_lower_bound, point.denominator, point.ratio, point.fact_value):
        exact(value)
    if point.denominator is not None and (type(point.denominator) is not int or point.denominator < 0):
        raise ValueError('trend_denominator')
    if point.known_lower_bound < 0 or (point.value is not None and point.value < point.known_lower_bound):
        raise ValueError('trend_lower_bound')
    expected = 'unknown' if point.value is None else 'calculated_zero' if point.value == 0 else 'calculated'
    ratio = Fraction(point.value,point.denominator) if point.value is not None and point.denominator else None
    if point.state != expected or point.ratio != ratio:
        raise ValueError('trend_metric_state_ratio')


def event_rows(event, values, request, limits, *, denominator_rule=None):
    if denominator_rule not in (None,'strict_c3_unknown_denominator/v1'):
        raise ValueError('trend_denominator_rule')
    statuses = [v for v in values if isinstance(v,c2.EventStatus)]
    if len(statuses) != 1:
        raise ValueError('trend_event_status_count')
    status = statuses[0]
    if (status.incident.incident_id,status.incident.revision) != event:
        raise ValueError('trend_incident_identity')
    cohort = status.cohort_id
    if any(getattr(v,'cohort_id',cohort) != cohort for v in values):
        raise ValueError('trend_cohort_identity')
    for value in values:
        if isinstance(value,(c2.InputEvidence,c2.CohortMember,PrefixPoint)):
            yield row('source_evidence',event,(type(value).__name__,digest(encode(value))),raw_typed=encode(value))
    yield row('event', event, state=status.state, reasons=status.reasons, cohort_id=cohort, basis='fixture_calculation')
    if cohort is None:
        if any(isinstance(v,(MetricPoint,PrefixPoint,AsnPoint,Completion,c2.CohortMember)) for v in values):
            raise ValueError('trend_unavailable_cohort_rows')
        yield row('availability', event, state='missing_baseline')
        return
    completions = [v for v in values if isinstance(v,Completion)]
    if len(completions) != 1:
        raise ValueError('trend_completion_count')
    completion = completions[0]
    metrics = {}
    for v in values:
        if isinstance(v,MetricPoint):
            metric_validate(v)
            key=(v.metric,v.sample_us)
            if key in metrics: raise ValueError('trend_duplicate_metric')
            metrics[key]=v
    samples=tuple(sorted(t for metric,t in metrics if metric=='visible_direction_count'))
    if len(samples)>limits.max_samples:
        raise ValueError('trend_sample_budget')
    start,end=completion.input_window
    event_start=(status.incident.onset or status.incident.detected_at).lower_us
    event_end=status.incident.end.lower_us if status.incident.end else end
    if start>=end or any(type(t) is not int or not start<t<=end or not event_start<t<=event_end for t in samples):
        raise ValueError('trend_input_window')
    expected_window=(samples[0],samples[-1]) if samples else None
    if (completion.sample_count,completion.sample_window,completion.data_through_us)!=(len(samples),expected_window,samples[-1] if samples else start):
        raise ValueError('trend_completion_grid')
    required={*( (name,t) for name in TRACKS for t in samples), *(('visible_direction_count',t) for t in samples)}
    if not required.issubset(metrics) or any(t not in samples for _,t in metrics):
        raise ValueError('trend_metric_matrix')
    yield row('source_completion',event,raw=completion)
    for name in (*TRACKS,'visible_direction_count'):
        points=[metrics[name,t] for t in samples]
        for p in points:
            yield row('metric',event,(name,p.sample_us),raw=p)
        denominators={p.denominator for p in points}
        if denominator_rule is None:
            if len(denominators)>1:raise ValueError('trend_fixed_denominator_drift')
        else:
            # 正式C3的不可定位原槽可没有D；不补回D，不改变任何MetricPoint。
            required_d=name in ('interrupted_prefix_count','completely_interrupted_prefix_count',
                'invisible_direction_count','affected_asn_count','route_interrupted_asn_count','visible_direction_count')
            missing=[p for p in points if p.denominator is None]
            boundaries={v.sample_us for v in values if isinstance(v,c2.BoundaryUnavailable)}
            if len(denominators-{None})>1:raise ValueError('trend_fixed_denominator_drift')
            if required_d and any(p.value is not None or p.state!='unknown' or p.sample_us not in boundaries for p in missing):
                raise ValueError('trend_unproven_missing_denominator')
        denominator=None if None in denominators else next(iter(denominators),None)
        yield from profile_rows(event,name,samples,[p.value for p in points],dict(c2.METRICS)[name],denominator)
    members=[v for v in values if isinstance(v,c2.CohortMember)]
    objects=[m.route.object_id for m in members]
    if len(objects)!=len(set(objects)):
        raise ValueError('trend_duplicate_member')
    main={t:metrics['visible_direction_count',t] for t in samples}
    qualities=[v for v in values if isinstance(v,Quality)]
    # 这是C2已有Unknown/质量，不能当作新M3支持；新Gap协议已在入口拒绝。
    for q in qualities:
        yield row('quality',event,(q.code,q.reference),raw=q)
    denominators,family_values=family_projection(event,samples,members,[v for v in values if isinstance(v,PrefixPoint)],status,main,qualities)
    yield from family_rows(event,samples,denominators,family_values)
    yield from asn_rows(event,samples,members,[v for v in values if isinstance(v,AsnPoint)])
    for v in values:
        if type(v).__name__ in ('WindowClass','AsnMetricPeak','Peak'):
            yield row('source_window_peak',event,(type(v).__name__,digest(encode(v))),raw=v)
    windows=tuple(sorted((w for w in request.activities if w.event==event),key=lambda w:(w.source_rank,w.start_us,w.end_us,w.mode,w.metric)))
    references=[r for r in request.references if r.event==event]
    if references and references[0].target != status.incident.country:
        raise ValueError('trend_reference_event_country')
    if len(references)>1:
        raise ValueError('trend_multiple_reference')
    main_values=[main[t].value for t in samples]
    denominator=main[samples[0]].denominator if samples else None
    yield from activity_rows(event,samples,main_values,denominator,windows)
    yield from reference_rows(event,samples,main_values,denominator,references[0] if references else None,cohort)


# 有限导航白名单；不把任意表或未知指标注册为可陈述科学结论。
GRAPH_KINDS = frozenset(('profile','fact','peak','point','analysis','atomic','phase',
                        'family_context','asn_context','activity_relation',
                        'reference_context','reference_country','reference_cdf',
                        'reference_shape','reference_common'))


def graph_rows(source, *, input_condition='fixture_input_only'):
    """科学行→Evidence→有限来源定位/条件；不可用值只形成Unknown。"""
    if source.kind not in GRAPH_KINDS:
        return
    event=source.event
    values=dict(source.values)
    locator=(source.kind,event,source.key)
    dependencies=[locator]
    if source.kind in ('point','phase','analysis','atomic','fact'):
        name=source.key[0]
        dependencies.append(('profile',event,(name,)))
        if source.kind=='point':
            for ref in source.refs:
                if isinstance(ref,str) and ref.startswith('metric:'):
                    _,metric,at=ref.split(':')
                    dependencies.append(('metric',event,(metric,int(at))))
        if source.kind=='phase':
            dependencies.extend(('metric',event,(name,values[k])) for k in ('start_us','end_us'))
    elif source.kind=='activity_relation':
        dependencies.extend((('activity_window',event,values['window_key']),
                             ('point',event,values['state_point_key'])))
    elif source.kind=='reference_cdf':
        dependencies.extend(('reference_country',event,(country,)) for country in values['contributors'])
        dependencies.append(('reference_context',event,()))
    node=digest(source)
    conditions=(input_condition,'fixed_result_event_population_window',
                'no_cause_user_impact_or_post_window_recovery')
    yield row('evidence',event,(node,),source_kind=source.kind,source_key=source.key,
              source_locator=locator,values=source.values,source_refs=source.refs,conditions=conditions)
    for index,dependency in enumerate(dict.fromkeys(dependencies)):
        yield row('evidence_source',event,(node,index),source_locator=dependency)
    unknown=(values.get('state') in ('unavailable','no_samples','degraded','not_provided','insufficient_data')
             or source.kind=='fact' and values['value'] is None
             or source.kind=='reference_cdf' and values['percentile'] is None)
    if unknown:
        yield row('unknown',event,(digest((node,'unsupported_value')),),code='scientific_value_unavailable',
                  evidence_id=node,source_locator=locator,reason=values.get('reason','insufficient_numeric_support'))
        return
    claim=digest((node,'claim'))
    yield row('claim',event,(claim,),evidence_id=node,source_locator=locator,
              conclusion_level='bounded_control_plane',
              state='known_subset' if source.kind=='peak' and values['exact_peak'] is None else 'bounded_fact')
    yield row('edge',event,(claim,'supported_by',node))
    for kind,code in (('limitation','fixed_collector_window_population'),
                      ('unknown','cause_user_impact_responsibility_post_window')):
        target=digest((event,kind,code,node))
        yield row(kind,event,(target,),code=code)
        yield row('edge',event,(claim,'limited_by' if kind=='limitation' else 'unknown_about',target))


def compute_trends(request, stream, *, limits=Limits(), guard=lambda:None, temporary_parent=None):
    """公开S1入口。人工batch/尾回执；异常或早停无TrendCompletion。"""
    started=time.monotonic()
    budget=Budget(limits,guard)
    iterator=db=temporary=completion=None
    primary=None
    input_hash=sha256();output_hash=sha256();input_count=output_count=0
    try:
        validate_input(request,budget)
        iterator=iter(stream)
        temporary=tempfile.TemporaryDirectory(prefix='country-trend-s1-',dir=temporary_parent)
        budget.directory=temporary.name
        db=sqlite3.connect(Path(temporary.name)/'sort.sqlite')
        db.execute('PRAGMA cache_size=-2048')
        db.execute('PRAGMA temp_store=FILE')
        db.execute('CREATE TABLE rows(event TEXT,revision INTEGER,sequence INTEGER,payload TEXT,bytes INTEGER,PRIMARY KEY(event,revision,sequence)) WITHOUT ROWID')
        end=None;component_end=None
        for item in iterator:
            if end is not None: raise ValueError('trend_after_input_end')
            if type(item) is FixtureEnd:
                end=item
                continue
            if type(item) is not FixtureBatch or len(item.rows)>limits.max_batch_rows:
                raise ValueError('trend_fixture_batch')
            batch_bytes=0
            for source in item.rows:
                event_key=None
                if type(source) is c2.C2Completion:
                    if component_end is not None: raise ValueError('trend_duplicate_component_end')
                    component_end=source
                    payload=encode(source)
                elif type(source) is c2.C2Row and type(source.value) in OUTPUTS and type(source.value) is not c2.C2Completion:
                    if (source.incident_id,source.revision) != (None,None) and (type(source.incident_id) is not str or type(source.revision) is not int or source.revision<1):
                        raise ValueError('trend_event_key')
                    payload=encode((source.incident_id,source.revision,source.value))
                    if source.incident_id is not None:
                        event_key=(source.incident_id,source.revision)
                    elif type(source.value) in (c2.InputEvidence,ObservationFact):
                        event_key=('',0)
                    else:
                        raise ValueError('trend_global_row_scope')
                else:
                    raise ValueError('trend_unsupported_input_row')
                blob=payload.encode();budget.charge(blob);batch_bytes+=len(blob)
                if batch_bytes>limits.max_batch_bytes:raise ValueError('trend_batch_bytes')
                if event_key is not None:
                    db.execute('INSERT INTO rows VALUES (?,?,?,?,?)',(*event_key,input_count,payload,len(blob)))
                input_hash.update(len(blob).to_bytes(8,'big'));input_hash.update(blob);input_count+=1
        if end is None or end.binding!=request.country or end.rows!=input_count or end.complete is not True or component_end is None:
            raise ValueError('trend_input_not_complete')
        db.commit()
        event_count=db.execute('SELECT COUNT(*) FROM (SELECT event,revision FROM rows WHERE revision>0 GROUP BY event,revision)').fetchone()[0]
        if component_end.event_count!=event_count:raise ValueError('trend_event_count')
        if any(db.execute('SELECT 1 FROM rows WHERE event=? AND revision=? LIMIT 1',w.event).fetchone() is None for w in (*request.activities,*request.references)):
            raise ValueError('trend_context_event_mismatch')
        result_id='fixture_trend_'+digest((VERSION,request,input_hash.hexdigest()))
        for sequence,payload in db.execute('SELECT sequence,payload FROM rows WHERE event=? AND revision=? ORDER BY sequence',('',0)):
            output=replace(row('source_evidence',(),(sequence,),raw_typed=payload),result_id=result_id)
            blob=packed(output);budget.charge(blob)
            output_hash.update(len(blob).to_bytes(8,'big'));output_hash.update(blob);output_count+=1
            yield output
        for event,revision,size in db.execute('SELECT event,revision,SUM(bytes) FROM rows WHERE revision>0 GROUP BY event,revision ORDER BY event,revision'):
            if size>limits.max_event_bytes:raise ValueError('trend_event_memory_budget')
            values=[decode(payload)[2] for payload, in db.execute('SELECT payload FROM rows WHERE event=? AND revision=? ORDER BY sequence',(event,revision))]
            budget.check()
            for result in event_rows((event,revision),values,request,limits):
                for output in chain((result,),graph_rows(result)):
                    output=replace(output,result_id=result_id)
                    blob=packed(output);budget.charge(blob)
                    output_hash.update(len(blob).to_bytes(8,'big'));output_hash.update(blob);output_count+=1
                    yield output
            del values
        budget.check()
        completion=TrendCompletion(result_id,input_count,output_count,input_hash.hexdigest(),output_hash.hexdigest(),event_count,time.monotonic()-started,rss(),budget.disk_peak)
    except BaseException as error:
        primary=error
        raise
    finally:
        cleanup_errors=[]
        # 每个已取得句柄都尝试释放；即使前一close失败也继续。
        for close in (getattr(db,'close',None),getattr(temporary,'cleanup',None),getattr(iterator,'close',None)):
            if close is None:
                continue
            try:
                close()
            except BaseException as error:
                cleanup_errors.append(error)
        if cleanup_errors:
            if primary is not None and not isinstance(primary,GeneratorExit):
                primary.cleanup_errors=tuple(getattr(primary,'cleanup_errors',()))+tuple(cleanup_errors)
            else:
                # GeneratorExit会被generator.close吞掉；实际清理失败必须显式报告。
                failure=cleanup_errors[0]
                failure.cleanup_errors=tuple(cleanup_errors)
                if primary is not None:
                    failure.interrupted_by=primary
                raise failure from primary
    if completion is not None:
        yield replace(completion,elapsed_seconds=time.monotonic()-started,process_peak_rss_bytes=rss())
