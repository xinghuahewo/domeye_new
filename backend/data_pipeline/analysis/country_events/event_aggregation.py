"""C2：先完整暂存C1，再一次全局保存态扫描；事件只保留相关成员与归约状态。"""
from dataclasses import dataclass, replace
from collections import defaultdict
from fractions import Fraction
from pathlib import Path
import resource
import sys
import tempfile
import time

from data_pipeline.analysis.country_events.models import Baseline, Binding, Incident, Time
from data_pipeline.analysis.country_events.cohort import freeze_cohorts, digest
from data_pipeline.analysis.country_events.compute import compute_country_enhancement, ObservationFact, PrefixPoint, AsnPoint, MetricPoint, WindowClass, Peak, Completion, Batch
from data_pipeline.analysis.country_events.incremental_types import InputRequest, DirectionPoint, FirstQualifiedRef, PathPeakQuality, AsnMetricPeak
from data_pipeline.analysis.country_events.aggregation_staging import Stage, dump, load
from data_pipeline.analysis.country_events.saved_input import CountryRevision


@dataclass(frozen=True)
class C2Row:
    incident_id: object
    revision: object
    value: object


@dataclass(frozen=True)
class EventStatus:
    incident: Incident
    revision_refs: tuple
    baseline: object
    cohort_id: object
    state: str
    reasons: tuple
    direction_count: object = None
    fixed_prefix_count: object = None
    reference_selections: tuple = ()


@dataclass(frozen=True)
class CohortMember:
    cohort_id: str
    route: object


@dataclass(frozen=True)
class BoundaryUnavailable:
    incident_id: str
    sample_us: object
    boundary_us: int
    affected_cursor_range: tuple
    reason: str = 'source_time_boundary_not_locatable'


@dataclass(frozen=True)
class InputEvidence:
    kind: str
    reference: str
    original: object


@dataclass(frozen=True)
class C2Completion:
    input_kind: str
    input_completion: object
    event_count: int
    unavailable_events: int
    costs: dict
    scope: tuple
    reference_interpretation: object = None


@dataclass(frozen=True)
class SparseGrid:
    input_start_us: int
    input_end_us: int
    samples_us: tuple


METRICS = (
 ('normal_prefix_count','prefix_count'),('partially_interrupted_prefix_count','prefix_count'),
 ('completely_interrupted_prefix_count','prefix_count'),('interrupted_prefix_count','prefix_count'),
 ('invisible_direction_count','endpoint_direction_count'),('visible_direction_count','endpoint_direction_count'),
 ('normal_asn_count','asn_count'),('affected_asn_count','asn_count'),('route_interrupted_asn_count','asn_count'),
 ('fixed_visible_ipv4_address_count','ipv4_address_count'),('fixed_visible_ipv6_slash48_equivalent','ipv6_slash48_equivalent'),
 ('new_visible_ipv4_address_count','ipv4_address_count'),('new_cumulative_ipv4_address_count','ipv4_address_count'),
 ('new_visible_ipv4_prefix_count','prefix_count'),('new_cumulative_ipv4_prefix_count','prefix_count'),
 ('new_visible_ipv6_slash48_equivalent','ipv6_slash48_equivalent'),('new_cumulative_ipv6_slash48_equivalent','ipv6_slash48_equivalent'),
 ('new_visible_ipv6_prefix_count','prefix_count'),('new_cumulative_ipv6_prefix_count','prefix_count'))


class EventDriver:
    def __init__(self,stage,cohort,context,grid,unsafe,limits,guard,reserve_member):
        self.stage,self.cohort,self.unsafe=stage,cohort,unsafe
        self.guard=guard;self.prefix_points={};self.asn_peaks={}
        self.samples=tuple(sorted((*grid.samples_us,*unsafe)))
        self.member_count=0  # 只由compute.current真正插入新对象时计入。
        self.interest=set(cohort.prefixes)|{r.prefix for r in context}
        self.gen=compute_country_enhancement((cohort,),stage.binding,grid,(),(),
            batch_rows=limits['batch_rows'],batch_bytes=limits['batch_bytes'],max_objects=limits['max_objects'],
            max_relations=limits['max_relations'],max_changes=limits['max_changes'],guard=guard,
            _incremental=True,_shared_paths=stage.paths,_shared_refs=stage.refs,_context_routes=context,_initial_global_gap=stage.global_gap,
            _before_current_insert=lambda:reserve_member(self),_change_refs_factory=stage.change_references)

    def rows(self,item):
        if isinstance(item,InputRequest):return
        if not isinstance(item,Batch):raise ValueError('C2_driver_protocol')
        for row in item.rows:
            self.guard()
            if isinstance(row,ObservationFact):continue  # A从原保存行单独保留，不能以失效后的基线B冒充原A。
            if isinstance(row,Completion) and self.unsafe:
                row=replace(row,sample_count=len(self.samples),data_through_us=max(self.samples),sample_window=(min(self.samples),max(self.samples)))
            if isinstance(row,PrefixPoint):self.prefix_points[row.prefix]=row
            if isinstance(row,AsnPoint):self._asn_peak(row)
            if isinstance(row,(WindowClass,Peak)):
                row=replace(row,unknown_slots=row.unknown_slots+len(self.unsafe))
            if isinstance(row,PathPeakQuality) and self.unsafe:
                row=replace(row,unknown_slots=row.unknown_slots+len(self.unsafe),exact_prefix_peak=None,exact_ipv4_peak=None,exact_ipv6_peak=None)
            yield row

    def _asn_peak(self,row):
        points=[self.prefix_points[p] for p in row.prefix_refs]
        unknown=row.state=='unknown' or any(p.expected is None for p in points)
        for name,value in (('partial_prefix_count',row.partial),('complete_prefix_count',row.complete),('invisible_direction_count',sum(p.absent for p in points))):
            key=(row.asn,row.afi,name);ledger=self.asn_peaks.setdefault(key,[None,None,0,0,0])
            if unknown:ledger[4]+=1;continue
            ledger[3]+=1
            if ledger[0] is None or value>ledger[0]:ledger[:3]=[value,row.sample_us,1]
            elif value==ledger[0]:ledger[2]+=1

    def advance(self,change='prime'):
        self.guard()
        try:item=next(self.gen) if change=='prime' else self.gen.send(change)
        except StopIteration:return
        while not isinstance(item,InputRequest):
            yield from self.rows(item)
            try:item=next(self.gen)
            except StopIteration:return

    def unknown_rows(self,sample):
        c=self.cohort
        directions=defaultdict(set)
        for r in c.routes:
            if r.presence=='present' and r.mapping_state=='bound':directions[r.prefix].add(r.endpoint)
        for p in c.prefixes:
            for endpoint in sorted(directions[p],key=repr):
                yield DirectionPoint(c.cohort_id,sample,p,endpoint,'unknown',(),())
            yield PrefixPoint(c.cohort_id,sample,p,'unknown',0,0,len(directions[p]),None if c.direction_count is None else len(directions[p]),())
        origins=defaultdict(set)
        for prefix,asn,_ in c.origins:
            if asn is None:continue
            origins[asn,0].add(prefix);origins[asn,1 if ':' not in prefix else 2].add(prefix)
        for (asn,afi),prefixes in sorted(origins.items()):
            yield AsnPoint(c.cohort_id,sample,asn,afi,'unknown',0,0,0,len(prefixes),tuple(sorted(prefixes)))
        for metric,unit in METRICS:
            yield MetricPoint(c.cohort_id,sample,metric,unit,None,Fraction(0),None,None,'unknown')

    def finalize_peaks(self):
        c=self.cohort
        # 即使所有采样均不可定位，也为全部固定ASN保留未知峰值。
        pairs={(asn,afi) for prefix,asn,_ in c.origins if asn is not None for afi in (0,1 if ':' not in prefix else 2)}
        for asn,afi in sorted(pairs):
            for metric in ('partial_prefix_count','complete_prefix_count','invisible_direction_count'):
                value,first,count,known,unknown=self.asn_peaks.get((asn,afi,metric),[None,None,0,0,0])
                unknown+=len(self.unsafe)
                yield AsnMetricPeak(c.cohort_id,asn,afi,metric,value,None if unknown else value,first,count,known,unknown)


def _incidents(stage):
    selected=[]
    for incident,revision,payload in stage.db.execute('SELECT r.incident,r.revision,r.payload FROM revisions r JOIN (SELECT incident,max(revision) AS revision FROM revisions GROUP BY incident) s USING(incident,revision) ORDER BY incident'):
        value=load(payload)
        if isinstance(value,CountryRevision):
            event=Incident(value.incident_id,value.revision,value.country,value.trigger_time,value.onset,None,
                           value.original['attributes'].get('legacy_ref'),None,value.carry_in_state,value.end)
        else:event=value
        refs=tuple(f'{incident}/revision/{r[0]}' for r in stage.db.execute('SELECT revision FROM revisions WHERE incident=? ORDER BY revision',(incident,)))
        if len(selected)>=stage.limits["max_events"]:raise ValueError("resource_limit:C2_events")
        selected.append((event,refs))
    return selected


def calculate(stage,grid,limits,guard):
    incidents=_incidents(stage)
    if len(incidents)>limits['max_events']:raise ValueError('resource_limit:C2_events')
    schedule=defaultdict(list);closing=defaultdict(list);active={};subscriptions=defaultdict(set);by_country=defaultdict(set)
    unavailable=0;members=0

    def reserve_member(driver):
        nonlocal members
        guard()
        stage.counts['active_member_limit_checks']+=1
        if members>=limits['max_total_members']:
            raise ValueError('resource_limit:C2_active_members')
        members+=1;driver.member_count+=1
        stage.counts['active_member_reservations']+=1
        stage.counts['peak_active_members']=max(stage.counts['peak_active_members'],members)
    for event,refs in incidents:
        boundary=(event.onset or event.detected_at).lower_us
        low,high=stage.boundary(boundary)
        if event.onset_cursor is not None:
            cur=event.onset_cursor
            if not stage.db.execute("SELECT 1 FROM units WHERE rank=? AND record=? AND ordinal=?",(cur.source_rank,cur.record,cur.ordinal)).fetchone():raise ValueError("onset_cursor_not_in_fixed_input")
            found=stage.db.execute('SELECT i FROM units WHERE (rank,record,phase,ordinal)<(?,?,1,?) ORDER BY rank DESC,record DESC,phase DESC,ordinal DESC LIMIT 1',(cur.source_rank,cur.record,cur.ordinal)).fetchone()
            low=high=found[0] if found else -1
        schedule[low].append((event,refs,high))

    def activate(index):
        nonlocal unavailable
        for event,refs,high in schedule.pop(index,[]):
            guard();anchor=(event.onset or event.detected_at).lower_us
            candidates=[]
            for route in stage.candidates(event.country) if index>=0 else ():
                guard()
                if len(candidates)>=limits["max_objects"]:raise ValueError("resource_limit:C2_members")
                candidates.append(route)
            candidates=tuple(candidates)
            prefixes={r.prefix for r in candidates}
            reason=None
            if stage.initial_end is None or index<stage.initial_end:reason='missing_baseline'
            elif stage.impact(index,high,event.country,prefixes,limits['max_boundary_span'],include_potential=True):reason='baseline_boundary_not_locatable'
            if reason:
                unavailable+=1
                yield C2Row(event.incident_id,event.revision,EventStatus(event,refs,None,None,'unavailable',(reason,)))
                yield C2Row(event.incident_id,event.revision,BoundaryUnavailable(event.incident_id,None,anchor,(index,high),reason))
                continue
            hi=stage.db.execute('SELECT prefix_hi FROM frontiers WHERE i=?',(index,)).fetchone()[0]
            change=load(stage.db.execute('SELECT payload FROM units WHERE hi=? AND i<=? ORDER BY i DESC LIMIT 1',(hi,index)).fetchone()[0])
            cursor_change=load(stage.db.execute('SELECT payload FROM units WHERE i=?',(index,)).fetchone()[0])
            baseline=Baseline(digest([stage.binding.run_id,stage.binding.snapshot_id,index,event.incident_id]),stage.binding,change.at,cursor_change.cursor,stage.binding.state_rule_version)
            if event.onset_cursor is None and baseline.at.upper_us>=anchor:
                unavailable+=1;yield C2Row(event.incident_id,event.revision,EventStatus(event,refs,None,None,'unavailable',('baseline_time_not_locatable',)));continue
            references=tuple(stage.refs[a] for a in sorted({r.origin for r in candidates if r.origin in stage.refs}))
            selections=[]
            if stage.reference_interpretation is not None:
                for ref in references:
                    stage.counts['activation_reference_locator_lookups']+=1
                    locator=stage.db.execute("SELECT payload FROM dictionaries WHERE kind='reference_selection' AND key=?",(str(ref.asn),)).fetchone()
                    if locator is None:raise ValueError('C2_missing_reference_selection')
                    selections.append((ref.asn,ref.source_ref,load(locator[0])))
            cohort=freeze_cohorts((event,),baseline,candidates,references,max_routes=limits['max_objects'],guard=guard)[0]
            future={}
            event_end=min(grid.input_end_us,event.end.lower_us if event.end else grid.input_end_us)
            end_index=stage.boundary(event_end)[1]
            for (prefix,) in stage.db.execute('SELECT prefix FROM country_prefix WHERE country=? AND last_i>? ORDER BY prefix',(event.country,index)):
                guard()
                if prefix in prefixes:continue
                stage.counts['future_prefix_index_probes']+=1
                first=stage.db.execute('SELECT i FROM units WHERE country=? AND prefix=? AND i>? AND i<=? AND lo<? ORDER BY i LIMIT 1',(event.country,prefix,index,end_index,event_end)).fetchone()
                if first is None:continue
                if len(future)>=limits['max_objects']:raise ValueError('resource_limit:C2_future_prefixes')
                future[prefix]=first[0]
            context=[]
            for prefix in sorted(future):
                for r in stage.db.execute('SELECT payload FROM current WHERE prefix=? ORDER BY object_id',(prefix,)):
                    guard()
                    if len(context)+len(candidates)>=limits['max_objects']:raise ValueError('resource_limit:C2_members')
                    context.append(load(r[0]))
            stage.counts['activation_member_rows']+=len(candidates)+len(context)
            if len(active)>=limits['max_active']:raise ValueError('resource_limit:C2_active_members')
            safe=[];unsafe={}
            for sample in grid.samples_us:
                if sample<=anchor or (event.end and sample>event.end.lower_us):continue
                lo,hi=stage.boundary(sample)
                relevant=prefixes|{p for p,i in future.items() if i<=hi}
                if stage.impact(lo,hi,event.country,relevant,limits['max_boundary_span']):unsafe[sample]=(lo,hi)
                else:safe.append(sample)
            driver=EventDriver(stage,cohort,tuple(context),SparseGrid(grid.input_start_us,grid.input_end_us,tuple(safe)),unsafe,limits,guard,reserve_member)
            driver.interest.update(future)
            close_boundary=min(grid.input_end_us,event.end.lower_us if event.end else grid.input_end_us)
            closing[max(index,stage.boundary(close_boundary)[1])].append(event.incident_id)
            active[event.incident_id]=driver;by_country[event.country].add(event.incident_id)
            for prefix in driver.interest:subscriptions[prefix].add(event.incident_id)
            yield C2Row(event.incident_id,event.revision,EventStatus(event,refs,baseline,cohort.cohort_id,'partial' if unsafe or cohort.reasons or stage.global_gap else 'available',cohort.reasons+tuple('sample_boundary_not_locatable' for _ in [0] if unsafe)+tuple('source_gap' for _ in [0] if stage.global_gap),cohort.direction_count,len(cohort.prefixes),tuple(selections)))
            for route in cohort.routes:yield C2Row(event.incident_id,event.revision,CohortMember(cohort.cohort_id,route))
            for sample,span in unsafe.items():
                yield C2Row(event.incident_id,event.revision,BoundaryUnavailable(event.incident_id,sample,sample,span))
                for row in driver.unknown_rows(sample):yield C2Row(event.incident_id,event.revision,row)
            for row in driver.advance():yield C2Row(event.incident_id,event.revision,row)

    def close_events(index):
        nonlocal members
        for incident in sorted(closing.pop(index,())):
            driver=active.pop(incident);event=driver.cohort.incident
            for row in driver.advance(None):yield C2Row(incident,event.revision,row)
            for row in driver.finalize_peaks():yield C2Row(incident,event.revision,row)
            members-=driver.member_count
            stage.counts['released_active_members']+=driver.member_count
            by_country[event.country].discard(incident)
            for prefix in driver.interest:
                subscriptions[prefix].discard(incident)
                if not subscriptions[prefix]:del subscriptions[prefix]
    yield from activate(-1)
    yield from close_events(-1)
    for index,payload in stage.db.execute('SELECT i,payload FROM units ORDER BY i'):
        guard();change=load(payload)
        recipients=set()
        if change.after is not None:
            recipients.update(subscriptions.get(change.after.prefix,()))
            ref=stage.refs.get(change.after.origin)
            if ref and ref.state=='known':recipients.update(by_country.get(ref.country,()))
        elif change.global_invalidation:recipients.update(active)
        else:
            for obj in change.invalidated_objects:
                row=stage.db.execute('SELECT prefix FROM current WHERE object_id=?',(obj,)).fetchone()
                if row:recipients.update(subscriptions.get(row[0],()))
        stage.apply(change)
        for incident in sorted(recipients):
            driver=active[incident];event=driver.cohort.incident
            if event.end and change.at.lower_us>=event.end.lower_us:continue
            stage.counts['related_changes_sent']+=1
            for row in driver.advance(change):yield C2Row(incident,event.revision,row)
        yield from close_events(index)
        yield from activate(index)
        yield from close_events(index)
    if active or members:raise ValueError("C2_unclosed_events")
    for incident,revision,raw in stage.db.execute('SELECT incident,revision,payload FROM revisions ORDER BY incident,revision'):
        yield C2Row(incident,revision,InputEvidence('event_revision',f'{incident}/revision/{revision}',load(raw)))
    for kind,key,raw in stage.db.execute("SELECT kind,key,payload FROM evidence WHERE kind!='as_info' ORDER BY kind,key"):
        yield C2Row(None,None,InputEvidence(kind,key,load(raw)))
    for (payload,) in stage.db.execute('SELECT payload FROM observed_fact ORDER BY reference'):
        guard();route=load(payload)
        if route.observed_at.lower_us<grid.input_end_us:
            yield C2Row(None,None,ObservationFact(route.observation_ref,route,route.local_message))
    yield C2Completion(getattr(stage,'input_kind','saved_C1' if stage.completion is not None else 'typed_fixture'),stage.completion,len(incidents),unavailable,dict(stage.counts),(grid.input_start_us,grid.input_end_us),stage.reference_interpretation)


def run_saved(c1,grid,*,scratch_root,**overrides):
    binding=Binding(c1.reader.run_id,str(c1.reader.snapshot),tuple(c1.sources),c1.identity['reference_version'],c1.identity['decoder_version'],c1.state_rule,c1.reader.manifest['collector'])
    def fill(stage):
        original_guard=c1.external_guard
        def combined_guard():
            original_guard();stage.guard()
            c1_bytes=sum(p.stat().st_size for p in Path(c1.workdir).iterdir() if p.is_file()) if c1.workdir else 0
            stage.counts['c1_peak_scratch_bytes']=max(stage.counts['c1_peak_scratch_bytes'],c1_bytes)
            c2_bytes=sum(p.stat().st_size for p in Path(stage.directory).rglob('*') if p.is_file())
            if c1_bytes+c2_bytes>stage.limits['max_disk_bytes']:raise ValueError('resource_limit:C2_combined_scratch')
        c1.external_guard=combined_guard
        try:stage.consume_c1(c1.stream(),c1.identity['reference_sources']['as_info']['source_id'])
        finally:c1.external_guard=original_guard
    yield from _run(binding,grid,scratch_root,fill,c1._check,overrides)


def run_typed(binding,grid,incidents,changes,paths,references,*,scratch_root,complete=True,**overrides):
    def fill(stage):
        for path in paths:stage.put('path',path.path_ref,path)
        for ref in references:stage.put('reference',ref.asn,ref)
        for event in incidents:stage.db.execute('INSERT INTO revisions VALUES (?,?,?)',(event.incident_id,event.revision,stage.checked(event)))
        for change in changes:stage.add_change(change,phase=0 if change.after is None else 1)
        if not complete:raise ValueError('typed_input_not_complete')
        stage.finish()
    yield from _run(binding,grid,scratch_root,fill,lambda:None,overrides)


def _run(binding,grid,scratch_root,fill,recheck,overrides,*,_stage_factory=Stage):
    limits=dict(batch_rows=256,batch_bytes=4*1024**2,max_objects=100000,max_relations=100000,max_changes=1000000,
                max_events=10000,max_active=128,max_total_members=300000,max_boundary_span=10000,
                max_rss_bytes=2*1024**3,max_disk_bytes=8*1024**3,max_row_bytes=4*1024**2,max_staged_rows=10000000)
    if set(overrides)-set(limits):raise ValueError('unknown_C2_limit')
    limits.update(overrides)
    if any(type(v) is not int or v<1 for v in limits.values()):raise ValueError("invalid_C2_limit")
    with tempfile.TemporaryDirectory(prefix='country-c2-',dir=scratch_root) as directory:
        def guard():
            rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
            size=sum(p.stat().st_size for p in Path(directory).rglob('*') if p.is_file())
            if rss>limits['max_rss_bytes'] or size>limits['max_disk_bytes']:raise ValueError('resource_limit:C2_working_set')
        stage=_stage_factory(Path(directory)/'working.sqlite',binding,guard,limits)
        try:
            started=time.monotonic()
            fill(stage);recheck()
            stage.counts["staging_validation_index_seconds"]=time.monotonic()-started
            scan_start=time.monotonic()
            for row in calculate(stage,grid,limits,guard):
                guard()
                if isinstance(row,C2Completion):
                    recheck()
                    row.costs.update(reduction_output_seconds=time.monotonic()-scan_start,
                        total_seconds=time.monotonic()-started,sqlite_writes=stage.db.total_changes,
                        final_disk_bytes=sum(p.stat().st_size for p in Path(directory).rglob('*') if p.is_file()),
                        process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=="darwin" else 1024))
                if len(repr(row).encode())>limits['batch_bytes']:raise ValueError('resource_limit:C2_output_row')
                stage.counts["output_rows"]+=1
                yield row
        finally:stage.close()
