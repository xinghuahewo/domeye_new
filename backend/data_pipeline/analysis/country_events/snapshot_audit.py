"""C3临时关系索引及全量封存审计；不重跑计算，不把缺口当损坏放行。"""
import hashlib
from data_pipeline.analysis.country_events import event_aggregation as c2, compute
from data_pipeline.analysis.country_events.incremental_types import DirectionPoint, FirstQualifiedRef
from data_pipeline.analysis.country_events.models import Endpoint, Incident
from data_pipeline.analysis.country_events.saved_input import SavedBatch, InputCompletion, CountryRevision, object_scope
from data_pipeline.bgp.archive.message_reader import MessageBatch
from data_pipeline.analysis.country_events.snapshot_schema import encode, decode, row_decode


def initialize(db):
    db.executescript('''
    CREATE TABLE events(incident TEXT PRIMARY KEY,revision TEXT,cohort TEXT UNIQUE,state TEXT,country TEXT,sequence INTEGER,details TEXT);
    CREATE TABLE revisions(incident TEXT,revision TEXT,body_sha TEXT,selected TEXT,PRIMARY KEY(incident,revision));
    CREATE TABLE source_revisions(incident TEXT,revision TEXT,body_sha TEXT,PRIMARY KEY(incident,revision));
    CREATE TABLE objects(object_id TEXT PRIMARY KEY,prefix TEXT,endpoint TEXT,path_id TEXT);
    CREATE TABLE fixed_directions(cohort TEXT,prefix TEXT,endpoint TEXT,PRIMARY KEY(cohort,prefix,endpoint));
    CREATE TABLE qualifications(cohort TEXT,prefix TEXT,reference TEXT,PRIMARY KEY(cohort,prefix));
    CREATE TABLE state_objects(reference TEXT,object_id TEXT,PRIMARY KEY(reference,object_id));
    CREATE TABLE facts(reference TEXT PRIMARY KEY,object_id TEXT,path TEXT,peer TEXT,mapping TEXT,details TEXT);
    CREATE INDEX fact_object ON facts(object_id);
    CREATE TABLE source_refs(kind TEXT,reference TEXT,PRIMARY KEY(kind,reference));
    CREATE TABLE source_facts(reference TEXT PRIMARY KEY,details TEXT);
    CREATE TABLE reference_rows(reference TEXT PRIMARY KEY,details TEXT);
    ''')


def index_output(db,item,sequence):
    if not isinstance(item,c2.C2Row):return
    value=item.value
    if isinstance(value,c2.EventStatus):
        if (item.incident_id,item.revision)!=(value.incident.incident_id,value.incident.revision):raise ValueError('event_identity_mismatch')
        db.execute('INSERT INTO events VALUES (?,?,?,?,?,?,?)',(item.incident_id,str(item.revision),value.cohort_id,value.state,value.incident.country,sequence,encode(value)))
    elif isinstance(value,c2.InputEvidence) and value.kind=='event_revision':
        original=value.original
        if (original.incident_id,original.revision)!=(item.incident_id,item.revision):raise ValueError('revision_identity_mismatch')
        if value.reference!=f'{item.incident_id}/revision/{item.revision}':raise ValueError('C3_revision_reference_mismatch')
        db.execute('INSERT INTO revisions VALUES (?,?,?,?)',(item.incident_id,str(item.revision),body_sha(original),encode(selected_incident(original))))
    elif isinstance(value,c2.CohortMember):
        db.execute('INSERT OR IGNORE INTO fixed_directions VALUES (?,?,?)',(value.cohort_id,value.route.prefix,encode(value.route.endpoint)))
    elif isinstance(value,FirstQualifiedRef):
        db.execute('INSERT INTO qualifications VALUES (?,?,?)',(value.cohort_id,value.prefix,value.first_qualified_ref))
    elif isinstance(value,compute.ObservationFact):
        if item.incident_id is not None:raise ValueError('fact_must_be_global')
        route=value.route
        identity=(route.prefix,encode(route.endpoint),encode(route.path_id))
        prior=db.execute('SELECT prefix,endpoint,path_id FROM objects WHERE object_id=?',(route.object_id,)).fetchone()
        if prior is not None and prior!=identity:raise ValueError('C3_object_identity_mismatch')
        db.execute('INSERT OR IGNORE INTO objects VALUES (?,?,?,?)',(route.object_id,*identity))
        if value.reference!=route.observation_ref:raise ValueError('fact_reference_mismatch')
        db.execute('INSERT INTO facts VALUES (?,?,?,?,?,?)',(value.reference,route.object_id,route.path_ref,route.peer_ref,route.mapping_ref,encode(route)))


def body_sha(value):return hashlib.sha256(encode(value).encode()).hexdigest()


def selected_incident(value):
    # 固定C2 _incidents的字段投影；不重算Detection或事件，不覆盖原正文。
    if isinstance(value,CountryRevision):
        return Incident(value.incident_id,value.revision,value.country,value.trigger_time,value.onset,None,
                        value.original['attributes'].get('legacy_ref'),None,value.carry_in_state,value.end)
    if type(value) is Incident:return value
    raise ValueError('C3_revision_body_type')


def source_audit(db,c1,expected,guard,stats):
    """一次C1固定流复核并建立原引用索引，成本独立于事件数。"""
    last=None;before=db.total_changes
    for item in c1.stream():
        guard();stats['source_audit_items']+=1
        if isinstance(item,MessageBatch):
            for m in item.messages:db.execute('INSERT OR IGNORE INTO source_refs VALUES (?,?)',('state',m['message_id']))
            for r in item.elements:
                db.execute('INSERT OR IGNORE INTO source_refs VALUES (?,?)',('observation',r['event_id']))
        elif isinstance(item,SavedBatch):
            for r in item.rows:
                guard()
                if item.table=='paths':db.execute('INSERT OR IGNORE INTO source_refs VALUES (?,?)',('path',r['path_key']))
                elif item.table=='invalidations':
                    db.execute('INSERT INTO state_objects VALUES (?,?)',(r.message_ref,r.object_key))
                elif item.table=='references':
                    ref=f'{c1.reader.run_id}:{c1.reader.snapshot}/references/{r["source_id"]}/{r["row"]}'
                    db.execute('INSERT INTO reference_rows VALUES (?,?)',(ref,encode(r)))
                elif item.table=='changes':
                    scope=object_scope(r.original)
                    canonical=(Endpoint(*scope[:8]),scope[8],scope[10],r.after_presence,r.after_path,r.after_origin,r.at,r.cursor,r.original['fact_after_presence'])
                    db.execute('INSERT INTO source_facts VALUES (?,?)',(r.event_ref,encode(canonical)))
        elif isinstance(item,CountryRevision):
            db.execute('INSERT INTO source_revisions VALUES (?,?,?)',(item.incident_id,str(item.revision),body_sha(item)))
            stats['source_revision_rows_checked']+=1
        elif isinstance(item,InputCompletion):last=item
    stats['source_audit_sqlite_changes']=db.total_changes-before
    if last!=expected:raise ValueError('C3_source_completion_drift')
    for left,right in (('source_revisions','revisions'),('revisions','source_revisions')):
        if db.execute(f'SELECT 1 FROM {left} a LEFT JOIN {right} b USING(incident,revision) WHERE b.incident IS NULL OR a.body_sha!=b.body_sha LIMIT 1').fetchone():raise ValueError('C3_revision_source_body_mismatch')


def require_row(db,sql,args,error):
    row=db.execute(sql,args).fetchone()
    if row is None:raise ValueError(error)
    return row


def validate(db,completion,c1,guard,stats,*,source_checks=True,m3_source=None,row_decoder=row_decode):
    events=db.execute('SELECT count(*) FROM events').fetchone()[0]
    unavailable=db.execute("SELECT count(*) FROM events WHERE state='unavailable'").fetchone()[0]
    if events!=completion.event_count or unavailable!=completion.unavailable_events:raise ValueError('C3_event_count_mismatch')
    context=None
    if c1 is not None:
        context=dict(run_id=c1.reader.run_id,snapshot=str(c1.reader.snapshot),collector=c1.reader.manifest['collector'],sources=tuple(c1.sources))
    if completion.input_kind=='saved_C1':
        if completion.input_completion is None or (source_checks and c1 is None):raise ValueError('C3_missing_formal_binding')
        if source_checks:source_audit(db,c1,completion.input_completion,guard,stats)
        interpretation=completion.reference_interpretation
        if not isinstance(interpretation,dict):raise ValueError('C3_missing_reference_interpretation')
        if c1 is not None:
            from data_pipeline.analysis.detection.reference_view import ReferenceView
            source=c1.identity['reference_sources']['as_info']
            if (interpretation.get('selection_rule'),interpretation.get('reference_version'),interpretation.get('snapshot_ref'),interpretation.get('source_id'),interpretation.get('original_rows'))!=(ReferenceView.version_rule,c1.identity['reference_version'],f'{c1.reader.run_id}:{c1.reader.snapshot}',source['source_id'],source['rows']):raise ValueError('C3_reference_interpretation_mismatch')
        if events==0 and completion.input_completion.enumeration!='complete_empty':raise ValueError('C3_empty_not_proven')
    elif completion.input_kind=='saved_M3':
        if completion.input_completion is not None or c1 is not None or source_checks and m3_source is None:
            raise ValueError('C3_missing_M3_formal_binding')
        interpretation=completion.reference_interpretation
        if not isinstance(interpretation,dict):raise ValueError('C3_missing_reference_interpretation')
        if m3_source is not None:
            from data_pipeline.analysis.country_events.source_audit import audit_source
            context=audit_source(db,m3_source,completion,guard,stats)
    elif completion.input_kind!='typed_fixture' or c1 is not None:raise ValueError('C3_input_kind_mismatch')
    if db.execute('SELECT 1 FROM revisions r LEFT JOIN events e ON r.incident=e.incident WHERE e.incident IS NULL LIMIT 1').fetchone():raise ValueError('C3_unselected_original_event')
    for incident,revision,cohort,state,_,_,detail in db.execute('SELECT * FROM events'):
        guard();event=decode(detail)
        highest=db.execute('SELECT revision FROM revisions WHERE incident=? ORDER BY length(revision) DESC,revision DESC LIMIT 1',(incident,)).fetchone()
        if highest!=(revision,):raise ValueError('C3_revision_selection_mismatch')
        selected=db.execute('SELECT selected FROM revisions WHERE incident=? AND revision=?',(incident,revision)).fetchone()[0]
        if encode(event.incident)!=selected:raise ValueError('C3_revision_body_selected_mismatch')
        refs=tuple(f'{incident}/revision/{r[0]}' for r in db.execute('SELECT revision FROM revisions WHERE incident=? ORDER BY length(revision),revision',(incident,)))
        if event.revision_refs!=refs:raise ValueError('C3_original_revision_refs_mismatch')
        if state not in ('available','partial','unavailable'):raise ValueError('C3_event_state')
        if state=='unavailable':
            if event.baseline is not None or cohort is not None:raise ValueError('C3_unavailable_baseline')
        elif event.baseline is None or cohort is None:raise ValueError('C3_missing_baseline')
        if completion.input_kind in ('saved_C1','saved_M3'):
            for asn,reference,locator in event.reference_selections:
                expected=f'{interpretation["snapshot_ref"]}/references/{interpretation["source_id"]}/{locator["row"]}'
                if reference!=expected or locator.get('source_id')!=interpretation['source_id'] or locator.get('snapshot_ref')!=interpretation['snapshot_ref']:raise ValueError('C3_reference_locator_mismatch')
                if context is not None:
                    source_row=decode(require_row(db,'SELECT details FROM reference_rows WHERE reference=?',(reference,),'C3_missing_reference_row')[0])
                    if any(source_row.get(k)!=v for k,v in locator.items() if k not in ('snapshot_ref',)):raise ValueError('C3_reference_locator_mismatch')
        if event.baseline is not None and context is not None:
            b=event.baseline.binding
            if (b.run_id,b.snapshot_id,b.collector,b.ordered_sources)!=(context['run_id'],context['snapshot'],context['collector'],context['sources']):
                raise ValueError('C3_baseline_scope_mismatch')
    for table,payload in db.execute('SELECT table_name,payload FROM rows ORDER BY sequence'):
        guard();item=row_decoder(table,decode(payload));stats['relation_rows_checked']+=1
        if completion.input_kind=='saved_M3':
            from data_pipeline.analysis.country_events.qualified_schema import NEW_TABLES
            if table in NEW_TABLES:continue  # 三个新表另经M3逐维源与关系审计，不套旧事件行规则。
        if isinstance(item,c2.C2Completion):continue
        value=item.value
        original_revision=isinstance(value,c2.InputEvidence) and value.kind=='event_revision'
        if item.incident_id is not None:
            if not original_revision:
                event=require_row(db,'SELECT revision,cohort FROM events WHERE incident=?',(item.incident_id,),'C3_orphan_event')
                if event[0]!=str(item.revision):raise ValueError('C3_wrong_revision')
                if hasattr(value,'cohort_id') and value.cohort_id!=event[1]:raise ValueError('C3_cohort_mismatch')
        elif not isinstance(value,(compute.ObservationFact,c2.InputEvidence)):raise ValueError('C3_missing_outer_event')
        if hasattr(value,'sample_us') and value.sample_us is not None:
            if not completion.scope[0]<value.sample_us<=completion.scope[1]:raise ValueError('C3_sample_scope_mismatch')
        if isinstance(value,compute.Completion) and value.input_window!=completion.scope:raise ValueError('C3_window_mismatch')
        route=value.route if isinstance(value,(c2.CohortMember,compute.ObservationFact)) else None
        if route is not None:
            fact=require_row(db,'SELECT details FROM facts WHERE reference=?',(route.observation_ref,),'C3_orphan_observation')
            original=decode(fact[0])
            if (route.object_id,route.endpoint,route.prefix,route.path_id)!=(original.object_id,original.endpoint,original.prefix,original.path_id):raise ValueError('C3_object_identity_mismatch')
            if context is not None:
                require_row(db,"SELECT 1 FROM source_refs WHERE kind='observation' AND reference=?",(route.observation_ref,),'C3_bad_source_reference')
                if route.endpoint.collector!=context['collector']:raise ValueError('C3_fact_scope_mismatch')
                if route.path_ref is not None:require_row(db,"SELECT 1 FROM source_refs WHERE kind='path' AND reference=?",(route.path_ref,),'C3_bad_path_reference')
                if isinstance(value,compute.ObservationFact) and not route.local_message:
                    canonical=require_row(db,'SELECT details FROM source_facts WHERE reference=?',(route.observation_ref,),'C3_bad_source_fact')
                    actual=(route.endpoint,route.prefix,route.path_id,route.presence,route.path_ref,route.origin,route.observed_at,route.cursor,route.fact_presence)
                    if decode(canonical[0])!=actual:raise ValueError('C3_source_fact_mismatch')
        if isinstance(value,compute.PathSample):
            fact=require_row(db,'SELECT path,peer,mapping FROM facts WHERE reference=?',(value.observation_ref,),'C3_orphan_path_observation')
            if fact!=(value.path_ref,value.peer_ref,value.mapping_ref):raise ValueError('C3_path_evidence_mismatch')
        validate_relations(db,value,context is not None,guard,stats)
    for table in ('objects','fixed_directions','qualifications','state_objects','revisions','source_revisions'):
        stats['index_'+table+'_rows']=db.execute('SELECT count(*) FROM '+table).fetchone()[0]
    return 'known_empty' if not events else 'unavailable' if unavailable==events else 'partial' if db.execute("SELECT 1 FROM events WHERE state!='available'").fetchone() else 'available'


def validate_relations(db,value,source_checks,guard,stats):
    """核对引用所属范围，不把跨时点同一对象的状态重算成当前态。"""
    prefix=getattr(value,'prefix',None);cohort=getattr(value,'cohort_id',None)
    endpoint=getattr(value,'endpoint',None)
    fixed=isinstance(value,(DirectionPoint,compute.PrefixPoint,compute.PathSample,compute.NewDirectionPoint))
    if fixed:
        args=(cohort,prefix);where='cohort=? AND prefix=?'
        if isinstance(value,DirectionPoint):where+=' AND endpoint=?';args+= (encode(endpoint),)
        require_row(db,'SELECT 1 FROM fixed_directions WHERE '+where,args,'C3_object_cohort_scope_mismatch')
    if isinstance(value,compute.NewDirectionPoint):
        if db.execute('SELECT 1 FROM fixed_directions WHERE cohort=? AND prefix=? AND endpoint=?',(cohort,prefix,encode(endpoint))).fetchone():raise ValueError('C3_new_direction_is_fixed')
    if isinstance(value,(compute.NewPrefixPoint,FirstQualifiedRef)):
        if db.execute('SELECT 1 FROM fixed_directions WHERE cohort=? AND prefix=?',(cohort,prefix)).fetchone():raise ValueError('C3_new_prefix_is_fixed')
        expected=value.first_observation_ref if isinstance(value,compute.NewPrefixPoint) else value.first_qualified_ref
        if require_row(db,'SELECT reference FROM qualifications WHERE cohort=? AND prefix=?',(cohort,prefix),'C3_missing_qualification')!=(expected,):raise ValueError('C3_qualification_reference_mismatch')
    for ref in getattr(value,'object_refs',()):
        guard();stats['object_relation_lookups']+=1
        row=require_row(db,'SELECT prefix,endpoint FROM objects WHERE object_id=?',(ref,),'C3_orphan_object')
        if row!=(prefix,encode(endpoint)):raise ValueError('C3_object_scope_mismatch')

    def observation(ref,*,evidence=False):
        guard();stats['observation_relation_lookups']+=1
        row=db.execute('SELECT details FROM facts WHERE reference=?',(ref,)).fetchone()
        if row is None:
            if not evidence:raise ValueError('C3_orphan_observation')
            # typed fixture没有保存原STATE目标；正式封存用同一次C1审计的目标索引核对。
            # Reader承接封存源审计，不重扫上游；文件/行摘要保护这些原引用。
            if source_checks:
                args=(ref,prefix,cohort)
                query='''SELECT 1 FROM state_objects s JOIN objects o USING(object_id)
                    JOIN fixed_directions f ON f.prefix=o.prefix AND f.endpoint=o.endpoint
                    WHERE s.reference=? AND o.prefix=? AND f.cohort=?'''
                if isinstance(value,DirectionPoint):query+=' AND o.endpoint=?';args+=(encode(endpoint),)
                require_row(db,query+' LIMIT 1',args,'C3_state_object_scope_mismatch')
            return None
        route=decode(row[0])
        if prefix is not None and route.prefix!=prefix:raise ValueError('C3_observation_prefix_scope_mismatch')
        if endpoint is not None and route.endpoint!=endpoint:raise ValueError('C3_observation_endpoint_scope_mismatch')
        if fixed:
            require_row(db,'SELECT 1 FROM fixed_directions WHERE cohort=? AND prefix=?'+(' AND endpoint=?' if not isinstance(value,compute.NewDirectionPoint) else ''),
                        (cohort,prefix,encode(route.endpoint)) if not isinstance(value,compute.NewDirectionPoint) else (cohort,prefix),'C3_observation_cohort_scope_mismatch')
        if isinstance(value,DirectionPoint) and route.object_id not in value.object_refs:raise ValueError('C3_direction_evidence_object_mismatch')
        return route

    for attr in ('first_qualified_ref','first_observation_ref','observation_ref'):
        ref=getattr(value,attr,None)
        if ref is None:continue
        route=observation(ref)
        if hasattr(value,'observed_at') and value.observed_at!=route.observed_at:raise ValueError('C3_observation_time_mismatch')
        if isinstance(value,compute.NewDirectionPoint) and value.mapping_state!=route.mapping_state:raise ValueError('C3_observation_mapping_mismatch')
        if isinstance(value,FirstQualifiedRef):
            if (value.first_qualified_us,value.first_qualified_cursor)!=(route.observed_at.lower_us,route.cursor):raise ValueError('C3_qualification_position_mismatch')
            if source_checks and value.country_reference is not None:
                require_row(db,'SELECT 1 FROM reference_rows WHERE reference=?',(value.country_reference,),'C3_missing_reference_row')
        if isinstance(value,compute.NewPrefixPoint) and value.first_observed_us!=route.observed_at.lower_us:raise ValueError('C3_qualification_position_mismatch')
    for asn,ref in getattr(value,'origin_refs',()):
        route=observation(ref)
        if route.origin!=asn:raise ValueError('C3_origin_reference_mismatch')
    for attr in ('evidence_refs','state_refs'):
        for ref in getattr(value,attr,()):observation(ref,evidence=True)
