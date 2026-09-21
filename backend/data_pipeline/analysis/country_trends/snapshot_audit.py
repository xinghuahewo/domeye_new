"""S2逐行关系审计；磁盘索引保留原revision、科学定位与图引用。"""
import hashlib
from data_pipeline.analysis.country_trends.snapshot_schema import encode, decode, row_hash
from data_pipeline.analysis.country_trends.contract import digest
from data_pipeline.analysis.country_trends.compute import GRAPH_KINDS, metric_validate
from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_schema as c3


def initialize(db):
    db.executescript('''CREATE TABLE targets(locator TEXT PRIMARY KEY,kind TEXT,event TEXT,key TEXT,payload TEXT);
    CREATE TABLE events(event TEXT PRIMARY KEY,cohort TEXT,status TEXT);
    CREATE TABLE selected_incidents(incident TEXT PRIMARY KEY,status TEXT);
    CREATE TABLE historical_targets(locator TEXT PRIMARY KEY,incident TEXT,revision TEXT);
    CREATE TABLE links(source TEXT,target TEXT,role TEXT);
    CREATE TABLE nodes(id TEXT PRIMARY KEY,kind TEXT,event TEXT);
    CREATE TABLE contexts(ref TEXT PRIMARY KEY,role TEXT,payload TEXT);
    CREATE INDEX target_event ON targets(kind,event);
    CREATE INDEX link_target ON links(target,role);
    CREATE TABLE raw_targets(locator TEXT PRIMARY KEY,payload TEXT);
    CREATE TABLE raw_order(sequence INTEGER PRIMARY KEY,payload TEXT,source_table TEXT);
    ''')


def locator(r):return encode((r.kind,r.event,r.key))


def append(db,r):
    loc=locator(r);event=encode(r.event);v=dict(r.values)
    db.execute('INSERT INTO targets VALUES (?,?,?,?,?)',(loc,r.kind,event,encode(r.key),encode((r.values,r.refs))))
    if r.kind=='raw_source':
        db.execute('INSERT INTO raw_order VALUES (?,?,?)',(v['source_sequence'],v['raw_typed'],v['source_table']))
        raw=c3.decode(v['raw_typed'])
        if v['source_table']=='c2_completion':
            if type(raw) is not c2.C2Completion or r.event:raise ValueError('trend_raw_completion')
        else:
            incident,revision,value=raw
            if r.event!=(() if incident is None else (incident,revision)) or c3.BY_CLASS.get(type(value))!=v['source_table']:raise ValueError('trend_raw_target')
            if isinstance(value,c2.EventStatus):
                if (value.incident.incident_id,value.incident.revision)!=r.event:raise ValueError('trend_original_revision')
                db.execute('INSERT INTO events VALUES (?,?,?)',(event,value.cohort_id,c3.encode(value)))
                db.execute('INSERT INTO selected_incidents VALUES (?,?)',(incident,c3.encode(value)))
            if isinstance(value,c2.InputEvidence) and value.kind=='event_revision':
                original=value.original
                if (original.incident_id,original.revision)!=(incident,revision) or value.reference!=f'{incident}/revision/{revision}':raise ValueError('trend_historical_revision_identity')
                db.execute('INSERT INTO historical_targets VALUES (?,?,?)',(loc,incident,str(revision)))
            if type(value).__name__=='MetricPoint':
                metric_validate(value)
                db.execute('INSERT INTO raw_targets VALUES (?,?)',(encode(('metric',r.event,(value.metric,value.sample_us))),c3.encode(value)))
    elif r.kind=='context_source':db.execute('INSERT INTO contexts VALUES (?,?,?)',(v['source_ref'],v['role'],v['raw_typed']))
    elif r.kind=='metric':
        db.execute('INSERT INTO links VALUES (?,?,?)',(loc,loc,'raw_metric'))
    elif r.kind=='evidence':
        if v['source_kind'] not in GRAPH_KINDS or v['source_locator']!=(v['source_kind'],r.event,v['source_key']):raise ValueError('trend_evidence_locator')
        db.execute('INSERT INTO links VALUES (?,?,?)',(loc,encode(v['source_locator']),'evidence'))
    elif r.kind=='evidence_source':
        db.execute('INSERT INTO links VALUES (?,?,?)',(loc,encode(v['source_locator']),'source_locator'))
        db.execute('INSERT INTO links VALUES (?,?,?)',(loc,encode(('evidence',r.event,(r.key[0],))),'evidence_owner'))
    elif r.kind=='edge':
        a,relation,b=r.key
        if relation not in ('supported_by','limited_by','unknown_about'):raise ValueError('trend_edge_relation')
        for target,kind in ((a,'claim'),(b,{'supported_by':'evidence','limited_by':'limitation','unknown_about':'unknown'}[relation])):
            db.execute('INSERT INTO links VALUES (?,?,?)',(loc,encode((kind,r.event,(target,))),'edge'))
    if r.kind in ('claim','evidence','unknown','limitation'):
        db.execute('INSERT INTO nodes VALUES (?,?,?)',(r.key[0],r.kind,event))
    if r.kind=='claim' or r.kind=='unknown' and 'evidence_id' in v:
        db.execute('INSERT INTO links VALUES (?,?,?)',(loc,encode(('evidence',r.event,(v['evidence_id'],))),'claim_evidence'))
    if r.kind=='activity_relation':
        for kind,key in (('activity_window',v['window_key']),('point',v['state_point_key'])):
            db.execute('INSERT INTO links VALUES (?,?,?)',(loc,encode((kind,r.event,key)),'activity_relation'))
    if r.kind=='reference_cdf':
        for country in v['contributors']:db.execute('INSERT INTO links VALUES (?,?,?)',(loc,encode(('reference_country',r.event,(country,))),'reference_contributor'))
    if r.kind in ('phase','peak','atomic','threshold_sample'):
        names={'phase':('start_us','end_us'),'peak':('first_us',),'atomic':('sample_us',),'threshold_sample':('sample_us',)}[r.kind]
        for name in names:db.execute('INSERT INTO links VALUES (?,?,?)',(loc,encode(('metric',r.event,(r.key[0],v[name]))),'sample_target'))
    for ref in r.refs:
        if not isinstance(ref,str):raise ValueError('trend_reference_type')
        if ref.startswith('metric:'):
            _,metric,t=ref.split(':');target=encode(('metric',r.event,(metric,int(t))))
        elif ref.startswith('profile:'):target=encode(('profile',r.event,(ref.split(':',1)[1],)))
        else:
            db.execute('INSERT INTO links VALUES (?,?,?)',(loc,ref,'context'));continue
        db.execute('INSERT INTO links VALUES (?,?,?)',(loc,target,'science_ref'))


def validate(db,event_count,input_count,guard):
    results=db.execute("SELECT event,key,payload FROM targets WHERE kind='result'").fetchall()
    if len(results)!=1 or decode(results[0][0])!=() or decode(results[0][1])!=():raise ValueError('trend_result_enumeration')
    result=dict(decode(results[0][2])[0])
    if result['event_count']!=event_count or result['state']!=('complete' if event_count else 'admitted_empty'):raise ValueError('trend_result_state')
    input_digest=hashlib.sha256();completed=0
    for raw,table in db.execute('SELECT payload,source_table FROM raw_order ORDER BY sequence'):
        guard();encoded=raw.encode();input_digest.update(len(encoded).to_bytes(8,'big'));input_digest.update(encoded)
        if table=='c2_completion':
            completed+=1;completion=c3.decode(raw)
            if completion.event_count!=event_count:raise ValueError('trend_original_completion')
    if completed!=1 or input_digest.hexdigest()!=result['input_sha256']:raise ValueError('trend_original_stream_digest')
    receipt=result['country_receipt']
    if receipt['rows']!=input_count or receipt['mode']!='component' or receipt['full_body_validated'] is not True:raise ValueError('trend_original_receipt')
    if db.execute('SELECT count(*) FROM events').fetchone()[0]!=event_count:raise ValueError('trend_event_enumeration')
    if db.execute('SELECT count(*),min(sequence),max(sequence) FROM raw_order').fetchone()!=(input_count,0,input_count-1):raise ValueError('trend_raw_enumeration')
    for event,cohort,status in db.execute('SELECT * FROM events'):
        guard();target=db.execute("SELECT payload FROM targets WHERE kind='event' AND event=?",(event,)).fetchall()
        if len(target)!=1 or dict(decode(target[0][0])[0])['cohort_id']!=cohort:raise ValueError('trend_event_cohort')
    for loc,event in db.execute("SELECT locator,event FROM targets WHERE event!=?",(encode(()),)):
        if db.execute('SELECT 1 FROM events WHERE event=?',(event,)).fetchone() is None:
            history=db.execute('SELECT incident,revision FROM historical_targets WHERE locator=?',(loc,)).fetchone()
            selected=None if history is None else db.execute('SELECT status FROM selected_incidents WHERE incident=?',(history[0],)).fetchone()
            if selected is None or f'{history[0]}/revision/{history[1]}' not in c3.decode(selected[0]).revision_refs:raise ValueError('trend_orphan_event')
    for source,target,role in db.execute('SELECT * FROM links'):
        guard()
        if role=='context':
            context=db.execute('SELECT role,payload FROM contexts WHERE ref=?',(target,)).fetchone()
            if context is None:raise ValueError('trend_context_fk')
            kind,event,payload=db.execute('SELECT kind,event,payload FROM targets WHERE locator=?',(source,)).fetchone()
            values=dict(decode(payload)[0]);original=decode(context[1])
            if kind=='activity_window':
                window=values['raw']
                if context[0]!='feature' or window.event!=decode(event) or window.source_ref!=target or any(getattr(window,k)!=original[k] for k in ('mode','source_rank','source_id','window_role')) or window.value!=original[window.metric]:raise ValueError('trend_activity_original_fk')
            if kind=='reference_country':
                if context[0]!='reference' or original.cohort_id!=values['cohort_id'] or original.definition_binding!=values['definition_binding']:raise ValueError('trend_reference_original_fk')
            continue
        if role=='raw_metric':
            raw=db.execute('SELECT payload FROM raw_targets WHERE locator=?',(target,)).fetchone()
            item=db.execute('SELECT payload FROM targets WHERE locator=?',(source,)).fetchone()
            if raw is None or c3.encode(dict(decode(item[0])[0])['raw'])!=raw[0]:raise ValueError('trend_raw_metric_fk')
            continue
        dest=db.execute('SELECT kind,event,key,payload FROM targets WHERE locator=?',(target,)).fetchone()
        if dest is None:raise ValueError('trend_target_fk:'+role)
        source_event=db.execute('SELECT event FROM targets WHERE locator=?',(source,)).fetchone()[0]
        if source_event!=dest[1]:raise ValueError('trend_cross_event_fk')
        if role=='evidence':
            from data_pipeline.analysis.country_trends.contract import TrendRow
            values,refs=decode(dest[3]);science=TrendRow(dest[0],decode(dest[1]),decode(dest[2]),values,refs)
            src=db.execute('SELECT key,payload FROM targets WHERE locator=?',(source,)).fetchone()
            e=dict(decode(src[1])[0])
            if decode(src[0])!=(digest(science),) or e['values']!=values or e['source_refs']!=refs:raise ValueError('trend_evidence_identity')
    # 每种承诺科学类型必须实际形成Evidence，不能漏图后重算表hash自报成功。
    for kind,event,key,payload in db.execute('SELECT kind,event,key,payload FROM targets'):
        if kind in GRAPH_KINDS:
            target=encode((kind,decode(event),decode(key)))
            if db.execute("SELECT 1 FROM links WHERE target=? AND role='evidence'",(target,)).fetchone() is None:raise ValueError('trend_graph_coverage')
