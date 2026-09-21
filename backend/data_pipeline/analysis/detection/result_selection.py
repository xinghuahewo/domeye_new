"""从既有完整修订、实际终态与独立资格选择结果；不是新事件算法。"""
import json
from datetime import datetime, timezone, timedelta
from data_pipeline.analysis.detection.result_window import identity_windows, instant


def _legacy_time(value):
    if value is None: return None
    try:
        if type(value) is dict and set(value)=={'$datetime'}:
            parsed=datetime.fromisoformat(value['$datetime'])
        else: parsed=datetime.strptime(value,'%Y-%m-%d %H:%M:%S')
        if parsed.tzinfo is None: parsed=parsed.replace(tzinfo=timezone(timedelta(hours=8)))
        return parsed.astimezone(timezone.utc)
    except (ValueError,TypeError): return None


def _members(value):
    if not isinstance(value,dict): return []
    if set(value)=={'$map'}: return [pair[1] for pair in value['$map']]
    return list(value.values())


def selection(row, binding):
    windows=identity_windows(binding['identity'],binding['scope'])
    first=json.loads(row.pop('selection_first'));last=json.loads(row.pop('selection_last'))
    final_q=json.loads(row.pop('selection_final_q'));state_text=row.pop('selection_state')
    state=json.loads(state_text) if state_text is not None else None
    proof=dict(first_sequence=row.pop('selection_first_sequence'),last_sequence=row.pop('selection_last_sequence'),
               revisions=row.pop('selection_revisions'),final_state_ordinal=row.pop('selection_state_ordinal'),
               final_qualification_id=row.pop('selection_final_qid'))
    kind=row['event_kind'];start=_legacy_time(first.get('s_time'));end=_legacy_time(last.get('e_time'))
    result=windows['result_window'];begin,finish=(instant(result[k]) for k in ('window_start','window_end_exclusive'))
    calc_start=instant(windows['calculation_window']['window_start'])
    same_anchor=first.get('s_time')==last.get('s_time')
    active=kind!='leak' and last.get('e_time') is None and last in _members(state)
    clean=final_q['coverage']=='complete'
    anchor_known=start is not None and calc_start<=start<instant(windows['calculation_window']['window_end_exclusive']) and same_anchor
    end_known=end is not None and start is not None and end>=start and clean and kind!='leak'
    if anchor_known and clean and start>=finish: return None
    if end_known and end<=begin: return None
    overlap='possible_unknown'
    if anchor_known and clean and (active or end_known):
        overlap='carry_in' if start<begin else 'started_in_result'
    proof.update(original_start=first.get('s_time'),original_end=last.get('e_time'),
        start_utc=start.isoformat() if start else None,end_utc=end.isoformat() if end else None,
        active_in_final_saved_state=active,selection=overlap,result_window=result,
        calculation_window=windows['calculation_window'],
        anchor_coverage='complete_within_declared_cold_start' if anchor_known and clean else 'unknown',
        end_coverage='recorded' if end_known else 'unknown',
        earlier_history='Unknown',interpretation='observed_heuristic_candidate_lifecycle_not_network_outage')
    q=json.loads(row.pop('selection_q'));q['qualification_id']=row.pop('selection_qid')
    # 每条原修订资格保留；结果选择的不确定性另外陈述，不覆盖原独立维度。
    return dict(raw=row,main=row if overlap!='possible_unknown' and q['coverage']=='complete' else None,
                qualification=q,lifecycle=proof,as_of_position=binding['identity']['qualification_as_of_position'])


def query(binding, scope):
    """聚合只取序号端点；不在Python累计全事件链，也不截掉前史修订。"""
    def table(name): return f"(SELECT * FROM lake.det_{binding['run_id']}.{name} AT (VERSION => {binding['snapshot']}))"
    records,qual,state=table('records'),table('m3_entries'),table('state_entries')
    condition=['r.sequence>=?'];args=[scope['start']]
    if scope['stop'] is not None:condition.append('r.sequence<?');args.append(scope['stop'])
    if scope['key'] is not None:condition.append('r.incident_id=?');args.append(scope['key'])
    sql=f'''WITH revisions AS (SELECT * FROM {records} WHERE record_kind='business_revision'),
        limits AS (SELECT incident_id,min(sequence) AS first_sequence,max(sequence) AS last_sequence,count(*) AS revisions FROM revisions GROUP BY incident_id),
        q AS (SELECT *,row_number() OVER(PARTITION BY incident_id,revision ORDER BY ordinal DESC) AS n FROM {qual} WHERE kind='event_qualification')
        SELECT r.*,f.legacy_json AS selection_first,l.legacy_json AS selection_last,
        limits.first_sequence AS selection_first_sequence,limits.last_sequence AS selection_last_sequence,limits.revisions AS selection_revisions,
        s.value_json AS selection_state,s.ordinal AS selection_state_ordinal,
        q.payload_json AS selection_q,q.entry_id AS selection_qid,fq.payload_json AS selection_final_q,fq.entry_id AS selection_final_qid
        FROM revisions r JOIN limits USING(incident_id) JOIN revisions f ON f.sequence=limits.first_sequence
        JOIN revisions l ON l.sequence=limits.last_sequence
        JOIN q ON q.incident_id=r.incident_id AND q.revision=r.revision AND q.n=1
        JOIN q fq ON fq.incident_id=l.incident_id AND fq.revision=l.revision AND fq.n=1
        LEFT JOIN {state} s ON s.container='entry' AND s.key_json=CAST(json_extract(l.attributes_json,'$.object') AS VARCHAR)
          AND s.family=CASE l.event_kind WHEN 'moas' THEN 'hijack' WHEN 'hijack' THEN 'hijack' WHEN 'sub_hijack' THEN 'subhijack' WHEN 'leak' THEN 'leak' ELSE 'outage' END
          AND s.attribute=CASE l.event_kind WHEN 'moas' THEN 'moas_event_dict' WHEN 'hijack' THEN 'moas_event_dict' WHEN 'sub_hijack' THEN 'sub_hijack_dict' WHEN 'leak' THEN 'phenomenon_dict' ELSE l.event_kind||'_event' END
        WHERE {' AND '.join(condition)} ORDER BY r.sequence'''
    return sql,args
