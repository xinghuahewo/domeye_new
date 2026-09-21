"""M3原21表的完整源引用审计：实际公开原行，不伪造旧C1流或完成证明。"""
import json

from data_pipeline.analysis.country_events import snapshot_audit as old
from data_pipeline.analysis.country_events.snapshot_schema import decode, encode
from data_pipeline.analysis.country_events.qualified_schema import row_decode
from data_pipeline.analysis.country_events.route_change_adapter import saved_change, saved_invalidation, saved_country_revision
from data_pipeline.analysis.country_events.saved_input import object_scope
from data_pipeline.analysis.country_events.models import Endpoint
from data_pipeline.analysis.country_events.event_aggregation import C2Row, InputEvidence
from data_pipeline.bgp.replay.quality_overlay import ScopeIndex, position_key
from data_pipeline.analysis.detection.reference_view import ReferenceView


def audit_source(db, source, completion, guard, stats):
    source.verify_saved()
    b = source.binding
    reference = source.inputs.detection_binding['identity']['reference_sources']['as_info']
    interpretation = completion.reference_interpretation
    if (interpretation.get('selection_rule'), interpretation.get('reference_version'),
            interpretation.get('snapshot_ref'), interpretation.get('source_id'), interpretation.get('original_rows')) != (
            ReferenceView.version_rule, b.reference_version, f'{b.run_id}:{b.snapshot_id}',
            reference['source_id'], reference['rows']):
        raise ValueError('M3 C3参考解释与实际原输入不符')
    # 每条原行必须原样保留且完整；不是仅验证被业务选中的少数引用。
    db.execute('CREATE TEMP TABLE IF NOT EXISTS m3_source_seen(aid TEXT,view TEXT,ordinal INTEGER,PRIMARY KEY(aid,view,ordinal))')
    db.execute('DELETE FROM m3_source_seen');seen_count=0
    for table, payload in db.execute("SELECT table_name,payload FROM rows WHERE table_name='input_evidence' ORDER BY sequence"):
        guard(); item = row_decode(table, decode(payload))
        if not isinstance(item, C2Row) or not isinstance(item.value, InputEvidence) or item.value.kind != 'm3_original':
            continue
        owner, aid, view, ordinal = item.value.reference.split('/')
        key = aid, view
        if (key not in source.rows or owner != source.inputs.admissions[aid]['owner']
                or not ordinal.isdecimal() or str(int(ordinal)) != ordinal):
            raise ValueError('M3 C3原引用范围不符')
        n = int(ordinal)
        if n >= len(source.rows[key]) or item.value.original != source.rows[key][n]:
            raise ValueError('M3 C3原引用重复或原正文变化')
        try:db.execute('INSERT INTO m3_source_seen VALUES (?,?,?)',(aid,view,n))
        except __import__('sqlite3').IntegrityError as error:raise ValueError('M3 C3原引用重复') from error
        seen_count+=1;stats['m3_original_rows_checked'] += 1
    if seen_count != sum(map(len, source.rows.values())):
        raise ValueError('M3 C3完整原输入保留缺项')
    for row in source.elements.values():
        guard()
        db.execute('INSERT INTO source_refs VALUES (?,?)', ('observation', row['event_id']))
    for path in source.paths:
        guard(); db.execute('INSERT INTO source_refs VALUES (?,?)', ('path', path))
    for row in source.get('canonical', 'changes'):
        guard(); element = source.elements[row['event_id']]
        value = saved_change(b, row, source.messages[row['message_id']], element, source.paths[element['path_key']])
        scope = object_scope(value.original)
        fact = (Endpoint(*scope[:8]), scope[8], scope[10], value.after_presence, value.after_path,
                value.after_origin, value.at, value.cursor, value.original['fact_after_presence'])
        db.execute('INSERT INTO source_facts VALUES (?,?)', (value.event_ref, encode(fact)))
    for row in source.get('canonical', 'invalidations'):
        guard(); value = saved_invalidation(b, row, source.messages[row['message_id']])
        db.execute('INSERT INTO state_objects VALUES (?,?)', (value.message_ref, value.object_key))
    for row in source.get('canonical', 'scope_gap'):
        guard(); raw = row['raw']; index = ScopeIndex(); index.add(raw)
        source.history.check_budget(); candidates = len(source.history.scopes)
        if source.history.stats['gap_candidates'] + candidates > source.max_gap_candidates:
            raise ValueError('resource_limit:M3_C3_gap_candidates')
        source.history.stats['gap_candidates'] += candidates
        for obj, scope in source.history.scopes.items():
            guard()
            if source.history.objects[obj][0][0] < position_key(raw['position']) and index.matching(scope):
                # 仅复用旧原引用→对象审计索引；Gap仍是原独立kind，不转成STATE正文。
                db.execute('INSERT INTO state_objects VALUES (?,?)', (raw['gap_id'], obj))
    for aid in source.inputs.owners['reference']:
        for row in source.rows[aid, 'references']:
            guard(); ref = f'{b.run_id}:{b.snapshot_id}/references/{row["source_id"]}/{row["row"]}'
            db.execute('INSERT INTO reference_rows VALUES (?,?)', (ref, encode(row)))
    for result in source.results:
        guard(); row = result['raw']; observation = json.loads(row['evidence_json'])['observation']
        value = saved_country_revision(b, row, source.messages[observation['message_ref']],
                                       source.elements[observation['observation_id']], legacy_timezone=source.timezone)
        db.execute('INSERT INTO source_revisions VALUES (?,?,?)', (value.incident_id, str(value.revision), old.body_sha(value)))
        stats['source_revision_rows_checked'] += 1
    for left, right in (('source_revisions', 'revisions'), ('revisions', 'source_revisions')):
        if db.execute(f'SELECT 1 FROM {left} a LEFT JOIN {right} b USING(incident,revision) WHERE b.incident IS NULL OR a.body_sha!=b.body_sha LIMIT 1').fetchone():
            raise ValueError('M3 C3原国家修订链缺项或改值')
    source.verify_saved()
    return dict(run_id=b.run_id, snapshot=b.snapshot_id, collector=b.collector, sources=b.global_sources)
