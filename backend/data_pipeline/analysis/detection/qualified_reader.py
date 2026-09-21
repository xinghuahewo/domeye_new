"""M3公共查询：原typed值不变，主值按独立固定资格查询。"""
from contextlib import closing
import json
import psycopg2
from data_pipeline.analysis.detection.store import read_stored_rows
from data_pipeline.analysis.detection.qualification_contract import validate_identity, validate_entry


def read_binding(dsn, run_id, snapshot):
    pg = psycopg2.connect(dsn)
    try:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute('SELECT state,schema_name,snapshot,identity,scope FROM detection.runs WHERE run_id=%s', (run_id,))
            row = c.fetchone()
    finally:
        pg.close()
    if row is None or row[:3] != ('complete','det_'+run_id,snapshot):
        raise ValueError('候选、失败或错版不可读取M3')
    validate_identity(row[3])
    return dict(run_id=run_id, snapshot=snapshot, identity=row[3], scope=row[4])


def read_coverage(dsn, run_id, snapshot, *, expected_binding_id, **limits):
    pinned = read_binding(dsn, run_id, snapshot)
    if pinned['identity']['input_binding_id'] != expected_binding_id:
        raise ValueError('M3输入绑定不符')
    count = 0
    gaps, sources, expected_events, actual_events = set(), [], set(), set()
    with closing(read_stored_rows(dsn,run_id,snapshot,'records',**limits)) as records:
        for row in records:
            if row['record_kind']=='business_revision': expected_events.add((row['incident_id'],row['revision']))
    with closing(read_stored_rows(dsn,run_id,snapshot,'m3_entries',**limits)) as rows:
        for row in rows:
            if row['ordinal'] != count:
                raise ValueError('资格枚举乱序')
            payload=validate_entry(row,run_id,pinned['identity'])
            if row['kind']=='scope_gap':
                if row['gap_id'] in gaps: raise ValueError('重复Gap')
                gaps.add(row['gap_id'])
            elif row['kind']=='source_coverage': sources.append(row['source_id'])
            elif row['kind']=='event_qualification':
                key=row['incident_id'],row['revision']
                if key not in expected_events: raise ValueError('资格坏revision引用')
                actual_events.add(key)
            else: raise ValueError('未知资格类型')
            if any(g not in gaps for g in payload.get('gap_refs',[])):
                raise ValueError('资格坏Gap引用')
            count += 1
            yield row  # 原JSON文本及所有typed字段。
    if sources != pinned['identity']['selected_sources'] or actual_events != expected_events:
        raise ValueError('来源/事件资格缺项')
    observed_counts=dict(entries=count,gaps=len(gaps),sources=len(sources),revisions=len(actual_events))
    if observed_counts != pinned['identity']['qualification_counts']:
        raise ValueError('资格表枚举计数不符')
    if read_binding(dsn,run_id,snapshot) != pinned:
        raise ValueError('M3绑定在读取末尾漂移')


def read_qualified_revisions(dsn, run_id, snapshot, *, expected_binding_id, at_position, **limits):
    """固定处理位置的主值；完全耗尽才取得末尾保证，早停须close。"""
    if len(at_position) != 4 or any(type(v) is not int or v < 0 for v in at_position):
        raise ValueError('必须明确四元处理位置')
    pinned = read_binding(dsn,run_id,snapshot)
    if tuple(at_position)>tuple(pinned['identity']['qualification_as_of_position']):
        raise ValueError('查询位置超出已消费范围')
    qualifications = {}
    with closing(read_coverage(dsn,run_id,snapshot,expected_binding_id=expected_binding_id,**limits)) as rows:
        for row in rows:
            if row['kind'] == 'event_qualification':
                payload = json.loads(row['payload_json'])
                if tuple(payload['effective_position']) <= tuple(at_position):
                    qualifications[row['incident_id'],row['revision']] = dict(payload, qualification_id=row['entry_id'])
    with closing(read_stored_rows(dsn,run_id,snapshot,'records',**limits)) as rows:
        for row in rows:
            if row['record_kind'] != 'business_revision': continue
            q = qualifications.get((row['incident_id'],row['revision']))
            if q is None: continue  # 该revision在查询位置尚未生成。
            yield dict(raw=row, main=row if q['coverage']=='complete' else None,
                       qualification=q, as_of_position=tuple(at_position))
    if read_binding(dsn,run_id,snapshot) != pinned:
        raise ValueError('M3跨表读取绑定漂移')


def qualify_vp_state(gap_payloads, *, legacy_vp, at_position):
    """只判精确VP依赖的缺口覆盖，不能用于origin/事件/国家聚合资格。"""
    refs = []
    for row in gap_payloads:
        if tuple(row['effective_position']) <= tuple(at_position):
            affected = row['dependency_scope']['legacy_vp']
            if affected is None or affected == str(legacy_vp):
                refs.append(row['gap']['gap_id'])
    return dict(coverage='unknown' if refs else 'complete', gap_refs=refs,
                dimension='vp_origin_current', scope='exact_legacy_vp_only',
                aggregate_qualification='not_evaluated')


def read_qualified_records(dsn, run_id, snapshot, *, expected_binding_id, at_position, **limits):
    """原记录全流与窗口资格；非事件记录按模块依赖保守判定，不改raw。"""
    if len(at_position)!=4 or any(type(v) is not int or v<0 for v in at_position):
        raise ValueError('必须明确处理位置')
    pinned=read_binding(dsn,run_id,snapshot)
    if tuple(at_position)>tuple(pinned['identity']['qualification_as_of_position']):
        raise ValueError('查询位置超出已消费范围')
    applicable=[]
    ranks={source['source_id']:i for i,source in enumerate(pinned['identity']['input_binding']['sources'])}
    with closing(read_coverage(dsn,run_id,snapshot,expected_binding_id=expected_binding_id,**limits)) as rows:
        for row in rows:
            if row['kind']=='scope_gap':
                p=json.loads(row['payload_json'])
                applicable.append((tuple(p['effective_position']),row['entry_id']))
    with closing(read_stored_rows(dsn,run_id,snapshot,'records',**limits)) as rows:
        for row in rows:
            observation=json.loads(row['evidence_json']).get('observation')
            position=None
            if observation and observation.get('source_id') in ranks:
                position=(ranks[observation['source_id']],int(observation['message_ref'].rsplit(':',1)[1]),1,observation['element_ordinal'])
            relevant=[entry for pos,entry in applicable if position is not None and pos<=position]
            # 无可定位科学触发游标或固定事件修订，不能从全模块标签推出主值。
            eligible=position is not None and position<=tuple(at_position) and not relevant and row['record_kind']!='business_revision'
            yield dict(raw=row, main=row if eligible else None,
                       qualification=dict(coverage='complete' if eligible else 'unknown',
                                          scope='original_trigger_position',
                                          scope_gap_entries=relevant,
                                          limitation='事件使用read_qualified_revisions；无触发位置不猜资格'),
                       original_position=position,as_of_position=tuple(at_position))
    if read_binding(dsn,run_id,snapshot)!=pinned:
        raise ValueError('M3全记录读取绑定漂移')


def read_qualified_state_entries(dsn,run_id,snapshot,*,expected_binding_id,**limits):
    """只读实际终态，不能把保存终态当作过去as_of状态。"""
    pinned=read_binding(dsn,run_id,snapshot)
    gaps=[]
    with closing(read_coverage(dsn,run_id,snapshot,expected_binding_id=expected_binding_id,**limits)) as rows:
        for row in rows:
            if row['kind']=='scope_gap': gaps.append(row['entry_id'])
    with closing(read_stored_rows(dsn,run_id,snapshot,'state_entries',**limits)) as rows:
        for row in rows:
            yield dict(raw=row,main=None if gaps else row,
                       qualification=dict(coverage='unknown' if gaps else 'complete',
                                          scope='conservative_aggregate_final_state',scope_gap_entries=gaps),
                       as_of_position=pinned['identity']['qualification_as_of_position'])
    if read_binding(dsn,run_id,snapshot)!=pinned:
        raise ValueError('M3终态读取绑定漂移')
