"""范围Gap真正经过C2状态扫描；空/LOCAL匹配不能升级成全局失效。"""
from data_pipeline.analysis.country_events.route_gap_adapter import M3Stage, ScopedGap
from data_pipeline.analysis.country_events.aggregation_staging import load
from data_pipeline.analysis.country_events.models import Route, Endpoint, Cursor, Time
from data_pipeline.analysis.country_events.compute import Change, Path, Segment
from data_pipeline.analysis.country_events.saved_input import object_scope
from tests.country.test_country_m3_history import history
from tests.observations.test_observation_ordered import message
import pytest


def stage(tmp_path, raw, budget=100):
    h = history(raw)
    limits = dict(max_row_bytes=1024**2, max_staged_rows=1000, max_changes=1000, max_objects=1000)
    s = M3Stage(tmp_path/'stage.sqlite', h.binding, lambda: None, limits,
                baseline_source='s', history=h, messages={r[0]['message_id']: r[0] for r in raw},
                max_gap_candidates=budget)
    for row in h.rows['changes']:
        value = row['raw']['calculation_after']; raw_scope = object_scope(row['raw'])
        p = row['position']; cursor = Cursor(p['message']['source_rank'], p['message']['record'], p['ordinal'])
        at = Time(value['epoch'], None)
        path = value['path_key']
        if path is not None:s.put('path', path, Path(path, (Segment('sequence', (1,)),), b''))
        route = Route(row['object_key'], Endpoint(*raw_scope[:8]), raw_scope[8], raw_scope[10],
                      value['presence'], path, value['origin'], row['event_id'], at, cursor,
                      'fixture-mapping', 'fixture-peer')
        s.add_change(Change(cursor, at, 's', row['event_id'], route))
    s.finish()
    return s


@pytest.mark.parametrize('direction,expected', [('received', 'unknown'), ('local', 'present')])
def test_scope_state_and_exact_object_recovery(tmp_path, direction, expected):
    raw = [message('s', 0, elements=2), message('s', 1, status='rejected', direction=direction),
           message('s', 2, elements=1)]
    s = stage(tmp_path, raw)
    try:
        at_gap = None
        for payload, in s.db.execute('SELECT payload FROM units ORDER BY i'):
            change = load(payload); s.apply(change)
            if isinstance(change, ScopedGap):
                at_gap = [r[0] for r in s.db.execute('SELECT presence FROM current ORDER BY object_id')]
                assert not change.global_invalidation
                assert change.original_gap['scope']['kind'] == ('local_observation' if direction == 'local' else 'received_endpoint')
        assert at_gap == [expected, expected]
        final = sorted(r[0] for r in s.db.execute('SELECT presence FROM current'))
        assert final == sorted(['present', expected])
        assert s.global_gap is False
        assert s.db.execute("SELECT count(*) FROM evidence WHERE kind='m3_scope_gap'").fetchone()[0] == 1
    finally:s.close()


def test_empty_scope_keeps_original_gap_without_global_state(tmp_path):
    s = stage(tmp_path, [message('s', 0, status='rejected')])
    try:
        change = load(s.db.execute('SELECT payload FROM units').fetchone()[0])
        assert isinstance(change, ScopedGap) and change.invalidated_objects == ()
        s.apply(change)
        assert s.global_gap is False and s.db.execute('SELECT count(*) FROM current').fetchone()[0] == 0
    finally:s.close()


def test_gap_candidate_budget_charged_before_matching(tmp_path):
    s=stage(tmp_path, [message('s', 0, elements=2), message('s', 1, status='rejected', direction='local')], budget=1)
    try:assert s.counts['gap_scope_candidates']==2
    finally:s.close()


def disjoint_input():
    import json
    m,es=message('s',0,elements=1);m['epoch']=10;m['peer_ip']='192.0.2.3'
    interp=json.loads(m['interpretation']);interp['header']['peer_ip']='192.0.2.3';m['interpretation']=json.dumps(interp)
    for e in es:e['epoch']=10;e['peer_ip']='192.0.2.3'
    gap,ge=message('s',1,status='rejected');gap['epoch']=20
    return [(m,es),(gap,ge)]


def test_ambiguous_time_disjoint_fixed_scope_but_potential_members_remain_unknown(tmp_path):
    from dataclasses import replace
    from data_pipeline.analysis.country_events.aggregation_staging import dump
    s=stage(tmp_path,disjoint_input())
    try:
        first=load(s.db.execute('SELECT payload FROM units WHERE i=0').fetchone()[0]);s.apply(first)
        low,high=s.boundary(20500000);assert (low,high)==(0,1)
        assert not s.impact(low,high,'IR',{'198.51.0.0/24'},100)
        assert s.impact(low,high,'IR',{'198.51.0.0/24'},100,include_potential=True)
        assert s.impact(low,high,'IR',{'203.0.113.0/24'},100)
        route=replace(first.after,mapping_state='unmapped')
        s.db.execute('UPDATE current SET payload=?',(dump(route),))
        assert s.impact(low,high,'IR',{'198.51.0.0/24'},100)
    finally:s.close()


def test_actual_c2_fixed_e2_sample_is_not_erased_by_e1_time_gap(tmp_path):
    from data_pipeline.analysis.country_events.models import Reference, Incident
    from data_pipeline.analysis.country_events.compute import Grid, MetricPoint
    from data_pipeline.analysis.country_events.event_aggregation import calculate, C2Row, CohortMember, EventStatus
    s=stage(tmp_path,disjoint_input())
    try:
        # 纯C2完成基线是首事实；本例不冒充正式M3来源角色链。
        s.initial_end=0
        s.limits.update(max_events=10,max_active=10,max_total_members=100,max_boundary_span=100,max_relations=100,batch_rows=256,batch_bytes=4194304)
        first=load(s.db.execute('SELECT payload FROM units WHERE i=0').fetchone()[0]);r=first.after
        s.put('reference',r.origin,Reference(r.origin,'IR','independent-ref'))
        event=Incident('fixed-IR',1,'IR',Time(16,0),onset=Time(15,0))
        s.db.execute('INSERT INTO revisions VALUES (?,?,?)',(event.incident_id,1,s.checked(event)))
        rows=list(calculate(s,Grid(10000000,22000000,(19000000,20500000,21500000)),s.limits,lambda:None))
        assert [x.value.route for x in rows if isinstance(x,C2Row) and isinstance(x.value,CohortMember)]==[r]
        assert next(x.value.direction_count for x in rows if isinstance(x,C2Row) and isinstance(x.value,EventStatus))==1
        points={x.value.sample_us:x.value.value for x in rows if isinstance(x,C2Row) and isinstance(x.value,MetricPoint) and x.value.metric=='visible_direction_count'}
        assert points=={19000000:1,20500000:1,21500000:1}
    finally:s.close()
