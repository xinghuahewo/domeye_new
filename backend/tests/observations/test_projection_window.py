"""显式计算窗的有限资格：原序、原时间和 baseline 不剪裁。"""
from dataclasses import replace
from copy import deepcopy
import pytest

from tests.observations.test_observation_ordered import message, binding, stream_source
from data_pipeline.bgp.ordered_reader import adapt
from data_pipeline.bgp.replay.quality_overlay import CanonicalReplay
from data_pipeline.bgp.replay.route_replay import ReplayPlan
from data_pipeline.bgp.replay.snapshot_contract import encode
from data_pipeline.bgp.replay.calculation_window import calculation_window, WINDOW_RULE

WINDOW={'window_start':'1970-01-01T00:01:40Z','window_end_exclusive':'1970-01-01T00:03:20Z'}
ENVELOPE={'window_start':'1970-01-01T00:00:00Z','window_end_exclusive':'1970-01-01T00:05:00Z'}


def at(source,record,epoch,**kwargs):
    row,es=message(source,record,**kwargs);row['epoch']=epoch
    for e in es:e['epoch']=epoch
    return row,es


def fixture_rows(updates=None):
    # baseline Header 晚于 start，仍作为完整唯一初态；额外 RIB 不进 selected。
    baseline=[at('base',0,110,kind='rib',elements=1)]
    baseline[0][1][0]['action']='rib_snapshot'
    updates=updates if updates is not None else [at('u',0,100,elements=1),at('u',1,180,status='rejected'),at('u',2,170,elements=1)]
    groups=[('base',baseline),('extra',[]),('u',updates),('empty',[])]
    b=binding(groups);b=replace(b,sources=tuple(replace(s,role='snapshot') if s.source_id=='extra' else s for s in b.sources))
    selected=('base','u','empty')
    def flow():
        for sid,rows in groups:
            if sid in selected:yield from stream_source(b,sid,rows)
    items=list(adapt(flow(),b,selected));plan=ReplayPlan('rrc25','base',('u','empty'))
    return b,selected,plan,items


def replay(explicit,updates=None):
    b,selected,plan,items=fixture_rows(updates)
    kwargs={} if explicit is None else dict(calculation_window=explicit)
    core=CanonicalReplay(plan,b,selected,window=ENVELOPE,**kwargs);rows=[]
    for item in items:rows.extend(core.apply(item))
    rows.extend(core.export());return rows


def test_baseline_cross_start_full_values_gap_order_and_global_ranks():
    rows=replay(WINDOW);legacy=replay(None)
    unchanged=lambda rs:[(t,r) for t,r in rs if t not in ('source_coverage','projection_metadata')]
    assert encode(unchanged(rows))==encode(unchanged(legacy))
    coverage=[r for t,r in rows if t=='source_coverage']
    assert [r['source_rank'] for r in coverage]==[0,2,3]
    assert coverage[0]['first_observation']['raw_time']['epoch']==110
    assert coverage[0]['window_qualification']['role']=='baseline_complete_cutover_assumed'
    assert [r['window_qualification']['checked_messages'] for r in coverage]==[0,3,0]
    assert all(r['declared_window']==WINDOW for r in coverage)
    assert coverage[1]['last_observation']['raw_time']['epoch']==170
    assert len([r for t,r in rows if t=='scope_gap'])==1
    assert coverage[-1]['inherited_gap_ids']
    current=[r for t,r in rows if t=='current_routes'][0]
    assert current['qualification']['continuity']=='partial'
    newmeta=next(r for t,r in rows if t=='projection_metadata')
    oldmeta=next(r for t,r in legacy if t=='projection_metadata')
    assert newmeta['calculation_window']==WINDOW and newmeta['plan_version']!=oldmeta['plan_version']
    assert 'calculation_window' not in oldmeta


@pytest.mark.parametrize('epoch,status',[(99,'decoded'),(200,'decoded'),(200,'rejected')])
def test_update_outside_or_gap_at_exclusive_end_rejects_with_original_position(epoch,status):
    with pytest.raises(ValueError) as e:replay(WINDOW,[at('u',0,epoch,status=status,elements=1 if status=='decoded' else 0)])
    for text in ('"source_id": "u"','"source_rank": 2','"record": 0',str(epoch),'update_outside_calculation_window'):
        assert text in str(e.value)
    # 默认旧路径不按名义窗裁剪或新增拒绝。
    assert replay(None,[at('u',0,epoch,status=status)])


def test_last_microsecond_is_inside_and_unknown_et_precision_is_rejected():
    assert replay(WINDOW,[at('u',0,199,microsecond=999999,elements=1)])
    b,selected,plan,items=fixture_rows([at('u',0,150,microsecond=0,elements=1)])
    core=CanonicalReplay(plan,b,selected,calculation_window=WINDOW)
    from data_pipeline.bgp.record_types import MessageBoundary
    for item in items:
        if isinstance(item,MessageBoundary) and item.raw['source_id']=='u':
            item=replace(item,raw_time=replace(item.raw_time,microsecond=None))
            with pytest.raises(ValueError,match='raw_time_et_precision_unknown'):list(core.apply(item))
            break
        list(core.apply(item))


@pytest.mark.parametrize('bad',[
 {'window_start':'1970-01-01T00:01:40','window_end_exclusive':WINDOW['window_end_exclusive']},
 {'window_start':WINDOW['window_end_exclusive'],'window_end_exclusive':WINDOW['window_start']},
 {**WINDOW,'unknown':True},
 {**WINDOW,'window_end_exclusive':'1970-01-01T00:05:01Z'},
])
def test_invalid_window_does_not_change_envelope(bad):
    original=deepcopy(ENVELOPE)
    with pytest.raises(ValueError):calculation_window(bad,ENVELOPE)
    assert ENVELOPE==original


def test_wrong_selected_source_and_binding_reject():
    b,selected,plan,items=fixture_rows()
    with pytest.raises(ValueError):CanonicalReplay(plan,b,('base','extra','u'),calculation_window=WINDOW)
    core=CanonicalReplay(plan,b,selected,calculation_window=WINDOW)
    with pytest.raises(ValueError,match='绑定冲突'):list(core.apply(replace(items[0],binding_ref='wrong')))
