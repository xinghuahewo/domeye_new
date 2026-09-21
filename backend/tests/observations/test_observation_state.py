import dataclasses
from data_pipeline.bgp.replay.route_replay import Replay, ReplayPlan, associate
from data_pipeline.bgp.input.mrt_reader import Message
import pytest


def message(source,record,action='announce',origin=64496,ip='192.0.2.1',local='192.0.2.2'):
    m=Message(source,record,0,1,100,16,4,'raw')
    m.kind='update'
    m.peer={'ip':ip,'asn':64497,'local_ip':local,'local_asn':12654,'interface':0,'bgp_id_present':False}
    m.paths=[{'path_key':str(origin),'as_path_text':f'64497 {origin}','attributed_origin_asn':origin}]
    m.elements=[{'ordinal':0,'action':action,'peer':m.peer,'afi':1,'safi':1,'prefix':'192.0.2.0/24',
                 'path_id_present':False,'path_id':None,'path_key':str(origin)}]
    return m


def test_two_files_replay_and_legacy_residual():
    plan=ReplayPlan('rrc25','rib',('a','b'),(('192.0.2.1',64497,'192.0.2.2',12654,0),))
    baseline=message('rib',0,'rib_snapshot')
    baseline.elements[0]['peer']={'ip':'192.0.2.1','asn':64497}
    inputs=[baseline,message('a',0,origin=64498),message('b',0,'withdraw')]
    def run():
        r=Replay(plan);rows=[row for m in inputs for row in r.consume(m)]
        return r,rows
    r,rows=run()
    assert rows==run()[1]
    assert rows[1]['calculation_before']['origin']==64496
    assert rows[1]['calculation_after']['origin']==64498
    assert rows[1]['legacy_origin_added']=='64498' and rows[1]['legacy_origin_count']==2
    assert rows[2]['calculation_after']['presence']=='absent'
    assert rows[2]['legacy_origin_removed']=='64498' and rows[2]['legacy_origin_count']==1
    assert r.legacy_origins['192.0.2.0/24']=={'64496'}
    assert rows[1]['fact_before_presence']=='unknown'
    assert list(r.consume(inputs[-1]))==[]
    with pytest.raises(ValueError):list(r.consume(inputs[1]))


def test_endpoint_gap_and_unknown_withdraw():
    r=Replay(ReplayPlan('rrc25','rib',('a',)))
    one=message('a',0);two=message('a',1,local='192.0.2.3')
    list(r.consume(one));list(r.consume(two))
    assert len(r.current)==2
    assert len(list(r.invalidate('gap','gap',one.peer,101)))==1
    assert sorted(v['presence'] for v in r.current.values())==['present','unknown']
    assert all(v['presence']=='present' for v in r.last_known.values())
    w=message('a',2,'withdraw',ip='192.0.2.9')
    row=list(r.consume(w))[0]
    assert row['calculation_before'] is None
    assert row['fact_after_presence']=='absent'


def test_unique_time_association_and_conflicts():
    m=message('a',0);rib=message('rib',0)
    p={'ip':'192.0.2.1','asn':64497,'bgp_id':'192.0.2.1','table_record':0,'index':0}
    assert associate(m,rib,[p])[0] is not None
    assert associate(m,dataclasses.replace(rib,epoch=101),[p])[0] is None
    assert associate(m,dataclasses.replace(rib,microsecond=0),[p])[0] is None
    assert associate(m,rib,[p,{**p,'bgp_id':'192.0.2.9'}])[0] is None
    assert associate(m,rib,[{**p,'ip':'2001:db8::1'}])[0] is None


def test_state_disconnect_is_scoped_record_not_withdraw():
    r=Replay(ReplayPlan('rrc25','rib',('a',)))
    m=message('a',0);list(r.consume(m))
    state=message('a',1);state.kind='state_change';state.new_state=1;state.elements=[]
    rows=list(r.consume(state))
    assert len(rows)==1 and rows[0]['record_type']=='invalidation'
    assert rows[0]['last_known_presence']=='present'
    later=message('a',2);later.elements[0]['prefix']='198.51.100.0/24'
    list(r.consume(later))
    assert sorted(v['presence'] for v in r.current.values())==['present','unknown']


def test_local_message_does_not_change_received_state():
    r=Replay(ReplayPlan('rrc25','rib',('a',)))
    m=message('a',0);m.peer['local_message']=True
    assert list(r.consume(m))==[] and not r.current


def test_legacy_unknown_withdraw_does_not_add_seen_vp():
    r=Replay(ReplayPlan('rrc25','rib',('a',)))
    list(r.consume(message('a',0,'withdraw')))
    assert not r.seen_vps


def test_legacy_default_filter_only_rib():
    r=Replay(ReplayPlan('rrc25','rib',('a',)))
    m=message('rib',0,'rib_snapshot');m.elements[0]['prefix']='0.0.0.0/0'
    assert list(r.consume(m))[0]['legacy_skip']=='legacy_default_route'
    m=message('a',0);m.elements[0]['prefix']='0.0.0.0/0'
    assert list(r.consume(m))[0]['legacy_after'] is not None
