"""紧凑状态与原算法逐步等价；不把失效当撤回，不复制最后已知负载。"""
from copy import deepcopy
import hashlib
from types import SimpleNamespace
import pytest
from data_pipeline.bgp.replay.route_replay import Replay, ReplayPlan


def message(record,action='announce',origin=64496,peer='192.0.2.1',present=False,pathid=None):
    path=hashlib.sha256(str(origin).encode()).hexdigest()
    return SimpleNamespace(source_id='u',record=record,message_id=f'u:{record}',epoch=100+record,
        kind='update',peer=dict(ip=peer,asn=64497,local_ip='192.0.2.2',local_asn=12654,interface=0),
        paths=[dict(path_key=path,as_path_text='64497 '+str(origin),attributed_origin_asn=origin)],
        elements=[dict(peer=dict(ip=peer,asn=64497,local_ip='192.0.2.2',local_asn=12654,interface=0),
                       action=action,prefix='192.0.2.0/24',afi=1,safi=1,path_key=path,
                       ordinal=0,path_id=pathid,path_id_present=present)])


def test_shared_state_preserves_stepwise_before_and_last_withdrawal():
    plan=ReplayPlan('fixture','rib',('u',));old=Replay(plan);new=Replay(plan,compact=True)
    entries=[message(0),message(1),message(2,origin=64498),message(3,'withdraw'),message(4,'withdraw'),
             message(5,peer='192.0.2.3'),message(6,present=True,pathid=0),message(7,present=True,pathid=7)]
    for m in entries:
        assert list(old.consume(m))==list(new.consume(m))
        assert old.current==new.current
        assert old.last_known==new.last_known
        assert old.legacy_paths==new.legacy_paths
    before=deepcopy(dict(new.current))
    for replay in (old,new):list(replay.invalidate('u:8','session_disconnect',entries[0].peer,108))
    assert old.current==new.current and old.last_known==new.last_known
    assert before==dict(new.last_known)
    assert len(new.memory.prefix_dict)==4
    assert list(old.consume(message(9)))==list(new.consume(message(9)))
    assert old.current==new.current and old.last_known==new.last_known


def test_position_and_unknown_do_not_overwrite_last_known_absence():
    replay=Replay(ReplayPlan('fixture','rib',('u',)),compact=True)
    m=message(0,'withdraw');list(replay.consume(m));key=next(iter(replay.current))
    replay.memory.positions[key]={'message':{'source_rank':3,'record':2},'ordinal':0}
    old=replay.last_known[key]
    replay.current[key]={**replay.current[key],'presence':'unknown','reason':'parse_gap','invalidated_by':'gap'}
    assert replay.last_known[key]==old and old['presence']=='absent'
    assert replay.current[key]['presence']=='unknown'
    assert replay.memory.positions[key]['message']['source_rank']==3
    assert len(replay.memory.prefix_dict)==1
