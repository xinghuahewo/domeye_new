"""冻结90e7dfe原实现生成的逐步完整输出；不得用被测实现刷新期望。"""
import copy
import json
from pathlib import Path

from data_pipeline.bgp.input.mrt_reader import Message
from data_pipeline.bgp.replay.route_replay import Replay, ReplayPlan


def inputs():
    """固定多源、同ASN多Peer、双栈、ADDPATH、LOCAL与失效流。"""
    for source, count in [('rib', 12), ('a', 36), ('b', 12)]:
        for record in range(count):
            i = record % 12
            peer = {'ip': f'192.0.2.{i % 2 + 1}', 'asn': 64497,
                    'local_ip': '198.51.100.1', 'local_asn': 12654,
                    'interface': 0, 'bgp_id_present': False}
            m = Message(source, record, record*10, 10, 100+record, 16, 4, 'raw')
            m.kind = 'rib' if source == 'rib' else 'update'
            m.peer = dict(peer)
            if record == 20:
                m.kind = 'state_change'; m.new_state = 1
                yield m
                continue
            if record == 21:
                m.peer['local_message'] = True
            text = ['64497 64496', '64497 {64496,64498}', '', '64497 64512'][i % 4]
            if source == 'a' and record < 12:
                text = '64497 64499'  # 替换后保留历史origin残留
            origin = int(text.split()[-1]) if text and '{' not in text else None
            key = 'path:'+text
            m.paths = [{'path_key': key, 'as_path_text': text, 'attributed_origin_asn': origin}]
            m.elements = [{'ordinal': 0, 'action': 'rib_snapshot' if source == 'rib' else
                           'withdraw' if record >= 24 or source == 'b' else 'announce',
                           'peer': peer, 'afi': 1 if i < 6 else 2, 'safi': 1,
                           'prefix': ('0.0.0.0/0' if i == 0 else f'10.0.{i//2}.0/24') if i < 6 else f'2001:db8:{i//2}::/48',
                           'path_id_present': i >= 8, 'path_id': i % 2 if i >= 8 else None, 'path_key': key}]
            yield m
            if record in (3, 27):
                yield copy.deepcopy(m)  # 相邻重复及撤回幂等


def plain(value):
    if isinstance(value, dict):
        return [[plain(k), plain(v)] for k, v in value.items()]
    if isinstance(value, (tuple, list)):
        return [plain(v) for v in value]
    if isinstance(value, set):
        return sorted(plain(v) for v in value)
    return value


def transcript(replay_class=Replay):
    plan = ReplayPlan('rrc25', 'rib', ('a', 'b'),
                      tuple((f'192.0.2.{i}', 64497, '198.51.100.1', 12654, 0) for i in (1, 2)))
    replay = replay_class(plan)
    steps = []
    for message in inputs():
        changes = list(replay.consume(message))
        steps.append(plain(changes))
    steps.append(plain(list(replay.invalidate('gap', 'coverage_unknown',
                 {'ip': '192.0.2.2', 'asn': 64497, 'local_ip': '198.51.100.1', 'local_asn': 12654, 'interface': 0}, 999))))
    state = {name: getattr(replay, name) for name in
             ('current', 'last_known', 'cursor', 'legacy_paths', 'legacy_by_prefix',
              'legacy_origins', 'seen_vps', 'legacy_baseline_epoch')}
    return {'steps': steps, 'state': plain(state)}


def test_complete_frozen_transcript():
    expected = json.loads((Path(__file__).parents[1]/'fixtures/replay-90e7dfe.json').read_text())
    assert transcript() == expected['transcript']


def test_cache_eviction_and_plain_dictionary_interface():
    replay = Replay(ReplayPlan('rrc25', 'rib', ('a', 'b')))
    messages = iter(inputs())
    row = list(replay.consume(next(messages)))[0]
    before = copy.deepcopy(row)
    for i in range(10000):
        replay._value('人工长尾'+str(i)); replay._prefix('人工前缀'+str(i))
    assert replay._value.cache_info().currsize == 8192
    assert replay._prefix.cache_info().currsize == 128
    assert row == before
    assert type(row['calculation_after']) is dict
    for message in messages:
        list(replay.consume(message))
    assert row == before
