"""使用真实 CanonicalReplay 的人工输出核对历史隔离与精确对象恢复。"""
import pytest

from tests.observations.test_canonical_projection import run_rows, key
from tests.observations.test_observation_ordered import message
from data_pipeline.analysis.country_events.route_binding import M3Binding
from data_pipeline.analysis.country_events.route_history import CanonicalHistory


def history(rows, *, max_gap_candidates=100):
    core, output, _ = run_rows(rows)
    binding = M3Binding('fixture', '1', ('s',), 'ref', 'decoder', 'state', 'rrc25',
                        global_sources=('s',), input_binding_id=core.binding.binding_id)
    index = CanonicalHistory(binding, max_rows=1000, max_bytes=1024**2,
                             max_gap_candidates=max_gap_candidates, guard=lambda: None)
    for table, row in output:
        if table in index.tables:
            index.append(table, row)
    index.seal()
    return index


def test_later_gap_and_final_recovery_do_not_rewrite_earlier_sample():
    index = history([message('s', 0, elements=2), message('s', 1, status='rejected'),
                     message('s', 2, elements=1)])
    before = index.at(key(), (0, 0, 1, 1))
    lost = index.at(key(), (0, 1, 0, 0))
    recovered = index.at(key(), (0, 2, 1, 0))
    other = index.at(key('198.51.1.0/24'), (0, 2, 1, 0))
    assert before['presence'] == 'present' and before['gap_refs'] == ()
    assert lost['presence'] == 'unknown' and lost['path_key'] is None
    assert lost['original']['raw']['calculation_after']['presence'] == 'present'
    assert recovered['presence'] == 'present' and recovered['active_gap_refs'] == ()
    assert recovered['continuity'] == 'partial' and recovered['gap_refs']
    assert other['presence'] == 'unknown'
    assert index.at(key(), (0, 0, 1, 1)) == before


def test_local_gap_never_invalidates_received_and_unseen_is_not_zero():
    index = history([message('s', 0, elements=1),
                     message('s', 1, status='rejected', direction='local')])
    assert index.at(key(), (0, 1, 0, 0))['presence'] == 'present'
    unseen = index.at(key('203.0.113.0/24'), (0, 1, 0, 0))
    assert unseen['presence'] == 'unknown' and unseen['source_ref'] is None


def test_state_invalidation_does_not_promote_last_known_to_current():
    index = history([message('s', 0, elements=1), message('s', 1, kind='state_change')])
    result = index.at(key(), (0, 1, 0, 0))
    assert result['presence'] == 'unknown' and result['origin'] is None
    assert result['source_ref']['table'] == 'invalidations'
    assert result['original']['raw']['last_known_presence'] == 'present'


def test_candidate_budget_is_shared_across_targets():
    index = history([message('s', 0, status='rejected')], max_gap_candidates=1)
    index.at(key(), (0, 0, 0, 0))
    index.at(key('203.0.113.0/24'), (0, 0, 0, 0))
    assert index.stats['gap_candidates']==2


@pytest.mark.parametrize('bad', [True, float('nan'), float('inf'), 0, -1, 1.5])
def test_budget_is_checked_again_at_use(bad):
    index = history([message('s', 0, elements=1)])
    index.max_gap_candidates = bad
    with pytest.raises(ValueError, match='M3_history_budget'):
        index.at(key(), (0, 0, 1, 0))


def test_wrong_collector_and_slot_cannot_be_silently_unknown():
    index = history([message('s', 0, elements=1)])
    for scope in (('other', *key()[1:]), (*key()[:9], False, 0)):
        with pytest.raises(ValueError, match='M3 查询对象'):
            index.at(scope, (0, 0, 1, 0))


def test_source_record_and_element_position_must_agree():
    from copy import deepcopy
    core, output, _ = run_rows([message('s', 0, elements=1)])
    row = next(r for table, r in output if table == 'changes')
    binding = M3Binding('fixture', '1', ('s',), 'ref', 'decoder', 'state', 'rrc25',
                        global_sources=('s',), input_binding_id=core.binding.binding_id)
    for position in ({'message': {'source_rank': 0, 'record': 1}, 'ordinal': 0},
                     {'message': {'source_rank': 0, 'record': 0}, 'ordinal': 1},
                     {'source_rank': 0, 'record': 0}):
        index = CanonicalHistory(binding, max_rows=10, max_bytes=1024**2,
                                 max_gap_candidates=10, guard=lambda: None)
        bad = deepcopy(row); bad['position'] = position
        with pytest.raises(ValueError):
            index.append('changes', bad)
