"""真实CanonicalReplay人工原行接合；不构造Admission或补造事件。"""
from copy import deepcopy
from datetime import datetime, timezone
import json
import pytest

from tests.observations.test_canonical_projection import run_rows
from tests.observations.test_observation_ordered import message
from data_pipeline.analysis.country_events.route_binding import M3Binding
from data_pipeline.analysis.country_events.route_change_adapter import saved_change, saved_invalidation, saved_country_revision
from data_pipeline.analysis.country_events.saved_input import object_scope


def fixture():
    raw = message('s', 0, elements=1)
    core, output, _ = run_rows([raw])
    row = next(r for table, r in output if table == 'changes')
    row = deepcopy(row); row['position']['message']['source_rank'] = 2
    binding = M3Binding('fixture', '1', ('s',), 'ref', 'decoder', 'state', 'rrc25',
                        global_sources=('unselected0', 'unselected1', 's'), input_binding_id=core.binding.binding_id)
    return binding, row, raw[0], raw[1][0]


def test_keeps_outer_global_rank_and_entire_original_before_after():
    b, row, msg, element = fixture()
    value = saved_change(b, row, msg, element, element)
    assert value.cursor.source_rank == 2 and value.original['source_rank'] == 0
    assert value.original == row['raw']
    assert object_scope(value.original) == list(row['raw']['scope'])
    legacy = dict(row['raw'], scope=tuple(None if v is None else str(v) for v in row['raw']['scope']))
    assert object_scope(legacy) == object_scope(value.original)
    row['raw']['calculation_after']['presence'] = 'unknown'
    assert value.after_presence == 'present' and value.original['calculation_after']['presence'] == 'present'


@pytest.mark.parametrize('field,value', [('message_id', 'other:0'), ('ordinal', 5), ('prefix', '203.0.113.0/24')])
def test_wrong_element_join_rejected(field, value):
    b, row, msg, element = fixture()
    bad = dict(element, **{field: value})
    with pytest.raises(ValueError):
        saved_change(b, row, msg, bad, element)


def test_gap_is_not_a_saved_state_invalidation():
    raw = [message('s', 0, elements=1), message('s', 1, status='rejected')]
    core, output, _ = run_rows(raw)
    b = M3Binding('fixture', '1', ('s',), 'ref', 'decoder', 'state', 'rrc25',
                  global_sources=('s',), input_binding_id=core.binding.binding_id)
    row = next(r for table, r in output if table == 'scope_gap')
    with pytest.raises(ValueError):
        saved_invalidation(b, row, raw[1][0])


def test_country_revision_trigger_does_not_invent_onset_or_carry_in():
    b, _, msg, element = fixture()
    observed = datetime.fromtimestamp(msg['epoch'], timezone.utc)
    evidence = dict(observation=dict(snapshot_ref='fixture:1', source_version='fixture:1', collector_id='rrc25',
                                    observation_id=element['event_id'], message_ref=msg['message_id'],
                                    element_ordinal=element['ordinal'], source_id='s', path_ref=element['path_key'],
                                    epoch=msg['epoch'], microsecond=msg['microsecond'], path_id=element['path_id'],
                                    path_id_present=element['path_id_present'], observed_at=observed.isoformat()))
    row = dict(record_kind='business_revision', subject_type='country', event_kind='country_outage',
               incident_id='fixture-event', revision=1, subject_key='ZZ', observed_at=observed,
               attributes_json=json.dumps(dict(incident_id='fixture-event', revision=1, object='ZZ', reference_version='ref')),
               evidence_json=json.dumps(evidence), legacy_json=json.dumps(dict(s_time='1970-01-01 08:00:00', e_time=None)))
    value = saved_country_revision(b, row, msg, element, legacy_timezone='Asia/Shanghai')
    assert value.trigger_cursor.source_rank == 2 and value.onset_cursor is None and value.carry_in_state == 'unknown'
    assert all(value.original[k] == v for k, v in row.items())
    evidence['observation']['epoch'] += 1
    with pytest.raises(ValueError, match='触发时间'):
        saved_country_revision(b, dict(row, evidence_json=json.dumps(evidence)), msg, element, legacy_timezone='Asia/Shanghai')
