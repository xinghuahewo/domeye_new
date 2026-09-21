"""国家消费 D 的完整链与独立覆盖；人工原行，不构造 Admission。"""
from copy import deepcopy
import hashlib
import json
import pytest

from data_pipeline.analysis.country_events.qualified_event_results import country_results
from data_pipeline.analysis.country_events.route_inputs import check_saved_read, update_read_digest
from data_pipeline.bgp.archive.admission import digest


def fixture():
    window = dict(window_start='1970-01-01T00:01:50Z', window_end_exclusive='1970-01-01T00:02:20Z')
    calc = dict(window_start='1970-01-01T00:01:40Z', window_end_exclusive=window['window_end_exclusive'])
    binding = dict(scope=dict(window_start=calc['window_start'], window_end=calc['window_end_exclusive']),
                   identity=dict(result_window=window, result_window_rule='detection-result-window/v1',
                                 window_coverage={'fixture': True}, qualification_as_of_position=[2, 18, 0, 0]))
    records = [dict(sequence=s, record_kind='business_revision', incident_id='country-event', revision=n,
                    subject_type='country', event_kind='country_outage',
                    legacy_json=json.dumps(dict(s_time='1970-01-01 08:01:46', e_time=None)))
               for n, s in ((1, 52), (2, 68))]
    entries = [dict(ordinal=n, entry_id=f'q{n}', kind='event_qualification', incident_id='country-event',
                    revision=n, payload_json=json.dumps(dict(coverage='unknown'))) for n in (1, 2)]
    entries.append(dict(ordinal=3, entry_id='source-coverage', kind='source_coverage', payload_json='{}'))
    proof = dict(first_sequence=52, last_sequence=68, revisions=2, final_qualification_id='q2',
                 original_start='1970-01-01 08:01:46', original_end=None, selection='possible_unknown',
                 result_window=window, calculation_window=calc, earlier_history='Unknown',
                 active_in_final_saved_state=True, final_state_ordinal=17)
    results = [dict(raw=deepcopy(r), main=None, qualification=dict(coverage='unknown', qualification_id=f'q{r["revision"]}'),
                    lifecycle=deepcopy(proof), as_of_position=[2, 18, 0, 0]) for r in records]
    coverage = [dict(raw=deepcopy(entries[-1]), windows=dict(result_window=window, calculation_window=calc),
                     window_coverage={'fixture': True}, earlier_history='Unknown')]
    return records, entries, results, coverage, binding


def test_complete_pre_d_chain_retains_unknown_and_original_rows():
    args = fixture()
    selected = country_results(*args, max_rows=9, guard=lambda: None)
    assert [r['raw'] for r in selected] == args[0]
    assert all(r['main'] is None and r['lifecycle']['selection'] == 'possible_unknown' for r in selected)
    args[2][0]['raw']['revision'] = 999
    assert selected[0]['raw']['revision'] == 1


@pytest.mark.parametrize('mutation', ['trim', 'q', 'raw', 'coverage', 'window', 'main', 'earlier'])
def test_no_truncation_qualification_promotion_or_window_rebinding(mutation):
    args = fixture()
    if mutation == 'trim': args[2].pop(0)
    elif mutation == 'q': args[2][0]['qualification']['coverage'] = 'complete'
    elif mutation == 'raw': args[2][0]['raw']['legacy_json'] = '{}'
    elif mutation == 'coverage': args[3].clear()
    elif mutation == 'window': args[2][0]['lifecycle']['result_window'] = {}
    elif mutation == 'main': args[2][0]['main'] = args[2][0]['raw']
    elif mutation == 'earlier': args[2][0]['lifecycle']['earlier_history'] = 'complete'
    with pytest.raises(ValueError):
        country_results(*args, max_rows=100, guard=lambda: None)


def test_zero_event_keeps_coverage_and_shared_budget():
    args = fixture()
    assert country_results([], [args[1][-1]], [], args[3], args[4], max_rows=2, guard=lambda: None) == ()
    with pytest.raises(ValueError, match='resource_limit'):
        country_results(*args, max_rows=7, guard=lambda: None)


@pytest.mark.parametrize('owner', ['m2', 'reference', 'canonical', 'detection'])
def test_saved_full_receipt_rejects_reorder_mutation_truncation_and_wrong_admission(owner):
    rows = [dict(row=1, value=None), dict(row=2, value=b'raw')]
    h = hashlib.sha256()
    for row in rows: update_read_digest(h, owner, row)
    request = dict(view='fixture')
    saved = dict(owner=owner, view='fixture', request=request,
                 receipt=dict(admission_id='original', request_digest=digest(request), execution='complete',
                              rows=2, typed_digest=h.hexdigest()))
    check_saved_read(owner, 'original', 'fixture', rows, saved, guard=lambda: None)
    for bad in (rows[::-1], rows[:1], [dict(rows[0], value=0), rows[1]]):
        with pytest.raises(ValueError):
            check_saved_read(owner, 'original', 'fixture', bad, saved, guard=lambda: None)
    with pytest.raises(ValueError):
        check_saved_read(owner, 'other', 'fixture', rows, saved, guard=lambda: None)
