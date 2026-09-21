"""新国家规则人工反例；不读取真实制品或导入旧代码。"""
from dataclasses import replace
import pytest
from data_pipeline.analysis.country_events import Baseline, Binding, Cursor, Endpoint, Incident, Reference, Route, Time, freeze_cohorts, select_baseline


def binding():
    return Binding('fixture-run', 'snapshot-1', ('rib', 'u1', 'u2'), 'refs-1', 'decoder-1', 'calculation-replay/v1', 'rrc25')


def baseline():
    return Baseline('baseline-1', binding(), Time(10, 0), Cursor(0, 9, 0), 'mapping-dataset-1')


def incident(country='IR'):
    return Incident('incident-'+country, 1, country, Time(20), onset=Time(15))


def route(identifier='r1', **values):
    initial = Route(identifier, Endpoint('rrc25', '192.0.2.1', 64496, '192.0.2.2', 12654, 0, 1, 1),
                    '198.51.100.0/24', None, 'present', 'p1', 64497,
                    'observation-'+identifier, Time(10, 0), Cursor(0, 1, 0), 'mapping-1', 'peer-'+identifier)
    return replace(initial, **values)


def refs():
    return (Reference(64497, 'IR', 'ref-row-1'), Reference(64498, 'US', 'ref-row-2'))


def test_same_asn_distinct_endpoints_addpath_and_all_origins():
    a = route()
    b = route('r2', endpoint=replace(a.endpoint, remote_ip='192.0.2.3'), origin=64498)
    c = route('r3', path_id=5, origin=None)
    cohort, = freeze_cohorts((incident(),), baseline(), iter([a, b, c]), refs())
    assert cohort.direction_count == 2
    assert {origin for _, origin, _ in cohort.origins} == {64497, 64498, None}
    assert len(cohort.routes) == 3 and cohort.baseline_kind == 'pre_onset'
    assert cohort == freeze_cohorts((incident(),), baseline(), [a, b, c], reversed(refs()))[0]


def test_ambiguous_mapping_preserved_not_a_peer_and_unknown_retained():
    a = route(mapping_state='unmapped')
    b = route('r2', prefix='203.0.113.0/24', origin=None)
    cohort, = freeze_cohorts((incident(),), baseline(), [a, b], refs())
    assert cohort.direction_count is None and cohort.known_direction_count == 0
    assert cohort.routes == (a,) and cohort.excluded_unknown == (b,)
    assert 'ambiguous_baseline_mapping' in cohort.reasons


def test_empty_and_missing_baseline_do_not_forge_denominator():
    empty, = freeze_cohorts((incident('US'),), baseline(), [route()], refs())
    assert empty.state == 'empty' and empty.direction_count == 0
    missing, = freeze_cohorts((incident(),), None, iter(()), refs())
    assert missing.state == 'baseline_unavailable' and missing.direction_count is None


def test_strict_seconds_boundary_no_later_fallback_and_exact_cursor():
    before = baseline()
    same_second = replace(before, baseline_id='same-second', at=Time(15, 0), cursor=Cursor(0, 10, 0))
    assert select_baseline(incident(), [same_second, before]) == before
    assert select_baseline(incident(), [same_second]) is None
    with pytest.raises(ValueError):
        freeze_cohorts((incident(),), same_second, [], refs())
    exact = replace(incident(), onset_cursor=Cursor(0, 11, 0))
    assert select_baseline(exact, [same_second, before]) == same_second
    pre_detection = replace(incident(), onset=None)
    assert freeze_cohorts((pre_detection,), before, [route()], refs())[0].baseline_kind == 'pre_detection'


def test_one_pass_multi_country_and_resource_guard():
    class Once:
        def __init__(self): self.used = False
        def __iter__(self):
            assert not self.used
            self.used = True
            yield route()
            yield route('r2', origin=64498, path_id=2)
    cohorts = freeze_cohorts((incident(), incident('US')), baseline(), Once(), refs())
    assert len(cohorts) == 2 and all(len(c.routes) == 2 for c in cohorts)
    with pytest.raises(ValueError, match='限额'):
        freeze_cohorts((incident(),), baseline(), [route()], refs(), max_routes=1)
    with pytest.raises(RuntimeError):
        freeze_cohorts((incident(),), baseline(), [], [], guard=lambda: (_ for _ in ()).throw(RuntimeError('stop')))


@pytest.mark.parametrize('rows', [[route(), route()], [route('later', prefix='203.0.113.0/24'), route()]])
def test_duplicate_and_out_of_order_input_rejected(rows):
    with pytest.raises(ValueError): freeze_cohorts((incident(),), baseline(), rows, refs())


def test_candidate_mixed_version_and_conflicting_references_rejected():
    with pytest.raises(ValueError): replace(binding(), state='candidate')
    with pytest.raises(ValueError):
        select_baseline(incident(), [baseline(), replace(baseline(), binding=replace(binding(), snapshot_id='other'))])
    with pytest.raises(ValueError): freeze_cohorts((incident(),), baseline(), [], [refs()[0], refs()[0]])


def test_local_baseline_cannot_select_country_and_duplicate_route_key_fails():
    local = route(local_message=True)
    c, = freeze_cohorts((incident(),), baseline(), [local], refs())
    assert not c.prefixes and c.excluded_unknown == (local,)
    with pytest.raises(ValueError, match='对象键'):
        freeze_cohorts((incident(),), baseline(), [route(), route('duplicate-key')], refs())
