"""独立复核0c613975的反例回归；expected不调用被测实现计算。"""
from dataclasses import replace
from fractions import Fraction
import pytest
from data_pipeline.analysis.country_events import freeze_cohorts
from data_pipeline.analysis.country_events.compute import Grid, ObservationFact, PrefixPoint, MetricPoint, Completion, compute_country_enhancement
from tests.country.test_country_enhancement_cohort import binding, baseline, incident, route, refs
from tests.country.test_country_enhancement_compute import paths, change


def run(cohorts, changes=(), sample=21):
    return [row for batch in compute_country_enhancement(
        cohorts, binding(), Grid(15000000, 31000000, (sample*1000000,)), changes, paths()
    ) for row in batch.rows]


def test_spec_p1_only_known_country_unknown_presence_is_not_empty_zero():
    unknown = route(presence='unknown')
    cohorts = freeze_cohorts((incident(),), baseline(), (unknown,), refs())
    c = cohorts[0]
    assert c.routes == (unknown,) and c.state != 'empty'
    assert c.direction_count is None and c.known_direction_count == 0
    rows = run(cohorts)
    assert [r.state for r in rows if isinstance(r, PrefixPoint)] == ['unknown']
    metrics = {r.metric: r for r in rows if isinstance(r, MetricPoint)}
    for name in ('normal_prefix_count', 'completely_interrupted_prefix_count',
                 'fixed_visible_ipv4_address_count', 'visible_direction_count'):
        assert metrics[name].value is None
        assert metrics[name].state == 'unknown'
    # 不把未知路由算成已知方向；其未知性也不污染没有候选成员的IPv6资源。
    assert metrics['visible_direction_count'].denominator is None
    assert metrics['normal_prefix_count'].denominator is None
    assert metrics['affected_asn_count'].denominator is None
    assert metrics['fixed_visible_ipv6_slash48_equivalent'].value == Fraction(0)


@pytest.mark.parametrize('sample', [21, 26])
def test_spec_p1_tail_object_key_corruption_rejected_for_either_grid(sample):
    c = freeze_cohorts((incident(),), baseline(), [route()], refs())
    malformed = change(0, 23, prefix='203.0.113.0/24')
    with pytest.raises(ValueError, match='对象ID'):
        run(c, [malformed], sample)


def test_spec_p1_tail_facts_are_grid_independent_but_right_edge_excluded():
    c = freeze_cohorts((incident(),), baseline(), [route()], refs())
    tail = change(0, 23, presence='absent', path_ref=None, local_message=True)
    right = change(1, 31, prefix='203.0.113.0/24')
    facts = []
    for sample in (21, 26):
        rows = run(c, [tail, right], sample)
        facts.append([r for r in rows if isinstance(r, ObservationFact)])
        assert sum(isinstance(r, Completion) for r in rows) == 1
    assert facts[0] == facts[1]
    assert [f.reference for f in facts[0]] == ['observation-r1', 'update-0']
    assert facts[0][-1].local is True


def test_spec_p2_baseline_local_preserved_without_received_state():
    local = route('local', local_message=True, presence='present')
    c = freeze_cohorts((incident(),), baseline(), [route(), local], refs())
    rows = run(c)
    assert {f.reference for f in rows if isinstance(f, ObservationFact)} == {'observation-r1', 'observation-local'}
    assert any(f.local for f in rows if isinstance(f, ObservationFact))
    assert [p.expected for p in rows if isinstance(p, PrefixPoint)] == [1]


@pytest.mark.parametrize('origin,local', [(64497, False), (None, False), (64498, False), (64497, True)])
@pytest.mark.parametrize('sample', [21, 26])
def test_spec_p2_all_applicable_dynamic_paths_checked_before_and_after_last_sample(origin, local, sample):
    c = freeze_cohorts((incident(),), baseline(), [route()], refs())
    new = route('new', prefix='203.0.113.0/24', origin=origin, path_ref='missing', local_message=local)
    with pytest.raises(ValueError, match='路径缺'):
        run(c, [change(0, 23, new)], sample)


def test_spec_p2_excluded_baseline_local_path_also_checked():
    local = route('local', local_message=True, path_ref='missing')
    c = freeze_cohorts((incident(),), baseline(), [route(), local], refs())
    with pytest.raises(ValueError, match='路径缺'):
        run(c)


def test_standards_p2_unsupported_cohort_rule_rejected():
    c = freeze_cohorts((incident(),), baseline(), [route()], refs())[0]
    with pytest.raises(ValueError, match='cohort规则'):
        run((replace(c, rule_version='country-enhancement/unsupported-v1'),))
