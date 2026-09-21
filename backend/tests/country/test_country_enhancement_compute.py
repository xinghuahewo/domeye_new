from dataclasses import replace
from fractions import Fraction
import pytest
from data_pipeline.analysis.country_events import freeze_cohorts, Cursor, Time, Reference
from data_pipeline.analysis.country_events.compute import Change, Segment, Path, Grid, PrefixPoint, AsnPoint, MetricPoint, NewPrefixPoint, PathSample, PathSummary, WindowClass, Peak, Quality, Completion, compute_country_enhancement
from tests.country.test_country_enhancement_cohort import binding, baseline, incident, route, refs


def paths():
    return [Path('p1', (Segment('sequence', (64496, 64497, 64498)),), b'raw',
                 (Segment('sequence', (64497, 64498)),), 'relationship-row-1')]


def change(n, at, original=None, **kw):
    r = original or route()
    stamp = at if isinstance(at, Time) else Time(at, 0)
    cursor = Cursor(1, n, 0)
    after = replace(r, observed_at=stamp, cursor=cursor, observation_ref=f'update-{n}', **kw)
    return Change(cursor, stamp, 'u1', f'update-{n}', after)


def execute(rows=(), seeds=None, samples=(21, 26, 31), **kw):
    cohorts = freeze_cohorts((incident(),), baseline(), seeds or [route()], refs())
    batches = list(compute_country_enhancement(cohorts, binding(), Grid(15000000, 31000000, tuple(t*1000000 for t in samples)),
                                             iter(rows), paths(), **kw))
    return [r for b in batches for r in b.rows]


def select(rows, cls):
    return [r for r in rows if isinstance(r, cls)]


def metric(rows, name):
    return [r for r in rows if isinstance(r, MetricPoint) and r.metric == name]


def test_aw_a_matrix_independent_peaks_and_fact_unknown():
    rows = execute([change(0, 23, presence='absent', path_ref=None), change(1, 28)])
    assert [p.state for p in select(rows, PrefixPoint)] == ['normal', 'complete', 'normal']
    assert [p.state for p in select(rows, AsnPoint) if p.afi == 0] == ['normal', 'route_interrupted', 'normal']
    assert select(rows, WindowClass)[0].classification == 'route_interrupted'
    peaks = {p.metric: p for p in select(rows, Peak)}
    assert peaks['normal_prefix_count'].first_sample_us == 21000000
    assert peaks['normal_prefix_count'].occurrence_count == 2
    assert peaks['completely_interrupted_prefix_count'].first_sample_us == 26000000
    assert all(m.fact_value is None and m.fact_state == 'unknown' for m in select(rows, MetricPoint))
    assert select(rows, Completion)[0].end_state == 'unknown'


def test_addpath_partial_withdraw_does_not_expand_direction_denominator():
    a = route(path_id=1); b = route('r2', path_id=2)
    rows = execute([change(0, 23, a, presence='absent', path_ref=None)], [a, b])
    assert all(p.state == 'normal' and p.expected == 1 for p in select(rows, PrefixPoint))
    assert all(m.value == 0 for m in metric(rows, 'invisible_direction_count'))


def test_two_endpoints_partial_unknown_and_scoped_recovery():
    a = route(); b = route('r2', endpoint=replace(a.endpoint, remote_ip='192.0.2.4'))
    invalidation = Change(Cursor(1, 0, 0), Time(22, 0), 'u1', 'state-down', invalidated_objects=('r1',))
    rows = execute([invalidation, change(1, 28, b)], [a, b])
    assert [p.state for p in select(rows, PrefixPoint)] == ['normal', 'unknown', 'unknown']
    assert select(rows, PrefixPoint)[-1].present == 1
    assert 'state-down' in select(rows, PrefixPoint)[-1].evidence_refs
    assert metric(rows, 'invisible_direction_count')[-1].value is None
    # 另一个方向仍支持该prefix的资源并集；A连续性仍unknown。
    assert metric(rows, 'fixed_visible_ipv4_address_count')[-1].known_lower_bound == 256


def test_unknown_scope_gap_keeps_subsets_but_totals_null():
    gap = Change(Cursor(1, 0, 0), Time(22, 0), 'u1', 'gap-1', gap=True)
    rows = execute([gap, change(1, 28)])
    assert metric(rows, 'fixed_visible_ipv4_address_count')[-1].value is None
    assert metric(rows, 'fixed_visible_ipv4_address_count')[-1].known_lower_bound == 256
    assert select(rows, Quality)[0].code == 'source_gap'


def test_local_excluded_and_new_prefix_separate_from_cohort():
    new = route('r-new', prefix='203.0.113.0/24')
    local = replace(change(0, 22, presence='absent', path_ref=None), local=True)
    rows = execute([local, change(1, 23, new)])
    assert all(p.state == 'normal' and p.expected == 1 for p in select(rows, PrefixPoint))
    assert metric(rows, 'new_cumulative_ipv4_prefix_count')[-1].value == 1
    assert select(rows, NewPrefixPoint)[0].first_observation_ref == 'update-1'
    assert metric(rows, 'fixed_visible_ipv4_address_count')[-1].value == 256


def test_unknown_new_origin_not_assigned_to_country():
    new = route('unknown', prefix='203.0.113.0/24', origin=None)
    rows = execute([change(0, 22, new)])
    assert not select(rows, NewPrefixPoint)
    assert metric(rows, 'new_cumulative_ipv4_prefix_count')[-1].value == 0


def test_ipv4_union_moas_and_ipv6_exact_fraction():
    a = route(prefix='198.51.100.0/24')
    b = route('r2', prefix='198.51.100.0/25', origin=64498)
    # MOAS前缀有明确IR起源，同时保留US关系。
    c = route('r3', prefix='198.51.100.0/25', path_id=3)
    d = route('r4', prefix='2001:db8::/49', endpoint=replace(a.endpoint, afi=2))
    rows = execute([], [a, b, c, d])
    assert metric(rows, 'fixed_visible_ipv4_address_count')[0].value == 256
    assert metric(rows, 'fixed_visible_ipv6_slash48_equivalent')[0].value == Fraction(1, 2)
    assert {p.asn for p in select(rows, AsnPoint)} == {64497, 64498}


def test_paths_keep_raw_positions_and_historical_basis_after_withdrawal():
    rows = execute([change(0, 23, presence='absent', path_ref=None)])
    samples = select(rows, PathSample)
    assert samples[0].affected_position == 1 and samples[0].downstream_position == 2
    assert samples[0].path_basis == 'baseline' and samples[0].observed_at == Time(10, 0)
    assert samples[0].sample_us == 21000000 and samples[0].relationship_ref == 'relationship-row-1'
    summary = select(rows, PathSummary)[0]
    assert summary.concurrent_sample_count == 2
    assert summary.first_concurrent_us == 26000000
    assert summary.route_observation_count == 1


def test_path_set_not_sorted_into_ordered_relation_and_missing_path_fails():
    c = freeze_cohorts((incident(),), baseline(), [route()], refs())
    grid = Grid(15000000, 31000000, (21000000,))
    set_path = Path('p1', (Segment('sequence', (64497,)), Segment('set', (64498, 64499))), b'raw-set')
    rows = [r for b in compute_country_enhancement(c, binding(), grid, [], [set_path]) for r in b.rows]
    assert not select(rows, PathSample)
    with pytest.raises(ValueError, match='路径缺'):
        list(compute_country_enhancement(c, binding(), grid, [], []))


def test_boundary_microsecond_precision_and_right_exclusion():
    rows = execute([change(0, Time(21, 0), presence='absent', path_ref=None), change(1, Time(31, 0))])
    assert [p.state for p in select(rows, PrefixPoint)] == ['normal', 'complete', 'complete']
    rows = execute([change(0, Time(20, 999999), presence='absent', path_ref=None)])
    assert select(rows, PrefixPoint)[0].state == 'complete'
    c = freeze_cohorts((incident(),), baseline(), [route()], refs())
    with pytest.raises(ValueError, match='精度'):
        list(compute_country_enhancement(c, binding(), Grid(15000000, 31000000, (20500000,)),
                                        [change(0, Time(20), presence='absent')], paths()))


@pytest.mark.parametrize('rows', [
    [change(0, 22), change(1, 20)],
    [change(1, 22), change(0, 24)],
    [replace(change(0, 22), source_id='not-bound')],
])
def test_backwards_or_unbound_input_fails_without_completion(rows):
    with pytest.raises(ValueError): execute(rows)


def test_empty_cohort_zero_counts_null_ratio_and_missing_baseline():
    cohorts = freeze_cohorts((incident('US'),), baseline(), [route()], refs())
    grid = Grid(15000000, 31000000, (21000000,))
    rows = [r for b in compute_country_enhancement(cohorts, binding(), grid, [], paths()) for r in b.rows]
    assert metric(rows, 'normal_prefix_count')[0].value == 0
    assert metric(rows, 'normal_prefix_count')[0].ratio is None
    missing = freeze_cohorts((incident(),), None, [], refs())
    rows = [r for b in compute_country_enhancement(missing, binding(), grid, [], []) for r in b.rows]
    assert not select(rows, MetricPoint)
    assert select(rows, Quality)[0].code == 'baseline_unavailable'


def test_single_pass_batch_size_and_stable_output():
    class Once:
        def __iter__(self):
            assert not getattr(self, 'used', False)
            self.used = True
            yield change(0, 22)
    cohorts = freeze_cohorts((incident(), incident('US')), baseline(), [route(), route('r2', origin=64498, path_id=2)], refs())
    grid = Grid(15000000, 31000000, (21000000, 26000000))
    batches = list(compute_country_enhancement(cohorts, binding(), grid, Once(), paths(), batch_rows=3))
    assert all(len(b.rows) <= 3 for b in batches)
    assert len([r for b in batches for r in b.rows if isinstance(r, Completion)]) == 2
    assert execute([], batch_rows=1) == execute([], batch_rows=1000)
    with pytest.raises(ValueError): execute([change(0, 22)], max_objects=1)


def test_event_end_and_unknown_carry_in_are_not_fake_recovery():
    i = replace(incident(), end=Time(26))
    c = freeze_cohorts((i,), baseline(), [route()], refs())
    grid = Grid(15000000, 31000000, (21000000, 26000000, 31000000))
    rows = [r for b in compute_country_enhancement(c, binding(), grid, [], paths()) for r in b.rows]
    receipt = select(rows, Completion)[0]
    assert receipt.sample_count == 2 and receipt.data_through_us == 26000000
    assert receipt.left_censored is True and receipt.end_state == 'known'


def test_new_direction_does_not_inflate_fixed_denominator_and_keeps_mapping():
    from data_pipeline.analysis.country_events.compute import NewDirectionPoint
    a = route()
    extra = route('new-endpoint', endpoint=replace(a.endpoint, local_ip='192.0.2.9'), mapping_state='ambiguous')
    rows = execute([change(0, 22, extra)])
    assert select(rows, PrefixPoint)[-1].expected == 1
    assert metric(rows, 'visible_direction_count')[-1].denominator == 1
    side = select(rows, NewDirectionPoint)[0]
    assert side.mapping_state == 'ambiguous'
    assert side.semantics == 'outside_fixed_denominator_not_new_peer_claim'


def test_new_prefix_withdrawal_keeps_cumulative_but_not_visible():
    new = route('new', prefix='203.0.113.0/24')
    rows = execute([change(0, 22, new), change(1, 28, new, presence='absent', path_ref=None)])
    assert metric(rows, 'new_visible_ipv4_prefix_count')[-1].value == 0
    assert metric(rows, 'new_cumulative_ipv4_prefix_count')[-1].value == 1
    assert select(rows, NewPrefixPoint)[-1].state == 'absent'


def test_partial_asn_classification_and_unknown_not_interrupted():
    a = route(); b = route('r2', endpoint=replace(a.endpoint, remote_ip='192.0.2.8'))
    rows = execute([change(0, 22, a, presence='absent', path_ref=None)], [a, b])
    assert select(rows, PrefixPoint)[-1].state == 'partial'
    assert metric(rows, 'affected_asn_count')[-1].value == 1
    assert metric(rows, 'route_interrupted_asn_count')[-1].value == 0
    assert select(rows, PathSummary)[0].peak_concurrent_prefix_count == 1
    assert select(rows, PathSummary)[0].peak_concurrent_ipv4_addresses == 256


def test_path_repetitions_keep_positions_and_as4_input_unmodified():
    c = freeze_cohorts((incident(),), baseline(), [route()], refs())
    p = Path('p1', (Segment('sequence', (64497, 64497, 64498)),), b'raw-original',
             (Segment('set', (64497, 64498)),))
    rows = [r for b in compute_country_enhancement(c, binding(), Grid(15000000, 31000000, (21000000,)), [], [p]) for r in b.rows]
    assert {s.affected_position for s in select(rows, PathSample)} == {0, 1}
    assert p.raw_attributes == b'raw-original' and p.as4_segments[0].kind == 'set'


def test_batch_byte_guard_and_no_completion_after_error():
    with pytest.raises(ValueError, match='字节'):
        execute([], batch_bytes=1)
    c = freeze_cohorts((incident(),), baseline(), [route()], refs())
    iterator = compute_country_enhancement(c, binding(), Grid(15000000, 31000000, (21000000, 26000000)),
                                           [change(0, 22), change(1, 20)], paths(), batch_rows=1)
    output = []
    with pytest.raises(ValueError):
        for b in iterator: output.extend(b.rows)
    assert not select(output, Completion)


def test_source_scoped_gap_and_unknown_baseline_denominator():
    a = route(); b = route('r2', endpoint=replace(a.endpoint, remote_ip='192.0.2.8'))
    gap = Change(Cursor(1, 0, 0), Time(22, 0), 'u1', 'scoped-gap', invalidated_objects=('r1',), gap=True)
    rows = execute([gap, change(1, 28, a)], [a, b])
    assert [p.state for p in select(rows, PrefixPoint)] == ['normal', 'unknown', 'normal']
    cohort, = freeze_cohorts((incident(),), baseline(), [a, replace(b, presence='unknown')], refs())
    assert cohort.direction_count is None and 'baseline_visibility_unknown' in cohort.reasons


def test_raw_local_and_unknown_observations_are_retained_as_facts():
    from data_pipeline.analysis.country_events.compute import ObservationFact
    local = change(0, 22, presence='absent', local_message=True)
    unknown = change(1, 23, route('unknown', prefix='203.0.113.0/24', origin=None))
    rows = execute([local, unknown])
    facts = select(rows, ObservationFact)
    assert any(f.local and f.route.presence == 'absent' for f in facts)
    assert any(f.route.origin is None for f in facts)
    assert select(rows, PrefixPoint)[-1].state == 'normal'


def test_mixed_collector_and_change_limits_rejected():
    wrong = change(0, 22, route(endpoint=replace(route().endpoint, collector='rrc00')))
    with pytest.raises(ValueError, match='Collector'): execute([wrong])
    with pytest.raises(ValueError, match='输入变化'): execute([change(0, 22), change(1, 23)], max_changes=1)


def test_different_baseline_groups_are_explicitly_rejected():
    a = freeze_cohorts((incident(),), baseline(), [route()], refs())[0]
    b = freeze_cohorts((incident('US'),), replace(baseline(), baseline_id='other'), [route()], refs())[0]
    with pytest.raises(ValueError, match='同一基线'):
        list(compute_country_enhancement((a, b), binding(), Grid(15000000, 31000000, (21000000,)), [], paths()))


def test_mapping_ambiguity_only_nulls_affected_address_family_resource():
    a = route(mapping_state='unmapped')
    v6 = route('v6', prefix='2001:db8::/49', endpoint=replace(a.endpoint, afi=2))
    rows = execute([], [a, v6])
    assert metric(rows, 'fixed_visible_ipv4_address_count')[0].value is None
    assert metric(rows, 'fixed_visible_ipv6_slash48_equivalent')[0].value == Fraction(1, 2)
