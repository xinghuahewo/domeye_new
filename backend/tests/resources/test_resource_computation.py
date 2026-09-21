from datetime import datetime, timedelta, timezone

import pytest

from data_pipeline.analysis.resources import ResourceComputer, ReferenceAs, RibContext, RibElement

START = datetime(2026, 2, 28, tzinfo=timezone.utc)


def context(hour=0):
    return RibContext('rib', 'rrc25', START + timedelta(hours=hour), 'ref-v1', 'fixture_epoch')


def element(prefix='1.0.0.0/24', path='9808 100', peer='200', ordinal=0, **kw):
    return RibElement('rib', f'm{ordinal}', ordinal, prefix, path, peer, **kw)


def by_bucket(rows):
    return {r.bucket: r for r in rows}


def test_independent_dimensions_units_and_retained_sets():
    result = ResourceComputer({}).compute(context(), [
        element('1.0.0.0/23', peer_ip='1.1.1.1'),
        element('1.0.0.0/24', peer_ip='1.1.1.2', ordinal=1),
        element('1.0.1.128/25', '4837 101', ordinal=2),
        element('2001:db8::/47', '4134 102', ordinal=3),
        element('2001:db8::/64', '300 103', peer='201', ordinal=4),
    ])
    rows, vps = by_bucket(result.rows), by_bucket(result.vp_rows)
    assert set(rows) == {'global', '9808', '4837', '4134'}
    assert set(vps) == {'200', '201'}
    g = rows['global']
    assert (g.ipv4_prefix_count, g.ipv4_address_count, g.ipv6_prefix_count) == (3, 512, 2)
    assert (g.path_count, g.public_as_count, g.vp_count) == (4, 4, 4)
    assert g.vp_set == {'9808', '4837', '4134', '300'}
    assert vps['200'].ipv4_prefix_count == 3
    assert rows['9808'].ipv4_prefix_count == 2
    assert len(result.decisions) == 5
    assert result.decisions[1].observation.peer_ip == '1.1.1.2'


@pytest.mark.parametrize('tail,private', [('64511', False), ('64512', True), ('65535', True), ('65536', False), ('4200000000', False), ('4294967294', False), ('4294967295', False), ('4294967296', True), ('0', False), ('-1', False), ('23456', False)])
def test_legacy_tail_is_not_standard_origin(tail, private):
    result = ResourceComputer({}).compute(context(), [element(path=f'9808 {tail}')])
    row = result.rows[0]
    assert row.private_as == ({tail} if private else set())
    assert row.public_as == (set() if private else {tail})
    assert result.decisions[0].observation.attributed_origin is None


def test_as_set_invalid_default_filter_order():
    result = ResourceComputer({}).compute(context(), [
        element(path='9808 {100,101}'), element('2.0.0.0/24', '9808 bad', ordinal=1),
        element('3.0.0.1/24', '4837 100', ordinal=2),
        element('0.0.0.0/0', ordinal=3), element('::/0', ordinal=4),
        element('4.0.0.0/24', ordinal=5, action='STATE'),
    ])
    rows = by_bucket(result.rows)
    assert rows['global'].ipv4_prefix == {'1.0.0.0/24', '2.0.0.0/24'}
    assert rows['global'].path == set()
    assert rows['global'].vp_set == {'9808'}
    assert rows['4837'].ipv4_prefix_count == 0  # 旧代码验证 prefix 前建桶
    assert [d.resource_status for d in result.decisions] == ['as_set_after_prefix', 'invalid_tail_after_prefix', 'invalid_prefix', 'default_or_state', 'default_or_state', 'default_or_state']


@pytest.mark.parametrize('rank,expected', [(None, None), ('', None), ('bad', None), ('nan', None), ('inf', None), ('12.9', 12)])
def test_reference_rank_and_null_name(rank, expected):
    result = ResourceComputer({'200': ReferenceAs(None, rank, raw_row_ref='ref:row:1')}).compute(context(), [element()])
    assert result.vp_rows[0].as_rank == expected
    assert result.vp_rows[0].as_name is None
    assert result.vp_rows[0].reference_row_ref == 'ref:row:1'


def test_first_six_seventh_outlier_chain_and_export_isolation():
    computer = ResourceComputer({})
    for i in range(6):
        result = computer.compute(context(i), [element()])
        band = result.state.normal_range['global']['ipv4_prefix_count']
        assert (band.list_len, band.lower_bound, band.upper_bound) == (i + 1, 0, 1)
    result = computer.compute(context(6), [element(), element('2.0.0.0/24', ordinal=1)])
    assert result.rows[0].is_outlier
    assert result.state.currently_abnormal['global']
    assert result.state.normal_range['global']['ipv4_prefix_count'].list_len == 6
    result = computer.compute(context(7), [element()])
    assert not result.rows[0].is_outlier
    assert result.state.normal_range['global']['ipv4_prefix_count'].list_len == 6
    result.state.normal_range.clear()
    assert computer.export_state().normal_range


def test_late_bucket_has_no_bounds_after_startup():
    computer = ResourceComputer({})
    for i in range(6):
        computer.compute(context(i), [])
    result = computer.compute(context(6), [element(path='4837 100')])
    assert result.state.normal_range['4837']['ipv4_prefix_count'].upper_bound is None
    assert result.state.vp_normal_range['200']['ipv4_prefix_count'].upper_bound is None
    assert result.state.initial_state_basis == 'cold_start'
    assert by_bucket(result.rows)['4837'].legacy_prefix_insert_ready is False


def test_cleanup_after_selection_and_list_len_never_falls():
    computer = ResourceComputer({})
    for i in range(7):
        computer.compute(context(i), [element()])
    result = computer.compute(context(100), [element()])
    assert result.state.normal_range['global']['ipv4_prefix_count'].list_len == 7
    assert set(result.state.prefix_count_dict['global']) == {context(100).snapshot_time}
    result = computer.compute(context(101), [element()])
    assert result.state.normal_range['global']['ipv4_prefix_count'].list_len == 7


def test_math_population_std_integer_truncation():
    computer = ResourceComputer({})
    computer.compute(context(0), [element()])
    result = computer.compute(context(1), [element(), element('2.0.0.0/24', ordinal=1), element('3.0.0.0/24', ordinal=2)])
    band = result.state.normal_range['global']['ipv4_prefix_count']
    # 独立预期：[1,3] mean=2、总体std=1；int(6.0)=6、int(-0.8)=0。
    assert (band.upper_bound, band.lower_bound) == (6, 0)


def test_topology_private_bridge_dedup_missing_and_default_prefix():
    refs = {str(i): ReferenceAs(country_cn='甲') for i in (100, 101, 102)}
    result = ResourceComputer(refs, topology_enabled=True).compute(context(), [
        element(path='100 100 64512 101 100'),
        element(path='101 4200000000 100', ordinal=1),
        element(path='100 999 102', ordinal=2),
        element('0.0.0.0/0', path='101 102', ordinal=3),
        element(path='100 {101} 102', ordinal=4),
    ])
    graph = result.topology[0]
    assert graph.edges == ((100, 101), (101, 102))
    assert graph.weight == 1
    assert graph.legacy_replace_edges and graph.legacy_update_snapshot
    assert result.topology[1].status == 'missing_reference'


def test_topology_empty_disabled_and_large_no_stale_snapshot():
    refs = {str(i): ReferenceAs(country_cn='甲') for i in range(1, 50003)}
    computer = ResourceComputer(refs, topology_enabled=True)
    empty = computer.compute(context(), [])
    assert empty.topology[0].status == 'no_edges'
    assert not empty.topology[0].legacy_replace_edges
    large = computer.compute(context(1), [element(path=' '.join(refs))])
    assert large.topology[0].status == 'graph_too_large'
    assert len(large.topology[0].edges) == 50001
    assert not large.topology[0].legacy_update_snapshot
    assert ResourceComputer({}).compute(context(), []).topology[0].status == 'not_calculated'


def test_failure_does_not_advance_and_duplicate_time_rejected():
    computer = ResourceComputer({})
    wrong = RibElement('other', 'm', 0, '1.0.0.0/24', '1 2', '1')
    with pytest.raises(ValueError):
        computer.compute(context(), [element(), wrong])
    assert computer.export_state().rib_count == 0
    computer.compute(context(), [element()])
    with pytest.raises(ValueError):
        computer.compute(context(), [element()])
    assert computer.export_state().rib_count == 1
    with pytest.raises(ValueError):
        computer.compute(RibContext('rib2', 'rrc26', context(1).snapshot_time, 'ref', 'fixture'), [])
    with pytest.raises(ValueError):
        ResourceComputer({}).compute(RibContext('rib', 'rrc25', datetime(2026, 2, 28), 'ref', 'fixture'), [])


def test_one_metric_outlier_excludes_entire_historical_row():
    computer = ResourceComputer({})
    for i in range(6):
        computer.compute(context(i), [element()])
    # 前缀数仍1，但多一条路径使整体 outlier，下一轮前缀样本也排除该时点。
    abnormal = computer.compute(context(6), [element(), element(path='9808 101', ordinal=1)])
    assert abnormal.rows[0].ipv4_prefix_count == 1
    assert abnormal.rows[0].is_outlier
    result = computer.compute(context(7), [element()])
    samples = result.state.normal_range['global']['ipv4_prefix_count'].samples
    assert len(samples) == 6
    assert context(6).snapshot_time not in {s.time for s in samples}


def test_retained_band_samples_survive_three_day_cleanup():
    computer = ResourceComputer({})
    for i in range(7):
        computer.compute(context(i), [element()])
    result = computer.compute(context(100), [element()])
    band = result.state.normal_range['global']['ipv4_prefix_count']
    assert len(band.samples) == 7
    assert band.mean == 1 and band.population_std == 0
    assert band.samples[0].time == START


def test_topology_threshold_exact_and_special_asn_filter():
    refs = {str(i): ReferenceAs(country_cn='甲') for i in range(1, 50002)}
    result = ResourceComputer(refs, topology_enabled=True).compute(context(), [element(path=' '.join(refs))])
    assert len(result.topology[0].edges) == 50000
    assert result.topology[0].legacy_update_snapshot
    special = ('64511', '64512', '65535', '65536', '4200000000', '4294967294', '4294967295', '4294967296', '0', '-1')
    refs = {x: ReferenceAs(country_cn='甲') for x in special}
    result = ResourceComputer(refs, topology_enabled=True).compute(context(), [element(path=' '.join(special))])
    assert result.topology[0].nodes == (-1, 0, 64511, 65536, 4294967295, 4294967296)


def test_space_rendering_and_topology_invalid_token_bridging():
    refs = {'100': ReferenceAs(country_cn='甲'), '101': ReferenceAs(country_cn='甲')}
    result = ResourceComputer(refs, topology_enabled=True).compute(context(), [element(path='100  bad 1_2 101')])
    assert result.topology[0].edges == ((100, 101),)
    assert result.rows[0].path == {'100  bad 1_2 101'}
    assert result.rows[0].public_as == {'101'}
