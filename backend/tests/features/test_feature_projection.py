"""固定观察Arrow行上的私有投影集成；无数据库/旧代码执行。"""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from copy import deepcopy
import json
import pyarrow as pa
import pytest

from data_pipeline.analysis.features.calculation import FileWindow, Reference
from data_pipeline.analysis.features.projection import FeatureAdapter, FeaturePlan, SourceBinding

T = datetime(2026, 2, 28, 15, 50, tzinfo=timezone.utc)
REF = Reference('ref-v1', {'1': '伊朗', '2': '美国', '3': '伊朗', '4200000000': '伊朗'},
                {'1': 'IR', '2': 'US', '3': 'IR', '4200000000': 'IR'}, {'美国': 'US'})


def binding(n, count, source=None):
    source = source or ['z-baseline', 'b-update', 'a-update', 'c-update'][n]
    return SourceBinding(source, 'a'*64, 'baseline' if n == 0 else 'update',
        FileWindow('observations-v1', source, T + timedelta(minutes=5*n),
                   T + timedelta(minutes=5*(n+1)), T + timedelta(minutes=5*n), 'complete'), count)


def saved_row(b, n, action='announce', path='100 1', prefix='10.0.0.0/24', vp=100):
    return {'source_id': b.source_id, 'content_sha256': b.content_sha256, 'record': n, 'ordinal': 0,
            'epoch': int(b.window.start.timestamp()), 'microsecond': 0, 'event_id': f'{b.source_id}:{n}:0',
            'action': action, 'prefix': prefix, 'peer_asn': vp, 'peer_ip': '192.0.2.1',
            'as_path_text': path, 'path_key': f'path:{path}', 'as_path_raw': b'fixture', 'raw_prefix': b'fixture'}


def batches(rows):
    if rows:
        yield pa.RecordBatch.from_pylist(rows)


def output(result, scope, subject):
    return next(r for r in result.rows if r.scope == scope and r.subject == subject)


def test_ordinary_and_ir_same_saved_inputs_independent_cross_window_paths():
    sources = (binding(0, 1), binding(1, 1), binding(2, 2), binding(3, 0))
    plan = FeaturePlan('observations-v1', 'rrc25', sources)
    ordinary, ir = [FeatureAdapter(mode, REF, plan, 'e') for mode in ('ordinary', 'ir')]
    initial = [saved_row(sources[0], 0, 'rib_snapshot')]
    for adapter in (ordinary, ir):
        assert adapter.consume_source(sources[0], batches(initial)) is None
    frozen = ordinary.state.projection
    before_export = ordinary.state.export()
    announcement = [saved_row(sources[1], 0, path='100 2')]
    first = ordinary.consume_source(sources[1], batches(announcement))
    ir_first = ir.consume_source(sources[1], batches(announcement))
    assert output(first, 'asn', '2').values.announ_num == 1
    assert first.sparse_asns == (('伊朗', '1'),)
    assert set(ordinary.state.projection.prefix_as['10.0.0.0/24']) == {'1', '2'}
    assert ir.state.projection.prefix_dict['10.0.0.0/24']['100'] == '100 1'
    assert output(ir_first, 'collect', 'collect').values.announ_num == 0
    withdrawals = [saved_row(sources[2], 0, 'withdraw', path=''),
                   saved_row(sources[2], 1, 'withdraw', path='', vp=200)]
    second = ordinary.consume_source(sources[2], batches(withdrawals))
    ir_second = ir.consume_source(sources[2], batches(withdrawals))
    assert output(second, 'asn', '2').values.withdraw_num == 1
    assert set(ordinary.state.projection.as_prefix['1']) == {'10.0.0.0/24'}  # 历史残留仍在
    assert output(ir_second, 'asn', '1').values.withdraw_num == 1
    assert output(ir_second, 'asn', '1').row_presence == 'ir_asn_write_disabled'
    assert output(ir_second, 'collect', 'collect').values.withdraw_num == 2
    assert not ir.state.projection.as_prefix
    last = ordinary.consume_source(sources[3], batches([]))
    assert all(r.scope != 'asn' for r in last.rows)
    assert frozen.prefix_dict['10.0.0.0/24']['100'] == '100 1'
    assert before_export['projection']['as_prefix'] == {'1': ['10.0.0.0/24']}
    json.dumps(ordinary.state.export())
    assert deepcopy(frozen.prefix_dict) is frozen.prefix_dict
    with pytest.raises(TypeError):
        frozen.prefix_dict['10.0.0.0/24']['100'] = 'changed'


def test_ir_two_distinct_origin_filters_count_without_projection_change():
    sources = (binding(0, 1), binding(1, 1))
    adapter = FeatureAdapter('ir', REF, FeaturePlan('observations-v1', 'rrc25', sources), 'e')
    adapter.consume_source(sources[0], batches([saved_row(sources[0], 0, 'rib_snapshot')]))
    result = adapter.consume_source(sources[1], batches([saved_row(sources[1], 0, path='2 4200000000')]))
    assert output(result, 'asn', '4200000000').values.announ_num == 1
    assert adapter.state.projection.prefix_dict['10.0.0.0/24']['100'] == '100 1'
    assert adapter.last_audit[0]['skip'] is None
    assert adapter.last_audit[0]['projection_skip'] == 'non_ir_bgprib_origin'


def test_rib_and_update_filters_differ_cross_vp_retains_origin():
    sources = (binding(0, 5), binding(1, 4))
    adapter = FeatureAdapter('ordinary', REF, FeaturePlan('observations-v1', 'rrc25', sources), 'e')
    initial = [saved_row(sources[0], 0, 'rib_snapshot'), saved_row(sources[0], 1, 'rib_snapshot', vp=200),
               saved_row(sources[0], 2, 'rib_snapshot', prefix='0.0.0.0/7'),
               saved_row(sources[0], 3, 'rib_snapshot', prefix='0.0.0.0/0'),
               saved_row(sources[0], 4, 'rib_snapshot', prefix='12.0.0.0/24', path='')]
    adapter.consume_source(sources[0], batches(initial))
    assert '0.0.0.0/7' in adapter.state.projection.as_prefix['1']
    assert '0.0.0.0/0' not in adapter.state.projection.prefix_dict
    updates = [saved_row(sources[1], 0, 'withdraw', path=''),
               saved_row(sources[1], 1, prefix='0.0.0.0/7', path='100 2'),
               saved_row(sources[1], 2, prefix='::/15'), saved_row(sources[1], 3, prefix='bad')]
    result = adapter.consume_source(sources[1], batches(updates))
    assert set(adapter.state.projection.prefix_dict['10.0.0.0/24']) == {'200'}
    assert '10.0.0.0/24' in adapter.state.projection.as_prefix['1']
    assert output(result, 'collect', 'collect').values.announ_num == 0
    assert [a['skip'] for a in adapter.last_audit[1:]] == ['oversized_ipv4_prefix', 'oversized_ipv6_prefix', 'invalid_prefix']


@pytest.mark.parametrize('fault', ['source', 'content', 'order', 'time', 'count', 'path'])
def test_source_failure_preserves_prior_frozen_state(fault):
    sources = (binding(0, 1), binding(1, 2))
    adapter = FeatureAdapter('ordinary', REF, FeaturePlan('observations-v1', 'rrc25', sources), 'e')
    adapter.consume_source(sources[0], batches([saved_row(sources[0], 0, 'rib_snapshot')]))
    before = adapter.state.export()
    rows = [saved_row(sources[1], 0), saved_row(sources[1], 1)]
    if fault == 'source': rows[1]['source_id'] = 'wrong'
    if fault == 'content': rows[1]['content_sha256'] = 'wrong'
    if fault == 'order': rows[1]['record'] = 0
    if fault == 'time': rows[1]['epoch'] = int(sources[1].window.end.timestamp())
    if fault == 'count': rows.pop()
    if fault == 'path': rows[1]['as_path_text'] = '(1)'
    with pytest.raises(ValueError):
        adapter.consume_source(sources[1], batches(rows))
    assert adapter.state.export() == before and adapter.cursor == 1


def test_same_content_distinct_sources_count_twice_and_plan_rejects_reordering():
    sources = (binding(0, 0), binding(1, 1), binding(2, 1))
    adapter = FeatureAdapter('ordinary', REF, FeaturePlan('observations-v1', 'rrc25', sources), 'e')
    adapter.consume_source(sources[0], batches([]))
    with pytest.raises(ValueError, match='顺序'):
        adapter.consume_source(sources[2], batches([]))
    for b in sources[1:]:
        result = adapter.consume_source(b, batches([saved_row(b, 0)]))
        assert output(result, 'collect', 'collect').values.announ_num == 1


def test_public_parser_runs_once_before_both_adapters(tmp_path, monkeypatch):
    from tests.observations.test_observation_two_phase import fixture_manifest
    from data_pipeline.bgp.input.mrt_reader import read_source
    manifest = fixture_manifest(tmp_path)
    frozen_rows = {}
    bindings = []
    for n, entry in enumerate(manifest['inputs']):
        b = replace(binding(n, 1, entry['source_id']), content_sha256=entry['sha256'],
                    window=FileWindow('observations-v1', entry['source_id'], datetime(1970, 1, 1, tzinfo=timezone.utc),
                                      datetime(1970, 1, 2, tzinfo=timezone.utc), datetime(1970, 1, 1, tzinfo=timezone.utc), 'unknown'))
        # 测试只用RIB和一个UPDATE；允许文件原始时间相同，不构造重叠计算窗口。
        if n == 2: break
        rows = []
        for message in read_source(entry['path'], entry['sha256'], source_id=entry['source_id']):
            paths = {p['path_key']: p for p in message.paths}
            for e in message.elements:
                rows.append({**e, **paths[e['path_key']], 'peer_asn': e['peer']['asn'],
                    'source_id': message.source_id, 'content_sha256': message.content_sha256, 'record': message.record,
                    'epoch': message.epoch, 'microsecond': message.microsecond,
                    'event_id': f'{message.message_id}:{e["ordinal"]}'})
        if n == 1:
            b = replace(b, window=replace(b.window, start=datetime.fromtimestamp(min(row['epoch'] for row in rows), timezone.utc)))
        bindings.append(replace(b, expected_elements=len(rows)))
        frozen_rows[b.source_id] = rows
    import data_pipeline.bgp.input.mrt_reader as mrt
    monkeypatch.setattr(mrt, 'read_source', lambda *a, **k: pytest.fail('不允许二次解析MRT'))
    plan = FeaturePlan('observations-v1', 'rrc25', tuple(bindings))
    for mode in ('ordinary', 'ir'):
        adapter = FeatureAdapter(mode, REF, plan, 'e')
        for b in bindings:
            adapter.consume_source(b, batches(frozen_rows[b.source_id]))
        assert adapter.cursor == 2


def test_rib_private_as_set_ir_and_empty_announcement_rules():
    sources = (binding(0, 4), binding(1, 2))
    initial = [saved_row(sources[0], 0, 'rib_snapshot', path='1 4200000000'),
               saved_row(sources[0], 1, 'rib_snapshot', path='100 {1,2}', prefix='11.0.0.0/24'),
               saved_row(sources[0], 2, 'rib_snapshot', path='100 2', prefix='12.0.0.0/24'),
               saved_row(sources[0], 3, 'rib_snapshot', path='64512', prefix='13.0.0.0/24')]
    plan = FeaturePlan('observations-v1', 'rrc25', sources)
    ordinary, ir = [FeatureAdapter(mode, REF, plan, 'e') for mode in ('ordinary', 'ir')]
    ordinary.consume_source(sources[0], batches(initial))
    ir.consume_source(sources[0], batches(initial))
    assert set(ordinary.state.projection.as_prefix) == {'1', '{1,2}', '2', '64512'}
    assert set(ir.state.projection.as_prefix) == {'1'}
    updates = [saved_row(sources[1], 0, path='', prefix='14.0.0.0/24'),
               saved_row(sources[1], 1, path='100 {1,2}', prefix='15.0.0.0/24')]
    result = ordinary.consume_source(sources[1], batches(updates))
    assert set(ordinary.state.projection.as_prefix['']) == {'14.0.0.0/24'}
    assert output(result, 'collect', 'collect').values.announ_num == 2
    assert all(row.scope != 'asn' for row in result.rows)


def test_zero_element_messages_require_scope_quality_and_gap_stays_partial():
    sources = tuple(replace(binding(n, 0), message_quality_refs=(f'messages:{n}',), message_quality_state='complete')
                    for n in range(3))
    # 留一个明确文件缺口；不得把各文件的complete合成连续覆盖。
    sources = (*sources[:2], replace(sources[2], window=replace(sources[2].window,
        start=sources[2].window.start+timedelta(minutes=5), end=sources[2].window.end+timedelta(minutes=5))))
    adapter = FeatureAdapter('ordinary', REF, FeaturePlan('observations-v1', 'rrc25', sources), 'e')
    adapter.consume_source(sources[0], batches([]))
    first = adapter.consume_source(sources[1], batches([]))
    assert first.quality == 'complete'
    assert first.calculation_parameters['message_quality_refs'] == ('messages:1',)
    second = adapter.consume_source(sources[2], batches([]))
    assert second.quality == 'partial'
    assert output(second, 'collect', 'collect').resource_status == 'unknown'
    missing = (binding(0, 0), binding(1, 0))
    other = FeatureAdapter('ordinary', REF, FeaturePlan('observations-v1', 'rrc25', missing), 'e')
    other.consume_source(missing[0], batches([]))
    assert other.consume_source(missing[1], batches([])).quality == 'unknown'


def test_frozen_index_editor_cannot_mutate_published_bucket():
    from data_pipeline.analysis.features.projection import FrozenIndex
    edit = FrozenIndex().edit()
    edit['1'] = frozenset({'prefix'})
    snapshot = edit.freeze()
    with pytest.raises(RuntimeError):
        edit['1'] = frozenset()
    assert snapshot['1'] == {'prefix'}


def test_projection_version_changes_with_quality_evidence_not_execution():
    b = binding(0, 0)
    complete = replace(b, message_quality_refs=('same-snapshot:messages:0',), message_quality_state='complete')
    versions = []
    for source, execution in ((b, 'first'), (b, 'second'), (complete, 'first')):
        adapter = FeatureAdapter('ordinary', REF, FeaturePlan('observations-v1', 'rrc25', (source,)), execution)
        adapter.consume_source(source, batches([]))
        versions.append(adapter.state.projection.version)
    assert versions[0] == versions[1] and versions[0] != versions[2]


def test_late_baseline_record_cannot_seed_earlier_update_cutpoint():
    sources = (binding(0, 1), binding(1, 0))
    broad_baseline = replace(sources[0], window=replace(sources[0].window, end=sources[1].window.end))
    adapter = FeatureAdapter('ordinary', REF, FeaturePlan('observations-v1', 'rrc25', (broad_baseline, sources[1])), 'e')
    row = saved_row(broad_baseline, 0, 'rib_snapshot')
    row['epoch'] = int(sources[1].window.start.timestamp())+1
    with pytest.raises(ValueError, match='切点'):
        adapter.consume_source(broad_baseline, batches([row]))
    assert adapter.state is None and adapter.cursor == 0


@pytest.mark.parametrize('mode', ['ordinary', 'ir'])
@pytest.mark.parametrize('gap_minutes', [0, 10])
@pytest.mark.parametrize('rib_seconds', [0, 299])
def test_first_update_gap_uses_baseline_bounds_and_survives_empty_files(mode, gap_minutes, rib_seconds):
    baseline = replace(binding(0, 1), message_quality_refs=('baseline:messages',), message_quality_state='complete')
    first = replace(binding(1, 0), message_quality_refs=('first:messages',), message_quality_state='complete')
    first = replace(first, window=replace(first.window,
        start=first.window.start+timedelta(minutes=gap_minutes),
        end=first.window.end+timedelta(minutes=gap_minutes)))
    second = replace(binding(2, 0), message_quality_refs=('second:messages',), message_quality_state='complete')
    second = replace(second, window=replace(second.window, start=first.window.end,
                                             end=first.window.end+timedelta(minutes=5)))
    adapter = FeatureAdapter(mode, REF, FeaturePlan('observations-v1', 'rrc25', (baseline, first, second)), 'e')
    # 非空合法来源；默认路由仅被兼容投影过滤，不能借拒绝空基线掩盖缺口。
    rib = saved_row(baseline, 0, 'rib_snapshot', prefix='0.0.0.0/0')
    rib['epoch'] += rib_seconds
    adapter.consume_source(baseline, batches([rib]))
    assert not adapter.state.projection.prefix_dict
    assert adapter.last_audit[0]['skip'] == 'legacy_invalid_or_default_route'
    frozen = adapter.state.projection
    for source in (first, second):
        result = adapter.consume_source(source, batches([]))
        assert result.quality == ('partial' if gap_minutes else 'complete')
        assert output(result, 'collect', 'collect').resource_status == ('unknown' if gap_minutes else 'observed_zero')
        assert 'session_continuity_unknown' in result.window.limitations
        gaps = result.next_state.projection.input_gaps
        assert len(gaps) == (1 if gap_minutes else 0)
        if gaps:
            assert (gaps[0].left_source, gaps[0].right_source) == (baseline.source_id, first.source_id)
            assert (gaps[0].start, gaps[0].end) == (baseline.window.end, first.window.start)
            assert result.calculation_parameters['input_gaps'][0]['basis'] == 'declared_input_bounds_not_session_continuity'
            assert result.next_state.export()['projection']['input_gaps'][0]['start'] == baseline.window.end.isoformat()
    assert frozen.input_gaps == ()
