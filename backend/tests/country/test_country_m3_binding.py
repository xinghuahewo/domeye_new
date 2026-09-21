"""原全局 rank 跨越未选来源时，国家历史边界不得压缩或错绑。"""
from dataclasses import asdict, replace

import pytest

from data_pipeline.analysis.country_events.models import Baseline, Binding, Cursor, Incident, Time
from data_pipeline.analysis.country_events.cohort import select_baseline
from data_pipeline.analysis.country_events.route_binding import M3Binding


def binding():
    return M3Binding('fixture', '1', ('rib', 'u9', 'u11'), 'ref', 'decoder',
                     'state', 'rrc25', global_sources=('rib', *tuple(f'u{i}' for i in range(1, 12))),
                     input_binding_id='fixture-input-binding')


def test_sparse_original_rank_selects_historical_boundary():
    b = binding()
    before = Baseline('before', b, Time(10, 0), Cursor(9, 2, 0), 'mapping')
    after = replace(before, baseline_id='after', at=Time(12, 0), cursor=Cursor(11, 2, 0))
    incident = Incident('event', 1, 'ZZ', Time(15, 0), onset=Time(11, 0),
                        onset_cursor=Cursor(11, 1, 0))
    assert select_baseline(incident, [before, after]) == before
    assert b.rank_of('u9') == 9 and b.source_at(11) == 'u11'
    # 压缩后的 1/2 以及未选原始 rank 不能冒充已选来源。
    for rank in (1, 2, 10, 12, -1, True):
        assert b.source_at(rank) is None
    with pytest.raises(ValueError):
        replace(before, cursor=Cursor(2, 2, 0))
    with pytest.raises(ValueError):
        select_baseline(replace(incident, onset_cursor=Cursor(10, 1, 0)), [before])


def test_selection_cannot_renumber_or_reorder_original_sources():
    with pytest.raises(ValueError):
        replace(binding(), ordered_sources=('rib', 'u11', 'u9'))
    with pytest.raises(ValueError):
        binding().rank_of('u10')


def test_legacy_binding_identity_keeps_exact_original_fields():
    old = Binding('run', '1', ('rib', 'u1'), 'ref', 'decoder', 'state', 'rrc25')
    assert set(asdict(old)) == {'run_id', 'snapshot_id', 'ordered_sources', 'reference_version',
                                'decoder_version', 'state_rule_version', 'collector', 'state'}
    assert old.source_at(1) == 'u1' and old.rank_of('u1') == 1
