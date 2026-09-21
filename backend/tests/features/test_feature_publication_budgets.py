"""Feature P1有限预算单项增量；复用已有人工M3，不生产新输入。"""
from dataclasses import replace
import pytest

from tests.features.test_feature_publication import context, request
from data_pipeline.analysis.features import publication as p, publication_io as io


@pytest.mark.parametrize('field', ['max_temp_bytes', 'max_rss_bytes'])
def test_budget_constructor_type_and_finiteness(context, field):
    _, runtime, _, _ = context
    for value in (float('nan'), float('inf'), -float('inf'), True, False, '1024', None, 0, -1, 0.5):
        with pytest.raises(ValueError, match=field): replace(runtime, **{field: value})
    for value in (1, 1.0, 1024**3, float(1024**3), 10**400):
        assert getattr(replace(runtime, **{field: value}), field) == value


@pytest.mark.parametrize('field', ['max_temp_bytes', 'max_rss_bytes'])
@pytest.mark.parametrize('when', ['first_batch', 'exhausted', 'tail_current'])
def test_invalid_mutation_cannot_issue_receipt(context, field, when):
    _, original, admission, _ = context
    for value in (float('nan'), float('inf'), True, '1024'):
        runtime = replace(original); tail = False
        def guard():
            if tail: setattr(runtime, field, value)
        with pytest.raises(ValueError, match=field) as held:
            with p.open_reader(runtime, admission, request('coverage'), guard=guard) as session:
                if when == 'first_batch':
                    next(session); setattr(runtime, field, value); list(session)
                else:
                    list(session)
                    if when == 'exhausted': setattr(runtime, field, value)
                    else: tail = True
        assert held.value.__traceback__ is not None
        assert session.receipt is None


@pytest.mark.parametrize('field', ['max_temp_bytes', 'max_rss_bytes'])
def test_finite_adjustment_and_finite_limit_control(context, field):
    _, original, admission, _ = context
    runtime = replace(original)
    with p.open_reader(runtime, admission, request('coverage'), guard=lambda: None) as session:
        next(session); setattr(runtime, field, float(2*1024**3)); list(session)
    assert session.receipt is not None and session.receipt['rows'] == 36
    runtime = replace(original)
    with pytest.raises(ValueError, match='临时盘/RSS保护'):
        with p.open_reader(runtime, admission, request('coverage'), guard=lambda: None) as session:
            next(session); setattr(runtime, field, 1); list(session)
    assert session.receipt is None


@pytest.mark.parametrize('field', ['max_temp_bytes', 'max_rss_bytes'])
def test_budget_check_rejects_callback_mutation(context, field):
    _, original, _, _ = context
    runtime = replace(original)
    def event(item):
        if item['kind'] == 'resource_sample': setattr(runtime, field, float('nan'))
    runtime.audit_sink = event
    with pytest.raises(ValueError, match=field): io.budget(runtime, runtime.scratch_root, lambda: None)
