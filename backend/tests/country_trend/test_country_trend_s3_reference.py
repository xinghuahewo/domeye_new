"""S3参考上下文容器接合；真实文件/codec，PG边界为fixture，不证明准入。"""
from contextlib import contextmanager
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
import json

import pytest

from data_pipeline.analysis.country_trends import reference_reader as reference
from data_pipeline.analysis.country_trends.contract import Projection, ReferenceInput
from data_pipeline.analysis.country_trends.stream_schema import encode, decode
from data_pipeline.analysis.country_trends.snapshot_inputs import REFERENCE_PROFILE
from data_pipeline.results.manifest_io import encode as json_text, file_hash, stamp


def fixture_runtime(tmp_path, monkeypatch, refs):
    path = tmp_path/'reference.json'
    path.write_text(json.dumps({'references': encode(refs)}, ensure_ascii=False))
    binding = dict(path=str(path), profile=REFERENCE_PROFILE,
                   historical_applicability='unknown', system_id='fixture-system',
                   database_oid=123, sha256=file_hash(path), stamp=stamp(path))
    runtime = SimpleNamespace(reference_binding=binding, path=Path,
                              limits=SimpleNamespace(max_context_bytes=8*1024**2))
    calls = []

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, query, args=None):
            self.identity_query = 'pg_control_system' in query
            if not self.identity_query: assert args == (binding['sha256'],)
        def fetchone(self):
            if self.identity_query: return ('fixture-system', 123)
            return ('complete', json_text(binding))

    @contextmanager
    def pg(actual_runtime):
        assert actual_runtime is runtime
        calls.append('verify')
        yield SimpleNamespace(cursor=Cursor)

    original_hash = reference.file_hash
    def tracked_hash(path, guard):
        calls.append('full_hash')
        return original_hash(path, guard)

    monkeypatch.setattr(reference.pg_api, '_pg', pg)
    monkeypatch.setattr(reference, 'file_hash', tracked_hash)
    return runtime, calls


def projection(country='ZZ'):
    return Projection(country, 'fixed-peer-prefix', 12, (100, 200),
                      (Fraction(12), Fraction(7, 2)), 'original-source')


def test_nonempty_reference_context_keeps_original_tuple_contract(tmp_path, monkeypatch):
    original = ReferenceInput(('fixture-event', 2), ('original-binding',), 'ZZ',
                              (projection(),))
    runtime, calls = fixture_runtime(tmp_path, monkeypatch, (original,))
    result, sources = reference.reference_context(runtime, guard=lambda: None)
    assert type(result) is tuple and type(sources) is tuple
    assert result[0].event == ('fixture-event', 2)
    assert result[0].projections[0].values == (Fraction(12), Fraction(7, 2))
    assert decode(encode((result, sources))) == (result, sources)
    assert calls == ['verify', 'full_hash', 'verify']


def test_multiple_references_preserve_all_fields_order_and_rebind_sources(tmp_path, monkeypatch):
    from dataclasses import replace
    first = replace(projection('乙国'), quality='unknown', asn_count=7,
                    persistent_asn_count=3, cohort_id='cohort-乙',
                    definition_binding=('definition', ('scope', 2)))
    second = replace(projection('甲国'), denominator=17, values=(None, Fraction(2, 3)),
                     source_ref='second-source', cohort_id='cohort-甲')
    refs = (ReferenceInput(('event-b', 3), ('old-b',), '乙国', (first, second), 'rule-b'),
            ReferenceInput(('event-a', 1), ('old-a',), '甲国', (second,), 'rule-a'))
    runtime, calls = fixture_runtime(tmp_path, monkeypatch, refs)
    result, sources = reference.reference_context(runtime, guard=lambda: None)
    prefix = 'reference:' + runtime.reference_binding['sha256']
    expected = (
        replace(refs[0], binding=(REFERENCE_PROFILE, json_text(runtime.reference_binding)),
                projections=(replace(first, source_ref=prefix+':0:0'),
                             replace(second, source_ref=prefix+':0:1'))),
        replace(refs[1], binding=(REFERENCE_PROFILE, json_text(runtime.reference_binding)),
                projections=(replace(second, source_ref=prefix+':1:0'),)),
    )
    assert result == expected
    assert tuple((key, role, decode(raw)) for key, role, raw in sources) == (
        (prefix+':0:0', 'reference', first), (prefix+':0:1', 'reference', second),
        (prefix+':1:0', 'reference', second))
    assert calls == ['verify', 'full_hash', 'verify']
    # S3 producer的紧邻下游预算合同包含ActivityWindow及同一tuple上下文。
    from data_pipeline.analysis.country_trends.contract import ActivityWindow
    activities = (ActivityWindow(('event-b', 3), 'ordinary', 'updates', 'fixed',
                                 100, 200, Fraction(2), 'complete', 'feature-source'),)
    context = (activities, result, sources)
    assert decode(encode(context)) == context


@pytest.mark.parametrize('refs', [(), (ReferenceInput(('empty', 1), (), 'ZZ', ()),)])
def test_empty_reference_or_projection_preserves_tuple_semantics(tmp_path, monkeypatch, refs):
    runtime, calls = fixture_runtime(tmp_path, monkeypatch, refs)
    result, sources = reference.reference_context(runtime, guard=lambda: None)
    assert type(result) is tuple and sources == ()
    assert len(result) == len(refs)
    if refs:
        assert result[0].projections == () and result[0].target == 'ZZ'
    assert calls == ['verify', 'full_hash', 'verify']


def test_context_limit_measures_exact_final_tuple_bytes(tmp_path, monkeypatch):
    refs = (ReferenceInput(('boundary', 1), (), 'ZZ', (projection(),)),)
    runtime, calls = fixture_runtime(tmp_path, monkeypatch, refs)
    result = reference.reference_context(runtime, guard=lambda: None)
    exact_bytes = len(encode(result).encode())
    assert exact_bytes > Path(runtime.reference_binding['path']).stat().st_size
    runtime.limits.max_context_bytes = exact_bytes
    calls.clear()
    assert reference.reference_context(runtime, guard=lambda: None) == result
    assert calls == ['verify', 'full_hash', 'verify']
    runtime.limits.max_context_bytes = exact_bytes - 1
    calls.clear()
    with pytest.raises(ValueError, match='^trend_s3_reference_context_bytes$'):
        reference.reference_context(runtime, guard=lambda: None)
    assert calls == ['verify', 'full_hash']


@pytest.mark.parametrize('failure_at', [1, 2])
def test_pre_and_post_reference_verification_failures_propagate(tmp_path, monkeypatch, failure_at):
    refs = (ReferenceInput(('failure', 1), (), 'ZZ', (projection(),)),)
    runtime, calls = fixture_runtime(tmp_path, monkeypatch, refs)
    original_pg = reference.pg_api._pg
    failure = RuntimeError('fixture external registration failed')
    attempts = []
    @contextmanager
    def failing_pg(actual_runtime):
        attempts.append(len(attempts)+1)
        if len(attempts) == failure_at:
            raise failure
        with original_pg(actual_runtime) as pg:
            yield pg
    monkeypatch.setattr(reference.pg_api, '_pg', failing_pg)
    with pytest.raises(RuntimeError) as caught:
        reference.reference_context(runtime, guard=lambda: None)
    assert caught.value is failure
    assert len(attempts) == failure_at
    assert calls == ([] if failure_at == 1 else ['verify', 'full_hash'])


def test_original_full_hash_failure_and_guard_failure_still_reject(tmp_path, monkeypatch):
    runtime, calls = fixture_runtime(tmp_path, monkeypatch, ())
    runtime.reference_binding['sha256'] = '0'*64
    with pytest.raises(ValueError, match='^trend_s3_reference_entity$'):
        reference.reference_context(runtime, guard=lambda: None)
    assert calls == ['verify', 'full_hash']
    failure = RuntimeError('fixture resource guard')
    def guard(): raise failure
    calls.clear()
    with pytest.raises(RuntimeError) as caught:
        reference.reference_context(runtime, guard=guard)
    assert caught.value is failure and calls == []
