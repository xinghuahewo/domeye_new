"""实际M2/reference公共入口验证组合锁与流；不冒充完整RFD国家P。"""
from contextlib import closing
import json
import os
from pathlib import Path
import pytest
import psycopg2
from data_pipeline.results.component_readers import Admissions, lock_key
from data_pipeline.bgp.archive import admission as owner
from data_pipeline.bgp.archive.value_codec import untyped
from tests.observations.test_observation_publication import request


@pytest.fixture(scope='module')
def graph():
    path = os.environ.get('DOMEYE_COMBINED_ACCESS_CONTEXT')
    if not path: pytest.skip('须显式提供自有真实人工M2/reference Admission')
    saved = json.loads(Path(path).read_text()); root = Path(path).parent
    values = saved['admissions']
    runtime = owner.Runtime(saved['dsn'], (root,), root, fixture_only=True,
                            dependency_admissions=tuple(a for a in values if a['owner'] == 'm2'))
    events = []; runtime.audit_sink = events.append
    return Admissions(values, {a['admission_id']: runtime for a in values}, guard=lambda: None), values, runtime, events


def test_actual_closed_dependencies_and_single_lock_order(graph):
    g, values, runtime, events = graph
    assert g.lock_targets == sorted(g.lock_targets, key=lock_key)
    assert len(g.lock_targets) == len({lock_key(t) for a in values for t in a['lock_targets']})
    with pytest.raises(ValueError, match='依赖'):
        Admissions([values[1]], {values[1]['admission_id']: runtime}, guard=lambda: None)
    events.clear()
    with g.locked():
        for a in values:
            key = next(t['key'] for t in a['lock_targets'] if t['namespace'] == a['owner'] + '.admission')
            table = owner.TABLES[a['owner'] + '.admission']
            with closing(psycopg2.connect(runtime.dsn)) as pg, pg.cursor() as c:
                c.execute("SET lock_timeout='100ms'")
                with pytest.raises(psycopg2.errors.LockNotAvailable):
                    c.execute(f'UPDATE {table} SET state=state WHERE record_key=%s', (key,))
        events.clear()
    assert events == []


def test_actual_reader_full_values_and_early_stop(graph):
    g, values, runtime, events = graph
    for a in values:
        req = request(a, 'messages' if a['owner'] == 'm2' else 'references', 1)
        original = []
        with owner.open_reader(runtime, a, req, guard=lambda: None) as direct:
            for b in direct: original.extend(untyped(b['rows_typed']))
        events.clear(); actual = []
        with g.read(a['admission_id'], req, max_rows=10000, max_bytes=16*1024**2) as session:
            for b in session: actual.extend(untyped(b['rows_typed']))
            assert session.receipt is None
        assert actual == original and session.receipt == direct.receipt
        assert not any(e['kind'] in ('entity_hash', 'admit_body_batch') for e in events)
        with g.read(a['admission_id'], req, max_rows=10000, max_bytes=16*1024**2) as early: next(early)
        assert early.receipt is None
        with g.read(a['admission_id'], req, max_rows=1, max_bytes=1) as counted:
            list(counted)
        assert counted.rows == session.rows and counted.bytes == session.bytes
        assert counted.receipt == session.receipt


def test_actual_dependency_revoked_at_stream_tail(graph):
    g, values, runtime, _ = graph
    m2, reference = values
    target = next(t for t in m2['lock_targets'] if t['namespace'] == 'm2.admission')
    def change(state):
        with closing(psycopg2.connect(runtime.dsn)) as pg, pg, pg.cursor() as c:
            c.execute('UPDATE observation_publication.m2_admissions SET state=%s WHERE record_key=%s', (state, target['key']))
    try:
        with pytest.raises(ValueError):
            with g.read(reference['admission_id'], request(reference, 'references'), max_rows=10000, max_bytes=16*1024**2) as s:
                list(s); change('revoked')
        assert s.receipt is None
    finally: change('accepted')
