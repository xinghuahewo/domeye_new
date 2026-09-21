"""8233：一次微型 M2 映射排列与原 canonical 原身份定点回读。"""
from collections import Counter
from contextlib import closing
from copy import deepcopy
import gzip
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import socket
import struct
import time
from types import SimpleNamespace
import uuid

import psycopg2
import pytest
from tests.observations.test_observation_mrt import mrt, update, attr
from tests.observations.test_canonical_m3b_integration import measured, drain
from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.bgp.replay.snapshot_contract import ProjectionBinding, encode, decode, digest
from data_pipeline.bgp.replay.snapshot_store import ProjectionReader, ProjectionWriter
from data_pipeline.bgp.replay.archive_input import build_mapping

OLD = Path('/tmp/domeye-canonical-m3b-integration-8233')
OUT = Path('/tmp/domeye-mapping-order-integration-8233')


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def guard():
    if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss > 4 * 1024**3 or shutil.disk_usage(OUT).free < 512 * 1024**2:
        raise ValueError('人工映射资源保护')


def pg_metadata(dsn, run):
    with closing(psycopg2.connect(dsn)) as pg, pg.cursor() as c:
        pg.set_session(readonly=True)
        c.execute('SELECT to_jsonb(r) FROM observation_m2.runs r WHERE run_id=%s', (run,))
        registration = c.fetchone()[0]
        c.execute('SELECT to_jsonb(c) FROM observation_m2.checkpoints c WHERE run_id=%s ORDER BY ordinal', (run,))
        return {'run': registration, 'checkpoints': c.fetchall()}


@pytest.fixture(scope='module')
def artificial():
    if os.environ.get('DOMEYE_MAPPING_INTEGRATION') != '8233':
        pytest.skip('须明确绑定本任务人工目标')
    OUT.mkdir(exist_ok=False)
    config = json.loads((OLD / 'reader.json').read_text())
    old = ProjectionReader(config['dsn'], ProjectionBinding(**config['binding']), guard=guard)
    baseline = old.input_reader.sources[0]
    with closing(old.input_reader.connect()) as db:
        peers = db.execute('SELECT "ip",asn,table_record,"index" FROM ' + old.input_reader.bound_table('peers') + ' WHERE source_id=?', [baseline]).fetchall()
    assert len(peers) == 3 and max(Counter((p[0], p[1]) for p in peers).values()) == 1
    save(OUT / '原M2样本边界.json', {'binding': config['binding'], 'baseline_peers': peers, 'reason': '原三组单引用不足覆盖同组多引用，新增一次极小M2'})
    protected = {}
    directories = [OLD, Path('/tmp/domeye-detection-m3-integration-8233/detection-m30'),
                   Path('/tmp/domeye-feature-m3-integration-8233/feature-m30'),
                   Path('/tmp/domeye-resource-m3-integration-8233/resource'),
                   Path('/tmp/domeye-resource-m3-integration-8233/catalog/resource_c4b0419f11fc4d6ab7f4abb714ae1078'),
                   Path('/tmp/domeye-integration-q3-8233/h1-integration-8233/acceptance-5455ef6903dd43639f106787a2c8d127'),
                   Path('/tmp/domeye-integration-q3-8233/q3-c1/integration-3cad2ad5257b40198ed8eebee7247551')]
    for directory in directories:
        protected.update({str(p): sha(p) for p in directory.rglob('*') if p.is_file()})
    # H1/C.1 实际保留正文也纳入 SHA，不打开其 PG 或正文查询。
    for file, key in ((directories[-2] / 'binding.json', 'core'), (directories[-1] / 'binding.json', 'token')):
        value = json.loads(file.read_text())[key]
        collection = value['collection'] if key == 'core' else value
        ids = [collection['collection_id'], *[c['import_id'] for c in collection['children']]]
        if key == 'core': ids.append(value['profile_id'])
        for ident in ids:
            protected.update({str(p): sha(p) for p in (Path('/tmp/domeye-integration-q3-8233/history') / ident).rglob('*') if p.is_file()})
    save(OUT / '原件SHA前.json', protected)
    base = 'host=/tmp/domeye-integration-detection-8233/socket port=55483'
    name = 'mapping_8233_' + uuid.uuid4().hex[:10]
    with closing(psycopg2.connect(base + ' dbname=postgres')) as pg:
        pg.autocommit = True
        with pg.cursor() as c: c.execute('CREATE DATABASE ' + name)
    dsn = base + ' dbname=' + name
    inputs = OUT / 'input'; inputs.mkdir()
    def peer_table(values):
        body = struct.pack('!IH', 25, 5) + b'rrc25' + struct.pack('!H', len(values))
        return mrt(body + b''.join(b'\x02' + socket.inet_aton(bgp) + socket.inet_aton(ip) + struct.pack('!I', 64497) for ip, bgp in values), 1, 13)
    peers = [('192.0.2.1', '203.0.113.200'), ('192.0.2.1', '192.0.2.1'),
             ('192.0.2.3', '192.0.2.3'), ('192.0.2.4', '192.0.2.4'), ('192.0.2.6', '192.0.2.6')]
    attrs = attr()
    rib = mrt(struct.pack('!I', 0) + b'\x18\xc0\0\x02' + struct.pack('!HHIH', 1, 0, 90, len(attrs)) + attrs, 2, 13)
    baseline_bytes = peer_table(peers) + rib + peer_table(peers[:1]) + rib
    ambiguous = update(peer=socket.inet_aton('192.0.2.4'))
    second_local = ambiguous[:28] + socket.inet_aton('192.0.2.5') + ambiguous[32:]
    update_bytes = update() + ambiguous + second_local + update(peer=socket.inet_aton('192.0.2.6'))
    manifest = {'schema_version': 'observation-run/v1', 'collector': 'rrc25', 'window_start': '1970-01-01T00:00:00Z',
                'window_end_exclusive': '1970-01-02T00:00:00Z', 'inputs': []}
    for label, raw, role in (('rib', baseline_bytes, 'baseline'), ('update', update_bytes, 'update')):
        path = inputs / (label + '.gz'); path.write_bytes(gzip.compress(raw, mtime=0))
        uri = 'fixture://mapping-integration-8233/' + label
        manifest['inputs'].append({'path': str(path), 'sha256': sha(path), 'size': path.stat().st_size,
                                   'source_id': source_identity('rrc25', uri, sha(path)), 'origin_uri': uri, 'role': role})
    sources = [r['source_id'] for r in manifest['inputs']]
    manifest.update(baseline_source=sources[0], update_sources=sources[1:])
    start = time.monotonic()
    seal = produce_checkpointed(manifest, dsn, OUT / 'm2', min_free_bytes=512*1024**2, batch_rows=2)
    save(OUT / '一次M2.json', {'dsn': dsn, 'seal': seal, 'manifest': manifest, 'wall_seconds': time.monotonic()-start})
    yield ObservationReader(dsn, seal['run_id'], seal['snapshot'], sources, profile='observation', guard=guard)
    assert all(sha(p) == value for p, value in protected.items())
    save(OUT / '原件SHA后.json', {'unchanged_files': len(protected), 'all_sha_equal': True})


def test_actual_mapping_orders(artificial):
    reader = artificial
    before = deepcopy(reader.selection.seal); registration = pg_metadata(reader.dsn, before['run_id'])
    files = {f['path']: sha(f['path']) for cp in reader.selection.checkpoints for f in cp['files']}
    files.update({str(p): sha(p) for p in (OUT / 'input').iterdir()})
    original = reader.connect; orders = []; results = []; costs = []
    class Connection:
        def __init__(self, db, order): self.db, self.order, self.peers = db, order, False
        def __getattr__(self, key): return getattr(self.db, key)
        def execute(self, sql, *args):
            self.peers = sql.startswith('SELECT "ip",asn,bgp_id,table_record,"index"')
            self.db.execute(sql, *args); return self
        def fetchall(self):
            rows = self.db.fetchall()
            if self.peers:
                rows = sorted(rows, key=lambda r: (r[3], r[4]))
                rows = rows if self.order == 'forward' else list(reversed(rows)) if self.order == 'reverse' else rows[1:] + rows[:1]
                orders.append(rows)
            return rows
    for order in ('forward', 'reverse', 'rotate'):
        reader.connect = lambda: Connection(original(), order)
        rows = []; sink = SimpleNamespace(dsn=reader.dsn, flush=guard, append=lambda table, row: rows.append(row))
        start = time.monotonic(); endpoints = build_mapping(sink, reader.sources[0], reader.sources[1:], reader=reader)
        result = (endpoints, rows); results.append(result)
        costs.append({'order': order, 'wall_seconds': time.monotonic()-start, 'logical_bytes': len(encode(result).encode())})
    reader.connect = original
    assert len({encode(o) for o in orders}) == 3 and all(Counter(o) == Counter(orders[0]) for o in orders)
    assert len({encode(r) for r in results}) == len({digest(r) for r in results}) == 1
    endpoints, rows = results[0]
    assert endpoints == (('192.0.2.6', 64497, '192.0.2.2', 12654, 0),)
    assert [r['status'] for r in rows] == ['conflicting_baseline_peers', 'no_observed_endpoint', 'ambiguous_local_endpoints', 'calculation_mapping']
    assert json.loads(rows[0]['peer_refs']) == [['203.0.113.200', 0, 0], ['192.0.2.1', 0, 1], ['203.0.113.200', 2, 0]]
    assert reader.selection.seal == before and pg_metadata(reader.dsn, before['run_id']) == registration
    assert all(sha(p) == value for p, value in files.items())
    save(OUT / '映射全字段.json', {'orders': orders, 'typed': encode(results[0]), 'digest': digest(results[0]), 'costs': costs,
                                'unchanged_files': files, 'registration_and_checkpoint_equal': True,
                                'rss_lifetime_peak_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                                'scope': '主动模拟SQL无承诺行序；metadata fetchall，非读批测试；RSS生命周期累计含M2且不含PG，阶段峰Unknown，无速度收益主张'})


def test_original_canonical_full_audit_and_mapping(artificial, monkeypatch):
    from data_pipeline.bgp.archive import checkpoint; from data_pipeline.bgp.input import mrt_reader as mrt_module
    def forbidden(*args, **kwargs): raise AssertionError('禁止原canonical/M2重产或MRT解析')
    monkeypatch.setattr(ProjectionWriter, '__init__', forbidden)
    monkeypatch.setattr(checkpoint, 'produce_checkpointed', forbidden)
    monkeypatch.setattr(mrt_module, 'read_source', forbidden)
    config = json.loads((OLD / 'reader.json').read_text())
    reader = ProjectionReader(config['dsn'], ProjectionBinding(**config['binding']), guard=guard)
    def registration():
        with closing(psycopg2.connect(reader.dsn)) as pg, pg.cursor() as c:
            pg.set_session(readonly=True)
            c.execute('SELECT to_jsonb(r) FROM m3_projection.runs r WHERE run_id=%s', (reader.binding.run_id,))
            return c.fetchone()[0]
    before = registration(); seal = deepcopy(reader.seal)
    files = {f['path']: sha(f['path']) for f in reader.seal['metrics']['parquet_files']}
    with measured() as cost:
        audit = reader.audit()
        actual, receipt = drain(reader.scan('baseline_mappings'))
    expected = decode((OLD / '生产完整13表.typed.json').read_text())['baseline_mappings']
    assert audit['audit'] == 'complete' and actual == expected and cost['replays'] == 2
    assert registration() == before and reader.seal == seal and all(sha(p) == value for p, value in files.items())
    save(OUT / '原canonical直接回读.json', {'binding': config['binding'], 'audit': audit, 'mapping': actual, 'receipt': receipt,
                                           'cost': cost, 'unchanged_parquet': files, 'registration_equal': True,
                                           'scope': '原三组单引用，full audit一次及baseline scan一次；未重产、未13scan，不能证明旧非规范多refs兼容'})
