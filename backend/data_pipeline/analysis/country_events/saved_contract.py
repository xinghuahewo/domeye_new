"""C1的独立生产回执与临时引用索引；不是状态重建或恢复协议。"""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3

from data_pipeline.bgp.archive.store import TABLES


@dataclass(frozen=True)
class ProductionReceiptBinding:
    path: str
    sha256: str

    def read(self, reader, state_rule):
        # SHA由调用方在生产完成时固定；不能用当前损坏表生成预期。
        if len(self.sha256) != 64:
            raise ValueError('invalid_receipt_sha256')
        raw = Path(self.path).read_bytes()
        if hashlib.sha256(raw).hexdigest() != self.sha256:
            raise ValueError('production_receipt_digest_mismatch')
        receipt = json.loads(raw)
        if (receipt.get('run_id'), receipt.get('snapshot'), receipt.get('state'), receipt.get('rule_version')) != (
                reader.run_id, reader.snapshot, 'ready', state_rule):
            raise ValueError('production_receipt_identity_mismatch')
        actual = receipt.get('actual_rows', {})
        if set(actual) != set(TABLES) or any(type(n) is not int or n < 0 for n in actual.values()):
            raise ValueError('production_receipt_table_contract_mismatch')
        sources = receipt.get('sources', [])
        expected = {s.source_id:(s.content_sha256,s.expected_messages,s.expected_elements) for s in reader.starts}
        if len({s['source_id'] for s in sources}) != len(sources):
            raise ValueError('production_receipt_duplicate_source')
        known = {s['source_id']:(s['content_sha256'],s['messages'],s['elements']) for s in sources}
        if any(known.get(k) != v for k,v in expected.items()):
            raise ValueError('production_receipt_source_mismatch')
        manifest_sources = {r['source_id']:r['sha256'] for r in reader.manifest['inputs']}
        if {k:v[0] for k,v in known.items()} != manifest_sources:
            raise ValueError('production_receipt_manifest_mismatch')
        return receipt


def verify_table_counts(db, table, receipt, guard):
    for name, expected in receipt['actual_rows'].items():
        guard()
        actual = db.execute(f'SELECT count(*) FROM {table(name)}').fetchone()[0]
        if actual != expected:
            raise ValueError(f'production_table_count_mismatch:{name}:expected={expected}:actual={actual}')


class FixedLookup:
    """磁盘SQLite B-tree。一次填充；Python仅持有输入批和单次查找结果。"""
    def __init__(self, directory, guard):
        self.db = sqlite3.connect(str(Path(directory)/'references.sqlite'))
        self.guard = guard
        self.db.execute('PRAGMA cache_size=-4096')  # 4MiB页缓存，不是全表dict。
        self.db.execute('PRAGMA temp_store=FILE')
        self.db.executescript('''
            CREATE TABLE events(event_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE peers(source_id TEXT, table_record INTEGER, peer_index INTEGER,
                bgp_id TEXT, ip TEXT, asn INTEGER, PRIMARY KEY(source_id,table_record,peer_index));
        ''')
        self.stats = dict(source_join_queries=0, source_peer_queries=0,
                          event_index_rows=0, peer_index_rows=0, event_index_lookups=0, peer_index_lookups=0)

    def build(self, source_db, table, rows):
        self.stats['source_join_queries'] += 1
        query = f'''SELECT e.event_id,e.message_id,e.ordinal,e.path_key,e.path_id,e.path_id_present,
                    m.source_id,m.record,m.epoch,m.microsecond
                    FROM {table('elements')} e JOIN {table('messages')} m USING(message_id)'''
        for row in rows(source_db, query):
            self.guard()
            self.db.execute('INSERT INTO events VALUES (?,?)', (row['event_id'],json.dumps(row)))
            self.stats['event_index_rows'] += 1
        self.stats['source_peer_queries'] += 1
        for row in rows(source_db, f'SELECT * FROM {table("peers")}'):
            self.guard()
            self.db.execute('INSERT INTO peers VALUES (?,?,?,?,?,?)',
                            (row['source_id'],row['table_record'],row['index'],row['bgp_id'],row['ip'],row['asn']))
            self.stats['peer_index_rows'] += 1
        self.db.commit()
        self.stats['event_lookup_plan'] = self.db.execute('EXPLAIN QUERY PLAN SELECT payload FROM events WHERE event_id=?', ('probe',)).fetchone()[3]
        self.stats['peer_lookup_plan'] = self.db.execute('EXPLAIN QUERY PLAN SELECT bgp_id,ip,asn FROM peers WHERE source_id=? AND table_record=? AND peer_index=?', ('probe',0,0)).fetchone()[3]

    def event(self, key):
        self.guard()
        self.stats['event_index_lookups'] += 1
        row = self.db.execute('SELECT payload FROM events WHERE event_id=?', (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def peer(self, source, record, index):
        self.guard()
        self.stats['peer_index_lookups'] += 1
        return self.db.execute('SELECT bgp_id,ip,asn FROM peers WHERE source_id=? AND table_record=? AND peer_index=?',
                               (source,record,index)).fetchone()

    def close(self):
        self.db.close()
