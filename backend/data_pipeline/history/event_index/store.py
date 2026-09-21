"""H1离线领域投影登记、固定typed载体及来源共同资格。"""
from contextlib import closing
from dataclasses import asdict
import hashlib
import json
import re
import uuid

import pyarrow as pa
from psycopg2 import sql

from data_pipeline.history.event_collection.store import History as CollectionHistory, checked_row, row_bytes
from data_pipeline.history.event_collection.children import checked_lake, directory_bytes
from data_pipeline.history.event_collection.freeze import safe_path
from data_pipeline.history.database_import.freeze import write_json, read_json
from data_pipeline.history.event_index.model import CoreToken, RULE, TABLES, SCHEMAS, ORDER, code_identity, rule_sha
from data_pipeline.history.event_index.project import Projector
from data_pipeline.history.event_index.cleanup import release


def rows(db, token, name, budget, cap=None):
    ti = TABLES.index(name)
    budget.check()
    width = db.execute(f'SELECT coalesce(max(octet_length(encode(to_json(t)))),0) FROM (SELECT * FROM lake.history.t{ti} AT (VERSION => {int(token.snapshot)})) t').fetchone()[0]
    count = min(cap or budget.limits.batch_rows, budget.limits.batch_rows,
                max(1, budget.limits.batch_bytes // max(1, width * 8 + len(SCHEMAS[name]) * 256)))
    if width > budget.limits.max_row_bytes:
        count = 1
    reader = db.execute(f'SELECT * FROM lake.history.t{ti} AT (VERSION => {int(token.snapshot)}) ORDER BY ' + ORDER[name]).fetch_record_batch(count)
    primary = None
    try:
        while True:
            budget.check()
            try:
                batch = next(reader)
            except StopIteration:
                break
            budget.add('arrow_batches', 1)
            if batch.nbytes > budget.limits.batch_bytes:
                raise ValueError('H1 Arrow读批超限')
            for row in batch.to_pylist():
                data = checked_row(row, budget)
                budget.add('typed_rows', 1, budget.limits.max_total_rows)
                budget.add('typed_bytes', len(data), budget.limits.max_total_bytes)
                yield row
    except BaseException as error:
        primary = error
        raise
    finally:
        cleanup = release(reader, 'Arrow reader', primary, getattr(budget, 'cleanup_errors', None))
        if primary is None and cleanup is not None:
            raise cleanup


class History(CollectionHistory):
    def project_core(self, collection, *, root_id):
        if type(root_id) is not int or not 0 <= root_id < self.collection_limits.files:
            raise ValueError('H1根scope参数无效')
        if self.target_identity is None:
            raise ValueError('H1须显式私有离线目标')
        budget = self._budget()
        identity = code_identity()
        rule = rule_sha()
        pid = uuid.uuid4().hex
        out = self.data_root / pid
        pg = self._connect()
        db = None
        registered = False
        try:
            self._qualify_collection(collection, budget)
            with pg, pg.cursor() as c:
                c.execute('''CREATE TABLE IF NOT EXISTS history_q3.core_profiles(
                    profile_id TEXT PRIMARY KEY,collection_id TEXT NOT NULL,root_id BIGINT NOT NULL,
                    rule_sha TEXT NOT NULL,state TEXT NOT NULL,snapshot BIGINT,ready_sha TEXT,
                    private_root TEXT NOT NULL,reason TEXT)''')
                c.execute('INSERT INTO history_q3.core_profiles VALUES (%s,%s,%s,%s,\'candidate\',NULL,NULL,%s,NULL)',
                          (pid, collection.collection_id, root_id, rule, str(self.root)))
            registered = True
            out.mkdir(exist_ok=False)
            counts = []
            with closing(Projector(self, collection, root_id, out, budget)) as projection:
                with self.collection(collection) as source:
                    projection.prepare(source)
                if source.receipt is None:
                    raise ValueError('H1集合输入未正常耗尽')
                db = checked_lake(self._lake(pid, False), out, budget, self.collection_limits.temporary_bytes - 2 * self.limits.max_metadata_bytes)
                db.execute('CREATE SCHEMA lake.history')
                for ti, name in enumerate(TABLES):
                    schema = SCHEMAS[name]
                    db.register('profile_schema', pa.Table.from_batches([], schema=schema))
                    db.execute(f'CREATE TABLE lake.history.t{ti} AS SELECT * FROM profile_schema')
                    db.unregister('profile_schema')
                    pending = []
                    size = count = 0
                    digest = hashlib.sha256()

                    def flush():
                        nonlocal size
                        if not pending:
                            return
                        budget.check()
                        batch = pa.Table.from_pylist(pending, schema=schema)
                        if batch.nbytes > self.limits.batch_bytes:
                            raise ValueError('H1 Arrow写批超限')
                        db.register('profile_batch', batch)
                        try:
                            db.execute(f'INSERT INTO lake.history.t{ti} SELECT * FROM profile_batch')
                        finally:
                            db.unregister('profile_batch')
                        budget.add('arrow_batches', 1)
                        used = directory_bytes(out, budget)
                        budget.counts['temporary_bytes'] = used
                        budget.counts['profile_candidate_disk_peak_bytes'] = max(used, budget.counts.get('profile_candidate_disk_peak_bytes', 0))
                        pending.clear()
                        size = 0

                    for row in projection.rows(name):
                        encoded = checked_row(row, budget)
                        budget.add('profile_write_rows', 1, self.limits.max_total_rows)
                        budget.add('profile_write_bytes', len(encoded), self.limits.max_total_bytes)
                        if pending and (len(pending) == self.limits.batch_rows or size + len(encoded) > self.limits.batch_bytes):
                            flush()
                        pending.append(row)
                        size += len(encoded)
                        count += 1
                        digest.update(encoded + b'\n')
                    flush()
                    counts.append({'name': name, 'rows': count, 'sha256': digest.hexdigest()})
                root = projection.root
            # 暂存不是完成载体，保留资源峰值后删除仅本次新建的派生文件。
            (out / 'projection.sqlite').unlink()
            snapshot = db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
            provisional = CoreToken(pid, collection, root_id, rule, snapshot, '')
            for info in counts:
                digest = hashlib.sha256()
                count = 0
                for row in rows(db, provisional, info['name'], budget):
                    digest.update(row_bytes(row) + b'\n')
                    count += 1
                if (count, digest.hexdigest()) != (info['rows'], info['sha256']):
                    raise ValueError('H1投影全表回读不符')
            files = self._files(db, {'tables': counts}, snapshot, out, budget.guard)
            budget.counts['temporary_bytes'] = directory_bytes(out, budget)
            ready = dict(profile_id=pid, collection=asdict(collection), root_id=root_id, rule=RULE, rule_sha256=rule,
                         snapshot=snapshot, tables=counts, files=files, root=root, code_sha256=identity,
                         scope='H1_artificial_core_queries', admission='profile_validated',
                         H3='unresolved_external', resources=budget.report(), collection_input_resources=source.receipt['resources'])
            write_json(out / 'ready.json', ready)
            if (out / 'ready.json').stat().st_size > self.collection_limits.metadata_bytes:
                raise ValueError('H1完成清单元数据超限')
            digest = budget.hash(out / 'ready.json')
            with pg:
                self._qualify_collection(collection, budget, pg=pg, lock=True)
                if identity != code_identity():
                    raise ValueError('H1投影代码漂移')
                for f in files:
                    if budget.hash(f['path']) != f['sha256']:
                        raise ValueError('H1最终Parquet漂移')
                with pg.cursor() as c:
                    c.execute('UPDATE history_q3.core_profiles SET state=\'complete\',snapshot=%s,ready_sha=%s WHERE profile_id=%s AND state=\'candidate\'', (snapshot, digest, pid))
                    if c.rowcount != 1:
                        raise ValueError('H1完成登记竞争')
            return CoreToken(pid, collection, root_id, rule, snapshot, digest)
        except BaseException as error:
            if registered:
                pg.rollback()
                with pg, pg.cursor() as c:
                    c.execute('UPDATE history_q3.core_profiles SET state=\'failed\',reason=%s WHERE profile_id=%s AND state=\'candidate\'', (str(error), pid))
                write_json(out / 'FAILED.json', {'profile_id': pid, 'state': 'failed', 'reason': str(error)})
            raise
        finally:
            if db:
                db.close()
            pg.close()

    def _qualify_core(self, token, budget, pg=None, lock=False):
        budget.check()
        if not isinstance(token, CoreToken) or not re.fullmatch('[0-9a-f]{32}', token.profile_id):
            raise ValueError('须固定CoreToken')
        owned = pg is None
        if owned:
            pg = self._connect()
        primary = None
        try:
            self._qualify_collection(token.collection, budget, pg=pg, lock=lock)
            with pg.cursor() as c:
                c.execute('SELECT collection_id,root_id,rule_sha,state,snapshot,ready_sha,private_root FROM history_q3.core_profiles WHERE profile_id=%s' + (' FOR SHARE' if lock else ''), (token.profile_id,))
                if c.fetchone() != (token.collection.collection_id, token.root_id, token.rule_sha256, 'complete', token.snapshot, token.ready_sha256, str(self.root)):
                    raise ValueError('H1候选/撤销/错版')
                c.execute(sql.SQL('SELECT snapshot_id FROM {}.ducklake_snapshot WHERE snapshot_id=%s').format(sql.Identifier('hl_' + token.profile_id)), (token.snapshot,))
                if c.fetchone() != (token.snapshot,):
                    raise ValueError('H1固定快照不存在')
            budget.add('sql_calls', 2)
            out = self.data_root / token.profile_id
            if budget.hash(out / 'ready.json') != token.ready_sha256:
                raise ValueError('H1 ready变化')
            ready = read_json(out / 'ready.json', self.collection_limits.metadata_bytes)
            if (ready['code_sha256'] != code_identity() or token.rule_sha256 != rule_sha()
                    or ready['collection'] != json.loads(row_bytes(asdict(token.collection))) or ready['root_id'] != token.root_id
                    or ready['admission'] != 'profile_validated'):
                raise ValueError('H1规则/来源绑定变化')
            total = 0
            for f in ready['files']:
                path = safe_path(f['path'], (out / 'parquet',))
                total += f['bytes']
                if total > self.limits.max_total_bytes or path.stat().st_size != f['bytes'] or budget.hash(path) != f['sha256']:
                    raise ValueError('H1 Parquet损坏/超限')
            return ready
        except BaseException as error:
            primary = error
            raise
        finally:
            if owned:
                cleanup = release(pg, 'qualification PostgreSQL', primary, getattr(budget, 'cleanup_errors', None))
                if primary is None and cleanup is not None:
                    raise cleanup

    def core(self, token):
        from data_pipeline.history.event_index.query import CoreSession
        return CoreSession(self, token)
