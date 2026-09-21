"""从已完成集合的封存件/typed child生成H1领域表；不回访source或freeze。"""
from contextlib import closing
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from data_pipeline.common.event_records import convert_anomaly_record, deserialize_record
from data_pipeline.overview.index import DailyIndex
from data_pipeline.overview.input import overview_item, overview_search_text, overview_level_filter, validate_level_conflicts
from data_pipeline.overview.diagnostics import validate_diagnostic
from data_pipeline.history.event_collection.freeze import safe_path
from data_pipeline.history.event_collection.store import checked_row
from data_pipeline.history.database_import.codec import canonical as row_bytes
from data_pipeline.history.event_index.exact import Object, loads, native, canonical, scalars, equal
from data_pipeline.history.event_index.model import DEFINITIONS, ORDER


def read_span(root, path, start, end, budget, maximum=None):
    budget.check()
    size = end - start
    if size < 0 or size > min(maximum or budget.limits.max_row_bytes, budget.collection_limits.document_bytes):
        raise ValueError('H1原文档返回/解释字节超限')
    budget.add('profile_source_bytes', size, budget.limits.max_total_bytes)
    path = safe_path(root / path, (root,))
    with path.open('rb') as stream:
        stream.seek(start)
        raw = stream.read(size)
    budget.add('read_blocks', 1)
    if len(raw) != size:
        raise ValueError('H1封存文档截断')
    budget.check()
    return raw


class Projector:
    def __init__(self, history, collection, root_id, out, budget):
        self.h, self.token, self.root_id, self.out, self.budget = history, collection, root_id, out, budget
        self.source = history.data_root / collection.collection_id / 'source'
        self.db = sqlite3.connect(out / 'projection.sqlite')
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=OFF')
        self.db.execute('PRAGMA cache_size=-2048')
        self.db.execute('PRAGMA temp_store=FILE')
        for name, cols in DEFINITIONS.items():
            self.db.execute('CREATE TABLE ' + name + ' (' + ','.join('"' + c + '" ' + ('INTEGER' if t == 'i' else 'TEXT') for c, t in cols) + ')')
        self.db.execute('CREATE TABLE documents(document_id INTEGER PRIMARY KEY,file_id INTEGER,ordinal INTEGER,table_name TEXT,row_ordinal INTEGER,column_name TEXT,entity_path TEXT,byte_start INTEGER,byte_end INTEGER,storage_class TEXT)')
        self.db.execute('CREATE INDEX doc_source ON documents(file_id,table_name,row_ordinal,column_name)')
        self.db.execute('CREATE TABLE edges(document_id INTEGER,parent_file INTEGER,reference TEXT,role TEXT,resolution TEXT,target_file INTEGER)')
        self.db.execute('CREATE INDEX edge_source ON edges(parent_file,role,reference)')
        self.db.execute('CREATE TABLE payloads(file_id INTEGER,document_id INTEGER,digest TEXT,used INTEGER DEFAULT 0)')
        self.db.execute('CREATE INDEX source_payload ON payloads(file_id,digest,used)')
        self.files = {}
        self.occurrence = self.link = 0
        self.root = None

    def close(self):
        self.db.close()

    def disk(self):
        self.db.commit()
        pages = self.db.execute('PRAGMA page_count').fetchone()[0]
        size = pages * self.db.execute('PRAGMA page_size').fetchone()[0]
        self.budget.counts['projection_spool_bytes'] = size
        self.budget.counts['temporary_bytes'] = size
        self.budget.counts['profile_candidate_disk_peak_bytes'] = max(size, self.budget.counts.get('profile_candidate_disk_peak_bytes', 0))
        if size > self.budget.collection_limits.temporary_bytes:
            raise ValueError('H1投影暂存磁盘超限')
        self.budget.check()

    def add(self, table, row):
        checked_row(row, self.budget)
        self.budget.add('profile_rows', 1, self.budget.limits.max_total_rows)
        self.budget.add('profile_bytes', len(row_bytes(row)), self.budget.limits.max_total_bytes)
        cols = DEFINITIONS[table]
        self.db.execute('INSERT INTO ' + table + ' VALUES (' + ','.join('?' for _ in cols) + ')', [row.get(k) for k, _ in cols])
        if self.budget.counts['profile_rows'] % self.budget.limits.batch_rows == 0:
            self.disk()

    def rows(self, table):
        for row in self.db.execute('SELECT * FROM ' + table + ' ORDER BY ' + ORDER[table]):
            self.budget.check()
            yield dict(row)

    def document(self, doc, *, metadata=False):
        raw = read_span(self.source, doc['entity_path'], doc['byte_start'], doc['byte_end'], self.budget,
                        self.budget.collection_limits.metadata_bytes if metadata else None)
        return raw, loads(raw)

    def doc(self, file_id, column=None):
        rows = self.db.execute('SELECT * FROM documents WHERE file_id=? AND column_name IS ? LIMIT 2', (file_id, column)).fetchall()
        if len(rows) != 1:
            raise ValueError('H1文档定位不唯一')
        return dict(rows[0])

    def target(self, file_id, role, reference):
        rows = self.db.execute('SELECT DISTINCT target_file FROM edges WHERE parent_file=? AND role=? AND reference=? AND resolution=\'resolved\' LIMIT 2', (file_id, role, reference)).fetchall()
        if len(rows) != 1 or rows[0][0] is None:
            raise ValueError('H1声明依赖未闭合')
        return rows[0][0]

    def prepare(self, session):
        for name in ('files', 'documents', 'edges'):
            with closing(session.bulk(name)) as stream:
                for batch in stream:
                    for r in batch['rows']:
                        self.budget.check()
                        self.budget.add('projection_input_rows', 1, self.budget.limits.max_total_rows)
                        if name == 'files':
                            self.files[r['file_id']] = r
                        elif name == 'documents':
                            self.db.execute('INSERT INTO documents VALUES (?,?,?,?,?,?,?,?,?,?)', [r[k] for k in ('document_id', 'file_id', 'document_ordinal', 'table_name', 'row_ordinal', 'column_name', 'entity_path', 'byte_start', 'byte_end', 'storage_class')])
                        else:
                            self.db.execute('INSERT INTO edges VALUES (?,?,?,?,?,?)', [r[k] for k in ('document_id', 'parent_file', 'reference', 'role', 'resolution', 'target_file')])
                    self.disk()
        roots = [f for f in self.files.values() if f['root_id'] == self.root_id and f['role'] == 'core-index']
        if len(roots) != 1:
            raise ValueError('H1须明确唯一Core根scope')
        root_file = roots[0]['file_id']
        root_doc = self.doc(root_file)
        raw, root = self.document(root_doc, metadata=True)
        names = ('schema_version', 'data_profile', 'source', 'kinds', 'interpretation_version', 'days', 'window')
        declaration = {k: native(root.get(k)) for k in names}
        if root.get('diagnostics', None) is not None:
            declaration['diagnostics'] = native(root.get('diagnostics'))
        # 复用已固定当前项目的纯构造校验，不调用其原文件读取/最后键覆盖。
        index = DailyIndex(self.out / 'never-source-manifest.json', json.dumps(declaration).encode())
        self.root = dict(document_id=root_doc['document_id'], entity_path=root_doc['entity_path'],
                         byte_start=root_doc['byte_start'], byte_end=root_doc['byte_end'],
                         original_index_version=index.version, declaration=declaration)
        prefix = 'overview_index_v2_' if declaration['schema_version'].endswith('/v2') else 'overview_index_v1_'
        self.root['original_index_version'] = prefix + hashlib.sha256(raw).hexdigest()
        for day, entry in sorted(index.days.items()):
            fid = self.target(root_file, 'day-sqlite', entry['file'])
            self.day(day, fid, entry, declaration, index.day_interpretation(day), root_doc['document_id'])
        for day, entry in sorted(index.diagnostics.items()):
            fid = self.target(root_file, 'diagnostic', entry['file'])
            doc = self.doc(fid)
            _, diagnostic = self.document(doc, metadata=True)
            values = {k: native(diagnostic.get(k)) for k in ('schema_version', 'stage', 'source', 'data_profile', 'window', 'compiler_sha256', 'reasons')}
            validate_diagnostic(values, declaration, entry['window'])
            self.add('core_days', dict(day=day, state='validation_failed', root_document=root_doc['document_id'], **self.location(doc),
                                      root_interpretation=declaration['interpretation_version'], window_start=entry['window']['start'], window_end=entry['window']['end_exclusive']))
            for i, reason in enumerate(values['reasons']):
                self.add('core_diagnostics', dict(day=day, reason_ordinal=i, stage=values['stage'], kind=reason['kind'],
                                                 code=reason['code'], count=reason['count'], document_id=doc['document_id']))
        self.disk()

    @staticmethod
    def location(doc):
        return {k: doc[k] for k in ('document_id', 'file_id', 'entity_path', 'byte_start', 'byte_end')}

    def day(self, day, fid, entry, declaration, interpretation, root_doc):
        doc = self.doc(fid, 'manifest')
        raw, provenance = self.document(doc, metadata=True)
        names = ('schema_version', 'data_profile', 'source', 'kinds', 'interpretation_version', 'window', 'records')
        p = {k: native(provenance.get(k)) for k in names}
        if provenance.get('level_conflicts', None) is not None:
            p['level_conflicts'] = native(provenance.get('level_conflicts'))
        if (p['schema_version'] != 'core-overview-input/v1'
                or 'overview_v1_' + hashlib.sha256(raw).hexdigest() != entry['input_version']
                or any(p[k] != declaration[k] for k in ('data_profile', 'source', 'kinds'))
                or p['interpretation_version'] != interpretation or p['records']['count'] != entry['count']
                or p['window']['source'] != entry['window']['source']
                or any(datetime.fromisoformat(p['window'][k]) != datetime.fromisoformat(entry['window'][k]) for k in ('start', 'end_exclusive'))):
            raise ValueError('H1根/日/provenance身份、口径、窗口或人口冲突')
        if interpretation.endswith('/v1'):
            validate_level_conflicts(p, [])
        elif not isinstance(p.get('level_conflicts'), dict):
            raise ValueError('H1缺等级冲突声明')
        input_file = self.target(fid, 'records-jsonl', p['records']['file'])
        for input_doc in self.db.execute('SELECT * FROM documents WHERE file_id=? ORDER BY ordinal', (input_file,)):
            _, value = self.document(input_doc)
            digest = hashlib.sha256(canonical(value).encode()).hexdigest()
            self.db.execute('INSERT INTO payloads(file_id,document_id,digest) VALUES (?,?,?)', (input_file, input_doc['document_id'], digest))
        file = self.files[fid]
        child = self.token.children[file['child_index']]
        component = self.h.component(child)
        tables = component['original']['tables']
        ti = next(i for i, t in enumerate(tables) if t['name'] == 'records')
        columns = [c['name'] for c in tables[ti]['columns']]
        expected_cols = ('reference', 'kind', 'object', 'start_time', 'hour', 'family', 'level', 'severity', 'search', 'item', 'payload')
        if not set(expected_cols) <= set(columns):
            raise ValueError('H1原检索列不完整')
        count = 0
        conflicts_seen = set()
        with self.h.bulk(child, table_indices=(ti,)) as stream:
            for batch in stream:
                for source in batch['rows']:
                    self.budget.check()
                    self.budget.add('source_record_rows', 1, self.budget.limits.max_total_rows)
                    cells = dict(zip(columns, source['values']))
                    self.record(day, fid, source['occurrence']['ordinal'], cells, p, input_file, conflicts_seen)
                    count += 1
        if not stream.receipt or count != entry['count'] or conflicts_seen != set(p.get('level_conflicts', {})):
            raise ValueError('H1日记录/等级冲突人口不完整')
        if self.db.execute('SELECT count(*) FROM payloads WHERE file_id=? AND used=0', (input_file,)).fetchone()[0]:
            raise ValueError('H1补验JSONL存在未匹配occurrence')
        self.add('core_days', dict(day=day, state='available', root_document=root_doc, **self.location(doc),
                                  input_version=entry['input_version'], root_interpretation=declaration['interpretation_version'],
                                  input_interpretation=interpretation, window_start=entry['window']['start'], window_end=entry['window']['end_exclusive'], record_count=count))

    def record(self, day, fid, ordinal, cells, p, input_file, conflicts_seen):
        docs = {d['column_name']: dict(d) for d in self.db.execute('SELECT * FROM documents WHERE file_id=? AND table_name=\'records\' AND row_ordinal=?', (fid, ordinal))}
        item_raw, item = self.document(docs['item'])
        payload_raw, exact = self.document(docs['payload'])
        for col, raw in (('item', item_raw), ('payload', payload_raw)):
            if cells[col]['storage_class'] != docs[col]['storage_class'] or bytes.fromhex(cells[col]['value']) != raw:
                raise ValueError('H1嵌入文档与typed child不一致')
        digest = hashlib.sha256(canonical(exact).encode()).hexdigest()
        match = self.db.execute('SELECT rowid FROM payloads WHERE file_id=? AND digest=? AND used=0 ORDER BY document_id LIMIT 1', (input_file, digest)).fetchone()
        if match is None:
            raise ValueError('H1 item/payload/补验JSONL多重集不一致')
        self.db.execute('UPDATE payloads SET used=1 WHERE rowid=?', (match[0],))
        # 前面已经精确拒绝payload重键。仅使用旧codec验证其自身content_version；
        # 丢弃此兼容解码结果，投影/字段对账继续使用独立精确树，绝不用其float作oracle。
        deserialize_record(payload_raw)
        result = native(exact, tags=True)
        record = result['record']
        ref = record['identity']['legacy_reference']
        if (record['schema_version'] != 'anomaly-record/v1' or record['mapping_version'] not in ('legacy-anomaly-mapping/v1', 'legacy-anomaly-mapping/v2')
                or record['association']['state'] != 'matched' or record['association']['candidate_count'] != 1
                or record['data_profile'] != p['data_profile'] or record['identity']['source_instance'] != p['source']['instance']
                or record['source']['instance'] != p['source']['instance'] or record['source']['read_scope'] != p['window']
                or record['source']['collector_id'] not in (None, 'rrc25') or ref.split('/')[-1] != p['source']['code']
                or record['common']['kind'] not in p['kinds']
                or (record['mapping_version'].endswith('/v2') and not p['interpretation_version'].endswith('/v3'))):
            raise ValueError('H1原记录关联/来源/profile未准入')
        # 复算现项目纯转换，核对完整已解释共同字段/明细；不重命名原身份。
        regenerated = convert_anomaly_record(ref, [record['raw_fields']], source={**record['source'], 'read_at': result['read_at']},
                                             data_profile=p['data_profile'], existing_id=record['identity']['legacy_id'],
                                             preserve_unresolved_as_set=record['mapping_version'].endswith('/v2'))['record']
        if any(not equal(regenerated[k], record[k]) for k in ('common', 'details', 'association', 'identity', 'limitations')):
            raise ValueError('H1原字段与既有解释矛盾')
        start = datetime.fromisoformat(record['common']['start_time']['value'].replace('Z', '+00:00'))
        if record['common']['start_time']['state'] != 'recorded' or not datetime.fromisoformat(p['window']['start']) <= start < datetime.fromisoformat(p['window']['end_exclusive']):
            raise ValueError('H1记录不在日窗口')
        if ref in p.get('level_conflicts', {}):
            validate_level_conflicts({**p, 'level_conflicts': {ref: p['level_conflicts'][ref]}}, [result])
            conflicts_seen.add(ref)
        expected = overview_item(result, p.get('level_conflicts'))
        known = {'reference', 'content_version', 'kind', 'object', 'start_time', 'end_time', 'level', 'address_family',
                 'asns', 'record_number', 'level_conflict', 'object_identity', 'parent_prefix', 'country_name'}
        if ({k for k, _ in item.members} & known != set(expected)
                or any(not equal(native(item.get(k)), v) for k, v in expected.items())):
            raise ValueError('H1 item与payload投影矛盾')
        values = dict(reference=expected['reference'], kind=expected['kind'], object=expected['object'], start_time=expected['start_time'],
                      hour=start.astimezone(ZoneInfo(p['data_profile']['timezone'])).hour, family=expected['address_family'],
                      level=overview_level_filter(expected), severity={'high': 0, 'middle': 1, 'low': 2}.get(expected['level'], 3), search=overview_search_text(expected))
        for key, expected_value in values.items():
            cell = cells[key]
            kind = 'integer' if type(expected_value) is int else 'text'
            actual = int(cell['value']) if cell['storage_class'] == 'integer' else bytes.fromhex(cell['value']).decode('utf8') if cell['storage_class'] == 'text' else None
            if cell['storage_class'] != kind or actual != expected_value:
                raise ValueError('H1原检索列与item/payload冲突: ' + key)
        row = dict(day=day, occurrence=self.occurrence, file_id=fid, source_ordinal=ordinal, content_version=result['content_version'], **values)
        for key, doc in docs.items():
            row.update({key + '_' + target: doc[source] for target, source in [('document', 'document_id'), ('path', 'entity_path'), ('start', 'byte_start'), ('end', 'byte_end'), ('storage', 'storage_class')]})
        self.add('core_records', row)
        record_member = next(i for i, (key, _) in enumerate(exact.members) if key == 'record')
        for i, scalar in enumerate(scalars(exact.get('record'), self.budget.collection_limits, p['data_profile']['timezone'], (['record', record_member],))):
            self.add('core_scalars', dict(occurrence=self.occurrence, scalar_ordinal=i, document_id=docs['payload']['document_id'], **scalar))
        self.add('core_links', dict(link_ordinal=self.link, occurrence=self.occurrence, document_id=docs['payload']['document_id'],
                                   reference=ref, role='H3_original_detail', resolution='unresolved_external'))
        self.link += 1
        for edge in self.db.execute('SELECT * FROM edges WHERE document_id=? ORDER BY rowid', (docs['payload']['document_id'],)):
            self.add('core_links', dict(link_ordinal=self.link, occurrence=self.occurrence, document_id=edge['document_id'],
                                       reference=edge['reference'], role=edge['role'], resolution=edge['resolution'], target_file=edge['target_file']))
            self.link += 1
        self.occurrence += 1
