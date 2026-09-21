"""C1固定保存态与事件输入；不计算cohort、不重放原始action、不发布。"""
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
import json
import hashlib
import resource
import sys
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2

from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch, SourceEnd, byte_size
from data_pipeline.analysis.detection.store import read_revisions, read_records
from data_pipeline.analysis.country_events.models import Time, Cursor
from data_pipeline.analysis.country_events.saved_contract import ProductionReceiptBinding, FixedLookup, verify_table_counts


@dataclass(frozen=True)
class DetectionBinding:
    dsn: str
    run_id: str
    snapshot: int


@dataclass(frozen=True)
class SavedDelta:
    cursor: Cursor
    at: Time
    source_id: str
    event_ref: str
    object_key: str
    after_presence: str
    after_path: str | None
    after_origin: int | None
    original: dict


@dataclass(frozen=True)
class SavedInvalidation:
    source_rank: int
    record: int
    at: Time
    message_ref: str
    object_key: str
    original: dict
    phase: str = 'message_before_elements'


@dataclass(frozen=True)
class SavedBatch:
    table: str
    rows: tuple
    observation_run: str
    observation_snapshot: int


@dataclass(frozen=True)
class OrderQuality:
    code: str
    previous_cursor: tuple
    cursor: tuple
    previous_at: Time
    at: Time
    scope: str = 'source_order_time_boundary_location'


@dataclass(frozen=True)
class CountryRevision:
    incident_id: str
    revision: int
    country: str
    trigger_cursor: Cursor
    trigger_time: Time
    onset: Time | None
    end: Time | None
    original: dict
    onset_cursor: None = None
    onset_link_state: str = 'unknown'
    carry_in_state: str = 'unknown'
    source_episode_ref: None = None
    episode_link_state: str = 'unknown'


@dataclass(frozen=True)
class InputCompletion:
    observation_run: str
    observation_snapshot: int
    detection_run: str
    detection_snapshot: int
    counts: dict
    selected_revisions: tuple
    enumeration: str
    boundary_capability: str
    baseline_initialized_elements: int
    legacy_timezone: str
    state_rule_version: str
    production_receipt_sha256: str
    lookup_stats: dict
    resource_limits: dict
    # 仅完成输入校验和枚举，不声称事件基线/cohort/数值已经计算。
    status: str = 'input_validated'


def require(condition, code):
    if not condition:
        raise ValueError(code)


def object_scope(row):
    """恢复保存scope的原标量类型，仅校验身份，不推算状态。"""
    scope = list(row['scope'])
    require(len(scope) == 11, 'invalid_object_scope')
    if type(row['scope']) is tuple and type(scope[9]) is bool:
        # Canonical公开typed原值。只保留原标量，不转成旧字符串后再解释。
        require(all(v is None or type(v) is int for v in (scope[i] for i in (2, 4, 5, 6, 7, 10))),
                'invalid_typed_object_scope')
    else:
        require(scope[9] in ('True', 'False'), 'invalid_object_scope')
        for index in (2, 4, 5, 6, 7, 10):
            scope[index] = int(scope[index]) if scope[index] is not None else None
        scope[9] = scope[9] == 'True'
    digest = hashlib.sha256(json.dumps(scope, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    require(digest == row['object_key'], 'object_scope_identity_mismatch')
    return scope


def legacy_time(value, timezone):
    if value is None or value == '':
        return None
    if isinstance(value, dict) and set(value) == {'$datetime'}:
        value = value['$datetime']
    require(isinstance(value, str), 'invalid_legacy_time')
    moment = datetime.fromisoformat(value)
    precision = moment.microsecond if '.' in value else None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=ZoneInfo(timezone))
    return Time(int(moment.timestamp()), precision)


class CountrySavedInput:
    """固定多表读取。仅在完整迭代得到InputCompletion后输入才算通过。"""
    metadata_tables = ('projection_metadata', 'baseline_mappings', 'paths', 'references',
                       'peers', 'associations', 'quality', 'eor')

    def __init__(self, reader: ObservationReader, detection: DetectionBinding, *,
                 legacy_timezone: str, production_receipt: ProductionReceiptBinding, scratch_root: str,
                 batch_rows=256, batch_bytes=4*1024**2, max_row_bytes=None,
                 max_events=100000, max_revisions=1000000, max_rss_bytes=2*1024**3,
                 max_scratch_bytes=4*1024**3, guard=lambda: None):
        require(type(detection.snapshot) is int and detection.snapshot >= 0
                and detection.run_id.isalnum(), 'invalid_detection_binding')
        max_row_bytes = batch_bytes if max_row_bytes is None else max_row_bytes
        require(1 <= batch_rows <= 10000 and min(batch_bytes,max_row_bytes,max_events,max_revisions,max_rss_bytes,max_scratch_bytes)>0, 'invalid_limits')
        ZoneInfo(legacy_timezone)
        self.reader, self.detection = reader, detection
        self.timezone, self.batch_rows, self.batch_bytes = legacy_timezone, batch_rows, batch_bytes
        self.max_events, self.max_revisions = max_events, max_revisions
        self.max_row_bytes, self.max_rss_bytes = max_row_bytes, max_rss_bytes
        self.max_scratch_bytes = max_scratch_bytes
        self.scratch_root = str(Path(scratch_root).resolve(strict=True))
        self.external_guard = guard
        self.workdir = None
        self.production_receipt = production_receipt
        self.sources = tuple([reader.manifest['baseline_source'], *reader.manifest['update_sources']])
        require(reader.sources == self.sources, 'unsupported_source_order')
        require(reader.starts[0].role == 'baseline' and all(s.role == 'update' for s in reader.starts[1:]),
                'unsupported_initialization_chain')
        self.state_rule = self._state_rule()
        self.receipt = production_receipt.read(reader, self.state_rule)
        self.identity = self._detection_identity()
        require(self.identity.get('input_run') == reader.run_id
                and self.identity.get('input_snapshot') == reader.snapshot
                and tuple(self.identity.get('selected_sources', ())) == self.sources,
                'detection_observation_binding_mismatch')
        require(self.identity.get('execution_mode') == 'frozen-fresh-process', 'detection_not_frozen')
        require(self.identity['_scope']['collector_id'] == reader.manifest['collector']
                and self.identity['_scope']['input_version'] == f'{reader.run_id}:{reader.snapshot}', 'detection_scope_mismatch')
        self.references = self._reference_receipts()
        require(set(self.references) == {r['sha256'] for r in reader.manifest.get('references',[])}, 'reference_manifest_mismatch')
        expected = self.identity.get('reference_sources', {})
        require(len(expected) == 11 and {r['source_id'] for r in expected.values()} == set(self.references),
                'reference_binding_mismatch')
        require(all(self.references[r['source_id']] == ('validated', r['rows']) for r in expected.values()),
                'reference_receipt_mismatch')

    def guard(self):
        self.external_guard()
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        require(rss <= self.max_rss_bytes, 'resource_limit:rss_bytes')
        if self.workdir is not None:
            size = sum(p.stat().st_size for p in Path(self.workdir).iterdir() if p.is_file())
            require(size <= self.max_scratch_bytes, 'resource_limit:scratch_bytes')

    def _check_row(self, row):
        self.guard()
        size = byte_size(asdict(row) if hasattr(row, '__dataclass_fields__') else row)
        self.guard()
        require(size <= self.max_row_bytes, 'resource_limit:row_bytes')
        return size

    def _state_rule(self):
        with psycopg2.connect(self.reader.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute('SELECT rule_version FROM domeye.run_specs WHERE run_id=%s', (self.reader.run_id,))
                row = c.fetchone()
        require(row is not None and row[0], 'missing_saved_state_rule')
        return row[0]

    def _detection_identity(self):
        with psycopg2.connect(self.detection.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute('''SELECT schema_name,state,snapshot,identity,scope,
                    (SELECT count(*) FROM detection.records WHERE run_id=r.run_id) FROM detection.runs r WHERE run_id=%s''',
                          (self.detection.run_id,))
                row = c.fetchone()
        require(row is not None and row[:3] == ('det_'+self.detection.run_id, 'complete', self.detection.snapshot),
                'detection_qualification_mismatch')
        return {**row[3], '_scope': row[4], '_records': row[5]}

    def _reference_receipts(self):
        with psycopg2.connect(self.reader.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute('SELECT source_id,state,row_count FROM domeye.reference_inputs WHERE run_id=%s',
                          (self.reader.run_id,))
                return {source: (state, count) for source, state, count in c.fetchall()}

    def _check(self):
        self.guard()
        connection = self.reader.connect()
        connection.close()
        require(self._detection_identity() == self.identity, 'detection_qualification_drift')
        require(self._reference_receipts() == self.references, 'reference_qualification_drift')
        require(self._state_rule() == self.state_rule, 'saved_state_rule_drift')
        require(self.production_receipt.read(self.reader, self.state_rule) == self.receipt, 'production_receipt_drift')

    def _rows(self, db, query, params=()):
        for batch in db.execute(query, params).fetch_record_batch(self.batch_rows):
            self.guard()
            for row in batch.to_pylist():
                self._check_row(row)
                yield row

    def _batches(self, table, rows):
        pending, size = [], 0
        for row in rows:
            self.guard()
            amount = self._check_row(row)
            if pending and (len(pending) >= self.batch_rows or size + amount > self.batch_bytes):
                yield SavedBatch(table, tuple(pending), self.reader.run_id, self.reader.snapshot)
                pending, size = [], 0
            pending.append(row)
            size += amount
        if pending:
            yield SavedBatch(table, tuple(pending), self.reader.run_id, self.reader.snapshot)

    def stream(self):
        self._check()
        db = self.reader.connect()
        # 每个表显式同一snapshot，不调用逐表解析latest的scan。
        def table(name):
            return self.reader.bound_table(name)
        counts, selected, last = {'country_revisions':0, 'changes':0, 'invalidations':0}, {}, None
        rollback = False
        scratch = tempfile.TemporaryDirectory(prefix='country-c1-',dir=self.scratch_root)
        self.workdir = scratch.name
        lookup = FixedLookup(scratch.name,self.guard)
        try:
            verify_table_counts(db,table,self.receipt,self.guard)
            lookup.build(db,table,self._rows)
            # 小元数据必须唯一；任一缺引用属于输入损坏，不以Unknown代替。
            for name, key in (('messages','message_id'),('elements','event_id'),('paths','path_key')):
                duplicate = db.execute(f'SELECT 1 FROM {table(name)} GROUP BY {key} HAVING count(*)>1 LIMIT 1').fetchone()
                require(duplicate is None, 'duplicate_'+key)
            for name, field, target, target_field in (
                ('elements','message_id','messages','message_id'),('elements','path_key','paths','path_key'),
                ('changes','event_id','elements','event_id'),('changes','after_event','elements','event_id'),
                ('changes','before_event','elements','event_id'),('changes','last_known_event','elements','event_id'),
                ('changes','after_path','paths','path_key'),('changes','before_path','paths','path_key'),
                ('invalidations','source','messages','message_id'),('invalidations','last_known_event','elements','event_id'),
                ('invalidations','last_known_path','paths','path_key'),('eor','message_id','messages','message_id'),
                ('quality','message_id','messages','message_id'),
                ('associations','message_id','messages','message_id'),('associations','reference_message','messages','message_id')):
                bad = db.execute(f'''SELECT 1 FROM {table(name)} a LEFT JOIN {table(target)} b
                    ON a.{field}=b.{target_field} WHERE a.{field} IS NOT NULL AND b.{target_field} IS NULL LIMIT 1''').fetchone()
                require(bad is None, 'missing_reference:'+name+'.'+field)
            for field in ('before_event','last_known_event'):
                bad = db.execute(f"SELECT 1 FROM {table('changes')} c JOIN {table('changes')} b ON c.{field}=b.event_id WHERE c.object_key<>b.object_key LIMIT 1").fetchone()
                require(bad is None, 'change_cross_object_reference:'+field)
            bad = db.execute(f"SELECT 1 FROM {table('invalidations')} i JOIN {table('changes')} c ON i.last_known_event=c.event_id WHERE i.object_key<>c.object_key LIMIT 1").fetchone()
            require(bad is None, 'invalidation_cross_object_reference')
            metadata = list(self._rows(db, f'SELECT * FROM {table("projection_metadata")} ORDER BY name'))
            require(len(metadata) == len({r['name'] for r in metadata}), 'duplicate_projection_metadata')
            meta = {r['name']: r['value'] for r in metadata}
            initialized = int(meta.get('baseline_initialized_elements', '-1'))
            require(0 <= int(meta.get('observation_snapshot', '-1')) <= self.reader.snapshot,
                    'missing_observation_snapshot')
            require(initialized == self.reader.starts[0].expected_elements and initialized > 0,
                    'baseline_initialization_mismatch')
            baseline_changes = db.execute(f'SELECT count(*) FROM {table("changes")} WHERE source_rank=0').fetchone()[0]
            require(baseline_changes == initialized, 'baseline_changes_incomplete')
            for name in self.metadata_tables:
                counts[name] = 0
                order = {'projection_metadata':'name','baseline_mappings':'baseline_source,peer_ip,peer_asn',
                         'paths':'path_key','references':'source_id,row','peers':'source_id,table_record,"index"',
                         'associations':'message_id,peer_ref','quality':'source_id,message_id,code,detail',
                         'eor':'message_id,afi,safi'}[name]
                def checked_rows(name=name, order=order):
                    ref_counts = {}
                    for row in self._rows(db, f'SELECT * FROM {table(name)} ORDER BY {order}'):
                        if name == 'baseline_mappings':
                            require(row['baseline_source'] == self.sources[0]
                                    and tuple(row['input_sources']) == self.sources[1:], 'mapping_source_mismatch')
                            require(row['status'] in ('calculation_mapping','conflicting_baseline_peers',
                                    'no_observed_endpoint','ambiguous_local_endpoints','missing_local_fields'), 'mapping_status_unknown')
                            require(json.loads(row['peer_refs']), 'mapping_peer_ref_missing')
                            for bgp_id, record, index in json.loads(row['peer_refs']):
                                found = lookup.peer(row['baseline_source'],record,index)
                                require(found == (bgp_id,row['peer_ip'],row['peer_asn']), 'mapping_peer_identity_mismatch')
                        if name == 'paths':
                            raw = row['attributes_raw']
                            require(hashlib.sha256(raw).hexdigest() == row['attributes_digest']
                                    and hashlib.sha256(bytes([row['asn_width']])+raw).hexdigest() == row['path_key'], 'path_content_identity_mismatch')
                        if name == 'references':
                            require(row['source_id'] in self.references, 'unbound_reference')
                            ref_counts[row['source_id']] = ref_counts.get(row['source_id'], 0)+1
                        counts[name] = counts.get(name, 0)+1
                        yield row
                    if name == 'references':
                        require(ref_counts == {k:v[1] for k,v in self.references.items()}, 'saved_reference_count_mismatch')
                yield from self._batches(name, checked_rows())
            # 公共Reader保留所有原消息、元素、LOCAL、EOR和质量，不从变化表反造事实。
            for item in self.reader.stream():
                self.guard()
                if isinstance(item, MessageBatch):
                    for message in item.messages:
                        cursor = (self.sources.index(message['source_id']), message['record'])
                        at = Time(message['epoch'], message['microsecond'])
                        if last and at.lower_us < last[1].lower_us:
                            rollback = True
                            yield OrderQuality('unsupported_order', last[0], cursor, last[1], at)
                        last = (cursor, at)
                    yield item
                elif isinstance(item, SourceEnd):
                    counts['source_messages'] = counts.get('source_messages',0)+item.messages
                    counts['source_elements'] = counts.get('source_elements',0)+item.elements
                    yield item
                else:
                    yield item
            query = f'''SELECT c.*,m.source_id,m.epoch,m.microsecond,m.record AS message_record,
                m.local_message,e.ordinal AS element_ordinal,e.prefix,e.afi,e.safi,e.path_id,e.path_id_present,
                e.peer_ip,e.peer_asn,m.local_ip,m.local_asn,m.interface,
                b.status AS mapping_status,b.endpoint_evidence AS mapping_endpoints FROM {table('changes')} c JOIN {table('elements')} e ON c.event_id=e.event_id
                JOIN {table('messages')} m USING(message_id)
                LEFT JOIN {table('baseline_mappings')} b ON c.source_rank=0 AND b.peer_ip=e.peer_ip AND b.peer_asn=e.peer_asn
                ORDER BY c.source_rank,c.record,c.ordinal'''
            def deltas():
                previous = None
                for row in self._rows(db, query):
                    cursor = Cursor(row['source_rank'],row['record'],row['ordinal'])
                    require(0 <= cursor.source_rank < len(self.sources)
                            and self.sources[cursor.source_rank] == row['source_id']
                            and row['record'] == row['message_record'] and row['ordinal'] == row['element_ordinal'],
                            'change_cursor_identity_mismatch')
                    require(previous is None or previous < cursor, 'duplicate_or_unordered_cursor')
                    previous = cursor
                    require(not row['local_message'], 'local_in_calculation_changes')
                    require(row['baseline_ref'] == self.sources[0], 'change_baseline_reference_mismatch')
                    require(row['rule_version'] == self.state_rule, 'unsupported_saved_state_rule')
                    require(row['after_event'] == row['event_id'] and row['after_epoch'] == row['epoch'], 'after_identity_mismatch')
                    require(row['after_presence'] in ('present','absent','unknown'), 'invalid_after_presence')
                    require(row['after_presence'] != 'present' or row['after_path'] is not None, 'present_path_missing')
                    scope = object_scope(row)
                    require(scope[0] == self.reader.manifest['collector'] and scope[1] == row['peer_ip']
                            and scope[2] == row['peer_asn'] and scope[6:] == [row['afi'],row['safi'],row['prefix'],row['path_id_present'],row['path_id']],
                            'object_element_identity_mismatch')
                    if cursor.source_rank == 0:
                        require(row['mapping_status'] is not None, 'baseline_mapping_missing')
                        if row['mapping_status'] == 'calculation_mapping':
                            endpoints = json.loads(row['mapping_endpoints'])
                            require(len(endpoints) == 1 and scope[3:6] == endpoints[0][:3], 'baseline_endpoint_mismatch')
                        else:
                            require(scope[3:6] == ['unmapped_rib:'+self.sources[0],None,None], 'ambiguous_baseline_endpoint_mismatch')
                    if cursor.source_rank != 0:
                        require(scope[3:6] == [row['local_ip'],row['local_asn'],row['interface']], 'object_endpoint_mismatch')
                    counts['changes'] = counts.get('changes',0)+1
                    yield SavedDelta(cursor,Time(row['epoch'],row['microsecond']),row['source_id'],row['event_id'],
                                     row['object_key'],row['after_presence'],row['after_path'],row['after_origin'],row)
            yield from self._batches('changes', deltas())
            source_order = 'CASE m.source_id '+ ' '.join(f'WHEN ? THEN {i}' for i in range(len(self.sources)))+' END'
            query = f'''SELECT i.*,m.source_id,m.record,m.epoch AS message_epoch,m.microsecond,m.kind,m.new_state,m.peer_ip,m.peer_asn,m.local_ip,m.local_asn,m.interface
                FROM {table('invalidations')} i JOIN {table('messages')} m ON i.source=m.message_id
                ORDER BY {source_order},m.record,i.object_key'''
            def invalidations():
                for row in self._rows(db, query, self.sources):
                    require(row['source_id'] in self.sources and row['kind']=='state_change'
                            and row['new_state'] != 6 and row['epoch'] == row['message_epoch'], 'invalidation_message_mismatch')
                    scope = object_scope(row)
                    require(scope[0] == self.reader.manifest['collector']
                            and scope[1:6] == [row['peer_ip'],row['peer_asn'],row['local_ip'],row['local_asn'],row['interface']],
                            'invalidation_endpoint_mismatch')
                    counts['invalidations'] = counts.get('invalidations',0)+1
                    yield SavedInvalidation(self.sources.index(row['source_id']),row['record'],
                                            Time(row['message_epoch'],row['microsecond']),row['source'],row['object_key'],row)
            yield from self._batches('invalidations', invalidations())
            # 核对完整Detection源枚举与回执；无事件不是过滤后的空列表证明。
            completion_count, source_ends, record_count = 0, [], 0
            for record in read_records(self.detection.dsn,self.detection.run_id,self.detection.snapshot):
                self.guard()
                record_count += 1
                require(record.get('scope') == self.identity['_scope'], 'detection_record_scope_mismatch')
                if record['kind'] == 'source_end':
                    source_ends.append(record['legacy'])
                if record['kind'] == 'input_completion':
                    completion_count += 1
                    require(record['legacy']['baseline_count'] == initialized, 'detection_baseline_count_mismatch')
                    require(record['legacy']['sources'] == source_ends, 'detection_completion_sources_mismatch')
                if record['kind'] in ('country_reduction','input_completion'):
                    yield from self._batches('detection_evidence', (record,))
            require(record_count == self.identity['_records'] and completion_count == 1, 'detection_enumeration_incomplete')
            require([(r['run_id'],r['snapshot'],r['source_id'],r['messages'],r['elements']) for r in source_ends]
                    == [(s.run_id,s.snapshot,s.source_id,s.expected_messages,s.expected_elements) for s in self.reader.starts],
                    'detection_source_receipts_mismatch')
            counts['detection_records'] = record_count
            for row in read_revisions(self.detection.dsn,self.detection.run_id,self.detection.snapshot):
                self.guard()
                if row['event_kind'] != 'country_outage':
                    continue
                require(row['subject_type']=='country' and row['subject_key'] and row['asn_roles']=='not_applicable',
                        'country_subject_mismatch')
                require(row['attributes'].get('incident_id') == row['incident_id']
                        and row['attributes'].get('revision') == row['revision']
                        and row['attributes'].get('object') == row['subject_key']
                        and row['attacker_asn'] is None and row['victim_asn'] is None, 'country_revision_identity_mismatch')
                observation = row['evidence'].get('observation', {})
                ref = observation.get('observation_id')
                source = lookup.event(ref)
                require(source is not None, 'incident_observation_reference_missing')
                for field in ('source_id','epoch','microsecond','path_id','path_id_present'):
                    require(observation.get(field) == source[field], 'incident_observation_field_mismatch:'+field)
                require(observation.get('message_ref') == source['message_id']
                        and observation.get('element_ordinal') == source['ordinal']
                        and observation.get('path_ref') == source['path_key']
                        and observation.get('collector_id') == self.reader.manifest['collector'], 'incident_observation_field_mismatch')
                require(observation.get('snapshot_ref') == f'{self.reader.run_id}:{self.reader.snapshot}'
                        and source['source_id'] in self.sources, 'incident_observation_identity_mismatch')
                at = Time(source['epoch'],source['microsecond'])
                observed = datetime.fromisoformat(observation['observed_at'])
                require(observed.tzinfo is not None and observed == datetime.fromtimestamp(at.epoch, timezone.utc)+timedelta(microseconds=at.microsecond or 0)
                        and row['observed_at'] == observed, 'incident_observation_time_mismatch')
                cursor = Cursor(self.sources.index(source['source_id']),source['record'],source['ordinal'])
                incident, revision = row['incident_id'],row['revision']
                require(isinstance(incident,str) and incident and isinstance(revision,int) and revision==selected.get(incident,0)+1,
                        'incident_revision_order_mismatch')
                selected[incident] = revision
                require(len(selected)<=self.max_events, 'resource_limit:event_count')
                counts['country_revisions'] = counts.get('country_revisions',0)+1
                require(counts['country_revisions'] <= self.max_revisions, 'resource_limit:revision_count')
                event = CountryRevision(incident,revision,row['subject_key'],cursor,at,
                                      legacy_time(row['legacy'].get('s_time'),self.timezone),
                                      legacy_time(row['legacy'].get('e_time'),self.timezone),row)
                self._check_row(event)
                yield event
            for name in (*self.metadata_tables, 'changes', 'invalidations'):
                require(counts[name] == self.receipt['actual_rows'][name], 'production_stream_count_mismatch:'+name)
            self._check()
            # 新只读连接避免复用开始时缓存的Parquet容器元数据。
            final_db = self.reader.connect()
            try:
                verify_table_counts(final_db,table,self.receipt,self.guard)
            finally:
                final_db.close()
            self._check()
            yield InputCompletion(self.reader.run_id,self.reader.snapshot,self.detection.run_id,self.detection.snapshot,
                                  counts,tuple(sorted(selected.items())), 'complete_empty' if not selected else 'complete',
                                  'unsupported_order' if rollback else 'requires_C2_boundary_evaluation',initialized,self.timezone,self.state_rule,self.production_receipt.sha256,dict(lookup.stats),
                                  dict(batch_rows=self.batch_rows,batch_target_bytes=self.batch_bytes,max_row_bytes=self.max_row_bytes,
                                       max_events=self.max_events,max_revisions=self.max_revisions,max_rss_bytes=self.max_rss_bytes,
                                       max_scratch_bytes=self.max_scratch_bytes,oversized_row_policy='single_row_within_max_row_bytes'))
        finally:
            lookup.close()
            scratch.cleanup()
            self.workdir = None
            db.close()
