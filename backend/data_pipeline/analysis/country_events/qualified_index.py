"""M3结果级C4：实际完整组件回执、全定位回读和PG准入；不创建P。"""
from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
import hashlib
import json
import sqlite3
import time

import pyarrow.parquet as pq
import psycopg2

from data_pipeline.analysis.country_events import qualified_schema as m3_schema, qualified_locators as m3_index, event_aggregation as c2
from data_pipeline.analysis.country_events.snapshot_store import file_hash, fsync_tree, cleanup
from data_pipeline.analysis.country_events.snapshot_schema import encode, decode
from data_pipeline.analysis.country_events.snapshot_reader import ReadBatch, ReadReceipt
from data_pipeline.analysis.country_events.snapshot_access import FixedComponentAccess
from data_pipeline.analysis.country_events.qualified_store import M3ComponentReader
from data_pipeline.analysis.country_events.selection_admission import _register_country_admission, verify_country_admission
from data_pipeline.analysis.country_events.selection_contract import CountryReadBinding, CountryAdmissionProof, QueryLimits, FileEntity, contract_json, contract_value, digest, canonical, result_id
from data_pipeline.analysis.country_events.selection_index import Budget, stamp, check_entities, readonly
from data_pipeline.analysis.country_events.route_contract import CountryCoverage, DIMENSIONS

VERSION = 'country-query-m3/v1'


@contextmanager
def hold_component(component_dsn, binding):
    """country.component，stage40：只锁定一个实际组件控制行。"""
    pg = psycopg2.connect(component_dsn); error = None
    try:
        with pg.cursor() as cur:
            cur.execute('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')
            if cur.fetchone() != (binding.system_id,binding.database_oid):raise ValueError('M3 C3锁数据库串绑')
            cur.execute('SELECT schema_name,state,snapshot,root,manifest_sha256 FROM country_components.components WHERE component_id=%s FOR SHARE',(binding.component_id,))
            if cur.fetchone() != (binding.schema_name,'complete',binding.snapshot,binding.root,binding.manifest_sha256):
                raise ValueError('M3 C3实际组件锁绑定不符')
        yield
    except BaseException as exc:
        error = exc; raise
    finally:cleanup((pg.rollback,pg.close),error)


class M3Access(FixedComponentAccess):
    tables, schemas = m3_schema.TABLES, m3_schema.SCHEMAS
    decode_row = staticmethod(m3_schema.row_decode)

    def __init__(self,*args,runtime=None,**kwargs):
        if runtime is not None:self.connect=runtime.lake_connect
        super().__init__(*args,**kwargs)


@dataclass(frozen=True)
class M3Descriptor:
    binding: CountryReadBinding
    proof: CountryAdmissionProof
    entities: tuple
    logical_run_id: str
    input_binding_id: str
    window_us: tuple
    event_count: int
    admitted_empty: bool


def prepare_qualified_country_index(component_dsn, component_binding, *, source, output, limits=QueryLimits()):
    source.check()
    with hold_component(component_dsn,component_binding):
        return _prepare(component_dsn,component_binding,source=source,output=output,limits=limits)


def _prepare(component_dsn, component_binding, *, source, output, limits):
    """仅离线builder在全部实际来源锁内登记；不存在从外部导入proof的入口。"""
    source.check(); root = Path(output).resolve(); root.mkdir(parents=True, exist_ok=False)
    budget = Budget(root, limits); budget.extra_roots = lambda: (component_binding.root,)
    db = sqlite3.connect(root/'index.sqlite'); db.execute('PRAGMA cache_size=-8192')
    db.executescript(m3_index.DDL + '''
        CREATE TABLE rows(sequence INTEGER PRIMARY KEY,table_name TEXT NOT NULL,row_hash TEXT NOT NULL,
            row_group INTEGER,row_offset INTEGER);
        CREATE TABLE events(incident TEXT PRIMARY KEY,revision TEXT NOT NULL,sequence INTEGER NOT NULL,
            lifecycle_typed TEXT NOT NULL);
        CREATE TABLE revisions(incident TEXT,revision TEXT,sequence INTEGER PRIMARY KEY);
    ''')
    stats = Counter(); receipt = completion = None; source_digest = hashlib.sha256()
    params = None; enumeration = None
    try:
        reader = M3ComponentReader(component_dsn, component_binding, source=source, guard=budget,
                                   **{k:getattr(limits,k) for k in ('batch_rows','batch_bytes','max_row_bytes','max_rows','max_rss_bytes','max_disk_bytes')})
        params = decode(reader.manifest['parameters'])
        proof_by_event = {}
        for r in source.results:
            budget()
            if len(proof_by_event)>=limits.max_rows:raise ValueError('resource_limit:M3_C4_resident_events')
            proof_by_event[r['raw']['incident_id'],r['raw']['revision']]=r['lifecycle']
        stream = reader.stream(); stats['stream_calls'] += 1
        try:
            for part in stream:
                if isinstance(part, ReadReceipt):
                    if receipt is not None:raise ValueError('M3 C4重复完整回执')
                    receipt = part; continue
                if not isinstance(part, ReadBatch) or receipt is not None:raise ValueError('M3 C4完整流协议不符')
                for item in part.rows:
                    budget(); seq = stats['source_rows']
                    table, row = m3_schema.row_encode(seq, item)
                    if len(encode(row).encode()) > min(limits.max_row_bytes, limits.batch_bytes):
                        raise ValueError('resource_limit:M3_C4_row_bytes')
                    db.execute('INSERT INTO rows VALUES (?,?,?,NULL,NULL)',(seq,table,row['_row_hash']))
                    m3_index.append_locator(db, seq, item)
                    source_digest.update(bytes.fromhex(row['_row_hash'])); stats['source_rows'] += 1
                    if isinstance(item, c2.C2Completion):completion = item
                    elif isinstance(item.value, c2.EventStatus):
                        lifecycle = proof_by_event[item.incident_id, item.revision]
                        db.execute('INSERT INTO events VALUES (?,?,?,?)',
                                   (item.incident_id,str(item.revision),seq,encode(lifecycle)))
                    elif isinstance(item.value, c2.InputEvidence) and item.value.kind == 'event_revision':
                        db.execute('INSERT INTO revisions VALUES (?,?,?)',(item.incident_id,str(item.revision),seq))
                    elif isinstance(item.value, CountryCoverage) and item.value.source_id is None and item.value.dimension == 'event_enumeration':
                        if enumeration is not None:raise ValueError('M3 C4事件枚举覆盖重复')
                        enumeration = item.value
        finally:stream.close()
        if (receipt is None or not receipt.full_body_validated or receipt.binding != component_binding
                or receipt.rows != stats['source_rows'] or completion is None or enumeration is None):
            raise ValueError('M3 C4缺实际全组件回执或独立事件覆盖')
        stats['locator_passes'] += 1
        for table in m3_schema.TABLES:
            budget(); path = Path(component_binding.root)/'data'/(table+'.parquet')
            with path.open('rb') as handle:
                parquet = pq.ParquetFile(handle)
                for group in range(parquet.num_row_groups):
                    budget()
                    if parquet.metadata.row_group(group).total_byte_size > limits.max_read_bytes:
                        raise ValueError('resource_limit:M3_C4_locator_group')
                    batch = parquet.read_row_group(group, columns=['_sequence','_row_hash'])
                    for offset, row in enumerate(batch.to_pylist()):
                        budget()
                        changed = db.execute('UPDATE rows SET row_group=?,row_offset=? WHERE sequence=? AND table_name=? AND row_hash=? AND row_group IS NULL',
                                             (group,offset,row['_sequence'],table,row['_row_hash'])).rowcount
                        if changed != 1:raise ValueError('M3 C4物理定位与完整源流不符')
        if db.execute('SELECT 1 FROM rows WHERE row_group IS NULL LIMIT 1').fetchone():
            raise ValueError('M3 C4定位缺项')
        db.commit(); actual_digest = hashlib.sha256()
        access = M3Access(component_dsn, component_binding, reader.manifest, limits=limits, guard=budget)
        try:
            cursor = db.execute('SELECT table_name,row_group,row_offset,sequence,row_hash FROM rows ORDER BY sequence')
            while locators := cursor.fetchmany(limits.batch_rows):
                decoded = access.read(locators)
                for table, group, offset, seq, sha in locators:
                    budget()
                    if m3_schema.row_encode(seq, decoded[seq])[1]['_row_hash'] != sha:
                        raise ValueError('M3 C4完整定位回读不符')
                    actual_digest.update(bytes.fromhex(sha)); stats['readback_rows'] += 1
        finally:
            stats.update({'access_'+k:v for k,v in access.stats.items()}); access.close()
        if stats['readback_rows'] != stats['source_rows'] or actual_digest.digest() != source_digest.digest():
            raise ValueError('M3 C4定位回读不完整')
        events = db.execute('SELECT count(*) FROM events').fetchone()[0]
        if events != completion.event_count or events != enumeration.event_count:
            raise ValueError('M3 C4原事件/独立覆盖枚举不符')
        new_rows = db.execute('SELECT count(*) FROM m3_locators').fetchone()[0]
        if db.execute('PRAGMA integrity_check').fetchone() != ('ok',):raise ValueError('M3 C4索引完整性失败')
        db.close(); db = None
        index_sha = file_hash(root/'index.sqlite',budget)
        validation = dict(source_rows=stats['source_rows'], readback_rows=stats['readback_rows'],
                          source_digest=source_digest.hexdigest(), new_rows=new_rows, event_count=events,
                          admitted_empty=events == 0 and enumeration.execution == 'complete' and enumeration.coverage == 'complete')
        identity = dict(query_version=VERSION, component=asdict(component_binding), index_sha256=index_sha,
                        logical_run_id=params['logical_run_id'], input_binding_id=source.binding.input_binding_id,
                        window_us=list(params['result_window_us']), validation=validation,
                        code={p.name:file_hash(p) for p in sorted([*(Path(__file__).parent / name for name in ('route_reference_audit.py', 'route_binding.py', 'qualified_store.py', 'source_audit.py', 'route_contract.py', 'route_history.py', 'history_staging.py', 'qualified_locators.py', 'route_inputs.py', 'result_admission.py', 'result_catalog.py', 'admission_sources.py', 'result_qualification.py', 'qualification_evidence.py', 'qualified_index.py', 'qualified_reader.py', 'qualified_event_results.py', 'route_change_adapter.py', 'qualified_schema.py', 'calculation_staging.py', 'route_source_reader.py', 'input_staging.py', 'route_gap_adapter.py', 'route_time_index.py')),
                              *(Path(__file__).parent / name for name in ('selection_reader.py', 'selection_admission.py', 'selection_contract.py', 'selection_index.py')),Path(__file__).parent/'snapshot_access.py'])})
        rid = result_id(component_binding); model = 'country_read_' + digest(identity)
        proof = CountryAdmissionProof(rid,model,component_binding.manifest_sha256,encode(asdict(receipt)),
                                       digest(validation),new_rows,stats['source_rows'],1,1,stats['readback_rows'])
        entities = tuple(stamp(Path(component_binding.root)/f['path'],f['sha256']) for f in reader.manifest['files'])
        entities += (stamp(Path(component_binding.root)/'manifest.json',component_binding.manifest_sha256),
                     stamp(Path(component_binding.root)/'ready.json',file_hash(Path(component_binding.root)/'ready.json')),
                     stamp(root/'index.sqlite',index_sha))
        stats.update(budget.stats);stats['total_elapsed_microseconds_before_manifest'] = round((time.monotonic()-budget.started)*1000000)
        manifest = dict(identity=identity,proof=contract_json(proof),entities=[asdict(e) for e in entities],
                        costs=dict(stats),upstream_typed=reader.manifest['upstream'])
        (root/'manifest.json').write_text(canonical(manifest)); fsync_tree(root)
        binding = CountryReadBinding(component_binding,rid,model,str(root),file_hash(root/'manifest.json'),None,VERSION)
        reader.check(); check_entities(entities)
        def finalize():
            (root/'ready.json').write_text(contract_json(binding)); fsync_tree(root)
            reader.check(); check_entities(entities)
        _register_country_admission(component_dsn,binding,proof,before_commit=finalize)
        return binding,proof
    except BaseException:
        ready = root/'ready.json'
        if ready.exists():ready.unlink()
        raise
    finally:
        if db is not None:db.close()


def inspect_qualified_country_result(component_dsn, binding, proof, *, source, limits=QueryLimits()):
    """真实PG准入、全量原回执、窗口身份与固定实体检查；无外部布尔verifier。"""
    source.check(); root = Path(binding.root)
    budget = Budget(root, limits); budget.extra_roots = lambda:(binding.component.root,)
    with verify_country_admission(component_dsn,binding,proof):
        if binding.query_version != VERSION or file_hash(root/'manifest.json',budget) != binding.manifest_sha256:
            raise ValueError('M3 C4结果schema或manifest不符')
        if contract_value((root/'ready.json').read_text()) != binding:raise ValueError('M3 C4结果未ready')
        manifest = json.loads((root/'manifest.json').read_text()); identity = manifest['identity']
        if (identity['query_version'] != VERSION or identity['component'] != asdict(binding.component)
                or result_id(binding.component) != binding.result_id or 'country_read_'+digest(identity) != binding.read_model_id
                or contract_value(manifest['proof']) != proof):
            raise ValueError('M3 C4实际身份/完整Proof不符')
        if (proof.component_manifest_sha256 != binding.component.manifest_sha256
                or proof.result_id != binding.result_id or proof.read_model_id != binding.read_model_id
                or proof.stream_calls != 1 or proof.locator_passes != 1
                or proof.source_rows != proof.readback_rows
                or digest(identity['validation']) != proof.index_validation_sha256):
            raise ValueError('M3 C4完整验证计数或原组件不符')
        receipt = decode(proof.c3_receipt_typed)
        if (receipt['binding'] != asdict(binding.component) or receipt['mode'] != 'component'
                or receipt['full_body_validated'] is not True or receipt['rows'] != proof.source_rows):
            raise ValueError('M3 C4缺原实际完整C3回执')
        reader = M3ComponentReader(component_dsn,binding.component,source=source,guard=budget)
        params = decode(reader.manifest['parameters'])
        if (identity['logical_run_id'] != params['logical_run_id'] or identity['input_binding_id'] != source.binding.input_binding_id
                or tuple(identity['window_us']) != params['result_window_us'] or manifest['upstream_typed'] != reader.manifest['upstream']):
            raise ValueError('M3 C4窗口/完整原输入串绑')
        entities = tuple(FileEntity(**e) for e in manifest['entities']); check_entities(entities)
        if file_hash(root/'index.sqlite',budget) != identity['index_sha256']:raise ValueError('M3 C4索引摘要不符')
        db = readonly(root/'index.sqlite')
        try:
            if (db.execute('PRAGMA integrity_check').fetchone() != ('ok',)
                    or db.execute('SELECT count(*) FROM rows').fetchone()[0] != proof.source_rows
                    or db.execute('SELECT count(*) FROM m3_locators').fetchone()[0] != proof.index_rows
                    or db.execute('SELECT 1 FROM rows WHERE row_group IS NULL OR row_offset IS NULL LIMIT 1').fetchone()):
                raise ValueError('M3 C4实际索引不完整')
        finally:db.close()
        entities += (stamp(root/'manifest.json',binding.manifest_sha256),stamp(root/'ready.json',file_hash(root/'ready.json')))
        return M3Descriptor(binding,proof,entities,identity['logical_run_id'],identity['input_binding_id'],
                             tuple(identity['window_us']),identity['validation']['event_count'],identity['validation']['admitted_empty'])
