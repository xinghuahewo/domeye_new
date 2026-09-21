"""Trend有限P1：完整admit、轻量current、单目标stage60锁与关闭后回执。"""
from contextlib import contextmanager
from contextvars import ContextVar
import threading
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
import hashlib
import re
import subprocess
import sys
import uuid

from psycopg2.extras import Json

from data_pipeline.bgp.archive import admission as pg_api
from data_pipeline.bgp.archive.value_codec import fields, digest, ADMISSION_FIELDS, PHYSICAL_FIELDS, VALIDATOR_FIELDS, ENTITY_FIELDS, LOCK_FIELDS, REQUEST_FIELDS
from data_pipeline.analysis.country_events.snapshot_store import cleanup
from data_pipeline.results.manifest_io import encode as json_text
from data_pipeline.analysis.country_trends import stream_schema as schema
from data_pipeline.analysis.country_trends.runtime import Runtime
from data_pipeline.analysis.country_trends.result_catalog import catalog, FORMAT as CATALOG_FORMAT
from data_pipeline.analysis.country_trends.stream_budget import StreamBudget, code_binding
from data_pipeline.analysis.country_trends import part_manifest as parts
from data_pipeline.common import process_resources as resources

ROOT = Path(__file__).resolve().parents[4]
REGISTRY = 'country_trends.publication_admissions'
CODEC = schema.CODEC
VIEWS = schema.TABLES


def _parts(runtime, binding):
    runtime.check_binding(binding)
    fields(binding, {'binding', 'proof'})
    b = binding['binding']
    from dataclasses import fields as dataclass_fields
    from data_pipeline.analysis.country_trends.stream_store import Binding
    fields(b, {f.name for f in dataclass_fields(Binding)})
    if (b['schema_version'] != schema.VERSION or b['profile'] != schema.PROFILE
            or type(b['component_id']) is not str
            or not re.fullmatch('[0-9a-f]{32}', b['component_id'])
            or type(b['system_id']) is not str or not b['system_id'].isdigit()
            or type(b['catalog_id']) is not str or not re.fullmatch('[0-9a-f]{32}', b['catalog_id'])
            or type(b['manifest_sha256']) is not str or not re.fullmatch('[0-9a-f]{64}', b['manifest_sha256'])
            or b['schema_name'] != 'trend_m3_' + b['component_id']
            or type(b['snapshot']) is not int or b['snapshot'] < 0
            or type(b['database_oid']) is not int or b['database_oid'] < 1
            or runtime.path(b['root']) != runtime.output_root):
        raise ValueError('trend_p1_binding_scope')
    inputs = schema.decode(binding['proof']['inputs_typed'])
    if b['result_id'] != 'trend_m3_' + hashlib.sha256(schema.encode(inputs).encode()).hexdigest():
        raise ValueError('trend_p1_result_identity')
    if (inputs['root'] != b['root'] or inputs['identity'] != {k:b[k] for k in ('system_id','database_oid','catalog_id')}
            or inputs['execution_profile'] != runtime._profile or inputs['window_us'] != runtime.country_window_us
            or schema.encode(inputs['dependencies']) != schema.encode(runtime.dependency_admissions)
            or schema.encode(inputs['reference_binding']) != schema.encode(runtime.reference_binding)
            or inputs['feature_selections'] != runtime.feature_selections):
        raise ValueError('trend_p1_complete_input_binding')
    return b


def _physical(b):
    return dict(system_identifier=b['system_id'], database_oid=b['database_oid'], catalog=b['catalog_id'],
                schema=b['schema_name'], root=b['root'], snapshot=b['snapshot'])


def _target(b, namespace, key):
    return dict(stage=60, system_identifier=b['system_id'], database_oid=b['database_oid'], namespace=namespace, key=key)


def _rules(runtime):
    from data_pipeline.common.admission_locks import code_sha
    code = code_binding(); code['lock_connection_code'] = code_sha()
    return dict(version='country-trend-publication/v1', code_sha256=digest(code),
                typed_schema=schema.VERSION, typed_codec=CODEC,
                rules_digest=digest(dict(profile=runtime._profile, science='country-trend-qualified-science/v1',
                                        full_source_recompute=True, views=VIEWS, single_lock_stage=60,
                                        physical_format=parts.FORMAT, catalog_format=CATALOG_FORMAT)))


def _shape(a):
    fields(a, ADMISSION_FIELDS); fields(a['physical'], PHYSICAL_FIELDS); fields(a['validator'], VALIDATOR_FIELDS)
    if (a['contract'] != 'component-publication-admission/v1' or a['owner'] != 'trend'
            or a['binding_codec'] != CODEC or a['admission_id'] != digest({k:v for k,v in a.items() if k != 'admission_id'})):
        raise ValueError('trend_p1_admission_shape')
    for e in a['entities']: fields(e, ENTITY_FIELDS)
    for t in a['lock_targets']: fields(t, LOCK_FIELDS)
    b = schema.decode(a['owner_binding'])['binding']
    targets = a['lock_targets']
    if (a['physical'] != _physical(b) or len(targets) not in (2, 3)
            or targets[0] != _target(b, 'trend.component', b['component_id'])
            or targets[1] != _target(b, 'trend.admission', targets[1]['key'])):
        raise ValueError('trend_p1_lock_shape')
    uuid.UUID(targets[1]['key'])
    reference = schema.decode(schema.decode(a['owner_binding'])['proof']['inputs_typed'])['reference_binding']
    if targets[2:] != ([_target(b, 'trend.reference', reference['sha256'])] if reference else []):
        raise ValueError('trend_p1_reference_lock_shape')


def _anchor(runtime, a):
    with pg_api._pg(runtime) as pg, pg.cursor() as cur:
        cur.execute(f'SELECT state,admission,audit FROM {REGISTRY} WHERE record_key=%s', (a['lock_targets'][1]['key'],))
        row = cur.fetchone()
    if row is None or row[:2] != ('accepted', a) or digest(row[2]) != a['validator']['validation_digest']:
        raise ValueError('trend_p1_actual_admission')
    return row[2]


class _AnchorFact:
    """仅owner活跃锁上下文签发；对象本身不携带可修改证据。"""
    __slots__ = ()


_anchor_facts = {}
_anchor_context = ContextVar('trend_locked_admission', default=None)


def _fact_audit(runtime, admission, fact):
    from data_pipeline.common.admission_locks import validated_connection
    if type(fact) is not _AnchorFact or fact not in _anchor_facts:
        raise ValueError('trend_anchor_fact_inactive')
    rt, expected, target, lease, audit_text, thread = _anchor_facts[fact]
    if (runtime is not rt or admission != expected or threading.get_ident() != thread
            or target != admission['lock_targets'][1]):
        raise ValueError('trend_anchor_fact_binding')
    validated_connection(runtime, admission, target, lease)
    import json
    return json.loads(audit_text)


@contextmanager
def use_locked_admission(runtime, admission, fact):
    """组合全锁及首次原current之后启用；失效不得降级为未持锁检查。"""
    if _anchor_context.get() is not None:
        raise ValueError('trend_anchor_context_reentry')
    _fact_audit(runtime, admission, fact)
    token = _anchor_context.set(fact)
    try:
        yield
    finally:
        _anchor_context.reset(token)


def _current_anchor(runtime, admission):
    fact = _anchor_context.get()
    if fact is None:
        return _anchor(runtime, admission)
    return _fact_audit(runtime, admission, fact)


def _reusable(runtime, owner_binding, rules, guard):
    """只检索同完整绑定/规则登记；实际current通过才原样返回。"""
    with pg_api._pg(runtime) as pg:
        cur = pg.cursor()
        try:
            guard(); cur.execute('SELECT to_regclass(%s)', (REGISTRY,))
            if cur.fetchone()[0] is None: return None
            count = size = 0; after = None
            while True:
                guard()
                cur.execute(f'''SELECT record_key,admission FROM {REGISTRY} WHERE state='accepted'
                    AND admission->>'owner_binding'=%s
                    AND ((admission->'validator') - 'validation_digest')=%s::jsonb
                    AND (%s::uuid IS NULL OR record_key>%s::uuid) ORDER BY record_key LIMIT 1''',
                    (schema.encode(owner_binding), json_text(rules), after, after))
                found = cur.fetchone()
                if found is None: break
                after, candidate = found
                guard(); count += 1; size += len(json_text(candidate).encode())
                if len(json_text(candidate).encode()) > runtime.limits.max_context_bytes:
                    raise ValueError('trend_p1_reuse_context_budget')
                runtime.event('trend_reuse_scan', rows=count, bytes=size)
                try: verify_current(runtime, candidate, guard=guard)
                except ValueError: continue
                runtime.event('trend_admit_reused', admission_id=candidate['admission_id'])
                return candidate
        finally: cleanup((cur.close,), sys.exc_info()[1])
    return None


def verify_current(runtime, admission, *, guard):
    guard = runtime.guarded(guard); guard(); _shape(admission)
    binding = schema.decode(admission['owner_binding']); _parts(runtime, binding)
    audit = _current_anchor(runtime, admission)
    if {k:v for k,v in admission['validator'].items() if k != 'validation_digest'} != _rules(runtime):
        raise ValueError('trend_p1_validator_changed')
    if admission['dependencies'] != sorted(a['admission_id'] for a in runtime.dependency_admissions):
        raise ValueError('trend_p1_dependencies_changed')
    pg_api._entities_current(runtime, admission['entities'], guard)
    parts.current_parts(runtime, binding, guard)
    if digest(schema.encode(catalog(runtime, binding, guard))) != audit['catalog_digest']:
        raise ValueError('trend_p1_catalog_changed')
    runtime.dependencies(guard)
    return admission


@contextmanager
def hold_lock(runtime, admission, lock_target, *, guard, lock_connection=None):
    """仅当前显式一个Trend目标；不调用上游current/lock，不在退出时验业务。"""
    guard = runtime.guarded(guard); guard(); _shape(admission); _anchor(runtime, admission)
    binding = schema.decode(admission['owner_binding']); b = _parts(runtime, binding)
    if lock_target not in admission['lock_targets']: raise ValueError('trend_p1_lock_outside')
    if {k:v for k,v in admission['validator'].items() if k != 'validation_digest'} != _rules(runtime):
        raise ValueError('trend_p1_lock_validator')
    owned = lock_connection is None
    if owned:
        pg = pg_api.psycopg2.connect(runtime.dsn)
    else:
        from data_pipeline.common.admission_locks import validated_connection
        pg = validated_connection(runtime, admission, lock_target, lock_connection)
    primary = None
    fact = None
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT set_config('lock_timeout',%s,true)", (str(runtime.lock_timeout_ms)+'ms',))
            cur.execute('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')
            if cur.fetchone() != (b['system_id'], b['database_oid']): raise ValueError('trend_p1_lock_physical')
            if lock_target['namespace'] == 'trend.component':
                cur.execute('SELECT state,root,schema_name,binding,proof FROM country_trends.components WHERE component_id=%s FOR SHARE', (lock_target['key'],))
                expected = ('complete', b['root'], b['schema_name'], json_text(b), json_text(binding['proof']))
            elif lock_target['namespace'] == 'trend.reference':
                cur.execute('SELECT state,binding FROM country_trends.references WHERE reference_id=%s FOR SHARE', (lock_target['key'],))
                expected = ('complete', json_text(runtime.reference_binding))
            else:
                cur.execute(f'SELECT state,admission,audit FROM {REGISTRY} WHERE record_key=%s FOR SHARE', (lock_target['key'],))
                row = cur.fetchone()
                if row is None or row[:2] != ('accepted', admission) or digest(row[2]) != admission['validator']['validation_digest']:
                    raise ValueError('trend_p1_lock_anchor_audit')
                if not owned:
                    fact = _AnchorFact()
                    _anchor_facts[fact] = (runtime, deepcopy(admission), deepcopy(lock_target),
                        lock_connection, json_text(row[2]), threading.get_ident())
                expected = None
            if expected is not None and cur.fetchone() != expected: raise ValueError('trend_p1_lock_target_changed')
        yield fact
    except BaseException as error: primary = error; raise
    finally:
        if fact is not None:
            _anchor_facts.pop(fact, None)
        if owned: pg_api._release(pg, primary)


def admit(runtime, owner_binding, *, guard):
    from data_pipeline.analysis.country_trends.stream_audit import full_audit
    resources.sqlite_temp()
    guard = runtime.guarded(guard); b = _parts(runtime, owner_binding)
    rules = _rules(runtime)
    revision = subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip()
    for relative in (*code_binding(), 'backend/data_pipeline/common/admission_locks.py'):
        try:
            committed = subprocess.check_output(['git','show',revision+':'+relative], cwd=ROOT, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as exc:
            raise ValueError('trend_p1_requires_fixed_commit') from exc
        if committed != (ROOT/relative).read_bytes():
            raise ValueError('trend_p1_requires_fixed_commit')
    reused = _reusable(runtime, owner_binding, rules, guard)
    if reused is not None: return reused
    runtime.dependencies(guard)
    validation = full_audit(runtime, owner_binding, guard)
    entities = [pg_api._entity(runtime, str(path), hash_body=True, guard=guard)
                for path in [runtime.output_root/'inputs.sqlite', runtime.output_root/'manifest.json',
                             runtime.output_root/'data'/parts.SUMMARY,
                             *(runtime.output_root/'data'/parts.manifest_name(kind) for kind in VIEWS)]]
    if runtime.reference_binding is not None:
        entities.append(pg_api._entity(runtime, runtime.reference_binding['path'], hash_body=True, guard=guard))
    audit = dict(validation=validation, catalog_digest=digest(schema.encode(catalog(runtime, owner_binding, guard))))
    key = str(uuid.uuid4())
    targets = [_target(b,'trend.component',b['component_id']), _target(b,'trend.admission',key)]
    if runtime.reference_binding is not None: targets.append(_target(b,'trend.reference',runtime.reference_binding['sha256']))
    a = dict(contract='component-publication-admission/v1', owner='trend', owner_revision=revision,
        owner_binding=schema.encode(owner_binding), binding_codec=CODEC, physical=_physical(b),
        validator=dict(rules, validation_digest=digest(audit)), inventory_digest=digest(schema.encode(owner_binding['proof']['body'])),
        entities=sorted(entities,key=lambda e:e['path']), dependencies=sorted(x['admission_id'] for x in runtime.dependency_admissions), lock_targets=targets)
    a['admission_id'] = digest(a); _shape(a)
    runtime.dependencies(guard); pg_api._entities_current(runtime, a['entities'], guard)
    parts.current_parts(runtime, owner_binding, guard)
    if _rules(runtime) != rules: raise ValueError('trend_p1_code_changed')
    with pg_api._pg(runtime, write=True) as pg, pg.cursor() as cur:
        cur.execute(f'''CREATE TABLE IF NOT EXISTS {REGISTRY}(record_key UUID PRIMARY KEY,
            state TEXT NOT NULL CHECK(state IN ('accepted','revoked')),admission JSONB NOT NULL,audit JSONB NOT NULL)''')
        cur.execute(f'INSERT INTO {REGISTRY} VALUES (%s,%s,%s,%s)', (key,'accepted',Json(a),Json(audit)))
    try:
        verify_current(runtime, a, guard=guard)
    except BaseException as error:
        def revoke():
            with pg_api._pg(runtime, write=True) as pg, pg.cursor() as cur:
                cur.execute(f"UPDATE {REGISTRY} SET state='revoked' WHERE record_key=%s", (key,))
        cleanup((revoke,), error)
        raise
    return a


@dataclass
class ReadSession:
    iterator: object = None
    receipt: object = None
    exhausted: bool = False
    def __iter__(self): return self
    def __next__(self): return next(self.iterator)
    def close(self): self.iterator.close()


def request_scope(request, limits):
    fields(request, REQUEST_FIELDS)
    if (request['view'] not in VIEWS or request['codec_version'] != CODEC
            or type(request['batch_rows']) is not int or not 0 < request['batch_rows'] <= limits.max_batch_rows
            or type(request['batch_bytes']) is not int or not 0 < request['batch_bytes'] <= limits.max_batch_bytes):
        raise ValueError('trend_p1_read_request')
    if type(request['scope_typed']) is not str or len(request['scope_typed'].encode()) > limits.max_context_bytes:
        raise ValueError('trend_p1_scope_bytes')
    scope = schema.decode(request['scope_typed'])
    fields(scope, {'event', 'key_typed', 'after_sequence', 'stop_sequence'})
    event = scope['event']
    if event is not None and (type(event) is not tuple or len(event) != 2 or type(event[0]) is not str or not event[0] or type(event[1]) is not int or event[1] < 1):
        raise ValueError('trend_p1_event_scope')
    if (type(scope['after_sequence']) is not int or scope['after_sequence'] < -1
            or scope['stop_sequence'] is not None and (type(scope['stop_sequence']) is not int or scope['stop_sequence'] <= scope['after_sequence'])):
        raise ValueError('trend_p1_sequence_scope')
    if scope['key_typed'] is not None and type(schema.decode(scope['key_typed'])) is not tuple:
        raise ValueError('trend_p1_key_scope')
    return scope


@contextmanager
def open_reader(runtime, admission, request, *, guard):
    request = deepcopy(request)
    scope = request_scope(request, runtime.limits)
    guard = runtime.guarded(guard); verify_current(runtime, admission, guard=guard)
    binding = schema.decode(admission['owner_binding']); b = binding['binding']
    budget = StreamBudget(runtime.limits, runtime.output_root, guard)
    summary = parts.read_summary(runtime.output_root/'data', runtime.limits, budget.check, binding['proof']['body'])
    stream = parts.iter_table(runtime.output_root/'data', request['view'], runtime.limits, budget.check, summary=summary)
    session = ReadSession(); count = 0; h = hashlib.sha256()
    def rows():
        nonlocal count
        pending = []
        for flat in stream:
            budget.charge(schema.encode(flat).encode())
            item = schema.restore(request['view'], flat)
            if item.result_id != b['result_id']: raise ValueError('trend_p1_row_result')
            if flat['sequence'] <= scope['after_sequence']: continue
            if scope['stop_sequence'] is not None and flat['sequence'] >= scope['stop_sequence']: continue
            if scope['event'] is not None and item.event != scope['event']: continue
            if scope['key_typed'] is not None and schema.encode(item.key) != scope['key_typed']: continue
            wrapped = dict(kind=item.kind, row=flat); encoded = schema.encode(wrapped).encode()
            if len(schema.encode((wrapped,)).encode()) > request['batch_bytes']: raise ValueError('trend_p1_row_bytes')
            if pending and (len(pending) >= request['batch_rows'] or len(schema.encode(tuple((*pending,wrapped))).encode()) > request['batch_bytes']):
                text = schema.encode(tuple(pending)); yield dict(rows_typed=text, codec_version=CODEC, rows=len(pending), bytes=len(text.encode())); pending=[]
            pending.append(wrapped); count += 1; h.update(len(encoded).to_bytes(8,'big')); h.update(encoded)
        if pending:
            text = schema.encode(tuple(pending)); yield dict(rows_typed=text, codec_version=CODEC, rows=len(pending), bytes=len(text.encode()))
        session.exhausted = True
    session.iterator = rows()
    try: yield session
    finally: cleanup((session.close, stream.close), sys.exc_info()[1])
    if session.exhausted:
        verify_current(runtime, admission, guard=guard); budget.finish()
        session.receipt = dict(contract='component-publication-read/v1', admission_id=admission['admission_id'],
            request_digest=digest(request), rows=count, typed_digest=h.hexdigest(), execution='complete',
            coverage_ref=schema.encode(dict(result_id=b['result_id'], view=request['view'], scope=scope,
                coverage_view='result_availability', coverage_root='country_coverage', full_science_audit=False)))
