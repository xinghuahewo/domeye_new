"""新版本S3离线候选：公开Country输入→科学编译→DuckLake/PG登记。"""
from dataclasses import dataclass, asdict
import hashlib
import sys
import time
import uuid

from data_pipeline.bgp.archive import admission as pg_api
from data_pipeline.bgp.archive.store import literal
from data_pipeline.analysis.country_events.snapshot_store import cleanup, write_json, fsync_tree
from data_pipeline.results.manifest_io import encode as json_text, file_hash
from data_pipeline.analysis.country_trends.stream_budget import StreamBudget, connect, code_binding
from data_pipeline.analysis.country_trends import stream_schema as schema
from data_pipeline.analysis.country_trends.country_source import initialize, capture_country
from data_pipeline.analysis.country_trends.compile_country_results import compile_country
from data_pipeline.analysis.country_trends.stream_files import write_body
from data_pipeline.analysis.country_trends.part_manifest import iter_parts
from data_pipeline.common import process_resources as resources


@dataclass(frozen=True)
class Binding:
    system_id: str
    database_oid: int
    catalog_id: str
    component_id: str
    result_id: str
    schema_name: str
    snapshot: int
    root: str
    manifest_sha256: str
    schema_version: str = schema.VERSION
    profile: str = schema.PROFILE


def prepare_trend_m3(runtime, *, guard=lambda: None):
    """不接任意fixture行流；公开来源必须有实际当前Admission。

    返回完整owner binding，不代替随后P1独立admit。
    """
    resources.sqlite_temp()
    guard = runtime.guarded(guard); runtime.dependencies(guard)
    component = uuid.uuid4().hex; name = 'trend_m3_' + component
    root = runtime.output_root / component
    root.mkdir(exist_ok=False); runtime.path(root)
    budget = StreamBudget(runtime.limits, root, guard)
    budget.scratch_roots.add(runtime.scratch_root)
    code = code_binding(); started = time.monotonic(); stages = {}
    with pg_api._pg(runtime, write=True) as pg, pg.cursor() as cur:
        cur.execute('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')
        system, oid = cur.fetchone()
        identity = dict(system_id=system, database_oid=oid)
        cur.execute('CREATE SCHEMA IF NOT EXISTS country_trends')
        cur.execute('CREATE TABLE IF NOT EXISTS country_trends.catalog(singleton BOOLEAN PRIMARY KEY CHECK(singleton),catalog_id TEXT NOT NULL)')
        cur.execute('INSERT INTO country_trends.catalog VALUES (true,%s) ON CONFLICT DO NOTHING', (uuid.uuid4().hex,))
        cur.execute('SELECT catalog_id FROM country_trends.catalog WHERE singleton'); identity['catalog_id'] = cur.fetchone()[0]
        cur.execute('''CREATE TABLE IF NOT EXISTS country_trends.components(component_id TEXT PRIMARY KEY,
            state TEXT NOT NULL CHECK(state IN ('candidate','failed','complete','revoked')),
            root TEXT NOT NULL,schema_name TEXT NOT NULL,binding TEXT,proof TEXT,reason TEXT)''')
        cur.execute("INSERT INTO country_trends.components(component_id,state,root,schema_name) VALUES (%s,'candidate',%s,%s)", (component, str(root), name))
    db = lake = None
    try:
        db = connect(root / 'inputs.sqlite', budget)
        initialize(db)
        country = next(a for a in runtime.dependency_admissions if a['owner'] == 'country')
        capture = capture_country(db, country, runtime.dependency_runtimes[country['admission_id']], runtime.country_window_us, budget)
        feature = next((a for a in runtime.dependency_admissions if a['owner'] == 'feature'), None)
        activities = (); references = (); context_sources = ()
        if feature is not None:
            from data_pipeline.analysis.country_trends.feature_source import capture_feature, feature_context
            capture_feature(db, feature, runtime.dependency_runtimes[feature['admission_id']], budget)
            activities = feature_context(db, feature, runtime.feature_selections, budget)
        elif runtime.feature_selections: raise ValueError('trend_feature_not_provided')
        if runtime.reference_binding is not None:
            from data_pipeline.analysis.country_trends.reference_reader import reference_context
            references, context_sources = reference_context(runtime, guard=budget.check)
        budget.stats['source_charged_rows'] = budget.rows
        budget.stats['source_charged_bytes'] = budget.byte_count
        if len(schema.encode((activities, references, context_sources)).encode()) > runtime.limits.max_context_bytes:
            raise ValueError('trend_s3_context_budget')
        stages['public_input_seconds'] = time.monotonic() - started
        inputs = dict(dependencies=runtime.dependency_admissions, window_us=runtime.country_window_us,
                      reference_binding=runtime.reference_binding, feature_selections=runtime.feature_selections,
                      execution_profile=runtime._profile, root=str(root), identity=identity, code=code, capture=capture)
        result_id = 'trend_m3_' + hashlib.sha256(schema.encode(inputs).encode()).hexdigest()
        before = time.monotonic()
        body = write_body(compile_country(db, result_id, runtime.country_window_us, budget,
            activities=activities, references=references, context_sources=context_sources,
            feature_admission=feature, feature_selections=runtime.feature_selections), root / 'data',
            limits=runtime.limits, budget=budget)
        db.commit(); db.close(); db = None
        stages['science_files_readback_seconds'] = time.monotonic() - before
        before = time.monotonic()
        lake = runtime.lake(root=root)
        lake.execute('CREATE SCHEMA lake.' + name)
        for kind in schema.TABLES:
            budget.check()
            columns = ','.join('"' + n + '" ' + t for n, t in schema.SCHEMAS[kind])
            lake.execute(f'CREATE TABLE lake.{name}.{kind} ({columns})')
            for part in iter_parts(root/'data', kind, runtime.limits, budget.check, summary=body):
                if part['rows']:
                    lake.execute("CALL ducklake_add_data_files('lake'," + literal(kind) + ',' + literal(root/'data'/part['path']) + ',schema=>' + literal(name) + ')')
        snapshot = lake.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        lake.close(); lake = None
        stages['lake_catalog_seconds'] = time.monotonic() - before
        if code_binding() != code: raise ValueError('trend_s3_code_drift')
        runtime.dependencies(budget.check)
        proof = dict(body=body, inputs_typed=schema.encode(inputs), source_sha256=file_hash(root/'inputs.sqlite'),
                     stages=stages, wall_seconds=time.monotonic()-started)
        resources.track(root/'manifest.json')
        write_json(root/'manifest.json', dict(version=schema.VERSION, component_id=component,
                   result_id=result_id, schema_name=name, snapshot=snapshot, proof=proof))
        resources.freeze(root/'manifest.json')
        fsync_tree(root)
        binding = Binding(**identity, component_id=component, result_id=result_id, schema_name=name,
                          snapshot=snapshot, root=str(root), manifest_sha256=file_hash(root/'manifest.json'))
        budget.finish()
        with pg_api._pg(runtime, write=True) as pg, pg.cursor() as cur:
            cur.execute("UPDATE country_trends.components SET state='complete',binding=%s,proof=%s WHERE component_id=%s AND state='candidate'", (json_text(asdict(binding)), json_text(proof), component))
            if cur.rowcount != 1: raise ValueError('trend_s3_control_changed')
        return dict(binding=asdict(binding), proof=proof)
    except BaseException as error:
        def failed():
            with pg_api._pg(runtime, write=True) as pg, pg.cursor() as cur:
                cur.execute("UPDATE country_trends.components SET state='failed',binding=NULL,proof=NULL,reason=%s WHERE component_id=%s", (str(error), component))
        cleanup((failed,), error)
        raise
    finally:
        cleanup((lambda: db.close() if db is not None else None,
                 lambda: lake.close() if lake is not None else None), sys.exc_info()[1])
