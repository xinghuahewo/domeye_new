"""S3完整准入审计：实际公开源重读复算、原序正文及实际湖逐表核对。"""
from pathlib import Path
import hashlib
import sys
import tempfile

from data_pipeline.analysis.country_events.snapshot_store import cleanup
from data_pipeline.results.manifest_io import file_hash
from data_pipeline.analysis.country_trends.stream_budget import StreamBudget, connect, code_binding
from data_pipeline.analysis.country_trends import stream_schema as schema
from data_pipeline.analysis.country_trends.country_source import initialize, capture_country
from data_pipeline.analysis.country_trends.compile_country_results import compile_country
from data_pipeline.analysis.country_trends.stream_files import read_body
from data_pipeline.analysis.country_trends.part_manifest import read_summary, iter_parts
from data_pipeline.common import process_resources as resources


def compare_sources(saved, fresh, budget, *, with_feature):
    """仅比较两份实际捕获；调用者负责公开源重读及current资格。"""
    comparisons = [('country_input','sequence'), ('country_events','sequence'),
                   ('country_reads','view,table_name')]
    if with_feature:
        from data_pipeline.analysis.country_trends.feature_identity import TABLES
        comparisons += [('feature_input','view,ordinal'), ('feature_reads','view'), *TABLES]
    for table, order in comparisons:
        left = saved.execute('SELECT * FROM ' + table + ' ORDER BY ' + order)
        right = fresh.execute('SELECT * FROM ' + table + ' ORDER BY ' + order)
        while True:
            budget.check(); a = left.fetchone(); z = right.fetchone()
            if a != z: raise ValueError('trend_audit_original_source_difference')
            if a is None: break


def full_audit(runtime, owner_binding, guard):
    resources.sqlite_temp()
    b, proof = owner_binding['binding'], owner_binding['proof']
    inputs = schema.decode(proof['inputs_typed'])
    if inputs['code'] != code_binding(): raise ValueError('trend_audit_science_code_changed')
    if inputs['execution_profile'] != runtime._profile: raise ValueError('trend_audit_mode')
    if (schema.encode(inputs['dependencies']) != schema.encode(runtime.dependency_admissions)
            or inputs['window_us'] != runtime.country_window_us
            or schema.encode(inputs['reference_binding']) != schema.encode(runtime.reference_binding)
            or inputs['feature_selections'] != runtime.feature_selections):
        raise ValueError('trend_audit_actual_inputs')
    root = runtime.path(b['root']); budget = StreamBudget(runtime.limits, root, runtime.guarded(guard))
    budget.scratch_roots.add(runtime.scratch_root)
    if file_hash(root/'manifest.json', budget.check) != b['manifest_sha256'] or file_hash(root/'inputs.sqlite', budget.check) != proof['source_sha256']:
        raise ValueError('trend_audit_retained_files')
    temporary = tempfile.TemporaryDirectory(prefix='trend-admit-', dir=runtime.scratch_root)
    resources.add_root(temporary.name)
    budget.monitor.add_root(temporary.name)
    db = lake = None
    try:
        db = connect(Path(temporary.name)/'fresh.sqlite', budget); initialize(db)
        country = next(a for a in runtime.dependency_admissions if a['owner'] == 'country')
        capture_country(db, country, runtime.dependency_runtimes[country['admission_id']], runtime.country_window_us, budget)
        feature = next((a for a in runtime.dependency_admissions if a['owner'] == 'feature'), None)
        activities = (); references = (); context_sources = ()
        if feature is not None:
            from data_pipeline.analysis.country_trends.feature_source import capture_feature, feature_context
            capture_feature(db, feature, runtime.dependency_runtimes[feature['admission_id']], budget)
            activities = feature_context(db, feature, runtime.feature_selections, budget)
        if runtime.reference_binding is not None:
            from data_pipeline.analysis.country_trends.reference_reader import reference_context
            references, context_sources = reference_context(runtime, guard=budget.check)
        budget.stats['source_charged_rows'] = budget.rows
        budget.stats['source_charged_bytes'] = budget.byte_count
        if len(schema.encode((activities, references, context_sources)).encode()) > runtime.limits.max_context_bytes:
            raise ValueError('trend_s3_context_budget')
        # 留存源与新实际公开源按原序typed逐项相等，不用留存回执自行认证来源。
        saved = connect((root/'inputs.sqlite').as_uri() + '?mode=ro', budget, uri=True)
        try:
            compare_sources(saved, db, budget, with_feature=feature is not None)
        finally: cleanup((saved.close,), sys.exc_info()[1])
        expected = compile_country(db, b['result_id'], runtime.country_window_us, budget,
            activities=activities, references=references, context_sources=context_sources,
            feature_admission=feature, feature_selections=runtime.feature_selections)
        read_summary(root/'data', runtime.limits, budget.check, proof['body'])
        actual = read_body(root/'data', limits=runtime.limits, budget=budget, temporary_parent=runtime.scratch_root)
        count = 0; combined = hashlib.sha256()
        try:
            while True:
                budget.check(); a = next(expected, None); z = next(actual, None)
                if a != z: raise ValueError('trend_audit_scientific_difference')
                if a is None: break
                combined.update(bytes.fromhex(schema.row_hash(a))); count += 1
        finally: cleanup((expected.close, actual.close), sys.exc_info()[1])
        if (count, combined.hexdigest()) != (proof['body']['rows'], proof['body']['sha256']):
            raise ValueError('trend_audit_body_digest')
        lake = runtime.lake()
        for kind in schema.TABLES:
            budget.check()
            cols = lake.execute(f'DESCRIBE SELECT * FROM lake.{b["schema_name"]}.{kind} AT (VERSION => {b["snapshot"]})').fetchall()
            if [(r[0], r[1]) for r in cols] != list(schema.SCHEMAS[kind]): raise ValueError('trend_audit_lake_columns')
            compare_lake_files(lake, b, proof['body'], kind, runtime.limits, budget.check)
            result = lake.execute(f'SELECT * FROM lake.{b["schema_name"]}.{kind} AT (VERSION => {b["snapshot"]}) ORDER BY sequence')
            h = hashlib.sha256(); n = 0
            for batch in result.fetch_record_batch(runtime.limits.max_batch_rows):
                for flat in batch.to_pylist():
                    budget.charge(schema.encode(flat).encode()); item = schema.restore(kind, flat)
                    h.update(bytes.fromhex(schema.row_hash(item))); n += 1
            if (n, h.hexdigest()) != (proof['body']['tables'][kind]['rows'], proof['body']['tables'][kind]['sha256']):
                raise ValueError('trend_audit_actual_lake_body')
        runtime.dependencies(budget.check)
        budget.finish()
        return dict(rows=count, typed_digest=combined.hexdigest(), actual_country_reads=25,
                    lake_tables=len(schema.TABLES), source_recomputed=True,
                    charged_rows=budget.rows, charged_bytes=budget.byte_count)
    finally:
        cleanup((lambda: lake.close() if lake is not None else None,
                 lambda: db.close() if db is not None else None, temporary.cleanup, lambda: resources.remove_root(temporary.name)), sys.exc_info()[1])


def compare_lake_files(lake, binding, body, kind, limits, guard):
    """实际湖文件目录与清单逐项比较，不能fetchall或忽略多余片。"""
    root = Path(binding['root'])/'data'
    files = lake.execute("SELECT data_file,data_file_size_bytes,delete_file FROM ducklake_list_files('lake',?,schema=>?,snapshot_version=>?) ORDER BY data_file", [kind,binding['schema_name'],binding['snapshot']])
    stream = iter_parts(root, kind, limits, guard, summary=body, hash_body=True)
    try:
        for part in stream:
            if part['rows'] and files.fetchone() != (str(root/part['path']), part['entity']['size'], None):
                raise ValueError('trend_audit_lake_file_binding')
        if files.fetchone() is not None: raise ValueError('trend_audit_lake_file_binding')
    finally: cleanup((stream.close,), sys.exc_info()[1])
