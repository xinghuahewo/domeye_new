"""Trend自有比较参考登记读取；复用原合同，显式关闭每个PG连接。"""
from dataclasses import replace
import json

from data_pipeline.bgp.archive import admission as pg_api
from data_pipeline.results.manifest_io import encode as json_text, file_hash, stamp
from data_pipeline.analysis.country_trends.snapshot_inputs import REFERENCE_PROFILE
from data_pipeline.analysis.country_trends.stream_schema import encode, decode


def verify_reference(runtime, *, guard, full=False):
    binding = runtime.reference_binding
    path = runtime.path(binding['path']); guard()
    if binding['profile'] != REFERENCE_PROFILE or binding['historical_applicability'] != 'unknown':
        raise ValueError('trend_s3_reference_profile')
    with pg_api._pg(runtime) as pg, pg.cursor() as cur:
        cur.execute('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')
        if cur.fetchone() != (binding['system_id'], binding['database_oid']):
            raise ValueError('trend_s3_reference_physical')
        cur.execute('SELECT state,binding FROM country_trends.references WHERE reference_id=%s', (binding['sha256'],))
        if cur.fetchone() != ('complete', json_text(binding)):
            raise ValueError('trend_s3_reference_registration')
    if stamp(path) != binding['stamp'] or full and file_hash(path, guard) != binding['sha256']:
        raise ValueError('trend_s3_reference_entity')
    guard()


def reference_context(runtime, *, guard):
    binding = runtime.reference_binding
    path = runtime.path(binding['path'])
    if path.stat().st_size > min(8*1024**2, runtime.limits.max_context_bytes):
        raise ValueError('trend_s3_reference_bytes')
    verify_reference(runtime, guard=guard, full=True)
    with path.open() as stream: document = json.load(stream)
    refs = decode(document['references'])
    result, sources = [], []
    for i, reference in enumerate(refs):
        guard(); projections = []
        for j, projection in enumerate(reference.projections):
            source = f"reference:{binding['sha256']}:{i}:{j}"
            projections.append(replace(projection, source_ref=source))
            sources.append((source, 'reference', encode(projection)))
        result.append(replace(reference, binding=(REFERENCE_PROFILE, json_text(binding)), projections=tuple(projections)))
    result, sources = tuple(result), tuple(sources)
    if len(encode((result, sources)).encode()) > runtime.limits.max_context_bytes:
        raise ValueError('trend_s3_reference_context_bytes')
    verify_reference(runtime, guard=guard)
    return result, sources
