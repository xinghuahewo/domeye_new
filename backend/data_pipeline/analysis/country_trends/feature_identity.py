"""Feature同版名称/代码关联：只读其既有Reference依赖，不另设直接依赖。"""
import hashlib
from pathlib import Path

import pyarrow as pa

from data_pipeline.analysis.features import publication as feature
from data_pipeline.analysis.features.reference import load_reference, USECOLS, REFERENCE_RULE
from data_pipeline.bgp.archive import admission as reference_owner
from data_pipeline.bgp.archive.value_codec import typed, untyped, digest, fields, CODEC
from data_pipeline.bgp.archive.checkpoint import sha
from data_pipeline.analysis.country_trends.compute import rss
from data_pipeline.analysis.country_trends import identity_index as index

TABLES = (('feature_reference_input', 'ordinal'), ('feature_reference_meta', 'id'),
          *index.TABLES)


def initialize(db):
    db.executescript('''CREATE TABLE feature_reference_input(ordinal INTEGER PRIMARY KEY,payload TEXT);
        CREATE TABLE feature_reference_meta(id INTEGER PRIMARY KEY,payload TEXT);''')
    index.initialize(db)


def _save(db, table, ordinal, value, budget):
    payload = typed(value); budget.charge(payload.encode()); budget.refs(1)
    db.execute('INSERT INTO '+table+' VALUES (?,?)', (ordinal, payload))


def reference_dependency(admission, runtime, guard):
    # 原Feature核验涵盖唯一Reference、M2、原件路径与传递current；不自行放宽。
    feature.verify_current(runtime, admission, guard=guard)
    refs = [a for a in runtime.dependency_admissions if a['owner']=='reference']
    if len(refs)!=1 or refs[0]['admission_id'] not in admission['dependencies']:
        raise ValueError('trend_feature_identity_dependency')
    ref = refs[0]
    fb, rb, cp = reference_binding(admission, ref)
    rr = runtime.dependency_runtimes[ref['admission_id']]
    path = rr.path(fb['specification']['reference_view']['raw_path'])
    return ref, rr, fb, rb, cp, path


def reference_binding(admission, ref):
    """原current之后的绑定一致性检查；独立调用不代表AD资格。"""
    rb = untyped(ref['owner_binding']); fb = untyped(admission['owner_binding'])
    spec = fb['specification']; view = spec['reference_view']; expected = spec['reference_binding']
    raw = rb['m2_binding']
    if (rb['source_id'] != view['source_sha256'] or rb['source_id'] != expected['source_sha256']
            or (raw['run_id'],raw['snapshot']) != (view['run_id'],view['snapshot'])
            or (raw['run_id'],raw['snapshot']) != (expected['run_id'],expected['snapshot'])
            or rb['checkpoint_ordinal'] != expected['checkpoint']['ordinal'] or rb['selected_sources'] != []):
        raise ValueError('trend_feature_identity_binding')
    cp = raw['seal']['checkpoints'][rb['checkpoint_ordinal']]
    if cp != expected['checkpoint'] or cp['source_id'] != rb['source_id'] or cp['counts']['references'] != view['expected_rows']:
        raise ValueError('trend_feature_identity_checkpoint')
    return fb, rb, cp


def preflight_cost(path, expected_rows, budget):
    """整列pandas不是流式：先限制原件/行数并预留保守内存，原guard再核实际RSS。"""
    size = Path(path).stat().st_size
    if type(expected_rows) is not int or expected_rows<1 or expected_rows>budget.limits.max_references:
        raise ValueError('trend_feature_identity_rows_budget')
    if size>budget.limits.max_total_bytes:
        raise ValueError('trend_feature_identity_raw_budget')
    # 这是启动前保守估计而非pandas分配硬上限；实际高水位由原guard持续核验。
    estimate = size*8 + expected_rows*len(USECOLS)*128
    if rss()+estimate>budget.limits.max_rss_bytes:
        raise ValueError('trend_feature_identity_frame_budget')
    budget.byte_count += size
    budget.stats['feature_reference_raw_bytes'] += size
    budget.stats['feature_reference_frame_estimate_bytes'] = estimate
    budget.check()


def retain_batch(db, batch, request, source_id, count, hasher, budget):
    fields(batch, {'rows_typed','codec_version','rows','bytes'})
    if (batch['codec_version']!=CODEC or type(batch['rows']) is not int
            or not 0<batch['rows']<=request['batch_rows'] or type(batch['bytes']) is not int
            or batch['bytes']!=len(batch['rows_typed'].encode()) or batch['bytes']>request['batch_bytes']):
        raise ValueError('trend_feature_reference_batch')
    rows=untyped(batch['rows_typed'])
    if type(rows) is not list or len(rows)!=batch['rows']:raise ValueError('trend_feature_reference_rows')
    for row in rows:
        if row['source_id']!=source_id or row['row']!=count:
            raise ValueError('trend_feature_reference_source_order')
        hasher.update(bytes.fromhex(sha(row)))  # 原Reference owner公式，不用Feature读流公式。
        _save(db,'feature_reference_input',count,row,budget);count+=1
    return count


def check_receipt(receipt, request, admission_id, checkpoint, count, hasher):
    fields(receipt, {'contract','admission_id','request_digest','rows','typed_digest','execution','coverage_ref'})
    if (receipt['contract'],receipt['admission_id'],receipt['request_digest'],receipt['rows'],receipt['typed_digest'],receipt['execution']) != (
            'component-publication-read/v1',admission_id,digest(request),count,hasher.hexdigest(),'complete'):
        raise ValueError('trend_feature_reference_receipt')
    expected=dict(admission_id=admission_id,source_ids=[checkpoint['source_id']],view='references',
        source_checkpoints=[{k:checkpoint[k] for k in ('source_id','ordinal','counts','raw','parse','ingest')}],
        observation_qualification='observation_sealed',business='not_run',business_absence='Unknown')
    if untyped(receipt['coverage_ref'])!=expected or count!=checkpoint['counts']['references']:
        raise ValueError('trend_feature_reference_coverage')


def interpret(db, path, source_id, expected_version, budget):
    class SavedReader:
        def reference_batches(self, requested):
            if requested!=source_id:raise ValueError('trend_feature_reference_interpretation_source')
            cursor=db.execute('SELECT payload FROM feature_reference_input ORDER BY ordinal')
            while True:
                batch=cursor.fetchmany(min(256,budget.limits.max_batch_rows))
                if not batch:break
                budget.check()
                yield pa.RecordBatch.from_pylist([untyped(x[0]) for x in batch])
    ordinal=0
    def retain(row):
        nonlocal ordinal
        index.save_interpretation(db,ordinal,row,budget);ordinal+=1
    ref=load_reference(SavedReader(),source_id,path,sink=retain,guard=budget.check)
    if ref.version!=expected_version:raise ValueError('trend_feature_identity_version')
    del ref
    index.build_index(db,budget)
    return expected_version


def capture_identity(db, admission, runtime, budget):
    index.install_progress(db,budget)
    initialize(db)
    ref,rr,fb,rb,cp,path=reference_dependency(admission,runtime,budget.check)
    preflight_cost(path,cp['counts']['references'],budget)
    source=rb['source_id']
    request=dict(view='references',scope_typed=typed(dict(source_ids=[source])),codec_version=CODEC,
                 batch_rows=min(256,budget.limits.max_batch_rows),batch_bytes=budget.limits.max_batch_bytes)
    h=hashlib.sha256();count=0
    with reference_owner.open_reader(rr,ref,request,guard=budget.check) as session:
        for batch in session:
            count=retain_batch(db,batch,request,source,count,h,budget)
    check_receipt(session.receipt,request,ref['admission_id'],cp,count,h)
    # 只有原公开流完整尾receipt通过后才进行整列解释。
    version=interpret(db,path,source,fb['reference_version'],budget)
    feature.verify_current(runtime,admission,guard=budget.check)
    _save(db,'feature_reference_meta',0,dict(feature_admission=admission,reference_admission=ref,
        request=request,receipt=session.receipt,source_id=source,checkpoint_ordinal=cp['ordinal'],
        reference_version=version,interpretation_rule=REFERENCE_RULE),budget)
    db.commit()


def window_identity(db, admission, ordinal, public, budget):
    raw=public['raw']
    if raw['scope']!='country':return None
    code=index.lookup(db,raw['country'],budget)
    if code is None:return None
    prefix='feature:'+admission['admission_id']
    body=dict(window_source_ref=prefix+':windows:'+str(ordinal),name=raw['country'],code=code,
              identity_group_ref=prefix+index.group_suffix('name',typed(raw['country'])),
              binding_source_ref=prefix+':identity-binding')
    return prefix+':windows:'+str(ordinal)+':identity',body


def _sources(db,admission,budget):
    """加入现有context_source，不覆盖独立比较参考的context_sources。"""
    prefix='feature:'+admission['admission_id']
    for table,kind in [('feature_reference_meta','identity-binding'),('feature_reference_input','reference-row')]:
        for ordinal,payload in db.execute('SELECT * FROM '+table+' ORDER BY 1'):
            budget.check()
            source=prefix+':'+kind+('' if kind=='identity-binding' else ':'+str(ordinal))
            value=untyped(payload)
            yield source,value
    for ordinal,value in index.interpretations(db,budget):
        yield prefix+':identity:'+str(ordinal),dict(value,
            reference_source_ref=prefix+':reference-row:'+str(ordinal),binding_source_ref=prefix+':identity-binding')
    yield from index.evidence(db,prefix,budget)
    for ordinal,payload in db.execute("SELECT ordinal,payload FROM feature_input WHERE view='windows' ORDER BY ordinal"):
        budget.check();identity=window_identity(db,admission,ordinal,untyped(payload),budget)
        if identity:yield identity


def sources(db,admission,budget):
    """来源字段补齐后的单项也受限制；编译器再核实际公开行外壳。"""
    for source,value in _sources(db,admission,budget):
        if len(typed((source,value)).encode())>min(budget.limits.max_context_bytes,budget.limits.max_row_bytes):
            raise ValueError('trend_feature_identity_source_budget')
        yield source,value
