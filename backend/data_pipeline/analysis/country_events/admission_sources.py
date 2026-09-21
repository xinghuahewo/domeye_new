"""从C3保留原证据逐行恢复实际M3依赖；完整历史原行仅在SQLite暂存。"""
import json
from copy import deepcopy
from pathlib import Path
import pyarrow.parquet as pq
from data_pipeline.analysis.country_events.snapshot_store import file_hash
from data_pipeline.analysis.country_events.snapshot_schema import decode, encode
from data_pipeline.analysis.country_events.qualified_schema import row_decode
from data_pipeline.analysis.country_events.route_source_reader import M3Source
from data_pipeline.analysis.country_events.input_staging import SourceStore
from data_pipeline.analysis.country_events.event_aggregation import C2Row, InputEvidence
from data_pipeline.analysis.country_events.snapshot_store import cleanup


def same_admission_value(left,right):
    if type(left) is not type(right):return False
    if isinstance(left,dict):
        return (all(type(k) is str for k in (*left,*right)) and set(left)==set(right)
                and all(same_admission_value(left[k],right[k]) for k in left))
    if isinstance(left,(tuple,list)):
        return len(left)==len(right) and all(same_admission_value(a,b) for a,b in zip(left,right))
    return encode(left)==encode(right)


def retained_admissions(inputs, original):
    """JSONB可调换对象字段顺序；逐值严格匹配后保留C3原typed顺序。"""
    current=tuple(inputs.admissions[k] for k in sorted(inputs.admissions))
    if not same_admission_value(current,original):
        raise ValueError('Country完整原依赖Admission不符')
    inputs.admissions={a['admission_id']:deepcopy(a) for a in original}


def load_source(runtime,binding,inputs):
    """首次准入调用；必须已持全部上游和C3锁。调用方负责source.close。"""
    runtime.event('country_source_load_start',operation='首次准入或显式inspect')
    root=runtime.path(Path(binding.root));manifest_path=runtime.path(root/'manifest.json')
    if file_hash(manifest_path,runtime.resource_guard)!=binding.manifest_sha256:
        raise ValueError('Country原C3 manifest摘要不符')
    manifest=json.loads(manifest_path.read_text());original=decode(manifest['upstream'])
    if original['profile']!='country-m3-source/v1':
        raise ValueError('Country完整原依赖Admission不符')
    retained_admissions(inputs,original['admissions'])
    inputs.verify()
    receipts=original['reads'];keys={(s['receipt']['admission_id'],s['view']) for s in receipts}
    path=runtime.path(root/'data/input_evidence.parquet')
    files=[f for f in manifest['files'] if root/f['path']==path]
    if len(files)!=1 or file_hash(path,runtime.resource_guard)!=files[0]['sha256']:
        raise ValueError('Country保留原证据实体不符')
    store=SourceStore(runtime.scratch_root,max_rows=runtime.max_source_rows,max_bytes=runtime.max_source_bytes,
        max_row_bytes=runtime.limits.max_row_bytes,batch_rows=1,guard=runtime.resource_guard,max_rss_bytes=runtime.limits.max_rss_bytes)
    count=size=0
    try:
        for key in keys:store.add_view(key,())
        with path.open('rb') as handle:
            parquet=pq.ParquetFile(handle)
            for group in range(parquet.num_row_groups):
                runtime.resource_guard()
                if parquet.metadata.row_group(group).total_byte_size>runtime.limits.max_read_bytes:
                    raise ValueError('resource_limit:Country_source_row_group')
                for batch in parquet.iter_batches(batch_size=1,row_groups=[group]):
                    for physical in batch.to_pylist():
                        runtime.resource_guard();count+=1;size+=len(encode(physical).encode())
                        if count>runtime.max_source_rows or size>runtime.max_source_bytes:
                            raise ValueError('resource_limit:Country_retained_source')
                        item=row_decode('input_evidence',physical)
                        if not isinstance(item,C2Row) or not isinstance(item.value,InputEvidence):raise ValueError('Country保留证据类型不符')
                        value=item.value
                        if value.kind!='m3_original':continue
                        owner,aid,view,ordinal=value.reference.split('/');key=aid,view
                        if key not in keys or inputs.admissions[aid]['owner']!=owner or not ordinal.isdecimal() or str(int(ordinal))!=ordinal:
                            raise ValueError('Country原证据序号或来源不符')
                        store.add_record(key,int(ordinal),value.original)
        store.check_ordinals();store.flush()
        source=M3Source(inputs,store,receipts,max_rows=runtime.max_source_rows,max_bytes=runtime.max_source_bytes,
                        max_gap_candidates=runtime.max_source_rows,legacy_timezone=original['legacy_timezone'])
        runtime.event('country_source_load_complete',physical_rows=count,bytes=size,complete_views=len(receipts),
                      storage='sqlite',sqlite_cache_bytes=8*1024**2,peak_encoded_row_bytes=store.peak_row_bytes)
        return source
    except BaseException as error:
        cleanup((store.close,),error);raise
