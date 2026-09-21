"""原请求封装与消费聚批目标分离；不改变 owner、资格或累计限制。"""
import hashlib
from data_pipeline.results.manifest_io import require
from data_pipeline.results.component_roles import codec


def envelope(owner, runtime, *, batch_rows, max_row_bytes):
    """按现有 owner 请求校验能力取交集，不提升 owner 上限。"""
    require(type(batch_rows) is int and batch_rows>0 and
            type(max_row_bytes) is int and max_row_bytes>0, '请求封装预算无效')
    if owner=='trend':
        rows, size=runtime.limits.max_batch_rows,runtime.limits.max_batch_bytes
    elif owner=='country':
        rows, size=runtime.limits.batch_rows,runtime.limits.batch_bytes
    elif owner=='resource':
        rows, size=runtime.max_rows,runtime.max_bytes
    elif owner in ('feature','detection'):
        rows, size=10000,max_row_bytes
    else:
        raise ValueError('无已接合 owner 请求封装能力：'+str(owner))
    return min(batch_rows,rows),min(max_row_bytes,size)


def consume(owner, source_batch, *, batch_rows, batch_bytes, envelope_bytes):
    """消费批明确标记来源批；绝不产生 owner receipt 或新的 request_digest。"""
    require(all(type(n) is int and n>0 for n in (batch_rows,batch_bytes,envelope_bytes)), '消费目标无效')
    c=codec(owner); text=source_batch['rows_typed']
    require(len(text.encode())==source_batch['bytes']<=envelope_bytes, '原批超过声明封装')
    values=c.untyped(text)
    require(type(values) in (list,tuple) and len(values)==source_batch['rows'], '原批行计量不符')
    require(c.typed(values)==text, '原批编码不能精确保真重组')
    sequence=type(values); pending=[]; start=0
    source_sha=hashlib.sha256(text.encode()).hexdigest()
    def output(items, offset):
        encoded=c.typed(sequence(items))
        return dict(kind='consumption_batch',source_batch_sha256=source_sha,
            source_row_start=offset,source_row_stop=offset+len(items),
            batch=dict(rows_typed=encoded,codec_version=source_batch['codec_version'],
                       rows=len(items),bytes=len(encoded.encode())))
    for row in values:
        require(len(c.typed(sequence([row])).encode())<=envelope_bytes, '单行超出现有表示合同')
        if pending and (len(pending)>=batch_rows or
                len(c.typed(sequence([*pending,row])).encode())>batch_bytes):
            yield output(pending,start);start+=len(pending);pending=[]
        pending.append(row)
        if len(c.typed(sequence(pending)).encode())>batch_bytes:
            yield output(pending,start);start+=len(pending);pending=[]
    if pending: yield output(pending,start)
