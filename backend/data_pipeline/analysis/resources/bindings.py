"""Resource有限多完成run绑定；同catalog，源顺序与结果窗口必须显式。"""
from dataclasses import asdict, dataclass
from datetime import datetime

from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.bgp.input.path_decoding import DECODER_VERSION
from data_pipeline.analysis.resources.compute import RibContext


@dataclass(frozen=True)
class SourceBinding:
    run_id: str
    snapshot: int
    purpose: str
    context: RibContext


def time_value(value):
    value=datetime.fromisoformat(value.replace('Z','+00:00')) if isinstance(value,str) else value
    if value.tzinfo is None:raise ValueError('窗口必须含时区')
    return value


def validate_sources(dsn, sources, result_window, *, guard=lambda:None, profile='complete'):
    start,end=(time_value(t) for t in result_window)
    if start>=end or not sources:raise ValueError('结果窗口或来源清单为空')
    seen=set();previous=None;collector=sources[0].context.collector;readers=[]
    for binding in sources:
        context=binding.context
        if context.source_id in seen:raise ValueError('重复来源或缓存别名')
        seen.add(context.source_id)
        if context.collector!=collector:raise ValueError('混合collector')
        if context.snapshot_time.tzinfo is None or (previous is not None and context.snapshot_time<=previous):
            raise ValueError('来源时点非严格递增')
        previous=context.snapshot_time
        if binding.purpose not in ('warmup','result'):raise ValueError('来源用途未知')
        if (binding.purpose=='warmup' and context.snapshot_time>=start) or (binding.purpose=='result' and not start<=context.snapshot_time<end):
            raise ValueError('来源用途与结果窗口不一致')
        if context.source_id!=source_identity(collector,context.origin_uri,context.content_sha256):
            raise ValueError('来源URI/SHA身份不符')
        reader=ObservationReader(dsn,binding.run_id,binding.snapshot,[context.source_id],guard=guard,profile=profile)
        entry=next(e for e in reader.manifest['inputs'] if e['source_id']==context.source_id)
        if (reader.manifest['collector']!=collector or entry['role'] not in ('baseline','snapshot')
                or entry['origin_uri']!=context.origin_uri or entry['sha256']!=context.content_sha256):
            raise ValueError('上游来源绑定不符')
        readers.append(reader)
    if not any(s.purpose=='result' for s in sources):raise ValueError('没有结果窗口来源')
    return readers


def manifest(sources,result_window,csv_binding,country_binding):
    return {'schema_version':'resource-bindings/v1','sources':[
        {**asdict(s),'context':{**asdict(s.context),'snapshot_time':s.context.snapshot_time.isoformat()}}
        for s in sources], 'result_window':[time_value(t).isoformat() for t in result_window],
        'csv_reference':csv_binding,'country_reference':country_binding,'decoder_version':DECODER_VERSION,
        'initial_state_basis':'cold_start_at_first_declared_source','old_continuous_state_equivalence':'Unknown'}
