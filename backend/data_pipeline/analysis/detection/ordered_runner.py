"""显式M3消费：唯一共用有序流，独立资格，不改变旧算法。"""
from dataclasses import asdict
from pathlib import Path
import resource
import shutil
import sys
import time

from data_pipeline.bgp.ordered_reader import ordered, binding_from_reader
from data_pipeline.bgp.record_types import SourceStart, SourceEnd, MessageBoundary, Element, SourceQuality, GAP_RULE_VERSION, ORDERED_VERSION
from data_pipeline.common.frozen_execution import execution_identity
from data_pipeline.analysis.detection.adapter import adapt_element
from data_pipeline.analysis.detection.streaming import StreamingDetectionEngine, StreamingSeed
from data_pipeline.analysis.detection.qualification_store import M3Store
from data_pipeline.analysis.detection.qualification import Qualification, PROFILE, QUALIFICATION_RULE
from data_pipeline.analysis.detection.qualification_contract import SCHEMA, LAKE_PROFILE, LAKE_SCHEMA
from data_pipeline.analysis.detection.roles import ROLE_RULE
from data_pipeline.analysis.detection.classification import CLASSIFICATION_RULE
from data_pipeline.analysis.detection.projection import DetectionProjection
from data_pipeline.bgp.input.path_decoding import DECODER_VERSION, DIRECTION_RULE


def run(reader, references, scope, boundaries, dsn, output, *, root, identity_builder,
        fixture_only=False, max_rss_bytes=1024**3, min_free_bytes=128*1024**2, reference_stage=None, output_profile=PROFILE, result_window=None):
    if output_profile not in (PROFILE,LAKE_PROFILE):raise ValueError("不支持的Detection输出profile")
    started = time.monotonic()
    binding = binding_from_reader(reader)
    from data_pipeline.analysis.detection.result_window import windows, time_coverage as new_time_coverage, observe_message, RULE
    window_binding = windows(asdict(scope), result_window, reader.selection.plan['manifest'])
    if set(boundaries) != {s for s in reader.sources if next(x for x in binding.sources if x.source_id==s).role=='update'}:
        raise ValueError('文件边界必须恰好覆盖所选UPDATE，不得混入其他源')
    chosen=[source for source in binding.sources if source.source_id in reader.sources]
    if [source.source_id for source in chosen]!=list(reader.sources) or not chosen or chosen[0].role!='baseline' or any(source.role!='update' for source in chosen[1:]):
        raise ValueError('必须按原source rank选择一个baseline及UPDATE，不读额外RIB或重排')
    time_coverage = new_time_coverage(list(reader.sources),asdict(binding)['sources'])
    if scope.input_version != f'{reader.run_id}:{reader.snapshot}' or scope.collector_id != binding.collector:
        raise ValueError('M3 scope与观察封存不符')
    if references.snapshot_ref != scope.input_version:
        raise ValueError('参考与观察封存不符')
    metadata = references.metadata()
    checkpoints = {cp['source_id']: cp for cp in reader.selection.checkpoints}
    reference_checkpoints = {}
    for role, source in metadata.raw_rows.items():
        cp = checkpoints[source['source_id']]
        if cp['counts']['references'] != source['rows']:
            raise ValueError('参考行数与checkpoint不符')
        reference_checkpoints[role] = cp
    def verify():
        return execution_identity(root, identity_builder, fixture_only=fixture_only)
    identity = dict(verify(), output_profile=output_profile, input_binding=asdict(binding),
                    result_window=window_binding["result_window"], result_window_rule=RULE, window_coverage=time_coverage,
                    input_binding_id=binding.binding_id, selected_sources=list(reader.sources),
                    input_run=reader.run_id, input_snapshot=reader.snapshot,
                    reference_version=references.version, reference_sources=metadata.raw_rows,
                    reference_checkpoints=reference_checkpoints,
                    reference_interpretation=metadata.row_refs, reference_stage=reference_stage,
                    ordered_version=ORDERED_VERSION, gap_rule=GAP_RULE_VERSION,
                    qualification_rule=QUALIFICATION_RULE,
                    algorithm_version=scope.computation_version,
                    projection_version=DetectionProjection.version, decoder_version=DECODER_VERSION,
                    direction_rule=DIRECTION_RULE, role_rule_version=ROLE_RULE,
                    classification_rule_version=CLASSIFICATION_RULE, store_schema_version=LAKE_SCHEMA if output_profile==LAKE_PROFILE else SCHEMA,
                    resource_limits=dict(max_rss_bytes=max_rss_bytes,min_free_bytes=min_free_bytes))
    store = M3Store(dsn, output, scope, identity)
    stream = iter(ordered(reader))
    qualification = Qualification(binding, scope, store.run_id, store.emit_qualification)
    ends = []
    engine = None
    def guard():
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform=='darwin' else 1024)
        if rss > max_rss_bytes or shutil.disk_usage(output).free < min_free_bytes:
            raise ValueError('M3计算资源保护触发')
    store.guard = guard
    def audit(kind, payload):
        store.emit(dict(kind=kind, scope=asdict(scope), reference_version=references.version,
                        reference_historical_applicability='Unknown', evidence={}, legacy=payload))
    def sink(row):
        store.emit(row)
        qualification.observe_output(row)
    def source_elements(start):
        current = None
        for item in stream:
            guard()
            rank = item.position.message.source_rank if isinstance(item, Element) else item.position.source_rank if isinstance(item, MessageBoundary) else item.source_rank
            if rank != start.source_rank:
                raise ValueError('源结束前rank变化')
            if isinstance(item, SourceEnd):
                ends.append(asdict(item))
                audit('source_end', asdict(item.raw))
                return
            if isinstance(item, SourceQuality):
                audit('source_quality', item.raw)
            elif isinstance(item, MessageBoundary):
                if start.raw.role == 'update':
                    observe_message(dict(item.raw,epoch=item.raw_time.epoch,microsecond=item.raw_time.microsecond),window_binding,time_coverage)
                current = item
                audit('source_message', item.raw)
                qualification.boundary(item)
            elif isinstance(item, Element):
                if current is None or item.raw['message_id'] != current.raw['message_id']:
                    raise ValueError('元素缺原消息')
                qualification.position = item.position.sort_key
                yield adapt_element(item.raw, run_id=reader.run_id, snapshot=reader.snapshot,
                                    collector_id=scope.collector_id, quality=current.raw['quality'])
            else:
                raise ValueError('来源未闭合')
        raise ValueError('缺少SourceEnd')
    def validate():
        verify()
        reader.selection.check()
        reader.selection.check_sources([cp['source_id'] for cp in reference_checkpoints.values()])
        if binding_from_reader(reader) != binding:
            raise ValueError('M3绑定漂移')
        current={cp['source_id']:cp for cp in reader.selection.checkpoints}
        actual_refs={role:current[source['source_id']] for role,source in metadata.raw_rows.items()}
        if actual_refs!=reference_checkpoints:raise ValueError('参考checkpoint漂移')
        return dict(input_binding=asdict(binding_from_reader(reader)),reference_checkpoints=actual_refs,
                    reference_sources=metadata.raw_rows,reference_interpretation=metadata.row_refs)
    try:
        first = next(stream)
        if not isinstance(first,SourceStart) or first.raw.role != 'baseline':
            raise ValueError('M3首来源必须为冷启动RIB')
        qualification.source_id = first.raw.source_id
        audit('source_start', asdict(first.raw))
        engine = StreamingDetectionEngine(StreamingSeed(source_elements(first),
                    f'{reader.run_id}:{reader.snapshot}/{first.raw.source_id}', first.raw.expected_elements),
                    references, scope, sink=sink)
        qualification.source_complete(ends[-1])
        seed_seconds = time.monotonic()-started
        for start in stream:
            if not isinstance(start,SourceStart) or start.raw.role != 'update':
                raise ValueError('M3基线之后仅UPDATE，不用snapshot重置')
            qualification.source_id = start.raw.source_id
            audit('source_start', asdict(start.raw))
            boundary = boundaries[start.raw.source_id]
            if boundary.file_id != start.raw.source_id or boundary.source_version != scope.input_version:
                raise ValueError('文件边界错绑定')
            engine.begin_file(boundary)
            for element in source_elements(start):
                engine.consume(element)
                if engine.status in ('partial','failed'): raise ValueError('M3科学计算失败')
            engine.finish_file()
            if engine.status in ('partial','failed'): raise ValueError('M3尾部计算失败')
            qualification.source_complete(ends[-1])
        if [e['raw']['source_id'] for e in ends] != list(reader.sources):
            raise ValueError('M3所选源没有完全耗尽')
        compute_seconds = time.monotonic()-started-seed_seconds
        engine.m3_qualification = qualification.state()
        store.save_state(engine)
        audit('input_completion',dict(sources=[e['raw'] for e in ends], baseline_count=engine.baseline_count))
        identity['qualification_as_of_position'] = qualification.position
        identity['telemetry'] = dict(seed_seconds=seed_seconds, compute_seconds=compute_seconds,
                                    before_finish_seconds=time.monotonic()-started,
                                    process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),
                                    memory_scope='Detection子进程；不含独立PG服务',
                                    scientific_records=store.count, state_entries=store.state_count,
                                    output_bytes=sum(p.stat().st_size for p in Path(output).rglob('*') if p.is_file()),
                                    output_bytes_scope='当前本地输出目录；不含PG/WAL；finish前',
                                    qualification_entries=qualification.count)
        snapshot = store.finish(validate_before_commit=validate)
        return dict(run_id=store.run_id,snapshot=snapshot,state='complete',output_profile=output_profile,
                    output=str(Path(output)), source_receipts=ends,
                    total_seconds=time.monotonic()-started)
    except BaseException as error:
        store.fail(str(error))
        raise
    finally:
        stream.close()
        store.close()
