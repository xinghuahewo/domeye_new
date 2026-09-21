"""冻结新进程Feature离线执行与显式人工fixture入口。"""
from dataclasses import asdict
import json
import time
import resource
import sys
import pyarrow as pa
from data_pipeline.analysis.features.calculation import RULES
from data_pipeline.analysis.features.projection import FeatureAdapter, PROJECTION_RULE
from data_pipeline.analysis.features.reference import load_reference, REFERENCE_RULE
from data_pipeline.analysis.features.store import FeatureStore
from data_pipeline.analysis.features.identity import execution_identity
from data_pipeline.analysis.features.inputs import FeatureInputs
from data_pipeline.analysis.features.qualification import Qualification, PROFILE, SCHEMA_VERSION, RULE as QUALIFICATION_RULE
from data_pipeline.bgp import record_types as ordered_types
from data_pipeline.bgp.archive.message_reader import SourceStart, SourceEnd, MessageBatch, byte_size
from data_pipeline.bgp.input.path_decoding import DECODER_VERSION, DIRECTION_RULE, decode_element, decoding_difference


def produce_bound_features(inputs, output, *, fixture_only=False, batch_rows=1000, batch_bytes=4*1024**2, guard=lambda: None, max_rss_bytes=1024**3):
    """固定多视图与参考；单一专用DSN，正式执行必须来自冻结新解释器。"""
    code=execution_identity(fixture_only=fixture_only)
    plan=inputs.plan
    external_guard=guard
    def guard():
        external_guard()
        if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)>max_rss_bytes:
            raise ValueError('Feature RSS资源保护触发')
    inputs.validate()
    source_keys={(v.run_id,v.snapshot) for v in inputs.views}
    single=next(iter(source_keys)) if len(source_keys)==1 else (None,None)
    spec={'observation_run':single[0],'observation_snapshot':single[1],
          'observation_version':plan.observation_version,'source_ids':[v.source_id for v in inputs.views],
          'source_bindings':inputs.source_specs,'reference_binding':inputs.reference_spec,
          'result_window':inputs._bounds(inputs.result_window),'comparison_window':inputs._bounds(inputs.comparison_window),
          'calculation_window':[plan.sources[1].window.start.isoformat(),plan.sources[-1].window.end.isoformat()] if len(plan.sources)>1 else None,
          'initial_rib_time':plan.sources[0].window.file_time.isoformat(),'prior_persistent_state':'Unknown',
          'windows':[dict(source_id=s.source_id,start=s.window.start.isoformat(),end=s.window.end.isoformat(),file_time=s.window.file_time.isoformat(),coverage=s.window.coverage,message_quality_state=s.message_quality_state,message_quality_refs=s.message_quality_refs) for s in plan.sources],
          'collector':plan.collector,'decoder_version':DECODER_VERSION,'direction_rule':DIRECTION_RULE,
          'algorithm_versions':RULES,'projection_rule':PROJECTION_RULE,'reference_rule':REFERENCE_RULE,
          'code_identity':code,'limitations':list(plan.limitations),
          'reference_sha256':inputs.reference_view.source_sha256,'reference_expected_rows':inputs.bound_reference[1]}
    qualified=inputs.profile=='observation'
    if qualified:
        spec.update(output_profile=PROFILE,output_schema=SCHEMA_VERSION,qualification_rule=QUALIFICATION_RULE,
                    ordered_bindings=[asdict(b) for b in inputs.ordered_bindings.values()],
                    required_references=[inputs.reference_spec],
                    observation_seals=[dict(run_id=r.run_id,snapshot=r.snapshot,seal=r.selection.seal)
                                       for r in sorted({*inputs.readers.values(),inputs.reference_reader},key=lambda r:(r.run_id,r.snapshot))],
                    reference_view=asdict(inputs.reference_view))
    store = FeatureStore(inputs.dsn,output,spec,batch_rows=batch_rows,batch_bytes=batch_bytes,guard=guard)
    try:
        states,timing,reference = _calculate(inputs,store,batch_rows=batch_rows,batch_bytes=batch_bytes,guard=guard)
        store.validate(states)
        if execution_identity(fixture_only=fixture_only)!=code:raise ValueError('完成前代码身份漂移')
        return store.finish({'source_metrics':timing,'reference_version':reference.version,
                             'peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)},
                            validate_inputs=inputs.validate)
    except BaseException as exc:
        store.fail(exc)
        raise
    finally:
        store.close()


def _calculate(inputs, store, *, batch_rows, batch_bytes, guard):
    """生产与离线审计复用同一计算循环；sink决定是否持久化，不创建运行。"""
    plan=inputs.plan
    qualified=inputs.profile=='observation'
    reference = load_reference(inputs.reference_reader,inputs.reference_view.source_sha256,inputs.reference_view.raw_path,
                sink=lambda row:store.append('reference_rows',row),guard=guard)
    states, timing = {}, []
    for mode in ('ordinary','ir'):
        adapter = FeatureAdapter(mode,reference,plan,store.run_id,retain_outputs=False)
        qualification=Qualification(mode,store) if qualified else None
        records = iter(inputs.stream())
        try:
            for rank,binding in enumerate(plan.sources):
                started = time.monotonic()
                control_start=next(records)
                if qualified and not isinstance(control_start,ordered_types.SourceStart):raise ValueError('缺少有序来源开始')
                start=control_start.raw if qualified else control_start
                source_spec=inputs.source_specs[rank]
                if not isinstance(start,SourceStart) or (start.run_id,start.snapshot,start.source_id,start.role)!=(source_spec['run_id'],source_spec['snapshot'],binding.source_id,source_spec['source_role']):
                    raise ValueError('缺少有序SourceStart')
                if qualified:
                    if (control_start.binding_ref,control_start.source_rank)!=(source_spec['ordered_binding_ref'],source_spec['upstream_source_rank']):
                        raise ValueError('原有序来源到Feature计算序映射冲突')
                    qualification.begin(rank,binding)
                source_end = None
                control_end = None
                def batches():
                    nonlocal source_end,control_end
                    pending=[];pending_bytes=0
                    for record in records:
                        if qualified:
                            guard()
                            # 仅在会改变资格的边界前清空科学元素；普通空边界不强制逐消息Arrow。
                            barrier=isinstance(record,(ordered_types.SourceEnd,ordered_types.SourceQuality)) or (
                                isinstance(record,ordered_types.MessageBoundary) and (record.gap is not None or record.raw['quality']))
                            if barrier and pending:
                                yield pa.RecordBatch.from_pylist(pending)
                                pending=[];pending_bytes=0
                            if record.binding_ref!=control_start.binding_ref or (record.position.message.source_rank if isinstance(record,ordered_types.Element) else record.position.source_rank if isinstance(record,ordered_types.MessageBoundary) else record.source_rank)!=control_start.source_rank:
                                raise ValueError('有序记录来源映射冲突')
                            if isinstance(record,ordered_types.SourceEnd):
                                source_end=record.raw;control_end=record
                                return
                            if isinstance(record,ordered_types.MessageBoundary):
                                if not binding.window.start.timestamp()<=record.raw_time.epoch+(record.raw_time.microsecond or 0)/1000000<binding.window.end.timestamp():
                                    raise ValueError('消息控制不属于声明窗口')
                                qualification.boundary(record)
                            elif isinstance(record,ordered_types.SourceQuality):
                                qualification.quality(None,record.raw['code'],record.raw['detail'])
                            elif isinstance(record,ordered_types.Element):
                                row=record.raw
                                qualification.element(row)
                                if mode=='ordinary':
                                    difference=decoding_difference(row)
                                    if difference is not None:store.append('decoding_differences',{**difference,'evidence':json.dumps(difference,ensure_ascii=False)})
                                decoded=decode_element(row);size=byte_size(decoded)
                                if pending and (len(pending)>=batch_rows or pending_bytes+size>batch_bytes):
                                    yield pa.RecordBatch.from_pylist(pending)
                                    pending=[];pending_bytes=0
                                pending.append(decoded);pending_bytes+=size
                                if len(pending)>=batch_rows or pending_bytes>=batch_bytes:
                                    yield pa.RecordBatch.from_pylist(pending)
                                    pending=[];pending_bytes=0
                            else:raise ValueError('不支持的有序控制')
                            continue
                        if isinstance(record,SourceEnd):
                            if (record.run_id,record.snapshot,record.source_id)!=(start.run_id,start.snapshot,binding.source_id): raise ValueError('SourceEnd身份不匹配')
                            source_end = record
                            return
                        if not isinstance(record,MessageBatch) or (record.run_id,record.snapshot,record.source_id)!=(start.run_id,start.snapshot,binding.source_id):
                            raise ValueError('消息流结构不匹配')
                        guard()
                        if record.elements:
                            decoded = []
                            for row in record.elements:
                                if mode == 'ordinary':
                                    difference = decoding_difference(row)
                                    if difference is not None:
                                        store.append('decoding_differences', {**difference,'evidence':json.dumps(difference,ensure_ascii=False)})
                                decoded.append(decode_element(row))
                            yield pa.RecordBatch.from_pylist(decoded)
                    raise ValueError('来源未结束')
                adapter.audit_sink = lambda row:store.audit(mode,rank,row)
                adapter.row_sink = lambda row:store.window_row(mode,rank,binding,reference,row)
                old = adapter.state
                result = adapter.consume_source(binding,batches())
                if source_end is None: raise ValueError('未核对来源结束回执')
                end_state = result.end_state if result else None
                store.save_diagnostics(mode,rank,binding,result)
                store.save_phases(mode,rank,binding,old,end_state,adapter.state)
                windows = (1+len(end_state.feature_dict)+sum(v.is_change for group in end_state.feature_dict.values() for v in group.values())) if result else 0
                if qualified:store.qualification_expected[mode,binding.source_id]=qualification.finish(control_end)
                store.source_complete(mode,rank,binding,source_end,old,adapter.state,windows)
                timing.append({'mode':mode,'source_id':binding.source_id,'seconds':time.monotonic()-started,
                               'messages':source_end.messages,'elements':source_end.elements,
                               'local_messages':source_end.local_messages,**adapter.copy_metrics})
            if next(records,None) is not None: raise ValueError('计划之外的来源')
        finally:
            records.close()
        states[mode] = adapter.state
    return states,timing,reference


def produce_features(reader,plan,reference_sha,reference_path,output,*,fixture_only=False,**kwargs):
    """保留已GO单run API，原来源角色和版本不变。"""
    execution_identity(fixture_only=fixture_only)
    inputs=FeatureInputs.single(reader,plan,reference_sha,reference_path)
    return produce_bound_features(inputs,output,fixture_only=fixture_only,**kwargs)


def run_fixture(reader,plan,reference_sha,reference_path,output,**kwargs):
    """显式人工兼容入口；正式读取默认拒绝其输出。"""
    return produce_features(reader,plan,reference_sha,reference_path,output,fixture_only=True,**kwargs)
