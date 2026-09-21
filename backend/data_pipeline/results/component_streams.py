"""固定组件完整流计划及尾回执屏障；不存第二套科学正文。"""
from contextlib import contextmanager
from dataclasses import dataclass
from copy import deepcopy
from data_pipeline.results.manifest_io import require, digest


def requests(owner, *, batch_rows, batch_bytes, admission=None):
    """只使用固定已接受的公开view；国家与趋势均保留各自原codec。"""
    if owner == 'resource':
        from data_pipeline.analysis.resources.publication import CODEC, typed
        views = ('metrics','normal_bands','topology_status','coverage')
        scope = {'scope':'all'}
    elif owner == 'feature':
        from data_pipeline.analysis.features.publication import typed
        from data_pipeline.analysis.features.publication_io import CODEC
        views = ('windows','coverage')
        scope = {'mode':'all','window_role':'all'}
    elif owner == 'detection':
        from data_pipeline.analysis.detection.publication import CODEC, typed, untyped
        require(admission is not None, 'Detection读取缺原Admission处理位置')
        position = untyped(admission['owner_binding'])['identity']['qualification_as_of_position']
        # 原三表和派生完整修订/details/资格/结果窗均可达，不只检查kind标签。
        views = ('records','state_entries','m3_entries','revisions','decisions',
                 'qualified_revisions','result_revisions','result_coverage')
        scope = {'start':0,'stop':None,'key':None,'at_position':None}
    elif owner == 'country':
        from data_pipeline.analysis.country_events.result_admission import CODEC, typed
        from data_pipeline.results.country_metadata import window
        require(admission is not None, 'Country读取缺原Admission窗口')
        views=('events','country_qualification','country_qualified_value','country_coverage')
        scope=dict(window_us=window(admission),dimension=None,incident_id=None,revision=None,
                   table=None,after_sequence=-1,stop_sequence=None)
    elif owner == 'trend':
        from data_pipeline.analysis.country_trends.stream_schema import CODEC, encode as typed, TABLES
        views=TABLES
        scope=dict(event=None,key_typed=None,after_sequence=-1,stop_sequence=None)
    else:
        raise ValueError('尚缺正式完整流计划：'+str(owner))
    require(type(batch_rows) is int and batch_rows > 0 and type(batch_bytes) is int and batch_bytes > 0, '批预算无效')
    return tuple(dict(view=view,scope_typed=typed(dict(scope, at_position=position)
                      if owner == 'detection' and view == 'qualified_revisions' else scope),codec_version=CODEC,
                      batch_rows=batch_rows,batch_bytes=batch_bytes) for view in views)


@dataclass
class CompleteStreams:
    iterator: object = None
    receipts: object = None
    qualification_summary: object = None
    progress_errors: object = None
    def __iter__(self): return self
    def __next__(self): return next(self.iterator)


@contextmanager
def read_required(graph, components, *, batch_rows, batch_bytes, max_rows, max_bytes, max_row_bytes=None, progress=None):
    """全量计划先固定；任一早停/封装/尾失败时不给整组成功回执。

    调用方可逐批建立已有业务查询副本或做领域边核验，不把本屏障
    当成六类/国家/趋势领域验收。主值/资格始终保持owner原typed文本。
    max_rows 仅保留正整数兼容；max_bytes 仅限制整件驻留资格摘要，
    不限制正文累计量。行/编码字节仍按原批计量，不向下传递剩余额度。
    """
    from data_pipeline.results.stream_policy import consume
    plan=[]
    for a in components:
        rows,size=(batch_rows,batch_bytes) if max_row_bytes is None else graph.envelope(
            a['admission_id'],batch_rows=batch_rows,max_row_bytes=max_row_bytes)
        plan.extend((a,req) for req in requests(a['owner'],batch_rows=rows,batch_bytes=size,admission=a))
    require(plan and type(max_rows) is int and max_rows > 0 and type(max_bytes) is int and max_bytes > 0, '完整读取兼容参数无效')
    session = CompleteStreams(); completed = []; summaries = []; exhausted = False
    rows=size=view_rows=view_bytes=0;current=None;last_completed=None
    def notify(state,error=None):
        if progress is not None:
            event=dict(state=state,owner=current[0] if current else None,view=current[1] if current else None,
                request_digest=current[2] if current else None,
                observed_rows=rows,observed_bytes=size,view_observed_rows=view_rows,view_observed_bytes=view_bytes,
                completed_views=len(completed),planned_views=len(plan),last_completed_view=last_completed,
                error=None if error is None else dict(type=type(error).__name__,message=str(error)[:1024]))
            try:progress(event)
            except Exception as observation_error:
                session.progress_errors=dict(type=type(observation_error).__name__,message=str(observation_error)[:512],
                    count=(session.progress_errors['count'] if session.progress_errors else 0)+1)
    def iterate():
        nonlocal exhausted,rows,size,view_rows,view_bytes,current,last_completed
        for admission, request in plan:
            current=(admission['owner'],request['view'],digest(request));view_rows=view_bytes=0
            notify('view_started')
            coverage_view = {'resource':'coverage','feature':'coverage','detection':'result_coverage','country':'country_coverage','trend':'result_availability'}.get(admission['owner'])
            coverage_batches = []; coverage_rows = coverage_bytes = 0
            with graph.read(admission['admission_id'],request,max_rows=max_rows,max_bytes=max_bytes) as stream:
                for batch in stream:
                    rows += batch['rows']; size += batch['bytes']
                    view_rows+=batch['rows'];view_bytes+=batch['bytes'];notify('reading')
                    if request['view'] == coverage_view:
                        # 此列表驻留至资格摘要封装；保护它，不用正文累计量挡流。
                        coverage_bytes += batch['bytes']
                        require(coverage_bytes <= max_bytes, '组合驻留资格原批字节超限')
                        coverage_batches.append(batch['rows_typed']); coverage_rows += batch['rows']
                    if max_row_bytes is None:
                        yield admission['owner'],deepcopy(request),batch
                    else:
                        # 消费批不是 owner 原批；原 coverage/receipt 仍按上方实际源记录。
                        for consumed in consume(admission['owner'],batch,batch_rows=batch_rows,
                                                batch_bytes=batch_bytes,envelope_bytes=request['batch_bytes']):
                            graph.guard()
                            yield admission['owner'],deepcopy(request),consumed
            require(stream.receipt is not None, '组件完整流缺尾回执')
            if request['view'] == coverage_view:
                from data_pipeline.results.component_roles import codec
                require(coverage_rows > 0, '必需覆盖流缺失，不能由零业务行补造')
                # 嵌套原typed文本保留原批边界和类型；不把coverage的Unknown改complete。
                text = codec(admission['owner']).typed(dict(request=request,batches=coverage_batches,receipt=stream.receipt))
                require(len(text.encode()) <= max_bytes, '组合资格摘要字节超限')
                summaries.append(dict(component_key=admission['owner'],storage_state='complete',
                                      coverage_typed=text,codec_version=request['codec_version']))
            completed.append(dict(owner=admission['owner'],request=deepcopy(request),receipt=deepcopy(stream.receipt)))
            last_completed=dict(owner=admission['owner'],view=request['view'])
            notify('view_completed')
        exhausted = True
    session.iterator = iterate()
    primary = None
    try: yield session
    except BaseException as error:
        primary = error
        raise
    finally:
        try: session.iterator.close()
        except BaseException as error:
            if primary is None: raise
            primary.cleanup_errors = (*getattr(primary,'cleanup_errors',()),error)
        if primary is not None:notify('failed',primary)
        elif not exhausted:notify('interrupted')
    if exhausted:
        try:
            notify('tail_verification')
            graph.current()
            require([s['component_key'] for s in summaries] == [a['owner'] for a in components], '尚缺正式主体覆盖摘要适配')
        except BaseException as error:
            notify('failed',error);raise
        session.receipts = tuple(completed)
        session.qualification_summary = tuple(summaries)
        notify('complete')
