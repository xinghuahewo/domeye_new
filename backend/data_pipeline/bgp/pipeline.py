"""有序原生批次直接计算，文件 EOF/归档校验后提交唯一状态写入者。"""
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
import time
import uuid

from data_pipeline.bgp.replay.quality_overlay import CanonicalReplay
from data_pipeline.bgp.archive.checkpoint import Guard, durable, event, plan_for, produce_checkpointed, sha
from data_pipeline.bgp.archive.file_reader import CheckpointBinding, selected_prefix
from data_pipeline.bgp.archive.native_writer import arrow_file
from data_pipeline.bgp.input.native_parser import NativeRuntime, PARSER_AUTHORITY
from data_pipeline.bgp.input.native_batches import HotPaths, SegmentStream, persisted_segments, persisted_paths, peer_rows
from data_pipeline.bgp.replay.run_from_files import code_identity
from data_pipeline.bgp.replay.archive_input import MAPPING_RULE, mapping_rows
from data_pipeline.bgp.state.rib_columns import RibBatch
from data_pipeline.bgp.state.update_columns import UpdateBatch
from data_pipeline.bgp.state.checkpoint import StateStore
from data_pipeline.common.run_metrics import Metrics
from data_pipeline.bgp.replay.route_replay import ReplayPlan

VERSION = 'route-batch-pipeline/v3'


def business_runtime():
    """旧检测存在集合枚举槽位；冻结解释器散列条件，不重定义原算法的起源选择。"""
    seed=os.environ.get('PYTHONHASHSEED','')
    if not seed.isdigit() or not 0<=int(seed)<=4294967295:
        raise ValueError('业务计算需从固定 PYTHONHASHSEED 的解释器启动；命令入口默认使用 0')
    return dict(python=list(sys.version_info[:3]),implementation=sys.implementation.name,hash_seed=seed,
                hash_probe=[hash(s) for s in ('domeye-runtime/v1','1','3','10.0.0.0/24')])


class BatchObserver:
    def __init__(self, store, plan, binding, inventory, root, guard, metrics, hook, business_config=None, runtime=None):
        from data_pipeline.bgp.replay.file_pipeline import _combined
        self.store,self.plan,self.binding,self.root=store,plan,binding,root
        self.inventory={f['source_id']:f for f in inventory['files']}
        self.endpoints=_combined(inventory)
        self.selected=(plan['manifest']['baseline_source'],*plan['manifest']['update_sources'])
        self.ranks={e['source_id']:i for i,e in enumerate(plan['manifest']['inputs'])}
        self.guard,self.metrics,self.hook=guard,metrics,hook
        self.runtime=runtime
        self.position,meta,_=store.status();self.reducer=None
        if meta is not None:
            self.reducer=CanonicalReplay(ReplayPlan(**meta['plan']),SimpleNamespace(binding_id=binding['binding_id']),self.selected,compact=True,detailed=store.audit_transitions,legacy_views=store.audit_transitions)
            with metrics.measure('state_restore'):store.restore(self.reducer)
            self.reducer.replay.memory.native=runtime
        # 仅为本次调用的可重建热索引；前缀状态的持久化不依赖该缓存存活。
        self.paths=HotPaths()
        self.context=self.delta=self.stream=self.cp=None;self.ordinal=None
        self.business=None
        if business_config is not None:
            from data_pipeline.bgp.state.business import BusinessLoop
            self.business=BusinessLoop(business_config,plan,binding,self.paths.texts,lambda t,r:self.delta.append(t,r),
                                       raw_sink=lambda t,r:self.delta.append_business(t,r),metrics=metrics)
            if meta is not None:store.restore_business(self.business)
        self.expected_peers=None;self.segments=0
        self.business_wall=0.;self.business_cpu=0.;self.canonical_wall=0.;self.canonical_cpu=0.
        self.update_wall=0.;self.update_cpu=0.
        self.worker=ThreadPoolExecutor(max_workers=1,thread_name_prefix="route-writer")
        self.pending=None;self.queue_peak=0

    def begin(self, ordinal, peers):
        if self.context is not None or ordinal!=self.position+1:raise ValueError('计算文件开始位置冲突')
        entry=self.plan['inputs'][ordinal];mappings=[]
        if entry['role']=='baseline':
            mapped,mappings=mapping_rows(peers,self.endpoints,self.selected[0],self.selected[1:])
            self.expected_peers=sorted(peers)
            self.reducer=CanonicalReplay(ReplayPlan(self.plan['manifest']['collector'],self.selected[0],self.selected[1:],mapped),
                SimpleNamespace(binding_id=self.binding['binding_id']),self.selected,compact=True,detailed=self.store.audit_transitions,legacy_views=self.store.audit_transitions)
            self.reducer.replay.memory.native=self.runtime
        if self.reducer is None and entry['source_id'] in self.selected:raise ValueError('缺少基线状态')
        self.ordinal=ordinal;self.cp=None;self.started=time.monotonic()
        event(self.root,'consume_start',ordinal=ordinal,mode='direct_batch')
        def check():
            if code_identity()!=self.plan['code']:raise ValueError('计算期间代码漂移')
            selected=selected_prefix(self.store.dsn,self.store.run_id)
            if selected is None or selected['checkpoints'][ordinal]!=self.cp:raise ValueError('提交前归档 checkpoint 漂移')
        self.context=self.store.commit_file(lambda:self.cp,self.reducer,ordinal=ordinal,check=check,hook=self.hook,business=self.business if self.reducer is not None else None)
        self.delta=self.context.__enter__()
        for row in mappings:self.delta.append('baseline_mappings',row)
        if entry['source_id'] in self.selected:
            binding=CheckpointBinding(self.binding['binding_id'],self.plan['manifest']['collector'],self.store.run_id,None,(),
                                      profile='route-batch-candidate/v1')
            self.stream=SegmentStream(binding,self.ranks[entry['source_id']],entry,self.paths)
            self.stream.rib_batches = not self.store.audit_transitions
            self.stream.update_batches = not self.store.audit_transitions
            self.stream.update_rows = self.business is not None
            self.stream.update_fields = self.business is not None and not self.store.audit_transitions
            self.apply(self.stream.start)

    def apply(self,item,*,measure=True):
        if isinstance(item, UpdateBatch):
            from data_pipeline.bgp.state.update_fields import prepare_update_batch, apply_update_fields
            started=time.monotonic();started_cpu=time.thread_time()
            def business_element(element):
                self.business.apply(element)
            def business_fields(batch,index,boundary):
                apply_update_fields(self.business,batch,index,boundary)
            direct=self.business is not None and not self.store.audit_transitions
            if direct:prepare_update_batch(self.business,item)
            for boundary,begin,end,is_new in item.frames:
                if is_new:self.apply(boundary,measure=False)
                if begin==end:continue
                self.reducer.apply_update_rows(item,boundary,begin,end,self.delta.append,
                    business_element if self.business is not None and self.store.audit_transitions else None,
                    after_fields=business_fields if direct else None)
            # 状态与业务交替执行，整批计时不再伪装成可精确拆分的逐模块时间。
            self.update_wall+=time.monotonic()-started;self.update_cpu+=time.thread_time()-started_cpu
            return
        if isinstance(item, RibBatch):
            start=time.monotonic();cpu=time.thread_time()
            self.reducer.apply_rib_batch(item)
            self.canonical_wall+=time.monotonic()-start;self.canonical_cpu+=time.thread_time()-cpu
            if self.business is not None:
                start=time.monotonic();cpu=time.thread_time();self.business.apply_rib_batch(item)
                self.business_wall+=time.monotonic()-start;self.business_cpu+=time.thread_time()-cpu
            return
        if measure:start=time.monotonic();cpu=time.thread_time()
        for table,row in self.reducer.apply(item):self.delta.append(table,row)
        from data_pipeline.bgp.record_types import SourceEnd
        if isinstance(item,SourceEnd) and self.reducer.replay.memory is not None:
            self.reducer.replay.memory._rib_rows=None;self.reducer.replay.memory._rib_token=None
        if measure:self.canonical_wall+=time.monotonic()-start;self.canonical_cpu+=time.thread_time()-cpu
        if self.business is not None:
            if measure:start=time.monotonic();cpu=time.thread_time()
            self.business.apply(item)
            if measure:self.business_wall+=time.monotonic()-start;self.business_cpu+=time.thread_time()-cpu

    def segment(self,ordinal,native,segment,tables):
        if ordinal<=self.position:return
        entry=self.plan['inputs'][ordinal]
        if self.context is None:
            peers=[]
            if entry['role']=='baseline':
                # 现有完整源扫描对每张 Peer 表都取证，并在 NativeAudit 中逐条核对。
                peers=peer_rows(arrow_file(Path(native)/'sample-peers.arrow'))
            self.begin(ordinal,peers)
        if ordinal!=self.ordinal:raise ValueError('批次跨文件交错')
        if self.stream is not None:
            with self.metrics.measure('direct_batch_compute'):
                for i,item in enumerate(self.stream.segment(segment,tables)):
                    self.apply(item)
                    if i%512==0:self.guard()
            self.segments+=1
            self.hook('batch_computed',ordinal)

    def submit_segment(self,*args):
        if self.pending is not None:self.pending.result()
        self.pending=self.worker.submit(self.segment,*args)
        self.queue_peak=max(self.queue_peak,1)
        return self.pending

    def checkpoint(self,cp):
        if self.pending is not None:self.pending.result()
        return self.worker.submit(self._checkpoint,cp).result()

    def finish(self,seal):
        return self.worker.submit(self.store.finish,self.reducer,seal,hook=self.hook).result()

    def _checkpoint(self,cp):
        ordinal=cp['ordinal']
        if ordinal<=self.position:
            # 恢复已提交状态后，后续批次仍可能引用早期路径；只重建窄字典。
            if cp['source_id'] in self.selected:
                for table in persisted_paths(cp):self.paths.register(table)
            return
        entry=self.plan['inputs'][ordinal]
        if self.context is None:
            peers=[]
            if entry['role']=='baseline':
                for _,tables in persisted_segments(cp):peers.extend(peer_rows(tables.get('peers')))
            self.begin(ordinal,peers)
            if self.stream is not None:
                # 归档已经成功、计算尚未提交：重放保留的段，不重查全文件 JOIN。
                for segment,tables in persisted_segments(cp):
                    with self.metrics.measure('archived_batch_replay'):
                        for item in self.stream.segment(segment,tables):self.apply(item)
                    self.hook('batch_computed',ordinal)
        if self.ordinal!=ordinal:raise ValueError('文件 checkpoint 乱序')
        if self.stream is not None:
            if entry['role']=='baseline' and sorted(self.stream.peers)!=self.expected_peers:
                raise ValueError('实际 RIB Peer 表与完整扫描不符')
            if entry['role']=='update':
                from data_pipeline.bgp.replay.file_pipeline import _same_endpoints
                expected=self.inventory[entry['source_id']]
                actual=[(*key,*v) for key,v in self.stream.endpoints.items()]
                if cp['counts']['messages']!=expected['records'] or not _same_endpoints(actual,expected['endpoints']):
                    raise ValueError('实际 M2 端点与预扫结果不一致')
            self.apply(self.stream.end(cp))
        self.cp=cp
        context=self.context
        try:
            with self.metrics.measure('route_file_commit'):context.__exit__(None,None,None)
        finally:self.context=self.delta=self.stream=None
        self.position=ordinal
        event(self.root,'consume_committed',ordinal=ordinal,seconds=time.monotonic()-self.started)

    def close(self,error=None):
        try:
            self.worker.submit(self._close,error).result()
        finally:self.worker.shutdown(wait=True)

    def _close(self, error=None):
        if self.context is not None:
            error=error or RuntimeError('未完成文件，回滚候选工作态')
            self.context.__exit__(type(error),error,error.__traceback__)
            self.context=None
        self.paths.close()


def run_pipeline(manifest,dsn,output,*,native_build,run_id=None,policy='strict/v1',catalog_data_path=None,
                 batch_rows=50000,min_free_bytes=2*1024**3,max_rss_bytes=4*1024**3,
                 duckdb_memory_limit='1GB',hook=lambda *a:None,producer_hook=None,audit_transitions=False,business_config=None,
                 native_rib_state=False):
    from data_pipeline.bgp.replay.file_pipeline import _inventory
    if type(native_rib_state) is not bool:raise ValueError('native_rib_state 必须为布尔值')
    root=Path(output).resolve();root.mkdir(parents=True,exist_ok=True)
    data_path=str(Path(catalog_data_path or root/'data').resolve())
    guard=Guard(root,data_path,min_free_bytes,max_rss_bytes);guard()
    if not 1<=batch_rows<=200000:raise ValueError('原生批大小无效')
    identity=root/'pipeline-run.json'
    if identity.exists():
        saved=json.loads(identity.read_text())['run_id']
        if run_id is not None and run_id!=saved:raise ValueError('流水线运行身份冲突')
        run_id=saved
    else:
        run_id=run_id or uuid.uuid4().hex
        if not run_id.isalnum():raise ValueError('运行身份非法')
        durable(identity,dict(run_id=run_id))
    runtime=NativeRuntime(native_build);plan,base_id=plan_for(manifest,policy)
    plan.update(native=runtime.identity,parser_authority=PARSER_AUTHORITY,native_batch_rows=batch_rows)
    plan_id=sha([base_id,runtime.identity,PARSER_AUTHORITY,batch_rows])
    invocation=uuid.uuid4().hex;metrics=Metrics(root/('route-samples-'+invocation+'.jsonl'),resource_metrics=True)
    observer=None;error=None
    try:
        config_digest=None
        if business_config is not None:
            # 保留原 stable 摘要的字节、整数/字符串键和 NaN 解释；
            # 大参考按行流式编码，不生成整份规范化映射与 JSON 副本。
            from data_pipeline.analysis.detection._results import stable_stream
            with metrics.measure('business_config_digest'):
                config_digest=stable_stream(business_config)
                guard()
        binding=dict(version=VERSION,observation_plan_id=plan_id,observation_run=run_id,rule=MAPPING_RULE,
            rib_state_encoder='native' if native_rib_state else 'python',
            business_config_digest=config_digest,
            detection_enabled=business_config is not None and business_config.get('detection_enabled',True),
            business_runtime=None if business_config is None else business_runtime(),
            data_path=data_path,calculation_window=None,profile='route-batch-candidate/v1',audit_transitions=audit_transitions)
        binding['binding_id']=sha(binding)
        with closing(StateStore(dsn,run_id,root/'route-files',binding,guard=guard,metrics=metrics,audit_transitions=audit_transitions)) as store:
            selected=selected_prefix(dsn,run_id);position,_,_=store.status()
            if selected is not None and selected['plan_id']!=plan_id:raise ValueError('观察计划漂移')
            if position>=0 and (selected is None or [r[1] for r in store.files()]!=[c['digest'] for c in selected['checkpoints'][:position+1]]):
                raise ValueError('计算游标没有对应归档')
            inventory=_inventory(runtime,plan,binding['binding_id'],root,guard,metrics)
            with metrics.measure('state_business_initialization'):
                # 真实片段的原生状态打包尚未快于 Python 紧凑打包，保留显式对照开关。
                observer=BatchObserver(store,plan,binding,inventory,root,guard,metrics,hook,business_config,
                                       runtime if native_rib_state else None)
            def boundary(label,ordinal,owner):
                if producer_hook is not None:producer_hook(label,ordinal)
            try:
                with metrics.measure('pipeline_total'):
                    seal=produce_checkpointed(manifest,dsn,root/'m2',run_id=run_id,policy=policy,
                        catalog_data_path=data_path,batch_rows=batch_rows,min_free_bytes=min_free_bytes,max_rss_bytes=max_rss_bytes,
                        duckdb_memory_limit=duckdb_memory_limit,native_build=native_build,observer=observer,hook=boundary)
                    if observer.reducer is None:raise ValueError('基线未完成')
                    with metrics.measure('route_finalize'):result=observer.finish(seal)
                    return result
            except BaseException as exc:error=exc;raise
            finally:
                observer.close(error)
                durable(root/('route-metrics-'+invocation+'.json'),dict(**metrics.receipt(),segments=observer.segments,compute_queue_peak=observer.queue_peak,
                    single_writer=dict(canonical_wall_seconds=observer.canonical_wall,canonical_thread_cpu_seconds=observer.canonical_cpu,
                        business_wall_seconds=observer.business_wall,business_thread_cpu_seconds=observer.business_cpu,
                        update_combined_wall_seconds=observer.update_wall,update_combined_thread_cpu_seconds=observer.update_cpu,
                        timing_scope='UPDATE按批计时，包含控制消息、规范状态及业务；direct_batch_compute包含这些子项，不能直接相加'),
                    path_text_cache=dict(queries=observer.paths.texts.queries,single_reads=observer.paths.texts.single_reads),
                    path_cache=dict(storage="compact_memory",peak_estimated_memory_bytes=observer.paths.peak_memory_bytes,temporary_disk_peak_bytes=observer.paths.disk_peak,peak_entries=observer.paths.peak_entries,peak_bytes=observer.paths.peak_bytes,queries=observer.paths.queries)))
    except BaseException as exc:
        durable(root/('route-failure-'+invocation+'.json'),dict(reason=str(exc),metrics=metrics.receipt(),qualification='incomplete'))
        raise
    finally:metrics.close()
