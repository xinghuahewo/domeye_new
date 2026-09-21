"""M3B 显式离线执行：固定 M2 → ordered → canonical → 独立投影。"""
from dataclasses import asdict
import json
from pathlib import Path
import resource
import shutil
import sys
import os
import subprocess
import time
from types import SimpleNamespace

from data_pipeline.bgp.ordered_reader import ordered, binding_from_reader
from data_pipeline.bgp.record_types import GAP_RULE_VERSION, ORDERED_VERSION
from data_pipeline.bgp.replay.archive_input import build_mapping, MAPPING_RULE
from data_pipeline.bgp.replay.route_replay import ReplayPlan, RULE
from data_pipeline.bgp.replay.quality_overlay import CanonicalReplay
from data_pipeline.bgp.replay.snapshot_store import ProjectionWriter, database_identity, closing
from data_pipeline.bgp.replay.snapshot_contract import encode
from data_pipeline.bgp.replay.snapshot_validation import reference_rows
from data_pipeline.common.run_metrics import Metrics
from data_pipeline.bgp.replay.run_from_files import code_identity
from data_pipeline.bgp.replay.calculation_window import calculation_window as checked_window, WINDOW_RULE


def produce_projection(reader,output,*,batch_rows=2048,memory_limit='1GB',
                       min_free_bytes=2*1024**3,max_rss_bytes=4*1024**3,hook=lambda stage,writer:None,
                       calculation_window=None):
    """不恢复业务中间状态；失败保留 Git 外候选，禁止 Web 隐式调用。"""
    binding=binding_from_reader(reader)
    selected=(reader.manifest['baseline_source'],*reader.manifest['update_sources'])
    if tuple(reader.sources)!=selected:raise ValueError('canonical 必须完整选择原基线与全部 UPDATE；不选额外 snapshot')
    if not 1<=batch_rows<=10000 or min_free_bytes<0 or max_rss_bytes<=0:raise ValueError('投影资源限制无效')
    explicit_window = None if calculation_window is None else checked_window(calculation_window, reader.manifest)
    root=Path(output).resolve();code=code_identity();writer=None;primary=None
    root.parent.mkdir(parents=True,exist_ok=True)
    samples=root.parent/(root.name+'-rss.jsonl')
    if samples.exists():raise ValueError('输出测量路径已经存在')
    metrics=None;volumes={s:dict(input_rows=0,output_rows=0,output_bytes=0) for s in ('mapping','replay','write','validate')}
    started=time.monotonic()
    def current_rss():
        if sys.platform=='linux':return int(Path('/proc/self/statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
        return int(subprocess.check_output(['/bin/ps','-o','rss=','-p',str(os.getpid())]))*1024
    rss_start=current_rss();data_root=Path(database_identity(reader.dsn)['data_root'])
    def guard():
        reader.guard()
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        if rss>max_rss_bytes:raise ValueError('M3 进程生命周期 RSS 上限触发')
        for target in (root,data_root):
            while not target.exists():target=target.parent
            if shutil.disk_usage(target).free<min_free_bytes:raise ValueError('M3 临时盘/数据盘资源保护触发')
    def write(table,row):
        guard()
        with metrics.measure('write'):writer.append(table,row)
        volumes['write']['input_rows']+=1;volumes['write']['output_rows']+=1
        volumes['write']['output_bytes']+=len(encode(row).encode())
    try:
        metrics=Metrics(samples)
        guard();mapping_rows=[]
        with metrics.measure('mapping'):
            sink=SimpleNamespace(dsn=reader.dsn,flush=lambda:None,append=lambda name,row:mapping_rows.append(row))
            endpoints=build_mapping(sink,selected[0],selected[1:],reader=reader)
        volumes['mapping']['output_rows']=len(mapping_rows)
        volumes['mapping']['output_bytes']=sum(len(encode(r).encode()) for r in mapping_rows)
        volumes['mapping']['input_rows']=sum(s.messages for s in binding.sources if s.source_id in selected)
        references=[]
        with metrics.measure('mapping'):
            references=list(reference_rows(reader,binding,guard))
        plan=json.loads(json.dumps(dict(input_manifest=reader.manifest,input_binding=asdict(binding),selected_sources=selected,references=references,
            code=code,algorithm=RULE,mapping_rule=MAPPING_RULE,gap_rule=GAP_RULE_VERSION,ordered_rule=ORDERED_VERSION,
            baseline_endpoints=endpoints,limitations=['cutover_assumed','source_order_declared','session_continuity_unknown'],
            resource_limits=dict(batch_rows=batch_rows,memory_limit=memory_limit,min_free_bytes=min_free_bytes,max_rss_bytes=max_rss_bytes))))
        if explicit_window is not None:
            plan.update(calculation_window=explicit_window,calculation_window_rule=WINDOW_RULE)
        with metrics.measure('write'):
            writer=ProjectionWriter(reader,root,plan,batch_rows=batch_rows,memory_limit=memory_limit,guard=guard)
        for row in mapping_rows:write('baseline_mappings',row)
        for row in references:write('reference_binding',row)
        core=CanonicalReplay(ReplayPlan(binding.collector,selected[0],tuple(selected[1:]),endpoints),binding,selected,window={k:reader.manifest[k] for k in ('window_start','window_end_exclusive')},
                             calculation_window=explicit_window)
        with closing(iter(ordered(reader))) as flow:
            while True:
                with metrics.measure('replay'):
                    try:item=next(flow)
                    except StopIteration:break
                volumes['replay']['input_rows']+=1
                results=iter(core.apply(item))
                while True:
                    with metrics.measure('replay'):
                        try:table,row=next(results)
                        except StopIteration:break
                    volumes['replay']['output_rows']+=1;volumes['replay']['output_bytes']+=len(encode(row).encode());write(table,row)
        exported=iter(core.export())
        while True:
            with metrics.measure('replay'):
                try:table,row=next(exported)
                except StopIteration:break
            volumes['replay']['output_rows']+=1;volumes['replay']['output_bytes']+=len(encode(row).encode());write(table,row)
        del core,exported,results  # 验证复用原 Replay 前释放生产态，不并存两份路由状态。
        hook('before_validate',writer)
        with metrics.measure('write'):writer.flush()
        with metrics.measure('validate'):
            ledger=writer.prepare()
            if code_identity()!=code:raise ValueError('M3 执行代码身份变化')
            reader.selection.check()
            files=writer.files()
        volumes['validate']['input_rows']=sum(v['rows'] for v in ledger.values())
        metrics.close()
        disk_bytes=sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
        measurements=metrics.receipt()|dict(wall_seconds=time.monotonic()-started,volumes=volumes,
            validation_reconsumption=writer.validation,
            output_payload_bytes=writer.payload_bytes,parquet_files=files,parquet_bytes=sum(f['bytes'] for f in files),
            current_rss_start_bytes=rss_start,current_rss_end_bytes=current_rss(),
            input_compressed_bytes=sum(e['size'] for e in reader.manifest['inputs'] if e['source_id'] in selected),
            temporary_disk_bytes=disk_bytes,
            temporary_disk_scope='output_directory_before_final_receipt_including_staging',
            input_messages=sum(s.messages for s in binding.sources if s.source_id in selected),
            input_elements=sum(s.elements for s in binding.sources if s.source_id in selected),
            gap_rows=writer.counts['scope_gap'],scale_scope='this_declared_run_only')
        result=writer.finish(ledger,measurements)
        return result
    except BaseException as exc:
        primary=exc
        if writer is not None:
            try:writer.fail(exc,metrics.receipt())
            except BaseException as failure:exc.cleanup_errors=(*getattr(exc,'cleanup_errors',()),failure)
        raise
    finally:
        errors=[]
        for action in (metrics.close if metrics is not None else lambda:None,writer.close if writer is not None else lambda:None):
            try:action()
            except BaseException as failure:errors.append(failure)
        if errors:
            if primary is not None:primary.cleanup_errors=(*getattr(primary,'cleanup_errors',()),*errors)
            else:
                errors[0].cleanup_errors=tuple(errors);raise errors[0]
