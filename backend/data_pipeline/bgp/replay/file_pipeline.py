"""显式离线候选：原生 M2 独立进程 + 文件 checkpoint 驱动的路由状态。

不调用特征、检测或发布；公开封存 Reader 的资格要求保持原样。
"""
from contextlib import closing
import ctypes
import json
import multiprocessing
import os
from pathlib import Path
import signal
import sys
import time
from types import SimpleNamespace
import uuid

import psycopg2

from data_pipeline.bgp.replay.quality_overlay import CanonicalReplay
from data_pipeline.bgp.archive.checkpoint import Guard, durable, event, file_sha, plan_for, produce_checkpointed, sha
from data_pipeline.bgp.archive.file_reader import CheckpointReader, selected_prefix
from data_pipeline.bgp.input.native_parser import NativeRuntime, PARSER_AUTHORITY
from data_pipeline.bgp.replay.run_from_files import code_identity
from data_pipeline.bgp.replay.archive_input import MAPPING_RULE, mapping_rows
from data_pipeline.bgp.state.checkpoint import StateStore
from data_pipeline.common.run_metrics import Metrics
from data_pipeline.bgp.replay.route_replay import ReplayPlan

VERSION='route-file-pipeline/v1'


def _producer(config,max_pending,hook,parent_pid):
    """独占进程组；父进程消失时连同所属原生子进程一起退出。"""
    os.setsid()
    def stop(*_):
        signal.signal(signal.SIGTERM,signal.SIG_DFL)
        os.killpg(os.getpgrp(),signal.SIGTERM)
    signal.signal(signal.SIGTERM,stop)
    if sys.platform=='linux':
        if ctypes.CDLL(None,use_errno=True).prctl(1,signal.SIGTERM,0,0,0)!=0:raise OSError('父进程退出保护设置失败')
        if os.getppid()!=parent_pid:stop()
    root=Path(config['output']);root.mkdir(parents=True,exist_ok=True)
    with (root/'producer.log').open('a') as log:
        os.dup2(log.fileno(),1);os.dup2(log.fileno(),2)
        def boundary(label,ordinal,owner):
            if hook is not None:hook(label,ordinal)
            if label!='after_checkpoint':return
            while True:
                owner.check()
                with psycopg2.connect(config['dsn']) as pg,pg.cursor() as c:
                    c.execute('SELECT cursor FROM route_file.runs WHERE run_id=%s',(config['run_id'],))
                    row=c.fetchone()
                if row is None:raise ValueError('路由消费者登记丢失')
                if ordinal-row[0]<max_pending:return
                time.sleep(.1)
        produce_checkpointed(**config,hook=boundary)


def _stop(process):
    if process is None:return
    if process.is_alive():
        # setsid 之前的启动失败只终止该 PID，绝不向调用者进程组发信号。
        try:
            if os.getpgid(process.pid)==process.pid:os.killpg(process.pid,signal.SIGTERM)
            else:process.terminate()
        except ProcessLookupError:pass
        process.join(10)
        if process.is_alive():
            try:
                if os.getpgid(process.pid)==process.pid:os.killpg(process.pid,signal.SIGKILL)
                else:process.kill()
            except ProcessLookupError:pass
    process.join(10);process.close()


def _inventory(runtime,plan,binding_id,root,guard,metrics):
    path=root/'endpoint-inventory.json'
    updates=[e for e in plan['manifest']['inputs'] if e['role']=='update']
    with metrics.measure('endpoint_inventory'):
        if path.exists():
            result=json.loads(path.read_text())
            if result['binding_id']!=binding_id or sha({k:v for k,v in result.items() if k!='digest'})!=result['digest']:
                raise ValueError('端点预扫绑定或摘要漂移')
        else:
            files=[]
            for e in updates:
                value=runtime.endpoints(e['path'],e['source_id'],e['sha256'],root/(e['source_id']+'.endpoints.json'),
                                        policy=plan['policy'],guard=guard)
                files.append(value)
                if sum(len(f['endpoints']) for f in files)>1000000:raise ValueError('端点预扫总容量上限')
            result=dict(binding_id=binding_id,rule=MAPPING_RULE,files=files)
            result['digest']=sha(result);durable(path,result)
        if [(f['source_id'],f['sha256']) for f in result['files']]!=[(e['source_id'],e['sha256']) for e in updates]:
            raise ValueError('端点预扫来源不全或乱序')
        for e in updates:
            guard()
            if file_sha(e['path'])!=e['sha256']:raise ValueError('端点预扫原件漂移')
    return result


def _combined(inventory):
    combined={}
    for f in inventory['files']:
        for *endpoint,witness,count in f['endpoints']:
            key=tuple(endpoint)
            old=combined.get(key)
            combined[key]=(min(old[0],witness),old[1]+count) if old else (witness,count)
            if len(combined)>65536:raise ValueError('完整端点集合容量上限')
    return [(*key,*value) for key,value in combined.items()]


def _same_endpoints(actual,expected):
    return sorted(tuple(row) for row in actual)==sorted(tuple(row) for row in expected)


def run_pipeline(manifest,dsn,output,*,native_build,run_id=None,policy='strict/v1',catalog_data_path=None,
                 batch_rows=50000,min_free_bytes=2*1024**3,max_rss_bytes=4*1024**3,
                 duckdb_memory_limit='1GB',max_pending_files=2,hook=lambda *a:None,producer_hook=None):
    """同一绑定和输出目录重入即恢复；hooks 仅供 fixture 注入明确故障边界。"""
    if not 1<=max_pending_files<=8:raise ValueError('未消费文件上限必须为 1 至 8')
    if not 1<=batch_rows<=200000 or min_free_bytes<0 or max_rss_bytes<1:raise ValueError('流水线资源参数无效')
    root=Path(output).resolve();root.mkdir(parents=True,exist_ok=True)
    data_path=str(Path(catalog_data_path or root/'data').resolve())
    guard=Guard(root,data_path,min_free_bytes,max_rss_bytes);guard()
    identity=root/'pipeline-run.json'
    if identity.exists():
        saved=json.loads(identity.read_text())['run_id']
        if run_id is not None and saved!=run_id:raise ValueError('流水线运行身份冲突')
        run_id=saved
    else:
        run_id=run_id or uuid.uuid4().hex
        if not run_id.isalnum():raise ValueError('流水线运行身份非法')
        durable(identity,dict(run_id=run_id))
    if not run_id.isalnum():raise ValueError('流水线运行身份非法')
    runtime=NativeRuntime(native_build)
    plan,base_id=plan_for(manifest,policy)
    plan['native']=runtime.identity;plan['parser_authority']=PARSER_AUTHORITY;plan['native_batch_rows']=batch_rows
    plan_id=sha([base_id,runtime.identity,PARSER_AUTHORITY,batch_rows])
    binding=dict(version=VERSION,observation_plan_id=plan_id,observation_run=run_id,rule=MAPPING_RULE,
                 data_path=data_path,calculation_window=None,profile='route-file-candidate/v1')
    binding['binding_id']=sha(binding)
    invocation=uuid.uuid4().hex;process=None;reducer=None
    metrics=Metrics(root/('route-samples-'+invocation+'.jsonl'),resource_metrics=True)
    try:
        with closing(StateStore(dsn,run_id,root/'route-files',binding,guard=guard,metrics=metrics)) as store:
            before=selected_prefix(dsn,run_id)
            if before is not None and before['plan_id']!=plan_id:raise ValueError('M2 与路由运行计划不符')
            position,meta,_=store.status()
            if position>=0:
                if before is None or len(before['checkpoints'])<=position:raise ValueError('路由消费超出 M2 提交前缀')
                if [r[1] for r in store.files()]!=[c['digest'] for c in before['checkpoints'][:position+1]]:
                    raise ValueError('已消费 checkpoint 漂移')
            config=dict(manifest=manifest,dsn=dsn,output=str(root/'m2'),run_id=run_id,policy=policy,
                catalog_data_path=data_path,batch_rows=batch_rows,min_free_bytes=min_free_bytes,max_rss_bytes=max_rss_bytes,
                duckdb_memory_limit=duckdb_memory_limit,native_build=str(runtime.build))
            process=multiprocessing.get_context('spawn').Process(target=_producer,args=(config,max_pending_files,producer_hook,os.getpid()))
            process.start();event(root,'producer_started',pid=process.pid,invocation=invocation)
            inventory=_inventory(runtime,plan,binding['binding_id'],root,guard,metrics)
            inventory_by_id={f['source_id']:f for f in inventory['files']}
            endpoints=_combined(inventory)
            selected=(plan['manifest']['baseline_source'],*plan['manifest']['update_sources'])
            if meta is not None:
                replay_plan=ReplayPlan(**meta['plan'])
                reducer=CanonicalReplay(replay_plan,SimpleNamespace(binding_id=binding['binding_id']),selected)
                with metrics.measure('state_restore'):store.restore(reducer)
            while True:
                guard();store.owner.check()
                current=selected_prefix(dsn,run_id)
                if current is not None and current['plan_id']!=plan_id:raise ValueError('M2 计划漂移')
                pending=[] if current is None else current['checkpoints'][position+1:]
                if pending:
                    cp=pending[0];entry=plan['inputs'][cp['ordinal']];reader=None;mappings=[]
                    started=time.monotonic();event(root,'consume_start',ordinal=cp['ordinal'],monotonic=started)
                    if entry['source_id'] in selected:
                        reader=CheckpointReader(dsn,run_id,cp['ordinal'],cp['digest'],guard=guard)
                        if entry['role']=='baseline':
                            mapped,mappings=mapping_rows(reader.peers(),endpoints,selected[0],selected[1:])
                            reducer=CanonicalReplay(ReplayPlan(manifest['collector'],selected[0],selected[1:],mapped),
                                SimpleNamespace(binding_id=binding['binding_id']),selected)
                        else:
                            expected=inventory_by_id[entry['source_id']]
                            if cp['counts']['messages']!=expected['records'] or not _same_endpoints(reader.endpoints(),expected['endpoints']):
                                raise ValueError('实际 M2 端点与预扫结果不一致')
                    def check():
                        guard()
                        if code_identity()!=plan['code']:raise ValueError('路由处理期间代码漂移')
                        checked=selected_prefix(dsn,run_id)
                        if checked is None or checked['plan_id']!=plan_id or checked['checkpoints'][cp['ordinal']]!=cp:
                            raise ValueError('消费提交前 checkpoint 漂移')
                    with metrics.measure('route_file_commit'):
                        with store.commit_file(cp,reducer,check=check,hook=hook) as delta:
                            for row in mappings:delta.append('baseline_mappings',row)
                            if reader is not None:
                                stream=reader.ordered(binding['binding_id'])
                                try:
                                    for count,item in enumerate(stream):
                                        for table,row in reducer.apply(item):delta.append(table,row)
                                        if count%512==0:guard()
                                finally:stream.close()
                    position=cp['ordinal']
                    event(root,'consume_committed',ordinal=position,seconds=time.monotonic()-started,monotonic=time.monotonic())
                    continue
                if current is not None and current['state']=='observation_sealed' and position+1==len(plan['inputs']):
                    process.join(.1)
                    if process.exitcode is not None:
                        if process.exitcode!=0:raise RuntimeError('M2 子进程失败，候选未完成；查看 m2/producer.log')
                        from data_pipeline.bgp.archive.selection import Selection
                        seal=current['seal'];selection=Selection(dsn,run_id,seal['snapshot'])
                        db=selection.connect();db.close()
                        if reducer is None:raise ValueError('路由基线未完成')
                        check_code=code_identity()
                        if check_code!=plan['code']:raise ValueError('路由完成前代码漂移')
                        with metrics.measure('route_finalize'):result=store.finish(reducer,seal,hook=hook)
                        durable(root/('route-metrics-'+invocation+'.json'),metrics.receipt())
                        return result
                if not process.is_alive():
                    latest=selected_prefix(dsn,run_id)
                    if latest is not None and latest!=current:continue
                    raise RuntimeError('M2 子进程退出，保留已提交候选；查看 m2/producer.log')
                time.sleep(.1)
    except BaseException as exc:
        event(root,'route_error',invocation=invocation,reason=str(exc))
        durable(root/('route-failure-'+invocation+'.json'),dict(reason=str(exc),metrics=metrics.receipt(),qualification='incomplete'))
        raise
    finally:
        _stop(process);metrics.close()
