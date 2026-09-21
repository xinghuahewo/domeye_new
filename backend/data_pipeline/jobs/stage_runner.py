"""单次离线阶段观测；没有总时限、重试或按目录存在跳过阶段的行为。"""
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time


def save_new(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())


def save_final(path, value, primary=None):
    """不让观测落盘失败替代原错误；没有原错误时仍传播持久化失败。"""
    primary = primary if primary is not None else sys.exc_info()[1]
    try:
        save_new(path, value)
    except Exception as error:
        if primary is None:
            raise
        detail = dict(operation='save_receipt', path=str(path), type=type(error).__name__, message=str(error))
        primary.migration_cleanup_errors = [*getattr(primary, 'migration_cleanup_errors', []), detail]
        primary.migration_receipt_error = detail


def process_sample(pid, *, timeout=None):
    """ps瞬时RSS采样；不是内核高水位，不包含独立PG服务。"""
    result = subprocess.run(['ps', '-axo', 'pid=,ppid=,rss='], check=True,
                            capture_output=True, text=True, timeout=timeout)
    rows = {int(a): (int(b), int(c)*1024) for a,b,c in
            (line.split() for line in result.stdout.splitlines() if line.strip())}
    selected = {pid}
    while True:
        expanded = selected | {p for p,(parent,_) in rows.items() if parent in selected}
        if expanded == selected:
            break
        selected = expanded
    return dict(parent_rss_bytes=rows.get(pid, (None, None))[1],
                children_rss_bytes=sum(rows[p][1] for p in selected-{pid} if p in rows),
                observed_pids=sorted(selected & rows.keys()))


def group_members(pgid):
    result = subprocess.run(['ps','-axo','pid=,pgid=,stat='], check=True, capture_output=True, text=True)
    return {int(pid): state for pid,group,state in (line.split() for line in result.stdout.splitlines() if line.strip())
            if int(group) == pgid}


def stop_process_group(process, grace_seconds):
    """仅终止本次start_new_session创建的组；等待整个组，而不只等待父PID。"""
    pgid = process.pid
    if type(pgid) is not int or pgid <= 1 or pgid == os.getpgrp():
        raise RuntimeError('拒绝清理非本次隔离进程组')
    signals = []
    observation_errors = []
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(pgid, sig)
            signals.append(sig.name)
        except ProcessLookupError:
            return dict(state='group_exited', signals=signals, exit_code=process.wait(), remaining_pids=[], observation_errors=observation_errors)
        end = time.monotonic()+grace_seconds
        while True:
            process.poll()  # 回收直接子进程，但不据此推断其后代已经退出。
            try:
                members = group_members(pgid)
                live = [pid for pid,state in members.items() if not state.startswith('Z')]
            except Exception as error:
                # 清理观测故障不能阻止对已确认失败的自建组发送后续KILL。
                observation_errors[:] = [dict(type=type(error).__name__, message=str(error)[:1024])]
                try:
                    os.killpg(pgid, 0)
                    live = ['group_members_unknown']
                except ProcessLookupError:
                    members, live = {}, []
            if not live:
                return dict(state='group_exited', signals=signals, exit_code=process.wait(),
                            remaining_pids=[], exited_unreaped_pids=list(members), observation_errors=observation_errors)
            if time.monotonic() >= end:
                break
            time.sleep(0.05)
    raise RuntimeError('SIGKILL后仍不能确认本次进程组退出：'+str(live))


def run_stage(argv, output, *, threads, max_rss_bytes, min_free_bytes, disk_roots,
              sample=process_sample, launch=subprocess.Popen, disk_usage=shutil.disk_usage,
              termination_grace_seconds=5):
    """执行原入口并留存原输出；成功退出也不等于业务准入。"""
    if any(type(n) is not int or n <= 0 for n in (threads, max_rss_bytes, min_free_bytes)):
        raise ValueError('线程、RSS与磁盘保护必须显式为正整数')
    if type(termination_grace_seconds) not in (int,float) or not math.isfinite(termination_grace_seconds) or not 0 < termination_grace_seconds <= 30:
        raise ValueError('终止清理宽限必须为0到30秒，不是处理总时限')
    roots = tuple(Path(p).resolve(strict=True) for p in disk_roots)
    if not roots:
        raise ValueError('必须明确监测输出和catalog所在磁盘')
    root = Path(output)
    root.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    receipt = dict(started_at=datetime.now(timezone.utc).isoformat(), state='failed',
        threads=threads, parent_sample_peak_rss_bytes=None, children_sample_peak_rss_bytes=None,
        process_tree_sample_peak_rss_bytes=None, sample_count=0,
        sampling_failures=0, rss_coverage='unavailable', last_sample_error=None,
        disk_sampling_failures=0, last_disk_sample_error=None,
        rss_scope='启动进程与可见后代ps瞬时RSS采样峰值；不含PG；短命子进程可能未采样',
        physical_input_bytes=None, physical_output_bytes=None, processed_count=None,
        processed_unit=None, admit_seconds=None, query_seconds=None,
        limits=dict(max_rss_bytes=max_rss_bytes, min_free_bytes=min_free_bytes), disk_roots=list(map(str, roots)))
    process = None
    primary = None
    try:
        def guard(*, running=False):
            free = {}
            errors = {}
            for path in roots:
                try:
                    value = disk_usage(path).free
                    if type(value) is not int or value < 0:
                        raise ValueError('磁盘空闲量样本非法')
                    free[str(path)] = value
                except Exception as exc:
                    free[str(path)] = None
                    errors[str(path)] = dict(type=type(exc).__name__, message=str(exc)[:1024])
            receipt['last_disk_free_bytes'] = free
            if any(v is not None and v < min_free_bytes for v in free.values()):
                raise RuntimeError('磁盘保护触发')
            if errors:
                receipt['disk_sampling_failures'] += 1
                receipt['last_disk_sample_error'] = errors
                if not running:
                    raise RuntimeError('启动前磁盘资格无法核对；未启动处理进程')
        guard()
        env = dict(os.environ, DOMEYE_DUCKDB_THREADS=str(threads), PYTHONDONTWRITEBYTECODE='1')
        env.pop('PYTHONPATH', None)
        with (root/'stdout.log').open('xb') as out, (root/'stderr.log').open('xb') as err:
            process = launch(argv, stdout=out, stderr=err, env=env, start_new_session=True)
            receipt['pid'] = process.pid
            while process.poll() is None:
                try:
                    values = sample(process.pid)
                    parent, children = values['parent_rss_bytes'], values['children_rss_bytes']
                    if any(type(v) is not int or v < 0 for v in (parent, children)):
                        raise ValueError('本次RSS样本缺失或非法')
                except Exception as exc:
                    # 采样工具故障不等于被观察的数据进程失败；不重启、不另建run。
                    receipt['sampling_failures'] += 1
                    receipt['last_sample_error'] = dict(type=type(exc).__name__, message=str(exc)[:1024])
                    receipt['rss_coverage'] = 'partial' if receipt['sample_count'] else 'unavailable'
                else:
                    receipt['sample_count'] += 1
                    receipt['rss_coverage'] = 'partial' if receipt['sampling_failures'] else 'sampled'
                    for field, value in (('parent_sample_peak_rss_bytes', parent),
                                         ('children_sample_peak_rss_bytes', children),
                                         ('process_tree_sample_peak_rss_bytes', parent+children)):
                        receipt[field] = max(receipt[field] or 0, value)
                    if parent+children > max_rss_bytes:
                        raise RuntimeError('进程树RSS保护触发')
                guard(running=True)
                time.sleep(0.1)
            receipt['exit_code'] = process.wait()
            if receipt['exit_code'] != 0:
                raise RuntimeError('原阶段入口失败；请查原stderr')
        receipt['state'] = 'process_exited_zero'
    except BaseException as exc:
        primary = exc
        receipt['error_type'] = type(exc).__name__
        receipt['error'] = str(exc)
        if process is not None:
            try:
                receipt['cleanup'] = stop_process_group(process, termination_grace_seconds)
                receipt['exit_code'] = receipt['cleanup']['exit_code']
            except Exception as cleanup_error:
                receipt.setdefault('cleanup_errors', []).append(dict(type=type(cleanup_error).__name__, message=str(cleanup_error)))
                exc.migration_cleanup_errors = receipt['cleanup_errors']
        raise
    finally:
        receipt['ended_at'] = datetime.now(timezone.utc).isoformat()
        receipt['wall_seconds'] = time.monotonic()-started
        save_final(root/'阶段观测.json', receipt, primary)
    return receipt
