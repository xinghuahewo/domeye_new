"""独立消费 pipeline 的原子完成回执；数据库故障不反馈给计算进程。"""
from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2

from data_pipeline.results.delivery import initialize, import_file


def run_binding(run):
    run = Path(run).resolve()
    content = (run / 'manifest.json').read_bytes()
    manifest = json.loads(content)
    return manifest, {
        'schema_version': 'completed-file-delivery/v1', 'source_run': str(run),
        'manifest_sha256': hashlib.sha256(content).hexdigest(), 'collector': manifest['collector'],
        # 保留初次交付身份；是否接入常驻系统由运行绑定记录，不改写来源身份。
        'usage': 'isolated_flow_validation', 'original_batch_status': 'preserved_in_source_run',
        'observation_archive': 'paused_by_user',
    }


def source_window(source):
    match = re.search(r'(?:bview|updates)\.(\d{8})\.(\d{4})\.', source['path'])
    if not match or source['role'] not in ('baseline', 'snapshot', 'update'):
        raise ValueError('输入缺少支持的 RIB／UPDATE 时间定位')
    start = datetime.strptime(''.join(match.groups()), '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
    return start, start + (timedelta(minutes=5) if source['role'] == 'update' else timedelta())


class DeliveryWorker:
    def __init__(self, run, dsn_file, progress, *, files=None, receipt_dir=None):
        self.run = Path(run).resolve()
        self.manifest, self.binding = run_binding(self.run)
        self.limit = len(self.manifest['inputs']) if files is None else files
        if not 1 <= self.limit <= len(self.manifest['inputs']):
            raise ValueError('文件数量超出原输入')
        self.receipts = Path(receipt_dir) if receipt_dir else self.run / 'run/results'
        self.dsn_file, self.progress = Path(dsn_file), Path(progress)
        self.conn = None
        self.last_state = None
        self.partial_receipt = None

    def report(self, state, **fields):
        value = {'schema_version': 'completed-file-delivery-progress/v1', 'state': state,
                 'source_run': str(self.run), 'receipt_directory': str(self.receipts),
                 'files_limit': self.limit, 'checked_at': datetime.now(timezone.utc).isoformat(), **fields}
        self.progress.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.progress.with_suffix('.tmp')
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
        temporary.replace(self.progress)
        identity = (state, fields.get('next_ordinal'), fields.get('error_type'))
        if identity != self.last_state:
            print(json.dumps(value, ensure_ascii=False), flush=True)
            self.last_state = identity
        return value

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def step(self):
        if hashlib.sha256((self.run / 'manifest.json').read_bytes()).hexdigest() != self.binding['manifest_sha256']:
            raise ValueError('来源清单发生变化，须显式重新绑定')
        if self.conn is None:
            self.conn = psycopg2.connect(self.dsn_file.read_text().strip(), connect_timeout=10)
            initialize(self.conn, self.binding)
        with self.conn, self.conn.cursor() as cur:
            cur.execute('SELECT count(*),coalesce(max(ordinal),-1)+1 FROM result_delivery.files')
            count, ordinal = cur.fetchone()
        if count != ordinal or count > self.limit:
            raise ValueError('交付游标不连续或超出配置范围')
        if ordinal == self.limit:
            return self.report('complete', files=count, next_ordinal=ordinal)
        path = self.receipts / f'file-{ordinal:04d}.json'
        if not path.exists():
            end = self.run / 'execution-end.json'
            source_status = json.loads(end.read_text()).get('status', 'unknown') if end.exists() else 'running_or_unknown'
            return self.report('waiting_source', files=count, next_ordinal=ordinal, source_status=source_status)
        # 新计算端原子发布；旧计算端直接写 JSON，兼容其短暂的未写完状态。
        try:
            receipt = json.loads(path.read_bytes())
        except json.JSONDecodeError as error:
            stat = path.stat()
            identity = (ordinal, stat.st_size, stat.st_mtime_ns)
            now = time.monotonic()
            if self.partial_receipt is None or self.partial_receipt[0] != identity:
                self.partial_receipt = (identity, now)
            if now - self.partial_receipt[1] >= 30:
                raise ValueError('完成回执持续不完整，保留已交付数据，等待修复') from error
            return self.report('waiting_receipt', files=count, next_ordinal=ordinal)
        self.partial_receipt = None
        if receipt.get('ordinal') != ordinal:
            raise ValueError('完成回执序号与预期文件不一致')
        source = self.manifest['inputs'][ordinal]
        start, end = source_window(source)
        self.report('delivering', files=count, next_ordinal=ordinal)
        started = time.monotonic()
        result = import_file(self.conn, path, source, window_start=start, window_end=end)
        return self.report('delivered', files=ordinal+1, next_ordinal=ordinal+1,
                           delivery_status=result['status'], seconds=round(time.monotonic()-started, 3))

    def follow(self, stop, *, poll_seconds=5):
        failures = 0
        try:
            while not stop.is_set():
                try:
                    result = self.step()
                    failures = 0
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as error:
                    self.close()
                    failures += 1
                    delay = min(60, poll_seconds * 2**min(failures-1, 5))
                    self.report('retrying_database', error_type=type(error).__name__, retry_seconds=delay)
                    stop.wait(delay)
                    continue
                except Exception as error:
                    self.report('blocked', error_type=type(error).__name__, message=str(error))
                    return 2  # 未知／无效结果不跳过；已交付数据继续可读。
                if result['state'] == 'complete':
                    return 0
                if result['state'] != 'delivered':
                    stop.wait(poll_seconds)
            self.report('stopped')
            return 0
        finally:
            self.close()
