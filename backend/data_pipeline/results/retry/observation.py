"""重试阶段与 view 进度；复用原 RSS 采样，不授予资格或恢复能力。"""
from contextlib import contextmanager
from datetime import datetime,timezone
from pathlib import Path
import json
import os
import time
from data_pipeline.jobs.downstream import measured
from data_pipeline.jobs.stage_runner import save_new

POLICY=dict(contract='publication-retry-observation/v1',stage_rss_sample_seconds=0.1,
            batch_progress_flush_seconds=1.0,physical_io='Unknown',progress_only=True)


@contextmanager
def stage(root,name,unit,guard=lambda:None):
    metrics=dict(stage=name,processed_unit=unit,state='failed',wall_seconds=None,
        processed_count=None,rss_coverage='unavailable',stage_process_group_peak_rss_bytes=None)
    def annotate(error):
        metrics.update(state='failed',error=dict(type=type(error).__name__,message=str(error)[:1024]))
        if not hasattr(error,'retry_stage'):
            error.retry_stage=name
            error.retry_observation=metrics
    def checked_guard():
        try:guard()
        except BaseException as error:
            annotate(error);raise
    try:
        directory=Path(root)/'stages'/name if root is not None else None
        with measured(directory,checked_guard,sample_first=True,observation=metrics):
            try:
                if directory is not None:
                    save_new(directory/'阶段开始.json',dict(stage=name,state='started',started_at=metrics['started_at'],processed_unit=unit))
                yield metrics
            except BaseException as error:
                annotate(error);raise
    except BaseException as error:
        annotate(error);raise


class ViewProgress:
    """每秒至多写一次批进度；view 边界及失败立即落盘，不保存 AD/正文。"""
    def __init__(self,root,metrics=None):
        self.root=Path(root);self.metrics=metrics;self.latest={};self.last_write=None;self.errors=[]

    def __call__(self,event):
        self.latest=dict(event,at=datetime.now(timezone.utc).isoformat(),progress_only=True,
            rows_unit='原owner已交付并通过批封装检查的行；未必完成消费，不是已准入行',
            bytes_unit='原owner批完整UTF-8编码字节；不是物理IO',
            physical_input_bytes=None,physical_output_bytes=None,
            completion_scope='仅组件读取及组合尾核验；不是ready/publish证明或恢复点',
            failure_position_scope='当前view与已交付批计量；owner未返回的行位置Unknown')
        if self.metrics is not None:
            self.metrics.update(processed_count=event['observed_rows'],encoded_input_bytes=event['observed_bytes'],
                completed_views=event['completed_views'],last_view=dict(owner=event['owner'],view=event['view']))
        now=time.monotonic()
        if event['state']=='reading' and self.last_write is not None and now-self.last_write<1:return
        try:
            self.latest['observation_errors']=list(self.errors)
            temporary=self.root/'读取进度.pending'
            with temporary.open('w') as handle:
                json.dump(self.latest,handle,ensure_ascii=False,allow_nan=False);handle.flush();os.fsync(handle.fileno())
            temporary.replace(self.root/'读取进度.json')
            if event['state']!='reading':
                with (self.root/'view边界.jsonl').open('a') as handle:
                    handle.write(json.dumps(self.latest,ensure_ascii=False,allow_nan=False)+'\n');handle.flush();os.fsync(handle.fileno())
            self.last_write=now
        except Exception as error:
            detail=dict(type=type(error).__name__,message=str(error)[:512])
            # 有界错误摘要；观测失败不覆盖原处理异常，阶段终态另记录次数。
            self.errors[:]=[dict(detail,count=(self.errors[-1]['count'] if self.errors else 0)+1)]
            self.latest['observation_errors']=list(self.errors)
        if self.metrics is not None:self.metrics['progress_observation_errors']=list(self.errors)
