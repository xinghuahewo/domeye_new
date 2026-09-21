"""仅测试：本次显式注入的PG日志，以随机服务端标记验证来源与采样范围。"""
from pathlib import Path
import os
import time
import uuid
import psycopg2
import pytest


class PgLogProbe:
    def __init__(self,dsn,path):
        self.dsn=dsn;self.path=Path(path)
        if not self.path.is_file():raise ValueError('measurement_log_missing')
        # 必须证明这份显式日志正在记录本次DSN，不用空文件表示0 SQL。
        offset=self.path.stat().st_size;token=self.marker()
        self.wait_marker(offset,token)

    def marker(self):
        token='country_c4_measure_'+uuid.uuid4().hex
        with psycopg2.connect(self.dsn) as pg,pg.cursor() as cur:cur.execute('SELECT %s',(token,))
        return token

    def wait_marker(self,offset,token):
        deadline=time.monotonic()+3
        while True:
            if self.path.stat().st_size<offset:raise ValueError('measurement_log_rotated')
            with self.path.open() as f:f.seek(offset);text=f.read()
            if token in text:return text
            if time.monotonic()>=deadline:raise ValueError('measurement_log_not_bound_or_not_recording')
            time.sleep(.025)

    def start(self):
        self.wait_marker(self.path.stat().st_size,self.marker())
        # start标记事务已经结束；从当前完整日志末尾记录业务调用。
        return self.path.stat().st_size

    def finish(self,offset):
        token=self.marker();text=self.wait_marker(offset,token)
        lines=text.splitlines(keepends=True)
        # 剔除测量尾标记连接的BEGIN/SELECT/COMMIT，不把探针算入业务成本。
        marker_index=next(i for i,line in enumerate(lines) if token in line)
        begin=marker_index
        while begin>0 and 'statement: BEGIN' not in lines[begin]:begin-=1
        snippet=''.join(lines[:begin])
        statements=sum('statement:' in line or 'execute ' in line for line in snippet.splitlines())
        if statements<1:raise ValueError('measurement_no_statements')
        return snippet,statements


@pytest.fixture
def country_pg_log():
    path=os.environ.get('DOMEYE_COUNTRY_TEST_PG_LOG')
    if not path:pytest.skip('未显式绑定本次私有PG日志，仅跳过SQL测量')
    dsn=os.environ.get('DOMEYE_COUNTRY_C1_TEST_DSN')
    if not dsn:pytest.skip('未绑定本次私有PG，仅跳过SQL测量')
    return PgLogProbe(dsn,path)
