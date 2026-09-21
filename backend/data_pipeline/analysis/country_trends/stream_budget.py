"""S3落盘行流计量；不改变S1/S2，未有界的表示仍明确拒绝。"""
from pathlib import Path
import sqlite3
from data_pipeline.analysis.country_trends.snapshot_store import SharedBudget
from data_pipeline.analysis.country_trends.compute import rss
from data_pipeline.common import process_resources as resources


class StreamBudget(SharedBudget):
    def __init__(self, limits, root, guard):
        super().__init__(limits, root, guard)
        resources.add_root(root)
        self.monitor = resources.Monitor((root,), max_rss=limits.max_rss_bytes,
            max_disk=limits.max_disk_bytes, rss=lambda:rss())

    def finish(self):
        self.guard()
        self.monitor.check(force=True, reconcile=True)
        for key, value in self.monitor.stats.items(): self.stats[key] = value

    def charge(self, payload):
        if len(payload) > self.limits.max_row_bytes:
            raise ValueError('trend_row_budget')
        self.rows += 1
        self.byte_count += len(payload)
        self.check()

    def refs(self, count):
        self.references += count
        self.check()

    def step(self):
        self.index_steps += 1
        self.check()

    def check(self):
        self.guard()
        for root in self.scratch_roots:
            if self.monitor.bucket(Path(root).resolve()) is None: self.monitor.add_root(root)
        self.monitor.check()
        for key, value in self.monitor.stats.items(): self.stats[key] = value


class _Cursor(sqlite3.Cursor):
    def execute(self, sql, parameters=()):
        return self.connection.call(super().execute, sql, parameters)
    def executemany(self, sql, parameters):
        raise ValueError('trend_s3_sql_use_bounded_execute')
    def fetchone(self): return self.connection.call(super().fetchone)
    def fetchmany(self, size=1): return self.connection.call(super().fetchmany, size)
    def fetchall(self): return self.connection.call(super().fetchall)
    def __next__(self): return self.connection.call(super().__next__)


class _Connection(sqlite3.Connection):
    budget = None
    callback_active = False
    def call(self, operation, *args):
        if self.callback_active:
            raise ValueError('trend_s3_recursive_sql_guard')
        if self.budget is not None and getattr(self.budget, 'sql_error', None) is not None:
            raise self.budget.sql_error
        if self.budget is not None: self.budget.check()
        try: return operation(*args)
        except sqlite3.Error:
            if self.budget is not None and getattr(self.budget, 'sql_error', None) is not None:
                raise self.budget.sql_error
            raise
    def commit(self): return self.call(super().commit)
    def cursor(self, *args, **kwargs): return super().cursor(factory=_Cursor)
    def execute(self, sql, parameters=()): return self.cursor().execute(sql, parameters)
    def executemany(self, *args): raise ValueError('trend_s3_sql_use_bounded_execute')
    def executescript(self, sql):
        # 当前S3初始化仅含无触发器DDL；不接受任意脚本或后置外部排序。
        for statement in sql.split(';'):
            if statement.strip(): self.execute(statement)
    def close(self):
        try: return super().close()
        finally:
            for path in getattr(self, 'tracked_paths', ()):
                resources.freeze(path)


def install_progress(db, budget):
    """不重置阶段VM计数；无递归SQL的资源/取消检查保留首异常。"""
    if not isinstance(db, _Connection):
        raise ValueError('trend_s3_requires_guarded_sqlite')
    db.budget = budget
    def progress():
        budget.stats['sqlite_steps'] += 1000
        db.callback_active = True
        try:
            budget.check()
            return 0
        except BaseException as error:
            if getattr(budget, 'sql_error', None) is None: budget.sql_error = error
            return 1
        finally: db.callback_active = False
    db.set_progress_handler(progress, 1000)


def connect(path, budget, *, uri=False):
    if not isinstance(budget, StreamBudget):
        raise ValueError('trend_s3_requires_stream_budget')
    temporary = resources.sqlite_temp()
    if budget.monitor.bucket(temporary) is None: budget.monitor.add_root(temporary)
    if uri:
        from urllib.parse import urlparse, unquote
        actual = Path(unquote(urlparse(str(path)).path))
    else: actual = Path(path)
    budget.check()
    tracked = () if uri else tuple(Path(str(actual)+suffix) for suffix in ('','-journal','-wal','-shm'))
    for item in tracked: resources.track(item)
    db = sqlite3.connect(path, uri=uri, factory=_Connection)
    db.tracked_paths = tracked
    try:
        db.execute('PRAGMA cache_size=-2048')
        db.execute('PRAGMA temp_store=FILE')
        db.execute('PRAGMA threads=0')
        # 自有temp目录与实际FD占用已绑定，允许原合法排序/索引。
        install_progress(db, budget)
        return db
    except BaseException:
        db.close(); raise


def code_binding():
    from data_pipeline.analysis.country_trends.snapshot_store import code_binding as original
    from data_pipeline.results.manifest_io import file_hash
    result = original()
    result['backend/data_pipeline/common/process_resources.py'] = file_hash(resources.__file__)
    return result
