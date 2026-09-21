"""S3资源接线定向测试；须由首次SQLite导入前绑定temp的新解释器运行。"""
from pathlib import Path
from dataclasses import replace
import os
import fcntl
import pytest
from data_pipeline.common import process_resources as resources
from data_pipeline.analysis.country_trends.stream_budget import StreamBudget, connect
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits


def test_sampling_not_linear_in_calls_and_closed_files_do_not_accumulate(tmp_path):
    clock=[0.0];calls=[]
    m=resources.Monitor([tmp_path],max_rss=10**9,clock=lambda:clock[0],fds=lambda:[],rss=lambda:1)
    for _ in range(10000):m.check()
    assert m.stats['directory_scans']==1 and m.stats['samples']==1
    path=tmp_path/'own';resources.track(path);path.write_bytes(b'x'*10000);resources.freeze(path)
    clock[0]=1;m.check();large=m.stats['disk_current_bytes']
    resources.track(path);path.write_bytes(b'x');resources.freeze(path)
    clock[0]=2;m.check()
    assert m.stats['disk_current_bytes']<large
    m.check(force=True,reconcile=True)
    assert m.stats['directory_scans']==2


def test_unlinked_fd_actual_occupancy_deduplicates_and_disappears_on_close(tmp_path):
    m=resources.Monitor([tmp_path],max_rss=10**9,temp_roots=[tmp_path],max_temp=10**7)
    path=tmp_path/'owned';resources.track(path)
    stream=path.open('w+b');stream.write(b'x'*8192);stream.flush();duplicate=fcntl.fcntl(stream.fileno(),fcntl.F_DUPFD,300)
    path.unlink()
    try:
        actual=os.fstat(duplicate).st_blocks*512
        m.check(force=True)
        assert m.stats['unlinked_current_bytes']==actual>0
        stream.close()
        m.check(force=True)
        assert duplicate>=300 and m.stats['unlinked_current_bytes']==actual
        limited=resources.Monitor([tmp_path],max_rss=10**9,temp_roots=[tmp_path],max_temp=1)
        with pytest.raises(ValueError,match='trend_runtime_temp_disk'):
            limited.check(force=True)
    finally:os.close(duplicate);stream.close();resources.freeze(path)
    m.check(force=True,reconcile=True)
    assert m.stats['disk_current_bytes']==m.stats['unlinked_current_bytes']==0


@pytest.mark.parametrize('mode',['cancel','disk','rss','missing'])
def test_sql_first_failure_preserved_and_only_owned_cleanup(tmp_path,mode):
    class Cancelled(BaseException):pass
    missing=RuntimeError('fixture missing FD sample');cancel=Cancelled('fixture cancelled')
    limits=replace(S2Limits(),max_disk_bytes=1024**2)
    state={'db':None,'file':None}
    def guard():
        db=state['db']
        if db is not None and db.callback_active:
            if mode=='cancel':raise cancel
            if mode=='disk' and state['file'] is None:
                path=tmp_path/'unlinked';f=path.open('w+b');f.write(b'x'*(2*1024**2));f.flush();path.unlink();state['file']=f
    b=StreamBudget(limits,tmp_path,guard);db=connect(tmp_path/'db.sqlite',b);state['db']=db
    db.execute('CREATE TABLE t(x)')
    clock=[0.0]
    def tick():clock[0]+=1;return clock[0]
    b.monitor.clock=tick; b.monitor.last=None
    original=b.monitor.fds
    def fds():
        if mode=='missing' and db.callback_active:raise missing
        return original()
    b.monitor.fds=fds
    if mode=='rss':b.monitor.rss=lambda:limits.max_rss_bytes+1 if db.callback_active else 1
    marker=tmp_path.parent/('unrelated-'+tmp_path.name);marker.write_text('keep')
    try:
        with pytest.raises(BaseException) as caught:
            db.execute('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<3000) SELECT sum(x) FROM n').fetchone()
        assert caught.value is b.sql_error
        if mode=='cancel':assert caught.value is cancel
        elif mode=='missing':assert caught.value is missing
        else:assert str(caught.value)==('trend_total_disk' if mode=='disk' else 'trend_rss_limit')
    finally:
        if state['file'] is not None:state['file'].close()
        db.close()
    assert marker.read_text()=='keep'
    assert not any(p.parent==tmp_path and st.st_nlink==0 for p,st,mode in resources.fd_sample())
    marker.unlink()


def test_legal_sort_index_and_output_are_unchanged(tmp_path):
    b=StreamBudget(S2Limits(),tmp_path,lambda:None);db=connect(tmp_path/'db.sqlite',b)
    try:
        db.execute('CREATE TABLE t(x INTEGER)')
        for i in range(300):db.execute('INSERT INTO t VALUES (?)',(300-i,))
        assert list(db.execute('SELECT x FROM t ORDER BY x'))==[(i,) for i in range(1,301)]
        db.execute('CREATE INDEX ix ON t(x)')
    finally:db.close()
    b.finish()
    assert b.stats['directory_scans']<=4


def test_runtime_sampling_and_external_guard_remain_separate(tmp_path):
    from data_pipeline.analysis.country_trends.runtime import ProductionRuntime
    out=tmp_path/'out';scratch=tmp_path/'scratch';socket=tmp_path/'socket'
    for path in (out,scratch,socket):path.mkdir()
    rt=ProductionRuntime(dsn='host='+str(socket)+' dbname=fixture',output_root=out,
        allowed_roots=(tmp_path,resources.sqlite_temp()),scratch_root=scratch,
        dependency_admissions=(),dependency_runtimes={},country_window_us=(0,1),fixture_only=True)
    monitor=rt._resource_monitor;scans=monitor.stats['directory_scans'];calls=[]
    def external():calls.append(1)
    checked=rt.guarded(external)
    assert rt.guarded(checked) is checked
    for _ in range(1000):checked()
    assert len(calls)==1000 and monitor.stats['directory_scans']==scans
    class Cancelled(BaseException):pass
    failure=Cancelled('immediate')
    def cancel():raise failure
    with pytest.raises(Cancelled) as result:rt.guarded(cancel)()
    assert result.value is failure
    monitor.check(force=True,reconcile=True)


def test_unprepared_entry_refuses_before_candidate_creation(tmp_path,monkeypatch):
    from data_pipeline.analysis.country_trends.stream_files import write_body
    monkeypatch.setattr(resources,'_binding',None)
    with pytest.raises(ValueError,match='requires_fresh_process'):
        write_body(iter(()),tmp_path/'not-created')
    assert not (tmp_path/'not-created').exists()
    with pytest.raises(ValueError,match='requires_fresh_process'):
        resources.bind_sqlite_temp(tmp_path)


def test_driver_binds_before_source_loading_without_running_producers(tmp_path,monkeypatch):
    import runpy,json,sys
    script=Path(__file__).resolve().parents[3]/'scripts/pipeline/migration-day-run.py'
    module=runpy.run_path(str(script),run_name='fixture_driver_module')
    path=tmp_path/'config.json'
    path.write_text(json.dumps(dict(enabled=True,invocation_root=str(tmp_path/'new'),
        tail={'trend':{'runtime':{'allowed_roots':[str(tmp_path)]}}},manifest='fixture',mapping='fixture')))
    calls=[]
    monkeypatch.setattr(resources,'bind_sqlite_temp',lambda parent:calls.append(parent))
    class StopBeforeInputs(Exception):pass
    def stop(*args):
        assert calls==[tmp_path]
        raise StopBeforeInputs()
    monkeypatch.setattr(module['FixedInputs'],'load',stop)
    monkeypatch.setattr(sys,'argv',[str(script),'run-full','--config',str(path)])
    with pytest.raises(StopBeforeInputs):module['main']()
    assert not (tmp_path/'new').exists()


def test_failed_temp_removal_does_not_drop_live_accounting(tmp_path):
    nested=tmp_path/'owned-temp';nested.mkdir()
    m=resources.Monitor([tmp_path],max_rss=10**9,fds=lambda:[],rss=lambda:1)
    resources.add_root(nested)
    file=nested/'data';resources.track(file);file.write_bytes(b'x'*4096);resources.freeze(file)
    with pytest.raises(ValueError,match='cleanup_incomplete'):resources.remove_root(nested)
    m.check(force=True);assert m.stats['disk_current_bytes']>=4096
    file.unlink();nested.rmdir();resources.remove_root(nested)
    m.check(force=True,reconcile=True);assert m.stats['disk_current_bytes']==0


def test_fixed_scope_does_not_reencode_and_rejects_nested_typed_drift(tmp_path,monkeypatch):
    from data_pipeline.analysis.country_trends import runtime as runtime
    out=tmp_path/'out';scratch=tmp_path/'scratch';socket=tmp_path/'socket'
    for path in (out,scratch,socket):path.mkdir()
    data={'owner_binding':'fixture-large-value:'+'x'*100000,'nested':{'number':1,'items':[1,2]}}
    rt=runtime.ProductionRuntime(dsn='host='+str(socket)+' dbname=fixture',output_root=out,
        allowed_roots=(tmp_path,resources.sqlite_temp()),scratch_root=scratch,
        dependency_admissions=(data,),dependency_runtimes={},country_window_us=(0,1),fixture_only=True)
    def no_encoding(*args):pytest.fail('repeated whole binding encode')
    monkeypatch.setattr(runtime,'encode',no_encoding)
    for _ in range(100):rt.resource_guard()
    for changed in (True,1.0,'1'):
        data['nested']['number']=changed
        with pytest.raises(ValueError,match='scope_drift'):rt.resource_guard()
    data['nested']['number']=1
    data['nested']['items']=(1,2)
    with pytest.raises(ValueError,match='scope_drift'):rt.resource_guard()
    data['nested']['items']=[1,2]
    rt.resource_guard()
    rt.dependency_admissions=list(rt.dependency_admissions)
    with pytest.raises(ValueError,match='scope_drift'):rt.resource_guard()


def test_bound_duckdb_spill_current_occupancy_limits_and_release(tmp_path):
    scope=tmp_path/'duckdb';scope.mkdir()
    monitor=resources.Monitor([tmp_path],max_rss=10**12,temp_roots=[tmp_path])
    resources.bind_duckdb_temp(scope)
    # 后创建的预算也继承显式绑定；关闭但未删除的spill不能从账本消失。
    later=resources.Monitor([tmp_path],max_rss=10**12,temp_roots=[tmp_path])
    path=scope/'duckdb_temp_storage-0.tmp'
    with path.open('w+b') as stream:
        stream.write(b'x'*8192);stream.flush()
        amount=max(path.stat().st_size,path.stat().st_blocks*512)
        for current in (monitor,later):
            current.check(force=True,reconcile=True)
            assert current.stats['temp_current_bytes']==amount
        for limits,error in (({'max_temp':1},'trend_runtime_temp_disk'),
                             ({'max_disk':1},'trend_total_disk')):
            limited=resources.Monitor([tmp_path],max_rss=10**12,temp_roots=[tmp_path],**limits)
            with pytest.raises(ValueError,match=error):limited.check(force=True)
    monitor.check(force=True,reconcile=True)
    assert monitor.stats['disk_current_bytes']==amount
    path.write_bytes(b'x')
    monitor.check(force=True,reconcile=True)
    assert monitor.stats['disk_current_bytes']==max(path.stat().st_size,path.stat().st_blocks*512)<amount
    # 在两次采样之间完成打开和关闭的合法文件也必须计入。
    closed=scope/'closed.tmp';closed.write_bytes(b'x'*16384)
    monitor.check(force=True,reconcile=True)
    assert monitor.stats['disk_current_bytes']>=16385
    path.unlink();closed.unlink()
    monitor.check(force=True,reconcile=True)
    assert monitor.stats['disk_current_bytes']==monitor.stats['temp_current_bytes']==0
    with (tmp_path/'unknown').open('wb') as stream:
        stream.write(b'x');stream.flush()
        with pytest.raises(ValueError,match='trend_s3_untracked_writer'):monitor.check(force=True)


def test_lake_binds_only_its_dedicated_spill_scope(tmp_path,monkeypatch):
    from data_pipeline.analysis.country_trends import runtime as s3_runtime
    out=tmp_path/'out';scratch=tmp_path/'scratch';socket=tmp_path/'socket'
    for path in (out,scratch,socket):path.mkdir()
    rt=s3_runtime.ProductionRuntime(dsn='host='+str(socket)+' dbname=fixture',output_root=out,
        allowed_roots=(tmp_path,resources.sqlite_temp()),scratch_root=scratch,
        dependency_admissions=(),dependency_runtimes={},country_window_us=(0,1),fixture_only=True)
    calls=[]
    class Lake:
        def execute(self,sql):calls.append(sql)
        def close(self):pass
    def connect(*args,**kwargs):
        scope=kwargs['scratch_root']
        assert scope.parent==scratch and scope!=scratch
        assert scope in rt._resource_monitor.duckdb_scopes
        calls.append(scope)
        return Lake()
    monkeypatch.setattr(s3_runtime,'lake_connect',connect)
    lake=rt.lake();lake.close()
    assert str(calls[0]) in calls[1]
    rt._resource_monitor.check(force=True,reconcile=True)
