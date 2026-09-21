"""显式自有PG小表锁实验；不是owner AD/current/Publication ready科学验收。"""
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
from types import SimpleNamespace

import psycopg2
from psycopg2 import sql, errors
import pytest

from data_pipeline.common.admission_locks import LockConnections, validated_connection
from data_pipeline.results.component_readers import lock_key

ROOT=Path('/private/tmp/domeye-locks-c533-9ebf')
BIN=Path('/opt/homebrew/opt/postgresql@14/bin')
PORT=29531
ENABLED=os.environ.get('DOMEYE_LOCK_PG_ROOT')
pytestmark=pytest.mark.skipif(ENABLED is None,reason='必须显式启用自有PG锁实验')


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def test_real_lock_transactions():
    assert ENABLED==str(ROOT), '仅允许这次授权的新私有目录'
    assert not ROOT.exists() and not ROOT.is_symlink(), '不复用任何已有PG或结果目录'
    started=time.monotonic();connections=[];server_started=False;primary=None
    report=dict(boundary='真实helper和FOR SHARE小表，不是科学AD/current/ready',
                phases=[],connections=[],faults=[],assertions={},port=PORT)
    project=Path(__file__).resolve().parents[3]
    report['revision']=subprocess.check_output(['git','rev-parse','HEAD'],cwd=project,text=True).strip()
    report['code_sha256']={name:sha(project/name) for name in (
        'backend/data_pipeline/common/admission_locks.py',
        'backend/data_pipeline/bgp/replay/snapshot_admission.py',
        'backend/data_pipeline/analysis/country_events/result_admission.py',
        'backend/data_pipeline/analysis/detection/publication.py')}
    report['test_sha256']=sha(__file__)
    ROOT.mkdir(mode=0o700);data=ROOT/'pgdata';socket=ROOT/'socket';socket.mkdir(mode=0o700)
    artifacts=ROOT/'artifacts';artifacts.mkdir(mode=0o700)
    def command(args,name):
        with (artifacts/(name+'.stdout')).open('wb') as out,(artifacts/(name+'.stderr')).open('wb') as err:
            subprocess.run([str(x) for x in args],stdout=out,stderr=err,check=True)
    def dsn(database,app):return psycopg2.extensions.make_dsn(host=str(socket),port=PORT,dbname=database,application_name=app)
    def connect(database,app):
        pg=psycopg2.connect(dsn(database,app));connections.append(pg);return pg
    def query(pg,text,args=None):
        with pg.cursor() as cur:cur.execute(text,args);return cur.fetchall() if cur.description else None
    def rss():return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
    def guard():
        import shutil
        assert rss()<512*1024**2, 'Python进程峰值RSS保护'
        assert shutil.disk_usage(ROOT).free>=256*1024**2, '磁盘余量保护'
    try:
        command([BIN/'postgres','--version'],'version')
        command([BIN/'initdb','-D',data,'--encoding=UTF8','--no-locale','--auth-local=trust','--auth-host=reject'],'initdb')
        config={
            'port':str(PORT),'listen_addresses':"''",'unix_socket_directories':"'"+str(socket)+"'",'unix_socket_permissions':'0700',
            'max_connections':'16','shared_buffers':"'32MB'",'work_mem':"'1MB'",'maintenance_work_mem':"'16MB'",
            'max_worker_processes':'0','max_parallel_workers':'0','autovacuum':'off','temp_file_limit':"'64MB'",
            'statement_timeout':"'5s'",'lock_timeout':"'100ms'",'idle_in_transaction_session_timeout':'0',
            'fsync':'on','full_page_writes':'on','synchronous_commit':'on'}
        with (data/'postgresql.conf').open('a') as f:
            f.write('\n# 本次隔离锁验证\n'+''.join(k+' = '+v+'\n' for k,v in config.items()))
        command([BIN/'pg_ctl','-D',data,'-l',artifacts/'postgres.log','-W','start'],'start')
        server_started=True
        while True:
            guard()
            result=subprocess.run([str(BIN/'pg_isready'),'-h',str(socket),'-p',str(PORT)],capture_output=True)
            if result.returncode==0:break
            if (data/'postmaster.pid').exists():
                os.kill(int((data/'postmaster.pid').read_text().splitlines()[0]),0)
            time.sleep(.1)
        report['postmaster_pid']=int((data/'postmaster.pid').read_text().splitlines()[0])
        observer=connect('postgres','lock-observer');observer.autocommit=True
        report['pg_version']=query(observer,'SELECT version()')[0][0]
        report['settings']=dict(query(observer,'SELECT name,setting FROM pg_settings WHERE name=ANY(%s)',(list(config),)))
        assert report['settings']['max_connections']=='16'
        for database in ('lock_a','lock_b'):
            query(observer,sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        writers={}
        for database,count in (('lock_a',601),('lock_b',3)):
            pg=connect(database,'lock-writer-'+database);writers[database]=pg
            query(pg,'CREATE TABLE lock_fixture (id integer PRIMARY KEY,value integer NOT NULL)')
            query(pg,'INSERT INTO lock_fixture SELECT i,0 FROM generate_series(1,%s) i',(count,));pg.commit()
        query(observer,'CREATE TABLE lock_control (id integer PRIMARY KEY,value integer NOT NULL)')
        query(observer,'INSERT INTO lock_control VALUES(1,0)')
        control=connect('postgres','lock-control')
        physical={db:query(pg,'SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')[0]
                  for db,pg in writers.items()}
        for pg in writers.values():pg.rollback()
        runtimes={db:SimpleNamespace(dsn=dsn(db,'lock-held-'+db)) for db in writers}
        targets=[]
        for database,count in (('lock_a',601),('lock_b',3)):
            for i in range(1,count+1):
                target=dict(stage=10*((i%3)+1),system_identifier=physical[database][0],database_oid=physical[database][1],
                            namespace='lock_fixture.row',key=str(i))
                targets.append((database,i,target))
        targets.sort(key=lambda item:lock_key(item[2]))
        report['target_count']=len(targets);report['target_order_sha256']=hashlib.sha256(json.dumps([lock_key(t)[:-1]+(t['key'],) for _,_,t in targets]).encode()).hexdigest()
        def carrier(t):
            # 仅helper入参的物理/目标载体；不含科学Admission合同或accepted资格。
            return dict(physical={k:t[k] for k in ('system_identifier','database_oid')},lock_targets=[t])
        def measure(phase):
            rows=query(observer,"SELECT application_name,count(*) FROM pg_stat_activity WHERE backend_type='client backend' GROUP BY application_name ORDER BY application_name")
            counts=dict(rows)
            report['connections'].append(dict(phase=phase,by_application=counts,total=sum(counts.values())))
            report['phases'].append(dict(name=phase,seconds=time.monotonic()-started,python_peak_rss_bytes=rss()))
            process_rows=subprocess.check_output(['ps','-axo','pid=,ppid=,rss='],text=True).splitlines()
            samples=[tuple(map(int,row.split())) for row in process_rows if len(row.split())==3]
            samples=[dict(pid=pid,ppid=ppid,rss_bytes=kb*1024) for pid,ppid,kb in samples
                     if pid==report['postmaster_pid'] or ppid==report['postmaster_pid']]
            report['phases'][-1]['pg_process_rss_samples']=samples
            report['phases'][-1]['pg_sampled_sum_rss_bytes']=sum(row['rss_bytes'] for row in samples)
            guard()
        def all_blocked(phase):
            count=0
            for database,i,_ in targets:
                pg=writers[database]
                with pg.cursor() as cur:
                    cur.execute('SAVEPOINT probe')
                    try:cur.execute('SELECT id FROM lock_fixture WHERE id=%s FOR UPDATE NOWAIT',(i,))
                    except errors.LockNotAvailable:count+=1
                    else:pytest.fail('锁scope内存在未受保护的原目标')
                    finally:cur.execute('ROLLBACK TO SAVEPOINT probe');cur.execute('RELEASE SAVEPOINT probe')
            for pg in writers.values():pg.rollback()
            assert count==604;report['assertions'][phase]=count;measure(phase)
        pids={};acquired=[]
        with LockConnections() as scope:
            for database,i,t in targets:
                lease=scope.borrow(runtimes[database],t,guard=guard)
                pg=validated_connection(runtimes[database],carrier(t),t,lease)
                with pg.cursor() as cur:
                    cur.execute('SELECT id FROM lock_fixture WHERE id=%s FOR SHARE',(i,));assert cur.fetchone()==(i,)
                    cur.execute('SELECT pg_backend_pid()');pid=cur.fetchone()[0]
                pids.setdefault(database,set()).add(pid);acquired.append(lock_key(t))
            assert acquired==sorted(acquired)
            assert all(len(value)==1 for value in pids.values()) and pids['lock_a']!=pids['lock_b']
            report['held_backend_pids']={k:sorted(v) for k,v in pids.items()}
            query(control,'UPDATE lock_control SET value=value+1 WHERE id=1')
            all_blocked('control_before_commit')
            control.commit()
            all_blocked('control_after_commit_scope_still_active')
            held=dict(report['connections'][-1]['by_application'])
            assert sum(n for k,n in held.items() if k.startswith('lock-held-'))==2
        count=0
        for database,i,_ in targets:
            rows=query(writers[database],'UPDATE lock_fixture SET value=value+1 WHERE id=%s RETURNING id',(i,))
            assert rows==[(i,)];count+=1
        for pg in writers.values():pg.commit()
        assert count==604;report['assertions']['after_scope_writable']=count
        measure('after_scope')
        assert not any(k.startswith('lock-held-') for k in report['connections'][-1]['by_application'])
        db,_,t=targets[0];rt=runtimes[db]
        with pytest.raises(ValueError):validated_connection(rt,carrier(t),t,lease)
        report['faults'].append('closed_scope_rejected')
        for fault in ('idle','new_transaction','autocommit','wrong_configuration','wrong_physical'):
            with LockConnections() as scope:
                lease=scope.borrow(rt,t,guard=guard);pg=lease.connection
                bad=rt;target=t
                if fault in ('idle','new_transaction','autocommit'):pg.rollback()
                if fault=='new_transaction':query(pg,'SELECT txid_current()')
                if fault=='autocommit':pg.autocommit=True
                if fault=='wrong_configuration':bad=SimpleNamespace(dsn=dsn(db,'different-application'))
                if fault=='wrong_physical':target={**t,'database_oid':0}
                with pytest.raises(ValueError):validated_connection(bad,carrier(target),target,lease)
            report['faults'].append(fault+'_rejected')
        expected=RuntimeError('本次真实终止连接后的原主异常')
        with pytest.raises(RuntimeError) as caught:
            with LockConnections() as scope:
                lease=scope.borrow(rt,t,guard=guard);pg=lease.connection
                pid=query(pg,'SELECT pg_backend_pid()')[0][0]
                assert query(observer,'SELECT pg_terminate_backend(%s)',(pid,))==[(True,)]
                with pytest.raises(psycopg2.Error):query(pg,'SELECT 1')
                raise expected
        assert caught.value is expected and expected.cleanup_errors
        report['fault_primary']=dict(type=type(expected).__name__,message=str(expected),cleanup=[dict(type=type(e).__name__,message=str(e)) for e in expected.cleanup_errors])
        measure('faults_finished')
        report['status']='passed'
    except BaseException as error:
        primary=error;report['status']='failed';report['primary']=dict(type=type(error).__name__,message=str(error));raise
    finally:
        cleanup=[]
        for pg in reversed(connections):
            try:pg.close()
            except BaseException as error:cleanup.append(str(error))
        if server_started:
            try:
                command([BIN/'pg_ctl','-D',data,'-m','smart','-W','stop'],'stop')
                while (data/'postmaster.pid').exists():time.sleep(.1)
                assert not (socket/('.s.PGSQL.'+str(PORT))).exists()
                report['server_stopped']=True
            except BaseException as error:
                cleanup.append(str(error));report['server_stopped']=False
        report['cleanup_errors']=cleanup;report['seconds']=time.monotonic()-started
        report['python_peak_rss_bytes']=rss()
        report['rss_scope']='pytest进程ru_maxrss峰值；PG按阶段ps采样自身postmaster及直属子进程RSS，求和含共享页重复计数，非连续峰值/全链RSS'
        (artifacts/'结果.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
        if cleanup and primary is None:raise RuntimeError('自有测试清理失败，见结果.json')
