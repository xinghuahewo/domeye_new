"""独占阶段wall及当前RSS采样；累计ru_maxrss仅描述整个进程生命周期。"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import threading
import time


class ResourceSampler:
    """同一显式 cgroup 的资源与整台宿主 CPU 封装能量；两者不冒称相同归属范围。"""
    def __init__(self,root,cgroup):
        self.root=Path(root);self.cgroup=Path(cgroup);self.stop=threading.Event()

    def __enter__(self):
        if not (self.cgroup/'cpu.stat').is_file():raise ValueError('资源观测 cgroup 不存在')
        self.root.mkdir(parents=True,exist_ok=True)
        self.thread=threading.Thread(target=self.sample,daemon=True);self.thread.start();return self

    def __exit__(self,*exc):self.stop.set();self.thread.join()

    def sample(self):
        packages=[]
        for p in Path('/sys/class/powercap').glob('intel-rapl:*'):
            try:
                if (p/'name').read_text().strip().startswith('package-'):packages.append(p)
            except OSError:pass
        with (self.root/'resource-samples.jsonl').open('a') as output:
            while True:
                row=dict(monotonic=time.monotonic(),wall_time=time.time(),cgroup={},processes=[],energy_scope='whole_host_cpu_packages; task_and_subphase_attribution_unknown',rapl={})
                try:
                    for name in ('cpu.stat','memory.current','memory.peak','memory.events','io.stat'):
                        p=self.cgroup/name
                        if p.exists():row['cgroup'][name]=p.read_text().strip()
                    for package in packages:
                        try:row['rapl'][package.name]=dict(name=(package/'name').read_text().strip(),energy_uj=int((package/'energy_uj').read_text()),max_energy_range_uj=int((package/'max_energy_range_uj').read_text()))
                        except (OSError,ValueError):pass
                    pids=set()
                    for p in self.cgroup.rglob('cgroup.procs'):
                        try:pids.update(map(int,p.read_text().split()))
                        except OSError:pass
                    for pid in pids:
                        try:
                            path=Path('/proc')/str(pid);s=(path/'stat').read_text().rsplit(')',1)[1].split()
                            row['processes'].append(dict(pid=pid,name=(path/'comm').read_text().strip(),rss_bytes=int((path/'statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE'),user_ticks=int(s[11]),system_ticks=int(s[12])))
                        except (OSError,ValueError,IndexError):pass
                except OSError as error:row['sample_error']=str(error)
                output.write(json.dumps(row)+'\n');output.flush()
                if self.stop.wait(1):break


class Metrics:
    def __init__(self,samples_path,*,resource_metrics=False):
        self.values={};self.stage=None;self.lock=threading.Lock();self.stop=threading.Event()
        self.resource_metrics=resource_metrics
        self.samples_path=Path(samples_path)
        self.thread=threading.Thread(target=self.sample,daemon=True);self.thread.start()

    def sample(self):
        while not self.stop.wait(.1):
            with self.lock:stage=self.stage
            if stage is None:continue
            try:
                if sys.platform=='linux':rss=int(Path('/proc/self/statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
                else:rss=int(subprocess.check_output(['/bin/ps','-o','rss=','-p',str(os.getpid())]))*1024
                with self.lock:
                    if self.stage!=stage:continue
                    v=self.values[stage];v['peak_rss_bytes']=max(v['peak_rss_bytes'] or 0,rss)
                    v['peak_scope']='sampled_current_process_rss';v['rss_samples']+=1
                with self.samples_path.open('a') as f:
                    f.write(json.dumps(dict(stage=stage,rss_bytes=rss,monotonic=time.monotonic()))+'\n');f.flush()
            except (OSError,ValueError,subprocess.SubprocessError):pass

    @contextmanager
    def measure(self,stage):
        start=time.monotonic();thread_start=time.thread_time()
        before=resource.getrusage(resource.RUSAGE_SELF) if self.resource_metrics else None
        def io():
            if sys.platform!='linux':return {}
            try:return {k:int(v) for k,v in (line.split(':') for line in Path('/proc/self/io').read_text().splitlines())}
            except (OSError,ValueError):return {}
        before_io=io() if self.resource_metrics else {}
        with self.lock:
            previous=self.stage;self.stage=stage
            self.values.setdefault(stage,dict(wall_seconds=0,calls=0,peak_rss_bytes=None,peak_scope='unknown_no_sample',rss_samples=0,sample_interval_seconds=.1,
                thread_cpu_seconds=0,thread_cpu_scope='calling_thread_only',user_cpu_seconds=0 if self.resource_metrics else None,system_cpu_seconds=0 if self.resource_metrics else None,io_bytes={},
                cpu_io_scope='current_process_including_native_threads' if self.resource_metrics else 'not_measured'))
        try:yield
        finally:
            after=resource.getrusage(resource.RUSAGE_SELF) if self.resource_metrics else None;after_io=io() if self.resource_metrics else {}
            with self.lock:
                v=self.values[stage];v['wall_seconds']+=time.monotonic()-start;v['thread_cpu_seconds']+=time.thread_time()-thread_start;v['calls']+=1;self.stage=previous
                if before is not None:
                    v['user_cpu_seconds']+=after.ru_utime-before.ru_utime;v['system_cpu_seconds']+=after.ru_stime-before.ru_stime
                for name in ('rchar','wchar','read_bytes','write_bytes'):
                    if name in before_io and name in after_io:v['io_bytes'][name]=v['io_bytes'].get(name,0)+max(0,after_io[name]-before_io[name])

    def receipt(self):
        with self.lock:values=json.loads(json.dumps(self.values))
        return dict(stages=values,process_lifetime_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),peak_scope='whole_process_lifetime',pid=os.getpid())

    def close(self):
        self.stop.set();self.thread.join()
