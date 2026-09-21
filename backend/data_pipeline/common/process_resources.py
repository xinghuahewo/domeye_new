"""S3进程私有SQLite临时目录与固定间隔资源采样；不提供调度/IPC。"""
import ctypes
import errno
import fcntl
import os
from pathlib import Path
import resource
import shutil
import stat
import sys
import tempfile
import time
import weakref

INTERVAL = 0.05
_binding = None
_monitors = weakref.WeakSet()
_duckdb_scopes = set()


def bind_sqlite_temp(parent):
    """只准在新解释器首次SQLite导入前调用；不修改调用者的父进程环境。"""
    global _binding
    if _binding is not None or 'sqlite3' in sys.modules or '_sqlite3' in sys.modules:
        raise ValueError('trend_s3_sqlite_requires_fresh_process')
    root = Path(tempfile.mkdtemp(prefix='trend-sqlite-', dir=Path(parent).resolve(strict=True)))
    st = root.stat()
    os.environ['SQLITE_TMPDIR'] = os.environ['TMPDIR'] = str(root)
    _binding = (os.getpid(), root, st.st_dev, st.st_ino)
    return root


def sqlite_temp():
    if _binding is None or _binding[0] != os.getpid():
        raise ValueError('trend_s3_sqlite_requires_fresh_process')
    _, root, device, inode = _binding
    st = root.stat()
    if ((st.st_dev,st.st_ino) != (device,inode) or not root.is_dir()
            or not os.access(root,os.W_OK|os.X_OK)
            or os.environ.get('SQLITE_TMPDIR') != str(root) or os.environ.get('TMPDIR') != str(root)):
        raise ValueError('trend_s3_sqlite_temp_binding_changed')
    return root


def _fds():
    """枚举当前进程完整FD快照；不使用实验中的固定0..255范围。"""
    if sys.platform.startswith('linux'):
        return [int(x) for x in os.listdir('/proc/self/fd')]
    if sys.platform != 'darwin': raise RuntimeError('trend_s3_fd_sampling_unavailable')
    lib = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
    call = lib.proc_pidinfo
    call.argtypes = [ctypes.c_int,ctypes.c_int,ctypes.c_uint64,ctypes.c_void_p,ctypes.c_int]
    call.restype = ctypes.c_int
    for _ in range(3):
        needed = call(os.getpid(),1,0,None,0)
        if needed <= 0: raise RuntimeError('trend_s3_fd_sampling_unavailable')
        buffer = ctypes.create_string_buffer(needed + 256)
        used = call(os.getpid(),1,0,buffer,len(buffer))
        if used <= 0 or used % 8: raise RuntimeError('trend_s3_fd_sampling_unavailable')
        if used < len(buffer):
            import struct
            return [fd for fd,typ in struct.iter_unpack('iI',buffer.raw[:used])]
    raise RuntimeError('trend_s3_fd_snapshot_unstable')


def fd_sample():
    records = []
    for fd in _fds():
        try: st = os.fstat(fd)
        except OSError as error:
            if error.errno == errno.EBADF: continue  # 枚举自身的目录FD或并发关闭，已不占用。
            raise
        if not stat.S_ISREG(st.st_mode): continue
        try:
            if sys.platform == 'darwin':
                name = fcntl.fcntl(fd,50,bytes(1024)).split(b'\0',1)[0].decode()
            else:
                name = os.readlink('/proc/self/fd/'+str(fd))
                if st.st_nlink == 0 and name.endswith(' (deleted)'): name = name[:-10]
        except OSError as error:
            if error.errno == errno.EBADF: continue
            raise RuntimeError('trend_s3_fd_path_unknown') from error
        if not name.startswith('/'): raise RuntimeError('trend_s3_fd_path_unknown')
        records.append((Path(name),st,fcntl.fcntl(fd,fcntl.F_GETFL) & os.O_ACCMODE))
    return records


def _amount(st):
    return (st.st_size, st.st_blocks * 512)


def _stat(path):
    try: return _amount(path.stat())
    except FileNotFoundError: return (0,0)


def _within(path, root):
    return path == root or root in path.parents


def track(path):
    """已知写入在打开前登记，关闭后freeze；累计写入不充当现占用。"""
    for monitor in tuple(_monitors): monitor.track(path)


def freeze(path):
    for monitor in tuple(_monitors): monitor.freeze(path)


def add_root(path):
    for monitor in tuple(_monitors):
        if monitor.bucket(Path(path).resolve()) is not None: monitor.add_root(path)


def bind_duckdb_temp(path):
    """只授予Runtime创建的专用DuckDB临时目录，后续监控复用此绑定。"""
    path=Path(path).resolve(strict=True)
    _duckdb_scopes.add(path)
    for monitor in tuple(_monitors):
        if monitor.bucket(path) is not None: monitor.bind_duckdb_temp(path)


def release_sqlite_temp():
    global _binding
    if _binding is None:return
    root=sqlite_temp()
    if any(_within(path,root) for path,st,mode in fd_sample()):
        raise ValueError('trend_s3_sqlite_temp_still_open')
    shutil.rmtree(root)  # 只清理本进程mkdtemp且身份仍匹配的目录。
    _binding = None


def remove_root(path):
    if Path(path).exists():
        raise ValueError('trend_s3_resource_cleanup_incomplete')
    for monitor in tuple(_monitors): monitor.remove_root(path)


class Monitor:
    def __init__(self, roots, *, max_rss, max_disk=None, temp_roots=(), max_temp=None, min_free=None,
                 clock=time.monotonic, fds=fd_sample, rss=None):
        self.max_rss=max_rss; self.max_disk=max_disk; self.max_temp=max_temp; self.min_free=min_free
        self.clock=clock; self.fds=fds; self.rss=rss or (lambda:resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024))
        self.roots={}; self.active={}; self.duckdb_scopes=set(); self.last=None; self.failure=None
        self.temp_roots=tuple(Path(p).resolve() for p in temp_roots)
        self.stats=dict(samples=0,directory_scans=0,disk_peak_bytes=0,unlinked_peak_bytes=0)
        for root in roots: self.add_root(root)
        for scope in tuple(_duckdb_scopes):
            if not scope.exists(): _duckdb_scopes.discard(scope)
            elif self.bucket(scope) is not None: self.bind_duckdb_temp(scope)
        _monitors.add(self)

    def scan(self, root):
        self.stats['directory_scans']+=1
        total=[0,0]
        for path in root.rglob('*'):
            if path.is_file():
                value=_stat(path)
                total=[a+b for a,b in zip(total,value)]
        return total

    def add_root(self, root):
        root=Path(root).resolve()
        if root in self.roots: return
        root = root.resolve(strict=True)
        value=self.scan(root); st=root.stat()
        # 子阶段目录从父基数中拆出，结束删除时无需保留每个已关闭文件路径。
        parents=[p for p in self.roots if _within(root,p)]
        if parents:
            parent=max(parents,key=lambda p:len(p.parts))
            self.roots[parent][0]=[a-b for a,b in zip(self.roots[parent][0],value)]
        self.roots[root]=[value,(st.st_dev,st.st_ino)]

    def bind_duckdb_temp(self,path):
        self.add_root(path)
        self.duckdb_scopes.add(Path(path).resolve(strict=True))

    def bucket(self,path):
        candidates=[p for p in self.roots if _within(path,p)]
        return max(candidates,key=lambda p:len(p.parts)) if candidates else None

    def track(self,path):
        path=Path(path).resolve(); root=self.bucket(path)
        if root is None or root in self.duckdb_scopes or path in self.active:return
        initial=_stat(path)
        self.roots[root][0]=[a-b for a,b in zip(self.roots[root][0],initial)]
        self.active[path]=root

    def freeze(self,path):
        path=Path(path).resolve(); root=self.active.pop(path,None)
        if root is not None:
            self.roots[root][0]=[a+b for a,b in zip(self.roots[root][0],_stat(path))]

    def remove_root(self,path):
        path=Path(path).resolve()
        for p in tuple(self.active):
            if _within(p,path):self.active.pop(p)
        for root in tuple(self.roots):
            if _within(root,path):
                self.roots.pop(root)
                self.duckdb_scopes.discard(root)

    def check(self, *, force=False, reconcile=False):
        if self.failure is not None: raise self.failure
        now=self.clock()
        if not force and self.last is not None and now-self.last<INTERVAL:return
        self.last=now
        try:self._sample(reconcile)
        except BaseException as error:
            self.failure=error
            raise

    def _sample(self,reconcile):
        self.stats['samples']+=1
        if _binding is not None: sqlite_temp()
        peak=self.rss(); self.stats['process_cumulative_peak_rss_bytes']=peak
        if peak>self.max_rss:raise ValueError('trend_rss_limit')
        totals={r:list(v[0]) for r,v in self.roots.items()}
        for root,(_,identity) in self.roots.items():
            st=root.stat()
            if (st.st_dev,st.st_ino)!=identity:raise ValueError('trend_s3_resource_root_changed')
            if self.min_free is not None and shutil.disk_usage(root).free<self.min_free:
                raise ValueError('trend_runtime_free_disk')
            if root in self.duckdb_scopes:
                # 仅扫描专用spill目录：捕获两次FD采样之间已关闭但仍占盘的文件。
                # 覆盖当前基数，删除即释放；不累计写入量或保留历史文件清单。
                self.roots[root][0]=self.scan(root)
                totals[root]=list(self.roots[root][0])
        for path,root in self.active.items():
            totals[root]=[a+b for a,b in zip(totals[root],_stat(path))]
        unlinked=set(); unlinked_size=0
        for path,st,mode in self.fds():
            root=self.bucket(path)
            if path.name.startswith('etilqs_') and (_binding is None or not _within(path,sqlite_temp())):
                raise ValueError('trend_s3_sqlite_temp_outside')
            if root is None:continue
            if st.st_dev != self.roots[root][1][0]:raise ValueError('trend_s3_fd_device_changed')
            if st.st_nlink==0:
                key=(st.st_dev,st.st_ino)
                if key not in unlinked:
                    unlinked.add(key);value=_amount(st)
                    totals[root]=[a+b for a,b in zip(totals[root],value)];unlinked_size+=value[1]
            elif mode != os.O_RDONLY and path not in self.active and root not in self.duckdb_scopes:
                raise ValueError('trend_s3_untracked_writer')
        self.stats['unlinked_peak_bytes']=max(self.stats['unlinked_peak_bytes'],unlinked_size)
        if reconcile:
            for root in sorted(self.roots,key=lambda p:len(p.parts),reverse=True):
                actual=self.scan(root)
                for child in self.roots:
                    if child!=root and _within(child,root):
                        # 只减直接子桶，避免更深子桶重复扣除。
                        if not any(p not in (root,child) and _within(child,p) and _within(p,root) for p in self.roots):
                            actual=[a-b for a,b in zip(actual,self._tree_total(child))]
                linked=list(self.roots[root][0])
                for path,bucket in self.active.items():
                    if bucket==root:linked=[a+b for a,b in zip(linked,_stat(path))]
                if actual!=linked:raise ValueError('trend_s3_resource_accounting_changed')
        total=max(sum(v[0] for v in totals.values()),sum(v[1] for v in totals.values()))
        temporary=max(sum(v[i] for r,v in totals.items() if any(_within(r,t) for t in self.temp_roots)) for i in (0,1))
        self.stats['disk_current_bytes']=total
        self.stats['temp_current_bytes']=temporary
        self.stats['unlinked_current_bytes']=unlinked_size
        self.stats['disk_peak_bytes']=max(self.stats['disk_peak_bytes'],total)
        if self.max_disk is not None and total>self.max_disk:raise ValueError('trend_total_disk')
        if self.max_temp is not None and temporary>self.max_temp:raise ValueError('trend_runtime_temp_disk')

    def _tree_total(self,root):
        total=list(self.roots[root][0])
        for path,bucket in self.active.items():
            if _within(bucket,root):total=[a+b for a,b in zip(total,_stat(path))]
        for child,value in self.roots.items():
            if child!=root and _within(child,root):total=[a+b for a,b in zip(total,value[0])]
        return total
