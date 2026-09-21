"""有限显式源码快照与新解释器绑定；只使用标准库，不推断依赖图。"""
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
from types import SimpleNamespace


def digest(identity):
    return hashlib.sha256(json.dumps(identity,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def code_identity(root,groups,packages,*,schema):
    root=Path(root).resolve();files={}
    for group,patterns in groups.items():
        for pattern in patterns:
            paths=sorted(root.glob(pattern))
            if not paths:raise ValueError('显式版本依赖缺失：'+pattern)
            for path in paths:
                if not path.is_file() or path.is_symlink():raise ValueError('源码清单只允许普通文件')
                relative=path.resolve().relative_to(root).as_posix()
                files[relative]={'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'group':group}
    return {'schema_version':schema,'files':files,'python':platform.python_version(),
            'packages':{package:version(package) for package in packages}}


def enter(root,entry,request,identity_builder,*,child=False,expected_digest=None,namespaces=('data_pipeline','utils')):
    """父进程只复制显式文件并启动子进程；子进程在导入业务前绑定。父进程返回False。"""
    root=Path(root).resolve();identity=identity_builder(root)
    if not child:
        with tempfile.TemporaryDirectory(prefix='domeye-code-') as temp:
            frozen=Path(temp)
            for relative in identity['files']:
                destination=frozen/relative;destination.parent.mkdir(parents=True,exist_ok=True)
                destination.write_bytes((root/relative).read_bytes());destination.chmod(0o444)
            if identity_builder(frozen)!=identity:raise ValueError('复制期间显式代码变化')
            directories=sorted((p for p in frozen.rglob('*') if p.is_dir()),key=lambda p:len(p.parts),reverse=True)
            for path in [*directories,frozen]:path.chmod(0o555)
            try:
                subprocess.run([sys.executable,'-I','-B',str(frozen/entry),str(Path(request).resolve()),
                                '--frozen-child','--expected-digest',digest(identity)],check=True)
            finally:
                for path in [frozen,*reversed(directories)]:path.chmod(0o755)
        return False
    if not sys.flags.isolated or not sys.dont_write_bytecode or digest(identity)!=expected_digest:
        raise ValueError('需要隔离新解释器和父进程绑定摘要')
    if any(p.stat().st_mode & 0o222 for p in [root,*root.rglob('*')]) or list(root.rglob('*.pyc')):
        raise ValueError('源码快照必须只读且无字节码缓存')
    if any(any(name==ns or name.startswith(ns+'.') for ns in namespaces) for name in sys.modules):
        raise ValueError('绑定前已有项目业务模块')
    sys.modules['_domeye_frozen_entry']=SimpleNamespace(root=root,identity=identity,pid=os.getpid(),namespaces=namespaces)
    return True


def execution_identity(root,identity_builder,*,fixture_only=False):
    root=Path(root).resolve();current=identity_builder(root)
    if fixture_only:return {**current,'execution_mode':'synthetic-fixture-api'}
    entry=sys.modules.get('_domeye_frozen_entry')
    if (entry is None or entry.root!=root or entry.pid!=os.getpid() or entry.identity!=current
            or not sys.flags.isolated or not sys.dont_write_bytecode):
        raise ValueError('正式执行必须通过冻结新进程入口')
    sources={}
    for name,module in tuple(sys.modules.items()):
        if not any(name==ns or name.startswith(ns+'.') for ns in entry.namespaces):continue
        path=Path(getattr(module,'__file__','')).resolve()
        try:relative=path.relative_to(root).as_posix()
        except ValueError:raise ValueError('实际业务模块来自快照外：'+name) from None
        if relative not in current['files'] or path.suffix!='.py':raise ValueError('实际业务模块未列入显式依赖：'+name)
        sources[name]=relative
    return {**current,'execution_mode':'frozen-fresh-process','execution_binding':{
            'pid':entry.pid,'snapshot_root':str(root),'snapshot_digest':digest(current),'module_sources':sources}}
