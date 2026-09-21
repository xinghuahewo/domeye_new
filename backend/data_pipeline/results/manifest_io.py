"""Q1人工发布的固定编码、资源预算和文件封存。"""
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import sys


def encode(value):
    def scalar(v):
        if isinstance(v, (datetime, date)): return v.isoformat()
        if isinstance(v, Decimal): return str(v)
        raise TypeError(type(v).__name__)
    return json.dumps(value, default=scalar, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(',', ':'))


def digest(value): return hashlib.sha256(encode(value).encode()).hexdigest()


def require(condition, reason):
    if not condition: raise ValueError(reason)


@dataclass(frozen=True)
class Limits:
    batch_rows: int = 256
    batch_bytes: int = 1024**2
    max_row_bytes: int = 4*1024**2
    max_rows: int = 1000000
    max_rss_bytes: int = 2*1024**3
    min_free_bytes: int = 256*1024**2

    def check(self, root, rows=0):
        require(all(v > 0 for v in (self.batch_rows,self.batch_bytes,self.max_row_bytes,
                                    self.max_rows,self.max_rss_bytes)), '无效资源预算')
        require(self.min_free_bytes >= 0, '无效磁盘预算')
        require(rows <= self.max_rows, '行数保护触发')
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        require(rss <= self.max_rss_bytes, 'RSS保护触发')
        require(shutil.disk_usage(root).free >= self.min_free_bytes, '磁盘保护触发')


def stamp(path):
    p=Path(path); require(p.is_file() and not p.is_symlink(), '文件缺失或符号链接')
    st=p.stat()
    return [st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns]


def file_hash(path, guard=lambda:None):
    before=stamp(path); h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024**2),b''):
            guard();h.update(block)
    require(stamp(path)==before, '校验期间文件漂移')
    return h.hexdigest()


def fsync_dir(path):
    fd=os.open(path,os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


def seal(path, guard=lambda:None):
    """首轮仅本任务文件；单写入者负责封存后不覆盖或清理。"""
    p=Path(path).resolve(); sha=file_hash(p,guard)
    if p.stat().st_mode & 0o222: p.chmod(0o444)
    with p.open('rb') as f: os.fsync(f.fileno())
    fsync_dir(p.parent)
    return {'path':str(p),'sha256':sha,'stamp':stamp(p)}


def write_sealed(path, value):
    path=Path(path)
    with path.open('x') as f:
        f.write(encode(value)); f.flush(); os.fsync(f.fileno())
    return seal(path)
