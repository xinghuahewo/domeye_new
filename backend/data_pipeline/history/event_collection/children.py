"""集合向旧child传递剩余额度；不修改旧模块或五字段Token。"""
from dataclasses import replace
from pathlib import Path
import os
import sqlite3
from contextlib import closing

from data_pipeline.history.database_import import History as BaseHistory
from data_pipeline.history.database_import.freeze import read_json, quote


def remaining(budget):
    budget.check(); limits=budget.limits
    rows=limits.max_total_rows-budget.counts.get('child_rows',0)
    data=limits.max_total_bytes-budget.counts.get('child_data_bytes',0)
    # SQLite页数替换逻辑暂存计量；child目录元数据另作保守预留。
    used=budget.counts['temporary_bytes']-budget.counts.get('spool_logical_bytes',0)+budget.counts.get('spool_physical_bytes',0)
    disk=budget.collection_limits.temporary_bytes-used
    metadata=2*limits.max_metadata_bytes
    available=min(data,disk-metadata)
    if rows<1 or available<1: raise ValueError('child剩余行/字节/临时磁盘预算不足')
    return replace(limits,max_total_rows=rows,max_total_bytes=available,batch_rows=min(limits.batch_rows,rows)),disk


def sqlite_preflight(path,limits):
    """已复制且绑定SHA的只读文件；先计数，避免明知601>399仍启动child。"""
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        names=db.execute("SELECT name,sql FROM sqlite_schema WHERE type='table' AND substr(name,1,7)!='sqlite_'").fetchmany(limits.max_tables+1)
        if len(names)>limits.max_tables: raise ValueError('child表数超限')
        rows=0
        for name,ddl in names:
            if 'VIRTUAL TABLE' in (ddl or '').upper(): raise ValueError('child不支持虚拟表')
            rows+=db.execute('SELECT count(*) FROM '+quote(name)).fetchone()[0]
            if rows>limits.max_total_rows: raise ValueError('child原始行数超过集合剩余额度')
    if Path(path).stat().st_size>limits.max_total_bytes: raise ValueError('child原件超过剩余字节/磁盘额度')


def manifest_preflight(path,limits):
    m=read_json(path,limits.max_metadata_bytes)
    values=[t['rows'] for t in m['tables']]+[o['bytes'] for o in m['originals']]+[b['bytes'] for t in m['tables'] for b in t['blocks']]
    if any(type(v) is not int or v<0 for v in values): raise ValueError('child人口/字节声明无效')
    rows=sum(t['rows'] for t in m['tables'])
    data=sum(o['bytes'] for o in m['originals'])+sum(b['bytes'] for t in m['tables'] for b in t['blocks'])
    if rows>limits.max_total_rows or data>limits.max_total_bytes: raise ValueError('child清单超过集合剩余行/字节/磁盘额度')
    return m,rows,data


def verification_plan(paths,budget):
    """一遍整包验证共享同一源人口额度；先读有界清单，再允许child正文扫描。"""
    plan=[]; rows_left=budget.limits.max_total_rows; bytes_left=budget.limits.max_total_bytes
    for path in paths:
        budget.check()
        limits=replace(budget.limits,max_total_rows=max(1,rows_left),max_total_bytes=max(1,bytes_left),
                       batch_rows=min(budget.limits.batch_rows,max(1,rows_left)))
        _,rows,data=manifest_preflight(path,limits)
        if rows>rows_left or data>bytes_left: raise ValueError('child验证超过集合剩余行/字节额度')
        plan.append((path,limits)); rows_left-=rows; bytes_left-=data
    return plan


def directory_bytes(root,budget):
    total=entries=0; pending=[Path(root)]
    while pending:
        with os.scandir(pending.pop()) as items:
            for item in items:
                budget.check(); entries+=1
                if entries>budget.collection_limits.metadata_bytes//64: raise ValueError('child输出文件目录预算超限')
                if item.is_symlink(): raise ValueError('child输出不允许链接')
                if item.is_dir(follow_symlinks=False): pending.append(Path(item.path))
                else: total+=item.stat(follow_symlinks=False).st_size
    return total


def checked_lake(db,path,budget,disk):
    # 只检查当前候选目录大小，无逐批全闭包SHA。原生写入按有界批前后检查；
    # 不声称SQL写入/文件系统配额具有跨存储原子性，超限批不能得到child完成。
    class Lake:
        def __getattr__(self,key): return getattr(db,key)
        def execute(self,*a,**kw):
            budget.check()
            if directory_bytes(path,budget)>disk: raise ValueError('child/集合临时磁盘预算超限')
            result=db.execute(*a,**kw)
            budget.check()
            if directory_bytes(path,budget)>disk: raise ValueError('child/集合临时磁盘预算超限')
            return result
    return Lake()


class ChildHistory(BaseHistory):
    def __init__(self,parent,budget,limits,disk):
        super().__init__(parent.dsn,parent.root,limits,target_identity=parent.target_identity)
        self.parent_budget=budget; self.disk=disk
    def _lake(self,import_id,readonly):
        return checked_lake(super()._lake(import_id,readonly),self.data_root/import_id,
                            self.parent_budget,self.disk-2*self.limits.max_metadata_bytes)
