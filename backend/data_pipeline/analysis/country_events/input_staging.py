"""私有SQLite原行暂存：顺序/重复不变，Python每次只解码有限批与单行。"""
from collections.abc import Mapping, Sequence, MutableMapping
from pathlib import Path
import sqlite3
import tempfile
import uuid
import resource
import sys

from data_pipeline.analysis.country_events.snapshot_schema import encode, decode
from data_pipeline.analysis.country_events.snapshot_store import cleanup


class Rows(Sequence):
    def __init__(self,store,name):self.store,self.name=store,name
    def __len__(self):return self.store.counts[self.name]
    def __getitem__(self,index):
        if type(index) is not int:raise TypeError('SQLite原行只接受单个原序号')
        if index<0:index+=len(self)
        row=self.store.db.execute('SELECT payload FROM bodies WHERE partition=? AND ordinal=?',(self.name,index)).fetchone()
        if row is None:raise IndexError(index)
        return self.store.unpack(row[0])
    def __iter__(self):
        cursor=self.store.db.execute('SELECT payload FROM bodies WHERE partition=? ORDER BY ordinal',(self.name,))
        try:
            while batch:=cursor.fetchmany(1):
                for row in batch:yield self.store.unpack(row[0])
        finally:cursor.close()


class Lookup(Mapping):
    def __init__(self,rows,field,identical=False):
        self.rows,self.store=rows,rows.store;self.name=encode((rows.name,field))
        found=self.store.db.execute('SELECT 1 FROM indexes WHERE name=?',(self.name,)).fetchone()
        if found:return
        for ordinal,row in enumerate(rows):
            key=encode(row[field]);prior=self.store.db.execute('SELECT ordinal FROM lookups WHERE name=? AND key=?',(self.name,key)).fetchone()
            if prior:
                if not identical or rows[prior[0]]!=row:raise ValueError('M3 原引用重复或冲突:'+field)
            else:self.store.db.execute('INSERT INTO lookups VALUES (?,?,?,?)',(self.name,key,rows.name,ordinal))
        self.store.db.execute('INSERT INTO indexes VALUES (?)',(self.name,));self.store.flush()
    def __len__(self):return self.store.db.execute('SELECT count(*) FROM lookups WHERE name=?',(self.name,)).fetchone()[0]
    def __iter__(self):
        cursor=self.store.db.execute('SELECT key FROM lookups WHERE name=? ORDER BY ordinal',(self.name,))
        try:
            for row in cursor:self.store.check();yield decode(row[0])
        finally:cursor.close()
    def ordinal(self,key):
        row=self.store.db.execute('SELECT ordinal FROM lookups WHERE name=? AND key=?',(self.name,encode(key))).fetchone()
        if row is None:raise KeyError(key)
        return row[0]
    def __getitem__(self,key):return self.rows[self.ordinal(key)]
    def values(self):
        cur=self.store.db.execute('SELECT b.payload FROM lookups l JOIN bodies b ON b.partition=l.partition AND b.ordinal=l.ordinal WHERE l.name=? ORDER BY l.ordinal',(self.name,))
        try:
            for row in cur:yield self.store.unpack(row[0])
        finally:cur.close()


class References(Mapping):
    def __init__(self,lookup,owner,aid,view):self.lookup,self.owner,self.aid,self.view=lookup,owner,aid,view
    def __len__(self):return len(self.lookup)
    def __iter__(self):return iter(self.lookup)
    def __getitem__(self,key):return self.owner,self.aid,self.view,str(self.lookup.ordinal(key))


class GapReferences:
    def __init__(self,rows,aid):self.rows,self.aid=rows,aid
    def __iter__(self):
        for i,row in enumerate(self.rows):yield row['raw'],('canonical',self.aid,'scope_gap',str(i))


class DiskMap(MutableMapping):
    def __init__(self,store):self.store=store;self.name=uuid.uuid4().hex
    def __len__(self):return self.store.db.execute('SELECT count(*) FROM maps WHERE name=?',(self.name,)).fetchone()[0]
    def __iter__(self):
        cursor=self.store.db.execute('SELECT key FROM maps WHERE name=? ORDER BY rowid',(self.name,))
        try:
            for row in cursor:self.store.check();yield decode(row[0])
        finally:cursor.close()
    def __getitem__(self,key):
        row=self.store.db.execute('SELECT value FROM maps WHERE name=? AND key=?',(self.name,encode(key))).fetchone()
        if row is None:raise KeyError(key)
        return self.store.unpack(row[0])
    def __setitem__(self,key,value):
        text=self.store.pack(value)
        self.store.db.execute('INSERT INTO maps VALUES (?,?,?) ON CONFLICT(name,key) DO UPDATE SET value=excluded.value',(self.name,encode(key),text))
        self.store.check()
    def __delitem__(self,key):
        if self.store.db.execute('DELETE FROM maps WHERE name=? AND key=?',(self.name,encode(key))).rowcount!=1:raise KeyError(key)
    def values(self):
        cur=self.store.db.execute('SELECT value FROM maps WHERE name=? ORDER BY rowid',(self.name,))
        try:
            for row in cur:yield self.store.unpack(row[0])
        finally:cur.close()
    def items(self):
        cur=self.store.db.execute('SELECT key,value FROM maps WHERE name=? ORDER BY rowid',(self.name,))
        try:
            for key,value in cur:yield decode(key),self.store.unpack(value)
        finally:cur.close()
    def close(self):self.store.db.execute('DELETE FROM maps WHERE name=?',(self.name,))


class GroupRows(Sequence):
    def __init__(self,group,key):self.group,self.key=group,encode(key)
    def __len__(self):
        row=self.group.store.db.execute('SELECT total FROM group_counts WHERE name=? AND key=?',(self.group.name,self.key)).fetchone()
        return row[0] if row else 0
    def __iter__(self):
        cur=self.group.store.db.execute('SELECT value FROM groups WHERE name=? AND key=? ORDER BY ordering',(self.group.name,self.key))
        try:
            for row in cur:yield self.group.store.unpack(row[0])
        finally:cur.close()
    def __getitem__(self,n):
        if type(n) is not int:raise TypeError('分组原行只接受单个序号')
        if n==-1:
            row=self.group.store.db.execute('SELECT value FROM groups WHERE name=? AND key=? ORDER BY ordering DESC LIMIT 1',(self.group.name,self.key)).fetchone()
            if row is None:raise IndexError(n)
            return self.group.store.unpack(row[0])
        if n<0:n+=len(self)
        row=self.group.store.db.execute('SELECT value FROM groups WHERE name=? AND key=? ORDER BY ordering LIMIT 1 OFFSET ?',(self.group.name,self.key,n)).fetchone()
        if row is None:raise IndexError(n)
        return self.group.store.unpack(row[0])
    def append(self,value):
        raw=value.get('raw',value);order=raw['sequence']
        self.group.store.db.execute('INSERT INTO groups VALUES (?,?,?,?)',(self.group.name,self.key,order,self.group.store.pack(value)))
        self.group.store.db.execute('INSERT INTO group_counts VALUES (?,?,1) ON CONFLICT(name,key) DO UPDATE SET total=total+1',(self.group.name,self.key))
        self.group.store.check()
    def sort(self,*,key):pass  # 原sequence在落盘主键中排序，不装载整条链。


class Groups(Mapping):
    def __init__(self,store):self.store=store;self.name=uuid.uuid4().hex
    def __len__(self):return self.store.db.execute('SELECT count(DISTINCT key) FROM groups WHERE name=?',(self.name,)).fetchone()[0]
    def __iter__(self):
        cur=self.store.db.execute('SELECT DISTINCT key FROM groups WHERE name=? ORDER BY key',(self.name,))
        try:
            for row in cur:self.store.check();yield decode(row[0])
        finally:cur.close()
    def __getitem__(self,key):return GroupRows(self,key)
    def close(self):
        self.store.db.execute('DELETE FROM groups WHERE name=?',(self.name,))
        self.store.db.execute('DELETE FROM group_counts WHERE name=?',(self.name,))


class DiskSet:
    def __init__(self,store):self.values=store.new_map()
    def __contains__(self,key):return key in self.values
    def add(self,key):self.values[key]=True
    def __iter__(self):return iter(self.values)
    def __len__(self):return len(self.values)
    def close(self):self.values.close()


class SourceStore(Mapping):
    def __init__(self,scratch_root,*,max_rows,max_bytes,max_row_bytes,batch_rows,guard,max_rss_bytes=2*1024**3,max_disk_bytes=None):
        self.guard=guard;self.max_rows=max_rows;self.max_bytes=max_bytes
        self.max_row_bytes=max_row_bytes;self.batch_rows=batch_rows
        self.max_rss_bytes=max_rss_bytes
        # 旧max_bytes兼容为物理SQLite预算；逻辑累计字节只计量。
        self.max_disk_bytes=max_bytes if max_disk_bytes is None else max_disk_bytes
        if any(type(n) is not int or n<1 for n in (max_rows,max_bytes,max_row_bytes,batch_rows,max_rss_bytes,self.max_disk_bytes)):
            raise ValueError('M3 暂存预算必须有限明确')
        self.temp=tempfile.TemporaryDirectory(prefix='country-m3-source-',dir=scratch_root)
        self.root=Path(self.temp.name);self.db=None;self.closed=False
        try:
            self.db=sqlite3.connect(self.root/'source.sqlite')
            self.db.execute('PRAGMA cache_size=-8192');self.db.execute('PRAGMA temp_store=FILE')
            self.db.executescript('''CREATE TABLE bodies(partition TEXT,ordinal INTEGER,payload TEXT,PRIMARY KEY(partition,ordinal));
                CREATE TABLE lookups(name TEXT,key TEXT,partition TEXT,ordinal INTEGER,PRIMARY KEY(name,key));
                CREATE TABLE indexes(name TEXT PRIMARY KEY);
                CREATE TABLE groups(name TEXT,key TEXT,ordering INTEGER,value TEXT,PRIMARY KEY(name,key,ordering));
                CREATE TABLE group_counts(name TEXT,key TEXT,total INTEGER,PRIMARY KEY(name,key));
                CREATE TABLE maps(name TEXT,key TEXT,value TEXT,PRIMARY KEY(name,key));''')
        except BaseException as error:
            cleanup((self.close,),error);raise
        self.partitions={};self.counts={};self.count=self.bytes=self.peak_row_bytes=0
    def __len__(self):return len(self.partitions)
    def __iter__(self):return iter(self.partitions)
    def __getitem__(self,key):return Rows(self,self.partitions[key])
    def check(self):
        self.guard()
        if self.closed:raise ValueError('M3 暂存已关闭')
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        if rss>self.max_rss_bytes:raise ValueError('resource_limit:M3_source_spool_RSS')
        pages=self.db.execute('PRAGMA page_count').fetchone()[0]*self.db.execute('PRAGMA page_size').fetchone()[0]
        main=self.root/'source.sqlite'
        disk=max(pages,main.stat().st_size)+sum(p.stat().st_size for p in self.root.iterdir() if p!=main and p.is_file())
        if disk>self.max_disk_bytes:raise ValueError('resource_limit:M3_source_spool_disk')
    def pack(self,value):
        self.check();text=encode(value);size=len(text.encode())
        if size>self.max_row_bytes:raise ValueError('resource_limit:M3_source_single_row')
        self.peak_row_bytes=max(self.peak_row_bytes,size);return text
    def unpack(self,text):
        self.check()
        if len(text.encode())>self.max_row_bytes:raise ValueError('resource_limit:M3_source_single_row')
        return decode(text)
    def add_view(self,key,rows):
        if key in self.partitions:raise ValueError('M3 暂存视图重复')
        name=encode(key);self.partitions[key]=name;self.counts[name]=0
        iterator=iter(rows);primary=None
        try:
            for ordinal,row in enumerate(iterator):
                self.add_record(key,ordinal,row)
            self.flush()
        except BaseException as error:primary=error;raise
        finally:
            close=getattr(iterator,'close',None)
            if close is not None:cleanup((close,),primary)
    def add_record(self,key,ordinal,row):
        if type(ordinal) is not int or ordinal<0:raise ValueError('M3 原序号无效')
        name=self.partitions.setdefault(key,encode(key));self.counts.setdefault(name,0);text=self.pack(row);size=len(text.encode())
        self.db.execute('INSERT INTO bodies VALUES (?,?,?)',(name,ordinal,text));self.count+=1;self.bytes+=size;self.counts[name]+=1;self.check()
    def check_ordinals(self):
        if self.db.execute('SELECT 1 FROM bodies GROUP BY partition HAVING min(ordinal)!=0 OR max(ordinal)!=count(*)-1 LIMIT 1').fetchone():
            raise ValueError('M3 原序号缺项')
    def flush(self):self.db.commit();self.check()
    def new_map(self):return DiskMap(self)
    def new_groups(self):return Groups(self)
    def new_set(self):return DiskSet(self)
    def close(self):
        if not self.closed:
            self.closed=True
            cleanup(((self.db.close if self.db is not None else lambda:None),self.temp.cleanup))
