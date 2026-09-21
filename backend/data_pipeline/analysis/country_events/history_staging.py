"""Canonical历史定位落盘；不把全变化正文或每对象全历史列表驻留Python。"""
from collections.abc import Mapping,Sequence
from data_pipeline.analysis.country_events.snapshot_schema import encode, decode
from data_pipeline.bgp.replay.quality_overlay import ScopeIndex, position_key
from data_pipeline.bgp.replay.route_replay import identity
from data_pipeline.analysis.country_events.models import Time


class Entries(Sequence):
    def __init__(self,objects,key):self.objects,self.key=objects,key
    def __len__(self):return self.objects.db.execute('SELECT count(*) FROM history_objects WHERE object=?',(self.key,)).fetchone()[0]
    @staticmethod
    def unpack(row):return tuple(row[:4]),row[4],row[5]
    def __getitem__(self,n):
        if type(n) is not int:raise TypeError('历史只接受原单行定位')
        if n<0:n+=len(self)
        row=self.objects.db.execute('SELECT rank,record,phase,element,table_name,ordinal FROM history_objects WHERE object=? ORDER BY rank,record,phase,element LIMIT 1 OFFSET ?',(self.key,n)).fetchone()
        if row is None:raise IndexError(n)
        return self.unpack(row)
    def __iter__(self):
        cur=self.objects.db.execute('SELECT rank,record,phase,element,table_name,ordinal FROM history_objects WHERE object=? ORDER BY rank,record,phase,element',(self.key,))
        try:
            for row in cur:self.objects.store.check();yield self.unpack(row)
        finally:cur.close()
    def append(self,value):
        p,table,ordinal=value
        self.objects.db.execute('INSERT INTO history_objects VALUES (?,?,?,?,?,?,?)',(self.key,*p,table,ordinal))
        self.objects.store.check()
    def before(self,p):
        row=self.objects.db.execute('SELECT rank,record,phase,element,table_name,ordinal FROM history_objects WHERE object=? AND (rank,record,phase,element)<=(?,?,?,?) ORDER BY rank DESC,record DESC,phase DESC,element DESC LIMIT 1',(self.key,*p)).fetchone()
        return None if row is None else self.unpack(row)


class Objects(Mapping):
    def __init__(self,store):
        self.store,self.db=store,store.db
        self.db.execute('CREATE TABLE history_objects(object TEXT,rank INTEGER,record INTEGER,phase INTEGER,element INTEGER,table_name TEXT,ordinal INTEGER,PRIMARY KEY(object,rank,record,phase,element))')
    def __len__(self):return self.db.execute('SELECT count(DISTINCT object) FROM history_objects').fetchone()[0]
    def __iter__(self):
        cur=self.db.execute('SELECT DISTINCT object FROM history_objects ORDER BY object')
        try:
            for row in cur:self.store.check();yield row[0]
        finally:cur.close()
    def __getitem__(self,key):
        if not self.db.execute('SELECT 1 FROM history_objects WHERE object=? LIMIT 1',(key,)).fetchone():raise KeyError(key)
        return Entries(self,key)
    def setdefault(self,key,default):return Entries(self,key)


class Gaps:
    def __init__(self,store,rows):self.store,self.gaps=store,rows;self.seen=store.new_map()
    def add(self,raw):
        if raw['gap_id'] in self.seen:raise ValueError('M3 原Gap身份重复')
        self.seen[raw['gap_id']]=True
    def matching(self,scope):
        for row in self.gaps:
            index=ScopeIndex();index.add(row['raw'])
            yield from index.matching(scope)


def state_before(history,messages,scope,sample_us):
    history.check_budget();history.validate_scope(scope)
    if not history.sealed or type(sample_us) is not int:raise ValueError('M3 时间边界未封存')
    history.stats['time_queries']+=1
    store=history.store;db=store.db;obj=identity(scope)
    if not hasattr(history,'_disk_times'):
        history._disk_times=store.new_map()
        db.execute('CREATE TABLE history_times(object TEXT,rank INTEGER,record INTEGER,phase INTEGER,element INTEGER,low INTEGER,high INTEGER,PRIMARY KEY(object,rank,record,phase,element))')
        db.execute('CREATE TABLE history_time_bounds(object TEXT,rank INTEGER,record INTEGER,phase INTEGER,element INTEGER,high_prefix INTEGER,low_suffix INTEGER)')
        db.execute('CREATE INDEX history_time_bound_query ON history_time_bounds(object,high_prefix,rank,record,phase,element)')
    if obj not in history._disk_times:
        cost=len(history.index.gaps)
        history.stats['gap_candidates']+=cost
        def add(p,at):
            history.guard();size=len(encode((p,at.lower_us,at.upper_us)).encode())*3
            db.execute('INSERT INTO history_times VALUES (?,?,?,?,?,?,?)',(obj,*p,at.lower_us,at.upper_us))
            history.stats['temporal_rows']+=1;history.stats['temporal_bytes']+=size;store.check()
        for p,table,n in history.objects.get(obj,()):
            row=history.rows[table][n];m=messages[row['message_id']]
            if m['message_id']!=row['message_id'] or m['source_id']!=row['source_id'] or m['record']!=p[1]:raise ValueError('M3 时间原消息冲突')
            add(p,Time(m['epoch'],m['microsecond']))
        for gap,_ in history.index.matching(scope):add(position_key(gap['position']),Time(**gap['raw_time']))
        db.execute('''INSERT INTO history_time_bounds SELECT object,rank,record,phase,element,
          max(high) OVER(ORDER BY rank,record,phase,element ROWS UNBOUNDED PRECEDING) AS high_prefix,
          min(low) OVER(ORDER BY rank,record,phase,element ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS low_suffix
        FROM history_times WHERE object=?''',(obj,))
        history._disk_times[obj]=True;store.flush()
    row=db.execute('''SELECT rank,record,phase,element,low_suffix FROM history_time_bounds WHERE object=? AND high_prefix<?
        ORDER BY high_prefix DESC,rank DESC,record DESC,phase DESC,element DESC LIMIT 1''',(obj,sample_us)).fetchone()
    if row is None or row[4] is not None and row[4]<sample_us:
        return dict(presence='unknown',path_key=None,origin=None,source_ref=None,time_boundary='not_locatable',position=None,gap_refs=(),active_gap_refs=())
    return dict(history.at(scope,tuple(row[:4])),time_boundary='located')
