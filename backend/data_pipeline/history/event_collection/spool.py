"""候选目录内的有界结构暂存；最终主体写DuckLake，不作为业务库。"""
import sqlite3
from data_pipeline.history.event_collection.model import DEFINITIONS, ORDER
from data_pipeline.history.database_import.codec import canonical


class Spool:
    def __init__(self,path,budget,create=False):
        self.path,self.budget=path,budget
        self.db=sqlite3.connect(path if create else path.as_uri()+'?mode=ro',uri=not create)
        self.db.row_factory=sqlite3.Row
        if create:
            self.db.execute('PRAGMA journal_mode=OFF'); self.db.execute('PRAGMA cache_size=-2048')
            self.db.execute('PRAGMA temp_store=FILE')
            for name,cols in DEFINITIONS.items():
                self.db.execute('CREATE TABLE '+name+' ('+','.join('"'+col+'" '+('INTEGER' if kind in ('i','b') else 'TEXT') for col,kind in cols)+')')
            self.db.execute('CREATE UNIQUE INDEX node_location ON nodes(document_id,node_ordinal)')
            self.db.execute('CREATE INDEX members ON nodes(document_id,parent_ordinal,member_ordinal)')
            self.db.execute('CREATE INDEX keys ON nodes(document_id,parent_ordinal,key)')
        self.counts={name:0 for name in DEFINITIONS}

    def add(self,table,row):
        self.budget.check(); size=len(canonical(row))
        if size>self.budget.limits.max_row_bytes: raise ValueError('typed结构行超限')
        self.budget.add('typed_rows',1,self.budget.limits.max_total_rows)
        self.budget.add('typed_bytes',size,self.budget.limits.max_total_bytes)
        # SQLite页/索引膨胀另按文件页数检查；这里预先限制逻辑增长。
        self.budget.add('temporary_bytes',size,self.budget.collection_limits.temporary_bytes)
        self.budget.add('spool_logical_bytes',size)
        cols=DEFINITIONS[table]
        self.db.execute('INSERT INTO '+table+' VALUES ('+','.join('?' for _ in cols)+')',[row.get(k) for k,_ in cols])
        self.counts[table]+=1
        if self.counts[table]%self.budget.limits.batch_rows==0: self.flush()

    def flush(self):
        self.budget.check(); self.db.commit()
        pages=self.db.execute('PRAGMA page_count').fetchone()[0]
        size=self.db.execute('PRAGMA page_size').fetchone()[0]*pages
        non_spool=self.budget.counts['temporary_bytes']-self.budget.counts.get('spool_logical_bytes',0)
        if size+non_spool>self.budget.collection_limits.temporary_bytes: raise ValueError('结构暂存物理磁盘超限')
        self.budget.counts['spool_physical_bytes']=size
        self.budget.counts['candidate_disk_bound_bytes']=size+non_spool

    def rows(self,table):
        cur=self.db.execute('SELECT * FROM '+table+' ORDER BY '+ORDER[table])
        try:
            while True:
                self.budget.check(); rows=cur.fetchmany(1)  # 单行上限先已验证，不预取宽行批。
                if not rows: break
                yield dict(rows[0])
        finally: cur.close()

    def close(self): self.db.close()

    def node(self,doc,ordinal=0):
        row=self.db.execute('SELECT * FROM nodes WHERE document_id=? AND node_ordinal=?',(doc,ordinal)).fetchone()
        if row is None: raise ValueError('缺结构节点')
        return Node(self,dict(row))


class Node:
    def __init__(self,spool,row): self.spool,self.row=spool,row
    @property
    def doc(self): return self.row['document_id']
    @property
    def ordinal(self): return self.row['node_ordinal']
    def field(self,key,required=True):
        if self.row['kind']!='object': raise ValueError('profile要求对象')
        rows=self.spool.db.execute('SELECT * FROM nodes WHERE document_id=? AND parent_ordinal=? AND key=? ORDER BY member_ordinal LIMIT 2',(self.doc,self.ordinal,key)).fetchall()
        if len(rows)>1: raise ValueError('profile身份/依赖字段重键: '+key)
        if not rows:
            if required: raise ValueError('profile缺字段: '+key)
            return None
        return Node(self.spool,dict(rows[0]))
    def text(self):
        if self.row['kind']!='string': raise ValueError('profile要求字符串')
        return self.row['text_value']
    def integer(self):
        value=self.row['int64_value']
        if self.row['kind']!='number' or value is None or self.row['number_lexeme']!=str(value): raise ValueError('profile要求原生整数')
        return value
    def children(self,kind):
        if self.row['kind']!=kind: raise ValueError('profile容器类型不符')
        cur=self.spool.db.execute('SELECT * FROM nodes WHERE document_id=? AND parent_ordinal=? ORDER BY member_ordinal',(self.doc,self.ordinal))
        seen=set()
        try:
            for row in cur:
                self.spool.budget.check()
                if kind=='object':
                    if row['key'] in seen: raise ValueError('profile成员重键')
                    seen.add(row['key'])
                    if len(seen)>self.spool.budget.collection_limits.edges: raise ValueError('profile成员预算超限')
                yield Node(self.spool,dict(row))
        finally: cur.close()
