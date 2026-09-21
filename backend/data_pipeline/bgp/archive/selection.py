"""固定读取接缝：选择与内容上限来自checkpoint，普通查询不重扫文件SHA。"""
import json
import psycopg2
from data_pipeline.bgp.archive.store import connect_duckdb, literal, bound_table


def legacy_table(schema,snapshot,name):
    return bound_table(schema,snapshot,name)


class Selection:
    def __init__(self,dsn,run_id,snapshot):
        from data_pipeline.bgp.archive.checkpoint import sha, OBS_TABLES
        self.dsn=dsn;self.run_id=run_id;self.snapshot=snapshot;self.columns=OBS_TABLES
        with psycopg2.connect(dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute("SELECT plan,state,seal FROM observation_m2.runs WHERE run_id=%s",(run_id,));row=c.fetchone()
                if row is None or row[1]!='observation_sealed':raise ValueError('观察尚未封存')
                self.plan,self.state,self.seal=row
                if self.seal['snapshot']!=snapshot or sha({k:v for k,v in self.seal.items() if k!='digest'})!=self.seal['digest']:raise ValueError('封存版本/摘要不符')
                c.execute('SELECT ordinal,payload FROM observation_m2.checkpoints WHERE run_id=%s ORDER BY ordinal',(run_id,));actual=c.fetchall()
        self.checkpoints=self.seal['checkpoints'];self.schema=self.seal['schema'];self.manifest=self.plan['manifest']
        if actual!=[(i,cp) for i,cp in enumerate(self.checkpoints)] or len(actual)!=len(self.plan['inputs']):raise ValueError('封存checkpoint清单漂移')
        previous=None
        for cp,e in zip(self.checkpoints,self.plan['inputs']):
            if cp['previous']!=previous or cp['source_id']!=e['source_id'] or sha({k:v for k,v in cp.items() if k!='digest'})!=cp['digest']:raise ValueError('checkpoint身份/链损坏')
            previous=cp['digest']

    def check(self):
        again=Selection(self.dsn,self.run_id,self.snapshot)
        if again.seal!=self.seal or again.plan!=self.plan:raise ValueError('固定选择漂移')

    def check_sources(self,sources):
        expected=[(cp['ordinal'],cp) for cp in self.checkpoints if cp['source_id'] in sources]
        with psycopg2.connect(self.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute("SELECT state,seal->>'digest',plan_id FROM observation_m2.runs WHERE run_id=%s",(self.run_id,))
                if c.fetchone()!=('observation_sealed',self.seal['digest'],self.seal['plan_id']):raise ValueError('固定选择资格漂移')
                c.execute('SELECT ordinal,payload FROM observation_m2.checkpoints WHERE run_id=%s AND ordinal=ANY(%s) ORDER BY ordinal',(self.run_id,[i for i,_ in expected]))
                if c.fetchall()!=expected:raise ValueError('绑定来源checkpoint漂移')

    def connect(self):
        self.check();db=connect_duckdb()
        try:
            db.execute('LOAD ducklake');db.execute('LOAD postgres')
            db.execute('ATTACH '+literal('ducklake:postgres:'+self.dsn)+' AS lake (READ_ONLY)')
            found=db.execute(f'SELECT source_id,attempt,ordinal,checkpoint_digest FROM lake.{self.schema}.seal_selection AT (VERSION => {self.snapshot}) WHERE seal_id=? ORDER BY ordinal',[self.seal['seal_id']]).fetchall()
            expected=[(cp['source_id'],cp['attempt'],cp['ordinal'],cp['digest']) for cp in self.checkpoints]
            if found!=expected:raise ValueError('物理封存选择与PG回执不一致')
            return db
        except BaseException:db.close();raise

    def table(self,name,source_id=None):
        if name not in self.columns:raise ValueError('观察阶段未执行该表；不得当作零结果')
        cps=[cp for cp in self.checkpoints if source_id is None or cp['source_id']==source_id]
        if not cps:raise ValueError('来源不在固定选择中')
        conditions=' OR '.join(f"(attempt={literal(cp['attempt'])} AND rowid<={cp['tables'][name]['max_rowid']})" for cp in cps)
        # rowid为DuckLake不可重用的追加行号；本片无UPDATE/DELETE/GC。
        # 同attempt晚到INSERT即使在seal前出现，仍大于checkpoint上限。
        base=f'lake.{self.schema}."{name}" AT (VERSION => {self.snapshot})'
        selected=f'(SELECT * FROM {base} WHERE {conditions})'
        columns=','.join('p."'+n+'"' for n,_ in self.columns[name])
        if name=='paths' and source_id is None:
            return f'(SELECT {columns} FROM {selected} p JOIN lake.{self.schema}.path_owner o AT (VERSION => {self.snapshot}) ON p.attempt=o.attempt AND p.path_key=o.path_key WHERE o.seal_id={literal(self.seal["seal_id"])})'
        return f'(SELECT * EXCLUDE(attempt) FROM {selected})'


def for_run(dsn,run_id):
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute("SELECT seal FROM observation_m2.runs WHERE run_id=%s AND state='observation_sealed'",(run_id,));r=c.fetchone()
    if not r:raise ValueError('观察未封存')
    return Selection(dsn,run_id,r[0]['snapshot'])
