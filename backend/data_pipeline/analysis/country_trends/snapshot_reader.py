"""S2公开固定Reader；全流独立审计，有限页不重复全文件SHA。"""
from contextlib import contextmanager,closing
from dataclasses import asdict,dataclass
from pathlib import Path
import base64,hashlib,json,sqlite3,tempfile,sys
import psycopg2
from data_pipeline.analysis.country_trends.snapshot_schema import VERSION, PROFILE, CODEC, RULES, TABLES, SCHEMAS, encode, decode, restore, row_hash, reference_count
from data_pipeline.bgp.archive.store import literal
from data_pipeline.analysis.country_trends.snapshot_inputs import verify_inputs
from data_pipeline.analysis.country_trends import snapshot_audit as audit
from data_pipeline.analysis.country_events.snapshot_store import database_identity, lake_connect, cleanup
from data_pipeline.results.manifest_io import encode as json_encode, file_hash, stamp


@dataclass(frozen=True)
class TrendReadReceipt:
    binding: object
    scope: str
    rows: int
    sha256: str
    full_body_validated: bool


@dataclass(frozen=True)
class TrendPage:
    items: tuple
    total: int
    next_cursor: str | None
    scope: str


def layout(db,schema,snapshot,root,proof,expected=None,kinds=TABLES):
    linked={}
    # 真实湖schema/table集合亦核，不只DESCRIBE预期表。
    actual=db.execute("SELECT table_name FROM duckdb_tables() WHERE database_name='lake' AND schema_name=?",[schema]).fetchall()
    if {r[0] for r in actual}!=set(TABLES):raise ValueError('trend_lake_table_set')
    for kind in kinds:
        files=db.execute("SELECT data_file,data_file_size_bytes,delete_file FROM ducklake_list_files('lake',?,schema=>?,snapshot_version=>?)",[kind,schema,snapshot]).fetchall()
        if any(d is not None or Path(p).resolve()!=root/'data'/(kind+'.parquet') for p,n,d in files):raise ValueError('trend_lake_file_scope')
        linked[kind]=[[str(Path(p).resolve()),n] for p,n,d in files]
        cols=db.execute(f'DESCRIBE SELECT * FROM lake.{schema}.{kind} AT (VERSION => {snapshot})').fetchall()
        if [(r[0],r[1]) for r in cols]!=list(SCHEMAS[kind]):raise ValueError('trend_lake_schema')
        count=db.execute(f'SELECT count(*) FROM lake.{schema}.{kind} AT (VERSION => {snapshot})').fetchone()[0]
        if count!=proof['tables'][kind]['rows']:raise ValueError('trend_lake_counts')
    if expected is not None and {k:expected[k] for k in kinds}!=linked:raise ValueError('trend_lake_binding')
    return linked


def _read_audited(dsn,schema,snapshot,root,proof,result_id,limits,budget):
    db=index=temporary=None;error=None
    try:
        temporary=tempfile.TemporaryDirectory(prefix='country-trend-read-')
        budget.scratch_roots.add(Path(temporary.name))
        db=lake_connect(dsn);db.execute('SET temp_directory='+literal(Path(temporary.name)/'duck-temp'))
        linked=layout(db,schema,snapshot,root,proof)
        from types import SimpleNamespace
        inspect_witnesses(db,SimpleNamespace(root=str(root),schema_name=schema,snapshot=snapshot),budget.check,budget,scratch=temporary.name)
        index=sqlite3.connect(Path(temporary.name)/'audit.sqlite');index.execute('PRAGMA cache_size=-4096')
        audit.initialize(index);index.execute('CREATE TABLE ordering(sequence INTEGER PRIMARY KEY,kind TEXT,payload TEXT)')
        total=0
        for kind in TABLES:
            digest=hashlib.sha256();n=0
            width=max(1,proof['tables'][kind]['max_bytes'])
            result=db.execute(f'SELECT * FROM lake.{schema}.{kind} AT (VERSION => {snapshot}) ORDER BY sequence')
            for batch in result.fetch_record_batch(min(limits.max_batch_rows,max(1,limits.max_batch_bytes//width))):
                budget.check()
                for flat in batch.to_pylist():
                    item=restore(kind,flat)
                    if item.result_id!=result_id:raise ValueError('trend_read_result')
                    payload=encode(flat);budget.charge(payload.encode());budget.refs(reference_count(item));audit.append(index,item)
                    index.execute('INSERT INTO ordering VALUES (?,?,?)',(flat['sequence'],kind,payload))
                    digest.update(bytes.fromhex(flat['row_sha256']));n+=1
            if (n,digest.hexdigest())!=(proof['tables'][kind]['rows'],proof['tables'][kind]['sha256']):raise ValueError('trend_read_digest')
            total+=n
        if total!=proof['output_rows']:raise ValueError('trend_read_total')
        audit.validate(index,proof['events'],proof['input_rows'],budget.step)
        combined=hashlib.sha256()
        for seq,(number,kind,payload) in enumerate(index.execute('SELECT * FROM ordering ORDER BY sequence')):
            if seq!=number:raise ValueError('trend_read_sequence')
            flat=decode(payload);combined.update(bytes.fromhex(flat['row_sha256']))
        if combined.hexdigest()!=proof['output_sha256']:raise ValueError('trend_read_ordered_digest')
        for _,kind,payload in index.execute('SELECT * FROM ordering ORDER BY sequence'):
            budget.check();yield restore(kind,decode(payload))
    except BaseException as e:error=e;raise
    finally:
        actions=(lambda:index.close() if index is not None else None,lambda:temporary.cleanup() if temporary is not None else None,lambda:budget.scratch_roots.discard(Path(temporary.name)) if temporary is not None else None,lambda:db.close() if db is not None else None)
        # GeneratorExit本身不会掩盖清理失败。
        cleanup(actions,None if isinstance(error,GeneratorExit) else error)


def audit_lake(dsn,schema,snapshot,root,proof,result_id,limits,budget):
    with closing(_read_audited(dsn,schema,snapshot,root,proof,result_id,limits,budget)) as source:
        for _ in source:pass
    db=lake_connect(dsn)
    try:return layout(db,schema,snapshot,root,proof)
    finally:db.close()


@contextmanager
def verify_trend(binding,proof,runtime,*,lock=False,guard=lambda:None):
    from data_pipeline.analysis.country_trends.snapshot_store import TrendBinding
    if type(binding) is not TrendBinding or binding.schema_version!=VERSION or binding.profile!=PROFILE or not binding.component_id.isalnum() or binding.schema_name!='trend_'+binding.component_id:raise ValueError('trend_binding_version')
    root=Path(binding.root)
    if not root.is_absolute() or root.resolve()!=root or not root.is_relative_to(Path(runtime.private_root).resolve()):raise ValueError('trend_binding_root')
    if database_identity(runtime.component_dsn)!={k:getattr(binding,k) for k in ('system_id','database_oid')}:raise ValueError('trend_binding_database')
    if file_hash(root/'manifest.json',guard)!=binding.manifest_sha256:raise ValueError('trend_manifest_hash')
    m=json.loads((root/'manifest.json').read_text())
    if (m['schema_version'],m['profile'],m['codec'],m['component_id'],m['schema_name'],m['snapshot'],m['root'],m['result_id'])!=(VERSION,PROFILE,CODEC,binding.component_id,binding.schema_name,binding.snapshot,binding.root,binding.result_id):raise ValueError('trend_manifest_identity')
    if m['schemas']!={k:[list(p) for p in v] for k,v in SCHEMAS.items()} or set(m['proof']['tables'])!=set(TABLES) or m['proof']!=proof:raise ValueError('trend_manifest_schema_proof')
    if m['identity']!={k:getattr(binding,k) for k in ('system_id','database_oid','catalog_id')}:raise ValueError('trend_manifest_database')
    if json.loads((root/'ready.json').read_text())!=dict(binding=asdict(binding),proof=proof):raise ValueError('trend_ready')
    if binding.result_id!='trend_'+hashlib.sha256(json_encode(m['input_identity']).encode()).hexdigest():raise ValueError('trend_input_identity')
    if tuple(m['input_identity']['rules'])!=RULES or m['input_identity']['profile']!=PROFILE:raise ValueError('trend_input_rules')
    if {str(p) for p in (root/'data').glob('*.parquet')}!={f['path'] for f in m['files']}:raise ValueError('trend_file_set')
    for f in m['files']:
        guard()
        if Path(f['path']).resolve().parent!=root/'data' or stamp(f['path'])!=f['stamp']:raise ValueError('trend_file_entity')
    pg=psycopg2.connect(runtime.component_dsn);error=None
    try:
        pg.set_session(readonly=not lock)
        with pg.cursor() as c:
            c.execute('SELECT catalog_id FROM country_trends.catalog WHERE singleton'+(' FOR SHARE' if lock else ''))
            if c.fetchone()!=(binding.catalog_id,):raise ValueError('trend_catalog')
            c.execute('SELECT state,root,schema_name,binding,proof FROM country_trends.components WHERE component_id=%s'+(' FOR SHARE' if lock else ''),(binding.component_id,))
            if c.fetchone()!=('complete',binding.root,binding.schema_name,json_encode(asdict(binding)),json_encode(proof)):raise ValueError('trend_control_anchor')
        with verify_inputs(m['input_identity']['anchor'],runtime,lock=lock,guard=guard):yield m
    except BaseException as e:error=e;raise
    finally:cleanup((pg.rollback,pg.close),None if isinstance(error,GeneratorExit) else error)


def inspect_witnesses(db,binding,guard,budget=None,*,scratch=None):
    """只读原资格相关typed行；旧v1不按版本一刀切，逐事件核实际见证。"""
    from data_pipeline.analysis.country_trends.snapshot_store import SharedBudget, S2Limits
    from data_pipeline.analysis.country_trends.snapshot_qualification import validate_unknown_witnesses
    from data_pipeline.analysis.country_events import snapshot_schema as c3
    budget=budget or SharedBudget(S2Limits(),Path(binding.root),guard)
    temporary=None
    if scratch is None:
        temporary=tempfile.TemporaryDirectory(prefix='country-trend-witness-')
        budget.scratch_roots.add(Path(temporary.name))
    try:
        if temporary is not None:db.execute('SET temp_directory='+literal(Path(temporary.name)/'duck-temp'))
        result=db.execute(f"SELECT * FROM lake.{binding.schema_name}.raw_source AT (VERSION => {binding.snapshot}) WHERE incident IS NOT NULL AND source_table IN (?,?,?) ORDER BY incident,revision,sequence",[encode(v) for v in ('event_status','boundary_unavailable','metric_point')])
        event=None;values=[];size=0
        for batch in result.fetch_record_batch(1):
            for flat in batch.to_pylist():
                guard();row=restore('raw_source',flat)
                payload=row.get('raw_typed');budget.charge(payload.encode())
                incident,revision,value=c3.decode(payload)
                if row.event!=(incident,revision) or c3.BY_CLASS.get(type(value))!=row.get('source_table'):raise ValueError('trend_witness_outer_event')
                if event is not None and row.event!=event:
                    validate_unknown_witnesses(event,values);values=[];size=0
                event=row.event;size+=len(payload.encode())
                if size>budget.limits.max_event_bytes:raise ValueError('trend_witness_event_budget')
                values.append(value)
        if event is not None:validate_unknown_witnesses(event,values)
    finally:
        if temporary is not None:cleanup((temporary.cleanup,lambda:budget.scratch_roots.discard(Path(temporary.name))),sys.exc_info()[1])


def inspect_trend(binding,proof,*,runtime,guard=lambda:None,budget=None):
    with verify_trend(binding,proof,runtime,guard=guard) as manifest:
        for f in manifest['files']:
            if file_hash(f['path'],guard)!=f['sha256']:raise ValueError('trend_file_hash')
        db=lake_connect(runtime.component_dsn)
        try:
            layout(db,binding.schema_name,binding.snapshot,Path(binding.root),proof,manifest['lake_files'])
            inspect_witnesses(db,binding,guard,budget)
        finally:cleanup((db.close,),sys.exc_info()[1])
    return json.loads(json_encode(dict(binding=asdict(binding),proof=proof,manifest=manifest)))


class TrendReader:
    def __init__(self,binding,proof,*,runtime,limits=None,guard=lambda:None):
        from data_pipeline.analysis.country_trends.snapshot_store import S2Limits, SharedBudget
        self.binding,self.proof,self.runtime=binding,proof,runtime
        self.limits=limits or S2Limits()
        if any(type(v) is not int or v<1 for v in asdict(self.limits).values()) or self.limits.max_batch_rows>256:raise ValueError('trend_batch_limit')
        self.budget=SharedBudget(self.limits,Path(binding.root),guard)
        self.descriptor=inspect_trend(binding,proof,runtime=runtime,guard=self.budget.check,budget=self.budget)
        self.manifest=self.descriptor['manifest']
        self.stats=dict(full_scans=0,pages=0)

    def scan(self):
        self.stats['full_scans']+=1;b=self.binding
        scope=json_encode(dict(binding=asdict(b),mode='full',version=VERSION))
        with verify_trend(b,self.proof,self.runtime,guard=self.budget.check):
            with closing(_read_audited(self.runtime.component_dsn,b.schema_name,b.snapshot,Path(b.root),self.proof,b.result_id,self.limits,self.budget)) as stream:
                yield from stream
        with verify_trend(b,self.proof,self.runtime,guard=self.budget.check):pass
        yield TrendReadReceipt(b,scope,self.proof['output_rows'],self.proof['output_sha256'],True)

    def query(self,kind,*,event=None,key=None,cursor=None,limit=100):
        if kind not in TABLES or type(limit) is not int or not 1<=limit<=self.limits.max_batch_rows:raise ValueError('trend_query_scope')
        if event is not None and (type(event) is not tuple or len(event)!=2 or type(event[0]) is not str or type(event[1]) is not int):raise ValueError('trend_query_event')
        if key is not None and type(key) is not tuple:raise ValueError('trend_query_key')
        b=self.binding;scope=json_encode(dict(binding=asdict(b),proof=self.proof,kind=kind,event=event,key=encode(key),version=VERSION));after=-1
        if cursor:
            try:
                v=json.loads(base64.urlsafe_b64decode(cursor))
                if set(v)!={'scope','after'} or v['scope']!=scope or type(v['after']) is not int or v['after']<0:raise ValueError()
                after=v['after']
            except Exception as e:raise ValueError('trend_cursor_mismatch') from e
        self.stats['pages']+=1
        # 湖表过滤/排序的保守访问上界，按全表候选计费，不以返回一行冒称一步。
        self.budget.index_steps+=self.proof['tables'][kind]['rows']
        self.budget.step()
        with verify_trend(b,self.proof,self.runtime,guard=self.budget.check):
            temporary=tempfile.TemporaryDirectory(prefix='country-trend-query-')
            self.budget.scratch_roots.add(Path(temporary.name));db=None
            try:
                db=lake_connect(self.runtime.component_dsn)
                db.execute('SET temp_directory='+literal(Path(temporary.name)/'duck-temp'))
                layout(db,b.schema_name,b.snapshot,Path(b.root),self.proof,self.manifest['lake_files'],tuple(dict.fromkeys((kind,'event'))) if event is not None else (kind,))
                if event is not None:
                    found=db.execute(f'SELECT count(*) FROM lake.{b.schema_name}.event AT (VERSION => {b.snapshot}) WHERE incident=? AND revision=?',[event[0],str(event[1])]).fetchone()[0]
                    if found!=1:raise ValueError('trend_unknown_event_revision')
                clauses=[];args=[]
                if event is not None:clauses+=['incident=?','revision=?'];args+=[event[0],str(event[1])]
                if key is not None:clauses+=['key_typed=?'];args+=[encode(key)]
                where=' AND '.join(clauses) or 'true'
                total=db.execute(f'SELECT count(*) FROM lake.{b.schema_name}.{kind} AT (VERSION => {b.snapshot}) WHERE {where}',args).fetchone()[0]
                result=db.execute(f'SELECT * FROM lake.{b.schema_name}.{kind} AT (VERSION => {b.snapshot}) WHERE {where} AND sequence>? ORDER BY sequence LIMIT ?',[*args,after,limit+1])
                output=[];size=0;more=False;last=after
                width=max(1,self.proof['tables'][kind]['max_bytes'])
                for batch in result.fetch_record_batch(min(limit,max(1,self.limits.max_batch_bytes//width))):
                    for flat in batch.to_pylist():
                        item=restore(kind,flat);payload=encode(flat).encode();self.budget.charge(payload)
                        # 与全读同口径：已解码的lookahead也计费，跨页复用同一预算。
                        self.budget.refs(reference_count(item))
                        if output and (len(output)>=limit or size+len(payload)>self.limits.max_batch_bytes):more=True;break
                        if len(payload)>self.limits.max_batch_bytes:raise ValueError('trend_page_bytes')
                        output.append(item);size+=len(payload);last=flat['sequence']
                    if more:break
            finally:
                cleanup((lambda:db.close() if db is not None else None,temporary.cleanup,lambda:self.budget.scratch_roots.discard(Path(temporary.name))),sys.exc_info()[1])
        with verify_trend(b,self.proof,self.runtime,guard=self.budget.check):pass
        next_cursor=base64.urlsafe_b64encode(json_encode(dict(scope=scope,after=last)).encode()).decode() if more else None
        return TrendPage(tuple(output),total,next_cursor,scope)

    def iter_query(self,kind,*,event=None,key=None,page_size=100):
        cursor=None;count=0;digest=hashlib.sha256();scope=None;total=None
        while True:
            page=self.query(kind,event=event,key=key,cursor=cursor,limit=page_size)
            if scope is None:scope,total=page.scope,page.total
            if (scope,total)!=(page.scope,page.total):raise ValueError('trend_page_scope_drift')
            for row in page.items:count+=1;digest.update(bytes.fromhex(row_hash(row)));yield row
            cursor=page.next_cursor
            if cursor is None:break
        if count!=total:raise ValueError('trend_page_total')
        with verify_trend(self.binding,self.proof,self.runtime,guard=self.budget.check):pass
        yield TrendReadReceipt(self.binding,scope,count,digest.hexdigest(),False)
