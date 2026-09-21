"""S2独立趋势湖组件；PG只有目录/资格，历史主体逐字段Parquet。"""
from collections import Counter
from contextlib import closing
from dataclasses import dataclass,asdict,replace
from itertools import chain
from pathlib import Path
from types import SimpleNamespace
import hashlib,json,sqlite3,time,uuid
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from data_pipeline.analysis.country_trends.contract import Limits, TrendRow, row
from data_pipeline.analysis.country_trends.compute import Budget, event_rows, graph_rows, rss
from data_pipeline.analysis.country_trends.snapshot_schema import VERSION, PROFILE, CODEC, RULES, TABLES, SCHEMAS, encode, decode, flatten, reference_count
from data_pipeline.analysis.country_trends import snapshot_audit as audit
from data_pipeline.analysis.country_trends.snapshot_qualification import validate_unknown_witnesses
from data_pipeline.analysis.country_trends.snapshot_inputs import strict_country, feature_context, reference_context, verify_inputs
from data_pipeline.analysis.country_events.snapshot_store import database_identity, lake_connect, cleanup, fsync_tree, write_json
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch, ReadReceipt
from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_schema as c3
from data_pipeline.analysis.country_events.selection_contract import contract_json
from data_pipeline.results.manifest_io import encode as json_encode, file_hash, stamp
from data_pipeline.bgp.archive.store import literal


@dataclass(frozen=True)
class S2Limits(Limits):
    max_total_bytes: int=8*1024**3
    max_references: int=1000000
    max_events: int=10000
    max_index_steps: int=10000000


@dataclass(frozen=True)
class TrendBinding:
    system_id: str
    database_oid: int
    catalog_id: str
    component_id: str
    result_id: str
    schema_name: str
    snapshot: int
    root: str
    manifest_sha256: str
    schema_version: str=VERSION
    profile: str=PROFILE


class SharedBudget(Budget):
    def __init__(self,limits,root,guard):
        self.root=Path(root);self.scratch_roots=set();self.byte_count=0;self.references=0;self.index_steps=0;self.stats=Counter()
        super().__init__(limits,guard)
    def charge(self,payload):
        self.byte_count+=len(payload)
        if self.byte_count>self.limits.max_total_bytes:raise ValueError('trend_total_bytes')
        super().charge(payload)
    def refs(self,count):
        self.references+=count
        if self.references>self.limits.max_references:raise ValueError('trend_reference_budget')
    def step(self):
        self.index_steps+=1
        if self.index_steps>self.limits.max_index_steps:raise ValueError('trend_index_budget')
        self.check()
    def check(self):
        super().check()
        size=sum(p.stat().st_size for directory in (self.root,*self.scratch_roots) if directory.exists() for p in directory.rglob('*') if p.is_file())
        self.stats['disk_peak_bytes']=max(self.stats['disk_peak_bytes'],size)
        self.stats['process_cumulative_peak_rss_bytes']=rss()
        if size>self.limits.max_disk_bytes:raise ValueError('trend_total_disk')


def code_binding():
    base=Path(__file__).resolve().parents[3]
    paths=[*sorted((base/'data_pipeline/analysis/country_trends').glob('*.py')),*sorted((base/'data_pipeline/analysis/country_events').glob('*.py')),base/'pyproject.toml',base/'uv.lock',base.parent/'frontend/package-lock.json']
    return {str(p.relative_to(base.parent)):file_hash(p) for p in paths}


def batches(db,query,args,limits,guard):
    batch=[];size=0
    for payload, in db.execute(query,args):
        guard();n=len(payload.encode())
        if n>limits.max_row_bytes or n>limits.max_batch_bytes:raise ValueError('trend_batch_row_bytes')
        if batch and (len(batch)>=limits.max_batch_rows or size+n>limits.max_batch_bytes):yield batch;batch=[];size=0
        batch.append(decode(payload));size+=n
    if batch:yield batch


def connection_schema():
    return '''CREATE TABLE output(sequence INTEGER PRIMARY KEY,kind TEXT,payload TEXT);
    CREATE INDEX output_kind ON output(kind,sequence);
    CREATE TABLE input(sequence INTEGER PRIMARY KEY,event TEXT,revision INTEGER,source_table TEXT,payload TEXT,bytes INTEGER);
    CREATE INDEX input_event ON input(event,revision,sequence);'''


def prepare_trend(descriptor,runtime,output,*,feature_receipt=None,feature_selections=(),reference_binding=None,rules=RULES,limits=S2Limits(),guard=lambda:None):
    """正式入口只接受经公开资格核对的strict绑定，不接受任意人工行流。"""
    from data_pipeline.analysis.country_trends.execution_identity import execution_identity
    execution=execution_identity()
    if rules!=RULES:raise ValueError('trend_rule_version')
    root=Path(output).resolve()
    if not root.is_relative_to(Path(runtime.private_root).resolve()):raise ValueError('trend_private_output')
    if any(type(v) is not int or v<1 for v in asdict(limits).values()) or limits.max_batch_rows>256:raise ValueError('trend_batch_limit')
    root.mkdir(parents=True,exist_ok=False)
    budget=SharedBudget(limits,root,guard);started=time.monotonic();stages={};db=lake=None
    component=uuid.uuid4().hex;schema='trend_'+component;registered=False
    fixed_code=code_binding()
    frozen_identity={k:v for k,v in execution.items() if k!='execution_binding'}
    try:
        country=strict_country(descriptor,runtime,budget.check)
        f=None;activities=();context_sources=[]
        if feature_receipt is not None:
            if runtime.feature_dsn is None:raise ValueError('trend_feature_runtime')
            f,activities,src=feature_context(feature_receipt,feature_selections,runtime,budget.check,budget);context_sources.extend(src)
        elif feature_selections:raise ValueError('trend_feature_not_provided')
        refs=()
        if reference_binding is not None:
            if runtime.reference_dsn is None:raise ValueError('trend_reference_runtime')
            refs,src=reference_context(reference_binding,runtime,budget.check);context_sources.extend(src)
            budget.refs(sum(len(r.projections) for r in refs))
        context_size=len(encode((activities,refs,tuple(context_sources))).encode())
        if context_size>limits.max_context_bytes:raise ValueError('trend_context_budget')
        for value in (*activities,*refs):budget.charge(encode(value).encode())
        anchor=json.loads(json_encode(dict(profile=PROFILE,country=country,feature=f,reference=reference_binding,feature_selections=feature_selections)))
        metadata_bytes=json_encode(anchor).encode()
        if context_size+len(metadata_bytes)>limits.max_context_bytes:raise ValueError('trend_context_binding_budget')
        budget.charge(metadata_bytes)
        stages['input_admission_seconds']=time.monotonic()-started
        identity=database_identity(runtime.component_dsn)
        pg=psycopg2.connect(runtime.component_dsn)
        try:
            with pg,pg.cursor() as c:
                c.execute('CREATE SCHEMA IF NOT EXISTS country_trends')
                c.execute('CREATE TABLE IF NOT EXISTS country_trends.catalog(singleton BOOLEAN PRIMARY KEY CHECK(singleton),catalog_id TEXT NOT NULL)')
                c.execute('INSERT INTO country_trends.catalog VALUES (true,%s) ON CONFLICT DO NOTHING',(uuid.uuid4().hex,))
                c.execute('SELECT catalog_id FROM country_trends.catalog WHERE singleton');identity['catalog_id']=c.fetchone()[0]
                c.execute('''CREATE TABLE IF NOT EXISTS country_trends.components(component_id TEXT PRIMARY KEY,state TEXT NOT NULL CHECK(state IN ('candidate','failed','complete','revoked')),root TEXT NOT NULL,schema_name TEXT NOT NULL,binding TEXT,proof TEXT,reason TEXT)''')
                c.execute("INSERT INTO country_trends.components(component_id,state,root,schema_name) VALUES (%s,'candidate',%s,%s)",(component,str(root),schema))
            registered=True
        finally:pg.close()
        db=sqlite3.connect(root/'work.sqlite');db.execute('PRAGMA cache_size=-4096');db.execute('PRAGMA temp_store=FILE')
        db.executescript(connection_schema());audit.initialize(db)
        before=time.monotonic();receipt=completion=None;input_count=0;input_sha=hashlib.sha256()
        # 公开Reader暴露的workdir纳入同一磁盘计数，不给C3排序另开额度。
        readers=[]
        budget.scratch_roots.add(Path(descriptor.read_binding.component.root))
        def country_guard():
            if readers and readers[0].workdir is not None:budget.scratch_roots.add(Path(readers[0].workdir))
            budget.check()
        c3_limits=dict(batch_rows=limits.max_batch_rows,batch_bytes=limits.max_batch_bytes,max_row_bytes=limits.max_row_bytes,max_rows=limits.max_rows,max_rss_bytes=limits.max_rss_bytes,max_disk_bytes=limits.max_disk_bytes,guard=country_guard)
        reader=ComponentReader(runtime.country.component_dsn,descriptor.read_binding.component,c1=runtime.country.c1,**c3_limits);readers.append(reader)
        with verify_inputs(anchor,runtime,guard=budget.check):
            with closing(reader.stream()) as source:
                for batch in source:
                    if receipt is not None:raise ValueError('trend_after_country_receipt')
                    if type(batch) is ReadReceipt:receipt=batch;continue
                    if type(batch) is not ReadBatch:raise ValueError('trend_country_batch')
                    for item in batch.rows:
                        if type(item) is c2.C2Completion:
                            if completion is not None:raise ValueError('trend_country_duplicate_completion')
                            completion=item;event=None;revision=None;payload=c3.encode(item);table='c2_completion'
                        elif type(item) is c2.C2Row:
                            event,revision=item.incident_id,item.revision;payload=c3.encode((event,revision,item.value));table=c3.BY_CLASS[type(item.value)]
                        else:raise ValueError('trend_country_row')
                        encoded=payload.encode();budget.charge(encoded)
                        input_sha.update(len(encoded).to_bytes(8,'big'));input_sha.update(encoded)
                        db.execute('INSERT INTO input VALUES (?,?,?,?,?,?)',(input_count,event,revision,table,payload,len(encoded)));input_count+=1
        if receipt is None or completion is None or receipt.binding!=descriptor.read_binding.component or receipt.rows!=input_count or receipt.mode!='component' or not receipt.full_body_validated or tuple(receipt.scope)!=completion.scope:raise ValueError('trend_country_incomplete')
        if c3.encode(completion)!=descriptor.completion_typed:raise ValueError('trend_country_completion')
        budget.stats['country_stream_calls']=1
        stages['country_stream_seconds']=time.monotonic()-before
        event_keys=tuple((a,b) for a,b in db.execute("SELECT event,revision FROM input WHERE event IS NOT NULL AND source_table='event_status' GROUP BY event,revision ORDER BY event,revision"))
        if len(event_keys)!=completion.event_count or len(event_keys)>limits.max_events:raise ValueError('trend_event_budget_count')
        if any(w.event not in event_keys for w in (*activities,*refs)):raise ValueError('trend_context_event')
        for event,country,mode in feature_selections:
            hit=db.execute("SELECT payload FROM input WHERE event=? AND revision=? AND source_table='event_status'",event).fetchone()
            if hit is None or c3.decode(hit[0])[2].incident.country!=country:raise ValueError('trend_feature_event_country')
        input_identity=dict(profile=PROFILE,schema=VERSION,codec=CODEC,rules=rules,denominator_rule='strict_c3_unknown_denominator/v1',code=fixed_code,frozen_identity=frozen_identity,anchor=anchor,country_descriptor=contract_json(descriptor),receipt=asdict(receipt),input_sha256=input_sha.hexdigest(),input_rows=input_count,events=event_keys,contexts=encode((activities,refs)))
        result_id='trend_'+hashlib.sha256(json_encode(input_identity).encode()).hexdigest()
        count=0;counts=Counter();hashes={k:hashlib.sha256() for k in TABLES};widths=Counter();overall=hashlib.sha256()
        def emit(item):
            nonlocal count
            item=replace(item,result_id=result_id)
            flat=flatten(count,item);payload=encode(flat);budget.charge(payload.encode());budget.refs(reference_count(item))
            if len(payload.encode())>limits.max_batch_bytes:raise ValueError('trend_output_row_bytes')
            audit.append(db,item)
            db.execute('INSERT INTO output VALUES (?,?,?)',(count,item.kind,payload))
            hashes[item.kind].update(bytes.fromhex(flat['row_sha256']));overall.update(bytes.fromhex(flat['row_sha256']))
            counts[item.kind]+=1;widths[item.kind]=max(widths[item.kind],len(payload.encode()));count+=1
        for seq,event,rev,table,payload,_ in db.execute('SELECT * FROM input ORDER BY sequence'):
            emit(row('raw_source',() if event is None else (event,rev),(seq,),source_sequence=seq,source_table=table,raw_typed=payload))
        for source,role,raw in dict.fromkeys(context_sources):emit(row('context_source',(),(source,),source_ref=source,role=role,raw_typed=raw))
        emit(row('result',(),state='admitted_empty' if not event_keys else 'complete',country_result_id=descriptor.read_binding.result_id,country_receipt=asdict(receipt),input_sha256=input_sha.hexdigest(),event_count=len(event_keys),feature_state='not_provided' if f is None else 'bound',reference_state='not_provided' if reference_binding is None else 'bound_artificial_history_unknown'))
        for seq,payload in db.execute("SELECT sequence,payload FROM input WHERE event IS NULL AND source_table IN ('input_evidence','observation_fact') ORDER BY sequence"):
            emit(row('source_evidence',(),(seq,),raw_typed=payload))
        before=time.monotonic();request=SimpleNamespace(activities=activities,references=refs)
        for event,revision in event_keys:
            size=db.execute('SELECT sum(bytes) FROM input WHERE event=? AND revision=?',(event,revision)).fetchone()[0]
            if size>limits.max_event_bytes:raise ValueError('trend_event_memory_budget')
            values=[];qualifier_sequences=[];metric_sequences={};status_sequences=[]
            for seq,table,payload in db.execute('SELECT sequence,source_table,payload FROM input WHERE event=? AND revision=? ORDER BY sequence',(event,revision)):
                original=c3.decode(payload)[2];values.append(original)
                if table=='event_status':status_sequences.append(seq)
                elif table=='boundary_unavailable':qualifier_sequences.append(seq)
                elif table=='metric_point':metric_sequences.setdefault(original.metric,[]).append(seq)
            validate_unknown_witnesses((event,revision),values)
            for scientific in event_rows((event,revision),values,request,limits,denominator_rule='strict_c3_unknown_denominator/v1'):
                if scientific.kind=='event':scientific=replace(scientific,values=tuple((k,'strict_c3_calculation' if k=='basis' else v) for k,v in scientific.values))
                if scientific.kind=='activity_context' and f is not None:
                    scientific=replace(scientific,values=(('state','unavailable'),('reason','no_matching_saved_country_window')))
                for item in chain((scientific,),graph_rows(scientific,input_condition='strict_c3_original_qualification')):
                    emit(item)
                    if item.kind=='evidence':
                        # 正式资格与原target导航；不改科学行/原C3值。
                        original_refs=chain(status_sequences,qualifier_sequences if scientific.kind=='profile' else (),metric_sequences.get(scientific.key[0],()) if scientific.kind=='profile' else ())
                        for seq in original_refs:
                            emit(row('evidence_source',(event,revision),(item.key[0],'original',seq),source_locator=('raw_source',(event,revision),(seq,))))
            del values
        audit.validate(db,len(event_keys),input_count,budget.step);db.commit()
        stages['science_and_relations_seconds']=time.monotonic()-before
        proof=dict(version=VERSION,result_id=result_id,input_rows=input_count,events=len(event_keys),output_rows=count,output_sha256=overall.hexdigest(),country_stream_calls=1,tables={k:dict(rows=counts[k],sha256=hashes[k].hexdigest(),max_bytes=widths[k]) for k in TABLES})
        before=time.monotonic();lake=lake_connect(runtime.component_dsn,root);lake.execute('CREATE SCHEMA lake.'+schema)
        data=root/'data';data.mkdir()
        for kind in TABLES:
            budget.check();cols=','.join('"'+n+'" '+t for n,t in SCHEMAS[kind])
            lake.execute(f'CREATE TABLE lake.{schema}.{kind} ({cols})')
            arrow=pa.schema([(n,pa.int64() if t=='BIGINT' else pa.string()) for n,t in SCHEMAS[kind]])
            path=data/(kind+'.parquet')
            with pq.ParquetWriter(path,arrow,compression='zstd') as writer:
                for batch in batches(db,'SELECT payload FROM output WHERE kind=? ORDER BY sequence',(kind,),limits,budget.check):
                    writer.write_table(pa.Table.from_pylist(batch,schema=arrow));budget.stats['row_groups']+=1
            if counts[kind]:lake.execute("CALL ducklake_add_data_files('lake',"+literal(kind)+','+literal(path)+',schema=>'+literal(schema)+')')
        snapshot=lake.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        lake.close();lake=None
        stages['parquet_write_seconds']=time.monotonic()-before
        # 全新湖连接、独立关系暂存，核原typed列及全表摘要，不能用writer计数冒充回读。
        from data_pipeline.analysis.country_trends.snapshot_reader import audit_lake
        before=time.monotonic();lake_files=audit_lake(runtime.component_dsn,schema,snapshot,root,proof,result_id,limits,budget)
        stages['independent_readback_seconds']=time.monotonic()-before
        db.close();db=None;(root/'work.sqlite').unlink();fsync_tree(root)
        files=[dict(path=str(p),stamp=stamp(p),sha256=file_hash(p,budget.check)) for p in sorted(data.glob('*.parquet'))]
        if code_binding()!=fixed_code or {k:v for k,v in execution_identity().items() if k!='execution_binding'}!=frozen_identity:raise ValueError('trend_code_drift')
        execution=execution_identity()
        with verify_inputs(anchor,runtime,guard=budget.check):pass
        manifest=dict(schema_version=VERSION,profile=PROFILE,codec=CODEC,schemas=SCHEMAS,identity=identity,component_id=component,schema_name=schema,snapshot=snapshot,root=str(root),result_id=result_id,input_identity=input_identity,proof=proof,files=files,lake_files=lake_files,stages=stages,execution=execution,costs={**budget.stats,'charged_rows':budget.rows,'charged_bytes':budget.byte_count,'references':budget.references,'rss_scope':'process_cumulative_peak_including_imports_and_inputs','wall_seconds':time.monotonic()-started})
        write_json(root/'manifest.json',manifest)
        binding=TrendBinding(**identity,component_id=component,result_id=result_id,schema_name=schema,snapshot=snapshot,root=str(root),manifest_sha256=file_hash(root/'manifest.json'))
        write_json(root/'ready.json',dict(binding=asdict(binding),proof=proof));fsync_tree(root);budget.check()
        # 所有写/校验连接均已关闭。最后独立控制事务才登记完成，资格锁覆盖提交。
        with verify_inputs(anchor,runtime,lock=True,guard=budget.check):
            pg=psycopg2.connect(runtime.component_dsn)
            try:
                with pg,pg.cursor() as c:
                    c.execute("UPDATE country_trends.components SET state='complete',binding=%s,proof=%s WHERE component_id=%s AND state='candidate' AND root=%s AND schema_name=%s",(json_encode(asdict(binding)),json_encode(proof),component,str(root),schema))
                    if c.rowcount!=1:raise ValueError('trend_control_changed')
            finally:pg.close()
        return binding,proof
    except BaseException as error:
        def failed():
            if registered:
                pg=psycopg2.connect(runtime.component_dsn)
                try:
                    with pg,pg.cursor() as c:c.execute("UPDATE country_trends.components SET state='failed',binding=NULL,proof=NULL,reason=%s WHERE component_id=%s AND state IN ('candidate','complete')",(type(error).__name__+':'+str(error),component))
                finally:pg.close()
        cleanup((failed,),error);raise
    finally:
        import sys
        cleanup((lambda:lake.close() if lake is not None else None,lambda:db.close() if db is not None else None),sys.exc_info()[1])
