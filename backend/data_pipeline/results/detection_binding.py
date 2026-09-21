"""Detection固定组件校验；显式独立输出/观察库，凭据不进入清单。"""
from contextlib import contextmanager,ExitStack
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import psycopg2
from data_pipeline.bgp.archive.message_reader import ObservationReader, SourceEnd, MessageBatch
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.analysis.detection.store import read_stored_rows, COLUMNS
from data_pipeline.results import resource_feature_bindings as bindings
from data_pipeline.results.manifest_io import require, encode, file_hash

ROLES={'as_info','important_as_dict','prefix_info','triplet_info','country','important_prefix_v4',
       'important_prefix_v6','as_prefix_dict','as_rel_dict','important_domain_dict','private_as_dict'}


class MessageProof:
    """按来源物理顺序核验完整消息；仅保存每源计数和摘要，不保存消息列表。"""
    version='observation-source-messages/v1'

    def __init__(self,sources):
        self.sources=list(sources)
        self.counts={s:0 for s in sources}
        self.hashes={s:hashlib.sha256() for s in sources}
        self.order=hashlib.sha256();self.total=0

    def add(self,message):
        source=message.get('source_id');require(source in self.counts,'消息来源不在固定观察范围')
        ordinal=self.counts[source]
        require(type(message.get('record')) is int and message['record']==ordinal and
                message.get('message_id')==f'{source}:{ordinal}','消息物理身份/顺序重复或缺失')
        # Reader内这三组是无序聚合；排序只用于校验，不改动保存的原JSON或消息顺序。
        value=dict(message)
        for key in ('peers','quality','eor'):
            require(isinstance(value.get(key),list),'消息关联集合缺失')
            value[key]=sorted(value[key],key=encode)
        encoded=(encode(value)+'\n').encode()
        self.hashes[source].update(encoded);self.order.update(encoded);self.counts[source]+=1;self.total+=1

    def result(self):
        return {'version':self.version,'total':self.total,'ordered_sha256':self.order.hexdigest(),
                'sources':[{'source_id':s,'messages':self.counts[s],'sha256':self.hashes[s].hexdigest()} for s in self.sources]}


def require_message_proof(binding):
    proof=binding.get('source_messages')
    require(isinstance(proof,dict) and proof.get('version')==MessageProof.version,'消息覆盖证明缺失，Q2须重新准入')
    return proof


def run_row(c,run,lock=False):
    c.execute('SELECT state,schema_name,snapshot,scope,identity FROM detection.runs WHERE run_id=%s'+(' FOR SHARE' if lock else ''),(run,))
    row=c.fetchone();require(row is not None,'Detection运行不存在')
    return list(row)


def config(p):
    require(isinstance(p.detection,dict) and set(p.detection)=={'output_dsn','observation_dsn'},'必须显式绑定Detection输出/观察库')
    return p.detection


def inspect(p,ready):
    cfg=config(p);path=Path(ready).resolve();require(path.is_relative_to(p.root),'Detection回执越界')
    report=json.loads(path.read_text());run=report['run_id'];snap=report['snapshot'];identity=report['identity'];scope=report['scope']
    require(all(type(report[k]) is int and report[k]>=0 for k in ('records','state_entries')),'Detection完成计数类型非法')
    p.guard(report['records']+report['state_entries'])
    require(report['state']=='ready' and type(snap) is int and run.isascii() and run.isalnum(),'Detection固定ready身份无效')
    require(identity['execution_mode']=='frozen-fresh-process' and identity['store_schema_version']=='detection-typed/v1','Detection非正式冻结/未知版本')
    files={str(path)};layouts=[];q=[]
    with psycopg2.connect(cfg['output_dsn']) as pg,pg.cursor() as c:
        output_identity=bindings.instance(c);actual=run_row(c,run)
        require(actual==['complete','det_'+run,snap,scope,identity],'Detection登记与ready不符')
        layouts.append({'database':'output','catalog':bindings.catalog(c,'public','det_'+run,snap)})
        require({t[1] for t in layouts[-1]['catalog']['tables']}=={'records','state_entries'},'Detection表集不完整')
        for table,key in [('records','records'),('state_entries','state_entries')]:
            c.execute('SELECT count(*) FROM detection.'+table+' WHERE run_id=%s',(run,));require(c.fetchone()[0]==report[key],'Detection PG行数与ready不符')
    obsrun=identity['input_run'];obssnap=identity['input_snapshot'];sources=identity['selected_sources']
    reader=ObservationReader(cfg['observation_dsn'],obsrun,obssnap,sources)
    require(scope['input_version']==f'{obsrun}:{obssnap}' and scope['collector_id']==reader.manifest['collector'],'Detection scope与观察版本不符')
    require([s.role for s in reader.starts]==['baseline']+['update']*(len(sources)-1),'Detection来源必须单基线后接UPDATE')
    refs=identity['reference_sources'];require(set(refs)==ROLES,'Detection须保留11类参考')
    with psycopg2.connect(cfg['observation_dsn']) as pg,pg.cursor() as c:
        input_identity=bindings.instance(c)
        for table in ('domeye.runs','domeye.run_specs','domeye.inputs','domeye.source_receipts','domeye.reference_inputs'):
            rows=bindings.qualification(c,table,obsrun);require(rows,'Detection上游资格缺失');q.append({'table':table,'key':obsrun,'rows':rows})
        layouts.append({'database':'observation','catalog':bindings.catalog(c,'public','r_'+obsrun,obssnap)})
        refrows=q[-1]['rows']
        for ref in refs.values():
            require(ref['snapshot_ref']==scope['input_version'],'Detection参考错版')
            require(any(r['source_id']==ref['source_id'] and r['state']=='validated' and r['row_count']==ref['rows'] for r in refrows),'Detection参考资格/计数不符')
            count=sum(batch.num_rows for batch in reader.reference_batches(ref['source_id']))
            require(count==ref['rows'],'Detection参考实际计数不符')
    ends=[];messages=MessageProof(sources)
    for record in reader.stream():
        p.guard()
        if isinstance(record,SourceEnd):ends.append(asdict(record))
        if isinstance(record,MessageBatch):
            for message in record.messages:
                require(message['source_id']==record.source_id,'消息批次来源不符');messages.add(message);p.guard(messages.total)
    require([e['source_id'] for e in ends]==sources,'Detection源流未完整结束')
    for entry in reader.manifest['inputs']:
        require(entry['origin_uri'].startswith('fixture://') and entry['source_id']==source_identity(reader.manifest['collector'],entry['origin_uri'],entry['sha256']),'Detection非人工或原身份不符')
    for entry in [*reader.manifest['inputs'],*reader.manifest.get('references',[])]:
        raw=Path(entry['path']).resolve();require(raw.is_relative_to(p.root),'Detection原件越界')
        require(file_hash(raw,p.guard)==entry['sha256'],'Detection原件SHA不符');files.add(str(raw))
    for layout in layouts:
        for entry in layout['catalog']['files']:
            raw=Path(entry['path']);require(raw.resolve().is_relative_to(p.root) and raw.stat().st_size==entry['bytes'],'Detection湖文件越界/大小不符');files.add(str(raw))
    return {'kind':'detection','run_id':run,'snapshot':snap,'report':report,'output_identity':output_identity,'input_identity':input_identity,
            'actual':actual,'qualifications':q,'layouts':layouts,'source_ends':ends,'source_messages':messages.result(),'files':sorted(files),
            'file_hashes':{f:file_hash(f,p.guard) for f in sorted(files)}}


@contextmanager
def verify(p,binding,lock=False):
    cfg=config(p)
    with ExitStack() as stack:
        for database in ('output','observation'):
            pg=psycopg2.connect(cfg[database+'_dsn']);stack.callback(pg.close)
            # FOR SHARE需要普通事务；不做业务写入。查询才使用只读事务。
            pg.set_session(readonly=not lock)
            c=stack.enter_context(pg.cursor())
            c.execute("SET LOCAL statement_timeout='10s'")
            if lock:c.execute("SET LOCAL lock_timeout='10s'")
            expected=binding['output_identity' if database=='output' else 'input_identity']
            require(bindings.instance(c)==expected,'Detection数据库持久身份漂移')
            if database=='output':
                require(run_row(c,binding['run_id'],lock)==binding['actual'],'Detection完成资格漂移')
                if lock:
                    for table in ('records','state_entries'):
                        c.execute('SELECT count(*) FROM detection.'+table+' WHERE run_id=%s',(binding['run_id'],))
                        require(c.fetchone()[0]==binding['report'][table],'Detection输出登记计数漂移')
            else:
                for q in binding['qualifications']:require(bindings.qualification(c,q['table'],q['key'],lock)==q['rows'],'Detection上游资格漂移')
            for layout in binding['layouts']:
                if layout['database']==database:
                    old=layout['catalog'];require(bindings.catalog(c,old['metadata_schema'],old['schema'],old['snapshot'])==old,'Detection固定catalog漂移')
        yield


def rows(p,binding,table):
    cfg=config(p)
    yield from read_stored_rows(cfg['output_dsn'],binding['run_id'],binding['snapshot'],table,
        batch_rows=p.limits.batch_rows,max_row_bytes=p.limits.max_row_bytes,guard=p.guard)
