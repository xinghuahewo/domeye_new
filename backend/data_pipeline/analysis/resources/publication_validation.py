"""首次完整准入的科学复核，与普通有界读取分离；不重产任何持久科学制品。"""
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
import hashlib
from itertools import chain
import json
import time

import pyarrow as pa
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.ordered_reader import ordered
from data_pipeline.bgp import record_types as o
from data_pipeline.bgp.input.path_decoding import decoding_difference
from data_pipeline.analysis.resources.adapter import reference_projection
from data_pipeline.analysis.resources.compute import ResourceComputer, RibContext, RibElement
from data_pipeline.analysis.resources.references import read_reference, _validate_original, _history, parse_rows, _history_row
from data_pipeline.analysis.resources.store import ResourceStore, TABLES, TYPES
from data_pipeline.analysis.resources.qualification import ALL_TABLES, normalized, validate_relations, check_table_set
from data_pipeline.analysis.resources.publication_codec import typed, digest, CODEC, untyped


def relation(b,t):return f"lake.resource_{b['run_id']}.{t} AT (VERSION => {b['snapshot']})"


@contextmanager
def _cursor(db,sql,args=None,batch_rows=512):
    from data_pipeline.analysis.resources.publication import _close
    cursor=db.execute(sql,args or []).fetch_record_batch(batch_rows);error=None
    try:yield cursor
    except BaseException as exc:error=exc;raise
    finally:_close((cursor.close,),error)


class Account:
    def __init__(self,rt,guard):self.rt=rt;self.guard=guard;self.rows=0;self.bytes=0
    def add(self,row):
        self.guard();size=len(typed(row).encode())
        self.rows+=1;self.bytes+=size


class ResidentAccount(Account):
    """只用于仍整体驻留的集合，不把流式处理总量当工作集。"""
    def add(self,row):
        super().add(row)
        if self.rows>self.rt.max_rows or self.bytes>self.rt.max_bytes:
            raise ValueError('驻留集合行/字节保护；需要分页或外置')


class ScienceRows:
    """同一DuckDB实例的私有临时连接；完整typed值落盘可溢出，Python仅留一批。"""
    def __init__(self,db,rt,guard):
        self.db=db;self.rt=rt;self.guard=guard;self.pending=[];self.size=0
        self.target_bytes=min(1024**2,rt.memory_bytes)
        db.execute('CREATE TEMP TABLE resource_audit_rows(side VARCHAR, name VARCHAR, payload VARCHAR, path_key VARCHAR)')

    def add(self,side,name,row):
        self.guard();text=typed(row);size=len(text.encode())
        if self.pending and (len(self.pending)>=512 or self.size+size>self.target_bytes):self.flush()
        self.pending.append((side,name,text,row['path_digest'] if name=='rendered_paths' else None));self.size+=size
        self.guard()
        if self.size>=self.target_bytes:self.flush()

    def flush(self):
        if not self.pending:return
        self.guard()
        batch=pa.Table.from_pylist([dict(zip(('side','name','payload','path_key'),r)) for r in self.pending],
            schema=pa.schema([(k,pa.string()) for k in ('side','name','payload','path_key')]))
        self.db.register('resource_audit_batch',batch)
        error=None
        try:self.db.execute('INSERT INTO resource_audit_rows SELECT * FROM resource_audit_batch')
        except BaseException as exc:error=exc;raise
        finally:
            from data_pipeline.analysis.resources.publication import _close
            _close((lambda:self.db.unregister('resource_audit_batch'),),error)
        self.pending.clear();self.size=0;self.guard()

    def finish_expected(self):
        self.flush();self.guard()
        collision=self.db.execute("SELECT 1 FROM resource_audit_rows WHERE side='expected' AND name='rendered_paths' GROUP BY path_key HAVING count(DISTINCT payload)>1 LIMIT 1").fetchone()
        self.guard()
        if collision:raise ValueError('路径摘要碰撞')

    def compare(self,name):
        self.flush();self.guard()
        # rendered_paths期望按原path摘要去重；其他表保留全部重复次数。
        mismatch=self.db.execute("""WITH e AS (
            SELECT payload,CASE WHEN name='rendered_paths' THEN 1 ELSE count(*) END AS n
            FROM resource_audit_rows WHERE side='expected' AND name=? GROUP BY name,payload
        ), a AS (SELECT payload,count(*) AS n FROM resource_audit_rows
                 WHERE side='actual' AND name=? GROUP BY payload)
        SELECT 1 FROM e FULL OUTER JOIN a USING(payload)
        WHERE coalesce(e.n,0)<>coalesce(a.n,0) LIMIT 1""",[name,name]).fetchone()
        self.guard()
        if mismatch:raise ValueError('固定原观察科学全值/多重性不符：'+name)
        count=self.db.execute("SELECT count(*) FROM resource_audit_rows WHERE side='actual' AND name=?",[name]).fetchone()[0]
        self.guard();return count


@contextmanager
def science_rows(db,rt,guard):
    # db的当前Arrow扫描不受另一个cursor上的INSERT影响；临时表在私有cursor关闭时释放。
    from data_pipeline.analysis.resources.publication import _close
    scratch=db.cursor();error=None
    try:yield ScienceRows(scratch,rt,guard)
    except BaseException as exc:error=exc;raise
    finally:_close((scratch.close,),error)


def science(rt,b,db,guard,account):
    """输入来自已独立准入的 M2；科学期望不读取输出的计数、normal或拓扑。"""
    m=b['binding'];csv=m['csv_reference'];country=m['country_reference']
    from data_pipeline.analysis.resources.publication import _country
    cb=_country(rt,b);raw=_validate_original(cb)
    expected_reference=[_history_row(r) for r in parse_rows(raw,guard)]
    actual_reference=list(_history(rt.dsn,cb,guard=guard))
    if typed(expected_reference)!=typed(actual_reference):raise ValueError('国家参考原件与历史全值不符')
    countries,identity=read_reference(rt.dsn,country['reference_id'],country['dataset_id'],allow_fixture=rt.fixture_only)
    if identity!=country:raise ValueError('参考科学绑定不符')
    csv_reader=ObservationReader(rt.dsn,csv['run_id'],csv['snapshot'],[csv['anchor_source_id']],profile='observation',guard=guard)
    country_rows=[dict(source_id=country['dataset_id'],row=i,location=asn,raw_row=json.dumps({'__object_pairs__':[['country_cn',v[0]]]},ensure_ascii=False)) for i,(asn,v) in enumerate(countries.items())]
    refs=reference_projection(chain(csv_reader.reference_batches(csv['source_id']),[pa.Table.from_pylist(country_rows)]),csv_source=csv['source_id'],country_source=country['dataset_id'])
    # 使用旧输出映射，但期望内容由固定原输入单独重算；不调用PG state()/finish()。
    sink=ResourceStore.__new__(ResourceStore);sink.tables=TABLES;sink.run_id=b['run_id'];sink.member_sizes={}
    sink.source_bindings={s['context']['source_id']:s for s in m['sources']};sink.binding_manifest=m
    sink.upstream_run=m['sources'][0]['run_id'];sink.upstream_snapshot=m['sources'][0]['snapshot']
    with science_rows(db,rt,guard) as audit:
        def append(t,row):
            projected={name:row.get(name) for name,_ in TABLES[t]}
            projected=pa.Table.from_pylist([projected],schema=pa.schema([(n,TYPES[k]) for n,k in TABLES[t]])).to_pylist()[0];account.add(projected)
            audit.add('expected',t,projected)
        sink.append=append
        computer=ResourceComputer(refs,topology_enabled=m['topology_enabled'])
        input_elements=0
        for source in m['sources']:
            context=RibContext(**{**source['context'],'snapshot_time':datetime.fromisoformat(source['context']['snapshot_time'])})
            sid=context.source_id;reader=ObservationReader(rt.dsn,source['run_id'],source['snapshot'],[sid],profile='observation',guard=guard)
            actual=[None,None,0];ended=[]
            def elements():
                nonlocal input_elements
                stream=ordered(reader);error=None
                try:
                    for item in stream:
                        guard()
                        if isinstance(item,o.SourceEnd):ended.append(item)
                        if not isinstance(item,o.Element):continue
                        row=item.raw;account.add(row);input_elements+=1
                        if row['action']!='rib_snapshot' or row['as_path_text'] is None or row['peer_asn'] is None:raise ValueError('非完整RIB元素')
                        actual[0]=row['epoch'] if actual[0] is None else min(actual[0],row['epoch']);actual[1]=row['epoch'] if actual[1] is None else max(actual[1],row['epoch']);actual[2]+=1
                        diff=decoding_difference(row)
                        if diff:append('decoding_differences',dict(source_id=sid,event_id=row['event_id'],detail=json.dumps(diff,ensure_ascii=False)))
                        yield RibElement(sid,row['message_id'],row['ordinal'],row['prefix'],row['as_path_text'],str(row['peer_asn']),row['peer_ip'],row['bgp_id'] if row['bgp_id_present'] else None,row['path_key'],row['afi'],row['safi'],row['action'],row['attributed_origin_asn'])
                except BaseException as exc:error=exc;raise
                finally:
                    from data_pipeline.analysis.resources.publication import _close
                    _close((stream.close,),error)
            result=computer.compute(context,elements(),decision_sink=sink.decision,membership_sink=sink.members)
            if len(ended)!=1:raise ValueError('RIB未真实耗尽')
            sink.result(result,actual)
        audit.finish_expected()
        for table in TABLES:
            with _cursor(db,'SELECT * FROM '+relation(b,table)+' ORDER BY ALL') as cursor:
                for batch in cursor:
                    for row in batch.to_pylist():
                        account.add(row);audit.add('actual',table,row)
            count=audit.compare(table)
            rt.event('science_compare',table=table,rows=count)
        return dict(algorithm_replays=1,input_elements=input_elements,science_tables=len(TABLES))


def validate(rt,b,guard):
    from data_pipeline.analysis.resources.publication import _lake, _descriptor, _country, upstream, _binding
    start=time.monotonic();account=Account(rt,guard)
    with _lake(rt) as db:
        check_table_set(rt.dsn,'resource_'+b['run_id'],b['snapshot'])
        descriptor=_descriptor(rt,db,b);country=_country(rt,b)
        paths=set(p for entry in descriptor.values() for p in entry['paths'])
        from pathlib import Path
        paths.update([country['raw_path'],str(Path(country['output'])/'execution.json')])
        layout=b['receipt']['storage_layout']
        execution_path=Path(layout['local_output'])/'execution.json'
        paths.add(str(execution_path))
        execution=json.loads(rt.path(execution_path).read_text())
        for key,value in [('run_id',b['run_id']),('snapshot',b['snapshot']),('dataset_id',b['dataset_id']),('binding_manifest',b['binding']),('inventory',b['receipt']['inventory'])]:
            if execution.get(key)!=value:raise ValueError('原执行回执与完成绑定不符：'+key)
        if execution.get('state') not in ('ready','complete'):raise ValueError('执行回执未完成')
        entities=[upstream._entity(rt,p,hash_body=True,guard=guard) for p in sorted(paths)]
        columns={}
        for table,wanted in ALL_TABLES.items():
            actual=[(r[0],r[1]) for r in db.execute('DESCRIBE SELECT * FROM '+relation(b,table)).fetchall()]
            if actual!=[(n,'TIMESTAMP WITH TIME ZONE' if t=='TIMESTAMPTZ' else t) for n,t in wanted]:raise ValueError('完整类型descriptor不符：'+table)
            columns[table]=actual
        inventory={}
        for table in ALL_TABLES:
            h=hashlib.sha256();count=size=0
            cursor=db.execute('SELECT * FROM '+relation(b,table)+' ORDER BY ALL').fetch_record_batch(512)
            error=None
            try:
                for batch in cursor:
                    for row in batch.to_pylist():
                        account.add(row)
                        raw=json.dumps(normalized(row),sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()+b'\n'
                        h.update(raw);count+=1;size+=len(raw)
            except BaseException as exc:error=exc;raise
            finally:
                from data_pipeline.analysis.resources.publication import _close
                _close((cursor.close,),error)
            inventory[table]=dict(rows=count,logical_bytes=size,sha256=h.hexdigest())
        if inventory!=b['receipt']['inventory']:raise ValueError('旧完整typed回执不符')
        rt.event('inventory_scan',tables=16,rows=sum(x['rows'] for x in inventory.values()),logical_bytes=sum(x['logical_bytes'] for x in inventory.values()))
        for table in ('peer_dependencies','observation_quality'):
            if db.execute('SELECT count(*) FROM '+relation(b,table)+' WHERE source_id NOT IN (SELECT source_id FROM '+relation(b,'sources')+') OR source_id IS NULL').fetchone()[0]:raise ValueError('孤立来源依赖：'+table)
        validate_relations(db,lambda t:relation(b,t),b['binding'],guard,dsn=rt.dsn)
        cost=science(rt,b,db,guard,account)
        if _descriptor(rt,db,b)!=descriptor:raise ValueError('准入期间目录漂移')
        upstream._entities_current(rt,entities,guard);_binding(rt,b)
    return dict(tables=inventory,columns=columns,descriptor=descriptor,country=country,fk='legacy-peer-normal-qualification-plus-all-science/v1'),entities,dict(**cost,wall_seconds=time.monotonic()-start,accounted_rows=account.rows,accounted_typed_bytes=account.bytes,full_inventory_scans=1,resource_science_comparison_scans=10,legacy_relation_audits=1)


def selected_rows(rt,b,request,coverage,guard):
    from data_pipeline.analysis.resources.publication import _lake
    scope=untyped(request['scope_typed'])['scope']
    with _lake(rt) as db:
        selected='SELECT source_id FROM '+relation(b,'sources')+(" WHERE purpose='result'" if scope=='result' else '')
        where=' WHERE source_id IN ('+selected+')'
        account=Account(rt,guard);retained=ResidentAccount(rt,guard)
        def ordered_query(table):
            return ('SELECT t.* FROM (SELECT * FROM '+relation(b,table)+') t'+where+
                ' ORDER BY (SELECT snapshot_time FROM '+relation(b,'sources')+' WHERE source_id=t.source_id),'+
                ','.join('t."'+n+'"' for n,_ in ALL_TABLES[table]))
        with _cursor(db,ordered_query('coverage')) as cursor:
            for batch in cursor:
                for row in batch.to_pylist():account.add(row);retained.add(row);coverage.append(row)
        if db.execute('SELECT 1 FROM ('+selected+') s WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM '+relation(b,'coverage')+') c WHERE c.source_id=s.source_id) LIMIT 1').fetchone():raise ValueError('选择缺必需Coverage')
        target=request['view']
        if target=='coverage':
            yield from coverage;return
        # 一次关系连接按原科学行序、原资格全列序输出；只暂存当前一行的资格。
        raw_order='(SELECT snapshot_time FROM '+relation(b,'sources')+' WHERE source_id=t.source_id),'+','.join('t."'+n+'"' for n,_ in ALL_TABLES[target])
        match="q.source_id IS NOT DISTINCT FROM r.raw.source_id"
        if target=='topology_status':match+=" AND q.dimension='country' AND q.bucket IS NOT DISTINCT FROM r.raw.country_cn"
        else:
            match+=" AND q.dimension IS NOT DISTINCT FROM r.raw.dimension AND q.bucket IS NOT DISTINCT FROM r.raw.bucket"
            if target=='normal_bands':match+=" AND q.metric IS NOT DISTINCT FROM r.raw.metric"
        sql=('WITH r AS (SELECT row_number() OVER (ORDER BY '+raw_order+') AS ordinal,t AS raw FROM (SELECT * FROM '+relation(b,target)+') t'+where+') '
             'SELECT r.ordinal,r.raw,CASE WHEN q.target IS NULL THEN NULL ELSE q END AS qualification FROM r LEFT JOIN (SELECT * FROM '+relation(b,'qualifications')+
             ') q ON '+match+' AND q.target=? ORDER BY r.ordinal,'+','.join('q."'+n+'"' for n,_ in ALL_TABLES['qualifications']))
        ordinal=None;raw=None;qs=[];size=0
        def output():
            if not qs:raise ValueError('科学行缺资格')
            if target=='metrics':main={q['metric']:raw[q['metric']] if q['status']=='qualified' else None for q in qs}
            else:
                names=('upper_bound','lower_bound','mean','population_std') if target=='normal_bands' else ('status','node_count','edge_count')
                main={name:raw[name] if qs[0]['status']=='qualified' else None for name in names}
            return dict(raw=raw,qualification=qs,main=main)
        with _cursor(db,sql,[target],batch_rows=min(512,request['batch_rows'])) as cursor:
            for batch in cursor:
                rt.event('selected_batch',table=target,rows=batch.num_rows,decoded_bytes=batch.nbytes)
                for joined in batch.to_pylist():
                    guard()
                    if joined['ordinal']!=ordinal:
                        if ordinal is not None:yield output()
                        ordinal=joined['ordinal'];raw=joined['raw'];qs=[];size=len(typed(raw).encode());account.add(raw)
                    q=joined['qualification']
                    if q is not None:
                        size+=len(typed(q).encode())
                        if size>rt.max_bytes:raise ValueError('单行资格驻留字节保护')
                        account.add(q);qs.append(q)
            if ordinal is not None:yield output()
