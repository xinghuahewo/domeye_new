"""M2 P1内容审计及原事实有界读；临时排序只在私有临时DuckDB，不写历史PG正文。"""
import hashlib
import io
import os
from pathlib import Path
import resource
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from data_pipeline.bgp.archive.checkpoint import OBS_TABLES, KEYS, sha
from data_pipeline.bgp.archive.store import connect_duckdb, literal, TYPES
from data_pipeline.bgp.archive.selection import Selection
from data_pipeline.bgp.archive.value_codec import typed, digest

# 有限原表的固定顺序；消息附属表按原record数值，绝不把参考CP ordinal当MRT rank。
ORDERS={**KEYS,'elements':"split_part(message_id,':',2)::BIGINT,ordinal",
    'eor':"split_part(message_id,':',2)::BIGINT,afi,safi",
    'associations':"split_part(message_id,':',2)::BIGINT,peer_ref",
    'quality':"split_part(message_id,':',2)::BIGINT NULLS FIRST,code,detail"}


class CountedFile(io.BufferedReader):
    def __init__(self,path,runtime):
        super().__init__(open(path,'rb',buffering=0));self.runtime=runtime
    def read(self,n=-1):
        value=super().read(n);self.runtime.event('parquet_read',bytes=len(value));return value


def _query(runtime,db,sql,args=None):
    runtime.event('duckdb_sql',sql=sql)
    return db.execute(sql,args) if args is not None else db.execute(sql)


def _guard_temp(runtime,root,guard):
    guard();size=sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
    runtime.event('temporary_disk',bytes=size)
    if size>runtime._checked_limits()['max_temp_bytes']:raise ValueError('临时磁盘资源保护')


from contextlib import contextmanager


def _finish_close(resources,primary):
    errors=[]
    for action in reversed(resources):
        try:action()
        except BaseException as exc:errors.append(exc)
    if errors:
        if primary is not None:primary.cleanup_errors=(*getattr(primary,'cleanup_errors',()),*errors)
        else:raise errors[0]


@contextmanager
def _closing(resources):
    primary=None
    try:yield resources
    except BaseException as exc:primary=exc;raise
    finally:_finish_close(resources,primary)


@contextmanager
def _stage(runtime,guard):
    with _closing([]) as cleanup:
        temp=tempfile.TemporaryDirectory(prefix='m2-p1-',dir=runtime.scratch_root);cleanup.append(temp.cleanup)
        root=Path(temp.name);db=connect_duckdb(str(root/'sort.duckdb'));cleanup.append(db.close)
        db.execute('SET memory_limit='+literal(runtime._checked_limits()['memory_limit']))
        db.execute('SET temp_directory='+literal(root/'spill'))
        yield db,root


def _load(runtime,db,root,cp,table,batch_rows,guard):
    columns=OBS_TABLES[table]
    schema=pa.schema([(n,TYPES[t]) for n,t in columns])
    _query(runtime,db,'DROP TABLE IF EXISTS body')
    _query(runtime,db,'CREATE TABLE body ('+','.join('"'+n+'" '+t for n,t in columns)+')')
    for f in cp['files']:
        if f['table']!=table:continue
        guard();path=runtime.path(f['path'])
        with _closing([]) as cleanup:
            file=CountedFile(path,runtime);cleanup.append(file.close)
            parquet=pq.ParquetFile(file);cleanup.append(parquet.close)
            expected=pa.schema([('attempt',pa.string()),*schema])
            if not parquet.schema_arrow.equals(expected,check_metadata=False):raise ValueError('Parquet原表类型/字段不符')
            for group in range(parquet.num_row_groups):
                guard();runtime.event('parquet_row_group',path=str(path),group=group,rows=parquet.metadata.row_group(group).num_rows)
                for batch in parquet.iter_batches(batch_size=min(batch_rows,512),row_groups=[group]):
                    guard();runtime.event('decoded_batch',rows=batch.num_rows,bytes=batch.nbytes)
                    rows=batch.to_pylist()
                    if any(row.pop('attempt')!=cp['attempt'] for row in rows):raise ValueError('文件attempt越界')
                    for row in rows:guard()
                    db.register('incoming',pa.Table.from_pylist(rows,schema=schema))
                    _query(runtime,db,'INSERT INTO body SELECT * FROM incoming')
                    db.unregister('incoming');_guard_temp(runtime,root,guard)


def read_rows(runtime,binding,table,sources,batch_rows,*,guard):
    guard=runtime.guarded(guard);guard();runtime.check_binding(binding)
    if table not in OBS_TABLES:raise ValueError('未知原观察表')
    raw=binding.get('m2_binding',binding)
    cps={cp['source_id']:cp for cp in raw['seal']['checkpoints']}
    with _stage(runtime,guard) as (db,root):
        for source in sources:
            cp=cps[source]
            _load(runtime,db,root,cp,table,batch_rows,guard)
            result=_query(runtime,db,'SELECT * FROM body ORDER BY '+ORDERS[table]).fetch_record_batch(min(batch_rows,512))
            try:
                for batch in result:
                    _guard_temp(runtime,root,guard)
                    for row in batch.to_pylist():
                        guard();runtime.event('read_row',bytes=len(typed(row).encode()));yield row
            finally:result.close()


def validate(runtime,binding,*,guard):
    guard=runtime.guarded(guard);guard();runtime.check_binding(binding)
    from data_pipeline.bgp.archive.admission import _entity, _entities_current, _ducklake_root, _physical
    raw=binding.get('m2_binding',binding);selection=Selection(runtime.dsn,raw['run_id'],raw['snapshot'])
    selected_cps=selection.checkpoints if binding['owner']=='m2' else [cp for cp in selection.checkpoints if cp['source_id']==binding['source_id']]
    if not selected_cps:raise ValueError('参考CP缺失')
    inventory=[];entities={};hash_bytes=rows_total=typed_bytes=0
    required={}
    # 原件仅读取SHA，不重解码MRT。引用和数据实体均限显式允许根。
    for cp in selected_cps:
        entry=selection.plan['inputs'][cp['ordinal']]
        required[entry['path']]=entry['sha256']
        for f in cp['files']:required[f['path']]=f['sha256']
    scratch=tempfile.TemporaryDirectory(prefix='m2-audit-',dir=runtime.scratch_root)
    try:lake=selection.connect()
    except BaseException:
        scratch.cleanup();raise
    primary=None
    try:
        _ducklake_root(runtime,lake,_physical(runtime,binding)['root'])
        lake.execute('SET memory_limit='+literal(runtime._checked_limits()['memory_limit']))
        lake.execute('SET temp_directory='+literal(Path(scratch.name)/'spill'))
        # 明确保存选中的索引物理文件；PG锁不会锁这些文件。
        for table in (*OBS_TABLES,'seal_selection','path_owner'):
            files=_query(runtime,lake,'SELECT data_file,delete_file FROM ducklake_list_files(?,?,schema => ?,snapshot_version => ?)',
                ['lake',table,selection.schema,selection.snapshot]).fetchall()
            if any(deleted is not None for _,deleted in files):raise ValueError('固定快照有UPDATE/DELETE文件')
            visible={path for path,_ in files}
            if table in OBS_TABLES:
                if any(f['path'] not in visible for cp in selected_cps for f in cp['files'] if f['table']==table):raise ValueError('CP文件不在固定快照')
            else:
                for path in visible:required[path]=None
        for path,expected in sorted(required.items()):
            e=_entity(runtime,path,hash_body=True,guard=guard,preserve_source_path=True)
            if expected is not None and e['sha256']!=expected:raise ValueError('原件或CP文件SHA不符')
            entities[path]=e;hash_bytes+=e['size']
        for cp in selected_cps:
            for table,columns in OBS_TABLES.items():
                # 参考准入也验证所选CP全部必需空表，不能隐藏参考CP混入MRT。
                guard();relation=selection.table(table,cp['source_id'])
                actual_schema=[(r[0],r[1]) for r in _query(runtime,lake,'DESCRIBE SELECT * FROM '+relation).fetchall()]
                if actual_schema!=columns:raise ValueError('固定读取schema不符')
                h=hashlib.sha256();count=0
                batches=_query(runtime,lake,'SELECT * FROM '+relation+' ORDER BY '+KEYS[table]).fetch_record_batch(512)
                try:
                    for batch in batches:
                        _guard_temp(runtime,Path(scratch.name),guard);runtime.event('admit_body_batch',table=table,rows=batch.num_rows,bytes=batch.nbytes)
                        for row in batch.to_pylist():
                            h.update(bytes.fromhex(sha(row)));count+=1;typed_bytes+=len(typed(row).encode())
                            if table=='paths' and hashlib.sha256(bytes([row['asn_width']])+row['attributes_raw']).hexdigest()!=row['path_key']:raise ValueError('路径原bytes/key不符')
                finally:batches.close()
                expected=cp['tables'][table]
                if count!=expected['count'] or h.hexdigest()!=expected['digest']:raise ValueError('实际CP正文计数/typed摘要不符')
                ceiling=_query(runtime,lake,f'SELECT coalesce(max(rowid),-1) FROM lake.{selection.schema}."{table}" AT (VERSION => {int(cp["snapshot"])}) WHERE attempt=?',[cp['attempt']]).fetchone()[0]
                if ceiling!=expected['max_rowid']:raise ValueError('实际CP物理上限不符')
                inventory.append(dict(source_id=cp['source_id'],checkpoint_ordinal=cp['ordinal'],table=table,columns=columns,rows=count,typed_digest=h.hexdigest()))
                rows_total+=count
            _fk(runtime,lake,selection,cp,guard)
        selection_schema=[('seal_id','VARCHAR'),('source_id','VARCHAR'),('attempt','VARCHAR'),('ordinal','BIGINT'),('checkpoint_digest','VARCHAR')]
        seal_table=f'lake.{selection.schema}.seal_selection AT (VERSION => {selection.snapshot})'
        if [(r[0],r[1]) for r in _query(runtime,lake,'DESCRIBE SELECT * FROM '+seal_table).fetchall()]!=selection_schema:raise ValueError('seal_selection类型不符')
        inventory.append(dict(table='seal_selection',columns=selection_schema,rows=len(selection.checkpoints),typed_digest=digest([(cp['source_id'],cp['attempt'],cp['ordinal'],cp['digest']) for cp in selection.checkpoints])))
        # seal全局path代表索引的真实正文摘要及FK；参考依赖已合格M2覆盖全局。
        if binding['owner']=='m2':
            relation=f'lake.{selection.schema}.path_owner AT (VERSION => {selection.snapshot})'
            h=hashlib.sha256();count=0
            batches=_query(runtime,lake,f'SELECT path_key,attempt FROM {relation} WHERE seal_id=? ORDER BY path_key',[selection.seal['seal_id']]).fetch_record_batch(512)
            try:
                for batch in batches:
                    guard();runtime.event('admit_index_batch',rows=batch.num_rows,bytes=batch.nbytes)
                    for row in batch.to_pylist():h.update(bytes.fromhex(sha(row)));count+=1
            finally:batches.close()
            if count!=selection.seal['path_owner_count'] or h.hexdigest()!=selection.seal['path_owner_digest']:raise ValueError('path_owner实际摘要不符')
            owner_columns=[('seal_id','VARCHAR'),('path_key','VARCHAR'),('attempt','VARCHAR')]
            if [(r[0],r[1]) for r in _query(runtime,lake,'DESCRIBE SELECT * FROM '+relation).fetchall()]!=owner_columns:raise ValueError('path_owner类型不符')
            available=' UNION ALL '.join(f'SELECT {literal(cp["attempt"])} AS selected_attempt,{cp["ordinal"]} AS source_ordinal,* FROM '+selection.table('paths',cp['source_id']) for cp in selection.checkpoints)
            owners=f'(SELECT path_key,attempt FROM {relation} WHERE seal_id={literal(selection.seal["seal_id"])})'
            expected=f'(SELECT path_key,arg_min(selected_attempt,source_ordinal) AS attempt FROM ({available}) GROUP BY path_key)'
            if _query(runtime,lake,f'SELECT 1 FROM {expected} e FULL OUTER JOIN {owners} o USING(path_key) WHERE e.attempt IS DISTINCT FROM o.attempt LIMIT 1').fetchone():raise ValueError('path_owner来源代表FK不符')
            if _query(runtime,lake,f'SELECT 1 FROM {owners} GROUP BY path_key HAVING count(*)<>1 LIMIT 1').fetchone():raise ValueError('path_owner重复位置')
            values='struct_pack('+','.join('"'+n+'" := "'+n+'"' for n,_ in OBS_TABLES['paths'])+')'
            if _query(runtime,lake,f'SELECT 1 FROM ({available}) GROUP BY path_key HAVING count(DISTINCT {values})>1 LIMIT 1').fetchone():raise ValueError('跨CP同路径key的typed正文冲突')
            inventory.append(dict(table='path_owner',columns=owner_columns,rows=count,typed_digest=h.hexdigest()))
    except BaseException as exc:primary=exc;raise
    finally:_finish_close([scratch.cleanup,lake.close],primary)
    # 与有界读相同的Parquet路径，完整核对CP清单无额外/漏行；记录实际行组和解码成本。
    with _stage(runtime,guard) as (db,root):
        for cp in selected_cps:
            for table in OBS_TABLES:
                _load(runtime,db,root,cp,table,512,guard)
                h=hashlib.sha256();count=0
                batches=_query(runtime,db,'SELECT * FROM body ORDER BY '+KEYS[table]).fetch_record_batch(512)
                try:
                    for batch in batches:
                        for row in batch.to_pylist():guard();h.update(bytes.fromhex(sha(row)));count+=1
                finally:batches.close()
                if (count,h.hexdigest())!=(cp['tables'][table]['count'],cp['tables'][table]['digest']):raise ValueError('CP文件清单与实际固定选择正文不同')
    _entities_current(runtime,list(entities.values()),guard)
    cost=dict(selected_body_rows=rows_total,typed_bytes=typed_bytes,entity_hash_bytes=hash_bytes,
        ordered_enumeration_passes=2,additional_queries='per-table physical-rowid maxima and per-CP FK queries; see SQL log',fk_scope='each-selected-cp-all-eight-tables',replay_calls=0,
        process_lifetime_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),peak_scope='whole_process_lifetime',pid=os.getpid())
    complete=dict(tables=inventory,fk=dict(version='m2-complete-cp-fk/v1',checkpoint_ordinals=[cp['ordinal'] for cp in selected_cps],checks=['message-position','element-message-path','attached-message','peer-table','reference-source','association-reference-message','interpretation-counts','path-owner-first-source-and-typed-consistency' if binding['owner']=='m2' else 'global-path-covered-by-m2-dependency'],result='passed'))
    return complete,sorted(entities.values(),key=lambda e:e['path']),cost


def _fk(runtime,db,s,cp,guard):
    m=s.table('messages',cp['source_id']);e=s.table('elements',cp['source_id']);p=s.table('paths',cp['source_id'])
    def reject(sql,args=None):
        guard()
        if _query(runtime,db,sql,args).fetchone():raise ValueError('完整CP/FK校验失败')
    # 合同的必需行键；未绑定 association.peer_ref、source quality.message_id 和科学未知值仍合法。
    required={'messages':('message_id','source_id','content_sha256','record','kind'),
        'elements':('event_id','message_id','ordinal','path_key','action'),
        'paths':('path_key','asn_width','attributes_raw'),
        'peers':('source_id','table_record','index'),
        'eor':('message_id','afi','safi'),
        'associations':('message_id',),
        'quality':('source_id','code'),
        'references':('source_id','row','location')}
    for name,keys in required.items():
        relation=s.table(name,cp['source_id'])
        reject('SELECT 1 FROM '+relation+' WHERE '+' OR '.join('"'+k+'" IS NULL' for k in keys)+' LIMIT 1')
    result=_query(runtime,db,f'SELECT count(*),count(DISTINCT record),min(record),max(record) FROM {m}').fetchone()
    if result[0] and result!=(result[0],result[0],0,result[0]-1):raise ValueError('消息record缺漏/重复')
    reject(f'SELECT 1 FROM {m} WHERE source_id IS DISTINCT FROM ? OR content_sha256 IS DISTINCT FROM ? OR message_id IS DISTINCT FROM source_id||\':\'||record LIMIT 1',[cp['source_id'],cp['source_sha']])
    reject(f'SELECT 1 FROM {p} GROUP BY path_key HAVING count(*)<>1 LIMIT 1')
    reject(f'SELECT 1 FROM {e} GROUP BY message_id HAVING min(ordinal)<>0 OR max(ordinal)+1<>count(*) OR count(DISTINCT ordinal)<>count(*) LIMIT 1')
    reject(f'SELECT 1 FROM {e} e LEFT JOIN {m} m USING(message_id) LEFT JOIN {p} p USING(path_key) WHERE m.message_id IS NULL OR p.path_key IS NULL OR e.event_id IS DISTINCT FROM e.message_id||\':\'||e.ordinal LIMIT 1')
    for name in ('eor','quality','associations'):
        relation=s.table(name,cp['source_id'])
        reject(f'SELECT 1 FROM {relation} a LEFT JOIN {m} m USING(message_id) WHERE a.message_id IS NOT NULL AND m.message_id IS NULL LIMIT 1')
    association=s.table('associations',cp['source_id']);all_messages=s.table('messages')
    reject(f'SELECT 1 FROM {association} a LEFT JOIN {all_messages} m ON a.reference_message=m.message_id WHERE a.reference_message IS NOT NULL AND (m.message_id IS NULL OR m.kind<>\'peer_index_table\') LIMIT 1')
    peers=s.table('peers',cp['source_id'])
    reject(f'SELECT 1 FROM {peers} p LEFT JOIN {m} m ON m.record=p.table_record WHERE m.record IS NULL OR m.kind<>\'peer_index_table\' OR p.source_id<>? LIMIT 1',[cp['source_id']])
    reject(f'''SELECT 1 FROM {e} e JOIN {m} m USING(message_id)
        LEFT JOIN {peers} p ON p.source_id=m.source_id AND p.table_record=e.peer_table_record AND p."index"=e.peer_index
        WHERE (e.action='rib_snapshot' OR m.kind='rib') AND
          (e.action IS DISTINCT FROM 'rib_snapshot' OR m.kind IS DISTINCT FROM 'rib'
           OR p.source_id IS NULL OR p.table_record>=m.record
           OR e.peer_ip IS DISTINCT FROM p.ip OR e.peer_asn IS DISTINCT FROM p.asn
           OR e.bgp_id IS DISTINCT FROM p.bgp_id OR e.bgp_id_present IS DISTINCT FROM p.bgp_id_present)
        LIMIT 1''')
    reject(f'SELECT 1 FROM {peers} GROUP BY source_id,table_record,"index" HAVING count(*)<>1 LIMIT 1')
    quality=s.table('quality',cp['source_id'])
    reject(f'SELECT 1 FROM {quality} WHERE source_id IS DISTINCT FROM ? LIMIT 1',[cp['source_id']])
    refs=s.table('references',cp['source_id'])
    reject(f'SELECT 1 FROM {refs} WHERE source_id IS DISTINCT FROM ? LIMIT 1',[cp['source_id']])
    statuses=dict(_query(runtime,db,f"SELECT json_extract_string(interpretation,'$.status'),count(*) FROM {m} GROUP BY 1").fetchall())
    if any(statuses.get(k,0)!=cp['counts'][k] for k in ('decoded','rejected','unsupported')):raise ValueError('原解释分类不符')
