"""从固定DuckLake观察快照盘点端点并回放；不重新打开MRT原件。"""
from collections import defaultdict
import json
import pyarrow as pa
from data_pipeline.bgp.input.mrt_reader import Message
from data_pipeline.bgp.archive.store import connect_duckdb, literal

MAPPING_RULE='full-input-unique-calculation-endpoint/v1'


def _reader_sources(reader,baseline,updates):
    sources={baseline,*updates}
    starts=[start for start in reader.starts if start.source_id in sources]
    if {start.source_id for start in starts}!=sources:raise ValueError('回放来源不在Reader绑定中')
    reader.check_sources(starts)
    return starts


def build_mapping(store,baseline,updates,*,reader=None):
    """全部UPDATE端点唯一性用于计算映射，不是物理Peer/Session证明。"""
    if reader and reader.dsn!=store.dsn:raise ValueError('回放读取catalog绑定不一致')
    store.flush()
    starts=_reader_sources(reader,baseline,updates) if reader else None
    db=reader.connect() if reader else store.db
    try:
        db.register('update_sources',pa.table({'source_id':list(updates)},schema=pa.schema([('source_id',pa.string())])))
        snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        from data_pipeline.bgp.archive.store import bound_table
        table=lambda name:reader.bound_table(name) if reader else bound_table(store.schema,snapshot,name)
        endpoints=db.execute(f'''SELECT peer_ip,peer_asn,local_ip,local_asn,interface,
            min(message_id),count(*) FROM {table("messages")} m JOIN update_sources USING(source_id)
            WHERE peer_ip IS NOT NULL AND NOT coalesce(local_message,false)
            GROUP BY peer_ip,peer_asn,local_ip,local_asn,interface''').fetchall()
        db.unregister('update_sources')
        peers=db.execute(f'SELECT "ip",asn,bgp_id,table_record,"index" FROM {table("peers")} WHERE source_id=?',[baseline]).fetchall()
        if reader:reader.check_sources(starts)
    finally:
        if reader:db.close()
    mappings,pending=mapping_rows(peers,endpoints,baseline,updates)
    if reader:reader.check_sources(starts)
    for row in pending:store.append('baseline_mappings',row)
    store.flush()
    if reader:reader.check_sources(starts)
    return mappings


def mapping_rows(peers,endpoints,baseline,updates):
    """同一完整端点集合的纯映射规则，供封存和文件流水线共用。"""
    candidates=defaultdict(list)
    for ip,asn,local_ip,local_asn,interface,witness,count in endpoints:
        candidates[ip,asn].append((local_ip,local_asn,interface,witness,count))
    by_peer=defaultdict(list)
    for ip,asn,bgp_id,record,index in peers:by_peer[ip,asn].append((bgp_id,record,index))
    mappings=[];pending=[]
    for (ip,asn),refs in sorted(by_peer.items()):
        # 同组证据按原表位置稳定枚举；不去重，也不改变映射唯一性判定。
        refs=sorted(refs,key=lambda ref:(ref[1],ref[2]))  # table_record, index
        matches=sorted(candidates.get((ip,asn),[]),key=lambda row:tuple(str(v) for v in row))
        if len(refs)!=1:
            status='conflicting_baseline_peers'
        elif len(matches)!=1:
            status='no_observed_endpoint' if not matches else 'ambiguous_local_endpoints'
        elif any(v is None for v in matches[0][:3]):
            status='missing_local_fields'
        else:
            status='calculation_mapping'
            mappings.append((ip,asn,*matches[0][:3]))
        pending.append({'baseline_source':baseline,'peer_ip':ip,'peer_asn':asn,
            'status':status,'rule':MAPPING_RULE,'peer_refs':json.dumps(refs),
            'endpoint_evidence':json.dumps(matches),'input_sources':list(updates)})
    return tuple(mappings),pending


def messages(store,baseline,updates,snapshot,*,reader=None):
    """内部candidate只读接口；外部消费者仍只能读取complete。"""
    if reader and reader.dsn!=store.dsn:raise ValueError('回放读取catalog绑定不一致')
    starts=_reader_sources(reader,baseline,updates) if reader else None
    db=reader.connect() if reader else connect_duckdb()
    try:
        db.execute('SET memory_limit='+literal(store.memory_limit))
        db.execute('SET temp_directory='+literal(store.root/'replay-temp'))
        if not reader:
            db.execute('LOAD ducklake');db.execute('LOAD postgres')
            db.execute('ATTACH '+literal('ducklake:postgres:'+store.dsn)+' AS lake (READ_ONLY)')
        sources=[baseline,*updates]
        db.register('source_order',pa.table({'source_id':sources,'source_rank':list(range(len(sources)))}))
        from data_pipeline.bgp.archive.store import bound_table
        table=lambda name:reader.bound_table(name) if reader else bound_table(store.schema,snapshot,name)
        result=db.execute(f'''SELECT m.*,e.ordinal,e.action,e.afi,e.safi,e.prefix,e.path_id,e.path_id_present,
            e.peer_ip AS element_peer_ip,e.peer_asn AS element_peer_asn,e.bgp_id AS element_bgp_id,
            e.bgp_id_present AS element_bgp_id_present,e.peer_table_record,e.peer_index,e.path_key,
            p.as_path_text,p.attributed_origin_asn
            FROM {table("messages")} m JOIN source_order s USING(source_id)
            LEFT JOIN {table("elements")} e USING(message_id)
            LEFT JOIN {table("paths")} p USING(path_key)
            ORDER BY s.source_rank,m.record,e.ordinal''')
        current=None
        for batch in result.fetch_record_batch(10000):
            for row in batch.to_pylist():
                key=(row['source_id'],row['record'])
                if current is None or (current.source_id,current.record)!=key:
                    if current is not None:yield current
                    current=Message(row['source_id'],row['record'],row['offset'],row['length'],row['epoch'],
                        row['mrt_type'],row['mrt_subtype'],row['raw_digest'],content_sha256=row['content_sha256'],
                        microsecond=row['microsecond'],kind=row['kind'],reason=row['reason'],old_state=row['old_state'],new_state=row['new_state'])
                    if row.get('interpretation'):
                        from data_pipeline.bgp.input.mrt_types import Interpretation, HeaderEvidence, FieldFailure, ReadPolicy, ParseStatus
                        meta=json.loads(row['interpretation']);meta['header']=HeaderEvidence(**meta['header'])
                        if meta['failure']:meta['failure']=FieldFailure(**meta['failure'])
                        meta['policy']=ReadPolicy(meta['policy']);meta['status']=ParseStatus(meta['status'])
                        current.interpretation=Interpretation(**meta)
                    current.peer={'ip':row['peer_ip'],'asn':row['peer_asn'],'bgp_id':row['bgp_id'],
                        'bgp_id_present':row['bgp_id_present'],'local_ip':row['local_ip'],'local_asn':row['local_asn'],
                        'interface':row['interface'],'local_message':row['local_message']}
                if row['action'] is not None:
                    peer={**current.peer,'ip':row['element_peer_ip'],'asn':row['element_peer_asn'],
                        'bgp_id':row['element_bgp_id'],'bgp_id_present':row['element_bgp_id_present'],
                        'table_record':row['peer_table_record'],'index':row['peer_index']}
                    current.elements.append({k:row[k] for k in ('ordinal','action','afi','safi','prefix','path_id','path_id_present','path_key')}|{'peer':peer})
                    current.paths.append({k:row[k] for k in ('path_key','as_path_text','attributed_origin_asn')})
        if reader:reader.check_sources(starts)
        if current is not None:yield current
        if reader:reader.check_sources(starts)
    finally:db.close()
