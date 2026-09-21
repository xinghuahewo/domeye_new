"""现有多文件 M2 的原生适配；计划、文件选择与上下游合同仍由 checkpoint 管理。"""
import json
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace
import uuid

import pyarrow as pa
import pyarrow.compute as pc

from data_pipeline.bgp.archive.checkpoint import OBS_TABLES, VERSION, durable, event, file_sha, sha, verify_checkpoint
from data_pipeline.bgp.archive.native_writer import NativeAudit, NativeStore, arrow_file
from data_pipeline.bgp.replay.route_replay import associate
from data_pipeline.bgp.archive.store import TYPES, connect_duckdb, literal


class NativeFile:
    def __init__(self,root,ordinal,entry,runtime,dsn,schema,data_path,owner,guard,batch_rows,memory_limit,metrics,policy):
        self.root=Path(root)/f'native-file-{ordinal:04d}';self.root.mkdir(exist_ok=True)
        identity=self.root/'attempt.json'
        if not identity.exists():durable(identity,dict(attempt=uuid.uuid4().hex,source_id=entry['source_id']))
        saved=json.loads(identity.read_text())
        if saved['source_id']!=entry['source_id']:raise ValueError('原生文件恢复身份漂移')
        staging=self.root/('staging-'+uuid.uuid4().hex);staging.mkdir()
        self.store=NativeStore(dsn,schema,data_path,staging,owner,guard,batch_rows,memory_limit,
            runtime=runtime,attempt=saved['attempt'],metrics=metrics)
        self.entry=entry;self.runtime=runtime;self.data_path=data_path;self.guard=guard
        self.batch_rows=batch_rows;self.metrics=metrics;self.policy=policy;self.ordinal=ordinal

    def ingest(self,context,hook,counts=None,observer=None):
        native=self.root/'native'
        if counts is None:counts=dict(messages=0,elements=0,references=0,decoded=0,rejected=0,unsupported=0)
        audit=None;prior=None;started=time.monotonic()
        def consume(directory,segment):
            nonlocal audit,prior
            if audit is None:audit=NativeAudit(native)
            messages=arrow_file(directory/'messages.arrow');extra={'quality':[],'associations':[]}
            if self.entry['role'] in ('baseline','snapshot'):
                counts['decoded']+=len(messages)
                if self.entry['role']=='baseline':
                    peer_messages=messages.filter(pc.equal(messages['kind'],'peer_index_table'))
                    if len(peer_messages):
                        m=peer_messages.slice(len(peer_messages)-1).to_pylist()[0]
                        context['reference']={k:m[k] for k in ('source_id','record','message_id','epoch','microsecond')}
                        peers=arrow_file(directory/'peers.arrow')
                        context['reference_peers']=[dict(p,ip_present=True,asn_present=True) for p in peers.filter(pc.equal(peers['table_record'],m['record'])).to_pylist()]
                    context['baseline_elements']+=segment['counts']['elements']
            else:
                # 每条消息一次上下文处理；NLRI 和属性的主数据始终走列式通路。
                fields=['message_id','epoch','microsecond','mrt_type','peer_ip','peer_asn','bgp_id','bgp_id_present','interpretation']
                for i,m in enumerate(messages.select(fields).to_pylist()):
                    interpretation=json.loads(m['interpretation']);counts[interpretation['status']]+=1
                    stamp=[m['epoch'],interpretation['header']['microsecond'] if m['mrt_type']==17 else 0]
                    known=stamp[1] is not None
                    def quality(code,detail):extra['quality'].append(dict(source_id=self.entry['source_id'],message_id=m['message_id'],code=code,detail=detail))
                    if counts['messages']+i==0 and context['prior_update_last'] is not None and known and context['prior_update_last'][1] is not None and stamp<=context['prior_update_last']:
                        quality('cross_file_time_overlap','保留声明源序；时间连续性未知')
                    if prior is not None and known and prior[1] is not None and stamp<prior:quality('timestamp_regression','保留物理顺序；时间连续性未知')
                    prior=stamp
                    if m['peer_ip'] is not None and context['reference']:
                        ref=SimpleNamespace(**context['reference'])
                        message=SimpleNamespace(epoch=m['epoch'],microsecond=m['microsecond'],peer=dict(ip=m['peer_ip'],asn=m['peer_asn'],bgp_id=m['bgp_id'],bgp_id_present=m['bgp_id_present']))
                        peer_ref,reason=associate(message,ref,context['reference_peers'])
                        extra['associations'].append(dict(message_id=m['message_id'],peer_ref=peer_ref,reference_message=ref.message_id,rule='bound_same_time_unique_endpoint/v1',reason=reason))
            additional={name:pa.Table.from_pylist(rows,schema=pa.schema([(n,TYPES[t]) for n,t in OBS_TABLES[name]])) for name,rows in extra.items() if rows}
            counts['messages']+=len(messages);counts['elements']+=segment['counts']['elements']
            consume_tables = None if observer is None else lambda tables: observer.submit_segment(self.ordinal, native, segment, tables)
            # 直接流水线故障点统一报告文件序号；旧原生／离线调用保留分段序号约定。
            segment_hook=hook if observer is None else lambda label,segment_ordinal,owner:hook(label,self.ordinal,owner)
            self.store.import_segment(directory,segment,self.data_path,audit,segment_hook,additional,consume=consume_tables)
            event(self.root.parent,'native_segment_imported',file_ordinal=self.ordinal,segment=segment['segment'],messages=counts['messages'],elements=counts['elements'],elapsed_seconds=time.monotonic()-started)
            hook('file_mid',self.ordinal,self.store.owner)
        result=self.runtime.parse(self.entry['path'],self.entry['source_id'],self.entry['sha256'],native,batch_rows=self.batch_rows,
            guard=self.guard,consume=consume,metrics=self.metrics,source_kind='update' if self.entry['role']=='update' else 'rib',policy=self.policy)
        if audit is None:audit=NativeAudit(native)
        checked=audit.complete();receipt=self.root/'source-to-columns-audit.json'
        if receipt.exists():
            if json.loads(receipt.read_text())!=checked:raise ValueError('文件源到列核对结果漂移')
        else:durable(receipt,checked)
        if counts['messages']!=result['records'] or counts['elements']!=result['elements']:raise ValueError('文件导入计数与原生 EOF 不符')
        if self.entry['role']=='update':context['prior_update_last']=prior
        return counts

    def verify_paths(self,data):
        path=self.root/'native/paths.sqlite'
        with self.metrics.measure('path_index_prefetch'):
            with path.open('rb') as stream:
                while stream.read(4*1024**2):pass
        with self.metrics.measure('path_index_count'):
            with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True) as db:count=db.execute('SELECT count(*) FROM paths').fetchone()[0]
        if count!=data['tables']['paths']['count']:raise ValueError('原生路径唯一索引与持久化行数不符')
        # 摘要文件是从实际 Parquet 生成的窄表；登记 SHA 后可用于跨文件封存。
        data['native_path_hashes']=[dict(path=str(p),size=p.stat().st_size,sha256=file_sha(p)) for p in self.store.hash_files['paths']]


def seal_native(dsn,schema,data_path,root,owner,checkpoints,plan_id,guard,hook,runtime,memory_limit,metrics):
    """按 checkpoint 源序选择首个路径所有者；临时 SQLite 保持内存有界。"""
    from psycopg2.extras import Json
    import pyarrow.ipc as ipc
    db=connect_duckdb();seal_id=uuid.uuid4().hex;index=Path(root)/('owners-'+seal_id+'.sqlite')
    db.execute('SET memory_limit='+literal(memory_limit));db.execute('SET temp_directory='+literal(Path(root)/('seal-temp-'+seal_id)))
    db.execute('LOAD ducklake');db.execute('LOAD postgres')
    db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (DATA_PATH '+literal(data_path)+')')
    try:
        visible=set()
        for name in OBS_TABLES:
            for path,deleted in db.execute('SELECT data_file,delete_file FROM ducklake_list_files(?,?,schema => ?)', ['lake',name,schema]).fetchall():
                if deleted is not None:raise ValueError('封存前存在删除文件')
                visible.add(path)
        empty_schema=pa.schema([('k1',pa.binary()),('k2',pa.int64()),('digest',pa.binary())])
        runtime.merge_owners(pa.RecordBatchReader.from_batches(empty_schema,[]),index,'')
        for cp in checkpoints:
            guard();owner.check();verify_checkpoint(cp)
            if any(f['path'] not in visible for f in cp['files']):raise ValueError('checkpoint 文件不可见')
            files=cp.get('native_path_hashes',[])
            if not files:
                if cp['tables']['paths']['count']:raise ValueError('原生路径摘要文件缺失')
                continue
            def batches():
                for item in files:
                    path=Path(item['path'])
                    if path.stat().st_size!=item['size'] or file_sha(path)!=item['sha256']:raise ValueError('封存路径行摘要漂移')
                    with pa.memory_map(str(path),'r') as stream:
                        reader=ipc.open_file(stream)
                        for i in range(reader.num_record_batches):yield reader.get_batch(i)
                    guard()
            db.register('path_hash_input',pa.RecordBatchReader.from_batches(empty_schema,batches()))
            try:db.execute('CREATE OR REPLACE TEMP TABLE path_hashes AS SELECT * FROM path_hash_input')
            finally:db.unregister('path_hash_input')
            with metrics.measure('seal_path_index'):
                n,digest=runtime.combine(db.execute('SELECT digest FROM path_hashes ORDER BY k1,k2').fetch_record_batch(65536))
                if (n,digest)!=(cp['tables']['paths']['count'],cp['tables']['paths']['digest']):raise ValueError('封存行摘要与 checkpoint 逻辑内容不符')
                count=runtime.merge_owners(db.execute('SELECT * FROM path_hashes ORDER BY k1').fetch_record_batch(65536),index,cp['attempt'])
                if count!=n:raise ValueError('所有者索引输入计数不符')
            db.execute('DROP TABLE path_hashes')
            event(root,'seal_source_indexed',ordinal=cp['ordinal'],path_rows=count)
        expected_owner_count,expected_owner_digest=runtime.digest(runtime.owner_rows(index))
        owner.check();db.execute('BEGIN')
        try:
            db.register('owner_input',runtime.owner_rows(index))
            try:db.execute(f'INSERT INTO lake.{schema}.path_owner SELECT ?,* FROM owner_input',[seal_id])
            finally:db.unregister('owner_input')
            db.register('seal_input',pa.Table.from_pylist([dict(seal_id=seal_id,source_id=cp['source_id'],attempt=cp['attempt'],ordinal=cp['ordinal'],checkpoint_digest=cp['digest']) for cp in checkpoints]))
            try:db.execute(f'INSERT INTO lake.{schema}.seal_selection SELECT * FROM seal_input')
            finally:db.unregister('seal_input')
            db.execute('COMMIT')
        except BaseException:db.execute('ROLLBACK');raise
        snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        count,digest=runtime.digest(db.execute(f'SELECT path_key,attempt FROM lake.{schema}.path_owner AT (VERSION => {snapshot}) WHERE seal_id=? ORDER BY path_key',[seal_id]).fetch_record_batch(65536))
        if (count,digest)!=(expected_owner_count,expected_owner_digest):raise ValueError('实际所有者内容与临时索引不符')
        with sqlite3.connect('file:'+str(index)+'?mode=ro',uri=True) as local:
            if count!=local.execute('SELECT count(*) FROM owners').fetchone()[0]:raise ValueError('所有者落盘计数不符')
        seal=dict(version=VERSION,run_id=owner.run_id,plan_id=plan_id,schema=schema,snapshot=snapshot,seal_id=seal_id,
            checkpoints=checkpoints,io=dict(observation_rows_rewritten=0,path_owner_rows_written=count,selection_rows_written=len(checkpoints),
                data_file_hash_bytes=sum(f['size'] for cp in checkpoints for f in cp['files']),scope='seal_verification_and_index_only'),
            path_owner_count=count,path_owner_digest=digest,qualification='observation_sealed',business='not_run')
        seal['digest']=sha(seal);durable(Path(root)/('seal-'+seal_id+'.json'),seal)
        hook('seal_pre_commit',len(checkpoints),owner)
        with owner.transaction() as cursor:cursor.execute("UPDATE observation_m2.runs SET state='observation_sealed',seal=%s WHERE run_id=%s",(Json(seal),owner.run_id))
        hook('seal_post_commit',len(checkpoints),owner)
        return seal
    finally:db.close()
