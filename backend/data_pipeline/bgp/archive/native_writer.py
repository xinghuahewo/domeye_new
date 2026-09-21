"""首份 RIB 的显式原生 M2 入口；沿用 v1 表、typed 摘要和封存选择。"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from psycopg2.extras import Json

from data_pipeline.bgp.archive.checkpoint import AttemptStore, Guard, Metrics, OBS_TABLES, Owner, VERSION, durable, event, file_sha, plan_for, setup, sha, verify_checkpoint
from data_pipeline.bgp.input.native_parser import NativeRuntime, DISPLAY_FIELDS, PARSER_AUTHORITY, VERSION as NATIVE_VERSION, _stamp, release_segment_cache
from data_pipeline.bgp.replay.run_from_files import code_identity
from data_pipeline.bgp.archive.store import literal


def arrow_file(path):
    with pa.memory_map(str(path),'r') as f:return ipc.open_file(f).read_all()


class NativeAudit:
    """核对全部原消息定位和摘要；业务解释字段核对独立分层样本。"""
    key={'messages':('message_id',),'elements':('event_id',),'paths':('path_key',),'peers':('table_record','index')}
    def __init__(self,root):
        self.frames=arrow_file(root/'source-frames.arrow');self.prefixes=arrow_file(root/'source-prefixes.arrow')
        self.source_kind=json.loads((root/'source-audit.json').read_text()).get('source_kind','rib')
        self.position=0;self.prefix_elements=0;self.samples={};self.expected={};self.seen={}
        for name in self.key:
            path=root/('sample-'+name+'.arrow')
            rows=arrow_file(path).to_pylist() if path.exists() else []
            if name=='messages':
                policy=json.loads((root/'binding.json').read_text()).get('policy','strict/v1')
                for row in rows:
                    obj=json.loads(row['interpretation']);obj['policy']=policy
                    row['interpretation']=json.dumps(obj,ensure_ascii=False)
            self.samples[name]=pa.array([r[self.key[name][0]] for r in rows]) if rows else None
            self.expected[name]={tuple(r[k] for k in self.key[name]):r for r in rows};self.seen[name]=set()

    def check(self,name,table):
        if name=='messages':
            actual=table.select(self.frames.column_names)
            if not actual.equals(self.frames.slice(self.position,len(actual))):raise ValueError('全量原消息边界或原字节摘要不符')
            self.message_ids=table['message_id'];self.message_prefixes=self.prefixes.slice(self.position,len(actual))
            self.position+=len(actual)
        if name=='elements':
            indices=pc.index_in(table['message_id'],value_set=self.message_ids)
            if indices.null_count:raise ValueError('元素引用非本段消息')
            expected=self.message_prefixes.take(indices)
            if self.source_kind=='rib' and not table.select(expected.column_names).equals(expected):raise ValueError('全量前缀规范文本、地址族或原字节不符')
            self.prefix_elements+=len(table)
        if name not in self.samples or self.samples[name] is None:return
        chosen=table.filter(pc.is_in(table[self.key[name][0]],value_set=self.samples[name]))
        for row in chosen.to_pylist():
            key=tuple(row[k] for k in self.key[name]);expected=self.expected[name].get(key)
            fields=[k for k in row if k not in DISPLAY_FIELDS[name]]
            if expected is None or any(row[k]!=expected[k] for k in fields):raise ValueError('原生分层样本证据字段不符：'+name)
            if name=='elements':
                from ipaddress import ip_network
                if ip_network(row['prefix'],strict=True)!=ip_network(expected['prefix'],strict=True):raise ValueError('规范网络键不符')
            self.seen[name].add(key)

    def complete(self):
        if self.position!=len(self.frames):raise ValueError('原消息核对未到 EOF')
        if any(self.seen[n]!=set(self.expected[n]) for n in self.key):raise ValueError('分层样本遗漏')
        return dict(parser_authority=PARSER_AUTHORITY,all_message_headers_and_digests=len(self.frames),all_element_raw_prefixes=self.prefix_elements,
                    prefix_validator='independent_source_scan' if self.source_kind=='rib' else 'native_raw_cursor_against_libbgpdump',
                    sample_rows={n:len(v) for n,v in self.seen.items()},display_text='bgpdump; not compared to legacy Python formatting')



@contextmanager
def consuming(consume, tables):
    """计算和归档共享同一段；两方结束前不得释放 IPC 缓存或确认消费。"""
    pending = consume(tables) if consume is not None else None
    try:
        yield
    except BaseException as primary:
        if pending is not None:
            try: pending.result()
            except BaseException as error: primary.calculation_error = error
        raise
    else:
        if pending is not None: pending.result()


class NativeStore(AttemptStore):
    digest_batch_rows=65536
    def __init__(self,*args,runtime,attempt,metrics,**kwargs):
        super().__init__(*args,**kwargs);self.runtime=runtime;self.attempt=attempt;self.metrics=metrics
        self.before_files={p:v for p,v in self.before_files.items() if '/native/'+attempt+'/' not in p}
        self.hash_files={name:[] for name in ('messages','elements','paths','peers')}
        self.readback_files={}
        self.db.execute(f'CREATE TABLE IF NOT EXISTS lake.{self.schema}.native_segments(attempt VARCHAR,ordinal BIGINT,payload VARCHAR)')

    def typed_digest(self,reader):
        return self.runtime.digest(reader)

    def check_element_ordinals(self,relation):
        # 同一消息的所有元素进入同一区；保持原判据，降低 DISTINCT 聚合的驻留规模。
        for bucket in range(16):
            self.guard()
            bad=self.db.execute(f'SELECT 1 FROM {relation} WHERE hash(message_id)%16=? GROUP BY message_id HAVING min(ordinal)<>0 OR max(ordinal)+1<>count(*) OR count(DISTINCT ordinal)<>count(*) LIMIT 1',[bucket]).fetchone()
            if bad:raise ValueError('元素序号损坏')

    def checkpoint_data(self,expected):
        data=super().checkpoint_data(expected)
        if {f['path']:f['sha256'] for f in data['files']}!=self.readback_files:
            raise ValueError('固定快照文件与分段读回核验身份不符')
        return data

    def table_digest(self,name,snapshot):
        mode={'messages':1,'elements':2,'paths':3,'peers':4}.get(name)
        if mode is None:return super().table_digest(name,snapshot)
        event(self.root.parent,'table_digest_start',table=name);start=time.monotonic()
        files=self.hash_files[name]
        if not files:return super().table_digest(name,snapshot)
        def batches():
            for path in files:
                with pa.memory_map(str(path),'r') as stream:
                    reader=ipc.open_file(stream)
                    for index in range(reader.num_record_batches):yield reader.get_batch(index)
        with pa.memory_map(str(files[0]),'r') as stream:schema=ipc.open_file(stream).schema
        hashed=pa.RecordBatchReader.from_batches(schema,batches());self.db.register('native_hash_input',hashed)
        try:
            self.db.execute('CREATE OR REPLACE TEMP TABLE native_typed_hashes AS SELECT * FROM native_hash_input')
        finally:self.db.unregister('native_hash_input')
        try:
            result=self.runtime.combine(self.db.execute('SELECT digest FROM native_typed_hashes ORDER BY k1,k2').fetch_record_batch(65536))
        finally:self.db.execute('DROP TABLE native_typed_hashes')
        event(self.root.parent,'table_digest_complete',table=name,rows=result[0],seconds=time.monotonic()-start)
        return result

    def hash_persisted(self,name,actual,ordinal):
        """每段实际 Parquet 读回后计算；与下一段 C 解析重叠，临时摘要不跨进程复用。"""
        mode={'messages':1,'elements':2,'paths':3,'peers':4}.get(name)
        if mode is None:return  # 附属小表在 checkpoint 中按既有键排序、原生编码。
        with self.metrics.measure('persisted_typed_hash'):
            reader=self.runtime.hash_rows(actual.drop(['attempt']).to_reader(max_chunksize=65536),mode)
            path=self.root/f'hashes-{ordinal:06d}-{name}.arrow'
            with pa.OSFile(str(path),'wb') as stream:
                with ipc.new_file(stream,reader.schema) as writer:
                    for batch in reader:writer.write_batch(batch)
            self.hash_files[name].append(path)

    def restore_completed(self,native,entry,batch_rows,data_path):
        """已完成源到列核对后仅恢复 checkpoint；从登记的 Parquet 重新生成行摘要。"""
        binding=json.loads((native/'binding.json').read_text())
        expected=dict(version=NATIVE_VERSION,parser_authority=PARSER_AUTHORITY,source_id=entry['source_id'],sha256=entry['sha256'],build=self.runtime.identity,batch_rows=batch_rows)
        if binding!=expected:raise ValueError('checkpoint 恢复绑定漂移')
        result=json.loads((native/'complete.json').read_text());source=json.loads((native/'source-audit.json').read_text())
        checked=json.loads((native.parent/'source-to-columns-audit.json').read_text())
        if _stamp(Path(entry['path']))!=source['source_stamp'] or file_sha(entry['path'])!=entry['sha256']:raise ValueError('原件漂移')
        if any(result[k]!=source[k] for k in ('records','elements','decoded_bytes')):raise ValueError('原生 EOF 与源核对不符')
        if checked['parser_authority']!=PARSER_AUTHORITY or checked['all_message_headers_and_digests']!=result['records'] or checked['all_element_raw_prefixes']!=result['elements']:raise ValueError('完整源到列核对回执不符')
        if any(checked['sample_rows'][name]!=source['sample_rows'][name] for name in ('messages','elements','paths','peers')):raise ValueError('完整源样本核对回执不符')
        rows=self.db.execute(f'SELECT ordinal,payload FROM lake.{self.schema}.native_segments WHERE attempt=? ORDER BY ordinal',[self.attempt]).fetchall()
        if len(rows)!=result['segments']:raise ValueError('完整分段登记数量不符')
        record=elements=0
        for ordinal,(saved_ordinal,payload) in enumerate(rows):
            self.guard();self.owner.check();saved=json.loads(payload);segment=saved['input']
            if saved_ordinal!=ordinal or segment['segment']!=ordinal or segment['first_record']!=record:raise ValueError('完整分段链不连续')
            marker=native/f'segment-{ordinal:06d}'/'committed.json'
            if json.loads(marker.read_text())!=segment or 'files' not in segment:raise ValueError('完整分段回执漂移')
            names=sorted(n for n,c in segment['counts'].items() if c)
            if sorted(f['table'] for f in saved['files'])!=names:raise ValueError('已登记文件清单不符')
            for item in saved['files']:
                expected_path=Path(data_path)/'native'/self.attempt/f'segment-{ordinal:06d}'/(item['table']+'.parquet')
                if item['path']!=str(expected_path):raise ValueError('已登记文件位置不符')
                with self.metrics.measure('checkpoint_readback'):
                    if file_sha(item['path'])!=item['sha256']:raise ValueError('已登记分段文件漂移')
                    actual=pq.read_table(item['path'])
                    if len(actual)!=segment['counts'][item['table']]:raise ValueError('已登记分段行数不符')
                self.hash_persisted(item['table'],actual,ordinal);self.readback_files[item['path']]=item['sha256']
            record=segment['next_record'];elements+=segment['counts']['elements']
            event(native.parent,'checkpoint_segment_restored',ordinal=ordinal,persisted_elements=elements,total_elements=result['elements'])
        if record!=result['records'] or elements!=result['elements']:raise ValueError('完整分段合计不符')
        return result

    def import_segment(self,directory,segment,data_path,audit,hook,additional=None,consume=None):
        ordinal=segment['segment'];self.guard();self.owner.check()
        old=self.db.execute(f'SELECT payload FROM lake.{self.schema}.native_segments WHERE attempt=? AND ordinal=?',[self.attempt,ordinal]).fetchall()
        if len(old)>1:raise ValueError('分段登记重复')
        if old:
            saved=json.loads(old[0][0])
            if saved['input']!=segment:raise ValueError('分段恢复内容漂移')
            tables={}
            for item in saved['files']:
                if file_sha(item['path'])!=item['sha256']:raise ValueError('已登记分段文件漂移')
                actual=pq.read_table(item['path'])
                self.hash_persisted(item['table'],actual,ordinal)
                if consume is not None:tables[item['table']]=actual.drop(['attempt'])
                self.readback_files[item['path']]=item['sha256']
            for f in sorted(directory.glob('*.arrow'),key=lambda p:(p.stem!='messages',p.name)):audit.check(f.stem,arrow_file(f))
            with consuming(consume,tables):pass
            with self.metrics.measure('ipc_cache_release'):release_segment_cache(directory)
            return
        output=Path(data_path)/'native'/self.attempt/directory.name;output.mkdir(parents=True,exist_ok=True)
        files=[]
        tables={}
        for f in sorted(directory.glob('*.arrow'),key=lambda p:(p.stem!='messages',p.name)):
            table=arrow_file(f);audit.check(f.stem,table);tables[f.stem]=table
        for name,table in (additional or {}).items():
            if len(table):tables[name]=pa.concat_tables([tables[name],table]) if name in tables else table
        with consuming(consume,tables):
            hook('before_segment_archive',ordinal,self.owner)
            for name,table in tables.items():
                table=table.add_column(0,'attempt',pa.repeat(pa.scalar(self.attempt),len(table)))
                dest=output/(name+'.parquet')
                with self.metrics.measure('parquet_write'):
                    if not dest.exists():
                        temp=dest.with_suffix('.partial');pq.write_table(table,temp,compression='zstd',compression_level=1,row_group_size=262144)
                        with temp.open('rb') as stream:os.fsync(stream.fileno())
                        temp.replace(dest)
                with self.metrics.measure('parquet_readback'):
                    actual=pq.read_table(dest)
                    if not table.equals(actual):raise ValueError('Arrow 与实际 Parquet 列式内容不一致')
                    files.append(dict(path=str(dest),table=name,size=dest.stat().st_size,sha256=file_sha(dest)))
                self.hash_persisted(name,actual,ordinal)
                self.readback_files[str(dest)]=files[-1]['sha256']
            fd=os.open(output,os.O_RDONLY)
            try:os.fsync(fd)
            finally:os.close(fd)
            hook('segment_files_ready',ordinal,self.owner)
            with self.metrics.measure('catalog_commit'):
                self.owner.check();self.db.execute('BEGIN')
                try:
                    for f in files:self.db.execute('CALL ducklake_add_data_files(?, ?, ?, schema => ?)', ['lake',f['table'],f['path'],self.schema])
                    payload=json.dumps(dict(input=segment,files=files),ensure_ascii=False)
                    self.db.execute(f'INSERT INTO lake.{self.schema}.native_segments VALUES (?,?,?)',[self.attempt,ordinal,payload])
                    self.db.execute('COMMIT')
                except BaseException:self.db.execute('ROLLBACK');raise
        with self.metrics.measure('ipc_cache_release'):
            del table,actual
            release_segment_cache(directory)
        self.flushes+=1;hook('after_segment',ordinal,self.owner)


def seal_single(store,owner,cp,plan_id,root,guard,hook):
    """单 checkpoint 已唯一化路径：集中写所有者，保留原 v1 typed 摘要。"""
    db=store.db;schema=store.schema;seal_id=uuid.uuid4().hex;guard();verify_checkpoint(cp)
    visible=store.files(cp['snapshot'])
    if any(f['path'] not in visible for f in cp['files']):raise ValueError('checkpoint 文件不可见')
    relation=store.relation('paths',cp['snapshot'])
    # 此入口严格只有一份 RIB，SQLite 唯一键与路径数核验使所有者无需再构建一份全量索引。
    owner.check();db.execute('BEGIN')
    try:
        db.execute(f'INSERT INTO lake.{schema}.path_owner SELECT ?,path_key,? FROM {relation}',[seal_id,store.attempt])
        db.execute(f'INSERT INTO lake.{schema}.seal_selection VALUES (?,?,?,?,?)',[seal_id,cp['source_id'],store.attempt,0,cp['digest']])
        db.execute('COMMIT')
    except BaseException:db.execute('ROLLBACK');raise
    snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
    query=f'SELECT path_key,attempt FROM lake.{schema}.path_owner AT (VERSION => {snapshot}) WHERE seal_id=? ORDER BY path_key'
    count,digest=store.typed_digest(db.execute(query,[seal_id]).fetch_record_batch(65536))
    if count!=cp['tables']['paths']['count']:raise ValueError('路径所有者计数不符')
    seal=dict(version=VERSION,run_id=owner.run_id,plan_id=plan_id,schema=schema,snapshot=snapshot,seal_id=seal_id,
        checkpoints=[cp],io=dict(observation_rows_rewritten=0,path_owner_rows_written=count,selection_rows_written=1,
            data_file_hash_bytes=sum(f['size'] for f in cp['files']),scope='seal_verification_and_index_only'),
        path_owner_count=count,path_owner_digest=digest,qualification='observation_sealed',business='not_run')
    seal['digest']=sha(seal);durable(root/('seal-'+seal_id+'.json'),seal);hook('seal_pre_commit',1,owner)
    guard()
    with owner.transaction() as c:c.execute("UPDATE observation_m2.runs SET state='observation_sealed',seal=%s WHERE run_id=%s",(Json(seal),owner.run_id))
    hook('seal_post_commit',1,owner);return seal


def produce_native_rib(manifest,dsn,output,*,build,run_id=None,catalog_data_path=None,
        batch_rows=100000,min_free_bytes=2*1024**3,max_rss_bytes=12*1024**3,duckdb_memory_limit='6GB',
        deadline_seconds=1800,hook=lambda *a:None):
    """已授权的离线单 RIB 实验；不接受引用/UPDATE，不创建业务完成标志。"""
    if len(manifest['inputs'])!=1 or manifest.get('references') or manifest['update_sources']:raise ValueError('原生候选只接受首份 RIB')
    if deadline_seconds is not None and not 1<=deadline_seconds<=1800:raise ValueError('有时限试验上限为 30 分钟；显式 None 表示完成优先')
    started=time.monotonic();root=Path(output).resolve();root.mkdir(parents=True,exist_ok=True)
    runtime=NativeRuntime(build);plan,base_id=plan_for(manifest,'strict/v1')
    plan['native']=runtime.identity;plan['parser_authority']=PARSER_AUTHORITY;plan['native_batch_rows']=batch_rows;plan_id=sha([base_id,plan['native'],PARSER_AUTHORITY,batch_rows])
    path=root/'run.json'
    if path.exists():
        saved=json.loads(path.read_text())
        if run_id is not None and saved['run_id']!=run_id:raise ValueError('运行身份冲突')
        run_id=saved['run_id'];attempt=saved['attempt']
    else:run_id=run_id or uuid.uuid4().hex;attempt=uuid.uuid4().hex;durable(path,dict(run_id=run_id,attempt=attempt))
    if not run_id.isalnum():raise ValueError('运行身份无效')
    data_path=str(Path(catalog_data_path or root/'data').resolve());schema='m2_'+run_id
    resource_guard=Guard(root,data_path,min_free_bytes,max_rss_bytes)
    def guard():
        resource_guard()
        if deadline_seconds is not None and time.monotonic()-started>deadline_seconds:raise RuntimeError('30 分钟完成门槛触发')
    guard();owner=Owner(dsn,run_id);metrics=Metrics(root/('samples-'+uuid.uuid4().hex+'.jsonl'),resource_metrics=True);store=None;phase='prepare';cp=None
    pa.set_cpu_count(2);pa.set_io_thread_count(2)
    try:
        setup(owner)
        with owner.transaction() as c:
            c.execute('SELECT plan_id,data_path,state,seal FROM observation_m2.runs WHERE run_id=%s',(run_id,));old=c.fetchone()
            if old:
                if old[:2]!=(plan_id,data_path):raise ValueError('计划或数据根漂移')
                if old[2]=='observation_sealed':
                    from data_pipeline.bgp.archive.selection import Selection
                    selection=Selection(dsn,run_id,old[3]['snapshot']);db=selection.connect();db.close()
                    for item in old[3]['checkpoints']:verify_checkpoint(item)
                    if file_sha(manifest['inputs'][0]['path'])!=manifest['inputs'][0]['sha256']:raise ValueError('封存源漂移')
                    return old[3]
            else:c.execute('INSERT INTO observation_m2.runs VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL)',(run_id,plan_id,Json(plan),schema,data_path,'running'))
            c.execute("UPDATE observation_m2.runs SET state='running',reason=NULL WHERE run_id=%s",(run_id,))
            c.execute('SELECT payload FROM observation_m2.checkpoints WHERE run_id=%s ORDER BY ordinal',(run_id,));cps=c.fetchall()
            if len(cps)>1:raise ValueError('单文件 checkpoint 数量不符')
            cp=cps[0][0] if cps else None
        staging=root/('staging-'+uuid.uuid4().hex);staging.mkdir()
        store=NativeStore(dsn,schema,data_path,staging,owner,guard,batch_rows,duckdb_memory_limit,runtime=runtime,attempt=attempt,metrics=metrics)
        entry=manifest['inputs'][0];native=root/'native'
        if cp is None:
            if (native/'complete.json').exists() and (root/'source-to-columns-audit.json').exists():
                phase='checkpoint_recovery';event(root,'checkpoint_recovery_start')
                result=store.restore_completed(native,entry,batch_rows,data_path)
            else:
                phase='native_pipeline';event(root,'native_parse_start');audit=None
                persisted=0;import_start=time.monotonic();slow=0;last=time.monotonic();last_rows=0;total=None
                def consume(directory,segment):
                    nonlocal audit,persisted,slow,last,last_rows,total
                    if audit is None:
                        audit=NativeAudit(native);total=json.loads((native/'source-audit.json').read_text())['elements']
                    guard();store.import_segment(directory,segment,data_path,audit,hook)
                    persisted+=segment['counts']['elements']
                    elapsed=time.monotonic()-started;rate=persisted/max(time.monotonic()-import_start,.001)
                    estimate=elapsed+(total-persisted)/max(rate,1)+360
                    event(root,'segment_imported',ordinal=segment['segment'],persisted_elements=persisted,total_elements=total,elapsed_seconds=elapsed,projected_total_seconds=estimate)
                    if time.monotonic()-last>=60:
                        recent=(persisted-last_rows)/(time.monotonic()-last);prediction=elapsed+(total-persisted)/max(recent,1)+360
                        slow=slow+1 if deadline_seconds is not None and prediction>deadline_seconds else 0;last=time.monotonic();last_rows=persisted
                        if slow>=2:raise RuntimeError('连续两个窗口预计超过 30 分钟，停止当前写入候选')
                result=runtime.parse(entry['path'],entry['source_id'],entry['sha256'],native,batch_rows=batch_rows,guard=guard,consume=consume,metrics=metrics)
                event(root,'native_parse_complete',**result)
                checked=audit.complete();receipt=root/'source-to-columns-audit.json'
                if receipt.exists():
                    if json.loads(receipt.read_text())!=checked:raise ValueError('恢复审计结果漂移')
                else:durable(receipt,checked)
            phase='checkpoint';event(root,'checkpoint_start')
            counts=dict(messages=result['records'],elements=result['elements'],references=0,decoded=result['records'],rejected=0,unsupported=0)
            with metrics.measure(phase):
                if code_identity()!=plan['code']:raise ValueError('处理期间代码漂移')
                data=store.checkpoint_data(counts)
                # 与磁盘唯一键目录的独立计数相符，不能只信生产端计数。
                import sqlite3
                # 大字典冷态按 B-tree 遍历会产生随机读取；先顺序预读，不省略计数。
                with metrics.measure('path_index_prefetch'):
                    with (native/'paths.sqlite').open('rb') as stream:
                        while stream.read(4*1024**2):pass
                with metrics.measure('path_index_count'):
                    with sqlite3.connect(f'file:{native}/paths.sqlite?mode=ro',uri=True) as index:
                        unique=index.execute('SELECT count(*) FROM paths').fetchone()[0]
                if unique!=data['tables']['paths']['count']:raise ValueError('路径唯一目录与落盘计数不符')
                first=store.db.execute(f"SELECT * FROM {store.relation('messages',data['snapshot'])} WHERE kind='peer_index_table' ORDER BY record DESC LIMIT 1").fetch_arrow_table().to_pylist()[0]
                peers=store.db.execute(f"SELECT * FROM {store.relation('peers',data['snapshot'])} WHERE table_record=? ORDER BY \"index\"",[first['record']]).fetch_arrow_table().to_pylist()
                peers=[dict(p,ip_present=True,asn_present=True) for p in peers]
                context=dict(reference={k:first[k] for k in ('source_id','record','message_id','epoch','microsecond')},reference_peers=peers,baseline_elements=result['elements'],prior_update_last=None)
                stat=Path(entry['path']).stat()
                cp=dict(**data,ordinal=0,source_id=entry['source_id'],attempt=attempt,previous=None,context=context,counts=counts,
                    raw='verified_source_eof',parse='complete',ingest='complete',source_sha=entry['sha256'],
                    source_stat=dict(size=stat.st_size,mtime_ns=stat.st_mtime_ns,ino=stat.st_ino),format=None,plan_id=plan_id,
                    elapsed_seconds=time.monotonic()-started,metrics=metrics.receipt(),metrics_scope='invocation_to_prepared',
                    resources=dict(min_free_bytes=min_free_bytes,max_rss_bytes=max_rss_bytes,batch_rows=batch_rows,duckdb_memory_limit=duckdb_memory_limit),input_bytes=stat.st_size)
                cp['digest']=sha(cp);durable(root/('prepared-'+uuid.uuid4().hex+'.json'),cp)
                store.close();store=None
                hook('before_checkpoint',0,owner)
                with owner.transaction() as c:c.execute('INSERT INTO observation_m2.checkpoints VALUES (%s,0,%s)',(run_id,Json(cp)))
                hook('after_checkpoint',0,owner)
        else:verify_checkpoint(cp)
        if store is None:
            staging=root/('staging-'+uuid.uuid4().hex);staging.mkdir()
            store=NativeStore(dsn,schema,data_path,staging,owner,guard,batch_rows,duckdb_memory_limit,runtime=runtime,attempt=attempt,metrics=metrics)
        phase='seal';event(root,'seal_start');guard()
        with metrics.measure(phase):seal=seal_single(store,owner,cp,plan_id,root,guard,hook)
        guard();durable(root/('metrics-'+uuid.uuid4().hex+'.json'),dict(**metrics.receipt(),elapsed_seconds=time.monotonic()-started))
        return seal
    except BaseException as exc:
        try:
            with owner.transaction() as c:c.execute("UPDATE observation_m2.runs SET state='failed',reason=%s WHERE run_id=%s AND state<>'observation_sealed'",(str(exc),run_id))
        except BaseException:pass
        durable(root/('failure-'+uuid.uuid4().hex+'.json'),dict(phase=phase,reason=str(exc),elapsed_seconds=time.monotonic()-started,metrics=metrics.receipt(),checkpoint_selected=cp is not None))
        raise
    finally:
        if store:store.close()
        owner.close();metrics.close()
