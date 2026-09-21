"""M3独立资格表与完整枚举；旧profile不要求本表。"""
import json
from dataclasses import asdict
import hashlib
import psycopg2
import pyarrow as pa
from psycopg2.extras import execute_values
from data_pipeline.analysis.detection.store import DetectionStore, encode
from data_pipeline.analysis.detection.qualification_contract import FIELDS, validate_entry, validate_identity

DDL = 'ordinal BIGINT, entry_id VARCHAR, kind VARCHAR, incident_id VARCHAR, revision BIGINT, source_id VARCHAR, gap_id VARCHAR, payload_json VARCHAR'


class M3Store(DetectionStore):
    def __init__(self, *args, **kwargs):
        identity=kwargs.get('identity',args[3] if len(args)>3 else None)
        validate_identity(identity,allow_synthetic=True)
        super().__init__(*args, **kwargs)
        self.qcount, self.qpending, self.qbytes = 0, [], 0
        self.db.execute(f'CREATE TABLE lake.{self.schema}.m3_entries ({DDL})')
        if self.mirror_body:
            with self.pg, self.pg.cursor() as c:
                c.execute('CREATE TABLE IF NOT EXISTS detection.m3_entries (run_id TEXT, ' + DDL.replace('VARCHAR','TEXT') + ', PRIMARY KEY(run_id,ordinal), UNIQUE(run_id,entry_id))')

    def emit_qualification(self, row):
        self.guard()
        if self.closed_for_writes or row['ordinal'] != self.qcount:
            raise ValueError('资格输出已关闭或序号不连续')
        validate_entry(row,self.run_id,self.identity)
        value = dict(row, payload_json=encode(row['payload_json']))
        size = len(encode(value).encode())
        if size > 8 * 1024**2:
            raise ValueError('单条资格超过字节保护')
        if self.qpending and (len(self.qpending) >= self.batch_rows or self.qbytes+size > self.batch_bytes):
            self.flush_qualification()
        if not self.mirror_body:
            self.written_inventory.add("m3_entries",value)
        self.qpending.append(value)
        self.qbytes += size
        self.qcount += 1

    def flush_qualification(self):
        self.guard()
        if not self.qpending:
            return
        table = pa.Table.from_pylist(self.qpending, schema=pa.schema([
            (name, pa.int64() if name in ('ordinal','revision') else pa.string()) for name in FIELDS]))
        self.db.register('qualification_batch', table)
        try:
            self.db.execute(f'INSERT INTO lake.{self.schema}.m3_entries SELECT * FROM qualification_batch')
            if self.mirror_body:
                with self.pg, self.pg.cursor() as c:
                    execute_values(c, 'INSERT INTO detection.m3_entries VALUES %s',
                                   [(self.run_id, *(r[n] for n in FIELDS)) for r in self.qpending])
        finally:
            self.db.unregister('qualification_batch')
        self.qpending.clear()
        self.qbytes = 0

    def finish(self, *, validate_before_commit=lambda: None):
        validate_identity(self.identity,allow_synthetic=True)
        if not self.mirror_body:
            return self._finish_lake(validate_before_commit)
        self.flush_qualification()
        self.flush()
        table = f'lake.{self.schema}.m3_entries'
        gaps, sources, events, qualified = set(), [], set(), set()
        source_gap_counts = {}
        scientific = self.db.execute(f"SELECT incident_id,revision FROM lake.{self.schema}.records WHERE record_kind='business_revision'").fetch_record_batch(self.batch_rows)
        for batch in scientific:
            self.guard()
            events.update((row['incident_id'],row['revision']) for row in batch.to_pylist())
        # 独立SQL游标在同连接下不可同时消费；先完成科学键枚举再开资格批流。
        rows = self.db.execute(f'SELECT * FROM {table} ORDER BY ordinal').fetch_record_batch(self.batch_rows)
        count = 0
        lake_digest=hashlib.sha256()
        for batch in rows:
            self.guard()
            for row in batch.to_pylist():
                if row['ordinal'] != count: raise ValueError('资格序号损坏')
                count += 1
                payload = validate_entry(row,self.run_id,self.identity)
                lake_digest.update((row["entry_id"]+"\n").encode())
                if payload['component_run'] != self.run_id or payload['binding_ref'] != self.identity['input_binding_id']:
                    raise ValueError('资格绑定错误')
                if row['kind'] == 'scope_gap':
                    if row['gap_id'] in gaps: raise ValueError('重复Gap')
                    gaps.add(row['gap_id'])
                    source_gap_counts[row['source_id']] = source_gap_counts.get(row['source_id'],0)+1
                elif row['kind'] == 'event_qualification':
                    key = row['incident_id'], row['revision']
                    if key not in events: raise ValueError('资格引用不存在的科学revision')
                    qualified.add(key)
                elif row['kind'] == 'source_coverage':
                    sources.append(row['source_id'])
                    receipt=payload['receipt']
                    if receipt['raw']['source_id'] != row['source_id'] or receipt['parse_counts']['gaps'] != source_gap_counts.get(row['source_id'],0):
                        raise ValueError('来源Gap覆盖与End不符')
                else: raise ValueError('未知资格类型')
                if any(g not in gaps for g in payload.get('gap_refs', [])):
                    raise ValueError('资格引用缺少Gap')
        if sources != self.identity['selected_sources'] or qualified != events or count != self.qcount:
            raise ValueError('资格覆盖枚举不完整')
        pgcheck=psycopg2.connect(self.dsn)
        pg_digest=hashlib.sha256()
        pg_count=0
        try:
            pgcheck.set_session(readonly=True)
            with pgcheck.cursor(name='qualification_integrity') as c:
                c.execute('SELECT '+','.join(FIELDS)+' FROM detection.m3_entries WHERE run_id=%s ORDER BY ordinal',(self.run_id,))
                while True:
                    self.guard()
                    batch=c.fetchmany(self.batch_rows)
                    if not batch:break
                    for values in batch:
                        row=dict(zip(FIELDS,values))
                        validate_entry(row,self.run_id,self.identity)
                        if row['ordinal']!=pg_count:raise ValueError('PG资格序号不符')
                        pg_count+=1
                        pg_digest.update((row['entry_id']+'\n').encode())
        finally:pgcheck.close()
        if pg_count!=count or pg_digest.digest()!=lake_digest.digest():
            raise ValueError('PG与湖资格内容/计数不符')
        self.identity['qualification_counts'] = dict(entries=count, gaps=len(gaps), sources=len(sources), revisions=len(events))
        # identity在候选阶段同步固定，Reader首尾继续绑定完整identity。
        with self.pg, self.pg.cursor() as c:
            c.execute('UPDATE detection.runs SET identity=%s WHERE run_id=%s AND state=\'candidate\'', (encode(self.identity),self.run_id))
        return super().finish(validate_before_commit=validate_before_commit)

    def _finish_lake(self, validate_before_commit):
        from data_pipeline.analysis.detection.lake_integrity import validate_snapshot
        if self.closed_for_writes or self.state_count is None:
            raise ValueError('输入或状态未完成，禁止完成')
        self.flush_qualification()
        self.flush()
        self.closed_for_writes=True
        snapshot=self.db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        source_binding=validate_before_commit()
        expected=self.written_inventory.result()
        if any(expected[table]['rows']!=count for table,count in (('records',self.count),('state_entries',self.state_count),('m3_entries',self.qcount))):
            raise ValueError('生产枚举与Writer计数不符')
        report=validate_snapshot(self.dsn,self.run_id,snapshot,self.identity,expected,
                                 source_binding=source_binding,batch_rows=self.batch_rows,guard=self.guard,scope=asdict(self.scope))
        if validate_before_commit()!=source_binding:raise ValueError('完整验收期间上游绑定漂移')
        self.identity['qualification_counts']=report['qualification_counts']
        self.identity['storage_integrity']=report
        with self.pg,self.pg.cursor() as c:
            c.execute("UPDATE detection.runs SET identity=%s WHERE run_id=%s AND state='candidate'",(encode(self.identity),self.run_id))
            if c.rowcount!=1:raise ValueError('候选状态已改变')
        return self._complete(snapshot,validate_before_commit=validate_before_commit)
