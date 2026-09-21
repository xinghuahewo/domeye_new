"""固定已完成快照的只读消息流；不解析MRT，不决定下游业务过滤。"""
from dataclasses import dataclass
import json
import psycopg2

from data_pipeline.bgp.archive.store import TABLES, connect_duckdb, literal, bound_table as legacy_table


def byte_size(value):
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, str):
        return len(value.encode('utf-8'))
    if isinstance(value, dict):
        return sum(byte_size(k)+byte_size(v) for k, v in value.items())
    if isinstance(value, (tuple, list)):
        return sum(map(byte_size, value))
    return 16


@dataclass(frozen=True)
class SourceStart:
    run_id: str
    snapshot: int
    source_id: str
    content_sha256: str
    role: str
    expected_messages: int
    expected_elements: int


@dataclass(frozen=True)
class MessageBatch:
    run_id: str
    snapshot: int
    source_id: str
    messages: tuple[dict, ...]
    elements: tuple[dict, ...]
    byte_count: int
    source_quality: tuple[dict, ...] = ()


@dataclass(frozen=True)
class SourceEnd:
    run_id: str
    snapshot: int
    source_id: str
    messages: int
    elements: int
    state_messages: int
    eor_records: int
    local_messages: int
    quality_records: int


class ObservationReader:
    def __init__(self, dsn, run_id, expected_snapshot, ordered_source_ids, *,
                 batch_rows=512, batch_bytes=4*1024**2, guard=lambda: None, profile='complete'):
        if not run_id.isalnum() or not isinstance(expected_snapshot, int) or expected_snapshot < 0:
            raise ValueError('必须绑定精确run与snapshot')
        if not ordered_source_ids or len(set(ordered_source_ids)) != len(ordered_source_ids):
            raise ValueError('来源清单为空或重复')
        if not 1 <= batch_rows <= 10000 or batch_bytes < 1:
            raise ValueError('批次限制无效')
        self.dsn, self.run_id, self.snapshot = dsn, run_id, expected_snapshot
        self.sources = tuple(ordered_source_ids)
        self.batch_rows, self.batch_bytes, self.guard = batch_rows, batch_bytes, guard
        self.selection=None
        if profile == 'observation':
            from data_pipeline.bgp.archive.selection import Selection
            self.selection=Selection(dsn,run_id,expected_snapshot)
            self.manifest=self.selection.manifest;self.schema=self.selection.schema
            entries={e['source_id']:e for e in self.manifest['inputs']}
            cps={c['source_id']:c for c in self.selection.checkpoints}
            if any(s not in entries for s in self.sources):raise ValueError('未声明观察来源')
            self.starts=[SourceStart(run_id,expected_snapshot,s,entries[s]['sha256'],entries[s]['role'],cps[s]['counts']['messages'],cps[s]['counts']['elements']) for s in self.sources]
            return
        if profile != 'complete':raise ValueError('未知消费资格')
        with psycopg2.connect(dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute("SELECT to_regclass('domeye.runs')")
                if c.fetchone()[0] is None:raise ValueError('没有旧complete观察资格')
                c.execute('''SELECT r.schema_name,r.snapshot,r.state,s.manifest FROM domeye.runs r
                    JOIN domeye.run_specs s USING(run_id) WHERE run_id=%s''', (run_id,))
                found = c.fetchone()
                if found is None or found[:3] != ('r_'+run_id, expected_snapshot, 'complete'):
                    raise ValueError('候选/失败/错版观察不可消费')
                self.manifest = found[3]
                entries = {e['source_id']: e for e in self.manifest['inputs']}
                if any(s not in entries for s in self.sources):
                    raise ValueError('未声明来源')
                c.execute('SELECT source_id,message_count,element_count FROM domeye.source_receipts WHERE run_id=%s', (run_id,))
                receipts = {s: (m, e) for s, m, e in c.fetchall()}
                c.execute('SELECT source_id,state FROM domeye.inputs WHERE run_id=%s', (run_id,))
                states = dict(c.fetchall())
                self.starts = []
                for source in self.sources:
                    if source not in receipts or states.get(source) != 'validated':
                        raise ValueError('缺少已验证来源回执')
                    e = entries[source]
                    self.starts.append(SourceStart(run_id,expected_snapshot,source, e['sha256'], e['role'], *receipts[source]))
        self.schema = 'r_'+run_id

    def columns(self,name):
        return self.selection.columns[name] if self.selection else TABLES[name]

    def bound_table(self,name,source_id=None):
        return self.selection.table(name,source_id) if self.selection else legacy_table(self.schema,self.snapshot,name)

    def check_sources(self, starts):
        """复验构造时绑定的资格与计数，不用新回执重解释旧Reader。"""
        if self.selection:
            self.selection.check_sources([start.source_id for start in starts])
            return
        with psycopg2.connect(self.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                for start in starts:
                    c.execute('''SELECT i.state,r.message_count,r.element_count FROM domeye.inputs i
                        JOIN domeye.source_receipts r USING(run_id,source_id)
                        WHERE i.run_id=%s AND i.source_id=%s''', (self.run_id,start.source_id))
                    if c.fetchone() != ('validated',start.expected_messages,start.expected_elements):
                        raise ValueError('绑定来源资格或消息/元素回执发生漂移')

    def connect(self):
        if self.selection:return self.selection.connect()
        with psycopg2.connect(self.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute('''SELECT r.schema_name,r.snapshot,r.state,s.manifest FROM domeye.runs r
                    JOIN domeye.run_specs s USING(run_id) WHERE run_id=%s''', (self.run_id,))
                if c.fetchone() != (self.schema,self.snapshot,'complete',self.manifest):
                    raise ValueError('读取前登记状态/固定版本发生漂移')
        self.check_sources(self.starts)
        db = connect_duckdb()
        try:
            db.execute('LOAD ducklake'); db.execute('LOAD postgres')
            db.execute('ATTACH '+literal('ducklake:postgres:'+self.dsn)+' AS lake (READ_ONLY)')
            return db
        except BaseException:
            db.close()
            raise

    def reference_batches(self, source_id):
        if source_id not in {r['sha256'] for r in self.manifest.get('references', [])}:
            raise ValueError('未绑定参考来源')
        if self.selection:
            cp=next(c for c in self.selection.checkpoints if c['source_id']==source_id)
            receipt=('validated',cp['counts']['references'])
        else:
            with psycopg2.connect(self.dsn) as pg:
                pg.set_session(readonly=True)
                with pg.cursor() as c:
                    c.execute('SELECT state,row_count FROM domeye.reference_inputs WHERE run_id=%s AND source_id=%s', (self.run_id,source_id))
                    receipt = c.fetchone()
                    if receipt is None or receipt[0] != 'validated': raise ValueError('参考没有完成回执')
        db = self.connect()
        try:
            result = db.execute(f'SELECT * FROM {self.bound_table("references",source_id)} WHERE source_id=? ORDER BY row', [source_id])
            count = 0
            for batch in result.fetch_record_batch(self.batch_rows):
                self.guard()
                count += batch.num_rows
                yield batch
            if count != receipt[1]: raise ValueError('参考行与来源回执不一致')
        finally:
            db.close()

    def stream(self):
        db = self.connect()
        # 每源一次参数化查询，不按source_id字典序，也不缓存全输入重排。
        mcols = ','.join(f'm."{n}" AS "m_{n}"' for n, _ in self.columns('messages'))
        ecols = ','.join(f'e."{n}" AS "e_{n}"' for n, _ in self.columns('elements'))
        pcols = ','.join(f'p."{n}" AS "p_{n}"' for n, _ in self.columns('paths'))
        try:
            for start in self.starts:
                yield start
                source_quality_count = 0
                # 诊断集合沿 checkpoint.KEYS；Peer 补原类型字段处理同键不同值，保留重复。
                source_quality = db.execute(f'SELECT * FROM {self.bound_table("quality",start.source_id)} WHERE source_id=? AND message_id IS NULL ORDER BY source_id NULLS LAST,message_id NULLS LAST,code NULLS LAST,detail NULLS LAST', [start.source_id])
                for chunk in source_quality.fetch_record_batch(self.batch_rows):
                    for row in chunk.to_pylist():
                        self.guard(); source_quality_count += 1
                        yield MessageBatch(self.run_id,self.snapshot,start.source_id,(),(),byte_size(row),(row,))
                query = f'''SELECT {mcols},{ecols},{pcols},
                    (SELECT to_json(list(struct_pack(afi:=x.afi,safi:=x.safi) ORDER BY x.message_id NULLS LAST,x.afi NULLS LAST,x.safi NULLS LAST)) FROM {self.bound_table("eor",start.source_id)} x WHERE x.message_id=m.message_id) AS eor,
                    (SELECT to_json(list(struct_pack(code:=q.code,detail:=q.detail) ORDER BY q.source_id NULLS LAST,q.message_id NULLS LAST,q.code NULLS LAST,q.detail NULLS LAST)) FROM {self.bound_table("quality",start.source_id)} q WHERE q.message_id=m.message_id) AS quality,
                    (SELECT to_json(list(struct_pack(ip:=r.ip,asn:=r.asn,bgp_id:=r.bgp_id,bgp_id_present:=r.bgp_id_present,peer_index:=r."index",table_record:=r.table_record) ORDER BY r.table_record NULLS LAST,r."index" NULLS LAST,r.bgp_id NULLS LAST,r.ip NULLS LAST,r.asn NULLS LAST,r.bgp_id_present NULLS LAST))
                        FROM {self.bound_table("peers",start.source_id)} r WHERE r.source_id=m.source_id AND r.table_record=m.record) AS peers
                    FROM {self.bound_table("messages",start.source_id)} m LEFT JOIN {self.bound_table("elements",start.source_id)} e USING(message_id)
                    LEFT JOIN {self.bound_table("paths",start.source_id)} p USING(path_key) WHERE m.source_id=? ORDER BY m.record,e.ordinal'''
                result = db.execute(query, [start.source_id])
                messages, elements, size = [], [], 0
                counts = dict(messages=0, elements=0, state_messages=0, eor_records=0, local_messages=0, quality_records=source_quality_count)
                last_record, last_ordinal = -1, -1
                for arrow in result.fetch_record_batch(min(self.batch_rows, 128)):
                    self.guard()
                    for row in arrow.to_pylist():
                        message = None
                        if row['m_record'] != last_record:
                            if row['m_record'] != last_record+1 or row['m_content_sha256'] != start.content_sha256:
                                raise ValueError('消息物理顺序或内容身份不一致')
                            last_record, last_ordinal = row['m_record'], -1
                            message = {n: row['m_'+n] for n, _ in self.columns('messages')}
                            message.update(eor=json.loads(row['eor'] or '[]'), quality=json.loads(row['quality'] or '[]'), peers=json.loads(row['peers'] or '[]'))
                            counts['messages'] += 1
                            counts['state_messages'] += message['kind'] == 'state_change'
                            counts['local_messages'] += bool(message['local_message'])
                            counts['eor_records'] += len(message['eor'])
                            counts['quality_records'] += len(message['quality'])
                        element = None
                        if row['e_event_id'] is not None:
                            if row['e_ordinal'] != last_ordinal+1 or row['p_path_key'] is None:
                                raise ValueError('元素顺序或路径引用损坏')
                            last_ordinal = row['e_ordinal']
                            element = {n: row['e_'+n] for n, _ in self.columns('elements')}
                            element.update({n: row['p_'+n] for n, _ in self.columns('paths')})
                            element.update({n: row['m_'+n] for n in ('source_id','content_sha256','record','epoch','microsecond',
                                'mrt_type','mrt_subtype','local_message','local_ip','local_asn','interface')})
                            counts['elements'] += 1
                        incoming = byte_size(message)+byte_size(element)
                        if messages or elements:
                            if len(messages)+len(elements)+int(message is not None)+int(element is not None) > self.batch_rows or size+incoming > self.batch_bytes:
                                yield MessageBatch(self.run_id,self.snapshot,start.source_id, tuple(messages), tuple(elements), size)
                                messages, elements, size = [], [], 0
                        if message is not None: messages.append(message)
                        if element is not None: elements.append(element)
                        size += incoming
                        self.guard()  # 单条大记录独占批次；不截断，并受RSS保护。
                if messages or elements:
                    yield MessageBatch(self.run_id,self.snapshot,start.source_id, tuple(messages), tuple(elements), size)
                if (counts['messages'], counts['elements']) != (start.expected_messages, start.expected_elements):
                    raise ValueError('来源回执与消息/元素计数不符')
                self.check_sources((start,))
                yield SourceEnd(self.run_id,self.snapshot,start.source_id, **counts)
        finally:
            db.close()
