"""文件流水线内部只读接缝；不授予公开 observation / canonical 封存资格。"""
from dataclasses import dataclass

import psycopg2

from data_pipeline.bgp.archive.checkpoint import OBS_TABLES, sha, verify_checkpoint
from data_pipeline.bgp.archive.message_reader import ObservationReader, SourceStart
from data_pipeline.bgp.ordered_reader import _adapt
from data_pipeline.bgp.record_types import SourceBinding, NATIVE_INTERPRETATION_VERSION
from data_pipeline.bgp.archive.store import connect_duckdb, literal


def selected_prefix(dsn, run_id):
    """只读取 PG 已选择的连续文件；prepared 文件和当前写入位置不可消费。"""
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True, isolation_level='REPEATABLE READ')
        with pg.cursor() as c:
            c.execute("SELECT to_regclass('observation_m2.runs')")
            if c.fetchone()[0] is None:return None
            c.execute('SELECT plan_id,plan,schema_name,state,seal FROM observation_m2.runs WHERE run_id=%s', (run_id,))
            row=c.fetchone()
            if row is None:return None
            plan_id,plan,schema,state,seal=row
            c.execute('SELECT ordinal,payload FROM observation_m2.checkpoints WHERE run_id=%s ORDER BY ordinal', (run_id,))
            checkpoints=c.fetchall()
    previous=None
    for ordinal,(actual,cp) in enumerate(checkpoints):
        if ordinal>=len(plan['inputs']):raise ValueError('checkpoint 超出绑定输入')
        entry=plan['inputs'][ordinal]
        if (actual,cp['ordinal'],cp['previous'],cp['source_id'],cp['source_sha'],cp['plan_id']) != (
                ordinal,ordinal,previous,entry['source_id'],entry['sha256'],plan_id):
            raise ValueError('checkpoint 链或输入绑定损坏')
        if sha({k:v for k,v in cp.items() if k!='digest'})!=cp['digest']:
            raise ValueError('checkpoint 摘要不符')
        if cp['raw']!='verified_source_eof' or cp['ingest']!='complete':raise ValueError('文件未完成')
        counts=cp['counts']
        if counts['messages']!=sum(counts[k] for k in ('decoded','rejected','unsupported')):
            raise ValueError('checkpoint 解析分类计数不符')
        previous=cp['digest']
    return dict(plan_id=plan_id,plan=plan,schema=schema,state=state,seal=seal,
                checkpoints=[cp for _,cp in checkpoints])


class CheckpointSelection:
    def __init__(self,dsn,run_id,ordinal,expected_digest):
        self.dsn=dsn;self.run_id=run_id;self.columns=OBS_TABLES
        prefix=selected_prefix(dsn,run_id)
        if prefix is None or not 0<=ordinal<len(prefix['checkpoints']):raise ValueError('文件尚未选择 checkpoint')
        self.cp=prefix['checkpoints'][ordinal]
        if self.cp['digest']!=expected_digest:raise ValueError('绑定 checkpoint 漂移')
        self.checkpoints=prefix['checkpoints'][:ordinal+1]
        self.plan=prefix['plan'];self.plan_id=prefix['plan_id'];self.schema=prefix['schema']
        self.manifest=self.plan['manifest'];self.snapshot=self.cp['snapshot']
        verify_checkpoint(self.cp)

    def check_sources(self,sources):
        if tuple(sources)!=(self.cp['source_id'],):raise ValueError('只允许当前文件选择')
        again=selected_prefix(self.dsn,self.run_id)
        if (again is None or again['plan']!=self.plan or again['plan_id']!=self.plan_id or
                again['schema']!=self.schema or again['checkpoints'][:len(self.checkpoints)]!=self.checkpoints):
            raise ValueError('已选择的 checkpoint 前缀漂移')

    def connect(self):
        self.check_sources((self.cp['source_id'],));db=connect_duckdb()
        try:
            db.execute('LOAD ducklake');db.execute('LOAD postgres')
            db.execute('ATTACH '+literal('ducklake:postgres:'+self.dsn)+' AS lake (READ_ONLY)')
            return db
        except BaseException:db.close();raise

    def table(self,name,source_id=None):
        if name not in self.columns or source_id not in (None,self.cp['source_id']):
            raise ValueError('未绑定的候选表或来源')
        cp=self.cp
        return (f'(SELECT * EXCLUDE(attempt) FROM lake.{self.schema}."{name}" '
                f'AT (VERSION => {int(self.snapshot)}) WHERE attempt={literal(cp["attempt"])} '
                f'AND rowid<={int(cp["tables"][name]["max_rowid"])})')


@dataclass(frozen=True)
class CheckpointBinding:
    """候选身份固定于完整运行计划；seal_snapshot 仅适配共用流的物理版本字段。"""
    binding_id: str
    collector: str
    observation_run: str
    seal_snapshot: int
    sources: tuple
    profile: str = 'route-file-candidate/v1'
    interpretation_version: str = NATIVE_INTERPRETATION_VERSION

    @property
    def ordered_source_ids(self):return tuple(s.source_id for s in self.sources)


class CheckpointReader(ObservationReader):
    def __init__(self,dsn,run_id,ordinal,expected_digest,*,guard=lambda:None):
        s=self.selection=CheckpointSelection(dsn,run_id,ordinal,expected_digest)
        self.dsn=dsn;self.run_id=run_id;self.snapshot=s.snapshot;self.schema=s.schema
        self.manifest=s.manifest;self.sources=(s.cp['source_id'],)
        entry=next((e for e in s.manifest['inputs'] if e['source_id']==s.cp['source_id']),None)
        if entry is None:raise ValueError('参考资料不进入路由状态流')
        self.batch_rows=512;self.batch_bytes=4*1024**2;self.guard=guard
        self.starts=[SourceStart(run_id,s.snapshot,entry['source_id'],entry['sha256'],entry['role'],
                                 s.cp['counts']['messages'],s.cp['counts']['elements'])]

    def ordered(self,binding_id):
        self.selection.check_sources(self.sources)
        cps={cp['source_id']:cp for cp in self.selection.checkpoints}
        sources=[]
        for entry in self.manifest['inputs']:
            cp=cps.get(entry['source_id'])
            if cp is None:break
            sources.append(SourceBinding(entry['source_id'],entry['sha256'],entry['role'],cp['digest'],
                *(cp['counts'][k] for k in ('messages','elements','decoded','rejected','unsupported'))))
        binding=CheckpointBinding(binding_id,self.manifest['collector'],self.run_id,self.snapshot,tuple(sources))
        yield from _adapt(self.stream(),binding,self.sources)

    def endpoints(self):
        db=self.connect()
        try:
            return db.execute(f'''SELECT peer_ip,peer_asn,local_ip,local_asn,interface,min(message_id),count(*)
                FROM {self.bound_table('messages')} WHERE peer_ip IS NOT NULL AND NOT coalesce(local_message,false)
                GROUP BY peer_ip,peer_asn,local_ip,local_asn,interface''').fetchall()
        finally:db.close()

    def peers(self):
        db=self.connect()
        try:return db.execute(f'SELECT "ip",asn,bgp_id,table_record,"index" FROM {self.bound_table("peers")}').fetchall()
        finally:db.close()
