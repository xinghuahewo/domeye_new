"""Feature M3 独立证据资格；不修改科学工作态，也不清除历史缺口。"""
from dataclasses import asdict
import hashlib
from data_pipeline.bgp.replay.route_replay import identity

PROFILE = 'feature-qualified/v1'
SCHEMA_VERSION = 'feature-qualified-tables/v1'
RULE = 'feature-dependency-qualification/v1'
DIMENSIONS = ('window_counts','announcement_attribution','withdrawal_attribution',
              'resources','sparse','comparison')
TABLES = {
 'input_gaps': [('mode','VARCHAR'),('gap_id','VARCHAR'),('binding_ref','VARCHAR'),
   ('source_id','VARCHAR'),('source_rank','BIGINT'),('upstream_source_rank','BIGINT'),('record','BIGINT'),
   ('message_id','VARCHAR'),('epoch','BIGINT'),('microsecond','BIGINT'),('direction','VARCHAR'),
   ('scope_kind','VARCHAR'),('peer_asn','BIGINT'),('peer_ip','VARCHAR'),('local_asn','BIGINT'),
   ('local_ip','VARCHAR'),('interface','BIGINT'),('all_future_prefixes','BOOLEAN'),('unknown_peer_covers_unseen_asns','BOOLEAN'),('all_future_origin_asns','BOOLEAN'),
   ('unmapped_overlap','VARCHAR'),('parse_status','VARCHAR'),('reason_code','VARCHAR'),
   ('interpretation_digest','VARCHAR'),('content_sha256','VARCHAR'),('offset','BIGINT'),('length','BIGINT'),
   ('raw_digest','VARCHAR'),('gap_rule','VARCHAR')],
 'source_qualities': [('mode','VARCHAR'),('source_id','VARCHAR'),('source_rank','BIGINT'),
   ('evidence_id','VARCHAR'),('record','BIGINT'),('code','VARCHAR'),('detail','VARCHAR')],
 'qualifications': [('qualification_id','VARCHAR'),('rule','VARCHAR'),('mode','VARCHAR'),
   ('source_id','VARCHAR'),('source_rank','BIGINT'),('dimension','VARCHAR'),('coverage','VARCHAR'),
   ('start','VARCHAR'),('end','VARCHAR'),('effective_record','BIGINT'),('effective_phase','VARCHAR'),
   ('dependency_scope','VARCHAR'),('peer_asns','BIGINT[]'),('unknown_peer','BOOLEAN'),('all_origin_asns','BOOLEAN'),
   ('gap_refs','VARCHAR[]'),('quality_refs','VARCHAR[]'),('reason_codes','VARCHAR[]')],
 'qualification_receipts': [('mode','VARCHAR'),('source_id','VARCHAR'),('source_rank','BIGINT'),
   ('binding_ref','VARCHAR'),('upstream_source_rank','BIGINT'),('decoded','BIGINT'),('rejected','BIGINT'),
   ('unsupported','BIGINT'),('gaps','BIGINT'),('qualities','BIGINT'),('qualifications','BIGINT'),
   ('qualification_digest','VARCHAR')],
}
for columns in TABLES.values():columns.append(('window_role','VARCHAR'))


ORDER_KEYS={'input_gaps':('mode','source_rank','record','gap_id'),
            'source_qualities':('mode','source_rank','evidence_id'),
            'qualifications':('mode','source_rank','dimension'),
            'qualification_receipts':('mode','source_rank')}


def rows_digest(rows):
    digest=hashlib.sha256()
    for row in rows:digest.update((identity(row)+'\n').encode())
    return digest.hexdigest()


class Qualification:
    """每mode独立；兼容键折叠导致资源/稀疏保守扩围，单对象A/W不恢复它们。

    范围覆盖未见prefix/ASN。输出标明全域可能相交；不声称所有对象实际受损。
    窗口A与W归属独立：A由本消息路径确定，W依赖旧私有路径。
    """
    def __init__(self, mode, store):
        self.mode,self.store=mode,store
        self.gaps={};self.qualities={};self.history_unknown=False

    def begin(self, rank, binding):
        self.rank,self.binding=rank,binding
        self.current_gaps=[];self.current_qualities=[]
        self.withdrawal_dependency=False
        self.last_record=None
        if rank and self.previous_end < binding.window.start:
            self.quality(None,'input_window_gap',self.previous_end.isoformat()+'..'+binding.window.start.isoformat())

    def quality(self, record, code, detail):
        row=dict(mode=self.mode,source_id=self.binding.source_id,source_rank=self.rank,
                 record=record,code=code,detail=detail)
        evidence=identity(row);row['evidence_id']=evidence
        if evidence not in self.qualities:
            self.qualities[evidence]=row;self.current_qualities.append(evidence)
            self.store.append('source_qualities',row)

    def boundary(self, item):
        self.last_record=item.position.record
        for q in item.raw['quality']:
            self.quality(item.position.record,q['code'],q['detail'])
        gap=item.gap
        if gap is None:return
        key=gap.gap_id
        if key in self.gaps:raise ValueError('Feature重复消费同一Gap')
        scope=gap.scope;endpoint=scope.endpoint
        row=dict(mode=self.mode,gap_id=key,binding_ref=gap.binding_ref,source_id=gap.raw_ref.source_id,
                 source_rank=self.rank,upstream_source_rank=gap.position.source_rank,record=gap.position.record,
                 message_id=gap.message_id,epoch=gap.raw_time.epoch,microsecond=gap.raw_time.microsecond,
                 direction=gap.direction.value,scope_kind=scope.kind.value,peer_asn=scope.peer_asn_evidence,
                 peer_ip=endpoint.peer_ip if endpoint else None,local_asn=endpoint.local_asn if endpoint else None,
                 local_ip=endpoint.local_ip if endpoint else None,interface=endpoint.interface if endpoint else None,
                 all_future_prefixes=True,unknown_peer_covers_unseen_asns=scope.unknown_peer_covers_unseen_asns,all_future_origin_asns=True,
                 unmapped_overlap=scope.unmapped_rib_overlap,parse_status=gap.parse_status.value,
                 reason_code=gap.reason_code,interpretation_digest=gap.interpretation_digest,
                 content_sha256=gap.raw_ref.content_sha256,offset=gap.raw_ref.offset,length=gap.raw_ref.length,
                 raw_digest=gap.raw_ref.raw_digest,gap_rule=gap.rule_version)
        self.gaps[key]=row;self.current_gaps.append(key)
        self.store.append('input_gaps',row)

    def element(self, row):
        # 原过滤继续在科学路径执行。本标记仅保守覆盖可能依赖缺口前路径的W。
        if row['action']=='withdraw' and (self.gaps or self.qualities):
            self.withdrawal_dependency=True

    def finish(self, end):
        if end.parse_counts.gaps!=len(self.current_gaps):raise ValueError('Feature漏Gap，拒绝来源完成')
        if end.raw.messages!=end.parse_counts.decoded+end.parse_counts.rejected+end.parse_counts.unsupported:
            raise ValueError('Feature解释计数不完整')
        window=self.binding.window
        base='complete' if window.coverage=='complete' and self.binding.message_quality_state=='complete' else 'unknown'
        if base=='unknown':self.history_unknown=True
        current_bad=bool(self.current_gaps or self.current_qualities)
        history_bad=bool(self.gaps or self.qualities)
        rows=[]
        for dimension in DIMENSIONS:
            current_only=dimension in ('window_counts','announcement_attribution')
            bad=current_bad if current_only else (current_bad or self.withdrawal_dependency if dimension=='withdrawal_attribution' else history_bad)
            coverage='partial' if bad else ('unknown' if not current_only and self.history_unknown else base)
            if not self.rank and dimension in ('window_counts','announcement_attribution','withdrawal_attribution','comparison'):
                coverage='not_applicable'
            refs=self.current_gaps if current_only else list(self.gaps)
            quality_refs=self.current_qualities if current_only else list(self.qualities)
            row=dict(rule=RULE,mode=self.mode,source_id=self.binding.source_id,source_rank=self.rank,
                     dimension=dimension,coverage=coverage,start=window.start.isoformat(),end=window.end.isoformat(),
                     effective_record=self.last_record,effective_phase='source_end',
                     dependency_scope='legacy_vp_all_potential_prefixes_asns',
                     peer_asns=sorted({self.gaps[g]['peer_asn'] for g in refs if self.gaps[g]['peer_asn'] is not None}),
                     unknown_peer=bool(quality_refs) or any(self.gaps[g]['peer_asn'] is None for g in refs),all_origin_asns=True,
                     gap_refs=list(refs),quality_refs=list(quality_refs),
                     reason_codes=(['unresolved_dependency'] if bad else [])+(['declared_coverage_unknown'] if base=='unknown' else []))
            row['qualification_id']=identity(row)
            rows.append(row);self.store.append('qualifications',row)
        receipt=dict(mode=self.mode,source_id=self.binding.source_id,source_rank=self.rank,
                     binding_ref=end.binding_ref,upstream_source_rank=end.source_rank,
                     **asdict(end.parse_counts),qualities=len(self.current_qualities),qualifications=len(rows),
                     qualification_digest=identity(rows))
        self.store.append('qualification_receipts',receipt)
        self.previous_end=window.end
        return receipt
