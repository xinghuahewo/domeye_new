"""默认人工 Reader 流；ET 集成仅显式自有 PG 配置下使用自造 MRT。"""
from copy import deepcopy
from dataclasses import asdict, replace
import json
from types import SimpleNamespace

import pytest

from data_pipeline.bgp.archive import message_reader as c
from data_pipeline.bgp.input.mrt_types import HeaderEvidence, FieldFailure, Interpretation, ParseStatus, ReadPolicy
from data_pipeline.bgp.ordered_reader import adapt, binding_from_reader, decode_interpretation, ordered
from data_pipeline.bgp.record_types import Direction, Element, ElementPosition, Extent, InputBinding, MessageBoundary, MessagePosition, ScopeKind, SourceBinding, SourceEnd, SourceQuality, SourceStart, metadata_json
from data_pipeline.bgp.archive.store import TABLES

SHA = 'a' * 64


def message(source, record, *, status='decoded', direction='received', endpoint=True,
            kind='update', microsecond=None, elements=0, path_id=None, path_present=False):
    """手工构造 M2 列形状，保留 nullable、bytes 和原始属性。"""
    header = HeaderEvidence(microsecond=microsecond, direction=direction)
    if endpoint:
        header = replace(header, endpoint_trust='complete_header', peer_ip='192.0.2.1',
                         peer_asn=64497, local_ip='192.0.2.2', local_asn=64496,
                         interface=0, endpoint_afi=1, bgp_type=2 if kind=='update' else None)
    failure = FieldFailure('field_truncated', 'attributes', record*100+20, 10, 2)
    interp = Interpretation(ParseStatus(status), ReadPolicy.ISOLATE_PAYLOAD,
                            None if status == 'decoded' else 'field_truncated', header,
                            None if status == 'decoded' else failure,
                            interpretation_level=({'update':'route_elements','rib':'route_elements','state_change':'state',
                                                   'peer_index_table':'peer_table'}.get(kind,
                                                   'bgp_header_only' if header.bgp_type else
                                                   'endpoint_header' if endpoint else 'frame')) if status == 'decoded' else
                            ('bgp_header_only' if header.bgp_type is not None else
                             'endpoint_header' if endpoint else 'frame'),
                            next_record_offset=(record+1)*100)
    row = dict.fromkeys(n for n,_ in TABLES['messages'])
    subtype = 6 if direction == 'local' else 1
    if kind == 'state_change':subtype=0
    row.update(message_id=f'{source}:{record}', source_id=source, content_sha256=SHA,
               record=record, offset=record*100, length=100, epoch=1000,
               microsecond=microsecond, mrt_type=17 if microsecond is not None else 16,
               mrt_subtype=subtype, raw_digest=SHA, kind=kind if status=='decoded' else 'unsupported',
               bgp_id_present=False, local_message=direction=='local' if status=='decoded' else None,
               old_state=6 if kind=='state_change' else None, new_state=1 if kind=='state_change' else None,
               reason=None if status=='decoded' else '仅诊断，不猜端点',
               interpretation=json.dumps(asdict(interp)), eor=[], quality=[], peers=[])
    if status == 'decoded' and endpoint:
        for key in ('peer_ip', 'peer_asn', 'local_ip', 'local_asn', 'interface', 'endpoint_afi'):
            row[key]=getattr(header,key)
    if status != 'decoded':row['quality']=[dict(code=interp.reason_code, detail=row['reason'])]
    rows=[]
    for i in range(elements):
        e=dict.fromkeys(n for n,_ in TABLES['elements']+TABLES['paths'])
        e.update({k:row[k] for k in ('message_id','source_id','content_sha256','record','epoch','microsecond',
                                    'mrt_type','mrt_subtype','local_message','local_ip','local_asn','interface')})
        e.update(event_id=f'{source}:{record}:{i}',ordinal=i,action='announce',afi=1,safi=1,
                 prefix=f'198.51.{i}.0/24',raw_prefix=b'\x18\xc6\x33\x00',
                 path_key=SHA,path_id=path_id,path_id_present=path_present,asn_width=4,
                 attributes_raw=b'\x00\xff',attributes_digest=SHA,as_path_raw=b'\x02\x00',
                 as4_path_raw=None,as_path_text='{64497} 64496',as4_path_text=None,
                 peer_ip='192.0.2.1',peer_asn=64497,bgp_id_present=False,
                 raw_origin_asn=64496,attributed_origin_asn=64496)
        rows.append(e)
    return row,rows


def binding(groups):
    sources=[]
    for i,(sid,rows) in enumerate(groups):
        statuses=[json.loads(m['interpretation'])['status'] for m,_ in rows]
        sources.append(SourceBinding(sid,SHA,'baseline' if i==0 else 'update',str(i+1)*64,
                       len(rows),sum(len(e) for _,e in rows),
                       *(statuses.count(v) for v in ('decoded','rejected','unsupported'))))
    return InputBinding('rrc25','run1',7,'b'*64,'seal1','c'*64,tuple(sources))


def stream_source(b, sid, rows, cut=999, quality=False):
    s=next(s for s in b.sources if s.source_id==sid)
    yield c.SourceStart(b.observation_run,b.seal_snapshot,sid,SHA,s.role,s.messages,s.elements)
    if quality:
        yield c.MessageBatch(b.observation_run,b.seal_snapshot,sid,(),(),1,
                             (dict(source_id=sid,message_id=None,code='cross_file_time_overlap',detail='原文'),))
    atoms=[]
    for m,elems in rows:
        atoms.append(('m',m))
        atoms.extend(('e',e) for e in elems)
    for pos in range(0,len(atoms),cut):
        chunk=atoms[pos:pos+cut]
        yield c.MessageBatch(b.observation_run,b.seal_snapshot,sid,
                             tuple(r for k,r in chunk if k=='m'),tuple(r for k,r in chunk if k=='e'),1)
    yield c.SourceEnd(b.observation_run,b.seal_snapshot,sid,len(rows),sum(len(e) for _,e in rows),
                      sum(m['kind']=='state_change' for m,_ in rows),sum(len(m['eor']) for m,_ in rows),
                      sum(bool(m['local_message']) for m,_ in rows),
                      sum(len(m['quality']) for m,_ in rows)+int(quality))


def signature(items):
    result=[]
    for item in items:
        if isinstance(item,MessageBoundary):
            result.append(('m',item.position,item.raw,item.interpretation,item.gap))
        elif isinstance(item,Element):result.append(('e',item.position,item.raw))
        else:result.append(item)
    return result


@pytest.mark.parametrize('cut', [1,2,3,4,5,7,13,999])
def test_full_fidelity_and_batch_invariance(cut):
    rows=[message('z',0,elements=3,path_id=0,path_present=True,microsecond=999999),
          message('z',1,status='rejected'),message('z',2,elements=2,microsecond=0),
          message('z',3,status='rejected',direction='local'),message('z',4)]
    other=[message('a',0,elements=1,microsecond=1)]
    b=binding([('z',rows),('a',other)])
    def events(size):
        yield from stream_source(b,'z',rows,size,True)
        yield from stream_source(b,'a',other,size)
    actual=list(adapt(events(cut),b))
    assert signature(actual)==signature(list(adapt(events(999),b)))
    expected=[]
    for rank,rs in enumerate((rows,other)):
        for m,es in rs:
            expected.append(('m',MessagePosition(rank,m['record']),m))
            expected.extend(('e',ElementPosition(MessagePosition(rank,m['record']),e['ordinal']),e) for e in es)
    assert [(('m' if isinstance(x,MessageBoundary) else 'e'),x.position,x.raw)
            for x in actual if isinstance(x,(MessageBoundary,Element))]==expected
    assert [x.position.sort_key for x in actual if isinstance(x,(MessageBoundary,Element))]==sorted(
        x.position.sort_key for x in actual if isinstance(x,(MessageBoundary,Element)))
    assert all(x.raw is next(m for m,_ in rows+other if m['message_id']==x.raw['message_id'])
               for x in actual if isinstance(x,MessageBoundary))
    assert [x.parse_counts.gaps for x in actual if isinstance(x,SourceEnd)]==[2,0]
    assert next(x for x in actual if isinstance(x,Element)).raw['path_id'] == 0


@pytest.mark.parametrize('direction,endpoint,kind', [
    ('received',True,ScopeKind.RECEIVED_ENDPOINT),('received',False,ScopeKind.COLLECTOR_CHAIN),
    ('local',True,ScopeKind.LOCAL_OBSERVATION),('local',False,ScopeKind.LOCAL_OBSERVATION),
    ('unknown',False,ScopeKind.COLLECTOR_CHAIN)])
def test_gap_scope_retains_future_and_folded_dependencies(direction,endpoint,kind):
    rows=[message('s',0,status='rejected',direction=direction,endpoint=endpoint)]
    # 顶层残留字段不能影响 Gap；原观察仍原样保留。
    rows[0][0]['peer_asn']=999
    rows[0][0]['peer_ip']='203.0.113.9'
    b=binding([('s',rows)])
    gap=next(x.gap for x in adapt(stream_source(b,'s',rows),b) if isinstance(x,MessageBoundary))
    assert gap.scope.kind==kind and gap.direction==Direction(direction)
    assert gap.scope.peer_asn_evidence==(64497 if endpoint else None)
    assert gap.scope.prefixes==gap.scope.path_slots==gap.scope.route_families==Extent.ALL_INCLUDING_UNSEEN
    assert gap.scope.unknown_peer_covers_unseen_asns is (not endpoint)
    assert gap.scope.session is None
    assert gap.scope.unmapped_rib_overlap=='requires_dependency_check'
    assert json.loads(metadata_json(gap))['scope']['kind']==kind.value


def test_state_eor_empty_and_peer_table_remain_distinct_without_gap():
    rows=[message('s',0,kind='state_change'),message('s',1),message('s',2,kind='keepalive')]
    rows[1][0]['eor']=[dict(afi=1,safi=1)]
    b=binding([('s',rows)])
    boundaries=[x for x in adapt(stream_source(b,'s',rows,1),b) if isinstance(x,MessageBoundary)]
    assert [x.raw for x in boundaries]==[m for m,_ in rows]
    assert all(x.gap is None for x in boundaries)
    assert boundaries[0].raw['new_state']==1 and boundaries[1].raw['eor']==[dict(afi=1,safi=1)]


def test_subset_uses_full_chain_rank_and_binding_identity():
    groups=[('s0',[message('s0',0)]),('s1',[message('s1',0)]),('s2',[message('s2',0,status='rejected')])]
    b=binding(groups)
    result=list(adapt(stream_source(b,'s2',groups[2][1]),b,('s2',)))
    assert result[0].source_rank==2 and result[1].position.source_rank==2
    with pytest.raises(ValueError):list(adapt([],b,('s2','s0')))
    changed=replace(b,seal_digest='f'*64)
    other=list(adapt(stream_source(changed,'s2',groups[2][1]),changed,('s2',)))
    assert result[1].gap.gap_id==other[1].gap.gap_id
    assert result[1].binding_ref!=other[1].binding_ref


@pytest.mark.parametrize('mutate', [
    lambda m:m.update(contract_version='future/v2'),lambda m:m.update(status='success'),
    lambda m:m.update(policy='unsafe'),lambda m:m.update(frame_complete=False),
    lambda m:m.update(continuation_allowed=False),lambda m:m.update(next_record_offset=True),
    lambda m:m.update(unexpected=1),lambda m:m.pop('failure'),
    lambda m:m['header'].update(peer_asn=True),lambda m:m['header'].update(endpoint_afi=2),
    lambda m:m['header'].update(direction='sent'),lambda m:m['header'].update(microsecond=1000000),
    lambda m:m['failure'].update(code='crc_failure'),lambda m:m['failure'].update(section='mrt_timestamp'),
    lambda m:m.update(reason_code='program_error'),
])
def test_interpretation_schema_fail_closed(mutate):
    raw=message('s',0,status='rejected')[0]['interpretation']
    meta=json.loads(raw);mutate(meta)
    with pytest.raises(ValueError):decode_interpretation(json.dumps(meta))


def test_duplicate_json_key_rejected():
    with pytest.raises(ValueError):decode_interpretation('{"status":"decoded","status":"rejected"}')


@pytest.mark.parametrize('change', ['duplicate_message','reverse_messages','missing_message','reverse_elements',
    'wrong_event','wrong_source','wrong_time','bad_bytes','partial_element','partial_eor','wrong_end',
    'wrong_snapshot','missing_end','negative_ordinal','path_flag','offset','missing_column'])
def test_corrupt_stream_has_no_completion(change):
    rows=[message('s',0,elements=2),message('s',1,elements=1)]
    b=binding([('s',rows)])
    stream=list(stream_source(b,'s',rows))
    batch=stream[1]
    ms,es=list(batch.messages),list(batch.elements)
    if change=='duplicate_message':ms.insert(1,deepcopy(ms[0]))
    elif change=='reverse_messages':ms.reverse()
    elif change=='missing_message':ms.pop(0)
    elif change=='reverse_elements':es.reverse()
    elif change=='wrong_event':es[0]['event_id']='wrong'
    elif change=='wrong_source':es[0]['source_id']='other'
    elif change=='wrong_time':es[0]['epoch']=2000
    elif change=='bad_bytes':es[0]['raw_prefix']='not bytes'
    elif change=='partial_element':ms[0]=message('s',0,status='rejected')[0]
    elif change=='partial_eor':
        ms[0]=message('s',0,status='rejected')[0];ms[0]['eor']=[dict(afi=1,safi=1)]
    elif change=='wrong_end':stream[-1]=replace(stream[-1],elements=99)
    elif change=='wrong_snapshot':stream[0]=replace(stream[0],snapshot=8)
    elif change=='missing_end':stream.pop()
    elif change=='negative_ordinal':es[0]['ordinal']=-1
    elif change=='path_flag':es[0]['path_id']=0
    elif change=='offset':ms[1]['offset']=101
    elif change=='missing_column':ms[0].pop('raw_digest')
    stream[1]=replace(batch,messages=tuple(ms),elements=tuple(es))
    emitted=[]
    with pytest.raises(ValueError):
        for x in adapt(iter(stream),b):emitted.append(x)
    assert not any(isinstance(x,SourceEnd) for x in emitted)


def test_early_close_closes_upstream_and_never_proves_completion():
    rows=[message('s',0,elements=4)]
    b=binding([('s',rows)]);closed=[]
    def upstream():
        try:yield from stream_source(b,'s',rows,1)
        finally:closed.append(True)
    flow=adapt(upstream(),b)
    assert isinstance(next(flow),SourceStart)
    assert isinstance(next(flow),MessageBoundary)
    flow.close()
    assert closed==[True]


def test_reader_binding_ignores_reference_checkpoint_ordinal_and_uses_stream():
    rows=[message('s',0)]
    b=binding([('s',rows)])
    cp=dict(source_id='s',source_sha=SHA,raw='verified_source_eof',ingest='complete',digest='1'*64,
            ordinal=5,counts=dict(messages=1,elements=0,decoded=1,rejected=0,unsupported=0))
    manifest=dict(collector='rrc25',baseline_source='s',update_sources=[],
                  inputs=[dict(source_id='s',sha256=SHA,role='baseline')])
    seal=dict(qualification='observation_sealed',business='not_run',version='observation-checkpoint/v1',
              run_id='run1',snapshot=7,plan_id='b'*64,seal_id='seal1',digest='c'*64)
    calls=[]
    def reader_stream():
        calls.append('start gate')
        yield from stream_source(b,'s',rows)
        calls.append('exhausted')
    reader=SimpleNamespace(selection=SimpleNamespace(seal=seal,manifest=manifest,
                           checkpoints=[dict(source_id='ref',ordinal=0),cp]),
                           run_id='run1',snapshot=7,sources=('s',),stream=reader_stream)
    assert binding_from_reader(reader)==b
    result=list(ordered(reader))
    assert result[1].position.source_rank==0 and calls==['start gate','exhausted']
    reader.selection=None
    with pytest.raises(ValueError):list(ordered(reader))


def test_unsupported_nlri_is_one_gap_and_never_decoded():
    rows=[message('s',0,status='rejected')]
    m=rows[0][0];meta=json.loads(m['interpretation'])
    meta.update(status='unsupported',policy='strict/v1',reason_code='unsupported_nlri')
    meta['failure'].update(code='unsupported_nlri',section='mp_nlri')
    m['interpretation']=json.dumps(meta)
    m['quality']=[dict(code='unsupported_nlri',detail='原诊断')]
    b=binding([('s',rows)])
    result=list(adapt(stream_source(b,'s',rows,1),b))
    assert result[1].gap.parse_status==ParseStatus.UNSUPPORTED
    assert result[-1].parse_counts==replace(result[-1].parse_counts,decoded=0,rejected=0,unsupported=1,gaps=1)


def test_unknown_outer_type_preserves_unknown_direction_and_has_no_guessed_peer():
    m,es=message('s',0,status='rejected',direction='unknown',endpoint=False)
    meta=json.loads(m['interpretation'])
    meta.update(status='unsupported',policy='strict/v1',reason_code='unsupported_mrt_type',failure=None)
    m.update(mrt_type=99,mrt_subtype=1,interpretation=json.dumps(meta))
    b=binding([('s',[(m,es)])])
    gap=list(adapt(stream_source(b,'s',[(m,es)]),b))[1].gap
    assert gap.scope.kind==ScopeKind.COLLECTOR_CHAIN and gap.scope.endpoint is None


def test_rib_peer_table_and_bytes_are_preserved():
    peer,_=message('s',0,kind='peer_index_table',direction='unknown',endpoint=False)
    rib,es=message('s',1,kind='rib',direction='unknown',endpoint=False,elements=1)
    peer.update(mrt_type=13,mrt_subtype=1,local_message=None,
                peers=[dict(ip='192.0.2.1',asn=64497,bgp_id='192.0.2.1',bgp_id_present=True,
                            peer_index=0,table_record=0)])
    rib.update(mrt_type=13,mrt_subtype=2,local_message=None)
    es[0].update(mrt_type=13,mrt_subtype=2,local_message=None,action='rib_snapshot',
                 peer_table_record=0,peer_index=0,originated_epoch=900,
                 bgp_id='192.0.2.1',bgp_id_present=True)
    rows=[(peer,[]),(rib,es)];b=binding([('s',rows)])
    result=list(adapt(stream_source(b,'s',rows,1),b))
    assert [x.raw for x in result if isinstance(x,MessageBoundary)]==[peer,rib]
    assert [x.raw for x in result if isinstance(x,Element)]==es
    assert all(x.gap is None for x in result if isinstance(x,MessageBoundary))


def test_numeric_record_order_and_timestamp_regression_are_not_resorted():
    rows=[message('s',r,microsecond=999-r,elements=1) for r in range(13)]
    rows[10][0]['epoch']=999
    rows[10][1][0]['epoch']=999
    b=binding([('s',rows)])
    result=list(adapt(stream_source(b,'s',rows,4,True),b))
    assert [x.position.record for x in result if isinstance(x,MessageBoundary)]==list(range(13))
    assert [x.raw_time for x in result if isinstance(x,MessageBoundary)][10].epoch==999
    q=next(x for x in result if isinstance(x,SourceQuality))
    assert q.raw['message_id'] is None and not hasattr(q,'position')


def test_zero_message_source_and_tail_gate_failure():
    b=binding([('s',[])])
    result=list(adapt(stream_source(b,'s',[],quality=True),b))
    assert [type(x) for x in result]==[SourceStart,SourceQuality,SourceEnd]
    rows=[message('s',0)];b=binding([('s',rows)])
    def failed_tail():
        yield from list(stream_source(b,'s',rows))[:-1]
        raise ValueError('人工 M2 来源尾资格复核失败')
    output=[]
    with pytest.raises(ValueError,match='尾资格'):
        for x in adapt(failed_tail(),b):output.append(x)
    assert not any(isinstance(x,SourceEnd) for x in output)


def test_bad_source_quality_reference_and_et_disagreement():
    rows=[message('s',0,microsecond=123,status='rejected')];b=binding([('s',rows)])
    events=list(stream_source(b,'s',rows,quality=True))
    events[1].source_quality[0]['message_id']='s:0'
    with pytest.raises(ValueError):list(adapt(events,b))
    rows[0][0]['microsecond']=124
    with pytest.raises(ValueError,match='ET'):list(adapt(stream_source(b,'s',rows),b))


def test_position_rejects_negative_bool_and_preserves_zero():
    for v in (-1,True,1.5):
        with pytest.raises(ValueError):MessagePosition(v,0)
        with pytest.raises(ValueError):ElementPosition(MessagePosition(0,0),v)
    assert MessagePosition(0,0).sort_key < ElementPosition(MessagePosition(0,0),0).sort_key


def test_ipv6_endpoint_does_not_narrow_route_families_and_multiple_diagnostics_do_not_duplicate_gap():
    rows=[message('s',0,status='rejected')]
    m=rows[0][0];meta=json.loads(m['interpretation'])
    meta['header'].update(endpoint_afi=2,peer_ip='2001:db8::1',local_ip='2001:db8::2')
    m['interpretation']=json.dumps(meta)
    m['quality'].append(dict(code='extra_diagnostic',detail='同消息附加证据'))
    b=binding([('s',rows)])
    result=list(adapt(stream_source(b,'s',rows),b))
    gap=result[1].gap
    assert gap.header.endpoint_afi==2 and gap.scope.route_families==Extent.ALL_INCLUDING_UNSEEN
    assert gap.scope.endpoint.peer_ip=='2001:db8::1'
    assert result[-1].parse_counts.gaps==1 and result[-1].raw.quality_records==2


def test_no_gap_preserves_local_withdraw_and_distinct_path_slots():
    rows=[message('s',0,direction='local',elements=1,path_present=True,path_id=0),
          message('s',1,direction='local',elements=1,path_present=True,path_id=1)]
    rows[1][1][0]['action']='withdraw'
    b=binding([('s',rows)])
    result=list(adapt(stream_source(b,'s',rows,1),b))
    assert [x.raw for x in result if isinstance(x,Element)]==[rows[0][1][0],rows[1][1][0]]
    assert all(x.raw['local_message'] is True for x in result if isinstance(x,Element))
    assert all(x.gap is None for x in result if isinstance(x,MessageBoundary))


@pytest.mark.parametrize('change', ['duplicate_start','duplicate_end','missing_source','wrong_parse'])
def test_source_protocol_and_checkpoint_parse_counts_fail(change):
    rows=[message('s',0)];b=binding([('s',rows)]);events=list(stream_source(b,'s',rows))
    if change=='duplicate_start':events.insert(1,events[0])
    elif change=='duplicate_end':events.append(events[-1])
    elif change=='missing_source':b=replace(b,sources=b.sources+(replace(b.sources[0],source_id='t',role='update'),))
    else:b=replace(b,sources=(replace(b.sources[0],decoded=0,rejected=1),))
    with pytest.raises(ValueError):list(adapt(events,b))


def snapshot_reader(roles=('baseline','snapshot','update'), selected=None, empty_snapshot=False):
    """人工已封存元数据；reference 不进入 MRT rank。"""
    groups=[]
    for i,role in enumerate(roles):
        sid=f's{i}'
        if role in ('baseline','snapshot'):
            m,es=message(sid,0,kind='rib',direction='unknown',endpoint=False,elements=1)
            m.update(mrt_type=13,mrt_subtype=2,local_message=None)
            es[0].update(mrt_type=13,mrt_subtype=2,local_message=None,action='rib_snapshot')
            rows=[(m,es)]
        else:
            rows=[message(sid,0,elements=2),message(sid,1)]
        if empty_snapshot and role == 'snapshot':rows=[]
        groups.append((sid,rows))
    b=binding(groups)
    b=replace(b,sources=tuple(replace(s,role=role) for s,role in zip(b.sources,roles)))
    entries=[dict(source_id=s.source_id,sha256=s.content_sha256,role=s.role,
                  origin_uri=f'https://fixture.invalid/{s.source_id}',path=f'/fixture/{s.source_id}')
             for s in b.sources]
    cps=[dict(source_id='reference',ordinal=0)]
    cps.extend(dict(source_id=s.source_id,source_sha=s.content_sha256,digest=s.checkpoint_digest,
                    raw='verified_source_eof',ingest='complete',ordinal=i+1,
                    counts={k:getattr(s,k) for k in ('messages','elements','decoded','rejected','unsupported')})
               for i,s in enumerate(b.sources))
    manifest=dict(collector=b.collector,inputs=entries,baseline_source=entries[0]['source_id'],
                  update_sources=[e['source_id'] for e in entries if e['role']=='update'])
    seal=dict(qualification='observation_sealed',business='not_run',version='observation-checkpoint/v1',
              run_id=b.observation_run,snapshot=b.seal_snapshot,plan_id=b.plan_id,
              seal_id=b.seal_id,digest=b.seal_digest)
    selected=b.ordered_source_ids if selected is None else selected
    reader=SimpleNamespace(selection=SimpleNamespace(seal=seal,manifest=manifest,checkpoints=cps),
                           run_id=b.observation_run,snapshot=b.seal_snapshot,sources=selected)
    def flow():
        for sid,rows in groups:
            if sid in reader.sources:yield from stream_source(b,sid,rows,1)
    reader.stream=flow
    return reader,b,groups


@pytest.mark.parametrize('roles,selected', [
    (('baseline','snapshot','update'),None),
    (('baseline','snapshot','snapshot','update'),None),
    (('baseline','snapshot','update'),('s1',)),
    (('baseline','snapshot','update'),('s1','s2')),
    (('baseline','snapshot','update'),('s0','s2')),
    (('baseline','update','snapshot','update'),('s2','s3')),
    (('baseline','snapshot','snapshot'),('s1','s2')),
    (('baseline','update','update'),None),
])
def test_snapshot_metadata_and_complete_typed_stream(roles,selected):
    reader,b,groups=snapshot_reader(roles,selected)
    assert binding_from_reader(reader)==b
    actual=list(ordered(reader))
    expected=[]
    for rank,(sid,rows) in enumerate(groups):
        if sid not in reader.sources:continue
        for m,es in rows:
            expected.append(('m',MessagePosition(rank,m['record']),m))
            expected.extend(('e',ElementPosition(MessagePosition(rank,m['record']),e['ordinal']),e) for e in es)
    assert [(('m' if isinstance(x,MessageBoundary) else 'e'),x.position,x.raw)
            for x in actual if isinstance(x,(MessageBoundary,Element))]==expected
    starts=[x for x in actual if isinstance(x,SourceStart)]
    assert [(x.source_rank,x.raw.role) for x in starts]==[
        (i,role) for i,role in enumerate(roles) if f's{i}' in reader.sources]
    assert [x.raw.source_id for x in actual if isinstance(x,SourceEnd)]==list(reader.sources)
    # 原无 snapshot 及新 snapshot 都复用完全相同的低层流，无字段增删或重标。
    assert signature(actual)==signature(list(adapt(reader.stream(),b,reader.sources)))


@pytest.mark.parametrize('change', ['wrong_role','second_baseline','wrong_baseline','update_order',
    'source_order','missing_cp','duplicate_cp','cp_order','same_role_alias','conflicting_alias',
    'conflicting_sha_alias','subset_reorder'])
def test_snapshot_sealed_metadata_rejects_ambiguity(change):
    reader,_,_=snapshot_reader(('baseline','snapshot','update','update'))
    manifest=reader.selection.manifest;entries=manifest['inputs'];cps=reader.selection.checkpoints
    if change=='wrong_role':entries[1]['role']='reference'
    elif change=='second_baseline':entries[1]['role']='baseline'
    elif change=='wrong_baseline':manifest['baseline_source']='s1'
    elif change=='update_order':manifest['update_sources'].reverse()
    elif change=='source_order':entries[1],entries[2]=entries[2],entries[1]
    elif change=='missing_cp':cps.pop(2)
    elif change=='duplicate_cp':cps.append(deepcopy(cps[2]))
    elif change=='cp_order':cps[1],cps[2]=cps[2],cps[1]
    elif change in ('same_role_alias','conflicting_alias','conflicting_sha_alias'):
        alias={**entries[1],'path':'/another/local/cache'}
        if change=='conflicting_alias':alias['role']='update'
        if change=='conflicting_sha_alias':alias['sha256']='f'*64
        entries.append(alias)
    elif change=='subset_reorder':reader.sources=('s2','s1')
    with pytest.raises(ValueError):list(ordered(reader))


def test_normalized_cache_path_alias_does_not_change_binding_or_output():
    reader,b,_=snapshot_reader()
    before=signature(list(ordered(reader)))
    # 上游同角色缓存别名已被 M2 规范为一个来源；代表路径不参与本层身份。
    reader.selection.manifest['inputs'][1]['path']='/another/local/cache'
    assert binding_from_reader(reader)==b
    assert signature(list(ordered(reader)))==before


def test_empty_snapshot_source_retains_rank_role_and_end():
    reader,_,_=snapshot_reader(selected=('s1',),empty_snapshot=True)
    result=list(ordered(reader))
    assert [type(x) for x in result]==[SourceStart,SourceEnd]
    assert result[0].source_rank==result[1].source_rank==1
    assert result[0].raw.role=='snapshot'
    assert result[1].raw.messages==result[1].raw.elements==0


def test_prevalidation_closes_started_upstream_temp_resource(tmp_path):
    from tempfile import TemporaryDirectory
    from pathlib import Path
    rows=[message('s',0)];b=binding([('s',rows)]);state={}
    def upstream():
        with TemporaryDirectory(dir=tmp_path) as scratch:
            state['path']=Path(scratch)
            try:
                yield 'already started'
                yield from stream_source(b,'s',rows)
            finally:state['closed']=True
    source=upstream();next(source)
    with pytest.raises(ValueError):list(adapt(source,b,('missing',)))
    assert state['closed'] and not state['path'].exists()


class ClosingIterator:
    def __init__(self, events=(), primary=None, cleanup=None):
        self.events=iter(events);self.primary=primary;self.cleanup=cleanup;self.closes=0
    def __iter__(self):return self
    def __next__(self):
        if self.primary is not None:raise self.primary
        return next(self.events)
    def close(self):
        self.closes+=1
        if self.cleanup is not None:raise self.cleanup


def test_primary_exception_retained_with_checkable_cleanup_errors():
    b=binding([('s',[])]);primary=ValueError('PRIMARY');cleanup=OSError('CLEANUP')
    source=ClosingIterator(primary=primary,cleanup=cleanup)
    with pytest.raises(ValueError,match='PRIMARY') as caught:list(adapt(source,b))
    assert caught.value is primary and caught.value.cleanup_errors==(cleanup,)
    assert source.closes==1


@pytest.mark.parametrize('mode',['validation','selected_conversion','iterator_acquisition'])
def test_failure_before_consumption_closes_stream_and_preserves_primary(mode):
    b=binding([('s',[])]);cleanup=OSError('CLEANUP');primary=ValueError('ITER_PRIMARY')
    class Stream(ClosingIterator):
        def __iter__(self):
            if mode=='iterator_acquisition':raise primary
            return self
    source=Stream(cleanup=cleanup)
    def bad_selection():
        raise ValueError('SELECT_PRIMARY')
        yield 's'
    selected=bad_selection() if mode=='selected_conversion' else ('missing',) if mode=='validation' else ('s',)
    with pytest.raises(ValueError) as caught:list(adapt(source,b,selected))
    assert caught.value.cleanup_errors==(cleanup,) and source.closes==1
    if mode=='iterator_acquisition':assert caught.value is primary


def test_distinct_iterable_and_iterator_both_closed_even_when_first_close_fails():
    b=binding([('s',[])]);primary=ValueError('PRIMARY')
    inner_error=OSError('INNER');outer_error=RuntimeError('OUTER')
    inner=ClosingIterator(primary=primary,cleanup=inner_error)
    class Outer:
        closes=0
        def __iter__(self):return inner
        def close(self):
            self.closes+=1
            raise outer_error
    outer=Outer()
    with pytest.raises(ValueError) as caught:list(adapt(outer,b))
    assert caught.value is primary and caught.value.cleanup_errors==(inner_error,outer_error)
    assert inner.closes==outer.closes==1


def test_normal_consumption_close_failure_withholds_final_end():
    rows=[message('s',0,elements=2)];b=binding([('s',rows)])
    cleanup=OSError('CLEANUP');source=ClosingIterator(stream_source(b,'s',rows),cleanup=cleanup)
    emitted=[]
    with pytest.raises(OSError,match='CLEANUP') as caught:
        for item in adapt(source,b):emitted.append(item)
    assert caught.value is cleanup and cleanup.cleanup_errors==(cleanup,)
    assert source.closes==1 and not any(isinstance(x,SourceEnd) for x in emitted)
    assert [x.raw for x in emitted if isinstance(x,Element)]==rows[0][1]


def test_success_closes_once_before_final_end_and_repeated_close_is_noop():
    rows=[message('s',0,elements=1)];b=binding([('s',rows)])
    source=ClosingIterator(stream_source(b,'s',rows));flow=adapt(source,b);emitted=[]
    for item in flow:
        emitted.append(item)
        if isinstance(item,SourceEnd):assert source.closes==1
    assert [x.raw for x in emitted if isinstance(x,MessageBoundary)]==[rows[0][0]]
    assert [x.raw for x in emitted if isinstance(x,Element)]==rows[0][1]
    flow.close();flow.close()
    assert source.closes==1 and isinstance(emitted[-1],SourceEnd)


def test_early_close_failure_is_visible_and_not_retried():
    rows=[message('s',0)];b=binding([('s',rows)])
    cleanup=OSError('EARLY_CLEANUP');source=ClosingIterator(stream_source(b,'s',rows),cleanup=cleanup)
    flow=adapt(source,b);assert isinstance(next(flow),SourceStart)
    with pytest.raises(OSError,match='EARLY_CLEANUP') as caught:flow.close()
    assert caught.value.cleanup_errors==(cleanup,)
    assert isinstance(caught.value.__cause__,GeneratorExit)
    flow.close();assert source.closes==1


def test_prior_source_end_is_not_whole_run_completion_on_later_cleanup_failure():
    groups=[('s',[message('s',0)]),('t',[message('t',0)])];b=binding(groups)
    events=[e for sid,rows in groups for e in stream_source(b,sid,rows)]
    source=ClosingIterator(events,cleanup=OSError('CLEANUP'));emitted=[]
    with pytest.raises(OSError):
        for item in adapt(source,b):emitted.append(item)
    assert [x.raw.source_id for x in emitted if isinstance(x,SourceEnd)]==['s']


@pytest.mark.parametrize('status',['rejected','unsupported'])
def test_atomic_et_header_time_keeps_raw_null(status):
    m,es=message('s',0,status='rejected',microsecond=900000)
    if status=='unsupported':
        meta=json.loads(m['interpretation'])
        meta.update(status='unsupported',reason_code='unsupported_nlri',policy='strict/v1')
        meta['failure'].update(code='unsupported_nlri')
        m['interpretation']=json.dumps(meta)
    m['microsecond']=None
    original=deepcopy(m);b=binding([('s',[(m,es)])])
    result=list(adapt(stream_source(b,'s',[(m,es)]),b))
    boundary=result[1]
    assert boundary.raw is m and boundary.raw==original and boundary.raw['microsecond'] is None
    assert boundary.raw_time.microsecond==boundary.gap.raw_time.microsecond==900000
    assert boundary.gap.header.microsecond==900000 and isinstance(result[-1],SourceEnd)


@pytest.mark.parametrize('change', ['decoded_null','conflict','header_null','out_of_range',
                                    'non_et','unsupported_without_failure','unsupported_subtype'])
def test_et_time_fallback_rejects_unproven_or_conflicting_time(change):
    m,es=message('s',0,status='rejected',microsecond=900000)
    m['microsecond']=None;meta=json.loads(m['interpretation'])
    if change=='decoded_null':
        meta.update(status='decoded',failure=None,reason_code=None,interpretation_level='route_elements')
        m['kind']='update'
    elif change=='conflict':m['microsecond']=500000
    elif change=='header_null':meta['header']['microsecond']=None
    elif change=='out_of_range':meta['header']['microsecond']=1000000
    elif change=='non_et':m['mrt_type']=16
    elif change=='unsupported_without_failure':
        meta.update(status='unsupported',failure=None,reason_code='unsupported_bgp_message_type')
    else:m['mrt_subtype']=99
    m['interpretation']=json.dumps(meta);b=binding([('s',[(m,es)])]);emitted=[]
    with pytest.raises(ValueError):
        for x in adapt(stream_source(b,'s',[(m,es)]),b):emitted.append(x)
    assert not any(isinstance(x,SourceEnd) for x in emitted)


def test_public_m2_reader_atomic_et_integration(tmp_path):
    """显式自有无 TCP PG + 自造 MRT；不读取真实输入，不复用其他任务数据库。"""
    import gzip
    import hashlib
    import os
    from pathlib import Path
    import struct
    import psycopg2
    from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
    from data_pipeline.bgp.input.mrt_reader import source_identity
    from tests.observations.test_observation_two_phase import fixture_manifest
    from tests.observations.test_observation_mrt import mrt, update
    dsn=os.environ.get('DOMEYE_M3_ET_TEST_DSN')
    if not dsn:pytest.skip('需显式绑定本任务自有无 TCP 人工 PG')
    params=psycopg2.extensions.parse_dsn(dsn)
    assert params.get('host','').startswith('/tmp/domeye-m3-et-efb1-')
    root=tmp_path/'input';root.mkdir();manifest=fixture_manifest(root)
    bad=mrt(struct.pack('!I',900000)+update(attrs=b'\xf0\x23\x04\0\x04\x2f\x66')[12:],4,17)
    good=mrt(struct.pack('!I',500000)+update()[12:],4,17)
    # MP_REACH 使用不支持的 NLRI AFI 3，触发 M1 _UnsupportedPayload 原子形态。
    unsupported=mrt(struct.pack('!I',900000)+update(attrs=b'\x80\x0e\x06\x00\x03\x01\x00\x00\x00')[12:],4,17)
    for entry,raw in zip(manifest['inputs'][1:],(bad+good+unsupported+good,good)):
        path=Path(entry['path']);path.write_bytes(gzip.compress(raw,mtime=0))
        entry.update(sha256=hashlib.sha256(path.read_bytes()).hexdigest(),size=path.stat().st_size)
        entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
    manifest['update_sources']=[e['source_id'] for e in manifest['inputs'][1:]]
    seal=produce_checkpointed(manifest,dsn,tmp_path/'run',policy='isolate-payload/v1',
                              min_free_bytes=0,batch_rows=2)
    ids=[e['source_id'] for e in manifest['inputs']]
    def reader():return c.ObservationReader(dsn,seal['run_id'],seal['snapshot'],ids,profile='observation',batch_rows=1)
    raw_flow=list(reader().stream())
    raw_messages=[m for batch in raw_flow if isinstance(batch,c.MessageBatch) for m in batch.messages]
    raw_elements=[e for batch in raw_flow if isinstance(batch,c.MessageBatch) for e in batch.elements]
    output=list(ordered(reader()))
    boundaries=[x for x in output if isinstance(x,MessageBoundary)]
    assert [x.raw for x in boundaries]==raw_messages
    assert [x.raw for x in output if isinstance(x,Element)]==raw_elements
    update_boundaries=[x for x in boundaries if x.raw['source_id'] in manifest['update_sources']]
    assert [(x.raw['epoch'],x.raw['microsecond'],x.raw_time.microsecond) for x in update_boundaries]==[
        (100,None,900000),(100,500000,500000),(100,None,900000),(100,500000,500000),(100,500000,500000)]
    gaps=[x.gap for x in boundaries if x.gap]
    assert [g.parse_status for g in gaps]==[ParseStatus.REJECTED,ParseStatus.UNSUPPORTED]
    assert [g.raw_time.microsecond for g in gaps]==[900000,900000]
    assert [x.raw for x in output if isinstance(x,SourceStart)]==[x for x in raw_flow if isinstance(x,c.SourceStart)]
    assert [x.raw for x in output if isinstance(x,SourceEnd)]==[x for x in raw_flow if isinstance(x,c.SourceEnd)]
    quality=[q for x in boundaries for q in x.raw['quality']]+[x.raw for x in output if isinstance(x,SourceQuality)]
    assert any(q['code']=='timestamp_regression' for q in quality)
    assert any(q['code']=='cross_file_time_overlap' for q in quality)
    (tmp_path/'实际Reader与派生时间.json').write_text(json.dumps(dict(
        run_id=seal['run_id'],snapshot=seal['snapshot'],seal_digest=seal['digest'],
        messages=raw_messages,derived=[dict(message_id=x.raw['message_id'],raw_time=asdict(x.raw_time),
            gap_id=x.gap.gap_id if x.gap else None) for x in boundaries],quality=quality,
        elements=len(raw_elements),gaps=len(gaps)),ensure_ascii=False,indent=2))
