"""8233：复用原人工 M2 的 canonical 独立生产、完整读取和固定 52f 对账。"""
from collections import Counter
from contextlib import contextmanager,closing
from copy import deepcopy
from dataclasses import asdict,replace
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
from types import SimpleNamespace

import psycopg2
import pytest
from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch
from data_pipeline.bgp.replay.snapshot_contract import TABLES, ProjectionBinding, encode, decode
from data_pipeline.bgp.replay.snapshot_store import ProjectionReader, ProjectionWriter
from data_pipeline.bgp.replay import route_snapshot as producer, snapshot_validation as validation
from data_pipeline.bgp.replay.archive_input import messages
from data_pipeline.bgp.replay.route_replay import ReplayPlan
from tests.observations.test_canonical_projection import test_gap_cross_family_slots_same_asn_peer_and_exact_recovery


def save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str))
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def old_module(root):
    path=root/'52f原state.py'
    assert sha(path)=='340025bb11b2249da409cb15fee766a64d4d3f168625869398161e264711d354'
    spec=importlib.util.spec_from_file_location('integration_52f_state',path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    return module


@contextmanager
def measured():
    """仅统计实际流返回；mapping 的 SQL 不冒充已计入物理 IO。"""
    count=dict(replays=0,projection_rows=0,projection_bytes=0,messages=0,elements=0,reference_rows=0,message_element_bytes=0,reference_bytes=0)
    start=time.monotonic();expected=validation.expected_rows;stream=ObservationReader.stream;refs=ObservationReader.reference_batches
    def expected_rows(*args,**kwargs):
        count['replays']+=1
        for table,row in expected(*args,**kwargs):
            count['projection_rows']+=1;count['projection_bytes']+=len(encode(row).encode());yield table,row
    def streamed(self):
        with closing(stream(self)) as source:
            for item in source:
                if isinstance(item,MessageBatch):
                    count['messages']+=len(item.messages);count['elements']+=len(item.elements)
                    count['message_element_bytes']+=len(encode((item.messages,item.elements)).encode())
                yield item
    def referenced(self,*args,**kwargs):
        with closing(refs(self,*args,**kwargs)) as source:
            for batch in source:
                rows=batch.to_pylist();count['reference_rows']+=len(rows);count['reference_bytes']+=len(encode(rows).encode());yield batch
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(validation,'expected_rows',expected_rows);patch.setattr(ObservationReader,'stream',streamed);patch.setattr(ObservationReader,'reference_batches',referenced)
        yield count
    count.update(wall=time.monotonic()-start,process_lifetime_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),rss_scope='本进程生命周期累计峰值，包含此前操作，不含PG；不是阶段峰值')


def drain(scan):
    rows=[]
    while True:
        try:rows.append(next(scan))
        except StopIteration as stop:
            assert stop.value['execution']=='complete' and stop.value['rows']==len(rows)
            return rows,stop.value


def child_read(config_path):
    q=json.loads(Path(config_path).read_text());root=Path(q['root'])
    reader=ProjectionReader(q['dsn'],ProjectionBinding(**q['binding']))
    with measured() as costs:
        rows={};receipts={}
        for table in TABLES:rows[table],receipts[table]=drain(reader.scan(table,batch_rows=128))
    assert costs['replays']==13
    (root/'新进程完整13表.typed.json').write_text(encode(rows))
    save(root/'新进程读取成本与回执.json',dict(costs=costs,receipts=receipts))


@pytest.fixture(scope='module')
def integrated():
    where=os.environ.get('DOMEYE_CANONICAL_OWN_ROOT');input_request=os.environ.get('DOMEYE_CANONICAL_INPUT_REQUEST')
    if not where or not input_request:pytest.skip('必须显式绑定本任务人工证据')
    root=Path(where);q=json.loads(Path(input_request).read_text());dsn=q['observation_dsn']
    assert psycopg2.extensions.parse_dsn(dsn)['host']=='/tmp/domeye-integration-detection-8233/socket'
    def source():return ObservationReader(dsn,q['input_run'],q['input_snapshot'],q['ordered_sources'],profile='observation')
    original=source();seal=deepcopy(original.selection.seal)
    files={f['path']:sha(f['path']) for cp in original.selection.checkpoints for f in cp['files']}
    for parent in (Path(input_request).parent,Path('/tmp/domeye-feature-m3-integration-8233/feature-m30')):
        files.update({str(p):sha(p) for p in parent.rglob('*') if p.is_file()})
    def metadata():
        with closing(psycopg2.connect(dsn)) as pg,pg.cursor() as c:
            c.execute('SELECT to_jsonb(r) FROM observation_m2.runs r WHERE run_id=%s',(q['input_run'],));run=c.fetchone()[0]
            c.execute('SELECT to_jsonb(c) FROM observation_m2.checkpoints c WHERE run_id=%s ORDER BY ordinal',(q['input_run'],));return dict(run=run,checkpoints=c.fetchall())
    initial=metadata();save(root/'M2及原FeatureDetection核对前.json',dict(seal=seal,metadata=initial,files=files))
    outputs={t:[] for t in TABLES};append=ProjectionWriter.append
    def capture(self,table,row):outputs[table].append(deepcopy(row));return append(self,table,row)
    from data_pipeline.bgp.archive.store import Store
    def forbidden(*args,**kwargs):raise AssertionError('禁止 M2 finish 或重新生产 M2')
    from data_pipeline.bgp.archive import checkpoint
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ProjectionWriter,'append',capture);patch.setattr(Store,'finish',forbidden);patch.setattr(checkpoint,'produce_checkpointed',forbidden)
        with measured() as cost:binding=producer.produce_projection(source(),root/'canonical',min_free_bytes=0,batch_rows=128)
    (root/'生产完整13表.typed.json').write_text(encode(outputs));save(root/'生产外层实测.json',cost)
    config=dict(dsn=dsn,binding=asdict(binding),root=str(root));save(root/'reader.json',config)
    yield root,source,binding,outputs
    assert source().selection.seal==seal and metadata()==initial
    assert all(sha(p)==h for p,h in files.items())
    save(root/'M2及原FeatureDetection核对后.json',dict(m2_run=q['input_run'],snapshot=q['input_snapshot'],seal=seal['digest'],original_files=len(files),all_original_sha_equal=True,m2_registration_and_checkpoints_equal=True,m2_seal_equal=True))


def test_real_m2_full_public_read_and_52f_science(integrated):
    root,source,binding,outputs=integrated;reader=ProjectionReader(source().dsn,binding)
    with measured() as audit_cost:
        descriptor=reader.descriptor();audit=reader.audit()
    assert audit_cost['replays']==1 and audit['audit']=='complete'
    save(root/'公有descriptor与audit.json',dict(descriptor=descriptor,audit=audit,cost=audit_cost))
    code='import sys;sys.path[:0]=sys.argv[1:3];from tests.observations.test_canonical_m3b_integration import child_read;child_read(sys.argv[3])'
    child=subprocess.run([sys.executable,'-I','-c',code,str(Path(__file__).resolve().parents[2]),str(Path(__file__).resolve().parent),str(root/'reader.json')],capture_output=True,text=True)
    (root/'reader.stdout').write_text(child.stdout);(root/'reader.stderr').write_text(child.stderr);assert child.returncode==0,child.stderr
    assert decode((root/'新进程完整13表.typed.json').read_text())==outputs
    assert set(outputs)==set(TABLES)
    # 独立旧代码从已有 SQL 消息转换路径读取原 M2；不复用新 canonical 转换器。
    plan=reader.seal['plan'];old=old_module(root);legacy=old.Replay(old.ReplayPlan('rrc25',source().sources[0],source().sources[1:],tuple(tuple(e) for e in plan['baseline_endpoints'])))
    helper=SimpleNamespace(dsn=source().dsn,memory_limit='1GB',root=root)
    original_messages=list(messages(helper,source().sources[0],source().sources[1:],source().snapshot,reader=source()))
    raw=[r for m in original_messages for r in legacy.consume(m)]
    combined=sorted(outputs['changes']+outputs['invalidations'],key=lambda r:(r['position'].get('message',r['position'])['source_rank'],r['position'].get('message',r['position'])['record'],r.get('raw',{}).get('ordinal',-1)))
    assert [r['raw'] for r in combined]==raw
    assert {(r['prefix'],r['vp']):r['path_text'] for r in outputs['legacy_state']}==legacy.legacy_paths
    assert {r['prefix']:r['paths'] for r in outputs['legacy_prefixes']}==legacy.legacy_by_prefix
    assert {r['prefix']:r['origins'] for r in outputs['legacy_prefixes']}==legacy.legacy_origins
    assert {r['vp'] for r in outputs['legacy_seen_vps']}==legacy.seen_vps
    meta=outputs['projection_metadata'][0];assert meta['cursor']==legacy.cursor and meta['legacy_baseline_epoch']==legacy.legacy_baseline_epoch
    assert {r['scope']:r['last_known'] for r in outputs['current_routes']}==legacy.last_known
    gaps=outputs['scope_gap'];assert len(gaps)==1
    # 实际样本尾部为 LOCAL Gap，不失效 received 当前态。
    assert gaps[0]['raw']['direction']=='local'
    assert {r['scope']:r['current'] for r in outputs['current_routes']}==legacy.current
    assert all(r['qualification']['current']=='known' for r in outputs['current_routes'])
    assert next(r for r in outputs['qualification_change'] if r['target']=='scope')['status']=='not_applicable'
    by_id={m.message_id:m for m in original_messages}
    for row in outputs['changes']:
        assert row['message_id'] in by_id and row['event_id']==row['raw']['event_id']
        assert row['position']['message']['source_rank']==source().sources.index(row['source_id'])
        assert any(e['ordinal']==row['raw']['ordinal'] for e in by_id[row['message_id']].elements)
    assert [r['raw']['messages'] for r in outputs['source_coverage']]==[4,14]
    assert sum(r['raw']['elements'] for r in outputs['source_coverage'])==22
    assert len(outputs['reference_binding'])==11 and sum(r['rows'] for r in outputs['reference_binding'])==24
    save(root/'完整对账摘要.json',dict(binding=asdict(binding),counts={t:len(v) for t,v in outputs.items()},digests={t:hashlib.sha256(encode(v).encode()).hexdigest() for t,v in outputs.items()},all_thirteen_bodies_equal=True,old_52f_raw_equal=True,old_eight_state_fields_equal=True,messages=len(original_messages),elements=sum(len(m.elements) for m in original_messages),gap_direction='local',actual_current_unknown=0,limitation='本实际M2没有received Gap、合法LOCAL元素、非连续rank或未选择snapshot；额外科学边界由手工流测试单列'))


def test_actual_legal_nested_body_and_unknown_rule_rejected(integrated,monkeypatch):
    root,source,binding,outputs=integrated;target=outputs['changes'][-1]['event_id'];original=ProjectionWriter.append;changed=[]
    def broken(self,table,row):
        if table=='changes' and not changed:
            row=deepcopy(row);changed.append((row['event_id'],target));assert row['event_id']!=target
            row['raw']['calculation_after']['event_id']=target  # 另一个真实事件；外层有效引用不变。
        return original(self,table,row)
    with monkeypatch.context() as patch:
        patch.setattr(ProjectionWriter,'append',broken)
        with pytest.raises(ValueError,match='绑定 M2 推导不符') as caught:
            producer.produce_projection(source(),root/'failed-body',min_free_bytes=0,batch_rows=128)
    failure=json.loads((root/'failed-body/failure.json').read_text())
    with closing(psycopg2.connect(source().dsn)) as pg,pg.cursor() as c:
        c.execute('SELECT state,seal FROM m3_projection.runs WHERE run_id=%s',(failure['run_id'],));assert c.fetchone()==('failed',None)
        c.execute('SELECT count(*) FROM m3_projection.runs');before=c.fetchone()[0]
    with monkeypatch.context() as patch:
        patch.setattr(producer,'RULE','unknown-replay/v999')
        with pytest.raises(ValueError,match='算法/规则'):
            producer.produce_projection(source(),root/'unknown-rule',min_free_bytes=0,batch_rows=128)
    with closing(psycopg2.connect(source().dsn)) as pg,pg.cursor() as c:
        c.execute('SELECT count(*) FROM m3_projection.runs');assert c.fetchone()[0]==before
    save(root/'真实正文与规则拒绝.json',dict(real_events=changed,failure_run=failure['run_id'],state='failed',seal=None,error=str(caught.value),unknown_rule_rejected_before_registration=True))


def test_fixed_52f_typed_science_and_unselected_snapshot():
    from tests.observations.test_observation_ordered import snapshot_reader, message, binding, stream_source
    from data_pipeline.bgp.ordered_reader import adapt
    from data_pipeline.bgp.replay.quality_overlay import CanonicalReplay
    from data_pipeline.bgp.input.mrt_reader import Message
    from itertools import chain
    location=os.environ.get('DOMEYE_CANONICAL_OWN_ROOT')
    if not location:pytest.skip('需本任务固定52f导出证据')
    root=Path(location)
    _,_,groups=snapshot_reader(selected=('s0','s2'))
    rows=groups[2][1]
    for e in rows[0][1]:e.update(afi=2,prefix='2001:db8::/32',path_id_present=True,path_id=0,as_path_text='{328405}',raw_origin_asn=None,attributed_origin_asn=None)
    rows[0][1][1].update(path_id=1)
    rows[1]=message('s2',1,elements=1,path_present=True,path_id=0)
    rows[1][1][0].update(afi=2,prefix='2001:db8::/32',action='withdraw')
    rows.extend([message('s2',2,direction='local',elements=1),message('s2',3,kind='state_change')])
    b=binding(groups);b=replace(b,sources=tuple(replace(s,role='snapshot') if s.source_id=='s1' else s for s in b.sources))
    selected=('s0','s2');endpoints=(('192.0.2.1',64497,'192.0.2.2',64496,0),)
    core=CanonicalReplay(ReplayPlan('rrc25','s0',('s2',),endpoints),b,selected)
    out=[pair for item in adapt(chain.from_iterable(stream_source(b,sid,rs) for sid,rs in groups if sid in selected),b,selected) for pair in core.apply(item)]
    old=old_module(root);legacy=old.Replay(old.ReplayPlan('rrc25','s0',('s2',),endpoints));expected=[]
    for sid,rs in groups:
        if sid not in selected:continue
        for r,elements in rs:
            m=Message(*(r[k] for k in ('source_id','record','offset','length','epoch','mrt_type','mrt_subtype','raw_digest')),content_sha256=r['content_sha256'],microsecond=r['microsecond'],kind=r['kind'],reason=r['reason'],old_state=r['old_state'],new_state=r['new_state'])
            m.peer={k:r[v] for k,v in (('ip','peer_ip'),('asn','peer_asn'),('bgp_id','bgp_id'),('bgp_id_present','bgp_id_present'),('local_ip','local_ip'),('local_asn','local_asn'),('interface','interface'),('local_message','local_message'))}
            for e in elements:
                m.elements.append({k:e[k] for k in ('ordinal','action','afi','safi','prefix','path_id','path_id_present','path_key')}|{'peer':dict(m.peer,ip=e['peer_ip'],asn=e['peer_asn'],bgp_id=e['bgp_id'],bgp_id_present=e['bgp_id_present'],table_record=e['peer_table_record'],index=e['peer_index'])})
                m.paths.append({k:e[k] for k in ('path_key','as_path_text','attributed_origin_asn')})
            expected.extend(legacy.consume(m))
    assert [r['raw'] for t,r in out if t in ('changes','invalidations')]==expected
    fields=('current','last_known','legacy_paths','legacy_by_prefix','legacy_origins','seen_vps','legacy_baseline_epoch','cursor')
    assert all(getattr(core.replay,k)==getattr(legacy,k) for k in fields)
    assert [r['source_rank'] for t,r in out if t=='source_coverage']==[0,2]
    updates=[r for t,r in out if t=='changes' and r['source_id']=='s2']
    assert all(r['position']['message']['source_rank']==2 and r['raw']['source_rank']==1 for r in updates)
    assert not any(r['raw']['record']==2 for r in updates)
    k=('rrc25','192.0.2.1',64497,'192.0.2.2',64496,0,2,1,'2001:db8::/32',True,0)
    assert core.replay.last_known[k]['presence']=='absent'
    assert core.replay.current[k]['presence']=='unknown'  # 真 STATE，保留最后已知 W。
    assert core.replay.legacy_origins['2001:db8::/32']==set()
    assert updates[0]['raw']['calculation_after']['origin'] is None
    assert decode(encode(list(core.export())))==list(core.export())
    save(root/'手工流固定52f与数值.json',dict(old_eight_fields_equal=True,complete_raw_equal=True,full_source_ranks=[0,2],old_selected_update_rank=1,local_changes=0,ipv6_path_id_zero_present=True,last_known_presence='absent',current_presence='unknown',as_set_attribution=None,legacy_empty_origins_preserved=True,scope='手工typed流；不是主M2的额外覆盖'))


def test_public_current_unseen_is_unknown():
    location=os.environ.get('DOMEYE_CANONICAL_OWN_ROOT')
    if not location:pytest.skip('需本任务已完成投影')
    root=Path(location);q=json.loads((root/'reader.json').read_text())
    reader=ProjectionReader(q['dsn'],ProjectionBinding(**q['binding']))
    saved=decode((root/'生产完整13表.typed.json').read_text())
    scope=(*saved['current_routes'][0]['scope'][:8],'203.0.113.128/25',False,None)
    assert scope not in {r['scope'] for r in saved['current_routes']}
    with measured() as costs:result=reader.current(scope)
    assert result['current'] is None and result['last_known'] is None
    assert result['qualification']['current']=='unknown'
    assert costs['replays']==2
    (root/'公有未见对象Unknown.typed.json').write_text(encode(result))
    save(root/'公有未见对象成本.json',costs)
