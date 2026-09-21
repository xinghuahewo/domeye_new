"""现有业务算法对照：逐条旧值、窗口计数、活动事件和恢复，不靠新实现自证。"""
from dataclasses import asdict,replace
from datetime import datetime,timedelta,timezone
from copy import deepcopy
from types import SimpleNamespace
import pyarrow as pa
import pytest
from data_pipeline.analysis.features.projection import FeatureAdapter, FeaturePlan, SourceBinding
from data_pipeline.analysis.detection.models import DetectionSeed, FileBoundary
from data_pipeline.analysis.detection.streaming import StreamingDetectionEngine
from data_pipeline.analysis.detection.adapter import adapt_element
from data_pipeline.analysis.detection._results import plain
from data_pipeline.bgp.ordered_reader import adapt
from data_pipeline.bgp.record_types import Element
from data_pipeline.bgp.state.business import BusinessLoop, pack, unpack
from data_pipeline.bgp.state.path_dictionary import TextPool, PathText
from data_pipeline.bgp.replay.snapshot_contract import encode, decode
from tests.observations.test_observation_ordered import message, binding, stream_source
from tests.features.test_feature_projection import REF
from tests.detection.test_detection_computation import refs, KINDS


def same_records(actual,expected):
    if actual==expected:return
    differences=[]
    def visit(a,b,path):
        if len(differences)>=12 or a==b:return
        if isinstance(a,dict) and isinstance(b,dict) and a.keys()==b.keys():
            for k in a:visit(a[k],b[k],path+'/'+str(k))
        elif isinstance(a,list) and isinstance(b,list) and len(a)==len(b):
            for i,(x,y) in enumerate(zip(a,b)):visit(x,y,path+'/'+str(i))
        else:differences.append((path,a,b))
    visit(actual,expected,'result')
    assert not differences,differences


def inputs():
    groups=[]
    # 首个 UPDATE 结束时前缀中断保持活动，恢复后才关闭。
    specifications=[[("rib_snapshot",p,f'{p} 1') for p in (100,101,102)],
        [('withdraw',p,'') for p in (100,101,102)],
        [('announce',100,'100 1'),('announce',100,'100 1'),('announce',101,'101 1'),
         ('announce',100,'100 3'),('announce',100,'100 2'),('withdraw',100,''),
         ('withdraw',199,''),('announce',100,'100 {1,2}')],[]]
    for rank,(sid,spec) in enumerate(zip(('z','b','a','c'),specifications)):
        rows=[]
        for n,(action,vp,path) in enumerate(spec):
            m,es=message(sid,n,elements=1,kind='rib' if rank==0 else 'update',direction='unknown' if rank==0 else 'received',endpoint=rank!=0)
            m['epoch']=1000+rank*300
            if rank==0:m.update(mrt_type=13,mrt_subtype=2,local_message=None)
            e=es[0];e.update(action=action,prefix='10.0.0.0/24',peer_asn=vp,as_path_text=path,epoch=m['epoch'],
                              mrt_type=m['mrt_type'],mrt_subtype=m['mrt_subtype'],local_message=m['local_message'])
            rows.append((m,es))
        groups.append((sid,rows))
    return groups


def config_for(groups):
    def at(sec):return datetime.fromtimestamp(sec,timezone.utc).isoformat()
    return dict(audit_detection=True,feature_reference=asdict(REF),detection_reference=asdict(refs()),windows={sid:dict(
        start=at(1000+rank*300),end=at(1000+(rank+1)*300),file_time=at(1000+rank*300),coverage='complete',
        message_quality_state='complete',legacy_tables={k:k+'_197001' for k in KINDS}) for rank,(sid,_) in enumerate(groups)})


def create(groups,config=None):
    b=binding(groups);config=config or config_for(groups)
    manifest=dict(collector='rrc25',baseline_source=groups[0][0],update_sources=[s for s,_ in groups[1:]],
        window_start='1970-01-01T00:00:00Z',window_end_exclusive='1970-01-02T00:00:00Z')
    bound=dict(binding_id=b.binding_id,observation_run='run1');pool=TextPool()
    pool.register([e['as_path_text'] for _,rs in groups for _,es in rs for e in es])
    output=[];loop=BusinessLoop(config,dict(manifest=manifest),bound,pool,lambda t,r:output.append((t,r)))
    return b,loop,output


def feed(b,loop,sid,rows):
    for item in adapt(stream_source(b,sid,rows,2),b,(sid,)):
        loop.apply(item)


@pytest.mark.parametrize('resume_after',[None,0,1,2])
def test_existing_feature_detection_and_recovery(resume_after):
    groups=inputs();b,loop,output=create(groups)
    expected_detection=[]
    sources=tuple(SourceBinding(sid,'a'*64,'baseline' if n==0 else 'update',loop.windows[sid],sum(len(es) for _,es in rs),
        ('fixture:messages:'+sid,),'complete') for n,(sid,rs) in enumerate(groups))
    adapters={mode:FeatureAdapter(mode,REF,FeaturePlan(b.binding_id,'rrc25',sources),loop.binding['observation_run']) for mode in loop.features}
    detector=None;initial=None
    def decoded(row):
        return replace(adapt_element(row,run_id=loop.binding['observation_run'],snapshot='candidate:'+b.binding_id,collector_id='rrc25'),source_version=b.binding_id)
    for rank,(sid,rows) in enumerate(groups):
        raw=[e for _,es in rows for e in es]
        if rank==0:
            detector=StreamingDetectionEngine(DetectionSeed(tuple(decoded(r) for r in raw),sid),refs(),loop.scope,sink=lambda r:expected_detection.append(plain(r)))
        else:
            detector.begin_file(FileBoundary(sid,b.binding_id,loop.windows[sid].file_time.isoformat(),loop.config['windows'][sid]['legacy_tables']))
            for r in raw:detector.consume(decoded(r))
            detector.finish_file()
        feed(b,loop,sid,rows)
        for mode,adapter in adapters.items():
            result=adapter.consume_source(sources[rank],() if not raw else [pa.RecordBatch.from_pylist(raw)])
            assert {p:{vp:str(v) for vp,v in paths.items()} for p,paths in loop.features[mode].prefix_dict.items()}=={p:dict(v) for p,v in adapter.state.projection.prefix_dict.items()}
            assert loop.features[mode].prefix_as==dict(adapter.state.projection.prefix_as)
            assert loop.features[mode].as_prefix=={k:set(v) for k,v in adapter.state.projection.as_prefix.items()}
            assert loop.work[mode].feature_dict==adapter.state.feature_dict
            assert loop.work[mode].feature_collect_dict==adapter.state.feature_collect_dict
            if result is not None:
                actual=[r['raw'] for t,r in output if t=='feature_result' and r['mode']==mode and r['source_id']==sid]
                assert actual==[plain(asdict(r)) for r in result.rows]
        assert loop.detection_routes.export()==detector.projection.export()
        actual_detection=[r for t,r in output if t=='detection_result']
        assert actual_detection==expected_detection
        if rank==1:assert loop.engine.outage.prefix_vp['10.0.0.0/24']['is_outage_now']
        if rank==resume_after:
            before=[(ns,encode(pack(key)),encode(pack(value))) for ns,key,value in loop.rows()]
            replacement=BusinessLoop(loop.config,loop.plan,loop.binding,loop.pool,lambda t,r:output.append((t,r)))
            replacement.restore((ns,unpack(decode(key),loop.pool),unpack(decode(value),loop.pool)) for ns,key,value in before)
            after=[(ns,encode(pack(key)),encode(pack(value))) for ns,key,value in replacement.rows()]
            assert before==after
            loop=replacement
    assert loop.engine.outage.prefix_vp['10.0.0.0/24']['is_outage_now'] is False
    assert loop.pool.single_reads==0


def native_business_fixture(root):
    """同一业务场景另行手工编码为 MRT，不调用 parser 反向生成。"""
    import gzip,hashlib,struct
    from tests.observations.test_observation_mrt import mrt
    from data_pipeline.bgp.input.mrt_reader import source_identity
    root.mkdir();groups=inputs();entries=[]
    def attrs(text):
        values=text.split();body=b'';sequence=[]
        for v in values:
            if '{' in v:
                if sequence:body+=bytes([2,len(sequence)])+b''.join(struct.pack('!I',n) for n in sequence);sequence=[]
                members=[int(n) for n in v.strip('{}').split(',')];body+=bytes([1,len(members)])+b''.join(struct.pack('!I',n) for n in members)
            else:sequence.append(int(v))
        if sequence:body+=bytes([2,len(sequence)])+b''.join(struct.pack('!I',n) for n in sequence)
        return bytes([64,2,len(body)])+body if body else b''
    for rank,(sid,rows) in enumerate(groups):
        timestamp=1000+rank*300;raw=b''
        if rank==0:
            table=b'\0\0\0\x19\0\x05rrc25'+struct.pack('!H',3)
            for vp in (100,101,102):table+=b'\x02'+bytes([192,0,2,vp])*2+struct.pack('!I',vp)
            raw+=mrt(table,1,13,timestamp)
        for n,(_,es) in enumerate(rows):
            e=es[0];vp=e['peer_asn'];path=attrs(e['as_path_text']);nlri=b'\x18\x0a\0\0'
            if rank==0:
                body=struct.pack('!I',n)+nlri+struct.pack('!HHIH',1,vp-100,timestamp-10,len(path))+path
                raw+=mrt(body,2,13,timestamp)
            else:
                endpoint=struct.pack('!IIHH',vp,12654,0,1)+bytes([192,0,2,vp])+bytes([192,0,2,254])
                withdraw=nlri if e['action']=='withdraw' else b'';announce=nlri if e['action']=='announce' else b''
                body=struct.pack('!H',len(withdraw))+withdraw+struct.pack('!H',len(path))+path+announce
                bgp=b'\xff'*16+struct.pack('!HB',len(body)+19,2)+body
                raw+=mrt(endpoint+bgp,4,16,timestamp)
        p=root/(sid+'.gz');p.write_bytes(gzip.compress(raw,mtime=0));sha=hashlib.sha256(p.read_bytes()).hexdigest();uri='fixture://rrc25/business/'+sid
        entries.append(dict(path=str(p),sha256=sha,size=p.stat().st_size,source_id=source_identity('rrc25',uri,sha),origin_uri=uri,role='baseline' if rank==0 else 'update'))
    config=config_for([(e['source_id'],rs) for e,(_,rs) in zip(entries,groups)])
    manifest=dict(schema_version='observation-run/v1',collector='rrc25',baseline_source=entries[0]['source_id'],
        update_sources=[e['source_id'] for e in entries[1:]],inputs=entries,window_start='1970-01-01T00:00:00Z',window_end_exclusive='1970-01-02T00:00:00Z')
    return manifest,config


def assert_business_serial(dsn,root,result,manifest,config):
    import json
    from pathlib import Path
    from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch
    from data_pipeline.bgp.state.checkpoint import StateStore
    from data_pipeline.bgp.state.business import BusinessLoop
    from data_pipeline.bgp.input.native_batches import HotPaths, persisted_segments
    from data_pipeline.bgp.archive.file_reader import selected_prefix
    import psycopg2
    import pyarrow.parquet as pq
    rid=result['run_id'];snapshot=result['observation_snapshot'];selected=[manifest['baseline_source'],*manifest['update_sources']]
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT binding FROM route_file.runs WHERE run_id=%s',(rid,));bound=c.fetchone()[0]
    paths=HotPaths();out=[]
    loop=BusinessLoop(config,dict(manifest=manifest),bound,paths.texts,lambda t,r:out.append((t,r)))
    for cp in selected_prefix(dsn,rid)['checkpoints']:
        if cp['source_id'] in selected:
            for _,tables in persisted_segments(cp):paths.register(tables.get('paths'))
    with __import__('contextlib').closing(StateStore(dsn,rid,Path(root)/'route-files',bound,audit_transitions=False)) as store:
        store.restore_business(loop)
        actual=[]
        for _,_,payload in store.files():
            for row in pq.read_table(payload['file']['path']).to_pylist():actual.append((row['table'],decode(row['payload'])))
        restored_meta=store.status()[1]
        assert restored_meta['business']['version']=='business-state/v2'
        assert restored_meta['row_counts']['prefix']==0 # 正常主链不再复制 canonical 旧 VP 视图。
    sources=[];groups={s:[] for s in selected}
    reader=ObservationReader(dsn,rid,snapshot,selected,profile='observation',batch_rows=2)
    for item in reader.stream():
        if isinstance(item,MessageBatch):groups[item.source_id].extend(item.elements)
    for n,s in enumerate(selected):sources.append(SourceBinding(s,manifest['inputs'][n]['sha256'],'baseline' if n==0 else 'update',loop.windows[s],len(groups[s]),('checked-messages:'+s,),'complete'))
    adapters={m:FeatureAdapter(m,loop.reference,FeaturePlan(bound['binding_id'],'rrc25',tuple(sources)),rid) for m in loop.features}
    expected_detection=[];detector=None
    def decoded(row):return replace(adapt_element(row,run_id=rid,snapshot='candidate:'+bound['binding_id'],collector_id='rrc25'),source_version=bound['binding_id'])
    for n,s in enumerate(selected):
        raw=groups[s]
        if n==0:detector=StreamingDetectionEngine(DetectionSeed(tuple(decoded(r) for r in raw),s),refs(),loop.scope,sink=lambda r:expected_detection.append(plain(r)))
        else:
            detector.begin_file(FileBoundary(s,bound['binding_id'],loop.windows[s].file_time.isoformat(),config['windows'][s]['legacy_tables']))
            for r in raw:detector.consume(decoded(r))
            detector.finish_file()
        for m,adapter in adapters.items():
            expected=adapter.consume_source(sources[n],() if not raw else [pa.RecordBatch.from_pylist(raw)])
            if expected:
                assert [r['raw'] for t,r in actual if t=='feature_result' and r['mode']==m and r['source_id']==s]==[plain(asdict(r)) for r in expected.rows]
    same_records([r for t,r in actual if t=='detection_result'],expected_detection)
    assert loop.detection_routes.export()==detector.projection.export()
    for m,a in adapters.items():assert loop.work[m].feature_dict==a.state.feature_dict
    assert loop.completed==len(selected) and loop.engine.outage.prefix_vp['10.0.0.0/24']['is_outage_now'] is False
    assert result['features']==result['detection']=='file_candidate_complete'
    paths.close()


def test_normal_output_drops_only_reproducible_audit_rows():
    groups=inputs();b,a,full=create(groups);config=config_for(groups);config['audit_detection']=False
    _,short,compact=create(groups,config)
    for sid,rs in groups:feed(b,a,sid,rs);feed(b,short,sid,rs)
    from data_pipeline.analysis.detection.streaming import BoundedResults
    expected=[(t,r) for t,r in full if t!='detection_result' or r['kind'] not in BoundedResults.OMITTED_AUDIT]
    assert compact==expected
    assert len(compact)<len(full) and a.detection_routes.export()==short.detection_routes.export()


def test_shared_texts_are_in_memory_and_missing_reference_rejected():
    pool=TextPool();pool.max_entries=3
    texts=[str(i)+' '+str(i+1) for i in range(100)]
    pool.register(texts)
    pool.register(texts)
    assert len(pool.texts)==len(texts) and pool.queries==0
    for text in texts:pool.token(text)
    assert len(pool.tokens)<=3
    saved=pool.token(texts[0]);pool.register(['later'])
    assert str(saved)==texts[0]
    with pytest.raises(ValueError,match='引用缺失'):pool.value(pool.ident('not_registered'))


def test_reference_interpretation_does_not_mutate_bound_config():
    groups=inputs();config=config_for(groups)
    config['detection_reference']['mappings']['as_info']['1']['import_as']="['2']"
    original=deepcopy(config);b,loop,_=create(groups,config)
    feed(b,loop,*groups[0])
    assert loop.engine.info.as_info['1']['import_as']==['2'] and config==original


@pytest.mark.parametrize('direct',[False,True])
@pytest.mark.parametrize('resume_after',[None,0,1,2])
def test_ir_only_without_detection_matches_existing_feature_and_resumes(monkeypatch,resume_after,direct):
    """关闭检测不加载检测参考、不建立引擎，IR字段仍与既有算法逐窗口相同。"""
    groups=inputs();config=config_for(groups)
    config.update(feature_modes=['ir'],detection_enabled=False)
    config.pop('detection_reference')
    for window in config['windows'].values():window.pop('legacy_tables')
    def forbidden(*args,**kwargs):raise AssertionError('仅IR任务不应创建检测引擎')
    monkeypatch.setattr(BusinessLoop,'make_engine',forbidden)
    b,loop,output=create(groups,config)
    sources=tuple(SourceBinding(sid,'a'*64,'baseline' if n==0 else 'update',loop.windows[sid],
        sum(len(es) for _,es in rs),('fixture:messages:'+sid,),'complete') for n,(sid,rs) in enumerate(groups))
    adapter=FeatureAdapter('ir',REF,FeaturePlan(b.binding_id,'rrc25',sources),loop.binding['observation_run'])
    from data_pipeline.bgp.state.dirty_keys import install_changes
    for rank,(sid,rows) in enumerate(groups):
        if not direct:
            feed(b,loop,sid,rows)
        else:
            from data_pipeline.bgp.state.rib_columns import RibBatch, FIELDS
            from data_pipeline.bgp.state.update_columns import UpdateBatch
            from data_pipeline.bgp.state.update_fields import prepare_update_batch, apply_update_fields
            for item in adapt(stream_source(b,sid,rows,2),b,(sid,)):
                if not isinstance(item,Element):
                    loop.apply(item);continue
                row=item.raw;path={row['path_key']:{'as_path_text':row['as_path_text']}}
                if row['action']=='rib_snapshot':
                    loop.apply_rib_batch(RibBatch(b.binding_id,loop.boundary,tuple([row.get(k)] for k in FIELDS),path))
                else:
                    batch=UpdateBatch(b.binding_id,{k:[v] for k,v in row.items()},path,((loop.boundary,0,1,False),))
                    prepare_update_batch(loop,batch);apply_update_fields(loop,batch,0,loop.boundary)
        raw=[e for _,es in rows for e in es]
        result=adapter.consume_source(sources[rank],() if not raw else [pa.RecordBatch.from_pylist(raw)])
        assert loop.work['ir'].feature_dict==adapter.state.feature_dict
        assert loop.work['ir'].feature_collect_dict==adapter.state.feature_collect_dict
        assert loop.features['ir'].as_prefix=={k:set(v) for k,v in adapter.state.projection.as_prefix.items()}
        if result is not None:
            assert [r['raw'] for t,r in output if t=='feature_result' and r['source_id']==sid]==[plain(asdict(r)) for r in result.rows]
        assert loop.engine is None and loop.bundle is None and not loop.detection_routes.prefix_dict
        assert all(not t.startswith('detection_') for t,_ in output)
        assert set(loop.features)=={'ir'}
        changes=install_changes(loop)
        assert [(n,encode(pack(k)),encode(pack(v))) for n,k,v in changes.baseline_rows()]==[(n,encode(pack(k)),encode(pack(v))) for n,k,v in loop.rows()]
        if rank==resume_after:
            saved=[(ns,encode(pack(key)),encode(pack(value))) for ns,key,value in loop.rows()]
            replacement=BusinessLoop(config,loop.plan,loop.binding,loop.pool,lambda t,r:output.append((t,r)))
            replacement.restore((ns,unpack(decode(key),loop.pool),unpack(decode(value),loop.pool)) for ns,key,value in saved)
            assert saved==[(ns,encode(pack(key)),encode(pack(value))) for ns,key,value in replacement.rows()]
            loop=replacement
    assert loop.pool.single_reads==0


def test_detection_setting_is_boolean_and_recovery_rejects_mismatch():
    groups=inputs();config=config_for(groups);config['detection_enabled']='false'
    with pytest.raises(ValueError,match='布尔值'):create(groups,config)
    b,loop,_=create(groups);feed(b,loop,*groups[0])
    config=config_for(groups);config.update(feature_modes=['ir'],detection_enabled=False)
    _,other,_=create(groups,config)
    with pytest.raises(ValueError,match='检测启用配置不一致'):other.restore(loop.rows())
