"""M3B 人工 canonical 手算与显式自有 PG 链路。"""
from copy import deepcopy
from dataclasses import asdict,replace
import json
import pytest
from tests.observations.test_observation_ordered import message, binding, stream_source
from data_pipeline.bgp.ordered_reader import adapt
from data_pipeline.bgp.record_types import MessageBoundary, Element
from data_pipeline.bgp.replay.route_replay import Replay, ReplayPlan
from data_pipeline.bgp.replay.quality_overlay import CanonicalReplay, message_from_boundary, element_for_replay, current_record
from data_pipeline.bgp.replay.snapshot_contract import encode, decode


def run_rows(rows,cut=1,endpoints=(('192.0.2.1',64497,'192.0.2.2',64496,0),)):
    b=binding([('s',rows)]);plan=ReplayPlan('rrc25','s',(),endpoints)
    core=CanonicalReplay(plan,b,('s',));output=[]
    items=list(adapt(stream_source(b,'s',rows,cut),b))
    for item in items:output.extend(core.apply(item))
    return core,output,items


def key(prefix='198.51.0.0/24',peer='192.0.2.1',afi=1,path_id=None,local='192.0.2.2'):
    return ('rrc25',peer,64497,local,64496 if not local.startswith('unmapped') else None,
            0 if not local.startswith('unmapped') else None,afi,1,prefix,path_id is not None,path_id)


def test_no_gap_full_replay_and_export_equivalence():
    rows=[message('s',0,elements=2,path_present=True,path_id=0),message('s',1,elements=1),
          message('s',2,direction='local',elements=1),message('s',3,kind='state_change'),message('s',4,elements=1)]
    rows[1][1][0]['action']='withdraw'
    core,actual,items=run_rows(rows)
    old=Replay(core.replay.plan);expected=[];current=None
    for item in items:
        if isinstance(item,MessageBoundary):
            if current is not None:expected.extend(old.consume(current))
            current=message_from_boundary(item)
        elif isinstance(item,Element):
            current.elements.append(element_for_replay(item.raw,current))
            current.paths.append({k:item.raw[k] for k in ('path_key','as_path_text','attributed_origin_asn')})
    if current is not None:expected.extend(old.consume(current))
    assert [row['raw'] for table,row in actual if table in ('changes','invalidations')]==expected
    for name in ('current','last_known','legacy_paths','legacy_by_prefix','legacy_origins','seen_vps','legacy_baseline_epoch','cursor'):
        assert getattr(core.replay,name)==getattr(old,name)
    assert decode(encode(list(core.export())))==list(core.export())


def test_gap_cross_family_slots_same_asn_peer_and_exact_recovery():
    rows=[message('s',0,elements=4),message('s',1,status='rejected'),
          message('s',2,elements=1,path_present=True,path_id=0),
          message('s',3,elements=1,path_present=True,path_id=1)]
    es=rows[0][1]
    es[1].update(afi=2,prefix='2001:db8::/32',path_id_present=True,path_id=0)
    es[2].update(afi=2,prefix='2001:db8::/32',path_id_present=True,path_id=1)
    es[3].update(peer_ip='192.0.2.3')
    for _,elements in rows[2:]:elements[0].update(afi=2,prefix='2001:db8::/32')
    rows[3][1][0]['action']='withdraw'
    core,out,_=run_rows(rows)
    assert core.replay.current[key()]['presence']=='unknown'
    assert core.replay.last_known[key()]['event_id']=='s:0:0'
    assert core.replay.current[key('2001:db8::/32',afi=2,path_id=0)]['presence']=='present'
    assert core.replay.current[key('2001:db8::/32',afi=2,path_id=1)]['presence']=='absent'
    assert core.replay.current[key('198.51.3.0/24',peer='192.0.2.3')]['presence']=='present'
    changes=[r['raw'] for t,r in out if t=='changes']
    assert changes[-2]['calculation_before']['presence']=='unknown'
    assert changes[-2]['last_known_before']['event_id']=='s:0:1'
    assert len([r for t,r in out if t=='scope_gap'])==1
    assert not [r for t,r in out if t=='invalidations']
    assert core.index.qualification(key('2001:db8::/32',afi=2,path_id=0),core.positions[key('2001:db8::/32',afi=2,path_id=0)])['continuity']=='partial'


def test_zero_hit_future_gap_local_and_repeated_gap():
    rows=[message('s',0,status='rejected'),message('s',1,elements=1),
          message('s',2,status='rejected',direction='local'),message('s',3,status='rejected')]
    core,out,_=run_rows(rows)
    changes=[r for t,r in out if t=='changes']
    assert changes[0]['raw']['calculation_before']['presence']=='unknown'
    assert changes[0]['raw']['last_known_before'] is None
    assert core.replay.current[key()]['presence']=='unknown'
    assert core.index.qualification(key('203.0.113.0/24'))['current']=='unknown'
    assert len([r for t,r in out if t=='scope_gap'])==3
    assert len([r for t,r in out if t=='qualification_change' and r['target']=='scope'])==3
    assert any(r['status']=='not_applicable' for t,r in out if t=='qualification_change')
    assert core.replay.legacy_paths[('198.51.0.0/24','64497')]=='{64497} 64496'


def test_unmapped_and_unknown_collector_gap():
    rows=[message('s',0,kind='rib',direction='unknown',endpoint=False,elements=1),message('s',1,status='rejected')]
    m,es=rows[0];m.update(mrt_type=13,mrt_subtype=2,local_message=None)
    es[0].update(mrt_type=13,mrt_subtype=2,local_message=None,action='rib_snapshot')
    core,_,_=run_rows(rows,endpoints=())
    k=key(local='unmapped_rib:s')
    assert core.replay.current[k]['presence']=='unknown'
    assert core.index.qualification(k)['overlap']=='possible_overlap'
    rows.append(message('s',2,status='rejected',direction='unknown',endpoint=False))
    core,_,_=run_rows(rows,endpoints=())
    assert core.index.qualification(key(peer='203.0.113.10'))['current']=='unknown'


@pytest.mark.parametrize('cut',[1,2,4,100])
def test_gap_batch_invariance_eor_and_no_unproved_rib_reset(cut):
    rows=[message('s',0,elements=1),message('s',1,status='rejected'),message('s',2)]
    rows[2][0]['eor']=[dict(afi=1,safi=1)]
    core,out,_=run_rows(rows,cut)
    assert core.replay.current[key()]['presence']=='unknown'
    assert out==run_rows(rows,100)[1]
    assert len(list(core.export()))>0
    with pytest.raises(ValueError):CanonicalReplay(core.replay.plan,core.binding,('s','unproven_snapshot'))


def test_private_m2_projection_public_reader(tmp_path):
    import os,hashlib,gzip,struct,subprocess,sys
    from pathlib import Path
    import psycopg2
    from tests.observations.test_observation_two_phase import fixture_manifest
    from tests.observations.test_observation_mrt import mrt, update
    from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.bgp.input.mrt_reader import source_identity
    from data_pipeline.bgp.replay.route_snapshot import produce_projection
    from data_pipeline.bgp.replay.snapshot_store import ProjectionReader
    from data_pipeline.bgp.replay.snapshot_contract import TABLES, ProjectionBinding
    dsn=os.environ.get('DOMEYE_M3B_TEST_DSN')
    if not dsn:pytest.skip('必须显式绑定自有 M3B 私有 PG')
    assert psycopg2.extensions.parse_dsn(dsn)['host'].startswith('/tmp/domeye-m3b-efb1-')
    inp=tmp_path/'input';inp.mkdir();manifest=fixture_manifest(inp)
    # 合法 A → 坏 ET → 合法 A → 再 Gap；另一来源零元素，覆盖必须仍落盘。
    bad=mrt(struct.pack('!I',900000)+update(attrs=b'\xf0\x23\x04\0\x04\x2f\x66')[12:],4,17)
    raws=(update()+bad+update()+bad,b'')
    for e,raw in zip(manifest['inputs'][1:],raws):
        p=Path(e['path']);p.write_bytes(gzip.compress(raw,mtime=0))
        e.update(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),size=p.stat().st_size)
        e['source_id']=source_identity('rrc25',e['origin_uri'],e['sha256'])
    manifest['update_sources']=[e['source_id'] for e in manifest['inputs'][1:]]
    snapshot={**manifest['inputs'][0],'role':'snapshot','origin_uri':'fixture://rrc25/independent-snapshot'}
    snapshot['source_id']=source_identity('rrc25',snapshot['origin_uri'],snapshot['sha256'])
    manifest['inputs'].insert(1,snapshot)
    reference=inp/'reference.json';reference.write_text('{"a":1,"a":2}')
    manifest['references']=[dict(path=str(reference),sha256=hashlib.sha256(reference.read_bytes()).hexdigest())]
    seal=produce_checkpointed(manifest,dsn,tmp_path/'m2',policy='isolate-payload/v1',min_free_bytes=0,batch_rows=2)
    ids=[manifest['baseline_source'],*manifest['update_sources']]
    def source(size=1):return ObservationReader(dsn,seal['run_id'],seal['snapshot'],ids,profile='observation',batch_rows=size)
    def m2_evidence():
        result=[]
        for cp in source().selection.checkpoints:
            for f in cp['files']:
                # M2 checkpoint 使用实际文件字典，保留每个文件摘要。
                result.append(f)
        return json.dumps(source().selection.seal,sort_keys=True),result
    before=m2_evidence()
    def hashes():
        return {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in (tmp_path/'m2').rglob('*.parquet')}
    before_files=hashes();assert before_files
    def retained_hashes():return {p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in before_files}
    a=produce_projection(source(1),tmp_path/'m3a',min_free_bytes=0,batch_rows=2)
    r=ProjectionReader(dsn,a);assert r.audit()['audit']=='complete'
    tables={t:list(r.scan(t)) for t in TABLES}
    descriptor=r.descriptor()
    assert descriptor['database']['system_identifier'] and descriptor['database']['database_oid']>0
    assert descriptor['state']=='complete' and set(descriptor['tables'])==set(TABLES)
    assert all(f['sha256'] and f['bytes']>0 for f in descriptor['files'])
    scan=r.scan('current_routes');drained=[]
    while True:
        try:drained.append(next(scan))
        except StopIteration as done:
            assert done.value['execution']=='complete';break
    assert drained==tables['current_routes']
    with r.locked() as checked:
        assert checked['audit']=='complete'
        contender=psycopg2.connect(dsn)
        try:
            for sql,rid in (("UPDATE m3_projection.runs SET state='failed' WHERE run_id=%s",a.run_id),
                            ("UPDATE observation_m2.runs SET state='failed' WHERE run_id=%s",seal['run_id']),
                            ("UPDATE observation_m2.checkpoints SET ordinal=ordinal WHERE run_id=%s",seal['run_id'])):
                with contender.cursor() as c:
                    c.execute("SET lock_timeout='100ms'")
                    with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute(sql,(rid,))
                contender.rollback()
        finally:contender.rollback();contender.close()
    with pytest.raises(ValueError):list(r.scan('current_routes',max_row_bytes=1))
    assert len(tables['scope_gap'])==2 and len(tables['source_coverage'])==3
    assert [s['source_rank'] for s in tables['source_coverage']]==[0,2,3]
    assert len(r.seal['plan']['input_binding']['sources'])==4
    assert len(tables['reference_binding'])==1 and tables['reference_binding'][0]['rows']>0
    assert tables['source_coverage'][-1]['raw']['elements']==0
    assert tables['source_coverage'][-1]['inherited_gap_ids']
    assert all(v['current']['presence']=='unknown' for v in tables['current_routes'])
    assert r.current(('rrc25','192.0.2.1',64497,'192.0.2.2',12654,0,1,1,'203.0.113.0/24',False,None))['qualification']['active_gap_ids']
    b=produce_projection(source(10),tmp_path/'m3b',min_free_bytes=0,batch_rows=7)
    assert {t:list(ProjectionReader(dsn,b).scan(t)) for t in TABLES}==tables
    # 新进程使用公开 Reader 扫描全部表，验证可逆 typed 正文，而非父进程对象共享。
    config=tmp_path/'reader.json';config.write_text(json.dumps(dict(dsn=dsn,binding=asdict(a))))
    target=tmp_path/'新进程全表.txt'
    code='''import json,sys
from pathlib import Path
from data_pipeline.bgp.replay.snapshot_store import ProjectionReader
from data_pipeline.bgp.replay.snapshot_contract import ProjectionBinding, TABLES, encode
c=json.loads(Path(sys.argv[1]).read_text());r=ProjectionReader(c['dsn'],ProjectionBinding(**c['binding']));r.audit()
Path(sys.argv[2]).write_text(encode({t:list(r.scan(t)) for t in TABLES}))
'''
    subprocess.run([sys.executable,'-c',code,str(config),str(target)],check=True)
    assert decode(target.read_text())==tables
    assert m2_evidence()==before and retained_hashes()==before_files
    # 失败投影不能读取：删掉一条 Gap 由完成校验检出，原 M2 不变。
    def corrupt(stage,writer):writer.db.execute(f'DELETE FROM lake.{writer.schema}.scope_gap WHERE seq=0')
    with pytest.raises(ValueError,match='覆盖计数|漏 Gap'):produce_projection(source(),tmp_path/'failed',min_free_bytes=0,batch_rows=1,hook=corrupt)
    pg=psycopg2.connect(dsn)
    try:
        with pg.cursor() as c:
            c.execute("SELECT run_id FROM m3_projection.runs WHERE state='failed'");failed=c.fetchall()
        assert failed
        with pytest.raises(ValueError):ProjectionReader(dsn,ProjectionBinding(failed[-1][0],0,'0'*64))
    finally:pg.close()
    with pytest.raises(ValueError):ProjectionReader(dsn,replace(a,seal_digest='0'*64))
    def wrong_counts(stage,writer):
        row=decode(writer.db.execute(f'SELECT payload FROM lake.{writer.schema}.source_coverage WHERE seq=0').fetchone()[0])
        row['raw']['messages']+=1
        writer.db.execute(f'UPDATE lake.{writer.schema}.source_coverage SET payload=? WHERE seq=0',[encode(row)])
    with pytest.raises(ValueError,match='计数'):
        produce_projection(source(),tmp_path/'wrong-count',min_free_bytes=0,batch_rows=1,hook=wrong_counts)
    early=source();original=early.stream
    def interrupted():
        inner=original()
        try:
            yield next(inner)
            raise ValueError('人工读取尾失败')
        finally:inner.close()
    early.stream=interrupted
    with pytest.raises(ValueError,match='尾失败'):
        produce_projection(early,tmp_path/'early',min_free_bytes=0,batch_rows=1)
    assert m2_evidence()==before and retained_hashes()==before_files
    revoked=ProjectionReader(dsn,b)
    pg=psycopg2.connect(dsn)
    try:
        with pg,pg.cursor() as c:c.execute("UPDATE m3_projection.runs SET state='failed' WHERE run_id=%s",(b.run_id,))
    finally:pg.close()
    for check in (revoked.audit,revoked.descriptor,lambda:list(revoked.scan('changes'))):
        with pytest.raises(ValueError,match='不可读'):check()
    # b 保持撤回，不恢复为 complete；第一健康 run 供后续兼容检查。
    (tmp_path/'M3B集成证据.json').write_text(json.dumps(dict(m2=seal['run_id'],input_seal=seal['digest'],
        first=asdict(a),second=asdict(b),table_counts={t:len(v) for t,v in tables.items()},
        m2_files_unchanged=True,m2_file_count=len(before_files),new_process_all_tables_equal=True,
        metrics=r.seal['metrics']),ensure_ascii=False,indent=2))


def test_public_scan_preserves_primary_and_reports_early_close_failure():
    from types import SimpleNamespace
    from data_pipeline.bgp.replay.snapshot_store import ProjectionReader
    from data_pipeline.bgp.replay.snapshot_contract import COLUMNS, ProjectionBinding
    primary=ValueError('人工扫描失败');cleanup=RuntimeError('人工关闭失败')
    class DB:
        closed=0
        def execute(self,*args):return self
        def fetch_record_batch(self,*args):
            payload={k:None for k in COLUMNS};payload.update(seq=0,payload=encode({'prefix':'192.0.2.0/24','paths':{},'origins':set()}))
            yield SimpleNamespace(to_pylist=lambda:[payload])
            raise primary
        def close(self):self.closed+=1;raise cleanup
    reader=object.__new__(ProjectionReader)
    reader.schema='m3_test';reader.binding=ProjectionBinding('test',1,'0'*64);reader.guard=lambda:None
    db=DB();reader._connect=lambda:db
    with pytest.raises(ValueError) as caught:list(reader.scan('legacy_prefixes'))
    assert caught.value is primary and primary.cleanup_errors==(cleanup,) and db.closed==1
    db=DB();reader._connect=lambda:db
    scan=reader.scan('legacy_prefixes');assert next(scan)=={'prefix':'192.0.2.0/24','paths':{},'origins':set()}
    with pytest.raises(RuntimeError) as caught:scan.close()
    assert caught.value is cleanup and db.closed==1


@pytest.fixture(scope='module')
def repair_input(tmp_path_factory):
    import os,hashlib
    from pathlib import Path
    import psycopg2
    from tests.observations.test_observation_two_phase import fixture_manifest
    from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    dsn=os.environ.get('DOMEYE_M3B_TEST_DSN')
    if not dsn:pytest.skip('必须显式绑定自有 M3B 私有 PG')
    assert psycopg2.extensions.parse_dsn(dsn)['host'].startswith('/tmp/domeye-m3b-efb1-')
    import uuid
    name='repair_'+uuid.uuid4().hex
    pg=psycopg2.connect(dsn);pg.autocommit=True
    try:
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    finally:pg.close()
    config=psycopg2.extensions.parse_dsn(dsn);config['dbname']=name
    dsn=psycopg2.extensions.make_dsn(**config)
    root=tmp_path_factory.mktemp('repair');inp=root/'input';inp.mkdir()
    manifest=fixture_manifest(inp)
    ref=inp/'reference.json';ref.write_text('{"one":1,"two":2}')
    manifest['references']=[dict(path=str(ref),sha256=hashlib.sha256(ref.read_bytes()).hexdigest())]
    seal=produce_checkpointed(manifest,dsn,root/'m2',min_free_bytes=0,batch_rows=2)
    ids=[manifest['baseline_source'],*manifest['update_sources']]
    def reader():return ObservationReader(dsn,seal['run_id'],seal['snapshot'],ids,profile='observation',batch_rows=1)
    files={f['path']:hashlib.sha256(Path(f['path']).read_bytes()).hexdigest() for cp in reader().selection.checkpoints for f in cp['files']}
    yield dsn,reader,root
    assert files=={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in files}
    assert reader().selection.seal==seal


@pytest.mark.parametrize('case',['inner_event','inner_scope','unknown_rule','legal_source_swap','legal_object','legal_nested_event','legal_reference','missing_field','wrong_enum','late_unknown_rule'])
def test_private_rejects_self_sealed_inconsistent_output(repair_input,monkeypatch,case):
    from data_pipeline.bgp.replay import route_snapshot as prod
    from data_pipeline.bgp.replay.snapshot_store import ProjectionWriter
    from data_pipeline.bgp.replay.route_replay import identity
    dsn,reader,root=repair_input
    sources=reader().sources
    assert len(sources)==3
    original=ProjectionWriter.append
    def broken(self,table,row):
        row=deepcopy(row)
        if case=='inner_event' and table=='changes':row['raw']['event_id']='unselected:999:999'
        if case=='inner_scope' and table=='current_routes':row['scope']=(*row['scope'][:8],'203.0.113.0/24',*row['scope'][9:])
        if case=='legal_source_swap' and table=='changes' and row['source_id'] in sources[1:]:
            old=row['source_id'];other=sources[2] if old==sources[1] else sources[1]
            row['source_id']=other;row['message_id']=other+':0';row['event_id']=other+':0:0'
            row['raw']['event_id']=row['event_id']
        if case=='legal_object' and table=='current_routes':
            row['scope']=(*row['scope'][:8],'198.51.100.0/24',*row['scope'][9:])
            row['object_key']=identity(row['scope'])
        if case=='legal_nested_event' and table=='changes':row['raw']['calculation_after']['event_id']=sources[2]+':0:0'
        if case=='legal_reference' and table=='reference_binding':
            row['rows_digest']='0'*64
            self.plan['references']=[deepcopy(row)]  # 声明和正文同错，不能只互相比摘要。
        if case=='missing_field' and table=='legacy_prefixes':row.pop('origins')
        if case=='wrong_enum' and table=='current_routes':row['qualification']['current']='complete'
        return original(self,table,row)
    if case=='unknown_rule':monkeypatch.setattr(prod,'RULE','unknown-replay/v999')
    elif case=='late_unknown_rule':
        finish=ProjectionWriter.finish
        def changed_finish(self,*args):
            self.plan['algorithm']='unknown-replay/v999'
            return finish(self,*args)
        monkeypatch.setattr(ProjectionWriter,'finish',changed_finish)
    else:monkeypatch.setattr(ProjectionWriter,'append',broken)
    with pytest.raises(ValueError):
        prod.produce_projection(reader(),root/case,min_free_bytes=0,batch_rows=2)
    if case!='unknown_rule':
        failure=json.loads((root/case/'failure.json').read_text())
        assert failure['execution']=='failed'
        import psycopg2
        pg=psycopg2.connect(dsn)
        try:
            with pg.cursor() as c:
                c.execute('SELECT state,seal FROM m3_projection.runs WHERE run_id=%s',(failure['run_id'],))
                assert c.fetchone()==('failed',None)
        finally:pg.close()


@pytest.mark.parametrize('field',['algorithm','mapping_rule','ordered_rule','gap_rule','profile','codec','tables',
                                  'input.profile','input.schema_version','input.interpretation_version','input.ordered_version'])
@pytest.mark.parametrize('bad',[None,'unknown/v999'])
def test_rules_reject_missing_and_unknown(field,bad):
    from data_pipeline.bgp.replay.snapshot_validation import validate_rules, RULES, INPUT_RULES, CODEC
    from data_pipeline.bgp.replay.snapshot_contract import TABLES, PROFILE
    plan=dict(**RULES,input_binding=dict(INPUT_RULES),input_manifest={'schema_version':'observation-run/v1'},
              selected_sources=[],references=[],code={'historic.py':'a'*64},baseline_endpoints=[],limitations=['cutover_assumed','source_order_declared','session_continuity_unknown'],resource_limits={})
    args=dict(profile=PROFILE,codec=CODEC,tables=TABLES)
    validate_rules(plan,**args)  # 历史 hash 是身份，语义规则兼容时不按当前 hash 一刀切。
    if field.startswith('input.'):
        key=field.split('.')[1]
        if bad is None:plan['input_binding'].pop(key)
        else:plan['input_binding'][key]=bad
    elif field in args:args[field]=bad
    elif bad is None:plan.pop(field)
    else:plan[field]=bad
    with pytest.raises(ValueError):validate_rules(plan,**args)


def test_writer_close_attempts_both_and_preserves_first():
    from data_pipeline.bgp.replay.snapshot_store import ProjectionWriter, closing
    first=ValueError('db 关闭失败');second=RuntimeError('pg 关闭失败');seen=[]
    class Resource:
        def __init__(self,name,error):self.name=name;self.error=error
        def close(self):seen.append(self.name);raise self.error
    writer=object.__new__(ProjectionWriter)
    writer.db=Resource('db',first);writer.pg=Resource('pg',second)
    primary=LookupError('原扫描失败')
    with pytest.raises(LookupError) as caught:
        with closing(writer):raise primary
    assert caught.value is primary and primary.cleanup_errors==(first,)
    assert first.cleanup_errors==(second,) and seen==['db','pg']
    writer.close();assert seen==['db','pg']


def test_ledger_checks_guard_for_each_row():
    from types import SimpleNamespace
    from data_pipeline.bgp.replay.snapshot_store import _ledger
    from data_pipeline.bgp.replay.snapshot_contract import COLUMNS
    calls=[];failure=RuntimeError('人工验证预算耗尽')
    def guard():
        calls.append(1)
        if len(calls)==3:raise failure
    class DB:
        def execute(self,*args):return self
        def fetch_record_batch(self,size):
            assert size<=128
            def rows():
                return [dict(zip(COLUMNS,(i,None,None,None,None,None,encode({'vp':'1'})))) for i in range(2)]
            yield SimpleNamespace(to_pylist=rows)
    with pytest.raises(RuntimeError) as caught:_ledger(DB(),'m3_test',1,guard)
    assert caught.value is failure and len(calls)==3


def test_reference_validation_budget_preserves_cleanup():
    from types import SimpleNamespace
    from data_pipeline.bgp.replay.snapshot_validation import reference_rows
    primary=ValueError('参考验证预算耗尽');cleanup=RuntimeError('参考关闭失败');closed=[]
    def batches(source):
        try:yield SimpleNamespace(to_pylist=lambda:[{'value':1}])
        finally:closed.append(source);raise cleanup
    reader=SimpleNamespace(selection=SimpleNamespace(checkpoints=[{'source_id':'reference'}]),reference_batches=batches)
    def guard():raise primary
    with pytest.raises(ValueError) as caught:list(reference_rows(reader,SimpleNamespace(ordered_source_ids=()),guard))
    assert caught.value is primary and primary.cleanup_errors==(cleanup,) and closed==['reference']
