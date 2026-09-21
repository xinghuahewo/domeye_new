"""真实私有人工M2准入/当前核验/单锁/流生命周期；不接他人库或真实输入。"""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import psycopg2
import pytest

from tests.observations.test_observation_checkpoint import dsn, fixture, run
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.archive.checkpoint import sha
from data_pipeline.bgp.archive import admission as p
from data_pipeline.bgp.archive.value_codec import CODEC, typed, untyped, digest


def setup_case(tmp_path,dsn):
    m=fixture(tmp_path/'input',True);s=run(m,dsn,tmp_path/'run',policy='isolate-payload/v1')
    logs=[];rt=p.Runtime(dsn,(tmp_path.resolve(),),tmp_path.resolve(),fixture_only=True,audit_sink=logs.append)
    binding=p.inspect_binding(rt,s['run_id'],s['snapshot'],[x['source_id'] for x in m['inputs']])
    return SimpleNamespace(m=m,seal=s,rt=rt,binding=binding,logs=logs)


def admissions(case):
    a=p.admit(case.rt,case.binding,guard=lambda:None)
    case.rt.dependency_admissions=(a,)
    b=p.reference_binding(case.rt,a,case.binding['reference_sources'][0]['source_id'])
    ref=p.admit(case.rt,b,guard=lambda:None)
    return a,ref


def request(a,view='messages',batch_rows=17,sources=None):
    b=untyped(a['owner_binding'])
    if sources is None:sources=b['ordered_source_ids'] if b['owner']=='m2' and view!='references' else ([b['source_id']] if b['owner']=='reference' else [x['source_id'] for x in b['reference_sources']])
    return dict(view=view,scope_typed=typed(dict(source_ids=sources)),codec_version=CODEC,batch_rows=batch_rows,batch_bytes=1024**2)


def consume(case,a,req):
    rows=[]
    with p.open_reader(case.rt,a,req,guard=lambda:None) as session:
        assert session.receipt is None
        for batch in session:
            assert batch['bytes']==len(batch['rows_typed'].encode())
            assert batch['rows']<=req['batch_rows'] and batch['bytes']<=req['batch_bytes']
            rows.extend(untyped(batch['rows_typed']))
        assert session.receipt is None
    assert session.receipt['execution']=='complete' and session.receipt['rows']==len(rows)
    return rows,session.receipt


def test_actual_admission_reuse_bounded_read_and_compatibility(tmp_path,dsn):
    c=setup_case(tmp_path,dsn)
    reader=ObservationReader(dsn,c.seal['run_id'],c.seal['snapshot'],c.binding['ordered_source_ids'],profile='observation')
    before=[sha(asdict(x)) for x in reader.stream()]
    files={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in tmp_path.rglob('*') if f.is_file()}
    a,ref=admissions(c)
    assert a['dependencies']==[] and ref['dependencies']==[a['admission_id']]
    assert c.binding['selected_sources'][0]['upstream_rank']==0
    assert c.binding['reference_sources'][0]['checkpoint_ordinal']==0
    assert c.seal['checkpoints'][1]['ordinal']==1  # 同一MRT source的原rank0并非CP1。
    assert untyped(ref['owner_binding'])['selected_sources']==[]
    c.logs.clear();assert p.admit(c.rt,c.binding,guard=lambda:None)==a
    assert p.admit(c.rt,untyped(ref['owner_binding']),guard=lambda:None)==ref
    assert not any(e['kind'] in ('entity_hash','admit_body_batch','parquet_row_group') for e in c.logs)
    c.logs.clear();p.verify_current(c.rt,a,guard=lambda:None);p.verify_current(c.rt,ref,guard=lambda:None)
    assert not any(e['kind'] in ('entity_hash','admit_body_batch','parquet_row_group') for e in c.logs)
    current=list(c.logs);c.logs.clear()
    for owner in (a,ref):
        for view in (tuple(p.OBS_TABLES) if owner['owner']=='m2' else ('references',)):
            one,r1=consume(c,owner,request(owner,view,1));many,r2=consume(c,owner,request(owner,view,17))
            assert one==many and r1['typed_digest']==r2['typed_digest']
    empty,receipt=consume(c,a,request(a,'messages',sources=[]))
    assert empty==[] and untyped(receipt['coverage_ref'])['business_absence']=='Unknown'
    assert before==[sha(asdict(x)) for x in reader.stream()]
    assert files=={path:hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in files}
    (tmp_path/'准入与成本.json').write_text(json.dumps(dict(m2=a,reference=ref,current_events=current,read_events=c.logs),ensure_ascii=False,indent=2))

@pytest.mark.parametrize('owner',['m2','reference'])
@pytest.mark.parametrize('fault',['self_signed','missing_anchor','wrong_rule','wrong_cp'])
def test_current_rejects_untrusted_or_revoked_binding(tmp_path,dsn,owner,fault):
    import copy
    from psycopg2.extras import Json
    c=setup_case(tmp_path,dsn);a,ref=admissions(c);chosen=copy.deepcopy(a if owner=='m2' else ref)
    with psycopg2.connect(dsn) as pg,pg.cursor() as cursor:
        if fault=='missing_anchor':
            t=p._own_target(chosen);cursor.execute(f'DELETE FROM {p.TABLES[t["namespace"]]} WHERE record_key=%s',(t['key'],))
        elif fault=='wrong_cp':
            cursor.execute("UPDATE observation_m2.checkpoints SET payload=jsonb_set(payload,'{digest}','\"changed\"') WHERE run_id=%s AND ordinal=1",(c.seal['run_id'],))
        else:
            if fault=='wrong_rule':chosen['validator']['rules_digest']='0'*64
            else:chosen['inventory_digest']='0'*64
            chosen['admission_id']=digest({k:v for k,v in chosen.items() if k!='admission_id'})
    with pytest.raises(ValueError):p.verify_current(c.rt,chosen,guard=lambda:None)


def test_entity_change_requires_new_record_and_bad_body_never_admitted(tmp_path,dsn):
    import os,time
    c=setup_case(tmp_path,dsn);a=p.admit(c.rt,c.binding,guard=lambda:None)
    path=Path(a['entities'][0]['path']);old=path.stat();os.utime(path,ns=(old.st_atime_ns,old.st_mtime_ns+1000000))
    with pytest.raises(ValueError,match='实体'):p.verify_current(c.rt,a,guard=lambda:None)
    new=p.admit(c.rt,c.binding,guard=lambda:None)
    assert new['admission_id']!=a['admission_id'] and p._own_target(new)['key']!=p._own_target(a)['key']
    with psycopg2.connect(dsn) as pg,pg.cursor() as cursor:
        cursor.execute('SELECT admission FROM observation_publication.m2_admissions WHERE record_key=%s',(p._own_target(a)['key'],));assert cursor.fetchone()[0]==a
    data=path.read_bytes();path.write_bytes(data+b'corrupt')
    with pytest.raises(ValueError):p.admit(c.rt,c.binding,guard=lambda:None)
    with psycopg2.connect(dsn) as pg,pg.cursor() as cursor:
        cursor.execute('SELECT count(*) FROM observation_publication.m2_admissions');assert cursor.fetchone()[0]==2


def test_actual_other_database_oid_rejected(tmp_path,dsn):
    from psycopg2 import sql
    import uuid
    c=setup_case(tmp_path,dsn);a=p.admit(c.rt,c.binding,guard=lambda:None)
    name='p1_clone_'+uuid.uuid4().hex
    admin=psycopg2.connect(dsn);admin.autocommit=True
    with admin.cursor() as cursor:
        cursor.execute('SELECT current_database()');original=cursor.fetchone()[0]
        cursor.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(sql.Identifier(name),sql.Identifier(original)))
    other=p.Runtime(dsn+' dbname='+name,(tmp_path.resolve(),),tmp_path.resolve(),fixture_only=True)
    try:
        with pytest.raises(ValueError,match='OID'):p.verify_current(other,a,guard=lambda:None)
    finally:
        with admin.cursor() as cursor:
            cursor.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s',(name,))
            cursor.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(name)))
        admin.close()


@pytest.mark.parametrize('namespace',['m2.run','m2.checkpoint','m2.admission','reference.admission'])
def test_each_explicit_target_holds_one_lock_and_exit_has_no_audit(tmp_path,dsn,namespace):
    c=setup_case(tmp_path,dsn);a,ref=admissions(c)
    target=next(t for t in ref['lock_targets'] if t['namespace']==namespace)
    def attempt():
        pg=psycopg2.connect(dsn)
        try:
            with pg.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout='100ms'")
                if namespace=='m2.run':cursor.execute('UPDATE observation_m2.runs SET state=state WHERE run_id=%s',(target['key'],))
                elif namespace=='m2.checkpoint':
                    run_id,ordinal=json.loads(target['key']);cursor.execute('UPDATE observation_m2.checkpoints SET payload=payload WHERE run_id=%s AND ordinal=%s',(run_id,ordinal))
                else:cursor.execute(f'UPDATE {p.TABLES[namespace]} SET state=state WHERE record_key=%s',(target['key'],))
        finally:pg.rollback();pg.close()
    c.logs.clear()
    with p.hold_lock(c.rt,ref,target,guard=lambda:None):
        assert sum('FOR SHARE' in e.get('sql','') for e in c.logs)==1
        with pytest.raises(psycopg2.errors.LockNotAvailable):attempt()
        marker=len(c.logs)
    assert len(c.logs)==marker
    attempt()


@pytest.mark.parametrize('owner',['m2','reference'])
@pytest.mark.parametrize('failure',['early_stop','revoke_during','revoke_tail','close_error'])
def test_reader_never_receipts_incomplete_or_revoked(tmp_path,dsn,monkeypatch,owner,failure):
    from data_pipeline.bgp.archive import validation as validation
    c=setup_case(tmp_path,dsn);a,ref=admissions(c);chosen=a if owner=='m2' else ref
    if failure=='close_error':
        original=validation.read_rows
        class CloseError:
            def __init__(self,*args,**kw):self.iterator=original(*args,**kw)
            def __iter__(self):return self
            def __next__(self):return next(self.iterator)
            def close(self):self.iterator.close();raise OSError('人工close失败')
        monkeypatch.setattr(validation,'read_rows',CloseError)
    session=None
    def run_read():
        nonlocal session
        with p.open_reader(c.rt,chosen,request(chosen,'messages' if owner=='m2' else 'references',1),guard=lambda:None) as session:
            next(session)
            if failure=='early_stop':return
            if failure=='revoke_tail':list(session)
            if failure.startswith('revoke'):
                with psycopg2.connect(dsn) as pg,pg.cursor() as cursor:
                    t=p._own_target(chosen);cursor.execute(f"UPDATE {p.TABLES[t['namespace']]} SET state='revoked' WHERE record_key=%s",(t['key'],))
            list(session)
    if failure=='early_stop':run_read()
    else:
        with pytest.raises((ValueError,OSError)):run_read()
    assert session is not None and session.receipt is None
    assert not list(tmp_path.glob('m2-p1-*'))


def test_finite_scope_and_reference_rank_cannot_be_forged(tmp_path,dsn):
    import copy
    c=setup_case(tmp_path,dsn);a,ref=admissions(c)
    wrong=copy.deepcopy(c.binding);wrong['selected_sources'][0]['upstream_rank']=1
    with pytest.raises(ValueError):p.admit(c.rt,wrong,guard=lambda:None)
    wrong=untyped(ref['owner_binding']);wrong['selected_sources']=[dict(source_id=wrong['source_id'],upstream_rank=0)]
    with pytest.raises(ValueError):p.admit(c.rt,wrong,guard=lambda:None)
    for change in (dict(view='arbitrary.sql'),dict(extra=1),dict(scope_typed=typed(dict(source_ids=['unbound'])))):
        req=request(a);req.update(change)
        with pytest.raises(ValueError):
            with p.open_reader(c.rt,a,req,guard=lambda:None):pass


def _replace_seal(dsn,seal):
    """仅人工反例：重签元数据并追加新固定选择，不更改业务正文；不能由P1 API调用。"""
    import uuid
    from psycopg2.extras import Json
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    previous=None
    for cp in seal['checkpoints']:
        cp['previous']=previous;cp['digest']=sha({k:v for k,v in cp.items() if k!='digest'});previous=cp['digest']
    old=seal['seal_id'];seal['seal_id']=uuid.uuid4().hex
    db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres');db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake')
    try:
        for cp in seal['checkpoints']:
            db.execute(f'INSERT INTO lake.{seal["schema"]}.seal_selection VALUES (?,?,?,?,?)',[seal['seal_id'],cp['source_id'],cp['attempt'],cp['ordinal'],cp['digest']])
        db.execute(f'INSERT INTO lake.{seal["schema"]}.path_owner SELECT ?,path_key,attempt FROM lake.{seal["schema"]}.path_owner WHERE seal_id=?',[seal['seal_id'],old])
        seal['snapshot']=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
    finally:db.close()
    seal['digest']=sha({k:v for k,v in seal.items() if k!='digest'})
    with psycopg2.connect(dsn) as pg,pg.cursor() as cursor:
        for cp in seal['checkpoints']:cursor.execute('UPDATE observation_m2.checkpoints SET payload=%s WHERE run_id=%s AND ordinal=%s',(Json(cp),seal['run_id'],cp['ordinal']))
        cursor.execute('UPDATE observation_m2.runs SET seal=%s WHERE run_id=%s',(Json(seal),seal['run_id']))


def test_late_row_file_cannot_bypass_cp_upper_bound(tmp_path,dsn):
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    c=setup_case(tmp_path,dsn);cp=c.seal['checkpoints'][2]
    db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres');db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake')
    try:
        before={r[0] for r in db.execute('SELECT data_file FROM ducklake_list_files(?,?,schema => ?)',['lake','messages',c.seal['schema']]).fetchall()}
        db.execute(f'INSERT INTO lake.{c.seal["schema"]}.messages SELECT * FROM lake.{c.seal["schema"]}.messages WHERE attempt=?',[cp['attempt']])
        late=[(path,size) for path,size in db.execute('SELECT data_file,data_file_size_bytes FROM ducklake_list_files(?,?,schema => ?)',['lake','messages',c.seal['schema']]).fetchall() if path not in before]
    finally:db.close()
    assert late
    # 恶意清单把上限外追加文件也装入CP；Selection仍排除它，文件路径却会读到额外行。
    cp['files'] += [dict(path=path,size=size,table='messages',sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest()) for path,size in late]
    _replace_seal(dsn,c.seal)
    b=p.inspect_binding(c.rt,c.seal['run_id'],c.seal['snapshot'],c.binding['ordered_source_ids'])
    with pytest.raises(ValueError,match='文件清单与实际固定选择正文不同'):p.admit(c.rt,b,guard=lambda:None)


@pytest.mark.parametrize('mode',['reverse_rows','mixed_attempt'])
def test_actual_parquet_order_and_attempt_mapping(tmp_path,dsn,monkeypatch,mode):
    import pyarrow as pa
    import pyarrow.parquet as pq
    m=fixture(tmp_path/'input',True)
    # 一文件多消息，才能真实调整Parquet内行序；原M2正常生产不会混合attempt。
    from data_pipeline.bgp.archive.checkpoint import produce_checkpointed, AttemptStore
    if mode=='reverse_rows':
        original=AttemptStore.flush
        def reverse_flush(self):
            self.pending['messages'].reverse()
            return original(self)
        monkeypatch.setattr(AttemptStore,'flush',reverse_flush)
    seal=produce_checkpointed(m,dsn,tmp_path/'run',batch_rows=1000,min_free_bytes=0,policy='isolate-payload/v1')
    rt=p.Runtime(dsn,(tmp_path.resolve(),),tmp_path.resolve(),fixture_only=True)
    cp=seal['checkpoints'][2];f=next(x for x in cp['files'] if x['table']=='messages')
    table=pq.read_table(f['path']);rows=table.to_pylist();assert len(rows)>1
    if mode=='reverse_rows':assert [r['record'] for r in rows]==sorted([r['record'] for r in rows],reverse=True)
    else:
        rows.append({**rows[0],'attempt':'foreign_attempt'})
        pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),f['path'])
        f.update(size=Path(f['path']).stat().st_size,sha256=hashlib.sha256(Path(f['path']).read_bytes()).hexdigest())
        _replace_seal(dsn,seal)
    b=p.inspect_binding(rt,seal['run_id'],seal['snapshot'],[x['source_id'] for x in m['inputs']])
    if mode=='mixed_attempt':
        import duckdb
        with pytest.raises((ValueError,duckdb.InvalidInputException)) as failure:p.admit(rt,b,guard=lambda:None)
        assert 'footer' in str(failure.value) or 'attempt越界' in str(failure.value)
        (tmp_path/'拒绝边界.txt').write_text(str(failure.value))
    else:
        a=p.admit(rt,b,guard=lambda:None);c=SimpleNamespace(rt=rt)
        result,_=consume(c,a,request(a,'messages',1))
        ranks={x['source_id']:x['upstream_rank'] for x in b['selected_sources']}
        positions=[(ranks[r['source_id']],r['record']) for r in result]
        assert positions==sorted(positions) and len(positions)==len(set(positions))



def test_selected_mrt_subset_keeps_full_input_binding_and_rank(tmp_path,dsn):
    c=setup_case(tmp_path,dsn)
    chosen=c.binding['ordered_source_ids'][-1:]
    b=p.inspect_binding(c.rt,c.seal['run_id'],c.seal['snapshot'],chosen)
    assert len(json.loads(b['input_binding'])['sources'])==3
    assert b['selected_sources']==[dict(source_id=chosen[0],upstream_rank=2,calculation_role='update')]
    a=p.admit(c.rt,b,guard=lambda:None)
    rows,_=consume(c,a,request(a))
    assert rows and {r['source_id'] for r in rows}==set(chosen)


def test_native_parquet_mixed_attempt_rejected_by_file_reader(tmp_path,dsn):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from data_pipeline.bgp.archive import validation as v
    from data_pipeline.bgp.archive.store import TYPES
    columns=p.OBS_TABLES['references']
    schema=pa.schema([('attempt',pa.string()),*[(n,TYPES[t]) for n,t in columns]])
    path=tmp_path/'mixed.parquet'
    pq.write_table(pa.Table.from_pylist([dict(attempt=attempt,**{n:None for n,_ in columns}) for attempt in ('owned','foreign')],schema=schema),path)
    rt=p.Runtime(dsn,(tmp_path.resolve(),),tmp_path.resolve(),fixture_only=True)
    cp=dict(attempt='owned',files=[dict(path=str(path.resolve()),table='references')])
    with v._stage(rt,lambda:None) as (db,root):
        with pytest.raises(ValueError,match='attempt越界'):v._load(rt,db,root,cp,'references',17,lambda:None)


def test_actual_changed_code_requires_new_admission_key(tmp_path,dsn):
    import shutil,subprocess,sys
    c=setup_case(tmp_path,dsn);a=p.admit(c.rt,c.binding,guard=lambda:None)
    clone=tmp_path/'code';shutil.copytree(p.ROOT/'backend/data_pipeline',clone/'backend/data_pipeline',ignore=shutil.ignore_patterns('__pycache__'))
    changed=clone/'backend/data_pipeline/bgp/archive/admission.py';changed.write_text(changed.read_text()+'\n# 人工新校验代码版本\n')
    subprocess.run(['git','init','-q',str(clone)],check=True)
    subprocess.run(['git','-C',str(clone),'add','.'],check=True)
    subprocess.run(['git','-C',str(clone),'-c','user.name=P1 Fixture','-c','user.email=fixture@example.invalid','commit','-qm','人工代码版本'],check=True)
    config=tmp_path/'new-code-request.json';result=tmp_path/'new-code-result.json'
    config.write_text(json.dumps(dict(dsn=dsn,root=str(tmp_path.resolve()),admission=a,binding=c.binding,result=str(result))))
    script='''import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from data_pipeline.bgp.archive import admission as p
c=json.loads(Path(sys.argv[2]).read_text());r=p.Runtime(c['dsn'],(c['root'],),Path(c['root']),fixture_only=True)
try:p.verify_current(r,c['admission'],guard=lambda:None)
except ValueError as e:assert '规则' in str(e)
else:raise AssertionError('旧规则未拒绝')
a=p.admit(r,c['binding'],guard=lambda:None)
assert a['admission_id']!=c['admission']['admission_id']
assert p._own_target(a)['key']!=p._own_target(c['admission'])['key']
Path(c['result']).write_text(json.dumps(a))
'''
    done=subprocess.run([sys.executable,'-I','-B','-c',script,str(clone/'backend'),str(config)],capture_output=True,text=True)
    (tmp_path/'新代码日志.txt').write_text(done.stdout+done.stderr)
    assert done.returncode==0,done.stderr
    new=json.loads(result.read_text());assert new['validator']['code_sha256']!=a['validator']['code_sha256']
    with psycopg2.connect(dsn) as pg,pg.cursor() as cursor:
        cursor.execute('SELECT admission FROM observation_publication.m2_admissions WHERE record_key=%s',(p._own_target(a)['key'],));assert cursor.fetchone()[0]==a


def test_failed_initial_admission_has_no_accepted_record(tmp_path,dsn):
    c=setup_case(tmp_path,dsn)
    Path(c.m['inputs'][1]['path']).write_bytes(b'changed original bytes')
    with pytest.raises(ValueError,match='SHA'):p.admit(c.rt,c.binding,guard=lambda:None)
    with psycopg2.connect(dsn) as pg,pg.cursor() as cursor:
        cursor.execute('SELECT count(*) FROM observation_publication.m2_admissions');assert cursor.fetchone()[0]==0


def test_read_primary_error_survives_close_error(tmp_path,dsn,monkeypatch):
    from data_pipeline.bgp.archive import validation as v
    c=setup_case(tmp_path,dsn);a=p.admit(c.rt,c.binding,guard=lambda:None)
    class Fault:
        def __iter__(self):return self
        def __next__(self):raise ValueError('原始读取错误')
        def close(self):raise OSError('清理错误')
    monkeypatch.setattr(v,'read_rows',lambda *args,**kw:Fault())
    with pytest.raises(ValueError,match='原始读取错误') as error:
        with p.open_reader(c.rt,a,request(a),guard=lambda:None) as session:list(session)
    assert session.receipt is None and isinstance(error.value.cleanup_errors[0],OSError)


def test_single_row_budget_failure_has_no_receipt(tmp_path,dsn):
    c=setup_case(tmp_path,dsn);a=p.admit(c.rt,c.binding,guard=lambda:None);req=request(a);req['batch_bytes']=1
    with pytest.raises(ValueError,match='单行'):
        with p.open_reader(c.rt,a,req,guard=lambda:None) as session:list(session)
    assert session.receipt is None and not list(tmp_path.glob('m2-p1-*'))
