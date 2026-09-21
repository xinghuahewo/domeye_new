"""独立审阅五项反例的修复回归；仅自有人工PG。"""
import pytest
import json,gzip,struct,hashlib,resource,sys,gc,time
from pathlib import Path
import psycopg2
from tests.observations.test_observation_checkpoint import dsn, fixture, run
from tests.observations.test_observation_mrt import update, mrt
from data_pipeline.bgp.archive.selection import Selection
from data_pipeline.bgp.input.mrt_reader import source_identity

def test_sealed_checkpoint_drift_regression(tmp_path,dsn):
    m=fixture(tmp_path/'input');s=run(m,dsn,tmp_path/'run')
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute('DELETE FROM observation_m2.checkpoints WHERE run_id=%s AND ordinal=1',(s['run_id'],))
    with pytest.raises(ValueError,match='checkpoint'):
        run(m,dsn,tmp_path/'run')

def test_bad_et_precision_regression(tmp_path,dsn):
    m=fixture(tmp_path/'input')
    for i,usec in [(1,900000),(2,500000)]:
        e=m['inputs'][i];p=Path(e['path'])
        u=update(attrs=b'\xf0\x23\x04\0\x04\x2f\x66') if i==1 else update()
        raw=mrt(struct.pack('!I',usec)+u[12:],4,17)
        p.write_bytes(gzip.compress(raw,mtime=0));e['sha256']=hashlib.sha256(p.read_bytes()).hexdigest();e['size']=p.stat().st_size;e['source_id']=source_identity('rrc25',e['origin_uri'],e['sha256'])
    m['update_sources']=[e['source_id'] for e in m['inputs'][1:]]
    s=run(m,dsn,tmp_path/'run',policy='isolate-payload/v1');b=Selection(dsn,s['run_id'],s['snapshot']);d=b.connect()
    msgs=d.execute('SELECT interpretation FROM '+b.table('messages',m['inputs'][1]['source_id'])).fetchall()
    q=d.execute('SELECT code FROM '+b.table('quality',m['inputs'][2]['source_id'])).fetchall();d.close()
    cp=next(c for c in s['checkpoints'] if c['source_id']==m['inputs'][1]['source_id'])
    evidence={'prior_update_last':cp['context']['prior_update_last'],'interpretation':json.loads(msgs[0][0]),'next_quality':q}
    (tmp_path/'反例.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2))
    assert cp['context']['prior_update_last']==[100,900000]
    assert '900000' in msgs[0][0] and ('cross_file_time_overlap',) in q

def test_peak_scope_regression(tmp_path):
    from data_pipeline.common.run_metrics import Metrics
    block=bytearray(200*1024**2);del block;gc.collect()
    before=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    metrics=Metrics(tmp_path/'samples')
    with metrics.measure('tiny'):time.sleep(.25)
    r=metrics.receipt();metrics.close()
    (tmp_path/'反例.json').write_text(json.dumps({'historical_ru_maxrss':before,'receipt':r},ensure_ascii=False,indent=2))
    assert r['process_lifetime_peak_rss_bytes']>=before*(1 if sys.platform=='darwin' else 1024)
    assert r['peak_scope']=='whole_process_lifetime'
    import os
    assert r['pid']==os.getpid() and 'invocation_peak_rss_bytes' not in r

def test_internal_helpers_qualification_bypass(tmp_path,dsn):
    from types import SimpleNamespace
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    from data_pipeline.bgp.replay.archive_input import build_mapping, messages
    m=fixture(tmp_path/'input');s=run(m,dsn,tmp_path/'run')
    r=ObservationReader(dsn,s['run_id'],s['snapshot'],[e['source_id'] for e in m['inputs']],profile='observation')
    d=connect_duckdb();d.execute('LOAD ducklake');d.execute('LOAD postgres');d.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
    saved=[];sink=SimpleNamespace(dsn=dsn,db=d,schema=r.schema,root=tmp_path,memory_limit='1GB',flush=lambda:None,append=lambda *v:saved.append(v))
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:c.execute("UPDATE observation_m2.runs SET state='failed' WHERE run_id=%s",(s['run_id'],))
    try:
        with pytest.raises(ValueError):build_mapping(sink,m['baseline_source'],m['update_sources'],reader=r)
        with pytest.raises(ValueError):list(messages(sink,m['baseline_source'],m['update_sources'],s['snapshot'],reader=r))
        assert not saved
    finally:d.close()

def test_rowid_ceiling_different_snapshot_regression(tmp_path,dsn,monkeypatch):
    from data_pipeline.bgp.archive.checkpoint import AttemptStore
    original=AttemptStore.checkpoint_data;injected=[];late_files=set()
    class Proxy:
        def __init__(self,db,store):self.db=db;self.store=store
        def __getattr__(self,key):return getattr(self.db,key)
        def execute(self,query,parameters=None):
            if not injected and 'coalesce(max(rowid),-1)' in query and '."messages"' in query:
                table='lake.'+self.store.schema+'.messages'
                n=self.db.execute('SELECT count(*) FROM '+table+' WHERE attempt=?',[self.store.attempt]).fetchone()[0]
                if n:
                    from data_pipeline.bgp.archive.store import connect_duckdb, literal
                    writer=connect_duckdb()
                    try:
                        writer.execute('LOAD ducklake');writer.execute('LOAD postgres')
                        writer.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake')
                        before={r[0] for r in writer.execute('SELECT data_file FROM ducklake_list_files(?, ?, schema => ?)',['lake','messages',self.store.schema]).fetchall()}
                        writer.execute('INSERT INTO '+table+' SELECT * FROM '+table+' WHERE attempt=?',[self.store.attempt])
                        late_files.update({r[0] for r in writer.execute('SELECT data_file FROM ducklake_list_files(?, ?, schema => ?)',['lake','messages',self.store.schema]).fetchall()}-before)
                        injected.append(n)
                    finally:writer.close()
            return self.db.execute(query,parameters) if parameters is not None else self.db.execute(query)
    def audit(self,expected):
        db=self.db;self.db=Proxy(db,self)
        try:return original(self,expected)
        finally:self.db=db
    monkeypatch.setattr(AttemptStore,'checkpoint_data',audit)
    m=fixture(tmp_path/'input');s=run(m,dsn,tmp_path/'run');b=Selection(dsn,s['run_id'],s['snapshot']);d=b.connect()
    actual=d.execute('SELECT count(*) FROM '+b.table('messages')).fetchone()[0];d.close()
    expected=sum(c['tables']['messages']['count'] for c in s['checkpoints'])
    (tmp_path/'反例.json').write_text(json.dumps({'injected_rows':injected,'checkpoint_messages':expected,'selected_messages':actual,'qualification':s['qualification']},ensure_ascii=False,indent=2))
    assert injected and actual==expected
    assert late_files and late_files.isdisjoint({f['path'] for cp in s['checkpoints'] for f in cp['files']})

@pytest.mark.parametrize('moment',['during_read','tail'])
def test_replay_revocation_during_consumption(tmp_path,dsn,moment):
    from types import SimpleNamespace
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.bgp.replay.archive_input import messages
    m=fixture(tmp_path/'input');s=run(m,dsn,tmp_path/'run')
    r=ObservationReader(dsn,s['run_id'],s['snapshot'],[e['source_id'] for e in m['inputs']],profile='observation')
    sink=SimpleNamespace(dsn=dsn,root=tmp_path,memory_limit='1GB')
    stream=messages(sink,m['baseline_source'],m['update_sources'],s['snapshot'],reader=r)
    n=1 if moment=='during_read' else sum(cp['counts']['messages'] for cp in s['checkpoints'])
    for _ in range(n):next(stream)
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE observation_m2.runs SET state='failed' WHERE run_id=%s",(s['run_id'],))
    with pytest.raises(ValueError):list(stream)


def test_mapping_revocation_at_read_end_has_no_output(tmp_path,dsn,monkeypatch):
    from types import SimpleNamespace
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.bgp.replay.archive_input import build_mapping
    m=fixture(tmp_path/'input');s=run(m,dsn,tmp_path/'run')
    r=ObservationReader(dsn,s['run_id'],s['snapshot'],[e['source_id'] for e in m['inputs']],profile='observation')
    original=r.connect
    class Connection:
        def __init__(self):self.db=original();self.query=''
        def __getattr__(self,key):return getattr(self.db,key)
        def execute(self,query,*args):self.query=query;self.db.execute(query,*args);return self
        def fetchone(self):return self.db.fetchone()
        def fetchall(self):
            rows=self.db.fetchall()
            if 'table_record' in self.query:
                with psycopg2.connect(dsn) as pg,pg.cursor() as c:
                    c.execute("UPDATE observation_m2.runs SET state='failed' WHERE run_id=%s",(s['run_id'],))
            return rows
    monkeypatch.setattr(r,'connect',Connection)
    saved=[];sink=SimpleNamespace(dsn=dsn,flush=lambda:None,append=lambda *x:saved.append(x))
    with pytest.raises(ValueError):build_mapping(sink,m['baseline_source'],m['update_sources'],reader=r)
    assert saved==[]


def test_et_rejection_in_file_and_checkpoint_restart(tmp_path,dsn):
    import os,signal
    from tests.observations.test_observation_checkpoint import worker, wait_marker
    m=fixture(tmp_path/'input')
    bad=mrt(struct.pack('!I',900000)+update(attrs=b'\xf0\x23\x04\0\x04\x2f\x66')[12:],4,17)
    good=mrt(struct.pack('!I',500000)+update()[12:],4,17)
    # 第一文件内回退，末记录仍以可信ET头留下900000；第二文件再次回退。
    for i,raw in [(1,bad+good+bad),(2,good)]:
        e=m['inputs'][i];p=Path(e['path']);p.write_bytes(gzip.compress(raw,mtime=0))
        e.update(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),size=p.stat().st_size)
        e['source_id']=source_identity('rrc25',e['origin_uri'],e['sha256'])
    m['update_sources']=[e['source_id'] for e in m['inputs'][1:]]
    config=dict(manifest=m,dsn=dsn,output=str(tmp_path/'run'),policy='isolate-payload/v1',stop='after_checkpoint',ordinal=2,marker=str(tmp_path/'marker'))
    p=worker(config,tmp_path/'config.json');wait_marker(p,tmp_path/'marker')
    os.kill(p.pid,signal.SIGKILL);assert p.wait()!=0
    config.pop('stop');p=worker(config,tmp_path/'config.json');assert p.wait(timeout=30)==0,(tmp_path/'config.log').read_text()
    rid=json.loads((tmp_path/'run/run.json').read_text())['run_id']
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('SELECT seal FROM observation_m2.runs WHERE run_id=%s',(rid,));s=c.fetchone()[0]
    b=Selection(dsn,rid,s['snapshot']);d=b.connect()
    try:
        assert ('timestamp_regression',) in d.execute('SELECT code FROM '+b.table('quality',m['inputs'][1]['source_id'])).fetchall()
        assert ('cross_file_time_overlap',) in d.execute('SELECT code FROM '+b.table('quality',m['inputs'][2]['source_id'])).fetchall()
        rejected=d.execute('SELECT microsecond,interpretation FROM '+b.table('messages')+" WHERE reason IS NOT NULL").fetchall()
        assert len(rejected)==2 and all(us is None and json.loads(meta)['header']['microsecond']==900000 for us,meta in rejected)
    finally:d.close()
    events=[json.loads(line) for line in (tmp_path/'run/events.jsonl').read_text().splitlines()]
    assert [e['ordinal'] for e in events if e['label']=='parse_start'].count(2)==1

@pytest.mark.parametrize('drift',['seal_digest','physical_selection'])
def test_sealed_resume_checks_seal_and_physical_selection(tmp_path,dsn,drift):
    from psycopg2.extras import Json
    from data_pipeline.bgp.archive.checkpoint import sha
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    m=fixture(tmp_path/'input');s=run(m,dsn,tmp_path/'run')
    if drift=='seal_digest':s['digest']='broken'
    else:
        d=connect_duckdb()
        try:
            d.execute('LOAD ducklake');d.execute('LOAD postgres')
            d.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake')
            d.execute(f'DELETE FROM lake.{s["schema"]}.seal_selection WHERE ordinal=1')
            s['snapshot']=d.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        finally:d.close()
        s['digest']=sha({k:v for k,v in s.items() if k!='digest'})
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('UPDATE observation_m2.runs SET seal=%s WHERE run_id=%s',(Json(s),s['run_id']))
    with pytest.raises(ValueError,match='摘要|物理封存'):run(m,dsn,tmp_path/'run')
