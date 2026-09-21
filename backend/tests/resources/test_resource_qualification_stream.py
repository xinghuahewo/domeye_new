"""本地DuckDB关系接合；只隔离PG身份边界，运行实际Reader与尾回执控制。"""
from contextlib import contextmanager

import duckdb
import pytest

from data_pipeline.analysis.resources import publication as p, publication_validation as v
from data_pipeline.analysis.resources.publication_codec import typed, untyped, CODEC

REAL_RELATION=v.relation


@pytest.fixture
def case(tmp_path,monkeypatch):
    db=duckdb.connect()
    for name in ('sources','coverage','qualifications','metrics','normal_bands','topology_status'):
        db.execute('CREATE TABLE '+name+' ('+','.join('"'+n+'" '+t for n,t in v.ALL_TABLES[name])+')')
    db.execute("INSERT INTO sources(source_id,snapshot_time,purpose) VALUES ('s','2026-01-01T00:00:00+00:00','result')")
    db.execute("INSERT INTO coverage(source_id) VALUES ('s')")
    for i in range(5):
        db.execute("INSERT INTO metrics(source_id,dimension,bucket,ipv6_cidr_count) VALUES ('s','asn',?,?)",[str(i),i])
        for status in ('qualified','unknown','not_applicable'):
            db.execute("INSERT INTO qualifications(source_id,qualification_id,target,dimension,bucket,metric,status) VALUES ('s',?,'metrics','asn',?,'ipv6_cidr_count',?)",[status,str(i),status])
    db.execute("INSERT INTO metrics SELECT * FROM metrics WHERE bucket='2'")
    db.execute("INSERT INTO qualifications SELECT * FROM qualifications WHERE bucket='2'")
    @contextmanager
    def lake(rt):yield db
    monkeypatch.setattr(p,'_lake',lake)
    monkeypatch.setattr(v,'relation',lambda b,t:t)
    monkeypatch.setattr(p,'verify_current',lambda *a,**k:None)
    rt=p.Runtime('unused',(tmp_path,),tmp_path,None,(),fixture_only=True,max_rows=1,max_bytes=10000)
    req=dict(view='metrics',scope_typed=typed({'scope':'all'}),codec_version=CODEC,batch_rows=1,batch_bytes=10000)
    yield db,rt,req,dict(admission_id='fixture',owner_binding=typed({}))
    db.close()


def test_multibatch_duplicate_rows_qualifications_and_unknown(case):
    db,rt,req,a=case
    with p.open_reader(rt,a,req,guard=lambda:None) as session:
        rows=[r for batch in session for r in untyped(batch['rows_typed'])]
    assert [r['raw']['bucket'] for r in rows]==['0','1','2','2','3','4']
    assert len(rows[2]['qualification'])==6 and rows[2]==rows[3]
    assert all(r['main']['ipv6_cidr_count'] is None for r in rows)
    assert all({q['status'] for q in r['qualification']}=={'qualified','unknown','not_applicable'} for r in rows)
    assert session.receipt['rows']==6
    assert len(untyped(session.receipt['coverage_ref']))==1


@pytest.mark.parametrize('failure',['early','missing','tail','source'])
def test_early_missing_qualification_and_tail_have_no_receipt(case,monkeypatch,failure):
    db,rt,req,a=case
    if failure=='missing':db.execute("DELETE FROM qualifications WHERE bucket='4'")
    checks=[]
    def current(*a,**k):
        checks.append(1)
        if failure=='tail' and len(checks)==2:raise ValueError('tail')
    monkeypatch.setattr(p,'verify_current',current)
    session=None;steps=[]
    def guard():
        steps.append(1)
        if failure=='source' and len(steps)>5:raise ValueError('source guard')
    def run():
        nonlocal session
        with p.open_reader(rt,a,req,guard=guard) as session:
            if failure=='early':next(session)
            else:list(session)
    if failure=='early':run()
    else:
        with pytest.raises(ValueError):run()
    assert session.receipt is None
    assert db.execute('SELECT count(*) FROM metrics').fetchone()==(6,)


def test_flush_insert_error_keeps_primary_when_unregister_fails():
    primary=ValueError('insert');cleanup=RuntimeError('unregister')
    class DB:
        def execute(self,sql):
            if sql.startswith('INSERT'):raise primary
        def register(self,*a):pass
        def unregister(self,*a):raise cleanup
    from types import SimpleNamespace
    audit=v.ScienceRows(DB(),SimpleNamespace(memory_bytes=1024),lambda:None)
    audit.add('expected','metrics',{'x':1})
    with pytest.raises(ValueError) as caught:audit.flush()
    assert caught.value is primary and primary.cleanup_errors==(cleanup,)


@pytest.mark.parametrize('target',['normal_bands','topology_status'])
def test_other_view_keys_and_qualified_values(case,target):
    db,rt,req,a=case;req['view']=target
    if target=='normal_bands':
        db.execute("INSERT INTO normal_bands(source_id,dimension,bucket,metric,mean) VALUES ('s','country',NULL,'ipv6_cidr_count',2.5)")
    else:
        db.execute("INSERT INTO topology_status(source_id,country_cn,status,node_count) VALUES ('s',NULL,'available',4)")
    db.execute("INSERT INTO qualifications(source_id,target,dimension,bucket,metric,status) VALUES ('s',?,'country',NULL,'ipv6_cidr_count','qualified')",[target])
    with p.open_reader(rt,a,req,guard=lambda:None) as session:
        rows=[r for b in session for r in untyped(b['rows_typed'])]
    assert len(rows)==1 and len(rows[0]['qualification'])==1
    assert rows[0]['main']['mean' if target=='normal_bands' else 'node_count']==(2.5 if target=='normal_bands' else 4)
    assert session.receipt


@pytest.mark.parametrize('target',['metrics','normal_bands','topology_status'])
def test_real_ducklake_snapshot_aliases_preserve_values(case,tmp_path,monkeypatch,target):
    db,rt,req,a=case
    db.execute("LOAD ducklake")
    assert dict(db.execute('SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded').fetchall())['ducklake']=='3f1b372'
    db.execute("ATTACH 'ducklake:"+str(tmp_path/'catalog.ducklake')+"' AS lake (DATA_PATH '"+str(tmp_path/'data')+"')")
    db.execute('CREATE SCHEMA lake.resource_fixture')
    if target=='normal_bands':
        db.execute("INSERT INTO normal_bands(source_id,dimension,bucket,metric,mean) VALUES ('s','country',NULL,'ipv6_cidr_count',2.5)")
    elif target=='topology_status':
        db.execute("INSERT INTO topology_status(source_id,country_cn,status,node_count) VALUES ('s',NULL,'available',4)")
    if target!='metrics':
        db.execute("INSERT INTO qualifications(source_id,target,dimension,bucket,metric,status) VALUES ('s',?,'country',NULL,'ipv6_cidr_count','qualified')",[target])
    for table in ('sources','coverage','qualifications','metrics','normal_bands','topology_status'):
        db.execute('CREATE TABLE lake.resource_fixture.'+table+' AS SELECT * FROM '+table)
    snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
    # 新快照改变科学数据；Reader必须仍消费上面固定的snapshot。
    db.execute("DELETE FROM lake.resource_fixture."+target)
    monkeypatch.setattr(v,'relation',REAL_RELATION)
    a['owner_binding']=typed(dict(run_id='fixture',snapshot=snapshot));req['view']=target
    with p.open_reader(rt,a,req,guard=lambda:None) as session:
        rows=[r for batch in session for r in untyped(batch['rows_typed'])]
    assert session.receipt
    if target=='metrics':
        assert [r['raw']['bucket'] for r in rows]==['0','1','2','2','3','4']
        assert rows[2]==rows[3] and len(rows[2]['qualification'])==6
        assert all(r['main']['ipv6_cidr_count'] is None for r in rows)
    else:
        assert len(rows)==1 and rows[0]['main']['mean' if target=='normal_bands' else 'node_count']==(2.5 if target=='normal_bands' else 4)
        assert rows[0]['qualification'][0]['bucket'] is None
