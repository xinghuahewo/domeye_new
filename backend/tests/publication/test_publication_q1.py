"""本任务私有PG：正式人工制品→两catalog→原子业务发布，非手填完成状态。"""
from collections import Counter
from dataclasses import replace
import json
import os
from pathlib import Path
import uuid
import psycopg2
import pytest
from data_pipeline.results import Publication, PROFILE, Token
from data_pipeline.results.manifest_io import Limits, encode, digest
from data_pipeline.analysis.resources.store import scan_resource
from data_pipeline.analysis.features.store import read_table
from tests.migration.test_migration_integration import test_resource_feature_share_observations_and_references as generate


@pytest.fixture(scope='module')
def produced(tmp_path_factory):
    base=os.environ.get('DOMEYE_Q1_TEST_DSN')
    if not base: pytest.skip('必须显式本任务私有PG')
    name='q1_fixture_'+uuid.uuid4().hex
    pg=psycopg2.connect(base);pg.autocommit=True
    with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    pg.close();dsn=base+' dbname='+name
    root=tmp_path_factory.mktemp('q1-formal')
    generate(root,dsn)
    (root/'q1-dsn.json').write_text(json.dumps({'dsn':dsn}));(root/'q1-dsn.json').chmod(0o600)
    reports=json.loads((root/'three-module-evidence.json').read_text())
    return dsn,root,reports


@pytest.fixture
def pub(produced):
    dsn,root,reports=produced
    # 同一私有实例不同测试共享固定制品；仅清本片自己的副本和head。
    p=Publication(dsn,root,limits=Limits(batch_rows=2,batch_bytes=2048));p.initialize()
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('TRUNCATE publication_q1.head,publication_q1.rows,publication_q1.builds')
    return p


def prepare(p,later=False):
    return p.prepare(p.root/('resource-later' if later else 'resource-first')/'execution.json',
                     p.root/'feature/output/execution.json')


def pages(p,t,kind,mode=''):
    first=p.query(t,kind,mode=mode,page_size=2)
    return [r for page in range(1,(first['total']+1)//2+1)
            for r in p.query(t,kind,mode=mode,page_size=2,page=page)['items']]


def test_formal_two_catalog_roundtrip_and_stable_pagination(pub,produced):
    _,root,reports=produced;t=prepare(pub)
    with pytest.raises(ValueError,match='候选'):pub.query(t,'resource')
    pub.publish(t,expected_generation=0)
    assert pub.discover()==(t,1)
    resource=pages(pub,t,'resource')
    expected=[r for b in scan_resource(pub.dsn,reports['resource_first']['run_id'],'metrics') for r in b.to_pylist()]
    assert Counter(encode(r['value']) for r in resource)==Counter(encode(r) for r in expected)
    assert all((r['value']['ipv4_prefix_count'],r['value']['ipv4_address_count'])==(2,512)
               for r in resource if r['value']['bucket']=='global')
    for mode,counts in [('ordinary',[(2,1),(0,0)]),('ir',[(1,1),(0,0)])]:
        rows=pages(pub,t,'feature',mode)
        collect=[r['value'] for r in rows if r['value']['scope']=='collect']
        assert [(r['announ_num'],r['withdraw_num']) for r in collect]==counts
        original=[r for b in read_table(pub.dsn,reports['feature']['run_id'],reports['feature']['snapshot'],'windows') for r in b.to_pylist() if r['mode']==mode]
        assert Counter(encode(r['value']) for r in rows)==Counter(encode(r) for r in original)
        assert all(r['calculation']['kind']=='calculated' and r['calculation']['rule'] for r in rows)
        assert all(r['value']['resource_status']=='unknown' for r in rows)
        if mode=='ir':assert any(r['value']['row_presence']=='ir_asn_write_disabled' for r in rows)
        assert rows==pages(pub,t,'feature',mode)
    a=pub.query(t,'resource')['source'];b=pub.query(t,'feature',mode='ordinary')['source']
    assert a['catalog_ref']['metadata_schema']=='public'
    assert b['catalog_ref']['metadata_schema']=='fl_'+b['run_id']
    assert a['snapshot']!=b['snapshot']
    (root/'q1-positive-evidence.json').write_text(encode({'token':t.__dict__,'resource_source':a,'feature_source':b,'resource_rows':resource}))


def test_new_head_preserves_old_token_and_rejects_mixed_build(pub):
    a=prepare(pub);pub.publish(a,expected_generation=0);old=pages(pub,a,'resource')
    b=prepare(pub,True);pub.publish(b,expected_generation=1)
    assert pub.discover()==(b,2) and pages(pub,a,'resource')==old
    assert pub.query(a,'resource')['source']['run_id']!=pub.query(b,'resource')['source']['run_id']
    with pytest.raises(ValueError):pub.query(replace(a,build_id=b.build_id),'resource')
    with pytest.raises(ValueError,match='不得重新选择'):pub.publish(a,expected_generation=2)
    assert pub.discover()==(b,2)


@pytest.mark.parametrize('change',[{'intent':'full_business'},{'data_kind':'real'},{'required':['resource.metrics']}])
def test_profile_cannot_shrink_or_become_real(pub,change):
    with pytest.raises(ValueError,match='profile'):pub.prepare('unused','unused',profile={**PROFILE,**change})


def test_full_selector_rejected(pub):
    t=prepare(pub)
    with pytest.raises(ValueError,match='selector'):pub.publish(t,expected_generation=0,selector='full')


def test_generation_conflict_keeps_old_head(pub):
    a=prepare(pub);pub.publish(a,expected_generation=0)
    b=prepare(pub,True)
    with pytest.raises(ValueError,match='generation'):pub.publish(b,expected_generation=0)
    assert pub.discover()==(a,1)
    with pytest.raises(ValueError,match='候选'):pub.query(b,'resource')


def test_commit_qualification_revoked(pub,produced):
    a=prepare(pub);pub.publish(a,expected_generation=0);b=prepare(pub,True)
    run=produced[2]['feature']['run_id']
    try:
        with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:c.execute("UPDATE feature.runs SET state='failed' WHERE run_id=%s",(run,))
        with pytest.raises(ValueError,match='资格'):pub.publish(b,expected_generation=1)
        assert pub.discover()==(a,1)
    finally:
        with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:c.execute("UPDATE feature.runs SET state='complete' WHERE run_id=%s",(run,))


@pytest.mark.parametrize('point',['component_written','files_sealed','manifest_sealed','before_visible'])
def test_failure_is_never_visible(pub,monkeypatch,point):
    a=prepare(pub);pub.publish(a,expected_generation=0)
    def fail(name):
        if name==point:raise OSError('fixture故障 '+point)
    monkeypatch.setattr(pub,'_checkpoint',fail)
    with pytest.raises(OSError):
        b=prepare(pub,True);pub.publish(b,expected_generation=1)
    assert pub.discover()==(a,1)
    with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:
        c.execute("SELECT build_id,publication_id FROM publication_q1.builds WHERE state!='published'")
        candidates=c.fetchall();assert candidates
    for build,p in candidates:
        with pytest.raises(ValueError,match='候选'):pub.query(Token(p or 'pending',build,digest(PROFILE)),'resource')


@pytest.mark.parametrize('fault',['delete','value'])
def test_pg_damage_detected_without_other_build_fallback(pub,fault):
    t=prepare(pub);pub.publish(t,expected_generation=0)
    with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:
        if fault=='delete':c.execute("DELETE FROM publication_q1.rows WHERE build_id=%s AND kind='resource' AND ordinal=0",(t.build_id,))
        else:c.execute("UPDATE publication_q1.rows SET payload=jsonb_set(payload,'{value,ipv4_prefix_count}','999') WHERE build_id=%s AND kind='resource'",(t.build_id,))
    with pytest.raises(ValueError,match='副本'):pub.query(t,'resource')


def test_input_bytes_corrupt_cannot_be_unknown(pub,produced):
    # 原件从实际已保存manifest取路径。
    with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:
        c.execute('SELECT manifest FROM domeye.run_specs WHERE run_id=%s',(produced[2]['day']['run_id'],));path=Path(c.fetchone()[0]['inputs'][0]['path'])
    saved=path.read_bytes();path.chmod(0o644);path.write_bytes(saved+b'bad')
    try:
        with pytest.raises(ValueError,match='摘要'):prepare(pub)
    finally:path.write_bytes(saved)


def test_same_snapshot_number_is_not_cross_catalog_identity(pub,produced):
    receipt=json.loads((pub.root/'resource-first/execution.json').read_text())
    receipt['snapshot']=produced[2]['feature']['snapshot']
    fake=pub.root/'wrong-snapshot.json';fake.write_text(encode(receipt))
    with pytest.raises(ValueError,match='错版'):pub.prepare(fake,pub.root/'feature/output/execution.json')


def test_file_loss_runtime_fails_and_no_fallback(pub):
    t=prepare(pub);pub.publish(t,expected_generation=0)
    with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:
        c.execute('SELECT manifest FROM publication_q1.builds WHERE build_id=%s',(t.build_id,));m=c.fetchone()[0]
    path=Path(next(p for p in m['components'][0]['files'] if p.endswith('.parquet')))
    moved=path.with_suffix('.held');path.rename(moved)
    try:
        with pytest.raises(ValueError,match='文件'):pub.query(t,'resource')
    finally:moved.rename(path)


@pytest.mark.parametrize('limits',[Limits(max_rows=1),Limits(max_row_bytes=1),Limits(max_rss_bytes=1),Limits(min_free_bytes=10**18)])
def test_resource_protection_leaves_no_head(pub,limits):
    p=Publication(pub.dsn,pub.root,limits=limits)
    with pytest.raises(ValueError,match='保护'):prepare(p)
    with pytest.raises(ValueError,match='没有'):pub.discover()


def test_reference_revocation_cannot_enter_candidate(pub,produced):
    run=produced[2]['day']['run_id']
    try:
        with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:
            c.execute("UPDATE domeye.reference_inputs SET state='failed' WHERE run_id=%s",(run,))
        with pytest.raises(ValueError):prepare(pub)
        with pytest.raises(ValueError,match='没有'):pub.discover()
    finally:
        with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:
            c.execute("UPDATE domeye.reference_inputs SET state='validated' WHERE run_id=%s",(run,))


def test_sort_metadata_is_bound_to_content(pub):
    t=prepare(pub);pub.publish(t,expected_generation=0)
    with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE publication_q1.rows SET sort_key='altered' WHERE build_id=%s AND kind='resource'",(t.build_id,))
    with pytest.raises(ValueError,match='副本'):pub.query(t,'resource')
