"""Q1.1 私有人工多视图联合验证；仅显式授权绑定时运行。"""
from collections import Counter
from dataclasses import asdict,replace
import copy
import gzip
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import uuid
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
import pytest
from data_pipeline.results import Publication, Token
from data_pipeline.results.manifest_io import encode
from data_pipeline.results import resource_feature_bindings as bindings
from data_pipeline.analysis.features.store import read_table
from data_pipeline.bgp.input.mrt_reader import source_identity
import tests.features.test_feature_views as views_fixture


@pytest.fixture(scope='module')
def setup():
    config=os.environ.get('DOMEYE_Q11_BINDING')
    if not config:pytest.skip('必须绑定本任务人工数据库')
    config=json.loads(Path(config).read_text()); legacy=config['legacy']
    saved=Path(config['output'])/'private.json'
    if saved.exists():
        data=json.loads(saved.read_text());spec=data['report']['specification']
        return dict(pub=Publication(data['dsn'],data['root']),root=saved.parent,legacy=legacy,report=data['report'],
                    views=[SimpleNamespace(**s) for s in spec['source_bindings']],
                    alias=SimpleNamespace(**spec['source_bindings'][0]['aliases'][1]),
                    reference=SimpleNamespace(**spec['reference_binding']))
    params=psycopg2.extensions.parse_dsn(legacy['dsn']);name='q11_'+uuid.uuid4().hex
    pg=psycopg2.connect(legacy['dsn']);pg.autocommit=True
    with pg.cursor() as c:c.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(sql.Identifier(name),sql.Identifier(params['dbname'])))
    pg.close();params['dbname']=name;dsn=psycopg2.extensions.make_dsn(**params)
    root=Path(config['output']);root.mkdir(parents=True,exist_ok=True)
    with psycopg2.connect(dsn) as pg,pg.cursor() as c:
        c.execute('DROP SCHEMA publication_q1 CASCADE')
        c.execute("SELECT value FROM public.ducklake_metadata WHERE key='data_path'");catalog=Path(c.fetchone()[0])
    original=views_fixture.produce
    def produce(manifest,dsn,output,**kw):
        kw['catalog_data_path']=catalog
        return original(manifest,dsn,output,**kw)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(views_fixture,'produce',produce)
        if config.get('diagnostics'):
            import struct
            from tests.observations.test_observation_mrt import update, attr
            old_event=views_fixture.event
            def event(t,**kwargs):
                raw=old_event(t,**kwargs)
                if not kwargs.get('withdraw'):
                    extra=update(ann=b'\x00',withdrawn=b'\x00',attrs=attr(kwargs.get('origin',64498)))
                    extra=struct.pack('!I',t)+extra[4:12]+struct.pack('!I',kwargs.get('vp',64497))+extra[16:]
                    raw+=extra
                return raw
            patch.setattr(views_fixture,'event',event)
        views,reference,readers,view=views_fixture.fixture_views(root,dsn,with_alias=True)
    alias=view(1,'aliasinitial',100,200,'initial_rib')
    path=root/'third.gz';path.write_bytes(gzip.compress(views_fixture.rib(50),mtime=0))
    sha=hashlib.sha256(path.read_bytes()).hexdigest();uri='fixture://rrc00/reference-only'
    manifest=dict(schema_version='observation-run/v1',collector='rrc00',window_start=views_fixture.dt(0).isoformat(),window_end_exclusive=views_fixture.dt(100).isoformat(),
        inputs=[dict(path=str(path),sha256=sha,size=path.stat().st_size,origin_uri=uri,source_id=source_identity('rrc00',uri,sha),role='baseline')],
        baseline_source=source_identity('rrc00',uri,sha),update_sources=[],references=[dict(path=reference.raw_path,sha256=reference.source_sha256)])
    producer=importlib.import_module('data_pipeline.bgp.replay.run_from_files');validator=producer.Draft202012Validator
    def fixture_validator(schema,**kw):
        schema=copy.deepcopy(schema);schema['properties']['collector']={'const':'rrc00'}
        return validator(schema,**kw)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(producer,'Draft202012Validator',fixture_validator)
        third=produce(manifest,dsn,root/'third',min_free_bytes=0)
    reference=replace(reference,run_id=third['run_id'],snapshot=third['snapshot'])
    request=dict(dsn=dsn,collector='rrc25',source_views=[asdict(v) for v in [views[0],alias,*views[1:]]],reference_view=asdict(reference),
        result_window=[views_fixture.dt(400),views_fixture.dt(600)],comparison_window=[views_fixture.dt(200),views_fixture.dt(400)],output=str(root/'feature'))
    req=root/'request.json';req.write_text(json.dumps(request,default=lambda t:t.isoformat()));req.chmod(0o600)
    process=subprocess.run([sys.executable,str(Path(__file__).resolve().parents[3]/'scripts/pipeline/feature-frozen-run.py'),str(req)],capture_output=True,text=True)
    (root/'feature-stdout.log').write_text(process.stdout);(root/'feature-stderr.log').write_text(process.stderr)
    assert process.returncode==0,process.stderr
    report=json.loads(process.stdout)
    pub=Publication(dsn,Path(legacy['root']).parents[1]);pub.initialize()
    result=dict(pub=pub,root=root,legacy=legacy,report=report,views=views,alias=alias,reference=reference)
    (root/'private.json').write_text(json.dumps(dict(dsn=dsn,root=str(pub.root),report=report,legacy=legacy)));(root/'private.json').chmod(0o600)
    return result


@pytest.fixture
def p(setup):
    pub=setup['pub']
    with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:c.execute('TRUNCATE publication_q1.head,publication_q1.rows,publication_q1.builds')
    return pub


def prepare(p,setup,later=False):
    return p.prepare(Path(setup['legacy']['root'])/('resource-later' if later else 'resource-first')/'execution.json',setup['root']/'feature/execution.json')


def test_formal_all_windows_provenance_and_old_head(p,setup):
    token=prepare(p,setup);p.publish(token,expected_generation=0)
    report=setup['report'];spec=report['specification']
    assert spec['observation_run'] is None and len(spec['source_bindings'])==5
    assert len(spec['source_bindings'][0]['aliases'])==2
    assert spec['reference_binding']['carrier_collector']=='rrc00' and spec['collector']=='rrc25'
    for mode in ('ordinary','ir'):
        result=p.query(token,'feature',mode=mode,page_size=100)
        expected=[r for b in read_table(p.dsn,report['run_id'],report['snapshot'],'windows') for r in b.to_pylist() if r['mode']==mode]
        assert Counter(encode(x['value']) for x in result['items'])==Counter(encode(r) for r in expected)
        assert {r['window_role'] for r in expected}=={'comparison','result'}
        basis=result['items'][0]['calculation']['basis']
        for field in ('source_bindings','reference_binding','result_window','comparison_window','calculation_window'):assert basis[field]==spec[field]
        assert any(r['announ_num']==r['withdraw_num']==0 for r in expected if r['scope']=='collect')
        if mode=='ir':assert any(r['row_presence']=='ir_asn_write_disabled' for r in expected)
        else:assert next(r for r in expected if r['window_role']=='result' and r['scope']=='asn' and r['subject']=='64498')['withdraw_num']==1
    old=p.query(token,'feature',mode='ordinary',page_size=100)
    new=prepare(p,setup,True);p.publish(new,expected_generation=1)
    assert p.query(token,'feature',mode='ordinary',page_size=100)==old
    assert p.query(token,'feature',mode='ordinary',page=999)['items']==[]
    (setup['root']/'positive.json').write_text(encode({'token':asdict(token),'report':report,'query':old}))


@pytest.mark.parametrize('target',['main','alias','reference'])
@pytest.mark.parametrize('phase',['prepare','publish'])
def test_all_qualifications_rejected(p,setup,target,phase):
    old=prepare(p,setup);p.publish(old,expected_generation=0)
    candidate=prepare(p,setup,True) if phase=='publish' else None
    view=setup['views'][0] if target=='main' else setup[target]
    table='domeye.reference_inputs' if target=='reference' else 'domeye.inputs'
    source=view.source_sha256 if target=='reference' else view.source_id
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        c.execute(sql.SQL('SELECT state FROM {} WHERE run_id=%s AND source_id=%s').format(sql.Identifier(*table.split('.'))),(view.run_id,source));state=c.fetchone()[0]
        c.execute(sql.SQL("UPDATE {} SET state='failed' WHERE run_id=%s AND source_id=%s").format(sql.Identifier(*table.split('.'))),(view.run_id,source))
    try:
        with pytest.raises(ValueError):
            if phase=='publish':p.publish(candidate,expected_generation=1)
            else:prepare(p,setup,True)
        assert p.discover()==(old,1)
    finally:
        with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute(sql.SQL('UPDATE {} SET state=%s WHERE run_id=%s AND source_id=%s').format(sql.Identifier(*table.split('.'))),(state,view.run_id,source))


@pytest.mark.parametrize('field',['messages','elements'])
def test_multiview_commit_count_rejected(p,setup,field):
    run=setup['report']['run_id']
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute(sql.SQL('UPDATE feature.source_commits SET {}={}+999 WHERE run_id=%s').format(sql.Identifier(field),sql.Identifier(field)),(run,))
    try:
        with pytest.raises(ValueError,match='消息|元素'):prepare(p,setup)
    finally:
        with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute(sql.SQL('UPDATE feature.source_commits SET {}={}-999 WHERE run_id=%s').format(sql.Identifier(field),sql.Identifier(field)),(run,))


@pytest.mark.parametrize('mode',['ordinary','ir'])
def test_mode_empty_page_gate(p,setup,mode):
    token=prepare(p,setup);p.publish(token,expected_generation=0)
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute("UPDATE publication_q1.rows SET mode='ir' WHERE build_id=%s AND kind='feature' AND mode='ordinary'",(token.build_id,))
    with pytest.raises(ValueError,match='请求能力行数'):p.query(token,'feature',mode=mode,page=999)


@pytest.mark.parametrize('field',['origin_uri','content_sha256','source_role','calculation_role','snapshot','run_id','collector','reference_rows','source_reference','window'])
def test_claimed_binding_mismatch_rejected(p,setup,field):
    report=json.loads((setup['root']/'feature/execution.json').read_text());spec=report['specification'];source=spec['source_bindings'][0]
    if field=='source_reference':
        for view in [source,*source['aliases']]:view['reference_sha256']='0'*64
    elif field=='window':
        source=spec['source_bindings'][1]
        for view in [source,*source['aliases']]:view['window']['start']='1970-01-01T00:03:21+00:00'
    elif field=='collector':spec['collector']='rrc00'
    elif field=='reference_rows':spec['reference_binding']['expected_rows']+=1
    elif field=='snapshot':source[field]=setup['report']['snapshot']
    elif field=='run_id':source[field]=spec['reference_binding']['run_id']
    else:source[field]={'origin_uri':'fixture://wrong','content_sha256':'0'*64,'source_role':'update','calculation_role':'update'}[field]
    path=setup['root']/'altered-execution.json';path.write_text(encode(report))
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute('UPDATE feature.runs SET specification=%s WHERE run_id=%s',(Json(spec),report['run_id']))
    try:
        with pytest.raises((ValueError,KeyError)):
            p.prepare(Path(setup['legacy']['root'])/'resource-first/execution.json',path)
    finally:
        with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute('UPDATE feature.runs SET specification=%s WHERE run_id=%s',(Json(setup['report']['specification']),report['run_id']))


def test_diagnostics_saved_and_versions(p,setup):
    report=setup['report'];spec=report['specification']
    token=prepare(p,setup);p.publish(token,expected_generation=0)
    diagnostics=p.query(token,'feature',mode='ordinary')['items'][0]['calculation']['basis']['diagnostics']
    if 'diagnostics_dataset_version' not in spec:
        assert diagnostics['state']=='not_saved'
        return
    rows=[r for b in read_table(p.dsn,report['run_id'],report['snapshot'],'module_diagnostics') for r in b.to_pylist()]
    assert rows and any(r['raw_prefix']=='0.0.0.0/0' for r in rows)
    assert diagnostics['state']=='saved' and sum(r['count'] for r in diagnostics['sources'])==len(rows)
    assert all(r['count']==0 for r in diagnostics['sources'] if r['state']=='not_applicable')
    assert any(r['state']=='saved' and r['count']==0 for r in diagnostics['sources'])
    history=bindings.feature_history(p.dsn,report['run_id'],report['snapshot'])
    broken=copy.deepcopy(history);broken[-1]['diagnostics']+=1
    with pytest.raises(ValueError,match='诊断来源计数'):bindings.validate_diagnostics(p.dsn,report['run_id'],report['snapshot'],spec,broken,lambda:None)
    with pytest.raises(ValueError,match='版本'):bindings.feature_tables({**spec,'diagnostics_dataset_version':'future'})


def test_legacy_70c_and_6106_single_run(p,setup):
    legacy=setup['legacy'];old=Publication(legacy['dsn'],legacy['root'])
    assert old.query(Token(**legacy['token']),'feature',mode='ordinary')['total']==8
    token=p.prepare(Path(legacy['root'])/'resource-first/execution.json',Path(legacy['root'])/'feature/output/execution.json')
    p.publish(token,expected_generation=0)
    rows=p.query(token,'feature',mode='ordinary')
    assert rows['total']==8 and rows['items'][0]['calculation']['basis']['diagnostics']['state']=='not_saved'


@pytest.mark.parametrize('change',['missing_table','unknown_version'])
def test_invalid_diagnostic_dataset_cannot_prepare(p,setup,change):
    report=setup['report'];run=report['run_id']
    if change=='missing_table':
        metadata='fl_'+run
        with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
            c.execute(sql.SQL("UPDATE {}.ducklake_table SET table_name='hidden_diagnostics' WHERE table_name='module_diagnostics'").format(sql.Identifier(metadata)))
        try:
            with pytest.raises(Exception):prepare(p,setup)
            with pytest.raises(ValueError):p.discover()
        finally:
            with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
                c.execute(sql.SQL("UPDATE {}.ducklake_table SET table_name='module_diagnostics' WHERE table_name='hidden_diagnostics'").format(sql.Identifier(metadata)))
    else:
        altered=json.loads((setup['root']/'feature/execution.json').read_text());altered['specification']['diagnostics_dataset_version']='future'
        path=setup['root']/'unknown-execution.json';path.write_text(encode(altered))
        with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute('UPDATE feature.runs SET specification=%s WHERE run_id=%s',(Json(altered['specification']),run))
        try:
            with pytest.raises(ValueError,match='版本'):p.prepare(Path(setup['legacy']['root'])/'resource-first/execution.json',path)
        finally:
            with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:c.execute('UPDATE feature.runs SET specification=%s WHERE run_id=%s',(Json(report['specification']),run))


def test_original_6106_token_still_reads(setup):
    config=json.loads(Path(os.environ['DOMEYE_Q11_BINDING']).read_text())
    old=Publication(config['dsn'],config['root']);token,generation=old.discover()
    assert generation==2
    assert old.query(token,'feature',mode='ordinary')['total']==8


@pytest.mark.parametrize('target',['alias','d'])
@pytest.mark.parametrize('column',['message_count','element_count'])
def test_each_upstream_view_counts(p,setup,target,column):
    view=setup['alias'] if target=='alias' else setup['views'][3]
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        c.execute(sql.SQL('UPDATE domeye.source_receipts SET {}={}+1 WHERE run_id=%s AND source_id=%s').format(sql.Identifier(column),sql.Identifier(column)),(view.run_id,view.source_id))
    try:
        with pytest.raises(ValueError):prepare(p,setup)
    finally:
        with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
            c.execute(sql.SQL('UPDATE domeye.source_receipts SET {}={}-1 WHERE run_id=%s AND source_id=%s').format(sql.Identifier(column),sql.Identifier(column)),(view.run_id,view.source_id))


def test_catalog_identity_and_independent_snapshots(p,setup):
    token=prepare(p,setup);p.publish(token,expected_generation=0)
    resource=p.query(token,'resource')['source'];feature=p.query(token,'feature',mode='ordinary')['source']
    assert resource['catalog_ref']['metadata_schema']=='public'
    assert feature['catalog_ref']['metadata_schema']=='fl_'+feature['run_id']
    assert resource['snapshot']!=feature['snapshot']
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        with pytest.raises(ValueError,match='schema版本缺失'):
            bindings.catalog(c,'public','f_'+feature['run_id'],feature['snapshot'])
    (setup['root']/'catalog-evidence.json').write_text(encode({'resource':resource,'feature':feature,
        'observations':[(v.run_id,v.snapshot) for v in setup['views']],
        'reference':(setup['reference'].run_id,setup['reference'].snapshot)}))
