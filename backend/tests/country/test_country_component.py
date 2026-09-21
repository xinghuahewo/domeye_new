"""C3自有PG/原件链与明确typed稀有分支；不接真实D或其他任务库。"""
from collections import Counter
from dataclasses import replace,asdict
from fractions import Fraction
from pathlib import Path
import json
import os
import uuid
import time

import psycopg2
import pytest

from data_pipeline.analysis.country_events import event_aggregation as c2, compute
from data_pipeline.analysis.country_events.snapshot_store import persist_stream, produce_component, ComponentBinding
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch, ReadReceipt
from data_pipeline.analysis.country_events.snapshot_schema import row_encode, row_decode, encode, TABLES
from tests.country.test_country_c2 import run_case
from tests.country.test_country_enhancement_cohort import route, incident
from tests.country.test_country_enhancement_compute import change
from data_pipeline.analysis.country_events.models import Time, Cursor


@pytest.fixture
def catalog():
    base=os.environ.get('DOMEYE_COUNTRY_C1_TEST_DSN')
    if not base:pytest.skip('仅本任务UTF8 socket PG')
    assert psycopg2.extensions.parse_dsn(base)['host'].startswith('/')
    pg=psycopg2.connect(base);pg.autocommit=True
    name='country_c3_'+uuid.uuid4().hex
    with pg.cursor() as cur:cur.execute('CREATE DATABASE '+name)
    pg.close()
    return base+' dbname='+name


def read_all(reader):
    parts=list(reader.stream())
    assert isinstance(parts[-1],ReadReceipt) and parts[-1].full_body_validated
    return [r for part in parts if isinstance(part,ReadBatch) for r in part.rows]


def test_codec_arbitrary_precision_and_ancillary_order(tmp_path):
    rows=run_case(tmp_path)
    for n,item in enumerate(rows):
        table,flat=row_encode(n,item)
        assert row_decode(table,flat)==item
    metric=next(r for r in rows if isinstance(r,c2.C2Row) and isinstance(r.value,compute.MetricPoint))
    for value in (0,Fraction(0),Fraction(1,2),Fraction(1,2**80),Fraction(2**128-1,2**80),None):
        item=replace(metric,value=replace(metric.value,value=value))
        table,flat=row_encode(3,item);out=row_decode(table,flat)
        assert out==item and type(out.value.value) is type(value)
    evidence=c2.C2Row(None,None,c2.InputEvidence('rare','reference',{'missing_is_absent':{},'x':None,'a':[3,3,1],'b':(1,None),'bytes':b'\x00\xff'}))
    table,flat=row_encode(1,evidence)
    assert repr(row_decode(table,flat))==repr(evidence)


def test_typed_component_round_trip_and_page(tmp_path,catalog):
    rows=run_case(tmp_path)
    binding=persist_stream(iter(rows),catalog,tmp_path/'component',batch_rows=1)
    reader=ComponentReader(catalog,binding,batch_rows=1)
    out=read_all(reader)
    assert list(map(repr,out))==list(map(repr,rows))
    page=reader.page('metric_point',incident_id=incident().incident_id,limit=2)
    assert len(page.rows)==2 and page.next_sequence is not None and not page.receipt.full_body_validated
    second=reader.page('metric_point',after_sequence=page.next_sequence,limit=2)
    assert len(second.rows)==2 and second.rows!=page.rows
    (tmp_path/'binding.json').write_text(json.dumps(asdict(binding)))


from tests.country.test_country_c1_repair import review_chain


def test_formal_chain_full_roundtrip(review_chain,catalog,monkeypatch):
    from tests.country.test_country_saved_input import adapter
    root=review_chain[0]
    grid=compute.Grid(100000000,125000000,(107000000,113000000,120000000,121000000,121500000,124000000,125000000))
    original=list(c2.run_saved(adapter(review_chain),grid,scratch_root=root))
    started=time.monotonic()
    binding=persist_stream(iter(original),catalog,root/'component',c1=adapter(review_chain),parameters={'grid':asdict(grid)})
    default_seconds=time.monotonic()-started
    out=read_all(ComponentReader(catalog,binding,c1=adapter(review_chain)))
    assert list(map(repr,original))==list(map(repr,out))
    assert len(out)==411
    heads=[r.value for r in out if isinstance(r,c2.C2Row) and isinstance(r.value,c2.EventStatus)]
    assert sorted(h.incident.onset.epoch for h in heads)==[106,112]
    assert all(h.fixed_prefix_count==h.direction_count==1 for h in heads)
    (root/'c3-binding.json').write_text(json.dumps(asdict(binding)))
    started=time.monotonic()
    one=persist_stream(iter(original),catalog,root/'component-one',c1=adapter(review_chain),parameters={'grid':asdict(grid)},batch_rows=1)
    one_seconds=time.monotonic()-started
    assert list(map(repr,read_all(ComponentReader(catalog,one,c1=adapter(review_chain),batch_rows=1))))==list(map(repr,original))
    captured=[];run_saved=c2.run_saved
    def record(*args,**kwargs):
        for item in run_saved(*args,**kwargs):captured.append(item);yield item
    monkeypatch.setattr(c2,'run_saved',record)
    started=time.monotonic()
    direct=produce_component(adapter(review_chain),grid,catalog,root/'component-direct')
    direct_seconds=time.monotonic()-started
    assert list(map(repr,read_all(ComponentReader(catalog,direct,c1=adapter(review_chain)))))==list(map(repr,captured))
    (root/'c3-end-to-end-seconds.json').write_text(json.dumps({'default':default_seconds,'one':one_seconds,'direct':direct_seconds}))
    (root/'c3-cost-summary.json').write_text(json.dumps({'catalog_database':psycopg2.extensions.parse_dsn(catalog)['dbname'],'default':asdict(binding),'one':asdict(one),'direct':asdict(direct)}))


def test_missing_completion_fails_candidate(tmp_path,catalog):
    rows=run_case(tmp_path)
    with pytest.raises(ValueError,match='missing_completion'):persist_stream(iter(rows[:-1]),catalog,tmp_path/'bad')
    with psycopg2.connect(catalog) as pg:
        with pg.cursor() as cur:
            cur.execute('SELECT state FROM country_components.components');assert cur.fetchall()==[('failed',)]
    assert not (tmp_path/'bad/ready.json').exists()


@pytest.mark.parametrize('mode',['tail','missing_row','duplicate_completion','bad_reference','wrong_revision','scope','row_bytes','disk','rss'])
def test_rejected_streams_keep_old_component(tmp_path,catalog,mode):
    rows=run_case(tmp_path)
    good=persist_stream(iter(rows),catalog,tmp_path/'old')
    altered=list(rows);limits={}
    if mode=='tail':altered.append(rows[0])
    elif mode=='missing_row':altered.pop(2)
    elif mode=='duplicate_completion':altered.append(rows[-1])
    elif mode=='bad_reference':
        n=next(n for n,r in enumerate(rows) if isinstance(r,c2.C2Row) and isinstance(r.value,compute.PathSample))
        altered[n]=replace(rows[n],value=replace(rows[n].value,observation_ref='missing'))
    elif mode=='wrong_revision':
        n=next(n for n,r in enumerate(rows) if isinstance(r,c2.C2Row) and isinstance(r.value,compute.MetricPoint))
        altered[n]=replace(rows[n],revision=999)
    elif mode=='scope':altered[-1]=replace(rows[-1],scope=(0,1))
    else:limits[{'rows':'max_rows','event_rows':'max_event_rows','row_bytes':'max_row_bytes','disk':'max_disk_bytes','rss':'max_rss_bytes'}[mode]]=1
    with pytest.raises(ValueError):persist_stream(iter(altered),catalog,tmp_path/'bad',**limits)
    assert read_all(ComponentReader(catalog,good))==rows
    with psycopg2.connect(catalog) as pg,pg.cursor() as cur:
        cur.execute("SELECT state FROM country_components.components ORDER BY state")
        # Constructor budget failures can leave candidate, never complete.
        states=[r[0] for r in cur.fetchall()];assert states.count('complete')==1


def test_default_batch_one_and_early_close(tmp_path,catalog):
    rows=run_case(tmp_path,events=[incident(),replace(incident(),revision=2)])
    one=persist_stream(iter(rows),catalog,tmp_path/'one',batch_rows=1,max_rows=1,max_event_rows=1)
    default=persist_stream(iter(rows),catalog,tmp_path/'default')
    assert read_all(ComponentReader(catalog,one))==read_all(ComponentReader(catalog,default,batch_rows=1))==rows
    reader=ComponentReader(catalog,default,batch_rows=1);stream=reader.stream()
    first=next(stream);assert isinstance(first,ReadBatch)
    workdir=reader.workdir;stream.close()
    assert not Path(workdir).exists()
    with pytest.raises(ValueError,match='unknown_incident'):reader.page('metric_point',incident_id='absent')
    with pytest.raises(ValueError,match='page_scope'):reader.page('not_a_table')


@pytest.mark.parametrize('mode',['file','manifest','ready','registration','index','snapshot','database'])
def test_fixed_binding_rejects_damage(tmp_path,catalog,mode):
    rows=run_case(tmp_path);binding=persist_stream(iter(rows),catalog,tmp_path/'component')
    if mode=='file':
        p=tmp_path/'component/data/metric_point.parquet';p.write_bytes(p.read_bytes()[:-1])
    elif mode in ('manifest','ready'):(tmp_path/'component'/f'{mode}.json').write_text('{}')
    elif mode=='registration':
        with psycopg2.connect(catalog) as pg,pg.cursor() as cur:cur.execute("UPDATE country_components.components SET state='failed'")
    elif mode=='index':
        with psycopg2.connect(catalog) as pg,pg.cursor() as cur:cur.execute("DELETE FROM country_components.event_index")
    elif mode=='snapshot':binding=replace(binding,snapshot=binding.snapshot+1)
    else:
        pg=psycopg2.connect(os.environ['DOMEYE_COUNTRY_C1_TEST_DSN']);pg.autocommit=True
        name='country_c3_'+uuid.uuid4().hex
        with pg.cursor() as cur:cur.execute('CREATE DATABASE '+name)
        pg.close();other=catalog.rsplit(' dbname=',1)[0]+' dbname='+name
        shadow=persist_stream(iter(rows),other,tmp_path/'shadow')
        assert shadow.snapshot==binding.snapshot
        catalog=other
    with pytest.raises((ValueError,FileNotFoundError)):ComponentReader(catalog,binding)


def test_fsync_failure_ready_not_admitted(tmp_path,catalog,monkeypatch):
    from data_pipeline.analysis.country_events import snapshot_store as component
    rows=run_case(tmp_path);old=persist_stream(iter(rows),catalog,tmp_path/'old')
    real=component.fsync_tree;calls=[]
    def fail_second(root):
        calls.append(root)
        if len(calls)==2:raise OSError('injected directory fsync failure')
        real(root)
    monkeypatch.setattr(component,'fsync_tree',fail_second)
    with pytest.raises(OSError):persist_stream(iter(rows),catalog,tmp_path/'bad')
    assert (tmp_path/'bad/ready.json').exists()
    assert read_all(ComponentReader(catalog,old))==rows
    with psycopg2.connect(catalog) as pg,pg.cursor() as cur:
        cur.execute("SELECT state FROM country_components.components WHERE root=%s",(str(tmp_path/'bad'),));assert cur.fetchone()==('failed',)


def test_formal_empty_produce_component(tmp_path,catalog):
    from tests.country.test_country_saved_input import build_pipeline, adapter
    from tests.observations.test_observation_consumer import observation_fixture
    prepared=build_pipeline(tmp_path,os.environ['DOMEYE_COUNTRY_C1_TEST_DSN'],observation_fixture(tmp_path),rich=False)
    grid=compute.Grid(100000000,125000000,(110000000,125000000))
    binding=produce_component(adapter(prepared),grid,catalog,tmp_path/'component')
    rows=read_all(ComponentReader(catalog,binding,c1=adapter(prepared)))
    assert rows[-1].input_kind=='saved_C1' and rows[-1].event_count==0
    assert rows[-1].input_completion.enumeration=='complete_empty'
    assert json.loads((tmp_path/'component/manifest.json').read_text())['availability']=='known_empty'


def test_typed_partial_and_exact_values_persist(tmp_path,catalog):
    import ipaddress
    rows=run_case(tmp_path,events=[incident(),replace(incident(),incident_id='early',onset=Time(9))])
    metric=next(r for r in rows if isinstance(r,c2.C2Row) and isinstance(r.value,compute.MetricPoint))
    prefixes=list(ipaddress.IPv6Network('::/0').address_exclude(ipaddress.IPv6Network('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128')))
    huge=compute.resource([str(p) for p in prefixes],2)
    assert huge==Fraction(2**128-1,2**80)
    rare=[replace(metric,value=replace(metric.value,value=v)) for v in (0,Fraction(0),Fraction(1,2),Fraction(1,2**80),huge,None)]
    extra=c2.C2Row(None,None,c2.InputEvidence('typed_rare','rare',{'a':[1,1,None],'b':(),'missing':{}}))
    altered=rows[:-1]+rare+[extra,replace(rows[-1],costs={**rows[-1].costs,'output_rows':len(rows)-1+len(rare)+1})]
    binding=persist_stream(iter(altered),catalog,tmp_path/'component')
    assert list(map(repr,read_all(ComponentReader(catalog,binding))))==list(map(repr,altered))
    assert json.loads((tmp_path/'component/manifest.json').read_text())['availability']=='partial'


@pytest.mark.parametrize('target',['observation','detection','reference'])
def test_formal_source_revocation_before_read_and_after_ready(review_chain,catalog,target,monkeypatch):
    from data_pipeline.analysis.country_events import snapshot_store as component
    from tests.country.test_country_saved_input import adapter
    root,dsns,_,o,d=review_chain
    grid=compute.Grid(100000000,125000000,(107000000,113000000,125000000))
    rows=list(c2.run_saved(adapter(review_chain),grid,scratch_root=root))
    good=persist_stream(iter(rows),catalog,root/('revocation-good-'+target),c1=adapter(review_chain),parameters={'grid':asdict(grid)})
    dsn=dsns[1] if target=='detection' else dsns[0]
    sql,key,old={
        'observation':('UPDATE domeye.runs SET state=%s WHERE run_id=%s',o['run_id'],'complete'),
        'detection':('UPDATE detection.runs SET state=%s WHERE run_id=%s',d['run_id'],'complete'),
        'reference':('UPDATE domeye.reference_inputs SET state=%s WHERE run_id=%s',o['run_id'],'validated'),
    }[target]
    def set_state(state):
        with psycopg2.connect(dsn) as pg,pg.cursor() as cur:cur.execute(sql,(state,key))
    real=component.fsync_tree
    def revoke_after_ready(path):
        real(path)
        if (path/'ready.json').exists():set_state('failed')
    try:
        existing=adapter(review_chain)
        monkeypatch.setattr(component,'fsync_tree',revoke_after_ready)
        with pytest.raises(ValueError):persist_stream(iter(rows),catalog,root/('revocation-bad-'+target),c1=adapter(review_chain),parameters={'grid':asdict(grid)})
        with pytest.raises(ValueError):ComponentReader(catalog,good,c1=existing)
        with psycopg2.connect(catalog) as pg,pg.cursor() as cur:
            cur.execute('SELECT state FROM country_components.components ORDER BY state');assert cur.fetchall()==[('complete',),('failed',)]
    finally:set_state(old)
    assert read_all(ComponentReader(catalog,good,c1=adapter(review_chain)))==rows


def test_formal_missing_fact_and_reference_binding(review_chain,catalog):
    from tests.country.test_country_saved_input import adapter, mutate_saved
    root=review_chain[0];grid=compute.Grid(100000000,125000000,(107000000,125000000))
    rows=list(c2.run_saved(adapter(review_chain),grid,scratch_root=root))
    n=next(n for n,r in enumerate(rows) if isinstance(r,c2.C2Row) and isinstance(r.value,c2.CohortMember))
    bad=list(rows);bad[n]=replace(rows[n],value=replace(rows[n].value,route=replace(rows[n].value.route,observation_ref='missing')))
    with pytest.raises(ValueError,match='orphan_observation'):
        persist_stream(iter(bad),catalog,root/'bad-fact',c1=adapter(review_chain),parameters={'grid':asdict(grid)})
    with mutate_saved(review_chain,'references',lambda table:table.slice(0,0)):
        with pytest.raises(ValueError):produce_component(adapter(review_chain),grid,catalog,root/'bad-references')


def test_typed_remaining_paths_and_new_direction_are_real_outputs(tmp_path,catalog):
    from data_pipeline.analysis.country_events.incremental_types import PathPeakQuality
    extra=route('new-endpoint',endpoint=replace(route().endpoint,local_ip='192.0.2.9'),mapping_state='ambiguous')
    rows=run_case(tmp_path,changes=[change(0,22,extra),change(1,Time(23),presence='absent',path_ref=None)],samples=(21,23.5,26))
    present={type(r.value) for r in rows if isinstance(r,c2.C2Row)}
    assert {compute.NewDirectionPoint,compute.PathSample,compute.PathSummary,PathPeakQuality}<=present
    binding=persist_stream(iter(rows),catalog,tmp_path/'component')
    assert list(map(repr,read_all(ComponentReader(catalog,binding))))==list(map(repr,rows))


@pytest.mark.parametrize('damage',['table_count','table_schema','file_link','event_scope'])
def test_actual_lake_validation_not_just_signed_manifest(tmp_path,catalog,damage):
    from data_pipeline.analysis.country_events.snapshot_store import file_hash
    from data_pipeline.analysis.country_events.snapshot_schema import packed
    rows=run_case(tmp_path);binding=persist_stream(iter(rows),catalog,tmp_path/'component')
    path=Path(binding.root)/'manifest.json';manifest=json.loads(path.read_text())
    if damage=='table_count':manifest['tables']['metric_point']['rows']-=1
    elif damage=='table_schema':manifest['schemas']['metric_point'].pop()
    elif damage=='file_link':manifest['lake_files']['metric_point'][0][0]='data/path_sample.parquet'
    else:manifest['scope']=[0,1]
    # 仅负例重签人工manifest和人工登记，迫使Reader独立检查实际湖schema/count/link。
    path.write_text(packed(manifest));sha=file_hash(path)
    (Path(binding.root)/'ready.json').write_text(packed({'component_id':binding.component_id,'manifest_sha256':sha}))
    with psycopg2.connect(catalog) as pg,pg.cursor() as cur:
        cur.execute('UPDATE country_components.components SET manifest_sha256=%s WHERE component_id=%s',(sha,binding.component_id))
    binding=replace(binding,manifest_sha256=sha)
    with pytest.raises(ValueError):read_all(ComponentReader(catalog,binding))


def test_page_does_not_claim_or_repeat_full_hash_audit(tmp_path,catalog,monkeypatch):
    from data_pipeline.analysis.country_events import snapshot_reader as component_reader
    rows=run_case(tmp_path);binding=persist_stream(iter(rows),catalog,tmp_path/'component')
    real=component_reader.file_hash
    def metadata_only(path,guard=lambda:None):
        assert Path(path).suffix!='.parquet','page不应全扫描正文'
        return real(path,guard)
    monkeypatch.setattr(component_reader,'file_hash',metadata_only)
    page=ComponentReader(catalog,binding).page('metric_point',limit=1)
    assert len(page.rows)==1 and not page.receipt.full_body_validated
    assert ComponentReader(catalog,binding,max_rows=1).manifest['row_count']==len(rows)
    with pytest.raises(ValueError,match='resource_limit'):ComponentReader(catalog,binding,batch_bytes=1)
    with pytest.raises(ValueError,match='resource_limit'):ComponentReader(catalog,binding,max_disk_bytes=1)
