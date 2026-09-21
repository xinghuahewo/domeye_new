"""C4人工模块检验；fixture资格器明确不代表真实P发布验收。"""
from contextlib import contextmanager
from dataclasses import replace,asdict
from pathlib import Path
from fractions import Fraction
import hashlib
import json
import pytest

from tests.country.country_query_test_support import country_pg_log
from tests.country.test_country_component import catalog
from tests.country.test_country_c1_repair import review_chain
from tests.country.test_country_c2 import run_case
from tests.country.test_country_enhancement_cohort import incident
from data_pipeline.analysis.country_events import event_aggregation as c2, compute
from data_pipeline.analysis.country_events.snapshot_store import persist_stream
from data_pipeline.analysis.country_events.snapshot_schema import encode
from data_pipeline.analysis.country_events.selection_contract import *
from data_pipeline.analysis.country_events.selection_index import prepare_country_index, inspect_country, read_country_event
from data_pipeline.analysis.country_events.selection_reader import open_qualified_country


@contextmanager
def fixture_qualification(descriptor,*,lock=False,selection=None):
    # 仅人工模块依赖注入，P字符串不是发布证明。
    yield


def runtime(dsn,c1=None): return CountryRuntime(dsn,c1,fixture_qualification)

def select(desc,event):
    status=read_country_event(desc,incident_id=event)
    return CountrySelection('fixture-not-published','fixture-build','fixture-profile','country',desc,event,status.incident.revision,status.cohort_id,encode(status))


def fixture_index(tmp_path,catalog,**kwargs):
    rows=run_case(tmp_path,**kwargs)
    binding=persist_stream(iter(rows),catalog,tmp_path/'c3',batch_rows=1)
    rt=runtime(catalog)
    b,p=prepare_country_index(binding,reference_binding=None,runtime=rt,output=tmp_path/'c4')
    return rows,rt,b,p,inspect_country(b,p,runtime=rt)


def test_complete_fixture_all_views(tmp_path,catalog):
    rows,rt,b,p,d=fixture_index(tmp_path,catalog)
    assert p.stream_calls==p.locator_passes==1 and p.source_rows==p.readback_rows==len(rows)
    selection=select(d,incident().incident_id)
    with open_qualified_country(selection,p,runtime=rt,verify_qualification=fixture_qualification) as q:
        for view in ('overview','series15','asns','asn_matrix','asn_window','asn_peaks','paths','path_samples','audit'):
            outputs=[]
            for size in (1,3,100):
                out=list(q.iter_query(QueryRequest(view),page_size=size))
                assert isinstance(out[-1],QueryReadReceipt)
                assert out[-1].rows==len(out)-1
                outputs.append(tuple(map(encode,out[:-1])))
            assert outputs[0]==outputs[1]==outputs[2]
        matrix=[r.value for r in rows if isinstance(r,c2.C2Row) and isinstance(r.value,compute.AsnPoint)]
        actual=list(q.iter_query(QueryRequest('asn_matrix'),page_size=2))[:-1]
        assert sorted(map(encode,matrix))==sorted(encode(r['value']) for r in actual)
        page=q.query(QueryRequest('series15'),limit=1)
        assert page.total==45
        with pytest.raises(ValueError,match='cursor_mismatch'): q.query(QueryRequest('audit'),cursor=page.next_cursor,limit=1)
        with pytest.raises(ValueError,match='alias_not_available'): q.query(QueryRequest('resolve_alias'))


def test_formal_static_and_all_pages(review_chain,catalog):
    from tests.country.test_country_saved_input import adapter
    from data_pipeline.analysis.country_events.snapshot_store import database_identity
    root=review_chain[0]; c1=adapter(review_chain)
    grid=compute.Grid(100000000,125000000,(107000000,113000000,120000000,121000000,121500000,124000000,125000000))
    rows=list(c2.run_saved(c1,grid,scratch_root=root))
    binding=persist_stream(iter(rows),catalog,root/'c3-c4',c1=c1,parameters={'grid':asdict(grid)})
    s=c1.identity['reference_sources']['as_info']
    ref=ReferenceBinding(**database_identity(c1.reader.dsn),run_id=c1.reader.run_id,snapshot=c1.reader.snapshot,
        manifest_sha256=digest(c1.reader.manifest),source_id=s['source_id'],content_sha256=s['source_id'],expected_rows=s['rows'])
    rt=runtime(catalog,c1)
    b,p=prepare_country_index(binding,reference_binding=ref,runtime=rt,output=root/'c4')
    d=inspect_country(b,p,runtime=rt)
    for head in (r.value for r in rows if isinstance(r,c2.C2Row) and isinstance(r.value,c2.EventStatus)):
        with open_qualified_country(select(d,head.incident.incident_id),p,runtime=rt,verify_qualification=fixture_qualification) as q:
            out=list(q.iter_query(QueryRequest('asns'),page_size=1))
            assert all(x['static']['as_name']['state'] in ('available','field_absent','record_not_found','empty') for x in out[:-1])
            assert all(x['static']['nature']['state']=='unknown' for x in out[:-1])
            for view in ('overview','series15','asn_matrix','asn_window','asn_peaks','paths','path_samples','audit'):
                assert isinstance(list(q.iter_query(QueryRequest(view),page_size=2))[-1],QueryReadReceipt)
    (root/'c4-binding.json').write_text(contract_json(b)); (root/'c4-proof.json').write_text(contract_json(p))


@pytest.mark.parametrize('damage',['manifest','index','proof','revision','cohort','profile','budget_rows','budget_bytes','budget_groups','budget_steps','revoked','early_close'])
def test_reject_damage_and_bounded_reads(tmp_path,catalog,damage,monkeypatch):
    rows,rt,b,p,d=fixture_index(tmp_path,catalog)
    s=select(d,incident().incident_id)
    if damage in ('manifest','index'):
        path=Path(b.root)/('manifest.json' if damage=='manifest' else 'index.sqlite')
        with path.open('ab') as f: f.write(b'bad')
        with pytest.raises(ValueError): inspect_country(b,p,runtime=rt)
        return
    if damage=='proof':
        with pytest.raises(ValueError): inspect_country(b,replace(p,readback_rows=0),runtime=rt)
        return
    if damage in ('revision','cohort'):
        s=replace(s,**({'revision':99} if damage=='revision' else {'cohort_id':'wrong'}))
        with pytest.raises(ValueError): open_qualified_country(s,p,runtime=rt,verify_qualification=fixture_qualification)
        return
    if damage=='profile':
        @contextmanager
        def published_fixture(desc,*,lock=False,selection=None):
            if selection and selection.profile_digest!='fixture-profile': raise ValueError('fixture_profile_mismatch')
            yield
        with pytest.raises(ValueError): open_qualified_country(replace(s,profile_digest='wrong'),p,runtime=rt,verify_qualification=published_fixture)
        return
    flag=[False]
    @contextmanager
    def qualify(desc,*,lock=False,selection=None):
        if flag[0]: raise ValueError('fixture_revoked')
        yield
    limits=QueryLimits()
    if damage.startswith('budget_'):
        field={'budget_rows':'max_page_rows','budget_bytes':'max_page_bytes','budget_groups':'max_row_groups','budget_steps':'max_index_steps'}[damage]
        limits=replace(limits,**{field:1})
    with open_qualified_country(s,p,runtime=rt,verify_qualification=qualify,limits=limits) as q:
        if damage=='budget_steps':
            assert q.query(QueryRequest('series15'),limit=100).total>=0
        elif damage.startswith('budget_'):
            with pytest.raises(ValueError): q.query(QueryRequest('series15'),limit=100)
        elif damage=='revoked':
            original=q.access.read
            def revoke(locs):
                got=original(locs); flag[0]=True; return got
            monkeypatch.setattr(q.access,'read',revoke)
            with pytest.raises(ValueError,match='revoked'): q.query(QueryRequest('series15'),limit=1)
        else:
            stream=q.iter_query(QueryRequest('series15'),page_size=1)
            first=next(stream); assert not isinstance(first,QueryReadReceipt); stream.close()


def test_no_full_stream_or_source_check_during_inspect_and_pages(tmp_path,catalog,monkeypatch):
    from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader
    from data_pipeline.analysis.country_events import snapshot_reader as component_reader
    rows,rt,b,p,d=fixture_index(tmp_path,catalog)
    def forbidden(*args,**kw): raise AssertionError('高频路径不得完整审计')
    monkeypatch.setattr(ComponentReader,'stream',forbidden)
    monkeypatch.setattr(ComponentReader,'check',forbidden)
    monkeypatch.setattr(component_reader,'upstream_binding',forbidden)
    d=inspect_country(b,p,runtime=rt)
    with open_qualified_country(select(d,incident().incident_id),p,runtime=rt,verify_qualification=fixture_qualification) as q:
        list(q.iter_query(QueryRequest('audit'),page_size=3))
        cold=q.access.stats['row_groups_read']
        list(q.iter_query(QueryRequest('audit'),page_size=3))
        assert q.access.stats['cache_hits']>0
        assert q.access.stats['row_groups_read']==cold


def test_highest_revision_missing_baseline_alias_and_full_resolution(tmp_path,catalog):
    from data_pipeline.analysis.country_events.models import Time
    events=[incident(),replace(incident(),revision=20),replace(incident(),incident_id='early',onset=Time(9))]
    rows=run_case(tmp_path,events=events)
    binding=persist_stream(iter(rows),catalog,tmp_path/'c3')
    heads=[r.value for r in rows if isinstance(r,c2.C2Row) and isinstance(r.value,c2.EventStatus)]
    aliases=tuple(dict(source='legacy-v1',table='event_metric',ref_kind='metric',id='same',incident_id=h.incident.incident_id,revision=h.incident.revision,cohort_id=h.cohort_id) for h in heads)
    rt=replace(runtime(catalog),aliases_typed=encode(aliases),aliases_source_json=canonical({'publication':'legacy-v1','version':'1'}))
    b,p=prepare_country_index(binding,reference_binding=None,runtime=rt,output=tmp_path/'c4');d=inspect_country(b,p,runtime=rt)
    assert read_country_event(d,incident_id=incident().incident_id).incident.revision==20
    with pytest.raises(ValueError): read_country_event(d,incident_id=incident().incident_id,revision=1)
    with open_qualified_country(select(d,'early'),p,runtime=rt,verify_qualification=fixture_qualification) as q:
        page=q.query(QueryRequest('asn_matrix'))
        assert page.total==0 and page.availability=='unavailable' and 'membership_unknown' in page.reasons
        req=QueryRequest('resolve_alias',canonical({'id':'same'}))
        first=q.query(req,limit=1)
        assert first.total==2 and first.availability=='ambiguous_reference' and first.next_cursor
        second=q.query(req,limit=1,cursor=first.next_cursor)
        assert len(second.items)==1 and second.next_cursor is None
        assert first.items!=second.items


def test_exact_rare_values_are_original_rows(tmp_path,catalog):
    samples=(21,22,23,24,25,26)
    rows=run_case(tmp_path,samples=samples)
    values=(0,Fraction(0),Fraction(1,2),Fraction(1,2**80),Fraction(2**128-1,2**80),None)
    n=0; altered=[]
    for item in rows:
        if isinstance(item,c2.C2Row) and isinstance(item.value,compute.MetricPoint) and item.value.metric=='fixed_visible_ipv6_slash48_equivalent':
            item=replace(item,value=replace(item.value,value=values[n]));n+=1
        altered.append(item)
    assert n==6
    binding=persist_stream(iter(altered),catalog,tmp_path/'c3')
    rt=runtime(catalog);b,p=prepare_country_index(binding,reference_binding=None,runtime=rt,output=tmp_path/'c4');d=inspect_country(b,p,runtime=rt)
    with open_qualified_country(select(d,incident().incident_id),p,runtime=rt,verify_qualification=fixture_qualification) as q:
        out=list(q.iter_query(QueryRequest('series15',canonical({'track':'fixed_visible_ipv6_slash48_count'})),page_size=1))[:-1]
        assert [exact_json(r['value'].value) for r in out]==[exact_json(v) for v in values]
        overview=q.query(QueryRequest('overview')).items[0]['tracks']['fixed_visible_ipv6_slash48_equivalent']
        assert overview['final_sample'].value is None
        assert overview['last_known'].sample_us==25000000


def test_fresh_process_all_formal_rows_and_independent_values(review_chain,catalog):
    from tests.country.test_country_saved_input import adapter
    from data_pipeline.analysis.country_events.snapshot_store import database_identity
    import subprocess,sys
    root=review_chain[0];c1=adapter(review_chain)
    grid=compute.Grid(100000000,125000000,(107000000,113000000,120000000,121000000,121500000,124000000,125000000))
    rows=list(c2.run_saved(c1,grid,scratch_root=root))
    binding=persist_stream(iter(rows),catalog,root/'fresh-c3',c1=c1,parameters={'grid':asdict(grid)})
    source=c1.identity['reference_sources']['as_info']
    reference=ReferenceBinding(**database_identity(c1.reader.dsn),run_id=c1.reader.run_id,snapshot=c1.reader.snapshot,
        manifest_sha256=digest(c1.reader.manifest),source_id=source['source_id'],content_sha256=source['source_id'],expected_rows=source['rows'])
    rt=runtime(catalog,c1);b,p=prepare_country_index(binding,reference_binding=reference,runtime=rt,output=root/'fresh-c4');d=inspect_country(b,p,runtime=rt)
    config=root/'fresh-request.json';target=root/'fresh-output.json'
    config.write_text(json.dumps(dict(descriptor=contract_json(d),component_dsn=catalog,output=str(target))))
    script=Path(__file__).with_name('country_query_fresh.py')
    import os
    env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parents[2])}
    run=subprocess.run([sys.executable,str(script),str(config)],capture_output=True,text=True,env=env)
    (root/'fresh.stdout').write_text(run.stdout);(root/'fresh.stderr').write_text(run.stderr)
    assert run.returncode==0,run.stderr
    data=json.loads(target.read_text())['results']
    from data_pipeline.analysis.country_events.snapshot_schema import decode
    from data_pipeline.analysis.country_events.incremental_types import AsnMetricPeak
    for head in (x.value for x in rows if isinstance(x,c2.C2Row) and isinstance(x.value,c2.EventStatus)):
        event=head.incident.incident_id
        for view,cls in [('asn_matrix',compute.AsnPoint),('asn_peaks',AsnMetricPeak),('path_samples',compute.PathSample)]:
            expected=sorted(encode(x.value) for x in rows if isinstance(x,c2.C2Row) and x.incident_id==event and isinstance(x.value,cls))
            for size in (1,7):
                actual=[decode(x) for x in data[f'{event}|{view}|{size}']]
                assert sorted(encode(x['value']) for x in actual)==expected
        for view in ('overview','series15','asns','asn_window','paths','audit'):
            assert data[f'{event}|{view}|1']==data[f'{event}|{view}|7']
        display_metrics={'interrupted_prefix_count','completely_interrupted_prefix_count','invisible_direction_count',
            'affected_asn_count','route_interrupted_asn_count','fixed_visible_ipv4_address_count','fixed_visible_ipv6_slash48_equivalent',
            'new_visible_ipv4_prefix_count','new_visible_ipv6_prefix_count','new_visible_ipv4_address_count','new_visible_ipv6_slash48_equivalent',
            'new_cumulative_ipv4_prefix_count','new_cumulative_ipv6_prefix_count','new_cumulative_ipv4_address_count','new_cumulative_ipv6_slash48_equivalent'}
        expected_series=sorted(encode(x.value) for x in rows if isinstance(x,c2.C2Row) and x.incident_id==event and isinstance(x.value,compute.MetricPoint) and x.value.metric in display_metrics)
        assert sorted(encode(decode(x)['value']) for x in data[f'{event}|series15|1'])==expected_series
        affected={x.value.asn for x in rows if isinstance(x,c2.C2Row) and x.incident_id==event and isinstance(x.value,compute.WindowClass) and x.value.afi==0 and x.value.classification in ('affected','route_interrupted')}
        expected_paths=sorted(encode(x.value) for x in rows if isinstance(x,c2.C2Row) and x.incident_id==event and isinstance(x.value,compute.PathSummary) and x.value.affected_asn in affected)
        assert sorted(encode(decode(x)['value']) for x in data[f'{event}|paths|1'])==expected_paths
        expected_windows=sorted(encode(x.value) for x in rows if isinstance(x,c2.C2Row) and x.incident_id==event and isinstance(x.value,compute.WindowClass))
        assert sorted(encode(decode(x)['window']) for x in data[f'{event}|asn_window|1'] if decode(x)['window'] is not None)==expected_windows
        audited=[decode(x) for x in data[f'{event}|audit|1']]
        assert sorted(encode(x['value']) for x in audited if x['incident_id']==event)==sorted(encode(x.value) for x in rows if isinstance(x,c2.C2Row) and x.incident_id==event)

        asns=[decode(x) for x in data[f'{event}|asns|1']]
        for row in asns:
            static=row['static'];asn=row['asn']
            if static['as_name']['state']=='available':
                assert static['as_name']['value']==f'AS{asn}'
                assert static['organization']['value']==f'Org{asn}'
                assert json.loads(static['raw_row']['raw_row'])[static['as_name']['column']]==f'AS{asn}'
                assert static['locator']['source_id']==reference.source_id
        overview=decode(data[f'{event}|overview|1'][0])
        for metric,summary in overview['tracks'].items():
            original=[x.value for x in rows if isinstance(x,c2.C2Row) and x.incident_id==event and isinstance(x.value,compute.MetricPoint) and x.value.metric==metric]
            assert summary['final_sample']==max(original,key=lambda x:x.sample_us)
            known=[x for x in original if x.value is not None]
            assert summary['last_known']==(max(known,key=lambda x:x.sample_us) if known else None)


def formal_index(prepared,catalog,name):
    from tests.country.test_country_saved_input import adapter
    from data_pipeline.analysis.country_events.snapshot_store import database_identity
    root=prepared[0];c1=adapter(prepared)
    grid=compute.Grid(100000000,125000000,(107000000,113000000,120000000,121000000,121500000,124000000,125000000))
    rows=list(c2.run_saved(c1,grid,scratch_root=root))
    binding=persist_stream(iter(rows),catalog,root/(name+'-c3'),c1=c1,parameters={'grid':asdict(grid)})
    source=c1.identity['reference_sources']['as_info']
    reference=ReferenceBinding(**database_identity(c1.reader.dsn),run_id=c1.reader.run_id,snapshot=c1.reader.snapshot,
        manifest_sha256=digest(c1.reader.manifest),source_id=source['source_id'],content_sha256=source['source_id'],expected_rows=source['rows'])
    rt=runtime(catalog,c1);b,p=prepare_country_index(binding,reference_binding=reference,runtime=rt,output=root/(name+'-c4'))
    return rows,rt,b,p,inspect_country(b,p,runtime=rt)


def test_static_first_row_empty_and_absent_are_actual_source(tmp_path,catalog,monkeypatch):
    import csv,os
    import tests.country.test_country_saved_input as source_module
    original=source_module.saved_reference_files
    def saved(root,rich=False):
        refs=original(root,rich=rich)
        entry=next(r for r in refs if r['role']=='as_info')
        path=Path(entry['path'])
        with path.open() as f: rows=list(csv.DictReader(f))
        for row in rows: row['as_name']='';row.pop('org_name',None)
        duplicates=[{**row,'as_name':'LATER_MUST_NOT_WIN'} for row in rows]
        with path.open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows+duplicates)
        entry['expected_rows']+=len(duplicates)
        entry['source_id']=entry['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
        return refs
    monkeypatch.setattr(source_module,'saved_reference_files',saved)
    prepared=source_module.build_pipeline(tmp_path,os.environ['DOMEYE_COUNTRY_C1_TEST_DSN'],source_module.multiple_country_mrt(tmp_path),rich=True,multicountry=True)
    rows,rt,b,p,d=formal_index(prepared,catalog,'first-row')
    head=next(r.value for r in rows if isinstance(r,c2.C2Row) and isinstance(r.value,c2.EventStatus))
    with open_qualified_country(select(d,head.incident.incident_id),p,runtime=rt,verify_qualification=fixture_qualification) as q:
        out=list(q.iter_query(QueryRequest('asns'),page_size=1))[:-1]
        assert out
        for row in out:
            assert row['static']['as_name']['value']=='' and row['static']['as_name']['state']=='empty'
            assert row['static']['organization']['state']=='field_absent'
            assert row['static']['raw_row']['row'] < len(out)+10
            assert 'LATER_MUST_NOT_WIN' not in row['static']['raw_row']['raw_row']


@pytest.fixture
def real_source_case(review_chain,catalog):
    import psycopg2,time
    from data_pipeline.analysis.country_events.selection_index import check_entities
    import uuid
    rows,rt,b,p,d=formal_index(review_chain,catalog,'qualification-'+uuid.uuid4().hex)
    c1=rt.c1;root=review_chain[0]
    controls=[(catalog,'country_components.components','component_id',b.component.component_id,'complete'),
              (c1.reader.dsn,'domeye.runs','run_id',c1.reader.run_id,'complete'),
              (c1.detection.dsn,'detection.runs','run_id',c1.detection.run_id,'complete'),
              (c1.reader.dsn,'domeye.reference_inputs','run_id',c1.reader.run_id,'validated')]
    calls=[0]
    @contextmanager
    def source_qualify(desc,*,lock=False,selection=None):
        # 明确fixture P；实际四类来源当前资格由本任务PG读取，无Detection全run COUNT。
        assert not lock
        for dsn,table,key,value,state in controls:
            with psycopg2.connect(dsn) as pg:
                pg.set_session(readonly=True)
                with pg.cursor() as cur:
                    cur.execute(f'SELECT state FROM {table} WHERE {key}=%s',(value,));calls[0]+=1
                    got=cur.fetchall()
                    if not got or any(r[0]!=state for r in got): raise ValueError('fixture_source_revoked')
        check_entities(desc.entities);yield
    head=next(r.value for r in rows if isinstance(r,c2.C2Row) and isinstance(r.value,c2.EventStatus))
    s=select(d,head.incident.incident_id)
    return rt,b,p,d,s,controls,source_qualify,calls,root


def test_real_source_revocation(real_source_case,monkeypatch):
    import psycopg2
    rt,b,p,d,s,controls,source_qualify,calls,root=real_source_case
    for dsn,table,key,value,state in controls:
        q=open_qualified_country(s,p,runtime=rt,verify_qualification=source_qualify)
        original=q.access.read
        def revoke(locs):
            result=original(locs)
            with psycopg2.connect(dsn) as pg,pg.cursor() as cur:cur.execute(f'UPDATE {table} SET state=%s WHERE {key}=%s',('failed',value))
            return result
        monkeypatch.setattr(q.access,'read',revoke)
        try:
            with pytest.raises(ValueError,match='revoked'):q.query(QueryRequest('series15'),limit=1)
        finally:
            q.close()
            with psycopg2.connect(dsn) as pg,pg.cursor() as cur:cur.execute(f'UPDATE {table} SET state=%s WHERE {key}=%s',(state,value))


def test_measured_query_cost(country_pg_log,real_source_case):
    import time
    rt,b,p,d,s,controls,source_qualify,calls,root=real_source_case
    measurements={};q=None
    try:
        for label in ('cold','warm'):
            offset=country_pg_log.start();start=time.monotonic();prior=calls[0]
            if label=='cold':q=open_qualified_country(s,p,runtime=rt,verify_qualification=source_qualify)
            q.query(QueryRequest('series15'),limit=2)
            elapsed=time.monotonic()-start
            snippet,count=country_pg_log.finish(offset)
            measurements[label]=dict(wall_seconds=str(elapsed),qualification_sql=calls[0]-prior,
                logged_statements=count,reader=dict(q.access.stats),query=dict(q.stats),
                log_scope='explicit_this_run_DSN_with_server_markers')
            (root/(label+'-query-pg.log')).write_text(snippet)
            assert 'SELECT count(*) FROM detection.records' not in snippet
    finally:
        if q:q.close()
    (root/'query-costs.json').write_text(json.dumps(measurements))


@pytest.mark.parametrize('case',['unknown','no_samples'])
def test_unknown_and_no_samples_do_not_make_zero(tmp_path,catalog,case):
    from tests.country.test_country_enhancement_compute import change
    from data_pipeline.analysis.country_events.models import Time
    kwargs={'changes':[change(0,Time(23),presence='absent',path_ref=None)],'samples':(23.5,)} if case=='unknown' else {'events':[replace(incident(),onset=Time(30),detected_at=Time(30))],'samples':(21,26)}
    rows,rt,b,p,d=fixture_index(tmp_path,catalog,**kwargs)
    with open_qualified_country(select(d,incident().incident_id),p,runtime=rt,verify_qualification=fixture_qualification) as q:
        overview=q.query(QueryRequest('overview')).items[0]
        if case=='unknown':
            for x in overview['tracks'].values():
                assert x['exact_peak'] is None
            for x in list(q.iter_query(QueryRequest('asn_peaks'),page_size=1))[:-1]:
                assert x['value'].exact_peak is None
        else:
            assert overview['sample_count']==0 and overview['view_reason']=='no_samples'
            assert q.query(QueryRequest('series15')).total==0


@pytest.mark.parametrize('field',['max_row_bytes','max_rss_bytes','max_disk_bytes'])
def test_failed_build_does_not_admit_or_rebuild(tmp_path,catalog,field):
    rows=run_case(tmp_path);component=persist_stream(iter(rows),catalog,tmp_path/'c3')
    output=tmp_path/'failed'
    with pytest.raises(ValueError):prepare_country_index(component,reference_binding=None,runtime=runtime(catalog),output=output,limits=replace(QueryLimits(),**{field:1}))
    assert not (output/'ready.json').exists()
    # 输出目录保留失败现场，不由另一次构建或查询覆盖。
    with pytest.raises(FileExistsError):prepare_country_index(component,reference_binding=None,runtime=runtime(catalog),output=output)


def test_locator_corruption_and_filter_cursor_rejected(tmp_path,catalog):
    import sqlite3
    rows,rt,b,p,d=fixture_index(tmp_path,catalog)
    selection=select(d,incident().incident_id)
    with open_qualified_country(selection,p,runtime=rt,verify_qualification=fixture_qualification) as q:
        first=q.query(QueryRequest('series15'),limit=1)
        with pytest.raises(ValueError):q.query(QueryRequest('series15',canonical({'track':'affected_asn_count'})),cursor=first.next_cursor,limit=1)
        page=q.query(QueryRequest('asn_matrix',canonical({'asn':4294967295})))
        assert page.total==0 and 'not_member' in page.reasons
        ref=next(x.value.observation_ref for x in rows if isinstance(x,c2.C2Row) and isinstance(x.value,compute.PathSample))
        found=q.query(QueryRequest('resolve_source',canonical({'ref':ref})))
        assert found.total>0
        db=sqlite3.connect(Path(b.root)/'index.sqlite');db.execute('UPDATE rows SET row_offset=row_offset+1');db.commit();db.close()
        with pytest.raises(ValueError):q.query(QueryRequest('series15'))


def test_saved_complete_empty_has_zero_events_with_receipt(tmp_path,catalog):
    import os
    from tests.country.test_country_saved_input import build_pipeline
    from tests.observations.test_observation_consumer import observation_fixture
    from data_pipeline.analysis.country_events.selection_index import iter_country_events
    prepared=build_pipeline(tmp_path,os.environ['DOMEYE_COUNTRY_C1_TEST_DSN'],observation_fixture(tmp_path),rich=False)
    rows,rt,b,p,d=formal_index(prepared,catalog,'empty')
    output=list(iter_country_events(d))
    assert len(output)==1 and isinstance(output[0],QueryReadReceipt) and output[0].rows==0
    with pytest.raises(ValueError,match='unknown_incident'):read_country_event(d,incident_id='not-there')


def test_typed_legal_empty_alias_and_event_stream(tmp_path,catalog):
    from data_pipeline.analysis.country_events.selection_index import iter_country_events, iter_country_aliases
    rows=run_case(tmp_path);component=persist_stream(iter(rows),catalog,tmp_path/'c3')
    rt=replace(runtime(catalog),aliases_typed=encode(()),aliases_source_json=canonical({'publication':'legacy-complete','version':'1'}))
    b,p=prepare_country_index(component,reference_binding=None,runtime=rt,output=tmp_path/'c4');d=inspect_country(b,p,runtime=rt)
    aliases=list(iter_country_aliases(d)); assert len(aliases)==1 and aliases[0].rows==0
    events=list(iter_country_events(d));assert len(events)==2 and events[-1].rows==1
    with open_qualified_country(select(d,incident().incident_id),p,runtime=rt,verify_qualification=fixture_qualification) as q:
        result=q.query(QueryRequest('resolve_alias',canonical({'id':'none'})))
        assert result.availability=='not_found' and result.total==0 and result.items==()


def test_final_cache_read_budgets_and_unavailable_overview(tmp_path,catalog):
    from data_pipeline.analysis.country_events.models import Time
    rows,rt,b,p,d=fixture_index(tmp_path,catalog,events=[incident(),replace(incident(),incident_id='early',onset=Time(9))])
    s=select(d,incident().incident_id)
    with pytest.raises(ValueError,match='footer_cache'):
        open_qualified_country(s,p,runtime=rt,verify_qualification=fixture_qualification,limits=replace(QueryLimits(),max_cache_bytes=1))
    with open_qualified_country(s,p,runtime=rt,verify_qualification=fixture_qualification,limits=replace(QueryLimits(),max_read_bytes=1)) as q:
        with pytest.raises(ValueError,match='read_bytes'):q.query(QueryRequest('series15'),limit=1)
    with open_qualified_country(select(d,'early'),p,runtime=rt,verify_qualification=fixture_qualification) as q:
        out=q.query(QueryRequest('overview')).items[0]
        assert out['path_count'] is None and out['asn_window_counts'] is None and out['sample_count'] is None
