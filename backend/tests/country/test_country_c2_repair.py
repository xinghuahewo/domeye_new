"""d45edf 两项原等级反例：自有人工输入，不改独立审核制品。"""
from dataclasses import replace
import json
import pytest

from data_pipeline.analysis.country_events.event_aggregation import C2Completion, C2Row, EventStatus
from data_pipeline.analysis.country_events.aggregation_staging import Stage, load
from data_pipeline.analysis.country_events.saved_input import SavedBatch, InputCompletion
from data_pipeline.analysis.detection.reference_view import ReferenceView
from tests.country.test_country_c2 import run_case, payload
from tests.country.test_country_enhancement_cohort import route, binding, incident
from tests.country.test_country_enhancement_compute import change
from tests.detection.test_detection_reference_view import csv_source


def test_standards_p2_dynamic_peer_cannot_exceed_live_member_limit(tmp_path):
    added=route('new',endpoint=replace(route().endpoint,remote_ip='192.0.2.99'))
    with pytest.raises(ValueError,match='resource_limit:C2_active_members'):
        run_case(tmp_path,changes=[change(0,23,added)],max_total_members=1)
    assert not list(tmp_path.glob('country-c2-*'))


@pytest.mark.parametrize('cells',[
    [['asn','as_country'],['2','ZZ'],['2','YY']],
    [['asn','as_country'],['2',''],['2','ZZ']],
    [['asn','as_country'],['002','ZZ'],['2','YY']],
    [['asn','as_country'],['002','ZZ'],['2','YY'],['text','ZZ']],
    [['asn','as_country'],['2.0','ZZ'],['3.0','YY']],
])
def test_spec_p2_same_reference_interpretation_and_selected_locator(tmp_path,cells):
    source=csv_source('as_info',cells)
    rows=tuple(source.rows)
    view=ReferenceView('v','fixture-run:snapshot-1',tmp_path/'detection-view')
    view.add(replace(source,rows=iter(rows)))
    stage=Stage(tmp_path/'stage.sqlite',binding(),lambda:None,
                dict(max_row_bytes=100000,max_staged_rows=100,max_changes=100,max_objects=100))
    done=InputCompletion('fixture-run',1,'detection',1,{},(),'complete_empty','unknown',0,'UTC','state','hash',{}, {})
    try:
        stage.consume_c1(iter((SavedBatch('references',rows,'fixture-run',1),done)),'as_info')
        for asn in (2,3):
            selected=view.mapping('as_info').get(str(asn))
            actual=stage.refs.get(asn)
            if selected is None:
                assert actual is None
            else:
                country=selected.get('as_country')
                assert actual.country==(country if country else None)
                assert actual.state==('known' if country else 'unknown')
                assert actual.source_ref.endswith('/'+str(view.selection('as_info',str(asn))['row']))
        saved=[load(r[0]) for r in stage.db.execute("SELECT payload FROM evidence WHERE kind='as_info' ORDER BY CAST(key AS INTEGER)")]
        assert saved==list(rows)
    finally:stage.close()


def new_peer():
    return route('new',endpoint=replace(route().endpoint,remote_ip='192.0.2.99'))


def test_standards_p2_equality_legal_and_outputs_unchanged(tmp_path):
    from collections import Counter
    changes=[change(0,23,new_peer())]
    rows=run_case(tmp_path,changes=changes,max_total_members=2)
    unlimited=run_case(tmp_path,changes=changes)
    assert Counter(repr(r) for r in rows if isinstance(r,C2Row))==Counter(repr(r) for r in unlimited if isinstance(r,C2Row))
    assert rows[-1].costs['peak_active_members']==2
    assert rows[-1].costs['active_member_reservations']==rows[-1].costs['released_active_members']==2


def test_standards_p2_same_object_updates_and_state_do_not_allocate(tmp_path):
    from data_pipeline.analysis.country_events.compute import Change, Path, Segment
    from data_pipeline.analysis.country_events.models import Time, Cursor
    from tests.country.test_country_enhancement_compute import paths
    from tests.country.test_country_enhancement_cohort import refs
    from data_pipeline.analysis.country_events.event_aggregation import run_typed
    from data_pipeline.analysis.country_events.compute import Grid
    seed=route()
    updates=[change(0,21,path_ref='other'),change(1,22,path_ref='other'),
             change(2,23,presence='absent',path_ref=None),
             Change(Cursor(1,3,0),Time(24,0),'u1','state',invalidated_objects=(seed.object_id,)),
             change(4,25)]
    rows=list(run_typed(binding(),Grid(15000000,31000000,(26000000,31000000)),[incident()],
        [Change(seed.cursor,seed.observed_at,'rib',seed.observation_ref,seed)]+updates,
        (*paths(),Path('other',(Segment('sequence',(64497,64498)),),b'other')),refs(),scratch_root=tmp_path,max_total_members=1))
    assert isinstance(rows[-1],C2Completion)
    assert rows[-1].costs['active_member_reservations']==rows[-1].costs['peak_active_members']==1


def test_standards_p2_withdraw_retained_object_still_occupies_slot(tmp_path):
    with pytest.raises(ValueError,match='resource_limit:C2_active_members'):
        run_case(tmp_path,changes=[change(0,21,presence='absent',path_ref=None),change(1,23,new_peer())],max_total_members=1)
    assert not list(tmp_path.glob('country-c2-*'))


def test_standards_p2_same_object_in_two_events_counts_twice(tmp_path):
    events=[incident(),replace(incident(),incident_id='second')]
    with pytest.raises(ValueError,match='resource_limit:C2_active_members'):
        run_case(tmp_path,events=events,changes=[change(0,23,new_peer())],max_total_members=3)
    rows=run_case(tmp_path,events=events,changes=[change(0,23,new_peer())],max_total_members=4)
    assert rows[-1].costs['peak_active_members']==4
    assert rows[-1].costs['released_active_members']==4
    assert rows[-1].costs['state_assignments']==2  # 全局SQLite只存两对象，不混成活跃占用。


def test_standards_p2_release_actual_dynamic_occupancy(tmp_path):
    from data_pipeline.analysis.country_events.models import Time
    first=replace(incident(),end=Time(24,0))
    second=replace(incident(),incident_id='second',onset=Time(27,0))
    rows=run_case(tmp_path,events=[first,second],changes=[change(0,23,new_peer())],max_total_members=2,max_active=1)
    assert len(payload(rows,EventStatus))==2
    assert rows[-1].costs['active_member_reservations']==rows[-1].costs['released_active_members']==4
    assert rows[-1].costs['peak_active_members']==2
    assert not list(tmp_path.glob('country-c2-*'))


@pytest.mark.parametrize('cells',[
    [['asn','as_country'],['002','ZZ'],['2','YY']],
    [['asn','as_country'],['002','ZZ'],['2','YY'],['text','ZZ']],
    [['asn','as_country'],['2.0','ZZ'],['3.0','YY']],
    [['asn','as_country'],['2',''],['2','ZZ']],
])
def test_spec_p2_detection_sink_same_record_and_all_locators(tmp_path,cells):
    source=csv_source('as_info',cells);rows=tuple(source.rows)
    ordinary=ReferenceView('v','r:1',tmp_path/'ordinary')
    streamed=ReferenceView('v','r:1',tmp_path/'streamed')
    ordinary.add(replace(source,rows=iter(rows)))
    actual=[]
    streamed.add(replace(source,rows=iter(rows)),selected_sink=lambda *record:actual.append(record))
    expected=[(key,record,ordinary.selection('as_info',key)) for key,record in ordinary.mapping('as_info').items()]
    assert actual==expected
    with pytest.raises(ValueError,match='sink'):streamed.mapping('as_info')
    with pytest.raises(ValueError,match='sink'):streamed.selection('as_info','2')
    with pytest.raises(ValueError,match='sink'):streamed.take_info()


@pytest.mark.parametrize('first_country',['ZZ',''])
def test_spec_p2_formal_duplicate_original_reference_chain(tmp_path,monkeypatch,first_country):
    import os,csv,hashlib
    from pathlib import Path
    import tests.country.test_country_saved_input as helper
    import data_pipeline.analysis.country_events.event_aggregation as c2
    from data_pipeline.analysis.country_events.compute import Grid
    from data_pipeline.analysis.detection.reference_view import ReferenceSource
    base=os.environ.get('DOMEYE_COUNTRY_C1_TEST_DSN')
    if not base:pytest.skip('仅自有UTF8 socket PG')
    original=helper.saved_reference_files
    def duplicate(*args,**kwargs):
        sources=original(*args,**kwargs)
        entry=next(r for r in sources if r['role']=='as_info');path=Path(entry['path'])
        with path.open(newline='') as stream:records=list(csv.DictReader(stream))
        first=next(r for r in records if r['asn']=='2');first['as_country']=first_country
        last=dict(first);last['as_country']='YY' if first_country else 'ZZ'
        records.append(last)
        with path.open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(records[0]));writer.writeheader();writer.writerows(records)
        entry['expected_rows']+=1
        entry['sha256']=entry['source_id']=hashlib.sha256(path.read_bytes()).hexdigest()
        return sources
    monkeypatch.setattr(helper,'saved_reference_files',duplicate)
    prepared=helper.build_pipeline(tmp_path,base,helper.multiple_country_mrt(tmp_path),rich=True,multicountry=True)
    before=list(helper.adapter(prepared).stream())
    source=next(r for r in json.loads((tmp_path/'request.json').read_text())['references'] if r['role']=='as_info')
    all_rows=tuple(r for b in before if isinstance(b,SavedBatch) and b.table=='references' for r in b.rows if r['source_id']==source['source_id'])
    assert len(all_rows)==source['expected_rows']
    detector=ReferenceView('country-artificial-v1',f'{prepared[3]["run_id"]}:{prepared[3]["snapshot"]}',tmp_path/'detector-view')
    detector.add(ReferenceSource('as_info',source['source_id'],len(all_rows),iter(all_rows)))
    assert detector.mapping('as_info')['2']['as_country']==first_country
    original_consume=Stage.consume_c1;selected=[]
    def verify(stage,*args,**kwargs):
        original_consume(stage,*args,**kwargs)
        saved=tuple(load(r[0]) for r in stage.db.execute("SELECT payload FROM evidence WHERE kind='as_info' ORDER BY CAST(key AS INTEGER)"))
        assert saved==all_rows  # 重复和原序全部保留，不只保留获选行。
        actual=stage.refs[2];locator=load(stage.db.execute("SELECT payload FROM dictionaries WHERE kind='reference_selection' AND key='2'").fetchone()[0])
        assert locator==detector.selection('as_info','2')
        assert actual.country==(first_country or None)
        assert actual.source_ref.endswith('/'+str(locator['row']))
        selected.append(dict(country=actual.country,source_ref=actual.source_ref,locator=locator))
    monkeypatch.setattr(Stage,'consume_c1',verify)
    rows=list(c2.run_saved(helper.adapter(prepared),Grid(100000000,120000000,(107000000,113000000)),scratch_root=tmp_path))
    headers=payload(rows,EventStatus)
    if first_country:
        zz=next(h for h in headers if h.incident.country=='ZZ')
        assert zz.incident.onset.epoch==106 and zz.fixed_prefix_count==zz.direction_count==1
        located=next(r for r in zz.reference_selections if r[0]==2)
        assert located[2]==detector.selection('as_info','2')
        assert located[1].endswith('/'+str(located[2]['row']))
    assert rows[-1].reference_interpretation['original_rows']==len(all_rows)
    (tmp_path/'repair-evidence.json').write_text(json.dumps(dict(selected=selected,original_rows=len(all_rows),
        rows_sha256=hashlib.sha256(json.dumps(all_rows,sort_keys=True,default=str).encode()).hexdigest(),
        events=[dict(country=h.incident.country,onset=h.incident.onset.epoch,fixed=h.fixed_prefix_count,directions=h.direction_count) for h in headers],costs=rows[-1].costs),indent=2,ensure_ascii=False))
    assert not list(tmp_path.glob('country-c2-*'))


from tests.country.test_country_c1_repair import review_chain


def test_repair_normal_formal_batches_and_actual_sql_cost(review_chain,monkeypatch):
    from collections import Counter
    import time
    import data_pipeline.analysis.country_events.event_aggregation as c2
    from data_pipeline.analysis.country_events.compute import Grid
    from tests.country.test_country_saved_input import adapter
    original_init=Stage.__init__;sql=[]
    def trace_init(stage,*args,**kwargs):
        original_init(stage,*args,**kwargs)
        stage.db.set_trace_callback(sql.append)
    monkeypatch.setattr(Stage,'__init__',trace_init)
    outputs=[];costs={}
    for batch in (1,2,256):
        sql.clear();start=time.monotonic()
        rows=list(c2.run_saved(adapter(review_chain,batch_rows=batch),
            Grid(100000000,125000000,(107000000,113000000,120000000,121000000,121500000,124000000,125000000)),
            scratch_root=review_chain[0],batch_rows=batch))
        costs[batch]=dict(rows[-1].costs,including_c1_constructor_seconds=time.monotonic()-start,
                          actual_sql_statements=len(sql),global_scan_selects=sum(s=='SELECT i,payload FROM units ORDER BY i' for s in sql))
        heads=payload(rows,EventStatus)
        assert sorted(h.incident.onset.epoch for h in heads)==[106,112]
        assert all(h.fixed_prefix_count==h.direction_count==1 for h in heads)
        assert rows[-1].input_completion.counts['changes']==25
        assert rows[-1].costs['staged_invalidations']==9
        assert rows[-1].costs['global_units_scanned']==27
        assert rows[-1].costs['state_assignments']==34
        assert rows[-1].costs['related_changes_sent']==7
        assert rows[-1].costs['output_rows']==410
        assert len(rows)==411 and sum(isinstance(r,C2Row) for r in rows)==410
        assert costs[batch]['global_scan_selects']==1
        outputs.append(Counter(repr(r) for r in rows if isinstance(r,C2Row)))
    assert outputs[0]==outputs[1]==outputs[2]
    (review_chain[0]/'repair-normal-costs.json').write_text(json.dumps(costs,indent=2))


@pytest.mark.parametrize('revocation',['detection','receipt'])
def test_repair_preserves_late_qualification_check(review_chain,revocation):
    import psycopg2
    from data_pipeline.analysis.country_events.event_aggregation import run_saved
    from data_pipeline.analysis.country_events.compute import Grid
    from tests.country.test_country_saved_input import adapter
    c1=adapter(review_chain)
    stream=run_saved(c1,Grid(100000000,125000000,(113000000,125000000)),scratch_root=review_chain[0])
    path=review_chain[0]/'observation/execution.json';raw=path.read_bytes()
    seen=[]
    try:
        seen.append(next(stream))
        if revocation=='receipt':path.write_bytes(raw+b'\n')
        else:
            with psycopg2.connect(review_chain[1][1]) as db:
                with db.cursor() as cur:cur.execute("UPDATE detection.runs SET state='failed' WHERE run_id=%s",(c1.detection.run_id,))
        error='production_receipt_digest_mismatch' if revocation=='receipt' else 'detection_qualification_mismatch'
        with pytest.raises(ValueError,match=error):
            for row in stream:seen.append(row)
        assert not any(isinstance(r,C2Completion) for r in seen)
    finally:
        stream.close();path.write_bytes(raw)
        if revocation=='detection':
            with psycopg2.connect(review_chain[1][1]) as db:
                with db.cursor() as cur:cur.execute("UPDATE detection.runs SET state='complete' WHERE run_id=%s",(c1.detection.run_id,))
    assert not list(review_chain[0].glob('country-c[12]-*'))
