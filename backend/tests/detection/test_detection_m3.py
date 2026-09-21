"""Detection M3独立资格手算及自有M2/PG真实接缝。"""
from dataclasses import replace
import json
from types import SimpleNamespace
import pytest
from tests.detection.test_detection_computation import engine, obs
from tests.observations.test_observation_ordered import message, binding as ordered_binding
from data_pipeline.bgp.ordered_reader import _boundary
from data_pipeline.analysis.detection.qualification import Qualification
from data_pipeline.analysis.detection.qualified_reader import qualify_vp_state


def gap_boundary(direction='local', endpoint=True):
    raw, elements = message('source', 1, status='rejected', direction=direction, endpoint=endpoint)
    b = ordered_binding([('source', [(raw,elements)])])
    source = b.sources[0]
    return b, _boundary(raw, b, 0, source)


def test_independent_qualification_no_revision_and_local_effect():
    e = engine()
    before = len(e.output.rows)
    e.consume(obs(1,path='100 2'))
    assert len(e.output.rows) > before
    b, boundary = gap_boundary()
    saved=[]
    q=Qualification(b,e.scope,'component-run',saved.append)
    q.source_id=b.sources[0].source_id
    raw=dict(kind='business_revision',incident_id='event',revision=1,legacy={'e_time':None})
    q.observe_output(raw)
    q.boundary(boundary)
    assert [r['kind'] for r in saved]==['event_qualification','scope_gap','event_qualification']
    assert saved[0]['payload_json']['coverage']=='complete'
    assert saved[-1]['revision']==1 and saved[-1]['payload_json']['coverage']=='unknown'
    assert raw['revision']==1
    assert saved[-1]['payload_json']['effective_position']==boundary.position.sort_key
    assert 'origin_anchor' in saved[-1]['payload_json']['dimensions']
    assert 'prefix_leak_suppression' in saved[-1]['payload_json']['dimensions']
    q.observe_output(dict(raw,revision=2))
    assert saved[-1]['payload_json']['coverage']=='unknown'
    q.source_complete({'raw':{'source_id':q.source_id}})
    assert saved[-1]['kind']=='source_coverage'
    # 单对象后来恢复不清除前缀级抑制、峰值/锚点历史，资格层没有清Gap操作。
    assert len(q.gaps)==1
    gap=saved[1]['payload_json']
    assert qualify_vp_state([gap],legacy_vp='64497',at_position=(0,2,1,0))['coverage']=='unknown'
    assert qualify_vp_state([gap],legacy_vp='64498',at_position=(0,2,1,0))['coverage']=='complete'
    assert qualify_vp_state([gap],legacy_vp='64497',at_position=(0,0,0,0))['coverage']=='complete'


def test_empty_unknown_scope_has_coverage():
    b, boundary=gap_boundary('unknown',False)
    saved=[];q=Qualification(b,engine().scope,'run',saved.append)
    q.source_id=b.sources[0].source_id
    q.boundary(boundary);q.source_complete({'raw':{'source_id':q.source_id}})
    assert [r['kind'] for r in saved]==['scope_gap','source_coverage']
    assert saved[0]['payload_json']['dependency_scope']['legacy_vp'] is None
    assert qualify_vp_state([saved[0]['payload_json']],legacy_vp='999',at_position=(1,0,0,0))['coverage']=='unknown'


def check_m3_readers(dsn, result, bid, coverage, tmp_path):
    import os,sys,subprocess
    import psycopg2
    from pathlib import Path
    from data_pipeline.analysis.detection.qualified_reader import read_coverage
    from data_pipeline.analysis.detection.store import connect_duckdb, literal
    run,snapshot=result['run_id'],result['snapshot']
    db=connect_duckdb()
    try:
        db.execute('LOAD ducklake');db.execute('LOAD postgres')
        db.execute('ATTACH '+literal('ducklake:postgres:'+dsn)+' AS lake (READ_ONLY)')
        r=db.execute(f'SELECT * FROM lake.det_{run}.m3_entries AT (VERSION => {snapshot}) ORDER BY ordinal')
        names=[c[0] for c in r.description]
        assert coverage == [dict(zip(names,row)) for row in r.fetchall()]
    finally: db.close()
    pg=psycopg2.connect(dsn);pg.autocommit=True
    try:
        with pg.cursor() as c:
            c.execute('SELECT ordinal,entry_id,kind,incident_id,revision,source_id,gap_id,payload_json FROM detection.m3_entries WHERE run_id=%s ORDER BY ordinal',(run,))
            assert coverage==[dict(zip(names,row)) for row in c.fetchall()]
        with pytest.raises(ValueError,match='绑定'):
            list(read_coverage(dsn,run,snapshot,expected_binding_id='wrong'))
        with pytest.raises(ValueError):
            list(read_coverage(dsn,run,snapshot+1,expected_binding_id=bid))
        stream=read_coverage(dsn,run,snapshot,expected_binding_id=bid)
        next(stream)
        with pg.cursor() as c: c.execute("UPDATE detection.runs SET state='failed' WHERE run_id=%s",(run,))
        try:
            with pytest.raises(ValueError): list(stream)
        finally:
            stream.close()
            with pg.cursor() as c: c.execute("UPDATE detection.runs SET state='complete' WHERE run_id=%s",(run,))
        stream=read_coverage(dsn,run,snapshot,expected_binding_id=bid)
        next(stream);stream.close()
    finally: pg.close()
    config=tmp_path/'reader-request.json'
    config.write_text(json.dumps(dict(dsn=dsn,run=run,snapshot=snapshot,binding=bid)))
    code='''import sys,json
sys.path.insert(0,sys.argv[1])
from data_pipeline.analysis.detection.qualified_reader import read_coverage, read_qualified_revisions, read_qualified_records, read_qualified_state_entries, read_binding
from data_pipeline.analysis.detection.store import read_stored_rows
c=json.load(open(sys.argv[2]))
asof=read_binding(c['dsn'],c['run'],c['snapshot'])['identity']['qualification_as_of_position']
rows=list(read_coverage(c['dsn'],c['run'],c['snapshot'],expected_binding_id=c['binding']))
qualified_raw=list(read_qualified_records(c['dsn'],c['run'],c['snapshot'],expected_binding_id=c['binding'],at_position=asof))
qualified_state=list(read_qualified_state_entries(c['dsn'],c['run'],c['snapshot'],expected_binding_id=c['binding']))
assert qualified_raw and qualified_state
if any(r['kind']=='scope_gap' for r in rows): assert all(r['main'] is None for r in qualified_state)
else: assert all(r['main']==r['raw'] for r in qualified_state)
print(json.dumps({'coverage':len(rows),'records':len(list(read_stored_rows(c['dsn'],c['run'],c['snapshot'],'records'))),'state':len(list(read_stored_rows(c['dsn'],c['run'],c['snapshot'],'state_entries'))),'qualified':len(list(read_qualified_revisions(c['dsn'],c['run'],c['snapshot'],expected_binding_id=c['binding'],at_position=asof)))}))
'''
    process=subprocess.run([sys.executable,'-I','-c',code,str(Path(__file__).resolve().parents[2]),str(config)],capture_output=True,text=True)
    assert process.returncode==0,process.stderr
    (tmp_path/'fresh-reader.json').write_text(process.stdout)
    assert json.loads(process.stdout)['coverage']==len(coverage)


def test_local_a_w_hand_calculation_preserves_raw_state():
    from data_pipeline.analysis.detection.adapter import adapt_element
    from tests.detection.test_detection_computation import P
    def local(n, action):
        _, rows=message('source',n,direction='local',elements=1)
        row=rows[0]
        row.update(prefix=P,raw_prefix=b'\x18\x0a\0\0',as_path_text='200 2',peer_asn=200,
                   action=action,epoch=1772236800+n)
        item=adapt_element(row,run_id='run',snapshot=1,collector_id='fixture-collector')
        return replace(item,source_version='fixture-v1')
    e=engine(vps=1)
    e.consume(local(1,'announce'))
    assert e.status=='computed'
    assert e.projection.prefix_as[P]=={'1','2'}
    assert e.projection.prefix_dict[P]=={'100':'100 1','200':'200 2'}
    assert e.hijack.prefix_event[P]['is_moas_now'] is True
    moas=[r for r in e.output.rows if r['kind']=='business_revision' and r.get('event_kind')=='moas']
    assert len(moas)==1
    b,g=gap_boundary()
    saved=[];q=Qualification(b,e.scope,'run',saved.append)
    q.source_id='source';q.observe_output(moas[-1])
    old=e.export_state()
    q.boundary(g)
    assert e.export_state()==old  # Gap不修改活动事件、锚点、峰值/抑制及任何raw态。
    e.consume(local(2,'withdraw'))
    assert e.status=='computed' and e.projection.prefix_as[P]=={'1'}
    assert e.hijack.prefix_event[P]['is_moas_now'] is False
    assert any(r['legacy'].get('e_time') for r in e.output.rows if r['kind']=='business_revision' and r.get('event_kind')=='moas')
    empty=engine(vps=1);before=empty.export_state()
    q2=Qualification(b,empty.scope,'run2',lambda r:None);q2.boundary(g)
    assert empty.export_state()==before and empty.projection.prefix_as[P]=={'1'}


def check_m3_finish_rejections(request, tmp_path, monkeypatch):
    """复用本次自己的封存人工输入，实际撤回/漏覆盖禁止完成。"""
    from pathlib import Path
    from dataclasses import asdict
    import psycopg2
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.analysis.detection.reference_view import ReferenceView, ReferenceSource
    from data_pipeline.analysis.detection.models import DetectionScope, FileBoundary
    from data_pipeline.analysis.detection.ordered_runner import run
    from data_pipeline.analysis.detection.qualification_store import M3Store
    from data_pipeline.analysis.detection.identity import identity_builder
    for fault in ('revoke','missing_coverage','retarget','wrong_schema','pg_target'):
        import uuid
        admin=psycopg2.connect(request['detection_dsn']);admin.autocommit=True
        database='det_m3_fault_'+uuid.uuid4().hex
        try:
            with admin.cursor() as c: c.execute("CREATE DATABASE "+database+" ENCODING 'UTF8' TEMPLATE template0")
        finally: admin.close()
        output_dsn=request['detection_dsn']+' dbname='+database
        reader=ObservationReader(request['observation_dsn'],request['input_run'],request['input_snapshot'],request['ordered_sources'],profile='observation')
        refs=ReferenceView(request['reference_version'],f'{reader.run_id}:{reader.snapshot}',tmp_path/('refs-'+fault))
        for s in request['references']:
            refs.add(ReferenceSource(s['role'],s['source_id'],s['expected_rows'],
                (row for batch in reader.reference_batches(s['source_id']) for row in batch.to_pylist())))
        original=M3Store.finish
        def finish(store, **kwargs):
            if fault=='revoke':
                pg=psycopg2.connect(request['observation_dsn'])
                try:
                    with pg,pg.cursor() as c:
                        c.execute("UPDATE observation_m2.runs SET state='failed' WHERE run_id=%s",(reader.run_id,))
                finally:pg.close()
            elif fault=='wrong_schema':
                store.identity['store_schema_version']='wrong/v999'
            else:
                store.flush_qualification()
                if fault=='missing_coverage':
                    store.db.execute(f"DELETE FROM lake.{store.schema}.m3_entries WHERE kind='source_coverage'")
                else:
                    rows=store.db.execute(f"SELECT ordinal,incident_id,revision FROM lake.{store.schema}.m3_entries WHERE kind='event_qualification' ORDER BY ordinal").fetchall()
                    from collections import Counter
                    counts=Counter((r[1],r[2]) for r in rows)
                    first=next(r for r in rows if counts[r[1],r[2]]>1)
                    other=next(r for r in rows if r[1:]!=first[1:])
                    if fault!='pg_target':
                        store.db.execute(f'UPDATE lake.{store.schema}.m3_entries SET incident_id=?,revision=? WHERE ordinal=?',[*other[1:],first[0]])
                    with store.pg,store.pg.cursor() as c:
                        c.execute('UPDATE detection.m3_entries SET incident_id=%s,revision=%s WHERE run_id=%s AND ordinal=%s',(*other[1:],store.run_id,first[0]))
            return original(store,**kwargs)
        output=tmp_path/('reject-'+fault)
        try:
            with monkeypatch.context() as m:
                m.setattr(M3Store,'finish',finish)
                with pytest.raises(ValueError):
                    run(reader,refs,DetectionScope(**request['scope']),
                        {k:FileBoundary(**v) for k,v in request['boundaries'].items()},
                        output_dsn,output,root=Path(__file__).resolve().parents[3],
                        identity_builder=identity_builder,fixture_only=True,min_free_bytes=0)
            assert not (output/'ready.json').exists()
            pgout=psycopg2.connect(output_dsn)
            try:
                with pgout.cursor() as c:
                    c.execute('SELECT state FROM detection.runs')
                    assert c.fetchall()==[('failed',)]
            finally: pgout.close()
        finally:
            pg=psycopg2.connect(request['observation_dsn'])
            try:
                with pg,pg.cursor() as c:
                    c.execute("UPDATE observation_m2.runs SET state='observation_sealed' WHERE run_id=%s",(reader.run_id,))
            finally:pg.close()
