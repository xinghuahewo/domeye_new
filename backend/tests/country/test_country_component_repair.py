"""e433cd3三项P2的本任务人工反例，不使用审阅者数据库。"""
from dataclasses import replace,asdict
from pathlib import Path
import json
import subprocess
import sys
import time
import pytest

from data_pipeline.analysis.country_events import event_aggregation as c2, compute
from data_pipeline.analysis.country_events.snapshot_store import persist_stream
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader
from data_pipeline.analysis.country_events.incremental_types import DirectionPoint
from data_pipeline.analysis.country_events.models import Time
from tests.country.test_country_component import catalog, read_all
from tests.country.test_country_c1_repair import review_chain
from tests.country.test_country_c2 import run_case
from tests.country.test_country_enhancement_cohort import binding, incident, route, refs
from tests.country.test_country_enhancement_compute import paths, change
from tests.country.test_country_saved_input import adapter


def test_started_real_c2_stream_closed_when_root_exists(tmp_path):
    r=route();grid=compute.Grid(15000000,31000000,(21000000,))
    stream=c2.run_typed(binding(),grid,[incident()],[compute.Change(r.cursor,r.observed_at,binding().ordered_sources[0],r.observation_ref,r)],paths(),refs(),scratch_root=tmp_path)
    next(stream)
    target=tmp_path/'existing';target.mkdir();sentinel=target/'keep';sentinel.write_text('untouched')
    try:
        assert list(tmp_path.glob('country-c2-*'))
        with pytest.raises(FileExistsError):persist_stream(stream,'unused',target)
        assert not list(tmp_path.glob('country-c2-*'))
        assert sentinel.read_text()=='untouched'
    finally:stream.close()


def test_existing_wrong_object_reference_rejected(tmp_path,catalog):
    r=route('a',prefix='2001:db8::/49',endpoint=replace(route().endpoint,afi=2),path_id=0)
    n=replace(r,object_id='n',prefix='2001:db8:1::/49',observation_ref='new')
    original=run_case(tmp_path,seeds=[r],changes=[change(0,21,n),change(1,23,n,origin=64498),change(2,25,n,origin=None),change(3,27,n,presence='absent',path_ref=None)],samples=(22,24,26,28))
    assert len(original)==137
    assert sum(isinstance(x,c2.C2Row) and isinstance(x.value,DirectionPoint) for x in original)==4
    altered=[replace(x,value=replace(x.value,object_refs=('n',))) if isinstance(x,c2.C2Row) and isinstance(x.value,DirectionPoint) else x for x in original]
    assert any(isinstance(x,c2.C2Row) and isinstance(x.value,compute.ObservationFact) and x.value.route.object_id=='n' for x in original)
    with pytest.raises(ValueError,match='object.*scope|object.*relation'):
        persist_stream(iter(altered),catalog,tmp_path/'bad')


def test_formal_original_revision_body_must_match_source(review_chain,catalog):
    root=review_chain[0];grid=compute.Grid(100000000,125000000,(107000000,113000000,120000000,121000000,121500000,124000000,125000000))
    original=list(c2.run_saved(adapter(review_chain),grid,scratch_root=root))
    assert len(original)==411
    altered=[replace(x,value=replace(x.value,original=replace(x.value.original,country='XX',onset=Time(1,0)))) if isinstance(x,c2.C2Row) and isinstance(x.value,c2.InputEvidence) and x.value.kind=='event_revision' else x for x in original]
    with pytest.raises(ValueError,match='revision.*source|revision.*body'):
        persist_stream(iter(altered),catalog,root/'bad-original',c1=adapter(review_chain),parameters={'grid':asdict(grid)})
    started=time.monotonic()
    good=persist_stream(iter(original),catalog,root/'good-original',c1=adapter(review_chain),parameters={'grid':asdict(grid)})
    elapsed=time.monotonic()-started
    (root/'expected-c3-repr.txt').write_text(repr(original))
    (root/'reader-request.json').write_text(json.dumps({'binding':asdict(good),'dsn':catalog}))
    script='''
import sys,json
from pathlib import Path
sys.path[:0]=['backend','backend/tests']
from tests.country.test_country_saved_input import adapter
from data_pipeline.analysis.country_events.snapshot_store import ComponentBinding
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch, ReadReceipt
root=Path(sys.argv[1]);request=json.loads((root/'request.json').read_text());source=json.loads((root/'binding.json').read_text());component=json.loads((root/'reader-request.json').read_text())
prepared=(root,[request['observation_dsn'],request['detection_dsn']],request['ordered_sources'],source['observation'],source['detection'])
reader=ComponentReader(component['dsn'],ComponentBinding(**component['binding']),c1=adapter(prepared))
parts=list(reader.stream());rows=[r for b in parts if isinstance(b,ReadBatch) for r in b.rows]
assert repr(rows)==(root/'expected-c3-repr.txt').read_text()
assert isinstance(parts[-1],ReadReceipt) and parts[-1].full_body_validated
print('fresh-process-full-original-match',len(rows))
'''
    child=subprocess.run([sys.executable,'-c',script,str(root)],cwd=Path(__file__).resolve().parents[3],capture_output=True,text=True)
    (root/'fresh-reader.txt').write_text(child.stdout+child.stderr)
    assert child.returncode==0,child.stderr
    (root/'repair-seal-seconds.json').write_text(json.dumps({'seal_including_close_and_complete_seconds':elapsed}))



@pytest.mark.parametrize('field',['prefix','endpoint','cohort','evidence','path_prefix','new_prefix','new_endpoint'])
def test_nearby_existing_reference_scope_mismatches(tmp_path,catalog,field):
    r=route('a');peer=route('peer',endpoint=replace(r.endpoint,remote_ip='192.0.2.8'),cursor=replace(r.cursor,ordinal=1))
    other=route('other',prefix='203.0.113.0/24',origin=64498,cursor=replace(r.cursor,ordinal=2))
    new=route('new',prefix='192.0.2.0/24')
    added=route('added',endpoint=replace(r.endpoint,local_ip='192.0.2.9'))
    rows=run_case(tmp_path,events=[incident(),incident('US')],seeds=[r,peer,other],changes=[change(0,22,new),change(1,23,added)])
    altered=list(rows)
    cls={'path_prefix':compute.PathSample,'new_prefix':compute.NewPrefixPoint,'new_endpoint':compute.NewDirectionPoint}.get(field,DirectionPoint)
    index=next(i for i,x in enumerate(rows) if isinstance(x,c2.C2Row) and isinstance(x.value,cls) and x.incident_id==incident().incident_id)
    row=rows[index];value=row.value
    if field=='prefix':value=replace(value,object_refs=('other',))
    elif field=='endpoint':value=replace(value,object_refs=('peer',)) if value.endpoint==r.endpoint else replace(value,object_refs=('a',))
    elif field=='evidence':value=replace(value,evidence_refs=(other.observation_ref,))
    elif field=='cohort':
        head=next(x for x in rows if isinstance(x,c2.C2Row) and isinstance(x.value,c2.EventStatus) and x.incident_id==incident('US').incident_id)
        row=replace(row,incident_id=head.incident_id,revision=head.revision);value=replace(value,cohort_id=head.value.cohort_id)
    elif field=='path_prefix':value=replace(value,prefix=other.prefix)
    elif field=='new_prefix':value=replace(value,first_observation_ref=other.observation_ref)
    else:value=replace(value,observation_ref=peer.observation_ref)
    altered[index]=replace(row,value=value)
    good=persist_stream(iter(rows),catalog,tmp_path/'good')
    assert read_all(ComponentReader(catalog,good))==rows
    with pytest.raises(ValueError):persist_stream(iter(altered),catalog,tmp_path/'bad')


def test_multipeer_addpath_history_and_unknown_stay_valid(tmp_path,catalog):
    a=route('a',path_id=0);b=route('b',path_id=1,cursor=replace(a.cursor,ordinal=1))
    peer=route('peer',endpoint=replace(a.endpoint,remote_ip='192.0.2.8'),cursor=replace(a.cursor,ordinal=2))
    changes=[change(0,22,a,presence='absent',path_ref=None),compute.Change(replace(a.cursor,source_rank=1,record=1),Time(23,0),binding().ordered_sources[1],'typed-state',invalidated_objects=('b','peer')),change(2,28,a)]
    rows=run_case(tmp_path,seeds=[a,b,peer],changes=changes)
    bnd=persist_stream(iter(rows),catalog,tmp_path/'good')
    assert list(map(repr,read_all(ComponentReader(catalog,bnd))))==list(map(repr,rows))
    directions=[x.value for x in rows if isinstance(x,c2.C2Row) and isinstance(x.value,DirectionPoint)]
    assert any(set(v.object_refs)=={'a','b'} for v in directions)
    assert any('typed-state' in v.evidence_refs for v in directions)


from tests.country.test_country_saved_input import prepared


@pytest.mark.parametrize('mode',['historical_body','missing_historical','selected_header','original_nested','original_order'])
def test_all_formal_revisions_bound_not_only_highest(prepared,catalog,mode):
    root=prepared[0];grid=compute.Grid(100000000,125000000,(113000000,125000000))
    rows=list(c2.run_saved(adapter(prepared),grid,scratch_root=root))
    revisions=[(n,x) for n,x in enumerate(rows) if isinstance(x,c2.C2Row) and isinstance(x.value,c2.InputEvidence) and x.value.kind=='event_revision']
    assert len(revisions)>=2
    n,row=min(revisions,key=lambda pair:pair[1].revision);assert row.revision<max(x.revision for _,x in revisions)
    bad=list(rows)
    if mode=='missing_historical':
        del bad[n];bad[-1]=replace(rows[-1],costs={**rows[-1].costs,'output_rows':rows[-1].costs['output_rows']-1})
    elif mode=='selected_header':
        n=next(n for n,x in enumerate(rows) if isinstance(x,c2.C2Row) and isinstance(x.value,c2.EventStatus))
        row=rows[n];bad[n]=replace(row,value=replace(row.value,incident=replace(row.value.incident,legacy_ref={'forged':True})))
    else:
        original=row.value.original
        if mode=='historical_body':original=replace(original,carry_in_state='forged')
        elif mode=='original_nested':original=replace(original,original={**original.original,'extra':{'nested':[None,None,{}]}})
        else:original=replace(original,original=dict(reversed(list(original.original.items()))))
        bad[n]=replace(row,value=replace(row.value,original=original))
    with pytest.raises(ValueError,match='revision.*body'):
        persist_stream(iter(bad),catalog,root/('bad-'+mode),c1=adapter(prepared),parameters={'grid':asdict(grid)})
    good=persist_stream(iter(rows),catalog,root/('good-'+mode),c1=adapter(prepared),parameters={'grid':asdict(grid)})
    assert list(map(repr,read_all(ComponentReader(catalog,good,c1=adapter(prepared)))))==list(map(repr,rows))


class CloseBomb:
    def __init__(self,stream):self.stream=stream;self.closed=False
    def __iter__(self):return self
    def __next__(self):return next(self.stream)
    def close(self):
        self.closed=True
        self.stream.close()
        raise RuntimeError('input-close-bomb')


def test_primary_init_error_retained_despite_real_stream_close_error(tmp_path):
    r=route();stream=c2.run_typed(binding(),compute.Grid(15000000,31000000,(21000000,)),[incident()],[compute.Change(r.cursor,r.observed_at,binding().ordered_sources[0],r.observation_ref,r)],paths(),refs(),scratch_root=tmp_path)
    next(stream);wrapped=CloseBomb(stream)
    with pytest.raises(FileExistsError) as error:persist_stream(wrapped,'unused',tmp_path)
    assert wrapped.closed and not list(tmp_path.glob('country-c2-*'))
    assert str(error.value.cleanup_errors[0])=='input-close-bomb'


@pytest.mark.parametrize('append_error',[False,True])
def test_real_produce_close_errors_cleanup_writer_and_restore_guard(review_chain,catalog,monkeypatch,append_error):
    from data_pipeline.analysis.country_events import snapshot_store as component
    root=review_chain[0];source=adapter(review_chain);guard=source.external_guard
    real_run=c2.run_saved;real_close=component.ComponentWriter.close;real_append=component.ComponentWriter.append;real_fail=component.ComponentWriter.fail
    captured=[];closed=[]
    def run(*args,**kwargs):
        stream=CloseBomb(real_run(*args,**kwargs));captured.append(stream);return stream
    def writer_close(writer):
        real_close(writer);closed.append(writer)
        raise RuntimeError('writer-close-bomb')
    def append(writer,item):
        real_append(writer,item)
        if append_error:raise ValueError('primary-append')
    def fail(writer,error):
        real_fail(writer,error)
        raise RuntimeError('registration-close-combination')
    monkeypatch.setattr(component.ComponentWriter,'fail',fail)
    monkeypatch.setattr(c2,'run_saved',run);monkeypatch.setattr(component.ComponentWriter,'close',writer_close);monkeypatch.setattr(component.ComponentWriter,'append',append)
    with pytest.raises(ValueError if append_error else RuntimeError,match='primary-append' if append_error else 'input-close-bomb') as error:
        component.produce_component(source,compute.Grid(100000000,125000000,(113000000,125000000)),catalog,root/('close-bombs-'+str(append_error)))
    assert source.external_guard is guard and captured[0].closed
    assert len(closed)==1 and closed[0].closed and all(getattr(closed[0],a) is None for a in ('lake','spool','pg'))
    assert not list(root.rglob('country-c2-*'))
    assert any(str(e)=='writer-close-bomb' for e in error.value.cleanup_errors)
    assert any(str(e)=='registration-close-combination' for e in error.value.cleanup_errors)
    import psycopg2
    with psycopg2.connect(catalog) as pg,pg.cursor() as cur:
        cur.execute('SELECT state FROM country_components.components');assert cur.fetchall()==[('failed',)]


def test_constructor_releases_real_handles_even_when_one_close_fails(tmp_path,catalog,monkeypatch):
    from data_pipeline.analysis.country_events import snapshot_store as component
    captured=[];real_guard=component.ComponentWriter.guard
    class Handle:
        def __init__(self,inner,fail):self.inner=inner;self.fail=fail;self.closed=False
        def close(self):
            self.inner.close();self.closed=True
            if self.fail:raise RuntimeError('spool-close-bomb')
    def fail_guard(writer):
        if not captured:
            writer.spool=Handle(writer.spool,True);writer.pg=Handle(writer.pg,False)
            captured.extend((writer.spool,writer.pg))
            raise ValueError('primary-init')
        real_guard(writer)
    monkeypatch.setattr(component.ComponentWriter,'guard',fail_guard)
    with pytest.raises(ValueError,match='primary-init') as error:persist_stream(iter(()),catalog,tmp_path/'init-fail')
    assert all(h.closed for h in captured)
    assert str(error.value.cleanup_errors[0])=='spool-close-bomb'


def test_started_formal_c2_existing_root_keeps_guard_and_files(review_chain):
    root=review_chain[0];source=adapter(review_chain);guard=source.external_guard
    stream=c2.run_saved(source,compute.Grid(100000000,125000000,(113000000,125000000)),scratch_root=root)
    next(stream)
    destination=root/'existing-preserved';destination.mkdir();(destination/'keep').write_bytes(b'fixed')
    try:
        assert list(root.glob('country-c2-*'))
        with pytest.raises(FileExistsError):persist_stream(stream,'unused',destination)
        assert source.external_guard is guard
        assert not list(root.glob('country-c2-*'))
        assert (destination/'keep').read_bytes()==b'fixed'
    finally:stream.close()


def test_all_real_writer_resources_attempted_when_lake_and_spool_close_fail(tmp_path,catalog):
    from data_pipeline.analysis.country_events import snapshot_store as component
    writer=component.ComponentWriter(catalog,tmp_path/'candidate')
    writer.lake=component.lake_connect(catalog,writer.root)
    calls=[]
    class Wrapped:
        def __init__(self,resource,name):self.resource,self.name=resource,name
        def close(self):
            self.resource.close();calls.append(self.name)
            if self.name!='pg':raise RuntimeError(self.name+'-close')
    for name in ('lake','spool','pg'):setattr(writer,name,Wrapped(getattr(writer,name),name))
    with pytest.raises(RuntimeError,match='lake-close') as error:writer.close()
    assert calls==['lake','spool','pg'] and writer.closed
    assert [str(e) for e in error.value.cleanup_errors]==['lake-close','spool-close']
    assert all(getattr(writer,name) is None for name in ('lake','spool','pg'))


def test_formal_state_evidence_wrong_endpoint_rejected(review_chain,catalog):
    from data_pipeline.analysis.country_events.saved_input import SavedBatch
    root=review_chain[0];grid=compute.Grid(100000000,125000000,(107000000,113000000,120000000,121000000,121500000,124000000,125000000))
    rows=list(c2.run_saved(adapter(review_chain),grid,scratch_root=root))
    facts={x.value.route.object_id:x.value.route for x in rows if isinstance(x,c2.C2Row) and isinstance(x.value,compute.ObservationFact)}
    targets={}
    for batch in adapter(review_chain).stream():
        if isinstance(batch,SavedBatch) and batch.table=='invalidations':
            for item in batch.rows:
                if item.object_key in facts:targets.setdefault(item.message_ref,set()).add(facts[item.object_key].direction)
    selected=next((n,ref) for n,x in enumerate(rows) if isinstance(x,c2.C2Row) and isinstance(x.value,DirectionPoint) for ref,scopes in targets.items() if (x.value.endpoint,x.value.prefix) not in scopes)
    n,ref=selected;bad=list(rows);bad[n]=replace(rows[n],value=replace(rows[n].value,evidence_refs=(ref,)))
    with pytest.raises(ValueError,match='state_object_scope'):
        persist_stream(iter(bad),catalog,root/'state-wrong-endpoint',c1=adapter(review_chain),parameters={'grid':asdict(grid)})


@pytest.mark.parametrize('target',['observation','detection','reference','receipt'])
def test_formal_reader_tail_revocation_still_has_no_receipt(review_chain,catalog,target):
    import psycopg2
    from data_pipeline.analysis.country_events.snapshot_store import produce_component
    from data_pipeline.analysis.country_events.snapshot_reader import ReadReceipt
    root,dsns,_,o,d=review_chain
    binding_out=produce_component(adapter(review_chain),compute.Grid(100000000,125000000,(113000000,125000000)),catalog,root/('tail-'+target))
    reader=ComponentReader(catalog,binding_out,c1=adapter(review_chain),batch_rows=1)
    stream=reader.stream();parts=[next(stream)];workdir=Path(reader.workdir)
    path=root/'observation/execution.json';original=path.read_bytes()
    sql,key,old={
        'observation':('UPDATE domeye.runs SET state=%s WHERE run_id=%s',o['run_id'],'complete'),
        'detection':('UPDATE detection.runs SET state=%s WHERE run_id=%s',d['run_id'],'complete'),
        'reference':('UPDATE domeye.reference_inputs SET state=%s WHERE run_id=%s',o['run_id'],'validated'),
        'receipt':(None,None,None),
    }[target]
    def update(state):
        with psycopg2.connect(dsns[1] if target=='detection' else dsns[0]) as pg,pg.cursor() as cur:cur.execute(sql,(state,key))
    try:
        if target=='receipt':path.write_bytes(original+b' ')
        else:update('failed')
        with pytest.raises(ValueError):
            for part in stream:parts.append(part)
        assert not any(isinstance(p,ReadReceipt) for p in parts)
        assert not workdir.exists()
    finally:
        stream.close()
        if target=='receipt':path.write_bytes(original)
        else:update(old)
