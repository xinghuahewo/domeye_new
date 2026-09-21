"""4f7ee67四项原P2回归，仅本次明确私有PG和临时文件。"""
from dataclasses import replace,asdict
from pathlib import Path
from collections import OrderedDict,Counter
from contextlib import contextmanager
import hashlib
import json
import shutil
import sqlite3
import pytest

from tests.country.test_country_component import catalog
from tests.country.test_country_c1_repair import review_chain
from tests.country.test_country_query import formal_index, fixture_index, fixture_qualification, select, runtime
from data_pipeline.analysis.country_events import event_aggregation as c2, compute
from data_pipeline.analysis.country_events.snapshot_store import persist_stream, file_hash
from data_pipeline.analysis.country_events.snapshot_schema import encode, decode
from data_pipeline.analysis.country_events.snapshot_access import FixedComponentAccess
from data_pipeline.analysis.country_events.selection_reader import CountryQuery, open_qualified_country
from data_pipeline.analysis.country_events.selection_index import prepare_country_index, inspect_country, stamp
from data_pipeline.analysis.country_events.selection_contract import *


def forged_copy(b,p,descriptor,destination):
    shutil.copytree(b.root,destination)
    db=sqlite3.connect(destination/'index.sqlite')
    for event,payload in db.execute("SELECT event,payload FROM views WHERE view='overview'").fetchall():
        obj=decode(payload);obj['path_count']=999999
        db.execute("UPDATE views SET payload=? WHERE event=? AND view='overview'",(encode(obj),event))
    db.commit();db.close()
    m=json.loads((destination/'manifest.json').read_text())
    m['identity']['index_sha256']=file_hash(destination/'index.sqlite')
    model='country_read_'+digest(m['identity']); forged_proof=replace(p,read_model_id=model)
    m['proof']=contract_json(forged_proof)
    entities=[]
    for item in m['entities']:
        original=Path(item['path'])
        path=destination/original.relative_to(b.root) if original.is_relative_to(b.root) else original
        entities.append(stamp(path,file_hash(path)))
    m['entities']=[asdict(x) for x in entities]
    (destination/'manifest.json').write_text(canonical(m))
    forged=replace(b,read_model_id=model,root=str(destination),manifest_sha256=file_hash(destination/'manifest.json'))
    (destination/'ready.json').write_text(contract_json(forged))
    forged_descriptor=replace(descriptor,read_binding=forged,admission_proof=forged_proof,
        entities=tuple(entities)+(stamp(destination/'manifest.json',forged.manifest_sha256),stamp(destination/'ready.json',file_hash(destination/'ready.json'))))
    return forged,forged_proof,forged_descriptor


def test_real_source_self_signed_overview_is_not_admitted(review_chain,catalog):
    from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader
    rows,rt,b,p,d=formal_index(review_chain,catalog,'repair-forgery')
    @contextmanager
    def actual_sources(desc,*,lock=False,selection=None):
        ComponentReader(catalog,b.component,c1=rt.c1)
        yield
    rt=replace(rt,verify_qualification=actual_sources)
    f,fp,fd=forged_copy(b,p,d,review_chain[0]/'forged-resigned')
    rejected=[];seen=[]
    try: inspect_country(f,fp,runtime=rt)
    except ValueError:rejected.append('inspect')
    event=next(r.value.incident.incident_id for r in rows if isinstance(r,c2.C2Row) and isinstance(r.value,c2.EventStatus))
    try:
        with open_qualified_country(select(fd,event),fp,runtime=rt,verify_qualification=actual_sources) as q:
            seen.append(q.query(QueryRequest('overview')).items[0]['path_count'])
    except ValueError:rejected.append('query')
    (review_chain[0]/'repair-forgery-evidence.json').write_text(json.dumps({'rejected':rejected,'returned_path_counts':seen}))
    assert rejected==['inspect','query'],seen
    assert inspect_country(b,p,runtime=rt)==d


class CloseThenRaise:
    def __init__(self,actual):self.actual=actual
    def close(self):self.actual.close();raise RuntimeError('injected_close')
    def __getattr__(self,key):return getattr(self.actual,key)


def test_close_error_still_closes_other_files_and_sqlite(tmp_path):
    first=(tmp_path/'first').open('w+');second=(tmp_path/'second').open('w+')
    access=FixedComponentAccess.__new__(FixedComponentAccess)
    access.cache=OrderedDict();access.cache_bytes=0;access.stats=Counter();access.files={}
    access.handles={'first':CloseThenRaise(first),'second':second}
    query=CountryQuery.__new__(CountryQuery);query.stats=Counter();query.access=access;query.db=sqlite3.connect(':memory:');query.closed=False
    db=query.db
    with pytest.raises(RuntimeError,match='injected_close'):query.close()
    assert first.closed and second.closed and query.closed
    with pytest.raises(sqlite3.ProgrammingError):db.execute('SELECT 1')
    query.close()


def test_query_constructor_preserves_primary_and_cleanup(tmp_path,catalog,monkeypatch):
    import data_pipeline.analysis.country_events.selection_reader as module
    rows,rt,b,p,d=fixture_index(tmp_path,catalog)
    class FailAccess:
        def __init__(self,*a,**k):raise ValueError('primary_layout_failure')
    actual=module.readonly;opened=[]
    def connection(path):
        db=actual(path);opened.append(db);return CloseThenRaise(db)
    monkeypatch.setattr(module,'readonly',connection);monkeypatch.setattr(module,'FixedComponentAccess',FailAccess)
    with pytest.raises(ValueError,match='primary_layout_failure') as caught:
        open_qualified_country(select(d,'incident-IR'),p,runtime=rt,verify_qualification=fixture_qualification)
    assert any(str(x)=='injected_close' for x in caught.value.cleanup_errors)
    with pytest.raises(sqlite3.ProgrammingError):opened[0].execute('SELECT 1')


def typed_137(root):
    from data_pipeline.analysis.country_events.models import Binding, Endpoint, Route, Time, Cursor, Incident, Reference
    binding=Binding('manual','1',('rib','u'),'refs','decoder','state','rrc')
    ep=Endpoint('rrc','192.0.2.1',1,'192.0.2.2',2,0,2,1)
    route=Route('a',ep,'2001:db8::/49',0,'present','p',3,'a0',Time(10,0),Cursor(0,0,0),'mapping','peer',fact_presence='present')
    event=Incident('e',1,'ZZ',Time(20,0),Time(15,0))
    refs=(Reference(3,'ZZ','ref3'),Reference(4,'YY','ref4'))
    paths=(compute.Path('p',(compute.Segment('sequence',(1,3,4)),),b'raw'),)
    seed=compute.Change(route.cursor,route.observed_at,'rib','a0',route)
    new=replace(route,object_id='n',prefix='2001:db8:1::/49',observation_ref='n0')
    changes=[]
    for n,t,r in [(0,21,new),(1,23,replace(new,origin=4)),(2,25,replace(new,origin=None)),(3,27,replace(new,presence='absent',path_ref=None))]:
        r=replace(r,cursor=Cursor(1,n,0),observed_at=Time(t,0),observation_ref='u'+str(n))
        changes.append(compute.Change(r.cursor,r.observed_at,'u',r.observation_ref,r))
    grid=compute.Grid(15000000,30000000,(22000000,24000000,26000000,28000000))
    rows=list(c2.run_typed(binding,grid,(event,),[seed,*changes],paths,refs,scratch_root=root))
    assert len(rows)==137
    return rows,grid


@pytest.mark.parametrize('mutation',['both','window','through'])
def test_completion_rejects_137_unchanged_rows_wrong_original_window(tmp_path,catalog,mutation):
    rows,grid=typed_137(tmp_path)
    values={'sample_window':(1,2),'data_through_us':999000000}
    if mutation=='window':values.pop('data_through_us')
    if mutation=='through':values.pop('sample_window')
    altered=[replace(r,value=replace(r.value,**values)) if isinstance(r,c2.C2Row) and isinstance(r.value,compute.Completion) else r for r in rows]
    assert len(altered)==137
    binding=persist_stream(iter(altered),catalog,tmp_path/'wrong-c3',parameters={'grid':asdict(grid)})
    with pytest.raises(ValueError,match='C4_completion_grid'):
        prepare_country_index(binding,reference_binding=None,runtime=runtime(catalog),output=tmp_path/'wrong-c4')
    assert not (tmp_path/'wrong-c4/ready.json').exists()


@pytest.mark.parametrize('damage',['missing','changed','revoked','directory'])
def test_admission_anchor_is_required_for_inspect_and_query(tmp_path,catalog,damage):
    import psycopg2
    rows,rt,b,p,d=fixture_index(tmp_path,catalog)
    s=select(d,'incident-IR')
    q=open_qualified_country(s,p,runtime=rt,verify_qualification=fixture_qualification)
    with psycopg2.connect(catalog) as pg,pg.cursor() as cur:
        if damage=='directory':cur.execute('DROP TABLE country_components.read_admissions')
        elif damage=='missing':cur.execute('DELETE FROM country_components.read_admissions WHERE read_model_id=%s',(b.read_model_id,))
        elif damage=='changed':cur.execute("UPDATE country_components.read_admissions SET proof_json='{}' WHERE read_model_id=%s",(b.read_model_id,))
        else:cur.execute("UPDATE country_components.read_admissions SET state='revoked' WHERE read_model_id=%s",(b.read_model_id,))
    with pytest.raises(ValueError,match='C4_admission'):inspect_country(b,p,runtime=rt)
    try:
        with pytest.raises(ValueError,match='C4_admission'):q.query(QueryRequest('overview'))
    finally:q.close()


def test_anchor_lock_readonly_duplicate_and_failed_finalization(tmp_path,catalog,monkeypatch):
    import psycopg2
    from data_pipeline.analysis.country_events.selection_admission import verify_country_admission, _register_country_admission
    from data_pipeline.analysis.country_events import selection_index as query_index
    rows,rt,b,p,d=fixture_index(tmp_path,catalog)
    # 正确复用不写新锚；同ID异内容INSERT拒绝，原记录不覆盖。
    for _ in range(2):
        with verify_country_admission(catalog,b,p):pass
    with pytest.raises(psycopg2.IntegrityError):
        _register_country_admission(catalog,replace(b,root='/different'),p,before_commit=lambda:None)
    with verify_country_admission(catalog,b,p,lock=True):
        with psycopg2.connect(catalog) as pg,pg.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout='100ms'")
            with pytest.raises(psycopg2.errors.LockNotAvailable):
                cur.execute("UPDATE country_components.read_admissions SET state='revoked' WHERE read_model_id=%s",(b.read_model_id,))
    with psycopg2.connect(catalog) as pg,pg.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout='100ms'")
        cur.execute("UPDATE country_components.read_admissions SET state='accepted' WHERE read_model_id=%s",(b.read_model_id,))
    original=query_index.fsync_tree
    def fail_ready(root):
        original(root)
        if (root/'ready.json').exists():raise OSError('injected_ready_fsync')
    monkeypatch.setattr(query_index,'fsync_tree',fail_ready)
    # 别的组件保证新model，不以重复主键失败替代末步失败。
    component=persist_stream(iter(rows),catalog,tmp_path/'second-c3')
    with pytest.raises(OSError,match='ready_fsync'):
        prepare_country_index(component,reference_binding=None,runtime=rt,output=tmp_path/'failed-c4')
    assert not (tmp_path/'failed-c4/ready.json').exists()
    with psycopg2.connect(catalog) as pg,pg.cursor() as cur:
        cur.execute('SELECT count(*) FROM country_components.read_admissions');assert cur.fetchone()==(1,)


def test_query_primary_survives_close_failure(tmp_path,catalog,monkeypatch):
    rows,rt,b,p,d=fixture_index(tmp_path,catalog)
    query=open_qualified_country(select(d,'incident-IR'),p,runtime=rt,verify_qualification=fixture_qualification)
    first=next(iter(query.access.handles));actual=query.access.handles[first]
    query.access.handles[first]=CloseThenRaise(actual)
    db=query.db
    def fail(*args):raise ValueError('primary_query_failure')
    monkeypatch.setattr(query.access,'read',fail)
    with pytest.raises(ValueError,match='primary_query_failure') as caught:
        with query:query.query(QueryRequest('series15'))
    assert caught.value.cleanup_errors and query.closed
    with pytest.raises(sqlite3.ProgrammingError):db.execute('SELECT 1')


def test_access_constructor_preserves_primary_with_real_file_cleanup(tmp_path,catalog,monkeypatch):
    import data_pipeline.analysis.country_events.snapshot_access as module
    rows,rt,b,p,d=fixture_index(tmp_path,catalog)
    real=module.pq.ParquetFile;calls=[0];opened=[]
    # 构造第二个footer时失败；先打开的真实句柄close后抛错，后续句柄仍关闭。
    old_close=module.FixedComponentAccess.close
    def close(self):
        opened.extend(self.handles.values())
        first=next(iter(self.handles))
        self.handles[first]=CloseThenRaise(self.handles[first])
        old_close(self)
    def fail_second(handle):
        calls[0]+=1
        if calls[0]==2:raise ValueError('primary_footer_failure')
        return real(handle)
    monkeypatch.setattr(module.FixedComponentAccess,'close',close)
    monkeypatch.setattr(module.pq,'ParquetFile',fail_second)
    with pytest.raises(ValueError,match='primary_footer_failure') as caught:
        open_qualified_country(select(d,'incident-IR'),p,runtime=rt,verify_qualification=fixture_qualification)
    assert caught.value.cleanup_errors and all(f.closed for f in opened) and len(opened)==2


def test_measurement_missing_config_skips_without_reading(monkeypatch):
    from tests.country.country_query_test_support import country_pg_log
    monkeypatch.delenv('DOMEYE_COUNTRY_TEST_PG_LOG',raising=False)
    with pytest.raises(pytest.skip.Exception,match='仅跳过SQL测量'):country_pg_log.__wrapped__()


def test_measurement_missing_and_unrelated_log_cannot_be_zero(tmp_path,catalog):
    from tests.country.country_query_test_support import PgLogProbe
    with pytest.raises(ValueError,match='log_missing'):PgLogProbe(catalog,tmp_path/'missing')
    empty=tmp_path/'empty';empty.write_text('')
    with pytest.raises(ValueError,match='not_bound_or_not_recording'):PgLogProbe(catalog,empty)
