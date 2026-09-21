"""一次有限spill用点：原行超出单行缓冲，类型/重复/顺序与历史查询不变。"""
from fractions import Fraction
import tracemalloc
import pytest

from data_pipeline.analysis.country_events.input_staging import SourceStore, Lookup
from data_pipeline.analysis.country_events.route_history import CanonicalHistory
from data_pipeline.analysis.country_events.route_time_index import state_before
from tests.country.test_country_m3_temporal import fixture
from tests.observations.test_canonical_projection import key


def store(tmp_path,**kw):
    return SourceStore(tmp_path,max_rows=10000,max_bytes=32*1024**2,max_row_bytes=8192,batch_rows=64,guard=lambda:None,**kw)


def test_finite_spill_keeps_original_order_duplicates_types_and_small_python_buffer(tmp_path):
    tracemalloc.start();s=store(tmp_path)
    try:
        def rows():
            for n in range(2000):yield dict(id=n//2,raw=b'x'*1024,value=Fraction(1,2),missing=None,ordered=('a','a',n//2))
        s.add_view(('fixture','rows'),rows());s.check_ordinals()
        lookup=Lookup(s['fixture','rows'],'id',identical=True)
        assert len(s['fixture','rows'])==2000 and len(lookup)==1000
        assert s['fixture','rows'][0]==s['fixture','rows'][1]
        assert lookup[999]['ordered']==('a','a',999)
        assert isinstance(lookup[999]['value'],Fraction) and lookup[999]['missing'] is None
        assert s.db.execute('PRAGMA cache_size').fetchone()[0]==-8192
        assert (s.root/'source.sqlite').stat().st_size>2*1024**2
        assert s.peak_row_bytes<8192
        assert tracemalloc.get_traced_memory()[1]<2*1024**2
        path=s.root
    finally:s.close();tracemalloc.stop()
    assert not path.exists()


@pytest.mark.parametrize('direction,regression',[('received',False),('local',False),('received',True)])
def test_spilled_history_matches_original_replay_temporal_semantics(tmp_path,direction,regression):
    old,messages=fixture(direction,regression);s=store(tmp_path)
    try:
        from data_pipeline.bgp.replay.snapshot_contract import encode, decode
        # 使用实际Canonical公开codec边界；Replay内的Enum在公开行中为原字符串值。
        for view in old.tables:s.add_view(('canonical',view),(decode(encode(r)) for r in old.rows[view]))
        s.add_view(('m2','messages'),messages.values())
        new=CanonicalHistory(old.binding,max_rows=1000,max_bytes=1024**2,max_gap_candidates=100,
            guard=lambda:None,store=s,original_rows={v:s['canonical',v] for v in old.tables})
        for view in old.tables:
            for row in s['canonical',view]:new.append(view,row)
        new.seal();lookup=Lookup(s['m2','messages'],'message_id')
        for obj in (key(),key('198.51.1.0/24')):
            for at in (101500000,101900000,101950000,102000000,102000001):
                assert state_before(new,lookup,obj,at)==state_before(old,messages,obj,at)
        assert new.store is s and not isinstance(new.rows['changes'],list)
    finally:s.close()


def test_spool_rejects_missing_or_duplicate_ordinal_and_oversized_row(tmp_path):
    s=store(tmp_path)
    try:
        s.add_record(('fixture','x'),1,{'raw':b'a'})
        with pytest.raises(ValueError,match='序号缺项'):s.check_ordinals()
        with pytest.raises(ValueError,match='single_row'):s.add_record(('fixture','x'),2,{'raw':b'x'*8192})
    finally:s.close()


def test_detection_full_history_indexes_spill_without_changing_original_chain(tmp_path):
    from tests.country.test_country_m3_results import fixture as result_fixture
    from data_pipeline.analysis.country_events.qualified_event_results import country_results
    args=result_fixture();expected=country_results(*args,max_rows=100,guard=lambda:None);s=store(tmp_path)
    try:
        for i,rows in enumerate(args[:4]):s.add_view(('detection',i),rows)
        result=country_results(*(s['detection',i] for i in range(4)),args[4],max_rows=100,guard=lambda:None,store=s)
        assert tuple(result)==expected and not isinstance(result,(tuple,list))
        assert [r['raw']['sequence'] for r in result]==[52,68]
        assert s.db.execute('SELECT count(*) FROM groups').fetchone()[0]==0
    finally:s.close()


def test_retained_admission_json_order_preserves_original_but_rejects_changed_values():
    from types import SimpleNamespace
    from data_pipeline.analysis.country_events.admission_sources import retained_admissions
    from data_pipeline.analysis.country_events.snapshot_schema import encode
    original=({'admission_id':'fixture','nested':{'z':1,'a':[2,2]}},)
    inputs=SimpleNamespace(admissions={'fixture':{'nested':{'a':[2,2],'z':1},'admission_id':'fixture'}})
    retained_admissions(inputs,original)
    assert encode(tuple(inputs.admissions.values()))==encode(original)
    inputs.admissions['fixture']['nested']['z']=True
    with pytest.raises(ValueError,match='Admission'):retained_admissions(inputs,original)


def test_rollback_journal_counts_and_limit_failure_cleans_all_files(tmp_path):
    s=SourceStore(tmp_path,max_rows=100,max_bytes=600000,max_row_bytes=500000,batch_rows=1,guard=lambda:None)
    path=s.root
    try:
        values=s.new_map();values['x']='a'*350000;s.flush()
        with pytest.raises(ValueError,match='spool_disk'):values['x']='b'*350000
        assert (path/'source.sqlite-journal').stat().st_size>0
    finally:s.close()
    assert not path.exists()


def test_constructor_failure_closes_connection_and_directory(tmp_path,monkeypatch):
    import sqlite3
    held=[];connect=sqlite3.connect
    def denied(*args,**kwargs):
        db=connect(*args,**kwargs);held.append(db)
        db.set_authorizer(lambda action,*_:sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_CREATE_TABLE else sqlite3.SQLITE_OK)
        return db
    monkeypatch.setattr('data_pipeline.analysis.country_events.input_staging.sqlite3.connect',denied)
    with pytest.raises(sqlite3.DatabaseError):store(tmp_path)
    with pytest.raises(sqlite3.ProgrammingError,match='closed'):held[0].execute('SELECT 1')
    assert not list(tmp_path.iterdir())


def test_read_primary_survives_iterator_close_failure(tmp_path):
    primary=ValueError('read failed')
    class Broken:
        def __iter__(self):return self
        def __next__(self):raise primary
        def close(self):raise OSError('close failed')
    s=store(tmp_path)
    try:
        with pytest.raises(ValueError) as caught:s.add_view(('fixture','rows'),Broken())
        assert caught.value is primary and isinstance(primary.cleanup_errors[0],OSError)
    finally:s.close()


@pytest.mark.parametrize('different',[True,1.0,(2,3),[3,2]])
def test_retained_admissions_reject_type_and_sequence_changes(different):
    from types import SimpleNamespace
    from data_pipeline.analysis.country_events.admission_sources import retained_admissions
    original=({'admission_id':'fixture','value':1 if type(different) in (bool,float) else [2,3]},)
    with pytest.raises(ValueError):retained_admissions(SimpleNamespace(admissions={'fixture':{'admission_id':'fixture','value':different}}),original)


def test_qualification_history_does_not_retain_all_input_python_objects():
    import weakref,gc
    from types import SimpleNamespace
    from data_pipeline.analysis.country_events.result_qualification import qualification_rows
    from data_pipeline.analysis.country_events.event_aggregation import C2Row, InputEvidence
    refs=[];alive=[]
    def rows():
        for i in range(2000):
            item=C2Row('event',1,InputEvidence('event_revision',str(i),{'body':'x'*1024}))
            refs.append(weakref.ref(item));yield item
        del item;gc.collect();alive.append(sum(r() is not None for r in refs))
    source=SimpleNamespace(verify_saved=lambda:None,windows={'result_window':{'window_start':'2026-01-01T00:00:00Z','window_end_exclusive':'2026-01-02T00:00:00Z'}})
    with pytest.raises(ValueError,match='完整原C2'):
        qualification_rows(source,'r',rows(),max_rows=10000,max_bytes=10000000,max_references_per_row=100,guard=lambda:None)
    assert alive==[1]


def test_actual_audit_spills_qualification_values_and_interval_history(tmp_path):
    from tests.country.test_country_m3_qualification import fixture
    from data_pipeline.analysis.country_events.result_qualification import qualification_rows
    from data_pipeline.analysis.country_events.route_reference_audit import M3RowAudit
    from data_pipeline.analysis.country_events.qualified_store import AUDIT_LIMITS
    from data_pipeline.analysis.country_events.route_contract import DIMENSIONS
    from itertools import chain
    source,raw=fixture();s=store(tmp_path)
    derived=qualification_rows(source,'logical',raw,max_rows=1000,max_bytes=1024**2,max_references_per_row=128,guard=lambda:None)
    audit=M3RowAudit('logical',source.binding.input_binding_id,{((15000000,31000000),d) for d in DIMENSIONS},guard=lambda:None,store=s,**AUDIT_LIMITS)
    try:
        for seq,item in enumerate(chain(raw,derived)):audit.append(seq,item)
        assert audit.finish()['rows']==len(raw)+len(derived)
        assert not isinstance(audit.values,list) and not isinstance(audit.qualifications,dict)
        assert list(audit.value_rows())
        audit.close()
    finally:derived.close();s.close()
