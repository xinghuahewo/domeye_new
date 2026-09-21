"""仅H5构造失败接缝：真实SQLite拒绝、双错、原Collection正常重投影。"""
from contextlib import closing
from dataclasses import asdict
import json
import os
from pathlib import Path
import sqlite3
import uuid
import pytest
from data_pipeline.history.event_collection.spool import Spool
from data_pipeline.history.country_events import History, PGIdentity
from data_pipeline.history.country_events.model import TABLES, row_bytes
from tests.historical.test_historical_general import decode, save, sha, table

@pytest.fixture(scope='module')
def context():
    if not os.environ.get('Q3_PRIVATE_ROOT'):pytest.skip('须自己的原私有目标与健康Collection')
    root=Path(os.environ['Q3_PRIVATE_ROOT']);original=root/'q3-c4/acceptance-ead737f1cc9141308779ed0891565286'
    b=json.loads((original/'binding.json').read_text());h=History(b['dsn'],root,target_identity=PGIdentity(**b['identity']))
    out=root/'q3-c4-cleanup'/('acceptance-'+uuid.uuid4().hex);out.mkdir()
    print('Q3C4_CLEANUP_EVIDENCE='+str(out));return h,decode(b['token']),out,original

class Connection:
    def __init__(self,db,close_error=None):self.raw=db;self.close_error=close_error;self.closes=0;self.primary=None;self.traceback=None
    def execute(self,*a,**kw):
        try:return self.raw.execute(*a,**kw)
        except sqlite3.DatabaseError as error:self.primary=error;self.traceback=error.__traceback__;raise
    def close(self):
        self.closes+=1;self.raw.close()
        if self.close_error is not None:raise self.close_error
    def __getattr__(self,key):return getattr(self.raw,key)

def traceback_contains(error,expected):
    tb=error.__traceback__
    while tb:
        if tb is expected:return True
        tb=tb.tb_next
    return False

@pytest.mark.parametrize('close_fails',[False,True])
def test_actual_create_rejection_and_primary_preserved(context,monkeypatch,close_fails):
    h,old,out,_=context;held=[];cleanup=RuntimeError('人工真实close后清理错误') if close_fails else None
    def factory(*a,**kw):
        spool=Spool(*a,**kw);connection=Connection(spool.db,cleanup);held.append(connection)
        spool.db=connection;connection.raw.set_authorizer(lambda action,*a:sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_CREATE_TABLE else sqlite3.SQLITE_OK)
        return spool
    monkeypatch.setattr('data_pipeline.history.country_events.project.Spool',factory)
    before=set(h.data_root.iterdir());token=None
    with pytest.raises(sqlite3.DatabaseError) as caught:token=h.project_general(old.collection)
    assert token is None and len(held)==1;connection=held[0]
    assert caught.value is connection.primary and traceback_contains(caught.value,connection.traceback)
    assert connection.closes==1
    with pytest.raises(sqlite3.ProgrammingError,match='closed'):connection.raw.execute('SELECT 1')
    candidates=set(h.data_root.iterdir())-before;assert len(candidates)==1;candidate=candidates.pop()
    assert (candidate/'FAILED.json').exists() and not (candidate/'ready.json').exists()
    errors=getattr(caught.value,'cleanup_errors',[])
    if close_fails:assert len(errors)==1 and errors[0]['message']==str(cleanup)
    else:assert errors==[]
    save(out/('create-close-error.json' if close_fails else 'create-rejected.json'),{'original_collection':asdict(old.collection),'primary_type':type(caught.value).__name__,'primary_message':str(caught.value),'same_exception_object':caught.value is connection.primary,'original_traceback_retained':True,'actual_connection_closed':True,'close_calls':connection.closes,'cleanup_errors':errors,'candidate':str(candidate),'failed':True,'ready':False,'token_returned':False})

def test_original_collection_reprojection_full_values(context,monkeypatch):
    h,old,out,original=context;held=[]
    def factory(*a,**kw):
        spool=Spool(*a,**kw);connection=Connection(spool.db);held.append(connection);spool.db=connection;return spool
    monkeypatch.setattr('data_pipeline.history.country_events.project.Spool',factory)
    before={str(p):sha(p) for p in (h.data_root/old.profile_id/'ready.json',h.data_root/old.collection.collection_id/'ready.json')}
    with pytest.raises(ValueError,match='身份变化'):
        with h.general(old):pass
    current=h.project_general(old.collection)
    assert current.collection==old.collection and current.profile_id!=old.profile_id and current.rule_sha256!=old.rule_sha256
    assert len(held)==1 and held[0].closes==1
    with pytest.raises(sqlite3.ProgrammingError,match='closed'):held[0].raw.execute('SELECT 1')
    expected=json.loads((original/'ordered-tables.json').read_text());raws=json.loads((original/'raw-documents.json').read_text());digests={}
    import hashlib
    with h.general(current) as s:
        for name in TABLES:
            actual=table(s,name);assert json.loads(row_bytes(actual))==expected[name]
            digest=hashlib.sha256()
            for row in actual:digest.update(row_bytes(row)+b'\n')
            digests[name]={'rows':len(actual),'sha256':digest.hexdigest()}
        for doc in expected['documents']:
            offset=0;raw=bytearray()
            while True:
                part=s.document(doc['document_id'],offset=offset);raw.extend(part['raw']);offset=part['next_offset']
                if offset is None:break
            assert raw.hex()==raws[str(doc['document_id'])]
    assert s.receipt and all(sha(Path(p))==v for p,v in before.items())
    save(out/'healthy-reprojection.json',{'old_token':asdict(old),'new_token':asdict(current),'same_collection':True,'freeze_or_import_repeated':False,'old_ready_unchanged':before,'all_tables_including_references_equal':True,'all_original_document_bytes_equal':True,'table_digests':digests,'normal_constructor_close_calls':held[0].closes,'receipt':s.receipt})
    save(out/'binding.json',{'dsn':h.dsn,'root':str(h.root),'identity':asdict(h.target_identity),'token':asdict(current)})
