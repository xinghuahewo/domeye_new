"""C.1四项固定P2反例；仅本任务人工输入和私有PG。"""
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path
import sqlite3

import pytest

from tests.historical.test_historical_collection import fixture, lake, encoded, sha, save
from data_pipeline.history.event_collection import Binding, Root, History, freeze_collection
from data_pipeline.history.event_collection.store import row_bytes
from data_pipeline.history.database_import import Limits
import data_pipeline.history.event_collection.freeze as module


def extra_children(root,count=600):
    root.mkdir(); days={}
    for i in range(2):
        records=root/f'records{i}.jsonl'; records.write_bytes(b'')
        path=root/f'day{i}.sqlite'
        with sqlite3.connect(path) as db:
            db.execute('CREATE TABLE records(item TEXT,payload BLOB)')
            db.execute('CREATE TABLE provenance(manifest BLOB)')
            db.execute('CREATE TABLE retained_extra(value TEXT)')
            db.executemany('INSERT INTO retained_extra VALUES (?)',[('x',)]*count)
            db.execute('INSERT INTO provenance VALUES (?)',(encoded({'schema_version':'core-overview-input/v1','records':{'file':records.name,'sha256':sha(records.read_bytes()),'count':0}}),))
        days[f'2026-03-0{i+1}']={'file':path.name,'sha256':sha(path.read_bytes()),'count':0}
    manifest=root/'manifest.json'; save(manifest,{'schema_version':'core-overview-index/v2','days':days})
    return Binding((Root('core-index/v1',str(manifest),manifest.as_uri(),'fixture',sha(manifest.read_bytes())),),(str(root),))


def test_s1_real_child_remaining_rows(tmp_path,monkeypatch):
    binding=extra_children(tmp_path/'source'); seen=[]; actual=module.freeze_sqlite_source
    def freeze(*a,**kw):
        result=actual(*a,**kw); m=json.loads(result.read_text())
        seen.append((kw['limits'].max_total_rows,sum(t['rows'] for t in m['tables'])))
        return result
    monkeypatch.setattr(module,'freeze_sqlite_source',freeze)
    with pytest.raises(ValueError): freeze_collection(binding,tmp_path/'out',limits=replace(Limits(),max_total_rows=1000))
    assert sum(rows for _,rows in seen)<=1000
    assert len(list((tmp_path/'out').glob('child-*/COMPLETE.json')))==1


@pytest.mark.parametrize('key',['source_note','publication_id','input_version'])
def test_p1_extensions_keep_all_occurrences(tmp_path,key):
    binding=fixture(tmp_path/'source'); path=Path(binding.roots[0].path)
    raw=path.read_bytes()[:-1]+b',"extra":{'+encoded(key)+b':1,'+encoded(key)+b':2}}'
    path.write_bytes(raw); binding=replace(binding,roots=(replace(binding.roots[0],sha256=sha(raw)),binding.roots[1]))
    manifest=freeze_collection(binding,tmp_path/'out')
    with sqlite3.connect(manifest.parent/'structure.sqlite') as db:
        assert db.execute("SELECT number_lexeme FROM nodes WHERE document_id=0 AND key=? AND parent_ordinal=(SELECT node_ordinal FROM nodes WHERE document_id=0 AND key='extra') ORDER BY node_ordinal",(key,)).fetchall()==[('1',),('2',)]


@pytest.mark.parametrize('operation',['bulk','nodes','rebuild'])
def test_s2_current_row_budget(lake,operation):
    h,token,out=lake; reader=History(h.dsn,h.root,target_identity=h.target_identity,limits=replace(h.limits,max_row_bytes=64))
    with reader.collection(token) as session:
        with pytest.raises(ValueError):
            if operation=='bulk':
                with closing(session.bulk('nodes',batch_rows=1)) as stream: next(stream)
            elif operation=='nodes': session.nodes(0,limit=1)
            else: session.rebuild()
    assert session.receipt is None and session.db is None


def test_p2_caught_actual_page_failure(lake):
    h,token,out=lake
    # 两行各自合法、合页超限，不能只依赖S2顺便挡住此反例。
    with h.collection(token) as s:
        pair=s.nodes(0,limit=2)['rows']; widths=[len(row_bytes(r)) for r in pair]
    maximum=max(widths); reader=History(h.dsn,h.root,target_identity=h.target_identity,limits=replace(h.limits,max_row_bytes=maximum,batch_bytes=maximum))
    with reader.collection(token) as session:
        with pytest.raises(ValueError): session.nodes(0,limit=2)
    assert session.receipt is None and session.db is None


def test_p2_caught_actual_span_truncation(lake):
    h,token,out=lake
    with h.collection(token) as session:
        docs=[r for b in session.bulk('documents') for r in b['rows']]
        doc=next(d for d in docs if d['column_name']=='item')
        path=h.data_root/token.collection_id/'source'/doc['entity_path']; original=path.read_bytes()
        try:
            path.write_bytes(b'')
            with pytest.raises(ValueError,match='截断'): session.span(doc['document_id'],0)
        finally: path.write_bytes(original)
        assert session.failed and session.db is None
        with pytest.raises(ValueError): session.nodes(0,limit=1)
    assert session.receipt is None and session.db is None


@pytest.mark.parametrize('count,maximum,complete',[(200,500,True),(600,1000,False)])
def test_s1_import_uses_remaining_limits(lake,tmp_path,monkeypatch,count,maximum,complete):
    from data_pipeline.history.event_collection.children import ChildHistory
    h,_,out=lake; binding=extra_children(tmp_path/'source',count)
    manifest=freeze_collection(binding,tmp_path/'freeze')
    calls=[]; actual=ChildHistory.import_package
    def importing(self,path):
        calls.append(self.limits.max_total_rows); return actual(self,path)
    monkeypatch.setattr(ChildHistory,'import_package',importing)
    reader=History(h.dsn,h.root,target_identity=h.target_identity,limits=replace(h.limits,max_total_rows=maximum))
    if complete:
        token=reader.import_collection(manifest); assert len(token.children)==2
        assert calls==[maximum,maximum-count-1]
    else:
        with pytest.raises(ValueError,match='剩余'): reader.import_collection(manifest)
        assert calls==[]
        with closing(h._connect()) as pg,pg.cursor() as c:
            cid=json.loads(manifest.read_text())['collection_id']
            c.execute('SELECT state FROM history_q3.collections WHERE collection_id=%s',(cid,)); assert c.fetchone() is None
    save(out/f'修复-child-import-{count}.json',{'maximum':maximum,'actual_child_limits':calls,'complete':complete})


@pytest.mark.parametrize('stage',['freeze','import'])
def test_s1_remaining_bytes(stage,lake,tmp_path,monkeypatch):
    from data_pipeline.history.event_collection.children import ChildHistory
    h,_,out=lake; binding=extra_children(tmp_path/'source',count=40)
    initial=freeze_collection(binding,tmp_path/'initial')
    m=json.loads(initial.read_text())
    sizes=[]
    for path in m['children']:
        child=json.loads((initial.parent/path).read_text())
        sizes.append(sum(o['bytes'] for o in child['originals'])+sum(b['bytes'] for t in child['tables'] for b in t['blocks']))
    maximum=sum(sizes)-1; limits=replace(h.limits,max_total_bytes=maximum); calls=[]
    if stage=='freeze':
        actual=module.freeze_sqlite_source
        def freezing(*a,**kw): calls.append(kw['limits'].max_total_bytes); return actual(*a,**kw)
        monkeypatch.setattr(module,'freeze_sqlite_source',freezing)
        with pytest.raises(ValueError): freeze_collection(binding,tmp_path/'limited',limits=limits)
        assert len(list((tmp_path/'limited').glob('child-*/COMPLETE.json')))==1
        assert calls[1]==maximum-sizes[0]
    else:
        actual=ChildHistory.import_package
        def importing(self,path): calls.append(self.limits.max_total_bytes); return actual(self,path)
        monkeypatch.setattr(ChildHistory,'import_package',importing)
        with pytest.raises(ValueError): History(h.dsn,h.root,target_identity=h.target_identity,limits=limits).import_collection(initial)
        assert calls==[]
    save(out/f'修复-child-bytes-{stage}.json',{'maximum':maximum,'declared_child_data_bytes':sizes,'actual_child_limits':calls})


def test_s1_no_child_when_disk_reservation_missing(tmp_path,monkeypatch):
    from data_pipeline.history.event_collection import CollectionLimits
    binding=extra_children(tmp_path/'source',20); calls=[]
    monkeypatch.setattr(module,'freeze_sqlite_source',lambda *a,**kw:calls.append(True))
    with pytest.raises(ValueError,match='剩余'): freeze_collection(binding,tmp_path/'freeze',collection_limits=replace(CollectionLimits(),temporary_bytes=1024**2))
    assert calls==[] and not list((tmp_path/'freeze').glob('child-*/COMPLETE.json'))


def test_s1_same_remaining_rows_positive(tmp_path,monkeypatch):
    binding=extra_children(tmp_path/'source',200); calls=[]; actual=module.freeze_sqlite_source
    def freezing(*a,**kw): calls.append(kw['limits'].max_total_rows); return actual(*a,**kw)
    monkeypatch.setattr(module,'freeze_sqlite_source',freezing)
    freeze_collection(binding,tmp_path/'freeze',limits=replace(Limits(),max_total_rows=500))
    assert calls==[500,299]


def test_p2_bad_parameters_and_not_retained_do_not_poison(lake):
    h,token,out=lake
    with h.collection(token) as session:
        for action in (lambda:session.nodes(0,limit=0),lambda:session.span(0,-1),lambda:session.span(0,999999),lambda:session.nodes(2**100,limit=1),lambda:session.span(0,2**100)):
            with pytest.raises(ValueError): action()
            assert not session.failed and session.db is not None
        assert session.nodes(0,limit=1)['present']
    assert session.receipt['qualification']=='complete'


def test_s2_exact_row_boundary(lake):
    h,token,out=lake
    with h.collection(token) as s: row=s.nodes(0,limit=1)['rows'][0]
    width=len(row_bytes(row))
    reader=History(h.dsn,h.root,target_identity=h.target_identity,limits=replace(h.limits,max_row_bytes=width))
    with reader.collection(token) as s: assert len(row_bytes(s.nodes(0,limit=1)['rows'][0]))==width
    assert s.receipt


def test_s1_import_disk_budget_before_child(lake,tmp_path,monkeypatch):
    from data_pipeline.history.event_collection import CollectionLimits
    from data_pipeline.history.event_collection.children import ChildHistory
    h,_,out=lake; binding=extra_children(tmp_path/'source',20)
    manifest=freeze_collection(binding,tmp_path/'freeze'); calls=[]
    monkeypatch.setattr(ChildHistory,'import_package',lambda *a,**kw:calls.append(True))
    reader=History(h.dsn,h.root,target_identity=h.target_identity,collection_limits=replace(CollectionLimits(),temporary_bytes=1024**2))
    with pytest.raises(ValueError,match='剩余'): reader.import_collection(manifest)
    assert calls==[]
