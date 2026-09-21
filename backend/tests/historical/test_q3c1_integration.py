"""Q3-C.1 独立人工集成，显式绑定本任务私有PG与旧Token。"""
from contextlib import closing
from dataclasses import asdict,replace
from decimal import Decimal
from pathlib import Path
import hashlib
import json
import os
import sqlite3
import time
import uuid

import pytest
from data_pipeline.history.database_import import PGIdentity, Token
from data_pipeline.history.database_import.freeze import private_pg
from data_pipeline.history.event_collection import History, freeze_collection
from data_pipeline.history.event_collection.model import DEFINITIONS, code_identity
from tests.historical.test_historical_collection import fixture, sha, save, encoded, oracle_nodes
from tests.historical.test_historical_collection import test_real_lake_all_tables_and_spans as all_tables
from tests.historical.test_historical_collection import test_actual_new_process_source_removed as fresh_read
from tests.historical.test_historical_collection_repairs import test_s1_real_child_remaining_rows, test_s1_import_uses_remaining_limits, test_s2_current_row_budget, test_p2_caught_actual_page_failure, test_p2_caught_actual_span_truncation

RAW=b'{"a":1,"a":1.0,"zero":-0,"n":9007199254740993,"decimal":1.2300e+02,"list":[null,"null",3,3,1]}'


@pytest.fixture(scope='module')
def lake():
    location=os.environ.get('Q3_PRIVATE_ROOT')
    if not location:pytest.skip('需显式本任务私有PG')
    root=Path(location).resolve();out=root/'q3-c1'/('integration-'+uuid.uuid4().hex);out.mkdir(parents=True)
    base=f'host={root / "socket"} port=28763';name='q3c1_integrated_'+uuid.uuid4().hex[:10]
    with closing(private_pg(base+' dbname=postgres',root)) as pg:
        pg.autocommit=True
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    dsn=base+' dbname='+name
    with closing(private_pg(dsn,root)) as pg:identity=PGIdentity.read(pg)
    h=History(dsn,root,target_identity=identity);binding=fixture(out/'sources',rows=17,events=2)
    core=Path(binding.roots[0].path).parent
    with sqlite3.connect(core/'day.sqlite3') as db:
        for i in range(17):db.execute('UPDATE records SET item=? WHERE reference=?',(RAW.decode() if i%2==0 else RAW,str(i)))
    m=json.loads((core/'manifest.json').read_text());m['days']['2026-03-01']['sha256']=sha((core/'day.sqlite3').read_bytes())
    raw=encoded(m)[:-1]+b',"extra":{"source_note":1,"source_note":2,"publication_id":3,"publication_id":4}}'
    (core/'manifest.json').write_bytes(raw);binding=replace(binding,roots=(replace(binding.roots[0],sha256=sha(raw)),binding.roots[1]))
    identity_before=code_identity();start=time.monotonic();manifest=freeze_collection(binding,out/'freeze');freeze_wall=time.monotonic()-start
    start=time.monotonic();token=h.import_collection(manifest);import_wall=time.monotonic()-start
    assert code_identity()==identity_before
    save(out/'binding.json',{'dsn':dsn,'root':str(root),'identity':asdict(identity),'token':asdict(token),'manifest':str(manifest)})
    frozen=json.loads(manifest.read_text());ready=json.loads((h.data_root/token.collection_id/'ready.json').read_text())
    save(out/'阶段回执.json',{'freeze_wall_seconds':freeze_wall,'import_wall_seconds':import_wall,'frozen_resources':frozen['resources'],'import_resources':ready['resources'],
        'code_identity':identity_before,'tables':ready['tables'],'parquet_bytes':sum(x['bytes'] for x in ready['files']),'retained_bytes':sum(x['bytes'] for x in ready['artifacts'])})
    print('Q3C1_INTEGRATION_ROOT='+str(out))
    return h,token,out


def test_full_tables_independent_raw_oracle(lake):
    all_tables(lake)
    h,token,out=lake
    with h.collection(token) as s:tables={name:[r for b in s.bulk(name,batch_rows=17) for r in b['rows']] for name in DEFINITIONS}
    base=h.data_root/token.collection_id/'source'
    for file in tables['files']:
        assert sha((base/file['raw_path']).read_bytes())==file['raw_sha']
        if file['decoded_path']:assert sha((base/file['decoded_path']).read_bytes())==file['decoded_sha']
    keys=spans=0
    for doc in tables['documents']:
        data=(base/doc['entity_path']).read_bytes();raw=data[doc['byte_start']:doc['byte_end']]
        nodes=[n for n in tables['nodes'] if n['document_id']==doc['document_id']]
        actual=[(n['node_ordinal'],n['parent_ordinal'],n['member_ordinal'],n['key'],n['kind'],n['child_count'],
            n['number_lexeme'] if n['kind']=='number' else n['text_value'] if n['kind']=='string' else n['bool_value'] if n['kind']=='bool' else None) for n in nodes]
        assert actual==oracle_nodes(raw)
        for n in nodes:
            fragment=data[n['byte_start']:n['byte_end']];json.loads(fragment);spans+=1
            if n['key_start'] is not None:assert json.loads(data[n['key_start']:n['key_end']])==n['key'];keys+=1
            if n['number_lexeme'] is not None:
                assert fragment.decode()==n['number_lexeme']
                if n['decimal128_value'] is not None:assert type(n['decimal128_value']) is Decimal and n['decimal128_value']==Decimal(n['number_lexeme'])
        if doc['column_name']=='item':assert raw==RAW
    itemdocs=[d for d in tables['documents'] if d['column_name']=='item']
    assert len(itemdocs)==17 and {d['storage_class'] for d in itemdocs}=={'text','blob'}
    assert len([n for n in tables['nodes'] if n['number_lexeme']=='-0' and n['negative_zero']])==17
    rootnodes=[n for n in tables['nodes'] if n['document_id']==0];extra=next(n for n in rootnodes if n['key']=='extra')
    children=[n for n in rootnodes if n['parent_ordinal']==extra['node_ordinal']]
    assert [(n['key'],n['number_lexeme']) for n in children]==[('source_note','1'),('source_note','2'),('publication_id','3'),('publication_id','4')]
    assert not any(i['document_id']==0 and i['node_ordinal'] in {n['node_ordinal'] for n in children} for i in tables['identities'])
    assert len([a for a in tables['availability'] if a['state']=='validation_failed'])==4
    save(out/'全字段oracle.json',{'counts':{k:len(v) for k,v in tables.items()},'all_node_spans':spans,'all_key_spans':keys,'item_raw_rows':17,'ordinary_extra_occurrences':4})


def test_fresh_process_without_source_or_freeze(lake):fresh_read(lake)


def test_three_batch_costs_same_fixed_collection(lake,monkeypatch):
    from tests.country.country_query_test_support import PgLogProbe
    h,token,out=lake;probe=PgLogProbe(h.dsn,h.root/'q3-c1-pg.log');actual=hashlib.sha256;counts={'calls':0,'bytes':0}
    class Hash:
        def __init__(self,data=b'',**kw):counts['calls']+=1;counts['bytes']+=len(data);self.h=actual(data,**kw)
        def update(self,data):counts['bytes']+=len(data);self.h.update(data)
        def __getattr__(self,k):return getattr(self.h,k)
    monkeypatch.setattr(hashlib,'sha256',Hash);results=[]
    for cap in (1,17,1000):
        offset=probe.start();before=dict(counts);start=time.monotonic()
        with h.collection(token) as s:
            rows={name:sum(len(b['rows']) for b in s.bulk(name,batch_rows=cap)) for name in DEFINITIONS}
        wall=time.monotonic()-start;snippet,sql=probe.finish(offset);(out/f'批{cap}-PG.log').write_text(snippet)
        results.append({'batch':cap,'wall_seconds':wall,'pg_statements':sql,'sha_calls':counts['calls']-before['calls'],'sha_bytes':counts['bytes']-before['bytes'],'rows':rows,'resources':s.receipt['resources']})
    assert len({(r['pg_statements'],r['sha_calls'],r['sha_bytes']) for r in results})==1
    save(out/'三批实际成本.json',results)


def test_empty_profiles_are_structurally_complete(lake,tmp_path):
    h,_,out=lake;binding=fixture(tmp_path/'source',rows=0,events=0);manifest=freeze_collection(binding,tmp_path/'freeze');token=h.import_collection(manifest)
    with h.collection(token) as s:rows={name:[r for b in s.bulk(name) for r in b['rows']] for name in DEFINITIONS}
    assert not any(d['column_name']=='item' for d in rows['documents'])
    assert len([r for r in rows['availability'] if r['state']=='validation_failed'])==4
    assert s.receipt['business_admission']=='not_implemented'
    save(out/'空集合.json',{'token':asdict(token),'rows':{k:len(v) for k,v in rows.items()},'receipt':s.receipt})


def test_true_identity_duplicate_rejected(tmp_path):
    from tests.historical.test_historical_collection import test_closure_failures as probe
    probe(tmp_path,'duplicate_identity')


def test_five_original_q3a_q3b_tokens_without_reimport(lake,monkeypatch):
    from data_pipeline.history.database_import import History as OldHistory
    h,_,out=lake;root=h.root
    a=json.loads((root/'acceptance-35e14f37925748b7aa7daaca0ee20071/交付回执.json').read_text())
    prior=root/'q3-b/acceptance-7987cb988d3f4988b493e1d83c91f9c8'
    tokens=[Token(**a[k]) for k in ('pg_token','sqlite_token')]+[Token(**json.loads((prior/f).read_text())['token']) for f in ('人工绑定与索引.json','SQLite独立文件回执.json','PG源移除完整闭环.json')]
    assert len(set(tokens))==5;result=[]
    monkeypatch.setattr(OldHistory,'import_package',lambda *a,**kw:pytest.fail('旧Token不得重导'))
    for token in tokens:
        directory=root/'history'/token.import_id;before={str(p):sha(p.read_bytes()) for p in directory.rglob('*') if p.is_file()}
        ready=json.loads((directory/'ready.json').read_text());oid=ready['catalog_ref']['database_oid']
        with closing(h._connect()) as pg,pg.cursor() as cur:cur.execute('SELECT datname FROM pg_database WHERE oid=%s',(oid,));name=cur.fetchone()[0]
        old=OldHistory(f'host={root / "socket"} port=28763 dbname={name}',root)
        component=old.component(token);expected=[list(old.scan(token,i,limit=1000,purpose='audit')) for i in range(len(component['original']['tables']))]
        with old.bulk(token,batch_rows=17,purpose='audit') as stream:batches=list(stream)
        got=[[r for b in batches if b['table_index']==i for r in b['rows']] for i in range(len(expected))]
        assert repr(got)==repr(expected) and stream.receipt['qualification']=='complete'
        assert all(sha(Path(p).read_bytes())==v for p,v in before.items())
        result.append({'token':asdict(token),'table_rows':[len(r) for r in got],'unchanged_files':before,'receipt':stream.receipt})
    save(out/'五原Token兼容.json',result)
