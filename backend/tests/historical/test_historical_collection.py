"""Q3-C.1仅人工输入；预期字节/节点独立列明，不使用产品解析器产生oracle。"""
from contextlib import closing
from dataclasses import asdict, replace
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import uuid

import pytest

from data_pipeline.history.database_import import Limits, PGIdentity, Token
from data_pipeline.history.database_import.freeze import private_pg
from data_pipeline.history.event_collection import Binding, Root, CollectionLimits, CollectionToken, History, freeze_collection
from data_pipeline.history.event_collection.freeze import decode_gzip, Freezer
from data_pipeline.history.event_collection.json_tokens import Parser, number_parts, compare_numbers
from data_pipeline.history.event_collection.model import Budget, DEFINITIONS


def sha(data): return hashlib.sha256(data).hexdigest()
def encoded(value): return json.dumps(value,ensure_ascii=False,separators=(',',':'),default=str).encode()
def save(path,value): path.write_bytes(encoded(value))


def fixture(root,rows=2,events=2):
    core=root/'core'; general=root/'general'; core.mkdir(parents=True); general.mkdir()
    records=b''.join(encoded({'record':{'identity':{'legacy_reference':f'old:{i}'},'raw_fields':{'n':9007199254740993}}})+b'\n' for i in range(rows))
    (core/'records.jsonl').write_bytes(records)
    provenance=encoded({'schema_version':'core-overview-input/v1','source':{'code':'rrc25'},'interpretation_version':'recorded-anomaly-overview/v2',
                        'records':{'file':'records.jsonl','sha256':sha(records),'count':rows}})
    with sqlite3.connect(core/'day.sqlite3') as db:
        db.execute('CREATE TABLE records(reference TEXT PRIMARY KEY,item TEXT,payload BLOB)')
        db.execute('CREATE TABLE provenance(manifest BLOB)')
        db.execute('INSERT INTO provenance VALUES (?)',(provenance,))
        for i in range(rows):
            db.execute('INSERT INTO records VALUES (?,?,?)',(str(i),'{"a":1,"a":1.0,"b":[null,"null",1e0],"n":9007199254740993}',b'{"schema_version":"anomaly-record/v1","record":{"raw_fields":{"p":{"$anomaly_scalar":"decimal","value":"1.2300"}}}}'))
    day_sha=sha((core/'day.sqlite3').read_bytes())
    diagnostics={}
    for day in ('2026-03-03','2026-03-04','2026-03-10','2026-03-20'):
        raw=encoded({'schema_version':'core-overview-diagnostic/v1','stage':'source_field_precheck','reasons':[{'code':day,'count':2,'evidence':{'source_data_sha256':'a'*64}}]})
        (core/(day+'.json')).write_bytes(raw); diagnostics[day]={'file':day+'.json','sha256':sha(raw)}
    manifest={'schema_version':'core-overview-index/v2','source':{'code':'rrc25'},'interpretation_version':'recorded-anomaly-overview/v3',
              'days':{'2026-03-01':{'file':'day.sqlite3','sha256':day_sha,'count':rows,'input_version':'original-input','interpretation_version':'recorded-anomaly-overview/v2'}},'diagnostics':diagnostics}
    save(core/'manifest.json',manifest)
    event_list=[]
    for i in range(events):
        event={'schema_version':'country-outage-general-event-read-model/v1','publication_id':f'old-p-{i}','revision':1,'incident_id':f'old-i-{i}',
               'event_read_model_id':f'old-rm-{i}','legacy_reference':f'old:{i}','cohort_id':f'old-c-{i}', 'event_metric_id':f'old-m-{i}', 'event_as_path_id':f'old-path-{i}',
               'affected_as_count':rows,'path_downstream_relation_count':rows,'state_point_count':rows,'path_sample_count':rows,'is_final':False}
        specs={'overview':('country-outage-general-overview-artifact/v1',False), 'series':('country-outage-general-series-artifact/v1',False),
               'affected_as':('country-outage-general-affected-as/v1',True),'path_downstreams':('country-outage-general-path-downstream/v1',True)}
        for key,(schema,jsonl) in specs.items():
            objects=[{'schema_version':schema,'event_read_model_id':event['event_read_model_id'],'publication_id':event['publication_id'],
                      'rank':j,'asn':42,'value':None,'event_end':None,'is_final':False,'samples':[{'as_path_canonical':'{328405}','independent_peer_asns':[42,42]}]} for j in range(rows)]
            if key=='series': objects[0].update(timestamps=list(range(rows)),tracks={'x':list(range(rows))},point_count=rows)
            raw=b'\r\n'.join(encoded(x) for x in objects) if jsonl else encoded(objects[0])
            compressed=gzip.compress(raw,mtime=0); filename=f'{i}-{key}.gz'; (general/filename).write_bytes(compressed)
            event[key]={'path':filename,'sha256':sha(compressed),'content_sha256':sha(raw),'size_bytes':len(compressed)}
            if jsonl: event[key]['row_count']=rows
        event_list.append(event)
    manifest={'schema_version':'country-outage-general-read-model-store/v1','status':'complete','dataset_id':'old-dataset','run_id':'old-run',
              'source_event_cohort_dataset_id':'original-upstream-only','event_count':events,'events':event_list,
              'state_point_count':rows*events,'affected_as_count':rows*events,'path_downstream_relation_count':rows*events,'path_sample_count':rows*events}
    data=encoded(manifest); (general/'manifest.json').write_bytes(data); (general/'COMPLETE.json').write_bytes(data)
    roots=tuple(Root(profile,str(p/'manifest.json'),f'fixture://{p.name}/manifest','original-v1',sha((p/'manifest.json').read_bytes())) for profile,p in [('core-index/v1',core),('general-read-model/v1',general)])
    return Binding(roots,(str(core),str(general)))


def test_exact_ordered_tokens_independent_oracle(tmp_path):
    data=b'{"a":1,"a":1.0,"b":[null,"null",1e0]}'
    rows=[]; b=Budget(tmp_path,collection_limits=CollectionLimits(chunk_bytes=1)); root,count,end=Parser(io.BytesIO(data),b,rows.append).parse()
    rows.sort(key=lambda r:r['node_ordinal'])
    expected=[(0,None,0,'object',None,b'{"a":1,"a":1.0,"b":[null,"null",1e0]}'),
              (1,0,0,'number','a',b'1'),(2,0,1,'number','a',b'1.0'),(3,0,2,'array','b',b'[null,"null",1e0]'),
              (4,3,0,'null',None,b'null'),(5,3,1,'string',None,b'"null"'),(6,3,2,'number',None,b'1e0')]
    assert [(r['node_ordinal'],r['parent_ordinal'],r['member_ordinal'],r['kind'],r['key'],data[r['byte_start']:r['byte_end']]) for r in rows]==expected
    assert count==7 and end==len(data) and rows[0]['child_count']==3 and rows[3]['child_count']==3
    assert compare_numbers(rows[1],rows[2])==compare_numbers(rows[2],rows[6])==0
    assert rows[5]['text_value']=='null'


@pytest.mark.parametrize('data',[b'{"x":NaN}',b'{"x":Infinity}',b'{"x":"\\ud800"}',b'{"x":"\xff"}',b'[1,]',b'{"a":01}',b'{} {}',b'{',b'',b'{"a":1e100001}'])
def test_strict_json_rejects(tmp_path,data):
    with pytest.raises(ValueError): Parser(io.BytesIO(data),Budget(tmp_path),lambda r:None).parse()


@pytest.mark.parametrize('raw,parts',[(b'-0',(-1,'0',0,True)),(b'1.2300e+02',(1,'12300',-2,False)),(b'9007199254740993',(1,'9007199254740993',0,False)),(b'1e99999',(1,'1',99999,False))])
def test_exact_numbers(tmp_path,raw,parts):
    n=number_parts(raw,CollectionLimits())
    assert tuple(n[k] for k in ('sign','coefficient_digits','exponent10','negative_zero'))==parts
    assert compare_numbers(n,n)==0


@pytest.mark.parametrize('change',['truncated','crc','isize','second','padding'])
def test_gzip_failure(tmp_path,change):
    data=gzip.compress(b'{"x":1}',mtime=0)
    if change=='truncated': data=data[:-1]
    if change=='crc': data=data[:-8]+bytes([data[-8]^1])+data[-7:]
    if change=='isize': data=data[:-1]+bytes([data[-1]^1])
    if change=='second': data+=gzip.compress(b'{}')
    if change=='padding': data+=b'\0'
    (tmp_path/'in').write_bytes(data)
    with pytest.raises(ValueError): decode_gzip(tmp_path/'in',tmp_path/'out',Budget(tmp_path))


def test_two_profiles_and_all_references(tmp_path):
    binding=fixture(tmp_path/'sources')
    manifest=freeze_collection(binding,tmp_path/'package')
    value=json.loads(manifest.read_text()); assert value['integrity_state']=='complete'
    with sqlite3.connect(manifest.parent/'structure.sqlite') as db:
        assert db.execute('SELECT DISTINCT profile FROM files ORDER BY profile').fetchall()==[('core-index/v1',),('general-read-model/v1',)]
        docs=db.execute("SELECT table_name,column_name,storage_class,count(*) FROM documents WHERE table_name IS NOT NULL GROUP BY 1,2,3 ORDER BY 1,2").fetchall()
        assert docs==[('provenance','manifest','blob',1),('records','item','text',2),('records','payload','blob',2)]
        assert db.execute("SELECT scope FROM availability WHERE state='validation_failed' ORDER BY scope").fetchall()==[(d,) for d in ('2026-03-03','2026-03-04','2026-03-10','2026-03-20')]
        edges=db.execute('SELECT role,relation_kind,resolution,target_file FROM edges ORDER BY edge_ordinal').fetchall()
        assert len(edges)==16 and all(target is not None for _,kind,_,target in edges if kind=='required_file')
        expected_refs=[(1,'records.jsonl','records-jsonl',2),(0,'day.sqlite3','day-sqlite',1)]
        expected_refs += [(0,d+'.json','diagnostic',i+3) for i,d in enumerate(('2026-03-03','2026-03-04','2026-03-10','2026-03-20'))]
        expected_refs += [(7,'COMPLETE.json','original-complete',8)]
        expected_refs += [(7,f'{i}-{key}.gz',key,9+i*4+j) for i in range(2) for j,key in enumerate(('overview','series','affected_as','path_downstreams'))]
        expected_refs += [(7,'original-upstream-only','source_event_cohort_dataset_id',None)]
        assert db.execute('SELECT parent_file,reference,role,target_file FROM edges ORDER BY edge_ordinal').fetchall()==expected_refs
        assert [e[:3] for e in edges if e[1]=='identity_only']==[('source_event_cohort_dataset_id','identity_only','unresolved_external')]
        # 每个成员的原字节定位以及全文件SHA，覆盖全部节点，不抽样。
        for path,digest in db.execute('SELECT raw_path,raw_sha FROM files'):
            assert sha((manifest.parent/path).read_bytes())==digest
        for entity,start,end in db.execute('SELECT d.entity_path,n.byte_start,n.byte_end FROM nodes n JOIN documents d USING(document_id) ORDER BY document_id,node_ordinal'):
            span=(manifest.parent/entity).read_bytes()[start:end]
            assert span and json.loads(span) is not ...


@pytest.mark.parametrize('change',['missing','extra','symlink','duplicate_identity','bad_complete','wrong_count'])
def test_closure_failures(tmp_path,change):
    binding=fixture(tmp_path/'sources'); core=Path(binding.roots[0].path).parent; general=Path(binding.roots[1].path).parent
    if change=='missing': (core/'day.sqlite3').unlink()
    if change=='extra': (core/'extra').write_text('extra')
    if change=='symlink':
        p=core/'day.sqlite3'; p.rename(core/'moved'); p.symlink_to(core/'moved')
    if change=='duplicate_identity':
        data=(general/'manifest.json').read_bytes().replace(b'"publication_id":"old-p-0"',b'"publication_id":"old-p-0","publication_id":"hidden"',1)
        (general/'manifest.json').write_bytes(data); (general/'COMPLETE.json').write_bytes(data)
        binding=replace(binding,roots=(binding.roots[0],replace(binding.roots[1],sha256=sha(data))))
    if change=='bad_complete': (general/'COMPLETE.json').write_text('{}')
    if change=='wrong_count':
        data=(general/'manifest.json').read_bytes().replace(b'"row_count":2',b'"row_count":3',1)
        (general/'manifest.json').write_bytes(data); (general/'COMPLETE.json').write_bytes(data)
        binding=replace(binding,roots=(binding.roots[0],replace(binding.roots[1],sha256=sha(data))))
    with pytest.raises((ValueError,OSError)): freeze_collection(binding,tmp_path/'package')
    assert not (tmp_path/'package'/'COMPLETE.json').exists()
    assert (tmp_path/'package'/'FAILED.json').exists()


@pytest.fixture(scope='module')
def lake():
    location=os.environ.get('Q3_PRIVATE_ROOT')
    if not location: pytest.skip('仅显式本任务人工PG')
    root=Path(location).resolve(); out=root/'q3-c1'/('acceptance-'+uuid.uuid4().hex); out.mkdir(parents=True)
    base=f'host={root / "socket"} port=28763'; name='q3c1_'+uuid.uuid4().hex[:12]
    pg=private_pg(base+' dbname=postgres',root); pg.autocommit=True
    with pg.cursor() as c: c.execute('CREATE DATABASE '+name)
    pg.close(); dsn=base+' dbname='+name
    with closing(private_pg(dsn,root)) as pg: identity=PGIdentity.read(pg)
    h=History(dsn,root,target_identity=identity)
    source=out/'sources'; binding=fixture(source)
    manifest=freeze_collection(binding,out/'freeze')
    token=h.import_collection(manifest)
    save(out/'binding.json',{'dsn':dsn,'root':str(root),'identity':asdict(identity),'token':asdict(token),'manifest':str(manifest)})
    print('Q3C1_ARTIFICIAL_EVIDENCE='+str(out))
    return h,token,out


def token_from(value):
    value=dict(value); value['children']=tuple(Token(**t) for t in value['children']); return CollectionToken(**value)


def test_real_lake_all_tables_and_spans(lake):
    h,token,out=lake
    with h.collection(token) as session:
        rows={}
        for name in DEFINITIONS:
            rows[name]=[r for batch in session.bulk(name,batch_rows=1) for r in batch['rows']]
        duplicate=next(r for r in rows['nodes'] if r['key']=='a')
        doc=duplicate['document_id']
        assert [r['number_lexeme'] for r in session.nodes(doc,parent=0,key='a')['rows']]==['1','1.0']
        assert session.nodes(doc,parent=0,key='not_present')['present'] is False
        assert session.span(doc,duplicate['node_ordinal'])==b'1'
        assert session.receipt is None
    assert session.receipt['datasets_exhausted']==sorted(DEFINITIONS)
    save(out/'ordered-typed.json',rows); save(out/'whole-session.json',session.receipt)
    digests={}
    from decimal import Decimal
    for name,values in rows.items():
        digest=hashlib.sha256()
        for row in values: digest.update(json.dumps(row,sort_keys=True,ensure_ascii=True,separators=(',',':'),default=lambda v:format(v,'f') if isinstance(v,Decimal) else str(v)).encode()+b'\n')
        digests[name]=digest.hexdigest()
    save(out/'expected-digests.json',digests)
    assert not (h.data_root/token.collection_id/'source'/'structure.sqlite').exists()


def test_actual_new_process_source_removed(lake):
    h,token,out=lake
    (out/'sources').rename(out/'sources-removed')
    (out/'freeze').rename(out/'freeze-removed')
    # 不传源路径/环境；真实新Python只取显式目标及固定token。
    code='''
import json,sys,hashlib
from decimal import Decimal
from pathlib import Path
from data_pipeline.history.database_import import PGIdentity, Token
from data_pipeline.history.event_collection import History, CollectionToken
p=Path(sys.argv[1]); value=json.loads((p/'binding.json').read_text()); t=value['token']; t['children']=tuple(Token(**c) for c in t['children'])
h=History(value['dsn'],value['root'],target_identity=PGIdentity(**value['identity']))
with h.collection(CollectionToken(**t)) as s:
    counts={}; digests={}
    for name in ('files','documents','nodes','edges','identities','availability'):
        digest=hashlib.sha256(); counts[name]=0
        for batch in s.bulk(name,batch_rows=17):
            for row in batch['rows']:
                counts[name]+=1
                digest.update(json.dumps(row,sort_keys=True,ensure_ascii=True,separators=(',',':'),default=lambda v:format(v,'f') if isinstance(v,Decimal) else str(v)).encode()+bytes([10]))
        digests[name]=digest.hexdigest()
    assert digests==json.loads((p/'expected-digests.json').read_text())
    assert s.nodes(0,limit=17)['rows']
    assert s.span(0,0).startswith(b'{')
    rebuilt=s.rebuild()
assert s.receipt and rebuilt['nodes_copied_to_pg']==0
(p/'new-process.json').write_text(json.dumps({'counts':counts,'receipt':s.receipt,'rebuild':rebuilt},ensure_ascii=False,indent=2))
'''
    result=subprocess.run([sys.executable,'-c',code,str(out)],cwd=Path(__file__).resolve().parents[2],capture_output=True,text=True)
    (out/'new-process.log').write_text(result.stdout+result.stderr)
    assert result.returncode==0,result.stderr
    report=json.loads((out/'new-process.json').read_text())
    expected=json.loads((out/'ordered-typed.json').read_text())
    assert report['counts']=={k:len(v) for k,v in expected.items()}
    with closing(h._connect()) as pg,pg.cursor() as c:
        c.execute('SELECT table_name FROM information_schema.tables WHERE table_schema=%s',(report['rebuild']['schema'],))
        assert c.fetchall()==[('documents',)]
        from psycopg2 import sql
        c.execute(sql.SQL('SELECT document_id,file_id,entity_path,byte_start,byte_end,node_count FROM {}.documents ORDER BY document_id').format(sql.Identifier(report['rebuild']['schema'])))
        assert c.fetchall()==[tuple(r[k] for k in ('document_id','file_id','entity_path','byte_start','byte_end','node_count')) for r in expected['documents']]


@pytest.mark.parametrize('cap',[1,17,1000])
def test_session_qualification_constant(lake,monkeypatch,cap):
    h,token,out=lake; calls=[]; original=h._qualify_collection
    def qualify(*a,**kw): calls.append(kw.get('lock',False)); return original(*a,**kw)
    monkeypatch.setattr(h,'_qualify_collection',qualify)
    with h.collection(token) as session:
        count=sum(len(b['rows']) for b in session.bulk('nodes',batch_rows=cap))
    assert calls==[False,True]
    save(out/f'cost-cap-{cap}.json',{'rows':count,'qualifications':calls,'receipt':session.receipt})


def test_budget_drift_before_entry_and_yield(lake,monkeypatch):
    h,token,out=lake; original=h.limits
    session=h.collection(token); monkeypatch.setattr(h,'limits',replace(original,batch_rows=17))
    with pytest.raises(ValueError,match='预算漂移'): session.__enter__()
    monkeypatch.setattr(h,'limits',original)
    with pytest.raises(ValueError,match='预算漂移'):
        with h.collection(token) as session:
            with closing(session.bulk('nodes',batch_rows=1)) as stream:
                next(stream); monkeypatch.setattr(h,'limits',replace(original,batch_rows=17)); next(stream)
    assert session.receipt is None and session.db is None
    monkeypatch.setattr(h,'limits',original)


def test_early_close_no_receipt(lake):
    h,token,out=lake
    with h.collection(token) as session:
        with closing(session.bulk('nodes',batch_rows=1)) as stream: next(stream)
    assert session.receipt is None


@pytest.mark.parametrize('field,value',[('nodes',10),('decoded_bytes',100),('temporary_bytes',1000),('files',2),('edges',2),('line_bytes',20),('document_nodes',2),('depth',1),('number_digits',2),('token_bytes',8)])
def test_cumulative_limits_fail_closed(tmp_path,field,value):
    binding=fixture(tmp_path/'sources'); limits=replace(CollectionLimits(),**{field:value})
    with pytest.raises(ValueError): freeze_collection(binding,tmp_path/'package',collection_limits=limits)
    assert not (tmp_path/'package'/'COMPLETE.json').exists()


def test_copy_source_inode_replaced_after_copy(tmp_path,monkeypatch):
    binding=fixture(tmp_path/'sources'); original=Freezer.copy
    def copy(self,path,dest,sha):
        original(self,path,dest,sha)
        if Path(path).name=='day.sqlite3':
            p=Path(path); replacement=p.with_suffix('.replacement'); replacement.write_bytes(p.read_bytes()); replacement.replace(p)
    monkeypatch.setattr(Freezer,'copy',copy)
    with pytest.raises(ValueError,match='最终源实体'): freeze_collection(binding,tmp_path/'package')
    assert not (tmp_path/'package'/'COMPLETE.json').exists()


def test_dependency_cycle_and_required_external(tmp_path):
    for case in ('cycle','external'):
        source=tmp_path/case; binding=fixture(source)
        core=Path(binding.roots[0].path).parent
        with sqlite3.connect(core/'day.sqlite3') as db:
            raw=json.loads(db.execute('SELECT manifest FROM provenance').fetchone()[0]); raw['records']['file']='day.sqlite3' if case=='cycle' else 'file://unbound/records.jsonl'
            db.execute('UPDATE provenance SET manifest=?',(encoded(raw),))
        path=core/'manifest.json'; value=json.loads(path.read_text()); value['days']['2026-03-01']['sha256']=sha((core/'day.sqlite3').read_bytes()); save(path,value)
        binding=replace(binding,roots=(replace(binding.roots[0],sha256=sha(path.read_bytes())),binding.roots[1]))
        with pytest.raises(ValueError,match='依赖环|未绑定'): freeze_collection(binding,tmp_path/(case+'-package'))


def test_unknown_member_duplicates_retained_and_utf8_blocks(tmp_path):
    data='{"x":"中文\\n\\ud83d\\ude00","x":null}'.encode()
    rows=[]; Parser(io.BytesIO(data),Budget(tmp_path,collection_limits=CollectionLimits(chunk_bytes=1)),rows.append).parse()
    rows.sort(key=lambda r:r['node_ordinal'])
    assert [r['key'] for r in rows]==[None,'x','x'] and rows[1]['text_value']=='中文\n😀'


def test_gzip_all_truncation_boundaries_and_cumulative_expansion(tmp_path):
    raw='{"中文":'.encode()+b'"'+b'x'*100000+b'"}'
    good=gzip.compress(raw,mtime=0)
    for i in range(len(good)):
        source=tmp_path/'in'; source.write_bytes(good[:i]); target=tmp_path/str(i)
        with pytest.raises(ValueError): decode_gzip(source,target,Budget(tmp_path,collection_limits=CollectionLimits(chunk_bytes=7)))
    source.write_bytes(good)
    with pytest.raises(ValueError,match='累计预算'): decode_gzip(source,tmp_path/'bomb',Budget(tmp_path,collection_limits=CollectionLimits(decoded_bytes=1000,chunk_bytes=7)))


def test_final_artifact_drift_no_receipt(lake):
    h,token,out=lake; artifact=h.data_root/token.collection_id/'source'/'raw-0'; original=artifact.read_bytes()
    try:
        with pytest.raises(ValueError,match='损坏'):
            with h.collection(token) as session:
                for _ in session.bulk('nodes'): pass
                artifact.write_bytes(original+b' ')
        assert session.receipt is None and session.db is None
    finally: artifact.write_bytes(original)


def test_cursor_binding_and_full_pages(lake):
    h,token,out=lake
    with h.collection(token) as session:
        expected=[r for b in session.bulk('nodes') for r in b['rows'] if r['document_id']==0]
        got=[]; cursor=None
        while True:
            page=session.nodes(0,limit=1,cursor=cursor); got+=page['rows']; cursor=page['next_cursor']
            if cursor is None: break
        assert got==expected
        cursor=session.nodes(0,limit=1)['next_cursor']
        with pytest.raises(ValueError,match='游标'): session.nodes(1,limit=1,cursor=cursor)


class Pairs(list): pass
class Number(str): pass


def oracle_nodes(raw):
    """独立标准库语法树：原数字词法及对象pairs，未调用产品parser。"""
    tree=json.loads(raw,object_pairs_hook=Pairs,parse_int=Number,parse_float=Number)
    result=[]
    def visit(value,parent=None,member=0,key=None):
        ordinal=len(result)
        kind='object' if isinstance(value,Pairs) else 'array' if isinstance(value,list) else 'number' if isinstance(value,Number) else 'null' if value is None else 'bool' if isinstance(value,bool) else 'string'
        count=len(value) if kind in ('object','array') else 0
        result.append((ordinal,parent,member,key,kind,count,str(value) if kind in ('number','string') else value if kind=='bool' else None))
        if kind=='object':
            for i,(k,v) in enumerate(value): visit(v,ordinal,i,k)
        elif kind=='array':
            for i,v in enumerate(value): visit(v,ordinal,i)
    visit(tree)
    return result


def test_all_ordered_nodes_independent_tree(lake):
    h,token,out=lake
    expected=json.loads((out/'ordered-typed.json').read_text()); nodes=expected['nodes']
    for doc in expected['documents']:
        raw=(h.data_root/token.collection_id/'source'/doc['entity_path']).read_bytes()[doc['byte_start']:doc['byte_end']]
        actual=[(n['node_ordinal'],n['parent_ordinal'],n['member_ordinal'],n['key'],n['kind'],n['child_count'],
                 n['number_lexeme'] if n['kind']=='number' else n['text_value'] if n['kind']=='string' else n['bool_value'] if n['kind']=='bool' else None)
                for n in nodes if n['document_id']==doc['document_id']]
        assert actual==oracle_nodes(raw)


def test_existing_own_tokens_read_without_reimport(lake,monkeypatch):
    h,_,out=lake; root=h.root; tokens=[]
    for version,path,key in [('b1','交付索引.json','资源与组件'),('4c','repair-b1c55d2/交付索引.json','资源')]:
        value=json.loads((root/path).read_text())
        tokens.extend((version,Token(**item['token'])) for item in value[key].values())
    old=json.loads((root/'acceptance-9984ffb3d02b4920bd4a093fcf25329b'/'交付回执.json').read_text())
    tokens.extend(('626',Token(**old[k])) for k in ('pg_token','sqlite_token'))
    old=json.loads((root/'q3-b/budget-ec/trace-e922b76124cc4a93a19f4f7fd37ca6fd/normal_1.json').read_text())
    tokens.append(('930',Token(**old['receipt']['token'])))
    proof=[]
    seen_tokens=set()
    for version,token in tokens:
        if token in seen_tokens: continue
        seen_tokens.add(token)
        ready=json.loads((root/'history'/token.import_id/'ready.json').read_text()); catalog=ready['catalog_ref']
        with closing(private_pg(f'host={root / "socket"} port=28763 dbname=postgres',root)) as pg,pg.cursor() as c:
            c.execute('SELECT datname FROM pg_database WHERE oid=%s',(catalog['database_oid'],)); name=c.fetchone()[0]
        reader=History(f'host={root / "socket"} port=28763 dbname={name}',root,target_identity=PGIdentity(catalog['system_identifier'],catalog['database_oid']))
        monkeypatch.setattr(reader,'import_package',lambda *a:pytest.fail('旧Token不得重导'))
        before=sha((root/'history'/token.import_id/'ready.json').read_bytes())
        with reader.bulk(token) as stream:
            count=sum(len(b['rows']) for b in stream)
        assert stream.receipt and before==sha((root/'history'/token.import_id/'ready.json').read_bytes())
        proof.append({'version':version,'aliases':[v for v,t in tokens if t==token],'token':asdict(token),'rows':count,'receipt':stream.receipt,'reimported':False})
    save(out/'existing-token-compatibility.json',proof)


@pytest.mark.parametrize('rows,events',[(17,2),(65,2),(1025,2),(1,81)])
def test_measured_cost_shapes(tmp_path,monkeypatch,rows,events):
    if os.environ.get('Q3C_COST')!='1': pytest.skip('显式必要成本矩阵')
    root=Path(os.environ['Q3_PRIVATE_ROOT']); out=root/'q3-c1'/f'cost-{rows}-{events}-{uuid.uuid4().hex}'; out.mkdir(parents=True)
    binding=fixture(out/'sources',rows=rows,events=events)
    base=f'host={root / "socket"} port=28763'; name='q3cc_'+uuid.uuid4().hex[:12]
    with closing(private_pg(base+' dbname=postgres',root)) as pg:
        pg.autocommit=True
        with pg.cursor() as c: c.execute('CREATE DATABASE '+name)
    dsn=base+' dbname='+name
    with closing(private_pg(dsn,root)) as pg: identity=PGIdentity.read(pg)
    history=History(dsn,root,target_identity=identity)
    log=root/'q3-c1-pg.log'; real_sha=hashlib.sha256; counters={'python_sha256_calls':0,'python_sha256_bytes':0}; phases=[]
    class Hash:
        def __init__(self,data=b'',**kw): counters['python_sha256_calls']+=1; counters['python_sha256_bytes']+=len(data); self.h=real_sha(data,**kw)
        def update(self,data): counters['python_sha256_bytes']+=len(data); self.h.update(data)
        def __getattr__(self,key): return getattr(self.h,key)
    monkeypatch.setattr(hashlib,'sha256',Hash)
    import time
    def phase(label,action):
        offset=log.stat().st_size; before=dict(counters); started=time.monotonic()
        value=action(); elapsed=time.monotonic()-started
        stats={k:v-before[k] for k,v in counters.items()}
        with log.open('rb') as f: f.seek(offset); raw=f.read()
        # 本任务独占私有PG实例，使用服务端实际日志，包括DuckLake隐藏PG连接。
        text=raw.decode(); statements=sum('statement:' in line or 'execute <unnamed>:' in line for line in text.splitlines())
        (out/(label+'.pg.log')).write_bytes(raw)
        phases.append({'phase':label,'elapsed_seconds':elapsed,'actual_private_pg_logged_statements':statements,**stats})
        return value
    manifest=phase('freeze',lambda:freeze_collection(binding,out/'freeze'))
    token=phase('import',lambda:history.import_collection(manifest))
    receipts=[]
    for cap in (1,17,1000):
        def read():
            with history.collection(token) as s:
                for table in DEFINITIONS:
                    for batch in s.bulk(table,batch_rows=cap): pass
            return s.receipt
        receipts.append(phase('read-'+str(cap),read))
    assert len({p['actual_private_pg_logged_statements'] for p in phases[2:]})==1
    assert len({p['python_sha256_calls'] for p in phases[2:]})==1
    assert len({p['python_sha256_bytes'] for p in phases[2:]})==1
    ready=json.loads((history.data_root/token.collection_id/'ready.json').read_text()); frozen=json.loads(manifest.read_text())
    table_counts={t['name']:t['rows'] for t in ready['tables']}
    report={'fixture_rows':rows,'fixture_events':events,'general_files':events*4,'token':asdict(token),'phases':phases,
            'frozen_resources':frozen['resources'],'import_resources':ready['resources'],'tables':table_counts,'receipts':receipts,
            'node_expansion_per_document':table_counts['nodes']/table_counts['documents'],
            'scope':'Python SHA256调用含模块身份/原件/封存/typed摘要；read_blocks仅集合显式Python读取边界，不含SQLite/PG/DuckDB内部OS读取；Arrow批为本集合native读写，不冒充总PG行数',
            'parquet_bytes':sum(f['bytes'] for f in ready['files']),'retained_artifact_bytes':sum(f['bytes'] for f in ready['artifacts'])}
    save(out/'实际成本.json',report)
    print('Q3C1_COST_EVIDENCE='+str(out))


def test_shared_file_edges_and_equal_bytes_distinct_files(tmp_path):
    binding=fixture(tmp_path/'sources'); core=Path(binding.roots[0].path).parent
    value=json.loads((core/'manifest.json').read_text()); entry=value['days']['2026-03-01']
    value['days']['2026-03-02']=dict(entry)
    shutil.copyfile(core/'day.sqlite3',core/'equal.sqlite3')
    value['days']['2026-03-05']={**entry,'file':'equal.sqlite3'}
    save(core/'manifest.json',value)
    binding=replace(binding,roots=(replace(binding.roots[0],sha256=sha((core/'manifest.json').read_bytes())),binding.roots[1]))
    manifest=freeze_collection(binding,tmp_path/'package')
    with sqlite3.connect(manifest.parent/'structure.sqlite') as db:
        ids=[r[0] for r in db.execute("SELECT target_file FROM edges WHERE parent_file=0 AND role='day-sqlite' ORDER BY edge_ordinal")]
        assert ids[0]==ids[1] and ids[2]!=ids[0]
        assert db.execute("SELECT count(*),count(DISTINCT raw_sha) FROM files WHERE role='day-sqlite'").fetchone()==(2,1)
        assert db.execute("SELECT count(*) FROM edges WHERE role='records-jsonl'").fetchone()==(2,)


def test_budget_drift_before_final_qualification(lake,monkeypatch):
    h,token,out=lake; original=h.collection_limits
    try:
        with pytest.raises(ValueError,match='预算漂移'):
            with h.collection(token) as session:
                for _ in session.bulk('nodes'): pass
                monkeypatch.setattr(h,'collection_limits',replace(original,nodes=original.nodes-1))
        assert session.receipt is None and session.db is None
    finally: monkeypatch.setattr(h,'collection_limits',original)


def test_repeated_root_rejected_without_silent_scope_merge(tmp_path):
    binding=fixture(tmp_path/'sources'); binding=replace(binding,roots=(binding.roots[0],binding.roots[0]))
    with pytest.raises(ValueError,match='重复根'): freeze_collection(binding,tmp_path/'package')
    assert not (tmp_path/'package').exists()


def test_original_sqlite_wal_cannot_be_hidden_by_copy(tmp_path):
    binding=fixture(tmp_path/'sources'); core=Path(binding.roots[0].path).parent
    (core/'day.sqlite3-wal').write_bytes(b'artificial-wal')
    with pytest.raises(ValueError,match='原SQLite存在WAL'): freeze_collection(binding,tmp_path/'package')
    assert not (tmp_path/'package'/'COMPLETE.json').exists()


def test_paged_session_actual_cost(lake):
    h,token,out=lake; log=h.root/'q3-c1-pg.log'; reports=[]
    for cap in (1,17,60):
        offset=log.stat().st_size
        with h.collection(token) as session:
            cursor=None; ordinals=[]; pages=0
            while True:
                result=session.nodes(0,limit=cap,cursor=cursor); pages+=1
                ordinals.extend(r['node_ordinal'] for r in result['rows']); cursor=result['next_cursor']
                if cursor is None: break
        assert ordinals==list(range(len(ordinals)))
        with log.open('rb') as f: f.seek(offset); raw=f.read()
        actual=sum('statement:' in line or 'execute <unnamed>:' in line for line in raw.decode().splitlines())
        (out/f'pages-{cap}.pg.log').write_bytes(raw)
        reports.append({'page_size':cap,'pages':pages,'nodes':len(ordinals),'actual_pg_statements':actual,'receipt':session.receipt})
    assert len({r['nodes'] for r in reports})==1
    assert len({r['receipt']['resources']['hash_calls'] for r in reports})==1
    assert len({r['receipt']['resources']['hash_bytes'] for r in reports})==1
    save(out/'分页实际成本.json',reports)
