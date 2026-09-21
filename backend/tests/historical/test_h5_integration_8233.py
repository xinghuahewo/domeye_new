"""8233自有H5基础一次生产、原Token兼容和有限拒绝路径。"""
import os
import json
import hashlib
import subprocess
import sys
import uuid
from pathlib import Path
from dataclasses import asdict
from contextlib import closing
import pytest
if os.environ.get('DOMEYE_H5_INTEGRATION')!='8233':pytest.skip('仅显式自有人工H5集成',allow_module_level=True)
from tests.historical import test_historical_general as b
from tests.historical import test_historical_general_constructor as ctor
from tests.country.country_query_test_support import PgLogProbe
OUT=Path('/tmp/domeye-h5-integration-8233').resolve();ROOT=Path('/tmp/domeye-integration-q3-8233').resolve()
def save(name,v):b.save(OUT/name,v)
def hashes(root):return {str(p):b.sha(p) for p in root.rglob('*') if p.is_file()}
def measured(label,call):
    with b.Measure(OUT/'pg.log',OUT/(label+'.pg.log')) as measure:result=call()
    save(label+'.cost.json',measure.result);return result
@pytest.fixture(scope='module')
def lake():
    if (OUT/'binding.json').exists():
        c=json.loads((OUT/'binding.json').read_text());h=b.History(c['dsn'],ROOT,target_identity=b.PGIdentity(**c['identity']))
        return h,b.decode(c['token']),OUT,json.loads((OUT/'expected.json').read_text())
    base=f'host={ROOT/"socket"} port=28763';name='h5_8233_'+uuid.uuid4().hex[:10]
    with closing(b.private_pg(base+' dbname=postgres',ROOT)) as pg:
        pg.autocommit=True
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    dsn=base+' dbname='+name
    with closing(b.private_pg(dsn,ROOT)) as pg:identity=b.PGIdentity.read(pg)
    PgLogProbe(dsn,OUT/'pg.log');h=b.History(dsn,ROOT,target_identity=identity)
    binding,expected=measured('人工完整基础',lambda:b.fixture(OUT/'source'))
    save('expected.json',expected);save('人工原绑定.json',asdict(binding));save('原sourceSHA.json',hashes(OUT/'source'))
    manifest=measured('freeze',lambda:b.freeze_collection(binding,OUT/'freeze'))
    collection=measured('import',lambda:h.import_collection(manifest))
    token=measured('project',lambda:h.project_general(collection))
    save('binding.json',dict(dsn=dsn,root=str(ROOT),identity=asdict(identity),token=asdict(token)))
    for ident,label in [(collection.collection_id,'collection'),(token.profile_id,'h5')]:save(label+'-ready.json',json.loads((h.data_root/ident/'ready.json').read_text()))
    save('健康原载体SHA.json',{**hashes(h.data_root/collection.collection_id),**hashes(h.data_root/token.profile_id)})
    return h,token,OUT,expected

def test_01_full_original_graph_and_types(lake):
    measured('完整全值及原span对账',lambda:b.test_full_typed_and_exact_original_graph(lake))
    from data_pipeline.history.country_events.model import SCHEMAS
    h,t,_,_=lake
    with h.general(t) as s:
        for name in b.TABLES:assert s.db.execute('SELECT * FROM '+s.table(name)+' LIMIT 0').fetch_arrow_table().schema==SCHEMAS[name]
    ready=json.loads((h.data_root/t.profile_id/'ready.json').read_text())
    spool=ready['input_spool_resources'];assert spool['candidate_disk_bound_bytes']>=spool['spool_physical_bytes']>0
    save('类型与Spool上界.json',dict(all_13_schemas_equal=True,input_spool_resources=spool,domain_resources=ready['resources'],receipt=s.receipt))

def test_02_legacy_pages_scope_and_scan(lake):
    measured('旧合法全页17',lambda:b.test_legacy_all_pages_filters_and_hash_scope(lake,17))
    measured('实际单扫描计划',lambda:b.test_actual_query_plan_scan_observation(lake))
    measured('累计扫描上界拒绝',lambda:b.test_cumulative_scan_bound_is_enforced(lake))

@pytest.mark.parametrize('fault',['unknown_end','complete'])
def test_03_metadata_negative(lake,fault):
    measured(fault+'拒绝',lambda:b.test_finite_admission_negatives(lake,fault))

def test_04_identity_candidates(lake):
    measured('跨根完整候选',lambda:b.test_cross_root_complete_candidates_and_unknown(lake))
    measured('上游摘要冲突完整候选',lambda:b.test_root_upstream_conflicts_keep_all_candidates(lake))

@pytest.mark.parametrize('secondary',[False,True])
def test_05_actual_constructor_release(lake,monkeypatch,secondary):
    h,t,out,_=lake
    measured('构造关闭'+str(secondary),lambda:ctor.test_actual_create_rejection_and_primary_preserved((h,t,out,out),monkeypatch,secondary))

def test_06_same_profile_batch_cost(lake):
    h,t,out,_=lake;expected=json.loads((OUT/'ordered-tables.json').read_text());results={}
    for cap in (17,1000):
        def read():
            digests={}
            with h.general(t) as s:
                for name in b.TABLES:
                    values=b.table(s,name,cap);assert json.loads(b.row_bytes(values))==expected[name]
                    digests[name]=dict(rows=len(values),sha256=hashlib.sha256(b''.join(b.row_bytes(row)+b'\n' for row in values)).hexdigest())
            assert s.receipt;return dict(digests=digests,receipt=s.receipt)
        results[str(cap)]=measured('同基础批'+str(cap),read)
    assert results['17']['digests']==results['1000']['digests']
    costs=[json.loads((OUT/('同基础批'+str(cap)+'.cost.json')).read_text()) for cap in (17,1000)]
    assert len({c['pg_logged_statements'] for c in costs})==1
    assert len({(c['all_python_sha256_calls'],c['all_python_sha256_bytes']) for c in costs})==1
    save('代表批变化.json',results)

def test_07_removed_source_fresh_full_read(lake):
    h,t,out,_=lake;assert hashes(OUT/'source')==json.loads((OUT/'原sourceSHA.json').read_text())
    (OUT/'source').rename(OUT/'source-removed');(OUT/'freeze').rename(OUT/'freeze-removed')
    code='''import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from tests.historical.test_historical_general import History, PGIdentity, decode, TABLES, row_bytes, table, save
p=Path(sys.argv[2]);b=json.loads((p/'binding.json').read_text());h=History(b['dsn'],b['root'],target_identity=PGIdentity(**b['identity']));t=decode(b['token'])
assert not(p/'source').exists() and not(p/'freeze').exists()
expected=json.loads((p/'ordered-tables.json').read_text());raws=json.loads((p/'raw-documents.json').read_text())
business={'overview_metrics','track_definitions','series_points','affected_as','path_relations','path_samples','sample_peer_members'}
with h.general(t) as s:
 initial=s.budget.counts['hash_calls']
 for name in TABLES:
  assert json.loads(row_bytes(table(s,name)))==expected[name]
  for eid in sorted({r['event_id'] for r in expected[name]}) if name in business else [None]:
   got=[];cursor=None
   while True:
    result=s.query(name,event_id=eid,limit=60,cursor=cursor);got+=result['rows'];cursor=result['next_cursor']
    if cursor is None:break
   assert json.loads(row_bytes(got))==[r for r in expected[name] if eid is None or r['event_id']==eid]
 assert s.budget.counts['hash_calls']==initial
 for doc in expected['documents']:
  cursor=0;got=bytearray()
  while True:
   result=s.document(doc['document_id'],offset=cursor);got.extend(result['raw']);cursor=result['next_offset']
   if cursor is None:break
  assert got.hex()==raws[str(doc['document_id'])]
assert s.receipt
save(p/'新进程结果.json',dict(all_13_tables_all_pages_all_fields_equal=True,all_documents_and_references_equal=True,receipt=s.receipt))
'''
    script=OUT/'新进程.py';script.write_text(code)
    def run():
        proc=subprocess.run([sys.executable,'-I','-B',str(script),str(b.REPO/'backend'),str(OUT)],capture_output=True,text=True)
        (OUT/'新进程.log').write_text(proc.stdout+proc.stderr);assert proc.returncode==0,proc.stderr
    measured('新进程全表全页原文档引用',run)
    assert {**hashes(h.data_root/t.collection.collection_id),**hashes(h.data_root/t.profile_id)}==json.loads((OUT/'健康原载体SHA.json').read_text())

def test_08_original_c1_h1_h2(lake,monkeypatch):
    monkeypatch.setenv('DOMEYE_H2_INTEGRATION','8233')
    from tests.historical import test_h2_integration as old
    monkeypatch.setattr(old,'OUT',OUT)
    old.test_06_original_h1_and_c1_tokens(lake)
    root=Path('/tmp/domeye-h2-integration-8233');conf=json.loads((root/'binding.json').read_text());t=old.b.decode_token(conf['token']);h=old.b.History(conf['dsn'],ROOT,target_identity=b.PGIdentity(**conf['identity']))
    expected=json.loads((root/'ordered-tables.json').read_text());docs=json.loads((root/'document-proof.json').read_text());refs=json.loads((root/'reference-proof.json').read_text())
    before=b.sha(h.data_root/t.profile_id/'ready.json')
    def read():
        with h.rib(t) as s:
            for name in old.b.TABLES:assert json.loads(b.row_bytes(old.b.table(s,name)))==expected[name]
            for doc in expected['documents']:assert json.loads(b.row_bytes(s.detail(doc['document_id'])))==docs[str(doc['document_id'])]
            for ref in expected['comparison_refs']:
                key=':'.join(str(ref[k]) for k in ('document_id','object_ordinal','side','ref_ordinal'))
                assert json.loads(b.row_bytes(s.reference(ref['document_id'],ref['object_ordinal'],ref['side'],ref['ref_ordinal'])))==refs[key]
        assert s.receipt;return s.receipt
    receipt=measured('原H2全15表文档引用',read);assert b.sha(h.data_root/t.profile_id/'ready.json')==before
    save('原H2兼容.json',dict(original_token=asdict(t),ready_sha256=before,receipt=receipt,reimported=False,reprojected=False))

def test_09_exact_numeric_structure_offline():
    # 标准库Decimal词法元组独立核全部数值结构，不调用产品数字转换器。
    from decimal import Decimal
    values=json.loads((OUT/'ordered-tables.json').read_text());count=negative=huge=0
    for row in values['scalar_fields']+values['series_points']:
        if row['kind']!='number':continue
        number=Decimal(row['number_lexeme']);parts=number.as_tuple();digits=''.join(map(str,parts.digits)).lstrip('0') or '0'
        assert (row['sign'],row['coefficient_digits'],row['exponent10'],bool(row['negative_zero']))==(-1 if parts.sign else 1,digits,parts.exponent,bool(parts.sign and number.is_zero()))
        count+=1;negative+=bool(row['negative_zero']);huge+=len(digits)>38
    assert negative and huge
    save('全部精确数值结构.json',dict(numeric_rows=count,negative_zero_rows=negative,over_decimal128_precision_rows=huge,oracle='标准库Decimal原词法tuple；未调用产品数字转换器'))
