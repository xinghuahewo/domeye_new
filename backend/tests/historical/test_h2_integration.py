"""8233 自己的单次两原生 profile 闭包、有限修复与原 Token 兼容。"""
import os
import json
from pathlib import Path
import hashlib
from dataclasses import asdict,replace
from contextlib import closing
import shutil
import subprocess
import sys
import uuid
import pytest
if os.environ.get('DOMEYE_H2_INTEGRATION')!='8233':pytest.skip('仅显式本任务人工集成',allow_module_level=True)
from tests.historical import test_historical_rib as b
from tests.historical import test_historical_rib_repair as repair
from tests.country.country_query_test_support import PgLogProbe
from data_pipeline.history.rib_index.model import SCHEMAS

OUT=Path('/tmp/domeye-h2-integration-8233').resolve()
ROOT=Path('/tmp/domeye-integration-q3-8233').resolve()

def save(name,value):b.save(OUT/name,value)
def hashes(root):return {str(p):b.sha(p) for p in root.rglob('*') if p.is_file()}

def measured(label,action):
    with b.Measure(OUT/'pg.log',OUT/(label+'.pg.log')) as m:result=action()
    save(label+'.cost.json',m.result)
    return result

@pytest.fixture(scope='module')
def lake():
    if (OUT/'binding.json').exists():
        conf=json.loads((OUT/'binding.json').read_text());h=b.History(conf['dsn'],ROOT,target_identity=b.PGIdentity(**conf['identity']))
        return h,b.decode_token(conf['token']),OUT,{}
    name='h2_8233_'+uuid.uuid4().hex[:10];base=f'host={ROOT/"socket"} port=28763'
    with closing(b.private_pg(base+' dbname=postgres',ROOT)) as pg:
        pg.autocommit=True
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    dsn=base+' dbname='+name
    with closing(b.private_pg(dsn,ROOT)) as pg:identity=b.PGIdentity.read(pg)
    h=b.History(dsn,ROOT,target_identity=identity);PgLogProbe(dsn,OUT/'pg.log')
    binding,expected=measured('人工原生输入',lambda:b.fixture(OUT/'source'))
    save('原始绑定.json',asdict(binding));save('原sourceSHA.json',hashes(OUT/'source'))
    manifest=measured('freeze',lambda:b.freeze_collection(binding,OUT/'freeze'))
    save('freeze-resources.json',json.loads(manifest.read_text())['resources'])
    collection=measured('Collection',lambda:h.import_collection(manifest))
    token=measured('H2project',lambda:h.project_rib(collection))
    save('binding.json',dict(dsn=dsn,root=str(ROOT),identity=asdict(identity),token=asdict(token)))
    for ident,label in [(collection.collection_id,'collection'),(token.profile_id,'h2')]:
        save(label+'-ready.json',json.loads((h.data_root/ident/'ready.json').read_text()))
    save('健康原载体SHA.json',{**hashes(h.data_root/collection.collection_id),**hashes(h.data_root/token.profile_id)})
    return h,token,OUT,expected


def test_01_full_native_oracles(lake):
    def verify():
        b.test_all_native_tables_and_original_documents(lake)
        b.test_all_mrt_paths_peers_and_original_sqlite_columns(lake)
        b.test_comparison_occurrences_and_origin_members_from_original_text(lake)
    measured('完整原文档MRT及数组oracle',verify)
    h,t,_,_=lake
    # 公共表类型/列次序与实际 Arrow 对照，含合法空值和二进制。
    with h.rib(t) as s:
        for name in b.TABLES:
            assert s.db.execute('SELECT * FROM '+s.table(name)+' LIMIT 0').fetch_arrow_table().schema==SCHEMAS[name]
    save('15表原类型.json',dict(all_exact_types=True,receipt=s.receipt))


@pytest.mark.parametrize('second',[30,61])
def test_02_child_budget(lake,monkeypatch,second):
    repair.test_real_child_remaining_and_total((ROOT,OUT,None,None),monkeypatch,second)


def test_03_duplicate_native_package_without_new_mrt(lake):
    h,t,_,_=lake;case=OUT/'重复example';case.mkdir();src=case/'source'
    shutil.copytree(OUT/'source',src)
    # 仅副本的本地绑定地址随副本变化，原 source 不改；MRT 字节不变。
    for f in src.rglob('*.json'):
        raw=f.read_text();changed=raw.replace(str(OUT/'source'),str(src))
        if changed!=raw:f.chmod(0o600);f.write_text(changed)
    original=json.loads((OUT/'原始绑定.json').read_text())
    roots=tuple(b.Root(**{**r,'path':r['path'].replace(str(OUT/'source'),str(src)),'origin_uri':r['origin_uri'].replace(str(OUT/'source'),str(src))}) for r in original['roots'])
    binding=b.Binding(roots,(str(src),),())
    binding=b.refresh(src,binding);repair.oldread(src)
    summary=src/'paths/summary.json';value=json.loads(summary.read_text());value['examples'].append(dict(value['examples'][0]));summary.chmod(0o600);b.plain_save(summary,value)
    binding=b.refresh(src,binding)
    with pytest.raises(ValueError):repair.oldread(src)
    for name in ['single.gz','comparison/left.gz','comparison/right.gz','comparison/left.mrt','comparison/right.mrt']:
        assert b.sha(src/name)==b.sha(OUT/'source'/name)
    manifest=measured('重复包freeze',lambda:b.freeze_collection(binding,case/'freeze'))
    collection=measured('重复包Collection',lambda:h.import_collection(manifest))
    error=measured('重复包拒绝',lambda:repair.rejected_projection(h,collection,case/'rejection.json'))
    assert '路径对照消费包校验失败' in error
    save('重复包原件不变.json',dict(same_mrt_bytes=True,new_mrt_production=False,collection=asdict(collection),error=error))


def test_04_removed_source_fresh_full_read(lake):
    h,t,_,_=lake
    assert hashes(OUT/'source')==json.loads((OUT/'原sourceSHA.json').read_text())
    (OUT/'source').rename(OUT/'source-removed');(OUT/'freeze').rename(OUT/'freeze-removed')
    script=OUT/'新进程.py'
    script.write_text('''import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from tests.historical.test_historical_rib import History, PGIdentity, decode_token, TABLES, row_bytes, save
p=Path(sys.argv[2]);c=json.loads((p/'binding.json').read_text());h=History(c['dsn'],c['root'],target_identity=PGIdentity(**c['identity']));t=decode_token(c['token'])
assert not (p/'source').exists() and not (p/'freeze').exists()
expected=json.loads((p/'ordered-tables.json').read_text());docs=json.loads((p/'document-proof.json').read_text());refs=json.loads((p/'reference-proof.json').read_text());counts={}
with h.rib(t) as s:
 for name in TABLES:
  actual=[r for batch in s.bulk(name,batch_rows=1) for r in batch['rows']]
  assert json.loads(row_bytes(actual))==expected[name];counts[name]=len(actual)
 for d in expected['documents']:assert json.loads(row_bytes(s.detail(d['document_id'])))==docs[str(d['document_id'])]
 for r in expected['comparison_refs']:
  key=':'.join(str(r[k]) for k in ('document_id','object_ordinal','side','ref_ordinal'))
  assert json.loads(row_bytes(s.reference(r['document_id'],r['object_ordinal'],r['side'],r['ref_ordinal'])))==refs[key]
assert s.receipt and s.db is None
save(p/'新进程结果.json',dict(rows=counts,all_full_values_equal=True,receipt=s.receipt))
''')
    def run():
        proc=subprocess.run([sys.executable,'-I','-B',str(script),str(b.REPO/'backend'),str(OUT)],capture_output=True,text=True)
        (OUT/'新进程.log').write_text(proc.stdout+proc.stderr);assert proc.returncode==0,proc.stderr
    measured('新进程全15表48文档62引用',run)
    assert {**hashes(h.data_root/t.collection.collection_id),**hashes(h.data_root/t.profile_id)}==json.loads((OUT/'健康原载体SHA.json').read_text())


def test_05_page_budget_and_cleanup_boundary(lake,monkeypatch):
    h,t,_,_=lake;expected=json.loads((OUT/'ordered-tables.json').read_text())['comparison_objects']
    def healthy():
        actual=[];cursor=None
        with h.rib(t) as s:
            before=s.budget.counts['hash_calls']
            while True:
                page=s.query('comparison_objects',limit=17,cursor=cursor);actual+=page['rows'];cursor=page['next_cursor']
                if cursor is None:break
            assert actual==expected and s.budget.counts['hash_calls']==before
        assert s.receipt and s.db is None;save('完整页回执.json',s.receipt)
    measured('完整分页',healthy)
    # 一次组合失败：第一页后预算变更，实际关闭句柄再注入close次错。
    from tests.historical.test_historical_core_cleanup import ClosingDuck, closed
    events=[]
    with h.rib(t) as s:
        s.query('comparison_objects',limit=17)
        db=s.db=ClosingDuck(s.db,events)
        original=h.limits;h.limits=replace(original,batch_rows=original.batch_rows+1)
        try:
            with pytest.raises(ValueError) as caught:s.query('comparison_objects',limit=17)
            assert caught.value.cleanup_errors
        finally:h.limits=original
    closed(db);assert s.failed and s.receipt is None and s.db is None
    save('页预算清理失败.json',dict(error=str(caught.value),cleanup_errors=caught.value.cleanup_errors,events=events,receipt=s.receipt,failed=s.failed))


def test_06_original_h1_and_c1_tokens(lake):
    from tests.historical.test_historical_core import decode_token
    from data_pipeline.history.event_index import History as H1
    from data_pipeline.history.event_collection import CollectionToken
    from data_pipeline.history.database_import import Token
    from data_pipeline.history.event_collection.store import row_bytes
    old=ROOT/'h1-integration-8233/acceptance-5455ef6903dd43639f106787a2c8d127';conf=json.loads((old/'binding.json').read_text());token=decode_token(conf['core']);h=H1(conf['dsn'],ROOT,target_identity=b.PGIdentity(**conf['identity']))
    ready=h.data_root/token.profile_id/'ready.json';before=b.sha(ready);expected=json.loads((old/'ordered-domain.json').read_text())
    def h1():
        with h.core(token) as s:
            for name,rows in expected.items():assert json.loads(b.row_bytes(b.table(s,name)))==rows
        assert s.receipt;return s.receipt
    receipt=measured('原H1全5表',h1);assert b.sha(ready)==before
    oldc=ROOT/'q3-c1/integration-3cad2ad5257b40198ed8eebee7247551';conf=json.loads((oldc/'binding.json').read_text());c=conf['token'];ct=CollectionToken(**{**c,'children':tuple(Token(**r) for r in c['children'])});reader=H1(conf['dsn'],ROOT,target_identity=b.PGIdentity(**conf['identity']))
    readyc=reader.data_root/ct.collection_id/'ready.json';beforec=b.sha(readyc);actual={}
    def c1():
        with reader.collection(ct) as s:
            for info in s.ready['tables']:
                rows=b.table(s,info['name']);material=b''.join(row_bytes(r)+b'\n' for r in rows)
                assert len(rows)==info['rows'] and hashlib.sha256(material).hexdigest()==info['sha256']
                expected_rows=json.loads((oldc/'ordered-typed.json').read_text())[info['name']]
                # 历史全文证据使用 Decimal.__str__，独立有序摘要仍使用正式 row_bytes。
                assert json.loads(json.dumps(rows,default=str))==expected_rows
                actual[info['name']]=dict(rows=len(rows),sha256=hashlib.sha256(material).hexdigest(),original_all_values_equal=True)
        assert s.receipt;return s.receipt
    cr=measured('原C1全6表',c1);assert b.sha(readyc)==beforec
    save('原Token兼容.json',dict(h1=asdict(token),h1_receipt=receipt,c1=asdict(ct),c1_tables=actual,c1_receipt=cr,ready_sha=[before,beforec],reimported=False,reprojected=False))


def test_07_original_summary_and_group_values(lake):
    h,t,_,_=lake
    actual=json.loads((OUT/'ordered-tables.json').read_text())
    with h.collection(t.collection) as s:files=b.table(s,'files')
    source=h.data_root/t.collection.collection_id/'source';checked={}
    for f in files:
        role=f['role']
        if role in ('scale-summary','origin-summary','path-summary','comparison-summary'):
            value=json.loads((source/f['raw_path']).read_text())
            if role in ('scale-summary','origin-summary'):
                name='scale_family' if role=='scale-summary' else 'origin_family'
                for row in [r for r in actual[name] if r['file_id']==f['file_id']]:
                    expected=value['families'][row['family']]
                    assert row['observed_at']==value['observed_at']
                    assert all(v==expected.get(k) for k,v in row.items() if k not in ('file_id','family','observed_at'))
            else:
                for row in [r for r in actual['path_endpoint_metrics'] if r['file_id']==f['file_id']]:
                    family=row['family'];values=(value['totals'] if family=='all' else value['families']['1' if family=='ipv4' else '2']) if role=='comparison-summary' else value['families'][family]
                    assert all(row[k]==values[k] for k in b.rib_path_comparison.STATUSES)
                    den=values['same']+values['different']
                    assert row['comparable_pairs']==den and row['different_fraction']==(str(values['different']/den) if den else None)
                    assert row['session_continuity']=='unknown' and row['interval_change_count'] is None
            checked[str(f['file_id'])]=role
        if role=='peer-groups':
            groups=json.loads((source/f['raw_path']).read_text())
            assert [r for r in actual['peer_groups'] if r['file_id']==f['file_id']]==[dict(file_id=f['file_id'],group_id=g['id'],**{k:g[k] for k in ('bgp_id','ip','asn')}) for g in groups]
            expected=[dict(file_id=f['file_id'],group_id=g['id'],side=side,member_ordinal=i,peer_index=index) for g in groups for side,label in enumerate(('left','right')) for i,index in enumerate(g[label+'_indexes'])]
            assert [r for r in actual['peer_group_members'] if r['file_id']==f['file_id']]==expected
    save('原summary分组全字段.json',dict(checked=checked,all_values_equal=True,zero_denominator_new_case=False,scientific_rules_unchanged=True))
