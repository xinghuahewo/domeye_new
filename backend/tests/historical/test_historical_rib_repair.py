"""H2两项P2有限定向复验；只用自己的原Collection与人工闭包。"""
from contextlib import closing
from dataclasses import asdict,replace
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import uuid
import pytest

from tests.historical import test_historical_rib as base
from data_pipeline.history.rib_index import History, PGIdentity, freeze_collection
from data_pipeline.history.rib_index.freeze import Freezer
from data_pipeline.history.database_import.freeze import Limits
from data_pipeline.history.event_collection.model import Binding, Root, CollectionLimits
from data_pipeline.overview.paths import read_comparison


@pytest.fixture(scope='module')
def context():
    root=Path(os.environ['Q3_PRIVATE_ROOT']) if os.environ.get('Q3_PRIVATE_ROOT') else None
    if root is None:pytest.skip('须自己的已绑定私有PG')
    repair=root/'q3-c3-repair';out=repair/('acceptance-'+uuid.uuid4().hex);out.mkdir()
    bad=json.loads((repair/'before-duplicate/binding.json').read_text())
    h=History(bad['dsn'],root,target_identity=PGIdentity(**bad['identity']))
    print('Q3C3_REPAIR_EVIDENCE='+str(out));return root,out,h,bad


@pytest.mark.parametrize('second',[30,61])
def test_real_child_remaining_and_total(context,monkeypatch,second):
    _,out,_,_=context;case=out/('child-'+str(second));case.mkdir();src=case/'source';src.mkdir();dummy=src/'root.json';dummy.write_text('{}')
    b=Binding((Root('core-rib-consumption/v1',str(dummy),dummy.as_uri(),'fixture',base.sha(dummy)),),(str(src),))
    f=Freezer(b,case/'freeze',replace(Limits(),max_total_rows=100,batch_rows=100),CollectionLimits())
    from data_pipeline.history.rib_index import freeze as module
    original_preflight=module.sqlite_preflight;original_freeze=module.freeze_sqlite_source;preflight=[];execute=[]
    def check(path,limits):preflight.append(limits.max_total_rows);return original_preflight(path,limits)
    def freeze(*a,**kw):execute.append(kw['limits'].max_total_rows);return original_freeze(*a,**kw)
    monkeypatch.setattr(module,'sqlite_preflight',check);monkeypatch.setattr(module,'freeze_sqlite_source',freeze)
    failure=None
    try:
        for i,n in enumerate((40,second)):
            p=src/f'{i}.sqlite'
            with sqlite3.connect(p) as db:db.execute('CREATE TABLE t(x INTEGER)');db.executemany('INSERT INTO t VALUES (?)',[(j,) for j in range(n)])
            copied=f.out/p.name;shutil.copyfile(p,copied);f.files.append({'version':'fixture','raw_sha':base.sha(p),'raw_path':copied.name});f.sources.append({'path':str(p)})
            try:f.sqlite(copied,i)
            except ValueError as error:failure=str(error);break
        assert preflight==[100,60]
        if second==30:assert execute==[100,60] and f.budget.counts['child_rows']==70 and failure is None
        else:assert execute==[100] and f.budget.counts['child_rows']==40 and '剩余额度' in failure
        base.save(case/'result.json',{'total_limit':100,'preflight_limits':preflight,'execution_limits':execute,'cumulative':f.budget.counts['child_rows'],'failure':failure})
    finally:f.spool.close()


def rejected_projection(h,collection,out):
    before=set(h.data_root.iterdir())
    with pytest.raises(ValueError) as error:h.project_rib(collection)
    created=set(h.data_root.iterdir())-before
    assert len(created)==1
    candidate=created.pop();assert (candidate/'FAILED.json').exists() and not (candidate/'ready.json').exists()
    base.save(out,{'error':str(error.value),'candidate':str(candidate),'ready_exists':False,'token_returned':False})
    return str(error.value)


def test_same_original_bad_collection_rejected_without_resealing(context):
    root,out,h,bad=context;token=base.decode_token(bad['token']);paths=[h.data_root/token.profile_id/'ready.json',h.data_root/token.collection.collection_id/'ready.json']
    before=[base.sha(p) for p in paths]
    with pytest.raises(ValueError,match='身份变化'):
        with h.rib(token):pass
    assert '路径对照消费包校验失败' in rejected_projection(h,token.collection,out/'duplicate-after.json')
    assert [base.sha(p) for p in paths]==before
    base.save(out/'old-bad-preserved.json',{'old_token':bad['token'],'old_reader_rejection':bad['legacy_rejection'],'old_H2_complete':bad['receipt'],'unchanged_ready':before})


def test_healthy_original_collection_reprojection(context):
    root,out,_,_=context
    old=root/'q3-c3/acceptance-8bb569e285ba4d32a7482036627f653f';binding=json.loads((old/'binding.json').read_text());token=base.decode_token(binding['token']);h=History(binding['dsn'],root,target_identity=PGIdentity(**binding['identity']))
    old_ready=h.data_root/token.profile_id/'ready.json';before=base.sha(old_ready)
    current=h.project_rib(token.collection)
    assert current.collection==token.collection and current.rule_sha256!=token.rule_sha256 and current.profile_id!=token.profile_id
    expected=json.loads((old/'ordered-tables.json').read_text());refs=json.loads((old/'reference-proof.json').read_text())
    with h.rib(current) as s:
        for name in base.TABLES:assert json.loads(base.row_bytes(base.table(s,name)))==expected[name]
        for ref in expected['comparison_refs']:
            key=':'.join(str(ref[k]) for k in ('document_id','object_ordinal','side','ref_ordinal'))
            value=s.reference(ref['document_id'],ref['object_ordinal'],ref['side'],ref['ref_ordinal']);assert json.loads(base.row_bytes(value))==refs[key]
    assert s.receipt and base.sha(old_ready)==before
    base.save(out/'healthy-reprojection.json',{'old_token':asdict(token),'new_token':asdict(current),'old_ready_sha256':before,'same_collection':True,'MRT_resealed':False,'receipt':s.receipt})


def oldread(src):
    summary=json.loads((src/'paths/summary.json').read_text())
    return read_comparison(src/'index.json',{'path_comparison':{'file':'paths/manifest.json','sha256':base.sha(src/'paths/manifest.json')},'data_profile':summary['data_profile'],'source':{'collector_id':'rrc25'}})


def binding_module():
    spec=importlib.util.spec_from_file_location('contract_fixture',base.REPO/'scripts/core_overview/bind-core-overview-paths.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def extra_sample(src,predicate):
    import gzip
    module=binding_module();summary=json.loads((src/'comparison/summary.json').read_text());groups=json.loads((src/'comparison/peer-groups.json').read_text())
    with closing((src/'comparison/left.mrt').open('rb')) as left,closing((src/'comparison/right.mrt').open('rb')) as right:
        for raw in gzip.decompress((src/'comparison/comparisons.jsonl.gz').read_bytes()).splitlines():
            row=json.loads(raw)
            for item in row['objects']:
                if predicate(row,item):return module.sample(row,item,groups,[left,right],summary['inputs'])
    raise AssertionError('fixture缺需要的样例')


@pytest.mark.parametrize('fault',['duplicate','total_cap','family_cap','ref_keys','path_length','path_type','ref_type'])
def test_native_reader_contract_branches(context,fault):
    _,out,h,_=context;case=out/('contract-'+fault);case.mkdir();src=case/'source';binding,_=base.fixture(src,n=27 if fault=='family_cap' else 17)
    oldread(src);p=src/'paths/summary.json';value=json.loads(p.read_text())
    if fault=='duplicate':value['examples'].append(deepcopy(value['examples'][0]))
    elif fault=='total_cap':
        while len(value['examples'])<11:value['examples'].append(deepcopy(value['examples'][0]))
    elif fault=='family_cap':
        present={v['prefix'] for v in value['examples']}
        example=extra_sample(src,lambda row,item:row['afi']==1 and item[1]=='different' and row['prefix'] not in present)
        value['examples']=[v for v in value['examples'] if v is not next(v for v in value['examples'] if v['family']=='ipv6')]+[example]
        assert len(value['examples'])==10 and len({(v['family'],v['prefix']) for v in value['examples']})==10
    elif fault=='ref_keys':value['examples'][0]['left_reference']['extra']=0
    elif fault=='path_length':value['examples'][0]['left_path']=[64496]*257
    elif fault=='path_type':value['examples'][0]['left_path'][0]=True
    else:value['examples'][0]['left_reference']['record']=True
    p.chmod(0o600);base.plain_save(p,value);binding=base.refresh(src,binding)
    with pytest.raises(ValueError):oldread(src)
    collection=h.import_collection(freeze_collection(binding,case/'freeze'))
    assert '路径对照消费包校验失败' in rejected_projection(h,collection,case/'rejection.json')


def replace_comparison(src,case,expected,left_epoch,right_rows):
    from data_pipeline.bgp.snapshots import path_comparison as rib_path_comparison
    (src/'comparison').rename(case/'comparison-before')
    staging=case/'mrt-input';staging.mkdir()
    left=base.source(staging,'l.gz',left_epoch,expected['peers'],expected['left_rows'])
    right=base.source(staging,'r.gz',base.RIGHT,expected['peers'],right_rows)
    rib_path_comparison.compare_ribs(left,right,base.sha(left),base.sha(right),src/'comparison',base.PROFILE)
    (src/'comparison/manifest.pending').unlink()
    manifest=json.loads((src/'comparison/manifest.json').read_text());summary=binding_module().retained_summary(src/'comparison',manifest,base.sha(src/'comparison/manifest.json'))
    p=src/'paths/summary.json';p.chmod(0o600);base.plain_save(p,summary)


def test_native_same_business_day_gate(context):
    _,out,h,_=context;case=out/'contract-business-day';case.mkdir();src=case/'source';binding,expected=base.fixture(src)
    replace_comparison(src,case,expected,base.LEFT-86400,expected['right_rows']);binding=base.refresh(src,binding)
    with pytest.raises(ValueError):oldread(src)
    collection=h.import_collection(freeze_collection(binding,case/'freeze'))
    assert '路径对照消费包校验失败' in rejected_projection(h,collection,case/'rejection.json')


def test_sample_requires_verified_different_object(context):
    _,out,h,_=context;case=out/'contract-not-comparable';case.mkdir();src=case/'source';binding,expected=base.fixture(src)
    rows=deepcopy(expected['right_rows']);q=base.path((2,[64496,64498]))
    rows=[(prefix,[(peer,q,as4) for peer,_,as4 in entries],addpath) if prefix=='198.51.100.0/24' else (prefix,entries,addpath) for prefix,entries,addpath in rows]
    replace_comparison(src,case,expected,base.LEFT,rows);binding=base.refresh(src,binding)
    # 旧Reader只核本地样例字段；构造真实但属于歧义Peer组的局部不同路径。
    summary=json.loads((src/'paths/summary.json').read_text());example=deepcopy(next(v for v in summary['examples'] if v['family']=='ipv4'))
    source=json.loads((src/'comparison/summary.json').read_text())
    import gzip
    row=next(json.loads(raw) for raw in gzip.decompress((src/'comparison/comparisons.jsonl.gz').read_bytes()).splitlines() if json.loads(raw)['prefix']=='198.51.100.0/24')
    item=row['objects'][0];assert item[1]=='not_comparable';example.update(prefix=row['prefix'],peer={'bgp_id':base.PEERS[1][0],'ip':base.PEERS[1][1],'asn':base.PEERS[1][2]},left_path=[64496,64497],right_path=[64496,64498])
    for side,label in enumerate(('left','right')):
        position,entry=item[2+side][0];record,offset,_=row[label+'_frames'][position]
        example[label+'_reference']={'record':record,'offset':offset,'entry':entry,'peer_index':1}
    i=next(i for i,v in enumerate(summary['examples']) if v['family']=='ipv4');summary['examples'][i]=example
    p=src/'paths/summary.json';p.chmod(0o600);base.plain_save(p,summary);binding=base.refresh(src,binding)
    oldread(src)
    collection=h.import_collection(freeze_collection(binding,case/'freeze'))
    assert 'different对象' in rejected_projection(h,collection,case/'rejection.json')


def test_new_closure_removed_source_and_original_h1_compatibility(context):
    _,out,h,_=context;case=out/'new-healthy';case.mkdir();binding,expected=base.fixture(case/'source')
    collection=h.import_collection(freeze_collection(binding,case/'freeze'));token=h.project_rib(collection)
    base.save(case/'binding.json',{'dsn':h.dsn,'root':str(h.root),'identity':asdict(h.target_identity),'token':asdict(token)})
    lake=(h,token,case,expected)
    base.test_all_native_tables_and_original_documents(lake)
    base.test_all_mrt_paths_peers_and_original_sqlite_columns(lake)
    base.test_comparison_occurrences_and_origin_members_from_original_text(lake)
    base.test_source_removed_new_process_full_read(lake)
    base.test_old_collection_and_h1_original_tokens_unchanged(lake)
