"""共享身份页独立关系核对与预算反例；纯合成输入，无成功AD/current。"""
import copy
import hashlib
import json
import sqlite3
from dataclasses import replace

import pytest

from data_pipeline.analysis.country_trends import feature_identity as identity, identity_index as index
from data_pipeline.analysis.country_trends.snapshot_store import SharedBudget, S2Limits
from data_pipeline.bgp.archive.value_codec import typed, untyped


def build(tmp_path, rows, **limits):
    db=sqlite3.connect(':memory:');identity.initialize(db)
    b=SharedBudget(replace(S2Limits(),**limits),tmp_path,lambda:None)
    for i,r in enumerate(rows):
        r=dict(r,row=i,source_id='fixture-reference')
        identity._save(db,'feature_reference_input',i,r,b)
        index.save_interpretation(db,i,r,b)
    index.build_index(db,b)
    identity._save(db,'feature_reference_meta',0,dict(fixture='unverified'),b)
    db.execute('CREATE TABLE feature_input(view TEXT,ordinal INTEGER,payload TEXT)')
    return db,b


def item(name,code,selected=True):
    return dict(legacy_country=json.dumps(name),legacy_country_code=json.dumps(code),selected=selected)


def verify_members(evidence,originals):
    """从独立原行推成员全集，不依赖构造器摘要或fresh重算。"""
    groups=[(ref,value) for ref,value in evidence.items() if 'first_page_ref' in value]
    for ref,group in groups:
        field='legacy_country' if group['kind']=='name' else 'legacy_country_code'
        expected=[i for i,row in enumerate(originals) if row['selected'] is True
                  and type(json.loads(row[field])) is str and json.loads(row[field])==group['value']]
        page=group['first_page_ref'];visited=set();actual=[]
        while page is not None:
            assert page not in visited
            visited.add(page);value=evidence[page]
            assert value['group_source_ref']==ref
            assert len(value['member_source_refs'])<=3
            for source in value['member_source_refs']:
                original=evidence[source]['original'];ordinal=original['row']
                assert original['source_id']=='fixture-reference'
                assert all(original[k]==v for k,v in originals[ordinal].items())
                raw=evidence[evidence[source]['reference_source_ref']]
                assert raw==original
                assert evidence[source]['reference_source_ref'].endswith(':'+str(ordinal))
                assert evidence[evidence[source]['binding_source_ref']]==dict(fixture='unverified')
                actual.append(ordinal)
            page=value['next_page_ref']
        assert actual==expected
        assert len(actual)==group['member_count']
        h=hashlib.sha256()
        for i in actual:h.update(hashlib.sha256(typed(i).encode()).digest())
        assert h.hexdigest()==group['member_digest']
        if 'code_group_ref' in group:
            code=evidence[group['code_group_ref']]
            assert code['kind']=='code' and code['value']==group['first_counterpart']


@pytest.mark.parametrize('fault',[None,'missing','cycle','duplicate','wrong_ordinal','count','digest','binding'])
def test_all_members_and_links_are_independently_checked(tmp_path,fault):
    originals=[item('甲','AA') for _ in range(8)]+[item('乙','AA'),item('甲','AA',False),item(None,'AA')]
    db,b=build(tmp_path,originals,max_batch_rows=3)
    evidence=dict(identity.sources(db,{'admission_id':'fixture'},b))
    group=next(v for v in evidence.values() if v.get('kind')=='name' and v['value']=='甲')
    page=group['first_page_ref']
    if fault=='missing':del evidence[page]
    elif fault=='cycle':evidence[page]['next_page_ref']=page
    elif fault=='duplicate':evidence[page]['member_source_refs'][1]=evidence[page]['member_source_refs'][0]
    elif fault=='wrong_ordinal':evidence[evidence[page]['member_source_refs'][0]]['original']['row']=8
    elif fault=='count':group['member_count']+=1
    elif fault=='digest':group['member_digest']='0'*64
    elif fault=='binding':evidence['feature:fixture:identity-binding']=dict(fixture='wrong')
    if fault:
        with pytest.raises((AssertionError,KeyError)):verify_members(evidence,originals)
    else:verify_members(evidence,originals)
    db.close()


def test_lookup_cannot_read_original_tables_and_window_refs_do_not_grow(tmp_path):
    db,b=build(tmp_path,[item('甲','AA') for _ in range(1024)],max_batch_rows=3)
    before=db.total_changes
    def allow(action,table,*args):
        if action==sqlite3.SQLITE_READ and table in ('feature_reference_input','feature_reference_interpretation'):
            return sqlite3.SQLITE_DENY
        if action in (sqlite3.SQLITE_INSERT,sqlite3.SQLITE_UPDATE,sqlite3.SQLITE_DELETE):return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK
    db.set_authorizer(allow)
    for i in range(576):
        ref,value=identity.window_identity(db,{'admission_id':'fixture'},i,dict(raw=dict(scope='country',country='甲')),b)
        assert len(value)==5 and 'mapping_source_refs' not in value
        assert value['identity_group_ref'] and value['code']=='AA'
    assert db.total_changes==before
    assert b.stats['identity_source_decodes']==0
    db.close()


@pytest.mark.parametrize('limits,match',[(dict(max_batch_bytes=128),'page_budget'),
    (dict(max_context_bytes=512),'page_budget'),(dict(max_references=1),'reference_budget')])
def test_existing_page_and_reference_budgets_reject(tmp_path,limits,match):
    with pytest.raises(ValueError,match=match):build(tmp_path,[item('甲','AA'),item('甲','AA')],**limits)


def test_sql_work_is_interrupted_by_original_budget(tmp_path):
    db,b=build(tmp_path,[item('甲','AA') for _ in range(20)])
    limited=SharedBudget(replace(S2Limits(),max_index_steps=200),tmp_path,lambda:None)
    index.install_progress(db,limited)
    with pytest.raises(sqlite3.OperationalError,match='interrupted'):
        db.execute('SELECT a.ordinal,b.ordinal FROM feature_reference_interpretation a CROSS JOIN feature_reference_interpretation b').fetchall()
    assert limited.stats['sqlite_steps']>200
    db.close()


def test_group_and_row_reasons_preserve_unknown_before_ambiguity(tmp_path):
    rows=[item('甲','AA'),item('甲','BB'),item('甲',''),item('乙','BB'),item(None,'BB'),item('甲','AA',False)]
    db,b=build(tmp_path,rows)
    assert [r['reason'] for _,r in index.interpretations(db,b)]==[
        'unknown_counterpart','unknown_counterpart','unknown_code','unknown_counterpart','unknown_name','not_selected']
    assert index.lookup(db,'甲',b) is None and index.lookup(db,'乙',b) is None
    db.close()


def test_large_raw_size_keeps_conservative_pandas_rss_gate(tmp_path,monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(identity,'Path',lambda _:SimpleNamespace(stat=lambda:SimpleNamespace(st_size=375961154)))
    b=SharedBudget(S2Limits(),tmp_path,lambda:None)
    with pytest.raises(ValueError,match='frame_budget'):
        identity.preflight_cost('未读取的纯大小反例',1,b)
    assert b.byte_count==0


@pytest.mark.parametrize('pairs,usable',[
    ([('甲','AA'),('甲','BB')],False),
    ([('甲','AA'),('甲','')],False),
    ([('甲','AA'),('乙','AA')],False),
    ([('甲','AA'),('甲','AA')],True)])
def test_code_association_edge_requires_usable_unique_mapping(tmp_path,pairs,usable):
    db,b=build(tmp_path,[item(*pair) for pair in pairs])
    values=dict(identity.sources(db,{'admission_id':'fixture'},b))
    root=next(v for v in values.values() if v.get('kind')=='name' and v['value']=='甲')
    assert root['usable_name_mapping'] is usable
    assert ('code_group_ref' in root) is usable
    db.close()


@pytest.mark.parametrize('field',['max_row_bytes','max_context_bytes'])
def test_source_refs_added_after_payload_are_bounded(tmp_path,field):
    db,b=build(tmp_path,[item('甲','AA')])
    values=dict(identity.sources(db,{'admission_id':'fixture'},b))
    source='feature:fixture:identity:0';body=values[source]
    bare={k:v for k,v in body.items() if k not in ('reference_source_ref','binding_source_ref')}
    bare_size=len(typed(bare).encode());wrapped_size=len(typed((source,body)).encode())
    assert wrapped_size>bare_size
    limited=SharedBudget(replace(S2Limits(),**{field:(bare_size+wrapped_size)//2}),tmp_path,lambda:None)
    with pytest.raises(ValueError,match='source_budget'):
        list(identity.sources(db,{'admission_id':'fixture'},limited))
    db.close()


@pytest.mark.parametrize('field',['max_row_bytes','max_context_bytes','max_batch_bytes'])
def test_final_context_source_envelope_is_bounded(tmp_path,field):
    from data_pipeline.analysis.country_trends.compile_country_results import compile_country
    from data_pipeline.analysis.country_trends import stream_schema as s3_schema
    db,b=build(tmp_path,[item('甲','AA')])
    first=next(compile_country(db,'fixture-result',(0,1),b,feature_admission={'admission_id':'fixture'}))
    public_size=len(s3_schema.encode(s3_schema.flatten(0,first)).encode())
    source,value=next(identity.sources(db,{'admission_id':'fixture'},b))
    assert len(typed((source,value)).encode())<public_size
    limited=SharedBudget(replace(S2Limits(),**{field:public_size-1}),tmp_path,lambda:None)
    with pytest.raises(ValueError,match='public_source_budget'):
        next(compile_country(db,'fixture-result',(0,1),limited,feature_admission={'admission_id':'fixture'}))
    db.close()


def test_sql_meter_keeps_prior_count_and_pending_capture_rows(tmp_path):
    db=sqlite3.connect(':memory:');db.execute('CREATE TABLE pending(v)');db.execute('INSERT INTO pending VALUES (1)')
    b=SharedBudget(S2Limits(),tmp_path,lambda:None);b.stats['sqlite_steps']=7000
    index.install_progress(db,b)
    assert b.stats['sqlite_steps']>=7000 and db.in_transaction
    assert db.execute('SELECT v FROM pending').fetchone()==(1,)
    db.close()
