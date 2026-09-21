"""有限Reference协议/原pandas/纯接线反例；不伪造成功AD或启动PG。"""
import csv
import hashlib
import json
import sqlite3
from dataclasses import replace

import pyarrow as pa
import pytest

from data_pipeline.analysis.country_trends import feature_identity as identity, feature_source as s3_feature, identity_index as index
from data_pipeline.analysis.country_trends.snapshot_store import SharedBudget, S2Limits
from data_pipeline.bgp.input.reference_reader import rows
from data_pipeline.bgp.archive.value_codec import typed, untyped, digest, CODEC
from data_pipeline.bgp.archive.checkpoint import sha
from data_pipeline.analysis.features.reference import load_reference, USECOLS
from data_pipeline.analysis.country_events import event_aggregation as c2, qualified_schema as m3_schema, snapshot_schema as original
from data_pipeline.analysis.country_events.models import Incident, Time


def budget(tmp_path, **limits):
    return SharedBudget(replace(S2Limits(), **limits), tmp_path, lambda: None)


def interpreted(name, code, selected=True):
    return dict(legacy_country=json.dumps(name), legacy_country_code=json.dumps(code), selected=selected)


@pytest.mark.parametrize('pairs,reasons', [
    ([('甲','AA'),('甲','AA')], [None,None]),
    ([('甲','AA'),('甲','BB')], ['ambiguous_name','ambiguous_name']),
    ([('甲','AA'),('乙','AA')], ['ambiguous_code','ambiguous_code']),
    ([('Unknown','AA'),('','BB'),(42,'CC'),('甲',12),('乙','zz')],
     ['unknown_name','unknown_name','unknown_name','unknown_code','unknown_code']),
    ([('甲','AA'),('甲','')], ['unknown_counterpart','unknown_code']),
])
def test_bidirectional_unique_or_explicit_insufficient(tmp_path,pairs, reasons):
    db=sqlite3.connect(':memory:');identity.initialize(db);b=budget(tmp_path)
    for i,pair in enumerate(pairs):index.save_interpretation(db,i,interpreted(*pair),b)
    index.build_index(db,b)
    result=[r for _,r in index.interpretations(db,b)];db.close()
    assert [r['reason'] for r in result]==reasons
    assert [r['state'] for r in result]==['usable' if r is None else 'insufficient' for r in reasons]


def make_reference(tmp_path):
    path=tmp_path/'reference.csv'
    with path.open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=USECOLS);writer.writeheader()
        for asn,name,code in [('001','测试国','ZZ'),('2','测试国','ZZ'),('001','冲突国','XX')]:
            writer.writerow(dict(asn=asn,as_country_cn=name,as_country=code,descr='逗号,和\n换行'))
    source=hashlib.sha256(path.read_bytes()).hexdigest()
    public=list(rows(path,source))
    class Reader:
        def reference_batches(self, requested):
            assert requested==source
            yield pa.RecordBatch.from_pylist(public)
    reference=load_reference(Reader(),source,path)
    return path,source,public,reference.version


def retained(tmp_path):
    path,source,public,version=make_reference(tmp_path)
    db=sqlite3.connect(':memory:');identity.initialize(db)
    b=budget(tmp_path)
    request=dict(view='references',scope_typed=typed(dict(source_ids=[source])),codec_version=CODEC,
                 batch_rows=256,batch_bytes=4*1024**2)
    payload=typed(public)
    batch=dict(rows_typed=payload,codec_version=CODEC,rows=len(public),bytes=len(payload.encode()))
    hasher=hashlib.sha256()
    count=identity.retain_batch(db,batch,request,source,0,hasher,b)
    return db,b,path,source,version,request,batch,hasher,count


def test_original_pandas_keep_first_and_provenance(tmp_path):
    db,b,path,source,version,*_=retained(tmp_path)
    with db:
        identity.preflight_cost(path,4,b)
        assert identity.interpret(db,path,source,version,b)==version
        values=[r for _,r in index.interpretations(db,b)]
        assert [v['original']['selected'] for v in values]==[None,True,True,False]
        assert [v['reason'] for v in values]==['not_selected',None,None,'not_selected']
        assert values[1]['original']['legacy_asn']=='1'
        assert values[1]['original']['csv_physical_end']>values[1]['original']['csv_physical_start']
        db.execute('CREATE TABLE feature_input(view TEXT,ordinal INTEGER,payload TEXT)')
        raw=dict(scope='country',country='测试国')
        db.execute('INSERT INTO feature_input VALUES (?,?,?)',('windows',2,typed(dict(raw=raw))))
        source_ref,mapping=identity.window_identity(db,{'admission_id':'fixture-unverified'},2,dict(raw=raw),b)
        assert mapping['code']=='ZZ'
        identity._save(db,'feature_reference_meta',0,dict(fixture='unverified-protocol'),b)
        evidence=dict(identity.sources(db,{'admission_id':'fixture-unverified'},b))
        assert evidence[mapping['binding_source_ref']]==dict(fixture='unverified-protocol')
        assert mapping['window_source_ref']=='feature:fixture-unverified:windows:2'
        assert evidence[source_ref]==mapping
        from itertools import islice
        from data_pipeline.analysis.country_trends.compile_country_results import compile_country
        from data_pipeline.analysis.country_trends import stream_schema as s3_schema
        independent=('fixture-independent','reference',s3_schema.encode(dict(fixture='comparison')))
        compiled=list(islice(compile_country(db,'fixture-output',(0,1),b,
            context_sources=(independent,),feature_admission={'admission_id':'fixture-unverified'}),len(evidence)+2))
        actual={r.get('source_ref'):s3_schema.decode(r.get('raw_typed')) for r in compiled}
        assert actual['fixture-independent']==dict(fixture='comparison')
        assert actual[mapping['window_source_ref']]==dict(raw=raw)
        assert all(actual[key]==value for key,value in evidence.items())
        group=evidence[mapping['identity_group_ref']]
        assert group['member_count']==2
        members=[];page=group['first_page_ref'];seen=set()
        while page is not None:
            assert page not in seen;seen.add(page)
            members.extend(evidence[page]['member_source_refs']);page=evidence[page]['next_page_ref']
        assert members==['feature:fixture-unverified:identity:1','feature:fixture-unverified:identity:2']
        for ref in members:
            original_row=evidence[evidence[ref]['reference_source_ref']]
            assert original_row['source_id']==source
            assert evidence[ref]['reference_source_ref'].endswith(':'+str(original_row['row']))
            assert all(evidence[ref]['original'][k]==v for k,v in original_row.items())
            assert evidence[ref]['binding_source_ref']==mapping['binding_source_ref']
    db.close()


@pytest.mark.parametrize('fault',['version','raw','row_hash'])
def test_original_interpretation_rejects_drift(tmp_path,fault):
    db,b,path,source,version,*_=retained(tmp_path)
    if fault=='version':version='0'*64
    elif fault=='raw':path.write_bytes(path.read_bytes()+b'\n')
    else:
        value=untyped(db.execute('SELECT payload FROM feature_reference_input WHERE ordinal=1').fetchone()[0])
        value['raw_record_sha256']='0'*64
        db.execute('UPDATE feature_reference_input SET payload=? WHERE ordinal=1',(typed(value),))
    with pytest.raises(ValueError):identity.interpret(db,path,source,version,b)
    db.close()


@pytest.mark.parametrize('fault',['missing','partial','digest','source','checkpoint','request'])
def test_tail_receipt_and_complete_count_reject_drift(tmp_path,fault):
    db,b,path,source,version,request,batch,h,count=retained(tmp_path)
    # 只构造纯协议值，没有AD/current成功桩；owner读取资格仍须实际PG验证。
    cp=dict(source_id=source,ordinal=0,counts=dict(references=count),raw='sealed',parse='complete',ingest='complete')
    coverage=dict(admission_id='fixture-protocol',source_ids=[source],view='references',source_checkpoints=[cp],
                  observation_qualification='observation_sealed',business='not_run',business_absence='Unknown')
    receipt=dict(contract='component-publication-read/v1',admission_id='fixture-protocol',request_digest=digest(request),
                 rows=count,typed_digest=h.hexdigest(),execution='complete',coverage_ref=typed(coverage))
    identity.check_receipt(receipt,request,'fixture-protocol',cp,count,h)
    if fault=='missing':receipt=None
    elif fault=='partial':count-=1
    elif fault=='digest':receipt['typed_digest']='0'*64
    elif fault=='source':coverage['source_ids']=['different'];receipt['coverage_ref']=typed(coverage)
    elif fault=='checkpoint':cp=dict(cp,ordinal=1)
    else:request=dict(request,batch_rows=1)
    with pytest.raises((ValueError,TypeError)):identity.check_receipt(receipt,request,'fixture-protocol',cp,count,h)
    db.close()


@pytest.mark.parametrize('fault',['source','order','bytes','count'])
def test_public_batch_rejects_partial_or_changed_rows(tmp_path,fault):
    db,b,path,source,version,request,batch,h,count=retained(tmp_path)
    db.execute('DELETE FROM feature_reference_input')
    values=untyped(batch['rows_typed'])
    if fault=='source':values[0]['source_id']='other'
    elif fault=='order':values[0]['row']=1
    elif fault=='bytes':batch['bytes']+=1
    else:batch['rows']+=1
    if fault in ('source','order'):
        batch['rows_typed']=typed(values);batch['bytes']=len(batch['rows_typed'].encode())
    with pytest.raises(ValueError):identity.retain_batch(db,batch,request,source,0,hashlib.sha256(),b)
    db.close()


@pytest.mark.parametrize('limits', [dict(max_total_bytes=1),dict(max_references=1),dict(max_rss_bytes=1)])
def test_whole_frame_preflight_respects_existing_limits(tmp_path,limits):
    path,*_=make_reference(tmp_path)
    with pytest.raises(ValueError):identity.preflight_cost(path,4,budget(tmp_path,**limits))


def test_activity_preserves_values_unknown_roles_and_source_identity(tmp_path):
    db,b,path,source,version,*_=retained(tmp_path)
    identity.interpret(db,path,source,version,b)
    db.execute('CREATE TABLE feature_input(view TEXT,ordinal INTEGER,payload TEXT)')
    db.execute('CREATE TABLE country_events(incident TEXT,revision INTEGER,payload TEXT)')
    status=c2.EventStatus(Incident('fixture-event',2,'ZZ',Time(0)),(),None,None,'unavailable',())
    table,value=m3_schema.row_encode(0,c2.C2Row('fixture-event',2,status))
    db.execute('INSERT INTO country_events VALUES (?,?,?)',('fixture-event',2,original.encode(dict(raw=dict(table=table,row=value)))))
    public=dict(raw=dict(scope='country',country='测试国',mode='ordinary',start='1970-01-01T00:05:00+00:00',
        end='1970-01-01T00:10:00+00:00',source_rank=7,source_id='fixture-source',window_role='P'),
        values=dict(announ_num=None,withdraw_num=0),qualifications=dict(coverage='Unknown'))
    before=typed(public)
    db.execute('INSERT INTO feature_input VALUES (?,?,?)',('windows',2,before))
    windows=s3_feature.feature_context(db,{'admission_id':'fixture-unverified'},((('fixture-event',2),'ordinary'),),b)
    assert len(windows)==2
    assert [(w.value,w.state) for w in windows]==[(None,'unknown'),(0,'complete')]
    assert all(w.start_us==300_000_000 and w.end_us==600_000_000 for w in windows)
    assert all(w.source_rank==7 and w.source_id=='fixture-source' and w.window_role=='P' for w in windows)
    assert db.execute('SELECT payload FROM feature_input').fetchone()[0]==before
    assert all(w.source_ref.endswith(':windows:2:identity') for w in windows)
    second=c2.EventStatus(Incident('fixture-second',2,'ZZ',Time(0)),(),None,None,'unavailable',())
    table,value=m3_schema.row_encode(1,c2.C2Row('fixture-second',2,second))
    db.execute('INSERT INTO country_events VALUES (?,?,?)',('fixture-second',2,original.encode(dict(raw=dict(table=table,row=value)))))
    order=(('fixture-second',2),('fixture-event',2))
    combined=s3_feature.feature_context(db,{'admission_id':'fixture-unverified'},tuple((e,'ordinary') for e in order),b)
    assert combined==tuple(replace(w,event=e) for e in order for w in windows)

    limited=budget(tmp_path,max_index_steps=1)
    identity.window_identity(db,{'admission_id':'fixture-unverified'},2,public,limited)
    with pytest.raises(ValueError,match='index_budget'):
        identity.window_identity(db,{'admission_id':'fixture-unverified'},2,public,limited)
    db.close()


@pytest.mark.parametrize('table', [name for name,_ in identity.TABLES])
def test_fresh_source_audit_rejects_any_retained_identity_drift(tmp_path,table):
    from data_pipeline.analysis.country_trends.stream_audit import compare_sources
    saved=sqlite3.connect(':memory:');fresh=sqlite3.connect(':memory:')
    for db in (saved,fresh):
        identity.initialize(db)
        db.executescript("CREATE TABLE country_input(sequence INTEGER);CREATE TABLE country_events(sequence INTEGER);"
                         "CREATE TABLE country_reads(view TEXT,table_name TEXT);"
                         "CREATE TABLE feature_input(view TEXT,ordinal INTEGER);CREATE TABLE feature_reads(view TEXT);")
        for name in ('feature_reference_input','feature_reference_meta'):
            db.execute('INSERT INTO '+name+' VALUES (0,?)',(typed(dict(fixture='original')),))
        index.save_interpretation(db,0,interpreted('测试国','ZZ'),budget(tmp_path))
        index.build_index(db,budget(tmp_path))
    compare_sources(saved,fresh,budget(tmp_path),with_feature=True)
    column='member_digest' if table=='feature_identity_groups' else 'ordinals' if table=='feature_identity_pages' else 'payload'
    saved.execute('UPDATE '+table+' SET '+column+'=?',(typed(dict(fixture='tampered')),))
    with pytest.raises(ValueError,match='original_source_difference'):
        compare_sources(saved,fresh,budget(tmp_path),with_feature=True)
    saved.close();fresh.close()


def test_reference_total_is_not_query_context_and_rows_still_bounded(tmp_path):
    path,*_=make_reference(tmp_path)
    identity.preflight_cost(path,4,budget(tmp_path,max_context_bytes=1))
    db=sqlite3.connect(':memory:');identity.initialize(db)
    with pytest.raises(ValueError,match='context_budget'):
        index.save_interpretation(db,0,interpreted('测试国','ZZ'),budget(tmp_path,max_context_bytes=1))
    db.close()
