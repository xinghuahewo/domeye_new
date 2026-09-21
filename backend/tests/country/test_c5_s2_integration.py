"""仅已准备的8233严格人工载体；全字段新进程与三项P2代表。"""
from collections import Counter
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path
import json
import os
import resource
import sqlite3
import subprocess
import sys
import time
from types import SimpleNamespace
import pytest
import pyarrow.parquet as pq
from tests.country.c5_s2_integration_support import OUT, OLD, REPO, load, save, sha, runtime, source_rows, frozen
from data_pipeline.analysis.country_trends.snapshot_reader import TrendReader, TrendReadReceipt
from data_pipeline.analysis.country_trends.snapshot_store import TrendBinding, S2Limits
from data_pipeline.analysis.country_trends.snapshot_schema import TABLES, SCHEMAS, decode, encode, row_material, reference_count
from data_pipeline.analysis.country_trends.contract import TrendRow
from data_pipeline.analysis.country_trends import snapshot_reader as module, snapshot_audit as s2_audit
from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_schema as c3
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader


@pytest.fixture(scope='module', autouse=True)
def authorized():
    if os.environ.get('DOMEYE_C5_S2_INTEGRATION') != '8233':pytest.skip('仅显式本任务人工制品')
    assert OUT.is_dir()


def read_binding(label):
    q=load(OUT/(label+'-request.json'));p=load(q['receipt'])
    return q,TrendBinding(**p['binding']),p['proof'],runtime(q)


def physical_rows(binding):
    """独立拼回公共typed字段，不以Reader.restore生成期望。"""
    rows=[];counts={};groups={}
    assert len(TABLES)==44
    for kind in TABLES:
        path=Path(binding.root)/'data'/(kind+'.parquet');table=pq.read_table(path)
        assert table.column_names==[n for n,t in SCHEMAS[kind]]
        counts[kind]=table.num_rows;groups[kind]=pq.ParquetFile(path).metadata.num_row_groups
        for flat in table.to_pylist():
            event=() if flat['incident'] is None else (flat['incident'],int(flat['revision']))
            order=decode(flat['field_order'])
            values=tuple((key,decode(flat[key])) for key in order)
            rows.append((flat['sequence'],TrendRow(kind,event,decode(flat['key_typed']),values,decode(flat['refs_typed']),flat['result_id'])))
    rows.sort(key=lambda v:v[0]);assert [i for i,r in rows]==list(range(len(rows)))
    return [r for i,r in rows],counts,groups


def check_source_and_links(label, rows):
    raw=[r for r in rows if r.kind=='raw_source'];expected=load(OUT/(label+'-source.json'))
    assert [r.get('raw_typed') for r in raw]==expected
    assert [r.get('source_sequence') for r in raw]==list(range(len(expected)))
    originals=[c3.decode(v) for v in expected];metric={}
    for v in originals:
        if isinstance(v,tuple) and type(v[2]).__name__=='MetricPoint':metric[(v[0],v[1],v[2].metric,v[2].sample_us)]=c3.encode(v[2])
    for r in rows:
        if r.kind=='metric':assert c3.encode(r.get('raw'))==metric[(*r.event,*r.key)]
    locations={(r.kind,r.event,r.key):r for r in rows}
    for r in rows:
        if r.kind=='evidence_source':assert r.get('source_locator') in locations
        if r.kind=='evidence':
            scientific=locations[r.get('source_locator')]
            assert r.get('values')==scientific.values and r.get('source_refs')==scientific.refs
    return raw,originals


def fresh_read(label):
    q,b,p,rt=read_binding(label);reader=TrendReader(b,p,runtime=rt)
    start=time.monotonic();output=list(reader.scan());assert isinstance(output[-1],TrendReadReceipt) and output[-1].full_body_validated
    rows=output[:-1];physical,counts,groups=physical_rows(b)
    assert [encode(row_material(r)) for r in rows]==[encode(row_material(r)) for r in physical]
    raw,original=check_source_and_links(label,rows)
    (OUT/(label+'-ordered.typed')).write_text(encode(tuple(row_material(r) for r in rows)))
    facts={}
    if label=='unknown411':
        assert len(raw)==411 and len(rows)==2139
        profiles=[r for r in rows if r.kind=='profile' and r.key==('visible_direction_count',)]
        peaks=[r for r in rows if r.kind=='peak' and r.key==('visible_direction_count',)]
        assert len(profiles)==2 and all(r.get('denominator') is None for r in profiles)
        assert all(r.get('known_peak') is not None and r.get('exact_peak') is None for r in peaks)
        assert any(isinstance(v,tuple) and isinstance(v[2],c2.BoundaryUnavailable) for v in original)
        facts['unknown_profiles']=len(profiles)
    elif label=='wide':
        assert len(raw)==953
        expected_counts={'activity_relation':6,'activity_window':4,'reference_cdf':6,'reference_common':6,'reference_shape':2}
        assert {k:counts[k] for k in expected_counts}==expected_counts
        for r in rows:
            if r.kind=='reference_cdf':assert r.get('percentile')==100 and r.get('comparable_count')==2
            if r.kind=='reference_shape':assert r.get('share')==1
            if r.kind=='reference_common':assert r.get('declining_count')==0 and r.get('share')==0 and r.get('causal_claim') is False
            if r.kind=='activity_relation':assert r.get('causal_claim') is False
        windows=[r.get('raw') for r in rows if r.kind=='activity_window']
        assert any(w.start_us==114000000 and w.end_us==120000000 and w.metric=='announ_num' and w.value==1 for w in windows)
        assert any(w.start_us==114000000 and w.end_us==120000000 and w.metric=='withdraw_num' and w.value==0 for w in windows)
        facts['nonempty_contexts']=expected_counts
    elif label=='revision':
        history=[r for r in raw if r.get('source_table')=='input_evidence' and c3.decode(r.get('raw_typed'))[2].kind=='event_revision']
        assert [r.event[1] for r in history]==[1,2]
        events=[r for r in rows if r.kind=='event'];assert len(events)==p['events']==1 and events[0].event[1]==2
        assert all(r.event==events[0].event for r in rows if r.kind!='raw_source' and r.event)
        assert reader.query('raw_source',key=history[0].key,limit=1).items==(history[0],)
        facts['history']=[r.get('raw_typed') for r in history]
    save(OUT/(label+'-new-process.json'),dict(binding=asdict(b),receipt=asdict(output[-1]),counts=counts,row_groups=groups,
         typed_bytes=sum(len(encode(row_material(r)).encode()) for r in rows),wall_seconds=time.monotonic()-start,
         charged_rows=reader.budget.rows,charged_bytes=reader.budget.byte_count,references=reader.budget.references,
         rss_lifetime_peak_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,facts=facts,
         scope='full scan与物理全列比较及原C3保留原文；RSS含导入/预期比较且不含PG，非阶段峰'))


@pytest.mark.parametrize('label',['unknown411','wide','revision'])
def test_frozen_new_process_all_fields(label):
    code='import sys;sys.path[:0]=sys.argv[1:3];from tests.country.test_c5_s2_integration import fresh_read;fresh_read(sys.argv[3])'
    child=subprocess.run([sys.executable,'-I','-c',code,str(REPO/'backend'),str(REPO/'backend/tests'),label],capture_output=True,text=True,cwd=REPO/'backend')
    (OUT/(label+'-new-process.log')).write_text(child.stdout+child.stderr)
    assert child.returncode==0,child.stderr


def test_actual_page_reference_budget():
    _,b,p,rt=read_binding('wide')
    reader=TrendReader(b,p,runtime=rt,limits=S2Limits(max_references=1))
    with pytest.raises(ValueError,match='trend_reference_budget'):reader.query('edge',limit=1)
    assert reader.budget.references==2 and not reader.budget.scratch_roots
    physical,_,_=physical_rows(b);edge=next(r for r in physical if r.kind=='edge')
    enough=TrendReader(b,p,runtime=rt,limits=S2Limits(max_references=2))
    result=list(enough.iter_query('edge',event=edge.event,key=edge.key,page_size=1))
    assert result[:-1]==[edge] and isinstance(result[-1],TrendReadReceipt) and result[-1].full_body_validated is False
    limited=TrendReader(b,p,runtime=rt,limits=S2Limits(max_references=6));emitted=[]
    with pytest.raises(ValueError,match='trend_reference_budget'):
        for r in limited.iter_query('edge',page_size=1):emitted.append(r)
    assert len(emitted)==1 and not any(isinstance(r,TrendReadReceipt) for r in emitted) and limited.budget.references==8
    save(OUT/'页引用预算.json',dict(max1_charged=reader.budget.references,filtered_max2_charged=enough.budget.references,
         filtered_receipt=asdict(result[-1]),cross_page_max6_charged=limited.budget.references,emitted_before_failure=len(emitted)))


def test_actual_wrong_inner_boundary_rejected():
    from data_pipeline.analysis.country_events.snapshot_store import persist_stream
    from data_pipeline.analysis.country_events.selection_contract import contract_value, contract_json
    from data_pipeline.analysis.country_events.selection_index import prepare_country_index, inspect_country
    q,_,_,rt=read_binding('unknown411');descriptor=contract_value(q['country_descriptor'])
    original=load(OUT/'unknown411-source.json')
    rows=[c3.decode(v) for v in original]
    actual=[v if isinstance(v,c2.C2Completion) else c2.C2Row(v[0],v[1],v[2]) for v in rows]
    bad=[replace(r,value=replace(r.value,incident_id='WRONG-EVENT')) if isinstance(r,c2.C2Row) and isinstance(r.value,c2.BoundaryUnavailable) else r for r in actual]
    assert sum(a!=b for a,b in zip(actual,bad))==2
    manifest=load(Path(descriptor.read_binding.component.root)/'manifest.json')
    binding=persist_stream(iter(bad),rt.country.component_dsn,OLD/'c5s2-bad-boundary-c3',c1=rt.country.c1,parameters=c3.decode(manifest['parameters']))
    read,proof=prepare_country_index(binding,reference_binding=descriptor.read_binding.reference,runtime=rt.country,output=OLD/'c5s2-bad-boundary-c4')
    desc=inspect_country(read,proof,runtime=rt.country)
    negative={**q,'country_descriptor':contract_json(desc),'output':str(OLD/'c5s2-bad-boundary-trend'),'receipt':str(OUT/'bad-boundary-receipt.json')}
    frozen(negative,'bad-boundary',failure=True)
    save(OUT/'错内层Boundary拒绝.json',dict(c3_rows=proof.source_rows,changed_rows=2,original_raw_unchanged=True,new_s2_state='failed',binding=None,proof=None))


def test_qualification_and_page_costs_separate(monkeypatch):
    _,b,p,rt=read_binding('unknown411');counts=Counter();connect=module.lake_connect;file_hash=module.file_hash;witness=module.inspect_witnesses
    class DB:
        def __init__(self,db):self.db=db
        def __getattr__(self,k):return getattr(self.db,k)
        def execute(self,*a,**k):counts['direct_lake_sql']+=1;return self.db.execute(*a,**k)
    def hashed(path,*a,**k):counts['direct_file_hash']+=1;counts['direct_hash_bytes']+=Path(path).stat().st_size;return file_hash(path,*a,**k)
    def inspected(db,binding,guard,budget=None,**kw):
        counts['witness_calls']+=1;rows=budget.rows;size=budget.byte_count
        result=witness(db,binding,guard,budget,**kw)
        counts['witness_rows']+=budget.rows-rows;counts['witness_typed_bytes']+=budget.byte_count-size
        return result
    monkeypatch.setattr(module,'lake_connect',lambda *a,**k:DB(connect(*a,**k)))
    monkeypatch.setattr(module,'file_hash',hashed);monkeypatch.setattr(module,'inspect_witnesses',inspected)
    monkeypatch.setattr(ComponentReader,'stream',lambda *a,**k:pytest.fail('S2 Reader不应重读公开C3流'))
    facts={};start=time.monotonic();reader=TrendReader(b,p,runtime=rt)
    facts['construction']=dict(counts,wall_seconds=time.monotonic()-start,charged_rows=reader.budget.rows,charged_bytes=reader.budget.byte_count)
    counts.clear();before=(reader.budget.rows,reader.budget.byte_count);start=time.monotonic();page=reader.query('metric',limit=1)
    assert len(page.items)==1 and counts['witness_calls']==0
    facts['ordinary_page']=dict(counts,wall_seconds=time.monotonic()-start,charged_rows=reader.budget.rows-before[0],charged_bytes=reader.budget.byte_count-before[1])
    counts.clear();before=(reader.budget.rows,reader.budget.byte_count);start=time.monotonic();output=list(reader.scan())
    assert isinstance(output[-1],TrendReadReceipt) and counts['witness_calls']==1
    facts['full_scan']=dict(counts,wall_seconds=time.monotonic()-start,charged_rows=reader.budget.rows-before[0],charged_bytes=reader.budget.byte_count-before[1])
    facts['scope']='仅s2_reader直接湖SQL/file_hash及资格计费；不是PG全SQL/OS IO。资格过滤排序扫描未等同返回行数。普通页无资格全扫；构造/全扫分别有一次。'
    save(OUT/'资格与普通页成本.json',facts)
