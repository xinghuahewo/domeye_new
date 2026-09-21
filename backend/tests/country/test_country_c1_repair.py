"""eb30432d原四反例：仅自己的正式人工制品；回执期待值不随损坏更新。"""
import gzip
import hashlib
import json
import os
from pathlib import Path
import struct
from dataclasses import replace

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from data_pipeline.bgp.archive.message_reader import MessageBatch
from data_pipeline.analysis.country_events import saved_input
from data_pipeline.analysis.country_events.saved_input import InputCompletion, CountryRevision
from tests.country.test_country_saved_input import build_pipeline, adapter, mutate_saved, multiple_country_mrt


@pytest.fixture(scope='module')
def review_chain(tmp_path_factory):
    base=os.environ.get('DOMEYE_COUNTRY_C1_TEST_DSN')
    if not base:pytest.skip('需要本任务私有PG')
    root=tmp_path_factory.mktemp('c1-review-repair')
    manifest=multiple_country_mrt(root)
    from tests.observations.test_observation_mrt import mrt
    from data_pipeline.bgp.input.mrt_reader import source_identity
    def endpoint(i):return struct.pack('!IIHH',100+i,999,0,1)+bytes([192,0,2,i+1,192,0,2,254])
    payload=b'\0\0'+struct.pack('!H',13)+b'\x40\x02\x0a\x02\x02'+struct.pack('!II',100,1)+bytes([24,10,9,0])
    bgp=b'\xff'*16+struct.pack('!HB',len(payload)+19,2)+payload
    extras=mrt(endpoint(0)+bgp,epoch=120)+mrt(endpoint(0)+struct.pack('!HH',6,1),epoch=121,subtype=5)+mrt(endpoint(1)+struct.pack('!HH',6,1),epoch=121,subtype=5)
    eor=b'\xff'*16+struct.pack('!HB',23,2)+b'\0'*4
    extras+=mrt(endpoint(0)+bgp,epoch=122,subtype=7)+mrt(struct.pack('!I',123456)+endpoint(0)+eor,epoch=123,kind=17)+mrt(endpoint(0)+eor,epoch=122)
    for name,raw in [('extras',extras),('empty',b'')]:
        path=root/(name+'.gz');path.write_bytes(gzip.compress(raw,mtime=0))
        sha=hashlib.sha256(path.read_bytes()).hexdigest();uri='fixture://rrc25/'+name
        entry=dict(path=str(path),sha256=sha,size=path.stat().st_size,origin_uri=uri,source_id=source_identity('rrc25',uri,sha),role='update')
        manifest['inputs'].append(entry);manifest['update_sources'].append(entry['source_id'])
    return build_pipeline(root,base,manifest,rich=True,multicountry=True)


def test_independent_shape_and_index_costs(review_chain):
    rows=list(adapter(review_chain).stream());end=rows[-1]
    assert (end.counts['changes'],end.counts['invalidations'],end.counts['country_revisions'])==(25,9,2)
    assert end.baseline_initialized_elements==12
    expected=dict(source_join_queries=1,source_peer_queries=1,event_index_rows=26,peer_index_rows=3,event_index_lookups=2,peer_index_lookups=3)
    assert {k:end.lookup_stats[k] for k in expected}==expected
    assert all('SEARCH' in end.lookup_stats[k] and 'USING INDEX' in end.lookup_stats[k] for k in ('event_lookup_plan','peer_lookup_plan'))
    assert not list(review_chain[0].glob('country-c1-*'))
    other=list(adapter(review_chain,batch_rows=1).stream())[-1]
    assert replace(other,resource_limits=end.resource_limits)==end


@pytest.mark.parametrize('table', ['changes','invalidations'])
def test_original_missing_saved_state_rows(review_chain,table):
    fn=(lambda t:t.filter(pc.equal(t['source_rank'],0))) if table=='changes' else (lambda t:t.slice(0,0))
    with mutate_saved(review_chain,table,fn):
        with pytest.raises(ValueError,match='production_table_count_mismatch:'+table):list(adapter(review_chain).stream())
    assert isinstance(list(adapter(review_chain).stream())[-1],InputCompletion)


def test_original_same_second_wrong_state_endpoint(review_chain):
    rows=list(adapter(review_chain).stream())
    states=[m for b in rows if isinstance(b,MessageBatch) for m in b.messages if m['kind']=='state_change']
    assert len(states)==2 and states[0]['epoch']==states[1]['epoch'] and states[0]['peer_ip']!=states[1]['peer_ip']
    def wrong(t):
        index=t.schema.get_field_index('source')
        return t.set_column(index,t.schema.field(index),pa.array([states[1]['message_id'] if s==states[0]['message_id'] else s for s in t['source'].to_pylist()]))
    with mutate_saved(review_chain,'invalidations',wrong):
        with pytest.raises(ValueError,match='invalidation_endpoint_mismatch'):list(adapter(review_chain).stream())


def test_revision_size_and_guard_before_yield(review_chain,monkeypatch):
    original=saved_input.read_revisions
    def large(*args,**kwargs):
        for row in original(*args,**kwargs):
            if row['event_kind']=='country_outage':row['legacy']['fixture_large_text']='X'*200000
            yield row
    monkeypatch.setattr(saved_input,'read_revisions',large)
    seen=[]
    with pytest.raises(ValueError,match='resource_limit:row_bytes'):
        for row in adapter(review_chain,batch_bytes=65536).stream():seen.append(row)
    assert not any(isinstance(r,(CountryRevision,InputCompletion)) for r in seen)
    entering=[False]
    def marked(*args,**kwargs):
        for row in original(*args,**kwargs):
            if row['event_kind']=='country_outage':entering[0]=True
            yield row
    monkeypatch.setattr(saved_input,'read_revisions',marked)
    def guard():
        if entering[0]:raise RuntimeError('revision_guard')
    with pytest.raises(RuntimeError,match='revision_guard'):list(adapter(review_chain,guard=guard).stream())


@pytest.mark.parametrize('limit', ['max_revisions','max_events'])
def test_event_and_revision_limits_separate(review_chain,limit):
    with pytest.raises(ValueError):list(adapter(review_chain,**{limit:1}).stream())


@pytest.mark.parametrize('mode',['missing','changed','midstream','late_table_loss'])
def test_receipt_or_rows_cannot_be_revoked(review_chain,mode):
    path=review_chain[0]/'observation/execution.json';raw=path.read_bytes()
    a=adapter(review_chain);stream=a.stream()
    try:
        if mode in ('midstream','late_table_loss'):
            # 到最后一个事件输出之后才撤销，必须最终复验。
            for row in stream:
                if isinstance(row,CountryRevision) and row.country=='YY':break
        if mode=='missing':path.unlink()
        elif mode in ('changed','midstream'):path.write_bytes(raw+b' ')
        if mode=='late_table_loss':
            with mutate_saved(review_chain,'changes',lambda t:t.filter(pc.equal(t['source_rank'],0))):
                with pytest.raises(ValueError,match='production_table_count_mismatch'):list(stream)
        else:
            with pytest.raises((ValueError,FileNotFoundError)):list(stream)
    finally:
        path.write_bytes(raw);stream.close()


def test_large_legal_reference_budget_is_configurable(tmp_path):
    base=os.environ.get('DOMEYE_COUNTRY_C1_TEST_DSN')
    if not base:pytest.skip('需要私有PG')
    prepared=build_pipeline(tmp_path,base,multiple_country_mrt(tmp_path),rich=True,multicountry=True,large_reference_bytes=20*1024**2)
    with pytest.raises(ValueError,match='resource_limit:row_bytes'):list(adapter(prepared).stream())
    rows=list(adapter(prepared,max_row_bytes=32*1024**2,batch_bytes=1024**2).stream())
    assert isinstance(rows[-1],InputCompletion)
    assert rows[-1].counts['references']>=25


def test_event_count_does_not_repeat_source_lookup_scan(review_chain,monkeypatch):
    original=saved_input.read_revisions
    # 只放大不同事件引用的输入适配成本，非新事件生产或全规模性能证明。
    def more(*args,**kwargs):
        for row in original(*args,**kwargs):
            if row['event_kind']=='country_outage':
                for revision in range(1,21):
                    incident=row['incident_id']+'_'+str(revision)
                    clone={**row,'incident_id':incident,'revision':1,'attributes':{**row['attributes'],'incident_id':incident,'revision':1}}
                    yield clone
            else:yield row
    baseline=list(adapter(review_chain).stream())[-1]
    monkeypatch.setattr(saved_input,'read_revisions',more)
    enlarged=list(adapter(review_chain).stream())[-1]
    assert baseline.lookup_stats['source_join_queries']==enlarged.lookup_stats['source_join_queries']==1
    assert baseline.lookup_stats['source_peer_queries']==enlarged.lookup_stats['source_peer_queries']==1
    assert (baseline.lookup_stats['event_index_lookups'],enlarged.lookup_stats['event_index_lookups'])==(2,40)
    assert baseline.lookup_stats['event_index_rows']==enlarged.lookup_stats['event_index_rows']==26
    (review_chain[0]/'lookup-cost-evidence.json').write_text(json.dumps({'two_events':baseline.lookup_stats,'forty_injected_events':enlarged.lookup_stats},indent=2))


@pytest.mark.parametrize('name', ['max_rss_bytes','max_scratch_bytes'])
def test_runtime_resource_limits_and_cleanup(review_chain,name):
    with pytest.raises(ValueError,match='resource_limit:'):
        list(adapter(review_chain,**{name:1}).stream())
    assert not list(review_chain[0].glob('country-c1-*'))


def test_complete_revision_size_includes_nested_typed_fields(review_chain):
    from dataclasses import asdict
    from data_pipeline.bgp.archive.message_reader import byte_size
    events=[r for r in adapter(review_chain).stream() if isinstance(r,CountryRevision)]
    largest=max(byte_size(asdict(r)) for r in events)
    with pytest.raises(ValueError,match='resource_limit:row_bytes'):
        list(adapter(review_chain,max_row_bytes=largest-1).stream())
