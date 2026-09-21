"""原 Reader 查询的类型与枚举回归；隔离 SQL fixture 不构成生产资格。"""
from dataclasses import asdict
import random
from types import SimpleNamespace

import duckdb
import pytest

from data_pipeline.bgp.archive.message_reader import ObservationReader, SourceStart, MessageBatch
from data_pipeline.bgp.archive.store import TABLES


def read_fixture(seed, batch_rows, profile, null_order='NULLS_FIRST'):
    db = duckdb.connect()
    db.execute('SET default_null_order='+null_order)
    columns = {k:list(TABLES[k]) for k in ('messages','elements','paths','quality','eor','peers')}
    if profile == 'observation': columns['messages'].append(('interpretation','VARCHAR'))
    rows = {k:[] for k in columns}
    rows['messages'] = [dict(message_id='m'+str(i),source_id='z',content_sha256='a'*64,record=i,kind='update',local_message=False) for i in range(2)]
    rows['elements'] = [dict(event_id='event',message_id='m1',ordinal=0,path_key='path',action='announce',raw_prefix=b'\xc0\x00\x02')]
    rows['paths'] = [dict(path_key='path',as_path_text='64497 {3,2} 1',as_path_raw=b'\x02\x01\x00\x01',attributes_raw=b'\xff\x00',attributed_origin_asn=None)]
    rows['quality'] = [dict(source_id='z',message_id=m,code=c,detail=d) for m in (None,'m1') for c,d in [('z','last'),('a',None),('a','first'),('a','first')]]
    rows['eor'] = [dict(message_id='m1',afi=a,safi=s) for a,s in [(2,1),(1,None),(1,1),(1,1)]]
    rows['peers'] = [dict(source_id='z',table_record=0,index=i,bgp_id=b,ip=p,asn=a,bgp_id_present=v) for i,b,p,a,v in [(1,None,'x',2,False),(0,'b','x',2,True),(0,'a','x',2,True),(0,'a','x',1,True),(0,'a','x',1,False),(0,'a','x',1,False)]]
    for table, cols in columns.items():
        db.execute('CREATE TABLE '+table+' ('+','.join('"'+n+'" '+t for n,t in cols)+')')
        random.Random(seed).shuffle(rows[table])
        for row in rows[table]: db.execute('INSERT INTO '+table+' VALUES ('+','.join('?' for _ in cols)+')',[row.get(n) for n,_ in cols])
    reader = object.__new__(ObservationReader)
    reader.run_id='fixture'; reader.snapshot=0; reader.batch_rows=batch_rows; reader.batch_bytes=10**6
    reader.selection=SimpleNamespace(columns=columns) if profile == 'observation' else None
    reader.starts=[SourceStart('fixture',0,'z','a'*64,'updates',2,1)]
    reader.connect=lambda:db; reader.bound_table=lambda name,source_id=None:name
    reader.guard=lambda:None; reader.check_sources=lambda starts:None
    result=[]
    for item in reader.stream():
        if isinstance(item,MessageBatch):
            result.extend(('quality',r) for r in item.source_quality)
            result.extend(('message',r) for r in item.messages)
            result.extend(('element',r) for r in item.elements)
        else: result.append((type(item).__name__,asdict(item)))
    return result


@pytest.mark.parametrize('profile',['complete','observation'])
def test_metadata_order_and_full_typed_rows(profile):
    expected=read_fixture(0,1,profile)
    for seed in (1,2,3):
        for batch in (1,17):
            assert read_fixture(seed,batch,profile,'NULLS_LAST') == expected
    qualities=[r for k,r in expected if k=='quality']
    assert [(r['code'],r['detail']) for r in qualities] == [('a','first'),('a','first'),('a',None),('z','last')]
    messages=[r for k,r in expected if k=='message']
    assert [r['record'] for r in messages]==[0,1]
    assert [(r['afi'],r['safi']) for r in messages[1]['eor']]==[(1,1),(1,1),(1,None),(2,1)]
    assert len(messages[0]['peers'])==6
    assert [r['bgp_id_present'] for r in messages[0]['peers'][:3]]==[False,False,True]
    element=next(r for k,r in expected if k=='element')
    assert element['as_path_text']=='64497 {3,2} 1'
    assert element['as_path_raw']==b'\x02\x01\x00\x01'
    assert element['attributes_raw']==b'\xff\x00' and element['attributed_origin_asn'] is None


def downstream_seams(stream):
    """实际资格/事件身份出口的小型探针，不代表完整科学计算。"""
    from datetime import datetime, timezone
    from data_pipeline.analysis.features.qualification import Qualification
    from data_pipeline.bgp.record_types import ParseCounts
    from data_pipeline.analysis.detection._results import Results
    from dataclasses import make_dataclass
    captured=[]
    store=SimpleNamespace(append=lambda table,row:captured.append((table,row)))
    q=Qualification('fixture',store)
    instant=datetime(2026,1,1,tzinfo=timezone.utc)
    q.begin(0,SimpleNamespace(source_id='z',message_quality_state='complete',window=SimpleNamespace(start=instant,end=instant,coverage='complete')))
    messages=[]
    for kind,row in stream:
        if kind=='quality':q.quality(None,row['code'],row['detail'])
        if kind=='message':
            messages.append(row)
            q.boundary(SimpleNamespace(position=SimpleNamespace(record=row['record']),raw=row,gap=None))
    q.finish(SimpleNamespace(parse_counts=ParseCounts(2,0,0,0),raw=SimpleNamespace(messages=2),binding_ref='fixture',source_rank=0))
    scope=make_dataclass('Scope',[('run_id',str),('source',str)])('fixture','z')
    result=Results(scope,SimpleNamespace(version='ref',historical_applicability='unknown'))
    result.context={'source_message':messages[1]}
    result.event_revision('leak','prefix','legacy',{'s_time':'2026-01-01','leak_phenomenon_table':'fixture'})
    return captured,result.rows


def test_clean_processes_and_downstream_identity_seams():
    import json
    from pathlib import Path
    import subprocess
    import sys
    from data_pipeline.bgp.archive.value_codec import typed
    expected=read_fixture(0,1,'observation')
    seam=downstream_seams(expected)
    for seed in (2,7):
        actual=read_fixture(seed,17,'observation')
        assert downstream_seams(actual)==seam
        code='''import sys;sys.path[:0]=sys.argv[1:3];from tests.observations.test_observation_metadata_order import read_fixture, downstream_seams;from data_pipeline.bgp.archive.value_codec import typed;import json;v=read_fixture(int(sys.argv[3]),17,'observation');print(json.dumps([typed(v),typed(downstream_seams(v))]))'''
        output=subprocess.check_output([sys.executable,'-I','-B','-c',code,str(Path(__file__).resolve().parents[2]),str(Path(__file__).resolve().parent),str(seed)],text=True)
        assert json.loads(output)==[typed(expected),typed(seam)]
