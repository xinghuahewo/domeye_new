"""8233自有M2→冻结Feature→新进程读取，保留旧strict/Q1原制品。"""
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
import gc
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
import psycopg2
from psycopg2.extensions import make_dsn
import pytest
from tests.features.test_feature_qualified import prepare, formal, raw_update, rows
from tests.features.test_feature_qualified import test_empty_inherited_gap_has_coverage_without_asn_rows
from tests.observations.test_observation_consumer import feature_dsn
from data_pipeline.analysis.features.store import TABLES, read_table
from data_pipeline.analysis.features.qualification import TABLES as QT, DIMENSIONS, PROFILE
from data_pipeline.analysis.features.qualified_read import inspect_binding, read_windows, read_coverage


def save(p,value):p.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str))
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,default=str).encode()).hexdigest()
def canonical(rows):return sorted(rows,key=lambda r:json.dumps(r,sort_keys=True,default=str))


@pytest.fixture(scope='module')
def integrated(tmp_path_factory):
    base=os.environ.get('DOMEYE_FEATURE_TEST_DSN')
    if not base:pytest.skip('需要显式自有人工PG')
    root=tmp_path_factory.mktemp('feature-m3');name='feature_m3_8233_'+uuid.uuid4().hex[:10]
    with closing(psycopg2.connect(base)) as pg:
        pg.autocommit=True
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    dsn=make_dsn(base,dbname=name)
    local=(raw_update(200,subtype=7,ann=b'',withdrawn=b'\x18\xc0\x00\x02',attrs=b'')+
           raw_update(200,subtype=7)+raw_update(200,subtype=7,attrs=b'\xf0\x23\x04\x00\x04\x2f\x66'))
    inputs,seal=prepare(root/'input',dsn,first_raw=local)
    from data_pipeline.bgp.archive.message_reader import MessageBatch
    from data_pipeline.bgp import record_types as o
    reader=inputs.blocks[0];raw=list(reader.stream());ordered=list(inputs.stream())
    messages=[r for b in raw if isinstance(b,MessageBatch) for r in b.messages]
    elements=[r for b in raw if isinstance(b,MessageBatch) for r in b.elements]
    boundaries=[x for x in ordered if isinstance(x,o.MessageBoundary)]
    assert [x.raw for x in boundaries]==messages
    assert [x.raw for x in ordered if isinstance(x,o.Element)]==elements
    gap=next(x.gap for x in boundaries if x.gap)
    assert gap.direction.value=='local' and gap.position.record==2
    assert [(x['action'],x['local_message']) for x in elements if x['source_id']==inputs.source_specs[1]['source_id']]==[('withdraw',True),('announce',True)]
    save(root/'原消息与元素.json',{'messages':messages,'elements':elements,'gap':asdict(gap),'complete_raw_digest':digest((messages,elements))})
    report=formal(root/'formal',inputs,batch_rows=8)
    save(root/'绑定请求.json',{'dsn':dsn,'run_id':report['run_id'],'snapshot':report['snapshot']})
    return root,dsn,inputs,seal,report,gap


def test_frozen_science_qualification_and_fresh_reader(integrated):
    root,dsn,inputs,seal,report,gap=integrated
    tables={t:rows(dsn,report,t) for t in {**TABLES,**QT}}
    assert {t:len(v) for t,v in tables.items()}==report['actual_rows']
    binding=inspect_binding(dsn,report['run_id'],report['snapshot'])
    windows=list(read_windows(dsn,binding));coverage=list(read_coverage(dsn,binding))
    collect={(x['raw']['mode'],x['raw']['source_rank']):x for x in windows if x['raw']['scope']=='collect'}
    for mode in ('ordinary','ir'):
        current=collect[mode,1];later=collect[mode,2]
        assert current['raw_values']['announ_num']==current['raw_values']['withdraw_num']==1
        assert all(v is None for v in current['values'].values())
        assert later['values']['announ_num']==(2 if mode=='ordinary' else 1) and later['values']['withdraw_num']==1
        assert later['values']['v4IP_num'] is None
        expected={'window_counts':'complete','announcement_attribution':'complete','withdrawal_attribution':'partial','resources':'partial','sparse':'partial','comparison':'partial'}
        assert {d:q['coverage'] for d,q in later['qualifications'].items()}==expected
        assert all(q['coverage']=='partial' for q in current['qualifications'].values())
        saved=next(g for g in tables['input_gaps'] if g['mode']==mode)
        for k,v in [('gap_id',gap.gap_id),('raw_digest',gap.raw_ref.raw_digest),('interpretation_digest',gap.interpretation_digest),('offset',gap.raw_ref.offset),('length',gap.raw_ref.length),('record',gap.position.record)]:assert saved[k]==v
        assert saved['all_future_prefixes'] and saved['all_future_origin_asns']
    assert len(coverage)==36 and seal['business']=='not_run'
    with pytest.raises(ValueError,match='profile'):list(read_table(dsn,report['run_id'],report['snapshot'],'windows'))
    from data_pipeline.results.resource_feature_bindings import feature_history
    with pytest.raises(ValueError,match='profile'):feature_history(dsn,report['run_id'],report['snapshot'])
    save(root/'全部主体.json',tables);save(root/'本进程公开读取.json',{'binding':binding,'windows':windows,'coverage':coverage})
    code='''import json,sys,time,resource
sys.path.insert(0,sys.argv[1])
from data_pipeline.analysis.features.qualified_read import inspect_binding, read_windows, read_coverage
q=json.load(open(sys.argv[2]));start=time.monotonic()
b=inspect_binding(q['dsn'],q['run_id'],q['snapshot']);w=list(read_windows(q['dsn'],b));c=list(read_coverage(q['dsn'],b))
print(json.dumps(dict(binding=b,windows=w,coverage=c,wall_seconds=time.monotonic()-start,peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024))))
'''
    child=subprocess.run([sys.executable,'-I','-c',code,str(Path(__file__).resolve().parents[2]),str(root/'绑定请求.json')],capture_output=True,text=True)
    (root/'新进程.stdout').write_text(child.stdout);(root/'新进程.stderr').write_text(child.stderr)
    assert child.returncode==0,child.stderr
    result=json.loads(child.stdout);assert result['binding']==binding
    assert canonical(result['windows'])==canonical(windows) and canonical(result['coverage'])==canonical(coverage)
    save(root/'完整对账与成本.json',{'counts':report['actual_rows'],'raw_table_digests':{t:digest(canonical(v)) for t,v in tables.items()},'public_windows':len(windows),'public_coverage':len(coverage),'fresh_reader_wall':result['wall_seconds'],'fresh_reader_lifetime_rss':result['peak_rss_bytes'],'feature_parquet_bytes':sum(p.stat().st_size for p in (root/'formal/output').rglob('*.parquet')),'upstream_cost':json.loads((root/'input/upstream-cost.json').read_text()),'formal_wall':json.loads((root/'formal/wall.json').read_text()),'run_id':report['run_id'],'snapshot':report['snapshot']})


def test_retained_exception_closed_without_gc(integrated):
    root,base,_,_,report,_=integrated;app='feature_m3_close_'+uuid.uuid4().hex;dsn=make_dsn(base,application_name=app)
    run=report['run_id'];held=[]
    with closing(psycopg2.connect(base)) as pg,pg,pg.cursor() as c:
        c.execute('SELECT receipt FROM feature.qualified_results WHERE run_id=%s',(run,));original=c.fetchone()[0]
        c.execute('UPDATE feature.qualified_results SET receipt=%s WHERE run_id=%s',(json.dumps({**original,'specification_digest':'bad'}),run))
    enabled=gc.isenabled();gc.disable()
    try:
        try:inspect_binding(dsn,run,report['snapshot'])
        except ValueError as e:held.append(e)
        assert len(held)==1 and held[0].__traceback__ is not None and '完成锚' in str(held[0])
        with closing(psycopg2.connect(base)) as pg,pg,pg.cursor() as c:
            c.execute('SELECT pid,state FROM pg_stat_activity WHERE application_name=%s',(app,));sessions=c.fetchall()
        assert sessions==[]
        save(root/'持有异常连接复验.json',{'exception':str(held[0]),'traceback_retained':True,'gc_disabled':True,'sessions':sessions})
    finally:
        if enabled:gc.enable()
        with closing(psycopg2.connect(base)) as pg,pg,pg.cursor() as c:c.execute('UPDATE feature.qualified_results SET receipt=%s WHERE run_id=%s',(json.dumps(original),run))


def test_original_strict_feature_and_q1_without_reproduction(tmp_path):
    location=os.environ.get('DOMEYE_FEATURE_OLD_ROOT')
    if not location:pytest.skip('需绑定本任务原人工制品')
    root=Path(location);request=json.loads((root/'feature-request.json').read_text());dsn=request['dsn']
    report=json.loads((root/'feature/execution.json').read_text());q1=json.loads((root/'q1-evidence.json').read_text())
    before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
    actual={t:[r for b in read_table(dsn,report['run_id'],report['snapshot'],t) for r in b.to_pylist()] for t in TABLES}
    assert {t:len(v) for t,v in actual.items()}==report['actual_rows']
    from data_pipeline.results import Publication, Token
    pub=Publication(dsn,root);pages=0
    for saved in q1['old_pages']:
        if saved['kind']!='feature':continue
        for page in saved['pages']:
            assert pub.query(Token(**q1['first']),'feature',mode=saved['mode'],page=page['page'],page_size=page['page_size'])==page;pages+=1
    assert pages>0
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==v for p,v in before.items())
    save(tmp_path/'旧Feature与Q1.json',{'root':str(root),'run':report['run_id'],'snapshot':report['snapshot'],'counts':{t:len(v) for t,v in actual.items()},'full_table_digests':{t:digest(canonical(v)) for t,v in actual.items()},'q1_token':q1['first'],'original_pages_equal':pages,'files_unchanged':len(before)})
