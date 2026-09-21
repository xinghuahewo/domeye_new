"""Q1.1 两项P2定向增量；仅复用本任务完成的人工制品副本。"""
import copy
from contextlib import contextmanager
import json
import os
from pathlib import Path
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from data_pipeline.results import Publication, Token
from data_pipeline.results import resource_feature_bindings as bindings
from data_pipeline.results.manifest_io import encode


@pytest.fixture
def case():
    path=os.environ.get('DOMEYE_Q11_REPAIR_BINDING')
    if not path:pytest.skip('仅显式本任务人工副本')
    data=json.loads(Path(path).read_text());data['p']=Publication(data['dsn'],data['root'])
    data['report']=json.loads(Path(data['ready']).read_text())
    with psycopg2.connect(data['dsn']) as pg,pg.cursor() as c:c.execute('TRUNCATE publication_q1.head,publication_q1.rows,publication_q1.builds')
    return data


def prepare(case,later=False):
    return case['p'].prepare(case['resource_later' if later else 'resource'],case['ready'])


@contextmanager
def specification(case,spec):
    original=Path(case['ready']).read_bytes();report={**case['report'],'specification':spec}
    Path(case['ready']).chmod(0o600);Path(case['ready']).write_text(encode(report))
    with psycopg2.connect(case['dsn']) as pg,pg.cursor() as c:c.execute('UPDATE feature.runs SET specification=%s WHERE run_id=%s',(Json(spec),report['run_id']))
    try:yield
    finally:
        Path(case['ready']).chmod(0o600);Path(case['ready']).write_bytes(original)
        with psycopg2.connect(case['dsn']) as pg,pg.cursor() as c:c.execute('UPDATE feature.runs SET specification=%s WHERE run_id=%s',(Json(case['report']['specification']),report['run_id']))


@contextmanager
def negative_files(case):
    report=case['report'];run=report['run_id'];backup=[]
    with psycopg2.connect(case['dsn']) as pg,pg.cursor() as c:cat=bindings.catalog(c,'fl_'+run,'f_'+run,report['snapshot'])
    try:
        for entry in cat['files']:
            if entry['table']!='module_diagnostics':continue
            path=Path(entry['path']);raw=path.read_bytes();mode=path.stat().st_mode;backup.append((path,raw,mode))
            table=pq.read_table(path);i=table.schema.get_field_index('source_rank')
            table=table.set_column(i,table.schema.field(i),pa.array([r-len(report['specification']['source_bindings']) for r in table['source_rank'].to_pylist()],type=pa.int64()))
            path.chmod(0o600);pq.write_table(table,path)
            with psycopg2.connect(case['dsn']) as pg,pg.cursor() as c:
                c.execute(sql.SQL('UPDATE {}.ducklake_data_file SET file_size_bytes=%s,footer_size=%s WHERE path=%s').format(sql.Identifier('fl_'+run)),(path.stat().st_size,int.from_bytes(path.read_bytes()[-8:-4],'little'),path.name));assert c.rowcount==1
        yield
    finally:
        for path,raw,mode in backup:
            path.chmod(0o600);path.write_bytes(raw);path.chmod(mode)
            with psycopg2.connect(case['dsn']) as pg,pg.cursor() as c:c.execute(sql.SQL('UPDATE {}.ducklake_data_file SET file_size_bytes=%s,footer_size=%s WHERE path=%s').format(sql.Identifier('fl_'+run)),(len(raw),int.from_bytes(raw[-8:-4],'little'),path.name))


@pytest.mark.parametrize('field',['result_window','comparison_window','calculation_window'])
def test_actual_overall_window_counterexample(case,field):
    spec=copy.deepcopy(case['report']['specification']);spec[field]=['2099-01-01T00:00:00+00:00','2099-01-02T00:00:00+00:00']
    with specification(case,spec):
        if os.environ.get('DOMEYE_Q11_REPAIR_BASELINE'):
            token=prepare(case);case['p'].publish(token,expected_generation=0)
            result=case['p'].query(token,'feature',mode='ordinary')
            assert result['availability']=='available' and result['items'][0]['calculation']['basis'][field]==spec[field]
            print(field,'旧版放行',result['items'][0]['value']['start'])
        else:
            with pytest.raises(ValueError,match='窗口|用途'):prepare(case)


def test_actual_negative_rank_counterexample(case):
    with negative_files(case):
        rows=[r for b in bindings.read_table(case['dsn'],case['report']['run_id'],case['report']['snapshot'],'module_diagnostics') for r in b.to_pylist()]
        assert len(rows)==9 and all(r['source_rank']<0 for r in rows)
        if os.environ.get('DOMEYE_Q11_REPAIR_BASELINE'):
            token=prepare(case);case['p'].publish(token,expected_generation=0)
            assert case['p'].query(token,'feature',mode='ordinary')['availability']=='available'
            print('9条实际负序号，旧版放行')
        else:
            with pytest.raises(ValueError,match='诊断来源序号'):prepare(case)


def set_window(spec,index,**values):
    source=spec['source_bindings'][index]
    for view in [source,*source['aliases']]:view['window'].update(values)
    for name,value in values.items():
        if name in spec['windows'][index]:spec['windows'][index][name]=value


@pytest.mark.parametrize('variant',['gap','partial_envelope','warmup','adjacent','offset_timezone','no_result','initial_only'])
def test_legal_window_contract(case,variant):
    spec=copy.deepcopy(case['report']['specification'])
    if variant=='gap':set_window(spec,2,start='1970-01-01T00:05:50+00:00')
    elif variant=='partial_envelope':
        spec['comparison_window'][0]='1970-01-01T00:02:30+00:00'
        spec['result_window'][1]='1970-01-01T00:12:00+00:00'
    elif variant=='warmup':
        set_window(spec,1,start='1970-01-01T00:01:40+00:00',end='1970-01-01T00:03:20+00:00')
        spec['source_bindings'][1]['window_role']='warmup';spec['calculation_window'][0]='1970-01-01T00:01:40+00:00'
    elif variant=='offset_timezone':
        spec['result_window']=['1970-01-01T08:06:40+08:00','1970-01-01T08:10:00+08:00']
    elif variant=='no_result':
        spec['result_window']=spec['comparison_window']=None
        for s in spec['source_bindings'][1:]:s['window_role']='result'
    elif variant=='initial_only':
        spec['source_bindings']=spec['source_bindings'][:1];spec['windows']=spec['windows'][:1]
        spec['result_window']=spec['comparison_window']=spec['calculation_window']=None
    bindings.validate_windows(spec)


@pytest.mark.parametrize('variant',['naive','empty','nonadjacent','cross_boundary','initial_time','summary','missing_calculation'])
def test_invalid_window_neighbors(case,variant):
    spec=copy.deepcopy(case['report']['specification'])
    if variant=='naive':spec['result_window'][0]='1970-01-01T00:06:40'
    elif variant=='empty':spec['result_window'][1]=spec['result_window'][0]
    elif variant=='nonadjacent':spec['comparison_window'][1]='1970-01-01T00:06:30+00:00'
    elif variant=='cross_boundary':set_window(spec,2,end='1970-01-01T00:06:41+00:00')
    elif variant=='initial_time':spec['initial_rib_time']='2099-01-01T00:00:00+00:00'
    elif variant=='summary':spec['windows'][1]['start']='2099-01-01T00:00:00+00:00'
    else:spec['calculation_window']=None
    with pytest.raises(ValueError,match='窗口|初态'):bindings.validate_windows(spec)


@pytest.mark.parametrize('variant',['negative','too_negative','upper','float','bool','source_sequence','receipt_sequence'])
def test_rank_type_bounds_and_cross_reference(case,monkeypatch,variant):
    report=case['report'];spec=copy.deepcopy(report['specification'])
    history=bindings.feature_history(case['dsn'],report['run_id'],report['snapshot'])
    rows=[r for b in bindings.read_table(case['dsn'],report['run_id'],report['snapshot'],'module_diagnostics') for r in b.to_pylist()]
    row=rows[0];rank=row['source_rank']
    if variant=='source_sequence':spec['source_bindings'][rank]['sequence']+=1
    elif variant=='receipt_sequence':
        next(r for r in history if (r['mode'],r['source_id'])==(row['mode'],row['source_id']))['source_rank']+=1
    else:
        value={'negative':-1,'too_negative':-len(spec['source_bindings'])-1,'upper':len(spec['source_bindings']),'float':float(rank),'bool':True}[variant]
        # 类型反例不经过Arrow自动整数强制转换，保留要审查的实际Python类型。
        row['source_rank']=value
    class Batch:
        def to_pylist(self):return rows
    monkeypatch.setattr(bindings,'read_table',lambda *a,**k:iter([Batch()]))
    with pytest.raises(ValueError,match='诊断来源序号'):
        bindings.validate_diagnostics(case['dsn'],report['run_id'],report['snapshot'],spec,history,lambda:None)


def test_publish_window_drift_keeps_old_head(case):
    old=prepare(case);case['p'].publish(old,expected_generation=0);new=prepare(case,True)
    spec=copy.deepcopy(case['report']['specification']);spec['result_window'][1]='2099-01-01T00:00:00+00:00'
    with psycopg2.connect(case['dsn']) as pg,pg.cursor() as c:c.execute('UPDATE feature.runs SET specification=%s WHERE run_id=%s',(Json(spec),case['report']['run_id']))
    try:
        with pytest.raises(ValueError,match='资格'):case['p'].publish(new,expected_generation=1)
        assert case['p'].discover()==(old,1)
    finally:
        with psycopg2.connect(case['dsn']) as pg,pg.cursor() as c:c.execute('UPDATE feature.runs SET specification=%s WHERE run_id=%s',(Json(case['report']['specification']),case['report']['run_id']))
    assert case['p'].query(old,'feature',mode='ordinary')['total']==15


def test_normal_empty_updates_partial_envelope_and_old_tokens(case):
    # 完成制品的空UPDATE与稀疏0保持；范围允许大于实际来源并仍按原角色消费。
    spec=copy.deepcopy(case['report']['specification']);spec['result_window'][1]='1970-01-01T00:12:00+00:00'
    with specification(case,spec):
        token=prepare(case);case['p'].publish(token,expected_generation=0)
        result=case['p'].query(token,'feature',mode='ordinary',page_size=100)
        assert result['total']==15 and any(r['value']['scope']=='collect' and r['value']['announ_num']==r['value']['withdraw_num']==0 for r in result['items'])
    config=json.loads(Path(case['old_config']).read_text())
    legacy=config['legacy'];p=Publication(legacy['dsn'],legacy['root'])
    assert p.query(Token(**legacy['token']),'feature',mode='ordinary')['total']==8
    for data,total in [(config,8),(case['old_dccb'],15),(json.loads(Path(case['old_seven']).read_text()),15)]:
        p=Publication(data['dsn'],data['root']);token,_=p.discover()
        assert p.query(token,'feature',mode='ordinary')['total']==total
