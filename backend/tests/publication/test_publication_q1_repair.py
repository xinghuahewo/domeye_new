"""Q1两项P2增量检查；复用本任务已正式计算的人工制品，不重跑业务。"""
import json
import os
from pathlib import Path
import psycopg2
from psycopg2 import sql
import pytest
from data_pipeline.results import Publication, Token


@pytest.fixture
def p():
    path=os.environ.get('DOMEYE_Q1_REPAIR_BINDING')
    if not path:pytest.skip('仅显式绑定本任务人工PG副本')
    binding=json.loads(Path(path).read_text())
    pub=Publication(binding['dsn'],binding['root']);pub.initialize()
    with psycopg2.connect(pub.dsn) as pg,pg.cursor() as c:
        c.execute('TRUNCATE publication_q1.head,publication_q1.rows,publication_q1.builds')
    return pub


def prepare(p,later=False):
    return p.prepare(p.root/('resource-later' if later else 'resource-first')/'execution.json',
                     p.root/'feature/output/execution.json')


def change(p,field,amount,joint=False):
    feature=json.loads((p.root/'feature/output/execution.json').read_text())
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        c.execute(sql.SQL('UPDATE feature.source_commits SET {}={}+%s WHERE run_id=%s').format(sql.Identifier(field),sql.Identifier(field)),(amount,feature['run_id']))
        if joint:
            column='message_count' if field=='messages' else 'element_count'
            c.execute(sql.SQL('UPDATE domeye.source_receipts SET {}={}+%s WHERE run_id=%s').format(sql.Identifier(column),sql.Identifier(column)),(amount,feature['specification']['observation_run']))


@pytest.mark.parametrize('field',['messages','elements'])
@pytest.mark.parametrize('joint',[False,True])
def test_prepare_rejects_counts_against_saved_observations(p,field,joint):
    change(p,field,999,joint)
    try:
        with pytest.raises(ValueError,match='计数|消息|元素|上游'):prepare(p)
        with pytest.raises(ValueError,match='没有'):p.discover()
    finally:change(p,field,-999,joint)


@pytest.mark.parametrize('field',['messages','elements'])
def test_final_gate_rejects_drift_and_keeps_head(p,field):
    old=prepare(p);p.publish(old,expected_generation=0);new=prepare(p,True)
    change(p,field,999)
    try:
        with pytest.raises(ValueError,match='资格|消息|元素'):p.publish(new,expected_generation=1)
        assert p.discover()==(old,1)
    finally:change(p,field,-999)
    assert p.query(old,'feature',mode='ordinary')['total']==8


@pytest.mark.parametrize('mode',['ordinary','ir'])
@pytest.mark.parametrize('page',[1,999])
def test_mode_outflow_and_inflow_rejected_even_on_empty_page(p,mode,page):
    token=prepare(p);p.publish(token,expected_generation=0)
    with psycopg2.connect(p.dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE publication_q1.rows SET mode='ir' WHERE build_id=%s AND kind='feature' AND mode='ordinary'",(token.build_id,))
    # kind仍为13；ordinary从8减至0，ir从5增至13，两种请求都须拒绝。
    with pytest.raises(ValueError,match='请求能力行数'):p.query(token,'feature',mode=mode,page=page)


def test_normal_old_token_and_proven_empty_input_and_offset_page(p):
    old=prepare(p);p.publish(old,expected_generation=0)
    before=p.query(old,'feature',mode='ordinary',page_size=100)
    assert before['total']==8
    collect=[r['value'] for r in before['items'] if r['value']['scope']=='collect']
    assert [(r['announ_num'],r['withdraw_num']) for r in collect]==[(2,1),(0,0)]
    # 完整原始空UPDATE已通过Reader零消息/零元素；不把缺失数据改成零。
    new=prepare(p,True);p.publish(new,expected_generation=1)
    assert p.query(old,'feature',mode='ordinary',page_size=100)==before
    empty=p.query(old,'feature',mode='ordinary',page=999)
    assert empty['total']==8 and empty['items']==[] and empty['availability']=='available'
    # 合法分页空集不等同于整个能力known_empty；Q1固定profile仍要求非空必需输出。
    assert empty['coverage']=='declared_fixture_only'


def test_70c_existing_legitimate_token_remains_readable():
    path=os.environ.get('DOMEYE_Q1_REPAIR_BINDING')
    if not path:pytest.skip('仅本任务70c人工发布')
    legacy=json.loads(Path(path).read_text())['legacy']
    p=Publication(legacy['dsn'],legacy['root']);token=Token(**legacy['token'])
    assert p.query(token,'feature',mode='ordinary')['total']==8
    assert p.query(token,'feature',mode='ir')['total']==5
    assert p.query(token,'feature',mode='ordinary',page=999)['items']==[]
