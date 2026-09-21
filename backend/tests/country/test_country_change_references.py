"""复用临时 Stage 验证计算引用域；不依赖 PG。"""
from dataclasses import replace
import pytest
from data_pipeline.analysis.country_events.aggregation_staging import Stage
from data_pipeline.analysis.country_events.compute import compute_country_enhancement, Grid, Completion
from data_pipeline.analysis.country_events import freeze_cohorts
from tests.country.test_country_enhancement_compute import execute, change, paths
from tests.country.test_country_enhancement_cohort import binding, baseline, incident, route, refs
from tests.country.test_country_c2 import run_case


@pytest.fixture
def stage(tmp_path):
    value=Stage(tmp_path/'stage.sqlite',binding(),lambda:None,{'max_row_bytes':4096})
    try:yield value
    finally:value.close()


def engine(stage, rows):
    return compute_country_enhancement(freeze_cohorts((incident(),),baseline(),[route()],refs()),
        binding(),Grid(15000000,31000000,(21000000,26000000,31000000)),rows,paths(),
        max_changes=1,batch_rows=1,_change_refs_factory=stage.change_references)


def remaining(stage):
    return stage.db.execute('SELECT domain,count(*) FROM change_references GROUP BY domain').fetchall()


def test_disk_full_output_matches_memory_and_c2_low_limit(stage,tmp_path):
    changes=[change(0,23,presence='absent',path_ref=None),change(1,28)]
    actual=[row for batch in engine(stage,changes) for row in batch.rows]
    assert actual==execute(changes,batch_rows=1,max_changes=10)
    assert remaining(stage)==[]
    with pytest.raises(ValueError,match='输入变化超过显式上限'):execute(changes,max_changes=1)
    low=run_case(tmp_path,changes=changes,max_changes=1)
    high=run_case(tmp_path,changes=changes,max_changes=10)
    assert low[:-1]==high[:-1]


def test_duplicate_tail_fails_without_completion_and_domains_are_independent(stage):
    first=change(0,23)
    tail=change(1,30)
    tail=replace(tail,reference=first.reference,after=replace(tail.after,observation_ref=first.reference))
    one=engine(stage,[first,tail]);two=engine(stage,[first])
    # 消费至两个域均保存同一原引用。
    while len(remaining(stage))<1:next(one)
    while len(remaining(stage))<2:next(two)
    seen=[]
    with pytest.raises(ValueError,match='变化原始引用重复'):
        for batch in one:seen.extend(batch.rows)
    assert not any(isinstance(r,Completion) for r in seen)
    assert len(remaining(stage))==1
    assert any(isinstance(r,Completion) for batch in list(two) for r in batch.rows)
    assert remaining(stage)==[]


def test_close_cancel_resource_and_single_row_preserve_other_domain(stage):
    other=stage.change_references();other.add('保留\x00原文')
    for primary in (KeyboardInterrupt('cancel'),ValueError('resource_limit:C2_working_set')):
        gen=engine(stage,[change(0,23),change(1,28)])
        while len(remaining(stage))<2:next(gen)
        def fail():raise primary
        stage.guard=fail
        with pytest.raises(type(primary)) as caught:list(gen)
        assert caught.value is primary
        stage.guard=lambda:None
        assert len(remaining(stage))==1 and '保留\x00原文' in other
    transient=stage.change_references();calls=[]
    primary=ValueError('resource_limit:C2_working_set')
    def after_write():
        calls.append(True)
        if len(calls)==2:raise primary
    stage.guard=after_write
    with pytest.raises(ValueError) as caught:transient.add('写入后超限')
    assert caught.value is primary
    transient.close();stage.guard=lambda:None
    assert len(remaining(stage))==1 and '保留\x00原文' in other
    gen=engine(stage,[change(0,23)])
    while len(remaining(stage))<2:next(gen)
    gen.close();assert len(remaining(stage))==1
    with pytest.raises(ValueError,match='resource_limit:C2_input_row'):other.add('x'*5000)
    other.add('1');other.add(1)
    assert '1' in other and 1 in other
    other.close();assert remaining(stage)==[]
