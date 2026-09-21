"""共享字典和文件边界保存合同；不是人工 BGP 吞吐试验。"""
from contextlib import closing
from types import SimpleNamespace

import pyarrow as pa
import pytest

from data_pipeline.bgp.input.native_batches import HotPaths
from data_pipeline.bgp.state.path_dictionary import TextPool
from data_pipeline.bgp.state.checkpoint import StateStore
from data_pipeline.bgp.replay.route_replay import ReplayPlan
from data_pipeline.bgp.replay.quality_overlay import CanonicalReplay
from tests.observations.test_observation_checkpoint import dsn


@pytest.mark.parametrize('batch_rows',[1,2,8192])
def test_path_fields_survive_cache_eviction_and_repeated_registration(batch_rows):
    rows=[dict(path_key=f'{i:064x}',as_path_text='1 {2,3}' if i<2 else '',
               attributed_origin_asn=None if i<2 else 0,raw_origin_asn=4294967295,
               as4_path_text='4 5' if i==1 else None,
               attributes_digest=None if i==2 else f'{i+10:064x}',reason='as_set' if i<2 else '')
          for i in range(3)]
    with closing(HotPaths(cache_entries=1,cache_bytes=2048)) as paths:
        table=pa.Table.from_pylist(rows)
        for batch in table.to_batches(max_chunksize=batch_rows):paths.register(pa.Table.from_batches([batch]))
        paths.prepare([r['path_key'] for r in reversed(rows)])
        for row in rows:assert paths.prepare([row['path_key']])[row['path_key']]==row
        size=len(paths.rows);paths.register(table)
        assert len(paths.rows)==size and len(paths.texts.texts)==2
        assert paths.queries==paths.texts.queries==paths.texts.single_reads==paths.disk_peak==0
        assert len(paths.cache)<=1 and paths.bytes<=2048
        assert paths.prepare([rows[0]['path_key']])[rows[0]['path_key']]['as_path_text'] is paths.texts.value(paths.texts.ident('1 {2,3}'))
        with pytest.raises(ValueError,match='冲突'):
            paths.register(pa.Table.from_pylist([{**rows[0],'as4_path_text':'different'}]))
        with pytest.raises(ValueError,match='缺失'):paths.prepare(['f'*64])
        with pytest.raises(ValueError,match='内存上限'):paths.prepare([rows[0]['path_key']],max_result_bytes=1)


def test_text_id_collision_rejected_without_merging(monkeypatch):
    pool=TextPool();monkeypatch.setattr(pool,'ident',lambda text:7)
    pool.register(['first'])
    with pytest.raises(ValueError,match='碰撞'):pool.register(['second'])
    assert pool.value(7)=='first'


def reducer():
    plan=ReplayPlan('rrc25','rib',('update',),())
    return CanonicalReplay(plan,SimpleNamespace(binding_id='binding'),('rib','update'),
                           compact=True,detailed=False,legacy_views=False)


@pytest.mark.parametrize('stop',['files_ready','before_state_commit','after_state_commit'])
def test_binary_file_commit_rollback_and_restore(tmp_path,dsn,stop):
    """真私有 PG 验证二进制 COPY、同槽多次变化合并，以及提交前后恢复游标。"""
    binding=dict(profile='test',binding_id='binding')
    scope=('rrc25','192.0.2.1',1,None,None,None,1,1,'192.0.2.0/24',False,None)
    cp=lambda n:dict(ordinal=n,source_id=('rib','update')[n],digest=str(n)*64)
    with closing(StateStore(dsn,'memory-test',tmp_path,binding,audit_transitions=False)) as store:
        state=reducer();memory=state.replay.memory
        with store.commit_file(cp(0),state) as delta:
            for record in range(8200):
                memory.observe(scope,0,record,0,100,'a'*64,1,position=(0,record,0))
                delta.append('changes',{})
            assert memory.dirty is None and not delta.dirty
            delta.cursor.execute('SELECT COUNT(*) FROM route_file.packed_rows')
            assert delta.cursor.fetchone()[0]==0
        baseline=dict(memory.prefix_dict)
        def hook(label,ordinal):
            if label==stop:raise RuntimeError('中断')
        with pytest.raises(RuntimeError,match='中断'):
            with store.commit_file(cp(1),state,hook=hook) as delta:
                memory.observe(scope,1,1,0,101,'b'*64,2,position=(1,1,0))
                memory.observe(scope,1,2,0,102,None,None,position=(1,2,0))
                assert len(delta.dirty)==1
                delta.cursor.execute('SELECT payload FROM route_file.packed_rows')
                assert bytes(delta.cursor.fetchone()[0])==next(iter(baseline.values()))
        restored=reducer();store.restore(restored)
        if stop=='after_state_commit':
            assert store.status()[0]==1
            assert restored.replay.memory.prefix_dict==memory.prefix_dict
            assert restored.replay.current[scope]['presence']=='absent'
        else:
            assert store.status()[0]==0 and restored.replay.memory.prefix_dict==baseline
            with store.commit_file(cp(1),restored):
                restored.replay.memory.observe(scope,1,2,0,102,None,None,position=(1,2,0))
            assert restored.replay.memory.prefix_dict==memory.prefix_dict
        store.validate_files()
