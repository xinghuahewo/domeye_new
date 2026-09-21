"""路径增量查询的完整写入回归；仅使用自有人工PG和临时制品。"""
from contextlib import contextmanager
import uuid

import pytest

from tests.observations.test_observation_checkpoint import dsn, fixture, run, selected_rows
from tests.observations.test_observation_mrt import attr
from data_pipeline.bgp.archive import checkpoint as module
from data_pipeline.bgp.input.mrt_reader import attributes


@contextmanager
def path_store(tmp_path, dsn):
    root=tmp_path/'attempt';root.mkdir()
    owner=module.Owner(dsn,uuid.uuid4().hex)
    store=module.AttemptStore(dsn,'paths_fixture',str(tmp_path/'data'),root,owner,
                              module.Guard(root,tmp_path/'data',0,4*1024**3),20000)
    try:yield store
    finally:store.close();owner.close()


def path(number):
    return attributes(attr(100000+number),4)[1]


def test_path_batches_preserve_unique_bytes_and_skip_only_dictionary_duplicates(tmp_path,dsn):
    with path_store(tmp_path,dsn) as store:
        for i in range(1300):store.append('paths',path(i))
        store.flush()
        # 大字典中的单key查询覆盖索引分支，包括已存在和新增两种结果。
        store.append('paths',path(0));store.flush()
        store.append('paths',path(1300));store.flush()
        # 跨批次重复、新路径和批内重复一起进入真实flush。
        for i in [*range(600,1900),*range(1800,1900)]:store.append('paths',path(i))
        store.flush()
        for i in range(1900):store.append('paths',path(i))
        store.flush()  # 全部重复也必须正常提交并清空pending。
        expected=dict(messages=0,elements=0,references=0,decoded=0,rejected=0,unsupported=0)
        checked=store.checkpoint_data(expected)
        assert checked['tables']['paths']['count']==1900
        actual=store.db.execute(f'SELECT path_key,attributes_raw,as_path_text FROM lake.{store.schema}.paths').fetchall()
        assert set(actual)=={(p['path_key'],p['attributes_raw'],p['as_path_text']) for p in map(path,range(1900))}
        assert store.db.execute('SELECT count(*) FROM path_keys').fetchone()[0]==1900
        assert store.path_count==1900
        assert not any(store.pending.values())


@pytest.mark.parametrize('across_batches',[False,True])
@pytest.mark.parametrize('damage',['typed','raw_bytes','asn_width'])
def test_path_conflicts_and_raw_corruption_rejected(tmp_path,dsn,across_batches,damage):
    with path_store(tmp_path,dsn) as store:
        original=path(1);store.append('paths',original)
        if across_batches:
            for i in range(2,302):store.append('paths',path(i))
            store.flush()
        changed=dict(original)
        if damage=='typed':changed['as_path_text']='不同解释'
        elif damage=='raw_bytes':changed['attributes_raw']+=b'\x00'
        else:changed['asn_width']=2
        store.append('paths',changed)
        with pytest.raises(ValueError,match='不同typed|原bytes'):store.flush()


@pytest.mark.parametrize('damage',['raw_bytes','asn_width'])
def test_new_path_key_must_match_original_bytes(tmp_path,dsn,damage):
    with path_store(tmp_path,dsn) as store:
        changed=path(1)
        if damage=='raw_bytes':changed['attributes_raw']+=b'\x00'
        else:changed['asn_width']=2
        store.append('paths',changed)
        with pytest.raises(ValueError,match='原bytes'):store.flush()


def test_failure_after_lake_commit_before_local_index_replays_unselected_file(tmp_path,dsn,monkeypatch):
    manifest=fixture(tmp_path/'input');data=tmp_path/'data'
    baseline=run(manifest,dsn,tmp_path/'baseline',catalog_data_path=data)
    connect=module.connect_duckdb;injected=[]
    class Connection:
        def __init__(self,db):self.db=db
        def __getattr__(self,name):return getattr(self.db,name)
        def execute(self,query,*args,**kwargs):
            if query.startswith('INSERT INTO path_keys') and not injected:
                injected.append(True)
                raise RuntimeError('模拟lake已提交但本地索引尚未写入')
            return self.db.execute(query,*args,**kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(module,'connect_duckdb',lambda *a,**kw:Connection(connect(*a,**kw)))
        with pytest.raises(RuntimeError,match='模拟lake'):
            run(manifest,dsn,tmp_path/'resume',catalog_data_path=data)
    resumed=run(manifest,dsn,tmp_path/'resume',catalog_data_path=data)
    assert injected and selected_rows(dsn,baseline)==selected_rows(dsn,resumed)
    assert [(c['counts'],{k:(v['count'],v['digest']) for k,v in c['tables'].items()}) for c in baseline['checkpoints']]==[
        (c['counts'],{k:(v['count'],v['digest']) for k,v in c['tables'].items()}) for c in resumed['checkpoints']]
