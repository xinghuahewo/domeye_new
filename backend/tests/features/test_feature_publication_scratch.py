"""Feature真实scratch普通替换/重指向增量；不测试管理员并发TOCTOU对抗。"""
import json
from pathlib import Path
import shutil
import tempfile

import pytest

from tests.features.test_feature_publication_real import case, runtime
from data_pipeline.analysis.features import publication as p, publication_io as io
from data_pipeline.bgp.archive.value_codec import typed


@pytest.fixture(scope='module')
def admission(case):
    c, root, _, _ = case
    a = p.admit(runtime(case), c['binding'], guard=lambda:None)
    (root/'feature-scratch-repair/new-admission.json').write_text(json.dumps(a,ensure_ascii=False,indent=2))
    return a


def request():
    return dict(view='coverage',scope_typed=typed(dict(mode='all',window_role='all')),codec_version=io.CODEC,batch_rows=1,batch_bytes=128*1024)


@pytest.mark.parametrize('kind', ['symlink','directory'])
@pytest.mark.parametrize('when', ['before_open','first_batch','exhausted'])
def test_replaced_scratch_rejected(case, admission, kind, when):
    rt=runtime(case);scratch=rt.scratch_root;held=scratch.with_name(scratch.name+'-test-held')
    outside=Path(tempfile.mkdtemp(prefix='feature-scratch-test-')).resolve()
    assert not any(outside==r or r in outside.parents for r in rt.allowed_roots)
    changed=False;session=None
    original_children=set(scratch.iterdir())
    def replace():
        nonlocal changed
        scratch.rename(held);changed=True
        if kind=='symlink':scratch.symlink_to(outside,target_is_directory=True)
        else:scratch.mkdir()
    try:
        with pytest.raises(ValueError,match='scratch'):
            if when=='before_open':replace()
            with p.open_reader(rt,admission,request(),guard=lambda:None) as session:
                if when=='first_batch':next(session);replace();list(session)
                elif when=='exhausted':list(session);replace()
                else:list(session)
        assert session is None or session.receipt is None
        assert list(outside.iterdir())==[]
        if kind=='directory':assert list(scratch.iterdir())==[]
    finally:
        if changed:
            if scratch.is_symlink():scratch.unlink()
            else:scratch.rmdir()
            held.rename(scratch)
        # 重命名期间原路径不可达，可能遗留自身临时目录；恢复后只清理本例新增目录。
        for path in set(scratch.iterdir())-original_children:shutil.rmtree(path)
        shutil.rmtree(outside)
    p.verify_current(rt,admission,guard=lambda:None)


def test_recheck_before_database_connection(case, admission, monkeypatch):
    rt=runtime(case);scratch=rt.scratch_root;held=scratch.with_name(scratch.name+'-connect-held')
    outside=Path(tempfile.mkdtemp(prefix='feature-scratch-connect-')).resolve()
    factory=io.tempfile.TemporaryDirectory;connect=io.connect_duckdb;temporary=[];connections=[]
    def change_after_directory(*args,**kwargs):
        result=factory(*args,**kwargs)
        if Path(kwargs.get('dir',''))==scratch:
            temporary.append(result);scratch.rename(held);scratch.symlink_to(outside,target_is_directory=True)
        return result
    def observed(*args,**kwargs):connections.append(args);return connect(*args,**kwargs)
    monkeypatch.setattr(io.tempfile,'TemporaryDirectory',change_after_directory)
    monkeypatch.setattr(io,'connect_duckdb',observed)
    try:
        with pytest.raises(ValueError,match='scratch'):
            with p.open_reader(rt,admission,request(),guard=lambda:None) as session:list(session)
        assert connections==[] and list(outside.iterdir())==[] and session.receipt is None
    finally:
        if scratch.is_symlink():scratch.unlink();held.rename(scratch)
        for item in temporary:item.cleanup()
        shutil.rmtree(outside)


def test_legal_scratch_and_mode_stay_valid(case, admission):
    rt=runtime(case)
    for _ in range(2):
        with p.open_reader(rt,admission,request(),guard=lambda:None) as session:
            count=sum(batch['rows'] for batch in session)
        assert session.receipt and count==36
    rt.execution_profile='unknown'
    with pytest.raises(ValueError,match='模式漂移'):p.verify_current(rt,admission,guard=lambda:None)
