"""P1 原路径别名与固定物理实体；仅人工文件及显式自有 PG。"""
import copy
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from data_pipeline.bgp.archive import admission as p
from data_pipeline.bgp.archive.value_codec import ENTITY_FIELDS, M2_ENTITY_FIELDS, fields, typed, untyped, CODEC
from tests.observations.test_observation_checkpoint import dsn, fixture, run


def runtime(root):
    scratch=root/'scratch'
    scratch.mkdir(exist_ok=True)
    return p.Runtime('',(root,),scratch,fixture_only=True)


def test_actual_tmp_path_and_entity_current():
    with TemporaryDirectory(prefix='p1-path-903b-',dir='/tmp') as name:
        root=Path(name);source=root/'as.csv';source.write_text('asn,name\n1,test\n')
        rt=runtime(root)
        entity=p._entity(rt,source,hash_body=True,guard=lambda:None,preserve_source_path=True)
        assert entity['source_path']==str(source)
        assert entity['path']==str(source.resolve())
        assert entity['sha256']==hashlib.sha256(source.read_bytes()).hexdigest()
        p._entities_current(runtime(root),[entity],lambda:None)


@pytest.mark.parametrize('parent_alias',[False,True])
def test_alias_repoint_rejected_in_same_and_fresh_runtime(tmp_path,parent_alias):
    root=tmp_path.resolve();left=root/'left';right=root/'right'
    left.mkdir();right.mkdir()
    for directory in (left,right):(directory/'as.csv').write_text('same bytes')
    alias=root/'alias';alias.symlink_to(left if parent_alias else left/'as.csv')
    source=alias/'as.csv' if parent_alias else alias
    rt=runtime(root);entity=p._entity(rt,source,hash_body=True,guard=lambda:None,preserve_source_path=True)
    alias.unlink();alias.symlink_to(right if parent_alias else right/'as.csv')
    with pytest.raises(ValueError,match='漂移'):rt.path(source)
    with pytest.raises(ValueError,match='漂移'):p._entities_current(runtime(root),[entity],lambda:None)


def test_outside_relative_and_allowed_root_repoint(tmp_path):
    root=tmp_path.resolve();inside=root/'inside';outside=root/'outside'
    inside.mkdir();outside.mkdir();(outside/'as.csv').write_text('outside')
    rt=runtime(inside);alias=inside/'alias';alias.symlink_to(outside/'as.csv')
    with pytest.raises(ValueError,match='允许根'):rt.path(alias)
    with pytest.raises(ValueError,match='绝对路径'):rt.path(Path('.'))
    root_alias=root/'root-alias';root_alias.symlink_to(inside)
    rt=runtime(root_alias);root_alias.unlink();root_alias.symlink_to(outside)
    with pytest.raises(ValueError,match='漂移'):rt.path(inside/'scratch')


def test_canonical_entity_replacement_and_modification(tmp_path):
    root=tmp_path.resolve();source=root/'as.csv';source.write_text('original')
    rt=runtime(root);entity=p._entity(rt,source,hash_body=True,guard=lambda:None,preserve_source_path=True)
    p._entities_current(rt,[entity],lambda:None)
    source.write_text('changed')
    with pytest.raises(ValueError,match='漂移'):p._entities_current(rt,[entity],lambda:None)
    source.unlink();source.write_text('original')
    with pytest.raises(ValueError,match='漂移'):p._entities_current(runtime(root),[entity],lambda:None)


def test_shared_seven_field_helper_contract(tmp_path):
    root=tmp_path.resolve();source=root/'data';source.write_text('shared')
    rt=runtime(root)
    legacy=p._entity(rt,source,hash_body=True,guard=lambda:None)
    fields(legacy,ENTITY_FIELDS)
    assert len(ENTITY_FIELDS)==7
    p._entities_current(rt,[legacy],lambda:None)
    extended=p._entity(rt,source,hash_body=True,guard=lambda:None,preserve_source_path=True)
    fields(extended,M2_ENTITY_FIELDS)
    p._entities_current(rt,[extended],lambda:None)
    alias=root/'alias';alias.symlink_to(source)
    with pytest.raises(ValueError,match='旧实体合同'):p._entity(rt,alias,guard=lambda:None)


def test_borrowed_path_keeps_stateless_strict_contract(tmp_path):
    # Resource/Detection原类直接借用方法，没有M2的构造状态或辅助方法。
    class Borrower:
        path=p.Runtime.path
    root=tmp_path.resolve();inside=root/'inside';inside.mkdir()
    rt=Borrower();rt.allowed_roots=(inside,)
    source=inside/'source';source.write_text('fixture')
    assert rt.path(source)==source
    assert vars(rt)=={'allowed_roots':(inside,)}
    alias=inside/'alias';alias.symlink_to(source)
    with pytest.raises(ValueError):rt.path(alias)
    with pytest.raises(ValueError):rt.path(root)
    with pytest.raises(ValueError):rt.path(Path('.'))


def test_tmp_csv_sealed_m2_admit_current_read_and_tail_repoint(dsn):
    # 一次人工M2；后续准入/读取不得重产或改写原manifest。
    with TemporaryDirectory(prefix='p1-path-m2-903b-',dir='/tmp') as name:
        root=Path(name);m=fixture(root/'inputs',True)
        csv=root/'inputs'/'as.csv';csv.write_text('asn,name\n64496,fixture\n')
        m['references']=[dict(path=str(csv),sha256=hashlib.sha256(csv.read_bytes()).hexdigest())]
        seal=run(m,dsn,root/'m2',policy='isolate-payload/v1')
        scratch=root/'scratch';scratch.mkdir()
        rt=p.Runtime(dsn,(root,),scratch,fixture_only=True)
        b=p.inspect_binding(rt,seal['run_id'],seal['snapshot'],[e['source_id'] for e in m['inputs']])
        original=copy.deepcopy(b)
        files={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in root.rglob('*') if f.is_file()}
        rt=p.Runtime(dsn,(root,),scratch,execution_profile='real-candidate/v1',
            input_manifest=b['plan']['manifest'],expected_m2_binding=b,memory_limit='256MB',
            max_temp_bytes=512*1024**2,lock_timeout_ms=2000,max_rss_bytes=4*1024**3,min_free_bytes=128*1024**2)
        a=p.admit(rt,b,guard=lambda:None);rt.dependency_admissions=(a,)
        reference=p.admit(rt,p.reference_binding(rt,a,b['reference_sources'][0]['source_id']),guard=lambda:None)
        assert next(e for e in a['entities'] if e['source_path']==str(csv))['path']==str(csv.resolve())
        assert p.admit(rt,b,guard=lambda:None)==a
        for admission,view in [(a,'messages'),(reference,'references')]:
            p.verify_current(rt,admission,guard=lambda:None)
            with p.hold_lock(rt,admission,p._own_target(admission),guard=lambda:None):pass
            raw=untyped(admission['owner_binding'])
            sources=raw['ordered_source_ids'] if view=='messages' else [raw['source_id']]
            request=dict(view=view,scope_typed=typed(dict(source_ids=sources)),codec_version=CODEC,batch_rows=1,batch_bytes=1024**2)
            with p.open_reader(rt,admission,request,guard=lambda:None) as session:
                rows=[row for batch in session for row in untyped(batch['rows_typed'])]
            assert rows and session.receipt['execution']=='complete'
        assert b==original
        assert files=={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in root.rglob('*') if f.is_file()}
        # EOF后重指向：尾current必须拒绝，不能留下成功Receipt。
        with pytest.raises(ValueError,match='漂移'):
            with p.open_reader(rt,reference,request,guard=lambda:None) as session:
                list(session)
                replacement=root/'replacement.csv';replacement.write_bytes(csv.read_bytes())
                csv.unlink();csv.symlink_to(replacement)
        assert session.receipt is None
