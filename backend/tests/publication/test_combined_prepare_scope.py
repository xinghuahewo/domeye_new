"""仅路径隔离与早期拒绝；不造AD/current/ready成功。"""
from types import SimpleNamespace
from pathlib import Path
import pytest
from data_pipeline.results.build_manifest import WriteScope, prepare
from data_pipeline.results.manifest_io import Limits
from data_pipeline.results.manifest_contract import CONTRACT, PROFILES


def request(root):
    return dict(components=[dict(owner='scope-only',physical=dict(root=str(root)),entities=[])],dependencies=[])


def test_disjoint_scope_and_input_overlap(tmp_path):
    source=tmp_path/'source';source.mkdir()
    output=tmp_path/'publication';output.mkdir()
    p=SimpleNamespace(root=output,combined_input=None)
    WriteScope(p,request(source),output/'q1-builds'/'one').check()
    p.root=source
    with pytest.raises(ValueError,match='交叠'):WriteScope(p,request(source),source/'q1-builds'/'one')


def test_output_parent_symlink_rejected(tmp_path):
    source=tmp_path/'source';source.mkdir()
    output=tmp_path/'publication';output.mkdir();(output/'q1-builds').symlink_to(source,target_is_directory=True)
    with pytest.raises(ValueError,match='符号链接'):
        WriteScope(SimpleNamespace(root=output,combined_input=None),request(source),output/'q1-builds'/'one')


def test_source_alias_drift_rejected(tmp_path):
    source=tmp_path/'source';source.mkdir();other=tmp_path/'other';other.mkdir()
    alias=tmp_path/'alias';alias.symlink_to(source,target_is_directory=True)
    output=tmp_path/'publication';output.mkdir()
    scope=WriteScope(SimpleNamespace(root=output,combined_input=None),request(alias),output/'q1-builds'/'one')
    alias.unlink();alias.symlink_to(other,target_is_directory=True)
    with pytest.raises(ValueError,match='别名漂移'):scope.check()


def test_incomplete_full_profile_rejected_before_control_access(tmp_path):
    p=SimpleNamespace(root=tmp_path,limits=Limits())
    q=dict(contract=CONTRACT,profile=PROFILES['fixture-m3-combined-country-trend/v1'],
           dependency_revisions={},components=[],dependencies=[],edges=[],role_graph=[])
    with pytest.raises(ValueError,match='主体缺失'):prepare(p,q,max_rows=1,max_bytes=100)


@pytest.mark.parametrize('replaced',['output','parent'])
def test_created_write_directory_replacement_rejected(tmp_path,replaced):
    source=tmp_path/'source';source.mkdir()
    root=tmp_path/'publication';root.mkdir()
    output=root/'q1-builds'/'one'
    scope=WriteScope(SimpleNamespace(root=root,combined_input=None),request(source),output)
    output.mkdir(parents=True)
    # candidate创建后固定同一scope，prepare不得重新接受新目录身份。
    scope.bind_created()
    victim=output if replaced=='output' else output.parent
    victim.rename(root/'retained-original')
    output.mkdir(parents=True)
    with pytest.raises(ValueError,match='写目录身份'):scope.check()


def test_write_directory_identity_cannot_be_rebased(tmp_path):
    source=tmp_path/'source';source.mkdir()
    root=tmp_path/'publication';root.mkdir();output=root/'q1-builds'/'one'
    scope=WriteScope(SimpleNamespace(root=root,combined_input=None),request(source),output)
    output.mkdir(parents=True);scope.bind_created()
    with pytest.raises(ValueError,match='不得重绑定'):scope.bind_created()
