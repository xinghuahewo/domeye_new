"""只执行原提交字节门禁，临时Git库；不执行admit或科学验证。"""
import ast
from pathlib import Path
import subprocess
import pytest

ROOT=Path(__file__).resolve().parents[3]
OWNERS=('bgp/replay/snapshot_admission.py','analysis/country_events/result_admission.py','analysis/detection/publication.py')


@pytest.mark.parametrize('owner',OWNERS)
def test_uncommitted_shared_helper_rejected(tmp_path,owner):
    source=ROOT/'backend/data_pipeline'/owner
    tree=ast.parse(source.read_text())
    if owner.startswith('bgp/'):
        nodes=[n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='CODE_FILES' for t in n.targets)]
        nodes += [n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_revision']
    else:
        admit=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='admit')
        nodes=[n for n in admit.body if isinstance(n,ast.For) and 'committed' in ast.unparse(n)]
        assert len(nodes)==1
    local=tmp_path/'backend/data_pipeline'/owner
    local.parent.mkdir(parents=True)
    # 原门禁所覆盖的文件名，内容仅用于真实git字节差异。
    for original in source.parent.glob('*.py'):(local.parent/original.name).write_text('# fixture\n')
    if owner.startswith('bgp/'):
        for relative in ast.literal_eval(nodes[0].value):
            target=tmp_path/'backend/data_pipeline'/relative
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text('# fixture\n')
    helper=tmp_path/'backend/data_pipeline/common/admission_locks.py'
    helper.parent.mkdir(parents=True,exist_ok=True);helper.write_text('# committed helper\n')
    def git(*args):return subprocess.check_output(['git',*args],cwd=tmp_path,stderr=subprocess.DEVNULL)
    git('init','-q');git('add','.');git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','fixture')
    revision=git('rev-parse','HEAD').decode().strip()
    namespace=dict(Path=Path,subprocess=subprocess,ROOT=tmp_path,__file__=str(local),revision=revision)
    compiled=compile(ast.fix_missing_locations(ast.Module(body=nodes,type_ignores=[])),str(source),'exec')
    def check():
        exec(compiled,namespace)
        if owner.startswith('bgp/'):namespace['_revision']()
    check()
    helper.write_text('# uncommitted changed helper\n')
    with pytest.raises(ValueError,match='固定.*提交'):check()
    helper.write_text('# committed helper\n')
    git('rm','--cached',str(helper.relative_to(tmp_path)))
    git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','missing helper')
    namespace['revision']=git('rev-parse','HEAD').decode().strip()
    with pytest.raises(ValueError,match='固定.*提交'):check()
