"""显式文件快照→实际新解释器→来源核验；不导入业务后补身份。"""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import pytest
from data_pipeline.common.frozen_execution import code_identity, enter, execution_identity

ROOT=Path(__file__).resolve().parents[3]


def tiny_project(root):
    (root/'backend/data_pipeline/common').mkdir(parents=True)
    (root/'scripts').mkdir()
    shutil.copy(ROOT/'data_pipeline/common/frozen_execution.py' if (ROOT/'data_pipeline/common/frozen_execution.py').exists() else ROOT/'backend/data_pipeline/common/frozen_execution.py',root/'backend/data_pipeline/common/frozen_execution.py')
    (root/'backend/data_pipeline/__init__.py').write_text('')
    (root/'backend/data_pipeline/common/__init__.py').write_text('')
    (root/'backend/data_pipeline/business.py').write_text('VALUE=17\n')
    (root/'scripts/run.py').write_text('''import argparse,json,runpy,sys
from pathlib import Path
root=Path(__file__).resolve().parents[1]
api=runpy.run_path(str(root/'backend/data_pipeline/common/frozen_execution.py'))
build=lambda r:api['code_identity'](r,{'implementation':('backend/data_pipeline/**/*.py','scripts/run.py')},(),schema='fixture/v1')
p=argparse.ArgumentParser();p.add_argument('request');p.add_argument('--frozen-child',action='store_true');p.add_argument('--expected-digest');a=p.parse_args()
if api['enter'](root,'scripts/run.py',a.request,build,child=a.frozen_child,expected_digest=a.expected_digest):
    sys.path.insert(0,str(root/'backend'))
    from data_pipeline import business
    from data_pipeline.common.frozen_execution import execution_identity
    request=json.loads(Path(a.request).read_text())
    if request.get('mutate'):
        file=root/'backend/data_pipeline/business.py';file.chmod(0o644);file.write_text('VALUE=99')
    result=execution_identity(root,build)
    Path(request['output']).write_text(json.dumps({'value':business.VALUE,'identity':result}))
''')


def test_real_child_origin_pid_and_mutation_rejected(tmp_path):
    root=tmp_path/'project';tiny_project(root)
    output=tmp_path/'out.json';request=tmp_path/'request.json'
    request.write_text(json.dumps({'output':str(output)}))
    subprocess.run([sys.executable,str(root/'scripts/run.py'),str(request)],check=True)
    report=json.loads(output.read_text())
    assert report['value']==17
    identity=report['identity'];assert identity['execution_mode']=='frozen-fresh-process'
    assert identity['execution_binding']['snapshot_root']!=str(root)
    assert identity['execution_binding']['module_sources']['data_pipeline.business']=='backend/data_pipeline/business.py'
    output.unlink();request.write_text(json.dumps({'output':str(output),'mutate':True}))
    result=subprocess.run([sys.executable,str(root/'scripts/run.py'),str(request)],capture_output=True,text=True)
    assert result.returncode!=0 and '冻结新进程' in result.stderr
    assert not output.exists()


def test_direct_api_and_missing_dependencies_rejected(tmp_path):
    (tmp_path/'a.py').write_text('x=1')
    build=lambda r:code_identity(r,{'impl':('a.py',)},(),schema='fixture/v1')
    assert execution_identity(tmp_path,build,fixture_only=True)['execution_mode']=='synthetic-fixture-api'
    with pytest.raises(ValueError,match='冻结新进程'):execution_identity(tmp_path,build)
    with pytest.raises(ValueError,match='依赖缺失'):code_identity(tmp_path,{'x':('absent.py',)},(),schema='fixture/v1')
