"""干净冻结解释器中的共享读取身份；不依赖pytest父进程预热模块。"""
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT=Path(__file__).resolve().parents[3]


@pytest.mark.parametrize('mode',['normal','source_drift','module_origin_drift'])
def test_fresh_frozen_read_identity(tmp_path,mode):
    import runpy
    identity=runpy.run_path(str(ROOT/'backend/data_pipeline/analysis/features/identity.py'))['code_identity']()
    root=tmp_path/'project'
    for relative in identity['files']:
        target=root/relative;target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(ROOT/relative,target)
    entry=root/'scripts/read-identity.py'
    entry.write_text('''import json,runpy,sys,argparse
from pathlib import Path
root=Path(__file__).resolve().parents[1]
api=runpy.run_path(str(root/'backend/data_pipeline/common/frozen_execution.py'))
spec=runpy.run_path(str(root/'backend/data_pipeline/analysis/features/identity.py'))
groups={**spec['GROUPS'],'test_entry':('scripts/read-identity.py',)}
build=lambda r:api['code_identity'](r,groups,spec['PACKAGES'],schema='read-identity-test/v1')
p=argparse.ArgumentParser();p.add_argument('request');p.add_argument('--frozen-child',action='store_true');p.add_argument('--expected-digest');a=p.parse_args()
if api['enter'](root,'scripts/read-identity.py',a.request,build,child=a.frozen_child,expected_digest=a.expected_digest):
    sys.path.insert(0,str(root/'backend'))
    from data_pipeline.bgp.archive import message_reader as consumer, store
    from data_pipeline.analysis.features import run as feature_run
    request=json.loads(Path(a.request).read_text())
    before=api['execution_identity'](root,build)
    reader=object.__new__(consumer.ObservationReader);reader.selection=None;reader.schema='fixture';reader.snapshot=1
    relation=reader.bound_table('messages')
    assert relation==store.bound_table('fixture',1,'messages')
    if request['mode']=='source_drift':
        file=root/'backend/data_pipeline/bgp/archive/store.py';file.chmod(0o644);file.write_text(file.read_text()+'\\n# 人工源码漂移\\n')
    if request['mode']=='module_origin_drift':consumer.__file__='/tmp/foreign-observation-consumer.py'
    after=api['execution_identity'](root,build)
    assert before==after,'完成前代码身份漂移'
    Path(request['output']).write_text(json.dumps({'identity':after,'relation':relation}))
''')
    output=tmp_path/'result.json';request=tmp_path/'request.json'
    request.write_text(json.dumps(dict(mode=mode,output=str(output))))
    result=subprocess.run([sys.executable,'-I','-B',str(entry),str(request)],capture_output=True,text=True)
    (tmp_path/'子进程日志.txt').write_text(result.stdout+result.stderr)
    if mode=='normal':
        assert result.returncode==0,result.stderr
        report=json.loads(output.read_text())
        assert report['identity']['execution_binding']['module_sources']['data_pipeline.bgp.archive.message_reader']=='backend/data_pipeline/bgp/archive/message_reader.py'
    else:
        assert result.returncode!=0
        assert ('冻结新进程' if mode=='source_drift' else '快照外') in result.stderr
        assert not output.exists()


@pytest.mark.parametrize('first',['store','selection','message_reader'])
def test_reading_import_order(first):
    result=subprocess.run([sys.executable,'-I','-B','-c',
        "import sys;sys.path.insert(0,"+repr(str(ROOT/'backend'))+");"
        "from data_pipeline.bgp.archive import "+first+";"
        "from data_pipeline.bgp.archive import store,selection,message_reader;"
        "assert selection.legacy_table(schema='fixture',snapshot=1,name='messages')==store.bound_table('fixture',1,'messages')"],
        capture_output=True,text=True)
    assert result.returncode==0,result.stderr
