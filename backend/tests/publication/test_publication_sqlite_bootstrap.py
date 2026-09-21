"""真实新解释器验证CLI导入/绑定顺序，只使用临时目录。"""
import json
import subprocess
import sys
from pathlib import Path
import pytest

@pytest.mark.parametrize('mode',['preflight','inspect','execute','failure'])
def test_retry_bootstrap_and_own_cleanup(tmp_path,mode):
    script=Path(__file__).resolve().parents[3]/'scripts/pipeline/publication-retry.py'
    spec=tmp_path/'spec.json';spec.write_text(json.dumps(dict(enabled=True,attempt_root=str(tmp_path/'attempt'))))
    code='''
import runpy,sys,json
from pathlib import Path
script,spec,mode=sys.argv[1:]
module=runpy.run_path(script)
assert 'sqlite3' not in sys.modules
roots=[]
def run(value,args):
    from data_pipeline.common import process_resources as resources
    if mode in ('execute','failure'):
        root=resources.sqlite_temp();roots.append(root)
        import sqlite3
        db=sqlite3.connect(root/'fixture.db');db.execute('CREATE TABLE x(v)');db.close()
        if mode=='failure':raise ValueError('primary fixture')
    else:
        assert resources._binding is None
        import sqlite3
module['main'].__globals__['run']=run
sys.argv=[script,spec]+(['--execute'] if mode in ('execute','failure') else ['--inspect-saved'] if mode=='inspect' else [])
try:module['main']()
except ValueError as error:
    assert mode=='failure' and str(error)=='primary fixture'
assert all(not root.exists() for root in roots)
assert sorted(p.name for p in Path(spec).parent.iterdir())==['spec.json']
print('passed')
'''
    result=subprocess.run([sys.executable,'-B','-c',code,str(script),str(spec),mode],capture_output=True,text=True,timeout=15)
    assert result.returncode==0,result.stderr
