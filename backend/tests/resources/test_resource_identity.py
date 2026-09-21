"""依赖身份使用临时源副本变更，不修改本项目真实helper或锁。"""
from pathlib import Path

import pytest

from data_pipeline.analysis.resources.identity import ROOT, code_identity, identity_digest


def copied_identity_root(tmp_path):
    target=tmp_path/'identity-source'
    for relative in code_identity()['files']:
        dest=target/relative
        dest.parent.mkdir(parents=True,exist_ok=True)
        dest.write_bytes((ROOT/relative).read_bytes())
    return target


@pytest.mark.parametrize('relative', ['backend/utils/prefix_quantity.py', 'backend/uv.lock',
    'backend/data_pipeline/bgp/archive/store.py', 'backend/utils/__init__.py', 'config/data-profile.json'])
def test_calculation_dependency_change_changes_version(tmp_path, relative):
    root=copied_identity_root(tmp_path)
    before=identity_digest(code_identity(root))
    path=root/relative
    path.write_bytes(path.read_bytes()+b'\n# fixture changed dependency\n')
    assert identity_digest(code_identity(root))!=before


def test_real_warm_import_cannot_claim_disk_identity(tmp_path):
    """真实导入旧helper再改源文件：旧结果仍在，但正式入口在访问PG前拒绝。"""
    import subprocess
    import sys
    root=copied_identity_root(tmp_path)
    child='''
import sys
from pathlib import Path
sys.path.insert(0,sys.argv[1]+'/backend')
from data_pipeline.analysis.resources.compute import calculate_c_segments_count
from data_pipeline.analysis.resources.identity import code_identity
from data_pipeline.analysis.resources.produce import produce_resources
helper=Path(sys.argv[1])/'backend/utils/prefix_quantity.py'
helper.write_text(helper.read_text().replace('return len(unique_c_segments_ints)','return 2 * len(unique_c_segments_ints)'))
first=code_identity()
assert calculate_c_segments_count({'192.0.2.0/24'})==1
assert first==code_identity()
try:
    produce_resources('invalid', 'unused', [], 'unused',csv_source='unused',country_source='unused')
except ValueError as error:
    assert 'resource-frozen-run.py' in str(error)
else:
    raise AssertionError('warm进程被错误准入')
'''
    subprocess.run([sys.executable,'-B','-c',child,str(root)],check=True)
    cold="import sys;sys.path.insert(0,sys.argv[1]+'/backend');from utils.prefix_quantity import calculate_c_segments_count;assert calculate_c_segments_count({'192.0.2.0/24'})==2"
    subprocess.run([sys.executable,'-B','-c',cold,str(root)],check=True)
