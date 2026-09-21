"""Resource显式依赖清单；修改导入/规则时在此审查，不自动猜测依赖图。"""
import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import platform

ROOT = Path(__file__).resolve().parents[4]
# Resource自身全部实现；共享观察Interface及其项目内传递依赖；数值helper与锁定依据。
GROUPS={
    'package_initializers': ('backend/data_pipeline/analysis/__init__.py', 'backend/data_pipeline/bgp/__init__.py', 'backend/data_pipeline/bgp/input/__init__.py', 'backend/data_pipeline/bgp/archive/__init__.py', 'backend/data_pipeline/bgp/state/__init__.py', 'backend/data_pipeline/bgp/replay/__init__.py', 'backend/data_pipeline/bgp/snapshots/__init__.py', 'backend/data_pipeline/common/__init__.py'),
    'shared_utilities': ('backend/data_pipeline/common/*.py',),
    'resource_implementation': ('backend/data_pipeline/analysis/resources/*.py', 'scripts/pipeline/resource-frozen-run.py'),
    'shared_observation_interface': ('backend/data_pipeline/bgp/**/*.py',
                                     'backend/data_pipeline/bgp/snapshots/origin.py', 'backend/data_pipeline/bgp/snapshots/__init__.py', 'backend/data_pipeline/__init__.py'),
    'quantity_calculation': ('backend/utils/prefix_quantity.py', 'backend/utils/__init__.py'),
    'locked_environment_and_contract': ('backend/pyproject.toml', 'backend/uv.lock',
        'config/data-profile.json', 'contracts/data/observation-run.schema.json'),
}
PACKAGES = ('numpy', 'pandas', 'duckdb', 'pyarrow', 'psycopg2-binary', 'jsonschema', 'openpyxl')


def code_identity(root=ROOT):
    root = Path(root)
    files = {}
    for group, patterns in GROUPS.items():
        for pattern in patterns:
            paths = sorted(root.glob(pattern))
            if not paths:
                raise ValueError('版本依赖缺失：' + pattern)
            for path in paths:
                files[path.relative_to(root).as_posix()] = {
                    'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'group': group}
    return {'schema_version': 'resource-code-identity/v1', 'files': files,
            'python': platform.python_version(),
            'packages': {package: version(package) for package in PACKAGES}}


def identity_digest(identity):
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def execution_identity(*, fixture_only=False):
    """正式结果必须来自业务导入前绑定的新进程；直接API只能声明合成fixture。"""
    import os
    import sys
    current=code_identity()
    if fixture_only:
        return {**current, 'execution_mode':'synthetic-fixture-api'}
    entry=sys.modules.get('_resource_frozen_entry')
    if (entry is None or entry.root!=ROOT or entry.identity!=current
            or entry.pid!=os.getpid() or not sys.flags.isolated or not sys.dont_write_bytecode):
        raise ValueError('正式Resource完成必须通过resource-frozen-run.py新进程入口')
    project_module_sources(current)
    return {**current, 'execution_mode':'frozen-fresh-process',
            'execution_binding':{'pid':entry.pid,'snapshot_root':str(ROOT),
                                 'snapshot_digest':identity_digest(current)}}


def project_module_sources(identity):
    """仅核对显式项目命名空间的真实来源，不发现新的依赖图。"""
    import sys
    sources={}
    for name,module in tuple(sys.modules.items()):
        if not (name=='data_pipeline' or name.startswith('data_pipeline.') or name=='utils' or name.startswith('utils.')):
            continue
        path=Path(getattr(module,'__file__','')).resolve()
        try:
            relative=path.relative_to(ROOT).as_posix()
        except ValueError:
            raise ValueError('项目模块来自代码快照外：'+name) from None
        if relative not in identity['files'] or path.suffix!='.py':
            raise ValueError('项目模块不在显式源码清单：'+name)
        sources[name]=relative
    return sources
