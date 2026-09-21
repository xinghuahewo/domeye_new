"""Feature有限依赖清单；可在业务包导入前以runpy执行。"""
from pathlib import Path
import runpy

ROOT=Path(__file__).resolve().parents[4]
GROUPS={
    'package_initializers': ('backend/data_pipeline/analysis/__init__.py', 'backend/data_pipeline/bgp/__init__.py', 'backend/data_pipeline/bgp/input/__init__.py', 'backend/data_pipeline/bgp/archive/__init__.py', 'backend/data_pipeline/bgp/state/__init__.py', 'backend/data_pipeline/bgp/replay/__init__.py', 'backend/data_pipeline/bgp/snapshots/__init__.py', 'backend/data_pipeline/common/__init__.py'),
    'shared_utilities': ('backend/data_pipeline/common/*.py',),
    'feature_implementation':('backend/data_pipeline/analysis/features/*.py','scripts/pipeline/feature-frozen-run.py'),
    'shared_reading_and_identity':('backend/data_pipeline/bgp/**/*.py','backend/data_pipeline/common/frozen_execution.py',
        'backend/data_pipeline/__init__.py','backend/data_pipeline/bgp/snapshots/origin.py', 'backend/data_pipeline/bgp/snapshots/__init__.py','backend/data_pipeline/common/prefix_networks.py'),
    'quantity_helpers':('backend/utils/prefix_quantity.py','backend/utils/__init__.py'),
    'locked_environment_and_contract':('backend/pyproject.toml','backend/uv.lock',
        'config/data-profile.json','contracts/data/observation-run.schema.json'),
}
PACKAGES=('numpy','pandas','duckdb','pyarrow','psycopg2-binary','jsonschema','openpyxl')
API=runpy.run_path(str(ROOT/'backend/data_pipeline/common/frozen_execution.py'))


def code_identity(root=ROOT):
    return API['code_identity'](root,GROUPS,PACKAGES,schema='feature-code-identity/v1')


def execution_identity(*,fixture_only=False):
    return API['execution_identity'](ROOT,code_identity,fixture_only=fixture_only)
