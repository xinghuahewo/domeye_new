"""S2显式依赖快照；在业务导入之前复用已有冻结入口。"""
from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[4]
GROUPS={
 'calculation_and_public_readers':('backend/data_pipeline/**/*.py','backend/utils/**/*.py'),
 'entry_and_locked_environment':('scripts/pipeline/country-trend-frozen-run.py','backend/pyproject.toml','backend/uv.lock','frontend/package-lock.json','config/data-profile.json','contracts/data/observation-run.schema.json'),
}
PACKAGES=('numpy','pandas','duckdb','pyarrow','psycopg2-binary','jsonschema','openpyxl')
API=runpy.run_path(str(ROOT/'backend/data_pipeline/common/frozen_execution.py'))
def code_identity(root=ROOT):return API['code_identity'](root,GROUPS,PACKAGES,schema='country-trend-code/v1')
def execution_identity():return API['execution_identity'](ROOT,code_identity)
