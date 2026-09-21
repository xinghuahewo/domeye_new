"""Detection有限源码分组；由入口runpy读取，业务导入前仅依赖标准库。"""

from pathlib import Path
import runpy


def identity_builder(root):
    root = Path(root)
    shared = runpy.run_path(str(root / "backend/data_pipeline/common/frozen_execution.py"))
    return shared["code_identity"](
        root,
        {
            "package_initializers": ('backend/data_pipeline/analysis/__init__.py', 'backend/data_pipeline/bgp/__init__.py', 'backend/data_pipeline/bgp/input/__init__.py', 'backend/data_pipeline/bgp/archive/__init__.py', 'backend/data_pipeline/bgp/state/__init__.py', 'backend/data_pipeline/bgp/replay/__init__.py', 'backend/data_pipeline/bgp/snapshots/__init__.py', 'backend/data_pipeline/common/__init__.py'),
            "detection": ["backend/data_pipeline/analysis/detection/*.py"],
            "shared": [
                "backend/data_pipeline/__init__.py",
                "backend/data_pipeline/common/frozen_execution.py",
                "backend/data_pipeline/bgp/snapshots/origin.py", "backend/data_pipeline/bgp/snapshots/__init__.py",
                "backend/data_pipeline/common/prefix_networks.py",
                "backend/data_pipeline/bgp/__init__.py",
                "backend/data_pipeline/bgp/input/mrt_reader.py",
                "backend/data_pipeline/bgp/input/reference_reader.py",
                "backend/data_pipeline/bgp/replay/run_from_files.py",
                "backend/data_pipeline/bgp/replay/archive_input.py",
                "backend/data_pipeline/bgp/replay/route_replay.py",
                "backend/data_pipeline/bgp/archive/message_reader.py",
                "backend/data_pipeline/bgp/archive/selection.py",
                "backend/data_pipeline/bgp/archive/checkpoint.py",
                "backend/data_pipeline/common/run_metrics.py",
                "backend/data_pipeline/bgp/ordered_reader.py",
                "backend/data_pipeline/bgp/record_types.py",
                "backend/data_pipeline/bgp/input/mrt_types.py",
                "backend/data_pipeline/bgp/archive/store.py",
                "backend/data_pipeline/bgp/input/path_decoding.py",
            ],
            "entry": ["scripts/pipeline/detection-frozen-run.py"],
            "lock": ["backend/pyproject.toml", "backend/uv.lock"],
        },
        ["duckdb", "pyarrow", "psycopg2-binary", "pandas", "openpyxl"],
        schema="detection-execution/v1",
    )
