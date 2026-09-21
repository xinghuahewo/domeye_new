"""Detection 的显式输入；仅表示计算引用，不证明真实 Session 连续。"""

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class DetectionInput:
    observation_id: str
    source_id: str
    source_version: str
    collector_id: str
    peer_ref: str
    legacy_vp: str
    prefix: str
    action: str
    observed_at: str
    raw_path: str = ""
    raw_record_ref: str = ""
    canonical_before_ref: str | None = None
    canonical_after_ref: str | None = None
    raw_prefix: str | None = None


@dataclass(frozen=True)
class DetectionScope:
    run_id: str
    source: str
    collector_id: str
    input_version: str
    window_start: str
    window_end: str
    computation_version: str = "detection-39578fe-compat-d09/v1"


@dataclass(frozen=True)
class FileBoundary:
    file_id: str
    source_version: str
    observed_at: str
    legacy_tables: Mapping[str, str]
    slot_ref: str | None = None


@dataclass(frozen=True)
class ReferenceBundle:
    version: str
    # 完整原行/JSON指针；与解释映射分开，整数和字符串键不归一。
    raw_rows: Mapping[str, Any]
    row_refs: Mapping[str, Any]
    mappings: Mapping[str, Any]
    historical_applicability: str = "Unknown"


@dataclass(frozen=True)
class DetectionSeed:
    observations: tuple[DetectionInput, ...]
    baseline_ref: str
    legacy_ids: Mapping[str, Mapping[str, int]] = field(default_factory=dict)
    initialization: str = "explicit_rib_cold_start"
