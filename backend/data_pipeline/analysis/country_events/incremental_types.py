"""C2新增typed输出；缺口与精确峰值分开，原S2行保持原合同。"""
from dataclasses import dataclass
from fractions import Fraction
from typing import Optional
from data_pipeline.analysis.country_events.models import Endpoint, Cursor


@dataclass(frozen=True)
class InputRequest:
    """私有驱动协议，不是业务输出。None表示完整输入结束。"""


@dataclass(frozen=True)
class DirectionPoint:
    cohort_id: str
    sample_us: int
    prefix: str
    endpoint: Endpoint
    state: str
    object_refs: tuple
    evidence_refs: tuple
    fact_state: str = 'unknown'


@dataclass(frozen=True)
class FirstQualifiedRef:
    cohort_id: str
    prefix: str
    first_qualified_ref: str
    first_qualified_us: int
    origin_refs: tuple
    first_qualified_cursor: Optional[Cursor] = None
    country_reference: Optional[str] = None
    membership_basis: str = 'ever_qualified_in_event'


@dataclass(frozen=True)
class PathPeakQuality:
    cohort_id: str
    affected_asn: int
    downstream_asn: int
    known_slots: int
    unknown_slots: int
    exact_prefix_peak: Optional[int]
    exact_ipv4_peak: Optional[Fraction]
    exact_ipv6_peak: Optional[Fraction]
    basis: str = 'calculation_with_explicit_unknown_slots'


@dataclass(frozen=True)
class AsnMetricPeak:
    cohort_id: str
    asn: int
    afi: int
    metric: str
    known_peak: Optional[int]
    exact_peak: Optional[int]
    first_sample_us: Optional[int]
    occurrence_count: int
    known_slots: int
    unknown_slots: int
