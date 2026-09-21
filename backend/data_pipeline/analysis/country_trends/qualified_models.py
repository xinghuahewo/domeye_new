"""C5 M3 纯计算接缝；这些值不授予 Country 或 Trend 准入。"""
from dataclasses import dataclass
from fractions import Fraction

RULE = 'country-trend-qualified-science/v1'
DIMENSIONS = ('event_enumeration', 'event_anchor', 'event_lifecycle',
              'cohort_membership', 'fixed_denominator', 'point_presence',
              'origin_attribution', 'new_member_enumeration', 'path_current',
              'path_history_completeness', 'window_peak', 'window_continuity')


@dataclass(frozen=True)
class Target:
    """从原行定位得到的预期关联；不能从待核资格反推这些字段。"""
    logical_run_id: str
    input_binding_id: str
    raw_target_ref: tuple
    incident_id: str | None
    revision: int | None
    cohort_id: str | None
    entity_key: str | None
    afi: int | None
    metric: str | None
    sample_us: int | None
    window_us: tuple

    def __post_init__(self):
        if (not self.logical_run_id or not self.input_binding_id
                or type(self.raw_target_ref) is not tuple or len(self.raw_target_ref) != 2
                or type(self.raw_target_ref[0]) is not str
                or type(self.raw_target_ref[1]) is not int or self.raw_target_ref[1] < 0
                or (self.incident_id is None) != (self.revision is None)
                or self.revision is not None and (type(self.revision) is not int or self.revision < 1)
                or type(self.window_us) is not tuple or len(self.window_us) != 2
                or any(type(v) is not int for v in self.window_us)
                or self.window_us[0] >= self.window_us[1]
                or self.sample_us is not None and type(self.sample_us) is not int):
            raise ValueError('trend_m3_target')


@dataclass(frozen=True)
class Selection:
    """原值与主值分离；原因和实际引用保留，缺值不补零。"""
    target: Target
    dimension: str
    population_ref: tuple
    raw: object
    value: int | Fraction | None
    known_lower_bound: int | Fraction | None
    state: str
    reasons: tuple
    qualification_refs: tuple
    raw_basis_refs: tuple
    qualified_value_id: str | None
    denominator: int | Fraction | None = None
    denominator_reasons: tuple = ()


def numeric(value):
    if value is not None and (type(value) not in (int, Fraction) or value < 0):
        raise ValueError('trend_m3_non_exact_value')
    return value


def metric_dimension(name):
    if name.startswith('new_'):
        return 'new_member_enumeration'
    return 'origin_attribution' if 'asn' in name else 'point_presence'
