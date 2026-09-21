"""C5 S1 的有限人工接口。没有真实资格、存储或发布入口。"""
from dataclasses import dataclass, fields, is_dataclass
from fractions import Fraction
from hashlib import sha256
import json

VERSION = 'country-trend-fixture/v2'
REFERENCE_DEFINITION = ('fixed_endpoint_direction/v1', 'visible_direction_count',
                        'endpoint_direction_count', 'fixed_endpoint_direction')
RULE = 'country-trend-exact-direction/v1'
ACTIVITY_RULE = 'trend-activity-exact-interval/v1'
STATES = ('normal', 'affected', 'route_interrupted', 'unknown')
TRACKS = (
    'interrupted_prefix_count', 'completely_interrupted_prefix_count',
    'invisible_direction_count', 'affected_asn_count', 'route_interrupted_asn_count',
    'fixed_visible_ipv4_address_count', 'fixed_visible_ipv6_slash48_equivalent',
    'new_visible_ipv4_prefix_count', 'new_visible_ipv6_prefix_count',
    'new_visible_ipv4_address_count', 'new_visible_ipv6_slash48_equivalent',
    'new_cumulative_ipv4_prefix_count', 'new_cumulative_ipv6_prefix_count',
    'new_cumulative_ipv4_address_count', 'new_cumulative_ipv6_slash48_equivalent',
)


@dataclass(frozen=True)
class FixtureBinding:
    """完整物理身份作为人工值保留；绝不授予真正 C3/PG 准入。"""
    component: tuple  # 九项 ComponentBinding 字段，按原声明顺序
    read_binding: tuple
    c1_binding: tuple
    qualification: str = 'fixture_strict_no_m3'
    interpretation: str = 'strict'


@dataclass(frozen=True)
class FixtureBatch:
    rows: tuple  # 与 C3 ReadBatch 同形：C2Row 或 C2Completion


@dataclass(frozen=True)
class FixtureEnd:
    binding: FixtureBinding
    rows: int
    complete: bool = True


@dataclass(frozen=True)
class ActivityWindow:
    event: tuple
    mode: str
    metric: str
    population: str
    start_us: int
    end_us: int
    value: Fraction | None
    state: str
    source_ref: str
    source_rank: int = 0
    source_id: str = 'fixture-source'
    window_role: str = 'result'


@dataclass(frozen=True)
class Projection:
    country: str
    population: str
    denominator: int
    samples: tuple
    values: tuple
    source_ref: str
    quality: str = 'complete'
    asn_count: int | None = None
    persistent_asn_count: int | None = None
    cohort_id: str | None = None
    definition_binding: tuple = ()


@dataclass(frozen=True)
class ReferenceInput:
    event: tuple
    binding: tuple
    target: str
    projections: tuple
    definition: str = 'fixed_endpoint_direction/v1'


@dataclass(frozen=True)
class TrendInput:
    country: FixtureBinding
    feature_binding: tuple = ()
    activities: tuple = ()
    references: tuple = ()
    rules: tuple = (RULE, ACTIVITY_RULE)
    data_kind: str = 'fixture'
    schema_version: str = VERSION


@dataclass(frozen=True)
class Limits:
    max_rows: int = 1_000_000  # 输入和输出合计
    max_row_bytes: int = 4 * 1024**2
    max_batch_rows: int = 256
    max_batch_bytes: int = 4 * 1024**2
    max_event_bytes: int = 32 * 1024**2
    max_context_bytes: int = 8 * 1024**2
    max_samples: int = 10000
    max_rss_bytes: int = 2 * 1024**3
    max_disk_bytes: int = 8 * 1024**3


@dataclass(frozen=True)
class TrendRow:
    """有限行种类、事件键和有序字段；矩阵/图/证据逐行，不含全制品JSON。"""
    kind: str
    event: tuple
    key: tuple
    values: tuple
    refs: tuple = ()
    result_id: str = ''

    def get(self, name):
        return dict(self.values)[name]


@dataclass(frozen=True)
class TrendCompletion:
    result_id: str
    input_rows: int
    output_rows: int
    input_sha256: str
    output_sha256: str
    events: int
    elapsed_seconds: float
    process_peak_rss_bytes: int
    temporary_peak_bytes: int
    data_kind: str = 'fixture'


def tree(value):
    """规范身份：保留记录类型、有理数和顺序，拒绝隐式浮点。"""
    if value is None or type(value) in (str, bool):
        return value
    if type(value) is int:
        return ['int', str(value)]
    if isinstance(value, Fraction):
        return ['fraction', str(value.numerator), str(value.denominator)]
    if isinstance(value, tuple):
        return ['tuple', [tree(x) for x in value]]
    if is_dataclass(value):
        return [type(value).__name__, [[f.name, tree(getattr(value, f.name))] for f in fields(value)]]
    raise ValueError('trend_non_exact_identity:' + type(value).__name__)


def packed(value):
    return json.dumps(tree(value), ensure_ascii=False, separators=(',', ':')).encode()


def digest(value):
    return sha256(packed(value)).hexdigest()


def row(kind, event, key=(), refs=(), **values):
    return TrendRow(kind, event, key, tuple(values.items()), tuple(refs))


def exact(value):
    if value is None or type(value) is int or isinstance(value, Fraction):
        return value
    raise ValueError('trend_non_exact_number')
