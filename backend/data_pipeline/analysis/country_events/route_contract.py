"""国家 M3 有限逐维行合同；不签发上游资格，不以零事件代替覆盖。"""
from dataclasses import asdict, dataclass, fields
from fractions import Fraction
import hashlib

from data_pipeline.analysis.country_events import snapshot_schema as original
from data_pipeline.bgp.record_types import GapScope, ScopeKind, Extent
from data_pipeline.bgp.replay.snapshot_contract import TABLES as CANONICAL_TABLES

RULE_VERSION = 'country-qualification/v1'
DIMENSIONS = ('event_enumeration', 'event_anchor', 'event_lifecycle', 'cohort_membership',
              'fixed_denominator', 'point_presence', 'origin_attribution', 'new_member_enumeration',
              'path_current', 'path_history_completeness', 'window_peak', 'window_continuity')
TARGET_KINDS = ('module', 'window', 'revision', 'cohort', 'object', 'sample', 'metric', 'path')
COVERAGE = ('complete', 'partial', 'unknown', 'not_applicable')


def require(condition, message):
    if not condition:
        raise ValueError('M3 ' + message)


def position(value):
    require(type(value) is tuple and len(value) == 4
            and all(type(n) is int and n >= 0 for n in value)
            and value[2] in (0, 1) and (value[2] == 1 or value[3] == 0), '原处理位置无效')


def interval(start, end):
    if start is not None:
        position(start)
    if end is not None:
        position(end)
    require(end is None or start is not None and start < end, '资格位置区间必须非空且右端不含')


def window(value):
    require(type(value) is tuple and len(value) == 2 and all(type(n) is int for n in value)
            and value[0] < value[1], '窗口必须是非空整数微秒半开区间')


def references(values):
    """有限原引用：(owner, admission_id, 原view, 原row key)，不解析任意查询。"""
    require(type(values) is tuple, '原引用必须为保序元组')
    views = {'m2': ('messages', 'elements', 'paths', 'peers', 'eor', 'associations', 'quality'),
             'reference': ('references',), 'canonical': CANONICAL_TABLES,
             'detection': ('records', 'm3_entries', 'result_revisions', 'result_coverage')}
    for ref in values:
        require(type(ref) is tuple and len(ref) == 4
                and all(type(v) is str and v for v in ref)
                and ref[0] in ('m2', 'reference', 'canonical', 'detection'), '原引用结构无效')
        require(ref[2] in views[ref[0]], '原引用视图不受支持')
    require(len(set(values)) == len(values), '原引用重复')


def target(value, *, optional=False):
    if value is None and optional:
        return
    require(type(value) is tuple and len(value) == 2 and value[0] in original.TABLES
            and type(value[1]) is int and value[1] >= 0, '目标必须是原21表的全局序号')


def scope(text, binding_id):
    """保存原 GapScope 有限字段；不重新分类或缩小潜在对象范围。"""
    raw = original.decode(text)
    require(type(raw) is dict and set(raw) == {f.name for f in fields(GapScope)}, '依赖范围字段不符')
    require(raw['kind'] in tuple(v.value for v in ScopeKind)
            and raw['input_chain_id'] == binding_id and type(raw['collector']) is str
            and bool(raw['collector']) and raw['session'] is None, '依赖范围原绑定无效')
    require(all(raw[k] == Extent.ALL_INCLUDING_UNSEEN.value for k in
                ('route_families', 'prefixes', 'path_slots')), '依赖范围不得缩小未见对象')
    endpoint = raw['endpoint']
    if endpoint is not None:
        require(type(endpoint) is dict and set(endpoint) ==
                {'peer_ip', 'peer_asn', 'local_ip', 'local_asn', 'interface'}, '端点原字段不符')
    require(raw['kind'] != ScopeKind.RECEIVED_ENDPOINT.value or endpoint is not None, 'received 范围缺原端点')
    return raw


def common(value):
    require(type(value.logical_run_id) is str and bool(value.logical_run_id)
            and type(value.input_binding_id) is str and bool(value.input_binding_id), '逻辑run/原输入绑定缺失')
    require(value.rule_version == RULE_VERSION and value.dimension in DIMENSIONS, '资格规则或维度不受支持')
    window(value.window_us)


def content_identity(value, field):
    content = asdict(value)
    supplied = content.pop(field)
    digest = hashlib.sha256(original.encode((RULE_VERSION, type(value).__name__, content)).encode()).hexdigest()
    require(not supplied or supplied == digest, '行内容身份冲突')
    object.__setattr__(value, field, digest)


@dataclass(frozen=True)
class CountryQualification:
    logical_run_id: str
    input_binding_id: str
    target_kind: str
    raw_target_ref: tuple | None
    incident_id: str | None
    revision: int | None
    cohort_id: str | None
    entity_key: str | None
    afi: int | None
    metric: str | None
    dimension: str
    dependency_scope: str
    effective_start_position: tuple | None
    effective_end_position: tuple | None
    sample_us: int | None
    window_us: tuple
    coverage: str
    gap_refs: tuple
    upstream_qualification_refs: tuple
    evidence_refs: tuple
    recovery_witnesses: tuple
    rule_version: str = RULE_VERSION
    qualification_id: str = ''

    def __post_init__(self):
        common(self)
        require(self.target_kind in TARGET_KINDS and self.coverage in COVERAGE, '资格目标/覆盖无效')
        target(self.raw_target_ref, optional=self.target_kind in ('module', 'window'))
        require((self.incident_id is None) == (self.revision is None)
                and (self.revision is None or type(self.revision) is int and self.revision > 0), '事件修订必须同时明确或同时为空')
        require(self.afi is None or type(self.afi) is int and self.afi in (1, 2), 'AFI 无效')
        require(self.sample_us is None or type(self.sample_us) is int
                and self.window_us[0] < self.sample_us <= self.window_us[1], '样本边界不在原C2网格窗口')
        interval(self.effective_start_position, self.effective_end_position)
        scope(self.dependency_scope, self.input_binding_id)
        for refs in (self.gap_refs, self.upstream_qualification_refs, self.evidence_refs, self.recovery_witnesses):
            references(refs)
        require(bool(self.evidence_refs), '资格必须指向原证据')
        content_identity(self, 'qualification_id')


@dataclass(frozen=True)
class CountryCoverage:
    logical_run_id: str
    input_binding_id: str
    module: str
    source_id: str | None
    window_us: tuple
    dimension: str
    start_position: tuple | None
    end_position: tuple | None
    event_count: int | None
    execution: str
    coverage: str
    qualification_count: int
    qualification_digest: str
    gap_count: int
    gap_digest: str
    unassigned_scope_refs: tuple
    completion_receipt_refs: tuple
    rule_version: str = RULE_VERSION
    coverage_id: str = ''

    def __post_init__(self):
        common(self)
        require(self.module == 'country' and self.execution in ('not_run', 'complete', 'failed')
                and self.coverage in COVERAGE[:3], '模块/执行/覆盖无效')
        interval(self.start_position, self.end_position)
        require(self.event_count is None or type(self.event_count) is int and self.event_count >= 0, '事件枚举数无效')
        require(self.execution != 'complete' or self.event_count is not None, '完整枚举必须有实际事件数')
        for count, digest in ((self.qualification_count, self.qualification_digest), (self.gap_count, self.gap_digest)):
            require(type(count) is int and count >= 0 and type(digest) is str and len(digest) == 64
                    and all(c in '0123456789abcdef' for c in digest), '覆盖行数/摘要无效')
        references(self.unassigned_scope_refs)
        references(self.completion_receipt_refs)
        require(self.execution != 'complete' or bool(self.completion_receipt_refs), '完整覆盖必须有原完整读回执引用')
        content_identity(self, 'coverage_id')


@dataclass(frozen=True)
class CountryQualifiedValue:
    logical_run_id: str
    input_binding_id: str
    raw_target_ref: tuple
    dimension: str
    value: object
    known_lower_bound: Fraction | None
    lower_bound_proven: bool
    unit: str
    population_ref: tuple
    denominator: Fraction | None
    raw_basis_refs: tuple
    qualification_refs: tuple
    window_us: tuple
    coverage: str
    rule_version: str = RULE_VERSION
    qualified_value_id: str = ''

    def __post_init__(self):
        common(self)
        target(self.raw_target_ref)
        target(self.population_ref)
        require(self.coverage in COVERAGE and type(self.lower_bound_proven) is bool, '主值资格无效')
        require(self.coverage == 'complete' or self.value is None, '不完整资格主值必须为None')
        require(self.value is None or type(self.value) in (str, int, bool, Fraction), '主值不能使用近似浮点或任意对象')
        require(type(self.unit) is str and bool(self.unit), '数值单位缺失')
        for v in (self.known_lower_bound, self.denominator):
            require(v is None or type(v) in (int, Fraction) and v >= 0, '分母/下界必须为非负精确数')
        require(self.lower_bound_proven == (self.known_lower_bound is not None), '未证明下界不能补零')
        require(self.known_lower_bound is None or type(self.value) not in (int, Fraction)
                or self.known_lower_bound <= self.value, '已知下界不能超过精确主值')
        references(self.raw_basis_refs)
        require(bool(self.raw_basis_refs), '主值必须有原事实依据')
        require(type(self.qualification_refs) is tuple and bool(self.qualification_refs)
                and all(type(v) is str and v for v in self.qualification_refs)
                and len(set(self.qualification_refs)) == len(self.qualification_refs), '资格引用缺失或重复')
        content_identity(self, 'qualified_value_id')
