"""C4公开合同：不可变绑定与JSON边界；不创建发布、连接或全局服务。"""
from dataclasses import dataclass, fields, is_dataclass, asdict
from fractions import Fraction
import hashlib
import json
from typing import Any, Callable, ContextManager, Optional, Protocol

from data_pipeline.analysis.country_events.snapshot_store import ComponentBinding
from data_pipeline.analysis.country_events.snapshot_schema import encode as typed_encode, decode as typed_decode, integer

VERSION = 'country-query/v1'
PROFILE = 'country-c4/v1'
SELECTOR = 'fixture:country:c4'
VIEWS = ('overview', 'series15', 'asns', 'asn_matrix', 'asn_window', 'asn_peaks',
         'paths', 'path_samples', 'audit', 'resolve_source', 'resolve_alias')
REQUIRED = tuple('country.' + view for view in VIEWS)
UNAVAILABLE = ('missing_baseline', 'baseline_boundary_not_locatable', 'baseline_time_not_locatable')
PARTIAL = ('ambiguous_baseline_mapping', 'baseline_visibility_unknown',
           'sample_boundary_not_locatable', 'source_gap')


def canonical(value):
    """仅JSON元数据；任意精度业务值须先经typed_encode，禁止隐式浮点。"""
    def check(v):
        if v is None or type(v) in (bool, int, str): return
        if type(v) in (list, tuple):
            for item in v: check(item)
            return
        if type(v) is dict and all(type(k) is str for k in v):
            for item in v.values(): check(item)
            return
        raise ValueError('country_non_json_metadata:' + type(v).__name__)
    check(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def result_id(binding: ComponentBinding):
    return 'country_result_' + digest(asdict(binding))


def exact_json(value):
    if value is None: return {'kind': 'none'}
    if type(value) is int: return {'kind': 'int', 'num': str(value)}
    if type(value) is Fraction:
        return {'kind': 'fraction', 'num': str(value.numerator), 'den': str(value.denominator)}
    raise ValueError('country_non_exact_value')


def exact_value(value):
    if value == {'kind': 'none'}: return None
    if value.get('kind') == 'int': result = integer(value['num'])
    elif value.get('kind') == 'fraction':
        if integer(value['den']) <= 0: raise ValueError('country_fraction_denominator')
        result = Fraction(integer(value['num']), integer(value['den']))
    else: raise ValueError('country_exact_kind')
    if exact_json(result) != value: raise ValueError('country_noncanonical_exact')
    return result


@dataclass(frozen=True)
class ReferenceBinding:
    system_id: str
    database_oid: int
    run_id: str
    snapshot: int
    manifest_sha256: str
    source_id: str
    content_sha256: str
    expected_rows: int
    role: str = 'as_info'
    table: str = 'references'
    storage_rule: str = 'reference-rows/v2'
    interpretation_rule: str = 'detection-reference-39578fe/v2'
    projection_rule: str = 'country-static-reference/v1'


@dataclass(frozen=True)
class FileEntity:
    """stat身份用于复验；sha256在离线完整审计固定。"""
    path: str
    size: int
    mtime_ns: int
    device: int
    inode: int
    sha256: str


@dataclass(frozen=True)
class CountryReadBinding:
    component: ComponentBinding
    result_id: str
    read_model_id: str
    root: str
    manifest_sha256: str
    reference: Optional[ReferenceBinding]
    query_version: str = VERSION


@dataclass(frozen=True)
class CountryAdmissionProof:
    """不是布尔授权；其摘要及实际回执须纳入唯一发布manifest。"""
    result_id: str
    read_model_id: str
    component_manifest_sha256: str
    c3_receipt_typed: str
    index_validation_sha256: str
    index_rows: int
    source_rows: int
    stream_calls: int
    locator_passes: int
    readback_rows: int


@dataclass(frozen=True)
class CountryAdmissionDescriptor:
    read_binding: CountryReadBinding
    admission_proof: CountryAdmissionProof
    # C3原upstream经现有无损codec保存，decode后完整比较Obs/Detection/全部参考；非删减摘要。
    upstream_typed: str
    parameters_typed: str
    completion_typed: str
    entities: tuple[FileEntity, ...]
    tables_json: str
    event_index_json: str
    capabilities: tuple[str, ...] = REQUIRED
    query_version: str = VERSION


@dataclass(frozen=True)
class CountrySelection:
    # 复制已有Token三字段值；不定义另一个Token类或生成P。
    publication_id: str
    build_id: str
    profile_digest: str
    component_key: str
    descriptor: CountryAdmissionDescriptor
    incident_id: str
    revision: int
    cohort_id: Optional[str]
    event_status_typed: str


@dataclass(frozen=True)
class QueryRequest:
    view: str
    filters_json: str = '{}'
    order: str = 'canonical_asc'

    def __post_init__(self):
        if self.view not in VIEWS or self.order != 'canonical_asc':
            raise ValueError('country_query_scope')
        value = json.loads(self.filters_json)
        if type(value) is not dict or canonical(value) != self.filters_json:
            raise ValueError('country_noncanonical_filters')


@dataclass(frozen=True)
class QueryPage:
    items: tuple
    total: int
    next_cursor: Optional[str]
    scope_json: str
    availability: str
    reasons: tuple
    definition_version: str = VERSION
    page_scope_verified: bool = True


@dataclass(frozen=True)
class QueryReadReceipt:
    scope_json: str
    rows: int
    sha256: str
    definition_version: str = VERSION


@dataclass(frozen=True)
class QueryLimits:
    batch_rows: int = 256
    batch_bytes: int = 4 * 1024**2
    max_row_bytes: int = 4 * 1024**2
    max_rows: int = 10_000_000
    max_rss_bytes: int = 2 * 1024**3
    max_disk_bytes: int = 8 * 1024**3
    max_page_rows: int = 256
    max_page_bytes: int = 4 * 1024**2
    max_row_groups: int = 256
    max_read_bytes: int = 64 * 1024**2
    max_cache_bytes: int = 32 * 1024**2
    max_index_steps: int = 1_000_000

    def __post_init__(self):
        if any(type(getattr(self, f.name)) is not int or getattr(self, f.name) < 1 for f in fields(self)):
            raise ValueError('country_invalid_limit')


@dataclass(frozen=True)
class CountryRuntime:
    """Git外运行依赖，不允许contract_json编码。c1仅离线完整审计使用。"""
    component_dsn: str
    c1: Any = None
    verify_qualification: Optional[Callable] = None
    # 旧alias只能由明确冻结来源提供；编码保留source/table/ref_kind/id等完整原字段。
    aliases_typed: Optional[str] = None
    aliases_source_json: Optional[str] = None


class QualificationVerifier(Protocol):
    def __call__(self, descriptor: CountryAdmissionDescriptor, *, lock: bool = False,
                 selection: Optional[CountrySelection] = None) -> ContextManager[None]:
        """selection非空时核真实P纳入关系；空时仅离线来源资格。"""
        ...


_TYPES = {cls.__name__: cls for cls in (ComponentBinding, ReferenceBinding, FileEntity,
    CountryReadBinding, CountryAdmissionProof, CountryAdmissionDescriptor, CountrySelection,
    QueryRequest, QueryLimits)}


def contract_json(value):
    """白名单元数据封包，显式类标签；不接受DSN/runtime或任意dataclass。"""
    def pack(v):
        if is_dataclass(v):
            if _TYPES.get(type(v).__name__) is not type(v): raise ValueError('country_contract_type')
            return {'type': type(v).__name__, 'fields': {f.name: pack(getattr(v, f.name)) for f in fields(v)}}
        if type(v) is tuple: return {'tuple': [pack(x) for x in v]}
        if v is None or type(v) in (str, int, bool): return v
        raise ValueError('country_contract_value')
    return canonical(pack(value))


def contract_value(payload):
    def unpack(v):
        if type(v) is dict:
            if set(v) == {'tuple'}: return tuple(unpack(x) for x in v['tuple'])
            if set(v) != {'type', 'fields'} or v['type'] not in _TYPES: raise ValueError('country_contract_tag')
            cls = _TYPES[v['type']]
            if set(v['fields']) != {f.name for f in fields(cls)}: raise ValueError('country_contract_fields')
            return cls(**{k: unpack(x) for k, x in v['fields'].items()})
        if v is None or type(v) in (str, int, bool): return v
        raise ValueError('country_contract_value')
    result = unpack(json.loads(payload))
    if contract_json(result) != payload: raise ValueError('country_noncanonical_contract')
    return result
