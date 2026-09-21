"""国家增强新规则的纯计算输入；不依赖旧库、MRT 或运行服务。"""
from dataclasses import dataclass
from ipaddress import ip_network
from typing import Optional, Tuple

RULE_VERSION = 'country-enhancement/bounded-v2'
ASSUMPTIONS = ('cutover_assumed', 'source_order_declared', 'session_continuity_unknown')


@dataclass(frozen=True, order=True)
class Cursor:
    source_rank: int
    record: int
    ordinal: int

    def __post_init__(self):
        if any(type(v) is not int for v in (self.source_rank, self.record, self.ordinal)) or min(self.source_rank, self.record, self.ordinal) < 0:
            raise ValueError('来源游标不能为负')


@dataclass(frozen=True)
class Time:
    epoch: int
    microsecond: Optional[int] = None

    def __post_init__(self):
        if type(self.epoch) is not int or (self.microsecond is not None and type(self.microsecond) is not int):
            raise ValueError('原时间必须为整数和明确精度')
        if self.microsecond is not None and not 0 <= self.microsecond < 1000000:
            raise ValueError('微秒越界')

    @property
    def lower_us(self):
        return self.epoch * 1000000 + (self.microsecond or 0)

    @property
    def upper_us(self):
        return self.lower_us if self.microsecond is not None else self.lower_us + 999999


@dataclass(frozen=True)
class Binding:
    run_id: str
    snapshot_id: str
    ordered_sources: Tuple[str, ...]
    reference_version: str
    decoder_version: str
    state_rule_version: str
    collector: str
    state: str = 'complete'

    def __post_init__(self):
        if self.state != 'complete' or not all((self.run_id, self.snapshot_id, self.reference_version,
                                               self.decoder_version, self.state_rule_version, self.collector)):
            raise ValueError('只接受固定完成版本')
        if not self.ordered_sources or len(set(self.ordered_sources)) != len(self.ordered_sources):
            raise ValueError('必须绑定唯一有序来源')

    def source_at(self, rank):
        """旧保存态 profile 使用连续来源序号；不改变其序列化身份。"""
        if type(rank) is not int or not 0 <= rank < len(self.ordered_sources):
            return None
        return self.ordered_sources[rank]

    def rank_of(self, source_id):
        return self.ordered_sources.index(source_id)


@dataclass(frozen=True)
class Incident:
    incident_id: str
    revision: int
    country: str
    detected_at: Time
    onset: Optional[Time] = None
    onset_cursor: Optional[Cursor] = None
    legacy_ref: Optional[str] = None
    source_episode_ref: Optional[str] = None
    carry_in_state: str = 'unknown'
    end: Optional[Time] = None

    def __post_init__(self):
        if not self.incident_id or self.revision < 1 or len(self.country) != 2 or not self.country.isascii() or not self.country.isalpha() or not self.country.isupper():
            raise ValueError('国家事件身份无效')
        if self.end is not None and self.end.upper_us < (self.onset or self.detected_at).lower_us:
            raise ValueError('事件结束早于已知起始')
        if self.onset_cursor is not None and self.onset is None:
            raise ValueError('起始游标必须有原始时间')


@dataclass(frozen=True, order=True)
class Endpoint:
    collector: str
    remote_ip: str
    remote_asn: int
    local_ip: str
    local_asn: Optional[int]
    interface: Optional[int]
    afi: int
    safi: int

    def __post_init__(self):
        if not self.collector or not self.remote_ip or not self.local_ip or self.afi not in (1, 2) or self.safi < 1:
            raise ValueError('计算端点范围无效')


@dataclass(frozen=True)
class Route:
    object_id: str
    endpoint: Endpoint
    prefix: str
    path_id: Optional[int]
    presence: str
    path_ref: Optional[str]
    origin: Optional[int]
    observation_ref: str
    observed_at: Time
    cursor: Cursor
    mapping_ref: str
    peer_ref: str
    mapping_state: str = 'bound'
    fact_presence: str = 'unknown'
    local_message: bool = False

    def __post_init__(self):
        network = ip_network(self.prefix, strict=True)
        if (network.version == 4) != (self.endpoint.afi == 1):
            raise ValueError('前缀地址族冲突')
        if self.presence not in ('present', 'absent', 'unknown') or self.fact_presence not in ('present', 'absent', 'unknown'):
            raise ValueError('路由必须使用三值状态')
        if not all((self.object_id, self.observation_ref, self.mapping_ref, self.peer_ref)):
            raise ValueError('必须保留原对象、观察和映射引用')
        if self.mapping_state not in ('bound', 'unmapped', 'ambiguous'):
            raise ValueError('映射状态无效')
        if self.path_id is not None and not 0 <= self.path_id <= 4294967295:
            raise ValueError('path_id越界')
        if self.origin is not None and not 1 <= self.origin <= 4294967295:
            raise ValueError('未知起源必须为None')

    @property
    def direction(self):
        return self.endpoint, self.prefix


@dataclass(frozen=True)
class Reference:
    asn: int
    country: Optional[str]
    source_ref: str
    state: str = 'known'
    historical_state: str = 'unknown'

    def __post_init__(self):
        if not self.source_ref or self.state not in ('known', 'unknown', 'conflict'):
            raise ValueError('参考状态或来源无效')
        if self.state == 'known' and (self.country is None or len(self.country) != 2 or not self.country.isascii() or not self.country.isalpha() or not self.country.isupper()):
            raise ValueError('已知国家映射必须有国家码')


@dataclass(frozen=True)
class Baseline:
    baseline_id: str
    binding: Binding
    at: Time
    cursor: Cursor
    mapping_ref: str

    def __post_init__(self):
        if not self.baseline_id or not self.mapping_ref or self.binding.source_at(self.cursor.source_rank) is None:
            raise ValueError('基线引用无效')
