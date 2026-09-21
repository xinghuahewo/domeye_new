"""M3 共用有序消费合同；不裁定业务资格，不声明 Session。"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_pipeline.bgp.archive import message_reader as consumer
import hashlib
import json

from data_pipeline.bgp.input.mrt_types import HeaderEvidence, Interpretation, ParseStatus

ORDERED_VERSION = 'ordered-observation/v1'
GAP_RULE_VERSION = 'payload-gap/v1'
INTERPRETATION_VERSION = 'mrt-interpretation/v1'
NATIVE_INTERPRETATION_VERSION = 'mrt-interpretation/bgpdump-v1'


class Direction(str, Enum):
    RECEIVED = 'received'
    LOCAL = 'local'
    UNKNOWN = 'unknown'


class ScopeKind(str, Enum):
    RECEIVED_ENDPOINT = 'received_endpoint'
    LOCAL_OBSERVATION = 'local_observation'
    COLLECTOR_CHAIN = 'collector_chain'


class Extent(str, Enum):
    ALL_INCLUDING_UNSEEN = 'all_including_unseen'


def metadata_json(value):
    """仅元数据（Binding/Gap 等）的稳定 JSON；原始观察 bytes 不走此入口。"""
    return json.dumps(asdict(value), ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)


def digest_metadata(value):
    return hashlib.sha256(metadata_json(value).encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class SourceBinding:
    source_id: str
    content_sha256: str
    role: str
    checkpoint_digest: str
    messages: int
    elements: int
    decoded: int
    rejected: int
    unsupported: int


@dataclass(frozen=True)
class InputBinding:
    collector: str
    observation_run: str
    seal_snapshot: int
    plan_id: str
    seal_id: str
    seal_digest: str
    sources: tuple[SourceBinding, ...]  # 完整 MRT 来源序；不含 reference checkpoint
    profile: str = 'observation'
    schema_version: str = 'observation-checkpoint/v1'
    interpretation_version: str = INTERPRETATION_VERSION
    ordered_version: str = ORDERED_VERSION

    @cached_property
    def binding_id(self):
        return digest_metadata(self)

    @property
    def ordered_source_ids(self):
        return tuple(s.source_id for s in self.sources)


@dataclass(frozen=True, order=True)
class MessagePosition:
    source_rank: int
    record: int

    def __post_init__(self):
        if any(type(v) is not int or v < 0 for v in (self.source_rank, self.record)):
            raise ValueError('消息位置必须为非负整数')

    @property
    def sort_key(self):
        return (self.source_rank, self.record, 0, 0)


@dataclass(frozen=True, order=True)
class ElementPosition:
    message: MessagePosition
    ordinal: int

    def __post_init__(self):
        if type(self.ordinal) is not int or self.ordinal < 0:
            raise ValueError('元素 ordinal 必须为非负整数')

    @property
    def sort_key(self):
        return (self.message.source_rank, self.message.record, 1, self.ordinal)


@dataclass(frozen=True)
class RawTime:
    epoch: int
    microsecond: int | None


@dataclass(frozen=True)
class RawReference:
    source_id: str
    content_sha256: str
    record: int
    offset: int
    length: int
    raw_digest: str


@dataclass(frozen=True)
class Endpoint:
    peer_ip: str
    peer_asn: int
    local_ip: str
    local_asn: int
    interface: int


@dataclass(frozen=True)
class GapScope:
    kind: ScopeKind
    collector: str
    input_chain_id: str  # 精确 InputBinding.binding_id
    endpoint: Endpoint | None
    # 供旧模块判断同 ASN 折叠依赖；不是 Peer Identity 或已裁定的业务资格。
    peer_asn_evidence: int | None
    route_families: Extent = Extent.ALL_INCLUDING_UNSEEN
    prefixes: Extent = Extent.ALL_INCLUDING_UNSEEN
    path_slots: Extent = Extent.ALL_INCLUDING_UNSEEN
    # 无可信原 Peer 时包含尚未出现的 ASN；有可信 Peer 也不能按当前 prefix 缩小。
    unknown_peer_covers_unseen_asns: bool = True
    unmapped_rib_overlap: str = 'requires_dependency_check'
    session: None = None


@dataclass(frozen=True)
class Gap:
    gap_id: str
    binding_ref: str
    message_id: str
    position: MessagePosition
    raw_time: RawTime
    scope: GapScope
    direction: Direction
    parse_status: ParseStatus
    reason_code: str
    interpretation_digest: str
    raw_ref: RawReference
    header: HeaderEvidence
    interpretation: Interpretation
    rule_version: str = GAP_RULE_VERSION


@dataclass(frozen=True)
class SourceStart:
    binding_ref: str
    source_rank: int
    raw: consumer.SourceStart  # 原对象保留


@dataclass(frozen=True)
class MessageBoundary:
    binding_ref: str
    position: MessagePosition
    raw_time: RawTime
    raw: dict  # 原字典及 bytes 保留，消费者不得就地修改
    interpretation: Interpretation
    gap: Gap | None
    # STATE/EOR 在 raw.kind/old_state/new_state/eor，绝非 Gap 的别名。


@dataclass(frozen=True)
class Element:
    binding_ref: str
    position: ElementPosition
    raw: dict


@dataclass(frozen=True)
class SourceQuality:
    binding_ref: str
    source_rank: int
    raw: dict  # 没有消息游标；不伪造 record，不重复执行 Gap


@dataclass(frozen=True)
class ParseCounts:
    decoded: int
    rejected: int
    unsupported: int
    gaps: int


@dataclass(frozen=True)
class SourceEnd:
    binding_ref: str
    source_rank: int
    raw: consumer.SourceEnd  # 只证明该来源已收到 M2 尾核验
    parse_counts: ParseCounts


OrderedItem = SourceStart | MessageBoundary | Element | SourceQuality | SourceEnd
