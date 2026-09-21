"""M1 逐帧解释合同；不是来源完成、Session 或业务准入合同。"""
from dataclasses import dataclass
from enum import Enum


class ReadPolicy(str, Enum):
    STRICT = 'strict/v1'
    ISOLATE_PAYLOAD = 'isolate-payload/v1'


class ParseStatus(str, Enum):
    DECODED = 'decoded'
    UNSUPPORTED = 'unsupported'
    REJECTED = 'rejected'


@dataclass(frozen=True)
class FieldFailure:
    code: str
    section: str
    offset: int | None = None  # 解压来源中的绝对字节偏移，未知为 None
    requested: int | None = None
    remaining: int | None = None
    attribute_offset: int | None = None
    attribute_flags: int | None = None
    attribute_type: int | None = None
    attribute_declared_length: int | None = None


@dataclass(frozen=True)
class HeaderEvidence:
    """仅独立读全的头字段；端点 AFI 不是 NLRI AFI，端点不是 Session。"""
    microsecond: int | None = None
    endpoint_trust: str = 'unknown'
    peer_ip: str | None = None
    peer_asn: int | None = None
    local_ip: str | None = None
    local_asn: int | None = None
    interface: int | None = None
    endpoint_afi: int | None = None
    direction: str = 'unknown'
    bgp_type: int | None = None  # 仅 marker 和总长度核对后的消息头


@dataclass(frozen=True)
class Interpretation:
    status: ParseStatus
    policy: ReadPolicy
    reason_code: str | None
    header: HeaderEvidence
    failure: FieldFailure | None = None
    interpretation_level: str = 'frame'
    source_path: str | None = None
    continuation_allowed: bool = True
    frame_complete: bool = True
    next_record_offset: int | None = None
    contract_version: str = 'mrt-interpretation/v1'
