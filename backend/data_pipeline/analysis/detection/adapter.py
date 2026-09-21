"""共享已保存观察到Detection的显式适配，不解析MRT或构造规范态。"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Mapping

from data_pipeline.bgp.input.path_decoding import DECODER_VERSION, DIRECTION_RULE, decoding_difference
from data_pipeline.analysis.detection.models import DetectionInput
from data_pipeline.analysis.detection._results import stable


@dataclass(frozen=True)
class DecodedDetectionInput(DetectionInput):
    decoder_version: str = DECODER_VERSION
    direction_rule: str = DIRECTION_RULE
    direction: str = "unknown"
    snapshot_ref: str = ""
    message_ref: str = ""
    element_ordinal: int = 0
    path_id: int | None = None
    epoch: int = 0
    microsecond: int | None = None
    peer_endpoint: Mapping | None = None
    local_endpoint: Mapping | None = None
    quality: tuple = ()
    decoding_difference: Mapping | None = None
    path_ref: str = ""
    path_quality: Mapping | None = None
    source_content_sha256: str = ""
    mrt_type: int | None = None
    mrt_subtype: int | None = None
    path_id_present: bool | None = None
    timestamp_precision: str = "unknown"


PEER_FIELDS = ("peer_ip", "peer_asn", "peer_table_record", "peer_index", "bgp_id", "bgp_id_present")


@lru_cache(maxsize=4096)
def _peer_ref(snapshot_ref, source_id, typed_values):
    values = (value for _, value in typed_values)
    return "saved-peer:" + stable([snapshot_ref, source_id, dict(zip(PEER_FIELDS, values))])


def adapt_element(row, *, run_id, snapshot, collector_id, quality=(), source_version=None):
    # decode_element 只给原行追加版本常量；这些常量已是返回类型的默认字段。
    # 直接读取原行，避免每条复制完整属性；不修改调用方输入。
    decoded = row
    action = {"rib_snapshot": "RIB", "announce": "A", "withdraw": "W"}.get(
        decoded["action"]
    )
    if action is None:
        raise ValueError("未支持的保存元素action")
    if action != "RIB" and not isinstance(decoded["local_message"], bool):
        raise ValueError("UPDATE方向字段未知，不猜接收方向")
    if decoded["peer_asn"] is None or not decoded["peer_ip"]:
        raise ValueError("真实Peer端点缺失，不猜legacy VP")
    instant = datetime.fromtimestamp(decoded["epoch"], timezone.utc)
    fraction = decoded["microsecond"]
    if fraction is not None:
        if not 0 <= fraction < 1000000:
            raise ValueError("微秒字段越界")
        instant += timedelta(microseconds=fraction)
    snapshot_ref = f"{run_id}:{snapshot}"
    peer = {
        key: decoded.get(key)
        for key in PEER_FIELDS
    }
    # 缓存键保留 int/string/bool 等原类型；通用离线调用的复杂值仍沿原摘要解释。
    peer_values = tuple(peer.values())
    peer_ref = (_peer_ref(snapshot_ref, decoded["source_id"], tuple((type(v), v) for v in peer_values))
                if all(type(v) in (str, int, bool, type(None)) for v in peer_values)
                else "saved-peer:" + stable([snapshot_ref, decoded["source_id"], peer]))
    return DecodedDetectionInput(
        observation_id=decoded["event_id"],
        source_id=decoded["source_id"],
        source_version=snapshot_ref if source_version is None else source_version,
        collector_id=collector_id,
        peer_ref=peer_ref,
        legacy_vp=str(decoded["peer_asn"]),
        prefix=decoded["prefix"],
        action=action,
        observed_at=instant.isoformat(),
        raw_path=decoded["as_path_text"],
        raw_record_ref=f"{snapshot_ref}/{decoded['source_id']}/{decoded['record']}/{decoded['ordinal']}",
        raw_prefix=decoded["raw_prefix"].hex(),
        direction="snapshot"
        if action == "RIB"
        else "sent"
        if decoded["local_message"]
        else "received",
        snapshot_ref=snapshot_ref,
        message_ref=decoded["message_id"],
        element_ordinal=decoded["ordinal"],
        path_id=decoded["path_id"],
        epoch=decoded["epoch"],
        microsecond=fraction,
        peer_endpoint=peer,
        local_endpoint={
            key: decoded.get(key) for key in ("local_ip", "local_asn", "interface")
        },
        quality=tuple(quality),
        decoding_difference=decoding_difference(decoded),
        path_ref=decoded["path_key"],
        path_quality={
            key: decoded.get(key)
            for key in (
                "reason",
                "raw_origin_asn",
                "attributed_origin_asn",
                "as4_path_text",
                "attributes_digest",
            )
        },
        source_content_sha256=decoded.get("content_sha256", ""),
        mrt_type=decoded["mrt_type"],
        mrt_subtype=decoded["mrt_subtype"],
        path_id_present=decoded.get("path_id_present"),
        timestamp_precision="microsecond" if fraction is not None else "second",
    )
