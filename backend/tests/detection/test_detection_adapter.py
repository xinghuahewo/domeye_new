"""显式人工已保存元素；默认计算采用真实字段，差异单独保留。"""

from data_pipeline.analysis.detection.adapter import adapt_element


def test_local_addpath_et_uses_actual_peer_path_and_full_time():
    row = dict(
        action="announce",
        peer_asn=123,
        peer_ip="192.0.2.1",
        epoch=1772236800,
        microsecond=123456,
        source_id="source",
        event_id="element",
        record=7,
        ordinal=2,
        prefix="10.0.0.0/24",
        as_path_text="123 456",
        raw_prefix=b"\x0a\0\0",
        local_message=True,
        message_id="message",
        path_id=77,
        local_ip="192.0.2.2",
        local_asn=789,
        interface=0,
        mrt_type=17,
        mrt_subtype=11,
        path_key="path",
    )
    item = adapt_element(row, run_id="run", snapshot=7, collector_id="fixture")
    assert item.legacy_vp == "123" and item.raw_path == "123 456"
    assert item.observed_at.endswith(".123456+00:00")
    assert item.direction == "sent" and item.path_id == 77
    assert item.decoding_difference["old_fixed_vp"] == "789"
    assert item.decoding_difference["old_fixed_path"] == "77"
    assert item.canonical_before_ref is None and item.canonical_after_ref is None
