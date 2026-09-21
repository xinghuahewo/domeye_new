"""异常记录转换的合成 fixture 验证；不读取真实数据库。"""

import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from data_pipeline.common.event_records import convert_anomaly_record


@pytest.fixture
def data_profile():
    return json.loads(
        (Path(__file__).resolve().parents[3] / "config/data-profile.json")
        .read_text(encoding="utf-8")
    )


@pytest.fixture
def source():
    return {
        "instance": "synthetic-fixture-db",
        "table": "prefix_outage_202602",
        "representation": "source_rows",
        "read_at": "2026-09-10T09:00:00Z",
        "read_scope": {
            "source": "r",
            "start": "2026-02-01T00:00:00+08:00",
            "end_exclusive": "2026-02-02T00:00:00+08:00",
        },
        "content_version": None,
        "collector_id": None,
        "detector_version": None,
        "evidence_refs": [],
    }


@pytest.fixture
def prefix_row():
    return {
        "source": "r",
        "prefix": "192.0.2.0/24",
        "outage_id": 7,
        "asn": "64512",
        "s_time": "2026-02-01 00:00:00",
        "e_time": None,
        "duration": None,
        "outage_level": "high",
        "outage_level_descr": "合成高等级",
        "event_info": "合成前缀中断记录",
        "pre_vp_paths": {"2026-01-31 23:55:00": ["64500 64512"]},
        "eve_vp_paths": [],
        "next_vp_paths": None,
    }


REFERENCE = "prefix_outage/2026-02-01 00:00:00/192.0.2.0-24/7/r"


def test_country_record_preserves_identity_and_uninterpreted_aggregates(source, data_profile):
    from data_pipeline.common.event_records import serialize_record, deserialize_record
    source["table"] = "country_outage_202602"
    row = {
        "source": "r", "country": "ZZ", "outage_id": 7,
        "country_chinese_name": "合成国家", "s_time": "2026-02-01 00:00:00",
        "e_time": None, "duration": None, "outage_level": "middle",
        "outage_level_descr": "合成等级说明", "max_outage_as_ratio": Decimal("0.300"),
        "max_outage_as_num": 2, "total_as_num": 10, "outage_ases": ["64512", "64513"],
        "event_info": "北京时间 2026-02-02 08:00:00 的合成说明，不作为事件起点",
    }
    reference = "country_outage/2026-02-01 00:00:00/ZZ/7/r"
    result = convert_anomaly_record(reference, [row], source=source, data_profile=data_profile)
    record = result["record"]
    assert record["association"]["state"] == "matched"
    assert record["association"]["locators"] == [{
        "source_instance": "synthetic-fixture-db", "source_table": "country_outage_202602",
        "key": {"source": "r", "country": "ZZ", "outage_id": 7},
    }]
    assert record["identity"]["legacy_reference"] == reference
    assert record["raw_fields"] == row
    assert record["common"]["object"] == {"kind": "country", "value": "ZZ"}
    assert record["common"]["start_time"]["value"] == "2026-01-31T16:00:00Z"
    assert record["common"]["end_time"]["state"] == "unknown"
    assert record["details"]["country_chinese_name"]["value"] == "合成国家"
    assert record["details"]["max_outage_as_ratio"]["value"] == Decimal("0.300")
    assert record["details"]["outage_ases"]["value"] == ["64512", "64513"]
    assert not any("path" in key or "v2" in key for key in record["details"])
    assert {"affected_population_not_established", "stored_ratio_not_authoritative",
            "description_time_not_event_start", "country_record_not_national_connectivity"} <= set(record["limitations"])
    assert deserialize_record(serialize_record(result)) == result


def test_sub_hijack_preserves_parent_roles_and_absent_path_fields(source, data_profile):
    from data_pipeline.common.event_records import serialize_record, deserialize_record
    source["table"] = "sub_hijack_202602"
    row = {
        "source": "r", "prefix": "192.0.2.0/25", "sub_hijack_eventid": 7,
        "hijacked_prefix": "192.0.2.0/24", "hijacked_as": "['64512']",
        "hijacked_as_name": "合成名称甲", "hijacked_as_org": "合成组织甲",
        "hijacked_as_country": "ZZ", "hijacked_as_descr": "", "hijacked_as_admin": "",
        "hijacker_as": "['64513']", "hijacker_as_name": "", "hijacker_as_org": "",
        "hijacker_as_country": "ZZ", "hijacker_as_descr": "", "hijacker_as_admin": "",
        "is_sub_hijack": True, "filter_reason": "possible hijack",
        "s_time": "2026-02-01 00:00:00", "e_time": None, "duration": None,
        "sub_hijack_level": "middle", "level_info": "合成等级说明",
        "event_info": "合成子前缀检测记录，不是攻击确认",
    }
    reference = "sub_hijack/2026-02-01 00:00:00/192.0.2.0-25/7/r"
    result = convert_anomaly_record(reference, [row], source=source, data_profile=data_profile)
    record = result["record"]
    assert record["association"]["state"] == "matched"
    assert record["identity"]["legacy_reference"] == reference
    assert record["association"]["locators"] == [{
        "source_instance": "synthetic-fixture-db", "source_table": "sub_hijack_202602",
        "key": {"source": "r", "prefix": "192.0.2.0/25", "sub_hijack_eventid": 7},
    }]
    assert record["raw_fields"] == row
    assert record["common"]["object"] == {"kind": "prefix", "value": "192.0.2.0/25"}
    assert record["common"]["end_time"]["state"] == "unknown"
    assert record["common"]["level_description"]["value"] == "合成等级说明"
    assert record["details"]["hijacked_prefix"]["value"] == "192.0.2.0/24"
    assert record["details"]["hijacked_as"]["value"] == "['64512']"
    assert record["details"]["hijacker_as"]["value"] == "['64513']"
    assert not any("path" in field for field in record["details"])
    assert {"recorded_roles_not_causal", "detector_flag_not_independent_confirmation",
            "sub_hijack_path_evidence_not_available"} <= set(record["limitations"])
    assert deserialize_record(serialize_record(result)) == result


@pytest.fixture
def hijack_row():
    return {
        "source": "r", "prefix": "192.0.2.0/24", "hijack_eventid": 7,
        "hijacked_as": "64512", "hijacked_as_name": "合成原角色名称",
        "hijacked_as_org": "合成组织甲", "hijacked_as_country": "ZZ",
        "hijacked_as_descr": "合成说明甲", "hijacked_as_admin": "合成管理字段甲",
        "hijacker_as": "64513", "hijacker_as_name": "合成新增角色名称",
        "hijacker_as_org": "合成组织乙", "hijacker_as_country": "ZZ",
        "hijacker_as_descr": "合成说明乙", "hijacker_as_admin": "合成管理字段乙",
        "s_time": "2026-02-01 00:00:00", "e_time": None, "duration": None,
        "end_as": None, "is_hijack": True, "filter_reason": "possible hijack",
        "pre_vp_paths": {"2026-01-31 23:55:00": ["64500 64512"]},
        "eve_vp_paths": {"2026-02-01 00:00:00": ["64500 64513"]},
        "next_vp_paths": None, "hijack_level": "middle",
        "hijack_level_info": "合成等级说明", "event_info": "合成检测记录，不是责任结论",
    }


def test_hijack_record_preserves_complete_detail_and_uncertain_semantics(
    source, hijack_row, data_profile,
):
    from data_pipeline.common.event_records import serialize_record, deserialize_record
    source["table"] = "hijack_202602"
    reference = "hijack/2026-02-01 00:00:00/192.0.2.0-24/7/r"
    result = convert_anomaly_record(reference, [hijack_row], source=source, data_profile=data_profile)
    record = result["record"]
    assert record["association"]["state"] == "matched"
    assert record["association"]["locators"] == [{
        "source_instance": "synthetic-fixture-db", "source_table": "hijack_202602",
        "key": {"source": "r", "prefix": "192.0.2.0/24", "hijack_eventid": 7},
    }]
    assert record["identity"]["legacy_reference"] == reference
    assert record["raw_fields"] == hijack_row
    assert record["common"]["kind"] == "hijack"
    assert record["common"]["start_time"]["value"] == "2026-01-31T16:00:00Z"
    assert record["common"]["end_time"]["state"] == "unknown"
    assert record["common"]["duration"] == {"state": "unknown", "value": None}
    assert record["common"]["level"] == {"state": "recorded", "value": "middle"}
    assert record["common"]["level_description"]["value"] == "合成等级说明"
    assert record["details"]["is_hijack"] == {"state": "recorded", "value": True}
    assert record["details"]["hijacked_as"]["value"] == "64512"
    assert record["details"]["hijacker_as"]["value"] == "64513"
    assert record["details"]["next_vp_paths"] == {"state": "unknown", "value": None}
    assert {"recorded_roles_not_causal", "detector_flag_not_independent_confirmation",
            "path_times_not_inferred_from_event_time", "formal_admission_not_established"} <= set(record["limitations"])
    assert deserialize_record(serialize_record(result)) == result


@pytest.mark.parametrize("case,expected", [
    ("missing", "missing"), ("multiple", "ambiguous"),
    ("missing_id", "conflict"), ("other_source", "conflict"),
    ("other_prefix", "conflict"), ("other_start", "conflict"),
])
def test_hijack_unconfirmed_candidates_never_become_matched(
    source, hijack_row, data_profile, case, expected,
):
    source["table"] = "hijack_202602"
    rows = [hijack_row]
    if case == "missing":
        rows = []
    elif case == "multiple":
        rows = [hijack_row, {**hijack_row, "hijacker_as": "64514"}]
    elif case == "missing_id":
        del hijack_row["hijack_eventid"]
    elif case == "other_source":
        hijack_row["source"] = "x"
    elif case == "other_prefix":
        hijack_row["prefix"] = "198.51.100.0/24"
    else:
        hijack_row["s_time"] = "2026-02-01 00:01:00"
    result = convert_anomaly_record(
        "hijack/2026-02-01 00:00:00/192.0.2.0-24/7/r", rows,
        source=source, data_profile=data_profile,
    )
    record = result["record"]
    assert record["association"]["state"] == expected
    assert record["raw_fields"] is record["common"] is record["details"] is None


@pytest.mark.parametrize("flag", [True, False, None])
def test_hijack_flag_and_empty_paths_are_preserved_without_confirmation(
    source, hijack_row, data_profile, flag,
):
    source["table"] = "hijack_202602"
    hijack_row.update(is_hijack=flag, eve_vp_paths={}, next_vp_paths=[])
    result = convert_anomaly_record(
        "hijack/2026-02-01 00:00:00/192.0.2.0-24/7/r", [hijack_row],
        source=source, data_profile=data_profile,
    )
    assert result["record"]["raw_fields"] == hijack_row
    assert result["record"]["details"]["is_hijack"] == {
        "state": "unknown" if flag is None else "recorded", "value": flag,
    }
    assert result["record"]["details"]["eve_vp_paths"] == {"state": "recorded", "value": {}}
    assert result["record"]["details"]["next_vp_paths"] == {"state": "recorded", "value": []}
    assert "detector_flag_not_independent_confirmation" in result["record"]["limitations"]


def test_hijack_ipv6_object_does_not_require_role_asn_in_the_source_key(
    source, hijack_row, data_profile,
):
    source["table"] = "hijack_202602"
    hijack_row.update(prefix="2001:db8::/32", hijacked_as=None, hijacker_as="")
    record = convert_anomaly_record(
        "hijack/2026-02-01 00:00:00/2001:db8::-32/7/r", [hijack_row],
        source=source, data_profile=data_profile,
    )["record"]
    assert record["association"]["state"] == "matched"
    assert record["common"]["object"] == {"kind": "prefix", "value": "2001:db8::/32"}
    assert record["details"]["hijacked_as"] == {"state": "unknown", "value": None}
    assert record["details"]["hijacker_as"] == {"state": "unknown", "value": ""}


@pytest.mark.parametrize("fraction", ["1", "12", "123", "1234", "12345", "123456"])
def test_read_receipt_fraction_precision_preserves_original_and_record_identity(
    source, prefix_row, data_profile, fraction,
):
    from data_pipeline.common.event_records import serialize_record, deserialize_record
    original = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    source["read_at"] = f"2026-09-10T09:00:00.{fraction}+00:00"
    reread = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    assert reread["read_at"] == source["read_at"]
    assert reread["record"] == original["record"]
    assert reread["content_version"] == original["content_version"]
    assert deserialize_record(serialize_record(reread)) == reread


def test_prefix_record_preserves_existing_identity_and_complete_source_key(
    source, prefix_row, data_profile,
):
    result = convert_anomaly_record(
        REFERENCE, [prefix_row], source=source, data_profile=data_profile,
        existing_id="fixture-existing-incident",
    )
    record = result["record"]
    assert record["identity"] == {
        "source_instance": "synthetic-fixture-db",
        "legacy_reference": REFERENCE,
        "legacy_id": "fixture-existing-incident",
    }
    assert record["association"]["state"] == "matched"
    assert record["association"]["locators"] == [{
        "source_instance": "synthetic-fixture-db",
        "source_table": "prefix_outage_202602",
        "key": {"source": "r", "prefix": "192.0.2.0/24", "outage_id": 7, "asn": "64512"},
    }]
    assert record["raw_fields"] == prefix_row
    assert record["common"]["object"] == {"kind": "prefix", "value": "192.0.2.0/24"}
    assert record["details"]["asn"] == {"state": "recorded", "value": "64512"}
    assert "causal_conclusion_not_supported" in record["limitations"]


@pytest.mark.parametrize("case,expected", [
    ("missing", "missing"),
    ("ambiguous", "ambiguous"),
    ("wrong_source", "conflict"),
    ("incomplete_key", "conflict"),
    ("wrong_table", "conflict"),
])
def test_unconfirmed_detail_never_selects_a_candidate(
    source, prefix_row, data_profile, case, expected,
):
    rows = [prefix_row]
    if case == "missing":
        rows = []
    elif case == "ambiguous":
        rows.append({**prefix_row, "asn": "64513"})
    elif case == "wrong_source":
        prefix_row["source"] = "x"
    elif case == "incomplete_key":
        del prefix_row["asn"]
    elif case == "wrong_table":
        source["table"] = "prefix_outage_202603"
    record = convert_anomaly_record(
        REFERENCE, rows, source=source, data_profile=data_profile,
    )["record"]
    assert record["association"]["state"] == expected
    assert record["identity"]["legacy_reference"] == REFERENCE
    assert record["raw_fields"] is None
    assert record["common"] is None
    assert record["details"] is None


@pytest.mark.parametrize("case", ["no_instance", "no_read_time", "naive_read_time", "bad_zone", "naive_scope", "unknown_scope", "invalid_reference", "unsupported_type"])
def test_incomplete_input_context_fails_explicitly(source, prefix_row, data_profile, case):
    reference = REFERENCE
    if case == "no_instance":
        source["instance"] = ""
    elif case == "no_read_time":
        del source["read_at"]
    elif case == "naive_read_time":
        source["read_at"] = "2026-09-10 09:00:00"
    elif case == "bad_zone":
        data_profile["timezone"] = "invalid-zone"
    elif case == "naive_scope":
        source["read_scope"]["start"] = "2026-02-01 00:00:00"
    elif case == "unknown_scope":
        source["read_scope"] = None
    elif case == "invalid_reference":
        reference = "missing-reference-parts"
    else:
        reference = REFERENCE.replace("prefix_outage/", "unsupported_event/")
    with pytest.raises(ValueError):
        convert_anomaly_record(reference, [prefix_row], source=source, data_profile=data_profile)


@pytest.mark.parametrize("end_time", ["invalid", "2026-01-31 23:59:59", "2026-02-01T00:03:00+25:00"])
def test_invalid_or_conflicting_end_time_does_not_become_a_recorded_recovery(
    source, prefix_row, data_profile, end_time,
):
    prefix_row["e_time"] = end_time
    result = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    assert result["record"]["raw_fields"]["e_time"] == end_time
    assert result["record"]["common"]["end_time"]["state"] == "unknown"
    assert result["record"]["common"]["end_time"]["value"] is None
    assert "end_time_unconfirmed" in result["record"]["limitations"]


def test_as_outage_keeps_its_list_and_stored_statistics_without_claiming_population(
    source, data_profile,
):
    source["table"] = "as_outage_202602"
    row = {
        "source": "r", "asn": "64512", "outage_id": 7,
        "s_time": "2026-02-01 00:00:00", "e_time": None, "duration": None,
        "outage_level": "middle", "outage_level_descr": "合成中等级",
        "event_info": "合成 AS 中断记录",
        "outage_prefixes": ["192.0.2.0/24", "2001:db8::/32"],
        "max_outage_prefix_num": 9, "total_prefix_num": 20,
        "max_outage_prefix_ratio": "0.450",
    }
    record = convert_anomaly_record(
        "as_outage/2026-02-01 00:00:00/64512/7/r", [row],
        source=source, data_profile=data_profile,
    )["record"]
    assert record["association"]["state"] == "matched"
    assert record["association"]["locators"][0]["key"] == {
        "source": "r", "asn": "64512", "outage_id": 7,
    }
    assert record["common"]["object"] == {"kind": "asn", "value": "64512"}
    assert record["details"]["outage_prefixes"] == {
        "state": "recorded", "value": ["192.0.2.0/24", "2001:db8::/32"],
    }
    assert record["details"]["max_outage_prefix_num"]["value"] == 9
    assert record["details"]["max_outage_prefix_ratio"]["value"] == "0.450"
    assert "affected_population_not_established" in record["limitations"]
    assert "stored_ratio_not_authoritative" in record["limitations"]


def test_leak_preserves_recorded_roles_and_explicitly_lacks_lifecycle(source, data_profile):
    source["table"] = "leak_event_202602"
    row = {
        "source": "r", "prefix": "2001:db8::/32", "leak_event_id": 4,
        "s_time": "2026-02-01 00:00:00", "prefix_ori_as": "64512",
        "leak_by": "64513", "leak_to": "64514", "as_path": "64514 64513 64512",
        "leak_level": "middle", "leak_level_info": "合成泄漏等级",
    }
    record = convert_anomaly_record(
        "leak/2026-02-01 00:00:00/2001:db8::-32/4/r", [row],
        source=source, data_profile=data_profile,
    )["record"]
    assert record["association"]["state"] == "matched"
    assert record["association"]["locators"][0]["key"] == {
        "source": "r", "prefix": "2001:db8::/32", "leak_event_id": 4,
    }
    assert record["details"]["leak_by"] == {"state": "recorded", "value": "64513"}
    assert record["details"]["as_path"]["value"] == "64514 64513 64512"
    assert record["common"]["end_time"] == {"state": "unavailable", "value": None}
    assert record["common"]["duration"] == {"state": "unavailable", "value": None}
    assert "leak_lifecycle_not_supported" in record["limitations"]
    assert "attacker_as" not in record["details"]


def test_common_fields_keep_raw_time_and_use_the_supplied_project_timezone(
    source, prefix_row, data_profile,
):
    prefix_row["e_time"] = "2026-02-01 00:03:00"
    prefix_row["duration"] = "0:03:00"
    result = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    record = result["record"]
    assert record["common"]["start_time"] == {
        "state": "recorded", "value": "2026-01-31T16:00:00Z", "precision": "second",
    }
    assert record["common"]["end_time"]["value"] == "2026-01-31T16:03:00Z"
    assert record["common"]["level"] == {"state": "recorded", "value": "high"}
    assert record["common"]["level_description"]["value"] == "合成高等级"
    assert record["common"]["summary"]["value"] == "合成前缀中断记录"
    assert record["raw_fields"]["s_time"] == "2026-02-01 00:00:00"
    assert record["data_profile"] == data_profile
    assert result["read_at"] == "2026-09-10T09:00:00Z"
    assert "read_at" not in record["source"]
    assert "detected_at" not in record["common"]


@pytest.mark.parametrize("value", [None, [], ["64500 64512"]])
def test_path_fields_preserve_unknown_empty_and_nonempty_values(
    source, prefix_row, data_profile, value,
):
    prefix_row["next_vp_paths"] = value
    result = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    assert result["record"]["details"]["next_vp_paths"] == {
        "state": "unknown" if value is None else "recorded", "value": value,
    }
    assert "path_times_not_inferred_from_event_time" in result["record"]["limitations"]


def test_content_revision_changes_version_not_identity_or_prior_output(
    source, prefix_row, data_profile,
):
    first = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    repeated = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    assert repeated == first
    prefix_row["e_time"] = "2026-02-01 00:03:00"
    prefix_row["outage_level"] = "middle"
    revised = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    assert revised["record"]["identity"] == first["record"]["identity"]
    assert revised["content_version"] != first["content_version"]
    assert first["record"]["raw_fields"]["outage_level"] == "high"
    assert first["record"]["raw_fields"]["e_time"] is None
    source["read_at"] = "2026-09-10T10:00:00Z"
    reread = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    assert reread["content_version"] == revised["content_version"]
    assert reread["read_at"] != revised["read_at"]
    source["instance"] = "another-fixture-db"
    other = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    assert other["record"]["identity"] != reread["record"]["identity"]


def test_saved_old_version_can_be_read_after_creating_a_new_version(
    source, prefix_row, data_profile, tmp_path,
):
    from data_pipeline.common.event_records import serialize_record, deserialize_record

    first = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    old_file = tmp_path / "first.json"
    old_file.write_bytes(serialize_record(first))
    prefix_row["outage_level"] = "low"
    prefix_row["e_time"] = "2026-02-01 00:05:00"
    revised = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    new_file = tmp_path / "revised.json"
    new_file.write_bytes(serialize_record(revised))
    old = deserialize_record(old_file.read_bytes())
    new = deserialize_record(new_file.read_bytes())
    assert old == first
    assert new == revised
    assert old["record"]["common"]["level"]["value"] == "high"
    assert old["record"]["common"]["end_time"]["state"] == "unknown"
    assert new["record"]["common"]["level"]["value"] == "low"
    assert old["content_version"] != new["content_version"]
    assert old["record"]["identity"] == new["record"]["identity"]


def test_saved_version_rejects_modified_content_and_unsupported_format(
    source, prefix_row, data_profile,
):
    from data_pipeline.common.event_records import serialize_record, deserialize_record

    result = convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)
    payload = serialize_record(result)
    with pytest.raises(ValueError, match="内容版本"):
        deserialize_record(payload.replace(b'"high"', b'"low"'))
    result["record"]["raw_fields"]["outage_level"] = "low"
    with pytest.raises(ValueError, match="内容版本"):
        serialize_record(result)
    with pytest.raises(ValueError, match="格式"):
        deserialize_record(b'[]')


def test_only_selected_source_fields_are_retained_without_promoting_missing_provenance(
    source, prefix_row, data_profile,
):
    prefix_row["judge_userid"] = "synthetic-private-field"
    prefix_row["attacker_as"] = "synthetic-display-label"
    source["unrelated_runtime_value"] = "synthetic-unrelated-value"
    record = convert_anomaly_record(
        REFERENCE, [prefix_row], source=source, data_profile=data_profile,
    )["record"]
    assert "judge_userid" not in record["raw_fields"]
    assert "attacker_as" not in record["raw_fields"]
    assert "unrelated_runtime_value" not in record["source"]
    assert record["source"]["collector_id"] is None
    assert record["source"]["content_version"] is None
    assert record["source"]["detector_version"] is None
    assert {"collector_unknown", "source_content_version_unknown", "detector_version_unknown"} <= set(record["limitations"])
    assert "formal_admission_not_established" in record["limitations"]


@pytest.mark.parametrize("case,reason", [
    ("source_scope", "source_scope_conflict"),
    ("read_window", "outside_read_scope"),
    ("profile_window", "outside_data_profile"),
    ("processed_detail", "source_values_not_available"),
    ("changed_start", "reference_conflict"),
])
def test_scope_or_identity_conflicts_cannot_be_silently_admitted(
    source, prefix_row, data_profile, case, reason,
):
    if case == "source_scope":
        source["read_scope"]["source"] = "x"
    elif case == "read_window":
        source["read_scope"]["end_exclusive"] = "2026-02-01T00:00:00+08:00"
        source["read_scope"]["start"] = "2026-01-31T00:00:00+08:00"
    elif case == "profile_window":
        data_profile["window_start"] = "2026-02-02T00:00:00+08:00"
    elif case == "processed_detail":
        source["representation"] = "processed_detail"
    else:
        prefix_row["s_time"] = "2026-02-01 00:00:01"
    record = convert_anomaly_record(
        REFERENCE, [prefix_row], source=source, data_profile=data_profile,
    )["record"]
    assert record["association"]["state"] == "conflict"
    assert reason in record["association"]["reasons"]
    assert record["identity"]["legacy_reference"] == REFERENCE
    assert record["details"] is None


def test_native_source_times_intervals_and_decimal_values_survive_saved_roundtrip(
    source, data_profile, tmp_path,
):
    from data_pipeline.common.event_records import serialize_record, deserialize_record

    source["table"] = "as_outage_202602"
    row = {
        "source": "r", "asn": "64512", "outage_id": 7,
        "s_time": datetime(2026, 2, 1),
        "e_time": datetime(2026, 2, 1, 0, 3),
        "duration": timedelta(minutes=3),
        "outage_level": "middle", "max_outage_prefix_ratio": Decimal("0.450"),
        "outage_prefixes": [],
    }
    original = convert_anomaly_record(
        "as_outage/2026-02-01 00:00:00/64512/7/r", [row],
        source=source, data_profile=data_profile,
    )
    assert original["record"]["association"]["state"] == "matched"
    assert original["record"]["common"]["start_time"]["value"] == "2026-01-31T16:00:00Z"
    saved = tmp_path / "native-values.json"
    saved.write_bytes(serialize_record(original))
    restored = deserialize_record(saved.read_bytes())
    assert restored == original
    assert restored["record"]["raw_fields"] == row
    assert restored["record"]["details"]["max_outage_prefix_ratio"]["value"].as_tuple() == Decimal("0.450").as_tuple()


def test_equivalent_prefix_spelling_and_explicit_utc_scope_keep_source_values(
    source, prefix_row, data_profile,
):
    prefix_row["prefix"] = "2001:0db8:0000:0000:0000:0000:0000:0000/32"
    source["read_scope"]["start"] = "2026-01-31T16:00:00Z"
    source["read_scope"]["end_exclusive"] = "2026-02-01T16:00:00Z"
    reference = "prefix_outage/2026-02-01 00:00:00/2001:db8::-32/7/r"
    record = convert_anomaly_record(
        reference, [prefix_row], source=source, data_profile=data_profile,
    )["record"]
    assert record["association"]["state"] == "matched"
    assert record["common"]["object"] == {"kind": "prefix", "value": "2001:db8::/32"}
    assert record["raw_fields"]["prefix"] == prefix_row["prefix"]
    assert record["identity"]["legacy_reference"] == reference


@pytest.mark.parametrize("value,state,precision,digits", [
    ("2026-02-01", "unknown", None, None),
    ("2026-02-01 00:03:00.000000", "recorded", "fractional_second", 6),
    ("2026-02-01 00:03:00.120", "recorded", "fractional_second", 3),
    ("2026-02-01 00:03:00.1234567", "unknown", None, None),
    (datetime(2026, 2, 1, 0, 3), "recorded", "unknown", None),
])
def test_time_precision_is_not_invented_or_silently_truncated(
    source, prefix_row, data_profile, value, state, precision, digits,
):
    prefix_row["e_time"] = value
    record = convert_anomaly_record(
        REFERENCE, [prefix_row], source=source, data_profile=data_profile,
    )["record"]
    end = record["common"]["end_time"]
    assert record["raw_fields"]["e_time"] == value
    assert end["state"] == state
    assert end["precision"] == precision
    assert end.get("fractional_digits") == digits


@pytest.mark.parametrize("case", ["source", "profile", "reference", "candidates", "row"])
def test_malformed_input_shapes_fail_at_the_public_boundary(
    source, prefix_row, data_profile, case,
):
    reference, rows = REFERENCE, [prefix_row]
    if case == "source":
        source = None
    elif case == "profile":
        data_profile = None
    elif case == "reference":
        reference = None
    elif case == "candidates":
        rows = None
    else:
        rows = [None]
    with pytest.raises(ValueError):
        convert_anomaly_record(reference, rows, source=source, data_profile=data_profile)


@pytest.mark.parametrize("value", [("path",), {1: "path"}, {"$anomaly_scalar": "reserved"}])
def test_source_values_that_cannot_roundtrip_losslessly_are_rejected(
    source, prefix_row, data_profile, value,
):
    prefix_row["next_vp_paths"] = value
    with pytest.raises(ValueError):
        convert_anomaly_record(REFERENCE, [prefix_row], source=source, data_profile=data_profile)


@pytest.mark.parametrize("payload", [
    b'{"$anomaly_scalar":"decimal","value":"not-a-number"}',
    b'{"$anomaly_scalar":"timedelta_us","value":99999999999999999999999999999999}',
])
def test_invalid_saved_scalar_has_an_explicit_decode_failure(payload):
    from data_pipeline.common.event_records import deserialize_record

    with pytest.raises(ValueError):
        deserialize_record(payload)


@pytest.mark.parametrize("start,end", [
    ("2026-02-01T00:00:00+08:00", "2026-02-01T00:03:00+08:00"),
    ("2026-01-31T16:00:00Z", "2026-01-31T16:03:00Z"),
])
def test_explicit_source_offsets_are_compared_as_instants(
    source, prefix_row, data_profile, start, end,
):
    prefix_row.update(s_time=start, e_time=end)
    record = convert_anomaly_record(
        REFERENCE, [prefix_row], source=source, data_profile=data_profile,
    )["record"]
    assert record["association"]["state"] == "matched"
    assert record["common"]["start_time"]["value"] == "2026-01-31T16:00:00Z"
    assert record["common"]["end_time"]["value"] == "2026-01-31T16:03:00Z"
    assert record["raw_fields"]["s_time"] == start
    assert record["raw_fields"]["e_time"] == end
    assert record["data_profile"]["timezone"] == "Asia/Shanghai"


def test_explicit_offset_does_not_hide_a_different_start_instant(
    source, prefix_row, data_profile,
):
    prefix_row["s_time"] = "2026-02-01T00:00:00Z"
    record = convert_anomaly_record(
        REFERENCE, [prefix_row], source=source, data_profile=data_profile,
    )["record"]
    assert record["association"]["state"] == "conflict"
    assert "reference_conflict" in record["association"]["reasons"]
    assert record["details"] is None
