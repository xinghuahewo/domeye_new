"""国家 v2 固定旧行为；显式区分旧调用次数与真实时间槽。"""

from copy import deepcopy
import pytest
from data_pipeline.analysis.detection.country_outage import build_live_observation, new_runtime_state, reduce_live_observation, legacy_peak_projection, CountryOutageV2Error
from tests.detection.test_detection_computation import engine, withdraw, business


def observation(at, affected, normal=None, baseline=None):
    baseline = baseline or list(range(1, 101))
    if normal is None:
        normal = sorted(set(baseline) - set(affected))
    return build_live_observation(
        source="r",
        country_code="ZZ",
        observed_at_local=at,
        outage_asns=affected,
        normal_asns=normal,
        baseline_asns=baseline,
        collector_id="fixture-collector",
    )


def initial():
    return new_runtime_state(
        source="r",
        country_code="ZZ",
        collector_id="fixture-collector",
        baseline_asns=range(1, 101),
    )


def test_two_candidates_strict_threshold_and_second_initial_peak():
    state = initial()
    result = reduce_live_observation(
        state, observation("2026-02-28 00:00:00", [1, 2, 3])
    )
    assert result["state"]["candidate_observations"] == []  # exact .03
    result = reduce_live_observation(
        result["state"], observation("2026-02-28 00:00:01", list(range(1, 31)))
    )
    assert result["lifecycle_action"] == "none"
    result = reduce_live_observation(
        result["state"], observation("2026-02-28 07:42:17", [1, 2, 3, 4])
    )
    assert result["lifecycle_action"] == "started"  # no cadence check
    incident = result["state"]["incident"]
    assert incident["onset_at"] == "2026-02-27T16:00:01Z"
    assert incident["peak_at"] == "2026-02-27T23:42:17Z"
    assert (
        result["state"]["peak_observation"]["asn_state"]["affected_asn_ratio"] == 0.04
    )
    assert len(result["persist_observations"]) == 2
    assert incident["milestones"]["partial_recovery"] is None


def test_unknown_population_and_dynamic_outside_fixed_cohort():
    ob = observation("2026-02-28 00:00:00", [1, 101], normal=[2, 3])
    assert ob["cohort"]["dynamic_asns"] == [101]
    assert ob["asn_state"]["unknown_asns"] == list(range(4, 101))
    assert ob["asn_state"]["affected_asn_count"] is None
    assert ob["asn_state"]["affected_asn_ratio"] is None
    assert ob["prefix_vp"]["visible_count"] is None
    result = reduce_live_observation(initial(), ob)
    assert result["state"]["candidate_observations"] == []


def started():
    result = reduce_live_observation(
        initial(), observation("2026-02-28 00:00:00", [1, 2, 3, 4])
    )
    return reduce_live_observation(
        result["state"], observation("2026-02-28 00:05:00", [1, 2, 3, 4])
    )["state"]


def test_live_full_visible_asn_cannot_recover_without_prefix_vp():
    state = started()
    for i in range(8):
        result = reduce_live_observation(
            state, observation(f"2026-02-28 01:{i:02}:00", [])
        )
        state = result["state"]
        assert result["lifecycle_action"] != "fully_recovered"
    assert state["incident"]["recovery_state"] == "unknown"
    assert state["incident"]["full_recovery_at"] is None
    assert len(state["recent_observations"]) == 6
    projection = legacy_peak_projection(
        incident=state["incident"],
        peak_observation=state["peak_observation"],
        country_chinese_name="测试国",
        outage_level="low",
        outage_level_descr="fixture",
        outage_id=3,
    )
    assert projection["total_as_num"] == 100
    assert projection["max_outage_as_num"] == 4
    assert projection["outage_ases"] == [1, 2, 3, 4]
    assert projection["e_time"] is None and projection["duration"] is None
    assert projection["s_time"] == "2026-02-28 00:05:00"


def test_hypothetical_observed_recovery_then_repeat_retains_same_incident():
    # 仅用于旧 reducer 的不可达分支特征测试；不是 live 能力验收。
    state = started()
    ident = state["incident"]["incident_id"]
    for i in range(6):
        ob = observation(f"2026-02-28 01:{i:02}:00", [])
        ob["prefix_vp"].update(measurement_state="observed", visible_ratio=1.0)
        state = reduce_live_observation(state, ob)["state"]
    assert state["incident"]["recovery_state"] == "fully_recovered"
    assert state["incident"]["full_recovery_at"] == "2026-02-27T17:00:00Z"
    result = reduce_live_observation(
        state, observation("2026-02-28 02:00:00", list(range(1, 51)))
    )
    assert result["state"]["incident"]["incident_id"] == ident
    assert result["state"]["incident"]["recovery_state"] == "fully_recovered"
    assert result["lifecycle_action"] == "peak_updated"
    assert result["state"]["episode"]["ordinal"] == 1


def test_same_timestamp_calls_are_not_real_distinct_slots_and_input_immutable():
    state = initial()
    ob = observation("2026-02-28 00:00:00", [1, 2, 3, 4])
    saved = deepcopy(ob)
    first = reduce_live_observation(state, ob)
    second = reduce_live_observation(first["state"], ob)
    assert second["lifecycle_action"] == "started"
    assert ob == saved and state["incident"] is None
    assert state["candidate_observations"] == []


def test_country_input_rejections_and_peak_binding():
    with pytest.raises(CountryOutageV2Error):
        observation("2026-02-28 00:00:00", [1], normal=[1])
    with pytest.raises(CountryOutageV2Error):
        observation("2026-02-28 00:00:00", [True])
    state = started()
    peak = deepcopy(state["peak_observation"])
    peak["snapshot_id"] = "wrong"
    with pytest.raises(CountryOutageV2Error):
        legacy_peak_projection(
            incident=state["incident"],
            peak_observation=peak,
            country_chinese_name="测试国",
            outage_level="low",
            outage_level_descr="",
            outage_id=1,
        )


def test_detection_country_actual_dispatch_and_notification_intent_only():
    prefixes = ("10.1.0.0/24", "10.2.0.0/24")
    e = engine(prefixes, origins={prefixes[0]: "1", prefixes[1]: "2"})
    withdraw(e, prefixes[0], 1)
    assert e.outage.country_outage_v2_runtime["ZZ"]["incident"] is None
    withdraw(e, prefixes[1], 10)
    runtime = e.outage.country_outage_v2_runtime["ZZ"]
    assert runtime["incident"] is not None
    assert runtime["baseline_asns"] == [1, 2]
    assert business(e, "country_outage")[-1]["legacy"]["structured_v2"] is True
    assert any(row["kind"] == "send_outage_alert" for row in e.output.rows)
    assert any(row["kind"] == "persist_country_outage_v2" for row in e.output.rows)
    assert (
        runtime["peak_observation"]["prefix_vp"]["measurement_state"] == "unavailable"
    )
    assert e.finish_file()[-1]["kind"] == "file_finished"


def test_old_country_index_missing_argument_is_explicit_partial_projection():
    e = engine(
        ("10.1.0.0/24", "10.2.0.0/24"), origins={"10.1.0.0/24": "1", "10.2.0.0/24": "2"}
    )
    withdraw(e, "10.1.0.0/24", 1)
    withdraw(e, "10.2.0.0/24", 10)
    index = next(
        r
        for r in e.output.rows
        if r["kind"] == "event_start" and r["legacy"]["event_type"] == "国家中断"
    )
    assert "attacked_as" not in index["legacy"]
    assert index["projection_quality"] == "partial"
    assert e.export_state()["legacy_projection_quality"] == "partial"
    assert e.export_state()["publication_eligible"] is False
    assert e.outage.country_outage_v2_runtime["ZZ"]["baseline_asns"] == [1, 2]


def test_country_cross_month_preserves_start_reference_and_current_old_projection():
    from tests.detection.test_detection_computation import begin, send

    e = engine(
        ("10.1.0.0/24", "10.2.0.0/24", "10.3.0.0/24"),
        origins={"10.1.0.0/24": "1", "10.2.0.0/24": "2", "10.3.0.0/24": "3"},
    )
    withdraw(e, "10.1.0.0/24", 1)
    withdraw(e, "10.2.0.0/24", 10)
    first = business(e, "country_outage")[-1]
    e.finish_file()
    begin(e, "202603")
    for j in range(3):
        send(
            e,
            20 + j,
            action="W",
            p="10.3.0.0/24",
            vp=str(100 + j),
            path="",
            at="2026-03-01T00:00:00Z",
        )
    last = business(e, "country_outage")[-1]
    assert last["incident_id"] == first["incident_id"]
    assert last["legacy_ref"]["table"] == "country_outage_202602"
    assert last["legacy_ref"]["current_legacy_table"] == "country_outage_202603"
    assert (
        last["legacy"]["table"] == "country_outage_202603"
    )  # 旧v2投影实际使用当前表，原值保留
