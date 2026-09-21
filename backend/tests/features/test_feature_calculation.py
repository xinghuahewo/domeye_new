"""静态源码特征化 fixture；不导入旧项目或读取真实初态。"""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

import pytest

from data_pipeline.analysis.features.calculation import RULES, Diagnostic, FeatureCalculationError, FileWindow, Observation, Projection, Reference, ReferenceRow, Values, _warnings, calculate_file, feature_origin, initialize, prefix_filter

T = datetime(2026, 2, 28, 15, 55, tzinfo=timezone.utc)
REF = Reference("fixture-reference-v1", {"1": "伊朗", "2": "美国", "3": "伊朗", "4": "", "5": None},
                {"1": "IR", "2": "US", "3": "IR"}, {"美国": "US"})


def projection(as_prefix, mode="ordinary", version="p0", paths=None, origins=None):
    return Projection(version, RULES[mode], paths or {}, origins or {},
                      {a: frozenset(p) for a, p in as_prefix.items()}, frozenset({"100", "200"}),
                      "fixture-rib", "2026-02-28 23:50:00", "IR" if mode == "ir" else None, "complete")


def window(n=0, coverage="complete"):
    return FileWindow("fixture-input-v1", f"file-{n}", T + timedelta(minutes=n*5),
                      T + timedelta(minutes=(n+1)*5), T + timedelta(minutes=n*5), coverage)


def event(seq=0, flag="A", path="100 1", old="", prefix="10.0.0.0/24", mode="ordinary", skip=None, n=0):
    return Observation(f"file-{n}:record-{seq}:element-0", seq, window(n).start, flag, prefix,
                       "100", path, old, frozenset({"1"}), frozenset({"1", "2"}), RULES[mode], skip)


def row(result, scope, subject):
    return next(x for x in result.rows if x.scope == scope and x.subject == subject)


def test_initial_two_files_and_next_window_sparse_reset_and_legacy_time():
    p = projection({"1": {"10.0.0.0/24"}, "2": {"11.0.0.0/24"}})
    state = initialize("ordinary", p, REF, "execution-a")
    assert state.feature_dict["伊朗"]["1"].v4IP_num == 0
    assert not state.feature_dict["伊朗"]["1"].is_change
    first = calculate_file(state, REF, window(), (event(), event(1)), replace(p, version="p1"))
    assert row(first, "asn", "1").values == Values(1, 0, 256, 2, 0)
    assert first.end_state.feature_dict["伊朗"]["1"].is_change
    assert first.end_state.feature_dict["伊朗"]["1"].announ_num == 2
    assert first.next_state.feature_dict["伊朗"]["1"].announ_num == 0
    assert first.sparse_asns == (("美国", "2"),)
    assert row(first, "collect", "collect").ipv4_prefixes == {"10.0.0.0/24", "11.0.0.0/24"}
    second = calculate_file(first.next_state, REF, window(1), (event(path="100 2", n=1),), replace(p, version="p2"))
    usa = row(second, "asn", "2")
    assert usa.legacy_table == "feature_US_202603"
    assert usa.legacy_payload(window(1), "r")["t"] == "2026-03-01 00:00:00"
    assert second.next_state.t == "2026-03-01 00:00:00"
    third = calculate_file(second.next_state, REF, window(2), (), replace(p, version="p3"))
    assert all(x.scope != "asn" for x in third.rows)
    assert len(third.rows) == 3
    assert row(third, "country", "伊朗").values == Values(1, 0, 256, 0, 0)
    assert third.next_state.feature_dict["伊朗"]["1"].v4IP_num == 256
    assert state.completed_files == 0 and state.feature_collect_dict.announ_num == 0
    exported = third.next_state.export()
    json.dumps(exported)
    assert exported["projection"]["seen_vps"] == ["100", "200"]
    assert exported["last_completed_update_file"] == "file-2"


def test_replacement_preserves_old_origin_residue_and_only_new_origin_dirty():
    p = projection({"1": {"10.0.0.0/24"}}, paths={"10.0.0.0/24": {"100": "100 1"}})
    end = projection({"1": {"10.0.0.0/24"}, "2": {"10.0.0.0/24"}}, version="p1",
                     paths={"10.0.0.0/24": {"100": "100 2"}}, origins={"10.0.0.0/24": frozenset({"1", "2"})})
    result = calculate_file(initialize("ordinary", p, REF, "e"), REF, window(), (event(path="100 2"),), end)
    assert result.sparse_asns == (("伊朗", "1"),)
    assert row(result, "collect", "collect").values.v4Prefix_num == 1
    assert result.next_state.projection.prefix_as["10.0.0.0/24"] == {"1", "2"}
    assert result.next_state.feature_dict["伊朗"]["1"].v4Prefix_num == 0  # 初始化数值仍未更新
    assert row(result, "country", "伊朗").values.v4Prefix_num == 1


def test_unknown_withdraw_and_known_withdraw_uses_old_vp_path():
    p = projection({"1": {"10.0.0.0/24"}})
    end = projection({"1": set()}, version="p1")
    result = calculate_file(initialize("ordinary", p, REF, "e"), REF, window(),
                            (event(flag="W", old="100 1", path="100 2"), event(1, flag="W", old="")), end)
    assert row(result, "collect", "collect").values.withdraw_num == 2
    assert row(result, "asn", "1").values == Values(0, 0, 0, 0, 1)
    assert row(result, "asn", "1").resource_status == "observed_zero"
    assert row(result, "country", "伊朗").values.withdraw_num == 1


def test_cross_vp_overlap_and_equivalent_blocks_independent_expected_sets():
    prefixes = {"10.0.0.0/23", "10.0.0.128/25", "10.0.1.0/24", "2001:db8::/47", "2001:db8:1::/49"}
    p = projection({"1": prefixes, "3": {"10.0.1.0/24"}})
    result = calculate_file(initialize("ordinary", p, REF, "e"), REF, window(), (event(),), replace(p, version="p1"))
    actual = row(result, "collect", "collect")
    expected_v4_blocks = {"10.0.0", "10.0.1"}
    expected_v6_blocks = {"2001:db8:0", "2001:db8:1"}
    assert actual.values == Values(len(expected_v4_blocks), len(expected_v6_blocks), 512, 1, 0)
    assert actual.raw_prefixes == prefixes
    assert len(actual.ipv4_prefixes) == 3  # CIDR数不是/24等效量
    one = projection({"1": {"10.0.0.128/25", "2001:db8::/49"}})
    small = calculate_file(initialize("ordinary", one, REF, "e"), REF, window(), (), replace(one, version="p1"))
    assert row(small, "collect", "collect").values == Values(1, 1, 256, 0, 0)


@pytest.mark.parametrize("path,expected", [(None, None), ("", None), ("1 64512", "1"), ("1 4294967296", "1"),
                                                   ("1 4200000000", "4200000000"), ("1 {2}", None),
                                                   ("1 {2,3}", None), ("64512", None), ("1 1", "1")])
def test_feature_specific_origin(path, expected):
    assert feature_origin(path) == expected


@pytest.mark.parametrize("path", ["1  ", "1 _", "(1)"])
def test_legacy_path_error_is_visible_failure_without_mutating_work(path):
    p = projection({})
    state = initialize("ordinary", p, REF, "e")
    before = state.export()
    with pytest.raises(FeatureCalculationError, match="record-0"):
        calculate_file(state, REF, window(), (event(path=path),), replace(p, version="p1"))
    assert state.export() == before


def test_unknown_country_single_space_and_empty_reference_values():
    p = projection({"4": {"10.0.0.0/24"}, "5": set(), "9": set()})
    result = calculate_file(initialize("ordinary", p, REF, "e"), REF, window(), (event(path="100 4"),), replace(p, version="p1"))
    assert row(result, "asn", "4").legacy_payload(window(), "r")["country"] == " "
    assert row(result, "country", "未知").country == "未知"
    assert result.sparse_asns == (("未知", "5"), ("未知", "9"))


def test_filtering_before_projection_and_aggregate_manual_exclusions_not_enabled():
    p = projection({"1": {"0.0.0.0/7", "::/15", "invalid", "120.0.0.0/11", "10.0.0.1/24"}})
    events = (event(prefix="0.0.0.0/7", skip="oversized_ipv4_prefix"),
              event(1, prefix="::/15", skip="oversized_ipv6_prefix"),
              event(2, prefix="bad", skip="invalid_prefix"), event(3, flag="STATE", skip="state_record"))
    result = calculate_file(initialize("ordinary", p, REF, "e"), REF, window(), events, replace(p, version="p1"))
    assert row(result, "collect", "collect").values.announ_num == 0
    assert row(result, "collect", "collect").ipv4_prefixes == {"120.0.0.0/11", "10.0.0.0/24"}
    assert len(result.diagnostics) == 7
    assert prefix_filter("173.0.0.0/11")[1] is None
    with pytest.raises(FeatureCalculationError, match="过滤"):
        calculate_file(initialize("ordinary", p, REF, "e"), REF, window(), (event(prefix="0.0.0.0/7"),), p)


def test_ir_cross_file_non_ir_announcement_keeps_ir_path_then_withdraw():
    p = projection({"1": {"10.0.0.0/24"}}, mode="ir", paths={"10.0.0.0/24": {"100": "100 1"}})
    state = initialize("ir", p, REF, "e")
    first = calculate_file(state, REF, window(),
                           (event(mode="ir", path="100 2", skip="non_ir_announcement"),), replace(p, version="p1"))
    assert first.next_state.projection.prefix_dict == p.prefix_dict
    assert row(first, "collect", "collect").values == Values(1, 0, 256, 0, 0)
    end = projection({"1": set()}, mode="ir", version="p2")
    second = calculate_file(first.next_state, REF, window(1),
                            (event(mode="ir", flag="W", old="100 1", n=1),
                             event(1, mode="ir", flag="W", old="", n=1)), end)
    assert row(second, "collect", "collect").values.withdraw_num == 2
    intermediate = row(second, "asn", "1")
    assert intermediate.row_presence == "ir_asn_write_disabled" and intermediate.legacy_table is None
    assert intermediate.values == Values(0, 0, 0, 0, 1)
    assert second.next_state.feature_dict["伊朗"]["1"].withdraw_num == 0
    with pytest.raises(FeatureCalculationError):
        initialize("ir", replace(p, country_filter=None), REF, "e")


def test_quality_order_identity_and_time_validation():
    p = projection({})
    state = initialize("ordinary", p, REF, "e")
    result = calculate_file(state, REF, window(coverage="unknown"), (), replace(p, version="p1"))
    assert result.quality == "unknown"
    assert row(result, "collect", "collect").resource_status == "unknown"
    for events in [(event(1), event(0)), (replace(event(), projection_rule_id=RULES["ir"]),)]:
        with pytest.raises(FeatureCalculationError):
            calculate_file(state, REF, window(), events, p)
    with pytest.raises(ValueError):
        replace(window(), file_time=T.replace(tzinfo=None))
    with pytest.raises(ValueError):
        replace(window(), file_time=T.replace(microsecond=1))
    with pytest.raises(FeatureCalculationError):
        calculate_file(state, replace(REF, version="different"), window(), (), p)
    with pytest.raises(FeatureCalculationError):
        calculate_file(result.next_state, REF, window(), (), p)


def test_warning_threshold_and_sample_are_diagnostics_not_events():
    prefixes = {f"{n}.0.0.0/8" for n in range(20)}
    reasons = _warnings("collect", "collect", Values(2**24, 0, 2**32, 0, 0), prefixes)
    assert [d.reason for d in reasons] == ["v4prefix_num_reached_full_ipv4_c_segments", "v4ip_num_reached_full_ipv4_space"]
    assert reasons[0].sample_prefixes == tuple(sorted(prefixes)[:10])
    assert _warnings("asn", "1", Values(0, 0, 10**9, 0, 0), set()) == [
        Diagnostic("asn", "1", "v4ip_num_exceeds_suspicious_threshold")]


def test_reference_duplicate_first_empty_and_underscore_attribution():
    rows = (ReferenceRow("ref:0", "1", "伊朗", "IR"), ReferenceRow("ref:1", "1", "美国", "US"),
            ReferenceRow("ref:2", "2", "", ""), ReferenceRow("ref:3", "2", "美国", "US"),
            ReferenceRow("ref:4", "3", None, None))
    ref = Reference.from_rows("v1", rows, {})
    assert ref.country("1_suffix") == "伊朗" and ref.code("1_suffix") == "IR"
    assert ref.country("2") == "未知" and ref.country("3") == "未知"
    assert ref.source_rows == rows
    state = initialize("ordinary", projection({"1_suffix": {"10.0.0.0/24"}}), ref, "e")
    assert "1_suffix" in state.feature_dict["伊朗"]


def test_complete_file_does_not_establish_unknown_baseline_resource_zero():
    p = replace(projection({}), coverage="unknown")
    result = calculate_file(initialize("ordinary", p, REF, "e"), REF, window(), (), p)
    assert result.quality == "unknown"
    assert row(result, "collect", "collect").resource_status == "unknown"


def test_as_set_announce_counts_collect_and_deduplicates_normalized_skip_warnings():
    p = projection({"{1,2}": {"10.0.0.0/24", "0.0.0.0/7", "0.0.0.1/7"}})
    result = calculate_file(initialize("ordinary", p, REF, "e"), REF, window(),
                            (event(path="100 {1,2}"),), replace(p, version="p1"), monthly=False)
    assert result.sparse_asns == (("未知", "{1,2}"),)
    assert row(result, "collect", "collect").values == Values(1, 0, 256, 1, 0)
    assert row(result, "country", "未知").values.announ_num == 0
    assert len(result.diagnostics) == 1
