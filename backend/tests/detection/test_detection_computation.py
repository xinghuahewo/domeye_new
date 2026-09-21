"""只用显式人工观察，覆盖实际旧计算和独立修正预期。"""

from dataclasses import replace
import json
import pytest
from data_pipeline.analysis.detection import DetectionEngine, DetectionInput, DetectionScope, DetectionSeed, FileBoundary, ReferenceBundle

P = "10.0.0.0/24"
KINDS = (
    "prefix_outage",
    "as_outage",
    "country_outage",
    "event",
    "moas",
    "hijack",
    "sub_hijack",
    "leak_phenomenon",
    "leak",
)


def refs(**changes):
    data = {
        k: {}
        for k in (
            "as_info",
            "prefix_info",
            "country",
            "important_as_dict",
            "important_prefix_dict",
            "as_prefix_dict",
            "as_rel_dict",
            "important_domain_dict",
            "private_as_dict",
            "triplet_info",
        )
    }
    data["as_info"] = {
        str(i): {
            "org_name": f"Org{i}",
            "org_name_cn": "",
            "as_country": "ZZ",
            "as_country_cn": "测试国",
            "as_name": f"AS{i}",
            "descr": "d",
            "descr_cn": "",
            "admin_info": "admin",
            "type": "ISP",
            "import_as": [],
            "export_as": [],
            "is_ddos_provider": False,
            "v4Peer": [],
            "v6Peer": [],
            "sibling_as": [],
        }
        for i in range(1, 210)
    }
    data["country"] = {"ZZ": {"chinese_short_name": "测试国"}}
    data.update(changes)
    return ReferenceBundle(
        "fixture-ref-v1", {"fixture": "人工参考"}, {"fixture": "fixture:1"}, data
    )


def obs(n, action="A", p=P, vp="100", path="100 1", at=None):
    return DetectionInput(
        str(n),
        "fixture-source",
        "fixture-v1",
        "fixture-collector",
        "peer-" + vp,
        vp,
        p,
        action,
        at or f"2026-02-28T00:{int(n) // 60 % 60:02}:{int(n) % 60:02}Z",
        path,
        "record:" + str(n),
    )


def engine(prefixes=(P,), vps=3, reference=None, origins=None):
    initial = tuple(
        obs(
            -100 + i * 10 + j,
            "RIB",
            p,
            str(100 + j),
            f"{100 + j} {(origins or {}).get(p, '1')}",
        )
        for i, p in enumerate(prefixes)
        for j in range(vps)
    )
    e = DetectionEngine(
        DetectionSeed(initial, "fixture-baseline"),
        reference or refs(),
        DetectionScope(
            "fixture-run",
            "r",
            "fixture-collector",
            "fixture-input",
            "2026-02-01T00:00:00Z",
            "2027-04-01T00:00:00Z",
        ),
    )
    begin(e)
    return e


def begin(e, month="202602"):
    e.begin_file(
        FileBoundary(
            "update-" + month,
            "fixture-v1",
            "2026-02-28T00:00:00Z",
            {k: k + "_" + month for k in KINDS},
        )
    )


def send(e, n, **kwargs):
    rows = e.consume(obs(n, **kwargs))
    assert not any(r["kind"] == "failed" for r in rows), rows
    return rows


def withdraw(e, p=P, start=1, vps=3):
    return [
        r
        for j in range(vps)
        for r in send(e, start + j, action="W", p=p, vp=str(100 + j), path="")
    ]


def business(e, kind):
    return [
        r
        for r in e.output.rows
        if r["kind"] == "business_revision" and r["event_kind"] == kind
    ]


@pytest.mark.parametrize("vps,expected", [(2, False), (3, True)])
def test_prefix_min_vps_and_full_withdraw(vps, expected):
    e = engine(vps=vps)
    withdraw(e, vps=vps)
    assert e.outage.prefix_vp[P]["is_outage_now"] is expected
    assert bool(business(e, "prefix_outage")) is expected


def test_prefix_restore_point_four_unknown_withdraw_and_cross_month():
    e = engine(vps=5)
    send(e, 1, action="W", vp="unknown", path="")
    assert e.outage.prefix_vp[P]["unreachable_vp_set"] == set()
    withdraw(e, start=2, vps=5)
    assert e.outage.prefix_vp[P]["is_outage_now"]
    e.finish_file()
    begin(e, "202603")
    send(e, 10, vp="100", at="2026-03-01T00:00:00Z")
    assert e.outage.prefix_vp[P]["is_outage_now"]
    send(e, 11, vp="101", path="101 1", at="2026-03-01T00:01:00Z")
    row = business(e, "prefix_outage")[-1]
    assert row["legacy"]["e_time"] == "2026-03-01 08:01:00"
    assert row["legacy_ref"]["table"] == "prefix_outage_202602"
    assert row["legacy"]["next_vp_paths"]
    assert row["revision"] == 2


def test_as_strict_threshold_and_independent_maxima_d09_tail():
    prefixes = tuple(f"10.{i}.0.0/24" for i in range(5))
    e = engine(prefixes)
    withdraw(e, prefixes[0], 1)
    assert not e.outage.as_prefix["1"]["is_outage_now"]  # exactly .2
    withdraw(e, prefixes[1], 10)
    assert e.outage.as_prefix["1"]["is_outage_now"]
    withdraw(e, prefixes[2], 20)
    event = e.outage.as_outage_event["1"][1]
    assert (
        event["max_outage_prefix_num"] == 3 and event["max_outage_prefix_ratio"] == 0.6
    )
    for i in range(5, 10):
        send(e, 30 + i, p=f"10.{i}.0.0/24")
    withdraw(e, prefixes[3], 50)
    assert event["max_outage_prefix_num"] == 4
    assert event["total_prefix_num"] == 10
    assert event["max_outage_prefix_ratio"] == 0.6  # maxima are independent
    rows = e.finish_file()
    update = next(r for r in rows if r["kind"] == "as_outage_update")
    assert update["legacy"]["as_outage_event"]["total_prefix_num"] == 10
    assert update["legacy"]["as_outage_event"]["max_outage_prefix_num"] == 4
    assert update["legacy"]["as_outage_event"]["max_outage_prefix_ratio"] == 0.6
    assert event["if_change"] is False
    assert any(r["kind"] == "event_as_outage_update" for r in rows)
    assert not any(r["kind"] == "failed" for r in rows)


def test_d09_minimal_legacy_counterexample():
    # 独立复现 Python 局部变量规则；不执行或import旧源码。
    def old_failure():
        changed_as = {"1": True}
        countries = {}
        for asn in changed_as:
            if changed_as[asn]:
                return countries[country][country_outage_id]  # noqa: F821 -- D09反例故意保留赋值前读取
        for country in countries:
            country_outage_id = 1

    with pytest.raises(UnboundLocalError):
        old_failure()


def test_as_restore_strict_point_eighty_five():
    prefixes = tuple(f"10.{i}.0.0/24" for i in range(20))
    e = engine(prefixes)
    for i in range(5):
        withdraw(e, prefixes[i], 1 + i * 3)
    for i in range(2):
        for j in range(2):
            send(e, 30 + i * 3 + j, p=prefixes[i], vp=str(100 + j))
    assert len(e.outage.as_prefix["1"]["normal_prefix_set"]) == 17
    assert e.outage.as_prefix["1"]["is_outage_now"]
    for j in range(2):
        send(e, 50 + j, p=prefixes[2], vp=str(100 + j))
    assert not e.outage.as_prefix["1"]["is_outage_now"]


def test_hijack_counts_equal_members_and_two_to_zero():
    e = engine(vps=1)
    send(e, 1, vp="101", path="101 2")
    assert business(e, "moas")[-1]["legacy"]["is_hijack"]
    assert len(e.hijack.moas_event_dict[P]) == 1
    send(e, 2, action="W", vp="101", path="")
    assert business(e, "moas")[-1]["legacy"]["end_as"] == "1"
    # 1->3 ignored; 3->2 ignored; equal-count changes ignored.
    rows = len(business(e, "moas"))
    e.hijack.hijack_detect("2026-03-01 00:00:00", P, {"1"}, {"1", "2", "3"}, {})
    e.hijack.hijack_detect("2026-03-01 00:00:01", P, {"1", "2", "3"}, {"1", "2"}, {})
    e.hijack.hijack_detect("2026-03-01 00:00:02", P, {"1", "2"}, {"3", "4"}, {})
    e.hijack.hijack_detect("2026-03-01 00:00:03", P, {"1", "2"}, set(), {})
    assert len(business(e, "moas")) == rows
    assert e.hijack.moas_event_dict[P] == {}


def test_projection_residual_origin_as_set_filter_and_raw_identity():
    e = engine(vps=1)
    send(e, 1, path="100 2")
    assert e.projection.prefix_as[P] == {"1", "2"}
    send(e, 2, action="W", path="")
    assert e.projection.prefix_as[P] == {"1"}  # historical residual stays
    assert business(e, "moas")[-1]["legacy"]["end_as"] == "1"
    send(e, 3, path="100 {1}")
    assert e.output.rows[-1]["kind"] == "filtered"
    send(e, 4, action="STATE", p="not a prefix")
    send(e, 5, p="0.0.0.0/0")
    assert e.output.rows[-1]["legacy"]["reason"] == "default_route"
    assert e.output.rows[-1]["evidence"]["observation"]["peer_ref"] == "peer-100"


@pytest.mark.parametrize(
    "parent,child,expected",
    [
        ("10.0.0.0/8", "10.0.0.0/16", True),
        ("10.0.0.0/8", "10.0.0.0/17", False),
        ("2001:db8::/32", "2001:db8::/35", True),
        ("2001:db8::/32", "2001:db8::/36", False),
    ],
)
def test_subhijack_length(parent, child, expected):
    e = engine((parent,), vps=1)
    send(e, 1, p=child, path="100 2")
    assert bool(business(e, "sub_hijack")) is expected


def test_subhijack_nearest_parent_no_scan_no_true_revision():
    parent = "10.0.0.0/8"
    child = "10.0.0.0/16"
    nearer = "10.0.0.0/12"
    e = engine((parent,), vps=1)
    send(e, 1, p=child, path="100 2")
    row = business(e, "sub_hijack")[-1]
    assert row["legacy"]["hijacked_prefix"] == parent
    send(e, 2, p=nearer, path="100 3")
    assert e.subhijack.sub_hijack_dict[child][1]["hijacked_prefix"] == parent
    send(e, 3, p=child, vp="101", path="101 4")
    assert len([r for r in business(e, "sub_hijack") if r["object"] == child]) == 1
    withdraw(e, child, 10, vps=1)
    send(e, 12, action="W", p=child, vp="101", path="")
    assert business(e, "sub_hijack")[-1]["legacy"]["e_time"] is not None


def leak_refs(level=True, stability=None):
    relation = {
        "4": {"customer": ["3"]},
        "2": {"customer": ["3", "1"]},
        "5": {"customer": ["1"]},
    }
    prefix = {
        P: {
            "route": "",
            "domain_num": 1 if level else 0,
            "domain_auth_num": 0,
            "domain": "[]",
            "domain_auth": "[]",
        }
    }
    triplet = {} if stability is None else {"4": {"3": {"2": {"stability": stability}}}}
    return refs(as_rel_dict=relation, prefix_info=prefix, triplet_info=triplet)


@pytest.mark.parametrize(
    "path,stability,count",
    [
        ("2 3 4", None, 0),
        ("5 2 3 4", 0.2, 0),
        ("5 2 3 4", 0.199, 1),
        ("5 2 3 3 4", None, 1),
        ("5 1 2 3 4", None, 2),
    ],
)
def test_leak_length_compression_triplets_stability(path, stability, count):
    e = engine(reference=leak_refs(stability=stability))
    send(e, 1, path=path)
    assert len(business(e, "leak")) == count
    for row in business(e, "leak"):
        assert row["end_state"] == "not_recorded"
        assert row["legacy"]["as_path"] == path
        assert row["legacy"]["leak_vp"] == "100"
    withdraw(e, start=10)
    send(e, 20, path=path)
    assert len(business(e, "leak")) == count


def test_leak_low_level_keeps_phenomenon_and_seen():
    e = engine(reference=leak_refs(False))
    send(e, 1, path="5 2 3 4")
    assert len(business(e, "leak")) == 1
    assert not any(r["kind"] == "leak_event_record" for r in e.output.rows)
    assert e.leak.prefix_path_dict[P]


def test_safe_reference_failure_is_partial_exportable():
    reference = refs(
        prefix_info={
            P: {
                "route": "",
                "domain_num": 1,
                "domain_auth_num": 0,
                "domain": "__import__('os').system('false')",
                "domain_auth": "[]",
            }
        }
    )
    e = engine(reference=reference)
    rows = e.consume(obs(1, vp="101", path="101 2"))
    assert rows[-1]["kind"] == "failed"
    assert e.status in ("failed", "partial")
    assert e.export_state()["publication_eligible"] is False
    assert e.export_state()["quality_errors"][0]["raw"].startswith("__import__")
    with pytest.raises(ValueError):
        e.consume(obs(2))
    json.dumps(e.export_state(), allow_nan=False)


def test_snapshot_independent_and_missing_values_preserved():
    e = engine()
    state = e.export_state()
    send(e, 1, vp="101", path="101 2")
    assert state["processed"] == 0
    assert e.export_state()["processed"] == 1
    assert state["references"]["historical_applicability"] == "Unknown"
    assert (
        e.export_state()["modules"]["hijack"]["moas_event_dict"]["10.0.0.0/24"]["$map"][
            0
        ][1]["e_time"]
        is None
    )


def test_rib_as_set_and_blank_origin_baseline():
    baseline = (
        obs(-1, "RIB", path="100 {1}"),
        obs(-2, "RIB", p="10.1.0.0/24", path=""),
    )
    e = DetectionEngine(
        DetectionSeed(baseline, "baseline"),
        refs(),
        DetectionScope(
            "run",
            "r",
            "fixture-collector",
            "input",
            "2026-02-01T00:00:00Z",
            "2026-04-01T00:00:00Z",
        ),
    )
    assert e.projection.prefix_as[P] == {"{1}"}
    assert "10.1.0.0/24" not in e.projection.prefix_dict
    begin(e)
    send(e, 1, path="100 {2}")
    assert e.projection.prefix_as[P] == {"{1}"}
    send(e, 2, action="W", path="")
    assert P not in e.projection.prefix_dict


def test_residual_origin_tree_removal_matches_legacy():
    e = engine(vps=1)
    send(e, 1, path="100 2")
    send(e, 2, action="W", path="")
    assert e.projection.prefix_as[P] == {"1"}
    assert not e.projection.ipv4_tree.has_key(P)
    send(e, 3, p="10.0.0.0/25", path="100 3")
    assert not business(e, "sub_hijack")


def test_actual_two_to_zero_keeps_active_hijack_business_state():
    e = engine(vps=1)
    send(e, 1, vp="101", path="101 2")
    # 单条输入接口无法一次撤去两个起源；以显式前后兼容投影构造此旧函数边界。
    before = e.hijack.moas_event_dict[P][1].copy()
    e.hijack.hijack_detect("2026-03-01 00:00:00", P, {"1", "2"}, set(), {})
    assert e.hijack.moas_event_dict[P][1] == before
    assert e.hijack.prefix_event[P]["is_moas_now"] is True
    assert before["e_time"] is None


def test_legacy_id_seed_and_distinct_year_run_identity():
    initial = (obs(-1, "RIB"),)
    scope = DetectionScope(
        "run-a",
        "r",
        "fixture-collector",
        "input",
        "2026-01-01T00:00:00Z",
        "2028-01-01T00:00:00Z",
    )
    e = DetectionEngine(
        DetectionSeed(initial, "baseline", {"moas": {P: 7}, "hijack": {P: 11}}),
        refs(),
        scope,
    )
    begin(e)
    send(e, 1, vp="101", path="101 2", at="2026-02-28T00:00:00Z")
    first = business(e, "hijack")[-1]
    assert first["legacy_ref"]["id"] == 12
    send(e, 2, action="W", vp="101", path="", at="2026-02-28T01:00:00Z")
    e.finish_file()
    begin(e, "202702")
    send(e, 3, vp="101", path="101 2", at="2027-02-28T00:00:00Z")
    second = business(e, "hijack")[-1]
    assert second["legacy_ref"]["id"] == 13  # same month across years does not reset
    assert first["incident_id"] != second["incident_id"]
    other = DetectionEngine(
        DetectionSeed(initial, "baseline", {"moas": {P: 7}, "hijack": {P: 11}}),
        refs(),
        replace(scope, run_id="run-b"),
    )
    begin(other)
    send(other, 1, vp="101", path="101 2")
    assert business(other, "hijack")[-1]["incident_id"] != first["incident_id"]


def test_snapshot_serialization_preserves_set_and_key_types():
    from data_pipeline.analysis.detection._results import plain

    assert plain({1: "a", "1": "b"}) == {"$map": [["1", "b"], [1, "a"]]}
    assert plain({"2", "1"}) == {"$set": ["1", "2"]}


def test_leak_phenomenon_records_both_month_references_and_allocated_id():
    e = engine(reference=leak_refs())
    send(e, 1, path="5 2 3 4")
    row = business(e, "leak")[-1]
    assert row["legacy_ref"]["table"] == "leak_phenomenon_202602"
    assert row["legacy"]["leak_event_table"] == "leak_202602"
    assert row["legacy"]["legacy_event_id"] == 1
