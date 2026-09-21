"""参考字段类型与实际过滤先后；通过真实观察触发判定。"""

from dataclasses import replace
import pytest
from tests.detection.test_detection_computation import engine, refs, send, obs, business, P, leak_refs


def change_ref(mutator):
    reference = refs()
    mutator(reference.mappings)
    return reference


def hijack_reason(reference):
    e = engine(reference=reference, vps=1)
    send(e, 1, vp="101", path="101 2")
    return e, business(e, "moas")[-1]["legacy"]["filter_reason"]


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("import_as", ["AS2"], "1 import filter 2"),
        ("export_as", ["AS2"], "1 export filter 2"),
        ("is_ddos_provider", True, "1 is AntiDDoS provider"),
        ("v4Peer", [2], "peer"),
        ("v6Peer", [2], "peer"),
        ("sibling_as", [2], "siblings"),
        ("org_name", "个人网络", "personal network or unknown org"),
        ("org_name", "未知组织", "personal network or unknown org"),
    ],
)
def test_hijack_entity_filters(field, value, reason):
    reference = change_ref(lambda d: d["as_info"]["1"].update({field: value}))
    e, actual = hijack_reason(reference)
    assert actual == reason
    assert not business(e, "hijack")
    assert any(
        r["kind"] == "rule_decision" and r["reference_version"] == "fixture-ref-v1"
        for r in e.output.rows
    )


@pytest.mark.parametrize(
    "relation,reason",
    [
        ("provider", "as rel provider-customer"),
        ("customer", "as rel provider-customer"),
        ("peers", "as rel peer"),
        ("sibling", "as rel same org"),
    ],
)
@pytest.mark.parametrize("reverse", [False, True])
def test_hijack_bilateral_relationship_filters(relation, reason, reverse):
    key, value = ("2", "1") if reverse else ("1", "2")
    _, actual = hijack_reason(refs(as_rel_dict={key: {relation: [value]}}))
    assert actual == reason


def test_reference_element_types_order_and_historical_prefix():
    _, reason = hijack_reason(refs(as_rel_dict={"1": {"provider": [2]}}))
    assert reason == "possible hijack"  # int is not str
    reference = change_ref(lambda d: d["as_info"]["1"].update(v4Peer=["2"]))
    _, reason = hijack_reason(reference)
    assert reason == "possible hijack"  # str is not int
    _, reason = hijack_reason(refs(as_prefix_dict={"1": {P: 1}, "2": {P: 2}}))
    assert reason == P + "both in moasset"
    reference = change_ref(
        lambda d: d["as_info"]["1"].update(import_as="['AS2']", is_ddos_provider=True)
    )
    _, reason = hijack_reason(reference)
    assert reason == "1 import filter 2"  # import precedes AntiDDoS


def test_prefix_route_expands_filter_before_relationship():
    reference = refs(
        prefix_info={P: {"route": "3"}}, as_rel_dict={"3": {"customer": ["2"]}}
    )
    _, reason = hijack_reason(reference)
    assert reason == "as rel provider-customer"


@pytest.mark.parametrize(
    "candidate,reason",
    [
        ("64511", "possible hijack"),
        ("64512", "private as"),
        ("4200000000", "private as"),
        ("4294967295", "possible hijack"),
    ],
)
def test_hijack_own_private_range(candidate, reason):
    e = engine(vps=1)
    # all-private paths fall back to the last element under this old origin rule.
    send(e, 1, vp="101", path=candidate)
    assert business(e, "moas")[-1]["legacy"]["filter_reason"] == reason


@pytest.mark.parametrize(
    "ordinary,authority,expected",
    [
        (0, 0, "low"),
        (1, 0, "middle"),
        (30, 15, "middle"),
        (31, 0, "high"),
        (0, 16, "high"),
    ],
)
def test_hijack_grade_thresholds(ordinary, authority, expected):
    reference = refs(
        prefix_info={
            P: {
                "route": "",
                "domain_num": ordinary,
                "domain_auth_num": authority,
                "domain": "[]",
                "domain_auth": "[]",
            }
        }
    )
    e, _ = hijack_reason(reference)
    assert business(e, "hijack")[-1]["legacy"]["level"] == expected


def test_important_domain_top_level_text_quirk_and_int_key():
    reference = refs(
        prefix_info={
            P: {
                "route": "",
                "domain_num": 0,
                "domain_auth_num": 0,
                "domain": "['example.test']",
                "domain_auth": "[]",
            }
        },
        important_domain_dict={"example.test": {"name": "特定名称"}},
    )
    e, _ = hijack_reason(reference)
    record = business(e, "hijack")[-1]["legacy"]
    assert record["level"] == "high"
    assert "重要" in record["level_info"] and "特定名称" not in record["level_info"]
    e, _ = hijack_reason(refs(important_as_dict={1: {"name": "important"}}))
    assert business(e, "hijack")[-1]["legacy"]["level"] == "high"
    e, _ = hijack_reason(refs(important_as_dict={"1": {"name": "important"}}))
    assert business(e, "hijack")[-1]["legacy"]["level"] == "low"


def test_leak_missing_triplet_is_legacy_default_not_observed_zero():
    e = engine(reference=leak_refs())
    send(e, 1, path="5 2 3 4")
    lookup = next(r for r in e.output.rows if r["kind"] == "reference_lookup")
    assert lookup["legacy"]["measurement_state"] == "missing"
    assert lookup["legacy"]["legacy_effective_value"] == 0
    assert lookup["legacy"]["reference_row"] is None
    assert business(e, "leak")[-1]["legacy"]["filter_reason"] == "possible leak"


def test_rejected_leak_still_seen():
    reference = leak_refs()
    reference.mappings["as_info"]["3"]["org_name"] = "Org2"
    e = engine(reference=reference)
    send(e, 1, path="5 2 3 4")
    assert business(e, "leak")[-1]["legacy"]["filter_reason"] == "org equal"
    assert not any(r["kind"] == "leak_event_record" for r in e.output.rows)
    send(e, 2, path="5 1 2 3 4")
    assert len(business(e, "leak")) == 1


def test_unknown_withdraw_and_failed_input_have_structured_quality():
    e = engine()
    e.consume(replace(obs(1), source_version="wrong"))
    assert e.status == "failed"
    assert e.output.rows[-1]["legacy"]["stage"] == "input"
    assert e.output.rows[-1]["legacy"]["publication_eligible"] is False


def test_outage_blacklist_exclusive_and_legacy_multi_origin_attribution():
    from data_pipeline.analysis.detection._blacklist import prefix_blacklist
    from tests.detection.test_detection_computation import withdraw

    assert len(set(prefix_blacklist)) == 973
    prefix = prefix_blacklist[0]
    e = engine((prefix,))
    withdraw(e, prefix)
    assert not business(e, "prefix_outage")
    # 多origin仍按旧sorted首项记录归属，不上升为新责任事实。
    e = engine()
    send(e, 1, vp="101", path="101 2")
    withdraw(e, start=2)
    record = business(e, "prefix_outage")[-1]
    assert record["legacy"]["asn"] == "1"  # 最后撤回102，之前101的起源2已移除
    assert record["conclusion"] == "指定观察范围内的启发式异常候选"


def test_computation_has_no_external_side_effect_imports():
    import ast
    from pathlib import Path
    import data_pipeline.analysis.detection as package

    root = Path(package.__file__).parent
    # 从实际纯计算入口沿包内导入检查，不能把并未调用的离线存储／发布模块误算入纯计算。
    pending = [root / "__init__.py", root / "streaming.py", root / "adapter.py"]
    checked = set()
    while pending:
        path = pending.pop()
        if path in checked:
            continue
        checked.add(path)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and node.level == 1 and node.module:
                dependency = root.joinpath(*node.module.split(".")).with_suffix(".py")
                if dependency.is_file():
                    pending.append(dependency)
            if isinstance(node, ast.Import):
                assert all(not n.name.startswith(("database", "config", "core", "requests", "subprocess", "psycopg")) for n in node.names)
            if isinstance(node, ast.ImportFrom):
                assert node.module not in ("store", "reference_view")
                assert not (node.module or "").startswith(
                    ("database", "config", "core", "requests", "subprocess", "psycopg")
                )
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id not in ("eval", "exec", "open", "__import__")
