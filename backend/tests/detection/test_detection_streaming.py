"""公开计算输出接缝；仅人工输入，逐条比较已冻结内存口径。"""

from data_pipeline.analysis.detection import DetectionEngine
from data_pipeline.analysis.detection.streaming import BoundedResults
from tests.detection.test_detection_computation import engine, begin, obs
import pytest


@pytest.mark.parametrize("route_filter", [False, True])
def test_legacy_new_prefix_off_pair_roles_remain_explicitly_hash_order_dependent(
    tmp_path, route_filter
):
    import json
    import os
    from pathlib import Path
    import subprocess
    import sys

    code = '''
import json
from tests.detection.test_detection_computation import engine, obs, refs
from data_pipeline.analysis.detection.roles import interpret_roles
from data_pipeline.analysis.detection.classification import interpret_classification
import os
reference = refs()
if os.environ["DETECTION_ROUTE_FIXTURE"] == "1":
    reference.mappings["prefix_info"]["10.9.0.0/24"] = {"route": "4", "domain_num": 0, "domain_auth_num": 0, "domain": "[]", "domain_auth": "[]"}
    reference.mappings["as_info"]["4"]["import_as"] = ["AS2"]
e = engine(reference=reference)
e.consume(obs(1, p='10.9.0.0/24', path='100 2'))
e.consume(obs(2, p='10.9.0.0/24', vp='101', path='101 3'))
r = next(r['legacy'] for r in e.output.rows if r.get('event_kind') == 'moas')
decision = next(row for row in e.output.rows if row['kind'] == 'rule_decision' and 'BGPHijack.py:200' in row['legacy']['rule'])
print(json.dumps({**{k:r[k] for k in ('ori_as','moas_as1','moas_as2','hijacked_as','hijacker_as','is_hijack','filter_reason')}, 'roles':interpret_roles('moas', r), 'classification':interpret_classification({'kind':'business_revision','event_kind':'moas','legacy':r}), 'decision_classification':interpret_classification(decision), 'hijack_revision_count':sum(row.get('event_kind') == 'hijack' for row in e.output.rows)}))
'''
    backend = Path(__file__).resolve().parents[2]
    outputs = []
    for seed in (0, 2):
        env = {
            **os.environ,
            "PYTHONHASHSEED": str(seed),
            "DETECTION_ROUTE_FIXTURE": "1" if route_filter else "0",
            "PYTHONPATH": str(backend) + os.pathsep + str(backend / "tests"),
        }
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        outputs.append(json.loads(result.stdout))
    (tmp_path / "固定hash旧角色差异.json").write_text(
        json.dumps(
            {
                "seeds": [0, 2],
                "route_filter_reference": route_filter,
                "outputs": outputs,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    assert {row["is_hijack"] for row in outputs} == (
        {False, True} if route_filter else {True}
    )
    assert all(row["ori_as"] == "" for row in outputs)
    assert {(row["hijacked_as"], row["hijacker_as"]) for row in outputs} == {
        ("2", "3"),
        ("3", "2"),
    }
    assert outputs[0]["roles"] == outputs[1]["roles"]
    assert outputs[0]["roles"]["asn_roles"] == "ambiguous"
    assert outputs[0]["roles"]["attacker_asn"] is None
    assert outputs[0]["roles"]["victim_asn"] is None
    for output in outputs:
        assert output["hijack_revision_count"] == int(output["is_hijack"])
        assert output["classification"] == output["decision_classification"]
        assert output["classification"]["classification_state"] == "ambiguous"
        assert output["classification"]["classified_hijack"] is None


def test_streamed_records_are_complete_and_equal_to_memory_lifecycle():
    reference = engine()
    rows = []
    streamed = DetectionEngine(
        reference.seed,
        reference.references,
        reference.scope,
        results_factory=lambda scope, refs: BoundedResults(scope, refs, rows.append),
    )
    begin(streamed)
    for item in [obs(i + 1, "W", vp=str(100 + i)) for i in range(3)] + [
        obs(20, path="100 2"),
        obs(21, path="100 1"),
    ]:
        reference.consume(item)
        receipt = streamed.consume(item)
        assert receipt.end >= receipt.start
    reference.finish_file()
    streamed.finish_file()
    assert rows == reference.output.rows
    assert any(r["kind"] == "business_revision" and r["revision"] == 1 for r in rows)
    assert streamed.output.rows == []


def test_output_limit_fails_without_truncation_or_further_delivery():
    original = engine()
    rows = []
    output = BoundedResults(
        original.scope, original.references, rows.append, max_record_bytes=32
    )
    with pytest.raises(ValueError, match="字节上限"):
        output.append("large", {"raw": "保留原文" * 100})
    assert not rows
    with pytest.raises(RuntimeError, match="已经失败"):
        output.append("small", {})


def test_dirty_engine_matches_all_memory_revision_order_and_final_state():
    from data_pipeline.analysis.detection.streaming import StreamingDetectionEngine

    original = engine(prefixes=("10.0.0.0/24", "10.0.1.0/24"))
    rows = []
    streamed = StreamingDetectionEngine(
        original.seed, original.references, original.scope, sink=rows.append
    )
    begin(streamed)
    for i in range(1, 70):
        item = obs(
            i,
            "W" if i % 5 < 3 else "A",
            p="10.0.0.0/24" if i % 2 else "10.0.1.0/24",
            vp=str(100 + i % 3),
            path=f"{100 + i % 3} {1 + i % 2}",
        )
        original.consume(item)
        streamed.consume(item)
    original.finish_file()
    streamed.finish_file()
    assert rows == original.output.rows
    left, right = original.export_state(), streamed.export_state()
    left.pop("rows")
    right.pop("rows")
    assert left == right


def test_one_pass_seed_requires_complete_declared_count_and_keeps_no_rib_rows():
    from data_pipeline.analysis.detection.streaming import StreamingDetectionEngine, StreamingSeed

    original = engine()
    seed = StreamingSeed(iter(original.seed.observations), "fixture-baseline", 3)
    streamed = StreamingDetectionEngine(
        seed, original.references, original.scope, sink=lambda row: None
    )
    assert streamed.projection.export() == original.projection.export()
    assert streamed.seed.observations == ()
    with pytest.raises(ValueError, match="基线.*计数"):
        StreamingDetectionEngine(
            StreamingSeed(iter(original.seed.observations), "fixture-baseline", 4),
            original.references,
            original.scope,
            sink=lambda row: None,
        )


@pytest.mark.parametrize("case", ["country", "sub_hijack", "leak"])
def test_dirty_stream_preserves_country_steps_subprefix_and_transient_leak(case):
    from data_pipeline.analysis.detection.streaming import StreamingDetectionEngine
    from tests.detection.test_detection_computation import leak_refs

    if case == "country":
        original = engine(
            ("10.1.0.0/24", "10.2.0.0/24"),
            origins={"10.1.0.0/24": "1", "10.2.0.0/24": "2"},
        )
        items = [
            obs(i * 10 + j + 1, "W", p=p, vp=str(100 + j))
            for i, p in enumerate(("10.1.0.0/24", "10.2.0.0/24"))
            for j in range(3)
        ]
        expected = "country_outage"
    elif case == "sub_hijack":
        original = engine(("10.0.0.0/16",))
        items = [obs(1, p="10.0.1.0/24", path="100 2"), obs(2, "W", p="10.0.1.0/24")]
        expected = "sub_hijack"
    else:
        original = engine(reference=leak_refs())
        items = [obs(1, path="5 1 2 3 4"), obs(2, "W"), obs(3, path="5 1 2 3 4")]
        expected = "leak"
    rows = []
    streamed = StreamingDetectionEngine(
        original.seed, original.references, original.scope, sink=rows.append
    )
    begin(streamed)
    for item in items:
        original.consume(item)
        streamed.consume(item)
    original.finish_file()
    streamed.finish_file()
    assert rows == original.output.rows
    assert any(r.get("event_kind") == expected for r in rows)
    if case == "country":
        assert any(r["kind"] == "country_reduction" for r in rows)
    left, right = original.export_state(), streamed.export_state()
    left.pop("rows")
    right.pop("rows")
    assert left == right
