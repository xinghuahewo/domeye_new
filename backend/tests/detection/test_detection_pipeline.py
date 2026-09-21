"""本任务PG：人工原件→公共保存→Reader→Detection→typed PG/Parquet。"""

import os
import uuid
from pathlib import Path
import psycopg2
import pytest
import csv
import hashlib
import json
import subprocess
import sys
from dataclasses import asdict
from openpyxl import Workbook

from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.analysis.detection.models import DetectionScope, FileBoundary
from data_pipeline.analysis.detection.runner import run
from data_pipeline.analysis.detection.store import read_records, reconstruct_state
from tests.observations.test_observation_consumer import observation_fixture
from tests.detection.test_detection_computation import refs, KINDS


@pytest.mark.parametrize("mismatch", ["collector", "input_version"])
def test_scope_binding_mismatch_rejects_before_output(tmp_path, mismatch):
    from types import SimpleNamespace

    reader = SimpleNamespace(run_id="run", snapshot=1, manifest={"collector": "rrc25"})
    scope = DetectionScope(
        "fixture",
        "r",
        "wrong" if mismatch == "collector" else "rrc25",
        "wrong" if mismatch == "input_version" else "run:1",
        "1970-01-01T00:00:00Z",
        "1970-01-02T00:00:00Z",
    )
    with pytest.raises(ValueError, match="scope"):
        run(
            reader,
            refs(),
            scope,
            {},
            "unused",
            tmp_path / "output",
            root=Path(__file__).resolve().parents[3],
            identity_builder=lambda root: {},
            fixture_only=True,
        )
    assert not (tmp_path / "output").exists()


def saved_reference_files(root, rich=False):
    entries = []
    mappings = refs().mappings
    for role in (
        "as_info",
        "important_as_dict",
        "prefix_info",
        "triplet_info",
        "country",
        "important_prefix_v4",
        "important_prefix_v6",
        "as_prefix_dict",
        "as_rel_dict",
        "important_domain_dict",
        "private_as_dict",
    ):
        if role in ("country", "important_prefix_v4", "important_prefix_v6"):
            path = root / (role + ".xlsx")
            book = Workbook()
            book.active.title = role
            rows = (
                [["two_letter_code", "chinese_short_name"], ["ZZ", "测试国"]]
                if role == "country"
                else [["prefix", "name"]]
            )
            for row in rows:
                book.active.append(row)
            book.save(path)
            count = len(rows)
        elif role in ("as_info", "important_as_dict", "prefix_info", "triplet_info"):
            path = root / (role + ".csv")
            if role == "as_info":
                header = ["asn", *mappings[role]["1"]]
                rows = [header]
                for asn in (
                    (1, 2, 3, 4, 5, 100, 101, 102) if rich else (64496, 64497, 64498)
                ):
                    row = {
                        **mappings[role]["1"],
                        "org_name": f"Org{asn}",
                        "as_name": f"AS{asn}",
                    }
                    rows.append([asn, *(str(row[key]) for key in header[1:])])
            else:
                rows = {
                    "important_as_dict": [["aut-num", "descr"]],
                    "prefix_info": [
                        [
                            "prefix",
                            "domain",
                            "route",
                            "domain_num",
                            "domain_auth_num",
                            "domain_auth",
                        ]
                    ],
                    "triplet_info": [
                        ["first_as", "second_as", "third_as", "stability"]
                    ],
                }[role]
                if role == "prefix_info" and rich:
                    rows.append(["10.0.0.0/24", "[]", "", 1, 0, "[]"])
            with path.open("w", newline="") as file:
                csv.writer(file).writerows(rows)
            count = len(rows)
        else:
            path = root / (role + ".json")
            value = {"fixture_role": role}
            if role == "as_rel_dict" and rich:
                from tests.detection.test_detection_computation import leak_refs

                value.update(leak_refs().mappings["as_rel_dict"])
            path.write_text(json.dumps(value))
            count = len(value)
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        entries.append(
            {
                "path": str(path),
                "sha256": sha,
                "source_id": sha,
                "role": role,
                "expected_rows": count,
            }
        )
    return entries


@pytest.mark.parametrize("input_profile", ["complete", "observation", "observation_gap"])
def test_frozen_child_consumes_saved_reference_rows_and_publishes_bound_identity(
    tmp_path, monkeypatch, input_profile,
):
    base = os.environ.get("DOMEYE_DETECTION_TEST_DSN")
    if not base:
        pytest.skip("未绑定本任务独立PG")
    admin = psycopg2.connect(base)
    admin.autocommit = True
    dsns = []
    with admin.cursor() as cursor:
        for _ in range(2):
            name = "det_frozen_" + uuid.uuid4().hex
            cursor.execute(
                "CREATE DATABASE " + name + " ENCODING 'UTF8' TEMPLATE template0"
            )
            dsns.append(base + " dbname=" + name)
    admin.close()
    manifest = six_class_observation_fixture(tmp_path)
    has_gap = input_profile == "observation_gap"
    if has_gap:
        import gzip
        from tests.observations.test_observation_mrt import update
        from data_pipeline.bgp.input.mrt_reader import source_identity
        source = manifest['inputs'][-1]
        path = Path(source['path'])
        path.write_bytes(gzip.compress(gzip.decompress(path.read_bytes()) + update(attrs=b'\xf0\x23\x04\x00\x04\x2f\x66',subtype=7),mtime=0))
        source['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        source['size'] = path.stat().st_size
        source['source_id'] = source_identity('rrc25',source['origin_uri'],source['sha256'])
        manifest['update_sources'] = [e['source_id'] for e in manifest['inputs'][1:]]
        input_profile = 'observation'
    reference_files = saved_reference_files(tmp_path, rich=True)
    manifest["references"] = [
        {"path": r["path"], "sha256": r["sha256"]} for r in reference_files
    ]
    if input_profile == "observation":
        from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
        report = produce_checkpointed(manifest, dsns[0], tmp_path / "saved", min_free_bytes=0, policy="isolate-payload/v1" if has_gap else "strict/v1")
    else:
        report = produce(manifest, dsns[0], tmp_path / "saved", min_free_bytes=0)
    sources = [entry["source_id"] for entry in manifest["inputs"]]
    version = f"{report['run_id']}:{report['snapshot']}"
    scope = DetectionScope(
        "frozen-fixture",
        "r",
        "rrc25",
        version,
        "1970-01-01T00:00:00Z",
        "1970-01-02T00:00:00Z",
    )
    request = {
        "input_profile": input_profile,
        "observation_dsn": dsns[0],
        "detection_dsn": dsns[1],
        "input_run": report["run_id"],
        "input_snapshot": report["snapshot"],
        "ordered_sources": sources,
        "scope": asdict(scope),
        "references": reference_files,
        "reference_version": "artificial-v1",
        "boundaries": {
            source: asdict(
                FileBoundary(
                    source,
                    version,
                    scope.window_start,
                    {kind: kind + "_197001" for kind in KINDS},
                )
            )
            for source in sources[1:]
        },
        "output": str(tmp_path / "frozen-output"),
        "min_free_bytes": 0,
    }
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request))
    root = Path(__file__).resolve().parents[3]
    process = subprocess.run(
        [sys.executable, str(root / "scripts/pipeline/detection-frozen-run.py"), str(path)],
        capture_output=True,
        text=True,
    )
    assert process.returncode == 0, process.stderr
    result = json.loads((tmp_path / "frozen-output/result.json").read_text())
    rows = list(read_records(dsns[1], result["run_id"], result["snapshot"]))
    assert any(row["kind"] == "source_end" for row in rows)
    ready = json.loads((tmp_path / "frozen-output/business/ready.json").read_text())
    assert ready["identity"]["execution_mode"] == "frozen-fresh-process"
    assert ready["identity"]["execution_binding"]["pid"] != os.getpid()
    assert (
        "data_pipeline.analysis.detection.runner"
        in ready["identity"]["execution_binding"]["module_sources"]
    )
    assert {
        row.get("event_kind") for row in rows if row["kind"] == "business_revision"
    } == {
        "prefix_outage",
        "as_outage",
        "country_outage",
        "moas",
        "hijack",
        "sub_hijack",
        "leak",
    }
    assert any(row["kind"] == "country_reduction" for row in rows)
    from data_pipeline.analysis.detection.store import read_revisions

    countries = [
        row
        for row in read_revisions(dsns[1], result["run_id"], result["snapshot"])
        if row["event_kind"] == "country_outage"
    ]
    assert countries
    assert all(
        row["subject_type"] == "country"
        and row["subject_key"] == "ZZ"
        and row["asn_roles"] == "not_applicable"
        for row in countries
    )
    assert all(
        row["attacker_asn"] is None and row["victim_asn"] is None for row in countries
    )
    # 原内存算法作为固定对照；不复用StreamingEngine或存储状态导出器。
    from data_pipeline.analysis.detection import DetectionEngine, DetectionSeed, ReferenceBundle
    from data_pipeline.analysis.detection.adapter import adapt_element
    from data_pipeline.analysis.detection.reference_view import ReferenceView, ReferenceSource
    from data_pipeline.bgp.archive.message_reader import SourceStart, SourceEnd, MessageBatch
    from data_pipeline.analysis.detection._results import plain

    reader = ObservationReader(dsns[0], report["run_id"], report["snapshot"], sources, profile=input_profile)
    view = ReferenceView("artificial-v1", version, tmp_path / "oracle-reference")
    for source in reference_files:
        saved = (
            row
            for batch in reader.reference_batches(source["source_id"])
            for row in batch.to_pylist()
        )
        view.add(
            ReferenceSource(
                source["role"], source["source_id"], source["expected_rows"], saved
            )
        )
    meta = view.metadata()
    mappings = {name: view.mapping(name) for name in refs().mappings}
    reference = ReferenceBundle(meta.version, meta.raw_rows, meta.row_refs, mappings)
    initial, oracle = [], None
    for item in reader.stream():
        if isinstance(item, SourceStart):
            if item.role == "update":
                oracle.begin_file(FileBoundary(**request["boundaries"][item.source_id]))
        elif isinstance(item, MessageBatch):
            messages = {message["message_id"]: message for message in item.messages}
            for row in item.elements:
                element = adapt_element(
                    row,
                    run_id=reader.run_id,
                    snapshot=reader.snapshot,
                    collector_id="rrc25",
                    quality=messages[row["message_id"]]["quality"],
                )
                if element.action == "RIB":
                    initial.append(element)
                else:
                    oracle.consume(element)
        elif isinstance(item, SourceEnd):
            if oracle is None:
                oracle = DetectionEngine(
                    DetectionSeed(tuple(initial), f"{version}/{item.source_id}"),
                    reference,
                    scope,
                )
            else:
                oracle.finish_file()
    business_rows = [
        row
        for row in rows
        if row["kind"]
        not in (
            "source_start",
            "source_message",
            "source_end",
            "source_quality",
            "input_completion",
        )
    ]
    expected_rows = plain(oracle.output.rows)
    (tmp_path / "expected-business.json").write_text(
        json.dumps(expected_rows, ensure_ascii=False)
    )
    (tmp_path / "actual-business.json").write_text(
        json.dumps(business_rows, ensure_ascii=False)
    )
    assert normalize_old_moas_pair(business_rows) == normalize_old_moas_pair(
        expected_rows
    )
    restored = reconstruct_state(dsns[1], result["run_id"], result["snapshot"])
    original_state = oracle.export_state()
    for family in ("hijack", "outage", "subhijack", "leak"):
        assert normalize_old_moas_pair(restored[family]) == normalize_old_moas_pair(
            original_state["modules"][family]
        )
    assert restored["projection"] == original_state["compatibility_projection"]
    from tests.detection.test_detection_stored_rows import check_formal_stream

    counts = check_formal_stream(dsns[1], result["run_id"], result["snapshot"], monkeypatch)
    if input_profile == "observation":
        from data_pipeline.analysis.detection.qualified_reader import read_binding, read_coverage, read_qualified_revisions
        binding = read_binding(dsns[1], result["run_id"], result["snapshot"])
        bid = binding['identity']['input_binding_id']
        coverage = list(read_coverage(dsns[1],result['run_id'],result['snapshot'],expected_binding_id=bid))
        assert coverage and sum(r['kind'] == 'scope_gap' for r in coverage) == int(has_gap)
        qualified = list(read_qualified_revisions(dsns[1],result['run_id'],result['snapshot'],expected_binding_id=bid,at_position=binding['identity']['qualification_as_of_position']))
        assert qualified
        assert any(r['main'] is None for r in qualified) if has_gap else all(r['main'] == r['raw'] for r in qualified)
        if has_gap:
            gap_position=json.loads(next(r['payload_json'] for r in coverage if r['kind']=='scope_gap'))['effective_position']
            before_position=(gap_position[0],gap_position[1]-1,1,1000000)
            before=list(read_qualified_revisions(dsns[1],result['run_id'],result['snapshot'],expected_binding_id=bid,at_position=before_position))
            known={(r['raw']['incident_id'],r['raw']['revision']) for r in before if r['main'] is not None}
            assert any((r['raw']['incident_id'],r['raw']['revision']) in known and r['main'] is None for r in qualified)
        with pytest.raises(ValueError,match='超出'):
            list(read_qualified_revisions(dsns[1],result['run_id'],result['snapshot'],expected_binding_id=bid,at_position=(999,0,0,0)))

        from tests.detection.test_detection_m3 import check_m3_readers
        check_m3_readers(dsns[1],result,bid,coverage,tmp_path)
        if has_gap:
            from tests.detection.test_detection_m3_integrity import check_target_integrity
            check_target_integrity(dsns[1],result["run_id"],result["snapshot"],bid)
            from tests.detection.test_detection_m3 import check_m3_finish_rejections
            check_m3_finish_rejections(request,tmp_path,monkeypatch)

    (tmp_path / "typed-stream-audit.json").write_text(
        json.dumps({**result, "typed_counts": counts}, ensure_ascii=False)
    )


def normalize_old_moas_pair(value):
    """仅旧set首/次成员对：保留原输出，跨随机hash进程比较无序成员。"""
    if isinstance(value, list):
        return [normalize_old_moas_pair(item) for item in value]
    if isinstance(value, dict):
        result = {key: normalize_old_moas_pair(item) for key, item in value.items()}
        if "moas_as1" in result and "moas_as2" in result:
            assert result.get("ori_as") in (result["moas_as1"], result["moas_as2"])
            result["moas_as1"], result["moas_as2"] = sorted(
                [result["moas_as1"], result["moas_as2"]]
            )
        return result
    return value


@pytest.mark.parametrize("withdrawn", ["input", "reference"])
def test_revoked_after_last_source_end_cannot_complete(tmp_path, withdrawn):
    from data_pipeline.analysis.detection.reference_view import ReferenceView, ReferenceSource

    base = os.environ.get("DOMEYE_DETECTION_TEST_DSN")
    if not base:
        pytest.skip("未绑定本任务独立PG")
    admin = psycopg2.connect(base)
    admin.autocommit = True
    dsns = []
    with admin.cursor() as c:
        for _ in range(2):
            name = "det_revoke_" + uuid.uuid4().hex
            c.execute("CREATE DATABASE " + name + " ENCODING 'UTF8' TEMPLATE template0")
            dsns.append(base + " dbname=" + name)
    admin.close()
    manifest = observation_fixture(tmp_path)
    reference_files = saved_reference_files(tmp_path)
    manifest["references"] = [
        {"path": r["path"], "sha256": r["sha256"]} for r in reference_files
    ]
    report = produce(manifest, dsns[0], tmp_path / "saved", min_free_bytes=0)
    sources = [entry["source_id"] for entry in manifest["inputs"]]
    reader = ObservationReader(dsns[0], report["run_id"], report["snapshot"], sources)
    version = f"{reader.run_id}:{reader.snapshot}"
    view = ReferenceView("artificial-v1", version, tmp_path / "reference-view")
    for source in reference_files:
        saved = (
            row
            for batch in reader.reference_batches(source["source_id"])
            for row in batch.to_pylist()
        )
        view.add(
            ReferenceSource(
                source["role"], source["source_id"], source["expected_rows"], saved
            )
        )
    original_stream = reader.stream

    def revoked_stream():
        yield from original_stream()
        with psycopg2.connect(dsns[0]) as pg, pg.cursor() as c:
            if withdrawn == "input":
                c.execute(
                    "UPDATE domeye.inputs SET state='failed' WHERE run_id=%s AND source_id=%s",
                    (reader.run_id, sources[-1]),
                )
            else:
                c.execute(
                    "UPDATE domeye.reference_inputs SET state='failed' WHERE run_id=%s AND source_id=%s",
                    (reader.run_id, reference_files[0]["source_id"]),
                )

    reader.stream = revoked_stream
    scope = DetectionScope(
        "fixture", "r", "rrc25", version, "1970-01-01T00:00:00Z", "1970-01-02T00:00:00Z"
    )
    boundaries = {
        source: FileBoundary(
            source,
            version,
            scope.window_start,
            {kind: kind + "_197001" for kind in KINDS},
        )
        for source in sources[1:]
    }
    with pytest.raises(ValueError, match="资格"):
        run(
            reader,
            view,
            scope,
            boundaries,
            dsns[1],
            tmp_path / "business",
            root=Path(__file__).resolve().parents[3],
            identity_builder=lambda root: {"fixture": "explicit"},
            fixture_only=True,
            min_free_bytes=0,
        )
    assert not (tmp_path / "business/ready.json").exists()
    with psycopg2.connect(dsns[1]) as pg, pg.cursor() as c:
        c.execute("SELECT state,snapshot FROM detection.runs")
        assert c.fetchall() == [("failed", None)]


def six_class_observation_fixture(root):
    import gzip
    import struct
    from ipaddress import ip_network
    from tests.observations.test_observation_mrt import mrt
    from data_pipeline.bgp.input.mrt_reader import source_identity

    def attributes(path):
        values = [int(value) for value in path.split()]
        return bytes([64, 2, 2 + 4 * len(values), 2, len(values)]) + struct.pack(
            "!" + "I" * len(values), *values
        )

    def nlri(prefix):
        net = ip_network(prefix)
        return (
            bytes([net.prefixlen])
            + net.network_address.packed[: (net.prefixlen + 7) // 8]
        )

    table = b"\0\0\0\x19\0\x05rrc25" + struct.pack("!H", 3)
    for i in range(3):
        ip = bytes([192, 0, 2, i + 1])
        table += b"\x02" + ip + ip + struct.pack("!I", 100 + i)
    baseline = mrt(table, 1, 13)
    for sequence, (prefix, origin) in enumerate(
        (("10.0.0.0/24", 1), ("10.1.0.0/24", 2), ("10.2.0.0/16", 1))
    ):
        body = struct.pack("!I", sequence) + nlri(prefix) + struct.pack("!H", 3)
        for i in range(3):
            attrs = attributes(f"{100 + i} {origin}")
            body += struct.pack("!HIH", i, 90, len(attrs)) + attrs
        baseline += mrt(body, 2, 13)
    steps = [
        ("W", prefix, i, "")
        for prefix in ("10.0.0.0/24", "10.1.0.0/24")
        for i in range(3)
    ]
    steps += [("A", "10.0.0.0/24", i, f"{100 + i} 1") for i in range(3)]
    steps += [
        ("A", "10.2.1.0/24", 0, "100 4"),
        ("W", "10.2.1.0/24", 0, ""),
        ("A", "10.2.0.0/16", 0, "100 3"),
        ("A", "10.0.0.0/24", 0, "5 1 2 3 4"),
    ]
    updates = b""
    for n, (action, prefix, i, path) in enumerate(steps):
        withdrawn, announced = (
            (nlri(prefix), b"") if action == "W" else (b"", nlri(prefix))
        )
        attrs = attributes(path) if path else b""
        payload = (
            struct.pack("!H", len(withdrawn))
            + withdrawn
            + struct.pack("!H", len(attrs))
            + attrs
            + announced
        )
        bgp = b"\xff" * 16 + struct.pack("!HB", len(payload) + 19, 2) + payload
        endpoint = (
            struct.pack("!IIHH", 100 + i, 999, 0, 1)
            + bytes([192, 0, 2, i + 1])
            + bytes([192, 0, 2, 254])
        )
        updates += mrt(endpoint + bgp, epoch=101 + n)
    entries = []
    for name, raw, role in (
        ("baseline", baseline, "baseline"),
        ("updates", updates, "update"),
    ):
        path = root / (name + ".gz")
        path.write_bytes(gzip.compress(raw, mtime=0))
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        uri = "fixture://rrc25/" + name
        entries.append(
            dict(
                path=str(path),
                sha256=sha,
                source_id=source_identity("rrc25", uri, sha),
                origin_uri=uri,
                size=path.stat().st_size,
                role=role,
            )
        )
    return dict(
        schema_version="observation-run/v1",
        collector="rrc25",
        window_start="1970-01-01T00:00:00Z",
        window_end_exclusive="1970-01-02T00:00:00Z",
        inputs=entries,
        baseline_source=entries[0]["source_id"],
        update_sources=[entries[1]["source_id"]],
    )


def test_saved_messages_local_state_eor_empty_source_and_fixed_output(tmp_path):
    base = os.environ.get("DOMEYE_DETECTION_TEST_DSN")
    if not base:
        pytest.skip("未绑定本任务独立PG")
    admin = psycopg2.connect(base)
    admin.autocommit = True
    dsns = []
    with admin.cursor() as cursor:
        for _ in range(2):
            name = "det_chain_" + uuid.uuid4().hex
            cursor.execute(
                "CREATE DATABASE " + name + " ENCODING 'UTF8' TEMPLATE template0"
            )
            dsns.append(base + " dbname=" + name)
    admin.close()
    manifest = observation_fixture(tmp_path)
    report = produce(manifest, dsns[0], tmp_path / "observations", min_free_bytes=0)
    sources = [entry["source_id"] for entry in manifest["inputs"]]
    reader = ObservationReader(
        dsns[0],
        report["run_id"],
        report["snapshot"],
        sources,
        batch_rows=2,
        batch_bytes=128,
    )
    version = f"{report['run_id']}:{report['snapshot']}"
    scope = DetectionScope(
        "fixture", "r", "rrc25", version, "1970-01-01T00:00:00Z", "1970-01-02T00:00:00Z"
    )
    boundaries = {
        source: FileBoundary(
            source,
            version,
            scope.window_start,
            {kind: kind + "_197001" for kind in KINDS},
        )
        for source in sources[1:]
    }
    result = run(
        reader,
        refs(),
        scope,
        boundaries,
        dsns[1],
        tmp_path / "detection",
        root=Path(__file__).resolve().parents[3],
        identity_builder=lambda root: {"fixture": "explicit"},
        fixture_only=True,
        min_free_bytes=0,
    )
    with pytest.raises(ValueError, match="synthetic"):
        list(read_records(dsns[1], result["run_id"], result["snapshot"]))
    rows = list(
        read_records(
            dsns[1], result["run_id"], result["snapshot"], allow_synthetic=True
        )
    )
    messages = [row["legacy"] for row in rows if row["kind"] == "source_message"]
    assert any(message["kind"] == "state_change" for message in messages)
    assert any(message["eor"] for message in messages)
    assert any(message["local_message"] for message in messages)
    assert result["source_receipts"][-1]["elements"] == 0
    transitions = [row for row in rows if row["kind"] == "compatibility_transition"]
    assert any(
        row["evidence"]["observation"]["direction"] == "sent" for row in transitions
    )
    assert all(
        row["evidence"]["observation"]["snapshot_ref"] == version for row in transitions
    )
    state = reconstruct_state(
        dsns[1], result["run_id"], result["snapshot"], allow_synthetic=True
    )
    assert state["run"]["baseline"]["observations"] == []
    assert len(state["projection"]["prefix_dict"]) == 2
