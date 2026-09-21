"""本任务显式隔离PG；缺DSN明确跳过。"""

import os
import uuid
import pytest
import psycopg2
from tests.detection.test_detection_computation import engine, obs


def test_typed_store_requires_ready_then_exact_snapshot_and_preserves_records(tmp_path):
    from data_pipeline.analysis.detection.store import DetectionStore, read_records

    dsn = os.environ.get("DOMEYE_DETECTION_TEST_DSN")
    if not dsn:
        pytest.skip("未绑定本任务隔离PG")
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    name = "det_test_" + uuid.uuid4().hex
    with admin.cursor() as cursor:
        cursor.execute(
            "CREATE DATABASE " + name + " ENCODING 'UTF8' TEMPLATE template0"
        )
    dsn += " dbname=" + name
    e = engine()
    e.consume(obs(1, path="100 2"))
    e.consume(obs(2, p="10.9.0.0/24", path="100 2"))
    e.consume(obs(3, p="10.9.0.0/24", vp="101", path="101 3"))
    e.finish_file()
    store = DetectionStore(
        dsn,
        tmp_path / "output",
        e.scope,
        {
            "input": "fixture-only",
            "execution_mode": "synthetic-fixture-api",
            "selected_sources": [],
        },
        batch_rows=2,
    )
    try:
        for row in e.output.rows:
            store.emit(row)
        with pytest.raises(ValueError, match="不可消费"):
            list(read_records(dsn, store.run_id, 0))
        with pytest.raises(ValueError, match="状态"):
            store.finish()
        store.save_state(e)
        snapshot = store.finish()
        from data_pipeline.analysis.detection._results import plain

        assert list(
            read_records(dsn, store.run_id, snapshot, allow_synthetic=True)
        ) == plain(e.output.rows)
        from data_pipeline.analysis.detection.store import read_revisions

        revisions = list(
            read_revisions(dsn, store.run_id, snapshot, allow_synthetic=True)
        )
        ambiguous = [row for row in revisions if row["asn_roles"] == "ambiguous"]
        assert ambiguous
        assert all(
            row["attacker_asn"] is None and row["victim_asn"] is None
            for row in ambiguous
        )
        assert all(row["legacy"]["ori_as"] == "" for row in ambiguous)
        assert all(
            row["classification_state"] == "ambiguous"
            and row["classified_hijack"] is None
            for row in ambiguous
        )
        from data_pipeline.analysis.detection.store import read_decisions

        decisions = list(
            read_decisions(dsn, store.run_id, snapshot, allow_synthetic=True)
        )
        assert any(
            row["classification_state"] == "ambiguous"
            and row["classified_hijack"] is None
            for row in decisions
        )
        from data_pipeline.analysis.detection.store import reconstruct_state

        restored = reconstruct_state(dsn, store.run_id, snapshot, allow_synthetic=True)
        assert restored["projection"]["prefix_dict"] == plain(e.projection.prefix_dict)
        assert restored["hijack"]["moas_event_dict"] == plain(e.hijack.moas_event_dict)
        assert (tmp_path / "output" / "ready.json").is_file()
        assert list((tmp_path / "output" / "parquet").rglob("*.parquet"))
        with pytest.raises(ValueError, match="不可消费"):
            list(read_records(dsn, store.run_id, snapshot + 1))
    finally:
        store.close()
        admin.close()


def test_required_ready_failure_never_enables_readonly_query(tmp_path):
    from data_pipeline.analysis.detection.store import DetectionStore, read_records

    dsn = os.environ.get("DOMEYE_DETECTION_TEST_DSN")
    if not dsn:
        pytest.skip("未绑定本任务隔离PG")
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    name = "det_failure_" + uuid.uuid4().hex
    with admin.cursor() as c:
        c.execute("CREATE DATABASE " + name + " ENCODING 'UTF8' TEMPLATE template0")
    dsn += " dbname=" + name
    e = engine()
    e.finish_file()
    store = DetectionStore(
        dsn,
        tmp_path / "output",
        e.scope,
        {"execution_mode": "synthetic-fixture-api", "selected_sources": []},
    )
    try:
        for row in e.output.rows:
            store.emit(row)
        store.save_state(e)
        (tmp_path / "output/ready.json").mkdir()
        with pytest.raises(OSError):
            store.finish()
        with psycopg2.connect(dsn) as pg, pg.cursor() as c:
            c.execute(
                "SELECT state,snapshot FROM detection.runs WHERE run_id=%s",
                (store.run_id,),
            )
            assert c.fetchone() == ("candidate", None)
        with pytest.raises(ValueError, match="不可消费"):
            list(read_records(dsn, store.run_id, 0, allow_synthetic=True))
    finally:
        store.close()
        admin.close()
