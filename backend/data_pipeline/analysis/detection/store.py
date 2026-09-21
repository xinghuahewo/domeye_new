"""隔离离线业务存储；每条有类型索引和完整详情，PG登记运行完成；公共P1准入另行实施。"""

from dataclasses import asdict
from datetime import datetime
import json
import os
from pathlib import Path
import uuid

import pyarrow as pa
import psycopg2
from psycopg2.extras import execute_values

from data_pipeline.bgp.archive.store import connect_duckdb, literal
from data_pipeline.analysis.detection._results import plain
from data_pipeline.analysis.detection.roles import interpret_roles
from data_pipeline.analysis.detection.classification import interpret_classification


COLUMNS = (
    ("sequence", "BIGINT"),
    ("record_kind", "VARCHAR"),
    ("incident_id", "VARCHAR"),
    ("revision", "BIGINT"),
    ("event_kind", "VARCHAR"),
    ("subject_type", "VARCHAR"),
    ("subject_key", "VARCHAR"),
    ("observed_at", "TIMESTAMPTZ"),
    ("asn_roles", "VARCHAR"),
    ("attacker_asn", "BIGINT"),
    ("victim_asn", "BIGINT"),
    ("role_reason", "VARCHAR"),
    ("role_rule_version", "VARCHAR"),
    ("classification_state", "VARCHAR"),
    ("classified_hijack", "BOOLEAN"),
    ("classification_reason", "VARCHAR"),
    ("classification_rule_version", "VARCHAR"),
    ("legacy_json", "VARCHAR"),
    ("evidence_json", "VARCHAR"),
    ("attributes_json", "VARCHAR"),
)


def encode(value):
    return json.dumps(
        plain(value), ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )


class DetectionStore:
    def __init__(
        self, dsn, root, scope, identity, *, batch_rows=256, batch_bytes=4 * 1024**2
    ):
        if not 1 <= batch_rows <= 10000 or batch_bytes < 1:
            raise ValueError("输出批次限制无效")
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=False)
        self.dsn, self.scope, self.identity = dsn, scope, identity
        from data_pipeline.analysis.detection.qualification_contract import LAKE_PROFILE
        self.mirror_body = identity.get("output_profile") != LAKE_PROFILE
        from data_pipeline.analysis.detection.lake_integrity import Inventory
        self.written_inventory = Inventory()
        self.run_id = uuid.uuid4().hex
        self.schema = "det_" + self.run_id
        self.batch_rows, self.batch_bytes = batch_rows, batch_bytes
        self.pending, self.pending_bytes, self.count = [], 0, 0
        self.closed_for_writes = False
        self.state_count = None
        self.source_starts, self.source_ends = {}, []
        self.guard = lambda: None
        self.db = connect_duckdb(str(self.root / "staging.duckdb"))
        self.db.execute("SET temp_directory=" + literal(self.root / "temp"))
        self.db.execute("LOAD ducklake")
        self.db.execute("LOAD postgres")
        loaded = dict(
            self.db.execute(
                "SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded"
            ).fetchall()
        )
        if (
            loaded.get("ducklake") != "3f1b372"
            or loaded.get("postgres_scanner") != "b9fce43"
        ):
            raise ValueError("DuckLake/Postgres扩展版本与已验版本不符")
        self.db.execute(
            "ATTACH "
            + literal("ducklake:postgres:" + dsn)
            + " AS lake (DATA_PATH "
            + literal(self.root / "parquet")
            + ", DATA_INLINING_ROW_LIMIT 0)"
        )
        self.db.execute("CREATE SCHEMA lake." + self.schema)
        self.db.execute(
            f"CREATE TABLE lake.{self.schema}.records ("
            + ",".join('"' + name + '" ' + kind for name, kind in COLUMNS)
            + ")"
        )
        self.db.execute(
            f"CREATE TABLE lake.{self.schema}.state_entries (ordinal BIGINT, family VARCHAR, attribute VARCHAR, key_json VARCHAR, container VARCHAR, value_json VARCHAR)"
        )
        self.pg = psycopg2.connect(dsn)
        with self.pg, self.pg.cursor() as c:
            c.execute("CREATE SCHEMA IF NOT EXISTS detection")
            c.execute("""CREATE TABLE IF NOT EXISTS detection.runs (
                run_id TEXT PRIMARY KEY, schema_name TEXT NOT NULL,
                state TEXT NOT NULL, snapshot BIGINT, scope JSONB NOT NULL,
                identity JSONB NOT NULL, reason TEXT)""")
            if self.mirror_body:
                c.execute("""CREATE TABLE IF NOT EXISTS detection.records (
                    run_id TEXT NOT NULL, sequence BIGINT NOT NULL, record_kind TEXT NOT NULL,
                    incident_id TEXT, revision BIGINT, event_kind TEXT, subject_type TEXT,
                    subject_key TEXT, observed_at TIMESTAMPTZ, asn_roles TEXT,
                    attacker_asn BIGINT, victim_asn BIGINT, role_reason TEXT, role_rule_version TEXT,
                    classification_state TEXT, classified_hijack BOOLEAN,
                    classification_reason TEXT, classification_rule_version TEXT,
                    legacy_json JSONB NOT NULL, evidence_json JSONB NOT NULL,
                    attributes_json JSONB NOT NULL, PRIMARY KEY(run_id,sequence))""")
            c.execute(
                "INSERT INTO detection.runs VALUES (%s,%s,'candidate',NULL,%s,%s,NULL)",
                (self.run_id, self.schema, encode(asdict(scope)), encode(identity)),
            )
            if self.mirror_body:
                c.execute("""CREATE TABLE IF NOT EXISTS detection.state_entries (
                    run_id TEXT, ordinal BIGINT, family TEXT, attribute TEXT,
                    key_json JSONB, container TEXT, value_json JSONB,
                    PRIMARY KEY(run_id,ordinal))""")

    def emit(self, row):
        self.guard()
        if self.closed_for_writes:
            raise ValueError("输出已关闭")
        if row["kind"] == "source_start":
            source = row["legacy"]
            if source["source_id"] in self.source_starts:
                raise ValueError("重复来源开始")
            self.source_starts[source["source_id"]] = source
        if row["kind"] == "source_end":
            source = row["legacy"]
            start = self.source_starts.get(source["source_id"])
            if start is None or (
                start["expected_messages"],
                start["expected_elements"],
            ) != (source["messages"], source["elements"]):
                raise ValueError("来源结束计数与开始合同不符")
            self.source_ends.append(source["source_id"])
        kind = row.get("event_kind")
        subject = (
            "country"
            if kind == "country_outage"
            else "asn"
            if kind == "as_outage"
            else "prefix"
            if kind
            else None
        )
        attrs = {k: v for k, v in row.items() if k not in ("legacy", "evidence")}
        roles = interpret_roles(kind, row["legacy"])
        classification = interpret_classification(row)
        if classification["classification_state"] == "ambiguous":
            roles.update(
                asn_roles="ambiguous",
                attacker_asn=None,
                victim_asn=None,
                role_reason="original_as_missing_or_outside_moas_pair",
            )
        observed_at = row.get("evidence", {}).get("observation", {}).get("observed_at")
        record = (
            self.count,
            row["kind"],
            row.get("incident_id"),
            row.get("revision"),
            kind,
            subject,
            str(row.get("object")) if subject else None,
            datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
            if observed_at
            else None,
            roles["asn_roles"],
            roles["attacker_asn"],
            roles["victim_asn"],
            roles["role_reason"],
            roles["role_rule_version"],
            classification["classification_state"],
            classification["classified_hijack"],
            classification["classification_reason"],
            classification["classification_rule_version"],
            encode(row["legacy"]),
            encode(row["evidence"]),
            encode(attrs),
        )
        size = sum(len(value.encode()) for value in record if isinstance(value, str))
        if size > 8 * 1024**2:
            raise ValueError("单条记录超过人工阶段字节上限；不截断")
        if self.pending and (
            len(self.pending) >= self.batch_rows
            or self.pending_bytes + size > self.batch_bytes
        ):
            self.flush()
        if not self.mirror_body:
            self.written_inventory.add("records", dict(zip((n for n, _ in COLUMNS), record)))
        self.pending.append(record)
        self.pending_bytes += size
        self.count += 1
        if self.pending_bytes >= self.batch_bytes:
            self.flush()

    def flush(self):
        self.guard()
        if not self.pending:
            return
        types = {
            "BIGINT": pa.int64(),
            "VARCHAR": pa.string(),
            "TIMESTAMPTZ": pa.timestamp("us", tz="UTC"),
            "BOOLEAN": pa.bool_(),
        }
        schema = pa.schema([(name, types[kind]) for name, kind in COLUMNS])
        table = pa.Table.from_pylist(
            [dict(zip((n for n, _ in COLUMNS), row)) for row in self.pending],
            schema=schema,
        )
        self.db.register("output_batch", table)
        try:
            self.db.execute(
                f"INSERT INTO lake.{self.schema}.records SELECT * FROM output_batch"
            )
            if self.mirror_body:
                with self.pg, self.pg.cursor() as c:
                    execute_values(
                        c,
                        "INSERT INTO detection.records VALUES %s",
                        [(self.run_id, *row) for row in self.pending],
                    )
        except BaseException as error:
            self.fail(str(error))
            raise
        finally:
            self.db.unregister("output_batch")
        self.pending.clear()
        self.pending_bytes = 0

    def save_state(self, engine):
        if self.closed_for_writes or self.state_count is not None:
            raise ValueError("状态输出已关闭或已经保存")
        if engine.file_boundary is not None or engine.status in ("failed", "partial"):
            raise ValueError("计算失败或文件未结束，不能保存完整状态")
        batch, byte_count, count = [], 0, 0
        for family, attribute, value in state_attributes(engine):
            container = (
                "dict"
                if isinstance(value, dict)
                else "set"
                if isinstance(value, (set, frozenset))
                else "list"
                if isinstance(value, (list, tuple))
                else "value"
            )

            def entries():
                yield None, container, value if container == "value" else None
                if container == "dict":
                    for key, item in value.items():
                        yield key, "entry", item
                elif container in ("set", "list"):
                    for key, item in enumerate(value):
                        yield key, "entry", item

            for key, record_type, item in entries():
                record = (
                    count,
                    family,
                    attribute,
                    encode(key),
                    record_type,
                    encode(item),
                )
                size = sum(len(v.encode()) for v in record if isinstance(v, str))
                if size > 8 * 1024**2:
                    raise ValueError("单项状态超过人工阶段字节上限；不截断")
                if batch and (
                    len(batch) >= self.batch_rows
                    or byte_count + size > self.batch_bytes
                ):
                    self._write_state(batch)
                    batch, byte_count = [], 0
                batch.append(record)
                byte_count += size
                count += 1
        self._write_state(batch)
        self.state_count = count

    def _write_state(self, rows):
        self.guard()
        if not rows:
            return
        names = (
            "ordinal",
            "family",
            "attribute",
            "key_json",
            "container",
            "value_json",
        )
        table = pa.Table.from_pylist([dict(zip(names, row)) for row in rows])
        if not self.mirror_body:
            for row in rows:
                self.written_inventory.add("state_entries", dict(zip(names, row)))
        self.db.register("state_batch", table)
        try:
            self.db.execute(
                f"INSERT INTO lake.{self.schema}.state_entries SELECT * FROM state_batch"
            )
            if self.mirror_body:
                with self.pg, self.pg.cursor() as c:
                    execute_values(
                        c,
                        "INSERT INTO detection.state_entries VALUES %s",
                        [(self.run_id, *row) for row in rows],
                    )
        except BaseException as error:
            self.fail(str(error))
            raise
        finally:
            self.db.unregister("state_batch")

    def finish(self, *, validate_before_commit=lambda: None):
        if self.closed_for_writes or self.state_count is None:
            raise ValueError("输入或状态未完成，禁止准入")
        if self.identity.get("selected_sources") != self.source_ends:
            raise ValueError("没有完整有序来源回执，禁止准入")
        self.flush()
        lake_count = self.db.execute(
            f"SELECT count(*) FROM lake.{self.schema}.records"
        ).fetchone()[0]
        with self.pg.cursor() as c:
            c.execute(
                "SELECT count(*) FROM detection.records WHERE run_id=%s", (self.run_id,)
            )
            pg_count = c.fetchone()[0]
        if lake_count != self.count or pg_count != self.count:
            raise ValueError("PG与湖表计数不符")
        lake_state = self.db.execute(
            f"SELECT count(*) FROM lake.{self.schema}.state_entries"
        ).fetchone()[0]
        with self.pg.cursor() as c:
            c.execute(
                "SELECT count(*) FROM detection.state_entries WHERE run_id=%s",
                (self.run_id,),
            )
            if c.fetchone()[0] != self.state_count or lake_state != self.state_count:
                raise ValueError("状态项计数不符")
        snapshot = self.db.execute(
            "SELECT max(snapshot_id) FROM lake.snapshots()"
        ).fetchone()[0]
        return self._complete(snapshot, validate_before_commit=validate_before_commit)

    def _complete(self, snapshot, *, validate_before_commit):
        validate_before_commit()
        receipt = dict(
            run_id=self.run_id,
            snapshot=snapshot,
            state="ready",
            records=self.count,
            state_entries=self.state_count,
            scope=asdict(self.scope),
            identity=self.identity,
        )
        with (self.root / "ready.json").open("x", encoding="utf-8") as f:
            json.dump(receipt, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        descriptor = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        with self.pg, self.pg.cursor() as c:
            c.execute(
                "UPDATE detection.runs SET state='complete',snapshot=%s WHERE run_id=%s AND state='candidate'",
                (snapshot, self.run_id),
            )
            if c.rowcount != 1:
                raise ValueError("候选状态已改变")
        self.closed_for_writes = True
        return snapshot

    def fail(self, reason):
        self.closed_for_writes = True
        try:
            self.pg.rollback()
            with self.pg, self.pg.cursor() as c:
                c.execute(
                    "UPDATE detection.runs SET state='failed',reason=%s WHERE run_id=%s AND state='candidate'",
                    (reason, self.run_id),
                )
                c.execute(
                    "SELECT state,snapshot FROM detection.runs WHERE run_id=%s",
                    (self.run_id,),
                )
                return c.fetchone() or ("unknown", None)
        except Exception:
            try:
                with psycopg2.connect(self.dsn) as pg, pg.cursor() as c:
                    c.execute(
                        "SELECT state,snapshot FROM detection.runs WHERE run_id=%s",
                        (self.run_id,),
                    )
                    return c.fetchone() or ("unknown", None)
            except Exception:
                return "unknown", None

    def close(self):
        self.db.close()
        self.pg.close()


def read_records(dsn, run_id, snapshot, *, batch_rows=256, allow_synthetic=False):
    for row in _read_table(
        dsn,
        run_id,
        snapshot,
        "records",
        batch_rows=batch_rows,
        allow_synthetic=allow_synthetic,
    ):
        yield {
            **json.loads(row["attributes_json"]),
            "legacy": json.loads(row["legacy_json"]),
            "evidence": json.loads(row["evidence_json"]),
        }


def read_revisions(dsn, run_id, snapshot, *, allow_synthetic=False):
    """固定版本的typed业务修订；原角色/文案仍留legacy，质量另列。"""
    for row in _read_table(
        dsn, run_id, snapshot, "records", allow_synthetic=allow_synthetic
    ):
        if row["record_kind"] == "business_revision":
            yield {
                **row,
                "legacy": json.loads(row["legacy_json"]),
                "evidence": json.loads(row["evidence_json"]),
                "attributes": json.loads(row["attributes_json"]),
            }


def read_decisions(dsn, run_id, snapshot, *, allow_synthetic=False):
    """包括没有生成事件的旧判定；未知规范分类保持NULL。"""
    for row in _read_table(
        dsn, run_id, snapshot, "records", allow_synthetic=allow_synthetic
    ):
        if row["record_kind"] == "rule_decision":
            yield {
                **row,
                "legacy": json.loads(row["legacy_json"]),
                "evidence": json.loads(row["evidence_json"]),
            }


def read_stored_rows(
    dsn, run_id, snapshot, table, *, batch_rows=256,
    max_row_bytes=8 * 1024**2, guard=None, allow_synthetic=False,
):
    """固定版本原typed行流；完全耗尽才完成末尾资格复验，早停须close。"""
    if type(max_row_bytes) is not int or max_row_bytes < 1:
        raise ValueError("完整行字节限额无效")
    yield from _stream_stored_rows(
        dsn, run_id, snapshot, table, batch_rows=batch_rows,
        max_row_bytes=max_row_bytes, guard=guard, allow_synthetic=allow_synthetic,
    )


def _stream_stored_rows(
    dsn, run_id, snapshot, table, *, batch_rows=256,
    max_row_bytes=None, guard=None, allow_synthetic=False,
):
    if (
        not isinstance(run_id, str) or not run_id.isascii() or not run_id.isalnum()
        or type(snapshot) is not int
        or type(batch_rows) is not int or not 1 <= batch_rows <= 10000
        or table not in ("records", "state_entries", "m3_entries")
        or (guard is not None and not callable(guard))
    ):
        raise ValueError("查询身份、表或批次/行限额无效")

    def check_guard():
        if guard is not None:
            guard()

    def qualification():
        # 独立连接读取新资格；不沿用湖catalog或事务的旧快照。
        pg = psycopg2.connect(dsn)
        try:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute(
                    "SELECT state,schema_name,snapshot,identity,scope "
                    "FROM detection.runs WHERE run_id=%s", (run_id,),
                )
                found = c.fetchone()
        finally:
            pg.close()
        if found is None or found[:3] != ("complete", "det_" + run_id, snapshot):
            raise ValueError("候选、失败或错版运行不可消费")
        if not isinstance(found[3], dict) or (
            found[3].get("execution_mode") != "frozen-fresh-process"
            and not allow_synthetic
        ):
            raise ValueError("非冻结正式执行不可消费；人工审计须显式允许synthetic")
        if (table == 'm3_entries' or str(found[3].get('output_profile','')).startswith(('detection-m3/','detection-m3-lake/'))
            or str(found[3].get('store_schema_version','')).startswith(('detection-typed-m3/','detection-typed-m3-lake/'))
            or 'qualification_rule' in found[3]):
            from data_pipeline.analysis.detection.qualification_contract import validate_identity
            validate_identity(found[3],allow_synthetic=allow_synthetic)
        return found

    check_guard()
    pinned = qualification()
    db = connect_duckdb()
    try:
        check_guard()
        db.execute("LOAD ducklake")
        db.execute("LOAD postgres")
        db.execute(
            "ATTACH " + literal("ducklake:postgres:" + dsn) + " AS lake (READ_ONLY)"
        )
        order = "sequence" if table == "records" else "ordinal"
        result = db.execute(
            f"SELECT * FROM lake.det_{run_id}.{table} AT (VERSION => {snapshot}) ORDER BY {order}"
        )
        batches = iter(result.fetch_record_batch(batch_rows))
        while True:
            check_guard()
            batch = next(batches, None)
            if batch is None:
                break
            # 逐行转Python，避免再复制整批完整详情；Arrow批仍由batch_rows约束。
            for index in range(batch.num_rows):
                check_guard()
                row = {name: batch.column(i)[index].as_py()
                       for i, name in enumerate(batch.schema.names)}
                if max_row_bytes is not None and len(encode(row).encode("utf-8")) > max_row_bytes:
                    raise ValueError("完整typed行超过字节限额")
                yield row
        check_guard()
        if qualification() != pinned:
            raise ValueError("运行资格、身份或scope在读取期间发生漂移")
    finally:
        db.close()


def _read_table(dsn, run_id, snapshot, table, *, batch_rows=256, allow_synthetic=False):
    # 既有语义Reader保持输出结构，统一固定流门禁。
    yield from _stream_stored_rows(
        dsn, run_id, snapshot, table, batch_rows=batch_rows,
        allow_synthetic=allow_synthetic,
    )


def state_attributes(engine):
    if hasattr(engine, "m3_qualification"):
        yield "m3", "qualification", engine.m3_qualification
    for family in ("outage", "hijack", "subhijack", "leak"):
        for name, value in vars(getattr(engine, family)).items():
            if name not in ("output", "bgp_info", "bgp_rib"):
                yield family, name, value
    for name in (
        "prefix_dict",
        "prefix_as",
        "as_prefix",
        "vp_set",
        "t",
        "tree_origins",
    ):
        yield "projection", name, getattr(engine.projection, name)
    for name in (
        "identities",
        "revisions",
        "last_records",
        "start_tables",
        "errors",
        "projection_gaps",
        "thresholds",
    ):
        yield "output", name, getattr(engine.output, name)
    yield "run", "baseline", asdict(engine.seed)
    yield "run", "references", asdict(engine.references)
    yield "run", "processed", engine.processed
    yield "run", "last_observation", engine.last_observation
    yield "run", "seen_observations", engine.seen_observations


def reconstruct_state(dsn, run_id, snapshot, *, allow_synthetic=False):
    """显式只读全状态重建，用于人工审计；不是恢复入口。"""
    result, maps, containers = {}, {}, {}
    for row in _read_table(
        dsn, run_id, snapshot, "state_entries", allow_synthetic=allow_synthetic
    ):
        family = result.setdefault(row["family"], {})
        name, container = row["attribute"], row["container"]
        if container in ("dict", "set", "list"):
            maps[(row["family"], name)] = []
            containers[(row["family"], name)] = container
        elif container == "entry":
            maps[(row["family"], name)].append(
                [json.loads(row["key_json"]), json.loads(row["value_json"])]
            )
        else:
            family[name] = json.loads(row["value_json"])
    for (family, name), pairs in maps.items():
        kind = containers[(family, name)]
        if kind == "set":
            value = {
                "$set": sorted(
                    (v for _, v in pairs), key=lambda x: json.dumps(x, sort_keys=True)
                )
            }
        elif kind == "list":
            value = [v for _, v in pairs]
        else:
            value = (
                dict(pairs)
                if all(isinstance(key, str) for key, _ in pairs)
                else {
                    "$map": sorted(
                        pairs, key=lambda pair: json.dumps(pair[0], sort_keys=True)
                    )
                }
            )
        result[family][name] = value
    return result
