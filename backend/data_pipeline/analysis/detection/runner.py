"""显式人工/冻结运行接缝；共享Reader是唯一观察读取实现。"""

from dataclasses import asdict
from pathlib import Path
import resource
import shutil
import sys
import os
import psycopg2

from data_pipeline.bgp.archive.message_reader import SourceStart, SourceEnd, MessageBatch
from data_pipeline.common.frozen_execution import execution_identity
from data_pipeline.bgp.input.path_decoding import DECODER_VERSION, DIRECTION_RULE
from data_pipeline.analysis.detection.projection import DetectionProjection
from data_pipeline.analysis.detection.adapter import adapt_element
from data_pipeline.analysis.detection.streaming import StreamingDetectionEngine, StreamingSeed
from data_pipeline.analysis.detection.store import DetectionStore
from data_pipeline.analysis.detection.roles import ROLE_RULE
from data_pipeline.analysis.detection.classification import CLASSIFICATION_RULE


def run(
    reader,
    references,
    scope,
    boundaries,
    dsn,
    output,
    *,
    root,
    identity_builder,
    fixture_only=False,
    max_rss_bytes=1024**3,
    min_free_bytes=128 * 1024**2,
):
    """Reader必须显式排序为一个基线及后续UPDATE；不支持恢复旧活动事件。"""

    def verify_execution():
        return execution_identity(root, identity_builder, fixture_only=fixture_only)

    if (
        scope.input_version != f"{reader.run_id}:{reader.snapshot}"
        or scope.collector_id != reader.manifest["collector"]
    ):
        raise ValueError("计算scope与固定观察版本/collector不一致")
    if (
        hasattr(references, "snapshot_ref")
        and references.snapshot_ref != f"{reader.run_id}:{reader.snapshot}"
    ):
        raise ValueError("参考视图与固定观察快照不一致")
    identity = verify_execution()
    identity = {
        **identity,
        "selected_sources": list(reader.sources),
        "input_run": reader.run_id,
        "input_snapshot": reader.snapshot,
        "reference_version": references.version,
        "reference_sources": references.metadata().raw_rows
        if hasattr(references, "metadata")
        else references.raw_rows,
        "decoder_version": DECODER_VERSION,
        "direction_rule": DIRECTION_RULE,
        "projection_version": DetectionProjection.version,
        "algorithm_version": scope.computation_version,
        "role_rule_version": ROLE_RULE,
        "classification_rule_version": CLASSIFICATION_RULE,
        "store_schema_version": "detection-typed/v1",
        "resource_limits": {
            "max_rss_bytes": max_rss_bytes,
            "min_free_bytes": min_free_bytes,
            "record_bytes": 8 * 1024**2,
            "state_item_bytes": 8 * 1024**2,
        },
        "python_hash_seed": os.environ.get("PYTHONHASHSEED", "Unknown")
        if not sys.flags.ignore_environment
        else "Unknown_ignored_environment",
    }
    store = DetectionStore(dsn, output, scope, identity)
    ends, engine = [], None
    stream = iter(reader.stream())
    last_message = None

    def guard():
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (
            1 if sys.platform == "darwin" else 1024
        )
        if rss > max_rss_bytes or shutil.disk_usage(output).free < min_free_bytes:
            raise ValueError("Detection内存或磁盘资源保护触发")

    store.guard = guard

    def validate_upstream():
        # 复用Reader登记/来源门禁；参考仅复验已有回执，不另实现数据Reader。
        connection = reader.connect()
        connection.close()
        if hasattr(references, "metadata"):
            with psycopg2.connect(reader.dsn) as pg:
                pg.set_session(readonly=True)
                with pg.cursor() as cursor:
                    for source in references.metadata().raw_rows.values():
                        cursor.execute(
                            "SELECT state,row_count FROM domeye.reference_inputs WHERE run_id=%s AND source_id=%s",
                            (reader.run_id, source["source_id"]),
                        )
                        if cursor.fetchone() != ("validated", source["rows"]):
                            raise ValueError("绑定参考资格或完整行回执发生漂移")
        elif not fixture_only:
            raise ValueError("正式运行必须绑定保存参考来源")

    def validate_before_commit():
        verify_execution()
        validate_upstream()

    def audit(kind, payload):
        store.emit(
            {
                "kind": kind,
                "scope": asdict(scope),
                "reference_version": references.version,
                "reference_historical_applicability": "Unknown",
                "evidence": {},
                "legacy": payload,
            }
        )

    def source_elements(start):
        nonlocal last_message
        messages = elements = 0
        last_message = None
        for item in stream:
            guard()
            if (item.run_id, item.snapshot, item.source_id) != (
                start.run_id,
                start.snapshot,
                start.source_id,
            ):
                raise ValueError("来源结束前身份发生变化")
            if isinstance(item, SourceEnd):
                if (messages, elements) != (
                    start.expected_messages,
                    start.expected_elements,
                ) or (item.messages, item.elements) != (messages, elements):
                    raise ValueError("来源元素/消息计数不符")
                ends.append(asdict(item))
                audit("source_end", asdict(item))
                return
            if not isinstance(item, MessageBatch):
                raise ValueError("来源流未按Start/Batch/End闭合")
            audit(
                "source_quality", item.source_quality
            ) if item.source_quality else None
            known = {last_message["message_id"]: last_message} if last_message else {}
            known.update((message["message_id"], message) for message in item.messages)
            for message in item.messages:
                audit("source_message", message)
                messages += 1
            for element in item.elements:
                message = known.get(element["message_id"])
                if message is None:
                    raise ValueError("元素缺少消息上下文")
                elements += 1
                yield adapt_element(
                    element,
                    run_id=item.run_id,
                    snapshot=item.snapshot,
                    collector_id=scope.collector_id,
                    quality=message["quality"],
                )
            if item.messages:
                last_message = item.messages[-1]
        raise ValueError("缺少SourceEnd，禁止准入")

    try:
        verify_execution()
        start = next(stream)
        if not isinstance(start, SourceStart) or start.role != "baseline":
            raise ValueError("首来源必须为明确RIB基线")
        audit("source_start", asdict(start))
        seed = StreamingSeed(
            source_elements(start),
            f"{start.run_id}:{start.snapshot}/{start.source_id}",
            start.expected_elements,
        )
        engine = StreamingDetectionEngine(seed, references, scope, sink=store.emit)
        for start in stream:
            if not isinstance(start, SourceStart) or start.role != "update":
                raise ValueError("基线后只允许明确UPDATE来源")
            audit("source_start", asdict(start))
            boundary = boundaries[start.source_id]
            if (
                boundary.source_version != f"{start.run_id}:{start.snapshot}"
                or boundary.file_id != start.source_id
            ):
                raise ValueError("文件边界必须绑定Reader精确来源和快照")
            engine.begin_file(boundary)
            for element in source_elements(start):
                engine.consume(element)
                if engine.status in ("failed", "partial"):
                    raise ValueError("Detection计算失败，禁止继续")
            engine.finish_file()
            if engine.status in ("failed", "partial"):
                raise ValueError("Detection文件尾失败")
        if [row["source_id"] for row in ends] != list(reader.sources):
            raise ValueError("未完整消费声明来源")
        guard()
        store.save_state(engine)
        audit(
            "input_completion",
            {"sources": ends, "baseline_count": engine.baseline_count},
        )
        snapshot = store.finish(validate_before_commit=validate_before_commit)
        return {
            "run_id": store.run_id,
            "snapshot": snapshot,
            "state": "complete",
            "output": str(Path(output).resolve()),
            "source_receipts": ends,
        }
    except BaseException as error:
        state, snapshot = store.fail(str(error))
        if state == "complete":
            return {
                "run_id": store.run_id,
                "snapshot": snapshot,
                "state": "complete",
                "output": str(Path(output).resolve()),
                "source_receipts": ends,
                "commit_confirmation": "verified_after_error",
            }
        raise
    finally:
        stream.close() if hasattr(stream, "close") else None
        store.close()
