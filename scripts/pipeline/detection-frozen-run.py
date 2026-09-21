"""显式请求的Detection冻结子进程入口；只读消费已保存观察。"""

import argparse
import json
from pathlib import Path
import resource
import runpy
import shutil
import sys
import time
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("request")
    parser.add_argument("--frozen-child", action="store_true")
    parser.add_argument("--expected-digest")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    shared = runpy.run_path(str(root / "backend/data_pipeline/common/frozen_execution.py"))
    identity_builder = runpy.run_path(
        str(root / "backend/data_pipeline/analysis/detection/identity.py")
    )["identity_builder"]
    if not shared["enter"](
        root,
        "scripts/pipeline/detection-frozen-run.py",
        args.request,
        identity_builder,
        child=args.frozen_child,
        expected_digest=args.expected_digest,
    ):
        return
    sys.path.insert(0, str(root / "backend"))
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.analysis.detection.reference_view import ReferenceView, ReferenceSource
    from data_pipeline.analysis.detection.models import DetectionScope, FileBoundary
    from data_pipeline.analysis.detection.runner import run

    request = json.loads(Path(args.request).read_text())
    work = Path(request["output"]).resolve()
    work.mkdir(parents=True, exist_ok=False)
    max_rss = request.get("max_rss_bytes", 1024**3)
    min_free = request.get("min_free_bytes", 128 * 1024**2)

    def guard():
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (
            1 if sys.platform == "darwin" else 1024
        )
        if rss > max_rss or shutil.disk_usage(work).free < min_free:
            raise ValueError("参考/观察读取触发内存或磁盘保护")

    profile = request.get("input_profile", "complete")
    if profile not in ("complete", "observation"):
        raise ValueError("未知Detection输入profile")
    if profile != "observation" and ("output_profile" in request or "result_window" in request):
        raise ValueError("显式M3输出profile要求observation输入")
    if profile == "observation":
        from data_pipeline.analysis.detection.ordered_runner import run
    reader = ObservationReader(
        request["observation_dsn"],
        request["input_run"],
        request["input_snapshot"],
        request["ordered_sources"],
        guard=guard,
        profile=profile,
    )
    view = ReferenceView(
        request["reference_version"],
        f"{reader.run_id}:{reader.snapshot}",
        work / "reference-view",
        guard=guard,
    )
    reference_started = time.monotonic()
    for source in request["references"]:
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
    reference_stage = {
        "wall_seconds": time.monotonic() - reference_started,
        "sources": len(request["references"]),
        "saved_rows": sum(s["expected_rows"] for s in request["references"]),
        "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024),
        "pid": os.getpid(),
        "memory_scope": "进程启动至参考加载结束累计峰值，冻结子进程RUSAGE_SELF；不含PG服务；不是独立阶段峰值",
    }
    boundaries = {
        source: FileBoundary(**value) for source, value in request["boundaries"].items()
    }
    result = run(
        reader,
        view,
        DetectionScope(**request["scope"]),
        boundaries,
        request["detection_dsn"],
        work / "business",
        root=root,
        identity_builder=identity_builder,
        max_rss_bytes=max_rss,
        min_free_bytes=min_free,
        **({"reference_stage": reference_stage, "result_window": request.get("result_window"), **({"output_profile": request["output_profile"]} if "output_profile" in request else {})} if profile == "observation" else {}),
    )
    (work / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
