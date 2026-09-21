#!/usr/bin/env python3
"""人工多文件规模证据：仅构造观察Arrow批次，不读取真实输入或写数据库。"""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import time
import tracemalloc

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'backend'))
import pyarrow as pa
from data_pipeline.analysis.features.calculation import FileWindow, Reference
from data_pipeline.analysis.features.projection import FeatureAdapter, FeaturePlan, SourceBinding


def benchmark(prefixes, files, updates):
    start = datetime(2026, 2, 28, tzinfo=timezone.utc)
    sources = tuple(SourceBinding(f'fixture-source-{n}', 'a'*64, 'baseline' if n == 0 else 'update',
        FileWindow('synthetic-observations-v1', f'fixture-source-{n}', start + timedelta(minutes=n*5),
                   start + timedelta(minutes=(n+1)*5), start + timedelta(minutes=n*5), 'complete'),
        prefixes if n == 0 else updates, (f'fixture-message-quality:{n}',), 'complete') for n in range(files+1))
    ref = Reference('synthetic-ref-v1', {str(a): '伊朗' for a in range(1,65)},
                    {str(a): 'IR' for a in range(1,65)}, {})
    adapter = FeatureAdapter('ordinary', ref, FeaturePlan('synthetic-observations-v1', 'rrc25', sources), 'synthetic-execution')

    def stream(source, n):
        rows = []
        for i in range(source.expected_elements):
            number = i if n == 0 else (n*updates+i) % prefixes
            prefix = f'10.{number//256}.{number%256}.0/24'
            rows.append({'source_id': source.source_id, 'content_sha256': source.content_sha256, 'record': i,
                'ordinal': 0, 'epoch': int(source.window.start.timestamp()), 'microsecond': 0,
                'event_id': f'{source.source_id}:{i}:0', 'action': 'rib_snapshot' if n == 0 else 'announce',
                'prefix': prefix, 'peer_asn': 100, 'as_path_text': f'100 {number%64+1}', 'path_key': f'path:{number%64+1}'})
            if len(rows) == 1024:
                yield pa.RecordBatch.from_pylist(rows)
                rows.clear()
        if rows:
            yield pa.RecordBatch.from_pylist(rows)

    tracemalloc.start()
    begin = time.perf_counter()
    adapter.consume_source(sources[0], stream(sources[0], 0))
    baseline_seconds = time.perf_counter()-begin
    frozen = adapter.state.projection
    baseline_paths = len(frozen.prefix_dict)
    baseline_members = sum(map(len, frozen.as_prefix.values()))
    retained = []
    touched = set()
    for n, source in enumerate(sources[1:], 1):
        begin = time.perf_counter()
        result = adapter.consume_source(source, stream(source, n))
        collect = next(row for row in result.rows if row.scope == 'collect')
        assert collect.values.v4Prefix_num == prefixes and collect.values.announ_num == updates
        assert len(frozen.prefix_dict) == baseline_paths
        assert sum(map(len, frozen.as_prefix.values())) == baseline_members
        untouched = '10.0.0.0/24'
        touched.update((n*updates+i) % prefixes for i in range(updates))
        if 0 not in touched:
            assert frozen.prefix_dict[untouched] is adapter.state.projection.prefix_dict[untouched]
        retained.append({'file': n, 'seconds': time.perf_counter()-begin, **adapter.copy_metrics})
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {'kind': 'synthetic_fixture_only', 'prefixes': prefixes, 'files': files, 'updates_per_file': updates,
            'baseline_seconds': baseline_seconds, 'python_peak_bytes': peak, 'python_current_bytes': current,
            'baseline_paths_preserved': baseline_paths, 'baseline_members_preserved': baseline_members,
            'files_evidence': retained, 'limits': ['非真实MRT吞吐', '非PG或DuckLake集成', 'Python分配峰值不是进程RSS',
                '仍逐文件扫描资源集合并生成结果', '只读分片避免复制路由路径，未宣称总计算为增量']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--prefixes', type=int, default=16384)
    parser.add_argument('--files', type=int, default=8)
    parser.add_argument('--updates', type=int, default=8)
    args = parser.parse_args()
    if not 1 <= args.prefixes <= 65536 or args.files < 1 or args.updates < 1:
        parser.error('人工规模参数无效')
    print(json.dumps(benchmark(args.prefixes, args.files, args.updates), ensure_ascii=False, indent=2))
