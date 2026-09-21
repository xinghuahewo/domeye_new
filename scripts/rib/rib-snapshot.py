#!/usr/bin/env python3
"""显式离线准备／登记单个RRC25 RIB共享快照；不连接业务数据库。"""
import argparse
import json
from pathlib import Path
import signal
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'backend'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    prepare = commands.add_parser('prepare', help='计算并核验候选；尚不可被Web发现')
    prepare.add_argument('--source', type=Path, required=True)
    prepare.add_argument('--source-sha256', required=True)
    prepare.add_argument('--collector', required=True)
    prepare.add_argument('--observed-at', required=True)
    prepare.add_argument('--output', type=Path, required=True)
    prepare.add_argument('--max-database-mib', type=int, default=4096, help='查询投影磁盘上限，只允许向下收紧，默认4096 MiB')
    prepare.add_argument('--max-seconds', type=int, default=1200, help='单源prepare调用时长上限，0表示不限制，1—7200秒，默认1200秒')
    register = commands.add_parser('register', help='核验并登记完整候选')
    register.add_argument('--candidate', type=Path, required=True)
    register.add_argument('--registry', type=Path, required=True)
    batch = commands.add_parser('batch', help='固定清单驱动的有界跨日批次')
    batch.add_argument('--manifest', type=Path, required=True)
    batch.add_argument('--registry', type=Path, required=True)
    batch.add_argument('--output', type=Path, required=True)
    batch.add_argument('--max-database-mib', type=int, default=4096)
    batch.add_argument('--max-batch-mib', type=int, default=8192, help='整批投影及登记副本预算，2—91136MiB，默认8192MiB')
    batch.add_argument('--max-seconds', type=int, default=1200, help='整个CLI调用时长上限，0表示不限制，1—1200秒，默认1200秒')
    args = parser.parse_args()

    def timeout(*_):
        raise TimeoutError('离线处理超过时长资源上限')
    signal.signal(signal.SIGALRM, timeout)
    try:
        seconds = getattr(args, 'max_seconds', 1200)
        minimum_seconds = 0 if args.command in {'prepare', 'batch'} else 1
        maximum_seconds = 7200 if args.command == 'prepare' else 1200
        if not minimum_seconds <= seconds <= maximum_seconds:
            raise ValueError(f'时长须在{minimum_seconds}—{maximum_seconds}秒资源上限内')
        # prepare／batch显式传0时取消墙钟截止；其余资源与完整核验仍然执行。
        signal.alarm(seconds)
        if args.command == 'prepare':
            from data_pipeline.bgp.snapshots.snapshot import prepare
            result = prepare(args.source, args.source_sha256, args.collector, args.observed_at, args.output, args.max_database_mib)
        elif args.command == 'batch':
            from data_pipeline.bgp.snapshots.batch import run_batch
            result = run_batch(args.manifest, args.registry, args.output, args.max_database_mib, args.max_batch_mib)
        else:
            from data_pipeline.bgp.snapshots.snapshot_store import register
            result = register(args.candidate, args.registry)
        print(json.dumps(result, ensure_ascii=False))
    except (ValueError, OSError, EOFError, sqlite3.DatabaseError) as error:
        parser.exit(1, f'快照{args.command}失败：{error}\n')
    finally:
        signal.alarm(0)


if __name__ == '__main__':
    main()
