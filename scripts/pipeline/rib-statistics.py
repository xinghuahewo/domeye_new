#!/usr/bin/env python3
"""复用已保留 RIB 归档生成统计，或将完成统计显式交付到独立查询库。"""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'backend'))
from data_pipeline.results.rib_statistics import bind_archive, compute_statistics, deliver_statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='operation', required=True)
    prepare = commands.add_parser('prepare')
    prepare.add_argument('--source-run', required=True)
    prepare.add_argument('--archive-run', required=True)
    prepare.add_argument('--ordinal', type=int, default=0)
    prepare.add_argument('--output', required=True)
    deliver = commands.add_parser('deliver')
    deliver.add_argument('--artifact', required=True)
    deliver.add_argument('--dsn-file', required=True)
    args = parser.parse_args()
    if args.operation == 'prepare':
        binding = bind_archive(args.source_run, args.archive_run, args.ordinal)
        result = compute_statistics(binding, args.output)
        print(json.dumps({'status':'complete','snapshot_id':result['snapshot_id']},ensure_ascii=False))
    else:
        import psycopg2
        conn = psycopg2.connect(Path(args.dsn_file).read_text().strip())
        try:
            print(json.dumps(deliver_statistics(conn,args.artifact),ensure_ascii=False))
        finally:
            conn.close()


if __name__ == '__main__':
    main()
