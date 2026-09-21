#!/usr/bin/env python3
"""迁移Canonical薄入口；仅调用项目既有produce_projection。"""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'backend'))


def main():
    from data_pipeline.jobs.migration import canonical_worker
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('request')
    args = parser.parse_args()
    print(json.dumps(canonical_worker(args.request), ensure_ascii=False))


if __name__ == '__main__':
    main()
