#!/usr/bin/env python3
"""离线比较两个已绑定RIB；不加载环境文件、源库、检测器或Web。"""
import argparse
import json
from pathlib import Path
import signal
import sqlite3
import struct
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'backend'))
from data_pipeline.bgp.snapshots.path_comparison import compare_ribs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for side in ('left', 'right'):
        parser.add_argument('--' + side, type=Path, required=True)
        parser.add_argument('--' + side + '-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    def timeout(*_):
        raise TimeoutError('离线比较超过3600秒上限')
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(3600)
    try:
        profile = json.loads((ROOT / 'config/data-profile.json').read_text())
        result = compare_ribs(args.left, args.right, args.left_sha256, args.right_sha256, args.output, profile)
        print(json.dumps(result, ensure_ascii=False))
    except (OSError, EOFError, ValueError, KeyError, TypeError, IndexError, struct.error, sqlite3.Error) as error:
        print('RIB路径对照失败：' + str(error), file=sys.stderr)
        return 1
    finally:
        signal.alarm(0)
    return 0


if __name__ == '__main__':
    sys.exit(main())
