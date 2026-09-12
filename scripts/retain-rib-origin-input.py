#!/usr/bin/env python3
"""显式离线复读单RIB并保留起源归属统计；不连接数据库、不执行检测。"""
import argparse
import json
from pathlib import Path
import signal
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))
from data_pipeline.rib_origin import retain_origins


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--source-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    def timeout(*_):
        raise TimeoutError('起源统计超过1200秒上限')
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(1200)
    try:
        profile = json.loads((ROOT / 'config/data-profile.json').read_bytes())
        print(json.dumps(retain_origins(args.source, args.source_sha256, args.output, profile), ensure_ascii=False))
    except (OSError, EOFError, ValueError, KeyError, TypeError, IndexError) as error:
        parser.exit(1, '起源留存失败：' + str(error) + '\n')
    finally:
        signal.alarm(0)


if __name__ == '__main__':
    main()
