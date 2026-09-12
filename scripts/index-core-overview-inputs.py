#!/usr/bin/env python3
"""从既有单日留存包生成新按日索引；离线执行，不连接数据库、不覆盖旧目录。"""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from data_pipeline.core_overview_index import build_index  # noqa: E402


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, action='append', type=Path, help='单日包 manifest.json，可重复')
    parser.add_argument('--output', required=True, type=Path, help='尚不存在的新版本目录')
    parser.add_argument('--diagnostic', action='append', default=[], type=Path, help='失败预检证据选择清单，可重复；不准入异常记录')
    args = parser.parse_args()
    print(json.dumps(build_index(args.input, args.output, args.diagnostic), ensure_ascii=False))
