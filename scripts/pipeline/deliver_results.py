#!/usr/bin/env python3
"""把指定连续完成文件交付至独立结果库；重复执行只补送未交付文件。"""
import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))
import psycopg2
from data_pipeline.results.delivery import initialize, import_file


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run',type=Path,required=True)
    p.add_argument('--files',type=int,required=True)
    p.add_argument('--dsn-file',type=Path,required=True)
    args=p.parse_args()
    manifest_path=args.run/'manifest.json'; manifest=json.loads(manifest_path.read_text())
    if not 1 <= args.files <= len(manifest['inputs']):raise ValueError('文件数量超出原输入')
    binding={'schema_version':'completed-file-delivery/v1','source_run':str(args.run.resolve()),
             'manifest_sha256':hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
             'collector':manifest['collector'],'usage':'isolated_flow_validation',
             'original_batch_status':'preserved_in_source_run','observation_archive':'paused_by_user'}
    conn=psycopg2.connect(args.dsn_file.read_text().strip())
    initialize(conn,binding)
    started=time.monotonic()
    try:
        for ordinal,source in enumerate(manifest['inputs'][:args.files]):
            match=re.search(r'(?:bview|updates)\.(\d{8})\.(\d{4})\.',source['path'])
            if not match:raise ValueError('输入文件缺少已知时间定位')
            start=datetime.strptime(''.join(match.groups()),'%Y%m%d%H%M').replace(tzinfo=timezone.utc)
            end=start+(timedelta(minutes=5) if source['role']=='update' else timedelta(0))
            result=import_file(conn,args.run/f'run/results/file-{ordinal:04d}.json',source,window_start=start,window_end=end)
            print(json.dumps({**result,'elapsed_seconds':round(time.monotonic()-started,3)},ensure_ascii=False),flush=True)
    finally:conn.close()

if __name__=='__main__':main()
