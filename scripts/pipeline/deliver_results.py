#!/usr/bin/env python3
"""把指定连续完成文件交付至独立结果库；重复执行只补送未交付文件。"""
import argparse
import json
import signal
import sys
import threading
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))
import psycopg2
from data_pipeline.results.delivery import initialize, import_file
from data_pipeline.results.delivery_worker import DeliveryWorker, run_binding, source_window


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run',type=Path,required=True)
    p.add_argument('--files',type=int)
    p.add_argument('--dsn-file',type=Path,required=True)
    p.add_argument('--follow',action='store_true',help='独立等待并交付新完成文件，数据库断连自动重试')
    p.add_argument('--progress',type=Path,help='跟随模式的 Git 外进度文件')
    p.add_argument('--receipt-dir',type=Path,help='默认读取原运行 run/results；验证时可显式指定回执到达目录')
    p.add_argument('--poll-seconds',type=float,default=5)
    args=p.parse_args()
    if args.follow:
        if not args.progress or not 1 <= args.poll_seconds <= 60:
            p.error('跟随模式需要 --progress，轮询间隔须为 1 至 60 秒')
        stop=threading.Event()
        for sig in (signal.SIGTERM,signal.SIGINT):
            signal.signal(sig,lambda *_:stop.set())
        try:
            worker=DeliveryWorker(args.run,args.dsn_file,args.progress,files=args.files,receipt_dir=args.receipt_dir)
        except (ValueError,KeyError,OSError) as error:
            p.error(str(error))
        return worker.follow(stop,poll_seconds=args.poll_seconds)
    if args.files is None or args.receipt_dir or args.progress:
        p.error('单次交付需要 --files；回执目录与进度参数仅用于跟随模式')
    manifest,binding=run_binding(args.run)
    if not 1 <= args.files <= len(manifest['inputs']):raise ValueError('文件数量超出原输入')
    conn=psycopg2.connect(args.dsn_file.read_text().strip())
    initialize(conn,binding)
    started=time.monotonic()
    try:
        for ordinal,source in enumerate(manifest['inputs'][:args.files]):
            start,end=source_window(source)
            result=import_file(conn,args.run/f'run/results/file-{ordinal:04d}.json',source,window_start=start,window_end=end)
            print(json.dumps({**result,'elapsed_seconds':round(time.monotonic()-started,3)},ensure_ascii=False),flush=True)
    finally:conn.close()

if __name__=='__main__':sys.exit(main())
