#!/usr/bin/env python3
"""独立二进制fixture多批写入测量；每轮独立数据库，仅隔离环境运行。"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import resource
import struct
import sys
import time
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))
import psycopg2
from data_pipeline.bgp.input.mrt_reader import read_source
from data_pipeline.bgp.replay.route_replay import Replay, ReplayPlan
from data_pipeline.bgp.archive.store import Store


def raw_update(origin):
    attrs=b'\x40\x02\x0a\x02\x02'+struct.pack('!II',64497,origin)
    payload=b'\0\0'+struct.pack('!H',len(attrs))+attrs+b'\x18\xc0\0\x02'
    body=struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\0\x02\x01\xc0\0\x02\x02'+b'\xff'*16+struct.pack('!HB',len(payload)+19,2)+payload
    return struct.pack('!IHHI',100,16,4,len(body))+body


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();root=Path(args.output);root.mkdir(parents=True,exist_ok=False)
    dsn=json.loads(Path(args.config).read_text())['dsn']
    admin=psycopg2.connect(dsn);admin.autocommit=True
    results=[]
    for count in (1000,4000,16000):
        name='benchmark_'+uuid.uuid4().hex
        with admin.cursor() as cursor:cursor.execute('CREATE DATABASE '+name)
        source=root/f'{count}.gz'
        with gzip.open(source,'wb') as f:
            for i in range(count):f.write(raw_update(100000+i))
        sha=hashlib.sha256(source.read_bytes()).hexdigest()
        store=Store(dsn+' dbname='+name,root/str(count),batch_rows=4000)
        replay=Replay(ReplayPlan('rrc25','none',(sha,)))
        store.bind({'inputs':[{'sha256':sha}]},replay.plan.version)
        with store.pg,store.pg.cursor() as c:
            c.execute('INSERT INTO domeye.inputs VALUES (%s,%s,%s,%s,%s)',(store.run_id,sha,str(source),source.stat().st_size,'validated'))
        start=time.monotonic()
        for m in read_source(source,sha,source_id=sha):
            store.message(m)
            for change in replay.consume(m):store.change(change)
        store.source_complete(sha,count,count)
        store.save_current(replay);store.finish()
        result={'messages':count,'seconds':time.monotonic()-start,'rows':store.actual_counts,
                'flush':store.flush_metrics,'peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),
                'parquet_bytes':sum(p.stat().st_size for p in (store.root/'parquet').rglob('*.parquet'))}
        results.append(result);store.close();print(count,result['seconds'],flush=True)
    admin.close();(root/'benchmark.json').write_text(json.dumps(results,indent=2))


if __name__=='__main__':main()
