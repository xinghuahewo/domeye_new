#!/usr/bin/env python3
"""仅按显式配置执行单 RIB 原生 M2；资源采样覆盖同一隔离 cgroup。"""
import argparse
import json
import os
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))
from data_pipeline.bgp.archive.native_writer import produce_native_rib


def sample(root,stop,cgroup):
    with (root/'resource-samples.jsonl').open('a') as output:
        while not stop.is_set():
            row=dict(monotonic=time.monotonic(),wall_time=time.time(),cgroup={},processes=[])
            try:
                for name in ('cpu.stat','memory.current','memory.peak','memory.events','io.stat'):
                    p=cgroup/name
                    if p.exists():row['cgroup'][name]=p.read_text().strip()
                pids=set()
                for p in cgroup.rglob('cgroup.procs'):
                    try:pids.update(map(int,p.read_text().split()))
                    except OSError:pass
                for pid in pids:
                    try:
                        path=Path('/proc')/str(pid);s=path.joinpath('stat').read_text().rsplit(')',1)[1].split()
                        rss=int(path.joinpath('statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
                        row['processes'].append(dict(pid=pid,name=path.joinpath('comm').read_text().strip(),rss_bytes=rss,
                            user_ticks=int(s[11]),system_ticks=int(s[12])))
                    except (OSError,ValueError,IndexError):pass
            except OSError as exc:row['sample_error']=str(exc)
            output.write(json.dumps(row)+'\n');output.flush();stop.wait(1)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',required=True);args=parser.parse_args()
    config=json.loads(Path(args.config).read_text());group=config.pop('resource_cgroup');root=Path(config['output']);root.mkdir(parents=True,exist_ok=True)
    stop=threading.Event();thread=threading.Thread(target=sample,args=(root,stop,Path(group)),daemon=True);thread.start();start=time.monotonic()
    try:
        seal=produce_native_rib(**config)
        receipt=dict(status='observation_sealed',business='not_run',elapsed_seconds=time.monotonic()-start,
            run_id=seal['run_id'],snapshot=seal['snapshot'],seal_digest=seal['digest'],counts=seal['checkpoints'][0]['counts'])
        (root/'execution.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n');print(json.dumps(receipt,ensure_ascii=False),flush=True)
    finally:stop.set();thread.join()


if __name__=='__main__':main()
