"""文件流水线 fixture 的独立 OS 进程及明确故障点。"""
import functools
import json
from pathlib import Path
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from data_pipeline.bgp.replay.file_pipeline import run_pipeline


def stop_at(label,ordinal,*,stop,at,marker):
    if (label,ordinal)!=(stop,at):return
    Path(marker).write_text(label)
    while True:time.sleep(.1)


def main():
    config=json.loads(Path(sys.argv[1]).read_text())
    run=run_pipeline
    if config.pop('direct_batches',False):
        from data_pipeline.bgp.pipeline import run_pipeline as run
    stop=config.pop('stop',None);ordinal=config.pop('ordinal',None);marker=config.pop('marker',None)
    if stop:
        key='producer_hook' if config.pop('producer_stop',False) else 'hook'
        config[key]=functools.partial(stop_at,stop=stop,at=ordinal,marker=marker)
    result=run(**config)
    print(json.dumps(result),flush=True)


if __name__=='__main__':main()
