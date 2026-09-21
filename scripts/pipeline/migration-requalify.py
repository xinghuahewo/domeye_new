#!/usr/bin/env python3
"""默认只读预检；显式执行固定人工制品的新准入与原尾段。"""
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config');parser.add_argument('--execute',action='store_true')
    args=parser.parse_args();spec=json.loads(Path(args.config).read_text())
    resources=None
    if args.execute:
        if spec.get('enabled') is not True:raise ValueError('禁用安排不能执行')
        from data_pipeline.common import process_resources as resources
        resources.bind_sqlite_temp(Path(spec['attempt_root']).parent)
    try:
        from data_pipeline.jobs.recheck_results import preflight, execute
        if args.execute:result=execute(spec)
        else:
            _,admissions=preflight(spec)
            result=dict(state='materials_checked',admissions=len(admissions),live_current='Unknown',enabled=spec['enabled'])
        print(json.dumps(result,ensure_ascii=False))
    finally:
        if resources is not None:
            primary=sys.exc_info()[1]
            try:resources.release_sqlite_temp()
            except BaseException as error:
                if primary is None:raise
                primary.migration_cleanup_errors=[*getattr(primary,'migration_cleanup_errors',[]),dict(operation='sqlite_temp_cleanup',error=str(error))]

if __name__=='__main__':main()
