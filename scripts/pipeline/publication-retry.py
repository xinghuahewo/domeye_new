"""默认只读预检；显式 execute 才准备新 attempt 并调用原 Publication。"""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config')
    action=parser.add_mutually_exclusive_group()
    action.add_argument('--execute',action='store_true')
    action.add_argument('--inspect-saved',action='store_true',help='显式在线只读分类，不创建attempt或迁移')
    args=parser.parse_args();spec=json.loads(Path(args.config).read_text())
    resources=None
    if args.execute:
        if spec.get('enabled') is not True:raise ValueError('禁用草案不能执行')
        # 业务模块会导入sqlite3；必须在新解释器中先绑定本次临时目录。
        from data_pipeline.common import process_resources as resources
        resources.bind_sqlite_temp(Path(spec['attempt_root']).parent)
    try:
        run(spec,args)
    finally:
        if resources is not None:
            primary=sys.exc_info()[1]
            try:resources.release_sqlite_temp()
            except BaseException as error:
                if primary is None:raise
                primary.migration_cleanup_errors=[*getattr(primary,'migration_cleanup_errors',[]),
                    dict(operation='sqlite_temp_cleanup',error=str(error))]


def run(spec,args):
    from data_pipeline.results.retry.driver import preflight, prepare_directories, execute, inspect_saved
    from data_pipeline.results.retry.observation import stage
    from data_pipeline.jobs.stage_runner import save_new
    metrics={}
    try:
        with stage(None,'materials','完整材料预检通过的AD数量') as metrics:
            checked=preflight(spec);metrics['processed_count']=len(checked['admissions'])
    except BaseException:
        print(json.dumps(dict(state='failed',observation=metrics),ensure_ascii=False),file=sys.stderr);raise
    if args.inspect_saved:
        print(json.dumps(inspect_saved(checked),ensure_ascii=False));return
    if not args.execute:print(json.dumps(dict(checked['summary'],observation=metrics),ensure_ascii=False));return
    if spec.get('enabled') is not True:raise ValueError('禁用草案不能执行')
    try:
        prepared=prepare_directories(checked)
        save_new(Path(spec['attempt_root'])/'入口材料观测.json',metrics)
        print(json.dumps(execute(prepared),ensure_ascii=False))
    except BaseException as error:
        print(json.dumps(dict(state='failed',observation=getattr(error,'retry_observation',None),
            result=getattr(error,'retry_status',None)),ensure_ascii=False),file=sys.stderr)
        raise


if __name__=='__main__':main()
