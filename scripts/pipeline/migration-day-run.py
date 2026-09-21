#!/usr/bin/env python3
"""一日迁移接线：只读核对／准备；显式启用且固定接受版本后才能运行人工全链。"""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'backend'))
from data_pipeline.jobs.input_plan import FixedInputs, UNAVAILABLE, build_requests, resource_requests
from data_pipeline.jobs.stage_runner import save_new
from data_pipeline.common import process_resources as s3_process_resources


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['check', 'prepare', 'run-fixed', 'run-full'])
    parser.add_argument('--config', required=True, help='Git外显式JSON配置；可能含DSN，不打印其内容')
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    # 禁用草案在元数据读取/目录创建/生产导入之前拒绝。
    if args.command in ('run-fixed', 'run-full') and config.get('enabled') is not True:
        raise ValueError('运行配置未显式启用，禁用草案不得调用producer')
    if args.command == 'run-full':
        parent = Path(config['invocation_root']).parent.resolve(strict=True)
        allowed = [Path(p).resolve(strict=True) for p in config['tail']['trend']['runtime']['allowed_roots']]
        if not any(parent == p or p in parent.parents for p in allowed):
            raise ValueError('trend_s3_sqlite_temp_parent_outside')
        s3_process_resources.bind_sqlite_temp(parent)
    fixed = FixedInputs.load(config['manifest'], config['mapping'], config.get('input'))
    if args.command in ('run-fixed', 'run-full'):
        import os
        import resource
        import shutil
        from data_pipeline.jobs.migration import run_fixed
        os.environ['DOMEYE_DUCKDB_THREADS'] = str(config['threads'])
        def guard():
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
            if peak > config['limits']['max_rss_bytes']:
                raise RuntimeError('驱动进程RSS保护触发')
            if any(shutil.disk_usage(p).free < config['limits']['min_free_bytes'] for p in config['disk_roots']):
                raise RuntimeError('驱动磁盘保护触发')
        print(json.dumps(run_fixed(fixed, config, config['invocation_root'], guard,
                                   full=args.command=='run-full'), ensure_ascii=False))
        return
    result = dict(state='metadata_checked', input_profile=fixed.input_profile, counts=fixed.counts,
                  unavailable=UNAVAILABLE, admission='not_checked', raw_data='not_read')
    if args.command == 'prepare':
        root = Path(config['request_root']).resolve()
        repo = Path(__file__).resolve().parents[2]
        if root == repo or repo in root.parents:
            raise ValueError('含运行配置的请求必须位于Git外')
        seal = json.loads(Path(config['m2_receipt']).read_text())
        requests = build_requests(fixed, seal, config)
        registration, resource = resource_requests(fixed, seal, config,
            json.loads(Path(config['country_receipt']).read_text()) if config.get('country_receipt') else None)
        requests['register-reference'] = registration
        if resource is not None:
            requests['resource'] = resource
        root.mkdir(parents=True, exist_ok=False, mode=0o700)
        for name, request in requests.items():
            save_new(root/(name+'.json'), request)
            (root/(name+'.json')).chmod(0o600)
        result.update(state='requests_prepared', request_names=list(requests),
                      resource='prepared' if resource else 'awaiting_actual_reference_receipt')
        save_new(root/'请求生成回执.json', result)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        if getattr(error, 'migration_cleanup_errors', None):
            print(json.dumps(dict(primary_error=str(error), cleanup_errors=error.migration_cleanup_errors),
                             ensure_ascii=False), file=sys.stderr)
        raise

    finally:
        primary = sys.exc_info()[1]
        try: s3_process_resources.release_sqlite_temp()
        except BaseException as cleanup_error:
            if primary is None: raise
            primary.migration_cleanup_errors = [*getattr(primary,'migration_cleanup_errors',[]),
                dict(operation='sqlite_temp_cleanup', error=str(cleanup_error))]
