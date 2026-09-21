#!/usr/bin/env python3
"""阶段1独立离线入口；凭据仅由Git外配置读取。"""
import argparse
import json
import os
from pathlib import Path
import sys
from contextlib import nullcontext
# 旧检测使用集合枚举槽位。只为新候选入口冻结 Python 散列，保持旧业务规则。
if __name__=='__main__' and len(sys.argv)>1 and sys.argv[1]=='route-pipeline' and not os.environ.get('PYTHONHASHSEED','').isdigit():
    os.execve(sys.executable,[sys.executable,*sys.argv],{**os.environ,'PYTHONHASHSEED':'0'})
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backend'))
from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.bgp.archive.store import query

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=['produce','checkpoint','route-pipeline','route-file-pipeline','query'])
    p.add_argument('--config',required=True)
    p.add_argument('--manifest')
    p.add_argument('--output')
    p.add_argument('--run-id')
    p.add_argument('--table',default='elements')
    a=p.parse_args()
    c=json.loads(Path(a.config).read_text())
    if a.command in ('produce','checkpoint','route-pipeline','route-file-pipeline'):
        if not a.manifest or not a.output:p.error('produce需要manifest/output')
        manifest=json.loads(Path(a.manifest).read_text())
        if c.get('require_readonly_inputs'):
            paths=[Path(i['path']).resolve() for i in manifest['inputs']+manifest.get('references',[])]+[Path(__file__).resolve()]
            checks=[{'path':str(path),'readonly':bool(os.statvfs(path).f_flag & os.ST_RDONLY)} for path in paths]
            if not all(item['readonly'] for item in checks):raise RuntimeError('实际只读挂载门禁拒绝')
            receipt=Path(c['preflight_receipt'])
            with receipt.open('x') as f:json.dump({'checks':checks},f,ensure_ascii=False)
        if a.command in ('route-pipeline','route-file-pipeline'):
            if a.command=='route-pipeline':
                from data_pipeline.bgp.pipeline import run_pipeline
                extra=dict(audit_transitions=c.get('audit_transitions',False),native_rib_state=c.get('native_rib_state',False),
                    business_config=json.loads(Path(c['business_config']).read_text()) if c.get('business_config') else None)
            else:
                from data_pipeline.bgp.replay.file_pipeline import run_pipeline
                extra=dict(max_pending_files=c.get('max_pending_files',2))
            from data_pipeline.common.run_metrics import ResourceSampler
            if not c.get('native_build'):p.error('route-pipeline 需要显式 native_build')
            with ResourceSampler(a.output,c['resource_cgroup']) if c.get('resource_cgroup') else nullcontext():
                result=run_pipeline(manifest,c['dsn'],a.output,run_id=a.run_id,native_build=c['native_build'],
                    catalog_data_path=c.get('catalog_data_path'),policy=c.get('read_policy','strict/v1'),
                    **extra,**c.get('limits',{}))
        elif a.command=='checkpoint':
            from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
            from data_pipeline.common.run_metrics import ResourceSampler
            with ResourceSampler(a.output,c['resource_cgroup']) if c.get('resource_cgroup') else nullcontext():
                result=produce_checkpointed(manifest,c['dsn'],a.output,catalog_data_path=c.get('catalog_data_path'),policy=c.get('read_policy','strict/v1'),native_build=c.get('native_build'),**c.get('limits',{}))
        else:result=produce(manifest,c['dsn'],a.output,catalog_data_path=c.get('catalog_data_path'),**c.get('limits',{}))
        print(json.dumps(result,ensure_ascii=False))
    else:
        print(json.dumps(query(c['dsn'],a.run_id,a.table),ensure_ascii=False,default=str))


if __name__=='__main__':main()
