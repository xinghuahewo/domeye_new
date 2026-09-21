"""Resource正式离线入口：请求JSON经独立只读源码快照在新解释器执行。"""
import argparse
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
from types import SimpleNamespace


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('request', help='含dsn、upstream_run、contexts、output及选项的JSON文件')
    parser.add_argument('--snapshot',help='复用既有只读显式代码归档目录')
    parser.add_argument('--frozen-child', action='store_true', help=argparse.SUPPRESS)
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[2]
    # run_path只执行stdlib身份模块，不触发data_pipeline或resources父包。
    identity_api=runpy.run_path(str(root/'backend/data_pipeline/analysis/resources/identity.py'))
    identity=identity_api['code_identity'](root)
    if not args.frozen_child:
        if args.snapshot:
            subprocess.run([sys.executable,'-I','-B',str(Path(args.snapshot).resolve()/'scripts/pipeline/resource-frozen-run.py'),
                str(Path(args.request).resolve()),'--frozen-child'],check=True)
            return
        with tempfile.TemporaryDirectory(prefix='resource-code-') as temp:
            frozen=Path(temp)
            for relative in identity['files']:
                dest=frozen/relative
                dest.parent.mkdir(parents=True,exist_ok=True)
                dest.write_bytes((root/relative).read_bytes())
                dest.chmod(0o444)
            if identity_api['code_identity'](frozen)!=identity:
                raise ValueError('复制期间代码版本变化')
            directories=sorted((p for p in frozen.rglob('*') if p.is_dir()),key=lambda p:len(p.parts),reverse=True)
            for path in [*directories,frozen]:path.chmod(0o555)
            try:
                subprocess.run([sys.executable,'-I','-B',str(frozen/'scripts/pipeline/resource-frozen-run.py'),
                    str(Path(args.request).resolve()),'--frozen-child'],check=True)
            finally:
                for path in [frozen,*reversed(directories)]:path.chmod(0o755)
        return
    if not sys.flags.isolated or not sys.dont_write_bytecode:
        raise ValueError('正式入口需要隔离新解释器')
    if any(p.stat().st_mode & 0o222 for p in [root,*root.rglob('*')]):
        raise ValueError('正式入口需要只读代码快照')
    if list(root.rglob('*.pyc')):
        raise ValueError('代码快照不得包含缓存字节码')
    if any(n=='data_pipeline' or n.startswith('data_pipeline.') or n=='utils' or n.startswith('utils.') for n in sys.modules):
        raise ValueError('绑定前已有项目业务模块')
    sys.modules['_resource_frozen_entry']=SimpleNamespace(root=root,identity=identity,pid=os.getpid())
    sys.path.insert(0,str(root/'backend'))
    from datetime import datetime
    from data_pipeline.analysis.resources import RibContext
    request=json.loads(Path(args.request).read_text())
    operation=request.pop('operation','resource')
    if operation=='register_reference':
        from data_pipeline.analysis.resources.references import register_reference
        result=register_reference(**request)
    elif operation in ('resource','resource_observation'):
        from data_pipeline.analysis.resources.produce import produce_bound_resources, produce_resources
        from data_pipeline.analysis.resources.bindings import SourceBinding
        if 'sources' in request:
            request['sources']=[SourceBinding(run_id=s['run_id'],snapshot=s['snapshot'],purpose=s['purpose'],
                context=RibContext(**{**s['context'],'snapshot_time':datetime.fromisoformat(s['context']['snapshot_time'].replace('Z','+00:00'))})) for s in request['sources']]
            if operation=='resource_observation':
                from data_pipeline.analysis.resources.observation import produce_observation_resources
                result=produce_observation_resources(**request)
            else:
                result=produce_bound_resources(**request)
        else:
            # 旧单上游入口保留用于既有fixture回归；新正式请求要求sources固定绑定。
            if operation=='resource_observation' or not request.get('fixture_only'):raise ValueError('正式请求必须显式sources与result_window')
            request['contexts']=[RibContext(**{**c,'snapshot_time':datetime.fromisoformat(c['snapshot_time'].replace('Z','+00:00'))}) for c in request['contexts']]
            result=produce_resources(**request)
    else:raise ValueError('未知离线操作')
    print(json.dumps(result,ensure_ascii=False,allow_nan=False))



if __name__=='__main__':main()
