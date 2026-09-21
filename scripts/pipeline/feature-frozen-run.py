"""显式固定观察与窗口的Feature离线入口；冻结源码后才在新进程导入业务。"""
import argparse
import json
from pathlib import Path
import runpy
import sys


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('request')
    parser.add_argument('--frozen-child',action='store_true',help=argparse.SUPPRESS)
    parser.add_argument('--expected-digest',help=argparse.SUPPRESS)
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[2]
    identity=runpy.run_path(str(root/'backend/data_pipeline/analysis/features/identity.py'))
    if not identity['API']['enter'](root,'scripts/pipeline/feature-frozen-run.py',args.request,identity['code_identity'],
                                   child=args.frozen_child,expected_digest=args.expected_digest):return
    sys.path.insert(0,str(root/'backend'))
    from datetime import datetime
    from data_pipeline.analysis.features.calculation import FileWindow
    from data_pipeline.analysis.features.projection import FeaturePlan, SourceBinding
    from data_pipeline.analysis.features.run import produce_features, produce_bound_features
    from data_pipeline.analysis.features.inputs import FeatureInputs, SourceView, ReferenceView
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    request=json.loads(Path(args.request).read_text())
    if 'fixture_only' in request:raise ValueError('正式入口不允许fixture模式覆盖')
    if 'source_views' in request:
        if any(k in request for k in ('sources','observation_run','observation_snapshot','reference_sha','reference_path')):
            raise ValueError('多视图请求不得混用单run绑定')
        views=[]
        for view in request.pop('source_views'):
            window=view.pop('window')
            window.setdefault('input_version',view['run_id']+':'+str(view['snapshot']))
            window.setdefault('file_ref',view['source_id'])
            for name in ('start','end','file_time'):window[name]=datetime.fromisoformat(window[name].replace('Z','+00:00'))
            window['limitations']=tuple(window.get('limitations',()))
            view['message_quality_refs']=tuple(view.get('message_quality_refs',()))
            views.append(SourceView(**view,window=FileWindow(**window)))
        reference=request.pop('reference_view')
        if reference.get('expected_rows') is None:raise ValueError('必须明确绑定参考原回执行数')
        bounds={k:tuple(datetime.fromisoformat(t.replace('Z','+00:00')) for t in request.pop(k))
                for k in ('result_window','comparison_window') if k in request}
        inputs=FeatureInputs(request.pop('dsn'),request.pop('collector'),views,ReferenceView(**reference),**bounds)
        print(json.dumps(produce_bound_features(inputs,**request),ensure_ascii=False))
        return
    sources=[]
    for source in request.pop('sources'):
        window=source.pop('window')
        for name in ('start','end','file_time'):window[name]=datetime.fromisoformat(window[name])
        window['limitations']=tuple(window.get('limitations',()))
        source['message_quality_refs']=tuple(source.get('message_quality_refs',()))
        sources.append(SourceBinding(**source,window=FileWindow(**window)))
    reader=ObservationReader(request.pop('dsn'),request.pop('observation_run'),request.pop('observation_snapshot'),
                             [s.source_id for s in sources])
    plan=FeaturePlan(reader.run_id+':'+str(reader.snapshot),request.pop('collector'),tuple(sources))
    print(json.dumps(produce_features(reader,plan,**request),ensure_ascii=False))


if __name__=='__main__':main()
