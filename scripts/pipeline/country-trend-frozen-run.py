"""人工strict C3→趋势S2；显式运行角色，冻结新解释器，不发布。"""
import argparse,json,runpy,sys
from pathlib import Path


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('request')
    parser.add_argument('--frozen-child',action='store_true');parser.add_argument('--expected-digest')
    args=parser.parse_args();root=Path(__file__).resolve().parents[2]
    identity=runpy.run_path(str(root/'backend/data_pipeline/analysis/country_trends/execution_identity.py'))
    if not identity['API']['enter'](root,'scripts/pipeline/country-trend-frozen-run.py',args.request,identity['code_identity'],child=args.frozen_child,expected_digest=args.expected_digest):return
    sys.path.insert(0,str(root/'backend'))
    from dataclasses import asdict
    from contextlib import contextmanager
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.analysis.country_events.saved_input import CountrySavedInput, DetectionBinding
    from data_pipeline.analysis.country_events.saved_contract import ProductionReceiptBinding
    from data_pipeline.analysis.country_events.selection_contract import CountryRuntime, contract_value
    from data_pipeline.analysis.country_trends.snapshot_inputs import Runtime
    from data_pipeline.analysis.country_trends.snapshot_store import prepare_trend, S2Limits
    request=json.loads(Path(args.request).read_text())
    allowed={'country_descriptor','roles','c1','private_root','output','receipt','feature_selections','feature_receipt','reference_binding','limits','rules'}
    if set(request)-allowed:raise ValueError('trend_request_fields')
    desc=contract_value(request['country_descriptor']);roles=request['roles'];c=request['c1']
    reader=ObservationReader(roles['observation_dsn'],c['observation_run'],c['observation_snapshot'],c['sources'])
    receipt=ProductionReceiptBinding(**c['production_receipt'])
    c1=CountrySavedInput(reader,DetectionBinding(roles['detection_dsn'],c['detection_run'],c['detection_snapshot']),legacy_timezone=c['timezone'],production_receipt=receipt,scratch_root=request['private_root'])
    @contextmanager
    def qualify(descriptor,**kwargs):
        c1._check();yield;c1._check()
    rt=Runtime(request['private_root'],roles['output_dsn'],CountryRuntime(roles['country_dsn'],c1,qualify),roles['observation_dsn'],roles['detection_dsn'],receipt,roles.get('feature_dsn'),roles.get('reference_dsn'))
    selections=tuple((tuple(v[0]),v[1],v[2]) for v in request.get('feature_selections',[]))
    binding,proof=prepare_trend(desc,rt,request['output'],feature_receipt=request.get('feature_receipt'),feature_selections=selections,reference_binding=request.get('reference_binding'),limits=S2Limits(**request.get('limits',{})),rules=tuple(request.get('rules',('country-trend-exact-direction/v1','trend-activity-exact-interval/v1'))))
    Path(request['receipt']).write_text(json.dumps(dict(binding=asdict(binding),proof=proof),ensure_ascii=False))
    print(json.dumps(dict(result_id=binding.result_id,rows=proof['output_rows'],events=proof['events']),ensure_ascii=False))


if __name__=='__main__':main()
