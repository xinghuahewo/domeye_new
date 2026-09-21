"""S2人工实际载体测试运行角色；只读取显式请求，不选latest或真实数据。"""
from contextlib import contextmanager
from data_pipeline.analysis.country_events.selection_contract import CountryRuntime, contract_value
from data_pipeline.analysis.country_events.saved_input import CountrySavedInput, DetectionBinding
from data_pipeline.analysis.country_events.saved_contract import ProductionReceiptBinding
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.analysis.country_trends.snapshot_inputs import Runtime
from data_pipeline.analysis.country_trends.snapshot_store import TrendBinding


def runtime(request):
    roles,c=request['roles'],request['c1']
    reader=ObservationReader(roles['observation_dsn'],c['observation_run'],c['observation_snapshot'],c['sources'])
    receipt=ProductionReceiptBinding(**c['production_receipt'])
    c1=CountrySavedInput(reader,DetectionBinding(roles['detection_dsn'],c['detection_run'],c['detection_snapshot']),legacy_timezone=c['timezone'],production_receipt=receipt,scratch_root=request['private_root'])
    @contextmanager
    def qualify(descriptor,**kwargs):c1._check();yield;c1._check()
    return Runtime(request['private_root'],roles['output_dsn'],CountryRuntime(roles['country_dsn'],c1,qualify),roles['observation_dsn'],roles['detection_dsn'],receipt,roles.get('feature_dsn'),roles.get('reference_dsn'))
