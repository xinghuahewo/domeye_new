"""仅C4人工模块fresh进程读证据，不声明真实P。"""
from contextlib import contextmanager
from pathlib import Path
import json
import sys
import time
from data_pipeline.analysis.country_events.snapshot_schema import encode
from data_pipeline.analysis.country_events.selection_contract import *
from data_pipeline.analysis.country_events.selection_index import iter_country_events
from data_pipeline.analysis.country_events.selection_reader import open_qualified_country

@contextmanager
def fixture_qualification(descriptor,*,lock=False,selection=None):
    if selection is not None and selection.publication_id!='fixture-not-published': raise ValueError('fixture_scope')
    yield

if __name__=='__main__':
    config=json.loads(Path(sys.argv[1]).read_text()); descriptor=contract_value(config['descriptor'])
    runtime=CountryRuntime(config['component_dsn'],verify_qualification=fixture_qualification)
    result={}; costs=[]
    for head in iter_country_events(descriptor):
        if isinstance(head,QueryReadReceipt): continue
        event=head.incident.incident_id
        selection=CountrySelection('fixture-not-published','fixture-build','fixture-profile','country',descriptor,event,head.incident.revision,head.cohort_id,encode(head))
        for page_size in (1,7):
            started=time.monotonic()
            with open_qualified_country(selection,descriptor.admission_proof,runtime=runtime,verify_qualification=fixture_qualification) as query:
                for view in ('overview','series15','asns','asn_matrix','asn_window','asn_peaks','paths','path_samples','audit'):
                    output=list(query.iter_query(QueryRequest(view),page_size=page_size))
                    result[event+'|'+view+'|'+str(page_size)]=[encode(x) for x in output[:-1]]
                    assert output[-1].rows==len(output)-1
                costs.append(dict(event=event,page_size=page_size,wall_seconds=str(time.monotonic()-started),stats=dict(query.access.stats),query_stats=dict(query.stats)))
    Path(config['output']).write_text(json.dumps({'results':result,'costs':costs}))
