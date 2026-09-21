"""真实人工r1/r2载体定点验读；不选择revision，不改原流。"""
from contextlib import closing
from dataclasses import replace
import sqlite3
import pytest
from tests.country_trend.test_country_trend_s2_saved import saved
from data_pipeline.analysis.country_trends.snapshot_reader import TrendReader
from data_pipeline.analysis.country_trends import snapshot_audit as audit
from data_pipeline.analysis.country_events import event_aggregation as c2, snapshot_schema as c3
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch
from data_pipeline.analysis.country_events.selection_contract import contract_value


def test_actual_c3_history_preserved_and_only_status_analyzed(saved):
    descriptor=contract_value(saved.q['country_descriptor']);original=[]
    with closing(ComponentReader(saved.rt.country.component_dsn,descriptor.read_binding.component,c1=saved.rt.country.c1).stream()) as stream:
        for batch in stream:
            if isinstance(batch,ReadBatch):
                original.extend(c3.encode(r if isinstance(r,c2.C2Completion) else (r.incident_id,r.revision,r.value)) for r in batch.rows)
    raw=[r for r in saved.rows if r.kind=='raw_source']
    assert [r.get('raw_typed') for r in raw]==original
    history=[r for r in raw if r.get('source_table')=='input_evidence' and isinstance(c3.decode(r.get('raw_typed'))[2],c2.InputEvidence) and c3.decode(r.get('raw_typed'))[2].kind=='event_revision']
    assert [r.event[1] for r in history]==[1,2]
    events=[r for r in saved.rows if r.kind=='event'];assert len(events)==1 and events[0].event[1]==2
    assert saved.p['events']==1 and all(r.event==events[0].event for r in saved.rows if r.kind!='raw_source' and r.event)
    reader=TrendReader(saved.b,saved.p,runtime=saved.rt)
    assert reader.query('raw_source',key=history[0].key,limit=1).items==(history[0],)


def test_forged_history_identity_rejected(saved):
    raw=next(r for r in saved.rows if r.kind=='raw_source' and r.event and r.event[1]==1)
    incident,revision,value=c3.decode(raw.get('raw_typed'))
    changed=c3.encode((incident,revision,replace(value,original=replace(value.original,incident_id='wrong'))))
    bad=replace(raw,values=tuple((k,changed if k=='raw_typed' else v) for k,v in raw.values))
    with sqlite3.connect(':memory:') as db:
        audit.initialize(db)
        with pytest.raises(ValueError,match='historical_revision_identity'):audit.append(db,bad)
