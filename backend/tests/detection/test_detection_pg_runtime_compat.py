"""已接受M2 helper与Detection的真实人工接合，不重产既有制品。"""
import copy
import json
import os
from pathlib import Path
import time
import psycopg2
import pytest
from data_pipeline.bgp.archive import admission as upstream
from data_pipeline.analysis.detection import publication as d
from data_pipeline.analysis.detection.store import read_stored_rows


def test_real_pg_helpers_and_detection_chain():
    config_path=os.environ.get('DETECTION_PG_COMPAT_CONFIG')
    if not config_path:pytest.skip('必须明确自有人工制品配置')
    config=json.loads(Path(config_path).read_text());q=json.loads(Path(config['request']).read_text())
    root=Path(config['output_root']);ready=json.loads((root/'ready.json').read_text());scratch=Path(config['scratch_root'])
    allowed=tuple(Path(p) for p in config['allowed_roots']);events=[]
    u=upstream.Runtime(q['observation_dsn'],allowed,scratch,fixture_only=True)
    ma=upstream.admit(u,upstream.inspect_binding(u,q['input_run'],q['input_snapshot'],q['ordered_sources']),guard=lambda:None);u.dependency_admissions=(ma,)
    deps=[ma]+[upstream.admit(u,upstream.reference_binding(u,ma,x['source_id']),guard=lambda:None) for x in q['references']]
    r=d.Runtime(q['detection_dsn'],root,allowed,scratch,tuple(deps),{a['admission_id']:u for a in deps},fixture_only=True,audit_sink=events.append)
    before=ready.copy();started=time.monotonic()
    binding=d.inspect_binding(r,ready['run_id'],ready['snapshot']);admission=d.admit(r,binding,guard=lambda:None)
    d.verify_current(r,admission,guard=lambda:None)
    request=dict(view='records',scope_typed=d.typed(dict(start=0,stop=None,key=None,at_position=None)),codec_version=d.CODEC,batch_rows=11,batch_bytes=4*1024**2)
    with d.open_reader(r,admission,request,guard=lambda:None) as session:rows=[row for batch in session for row in d.untyped(batch['rows_typed'])]
    assert session.receipt and rows==list(read_stored_rows(r.dsn,binding['run_id'],binding['snapshot'],'records'))
    assert json.loads((root/'ready.json').read_text())==before
    locks=[]
    for target in admission['lock_targets']:
        with d.hold_lock(r,admission,target,guard=lambda:None):
            pg=psycopg2.connect(r.dsn)
            try:
                with pg.cursor() as c:
                    c.execute("SET lock_timeout='100ms'")
                    sql='UPDATE detection.runs SET state=state WHERE run_id=%s' if target['namespace']=='detection.run' else 'UPDATE detection.publication_admissions SET state=state WHERE record_key=%s'
                    with pytest.raises(psycopg2.errors.LockNotAvailable):c.execute(sql,(target['key'],))
            finally:pg.rollback();pg.close()
        locks.append(dict(target=target,blocked=True))
    low=copy.copy(r);low.max_rss_bytes=1
    with pytest.raises(ValueError,match='RSS'):d.inspect_binding(low,binding['run_id'],binding['snapshot'])
    # 已进入实际PG事务后降低临时预算，直接调用共享_sql也必须在执行前拒绝。
    low=copy.copy(r)
    with upstream._pg(low) as pg,pg.cursor() as c:
        assert upstream._sql(low,c,'SELECT 1').fetchone()==(1,)
        probe=scratch/'budget-probe';probe.write_bytes(b'xx');low.max_temp_bytes=1
        try:
            count=len(events)
            with pytest.raises(ValueError,match='临时空间'):upstream._sql(low,c,'SELECT 2')
            assert len(events)==count
        finally:probe.unlink()
    assert r.resource_usage['checks'] > 0 and r._profile == 'synthetic-fixture/v1'
    Path(config['evidence']).write_text(json.dumps(dict(binding=binding,admission=admission,dependencies=deps,records=len(rows),receipt=session.receipt,locks=locks,low_rss_rejected=True,low_temp_sql_rejected_before_execution=True,original_ready_unchanged=True,wall_seconds=time.monotonic()-started,events=events),ensure_ascii=False,indent=2))
