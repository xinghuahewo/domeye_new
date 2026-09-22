"""完成文件交付的事务、重复导入与版本连续性；只使用临时 fixture 库。"""
import hashlib
import json
import os
from datetime import datetime, timezone, timedelta

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from data_pipeline.bgp.replay.snapshot_contract import encode
from data_pipeline.results.delivery import canonical_detail, initialize, import_file, create_query_views


def feature(subject='伊朗', n=1):
    return {'mode':'ordinary','source_id':'source0','raw':{'scope':'country','subject':subject,'country':subject,
        'legacy_table':'feature_country','values':{'announ_num':n,'withdraw_num':0,'v4Prefix_num':2,'v6Prefix_num':None,'v4IP_num':512}},
        'window':{'file_time':{'$datetime':'2026-02-24T00:00:00+00:00'}}}


def receipt(tmp_path, rows, ordinal=0):
    path=tmp_path/f'{ordinal}.parquet';pq.write_table(pa.Table.from_pylist([{'table':t,'payload':encode(x)} for t,x in rows]),path)
    raw={'ordinal':ordinal,'source_id':'source'+str(ordinal),'file':{'path':str(path),'size':path.stat().st_size,
         'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'rows':len(rows)},
         'source_receipt':{'kind':'source_complete','ordinal':ordinal,'source_id':'source'+str(ordinal),'source_sha':'input-sha','counts':{'rejected':0,'unsupported':0}}}
    file=tmp_path/f'{ordinal}.json';file.write_text(json.dumps(raw));return file


@pytest.fixture
def db():
    dsn=os.environ.get('DOMEYE_DELIVERY_TEST_DSN')
    if not dsn:pytest.skip('需要显式绑定临时 fixture 数据库')
    conn=psycopg2.connect(dsn)
    assert conn.get_dsn_parameters()['dbname'].endswith('_fixture')
    with conn,conn.cursor() as cur:
        cur.execute('DROP SCHEMA IF EXISTS result_delivery CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;')
    initialize(conn,{'collector':'rrc25','test':True})
    yield conn
    conn.close()


def test_transaction_idempotency_and_failed_file_isolation(db,tmp_path):
    t=datetime(2026,2,24,tzinfo=timezone.utc)
    source={'source_id':'source0','sha256':'input-sha'}
    f=receipt(tmp_path,[('feature_result',feature())])
    assert import_file(db,f,source,window_start=t,window_end=t+timedelta(minutes=5))['status']=='delivered'
    assert import_file(db,f,source,window_start=t,window_end=t+timedelta(minutes=5))['status']=='already_delivered'
    create_query_views(db)
    with db.cursor() as c:
        c.execute('SELECT announ_num,withdraw_num,v6prefix_num FROM feature_country');assert c.fetchall()==[(1,0,None)]
    bad=feature('坏文件');bad['source_id']='source1'
    f1=receipt(tmp_path,[('feature_result',bad),('feature_result',bad)],ordinal=1)
    with pytest.raises(psycopg2.IntegrityError):import_file(db,f1,{'source_id':'source1','sha256':'input-sha'},window_start=t,window_end=t+timedelta(minutes=10))
    with db.cursor() as c:
        c.execute('SELECT count(*) FROM result_delivery.files');assert c.fetchone()==(1,)
        c.execute('SELECT count(*) FROM result_delivery.features');assert c.fetchone()==(1,)
    raw=json.loads(f.read_text());raw['file']['sha256']='0'*64;f.write_text(json.dumps(raw))
    with pytest.raises(ValueError,match='摘要'):import_file(db,f,source,window_start=t,window_end=t+timedelta(minutes=5))


def test_leak_uses_event_id_and_preserves_unknown_end():
    row={'event_kind':'leak','object':'192.0.2.0/24','legacy_ref':{'source':'r','id':20},
         'legacy':{'is_leak':True,'legacy_event_id':3,'s_time':{'$datetime':'2026-02-24T08:00:00'}}}
    ref,data=canonical_detail(row)
    assert ref=='leak/2026-02-24 08:00:00/192.0.2.0-24/3/r'
    assert data['leak_event_id']==3 and 'e_time' not in data
    row['legacy']['is_leak']=False
    assert canonical_detail(row) is None


def revision(number=1,end=None):
    return {'kind':'business_revision','event_kind':'prefix_outage','incident_id':'det_fixture','revision':number,
       'object':'192.0.2.0/24','legacy_ref':{'source':'r','id':1},
       'legacy':{'s_time':'2026-02-24 08:00:01','e_time':end,'duration':'0 days 0 hours 0 minutes 9 seconds' if end else None,
                 'asn':'64500','outage_level':'low','pre_vp_paths':{},'eve_vp_paths':{},'next_vp_paths':None},
       'scope':{'run_id':'fixture','collector_id':'rrc25','source':'r','window_start':'2026-02-24T00:00:00Z','window_end':'2026-02-25T00:00:00Z','computation_version':'fixture'},
       'end_state':'recorded' if end else 'unknown','conclusion':'候选','reference_version':'fixture','reference_historical_applicability':'Unknown'}


def test_revision_merge_core_consistency_and_partial_hours(db,tmp_path,monkeypatch):
    from data_pipeline.results import delivery_read
    from data_pipeline.overview.input import InputError
    from services.core_overview_service import get_core_overview, get_core_overview_record
    with db,db.cursor() as c:
        c.execute("UPDATE result_delivery.binding SET body=body || '{\"source_run\":\"fixture\"}'::jsonb")
    t=datetime(2026,2,24,tzinfo=timezone.utc)
    f=receipt(tmp_path,[('detection_result',revision()),('detection_result',revision(2,'2026-02-24 08:00:10'))])
    import_file(db,f,{'source_id':'source0','sha256':'input-sha'},window_start=t,window_end=t+timedelta(minutes=5))
    monkeypatch.setattr(delivery_read,'conn_11',db)
    original_status=delivery_read.status
    monkeypatch.setattr(delivery_read,'status',lambda:original_status(db))
    monkeypatch.setenv('DOMEYE_RESULT_DELIVERY','true')
    result=get_core_overview({'date':'2026-02-24'})
    assert result['events']['total']==1
    assert result['overview']['record_count']==1
    assert len(result['trend']['buckets'])==1
    assert result['trend']['buckets'][0]['start'].endswith('08:00:00+08:00')
    item=result['events']['items'][0]
    detail=get_core_overview_record({'ref':item['reference'],'version':result['version']})
    assert detail['item']['content_version']==item['content_version']
    assert detail['item']['end_time']['state']=='recorded'
    assert get_core_overview({'date':'2026-02-24','hour':'7'})['events'] is None
    assert get_core_overview({'date':'2026-02-25'})['state']=='window_not_retained'
    with pytest.raises(InputError,match='版本'):
        get_core_overview_record({'ref':item['reference'],'version':'wrong'})
    with db.cursor() as c:
        c.execute('SELECT count(*) FROM result_delivery.revisions');assert c.fetchone()==(2,)
        c.execute('SELECT revision FROM result_delivery.events');assert c.fetchone()==(2,)


def test_country_numeric_members_preserve_identity_and_unknown():
    row=revision();row.update(event_kind='country_outage',object='SB')
    row['legacy']['outage_ases']=[132462,'{64500,64501}']
    ref,data=canonical_detail(row)
    assert data['outage_ases']==['132462','{64500,64501}']
    assert row['legacy']['outage_ases']==[132462,'{64500,64501}']
