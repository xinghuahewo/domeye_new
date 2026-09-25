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


def insert_outage_fixture(db, ident, kind='as_outage', *, projected=True, **changes):
    """只在临时库放入人工事件；小型起止投影与原始正文使用相同时间。"""
    from psycopg2.extras import Json
    data = {'asn': '64500', 'prefix': '192.0.2.0/24', 'source': 'r', 'country': '测试地区',
            's_time': '2026-02-28 17:00:00', 'e_time': None, **changes}
    zone = timezone(timedelta(hours=8))
    def utc(value):
        return datetime.fromisoformat(value).replace(tzinfo=zone).astimezone(timezone.utc).isoformat()
    core = {'start_time': utc(data['s_time']), 'end_time': {
        'state': 'recorded' if data['e_time'] else 'unknown',
        'value': utc(data['e_time']) if data['e_time'] else None,
    }} if projected else None
    with db.cursor() as cur:
        cur.execute('INSERT INTO result_delivery.events '
                    '(incident_id,revision,ordinal,row_number,kind,reference,data,context,core_item) '
                    'VALUES (%s,1,0,0,%s,%s,%s,%s,%s)',
                    (ident, kind, 'fixture/' + ident, Json(data), Json({}), Json(core) if core else None))


@pytest.mark.parametrize('kind,filters', [
    ('as_outage', {}), ('as_outage', {'country': '测试地区'}),
    ('prefix_outage', {}), ('prefix_outage', {'country': '测试地区'}),
    ('prefix_outage', {'asn': '64500'}),
])
def test_outage_sql_retains_overlap_null_end_unprojected_and_selectors(db, kind, filters):
    from data_pipeline.results.delivery_read import read_outage_intervals
    zone = timezone(timedelta(hours=8))
    start, end = [datetime(2026, 2, 28, hour, tzinfo=zone) for hour in [18, 19]]
    insert_outage_fixture(db, 'open', kind, s_time='2026-02-27 17:00:00')
    insert_outage_fixture(db, 'duplicate', kind, s_time='2026-02-27 17:00:00')
    insert_outage_fixture(db, 'ended_at_start', kind, e_time='2026-02-28 18:00:00')
    insert_outage_fixture(db, 'starts_at_end', kind, s_time='2026-02-28 19:00:00')
    insert_outage_fixture(db, 'crosses_end', kind, s_time='2026-02-28 18:59:00', e_time='2026-02-28 19:10:00')
    insert_outage_fixture(db, 'unprojected', kind, projected=False, s_time='2026-02-28 18:10:00', e_time='2026-02-28 18:20:00')
    insert_outage_fixture(db, 'other_country', kind, asn='64501', prefix='198.51.100.0/24', country='另一地区')
    insert_outage_fixture(db, 'other_source', kind, source='other')
    rows = read_outage_intervals(kind, start, end, conn=db, **filters)
    identifier = '64500' if kind == 'as_outage' else '192.0.2.0/24'
    expected = {
        (identifier, datetime(2026, 2, 27, 17, tzinfo=zone), None),
        (identifier, datetime(2026, 2, 28, 18, 59, tzinfo=zone), datetime(2026, 2, 28, 19, 10, tzinfo=zone)),
        (identifier, datetime(2026, 2, 28, 18, 10, tzinfo=zone), datetime(2026, 2, 28, 18, 20, tzinfo=zone)),
    }
    if not filters:
        expected.add(('64501' if kind == 'as_outage' else '198.51.100.0/24',
                      datetime(2026, 2, 28, 17, tzinfo=zone), None))
    assert set(rows) == expected and len(rows) == len(expected)


def test_outage_sql_does_not_read_large_evidence_for_outside_window_events(db):
    """用实际缓冲页访问量发现无关正文扫描；不以机器快慢作为断言。"""
    from data_pipeline.results.delivery_read import read_outage_intervals
    captured = []
    class Cursor(psycopg2.extensions.cursor):
        def execute(self, query, values=None):
            captured.append((query, values))
            return super().execute(query, values)
    class Connection:
        def cursor(self):
            return db.cursor(cursor_factory=Cursor)
    evidence = ''.join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(512))
    for i in range(128):
        insert_outage_fixture(db, 'past' + str(i), e_time='2026-02-28 17:10:00', evidence=evidence)
    insert_outage_fixture(db, 'current')
    zone = timezone(timedelta(hours=8))
    rows = read_outage_intervals('as_outage', datetime(2026, 2, 28, 18, tzinfo=zone),
                                datetime(2026, 2, 28, 19, tzinfo=zone),
                                country='测试地区', conn=Connection())
    assert len(rows) == 1
    query, values = captured[-1]
    with db.cursor() as cur:
        cur.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + query, values)
        plan = cur.fetchone()[0][0]['Plan']
    accesses = plan['Shared Hit Blocks'] + plan['Shared Read Blocks']
    assert accesses < 128, f'读取一条中断仍访问了 {accesses} 个缓冲页，扫描了窗口外的大正文'


def test_outage_sql_incomplete_projection_still_checks_original_end(db):
    from data_pipeline.overview.input import InputError
    from data_pipeline.results.delivery_read import read_outage_intervals
    zone = timezone(timedelta(hours=8))
    start, end = [datetime(2026, 2, 28, hour, tzinfo=zone) for hour in [18, 19]]
    insert_outage_fixture(db, 'incomplete')
    with db.cursor() as cur:
        cur.execute("UPDATE result_delivery.events SET core_item='{}'::jsonb")
    assert len(read_outage_intervals('as_outage', start, end, conn=db)) == 1
    with db.cursor() as cur:
        cur.execute("UPDATE result_delivery.events SET data=data-'e_time'")
    with pytest.raises(InputError, match='不完整'):
        read_outage_intervals('as_outage', start, end, conn=db)


def test_rib_statistics_cli_transaction_retry_and_conflict(db,tmp_path,monkeypatch,capsys):
    from pathlib import Path
    import runpy
    from data_pipeline.results.rib_statistics import PROFILE, encoded, deliver_statistics
    t=datetime(2026,2,24,tzinfo=timezone.utc)
    f=receipt(tmp_path,[('feature_result',feature())])
    import_file(db,f,{'source_id':'source0','sha256':'input-sha'},window_start=t,window_end=t+timedelta(minutes=5))
    body={'schema_version':PROFILE,'observed_at':t.isoformat(),'source_id':'source0','source_sha256':'input-sha'}
    body['snapshot_id']='rib_statistics_v1_'+hashlib.sha256(encoded(body)).hexdigest()
    artifact=tmp_path/'statistics.json';artifact.write_bytes(encoded(body))
    dsn=tmp_path/'writer.dsn';dsn.write_text(os.environ['DOMEYE_DELIVERY_TEST_DSN']);dsn.chmod(0o600)
    script=Path(__file__).resolve().parents[2]/'scripts/pipeline/rib-statistics.py'
    monkeypatch.setattr('sys.argv',[str(script),'deliver','--artifact',str(artifact),'--dsn-file',str(dsn)])
    for expected in ('delivered','already_delivered'):
        runpy.run_path(str(script),run_name='__main__')
        assert json.loads(capsys.readouterr().out)['status']==expected
    body.pop('snapshot_id');body['observed_at']=(t+timedelta(minutes=1)).isoformat()
    body['snapshot_id']='rib_statistics_v1_'+hashlib.sha256(encoded(body)).hexdigest()
    artifact.write_bytes(encoded(body))
    with pytest.raises(ValueError,match='冲突'):deliver_statistics(db,artifact)
    with db.cursor() as cur:
        cur.execute('SELECT count(*),min(observed_at) FROM result_delivery.rib_statistics')
        assert cur.fetchone()==(1,t)


def make_worker(db, tmp_path, files=2):
    from data_pipeline.results.delivery_worker import DeliveryWorker
    run=tmp_path/'run'; run.mkdir(); spool=tmp_path/'spool'; spool.mkdir()
    manifest={'collector':'rrc25','inputs':[
        {'source_id':'source'+str(n),'sha256':'input-sha','role':'baseline' if n==0 else 'update',
         'path':f'/fixture/{"bview" if n==0 else "updates"}.20260224.0000.gz'} for n in range(files)]}
    (run/'manifest.json').write_text(json.dumps(manifest))
    dsn=tmp_path/'writer.dsn'; dsn.write_text(os.environ['DOMEYE_DELIVERY_TEST_DSN'])
    worker=DeliveryWorker(run,dsn,tmp_path/'progress.json',receipt_dir=spool)
    from psycopg2.extras import Json
    with db,db.cursor() as c:
        c.execute('UPDATE result_delivery.binding SET body=%s WHERE id=1',(Json(worker.binding),))
    return worker, spool


def test_worker_waits_for_atomic_complete_and_reads_new_update(db,tmp_path):
    worker,spool=make_worker(db,tmp_path)
    try:
        assert worker.step()['state']=='waiting_source'
        source=receipt(tmp_path,[('feature_result',feature())])
        pending=spool/'file-0000.json.tmp'; pending.write_bytes(source.read_bytes())
        assert worker.step()['state']=='waiting_source'
        pending.replace(spool/'file-0000.json')
        assert worker.step()['files']==1
        (worker.run/'execution-end.json').write_text('{"status":"failed"}')
        status=worker.step()
        assert status['state']=='waiting_source' and status['source_status']=='failed' and status['files']==1
        row=feature(n=3); row['source_id']='source1'
        source=receipt(tmp_path,[('feature_result',row)],ordinal=1)
        (spool/'file-0001.json').write_bytes(source.read_bytes())
        assert worker.step()['files']==2
        assert worker.step()['state']=='complete'
        with db,db.cursor() as c:
            c.execute('SELECT announ_num FROM feature_country ORDER BY announ_num')
            assert c.fetchall()==[(1,),(3,)]
    finally:worker.close()


def test_worker_recovers_database_loss_after_committed_file_without_duplicate(db,tmp_path,monkeypatch):
    import threading
    from data_pipeline.results import delivery_worker
    worker,spool=make_worker(db,tmp_path,files=1)
    source=receipt(tmp_path,[('feature_result',feature())])
    (spool/'file-0000.json').write_bytes(source.read_bytes())
    original=delivery_worker.import_file; calls=[]
    def lost_ack(*args,**kwargs):
        result=original(*args,**kwargs); calls.append(result)
        raise psycopg2.OperationalError('fixture: commit 后连接中断')
    monkeypatch.setattr(delivery_worker,'import_file',lost_ack)
    assert worker.follow(threading.Event(),poll_seconds=0)==0
    assert len(calls)==1
    with db,db.cursor() as c:
        c.execute('SELECT count(*) FROM result_delivery.files'); assert c.fetchone()==(1,)
        c.execute('SELECT count(*) FROM result_delivery.features'); assert c.fetchone()==(1,)
    assert json.loads(worker.progress.read_text())['state']=='complete'


def test_worker_blocks_invalid_file_and_preserves_previous_data(db,tmp_path):
    import threading
    worker,spool=make_worker(db,tmp_path)
    source=receipt(tmp_path,[('feature_result',feature())])
    (spool/'file-0000.json').write_bytes(source.read_bytes())
    row=feature(); row['source_id']='source1'
    source=receipt(tmp_path,[('feature_result',row)],ordinal=1)
    invalid=json.loads(source.read_bytes()); invalid['file']['sha256']='0'*64
    (spool/'file-0001.json').write_text(json.dumps(invalid))
    assert worker.follow(threading.Event())==2
    assert json.loads(worker.progress.read_text())['state']=='blocked'
    with db,db.cursor() as c:
        c.execute('SELECT count(*) FROM result_delivery.files'); assert c.fetchone()==(1,)
        c.execute('SELECT announ_num FROM feature_country'); assert c.fetchone()==(1,)


def test_worker_rejects_changed_manifest_before_consuming(db,tmp_path):
    worker,_=make_worker(db,tmp_path)
    path=worker.run/'manifest.json'; path.write_text(path.read_text()+' ')
    with pytest.raises(ValueError,match='清单发生变化'):worker.step()


def test_legacy_partial_receipt_waits_then_blocks_only_if_unchanged(db,tmp_path,monkeypatch):
    from data_pipeline.results import delivery_worker
    worker,spool=make_worker(db,tmp_path,files=1)
    clock=[0]; monkeypatch.setattr(delivery_worker.time,'monotonic',lambda:clock[0])
    path=spool/'file-0000.json'; path.write_text('{')
    try:
        assert worker.step()['state']=='waiting_receipt'
        clock[0]=31
        with pytest.raises(ValueError,match='持续不完整'):worker.step()
        source=receipt(tmp_path,[('feature_result',feature())])
        path.write_bytes(source.read_bytes())
        assert worker.step()['state']=='delivered'
    finally:worker.close()


def test_completed_receipt_publication_is_atomic_and_never_overwrites(tmp_path,monkeypatch):
    from data_pipeline.bgp.archive import checkpoint
    target=tmp_path/'file-0000.json'; original=checkpoint.json.dump
    def observe(value,stream,**kwargs):
        assert not target.exists()
        original(value,stream,**kwargs)
        assert not target.exists()
    monkeypatch.setattr(checkpoint.json,'dump',observe)
    checkpoint.durable(target,{'complete':True})
    assert json.loads(target.read_text())=={'complete':True}
    monkeypatch.setattr(checkpoint.json,'dump',original)
    with pytest.raises(FileExistsError):checkpoint.durable(target,{'complete':False})
    assert json.loads(target.read_text())=={'complete':True}
    assert list(tmp_path.iterdir())==[target]
