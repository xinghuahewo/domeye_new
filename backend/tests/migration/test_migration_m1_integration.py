"""M1解析层集成：strict坏帧不能成为观察完成态或业务输入。"""
import gzip
import hashlib
import json
from pathlib import Path
import resource
import time
import psycopg2
import pytest
from data_pipeline.bgp.input.mrt_reader import source_identity, PARSER_VERSION
from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.archive.store import connect_duckdb, literal, TABLES
from data_pipeline.bgp.snapshots.origin import InputRejected
from tests.observations.test_observation_consumer import feature_dsn
from tests.observations.test_observation_two_phase import fixture_manifest
from tests.observations.test_observation_mrt import update


def test_strict_bad_payload_production_stays_failed(tmp_path,feature_dsn):
    manifest=fixture_manifest(tmp_path)
    good=update();bad=update(attrs=b'\xf0\x23\x04\x00\x04\x2f\x66')
    entry=manifest['inputs'][1];path=Path(entry['path'])
    path.write_bytes(gzip.compress(good+bad+good,mtime=0))
    entry.update(sha256=hashlib.sha256(path.read_bytes()).hexdigest(),size=path.stat().st_size)
    entry['source_id']=source_identity('rrc25',entry['origin_uri'],entry['sha256'])
    manifest['update_sources']=[e['source_id'] for e in manifest['inputs'][1:]]
    started=time.monotonic()
    with pytest.raises(InputRejected):produce(manifest,feature_dsn,tmp_path/'failed',min_free_bytes=0,batch_rows=1)
    elapsed=time.monotonic()-started
    failure=json.loads((tmp_path/'failed/failure.json').read_text());run=failure['run_id']
    assert failure['state']=='failed' and not (tmp_path/'failed/execution.json').exists()
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as cur:
        cur.execute('SELECT state,snapshot,schema_name FROM domeye.runs WHERE run_id=%s',(run,))
        state,snapshot,schema=cur.fetchone();assert (state,snapshot)==('failed',None)
        cur.execute('SELECT source_id FROM domeye.source_receipts WHERE run_id=%s',(run,))
        assert cur.fetchall()==[(manifest['baseline_source'],)]
        cur.execute('SELECT current_exported,manifest FROM domeye.run_specs WHERE run_id=%s',(run,))
        exported,saved=cur.fetchone();assert not exported
    with pytest.raises(ValueError):ObservationReader(feature_dsn,run,0,manifest['update_sources'])
    db=connect_duckdb()
    try:
        db.execute('LOAD ducklake');db.execute('LOAD postgres')
        db.execute('ATTACH '+literal('ducklake:postgres:'+feature_dsn)+' AS lake (READ_ONLY)')
        messages=db.execute(f'SELECT record,reason,raw_digest FROM lake.{schema}.messages WHERE source_id=? ORDER BY record',[entry['source_id']]).fetchall()
        assert [(r[0],r[2]) for r in messages]==[(0,hashlib.sha256(good).hexdigest()),(1,hashlib.sha256(bad).hexdigest())]
        assert messages[0][1] is None and messages[1][1] is not None
        counts={t:db.execute(f'SELECT count(*) FROM lake.{schema}.{t}').fetchone()[0] for t in ('messages','elements','changes')}
        assert counts=={'messages':4,'elements':2,'changes':0}
    finally:db.close()
    assert not any('interpretation' in name for cols in TABLES.values() for name,_ in cols)
    assert PARSER_VERSION=='mrt-observation/v2'
    source_files=saved['code_identity'];repo=Path(__file__).resolve().parents[3]
    for relative in ('backend/data_pipeline/bgp/input/mrt_reader.py','backend/data_pipeline/bgp/input/mrt_types.py'):
        assert source_files[relative]==hashlib.sha256((repo/relative).read_bytes()).hexdigest()
    (tmp_path/'strict-production-evidence.json').write_text(json.dumps({'failure':failure,'parser_version':PARSER_VERSION,'bound_code':source_files,'counts':counts,'strict_bad_source_records':len(messages),'bad_following_record_not_read':True,'no_completed_snapshot_or_execution':True,'interpretation_not_persisted':True,'wall_seconds':elapsed,'process_cumulative_maxrss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'rss_scope':'pytest进程累计高水位，含进程内DuckDB；非独立阶段峰值','stage_total_rss':'Unknown','postgres_rss':'Unknown','duckdb_separate_rss':'Unknown'},ensure_ascii=False,indent=2))
