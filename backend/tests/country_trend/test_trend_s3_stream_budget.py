"""S3独立累计迁移夹具；SQLite/Parquet均临时文件，无PG或成功AD桩。"""
from dataclasses import replace
import pytest
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits, SharedBudget
from data_pipeline.analysis.country_trends.stream_budget import StreamBudget, connect, install_progress
from data_pipeline.analysis.country_trends import parquet_metadata as s3_parquet
from data_pipeline.analysis.country_trends.stream_files import write_body, read_body
from tests.country_trend.test_country_trend_s3_files import fixture


def budget(tmp_path, guard=lambda: None, **kw):
    return StreamBudget(replace(S2Limits(), max_rows=1, max_total_bytes=1,
        max_references=1, max_index_steps=1, **kw), tmp_path, guard)


def test_totals_count_beyond_old_limits_and_s2_still_rejects(tmp_path):
    b = budget(tmp_path)
    for _ in range(30):
        b.charge(b'ab'); b.refs(2); b.step()
    assert (b.rows, b.byte_count, b.references, b.index_steps) == (30,60,60,30)
    with pytest.raises(ValueError, match='total_bytes'):
        SharedBudget(b.limits,tmp_path,lambda:None).charge(b'ab')


def test_complete_real_files_across_cumulative_limits_and_batch_boundaries(tmp_path):
    rows = fixture()
    proofs=[]
    for batch in (1,3):
        b=budget(tmp_path,max_batch_rows=batch)
        root=tmp_path/str(batch)
        proof=write_body(iter(rows),root,limits=b.limits,budget=b)
        assert tuple(read_body(root,limits=b.limits)) == rows
        assert b.rows == 2*len(rows)
        assert b.references>1
        proofs.append((proof['sha256'],{k:(v['rows'],v['sha256']) for k,v in proof['tables'].items()}))
    assert proofs[0]==proofs[1]


def test_sql_quantum_accumulates_across_installs(tmp_path):
    b=budget(tmp_path)
    db=connect(tmp_path/'test.sqlite',b)
    try:
        query='WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2000) SELECT sum(x) FROM n'
        assert db.execute(query).fetchone()==(2001000,)
        first=b.stats['sqlite_steps'];install_progress(db,b)
        assert db.execute(query).fetchone()==(2001000,)
        assert b.stats['sqlite_steps']>=2*first>1000
    finally:db.close()


# SQL取消/资源失败与合法排序的定向用例已迁入test_trend_s3_resources.py。


def test_footer_limit_precedes_pyarrow_allocation(tmp_path,monkeypatch):
    path=tmp_path/'large.parquet';path.write_bytes(b'PAR1'+b'x'*20+(20).to_bytes(4,'little')+b'PAR1')
    monkeypatch.setattr(s3_parquet.pq,'ParquetFile',lambda *a,**k:pytest.fail('footer loaded'))
    with pytest.raises(ValueError,match='footer_representation'):
        s3_parquet.open_parquet(path,replace(S2Limits(),max_context_bytes=10),lambda:None)


def test_disk_paths_deduplicated_and_single_row_retained(tmp_path):
    path=tmp_path/'sub';path.mkdir();(path/'data').write_bytes(b'x'*32)
    actual=max(32,(path/'data').stat().st_blocks*512)
    b=budget(tmp_path,max_disk_bytes=actual,max_row_bytes=2);b.scratch_roots.add(path)
    b.check();assert b.stats['disk_peak_bytes']==actual
    with pytest.raises(ValueError,match='row_budget'):b.charge(b'123')


def test_guarded_identity_pages_cross_limits_and_keep_members(tmp_path):
    from data_pipeline.analysis.country_trends import identity_index as index, feature_identity as identity
    from data_pipeline.bgp.archive.value_codec import untyped
    from tests.country_trend.test_country_trend_identity_index import item
    b=budget(tmp_path,max_batch_rows=3);db=connect(tmp_path/'identity.sqlite',b)
    try:
        identity.initialize(db);index.install_progress(db,b)
        for n in range(40):index.save_interpretation(db,n,item('甲','AA'),b)
        index.build_index(db,b)
        assert index.lookup(db,'甲',b)=='AA'
        actual=[]
        for payload, in db.execute("SELECT ordinals FROM feature_identity_pages WHERE kind='name' ORDER BY key,page"):
            actual.extend(untyped(payload))
        assert actual==list(range(40))
        assert b.references>40 and b.stats['identity_pages']>20
        assert len(list(index.interpretations(db,b)))==40
        assert len(list(index.evidence(db,'feature:fixture',b)))>20
    finally:db.close()
