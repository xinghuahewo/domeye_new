"""实际本地DuckDB临时对账；不连接PG或调用科学producer。"""
from types import SimpleNamespace

import duckdb
import pytest

from data_pipeline.analysis.resources.publication_validation import science_rows


@pytest.fixture
def db(tmp_path):
    connection=duckdb.connect()
    connection.execute("SET memory_limit='32MB'")
    connection.execute("SET max_temp_directory_size='16MB'")
    connection.execute("SET temp_directory='"+str(tmp_path)+"'")
    yield connection
    connection.close()


def runtime():return SimpleNamespace(memory_bytes=128,max_rows=1,max_bytes=1)


def test_many_small_batches_exact_values_duplicates_and_parallel_scan(db):
    rows=[{'x':i,'value':b'raw'} for i in range(12)]+[{'x':3,'value':b'raw'}]*3
    with science_rows(db,runtime(),lambda:None) as audit:
        assert audit.db.execute("SELECT current_setting('memory_limit'),current_setting('max_temp_directory_size')").fetchone()==('30.5 MiB','15.2 MiB')
        for row in rows:audit.add('expected','metrics',row)
        audit.finish_expected()
        stream=db.execute('SELECT range AS x FROM range(12)').fetch_record_batch(2)
        for batch in stream:
            for row in batch.to_pylist():audit.add('actual','metrics',dict(row,value=b'raw'))
        stream.close()
        for _ in range(3):audit.add('actual','metrics',{'x':3,'value':b'raw'})
        assert audit.compare('metrics')==15
        assert audit.compare('empty')==0
    with pytest.raises(duckdb.ConnectionException):audit.db.execute('SELECT 1')
    assert db.execute('SELECT 1').fetchone()==(1,)


@pytest.mark.parametrize('actual',[[{'x':1},{'x':3}],[{'x':1},{'x':1}]])
def test_same_count_wrong_value_or_multiplicity_rejected(db,actual):
    with science_rows(db,runtime(),lambda:None) as audit:
        for row in [{'x':1},{'x':2}]:audit.add('expected','metrics',row)
        audit.finish_expected()
        for row in actual:audit.add('actual','metrics',row)
        with pytest.raises(ValueError,match='全值/多重性'):audit.compare('metrics')


def test_paths_deduplicate_but_actual_multiplicity_and_collisions_reject(db):
    row={'path_digest':'p','raw':'1 2'}
    with science_rows(db,runtime(),lambda:None) as audit:
        for _ in range(8):audit.add('expected','rendered_paths',row)
        audit.finish_expected();audit.add('actual','rendered_paths',row)
        assert audit.compare('rendered_paths')==1
        audit.add('actual','rendered_paths',row)
        with pytest.raises(ValueError,match='多重性'):audit.compare('rendered_paths')
    with science_rows(db,runtime(),lambda:None) as audit:
        audit.add('expected','rendered_paths',row)
        audit.add('expected','rendered_paths',dict(row,raw='1 3'))
        with pytest.raises(ValueError,match='路径摘要碰撞'):audit.finish_expected()


@pytest.mark.parametrize('failure',['caller','guard','memory'])
def test_exception_resource_failure_closes_private_storage(db,failure):
    error=ValueError('fixture resource guard');armed=[False]
    def guard():
        if armed[0]:raise error
    audit=None
    with pytest.raises((ValueError,duckdb.OutOfMemoryException)) as caught:
        with science_rows(db,runtime(),guard) as audit:
            audit.add('expected','metrics',{'x':1})
            if failure=='caller':raise error
            if failure=='guard':armed[0]=True
            else:db.execute("SET memory_limit='1B'")
            audit.add('actual','metrics',{'x':1});audit.compare('metrics')
    if failure!='memory':assert caught.value is error
    with pytest.raises(duckdb.ConnectionException):audit.db.execute('SELECT 1')
