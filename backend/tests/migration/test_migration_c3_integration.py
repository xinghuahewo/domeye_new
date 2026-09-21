"""C3集成：本任务正式人工多事件链、21表及固定快照完整读取。"""
from dataclasses import asdict,fields,is_dataclass
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import sys
import time

import psycopg2
import pyarrow.parquet as pq

from data_pipeline.analysis.country_events import event_aggregation as c2, compute, snapshot_reader as component_reader
from data_pipeline.analysis.country_events.snapshot_store import persist_stream
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch, ReadReceipt
from data_pipeline.analysis.country_events.snapshot_schema import TABLES, SCHEMAS
from tests.country.test_country_component import catalog
from tests.country.test_country_c1_repair import review_chain
from tests.country.test_country_saved_input import adapter


def fractions(value):
    if isinstance(value,Fraction):return [(str(value.numerator),str(value.denominator))]
    if is_dataclass(value):return [r for f in fields(value) for r in fractions(getattr(value,f.name))]
    if isinstance(value,dict):return [r for k,v in value.items() for x in (k,v) for r in fractions(x)]
    if isinstance(value,(tuple,list)):return [r for v in value for r in fractions(v)]
    return []


def test_formal_411_two_batches_fixed_snapshot(review_chain,catalog,monkeypatch):
    root=review_chain[0];grid=compute.Grid(100000000,125000000,(107000000,113000000,120000000,121000000,121500000,124000000,125000000))
    started=time.monotonic();original=list(c2.run_saved(adapter(review_chain),grid,scratch_root=root))
    c2_wall=time.monotonic()-started
    assert len(original)==411 and original[-1].event_count==2
    exact=fractions(original);assert exact
    (root/'integration-original-repr.txt').write_text(repr(original))
    before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for folder in ('observation','detection') for p in (root/folder).rglob('*') if p.is_file()}
    records=[];bindings=[]
    for cap in (256,1):
        source=adapter(review_chain);calls=[];real=source.stream
        def audited():
            calls.append('C1_full_audit')
            yield from real()
        monkeypatch.setattr(source,'stream',audited)
        started=time.monotonic()
        binding=persist_stream(iter(original),catalog,root/('integration-c3-'+str(cap)),c1=source,parameters={'grid':asdict(grid)},batch_rows=cap)
        elapsed=time.monotonic()-started
        assert calls==['C1_full_audit']
        manifest=json.loads((Path(binding.root)/'manifest.json').read_text())
        assert len(manifest['files'])==len(TABLES)==21 and manifest['row_count']==411
        physical={}
        for table in TABLES:
            p=Path(binding.root)/'data'/(table+'.parquet');metadata=pq.ParquetFile(p)
            assert metadata.metadata.num_rows==manifest['tables'][table]['rows']
            assert metadata.schema_arrow.names==[n for n,_,_ in SCHEMAS[table]]
            physical[table]={'rows':metadata.metadata.num_rows,'row_groups':metadata.metadata.num_row_groups,'bytes':p.stat().st_size,'schema':str(metadata.schema_arrow)}
        bindings.append(binding)
        records.append({'binding':asdict(binding),'save_wall_seconds':elapsed,'C1_stream_calls':len(calls),'manifest':manifest,'physical':physical,'process_cumulative_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss})
    assert bindings[1].snapshot>bindings[0].snapshot
    with psycopg2.connect(catalog) as pg,pg.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='country_components' ORDER BY table_name")
        assert cur.fetchall()==[('catalog',),('components',),('event_index',)]
        cur.execute('SELECT component_id,count(*) FROM country_components.event_index GROUP BY component_id')
        assert dict(cur.fetchall())=={b.component_id:2 for b in bindings}
    # 新snapshot之后另起进程，两个固定组件分别使用batch=1/default完整读取。
    (root/'integration-reader-request.json').write_text(json.dumps({'dsn':catalog,'bindings':[asdict(b) for b in bindings]}))
    script='''
import json,sys,time,resource
from pathlib import Path
sys.path.insert(0,str(Path.cwd()/'tests'))
from data_pipeline.analysis.country_events.snapshot_store import ComponentBinding
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch, ReadReceipt
from tests.country.test_country_saved_input import adapter
from tests.migration.test_migration_c3_integration import fractions
root=Path(sys.argv[1]);request=json.loads((root/'request.json').read_text());source=json.loads((root/'binding.json').read_text());saved=json.loads((root/'integration-reader-request.json').read_text())
prepared=(root,[request['observation_dsn'],request['detection_dsn']],request['ordered_sources'],source['observation'],source['detection'])
results=[]
for binding in saved['bindings']:
 for cap in (1,256):
  c1=adapter(prepared)
  def forbidden():raise AssertionError('Reader不得重扫C1正文')
  c1.stream=forbidden
  started=time.monotonic();parts=list(ComponentReader(saved['dsn'],ComponentBinding(**binding),c1=c1,batch_rows=cap).stream())
  rows=[r for part in parts if isinstance(part,ReadBatch) for r in part.rows]
  assert repr(rows)==(root/'integration-original-repr.txt').read_text()
  assert isinstance(parts[-1],ReadReceipt) and parts[-1].full_body_validated
  results.append({'component_id':binding['component_id'],'batch_rows':cap,'rows':len(rows),'fractions':fractions(rows),'wall_seconds':time.monotonic()-started,'process_cumulative_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss})
(root/'integration-fresh-reader.json').write_text(json.dumps(results))
print('fresh-process original repr and exact Fraction: 411 x 4')
'''
    child=subprocess.run([sys.executable,'-c',script,str(root)],cwd=Path(__file__).resolve().parents[2],capture_output=True,text=True)
    (root/'integration-fresh-reader.log').write_text(child.stdout+child.stderr)
    assert child.returncode==0,child.stderr
    fresh=json.loads((root/'integration-fresh-reader.json').read_text())
    assert all(r['fractions']==[list(f) for f in exact] for r in fresh)
    # 实际目录查询/文件核验透传测量；不把本探针当成全部上游SQL。
    queries=[];hashes=[];real_lake=component_reader.lake_connect;real_hash=component_reader.file_hash
    class Lake:
        def __init__(self,db):self.db=db
        def __getattr__(self,key):return getattr(self.db,key)
        def execute(self,query,*args,**kwargs):
            queries.append(query);self.db.execute(query,*args,**kwargs);return self
        def close(self):self.db.close()
    monkeypatch.setattr(component_reader,'lake_connect',lambda *a,**k:Lake(real_lake(*a,**k)))
    def hashing(path,*a):hashes.append(str(path));return real_hash(path,*a)
    monkeypatch.setattr(component_reader,'file_hash',hashing)
    log=Path(os.environ['C3_PRIVATE_PG_LOG']);offset=log.stat().st_size
    started=time.monotonic()
    reader=ComponentReader(catalog+" options='-c log_statement=all'",bindings[0],c1=adapter(review_chain))
    parts=list(reader.stream());elapsed=time.monotonic()-started
    assert isinstance(parts[-1],ReadReceipt) and parts[-1].rows==411
    with log.open('rb') as stream:stream.seek(offset);protocol=stream.read().decode()
    (root/'integration-reader-pg.log').write_text(protocol)
    statements=len(re.findall(r'statement:|execute [^:]+:',protocol));assert statements>0
    counts={'file_links':sum('ducklake_list_files' in q for q in queries),'describe':sum(q.startswith('DESCRIBE') for q in queries),'counts':sum(q.startswith('SELECT count(*)') for q in queries),'typed_scans':sum(q.startswith('SELECT *') for q in queries)}
    assert counts==dict(file_links=21,describe=21,counts=21,typed_scans=21)
    assert sum(p.endswith('.parquet') for p in hashes)==21
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==sha for p,sha in before.items())
    (root/'integration-c3-evidence.json').write_text(json.dumps({'c2_wall_seconds':c2_wall,'output_rows':411,'event_count':2,'exact_fractions':[list(f) for f in exact],'saves':records,'fresh_readers':fresh,'metadata_read':{'wall_seconds':elapsed,'queries':counts,'file_hash_calls':hashes,'component_pg_statements':statements},'upstream_files_unchanged':before,'rss_scope':'当前Python进程累计高水位，含进程内DuckDB；非独立阶段峰值，PG及子进程另计','stage_total_rss':'Unknown','postgres_peak_rss':'Unknown','duckdb_separate_peak_rss':'Unknown'},ensure_ascii=False,indent=2))
