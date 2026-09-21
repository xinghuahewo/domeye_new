"""8233 自有既存 M2 的有限 P1 集成与实际成本记录。"""
from contextlib import contextmanager
from pathlib import Path
import hashlib
import json
import time
from collections import Counter

import psycopg2
from data_pipeline.bgp.archive import admission as p
from data_pipeline.bgp.archive.value_codec import typed, untyped
from tests.country.country_query_test_support import PgLogProbe

OUT=Path('/tmp/domeye-m2-p1-integration-8233').resolve()
ROOT=Path('/tmp/domeye-detection-m3-integration-8233/detection-m30').resolve()
CONFIG=json.loads((ROOT/'request.json').read_text())
DSN=CONFIG['observation_dsn']
EVENTS=[]
RT=p.Runtime(DSN,(ROOT,OUT),OUT,fixture_only=True,audit_sink=EVENTS.append)

def guard():
    import resource
    import shutil
    if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss>4*1024**3 or shutil.disk_usage(OUT).free<512*1024**2:
        raise ValueError("人工 P1 RSS/磁盘资源保护")

def save(name,value):
    (OUT/name).write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str))

def files(root):
    return {str(x):hashlib.sha256(x.read_bytes()).hexdigest() for x in root.rglob('*') if x.is_file()}

def original_metadata():
    pg=psycopg2.connect(DSN)
    try:
        with pg.cursor() as c:
            c.execute('SELECT row_to_json(r) FROM observation_m2.runs r WHERE run_id=%s',(CONFIG['input_run'],));run=c.fetchone()[0]
            c.execute('SELECT ordinal,payload FROM observation_m2.checkpoints WHERE run_id=%s ORDER BY ordinal',(CONFIG['input_run'],));cps=c.fetchall()
        return dict(run=run,checkpoints=cps)
    finally:pg.close()

@contextmanager
def measure(name):
    probe=PgLogProbe(DSN,OUT/'pg.log');offset=probe.start();EVENTS.clear();start=time.monotonic()
    info={}
    try:yield info
    finally:
        info['wall_seconds']=time.monotonic()-start
        snippet,count=probe.finish(offset)
        (OUT/(name+'.pg.log')).write_text(snippet)
        kinds=Counter(e['kind'] for e in EVENTS)
        info.update(pg_statements=count,event_counts=dict(kinds),
                    bytes_by_event={k:sum(e.get('bytes',0) for e in EVENTS if e['kind']==k) for k in kinds},
                    rows_by_event={k:sum(e.get('rows',0) for e in EVENTS if e['kind']==k) for k in kinds},
                    temporary_disk_observed_max=max([e.get('bytes',0) for e in EVENTS if e['kind']=='temporary_disk'] or [0]))
        save(name+'.events.json',EVENTS);save(name+'.cost.json',info)

def request(a,view,batch=17,sources=None):
    b=untyped(a['owner_binding'])
    if sources is None:sources=b['ordered_source_ids'] if a['owner']=='m2' and view!='references' else ([b['source_id']] if a['owner']=='reference' else [s['source_id'] for s in b['reference_sources']])
    return dict(view=view,scope_typed=typed(dict(source_ids=sources)),codec_version=p.CODEC,batch_rows=batch,batch_bytes=1024**2)

def read(a,req):
    rows=[];first=None;start=time.monotonic()
    with p.open_reader(RT,a,req,guard=guard) as session:
        for batch in session:
            if first is None:
                first=dict(seconds=time.monotonic()-start,events_before_first=list(EVENTS))
            assert batch['rows']<=req['batch_rows'] and batch['bytes']==len(batch['rows_typed'].encode())<=req['batch_bytes']
            rows.extend(untyped(batch['rows_typed']))
        assert session.receipt is None
    assert session.receipt['rows']==len(rows)
    return rows,session.receipt,first

def load():
    a=json.loads((OUT/'M2_Admission.json').read_text());ref=json.loads((OUT/'reference_Admission.json').read_text())
    RT.dependency_admissions=(a,)
    return a,ref

def consumer_probe(seed,batch,augment=False):
    """真实固定选择的全行副本仅改变临时 SQL 枚举；增强组单独标明。"""
    import duckdb
    import random
    import pyarrow as pa
    from dataclasses import asdict
    from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch
    from data_pipeline.bgp.archive.store import TYPES
    reader=ObservationReader(DSN,CONFIG['input_run'],CONFIG['input_snapshot'],CONFIG['ordered_sources'],profile='observation',batch_rows=batch,guard=guard)
    lake=reader.connect();db=duckdb.connect();inputs={}
    try:
        for name in ('messages','elements','paths','quality','eor','peers'):
            cols=reader.columns(name)
            # 保留 CP 归属，避免跨 source 同 path_key 副本放大 join。
            for rank,source in enumerate(CONFIG['ordered_sources']):
                rows=lake.execute('SELECT * FROM '+reader.bound_table(name,source)).fetch_arrow_table().to_pylist()
                if augment and name=='peers' and rows:
                    rows.extend([dict(rows[0]),dict(rows[0],bgp_id=None,bgp_id_present=False)])
                if augment and name=='quality':
                    rows.extend([dict(source_id=source,message_id=None,code='fixture_unknown',detail=None)]*2)
                inputs[name+'.'+source]=rows.copy()
                random.Random(seed).shuffle(rows)
                schema=pa.schema([(n,TYPES[t]) for n,t in cols]);db.register('incoming',pa.Table.from_pylist(rows,schema=schema))
                db.execute('CREATE TABLE '+name+'_'+str(rank)+' AS SELECT * FROM incoming');db.unregister('incoming')
    finally:lake.close()
    db.execute('SET default_null_order='+('NULLS_FIRST' if seed%2 else 'NULLS_LAST'))
    reader.connect=lambda:db
    reader.bound_table=lambda name,source=None:name+'_'+str(CONFIG['ordered_sources'].index(source))
    # 批打包可不同，分流保留每源每类及嵌套原序，绝不排序比较结果。
    result={source:{'boundary':[],'quality':[],'message':[],'element':[]} for source in CONFIG['ordered_sources']}
    for item in reader.stream():
        target=result[item.source_id]
        if isinstance(item,MessageBatch):
            target['quality'].extend(item.source_quality);target['message'].extend(item.messages);target['element'].extend(item.elements)
        else:target['boundary'].append(asdict(item))
    return result,inputs


def actual_seams(result):
    from datetime import datetime,timezone
    from types import SimpleNamespace
    from data_pipeline.analysis.features.qualification import Qualification
    from data_pipeline.bgp.record_types import ParseCounts
    from data_pipeline.analysis.detection._results import Results
    from dataclasses import make_dataclass
    captured=[];events={}
    q=Qualification('integration-8233',SimpleNamespace(append=lambda table,row:captured.append((table,row))))
    instant=datetime(1970,1,1,tzinfo=timezone.utc)
    for rank,(source,stream) in enumerate(result.items()):
        q.begin(rank,SimpleNamespace(source_id=source,message_quality_state='complete',window=SimpleNamespace(start=instant,end=instant,coverage='complete')))
        for row in stream['quality']:q.quality(None,row['code'],row['detail'])
        for row in stream['message']:q.boundary(SimpleNamespace(position=SimpleNamespace(record=row['record']),raw=row,gap=None))
        count=len(stream['message'])
        q.finish(SimpleNamespace(parse_counts=ParseCounts(count,0,0,0),raw=SimpleNamespace(messages=count),binding_ref='fixture-seam',source_rank=rank))
        scope=make_dataclass('Scope',[('run_id',str),('source',str)])('integration-8233',source)
        sink=Results(scope,SimpleNamespace(version='fixture-ref',historical_applicability='unknown'))
        for row in stream['message']:
            sink.context={'source_message':row}
            sink.event_revision('leak','prefix','legacy',{'s_time':'1970-01-01','leak_phenomenon_table':'fixture'})
        events[source]=sink.rows
    return captured,events
