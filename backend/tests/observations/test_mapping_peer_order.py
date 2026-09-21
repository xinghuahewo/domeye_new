"""同一基线 Peer 的多条位置证据排列不能污染映射正文。"""
from types import SimpleNamespace
import json

from data_pipeline.bgp.replay.archive_input import build_mapping
from data_pipeline.bgp.replay.snapshot_contract import encode, digest


def mapping_with_order(peers):
    class DB:
        def register(self,*args):pass
        def unregister(self,*args):pass
        def execute(self,sql,*args):
            if 'max(snapshot_id)' in sql:self.rows=[(1,)]
            elif 'GROUP BY' in sql:self.rows=[('192.0.2.1',64497,'192.0.2.2',12654,0,'u:0',1),
                ('192.0.2.4',64497,'192.0.2.2',12654,0,'u:1',1),
                ('192.0.2.4',64497,'192.0.2.5',12654,0,'u:2',1),
                ('192.0.2.6',64497,'192.0.2.2',12654,0,'u:3',1)]
            else:self.rows=peers
            return self
        def fetchone(self):return self.rows[0]
        def fetchall(self):return self.rows
    rows=[]
    store=SimpleNamespace(db=DB(),schema='r_fixture',flush=lambda:None,append=lambda table,row:rows.append(row))
    endpoints=build_mapping(store,'b',['u'])
    return endpoints,rows


def test_same_peer_refs_order_preserves_all_content_and_states():
    peers=[('192.0.2.1',64497,'192.0.2.1',3,0),('192.0.2.1',64497,'192.0.2.1',0,1),
           ('192.0.2.1',64497,'192.0.2.1',0,0),('192.0.2.3',64497,'192.0.2.3',0,2),
           ('192.0.2.4',64497,'192.0.2.4',0,3),('192.0.2.6',64497,'192.0.2.6',0,4)]
    variants=[mapping_with_order(p) for p in (peers,list(reversed(peers)),peers[2:]+peers[:2])]
    assert len({encode(v) for v in variants})==1
    assert len({digest(v) for v in variants})==1
    endpoints,rows=variants[0]
    assert endpoints==(('192.0.2.6',64497,'192.0.2.2',12654,0),)
    assert [r['status'] for r in rows]==['conflicting_baseline_peers','no_observed_endpoint','ambiguous_local_endpoints','calculation_mapping']
    assert json.loads(rows[0]['peer_refs'])==[['192.0.2.1',0,0],['192.0.2.1',0,1],['192.0.2.1',3,0]]


def test_sealed_m2_mapping_metadata_permutations():
    import os,time,hashlib,resource,shutil
    from pathlib import Path
    from collections import Counter
    import pytest,psycopg2
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.common.run_metrics import Metrics
    config=os.environ.get('DOMEYE_MAPPING_TEST_CONFIG')
    if not config:pytest.skip('需显式自有映射测试 M2 绑定')
    c=json.loads(Path(config).read_text())
    assert psycopg2.extensions.parse_dsn(c['dsn'])['host'].startswith('/tmp/domeye-mapping-efb1-')
    output=Path(os.environ['DOMEYE_MAPPING_EVIDENCE'])
    def guard():
        if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss>4*1024**3 or shutil.disk_usage(output).free<512*1024**2:raise ValueError('映射测试资源保护')
    reader=ObservationReader(c['dsn'],c['run_id'],c['snapshot'],c['sources'],profile='observation',guard=guard)
    seal=reader.selection.seal
    files={f['path']:hashlib.sha256(Path(f['path']).read_bytes()).hexdigest() for cp in reader.selection.checkpoints for f in cp['files']}
    connect=reader.connect;orders=[];results=[];timings=[]
    class Connection:
        def __init__(self,db,shape):self.db=db;self.shape=shape;self.peer_query=False
        def __getattr__(self,name):return getattr(self.db,name)
        def execute(self,sql,*args):
            self.peer_query=sql.startswith('SELECT "ip",asn,bgp_id,table_record,"index"')
            self.db.execute(sql,*args);return self
        def fetchall(self):
            rows=self.db.fetchall()
            if self.peer_query:
                # SQL未承诺枚举行序；只排列真实已封存查询结果，不造/删/改任何原字段。
                rows=sorted(rows,key=lambda r:(r[3],r[4]))
                if self.shape=='reverse':rows.reverse()
                if self.shape=='rotate':rows=rows[2:]+rows[:2]
                orders.append(rows)
            return rows
    metrics=Metrics(output/'RSS.jsonl')
    try:
        for batch,shape in ((1,'forward'),(10,'reverse'),(1,'rotate')):
            reader.batch_rows=batch;reader.connect=lambda:Connection(connect(),shape)
            rows=[];sink=SimpleNamespace(dsn=reader.dsn,flush=guard,append=lambda table,row:rows.append(row))
            started=time.monotonic()
            with metrics.measure('build_mapping'):endpoints=build_mapping(sink,c['sources'][0],c['sources'][1:],reader=reader)
            timings.append(dict(batch_rows=batch,shape=shape,wall_seconds=time.monotonic()-started))
            results.append((endpoints,rows))
    finally:metrics.close()
    assert len(orders)==3 and len({encode(o) for o in orders})==3
    assert all(Counter(o)==Counter(orders[0]) for o in orders)
    assert len({encode(r) for r in results})==1
    assert len({digest(r) for r in results})==1
    endpoints,rows=results[0]
    assert [r['status'] for r in rows]==['conflicting_baseline_peers','no_observed_endpoint','ambiguous_local_endpoints','calculation_mapping']
    assert endpoints==(('192.0.2.6',64497,'192.0.2.2',12654,0),)
    assert json.loads(rows[0]['peer_refs'])==[['192.0.2.1',0,0],['192.0.2.1',0,1],['192.0.2.1',2,0]]
    assert reader.selection.seal==seal
    assert files=={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in files}
    (output/'映射定点证据.json').write_text(json.dumps(dict(m2=seal['run_id'],seal=seal['digest'],m2_files_unchanged=len(files),
        metadata_orders=orders,typed_result=encode(results[0]),logical_digest=digest(results[0]),timings=timings,metrics=metrics.receipt()),ensure_ascii=False,indent=2))
