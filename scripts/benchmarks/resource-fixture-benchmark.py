#!/usr/bin/env python3
"""隔离人工fixture实测；prepare和compute必须分进程，避免RSS混入上游生产。"""
import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
import uuid
import time
import resource

BASE=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(BASE/'backend'))
sys.path.insert(0,str(BASE/'backend/tests'))
import psycopg2
from tests.resources.test_resource_store import fixture_inputs
from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.analysis.resources import RibContext
from data_pipeline.analysis.resources.produce import produce_resources

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('mode',choices=['prepare','compute'])
parser.add_argument('--root',required=True)
parser.add_argument('--dsn',required=True)
parser.add_argument('--count',type=int,default=20000)
parser.add_argument('--ribs',type=int,default=6)
parser.add_argument('--result-name',default='resource')
args=parser.parse_args()
if args.count<=0 or args.count%2 or args.ribs<=0:
    parser.error('count须为正偶数，ribs须为正整数')
root=Path(args.root)
if args.mode=='prepare':
    root.mkdir(parents=True,exist_ok=False)
    admin=psycopg2.connect(args.dsn);admin.autocommit=True
    database='bench_'+uuid.uuid4().hex
    with admin.cursor() as c:c.execute('CREATE DATABASE '+database)
    admin.close()
    dsn=args.dsn+' dbname='+database
    manifest,contexts=fixture_inputs(root,count=args.count,ribs=args.ribs,distinct_ribs=True)
    report=produce(manifest,dsn,root/'upstream')
    from dataclasses import asdict
    binding={'dsn':dsn,'upstream':report,'contexts':[asdict(c) for c in contexts],
        'csv_source':manifest['references'][0]['sha256'],'country_source':manifest['references'][1]['sha256'],
        'fixture':{'ribs':args.ribs,'elements_per_rib':args.count,'distinct_prefixes':args.count*args.ribs,
                   'distinct_paths':args.count//2*args.ribs,'same_path_different_med':True}}
    (root/'binding.json').write_text(json.dumps(binding,default=str,ensure_ascii=False,indent=2))
    print(json.dumps({'prepared':str(root),'fixture':binding['fixture'],'upstream_seconds':report['seconds']},ensure_ascii=False))
else:
    binding=json.loads((root/'binding.json').read_text())
    contexts=[RibContext(**{**c,'snapshot_time':datetime.fromisoformat(c['snapshot_time'])}) for c in binding['contexts']]
    begin=time.monotonic()
    report=produce_resources(binding['dsn'],binding['upstream']['run_id'],contexts,root/args.result_name,fixture_only=True,
        csv_source=binding['csv_source'],country_source=binding['country_source'],topology_enabled=True)
    wall_seconds=time.monotonic()-begin
    from data_pipeline.analysis.resources.store import scan_resource
    metric_rows=[r for batch in scan_resource(binding['dsn'],report['run_id'],'metrics',allow_fixture=True) for r in batch.to_pylist()]
    expected=binding['fixture']
    globals=[r for r in metric_rows if r['bucket']=='global']
    assert len(globals)==expected['ribs']
    assert all(r['ipv4_prefix_count']==expected['elements_per_rib'] and r['path_count']==expected['elements_per_rib']//2 for r in globals)
    assert report['actual_rows']['rendered_paths']==expected['distinct_paths']
    assert report['actual_rows']['decision_refs']==expected['distinct_prefixes']
    audit={'fixture':expected,'resource':report,'wall_seconds_including_completion_gate':wall_seconds,'peak_rss_bytes_including_completion_gate':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),'independent_count_audit':'passed'}
    (root/(args.result_name+'-measurement.json')).write_text(json.dumps(audit,ensure_ascii=False,indent=2))
    print(json.dumps(audit,ensure_ascii=False))
