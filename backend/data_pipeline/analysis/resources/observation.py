"""显式 M2 observation_sealed → 独立 Resource；无真实数据自动运行。"""
from dataclasses import asdict
from itertools import chain
import json
from pathlib import Path
import resource
import shutil
import sys
import time

import pyarrow as pa
from psycopg2.extras import Json

from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.ordered_reader import ordered, binding_from_reader
from data_pipeline.bgp import record_types as o
from data_pipeline.bgp.input.path_decoding import DECODER_VERSION, decoding_difference
from data_pipeline.analysis.resources.adapter import reference_projection
from data_pipeline.analysis.resources.bindings import validate_sources, manifest
from data_pipeline.analysis.resources.compute import ResourceComputer, RibElement
from data_pipeline.analysis.resources.identity import execution_identity, identity_digest
from data_pipeline.analysis.resources.references import read_reference
from data_pipeline.analysis.resources.store import ResourceStore
from data_pipeline.analysis.resources.qualification import PROFILE, VERSION, ALL_TABLES, pg_identity, table_inventory, expected_qualifications, validate_relations, check_table_set


def source_metadata(reader):
    binding=binding_from_reader(reader);sid=reader.sources[0]
    cp=next(cp for cp in reader.selection.checkpoints if cp['source_id']==sid)
    return dict(binding_id=binding.binding_id,binding=asdict(binding),
        source_rank=binding.ordered_source_ids.index(sid),checkpoint=cp,
        seal=reader.selection.seal,manifest=reader.manifest)


def reference_metadata(reader, source):
    binding=binding_from_reader(reader)
    cp=next((c for c in reader.selection.checkpoints if c['source_id']==source),None)
    entry=next((e for e in reader.selection.plan['inputs'] if e['source_id']==source and e['role']=='reference'),None)
    if cp is None or entry is None or cp['ingest']!='complete' or cp['raw']!='verified_source_eof':
        raise ValueError('参考不在固定已完成观察选择中')
    return dict(binding=asdict(binding),binding_id=binding.binding_id,checkpoint=cp,entry=entry)


class ObservationResourceStore(ResourceStore):
    tables=ALL_TABLES
    schema_version=PROFILE

    def upstream_binding(self,dsn,run):
        if self.binding_manifest['profile']!=PROFILE:raise ValueError('Resource观察profile不符')
        self.validate_upstreams()
        first=self.binding_manifest['sources'][0]
        return None,first['snapshot']

    def finish(self,report_details=None):
        self.flush()
        relation=lambda t:f'lake.{self.schema}.{t}'
        for table,rows in expected_qualifications(self.db,relation,self.binding_manifest,self.guard).items():
            for row in rows:self.append(table,row)
        self.flush()
        validate_relations(self.db,relation,self.binding_manifest,self.guard,dsn=self.dsn)
        check_table_set(self.dsn,self.schema,self.db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0])
        self.inventory=table_inventory(self.db,relation,self.guard)
        self.validate_upstreams()
        self.receipt=dict(profile=PROFILE,qualification_version=VERSION,inventory=self.inventory,
            binding_manifest=self.binding_manifest,storage_layout=self.storage_layout,
            code_digest=self.code_digest,code_identity=self.code_identity)
        with self.pg,self.pg.cursor() as cur:
            cur.execute('ALTER TABLE domeye.resource_runs ADD COLUMN IF NOT EXISTS observation_receipt JSONB')
            cur.execute("UPDATE domeye.resource_runs SET observation_receipt=%s WHERE run_id=%s AND state='candidate'",(Json(self.receipt),self.run_id))
            if cur.rowcount!=1:raise ValueError('资格回执写入时运行已终结')
        return super().finish({**(report_details or {}),'profile':PROFILE,'qualification_version':VERSION,
            'inventory':self.inventory})

    def dataset_identifier(self, source_rows, topology_modes, snapshot):
        return identity_digest(dict(run_id=self.run_id,snapshot=snapshot,receipt=self.receipt))


def produce_observation_resources(dsn,sources,result_window,csv_binding,country_binding,output,*,
        topology_enabled=True,fixture_only=False,max_rss_bytes=2*1024**3,min_free_bytes=1024**3):
    started=time.monotonic();expected=execution_identity(fixture_only=fixture_only)
    root=Path(output);sources=tuple(sources);probe=None
    def guard():
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        path=root if root.exists() else root.parent
        if rss>max_rss_bytes or shutil.disk_usage(path).free<min_free_bytes or (probe and shutil.disk_usage(probe).free<min_free_bytes):
            raise ValueError('Resource内存或磁盘保护触发')
    guard();instance=pg_identity(dsn)
    readers=validate_sources(dsn,sources,result_window,guard=guard,profile='observation')
    if fixture_only and any(not s.context.origin_uri.startswith('fixture://') for s in sources):raise ValueError('fixture来源须显式声明')
    bound={r.sources[0]:source_metadata(r) for r in readers}
    csv_reader=ObservationReader(dsn,csv_binding['run_id'],csv_binding['snapshot'],[csv_binding['anchor_source_id']],profile='observation',guard=guard)
    csv_meta=reference_metadata(csv_reader,csv_binding['source_id'])
    countries,country_identity=read_reference(dsn,**country_binding,allow_fixture=fixture_only)
    rows=[{'source_id':country_binding['dataset_id'],'row':i,'location':asn,
        'raw_row':json.dumps({'__object_pairs__':[['country_cn',values[0]]]},ensure_ascii=False)} for i,(asn,values) in enumerate(countries.items())]
    references=reference_projection(chain(csv_reader.reference_batches(csv_binding['source_id']),[pa.Table.from_pylist(rows)]),
        csv_source=csv_binding['source_id'],country_source=country_binding['dataset_id'])
    csv_reader.selection.check_sources([csv_binding['source_id']])
    reference_id=identity_digest({'csv':csv_binding,'country':country_identity})
    if any(s.context.reference_id!=reference_id for s in sources):raise ValueError('参考绑定摘要不符')
    observation_runs={item['binding_id']:{k:item[k] for k in ('binding','seal','manifest')} for item in bound.values()}
    selected_inputs={sid:{k:item[k] for k in ('binding_id','source_rank','checkpoint')} for sid,item in bound.items()}
    binding_manifest={**manifest(sources,result_window,csv_binding,country_identity),
        'profile':PROFILE,'qualification_version':VERSION,'pg_identity':instance,'observation_inputs':selected_inputs,'observation_runs':observation_runs,
        'csv_observation':csv_meta,'topology_enabled':topology_enabled}
    # JSON固定tuple/list的表现；PG JSONB读回与本进程比较使用同一形态。
    binding_manifest=json.loads(json.dumps(binding_manifest))
    def validate():
        guard()
        if pg_identity(dsn)!=instance:raise ValueError('PG系统/库身份漂移')
        for binding in sources:
            r=ObservationReader(dsn,binding.run_id,binding.snapshot,[binding.context.source_id],profile='observation',guard=guard)
            selected=binding_manifest['observation_inputs'][binding.context.source_id]
            expected_meta={**selected,**binding_manifest['observation_runs'][selected['binding_id']]}
            if json.loads(json.dumps(source_metadata(r)))!=expected_meta:raise ValueError('M2输入绑定漂移')
            db=r.connect();db.close();r.check_sources(r.starts)
        r=ObservationReader(dsn,csv_binding['run_id'],csv_binding['snapshot'],[csv_binding['anchor_source_id']],profile='observation')
        if json.loads(json.dumps(reference_metadata(r,csv_binding['source_id'])))!=binding_manifest['csv_observation']:raise ValueError('CSV封存绑定漂移')
        # 完整公共参考读取包括计数核对；末尾再核对独立reference checkpoint。
        for batch in r.reference_batches(csv_binding['source_id']):guard()
        r.selection.check_sources([csv_binding['source_id']])
        if read_reference(dsn,**country_binding,allow_fixture=fixture_only)[1]!=country_identity:raise ValueError('补充参考版本漂移')
    store=ObservationResourceStore(dsn,root,sources[0].run_id,[s.context.source_id for s in sources],
        expected_code_identity=expected,fixture_only=fixture_only,binding_manifest=binding_manifest,validate_upstreams=validate)
    store.guard=guard;probe=Path(store.storage_layout['catalog_data_path'])
    while not probe.exists() and probe!=probe.parent:probe=probe.parent
    try:
        computer=ResourceComputer(references,topology_enabled=topology_enabled);timings=[]
        for binding,reader in zip(sources,readers):
            begin=time.monotonic();sid=binding.context.source_id;actual=[None,None,0];end=None;peers={};quality=0
            meta=bound[sid]
            def elements():
                nonlocal end,quality
                stream=ordered(reader)
                try:
                    for item in stream:
                        guard()
                        if isinstance(item,o.SourceEnd):end=item
                        elif isinstance(item,o.SourceQuality):
                            quality+=1
                            store.append('observation_quality',dict(source_id=sid,record=None,code=item.raw['code'],detail=item.raw['detail']))
                        elif isinstance(item,o.MessageBoundary):
                            if item.gap:raise ValueError('损坏RIB不能由M3容错')
                            for q in item.raw['quality']:
                                quality+=1;store.append('observation_quality',dict(source_id=sid,record=item.position.record,code=q['code'],detail=q['detail']))
                            for p in item.raw['peers']:
                                key=(p['table_record'],p['peer_index'])
                                if key in peers:raise ValueError('重复Peer依赖')
                                peers[key]=p
                                store.append('peer_dependencies',dict(source_id=sid,table_record=key[0],peer_index=key[1],
                                    peer_ip=p['ip'],peer_asn=p['asn'],bgp_id=p['bgp_id'],bgp_id_present=p['bgp_id_present']))
                        elif isinstance(item,o.Element):
                            row=item.raw
                            if row['action']!='rib_snapshot' or row['as_path_text'] is None or row['peer_asn'] is None:raise ValueError('非完整RIB元素')
                            peer=peers.get((row['peer_table_record'],row['peer_index']))
                            if peer is None or (peer['ip'],peer['asn'],peer['bgp_id'],peer['bgp_id_present'])!=(row['peer_ip'],row['peer_asn'],row['bgp_id'],row['bgp_id_present']):raise ValueError('RIB元素Peer表依赖缺失或不符')
                            if not actual[2] and row['epoch']!=binding.context.snapshot_time.timestamp():raise ValueError('声明时点不是首元素epoch')
                            actual[0]=row['epoch'] if actual[0] is None else min(actual[0],row['epoch'])
                            actual[1]=row['epoch'] if actual[1] is None else max(actual[1],row['epoch']);actual[2]+=1
                            difference=decoding_difference(row)
                            if difference:store.append('decoding_differences',dict(source_id=sid,event_id=row['event_id'],detail=json.dumps(difference,ensure_ascii=False)))
                            yield RibElement(sid,row['message_id'],row['ordinal'],sys.intern(row['prefix']),sys.intern(row['as_path_text']),str(row['peer_asn']),
                                row['peer_ip'],row['bgp_id'] if row['bgp_id_present'] else None,row['path_key'],row['afi'],row['safi'],row['action'],row['attributed_origin_asn'])
                finally:stream.close()
            result=computer.compute(binding.context,elements(),decision_sink=store.decision,membership_sink=store.members)
            if end is None:raise ValueError('独立RIB未完整消费')
            store.append('input_receipts',dict(source_id=sid,binding_id=meta['binding_id'],source_rank=meta['source_rank'],
                checkpoint_digest=meta['checkpoint']['digest'],attempt=meta['checkpoint']['attempt'],
                messages=end.raw.messages,elements=actual[2],**asdict(end.parse_counts),peers=len(peers),
                quality_records=quality,first_epoch=actual[0],last_epoch=actual[1]))
            store.result(result,actual);store.state(result.state);guard()
            timings.append(dict(source_id=sid,elements=actual[2],messages=end.raw.messages,seconds=time.monotonic()-begin))
        report=store.finish(dict(ribs=timings,seconds_before_completion_gate=time.monotonic()-started,
            peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),
            rss_scope='current_process_lifetime_excludes_PG',decoder_version=DECODER_VERSION,
            limitations=['independent_rib_not_route_recovery','reference_historical_effectivity_Unknown','cold_start_not_historical_equivalence']))
        return report
    except BaseException as exc:
        state=store.fail(exc)
        if state=='complete':return {**json.loads((root/'execution.json').read_text()),'state':'complete','commit_confirmation':'verified_after_error'}
        (root/'failure.json').write_text(json.dumps(dict(state=state,run_id=store.run_id,reason=str(exc)),ensure_ascii=False))
        raise
    finally:store.close()
