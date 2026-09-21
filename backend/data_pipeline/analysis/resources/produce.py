"""已完成观察至Resource的显式离线入口；不发布页面，不读取真实原件。"""
from dataclasses import asdict
import json
from pathlib import Path
import resource
import shutil
import sys
import time

import psycopg2

from data_pipeline.bgp.archive.store import scan, scan_observations
from data_pipeline.bgp.replay.run_from_files import normalized_inputs
from data_pipeline.analysis.resources.adapter import ObservationInput, reference_projection
from data_pipeline.analysis.resources.compute import ResourceComputer
from data_pipeline.analysis.resources.store import ResourceStore, completed_binding
from data_pipeline.analysis.resources.identity import execution_identity


def produce_resources(dsn, upstream_run, contexts, output, *, csv_source, country_source,
                      topology_enabled=False, fixture_only=False, max_rss_bytes=2*1024**3, min_free_bytes=1024**3):
    """contexts明确声明每RIB顺序；时点取MRT、原文件名及实际范围独立保存。"""
    started=time.monotonic()
    expected_code_identity=execution_identity(fixture_only=fixture_only)
    contexts=tuple(contexts)
    if not contexts or any(b.snapshot_time<=a.snapshot_time for a,b in zip(contexts,contexts[1:])):
        raise ValueError('RIB清单为空或时点非递增')
    binding=completed_binding(dsn,upstream_run)
    with psycopg2.connect(dsn) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            c.execute('SELECT manifest FROM domeye.run_specs WHERE run_id=%s',(upstream_run,))
            manifest=c.fetchone()[0]
    if any(not {'source_id','origin_uri','sha256'}<=set(entry) for entry in manifest['inputs']):
        raise ValueError('上游缺少新版来源身份，不能沿用SHA身份')
    inputs={entry['source_id']:entry for entry in normalized_inputs(manifest)}
    if fixture_only and any(not e['origin_uri'].startswith('fixture://') for e in inputs.values()):
        raise ValueError('fixture API仅接受显式fixture来源')
    if any(c.source_id not in inputs or inputs[c.source_id]['role'] not in ('baseline','snapshot') for c in contexts):
        raise ValueError('清单含非RIB或未知来源')
    if any(c.content_sha256!=inputs[c.source_id]['sha256'] or c.origin_uri!=inputs[c.source_id]['origin_uri'] for c in contexts):
        raise ValueError('Resource来源定位或内容版本与上游不符')
    if any(c.collector!=manifest['collector'] for c in contexts):
        raise ValueError('Collector与上游绑定不符')
    if any(c.reference_id!=csv_source+'+'+country_source for c in contexts):
        raise ValueError('参考版本与声明不一致')
    references=reference_projection(scan(dsn,upstream_run,'references'),csv_source=csv_source,country_source=country_source)
    root=Path(output)
    store=ResourceStore(dsn,root,upstream_run,[c.source_id for c in contexts],expected_code_identity=expected_code_identity,fixture_only=fixture_only)
    staged=ObservationInput(root/'observations.duckdb')
    metrics=[]
    def guard():
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        if rss>max_rss_bytes or shutil.disk_usage(root).free<min_free_bytes:
            raise ValueError('Resource内存或磁盘保护触发')
    try:
        def batches():
            for batch in scan_observations(dsn,upstream_run):
                guard()
                yield batch
        staged.load(batches())
        if completed_binding(dsn,upstream_run)!=binding:
            raise ValueError('上游完成版本变化')
        computer=ResourceComputer(references,topology_enabled=topology_enabled)
        for context in contexts:
            begin=time.monotonic()
            actual=staged.range(context.source_id)
            if staged.content_sha256(context.source_id)!=context.content_sha256:
                raise ValueError('同版观察内容SHA与清单不符')
            if int(context.snapshot_time.timestamp())!=staged.snapshot_epoch(context.source_id):
                raise ValueError('声明快照时点不是该RIB首个MRT元素epoch')
            def elements():
                for index,e in enumerate(staged.elements(context.source_id)):
                    if index%10000==0:
                        guard()
                    yield e
            result=computer.compute(context,elements(),decision_sink=store.decision,membership_sink=store.members)
            store.result(result,actual)
            store.state(result.state)
            guard()
            metrics.append({'source_id':context.source_id,'elements':actual[2],
                'seconds':time.monotonic()-begin,'normal_basis':result.state.initial_state_basis})
        report=store.finish(dict(seconds_before_completion_gate=time.monotonic()-started,ribs=metrics,
            peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),
            limitations=['synthetic_or_declared_scope_only','cold_start_not_historical_equivalence']))
        return report
    except BaseException as exc:
        state=store.fail(exc)
        if state=='complete':
            saved=json.loads((root/'execution.json').read_text())
            return {**saved,'state':'complete','commit_confirmation':'verified_after_error'}
        (root/'failure.json').write_text(json.dumps({'state':state,'run_id':store.run_id,'reason':str(exc)},ensure_ascii=False))
        raise
    finally:
        staged.close()
        store.close()


def produce_bound_resources(dsn, sources, result_window, csv_binding, country_binding, output, *,
                            topology_enabled=True, fixture_only=False, max_rss_bytes=2*1024**3,
                            min_free_bytes=1024**3):
    """同catalog多完成来源，公共Reader下推指定RIB；不创建观察暂存副本。"""
    from itertools import chain
    from data_pipeline.bgp.archive.message_reader import ObservationReader, MessageBatch, SourceEnd
    from data_pipeline.bgp.input.path_decoding import DECODER_VERSION, decoding_difference
    from data_pipeline.analysis.resources.bindings import validate_sources, manifest
    from data_pipeline.analysis.resources.compute import RibElement
    from data_pipeline.analysis.resources.references import read_reference
    from data_pipeline.analysis.resources.identity import identity_digest
    started=time.monotonic()
    expected=execution_identity(fixture_only=fixture_only)
    root=Path(output)
    data_probe=None
    def guard():
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        probe=root if root.exists() else root.parent
        low_disk=shutil.disk_usage(probe).free<min_free_bytes
        if data_probe is not None:low_disk |= shutil.disk_usage(data_probe).free<min_free_bytes
        if rss>max_rss_bytes or low_disk:
            raise ValueError('Resource内存或磁盘保护触发')
    readers=validate_sources(dsn,sources,result_window,guard=guard)
    if fixture_only and any(not s.context.origin_uri.startswith('fixture://') for s in sources):
        raise ValueError('fixture来源须显式声明')
    csv_reader=ObservationReader(dsn,csv_binding['run_id'],csv_binding['snapshot'],[csv_binding['anchor_source_id']],guard=guard)
    def csv_receipt():
        with psycopg2.connect(dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute('SELECT state,row_count FROM domeye.reference_inputs WHERE run_id=%s AND source_id=%s',(csv_binding['run_id'],csv_binding['source_id']))
                receipt=c.fetchone()
        if receipt is None or receipt[0]!='validated':raise ValueError('CSV参考未验证')
        return receipt
    expected_csv_receipt=csv_receipt()
    countries,country_identity=read_reference(dsn,**country_binding,allow_fixture=fixture_only)
    # 复用已GO CSV投影，临时对象行只来自已complete独立参考的明确last-wins投影。
    import pyarrow as pa
    country_rows=[{'source_id':country_binding['dataset_id'],'row':i,'location':asn,
        'raw_row':json.dumps({'__object_pairs__':[['country_cn',values[0]]]},ensure_ascii=False)}
        for i,(asn,values) in enumerate(countries.items())]
    references=reference_projection(chain(csv_reader.reference_batches(csv_binding['source_id']),
        [pa.Table.from_pylist(country_rows)]),csv_source=csv_binding['source_id'],country_source=country_binding['dataset_id'])
    reference_id=identity_digest({'csv':csv_binding,'country':country_identity})
    if any(s.context.reference_id!=reference_id for s in sources):raise ValueError('参考绑定摘要不符')
    binding_manifest=manifest(sources,result_window,csv_binding,country_identity)
    # Reader构造和每次connect都会复核精确run/snapshot/manifest；完成前再次逐一核对。
    def validate_before_finish():
        current=validate_sources(dsn,sources,result_window,guard=guard)
        if [(r.manifest,r.starts) for r in current]!=[(r.manifest,r.starts) for r in readers]:raise ValueError('上游manifest或来源回执漂移')
        if ObservationReader(dsn,csv_binding['run_id'],csv_binding['snapshot'],[csv_binding['anchor_source_id']]).manifest!=csv_reader.manifest:
            raise ValueError('CSV上游版本漂移')
        if csv_receipt()!=expected_csv_receipt:raise ValueError('CSV参考回执漂移')
        if read_reference(dsn,**country_binding,allow_fixture=fixture_only)[1]!=country_identity:raise ValueError('补充参考版本漂移')
    store=ResourceStore(dsn,root,sources[0].run_id,[s.context.source_id for s in sources],
        expected_code_identity=expected,fixture_only=fixture_only,binding_manifest=binding_manifest,
        validate_upstreams=validate_before_finish)
    data_probe=Path(store.storage_layout['catalog_data_path'])
    while not data_probe.exists() and data_probe!=data_probe.parent:data_probe=data_probe.parent
    try:
        computer=ResourceComputer(references,topology_enabled=topology_enabled)
        timings=[]
        for binding,reader in zip(sources,readers):
            begin=time.monotonic();actual=[None,None,0];ended=False
            def elements():
                nonlocal ended
                for batch in reader.stream():
                    guard()
                    if isinstance(batch,SourceEnd):ended=True
                    if not isinstance(batch,MessageBatch):continue
                    for row in batch.elements:
                        if row['action']!='rib_snapshot':raise ValueError('选中来源包含非RIB元素')
                        if row['as_path_text'] is None or row['peer_asn'] is None:raise ValueError('RIB路径或Peer缺失')
                        if not actual[2] and row['epoch']!=binding.context.snapshot_time.timestamp():
                            raise ValueError('声明时点不是首元素epoch')
                        actual[0]=row['epoch'] if actual[0] is None else min(actual[0],row['epoch'])
                        actual[1]=row['epoch'] if actual[1] is None else max(actual[1],row['epoch']);actual[2]+=1
                        difference=decoding_difference(row)
                        if difference:store.append('decoding_differences',{'source_id':row['source_id'],'event_id':row['event_id'],'detail':json.dumps(difference,ensure_ascii=False)})
                        yield RibElement(row['source_id'],row['message_id'],row['ordinal'],sys.intern(row['prefix']),
                            sys.intern(row['as_path_text']),str(row['peer_asn']),row['peer_ip'],
                            row['bgp_id'] if row['bgp_id_present'] else None,row['path_key'],row['afi'],row['safi'],
                            row['action'],row['attributed_origin_asn'])
            result=computer.compute(binding.context,elements(),decision_sink=store.decision,membership_sink=store.members)
            if not ended or not actual[2]:raise ValueError('RIB未完整读到结束回执或没有元素')
            store.result(result,actual);store.state(result.state);guard()
            timings.append({'source_id':binding.context.source_id,'purpose':binding.purpose,'elements':actual[2],'seconds':time.monotonic()-begin})
        return store.finish({'ribs':timings,'decoder_version':DECODER_VERSION,'reference_binding_id':reference_id,
            'seconds_before_completion_gate':time.monotonic()-started,'topology_enabled':topology_enabled,
            'limitations':['old_continuous_state_Unknown','reference_historical_effectivity_Unknown',
                          'legacy_unknown_is_not_a_country','warmup_does_not_guarantee_sufficient_samples']})
    except BaseException as exc:
        state=store.fail(exc)
        if state=='complete':return {**json.loads((root/'execution.json').read_text()),'state':'complete','commit_confirmation':'verified_after_error'}
        (root/'failure.json').write_text(json.dumps({'state':state,'run_id':store.run_id,'reason':str(exc)},ensure_ascii=False))
        raise
    finally:store.close()
