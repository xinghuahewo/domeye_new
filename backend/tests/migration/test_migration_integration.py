"""仅人工输入：共享观察catalog与正式冻结双Feature的联合接缝。"""
import csv
import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import struct
import subprocess
import sys

import psycopg2
from data_pipeline.analysis.features.calculation import FileWindow
from data_pipeline.analysis.features.projection import FeaturePlan, SourceBinding
from data_pipeline.analysis.features.reference import USECOLS
from data_pipeline.analysis.features.store import TABLES, read_table, reconstruct_phases, DIAGNOSTICS_VERSION
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.bgp.archive.store import query
from tests.features.test_feature_storage import formal_call
from tests.observations.test_observation_consumer import feature_dsn
from tests.observations.test_observation_mrt import update, attr
from tests.observations.test_observation_two_phase import fixture_manifest

ROOT = Path(__file__).resolve().parents[3]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs(root, index):
    root.mkdir()
    manifest = fixture_manifest(root)
    origin = 64498 + index
    def message(withdraw, vp, prefix):
        raw = update(ann=b'' if withdraw else prefix, withdrawn=prefix if withdraw else b'',
                     attrs=b'' if withdraw else attr(origin))
        return struct.pack('!I', 200)+raw[4:12]+struct.pack('!I', vp)+raw[16:]
    # 新VP同批A→W；另一VP最后路径保留不同真实起源。
    raw = message(False, 64500, b'\x18\xc6\x33\x64')+message(True, 64500, b'\x18\xc6\x33\x64')
    raw += message(False, 64497, b'\x18\xc0\x00\x02')
    for entry, data in zip(manifest['inputs'][1:], (raw, b'')):
        Path(entry['path']).write_bytes(gzip.compress(data, mtime=0))
    for entry in manifest['inputs']:
        path = Path(entry['path'])
        entry.update(sha256=sha(path), size=path.stat().st_size,
                     origin_uri=f'fixture://integration/{index}/'+path.name)
        entry['source_id'] = source_identity('rrc25', entry['origin_uri'], entry['sha256'])
    manifest['baseline_source'] = manifest['inputs'][0]['source_id']
    manifest['update_sources'] = [e['source_id'] for e in manifest['inputs'][1:]]
    ref = root/'reference.csv'
    with ref.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=USECOLS); writer.writeheader()
        for asn, name, code in [(64496, '伊朗', 'IR'), (origin, '美国' if index == 0 else '伊朗', 'US' if index == 0 else 'IR')]:
            writer.writerow({'asn': asn, 'as_country_cn': name, 'as_country': code})
    manifest['references'] = [{'path': str(ref), 'sha256': sha(ref)}]
    return manifest, ref


def all_tables(dsn, report):
    return {name: [r for b in read_table(dsn, report['run_id'], report['snapshot'], name) for r in b.to_pylist()]
            for name in TABLES}


def table_multisets(tables):
    """只忽略未承诺的外层行序；保留重复、类型及嵌套列表顺序。"""
    from collections import Counter
    def frozen(value):
        if isinstance(value, dict):
            return ('dict', tuple(sorted((key, frozen(item)) for key, item in value.items())))
        if isinstance(value, (list, tuple)):
            return (type(value).__name__, tuple(frozen(item) for item in value))
        return (type(value).__module__, type(value).__qualname__, repr(value))
    return {name: Counter(frozen(row) for row in rows) for name, rows in tables.items()}


def test_two_producers_formal_features_fixed_history(tmp_path, feature_dsn):
    catalog = tmp_path/'shared-catalog'
    evidence = []
    for index in (0, 1):
        root = tmp_path/str(index)
        manifest, ref = inputs(root, index)
        if index == 0:
            upstream = produce(manifest, feature_dsn, root/'observation', catalog_data_path=catalog,
                               min_free_bytes=0, batch_rows=2)
        else:
            config = root/'config.json'; source = root/'manifest.json'
            config.write_text(json.dumps({'dsn': feature_dsn, 'catalog_data_path': str(catalog), 'limits': {'min_free_bytes': 0, 'batch_rows': 2}}))
            source.write_text(json.dumps(manifest))
            result = subprocess.run([sys.executable, str(ROOT/'scripts/observations.py'), 'produce',
                '--config', str(config), '--manifest', str(source), '--output', str(root/'observation')], capture_output=True, text=True)
            (root/'produce.stdout').write_text(result.stdout); (root/'produce.stderr').write_text(result.stderr)
            assert result.returncode == 0, result.stderr
            upstream = json.loads(result.stdout)
        assert upstream['catalog_data_path'] == str(catalog)
        reader = ObservationReader(feature_dsn, upstream['run_id'], upstream['snapshot'], [e['source_id'] for e in manifest['inputs']])
        version = f"{reader.run_id}:{reader.snapshot}"
        dt = lambda second: datetime.fromtimestamp(second, timezone.utc)
        bindings = tuple(SourceBinding(s.source_id, s.content_sha256, s.role,
            FileWindow(version, s.source_id, dt(100*(rank+1)), dt(100*(rank+2)), dt(100*(rank+1)), 'unknown'), s.expected_elements)
            for rank, s in enumerate(reader.starts))
        plan = FeaturePlan(version, 'rrc25', bindings)
        result = formal_call(root/'formal', reader, plan, sha(ref), ref, batch_rows=100 if index == 0 else 1)
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        tables = all_tables(feature_dsn, report)
        spec = report['specification']
        assert spec['observation_run'] == upstream['run_id'] and spec['observation_snapshot'] == upstream['snapshot']
        assert spec['source_ids'] == [e['source_id'] for e in manifest['inputs']]
        assert spec['reference_sha256'] == sha(ref) and spec['collector'] == 'rrc25'
        identity = spec['code_identity']
        assert identity['execution_mode'] == 'frozen-fresh-process'
        assert identity['execution_binding']['pid'] != __import__('os').getpid()
        for name, entry in identity['files'].items():
            assert entry['sha256'] == sha(ROOT/name)
        for path in identity['execution_binding']['module_sources'].values():
            assert path in identity['files']
        assert identity['files']['backend/data_pipeline/bgp/archive/store.py']['sha256'] == sha(ROOT/'backend/data_pipeline/bgp/archive/store.py')
        for mode in ('ordinary', 'ir'):
            collect = sorted((r for r in tables['windows'] if r['mode'] == mode and r['scope'] == 'collect'), key=lambda r:r['source_rank'])
            assert [(r['announ_num'], r['withdraw_num']) for r in collect] == ([(2,1),(0,0)] if mode == 'ordinary' or index == 1 else [(0,1),(0,0)])
            assert [r['start'] for r in collect] == [dt(200).isoformat(), dt(300).isoformat()]
            assert all(r['resource_status'] == 'unknown' for r in collect)
            phases = reconstruct_phases(feature_dsn, report['run_id'], report['snapshot'], mode)
            assert phases[1,'window_end']['collect','collect',''][3:5] == ((2,1) if mode == 'ordinary' or index == 1 else (0,1))
            assert phases[1,'next_window']['collect','collect',''][3:5] == (0,0)
        assert {r['source_id'] for r in tables['reference_rows']} == {sha(ref)}
        assert all(r['elements'] == 0 and r['messages'] == 0 for r in tables['source_receipts'] if r['source_rank'] == 2)
        with psycopg2.connect(feature_dsn) as pg, pg.cursor() as c:
            c.execute('SELECT mode,vp FROM feature.seen_vps WHERE run_id=%s ORDER BY mode,vp', (report['run_id'],))
            seen = c.fetchall()
            expected = [('ir','64497'),('ordinary','64497'),('ordinary','64500')] if index == 0 else [('ir','64497'),('ir','64500'),('ordinary','64497'),('ordinary','64500')]
            assert seen == expected
            c.execute('SELECT mode,path FROM feature.paths WHERE run_id=%s ORDER BY mode,path', (report['run_id'],))
            assert c.fetchall() == [('ir', '64497 '+str(64496 if index == 0 else 64499)), ('ordinary', '64497 '+str(64498+index))]
        evidence.append({'upstream': upstream, 'feature': report, 'seen': seen})
        if index == 0:
            old_tables, old_elements = tables, query(feature_dsn, upstream['run_id'], 'elements')
            old_files = {str(p): sha(p) for p in tmp_path.rglob('*.parquet')}
            old_receipts = {str(p): p.read_bytes() for p in tmp_path.rglob('execution.json')}
    first = evidence[0]
    assert all_tables(feature_dsn, first['feature']) == old_tables
    assert query(feature_dsn, first['upstream']['run_id'], 'elements') == old_elements
    assert all(sha(Path(p)) == value for p, value in old_files.items())
    assert all(Path(p).read_bytes() == value for p, value in old_receipts.items())
    assert evidence[0]['upstream']['snapshot'] < evidence[1]['upstream']['snapshot']
    (tmp_path/'integration-evidence.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2))


def test_resource_feature_share_observations_and_references(tmp_path, feature_dsn):
    """9+3 RIB与UPDATE共存：按模块选源，正式计算相互追加而旧结果不变。"""
    from dataclasses import asdict, replace
    from data_pipeline.analysis.features.inputs import SourceView, ReferenceView
    from datetime import timedelta
    from data_pipeline.analysis.resources.bindings import SourceBinding as ResourceBinding
    from data_pipeline.analysis.resources.identity import identity_digest
    from data_pipeline.analysis.resources.references import read_reference, scan_reference_rows, read_reference_original
    from data_pipeline.analysis.resources.store import TABLES as RESOURCE_TABLES, scan_resource
    from tests.resources.test_resource_store import fixture_inputs
    from tests.resources.test_resource_bindings import json_request

    manifest, contexts = fixture_inputs(tmp_path, ribs=12)
    first = datetime(2026, 2, 24, 16, tzinfo=timezone.utc)
    contexts_new = []
    for index, (entry, context) in enumerate(zip(manifest['inputs'], contexts)):
        time = first + timedelta(hours=8*index)
        raw = bytearray(gzip.decompress(Path(entry['path']).read_bytes()))
        offset = 0
        while offset < len(raw):
            length = struct.unpack_from('!I', raw, offset+8)[0]
            struct.pack_into('!I', raw, offset, int(time.timestamp()))
            offset += 12+length
        Path(entry['path']).write_bytes(gzip.compress(raw, mtime=0))
        entry.update(sha256=sha(Path(entry['path'])), size=Path(entry['path']).stat().st_size)
        entry['source_id'] = source_identity('rrc25', entry['origin_uri'], entry['sha256'])
        contexts_new.append(replace(context, source_id=entry['source_id'], content_sha256=entry['sha256'], snapshot_time=time))
    csv_path = tmp_path/'as.csv'
    with csv_path.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[*USECOLS, 'global_rank']); writer.writeheader()
        for asn, name, country, code, rank in [(64497,'peer-first','伊朗','IR','12.5'),
                (64497,'peer-duplicate','美国','US','99'),(9808,'path-vp','伊朗','IR','1'),
                (100,'baseline-origin','伊朗','IR','2'),(101,'update-origin','美国','US','3')]:
            writer.writerow({'asn':asn,'as_name':name,'as_country_cn':country,'as_country':code,'global_rank':rank,'org_name':'Org'+str(asn),'type':'ISP',
                'import_as':'[]','export_as':'[]','v4Peer':'[]','v6Peer':'[]','sibling_as':'[]','is_ddos_provider':'False'})
    from tests.detection.test_detection_pipeline import saved_reference_files
    detection_refs = saved_reference_files(tmp_path,rich=True)
    as_info = next(entry for entry in detection_refs if entry['role']=='as_info')
    as_info.update(path=str(csv_path),sha256=sha(csv_path),source_id=sha(csv_path),expected_rows=6)
    # 同一CSV供三个计算；其余十类原件随D保存，不在各计算内另造数据。
    manifest['references'] = [{'path':entry['path'],'sha256':entry['sha256']} for entry in detection_refs]
    manifest['window_start'] = first.isoformat()
    manifest['window_end_exclusive'] = (first+timedelta(days=4)).isoformat()
    warm = {**manifest, 'inputs':manifest['inputs'][:9], 'baseline_source':contexts_new[0].source_id, 'references':[]}
    day = {**manifest, 'inputs':[{**e,'role':'baseline' if i == 0 else 'snapshot'} for i,e in enumerate(manifest['inputs'][9:])],
           'baseline_source':contexts_new[9].source_id}
    result_start = contexts_new[9].snapshot_time
    # P最后一个snapshot作为显式initial_rib；D保存同来源别名但不重新初始化。
    alias = dict(warm['inputs'][8])
    alias_path = tmp_path/'alias-initial.gz'
    alias_path.write_bytes(Path(alias['path']).read_bytes()); alias['path'] = str(alias_path)
    day['inputs'].append(alias)
    def msg(seconds, withdraw, vp, prefix, origin):
        attrs = b'\x40\x02\x0a\x02\x02'+struct.pack('!II',9808,origin)
        raw = update(ann=b'' if withdraw else prefix, withdrawn=prefix if withdraw else b'', attrs=b'' if withdraw else attrs)
        return struct.pack('!I',int(result_start.timestamp())+seconds)+raw[4:12]+struct.pack('!I',vp)+raw[16:]
    messages = msg(-600,False,64500,b'\x18\xc6\x33\x64',100)+msg(-600,True,64500,b'\x18\xc6\x33\x64',100)+msg(-600,False,64497,b'\x18\x0a\x00\x00',101)
    # 合法默认路由触发原skip规则，不改变业务阈值和有效A/W。
    messages += msg(-600,False,64497,b'\x00',100)
    for target, records in [(warm,[('p-update',messages),('p-empty',b'')]),
                            (day,[('d-update',msg(0,True,64497,b'\x18\x0a\x00\x00',101)+msg(1,False,64498,b'\x18\x0a\x00\x01',101)),('d-empty',b'')])]:
        for name, raw in records:
            path = tmp_path/(name+'.gz'); path.write_bytes(gzip.compress(raw,mtime=0))
            uri = 'fixture://integration-resource/'+name; digest = sha(path)
            target['inputs'].append({'path':str(path),'sha256':digest,'size':path.stat().st_size,'origin_uri':uri,
                'source_id':source_identity('rrc25',uri,digest),'role':'update'})
        target['update_sources'] = [e['source_id'] for e in target['inputs'] if e['role']=='update']
    catalog = tmp_path/'shared-catalog'
    upstream_warm = produce(warm,feature_dsn,tmp_path/'warm',catalog_data_path=catalog,min_free_bytes=0)
    config = tmp_path/'day-config.json'; manifest_path = tmp_path/'day-manifest.json'
    config.write_text(json.dumps({'dsn':feature_dsn,'catalog_data_path':str(catalog),'limits':{'min_free_bytes':0}}))
    manifest_path.write_text(json.dumps(day))
    def cli(script, arguments, prefix):
        completed = subprocess.run([sys.executable,str(ROOT/'scripts'/script),*arguments],capture_output=True,text=True)
        (tmp_path/(prefix+'.stdout')).write_text(completed.stdout)
        (tmp_path/(prefix+'.stderr')).write_text(completed.stderr)
        assert completed.returncode == 0, completed.stderr
        return json.loads(completed.stdout)
    upstream_day = cli('observations.py',['produce','--config',str(config),'--manifest',str(manifest_path),'--output',str(tmp_path/'day')],'day')
    # 生产完成立即固定原回执SHA；不从之后的表扫描重建完整性预期。
    day_execution = tmp_path/'day/execution.json'
    day_execution_sha = sha(day_execution)
    from data_pipeline.bgp.archive.store import TABLES as OBSERVATION_TABLES, scan
    def observation_tables(run):
        return {name:[row for batch in scan(feature_dsn,run,name) for row in batch.to_pylist()] for name in OBSERVATION_TABLES}
    fixed_observations = {r['run_id']:observation_tables(r['run_id']) for r in (upstream_warm,upstream_day)}
    observation_files = {str(p):sha(p) for p in catalog.rglob('*.parquet')}

    raw_reference = ('{"9808":{"asn":"9808","aut_name":"vp","country":"X","country_cn":"未知","org_name":"组织"},'
        '"100":{"asn":"100","aut_name":"origin","country":NaN,"country_cn":"甲","country_cn":"未知","org_name":"组织"},'
        '"101":{"asn":"101","aut_name":"update","country":null,"country_cn":"未知","org_name":"组织"}}').encode()
    supplement = tmp_path/'supplement.json'; supplement.write_bytes(raw_reference)
    ref_request = tmp_path/'reference-request.json'
    ref_request.write_text(json.dumps({'operation':'register_reference','dsn':feature_dsn,'path':str(supplement),
        'output':str(tmp_path/'supplement-output'),'origin_uri':'fixture://integration-resource/country',
        'content_sha256':sha(supplement),'min_free_bytes':0}))
    reference = cli('pipeline/resource-frozen-run.py',[str(ref_request)],'reference')
    country_binding = {k:reference[k] for k in ('reference_id','dataset_id')}
    country, country_identity = read_reference(feature_dsn,**country_binding)
    assert country['100'][0] == '未知'
    assert read_reference_original(feature_dsn,**country_binding) == raw_reference
    saved_reference = list(scan_reference_rows(feature_dsn,**country_binding))
    assert len(saved_reference) == 3
    by_asn = {row['raw_key']:row for row in saved_reference}
    assert dict(by_asn['100']['value']['pairs'])['country'] == {'kind':'nonstandard_constant','token':'NaN','value_state':'non_finite'}
    assert dict(by_asn['101']['value']['pairs'])['country'] == {'kind':'scalar','value':None}
    assert all(row['country_scope']=='legacy_unknown' for row in saved_reference)
    for row in saved_reference:
        assert raw_reference[row['byte_offset']:row['byte_offset']+row['byte_length']].decode() == row['raw_entry']
    csv_binding = {'run_id':upstream_day['run_id'],'snapshot':upstream_day['snapshot'],
                   'source_id':sha(csv_path),'anchor_source_id':contexts_new[9].source_id}
    ref_id = identity_digest({'csv':csv_binding,'country':country_identity})
    bindings = [ResourceBinding(up['run_id'],up['snapshot'],purpose,replace(context,reference_id=ref_id))
        for up,purpose,selected in [(upstream_warm,'warmup',contexts_new[:9]),(upstream_day,'result',contexts_new[9:])]
        for context in selected]
    resource_request = {'dsn':feature_dsn,'sources':bindings,'result_window':[result_start,result_start+timedelta(days=1)],
        'csv_binding':csv_binding,'country_binding':country_binding,'output':str(tmp_path/'resource-first'),'min_free_bytes':0}
    request_path = tmp_path/'resource-first.json';request_path.write_text(json.dumps(json_request(resource_request)))
    resource_first = cli('pipeline/resource-frozen-run.py',[str(request_path)],'resource-first')
    def resource_tables(report, scope):
        return {name:[r for b in scan_resource(feature_dsn,report['run_id'],name,scope=scope) for r in b.to_pylist()] for name in RESOURCE_TABLES}
    old_resource = {scope:resource_tables(resource_first,scope) for scope in ('result','all')}
    assert {r['source_id'] for r in old_resource['result']['sources']} == {c.source_id for c in contexts_new[9:]}
    assert {r['source_id'] for r in old_resource['all']['sources']} == {c.source_id for c in contexts_new}
    assert not set(warm['update_sources']+day['update_sources']) & {r['source_id'] for r in old_resource['all']['decision_refs']}
    assert len(old_resource['all']['decision_refs']) == 24
    assert len(old_resource['result']['decision_refs']) == 6
    for scope in ('result','all'):
        rows = old_resource[scope]
        assert all((r['ipv4_prefix_count'],r['ipv4_address_count'],r['path_count']) == (2,512,1) for r in rows['metrics'] if r['bucket']=='global')
        assert all((r['as_name'],r['as_rank']) == ('peer-first',12) for r in rows['metrics'] if r['dimension']=='peer_asn')
        assert all(r['country_scope']=='legacy_unknown' and {int(r['a_asn']),int(r['b_asn'])} == {9808,100} for r in rows['topology_edges'])
        assert len(rows['topology_edges']) == (3 if scope=='result' else 12)
    assert any(r['sample_time'] < result_start for r in old_resource['result']['normal_samples'])
    assert all(r['list_len'] >= 9 for r in old_resource['result']['normal_bands'] if r['bucket']=='global')
    assert not (tmp_path/'resource-first/observations.duckdb').exists()
    old_files = {str(p):sha(p) for p in tmp_path.rglob('*.parquet')}
    old_receipts = {str(p):p.read_bytes() for p in tmp_path.rglob('execution.json')}

    def view(upstream, entry, left, right, role):
        reader = ObservationReader(feature_dsn,upstream['run_id'],upstream['snapshot'],[entry['source_id']])
        receipt = reader.starts[0]
        version = f'{reader.run_id}:{reader.snapshot}'
        return SourceView(reader.run_id,reader.snapshot,entry['source_id'],entry['origin_uri'],entry['sha256'],entry['role'],
            role,sha(csv_path),receipt.expected_messages,receipt.expected_elements,
            FileWindow(version,entry['source_id'],left,right,left,'unknown'))
    at = lambda seconds: result_start+timedelta(seconds=seconds)
    seed = view(upstream_warm,warm['inputs'][8],contexts_new[8].snapshot_time,at(-600),'initial_rib')
    alias_view = view(upstream_day,alias,contexts_new[8].snapshot_time,at(-600),'initial_rib')
    views = [seed,alias_view,
        view(upstream_warm,warm['inputs'][-2],at(-600),at(-300),'update'),
        view(upstream_warm,warm['inputs'][-1],at(-300),at(0),'update'),
        view(upstream_day,day['inputs'][-2],at(0),at(300),'update'),
        view(upstream_day,day['inputs'][-1],at(300),at(600),'update')]
    ref_view = ReferenceView(upstream_day['run_id'],upstream_day['snapshot'],sha(csv_path),str(csv_path),6)
    feature_request = dict(dsn=feature_dsn,collector='rrc25',source_views=[asdict(v) for v in views],
        reference_view=asdict(ref_view),comparison_window=[at(-600),at(0)],result_window=[at(0),at(600)],
        output=str(tmp_path/'feature'),batch_rows=2)
    feature_path = tmp_path/'feature-request.json'
    feature_path.write_text(json.dumps(feature_request,default=lambda t:t.isoformat()))
    feature = cli('pipeline/feature-frozen-run.py',[str(feature_path)],'feature')
    feature_tables = all_tables(feature_dsn,feature)
    assert len(feature_tables) == 8
    diagnostics = feature_tables['module_diagnostics']
    assert {r['mode'] for r in diagnostics} == {'ordinary','ir'}
    defaults = [r for r in diagnostics if r['raw_prefix']=='0.0.0.0/0']
    assert len(defaults)==2 and {r['mode'] for r in defaults}=={'ordinary','ir'}
    assert all(r['reason']=='oversized_ipv4_prefix' and r['normalized_prefix']=='0.0.0.0/0' and r['window_role']=='comparison' for r in defaults)
    for row in diagnostics:
        assert row['dataset_version']==DIAGNOSTICS_VERSION
        assert row['scope']=='update' and row['identifier']
        assert all(row[k] is None for k in ('metric','threshold','comparison','value','unit'))
        assert row['sample_prefixes']==[]
        assert row['rule_id'] and row['reference_version'] and row['projection_version']
        assert row['observation_version']==feature['specification']['observation_version']
        binding=feature['specification']['source_bindings'][row['source_rank']]
        assert row['source_id']==binding['source_id']
    for receipt in feature_tables['source_receipts']:
        selected=[r for r in diagnostics if (r['mode'],r['source_id'])==(receipt['mode'],receipt['source_id'])]
        assert receipt['diagnostics']==len(selected)
        assert receipt['diagnostics_state']==('saved' if receipt['source_rank'] else 'not_applicable')
        assert sorted(r['sequence'] for r in selected)==list(range(len(selected)))
    assert feature['actual_rows']['module_diagnostics']==len(diagnostics)
    spec = feature['specification']
    assert spec['reference_sha256'] == csv_binding['source_id']
    assert spec['reference_binding']['run_id'] == csv_binding['run_id']
    assert spec['reference_binding']['snapshot'] == csv_binding['snapshot']
    assert spec['reference_binding']['carrier_collector'] == 'rrc25'
    assert spec['reference_binding']['expected_rows'] == 6
    assert spec['observation_run'] is None and len(spec['source_bindings']) == 5
    assert len(spec['source_bindings'][0]['aliases']) == 2
    assert spec['source_bindings'][0]['source_role'] == 'snapshot'
    assert day['baseline_source'] not in spec['source_ids']
    assert warm['inputs'][-1]['sha256'] == day['inputs'][-1]['sha256']
    assert warm['inputs'][-1]['source_id'] != day['inputs'][-1]['source_id']
    assert set((warm['inputs'][-1]['source_id'],day['inputs'][-1]['source_id'])) <= set(spec['source_ids'])
    assert feature['actual_rows']['source_receipts'] == 10
    for mode in ('ordinary','ir'):
        rows = sorted((r for r in feature_tables['windows'] if r['mode']==mode and r['scope']=='collect'),key=lambda r:r['source_rank'])
        assert [(r['announ_num'],r['withdraw_num']) for r in rows] == ([(2,1),(0,0),(1,1),(0,0)] if mode=='ordinary' else [(1,1),(0,0),(0,1),(0,0)])
    def feature_scope(role):
        return [r for b in read_table(feature_dsn,feature['run_id'],feature['snapshot'],'windows',window_role=role) for r in b.to_pylist()]
    result_rows, comparison = feature_scope('result'),feature_scope('comparison')
    assert {r['source_id'] for r in result_rows} == set(day['update_sources'])
    assert {r['source_id'] for r in comparison} == set(warm['update_sources'])
    assert not {r['source_id'] for r in result_rows} & {r['source_id'] for r in comparison}
    us = next(r for r in result_rows if r['mode']=='ordinary' and r['scope']=='asn' and r['subject']=='101')
    assert us['withdraw_num']==1  # 若D RIB重置，则会错误归属100。
    ir = next(r for r in result_rows if r['mode']=='ir' and r['scope']=='asn' and r['subject']=='100')
    assert ir['withdraw_num']==1 and ir['row_presence']=='ir_asn_write_disabled'
    assert all(r['scope']!='asn' for r in result_rows if r['source_rank']==4)
    phases = reconstruct_phases(feature_dsn,feature['run_id'],feature['snapshot'],'ordinary')
    assert phases[3,'window_end']['asn','美国','101'][4]==1
    assert phases[3,'next_window']['asn','美国','101'][4]==0
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT mode,vp FROM feature.seen_vps WHERE run_id=%s ORDER BY mode,vp',(feature['run_id'],))
        assert c.fetchall() == [('ir','64497'),('ir','64500'),('ordinary','64497'),('ordinary','64498'),('ordinary','64500')]
        c.execute('SELECT value FROM public.ducklake_metadata WHERE key=%s',('data_path',))
        assert Path(c.fetchone()[0]).resolve()==catalog.resolve()
        c.execute('SELECT rib_count FROM domeye.resource_work_meta WHERE run_id=%s',(resource_first['run_id'],))
        assert c.fetchone()[0]==12
    old_files.update({str(p):sha(p) for p in catalog.rglob('*.parquet')})
    old_files.update({str(p):sha(p) for p in (tmp_path/'feature').rglob('*.parquet')})
    old_receipts.update({str(p):p.read_bytes() for p in (tmp_path/'feature').rglob('execution.json')})
    from data_pipeline.analysis.detection.models import DetectionScope, FileBoundary
    from data_pipeline.analysis.detection.store import read_records, read_revisions, read_decisions, reconstruct_state, _read_table, read_stored_rows, COLUMNS
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    from tests.detection.test_detection_computation import KINDS
    import uuid
    admin=psycopg2.connect(feature_dsn);admin.autocommit=True
    database='detection_joint_'+uuid.uuid4().hex
    with admin.cursor() as c:c.execute('CREATE DATABASE '+database)
    admin.close()
    detection_dsn=feature_dsn+' dbname='+database
    detection_sources=[day['baseline_source'],*day['update_sources']]
    version=upstream_day['run_id']+':'+str(upstream_day['snapshot'])
    scope=DetectionScope('joint-single-day','r','rrc25',version,at(0).isoformat(),at(600).isoformat())
    detection_request={'observation_dsn':feature_dsn,'detection_dsn':detection_dsn,
        'input_run':upstream_day['run_id'],'input_snapshot':upstream_day['snapshot'],
        'ordered_sources':detection_sources,'scope':asdict(scope),'references':detection_refs,
        'reference_version':'joint-eleven-'+sha(csv_path),
        'boundaries':{source:asdict(FileBoundary(source,version,at(300*i).isoformat(),
            {kind:kind+'_202602' for kind in KINDS})) for i,source in enumerate(day['update_sources'])},
        'output':str(tmp_path/'detection'),'min_free_bytes':0}
    detection_path=tmp_path/'detection-request.json';detection_path.write_text(json.dumps(detection_request))
    detection=cli('pipeline/detection-frozen-run.py',[str(detection_path)],'detection')
    detection_args=(detection_dsn,detection['run_id'],detection['snapshot'])
    det_rows=list(read_records(*detection_args)); det_state=reconstruct_state(*detection_args)
    revisions=list(read_revisions(*detection_args));decisions=list(read_decisions(*detection_args))
    assert revisions and decisions and det_state
    assert any(row['event_kind']=='moas' and row['subject_key']=='10.0.1.0/24' for row in revisions)
    assert any(row['record_kind']=='rule_decision' for row in decisions)
    assert det_state['run']['references']['row_refs']['selection_rule']=='detection-reference-39578fe/v2'
    ready=json.loads((tmp_path/'detection/business/ready.json').read_text())
    assert ready['identity']['selected_sources']==detection_sources
    assert ready['identity']['input_snapshot']==upstream_day['snapshot']
    bound_refs=ready['identity']['reference_sources']
    assert len(bound_refs)==11 and bound_refs['as_info']['source_id']==sha(csv_path)
    assert {r['source_id'] for r in bound_refs.values()}=={r['source_id'] for r in detection_refs}
    assert all(bound_refs[r['role']]['rows']==r['expected_rows'] for r in detection_refs)
    assert ready['identity']['execution_mode']=='frozen-fresh-process'
    for path,entry in ready['identity']['files'].items():assert sha(ROOT/path)==entry['sha256']
    detection_tables={}
    for table,columns in [('records',[name for name,_ in COLUMNS]),
                          ('state_entries',['ordinal','family','attribute','key_json','container','value_json'])]:
        lake_rows=list(read_stored_rows(*detection_args,table,batch_rows=3))
        assert lake_rows==list(_read_table(*detection_args,table))
        direct=connect_duckdb()
        try:
            direct.execute('LOAD ducklake');direct.execute('LOAD postgres')
            direct.execute('ATTACH '+literal('ducklake:postgres:'+detection_dsn)+' AS lake')
            result=direct.execute(f'SELECT * FROM lake.det_{detection["run_id"]}.{table} AT (VERSION => {detection["snapshot"]}) ORDER BY {columns[0]}')
            names=[column[0] for column in result.description]
            assert lake_rows==[dict(zip(names,row)) for row in result.fetchall()]
        finally:direct.close()
        detection_tables[table]=lake_rows
        def values(row):
            return tuple(json.loads(row[n]) if n.endswith('_json') and isinstance(row[n],str) else row[n] for n in columns)
        with psycopg2.connect(detection_dsn) as pg,pg.cursor() as c:
            c.execute('SELECT '+','.join(columns)+' FROM detection.'+table+' WHERE run_id=%s ORDER BY '+columns[0],(detection['run_id'],))
            assert c.fetchall()==[values(row) for row in lake_rows]
    assert det_rows==[{**json.loads(r['attributes_json']),'legacy':json.loads(r['legacy_json']),
        'evidence':json.loads(r['evidence_json'])} for r in detection_tables['records']]
    assert [r['sequence'] for r in revisions]==[r['sequence'] for r in detection_tables['records'] if r['record_kind']=='business_revision']
    assert [r['sequence'] for r in decisions]==[r['sequence'] for r in detection_tables['records'] if r['record_kind']=='rule_decision']
    # 首行交付后scope漂移：公共typed流及旧Reader均须在耗尽时拒绝；finally恢复自身登记。
    import pytest
    with psycopg2.connect(detection_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT scope FROM detection.runs WHERE run_id=%s',(detection['run_id'],));original_scope=c.fetchone()[0]
    for factory in (lambda:read_stored_rows(*detection_args,'records'),lambda:read_stored_rows(*detection_args,'state_entries'),lambda:read_records(*detection_args)):
        stream=factory();next(stream)
        try:
            with psycopg2.connect(detection_dsn) as pg,pg.cursor() as c:
                c.execute('UPDATE detection.runs SET scope=%s::jsonb WHERE run_id=%s',(json.dumps({**original_scope,'joint_tail_probe':True}),detection['run_id']))
            with pytest.raises(ValueError,match='漂移'):list(stream)
        finally:
            stream.close()
            with psycopg2.connect(detection_dsn) as pg,pg.cursor() as c:
                c.execute('UPDATE detection.runs SET scope=%s::jsonb WHERE run_id=%s',(json.dumps(original_scope),detection['run_id']))
    for table in detection_tables:
        assert list(read_stored_rows(*detection_args,table))==detection_tables[table]
    (tmp_path/'typed-reader-evidence.json').write_text(json.dumps({
        'run_id':detection['run_id'],'snapshot':detection['snapshot'],
        'counts':{table:len(rows) for table,rows in detection_tables.items()},
        'direct_lake_all_columns_and_order':True,'pg_json_semantics':True,
        'tail_scope_rejection':['records','state_entries','read_records'],'restored_identical':True},indent=2))
    old_files.update({str(p):sha(p) for p in (tmp_path/'detection').rglob('*.parquet')})
    detection_ready=(tmp_path/'detection/business/ready.json').read_bytes()
    # 已接受Detection仍单输出DATA_PATH；无关表追加检验旧catalog快照，不伪称第二run复用能力。
    db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres')
    db.execute('ATTACH '+literal('ducklake:postgres:'+detection_dsn)+' AS lake')
    db.execute('CREATE TABLE lake.integration_append(value BIGINT)');db.execute('INSERT INTO lake.integration_append VALUES (17)')
    appended_snapshot=db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
    assert appended_snapshot>detection['snapshot'];db.close()
    resource_request['output'] = str(tmp_path/'resource-later')
    request_path = tmp_path/'resource-later.json';request_path.write_text(json.dumps(json_request(resource_request)))
    resource_later = cli('pipeline/resource-frozen-run.py',[str(request_path)],'resource-later')
    assert resource_later['run_id'] != resource_first['run_id']
    for scope in ('result','all'):
        assert table_multisets(resource_tables(resource_first,scope))==table_multisets(old_resource[scope])
        later_tables = resource_tables(resource_later,scope)
        # 引用含自身run，逐条核验真实归属，再仅替换已核验的run前缀比较。
        for row in later_tables['metrics']:
            suffix = '/'.join((row['source_id'],row['dimension'],row['bucket']))
            assert row['membership_ref'] == resource_later['run_id']+'/'+suffix
            row['membership_ref'] = resource_first['run_id']+'/'+suffix
        assert table_multisets(later_tables)==table_multisets(old_resource[scope])
    feature_request['output']=str(tmp_path/'feature-later')
    later_path=tmp_path/'feature-later-request.json'
    later_path.write_text(json.dumps(feature_request,default=lambda t:t.isoformat()))
    feature_later=cli('pipeline/feature-frozen-run.py',[str(later_path)],'feature-later')
    assert feature_later['run_id']!=feature['run_id']
    assert feature_later['actual_rows']['module_diagnostics']==len(diagnostics)
    before_country_resource={scope:resource_tables(resource_later,scope) for scope in ('result','all')}
    # C1消费同一D/Detection/11参考，保留清单额外snapshot，不另造输入。
    import pytest
    from data_pipeline.analysis.country_events.saved_input import CountrySavedInput, DetectionBinding, InputCompletion, CountryRevision
    from data_pipeline.analysis.country_events.saved_contract import ProductionReceiptBinding
    from data_pipeline.bgp.archive.message_reader import SourceEnd
    scratch=tmp_path/'country-scratch';scratch.mkdir()
    reader=ObservationReader(feature_dsn,upstream_day['run_id'],upstream_day['snapshot'],detection_sources)
    receipt=ProductionReceiptBinding(str(day_execution),day_execution_sha)
    def country_adapter(binding=receipt):
        return CountrySavedInput(reader,DetectionBinding(*detection_args),legacy_timezone='Asia/Shanghai',
            production_receipt=binding,scratch_root=str(scratch),batch_rows=2)
    assert len([r for r in reader.manifest['inputs'] if r['role']=='snapshot'])==3
    assert len(reader.manifest['inputs'])==6 and len(reader.sources)==3
    production=json.loads(day_execution.read_text())
    assert production['actual_rows']=={name:len(rows) for name,rows in fixed_observations[upstream_day['run_id']].items()}
    assert len(production['actual_rows'])==15 and len(production['sources'])==6
    entire_reader=ObservationReader(feature_dsn,reader.run_id,reader.snapshot,[r['source_id'] for r in reader.manifest['inputs']])
    assert {(r['source_id'],r['content_sha256'],r['messages'],r['elements']) for r in production['sources']}=={
        (r.source_id,r.content_sha256,r.expected_messages,r.expected_elements) for r in entire_reader.starts}
    country=country_adapter()
    country_rows=list(country.stream())
    assert len([r for r in country_rows if isinstance(r,InputCompletion)])==1
    completion=country_rows[-1]
    assert isinstance(completion,InputCompletion) and completion.status=='input_validated'
    assert completion.enumeration=='complete_empty' and completion.selected_revisions==()
    assert not any(isinstance(r,CountryRevision) for r in country_rows)
    assert completion.production_receipt_sha256==day_execution_sha
    assert (completion.observation_run,completion.observation_snapshot)==(upstream_day['run_id'],upstream_day['snapshot'])
    assert (completion.detection_run,completion.detection_snapshot)==(detection['run_id'],detection['snapshot'])
    assert completion.counts['detection_records']==len(det_rows)
    assert completion.counts['references']==sum(r['expected_rows'] for r in detection_refs)
    ends=[r for r in country_rows if isinstance(r,SourceEnd)]
    assert [(r.source_id,r.messages,r.elements) for r in ends]==[(r.source_id,r.expected_messages,r.expected_elements) for r in reader.starts]
    assert completion.counts['source_messages']==sum(r.messages for r in ends)
    assert completion.counts['source_elements']==sum(r.elements for r in ends)
    assert completion.baseline_initialized_elements==reader.starts[0].expected_elements
    assert country.workdir is None and list(scratch.iterdir())==[]
    # 错摘要与错误15表计数均基于本联合绑定；不修改原回执或已保存表。
    with pytest.raises(ValueError,match='production_receipt_digest_mismatch'):
        country_adapter(ProductionReceiptBinding(str(day_execution),'0'*64))
    bad_path=tmp_path/'country-wrong-receipt.json'
    bad=json.loads(day_execution.read_text());bad['actual_rows']['changes']+=1
    bad_path.write_text(json.dumps(bad))
    failed=[]
    with pytest.raises(ValueError,match='production_table_count_mismatch:changes'):
        for row in country_adapter(ProductionReceiptBinding(str(bad_path),sha(bad_path))).stream():failed.append(row)
    assert not any(isinstance(r,InputCompletion) for r in failed)
    assert list(scratch.iterdir())==[] and sha(day_execution)==day_execution_sha
    def output_value(value):
        if isinstance(value,bytes):return {'bytes_hex':value.hex()}
        if isinstance(value,datetime):return {'datetime':value.isoformat()}
        raise TypeError(type(value).__name__)
    (tmp_path/'country-input.json').write_text(json.dumps([
        {'type':type(row).__name__,'value':asdict(row)} for row in country_rows],default=output_value,ensure_ascii=False,indent=2))
    (tmp_path/'country-binding.json').write_text(json.dumps({
        'observation_run':reader.run_id,'observation_snapshot':reader.snapshot,'sources':reader.sources,
        'manifest_source_count':len(reader.manifest['inputs']),'production_receipt':asdict(receipt),
        'detection':asdict(DetectionBinding(*detection_args)),'completion':asdict(completion),
        'negative_checks':['production_receipt_digest_mismatch','production_table_count_mismatch:changes'],
        'scratch_clean':True},ensure_ascii=False,indent=2))
    from data_pipeline.analysis.country_events.event_aggregation import run_saved, C2Row, C2Completion, EventStatus, InputEvidence
    from data_pipeline.analysis.country_events.compute import Grid
    c2_rows=list(run_saved(country_adapter(),Grid(int(at(0).timestamp()*1000000),int(at(600).timestamp()*1000000),
        (int(at(300).timestamp()*1000000),)),scratch_root=scratch,batch_rows=2))
    c2_end=c2_rows[-1]
    assert isinstance(c2_end,C2Completion) and c2_end.input_kind=='saved_C1'
    assert c2_end.input_completion==completion and c2_end.event_count==0
    assert c2_end.input_completion.enumeration=='complete_empty'
    assert not any(isinstance(r,C2Row) and isinstance(r.value,EventStatus) for r in c2_rows)
    assert c2_end.costs['global_units_scanned']==completion.counts['changes']==4
    assert c2_end.reference_interpretation and c2_end.reference_interpretation['source_id']==sha(csv_path)
    assert any(isinstance(r,C2Row) and isinstance(r.value,InputEvidence) for r in c2_rows)
    assert list(scratch.iterdir())==[]
    (tmp_path/'c2-empty-rows.txt').write_text('\n'.join(repr(r) for r in c2_rows))
    (tmp_path/'c2-empty-evidence.json').write_text(json.dumps(asdict(c2_end),default=output_value,ensure_ascii=False,indent=2))
    # C3保存本链合法空枚举；不从独立多事件fixture补入国家事件。
    from data_pipeline.analysis.country_events.snapshot_store import persist_stream
    from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch, ReadReceipt
    from data_pipeline.analysis.country_events.snapshot_schema import TABLES as C3_TABLES
    c3_grid=Grid(int(at(0).timestamp()*1000000),int(at(600).timestamp()*1000000),
        (int(at(300).timestamp()*1000000),))
    c3_binding=persist_stream(iter(c2_rows),feature_dsn,tmp_path/'c3-empty',c1=country_adapter(),parameters={'grid':asdict(c3_grid)})
    c3_parts=list(ComponentReader(feature_dsn,c3_binding,c1=country_adapter()).stream())
    c3_restored=[r for part in c3_parts if isinstance(part,ReadBatch) for r in part.rows]
    assert repr(c3_restored)==repr(c2_rows)
    assert isinstance(c3_parts[-1],ReadReceipt) and c3_parts[-1].full_body_validated
    assert c3_restored[-1].event_count==0 and c3_restored[-1].input_completion.enumeration=='complete_empty'
    c3_manifest=json.loads((tmp_path/'c3-empty/manifest.json').read_text())
    assert c3_manifest['availability']=='known_empty' and set(c3_manifest['tables'])==set(C3_TABLES)
    assert len(c3_manifest['files'])==len(C3_TABLES)==21 and c3_manifest['event_index']['rows']==0
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT count(*) FROM country_components.event_index WHERE component_id=%s',(c3_binding.component_id,))
        assert c.fetchone()==(0,)
    (tmp_path/'c3-empty-evidence.json').write_text(json.dumps({'binding':asdict(c3_binding),
        'rows':len(c3_restored),'full_typed_original_match':True,'receipt':asdict(c3_parts[-1]),
        'manifest':c3_manifest},ensure_ascii=False,indent=2))
    # Q1只发布已完成Resource/Feature；Detection与C1仍只是上面的同源验证。
    from data_pipeline.results import Publication, PROFILE
    from data_pipeline.results.manifest_io import encode
    pub=Publication(feature_dsn,tmp_path);pub.initialize()
    token=pub.prepare(tmp_path/'resource-first/execution.json',tmp_path/'feature/execution.json')
    pub.publish(token,expected_generation=0)
    assert PROFILE['intent']=='module_scoped' and PROFILE['data_kind']=='fixture'
    assert pub.discover()==(token,1)
    def query_pages(token,kind,mode=''):
        first=pub.query(token,kind,mode=mode,page_size=7)
        pages=[first]+[pub.query(token,kind,mode=mode,page=page,page_size=7) for page in range(2,(first['total']+6)//7+1)]
        assert all(p['publication_id']==token.publication_id and p['availability']=='available' and p['coverage']=='declared_fixture_only' for p in pages)
        return pages,[r for p in pages for r in p['items']]
    old_queries={}
    for kind,mode in [('resource',''),('feature','ordinary'),('feature','ir')]:
        pages,items=query_pages(token,kind,mode);old_queries[kind,mode]=pages
        expected=old_resource['result']['metrics'] if kind=='resource' else [r for r in feature_tables['windows'] if r['mode']==mode]
        assert table_multisets({'rows':[r['value'] for r in items]})==table_multisets({'rows':json.loads(encode(expected))})
        source_report=resource_first if kind=='resource' else feature
        assert all(p['source']['run_id']==source_report['run_id'] and p['source']['snapshot']==source_report['snapshot'] for p in pages)
        for item in items:
            basis=item['calculation']['basis'];value=item['value']
            if kind=='resource':
                source=next(r for r in old_resource['result']['sources'] if r['source_id']==value['source_id'])
                assert basis==json.loads(encode(source))
            else:
                for key in ('source_bindings','reference_binding','result_window','comparison_window','calculation_window','windows'):
                    assert basis[key]==spec[key]
                binding=spec['source_bindings'][value['source_rank']]
                assert value['source_id']==binding['source_id'] and value['window_role']==binding['window_role']
                assert all(value[key]==binding['window'][key] for key in ('start','end','file_time'))
                saved=basis['diagnostics']
                assert saved['state']=='saved' and saved['dataset_version']==DIAGNOSTICS_VERSION
                assert sum(r['count'] for r in saved['sources'])==4
                assert table_multisets({'rows':saved['sources']})==table_multisets({'rows':[
                    {'mode':r['mode'],'source_id':r['source_id'],'count':r['diagnostics'],'state':r['diagnostics_state']} for r in feature_tables['source_receipts']]})
    assert [old_queries['feature',mode][0]['total'] for mode in ('ordinary','ir')]==[15,10]
    # ordinary每窗口5/3/4/3行；IR为3/2/3/2，空源保留country/collect而无未变ASN。
    for mode,counts in [('ordinary',[5,3,4,3]),('ir',[3,2,3,2])]:
        assert [sum(r['mode']==mode and r['source_rank']==rank for r in feature_tables['windows']) for rank in range(1,5)]==counts
    assert spec['comparison_window']==[at(-600).isoformat(),at(0).isoformat()]
    assert spec['result_window']==[at(0).isoformat(),at(600).isoformat()]
    candidate=pub.prepare(tmp_path/'resource-later/execution.json',tmp_path/'feature/execution.json')
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT state FROM domeye.inputs WHERE run_id=%s AND source_id=%s',(upstream_day['run_id'],alias['source_id']));alias_state=c.fetchone()[0]
        c.execute("UPDATE domeye.inputs SET state='failed' WHERE run_id=%s AND source_id=%s",(upstream_day['run_id'],alias['source_id']))
    try:
        with pytest.raises(ValueError):pub.publish(candidate,expected_generation=1)
        assert pub.discover()==(token,1)
    finally:
        with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
            c.execute('UPDATE domeye.inputs SET state=%s WHERE run_id=%s AND source_id=%s',(alias_state,upstream_day['run_id'],alias['source_id']))
    pub.publish(candidate,expected_generation=1)
    assert pub.discover()==(candidate,2) and candidate!=token
    for (kind,mode),pages in old_queries.items():
        assert query_pages(token,kind,mode)[0]==pages
        assert pub.query(token,kind,mode=mode,page=999)['items']==[]
    (tmp_path/'q1-evidence.json').write_text(encode({'profile':PROFILE,'first':asdict(token),'second':asdict(candidate),
        'generation':2,'alias_drift_rejected':True,'old_pages':[{'kind':k,'mode':m,'pages':v} for (k,m),v in old_queries.items()]}))
    # 同一发布层第二个fixture选择器，仅发布原Detection实际事件。
    from tests.migration.test_migration_q2_integration import check_q2
    from data_pipeline.results.profiles import SELECTORS
    q2_pub=Publication(feature_dsn,tmp_path,detection={'output_dsn':detection_dsn,'observation_dsn':feature_dsn})
    q2_pub.migrate_profiles()
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT version FROM publication_q1.schema_version');assert c.fetchall()==[(3,)]
        c.execute('SELECT selector,profile FROM publication_q1.profiles');assert dict(c.fetchall())==SELECTORS
    q2=check_q2(q2_pub,tmp_path/'detection/business/ready.json',(2,2,6,0))
    assert q2['source_message_proof']['total']==5
    assert q2['source_message_proof']['sources'][-1]['messages']==0
    assert {r['event_kind'] for r in q2['events']['items']}=={'hijack','moas'}
    for (kind,mode),pages in old_queries.items():assert query_pages(token,kind,mode)[0]==pages
    assert pub.discover()==(candidate,2)
    (tmp_path/'q2-joint-evidence.json').write_text(encode(q2))
    assert table_multisets(all_tables(feature_dsn,feature))==table_multisets(feature_tables)
    for scope in ('result','all'):
        assert table_multisets(resource_tables(resource_first,scope))==table_multisets(old_resource[scope])
        assert table_multisets(resource_tables(resource_later,scope))==table_multisets(before_country_resource[scope])
    assert table_multisets({name:list(read_stored_rows(*detection_args,name)) for name in detection_tables})==table_multisets(detection_tables)
    assert list(read_records(*detection_args))==det_rows
    assert reconstruct_state(*detection_args)==det_state
    assert (tmp_path/'detection/business/ready.json').read_bytes()==detection_ready
    assert list(scan_reference_rows(feature_dsn,**country_binding))==saved_reference
    assert all(table_multisets(observation_tables(run))==table_multisets(tables) for run,tables in fixed_observations.items())
    assert all(sha(Path(p))==digest for p,digest in {**observation_files,**old_files}.items())
    assert all(Path(p).read_bytes()==value for p,value in old_receipts.items())
    for report in (resource_first,resource_later,reference):
        layout = report['storage_layout']
        assert Path(layout['catalog_data_path']).resolve()==catalog.resolve()
        assert list((catalog/layout['schema_path']).rglob('*.parquet'))
    assert Path(reference['raw_path']).read_bytes()==raw_reference
    for report in (resource_first,resource_later):
        identity = json.loads((Path(report['storage_layout']['local_output'])/'code-identity.json').read_text())
        assert identity['execution_mode']=='frozen-fresh-process'
        assert report['project_module_sources']
        assert set(report['project_module_sources'].values()) <= set(identity['files'])
        for path,entry in identity['files'].items():assert sha(ROOT/path)==entry['sha256']
    # M1版本通过实际保存manifest的源码SHA绑定；interpretation尚未持久化。
    from data_pipeline.bgp.input.mrt_reader import PARSER_VERSION
    assert PARSER_VERSION=='mrt-observation/v2'
    parser_bindings={}
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        for observed in (upstream_warm,upstream_day):
            c.execute('SELECT manifest FROM domeye.run_specs WHERE run_id=%s',(observed['run_id'],))
            saved=c.fetchone()[0]['code_identity']
            selected={name:saved[name] for name in ('backend/data_pipeline/bgp/input/mrt_reader.py','backend/data_pipeline/bgp/input/mrt_types.py')}
            assert all(sha(ROOT/name)==digest for name,digest in selected.items())
            parser_bindings[observed['run_id']]=selected
    (tmp_path/'m1-parser-binding.json').write_text(json.dumps({'parser_version':PARSER_VERSION,
        'binding_kind':'saved_manifest_code_identity','runs':parser_bindings,
        'interpretation_persisted':False},ensure_ascii=False,indent=2))
    (tmp_path/'three-module-evidence.json').write_text(json.dumps({'warm':upstream_warm,'day':upstream_day,
        'reference':reference,'resource_first':resource_first,'feature':feature,'feature_later':feature_later,'resource_later':resource_later,'detection':detection,'detection_appended_snapshot':appended_snapshot,'country_input':asdict(completion)},ensure_ascii=False,indent=2))
