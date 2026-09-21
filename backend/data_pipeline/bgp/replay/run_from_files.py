"""一次固定输入：原件→完整观察→端点盘点→显式计算映射→同版观察回放。"""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import resource
import shutil
import sys
import time
from jsonschema import Draft202012Validator, FormatChecker

from data_pipeline.bgp.input.mrt_reader import read_source, source_identity
from data_pipeline.bgp.replay.route_replay import Replay, ReplayPlan, associate
from data_pipeline.bgp.input.reference_reader import rows as reference_rows
from data_pipeline.bgp.replay.archive_input import build_mapping, messages as stored_messages
from data_pipeline.bgp.archive.store import Store


def code_identity():
    base = Path(__file__).resolve().parents[4]
    pipeline = base / 'backend/data_pipeline'
    files = [*pipeline.joinpath('bgp').rglob('*.py'),
             *pipeline.joinpath('analysis/features').glob('*.py'),
             *pipeline.joinpath('analysis/detection').glob('*.py'),
             *pipeline.joinpath('common').glob('*.py'),
             pipeline/'__init__.py', pipeline/'analysis/__init__.py',
             *pipeline.joinpath('bgp/input/native').glob('*.cpp'),
             *pipeline.joinpath('bgp/input/native').glob('*.hpp'),
             base/'backend/utils/prefix_quantity.py', base/'scripts/native/build-observation-native.py',
             base/'backend/uv.lock', base/'scripts/observations.py', base/'config/data-profile.json',
             base/'contracts/data/observation-run.schema.json']
    return {str(p.relative_to(base)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}


def normalized_inputs(manifest):
    seen = {}
    for entry in manifest['inputs']:
        source = source_identity(manifest['collector'], entry['origin_uri'], entry['sha256'])
        if source != entry['source_id']:
            raise ValueError('观察来源身份与原始定位/内容版本不符')
        if source in seen:
            if seen[source]['role'] != entry['role']:
                raise ValueError('同来源角色冲突')
            continue  # 同一原件的本地缓存路径别名，不创造新观察。
        seen[source] = entry
    inputs = list(seen.values())
    baseline = manifest['baseline_source']
    if not inputs or inputs[0]['source_id'] != baseline or inputs[0]['role'] != 'baseline':
        raise ValueError('指定基线必须是首个baseline角色输入')
    if sum(i['role'] == 'baseline' for i in inputs) != 1:
        raise ValueError('必须且只能有一个计算基线')
    updates = tuple(i['source_id'] for i in inputs if i['role'] == 'update')
    if updates != tuple(manifest['update_sources']):
        raise ValueError('UPDATE顺序与固定声明不符')
    return inputs


def produce(manifest, dsn, output, *, min_free_bytes=2*1024**3, max_rss_bytes=4*1024**3,
            batch_rows=50000, duckdb_memory_limit='1GB', catalog_data_path=None):
    schema = json.loads((Path(__file__).resolve().parents[4]/'contracts/data/observation-run.schema.json').read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(manifest)
    if datetime.fromisoformat(manifest['window_start'].replace('Z', '+00:00')) >= datetime.fromisoformat(manifest['window_end_exclusive'].replace('Z', '+00:00')):
        raise ValueError('结果时间窗无效')
    inputs = normalized_inputs(manifest)
    manifest = {**manifest, 'inputs': inputs}
    baseline, updates = manifest['baseline_source'], tuple(manifest['update_sources'])
    code = code_identity()
    canonical = {**manifest, 'inputs': [{k:v for k,v in i.items() if k != 'path'} for i in inputs],
                 'references': [{'sha256':r['sha256']} for r in manifest.get('references', [])]}
    dataset_id = hashlib.sha256(json.dumps([canonical, code], sort_keys=True).encode()).hexdigest()
    output = Path(output).resolve()
    catalog_root = Path(catalog_data_path).resolve() if catalog_data_path is not None else output/'parquet'
    def disk_guard():
        # 新目录尚不存在时检查最近的已有父目录；分别覆盖临时盘与真实数据盘。
        for target in (output, catalog_root):
            while not target.exists():target=target.parent
            if shutil.disk_usage(target).free < min_free_bytes:
                raise ValueError('磁盘资源保护触发: '+str(target))
    disk_guard()
    store = Store(dsn, output, batch_rows=batch_rows, memory_limit=duckdb_memory_limit,
                  catalog_data_path=catalog_root)
    started = time.monotonic()
    source_metrics = []
    reference = None
    reference_peers = []
    baseline_elements = 0
    prior_update_last = None
    def guard():
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform == 'darwin' else 1024)
        disk_guard()
        if rss > max_rss_bytes:
            raise ValueError('内存或磁盘资源保护触发')
    try:
        store.bind({**manifest, 'code_identity':code, 'dataset_id':dataset_id}, 'mapping_pending')
        for ref in manifest.get('references', []):
            count = 0
            with store.pg, store.pg.cursor() as c:
                c.execute('INSERT INTO domeye.reference_inputs VALUES (%s,%s,%s,%s,%s)',
                          (store.run_id, ref['sha256'], ref['path'], 'candidate', None))
            for row in reference_rows(ref['path'], ref['sha256']):
                if count % 1000 == 0: guard()
                store.append('references', row)
                count += 1
            with store.pg, store.pg.cursor() as c:
                c.execute("UPDATE domeye.reference_inputs SET state='validated',row_count=%s WHERE run_id=%s AND source_id=%s",
                          (count, store.run_id, ref['sha256']))
        reference_seconds = time.monotonic()-started
        for entry in inputs:
            path = Path(entry['path'])
            source_start = time.monotonic()
            count = elements = 0
            if path.stat().st_size != entry['size']: raise ValueError('原件大小与绑定不符')
            with store.pg, store.pg.cursor() as c:
                c.execute('INSERT INTO domeye.inputs VALUES (%s,%s,%s,%s,%s)',
                          (store.run_id, entry['source_id'], str(path), entry['size'], 'candidate'))
            prior_time = None
            rib_epoch = None
            for m in read_source(path, entry['sha256'], source_id=entry['source_id']):
                if m.record % 1000 == 0: guard()
                if (entry['role'] in ('baseline', 'snapshot') and m.kind not in ('peer_index_table','rib')) or (entry['role'] == 'update' and m.mrt_type not in (16,17)):
                    m.reason = m.reason or 'input_role_conflict'
                if entry['role'] in ('baseline','snapshot'):
                    if rib_epoch is None:rib_epoch=m.epoch
                    if m.epoch!=rib_epoch:m.reason=m.reason or 'rib_timestamp_conflict'
                store.message(m)
                if entry['role'] == 'baseline' and m.kind == 'peer_index_table':
                    reference, reference_peers = m, m.peers
                if m.peer and reference is not None:
                    peer_ref, reason = associate(m, reference, reference_peers)
                    store.append('associations', {'message_id':m.message_id, 'peer_ref':peer_ref,
                        'reference_message':reference.message_id, 'rule':'bound_same_time_unique_endpoint/v1', 'reason':reason})
                stamp = (m.epoch, m.microsecond or 0)
                if entry['role']=='update' and count==0 and prior_update_last is not None and stamp<=prior_update_last:
                    store.append('quality',{'source_id':m.source_id,'message_id':m.message_id,
                        'code':'cross_file_time_overlap','detail':'跨文件同秒或时间重叠；声明计算序不等于原始时序'})
                if prior_time is not None and stamp < prior_time:
                    store.append('quality', {'source_id':m.source_id, 'message_id':m.message_id,
                        'code':'timestamp_regression', 'detail':'保留物理顺序；时序连续性未知'})
                prior_time = stamp
                count += 1
                elements += len(m.elements)
            if entry['role']=='update':prior_update_last=prior_time
            store.source_complete(entry['source_id'], count, elements)
            if entry['role'] == 'baseline': baseline_elements = elements
            source_metrics.append({'source_id':entry['source_id'], 'content_sha256':entry['sha256'],
                'messages':count, 'elements':elements, 'seconds':time.monotonic()-source_start})
        if reference is None or baseline_elements == 0:
            raise ValueError('指定基线没有有效Peer表和RIB元素，不允许回放发布')
        store.flush()
        observation_snapshot = store.db.execute('SELECT max(snapshot_id) FROM lake.snapshots()').fetchone()[0]
        store.phase='mapping'
        mapping_started = time.monotonic()
        endpoints = build_mapping(store, baseline, updates)
        if manifest.get('baseline_endpoints') and tuple(tuple(v) for v in manifest['baseline_endpoints']) != endpoints:
            raise ValueError('预声明基线映射与全输入盘点不符')
        plan = ReplayPlan(manifest['collector'], baseline, updates, endpoints)
        mapping_seconds = time.monotonic()-mapping_started
        store.phase='replay'
        replay = Replay(plan)
        replay_started = time.monotonic()
        replay_baseline = 0
        for m in stored_messages(store, baseline, updates, observation_snapshot):
            if m.record % 1000 == 0: guard()
            for row in replay.consume(m):
                store.change(row)
                if m.source_id == baseline and row.get('record_type') != 'invalidation': replay_baseline += 1
        if replay_baseline != baseline_elements:
            raise ValueError('已保存基线未完整初始化计算回放')
        if code_identity() != code: raise ValueError('运行期间生产代码变化')
        with store.pg, store.pg.cursor() as c:
            c.execute('UPDATE domeye.run_specs SET rule_version=%s WHERE run_id=%s', (plan.version, store.run_id))
        store.append('projection_metadata', {'name':'observation_snapshot', 'value':str(observation_snapshot)})
        store.append('projection_metadata', {'name':'baseline_initialized_elements', 'value':str(replay_baseline)})
        store.save_current(replay)
        report = {'dataset_id':dataset_id, 'run_id':store.run_id, 'rule_version':plan.version,
            'sources':source_metrics, 'active_objects':len(replay.current), 'reference_seconds':reference_seconds,
            'mapping_seconds':mapping_seconds, 'replay_seconds':time.monotonic()-replay_started,
            'seconds':time.monotonic()-started,
            'peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024),
            'limitations':['cutover_assumed','source_order_declared','session_continuity_unknown']}
        store.finish(report)
        return {**report, 'state':'complete'}
    except BaseException as exc:
        state=store.fail(exc)
        if state=='complete':
            return {**report,'state':'complete','commit_confirmation':'verified_after_error'}
        try:
            store.append('quality', {'source_id':None, 'message_id':None, 'code':'run_failed', 'detail':str(exc)})
            store.flush()
        except Exception:
            pass
        (Path(output)/'failure.json').write_text(json.dumps({'state':state, 'reason':str(exc), 'run_id':store.run_id}, ensure_ascii=False))
        raise
    finally:
        store.close()
