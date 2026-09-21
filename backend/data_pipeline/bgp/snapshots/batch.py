"""固定候选清单驱动的有界离线批次；复用单快照准备和原子登记。"""
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import re
import shutil
import uuid
from zoneinfo import ZoneInfo

from data_pipeline.bgp.snapshots.origin import InputRejected, DeclarationConflict
from data_pipeline.bgp.snapshots.snapshot import prepare, implementation_binding
from data_pipeline.bgp.snapshots.snapshot_store import business_date, encode, profile, read_json, register_batch, require, sha_file


def utc(value):
    require(isinstance(value, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', value), '批次时点须为UTC秒级字符串')
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def source_stamp(path):
    try:
        value = path.lstat()
    except FileNotFoundError:
        return None
    return {key: getattr(value, key) for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns', 'st_mode')}


def normalize(manifest):
    require(manifest['schema_version'] == 'rib-batch-input/v1', '批次输入版本不支持')
    require(set(manifest) == {'schema_version', 'window_start', 'window_end_exclusive', 'candidates'}, '批次输入字段无效')
    settings = profile()
    start, end = utc(manifest['window_start']), utc(manifest['window_end_exclusive'])
    zone = ZoneInfo(settings['timezone'])
    first, last = start.astimezone(zone), end.astimezone(zone)
    require(start < end and first.time().isoformat() == last.time().isoformat() == '00:00:00', '批次须为完整业务日半开范围')
    require(1 <= (last.date() - first.date()).days <= 31, '批次天数上限31天')
    business_date(start.strftime('%Y-%m-%dT%H:%M:%SZ'))
    business_date((end - timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))
    require(isinstance(manifest['candidates'], list) and len(manifest['candidates']) <= 64, '候选上限64份（含别名声明）')
    sources = {}
    for row in manifest['candidates']:
        require(isinstance(row, dict) and set(row) == {'path', 'sha256', 'collector', 'observed_at'}, '候选字段无效')
        require(isinstance(row['sha256'], str) and re.fullmatch('[0-9a-f]{64}', row['sha256']), '候选SHA256无效')
        require(isinstance(row['path'], str) and Path(row['path']).is_absolute(), '候选路径须为绝对路径')
        stamp = utc(row['observed_at'])
        require(start <= stamp < end, '候选时点超出批次范围')
        business_date(row['observed_at'])
        previous = sources.get(row['sha256'])
        if previous and any(previous[key] != row[key] for key in ('collector', 'observed_at')):
            raise DeclarationConflict('同一来源摘要的Collector或时点声明冲突')
        if row['collector'] != 'rrc25':
            raise DeclarationConflict('仅支持RRC25声明')
        if previous is None:
            previous = sources[row['sha256']] = {key: row[key] for key in ('sha256', 'collector', 'observed_at')}
            previous['paths'] = set()
        previous['paths'].add(row['path'])
    candidates = sorted(sources.values(), key=lambda row: (row['observed_at'], row['sha256']))
    days = [(first.date() + timedelta(days=i)).isoformat() for i in range((last.date() - first.date()).days)]
    return candidates, days, settings


def run_batch(manifest_path, root, output, max_database_mib=4096, max_batch_mib=8192):
    require(1 <= max_database_mib <= 4096 and 2 <= max_batch_mib <= 91136, '磁盘预算超出上限')
    manifest_sha = sha_file(manifest_path)
    manifest = read_json(manifest_path)
    candidates, days, settings = normalize(manifest)
    implementation = implementation_binding()
    batch_sha = sha_file(Path(__file__))
    identity = {'window_start': manifest['window_start'],
                'window_end_exclusive': manifest['window_end_exclusive'],
                'data_profile': settings, 'selection_rule': 'first-verified-rib/v1',
                'input_verification': {'rule': 'rib-input-check/v1', 'implementation': implementation, 'batch_sha256': batch_sha},
                'candidates': [{key: row[key] for key in ('sha256', 'collector', 'observed_at')} for row in candidates]}
    selection_id = 'rib_batch_v1_' + hashlib.sha256(encode(identity)).hexdigest()
    report = {'schema_version': 'rib-batch-report/v1', 'selection_id': selection_id, 'identity': identity, 'state': 'committed', 'coverage': 'complete',
              'days': [{'date': day, 'state': 'not_calculated', 'candidates': []} for day in days]}
    require(not output.exists() and not output.is_symlink(), '执行输出目录必须不存在')
    require(not root.resolve().is_relative_to(output.resolve()) and not output.resolve().is_relative_to(root.resolve()), '执行目录与登记目录须独立')
    output.mkdir(parents=True, mode=0o700)
    execution = {'execution_id': str(uuid.uuid4()), 'selection_id': selection_id,
                 'started_at': datetime.now(timezone.utc).isoformat(), 'manifest_local_path': str(manifest_path.resolve()),
                 'max_database_mib': max_database_mib, 'max_batch_mib': max_batch_mib,
                 'source_paths': {row['sha256']: sorted(row['paths']) for row in candidates}, 'attempts': []}
    (output / 'execution.json').write_bytes(encode(execution))
    prepared = {}
    stamps = {Path(alias): source_stamp(Path(alias)) for row in candidates for alias in row['paths']}

    def verify_execution():
        require(implementation_binding() == implementation and sha_file(Path(__file__)) == batch_sha, '执行期间代码发生变化')
        require(profile() == settings and sha_file(manifest_path) == manifest_sha, '执行期间数据档或候选清单变化')
        require(all(source_stamp(path) == stamp for path, stamp in stamps.items()), '运行中输入变化')
    used_bytes = 0
    try:
        for day in report['days']:
            for row in (row for row in candidates if business_date(row['observed_at']) == day['date']):
                record = {'sha256': row['sha256'], 'observed_at': row['observed_at'], 'state': 'rejected'}
                rejections = []
                # 别名仅用于定位。同内容成功一次，其他路径不再产生事实。
                for alias_number, alias in enumerate(sorted(row['paths'])):
                    destination = output / (row['sha256'] + '-' + str(alias_number))
                    cap = min(max_database_mib, (max_batch_mib * 1024 ** 2 - used_bytes) // (2 * 1024 ** 2))
                    require(cap >= 1, '批次磁盘预算耗尽')
                    source = Path(alias)
                    require(source_stamp(source) == stamps[source], '运行中输入变化')
                    try:
                        result = prepare(source, row['sha256'], row['collector'], row['observed_at'], destination, cap)
                    except InputRejected as error:
                        rejections.append(error.detail())
                        execution['attempts'].append({'path': alias, 'sha256': row['sha256'], 'state': 'rejected', 'rejection': error.detail()})
                        # 损坏输入的部分SQLite不保留；拒绝类别、阶段和原原因保存在逐日报告。
                        if destination.exists():
                            shutil.rmtree(destination)
                    else:
                        record = {**record, 'state': 'verified', 'version': result['version']}
                        execution['attempts'].append({'path': alias, 'sha256': row['sha256'], 'state': 'verified'})
                        if 'version' not in day:
                            day.update(state='available', version=result['version'])
                            prepared[result['version']] = destination
                            used_bytes += 2 * sum(path.stat().st_size for path in destination.iterdir())
                            require(used_bytes <= max_batch_mib * 1024 ** 2, '批次磁盘预算耗尽')
                        else:
                            shutil.rmtree(destination)
                        break
                    finally:
                        require(source_stamp(source) == stamps[source], '运行中输入变化')
                if record['state'] == 'rejected':
                    # 来源拒绝不依赖路径或尝试顺序；有内容核验失败时不能被缺失别名掩盖。
                    record['rejection'] = min(rejections, key=lambda error: (error['code'] == 'missing_input', error['code'], error['message']))
                day['candidates'].append(record)
            if day['state'] != 'available':
                day['state'] = 'validation_failed' if any(row['rejection']['code'] != 'missing_input' for row in day['candidates']) else 'missing_input'
                report['coverage'] = 'gaps'
        verify_execution()
        (output / 'execution.json').write_bytes(encode(execution))
        # 本地报告仅标为待提交；最终提交状态由同一次原子索引替换及CLI回执确认。
        (output / 'report.json').write_bytes(encode({**report, 'state': 'ready'}))
        register_batch(prepared, root, report, verify_execution)
        return report
    except Exception as error:
        try:
            (output / 'execution.json').write_bytes(encode(execution))
            (output / 'failure.json').write_bytes(encode({**report, 'state': 'aborted', 'coverage': 'unknown',
                'error': {'type': type(error).__name__, 'message': str(error)}}))
        except OSError:
            pass  # 报告写入也失败时保留原异常；登记尚未提交。
        raise
