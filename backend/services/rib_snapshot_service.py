"""已登记共享快照的公开读取；缺失或损坏不回退其他来源。"""
import os
from pathlib import Path
import re
import sqlite3
import time

from data_pipeline.bgp.snapshots.snapshot_store import FAMILIES, VERSION_PATTERN, check_registered_database, read_database, read_registry, read_registered, validate_date


class SnapshotError(Exception):
    def __init__(self, message, status=503, state='unavailable'):
        super().__init__(message)
        self.status, self.state = status, state


def observation_item(row):
    return {**{key: row[key] for key in ('family', 'prefix', 'physical_record', 'decoded_offset',
                'entry_index', 'originated_time_epoch', 'raw_origin_asn', 'attributed_origin_asn', 'reason')},
            'safi': 1, 'peer': {'index': row['peer_index'], 'bgp_id': row['bgp_id'], 'ip': row['ip'], 'asn': row['peer_asn']},
            'as_path_hex': row['path_key'][1:].hex() if row['path_key'][0] else None,
            'as4_path_hex': row['as4_key'][1:].hex() if row['as4_key'][0] else None}


def query_snapshots(params, version=None, observations=False, asn=None):
    allowed = {'page', 'page_size'} if observations else {'date', 'latest'} if version is None else {'family'}
    if set(params) - allowed:
        raise SnapshotError('查询参数不支持', 400)
    if 'latest' in params and (params['latest'] != 'true' or 'date' in params):
        raise SnapshotError('latest仅允许true，不能同时指定日期', 400)
    try:
        if 'date' in params:
            validate_date(params['date'])
    except (ValueError, TypeError):
        raise SnapshotError('查询日期无效或超出数据档', 400)
    family = params.get('family', 'all')
    if family not in FAMILIES:
        raise SnapshotError('地址族不支持', 400)
    if asn is not None:
        if not re.fullmatch(r'[0-9]{1,10}', asn) or not 1 <= int(asn) <= 4294967295:
            raise SnapshotError('ASN须为1至4294967295的整数', 400)
        asn = int(asn)
    try:
        page, page_size = int(params.get('page', 1)), int(params.get('page_size', 50))
        if not 1 <= page <= 80_000_000 or not 1 <= page_size <= 100:
            raise ValueError()
    except ValueError:
        raise SnapshotError('分页超出允许范围', 400)
    configured = os.environ.get('DOMEYE_RIB_SNAPSHOT_REGISTRY')
    if configured is None:
        return {'state': 'not_configured', 'message': '未配置共享RIB快照'}
    if not configured.strip():
        raise SnapshotError('已配置的共享快照登记位置为空')
    try:
        root = Path(configured)
        index = read_registry(root)
        if version is None:
            chosen = [(day, value) for day, value in sorted(index['dates'].items())
                      if not params.get('date') or day == params['date']]
            if 'latest' in params:
                chosen = chosen[-1:]
            snapshots = []
            for day, selected in chosen:
                manifest = read_registered(root, index, selected)
                snapshots.append({'version': selected, 'date': day, 'observed_at': manifest['summary']['observed_at']})
            selected = dict(chosen)
            days = [params['date']] if 'date' in params else list(selected) if 'latest' in params else sorted(set(index['dates']) | set(index.get('batch_dates', {})))
            statuses = []
            for day in days:
                status = {'date': day, 'state': 'available' if day in selected else 'not_calculated'}
                if day in selected:
                    status['version'] = selected[day]
                batch_id = index.get('batch_dates', {}).get(day)
                if batch_id:
                    outcome = next(row for row in index['batches'][batch_id]['days'] if row['date'] == day)
                    status['last_batch'] = {'selection_id': batch_id, 'state': outcome['state']}
                    if day not in selected:
                        status['state'] = outcome['state']
                statuses.append(status)
            return {'state': 'available', 'snapshots': snapshots, 'days': statuses}
        if not re.fullmatch(VERSION_PATTERN, version) or version not in index['versions']:
            raise SnapshotError('未知快照版本', 404, 'unknown_version')
        manifest = read_registered(root, index, version)
        value = manifest['summary']
        if asn is not None:
            with read_database(root / 'versions' / version / 'snapshot.sqlite') as database:
                deadline = time.monotonic() + 2
                database.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
                where = 'asn=?' + (' AND family=?' if family != 'all' else '')
                args = (asn, family) if family != 'all' else (asn,)
                count = database.execute('SELECT count(*) FROM origins WHERE ' + where, args).fetchone()[0]
                # 起源关联驱动有界取证；同前缀的其他起源或歧义观察不归给该ASN。
                items = []
                prefixes = database.execute('SELECT family,prefix FROM origins WHERE ' + where
                                            + ' ORDER BY family,prefix', args)
                for prefix in prefixes:
                    rows = database.execute('''SELECT * FROM observation_facts
                        WHERE physical_record=(SELECT physical_record FROM prefixes WHERE family=? AND prefix=?)
                        AND attributed_origin_asn=? ORDER BY entry_index LIMIT ?''',
                        (*prefix, asn, 21 - len(items)))
                    items.extend(observation_item(row) for row in rows)
                    if len(items) == 21:
                        break
            check_registered_database(root, index, version)
            return {'state': 'available', 'version': version, 'family': family, 'asn': asn,
                    **{key: value[key] for key in ('date', 'observed_at', 'source', 'origin_rule', 'limitations')},
                    'unit': 'distinct_origin_prefix', 'prefix_count': count,
                    'sample_limit': 20, 'sample_truncated': len(items) > 20, 'items': items[:20]}
        if observations:
            with read_database(root / 'versions' / version / 'snapshot.sqlite') as database:
                deadline = time.monotonic() + 2
                database.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
                rows = database.execute('SELECT * FROM observation_facts ORDER BY physical_record,entry_index LIMIT ? OFFSET ?',
                                        (page_size, (page - 1) * page_size))
                items = [observation_item(row) for row in rows]
            check_registered_database(root, index, version)
            return {'state': 'available', 'version': version, 'source': value['source'],
                    'observed_at': value['observed_at'], 'items': items, 'page': page, 'page_size': page_size,
                    'total': value['families']['all']['rib_entries']}
        return {'state': 'available', 'version': version, 'family': family,
                **{key: value[key] for key in ('date', 'observed_at', 'source', 'origin_rule', 'unit', 'limitations')},
                'metrics': value['families'][family]}
    except (OSError, ValueError, KeyError, TypeError, sqlite3.DatabaseError) as error:
        raise SnapshotError('共享快照校验失败，不能提供数值', state='validation_failed') from error
