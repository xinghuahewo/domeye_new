#!/usr/bin/env python3
"""将已验证的单日异常输入留存为 C 消费包；只读审计目录，输出目录须不存在。

不连接数据库、不运行检测；旧检测版本保持未知。此命令仅支持当前三类
source=r 历史记录及 2026-09-10 用户确认的 RRC25 来源解释。
"""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))
from data_pipeline.anomaly_records import deserialize_record  # noqa: E402
from data_pipeline.core_overview_input import validate_record_time  # noqa: E402


def validate_empty_day(directory, payload, audit, receipt, profile):
    """空文件本身不证明零；要求完整日窗、三类源计数及闭合读取证据。"""
    raw_path = directory / 'day.jsonl'
    if payload or raw_path.stat().st_size > 65536:
        raise ValueError('成功空日必须是空记录文件和有界的完整读取回执')
    entries = [json.loads(line) for line in raw_path.read_bytes().splitlines()]
    context, ending = receipt['context'], receipt['receipt']
    if ([entry['kind'] for entry in entries] != ['context', 'event_counts', 'prefix_buckets', 'receipt']
            or entries[0] != context or entries[-1] != ending
            or context['isolation'] != 'repeatable read'
            or type(context['per_kind']) is not int or context['per_kind'] != 0
            or audit['input_sha256'] != receipt['payload_sha256']):
        raise ValueError('成功空日缺少完整非抽样读取及一致的原始回执')
    counts = entries[1]['counts']
    bucket_audit = audit['hourly_prefix_reconciliation']
    if (set(counts) != {'prefix_outage', 'as_outage', 'leak'}
            or any(type(value) is not int or value != 0 for value in counts.values())
            or entries[2]['items'] != []
            or any(type(value) is not int or value != 0 for value in (
                audit['records'], receipt['event_count'], bucket_audit['prefix_rows'],
                bucket_audit['sum_bucket_distinct'], bucket_audit['window_distinct']))):
        raise ValueError('三类源计数与成功空日不一致')
    if not isinstance(receipt['source_instance'], str) or not receipt['source_instance'].strip():
        raise ValueError('成功空日必须绑定非空来源实例')
    # PostgreSQL 可输出 1—6 位小数；兼容项目 Python 3.10，不改写原始字符串。
    read_at, finished = [datetime.strptime(value, '%Y-%m-%dT%H:%M:%S' + ('.%f' if '.' in value else '') + '%z')
                         for value in (context['read_at'], ending['finished_at'])]
    start = datetime.fromisoformat(context['window_start'])
    end = datetime.fromisoformat(context['window_end_exclusive'])
    if any(value.utcoffset() is None for value in (read_at, finished, start, end)):
        raise ValueError('空日回执和窗口必须带时区')
    start = start.astimezone(ZoneInfo(profile['timezone']))
    if (finished < read_at or start.hour or start.minute or start.second or start.microsecond
            or end != start + timedelta(days=1)
            or not datetime.fromisoformat(profile['window_start']) <= start < end <= datetime.fromisoformat(profile['window_end_exclusive'])):
        raise ValueError('空日回执顺序或完整业务日范围无效')
    return {'basis': 'complete-day-zero-source-counts/v1',
            'audit_sha256': hashlib.sha256((directory / 'day-audit.json').read_bytes()).hexdigest(),
            'receipt_sha256': hashlib.sha256((directory / 'day-receipt.json').read_bytes()).hexdigest(),
            'source_counts': counts,
            'limitation': '仅证明本次读取的三类源记录为空，不证明观察连续、无异常或网络正常'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit-dir', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('输出目录已存在；保留旧版本，选择新的目录')
    audit = json.loads((args.audit_dir / 'day-audit.json').read_text())
    receipt = json.loads((args.audit_dir / 'day-receipt.json').read_text())
    payload = (args.audit_dir / 'day-records.jsonl').read_bytes()
    checks = {
        'day.jsonl': receipt['payload_sha256'],
        'extract.sql': receipt['query_sha256'],
        'day-records.jsonl': audit['converted_file_sha256'],
        'anomaly_records_source.py': audit['mapping_code_sha256'],
        'data-profile.json': audit['data_profile_sha256'],
    }
    for name, expected in checks.items():
        if hashlib.sha256((args.audit_dir / name).read_bytes()).hexdigest() != expected:
            raise ValueError(f'留存摘要不一致：{name}')
    if (audit['errors'] or not audit['roundtrip_equal']
            or audit['hourly_prefix_reconciliation']['mismatches']
            or not audit['hourly_prefix_reconciliation']['checked']
            or receipt['context']['read_only'] != 'on' or receipt['receipt']['read_only'] != 'on'):
        raise ValueError('缺少通过的映射、对账或只读回执')
    profile = json.loads((ROOT / 'config/data-profile.json').read_text())
    if profile != json.loads((args.audit_dir / 'data-profile.json').read_text()):
        raise ValueError('留存数据档与项目配置不同')
    records = [deserialize_record(line) for line in payload.splitlines()]
    if len(records) != audit['records'] or len(records) != receipt['event_count']:
        raise ValueError('记录数量未对齐')
    empty_evidence = validate_empty_day(args.audit_dir, payload, audit, receipt, profile) if not records else None
    context = receipt['context']
    window = {'source': context['source'], 'start': context['window_start'], 'end_exclusive': context['window_end_exclusive']}
    if window['source'] != 'r':
        raise ValueError('本来源确认只适用于 source=r')
    references = set()
    for result in records:
        record = result['record']
        reference = record['identity']['legacy_reference']
        if (reference in references or record['association']['state'] != 'matched'
                or record['source']['read_scope'] != window
                or record['source']['instance'] != receipt['source_instance']):
            raise ValueError('记录身份或读取范围不一致')
        references.add(reference)
        validate_record_time(record)
    manifest = {
        'schema_version': 'core-overview-input/v1',
        'interpretation_version': 'recorded-anomaly-overview/v1',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'data_profile': profile,
        'source': {'instance': receipt['source_instance'], 'code': 'r', 'collector_id': 'rrc25',
                   'collector_basis': 'user_confirmation', 'confirmed_on': '2026-09-10',
                   'coverage': 'unknown', 'detector_version': None},
        'window': window, 'kinds': ['prefix_outage', 'as_outage', 'leak'],
        'records': {'file': 'records.jsonl', 'sha256': checks['day-records.jsonl'], 'count': len(records)},
        'evidence': {'retained_input_sha256': checks['day.jsonl'], 'query_sha256': checks['extract.sql'],
                     'mapping_code_sha256': checks['anomaly_records_source.py'],
                     'retainer_code_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                     'read_at': context['read_at'], 'audit_directory': str(args.audit_dir.resolve()),
                     'collector_confirmation': '本任务 2026-09-10 用户明确说明 r 为 RRC25；不是源行原生字段'},
        'rules': {'family': 'structured-prefix-membership/v1', 'bucket_seconds': 3600,
                  'metric': 'recorded_prefix_outage_starts_distinct',
                  'selection': '指定窗口三类总表引用，无额外时长、编号或 INFO 成员过滤',
                  'limitations': ['观察覆盖未知', '历史检测版本未知', '不代表完整影响集合或全部异常类型']},
    }
    if empty_evidence is not None:
        manifest['evidence']['empty_result'] = empty_evidence
    encoded = (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode('utf-8')
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name, content in [('records.jsonl', payload), ('retainer.py', Path(__file__).read_bytes()), ('manifest.json', encoded)]:
        path = args.output / name
        with path.open('xb') as handle:
            handle.write(content)
        path.chmod(0o600)
    print(json.dumps({'manifest': str((args.output / 'manifest.json').resolve()), 'records': len(records),
                      'version': 'overview_v1_' + hashlib.sha256(encoded).hexdigest()}, ensure_ascii=False))


if __name__ == '__main__':
    main()
