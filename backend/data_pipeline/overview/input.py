"""C 首页的留存异常只读查询；不检测、不生产、不连接数据库。"""

import ast
import hashlib
import ipaddress
import json
import re
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from data_pipeline.common.event_records import deserialize_record, serialize_record, is_unresolved_as_set_text

SUPPORTED_KIND_SETS = (
    frozenset({'prefix_outage', 'as_outage', 'leak'}),
    frozenset({'prefix_outage', 'as_outage', 'leak', 'hijack'}),
    frozenset({'prefix_outage', 'as_outage', 'leak', 'hijack', 'sub_hijack'}),
    frozenset({'prefix_outage', 'as_outage', 'leak', 'hijack', 'sub_hijack', 'country_outage'}),
)
SUPPORTED_INTERPRETATIONS = {'recorded-anomaly-overview/v1', 'recorded-anomaly-overview/v2', 'recorded-anomaly-overview/v3'}


def validate_level_conflicts(manifest, records):
    """只解释已绑定输入内的AS等级冲突，不改写明细记录或放宽其他准入条件。"""
    if manifest['interpretation_version'] == 'recorded-anomaly-overview/v1':
        if 'level_conflicts' in manifest:
            raise ValueError('旧解释版本不能携带等级冲突规则')
        return
    conflicts = manifest['level_conflicts']
    if not isinstance(conflicts, dict):
        raise ValueError('等级冲突须按原引用唯一定位')
    by_reference = {row['record']['identity']['legacy_reference']: row['record'] for row in records}
    for reference, evidence in conflicts.items():
        record = by_reference[reference]
        month = reference.split('/')[1][:7].replace('-', '')
        if (set(evidence) != {'event_table', 'event_level', 'detail_level', 'source_input_sha256'}
                or record['common']['kind'] != 'as_outage'
                or evidence['event_table'] != 'event_table_' + month
                or evidence['event_level'] not in {'high', 'middle', 'low'}
                or evidence['detail_level'] not in {'high', 'middle', 'low'}
                or evidence['event_level'] == evidence['detail_level']
                or evidence['detail_level'] != record['raw_fields']['outage_level']
                or evidence['detail_level'] != record['common']['level']['value']
                or not re.fullmatch('[0-9a-f]{64}', evidence['source_input_sha256'])
                or not any(item.get('sha256') == evidence['source_input_sha256']
                           for item in record['source']['evidence_refs'])):
            raise ValueError('等级冲突与记录或留存来源不一致')


def overview_level_filter(item):
    """待核实不是危险等级，也不同于没有等级原值的未知。"""
    return 'conflict' if 'level_conflict' in item else item['level'] or 'unknown'


class RecordTimeConflict(ValueError):
    """可解析的原始时间确有矛盾；不包含无法解释的格式。"""


class InputError(Exception):
    def __init__(self, message, status=503, payload=None):
        super().__init__(message)
        self.status = status
        self.payload = payload


def _source_time(value, zone):
    if isinstance(value, datetime):
        parsed = value
    else:
        match = re.fullmatch(r'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})?', value)
        if not match:
            raise ValueError('原始时间格式不可验证')
        time_format = '%Y-%m-%dT%H:%M:%S' + ('.%f' if match[1] else '') + ('%z' if match[2] else '')
        parsed = datetime.strptime(value.replace(' ', 'T'), time_format)
    return parsed if parsed.utcoffset() is not None else parsed.replace(tzinfo=ZoneInfo(zone))


def validate_record_time(record):
    """消费准入检查原始时间，不把转换器保留的未知状态当作无矛盾证明。"""
    if record['common']['kind'] not in {'prefix_outage', 'as_outage', 'hijack', 'sub_hijack', 'country_outage'}:
        return
    raw, zone = record['raw_fields'], record['data_profile']['timezone']
    start = _source_time(raw['s_time'], zone)
    end = _source_time(raw['e_time'], zone) if raw.get('e_time') not in (None, '') else None
    if end is not None and end < start:
        raise RecordTimeConflict('原始结束时间早于开始，不能准入首页消费')
    value = raw.get('duration')
    if value not in (None, ''):
        if isinstance(value, timedelta):
            duration = (value.days * 86400 + value.seconds) * 1000000 + value.microseconds
        else:
            # PostgreSQL interval 的天和时钟可带不同符号，不能简单看首字符。
            match = re.fullmatch(r'(?:(?P<days>[+-]?\d+) days?\s*)?(?:(?P<sign>[+-]?)(?P<hours>\d+):(?P<minutes>\d{2}):(?P<seconds>\d{2}(?:\.\d{1,6})?))?', value)
            if not match or (match['days'] is None and match['hours'] is None):
                raise ValueError('原始持续时间格式不可验证')
            seconds = Decimal(match['days'] or 0) * 86400
            if match['hours'] is not None:
                if int(match['minutes']) >= 60 or Decimal(match['seconds']) >= 60:
                    raise ValueError('原始持续时间分秒越界')
                part = Decimal(match['hours']) * 3600 + Decimal(match['minutes']) * 60 + Decimal(match['seconds'])
                seconds += -part if match['sign'] == '-' else part
            duration = int(seconds * 1000000)
        if duration < 0:
            raise RecordTimeConflict('原始持续时间为负，不能准入首页消费')
        if end is not None:
            delta = end - start
            expected = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
            if duration != expected:
                raise RecordTimeConflict('原始起止与持续时间矛盾，不能准入首页消费')


def load_legacy_input(configured):
    """直接查询仅接受64MiB原包；校验完成后才返回全部记录。"""
    manifest, records, version = _read_input(configured, 64 * 1024 * 1024)
    return manifest, list(records), version


def stream_index_input(configured):
    """仅供显式离线构建，单包至多2GiB／100万条；须迭代到EOF才完成准入。

    只保留单条正文、原引用集合及冲突引用的记录，不保存整个日窗正文。
    尾部摘要、计数或文件身份失败会抛InputError，调用方不得发布部分结果。
    Web不调用此入口；普通查询仍保持原包64MiB限制。
    """
    return _read_input(configured, 2 * 1024 * 1024 * 1024)


def _read_input(configured, maximum_bytes):
    if not configured:
        raise InputError('未配置首页留存输入')
    try:
        path = Path(configured)
        if path.stat().st_size > 65536:
            raise ValueError('清单过大')
        manifest_bytes = path.read_bytes()
        manifest = json.loads(manifest_bytes)
        if manifest['schema_version'] != 'core-overview-input/v1':
            raise ValueError('不支持的输入格式')
        if manifest['interpretation_version'] not in SUPPORTED_INTERPRETATIONS:
            raise ValueError('不支持的解释版本')
        profile = json.loads((Path(__file__).resolve().parents[3] / 'config/data-profile.json').read_text())
        if manifest['data_profile'] != profile:
            raise ValueError('数据档不一致')
        source = manifest['source']
        if (source['code'] != 'r' or source['collector_id'] != 'rrc25'
                or source['collector_basis'] != 'user_confirmation'
                or source['coverage'] != 'unknown' or source['detector_version'] is not None
                or manifest['window']['source'] != source['code']
                or set(manifest['kinds']) not in SUPPORTED_KIND_SETS):
            raise ValueError('声明超出当前已确认的历史记录用途')
        datetime.strptime(source['confirmed_on'], '%Y-%m-%d')
        window_start = datetime.fromisoformat(manifest['window']['start'])
        window_end = datetime.fromisoformat(manifest['window']['end_exclusive'])
        if (window_start.utcoffset() is None or window_end.utcoffset() is None
                or not datetime.fromisoformat(profile['window_start']) <= window_start < window_end <= datetime.fromisoformat(profile['window_end_exclusive'])):
            raise ValueError('留存窗口未绑定项目数据档')
        filename = manifest['records']['file']
        payload_path = path.parent / filename
        if Path(filename).name != filename or payload_path.resolve().parent != path.parent.resolve():
            raise ValueError('输入文件须位于清单目录内')
        if payload_path.stat().st_size > maximum_bytes:
            raise ValueError('输入超过本查询模块的有界范围')
        if type(manifest['records']['count']) is not int or not 0 <= manifest['records']['count'] <= 1000000:
            raise ValueError('留存记录数量超出有界范围')
        if manifest['interpretation_version'] == 'recorded-anomaly-overview/v1':
            if 'level_conflicts' in manifest:
                raise ValueError('旧解释版本不能携带等级冲突规则')
        elif not isinstance(manifest['level_conflicts'], dict):
            raise ValueError('等级冲突须按原引用唯一定位')
        records = _stream_records(payload_path, manifest, maximum_bytes)
        return manifest, records, 'overview_v1_' + hashlib.sha256(manifest_bytes).hexdigest()
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
        raise InputError('首页留存输入不可用或校验失败', 503) from error


def _stream_records(payload_path, manifest, maximum_bytes):
    try:
        profile, source = manifest['data_profile'], manifest['source']
        window_start = datetime.fromisoformat(manifest['window']['start'])
        window_end = datetime.fromisoformat(manifest['window']['end_exclusive'])
        before = payload_path.stat()
        identity = lambda stat: (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        digest, total_bytes = hashlib.sha256(), 0
        refs = set()
        conflict_records = []
        with payload_path.open('rb') as stream:
            while True:
                line = stream.readline(64 * 1024 * 1024 + 1)
                if not line:
                    break
                total_bytes += len(line)
                if len(line) > 64 * 1024 * 1024 or total_bytes > maximum_bytes:
                    raise ValueError('单条或输入正文超过有界范围')
                digest.update(line)
                result = deserialize_record(line)
                record = result['record']
                if record['mapping_version'] == 'legacy-anomaly-mapping/v2' and manifest['interpretation_version'] != 'recorded-anomaly-overview/v3':
                    raise ValueError('对象待核实记录必须显式绑定新的消费解释版本')
                ref = record['identity']['legacy_reference']
                if ref in refs or record['association']['state'] != 'matched':
                    raise ValueError('记录身份不唯一或关联未确认')
                refs.add(ref)
                if len(refs) > manifest['records']['count']:
                    raise ValueError('留存记录数量不一致')
                if (record['data_profile'] != profile
                        or record['source']['instance'] != source['instance']
                        or record['identity']['source_instance'] != source['instance']
                        or record['source']['collector_id'] not in (None, 'rrc25')
                        or ref.split('/')[-1] != source['code']
                        or record['source']['read_scope'] != manifest['window']
                        or record['common']['kind'] not in manifest['kinds']):
                    raise ValueError('记录与清单范围不一致')
                observed = datetime.fromisoformat(record['common']['start_time']['value'].replace('Z', '+00:00'))
                if record['common']['start_time']['state'] != 'recorded' or not window_start <= observed < window_end:
                    raise ValueError('记录时间未绑定留存窗口')
                overview_item(result)
                if ref in manifest.get('level_conflicts', {}):
                    conflict_records.append(result)
                yield result
        if total_bytes != before.st_size or identity(before) != identity(payload_path.stat()):
            raise ValueError('读取期间输入发生变化')
        if digest.hexdigest() != manifest['records']['sha256'] or len(refs) != manifest['records']['count']:
            raise ValueError('留存输入摘要或数量不一致')
        validate_level_conflicts(manifest, conflict_records)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
        raise InputError('首页留存输入不可用或校验失败', 503) from error


def overview_search_text(item):
    """索引与直接读取使用同一检索文本；不改变原对象或原字段。"""
    fields = [item['object'], item['record_number'], *('AS' + asn for asn in item['asns'])]
    if 'parent_prefix' in item:
        fields.append(item['parent_prefix'])
    if item.get('country_name'):
        fields.append(item['country_name'])
    return ' '.join(fields).lower()


def overview_item(result, level_conflicts=None):
    record = result['record']
    validate_record_time(record)
    common = record['common']
    if common['object']['kind'] == 'prefix':
        family = f'ipv{ipaddress.ip_network(common["object"]["value"], strict=True).version}'
    else:
        prefixes = record['raw_fields'].get('outage_prefixes')
        family = 'unknown'
        if isinstance(prefixes, list) and prefixes:
            try:
                if any(not isinstance(prefix, str) for prefix in prefixes):
                    raise ValueError('前缀不是文本')
                families = {ipaddress.ip_network(prefix, strict=True).version for prefix in prefixes}
                family = 'mixed' if len(families) == 2 else f'ipv{next(iter(families))}'
            except ValueError:
                pass
    asns = set()
    for field in ('asn', 'prefix_ori_as', 'leak_by', 'leak_to', 'hijacked_as', 'hijacker_as'):
        value = str(record['raw_fields'].get(field, ''))
        if re.fullmatch(r'[0-9]+', value) and 0 <= int(value) <= 4294967295:
            asns.add(str(int(value)))
    extra = {}
    if record['mapping_version'] == 'legacy-anomaly-mapping/v2' or common['object']['kind'] == 'unresolved_asn':
        raw = common['object']['value']
        if (record['mapping_version'] != 'legacy-anomaly-mapping/v2' or common['kind'] != 'as_outage'
                or common['object']['kind'] != 'unresolved_asn' or not is_unresolved_as_set_text(raw)
                or raw != record['raw_fields']['asn'] or raw != record['identity']['legacy_reference'].split('/')[2]
                or 'as_set_object_identity_unresolved' not in record['limitations'] or asns):
            raise ValueError('未确认对象身份与源原文或解释版本不一致')
        extra['object_identity'] = {'state': 'unresolved', 'reason': 'as_set_in_asn_field', 'label': '对象待核实'}
    if common['kind'] == 'country_outage':
        # 原数组仅用于记录检索，不推定前缀、固定人口或真实影响。
        values = record['raw_fields'].get('outage_ases')
        if values is not None:
            if not isinstance(values, list) or any(
                not isinstance(asn, str) or not re.fullmatch(r'[0-9]{1,10}', asn)
                or not 0 <= int(asn) <= 4294967295 for asn in values
            ):
                raise ValueError('国家中断ASN原数组含无效值')
            asns.update(str(int(asn)) for asn in values)
        name = record['raw_fields'].get('country_chinese_name')
        if name is not None and not isinstance(name, str):
            raise ValueError('国家原名称须为文本或空值')
        extra['country_name'] = name
        family = 'unknown'
    if common['kind'] == 'sub_hijack':
        parent = ipaddress.ip_network(record['raw_fields']['hijacked_prefix'], strict=True)
        child = ipaddress.ip_network(common['object']['value'], strict=True)
        if parent.version != child.version or parent == child or not child.subnet_of(parent):
            raise ValueError('子前缀劫持父子前缀须为同地址族严格包含关系')
        extra['parent_prefix'] = str(parent)
        for field in ('hijacked_as', 'hijacker_as'):
            value = record['raw_fields'].get(field)
            if value in (None, ''):
                continue
            if not isinstance(value, str) or len(value) > 4096:
                raise ValueError('子前缀劫持角色ASN须为有界列表文本')
            try:
                values = ast.literal_eval(value)
            except (ValueError, SyntaxError, RecursionError) as error:
                raise ValueError('子前缀劫持角色ASN列表不可解析') from error
            if not isinstance(values, list) or any(
                not isinstance(asn, str) or not re.fullmatch(r'[0-9]+', asn)
                or not 0 <= int(asn) <= 4294967295 for asn in values
            ):
                raise ValueError('子前缀劫持角色ASN列表含无效值')
            asns.update(str(int(asn)) for asn in values)
    conflict = (level_conflicts or {}).get(record['identity']['legacy_reference'])
    if conflict is not None:
        extra['level_conflict'] = conflict
    return {
        **extra,
        'reference': record['identity']['legacy_reference'],
        'content_version': result['content_version'],
        'kind': common['kind'],
        'object': common['object']['value'],
        'start_time': common['start_time']['value'],
        'end_time': common['end_time'],
        'level': common['level']['value'] if conflict is None and common['level']['value'] in {'high', 'middle', 'low'} else None,
        'address_family': family,
        'asns': sorted(asns, key=int),
        'record_number': record['identity']['legacy_reference'].split('/')[3],
    }
