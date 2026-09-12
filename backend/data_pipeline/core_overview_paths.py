"""只读、有界的两次RIB观察摘要；请求不解析MRT或生产比较结果。"""
from datetime import datetime
import hashlib
import ipaddress
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo


RULE = 'rrc25-raw-peer-rib-endpoint/as-sequence-v1'
STATUSES = {'same', 'different', 'not_comparable', 'left_only', 'right_only'}


def _require(value):
    if not value:
        raise ValueError('路径对照消费包校验失败')


def _object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value)


def _read(folder, entry, filename):
    _require(entry['file'] == filename and _sha(entry['sha256']))
    path = folder / filename
    _require(path.resolve() == folder.resolve() / filename and path.is_file())
    with path.open('rb') as stream:
        payload = stream.read(65537)
    _require(len(payload) <= 65536 and hashlib.sha256(payload).hexdigest() == entry['sha256'])
    return json.loads(payload, object_pairs_hook=_object)


def read_comparison(index_path, index_manifest):
    """离线绑定与HTTP共用摘要验证，完整源证据只由离线命令核验。"""
    folder = Path(index_path).parent
    binding = index_manifest['path_comparison']
    package = _read(folder, binding, 'paths/manifest.json')
    _require(package['schema_version'] == 'core-rib-path-package/v1'
             and _sha(package['source_manifest_sha256']) and _sha(package['binding_code_sha256']))
    summary = _read(folder / 'paths', package['summary'], 'summary.json')
    _require(summary['schema_version'] == 'core-rib-path-summary/v1'
             and summary['interpretation_version'] == RULE
             and summary['comparison_version'] == 'rib_path_comparison_v1_' + package['source_manifest_sha256']
             and summary['data_profile'] == index_manifest['data_profile']
             and summary['collector_id'] == index_manifest['source']['collector_id'] == 'rrc25'
             and summary['coverage'] == summary['session_continuity'] == 'unknown'
             and summary['interval_change_count'] is None)
    profile = summary['data_profile']
    times = []
    for side in ('left', 'right'):
        source = summary[side]
        _require(_sha(source['sha256']) and isinstance(source['observed_at'], str)
                 and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', source['observed_at']))
        times.append(datetime.fromisoformat(source['observed_at'].replace('Z', '+00:00')))
    _require(datetime.fromisoformat(profile['window_start']) <= times[0] < times[1]
             < datetime.fromisoformat(profile['window_end_exclusive'])
             and times[1] <= datetime.fromisoformat(profile['snapshot_time']))
    days = [stamp.astimezone(ZoneInfo(profile['timezone'])).date().isoformat() for stamp in times]
    _require(days[0] == days[1])
    families = summary['families']
    _require(set(families) == {'all', 'ipv4', 'ipv6'})
    for metric in families.values():
        _require(set(metric) == STATUSES and all(type(n) is int and 0 <= n <= 160_000_000 for n in metric.values()))
    _require(all(families['all'][key] == families['ipv4'][key] + families['ipv6'][key] for key in STATUSES))
    _require(isinstance(summary['examples'], list) and len(summary['examples']) <= 10)
    seen = set()
    for example in summary['examples']:
        family = example['family']
        _require(family in {'ipv4', 'ipv6'})
        network = ipaddress.ip_network(example['prefix'], strict=True)
        peer = example['peer']
        _require(network.version == (4 if family == 'ipv4' else 6) and str(network) == example['prefix']
                 and isinstance(peer['bgp_id'], str) and isinstance(peer['ip'], str)
                 and ipaddress.ip_address(peer['bgp_id']).version == 4
                 and ipaddress.ip_address(peer['ip']).version in (4, 6)
                 and type(peer['asn']) is int and 0 <= peer['asn'] <= 4294967295)
        for side in ('left', 'right'):
            path = example[side + '_path']
            ref = example[side + '_reference']
            _require(isinstance(path, list) and 1 <= len(path) <= 256
                     and all(type(asn) is int and 0 <= asn <= 4294967295 for asn in path)
                     and set(ref) == {'record', 'offset', 'entry', 'peer_index'}
                     and all(type(n) is int and 0 <= n <= 12 * 1024**3 for n in ref.values()))
        key = (family, example['prefix'], peer['bgp_id'], peer['ip'], peer['asn'])
        _require(example['left_path'] != example['right_path'] and key not in seen)
        seen.add(key)
    for family in ('ipv4', 'ipv6'):
        _require(sum(example['family'] == family for example in summary['examples']) <= min(5, families[family]['different']))
    _require(isinstance(summary['limits'], list) and 1 <= len(summary['limits']) <= 10
             and all(isinstance(text, str) and 0 < len(text) <= 1000 for text in summary['limits']))
    return summary, 'rib_path_consumption_v1_' + binding['sha256'], days[0]


def attach_comparison(response, index_path, index_manifest):
    if 'path_comparison' not in index_manifest or response['state'] != 'available':
        return response
    try:
        summary, version, day = read_comparison(index_path, index_manifest)
        family = response['query']['family']
        state = 'date_not_retained' if response['query']['date'] != day else 'family_not_supported' if family == 'unknown' else 'available'
        comparison = {key: summary[key] for key in ('comparison_version', 'interpretation_version', 'collector_id',
                      'coverage', 'session_continuity', 'interval_change_count', 'left', 'right', 'limits')}
        comparison.update(state=state, version=version, available_dates=[day], family=family,
                          unit='raw_peer_afi_safi_prefix', metrics=None, examples=[])
        if state == 'available':
            metric = summary['families'][family]
            total = metric['same'] + metric['different']
            comparison['metrics'] = {**metric, 'comparable_pairs': total,
                                      'different_fraction': metric['different'] / total if total else None}
            comparison['examples'] = [example for example in summary['examples'] if family == 'all' or example['family'] == family]
        response['metadata']['path_comparison'] = comparison
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError, RuntimeError):
        response['metadata']['path_comparison'] = {'state': 'unavailable', 'metrics': None, 'examples': [],
            'message': '路径对照消费包缺失或校验失败；异常记录和规模查询不受影响。'}
    return response
