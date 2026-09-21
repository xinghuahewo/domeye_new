"""读取明确绑定的单RIB消费摘要；不读MRT、不重建状态、不执行证据代码。"""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo


EVIDENCE_NAMES = {'candidate-0800-proof.json', 'candidate-0800-frames.json', 'candidate-0800-receipt.json',
                  'candidate-0800-executed.py', 'candidate_frame_census.py'}


def _require(condition):
    if not condition:
        raise ValueError('规模消费包校验失败')


def _object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value)


def _natural(value, maximum):
    return type(value) is int and 0 <= value <= maximum


def _read(folder, entry, filename, limit=65536):
    _require(entry['file'] == filename and _sha(entry['sha256']))
    path = folder / filename
    _require(path.resolve() == folder.resolve() / filename and path.is_file() and path.stat().st_size <= limit)
    with path.open('rb') as stream:
        payload = stream.read(limit + 1)
    _require(len(payload) <= limit and hashlib.sha256(payload).hexdigest() == entry['sha256'])
    return payload


def read_scale(index_path, index_manifest):
    """离线绑定和只读请求共用的消费校验；异常由调用方限制在规模模块内。"""
    binding = index_manifest['scale']
    payload = _read(Path(index_path).parent, binding, 'scale/manifest.json')
    manifest = json.loads(payload, object_pairs_hook=_object)
    _require(manifest['schema_version'] == 'core-rib-scale-manifest/v1' and _sha(manifest['retainer_sha256']))
    folder = Path(index_path).parent / 'scale'
    summary = json.loads(_read(folder, manifest['summary'], 'summary.json'), object_pairs_hook=_object)
    _require(set(manifest['evidence']) == EVIDENCE_NAMES)
    for name, entry in manifest['evidence'].items():
        # 请求只校验摘要中的绑定；证据文件的完整复核由离线绑定命令执行。
        _require(entry['file'] == 'evidence/' + name and _sha(entry['sha256']))
    profile = index_manifest['data_profile']
    source = summary['source']
    _require(summary['schema_version'] == 'core-rib-scale/v1' and summary['interpretation_version'] == 'rib-prefix-union/v1'
             and summary['data_profile'] == profile and summary['origin_metric_state'] == 'pending_definition'
             and source['collector_id'] == index_manifest['source']['collector_id'] == source['view_name'] == 'rrc25'
             and source['collector_bgp_id'] == '0.0.0.25' and source['coverage'] == 'unknown'
             and _sha(source['sha256']) and isinstance(source['path'], str) and 0 < len(source['path']) <= 4096
             and _natural(source['compressed_bytes'], 512 * 1024 * 1024) and source['compressed_bytes'] > 0)
    _require(isinstance(summary['observed_at'], str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', summary['observed_at']))
    stamp = datetime.fromisoformat(summary['observed_at'].replace('Z', '+00:00'))
    _require(datetime.fromisoformat(profile['window_start']) <= stamp < datetime.fromisoformat(profile['window_end_exclusive'])
             and stamp <= datetime.fromisoformat(profile['snapshot_time']))
    families = summary['families']
    _require(set(families) == {'ipv4', 'ipv6', 'all'})
    for family, metric in families.items():
        peers = metric['peer_indices']
        _require(_natural(metric['visible_prefixes'], 4_000_000) and _natural(metric['rib_entries'], 160_000_000)
                 and metric['rib_entries'] >= metric['visible_prefixes'] and metric['visible_origin_ases'] is None
                 and isinstance(peers, list) and len(peers) <= 65535
                 and all(_natural(peer, 65534) for peer in peers) and peers == sorted(set(peers))
                 and (metric['visible_prefixes'] == 0) == (len(peers) == 0)
                 and (family == 'all' or _sha(metric['prefix_set_sha256'])))
    _require(all(families['all'][key] == families['ipv4'][key] + families['ipv6'][key] for key in ('visible_prefixes', 'rib_entries'))
             and families['all']['peer_indices'] == sorted(set(families['ipv4']['peer_indices']) | set(families['ipv6']['peer_indices'])))
    _require(isinstance(summary['limits'], list) and 1 <= len(summary['limits']) <= 10
             and all(isinstance(limit, str) and 0 < len(limit) <= 1000 for limit in summary['limits']))
    return summary, 'rib_scale_v1_' + binding['sha256']


def attach_scale(response, index_path, index_manifest):
    if 'scale' not in index_manifest or response['state'] != 'available':
        return response
    try:
        summary, version = read_scale(index_path, index_manifest)
        day = datetime.fromisoformat(summary['observed_at'].replace('Z', '+00:00')).astimezone(ZoneInfo(summary['data_profile']['timezone'])).date().isoformat()
        family = response['query']['family']
        state = 'date_not_retained' if response['query']['date'] != day else 'family_not_supported' if family == 'unknown' else 'available'
        scale = {'state': state, 'version': version, 'observed_at': summary['observed_at'], 'available_dates': [day],
                 'family': family, 'source': summary['source'], 'interpretation_version': summary['interpretation_version'],
                 'origin_metric_state': summary['origin_metric_state'], 'limits': summary['limits']}
        if state == 'available':
            metric = summary['families'][family]
            response['overview']['visible_prefixes'] = metric['visible_prefixes']
            scale['peer_position_count'] = len(metric['peer_indices'])
            if family != 'all':
                scale['prefix_set_sha256'] = metric['prefix_set_sha256']
        response['metadata']['scale'] = scale
        if 'origin' in index_manifest:
            try:
                origin, origin_version = read_origin(index_path, index_manifest, summary)
                scale['origin_metric_state'] = state
                scale['origin'] = {'version': origin_version, 'interpretation_version': origin['interpretation_version'],
                                   'unattributed_entries': origin['families'][family]['unattributed_entries'] if state == 'available' else None,
                                   'limits': origin['limits']}
                if state == 'available':
                    response['overview']['visible_origin_ases'] = origin['families'][family]['visible_origin_ases']
            except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError, RuntimeError):
                scale['origin_metric_state'] = 'unavailable'
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError, RuntimeError):
        response['metadata']['scale'] = {'state': 'unavailable', 'message': '规模消费包缺失或校验失败；异常记录查询不受影响。'}
    return response


def read_origin(index_path, index_manifest, prefix_summary):
    """只读两个有界摘要；完整路径目录与ASN集合只在离线绑定阶段核验。"""
    binding = index_manifest['origin']
    manifest = json.loads(_read(Path(index_path).parent, binding, 'origin/manifest.json'), object_pairs_hook=_object)
    _require(manifest['schema_version'] == 'core-rib-origin-manifest/v1'
             and _sha(manifest['producer_sha256']) and _sha(manifest['cli_sha256']))
    _require(set(manifest['evidence']) == {'paths.sqlite', 'origins.json', 'peers.json'})
    for name, evidence in manifest['evidence'].items():
        _require(evidence['file'] == name and _sha(evidence['sha256']) and _natural(evidence['bytes'], 4 * 1024 ** 3))
    folder = Path(index_path).parent / 'origin'
    summary = json.loads(_read(folder, manifest['summary'], 'summary.json'), object_pairs_hook=_object)
    _require(summary['schema_version'] == 'core-rib-origin/v1'
             and summary['interpretation_version'] == 'rib-attributed-origin/private-skip-v1'
             and summary['data_profile'] == prefix_summary['data_profile']
             and summary['observed_at'] == prefix_summary['observed_at']
             and summary['gzip_eof'] is True and _natural(summary['distinct_paths'], 80_000_000))
    for key in ('collector_id', 'coverage', 'sha256', 'compressed_bytes'):
        _require(summary['source'][key] == prefix_summary['source'][key])
    rules = summary['rules']
    _require(rules['skip_ranges_inclusive'] == [[64512, 65535], [4200000000, 4294967294]]
             and rules['reserved_legacy_exclusion'] == [65535] and rules['stop_special_asns'] == [0, 23456, 4294967295])
    families = summary['families']
    _require(set(families) == {'ipv4', 'ipv6', 'all'})
    for name, metric in families.items():
        original = prefix_summary['families'][name]
        _require(_natural(metric['visible_prefixes'], 4_000_000) and _natural(metric['rib_entries'], 80_000_000)
                 and metric['visible_prefixes'] == original['visible_prefixes'] and metric['rib_entries'] == original['rib_entries']
                 and _natural(metric['visible_origin_ases'], 4_000_000) and _natural(metric['unattributed_entries'], metric['rib_entries'])
                 and metric['visible_origin_ases'] <= metric['rib_entries'] - metric['unattributed_entries'])
        if name != 'all':
            peers = metric['peer_entry_counts']
            _require(metric['prefix_set_sha256'] == original['prefix_set_sha256'] and isinstance(peers, dict)
                     and list(sorted(int(key) for key in peers)) == original['peer_indices']
                     and all(isinstance(key, str) and str(int(key)) == key and _natural(value, 80_000_000) and value > 0 for key, value in peers.items())
                     and sum(peers.values()) == metric['rib_entries'])
    union = families['all']['visible_origin_ases']
    _require(max(families['ipv4']['visible_origin_ases'], families['ipv6']['visible_origin_ases']) <= union
             <= families['ipv4']['visible_origin_ases'] + families['ipv6']['visible_origin_ases']
             and families['all']['unattributed_entries'] == families['ipv4']['unattributed_entries'] + families['ipv6']['unattributed_entries'])
    _require(isinstance(summary['limits'], list) and 1 <= len(summary['limits']) <= 10
             and all(isinstance(limit, str) and 0 < len(limit) <= 1000 for limit in summary['limits']))
    return summary, 'rib_origin_v1_' + binding['sha256']
