"""M3 公开原行到既有 C2 保存态输入的有限接合，不重放 action。"""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import json

from data_pipeline.bgp.replay.quality_overlay import position_key
from data_pipeline.bgp.replay.snapshot_validation import validate_row
from data_pipeline.analysis.country_events.models import Cursor, Time
from data_pipeline.analysis.country_events.saved_input import SavedDelta, SavedInvalidation, CountryRevision, legacy_time


def saved_change(binding, canonical_row, message, element, path):
    """四行均须来自同一固定公开输入；保留Canonical原before/after全文。"""
    validate_row('changes', canonical_row)
    row = canonical_row
    raw = row['raw']
    pos = position_key(row['position'])
    if (pos[2] != 1 or binding.source_at(pos[0]) != row['source_id']
            or row['source_id'] != message['source_id']
            or row['message_id'] != message['message_id']
            or row['message_id'] != element['message_id']
            or row['event_id'] != element['event_id']
            or pos[1] != message['record'] or pos[3] != element['ordinal']
            or element['path_key'] != path['path_key']):
        raise ValueError('M3 保存态原消息/元素/路径/全局位置不符')
    if message['local_message']:
        raise ValueError('M3 LOCAL 原事实不能冒充Canonical received变化')
    after = raw['calculation_after']
    if after['event_id'] != element['event_id'] or after['epoch'] != message['epoch']:
        raise ValueError('M3 Canonical 后态的原观察引用不符')
    scope = raw['scope']
    if (scope[0] != binding.collector or scope[1:3] != (element['peer_ip'], element['peer_asn'])
            or (scope[6], scope[7], scope[8], scope[9], scope[10]) !=
            (element['afi'], element['safi'], element['prefix'], element['path_id_present'], element['path_id'])):
        raise ValueError('M3 规范对象与原元素槽不符')
    if after['presence'] == 'present' and (after['path_key'], after['origin']) != (path['path_key'], path['attributed_origin_asn']):
        raise ValueError('M3 后态路径/显式起源与原路径不符')
    return SavedDelta(Cursor(pos[0], pos[1], pos[3]), Time(message['epoch'], message['microsecond']),
                      row['source_id'], row['event_id'], row['object_key'], after['presence'],
                      after['path_key'], after['origin'], deepcopy(raw))


def saved_invalidation(binding, canonical_row, message):
    """仅承接真实STATE失效；Gap继续作为独立范围资格，不伪装withdraw。"""
    validate_row('invalidations', canonical_row)
    row = canonical_row
    pos = position_key(row['position'])
    if (pos[2] != 0 or binding.source_at(pos[0]) != row['source_id']
            or row['source_id'] != message['source_id'] or row['message_id'] != message['message_id']
            or pos[1] != message['record'] or message['kind'] != 'state_change'
            or message['new_state'] == 6 or row['raw']['epoch'] != message['epoch']):
        raise ValueError('M3 STATE原消息/位置不符')
    return SavedInvalidation(pos[0], pos[1], Time(message['epoch'], message['microsecond']),
                             row['message_id'], row['object_key'], deepcopy(row['raw']))


def saved_country_revision(binding, record, message, element, *, legacy_timezone):
    """保留原国家修订与触发证据；不执行D选择，也不从触发位置伪造onset/carry-in。"""
    if (record['record_kind'] != 'business_revision' or record['subject_type'] != 'country'
            or record['event_kind'] != 'country_outage'):
        raise ValueError('M3 必须显式选择原国家业务修订')
    original = deepcopy(record)
    original.update(attributes=json.loads(record['attributes_json']),
                    evidence=json.loads(record['evidence_json']), legacy=json.loads(record['legacy_json']))
    attributes = original['attributes']
    observation = original['evidence'].get('observation', {})
    if (attributes.get('incident_id'), attributes.get('revision'), attributes.get('object')) != (
            record['incident_id'], record['revision'], record['subject_key']):
        raise ValueError('M3 原国家修订索引与正文不符')
    snapshot = f'{binding.run_id}:{binding.snapshot_id}'
    if (attributes.get('reference_version') != binding.reference_version
            or observation.get('snapshot_ref') != snapshot or observation.get('source_version') != snapshot
            or observation.get('collector_id') != binding.collector
            or observation.get('observation_id') != element['event_id']
            or observation.get('message_ref') != message['message_id']
            or message['message_id'] != element['message_id']
            or observation.get('element_ordinal') != element['ordinal']
            or observation.get('source_id') != message['source_id']
            or observation.get('path_ref') != element['path_key']):
        raise ValueError('M3 原国家修订触发引用/输入绑定不符')
    for field in ('epoch', 'microsecond'):
        if observation.get(field) != message[field]:
            raise ValueError('M3 原国家修订触发时间不符')
    for field in ('path_id', 'path_id_present'):
        if observation.get(field) != element[field]:
            raise ValueError('M3 原国家修订路径槽不符')
    at = Time(message['epoch'], message['microsecond'])
    expected = datetime.fromtimestamp(at.epoch, timezone.utc) + timedelta(microseconds=at.microsecond or 0)
    observed = datetime.fromisoformat(observation['observed_at'])
    if observed.tzinfo is None or observed != expected or record['observed_at'] != expected:
        raise ValueError('M3 原国家修订已存UTC时间不符')
    cursor = Cursor(binding.rank_of(message['source_id']), message['record'], element['ordinal'])
    return CountryRevision(record['incident_id'], record['revision'], record['subject_key'], cursor, at,
                           legacy_time(original['legacy'].get('s_time'), legacy_timezone),
                           legacy_time(original['legacy'].get('e_time'), legacy_timezone), original)
