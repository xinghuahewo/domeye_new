"""已交付事件的公开语义投影，供列表与详情共用。

历史 anomaly-record 及其内容摘要原样保留。这里直接解释当前交付数据的
生命周期与结构化国家事件，不解析摘要、不查文件、不写回数据库。
"""
from copy import deepcopy
from datetime import datetime, timezone
from math import isclose, isfinite

from data_pipeline.overview.input import InputError


COUNTRY_FIELDS = (
    'structured_v2', 'structured_incident', 'peak_snapshot_id',
    'max_outage_as_num', 'total_as_num', 'max_outage_as_ratio', 'outage_ases',
)


def _time(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('语义时间必须带时区')
    return result.astimezone(timezone.utc)


def _utc(value):
    return _time(value).isoformat().replace('+00:00', 'Z')


def _country_incident(item, data, intervals):
    """只接受同一峰值快照的时间、名单、分子和分母，不复算检测。"""
    incident = data.get('structured_incident')
    if incident is None and not data.get('structured_v2'):
        return None
    try:
        if (incident['schema_version'] != 'country-outage-incident/v2'
                or incident['algorithm_version'] != 'country_outage_live_event_model_v2'
                or incident['country_code'] != item['object']
                or any(not isinstance(incident[name], str) or not incident[name]
                       for name in ('incident_id', 'cohort_id'))
                or _time(incident['detected_at']) != _time(item['start_time'])):
            raise ValueError('国家事件身份或模型不一致')
        onset, peak, observed = (_time(incident[name]) for name in ('onset_at', 'peak_at', 'observation_end_at'))
        if not (onset <= _time(incident['detected_at']) <= observed and onset <= peak <= observed
                and any(left <= observed < right for left, right in intervals)):
            raise ValueError('国家事件时间不在已交付观察范围内')
        milestone = incident['milestones']['peak']
        for field in ('trough_at', 'partial_recovery_at'):
            if incident.get(field) is not None:
                _time(incident[field])
        if 'time_precision' in milestone and not isinstance(milestone['time_precision'], str):
            raise ValueError('峰值时间精度字段无效')
        snapshot = incident['peak_snapshot_id']
        if (not isinstance(snapshot, str) or not snapshot
                or snapshot != data['peak_snapshot_id'] or snapshot != milestone['snapshot_id']
                or peak != _time(milestone['at']) or milestone['metric'] != 'affected_asn_ratio'):
            raise ValueError('国家峰值时间与快照不一致')
        count, total, ratio = (data[name] for name in ('max_outage_as_num', 'total_as_num', 'max_outage_as_ratio'))
        members = data['outage_ases']
        if (type(count) is not int or type(total) is not int or not 0 <= count <= total or total == 0
                or not isinstance(members, list) or len(members) != count
                or any(not isinstance(asn, str) or not asn.isascii() or not asn.isdecimal()
                       or not 0 <= int(asn) <= 4294967295 for asn in members)
                or len(set(item['asns'])) != count
                or {str(int(asn)) for asn in members} != set(item['asns'])
                or type(ratio) not in (int, float) or not isfinite(ratio)
                or not isclose(ratio, count / total, rel_tol=0, abs_tol=1e-12)
                or type(milestone['metric_value']) not in (int, float)
                or not isclose(ratio, milestone['metric_value'], rel_tol=0, abs_tol=1e-12)):
            raise ValueError('国家峰值名单或数量不一致')
        recovery = incident['recovery_state']
        if recovery == 'fully_recovered':
            recovered_at = _time(incident['full_recovery_at'])
            if (incident['duration_state'] != 'exact' or not onset <= recovered_at <= observed
                    or item['end_time']['state'] != 'recorded'
                    or recovered_at != _time(item['end_time']['value'])):
                raise ValueError('国家事件结束与恢复记录不一致')
        elif (recovery != 'unknown' or incident['duration_state'] != 'lower_bound'
              or incident['full_recovery_at'] is not None or item['end_time']['state'] == 'recorded'):
            raise ValueError('国家事件持续状态不一致')
        return {**deepcopy(incident), 'asn_membership': {
            'basis': 'peak_snapshot', 'count': count, 'total': total, 'ratio': ratio,
        }}
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError) as error:
        raise InputError('国家结构化事件与峰值字段不一致，不能确定其状态或成员口径', 503) from error


def event_item(item, *, null_end_time, country_fields, delivery):
    """扩展公开 item；内容版本仍标识原 anomaly-record，读取版本由响应绑定。"""
    end = _utc(delivery['end_exclusive'])
    intervals = [(_time(value['start']), _time(value['end_exclusive'])) for value in delivery['intervals']]
    lifecycle = {'state': 'unknown', 'basis': 'unknown', 'data_end_exclusive': end, 'observed_at': None}
    result = {**item, 'lifecycle': lifecycle}
    if item['kind'] in ('as_outage', 'prefix_outage'):
        if item['end_time']['state'] == 'recorded':
            lifecycle.update(state='ended', basis='detector_end_time')
        elif null_end_time and any(left <= _time(item['start_time']) < right == _time(end)
                                   for left, right in intervals):
            # 当前完成文件保留检测器的显式 NULL：未触发结束。不能跨处理缺口延续。
            lifecycle.update(state='ongoing', basis='detector_end_time')
    elif item['kind'] == 'country_outage':
        incident = _country_incident(item, country_fields or {}, intervals)
        if incident:
            result['country_incident'] = incident
            lifecycle.update(state='ended' if incident['recovery_state'] == 'fully_recovered' else 'ongoing',
                             basis='country_incident', observed_at=incident['observation_end_at'])
    elif item['kind'] == 'leak':
        lifecycle.update(state='unavailable', basis='unsupported')
    return result
