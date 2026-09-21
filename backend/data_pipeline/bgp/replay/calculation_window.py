"""Canonical 显式计算窗的有限输入资格；不裁剪观察、不定义另一条回放路径。"""
from datetime import datetime, timezone
import json

WINDOW_RULE = 'canonical-selected-update-window/v1'
WINDOW_FIELDS = {'window_start', 'window_end_exclusive'}


def _utc(value):
    if type(value) is not str:
        raise ValueError('计算窗端点必须为 UTC 秒级字符串')
    try:
        result = datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError('计算窗端点必须明确 UTC：YYYY-MM-DDTHH:MM:SSZ') from None
    if result.strftime('%Y-%m-%dT%H:%M:%SZ') != value:
        raise ValueError('计算窗端点必须使用规范 UTC 秒级表示')
    return result


def calculation_window(value, manifest=None):
    if type(value) is not dict or set(value) != WINDOW_FIELDS:
        raise ValueError('计算窗字段必须恰为 window_start/window_end_exclusive')
    start, end = (_utc(value[k]) for k in ('window_start', 'window_end_exclusive'))
    if start >= end:
        raise ValueError('计算窗必须为非空半开区间 [start,end)')
    if manifest is not None:
        bounds = [datetime.fromisoformat(manifest[k].replace('Z', '+00:00'))
                  for k in ('window_start', 'window_end_exclusive')]
        if any(v.tzinfo is None for v in bounds) or not bounds[0] <= start < end <= bounds[1]:
            raise ValueError('显式计算窗必须位于原 M2 名义包络内，不能重写包络')
    return dict(value)


def window_from_plan(plan):
    present = {'calculation_window', 'calculation_window_rule'} & set(plan)
    if not present: return None  # 旧计划完全沿原路径；不补字段或改签。
    if present != {'calculation_window', 'calculation_window_rule'} or plan['calculation_window_rule'] != WINDOW_RULE:
        raise ValueError('显式计算窗规则缺失或未知')
    return calculation_window(plan['calculation_window'], plan['input_manifest'])


def epoch_bounds(window):
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return tuple((_utc(window[k]) - epoch).days * 86400 + (_utc(window[k]) - epoch).seconds
                 for k in ('window_start', 'window_end_exclusive'))


def check_update_time(item, bounds):
    """按 ordered RawTime 判定 UPDATE 消息（含 LOCAL/STATE/Gap），绝不使用文件名。"""
    epoch, micro = item.raw_time.epoch, item.raw_time.microsecond
    kind = item.raw.get('mrt_type')
    reason = None
    if type(epoch) is not int or epoch < 0:
        reason = 'raw_time_epoch_unknown'
    elif kind == 17 and (type(micro) is not int or not 0 <= micro <= 999999):
        reason = 'raw_time_et_precision_unknown'
    elif kind == 16 and micro is not None:
        reason = 'raw_time_non_et_precision_conflict'
    elif kind not in (16, 17):
        reason = 'selected_update_frame_type_invalid'
    elif not bounds[0] <= epoch < bounds[1]:
        reason = 'update_outside_calculation_window'
    if reason:
        evidence = dict(reason=reason, source_id=item.raw['source_id'], source_rank=item.position.source_rank,
                        record=item.position.record, raw_time=dict(epoch=epoch, microsecond=micro))
        raise ValueError('canonical 计算窗资格失败：' + json.dumps(evidence, ensure_ascii=False, sort_keys=True))
