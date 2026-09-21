"""显式结果窗与真实消息时间门禁；不裁剪计算流或改变原事件算法。"""
from datetime import datetime, timezone

RULE = 'detection-result-window/v1'
FIELDS = {'window_start', 'window_end_exclusive'}


def instant(value):
    if type(value) is not str: raise ValueError('窗口端点必须明确时区')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None: raise ValueError('窗口端点必须明确时区')
    return result.astimezone(timezone.utc)


def windows(scope, result=None, envelope=None):
    calculation = dict(window_start=scope['window_start'], window_end_exclusive=scope['window_end'])
    bounds = tuple(instant(calculation[k]) for k in ('window_start','window_end_exclusive'))
    if not bounds[0] < bounds[1]: raise ValueError('计算窗必须为非空半开区间')
    if envelope is not None:
        if not instant(envelope['window_start']) <= bounds[0] < bounds[1] <= instant(envelope['window_end_exclusive']):
            raise ValueError('计算窗必须位于原M2包络，不能替换输入身份')
    if result is None:
        if any(x.microsecond for x in bounds): raise ValueError('结果窗端点使用UTC秒级边界')
        result = {k:v.strftime('%Y-%m-%dT%H:%M:%SZ') for k,v in zip(('window_start','window_end_exclusive'),bounds)}
    if type(result) is not dict or set(result) != FIELDS: raise ValueError('结果窗字段必须为window_start/window_end_exclusive')
    selected = tuple(instant(result[k]) for k in ('window_start','window_end_exclusive'))
    if any(value.strftime('%Y-%m-%dT%H:%M:%SZ') != result[key] for key,value in zip(('window_start','window_end_exclusive'),selected)):
        raise ValueError('显式结果窗必须为规范UTC秒级字符串')
    if not bounds[0] <= selected[0] < selected[1] <= bounds[1]: raise ValueError('结果窗必须是计算窗内非空半开区间')
    return dict(calculation_window=calculation, result_window=dict(result))


def check_message(raw, calculation):
    """与共用 RawTime 一致，UPDATE/LOCAL/STATE/Gap 都核验；基线由角色另行豁免。"""
    epoch, micro, kind = raw.get('epoch'), raw.get('microsecond'), raw.get('mrt_type')
    if type(epoch) is not int or epoch < 0: raise ValueError('UPDATE RawTime epoch未知')
    if kind not in (16,17): raise ValueError('UPDATE来源出现非BGP4MP消息')
    if kind == 17 and (type(micro) is not int or not 0 <= micro <= 999999): raise ValueError('ET RawTime微秒未知')
    if kind == 16 and micro is not None: raise ValueError('非ET RawTime微秒冲突')
    time = datetime.fromtimestamp(epoch, timezone.utc).replace(microsecond=micro or 0)
    if not instant(calculation['window_start']) <= time < instant(calculation['window_end_exclusive']):
        raise ValueError('实际消息RawTime在计算窗之外：'+str(raw.get('message_id')))
    return time.isoformat()


def identity_windows(identity, scope):
    present = {'result_window','result_window_rule','window_coverage'} & identity.keys()
    if not present: raise ValueError('旧制品没有结果窗生产证明，仍使用原完整读取')
    if present != {'result_window','result_window_rule','window_coverage'} or identity['result_window_rule'] != RULE:
        raise ValueError('结果窗身份/证明不完整')
    return windows(scope, identity['result_window'])


def time_coverage(selected, sources):
    return dict(rule=RULE,baseline='complete_selected_baseline_cutover_assumed',earlier_history='Unknown',
                update_messages=0,first_raw_time=None,last_raw_time=None,
                result_messages=0,result_first_raw_time=None,result_last_raw_time=None,
                source_result_messages={s:0 for s in selected},
                selected_source_ranks=[dict(source_id=s,upstream_rank=i,role=source['role']) for i,source in enumerate(sources) for s in selected if s==source['source_id']])


def observe_message(raw, binding, coverage):
    at=check_message(raw,binding['calculation_window']);coverage['update_messages']+=1
    if coverage['first_raw_time'] is None:coverage['first_raw_time']=at
    coverage['last_raw_time']=at
    result=binding['result_window']
    if instant(result['window_start'])<=instant(at)<instant(result['window_end_exclusive']):
        coverage['result_messages']+=1;coverage['source_result_messages'][raw['source_id']]+=1
        if coverage['result_first_raw_time'] is None:coverage['result_first_raw_time']=at
        coverage['result_last_raw_time']=at
