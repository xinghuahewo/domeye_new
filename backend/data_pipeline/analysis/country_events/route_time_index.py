"""对象相关的原处理位置/原时间边界索引；不以末态或时间排序重排原流。"""
from bisect import bisect_left

from data_pipeline.analysis.country_events.models import Time
from data_pipeline.bgp.replay.quality_overlay import position_key
from data_pipeline.bgp.replay.route_replay import identity
from data_pipeline.bgp.replay.snapshot_contract import encode


def state_before(history, messages, scope, sample_us):
    """查询样本左极限。时序回退/精度区间不可定位时返回Unknown。

    每个对象的索引最多构造一次；范围Gap匹配沿用ScopeIndex。所有对象共用
    原history行/字节/候选及时间查询预算，LOCAL不进入received对象的时间线。
    """
    if history.store is not None:
        from data_pipeline.analysis.country_events.history_staging import state_before as disk_before
        return disk_before(history,messages,scope,sample_us)
    history.check_budget()
    history.validate_scope(scope)
    if not history.sealed or type(sample_us) is not int:
        raise ValueError('M3 时间查询缺完整历史或精确样本边界')
    history.stats['time_queries'] += 1
    if not hasattr(history, '_time_indexes'):history._time_indexes = {}
    obj = identity(scope)
    if obj not in history._time_indexes:
        candidates = len(history.index.gaps)
        history.stats['gap_candidates'] += candidates
        matched = history.index.matching(scope)
        incoming = len(history.objects.get(obj, ())) + len(matched)
        if history.stats['rows'] + history.stats['temporal_rows'] + incoming > history.max_rows:
            raise ValueError('resource_limit:M3_time_index_rows')
        units = []
        for position, table, ordinal in history.objects.get(obj, ()):
            history.guard(); row = history.rows[table][ordinal]
            message = messages[row['message_id']]
            if (message['message_id'] != row['message_id'] or message['source_id'] != row['source_id']
                    or message['record'] != position[1]):
                raise ValueError('M3 时间边界原消息引用冲突')
            at = Time(message['epoch'], message['microsecond'])
            units.append((position, at.lower_us, at.upper_us))
        for gap, _ in matched:
            history.guard(); at = Time(**gap['raw_time'])
            units.append((position_key(gap['position']), at.lower_us, at.upper_us))
        units.sort()
        if any(a[0] == b[0] for a,b in zip(units,units[1:])):
            raise ValueError('M3 同对象时间边界原位置重复')
        size = 3 * len(encode(units).encode())  # 预留原位置、前缀/后缀标量索引的编码空间。
        if history.stats['bytes'] + history.stats['temporal_bytes'] + size > history.max_bytes:
            raise ValueError('resource_limit:M3_time_index_bytes')
        history.stats['temporal_rows'] += incoming; history.stats['temporal_bytes'] += size
        prefix=[];suffix=[None]*(len(units)+1)
        for i,(_,low,high) in enumerate(units):prefix.append(high if not prefix else max(prefix[-1],high))
        for i in range(len(units)-1,-1,-1):
            suffix[i]=units[i][1] if suffix[i+1] is None else min(units[i][1],suffix[i+1])
        history._time_indexes[obj]=(units,prefix,suffix)
    units,prefix,suffix=history._time_indexes[obj]
    at=bisect_left(prefix,sample_us)-1
    if at < 0 or suffix[at+1] is not None and suffix[at+1] < sample_us:
        return dict(presence='unknown',path_key=None,origin=None,source_ref=None,
                    time_boundary='not_locatable',position=None,gap_refs=(),active_gap_refs=())
    result=history.at(scope,units[at][0])
    return dict(result,time_boundary='located')
