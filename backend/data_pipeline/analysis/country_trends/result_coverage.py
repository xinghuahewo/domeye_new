"""结果级覆盖独立于事件行；执行完成、零事件与可用空结果分开。"""
from data_pipeline.analysis.country_trends.contract import row
from data_pipeline.analysis.country_trends.qualified_models import DIMENSIONS


def coverage_rows(logical_run_id, input_binding_id, window_us, records, event_count):
    """消费已解码的完整原覆盖字段；无事件时仍要求实际12维原覆盖。

    这是纯语义检查，不替代上游 admit 或完整读回执核对。
    """
    if type(event_count) is not int or event_count < 0:
        raise ValueError('trend_m3_event_count')
    by_dimension = {}
    for record in records:
        dimension = record['dimension']
        if dimension not in DIMENSIONS or dimension in by_dimension:
            raise ValueError('trend_m3_coverage_dimensions')
        if (record['logical_run_id'] != logical_run_id or record['input_binding_id'] != input_binding_id
                or record['window_us'] != window_us or record['module'] != 'country'
                or record['rule_version'] != 'country-qualification/v1'
                or record['execution'] != 'complete' or type(record['event_count']) is not int
                or record['event_count'] != event_count
                or record['coverage'] not in ('complete', 'partial', 'unknown')
                or not record['completion_receipt_refs'] or not record['coverage_id']):
            raise ValueError('trend_m3_coverage_identity_or_completion')
        by_dimension[dimension] = dict(record)
    if set(by_dimension) != set(DIMENSIONS):
        raise ValueError('trend_m3_missing_independent_coverage')
    enumeration = by_dimension['event_enumeration']
    for dimension in DIMENSIONS:
        record = by_dimension[dimension]
        yield row('country_coverage', (), (dimension,), raw=record)
    state = ('admitted_empty' if enumeration['coverage'] == 'complete' else 'unknown_empty') if event_count == 0 else 'observed_events'
    yield row('result_availability', (), execution='complete', event_count=event_count,
              enumeration_coverage=enumeration['coverage'], state=state,
              coverage_id=enumeration['coverage_id'], completion_receipt_refs=enumeration['completion_receipt_refs'])
    if enumeration['coverage'] != 'complete':
        yield row('unknown', (), ('event_enumeration',), code='event_enumeration_not_complete',
                  reasons=('event_enumeration:' + enumeration['coverage'],),
                  qualification_refs=())
