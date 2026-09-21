"""S2有限Unknown分母见证资格；不修改原行，不替代C3或S1科学核。"""
from data_pipeline.analysis.country_trends.compute import MetricPoint
from data_pipeline.analysis.country_events import event_aggregation as c2

DENOMINATOR_TRACKS=('interrupted_prefix_count','completely_interrupted_prefix_count',
    'invisible_direction_count','affected_asn_count','route_interrupted_asn_count','visible_direction_count')


def validate_unknown_witnesses(event,values):
    statuses=[v for v in values if isinstance(v,c2.EventStatus)]
    if len(statuses)!=1:raise ValueError('trend_witness_status')
    status=statuses[0]
    if (status.incident.incident_id,status.incident.revision)!=event:raise ValueError('trend_witness_event')
    boundaries={}
    for v in values:
        if isinstance(v,c2.BoundaryUnavailable):boundaries.setdefault(v.sample_us,[]).append(v)
    for p in values:
        if not isinstance(p,MetricPoint) or p.metric not in DENOMINATOR_TRACKS or p.denominator is not None:continue
        if status.cohort_id is None or p.cohort_id!=status.cohort_id:raise ValueError('trend_witness_cohort')
        if p.value is not None or p.state!='unknown':raise ValueError('trend_witness_not_unknown')
        witness=boundaries.get(p.sample_us,())
        if not witness:raise ValueError('trend_witness_missing_sample')
        # BoundaryUnavailable自身没有revision/cohort字段；从原外event/revision及EventStatus限定适用域。
        if any(v.incident_id!=event[0] or type(v.sample_us) is not int or type(v.boundary_us) is not int or v.boundary_us!=p.sample_us for v in witness):
            raise ValueError('trend_witness_identity_or_sample')
