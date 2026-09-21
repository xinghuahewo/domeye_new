"""资格化科学导航；所有主结论依附实际科学行和原资格引用。"""
from data_pipeline.analysis.country_trends.compute import GRAPH_KINDS, graph_rows
from data_pipeline.analysis.country_trends.contract import row
from data_pipeline.analysis.country_trends.stream_schema import row_hash


def evidence_rows(scientific):
    """复用旧科学图，追加精确资格边；新局部行分别陈述可用和Unknown。"""
    values = dict(scientific.values)
    if scientific.kind in GRAPH_KINDS:
        if 'qualification_refs' not in values:
            raise ValueError('trend_m3_graph_missing_qualification')
        for item in graph_rows(scientific, input_condition='country_m3_per_target_qualification'):
            yield item
            if item.kind == 'evidence':
                for qid in values['qualification_refs']:
                    yield row('evidence_qualification', scientific.event, (item.key[0], qid),
                              qualification_id=qid)
        return
    if scientific.kind not in ('qualified_metric', 'qualified_asn_point', 'qualified_path', 'family_point'):
        return
    locator = scientific.kind, scientific.event, scientific.key
    node = row_hash(scientific)
    refs = scientific.refs if scientific.kind != 'family_point' else values['qualification_refs']
    yield row('evidence', scientific.event, (node,), source_kind=scientific.kind,
              source_key=scientific.key, source_locator=locator, values=scientific.values,
              source_refs=scientific.refs,
              conditions=('country_m3_per_target_qualification', 'no_cause_user_impact',
                          'current_does_not_complete_history_or_session'))
    yield row('evidence_source', scientific.event, (node, 0), source_locator=locator)
    for qid in dict.fromkeys(refs):
        yield row('evidence_qualification', scientific.event, (node, qid), qualification_id=qid)
    if scientific.kind == 'qualified_path':
        parts = (('current', values['current'], values['current_reasons']),
                 ('history', values['history'], values['history_reasons']))
    else:
        main = values['main'] if scientific.kind == 'qualified_asn_point' else values['value']
        parts = (('value', main, values.get('reasons', values.get('qualification_reasons', ()))),)
    for part, main, reasons in parts:
        if main is None:
            yield row('unknown', scientific.event, (node, part), code=part + '_qualification_unavailable',
                      reasons=reasons, qualification_refs=tuple(refs))
        else:
            # observed_zero仍是已观测值；不使用truthiness判断。
            claim = node + ':' + part
            yield row('claim', scientific.event, (claim,), evidence_id=node,
                      source_locator=locator, conclusion_level='bounded_control_plane', state='bounded_fact')
            yield row('edge', scientific.event, (claim, 'supported_by', node))
