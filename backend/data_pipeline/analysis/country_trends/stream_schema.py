"""S3 独立有限表合同；保留旧44类并新增资格，不改变 S2/v1。"""
from copy import deepcopy
import hashlib

from data_pipeline.analysis.country_trends import snapshot_schema as old
from data_pipeline.analysis.country_trends.contract import TrendRow

VERSION = 'country-trend-store/v2'
PROFILE = 'country-trend-qualified/v1'
CODEC = 'country-trend-typed/v2'
QUALIFICATION_FIELDS = ('qualification_refs', 'qualification_reasons', 'sample_basis',
                        'continuous_duration_claimed')
SCIENTIFIC = ('profile', 'peak', 'point', 'analysis', 'atomic', 'phase', 'fact',
              'threshold', 'threshold_sample', 'family_point', 'family_context',
              'family_divergence', 'asn_point', 'asn_context', 'asn_population',
              'asn_priority', 'asn_summary', 'asn_transition', 'asn_transition_cell',
              'activity_context', 'activity_window', 'activity_relation', 'reference_context',
              'reference_country', 'reference_exclusion', 'reference_cdf', 'reference_shape', 'reference_common')
VARIANTS = deepcopy(old.VARIANTS)
VARIANTS['reference_context'].append(('state', 'reason'))
for kind in SCIENTIFIC:
    VARIANTS[kind] = [tuple(dict.fromkeys((*variant, *QUALIFICATION_FIELDS)))
                      for variant in VARIANTS[kind]]
VARIANTS.update({
    'analysis_scope': [('calculation_window_us', 'calculation_samples', 'result_window_us',
                        'result_samples', 'original_incident', 'earlier_history')],
    'qualified_metric': [('raw', 'value', 'known_lower_bound', 'state', 'reasons',
                           'qualified_value_id', 'raw_basis_refs', 'unit')],
    'qualified_asn_point': [('raw', 'main', 'reasons', 'raw_target_ref')],
    'qualified_path': [('raw', 'raw_target_ref', 'current', 'current_reasons', 'history',
                        'history_reasons', 'continuity', 'causal_claim')],
    'country_qualification_source': [('raw_typed', 'qualification_id', 'raw_target_ref', 'dimension')],
    'qualified_value_source': [('raw_typed', 'qualified_value_id', 'raw_target_ref', 'dimension')],
    'coverage_source': [('raw_typed', 'coverage_id', 'dimension')],
    'evidence_qualification': [('qualification_id',)],
    'country_coverage': [('raw',)],
    'result_availability': [('execution', 'event_count', 'enumeration_coverage', 'state',
                             'coverage_id', 'completion_receipt_refs')],
})
VARIANTS['unknown'].append(('code', 'reasons', 'qualification_refs'))
VARIANTS['event'].append(('state', 'reasons', 'cohort_id', 'basis', 'raw', 'main', 'lifecycle', 'original_revisions'))
FIELDS = {k: tuple(dict.fromkeys(f for variant in vv for f in variant)) for k, vv in VARIANTS.items()}
TABLES = tuple(VARIANTS)
SCHEMAS = {k: (*old.COMMON, *((f, 'VARCHAR') for f in FIELDS[k])) for k in TABLES}
ENUMS = deepcopy(old.ENUMS)
ENUMS['event']['basis'] = (*ENUMS['event']['basis'], 'country_m3_calculation')
ENUMS.update({'qualified_metric': {'state': ('unknown', 'observed_zero', 'qualified')},
              'qualified_path': {'continuity': ('unknown',)},
              'result_availability': {'execution': ('complete',),
                  'enumeration_coverage': ('complete', 'partial', 'unknown'),
                  'state': ('admitted_empty', 'unknown_empty', 'observed_events')}})
for kind in SCIENTIFIC:
    ENUMS.setdefault(kind, {})['sample_basis'] = ('qualified_samples',)

# 复用已接受的无损值编码实现；新表/字段/语义独立版本。
encode, decode = old.encode, old.decode


def row_hash(r):
    return hashlib.sha256(encode((CODEC, old.row_material(r))).encode()).hexdigest()


def flatten(sequence, r):
    if type(r) is not TrendRow or r.kind not in TABLES:
        raise ValueError('trend_s3_table_kind')
    if (type(sequence) is not int or sequence < 0 or type(r.event) is not tuple
            or type(r.key) is not tuple or type(r.refs) is not tuple or not r.result_id):
        raise ValueError('trend_s3_row_identity')
    if r.event and (len(r.event) != 2 or type(r.event[0]) is not str
                    or not r.event[0] or type(r.event[1]) is not int or r.event[1] < 1):
        raise ValueError('trend_s3_event')
    order = tuple(k for k, _ in r.values)
    if order not in VARIANTS[r.kind]:
        raise ValueError('trend_s3_row_fields')
    for k, v in r.values:
        if k in ENUMS.get(r.kind, {}) and v not in ENUMS[r.kind][k]:
            raise ValueError('trend_s3_enum')
        if k == 'continuous_duration_claimed' and v is not False:
            raise ValueError('trend_s3_unproven_continuity')
    out = dict(sequence=sequence, result_id=r.result_id,
               incident=r.event[0] if r.event else None,
               revision=str(r.event[1]) if r.event else None,
               key_typed=encode(r.key), refs_typed=encode(r.refs),
               field_order=encode(order), row_sha256=row_hash(r))
    out.update({f: None for f in FIELDS[r.kind]})
    out.update({k: encode(v) for k, v in r.values})
    return out


def restore(kind, value):
    if kind not in TABLES or set(value) != {n for n, _ in SCHEMAS[kind]}:
        raise ValueError('trend_s3_actual_columns')
    order = decode(value['field_order'])
    if order not in VARIANTS[kind] or any(value[f] is not None for f in FIELDS[kind] if f not in order):
        raise ValueError('trend_s3_field_order')
    event = () if value['incident'] is None and value['revision'] is None else (value['incident'], int(value['revision']))
    r = TrendRow(kind, event, decode(value['key_typed']),
                 tuple((f, decode(value[f])) for f in order), decode(value['refs_typed']), value['result_id'])
    if flatten(value['sequence'], r) != value:
        raise ValueError('trend_s3_row_identity')
    return r
