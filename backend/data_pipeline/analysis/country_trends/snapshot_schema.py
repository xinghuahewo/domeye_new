"""S2有限湖表合同：逐kind字段列与原顺序，字段省略不等于typed NULL。"""
from dataclasses import fields,is_dataclass
import hashlib,json
from data_pipeline.analysis.country_trends.contract import TrendRow, ActivityWindow, Projection, ReferenceInput
from data_pipeline.analysis.country_events import snapshot_schema as c3

VERSION='country-trend-store/v1'
CODEC='country-trend-typed/v1'
PROFILE='country-trend-strict/v1'
RULES=('country-trend-exact-direction/v1','trend-activity-exact-interval/v1')
VARIANTS = {'activity_context': [('state',)], 'activity_relation': [('window_key', 'state_point_key', 'peak_interval_end', 'peak_value', 'offset_slots', 'relation', 'causal_claim')], 'activity_window': [('raw', 'state', 'anchor_us', 'reason')], 'analysis': [('state', 'pattern', 'features'), ('state', 'reason')], 'asn_context': [('state', 'asn_count'), ('state', 'reason'), ('state',)], 'asn_point': [('raw',)], 'asn_population': [('counts', 'total')], 'asn_priority': [('asn', 'afi', 'scale_prefix_count', 'longest_observed_non_normal', 'single_impact_score')], 'asn_summary': [('start', 'end', 'persistent_not_at_start', 'state_counts', 'longest_runs', 'scale_prefix_count')], 'asn_transition': [('from_state', 'to_state', 'from_us', 'to_us', 'continuous_recovery_claim')], 'asn_transition_cell': [('count',)], 'atomic': [('sample_us', 'state', 'tags', 'delta', 'delta_pp')], 'availability': [('state',)], 'claim': [('evidence_id', 'source_locator', 'conclusion_level', 'state')], 'context_source': [('source_ref', 'role', 'raw_typed')], 'edge': [()], 'event': [('state', 'reasons', 'cohort_id', 'basis')], 'evidence': [('source_kind', 'source_key', 'source_locator', 'values', 'source_refs', 'conditions')], 'evidence_source': [('source_locator',)], 'fact': [('value', 'rounded_value', 'rounding', 'formula', 'operands', 'unit')], 'family_context': [('state', 'maximum_divergence', 'maximum_at', 'extreme_delta_slots', 'relation', 'denominator_ratio', 'asymmetry'), ('state', 'reason')], 'family_divergence': [('ipv4_minus_ipv6_pp',)], 'family_point': [('value', 'denominator', 'unit')], 'limitation': [('code',)], 'metric': [('raw',)], 'peak': [('known_peak', 'exact_peak', 'first_us', 'occurrence_count', 'known_slots', 'unknown_slots', 'unit')], 'phase': [('start_us', 'end_us', 'state', 'tags', 'start_value', 'end_value', 'unit')], 'point': [('sample_us', 'value', 'unit')], 'profile': [('state', 'unit', 'denominator', 'sample_count')], 'quality': [('raw',)], 'raw_source': [('source_sequence', 'source_table', 'raw_typed')], 'reference_cdf': [('target', 'percentile', 'comparable_count', 'contributors')], 'reference_common': [('declining_count', 'share', 'target_declined', 'causal_claim')], 'reference_context': [('state', 'comparable_count'), ('state',)], 'reference_country': [('cohort_id', 'definition_binding', 'decline_pp', 'below95_samples', 'migration_ratio', 'shape')], 'reference_exclusion': [('reason',)], 'reference_shape': [('count', 'share')], 'result': [('state', 'country_result_id', 'country_receipt', 'input_sha256', 'event_count', 'feature_state', 'reference_state')], 'source_completion': [('raw',)], 'source_evidence': [('raw_typed',)], 'source_window_peak': [('raw',)], 'threshold': [('count', 'continuous_duration_claimed')], 'threshold_sample': [('sample_us',)], 'unknown': [('code', 'evidence_id', 'source_locator', 'reason'), ('code',)]}
VARIANTS['activity_context'].append(('state','reason'))
TABLES=tuple(VARIANTS)
FIELDS={k:tuple(dict.fromkeys(f for variant in vv for f in variant)) for k,vv in VARIANTS.items()}
COMMON=(('sequence','BIGINT'),('result_id','VARCHAR'),('incident','VARCHAR'),('revision','VARCHAR'),('key_typed','VARCHAR'),('refs_typed','VARCHAR'),('field_order','VARCHAR'),('row_sha256','VARCHAR'))
SCHEMAS={k:(*COMMON,*((f,'VARCHAR') for f in FIELDS[k])) for k in TABLES}
LOCAL={c.__name__:c for c in (ActivityWindow,Projection,ReferenceInput)}


def pack(v):
    if type(v) is tuple:return ['tuple',[pack(x) for x in v]]
    if is_dataclass(v) and type(v).__name__ in LOCAL and LOCAL[type(v).__name__] is type(v):
        return ['context',type(v).__name__,[[f.name,pack(getattr(v,f.name))] for f in fields(v)]]
    return ['c3',c3.encode(v)]


def unpack(v):
    if v[0]=='tuple':return tuple(unpack(x) for x in v[1])
    if v[0]=='c3':return c3.decode(v[1])
    if v[0]=='context':
        cls=LOCAL[v[1]]
        if [k for k,_ in v[2]]!=[f.name for f in fields(cls)]:raise ValueError('trend_context_fields')
        return cls(**{k:unpack(x) for k,x in v[2]})
    raise ValueError('trend_codec_tag')


def encode(v):return json.dumps(pack(v),ensure_ascii=False,separators=(',',':'),allow_nan=False)
def decode(v):
    result=unpack(json.loads(v))
    if encode(result)!=v:raise ValueError('trend_noncanonical_typed')
    return result

def row_material(r):return (r.kind,r.event,r.key,r.values,r.refs,r.result_id)
def row_hash(r):return hashlib.sha256(encode(row_material(r)).encode()).hexdigest()


# 固定科学/资格枚举；未知值不能借合法列名与重算hash通过。
ENUMS={
 'event':{'state':('available','partial','unavailable'),'basis':('fixture_calculation','strict_c3_calculation')},
 'profile':{'state':('complete','no_samples','unavailable','degraded')},
 'analysis':{'state':('complete','unavailable'),'pattern':('small_denominator','plateau','oscillation','multi_wave','single_wave_partial_rebound','single_wave_return_to_window_start','single_wave_above_window_start','mixed','unmatched')},
 'availability':{'state':('missing_baseline',)},
 'atomic':{'state':('stable','decline','rise','abrupt_drop','abrupt_rise','low_plateau')},
 'phase':{'state':('stable','decline','rise','abrupt_drop','abrupt_rise','low_plateau')},
 'activity_context':{'state':('not_provided','unavailable')},
 'activity_window':{'state':('aligned','unavailable')},
 'asn_context':{'state':('unavailable','no_samples','complete')},
 'family_context':{'state':('unavailable','complete')},
 'reference_context':{'state':('not_provided','insufficient_data','complete')},
 'claim':{'state':('known_subset','bounded_fact')},
 'context_source':{'role':('reference','feature')},
 'result':{'state':('complete','admitted_empty'),'feature_state':('not_provided','bound'),'reference_state':('not_provided','bound_artificial_history_unknown')},
}
for kind in ('activity_relation','family_context'):ENUMS.setdefault(kind,{})['relation']=('same_slot','adjacent_slot','lagged_slot')
for field in ('from_state','to_state'):ENUMS.setdefault('asn_transition',{})[field]=('normal','affected','route_interrupted','unknown')


def reference_count(r):
    return len(r.refs)+(2 if r.kind=='edge' else 1 if r.kind in ('evidence','evidence_source','claim') else 0)


def flatten(sequence,r):
    if type(r) is not TrendRow or r.kind not in TABLES:raise ValueError('trend_table_kind')
    names=tuple(k for k,v in r.values)
    for name,value in r.values:
        if name in ENUMS.get(r.kind,{}) and value not in ENUMS[r.kind][name]:raise ValueError('trend_unknown_enum')
    if names not in VARIANTS[r.kind]:raise ValueError('trend_row_fields')
    if type(sequence) is not int or sequence<0 or type(r.event) is not tuple or type(r.key) is not tuple or type(r.refs) is not tuple:raise ValueError('trend_row_key')
    if r.event and (len(r.event)!=2 or type(r.event[0]) is not str or type(r.event[1]) is not int or r.event[1]<1):raise ValueError('trend_row_event')
    if not r.result_id:raise ValueError('trend_result_identity')
    out=dict(sequence=sequence,result_id=r.result_id,incident=r.event[0] if r.event else None,
             revision=str(r.event[1]) if r.event else None,key_typed=encode(r.key),refs_typed=encode(r.refs),
             field_order=encode(names),row_sha256=row_hash(r))
    out.update({f:None for f in FIELDS[r.kind]})
    out.update({k:encode(v) for k,v in r.values})
    return out


def restore(kind,v):
    if kind not in TABLES or set(v)!={n for n,t in SCHEMAS[kind]}:raise ValueError('trend_actual_columns')
    order=decode(v['field_order'])
    if order not in VARIANTS[kind]:raise ValueError('trend_field_variant')
    if any(v[f] is not None for f in FIELDS[kind] if f not in order):raise ValueError('trend_extra_field')
    event=() if v['incident'] is None and v['revision'] is None else (v['incident'],int(v['revision']))
    r=TrendRow(kind,event,decode(v['key_typed']),tuple((f,decode(v[f])) for f in order),decode(v['refs_typed']),v['result_id'])
    if flatten(v['sequence'],r)!=v:raise ValueError('trend_row_identity')
    return r
