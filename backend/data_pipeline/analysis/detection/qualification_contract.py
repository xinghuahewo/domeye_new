"""M3 v2有限内容身份与版本白名单；写入、完成和读取共用。"""
import json
from data_pipeline.analysis.detection._results import stable
from data_pipeline.analysis.detection.roles import ROLE_RULE
from data_pipeline.analysis.detection.classification import CLASSIFICATION_RULE
from data_pipeline.analysis.detection.projection import DetectionProjection
from data_pipeline.bgp.input.path_decoding import DECODER_VERSION, DIRECTION_RULE
from data_pipeline.bgp.record_types import ORDERED_VERSION, GAP_RULE_VERSION

PROFILE = 'detection-m3/v2'
SCHEMA = 'detection-typed-m3/v2'
QUALIFICATION_RULE = 'detection-gap-qualification/v2'
LAKE_PROFILE = 'detection-m3-lake/v2'
LAKE_SCHEMA = 'detection-typed-m3-lake/v2'
FIELDS = ('ordinal', 'entry_id', 'kind', 'incident_id', 'revision', 'source_id', 'gap_id', 'payload_json')


def validate_identity(identity, *, allow_synthetic=False):
    profile=identity.get('output_profile') if isinstance(identity,dict) else None
    schema={PROFILE:SCHEMA,LAKE_PROFILE:LAKE_SCHEMA}.get(profile)
    if schema is None: raise ValueError('M3 profile不受支持')
    versions = dict(output_profile=profile, store_schema_version=schema,
                    qualification_rule=QUALIFICATION_RULE, ordered_version=ORDERED_VERSION,
                    gap_rule=GAP_RULE_VERSION, decoder_version=DECODER_VERSION,
                    direction_rule=DIRECTION_RULE, role_rule_version=ROLE_RULE,
                    classification_rule_version=CLASSIFICATION_RULE,
                    projection_version=DetectionProjection.version,
                    algorithm_version='detection-39578fe-compat-d09/v1')
    if not isinstance(identity,dict) or any(identity.get(k)!=v for k,v in versions.items()):
        raise ValueError('M3 profile/schema/规则版本不受支持')
    allowed=('frozen-fresh-process','synthetic-fixture-api') if allow_synthetic else ('frozen-fresh-process',)
    if identity.get('execution_mode') not in allowed:
        raise ValueError('M3执行模式不受支持')
    if not identity.get('input_binding_id') or not identity.get('selected_sources'):
        raise ValueError('M3固定输入身份缺失')
    binding=identity.get('input_binding',{})
    if (binding.get('profile')!='observation' or binding.get('schema_version')!='observation-checkpoint/v1'
        or binding.get('interpretation_version')!='mrt-interpretation/v1'
        or binding.get('ordered_version')!=ORDERED_VERSION
        or identity.get('reference_interpretation',{}).get('selection_rule')!='detection-reference-39578fe/v2'):
        raise ValueError('M3输入/参考规则版本不受支持')


def payload(row):
    value=row['payload_json']
    def unique(pairs):
        result={}
        for k,v in pairs:
            if k in result: raise ValueError('资格JSON重复键')
            result[k]=v
        return result
    value=json.loads(value,object_pairs_hook=unique) if isinstance(value,str) else value
    if type(value) is not dict: raise ValueError('资格payload必须为对象')
    return value


def target(row,run_id):
    return dict(component='detection',component_run=run_id,
                **{k:row[k] for k in ('source_id','incident_id','revision','gap_id')})


def entry_id(run_id,row):
    """所有语义typed列及完整payload参与身份；JSON文本格式不改变语义身份。"""
    content={k:row[k] for k in FIELDS if k!='entry_id'}
    content['payload_json']=payload(row)
    return stable([QUALIFICATION_RULE,run_id,content])


def validate_entry(row,run_id,identity):
    if set(row)!=set(FIELDS): raise ValueError('资格列集合不符')
    p=payload(row)
    if type(row['ordinal']) is not int or row['ordinal']<0:
        raise ValueError('资格ordinal非法')
    if row['source_id'] not in identity['selected_sources']:
        raise ValueError('资格source不在固定选择')
    if (p.get('component')!='detection' or p.get('component_run')!=run_id or
        p.get('binding_ref')!=identity['input_binding_id'] or p.get('rule_version')!=QUALIFICATION_RULE or
        p.get('target_ref')!=target(row,run_id) or row['entry_id']!=entry_id(run_id,row)):
        raise ValueError('资格内容/目标/身份不符')
    kind=row['kind']
    required={'effective_position'}
    if kind in ('event_qualification','source_coverage'):
        required.update(('coverage','dimensions','dimension_coverage','gap_refs','window'))
        if p.get('coverage') not in ('complete','unknown') or type(p.get('gap_refs')) is not list:
            raise ValueError('资格coverage或Gap引用非法')
    if not required.issubset(p): raise ValueError('资格kind必填payload缺失')
    if kind=='event_qualification':
        if not isinstance(row['incident_id'],str) or not row['incident_id'] or type(row['revision']) is not int or row['revision']<1 or row['gap_id'] is not None:
            raise ValueError('事件资格目标列非法')
    elif kind in ('scope_gap','source_coverage'):
        if row['incident_id'] is not None or row['revision'] is not None:
            raise ValueError('非事件资格不能带科学目标')
        if kind=='scope_gap':
            g=p.get('gap',{})
            if not isinstance(row['gap_id'],str) or not row['gap_id'] or g.get('gap_id')!=row['gap_id'] or g.get('raw_ref',{}).get('source_id')!=row['source_id'] or g.get('binding_ref')!=identity['input_binding_id']:
                raise ValueError('Gap原ID/source/binding不符')
        else:
            receipt=p.get('receipt',{})
            if row['gap_id'] is not None or p.get('selected_source')!=row['source_id'] or receipt.get('raw',{}).get('source_id')!=row['source_id'] or receipt.get('binding_ref')!=identity['input_binding_id']:
                raise ValueError('source coverage原回执绑定不符')
    else:
        raise ValueError('未知资格kind')
    return p
