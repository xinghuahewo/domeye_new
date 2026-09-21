"""湖完整验收v2的独立结构合同；不从Writer实例或导出行推导期望。"""
from copy import deepcopy
from datetime import datetime
from data_pipeline.analysis.detection.roles import interpret_roles
from data_pipeline.analysis.detection.classification import interpret_classification, DECISION_LOCATION

INTEGRITY_VERSION = 'detection-lake-integrity/v2'
# 对照四个旧模块构造器、projection及store.state_attributes固定列出。
# 科学模块不因验收改变；扩展终态结构须显式更新版本合同。
STATE_LAYOUT = (
    ('m3', {'qualification':'dict'}),
    ('outage', {**dict.fromkeys(('prefix_vp','as_prefix','country_as','prefix_init_id','as_init_id','country_init_id',
                               'prefix_outage_event','as_outage_event','country_outage_event','country_outage_v2_runtime'),'dict'),
                **dict.fromkeys(('prefix_table','as_table','country_table','event_table'),'value')}),
    ('hijack', {'prefix_event':'dict','moas_event_dict':'dict','moas_table':'value','hijack_table':'value',
                'event_table':'value','moas_init_id':'dict','hijack_init_id':'dict'}),
    ('subhijack', {'prefix_event':'dict','sub_hijack_dict':'dict','sub_hijack_table':'value','event_table':'value',
                   'moas_init_id':'dict','sub_hijack_init_id':'dict'}),
    ('leak', {**dict.fromkeys(('leak_phenomenon_table','leak_event_table','event_table'),'value'),
              **dict.fromkeys(('prefix_event','prefix_path_dict','phenomenon_dict','phenomenon_init_id','event_init_id'),'dict')}),
    ('projection', {'prefix_dict':'dict','prefix_as':'dict','as_prefix':'dict','vp_set':'set','t':'value','tree_origins':'dict'}),
    ('output', {'identities':'dict','revisions':'dict','last_records':'dict','start_tables':'dict',
                'errors':'list','projection_gaps':'list','thresholds':'dict'}),
    ('run', {'baseline':'dict','references':'dict','processed':'value','last_observation':'value','seen_observations':'set'}),
)
STATE_HEADERS = {(family,name):container for family,attrs in STATE_LAYOUT for name,container in attrs.items()}


def expected_indexes(attrs, legacy, evidence):
    """从原正文复算所有非JSON索引，直接用固定规则，不引用store的映射函数别名。"""
    kind=attrs.get('event_kind')
    subject='country' if kind=='country_outage' else 'asn' if kind=='as_outage' else 'prefix' if kind else None
    roles=interpret_roles(kind,legacy)
    original=dict(attrs,legacy=legacy,evidence=evidence)
    # 原codec把规则入参set保存为显式$set；仅恢复该已知成员集合供原分类规则判定。
    if attrs['kind']=='rule_decision' and legacy.get('rule')==DECISION_LOCATION:
        original=deepcopy(original)
        pair=original['legacy']['inputs']['args'][1]
        if isinstance(pair,dict) and set(pair)=={'$set'}:
            original['legacy']['inputs']['args'][1]=pair['$set']
    classification=interpret_classification(original)
    if classification['classification_state']=='ambiguous':
        roles.update(asn_roles='ambiguous',attacker_asn=None,victim_asn=None,
                     role_reason='original_as_missing_or_outside_moas_pair')
    observed_at=evidence.get('observation',{}).get('observed_at')
    return dict(record_kind=attrs['kind'],incident_id=attrs.get('incident_id'),revision=attrs.get('revision'),
                event_kind=kind,subject_type=subject,subject_key=str(attrs.get('object')) if subject else None,
                observed_at=datetime.fromisoformat(observed_at.replace('Z','+00:00')) if observed_at else None,
                **roles,**classification)
