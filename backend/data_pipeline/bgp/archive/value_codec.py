"""M2 P1有限字段与原checkpoint typed编码；不是通用注册合同。"""
import hashlib
import json
from data_pipeline.bgp.archive.checkpoint import encoded

CODEC='m2-checkpoint-typed/v1'
CONTRACT='component-publication-admission/v1'
ADMISSION_FIELDS={'contract','owner','owner_revision','admission_id','owner_binding','binding_codec','physical','validator','inventory_digest','entities','dependencies','lock_targets'}
PHYSICAL_FIELDS={'system_identifier','database_oid','catalog','schema','root','snapshot'}
VALIDATOR_FIELDS={'version','code_sha256','typed_schema','typed_codec','rules_digest','validation_digest'}
ENTITY_FIELDS={'path','device','inode','size','mtime_ns','ctime_ns','sha256'}
M2_ENTITY_FIELDS=ENTITY_FIELDS|{'source_path'}
LOCK_FIELDS={'stage','system_identifier','database_oid','namespace','key'}
REQUEST_FIELDS={'view','scope_typed','codec_version','batch_rows','batch_bytes'}


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def typed(value):return canonical(encoded(value))


def _pairs(pairs):
    result={}
    for k,v in pairs:
        if k in result:raise ValueError('重复字段')
        result[k]=v
    return result


def untyped(text):
    def decode(v):
        if type(v) is not list or len(v)!=2:raise ValueError('typed结构无效')
        tag,item=v
        if tag=='dict':return dict((k,decode(x)) for k,x in item)
        if tag=='list':return [decode(x) for x in item]
        if tag=='bytes':return bytes.fromhex(item)
        types={'str':str,'int':int,'bool':bool,'NoneType':type(None),'float':float}
        if tag not in types or type(item) is not types[tag]:raise ValueError('typed原值类型无效')
        return item
    value=decode(json.loads(text,object_pairs_hook=_pairs))
    if typed(value)!=text:raise ValueError('必须为原codec规范文本')
    return value


def fields(value,expected):
    if type(value) is not dict or set(value)!=set(expected):raise ValueError('有限合同字段缺失或未知')


def admission_shape(a):
    fields(a,ADMISSION_FIELDS);fields(a['physical'],PHYSICAL_FIELDS);fields(a['validator'],VALIDATOR_FIELDS)
    if a['contract']!=CONTRACT or a['owner'] not in ('m2','reference') or a['binding_codec']!=CODEC:raise ValueError('M2/reference合同版本不符')
    # 旧实体形态只允许读入以报告规则版本失效；不能据此升级或重签旧Admission。
    for e in a['entities']:fields(e,M2_ENTITY_FIELDS if 'source_path' in e else ENTITY_FIELDS)
    for t in a['lock_targets']:fields(t,LOCK_FIELDS)
    if a['admission_id']!=digest({k:v for k,v in a.items() if k!='admission_id'}):raise ValueError('Admission摘要错误')
    def sha(value):
        return type(value) is str and len(value)==64 and all(x in '0123456789abcdef' for x in value)
    if type(a['owner_revision']) is not str or len(a['owner_revision'])!=40 or any(x not in '0123456789abcdef' for x in a['owner_revision']):raise ValueError('owner_revision必须为完整提交')
    if not sha(a['inventory_digest']) or any(not sha(a['validator'][k]) for k in ('code_sha256','rules_digest','validation_digest')):raise ValueError('验收摘要类型无效')
    physical=a['physical']
    if type(physical['database_oid']) is not int or physical['database_oid']<=0 or type(physical['snapshot']) is not int or physical['snapshot']<0 or physical['catalog']!='lake':raise ValueError('物理字段类型无效')
    if any(type(physical[k]) is not str or not physical[k] for k in ('system_identifier','schema','root')):raise ValueError('物理身份不能为空')
    if type(a['dependencies']) is not list or sorted(set(a['dependencies']))!=a['dependencies'] or any(not sha(x) for x in a['dependencies']):raise ValueError('依赖必须为有序ID')
    if type(a['entities']) is not list or sorted(e['path'] for e in a['entities'])!=[e['path'] for e in a['entities']] or len({e['path'] for e in a['entities']})!=len(a['entities']):raise ValueError('实体必须按路径唯一排序')
    for e in a['entities']:
        if not sha(e['sha256']) or any(type(e[k]) is not int or e[k]<0 for k in ('device','inode','size','mtime_ns','ctime_ns')):raise ValueError('实体字段类型无效')
    for t in a['lock_targets']:
        if t['stage']!=10 or type(t['stage']) is not int or t['system_identifier']!=physical['system_identifier'] or t['database_oid']!=physical['database_oid'] or t['namespace'] not in ('m2.run','m2.checkpoint','m2.admission','reference.admission') or type(t['key']) is not str:raise ValueError('不是本owner的有限stage10锁')
    if len({canonical(t) for t in a['lock_targets']})!=len(a['lock_targets']):raise ValueError('重复锁目标')
    untyped(a['owner_binding'])
