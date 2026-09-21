"""C3固定白名单类型与物理列；主要typed字段逐列，ancillary值可解释无损编码。"""
from dataclasses import fields, is_dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import pytz
from decimal import Decimal
from fractions import Fraction
import base64
import hashlib
import json
import re
import types
import typing

from data_pipeline.analysis.country_events import models, compute, incremental_types, event_aggregation as c2, saved_input

SCHEMA_VERSION='country-component/v1'
OUTPUTS=(c2.EventStatus,c2.CohortMember,c2.BoundaryUnavailable,c2.InputEvidence,
    compute.PrefixPoint,compute.AsnPoint,compute.MetricPoint,compute.NewPrefixPoint,
    compute.ObservationFact,compute.NewDirectionPoint,compute.PathSample,compute.PathSummary,
    compute.WindowClass,compute.Peak,compute.Quality,compute.Completion,
    incremental_types.DirectionPoint,incremental_types.FirstQualifiedRef,
    incremental_types.PathPeakQuality,incremental_types.AsnMetricPeak,c2.C2Completion)
NESTED=(models.Time,models.Cursor,models.Endpoint,models.Route,models.Binding,models.Incident,
        models.Baseline,saved_input.CountryRevision,saved_input.InputCompletion)
REGISTRY={cls.__name__:cls for cls in (*OUTPUTS,*NESTED)}
TABLES={re.sub(r'(?<!^)(?=[A-Z])','_',cls.__name__).lower():cls for cls in OUTPUTS}
# C2Completion的自然名称特殊固定，避免数字拆分规则漂移。
TABLES['c2_completion']=TABLES.pop('c2_completion',c2.C2Completion)
BY_CLASS={cls:name for name,cls in TABLES.items()}


def tree(value):
    if value is None:return ['none']
    if type(value) is bool:return ['bool',value]
    if type(value) is int:return ['int',str(value)]
    if type(value) is float:return ['float',value.hex()]
    if type(value) is str:return ['str',value]
    if type(value) is bytes:return ['bytes',base64.b64encode(value).decode()]
    if isinstance(value,Fraction):return ['fraction',str(value.numerator),str(value.denominator)]
    if isinstance(value,Decimal):return ['decimal',str(value)]
    if isinstance(value,datetime):return ['datetime',value.replace(tzinfo=None).isoformat(),value.fold,tz_encode(value.tzinfo)]
    if isinstance(value,timedelta):return ['timedelta',value.days,value.seconds,value.microseconds]
    if isinstance(value,date):return ['date',value.isoformat()]
    if type(value) in (tuple,list):return [type(value).__name__,[tree(v) for v in value]]
    if type(value) is dict:return ['dict',[[tree(k),tree(v)] for k,v in value.items()]]
    if type(value) in (set,frozenset):return [type(value).__name__,sorted((tree(v) for v in value),key=packed)]
    if is_dataclass(value) and type(value).__name__ in REGISTRY and REGISTRY[type(value).__name__] is type(value):
        return ['record',type(value).__name__,[[f.name,tree(getattr(value,f.name))] for f in fields(value)]]
    raise ValueError('unsupported_component_value:'+type(value).__name__)


def tz_encode(tz):
    if tz is None:return None
    if isinstance(tz,ZoneInfo):return ['zoneinfo',tz.key]
    if type(tz) is timezone:return ['timezone',[tree(a) for a in tz.__reduce__()[1]]]
    if getattr(tz,'zone',None) is not None:
        return ['pytz',tz.zone,tree(getattr(tz,'_utcoffset',timedelta(0))),tree(getattr(tz,'_dst',timedelta(0))),getattr(tz,'_tzname','UTC')]
    raise ValueError('unsupported_timezone_type')


def tz_decode(item):
    if item is None:return None
    if item[0]=='zoneinfo':return ZoneInfo(item[1])
    if item[0]=='timezone':return timezone(*(untree(v) for v in item[1]))
    if item[0]=='pytz':
        tz=pytz.timezone(item[1])
        return tz._tzinfos[(untree(item[2]),untree(item[3]),item[4])] if hasattr(tz,'_tzinfos') else tz
    raise ValueError('unsupported_timezone_tag')


def packed(value):return json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(',',':'))
def encode(value):return packed(tree(value))
def integer(value):
    if not isinstance(value,str) or re.fullmatch(r'0|-?[1-9][0-9]*',value) is None:raise ValueError('noncanonical_integer')
    return int(value)


def untree(item):
    tag=item[0]
    if tag=='none':return None
    if tag=='bool':return item[1]
    if tag=='int':return integer(item[1])
    if tag=='float':return float.fromhex(item[1])
    if tag=='str':return item[1]
    if tag=='bytes':return base64.b64decode(item[1],validate=True)
    if tag=='fraction':return Fraction(integer(item[1]),integer(item[2]))
    if tag=='decimal':return Decimal(item[1])
    if tag=='datetime':return datetime.fromisoformat(item[1]).replace(fold=item[2],tzinfo=tz_decode(item[3]))
    if tag=='timedelta':return timedelta(days=item[1],seconds=item[2],microseconds=item[3])
    if tag=='date':return date.fromisoformat(item[1])
    if tag in ('tuple','list','set','frozenset'):
        return {'tuple':tuple,'list':list,'set':set,'frozenset':frozenset}[tag](untree(v) for v in item[1])
    if tag=='dict':return {untree(k):untree(v) for k,v in item[1]}
    if tag=='record':
        cls=REGISTRY[item[1]]
        if [v[0] for v in item[2]]!=[f.name for f in fields(cls)]:raise ValueError('record_fields_mismatch')
        return cls(**{k:untree(v) for k,v in item[2]})
    raise ValueError('unknown_component_tag')


def decode(value):
    result=untree(json.loads(value))
    if encode(result)!=value:raise ValueError('noncanonical_component_value')
    return result


OVERRIDES={
 (models.Incident,'legacy_ref'):object,(models.Incident,'source_episode_ref'):object,
 (c2.EventStatus,'baseline'):models.Baseline,(c2.EventStatus,'cohort_id'):str,
 (c2.EventStatus,'direction_count'):int,(c2.EventStatus,'fixed_prefix_count'):int,
 (c2.CohortMember,'route'):models.Route,(c2.BoundaryUnavailable,'sample_us'):int,
 (c2.C2Completion,'input_completion'):saved_input.InputCompletion,
}


def field_kind(cls,f):
    t=OVERRIDES.get((cls,f.name),typing.get_type_hints(cls).get(f.name))
    if typing.get_origin(t) in (typing.Union,types.UnionType):
        t=next((v for v in typing.get_args(t) if v is not type(None)),object)
    if t in (str,int,bool,bytes,Fraction):return t
    if t in NESTED:return t
    return 'ancillary'


def columns(cls,prefix=''):
    out=[]
    for f in fields(cls):
        name=prefix+f.name;kind=field_kind(cls,f)
        if kind in NESTED:
            out.append((name+'__present','BOOLEAN','optional_record'))
            out.extend(columns(kind,name+'__'))
        elif kind is Fraction:
            out.extend((name+'__'+suffix,'VARCHAR',meaning) for suffix,meaning in
                       [('kind','int_or_fraction_or_none'),('num','exact_integer_decimal'),('den','exact_positive_integer_decimal')])
        else:
            physical='BOOLEAN' if kind is bool else 'BLOB' if kind is bytes else 'VARCHAR'
            semantic='exact_integer_decimal' if kind is int else 'tagged_ancillary_v1' if kind=='ancillary' else kind.__name__
            out.append((name,physical,semantic))
    return out


def flatten(value,cls=None,prefix=''):
    cls=cls or type(value);out={}
    for f in fields(cls):
        name=prefix+f.name;kind=field_kind(cls,f);v=getattr(value,f.name) if value is not None else None
        if kind in NESTED:
            if v is not None and type(v) is not kind:raise ValueError('component_record_type:'+name)
            out[name+'__present']=v is not None;out.update(flatten(v,kind,name+'__'))
        elif kind is Fraction:
            if v is None:parts=(None,None,None)
            elif type(v) is int:parts=('int',str(v),None)
            elif type(v) is Fraction:parts=('fraction',str(v.numerator),str(v.denominator))
            else:raise ValueError('component_exact_type:'+name)
            out.update(zip((name+'__kind',name+'__num',name+'__den'),parts))
        elif kind=='ancillary':out[name]=encode(v)
        elif v is None:out[name]=None
        else:
            if type(v) is not kind:raise ValueError('component_field_type:'+name)
            out[name]=str(v) if kind is int else v
    return out


def inflate(cls,row,prefix=''):
    values={}
    for f in fields(cls):
        name=prefix+f.name;kind=field_kind(cls,f)
        if kind in NESTED:
            values[f.name]=inflate(kind,row,name+'__') if row[name+'__present'] else None
        elif kind is Fraction:
            tag,num,den=(row[name+'__'+s] for s in ('kind','num','den'))
            if tag is None and num is None and den is None:v=None
            elif tag=='int' and den is None:v=integer(num)
            elif tag=='fraction' and integer(den)>0:v=Fraction(integer(num),integer(den))
            else:raise ValueError('invalid_component_exact')
            values[f.name]=v
        elif kind=='ancillary':values[f.name]=decode(row[name])
        elif kind is int:values[f.name]=integer(row[name]) if row[name] is not None else None
        else:values[f.name]=row[name]
    value=cls(**values)
    if flatten(value,cls,prefix)!={k:row[k] for k,_,_ in columns(cls,prefix)}:raise ValueError('component_noncanonical_fields')
    return value


COMMON=[('_sequence','BIGINT','global_sequence'),('_incident_id','VARCHAR','outer_incident'),
        ('_revision','VARCHAR','outer_revision_integer'),('_row_hash','VARCHAR','sha256')]
SCHEMAS={name:COMMON+columns(cls) for name,cls in TABLES.items()}


def row_encode(sequence,item):
    if isinstance(item,c2.C2Row):incident,revision,value=item.incident_id,item.revision,item.value
    elif isinstance(item,c2.C2Completion):incident,revision,value=None,None,item
    else:raise ValueError('unsupported_C3_input')
    if type(value) not in BY_CLASS:raise ValueError('unsupported_C3_row_type')
    if (incident is None)!=(revision is None) or (revision is not None and type(revision) is not int):raise ValueError('invalid_C3_outer_identity')
    digest=hashlib.sha256(encode((sequence,incident,revision,value)).encode()).hexdigest()
    flat=dict(_sequence=sequence,_incident_id=incident,_revision=str(revision) if revision is not None else None,_row_hash=digest)
    flat.update(flatten(value))
    return BY_CLASS[type(value)],flat


def row_decode(table,row):
    value=inflate(TABLES[table],row)
    item=value if type(value) is c2.C2Completion else c2.C2Row(row['_incident_id'],integer(row['_revision']) if row['_revision'] is not None else None,value)
    _,expected=row_encode(row['_sequence'],item)
    if expected!=row:raise ValueError('component_row_digest_mismatch')
    return item
