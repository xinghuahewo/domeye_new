"""Resource P1 的有限 typed 值；时间、Decimal 与 NULL 不混同。"""
from datetime import datetime,timezone
from decimal import Decimal
import json
from data_pipeline.bgp.archive.value_codec import canonical, digest, fields

CODEC='resource-publication-typed/v1'

def encoded(v):
    if type(v) is bytes:return ['bytes',v.hex()]
    if isinstance(v,datetime):return ['datetime',v.astimezone(timezone.utc).isoformat()] if v.tzinfo is not None else ['datetime',v.isoformat()]
    if isinstance(v,Decimal):return ['decimal',str(v)]
    if type(v) is dict:return ['dict',[[k,encoded(x)] for k,x in sorted(v.items())]]
    if type(v) in (tuple,list):return ['list',[encoded(x) for x in v]]
    if type(v) not in (str,int,float,bool,type(None)):raise ValueError('非有限原值类型')
    return [type(v).__name__,v]

def typed(v):return canonical(encoded(v))

def untyped(text):
    def decode(v):
        if type(v) is not list or len(v)!=2:raise ValueError('typed结构无效')
        t,x=v
        if t=='bytes':return bytes.fromhex(x)
        if t=='datetime':return datetime.fromisoformat(x)
        if t=='decimal':return Decimal(x)
        if t=='dict':return {k:decode(z) for k,z in x}
        if t=='list':return [decode(z) for z in x]
        types={'str':str,'int':int,'float':float,'bool':bool,'NoneType':type(None)}
        if t not in types or type(x) is not types[t]:raise ValueError('typed原值类型无效')
        return x
    result=decode(json.loads(text))
    if typed(result)!=text:raise ValueError('非规范typed文本')
    return result
