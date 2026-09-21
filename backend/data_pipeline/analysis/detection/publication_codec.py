"""Detection 完整原值的有限编码；正文 JSON 字符串和列表原序不重写。"""
from datetime import datetime, timezone
import json
from data_pipeline.bgp.archive.value_codec import canonical, digest, fields, REQUEST_FIELDS

CODEC = 'detection-publication-typed/v1'


def _encode(v):
    if isinstance(v, datetime):
        if v.tzinfo is None: raise ValueError('时间必须明确时区')
        return ['datetime', v.astimezone(timezone.utc).isoformat()]
    if type(v) is dict: return ['dict', [[k, _encode(x)] for k, x in sorted(v.items())]]
    if type(v) in (list, tuple): return ['list', [_encode(x) for x in v]]
    if type(v) is bytes: return ['bytes', v.hex()]
    if type(v) not in (str, int, bool, float, type(None)): raise ValueError('未知原值类型')
    return [type(v).__name__, v]


def typed(value): return canonical(_encode(value))


def untyped(text):
    def decode(v):
        if type(v) is not list or len(v) != 2: raise ValueError('typed结构无效')
        tag, x = v
        if tag == 'dict': return {k: decode(w) for k, w in x}
        if tag == 'list': return [decode(w) for w in x]
        if tag == 'bytes': return bytes.fromhex(x)
        if tag == 'datetime': return datetime.fromisoformat(x)
        types = {'str': str, 'int': int, 'bool': bool, 'float': float, 'NoneType': type(None)}
        if tag not in types or type(x) is not types[tag]: raise ValueError('typed类型无效')
        return x
    value = decode(json.loads(text))
    if typed(value) != text: raise ValueError('必须为规范typed文本')
    return value
