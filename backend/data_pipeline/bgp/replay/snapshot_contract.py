"""M3B 独立 canonical 投影的有限公开合同；不代表业务发布资格。"""
from dataclasses import dataclass
from enum import Enum
import base64
import hashlib
import json
from functools import lru_cache

PROFILE = 'canonical_projection/v1'
TABLES = ('baseline_mappings', 'changes', 'invalidations', 'scope_gap',
          'qualification_change', 'source_coverage', 'source_quality', 'current_routes',
          'legacy_state', 'legacy_prefixes', 'legacy_seen_vps', 'projection_metadata',
          'reference_binding')
# 独立投影专用列：原 typed 正文可逆保存，引用列供 SQL 完整性审计。
COLUMNS = ('seq', 'source_id', 'message_id', 'event_id', 'object_key', 'gap_id', 'payload')


@lru_cache(maxsize=4096)
def _string_key_order(value):
    return json.dumps(value, sort_keys=True)


def _key_order(value):
    if type(value) is str and len(value) <= 128:
        return _string_key_order(value)
    return json.dumps(_pack(value), sort_keys=True)


def _pack(value):
    if isinstance(value,Enum):return _pack(value.value)
    if isinstance(value, bytes):return {'bytes':base64.b64encode(value).decode('ascii')}
    if isinstance(value, tuple):return {'tuple':[_pack(v) for v in value]}
    if isinstance(value, set):return {'set':sorted((_pack(v) for v in value),key=lambda v:json.dumps(v,sort_keys=True))}
    if isinstance(value, dict):return {'dict':[[_pack(k),_pack(v)] for k,v in sorted(value.items(),key=lambda pair:_key_order(pair[0]))]}
    if isinstance(value, list):return [_pack(v) for v in value]
    if value is None or type(value) in (str,int,bool,float):return value
    raise TypeError('投影正文出现合同外类型: '+type(value).__name__)


def _unpack(value):
    if isinstance(value,list):return [_unpack(v) for v in value]
    if not isinstance(value,dict):return value
    if len(value)!=1:raise ValueError('投影编码标签错误')
    key,body=next(iter(value.items()))
    if key=='bytes':return base64.b64decode(body,validate=True)
    if key=='tuple':return tuple(_unpack(v) for v in body)
    if key=='set':return set(_unpack(v) for v in body)
    if key=='dict':return {_unpack(k):_unpack(v) for k,v in body}
    raise ValueError('未知投影编码标签')


def encode(value):
    return json.dumps(_pack(value),ensure_ascii=False,separators=(',',':'),allow_nan=False)


def decode(value):return _unpack(json.loads(value))


def digest(value):return hashlib.sha256(encode(value).encode()).hexdigest()


@dataclass(frozen=True)
class ProjectionBinding:
    """外部 Reader 必须同时给定三值，禁止按 latest 查询。"""
    run_id: str
    snapshot: int
    seal_digest: str
    profile: str = PROFILE
