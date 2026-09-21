"""历史原值规则 v1：不执行源文本；类型化保存与便利解释分开。"""
import datetime as dt
from decimal import Decimal
import json
import math
import re
import struct

import pyarrow as pa

RULE = 'history-lossless/v1'
SCHEMA = 'history-freeze/v1'
INTS = {'int2': (-2**15, 2**15-1), 'int4': (-2**31, 2**31-1), 'int8': (-2**63, 2**63-1)}
TEXT = {'text', 'varchar', 'bpchar', 'name'}
SUPPORTED = set(INTS) | TEXT | {'numeric', 'float4', 'float8', 'bool', 'bytea', 'json', 'jsonb', 'date', 'timestamp', 'timestamptz', 'interval'}


def canonical(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('ascii')


def pg_kind(column):
    kind = column['base_type']
    if column['type_schema'] != 'pg_catalog' or kind not in SUPPORTED:
        raise ValueError('未支持的原类型: ' + str(column))
    return kind


def interval(raw):
    m = re.fullmatch(r'P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)D)?(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?\d+(?:\.\d{1,6})?)S)?)?', raw)
    if not m or raw == 'P':
        raise ValueError('非冻结ISO interval')
    y, mo, d, h, mi, s = m.groups()
    seconds = Decimal(s or 0)
    return {'months': int(y or 0)*12+int(mo or 0), 'days': int(d or 0),
            'micros': (int(h or 0)*3600+int(mi or 0)*60)*1000000+int(seconds*1000000)}


def parse_array(raw):
    """PG输出语法：引号、反斜线、NULL及显式下界；不使用eval。"""
    pos = 0
    bounds = []
    while pos < len(raw) and raw[pos] == '[':
        m = re.match(r'\[(-?\d+):(-?\d+)\]', raw[pos:])
        if not m: raise ValueError('数组下界损坏')
        lo, hi = map(int, m.groups()); bounds.append((lo, hi)); pos += len(m[0])
    if bounds:
        if raw[pos:pos+1] != '=': raise ValueError('数组缺等号')
        pos += 1
    def level():
        nonlocal pos
        if raw[pos:pos+1] != '{': raise ValueError('数组缺左括号')
        pos += 1; result = []
        if raw[pos:pos+1] == '}': pos += 1; return result
        while True:
            if raw[pos:pos+1] == '{': value = level()
            elif raw[pos:pos+1] == '"':
                pos += 1; chars = []
                while pos < len(raw) and raw[pos] != '"':
                    if raw[pos] == '\\': pos += 1
                    if pos >= len(raw): raise ValueError('数组转义损坏')
                    chars.append(raw[pos]); pos += 1
                if pos >= len(raw): raise ValueError('数组引号未闭合')
                pos += 1; value = ''.join(chars)
            else:
                start = pos
                while pos < len(raw) and raw[pos] not in ',}': pos += 1
                token = raw[start:pos]
                if not token: raise ValueError('数组空token')
                value = None if token == 'NULL' else token
            result.append(value)
            if raw[pos:pos+1] == '}': pos += 1; return result
            if raw[pos:pos+1] != ',': raise ValueError('数组分隔损坏')
            pos += 1
    nested = level()
    if pos != len(raw): raise ValueError('数组尾部损坏')
    def shape(x):
        if not isinstance(x, list): return []
        if not x: return [0]
        sub = shape(x[0])
        if any(shape(v) != sub for v in x): raise ValueError('非矩形数组')
        return [len(x), *sub]
    dims = shape(nested)
    if dims == [0]: dims = []
    if len(dims) > 6: raise ValueError('数组维数超限')
    if bounds and [hi-lo+1 for lo, hi in bounds] != dims: raise ValueError('数组维度与下界不符')
    def flatten(x):
        for item in x:
            if isinstance(item, list): yield from flatten(item)
            else: yield item
    return dims, [lo for lo, _ in bounds] if bounds else [1]*len(dims), list(flatten(nested))


def scalar(kind, raw):
    if not isinstance(raw, str): raise ValueError('PG原值必须为文本或SQL NULL')
    if '\x00' in raw: raise ValueError('PG文本输出不能含NUL字节')
    if kind in INTS:
        if not re.fullmatch(r'-?\d+', raw): raise ValueError('整数类型不符')
        value = int(raw); low, high = INTS[kind]
        if not low <= value <= high: raise ValueError('整数溢出')
        return value
    if kind == 'numeric':
        if not re.fullmatch(r'(?:-?\d+(?:\.\d+)?|NaN|Infinity|-Infinity)', raw): raise ValueError('numeric类型不符')
        Decimal(raw)  # 永不通过float；包括超过decimal128/256精度的值。
        return raw
    if kind in ('float4', 'float8'):
        if not re.fullmatch(r'(?:-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|NaN|Infinity|-Infinity)', raw): raise ValueError('浮点类型不符')
        special = raw in ('NaN','Infinity','-Infinity')
        try:
            value = float(raw)
            if kind == 'float4': value = struct.unpack('>f',struct.pack('>f',value))[0]
        except OverflowError as exc:
            raise ValueError('有限浮点溢出') from exc
        if not special and (not math.isfinite(value) or (value == 0 and Decimal(raw) != 0)):
            raise ValueError('有限浮点溢出或非零下溢')
        return value
    if kind == 'bool':
        if raw not in ('t', 'f', 'true', 'false'): raise ValueError('bool类型不符')
        return raw in ('t', 'true')
    if kind == 'bytea':
        if not re.fullmatch(r'\\x(?:[0-9a-f]{2})*', raw): raise ValueError('bytea类型不符')
        return bytes.fromhex(raw[2:])
    if kind in ('json', 'jsonb'):
        json.loads(raw, parse_float=Decimal, parse_int=Decimal,
                   parse_constant=lambda _: (_ for _ in ()).throw(ValueError('非法JSON常量')))
        return raw  # SQL NULL在外层；JSON null是非空字符串，JSON原文不规范化。
    if kind == 'interval': return interval(raw)
    if kind in ('date', 'timestamp', 'timestamptz'):
        if raw in ('infinity', '-infinity'): return {'state': raw, 'finite': None}
        pattern = r'\d{4}-\d{2}-\d{2}' if kind == 'date' else r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?' + (r'\+00(?::00)?' if kind == 'timestamptz' else '')
        if not re.fullmatch(pattern, raw): raise ValueError('非冻结时间格式或精度/时区不符')
        try:
            value = dt.datetime.fromisoformat(raw+':00' if kind == 'timestamptz' and raw.endswith('+00') else raw) if kind != 'date' else dt.datetime.combine(dt.date.fromisoformat(raw), dt.time())
        except ValueError as exc: raise ValueError('时间超出v1可计算范围，拒绝导入') from exc
        if (value.tzinfo is not None) != (kind == 'timestamptz'): raise ValueError('时间时区语义不符')
        return {'state': 'finite', 'finite': value.date() if kind == 'date' else value}
    if kind in TEXT: return raw
    raise ValueError('未支持类型')


def scalar_type(kind):
    if kind in INTS: return pa.int64()
    if kind == 'float4': return pa.float32()
    if kind == 'float8': return pa.float64()
    if kind == 'bool': return pa.bool_()
    if kind == 'bytea': return pa.binary()
    if kind == 'interval': return pa.struct([('months', pa.int32()), ('days', pa.int32()), ('micros', pa.int64())])
    if kind in ('date', 'timestamp', 'timestamptz'):
        native = pa.date32() if kind == 'date' else pa.timestamp('us', tz='UTC' if kind == 'timestamptz' else None)
        return pa.struct([('state', pa.string()), ('finite', native)])
    return pa.string()


def column_type(engine, col):
    if engine == 'sqlite':
        return pa.struct([('storage_class', pa.string()), ('integer', pa.int64()), ('real_bits', pa.binary()), ('real', pa.float64()), ('bytes', pa.binary())])
    kind = pg_kind(col); value = projected_type(col)
    if col['array']:
        value = pa.struct([('dimensions', pa.list_(pa.int32())), ('lower_bounds', pa.list_(pa.int32())), ('items', pa.list_(value))])
    return pa.struct([('raw', pa.string()), ('value', value)])


def typed(engine, col, raw):
    if engine == 'sqlite':
        if not isinstance(raw, dict) or set(raw) != {'storage_class', 'value'}: raise ValueError('SQLite cell不完整')
        kind, value = raw['storage_class'], raw['value']
        result = dict(storage_class=kind, integer=None, real_bits=None, real=None, bytes=None)
        if kind == 'null':
            if value is not None: raise ValueError('NULL类型不符')
        elif kind == 'integer': result['integer'] = scalar('int8', value)
        elif kind == 'real':
            if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{16}', value): raise ValueError('real位表示不符')
            result['real_bits'] = bytes.fromhex(value)
            result['real'] = struct.unpack('>d', result['real_bits'])[0]
        elif kind in ('text', 'blob'):
            if not isinstance(value, str) or not re.fullmatch('(?:[0-9a-f]{2})*', value): raise ValueError('字节类型不符')
            result['bytes'] = bytes.fromhex(value)
        else: raise ValueError('未知SQLite storage class')
        if col['not_null'] and kind == 'null': raise ValueError('原NOT NULL不符')
        return result
    kind = pg_kind(col)
    if raw is None:
        if col['not_null']: raise ValueError('原NOT NULL不符')
        return None
    if col['array']:
        dims, lower, items = parse_array(raw)
        value = {'dimensions': dims, 'lower_bounds': lower, 'items': [None if item is None else project_scalar(col, item) for item in items]}
    else: value = project_scalar(col, raw)
    return {'raw': raw, 'value': value}


def untyped(engine, cell):
    if engine == 'postgres': return None if cell is None else cell['raw']
    kind = cell['storage_class']
    value = str(cell['integer']) if kind == 'integer' else cell['real_bits'].hex() if kind == 'real' else cell['bytes'].hex() if kind in ('text', 'blob') else None
    return {'storage_class': kind, 'value': value}


def arrow_schema(engine, table):
    fields = [pa.field('_ordinal', pa.int64(), nullable=False), pa.field('_block', pa.int64(), nullable=False),
              pa.field('_row', pa.int64(), nullable=False), pa.field('_sha256', pa.string(), nullable=False)]
    for i, col in enumerate(table['columns']):
        fields.append(pa.field('c'+str(i), column_type(engine, col), metadata={b'original': canonical(col), b'rule': RULE.encode()}))
    return pa.schema(fields, metadata={b'original_table': canonical(table), b'rule': RULE.encode()})


def numeric_shape(col):
    modifier = col.get('typmod', -1)
    if pg_kind(col) == 'numeric' and modifier >= 4:
        precision = ((modifier-4) >> 16) & 65535
        scale = (modifier-4) & 65535
        if 1 <= precision <= 38 and 0 <= scale <= precision: return precision, scale
    return None

def projected_type(col):
    shape = numeric_shape(col)
    if shape:
        return pa.struct([('state', pa.string()), ('finite', pa.decimal128(*shape))])
    return scalar_type(pg_kind(col))

def project_scalar(col, raw):
    result = scalar(pg_kind(col), raw)
    if pg_kind(col) == 'numeric' and col.get('typmod',-1) >= 4:
        modifier = col['typmod']-4; precision = (modifier >> 16) & 65535; scale = modifier & 65535
        number = Decimal(raw)
        if not 1 <= precision <= 1000 or not 0 <= scale <= precision: raise ValueError('未支持numeric精度')
        if number.is_infinite(): raise ValueError('有限typmod的numeric不允许Infinity')
        if number.is_finite():
            _, digits, exponent = number.as_tuple()
            if exponent != -scale or (number != 0 and len(digits)+exponent > precision-scale): raise ValueError('numeric值与原精度不符')
    if numeric_shape(col):
        number = Decimal(raw)
        return {'state': 'finite' if number.is_finite() else raw, 'finite': number if number.is_finite() else None}
    return result
