"""H1有界JSON视图；原词法/有序重键与用于既有规则的唯一字段分开。"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import re
from zoneinfo import ZoneInfo

from data_pipeline.history.event_collection.json_tokens import number_parts


@dataclass(frozen=True)
class Number:
    text: str


@dataclass(frozen=True)
class Object:
    members: tuple

    def get(self, key, default=...):
        values = [v for k, v in self.members if k == key]
        if len(values) > 1:
            raise ValueError('H1语义字段重键: ' + key)
        if not values:
            if default is ...:
                raise ValueError('H1缺少语义字段: ' + key)
            return default
        return values[0]


def loads(raw):
    return json.loads(raw, object_pairs_hook=lambda p: Object(tuple(p)),
                      parse_int=Number, parse_float=Number,
                      parse_constant=lambda x: (_ for _ in ()).throw(ValueError('H1非法数值')))


def canonical(value):
    """源anomaly-record规范键顺序，数字仍是原词法，永不经float。"""
    if isinstance(value, Object):
        keys = [k for k, _ in value.members]
        if len(keys) != len(set(keys)):
            raise ValueError('源content_version规范无法无损解释重键；仅保留结构资格')
        return '{' + ','.join(json.dumps(k, ensure_ascii=False) + ':' + canonical(v)
                              for k, v in sorted(value.members)) + '}'
    if isinstance(value, list):
        return '[' + ','.join(canonical(v) for v in value) + ']'
    if isinstance(value, Number):
        return value.text
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':'))


def native(value, *, tags=False):
    """仅对要解释的字段调用；重键拒绝，非整数精确Decimal而不是float。"""
    if isinstance(value, Object):
        result = {k: native(value.get(k), tags=tags) for k, _ in value.members}
        if tags and '$anomaly_scalar' in result:
            if set(result) != {'$anomaly_scalar', 'value'}:
                raise ValueError('H1 scalar tag字段冲突')
            kind, v = result['$anomaly_scalar'], result['value']
            if kind == 'decimal' and isinstance(v, str):
                d = Decimal(v)
                if d.is_finite():
                    return d
            elif kind == 'datetime' and isinstance(v, str):
                if not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?', v):
                    raise ValueError('H1 datetime tag精度/偏移不支持')
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            elif kind == 'timedelta_us' and type(v) is int:
                return timedelta(microseconds=v)
            raise ValueError('H1 scalar tag类型不支持')
        return result
    if isinstance(value, list):
        return [native(v, tags=tags) for v in value]
    if isinstance(value, Number):
        return int(value.text) if re.fullmatch(r'-?(?:0|[1-9]\d*)', value.text) else Decimal(value.text)
    return value


def equal(left, right):
    """解释字段严格区分bool/int及容器类型；不让Python的True==1掩盖冲突。"""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(equal(left[k], right[k]) for k in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(equal(a, b) for a, b in zip(left, right))
    return left == right


def wire(value):
    """可序列化的无损文档视图；对象members保持顺序、重复、缺失与null。"""
    if isinstance(value, Object):
        return {'kind': 'object', 'members': [[k, wire(v)] for k, v in value.members]}
    if isinstance(value, Number):
        return {'kind': 'number', 'lexeme': value.text}
    if isinstance(value, list):
        return {'kind': 'array', 'items': [wire(v) for v in value]}
    return {'kind': 'null' if value is None else 'bool' if type(value) is bool else 'string', 'value': value}


def scalars(value, limits, zone, path=()):
    if isinstance(value, Object):
        if value.get('$anomaly_scalar', None) is not None:
            decoded = native(value, tags=True)
            kind = native(value.get('$anomaly_scalar'))
            original = value.get('value')
            text = original.text if isinstance(original, Number) else original
            utc = None
            micros = None
            numeric = {}
            if kind in ('decimal', 'timedelta_us'):
                numeric_text = str(decoded) if kind == 'decimal' else text
                parts = number_parts(numeric_text.encode(), limits)
                numeric = {k: parts[k] for k in ('sign', 'coefficient_digits', 'exponent10')}
                numeric.update(negative_zero=int(parts['negative_zero']), integer_value=parts['int64_value'])
            if isinstance(decoded, datetime):
                parsed = decoded if decoded.utcoffset() is not None else decoded.replace(tzinfo=ZoneInfo(zone))
                parsed = parsed.astimezone(timezone.utc)
                utc = parsed.isoformat()
                delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
                micros = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
            yield {'member_path': json.dumps(path, ensure_ascii=False), 'scalar_kind': kind,
                   'original_text': text, 'utc_time': utc, 'utc_microseconds': micros,
                   'interpreted_timezone': zone if kind == 'datetime' and decoded.utcoffset() is None else None,
                   **numeric,
                   'precision': 'microsecond' if kind == 'timedelta_us' else 'native_datetime_precision_unknown' if kind == 'datetime' else 'exact_decimal'}
        else:
            for i, (key, v) in enumerate(value.members):
                yield from scalars(v, limits, zone, (*path, [key, i]))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from scalars(v, limits, zone, (*path, i))
