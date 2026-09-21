"""C3 M3 显式24表schema：原21表编码逐字复用，新资格行单独版本化。"""
from dataclasses import asdict, replace
import hashlib

from data_pipeline.analysis.country_events import snapshot_schema as original
from data_pipeline.analysis.country_events.event_aggregation import C2Row
from data_pipeline.analysis.country_events.route_contract import CountryQualification, CountryCoverage, CountryQualifiedValue

SCHEMA_VERSION = 'country-component-m3/v1'
NEW_TABLES = {'country_qualification': CountryQualification,
              'country_coverage': CountryCoverage,
              'country_qualified_value': CountryQualifiedValue}
TABLES = {**original.TABLES, **NEW_TABLES}
SCHEMAS = {**original.SCHEMAS,
           **{table: original.COMMON + original.columns(cls) for table, cls in NEW_TABLES.items()}}
BY_CLASS = {cls: table for table, cls in NEW_TABLES.items()}


def row_encode(sequence, item):
    if not isinstance(item, C2Row) or type(item.value) not in BY_CLASS:
        return original.row_encode(sequence, item)
    if type(sequence) is not int or sequence < 0:
        raise ValueError('M3 全局行序号无效')
    incident, revision, value = item.incident_id, item.revision, item.value
    if (incident is None) != (revision is None) or revision is not None and (type(revision) is not int or revision < 1):
        raise ValueError('M3 外层事件修订无效')
    # 每次编码重验内容ID和有限类型，不能只相信首次构造。
    replace(value)
    if isinstance(value, CountryCoverage) and incident is not None:
        raise ValueError('M3 独立覆盖不能伪造事件')
    if isinstance(value, CountryQualification) and (incident, revision) != (value.incident_id, value.revision):
        raise ValueError('M3 资格内外事件修订冲突')
    table = BY_CLASS[type(value)]
    digest = hashlib.sha256(original.encode((SCHEMA_VERSION, sequence, incident, revision, table, asdict(value))).encode()).hexdigest()
    row = dict(_sequence=sequence, _incident_id=incident,
               _revision=str(revision) if revision is not None else None, _row_hash=digest)
    row.update(original.flatten(value))
    return table, row


def row_decode(table, row):
    if table in original.TABLES:
        return original.row_decode(table, row)
    if table not in NEW_TABLES or set(row) != {name for name, _, _ in SCHEMAS[table]}:
        raise ValueError('M3 资格表/列集合不符')
    value = original.inflate(NEW_TABLES[table], row)
    item = C2Row(row['_incident_id'], original.integer(row['_revision']) if row['_revision'] is not None else None, value)
    if row_encode(row['_sequence'], item) != (table, row):
        raise ValueError('M3 资格typed行摘要不符')
    return item
