"""RIB 候选基线的有界列批；消息控制沿用有序合同，原始列只在 Arrow 中核验。"""
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from data_pipeline.bgp.archive import message_reader as consumer
from data_pipeline.bgp.archive.store import TABLES, TYPES


FIELDS = ('afi', 'safi', 'prefix', 'path_id_present', 'path_id',
          'peer_ip', 'peer_asn', 'path_key', 'ordinal')


@dataclass(frozen=True)
class RibPrepared:
    columns: tuple
    paths: dict
    arrow: object
    token: object


@dataclass(frozen=True)
class RibBatch:
    binding_ref: str
    boundary: object
    columns: tuple
    paths: dict
    arrow: object = None
    prepared: object = None
    prepared_offset: int = 0

    def __len__(self):
        return len(self.columns[0])


def _all(test, reason):
    if not pc.all(pc.fill_null(test, False)).as_py():
        raise ValueError(reason)


def checked_elements(elements, actions):
    """原生列批共用元素合同；向量校验保留原行序和原始证据列。"""
    if elements is None:
        elements = pa.table({name: pa.array([], type=TYPES[typ]) for name, typ in TABLES['elements']})
    columns = TABLES['elements']
    if (set(elements.column_names) != {name for name, _ in columns}
            or len(elements.column_names) != len(columns)
            or any(elements.schema.field(name).type != TYPES[typ] for name, typ in columns)):
        raise ValueError('Arrow 列不符合元素合同')
    elements.validate(full=True)
    runs = pc.run_end_encode(elements['message_id'].combine_chunks())
    ends, mids = runs.run_ends.to_pylist(), runs.values.to_pylist()
    if len(elements):
        starts = pc.run_end_decode(pa.RunEndEncodedArray.from_arrays(
            runs.run_ends, pa.array([0, *ends[:-1]], type=pa.int64())))
        ordinal = pc.subtract(pa.array(range(len(elements)), type=pa.int64()), starts)
        _all(pc.equal(elements['ordinal'], ordinal), '元素 ordinal 不连续')
        _all(pc.equal(elements['event_id'], pc.binary_join_element_wise(
            elements['message_id'], pc.cast(ordinal, pa.string()), pa.scalar(':'))), '事件引用不符')
        _all(pc.is_in(elements['action'], value_set=pa.array(actions)), '列批 action 与文件角色不符')
        _all(pc.greater(pc.utf8_length(elements['path_key']), 0), '路径引用为空')
        present, pathid = elements['path_id_present'], elements['path_id']
        valid_id = pc.and_(pc.greater_equal(pathid, 0), pc.less_equal(pathid, 2**32-1))
        _all(pc.if_else(present, valid_id, pc.is_null(pathid)), 'ADD-PATH 存在标志或编号错误')
    return elements, ends, mids


def segments(stream, messages, elements, source_quality):
    """只用于完整消息边界的 RIB；不处理 UPDATE，也不改变其紧邻旧值消费。"""
    elements, ends, mids = checked_elements(elements, ('rib_snapshot',))
    mi = start = 0
    prepared_until = prepared_from = 0
    prepared = {}
    prepared_columns = ()
    prepared_batch = None
    for stop, mid in zip(ends, mids):
        end = mi
        while end < len(messages) and messages[end]['message_id'] != mid:
            end += 1
        if end == len(messages):
            raise ValueError('RIB 元素缺消息、重复或乱序')
        yield from stream.cursor.batch(consumer.MessageBatch(
            stream.binding.observation_run, None, stream.entry['source_id'],
            tuple(messages[mi:end+1]), (), 0, tuple(source_quality)))
        source_quality = []
        mi = end + 1
        boundary = stream.cursor.current
        if boundary.gap is not None or boundary.raw['kind'] != 'rib' or boundary.raw['local_message']:
            raise ValueError('RIB 列批引用非接收基线消息')
        batch_rows = min(stream.batch_rows, 8192)
        for offset in range(start, stop, batch_rows):
            count = min(batch_rows, stop-offset)
            if offset+count > prepared_until:
                prepared_from = offset
                prepared_until = min(len(elements), offset+8192)
                prepared_columns = tuple(elements[name].slice(offset, prepared_until-offset).to_pylist() for name in FIELDS)
                prepared = stream.paths.prepare(prepared_columns[7], max_result_bytes=64*1024**2)
                # 稀疏字典批量准备；不能变成每条消息一次磁盘查询。
                for path in prepared.values():
                    for name, typ in stream.path_columns:
                        value = path[name]
                        expected = {'VARCHAR': str, 'BIGINT': int, 'INTEGER': int, 'BOOLEAN': bool, 'BLOB': bytes}[typ]
                        if value is not None and type(value) is not expected:
                            raise ValueError('RIB 路径解释字段类型错误: '+name)
                prepared_batch = RibPrepared(prepared_columns, prepared,
                    elements.slice(offset, prepared_until-offset), object())
            values = tuple(column[offset-prepared_from:offset-prepared_from+count] for column in prepared_columns)
            paths = {key: prepared[key] for key in dict.fromkeys(values[7])}
            stream.cursor.ordinal = offset-start+count-1
            stream.cursor.counts['elements'] += count
            yield RibBatch(stream.binding.binding_id, boundary, values, paths, elements.slice(offset, count),
                           prepared_batch, offset-prepared_from)
        start = stop
    if mi < len(messages) or source_quality:
        yield from stream.cursor.batch(consumer.MessageBatch(
            stream.binding.observation_run, None, stream.entry['source_id'],
            tuple(messages[mi:]), (), 0, tuple(source_quality)))
