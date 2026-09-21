"""UPDATE 的有界列批与消息边界；计算者必须逐元素交替推进状态和业务。"""
from bisect import bisect_left, bisect_right
from dataclasses import dataclass

from data_pipeline.bgp.archive import message_reader as consumer
from data_pipeline.bgp.input.native_batches import MESSAGE_FIELDS
from data_pipeline.bgp.ordered_reader import _row_validator
from data_pipeline.bgp.record_types import SourceQuality
from data_pipeline.bgp.state.rib_columns import checked_elements

CORE_FIELDS = ('action', 'afi', 'safi', 'prefix', 'path_id_present', 'path_id',
               'peer_ip', 'peer_asn', 'path_key', 'ordinal', 'bgp_id',
               'bgp_id_present', 'peer_table_record', 'peer_index')


@dataclass
class UpdateBatch:
    binding_ref: str
    columns: dict
    paths: dict
    frames: tuple  # (消息边界, 本批首元素下标, 尾后下标, 是否新消息)
    prepared_rows: list | None = None
    evidence: object = None  # 原始 Arrow 列；仅实际诊断、事件或慢路径按需展开。
    business_paths: dict | None = None
    business_features: tuple = ()

    def __len__(self):
        return len(self.columns.get('ordinal', ()))

    def row(self, index, boundary):
        if self.prepared_rows is not None:
            return self.prepared_rows[index]
        row = ({name: self.evidence[name][index].as_py() for name in self.evidence.column_names}
               if self.evidence is not None else
               {name: values[index] for name, values in self.columns.items()})
        row.update(self.paths[row['path_key']])
        row.update((name, boundary.raw[name]) for name in MESSAGE_FIELDS)
        row.setdefault('message_id', boundary.raw['message_id'])
        row.setdefault('event_id', f'{row["message_id"]}:{row["ordinal"]}')
        return row


def segments(stream, messages, elements, source_quality):
    elements, ends, mids = checked_elements(elements, ('announce', 'withdraw'))
    indices = {m['message_id']: i for i, m in enumerate(messages)}
    try:
        owners = [indices[mid] for mid in mids]
    except KeyError as exc:
        raise ValueError('UPDATE 元素缺少本段消息') from exc
    if any(a >= b for a, b in zip(owners, owners[1:])):
        raise ValueError('UPDATE 元素消息重复或乱序')
    # 即使元素批切在消息中间，也不移动下一条控制消息到这些元素之前。
    mi = ei = 0
    cap = min(stream.batch_rows, 1024)
    if cap < 1:
        raise ValueError('UPDATE 批次大小必须为正数')
    path_validator = _row_validator(tuple(stream.path_columns))
    direct_fields = getattr(stream, 'update_fields', False)
    fields = elements.column_names if stream.update_rows and not direct_fields else CORE_FIELDS
    while mi < len(messages) or ei < len(elements) or source_quality:
        message_stop = min(len(messages), mi + 1024)
        owned = bisect_left(owners, message_stop)
        available = ends[owned-1] if owned else 0
        stop = min(len(elements), ei + cap, available)
        if stop < available:
            message_stop = max(mi, owners[bisect_right(ends, stop-1)] + 1)
        boundaries = tuple(stream.cursor.batch(consumer.MessageBatch(
            stream.binding.observation_run, None, stream.entry['source_id'],
            tuple(messages[mi:message_stop]), (), 0, tuple(source_quality))))
        source_quality = []
        # SourceQuality 仍在消息前单独发出；不把它当作有位置的消息。
        for item in boundaries:
            if isinstance(item, SourceQuality):
                yield item
        fresh = {item.raw['message_id']: item for item in boundaries if not isinstance(item, SourceQuality)}
        frames = []
        run = bisect_right(ends, ei)
        first_owner = owners[run] if run < len(owners) and ei < stop else message_stop
        first = min(mi, first_owner)
        offset = ei
        for m in range(first, message_stop):
            boundary = fresh.get(messages[m]['message_id'])
            is_new = boundary is not None
            if boundary is None:
                boundary = stream.update_boundary
            count = min(ends[run], stop)-offset if run < len(ends) and owners[run] == m and offset < stop else 0
            if count and (boundary is None or boundary.raw['message_id'] != messages[m]['message_id']
                          or boundary.gap is not None or boundary.raw['kind'] != 'update'):
                raise ValueError('UPDATE 元素引用受限帧或非 UPDATE 消息')
            frames.append((boundary, offset-ei, offset-ei+count, is_new))
            offset += count
            if run < len(ends) and offset == ends[run]:
                run += 1
        if offset != stop:
            raise ValueError('UPDATE 列批消息关联不完整')
        columns = {name: elements[name].slice(ei, stop-ei).to_pylist() for name in fields}
        paths = stream.paths.prepare(columns['path_key'], max_result_bytes=64*1024**2)
        for path in paths.values():
            path_validator(path)
        batch = UpdateBatch(stream.binding.binding_id, columns, paths, tuple(frames),
                            evidence=elements.slice(ei, stop-ei) if direct_fields else None)
        if stream.update_rows and not direct_fields:
            rows = [batch.row(i, boundary) for boundary, begin, end, _ in frames for i in range(begin, end)]
            batch.prepared_rows = rows
        if frames:
            stream.update_boundary = frames[-1][0]
        stream.cursor.counts['elements'] += stop-ei
        if stop > ei and frames[-1][2] > frames[-1][1]:
            stream.cursor.ordinal = columns['ordinal'][-1]
        mi, ei = message_stop, stop
        yield batch
