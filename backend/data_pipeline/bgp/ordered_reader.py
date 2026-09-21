"""把 M2 Reader 的分离消息/元素批归并为唯一有序边界流。

公开入口 ordered(reader)；adapt(stream, binding, selected_sources) 供人工流及受控桥接。
此层不重新解析 MRT、不写库；调用方必须耗尽迭代器，不能把早停当完成。
"""
from dataclasses import asdict, fields, replace
from functools import lru_cache
import hashlib
import ipaddress
import json
import re
from threading import local
from typing import Iterable, Iterator

from data_pipeline.bgp.archive import message_reader as consumer
from data_pipeline.bgp.input.mrt_types import FieldFailure, HeaderEvidence, Interpretation, ParseStatus, ReadPolicy
from data_pipeline.bgp.record_types import Direction, Element, ElementPosition, Endpoint, Gap, GapScope, InputBinding, INTERPRETATION_VERSION, NATIVE_INTERPRETATION_VERSION, GAP_RULE_VERSION, MessageBoundary, MessagePosition, ParseCounts, RawReference, RawTime, ScopeKind, SourceBinding, SourceEnd, SourceQuality, SourceStart, OrderedItem, digest_metadata
from data_pipeline.bgp.archive.store import TABLES

# mrt-interpretation/v1 的已接受 M1 许可；不能从任意 reason 文本扩充。
_REJECTED = frozenset({'field_truncated', 'unparsed_tail', 'nlri_prefix_invalid',
                       'duplicate_attribute', 'bgp_marker_invalid', 'bgp_length_mismatch'})
_UNSUPPORTED = frozenset({'unsupported_nlri', 'unsupported_table_dump_v2_subtype',
                          'unsupported_mrt_type', 'unsupported_bgp4mp_subtype',
                          'unsupported_bgp_message_type'})
_SUBTYPES = (0, 1, 4, 5, 6, 7, 8, 9, 10, 11)
_DIRECTIONS = tuple(v.value for v in Direction)
_SHA256 = re.compile(r'[0-9a-f]{64}')
_ip_address = lru_cache(maxsize=4096)(ipaddress.ip_address)


def _require(test, text):
    if not test:
        raise ValueError(text)


def _integer(value, name, maximum=None, nullable=False):
    if value is None and nullable:
        return
    if not (type(value) is int and value >= 0 and (maximum is None or value <= maximum)):
        raise ValueError(name + ' 必须为范围内整数')


def _text(value, name, nullable=False):
    if not ((value is None and nullable) or (type(value) is str and bool(value))):
        raise ValueError(name + ' 必须为非空文本')


def _sha(value):
    _require(type(value) is str and _SHA256.fullmatch(value) is not None,
             '摘要必须为 SHA256 小写十六进制')


@lru_cache(maxsize=32)
def _field_names(cls):
    return frozenset(f.name for f in fields(cls))


def _keys(value, cls):
    if type(value) is not dict or value.keys() != _field_names(cls):
        raise ValueError(cls.__name__ + ' 字段不符合固定版本')


def _json_object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, 'interpretation JSON 重复字段')
        result[key] = value
    return result


class _InterpretationDecoder(local):
    """各线程复用解码器，保留逐条 JSON 与重复字段校验，不共享扫描工作态。"""
    def __init__(self):
        self.decode = json.JSONDecoder(object_pairs_hook=_json_object).decode


_interpretation_decoder = _InterpretationDecoder()


def decode_interpretation(raw: str, *, native_allowed=False) -> Interpretation:
    """严格解码实际 M1 JSON；不接受遗漏字段、未知版本/策略或残留头字段。"""
    _require(type(raw) is str, 'interpretation 必须为 M2 JSON 文本')
    try:
        meta = _interpretation_decoder.decode(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError('interpretation JSON 无效') from exc
    _keys(meta, Interpretation)
    native = meta['contract_version'] == NATIVE_INTERPRETATION_VERSION
    _require(meta['contract_version'] == INTERPRETATION_VERSION or native and native_allowed, '未知解释版本')
    try:
        status, policy = ParseStatus(meta['status']), ReadPolicy(meta['policy'])
    except (ValueError, TypeError) as exc:
        raise ValueError('未知解释状态或策略') from exc
    for name in ('continuation_allowed', 'frame_complete'):
        _require(type(meta[name]) is bool and meta[name], '不可继续或外框不完整')
    _integer(meta['next_record_offset'], 'next_record_offset')
    _text(meta['source_path'], 'source_path', nullable=True)
    _text(meta['reason_code'], 'reason_code', nullable=True)
    _require(meta['interpretation_level'] in
             ('frame', 'endpoint_header', 'bgp_header_only', 'route_elements', 'peer_table', 'state'),
             '未知解释层级')
    header = meta['header']
    _keys(header, HeaderEvidence)
    _require(header['endpoint_trust'] in ('unknown', 'complete_header'), '未知端点可信级别')
    _require(header['direction'] in _DIRECTIONS, '未知方向枚举')
    _integer(header['microsecond'], 'header.microsecond', 999999, nullable=True)
    _integer(header['bgp_type'], 'header.bgp_type', 255, nullable=True)
    endpoint_fields = ('peer_ip', 'peer_asn', 'local_ip', 'local_asn', 'interface', 'endpoint_afi')
    if header['endpoint_trust'] == 'complete_header':
        _require(header['endpoint_afi'] in (1, 2) and type(header['endpoint_afi']) is int,
                 '端点 AFI 无效')
        for name in ('peer_ip', 'local_ip'):
            _text(header[name], name)
            try:
                ip = _ip_address(header[name])
            except ValueError as exc:
                raise ValueError('可信端点地址无效') from exc
            _require(ip.version == (4 if header['endpoint_afi'] == 1 else 6), '端点地址族冲突')
        for name in ('peer_asn', 'local_asn'):
            _integer(header[name], name, 2**32-1)
        _integer(header['interface'], 'interface', 65535)
    else:
        _require(all(header[k] is None for k in endpoint_fields) and header['bgp_type'] is None,
                 '不可信头不能夹带端点或 BGP 类型')
    failure = meta['failure']
    if failure is not None:
        _keys(failure, FieldFailure)
        _text(failure['code'], 'failure.code')
        _text(failure['section'], 'failure.section')
        for name in set(failure) - {'code', 'section'}:
            _integer(failure[name], 'failure.' + name,
                     {'attribute_flags': 255, 'attribute_type': 255,
                      'attribute_declared_length': 65535}.get(name), nullable=True)
        _require(failure['code'] == meta['reason_code'] and failure['section'] != 'mrt_timestamp',
                 '诊断引用冲突或 ET 时间失败')
        failure = FieldFailure(**failure)
    if native:
        # 原生诊断的粒度由 libbgpdump 决定，不伪造旧 parser 的字段级 failure。
        _require(failure is None, '原生诊断不得夹带旧解析器 failure')
        if status == ParseStatus.REJECTED:
            _require(policy == ReadPolicy.ISOLATE_PAYLOAD and meta['reason_code']=='bgpdump_payload_rejected', '未知原生拒绝原因或策略')
        elif status == ParseStatus.UNSUPPORTED:
            _require(meta['reason_code'] in ('unsupported_bgp4mp_subtype','unsupported_endpoint_afi',
                'bgpdump_route_refresh_not_implemented','unsupported_bgp_message_type','unsupported_nlri_family'), '未知原生不支持原因')
        else:_require(meta['reason_code'] is None, '原生 decoded 不得有失败原因')
    elif status == ParseStatus.DECODED:
        _require(meta['reason_code'] is None and failure is None, 'decoded 不得有失败原因')
    elif status == ParseStatus.REJECTED:
        _require(policy == ReadPolicy.ISOLATE_PAYLOAD and failure is not None and
                 meta['reason_code'] in _REJECTED, '拒绝原因不在 M1 隔离许可中')
    else:
        _require(meta['reason_code'] in _UNSUPPORTED, '未知 unsupported 原因')
        _require((meta['reason_code'] == 'unsupported_nlri') == (failure is not None),
                 'unsupported 诊断形态不符')
    return Interpretation(**{**meta, 'status': status, 'policy': policy,
                              'header': HeaderEvidence(**header), 'failure': failure})


def binding_from_reader(reader: consumer.ObservationReader) -> InputBinding:
    """取已构造 observation Reader 的固定元数据；实际资格复验仍由 stream 执行。"""
    selection = reader.selection
    _require(selection is not None, 'M3 共用入口只接受显式 observation Reader')
    seal, manifest = selection.seal, selection.manifest
    _require(seal['qualification'] == 'observation_sealed' and seal['business'] == 'not_run',
             '必须绑定观察封存阶段')
    _require(seal['version'] == 'observation-checkpoint/v1', '未知观察合同版本')
    _require((reader.run_id, reader.snapshot) == (seal['run_id'], seal['snapshot']), 'Reader 绑定漂移')
    entries = manifest['inputs']
    ids = tuple(e['source_id'] for e in entries)
    # M2 plan_for 已经通过 normalized_inputs 按首次出现去除本地路径别名。
    # 使用封存后的完整 MRT 序；不把 UPDATE 子序列误当全部来源，也不再次去重。
    _require(bool(ids) and len(set(ids)) == len(ids), '封存来源重复或为空；必须使用 M2 规范清单')
    _require(ids[0] == manifest['baseline_source'] and entries[0]['role'] == 'baseline'
             and sum(e['role'] == 'baseline' for e in entries) == 1, '基线角色或位置不符')
    _require(tuple(e['source_id'] for e in entries if e['role'] == 'update') ==
             tuple(manifest['update_sources']), 'UPDATE 角色子序列不符')
    checkpoint_ids = tuple(cp['source_id'] for cp in selection.checkpoints)
    _require(len(set(checkpoint_ids)) == len(checkpoint_ids), 'checkpoint 来源身份重复')
    _require(tuple(s for s in checkpoint_ids if s in ids) == ids, 'MRT checkpoint 缺失或顺序不符')
    checkpoints = {cp['source_id']: cp for cp in selection.checkpoints}
    sources = []
    for entry in entries:
        cp = checkpoints[entry['source_id']]
        _require(cp['source_sha'] == entry['sha256'] and cp['raw'] == 'verified_source_eof'
                 and cp['ingest'] == 'complete', '来源 checkpoint 未完成或身份不符')
        counts = cp['counts']
        sources.append(SourceBinding(entry['source_id'], entry['sha256'], entry['role'], cp['digest'],
                                     *(counts[k] for k in ('messages', 'elements', 'decoded', 'rejected', 'unsupported'))))
    binding = InputBinding(manifest['collector'], reader.run_id, reader.snapshot,
                           seal['plan_id'], seal['seal_id'], seal['digest'], tuple(sources),
                           interpretation_version=NATIVE_INTERPRETATION_VERSION if getattr(selection,'plan',{}).get('native') else INTERPRETATION_VERSION)
    _validate_binding(binding, tuple(reader.sources))
    return binding


def _validate_binding(binding, selected):
    _require(type(binding) is InputBinding and binding.profile == 'observation' and
             binding.schema_version == 'observation-checkpoint/v1' and
             binding.interpretation_version in (INTERPRETATION_VERSION,NATIVE_INTERPRETATION_VERSION) and
             binding.ordered_version == 'ordered-observation/v1', '输入绑定版本不符')
    for name in ('collector', 'observation_run', 'seal_id'):
        _text(getattr(binding, name), name)
    for value in (binding.plan_id, binding.seal_digest):
        _sha(value)
    _integer(binding.seal_snapshot, 'seal_snapshot')
    ids = binding.ordered_source_ids
    _require(bool(ids) and len(set(ids)) == len(ids), '完整来源为空或重复')
    _require(bool(selected) and len(set(selected)) == len(selected) and
             tuple(s for s in ids if s in selected) == selected, '子集不在完整绑定序或重编号')
    for rank, source in enumerate(binding.sources):
        _text(source.source_id, 'source_id')
        _sha(source.content_sha256)
        _sha(source.checkpoint_digest)
        _require(source.role == 'baseline' if rank == 0 else source.role in ('snapshot', 'update'), '来源角色不符')
        for name in ('messages', 'elements', 'decoded', 'rejected', 'unsupported'):
            _integer(getattr(source, name), name)
        _require(source.messages == source.decoded + source.rejected + source.unsupported,
                 '解释分类与消息数不符')


@lru_cache(maxsize=32)
def _row_validator(columns, extra=()):
    """固定列合同只编译一次；逐行仍核对完整字段集合及每个值的实际类型。"""
    names = frozenset(n for n, _ in columns) | frozenset(extra)
    types = {'VARCHAR': str, 'BIGINT': int, 'INTEGER': int, 'BOOLEAN': bool, 'BLOB': bytes}
    checks = tuple((name, types[sql_type]) for name, sql_type in columns)

    def validate(row):
        if type(row) is not dict or row.keys() != names:
            raise ValueError('观察行字段不符合固定 Reader 合同')
        for name, expected in checks:
            value = row[name]
            if value is not None and type(value) is not expected:
                raise ValueError('观察字段类型错误: ' + name)
    return validate


def _row_types(row, columns, extra=()):
    _row_validator(tuple(columns), tuple(extra))(row)


_MESSAGE_COLS = TABLES['messages'] + [('interpretation', 'VARCHAR')]
_ELEMENT_COLS = list(dict(TABLES['elements'] + TABLES['paths'] +
    [(n, t) for n, t in TABLES['messages'] if n in
     ('source_id', 'content_sha256', 'record', 'epoch', 'microsecond', 'mrt_type',
      'mrt_subtype', 'local_message', 'local_ip', 'local_asn', 'interface')]).items())


def _boundary(row, binding, rank, source):
    _row_types(row, _MESSAGE_COLS, ('eor', 'quality', 'peers'))
    _integer(row['record'], 'record')
    _require(row['source_id'] == source.source_id and row['content_sha256'] == source.content_sha256
             and row['message_id'] == f'{source.source_id}:{row["record"]}', '消息身份/引用不符')
    for name in ('offset', 'length', 'epoch', 'mrt_type', 'mrt_subtype'):
        _integer(row[name], name)
    _require(row['length'] >= 12, 'MRT 长度不足外框')
    _sha(row['raw_digest'])
    _integer(row['microsecond'], 'microsecond', 999999, nullable=True)
    for name in ('eor', 'quality', 'peers'):
        _require(type(row[name]) is list and all(type(v) is dict for v in row[name]), '消息附属观察类型错误')
    interp = decode_interpretation(row['interpretation'],native_allowed=binding.interpretation_version==NATIVE_INTERPRETATION_VERSION)
    native=interp.contract_version==NATIVE_INTERPRETATION_VERSION
    _require(row['kind'] in ('peer_index_table','rib','state_change','open','update','notification',
        'keepalive','route_refresh','unsupported','unsupported_bgp_message') or
        native and row['kind']=='unknown' and interp.status!=ParseStatus.DECODED, '未知消息 kind')
    if native:
        _require(row['mrt_type'] in (16,17) and row['reason']==interp.reason_code, '原生诊断与观察正文不符')
        if interp.status!=ParseStatus.DECODED:
            _require(row['kind']=='unknown' and all(row[k] is None for k in
                ('peer_ip','peer_asn','local_ip','local_asn','interface','local_message')), '原生受限帧夹带未完成字段')
            _require(any(q.get('code')==interp.reason_code for q in row['quality']), '原生诊断缺少持久化质量记录')
    _require(interp.next_record_offset == row['offset'] + row['length'], '解释下一外框位置冲突')
    microsecond = interp.header.microsecond
    if row['mrt_type'] == 17:
        # M1 原子失败只提交 interpretation；顶层原始 microsecond 可仍为 None。
        # 仅许可已支持 BGP4MP 的明确 payload 失败，不覆盖 decoded 或普通 unsupported。
        atomic_failure = (interp.failure is not None and row['kind'] == 'unsupported'
                          and row['mrt_subtype'] in _SUBTYPES
                          and (interp.status == ParseStatus.REJECTED or
                               (interp.status == ParseStatus.UNSUPPORTED and
                                interp.reason_code == 'unsupported_nlri')))
        _require(microsecond is not None and
                 (row['microsecond'] == microsecond or
                  (row['microsecond'] is None and atomic_failure)), 'ET 可信时间冲突')
    else:
        _require(row['microsecond'] is None and microsecond is None, '非 ET 夹带微秒')
    header_level = ('bgp_header_only' if interp.header.bgp_type is not None else
                    'endpoint_header' if interp.header.endpoint_trust == 'complete_header' else 'frame')
    expected_level = ({'update': 'route_elements', 'rib': 'route_elements',
                       'peer_index_table': 'peer_table', 'state_change': 'state'}.get(row['kind'], header_level)
                      if interp.status == ParseStatus.DECODED else header_level)
    _require(interp.interpretation_level == expected_level, '解释层级与完整解码/可信头不符')
    if interp.header.direction != 'unknown':
        _require(row['mrt_type'] in (16, 17) and row['mrt_subtype'] in _SUBTYPES and
                 interp.header.direction == ('local' if row['mrt_subtype'] in (6, 7, 10, 11) else 'received'),
                 '可信方向与帧类型冲突')
    if interp.failure is not None:
        for name in ('offset', 'attribute_offset'):
            offset = getattr(interp.failure, name)
            _require(offset is None or row['offset'] <= offset <= interp.next_record_offset,
                     '失败诊断偏移超出原消息')
    if interp.status != ParseStatus.DECODED:
        _require(row['kind'] == 'unsupported' or interp.failure is None, '拒绝帧夹带解码 kind')
        _require(not row['eor'] and not row['peers'] and row['old_state'] is None and
                 row['new_state'] is None, '受限帧夹带 EOR/STATE/Peer 表')
        if interp.status == ParseStatus.REJECTED:
            _require(row['mrt_type'] in (16, 17) and row['mrt_subtype'] in _SUBTYPES,
                     '非已支持 BGP4MP 不允许拒绝隔离')
    if row['kind'] != 'state_change':
        _require(row['old_state'] is None and row['new_state'] is None, '非 STATE 夹带状态字段')
    if row['kind'] == 'state_change':
        _require(interp.status == ParseStatus.DECODED and row['mrt_type'] in (16, 17) and
                 row['mrt_subtype'] in (0, 5), 'STATE 必须为真实状态帧')
        for name in ('old_state', 'new_state'):
            _integer(row[name], name, 65535)
    for quality in row['quality']:
        _require(set(quality) == {'code', 'detail'} and
                 all(type(v) is str for v in quality.values()), '消息 quality 字段错误')
    for eor in row['eor']:
        _require(set(eor) == {'afi', 'safi'} and row['kind'] == 'update', 'EOR 引用错误')
        _integer(eor['afi'], 'EOR AFI', 65535)
        _integer(eor['safi'], 'EOR SAFI', 255)
    position = MessagePosition(rank, row['record'])
    raw_time = RawTime(row['epoch'], microsecond)
    gap = None
    if interp.status != ParseStatus.DECODED:
        header = interp.header
        endpoint = (Endpoint(header.peer_ip, header.peer_asn, header.local_ip,
                             header.local_asn, header.interface)
                    if header.endpoint_trust == 'complete_header' else None)
        direction = Direction(header.direction)
        kind = (ScopeKind.LOCAL_OBSERVATION if direction == Direction.LOCAL else
                ScopeKind.RECEIVED_ENDPOINT if direction == Direction.RECEIVED and endpoint else
                ScopeKind.COLLECTOR_CHAIN)
        scope = GapScope(kind, binding.collector, binding.binding_id, endpoint,
                         endpoint.peer_asn if endpoint else None,
                         unknown_peer_covers_unseen_asns=endpoint is None)
        raw_ref = RawReference(*(row[k] for k in ('source_id', 'content_sha256', 'record',
                                                  'offset', 'length', 'raw_digest')))
        # 路径是定位提示，不让本地缓存路径改变相同解释的自然身份。
        interpretation_digest = digest_metadata(replace(interp, source_path=None))
        natural = json.dumps([GAP_RULE_VERSION, asdict(raw_ref), interpretation_digest],
                             sort_keys=True, separators=(',', ':'))
        gap = Gap(hashlib.sha256(natural.encode()).hexdigest(), binding.binding_id,
                  row['message_id'], position, raw_time, scope, direction, interp.status,
                  interp.reason_code, interpretation_digest, raw_ref, header, interp)
    return MessageBoundary(binding.binding_id, position, raw_time, row, interp, gap)


def ordered(reader: consumer.ObservationReader) -> Iterator[OrderedItem]:
    """生产调用入口：复用 reader.stream 的封印、表选择、来源尾核验。"""
    binding = binding_from_reader(reader)
    yield from adapt(reader.stream(), binding, tuple(reader.sources))


def adapt(
    stream: Iterable[consumer.SourceStart | consumer.MessageBatch | consumer.SourceEnd],
    binding: InputBinding, selected_sources: Iterable[str] | None = None,
) -> Iterator[OrderedItem]:
    """有界归并：只持有当前批与当前消息，不缓存整个来源；纯人工流也可验证。"""
    yield from _adapt(stream,binding,selected_sources,validate_binding=_validate_binding)


class SourceCursor:
    """同一文件的有界顺序校验；候选计数只能由实际文件 checkpoint 收尾。"""
    def __init__(self, binding, rank, source, start, *, provisional=False):
        self.binding, self.rank, self.source = binding, rank, source
        self.provisional = provisional
        self.element_columns = _ELEMENT_COLS
        if provisional:
            _require(binding.profile == 'route-batch-candidate/v1' and
                     start.snapshot is None and start.expected_messages is None and
                     start.expected_elements is None, '未完成输入必须显式标为候选')
        else:
            for name in ('expected_messages', 'expected_elements'):
                _integer(getattr(start, name), name)
            _require((start.expected_messages, start.expected_elements) ==
                     (source.messages, source.elements), '来源回执不符')
        _require((start.source_id, start.content_sha256, start.role) ==
                 (source.source_id, source.content_sha256, source.role), '来源回执不符')
        self.record = self.ordinal = -1
        self.current = None
        self.counts = dict(messages=0, elements=0, state_messages=0, eor_records=0,
                           local_messages=0, quality_records=0)
        self.parse = dict(decoded=0, rejected=0, unsupported=0, gaps=0)

    def batch(self, item):
        binding, rank, source = self.binding, self.rank, self.source
        record, ordinal, current = self.record, self.ordinal, self.current
        counts, parse = self.counts, self.parse
        validate_element = _row_validator(tuple(self.element_columns))
        _require(item.source_id == source.source_id and
                 item.run_id == binding.observation_run and item.snapshot == binding.seal_snapshot,
                 '批次来源或输入版本漂移')

        _integer(item.byte_count, 'byte_count')
        _require(all(type(v) is tuple for v in (item.messages, item.elements, item.source_quality)),
                 'MessageBatch 必须为原始 tuple 批')
        for quality in item.source_quality:
            _row_types(quality, TABLES['quality'])
            _require(quality['source_id'] == source.source_id and quality['message_id'] is None,
                     '来源 quality 不得伪造消息位置')
            counts['quality_records'] += 1
            yield SourceQuality(binding.binding_id, rank, quality)
        mi = ei = 0
        # 两数组分别已经有序，按 record 归并；不排序来掩盖非法输入。
        while mi < len(item.messages) or ei < len(item.elements):
            message = item.messages[mi] if mi < len(item.messages) else None
            element = item.elements[ei] if ei < len(item.elements) else None
            if message is not None:
                _require(type(message) is dict, '消息不是字典')
                _integer(message.get('record'), 'message.record')
            if element is not None:
                _require(type(element) is dict, '元素不是字典')
                _integer(element.get('record'), 'element.record')
            if message is not None and (element is None or message['record'] <= element['record']):
                _require(message['record'] == record + 1, '消息重复、遗漏或乱序')
                boundary = _boundary(message, binding, rank, source)
                if current is not None:
                    _require(message['offset'] == current.raw['offset'] + current.raw['length'],
                             '消息物理外框不连续')
                else:
                    _require(message['offset'] == 0, '首条消息偏移不是零')
                record, ordinal, current = message['record'], -1, boundary
                counts['messages'] += 1
                counts['state_messages'] += message['kind'] == 'state_change'
                counts['local_messages'] += bool(message['local_message'])
                counts['eor_records'] += len(message['eor'])
                counts['quality_records'] += len(message['quality'])
                parse[boundary.interpretation.status.value] += 1
                parse['gaps'] += boundary.gap is not None
                mi += 1
                yield boundary
            else:
                validate_element(element)
                _require(current is not None and element['record'] == record and
                         current.gap is None and current.raw['kind'] in ('update', 'rib'),
                         '元素缺消息头、乱序或受限帧残留')
                for name in ('source_id', 'content_sha256', 'message_id', 'epoch', 'microsecond',
                             'mrt_type', 'mrt_subtype', 'local_message', 'local_ip', 'local_asn', 'interface'):
                    _require(element[name] == current.raw[name], '元素消息引用不符: ' + name)
                _integer(element['ordinal'], 'ordinal')
                _require(element['ordinal'] == ordinal + 1 and
                         element['event_id'] == f'{current.raw["message_id"]}:{element["ordinal"]}',
                         '元素 ordinal 或事件引用不符')
                _text(element['path_key'], 'path_key')
                _require(element['action'] in ('announce', 'withdraw', 'rib_snapshot') and
                         (element['action'] == 'rib_snapshot') == (current.raw['kind'] == 'rib'),
                         '路由 action 与消息种类不符')
                _require(type(element['path_id_present']) is bool, '缺少 ADDPATH 存在标志')
                _integer(element['path_id'], 'path_id', 2**32-1,
                         nullable=not element['path_id_present'])
                _require(element['path_id_present'] or element['path_id'] is None,
                         '不存在的 ADDPATH 槽不能有值')
                ordinal = element['ordinal']
                counts['elements'] += 1
                ei += 1
                yield Element(binding.binding_id, ElementPosition(current.position, ordinal), element)

        self.record, self.ordinal, self.current = record, ordinal, current

    def end(self, item, completed=None):
        source = completed if completed is not None else self.source
        _require(item.source_id == self.source.source_id and item.run_id == self.binding.observation_run,
                 '来源尾身份不符')
        _require(source.source_id == self.source.source_id and
                 source.content_sha256 == self.source.content_sha256, '完成来源身份不符')
        _require(all(type(getattr(item, k)) is int and getattr(item, k) == v
                     for k, v in self.counts.items()), 'SourceEnd 计数不符')
        _require((self.counts['messages'], self.counts['elements']) ==
                 (source.messages, source.elements) and
                 all(self.parse[k] == getattr(source, k) for k in ('decoded', 'rejected', 'unsupported')),
                 '来源完成与固定 checkpoint 不符')
        return SourceEnd(self.binding.binding_id, self.rank, item, ParseCounts(**self.parse))


def _adapt(stream,binding,selected_sources,*,validate_binding=None):
    """内部共用校验器；调用者先核验各自的封存或候选绑定合同。"""
    iterator = None
    primary = None
    pending_end = None
    try:
        selected = binding.ordered_source_ids if selected_sources is None else tuple(selected_sources)
        if validate_binding is not None:validate_binding(binding,selected)
        by_id = {s.source_id: (i, s) for i, s in enumerate(binding.sources)}
        cursor = None
        source_index = 0
        iterator = iter(stream)
        for item in iterator:
            _require(isinstance(item, (consumer.SourceStart, consumer.MessageBatch, consumer.SourceEnd)),
                     '未知 Reader 流类型')
            _integer(item.snapshot, 'snapshot')
            _require((item.run_id, item.snapshot) == (binding.observation_run, binding.seal_snapshot),
                     '流输入版本漂移')
            if isinstance(item, consumer.SourceStart):
                _require(cursor is None and source_index < len(selected) and
                         item.source_id == selected[source_index], '来源开始乱序/重复')
                rank, source = by_id[item.source_id]
                cursor = SourceCursor(binding, rank, source, item)
                yield SourceStart(binding.binding_id, rank, item)
            else:
                _require(cursor is not None and item.source_id == cursor.source.source_id, '来源外批次/结束')
                if isinstance(item, consumer.SourceEnd):
                    end = cursor.end(item)
                    cursor = None
                    source_index += 1
                    if source_index == len(selected):pending_end = end
                    else:yield end
                else:
                    yield from cursor.batch(item)
        _require(cursor is None and source_index == len(selected), '流未收到所有来源 SourceEnd')
    except BaseException as exc:
        primary = exc
        raise
    finally:
        cleanup_errors = []
        closed_ids = set()
        # 迭代器与可迭代资源可能不同；只按对象身份去重，各尝试关闭一次。
        for resource in (iterator, stream):
            if resource is None or id(resource) in closed_ids:
                continue
            closed_ids.add(id(resource))
            try:
                close = getattr(resource, 'close', None)
                if close is not None:
                    close()
            except BaseException as exc:
                cleanup_errors.append(exc)
        if cleanup_errors:
            if primary is not None and not isinstance(primary, GeneratorExit):
                # 保留原异常对象/类型/回溯，同时暴露全部清理错误供回执记录。
                primary.cleanup_errors = (*getattr(primary, 'cleanup_errors', ()), *cleanup_errors)
            else:
                failure = cleanup_errors[0]
                failure.cleanup_errors = tuple(cleanup_errors)
                # close() 会吞掉 GeneratorExit，故早停清理失败必须显式抛出。
                raise failure from primary
    if pending_end is not None:
        yield pending_end
