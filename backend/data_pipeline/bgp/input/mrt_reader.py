"""流式 MRT 解码：原消息与路由元素分开；不关联 Peer 或推断 Session。

原件定位是全部未解释字节的权威来源。路径归属复用新项目已验证规则。
"""
from dataclasses import dataclass, field, replace
import gzip
import hashlib
import ipaddress
import struct
from pathlib import Path

from data_pipeline.bgp.snapshots.origin import _interpret, _peers, InputRejected

from data_pipeline.bgp.input.mrt_types import ReadPolicy, ParseStatus, FieldFailure, HeaderEvidence, Interpretation

PARSER_VERSION = 'mrt-observation/v2'
# 只在已支持 BGP4MP 帧中允许这些本模块明确产生的数据错误。
ISOLATABLE_FAILURES = frozenset({
    'field_truncated', 'unparsed_tail', 'nlri_prefix_invalid',
    'duplicate_attribute', 'bgp_marker_invalid', 'bgp_length_mismatch',
})


class _UnsupportedPayload(InputRejected):
    def __init__(self, message, section, offset):
        super().__init__(message, code='unsupported_nlri')
        self.failure = FieldFailure('unsupported_nlri', section, offset)


class _PayloadRejected(InputRejected):
    def __init__(self, message, failure):
        super().__init__(message)
        self.failure = failure


def digest(data):
    return hashlib.sha256(data).hexdigest()


class Cursor:
    def __init__(self, data, *, offset=None, section='payload'):
        self.data, self.pos = data, 0
        self.offset, self.section = offset, section

    @property
    def absolute(self):
        return None if self.offset is None else self.offset + self.pos

    def reject(self, text, code, requested=None):
        raise _PayloadRejected(text, FieldFailure(code, self.section, self.absolute,
                                                  requested, len(self.data) - self.pos))

    def take(self, size):
        if size < 0 or self.pos + size > len(self.data):
            self.reject('MRT/BGP 字段截断', 'field_truncated', size)
        value = self.data[self.pos:self.pos + size]
        self.pos += size
        return value

    def uint(self, size):
        return int.from_bytes(self.take(size), 'big')

    def done(self):
        if self.pos != len(self.data):
            self.reject('MRT/BGP 未解释尾部', 'unparsed_tail')

    def __bool__(self):
        return self.pos < len(self.data)


def nlri(c, afi, safi, addpath=False):
    if afi not in (1, 2) or safi not in (1, 2):
        raise _UnsupportedPayload('未支持 NLRI AFI/SAFI', c.section, c.absolute)
    path_id = c.uint(4) if addpath else None
    bits = c.uint(1)
    maximum = 32 if afi == 1 else 128
    if bits > maximum:
        c.reject('NLRI prefix长度无效', 'nlri_prefix_invalid')
    raw = c.take((bits + 7) // 8)
    canonical = bytearray(raw.ljust(maximum // 8, b'\0'))
    if bits % 8:
        canonical[bits // 8] &= 255 << (8 - bits % 8)
    return {'afi': afi, 'safi': safi, 'prefix': f'{ipaddress.ip_address(bytes(canonical))}/{bits}',
            'raw_prefix': bytes([bits]) + raw, 'path_id': path_id,
            'path_id_present': addpath}


def path32(raw, width):
    if raw is None or width == 4:
        return raw
    c, result = Cursor(raw), bytearray()
    while c:
        kind, count = c.uint(1), c.uint(1)
        result.extend((kind, count))
        for _ in range(count):
            result.extend(c.uint(width).to_bytes(4, 'big'))
    return bytes(result)


def path_text(segments):
    marks = {1: ('{', '}'), 2: ('', ''), 3: ('(', ')'), 4: ('[', ']')}
    return ' '.join(marks[k][0] + (',' if k in (1, 4) else ' ').join(map(str, vs)) + marks[k][1]
                    for k, vs in segments)


def attributes(raw, width, *, offset=None):
    c, attrs, seen = Cursor(raw, offset=offset, section='attributes'), [], set()
    while c:
        start = c.pos
        flags = code = length = None
        try:
            flags = c.uint(1)
            code = c.uint(1)
            length = c.uint(2 if flags & 16 else 1)
            value = c.take(length)
            if code in seen:
                c.reject('重复BGP属性', 'duplicate_attribute')
        except _PayloadRejected as exc:
            exc.failure = replace(exc.failure, attribute_offset=None if offset is None else offset + start,
                                  attribute_flags=flags, attribute_type=code,
                                  attribute_declared_length=length)
            raise
        seen.add(code)
        attrs.append({'flags': flags, 'code': code, 'value': value,
                      'offset': start, 'length': c.pos - start})
    by_code = {a['code']: a['value'] for a in attrs}
    original, as4 = by_code.get(2), by_code.get(17)
    interpreted = _interpret(path32(original, width), as4)
    return attrs, {'path_key': digest(bytes([width]) + raw), 'attributes_raw': raw,
                   'attributes_digest': digest(raw), 'asn_width': width,
                   'as_path_raw': original, 'as4_path_raw': as4,
                   'as_path_text': path_text(interpreted['segments']),
                   'as4_path_text': path_text(_interpret(as4, None)['segments']) if as4 is not None else None,
                   **{k: interpreted[k] for k in ('raw_origin_asn', 'attributed_origin_asn', 'reason')}}


@dataclass
class Message:
    source_id: str
    record: int
    offset: int
    length: int
    epoch: int
    mrt_type: int
    mrt_subtype: int
    raw_digest: str
    content_sha256: str | None = None
    microsecond: int | None = None
    kind: str = 'unsupported'
    reason: str | None = None
    peer: dict = field(default_factory=dict)
    peers: list = field(default_factory=list)
    elements: list = field(default_factory=list)
    paths: list = field(default_factory=list)
    old_state: int | None = None
    new_state: int | None = None
    eor_families: list = field(default_factory=list)
    interpretation: Interpretation | None = None

    @property
    def message_id(self):
        return f'{self.source_id}:{self.record}'


def decode(message, body, peer_table, peer_table_record):
    """原子解释：失败仅提交结构化诊断，成功才提交全部 payload 产物。"""
    staged = replace(message, peer={}, peers=[], elements=[], paths=[], eor_families=[],
                     kind='unsupported', reason=None, old_state=None, new_state=None,
                     microsecond=None, interpretation=Interpretation(
                         ParseStatus.DECODED, ReadPolicy.STRICT, None, HeaderEvidence(),
                         next_record_offset=message.offset + message.length))
    try:
        result = _decode(staged, body, peer_table, peer_table_record)
    except InputRejected as exc:
        status = ParseStatus.UNSUPPORTED if isinstance(exc, _UnsupportedPayload) else ParseStatus.REJECTED
        failure = exc.failure if isinstance(exc, (_PayloadRejected, _UnsupportedPayload)) else FieldFailure(exc.code, exc.stage)
        message.interpretation = replace(staged.interpretation, status=status,
            reason_code=failure.code, failure=failure,
            continuation_allowed=isinstance(exc, _UnsupportedPayload),
            interpretation_level=_header_level(staged.interpretation.header))
        raise
    staged.interpretation = replace(staged.interpretation,
        status=ParseStatus.UNSUPPORTED if staged.reason else ParseStatus.DECODED,
        reason_code=staged.reason,
        interpretation_level=({'update': 'route_elements', 'rib': 'route_elements',
            'peer_index_table': 'peer_table', 'state_change': 'state'}.get(staged.kind,
                _header_level(staged.interpretation.header)) if not staged.reason
                else _header_level(staged.interpretation.header)))
    message.__dict__.update(staged.__dict__)
    return result


def _header_level(header):
    if header.bgp_type is not None:
        return 'bgp_header_only'
    if header.endpoint_trust == 'complete_header':
        return 'endpoint_header'
    return 'frame'


def _decode(message, body, peer_table, peer_table_record):
    c = Cursor(body, offset=message.offset + 12, section='mrt_body')
    if message.mrt_type == 13:
        subtype = message.mrt_subtype
        if subtype == 1:
            message.kind = 'peer_index_table'
            message.peers = _peers(body)
            for p in message.peers:
                p.update(bgp_id_present=True, ip_present=True, asn_present=True,
                         table_record=message.record, source_id=message.source_id)
            return message.peers, message.record
        if subtype not in (2, 3, 4, 5, 6, 8, 9, 10, 11, 12):
            message.reason = 'unsupported_table_dump_v2_subtype'
            return peer_table, peer_table_record
        message.kind = 'rib'
        c.uint(4)  # 原sequence仍保留在原件定位中
        generic = subtype in (6, 12)
        afi = c.uint(2) if generic else (1 if subtype in (2, 3, 8, 9) else 2)
        safi = c.uint(1) if generic else (1 if subtype in (2, 4, 8, 10) else 2)
        prefix = nlri(c, afi, safi)
        count = c.uint(2)
        addpath = subtype >= 8
        for ordinal in range(count):
            index, originated = c.uint(2), c.uint(4)
            path_id = c.uint(4) if addpath else None
            size = c.uint(2)
            attr_offset = c.pos
            raw = c.take(size)
            if index >= len(peer_table):
                raise InputRejected('RIB Peer索引越界或无Peer表')
            attrs, path = attributes(raw, 4, offset=message.offset + 12 + attr_offset)
            message.paths.append(path)
            message.elements.append({**prefix, 'path_id': path_id, 'path_id_present': addpath,
                'ordinal': ordinal, 'action': 'rib_snapshot', 'originated_epoch': originated,
                'peer': peer_table[index], 'path_key': path['path_key'],
                'attributes_offset': message.offset + 12 + attr_offset, 'attributes_length': size,
                'attributes_digest': path['attributes_digest']})
        c.done()
        return peer_table, peer_table_record
    if message.mrt_type not in (16, 17):
        message.reason = 'unsupported_mrt_type'
        return peer_table, peer_table_record
    if message.mrt_type == 17:
        c.section = 'mrt_timestamp'
        message.microsecond = c.uint(4)
        if message.microsecond > 999999:
            raise InputRejected('MRT微秒越界', code='invalid_mrt_timestamp')
        message.interpretation = replace(message.interpretation, header=HeaderEvidence(microsecond=message.microsecond))
        c.section = 'mrt_body'
    subtype = message.mrt_subtype
    if subtype not in (0, 1, 4, 5, 6, 7, 8, 9, 10, 11):
        message.reason = 'unsupported_bgp4mp_subtype'
        return peer_table, peer_table_record
    message.interpretation = replace(message.interpretation, header=replace(
        message.interpretation.header, direction='local' if subtype in (6, 7, 10, 11) else 'received'))
    width = 4 if subtype in (4, 5, 7, 9, 11) else 2
    peer_asn, local_asn, interface, afi = c.uint(width), c.uint(width), c.uint(2), c.uint(2)
    if afi not in (1, 2):
        raise _UnsupportedPayload('BGP4MP端点AFI未支持', 'endpoint_afi', c.absolute)
    addr_size = 4 if afi == 1 else 16
    message.peer = {'ip': str(ipaddress.ip_address(c.take(addr_size))),
        'local_ip': str(ipaddress.ip_address(c.take(addr_size))), 'asn': peer_asn,
        'local_asn': local_asn, 'interface': interface, 'endpoint_afi': afi,
        'bgp_id': None, 'bgp_id_present': False, 'ip_present': True, 'asn_present': True,
        'local_message': subtype in (6, 7, 10, 11)}
    p = message.peer
    message.interpretation = replace(message.interpretation, header=replace(
        message.interpretation.header, endpoint_trust='complete_header',
        peer_ip=p['ip'], peer_asn=p['asn'], local_ip=p['local_ip'], local_asn=p['local_asn'],
        interface=p['interface'], endpoint_afi=p['endpoint_afi'],
        direction='local' if p['local_message'] else 'received'))
    if subtype in (0, 5):
        message.kind = 'state_change'
        message.old_state, message.new_state = c.uint(2), c.uint(2)
        c.done()
        return peer_table, peer_table_record
    bgp_start = c.pos
    if c.take(16) != b'\xff' * 16:
        c.reject('BGP marker无效', 'bgp_marker_invalid')
    size, kind = c.uint(2), c.uint(1)
    if size < 19 or bgp_start + size != len(body):
        c.reject('BGP消息长度不符', 'bgp_length_mismatch', size)
    message.interpretation = replace(message.interpretation, header=replace(message.interpretation.header, bgp_type=kind))
    message.kind = {1: 'open', 2: 'update', 3: 'notification', 4: 'keepalive', 5: 'route_refresh'}.get(kind, 'unsupported_bgp_message')
    if kind != 2:
        if kind not in (1, 3, 4, 5):
            message.reason = 'unsupported_bgp_message_type'
        return peer_table, peer_table_record
    addpath = subtype in (8, 9, 10, 11)
    withdrawn_size = c.uint(2)
    withdrawn_offset = c.absolute
    withdrawn = Cursor(c.take(withdrawn_size), offset=withdrawn_offset, section='withdrawn_nlri')
    attr_size = c.uint(2)
    attr_offset = c.pos
    attr_raw = c.take(attr_size)
    attrs, path = attributes(attr_raw, width, offset=message.offset + 12 + attr_offset)
    message.paths.append(path)
    def add_routes(cur, route_afi, safi, action):
        while cur:
            message.elements.append({**nlri(cur, route_afi, safi, addpath),
                'ordinal': len(message.elements), 'action': action, 'originated_epoch': None,
                'peer': message.peer, 'path_key': path['path_key'],
                'attributes_offset': message.offset + 12 + attr_offset,
                'attributes_length': attr_size, 'attributes_digest': path['attributes_digest']})
    add_routes(withdrawn, 1, 1, 'withdraw')
    # 编码顺序作为元素ordinal：withdraw，属性中的MP NLRI，尾部IPv4 NLRI。
    for attr in attrs:
        if attr['code'] not in (14, 15):
            continue
        mp = Cursor(attr['value'], offset=message.offset + 12 + attr_offset + attr['offset'] +
                    (4 if attr['flags'] & 16 else 3), section='mp_nlri')
        route_afi, safi = mp.uint(2), mp.uint(1)
        if attr['code'] == 14:
            mp.take(mp.uint(1))
            mp.uint(1)
            add_routes(mp, route_afi, safi, 'announce')
        else:
            if not mp:
                message.eor_families.append([route_afi, safi])
            add_routes(mp, route_afi, safi, 'withdraw')
    add_routes(c, 1, 1, 'announce')
    if not message.elements and not attr_raw:
        message.eor_families.append([1, 1])
    return peer_table, peer_table_record


def source_identity(collector, origin_uri, content_sha256):
    return digest((collector+'\0'+origin_uri+'\0'+content_sha256).encode())


def read_source(path, expected_sha, *, source_id, max_record_bytes=16 * 1024**2,
                max_decoded_bytes=64 * 1024**3, policy=ReadPolicy.STRICT):
    """读到gzip EOF才能证明文件完整；异常前已yield的数据仍只能是candidate。"""
    policy = ReadPolicy(policy)  # 未知策略在打开输入前拒绝；生产默认仍严格。
    path = Path(path)
    stat = path.stat()
    sha = hashlib.sha256()
    with path.open('rb') as raw:
        for block in iter(lambda: raw.read(1024**2), b''):
            sha.update(block)
    if sha.hexdigest() != expected_sha:
        raise InputRejected('压缩源SHA不符', code='source_digest_mismatch')
    peers, peer_record, offset, record = [], None, 0, 0
    with gzip.open(path, 'rb') as stream:
        while True:
            header = stream.read(12)
            if not header:
                break
            if len(header) != 12:
                raise InputRejected('MRT头截断')
            epoch, kind, subtype, size = struct.unpack('!IHHI', header)
            if size > max_record_bytes or offset + 12 + size > max_decoded_bytes:
                raise ValueError('MRT资源限制触发')
            body = stream.read(size)
            if len(body) != size:
                raise InputRejected('MRT正文截断')
            message = Message(source_id, record, offset, 12 + size, epoch, kind, subtype, digest(header + body), content_sha256=expected_sha)
            try:
                peers, peer_record = decode(message, body, peers, peer_record)
            except InputRejected as exc:
                message.reason = exc.code + ': ' + str(exc)
                message.interpretation = replace(message.interpretation, policy=policy, source_path=str(path))
                supported_bgp4mp = kind in (16, 17) and subtype in (0, 1, 4, 5, 6, 7, 8, 9, 10, 11)
                isolate = (policy == ReadPolicy.ISOLATE_PAYLOAD and supported_bgp4mp
                           and isinstance(exc, _PayloadRejected)
                           and exc.failure.code in ISOLATABLE_FAILURES
                           and exc.failure.section != 'mrt_timestamp')
                # 仅本模块明确产生的 unsupported 受限观察保持旧行为。
                can_continue = isinstance(exc, _UnsupportedPayload) or isolate
                message.interpretation = replace(message.interpretation, continuation_allowed=can_continue)
                yield message
                if not can_continue:
                    raise
            else:
                message.interpretation = replace(message.interpretation, policy=policy, source_path=str(path))
                yield message
            offset += 12 + size
            record += 1
    after = path.stat()
    if (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise InputRejected('解析期间原件变动', code='source_changed')
