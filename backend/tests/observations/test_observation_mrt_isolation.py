"""M1 对外读取合同：人工帧、保真坏记录，不连接数据库或远端来源。"""
from dataclasses import asdict
import gzip
import hashlib
import os
from pathlib import Path
import struct

import pytest
from data_pipeline.bgp.input import mrt_reader as parser
from data_pipeline.bgp.input.mrt_types import ReadPolicy, ParseStatus
from data_pipeline.bgp.snapshots.origin import InputRejected
from tests.observations.test_observation_mrt import mrt, update, attr

ISOLATE = ReadPolicy.ISOLATE_PAYLOAD


def stream(tmp_path, raw, **kwargs):
    path = tmp_path / 'input.gz'
    path.write_bytes(gzip.compress(raw, mtime=0))
    return parser.read_source(path, hashlib.sha256(path.read_bytes()).hexdigest(),
                              source_id='fixture', **kwargs)


def assert_no_payload(message):
    assert message.elements == message.paths == message.peers == message.eor_families == []
    assert message.peer == {}
    assert message.old_state is message.new_state is None
    assert message.kind == 'unsupported'


def test_default_strict_yields_reject_then_stops(tmp_path):
    records = stream(tmp_path, update() + update(attrs=b'\xf0\x23\x04\x00\x04\x2f\x66') + update())
    assert next(records).record == 0
    bad = next(records)
    assert bad.record == 1 and bad.interpretation.status == ParseStatus.REJECTED
    assert bad.interpretation.policy == ReadPolicy.STRICT
    assert not bad.interpretation.continuation_allowed
    assert_no_payload(bad)
    with pytest.raises(InputRejected):
        next(records)


def test_explicit_mode_preserves_every_record_and_tlv_evidence(tmp_path):
    good = update()
    broken = update(attrs=b'\xf0\x23\x04\x00\x04\x2f\x66')
    records = list(stream(tmp_path, good + broken + good, policy=ISOLATE))
    assert [m.record for m in records] == [0, 1, 2]
    assert [m.offset for m in records] == [0, len(good), len(good) + len(broken)]
    assert [m.raw_digest for m in records] == [hashlib.sha256(r).hexdigest() for r in (good, broken, good)]
    bad = records[1]
    assert_no_payload(bad)
    evidence = bad.interpretation
    assert evidence.status == 'rejected' and evidence.continuation_allowed
    assert evidence.next_record_offset == records[2].offset and evidence.frame_complete
    assert evidence.header.endpoint_trust == 'complete_header'
    assert (evidence.header.peer_asn, evidence.header.peer_ip, evidence.header.bgp_type) == (64497, '192.0.2.1', 2)
    failure = evidence.failure
    assert (failure.attribute_flags, failure.attribute_type, failure.attribute_declared_length) == (240, 35, 1024)
    assert (failure.requested, failure.remaining) == (1024, 3)
    assert failure.attribute_offset == len(good) + 55
    assert failure.offset == failure.attribute_offset + 4
    assert len(records[2].elements) == 1


def test_real_162_bytes_unchanged(tmp_path):
    fixture = os.environ.get('DOMEYE_MRT_REJECT_FIXTURE')
    if not fixture:
        pytest.skip('需显式绑定 Git 外已核验 162B 原记录')
    raw = Path(fixture).read_bytes()
    assert len(raw) == 162
    assert hashlib.sha256(raw).hexdigest() == 'e322df06df21c21edf9d0370b51df5a4e120eeaad1c0270b085f438e91f57fa1'
    strict = stream(tmp_path, raw)
    bad = next(strict)
    assert_no_payload(bad)
    with pytest.raises(InputRejected): next(strict)
    messages = list(stream(tmp_path, raw + update(), policy=ISOLATE))
    bad = messages[0]
    assert bad.raw_digest == hashlib.sha256(raw).hexdigest()
    assert_no_payload(bad)
    h, f = bad.interpretation.header, bad.interpretation.failure
    assert (h.peer_asn, h.peer_ip, h.local_asn, h.endpoint_afi, h.direction) == (210633, '2a0f:5707:ab80:633::1', 12654, 2, 'received')
    assert (f.attribute_offset, f.offset, f.attribute_flags, f.attribute_type, f.requested, f.remaining) == (155, 159, 240, 35, 1024, 3)
    assert messages[1].offset == 162 and len(messages[1].elements) == 1


@pytest.mark.parametrize('subtype,width,local,addpath', [
    (1,2,False,False),(4,4,False,False),(6,2,True,False),(7,4,True,False),
    (8,2,False,True),(9,4,False,True),(10,2,True,True),(11,4,True,True)])
@pytest.mark.parametrize('extended', [False, True])
def test_normal_typed_values_direction_et_and_addpath_unchanged(tmp_path, subtype, width, local, addpath, extended):
    attrs = attr() if width == 4 else b'\x40\x02\x06\x02\x02' + struct.pack('!HH',64497,64496)
    prefix = (struct.pack('!I',42) if addpath else b'') + b'\x18\xc0\0\x02'
    payload = b'\0\0' + struct.pack('!H',len(attrs)) + attrs + prefix
    bgp = b'\xff'*16 + struct.pack('!HB',len(payload)+19,2) + payload
    endpoint = struct.pack('!IIHH' if width == 4 else '!HHHH',64497,12654,0,1) + b'\xc0\0\x02\x01\xc0\0\x02\x02'
    raw = mrt((struct.pack('!I',123456) if extended else b'') + endpoint + bgp, subtype, 17 if extended else 16)
    strict = list(stream(tmp_path, raw))[0]
    isolated = list(stream(tmp_path, raw, policy=ISOLATE))[0]
    a, b = asdict(strict), asdict(isolated)
    a.pop('interpretation'); b.pop('interpretation')
    assert a == b
    assert strict.peer['local_message'] == local and strict.peer['asn'] == 64497
    assert strict.paths[0]['as_path_text'] == '64497 64496'
    assert strict.elements[0]['prefix'] == '192.0.2.0/24'
    assert strict.elements[0]['path_id'] == (42 if addpath else None)
    assert strict.microsecond == (123456 if extended else None)
    assert isolated.interpretation.status == ParseStatus.DECODED
    assert isolated.raw_digest == hashlib.sha256(raw).hexdigest()


@pytest.mark.parametrize('state', [False, True])
def test_valid_zero_element_is_not_reject(tmp_path, state):
    endpoint = struct.pack('!IIHH',64497,12654,0,1) + b'\xc0\0\x02\x01'*2
    raw = mrt(endpoint+struct.pack('!HH',6,1),5) if state else update(ann=b'',attrs=b'')
    m = list(stream(tmp_path,raw,policy=ISOLATE))[0]
    assert not m.elements and m.interpretation.status == 'decoded' and m.reason is None
    assert m.eor_families == ([] if state else [[1,1]])
    assert (m.old_state,m.new_state) == ((6,1) if state else (None,None))


def test_partial_paths_elements_and_eor_are_atomic(tmp_path):
    # MP_UNREACH 的 EOR、withdraw 及 path 已暂存；末尾 prefix 截断才失败。
    raw = update(ann=b'\x18\xc0',withdrawn=b'\x18\xcb\0\x71',attrs=attr()+b'\x80\x0f\x03\0\x02\x01')
    bad, good = list(stream(tmp_path,raw+update(),policy=ISOLATE))
    assert_no_payload(bad)
    assert bad.interpretation.header.peer_ip == '192.0.2.1'
    assert len(good.elements) == 1 and not good.eor_families


@pytest.mark.parametrize('body', [b'\0', struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\0\x02\x01'])
def test_partial_endpoint_never_claims_complete_identity(tmp_path,body):
    bad, good = list(stream(tmp_path,mrt(body)+update(),policy=ISOLATE))
    assert_no_payload(bad)
    assert bad.interpretation.header.endpoint_trust == 'unknown'
    assert bad.interpretation.header.peer_asn is None
    assert good.interpretation.header.endpoint_trust == 'complete_header'


def test_bad_state_has_no_state_side_effect(tmp_path):
    endpoint = struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\0\x02\x01'*2
    bad = list(stream(tmp_path,mrt(endpoint+struct.pack('!HH',6,1)+b'X',5),policy=ISOLATE))[0]
    assert_no_payload(bad)
    assert bad.interpretation.failure.code == 'unparsed_tail'


def test_bad_peer_table_fails_instead_of_reusing_previous(tmp_path):
    table = b'\0\0\0\x19\0\x05rrc25\0\x01\x02'+b'\xc0\0\x02\x01'*2+struct.pack('!I',64497)
    # 第二张表先读入一条合法 peer，再因声明两条而失败。
    broken = table[:11]+b'\0\x02'+table[13:]
    records = stream(tmp_path,mrt(table,1,13)+mrt(broken,1,13)+update(),policy=ISOLATE)
    first = next(records)
    assert len(first.peers) == 1
    bad = next(records)
    assert_no_payload(bad)
    assert not bad.interpretation.continuation_allowed
    with pytest.raises(InputRejected): next(records)
    assert first.peers[0]['table_record'] == 0


def test_rib_partial_elements_fail_atomically(tmp_path):
    table = b'\0\0\0\x19\0\x05rrc25\0\x01\x02'+b'\xc0\0\x02\x01'*2+struct.pack('!I',64497)
    attrs = attr()
    rib = struct.pack('!I',7)+b'\x18\xc0\0\x02'+struct.pack('!HHIH',2,0,90,len(attrs))+attrs+b'\0'
    records = stream(tmp_path,mrt(table,1,13)+mrt(rib,2,13)+update(),policy=ISOLATE)
    next(records)
    bad = next(records)
    assert_no_payload(bad)
    assert not bad.interpretation.continuation_allowed
    with pytest.raises(InputRejected): next(records)


def test_unsupported_is_separate_and_atomic(tmp_path):
    # 第一个 withdraw 可解码；随后不支持的 MP family 不得残留该 withdraw。
    attrs = attr()+b'\x80\x0f\x04\0\x03\x01\0'
    messages = list(stream(tmp_path,update(withdrawn=b'\x18\xcb\0\x71',attrs=attrs)+update(),policy=ISOLATE))
    assert messages[0].interpretation.status == 'unsupported'
    assert_no_payload(messages[0])
    assert messages[1].interpretation.status == 'decoded'
    for raw in (mrt(b'x',99,99),mrt(b'x',99,13),mrt(b'x',99,16)):
        assert list(stream(tmp_path,raw,policy=ISOLATE))[0].interpretation.status == 'unsupported'


@pytest.mark.parametrize('raw', [b'\0'*5, update()[:-1]])
def test_outer_failure_is_never_isolated(tmp_path,raw):
    with pytest.raises(InputRejected): list(stream(tmp_path,raw,policy=ISOLATE))


@pytest.mark.parametrize('damage', ['crc','eof'])
def test_gzip_failure_is_never_isolated(tmp_path,damage):
    path = tmp_path/'input.gz'; data = bytearray(gzip.compress(update()))
    if damage == 'crc': data[-8] ^= 1
    else: data = data[:-4]
    path.write_bytes(data)
    with pytest.raises((gzip.BadGzipFile,EOFError)):
        list(parser.read_source(path,hashlib.sha256(data).hexdigest(),source_id='fixture',policy=ISOLATE))


@pytest.mark.parametrize('kwargs', [{'max_record_bytes':1},{'max_decoded_bytes':1}])
def test_size_protection_remains_fatal(tmp_path,kwargs):
    with pytest.raises(ValueError,match='资源限制'):
        list(stream(tmp_path,update(),policy=ISOLATE,**kwargs))


def test_timestamp_and_source_identity_remain_fatal(tmp_path):
    records = stream(tmp_path,mrt(struct.pack('!I',1000000)+update()[12:],4,17)+update(),policy=ISOLATE)
    assert next(records).interpretation.reason_code == 'invalid_mrt_timestamp'
    with pytest.raises(InputRejected): next(records)
    with pytest.raises(InputRejected,match='SHA'):
        list(parser.read_source(tmp_path/'input.gz','wrong',source_id='fixture',policy=ISOLATE))


@pytest.mark.parametrize('error', [RuntimeError('bug'), MemoryError('resource'), OSError('io'), InputRejected('unreviewed'), InputRejected('unreviewed',code='unsupported_nlri')])
def test_unknown_and_resource_errors_never_continue(tmp_path,monkeypatch,error):
    def fail(*args): raise error
    monkeypatch.setattr(parser,'_decode',fail)
    records = stream(tmp_path,update()+update(),policy=ISOLATE)
    if isinstance(error,InputRejected):
        bad = next(records)
        assert bad.interpretation.status == 'rejected'
        assert not bad.interpretation.continuation_allowed
    with pytest.raises(type(error),match=str(error)):
        next(records)


def test_unknown_policy_rejected_before_io(tmp_path):
    with pytest.raises(ValueError):
        list(parser.read_source(tmp_path/'absent','none',source_id='fixture',policy='guess'))


@pytest.mark.parametrize('case,code', [('marker','bgp_marker_invalid'),('length','bgp_length_mismatch'),('duplicate','duplicate_attribute'),('prefix','nlri_prefix_invalid')])
def test_other_reviewed_payload_failures(tmp_path,case,code):
    raw = bytearray(update())
    if case == 'marker': raw[32] = 0
    elif case == 'length': raw[48:50] = struct.pack('!H',19)
    elif case == 'duplicate': raw = update(attrs=attr()+attr())
    else: raw = update(ann=b'\x21')
    bad, good = list(stream(tmp_path,bytes(raw)+update(),policy=ISOLATE))
    assert_no_payload(bad)
    assert bad.interpretation.failure.code == code
    assert len(good.elements) == 1


def test_truncated_tlv_header_preserves_only_read_fields(tmp_path):
    bad = list(stream(tmp_path,update(attrs=b'\xf0'),policy=ISOLATE))[0]
    f = bad.interpretation.failure
    assert f.attribute_flags == 240 and f.attribute_type is None and f.attribute_declared_length is None
    assert f.remaining == 0 and f.requested == 1


def test_et_timestamp_truncation_is_fatal(tmp_path):
    records = stream(tmp_path,mrt(b'\0',4,17)+update(),policy=ISOLATE)
    bad = next(records)
    assert not bad.interpretation.continuation_allowed
    assert bad.interpretation.header.microsecond is None
    with pytest.raises(InputRejected): next(records)


def test_opaque_non_update_is_only_header_interpretation(tmp_path):
    raw = bytearray(update())
    raw[50] = 1  # 已有解码器对 OPEN 只解释 BGP 头，不扩大成完整 OPEN 语义验证。
    message = list(stream(tmp_path,bytes(raw),policy=ISOLATE))[0]
    assert message.kind == 'open' and message.interpretation.interpretation_level == 'bgp_header_only'
    raw[50] = 255
    message = list(stream(tmp_path,bytes(raw),policy=ISOLATE))[0]
    assert message.interpretation.status == 'unsupported'
    assert message.interpretation.header.bgp_type == 255


def test_source_change_detected_even_after_isolated_record(tmp_path):
    records = stream(tmp_path,update(attrs=b'\xf0'),policy=ISOLATE)
    next(records)
    path = tmp_path/'input.gz'
    # 同一时钟粒度内 touch 可能保留原时间戳；明确制造实体戳变化。
    stat=path.stat()
    os.utime(path,ns=(stat.st_atime_ns,stat.st_mtime_ns+1_000_000_000))
    with pytest.raises(InputRejected,match='原件变动'):
        next(records)


@pytest.mark.parametrize('case', ['nlri','record162','peer_table','timestamp'])
def test_direct_decode_rejected_is_strict_and_cannot_continue(case):
    if case == 'nlri': raw = update(ann=b'\x21')
    elif case == 'peer_table': raw = mrt(b'\0',1,13)
    elif case == 'timestamp': raw = mrt(struct.pack('!I',1000000)+update()[12:],4,17)
    else:
        fixture = os.environ.get('DOMEYE_MRT_REJECT_FIXTURE')
        if not fixture: pytest.skip('需显式绑定 Git 外已核验 162B 原记录')
        raw = Path(fixture).read_bytes()
        assert len(raw) == 162 and hashlib.sha256(raw).hexdigest() == 'e322df06df21c21edf9d0370b51df5a4e120eeaad1c0270b085f438e91f57fa1'
    epoch, kind, subtype, size = struct.unpack('!IHHI',raw[:12])
    message = parser.Message('fixture',0,0,len(raw),epoch,kind,subtype,parser.digest(raw))
    with pytest.raises(InputRejected): parser.decode(message,raw[12:],[],None)
    evidence = message.interpretation
    assert evidence.policy == ReadPolicy.STRICT and evidence.status == ParseStatus.REJECTED
    assert evidence.continuation_allowed is False
    assert_no_payload(message)


def test_direct_decode_unsupported_retains_distinct_continuation():
    raw = update(attrs=attr()+b'\x80\x0f\x04\0\x03\x01\0')
    epoch, kind, subtype, size = struct.unpack('!IHHI',raw[:12])
    message = parser.Message('fixture',0,0,len(raw),epoch,kind,subtype,parser.digest(raw))
    with pytest.raises(InputRejected): parser.decode(message,raw[12:],[],None)
    assert message.interpretation.policy == ReadPolicy.STRICT
    assert message.interpretation.status == ParseStatus.UNSUPPORTED
    assert message.interpretation.continuation_allowed is True
    assert_no_payload(message)
