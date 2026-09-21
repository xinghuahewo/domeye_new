"""独立手工构造原始二进制，不调用解析器反向编码。"""
import gzip
import hashlib
import struct
import pytest
from data_pipeline.bgp.input.mrt_reader import read_source


def mrt(body, subtype=4, kind=16, epoch=100):
    return struct.pack('!IHHI', epoch, kind, subtype, len(body)) + body


def attr(origin=64496):
    return b'\x40\x02\x0a\x02\x02' + struct.pack('!II', 64497, origin)


def update(ann=b'\x18\xc0\x00\x02', withdrawn=b'', attrs=None, subtype=4, peer=b'\xc0\x00\x02\x01'):
    attrs = attr() if attrs is None else attrs
    payload = struct.pack('!H', len(withdrawn)) + withdrawn + struct.pack('!H', len(attrs)) + attrs + ann
    bgp = b'\xff'*16 + struct.pack('!HB', len(payload)+19, 2) + payload
    return mrt(struct.pack('!IIHH', 64497, 12654, 0, 1)+peer+b'\xc0\x00\x02\x02'+bgp, subtype)


def load(tmp_path, raw):
    p = tmp_path/'input.gz'
    p.write_bytes(gzip.compress(raw, mtime=0))
    return list(read_source(p, hashlib.sha256(p.read_bytes()).hexdigest(),source_id='fixture'))


def test_messages_elements_positions_and_same_content(tmp_path):
    raw = update(ann=b'\x18\xc0\x00\x02\x18\xc6\x33\x64')
    messages = load(tmp_path, raw+raw)
    assert len(messages) == 2
    assert [len(m.elements) for m in messages] == [2, 2]
    assert messages[1].offset == len(raw)
    assert messages[0].raw_digest == messages[1].raw_digest
    assert messages[0].message_id != messages[1].message_id
    assert [e['prefix'] for e in messages[0].elements] == ['192.0.2.0/24','198.51.100.0/24']
    assert messages[0].paths[0]['attributed_origin_asn'] == 64496
    assert messages[0].peer['bgp_id_present'] is False


def test_addpath_zero_and_withdraw(tmp_path):
    m = load(tmp_path, update(ann=b'\0\0\0\0\x18\xc0\x00\x02',
           withdrawn=b'\0\0\0\x09\x18\xcb\x00\x71', subtype=9))[0]
    assert [(e['action'],e['path_id']) for e in m.elements] == [('withdraw',9),('announce',0)]
    assert all(e['path_id_present'] for e in m.elements)


def test_as_set_as4_private(tmp_path):
    for raw, expected in [(b'\x40\x02\x06\x01\x01'+struct.pack('!I',64496),'as_set_ambiguous'),
                          (attr(64512),'private_as_skipped'),
                          (attr()+b'\xc0\x11\x06\x02\x01'+struct.pack('!I',64496),'as4_path_requires_separate_rule')]:
        m = load(tmp_path, update(attrs=raw))[0]
        assert m.paths[0]['reason'] == expected
        assert m.paths[0]['attributes_raw'] == raw


def test_mp_v6_and_eor(tmp_path):
    # MP_REACH IPv6 unicast, 16-byte next-hop, /32 2001:db8::
    value=b'\0\x02\x01\x10'+bytes(16)+b'\0\x20\x20\x01\x0d\xb8'
    m=load(tmp_path,update(ann=b'',attrs=attr()+bytes([128,14,len(value)])+value))[0]
    assert m.elements[0]['prefix']=='2001:db8::/32'
    assert load(tmp_path,update(ann=b'',attrs=b''))[0].eor_families==[[1,1]]
    assert load(tmp_path,update(ann=b'',attrs=b'\x80\x0f\x03\0\x02\x01'))[0].eor_families==[[2,1]]


def test_rib_peer_originated_and_raw_ref(tmp_path):
    table=b'\0\0\0\x19\0\x05rrc25\0\x01\x02'+b'\xc0\0\x02\x01'*2+struct.pack('!I',64497)
    rawattrs=attr()
    rib=struct.pack('!I',7)+b'\x18\xc0\0\x02'+struct.pack('!HHIH',1,0,90,len(rawattrs))+rawattrs
    m=load(tmp_path,mrt(table,1,13)+mrt(rib,2,13))[1]
    assert m.elements[0]['action']=='rib_snapshot'
    assert m.elements[0]['originated_epoch']==90
    assert m.elements[0]['peer']['bgp_id_present'] is True
    assert m.elements[0]['peer']['table_record']==0


def test_nonrouting_and_corruption(tmp_path):
    unknown=mrt(b'unchanged opaque bytes',99,99)
    assert load(tmp_path,unknown)[0].reason=='unsupported_mrt_type'
    endpoint=struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\0\x02\x01'*2
    state=load(tmp_path,mrt(endpoint+struct.pack('!HH',6,1),5))[0]
    assert (state.kind,state.old_state,state.new_state)==('state_change',6,1)
    for raw in (update()[:-1],b'\0'*5):
        with pytest.raises(ValueError): load(tmp_path,raw)
    p=tmp_path/'broken.gz'
    p.write_bytes(gzip.compress(update())[:-4])
    with pytest.raises(EOFError): list(read_source(p,hashlib.sha256(p.read_bytes()).hexdigest(),source_id='fixture'))


def test_open_et_and_same_asn_different_ip(tmp_path):
    endpoint=struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\0\x02\x01'*2
    payload=b'\x04'+struct.pack('!HH',64497,90)+b'\xc0\0\x02\x01\0'
    bgp=b'\xff'*16+struct.pack('!HB',len(payload)+19,1)+payload
    m=load(tmp_path,mrt(struct.pack('!I',123)+endpoint+bgp,4,17))[0]
    assert m.kind=='open' and m.microsecond==123 and not m.elements
    ms=load(tmp_path,update()+update(peer=b'\xc0\0\x02\x09'))
    assert ms[0].peer['asn']==ms[1].peer['asn']
    assert ms[0].peer['ip']!=ms[1].peer['ip']


def test_gzip_crc_and_mrt_bad_attribute(tmp_path):
    p=tmp_path/'crc.gz';raw=bytearray(gzip.compress(update()));raw[-8]^=1;p.write_bytes(raw)
    with pytest.raises(gzip.BadGzipFile):list(read_source(p,hashlib.sha256(raw).hexdigest(),source_id='fixture'))
    with pytest.raises(ValueError):load(tmp_path,update(attrs=b'\x40\x02\xff'))
