"""官方 bgpdump 1.6.2 的可重放文本证据，不规定下游业务解释。"""
import os
import struct
import subprocess
import pytest
from tests.observations.test_observation_mrt import update, load

STDOUT = {
    4: 'BGP4MP|100|A|192.0.2.1|64497|192.0.2.0/24|64497 64496|INCOMPLETE|255.255.255.255|0|0||NAG||',
    7: 'BGP4MP_LOCAL|100|A|192.0.2.1|64497|192.0.2.0/24|64497 64496|INCOMPLETE|255.255.255.255|0|0||NAG||',
    9: 'BGP4MP_AP|100|A|192.0.2.1|64497|192.0.2.0/24|42|64497 64496|INCOMPLETE|255.255.255.255|0|0||NAG||',
    11: 'BGP4MP_AP|100|A|192.0.2.2|12654|192.0.2.0/24|42|64497 64496|INCOMPLETE|255.255.255.255|0|0||NAG||',
    'et': 'BGP4MP_ET|100.123456|A|192.0.2.1|64497|192.0.2.0/24|64497 64496|INCOMPLETE|255.255.255.255|0|0||NAG||',
}

@pytest.mark.parametrize('kind', [4,7,9,11,'et'])
def test_official_binary_stdout_and_original_fields(tmp_path, kind):
    binary = os.environ.get('DOMEYE_FEATURE_BGPDUMP_BIN')
    if not binary: pytest.skip('必须显式绑定已核对的官方 bgpdump 1.6.2 二进制')
    raw = update(subtype=kind if kind != 'et' else 4,
                 ann=(struct.pack('!I',42) if kind in (9,11) else b'')+b'\x18\xc0\x00\x02')
    if kind == 'et':
        raw = struct.pack('!IHHI',100,17,4,len(raw)-12+4)+struct.pack('!I',123456)+raw[12:]
    path = tmp_path/'direction.mrt'; path.write_bytes(raw)
    result = subprocess.run([binary,'-m',str(path)],capture_output=True,text=True,check=True)
    assert result.stdout.strip() == STDOUT[kind]
    message = load(tmp_path,raw)[0]
    assert message.peer['asn'] == 64497
    assert message.paths[0]['as_path_text'] == '64497 64496'
    assert message.elements[0]['path_id'] == (42 if kind in (9,11) else None)

@pytest.mark.parametrize('subtype,width,local,addpath',[(1,2,False,False),(6,2,True,False),(8,2,False,True),(10,2,True,True)])
def test_as2_actual_output(tmp_path,subtype,width,local,addpath):
    binary=os.environ.get('DOMEYE_FEATURE_BGPDUMP_BIN')
    if not binary:pytest.skip('需要官方二进制')
    attrs=b'\x40\x02\x06\x02\x02'+struct.pack('!HH',64497,64496)
    ann=(struct.pack('!I',42) if addpath else b'')+b'\x18\xc0\x00\x02'
    payload=b'\0\0'+struct.pack('!H',len(attrs))+attrs+ann
    bgp=b'\xff'*16+struct.pack('!HB',len(payload)+19,2)+payload
    body=struct.pack('!HHHH',64497,12654,0,1)+b'\xc0\x00\x02\x01\xc0\x00\x02\x02'+bgp
    raw=struct.pack('!IHHI',100,16,subtype,len(body))+body
    path=tmp_path/'as2.mrt';path.write_bytes(raw)
    stdout=subprocess.run([binary,'-m',str(path)],capture_output=True,text=True,check=True).stdout.strip()
    # AS2的对应格式与已核对AS4复现字段一致。
    expected=STDOUT[{1:4,6:7,8:9,10:11}[subtype]]
    assert stdout==expected
    m=load(tmp_path,raw)[0]
    assert m.peer['asn']==64497 and m.peer['local_message']==local
    assert m.paths[0]['as_path_text']=='64497 64496'

@pytest.mark.parametrize('ipv6,addpath',[(False,False),(False,True),(True,False),(True,True)])
def test_rib_real_stdout(tmp_path,ipv6,addpath):
    from tests.observations.test_observation_mrt import mrt, attr
    binary=os.environ.get('DOMEYE_FEATURE_BGPDUMP_BIN')
    if not binary:pytest.skip('需要官方二进制')
    table=b'\0\0\0\x19\0\x05rrc25\0\x01\x02'+b'\xc0\0\x02\x01'*2+struct.pack('!I',64497)
    prefix=b'\x20\x20\x01\x0d\xb8' if ipv6 else b'\x18\xc0\0\x02'
    attrs=attr()
    rib=struct.pack('!I',7)+prefix+struct.pack('!HHI',1,0,90)+(struct.pack('!I',42) if addpath else b'')+struct.pack('!H',len(attrs))+attrs
    subtype=(4 if ipv6 else 2)+(6 if addpath else 0)
    raw=mrt(table,1,13)+mrt(rib,subtype,13)
    path=tmp_path/'rib.mrt';path.write_bytes(raw)
    stdout=subprocess.run([binary,'-m',str(path)],capture_output=True,text=True,check=True).stdout.strip()
    expected=('TABLE_DUMP2_AP' if addpath else 'TABLE_DUMP2')+'|100|B|192.0.2.1|64497|'+('2001:db8::/32' if ipv6 else '192.0.2.0/24')+'|'+('42|' if addpath else '')+'64497 64496|INCOMPLETE|255.255.255.255|0|0||NAG||'
    assert stdout==expected
    m=load(tmp_path,raw)[1]
    assert m.elements[0]['peer']['asn']==64497
    assert m.paths[0]['as_path_text']=='64497 64496'
    assert m.elements[0]['path_id']==(42 if addpath else None)

@pytest.mark.parametrize('local,addpath',[(False,False),(True,False),(False,True),(True,True)])
def test_ipv6_update_real_stdout(tmp_path,local,addpath):
    from tests.observations.test_observation_mrt import attr
    binary=os.environ.get('DOMEYE_FEATURE_BGPDUMP_BIN')
    if not binary:pytest.skip('需要官方二进制')
    nlri=(struct.pack('!I',42) if addpath else b'')+b'\x20\x20\x01\x0d\xb8'
    value=b'\0\x02\x01\x10'+bytes(16)+b'\0'+nlri
    attrs=attr()+bytes([128,14,len(value)])+value
    subtype=(11 if local else 9) if addpath else (7 if local else 4)
    raw=update(ann=b'',attrs=attrs,subtype=subtype)
    path=tmp_path/'v6.mrt';path.write_bytes(raw)
    stdout=subprocess.run([binary,'-m',str(path)],capture_output=True,text=True,check=True).stdout.strip()
    expected=STDOUT[subtype].replace('192.0.2.0/24','2001:db8::/32').replace('255.255.255.255','::')
    assert stdout==expected
    m=load(tmp_path,raw)[0]
    assert m.elements[0]['prefix']=='2001:db8::/32'
    assert m.elements[0]['path_id']==(42 if addpath else None)

@pytest.mark.parametrize('state', [False,True])
def test_zero_element_real_stdout(tmp_path,state):
    from tests.observations.test_observation_mrt import mrt
    binary=os.environ.get('DOMEYE_FEATURE_BGPDUMP_BIN')
    if not binary:pytest.skip('需要官方二进制')
    peer=struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\x00\x02\x01\xc0\x00\x02\x02'
    raw=mrt(peer+struct.pack('!HH',6,1),subtype=5) if state else update(ann=b'',attrs=b'')
    path=tmp_path/'zero.mrt';path.write_bytes(raw)
    stdout=subprocess.run([binary,'-m',str(path)],capture_output=True,text=True,check=True).stdout.strip()
    assert stdout==('BGP4MP|100|STATE|192.0.2.1|64497|6|1' if state else '')
    m=load(tmp_path,raw)[0]
    assert not m.elements
    assert m.kind==('state_change' if state else 'update')

@pytest.mark.parametrize('kind,subtype,path_id,old_vp,reason',[(16,8,42,'64497','addpath_fixed_path_column'),(16,11,42,'12654','local_addpath_text_endpoint_swap'),(13,10,42,'64497','rib_addpath_fixed_path_column'),(17,4,None,'64497','et_fixed_integer_timestamp')])
def test_saved_field_difference_identity(kind,subtype,path_id,old_vp,reason):
    from data_pipeline.bgp.input.path_decoding import decoding_difference, decode_element, DECODER_VERSION
    row=dict(source_id='s',event_id='s:1:0',record=1,ordinal=0,mrt_type=kind,mrt_subtype=subtype,
             action='announce',peer_asn=64497,local_asn=12654,epoch=100,microsecond=123456 if kind==17 else None,
             path_id=path_id,path_key='p',as_path_text='64497 64496')
    difference=decoding_difference(row)
    assert reason in difference['reason'] and difference['old_fixed_vp']==old_vp
    assert difference['new_vp']=='64497' and difference['historical_tool_version']=='Unknown'
    assert difference['old_fixed_path']==('42' if path_id else row['as_path_text'])
    assert decode_element(row)=={**row,'decoder_version':DECODER_VERSION,'direction_rule':'legacy-feature-bidirectional-original-peer/v1'}
