"""原生 UPDATE 的实际 libbgpdump 输出、原始字节和多文件接线。"""
import gzip
import hashlib
import json
from pathlib import Path
import struct
import subprocess

import pytest

from data_pipeline.bgp.archive.checkpoint import AttemptStore, OBS_TABLES
from data_pipeline.bgp.input.mrt_reader import read_source
from tests.observations.test_observation_mrt import attr, mrt, update
from tests.observations.test_observation_native import native
from tests.observations.test_observation_checkpoint import dsn, fixture


def run_native(tmp_path,native,raw,policy='strict/v1'):
    path=tmp_path/'updates.gz';path.write_bytes(gzip.compress(raw,mtime=0))
    digest=hashlib.sha256(path.read_bytes()).hexdigest();output=tmp_path/'native'
    native.parse(path,'fixture',digest,output,batch_rows=3,source_kind='update',policy=policy)
    return path,native.rows(output)


def update_fixture():
    mp=b'\0\x02\x01\x10'+bytes(16)+b'\0\x21\x20\x01\x0d\xb8\xff'
    endpoint=struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\0\x02\x01'*2
    return b''.join([
        update(),update(withdrawn=b'\x18\xcb\0\x71'),
        update(ann=b'\0\0\0\0\x18\xc0\0\x02',withdrawn=b'\0\0\0\x09\x18\xcb\0\x71',subtype=9),
        update(ann=b'',attrs=attr()+bytes([128,14,len(mp)])+mp),
        update(ann=b'',attrs=b''),update(ann=b'',attrs=b'\x80\x0f\x03\0\x02\x01'),
        update(subtype=7),mrt(struct.pack('!I',123456)+update()[12:],4,17),
        mrt(endpoint+struct.pack('!HH',6,1),5),
        mrt(endpoint+b'\xff'*16+struct.pack('!HB',19,4),4),
    ])


def test_native_update_raw_evidence_and_cli(tmp_path,native):
    from ipaddress import ip_network
    path,actual=run_native(tmp_path,native,update_fixture())
    class Rows:
        def __init__(self):self.rows={n:[] for n in OBS_TABLES}
        def append(self,name,row):self.rows[name].append({k:row.get(k) for k,_ in OBS_TABLES[name]})
    expected=Rows()
    for m in read_source(path,hashlib.sha256(path.read_bytes()).hexdigest(),source_id='fixture'):AttemptStore.message(expected,m)
    # 字符串只要求与权威 bgpdump 一致；全部原始证据列仍与手工编码核对。
    for table in ('messages','elements','eor'):
        ignored={'interpretation'} if table=='messages' else {'prefix'}
        assert [{k:v for k,v in r.items() if k not in ignored} for r in actual[table]]==[{k:v for k,v in r.items() if k not in ignored} for r in expected.rows[table]],table
    paths={p['path_key']:p for p in actual['paths']}
    for p in expected.rows['paths']:
        assert {k:v for k,v in p.items() if k not in ('as_path_text','as4_path_text')}=={k:v for k,v in paths[p['path_key']].items() if k not in ('as_path_text','as4_path_text')}
    cli=subprocess.run([str(native.build/'bgpdump-reference'),'-m',str(path)],capture_output=True,text=True,check=True,timeout=30)
    expected_cli=[]
    for line in cli.stdout.splitlines():
        fields=line.split('|')
        if len(fields)<6 or fields[2] not in ('A','W'):continue
        value=tuple(fields[2:6])
        if fields[2]=='A':value+=(fields[7 if '_AP' in fields[0] else 6],)
        expected_cli.append(value)
    actual_cli=[(('A' if e['action']=='announce' else 'W'),e['peer_ip'],str(e['peer_asn']),e['prefix'],paths[e['path_key']]['as_path_text'])[:4 if e['action']=='withdraw' else 5] for e in actual['elements']]
    def normalized(rows):return sorted((r[0],r[1],r[2],str(ip_network(r[3],strict=False)),*r[4:]) for r in rows)
    actual_cli=normalized(actual_cli);expected_cli=normalized(expected_cli)
    assert actual_cli==expected_cli
    et=next(m for m in actual['messages'] if m['mrt_type']==17)
    assert et['microsecond']==123456 and json.loads(et['interpretation'])['header']['microsecond']==123456


def test_native_update_isolates_payload_and_keeps_next_frame(tmp_path,native):
    broken=update(attrs=b'\x40\x02\xff')
    path,rows=run_native(tmp_path,native,update()+broken+update(),policy='isolate-payload/v1')
    assert [json.loads(m['interpretation'])['status'] for m in rows['messages']]==['decoded','rejected','decoded']
    assert [e['message_id'] for e in rows['elements']]==['fixture:0','fixture:2']
    assert rows['messages'][1]['raw_digest']==hashlib.sha256(broken).hexdigest()
    assert rows['quality'][0]['code']=='bgpdump_payload_rejected'


@pytest.mark.parametrize('damage',['payload','crc','frame'])
def test_native_update_strict_errors_do_not_complete(tmp_path,native,damage):
    raw=update(attrs=b'\x40\x02\xff') if damage=='payload' else update()
    path=tmp_path/'updates.gz';compressed=bytearray(gzip.compress(raw if damage!='frame' else raw[:-1],mtime=0))
    if damage=='crc':compressed[-8]^=1
    path.write_bytes(compressed)
    with pytest.raises((ValueError,RuntimeError,EOFError,gzip.BadGzipFile)):
        native.parse(path,'fixture',hashlib.sha256(compressed).hexdigest(),tmp_path/'native',source_kind='update')
    assert not (tmp_path/'native/complete.json').exists()


def test_native_update_as2_ipv6_local_endpoint(tmp_path,native):
    endpoint=struct.pack('!HHHH',64497,12654,3,2)+bytes.fromhex('20010db8000000000000000000000001')+bytes.fromhex('20010db8000000000000000000000002')
    attributes=b'\x40\x02\x06\x02\x02'+struct.pack('!HH',64497,64496)
    payload=b'\0\0'+struct.pack('!H',len(attributes))+attributes+b'\x18\xc0\0\x02'
    raw=mrt(endpoint+b'\xff'*16+struct.pack('!HB',len(payload)+19,2)+payload,6)
    _,rows=run_native(tmp_path,native,raw)
    assert rows['messages'][0]['local_message'] is True
    assert rows['elements'][0]['peer_ip']=='2001:db8::1'
    assert rows['paths'][0]['asn_width']==2 and rows['paths'][0]['as_path_text']=='64497 64496'


def test_native_multifile_matches_evidence_and_reuses_checkpoints(tmp_path,native,dsn):
    from data_pipeline.bgp.archive.checkpoint import produce_checkpointed, sha
    from data_pipeline.bgp.archive.selection import Selection
    manifest=fixture(tmp_path/'inputs')
    shared=tmp_path/'data'
    baseline=produce_checkpointed(manifest,dsn,tmp_path/'legacy',batch_rows=3,min_free_bytes=0,catalog_data_path=shared)
    root=tmp_path/'native-run'
    seal=produce_checkpointed(manifest,dsn,root,batch_rows=3,min_free_bytes=0,native_build=native.build,catalog_data_path=shared)
    assert len(seal['checkpoints'])==4 and seal['qualification']=='observation_sealed'
    assert seal['path_owner_count']==baseline['path_owner_count']
    def rows(result):
        binding=Selection(dsn,result['run_id'],result['snapshot']);db=binding.connect();tables={}
        try:
            for name in OBS_TABLES:
                items=db.execute('SELECT * FROM '+binding.table(name)).fetch_arrow_table().to_pylist()
                for row in items:
                    if name=='messages':
                        row['interpretation']=json.loads(row['interpretation'])
                        expected_version='mrt-interpretation/bgpdump-v1' if result is seal and row['mrt_type'] in (16,17) else 'mrt-interpretation/v1'
                        assert row['interpretation'].pop('contract_version')==expected_version
                tables[name]=sorted(sha(row) for row in items)
            return tables
        finally:db.close()
    assert rows(seal)==rows(baseline)
    def forbid(*args,**kwargs):raise AssertionError('封存来源不应重新解析')
    from data_pipeline.bgp.input.native_parser import NativeRuntime
    original=NativeRuntime.parse;NativeRuntime.parse=forbid
    try:assert produce_checkpointed(manifest,dsn,root,batch_rows=3,min_free_bytes=0,native_build=native.build,catalog_data_path=shared)==seal
    finally:NativeRuntime.parse=original


def test_native_multifile_partial_segment_resume(tmp_path,native,dsn):
    from data_pipeline.bgp.archive.checkpoint import produce_checkpointed
    manifest=fixture(tmp_path/'inputs');root=tmp_path/'run';stopped=False
    def interrupt(label,ordinal,owner):
        nonlocal stopped
        if label=='after_segment' and not stopped:stopped=True;raise RuntimeError('人工分段中断')
    with pytest.raises(RuntimeError,match='人工分段中断'):
        produce_checkpointed(manifest,dsn,root,batch_rows=3,min_free_bytes=0,native_build=native.build,hook=interrupt)
    seal=produce_checkpointed(manifest,dsn,root,batch_rows=3,min_free_bytes=0,native_build=native.build)
    assert seal['qualification']=='observation_sealed'
    assert [cp['ordinal'] for cp in seal['checkpoints']]==[0,1,2,3]
    assert sum(cp['counts']['messages'] for cp in seal['checkpoints'])==14


def test_native_owner_index_rejects_conflicting_body(tmp_path,native):
    import pyarrow as pa
    schema=pa.schema([('k1',pa.binary()),('k2',pa.int64()),('digest',pa.binary())])
    def reader(digest):return pa.Table.from_pylist([dict(k1=b'a'*32,k2=0,digest=digest)],schema=schema).to_reader()
    index=tmp_path/'owners.sqlite'
    assert native.merge_owners(reader(b'b'*32),index,'first')==1
    assert native.merge_owners(reader(b'b'*32),index,'second')==1
    assert native.owner_rows(index).read_all().to_pylist()==[dict(path_key=(b'a'*32).hex(),attempt='first')]
    with pytest.raises(ValueError,match='typed 正文不符'):native.merge_owners(reader(b'c'*32),index,'third')


def test_native_empty_update_is_verified_eof(tmp_path,native):
    _,rows=run_native(tmp_path,native,b'')
    assert all(not value for value in rows.values())
    receipt=json.loads((tmp_path/'native/complete.json').read_text())
    assert receipt['records']==receipt['elements']==receipt['segments']==0


def test_native_unsupported_refresh_preserves_trusted_header(tmp_path,native):
    endpoint=struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\0\x02\x01'*2
    raw=mrt(endpoint+b'\xff'*16+struct.pack('!HB',23,5)+b'\0\x01\0\x01',4)
    _,rows=run_native(tmp_path,native,raw+update())
    first=rows['messages'][0];interpretation=json.loads(first['interpretation'])
    assert interpretation['status']=='unsupported'
    assert interpretation['header']['peer_asn']==64497 and interpretation['header']['bgp_type']==5
    assert first['peer_ip'] is None and first['local_asn'] is None
    assert [e['message_id'] for e in rows['elements']]==['fixture:1']


def test_native_prefix_padding_preserves_one_network_key(tmp_path,native):
    raw=update(ann=b'\x19\xc0\0\x02\x80')+update(ann=b'',withdrawn=b'\x19\xc0\0\x02\xff')
    _,rows=run_native(tmp_path,native,raw)
    assert [(e['action'],e['prefix']) for e in rows['elements']]==[('announce','192.0.2.128/25'),('withdraw','192.0.2.128/25')]
    assert [e['raw_prefix'] for e in rows['elements']]==[b'\x19\xc0\0\x02\x80',b'\x19\xc0\0\x02\xff']


def test_native_as4_reconstruction_does_not_rewrite_wire_path(tmp_path,native):
    endpoint=struct.pack('!HHHH',64497,12654,0,1)+b'\xc0\0\x02\x01'*2
    original=b'\x02\x02'+struct.pack('!HH',64497,23456);as4=b'\x02\x01'+struct.pack('!I',65536)
    attributes=b'\x40\x02'+bytes([len(original)])+original+b'\xc0\x11'+bytes([len(as4)])+as4
    payload=b'\0\0'+struct.pack('!H',len(attributes))+attributes+b'\x18\xc0\0\x02'
    raw=mrt(endpoint+b'\xff'*16+struct.pack('!HB',len(payload)+19,2)+payload,1)
    path,rows=run_native(tmp_path,native,raw);value=rows['paths'][0]
    assert value['attributes_raw']==attributes and value['as_path_raw']==original and value['as4_path_raw']==as4
    cli=subprocess.run([str(native.build/'bgpdump-reference'),'-m',str(path)],check=True,capture_output=True,text=True)
    assert value['as_path_text']==cli.stdout.splitlines()[0].split('|')[6]=='64497 65536'
    assert value['raw_origin_asn']==23456 and value['attributed_origin_asn'] is None
    assert value['reason']=='as4_path_requires_separate_rule'
