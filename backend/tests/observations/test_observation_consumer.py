"""仅显式独立PG：固定源存储→固定快照有序消息Reader。"""
import gzip
import hashlib
import os
from pathlib import Path
import struct
import uuid
import psycopg2
import pytest

from data_pipeline.bgp.archive.message_reader import ObservationReader, SourceStart, MessageBatch, SourceEnd
from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.bgp.input.mrt_reader import source_identity
from tests.observations.test_observation_two_phase import fixture_manifest
from tests.observations.test_observation_mrt import update, mrt


@pytest.fixture
def feature_dsn():
    base = os.environ.get('DOMEYE_FEATURE_TEST_DSN')
    if not base: pytest.skip('必须显式绑定本任务隔离PG')
    name = 'feature_fixture_'+uuid.uuid4().hex
    pg = psycopg2.connect(base); pg.autocommit = True
    with pg.cursor() as c: c.execute('CREATE DATABASE '+name)
    pg.close()
    return base+' dbname='+name


def observation_fixture(root):
    manifest = fixture_manifest(root)
    peer = struct.pack('!IIHH',64497,12654,0,1)+b'\xc0\x00\x02\x01\xc0\x00\x02\x02'
    raw = (update(ann=b'\x18\xc0\x00\x02\x18\xc6\x33\x64') + update(subtype=7)
           + mrt(peer+struct.pack('!HH',6,1),subtype=5)
           + update(ann=b'',attrs=b''))
    for entry, data in zip(manifest['inputs'][1:], (raw,b'')):
        path = Path(entry['path']); path.write_bytes(gzip.compress(data,mtime=0))
        entry['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        entry['size'] = path.stat().st_size
        entry['source_id'] = source_identity('rrc25',entry['origin_uri'],entry['sha256'])
    manifest['update_sources'] = [e['source_id'] for e in manifest['inputs'][1:]]
    return manifest


def test_fixed_snapshot_message_zero_elements_direction_and_counts(tmp_path, feature_dsn):
    manifest = observation_fixture(tmp_path)
    report = produce(manifest,feature_dsn,tmp_path/'observations',min_free_bytes=0,batch_rows=4)
    order = [e['source_id'] for e in reversed(manifest['inputs'])]
    reader = ObservationReader(feature_dsn,report['run_id'],report['snapshot'],order,batch_rows=2,batch_bytes=128)
    stream = list(reader.stream())
    starts = [r for r in stream if isinstance(r,SourceStart)]
    ends = [r for r in stream if isinstance(r,SourceEnd)]
    assert [r.source_id for r in starts] == order == [r.source_id for r in ends]
    assert all(r.run_id == report['run_id'] and r.snapshot == report['snapshot'] for r in stream)
    assert (ends[0].messages,ends[0].elements) == (0,0)
    assert (ends[1].messages,ends[1].elements,ends[1].local_messages,ends[1].state_messages,ends[1].eor_records) == (4,3,1,1,1)
    messages = [m for b in stream if isinstance(b,MessageBatch) for m in b.messages]
    elements = [e for b in stream if isinstance(b,MessageBatch) for e in b.elements]
    assert len(messages) == 6 and len(elements) == 4
    assert any(m['kind']=='state_change' and m['old_state']==6 and m['new_state']==1 for m in messages)
    assert any(m['eor']==[{'afi':1,'safi':1}] for m in messages)
    assert any(e['local_message'] is True and e['peer_asn']==64497 and e['local_asn']==12654 for e in elements)
    assert all(isinstance(e['as_path_raw'],bytes) for e in elements)
    assert len({m['message_id'] for m in messages}) == len(messages)
    # 同run错snapshot、未声明源、重复源均拒绝；不读取latest。
    for snapshot, sources in [(report['snapshot']+1,order),(report['snapshot'],['unknown']),(report['snapshot'],[order[0],order[0]])]:
        with pytest.raises(ValueError): ObservationReader(feature_dsn,report['run_id'],snapshot,sources)
    with psycopg2.connect(feature_dsn) as pg, pg.cursor() as c:
        c.execute("UPDATE domeye.runs SET state='failed' WHERE run_id=%s", (report['run_id'],))
    with pytest.raises(ValueError): ObservationReader(feature_dsn,report['run_id'],report['snapshot'],order)
    with pytest.raises(ValueError): list(reader.stream())


def test_source_receipt_mismatch_never_emits_source_end(tmp_path, feature_dsn):
    manifest = observation_fixture(tmp_path)
    report = produce(manifest,feature_dsn,tmp_path/'observations',min_free_bytes=0)
    source = manifest['inputs'][1]['source_id']
    with psycopg2.connect(feature_dsn) as pg, pg.cursor() as c:
        c.execute('UPDATE domeye.source_receipts SET message_count=message_count+1 WHERE run_id=%s AND source_id=%s', (report['run_id'],source))
    reader = ObservationReader(feature_dsn,report['run_id'],report['snapshot'],[source])
    seen = []
    with pytest.raises(ValueError,match='计数'):
        for item in reader.stream(): seen.append(item)
    assert not any(isinstance(item,SourceEnd) for item in seen)

@pytest.mark.parametrize('field',['state','message_count','element_count'])
@pytest.mark.parametrize('during',[False,True])
def test_selected_source_qualification_and_receipt_drift(tmp_path,feature_dsn,field,during):
    manifest=observation_fixture(tmp_path)
    report=produce(manifest,feature_dsn,tmp_path/'observations',min_free_bytes=0)
    source=manifest['inputs'][1]['source_id']
    reader=ObservationReader(feature_dsn,report['run_id'],report['snapshot'],[source],batch_rows=2)
    records=reader.stream();seen=[]
    if during:
        seen.append(next(records));assert isinstance(seen[-1],SourceStart)
        # 已读取并发出批次后再修改资格，收尾也不得发End。
        seen.append(next(records));assert isinstance(seen[-1],MessageBatch)
    with psycopg2.connect(feature_dsn) as pg,pg.cursor() as c:
        if field=='state':
            c.execute("UPDATE domeye.inputs SET state='failed' WHERE run_id=%s AND source_id=%s",(report['run_id'],source))
        else:
            c.execute('UPDATE domeye.source_receipts SET '+field+'='+field+'+1 WHERE run_id=%s AND source_id=%s',(report['run_id'],source))
    with pytest.raises(ValueError,match='漂移'):
        for item in records:seen.append(item)
    assert not any(isinstance(item,SourceEnd) for item in seen)
