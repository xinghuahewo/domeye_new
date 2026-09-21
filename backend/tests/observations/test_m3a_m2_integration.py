"""真实人工 M2 PG 封存→公开 Reader→ordered；不实现业务消费者。"""
from collections import Counter
from dataclasses import asdict
import gzip
import hashlib
import json
from pathlib import Path
import resource
import struct
import time

import psycopg2
import pytest
from psycopg2.extras import Json
from tests.observations.test_observation_checkpoint import fixture, run, dsn
from tests.observations.test_observation_mrt import update
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.bgp.archive.message_reader import ObservationReader, SourceStart, SourceEnd, MessageBatch
from data_pipeline.bgp.ordered_reader import ordered, binding_from_reader
from data_pipeline.bgp import record_types as o


def make_input(root,bad):
    m=fixture(root,bad)
    e=m['inputs'][1];path=Path(e['path'])
    raw=gzip.decompress(path.read_bytes())+update(ann=struct.pack('!I',0)+b'\x18\xc0\0\x02',subtype=9)
    path.write_bytes(gzip.compress(raw,mtime=0));e['size']=path.stat().st_size;e['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    e['source_id']=source_identity(m['collector'],e['origin_uri'],e['sha256'])
    snapshot=dict(m['inputs'][0]);p=root/'extra-rib.gz';p.write_bytes(Path(snapshot['path']).read_bytes())
    snapshot.update(path=str(p),origin_uri='fixture://rrc25/extra-rib',role='snapshot')
    snapshot['source_id']=source_identity(m['collector'],snapshot['origin_uri'],snapshot['sha256'])
    m['inputs'].insert(1,snapshot);m['update_sources']=[r['source_id'] for r in m['inputs'] if r['role']=='update']
    return m


def reader_for(dsn,seal,sources):
    return ObservationReader(dsn,seal['run_id'],seal['snapshot'],sources,profile='observation',batch_rows=1)


def raw_sequence(parts,rank):
    output=[];messages=[];elements=[];qualities=[];start=None
    for part in parts:
        if isinstance(part,SourceStart):start=part;messages=[];elements=[];qualities=[]
        elif isinstance(part,MessageBatch):
            messages.extend(part.messages);elements.extend(part.elements);qualities.extend(part.source_quality)
        else:
            assert isinstance(part,SourceEnd)
            r=rank[part.source_id];output.append(('SourceStart',r,start))
            output.extend(('SourceQuality',r,q) for q in qualities)
            for m in messages:
                output.append(('MessageBoundary',r,m))
                output.extend(('Element',r,e) for e in elements if e['message_id']==m['message_id'])
            output.append(('SourceEnd',r,part))
    return output


@pytest.mark.parametrize('bad',[False,True])
def test_real_m2_ordered_full_and_subsets(tmp_path,dsn,bad):
    m=make_input(tmp_path/'input',bad);policy='isolate-payload/v1' if bad else 'strict/v1'
    t=time.monotonic();seal=run(m,dsn,tmp_path/'m2',policy=policy);produce_wall=time.monotonic()-t
    ids=[e['source_id'] for e in m['inputs']];ranks={s:i for i,s in enumerate(ids)};summary=[]
    for label,selected in [('full',ids),('snapshot_only',[ids[1]]),('without_snapshot',[ids[0],*ids[2:]])]:
        reader=reader_for(dsn,seal,selected);binding=binding_from_reader(reader)
        assert binding.ordered_source_ids==tuple(ids) and [s.role for s in binding.sources]==['baseline','snapshot','update','update']
        assert len(seal['checkpoints'])==5 and seal['checkpoints'][0]['source_id']==m['references'][0]['sha256']
        original=list(reader.stream());t=time.monotonic();actual=list(ordered(reader));wall=time.monotonic()-t
        restored=[]
        for item in actual:
            rank=item.position.source_rank if isinstance(item,o.MessageBoundary) else item.position.message.source_rank if isinstance(item,o.Element) else item.source_rank
            restored.append((type(item).__name__,rank,item.raw))
            assert item.binding_ref==binding.binding_id
        assert restored==raw_sequence(original,ranks)
        assert isinstance(actual[-1],o.SourceEnd)
        boundaries=[x for x in actual if isinstance(x,o.MessageBoundary)];elements=[x for x in actual if isinstance(x,o.Element)]
        assert len({(x.position.source_rank,x.position.record) for x in boundaries})==len(boundaries)
        assert all(x.raw_time.epoch==x.raw['epoch'] and x.raw_time.microsecond==x.raw['microsecond'] for x in boundaries)
        if label=='full':
            assert any(x.raw['path_id_present'] and x.raw['path_id']==0 for x in elements)
            assert any(isinstance(x.raw['attributes_raw'],bytes) and x.raw['attributes_raw'] for x in elements)
            assert any(x.raw_time.microsecond==123456 for x in boundaries)
            assert any(x.raw['kind']=='state_change' for x in boundaries) and any(x.raw['eor'] for x in boundaries)
            assert any(not any(e.raw['message_id']==x.raw['message_id'] for e in elements) for x in boundaries)
            assert sum(x.gap is not None for x in boundaries)==int(bad)
        summary.append({'selection':label,'ranks':[ranks[s] for s in selected],'items':len(actual),'messages':len(boundaries),'elements':len(elements),
                        'source_quality':sum(isinstance(x,o.SourceQuality) for x in actual),'message_quality':sum(len(x.raw['quality']) for x in boundaries),'ordered_wall_seconds':wall,
                        'raw_sequence_repr_sha256':hashlib.sha256(repr(restored).encode()).hexdigest()})
    (tmp_path/'接缝回执.json').write_text(json.dumps({'policy':policy,'manifest':m,'binding':asdict(binding),'seal_digest':seal['digest'],'produce_wall_seconds':produce_wall,
        'selections':summary,'process_lifetime_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'rss_scope':'macOS_current_pytest_process_lifetime_includes_DuckDB_not_PG'},ensure_ascii=False,indent=2))


@pytest.mark.parametrize('phase',['before_seal_damage','during_source_revocation'])
def test_real_pg_gate_tail_and_cleanup(tmp_path,dsn,monkeypatch,phase):
    m=make_input(tmp_path/'input',False);seal=run(m,dsn,tmp_path/'m2');source=m['inputs'][-1]['source_id']
    reader=reader_for(dsn,seal,[source]);original_connect=reader.connect;closed=[]
    class Tracked:
        def __init__(self,db):self.db=db
        def __getattr__(self,name):return getattr(self.db,name)
        def close(self):closed.append(True);self.db.close()
    monkeypatch.setattr(reader,'connect',lambda:Tracked(original_connect()))
    cp=next(x for x in seal['checkpoints'] if x['source_id']==source)
    def change(restore=False):
        with psycopg2.connect(dsn) as db,db.cursor() as cur:
            if phase=='before_seal_damage':
                cur.execute('UPDATE observation_m2.runs SET seal=%s WHERE run_id=%s',(Json(seal if restore else {**seal,'digest':'0'*64}),seal['run_id']))
            else:
                cur.execute('UPDATE observation_m2.checkpoints SET payload=%s WHERE run_id=%s AND ordinal=%s',
                    (Json(cp if restore else {**cp,'ingest':'revoked'}),seal['run_id'],cp['ordinal']))
    stream=ordered(reader);seen=[]
    try:
        if phase=='during_source_revocation':
            seen.extend([next(stream),next(stream)])
            assert isinstance(seen[-1],o.MessageBoundary)
        change()
        with pytest.raises(ValueError):
            for item in stream:seen.append(item)
        assert not any(isinstance(x,o.SourceEnd) for x in seen)
        assert stream.gi_frame is None
        assert len(closed)==int(phase=='during_source_revocation')
    finally:stream.close();change(restore=True)
    recovered=list(ordered(reader));assert isinstance(recovered[-1],o.SourceEnd)
    assert len(closed)==1+int(phase=='during_source_revocation')
    (tmp_path/'撤销与清理.json').write_text(json.dumps({'phase':phase,'prefix_items':len(seen),'success_end_emitted':False,'actual_duckdb_closes':len(closed),'restored_complete':True},ensure_ascii=False))


def test_actual_isolated_et_preserves_trusted_header(tmp_path,dsn):
    from tests.observations.test_observation_mrt import mrt
    m=make_input(tmp_path/'input',False);e=m['inputs'][2];path=Path(e['path'])
    bad=mrt(struct.pack('!I',900000)+update(attrs=b'\xf0\x23\x04\0\x04\x2f\x66')[12:],4,17)
    path.write_bytes(gzip.compress(bad,mtime=0));e.update(sha256=hashlib.sha256(path.read_bytes()).hexdigest(),size=path.stat().st_size)
    e['source_id']=source_identity(m['collector'],e['origin_uri'],e['sha256']);m['update_sources']=[x['source_id'] for x in m['inputs'] if x['role']=='update']
    seal=run(m,dsn,tmp_path/'m2',policy='isolate-payload/v1');reader=reader_for(dsn,seal,[e['source_id']])
    raw=[r for b in reader.stream() if isinstance(b,MessageBatch) for r in b.messages]
    assert len(raw)==1 and raw[0]['microsecond'] is None
    interpretation=json.loads(raw[0]['interpretation'])
    assert interpretation['status']=='rejected' and interpretation['header']['microsecond']==900000
    (tmp_path/'实际ET原Reader.json').write_text(json.dumps({'manifest':m,'seal_digest':seal['digest'],'run_id':seal['run_id'],'snapshot':seal['snapshot'],'raw_reader':raw},ensure_ascii=False,indent=2,default=str))
    items=list(ordered(reader))
    boundaries=[item for item in items if isinstance(item,o.MessageBoundary)]
    assert len(boundaries)==1 and boundaries[0].raw==raw[0]
    assert boundaries[0].gap.header.microsecond==900000
    assert isinstance(items[-1],o.SourceEnd)


def test_actual_mixed_et_time_and_complete_raw_sequence(tmp_path,dsn):
    from tests.observations.test_observation_mrt import mrt
    m=make_input(tmp_path/'input',False)
    bad=mrt(struct.pack('!I',900000)+update(attrs=b'\xf0\x23\x04\0\x04\x2f\x66')[12:],4,17)
    good=mrt(struct.pack('!I',500000)+update()[12:],4,17)
    unsupported=mrt(struct.pack('!I',900000)+update(attrs=b'\x80\x0e\x06\x00\x03\x01\x00\x00\x00')[12:],4,17)
    for entry,raw in zip(m['inputs'][2:],(bad+good+unsupported+good,good)):
        path=Path(entry['path']);path.write_bytes(gzip.compress(raw,mtime=0))
        entry.update(sha256=hashlib.sha256(path.read_bytes()).hexdigest(),size=path.stat().st_size)
        entry['source_id']=source_identity(m['collector'],entry['origin_uri'],entry['sha256'])
    m['update_sources']=[e['source_id'] for e in m['inputs'][2:]]
    seal=run(m,dsn,tmp_path/'m2',policy='isolate-payload/v1');ids=[e['source_id'] for e in m['inputs']]
    reader=reader_for(dsn,seal,ids);original=list(reader.stream());started=time.monotonic();items=list(ordered(reader));wall=time.monotonic()-started
    ranks={s:i for i,s in enumerate(ids)}
    restored=[]
    for item in items:
        rank=item.position.source_rank if isinstance(item,o.MessageBoundary) else item.position.message.source_rank if isinstance(item,o.Element) else item.source_rank
        restored.append((type(item).__name__,rank,item.raw))
    assert restored==raw_sequence(original,ranks)
    boundaries=[x for x in items if isinstance(x,o.MessageBoundary) and x.raw['source_id'] in m['update_sources']]
    assert [(x.raw['microsecond'],x.raw_time.microsecond) for x in boundaries]==[(None,900000),(500000,500000),(None,900000),(500000,500000),(500000,500000)]
    gaps=[x.gap for x in boundaries if x.gap]
    assert [g.parse_status.value for g in gaps]==['rejected','unsupported']
    assert all(g.raw_time.microsecond==g.header.microsecond==900000 for g in gaps)
    assert isinstance(items[-1],o.SourceEnd)
    (tmp_path/'混合ET完整接缝.json').write_text(json.dumps({'manifest':m,'run_id':seal['run_id'],'snapshot':seal['snapshot'],'seal_digest':seal['digest'],
        'items':len(items),'messages':sum(isinstance(x,o.MessageBoundary) for x in items),'elements':sum(isinstance(x,o.Element) for x in items),
        'ordered_wall_seconds':wall,'process_lifetime_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'time_pairs':[(x.raw['microsecond'],x.raw_time.microsecond) for x in boundaries],'gaps':[asdict(g) for g in gaps],
        'complete_raw_sequence_sha256':hashlib.sha256(repr(restored).encode()).hexdigest()},ensure_ascii=False,indent=2))
