"""本任务隔离PG：多run人工观察与独立无损国家参考接缝。"""
from dataclasses import asdict, replace
from datetime import datetime, timezone, timedelta
import gzip
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import uuid

import psycopg2
import pytest

from data_pipeline.bgp.replay.run_from_files import produce
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.analysis.resources.bindings import SourceBinding
from data_pipeline.analysis.resources.identity import ROOT, identity_digest
from data_pipeline.analysis.resources.produce import produce_bound_resources
from data_pipeline.analysis.resources.references import parse_rows, register_reference, read_reference, scan_reference_rows, read_reference_original
from data_pipeline.analysis.resources.store import scan_resource


@pytest.fixture
def resource_dsn():
    base=os.environ.get('DOMEYE_RESOURCE_TEST_DSN')
    if not base:pytest.skip('仅本任务独立PG')
    pg=psycopg2.connect(base);pg.autocommit=True
    name='resource_multi_'+uuid.uuid4().hex
    with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    pg.close()
    return base+' dbname='+name


def launch(request,path,root=ROOT):
    path.write_text(json.dumps(request,ensure_ascii=False))
    result=subprocess.run([sys.executable,str(root/'scripts/pipeline/resource-frozen-run.py'),str(path)],check=True,capture_output=True,text=True)
    return json.loads(result.stdout)


def bind_request(dsn,upstream,contexts,manifest,root,*,fresh=False):
    path=root/'supplement.json'
    path.write_text('{"9808":{"asn":"9808","aut_name":"a","country":"X","country_cn":"未知","org_name":"o"},'
                    '"100":{"asn":"100","aut_name":"b","country":NaN,"country_cn":"甲","org_name":"p"},'
                    '"100":{"asn":"100","aut_name":"b","country":NaN,"country_cn":"未知","org_name":"p"}}')
    args=dict(dsn=dsn,path=str(path),output=str(root/'reference'),origin_uri='fixture://as_dict',content_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    ref=launch({'operation':'register_reference',**args},root/'reference-request.json') if fresh else register_reference(**args,fixture_only=True)
    country={'reference_id':ref['reference_id'],'dataset_id':ref['dataset_id']}
    _,identity=read_reference(dsn,**country,allow_fixture=not fresh)
    csv={'run_id':upstream['run_id'],'snapshot':upstream['snapshot'],'source_id':manifest['references'][0]['sha256'],'anchor_source_id':contexts[0].source_id}
    reference_id=identity_digest({'csv':csv,'country':identity})
    bound=[SourceBinding(upstream['run_id'],upstream['snapshot'],'result',replace(c,reference_id=reference_id)) for c in contexts]
    return dict(dsn=dsn,sources=bound,result_window=[contexts[0].snapshot_time,contexts[-1].snapshot_time+timedelta(hours=8)],
                csv_binding=csv,country_binding=country,output=str(root/'resource'),min_free_bytes=0),ref


def json_request(request):
    return {**request,'sources':[{**asdict(s),'context':{**asdict(s.context),'snapshot_time':s.context.snapshot_time.isoformat()}} for s in request['sources']],
            'result_window':[t.isoformat() for t in request['result_window']]}


def test_lossless_tokens_duplicates_positions():
    raw=' { "1" : {"asn":"1","country":NaN,"country_cn":"甲","country_cn":"未知","org_name":"é"}, "1":{"country":null,"country_cn":"未知"} } '.encode()
    rows=list(parse_rows(raw))
    assert len(rows)==2
    for row in rows:
        assert raw[row['byte_offset']:row['byte_offset']+row['byte_length']].decode()==row['raw_entry']
        json.dumps(row['value'],allow_nan=False)
    assert rows[0]['value']['pairs'][1][1]=={'kind':'nonstandard_constant','token':'NaN','value_state':'non_finite'}
    assert rows[1]['value']['pairs'][0][1]=={'kind':'scalar','value':None}
    assert any(q['status']=='duplicate_key_last_wins' for q in rows[0]['quality'])
    assert any(q['status']=='duplicate_key_last_wins' for q in rows[1]['quality'])
    assert rows[0]['country_scope']=='legacy_unknown'
    with pytest.raises(ValueError):list(parse_rows(b'{"1":{},}'))


def test_reference_lifecycle_and_exact_binding(tmp_path,resource_dsn,monkeypatch):
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres')
    db.execute('ATTACH '+literal('ducklake:postgres:'+resource_dsn)+' AS lake (DATA_PATH '+literal(str(tmp_path/'catalog'))+')');db.close()
    path=tmp_path/'ref';path.write_bytes(b'{"1":{"country":NaN,"country_cn":"unknown"}}')
    args=dict(dsn=resource_dsn,path=path,origin_uri='fixture://ref',content_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),fixture_only=True)
    report=register_reference(**args,output=tmp_path/'ok')
    assert read_reference_original(resource_dsn,report['reference_id'],report['dataset_id'],allow_fixture=True)==path.read_bytes()
    saved=list(scan_reference_rows(resource_dsn,report['reference_id'],report['dataset_id'],allow_fixture=True))
    assert saved[0]['value']['pairs'][0][1]['token']=='NaN'
    with pytest.raises(ValueError):read_reference(resource_dsn,report['reference_id'],report['dataset_id'])
    with pytest.raises(ValueError):read_reference(resource_dsn,report['reference_id'],'wrong',allow_fixture=True)
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT manifest,row_count FROM domeye.resource_references WHERE reference_id=%s',(report['reference_id'],))
        binding,count=c.fetchone();assert Path(binding['raw_path']).read_bytes()==path.read_bytes() and count==1
        c.execute("UPDATE domeye.resource_references SET state='candidate' WHERE reference_id=%s",(report['reference_id'],))
    with pytest.raises(ValueError):read_reference(resource_dsn,report['reference_id'],report['dataset_id'],allow_fixture=True)
    with pytest.raises(ValueError):register_reference(**{**args,'content_sha256':'0'*64},output=tmp_path/'bad')
    rejected=json.loads((tmp_path/'bad/failure.json').read_text())['capture']
    assert rejected['expected_sha256']=='0'*64 and rejected['actual_sha256']==args['content_sha256']
    assert rejected['capture_state']=='complete' and Path(rejected['raw_path']).read_bytes()==path.read_bytes()
    assert Path(rejected['raw_path']).parent.name=='quarantine'
    for name,limits in [('size',{'max_bytes':8}),('rss',{'max_rss_bytes':1}),('disk',{'min_free_bytes':10**30})]:
        with pytest.raises(ValueError):register_reference(**args,output=tmp_path/name,**limits)
        failure=json.loads((tmp_path/name/'failure.json').read_text())
        assert failure['state']=='failed' and failure['capture']['capture_state']!='complete'
        assert not (tmp_path/name/'original/as_dict.json').exists()
    single=register_reference(**args,output=tmp_path/'single-large-row',batch_bytes=1)
    assert read_reference_original(resource_dsn,single['reference_id'],single['dataset_id'],allow_fixture=True)==path.read_bytes()
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
        c.execute("SELECT state FROM domeye.resource_references WHERE content_sha256=%s",('0'*64,));assert c.fetchone()==('failed',)
    original_open=Path.open
    def fail_receipt(file,*a,**kw):
        if file.name=='execution.json' and file.parent.name=='receipt-failure':raise OSError('人工回执写失败')
        return original_open(file,*a,**kw)
    with monkeypatch.context() as patch:
        patch.setattr(Path,'open',fail_receipt)
        with pytest.raises(OSError):register_reference(**args,output=tmp_path/'receipt-failure')
    failure=json.loads((tmp_path/'receipt-failure/failure.json').read_text())
    assert failure['state']=='failed'
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT manifest,state FROM domeye.resource_references WHERE reference_id=%s',(failure['reference_id'],))
        binding,state=c.fetchone();assert Path(binding['raw_path']).read_bytes()==path.read_bytes() and state=='failed'


def test_twelve_points_two_runs_window_guards_and_topology(tmp_path,resource_dsn,monkeypatch):
    from tests.resources.test_resource_store import fixture_inputs
    from tests.observations.test_observation_mrt import update
    manifest,contexts=fixture_inputs(tmp_path,ribs=12)
    first=datetime(2026,2,24,16,tzinfo=timezone.utc)
    newcontexts=[]
    for index,(entry,context) in enumerate(zip(manifest['inputs'],contexts)):
        epoch=int((first+timedelta(hours=8*index)).timestamp())
        path=Path(entry['path']);raw=bytearray(gzip.decompress(path.read_bytes()));offset=0
        while offset<len(raw):
            size=struct.unpack_from('!I',raw,offset+8)[0]
            struct.pack_into('!I',raw,offset,epoch);offset+=12+size
        if index==9:
            converted=bytearray();at=0
            while at<len(raw):
                stamp,kind,subtype,length=struct.unpack_from('!IHHI',raw,at)
                body=raw[at+12:at+12+length]
                if subtype==2:
                    body=body[:16]+struct.pack('!I',42)+body[16:];subtype=8
                converted.extend(struct.pack('!IHHI',stamp,kind,subtype,len(body))+body);at+=12+length
            raw=converted
        path.write_bytes(gzip.compress(raw,mtime=0));sha=hashlib.sha256(path.read_bytes()).hexdigest()
        entry.update(sha256=sha,size=path.stat().st_size,source_id=source_identity('rrc25',entry['origin_uri'],sha))
        newcontexts.append(replace(context,source_id=entry['source_id'],content_sha256=sha,snapshot_time=datetime.fromtimestamp(epoch,timezone.utc)))
    manifest['window_start']=first.isoformat();manifest['window_end_exclusive']=(first+timedelta(days=4)).isoformat()
    warm={**manifest,'inputs':manifest['inputs'][:9],'baseline_source':newcontexts[0].source_id}
    day={**manifest,'inputs':[{**e,'role':'baseline' if i==0 else 'snapshot'} for i,e in enumerate(manifest['inputs'][9:])],'baseline_source':newcontexts[9].source_id}
    # D中有额外UPDATE，Reader应只取选中的3个RIB，不能进入Resource决策/成员。
    path=tmp_path/'update.gz';path.write_bytes(gzip.compress(update(),mtime=0));sha=hashlib.sha256(path.read_bytes()).hexdigest();uri='fixture://update'
    entry={'path':str(path),'sha256':sha,'size':path.stat().st_size,'origin_uri':uri,'source_id':source_identity('rrc25',uri,sha),'role':'update'}
    day['inputs'].append(entry);day['update_sources']=[entry['source_id']]
    w=produce(warm,resource_dsn,tmp_path/'warm',min_free_bytes=0,catalog_data_path=tmp_path/'observation-parquet')
    d=produce(day,resource_dsn,tmp_path/'day',min_free_bytes=0,catalog_data_path=tmp_path/'observation-parquet')
    request,ref=bind_request(resource_dsn,d,newcontexts[9:],day,tmp_path,fresh=True)
    rid=request['sources'][0].context.reference_id
    request['sources']=[SourceBinding(w['run_id'],w['snapshot'],'warmup',replace(c,reference_id=rid)) for c in newcontexts[:9]]+request['sources']
    report=launch(json_request(request),tmp_path/'twelve-request.json')
    request['fixture_only']=True
    def rows(table,scope='result'):
        return [r for b in scan_resource(resource_dsn,report['run_id'],table,allow_fixture=True,scope=scope) for r in b.to_pylist()]
    assert len(rows('sources'))==3 and len(rows('sources','all'))==12
    assert {r['source_id'] for r in rows('sources')}=={c.source_id for c in newcontexts[9:]}
    assert len(rows('decision_refs','all'))==24
    differences=[json.loads(r['detail']) for r in rows('decoding_differences')]
    assert len(differences)==2 and all(r['old_fixed_path']=='42' and r['as_path_text']=='9808 100' for r in differences)
    assert all(r['decoder_version']=='mrt-fields-peer-path-et/v1' for r in rows('sources'))
    assert not (tmp_path/'resource/observations.duckdb').exists()
    assert all(r['country_scope']=='legacy_unknown' for r in rows('topology_edges','all'))
    assert len(rows('topology_edges','all'))==12
    assert len(rows('topology_status'))==3
    globals=[r for r in rows('normal_bands') if r['bucket']=='global']
    assert globals and all(r['list_len']>=9 for r in globals)
    assert any(r['sample_time']<request['result_window'][0] for r in rows('normal_samples'))
    assert list((tmp_path/'observation-parquet'/('resource_'+report['run_id'])).rglob('*.parquet'))
    assert report['storage_layout']['catalog_data_path'].rstrip('/')==str(tmp_path/'observation-parquet')
    badcases=[
        [replace(request['sources'][0],snapshot=w['snapshot']+100),*request['sources'][1:]],
        [request['sources'][0],*request['sources']],
        [request['sources'][1],request['sources'][0],*request['sources'][2:]],
        [replace(request['sources'][0],context=replace(request['sources'][0].context,collector='rrc26')),*request['sources'][1:]],
    ]
    for i,sources in enumerate(badcases):
        with pytest.raises(ValueError):produce_bound_resources(**{**request,'sources':sources,'output':str(tmp_path/f'bad-{i}')})
    for state in ('candidate','failed'):
        with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:c.execute('UPDATE domeye.runs SET state=%s WHERE run_id=%s',(state,w['run_id']))
        with pytest.raises(ValueError):produce_bound_resources(**{**request,'output':str(tmp_path/state)})
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:c.execute("UPDATE domeye.runs SET state='complete' WHERE run_id=%s",(w['run_id'],))
    from data_pipeline.analysis.resources.store import ResourceStore
    original=ResourceStore.state
    def drift(store,state):
        original(store,state)
        if state.rib_count==12:
            with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:c.execute("UPDATE domeye.runs SET state='failed' WHERE run_id=%s",(w['run_id'],))
    monkeypatch.setattr(ResourceStore,'state',drift)
    with pytest.raises(ValueError):produce_bound_resources(**{**request,'output':str(tmp_path/'drift')})
    assert not (tmp_path/'drift/execution.json').exists()
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:c.execute("UPDATE domeye.runs SET state='complete' WHERE run_id=%s",(w['run_id'],))
    def reference_drift(store,state):
        original(store,state)
        if state.rib_count==12:
            # 已消费12点后，同计数改规范历史country投影，元数据摘要仍不变。
            import pyarrow as pa
            import pyarrow.parquet as pq
            directory=Path(ref['storage_layout']['catalog_data_path'])/ref['storage_layout']['schema_path']
            file=next(directory.rglob('*.parquet'));before=file.read_bytes();table=pq.read_table(file)
            index=table.schema.get_field_index('country_cn')
            with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
                c.execute('SELECT data_file_id,footer_size FROM public.ducklake_data_file WHERE path=%s',(file.name,))
                file_id,footer=c.fetchone()
            pq.write_table(table.set_column(index,table.schema.field(index),pa.array(['changed']*table.num_rows)),file)
            after=file.read_bytes()
            delta=int.from_bytes(after[-8:-4],'little')-int.from_bytes(before[-8:-4],'little')
            with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
                c.execute('UPDATE public.ducklake_data_file SET file_size_bytes=%s,footer_size=%s WHERE data_file_id=%s',(len(after),footer+delta,file_id))
    monkeypatch.setattr(ResourceStore,'state',reference_drift)
    with pytest.raises(ValueError,match='规范历史内容'):
        produce_bound_resources(**{**request,'output':str(tmp_path/'reference-drift')})
    assert json.loads((tmp_path/'reference-drift/failure.json').read_text())['state']=='failed'
    assert not (tmp_path/'reference-drift/execution.json').exists()



def test_reference_file_lake_directory_identity_and_same_count_tamper(tmp_path,resource_dsn):
    from data_pipeline.bgp.archive.store import connect_duckdb, literal
    import pyarrow as pa
    import pyarrow.parquet as pq
    db=connect_duckdb();db.execute('LOAD ducklake');db.execute('LOAD postgres')
    db.execute('ATTACH '+literal('ducklake:postgres:'+resource_dsn)+' AS lake (DATA_PATH '+literal(str(tmp_path/'catalog'))+')');db.close()
    path=tmp_path/'input.json';raw=b'{"1":{"country_cn":"A","country":NaN}}';path.write_bytes(raw)
    report=register_reference(resource_dsn,path,tmp_path/'reference',origin_uri='fixture://storage',content_sha256=hashlib.sha256(raw).hexdigest(),fixture_only=True)
    args=(resource_dsn,report['reference_id'],report['dataset_id'])
    assert Path(report['raw_path']).read_bytes()==raw
    assert read_reference(*args,allow_fixture=True)[0]['1'][0]=='A'
    schema_dir=Path(report['storage_layout']['catalog_data_path'])/report['storage_layout']['schema_path']
    files=list(schema_dir.rglob('*.parquet'));assert files
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
        c.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='domeye' AND table_name='resource_references'")
        assert 'raw_bytes' not in {r[0] for r in c.fetchall()}
        c.execute("SELECT to_regclass('domeye.resource_reference_rows')");assert c.fetchone()[0] is None
    # 同计数实际Parquet投影改值；raw_value/原件未改，完整行摘要仍必须拒绝。
    file=files[0];saved=file.read_bytes();table=pq.read_table(file)
    index=table.schema.get_field_index('country_cn')
    altered=table.set_column(index,table.schema.field(index),pa.array(['B']*table.num_rows))
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
        c.execute('SELECT data_file_id,file_size_bytes,footer_size FROM public.ducklake_data_file WHERE path=%s',(file.name,))
        file_id,old_size,old_footer=c.fetchone()
    pq.write_table(altered,file)
    changed=file.read_bytes()
    footer_delta=int.from_bytes(changed[-8:-4],'little')-int.from_bytes(saved[-8:-4],'little')
    # 同步容器尺寸元数据，排除单纯Parquet页脚损坏；保留原row_count与发布摘要。
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
        c.execute('UPDATE public.ducklake_data_file SET file_size_bytes=%s,footer_size=%s WHERE data_file_id=%s',(len(changed),old_footer+footer_delta,file_id))
    with pytest.raises(ValueError,match='规范历史内容'):read_reference(*args,allow_fixture=True)
    file.write_bytes(saved)
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
        c.execute('UPDATE public.ducklake_data_file SET file_size_bytes=%s,footer_size=%s WHERE data_file_id=%s',(old_size,old_footer,file_id))
    original=Path(report['raw_path']);original.chmod(0o644);original.write_bytes(raw.replace(b'NaN',b'123'))
    with pytest.raises(ValueError,match='原件摘要'):list(scan_reference_rows(*args,allow_fixture=True))
    original.write_bytes(raw);original.chmod(0o444)
    # 目录元数据改变，即使行数和内容相同也不能沿用原身份。
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE public.ducklake_metadata SET value=%s WHERE key='data_path'",(str(tmp_path/'other'),))
    with pytest.raises(ValueError,match='目录身份'):read_reference_original(*args,allow_fixture=True)
    with psycopg2.connect(resource_dsn) as pg,pg.cursor() as c:
        c.execute("UPDATE public.ducklake_metadata SET value=%s WHERE key='data_path'",(report['storage_layout']['catalog_data_path'],))
        c.execute("UPDATE domeye.resource_references SET manifest=jsonb_set(manifest,'{snapshot}',to_jsonb(%s::bigint)) WHERE reference_id=%s",(report['snapshot']+1,report['reference_id']))
    with pytest.raises(ValueError,match='登记身份'):read_reference(*args,allow_fixture=True)
