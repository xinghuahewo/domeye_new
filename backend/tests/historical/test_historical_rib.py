"""Q3-C.3自己的人工原MRT/原生JSON/SQLite，接口全字段定向验收。"""
from contextlib import closing
from dataclasses import asdict, replace
import gzip
import hashlib
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import shutil
import sqlite3
import struct
import subprocess
import sys
import time
import uuid
import pytest

from data_pipeline.history.rib_index import Binding, Root, History, RibToken, CollectionToken, PGIdentity, freeze_collection
from data_pipeline.history.database_import import Token
from data_pipeline.history.database_import.freeze import private_pg
from data_pipeline.history.rib_index.model import TABLES, ORDER, row_bytes
from data_pipeline.bgp.snapshots import origin as rib_origin, path_comparison as rib_path_comparison
from tests.rib.test_rib_path_comparison import source, path, LEFT, RIGHT, PEERS
from tests.historical.test_historical_core import oracle_wire

REPO=Path(__file__).resolve().parents[3]
PROFILE=json.loads((REPO/'config/data-profile.json').read_text())
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def save(path,value):path.write_bytes(row_bytes(value))
def plain_save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,separators=(',',':'))+'\n')
def filemeta(path):return {'file':path.name,'sha256':sha(path),'bytes':path.stat().st_size}


def fixture(out,n=17):
    out.mkdir();staging=out.parent/('staging-'+uuid.uuid4().hex);staging.mkdir()
    p=path((2,[64496,64497]));q=path((2,[64496,64498]));unknown=path((1,[64497]))
    left_rows=[];right_rows=[]
    for family in (4,6):
        for i in range(n):
            prefix=f'10.{i//256}.{i%256}.0/24' if family==4 else f'2001:db8:{i:x}::/48'
            kind=i%5
            if kind!=3:left_rows.append((prefix,[(0,unknown if kind==4 else p,None)],False))
            if kind!=2:right_rows.append((prefix,[(0,unknown if kind==4 else q if kind==1 else p,None)],False))
    # 原同值重复entry和重复Peer属性都保留，不能被去重成same。
    peers=[*PEERS,PEERS[1]]
    left_rows.extend([('198.51.100.0/24',[(1,p,None),(2,p,None)],False),('203.0.113.0/24',[(0,p,path((2,[64499])))],False)])
    right_rows.extend([('198.51.100.0/24',[(1,p,None),(2,p,None)],False),('203.0.113.0/24',[(0,p,path((2,[64499])))],False)])
    left=source(staging,'l.gz',LEFT,peers,left_rows);right=source(staging,'r.gz',RIGHT,peers,right_rows)
    result=rib_path_comparison.compare_ribs(left,right,sha(left),sha(right),out/'comparison',PROFILE)
    (out/'comparison/manifest.pending').unlink()
    single_rows=[('192.0.2.0/24',[(0,path((2,[64496,64512])),None),(1,unknown,None)],False),
                 ('2001:db8:abcd::/48',[(0,p,None),(1,p,path((2,[64499])))],False)]
    single=source(out,'single.gz',RIGHT,PEERS,single_rows)
    rib_origin.retain_origins(single,sha(single),out/'origin',PROFILE)
    origin=json.loads((out/'origin/summary.json').read_text());scale=out/'scale';scale.mkdir()
    families={}
    for family,m in origin['families'].items():
        families[family]={k:m[k] for k in ('visible_prefixes','rib_entries')};families[family]['visible_origin_ases']=None
        if family!='all':families[family].update(peer_indices=sorted(map(int,m['peer_entry_counts'])),prefix_set_sha256=m['prefix_set_sha256'])
    families['all']['peer_indices']=sorted(set(families['ipv4']['peer_indices'])|set(families['ipv6']['peer_indices']))
    summary={'schema_version':'core-rib-scale/v1','interpretation_version':'rib-prefix-union/v1','data_profile':PROFILE,
        'source':{**origin['source'],'path':str(single),'collector_bgp_id':'0.0.0.25','view_name':'rrc25'},'observed_at':origin['observed_at'],
        'families':families,'origin_metric_state':'pending_definition','limits':['单时点人工原件；非连续状态']}
    plain_save(scale/'summary.json',summary);evidence={}
    from data_pipeline.overview.scale import EVIDENCE_NAMES
    (scale/'evidence').mkdir()
    for name in EVIDENCE_NAMES:
        (scale/'evidence'/name).write_text('人工原证据，非执行输入\n');evidence[name]={**filemeta(scale/'evidence'/name),'file':'evidence/'+name}
    plain_save(scale/'manifest.json',{'schema_version':'core-rib-scale-manifest/v1','summary':filemeta(scale/'summary.json'),'evidence':evidence,'retainer_sha256':'f'*64})
    spec=importlib.util.spec_from_file_location('fixture_binding',REPO/'scripts/core_overview/bind-core-overview-paths.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    comparison_manifest=json.loads((out/'comparison/manifest.json').read_text())
    consumed=module.retained_summary(out/'comparison',comparison_manifest,sha(out/'comparison/manifest.json'))
    consumer=out/'paths';consumer.mkdir();plain_save(consumer/'summary.json',consumed)
    shutil.copyfile(out/'comparison/manifest.json',consumer/'source-manifest.json')
    plain_save(consumer/'manifest.json',{'schema_version':'core-rib-path-package/v1','source_manifest_sha256':sha(consumer/'source-manifest.json'),
        'summary':filemeta(consumer/'summary.json'),'evidence':filemeta(consumer/'source-manifest.json'),'binding_code_sha256':'a'*64})
    # 原生消费包仅留source-manifest；八项原依赖必须显式绑定到自己的原比较目录。
    external=tuple(((consumer/'source-manifest.json').as_uri()+'/'+name,str(out/'comparison'/name),meta['sha256']) for name,meta in comparison_manifest['files'].items())
    # comparison原manifest也是另一根；消费中的同字节源manifest仍单独保留。
    roots=tuple(Root(profile,str(p),p.as_uri(),'fixture-h2-v1',sha(p)) for profile,p in [
        ('core-rib-consumption/v1',scale/'manifest.json'),('core-rib-consumption/v1',out/'origin/manifest.json'),
        ('core-rib-consumption/v1',consumer/'manifest.json'),('rib-comparison-jsonl/v1',out/'comparison/manifest.json')])
    shutil.rmtree(staging)
    return Binding(roots,(str(out),),external),{'single_rows':single_rows,'left_rows':left_rows,'right_rows':right_rows,'peers':peers}


@pytest.fixture(scope='module')
def lake():
    location=os.environ.get('Q3_PRIVATE_ROOT')
    if not location:pytest.skip('须明确自己的人工PG')
    root=Path(location);out=root/'q3-c3'/('acceptance-'+uuid.uuid4().hex);out.mkdir(parents=True)
    base=f'host={root/"socket"} port=28763';name='q3c3_'+uuid.uuid4().hex[:12]
    with closing(private_pg(base+' dbname=postgres',root)) as pg:
        pg.autocommit=True
        with pg.cursor() as c:c.execute('CREATE DATABASE '+name)
    dsn=base+' dbname='+name
    with closing(private_pg(dsn,root)) as pg:identity=PGIdentity.read(pg)
    h=History(dsn,root,target_identity=identity)
    binding,expected=fixture(out/'source')
    costs={};log=root/'q3-c3-pg.log'
    with Measure(log,out/'freeze.pg.log') as m:manifest=freeze_collection(binding,out/'freeze')
    costs['freeze']=m.result
    with Measure(log,out/'import.pg.log') as m:collection=h.import_collection(manifest)
    costs['import']=m.result
    with Measure(log,out/'project.pg.log') as m:token=h.project_rib(collection)
    costs['project']=m.result
    save(out/'binding.json',{'dsn':dsn,'root':str(root),'identity':asdict(identity),'token':asdict(token)})
    save(out/'costs.json',costs);print('Q3C3_EVIDENCE='+str(out))
    return h,token,out,expected


def test_all_native_tables_and_original_documents(lake):
    h,token,out,_=lake;actual={};documents_proof={};references_proof={}
    with h.rib(token) as s:
        for name in TABLES:actual[name]=[r for b in s.bulk(name,batch_rows=17) for r in b['rows']]
        for doc in actual['documents']:
            detail=s.detail(doc['document_id']);raw=(h.data_root/token.collection.collection_id/'source'/doc['entity_path']).read_bytes()[doc['byte_start']:doc['byte_end']]
            assert detail['raw']==raw and detail['exact']==oracle_wire(raw)
            documents_proof[str(doc['document_id'])]=detail
        for ref in actual['comparison_refs']:
            detail=s.reference(ref['document_id'],ref['object_ordinal'],ref['side'],ref['ref_ordinal'])
            assert detail['resolution']=='verified_original_mrt' and detail['reference']==ref
            frame=detail['frame'];raw=(h.data_root/token.collection.collection_id/'source'/frame['entity_path']).read_bytes()
            assert detail['raw_header_and_body']==raw[frame['decoded_offset']:frame['decoded_offset']+12+frame['body_bytes']]
            references_proof[':'.join(str(ref[k]) for k in ('document_id','object_ordinal','side','ref_ordinal'))]=detail
    assert s.receipt and not s.failed
    assert {r['status'] for r in actual['comparison_objects']}==set(rib_path_comparison.STATUSES)
    assert all(r['interval_change_count'] is None and r['session_continuity']=='unknown' for r in actual['path_endpoint_metrics'])
    assert s.receipt['H3']=='unresolved_external'
    save(out/'ordered-tables.json',actual);save(out/'complete.json',s.receipt)
    save(out/'document-proof.json',documents_proof);save(out/'reference-proof.json',references_proof)


def decoded_oracle(raw):
    """直接按fixture二进制逐字段解包；不调用H2 parser/Projector。"""
    peers=[];records=[];observations=[];offset=record=0
    while offset<len(raw):
        epoch,kind,subtype,size=struct.unpack_from('!IHHI',raw,offset);assert kind==13
        body=raw[offset+12:offset+12+size]
        if subtype==1:
            pos=6+int.from_bytes(body[4:6],'big');n=int.from_bytes(body[pos:pos+2],'big');pos+=2
            for i in range(n):
                flags=body[pos];width=16 if flags&1 else 4;asn_width=4 if flags&2 else 2
                peers.append({'index':i,'bgp_id':str(ipaddress.ip_address(body[pos+1:pos+5])),
                    'ip':str(ipaddress.ip_address(body[pos+5:pos+5+width])),'asn':int.from_bytes(body[pos+5+width:pos+5+width+asn_width],'big')})
                pos+=5+width+asn_width
            prefix=afi=None
        else:
            afi=1 if subtype in (2,8) else 2;bits=body[4];width=(bits+7)//8
            prefix=str(ipaddress.ip_address(body[5:5+width].ljust(4 if afi==1 else 16,b'\0')))+'/'+str(bits)
            pos=5+width;n=int.from_bytes(body[pos:pos+2],'big');pos+=2
            for i in range(n):
                peer,started=struct.unpack_from('!HI',body,pos);pos+=6;path_id=None
                if subtype in (8,10):path_id=int.from_bytes(body[pos:pos+4],'big');pos+=4
                length=int.from_bytes(body[pos:pos+2],'big');pos+=2;end=pos+length;attrs={}
                while pos<end:
                    flags,code=body[pos:pos+2];pos+=2;w=2 if flags&16 else 1
                    length=int.from_bytes(body[pos:pos+w],'big');pos+=w;attrs[code]=body[pos:pos+length];pos+=length
                interpreted=rib_origin._interpret(attrs.get(2),attrs.get(17))
                canonical,reasons=rib_path_comparison._path_value(attrs.get(2),attrs.get(17),{})
                if subtype in (8,10):reasons+=('add_path',)
                observations.append({'record':record,'entry_index':i,'decoded_offset':offset,'epoch':epoch,'subtype':subtype,'afi':afi,'safi':1,'prefix':prefix,
                    'peer_index':peer,**{k:peers[peer][k] for k in ('bgp_id','ip','asn')},'originated_time_epoch':started,'path_id':path_id,
                    'as_path':attrs.get(2),'as4_path':attrs.get(17),'canonical_path':json.dumps(canonical,separators=(',',':')),
                    'comparison_reasons':json.dumps(reasons,separators=(',',':')),'raw_origin_asn':interpreted['raw_origin_asn'],
                    'attributed_origin_asn':interpreted['attributed_origin_asn'],'origin_reason':interpreted['reason']})
        records.append({'record':record,'decoded_offset':offset,'body_bytes':size,'epoch':epoch,'subtype':subtype,'afi':afi,'safi':1 if afi else None,'prefix':prefix})
        offset+=12+size;record+=1
    return peers,records,observations


def table(s,name):return [r for b in s.bulk(name,batch_rows=17) for r in b['rows']]

def test_all_mrt_paths_peers_and_original_sqlite_columns(lake):
    h,token,out,_=lake
    with h.collection(token.collection) as s:files=table(s,'files')
    with h.rib(token) as s:
        all_obs=table(s,'mrt_observations');all_frames=table(s,'mrt_frames');all_peers=table(s,'peers');paths=table(s,'origin_paths')
    decoded_count=0
    for f in files:
        original=h.data_root/token.collection.collection_id/'source'/f['raw_path']
        if f['role']=='mrt-gzip':
            peers,frames,observations=decoded_oracle(gzip.decompress(original.read_bytes()));decoded_count+=len(observations)
            assert [r for r in all_obs if r['file_id']==f['file_id']]==[{'file_id':f['file_id'],**r} for r in observations]
            assert [r for r in all_frames if r['file_id']==f['file_id']]==[{'file_id':f['file_id'],'entity_path':f['decoded_path'],**r} for r in frames]
            assert [r for r in all_peers if r['file_id']==f['file_id']]==[{'file_id':f['file_id'],'peer_ordinal':r['index'],**{k:r[k] for k in ('bgp_id','ip','asn')}} for r in peers]
        if f['role']=='rib-sqlite':
            with closing(sqlite3.connect(original.as_uri()+'?mode=ro',uri=True)) as db:
                db.row_factory=sqlite3.Row
                if db.execute("SELECT name FROM sqlite_master WHERE name='paths'").fetchone():
                    expected=[dict(row) for row in db.execute('SELECT * FROM paths')]
                    actual=[{k:v for k,v in r.items() if k not in ('file_id','occurrence')} for r in paths if r['file_id']==f['file_id']]
                    assert actual==expected
    save(out/'independent-mrt-proof.json',{'all_observations':decoded_count,'all_fields_equal':True,'oracle':'独立struct解包+固定旧科学规则，未调用H2解释器'})


@pytest.mark.parametrize('limit',[1,17,60])
def test_complete_pages_and_filters(lake,limit):
    h,token,out,_=lake
    with h.rib(token) as s:
        expected=table(s,'comparison_objects');actual=[];cursor=None;before=s.budget.counts['hash_calls']
        while True:
            result=s.query('comparison_objects',limit=limit,cursor=cursor);actual+=result['rows'];cursor=result['next_cursor']
            if cursor is None:break
        assert actual==expected
        assert s.budget.counts['hash_calls']==before
        selected=[];cursor=None
        while True:
            result=s.query('comparison_objects',status='different',limit=limit,cursor=cursor);selected+=result['rows'];cursor=result['next_cursor']
            if cursor is None:break
        assert selected==[r for r in expected if r['status']=='different']
        for family,afi in [('ipv4',1),('ipv6',2)]:
            selected=[];cursor=None
            while True:
                response=s.query('comparison_objects',family=family,limit=limit,cursor=cursor);selected+=response['rows'];cursor=response['next_cursor']
                if cursor is None:break
            assert selected==[r for r in expected if r['afi']==afi]
    assert s.receipt
    save(out/('pages-'+str(limit)+'.json'),s.receipt)


def test_parameter_correction_suffix_and_unexhausted(lake):
    h,t,out,_=lake
    with h.rib(t) as s:
        with pytest.raises(ValueError):s.query('comparison_objects',file_id=-1)
        assert not s.failed
        first=s.query('comparison_objects',limit=1)
    assert s.receipt is None and not s.failed
    with h.rib(t) as s:
        cursor=first['next_cursor']
        while cursor:
            result=s.query('comparison_objects',limit=1,cursor=cursor);cursor=result['next_cursor']
    assert s.receipt and all(v['scope']=='query_suffix' for v in s.receipt['query_scopes'].values())
    save(out/'suffix-scope.json',s.receipt)


@pytest.mark.parametrize('fault',['close','final','budget','early_stop'])
def test_terminal_failures(lake,monkeypatch,fault):
    h,t,out,_=lake
    from tests.historical.test_historical_core_cleanup import ClosingDuck, closed
    events=[];s=h.rib(t)
    if fault=='close':
        with pytest.raises(RuntimeError):
            with s:
                table(s,'scale_family');db=s.db=ClosingDuck(s.db,events)
        closed(db)
    elif fault=='final':
        original=h._qualify_rib
        def fail(*a,**kw):
            result=original(*a,**kw)
            if kw.get('lock'):raise ValueError('人工最终资格错误')
            return result
        monkeypatch.setattr(h,'_qualify_rib',fail)
        with pytest.raises(ValueError,match='最终资格'):
            with s:table(s,'scale_family')
    elif fault=='budget':
        with s:
            s.query('comparison_objects',limit=1)
            original=h.limits;h.limits=replace(h.limits,batch_rows=h.limits.batch_rows+1)
            try:
                with pytest.raises(ValueError):s.query('comparison_objects',limit=1)
            finally:h.limits=original
    else:
        with s:
            stream=s.bulk('mrt_observations',batch_rows=1);next(stream);stream.close()
    assert s.failed and s.receipt is None and s.db is None
    save(out/('terminal-'+fault+'.json'),{'failed':s.failed,'receipt':s.receipt,'cleanup_errors':s.cleanup_errors,'events':events})


def refresh(out,binding):
    def put(p,value):
        if p.exists():p.chmod(0o600)
        plain_save(p,value)
    for folder in ('scale','origin'):
        p=out/folder/'manifest.json';m=json.loads(p.read_text())
        for entry in [m['summary'],*m['evidence'].values()]:
            f=p.parent/entry['file'];entry.update(sha256=sha(f),bytes=f.stat().st_size)
        put(p,m)
    p=out/'comparison/manifest.json';m=json.loads(p.read_text())
    for name,entry in m['files'].items():
        f=p.parent/name
        if f.exists():entry.update(sha256=sha(f),bytes=f.stat().st_size)
    put(p,m);put(out/'paths/source-manifest.json',m)
    summary=json.loads((out/'paths/summary.json').read_text());summary['comparison_version']='rib_path_comparison_v1_'+sha(p);put(out/'paths/summary.json',summary)
    package=json.loads((out/'paths/manifest.json').read_text());package['source_manifest_sha256']=sha(p);package['summary']=filemeta(out/'paths/summary.json');package['evidence']=filemeta(out/'paths/source-manifest.json');put(out/'paths/manifest.json',package)
    roots=tuple(replace(r,sha256=sha(Path(r.path))) for r in binding.roots)
    ext=tuple(((out/'paths/source-manifest.json').as_uri()+'/'+name,str(out/'comparison'/name),meta['sha256']) for name,meta in m['files'].items())
    return replace(binding,roots=roots,external_files=ext)


@pytest.mark.parametrize('fault',['frame','duplicate_frame','ref','peer','origin_path','fake_complete','missing_mrt'])
def test_self_consistent_bad_semantics_rejected(lake,fault):
    h,_,out,_=lake;case=out/('negative-'+fault);case.mkdir();source_root=case/'source';binding,_=fixture(source_root,n=3)
    if fault in ('frame','duplicate_frame'):
        p=source_root/'comparison/frames.sqlite';p.chmod(0o600)
        with closing(sqlite3.connect(p)) as db:
            if fault=='frame':db.execute('UPDATE frames SET offset=offset+1 WHERE rowid=1')
            else:
                row=db.execute('SELECT * FROM frames WHERE rowid=1').fetchone();db.execute('DELETE FROM frames WHERE rowid=2');db.execute('INSERT INTO frames VALUES (?,?,?,?,?,?,?)',row)
            db.commit()
    elif fault=='ref':
        p=source_root/'comparison/comparisons.jsonl.gz';original=gzip.decompress(p.read_bytes());values=[json.loads(v) for v in original.splitlines()]
        values[0]['objects'][0][2][0][1]=1
        p.chmod(0o600);p.write_bytes(gzip.compress(b''.join((json.dumps(v)+'\n').encode() for v in values),mtime=0))
    elif fault=='peer':
        p=source_root/'comparison/peer-groups.json';value=json.loads(p.read_text());value[0]['asn']+=1;p.chmod(0o600);plain_save(p,value)
    elif fault=='origin_path':
        p=source_root/'origin/paths.sqlite';p.chmod(0o600)
        with closing(sqlite3.connect(p)) as db:db.execute("UPDATE paths SET peer_index=peer_index+1");db.commit()
    elif fault=='fake_complete':
        p=source_root/'comparison/summary.json';value=json.loads(p.read_text());value['interval_change_count']=0;p.chmod(0o600);plain_save(p,value)
    else:(source_root/'comparison/left.mrt').unlink()
    binding=refresh(source_root,binding)
    with pytest.raises((ValueError,OSError,sqlite3.Error)) as error:
        manifest=freeze_collection(binding,case/'freeze')
        collection=h.import_collection(manifest)
        h.project_rib(collection)
    save(case/'rejection.json',{'fault':fault,'type':type(error.value).__name__,'reason':str(error.value),'changed_population':False})


def decode_token(value):
    c=value['collection'];collection=CollectionToken(**{**c,'children':tuple(Token(**t) for t in c['children'])})
    return RibToken(**{**value,'collection':collection})


def test_source_removed_new_process_full_read(lake):
    h,t,out,_=lake
    # 原始source和freeze移走后，不允许子进程回访原路径。
    (out/'source').rename(out/'source-removed');(out/'freeze').rename(out/'freeze-removed')
    script=out/'new-process.py'
    script.write_text('''import json,sys,hashlib
from pathlib import Path
from tests.historical.test_historical_rib import History, PGIdentity, decode_token, TABLES, row_bytes, table, save
p=Path(sys.argv[1]);v=json.loads((p/'binding.json').read_text());h=History(v['dsn'],v['root'],target_identity=PGIdentity(**v['identity']));t=decode_token(v['token'])
expected=json.loads((p/'ordered-tables.json').read_text());result={}
documents=json.loads((p/'document-proof.json').read_text());references=json.loads((p/'reference-proof.json').read_text())
with h.rib(t) as s:
 for name in TABLES:
  actual=table(s,name);assert json.loads(row_bytes(actual))==expected[name];result[name]=len(actual)
 for doc in expected['documents']:
  detail=s.detail(doc['document_id']);assert json.loads(row_bytes(detail))==documents[str(doc['document_id'])]
 for ref in expected['comparison_refs']:
  value=s.reference(ref['document_id'],ref['object_ordinal'],ref['side'],ref['ref_ordinal']);key=':'.join(str(ref[k]) for k in ('document_id','object_ordinal','side','ref_ordinal'));assert json.loads(row_bytes(value))==references[key]
assert s.receipt;save(p/'new-process-result.json',{'rows':result,'receipt':s.receipt,'all_fields_equal':True})
''')
    env=dict(os.environ,PYTHONPATH=str(REPO/'backend'))
    result=subprocess.run([sys.executable,str(script),str(out)],env=env,capture_output=True,text=True)
    (out/'new-process.log').write_text(result.stdout+result.stderr);assert result.returncode==0,result.stderr
    assert json.loads((out/'new-process-result.json').read_text())['all_fields_equal']


class Measure:
    def __init__(self,pglog,output):self.pglog,self.output=pglog,output
    def __enter__(self):
        self.start=time.monotonic();self.offset=self.pglog.stat().st_size;self.calls=self.bytes=0;self.original=hashlib.sha256;owner=self
        class Hash:
            def __init__(self,data=b'',*a,**kw):
                owner.calls+=1;owner.bytes+=len(data);self.actual=owner.original(data,*a,**kw)
            def update(self,data):owner.bytes+=len(data);return self.actual.update(data)
            def __getattr__(self,key):return getattr(self.actual,key)
        hashlib.sha256=Hash
        return self
    def __exit__(self,*_):
        hashlib.sha256=self.original
        with self.pglog.open('rb') as f:f.seek(self.offset);data=f.read()
        self.output.write_bytes(data)
        self.result={'wall_seconds':time.monotonic()-self.start,'all_python_sha256_calls':self.calls,'all_python_sha256_bytes':self.bytes,
                     'pg_logged_statements':sum(b' statement:' in line or b' execute ' in line for line in data.splitlines()),
                     'pg_log':str(self.output),'scope':'显式单阶段Python所有SHA256及自己PG实际日志；不是所有OS读写'}


def test_old_collection_and_h1_original_tokens_unchanged(lake):
    h,_,out,_=lake;root=h.root
    original=root/'q3-c2-cleanup/acceptance-bf3ca8823cf94024aba15bf8c1a6997c/binding.json'
    v=json.loads(original.read_text())
    from tests.historical.test_historical_core import decode_token as core_decode
    from data_pipeline.history.event_index import History as H1History
    from data_pipeline.history.event_index.model import TABLES as H1TABLES
    core=core_decode(v['token']);reader=H1History(v['dsn'],root,target_identity=PGIdentity(**v['identity']))
    files=[root/'history'/core.profile_id/'ready.json',root/'history'/core.collection.collection_id/'ready.json']
    before=[sha(p) for p in files]
    with reader.collection(core.collection) as s:
        for info in s.ready['tables']:
            digest=hashlib.sha256();count=0
            from data_pipeline.history.event_collection.store import row_bytes as cbytes
            for b in s.bulk(info['name'],batch_rows=17):
                for row in b['rows']:digest.update(cbytes(row)+b'\n');count+=1
            assert (count,digest.hexdigest())==(info['rows'],info['sha256'])
    expected=json.loads((root/'q3-c2/acceptance-513d7b18ff7542cda5dc7087a2ef9841/ordered-domain.json').read_text())
    with reader.core(core) as s:
        for name in H1TABLES:assert table(s,name)==expected[name]
    assert s.receipt and [sha(p) for p in files]==before
    save(out/'old-collection-h1-compatibility.json',{'original_token':asdict(core),'original_ready_sha256':before,'unchanged':True,'reimported':False,'reprojected':False})


def test_fixed_small_fixture_costs(lake):
    h,t,out,_=lake;log=h.root/'q3-c3-pg.log';results={}
    for cap in (1,17,1000):
        with Measure(log,out/f'cost-bulk-{cap}.pg.log') as m:
            with h.rib(t) as s:
                for name in TABLES:
                    for batch in s.bulk(name,batch_rows=cap):pass
        results['bulk_'+str(cap)]={**m.result,'resources':s.receipt['resources']}
    for limit in (1,17,60):
        with Measure(log,out/f'cost-pages-{limit}.pg.log') as m:
            with h.rib(t) as s:
                cursor=None;pages=0;count=0
                while True:
                    response=s.query('comparison_objects',limit=limit,cursor=cursor);pages+=1;count+=len(response['rows']);cursor=response['next_cursor']
                    if cursor is None:break
        results['page_'+str(limit)]={**m.result,'resources':s.receipt['resources'],'pages':pages,'rows':count}
    assert len({results['bulk_'+str(cap)]['all_python_sha256_calls'] for cap in (1,17,1000)})==1
    assert len({results['page_'+str(limit)]['resources']['hash_calls'] for limit in (1,17,60)})==1
    save(out/'query-costs.json',results)


def test_comparison_occurrences_and_origin_members_from_original_text(lake):
    h,t,out,_=lake
    with h.collection(t.collection) as s:files=table(s,'files')
    with h.rib(t) as s:
        actual={name:table(s,name) for name in ('documents','comparison_objects','comparison_frames','comparison_refs','comparison_reasons','mrt_observations','origin_members')}
    mids=[next(f['file_id'] for f in files if f['role']=='mrt-gzip' and f['uri'].endswith('/'+side+'.gz')) for side in ('left','right')]
    observations={(r['file_id'],r['record'],r['entry_index']):r for r in actual['mrt_observations']}
    expected={k:[] for k in ('comparison_objects','comparison_frames','comparison_refs','comparison_reasons','origin_members')}
    source_root=h.data_root/t.collection.collection_id/'source'
    for f in files:
        if f['role']=='comparisons':
            documents=[d for d in actual['documents'] if d['file_id']==f['file_id']]
            for i,line in enumerate((source_root/f['decoded_path']).read_bytes().splitlines()):
                row=json.loads(line);doc=documents[i]['document_id'];fid=f['file_id']
                for side,label in enumerate(('left','right')):
                    for position,(record,offset,subtype) in enumerate(row[label+'_frames']):
                        expected['comparison_frames'].append(dict(file_id=fid,document_id=doc,side=side,frame_position=position,record=record,offset=offset,subtype=subtype,mrt_file=mids[side]))
                for oi,(group,status,left,right,reasons) in enumerate(row['objects']):
                    expected['comparison_objects'].append(dict(file_id=fid,document_id=doc,line_ordinal=i,object_ordinal=oi,afi=row['afi'],safi=row['safi'],prefix=row['prefix'],group_id=group,status=status))
                    for ri,reason in enumerate(reasons):expected['comparison_reasons'].append(dict(file_id=fid,document_id=doc,object_ordinal=oi,reason_ordinal=ri,reason=reason))
                    for side,refs in enumerate((left,right)):
                        for ri,(position,entry) in enumerate(refs):
                            record,offset,_=row[('left','right')[side]+'_frames'][position]
                            expected['comparison_refs'].append(dict(file_id=fid,document_id=doc,object_ordinal=oi,side=side,ref_ordinal=ri,frame_position=position,entry_index=entry,mrt_file=mids[side],record=record,decoded_offset=offset,peer_index=observations[mids[side],record,entry]['peer_index'],resolution='verified_original_mrt'))
        if f['role']=='origins':
            value=json.loads((source_root/f['raw_path']).read_bytes())
            for family in sorted(value):
                for i,asn in enumerate(value[family]):expected['origin_members'].append(dict(file_id=f['file_id'],family=family,member_ordinal=i,asn=asn))
    for name,rows in expected.items():assert actual[name]==rows,name
    save(out/'original-array-occurrences-proof.json',{'rows':{k:len(v) for k,v in expected.items()},'all_original_values_and_positions_equal':True})


def test_zero_comparison_denominator_is_null(lake):
    h,_,out,_=lake;case=out/'zero-denominator';case.mkdir();binding,_=fixture(case/'source',n=0)
    manifest=freeze_collection(binding,case/'freeze');collection=h.import_collection(manifest);token=h.project_rib(collection)
    with h.rib(token) as s:metrics=table(s,'path_endpoint_metrics')
    assert s.receipt and all(r['comparable_pairs']==0 and r['different_fraction'] is None for r in metrics)
    save(case/'proof.json',{'token':asdict(token),'metrics':metrics,'receipt':s.receipt})


def test_sql_primary_preserved_when_actual_close_also_fails(lake):
    h,t,out,_=lake
    import duckdb
    from tests.historical.test_historical_core_cleanup import ClosingDuck, closed
    events=[]
    with h.rib(t) as s:
        db=s.db=ClosingDuck(s.db,events,sql_error=True)
        with pytest.raises(duckdb.CatalogException) as error:s.query('comparison_objects')
        assert error.value is db.primary and error.value.cleanup_errors
    closed(db);assert s.failed and s.receipt is None
    save(out/'sql-primary-and-close.json',{'primary_type':type(error.value).__name__,'cleanup_errors':error.value.cleanup_errors,'failed':s.failed,'receipt':s.receipt})


@pytest.mark.parametrize('primary_failure',[False,True])
def test_actual_pg_close_and_final_primary(lake,monkeypatch,primary_failure):
    h,t,out,_=lake
    from tests.historical.test_historical_core_cleanup import ClosingPG, ClosingDuck, closed
    events=[];connections=[];connect=h._connect;qualify=h._qualify_rib;primary=ValueError('H2原末尾资格失败')
    def open_pg():
        connection=ClosingPG(connect(),events,True);connections.append(connection);return connection
    def final(*a,**kw):
        result=qualify(*a,**kw)
        if kw.get('lock') and primary_failure:raise primary
        return result
    with pytest.raises(ValueError if primary_failure else RuntimeError) as error:
        with h.rib(t) as s:
            table(s,'origin_members');db=s.db=ClosingDuck(s.db,events,fail=primary_failure)
            monkeypatch.setattr(h,'_connect',open_pg);monkeypatch.setattr(h,'_qualify_rib',final)
    assert s.failed and s.receipt is None and s.db is None
    assert connections and all(p.actual.closed for p in connections);closed(db)
    if primary_failure:assert error.value is primary and len(primary.cleanup_errors)==2
    save(out/('pg-close-'+str(primary_failure)+'.json'),{'failed':s.failed,'receipt':s.receipt,'error_type':type(error.value).__name__,'cleanup_errors':s.cleanup_errors,'events':events})


def test_projection_catalog_close_revokes_candidate(lake,monkeypatch):
    h,t,out,_=lake;original=h._connect;connections=[]
    class Connection:
        def __init__(self,actual,fail):self.actual,self.fail=actual,fail
        def __getattr__(self,key):return getattr(self.actual,key)
        def __enter__(self):self.actual.__enter__();return self
        def __exit__(self,*a):return self.actual.__exit__(*a)
        def close(self):
            self.actual.close()
            if self.fail:raise RuntimeError('H2 writer PG real close fault')
    def connect():
        c=Connection(original(),len(connections)==1);connections.append(c);return c
    monkeypatch.setattr(h,'_connect',connect)
    with pytest.raises(RuntimeError,match='writer PG real close'):h.project_rib(t.collection)
    assert connections and all(c.actual.closed for c in connections)
    with closing(original()) as pg,pg.cursor() as c:
        c.execute("SELECT profile_id,state,reason FROM history_q3.rib_profiles WHERE reason=%s",('H2 writer PG real close fault',));rows=c.fetchall()
    assert len(rows)==1 and rows[0][1]=='failed'
    save(out/'projection-close-revocation.json',{'registration':rows,'all_native_owners_closed':True,'token_returned':False})
