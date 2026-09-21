#!/usr/bin/env python3
"""人工路由的内存斜率及原始 MRT→批次→归档→状态→完整校验实测，不读取真实输入。"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import resource
import struct
import sys
import time
import threading
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))
from data_pipeline.bgp.replay.route_replay import Replay, ReplayPlan
from data_pipeline.common.run_metrics import Metrics
from types import SimpleNamespace


def rss():
    if sys.platform=='linux':return int(Path('/proc/self/statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def memory(args):
    started=time.monotonic();cpu=time.process_time();before=rss()
    replay=Replay(ReplayPlan('synthetic','rib',('u',)),compact=args.compact,detailed=not args.compact,legacy_views=not args.compact)
    peers=[dict(ip=f'192.0.2.{i+1}',asn=64400+i,local_ip='192.0.2.254',local_asn=12654,interface=0) for i in range(16)]
    paths=[dict(path_key=hashlib.sha256(str(i).encode()).hexdigest(),as_path_text=f'64400 {130000+i}',attributed_origin_asn=130000+i) for i in range(args.paths)]
    business=None
    if args.business:
        if not args.compact:raise ValueError('业务内存实验使用紧凑规范态')
        from data_pipeline.bgp.state.business import BusinessLoop
        from data_pipeline.bgp.state.path_dictionary import TextPool
        fake=dict(collector='synthetic',baseline_source='rib',update_sources=['u'],inputs=[dict(source_id='rib'),dict(source_id='u')],
            window_start='1970-01-01T00:00:00Z',window_end_exclusive='1970-01-02T00:00:00Z')
        pool=TextPool();pool.register([p['as_path_text'] for p in paths])
        business=BusinessLoop(business_config(fake),dict(manifest=fake),dict(binding_id='synthetic',observation_run='memory'),pool,lambda *a:None)
    for i in range(args.states):
        prefix=f'10.{(i//16)>>16&255}.{(i//16)>>8&255}.{i//16&255}/32';peer=peers[i%16];path=paths[i%len(paths)]
        m=SimpleNamespace(source_id='u',record=i,message_id=f'u:{i}',epoch=100,kind='update',peer=peer,
            paths=[path],elements=[dict(peer=peer,action='announce',prefix=prefix,afi=1,safi=1,
            path_key=path['path_key'],ordinal=0,path_id_present=False,path_id=None)])
        for _ in replay.consume(m):pass
        if business is not None:
            row=dict(action='rib_snapshot',prefix=prefix,peer_asn=peer['asn'],as_path_text=path['as_path_text'],epoch=100,event_id=f'rib:{i}:0')
            for routes in business.features.values():routes.observe(row,i)
            business.detection_routes.apply('RIB',prefix,str(peer['asn']),path['as_path_text'])
    if business is not None:
        from data_pipeline.analysis.features.calculation import initialize
        business.engine=business.make_engine(business.detection_routes)
        for mode in business.features:business.work[mode]=initialize(mode,business.projection(mode,'rib','complete'),business.reference,'memory')
    after=rss()
    result=dict(mode='compact' if args.compact else 'legacy',states=len(replay.current),unique_paths=len(paths),
        wall_seconds=time.monotonic()-started,cpu_seconds=time.process_time()-cpu,base_rss_bytes=before,
        final_rss_bytes=after,rss_increment_bytes=after-before,bytes_per_state=(after-before)/args.states,
        state_payload_bytes=sum(len(k)+len(v) for k,v in replay.memory.prefix_dict.items()) if args.compact else None,
        includes='实际紧凑规范态，无重复兼容视图；不含解析/归档/PG/Feature/Detection' if args.compact else '实际旧 Replay 含兼容 Prefix×ASN 索引；不含解析/归档/PG/Feature/Detection',energy_joules=None)
    if business is not None:
        result['includes']='紧凑规范态、普通/IR（IR 基线空）共享路径视图、树、既有 Detection 基线观察集合、Feature 计数、10000 或指定数量路径；不含解析/归档/PG及后续活动事件增长'
    return result


def mrt(body,subtype,kind=13,epoch=100):return struct.pack('!IHHI',epoch,kind,subtype,len(body))+body


def inputs(root,prefixes,peers):
    root.mkdir();entries=[]
    peer_table=b'\0\0\0\x19\0\x05rrc25'+struct.pack('!H',peers)
    for i in range(peers):peer_table+=b'\x02'+bytes([192,0,2,i+1])*2+struct.pack('!I',64400+i)
    def attrs(i):return b'\x40\x02\x0a\x02\x02'+struct.pack('!II',64400+i,130000+i%16)
    def prefix(i):return b'\x20'+struct.pack('!I',0x0a000000+i)
    def save(name,role,writer):
        path=root/(name+'.gz')
        with path.open('wb') as output,gzip.GzipFile(fileobj=output,mode='wb',mtime=0) as stream:writer(stream)
        digest=hashlib.sha256(path.read_bytes()).hexdigest();uri='fixture://rrc25/'+name
        from data_pipeline.bgp.input.mrt_reader import source_identity
        entries.append(dict(path=str(path),sha256=digest,source_id=source_identity('rrc25',uri,digest),origin_uri=uri,size=path.stat().st_size,role=role))
    def rib(stream):
        stream.write(mrt(peer_table,1))
        for i in range(prefixes):
            body=struct.pack('!I',i)+prefix(i)+struct.pack('!H',peers)
            for vp in range(peers):
                raw=attrs(vp);body+=struct.pack('!HIH',vp,90,len(raw))+raw
            stream.write(mrt(body,2))
    save('rib','baseline',rib)
    for f in range(3):
        def update(stream):
            for vp in range(peers):
                for start in range(0,min(prefixes,1000),32):
                    announced=b''.join(prefix(i) for i in range(start,min(start+32,prefixes,1000)))
                    raw=attrs(vp+f);withdrawn=announced if f==1 else b'';announced=b'' if f==1 else announced
                    payload=struct.pack('!H',len(withdrawn))+withdrawn+struct.pack('!H',len(raw))+raw+announced
                    bgp=b'\xff'*16+struct.pack('!HB',len(payload)+19,2)+payload
                    endpoint=struct.pack('!IIHH',64400+vp,12654,0,1)+bytes([192,0,2,vp+1])+bytes([192,0,2,254])
                    stream.write(mrt(endpoint+bgp,4,16,101+f))
        save(f'update-{f}','update',update)
    return dict(schema_version='observation-run/v1',collector='rrc25',window_start='1970-01-01T00:00:00Z',
        window_end_exclusive='1970-01-02T00:00:00Z',inputs=entries,baseline_source=entries[0]['source_id'],update_sources=[r['source_id'] for r in entries[1:]])


def business_config(manifest):
    from datetime import datetime,timezone
    def stamp(n):return datetime.fromtimestamp(n,timezone.utc).isoformat()
    codes={str(130000+i):'ZZ' for i in range(64)}
    maps={k:{} for k in ('as_info','prefix_info','country','important_as_dict','important_prefix_dict','as_prefix_dict',
        'as_rel_dict','important_domain_dict','private_as_dict','triplet_info')}
    maps['country']={'ZZ':{'chinese_short_name':'人工测试国'}}
    maps['as_info']={asn:dict(org_name='Synthetic'+asn,org_name_cn='',as_country='ZZ',as_country_cn='人工测试国',
        as_name='AS'+asn,descr='',descr_cn='',admin_info='',type='ISP',import_as=[],export_as=[],is_ddos_provider=False,
        v4Peer=[],v6Peer=[],sibling_as=[]) for asn in codes}
    kinds=('prefix_outage','as_outage','country_outage','event','moas','hijack','sub_hijack','leak_phenomenon','leak')
    return dict(feature_reference=dict(version='synthetic/v1',country_names={a:'人工测试国' for a in codes},country_codes=codes,big_countries={}),
        detection_reference=dict(version='synthetic/v1',raw_rows={'scope':'仅人工数据'},row_refs={'scope':'fixture:synthetic'},mappings=maps),
        windows={entry['source_id']:dict(start=stamp(100+i),end=stamp(101+i),file_time=stamp(100+i),coverage='complete',
            message_quality_state='complete',legacy_tables={k:k+'_197001' for k in kinds}) for i,entry in enumerate(manifest['inputs'])})


class BenchResources:
    """独立测试数据库 cgroup 和本次目录的采样；不读取或分摊整机功耗。"""
    def __init__(self,root,cgroup):
        self.root=root;self.cgroup=Path(cgroup) if cgroup else None
        self.stop=threading.Event();self.pg_peak=None;self.disk_peak=0;self.process_group_rss_peak=0;self.samples=0;self.native_queue_peak=0;self.native_directory_peak=0
    def counters(self):
        if self.cgroup is None:return None
        cpu={k:int(v) for k,v in (l.split() for l in (self.cgroup/'cpu.stat').read_text().splitlines())}
        io={}
        for line in (self.cgroup/'io.stat').read_text().splitlines():
            for entry in line.split()[1:]:
                k,v=entry.split('=');io[k]=io.get(k,0)+int(v)
        return dict(cpu=cpu,io=io)
    def sample(self):
        while True:
            try:
                if self.cgroup is not None:
                    self.pg_peak=max(self.pg_peak or 0,int((self.cgroup/'memory.current').read_text()))
                self.disk_peak=max(self.disk_peak,sum(p.stat().st_size for p in self.root.rglob('*') if p.is_file()))
                pids=[os.getpid()]
                if sys.platform=='linux':
                    pids+=list(map(int,Path(f'/proc/self/task/{os.getpid()}/children').read_text().split()))
                    combined=0
                    for pid in pids:
                        try:combined+=int(Path(f'/proc/{pid}/statm').read_text().split()[1])*os.sysconf('SC_PAGE_SIZE')
                        except FileNotFoundError:pass
                    self.process_group_rss_peak=max(self.process_group_rss_peak,combined)
                for native in self.root.glob('run/m2/native-file-*/native'):
                    try:consumed=int((native/'consumed-count').read_text())
                    except (FileNotFoundError,ValueError):consumed=0
                    self.native_queue_peak=max(self.native_queue_peak,len(list(native.glob('segment-*/committed.json')))-consumed)
                    self.native_directory_peak=max(self.native_directory_peak,len(list(native.glob('segment-*')))-consumed)
                self.samples+=1
            except FileNotFoundError:pass
            if self.stop.wait(.25):return
    def __enter__(self):
        self.before=self.counters();self.thread=threading.Thread(target=self.sample,daemon=True);self.thread.start();return self
    def __exit__(self,*exc):self.stop.set();self.thread.join()
    def result(self):
        after=self.counters()
        delta=None if after is None else {kind:{k:v-self.before[kind].get(k,0) for k,v in values.items()} for kind,values in after.items()}
        return dict(private_pg=delta,private_pg_sampled_memory_peak_bytes=self.pg_peak,artifact_peak_bytes=self.disk_peak,
            native_unconsumed_segments_sampled_peak=self.native_queue_peak,native_unconfirmed_directories_sampled_peak=self.native_directory_peak,
            python_and_native_sampled_rss_peak_bytes=self.process_group_rss_peak,samples=self.samples,interval_seconds=.25,
            pg_scope='explicit_private_cgroup; includes its background maintenance',energy_joules=None)


def pipeline(args):
    import psycopg2
    from psycopg2 import sql
    from data_pipeline.bgp.pipeline import run_pipeline
    base=os.environ['DOMEYE_M2_TEST_DSN'];name='batch_bench_'+uuid.uuid4().hex
    root=Path(args.output).resolve();root.mkdir(parents=True,exist_ok=False)
    manifest=inputs(root/'inputs',args.prefixes,args.peers)
    (root/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    pg=psycopg2.connect(base);pg.autocommit=True
    with pg.cursor() as c:c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    start=time.monotonic();cpu=time.process_time();children=resource.getrusage(resource.RUSAGE_CHILDREN)
    monitor=BenchResources(root,args.pg_cgroup)
    try:
        monitor.__enter__()
        receipt=run_pipeline(manifest,base+' dbname='+name,root/'run',native_build=os.environ['DOMEYE_NATIVE_BUILD'],
            batch_rows=args.batch_rows,min_free_bytes=256*1024**2,max_rss_bytes=6*1024**3,
            business_config=business_config(manifest) if args.business else None)
        elapsed=time.monotonic()-start;child_after=resource.getrusage(resource.RUSAGE_CHILDREN)
        count=args.prefixes*args.peers+3*min(args.prefixes,1000)*args.peers
        with pg.cursor() as c:
            c.execute('SELECT pg_database_size(%s)',(name,));db_size=c.fetchone()[0]
        result=dict(qualification=receipt['qualification'],elements=count,states=args.prefixes*args.peers,
            wall_seconds=elapsed,cpu_seconds=time.process_time()-cpu,elements_per_second=count/elapsed,
            child_cpu_seconds=child_after.ru_utime+child_after.ru_stime-children.ru_utime-children.ru_stime,resources=monitor.result(),
            process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
            artifact_bytes=sum(p.stat().st_size for p in root.rglob('*') if p.is_file()),database_bytes=db_size,
            energy_joules=None,scope='人工 1 RIB + 3 UPDATE；含全部归档、状态提交、完整 EOF 和封存校验；'+('普通/IR Feature（IR 基线空）及既有 Detection 全规则、活动事件事务' if args.business else '不含业务模块'),
            metrics=[json.loads(p.read_text()) for p in (root/'run').glob('route-metrics-*.json')])
        (root/'结果.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
        return result
    finally:
        if hasattr(monitor,"thread"):monitor.__exit__(None,None,None)
        with pg.cursor() as c:
            c.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s',(name,))
            c.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(name)))
        pg.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('mode',choices=['memory','pipeline'])
    parser.add_argument('--business',action='store_true');parser.add_argument('--compact',action='store_true');parser.add_argument('--states',type=int,default=100000)
    parser.add_argument('--paths',type=int,default=10000);parser.add_argument('--prefixes',type=int,default=5000)
    parser.add_argument('--peers',type=int,default=16);parser.add_argument('--batch-rows',type=int,default=10000)
    parser.add_argument('--output');parser.add_argument('--pg-cgroup');args=parser.parse_args()
    if not 0<args.states<=2000000 or not 0<args.paths<=1000000 or not 0<args.prefixes<=100000 or not 0<args.peers<=128:
        parser.error('仅允许有界人工实验')
    print(json.dumps(memory(args) if args.mode=='memory' else pipeline(args),ensure_ascii=False))

if __name__=='__main__':main()
