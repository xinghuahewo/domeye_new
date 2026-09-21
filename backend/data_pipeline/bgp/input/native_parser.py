"""显式选择的原生首份 RIB 候选；默认生产入口不切换。"""
from dataclasses import asdict
from contextlib import nullcontext
import ctypes
import gzip
import hashlib
import json
import os
from pathlib import Path
import resource
import struct
import subprocess
import time

import pyarrow as pa
import pyarrow.ipc as ipc

from data_pipeline.bgp.archive.checkpoint import OBS_TABLES, durable, file_sha, sha
from data_pipeline.bgp.input.mrt_reader import Message, decode
from data_pipeline.bgp.input.mrt_types import HeaderEvidence, Interpretation, ParseStatus, ReadPolicy
from data_pipeline.bgp.archive.store import TYPES

VERSION='native-mrt-columns/bgpdump-v3'
PARSER_AUTHORITY='RIPE-NCC/bgpdump@63fe1c50c7d07bb4c57d4fcc690696adc9b3c306'
# 文本以 bgpdump 为准；独立样本核对原字节和业务字段，不强制旧解析器的文本表现。
DISPLAY_FIELDS={'messages':set(),'elements':{'prefix','peer_ip','bgp_id'},
                'paths':{'as_path_text','as4_path_text'},'peers':{'ip','bgp_id'}}


def message_rows(m):
    """只用于少量独立审计样本；主导入不构造逐元素 Python 对象。"""
    result={name:[] for name in OBS_TABLES}
    row={name:getattr(m,name,None) for name,_ in OBS_TABLES['messages']}
    row.update(message_id=m.message_id,bgp_id_present=False,interpretation=json.dumps(asdict(m.interpretation),ensure_ascii=False))
    result['messages'].append(row)
    result['peers']=[{k:p.get(k) for k,_ in OBS_TABLES['peers']} for p in m.peers]
    for e in m.elements:
        p=e['peer'];row={**e,'event_id':m.message_id+':'+str(e['ordinal']),'message_id':m.message_id,
            'peer_ip':p['ip'],'peer_asn':p['asn'],'bgp_id':p['bgp_id'],'bgp_id_present':True,
            'peer_table_record':p['table_record'],'peer_index':p['index']}
        result['elements'].append({k:row.get(k) for k,_ in OBS_TABLES['elements']})
    result['paths']=list({p['path_key']:p for p in m.paths}.values())
    return result


def _stamp(path):
    s=path.stat();return [s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns]


def release_segment_cache(directory):
    """仅释放已经读完的临时 IPC 页缓存；文件与校验回执仍保留。"""
    if not hasattr(os,'posix_fadvise'):return
    for path in directory.glob('*.arrow'):
        with path.open('rb') as stream:
            os.posix_fadvise(stream.fileno(),0,0,os.POSIX_FADV_DONTNEED)


def inspect_source(path,source_id,expected_sha,root,*,source_kind='rib'):
    """完整 gzip EOF/CRC、物理帧摘要与计数；分层样本复用项目解释器。"""
    start=time.monotonic();path=Path(path);before=_stamp(path);records=elements=offset=0
    h=hashlib.sha256();peers=[];peer_record=None;epoch=None;samples={k:[] for k in OBS_TABLES};frames=[];prefixes=[];peer_count=0;families=set()
    frame_schema=pa.schema([(n,TYPES[t]) for n,t in OBS_TABLES['messages'] if n in ('record','offset','length','epoch','mrt_type','mrt_subtype','raw_digest')])
    prefix_schema=pa.schema([('afi',pa.int32()),('raw_prefix',pa.binary())])
    class Reader:
        def __init__(self,f):self.f=f
        def read(self,n=-1):data=self.f.read(n);h.update(data);return data
    with path.open('rb') as raw, gzip.GzipFile(fileobj=Reader(raw)) as stream, pa.OSFile(str(root/'source-frames.arrow'),'wb') as output, pa.OSFile(str(root/'source-prefixes.arrow'),'wb') as prefix_output:
        writer=ipc.new_file(output,frame_schema)
        prefix_writer=ipc.new_file(prefix_output,prefix_schema)
        while head:=stream.read(12):
            if len(head)!=12:raise ValueError('MRT 头截断')
            stamp,kind,subtype,size=struct.unpack('!IHHI',head)
            if source_kind=='rib' and (kind!=13 or subtype not in (1,2,4)):raise ValueError('RIB 来源角色不符')
            if source_kind=='update' and kind not in (16,17):raise ValueError('UPDATE 来源角色不符')
            if size>16*1024**2 or offset+size+12>12*1024**3 or records>=2000000:raise ValueError('MRT 资源限制')
            if epoch is None:epoch=stamp
            if source_kind=='rib' and epoch!=stamp:raise ValueError('RIB 时点冲突')
            body=stream.read(size)
            if len(body)!=size:raise ValueError('MRT 正文截断')
            digest=hashlib.sha256(head+body).hexdigest()
            frames.append(dict(record=records,offset=offset,length=size+12,epoch=stamp,mrt_type=kind,mrt_subtype=subtype,raw_digest=digest))
            if source_kind=='update':
                # 独立核对物理帧；NLRI 原字节由原生适配器与 libbgpdump 逐项对照。
                if kind==17 and (len(body)<4 or struct.unpack_from('!I',body)[0]>999999):raise ValueError('MRT 微秒头无效')
                prefixes.append(dict(afi=None,raw_prefix=None));records+=1;offset+=size+12
                if len(frames)>=65536:
                    writer.write_table(pa.Table.from_pylist(frames,schema=frame_schema));frames.clear()
                    prefix_writer.write_table(pa.Table.from_pylist(prefixes,schema=prefix_schema));prefixes.clear()
                continue
            first_family=False
            if subtype==1:
                peer_count+=1;prefixes.append(dict(afi=None,raw_prefix=None))
            else:
                if len(body)<7:raise ValueError('RIB 头截断')
                bits=body[4]
                if bits>(32 if subtype==2 else 128):raise ValueError('前缀长度越界')
                count_at=5+(bits+7)//8
                if count_at+2>len(body):raise ValueError('元素计数截断')
                elements+=struct.unpack_from('!H',body,count_at)[0]
                if elements>80000000:raise ValueError('元素数资源边界')
                afi=1 if subtype==2 else 2;raw_prefix=body[4:count_at]
                prefixes.append(dict(afi=afi,raw_prefix=raw_prefix))
                first_family=afi not in families;families.add(afi)
            if subtype==1 or first_family or records<16 or records%4096==0:
                m=Message(source_id,records,offset,size+12,stamp,kind,subtype,digest,content_sha256=expected_sha)
                peers,peer_record=decode(m,body,peers,peer_record)
                from dataclasses import replace
                m.interpretation=replace(m.interpretation,source_path=str(path))
                for k,rows in message_rows(m).items():samples[k].extend(rows)
            records+=1;offset+=size+12
            if len(frames)>=65536:
                writer.write_table(pa.Table.from_pylist(frames,schema=frame_schema));frames.clear()
                prefix_writer.write_table(pa.Table.from_pylist(prefixes,schema=prefix_schema));prefixes.clear()
        if frames:
            writer.write_table(pa.Table.from_pylist(frames,schema=frame_schema))
            prefix_writer.write_table(pa.Table.from_pylist(prefixes,schema=prefix_schema))
        writer.close();prefix_writer.close()
    if _stamp(path)!=before or h.hexdigest()!=expected_sha:raise ValueError('源身份或摘要不符')
    if source_kind=='rib' and (not records or not peer_count):raise ValueError('空 RIB 或缺少 Peer 表')
    samples['paths']=list({r['path_key']:r for r in samples['paths']}.values())
    for name,rows in samples.items():
        if rows:
            schema=pa.schema([(n,TYPES[t]) for n,t in OBS_TABLES[name]])
            with pa.OSFile(str(root/('sample-'+name+'.arrow')),'wb') as f:
                with ipc.new_file(f,schema) as w:w.write_table(pa.Table.from_pylist(rows,schema=schema))
    result=dict(records=records,elements=elements if source_kind=='rib' else None,source_kind=source_kind,decoded_bytes=offset,peer_tables=peer_count,source_stamp=before,
        sha256=expected_sha,gzip_eof_verified=True,sample_rows={k:len(v) for k,v in samples.items()},seconds=time.monotonic()-start)
    durable(root/'source-audit.json',result);return result


class NativeRuntime:
    def __init__(self,build):
        self.build=Path(build).resolve();self.identity=json.loads((self.build/'build.json').read_text())
        for name in ('domeye-rib','libdomeye_digest.so','libsqlite3.so.0'):
            if file_sha(self.build/name)!=self.identity['files'][name]:raise ValueError('原生构建漂移')
        self.lib=ctypes.CDLL(str(self.build/'libdomeye_digest.so'))
        self.lib.domeye_digest.argtypes=[ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p]
        self.lib.domeye_digest.restype=ctypes.c_int
        self.lib.domeye_combine.argtypes=self.lib.domeye_digest.argtypes
        self.lib.domeye_combine.restype=ctypes.c_int
        self.lib.domeye_hash_rows.argtypes=[ctypes.c_void_p,ctypes.c_void_p,ctypes.c_int,ctypes.c_void_p]
        self.lib.domeye_hash_rows.restype=ctypes.c_int
        self.lib.domeye_merge_owners.argtypes=[ctypes.c_void_p,ctypes.c_char_p,ctypes.c_char_p,ctypes.c_void_p,ctypes.c_void_p]
        self.lib.domeye_merge_owners.restype=ctypes.c_int
        self.lib.domeye_owner_rows.argtypes=[ctypes.c_char_p,ctypes.c_void_p,ctypes.c_void_p]
        self.lib.domeye_owner_rows.restype=ctypes.c_int
        self.lib.domeye_pack_rib.argtypes=[ctypes.c_void_p]*4+[
            ctypes.c_uint32,ctypes.c_uint32,ctypes.c_uint64,ctypes.c_int64,ctypes.c_int64,ctypes.c_void_p]
        self.lib.domeye_pack_rib.restype=ctypes.c_int

    def endpoints(self,path,source_id,expected_sha,output,*,policy='strict/v1',guard=lambda:None):
        """固定原件的端点预扫；正常解码与隔离规则沿用同版原生代码。"""
        path=Path(path).resolve();output=Path(output);before=_stamp(path)
        if file_sha(path)!=expected_sha:raise ValueError('端点预扫源摘要不符')
        env={**os.environ,'DOMEYE_NATIVE_POLICY':ReadPolicy(policy).value,'DOMEYE_ENDPOINT_PARENT_PID':str(os.getpid())}
        with output.open('wb') as out,output.with_suffix('.stderr').open('wb') as err:
            child=subprocess.Popen([str(self.build/'domeye-rib'),'--endpoints',str(path),source_id],stdout=out,stderr=err,env=env)
            try:
                while child.poll() is None:
                    guard()
                    if output.stat().st_size>32*1024**2:raise ValueError('端点盘点输出上限')
                    time.sleep(.05)
                if child.returncode:raise ValueError('原生端点盘点失败')
            finally:
                if child.poll() is None:
                    child.terminate()
                    try:child.wait(timeout=10)
                    except subprocess.TimeoutExpired:child.kill();child.wait(timeout=10)
        if _stamp(path)!=before or file_sha(path)!=expected_sha:raise ValueError('端点预扫期间原件漂移')
        if output.stat().st_size>32*1024**2:raise ValueError('端点盘点输出上限')
        result=json.loads(output.read_text());result.update(source_id=source_id,sha256=expected_sha,source_stamp=before)
        return result

    def digest(self,reader):
        return self._reduce(reader,self.lib.domeye_digest)

    def combine(self,reader):
        return self._reduce(reader,self.lib.domeye_combine)

    def _reduce(self,reader,function):
        # Arrow C Stream 在同一进程移交所有权；不往返 Python 行对象。
        stream=(ctypes.c_void_p*5)();reader._export_to_c(ctypes.addressof(stream))
        output=ctypes.create_string_buffer(65);count=ctypes.c_int64();error=ctypes.create_string_buffer(1024)
        code=function(ctypes.addressof(stream),output,ctypes.byref(count),error)
        if code:raise ValueError(error.value.decode())
        return count.value,output.value.decode()

    def hash_rows(self,reader,mode):
        stream=(ctypes.c_void_p*5)();output=(ctypes.c_void_p*5)();error=ctypes.create_string_buffer(1024)
        reader._export_to_c(ctypes.addressof(stream))
        code=self.lib.domeye_hash_rows(ctypes.addressof(stream),ctypes.addressof(output),mode,error)
        if code:raise ValueError(error.value.decode())
        return pa.RecordBatchReader._import_from_c(ctypes.addressof(output))

    def pack_rib(self,elements,endpoints,paths,source,rank,record,epoch,previous):
        """已校验 RIB 的必要列原生批量编码；不向归档增加另一份路由表。"""
        for value in (source,rank):
            if not 0<=value<2**32:raise ValueError('RIB 来源编号越界')
        if record is None:record=2**64-1
        if not 0<=record<2**64 or not -(2**63)<=epoch<2**63:
            raise ValueError('RIB 位置或时间越界')
        endpoint_table=pa.table({
            'peer_ip':pa.array([key[0] for key in endpoints],type=pa.string()),
            'peer_asn':pa.array([key[1] for key in endpoints],type=pa.int64()),
            'endpoint_id':pa.array(list(endpoints.values()),type=pa.int64())})
        path_table=pa.table({'path_key':pa.array(list(paths),type=pa.string()),
            'origin':pa.array([value['attributed_origin_asn'] for value in paths.values()],type=pa.int64())})
        streams=[(ctypes.c_void_p*5)() for _ in range(3)]
        output=(ctypes.c_void_p*5)();error=ctypes.create_string_buffer(1024)
        from data_pipeline.bgp.state.rib_columns import FIELDS
        fields=(*FIELDS,'message_id') if record==2**64-1 else FIELDS
        for table,stream in zip((elements.select(fields),endpoint_table,path_table),streams):
            table.to_reader()._export_to_c(ctypes.addressof(stream))
        if self.lib.domeye_pack_rib(*(ctypes.addressof(s) for s in streams),ctypes.addressof(output),
                source,rank,record,epoch,previous,error):
            raise ValueError(error.value.decode())
        return pa.RecordBatchReader._import_from_c(ctypes.addressof(output)).read_all()

    def merge_owners(self,reader,path,attempt):
        stream=(ctypes.c_void_p*5)();reader._export_to_c(ctypes.addressof(stream))
        count=ctypes.c_int64();error=ctypes.create_string_buffer(1024)
        if self.lib.domeye_merge_owners(ctypes.addressof(stream),os.fsencode(path),attempt.encode(),ctypes.byref(count),error):raise ValueError(error.value.decode())
        return count.value

    def owner_rows(self,path):
        output=(ctypes.c_void_p*5)();error=ctypes.create_string_buffer(1024)
        if self.lib.domeye_owner_rows(os.fsencode(path),ctypes.addressof(output),error):raise ValueError(error.value.decode())
        return pa.RecordBatchReader._import_from_c(ctypes.addressof(output))

    def parse(self,path,source_id,expected_sha,output,*,batch_rows=100000,guard=lambda:None,consume=None,metrics=None,source_kind='rib',policy='strict/v1'):
        policy=ReadPolicy(policy).value
        if source_kind not in ('rib','update'):raise ValueError('原生来源种类无效')
        root=Path(output);root.mkdir(parents=True,exist_ok=True);path=Path(path).resolve()
        binding=dict(version=VERSION,parser_authority=PARSER_AUTHORITY,source_id=source_id,sha256=expected_sha,build=self.identity,batch_rows=batch_rows)
        if source_kind!='rib' or policy!='strict/v1':binding.update(source_kind=source_kind,policy=policy)
        if (root/'binding.json').exists():
            if json.loads((root/'binding.json').read_text())!=binding:raise ValueError('原生恢复绑定漂移')
        else:durable(root/'binding.json',binding)
        if (root/'source-audit.json').exists():
            audit=json.loads((root/'source-audit.json').read_text())
            if audit['source_stamp']!=_stamp(path) or file_sha(path)!=expected_sha:raise ValueError('原件漂移')
        else:
            with metrics.measure('source_scan') if metrics else nullcontext():audit=inspect_source(path,source_id,expected_sha,root,source_kind=source_kind)
        consumed=0
        def drain():
            nonlocal consumed
            self.seal_segments(root)
            segments=sorted(root.glob('segment-*/committed.json'))
            for p in segments[consumed:]:
                obj=json.loads(p.read_text())
                # C 可能在本轮 seal_segments 扫描之后刚提交：留待下轮补齐文件摘要再登记。
                if 'files' not in obj:break
                if consume:consume(p.parent,obj)
                consumed+=1
                temp=root/'consumed-count.partial';temp.write_text(str(consumed));temp.replace(root/'consumed-count')
        if (root/'complete.json').exists():
            result=self.verify(root);drain();return result
        templates=[]
        for level in ('peer_table','route_elements'):
            obj=asdict(Interpretation(ParseStatus.DECODED,ReadPolicy(policy),None,HeaderEvidence(),interpretation_level=level,source_path=str(path),next_record_offset='__offset__'))
            templates.append(json.dumps(obj,ensure_ascii=False).replace('"__offset__"','@offset@'))
        (root/'interpretation.txt').write_text('\n'.join(templates)+'\n')
        segments=sorted(root.glob('segment-*/committed.json'));next_record=0
        for index,p in enumerate(segments):
            segment=json.loads(p.read_text())
            if segment['segment']!=index or segment['first_record']!=next_record:raise ValueError('原生分段链损坏')
            # 已由本进程核验的段才有摘要，异常退出后的尾段重新生产。
            if 'files' not in segment:segments=segments[:index];break
            self.verify_segment(p.parent,segment);next_record=segment['next_record']
        for p in root.glob('segment-*'):
            if int(p.name.split('-')[1])>=len(segments):
                import shutil
                shutil.rmtree(p)
        drain()
        before=resource.getrusage(resource.RUSAGE_CHILDREN);start=time.monotonic()
        with (root/'native.stdout.jsonl').open('a') as log,(root/'native.stderr.log').open('a') as err:
            env={**os.environ,'DOMEYE_NATIVE_POLICY':policy,'DOMEYE_NATIVE_PARENT_PID':str(os.getpid())}
            if consume:env['DOMEYE_NATIVE_STREAMING']='1'
            child=subprocess.Popen([str(self.build/'domeye-rib'),str(path),source_id,expected_sha,str(root),str(batch_rows),str(len(segments)),str(next_record)],stdout=log,stderr=err,env=env)
            try:
                while child.poll() is None:
                    guard()
                    drain()
                    time.sleep(.2)
                if child.returncode:raise RuntimeError('原生进程拒绝输入或失败；见 native.stderr.log')
            finally:
                if child.poll() is None:child.terminate();child.wait(timeout=10)
        after=resource.getrusage(resource.RUSAGE_CHILDREN)
        errors=(root/'native.stderr.log').read_text(errors='replace').lower()
        if 'out of memory' in errors or 'out of memmory' in errors:raise RuntimeError('libbgpdump 内存分配失败，禁止按坏记录隔离')
        drain()
        eof=json.loads((root/'native-eof.json').read_text())
        if any(eof[k]!=audit[k] for k in ('records','decoded_bytes')) or (audit['elements'] is not None and eof['elements']!=audit['elements']):raise ValueError('原生 EOF 与独立扫描不符')
        if _stamp(path)!=audit['source_stamp']:raise ValueError('解析期间源变动')
        eof.update(child_wall_seconds=time.monotonic()-start,child_user_cpu_seconds=after.ru_utime-before.ru_utime,
                   child_system_cpu_seconds=after.ru_stime-before.ru_stime,child_peak_rss_bytes=after.ru_maxrss*1024)
        durable(root/'complete.json',eof)
        with metrics.measure('native_output_verify') if metrics else nullcontext():return self.verify(root)

    @staticmethod
    def verify_segment(root,segment):
        for name,info in segment['files'].items():
            p=root/name
            if p.stat().st_size!=info['bytes'] or file_sha(p)!=info['sha256']:raise ValueError('分段文件漂移')

    @staticmethod
    def seal_segments(root):
        for p in sorted(root.glob('segment-*/committed.json')):
            obj=json.loads(p.read_text())
            if 'files' in obj:continue
            obj['files']={f.name:dict(bytes=f.stat().st_size,sha256=file_sha(f)) for f in sorted(p.parent.glob('*.arrow'))}
            # 原子替换只增加实际文件校验结果；原生进程不再改写已提交段。
            temp=p.with_suffix('.verified');durable(temp,obj);temp.replace(p)

    def verify(self,root):
        result=json.loads((root/'complete.json').read_text());record=count=0
        for i,p in enumerate(sorted(root.glob('segment-*/committed.json'))):
            obj=json.loads(p.read_text());self.verify_segment(p.parent,obj)
            release_segment_cache(p.parent)
            if obj['segment']!=i or obj['first_record']!=record:raise ValueError('分段链不连续')
            record=obj['next_record'];count+=obj['counts']['elements']
        if record!=result['records'] or count!=result['elements']:raise ValueError('分段总计不符')
        return result

    @staticmethod
    def rows(root):
        result={name:[] for name in OBS_TABLES}
        for p in sorted(Path(root).glob('segment-*/*.arrow')):
            with pa.memory_map(str(p),'r') as f:result[p.stem].extend(ipc.open_file(f).read_all().to_pylist())
        return result
