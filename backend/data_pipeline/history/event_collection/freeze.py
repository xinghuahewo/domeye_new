"""仅显式人工绑定的集合闭包；文件/JSON结构与业务准入分别封存。"""
from dataclasses import asdict
from contextlib import closing
import hashlib
import io
import os
from pathlib import Path
import re
import sqlite3
import stat
import uuid
import zlib

from data_pipeline.history.database_import import SQLiteSource, freeze_sqlite_source
from data_pipeline.history.database_import.binding import source_label
from data_pipeline.history.database_import.freeze import Limits, write_json, read_json, verify_package, block_rows
from data_pipeline.history.event_collection.model import Binding, Budget, CollectionLimits, FREEZE_RULE, PROFILE_RULE, PROFILES, profile_hash
from data_pipeline.history.event_collection.json_tokens import Parser
from data_pipeline.history.event_collection.spool import Spool
from data_pipeline.history.event_collection.children import remaining, sqlite_preflight, verification_plan

SCHEMAS = {
    'core-index':('core-overview-index/v1','core-overview-index/v2'),
    'diagnostic':('core-overview-diagnostic/v1','core-overview-diagnostic/v2'),
    'provenance':('core-overview-input/v1',),
    'general-root':('country-outage-general-read-model-store/v1',),
    'overview':('country-outage-general-overview-artifact/v1',),
    'series':('country-outage-general-series-artifact/v1',),
    'affected_as':('country-outage-general-affected-as/v1',),
    'path_downstreams':('country-outage-general-path-downstream/v1',),
}
GENERAL_SOURCE_FIELDS=tuple('source_'+kind+'_'+field for kind,fields in [('event_cohort',('dataset_id','content_sha256','manifest_sha256')),('event_metric',('dataset_id','content_sha256','manifest_sha256')),('event_as_path',('dataset_id','content_sha256','manifest_sha256')),('lifecycle',('snapshot_id','snapshot_content_sha256'))] for field in fields)


def valid_sha(value):
    if not isinstance(value,str) or not re.fullmatch('[0-9a-f]{64}',value): raise ValueError('缺明确SHA256')
    return value


def identity(st): return [st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns]


def safe_path(path,roots):
    path=Path(os.path.abspath(path))
    if not any(path.is_relative_to(root) for root in roots): raise ValueError('源路径越界')
    for part in [path,*path.parents]:
        if part.is_symlink(): raise ValueError('拒绝符号链接')
    if not stat.S_ISREG(path.stat().st_mode): raise ValueError('只接受普通文件')
    return path


def decode_gzip(source,destination,budget):
    """zlib验证CRC/ISIZE；限制输出分配，继续验证物理EOF及单成员。"""
    d=zlib.decompressobj(16+zlib.MAX_WBITS); decoded=0; compressed=0; header=0
    with source.open('rb') as src,destination.open('xb') as dst:
        try:
            while True:
                budget.check(); block=src.read(budget.collection_limits.chunk_bytes); budget.add('read_blocks',1)
                if not block: break
                compressed+=len(block)
                if compressed>budget.collection_limits.file_bytes: raise ValueError('gzip压缩字节超限')
                if d.eof: raise ValueError('gzip第二成员/尾随字节')
                pending=block
                while pending:
                    budget.check(); output=d.decompress(pending,budget.collection_limits.chunk_bytes)
                    pending=d.unconsumed_tail
                    if not decoded and not output:
                        header+=len(block)
                        if header>budget.collection_limits.token_bytes: raise ValueError('gzip头/无输出前缀超限')
                    if decoded+len(output)>budget.collection_limits.file_bytes: raise ValueError('gzip单文件解码超限')
                    budget.add('decoded_bytes',len(output),budget.collection_limits.decoded_bytes)
                    budget.add('temporary_bytes',len(output),budget.collection_limits.temporary_bytes)
                    decoded+=len(output); dst.write(output)
                    if d.unused_data: raise ValueError('gzip第二成员/尾随字节')
            if not d.eof: raise ValueError('gzip未到完整EOF')
            dst.flush(); os.fsync(dst.fileno())
        except zlib.error: raise ValueError('gzip CRC/ISIZE/压缩流损坏') from None
    return decoded


class Freezer:
    def __init__(self,binding,destination,limits,collection_limits):
        if not isinstance(binding,Binding) or binding.closed_immutable is not True or binding.data_kind!='fixture':
            raise ValueError('C.1仅接受明确关闭的人工fixture集合')
        if not binding.roots or len(binding.roots)>collection_limits.files: raise ValueError('根数量无效')
        root_paths=[os.path.abspath(r.path) for r in binding.roots]
        if len(root_paths)!=len(set(root_paths)): raise ValueError('重复根物理路径须先明确独立scope，首版拒绝折叠')
        self.binding=binding; self.out=Path(destination).absolute()
        self.roots=tuple(Path(p).absolute() for p in binding.allowed_roots)
        if not self.roots or any(not p.is_dir() or p.is_symlink() for p in self.roots): raise ValueError('缺受准源目录')
        if any(self.out.is_relative_to(p) or p.is_relative_to(self.out) for p in self.roots): raise ValueError('源与输出目录必须独立')
        self.out.mkdir(parents=True,exist_ok=False); self._created=True
        self.budget=Budget(self.out,limits,collection_limits); self.spool=Spool(self.out/'structure.sqlite',self.budget,True)
        self.id=uuid.uuid4().hex; self.sqlite_sources=[]; self.files=[]; self.by_path={}; self.active=set(); self.children=[]; self.sources=[]; self.artifacts={}
        self.ext={}; self.doc_count=0; self.edge_count=0; self.identity_count=0; self.scope_count=0
        for uri,path,sha in binding.external_files:
            if uri in self.ext: raise ValueError('外部URI重复绑定')
            source_label(uri,'bound'); self.ext[uri]=(path,valid_sha(sha))
        self.inventory={}
        self.artifact_metadata_bytes=0
        # 调用者明确提供的根目录为封闭目录；不递归未声明子目录。
        for root in self.roots:
            self.inventory[str(root)]=self.directory_names(root)

    def directory_names(self,root):
        names=[]; pending=[root]
        while pending:
            directory=pending.pop()
            for p in directory.iterdir():
                self.budget.check()
                if p.is_symlink(): raise ValueError('封闭目录含链接')
                names.append(str(p.relative_to(root)))
                if len(names)>self.budget.collection_limits.files: raise ValueError('封闭目录条目超限')
                if p.is_dir(): pending.append(p)
                elif not p.is_file(): raise ValueError('封闭目录非普通文件')
        return tuple(sorted(names))

    def artifact(self,path):
        rel=str(path.relative_to(self.out))
        item={'path':rel,'bytes':path.stat().st_size,'sha256':self.budget.hash(path)}
        from data_pipeline.history.database_import.codec import canonical
        if rel not in self.artifacts: self.artifact_metadata_bytes+=len(canonical(item))
        if self.artifact_metadata_bytes>self.budget.collection_limits.metadata_bytes//2: raise ValueError('文件索引元数据超限')
        self.artifacts[rel]=item

    def copy(self,path,dest,sha):
        path=safe_path(path,self.roots); before=identity(path.stat()); valid_sha(sha)
        if before[2]>self.budget.collection_limits.file_bytes: raise ValueError('原文件字节超限')
        self.budget.add('raw_bytes',before[2],self.budget.limits.max_total_bytes)
        self.budget.add('temporary_bytes',before[2],self.budget.collection_limits.temporary_bytes)
        if self.budget.hash(path)!=sha: raise ValueError('源绑定SHA不符')
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        h=hashlib.sha256(); count=0
        with os.fdopen(fd,'rb') as src,dest.open('xb') as dst:
            if identity(os.fstat(src.fileno()))!=before: raise ValueError('源实体替换')
            while True:
                self.budget.check(); block=src.read(self.budget.collection_limits.chunk_bytes); self.budget.add('read_blocks',1)
                if not block: break
                count+=len(block)
                if count>before[2]: raise ValueError('复制源增长')
                h.update(block); dst.write(block)
            if identity(os.fstat(src.fileno()))!=before: raise ValueError('复制期间实体变化')
            dst.flush(); os.fsync(dst.fileno())
        if h.hexdigest()!=sha or count!=before[2] or identity(path.stat())!=before or self.budget.hash(path)!=sha:
            raise ValueError('源复制期间变化')
        self.sources.append({'path':str(path),'identity':before,'sha256':sha})

    def document(self,entity,file_id,ordinal=0,line=1,newline='',start=0,table=None,row=None,column=None,storage=None,data=None):
        doc=self.doc_count; self.doc_count+=1
        def emit(node): self.spool.add('nodes',{'document_id':doc,**node})
        with (io.BytesIO(data) if data is not None else entity.open('rb')) as src:
            parser=Parser(src,self.budget,emit,start=start); root,count,end=parser.parse()
        self.spool.add('documents',dict(document_id=doc,file_id=file_id,document_ordinal=ordinal,line=line,newline=newline,
                       byte_start=start,byte_end=end,node_count=count,table_name=table,row_ordinal=row,column_name=column,
                       storage_class=storage,entity_path=str(entity.relative_to(self.out))))
        self.identities(doc,file_id,column)
        self.spool.flush()
        return self.spool.node(doc)

    def identities(self,doc,file_id,column=None):
        # 只选择有限profile上下文；extra/raw_fields等普通扩展不按名字猜身份。
        root=self.spool.node(doc); role=self.files[file_id]['role']; contexts=[]
        if root.row['kind']!='object': return
        def selected(node,keys):
            if node is not None and node.row['kind']=='object': contexts.append((node,keys))
        if role=='core-index':
            selected(root,('interpretation_version',))
            days=root.field('days',False)
            if days:
                for day in days.children('object'): selected(day,('input_version','interpretation_version'))
        elif role in ('general-root','original-complete'):
            selected(root,('dataset_id','run_id','implementation_id','content_sha256',*GENERAL_SOURCE_FIELDS))
            events=root.field('events',False)
            if events:
                for event in events.children('array'): selected(event,('publication_id','revision','incident_id','legacy_reference','event_read_model_id','cohort_id','event_metric_id','event_as_path_id','content_sha256'))
        elif role in ('overview','series','affected_as','path_downstreams'):
            selected(root,('publication_id','event_read_model_id','content_sha256'))
        elif column=='manifest': selected(root,('interpretation_version','input_version'))
        elif column=='payload' or role=='records-jsonl':
            selected(root,('content_version',))
            record=root.field('record',False)
            if record and record.row['kind']=='object': selected(record.field('identity',False),('legacy_reference',))
        entries=[]
        for parent,keys in contexts:
            for key in sorted(keys):
                node=parent.field(key,False)
                if node is None or node.row['kind'] not in ('string','number','null'): continue
                entries.append(node)
        for node in sorted(entries,key=lambda n:n.ordinal):
            row=node.row
            value=row['text_value'] if row['kind']=='string' else row['number_lexeme']
            self.spool.add('identities',dict(identity_ordinal=self.identity_count,file_id=file_id,document_id=doc,
                           node_ordinal=node.ordinal,name=row['key'],value=value)); self.identity_count+=1

    def jsonl(self,entity,file_id,role):
        offset=0; ordinal=0
        with entity.open('rb') as f:
            while True:
                self.budget.check(); data=f.readline(self.budget.collection_limits.line_bytes+1); self.budget.add('read_blocks',1)
                if not data: break
                if len(data)>self.budget.collection_limits.line_bytes: raise ValueError('JSONL行超限')
                newline='CRLF' if data.endswith(b'\r\n') else 'LF' if data.endswith(b'\n') else ''
                node=self.document(entity,file_id,ordinal,ordinal+1,newline,offset,data=data)
                if node.row['kind']!='object': raise ValueError('JSONL必须逐行对象')
                if role in SCHEMAS: self.schema(node,role)
                if role=='records-jsonl': self.evidence(node,file_id)
                ordinal+=1; offset+=len(data)
        return ordinal

    def schema(self,node,role):
        if node.field('schema_version').text() not in SCHEMAS[role]: raise ValueError('未知profile schema: '+role)

    def edge(self,parent,node,ref,role,kind,target,sha=None,resolution='resolved'):
        if self.edge_count>=self.budget.collection_limits.edges: raise ValueError('依赖边超限')
        self.spool.add('edges',dict(edge_ordinal=self.edge_count,parent_file=parent,document_id=node.doc if node else None,
                       node_ordinal=node.ordinal if node else None,reference=ref,role=role,relation_kind=kind,
                       resolution=resolution,target_file=target,expected_sha=sha)); self.edge_count+=1

    def dependency(self,parent,node,ref,role,sha=None):
        source=self.files[parent]; local=Path(self.sources[parent]['path']).parent/ref if '://' not in ref else None
        if local is None:
            if ref not in self.ext:
                self.edge(parent,node,ref,role,'required_file',None,sha,'not_authorized')
                raise ValueError('必需外部文件未绑定: '+ref)
            local,bound_sha=self.ext[ref]
            if sha and sha!=bound_sha: raise ValueError('外部绑定SHA冲突')
            sha=bound_sha
        if not sha:
            # 文件声明不带SHA时必须显式以原URI绑定，不接受扫描后自造预期。
            key=str(local)
            if key not in self.ext: raise ValueError('必需文件缺预绑定SHA')
            _,sha=self.ext[key]
        try:
            target=self.visit(local,ref if '://' in ref else Path(local).absolute().as_uri(),role,source['profile'],source['version'],sha,source['root_id'])
        except (OSError,ValueError):
            self.edge(parent,node,ref,role,'required_file',None,sha,'failed'); raise
        self.edge(parent,node,ref,role,'required_file',target,sha)
        return target

    def scope(self,root_id,scope,state,node):
        self.spool.add('availability',dict(scope_ordinal=self.scope_count,root_id=root_id,scope=scope,state=state,document_id=node.doc,node_ordinal=node.ordinal)); self.scope_count+=1

    def dispatch(self,node,file_id,role):
        if role in SCHEMAS: self.schema(node,role)
        if role=='core-index':
            for key in ('scale','origin','path_comparison','composition'):
                if node.field(key,False): raise ValueError('C.1尚未支持该必需角色: '+key)
            self.scope(self.files[file_id]['root_id'],'root','unknown',node)
            days=node.field('days'); day_names=set()
            for entry in days.children('object'):
                day=entry.row['key']; day_names.add(day)
                target=self.dependency(file_id,entry,entry.field('file').text(),'day-sqlite',entry.field('sha256').text())
                if self.files[target]['_record_count']!=entry.field('count').integer(): raise ValueError('core日原行数冲突')
                self.scope(self.files[file_id]['root_id'],day,'available',entry)
            diagnostics=node.field('diagnostics',False)
            if diagnostics:
                for entry in diagnostics.children('object'):
                    day=entry.row['key']
                    if day in day_names: raise ValueError('正常/隔离日期冲突')
                    self.dependency(file_id,entry,entry.field('file').text(),'diagnostic',entry.field('sha256').text())
                    self.scope(self.files[file_id]['root_id'],day,'validation_failed',entry)
        elif role=='provenance':
            records=node.field('records')
            target=self.dependency(file_id,records,records.field('file').text(),'records-jsonl',records.field('sha256').text())
            if self.files[target]['_document_count']!=records.field('count').integer() or self.files[file_id]['_record_count']!=records.field('count').integer(): raise ValueError('provenance记录数冲突')
            self.evidence(node,file_id)
        elif role=='general-root':
            # COMPLETE为原根字节副本；不把原声明当新集合资格。
            target=self.dependency(file_id,node,'COMPLETE.json','original-complete',self.files[file_id]['raw_sha'])
            if self.files[target]['raw_sha']!=self.files[file_id]['raw_sha']: raise ValueError('原COMPLETE不符')
            seen=set(); events=node.field('events')
            if node.field('status').text()!='complete': raise ValueError('原General未完成')
            totals={k:0 for k in ('state_point_count','affected_as_count','path_downstream_relation_count','path_sample_count')}
            event_count=0
            for event in events.children('array'):
                event_count+=1
                for k in totals:
                    n=event.field(k).integer()
                    if n<0: raise ValueError('原人口负值')
                    totals[k]+=n
                if event.field('schema_version').text()!='country-outage-general-event-read-model/v1': raise ValueError('未知General事件schema')
                for key in ('publication_id','event_read_model_id','legacy_reference','incident_id'):
                    identity_=(key,event.field(key).text())
                    if identity_ in seen: raise ValueError('General原身份冲突')
                    seen.add(identity_)
                self.scope(self.files[file_id]['root_id'],event.field('event_read_model_id').text(),'unknown',event)
                for key in ('overview','series','affected_as','path_downstreams'):
                    meta=event.field(key); child=self.dependency(file_id,meta,meta.field('path').text(),key,meta.field('sha256').text())
                    actual=self.files[child]
                    if actual['raw_bytes']!=meta.field('size_bytes').integer() or actual['decoded_sha']!=meta.field('content_sha256').text(): raise ValueError('General子件大小/解码SHA冲突')
                    docs=self.spool.db.execute('SELECT document_id FROM documents WHERE file_id=? ORDER BY document_id',(child,))
                    samples=0
                    for (docid,) in docs:
                        content=self.spool.node(docid)
                        for identity_key in ('event_read_model_id','publication_id'):
                            if content.field(identity_key).text()!=event.field(identity_key).text(): raise ValueError('General子件原身份冲突')
                        if key=='series':
                            points=content.field('point_count').integer()
                            timestamps=content.field('timestamps')
                            if timestamps.row['kind']!='array' or timestamps.row['child_count']!=points or points!=event.field('state_point_count').integer(): raise ValueError('series时点计数冲突')
                            for track in content.field('tracks').children('object'):
                                if track.row['kind']!='array' or track.row['child_count']!=points: raise ValueError('series轨道长度冲突')
                        if key=='path_downstreams':
                            sample=content.field('path_samples',False) or content.field('samples',False)
                            if sample is None or sample.row['kind']!='array': raise ValueError('路径样本数组缺失')
                            samples+=sample.row['child_count']
                    if key=='path_downstreams' and samples!=event.field('path_sample_count').integer(): raise ValueError('实际样本总数冲突')
                    if key in ('affected_as','path_downstreams'):
                        count=actual['_document_count']
                        expected=event.field('affected_as_count' if key=='affected_as' else 'path_downstream_relation_count').integer()
                        if count!=expected or count!=meta.field('row_count').integer(): raise ValueError('General实际行数冲突')
            if event_count!=node.field('event_count').integer() or any(v!=node.field(k).integer() for k,v in totals.items()): raise ValueError('General根人口冲突')
            # 上游只有身份没有物理路径；不伪造原源快照。
            for key in GENERAL_SOURCE_FIELDS:
                member=node.field(key,False)
                if member is not None:
                    self.edge(file_id,member,member.text(),key,'identity_only',None,resolution='unresolved_external')
        elif role=='diagnostic':
            self.evidence(node,file_id)

    def evidence(self,node,file_id):
        # 仅固定record/source/evidence_refs路径；其他普通字符串不是依赖。
        record=node.field('record',False)
        if record and record.row['kind']=='object': self.evidence(record,file_id)
        refs=node.field('evidence_refs',False)
        source=node.field('source',False)
        if refs is None and source and source.row['kind']=='object': refs=source.field('evidence_refs',False)
        if refs:
            for ref in refs.children('array'):
                path=ref.field('file',False) or ref.field('path',False)
                uri=ref.field('uri',False)
                sha=ref.field('sha256',False)
                if path or uri:
                    value=(path or uri).text()
                    self.dependency(file_id,ref,value,'evidence',sha.text() if sha else None)
                else: raise ValueError('证据引用无明确位置/身份')

    def sqlite(self,path,file_id):
        source=self.files[file_id]
        child_dir=self.out/('child-'+str(file_id))
        child_limits,disk=remaining(self.budget)
        sqlite_preflight(path,child_limits)
        manifest=freeze_sqlite_source(path,child_dir,binding=SQLiteSource(Path(self.sources[file_id]['path']).as_uri(),source['version'],source['raw_sha'],True),
                  publication={'status':'unknown','reason':'Q3-C.1人工结构片'},availability={'status':'unknown','reason':'未做领域准入'},limits=child_limits)
        self.children.append(str(manifest.relative_to(self.out))); source['child_index']=len(self.children)-1
        original=child_dir/'original.sqlite'
        path.unlink(); source['raw_path']=str(original.relative_to(self.out))
        child=read_json(manifest,self.budget.limits.max_metadata_bytes)
        self.budget.add('child_rows',sum(t['rows'] for t in child['tables']),self.budget.limits.max_total_rows)
        self.budget.add('child_data_bytes',sum(o['bytes'] for o in child['originals'])+sum(b['bytes'] for t in child['tables'] for b in t['blocks']),self.budget.limits.max_total_bytes)
        self.budget.add('child_block_bytes',sum(b['bytes'] for t in child['tables'] for b in t['blocks']),self.budget.limits.max_total_bytes)
        self.budget.add('temporary_bytes',sum(p.stat().st_size for p in child_dir.iterdir() if p.is_file()),self.budget.collection_limits.temporary_bytes)
        names={t['name']:t for t in child['tables']}
        if not {'records','provenance'}<=names.keys(): raise ValueError('core日SQLite缺records/provenance')
        if names['provenance']['rows']!=1: raise ValueError('core日provenance必须恰好一行')
        source['_record_count']=names['records']['rows']
        for ti,table in enumerate(child['tables']):
            if table['name'] not in ('records','provenance'): continue
            columns=('item','payload') if table['name']=='records' else ('manifest',)
            positions={col['name']:ci for ci,col in enumerate(table['columns'])}
            if not set(columns)<=positions.keys(): raise ValueError('core嵌入JSON列缺失')
            ri=0
            for block in table['blocks']:
                for values in block_rows(child_dir,block,self.budget.limits):
                    self.budget.check()
                    for col in columns:
                        cell=values[positions[col]]; storage=cell['storage_class']
                        if storage not in ('text','blob') or len(cell['value'])>2*self.budget.collection_limits.document_bytes: raise ValueError('嵌入JSON storage class/字节不符')
                        data=bytes.fromhex(cell['value'])
                        entity=self.out/f'embedded-{file_id}-{table["name"]}-{ri}-{col}.json'
                        self.budget.add('decoded_bytes',len(data),self.budget.collection_limits.decoded_bytes)
                        self.budget.add('temporary_bytes',len(data),self.budget.collection_limits.temporary_bytes)
                        with entity.open('xb') as output:
                            output.write(data); output.flush(); os.fsync(output.fileno())
                        self.artifact(entity)
                        node=self.document(entity,file_id,ri,table=table['name'],row=ri,column=col,storage=storage)
                        if col=='manifest': self.dispatch(node,file_id,'provenance')
                        else: self.evidence(node,file_id)
                    ri+=1
        # 所有child原表继续由Q3保留，不仅抽取的JSON列。
        for p in child_dir.iterdir():
            if p.is_file(): self.artifact(p)

    def visit(self,path,uri,role,profile,version,sha,root_id):
        path=safe_path(path,self.roots); key=str(path)
        if key in self.active: raise ValueError('集合依赖环')
        if key in self.by_path:
            index=self.by_path[key]; row=self.files[index]
            if (row['raw_sha'],row['role'],row['profile'],row['version'])!=(sha,role,profile,version): raise ValueError('共享文件角色/摘要冲突')
            return index
        if len(self.files)>=self.budget.collection_limits.files: raise ValueError('集合文件数超限')
        if role=='day-sqlite':
            if Path(str(path)+'-wal').exists(): raise ValueError('原SQLite存在WAL，不能通过复制主文件绕过关闭门禁')
            self.sqlite_sources.append(path)
        self.active.add(key); file_id=len(self.files); self.by_path[key]=file_id
        raw=self.out/f'raw-{file_id}'; self.copy(path,raw,sha)
        row=dict(file_id=file_id,root_id=root_id,uri=uri,role=role,profile=profile,version=version,raw_path=raw.name,raw_sha=sha,raw_bytes=raw.stat().st_size,members=0)
        self.files.append(row)
        try:
            if role=='day-sqlite': self.sqlite(raw,file_id)
            elif role=='evidence': pass
            else:
                entity=raw
                if role in ('overview','series','affected_as','path_downstreams'):
                    entity=self.out/f'decoded-{file_id}'; size=decode_gzip(raw,entity,self.budget)
                    row.update(decoded_path=entity.name,decoded_sha=self.budget.hash(entity),decoded_bytes=size,members=1); self.artifact(entity)
                else:
                    row.update(decoded_path=raw.name,decoded_sha=sha,decoded_bytes=row['raw_bytes'])
                    self.budget.add('decoded_bytes',row['raw_bytes'],self.budget.collection_limits.decoded_bytes)
                if role in ('affected_as','path_downstreams','records-jsonl'): row['_document_count']=self.jsonl(entity,file_id,role)
                else:
                    if role in ('core-index','general-root','diagnostic','provenance') and row['raw_bytes']>self.budget.collection_limits.metadata_bytes: raise ValueError('清单元数据超限')
                    node=self.document(entity,file_id)
                    if role!='original-complete': self.dispatch(node,file_id,role)
            self.artifact(self.out/row['raw_path']); self.spool.add('files',row)
            return file_id
        finally: self.active.remove(key)

    def finish(self):
        for index,root in enumerate(self.binding.roots):
            if root.profile not in PROFILES: raise ValueError('未知集合profile')
            source_label(root.origin_uri,root.source_version)
            self.visit(root.path,root.origin_uri,'core-index' if root.profile==PROFILES[0] else 'general-root',root.profile,root.source_version,root.sha256,index)
        for source in self.sqlite_sources:
            if Path(str(source)+'-wal').exists(): raise ValueError('原SQLite最终出现WAL')
        for source in self.sources:
            path=safe_path(source['path'],self.roots)
            if identity(path.stat())!=source['identity'] or self.budget.hash(path)!=source['sha256']: raise ValueError('最终源实体/内容变化')
        for directory,names in self.inventory.items():
            if self.directory_names(Path(directory))!=names: raise ValueError('封闭目录变化')
            for name in names:
                p=Path(directory)/name
                if p.is_file() and str(p) not in self.by_path: raise ValueError('封闭目录额外未声明文件')
                if p.is_symlink(): raise ValueError('C.1封闭目录无链接')
        self.spool.flush(); self.spool.close(); self.artifact(self.out/'structure.sqlite')
        for artifact in self.artifacts.values():
            p=self.out/artifact['path']
            if p.stat().st_size!=artifact['bytes'] or self.budget.hash(p)!=artifact['sha256']: raise ValueError('最终封存件变化')
        manifest=dict(schema_version=FREEZE_RULE,profile_rule=PROFILE_RULE,collection_id=self.id,profile_sha256=profile_hash(),
                      data_kind='fixture',integrity_state='complete',profile_admission='structure_audit_only',
                      roots=[asdict(r) for r in self.binding.roots],sources=self.sources,closed_directories=self.inventory,
                      children=self.children,artifacts=list(self.artifacts.values()),limits=asdict(self.budget.limits),
                      collection_limits=asdict(self.budget.collection_limits),resources=self.budget.report(),business_publication=None)
        from data_pipeline.history.database_import.codec import canonical
        if len(canonical(manifest))>self.budget.collection_limits.metadata_bytes: raise ValueError('集合清单预算超限')
        write_json(self.out/'manifest.json',manifest)
        write_json(self.out/'COMPLETE.json',{'collection_id':self.id,'manifest_sha256':self.budget.hash(self.out/'manifest.json')})
        return self.out/'manifest.json'


def freeze_collection(binding,destination,*,limits=Limits(),collection_limits=CollectionLimits()):
    freezer=object.__new__(Freezer); freezer._created=False
    try:
        freezer.__init__(binding,destination,limits,collection_limits)
        return freezer.finish()
    except BaseException as error:
        if freezer._created:
            if hasattr(freezer,'spool'):
                try: freezer.spool.flush()
                except Exception: pass
                freezer.spool.close()
            write_json(freezer.out/'FAILED.json',{'state':'failed','reason':str(error),
                       'resources':freezer.budget.report() if hasattr(freezer,'budget') else {},
                       'scope':'未签发集合完成；已有child仅代表child'})
        raise


def verify_collection(package,budget):
    package=Path(package).absolute(); budget.check()
    m=read_json(package/'manifest.json',budget.collection_limits.metadata_bytes)
    complete=read_json(package/'COMPLETE.json',budget.collection_limits.metadata_bytes)
    if m['schema_version']!=FREEZE_RULE or m['profile_rule']!=PROFILE_RULE or m['data_kind']!='fixture' or m['profile_sha256']!=profile_hash(): raise ValueError('集合规则/人工scope不符')
    if complete!={'collection_id':m['collection_id'],'manifest_sha256':budget.hash(package/'manifest.json')}: raise ValueError('集合完成标记不符')
    if len(m['children'])>budget.collection_limits.files: raise ValueError('child清单数超限')
    plan=verification_plan([safe_path(package/child,(package,)) for child in m['children']],budget)
    total=0
    for artifact in m['artifacts']:
        path=safe_path(package/artifact['path'],(package,)); total+=artifact['bytes']
        if total>budget.collection_limits.temporary_bytes or path.stat().st_size!=artifact['bytes'] or budget.hash(path)!=artifact['sha256']: raise ValueError('集合封存件损坏/超限')
    for path,limits in plan:
        budget.check(); verify_package(path.parent,limits)
    return m
