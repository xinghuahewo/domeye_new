"""固定RibToken的有界离线阅读；完整scope及owner成功后才有终端回执。"""
from dataclasses import asdict
from copy import deepcopy
import hashlib
import json
from data_pipeline.history.event_index.query import CoreSession
from data_pipeline.history.event_index.cleanup import owner, release
from data_pipeline.history.event_index.exact import loads, wire
from data_pipeline.history.event_index.project import read_span
from data_pipeline.history.rib_index.model import TABLES, DEFINITIONS, ORDER, RULE, row_bytes
from data_pipeline.history.rib_index.store import rows
from data_pipeline.history.rib_index.mrt import require


class RibSession(CoreSession):
    def __enter__(self):
        if self.ready is not None or self.failed:raise ValueError('H2会话不可重复进入')
        try:
            self.ready=self.h._qualify_rib(self.token,self.budget)
            self.db=self.h._lake(self.token.profile_id,True);return self
        except BaseException as error:self._fail(error);raise
    def __exit__(self,kind,error,trace):
        primary=error;pg=None;receipt=None
        try:
            if kind is None and not self.failed and not self.active and not self.pending:
                pg=self.h._connect();self.h._qualify_rib(self.token,self.budget,pg=pg,lock=True);self._check()
                receipt={'qualification':'complete','scope':'H2_artificial_single_and_endpoint','profile':RULE,'token':asdict(self.token),
                         'completed_operations':sorted(self.completed),'query_scopes':self.progress,'operation_scopes':self.operation_scopes,
                         'resources':self.budget.report(),'session_continuity':'unknown','interval_change_count':None,'H3':'unresolved_external'}
                require(len(row_bytes(receipt))<=self.h.collection_limits.metadata_bytes,'终端元数据超限')
        except BaseException as caught:primary=caught
        finally:
            db,self.db=self.db,None;primary=release(db,'H2 DuckDB',primary,self.cleanup_errors)
            if primary is None and receipt is not None:
                try:self.budget.check()
                except BaseException as caught:primary=caught
            primary=release(pg,'H2 qualification PostgreSQL',primary,self.cleanup_errors)
        if primary is not None or self.cleanup_errors:
            self.failed=True;self.receipt=None
            if error is None and primary is not None:raise primary.with_traceback(primary.__traceback__)
        elif receipt is not None:self.receipt=receipt
        return False
    def table(self,name):return f'lake.history.t{TABLES.index(name)} AT (VERSION => {int(self.token.snapshot)})'
    def _fetch(self,statement,params=(),maximum=60):
        self._check();result=self.db.execute(statement,params);names=[d[0] for d in result.description];output=[];size=0
        while True:
            self._check();value=result.fetchone()
            if value is None:break
            row=dict(zip(names,value));data=row_bytes(row);size+=len(data)
            require(len(output)<maximum and len(data)<=self.h.limits.max_row_bytes and size<=self.h.limits.batch_bytes,'有界读取超限')
            self.budget.add('typed_rows',1,self.h.limits.max_total_rows);self.budget.add('typed_bytes',len(data),self.h.limits.max_total_bytes);output.append(row)
        return output
    def bulk(self,name,*,batch_rows=None):
        self._check();cap=self.h.limits.batch_rows if batch_rows is None else batch_rows
        if self.active or name not in TABLES or type(cap) is not int or not 1<=cap<=self.h.limits.batch_rows:raise ValueError('H2批参数无效')
        self.active=True;pending=[];size=count=0;digest=hashlib.sha256()
        try:
            with owner(rows(self.db,self.token,name,self.budget,cap),'H2 typed stream',self.cleanup_errors) as stream:
                for row in stream:
                    data=row_bytes(row)
                    if pending and (len(pending)==cap or size+len(data)>self.h.limits.batch_bytes):
                        yield {'qualification':'provisional','dataset':name,'rows':pending};self._check();pending=[];size=0
                    pending.append(row);size+=len(data);count+=1;digest.update(data+b'\n')
                if pending:yield {'qualification':'provisional','dataset':name,'rows':pending};self._check()
            info=next(t for t in self.ready['tables'] if t['name']==name)
            require((count,digest.hexdigest())==(info['rows'],info['sha256']),'全表有序摘要不符');self._complete('bulk:'+name)
        except BaseException as error:self._fail(error);raise
        finally:self.active=False
    def query(self,name,*,file_id=None,family=None,status=None,limit=60,cursor=None):
        self._check()
        if self.active or name not in TABLES or type(limit) is not int or not 1<=limit<=min(60,self.h.limits.batch_rows):raise ValueError('H2查询参数无效')
        fields={k for k,_ in DEFINITIONS[name]};filters={'file_id':file_id,'family':family,'status':status}
        if (file_id is not None and (type(file_id) is not int or not 0<=file_id<2**63)) or (family is not None and family not in ('ipv4','ipv6','all')) or (status is not None and status not in ('same','different','left_only','right_only','not_comparable')):raise ValueError('H2筛选参数无效')
        if any(value is not None and k not in fields and not (k=='family' and 'afi' in fields) for k,value in filters.items()):raise ValueError('H2表不支持该筛选')
        binding=json.loads(row_bytes({'token':asdict(self.token),'table':name,**filters,'limit':limit}));key=hashlib.sha256(row_bytes(binding)).hexdigest();after=None
        order=ORDER[name].split(',');types=dict(DEFINITIONS[name])
        if cursor is not None:
            if not isinstance(cursor,dict) or set(cursor)!={'binding','after'} or cursor['binding']!=binding or not isinstance(cursor['after'],list) or len(cursor['after'])!=len(order) or any(type(v) is not {'i':int,'s':str}[types[k]] for k,v in zip(order,cursor['after'])):raise ValueError('H2游标scope不符')
            after=cursor['after']
        previous=self.progress.get(key)
        if previous and not previous['finished'] and after!=previous['next_after']:raise ValueError('H2必须继续未读游标')
        with self._reading():
            actual_filters=dict(filters)
            if 'family' not in fields and 'afi' in fields:
                actual_filters.pop('family')
                if family in ('ipv4','ipv6'):actual_filters['afi']=1 if family=='ipv4' else 2
            clauses=['"'+k+'"=?' for k,v in actual_filters.items() if v is not None];values=[v for v in actual_filters.values() if v is not None]
            if after is not None:
                clauses.append('('+','.join(order)+') > ('+','.join('?' for _ in order)+')');values.extend(after)
            result=self._fetch('SELECT * FROM '+self.table(name)+(' WHERE '+' AND '.join(clauses) if clauses else '')+' ORDER BY '+ORDER[name]+' LIMIT ?',[*values,limit],maximum=limit)
            next_cursor={'binding':binding,'after':[result[-1][k] for k in order]} if len(result)==limit else None
            scope=deepcopy(previous) if previous and not previous['finished'] else {'scope':'full_query' if after is None else 'query_suffix','start_after':after,'binding':binding}
            scope.update(finished=next_cursor is None,next_after=next_cursor['after'] if next_cursor else None)
            if key not in self.progress and len(self.progress)>=self.h.collection_limits.edges:raise ValueError('H2scope数量超限')
            self.progress[key]=scope
            require(len(row_bytes(self.progress))<=self.h.collection_limits.metadata_bytes,'H2scope字节超限')
            if next_cursor:self.pending.add(key)
            else:self.pending.discard(key);self._complete('query:'+key)
            response={'qualification':'provisional','binding':binding,'rows':result,'next_cursor':next_cursor,'query_scope':deepcopy(scope)}
            require(len(row_bytes(response))<=self.h.limits.batch_bytes,'H2完整返回页超限');return response
    def detail(self,document_id):
        self._check()
        if self.active or type(document_id) is not int or document_id<0:raise ValueError('H2文档参数无效')
        with self._reading():
            found=self._fetch('SELECT * FROM '+self.table('documents')+' WHERE document_id=?',[document_id],maximum=1)
            response={'qualification':'provisional','document_id':document_id,'resolution':'missing','raw':None}
            if found:
                doc=found[0];source=self.h.data_root/self.token.collection.collection_id/'source'
                raw=read_span(source,doc['entity_path'],doc['byte_start'],doc['byte_end'],self.budget)
                response.update(resolution='matched',location=doc,raw=raw,exact=wire(loads(raw)))
            require(len(row_bytes(response))<=self.h.limits.batch_bytes,'H2完整详情超限')
            self._complete('document:'+str(document_id),{'document_id':document_id,'resolution':response['resolution']});return response
    def reference(self,document_id,object_ordinal,side,ref_ordinal):
        self._check();values=[document_id,object_ordinal,side,ref_ordinal]
        if self.active or any(type(v) is not int or v<0 for v in values) or side not in (0,1):raise ValueError('H2引用参数无效')
        with self._reading():
            refs=self._fetch('SELECT * FROM '+self.table('comparison_refs')+' WHERE document_id=? AND object_ordinal=? AND side=? AND ref_ordinal=?',values,maximum=1)
            response={'qualification':'provisional','resolution':'missing','reference':None}
            if refs:
                ref=refs[0];entry=self._fetch('SELECT * FROM '+self.table('mrt_observations')+' WHERE file_id=? AND record=? AND entry_index=?',[ref['mrt_file'],ref['record'],ref['entry_index']],maximum=1)
                require(len(entry)==1,'H2固定原条目缺失')
                frame=self._fetch('SELECT * FROM '+self.table('mrt_frames')+' WHERE file_id=? AND record=?',[ref['mrt_file'],ref['record']],maximum=1)
                require(len(frame)==1,'H2固定原帧缺失');frame=frame[0]
                source=self.h.data_root/self.token.collection.collection_id/'source'
                raw=read_span(source,frame['entity_path'],frame['decoded_offset'],frame['decoded_offset']+12+frame['body_bytes'],self.budget)
                response.update(frame=frame,raw_header_and_body=raw)
                response.update(resolution='verified_original_mrt',reference=ref,observation=entry[0])
            require(len(row_bytes(response))<=self.h.limits.batch_bytes,'H2完整引用返回超限')
            self._complete('reference:'+':'.join(map(str,values)),{'locator':values,'resolution':response['resolution']});return response
