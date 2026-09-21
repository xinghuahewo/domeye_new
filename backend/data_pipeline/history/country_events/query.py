"""固定GeneralToken的有界离线阅读；完整scope及owner成功后才有终端回执。"""
from dataclasses import asdict
from copy import deepcopy
import hashlib
import json
from data_pipeline.history.event_index.query import CoreSession
from data_pipeline.history.event_index.cleanup import owner, release
from data_pipeline.history.event_index.exact import loads, wire
from data_pipeline.history.event_index.project import read_span
from data_pipeline.history.country_events.model import TABLES, DEFINITIONS, ORDER, RULE, row_bytes
from data_pipeline.history.country_events.store import rows
from data_pipeline.history.country_events.model import require


class GeneralSession:
    # 仅复用既有owner/预算/操作回执设施，不继承H1业务方法。
    __init__ = CoreSession.__init__
    _fail = CoreSession._fail
    _check = CoreSession._check
    _reading = CoreSession._reading
    _complete = CoreSession._complete

    def __enter__(self):
        if self.ready is not None or self.failed:raise ValueError('H5会话不可重复进入')
        try:
            self.ready=self.h._qualify_general(self.token,self.budget)
            self.db=self.h._lake(self.token.profile_id,True)
            # 锁定版本的LIMIT late materialization会二次扫描；此有限单表Interface禁用它。
            self.db.execute('SET late_materialization_max_rows=0');return self
        except BaseException as error:self._fail(error);raise
    def __exit__(self,kind,error,trace):
        primary=error;pg=None;receipt=None
        try:
            if kind is None and not self.failed and not self.active and not self.pending:
                pg=self.h._connect();self.h._qualify_general(self.token,self.budget,pg=pg,lock=True);self._check()
                receipt={'qualification':'complete','scope':'H5_artificial_general','profile':RULE,'token':asdict(self.token),
                         'completed_operations':sorted(self.completed),'query_scopes':self.progress,'operation_scopes':self.operation_scopes,
                         'resources':self.budget.report(),'upstream_artifacts':'unresolved_external'}
                require(len(row_bytes(receipt))<=self.h.collection_limits.metadata_bytes,'终端元数据超限')
        except BaseException as caught:primary=caught
        finally:
            db,self.db=self.db,None;primary=release(db,'H5 DuckDB',primary,self.cleanup_errors)
            if primary is None and receipt is not None:
                try:self.budget.check()
                except BaseException as caught:primary=caught
            primary=release(pg,'H5 qualification PostgreSQL',primary,self.cleanup_errors)
        if primary is not None or self.cleanup_errors:
            self.failed=True;self.receipt=None
            if error is None and primary is not None:raise primary.with_traceback(primary.__traceback__)
        elif receipt is not None:self.receipt=receipt
        return False
    def table(self,name):return f'lake.history.t{TABLES.index(name)} AT (VERSION => {int(self.token.snapshot)})'
    def _fetch(self,statement,params=(),maximum=60):
        self._check();self._scan_bound(statement);result=self.db.execute(statement,params);names=[d[0] for d in result.description];output=[];size=0
        while True:
            self._check();value=result.fetchone()
            if value is None:break
            row=dict(zip(names,value));data=row_bytes(row);size+=len(data)
            require(len(output)<maximum and len(data)<=self.h.limits.max_row_bytes and size<=self.h.limits.batch_bytes,'有界读取超限')
            self.budget.add('typed_rows',1,self.h.limits.max_total_rows);self.budget.add('typed_bytes',len(data),self.h.limits.max_total_bytes);output.append(row)
        return output
    def _scan_bound(self,statement):
        require(self.db.execute("SELECT current_setting('late_materialization_max_rows')").fetchone()==(0,),'H5扫描配置漂移')
        # SQL引擎可能剪枝，实际扫描可能小于全表；按涉及固定表的全表行/逻辑字节保守收费。
        # 这是显式上界，不冒充EXPLAIN的实际扫描量；分页次数无法绕过累计预算。
        for i,name in enumerate(TABLES):
            if f'lake.history.t{i} AT ' in statement:
                info=next(t for t in self.ready['tables'] if t['name']==name)
                self.budget.add('query_scan_rows_upper_bound',info['rows'],self.h.limits.max_total_rows)
                self.budget.add('query_scan_logical_bytes_upper_bound',info['logical_bytes'],self.h.limits.max_total_bytes)
                self.budget.add('query_statements',1)

    def bulk(self,name,*,batch_rows=None):
        self._check();cap=self.h.limits.batch_rows if batch_rows is None else batch_rows
        if self.active or name not in TABLES or type(cap) is not int or not 1<=cap<=self.h.limits.batch_rows:raise ValueError('H5批参数无效')
        self.active=True;pending=[];size=count=0;digest=hashlib.sha256()
        try:
            with owner(rows(self.db,self.token,name,self.budget,cap),'H5 typed stream',self.cleanup_errors) as stream:
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

    def query(self,name,*,event_id=None,track_ordinal=None,relation_ordinal=None,classification='all',search='',sort='default',affected_asn=None,scope='all',limit=60,cursor=None):
        """固定typed表与有限General筛选；业务表须先核事件资格。"""
        self._check()
        if self.active or name not in TABLES or type(limit) is not int or not 1<=limit<=min(60,self.h.limits.batch_rows):raise ValueError('H5页参数无效')
        if classification not in ('all','affected','route_interrupted') or sort not in ('default','asn_asc') or scope not in ('all','concurrent') or not isinstance(search,str) or len(search.encode())>4096:raise ValueError('H5筛选无效')
        if any(v is not None and (type(v) is not int or not -2**63<v<2**63) for v in (event_id,track_ordinal,relation_ordinal,affected_asn)):raise ValueError('H5定位参数无效')
        fields=dict(DEFINITIONS[name]);filters={'event_id':event_id,'track_ordinal':track_ordinal,'relation_ordinal':relation_ordinal,'affected_asn':affected_asn}
        if any(v is not None and k not in fields for k,v in filters.items()):raise ValueError('H5表不支持该定位')
        if (classification!='all' or sort!='default') and name!='affected_as':raise ValueError('H5仅AS支持分类/ASN排序')
        if (scope!='all' or affected_asn is not None) and name!='path_relations':raise ValueError('H5仅关系支持该筛选')
        if search and name not in ('affected_as','path_relations'):raise ValueError('H5该表不支持搜索')
        business=name in ('overview_metrics','track_definitions','series_points','affected_as','path_relations','path_samples','sample_peer_members')
        if business and event_id is None:raise ValueError('H5业务查询须固定event occurrence')
        binding=json.loads(row_bytes({'token':asdict(self.token),'table':name,**filters,'classification':classification,'search':search,'sort':sort,'scope':scope,'limit':limit}))
        key=hashlib.sha256(row_bytes(binding)).hexdigest();order=ORDER[name].split(',')
        if sort=='asn_asc':order=['asn','row_ordinal']
        after=None
        if cursor is not None:
            if not isinstance(cursor,dict) or set(cursor)!={'binding','after'} or cursor['binding']!=binding or not isinstance(cursor['after'],list) or len(cursor['after'])!=len(order) or any(type(v) is not {'i':int,'s':str}[fields[k]] for k,v in zip(order,cursor['after'])):raise ValueError('H5游标身份不符')
            after=cursor['after']
        previous=self.progress.get(key)
        if previous and not previous['finished'] and after!=previous['next_after']:raise ValueError('H5必须继续未读游标')
        with self._reading():
            if business:self._gate(event_id)
            clauses=['"'+k+'"=?' for k,v in filters.items() if v is not None];values=[v for v in filters.values() if v is not None]
            if classification!='all':clauses.append('event_classification=?');values.append(classification)
            if scope=='concurrent':clauses.append('concurrent_state_point_count<>0')
            if search:clauses.append('contains(search,?)');values.append(search.strip().lower().removeprefix('as'))
            if after is not None:clauses.append('('+','.join(order)+') > ('+','.join('?' for _ in order)+')');values.extend(after)
            result=self._fetch('SELECT * FROM '+self.table(name)+(' WHERE '+' AND '.join(clauses) if clauses else '')+' ORDER BY '+','.join(order)+' LIMIT ?',[*values,limit],maximum=limit)
            next_cursor={'binding':binding,'after':[result[-1][k] for k in order]} if len(result)==limit else None
            progress=deepcopy(previous) if previous and not previous['finished'] else {'scope':'full_query' if after is None else 'query_suffix','start_after':after,'binding':binding}
            progress.update(finished=next_cursor is None,next_after=next_cursor['after'] if next_cursor else None)
            require(key in self.progress or len(self.progress)<self.h.collection_limits.edges,'H5scope数量超限')
            self.progress[key]=progress;require(len(row_bytes(self.progress))<=self.h.collection_limits.metadata_bytes,'H5scope字节超限')
            if next_cursor:self.pending.add(key)
            else:self.pending.discard(key);self._complete('query:'+key)
            result={'qualification':'provisional','binding':binding,'rows':result,'next_cursor':next_cursor,'query_scope':deepcopy(progress)}
            require(len(row_bytes(result))<=self.h.limits.batch_bytes,'H5整页返回超限');return result

    def _gate(self,event_id):
        rows=self._fetch('SELECT * FROM '+self.table('general_events')+' WHERE event_id=?',[event_id],maximum=1)
        if len(rows)!=1:raise ValueError('H5事件missing')
        if rows[0]['admission']!='available':raise ValueError('H5事件身份隔离: '+rows[0]['admission'])
        return rows[0]

    def resolve(self,*,incident_id=None,reference=None,publication_id=None,revision=None,limit=60,cursor=None):
        """按原身份返回所有候选，可分页；不把country/cohort当唯一键。"""
        from services.country_outage_general_read_model import _canonical_reference
        self._check()
        if self.active or type(limit) is not int or not 1<=limit<=min(60,self.h.limits.batch_rows) or (incident_id is None and reference is None):raise ValueError('H5解析参数无效')
        if any(v is not None and (not isinstance(v,str) or len(v.encode())>4096) for v in (incident_id,reference,publication_id)) or (revision is not None and (type(revision) is not int or revision<1)):raise ValueError('H5身份筛选无效')
        canonical=_canonical_reference(reference) if reference is not None else None
        filters={'incident_id':incident_id,'canonical_reference':canonical,'publication_id':publication_id,'revision':revision}
        binding={'token':json.loads(row_bytes(asdict(self.token))),'resolve':filters,'reference_original':reference,'limit':limit};key=hashlib.sha256(row_bytes(binding)).hexdigest();after=-1
        if cursor is not None:
            if not isinstance(cursor,dict) or set(cursor)!={'binding','after'} or cursor['binding']!=binding or type(cursor['after']) is not int:raise ValueError('H5解析游标不符')
            after=cursor['after']
        previous=self.progress.get(key)
        if previous and not previous['finished'] and after!=previous['next_after']:raise ValueError('H5须继续解析游标')
        with self._reading():
            clauses=['"'+k+'"=?' for k,v in filters.items() if v is not None];values=[v for v in filters.values() if v is not None]
            if reference is not None and canonical is None:clauses.append('FALSE')
            where=' WHERE '+' AND '.join(clauses)
            total=self._fetch('SELECT count(*) AS n FROM '+self.table('general_events')+where,values,maximum=1)[0]['n']
            rows=self._fetch('SELECT * FROM '+self.table('general_events')+where+' AND event_id>? ORDER BY event_id LIMIT ?',[*values,after,limit],maximum=limit)
            state='Unknown' if reference is not None and canonical is None else 'missing' if total==0 else 'ambiguous' if total>1 else 'matched'
            next_cursor={'binding':binding,'after':rows[-1]['event_id']} if len(rows)==limit else None
            progress={'scope':previous['scope'] if previous and not previous['finished'] else 'full_query' if after==-1 else 'query_suffix','binding':binding,'finished':next_cursor is None,'next_after':next_cursor['after'] if next_cursor else None}
            require(key in self.progress or len(self.progress)<self.h.collection_limits.edges,'H5解析scope超限');self.progress[key]=progress
            require(len(row_bytes(self.progress))<=self.h.collection_limits.metadata_bytes,'H5解析scope字节超限')
            if next_cursor:self.pending.add(key)
            else:self.pending.discard(key);self._complete('resolve:'+key,{'resolution':state,'total':total})
            result={'qualification':'provisional','resolution':state,'total':total,'candidates':rows,'next_cursor':next_cursor,'query_scope':progress}
            require(len(row_bytes(result))<=self.h.limits.batch_bytes,'H5解析整页超限');return result

    def document(self,document_id,*,offset=0,length=65536):
        """原文档分块读取；大series不用detail重建Python列表。"""
        self._check()
        if self.active or any(type(v) is not int or v<0 for v in (document_id,offset,length)) or not 1<=length<=self.h.collection_limits.chunk_bytes:raise ValueError('H5原件分块参数无效')
        with self._reading():
            docs=self._fetch('SELECT * FROM '+self.table('documents')+' WHERE document_id=?',[document_id],maximum=1)
            if not docs:raise ValueError('H5原文档missing')
            doc=docs[0];size=doc['byte_end']-doc['byte_start'];require(offset<=size,'H5原件offset越界')
            end=min(size,offset+length);source=self.h.data_root/self.token.collection.collection_id/'source'
            raw=read_span(source,doc['entity_path'],doc['byte_start']+offset,doc['byte_start']+end,self.budget)
            response={'qualification':'provisional','document':doc,'offset':offset,'raw':raw,'next_offset':end if end<size else None,'scope':'original_span'}
            require(len(row_bytes(response))<=self.h.limits.batch_bytes,'H5原件块返回超限')
            self._complete(f'document:{document_id}:{offset}:{end}',{'document_id':document_id,'byte_start':offset,'byte_end':end,'scope':'original_span'});return response

    def _object(self,document_id):
        from data_pipeline.history.event_index.exact import native
        doc=self._fetch('SELECT * FROM '+self.table('documents')+' WHERE document_id=?',[document_id],maximum=1)[0]
        raw=read_span(self.h.data_root/self.token.collection.collection_id/'source',doc['entity_path'],doc['byte_start'],doc['byte_end'],self.budget,min(self.h.collection_limits.metadata_bytes,self.h.limits.batch_bytes))
        return native(loads(raw))

    def overview(self,event_id):
        self._check()
        if self.active or type(event_id) is not int or event_id<0:raise ValueError('H5 overview参数无效')
        with self._reading():
            event=self._gate(event_id)
            doc=self._fetch('SELECT document_id FROM '+self.table('event_files')+" WHERE event_id=? AND role='overview'",[event_id],maximum=1)[0]['document_id']
            source=self._object(doc)
            result={'qualification':'provisional','event':event,'source':source,'legacy_derived':{'observation_state':'evidence_complete','data_mode':'replay','quality_state':'complete','missing_slot_count':0},'upstream_artifacts':'unresolved_external'}
            # 源缺失常量不倒灌进source；原文/精确词法在scalar_fields和原span中。
            require(len(row_bytes(result))<=self.h.limits.batch_bytes,'H5 overview返回超限');self._complete('overview:'+str(event_id));return result

    def affected_asns(self,event_id,**parameters):
        return self._legacy_page('affected_as',event_id,parameters)

    def path_downstreams(self,event_id,**parameters):
        return self._legacy_page('path_relations',event_id,parameters)

    def _legacy_page(self,name,event_id,parameters):
        from data_pipeline.history.country_events.model import AS_FIELDS, REL_FIELDS
        result=self.query(name,event_id=event_id,**parameters)
        with self._reading():
            items=[];size=0
            for row in result['rows']:
                source=self._object(row['document_id']);keys=AS_FIELDS if name=='affected_as' else [*REL_FIELDS,'path_samples']
                item={k:source[k] for k in keys};size+=len(row_bytes(item))
                require(size<=self.h.limits.batch_bytes,'H5兼容页累计字节超限');items.append(item)
            response={**result,'items':items}
            require(len(row_bytes(response))<=self.h.limits.batch_bytes,'H5旧字段兼容页超限');return response
