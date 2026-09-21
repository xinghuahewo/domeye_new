"""只从C.1固定节点逐项投影；series按node/index归并，不重建整事件列表。"""
from datetime import datetime, timezone
import hashlib
import json
import math
from decimal import Decimal
from data_pipeline.history.event_collection.spool import Spool, Node
from data_pipeline.history.event_collection.model import DEFINITIONS as STRUCTURE
from data_pipeline.history.event_collection.children import directory_bytes
from data_pipeline.history.event_collection.freeze import GENERAL_SOURCE_FIELDS
from data_pipeline.history.event_index.cleanup import owner, release
from services import country_outage_general_read_model as legacy
from data_pipeline.history.country_events.model import DEFINITIONS, ORDER, AS_FIELDS, REL_FIELDS, EVENT_FIELDS, IDENTITIES, row_bytes, require


def scalar(node):
    if node is None:return None
    r=node.row;k=r['kind']
    require(k not in ('array','object'),'H5要求scalar')
    if k=='string':return r['text_value']
    if k=='number':return Decimal(r['number_lexeme'])
    if k=='bool':return bool(r['bool_value'])
    return None


def numeric(node):
    if node is None:return {'presence':'missing','kind':None}
    r=node.row
    return {'presence':'present',**{k:r[k] for k in ('kind','text_value','bool_value','number_lexeme','sign','coefficient_digits','exponent10','negative_zero')}}


class Projector:
    def __init__(self,h,token,out,budget):
        self.h,self.token,self.out,self.budget=h,token,out,budget
        # C.1 Spool的temporary_bytes是逻辑累计；领域磁盘是实际占用，不能混写同一计数。
        self.spool=Spool(out/'projection.sqlite',h._budget(),create=True)
        try:
            self.db=self.spool.db
            for name,columns in DEFINITIONS.items():
                if name=='documents':continue
                self.db.execute('CREATE TABLE '+name+' ('+','.join('"'+c+'" '+{'i':'INTEGER','s':'TEXT'}[t] for c,t in columns)+')')
            self.db.execute('CREATE INDEX event_identity ON general_events(incident_id,publication_id,revision)')
            self.db.execute('CREATE INDEX event_document ON event_files(document_id)')
            self.files={};self.event_count=0;self.ref_count={}
        except BaseException as error:
            # 构造表达式失败时外层owner尚未进入，由取得Spool的一方立即释放。
            release(self.spool,'H5 constructor Spool',error)
            raise
    def close(self):self.spool.close()
    def disk(self):
        self.db.commit();used=directory_bytes(self.out,self.budget)
        require(used<=self.budget.collection_limits.temporary_bytes,'H5暂存磁盘超限')
        self.budget.counts['projection_spool_bytes']=self.db.execute('PRAGMA page_count').fetchone()[0]*self.db.execute('PRAGMA page_size').fetchone()[0]
        self.budget.counts['temporary_bytes']=used
        self.budget.counts['profile_candidate_disk_peak_bytes']=max(used,self.budget.counts.get('profile_candidate_disk_peak_bytes',0));self.budget.check()
    def add(self,table,values):
        row={c:values.get(c) for c,_ in DEFINITIONS[table]}
        for c,t in DEFINITIONS[table]:
            if isinstance(row[c],bool):row[c]=int(row[c])
            require(row[c] is None or type(row[c]) is {'i':int,'s':str}[t],'H5领域类型冲突 '+table+'.'+c)
        size=len(row_bytes(row));require(size<=self.budget.limits.max_row_bytes,'H5领域单行超限')
        self.budget.add('profile_rows',1,self.budget.limits.max_total_rows);self.budget.add('profile_bytes',size,self.budget.limits.max_total_bytes)
        self.db.execute('INSERT INTO '+table+' VALUES ('+','.join('?' for _ in row)+')',list(row.values()))
        if self.budget.counts['profile_rows']%self.budget.limits.batch_rows==0:self.disk()
    def rows(self,name):
        for r in self.db.execute('SELECT * FROM '+name+' ORDER BY '+ORDER[name]):self.budget.check();yield dict(r)
    def node(self,doc,ordinal=0):return self.spool.node(doc,ordinal)
    def document(self,fid):
        rows=self.db.execute('SELECT document_id FROM documents WHERE file_id=? LIMIT 2',(fid,)).fetchall()
        require(len(rows)==1,'H5对象文档不唯一');return self.node(rows[0][0])
    def target(self,fid,role,ref):
        rows=self.db.execute('SELECT target_file FROM edges WHERE parent_file=? AND role=? AND reference=?',(fid,role,ref)).fetchall()
        require(len(rows)==1 and rows[0][0] is not None,'H5文件引用未唯一闭合');return rows[0][0]
    def fields(self,event,node,owner_name,ordinal=0,skip=()):
        # 本方法只展开有限角色的元数据；series长数组另按索引投影。
        for child in node.children('object'):
            if child.row['key'] in skip:continue
            self.field_tree(event,child,owner_name,ordinal)
    def field_tree(self,event,node,owner_name,ordinal):
        self.budget.check()
        r=node.row
        if r['kind'] in ('object','array'):
            for child in node.children(r['kind']):self.field_tree(event,child,owner_name,ordinal)
        else:self.add('scalar_fields',{'event_id':event,'document_id':node.doc,'node_ordinal':node.ordinal,'owner':owner_name,'owner_ordinal':ordinal,'member_ordinal':r['member_ordinal'],'name':r['key'] or '',**numeric(node)})
    def canonical_parts(self,node,omit_content=False):
        """只复现旧content hash规范；typed值从原节点取，绝不从本float通道取。"""
        self.budget.check();kind=node.row['kind']
        if kind=='object':
            require(self.db.execute('SELECT 1 FROM nodes WHERE document_id=? AND parent_ordinal=? GROUP BY key HAVING count(*)>1 LIMIT 1',(node.doc,node.ordinal)).fetchone() is None,'H5旧content规范无法解释重键')
            yield b'{';first=True
            cursor=self.db.execute('SELECT * FROM nodes WHERE document_id=? AND parent_ordinal=? ORDER BY key',(node.doc,node.ordinal))
            for row in cursor:
                if omit_content and row['key']=='content_sha256':continue
                if not first:yield b','
                first=False;yield json.dumps(row['key'],ensure_ascii=False).encode();yield b':'
                yield from self.canonical_parts(Node(self.spool,dict(row)))
            yield b'}'
        elif kind=='array':
            yield b'[';first=True
            for child in node.children('array'):
                if not first:yield b','
                first=False;yield from self.canonical_parts(child)
            yield b']'
        else:
            value=json.loads(node.row['number_lexeme']) if kind=='number' else scalar(node)
            require(not isinstance(value,float) or math.isfinite(value),'H5旧content数值规范Unknown')
            yield json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
    def content(self,node):
        digest=hashlib.sha256()
        for block in self.canonical_parts(node,True):
            self.budget.add('legacy_canonical_bytes',len(block),self.budget.limits.max_total_bytes);digest.update(block)
        require(digest.hexdigest()==node.field('content_sha256').text(),'H5原content摘要冲突')
    def prepare(self):
        with self.h.collection(self.token) as session:
            for name in STRUCTURE:
                with owner(session.bulk(name)) as stream:
                    for batch in stream:
                        for original in batch['rows']:
                            row={k:format(v,'f') if isinstance(v,Decimal) else v for k,v in original.items()}
                            self.spool.add(name,row)
                            if name=='files':
                                require(row['profile']=='general-read-model/v1','H5仅接受General闭包');self.files[row['file_id']]=row
                        self.disk()
        require(session.receipt is not None,'H5结构输入未完成');self.input_resources=session.receipt['resources']
        self.spool.flush();self.input_spool_resources=self.spool.budget.report()
        for fid,f in self.files.items():
            if f['role']=='general-root':self.store(fid,self.document(fid))
        require(self.event_count>0,'H5没有事件')
        # 跨根保留全部候选；单根内部冲突仍沿不变C.1冻结门禁拒绝。
        for key in ('incident_id','publication_id','event_read_model_id','canonical_reference'):
            self.db.execute('UPDATE general_events SET admission=\'identity_conflict\' WHERE '+key+' IN (SELECT '+key+' FROM general_events WHERE '+key+' IS NOT NULL GROUP BY '+key+' HAVING count(*)>1)')
        self.bind_references();self.bind_stores();self.disk()
    def store(self,fid,node):
        self.content(node)
        expected={'schema_version':legacy.STORE_SCHEMA,'status':'complete','collector_id':legacy.COLLECTOR_ID,'window_start_utc':legacy.WINDOW_START_UTC,'window_end_exclusive_utc':legacy.WINDOW_END_EXCLUSIVE_UTC,'api_read_semantics':'precompiled_event_window_read_model_only','pagination_semantics':'stable_server_side_pages_maximum_60_items','path_evidence_semantics':'bounded_real_path_samples_full_evidence_remains_in_s3_audit_artifact','causal_boundary':'rrc25_path_association_is_not_dependency_propagation_user_impact_or_cause'}
        for k,v in expected.items():require(node.field(k).text()==v,'H5根合同冲突 '+k)
        for k in ('run_id','dataset_id','implementation_id',*GENERAL_SOURCE_FIELDS):require(node.field(k).text(),'H5根身份缺失 '+k)
        self.add('general_stores',{'store_id':fid,'file_id':fid,'document_id':node.doc,'manifest_sha256':self.files[fid]['raw_sha'],'content_sha256':node.field('content_sha256').text(),'admission':'available',**{k:node.field(k).text() for k in ('dataset_id','run_id','implementation_id')}})
        self.fields(-fid-1,node,'store',skip=('events',))
        for event in node.field('events').children('array'):self.event(fid,event)
    def event(self,fid,event):
        eid=self.event_count;self.event_count+=1;self.content(event)
        require(event.field('status').text()=='complete' and event.field('publication_state').text()=='published' and event.field('revision').integer()==1,'H5原事件合同冲突')
        for k in EVENT_FIELDS:event.field(k)
        require(event.field('window_end_utc').text()==event.field('data_through').text(),'H5截止点冲突')
        reference=event.field('legacy_reference').text();canonical=legacy._canonical_reference(reference)
        admission='identity_unknown' if canonical is None else 'available' if canonical.split('/')[2]==event.field('country_code').text() else 'identity_conflict'
        values={k:event.field(k).text() for k in ('incident_id','publication_id','event_read_model_id','country_code','cohort_id','event_metric_id','event_as_path_id','lifecycle_state')}
        self.add('general_events',{'event_id':eid,'store_id':fid,'event_ordinal':event.row['member_ordinal'],'document_id':event.doc,'node_ordinal':event.ordinal,'revision':event.field('revision').integer(),'legacy_reference':reference,'canonical_reference':canonical,'admission':admission,**values})
        self.fields(eid,event,'event',skip=('overview','series','affected_as','path_downstreams'))
        for role in ('overview','series','affected_as','path_downstreams'):
            target=self.target(fid,role,event.field(role).field('path').text())
            for row in self.db.execute('SELECT document_id,document_ordinal FROM documents WHERE file_id=? ORDER BY document_ordinal',(target,)):
                doc,ordinal=row;node=self.node(doc)
                self.add('event_files',{'event_id':eid,'role':role,'file_id':target,'document_id':doc,'row_ordinal':ordinal})
                self.references(eid,node,event,self.document(fid))
                if role=='overview':self.overview(eid,node,event)
                elif role=='series':self.series(eid,node,event)
                else:self.list_row(eid,role,ordinal,node)
    def overview(self,eid,node,event):
        self.content(node);self.fields(eid,node,'overview')
        for k in ('state_point_count','affected_as_count','path_downstream_relation_count'):
            require(node.field(k).integer()==event.field(k).integer(),'H5 overview人口冲突 '+k)
        if event.field('lifecycle_state').text()=='event_end_unknown':
            require(node.field('event_end_at_utc').row['kind']=='null' and node.field('event_duration_seconds').row['kind']=='null' and scalar(event.field('is_final_in_data_range')) is False,'H5未知结束不能补结束或final')
        for section in ('cohort','final_values','peaks'):
            for metric in node.field(section).children('object'):
                value=metric.field('value') if section=='peaks' else metric
                when=metric.field('state_point_utc') if section=='peaks' else None
                self.add('overview_metrics',{'event_id':eid,'section':section,'metric_ordinal':metric.row['member_ordinal'],'metric':metric.row['key'],'document_id':node.doc,'node_ordinal':metric.ordinal,'value_node':value.ordinal,'peak_time_node':when.ordinal if when else None})
        for k in ('capabilities','semantic_boundary','interval_seconds','route_interrupted_as_count','concurrent_path_downstream_relation_count'):node.field(k)
    def series(self,eid,node,event):
        self.content(node);self.fields(eid,node,'series',skip=('timestamps','tracks','track_definitions'))
        points=node.field('point_count').integer();times=node.field('timestamps');definitions=node.field('track_definitions')
        require(points==event.field('state_point_count').integer() and times.row['child_count']==points,'H5时点人口冲突')
        for track in node.field('tracks').children('object'):
            ordinal=track.row['member_ordinal'];definition=definitions.field(track.row['key'],False)
            require(track.row['kind']=='array' and track.row['child_count']==points,'H5轨道长度冲突')
            self.add('track_definitions',{'event_id':eid,'track_ordinal':ordinal,'track':track.row['key'],'document_id':node.doc,'node_ordinal':track.ordinal,'definition_node':definition.ordinal if definition else None,'presence':'present' if definition else 'missing'})
            if definition:self.field_tree(eid,definition,'track_definition',ordinal)
            # 两个SQLite游标各只持当前节点；不list timestamps/tracks，不重建大JSON。
            for i,(timestamp,value) in enumerate(zip(times.children('array'),track.children('array'),strict=True)):
                require(timestamp.row['member_ordinal']==value.row['member_ordinal']==i,'H5轨道原序冲突')
                text=timestamp.text();parsed=datetime.fromisoformat(text.replace('Z','+00:00'))
                require(parsed.utcoffset() is not None,'H5无时区时点Unknown')
                require(value.row['kind'] in ('number','null'),'H5轨道值必须数值或null')
                self.add('series_points',{'event_id':eid,'track_ordinal':ordinal,'point_index':i,'document_id':node.doc,'timestamp_node':timestamp.ordinal,'value_node':value.ordinal,'timestamp_text':text,'timestamp_utc':parsed.astimezone(timezone.utc).isoformat(),**numeric(value)})
        # 未配有轨道的额外定义也保留且可枚举。
        for definition in definitions.children('object'):
            if node.field('tracks').field(definition.row['key'],False) is None:
                self.add('track_definitions',{'event_id':eid,'track_ordinal':-definition.row['member_ordinal']-1,'track':definition.row['key'],'document_id':node.doc,'node_ordinal':definition.ordinal,'definition_node':definition.ordinal,'presence':'definition_without_track'})
                self.field_tree(eid,definition,'track_definition',-definition.row['member_ordinal']-1)
    def list_row(self,eid,role,ordinal,node):
        fields=AS_FIELDS if role=='affected_as' else REL_FIELDS
        for key in fields:node.field(key)
        self.fields(eid,node,role,ordinal,skip=('path_samples',))
        search_fields=('asn','as_name','organization','nature') if role=='affected_as' else ('affected_asn','downstream_asn','downstream_as_name','downstream_organization','downstream_nature')
        search=' '.join(str(scalar(node.field(k)) or '').lower() for k in search_fields)
        if role=='affected_as':
            classification=node.field('event_classification').text();require(classification in ('affected','route_interrupted'),'H5 AS分类无效')
            self.add('affected_as',{'event_id':eid,'row_ordinal':ordinal,'document_id':node.doc,'asn':node.field('asn').integer(),'rank':node.field('rank').integer(),'event_classification':classification,'search':search})
        else:
            self.add('path_relations',{'event_id':eid,'relation_ordinal':ordinal,'document_id':node.doc,'search':search,**{k:node.field(k).integer() for k in ('affected_asn','downstream_asn','concurrent_state_point_count')}})
            for sample in node.field('path_samples').children('array'):
                si=sample.row['member_ordinal']
                self.add('path_samples',{'event_id':eid,'relation_ordinal':ordinal,'sample_ordinal':si,'document_id':node.doc,'node_ordinal':sample.ordinal,**{k:sample.field(k).text() for k in ('prefix','address_family','as_path_id','as_path_canonical')}})
                sample.field('route_observation_count');self.fields(eid,sample,'sample',si,skip=('independent_peer_asns',))
                for peer in sample.field('independent_peer_asns').children('array'):
                    self.add('sample_peer_members',{'event_id':eid,'relation_ordinal':ordinal,'sample_ordinal':si,'member_ordinal':peer.row['member_ordinal'],'document_id':node.doc,'node_ordinal':peer.ordinal,'asn':peer.integer()})
    def references(self,eid,node,event,root):
        # 原可选身份缺失是Unknown，显式冲突必须隔离event，不能照旧dict覆盖。
        for i,name in enumerate(IDENTITIES):
            actual=node.field(name,False);expected=event.field(name,False)
            self.ref(eid,node.doc,actual.ordinal if actual else -i-1,name,actual,expected,'event_identity')
        cohort=node.field('cohort',False)
        if cohort is not None:self.ref(eid,node.doc,cohort.ordinal,'cohort_id',cohort.field('cohort_id'),event.field('cohort_id'),'cohort_identity')
        for name in GENERAL_SOURCE_FIELDS:
            actual=node.field(name,False)
            if actual:self.ref(eid,node.doc,actual.ordinal,name,actual,root.field(name),'upstream_declaration')
    def ref(self,eid,doc,ordinal,name,actual,expected,kind):
        def text(n):
            if n is None:return None
            r=n.row;require(r['kind'] not in ('array','object'),'H5身份必须scalar')
            return r['number_lexeme'] if r['kind']=='number' else json.dumps(scalar(n),ensure_ascii=False,separators=(',',':'))
        a,b=text(actual),text(expected)
        state='Unknown' if actual is None or actual.row['kind']=='null' or kind=='identity_only' else 'matched' if a==b else 'missing'
        ordinal_ref=self.ref_count.get(eid,0);self.ref_count[eid]=ordinal_ref+1
        self.add('identity_references',{'event_id':eid,'reference_ordinal':ordinal_ref,'document_id':doc,'node_ordinal':ordinal,'name':name,'actual':a,'expected':b,'resolution':state,'target_event':eid if state=='matched' else None,'relation_kind':kind})
        if state=='missing':self.db.execute("UPDATE general_events SET admission='identity_conflict' WHERE event_id=?",(eid,))
    def bind_references(self):
        # 跨根同一原身份保留全部事件候选；完整集合可审计，受影响business查询隔离。
        for event in self.db.execute('SELECT * FROM general_events ORDER BY event_id'):
            for name in ('incident_id','publication_id','event_read_model_id','canonical_reference'):
                value=event[name]
                if value is None:continue
                matches=self.db.execute('SELECT event_id FROM general_events WHERE '+name+'=? ORDER BY event_id',(value,))
                count=self.db.execute('SELECT count(*) FROM general_events WHERE '+name+'=?',(value,)).fetchone()[0]
                for (target,) in matches:
                    i=self.ref_count.get(event['event_id'],0);self.ref_count[event['event_id']]=i+1
                    self.add('identity_references',{'event_id':event['event_id'],'reference_ordinal':i,'document_id':event['document_id'],'node_ordinal':event['node_ordinal'],'name':name,'actual':value,'expected':value,'resolution':'ambiguous' if count>1 else 'matched','target_event':target,'relation_kind':'event_candidate'})
            root=self.document(event['store_id'])
            for name in GENERAL_SOURCE_FIELDS:self.ref(event['event_id'],root.doc,root.field(name).ordinal,name,root.field(name),None,'identity_only')

    def bind_stores(self):
        # 原根/上游身份声明跨根冲突时保留每个候选，隔离其事件；不声称上游原件已取得。
        stores=self.db.execute('SELECT store_id FROM general_stores ORDER BY store_id')
        for (sid,) in stores:
            source=self.document(sid)
            for field in ('dataset_id','source_event_cohort_dataset_id','source_event_metric_dataset_id','source_event_as_path_dataset_id','source_lifecycle_snapshot_id'):
                actual=source.field(field).text()
                digest_field='content_sha256' if field=='dataset_id' else field.removesuffix('dataset_id')+'content_sha256' if field.endswith('dataset_id') else 'source_lifecycle_snapshot_content_sha256'
                digest_fields=(digest_field,field.removesuffix('dataset_id')+'manifest_sha256') if field.startswith('source_') and field.endswith('dataset_id') else (digest_field,)
                expected_digest=tuple(source.field(k).text() for k in digest_fields)
                candidates=[]
                for (other,) in self.db.execute('SELECT store_id FROM general_stores ORDER BY store_id'):
                    node=self.document(other)
                    if node.field(field).text()==actual:
                        candidates.append((other,tuple(node.field(k).text() for k in digest_fields)))
                        require(len(candidates)<=self.budget.collection_limits.files,'H5根身份候选超限')
                conflict=len(candidates)>1 if field=='dataset_id' else any(digest!=expected_digest for _,digest in candidates)
                if conflict:
                    self.db.execute("UPDATE general_stores SET admission='identity_conflict' WHERE store_id=?",(sid,))
                    self.db.execute("UPDATE general_events SET admission='identity_conflict' WHERE store_id=?",(sid,))
                for (eid,) in self.db.execute('SELECT event_id FROM general_events WHERE store_id=? ORDER BY event_id',(sid,)):
                    for target,digest in candidates:
                        i=self.ref_count.get(eid,0);self.ref_count[eid]=i+1
                        self.add('identity_references',{'event_id':eid,'reference_ordinal':i,'document_id':source.doc,'node_ordinal':source.field(field).ordinal,'name':field,'actual':actual,'expected':actual,'resolution':'ambiguous' if conflict else 'matched','target_store':target,'relation_kind':'store_declaration_candidate'})
