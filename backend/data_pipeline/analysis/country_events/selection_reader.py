"""固定CountrySelection查询，有限B-tree请求；不发布、不修复、不重跑生产。"""
from collections import Counter
from pathlib import Path
import base64
import hashlib
import json
import sqlite3

from data_pipeline.analysis.country_events import event_aggregation as c2
from data_pipeline.analysis.country_events.snapshot_access import FixedComponentAccess
from data_pipeline.analysis.country_events.snapshot_store import cleanup
from data_pipeline.analysis.country_events.selection_admission import verify_country_admission
from data_pipeline.analysis.country_events.snapshot_schema import encode, decode
from data_pipeline.analysis.country_events.selection_contract import *
from data_pipeline.analysis.country_events.selection_index import readonly, check_entities, read_country_event, iter_country_aliases, TRACKS, TRACK_ALIASES, Budget

FILTERS = {
 'overview':(), 'series15':('track',), 'asns':('asn','afi','classification'),
 'asn_matrix':('asn','afi','sample_from','sample_to'), 'asn_window':('asn','afi'),
 'asn_peaks':('asn','afi'), 'paths':('affected_asn',),
 'path_samples':('affected_asn','downstream_asn'), 'audit':('kind',),
 'resolve_source':('ref',), 'resolve_alias':('source','table','ref_kind','id')}


class CountryQuery:
    def __init__(self,selection,proof,*,runtime,verify_qualification,limits):
        self.component_dsn=runtime.component_dsn
        self.selection,self.descriptor,self.limits,self.verify=selection,selection.descriptor,limits,verify_qualification
        self.stats=Counter(); self.db=None; self.access=None; self.closed=False
        self.guard=Budget(selection.descriptor.read_binding.root,limits,_sample=True)
        try:
            if proof!=self.descriptor.admission_proof: raise ValueError('C4_selection_proof')
            with self.qualify():
                self.check()
                head=read_country_event(self.descriptor,incident_id=selection.incident_id,revision=selection.revision)
                if encode(head)!=selection.event_status_typed or head.cohort_id!=selection.cohort_id: raise ValueError('C4_selection_event')
                self.head=head
                self.db=readonly(Path(self.descriptor.read_binding.root)/'index.sqlite')
                self.db.set_trace_callback(lambda sql: self.stats.update({'sqlite_statements':1}))
                manifest=json.loads((Path(self.descriptor.read_binding.component.root)/'manifest.json').read_text())
                self.access=FixedComponentAccess(runtime.component_dsn,self.descriptor.read_binding.component,manifest,limits=limits,guard=self.check)
                self.check()
        except BaseException as error: cleanup((self.close,),error); raise

    def check(self,*,force_sample=False):
        if self.closed: raise ValueError('C4_query_closed')
        self.guard(force=force_sample); check_entities(self.descriptor.entities)

    def close(self):
        def access():
            if self.access:
                self.stats.update(self.access.stats);self.access.close()
        def database():
            if self.db is not None:self.db.close()
        try:cleanup((access,database))
        finally:self.access=None;self.db=None;self.closed=True
    def __enter__(self): return self
    def __exit__(self,kind,error,traceback): cleanup((self.close,),error)

    def qualify(self):
        from contextlib import contextmanager
        @contextmanager
        def checked():
            # 保持owner上游锁序在先，国家验收目录在后；两者均在本次with内。
            with self.verify(self.descriptor,selection=self.selection):
                with verify_country_admission(self.component_dsn,self.descriptor.read_binding,self.descriptor.admission_proof):
                    yield
        return checked()

    def scope(self,request):
        s=self.selection
        return dict(publication_id=s.publication_id,build_id=s.build_id,profile_digest=s.profile_digest,
            component_key=s.component_key,result_id=s.descriptor.read_binding.result_id,read_model_id=s.descriptor.read_binding.read_model_id,
            component_manifest=s.descriptor.read_binding.component.manifest_sha256,index_manifest=s.descriptor.read_binding.manifest_sha256,
            incident_id=s.incident_id,revision=str(s.revision),cohort_id=s.cohort_id,view=request.view,
            filters=json.loads(request.filters_json),order=request.order,version=VERSION)

    def conditions(self,request):
        filters=json.loads(request.filters_json)
        if set(filters)-set(FILTERS[request.view]): raise ValueError('C4_filter_scope')
        component_scope=request.view=='audit' and filters.get('kind')=='component_scope'
        where=['event=?','view=?']; args=['' if component_scope else self.selection.incident_id,request.view]
        for k,v in filters.items():
            if component_scope and k=='kind': continue
            if k=='kind':
                from data_pipeline.analysis.country_events.snapshot_schema import TABLES
                if v not in TABLES: raise ValueError('C4_audit_kind')
            if k in ('asn','affected_asn','downstream_asn'):
                if type(v) is not int or not 1<=v<=4294967295: raise ValueError('C4_asn_filter')
            elif k=='afi':
                if type(v) is not int or v not in (0,1,2): raise ValueError('C4_afi_filter')
            elif k.startswith('sample_'):
                if type(v) is not int: raise ValueError('C4_sample_filter')
            elif type(v) is not str: raise ValueError('C4_text_filter')
            if k=='classification' and v not in ('all','normal','affected','route_interrupted','unknown'): raise ValueError('C4_class_filter')
            if k=='track':
                if v=='all': continue
                v=TRACK_ALIASES.get(v,v)
                if v not in TRACKS: raise ValueError('C4_track_filter')
            if k=='classification' and v=='all': continue
            column={'affected_asn':'affected','downstream_asn':'downstream','track':'metric','sample_from':'sample','sample_to':'sample'}.get(k,k)
            op='>=' if k=='sample_from' else '<=' if k=='sample_to' else '='
            where.append(column+op+'?'); args.append(v)
        if filters.get('sample_from',0)>filters.get('sample_to',2**63-1): raise ValueError('C4_sample_range')
        return ' AND '.join(where),args

    def static(self,asn):
        row=self.db.execute('SELECT payload FROM static_asns WHERE asn=?',(asn,)).fetchone()
        return decode(row[0]) if row else None

    def item(self,row,actual):
        key,seq,payload=row
        if payload is not None:
            value=decode(payload)
            if 'asn' in value: value={**value,'static':self.static(value['asn'])}
            return value
        item=actual[seq]; source=self.db.execute('SELECT table_name,revision,row_hash FROM rows WHERE seq=?',(seq,)).fetchone()
        value=item.value if isinstance(item,c2.C2Row) else item
        result=dict(source_sequence=seq,table=source[0],row_hash=source[2],incident_id=item.incident_id if isinstance(item,c2.C2Row) else None,
                    revision=int(source[1]) if source[1] is not None else None,value=value,scope='event' if isinstance(item,c2.C2Row) and item.incident_id is not None else 'component')
        if hasattr(value,'asn'): result['static']=self.static(value.asn)
        if hasattr(value,'affected_asn'):
            result['affected_static']=self.static(value.affected_asn); result['downstream_static']=self.static(value.downstream_asn)
            # 路径独立精确峰值保持typed原记录；原值经相同定位Reader核验。
            if source[0]=='path_summary':
                quality=self.db.execute("SELECT table_name,row_group,row_offset,seq,row_hash FROM rows WHERE event=? AND table_name='path_peak_quality' AND affected=? AND downstream=?",(self.selection.incident_id,value.affected_asn,value.downstream_asn)).fetchall()
                result['peak_quality']=tuple(actual[loc[3]].value for loc in quality)
        return result

    def query(self,request,*,cursor=None,limit=100):
        if type(limit) is not int or not 1<=limit<=self.limits.max_page_rows: raise ValueError('C4_page_limit')
        scope=self.scope(request); scope_json=canonical(scope); after=''; returned=0
        if cursor:
            try:
                decoded=json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                if decoded['scope']!=scope or set(decoded)!={'scope','last_key','returned'}: raise ValueError()
                after=decoded['last_key']; returned=decoded['returned']
                if type(after) is not str or type(returned) is not int or returned<1: raise ValueError()
            except Exception as error: raise ValueError('C4_cursor_mismatch') from error
        self.stats['query_calls']+=1
        with self.qualify():
            self.check(force_sample=True)
            from data_pipeline.analysis.country_events.selection_index import counted_progress
            steps=Counter();progress,errors=counted_progress(steps,self.guard)
            self.db.set_progress_handler(progress,1000)
            try:
                if request.view=='resolve_alias':
                    page=self.alias(request,scope_json,limit,returned)
                    with self.qualify(): self.check(force_sample=True)
                    return page
                if request.view=='resolve_source':
                    filters=json.loads(request.filters_json)
                    if set(filters)!={'ref'} or type(filters['ref']) is not str: raise ValueError('C4_source_filter')
                    # 只返回该事件audit已证明可达的引用，不能跨事件凭同ID捞取。
                    where="event=? AND view='audit' AND seq IN (SELECT seq FROM refs WHERE ref=?)"; args=[self.selection.incident_id,filters['ref']]
                else: where,args=self.conditions(request)
                if request.filters_json=='{}' and request.view!='resolve_source':
                    total=self.db.execute('SELECT total FROM totals WHERE event=? AND view=?',(self.selection.incident_id,request.view)).fetchone()[0]
                else:
                    total=self.db.execute('SELECT count(*) FROM views WHERE '+where,args).fetchone()[0]
                rows=self.db.execute('SELECT key,seq,payload FROM views WHERE '+where+' AND key>? ORDER BY key LIMIT ?',(*args,after,limit+1)).fetchall()
                more=len(rows)>limit; rows=rows[:limit]
                locs=[self.db.execute('SELECT table_name,row_group,row_offset,seq,row_hash FROM rows WHERE seq=?',(seq,)).fetchone() for _,seq,payload in rows if seq is not None]
                extra=[]
                for table,group,offset,seq,sha in locs:
                    if table=='path_summary':
                        relation=self.db.execute('SELECT affected,downstream FROM rows WHERE seq=?',(seq,)).fetchone()
                        extra.extend(self.db.execute("SELECT table_name,row_group,row_offset,seq,row_hash FROM rows WHERE event=? AND table_name='path_peak_quality' AND affected=? AND downstream=?",(self.selection.incident_id,*relation)).fetchall())
                actual=self.access.read(locs+extra)
                items=tuple(self.item(r,actual) for r in rows)
                sizes=[len(encode(x).encode()) for x in items]
                if any(n>self.limits.max_row_bytes for n in sizes): raise ValueError('resource_limit:C4_row_bytes')
                size=sum(sizes)
                if size>self.limits.max_page_bytes: raise ValueError('resource_limit:C4_page_bytes')
                self.stats['max_page_bytes']=max(self.stats['max_page_bytes'],size)
                next_cursor=base64.urlsafe_b64encode(canonical(dict(scope=scope,last_key=rows[-1][0],returned=returned+len(rows))).encode()).decode() if more else None
                availability=self.head.state; reasons=self.head.reasons
                if not self.head.cohort_id and request.view in ('asns','asn_matrix','asn_window','asn_peaks','paths','path_samples','series15'):
                    availability='unavailable'; reasons=(*reasons,'membership_unknown')
                elif total==0 and 'asn' in json.loads(request.filters_json) and request.view in ('asn_matrix','asn_window','asn_peaks'):
                    f=json.loads(request.filters_json)
                    member=self.db.execute("SELECT 1 FROM views WHERE event=? AND view='asns' AND asn=? LIMIT 1",(self.selection.incident_id,f['asn'])).fetchone()
                    reasons=(*reasons,'not_member' if member is None else 'no_original_rows')
                self.check()
            except sqlite3.OperationalError as error:
                if errors:raise errors[0] from error
                if 'interrupt' in str(error): raise ValueError('query_budget_exceeded') from error
                raise
            finally:
                self.stats['sqlite_steps']+=steps['index_steps']; self.db.set_progress_handler(None,0)
        # 离开前一资格上下文后再核当前资格，防止页读取期间撤销被旧事务快照遮住。
        with self.qualify(): self.check(force_sample=True)
        return QueryPage(items,total,next_cursor,scope_json,availability,reasons)

    def alias(self,request,scope_json,limit,returned):
        matches=[]; receipt=None; targets=set(); count=0
        for row in iter_country_aliases(self.descriptor,filters_json=request.filters_json,limits=self.limits):
            if isinstance(row,QueryReadReceipt): receipt=row
            else:
                if len(targets)<2: targets.add((row['incident_id'],row['revision'],row['cohort_id']))
                if returned<=count<returned+limit: matches.append(row)
                count+=1
        if receipt is None: raise ValueError('C4_alias_incomplete')
        state='not_found' if not targets else 'resolved' if len(targets)==1 else 'ambiguous_reference'
        if sum(len(encode(row).encode()) for row in matches)>self.limits.max_page_bytes: raise ValueError('resource_limit:C4_alias_candidates')
        more=returned+len(matches)<count
        cursor=base64.urlsafe_b64encode(canonical(dict(scope=json.loads(scope_json),last_key=str(returned+len(matches)),returned=returned+len(matches))).encode()).decode() if more else None
        self.check()
        return QueryPage(tuple(matches),count,cursor,scope_json,state,())

    def iter_query(self,request,*,page_size=100):
        cursor=None; sha=hashlib.sha256(); count=0; scope=None; total=None
        while True:
            page=self.query(request,cursor=cursor,limit=page_size)
            if total is None: total=page.total; scope=page.scope_json
            if page.total!=total or page.scope_json!=scope: raise ValueError('C4_stream_scope_drift')
            for item in page.items:
                sha.update(encode(item).encode()); count+=1; yield item
            cursor=page.next_cursor
            if cursor is None: break
        if count!=total: raise ValueError('C4_stream_total_mismatch')
        with self.qualify(): self.check(force_sample=True)
        yield QueryReadReceipt(scope,count,sha.hexdigest())


def open_qualified_country(selection, admission_proof, *, runtime, verify_qualification, limits=QueryLimits()):
    return CountryQuery(selection,admission_proof,runtime=runtime,verify_qualification=verify_qualification,limits=limits)
