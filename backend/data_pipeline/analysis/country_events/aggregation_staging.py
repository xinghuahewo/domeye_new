"""C2私有磁盘暂存：只接收本次内存typed行，不读取外部pickle或恢复旧工作库。"""
from collections import OrderedDict
from dataclasses import replace
import pickle
import sqlite3
import struct
from pathlib import Path as FilePath
from collections import Counter
from data_pipeline.analysis.country_events.models import Binding, Cursor, Endpoint, Incident, Reference, Route, Time
from data_pipeline.analysis.country_events.compute import Change, Path, Segment
from data_pipeline.analysis.country_events.saved_input import SavedBatch, CountryRevision, InputCompletion, SavedDelta, SavedInvalidation, object_scope
from data_pipeline.bgp.archive.message_reader import MessageBatch
from data_pipeline.analysis.detection.reference_view import ReferenceView, ReferenceSource


def dump(value):return pickle.dumps(value,protocol=5)
def load(value):return pickle.loads(value)


class ChangeReferences:
    """本次 Stage 中独立计算域；保存完整 typed 引用，不拼接或截断。"""
    def __init__(self,stage,domain):
        self.stage,self.domain,self.closed=stage,domain,False

    def key(self,value):
        self.stage.guard()
        data=dump(value)
        if len(data)>self.stage.limits['max_row_bytes']:
            raise ValueError('resource_limit:C2_input_row')
        return data

    def sql(self,statement,args,*,write=False):
        errors=[]
        def progress():
            try:return self.stage._progress()
            except BaseException as error:errors.append(error);return 1
        # 单条写入前后检查；禁止 SQLite 中断写语句回滚同 Stage 其他活动域。
        self.stage.db.set_progress_handler(None if write else progress,1000)
        try:return self.stage.db.execute(statement,args).fetchone()
        except sqlite3.OperationalError as error:
            if errors:raise errors[0] from error
            raise
        finally:self.stage.db.set_progress_handler(self.stage._progress,1000)

    def __contains__(self,value):
        if self.closed:raise ValueError('C2_reference_domain_closed')
        return self.sql('SELECT 1 FROM change_references WHERE domain=? AND reference=?',
                        (self.domain,self.key(value))) is not None

    def add(self,value):
        if self.closed:raise ValueError('C2_reference_domain_closed')
        self.sql('INSERT INTO change_references VALUES (?,?)',(self.domain,self.key(value)),write=True)
        self.stage.guard()

    def close(self):
        if self.closed:return
        if not self.stage.closed:
            # 清理不可被已取消的 guard 阻断，随后恢复本 Stage 原回调。
            self.stage.db.set_progress_handler(None,0)
            try:self.stage.db.execute('DELETE FROM change_references WHERE domain=?',(self.domain,))
            finally:self.stage.db.set_progress_handler(self.stage._progress,1000)
        self.closed=True


class DiskMap:
    def __init__(self,stage,kind,cache_size=256):
        self.stage,self.kind,self.cache_size=stage,kind,cache_size
        self.cache=OrderedDict()
    def get(self,key,default=None):
        self.stage.guard()
        if key in self.cache:
            self.cache.move_to_end(key);return self.cache[key]
        row=self.stage.db.execute('SELECT payload FROM dictionaries WHERE kind=? AND key=?',(self.kind,str(key))).fetchone()
        if row is None:return default
        value=load(row[0]);self.cache[key]=value
        if len(self.cache)>self.cache_size:self.cache.popitem(last=False)
        return value
    def __contains__(self,key):return self.get(key) is not None
    def __getitem__(self,key):
        value=self.get(key)
        if value is None:raise KeyError(key)
        return value


def segments(raw,width):
    result=[];offset=0
    while raw and offset<len(raw):
        kind,count=raw[offset:offset+2];offset+=2
        values=struct.unpack('!'+('I' if width==4 else 'H')*count,raw[offset:offset+width*count]);offset+=width*count
        result.append(Segment({1:'set',2:'sequence',3:'confed_sequence',4:'confed_set'}[kind],values))
    if raw and offset!=len(raw):raise ValueError('stored_path_segment_length')
    return tuple(result)


class Stage:
    def __init__(self,path,binding,guard,limits):
        self.db=sqlite3.connect(str(path));self.db.execute('PRAGMA cache_size=-8192');self.db.execute('PRAGMA temp_store=FILE')
        self.binding,self.guard,self.limits=binding,guard,limits
        self.directory=FilePath(path).parent
        self.reference_interpretation=None
        self.closed=False;self._reference_domain=0
        self.db.executescript('''
          CREATE TABLE change_references(domain INTEGER,reference BLOB,PRIMARY KEY(domain,reference)) WITHOUT ROWID;
          CREATE TABLE raw_elements(id TEXT PRIMARY KEY,payload BLOB);
          CREATE TABLE raw_deltas(id TEXT PRIMARY KEY,payload BLOB);
          CREATE TABLE raw_invalidations(message TEXT,object_id TEXT,payload BLOB,PRIMARY KEY(message,object_id));
          CREATE TABLE dictionaries(kind TEXT,key TEXT,payload BLOB,PRIMARY KEY(kind,key));
          CREATE TABLE revisions(incident TEXT,revision INTEGER,payload BLOB,PRIMARY KEY(incident,revision));
          CREATE TABLE evidence(kind TEXT,key TEXT,payload BLOB);
          CREATE TABLE unsorted_units(rank INTEGER,record INTEGER,phase INTEGER,ordinal INTEGER,lo INTEGER,hi INTEGER,prefix TEXT,country TEXT,payload BLOB,
            PRIMARY KEY(rank,record,phase,ordinal));
          CREATE TABLE current(object_id TEXT PRIMARY KEY,prefix TEXT,country TEXT,presence TEXT,payload BLOB);
          CREATE INDEX current_country ON current(country,presence,prefix);
          CREATE INDEX current_prefix ON current(prefix,object_id);
          CREATE TABLE observed_fact(reference TEXT PRIMARY KEY,payload BLOB);
          CREATE TABLE identities(object_id TEXT PRIMARY KEY,identity BLOB);
        ''')
        self.paths,self.refs=DiskMap(self,'path'),DiskMap(self,'reference')
        self.counts=Counter(staged_changes=0,staged_invalidations=0,global_units_scanned=0,state_assignments=0,activation_member_rows=0,related_changes_sent=0,boundary_range_rows=0)
        self.completion=None
        self.global_gap=False
        def progress():
            self.counts["sqlite_vm_steps_lower_bound"]+=1000
            self.guard()
            return 0
        self._progress=progress
        self.db.set_progress_handler(progress,1000)

    def change_references(self):
        self._reference_domain+=1
        return ChangeReferences(self,self._reference_domain)

    def checked(self,value):
        self.guard()
        data=dump(value)
        if len(data)>self.limits["max_row_bytes"]:raise ValueError("resource_limit:C2_input_row")
        self.counts["staged_rows"]+=1
        self.counts["staged_bytes"]+=len(data)
        return data

    def put(self,kind,key,value):
        self.guard();self.db.execute('INSERT OR REPLACE INTO dictionaries VALUES (?,?,?)',(kind,str(key),self.checked(value)))

    def consume_c1(self,stream,reference_source):
        for item in stream:
            self.guard()
            if self.completion is not None:raise ValueError('rows_after_C1_completion')
            if isinstance(item,MessageBatch):
                for element in item.elements:self.db.execute('INSERT INTO raw_elements VALUES (?,?)',(element['event_id'],self.checked(element)))
            elif isinstance(item,CountryRevision):
                self.db.execute('INSERT INTO revisions VALUES (?,?,?)',(item.incident_id,item.revision,self.checked(item)))
            elif isinstance(item,SavedBatch):
                for row in item.rows:
                    self.guard()
                    if item.table=='changes':self.db.execute('INSERT INTO raw_deltas VALUES (?,?)',(row.event_ref,self.checked(row)))
                    elif item.table=='invalidations':self.db.execute('INSERT INTO raw_invalidations VALUES (?,?,?)',(row.message_ref,row.object_key,self.checked(row)))
                    elif item.table=='paths':
                        self.put('path',row['path_key'],Path(row['path_key'],segments(row['as_path_raw'],row['asn_width']),row['attributes_raw'],segments(row['as4_path_raw'],4)))
                    elif item.table=='references' and row['source_id']==reference_source:
                        self.db.execute('INSERT INTO evidence VALUES (?,?,?)',('as_info',str(row['row']),self.checked(row)))
                    elif item.table in ('quality','baseline_mappings','detection_evidence'):
                        if item.table=='quality' and row['code'] not in ('timestamp_regression','cross_file_time_overlap'):raise ValueError('unsupported_saved_quality:'+row['code'])
                        self.db.execute('INSERT INTO evidence VALUES (?,?,?)',(item.table,str(self.db.total_changes),self.checked(row)))
            elif isinstance(item,InputCompletion):self.completion=item
        if self.completion is None:raise ValueError('C1_input_not_complete')
        self.prepare_saved(reference_source)

    def prepare_saved(self,reference_source):
        """从已完整暂存的原保存态连接参考与变化；完成证明由各入口分别核验。"""
        # 使用Detection同一整列类型推断、重复选择、关系字段解析和原行定位。
        # sink只把选中解释移交磁盘，不能另造按int/ascii转换后取首/末行的规则。
        snapshot_ref=f'{self.binding.run_id}:{self.binding.snapshot_id}'
        view=ReferenceView(self.binding.reference_version,snapshot_ref,
                           self.directory/'reference-view',guard=self.guard)
        count=self.db.execute("SELECT count(*) FROM evidence WHERE kind='as_info'").fetchone()[0]
        def selected(key,record,locator):
            self.counts['reference_selected_rows']+=1
            self.put('reference_selection',key,locator)
            # Route.origin是显式整数；Detection以str(origin)精确查键。
            # '002'（字符串列）、'2.0'等不得偷偷归并成整数2。
            if not key.isascii() or not key.isdecimal() or str(int(key))!=key:
                return
            asn=int(key)
            if not 1<=asn<=4294967295:return
            country=record.get('as_country')
            known=isinstance(country,str) and len(country)==2 and country.isascii() and country.isalpha() and country.isupper()
            self.put('reference',asn,Reference(asn,country if known else None,
                f'{snapshot_ref}/references/{reference_source}/{locator["row"]}',
                'known' if known else 'unknown'))
        rows=(load(r[0]) for r in self.db.execute("SELECT payload FROM evidence WHERE kind='as_info' ORDER BY CAST(key AS INTEGER)"))
        view.add(ReferenceSource('as_info',reference_source,count,rows),selected_sink=selected)
        self.reference_interpretation=dict(selection_rule=view.version_rule,
            reference_version=self.binding.reference_version,snapshot_ref=snapshot_ref,
            source_id=reference_source,original_rows=count,selected_rows=self.counts['reference_selected_rows'],
            original_evidence='fixed_observation_references_all_rows',
            selected_locator='EventStatus.reference_selections_and_FirstQualifiedRef.country_reference')
        for _,payload in self.db.execute('SELECT id,payload FROM raw_deltas'):
            delta=load(payload)
            element=load(self.db.execute('SELECT payload FROM raw_elements WHERE id=?',(delta.event_ref,)).fetchone()[0])
            scope=object_scope(delta.original)
            endpoint=Endpoint(*scope[:8])
            mapping_state='unmapped' if endpoint.local_ip.startswith('unmapped_rib:') else 'bound'
            route=Route(delta.object_key,endpoint,scope[8],scope[10],delta.after_presence,delta.after_path,delta.after_origin,delta.event_ref,delta.at,delta.cursor,
                        f'{self.binding.run_id}:{self.binding.snapshot_id}/mapping/{endpoint.remote_ip}/{endpoint.remote_asn}',
                        f'{self.binding.run_id}:{self.binding.snapshot_id}/elements/{delta.event_ref}/peer',mapping_state,delta.original['fact_after_presence'])
            self.add_change(Change(delta.cursor,delta.at,delta.source_id,delta.event_ref,route))
        for (message,) in self.db.execute('SELECT DISTINCT message FROM raw_invalidations'):
            values=[]
            for r in self.db.execute('SELECT payload FROM raw_invalidations WHERE message=? ORDER BY object_id',(message,)):
                self.guard()
                if len(values)>=self.limits['max_objects']:raise ValueError('resource_limit:C2_invalidation_group')
                values.append(load(r[0]))
            first=values[0]
            self.add_change(Change(Cursor(first.source_rank,first.record,0),first.at,self.binding.source_at(first.source_rank),message,invalidated_objects=tuple(r.object_key for r in values)),phase=0)
        # LOCAL只保留原事实，不变成规范当前态或采样边界。
        for (payload,) in self.db.execute('SELECT payload FROM raw_elements'):
            row=load(payload)
            if not row['local_message']:continue
            endpoint=Endpoint(self.binding.collector,row['peer_ip'],row['peer_asn'],row['local_ip'],row['local_asn'],row['interface'],row['afi'],row['safi'])
            route=Route('local:'+row['event_id'],endpoint,row['prefix'],row['path_id'],'absent' if row['action']=='withdraw' else 'present',row['path_key'],row['attributed_origin_asn'],row['event_id'],Time(row['epoch'],row['microsecond']),Cursor(self.binding.rank_of(row['source_id']),row['record'],row['ordinal']),
                        'local_original_endpoint',f'{self.binding.run_id}:{self.binding.snapshot_id}/elements/{row["event_id"]}/peer',fact_presence='absent' if row['action']=='withdraw' else 'present',local_message=True)
            self.validate_route(route)
            self.db.execute('INSERT INTO observed_fact VALUES (?,?)',(route.observation_ref,dump(route)))
        self.finish()

    def validate_route(self,route):
        self.guard()
        if route.endpoint.collector!=self.binding.collector:raise ValueError('C2_collector_mismatch')
        if (route.presence=='present' and route.path_ref is None) or (route.path_ref is not None and route.path_ref not in self.paths):raise ValueError('C2_missing_path')
        identity=dump((route.endpoint,route.prefix,route.path_id))
        found=self.db.execute('SELECT identity FROM identities WHERE object_id=?',(route.object_id,)).fetchone()
        if found and found[0]!=identity:raise ValueError('C2_object_identity_mismatch')
        self.db.execute('INSERT OR IGNORE INTO identities VALUES (?,?)',(route.object_id,identity))

    def add_change(self,change,phase=1):
        self.guard()
        if self.binding.source_at(change.cursor.source_rank)!=change.source_id:raise ValueError('C2_source_cursor_mismatch')
        self.counts["input_units"]+=1
        encoded=self.checked(change)
        prefix=country=None
        if change.after is not None:
            self.validate_route(change.after);prefix=change.after.prefix
            ref=self.refs.get(change.after.origin)
            country=ref.country if ref and ref.state=='known' else None
            self.db.execute('INSERT INTO observed_fact VALUES (?,?)',(change.reference,dump(change.after)))
            if change.local or change.after.local_message:return
            self.counts['staged_changes']+=1
        else:self.counts['staged_invalidations']+=len(change.invalidated_objects)
        self.db.execute('INSERT INTO unsorted_units VALUES (?,?,?,?,?,?,?,?,?)',
                        (change.cursor.source_rank,change.cursor.record,phase,change.cursor.ordinal,change.at.lower_us,change.at.upper_us,prefix,country,encoded))

    def finish(self):
        self.db.executescript('''
          CREATE TABLE units AS SELECT ROW_NUMBER() OVER(ORDER BY rank,record,phase,ordinal)-1 AS i,* FROM unsorted_units;
          CREATE UNIQUE INDEX units_i ON units(i);
          CREATE INDEX units_country ON units(country,prefix,i,lo);
          CREATE TABLE country_prefix AS SELECT country,prefix,max(i) AS last_i FROM units WHERE country IS NOT NULL GROUP BY country,prefix;
          CREATE INDEX country_prefix_lookup ON country_prefix(country,last_i,prefix);
          CREATE INDEX units_cursor ON units(rank,record,phase,ordinal,i);
          CREATE INDEX units_hi ON units(hi,i);
          CREATE TABLE frontiers AS SELECT i,MAX(hi) OVER(ORDER BY i ROWS UNBOUNDED PRECEDING) AS prefix_hi,
            COALESCE(MIN(lo) OVER(ORDER BY i ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),9223372036854775807) AS suffix_lo FROM units;
          CREATE INDEX frontier_prefix ON frontiers(prefix_hi,i);
          CREATE INDEX frontier_suffix ON frontiers(suffix_lo,i);
        ''')
        self.initial_end=self.db.execute('SELECT max(i) FROM units WHERE rank=0').fetchone()[0]
        self.total=self.db.execute('SELECT count(*) FROM units').fetchone()[0]
        self.db.commit()
        self.counts["indexed_units"]=self.total
        self.counts["staging_sql_writes"]=self.db.total_changes

    def boundary(self,time):
        row=self.db.execute('SELECT i,suffix_lo FROM frontiers WHERE prefix_hi<? ORDER BY prefix_hi DESC,i DESC LIMIT 1',(time,)).fetchone()
        if row is None:
            first=self.db.execute('SELECT min(lo) FROM units').fetchone()[0]
            low=-1;suffix=first if first is not None else time
        else:low,suffix=row
        if suffix>=time:return low,low
        high=self.db.execute('SELECT i FROM frontiers WHERE suffix_lo>=? ORDER BY suffix_lo,i LIMIT 1',(time,)).fetchone()[0]
        return low,high

    def impact(self,low,high,country,prefixes,max_span,*,include_potential=False):
        if low==high:return False
        if high-low>max_span:return True
        for (payload,) in self.db.execute('SELECT payload FROM units WHERE i>? AND i<=? ORDER BY i',(low,high)):
            self.guard();self.counts['boundary_range_rows']+=1
            change=load(payload)
            if change.global_invalidation:return True
            if change.after is not None:
                ref=self.refs.get(change.after.origin)
                if change.after.prefix in prefixes or (ref and ref.state=='known' and ref.country==country):return True
            else:
                for obj in change.invalidated_objects:
                    current=self.db.execute('SELECT prefix,country FROM current WHERE object_id=?',(obj,)).fetchone()
                    identity=self.db.execute('SELECT identity FROM identities WHERE object_id=?',(obj,)).fetchone()
                    if identity and load(identity[0])[1] in prefixes:return True
                    if current and current[1]==country:return True
        return False

    def candidates(self,country):
        return (load(r[0]) for r in self.db.execute("SELECT payload FROM current WHERE prefix IN (SELECT prefix FROM current WHERE country=? AND presence IN ('present','unknown')) ORDER BY prefix,object_id",(country,)))

    def apply(self,change):
        self.counts['global_units_scanned']+=1
        if change.global_invalidation:self.global_gap=True
        if change.after is not None:
            route=change.after;ref=self.refs.get(route.origin)
            self.db.execute('INSERT OR REPLACE INTO current VALUES (?,?,?,?,?)',(route.object_id,route.prefix,ref.country if ref and ref.state=='known' else None,route.presence,dump(route)))
            self.counts['state_assignments']+=1
        else:
            query='SELECT object_id,payload FROM current' if change.global_invalidation else 'SELECT object_id,payload FROM current WHERE object_id=?'
            for args in [()] if change.global_invalidation else [(o,) for o in change.invalidated_objects]:
                for obj,payload in self.db.execute(query,args):
                    route=replace(load(payload),presence='unknown',fact_presence='unknown')
                    self.db.execute("UPDATE current SET presence='unknown',payload=? WHERE object_id=?",(dump(route),obj));self.counts['state_assignments']+=1

    def close(self):
        try:self.db.close()
        finally:self.closed=True
