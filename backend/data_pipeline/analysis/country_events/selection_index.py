"""C4离线派生索引；全正文仅临时暂存，封存物只含定位与小汇总。"""
from collections import Counter
from dataclasses import asdict, fields, is_dataclass
from pathlib import Path
import hashlib
import json
import os
import resource
import shutil
import sqlite3
import sys
import time

from data_pipeline.analysis.country_events import event_aggregation as c2, compute
from data_pipeline.analysis.country_events.snapshot_store import file_hash, fsync_tree, database_identity
from data_pipeline.analysis.country_events.snapshot_reader import ComponentReader, ReadBatch, ReadReceipt
from data_pipeline.analysis.country_events.snapshot_access import FixedComponentAccess
from data_pipeline.analysis.country_events.snapshot_schema import TABLES, encode, decode, row_encode
from data_pipeline.analysis.country_events.selection_contract import *
from data_pipeline.analysis.country_events.selection_admission import _register_country_admission, verify_country_admission

TRACKS = ('interrupted_prefix_count','completely_interrupted_prefix_count','invisible_direction_count',
          'affected_asn_count','route_interrupted_asn_count','fixed_visible_ipv4_address_count',
          'fixed_visible_ipv6_slash48_equivalent','new_visible_ipv4_prefix_count','new_visible_ipv6_prefix_count',
          'new_visible_ipv4_address_count','new_visible_ipv6_slash48_equivalent','new_cumulative_ipv4_prefix_count',
          'new_cumulative_ipv6_prefix_count','new_cumulative_ipv4_address_count','new_cumulative_ipv6_slash48_equivalent')
TRACK_ALIASES = {x.replace('_slash48_equivalent','_slash48_count'):x for x in TRACKS}


def stamp(path, sha):
    path=Path(path); st=path.stat()
    return FileEntity(str(path.resolve()),st.st_size,st.st_mtime_ns,st.st_dev,st.st_ino,sha)


def check_entities(entities):
    for e in entities:
        if stamp(e.path,e.sha256)!=e: raise ValueError('C4_file_entity_changed')


def readonly(path):
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)
    db.execute('PRAGMA query_only=ON'); db.execute('PRAGMA cache_size=-4096')
    return db


class Budget:
    def __init__(self,root,limits,*,_sample=False):
        self.root=Path(root); self.limits=limits; self.stats=Counter(); self.started=time.monotonic(); self.extra_roots=lambda:()
        # 仅查询路径启用私有采样；离线构建/inspect的原资源检查频率不变。
        self._interval=0.05 if _sample else 0
        self._sampled_at=None;self._sample_key=None
    def __call__(self,*,force=False):
        roots=[self.root,*[Path(x) for x in self.extra_roots() if x is not None]]
        roots=[r for i,r in enumerate(roots) if not any(r.is_relative_to(x) for x in roots[:i])]
        key=(tuple(roots),tuple(vars(self.limits).items()))
        if (not force and self._sampled_at is not None and key==self._sample_key
                and time.monotonic()-self._sampled_at<self._interval):return
        self._sampled_at=None
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        size=sum(p.stat().st_size for root in roots for p in root.rglob('*') if p.is_file())
        self.stats['process_peak_rss_bytes']=max(rss,self.stats['process_peak_rss_bytes'])
        self.stats['peak_working_bytes']=max(size,self.stats['peak_working_bytes'])
        if rss>self.limits.max_rss_bytes or size>self.limits.max_disk_bytes: raise ValueError('resource_limit:C4_working_set')
        self._sample_key=key;self._sampled_at=time.monotonic()
    def row(self,value):
        size=len(encode(value).encode())
        if size>min(self.limits.max_row_bytes,self.limits.batch_bytes): raise ValueError('resource_limit:C4_row_bytes')
        return size


def strings(value):
    """只枚举有语义引用字段，不按相同ASN/时间猜事件关联。"""
    if is_dataclass(value):
        for f in fields(value):
            v=getattr(value,f.name)
            if 'ref' in f.name or f.name in ('object_id','reference'):
                yield from leaves(v)
            elif is_dataclass(v): yield from strings(v)
    elif isinstance(value,dict):
        for k,v in value.items():
            if 'ref' in str(k) or k in ('object_id','reference'): yield from leaves(v)


def leaves(value):
    if type(value) is str: yield value
    elif isinstance(value,(tuple,list)):
        for v in value: yield from leaves(v)
    elif isinstance(value,dict):
        for v in value.values(): yield from leaves(v)


def sort_key(*values):
    def part(x):
        if type(x) is int:
            # 有界物理时间/ASN/sequence；原revision仍无损TEXT，不参与字典序选择。
            if not -(10**29)<=x<10**29: raise ValueError('C4_sort_integer_range')
            return 'i'+str(x+10**29).zfill(30)
        return 's'+str(x).encode().hex()
    return '/'.join(part(x) for x in values)


SCHEMA = '''
CREATE TABLE rows(seq INTEGER PRIMARY KEY, table_name TEXT NOT NULL, event TEXT, revision TEXT, cohort TEXT,
 sample INTEGER, asn INTEGER, afi INTEGER, metric TEXT, affected INTEGER, downstream INTEGER,
 row_hash TEXT NOT NULL, row_group INTEGER, row_offset INTEGER);
CREATE INDEX source_event ON rows(event,table_name,seq);
CREATE INDEX metric_set ON rows(event,table_name,metric,sample);
CREATE INDEX asn_set ON rows(event,table_name,asn,afi,sample);
CREATE INDEX path_set ON rows(event,table_name,affected,downstream,sample);
CREATE TABLE refs(ref TEXT,seq INTEGER,PRIMARY KEY(ref,seq));
CREATE TABLE event_refs(event TEXT,ref TEXT,PRIMARY KEY(event,ref));
CREATE TABLE views(event TEXT,view TEXT,key TEXT,seq INTEGER,payload TEXT,asn INTEGER,afi INTEGER,
 sample INTEGER,metric TEXT,classification TEXT,affected INTEGER,downstream INTEGER,kind TEXT,
 PRIMARY KEY(event,view,key));
CREATE INDEX view_asn ON views(event,view,asn,afi,key);
CREATE INDEX view_sample ON views(event,view,sample,key);
CREATE INDEX view_track ON views(event,view,metric,key);
CREATE INDEX view_class ON views(event,view,classification,afi,key);
CREATE INDEX view_relation ON views(event,view,affected,downstream,key);
CREATE INDEX view_kind ON views(event,view,kind,key);
CREATE TABLE events(event TEXT PRIMARY KEY,revision TEXT,cohort TEXT,status TEXT);
CREATE TABLE static_asns(asn INTEGER PRIMARY KEY,payload TEXT);
CREATE TABLE totals(event TEXT,view TEXT,total INTEGER,sha256 TEXT,PRIMARY KEY(event,view));
CREATE TABLE aliases(source TEXT,table_name TEXT,ref_kind TEXT,id TEXT,ordinal INTEGER,payload TEXT,
 PRIMARY KEY(source,table_name,ref_kind,id,ordinal));
CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT);
'''


class Builder:
    def __init__(self,binding,reference,runtime,output,limits):
        self.binding,self.reference,self.runtime,self.root,self.limits=binding,reference,runtime,Path(output).resolve(),limits
        self.root.mkdir(parents=True,exist_ok=False); self.guard=Budget(self.root,limits)
        self.db=sqlite3.connect(self.root/'index.sqlite'); self.db.execute('PRAGMA cache_size=-8192'); self.db.execute('PRAGMA temp_store=FILE')
        self.db.executescript(SCHEMA)
        self.db.execute('ATTACH DATABASE ? AS scratch',(str(self.root/'scratch.sqlite'),))
        self.db.executescript('CREATE TABLE scratch.body(seq INTEGER PRIMARY KEY,payload TEXT); CREATE TABLE scratch.reference_rows(row INTEGER PRIMARY KEY,payload TEXT);')
        self.stats=Counter(); self.receipt=None; self.completion=None
        self.db.set_trace_callback(lambda sql: self.stats.update({'sqlite_statements':1}))

    def view(self,event,view,key,seq=None,payload=None,**meta):
        data=[meta.get(k) for k in ('asn','afi','sample','metric','classification','affected','downstream','kind')]
        if payload is not None: self.guard.row(payload)
        self.db.execute('INSERT INTO views VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',(event,view,key,seq,encode(payload) if payload is not None else None,*data))

    def value(self,seq): return decode(self.db.execute('SELECT payload FROM scratch.body WHERE seq=?',(seq,)).fetchone()[0])
    def values(self,event,table):
        for (seq,) in self.db.execute('SELECT seq FROM rows WHERE event=? AND table_name=? ORDER BY seq',(event,table)):
            self.guard(); yield seq,self.value(seq)

    def ingest(self):
        limits={k:getattr(self.limits,k) for k in ('batch_rows','batch_bytes','max_row_bytes','max_rows','max_rss_bytes','max_disk_bytes')}
        reader=ComponentReader(self.runtime.component_dsn,self.binding,c1=self.runtime.c1,guard=self.guard,**limits)
        self.guard.extra_roots=lambda:(self.binding.root,reader.workdir)
        self.manifest=reader.manifest; self.stats['c3_stream_calls']+=1; sequence=0
        stream=reader.stream()
        try:
            for part in stream:
                if isinstance(part,ReadReceipt): self.receipt=part; continue
                if not isinstance(part,ReadBatch) or self.receipt is not None: raise ValueError('C4_stream_protocol')
                for item in part.rows:
                    self.guard(); self.guard.row(item.value if isinstance(item,c2.C2Row) else item)
                    table,flat=row_encode(sequence,item)
                    value=item.value if isinstance(item,c2.C2Row) else item
                    event=item.incident_id if isinstance(item,c2.C2Row) else None
                    revision=item.revision if isinstance(item,c2.C2Row) else None
                    args=[getattr(value,k,None) for k in ('cohort_id','sample_us','asn','afi','metric','affected_asn','downstream_asn')]
                    self.db.execute('INSERT INTO rows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                    (sequence,table,event,str(revision) if revision is not None else None,*args,flat['_row_hash'],None,None))
                    self.db.execute('INSERT INTO scratch.body VALUES (?,?)',(sequence,encode(value)))
                    for ref in set(strings(value)):
                        self.db.execute('INSERT OR IGNORE INTO refs VALUES (?,?)',(ref,sequence))
                        if event: self.db.execute('INSERT OR IGNORE INTO event_refs VALUES (?,?)',(event,ref))
                    if isinstance(value,c2.EventStatus):
                        if value.incident.revision!=revision: raise ValueError('C4_selected_revision')
                        self.db.execute('INSERT INTO events VALUES (?,?,?,?)',(event,str(revision),value.cohort_id,encode(value)))
                    if isinstance(value,c2.C2Completion): self.completion=value
                    sequence+=1
                self.db.commit()
        finally: stream.close()
        if self.receipt is None or not self.receipt.full_body_validated or self.receipt.rows!=sequence: raise ValueError('C4_missing_full_receipt')
        self.guard.extra_roots=lambda:(self.binding.root,)
        self.stats['source_rows']=sequence
        if self.completion.input_kind=='saved_C1' and self.reference is None: raise ValueError('C4_reference_required')

    def locators(self):
        import pyarrow.parquet as pq
        self.stats['locator_passes']+=1
        for table in TABLES:
            with (Path(self.binding.root)/'data'/(table+'.parquet')).open('rb') as f:
                pf=pq.ParquetFile(f)
                for g in range(pf.num_row_groups):
                    self.guard()
                    batch=pf.read_row_group(g,columns=['_sequence','_row_hash'])
                    self.stats['locator_row_groups']+=1; self.stats['locator_bytes']+=batch.nbytes
                    for offset,row in enumerate(batch.to_pylist()):
                        cur=self.db.execute('UPDATE rows SET row_group=?,row_offset=? WHERE seq=? AND table_name=? AND row_hash=? AND row_group IS NULL',
                                            (g,offset,row['_sequence'],table,row['_row_hash']))
                        if cur.rowcount!=1: raise ValueError('C4_locator_crosscheck')
        if self.db.execute('SELECT count(*) FROM rows WHERE row_group IS NULL').fetchone()[0]: raise ValueError('C4_missing_locator')
        self.db.commit()

    def project(self):
        for seq,table in self.db.execute('SELECT seq,table_name FROM rows WHERE event IS NULL ORDER BY seq'):
            self.view('', 'audit', sort_key(list(TABLES).index(table),seq),seq,kind=table)
        for event,revision,cohort,status in self.db.execute('SELECT * FROM events ORDER BY event'):
            self.guard(); head=decode(status)
            # 所有原事件行均可审计，辅以明确引用指向的全局证据。
            seqs=self.db.execute('SELECT seq FROM rows WHERE event=? UNION SELECT r.seq FROM refs r JOIN event_refs e USING(ref) JOIN rows s ON s.seq=r.seq WHERE e.event=? AND s.event IS NULL ORDER BY seq',(event,event))
            for (seq,) in seqs:
                table=self.db.execute('SELECT table_name FROM rows WHERE seq=?',(seq,)).fetchone()[0]
                self.view(event,'audit',sort_key(list(TABLES).index(table),seq),seq,kind=table)
            samples=[]
            for seq,v in self.values(event,'metric_point'):
                if v.metric in TRACKS:
                    self.view(event,'series15',sort_key(TRACKS.index(v.metric),v.sample_us,seq),seq,sample=v.sample_us,metric=v.metric)
                    if v.metric==TRACKS[0]: samples.append(v.sample_us)
            samples.sort()
            grid=decode(self.manifest['parameters']).get('grid')
            if grid and cohort:
                low=(head.incident.onset or head.incident.detected_at).lower_us
                high=head.incident.end.lower_us if head.incident.end else grid['input_end_us']
                expected=[t for t in grid['samples_us'] if low<t<=high]
                if samples!=expected: raise ValueError('C4_grid_coverage')
            for metric in TRACKS:
                rows=list(self.db.execute('SELECT sample,count(*) FROM rows WHERE event=? AND table_name=? AND metric=? GROUP BY sample ORDER BY sample',(event,'metric_point',metric)))
                if rows!=[(t,1) for t in samples]: raise ValueError('C4_series_coverage')
            members=set()
            for seq,v in self.values(event,'cohort_member'):
                if v.route.origin is not None:
                    members.add((v.route.origin,0)); members.add((v.route.origin,v.route.endpoint.afi))
            # C2可能包含事件上下文中新增origin，实际矩阵成员一并保留。
            for member in self.db.execute('SELECT DISTINCT asn,afi FROM rows WHERE event=? AND table_name=?',(event,'asn_point')):
                self.guard(); members.add(member)
            for asn,afi in sorted(members):
                self.guard()
                points=list(self.db.execute('SELECT sample,count(*) FROM rows WHERE event=? AND table_name=? AND asn=? AND afi=? GROUP BY sample ORDER BY sample',(event,'asn_point',asn,afi)))
                if points!=[(t,1) for t in samples]: raise ValueError('C4_matrix_coverage')
                windows=[(s,self.value(s)) for s, in self.db.execute("SELECT seq FROM rows WHERE event=? AND table_name='window_class' AND asn=? AND afi=? ORDER BY seq",(event,asn,afi))]
                if len(windows)>1: raise ValueError('C4_window_duplicate')
                window=windows[0][1] if windows else None
                classification=window.classification if window else 'unknown'
                payload=dict(asn=asn,afi=afi,classification=classification,window=window,
                    window_source_sequence=windows[0][0] if windows else None,
                    view_reason=None if window else ('all_unknown' if samples else 'no_samples'),sample_count=len(samples))
                self.view(event,'asns',sort_key(asn,afi),payload=payload,asn=asn,afi=afi,classification=classification)
                self.view(event,'asn_window',sort_key(asn,afi),payload=payload,asn=asn,afi=afi)
                self.db.execute('INSERT OR IGNORE INTO static_asns VALUES (?,NULL)',(asn,))
            for seq,v in self.values(event,'asn_point'):
                self.view(event,'asn_matrix',sort_key(v.sample_us,v.asn,v.afi,seq),seq,asn=v.asn,afi=v.afi,sample=v.sample_us)
            for seq,v in self.values(event,'asn_metric_peak'):
                self.view(event,'asn_peaks',sort_key(v.asn,v.afi,v.metric,seq),seq,asn=v.asn,afi=v.afi,metric=v.metric)
            affected={asn for asn, in self.db.execute("SELECT asn FROM views WHERE event=? AND view='asns' AND afi=0 AND classification IN ('affected','route_interrupted')",(event,))}
            for seq,v in self.values(event,'path_summary'):
                if v.affected_asn in affected:
                    self.view(event,'paths',sort_key(v.affected_asn,v.downstream_asn,seq),seq,affected=v.affected_asn,downstream=v.downstream_asn)
            for seq,v in self.values(event,'path_sample'):
                self.view(event,'path_samples',sort_key(v.affected_asn,v.downstream_asn,v.path_ref,v.observation_ref,v.affected_position,v.downstream_position,v.sample_us,seq),seq,affected=v.affected_asn,downstream=v.downstream_asn,sample=v.sample_us)
                for asn in (v.affected_asn,v.downstream_asn): self.db.execute('INSERT OR IGNORE INTO static_asns VALUES (?,NULL)',(asn,))
            tracks={}
            for metric in TRACKS:
                last=self.db.execute('SELECT seq FROM rows WHERE event=? AND table_name=? AND metric=? ORDER BY sample DESC LIMIT 1',(event,'metric_point',metric)).fetchone()
                final=self.value(last[0]) if last else None
                known=None
                for seq,v in self.values(event,'metric_point'):
                    if v.metric==metric and v.value is not None and (known is None or v.sample_us>known.sample_us): known=v
                peak=next((v for _,v in self.values(event,'peak') if v.metric==metric),None)
                tracks[metric]=dict(final_sample=final,last_known=known,known_peak=peak,exact_peak=peak.value if peak and peak.unknown_slots==0 else None)
            completions=[v for _,v in self.values(event,'completion')]
            if len(completions)!=(1 if cohort else 0): raise ValueError('C4_completion_grid')
            completion=completions[0] if completions else None
            if completion is not None:
                window=(samples[0],samples[-1]) if samples else None
                input_start=grid['input_start_us'] if grid else self.completion.scope[0]
                through=samples[-1] if samples else input_start
                if (completion.sample_count,completion.sample_window,completion.data_through_us)!=(len(samples),window,through):
                    raise ValueError('C4_completion_grid')
            counts=dict(self.db.execute("SELECT classification,count(*) FROM views WHERE event=? AND view='asns' AND afi=0 GROUP BY classification",(event,)))
            self.view(event,'overview',sort_key(0),payload=dict(event_status=head,completion=completion,tracks=tracks,
                asn_window_counts=counts if cohort else None,path_count=self.db.execute("SELECT count(*) FROM views WHERE event=? AND view='paths'",(event,)).fetchone()[0] if cohort else None,
                sample_count=len(samples) if cohort else None,view_reason='missing_baseline' if not cohort else 'no_samples' if not samples else None))
        self.db.commit()

    def static(self):
        from data_pipeline.analysis.detection.reference_view import ReferenceView, ReferenceSource
        if self.reference is None:
            for asn, in self.db.execute('SELECT asn FROM static_asns'):
                self.db.execute('UPDATE static_asns SET payload=? WHERE asn=?',(encode(dict(asn=asn,fixture_only=True,as_name={'value':None,'state':'not_bound_fixture'},organization={'value':None,'state':'not_bound_fixture'},nature={'value':None,'state':'unknown','reason':'no_confirmed_field'},historical_applicability='Unknown')),asn))
            return
        r=self.reference; c1=self.runtime.c1
        if c1 is None: raise ValueError('C4_reference_runtime_required')
        reader=c1.reader; refs=c1.identity['reference_sources']
        source=refs[r.role]
        expected=ReferenceBinding(**database_identity(reader.dsn),run_id=reader.run_id,snapshot=reader.snapshot,
            manifest_sha256=digest(reader.manifest),source_id=source['source_id'],content_sha256=source['source_id'],expected_rows=source['rows'])
        if r!=expected: raise ValueError('C4_reference_binding_mismatch')
        def rows():
            for batch in reader.reference_batches(r.source_id):
                for row in batch.to_pylist():
                    self.guard(); self.guard.row(row)
                    self.db.execute('INSERT INTO scratch.reference_rows VALUES (?,?)',(row['row'],encode(row)))
                    self.stats['static_reference_rows']+=1
                    yield row
        view=ReferenceView(c1.identity['reference_version'],f'{r.run_id}:{r.snapshot}',self.root/'reference-work',guard=self.guard)
        view.add(ReferenceSource(r.role,r.source_id,r.expected_rows,rows()))
        header=next((decode(payload) for payload, in self.db.execute('SELECT payload FROM scratch.reference_rows ORDER BY row') if decode(payload).get('csv_record_kind') not in ('blank_line','whitespace_line')),None)
        columns=json.loads(header['raw_row']) if header else []
        for asn, in self.db.execute('SELECT asn FROM static_asns ORDER BY asn'):
            self.guard(); key=str(asn); record=view.mapping('as_info').get(key)
            location=view.selection('as_info',key) if record is not None else None
            raw=decode(self.db.execute('SELECT payload FROM scratch.reference_rows WHERE row=?',(location['row'],)).fetchone()[0]) if location else None
            def field(name):
                if record is None:return dict(value=None,state='record_not_found',field=name)
                if name not in record:return dict(value=None,state='field_absent',field=name)
                value=record[name]
                return dict(value=value,state='empty' if value=='' else 'available',field=name,column=columns.index(name) if name in columns else None)
            out=dict(asn=asn,as_name=field('as_name'),organization=field('org_name'),nature=dict(value=None,state='unknown',reason='no_confirmed_field'),
                locator=location,raw_row=raw,row_sha256=hashlib.sha256(encode(raw).encode()).hexdigest() if raw else None,historical_applicability='Unknown')
            self.guard.row(out); self.db.execute('UPDATE static_asns SET payload=? WHERE asn=?',(encode(out),asn))
        del view; shutil.rmtree(self.root/'reference-work'); self.db.commit()

    def aliases(self):
        if (self.runtime.aliases_typed is None)!=(self.runtime.aliases_source_json is None): raise ValueError('C4_alias_source_required')
        self.db.execute('INSERT INTO meta VALUES (?,?)',('aliases_source',self.runtime.aliases_source_json or 'null'))
        if self.runtime.aliases_typed:
            source=json.loads(self.runtime.aliases_source_json)
            if canonical(source)!=self.runtime.aliases_source_json: raise ValueError('C4_alias_source_encoding')
            for n,row in enumerate(decode(self.runtime.aliases_typed)):
                self.guard.row(row)
                for field in ('source','table','ref_kind','id','incident_id','revision','cohort_id'):
                    if field not in row: raise ValueError('C4_alias_field')
                target=self.db.execute('SELECT revision,cohort FROM events WHERE event=?',(row['incident_id'],)).fetchone()
                if target!=(str(row['revision']),row['cohort_id']): raise ValueError('C4_alias_target')
                self.db.execute('INSERT INTO aliases VALUES (?,?,?,?,?,?)',(row['source'],row['table'],row['ref_kind'],row['id'],n,encode(row)))

    def verify(self):
        self.db.commit(); self.stats['readback_passes']+=1
        access=FixedComponentAccess(self.runtime.component_dsn,self.binding,self.manifest,limits=self.limits,guard=self.guard)
        audit=hashlib.sha256()
        try:
            cur=self.db.execute('SELECT table_name,row_group,row_offset,seq,row_hash FROM rows ORDER BY seq')
            while locs:=cur.fetchmany(min(self.limits.batch_rows,self.limits.max_row_groups)):
                actual=access.read(locs)
                for table,group,offset,seq,sha in locs:
                    item=actual[seq]; value=item.value if isinstance(item,c2.C2Row) else item
                    if encode(value)!=encode(self.value(seq)): raise ValueError('C4_independent_readback')
                    audit.update(bytes.fromhex(sha)); self.stats['readback_rows']+=1
            # 不生成fixture P：离线按全部业务定位页回读，再对临时原流逐值核对。
            cur=self.db.execute('SELECT event,view,key,seq,payload FROM views ORDER BY event,view,key')
            last=None
            while page:=cur.fetchmany(min(self.limits.batch_rows,self.limits.max_row_groups)):
                self.guard(); self.stats['view_readback_pages']+=1
                locs=[self.db.execute('SELECT table_name,row_group,row_offset,seq,row_hash FROM rows WHERE seq=?',(seq,)).fetchone() for _,_,_,seq,_ in page if seq is not None]
                actual=access.read(locs)
                for event,view,key,seq,payload in page:
                    current=(event,view,key)
                    if last is not None and current<=last: raise ValueError('C4_view_order')
                    last=current
                    if seq is not None:
                        item=actual[seq]; value=item.value if isinstance(item,c2.C2Row) else item
                        if encode(value)!=encode(self.value(seq)): raise ValueError('C4_view_readback_mismatch')
                    elif payload is None: raise ValueError('C4_empty_view_item')
                    else:
                        value=decode(payload); self.guard.row(value)
                        if view in ('asns','asn_window') and value['window_source_sequence'] is not None:
                            if value['window']!=self.value(value['window_source_sequence']): raise ValueError('C4_window_projection')
                    self.stats['view_readback_rows']+=1
        finally:
            self.stats.update({'readback_'+k:v for k,v in access.stats.items()}); access.close()
        for event, in self.db.execute('SELECT event FROM events'):
            for view in VIEWS:
                d=hashlib.sha256(); count=0
                for key,seq,payload in self.db.execute('SELECT key,seq,payload FROM views WHERE event=? AND view=? ORDER BY key',(event,view)):
                    d.update(encode((key,seq,payload)).encode()); count+=1
                self.db.execute('INSERT INTO totals VALUES (?,?,?,?)',(event,view,count,d.hexdigest()))
        self.validation=dict(source_rows=self.stats['source_rows'],readback_rows=self.stats['readback_rows'],source_digest=audit.hexdigest(),
            view_rows=self.db.execute('SELECT count(*) FROM views').fetchone()[0],locator_rows=self.db.execute('SELECT count(*) FROM rows').fetchone()[0])

    def seal(self):
        self.ingest(); self.locators(); self.project(); self.static(); self.aliases(); self.verify()
        self.db.commit(); self.db.execute('DETACH DATABASE scratch'); (self.root/'scratch.sqlite').unlink()
        if self.db.execute('PRAGMA integrity_check').fetchone()!=('ok',): raise ValueError('C4_index_integrity')
        self.db.close(); self.db=None
        self.guard(); self.stats.update(self.guard.stats); self.stats['wall_seconds']=str(time.monotonic()-self.guard.started)
        self.stats['index_bytes']=(self.root/'index.sqlite').stat().st_size
        sha=file_hash(self.root/'index.sqlite',self.guard)
        rid=result_id(self.binding)
        identity=dict(component=asdict(self.binding),reference=asdict(self.reference) if self.reference else None,
            query_version=VERSION,index_sha256=sha,validation=self.validation,
            code={p.name:file_hash(p) for p in sorted([*(Path(__file__).parent / name for name in ('selection_reader.py', 'selection_admission.py', 'selection_contract.py', 'selection_index.py')),Path(__file__).parent/'snapshot_access.py'])})
        model='country_read_'+digest(identity)
        proof=CountryAdmissionProof(rid,model,self.binding.manifest_sha256,
            encode(asdict(self.receipt)),digest(self.validation),self.validation['view_rows'],self.stats['source_rows'],
            self.stats['c3_stream_calls'],self.stats['locator_passes'],self.stats['readback_rows'])
        entities=tuple(stamp(Path(self.binding.root)/f['path'],f['sha256']) for f in self.manifest['files'])
        entities+=(stamp(Path(self.binding.root)/'manifest.json',self.binding.manifest_sha256),
            stamp(Path(self.binding.root)/'ready.json',file_hash(Path(self.binding.root)/'ready.json')),stamp(self.root/'index.sqlite',sha))
        manifest=dict(identity=identity,proof=contract_json(proof),entities=[asdict(e) for e in entities],costs=dict(self.stats),
            upstream_typed=self.manifest['upstream'],parameters_typed=self.manifest['parameters'],completion_typed=encode(self.completion),
            tables=self.manifest['tables'],event_index=self.manifest['event_index'])
        path=self.root/'manifest.json'; path.write_text(canonical(manifest))
        fsync_tree(self.root)
        binding=CountryReadBinding(self.binding,rid,model,str(self.root),file_hash(path),self.reference)
        # 来源终检与P发布分开；正式构建仍复用原C1严格检查，查询永不调用。
        if self.runtime.c1 is not None: self.runtime.c1._check()
        check_entities(entities)
        def finalize():
            (self.root/'ready.json').write_text(contract_json(binding)); fsync_tree(self.root)
            check_entities(entities)
        _register_country_admission(self.runtime.component_dsn,binding,proof,before_commit=finalize)
        return binding,proof


def prepare_country_index(c3_binding, *, reference_binding, runtime, output, limits=QueryLimits()):
    builder=Builder(c3_binding,reference_binding,runtime,output,limits)
    try: return builder.seal()
    except BaseException:
        ready=Path(output)/'ready.json'
        if ready.exists(): ready.unlink()
        raise
    finally:
        if builder.db is not None: builder.db.close()


def inspect_country(read_binding, admission_proof, *, runtime, limits=QueryLimits()):
    b=read_binding; root=Path(b.root)
    with verify_country_admission(runtime.component_dsn,b,admission_proof): pass
    guard=Budget(root,limits); guard.extra_roots=lambda:(b.component.root,); guard()
    if file_hash(root/'manifest.json')!=b.manifest_sha256: raise ValueError('C4_manifest_digest')
    if contract_value((root/'ready.json').read_text())!=b: raise ValueError('C4_not_ready')
    m=json.loads((root/'manifest.json').read_text())
    if (result_id(b.component), 'country_read_'+digest(m['identity']))!=(b.result_id,b.read_model_id): raise ValueError('C4_identity')
    if m['identity']['component']!=asdict(b.component) or m['identity']['reference']!=(asdict(b.reference) if b.reference else None): raise ValueError('C4_dependency_binding')
    if contract_value(m['proof'])!=admission_proof or admission_proof.result_id!=b.result_id or admission_proof.read_model_id!=b.read_model_id: raise ValueError('C4_admission_proof')
    if admission_proof.stream_calls!=1 or admission_proof.locator_passes!=1 or admission_proof.source_rows!=admission_proof.readback_rows: raise ValueError('C4_incomplete_audit')
    if digest(m['identity']['validation'])!=admission_proof.index_validation_sha256: raise ValueError('C4_validation_digest')
    receipt=decode(admission_proof.c3_receipt_typed)
    if receipt.get('binding')!=asdict(b.component) or receipt.get('mode')!='component' or receipt.get('full_body_validated') is not True or receipt.get('rows')!=admission_proof.source_rows:
        raise ValueError('C4_original_read_receipt')
    entities=tuple(FileEntity(**e) for e in m['entities']); check_entities(entities)
    if file_hash(root/'index.sqlite',guard)!=m['identity']['index_sha256']: raise ValueError('C4_index_digest')
    db=readonly(root/'index.sqlite')
    try:
        if db.execute('PRAGMA integrity_check').fetchone()!=('ok',): raise ValueError('C4_index_integrity')
        if db.execute('SELECT count(*) FROM rows').fetchone()[0]!=admission_proof.source_rows: raise ValueError('C4_index_count')
        if db.execute('SELECT count(*) FROM rows WHERE row_group IS NULL OR row_offset IS NULL').fetchone()[0]: raise ValueError('C4_index_locator')
        if db.execute('SELECT count(*) FROM views').fetchone()[0]!=admission_proof.index_rows: raise ValueError('C4_view_count')
    finally: db.close()
    descriptor=CountryAdmissionDescriptor(b,admission_proof,m['upstream_typed'],m['parameters_typed'],m['completion_typed'],
        entities+(stamp(root/'manifest.json',b.manifest_sha256),stamp(root/'ready.json',file_hash(root/'ready.json'))),canonical(m['tables']),canonical(m['event_index']))
    if runtime.verify_qualification is None: raise ValueError('C4_qualification_verifier_required')
    with runtime.verify_qualification(descriptor):
        with verify_country_admission(runtime.component_dsn,b,admission_proof): check_entities(descriptor.entities)
    return descriptor


def read_country_event(descriptor, *, incident_id, revision=None):
    """发布owner在自身资格上下文内调用；只读固定事件，无新最高版本发现。"""
    check_entities(descriptor.entities)
    db=readonly(Path(descriptor.read_binding.root)/'index.sqlite')
    try:
        row=db.execute('SELECT revision,status FROM events WHERE event=?',(incident_id,)).fetchone()
        if row is None: raise ValueError('C4_unknown_incident')
        if revision is not None and (type(revision) is not int or revision!=int(row[0])): raise ValueError('C4_revision_mismatch')
        result=decode(row[1])
        if result.incident.incident_id!=incident_id or result.incident.revision!=int(row[0]): raise ValueError('C4_event_identity')
        check_entities(descriptor.entities); return result
    finally: db.close()


def counted_progress(stats, guard, step=1000):
    """SQL步数仅计量；保留即时取消/真实资源异常的原始异常。"""
    errors=[]
    def progress():
        stats['index_steps']+=step
        try:guard()
        except BaseException as error:
            errors.append(error);return 1
        return 0
    return progress,errors


def iter_country_aliases(descriptor, *, filters_json='{}', limits=QueryLimits()):
    """完整候选流；源缺席不同于完整查询无命中；早停无尾回执。"""
    filters=json.loads(filters_json)
    if type(filters) is not dict or canonical(filters)!=filters_json or set(filters)-{'source','table','ref_kind','id'} or any(type(v) is not str for v in filters.values()): raise ValueError('C4_alias_filters')
    check_entities(descriptor.entities); db=readonly(Path(descriptor.read_binding.root)/'index.sqlite')
    budget=Budget(descriptor.read_binding.root,limits,_sample=True)
    progress,errors=counted_progress(budget.stats,budget)
    db.set_progress_handler(progress,1000)
    try:
        budget(force=True)
        source=db.execute("SELECT value FROM meta WHERE key='aliases_source'").fetchone()[0]
        if source=='null': raise ValueError('C4_alias_not_available')
        where=' AND '.join(('table_name' if k=='table' else k)+'=?' for k in filters) or '1'
        count=0; sha=hashlib.sha256()
        for (payload,) in db.execute('SELECT payload FROM aliases WHERE '+where+' ORDER BY source,table_name,ref_kind,id,ordinal',tuple(filters.values())):
            if len(payload.encode())>limits.max_row_bytes: raise ValueError('resource_limit:C4_alias')
            budget();check_entities(descriptor.entities); value=decode(payload); sha.update(payload.encode()); count+=1; yield value
        check_entities(descriptor.entities);budget(force=True)
        yield QueryReadReceipt(canonical({'view':'resolve_alias','filters':filters,'source':json.loads(source)}),count,sha.hexdigest())
    except sqlite3.OperationalError as error:
        if errors:raise errors[0] from error
        if 'interrupt' in str(error): raise ValueError('query_budget_exceeded') from error
        raise
    finally: db.close()


def iter_country_events(descriptor, *, limits=QueryLimits()):
    """仅E个已封存EventStatus；统一prepare用于完整profile门禁，无C3正文扫描。"""
    check_entities(descriptor.entities); db=readonly(Path(descriptor.read_binding.root)/'index.sqlite')
    count=0; sha=hashlib.sha256()
    budget=Budget(descriptor.read_binding.root,limits,_sample=True)
    progress,errors=counted_progress(budget.stats,budget);db.set_progress_handler(progress,1000)
    try:
        budget(force=True)
        for event,revision,status in db.execute('SELECT event,revision,status FROM events ORDER BY event'):
            if len(status.encode())>limits.max_row_bytes: raise ValueError('resource_limit:C4_events')
            budget();value=decode(status)
            if value.incident.incident_id!=event or value.incident.revision!=int(revision): raise ValueError('C4_event_identity')
            sha.update(status.encode()); count+=1; check_entities(descriptor.entities); yield value
        if count!=json.loads(descriptor.event_index_json)['rows']: raise ValueError('C4_event_count')
        check_entities(descriptor.entities);budget(force=True)
        yield QueryReadReceipt(canonical({'view':'events','read_model_id':descriptor.read_binding.read_model_id}),count,sha.hexdigest())
    except sqlite3.OperationalError as error:
        if errors:raise errors[0] from error
        raise
    finally: db.close()
