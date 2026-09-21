"""固定Collection到有限H2列；原件和引用全量复验只在离线投影进行。"""
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from data_pipeline.overview.paths import read_comparison

from data_pipeline.bgp.snapshots import origin as origin, path_comparison as comparison
from data_pipeline.history.event_index.exact import loads, native
from data_pipeline.history.event_index.project import read_span
from data_pipeline.history.event_index.cleanup import owner
from data_pipeline.history.event_collection.freeze import safe_path
from data_pipeline.history.event_collection.children import directory_bytes
from data_pipeline.history.database_import.freeze import read_json
from data_pipeline.history.rib_index.model import DEFINITIONS, ORDER, row_bytes, code_identity, freeze_identity, valid_freeze_identity
from data_pipeline.history.rib_index.mrt import frames, require


class Projector:
    def __init__(self,h,token,out,budget):
        self.h,self.token,self.out,self.budget=h,token,out,budget
        self.source=h.data_root/token.collection_id/'source'
        self.db=sqlite3.connect(out/'projection.sqlite');self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA journal_mode=OFF');self.db.execute('PRAGMA cache_size=-2048');self.db.execute('PRAGMA temp_store=FILE')
        for name,columns in DEFINITIONS.items():
            self.db.execute('CREATE TABLE '+name+' ('+','.join('"'+c+'" '+{'i':'INTEGER','s':'TEXT','x':'BLOB'}[t] for c,t in columns)+')')
        self.db.execute('CREATE INDEX mrt_location ON mrt_observations(file_id,record,entry_index)')
        self.db.execute('CREATE INDEX mrt_prefix ON mrt_observations(file_id,afi,prefix,record,entry_index)')
        self.db.execute('CREATE INDEX frame_location ON mrt_frames(file_id,record)')
        self.db.execute('CREATE TABLE edges(parent_file INTEGER,role TEXT,reference TEXT,target_file INTEGER)')
        self.files={};self.mrt={};self.compared=set()
    def close(self):self.db.close()
    def disk(self):
        self.db.commit(); size=self.db.execute('PRAGMA page_count').fetchone()[0]*self.db.execute('PRAGMA page_size').fetchone()[0]
        used=directory_bytes(self.out,self.budget)
        require(used<=self.budget.collection_limits.temporary_bytes,'暂存磁盘超限')
        self.budget.counts['projection_spool_bytes']=size
        self.budget.counts['temporary_bytes']=used
        self.budget.counts['profile_candidate_disk_peak_bytes']=max(used,self.budget.counts.get('profile_candidate_disk_peak_bytes',0));self.budget.check()
    def add(self,table,row):
        row={c:row.get(c) for c,_ in DEFINITIONS[table]}
        for c,t in DEFINITIONS[table]:
            require(row[c] is None or type(row[c]) is {'i':int,'s':str,'x':bytes}[t], '领域原类型不符: '+table+'.'+c)
        data=row_bytes(row)
        require(len(data)<=self.budget.limits.max_row_bytes,'领域行超限')
        self.budget.add('profile_rows',1,self.budget.limits.max_total_rows);self.budget.add('profile_bytes',len(data),self.budget.limits.max_total_bytes)
        self.db.execute('INSERT INTO '+table+' VALUES ('+','.join('?' for _ in row)+')',list(row.values()))
        if self.budget.counts['profile_rows']%self.budget.limits.batch_rows==0:self.disk()
    def rows(self,name):
        for row in self.db.execute('SELECT * FROM '+name+' ORDER BY '+ORDER[name]):
            self.budget.check();yield dict(row)
    def doc(self,fid):
        result=self.db.execute('SELECT * FROM documents WHERE file_id=?',(fid,)).fetchall()
        require(len(result)==1,'文档必须唯一');return dict(result[0])
    def value(self,doc):
        raw=read_span(self.source,doc['entity_path'],doc['byte_start'],doc['byte_end'],self.budget,self.budget.collection_limits.document_bytes)
        # 精确树先拒绝使用路径上的重键；数字解释保留原节点/原span，不依赖float作全值oracle。
        return native(loads(raw))
    def json(self,fid):return self.value(self.doc(fid))
    def target(self,fid,role,reference=None):
        q='SELECT target_file FROM edges WHERE parent_file=? AND role=?';params=[fid,role]
        if reference is not None:q+=' AND reference=?';params.append(reference)
        result=self.db.execute(q,params).fetchall();require(len(result)==1,'必需依赖不唯一/缺失');return result[0][0]
    def path(self,fid,decoded=False):
        f=self.files[fid];return safe_path(self.source/f['decoded_path' if decoded else 'raw_path'],(self.source,))
    def prepare(self):
        manifest=read_json(self.h.data_root/self.token.collection_id/'source-manifest.json',self.budget.collection_limits.metadata_bytes)
        require(valid_freeze_identity(manifest.get('h2_freezer_code')),'冻结H2适配器身份不符')
        with self.h.collection(self.token) as session:
            for name in ('files','documents','edges'):
                with owner(session.bulk(name)) as stream:
                    for batch in stream:
                        for r in batch['rows']:
                            self.budget.add('projection_input_rows',1,self.budget.limits.max_total_rows)
                            if name=='files':self.files[r['file_id']]=r
                            elif name=='documents':
                                f=self.files[r['file_id']];self.add('documents',{**r,'role':f['role'],'root_id':f['root_id']})
                            else:
                                require(r['resolution']=='resolved' and r['target_file'] is not None,'H2必需引用未闭合')
                                self.db.execute('INSERT INTO edges VALUES (?,?,?,?)',[r[k] for k in ('parent_file','role','reference','target_file')])
                        self.disk()
        require(session.receipt is not None,'集合输入未耗尽');self.input_resources=session.receipt['resources']
        for fid,f in self.files.items():
            if f['role']=='mrt-gzip':self.parse_mrt(fid)
        for fid,f in self.files.items():
            if f['role'] in ('rib-root','comparison-manifest'):
                value=self.json(fid);schema=value['schema_version']
                if schema=='core-rib-origin-manifest/v1':self.origins(fid,value)
                elif schema=='core-rib-scale-manifest/v1':self.scale(fid,value)
                elif schema=='rib-path-comparison-manifest/v1':self.compare(fid,value)
                elif schema!='core-rib-path-package/v1':raise ValueError('H2未知根')
        for fid,f in self.files.items():
            if f['role']=='rib-root' and self.json(fid)['schema_version']=='core-rib-path-package/v1':self.package(fid,self.json(fid))
        self.disk()
    def parse_mrt(self,fid):
        epochs=set();peer_values=None
        for frame,peers,entries in frames(self.path(fid,True),self.budget):
            self.add('mrt_frames',{'file_id':fid,'entity_path':self.files[fid]['decoded_path'],**frame});epochs.add(frame['epoch'])
            if peers is not None:
                peer_values=peers
                for peer in peers:self.add('peers',{'file_id':fid,'peer_ordinal':peer['index'],**peer})
            for entry in entries:self.add('mrt_observations',{'file_id':fid,**frame,**entry})
        require(len(epochs)==1,'MRT时点不唯一');self.mrt[fid]={'epoch':epochs.pop(),'peers':peer_values}
    def verify_source(self,source,fid,observed_at,profile):
        f=self.files[fid];epoch=self.mrt[fid]['epoch']
        require(source['sha256']==f['raw_sha'] and source['compressed_bytes']==f['raw_bytes'],'MRT源身份/大小不符')
        require(source.get('collector_id','rrc25')=='rrc25' and source.get('coverage','unknown')=='unknown','Collector范围不符')
        stamp=datetime.fromtimestamp(epoch,timezone.utc)
        require(observed_at==stamp.isoformat().replace('+00:00','Z'),'声明时点不符')
        require(datetime.fromisoformat(profile['window_start'])<=stamp<datetime.fromisoformat(profile['window_end_exclusive']) and stamp<=datetime.fromisoformat(profile['snapshot_time']),'MRT不在数据档')
        require(profile==json.loads((Path(__file__).resolve().parents[4]/'config/data-profile.json').read_text()),'原数据档非当前固定配置')
    def sqlrows(self,fid,table):
        # 原SQLite只读；原storage/BLOB完整child还可独立读取，不从新投影反造原值。
        with closing(sqlite3.connect(self.path(fid).as_uri()+'?mode=ro',uri=True)) as db:
            db.row_factory=sqlite3.Row
            require(db.execute('PRAGMA integrity_check').fetchone()[0]=='ok','SQLite损坏')
            for row in db.execute('SELECT * FROM "'+table+'"'):
                self.budget.check();yield dict(row)
    def origins(self,fid,manifest):
        sid=self.target(fid,'origin-summary');summary=self.json(sid);mid=self.target(fid,'mrt-gzip')
        require(summary['schema_version']=='core-rib-origin/v1' and summary['interpretation_version']==origin.RULE and summary['gzip_eof'] is True,'起源伪合格/规则')
        self.verify_source(summary['source'],mid,summary['observed_at'],summary['data_profile'])
        rules=summary['rules'];require(rules['skip_ranges_inclusive']==[list(v) for v in origin.SKIP_RANGES] and rules['stop_special_asns']==list(origin.STOP_SPECIAL_ASNS) and rules['reserved_legacy_exclusion']==[65535],'私用归属规则冲突')
        peers_id=self.target(fid,'origin-peers');require(self.json(peers_id)==self.mrt[mid]['peers'],'原Peer属性不符')
        for p in self.mrt[mid]['peers']:self.add('peers',{'file_id':peers_id,'peer_ordinal':p['index'],**p})
        paths_id=self.target(fid,'rib-sqlite');count=0
        # 旧parse_rib拒绝单快照不支持的重复prefix/peer/addpath，独立核对原路径目录所有列。
        scratch=self.out/('origin-check-'+str(fid)+'.sqlite')
        with closing(sqlite3.connect(scratch)) as db, self.path(mid,True).open('rb') as stream:
            budget=self.budget;candidate=self.out
            class CheckedStream:
                def read(self, size):
                    budget.check()
                    data=stream.read(size);budget.add('origin_validation_bytes',len(data),budget.limits.max_total_bytes)
                    used=directory_bytes(candidate,budget)
                    budget.counts['origin_validation_spool_peak_bytes']=max(scratch.stat().st_size,budget.counts.get('origin_validation_spool_peak_bytes',0))
                    budget.counts['profile_candidate_disk_peak_bytes']=max(used,budget.counts.get('profile_candidate_disk_peak_bytes',0))
                    require(used<=budget.collection_limits.temporary_bytes,'起源验证暂存超限')
                    return data
            epoch,prefixes,peer_counts,peers,records,decoded=origin.parse_rib(CheckedStream(),db,expected_epoch=self.mrt[mid]['epoch'])
            columns=[v[1] for v in db.execute('PRAGMA table_info(paths)')]
            expected_count=db.execute('SELECT count(*) FROM paths').fetchone()[0]
            for row in self.sqlrows(paths_id,'paths'):
                found=db.execute('SELECT * FROM paths WHERE path_key=? AND as4_key=?',(row['path_key'],row['as4_key'])).fetchone()
                require(found is not None and dict(zip(columns,found))==row,'原路径BLOB/首次位置/归属/计数不符')
                self.add('origin_paths',{'file_id':paths_id,'occurrence':count,**row});count+=1
                db.execute('DELETE FROM paths WHERE path_key=? AND as4_key=?',(row['path_key'],row['as4_key']))
            require(count==expected_count==summary['distinct_paths'] and db.execute('SELECT count(*) FROM paths').fetchone()[0]==0,'路径目录人口不符')
        scratch.unlink()
        members=self.json(self.target(fid,'origins'));require(set(members)=={'ipv4','ipv6','all'},'起源family不符')
        for family in ('ipv4','ipv6','all'):
            m=summary['families'][family]
            where='' if family=='all' else ' AND afi='+str(1 if family=='ipv4' else 2)
            actual=[r[0] for r in self.db.execute('SELECT DISTINCT attributed_origin_asn FROM mrt_observations WHERE file_id=? AND attributed_origin_asn IS NOT NULL'+where+' ORDER BY attributed_origin_asn',(mid,))]
            require(members[family]==actual and m['visible_origin_ases']==len(actual),'起源有序成员/并集不符')
            n=self.db.execute('SELECT count(*),sum(attributed_origin_asn IS NULL) FROM mrt_observations WHERE file_id=?'+where,(mid,)).fetchone()
            pc=sum(len(prefixes[f]) for f in ('ipv4','ipv6')) if family=='all' else len(prefixes[family])
            require(m['rib_entries']==n[0] and m['unattributed_entries']==(n[1] or 0) and m['visible_prefixes']==pc,'起源family原量不符')
            if family!='all':
                require(m['prefix_set_sha256']==hashlib.sha256(b''.join(sorted(prefixes[family]))).hexdigest(),'前缀集合不符')
                require(m['peer_entry_counts']=={str(k):v for k,v in peer_counts[family].items()},'原Peer计数不符')
                reasons=dict(self.db.execute('SELECT origin_reason,count(*) FROM mrt_observations WHERE file_id=?'+where+' GROUP BY origin_reason',(mid,)))
                require(m['reasons']==reasons,'原归属理由计数不符')
            self.add('origin_family',{'file_id':sid,'family':family,'observed_at':summary['observed_at'],**m})
            for i,asn in enumerate(members[family]):self.add('origin_members',{'file_id':self.target(fid,'origins'),'family':family,'member_ordinal':i,'asn':asn})
        require(summary['mrt_records']==records and summary['decoded_bytes']==decoded,'起源原MRT人口/字节不符')
    def scale(self,fid,manifest):
        sid=self.target(fid,'scale-summary');s=self.json(sid);mid=self.target(sid,'mrt-gzip')
        require(s['schema_version']=='core-rib-scale/v1' and s['interpretation_version']=='rib-prefix-union/v1' and s['origin_metric_state']=='pending_definition','规模规则不符')
        self.verify_source(s['source'],mid,s['observed_at'],s['data_profile'])
        for family in ('ipv4','ipv6','all'):
            m=s['families'][family];where='' if family=='all' else ' AND afi='+str(1 if family=='ipv4' else 2)
            counts=self.db.execute('SELECT count(*),count(DISTINCT prefix) FROM mrt_observations WHERE file_id=?'+where,(mid,)).fetchone()
            peers=[r[0] for r in self.db.execute('SELECT DISTINCT peer_index FROM mrt_observations WHERE file_id=?'+where+' ORDER BY peer_index',(mid,))]
            require(m['rib_entries']==counts[0] and m['visible_prefixes']==counts[1] and m['peer_indices']==peers and m['visible_origin_ases'] is None,'规模原量/Peer不符')
            if family!='all':
                self.db.execute('CREATE TABLE scale_prefix_keys(key BLOB PRIMARY KEY)')
                for (prefix,) in self.db.execute('SELECT DISTINCT prefix FROM mrt_observations WHERE file_id=?'+where,(mid,)):
                    self.budget.check();net=ipaddress.ip_network(prefix)
                    self.db.execute('INSERT INTO scale_prefix_keys VALUES (?)',(bytes([net.prefixlen])+net.network_address.packed,))
                digest=hashlib.sha256()
                for (key,) in self.db.execute('SELECT key FROM scale_prefix_keys ORDER BY key'):self.budget.check();digest.update(key)
                self.db.execute('DROP TABLE scale_prefix_keys')
                require(m['prefix_set_sha256']==digest.hexdigest(),'规模前缀集合不符')
            self.add('scale_family',{'file_id':sid,'family':family,'observed_at':s['observed_at'],**m})
    def metrics(self,fid,summary,families):
        require(summary['interval_change_count'] is None and summary['session_continuity']=='unknown','伪连续资格')
        for family,values in families.items():
            denominator=values['same']+values['different']
            self.add('path_endpoint_metrics',{'file_id':fid,'family':family,**values,'comparable_pairs':denominator,
                    'different_fraction':str(values['different']/denominator) if denominator else None,
                    'interval_change_count':None,'session_continuity':'unknown'})
    def compare(self,fid,manifest):
        sid=self.target(fid,'comparison-summary');s=self.json(sid);inputs=s['inputs']
        if sid in self.compared:return
        self.compared.add(sid)
        require(s['schema_version']=='rib-path-comparison/v1' and s['interpretation_version']==comparison.RULE and manifest['interpretation_version']==comparison.RULE,'比较规则不符')
        require(len(inputs)==2 and inputs[0]['epoch']<inputs[1]['epoch'],'非有序两端点')
        require(s['collector_id']=='rrc25' and s['coverage']=='unknown','比较Collector/coverage不符')
        mids=[self.target(fid,'mrt-gzip',side+'.gz') for side in ('left','right')]
        for side,mid,source in zip(('left','right'),mids,inputs):
            self.verify_source(source,mid,source['observed_at'],s['data_profile'])
            rawid=self.target(fid,'mrt',side+'.mrt');raw=self.files[rawid];gz=self.files[mid]
            require(raw['raw_sha']==gz['decoded_sha']==source['decoded_sha256'] and raw['raw_bytes']==gz['decoded_bytes']==source['decoded_bytes'] and source['gzip_eof'] is True,'压缩和原MRT不一致')
            require(source['peers']==self.mrt[mid]['peers'] and source['epoch']==self.mrt[mid]['epoch'],'摘要原Peer/时点不符')
            require(source['mrt_records']==self.db.execute('SELECT count(*) FROM mrt_frames WHERE file_id=?',(mid,)).fetchone()[0],'MRT帧人口不符')
        groups_id=self.target(fid,'peer-groups');groups=self.json(groups_id)
        expected_groups,maps=comparison._groups(*[self.mrt[mid] for mid in mids])
        require(groups==expected_groups,'Peer分组原属性/成员不符')
        for group in groups:
            self.add('peer_groups',{'file_id':groups_id,'group_id':group['id'],**group})
            for side,label in enumerate(('left','right')):
                for i,p in enumerate(group[label+'_indexes']):self.add('peer_group_members',{'file_id':groups_id,'group_id':group['id'],'side':side,'member_ordinal':i,'peer_index':p})
        frameid=self.target(fid,'rib-sqlite');frame_count=0
        self.db.execute('CREATE TABLE seen_frame(side INTEGER,record INTEGER,PRIMARY KEY(side,record))')
        for row in self.sqlrows(frameid,'frames'):
            require(type(row['side']) is int and row['side'] in (0,1),'frame side无效')
            self.db.execute('INSERT INTO seen_frame VALUES (?,?)',(row['side'],row['record']))
            actual=self.db.execute('SELECT * FROM mrt_frames WHERE file_id=? AND record=?',(mids[row['side']],row['record'])).fetchone()
            require(actual is not None and actual['prefix'] is not None,'frame指向非前缀')
            net=ipaddress.ip_network(actual['prefix'])
            expected={'side':row['side'],'afi':actual['afi'],'prefix':net.network_address.packed+bytes([net.prefixlen]),'record':actual['record'],'offset':actual['decoded_offset'],'size':actual['body_bytes'],'subtype':actual['subtype']}
            require(row==expected,'frames.sqlite原位置/前缀不符');frame_count+=1
            if frame_count%self.budget.limits.batch_rows==0:self.disk()
        require(frame_count==sum(self.db.execute('SELECT count(*) FROM mrt_frames WHERE file_id=? AND prefix IS NOT NULL',(mid,)).fetchone()[0] for mid in mids),'frames.sqlite遗漏/重复')
        self.db.execute('DROP TABLE seen_frame')
        cid=self.target(fid,'comparisons');counts={afi:Counter({k:0 for k in comparison.STATUSES}) for afi in (1,2)};reasons_total=Counter();entries_total=[Counter(),Counter()]
        self.db.execute('CREATE TABLE seen_prefix(afi INTEGER,prefix TEXT,PRIMARY KEY(afi,prefix))')
        n=0
        for doc in self.db.execute('SELECT * FROM documents WHERE file_id=? ORDER BY document_id',(cid,)):
            doc=dict(doc);row=self.value(doc);afi=row['afi'];prefix=row['prefix']
            require(type(afi) is int and afi in (1,2) and row['safi']==1,'comparison族无效')
            self.db.execute('INSERT INTO seen_prefix VALUES (?,?)',(afi,prefix))
            objects_by_side=[]
            for side,label in enumerate(('left','right')):
                actual_frames=[dict(r) for r in self.db.execute('SELECT * FROM mrt_frames WHERE file_id=? AND afi=? AND prefix=? ORDER BY record LIMIT 1001',(mids[side],afi,prefix))]
                require(len(actual_frames)<=1000 and sum(r['body_bytes'] for r in actual_frames)<=32*1024**2,'同Prefix帧资源超限')
                expected_frames=[[r['record'],r['decoded_offset'],r['subtype']] for r in actual_frames]
                require(row[label+'_frames']==expected_frames,'comparison frame位置指错/遗漏')
                by_group={};entry_count=0
                for position,f in enumerate(actual_frames):
                    self.add('comparison_frames',{'file_id':cid,'document_id':doc['document_id'],'side':side,'frame_position':position,'record':f['record'],'offset':f['decoded_offset'],'subtype':f['subtype'],'mrt_file':mids[side]})
                    for entry in self.db.execute('SELECT * FROM mrt_observations WHERE file_id=? AND record=? ORDER BY entry_index',(mids[side],f['record'])):
                        gid=maps[side][entry['peer_index']]
                        by_group.setdefault(gid,[]).append(([position,entry['entry_index']],dict(entry)))
                        entry_count+=1
                        require(entry_count<=100000,'同Prefix refs超限')
                objects_by_side.append(by_group)
                entries_total[side][str(afi)]+=sum(len(v) for v in by_group.values())
            expected_objects=[]
            for gid in sorted(objects_by_side[0].keys()|objects_by_side[1].keys()):
                left,right=[entries.get(gid,[]) for entries in objects_by_side]
                reasons=sorted({reason for _,entry in left+right for reason in json.loads(entry['comparison_reasons'])})
                if len(left)>1 or len(right)>1:reasons.append('duplicate_entry')
                if any(len(groups[gid][label+'_indexes'])>1 for label in ('left','right')):
                    reasons.append('ambiguous_peer_group');status='not_comparable'
                elif not left:status='right_only'
                elif not right:status='left_only'
                elif reasons:status='not_comparable'
                else:status='same' if left[0][1]['canonical_path']==right[0][1]['canonical_path'] else 'different'
                expected_objects.append([gid,status,[v[0] for v in left],[v[0] for v in right],reasons])
            require(row['objects']==expected_objects,'全量对象refs/Peer/路径分类或理由不符')
            for oi,(gid,status,left,right,reasons) in enumerate(row['objects']):
                self.add('comparison_objects',{'file_id':cid,'document_id':doc['document_id'],'line_ordinal':n,'object_ordinal':oi,'afi':afi,'safi':1,'prefix':prefix,'group_id':gid,'status':status})
                counts[afi][status]+=1;reasons_total.update(reasons)
                for i,reason in enumerate(reasons):self.add('comparison_reasons',{'file_id':cid,'document_id':doc['document_id'],'object_ordinal':oi,'reason_ordinal':i,'reason':reason})
                for side,refs in enumerate((left,right)):
                    for ri,(position,entry_index) in enumerate(refs):
                        record,offset,subtype=row[('left','right')[side]+'_frames'][position]
                        entry=self.db.execute('SELECT peer_index FROM mrt_observations WHERE file_id=? AND record=? AND entry_index=?',(mids[side],record,entry_index)).fetchone()
                        self.add('comparison_refs',{'file_id':cid,'document_id':doc['document_id'],'object_ordinal':oi,'side':side,'ref_ordinal':ri,'frame_position':position,'entry_index':entry_index,'mrt_file':mids[side],'record':record,'decoded_offset':offset,'peer_index':entry[0],'resolution':'verified_original_mrt'})
            n+=1
        union=self.db.execute('SELECT count(*) FROM (SELECT afi,prefix FROM mrt_frames WHERE file_id IN (?,?) AND prefix IS NOT NULL GROUP BY afi,prefix)',mids).fetchone()[0]
        require(n==union==s['prefix_rows'],'comparison全部前缀未闭合')
        totals={k:counts[1][k]+counts[2][k] for k in comparison.STATUSES}
        require(s['totals']==totals and s['families']=={str(k):dict(v) for k,v in counts.items()},'comparison分类摘要不符')
        require(s['reason_counts']==dict(reasons_total) and s['source_entries']==[dict(c) for c in entries_total] and s['raw_peer_groups']==len(groups),'comparison来源/理由摘要不符')
        denominator=totals['same']+totals['different']
        from decimal import Decimal
        require(s['comparable_pairs']==denominator and s['different_fraction']==(Decimal(str(totals['different']/denominator)) if denominator else None),'comparison分母/比例不符')
        self.metrics(sid,s,{'ipv4':dict(counts[1]),'ipv6':dict(counts[2]),'all':totals})
        self.db.execute('DROP TABLE seen_prefix')
    def package(self,fid,package):
        sid=self.target(fid,'path-summary');summary=self.json(sid)
        sourceid=self.target(fid,'comparison-manifest');source=self.json(self.target(sourceid,'comparison-summary'))
        require(package['source_manifest_sha256']==self.files[sourceid]['raw_sha'],'消费源manifest身份不符')
        require(summary['schema_version']=='core-rib-path-summary/v1' and summary['comparison_version']=='rib_path_comparison_v1_'+package['source_manifest_sha256'],'消费比较身份不符')
        for key in ('data_profile','collector_id','coverage','session_continuity','interval_change_count','interpretation_version','limits'):
            require(summary[key]==source[key],'消费summary与源summary冲突: '+key)
        require(summary['families']=={'ipv4':source['families']['1'],'ipv6':source['families']['2'],'all':source['totals']},'消费summary分类量不符')
        for i,label in enumerate(('left','right')):
            require(summary[label]=={k:source['inputs'][i][k] for k in ('observed_at','sha256')},'消费summary端点来源不符')
        # 仅复制两个有界原字节文件构成旧Reader的目录接缝，不重编码原JSON，
        # 不回访原source、不改旧科学规则或全局_read函数。
        with TemporaryDirectory(prefix='consumer-contract-', dir=self.out) as temporary:
            folder=Path(temporary);(folder/'paths').mkdir()
            for file_id,name in ((fid,'manifest.json'),(sid,'summary.json')):
                doc=self.doc(file_id)
                raw=read_span(self.source,doc['entity_path'],0,self.files[file_id]['raw_bytes'],self.budget,65536)
                require(directory_bytes(self.out,self.budget)+len(raw)<=self.budget.collection_limits.temporary_bytes,'原生消费校验暂存超限')
                (folder/'paths'/name).write_bytes(raw)
            self.disk()
            read_comparison(folder/'index.json',{
                'path_comparison':{'file':'paths/manifest.json','sha256':self.files[fid]['raw_sha']},
                'data_profile':source['data_profile'],'source':{'collector_id':source['collector_id']}})
        cid=self.target(sourceid,'comparisons');groups_id=self.target(sourceid,'peer-groups')
        # 原消费门禁之后，样例还须对应已核验的different对象及其唯一原refs。

        for example in summary['examples']:
            peer=example['peer']
            matched=self.db.execute('SELECT o.document_id,o.object_ordinal,o.status FROM comparison_objects o JOIN peer_groups g ON o.group_id=g.group_id WHERE o.file_id=? AND o.afi=? AND o.safi=1 AND o.prefix=? AND g.file_id=? AND g.bgp_id=? AND g.ip=? AND g.asn=?',
                (cid,1 if example['family']=='ipv4' else 2,example['prefix'],groups_id,peer['bgp_id'],peer['ip'],peer['asn'])).fetchall()
            require(len(matched)==1 and matched[0]['status']=='different','消费样例未唯一绑定已核验different对象')
            for side,label in enumerate(('left','right')):
                mid=self.target(sourceid,'mrt-gzip',label+'.gz');ref=example[label+'_reference']
                links=self.db.execute('SELECT record,decoded_offset,entry_index,peer_index FROM comparison_refs WHERE file_id=? AND document_id=? AND object_ordinal=? AND side=? ORDER BY ref_ordinal',
                    (cid,matched[0]['document_id'],matched[0]['object_ordinal'],side)).fetchall()
                require(len(links)==1 and tuple(links[0])==(ref['record'],ref['offset'],ref['entry'],ref['peer_index']),'消费样例与已验证对象refs不符')
                record=self.db.execute('SELECT * FROM mrt_observations WHERE file_id=? AND record=? AND entry_index=?',(mid,ref['record'],ref['entry'])).fetchone()
                require(record is not None and record['decoded_offset']==ref['offset'] and record['peer_index']==ref['peer_index'] and record['prefix']==example['prefix'] and ('ipv4' if record['afi']==1 else 'ipv6')==example['family'],'消费样例原位置不符')
                require({k:record[k] for k in ('bgp_id','ip','asn')}==example['peer'] and json.loads(record['canonical_path'])==example[label+'_path'] and not json.loads(record['comparison_reasons']),'消费样例原Peer/路径不符')
            require(example['left_path']!=example['right_path'],'消费样例非different')
        self.metrics(sid,summary,summary['families'])
