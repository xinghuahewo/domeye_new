"""M3 范围资格覆盖；合法事实与兼容状态由既有 Replay 计算。"""
from dataclasses import asdict
from data_pipeline.bgp.input.mrt_reader import Message
from data_pipeline.bgp.replay.route_replay import Replay, identity
from data_pipeline.bgp.replay.calculation_window import calculation_window as checked_window, epoch_bounds, check_update_time, WINDOW_RULE
from data_pipeline.bgp.record_types import MessageBoundary, Element, ElementPosition, SourceStart, SourceEnd, SourceQuality


def message_from_boundary(boundary):
    r=boundary.raw
    m=Message(*(r[k] for k in ('source_id','record','offset','length','epoch','mrt_type','mrt_subtype','raw_digest')),
              content_sha256=r['content_sha256'],microsecond=r['microsecond'],kind=r['kind'],reason=r['reason'],
              old_state=r['old_state'],new_state=r['new_state'],interpretation=boundary.interpretation)
    m.peer={k:r[v] for k,v in (('ip','peer_ip'),('asn','peer_asn'),('bgp_id','bgp_id'),
        ('bgp_id_present','bgp_id_present'),('local_ip','local_ip'),('local_asn','local_asn'),
        ('interface','interface'),('local_message','local_message'))}
    return m


def element_for_replay(row, message):
    peer={**message.peer,'ip':row['peer_ip'],'asn':row['peer_asn'],'bgp_id':row['bgp_id'],
          'bgp_id_present':row['bgp_id_present'],'table_record':row['peer_table_record'],'index':row['peer_index']}
    return {k:row[k] for k in ('ordinal','action','afi','safi','prefix','path_id','path_id_present','path_key')}|{'peer':peer}


def position_key(position):
    if 'message' in position:
        return (*position_key(position['message'])[:2],1,position['ordinal'])
    return (position['source_rank'],position['record'],0,0)


class ScopeIndex:
    """保存全部历史范围；按端点/远端索引候选，未见对象也能查询。"""
    def __init__(self):
        self.gaps={};self.collector={};self.endpoint={};self.remote={}

    def add(self,gap):
        if gap['gap_id'] in self.gaps:raise ValueError('重复 Gap')
        self.gaps[gap['gap_id']]=gap
        scope=gap['scope'];kind=scope['kind'];collector=scope['collector']
        if kind=='local_observation':return
        endpoint=scope['endpoint']
        if kind=='collector_chain':self.collector.setdefault(collector,[]).append(gap)
        else:
            key=(collector,*(endpoint[n] for n in ('peer_ip','peer_asn','local_ip','local_asn','interface')))
            self.endpoint.setdefault(key,[]).append(gap)
            self.remote.setdefault(key[:3],[]).append(gap)

    def matching(self,key):
        candidates=[(g,'definite') for g in self.collector.get(key[0],())]
        if isinstance(key[3],str) and key[3].startswith('unmapped_rib:'):
            candidates += [(g,'possible_overlap') for g in self.remote.get(key[:3],())]
        else:candidates += [(g,'definite') for g in self.endpoint.get(key[:6],())]
        return sorted(candidates,key=lambda pair:position_key(pair[0]['position']))

    def qualification(self,key,last_position=None):
        matches=self.matching(key)
        active=[(g,overlap) for g,overlap in matches if last_position is None or
                position_key(g['position'])>position_key(last_position)]
        return dict(current='unknown' if active else 'known',
                    continuity='partial' if matches else 'unknown_session_continuity',
                    gap_ids=[g['gap_id'] for g,_ in matches],active_gap_ids=[g['gap_id'] for g,_ in active],
                    overlap='possible_overlap' if any(o=='possible_overlap' for _,o in active) else
                            'definite' if active else 'none',session='unknown')


def current_record(replay,index,key,last_position):
    current=replay.current.get(key);known=replay.last_known.get(key)
    q=index.qualification(key,last_position)
    if current is None:
        q['current']='unknown'  # 没有观察并不证明 absent，即使无 Gap。
    elif current['presence']=='unknown':q['current']='unknown'
    return dict(object_key=identity(key),scope=key,current=current,last_known=known,
                last_position=last_position,qualification=q)


class CanonicalReplay:
    def __init__(self,plan,binding,selected,window=None,*,calculation_window=None,compact=False,detailed=True,legacy_views=True):
        self.replay=Replay(plan,compact=compact,detailed=detailed,legacy_views=legacy_views);self.binding=binding;self.selected=tuple(selected)
        if self.selected!=(plan.baseline_source,*plan.update_sources):raise ValueError('canonical 选择必须为绑定基线和 UPDATE')
        self.index=ScopeIndex();self.positions=self.replay.memory.positions if compact else {};self.boundary=None;self.message=None
        self.ends=[];self.source=None;self.last_order=None;self.window=window
        self.calculation_window = None if calculation_window is None else checked_window(calculation_window)
        self.window_bounds = None if self.calculation_window is None else epoch_bounds(self.calculation_window)
        if self.calculation_window is not None: self.window = self.calculation_window
        self.window_checked = 0
        self.first=self.last=None
        self._rib_endpoint_token=None
        self._rib_endpoint_map=None
        self.counts={s:{t:0 for t in ('changes','invalidations','scope_gap','qualification_change','source_quality')} for s in selected}

    def apply_rib_batch(self, batch):
        """已逐列核验的 RIB 直接写唯一工作态；不生成无人消费的逐条审计对象。"""
        r = self.replay
        if (r.memory is None or r.detailed or r.legacy_views or self.index.gaps
                or batch.binding_ref != self.binding.binding_id or self.source != self.selected[0]
                or self.boundary != batch.boundary or self.boundary.raw['kind'] != 'rib'
                or self.message.peer.get('local_message')):
            raise ValueError('当前规范态不能消费 RIB 列批')
        rank, record = batch.boundary.position.source_rank, batch.boundary.position.record
        first_order = (rank, record, 1, batch.columns[-1][0])
        if self.last_order is not None and first_order <= self.last_order:
            raise ValueError('canonical RIB 列批重复或乱序')
        if (rank,record,batch.columns[-1][0])<=r.cursor:
            raise ValueError('RIB 工作态位置重复或乱序')
        if batch.prepared is None or self._rib_endpoint_token is not batch.prepared.token:
            endpoints={}
            endpoint_columns = batch.prepared.columns if batch.prepared is not None else batch.columns
            for peer in dict.fromkeys(zip(endpoint_columns[5],endpoint_columns[6])):
                local=r._baseline_endpoints.get(peer,('unmapped_rib:'+self.source,None,None))
                endpoints[peer]=(r.plan.collector,*peer,*local)
            self._rib_endpoint_map=endpoints
            self._rib_endpoint_token=None if batch.prepared is None else batch.prepared.token
        endpoints=self._rib_endpoint_map
        last=r.memory.observe_rib_batch(batch.columns,endpoints,batch.paths,r._source_ranks[self.source],
            rank,record,self.message.epoch,-1,arrow=batch.arrow,
            prepared=batch.prepared,prepared_offset=batch.prepared_offset)
        r.cursor=(rank,record,last)
        self.last_order = (rank, record, 1, batch.columns[-1][-1])
        self.counts[self.source]['changes'] += len(batch)

    def apply(self,item):
        for table,row in self._apply(item):
            if table in self.counts.get(row.get('source_id'),{}):self.counts[row['source_id']][table]+=1
            yield table,row

    def apply_update_rows(self, batch, boundary, begin, end, sink, after=None, *, after_row=None, after_fields=None):
        """已核验的本消息列片段；每写一条便回调业务，不能整批推进后再检测。"""
        r = self.replay
        if (batch.binding_ref != self.binding.binding_id or self.boundary != boundary
                or self.source != boundary.raw['source_id'] or boundary.raw['kind'] != 'update'
                or boundary.gap is not None):
            raise ValueError('UPDATE 列批与当前消息边界冲突')
        if r.memory is None or r.detailed or r.legacy_views or self.index.gaps:
            for i in range(begin, end):
                item = Element(batch.binding_ref, ElementPosition(boundary.position, batch.columns['ordinal'][i]),
                               batch.row(i, boundary))
                for table, row in self.apply(item):
                    sink(table, row)
                if after is not None:
                    after(item)
                if after_row is not None:
                    after_row(item.raw,boundary,item.position.ordinal)
                if after_fields is not None:
                    after_fields(batch,i,boundary)
            return
        rank, record = boundary.position.source_rank, boundary.position.record
        source = r._source_ranks[self.source]
        message = self.message
        local = tuple(message.peer.get(name) for name in ('local_ip', 'local_asn', 'interface'))
        fields = ('action', 'afi', 'safi', 'prefix', 'path_id_present', 'path_id',
                  'peer_ip', 'peer_asn', 'path_key', 'ordinal')
        for i, values in enumerate(zip(*(batch.columns[name][begin:end] for name in fields)), begin):
            action, afi, safi, prefix, present, pathid, ip, asn, pathkey, ordinal = values
            order = (rank, record, 1, ordinal)
            if self.last_order is not None and order <= self.last_order:
                raise ValueError('canonical UPDATE 列批重复或乱序')
            self.last_order = order
            if not message.peer.get('local_message'):
                cursor = (rank, record, ordinal)
                if cursor <= r.cursor:
                    raise ValueError('UPDATE 工作态位置重复或乱序')
                scope = (r.plan.collector, ip, asn, *local, afi, safi, prefix, present, pathid)
                path = batch.paths[pathkey]
                r.memory.observe(scope, source, record, ordinal, message.epoch,
                                 None if action == 'withdraw' else pathkey,
                                 None if action == 'withdraw' else path['attributed_origin_asn'], position=cursor)
                r.cursor = cursor
                self.counts[self.source]['changes'] += 1
            if after is not None:
                after(Element(batch.binding_ref, ElementPosition(boundary.position, ordinal), batch.row(i, boundary)))
            if after_row is not None:
                after_row(batch.row(i,boundary),boundary,ordinal)
            if after_fields is not None:
                after_fields(batch,i,boundary)

    def _apply(self,item):
        if item.binding_ref!=self.binding.binding_id:raise ValueError('canonical 输入绑定冲突')
        if isinstance(item,SourceStart):
            if self.source is not None or item.raw.source_id!=self.selected[len(self.ends)]:raise ValueError('canonical 来源乱序')
            self.source=item.raw.source_id;self.first=self.last=None;self.window_checked=0
            return
        if isinstance(item,SourceQuality):
            yield 'source_quality',dict(source_id=item.raw['source_id'],raw=item.raw,source_rank=item.source_rank)
            return
        if isinstance(item,SourceEnd):
            if self.source!=item.raw.source_id:raise ValueError('canonical 来源尾不符')
            coverage=dict(source_id=self.source,source_rank=item.source_rank,
                raw=asdict(item.raw),parse_counts=asdict(item.parse_counts),derived_counts=dict(self.counts[self.source]),
                declared_window=self.window,first_observation=self.first,last_observation=self.last,
                inherited_gap_ids=list(self.index.gaps),execution='complete',
                parse_coverage='partial' if item.parse_counts.rejected or item.parse_counts.unsupported else 'complete',
                current_coverage='requires_object_query',continuity='partial' if any(g['direction']!='local' for g in self.index.gaps.values()) else 'unknown_session_continuity')
            if self.calculation_window is not None:
                coverage['window_qualification']=dict(rule=WINDOW_RULE,
                    role='baseline_complete_cutover_assumed' if self.source==self.selected[0] else 'update_half_open',
                    checked_messages=self.window_checked)
            yield 'source_coverage',coverage
            self.ends.append(self.source);self.source=None;self.boundary=self.message=None
            return
        if self.last_order is not None and item.position.sort_key<=self.last_order:raise ValueError('canonical 重复或乱序')
        self.last_order=item.position.sort_key
        if isinstance(item,MessageBoundary):
            if item.raw['source_id']!=self.source:raise ValueError('canonical 边界来源不符')
            if self.window_bounds is not None and self.source != self.selected[0]:
                check_update_time(item,self.window_bounds)
                self.window_checked += 1
            self.boundary=item;self.message=message_from_boundary(item)
            self.last=dict(position=dict(source_rank=item.position.source_rank,record=item.position.record),
                           raw_time=dict(epoch=item.raw_time.epoch,microsecond=item.raw_time.microsecond))
            if self.first is None:self.first=self.last
            if item.gap is not None:
                gap=asdict(item.gap)
                # str 枚举归为其原始值；不重新分类。
                gap['scope']['kind']=item.gap.scope.kind.value
                gap['direction']=item.gap.direction.value
                gap['parse_status']=item.gap.parse_status.value
                self.index.add(gap)
                yield 'scope_gap',dict(gap_id=gap['gap_id'],source_id=item.raw['source_id'],message_id=item.raw['message_id'],raw=gap)
                yield 'qualification_change',dict(gap_id=gap['gap_id'],source_id=item.raw['source_id'],message_id=item.raw['message_id'],
                    target='scope',position=asdict(item.position),status='not_applicable' if gap['direction']=='local' else 'unknown',
                    dimension='canonical_current_and_continuity',scope=gap['scope'])
                if gap['direction']!='local':
                    for key,value in self.replay.current.items():
                        active=self.index.qualification(key,self.positions.get(key))
                        if gap['gap_id'] not in active['active_gap_ids']:continue
                        self.replay.current[key]={**value,'presence':'unknown','reason':'parse_gap','invalidated_by':gap['gap_id']}
                        yield 'qualification_change',dict(gap_id=gap['gap_id'],object_key=identity(key),source_id=item.raw['source_id'],
                            message_id=item.raw['message_id'],target='object',position=asdict(item.position),status='unknown',
                            dimension='current',overlap=active['overlap'])
                return
            for row in self.replay.consume(self.message):
                yield 'invalidations',dict(object_key=row['object_key'],source_id=self.source,message_id=item.raw['message_id'],raw=row,position=asdict(item.position))
            return
        if not isinstance(item,Element) or self.boundary is None:raise ValueError('canonical 元素缺边界')
        if item.position.message!=self.boundary.position or self.boundary.gap:raise ValueError('canonical 元素引用冲突')
        e=element_for_replay(item.raw,self.message)
        key=self.replay.object_key(self.message,e)
        # 尚无任何 Gap 时，资格索引必为空；无需展开旧位置或扫描空索引。
        # 一旦出现 Gap（包括历史 Gap），继续使用原有逐对象资格判断。
        active=self.index.qualification(key,self.positions.get(key)) if self.index.gaps else None
        if not self.message.peer.get('local_message') and active is not None and active['active_gap_ids']:
            value=self.replay.current.get(key) or dict(presence='unknown',path_key=None,origin=None,event_id=None,epoch=None)
            self.replay.current[key]={**value,'presence':'unknown','reason':'parse_gap','invalidated_by':active['active_gap_ids'][-1]}
        self.message.elements=[e]
        self.message.paths=[{k:item.raw[k] for k in ('path_key','as_path_text','attributed_origin_asn')}]
        position=(item.position.message.source_rank,item.position.message.record,item.position.ordinal)
        for row in self.replay.consume(self.message,key_hint=key,position_hint=position):
            if self.replay.memory is None:self.positions[key]=asdict(item.position)
            if not self.replay.detailed:
                yield 'changes',dict(source_id=self.source,raw=row)
                if active is not None and active['gap_ids']:
                    yield 'qualification_change',dict(event_id=f'{self.message.message_id}:{e["ordinal"]}',object_key=identity(key),
                        source_id=self.source,message_id=item.raw['message_id'],target='object',position=asdict(item.position),status='known',
                        dimension='current_only',historical_gap_ids=active['gap_ids'])
                continue
            yield 'changes',dict(event_id=row['event_id'],object_key=row['object_key'],source_id=self.source,
                                  message_id=item.raw['message_id'],position=asdict(item.position),raw=row)
            if active is not None and active['gap_ids']:
                yield 'qualification_change',dict(event_id=row['event_id'],object_key=row['object_key'],source_id=self.source,
                    message_id=item.raw['message_id'],target='object',position=asdict(item.position),status='known',
                    dimension='current_only',historical_gap_ids=active['gap_ids'])
        self.message.elements=[];self.message.paths=[]

    def export(self):
        if tuple(self.ends)!=self.selected or self.source is not None:raise ValueError('canonical 输入未完整结束')
        for key in self.replay.current:
            yield 'current_routes',current_record(self.replay,self.index,key,self.positions.get(key))
        for (prefix,vp),path in self.replay.legacy_paths.items():
            yield 'legacy_state',dict(prefix=prefix,vp=vp,path_text=path)
        for prefix in sorted(self.replay.legacy_by_prefix.keys() | self.replay.legacy_origins.keys()):
            yield 'legacy_prefixes',dict(prefix=prefix,paths=self.replay.legacy_by_prefix.get(prefix),origins=self.replay.legacy_origins.get(prefix))
        for vp in sorted(self.replay.seen_vps):yield 'legacy_seen_vps',dict(vp=vp)
        metadata=dict(plan=asdict(self.replay.plan),plan_version=self.replay.plan.version,
            legacy_baseline_epoch=self.replay.legacy_baseline_epoch,cursor=self.replay.cursor,
            limits=dict(value_cache=8192,prefix_cache=128),legacy_country_filter=None)

        if self.calculation_window is not None:
            metadata.update(calculation_window=self.calculation_window,calculation_window_rule=WINDOW_RULE,
                plan_version=identity([self.replay.plan.version,WINDOW_RULE,self.calculation_window]))
        yield 'projection_metadata',metadata
