"""唯一的紧凑路由工作态；current、last_known、position 只是兼容读取视图。"""
from collections.abc import MutableMapping
from functools import lru_cache
import struct

_KEY = struct.Struct('>IHBBI')
# 存在标志、路径摘要、起源、事件来源/记录/元素、时间、失效上下文、最后位置。
_VALUE = struct.Struct('>B32sqIQIqIIQI')
_EMPTY = (0, bytes(32), -1, 0, 0, 0, -1, 0, 0, 0, 0)


class PrefixState:
    """每个路由槽仅一份 bytes 负载；失效只存上下文编号，明确撤回可为最后已知。"""
    def __init__(self, plan):
        self.plan=plan;self.sources=(plan.baseline_source,*plan.update_sources)
        self.source_ids={s:i for i,s in enumerate(self.sources)}
        self.prefix_dict={};self.endpoints=[];self.endpoint_ids={}
        self.endpoint_members={}
        self.contexts=[None];self.context_ids={};self.dirty=None
        self.current=StateView(self,'current');self.last_known=StateView(self,'last_known')
        self.positions=StateView(self,'positions')
        self._prefix=lru_cache(maxsize=1024)(lambda value:value.encode('ascii'))
        self.native=None
        self._rib_token=None
        self._rib_rows=None

    def endpoint_id(self, endpoint):
        ident=self.endpoint_ids.get(endpoint)
        if ident is None:
            ident=len(self.endpoints);self.endpoints.append(endpoint);self.endpoint_ids[endpoint]=ident
            if ident>=65536:raise ValueError('路由端点容量上限')
        return ident

    def key(self, scope):
        return _KEY.pack(self.endpoint_id(tuple(scope[:6])),scope[6],scope[7],scope[9],scope[10] or 0)+self._prefix(scope[8])

    def observe_rib_batch(self, columns, endpoints, paths, source, rank, record, epoch, previous, *, arrow=None,
                          prepared=None, prepared_offset=0):
        """已验证的 RIB 列直接打包；每批只解释一次路径摘要与端点头。"""
        if arrow is not None and self.native is not None:
            # 编号按该批首次观察顺序分配，与原逐行实现及恢复元数据保持一致。
            if prepared is None or self._rib_token is not prepared.token:
                source_columns=columns if prepared is None else prepared.columns
                endpoint_ids={key:self.endpoint_id(endpoints[key])
                              for key in dict.fromkeys(zip(source_columns[5],source_columns[6]))}
                batch=self.native.pack_rib(arrow if prepared is None else prepared.arrow,endpoint_ids,
                    paths if prepared is None else prepared.paths,source,rank,
                    record if prepared is None else None,epoch,previous)
                self._rib_rows=tuple(batch[name].to_pylist() for name in ('key','value','endpoint_id'))
                self._rib_token=None if prepared is None else prepared.token
                for ident in endpoint_ids.values():self.endpoint_members.setdefault(ident,[])
            begin=0 if prepared is None else prepared_offset
            end=begin+len(columns[0])
            keys,values,idents=(part[begin:end] for part in self._rib_rows)
            for key,value,ident in zip(keys,values,idents):
                if key not in self.prefix_dict:self.endpoint_members[ident].append(key)
                self.prefix_dict[key]=value
            if self.dirty is not None:self.dirty.update(keys)
            return columns[8][-1] if columns[8] else previous
        prepared={key:(bytes.fromhex(key),value['attributed_origin_asn']) for key,value in paths.items()}
        headers={};prefixes={};last=previous
        packed_rows=self.prefix_dict;members=self.endpoint_members;dirty=self.dirty
        for afi,safi,prefix,present,pathid,ip,asn,pathkey,ordinal in zip(*columns):
            if ordinal<=last:raise ValueError('RIB 工作态位置重复或乱序')
            last=ordinal
            slot=(ip,asn,afi,safi,present,pathid)
            header=headers.get(slot)
            if header is None:
                ident=self.endpoint_id(endpoints[ip,asn])
                header=(_KEY.pack(ident,afi,safi,present,pathid or 0),members.setdefault(ident,[]))
                headers[slot]=header
            suffix=prefixes.get(prefix)
            if suffix is None:suffix=prefixes[prefix]=prefix.encode('ascii')
            key=header[0]+suffix
            path,origin=prepared[pathkey]
            value=_VALUE.pack(7,path,origin if origin is not None else -1,source,record,ordinal,epoch,0,rank,record,ordinal)
            if key not in packed_rows:header[1].append(key)
            packed_rows[key]=value
            if dirty is not None:dirty.add(key)
        return last

    def scope(self,key):
        endpoint,afi,safi,present,pathid=_KEY.unpack_from(key)
        return (*self.endpoints[endpoint],afi,safi,key[_KEY.size:].decode('ascii'),bool(present),pathid if present else None)

    def put(self,key,values):
        self.load(key,_VALUE.pack(*values))
        if self.dirty is not None:self.dirty.add(key)

    def load(self,key,packed):
        """状态写入与恢复共用；端点索引仅保存唯一字典已有键的引用。"""
        if key not in self.prefix_dict:
            endpoint=_KEY.unpack_from(key)[0]
            self.endpoint_members.setdefault(endpoint,[]).append(key)
        self.prefix_dict[key]=packed

    def current_for_peer(self,peer):
        target=tuple(peer.get(name) for name in ('ip','asn','local_ip','local_asn','interface'))
        # 端点数有界；精确匹配完整观察端点，不能按 Peer ASN 猜测会话。
        endpoints=[i for i,endpoint in enumerate(self.endpoints) if tuple(endpoint[1:6])==target]
        if len(endpoints)>1:
            # 兼容跨 Collector 的独立调用，仍按原字典插入顺序输出。
            for key,packed in self.prefix_dict.items():
                if _KEY.unpack_from(key)[0] in endpoints:
                    yield self.scope(key),self.value(packed,'current')
            return
        for endpoint in endpoints:
            for key in self.endpoint_members.get(endpoint,()):
                yield self.scope(key),self.value(self.prefix_dict[key],'current')

    def value(self,packed,view):
        flag,path,origin,source,record,ordinal,epoch,context,rank,posrecord,posordinal=_VALUE.unpack(packed)
        if view=='positions':
            if not flag&4:return None
            return dict(message=dict(source_rank=rank,record=posrecord),ordinal=posordinal)
        if view=='last_known' and not flag&1:return None
        event=f'{self.sources[source]}:{record}:{ordinal}' if flag&1 else None
        result=dict(presence='present' if flag&2 else 'absent' if flag&1 else 'unknown',
            path_key=path.hex() if flag&2 else None,origin=origin if origin>=0 else None,
            event_id=event,epoch=epoch if epoch>=0 else None)
        if view=='current' and context:
            reason,invalidated_by=self.contexts[context]
            result.update(presence='unknown',reason=reason,invalidated_by=invalidated_by)
        return result

    def set(self,scope,value,view):
        key=self.key(scope);values=list(_VALUE.unpack(self.prefix_dict[key]) if key in self.prefix_dict else _EMPTY)
        if view=='positions':
            values[0]|=4;values[8:]=[value['message']['source_rank'],value['message']['record'],value['ordinal']]
        elif view=='current' and value['presence']=='unknown':
            context=(value.get('reason'),value.get('invalidated_by'))
            ident=self.context_ids.get(context)
            if ident is None:
                ident=len(self.contexts);self.context_ids[context]=ident;self.contexts.append(context)
            values[7]=ident
        elif value is not None:
            source,record,ordinal=value['event_id'].rsplit(':',2)
            path=value['path_key']
            if path is not None and (len(path)!=64 or len(bytes.fromhex(path))!=32):raise ValueError('路径引用不是 SHA256')
            values[:8]=[(values[0]&4)|1|(2 if value['presence']=='present' else 0),bytes.fromhex(path) if path else bytes(32),
                        value['origin'] if value['origin'] is not None else -1,self.source_ids[source],int(record),int(ordinal),
                        value['epoch'],0]
        self.put(key,values)

    def observe(self,scope,source,record,ordinal,epoch,path,origin,*,position=None):
        key=self.key(scope)
        # canonical 已知本条位置，一次打包写入；独立 Replay 保留不带位置的原行为。
        self.put(key,(1|(2 if path is not None else 0)|(4 if position is not None else 0),bytes.fromhex(path) if path else bytes(32),
                      origin if origin is not None else -1,source,record,ordinal,epoch,0,*(position if position is not None else (0,0,0))))

    def metadata(self):return dict(endpoints=self.endpoints,contexts=self.contexts)

    def restore_metadata(self,meta):
        self.endpoints=list(meta['endpoints']);self.endpoint_ids={tuple(e):i for i,e in enumerate(self.endpoints)}
        self.contexts=list(meta['contexts']);self.context_ids={tuple(e):i for i,e in enumerate(self.contexts) if e is not None}


class StateView(MutableMapping):
    """旧算法访问接口；从唯一负载临时展开一个对象，返回值不会随后续更新改变。"""
    def __init__(self,memory,name):self.memory,self.name=memory,name
    def __getitem__(self,key):
        value=self.memory.value(self.memory.prefix_dict[self.memory.key(key)],self.name)
        if value is None:raise KeyError(key)
        return value
    def __setitem__(self,key,value):self.memory.set(key,value,self.name)
    def __delitem__(self,key):raise TypeError('工作态不得无来源删除')
    def __iter__(self):
        for key,value in self.memory.prefix_dict.items():
            if self.name=='current' or self.memory.value(value,self.name) is not None:yield self.memory.scope(key)
    def __len__(self):
        if self.name=='current':return len(self.memory.prefix_dict)
        return sum(1 for _ in self)
    def items(self):
        for key,value in self.memory.prefix_dict.items():
            result=self.memory.value(value,self.name)
            if result is not None:yield self.memory.scope(key),result


class FlatPaths(MutableMapping):
    """旧 Prefix × ASN 扁平访问兼容层，不再复制第二份路径键或路径正文。"""
    def __init__(self,nested):self.nested=nested
    def __getitem__(self,key):return self.nested[key[0]][key[1]]
    def __setitem__(self,key,value):self.nested.setdefault(key[0],{})[key[1]]=value
    def __delitem__(self,key):del self.nested[key[0]][key[1]]
    def __iter__(self):
        for prefix,values in self.nested.items():
            for vp in values:yield prefix,vp
    def __len__(self):return sum(map(len,self.nested.values()))
    def items(self):
        for prefix,values in self.nested.items():
            for vp,path in values.items():yield (prefix,vp),path
