"""业务恢复变化键；不复制路由负载，不借用窗口的 is_change 标记。

高基数路由和计数由主循环显式通知。检测持久化容器只在基线或恢复后
安装一次可变容器观察器，嵌套写入及共享对象会通知全部持久化拥有者。
"""
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict

MISSING = object()
_CONTAINERS = (dict, list, set, tuple)
_MUTABLES = (dict, list, set)
EXCLUDED = {'output', 'bgp_info', 'bgp_rib', 'scope', 'references', 'sink', 'context', 'rows'}


class _Node:
    __slots__=()
    def _changed(self):
        changes=self._changes
        if changes.installing: return
        if changes.failed: raise RuntimeError('失败候选必须从已确认检查点重建')
        for owner in self._owners:
            # 失效旧引用不能撤销delete；真正重插仍由根的mark(inserted=True)处理。
            changes.dirty.setdefault(owner,'put')

    def _child(self, value):
        return self._changes.wrap(value, self._owners)

    def __copy__(self):
        if isinstance(self, dict): return dict(dict.items(self))
        if isinstance(self, list): return list(self)
        return set(self)

    def __deepcopy__(self, memo):
        if isinstance(self, dict):
            result = {}; memo[id(self)] = result
            result.update((deepcopy(k, memo), deepcopy(v, memo)) for k, v in dict.items(self))
        elif isinstance(self, list):
            result = []; memo[id(self)] = result
            result.extend(deepcopy(v, memo) for v in self)
        else:
            result = set(); memo[id(self)] = result
            result.update(deepcopy(v, memo) for v in self)
        return result


class _Dict(_Node):
    __slots__=()
    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        # 保留原类的写入转换/修订通知；标量不需要第二次写入或包装调用。
        stored=dict.__getitem__(self,key)
        if isinstance(stored,_CONTAINERS):
            wrapped=self._child(stored)
            if wrapped is not stored: dict.__setitem__(self,key,wrapped)
        self._changed()

    def __delitem__(self, key):
        super().__delitem__(key); self._changed()

    def setdefault(self, key, default=None):
        if key not in self: self[key] = default
        return self[key]

    def update(self, *args, **kwargs):
        for key, value in dict(*args, **kwargs).items(): self[key] = value

    def pop(self, key, *default):
        if len(default) > 1: raise TypeError('pop 最多接受一个默认值')
        if key in self:
            value = self[key]; del self[key]; return value
        if default: return default[0]
        raise KeyError(key)

    def popitem(self):
        if not self: raise KeyError('popitem(): dictionary is empty')
        key = next(reversed(self)); return key, self.pop(key)

    def clear(self):
        if self: super().clear(); self._changed()

    def __ior__(self, other):
        self.update(other); return self


class _List(_Node, list):
    __slots__=('_changes','_owners','_state_id')
    def __setitem__(self, key, value):
        super().__setitem__(key, [self._child(v) for v in value] if isinstance(key, slice) else self._child(value)); self._changed()
    def __delitem__(self, key): super().__delitem__(key); self._changed()
    def append(self, value): super().append(self._child(value)); self._changed()
    def extend(self, values):
        if values is self: values=list(values)
        super().extend(self._child(v) for v in values); self._changed()
    def insert(self, index, value): super().insert(index, self._child(value)); self._changed()
    def pop(self, index=-1):
        value=super().pop(index); self._changed(); return value
    def remove(self, value): super().remove(value); self._changed()
    def clear(self):
        if self: super().clear(); self._changed()
    def reverse(self): super().reverse(); self._changed()
    def sort(self, *args, **kwargs): super().sort(*args, **kwargs); self._changed()
    def __iadd__(self, other): self.extend(other); return self
    def __imul__(self, count): super().__imul__(count); self._changed(); return self


class _Set(_Node, set):
    __slots__=('_changes','_owners','_state_id')
    def add(self, value):
        if value not in self: super().add(value); self._changed()
    def discard(self, value):
        if value in self: super().discard(value); self._changed()
    def remove(self, value): super().remove(value); self._changed()
    def pop(self):
        value=super().pop(); self._changed(); return value
    def clear(self):
        if self: super().clear(); self._changed()
    def update(self, *others): super().update(*others); self._changed()
    def difference_update(self, *others): super().difference_update(*others); self._changed()
    def intersection_update(self, *others): super().intersection_update(*others); self._changed()
    def symmetric_difference_update(self, other): super().symmetric_difference_update(other); self._changed()
    def __ior__(self, other): self.update(other); return self
    def __iand__(self, other): self.intersection_update(other); return self
    def __ixor__(self, other): self.symmetric_difference_update(other); return self
    def __isub__(self, other): self.difference_update(other); return self


class _Root:
    """保留原 dict 子类方法；根值对应独立持久化行。"""
    def __setitem__(self, key, value):
        fresh = key not in self
        super().__setitem__(key, value)
        owner = (self._namespace, self._key(key))
        wrapped=self._changes.wrap(dict.__getitem__(self,key),(owner,))
        dict.__setitem__(self,key,wrapped)
        if self._event_root: self._changes._bind_event_record(self,key,wrapped)
        self._changes.mark(*owner, inserted=fresh)

    def __delitem__(self, key):
        super().__delitem__(key); self._changes.mark(self._namespace, self._key(key), deleted=True)

    def setdefault(self, key, default=None):
        if key not in self: self[key] = default
        return self[key]

    def update(self, *args, **kwargs):
        for key, value in dict(*args, **kwargs).items(): self[key] = value

    def pop(self, key, *default):
        if len(default) > 1: raise TypeError('pop 最多接受一个默认值')
        if key in self:
            value = self[key]; del self[key]; return value
        if default: return default[0]
        raise KeyError(key)

    def popitem(self):
        if not self: raise KeyError('popitem(): dictionary is empty')
        key=next(reversed(self)); return key, self.pop(key)

    def clear(self):
        for key in tuple(self): del self[key]

    def __ior__(self, other): self.update(other); return self

    def __copy__(self): return dict(dict.items(self))
    def __deepcopy__(self, memo): return deepcopy(dict(dict.items(self)), memo)


_TYPES = {}
def _subclass(mixin, base):
    pair = mixin, base
    if pair not in _TYPES:
        attrs={'__slots__':('_changes','_owners','_state_id')} if mixin is _Dict and base is dict else {}
        _TYPES[pair] = type('Changed' + base.__name__, (mixin, base), attrs)
    return _TYPES[pair]


class BusinessChanges:
    def __init__(self, business, receipt=None, shared=None, guard=lambda:None):
        from data_pipeline.analysis.detection.streaming import _EventObjects, _EventRecords
        self._event_objects_type=_EventObjects; self._event_records_type=_EventRecords
        self.business=business; self.receipt=receipt; self.guard=guard; self.dirty={}; self.reinserted=set()
        self._route_namespaces={mode:(mode+'_paths',mode+'_origins') for mode in ('detection',*business.features)}
        self._next_node=0 if receipt is None else receipt['next_shared_id']
        self.failed=False; self.installing=True; self._memo={}; self._roots={}; self._values={}; self._wrapping=0; self._retained=[]; self._bulk=0
        self._restored_ids={} if shared is None else {id(value):ident for ident,(_,value) in shared.items()}
        self._sync_roots()
        self._memo.clear(); self._restored_ids.clear(); self._retained.clear(); self.installing=False; self.dirty.clear(); self.reinserted.clear(); self._fixed_keys={(n,k) for n,k,_ in self.fixed_rows()}

    def mark(self, namespace, key, *, deleted=False, inserted=False):
        if self.installing: return
        if self.failed: raise RuntimeError('失败候选必须从已确认检查点重建')
        pair=namespace,key
        if inserted and self.dirty.get(pair) == 'delete':
            self.reinserted.add(pair); self.dirty.pop(pair)
        self.dirty[pair] = 'delete' if deleted else 'put'

    def mark_route(self, mode, prefix):
        routes=self.business.detection_routes if mode=='detection' else self.business.features[mode]
        paths_namespace,origins_namespace=self._route_namespaces[mode]
        for namespace,values in ((paths_namespace,routes.prefix_dict),(origins_namespace,routes.prefix_as)):
            present=prefix in values
            self.mark(namespace,prefix,deleted=not present,inserted=present)

    def mark_count(self, mode, country, asn):
        work=self.business.work[mode]
        self.mark(mode+'_counts',(country,asn),deleted=asn not in work.feature_dict.get(country,{}))

    def begin(self):
        if self.failed: raise RuntimeError('失败候选必须从已确认检查点重建')
        if self.dirty: raise RuntimeError('业务上文件尚未确认')

    def committed(self, receipt):
        self.receipt=receipt; self.dirty.clear(); self.reinserted.clear()

    def failed_file(self): self.failed=True

    def wrap(self, value, owners):
        # 标量、PathText和只读值没有可追踪的容器写入；不进入递归/memo管理。
        # _Node均继承dict/list/set，tuple仍需递归发现其中的共享可变容器。
        if not isinstance(value,_CONTAINERS): return value
        if isinstance(value,_Node) and value._owners is owners: return value
        self._wrapping += 1
        try: return self._wrap(value, owners)
        finally:
            self._wrapping -= 1
            if not self.installing and not self._wrapping and not self._bulk: self._memo.clear()

    def _wrap(self, value, owners):
        owners=tuple(owners)
        if isinstance(value, _Node):
            fresh=tuple(owner for owner in owners if owner not in value._owners)
            if fresh:
                value._owners+=fresh
                if not self.installing: value._changed()
                children=dict.values(value) if isinstance(value,dict) else value
                for child in children: self.wrap(child,fresh)
            return value
        if isinstance(value, tuple): return tuple(self.wrap(v,owners) for v in value)
        if not isinstance(value,_MUTABLES): return value
        old=self._memo.get(id(value))
        if old is not None:
            return self.wrap(old,owners)
        if isinstance(value,dict):
            cls=_subclass(_Dict,type(value)); result=cls.__new__(cls)
            if type(value) is not dict and hasattr(value,'__dict__'): result.__dict__.update(vars(value))
            result._changes=self; result._owners=owners; result._state_id=self._next_node; self._next_node+=1; self._memo[id(value)]=result
            for key,child in dict.items(value):
                dict.__setitem__(result,key,self.wrap(child,owners) if isinstance(child,_CONTAINERS) else child)
        elif isinstance(value,list):
            result=_List(); result._changes=self; result._owners=owners; result._state_id=self._next_node; self._next_node+=1; self._memo[id(value)]=result
            list.extend(result,(self.wrap(child,owners) if isinstance(child,_CONTAINERS) else child for child in value))
        else:
            result=_Set(value); result._changes=self; result._owners=owners; result._state_id=self._next_node; self._next_node+=1; self._memo[id(value)]=result
        if self._restored_ids and id(value) in self._restored_ids: result._state_id=self._restored_ids[id(value)]
        if self._next_node%16384==0: self.guard()
        return result

    def _bind_event_record(self, root, key, records):
        if not isinstance(records,self._event_records_type):
            raise TypeError('事件根的子容器不是修订记录索引')
        # 原touch闭包可能捕获包装前的整棵事件树；只绑定当前根和当前位置。
        # 不写dirty、不改order，读出/写入时才继续原修订通知语义。
        records.touch=lambda ident,owner=root,object_key=key: owner.dirty.add((object_key,ident))

    def _root(self, obj, attr, namespace, entry=False):
        value=getattr(obj,attr)
        self._retained.append(value)
        old=self._roots.get((id(obj),attr))
        if old is value: return
        if old is not None:
            for key in old: self.mark(namespace,old._key(key),deleted=True)
        cls=_subclass(_Root,type(value)); result=cls.__new__(cls)
        if hasattr(value,'__dict__'): result.__dict__.update(vars(value))
        result._changes=self; result._namespace=namespace
        result._event_root=isinstance(value,self._event_objects_type)
        result._key=(lambda key:('$entry',key)) if entry else (lambda key:key)
        self._roots[id(obj),attr]=result
        for key,child in dict.items(value):
            owner=namespace,result._key(key)
            wrapped=self.wrap(child,(owner,))
            dict.__setitem__(result,key,wrapped)
            if result._event_root: self._bind_event_record(result,key,wrapped)
            if not self.installing: self.mark(*owner,inserted=True)
        setattr(obj,attr,result)
        # StreamingDetectionEngine 的修订队列必须继续指向同一个根。
        if self.business.engine is not None:
            self.business.engine._event_indexes=[(kind,result if index is value else index)
                                                 for kind,index in self.business.engine._event_indexes]

    def _sync_roots(self):
        self._bulk+=1
        try: self._sync_roots_inner()
        finally:
            self._bulk-=1
            if not self._bulk and not self.installing:
                self._memo.clear(); self._retained.clear()

    def _sync_roots_inner(self):
        b=self.business
        for mode,q in b.feature_quality.items():
            self._root(q,'gaps',mode+'_gaps'); self._root(q,'qualities',mode+'_qualities')
        q=b.detection_quality
        self._root(q,'gaps','detection_gaps'); self._root(q,'events','detection_event_quality')
        if b.engine is None: return
        for name in ('outage','hijack','subhijack','leak','output'):
            obj=getattr(b.engine,name)
            for attr,value in list(vars(obj).items()):
                if attr in EXCLUDED or name=='output' and attr in ('max_record_bytes','failed'): continue
                namespace=name+':'+attr
                if isinstance(value,dict):
                    self._root(obj,attr,namespace,entry=True)
                else:
                    old=self._roots.pop((id(obj),attr),None)
                    if old is not None:
                        for key in old: self.mark(namespace,old._key(key),deleted=True)
                    if isinstance(value,(list,set,tuple)):
                        wrapped=self.wrap(value,((namespace,'$value'),))
                        setattr(obj,attr,wrapped)

    def fixed_rows(self):
        """只有有限的运行/模块元数据；不扫描路由、ASN或活动事件。"""
        b=self.business
        from data_pipeline.bgp.state.business import VERSION
        yield 'meta','run',dict(version=VERSION,detection_enabled=b.detection_enabled,completed=b.completed,baseline_count=b.baseline_count,input_gaps=[asdict(g) for g in b.input_gaps])
        for name,r in [('detection',b.detection_routes),*b.features.items()]:
            yield name+'_projection','meta',dict(t=r.t,vps=r.vp_set)
        for mode,w in b.work.items():
            yield mode+'_work','meta',{k:v for k,v in vars(w).items() if k not in ('projection','feature_dict','feature_collect_dict')}
            yield mode+'_work','collect',asdict(w.feature_collect_dict)
            yield mode+'_work','projection',dict(version=w.projection.version,coverage=w.projection.coverage)
            q=b.feature_quality[mode]
            yield mode+'_quality','meta',{k:getattr(q,k) for k in ('history_unknown','previous_end')}
        q=b.detection_quality
        yield 'detection_quality','meta',dict(position=q.position,source_id=q.source_id,count=q.count)
        e=b.engine
        if e is None: return
        yield 'engine','meta',dict(status=e.status,processed=e.processed,last_observation=e.last_observation,seen=e.seen_observations.position,baseline_count=e.baseline_count)
        for name in ('outage','hijack','subhijack','leak','output'):
            for attr,value in vars(getattr(e,name)).items():
                if attr in EXCLUDED or name=='output' and attr in ('max_record_bytes','failed'): continue
                yield name+':'+attr,('$mapping' if isinstance(value,dict) else '$value'),(None if isinstance(value,dict) else value)

    def baseline_rows(self):
        """初次完整状态；路径只读编号，不调用会解引用文本的 PrefixSlots.items。"""
        b=self.business; fixed={(n,k):v for n,k,v in self.fixed_rows()}
        yield 'meta','run',fixed['meta','run']
        for name,r in [('detection',b.detection_routes),*b.features.items()]:
            yield name+'_projection','meta',fixed[name+'_projection','meta']
            for prefix,paths in dict.items(r.prefix_dict): yield name+'_paths',prefix,paths.ids
            for prefix,origins in dict.items(r.prefix_as): yield name+'_origins',prefix,origins
        for mode,w in b.work.items():
            for key in ('meta','collect','projection'): yield mode+'_work',key,fixed[mode+'_work',key]
            for country,asns in w.feature_dict.items():
                for asn,value in asns.items(): yield mode+'_counts',(country,asn),asdict(value)
            yield mode+'_quality','meta',fixed[mode+'_quality','meta']
            q=b.feature_quality[mode]
            for key,value in dict.items(q.gaps): yield mode+'_gaps',key,value
            for key,value in dict.items(q.qualities): yield mode+'_qualities',key,value
        yield 'detection_quality','meta',fixed['detection_quality','meta']
        for key,value in dict.items(b.detection_quality.gaps): yield 'detection_gaps',key,value
        for key,value in dict.items(b.detection_quality.events): yield 'detection_event_quality',key,value
        if b.engine is None: return
        yield 'engine','meta',fixed['engine','meta']
        for name in ('outage','hijack','subhijack','leak','output'):
            for attr,value in vars(getattr(b.engine,name)).items():
                if attr in EXCLUDED or name=='output' and attr in ('max_record_bytes','failed'): continue
                namespace=name+':'+attr
                if isinstance(value,dict):
                    yield namespace,'$mapping',None
                    for key,row in dict.items(value): yield namespace,('$entry',key),row
                else: yield namespace,'$value',value

    def prepare(self):
        self._sync_roots(); fixed=list(self.fixed_rows())
        self._values={(n,k):v for n,k,v in fixed}
        for n,k in self._fixed_keys-self._values.keys(): self.mark(n,k,deleted=True)
        self._fixed_keys=set(self._values)
        for n,k,_ in fixed: self.mark(n,k)

    def value(self, namespace, key):
        pair=namespace,key
        if pair in self._values: return self._values[pair]
        b=self.business
        if ':' in namespace:
            if key in ('$mapping','$value'): return MISSING
            name,attr=namespace.split(':',1); mapping=getattr(getattr(b.engine,name),attr)
            return dict.get(mapping,key[1],MISSING) if isinstance(mapping,dict) and key[0]=='$entry' else MISSING
        if namespace=='detection_gaps': return dict.get(b.detection_quality.gaps,key,MISSING)
        if namespace=='detection_event_quality': return dict.get(b.detection_quality.events,key,MISSING)
        mode,kind=namespace.rsplit('_',1)
        if kind in ('paths','origins'):
            r=b.detection_routes if mode=='detection' else b.features[mode]
            values=r.prefix_dict if kind=='paths' else r.prefix_as
            value=dict.get(values,key,MISSING)
            return value.ids if kind=='paths' and value is not MISSING else value
        if kind=='counts':
            value=b.work[mode].feature_dict.get(key[0],{}).get(key[1],MISSING)
            return asdict(value) if value is not MISSING else MISSING
        if kind in ('gaps','qualities'): return dict.get(getattr(b.feature_quality[mode],kind),key,MISSING)
        raise ValueError('未知业务变化命名空间 '+namespace)


def install_changes(business, receipt=None, shared=None, guard=lambda:None):
    old=getattr(business,'changes',None)
    if old is not None:
        if receipt is not None: old.receipt=receipt
        return old
    business.changes=BusinessChanges(business,receipt,shared,guard)
    return business.changes


def encode_state(value):
    """沿用业务编码；只有跨持久化行共享的可变容器增加稳定别名编号。"""
    from datetime import datetime
    from data_pipeline.bgp.state.business import pack
    from data_pipeline.bgp.state.path_dictionary import PathText
    from data_pipeline.bgp.replay.snapshot_contract import _pack
    import json

    def encoded(item, body=False):
        # 精确标量沿原 _pack 原样输出；Enum/标量子类及浮点仍走原合同。
        if item is None or type(item) in (str,int,bool): return item
        if isinstance(item, _Node) and len(item._owners)>1 and not body:
            kind='dict' if isinstance(item,dict) else 'list' if isinstance(item,list) else 'set'
            return {'dict': [['$shared', {'tuple': [kind,item._state_id,encoded(item,True)]}]]}
        if isinstance(item,PathText): return {'dict': [['$path_text',item.ident]]}
        if isinstance(item,datetime): return {'dict': [['$instant',item.isoformat()]]}
        if isinstance(item,Mapping):
            return {'dict': [['$ordered_map', [
                {'tuple': [encoded(key),encoded(child)]} for key,child in (dict.items(item) if isinstance(item,dict) else item.items())]]]}
        if isinstance(item,(set,frozenset)): return _pack(pack(item))
        if isinstance(item,list): return [encoded(child) for child in item]
        if isinstance(item,tuple): return {'tuple': [encoded(child) for child in item]}
        return _pack(item)
    return json.dumps(encoded(value),ensure_ascii=False,separators=(',',':'),allow_nan=False)


def unpack_state(value, pool, shared):
    """同一编号恢复到同一对象；重复正文不一致立即拒绝。"""
    from datetime import datetime
    from data_pipeline.bgp.replay.snapshot_contract import digest
    if isinstance(value,dict):
        if set(value)=={'$shared'}:
            kind,ident,raw=value['$shared']; signature=digest(raw)
            if type(ident) is not int or ident<0 or kind not in ('dict','list','set'):
                raise ValueError('共享业务容器编号无效')
            if ident in shared:
                previous,item=shared[ident]
                if previous!=signature: raise ValueError('共享业务容器正文冲突')
                return item
            item=unpack_state(raw,pool,shared)
            expected={'dict':dict,'list':list,'set':set}[kind]
            if not isinstance(item,expected): raise ValueError('共享业务容器种类不符')
            shared[ident]=signature,item
            return item
        if set(value)=={'$path_text'}: return pool.by_id(value['$path_text'])
        if set(value)=={'$instant'}: return datetime.fromisoformat(value['$instant'])
        if set(value)=={'$ordered_map'}: return {unpack_state(k,pool,shared):unpack_state(v,pool,shared) for k,v in value['$ordered_map']}
        if set(value)=={'$set'}: return {unpack_state(v,pool,shared) for v in value['$set']}
        if set(value)=={'$frozen_set'}: return frozenset(unpack_state(v,pool,shared) for v in value['$frozen_set'])
        raise ValueError('未知业务恢复值标签')
    if isinstance(value,list): return [unpack_state(v,pool,shared) for v in value]
    if isinstance(value,tuple): return tuple(unpack_state(v,pool,shared) for v in value)
    if isinstance(value,set): return {unpack_state(v,pool,shared) for v in value}
    return value
