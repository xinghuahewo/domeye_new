"""同一条有序观察驱动既有 Feature/Detection；独立语义索引共享路径文本编号。"""
from collections.abc import Mapping
from contextlib import nullcontext
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timezone
from functools import lru_cache
from itertools import groupby
from ipaddress import ip_network
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import json
import math

from data_pipeline.analysis.features.calculation import Reference, ReferenceRow, Projection, Observation, WorkState, Values, AsnValues, FileWindow, InputGap, initialize, consume_file, count_observation, count_accepted, Diagnostic, _feature_time, observation_filter_fields, prefix_filter, RULES
from data_pipeline.analysis.features.qualification import Qualification as FeatureQualification
from data_pipeline.analysis.detection.models import ReferenceBundle, DetectionScope, DetectionSeed, FileBoundary
from data_pipeline.analysis.detection.engine import _local
from data_pipeline.analysis.detection.streaming import StreamingDetectionEngine, _EventObjects
from data_pipeline.analysis.detection.projection import DetectionProjection, legacy_origin as detection_origin
from data_pipeline.analysis.detection.adapter import adapt_element
from data_pipeline.analysis.detection.qualification import Qualification as DetectionQualification
from data_pipeline.bgp.record_types import SourceStart, SourceEnd, MessageBoundary, Element, SourceQuality
from data_pipeline.bgp.state.path_dictionary import PrefixSlots, SlotPaths, PathText, vp_text
from data_pipeline.bgp.replay.route_replay import legacy_origin, identity

VERSION='direct-existing-business/v1'


def encode_result(value):
    """等同 encode(plain(value))，在落盘边界一次冻结及编码业务结果。"""
    from data_pipeline.analysis.detection._results import plain
    from data_pipeline.bgp.replay.snapshot_contract import _pack, _key_order

    def encoded(item):
        # 结果的大部分节点是标量；保持 Enum 和标量子类沿原编码合同处理。
        if item is None or type(item) in (str, int, bool):
            return item
        if isinstance(item, PathText):
            return item.text
        if isinstance(item, bytes):
            return {'dict': [['$bytes', item.hex()]]}
        if isinstance(item, float) and not math.isfinite(item):
            return {'dict': [['$float', str(item)]]}
        if isinstance(item, Mapping):
            if any(not isinstance(key, str) for key in item):
                return _pack(plain(item))
            return {'dict': [[_pack(key), encoded(item[key])]
                             for key in sorted(item, key=_key_order)]}
        if isinstance(item, (list, tuple)):
            return [encoded(entry) for entry in item]
        if isinstance(item, datetime):
            return {'dict': [['$datetime', item.isoformat()]]}
        # 集合的旧排序规则和其他类型仍由原函数判定。
        return _pack(plain(item))

    return json.dumps(encoded(value), ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def pack(value):
    """恢复用有序值编码；路径保存编号，时间/集合不退化成字符串或列表。"""
    if isinstance(value,PathText):return {'$path_text':value.ident}
    if isinstance(value,datetime):return {'$instant':value.isoformat()}
    if isinstance(value,Mapping):return {'$ordered_map':[(pack(k),pack(v)) for k,v in value.items()]}
    if isinstance(value,frozenset):return {'$frozen_set':sorted((pack(v) for v in value),key=repr)}
    if isinstance(value,set):return {'$set':sorted((pack(v) for v in value),key=repr)}
    if isinstance(value,list):return [pack(v) for v in value]
    if isinstance(value,tuple):return tuple(pack(v) for v in value)
    return value


def encode_business(value):
    """生成与 encode(pack(value)) 相同的正文，避免普通容器的两遍递归复制。"""
    from data_pipeline.bgp.replay.snapshot_contract import _pack

    def encoded(item):
        if isinstance(item, PathText):
            return {'dict': [['$path_text', item.ident]]}
        if isinstance(item, datetime):
            return {'dict': [['$instant', item.isoformat()]]}
        if isinstance(item, Mapping):
            return {'dict': [['$ordered_map', [
                {'tuple': [encoded(key), encoded(value)]} for key, value in item.items()
            ]]]}
        if isinstance(item, (set, frozenset)):
            # 复用旧 repr 排序合同；不让散列枚举差异改写恢复正文。
            return _pack(pack(item))
        if isinstance(item, list):
            return [encoded(value) for value in item]
        if isinstance(item, tuple):
            return {'tuple': [encoded(value) for value in item]}
        return _pack(item)

    return json.dumps(encoded(value), ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def unpack(value,pool):
    if isinstance(value,dict):
        if set(value)=={'$path_text'}:return pool.by_id(value['$path_text'])
        if set(value)=={'$instant'}:return datetime.fromisoformat(value['$instant'])
        if set(value)=={'$ordered_map'}:return {unpack(k,pool):unpack(v,pool) for k,v in value['$ordered_map']}
        if set(value)=={'$set'}:return {unpack(v,pool) for v in value['$set']}
        if set(value)=={'$frozen_set'}:return frozenset(unpack(v,pool) for v in value['$frozen_set'])
        raise ValueError('未知业务恢复值标签')
    if isinstance(value,set):return {unpack(v,pool) for v in value}
    if isinstance(value,list):return [unpack(v,pool) for v in value]
    if isinstance(value,tuple):return tuple(unpack(v,pool) for v in value)
    return value


class SharedProjection(Projection):
    """仅供本单写入者候选使用；窗口产物立即落盘，不对外提供可变历史投影。"""
    def __deepcopy__(self,memo):return self


class DerivedRoutes(DetectionProjection):
    """保留旧 VP=ASN 和旧起源残留，槽内只保存共享文本编号。"""
    def __init__(self,pool):
        super().__init__();self.pool=pool;self.prefix_dict=PrefixSlots(pool)
        self.change_sink=None;self.coverage=None;self._coverage_before={}
        import pytricia
        self.ipv4_tree,self.ipv6_tree=pytricia.PyTricia(32),pytricia.PyTricia(128)

    def mark_changed(self,prefix):
        if self.coverage is not None:
            self._coverage_before[prefix]={asn for asn in self.prefix_as.get(prefix,())
                                           if prefix in self.as_prefix.get(asn,())}

    def finish_changed(self,prefix):
        # 持久化记录修改后的存在性，才能区分删除和同文件内重新插入。
        if self.change_sink is not None:self.change_sink(prefix)
        if self.coverage is not None:
            before=self._coverage_before.pop(prefix)
            after={asn for asn in self.prefix_as.get(prefix,()) if prefix in self.as_prefix.get(asn,())}
            self.coverage.sync_prefix(prefix,before,after)

    def refresh_tree(self,prefix):
        tree=self.ipv6_tree if ':' in prefix else self.ipv4_tree
        if prefix in self.prefix_dict:
            # 树只持有前缀关系和同一个起源集合的引用。
            self.tree_origins[prefix]=self.prefix_as.get(prefix,set())
            tree[prefix]=self.tree_origins[prefix]
        else:
            self.tree_origins.pop(prefix,None)
            if tree.has_key(prefix):del tree[prefix]

    def apply(self,action,prefix,vp,path):
        self.mark_changed(prefix)
        super().apply(action,prefix,vp,path);self.refresh_tree(prefix)
        self.finish_changed(prefix)

    def add_rib(self, prefix, values):
        """仅建基线；保持原始 VP 覆盖顺序和起源集合逐次插入顺序。"""
        slots = None
        for vp, ident, origin in values:
            self.vp_set.add(vp)
            if slots is None:
                slots = self.prefix_dict.setdefault(prefix, {})
            slots.ids[vp] = ident
            # 不以排序或集合并集重排旧算法可能枚举的起源集合。
            self.prefix_as.setdefault(prefix, set()).add(origin)
            self.as_prefix.setdefault(origin, set()).add(prefix)
        if slots is not None:
            self.refresh_tree(prefix)

    def at(self,prefix):
        paths=self.prefix_dict.get(prefix)
        return dict(origins=self.prefix_as.get(prefix,set()).copy(),
                    vp_paths={} if paths is None else paths.snapshot())

    def export(self):
        return dict(prefix_dict={p:{vp:str(path) for vp,path in paths.items()} for p,paths in self.prefix_dict.items()},
                    prefix_as=deepcopy(self.prefix_as),as_prefix=deepcopy(self.as_prefix),vp_set=set(self.vp_set),
                    t=self.t,tree_origins=deepcopy(self.tree_origins))


class FeatureRoutes(DerivedRoutes):
    def __init__(self,mode,reference,pool):super().__init__(pool);self.mode=mode;self.reference=reference
    def observe_fields(self,action,prefix,vp,path):
        from data_pipeline.bgp.state.update_fields import observe_feature_fields
        return observe_feature_fields(self,action,prefix,vp,path)
    def observe_rib_batch(self, prefix, stamp, values):
        prefix, _ = prefix_filter(prefix)
        if prefix is None or prefix.endswith('/0'):
            return
        if not self.t:
            self.t = _local(stamp.isoformat())
        self.add_rib(prefix, ((vp, ident, origin) for vp, ident, origin in values
                            if origin and (self.mode != 'ir' or self.reference.code(origin) == 'IR')))

    def observe(self,row,sequence,*,retain_rib_observation=True,emit_observation=True):
        action=row['action'];prefix,_=prefix_filter(row['prefix']);vp=str(row['peer_asn']);text=row['as_path_text']
        # RIB 只建立候选基线；主循环不使用这类记录的统计 Observation。
        # 独立调用默认仍返回完整观察，UPDATE 始终保留紧邻的 before/after。
        retain=action!='rib_snapshot' or retain_rib_observation
        stamp=datetime.fromtimestamp(row['epoch'],timezone.utc).replace(microsecond=row.get('microsecond') or 0) if retain and emit_observation else None
        paths=self.prefix_dict.get(prefix,{}) if prefix is not None else {}
        before=self.prefix_as.get(prefix,set()) if prefix is not None else set()
        flag='A' if action=='announce' else 'W'
        old_path=paths.get(vp,'') if retain else None
        before_origins=frozenset(before) if retain and emit_observation else None
        origin=legacy_origin(text);skip=projection_skip=None
        if action=='rib_snapshot':
            if prefix is None or prefix.endswith('/0'):skip='legacy_invalid_or_default_route'
            else:
                if not self.t:
                    first=stamp or datetime.fromtimestamp(row['epoch'],timezone.utc).replace(microsecond=row.get('microsecond') or 0)
                    self.t=_local(first.isoformat())
                if not origin:skip='legacy_empty_origin'
                elif self.mode=='ir' and self.reference.code(origin)!='IR':skip='non_ir_rib'
        else:
            skip=observation_filter_fields(self.mode,flag,row['prefix'],text,self.reference);origin=origin if origin is not None else ''
            if not skip and action=='announce' and self.mode=='ir' and origin and self.reference.code(origin)!='IR':
                projection_skip='non_ir_bgprib_origin'
        if not skip and not projection_skip:
            self.mark_changed(prefix)
            if action=='withdraw':
                if vp in paths:
                    removed=legacy_origin(paths.pop(vp));removed=removed if removed is not None else ''
                    remaining={legacy_origin(p) if legacy_origin(p) is not None else '' for p in paths.values()}
                    if removed not in remaining:
                        self.prefix_as.get(prefix,set()).discard(removed);self.as_prefix.get(removed,set()).discard(prefix)
                        if not self.as_prefix.get(removed):self.as_prefix.pop(removed,None)
                    if not paths:self.prefix_dict.pop(prefix,None)
                    if not self.prefix_as.get(prefix):self.prefix_as.pop(prefix,None)
            else:
                self.vp_set.add(vp);self.prefix_dict.setdefault(prefix,{})[vp]=text
                self.prefix_as.setdefault(prefix,set()).add(origin);self.as_prefix.setdefault(origin,set()).add(prefix)
            self.refresh_tree(prefix)
            self.finish_changed(prefix)
        if retain:
            if not emit_observation:return old_path,skip
            return Observation(row['event_id'],sequence,stamp,flag,row['prefix'],vp,text,old_path,
                               before_origins,frozenset(self.prefix_as.get(prefix,set())),RULES[self.mode],skip)


class OrderedSeen:
    """上游严格源序已验证；只保留最后位置，替代全部观察 ID 集合。"""
    def __init__(self,sources):self.ranks={s:i for i,s in enumerate(sources)};self.position=(-1,-1,-1)
    def key(self,ref):
        source,record,ordinal=ref.rsplit(':',2);return self.ranks[source],int(record),int(ordinal)
    def __contains__(self,ref):return self.key(ref)<=self.position
    def add(self,ref):
        position=self.key(ref)
        if position<=self.position:raise ValueError('重复或倒序的业务观察')
        self.position=position

    def advance(self,source,record,ordinal):
        position=(self.ranks[source],record,ordinal)
        if position<=self.position:raise ValueError('重复或倒序的业务观察')
        self.position=position


class SharedReferences:
    def __init__(self,bundle):self.bundle=bundle
    def __getattr__(self,name):return getattr(self.bundle,name)
    def metadata(self):return replace(self.bundle,mappings={})
    def take_info(self):
        mappings=self.bundle.mappings
        fields=('import_as','export_as','v4Peer','v6Peer','sibling_as')
        # 原算法会解释字符串列表；只复制需解释的行，不改调用方的原参考，也不按模块复制整套资料。
        as_info={key:dict(row) if any(isinstance(row.get(f),str) for f in fields) else row
                 for key,row in mappings['as_info'].items()}
        from data_pipeline.analysis.detection._leak import as_relationship
        # 本单写入者的参考在配置绑定后只读。缓存方向和原值类型，审计仍逐次经过 rule。
        lookup = lru_cache(maxsize=8192, typed=True)(
            lambda left, right: as_relationship(mappings['as_rel_dict'], left, right))
        return SimpleNamespace(**{**mappings,'as_info':as_info,'relationship_lookup':lookup})


class BusinessLoop:
    def __init__(self,config,plan,binding,pool,sink,*,raw_sink=None,metrics=None):
        self.measure=metrics.measure if metrics is not None else lambda name:nullcontext()
        self.config=config;self.plan=plan;self.binding=binding;self.pool=pool
        from data_pipeline.analysis.detection._results import plain
        # raw_sink 必须在返回前冻结正文；文件写入端用一次编码代替两遍递归转换。
        self.sink=raw_sink if raw_sink is not None else lambda table,row:sink(table,plain(row))
        self.sources=(plan['manifest']['baseline_source'],*plan['manifest']['update_sources'])
        if set(config['windows'])!=set(self.sources):raise ValueError('业务窗口必须恰好绑定基线和全部 UPDATE')
        self.reference=Reference(**{**config['feature_reference'],'source_rows':tuple(ReferenceRow(**r) for r in config['feature_reference'].get('source_rows',[]))})
        self.detection_enabled=config.get('detection_enabled',True)
        if type(self.detection_enabled) is not bool:raise ValueError('detection_enabled 必须为布尔值')
        self.bundle=ReferenceBundle(**config['detection_reference']) if self.detection_enabled else None
        self.scope=DetectionScope(binding['observation_run'],'r',plan['manifest']['collector'],binding['binding_id'],
            plan['manifest']['window_start'],plan['manifest']['window_end_exclusive'])
        modes=tuple(config.get('feature_modes',('ordinary','ir')))
        if not modes or len(set(modes))!=len(modes) or any(m not in RULES for m in modes):raise ValueError('Feature 模式无效')
        self.features={mode:FeatureRoutes(mode,self.reference,pool) for mode in modes};self.work={}
        self.feature_quality={mode:FeatureQualification(mode,self) for mode in modes}
        self.detection_routes=DerivedRoutes(pool);self.engine=None
        self.count_callbacks={mode:(lambda country,asn,mode=mode:self.count_changed(mode,country,asn))
                              for mode in modes}
        for mode,routes in [('detection',self.detection_routes),*self.features.items()]:
            routes.change_sink=lambda prefix,mode=mode:self.route_changed(mode,prefix)
        self.detection_quality=DetectionQualification(SimpleNamespace(binding_id=binding['binding_id']),self.scope,
            binding['observation_run'],lambda row:self.sink('detection_qualification',row))
        self.input_gaps=();self.current=None;self.completed=0;self.boundary=None;self.sequence=0;self.baseline_count=0
        self._prepared_boundary=None
        self.rib_time=lru_cache(maxsize=8)(lambda epoch:datetime.fromtimestamp(epoch,timezone.utc))
        # 路径解释只依赖原文本；缓存受条数及文本长度限制，不保存额外路由负载。
        self._rib_details=lru_cache(maxsize=8192)(lambda text:(self.pool.ident(text),legacy_origin(text),detection_origin(text) if self.detection_enabled else None))
        self.windows={s:FileWindow(binding['binding_id'],s,*(datetime.fromisoformat(config['windows'][s][k].replace('Z','+00:00')) for k in ('start','end','file_time')),
            config['windows'][s].get('coverage','unknown')) for s in self.sources}
        for left,right in zip(self.sources[1:],self.sources[2:]):
            if self.windows[left].end>self.windows[right].start:raise ValueError('业务 UPDATE 窗口重叠')

    def append(self,table,row):self.sink('feature_'+table,row)

    def route_changed(self,mode,prefix):
        changes=getattr(self,'changes',None)
        if changes is not None:changes.mark_route(mode,prefix)

    def count_changed(self,mode,country,asn):
        changes=getattr(self,'changes',None)
        if changes is not None:changes.mark_count(mode,country,asn)
        coverage=self.features[mode].coverage
        if coverage is not None:coverage.ensure_asn(asn,country)

    def rebuild_coverage(self):
        if not self.config.get('incremental_coverage',True):return
        from data_pipeline.analysis.features.coverage import FeatureCoverage
        for mode,routes in self.features.items():
            if mode=='ordinary' and mode in self.work:
                routes.coverage=FeatureCoverage(self.reference,routes,self.work[mode].feature_dict)

    def projection(self,mode,source,coverage):
        routes=self.features[mode]
        return SharedProjection(identity([VERSION,self.binding['binding_id'],mode,source]),RULES[mode],routes.prefix_dict,
            routes.prefix_as,routes.as_prefix,frozenset(routes.vp_set),self.sources[0],routes.t,'IR' if mode=='ir' else None,coverage,self.input_gaps)

    def make_engine(self,projection):
        engine=StreamingDetectionEngine(DetectionSeed((),self.sources[0]),SharedReferences(self.bundle),self.scope,
            sink=self.output,projection=projection,audit=self.config.get("audit_detection",False),
            defer_input_evidence=True)
        engine.seen_observations=OrderedSeen(self.sources)
        return engine

    def output(self,row):
        self.sink('detection_result',row);self.detection_quality.observe_output(row)

    def apply_rib_batch(self, batch):
        """RIB 无 UPDATE 计数或活动检测；按原序建立三个必要业务索引。"""
        if (self.current != self.sources[0] or self.engine is not None
                or self.boundary != batch.boundary or self.boundary.raw['kind'] != 'rib'
                or batch.binding_ref != self.binding['binding_id']):
            raise ValueError('业务 RIB 列批不属于当前候选基线')
        stamp = self.rib_time(batch.boundary.raw['epoch'])
        if not self.windows[self.current].start <= stamp < self.windows[self.current].end:
            raise ValueError('RIB 超出业务窗口')
        if len(self.sources) > 1 and stamp > self.windows[self.sources[1]].start:
            raise ValueError('RIB 晚于 UPDATE 切点')
        details = {key: (self._rib_details(path['as_path_text']) if len(path['as_path_text'])<=4096 else
                         (self.pool.ident(path['as_path_text']),legacy_origin(path['as_path_text']),detection_origin(path['as_path_text']) if self.detection_enabled else None))
                   for key,path in batch.paths.items()}
        # 同一 RIB 消息的前缀通常相同；仍按连续前缀分组，不全局重排或去重。
        rows = zip(batch.columns[2], batch.columns[6], batch.columns[7])
        for prefix, group in groupby(rows, key=lambda value: value[0]):
            values = [(vp_text(asn), details[key]) for _, asn, key in group]
            for routes in self.features.values():
                routes.observe_rib_batch(prefix, stamp, ((vp, item[0], item[1]) for vp, item in values))
            if self.detection_enabled and prefix not in ('0.0.0.0/0', '::/0'):
                if not self.detection_routes.t:
                    self.detection_routes.t = _local(stamp.isoformat())
                self.detection_routes.add_rib(prefix, ((vp, item[0], item[2]) for vp, item in values if item[2] != ''))
        # Feature 资格的 element 对 RIB 无操作；消息质量仍逐边界处理。
        self.sequence += len(batch)
        self.baseline_count += len(batch)

    def _prepare_update_message(self,item):
        stamp=datetime.fromtimestamp(item.raw['epoch'],timezone.utc).replace(microsecond=item.raw.get('microsecond') or 0)
        if not self.windows[self.current].start<=stamp<self.windows[self.current].end:
            raise ValueError('观察不属于声明窗口')
        self._feature_stamp=_feature_time(stamp,stamp.fold)
        if self.detection_enabled:
            if not self.engine.window[0]<=stamp<self.engine.window[1]:raise ValueError('UPDATE 在声明检测窗口之外')
            self._detection_stamp=_local(stamp.isoformat())
            self._detection_context=dict(run_id=self.binding['observation_run'],snapshot='candidate:'+self.binding['binding_id'],
                collector_id=self.scope.collector_id,quality=item.raw['quality'],source_version=self.binding['binding_id'])
        self._prepared_boundary=item

    def apply_update_row(self,row,boundary,ordinal):
        """直接批次的同步业务入口；上游已核验列与来源顺序，不构造中间观察对象。"""
        if self.boundary is not boundary or self.current!=row['source_id']:
            raise ValueError('直接业务行与消息边界不符')
        if self._prepared_boundary is not boundary:self._prepare_update_message(boundary)
        self.sequence+=1
        flag='A' if row['action']=='announce' else 'W'
        if row['action'] not in ('announce','withdraw'):raise ValueError('直接业务行不是 UPDATE')
        for mode,routes in self.features.items():
            old_path,reason=routes.observe(row,self.sequence-1,emit_observation=False)
            self.feature_quality[mode].element(row)
            if reason:
                diagnostic=Diagnostic('update',row['event_id'],reason,row['prefix'],prefix_filter(row['prefix'])[0])
                self.sink('feature_diagnostic',dict(mode=mode,raw=vars(diagnostic).copy()))
            else:
                work=self.work[mode];work.t=self._feature_stamp
                count_accepted(work,self.reference,flag,row['as_path_text'],old_path,on_change=self.count_callbacks[mode])
        if not self.detection_enabled:return
        # ElementPosition 的排序键在消息 rank/record 后固定插入元素标志。
        self.detection_quality.position=(boundary.position.source_rank,boundary.position.record,1,ordinal)
        self.engine.consume_fields(row,self._detection_stamp,self._detection_context)
        if self.engine.status in ('failed','partial'):raise ValueError('既有 Detection 规则计算失败，候选回滚')

    def apply(self,item):
        if isinstance(item,SourceStart):
            self.current=item.raw.source_id;self.sequence=0;self.boundary=None
            window=self.windows[self.current]
            if self.completed and self.windows[self.sources[self.completed-1]].end<window.start:
                previous=self.sources[self.completed-1]
                self.input_gaps+=(InputGap(previous,self.current,self.windows[previous].end,window.start),)
            if self.detection_enabled:self.detection_quality.source_id=self.current
            for mode,q in self.feature_quality.items():
                q.begin(self.completed,SimpleNamespace(source_id=self.current,window=window,
                    message_quality_state=self.config['windows'][self.current].get('message_quality_state','unknown')))
            if self.current!=self.sources[0]:
                if not self.work:raise ValueError('业务基线尚未确认')
                if self.detection_enabled:
                    if self.engine is None:raise ValueError('检测基线尚未确认')
                    self.engine.begin_file(FileBoundary(self.current,self.binding['binding_id'],window.file_time.isoformat(),
                        self.config['windows'][self.current]['legacy_tables']))
            return
        if isinstance(item,MessageBoundary):
            self.boundary=item
            if self.detection_enabled:self.detection_quality.boundary(item)
            for q in self.feature_quality.values():q.boundary(item)
            return
        if isinstance(item,SourceQuality):
            for q in self.feature_quality.values():q.quality(None,item.raw['code'],item.raw['detail'])
            return
        if isinstance(item,Element):
            row=item.raw;self.sequence+=1
            for mode,routes in self.features.items():
                observation=routes.observe(row,self.sequence-1,retain_rib_observation=False);self.feature_quality[mode].element(row)
                if row['action']!='rib_snapshot':
                    diagnostic=count_observation(self.work[mode],self.reference,self.windows[self.current],observation,
                                                 on_change=self.count_callbacks[mode])
                    # Diagnostic 只有值字段；sink 的 plain 已递归冻结，不先做一次 asdict 深拷贝。
                    if diagnostic:self.sink('feature_diagnostic',dict(mode=mode,raw=vars(diagnostic).copy()))
            if row['action']=='rib_snapshot':
                self.baseline_count+=1
                prefix=row['prefix'];stamp=self.rib_time(row['epoch'])
                if not self.windows[self.current].start<=stamp<self.windows[self.current].end:raise ValueError('RIB 超出业务窗口')
                if len(self.sources)>1 and stamp>self.windows[self.sources[1]].start:raise ValueError('RIB 晚于 UPDATE 切点')
                if self.detection_enabled and prefix not in ('0.0.0.0/0','::/0'):
                    if not self.detection_routes.t:self.detection_routes.t=_local(stamp.isoformat())
                    if detection_origin(row['as_path_text'])!='':self.detection_routes.apply('RIB',prefix,str(row['peer_asn']),row['as_path_text'])
            elif self.detection_enabled:
                self.detection_quality.position=item.position.sort_key
                decoded=adapt_element(row,run_id=self.binding['observation_run'],snapshot='candidate:'+self.binding['binding_id'],
                    collector_id=self.scope.collector_id,quality=self.boundary.raw['quality'],source_version=self.binding['binding_id'])
                self.engine.consume(decoded)
                if self.engine.status in ('failed','partial'):raise ValueError('既有 Detection 规则计算失败，候选回滚')
            return
        if not isinstance(item,SourceEnd):raise ValueError('未知业务有序输入')
        coverage=self.windows[self.current].coverage
        quality=self.config['windows'][self.current].get('message_quality_state','unknown')
        if quality not in ('unknown','partial','complete'):raise ValueError('消息质量声明无效')
        if quality=='unknown':coverage='unknown'
        elif quality=='partial' and coverage=='complete':coverage='partial'
        if coverage!='unknown' and (item.parse_counts.gaps or self.input_gaps):coverage='partial'
        if self.current==self.sources[0]:
            if self.detection_enabled:
                with self.measure('detection_baseline_initialize'):
                    self.engine=self.make_engine(self.detection_routes);self.engine.baseline_count=self.baseline_count
            with self.measure('feature_baseline_initialize'):
                for mode in self.features:self.work[mode]=initialize(mode,self.projection(mode,self.current,coverage),self.reference,self.binding['observation_run'])
            with self.measure('feature_coverage_rebuild'):self.rebuild_coverage()
        else:
            if self.detection_enabled:
                self.engine.finish_file()
                if self.engine.status in ('failed','partial'):raise ValueError('Detection 文件尾失败')
            for mode,q in self.feature_quality.items():
                scientific='unknown' if self.work[mode].projection.coverage=='unknown' or coverage=='unknown' else (
                    'partial' if self.work[mode].projection.coverage=='partial' or coverage=='partial' else 'complete')
                result=consume_file(self.work[mode],self.reference,self.windows[self.current],self.projection(mode,self.current,scientific),
                    coverage=self.features[mode].coverage,
                    # 前缀集合已冻结；同步sink负责正文冻结，不再深拷贝这些集合。
                    row_sink=lambda row:self.sink('feature_result',dict(mode=mode,source_id=self.current,
                        window=asdict(self.windows[self.current]),raw=dict(vars(row),values=vars(row.values).copy()))))
                for diagnostic in result.diagnostics:self.sink('feature_diagnostic',dict(mode=mode,raw=vars(diagnostic).copy()))
                self.sink('feature_window',dict(mode=mode,source_id=self.current,quality=result.quality,sparse=result.sparse_asns,
                    counts=asdict(result.end_counts)))
                self.work[mode]=result.next_state
        for q in self.feature_quality.values():q.finish(item)
        if self.detection_enabled:self.detection_quality.source_complete(asdict(item))
        self.completed+=1;self.current=None;self.boundary=None

    def rows(self):
        """逐对象导出恢复所需值；不复制完整路由、参考资料或全部观察 ID。"""
        if self.current is not None:raise ValueError('业务文件未结束')
        yield 'meta','run',dict(version=VERSION,detection_enabled=self.detection_enabled,completed=self.completed,baseline_count=self.baseline_count,input_gaps=[asdict(g) for g in self.input_gaps])
        for name,routes in [('detection',self.detection_routes),*((m,self.features[m]) for m in self.features)]:
            yield name+'_projection','meta',dict(t=routes.t,vps=routes.vp_set)
            for prefix,paths in routes.prefix_dict.items():yield name+'_paths',prefix,paths.ids
            for prefix,origins in routes.prefix_as.items():yield name+'_origins',prefix,origins
            # as_prefix 和树均可由明确持久化的 origins 索引恢复，不另存副本。
        for mode,work in self.work.items():
            yield mode+'_work','meta',{k:v for k,v in vars(work).items() if k not in ('projection','feature_dict','feature_collect_dict')}
            yield mode+'_work','collect',asdict(work.feature_collect_dict)
            yield mode+'_work','projection',dict(version=work.projection.version,coverage=work.projection.coverage)
            for country,asns in work.feature_dict.items():
                for asn,value in asns.items():yield mode+'_counts',(country,asn),asdict(value)
            q=self.feature_quality[mode]
            yield mode+'_quality','meta',{k:getattr(q,k) for k in ('history_unknown','previous_end')}
            for key,value in q.gaps.items():yield mode+'_gaps',key,value
            for key,value in q.qualities.items():yield mode+'_qualities',key,value
        q=self.detection_quality
        yield 'detection_quality','meta',dict(position=q.position,source_id=q.source_id,count=q.count)
        for key,value in q.gaps.items():yield 'detection_gaps',key,value
        for key,value in q.events.items():yield 'detection_event_quality',key,value
        if self.engine is None:return
        e=self.engine
        yield 'engine','meta',dict(status=e.status,processed=e.processed,last_observation=e.last_observation,
            seen=e.seen_observations.position,baseline_count=e.baseline_count)
        for name in ('outage','hijack','subhijack','leak','output'):
            obj=getattr(e,name)
            excluded={'output','bgp_info','bgp_rib','scope','references','sink','context','rows'}
            for attr,value in vars(obj).items():
                if attr in excluded or name=='output' and attr in ('max_record_bytes','failed'):continue
                if isinstance(value,dict):
                    yield name+':'+attr,'$mapping',None
                    for key,row in value.items():yield name+':'+attr,('$entry',key),row
                else:yield name+':'+attr,'$value',value

    def restore(self,rows):
        # 空引擎只建立算法对象，随后逐行恢复；不会用 UPDATE 末态重新初始化旧基线。
        self.engine=self.make_engine(DerivedRoutes(self.pool)) if self.detection_enabled else None;e=self.engine
        work_meta={};projection_meta={}
        for namespace,key,value in rows:
            if namespace=='meta':
                if value.get('detection_enabled',True)!=self.detection_enabled:raise ValueError('恢复的检测启用配置不一致')
                self.completed=value['completed'];self.baseline_count=value['baseline_count']
                self.input_gaps=tuple(InputGap(**g) for g in value['input_gaps']);continue
            if namespace=='engine':
                for name,item in value.items():
                    if name=='seen':e.seen_observations.position=item
                    else:setattr(e,name,item)
                continue
            if namespace=='detection_quality':
                for name,item in value.items():setattr(self.detection_quality,name,item)
                continue
            if namespace=='detection_gaps':self.detection_quality.gaps[key]=value;continue
            if namespace=='detection_event_quality':self.detection_quality.events[key]=value;continue
            if ':' in namespace:
                name,attr=namespace.split(':',1);obj=getattr(e,name)
                if key=='$mapping':setattr(obj,attr,{})
                elif key=='$value':setattr(obj,attr,value)
                else:getattr(obj,attr)[key[1]]=value
                continue
            name,kind=namespace.rsplit('_',1)
            routes=self.detection_routes if name=='detection' else self.features.get(name)
            if kind=='projection':routes.t=value['t'];routes.vp_set=value['vps']
            elif kind=='paths':routes.prefix_dict[key]=SlotPaths(self.pool,value)
            elif kind=='origins':routes.prefix_as[key]=value
            elif kind=='work':
                work_meta.setdefault(name,{})[key]=value
                if key=='meta':self.work[name]=WorkState(**value,projection=self.projection(name,self.sources[max(0,self.completed-1)],'unknown'))
            elif kind=='counts':self.work[name].feature_dict.setdefault(key[0],{})[key[1]]=AsnValues(**value)
            elif kind=='quality':
                for attr,item in value.items():setattr(self.feature_quality[name],attr,item)
            elif kind=='gaps':self.feature_quality[name].gaps[key]=value
            elif kind=='qualities':self.feature_quality[name].qualities[key]=value
            else:raise ValueError('未知业务工作态命名空间')
        for name,routes in [('detection',self.detection_routes),*self.features.items()]:
            for prefix,origins in routes.prefix_as.items():
                for origin in origins:routes.as_prefix.setdefault(origin,set()).add(prefix)
            for prefix in routes.prefix_dict:routes.refresh_tree(prefix)
            if name!='detection' and name in self.work:
                info=work_meta[name];self.work[name].feature_collect_dict=Values(**info['collect'])
                self.work[name].projection=replace(self.projection(name,self.sources[max(0,self.completed-1)],info['projection']['coverage']),version=info['projection']['version'])
        if e is None:
            if self.detection_routes.prefix_dict:raise ValueError('仅特征恢复不能包含检测路由')
            with self.measure('feature_coverage_restore'):self.rebuild_coverage()
            return
        e.projection=self.detection_routes
        for name in ('outage','hijack','subhijack','leak'):getattr(e,name).bgp_rib=e.projection
        e._event_indexes=[]
        for kind,module,attr in [('prefix_outage',e.outage,'prefix_outage_event'),('as_outage',e.outage,'as_outage_event'),
            ('country_outage',e.outage,'country_outage_event'),('moas',e.hijack,'moas_event_dict'),('sub_hijack',e.subhijack,'sub_hijack_dict')]:
            index=_EventObjects(getattr(module,attr));index.dirty.clear();setattr(module,attr,index);e._event_indexes.append((kind,index))
        with self.measure('feature_coverage_restore'):self.rebuild_coverage()
