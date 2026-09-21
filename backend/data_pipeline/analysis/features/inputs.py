"""Feature显式多观察视图；只读复用保存观察，不改上游角色或业务算法。"""
from dataclasses import asdict, dataclass, replace
from itertools import groupby
import psycopg2

from data_pipeline.analysis.features.calculation import FileWindow
from data_pipeline.analysis.features.projection import FeaturePlan, SourceBinding
from data_pipeline.analysis.features.reference import REFERENCE_RULE
from data_pipeline.bgp.archive.message_reader import ObservationReader
from data_pipeline.bgp.input.mrt_reader import source_identity
from data_pipeline.bgp.replay.route_replay import identity


@dataclass(frozen=True)
class SourceView:
    run_id: str
    snapshot: int
    source_id: str
    origin_uri: str
    content_sha256: str
    source_role: str
    calculation_role: str
    reference_sha256: str
    expected_messages: int
    expected_elements: int
    window: FileWindow
    message_quality_refs: tuple[str, ...] = ()
    message_quality_state: str = 'unknown'
    profile: str = 'complete'


@dataclass(frozen=True)
class ReferenceView:
    run_id: str
    snapshot: int
    source_sha256: str
    raw_path: str
    expected_rows: int | None = None
    profile: str = 'complete'


def _window_data(window):
    return {**asdict(window), 'start':window.start.isoformat(), 'end':window.end.isoformat(),
            'file_time':window.file_time.isoformat()}


class FeatureInputs:
    def __init__(self, dsn, collector, source_views, reference_view, *, result_window=None,
                 comparison_window=None, _legacy_plan=None, _readers=None):
        if not source_views: raise ValueError('缺少Feature有序来源')
        profiles={v.profile for v in source_views}
        if len(profiles)!=1 or not profiles <= {'complete','observation'} or reference_view.profile not in profiles:
            raise ValueError('Feature来源与参考必须显式使用同一受支持profile')
        self.profile=next(iter(profiles))
        self.dsn, self.collector, self.reference_view = dsn, collector, reference_view
        self.result_window, self.comparison_window = result_window, comparison_window
        for bounds in (result_window,comparison_window):
            if bounds is not None and (len(bounds)!=2 or any(t.tzinfo is None or t.utcoffset() is None for t in bounds) or bounds[0]>=bounds[1]):
                raise ValueError('交付/比较窗口必须为带时区半开区间')
        if comparison_window is not None and (result_window is None or comparison_window[1]!=result_window[0]):
            raise ValueError('比较窗口必须紧邻结果窗口之前')
        grouped={}
        for view in source_views:
            grouped.setdefault((view.run_id,view.snapshot),[])
            if view.source_id not in grouped[view.run_id,view.snapshot]:grouped[view.run_id,view.snapshot].append(view.source_id)
        self.readers={}
        for key,sources in grouped.items():
            existing=(_readers or {}).get(key)
            reader=existing or ObservationReader(dsn,*key,sources,profile=self.profile)
            if (reader.selection is not None)!=(self.profile=='observation'):raise ValueError('Reader实际profile与声明不符')
            if reader.sources!=tuple(sources):raise ValueError('原Reader来源范围不匹配')
            if reader.manifest.get('collector')!=collector:raise ValueError('Feature collector与固定观察manifest不一致')
            self.readers[key]=reader
        unique={};aliases={}
        for view in source_views:
            reader=self.readers[view.run_id,view.snapshot]
            entry=next(e for e in reader.manifest['inputs'] if e['source_id']==view.source_id)
            start=next(s for s in reader.starts if s.source_id==view.source_id)
            if (entry['origin_uri'],entry['sha256'],entry['role'])!=(view.origin_uri,view.content_sha256,view.source_role):
                raise ValueError('来源URI/SHA/原角色与固定观察不一致')
            if source_identity(collector,view.origin_uri,view.content_sha256)!=view.source_id:
                raise ValueError('来源真实身份不一致')
            if (view.expected_messages,view.expected_elements)!=(start.expected_messages,start.expected_elements):
                raise ValueError('声明消息/元素回执不一致')
            if view.reference_sha256!=reference_view.source_sha256:raise ValueError('跨参考版本解释冲突')
            if view.window.input_version!=view.run_id+':'+str(view.snapshot):raise ValueError('来源窗口必须绑定原run:snapshot')
            if view.calculation_role=='initial_rib':
                if view.source_role not in ('baseline','snapshot'):raise ValueError('私有初始基线必须来自原RIB角色')
            elif view.calculation_role!='update' or view.source_role!='update':
                raise ValueError('计算UPDATE必须是原update角色')
            if view.source_id in unique:
                previous=unique[view.source_id]
                # 原role可在不同观察run中为baseline/snapshot，但计算用途和时间不能改变。
                fields=('origin_uri','content_sha256','calculation_role','reference_sha256','expected_messages','expected_elements',
                        'message_quality_refs','message_quality_state')
                if any(getattr(previous,n)!=getattr(view,n) for n in fields) or replace(previous.window,input_version='alias')!=replace(view.window,input_version='alias'):
                    raise ValueError('同来源别名的用途/时间/回执冲突')
                aliases[view.source_id].append(view)
            else:
                unique[view.source_id]=view;aliases[view.source_id]=[view]
        self.views=tuple(unique.values())
        if self.views[0].calculation_role!='initial_rib' or any(v.calculation_role!='update' for v in self.views[1:]):
            raise ValueError('必须且只能有一个首位私有RIB基线')
        reference_key=(reference_view.run_id,reference_view.snapshot)
        if reference_key in self.readers:
            self.reference_reader=self.readers[reference_key]
        else:
            # ReferenceReader复用公共协议；显式保留其读取资格所依赖的原baseline锚点。
            if self.profile == 'observation':
                from data_pipeline.bgp.archive.selection import Selection
                anchor=Selection(dsn,*reference_key).manifest['baseline_source']
            else:
                with psycopg2.connect(dsn) as pg:
                    pg.set_session(readonly=True)
                    with pg.cursor() as c:
                        c.execute('SELECT manifest FROM domeye.run_specs WHERE run_id=%s',(reference_view.run_id,))
                        row=c.fetchone()
                        if row is None:raise ValueError('参考观察run不存在')
                        anchor=row[0]['baseline_source']
            self.reference_reader=ObservationReader(dsn,*reference_key,[anchor],profile=self.profile)
        self.bound_reference=self.reference_receipt()
        if reference_view.expected_rows is not None and reference_view.expected_rows!=self.bound_reference[1]:
            raise ValueError('参考行数声明与固定回执不一致')
        self.source_specs=[]
        for sequence,view in enumerate(self.views):
            record=self._source_record(view)
            record.update(sequence=sequence,window_role=self.window_role(view),aliases=[self._source_record(v) for v in aliases[view.source_id]])
            self.source_specs.append(record)
        reference_spec={'run_id':reference_view.run_id,'snapshot':reference_view.snapshot,
                        'carrier_collector':self.reference_reader.manifest.get('collector'),
                        'source_sha256':reference_view.source_sha256,'expected_rows':self.bound_reference[1],
                        'rule':REFERENCE_RULE,'historical_effectivity':'Unknown',
                        'reader_qualification_sources':list(self.reference_reader.sources)}
        if self.profile == 'observation':
            from data_pipeline.bgp.ordered_reader import binding_from_reader
            self.ordered_bindings={key:binding_from_reader(reader) for key,reader in self.readers.items()}
            for record in self.source_specs:
                for view in [record,*record['aliases']]:
                    bound=self.ordered_bindings[view['run_id'],view['snapshot']]
                    view['ordered_binding_ref']=bound.binding_id
                    view['upstream_source_rank']=bound.ordered_source_ids.index(view['source_id'])
            selection=self.reference_reader.selection
            reference_spec.update(profile=self.profile,seal_digest=selection.seal['digest'],
                                  checkpoint=next(cp for cp in selection.checkpoints if cp['source_id']==reference_view.source_sha256))
        self.reference_spec=reference_spec
        self.version=(_legacy_plan.observation_version if _legacy_plan else
                      identity(['feature-fixed-views/v1',collector,self.source_specs,reference_spec,self._bounds(result_window),self._bounds(comparison_window)]))
        bindings=tuple(SourceBinding(v.source_id,v.content_sha256,'baseline' if v.calculation_role=='initial_rib' else 'update',
                                     replace(v.window,input_version=self.version),v.expected_elements,
                                     v.message_quality_refs,v.message_quality_state) for v in self.views)
        if _legacy_plan is not None and bindings!=_legacy_plan.sources:raise ValueError('单run兼容计划发生变化')
        self.plan=_legacy_plan or FeaturePlan(self.version,collector,bindings)
        self.blocks=[]
        for key,views in groupby(self.views,key=lambda v:(v.run_id,v.snapshot)):
            block=tuple(views); sources=[v.source_id for v in block]
            bound=self.readers[key]
            reader=bound if bound.sources==tuple(sources) else ObservationReader(dsn,*key,sources,profile=self.profile)
            expected={s.source_id:s for s in bound.starts}
            if reader.manifest!=bound.manifest or any(s!=expected[s.source_id] for s in reader.starts):
                raise ValueError('组装流时固定来源漂移')
            self.blocks.append(reader)

    @staticmethod
    def _bounds(bounds):return [t.isoformat() for t in bounds] if bounds else None

    @staticmethod
    def _source_record(view):
        record={**asdict(view),'window':_window_data(view.window)}
        if view.profile=='complete':record.pop('profile')
        return record

    def window_role(self, view):
        if view.calculation_role=='initial_rib':return 'initial'
        if self.result_window is None:return 'result'
        start,end=view.window.start,view.window.end
        for role,bounds in (('result',self.result_window),('comparison',self.comparison_window)):
            if bounds is not None and bounds[0]<=start and end<=bounds[1]:return role
        earliest=(self.comparison_window or self.result_window)[0]
        if end<=earliest:return 'warmup'
        raise ValueError('来源窗口跨交付边界或超出声明范围')

    def reference_receipt(self):
        reference=self.reference_view
        if reference.source_sha256 not in {r['sha256'] for r in self.reference_reader.manifest.get('references',())}:
            raise ValueError('未声明的Feature参考身份')
        if self.profile=='observation':
            self.reference_reader.selection.check()
            cp=next(cp for cp in self.reference_reader.selection.checkpoints if cp['source_id']==reference.source_sha256)
            if cp['ingest']!='complete' or cp['raw']!='verified_source_eof':raise ValueError('参考checkpoint不完整')
            return ('validated',cp['counts']['references'])
        with psycopg2.connect(self.dsn) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute('SELECT state,row_count FROM domeye.reference_inputs WHERE run_id=%s AND source_id=%s',
                          (reference.run_id,reference.source_sha256))
                row=c.fetchone()
                if row is None or row[0]!='validated' or row[1] is None:raise ValueError('Feature参考资格失效')
                return row

    def validate(self):
        for reader in {*self.readers.values(),*self.blocks,self.reference_reader}:
            connection=reader.connect();connection.close()
        if self.reference_receipt()!=self.bound_reference:raise ValueError('Feature固定参考回执漂移')

    def stream(self):
        for reader in self.blocks:
            if self.profile=='observation':
                from data_pipeline.bgp.ordered_reader import ordered
                yield from ordered(reader)
            else:yield from reader.stream()

    @classmethod
    def single(cls,reader,plan,reference_sha,reference_path):
        if plan.observation_version!=reader.run_id+':'+str(reader.snapshot):raise ValueError('观察版本必须由精确run:snapshot构成')
        if tuple(s.source_id for s in plan.sources)!=reader.sources:raise ValueError('Reader与Feature来源顺序不一致')
        entries={e['source_id']:e for e in reader.manifest['inputs']};starts={s.source_id:s for s in reader.starts}
        profile='observation' if reader.selection is not None else 'complete'
        views=[SourceView(reader.run_id,reader.snapshot,s.source_id,entries[s.source_id]['origin_uri'],s.content_sha256,s.role,
                          'initial_rib' if s.role=='baseline' else 'update',reference_sha,starts[s.source_id].expected_messages,
                          s.expected_elements,s.window,s.message_quality_refs,s.message_quality_state,profile) for s in plan.sources]
        return cls(reader.dsn,plan.collector,views,ReferenceView(reader.run_id,reader.snapshot,reference_sha,str(reference_path),profile=profile),
                   _legacy_plan=plan,_readers={(reader.run_id,reader.snapshot):reader})
