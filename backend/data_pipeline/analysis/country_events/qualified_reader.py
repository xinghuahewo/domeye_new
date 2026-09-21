"""M3固定结果的有限公开读取；持实际C3/C4锁，完整退出后才给尾回执。"""
from collections import Counter
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
from pathlib import Path
import hashlib

from data_pipeline.analysis.country_events import qualified_schema as m3_schema, snapshot_schema as component_schema
from data_pipeline.analysis.country_events.snapshot_store import cleanup
from data_pipeline.analysis.country_events.qualified_index import inspect_qualified_country_result, hold_component, M3Access
from data_pipeline.analysis.country_events.selection_admission import verify_country_admission
from data_pipeline.analysis.country_events.selection_index import Budget, readonly, check_entities, counted_progress
from data_pipeline.analysis.country_events.selection_contract import QueryLimits
from data_pipeline.analysis.country_events.route_contract import DIMENSIONS, CountryQualification, CountryCoverage, CountryQualifiedValue

CODEC = 'country-result-typed/v1'
VIEWS = ('events', 'country_qualification', 'country_qualified_value', 'country_coverage', 'raw')


@dataclass(frozen=True)
class ResultLimits(QueryLimits):
    max_references_per_row: int = 128
    max_references: int = 1000000
    max_result_bytes: int = 256 * 1024**2


def ref_count(value):
    if isinstance(value, CountryQualification):
        return sum(len(getattr(value,k)) for k in ('gap_refs','upstream_qualification_refs','evidence_refs','recovery_witnesses'))
    if isinstance(value, CountryCoverage):return len(value.completion_receipt_refs)+len(value.unassigned_scope_refs)
    if isinstance(value, CountryQualifiedValue):return len(value.raw_basis_refs)+len(value.qualification_refs)
    from data_pipeline.analysis.country_events.selection_index import strings
    return sum(1 for _ in strings(value))


def request_scope(descriptor, view, *, window_us, dimension=None, incident_id=None, revision=None,
                  table=None, after_sequence=-1, stop_sequence=None):
    if (view not in VIEWS or type(window_us) is not tuple or any(type(n) is not int for n in window_us)
            or window_us != descriptor.window_us):
        raise ValueError('M3 结果读取不能改绑窗口或view')
    if dimension is not None and dimension not in DIMENSIONS:raise ValueError('M3 结果维度不受支持')
    if (incident_id is None) != (revision is None) or revision is not None and (type(revision) is not int or revision <= 0):
        raise ValueError('M3 结果事件与原revision必须同时明确')
    if incident_id is not None and (type(incident_id) is not str or not incident_id):raise ValueError('M3 结果事件无效')
    if (view == 'raw') != (table is not None) or table is not None and table not in component_schema.TABLES:
        raise ValueError('M3 原科学读取必须明确原21表之一')
    if view in ('raw','events') and dimension is not None:raise ValueError('M3 原行/事件不能伪装成维度筛选')
    if view == 'country_coverage' and incident_id is not None:raise ValueError('M3 独立覆盖不属于某个伪事件')
    if (type(after_sequence) is not int or after_sequence < -1 or stop_sequence is not None
            and (type(stop_sequence) is not int or stop_sequence <= after_sequence)):
        raise ValueError('M3 结果原序号范围无效')
    return dict(result_id=descriptor.binding.result_id, read_model_id=descriptor.binding.read_model_id,
                logical_run_id=descriptor.logical_run_id, input_binding_id=descriptor.input_binding_id,
                window_us=descriptor.window_us, view=view, dimension=dimension, incident_id=incident_id,
                revision=revision, table=table, after_sequence=after_sequence, stop_sequence=stop_sequence)


class ResultReader:
    def __init__(self, component_dsn, descriptor, source, limits, shared_stats=None):
        self.descriptor, self.source, self.limits = descriptor, source, limits
        self.budget = Budget(descriptor.binding.root, limits,_sample=True)
        self.budget.extra_roots = lambda:(descriptor.binding.component.root,)
        self.stats = shared_stats if shared_stats is not None else Counter()
        self.db = self.access = None
        try:
            self.db = readonly(Path(descriptor.binding.root)/'index.sqlite')
            progress,self._progress_errors=counted_progress(self.stats,self.check)
            self.db.set_progress_handler(progress,1000)
            manifest = __import__('json').loads((Path(descriptor.binding.component.root)/'manifest.json').read_text())
            self.access = M3Access(component_dsn,descriptor.binding.component,manifest,limits=limits,guard=self.check,
                                   runtime=getattr(source,'query_runtime',None))
        except BaseException as error:
            cleanup((self.close,),error); raise

    def check(self,*,force_sample=False):
        self.source.check(); self.budget(force=force_sample)

    def charge(self, item, sequence):
        self.check()
        value = item.value if hasattr(item,'value') else item
        refs = ref_count(value)
        if refs > self.limits.max_references_per_row:
            raise ValueError('resource_limit:M3_result_references')
        table, row = m3_schema.row_encode(sequence,item); size = len(component_schema.encode(row).encode())
        if size > min(self.limits.batch_bytes,self.limits.max_row_bytes):raise ValueError('resource_limit:M3_result_row_bytes')
        self.stats.update(rows=1,references=refs,bytes=size)
        return dict(table=table,row=row)

    def read_sequences(self, sequences):
        if len(sequences) > self.limits.max_page_rows:raise ValueError('resource_limit:M3_result_reference_expansion')
        # 在构造二级原行列表前计费数量；原行自身引用另由charge累计。
        self.stats['references'] += len(sequences)
        locators=[]
        for seq in sequences:
            row=self.db.execute('SELECT table_name,row_group,row_offset,sequence,row_hash FROM rows WHERE sequence=?',(seq,)).fetchone()
            if row is None:raise ValueError('M3 结果原行引用缺失')
            locators.append(row)
        decoded=self.access.read(locators)
        return tuple(self.charge(decoded[seq],seq) for seq in sequences)

    def rows(self, scope):
        try:yield from self._rows(scope)
        except BaseException as error:
            if getattr(self,'_progress_errors',None):raise self._progress_errors[0] from error
            raise

    def _rows(self, scope):
        self.check(force_sample=True); check_entities(self.descriptor.entities)
        view=scope['view'];args=[scope['after_sequence']];conditions=['r.sequence>?']
        if scope['stop_sequence'] is not None:conditions.append('r.sequence<?');args.append(scope['stop_sequence'])
        if scope['incident_id'] is not None:
            if self.db.execute('SELECT 1 FROM revisions WHERE incident=? AND revision=?',
                               (scope['incident_id'],str(scope['revision']))).fetchone() is None:
                raise ValueError('M3 结果未包含指定原事件revision')
        if view=='events':
            join='JOIN events e USING(sequence)'
            if scope['incident_id'] is not None:
                if self.db.execute('SELECT 1 FROM events WHERE incident=? AND revision=?',
                                   (scope['incident_id'],str(scope['revision']))).fetchone() is None:
                    raise ValueError('M3 历史revision不是所选EventStatus，不能补造分析事件')
                conditions.extend(['e.incident=?','e.revision=?']);args.extend([scope['incident_id'],str(scope['revision'])])
        elif view=='raw':
            join='';conditions.append('r.table_name=?');args.append(scope['table'])
            if scope['incident_id'] is not None:raise ValueError('M3 原表全流只按原sequence定位，事件结果另读events')
        else:
            join='JOIN m3_locators m USING(sequence)'
            conditions += ['m.table_name=?','m.logical_run=?','m.input_binding=?','m.window_start=?','m.window_end=?']
            args.extend([view,scope['logical_run_id'],scope['input_binding_id'],*scope['window_us']])
            if scope['dimension'] is not None:conditions.append('m.dimension=?');args.append(scope['dimension'])
            if scope['incident_id'] is not None:
                conditions.extend(['m.incident=?','m.revision=?']);args.extend([scope['incident_id'],str(scope['revision'])])
        sql='SELECT r.table_name,r.row_group,r.row_offset,r.sequence,r.row_hash FROM rows r '+join+' WHERE '+' AND '.join(conditions)+' ORDER BY r.sequence'
        cursor=self.db.execute(sql,args)
        try:
            while locators:=cursor.fetchmany(min(self.limits.batch_rows,self.limits.max_page_rows)):
                decoded=self.access.read(locators)
                for _,_,_,seq,_ in locators:
                    raw=self.charge(decoded[seq],seq)
                    if view!='events':yield raw;continue
                    event=self.db.execute('SELECT incident,revision,lifecycle_typed FROM events WHERE sequence=?',(seq,)).fetchone()
                    revisions=[r[0] for r in self.db.execute('SELECT sequence FROM revisions WHERE incident=? ORDER BY sequence',(event[0],))]
                    qualification_sequences=[r[0] for r in self.db.execute("SELECT sequence FROM m3_locators WHERE table_name='country_qualification' AND incident=? AND revision=? AND dimension IN ('event_anchor','event_lifecycle') ORDER BY sequence",event[:2])]
                    quals=self.read_sequences(qualification_sequences)
                    # 包含历史revision的资格；主事件只使用其EventStatus原目标的两维。
                    selected=[]
                    for q in quals:
                        value=m3_schema.row_decode(q['table'],q['row']).value
                        if value.raw_target_ref==('event_status',seq):selected.append(value)
                    main=raw if {q.dimension for q in selected}=={'event_anchor','event_lifecycle'} and all(q.coverage=='complete' for q in selected) else None
                    yield dict(raw=raw,main=main,lifecycle=component_schema.decode(event[2]),
                               original_revisions=self.read_sequences(revisions),qualifications=quals)
            check_entities(self.descriptor.entities);self.check(force_sample=True)
        finally:cursor.close()

    def close(self):
        actions=[]
        if self.access is not None:
            actions.append(lambda:self.budget(force=True))
            actions.append(self._merge_stats)
            actions.append(self.access.close)
        if self.db is not None:actions.append(self.db.close)
        try:cleanup(actions)
        finally:self.access=self.db=None

    def _merge_stats(self):
        # 多个Reader共用累计预算；峰值保持max，处理量和累计字节仍相加。
        for prefix, values in (('physical_', self.access.stats), ('budget_', self.budget.stats)):
            for key, value in values.items():
                target = prefix + key
                if target in ('physical_peak_cache_bytes', 'budget_process_peak_rss_bytes',
                              'budget_peak_working_bytes'):
                    self.stats[target] = max(self.stats[target], value)
                else:
                    self.stats[target] += value


@dataclass
class ResultSession:
    iterator: object = None
    receipt: object = None
    exhausted: bool = False
    def __iter__(self):return self
    def __next__(self):return next(self.iterator)


def open_qualified_country_result(component_dsn, descriptor, *, source, view, window_us,
                                  dimension=None, incident_id=None, revision=None, table=None,
                                  after_sequence=-1, stop_sequence=None, limits=ResultLimits()):
    return _open_result(component_dsn, descriptor, source=source, view=view, window_us=window_us,
                        dimension=dimension, incident_id=incident_id, revision=revision, table=table,
                        after_sequence=after_sequence, stop_sequence=stop_sequence, limits=limits)


@contextmanager
def _open_result(component_dsn, descriptor, *, source, view, window_us,
                                  dimension=None, incident_id=None, revision=None, table=None,
                                  after_sequence=-1, stop_sequence=None, limits=ResultLimits(), _locks_held=False,
                                  _shared_stats=None):
    scope=request_scope(descriptor,view,window_us=window_us,dimension=dimension,incident_id=incident_id,
                        revision=revision,table=table,after_sequence=after_sequence,stop_sequence=stop_sequence)
    session=ResultSession();digest=hashlib.sha256();count=0;reader=None
    with ExitStack() as locks:
        if not _locks_held:
            locks.enter_context(hold_component(component_dsn,descriptor.binding.component))
            locks.enter_context(verify_country_admission(component_dsn,descriptor.binding,descriptor.proof,lock=True))
        actual=inspect_qualified_country_result(component_dsn,descriptor.binding,descriptor.proof,source=source,limits=limits)
        if actual!=descriptor:raise ValueError('M3 结果descriptor不是实际准入')
        reader=ResultReader(component_dsn,descriptor,source,limits,_shared_stats)
        def iterate():
            nonlocal count
            pending=[];size=0
            for row in reader.rows(scope):
                encoded=component_schema.encode(row).encode()
                if len(encoded)>limits.batch_bytes:raise ValueError('resource_limit:M3_result_encoded_row')
                if pending and (len(pending)>=limits.batch_rows or size+len(encoded)>limits.batch_bytes):
                    yield tuple(pending);pending=[];size=0
                digest.update(len(encoded).to_bytes(8,'big'));digest.update(encoded);count+=1
                pending.append(row);size+=len(encoded)
            if pending:yield tuple(pending)
            session.exhausted=True
        session.iterator=iterate();primary=None
        try:yield session
        except BaseException as error:primary=error;raise
        finally:
            cleanup((session.iterator.close,reader.close),primary)
            if primary is None:source.check();check_entities(descriptor.entities)
    # C3/C4两层真实锁及所有句柄正常退出后才交回执。外层四类来源锁由source调用方持有。
    if session.exhausted:
        session.receipt=dict(contract='country-result-read/v1',codec_version=CODEC,scope_typed=component_schema.encode(scope),
                             rows=count,typed_digest=digest.hexdigest(),execution='complete',
                             admitted_empty=descriptor.admitted_empty,full_component_receipt=False,costs=dict(reader.stats))
