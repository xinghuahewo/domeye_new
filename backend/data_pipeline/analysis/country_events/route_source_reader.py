"""持实际上游锁复用完整公开原行；只将精确原引用接到国家 C2 保存态。"""
from copy import deepcopy
from contextlib import contextmanager
from data_pipeline.analysis.country_events.snapshot_store import cleanup
import json

from data_pipeline.analysis.country_events import snapshot_schema as component_schema, event_aggregation as c2
from data_pipeline.analysis.country_events.route_inputs import check_saved_read, m2, decode, duntyped
from data_pipeline.analysis.country_events.route_binding import M3Binding
from data_pipeline.analysis.country_events.route_history import CanonicalHistory
from data_pipeline.analysis.country_events.qualified_event_results import country_results
from data_pipeline.analysis.country_events.route_change_adapter import saved_change, saved_invalidation, saved_country_revision
from data_pipeline.analysis.country_events.route_gap_adapter import M3Stage
from data_pipeline.bgp.replay.snapshot_contract import TABLES as CANONICAL_TABLES
from data_pipeline.analysis.detection.result_window import identity_windows, instant


class M3Source:
    """rows/receipts 是本次或此前实际公开读的保存结果，不是新 Admission。

    构造与计算均须持 M3Inputs 的实际固定锁；当前准入由原 owner 核验。
    保存结果的来源由实际读证据链负责，本类重验全部摘要、原绑定与完整范围。
    """
    def __init__(self, inputs, rows, receipts, *, max_rows, max_bytes,
                 max_gap_candidates, legacy_timezone='Asia/Shanghai'):
        self.inputs, self.timezone = inputs, legacy_timezone
        if any(type(n) is not int or n <= 0 for n in (max_rows, max_bytes, max_gap_candidates)):
            raise ValueError('M3 完整源预算无效')
        self.max_rows, self.max_bytes = max_rows, max_bytes
        self.max_gap_candidates = max_gap_candidates
        from data_pipeline.analysis.country_events.input_staging import SourceStore
        self.store=rows if isinstance(rows,SourceStore) else None
        self.check()
        expected = set()
        for owner, aids in inputs.owners.items():
            views = CANONICAL_TABLES if owner == 'canonical' else (
                ('records', 'm3_entries', 'result_revisions', 'result_coverage') if owner == 'detection'
                else ('references',) if owner == 'reference'
                else ('messages', 'elements', 'paths', 'peers', 'eor', 'associations', 'quality'))
            expected.update((aid, view) for aid in aids for view in views)
        if set(rows) != expected:
            raise ValueError('M3 四类完整公开视图缺项或越界')
        self.rows, self.receipts = {}, {}
        used_rows = used_bytes = 0
        for saved in receipts:
            self.check()
            aid, view = saved['receipt']['admission_id'], saved['view']
            key = aid, view
            if key not in expected or key in self.receipts:
                raise ValueError('M3 完整原回执缺项、重复或越界')
            owner = inputs.admissions[aid]['owner']
            request = saved['request']
            if owner == 'canonical':
                scope = decode(request['scope_typed']); required = dict(source_ids=None)
            elif owner == 'detection':
                scope = duntyped(request['scope_typed']); required = dict(start=0, stop=None, key=None, at_position=None)
            else:
                scope = m2.untyped(request['scope_typed'])
                raw = m2.untyped(inputs.admissions[aid]['owner_binding'])
                required = dict(source_ids=[raw['source_id']] if owner == 'reference' else raw['ordered_source_ids'])
            if scope != required:
                raise ValueError('M3 缓存原读不是完整固定视图')
            # 落盘路径只计量；驻留副本仍在deepcopy之前检查工作集。
            for row in rows[key]:
                self.check()
                used_rows += 1; used_bytes += len(component_schema.encode(row).encode())
                if self.store is None and (used_rows > max_rows or used_bytes > max_bytes):
                    raise ValueError('resource_limit:M3_source_rows')
            check_saved_read(owner, aid, view, rows[key], saved, guard=self.check)
            self.rows[key] = rows[key] if self.store is not None else deepcopy(rows[key])
            self.receipts[key] = deepcopy(saved)
        if set(self.receipts) != expected:
            raise ValueError('M3 原完整回执缺项')
        self.costs = dict(original_rows=used_rows, original_bytes=used_bytes)
        mb, cb, db = inputs.m2_binding, inputs.canonical_binding, inputs.detection_binding
        original = json.loads(mb['input_binding'])
        self.binding = M3Binding(mb['run_id'], str(mb['snapshot']), tuple(mb['ordered_source_ids']),
                                 db['identity']['reference_version'], original['interpretation_version'],
                                 cb['descriptor']['plan']['algorithm'], original['collector'],
                                 global_sources=tuple(s['source_id'] for s in original['sources']),
                                 input_binding_id=mb['input_binding_id'])
        baselines = [s['source_id'] for s in mb['selected_sources'] if s['calculation_role'] == 'baseline']
        if len(baselines) != 1:
            raise ValueError('M3 实际基线角色必须唯一')
        self.baseline_source = baselines[0]
        self.history = CanonicalHistory(self.binding, max_rows=max_rows, max_bytes=max_bytes,
                                        max_gap_candidates=max_gap_candidates, guard=self.check,store=self.store,
                                        original_rows={v:self.get('canonical',v) for v in CanonicalHistory.tables})
        for view in self.history.tables:
            for row in self.get('canonical', view): self.history.append(view, row)
        self.history.seal()
        self.messages = self.unique(self.get('m2', 'messages'), 'message_id')
        self.elements = self.unique(self.get('m2', 'elements'), 'event_id')
        self.paths = self.unique(self.get('m2', 'paths'), 'path_key', identical=True)
        self.results = country_results(*(self.get('detection', v) for v in
                                         ('records', 'm3_entries', 'result_revisions', 'result_coverage')),
                                       db, max_rows=max_rows, guard=self.check,store=self.store)
        self.windows = identity_windows(db['identity'], db['scope'])

    def check(self):
        self.inputs.check_budget()
        if self.store is not None:self.store.check()
        if not self.inputs.active:
            raise ValueError('M3 国家源必须持有实际四类上游锁')

    def verify_saved(self):
        """重复计算或封存前后重验保留原行，不能接受内存缓存被改写。"""
        self.check()
        for (aid, view), rows in self.rows.items():
            check_saved_read(self.inputs.admissions[aid]['owner'], aid, view, rows,
                             self.receipts[aid, view], guard=self.check)

    def unique(self, rows, key, identical=False):
        if self.store is not None:
            from data_pipeline.analysis.country_events.input_staging import Lookup
            return Lookup(rows,key,identical)
        out = {}
        for row in rows:
            self.check()
            if row[key] in out and (not identical or out[row[key]] != row):
                raise ValueError('M3 原引用重复或冲突:' + key)
            out[row[key]] = row
        return out

    def close(self):
        if self.store is not None:self.store.close()

    @classmethod
    @contextmanager
    def capture(cls,inputs,*,scratch_root,max_rows,max_bytes,max_row_bytes,max_gap_candidates,max_rss_bytes=2*1024**3,max_disk_bytes=None):
        """正式生产入口：35原流逐行落盘，全回执到齐后交给同一保存态算法。"""
        from data_pipeline.analysis.country_events.input_staging import SourceStore
        with inputs.locked():
            store=SourceStore(scratch_root,max_rows=max_rows,max_bytes=max_bytes,max_row_bytes=max_row_bytes,
                               batch_rows=inputs.batch_rows,guard=inputs.check_budget,max_rss_bytes=max_rss_bytes,
                               max_disk_bytes=max_disk_bytes)
            primary=None
            try:
                for aid,a in inputs.admissions.items():
                    owner=a['owner']
                    views=CANONICAL_TABLES if owner=='canonical' else ('records','m3_entries','result_revisions','result_coverage') if owner=='detection' else ('references',) if owner=='reference' else ('messages','elements','paths','peers','eor','associations','quality')
                    for view in views:store.add_view((aid,view),inputs.read(aid,view))
                yield cls(inputs,store,inputs.receipts,max_rows=max_rows,max_bytes=max_bytes,max_gap_candidates=max_gap_candidates)
            except BaseException as error:primary=error;raise
            finally:cleanup((store.close,),primary)

    def get(self, owner, view):
        return self.rows[self.inputs.owners[owner][0], view]

    def fill(self, stage):
        self.check()
        # 所有未参与计算的原行也作为输入证据保真，不能借连接过滤掉Gap或旧列表。
        for (aid, view), rows in self.rows.items():
            owner = self.inputs.admissions[aid]['owner']
            for ordinal, row in enumerate(rows):
                stage.db.execute('INSERT INTO evidence VALUES (?,?,?)',
                    ('m3_original', f'{owner}/{aid}/{view}/{ordinal}', stage.checked(row)))
        for element in self.elements.values():
            message, path = self.messages[element['message_id']], self.paths[element['path_key']]
            enriched = dict(element, **path)
            enriched.update({k: message[k] for k in ('source_id', 'content_sha256', 'record', 'epoch', 'microsecond',
                             'mrt_type', 'mrt_subtype', 'local_message', 'local_ip', 'local_asn', 'interface')})
            stage.db.execute('INSERT INTO raw_elements VALUES (?,?)', (element['event_id'], stage.checked(enriched)))
        from data_pipeline.analysis.country_events.compute import Path
        from data_pipeline.analysis.country_events.aggregation_staging import segments
        for row in self.paths.values():
            stage.put('path', row['path_key'], Path(row['path_key'], segments(row['as_path_raw'], row['asn_width']),
                       row['attributes_raw'], segments(row['as4_path_raw'], 4)))
        for row in self.get('canonical', 'changes'):
            element = self.elements[row['event_id']]
            delta = saved_change(self.binding, row, self.messages[row['message_id']], element, self.paths[element['path_key']])
            stage.db.execute('INSERT INTO raw_deltas VALUES (?,?)', (delta.event_ref, stage.checked(delta)))
        for row in self.get('canonical', 'invalidations'):
            value = saved_invalidation(self.binding, row, self.messages[row['message_id']])
            stage.db.execute('INSERT INTO raw_invalidations VALUES (?,?,?)',
                             (value.message_ref, value.object_key, stage.checked(value)))
        for result in self.results:
            row = result['raw']; observation = json.loads(row['evidence_json'])['observation']
            value = saved_country_revision(self.binding, row, self.messages[observation['message_ref']],
                                            self.elements[observation['observation_id']], legacy_timezone=self.timezone)
            stage.db.execute('INSERT INTO revisions VALUES (?,?,?)',
                             (value.incident_id, value.revision, stage.checked(value)))
        reference_source = self.inputs.detection_binding['identity']['reference_sources']['as_info']['source_id']
        for aid in self.inputs.owners['reference']:
            for row in self.rows[aid, 'references']:
                if row['source_id'] == reference_source:
                    stage.db.execute('INSERT INTO evidence VALUES (?,?,?)', ('as_info', str(row['row']), stage.checked(row)))
        stage.prepare_saved(reference_source)
        stage.input_kind = 'saved_M3'

    def run_c2(self, grid, *, scratch_root, **limits):
        self.verify_saved()
        calc = self.windows['calculation_window']
        bounds = tuple(int(instant(calc[k]).timestamp()) * 1000000 for k in ('window_start', 'window_end_exclusive'))
        if (grid.input_start_us, grid.input_end_us) != bounds:
            raise ValueError('M3 C2 必须保留完整实际计算窗，结果窗另外选择')
        def factory(path, binding, guard, options):
            return M3Stage(path, binding, guard, options, baseline_source=self.baseline_source,
                           history=self.history, messages=self.messages, max_gap_candidates=self.max_gap_candidates)
        yield from c2._run(self.binding, grid, scratch_root, self.fill, self.check, limits, _stage_factory=factory)
        self.verify_saved()
