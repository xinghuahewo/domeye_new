"""一次完整Feature审计与纯选择；临时期望行不创建科学run或M2输出。"""
from dataclasses import fields as dataclass_fields
from datetime import datetime
import hashlib
import json
from pathlib import Path
import time

from data_pipeline.analysis.features.inputs import FeatureInputs, SourceView, ReferenceView
from data_pipeline.analysis.features.calculation import FileWindow, RULES
from data_pipeline.analysis.features.projection import PROJECTION_RULE
from data_pipeline.analysis.features.reference import REFERENCE_RULE
from data_pipeline.analysis.features.store import FeatureStore
from data_pipeline.analysis.features.run import _calculate
from data_pipeline.analysis.features.qualified_read import qualified_window
from data_pipeline.analysis.features.qualification import RULE, DIMENSIONS, ORDER_KEYS, rows_digest
from data_pipeline.bgp.input.path_decoding import DECODER_VERSION, DIRECTION_RULE
from data_pipeline.bgp.archive.validation import _closing
from data_pipeline.bgp.archive.value_codec import typed, untyped, digest, canonical
from data_pipeline.bgp.replay.route_replay import identity
from data_pipeline.common.frozen_execution import digest as frozen_digest
from data_pipeline.analysis.features import publication_io as io


# 只支持已接受M3科学规则；旧producer摘要不替换为本validator身份。
SUPPORTED_PRODUCER_FILES = {'backend/data_pipeline/feature_calculation.py': ['2f6419b8165d20751ee6080eb159436c4e0351b2f32d7d93c4573fac1225c76d'], 'backend/data_pipeline/feature_projection.py': ['3babc83653c4bba318e81df910237188460e6456af1137718caa89c2756bb51f'], 'backend/data_pipeline/feature_reference.py': ['15ef67a3d22a219a7a62746cf07d272baaf31854640c72b25479ac130c326e36'], 'backend/data_pipeline/feature_qualification.py': ['e5e4ee1ca5f30842d005a8b80ad645716dd1087f90db4303f6b0706719d75168'], 'backend/data_pipeline/observations/decoding.py': ['e1fbe83ecc44bceec5e9a8aa1cabc20ebb727c43ce592b21fc7cf5074d5852e6'], 'backend/data_pipeline/observations/state.py': ['340025bb11b2249da409cb15fee766a64d4d3f168625869398161e264711d354', '62a78822595807075c76042dc1e37efdd31a51005c2cc197188c9d86bedcabee'], 'backend/data_pipeline/observations/ordered.py': ['1bf77cfd777b76cebc3357b6d8df2c746868ce745ded6db5ab647633854d76c5'], 'backend/data_pipeline/observations/ordered_contract.py': ['7f880926add9c461dac438e447095686df9eebb76b4a5da9397bc06a2939240a']}


def _producer(spec):
    expected = dict(algorithm_versions=RULES, projection_rule=PROJECTION_RULE, reference_rule=REFERENCE_RULE,
                    qualification_rule=RULE, decoder_version=DECODER_VERSION, direction_rule=DIRECTION_RULE)
    if any(spec.get(k) != v for k, v in expected.items()):
        raise ValueError('未支持的历史Feature科学/资格规则')
    code = spec['code_identity']
    if code.get('execution_mode') != 'frozen-fresh-process' or code.get('schema_version') != 'feature-code-identity/v1':
        raise ValueError('缺少原冻结producer身份')
    core = {k: code[k] for k in ('schema_version', 'files', 'python', 'packages')}
    if frozen_digest(core) != code['execution_binding']['snapshot_digest']:
        raise ValueError('原冻结producer快照摘要不一致')
    for path, supported in SUPPORTED_PRODUCER_FILES.items():
        if code['files'].get(path, {}).get('sha256') not in supported:
            raise ValueError('未支持的历史producer实现：' + path)
    # 有序诊断等实现差异不靠删ID兼容；以下完整审计比较原列表、ID和引用。


def _inputs(runtime, b):
    spec = b['specification']; views = []
    keys = {f.name for f in dataclass_fields(SourceView)}
    for source in spec['source_bindings']:
        for alias in source['aliases']:
            raw = {k: v for k, v in alias.items() if k in keys}; window = dict(raw.pop('window'))
            for k in ('start', 'end', 'file_time'): window[k] = datetime.fromisoformat(window[k])
            window['limitations'] = tuple(window.get('limitations', ()))
            raw['message_quality_refs'] = tuple(raw['message_quality_refs'])
            views.append(SourceView(**raw, window=FileWindow(**window)))
    # 现FeatureInputs多run仍使用同一显式输入库；本片不扩充跨输入库计算适配。
    m2 = [a for a in runtime.dependency_admissions if a['owner'] == 'm2']
    physical = {(a['physical']['system_identifier'], a['physical']['database_oid']) for a in m2}
    if len(physical) != 1: raise ValueError('本片Feature计算审计仅原同库多run输入')
    dsn = runtime.dependency_runtimes[m2[0]['admission_id']].dsn
    bounds = {k: tuple(datetime.fromisoformat(t) for t in spec[k]) if spec[k] else None
              for k in ('result_window', 'comparison_window')}
    inputs = FeatureInputs(dsn, spec['collector'], views, ReferenceView(**spec['reference_view']), **bounds)
    if (canonical(inputs.source_specs) != canonical(spec['source_bindings'])
            or canonical(inputs.reference_spec) != canonical(spec['reference_binding'])
            or inputs.version != spec['observation_version']):
        raise ValueError('Feature实际来源/别名/参考/窗口映射漂移')
    return inputs


class AuditSink:
    """复用原行构造方法，仅写临时对账表，无FeatureStore初始化/写库。"""
    audit = FeatureStore.audit
    window_row = FeatureStore.window_row
    save_diagnostics = FeatureStore.save_diagnostics
    save_phases = FeatureStore.save_phases
    source_receipt = FeatureStore.source_receipt

    def __init__(self, runtime, db, root, b, guard):
        self.runtime, self.db, self.root, self.guard = runtime, db, root, guard
        self.run_id = b['run_id']; self.specification = b['specification']
        self.source_bindings = {s['source_id']: s for s in self.specification['source_bindings']}
        self.source_counts = {}; self.diagnostic_expected = {}; self.diagnostic_values = {}
        self.qualification_expected = {}; self.pending = {t: [] for t in io.TABLES}
        self.pending_size = 0
        for table in io.TABLES: io.create(runtime, db, 'expected_' + table, table)

    def append(self, table, row):
        self.guard(); binding = self.source_bindings.get(row.get('source_id'))
        if binding is not None and ('window_role', 'VARCHAR') in io.TABLES[table]:
            row = {**row, 'window_role': binding['window_role']}
            if table == 'source_receipts':
                row.update(upstream_run_id=binding['run_id'], upstream_snapshot=binding['snapshot'],
                           **{k: binding[k] for k in ('source_role', 'calculation_role', 'origin_uri', 'content_sha256')})
        self.pending[table].append(row); self.pending_size += len(typed(row).encode())
        if len(self.pending[table]) >= 256 or self.pending_size >= 1024**2: self.flush()

    def flush(self):
        for table, rows in self.pending.items():
            if rows:
                io.insert(self.runtime, self.db, 'expected_' + table, table, rows); rows.clear()
        self.pending_size = 0; io.budget(self.runtime, self.root, self.guard)

    def source_complete(self, mode, rank, binding, source_end, old_state, state, expected_windows):
        if self.source_counts.get((mode, binding.source_id), {'audit': 0, 'windows': 0}) != {
                'audit': binding.expected_elements, 'windows': expected_windows}:
            raise ValueError('审计来源元素/窗口必要输出不完整')
        self.append('source_receipts', self.source_receipt(mode, rank, binding, source_end, old_state, state, expected_windows))
        self.flush()


def validate(runtime, b, files, completion, guard):
    started = time.monotonic(); _producer(b['specification'])
    paths = {f['path'] for f in files}
    paths.add(str(runtime.output_root / 'execution.json'))
    # 参考原件通过上游授权root读取；本owner输出entities不越权复制其权限判断。
    entities = [io.entity(runtime, path, guard, full=True) for path in sorted(paths)]
    receipt = json.loads((runtime.output_root / 'execution.json').read_text())
    if (receipt.get('run_id'), receipt.get('snapshot'), receipt.get('specification'), receipt.get('state')) != (
            b['run_id'], b['snapshot'], b['specification'], 'ready') or receipt.get('qualification_completion') != completion:
        raise ValueError('Feature原ready与实际完成锚不符')
    inputs = _inputs(runtime, b)
    inventory = []
    with io.stage(runtime, guard) as (db, root):
        for table in io.TABLES:
            io.load(runtime, db, root, table, files, guard)
        runtime.event('feature_full_audit', passes=1, modes=2, mrt_reparse=0)
        sink = AuditSink(runtime, db, root, b, guard)
        states, timing, reference = _calculate(inputs, sink, batch_rows=512, batch_bytes=1024**2, guard=guard)
        sink.flush()
        if reference.version != b['reference_version'] or reference.version != completion['reference_version']:
            raise ValueError('Feature实际参考解释版本漂移')
        for table in io.TABLES:
            # EXCEPT ALL保留合法重复；列表顺序、原ID和refs逐值参与比较，不归一化。
            sql = f'''SELECT * FROM ((SELECT * FROM actual_{table} EXCEPT ALL SELECT * FROM expected_{table})
                UNION ALL (SELECT * FROM expected_{table} EXCEPT ALL SELECT * FROM actual_{table})) LIMIT 1'''
            mismatch = io.query(runtime, db, sql).fetchone()
            if mismatch is not None:
                row = dict(zip((n for n, _ in io.TABLES[table]), mismatch))
                raise ValueError('Feature完整科学/资格关系不符：' + canonical(dict(run_id=b['run_id'], table=table,
                    source_id=row.get('source_id'), source_rank=row.get('source_rank'), row=row)))
            values = io.rows(runtime, db, f'SELECT * FROM actual_{table} ORDER BY ALL NULLS FIRST', guard)
            count, content = io.row_digest(values)
            if count != completion['actual_rows'][table]: raise ValueError('Feature真实计数与完成锚不符')
            inventory.append(dict(table=table, schema=io.TABLES[table], rows=count, typed_digest=content,
                                  relation='exact-original-rule-multiset'))
            if table in ORDER_KEYS:
                values = io.rows(runtime, db, f'SELECT * FROM actual_{table} ORDER BY ' + ','.join(ORDER_KEYS[table]), guard)
                if rows_digest(values) != completion['qualification_hashes'][table]:
                    raise ValueError('Feature资格内容与原完成锚不符')
        qids = [r[0] for r in io.query(runtime, db, 'SELECT qualification_id FROM actual_qualifications ORDER BY qualification_id').fetchall()]
        if identity(qids) != b['qualification_digest']:
            raise ValueError('Feature原绑定资格摘要不符')
        if b['table_counts'] != {item['table']: item['rows'] for item in inventory if item['table'] in ORDER_KEYS}:
            raise ValueError('Feature原绑定资格表计数不符')
        if b['specification_digest'] != identity(b['specification']): raise ValueError('Feature原绑定规格不符')
        if b['input_seals'] != [{'run_id': x['run_id'], 'snapshot': x['snapshot'], 'seal_digest': x['seal']['digest']} for x in b['specification']['observation_seals']]:
            raise ValueError('Feature原绑定输入摘要不符')
    inputs.validate(); io.entities_current(runtime, entities, guard)
    return inventory, entities, dict(seconds=time.monotonic() - started, full_audits=1, modes=2,
        original_tables=12, mrt_reparse=0, scientific_output_writes=0,
        cost_boundary='同一次现计算审计含ordinary/ir各一次输入流，状态与参考整列仍按现算法驻内存；临时表外排受资源保护')



def selected_rows(runtime, b, view, scope, guard):
    from data_pipeline.analysis.features.publication import _metadata
    files = _metadata(runtime, b)[2]
    sources = [s['source_id'] for s in b['specification']['source_bindings']
               if scope['window_role'] == 'all' or s['window_role'] == scope['window_role']]
    modes = ('ordinary', 'ir') if scope['mode'] == 'all' else (scope['mode'],)
    with io.stage(runtime, guard) as (db, root):
        # 无物理来源分区：首批仍完整装载所选view表，绝不称batch_rows限制前置扫描。
        io.load(runtime, db, root, 'qualifications', files, guard)
        if view == 'windows': io.load(runtime, db, root, 'windows', files, guard)
        from data_pipeline.bgp.archive.store import literal
        for source in sources:
            for mode in modes:
                predicate = ' WHERE source_id=' + literal(source) + ' AND mode=' + literal(mode)
                qualifications = list(io.rows(runtime, db, 'SELECT * FROM actual_qualifications' + predicate + ' ORDER BY dimension', guard))
                if len(qualifications) != 6 or {q['dimension'] for q in qualifications} != set(DIMENSIONS):
                    raise ValueError('选择缺少六维Coverage')
                if view == 'coverage':
                    yield from qualifications
                else:
                    by_dimension = {q['dimension']: q for q in qualifications}
                    stream = io.rows(runtime, db, 'SELECT * FROM actual_windows' + predicate + ' ORDER BY ALL NULLS FIRST', guard)
                    with _closing([stream.close]):
                        for row in stream:
                            io.budget(runtime, root, guard)
                            yield qualified_window(row, by_dimension)
