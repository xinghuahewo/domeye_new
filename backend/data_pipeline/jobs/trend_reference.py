"""从实际 Country 公开捕获生成独立比较输入；原始人工比较值只读。"""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys

from data_pipeline.analysis.country_events import snapshot_schema as original, qualified_schema as m3_schema
from data_pipeline.analysis.country_events.compute import MetricPoint
from data_pipeline.analysis.country_trends.contract import Projection, ReferenceInput, REFERENCE_DEFINITION
from data_pipeline.analysis.country_trends.event_inputs import EventSources
from data_pipeline.analysis.country_trends.country_source import initialize, capture_country, decoded_rows
from data_pipeline.analysis.country_trends.feature_source import capture_feature, feature_context
from data_pipeline.analysis.country_trends.stream_schema import encode
from data_pipeline.analysis.country_trends.snapshot_inputs import register_reference, REFERENCE_PROFILE
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
from data_pipeline.analysis.country_trends.stream_budget import StreamBudget, connect
from data_pipeline.results.manifest_io import file_hash
from data_pipeline.jobs.stage_runner import save_new, save_final
from data_pipeline.analysis.country_events.snapshot_store import cleanup
from data_pipeline.common import process_resources as resources


class ContextUnavailable(ValueError):
    """当前实际结果不足以登记所要求的上下文；不能默默省略。"""


def read_comparison(path, sha):
    path = Path(path)
    if path.stat().st_size > 8*1024**2 or file_hash(path) != sha:
        raise ValueError('独立比较原件大小或SHA不符')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != sha:
        raise ValueError('独立比较原件读取漂移')
    doc = json.loads(raw)
    if (not doc['origin_uri'].startswith('fixture://') or doc['historical_applicability'] != 'unknown'
            or tuple(doc['definition_binding']) != REFERENCE_DEFINITION
            or doc['population'] != REFERENCE_DEFINITION[-1] or not doc['projections']):
        raise ValueError('独立比较原定义不符')
    codes = set()
    for p in doc['projections']:
        samples, values = p['samples'], p['values']
        if (p['country'] in codes or not samples or len(samples) != len(values)
                or any(type(t) is not int for t in samples)
                or any(a >= b for a, b in zip(samples, samples[1:]))
                or type(p['denominator']) is not int or p['denominator'] <= 0
                or any(v is not None and (type(v) is not int or not 0 <= v <= p['denominator']) for v in values)):
            raise ValueError('独立比较原投影无效')
        codes.add(p['country'])
    return doc


def reference_for_source(source, comparison, sha, ordinal):
    """纯值接合；仅允许外部原网格覆盖完整实际D样本，不裁掉目标点。"""
    status = source.status
    selected = [source.metric(k) for k, v in source.originals.items()
                if type(v) is MetricPoint and v.metric == 'visible_direction_count'
                and source.window_us[0] < v.sample_us <= source.window_us[1]]
    selected.sort(key=lambda p: p.target.sample_us)
    samples = tuple(p.target.sample_us for p in selected)
    denominator = source.denominator().value
    if (not status.cohort_id or not samples or denominator is None
            or any(p.value is None or p.denominator != denominator for p in selected)):
        raise ContextUnavailable('实际Country主值、分母或样本不足，未登记比较参考')
    target = status.incident.country
    if target in {p['country'] for p in comparison['projections']}:
        raise ValueError('外部比較不得覆盖实际目标国家')
    projections = [Projection(target, comparison['population'], denominator, samples,
        tuple(p.value for p in selected), f'row:{ordinal}:0', cohort_id=status.cohort_id,
        definition_binding=REFERENCE_DEFINITION)]
    for j, p in enumerate(comparison['projections'], 1):
        by_time = dict(zip(p['samples'], p['values']))
        if not set(samples) <= set(by_time):
            raise ContextUnavailable('外部原值不能覆盖全部实际Country D样本；禁止插值或删除目标点')
        projections.append(Projection(p['country'], comparison['population'], p['denominator'], samples,
            tuple(by_time[t] for t in samples), f'row:{ordinal}:{j}', quality=p['quality'],
            asn_count=p['asn_count'], persistent_asn_count=p['persistent_asn_count'],
            cohort_id=f'artificial-external-{sha}-{p["country"]}', definition_binding=REFERENCE_DEFINITION))
    return ReferenceInput(source.event, (), target, tuple(projections))


def references_from_capture(db, comparison, sha, window, target, budget):
    coverages = [item.value for _, item in decoded_rows(db, tables=('country_coverage',))]
    if not coverages:
        raise ContextUnavailable('实际Country独立覆盖缺失')
    refs, selections = [], []
    reference_bytes = 0
    for incident, revision, _, payload in db.execute('SELECT * FROM country_events ORDER BY sequence'):
        budget.check()
        envelope = original.decode(payload)
        wrapped = envelope['raw']
        status = m3_schema.row_decode(wrapped['table'], wrapped['row']).value
        if status.incident.country != target:
            continue
        event = (incident, revision)
        size = db.execute('SELECT coalesce(sum(bytes),0) FROM country_input WHERE incident=? AND revision=?', event).fetchone()[0]
        if size + len(payload.encode()) > budget.limits.max_event_bytes:
            raise ValueError('参考生成单事件字节超限')
        originals, qs, qvs = [], [], []
        for seq, item in decoded_rows(db, event=event):
            budget.check()
            if type(item.value) in original.OUTPUTS:
                originals.append((seq, item))
            elif type(item.value) is m3_schema.NEW_TABLES['country_qualification']:
                qs.append(asdict(item.value))
            elif type(item.value) is m3_schema.NEW_TABLES['country_qualified_value']:
                qvs.append(asdict(item.value))
        source = EventSources(coverages[0].logical_run_id, coverages[0].input_binding_id,
                              window, event, originals, qs, qvs)
        reference = reference_for_source(source, comparison, sha, len(refs))
        reference_bytes += len(encode(reference).encode())
        if reference_bytes > budget.limits.max_context_bytes:
            raise ValueError('独立比较上下文累计字节超限')
        refs.append(reference)
        selections.append((event, 'ordinary'))
        if len(refs) > budget.limits.max_events:
            raise ValueError('参考生成事件数量超限')
    if not refs:
        raise ContextUnavailable('无实际目标Country事件，未预造事件或登记空比较参考')
    return tuple(refs), tuple(selections)


def prepare_context(config, country, feature, output, guard):
    """25个Country原公开流及Feature原两流捕获后，才调用原参考登记。"""
    resources.sqlite_temp()
    root = Path(output)
    root.mkdir(parents=True, exist_ok=False)
    limits = S2Limits(**config['limits'])
    budget = StreamBudget(limits, root, guard)
    comparison = read_comparison(config['source_path'], config['source_sha256'])
    db = connect(root/'公开上下文.sqlite', budget)
    try:
        initialize(db)
        cr, ca = country; fr, fa = feature
        capture = capture_country(db, ca, cr, tuple(config['window_us']), budget)
        capture_feature(db, fa, fr, budget)
        counts = dict(db.execute('SELECT view,count(*) FROM feature_input GROUP BY view'))
        resources.track(root/'实际公开捕获.json')
        save_new(root/'实际公开捕获.json', dict(country=capture, feature_rows=counts,
            repeated_scan='Trend原producer随后独立再读；此处用于真实事件选择与派生参考，不替代其审计'))
        resources.freeze(root/'实际公开捕获.json')
        if any(counts.get(k, 0) == 0 for k in ('windows', 'coverage')):
            raise ContextUnavailable('实际Feature windows/coverage为空，不能省略后继续完整尾段')
        refs, selections = references_from_capture(db, comparison, config['source_sha256'],
            tuple(config['window_us']), config['target'], budget)
        activities = feature_context(db, fa, selections, budget)
        if not activities:
            raise ContextUnavailable('实际Feature没有与Country国家代码匹配的窗口')
        doc = dict(profile=REFERENCE_PROFILE, version=config['version'], origin_uri=config['origin_uri'],
                   historical_applicability='unknown', references=encode(refs))
        if len(json.dumps(doc, ensure_ascii=False).encode()) > min(8*1024**2, limits.max_context_bytes):
            raise ValueError('派生参考整包字节超限')
        path = Path(config['derived_root'])/'trend-reference.json'
        if file_hash(config['source_path'], guard) != config['source_sha256']:
            raise ValueError('生成派生参考期间原件改变')
        save_new(path, doc)
        path.chmod(0o444)
        binding = register_reference(config['dsn'], path, config['derived_root'], guard)
        resources.track(root/'原参考登记.json')
        save_new(root/'原参考登记.json', binding)
        resources.freeze(root/'原参考登记.json')
        budget.finish()
        return selections, dict(source_path=config['source_path'], source_sha256=config['source_sha256'],
            derived_root=config['derived_root'], country_admission_id=ca['admission_id'], binding=binding)
    except ContextUnavailable as error:
        save_final(root/'上下文不足.json', dict(state='insufficient_data', reason=str(error), omitted=False))
        raise
    finally:
        cleanup((db.close,), sys.exc_info()[1])
