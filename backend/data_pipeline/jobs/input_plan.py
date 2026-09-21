"""一日迁移的有限请求接线；只读元数据，不连接数据库或声明 Admission。"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from datetime import datetime


MANIFEST_SHA = '4578d7889c3991735142b68547f56307f462afd6260c2a8b72aea925bfaa819b'
MAPPING_SHA = 'd2fac878c507587a2bf2d3313fa3de7abcd54f415e5f4f37e45743b083a54bed'
REAL_INPUT = 'rrc25-fixed/v1'
ARTIFICIAL_INPUT = 'artificial/v1'
UNAVAILABLE = {
    'Country': '同driver尾接线候选待独立复核与最终运行绑定；模块接口已接入',
    'Trend': '同driver非空Feature及独立参考接线待实际验收',
    'fullP': '人工同driver完整链待实际验收；真实全天另受恢复前置门槛约束',
}


def read_fixed(path, digest):
    """只读取调用方明确提供的JSON；不跟随其中的原始数据路径。"""
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError('固定JSON摘要不符')
    return json.loads(raw)


def require_available(stages):
    missing = {name: UNAVAILABLE[name] for name in stages if name in UNAVAILABLE}
    if missing:
        raise ValueError('启动前拒绝：' + json.dumps(missing, ensure_ascii=False))


class FixedInputs:
    """保留588/11固定来源；消费顺序不改变上游rank。"""

    def __init__(self, manifest, mapping, *, artificial=None):
        self.manifest, self.mapping = deepcopy(manifest), deepcopy(mapping)
        self.input_profile = ARTIFICIAL_INPUT if artificial is not None else REAL_INPUT
        self.artificial = deepcopy(artificial)
        self.entries = {e['source_id']: e for e in manifest['inputs']}
        self.references = {e['sha256']: e for e in manifest['references']}
        counts = dict(mrt_sources=588, reference_sources=11, resource_sources=12, calculation_sources=577)
        if artificial is not None:
            counts = artificial['counts']
            caps = dict(mrt_sources=32, reference_sources=32, resource_sources=16, calculation_sources=32)
            if set(counts) != set(caps) or any(type(counts[k]) is not int or not 1 <= counts[k] <= cap for k,cap in caps.items()):
                raise ValueError('人工输入必须显式声明有限小规模')
            if not artificial['description'].strip():
                raise ValueError('人工输入缺来源说明')
            root = Path(artificial['source_root'])
            if not root.is_absolute():
                raise ValueError('人工来源根必须为绝对路径')
            root = root.resolve()
            for e in [*manifest['inputs'], *manifest['references']]:
                if root not in Path(e['path']).resolve().parents:
                    raise ValueError('人工输入路径超出明确来源根')
            if any(not e['origin_uri'].startswith('fixture://') for e in manifest['inputs']):
                raise ValueError('人工MRT必须显式fixture URI，不能放宽真实来源门槛')
        self.counts = dict(counts)
        if (len(manifest['inputs']), len(self.entries), len(manifest['references']),
                len(self.references)) != (counts['mrt_sources'], counts['mrt_sources'], counts['reference_sources'], counts['reference_sources']):
            raise ValueError('输入规模与固定声明不符；真实模式要求588 MRT与11参考')
        ids = list(self.entries)
        for consumer, count in [('Resource', counts['resource_sources']),
                *((c, counts['calculation_sources']) for c in ('Feature','canonical','Detection'))]:
            rows = self.selection(consumer)
            if len(rows) != count or len({r['source_id'] for r in rows}) != count:
                raise ValueError('消费来源数量或唯一性不符：' + consumer)
            for i, row in enumerate(rows):
                rank = row['joint_mrt_rank_zero_based']
                if (type(rank) is not int or not 0 <= rank < len(ids)
                        or row['selection_sequence_zero_based'] != i
                        or ids[rank] != row['source_id']
                        or row['joint_role'] != self.entries[row['source_id']]['role']):
                    raise ValueError('消费映射全局rank/身份/角色漂移')
        expected = [manifest['baseline_source'], *manifest['update_sources']]
        if any(self.sources(c) != expected for c in ('Feature', 'canonical', 'Detection')):
            raise ValueError('F/C/D必须保留共同计算来源顺序')
        if artificial is None and [r['joint_mrt_rank_zero_based'] for r in self.selection('Resource')] != [1,2,3,4,5,6,0,7,8,297,586,587]:
            raise ValueError('Resource时间消费顺序漂移')
        if artificial is not None:
            rows = self.selection('Resource')
            times = [datetime.fromisoformat(r['nominal_time_utc'].replace('Z','+00:00')) for r in rows]
            if any(t.tzinfo is None for t in times) or any(a >= b for a,b in zip(times,times[1:])):
                raise ValueError('人工Resource仍须严格时点消费序')
            if any(self.entries[r['source_id']]['role'] not in ('baseline','snapshot') for r in rows):
                raise ValueError('人工Resource必须选择RIB')

    @classmethod
    def load(cls, manifest, mapping, input_config=None):
        config = input_config or {'profile': REAL_INPUT}
        if config == {'profile': REAL_INPUT}:
            return cls(read_fixed(manifest, MANIFEST_SHA), read_fixed(mapping, MAPPING_SHA))
        fields = {'profile','description','source_root','counts','manifest_sha256','mapping_sha256'}
        if set(config) != fields or config['profile'] != ARTIFICIAL_INPUT:
            raise ValueError('未知输入模式；真实模式不接受自定义摘要/规模')
        return cls(read_fixed(manifest, config['manifest_sha256']), read_fixed(mapping, config['mapping_sha256']), artificial=config)

    def selection(self, consumer):
        return self.mapping['consumers'][consumer]['selected_sources']

    def sources(self, consumer):
        return [r['source_id'] for r in self.selection(consumer)]

    def window(self, name):
        w = self.mapping['windows'][name]
        return [w['start'], w['end_exclusive']]


def sealed_receipt(value, fixed):
    """校对原封存回执的身份和来源；实际current资格仍须由原公共Runtime验证。"""
    if (value.get('qualification') != 'observation_sealed'
            or not isinstance(value.get('run_id'), str) or not value['run_id']
            or type(value.get('snapshot')) is not int or value['snapshot'] < 0):
        raise ValueError('必须提供实际M2封存回执')
    from data_pipeline.bgp.archive.checkpoint import sha
    if sha({k:v for k,v in value.items() if k != 'digest'}) != value.get('digest'):
        raise ValueError('M2封存回执摘要不符')
    cps = value['checkpoints']
    # 原checkpoint计划先保存reference，再保存MRT；MRT rank不等于CP ordinal。
    if [cp['source_id'] for cp in cps] != [*fixed.references, *fixed.entries]:
        raise ValueError('封存回执未覆盖固定来源原序（真实模式588/11）')
    for i, cp in enumerate(cps):
        if cp['ordinal'] != i or cp['ingest'] != 'complete' or cp['raw'] != 'verified_source_eof':
            raise ValueError('checkpoint未完整保存')
    return deepcopy(value)


def build_requests(fixed, seal, config):
    """生成原入口请求，保留显式时间/质量/参考配置；不生成任何新的业务身份。"""
    seal = sealed_receipt(seal, fixed)
    run, snapshot = seal['run_id'], seal['snapshot']
    cps = {cp['source_id']: cp for cp in seal['checkpoints']}
    if type(config['threads']) is not int or config['threads'] <= 0:
        raise ValueError('线程预算必须为正整数')
    limits = config['limits']
    for key in ('max_rss_bytes', 'min_free_bytes'):
        if type(limits[key]) is not int or limits[key] <= 0:
            raise ValueError('资源保护必须显式为正整数')
    if set(limits) != {'max_rss_bytes', 'min_free_bytes'}:
        raise ValueError('本接线不接受总处理时间上限或未知保护字段')
    dsn = config['observation_dsn']
    if not isinstance(dsn, str) or not dsn:
        raise ValueError('必须显式绑定共享M2/Resource/Feature/Canonical数据库')
    output = Path(config['output_root'])
    if not output.is_absolute():
        raise ValueError('输出根必须为Git外绝对路径')
    reference_id = config['csv_source_id']
    reference = fixed.references[reference_id]
    views = []
    for row in fixed.selection('Feature'):
        sid = row['source_id']; entry = fixed.entries[sid]
        explicit = config['feature_sources'][sid]
        window = deepcopy(explicit['window'])
        if {'input_version', 'file_ref'} & set(window):
            raise ValueError('Feature窗口身份必须来自实际M2回执')
        window.update(input_version=f'{run}:{snapshot}', file_ref=sid)
        views.append(dict(run_id=run, snapshot=snapshot, source_id=sid,
            origin_uri=entry['origin_uri'], content_sha256=entry['sha256'],
            source_role=entry['role'], calculation_role=row['calculation_role'],
            reference_sha256=reference['sha256'], expected_messages=cps[sid]['counts']['messages'],
            expected_elements=cps[sid]['counts']['elements'], profile='observation', window=window,
            message_quality_state=explicit['message_quality_state'],
            message_quality_refs=explicit['message_quality_refs']))
    feature = dict(dsn=dsn, collector=fixed.manifest['collector'], source_views=views,
        reference_view=dict(run_id=run, snapshot=snapshot, source_sha256=reference['sha256'],
            raw_path=reference['path'], expected_rows=cps[reference_id]['counts']['references'], profile='observation'),
        result_window=fixed.window('D_result'), comparison_window=fixed.window('P_comparison'),
        output=str(output/'feature'), max_rss_bytes=limits['max_rss_bytes'])
    start, end = fixed.window('canonical_detection_computation')
    canonical = dict(dsn=dsn, run_id=run, snapshot=snapshot,
        ordered_sources=fixed.sources('canonical'), profile='observation',
        output=str(output/'canonical'), calculation_window=dict(window_start=start, window_end_exclusive=end), **limits)
    detection = deepcopy(config['detection'])
    forbidden = {'input_run','input_snapshot','ordered_sources','observation_dsn','input_profile',
                 'output_profile','result_window','output','max_rss_bytes','min_free_bytes'}
    if forbidden & set(detection):
        raise ValueError('Detection配置不得覆盖固定身份/profile/窗口')
    detection.update(observation_dsn=dsn, input_run=run, input_snapshot=snapshot,
        ordered_sources=fixed.sources('Detection'), input_profile='observation',
        output_profile='detection-m3-lake/v2', result_window=dict(window_start=fixed.window('D_result')[0],
        window_end_exclusive=fixed.window('D_result')[1]), output=str(output/'detection'), **limits)
    scope = detection['scope']
    if (scope['window_start'], scope['window_end']) != (start, end):
        raise ValueError('Detection计算窗与P→D固定窗不符')
    if 'input_version' in scope:
        raise ValueError('Detection input_version必须来自实际M2回执')
    scope['input_version'] = f'{run}:{snapshot}'
    if set(detection['boundaries']) != set(fixed.manifest['update_sources']):
        raise ValueError('Detection必须逐一显式绑定全部UPDATE边界')
    for source_id, boundary in detection['boundaries'].items():
        if boundary.get('file_id') != source_id:
            raise ValueError('Detection边界file_id必须等于原UPDATE source_id')
        # 运行身份来自本次真实M2；模板来源标签仍保留在未修改的输入配置。
        boundary['source_version'] = scope['input_version']
    refs = detection['references']
    if len(refs) != len(fixed.references) or {r['source_id'] for r in refs} != set(fixed.references):
        raise ValueError('Detection参考必须恰为原固定参考集合')
    for ref in refs:
        if 'expected_rows' in ref:
            raise ValueError('参考行数必须来自原checkpoint')
        ref['expected_rows'] = cps[ref['source_id']]['counts']['references']
    return {'feature': feature, 'canonical': canonical, 'detection': detection}


def resource_requests(fixed, seal, config, registered=None):
    """登记请求先生成；Resource请求只能在原登记入口实际返回后生成。"""
    seal = sealed_receipt(seal, fixed)
    root = Path(config['output_root'])
    registration = deepcopy(config['country_reference'])
    if fixed.input_profile == ARTIFICIAL_INPUT:
        if (not registration['origin_uri'].startswith('fixture://')
                or Path(fixed.artificial['source_root']).resolve() not in Path(registration['path']).resolve().parents):
            raise ValueError('人工国家参考必须来自同一人工来源根及fixture URI')
    if {'dsn', 'output', 'operation', 'fixture_only'} & set(registration):
        raise ValueError('国家参考配置不得覆盖运行范围或模式')
    registration.update(operation='register_reference', dsn=config['observation_dsn'],
                        output=str(root/'country-reference'), **config['limits'])
    if registered is None:
        return registration, None
    if registered['state'] != 'complete':
        raise ValueError('参考尚未完成登记')
    if (registered['content_sha256'], registered['origin_uri']) != (registration['content_sha256'], registration['origin_uri']):
        raise ValueError('参考登记回执与显式来源不符')
    country = {k: registered[k] for k in ('reference_id', 'dataset_id', 'content_sha256',
        'origin_uri', 'rule_version', 'quality', 'lake_schema', 'snapshot', 'normalized_sha256')}
    csv = dict(run_id=seal['run_id'], snapshot=seal['snapshot'],
               anchor_source_id=fixed.manifest['baseline_source'], source_id=config['csv_source_id'])
    from data_pipeline.analysis.resources.identity import identity_digest
    reference_id = identity_digest({'csv': csv, 'country': country})
    sources = []
    for row in fixed.selection('Resource'):
        sid = row['source_id']; entry = fixed.entries[sid]
        context = deepcopy(config['resource_contexts'][sid])
        if {'source_id','collector','reference_id','content_sha256','origin_uri'} & set(context):
            raise ValueError('Resource上下文不得覆盖固定来源身份')
        context.update(source_id=sid, collector=fixed.manifest['collector'], reference_id=reference_id,
                       content_sha256=entry['sha256'], origin_uri=entry['origin_uri'])
        sources.append(dict(run_id=seal['run_id'], snapshot=seal['snapshot'], purpose=row['purpose'], context=context))
    request = dict(operation='resource_observation', dsn=config['observation_dsn'], sources=sources,
        result_window=fixed.window('D_result'), csv_binding=csv,
        country_binding={k: registered[k] for k in ('reference_id','dataset_id')},
        output=str(root/'resource'), **config['limits'])
    return registration, request
