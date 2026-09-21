"""固定输入的顺序驱动；显式人工尾段只消费同次实际返回。"""
from dataclasses import asdict
import json
from pathlib import Path
import sys

from data_pipeline.jobs.input_plan import build_requests, resource_requests, sealed_receipt
from data_pipeline.jobs.stage_runner import run_stage, save_new, save_final


ROOT = Path(__file__).resolve().parents[3]


def read_last_json(path, max_bytes):
    """只读取有界尾部；末条JSON超过上限就保留原日志并拒绝解码。"""
    if type(max_bytes) is not int or max_bytes < 1:
        raise ValueError('回执字节上限必须为正整数')
    with Path(path).open('rb') as stream:
        size = stream.seek(0, 2)
        start = max(0, size-max_bytes-2)
        stream.seek(start)
        tail = stream.read(max_bytes+2).rstrip(b'\r\n')
    if start and b'\n' not in tail:
        raise ValueError('末条JSON超过回执字节上限')
    line = tail.rsplit(b'\n', 1)[-1]
    if len(line) > max_bytes:
        raise ValueError('末条JSON超过回执字节上限')
    return json.loads(line)


def invoke(name, argv, config, root):
    stage = root/name
    observation = run_stage(argv, stage, threads=config['threads'], disk_roots=config['disk_roots'], **config['limits'])
    try:
        # 原入口最终一行是JSON；原日志完整保留，不从业务目录存在推断成功。
        result = read_last_json(stage/'stdout.log', config['max_receipt_bytes'])
        if type(result) is not dict:
            raise ValueError('原入口未返回对象回执')
        save_new(stage/'原返回.json', result)
        counts = []
        if name == 'm2':
            for key,unit in [('messages','保存消息'),('elements','保存元素'),('references','保存参考行')]:
                counts.append(dict(value=sum(cp['counts'][key] for cp in result['checkpoints']),
                                   unit=unit, source='原seal.checkpoints.counts.'+key))
        for table,count in result.get('actual_rows', {}).items():
            counts.append(dict(value=count, unit='保存表行', table=table, source='原返回.actual_rows'))
        if name == 'register-reference':
            counts.append(dict(value=result['row_count'], unit='参考顶层对象', source='原返回.row_count'))
        for counter in counts:
            counter['per_second'] = counter['value']/observation['wall_seconds'] if observation['wall_seconds'] > 0 else None
        save_new(stage/'计数与产率.json', dict(counters=counts,
            unknown_if_absent=True, wall_scope='整个原入口进程，包括冻结及启动；不是独立算法时间',
            physical_input_bytes=None, physical_output_bytes=None,
            receipt_file_bytes=(stage/'原返回.json').stat().st_size,
            byte_scope='仅本地JSON回执文件长度；不冒充数据库/原始输入物理IO'))
        return result
    except BaseException as exc:
        save_final(stage/'回执读取失败.json', dict(error_type=type(exc).__name__, error=str(exc)))
        raise


def produce_request(name, request, script, config, root):
    path = root/(name+'-request.json')
    save_new(path, request)
    path.chmod(0o600)
    return invoke(name, [sys.executable, '-B', str(ROOT/'scripts/pipeline'/script), str(path)], config, root)


def canonical_worker(path):
    """仅供显式离线驱动调用原Canonical producer。"""
    from data_pipeline.bgp.archive.message_reader import ObservationReader
    from data_pipeline.bgp.replay.route_snapshot import produce_projection
    request = json.loads(Path(path).read_text())
    reader = ObservationReader(request.pop('dsn'), request.pop('run_id'), request.pop('snapshot'),
        request.pop('ordered_sources'), profile=request.pop('profile'))
    return asdict(produce_projection(reader, **request))


def run_fixed(fixed, config, root, guard, *, full=False):
    from data_pipeline.jobs import result_admission as admission
    if config.get('enabled') is not True:
        raise ValueError('运行配置未显式启用，禁用草案不得调用producer')
    if full:
        if Path(root) != Path(config['invocation_root']):
            raise ValueError('实际驱动输出根不等于已核配置')
        from data_pipeline.jobs.downstream import full_preflight
        full_preflight(fixed, config)
    root = Path(root)
    root.mkdir(parents=True, exist_ok=False, mode=0o700)
    status = dict(state='failed', scope='fixed_prefix_only', full_chain='unavailable',
                  input_profile=fixed.input_profile, input_counts=fixed.counts,
                  runtime_profile='real-candidate/v1', artificial=fixed.artificial)
    try:
        if full:
            for name in ('resource','feature','canonical','detection'):
                Path(config['runtime'][name]['scratch_root']).mkdir(parents=True, exist_ok=False, mode=0o700)
        m2 = config['m2']
        if m2['mode'] == 'reuse':
            seal = sealed_receipt(json.loads(Path(m2['receipt']).read_text()), fixed)
            save_new(root/'原M2封存回执.json', seal)
        elif m2['mode'] == 'checkpoint':
            original = json.loads(Path(m2['config']).read_text())
            if original['dsn'] != config['observation_dsn'] or 'read_policy' not in original:
                raise ValueError('M2必须绑定同一数据库及显式原读取策略')
            seal = invoke('m2', [sys.executable, '-B', str(ROOT/'scripts/observations.py'),
                'checkpoint', '--config', m2['config'], '--manifest', config['manifest'],
                '--output', str(Path(config['output_root'])/'m2')], config, root)
            sealed_receipt(seal, fixed)
        else:
            raise ValueError('M2模式须为reuse或显式checkpoint')
        if full:
            from data_pipeline.jobs.downstream import bind_feature_checkpoints
            config = bind_feature_checkpoints(fixed, seal, config)
            save_new(root/'实际Feature窗口绑定.json', config['feature_sources'])
        requests = build_requests(fixed, seal, config)
        # 仅两份不同的合法M2选择；每个owner依赖集合内部不重复同binding_id。
        r_ids = set(fixed.sources('Resource'))
        groups = dict(resource=[sid for sid in fixed.entries if sid in r_ids],
                      consumers=fixed.sources('canonical'))
        bound = {}; pairs = []
        for name,sources in groups.items():
            options = dict(config['runtime']['m2'], dsn=config['observation_dsn'])
            scratch = Path(options.pop('scratch_parent'))/name
            scratch.mkdir(parents=True, exist_ok=False)
            options['scratch_root'] = scratch
            rt, ma = admission.admit_m2(options, fixed.manifest, seal, sources, root/(name+'-m2-admit'), guard, input_profile=fixed.input_profile)
            refs = {}
            needed = [config['csv_source_id']] if name == 'resource' else list(fixed.references)
            for source in needed:
                refs[source] = admission.admit_reference(rt, ma, source, root/(name+'-reference-'+source), guard)
            bound[name] = rt, ma, refs
            pairs.extend([(rt, ma), *((rt, a) for a in refs.values())])
        registration, _ = resource_requests(fixed, seal, config)
        registered = produce_request('register-reference', registration, 'resource-frozen-run.py', config, root)
        _, request = resource_requests(fixed, seal, config, registered)
        resource = produce_request('resource', request, 'resource-frozen-run.py', config, root)
        rr, rm, refs = bound['resource']
        options = dict(config['runtime']['resource'], dsn=config['observation_dsn'], output_root=request['output'])
        pairs.append(admission.admit_resource(options, resource, [(rr,rm),(rr,refs[config['csv_source_id']])], root/'resource-admit', guard, input_profile=fixed.input_profile))
        cr, cm, refs = bound['consumers']
        dependencies = [(cr,cm), *((cr,r) for r in refs.values())]
        feature = produce_request('feature', requests['feature'], 'feature-frozen-run.py', config, root)
        pairs.append(admission.admit_feature(dict(config['runtime']['feature'], dsn=config['observation_dsn'],
            output_root=requests['feature']['output']), feature,
            [(cr,cm),(cr,refs[config['csv_source_id']])], root/'feature-admit', guard, input_profile=fixed.input_profile))
        canonical = produce_request('canonical', requests['canonical'], 'migration-canonical-run.py', config, root)
        pairs.append(admission.admit_canonical(dict(config['runtime']['canonical'], dsn=config['observation_dsn'],
            output_root=requests['canonical']['output']), fixed.manifest, canonical, dependencies, root/'canonical-admit', guard, input_profile=fixed.input_profile))
        produce_request('detection', requests['detection'], 'detection-frozen-run.py', config, root)
        ready = json.loads((Path(requests['detection']['output'])/'business/ready.json').read_text())
        pairs.append(admission.admit_detection(dict(config['runtime']['detection'], dsn=requests['detection']['detection_dsn'],
            output_root=str(Path(requests['detection']['output'])/'business')), ready, dependencies, root/'detection-admit', guard, input_profile=fixed.input_profile))
        status['state'] = 'fixed_prefix_current_verified'
        if full:
            from data_pipeline.jobs.downstream import run_tail
            status.update(state='failed', scope='artificial_full_chain',
                          completed_prefix=True, full_chain='not_completed')
            result = run_tail(fixed, config, pairs, root, guard)
            status.update(scope='artificial_full_chain', full_chain=result)
            status['state'] = result['state']
    finally:
        save_final(root/'驱动回执.json', status)
    return status
