"""同驱动尾段：只组合原 Country、Trend、Publication 公共入口。"""
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
import resource
import os
import threading
from datetime import datetime, timezone
import subprocess
import sys
import time
from copy import deepcopy

from data_pipeline.jobs.result_admission import admit_current, candidate_mode
from data_pipeline.jobs.stage_runner import save_new, save_final, process_sample


def bind_feature_checkpoints(fixed, seal, config):
    """仅将实际完整文件CP的解析资格接入窗口，独立业务资格仍由Feature核验。"""
    from data_pipeline.bgp.archive.checkpoint import sha
    result = deepcopy(config)
    cps = {cp['source_id']: cp for cp in seal['checkpoints']}
    for sid in fixed.sources('Feature'):
        cp = cps[sid]
        if (cp['digest'] != sha({k:v for k,v in cp.items() if k != 'digest'})
                or cp['raw'] != 'verified_source_eof' or cp['ingest'] != 'complete'):
            raise ValueError('Feature需要实际完整CP')
        counts = cp['counts']
        if counts['messages'] != counts['decoded']+counts['rejected']+counts['unsupported']:
            raise ValueError('CP解析状态计数不符')
        state = 'complete' if cp['parse']=='complete' and counts['messages']==counts['decoded'] else 'partial'
        window = result['feature_sources'][sid]
        window['message_quality_state'] = state
        window['message_quality_refs'] = [f"m2:{seal['run_id']}:{seal['snapshot']}:checkpoint:{cp['ordinal']}:{cp['digest']}"]
        window['window']['limitations'] = ['人工设定区间；CP仅证明文件解析/入库完成，Session连续性与业务资格另行判断']
    return result


@contextmanager
def measured(root, guard, *, sample_first=False, observation=None):
    # root=None 供纯材料预检使用：只在内存返回观测，不暗中创建目录。
    root = Path(root) if root is not None else None
    if root is not None: root.mkdir(parents=True, exist_ok=False)
    start = time.monotonic()
    result = dict(state='failed', processed_count=None, processed_unit=None,
        started_at=datetime.now(timezone.utc).isoformat(), driver_pid=os.getpid(),
        stage_identity=str(root.resolve()) if root is not None else 'in-memory', sample_count=0, sampling_failures=0,
        rss_coverage='unavailable', last_sample_error=None,
        first_sample_at=None, last_sample_at=None, sample_interval_seconds=0.1,
        parent_sample_peak_rss_bytes=None, children_sample_peak_rss_bytes=None,
        peak_samples={}, pg_sample_peak_rss_bytes=None,
        pg_rss_scope='Unknown：本入口不采集独立PG；另见外层PG采样',
        physical_input_bytes=None, physical_output_bytes=None, query_seconds=None,
        rss_scope='当前driver与可见后代同点ps RSS采样峰；不含PG，短命子进程可能漏采；不是独占物理内存',
        lifetime_rss_scope='同一driver启动以来RUSAGE_SELF高水位；不是独立阶段峰值，不含PG',
        stage_process_group_peak_rss_bytes=None)
    if observation is not None:
        result.update(stage=observation.get('stage'), processed_unit=observation.get('processed_unit'))
        observation.update(result)
        result = observation
    stopped = threading.Event()
    sample_lock = threading.Lock()
    accepting_samples = True
    sample_timeout = 1.0
    result['sample_command_timeout_seconds'] = sample_timeout
    result['sample_join_timeout_seconds'] = sample_timeout + 0.5

    def collect():
        try:
            values = process_sample(result['driver_pid'], timeout=sample_timeout)
            parent, children = values['parent_rss_bytes'], values['children_rss_bytes']
            if any(type(v) is not int or v < 0 for v in (parent, children)):
                raise ValueError('阶段RSS采样缺失或无效')
            with sample_lock:
                if not accepting_samples:
                    return
                at = datetime.now(timezone.utc).isoformat()
                result['sample_count'] += 1
                result['first_sample_at'] = result['first_sample_at'] or at
                result['last_sample_at'] = at
                result['rss_coverage'] = 'partial' if result['sampling_failures'] else 'sampled'
                for field, value in (('parent_sample_peak_rss_bytes', parent),
                                     ('children_sample_peak_rss_bytes', children),
                                     ('stage_process_group_peak_rss_bytes', parent+children)):
                    if result[field] is None or value > result[field]:
                        result[field] = value
                        result['peak_samples'][field] = dict(at=at, **values)
        except Exception as error:
            with sample_lock:
                if accepting_samples:
                    result['sampling_failures'] += 1
                    result['rss_coverage'] = 'partial' if result['sample_count'] else 'unavailable'
                    result['last_sample_error'] = dict(type=type(error).__name__, message=str(error)[:1024])

    def monitor():
        if sample_first: collect()
        while not stopped.wait(result['sample_interval_seconds']):
            collect()

    worker = threading.Thread(target=monitor, name='migration-stage-rss', daemon=True)
    primary = None
    try:
        try:
            worker.start()
        except Exception as error:
            result['sampling_failures'] += 1
            result['last_sample_error'] = dict(type=type(error).__name__, message=str(error)[:1024])
        guard()
        yield result
        guard(); result['state'] = 'completed'
    except BaseException as error:
        primary = error
        raise
    finally:
        stopped.set()
        try:
            if worker.ident is not None:
                worker.join(timeout=result['sample_join_timeout_seconds'])
            with sample_lock:
                accepting_samples = False
                if worker.is_alive():
                    result['sampling_failures'] += 1
                    result['rss_coverage'] = 'partial' if result['sample_count'] else 'unavailable'
                    result['last_sample_error'] = dict(type='TimeoutError', message='观测收尾超时；已关闭样本接收')
            result.update(ended_at=datetime.now(timezone.utc).isoformat(),
                wall_seconds=time.monotonic()-start,
                driver_lifetime_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024))
        except BaseException as error:
            if primary is None:
                raise
            primary.migration_cleanup_errors = [*getattr(primary, 'migration_cleanup_errors', []),
                dict(operation='stage_metrics_cleanup', type=type(error).__name__, message=str(error))]
        finally:
            # 收尾自身被打断时也禁止后台采样继续修改已交付结果。
            with sample_lock:
                accepting_samples = False
            if root is not None: save_final(root/'阶段观测.json', result, primary)


def full_preflight(fixed, config):
    """必须在创建任何运行目录和连接前执行；配置不是生产许可的替身。"""
    from data_pipeline.jobs.input_plan import ARTIFICIAL_INPUT
    if config.get('enabled') is not True:
        raise ValueError('运行配置未显式启用，禁用草案不得调用producer')
    if fixed.input_profile != ARTIFICIAL_INPUT:
        raise ValueError('本次尾段仅接已批准人工输入；真实全天仍待恢复前置验收')
    repo = Path(__file__).resolve().parents[3]
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
    if config.get('execution_code') != head or subprocess.check_output(
            ['git', 'status', '--porcelain', '--untracked-files=normal'], cwd=repo, text=True).strip():
        raise ValueError('必须绑定干净的最终接受源码提交')
    tail = config['tail']
    if tail['accepted'] is not True:
        raise ValueError('尾段尚未独立接受')
    # 正式owner角色与派生参考扩展必须已进入同一源码树。
    from data_pipeline.results.component_readers import api
    from data_pipeline.results.fixture_metadata import ArtificialInput
    import inspect
    api('trend')
    if 'trend_reference' not in inspect.signature(ArtificialInput).parameters:
        raise ValueError('Publication派生参考正式合同未接入')
    if not all(config.get(k) for k in ('observation_dsn', 'runtime', 'm2')):
        raise ValueError('上游运行绑定缺失')
    if config['m2'].get('mode') != 'checkpoint' or not config['m2'].get('config'):
        raise ValueError('首条人工链必须显式绑定新的M2 checkpoint配置')
    import json
    m2_config = json.loads(Path(config['m2']['config']).read_text())
    if m2_config.get('dsn') != config['observation_dsn'] or 'read_policy' not in m2_config:
        raise ValueError('M2必须绑定同一数据库及原读取策略')
    country, trend, reference, publication = (tail[k] for k in ('country','trend','reference','publication'))
    from data_pipeline.analysis.country_events.result_admission import Runtime as CountryRuntime
    from data_pipeline.analysis.country_trends.runtime import ProductionRuntime as TrendRuntime
    # 只绑定签名，不构造Runtime、连接数据库或制造Admission。
    inspect.signature(CountryRuntime).bind(**country['runtime'], dsn=country['dsn'],
        output_root=country['query_root'], dependency_admissions=(), dependency_runtimes={},
        expected_country_binding=None, **candidate_mode(fixed.input_profile))
    inspect.signature(TrendRuntime).bind(**trend['runtime'], dsn=trend['dsn'],
        country_window_us=tuple(trend['window_us']), reference_binding=None, feature_selections=(),
        dependency_admissions=(), dependency_runtimes={}, **candidate_mode(fixed.input_profile))
    if any(type(c.get('dsn')) is not str or not c['dsn'] for c in (country,trend,reference,publication)):
        raise ValueError('尾段DSN未绑定')
    from psycopg2.extensions import parse_dsn
    for c in (country,trend,reference,publication):
        dsn = parse_dsn(c['dsn'])
        if not dsn.get('host') or not dsn.get('dbname') or dsn.get('service'):
            raise ValueError('尾段DSN必须显式主机与数据库')
    for c in (country['input_limits'], country['capture'], country['c2_limits'],
              country['component_limits'], country['audit_limits'], country['query_limits'],
              country['qualification_limits'], country['runtime']['limits'],
              trend['runtime']['limits'], reference['limits'], publication['limits']):
        for key, value in c.items():
            if key != 'scratch_root' and (type(value) is not int or value <= 0):
                raise ValueError('尾段预算必须显式为正整数：'+key)
    if reference['dsn'] != trend['dsn']:
        raise ValueError('Trend参考须登记到实际Trend数据库')
    if tuple(country['window_us']) != tuple(trend['window_us']) or tuple(reference['window_us']) != tuple(country['window_us']):
        raise ValueError('Country/Trend/参考结果窗必须相同')
    from data_pipeline.analysis.detection.result_window import instant
    result = tuple(int(instant(t).timestamp()*1000000) for t in fixed.window('D_result'))
    if tuple(country['window_us']) != result:
        raise ValueError('尾段结果窗不等于冻结D窗口')
    from data_pipeline.analysis.country_events.compute import Grid
    grid = Grid(**dict(country['grid'], samples_us=tuple(country['grid']['samples_us'])))
    calc = tuple(int(instant(t).timestamp()*1000000) for t in fixed.window('canonical_detection_computation'))
    if (grid.input_start_us,grid.input_end_us) != calc:
        raise ValueError('Country计算窗不等于冻结原计算窗')
    frozen = Path(fixed.artificial['source_root']).resolve(strict=True)
    run_root = Path(config['run_root'])
    if not run_root.is_absolute() or run_root.resolve() != run_root:
        raise ValueError('必须绑定独立绝对运行根')
    raw = Path(reference['source_path'])
    if frozen not in raw.resolve(strict=True).parents:
        raise ValueError('独立比较原件不在冻结raw根')
    from data_pipeline.jobs.trend_reference import read_comparison
    read_comparison(raw, reference['source_sha256'])
    paths = [config['invocation_root'], config['output_root'], reference['derived_root'], publication['private_root'],
             country['component_root'], country['query_root'], country['c2_scratch_root'],
             country['capture']['scratch_root'], country['runtime']['scratch_root'],
             trend['runtime']['output_root'], trend['runtime']['scratch_root']]
    paths += [config['runtime'][name]['scratch_root'] for name in ('resource','feature','canonical','detection')]
    paths += [config['runtime']['m2']['scratch_parent']]
    # 与原observations CLI/checkpoint的实际条件写路径一致，包含默认湖目录。
    m2_writes = [m2_config.get('catalog_data_path') or str(Path(config['output_root'])/'m2'/'data')]
    readonly = m2_config.get('require_readonly_inputs', False)
    if type(readonly) is not bool:
        raise ValueError('M2只读挂载检查必须显式为bool')
    if readonly:
        m2_writes.append(m2_config['preflight_receipt'])
    paths += m2_writes
    for value in paths:
        p = Path(value)
        if not p.is_absolute() or p.resolve() != p or any(q.is_symlink() for q in (p,*p.parents)):
            raise ValueError('运行路径必须是无别名绝对路径')
        if p == frozen or frozen in p.parents or p in frozen.parents or p == repo or repo in p.parents:
            raise ValueError('运行写根不能交叠冻结原件或位于Git树内')
        if run_root not in p.parents:
            raise ValueError('本次全部写路径必须在同一独立运行根内')
    separate = [Path(reference['derived_root']), Path(publication['private_root']),
                Path(country['component_root']), Path(country['query_root']),
                Path(country['c2_scratch_root']), Path(country['capture']['scratch_root']),
                Path(country['runtime']['scratch_root']), Path(trend['runtime']['output_root']),
                Path(trend['runtime']['scratch_root'])]
    separate += [Path(config['runtime'][name]['scratch_root']) for name in ('resource','feature','canonical','detection')]
    separate += [Path(config['runtime']['m2']['scratch_parent']),Path(config['invocation_root'])]
    separate += [Path(p) for p in m2_writes]
    if any(a == b or a in b.parents or b in a.parents for i,a in enumerate(separate) for b in separate[i+1:]):
        raise ValueError('尾段输出、派生参考及scratch必须分别隔离')
    if any(Path(p).exists() for p in paths):
        raise ValueError('本次运行不可覆盖或自动续接旧产物')
    if type(publication['expected_generation']) is not int or publication['expected_generation'] < 0:
        raise ValueError('Publication必须显式绑定generation')


def combined_request(pairs, artificial):
    """实际AD图→正式known_role；不合成AD/Token/资格主值。"""
    from data_pipeline.results.manifest_contract import CONTRACT, PROFILES, validate_structure
    from data_pipeline.results.component_roles import known_role
    from data_pipeline.bgp.archive.value_codec import digest
    nodes, by_id = {}, {}
    components, dependencies = [], []
    for runtime, a in pairs:
        aid = a['admission_id']
        if aid in by_id:
            if by_id[aid][1] != a or by_id[aid][0] is not runtime:
                raise ValueError('同Admission出现不同原值或Runtime')
            continue
        by_id[aid] = runtime, a
        key = a['owner'] if a['owner'] in ('resource','feature','detection','country','trend') else a['owner']+':'+aid
        if key in nodes:
            raise ValueError('组合主体重复')
        nodes[key] = a
        if ':' in key:
            dependencies.append(dict(dependency_key=key, owner=a['owner'], admission=a))
        else:
            components.append(a)
    keys = {a['admission_id']: key for key, a in nodes.items()}
    roles, edges = [], []
    for key, a in sorted(nodes.items()):
        for aid in a['dependencies']:
            if aid not in keys:
                raise ValueError('实际AD图依赖缺失')
            dk = keys[aid]
            role = known_role(key, a, dk, nodes[dk]); roles.append(role)
            edges.append(dict(consumer_key=key, dependency_key=dk, role=role['role'],
                expected_admission_id=aid, selection_digest=digest(role)))
    revisions = {name: next(a['owner_revision'] for a in nodes.values() if a['owner']==name)
                 for name in ('canonical','resource','feature','detection','country','trend')}
    request = dict(contract=CONTRACT, profile=PROFILES['artificial-m3-combined-country-trend/v1'],
        dependency_revisions=revisions, components=sorted(components,key=lambda a:a['owner']),
        dependencies=sorted(dependencies,key=lambda a:a['dependency_key']), edges=edges, role_graph=roles,
        artificial_input=artificial.verify(list(nodes.values())))
    return validate_structure(request), {aid: runtime for aid, (runtime,a) in by_id.items()}


def run_tail(fixed, config, pairs, root, guard, *, resume_context=None):
    from data_pipeline.jobs.country_events import produce_country
    from data_pipeline.jobs.trend_reference import prepare_context
    from data_pipeline.analysis.country_trends.runtime import ProductionRuntime, Runtime
    from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
    from data_pipeline.analysis.country_trends.stream_store import prepare_trend_m3
    from data_pipeline.analysis.country_trends import result_admission as trend_owner
    from data_pipeline.results import Publication
    from data_pipeline.results.manifest_io import Limits
    from data_pipeline.results.fixture_metadata import ArtificialInput
    tail = config['tail']; root = Path(root)
    country_config, trend_config = tail['country'], tail['trend']
    if resume_context is None:
        for p in (country_config['c2_scratch_root'], country_config['capture']['scratch_root'],
                  country_config['runtime']['scratch_root'], trend_config['runtime']['output_root'],
                  trend_config['runtime']['scratch_root'], tail['reference']['derived_root'],
                  tail['publication']['private_root']):
            Path(p).mkdir(parents=True, exist_ok=False, mode=0o700)
        country_deps = [p for p in pairs if p[1]['owner'] in ('canonical','detection')]
        detection = next(p[1] for p in country_deps if p[1]['owner']=='detection')
        country_deps += [p for p in pairs if p[1]['admission_id'] in detection['dependencies']]
        with measured(root/'country', guard) as metrics:
            country = produce_country(country_config, country_deps, root/'country', guard,
                                      input_profile=fixed.input_profile)
            from data_pipeline.analysis.country_events.selection_contract import contract_value
            from data_pipeline.analysis.country_events.snapshot_schema import decode
            proof = contract_value(decode(country[1]['owner_binding'])['proof'])
            metrics.update(processed_count=proof.source_rows, processed_unit='原Country C4 proof.source_rows')
        pairs.append(country)
        feature = next(p for p in pairs if p[1]['owner']=='feature')
        with measured(root/'trend-context', guard):
            selections, reference = prepare_context(tail['reference'], country, feature,
                                                   root/'trend-context'/'capture', guard)
    else:
        # 已完成Country及上下文：仅新建Trend/P写根，不捕获或准入原上游。
        country = next(p for p in pairs if p[1]['owner']=='country')
        feature = next(p for p in pairs if p[1]['owner']=='feature')
        selections, reference = resume_context
        for p in (trend_config['runtime']['output_root'],trend_config['runtime']['scratch_root'],tail['publication']['private_root']):
            Path(p).mkdir(parents=True,exist_ok=False,mode=0o700)
    options = dict(trend_config['runtime'], dsn=trend_config['dsn'],
        country_window_us=tuple(trend_config['window_us']), feature_selections=selections,
        reference_binding=reference['binding'], dependency_admissions=(country[1], feature[1]),
        dependency_runtimes={a['admission_id']:r for r,a in (country, feature)},
        **candidate_mode(fixed.input_profile))
    options['limits'] = S2Limits(**options['limits'])
    with measured(root/'trend', guard) as metrics:
        produced = prepare_trend_m3(ProductionRuntime(**options), guard=guard)
        save_new(root/'trend'/'原Trend返回.json', produced)
        runtime = Runtime(**dict(options, output_root=produced['binding']['root']),
                          expected_trend_binding=produced)
        trend = runtime, admit_current(trend_owner, runtime, produced, root/'trend-admit', guard)
        metrics['processed_unit'] = '原Trend proof.body.tables行数；见原返回，不混作输入量'
        metrics['processed_count'] = sum(t['rows'] for t in produced['proof']['body']['tables'].values())
    pairs.append(trend)
    return publish_pairs(config,pairs,root,guard,reference)


def publish_pairs(config,pairs,root,guard,reference):
    """共享仅P尾段；观测根独立于控制根，调用既有发布动作与状态记录。"""
    from data_pipeline.results import Publication
    from data_pipeline.results.manifest_io import Limits
    from data_pipeline.results.fixture_metadata import ArtificialInput
    from data_pipeline.results.retry.driver import perform_action
    root=Path(root);tail=config['tail']
    artificial = ArtificialInput(manifest=config['manifest'], mapping=config['mapping'],
        input=config['input'], country_reference=config['country_reference'], trend_reference=reference)
    request, runtimes = combined_request(pairs, artificial)
    if config.get('expected_publication_requests') is not None:
        from data_pipeline.results.component_streams import requests
        count=sum(len(requests(a['owner'],batch_rows=1,batch_bytes=1,admission=a)) for a in request['components'])
        if len(request['components']) != 5 or count != config['expected_publication_requests']:
            raise ValueError('固定人工组合必须为5组件72请求')
    pc = tail['publication']
    observation=root/'publication-observation'
    with measured(observation,guard):
        save_new(observation/'原组合请求.json',request)
        publication=Publication(pc['dsn'],pc['private_root'],limits=Limits(**pc['limits']),
                                combined=runtimes,combined_input=artificial)
        if config.get('require_pristine_publication'):
            require_pristine_publication(publication,config['publication_physical'])
        if not config.get('failed_publication'):publication.initialize()
        status=dict(state='failed',last_completed_state=None,publication_state='not_attempted',generation=None)
        checked=dict(request=request,spec=dict(action='publish',migrate_profiles=not bool(config.get('failed_publication')),
            expected_generation=pc['expected_generation'],selector='artificial:m3:combined-country-trend'))
        primary=None
        try:
            perform_action(publication,checked,pc,observation,status)
        except BaseException as error:
            primary=error;status.update(error_type=type(error).__name__,error=str(error));raise
        finally:save_final(observation/'发布结果.json',status,primary)
    return dict(status,state='artificial_combined_published',
        science_coverage='见原Country/Trend资格与Publication完整流记录；Unknown不升级')


def require_pristine_publication(publication,expected):
    """只读确认指定空控制空间；任何既有schema/文件均不自动接管。"""
    import psycopg2
    from contextlib import closing
    from data_pipeline.results.manifest_io import require
    with closing(psycopg2.connect(publication.dsn)) as pg:
        pg.set_session(readonly=True,isolation_level='REPEATABLE READ')
        with pg.cursor() as c:
            c.execute('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')
            require(c.fetchone()==tuple(expected),'指定P库物理身份漂移')
            c.execute("SELECT to_regnamespace('publication_q1')")
            require(c.fetchone()==(None,),'指定P库已有控制schema，停止并另行分类')
    require(publication.root.is_dir() and not any(publication.root.iterdir()),'指定P根不再为空，停止')


def require_failed_publication(publication,expected,failure):
    """只接已知无Token失败构建；先读控制状态，保留失败行与目录。"""
    import psycopg2
    from contextlib import closing
    from data_pipeline.results.manifest_io import require
    from data_pipeline.results.retry.driver import verify_profile
    verify_profile(publication,'artificial:m3:combined-country-trend')
    with closing(psycopg2.connect(publication.dsn)) as pg:
        pg.set_session(readonly=True,isolation_level='REPEATABLE READ')
        with pg.cursor() as c:
            c.execute('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')
            require(c.fetchone()==tuple(expected),'指定P库物理身份漂移')
            publication._owner(c)
            c.execute('SELECT build_id,state,input_digest,manifest,manifest_path,manifest_sha,publication_id FROM publication_q1.builds')
            require(c.fetchall()==[(failure['build_id'],'failed',failure['input_digest'],None,None,None,None)],'P构建不是指定无Token失败状态')
            c.execute('SELECT selector,build_id,publication_id,generation FROM publication_q1.head')
            require(c.fetchall()==[],'P已有head，不得按失败构建续接')
    build_root=publication.root/'q1-builds'
    require(build_root.is_dir() and {p.name for p in build_root.iterdir()}=={failure['build_id']},'P构建目录与失败身份不符')
    require((build_root/failure['build_id']).is_dir() and not (build_root/failure['build_id']).is_symlink(),'失败构建目录无效')
    return dict(state='known_failed_without_token',physical=expected,**failure,head=None,history_preserved=True)
