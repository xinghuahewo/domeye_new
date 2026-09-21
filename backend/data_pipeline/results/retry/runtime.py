"""从原配置及完整 AD 构造消费 Runtime；不调用 inspect/admit/producer。"""
from copy import deepcopy
from pathlib import Path
from data_pipeline.jobs.result_admission import candidate_mode
from data_pipeline.jobs.input_plan import ARTIFICIAL_INPUT
from data_pipeline.results.component_roles import codec
from data_pipeline.results.manifest_io import require


def specification(config, admission, attempt):
    owner=admission['owner'];b=codec(owner).untyped(admission['owner_binding'])
    if owner in ('country','trend'):
        section=config['tail'][owner];options=deepcopy(section['runtime']);options['dsn']=section['dsn']
        options['output_root']=config['tail']['country']['query_root'] if owner=='country' else b['binding']['root']
    else:
        options=deepcopy(config['runtime']['m2' if owner=='reference' else owner])
        # 与 build_requests→admit_detection 相同：业务库和观察依赖库分离。
        options['dsn']=config['detection']['detection_dsn'] if owner=='detection' else config['observation_dsn']
        options.pop('scratch_parent',None)
        if owner not in ('m2','reference'):
            options['output_root']=str(Path(config['output_root'])/owner/('business' if owner=='detection' else ''))
    options['scratch_root']=str(Path(attempt)/'scratch'/admission['admission_id'])
    options['allowed_roots']=[*options['allowed_roots'],str(Path(attempt))]
    deps=admission['dependencies']
    if owner=='trend':
        inputs=codec(owner).untyped(b['proof']['inputs_typed'])
        options.update(country_window_us=inputs['window_us'],feature_selections=inputs['feature_selections'],
                       reference_binding=inputs['reference_binding'])
        deps=[a['admission_id'] for a in inputs['dependencies']]
    require(set(deps)==set(admission['dependencies']), 'Runtime原依赖绑定错误')
    return options,tuple(deps)


def construct(config, admissions, attempt, *, progress=None):
    """目录准备之后调用；构造器可检查本地路径/资源，仍不连接 PG。"""
    from data_pipeline.results.component_readers import api
    by_id={a['admission_id']:a for a in admissions};runtimes={};visiting=set()
    def make(aid):
        if aid in runtimes:return runtimes[aid]
        require(aid not in visiting,'Runtime依赖环');visiting.add(aid)
        a=by_id[aid];owner=a['owner'];b=codec(owner).untyped(a['owner_binding'])
        options,order=specification(config,a,attempt)
        deps={dep:make(dep) for dep in order}
        options.update(candidate_mode(ARTIFICIAL_INPUT))
        options['scratch_root']=Path(options['scratch_root'])
        if 'output_root' in options:options['output_root']=Path(options['output_root'])
        if owner in ('m2','reference'):
            mb=b if owner=='m2' else b['m2_binding']
            options.update(input_manifest=mb['plan']['manifest'],expected_m2_binding=mb,
                           dependency_admissions=tuple(by_id[d] for d in order))
        else:
            options.update(dependency_admissions=tuple(by_id[d] for d in order),dependency_runtimes=deps)
            options['expected_'+owner+'_binding']=b
            if owner=='resource':
                parents=[d for d in order if by_id[d]['owner']=='m2']
                require(len(parents)==1,'Resource缺唯一原M2 Runtime');options['upstream_runtime']=deps[parents[0]]
            elif owner=='canonical':
                options['input_manifest']=b['descriptor']['plan']['input_manifest']
            elif owner=='country':
                from data_pipeline.analysis.country_events.qualified_reader import ResultLimits
                options['limits']=ResultLimits(**options['limits'])
            elif owner=='trend':
                from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
                options['limits']=S2Limits(**options['limits'])
        runtime=api(owner).Runtime(**options)
        runtimes[aid]=runtime;visiting.remove(aid)
        if progress is not None:progress(len(runtimes))
        return runtime
    for aid in by_id:make(aid)
    return runtimes
