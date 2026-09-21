"""固定人工制品的新准入与既有尾段接线；不调用MRT或上游科学producer。"""
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

from data_pipeline.results.manifest_io import require
from data_pipeline.results.retry.driver import material
from data_pipeline.results.retry.runtime import construct
from data_pipeline.results.component_readers import api
from data_pipeline.results.component_roles import codec
from data_pipeline.jobs.result_admission import admit_current
from data_pipeline.jobs.stage_runner import save_new
from data_pipeline.jobs.downstream import run_tail
from data_pipeline.results.retry.observation import stage

REUSE = {'m2', 'reference', 'detection'}
READMIT = {'canonical', 'resource', 'feature'}


def preflight(spec):
    """只读控制材料；不连接数据库或创建目录。"""
    require(spec['contract']=='artificial-requalify/v1','未知人工再准入合同')
    repo=Path(__file__).resolve().parents[3]
    require(subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==spec['execution_code'],'源码提交不符')
    require(not subprocess.check_output(['git','status','--porcelain'],cwd=repo,text=True).strip(),'源码树须干净')
    if spec.get('resume_from') in ('country_complete','publication_only'):return resume_preflight(spec)
    original=material(spec['source_config']);request=material(spec['source_request'])
    require(original['input']['profile']=='artificial/v1' and request['profile']['data_kind']=='artificial','仅允许原人工输入')
    saved=[material(p) for p in spec['admissions']]
    values=request['components']+[d['admission'] for d in request['dependencies']]
    require(len(saved)==len(values) and {a['admission_id']:a for a in saved}=={a['admission_id']:a for a in values},'原AD图不符')
    selected=[a for a in saved if a['owner'] in REUSE|READMIT]
    require({a['owner'] for a in selected}==REUSE|READMIT,'上游主体缺失')
    require(all(set(a['dependencies'])<={v['admission_id'] for v in selected} for a in selected),'上游依赖不闭合')
    from data_pipeline.jobs.input_plan import FixedInputs
    FixedInputs.load(original['manifest'],original['mapping'],original['input'])
    config=json.loads(json.dumps(original));config['tail']=spec['tail']
    config['expected_publication_requests']=72
    root=Path(spec['attempt_root'])
    require(root.is_absolute() and root.resolve()==root and not root.exists(),'新候选根无效或已存在')
    protected={Path(original['run_root']),Path(original['input']['source_root']),repo}
    require(all(not(root.is_relative_to(p) or p.is_relative_to(root)) for p in protected),'候选根交叠原件')
    for name in ('country','trend','reference'):
        old=original['tail'][name];new=config['tail'][name]
        # 科学窗口、参数和预算均复用；仅路径/新库与来源落盘预算允许变化。
        def shape(value):
            if isinstance(value,dict):return {k:shape(v) for k,v in value.items() if k not in ('dsn','allowed_roots','scratch_root','output_root','component_root','query_root','c2_scratch_root','derived_root','max_disk_bytes')}
            return value
        require(shape(old)==shape(new),'原人工参数改变:'+name)
    pc=config['tail']['publication'];oldpc=original['tail']['publication']
    require({k:v for k,v in pc.items() if k not in ('dsn','private_root')}=={k:v for k,v in oldpc.items() if k not in ('dsn','private_root')},'原发布预算改变')
    from psycopg2.extensions import parse_dsn
    base=parse_dsn(original['observation_dsn'])
    require(spec['source_databases']=={parse_dsn(original['observation_dsn'])['dbname']:16384,parse_dsn(original['detection']['detection_dsn'])['dbname']:16385},'原物理库声明不符')
    require(spec['system_identifier']=='7685154057349199474','原cluster身份不符')
    for name in ('country','trend','publication'):
        dsn=parse_dsn(config['tail'][name]['dsn'])
        require(all(dsn.get(k)==base.get(k) for k in ('host','port','user')) and dsn['dbname']==spec['new_databases'][name],'新库连接越界')
        require(dsn['dbname'] not in {parse_dsn(original['tail'][n]['dsn'])['dbname'] for n in ('country','trend','publication')},'不得写入原尾段库')
    require(len(set(spec['new_databases'].values()))==3,'新库名称重复')
    require(config['tail']['reference']['dsn']==config['tail']['trend']['dsn'],'参考库不符')
    paths=[]
    def writes(value):
        if isinstance(value,dict):
            for k,v in value.items():
                if k in ('scratch_root','output_root','component_root','query_root','c2_scratch_root','derived_root','private_root'):paths.append(Path(v))
                else:writes(v)
    writes(config['tail'])
    require(all(p.resolve()==p and p.is_relative_to(root) and p!=root for p in paths),'新写根不在候选内')
    require(len(set(paths))==len(paths),'新写根重复')
    return config,selected


def qualify(config,selected,root,guard):
    """旧AD只对未变owner调用current；三个变化owner正常admit，不改签。"""
    runtimes=construct(config,selected,root)
    pairs=[]
    for owner in ('m2','reference','detection','canonical','resource','feature','country','trend'):
        for old in (a for a in selected if a['owner']==owner):
            rt=runtimes[old['admission_id']];module=api(owner)
            with stage(root,'upstream-'+old['admission_id'], '完成原生资格检查的AD数',guard) as metrics:
                if owner not in config.get('readmit_owners',()) and (owner in REUSE or config.get('reuse_all_admissions')):
                    module.verify_current(rt,old,guard=guard);new=old
                else:
                    new=admit_current(module,rt,codec(owner).untyped(old['owner_binding']),root/(owner+'-admit'),guard)
                pairs.append((rt,new));metrics['processed_count']=1
                save_new(root/('资格映射-'+old['admission_id']+'.json'),dict(owner=owner,old=old['admission_id'],new=new['admission_id'],science_recomputed=False))
    return pairs


def execute(spec):
    require(spec.get('enabled') is True,'禁用安排不能执行')
    config,selected=preflight(spec)
    # 旧科学库不改物理环境；新DB必须由已授权看护新建，不能冒用旧OID。
    import psycopg2
    from contextlib import closing
    from psycopg2.extensions import parse_dsn
    identities={}
    for dsn in (config['observation_dsn'],config['detection']['detection_dsn'],*[config['tail'][n]['dsn'] for n in ('country','trend','publication')]):
        with closing(psycopg2.connect(dsn)) as pg:
            pg.set_session(readonly=True)
            with pg.cursor() as c:
                c.execute('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')
                sid,oid=c.fetchone();name=parse_dsn(dsn)['dbname']
                require(sid==spec['system_identifier'],'cluster漂移')
                if name in spec['source_databases']:require(oid==spec['source_databases'][name],'旧库OID漂移')
                else:require(oid not in (16384,16385,16386,16387,16388),'新库不得冒用旧OID')
                identities[name]=[sid,oid]
    root=Path(spec['attempt_root']);root.mkdir(mode=0o700)
    for a in selected:(root/'scratch'/a['admission_id']).mkdir(parents=True,mode=0o700)
    save_new(root/'物理环境.json',identities)
    if config.get('failed_publication'):
        from data_pipeline.results import Publication
        from data_pipeline.results.manifest_io import Limits
        from data_pipeline.jobs.downstream import require_failed_publication
        pc=config['tail']['publication']
        publication=Publication(pc['dsn'],pc['private_root'],limits=Limits(**pc['limits']))
        state=require_failed_publication(publication,config['publication_physical'],config['failed_publication'])
        save_new(root/'原P只读分类.json',state)
    import resource,shutil,sys
    def guard():
        rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
        require(rss<=config['limits']['max_rss_bytes'],'人工驱动RSS保护')
        require(shutil.disk_usage(root).free>=config['limits']['min_free_bytes'],'人工驱动磁盘保护')
    pairs=qualify(config,selected,root,guard)
    if spec.get('resume_from')=='publication_only':
        from data_pipeline.jobs.downstream import publish_pairs
        return publish_pairs(config,pairs,root,guard,config['resume_context'][1])
    return run_tail(SimpleNamespace(input_profile=config['input']['profile']),config,pairs,root,guard,
                    resume_context=config.get('resume_context'))


def resume_preflight(spec):
    """复用保存的原Country/Feature依赖及参考；不以旧Trend取得新资格。"""
    from copy import deepcopy
    from psycopg2.extensions import parse_dsn
    from data_pipeline.jobs.input_plan import FixedInputs
    from data_pipeline.results.manifest_io import digest
    original=material(spec['source_config'])
    previous=material(spec['previous_spec'])
    saved=[material(p) for p in spec['admissions']]
    only_p=spec.get('resume_from')=='publication_only'
    count=20 if only_p else 19
    require(len(saved)==count and len({a['admission_id'] for a in saved})==count,'续接AD数量或唯一性不符')
    require({a['owner'] for a in saved}==REUSE|READMIT|{'country'}|({'trend'} if only_p else set()),'续接主体不符')
    by_id={a['admission_id']:a for a in saved}
    for a in saved:
        require(a['admission_id']==digest({k:v for k,v in a.items() if k!='admission_id'}),'原AD摘要不符')
        require(set(a['dependencies'])<=set(by_id),'续接依赖缺失')
        require(a['physical']['system_identifier']==spec['system_identifier'],'原AD cluster不符')
    produced=material(spec['context_source'])
    from data_pipeline.analysis.country_trends import stream_schema as schema
    inputs=schema.decode(produced['proof']['inputs_typed'])
    dependencies=tuple(a for a in saved if a['owner'] in ('country','feature'))
    require({a['admission_id']:a for a in inputs['dependencies']}=={a['admission_id']:a for a in dependencies},'保存上下文Country/Feature完整AD不符')
    country=next(a for a in dependencies if a['owner']=='country')
    config=deepcopy(original);config['tail']=deepcopy(previous['tail'])
    tc=config['tail']['trend'];ref=config['tail']['reference']
    require(tuple(tc['window_us'])==inputs['window_us'],'保存上下文窗口不符')
    binding=inputs['reference_binding']
    require(binding==material(spec['reference_receipt']),'保存参考登记与Trend输入不符')
    require(binding['path']==str(Path(ref['derived_root'])/'trend-reference.json'),'原派生参考路径不符')
    from data_pipeline.results.manifest_io import stamp, file_hash
    require(stamp(Path(binding['path']))==binding['stamp'] and file_hash(binding['path'])==binding['sha256'],'原参考实体漂移')
    require(file_hash(ref['source_path'])==ref['source_sha256'],'原比较输入漂移')
    expected={'domeye_8233_obs_v9':16384,'domeye_8233_detection_v9':16385,'domeye_8233_country_v10':17417,'domeye_8233_trend_v10':17418}
    if only_p:expected['domeye_8233_publication_v11']=17729
    require(spec['source_databases']==expected and spec['system_identifier']=='7685154057349199474','原环境声明不符')
    require(binding['system_id']==spec['system_identifier'] and binding['database_oid']==17418,'原参考物理身份不符')
    require(parse_dsn(tc['dsn'])['dbname']=='domeye_8233_trend_v10','Trend必须复用原参考库')
    require(parse_dsn(config['tail']['country']['dsn'])['dbname']=='domeye_8233_country_v10','Country原库不符')
    root=Path(spec['attempt_root']);old=Path(previous['attempt_root'])
    require(root.is_absolute() and root.resolve()==root and not root.exists(),'新续接根已存在或为别名')
    for protected in (old,Path(original['run_root']),Path(original['input']['source_root']),Path(__file__).resolve().parents[3]):
        require(not(root.is_relative_to(protected) or protected.is_relative_to(root)),'续接根交叠原件')
    target=spec['publication'];source=parse_dsn(tc['dsn']);destination=parse_dsn(target['dsn'])
    require(all(source.get(k)==destination.get(k) for k in ('host','port','user')) and destination['dbname']=='domeye_8233_publication_v11','新P数据库越界')
    if only_p:
        require(target['private_root']=='/Users/botongwu/.codex/outputs/domeye-artificial-trend-resume-8233-v11/publication','原v11 P根不符')
        require(not root.is_relative_to(Path(target['private_root']).parent),'P attempt不得覆盖原v11')
        trend=next(a for a in saved if a['owner']=='trend')
        require(codec('trend').untyped(trend['owner_binding'])==produced,'仅P须绑定已接受Trend完整返回')
        require(Path(produced['binding']['root']).is_relative_to(Path(target['private_root']).parent/'products/trend'),'Trend来源根须为原v11')
        tc['runtime']['allowed_roots'].append(str(Path(target['private_root']).parent))
        config.update(require_pristine_publication=True,publication_physical=[spec['system_identifier'],17729])
        if 'failed_publication' in spec:
            failure=spec['failed_publication'];request=material(failure['request'])
            values=request['components']+[d['admission'] for d in request['dependencies']]
            require(len(values)==len(saved) and {a['admission_id']:a for a in values}==by_id,'失败P请求与保存AD图不符')
            require(request['profile']['data_kind']=='artificial','失败P不是人工组合')
            build=failure['build_id']
            require(type(build) is str and len(build)==32 and all(c in '0123456789abcdef' for c in build),'失败build身份无效')
            require(config['tail']['publication']['expected_generation']==0,'失败无Token续接只接受空head')
            require(spec.get('readmit_owners',[]) in ([],['resource']),'本次仅允许Resource正常再准入')
            config.update(require_pristine_publication=False,readmit_owners=spec.get('readmit_owners',[]),
                          failed_publication=dict(build_id=build,input_digest=digest(request)))
    else:require(target['private_root']==str(root/'publication'),'新P根不符')
    config['tail']['publication'].update(target)
    tc['runtime'].update(output_root=str(root/'products/trend'),scratch_root=str(root/'scratch/trend'))
    tc['runtime']['allowed_roots'].append(str(root))
    config.update(reuse_all_admissions=True,expected_publication_requests=72,
        resume_context=(inputs['feature_selections'],dict(source_path=ref['source_path'],source_sha256=ref['source_sha256'],
        derived_root=ref['derived_root'],country_admission_id=country['admission_id'],binding=binding)))
    FixedInputs.load(config['manifest'],config['mapping'],config['input'])
    return config,saved
