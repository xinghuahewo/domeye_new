"""窄人工 Publication-only 重试：材料预检、目录准备、原发布链三步分离。"""
from copy import deepcopy
from dataclasses import asdict
import json
import hashlib
from pathlib import Path
from contextlib import closing
import psycopg2
from data_pipeline.results.manifest_io import require, file_hash, digest
from data_pipeline.results.manifest_contract import validate_structure
from data_pipeline.results.component_roles import verify_known_roles
from data_pipeline.results.component_streams import requests
from data_pipeline.jobs.stage_runner import save_new, save_final
from data_pipeline.results.retry.observation import stage, ViewProgress, POLICY


def material(spec):
    path=Path(spec['path'])
    require(path.is_absolute() and path.resolve()==path,'材料路径必须为原绝对路径')
    require(file_hash(path)==spec['sha256'],'材料 SHA 不符')
    with path.open('rb') as stream:raw=stream.read(16*1024**2+1)
    require(len(raw)<=16*1024**2,'控制材料超过现有表示合同')
    require(hashlib.sha256(raw).hexdigest()==spec['sha256'],'材料读取期间 SHA 改变')
    return json.loads(raw)


def preflight(spec, *, prepared=False):
    """纯材料预检：不创建目录、不构造 Runtime、不连接数据库。"""
    request=validate_structure(material(spec['request']))
    config=material(spec['config'])
    require(spec['contract']=='publication-retry/v1','未知重试合同')
    require(spec.get('observation',POLICY)==POLICY,'重试观测合同不符')
    require(request['profile']['data_kind']=='artificial','该入口仅接原人工组合')
    require(not verify_known_roles(request),'原角色图未闭合')
    values=request['components']+[d['admission'] for d in request['dependencies']]
    saved=[material(p) for p in spec['admissions']]
    by_id={a['admission_id']:a for a in saved}
    require(len(by_id)==len(saved)==len(values) and all(by_id.get(a['admission_id'])==a for a in values),'原 AD 图缺项或绑定不符')
    for a in values:
        require(a['admission_id']==digest({k:v for k,v in a.items() if k!='admission_id'}),'原 AD 摘要不符')
        require(a['physical']==spec['physical'][a['admission_id']],'原物理绑定不符')
    attempt=Path(spec['attempt_root'])
    require(attempt.is_absolute() and attempt.resolve()==attempt,'新 attempt 根为别名')
    if prepared:
        require(attempt.is_dir(),'缺本次准备目录')
        saved=json.loads((attempt/'材料预检.json').read_text())
        stat=attempt.stat()
        require(saved['spec_digest']==digest(spec) and saved['directory']==[stat.st_dev,stat.st_ino],'准备目录或配置漂移')
    else:
        require(not attempt.exists(),'新 attempt 根已存在')
    pc=config['tail']['publication']
    require(spec['publication_root']==pc['private_root'],'原 Publication 控制根不符')
    physical=spec['publication_physical']
    require(type(physical) is list and len(physical)==2 and type(physical[0]) is str
            and physical[0].isdigit() and type(physical[1]) is int and physical[1]>0,'Publication物理身份格式错误')
    protected={Path(config['run_root']),Path(config['input']['source_root']),Path(pc['private_root'])}
    protected.update(Path(a['physical']['root']) for a in values)
    protected.update(Path(e['path']).parent for a in values for e in a['entities'])
    protected.update(Path(p['path']).parent for p in [spec['request'],spec['config'],*spec['admissions']])
    require(type(spec.get('migrate_profiles',False)) is bool,'迁移开关必须显式布尔值')
    saved_token=None
    if spec['action']=='publish_saved':
        from data_pipeline.results import Token
        saved_token=material(spec['prepared_token'])
        require(type(saved_token) is dict and set(saved_token)=={'publication_id','build_id','profile_digest'}
                and all(type(v) is str and v for v in saved_token.values()),'保存Token字段无效')
        require(saved_token['profile_digest']==digest(request['profile']),'保存Token与原profile不符')
        Token(**saved_token)
        protected.add(Path(spec['prepared_token']['path']).parent)
    else:require('prepared_token' not in spec,'非续发动作不得携带保存Token')
    for path in protected:
        require(path.is_absolute() and path.resolve()==path and path.is_dir(),'原保护目录无效')
        require(not (attempt.is_relative_to(path) or path.is_relative_to(attempt)),'attempt 与原件/控制根交叠')
    require(type(spec['expected_generation']) is int and spec['expected_generation']>=0,'generation 无效')
    require(spec['action'] in ('prepare','publish','publish_saved'),'未知实际执行范围')
    require(spec['selector']=='artificial:m3:combined-country-trend','selector 越界')
    # 代码原件清单由固定基线导出，未修改的 owner/锁依赖必须仍相同。
    root=Path(__file__).resolve().parents[4]
    codes=material(spec['owner_code'])
    scopes=('bgp','analysis/resources','analysis/detection','analysis/country_events','analysis/country_trends')
    required={str(p.relative_to(root)) for name in scopes for p in (root/'backend/data_pipeline'/name).rglob('*.py')}
    required.update(str(p.relative_to(root)) for p in (root/'backend/data_pipeline/analysis/features').glob('*.py'))
    required.update(('backend/pyproject.toml','backend/uv.lock','frontend/package-lock.json'))
    require(set(codes)==required,'owner 代码清单不完整')
    require(all(file_hash(root/path)==sha for path,sha in codes.items()),'owner/锁代码漂移，不能复用原 AD')
    count=sum(len(requests(a['owner'],batch_rows=1,batch_bytes=1,admission=a)) for a in request['components'])
    return dict(spec=deepcopy(spec),config=config,request=request,admissions=values,saved_token=saved_token,
                summary=dict(state='materials_checked',admissions=len(values),requests=count,live_current='Unknown'))


def prepare_directories(checked):
    """明确的新 attempt 写步骤；不创建或变更原 Publication 控制根。"""
    with stage(None,'directory_preflight','完整材料预检通过的AD数量') as validation:
        checked=preflight(checked['spec'])
        validation['processed_count']=len(checked['admissions'])
    root=Path(checked['spec']['attempt_root']);root.mkdir(mode=0o700)
    save_new(root/'目录前材料观测.json',validation)
    with stage(root,'directories','新建的隔离scratch目录数') as metrics:
        metrics['scope']='独占attempt根建立后；不包括根mkdir及前置纯预检'
        metrics['processed_count']=0
        (root/'scratch').mkdir(mode=0o700)
        for a in checked['admissions']:
            (root/'scratch'/a['admission_id']).mkdir(mode=0o700);metrics['processed_count']+=1
        stat=root.stat()
        save_new(root/'材料预检.json',dict(checked['summary'],spec_digest=digest(checked['spec']),directory=[stat.st_dev,stat.st_ino]))
    return checked


def verify_control(publication, expected):
    """先只读验证原控制库及 private_root，不初始化替代控制空间。"""
    with closing(psycopg2.connect(publication.dsn)) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as cur:
            cur.execute('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()')
            require(cur.fetchone()==tuple(expected),'原 Publication 物理库不符')
            publication._owner(cur)


def finish_publication(publication,token,spec,root,status):
    """发布返回即记确认；回执保存及discover失败不得退回未发布。"""
    with stage(root,'publish','原publish成功返回次数',publication.guard) as metrics:
        metrics.update(processed_count=0,publication_state='outcome_unknown')
        status.update(publication_state='outcome_unknown',generation=None)
        published=publication.publish(token,expected_generation=spec['expected_generation'],selector=spec['selector'])
        status.update(state='published',last_completed_state='published',publication_state='confirmed',published_token=asdict(published))
        metrics.update(processed_count=1,publication_state='confirmed',published_token=asdict(published))
        save_new(Path(root)/'原publish返回.json',asdict(published))
    confirm_discovery(publication,published,spec,root,status)


def confirm_discovery(publication,published,spec,root,status):
    with stage(root,'discover','原discover返回并与本次发布匹配次数',publication.guard) as metrics:
        metrics['processed_count']=0
        current,generation=publication.discover(spec['selector'])
        require(current==published,'原 discover 与 publish 不符')
        status.update(state='discovered',last_completed_state='discovered',generation=generation)
        metrics['processed_count']=1
        save_new(Path(root)/'原discover返回.json',dict(token=asdict(current),generation=generation))


def execute(checked):
    """目录准备后的显式 live 步骤；只使用原 Publication 实现，不调用准入/生产。"""
    from data_pipeline.results.retry.runtime import construct
    from data_pipeline.results import Publication
    from data_pipeline.results.manifest_io import Limits
    from data_pipeline.results.fixture_metadata import ArtificialInput
    from data_pipeline.results.published_reader import validate_mode
    with stage(None,'material_revalidation','完整材料预检通过的AD数量') as validation:
        checked=preflight(checked['spec'],prepared=True)
        validation['processed_count']=len(checked['admissions'])
    spec=checked['spec'];require(spec.get('enabled') is True,'重试未显式启用')
    root=Path(spec['attempt_root']);require(root.is_dir(),'先完成新目录准备')
    save_new(root/'执行前材料观测.json',validation)
    # 目录准备后再次核原材料，允许的唯一差异是本次新建目录。
    require(material(spec['request'])==checked['request'] and material(spec['config'])==checked['config'],'准备后原材料改变')
    save_new(root/'仅一次执行.json',dict(state='started',request_digest=digest(checked['request'])))
    config=checked['config'];request=checked['request'];pc=config['tail']['publication']
    status=dict(state='failed',last_completed_state=None,science_recomputed=False,
                publication_state='not_attempted',generation=None);primary=None
    try:
        with stage(root,'runtime','构造完成的原AD Runtime数量') as metrics:
            metrics['processed_count']=0
            runtimes=construct(config,checked['admissions'],root,progress=lambda n:metrics.update(processed_count=n))
        with stage(root,'input_binding','原人工输入合同核验通过次数') as metrics:
            metrics['processed_count']=0
            derived=request.get('artificial_input',{}).get('trend_reference')
            if derived is not None:
                derived={k:derived[k] for k in ('source_path','source_sha256','derived_root','country_admission_id','binding')}
            artificial=ArtificialInput(manifest=config['manifest'],mapping=config['mapping'],input=config['input'],
                country_reference=config['country_reference'],trend_reference=derived)
            validate_mode(request['profile'],checked['admissions'],runtimes,artificial_input=artificial,declared=request['artificial_input'])
            metrics['processed_count']=1
        publication=Publication(pc['dsn'],spec['publication_root'],limits=Limits(**pc['limits']),combined=runtimes,combined_input=artificial)
        # 原控制库身份先只读核实。绝不 initialize 一个替代库或改原 root。
        with stage(root,'control','原控制库及根身份核验通过次数',publication.guard) as metrics:
            metrics['processed_count']=0
            verify_control(publication,spec['publication_physical']);metrics['processed_count']=1
        perform_action(publication,checked,pc,root,status)
        return status
    except BaseException as error:
        primary=error;status.update(state='failed',failed_stage=getattr(error,'retry_stage',None),
                                   error_type=type(error).__name__,error=str(error))
        error.retry_status=dict(status);raise
    finally:save_final(root/'重试结果.json',status,primary)


def verify_profile(publication,selector):
    """只读检查固定profile可写入head；不在检查中执行DDL。"""
    from data_pipeline.results.profiles import SELECTORS
    require(selector in SELECTORS,'未知固定selector')
    with closing(psycopg2.connect(publication.dsn)) as pg:
        pg.set_session(readonly=True,isolation_level='REPEATABLE READ')
        with pg.cursor() as c:
            publication._owner(c)
            c.execute("SELECT to_regclass('publication_q1.schema_version'),to_regclass('publication_q1.profiles')")
            require(all(c.fetchone()),'Publication profile未迁移；须显式迁移后再prepare')
            c.execute('SELECT version FROM publication_q1.schema_version')
            require(c.fetchall()==[(3,)],'Publication profile schema版本不符')
            c.execute('SELECT profile FROM publication_q1.profiles WHERE selector=%s',(selector,))
            require(c.fetchone()==(SELECTORS[selector],),'Publication所需固定profile未登记')
            c.execute("SELECT conname FROM pg_constraint WHERE conrelid='publication_q1.head'::regclass AND conname IN ('head_selector_check','head_profile_fk')")
            require(c.fetchall()==[('head_profile_fk',)],'Publication head仍为旧约束或缺固定profile外键')


def classify_saved(publication,token,request,spec):
    """只读分类既有build/head，未知或冲突绝不自动写入。"""
    with closing(psycopg2.connect(publication.dsn)) as pg:
        pg.set_session(readonly=True,isolation_level='REPEATABLE READ')
        with pg.cursor() as c:
            publication._owner(c)
            manifest=publication._manifest(c,token,{'ready','published'})
            fields=('contract','profile','components','dependencies','edges','role_graph','artificial_input')
            require(all(manifest.get(k)==request.get(k) for k in fields),'保存prepare不属于本次原请求/AD/人工输入')
            c.execute('SELECT state FROM publication_q1.builds WHERE build_id=%s',(token.build_id,))
            state=c.fetchone()[0]
            c.execute('SELECT publication_id,build_id,generation FROM publication_q1.head WHERE selector=%s',(spec['selector'],))
            head=c.fetchone()
            if state=='ready':
                require(head is None and spec['expected_generation']==0,'保存prepare的head/generation冲突')
                return dict(state='ready',generation=0)
            require(state=='published' and head is not None and head[:2]==(token.publication_id,token.build_id)
                    and type(head[2]) is int and head[2]>0,'保存prepare发布状态未知或head冲突')
            return dict(state='published',generation=head[2])


def perform_action(publication,checked,pc,root,status):
    """续发只使用保存Token；迁移是独立显式步骤，不重做prepare。"""
    from data_pipeline.results import Token
    spec=checked['spec'];request=checked['request'];saved=spec['action']=='publish_saved'
    if saved:
        token=Token(**checked['saved_token'])
        status.update(token=asdict(token),publication_state='outcome_unknown',prepare_reused=True)
        with stage(root,'saved_state','原build/head只读分类通过次数',publication.guard) as metrics:
            classification=classify_saved(publication,token,request,spec)
            metrics.update(processed_count=1,classification=classification)
            if classification['state']=='published':
                status.update(last_completed_state='published',publication_state='confirmed',published_token=asdict(token),generation=classification['generation'])
            else:status.update(last_completed_state='prepared',publication_state='not_attempted',generation=0)
            save_new(Path(root)/'原build与head分类.json',classification)
    if spec.get('migrate_profiles',False) and not (saved and classification['state']=='published'):
        with stage(root,'profile_migration','原显式profile迁移成功次数',publication.guard) as metrics:
            metrics['processed_count']=0
            publication.migrate_profiles();metrics['processed_count']=1
    with stage(root,'profile_check','所需profile与schema只读校验次数',publication.guard) as metrics:
        metrics['processed_count']=0
        verify_profile(publication,spec['selector']);metrics['processed_count']=1
    if saved:
        if classification['state']=='published':confirm_discovery(publication,token,spec,root,status)
        else:finish_publication(publication,token,spec,root,status)
        return
    with stage(root,'prepare','原owner已交付并通过批封装检查的行；未必完成消费',publication.guard) as metrics:
        metrics.update(processed_count=0,encoded_input_bytes=0,completed_views=0)
        observation=ViewProgress(root,metrics)
        token=publication.prepare_combined(request,max_rows=pc['max_rows'],max_bytes=pc['max_bytes'],progress=observation)
        status.update(state='prepared',last_completed_state='prepared',token=asdict(token))
        save_new(root/'原prepare返回.json',asdict(token))
    if spec['action']=='publish':
        finish_publication(publication,token,spec,root,status)


def inspect_saved(checked):
    """显式只读在线分类：不创建attempt、Runtime或迁移schema。"""
    from data_pipeline.results import Publication, Token
    from data_pipeline.results.manifest_io import Limits
    spec=checked['spec'];require(spec['action']=='publish_saved','只读分类仅适用于保存Token')
    pc=checked['config']['tail']['publication']
    publication=Publication(pc['dsn'],spec['publication_root'],limits=Limits(**pc['limits']))
    verify_control(publication,spec['publication_physical'])
    return classify_saved(publication,Token(**checked['saved_token']),checked['request'],spec)
