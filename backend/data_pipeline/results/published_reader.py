"""组合清单/读取接入既有Publication；不提供外部清单导入或资格补签。"""
from contextlib import contextmanager, closing
from copy import deepcopy
from dataclasses import dataclass
import uuid
import psycopg2
from data_pipeline.results.manifest_io import require, digest
from data_pipeline.results.component_readers import Admissions
from data_pipeline.results.manifest_contract import CONTRACT, PROFILES, validate_structure, _fields
from data_pipeline.results.component_roles import verify_known_roles, codec

SCHEMA_SHA = 'fb6c97594a59d2660eaec7b7c93e9aef7e88378139243577e4decebbc707fd86'
# 真实模式是明确增量，不把旧P0人工schema SHA用作新字段范围的证明。
REAL_SCHEMA = dict(base_schema_sha256=SCHEMA_SHA,extension='publication-combined-real/v1',
                   profiles=[p for p in PROFILES.values() if p['data_kind']=='real'],
                   execution_profile='real-candidate/v1',fixture_only=False,
                   source_rule='explicit-original-m2-binding-no-fixture-origin/v1')

ARTIFICIAL_SCHEMA = dict(base_schema_sha256=SCHEMA_SHA,extension='publication-combined-artificial/v1',
    profiles=[p for p in PROFILES.values() if p['data_kind']=='artificial'],
    input_profile='artificial/v1',execution_profile='real-candidate/v1',fixture_only=False,
    artificial_input_fields=['input','source_root_identity','country_reference'],
    source_rule='original-json-byte-sha-m2-binding-fixture-root/v1')


ARTIFICIAL_DERIVED_SCHEMA=dict(base_schema_sha256=digest(ARTIFICIAL_SCHEMA),
    extension='publication-combined-artificial-derived-reference/v1',
    optional_artificial_input_field='trend_reference',
    source_rule='frozen-source-byte-sha-explicit-derived-root-original-reference-binding/v1')

def schema_sha(profile, *, derived_reference=False):
    if profile['data_kind']=='artificial' and derived_reference:return digest(ARTIFICIAL_DERIVED_SCHEMA)
    if profile['data_kind']=='artificial': return digest(ARTIFICIAL_SCHEMA)
    return digest(REAL_SCHEMA) if profile['data_kind']=='real' else SCHEMA_SHA


def validate_manifest(manifest):
    """身份/边守恒；科学验收由离线prepare完成，不由此函数签署。"""
    _fields(manifest, 'contract schema_sha256 profile build_id components dependencies edges role_graph qualification_summary composition_validation code_sha256'
            + (' artificial_input' if manifest.get('profile',{}).get('data_kind')=='artificial' else ''))
    require(manifest['contract'] == CONTRACT and manifest['schema_sha256'] == schema_sha(manifest['profile'],derived_reference='trend_reference' in manifest.get('artificial_input',{})), '组合清单合同或schema错误')
    require(type(manifest['build_id']) is str and bool(manifest['build_id']), '组合build缺失')
    admissions = manifest['components'] + [d['admission'] for d in manifest['dependencies']]
    # request固定提交不另写入manifest；原完整Admission保留owner_revision。
    revisions = {owner: None for owner in ('canonical','resource','feature','detection','country','trend')}
    for a in admissions:
        if a['owner'] in revisions: revisions[a['owner']] = a['owner_revision']
    validate_structure({k:deepcopy(manifest[k]) for k in ('contract','profile','components','dependencies','edges','role_graph')}
                       | {'dependency_revisions':revisions}
                       | ({'artificial_input':deepcopy(manifest['artificial_input'])} if manifest['profile']['data_kind']=='artificial' else {}))
    summary = manifest['qualification_summary']
    require(type(summary) is list and [s['component_key'] for s in summary] == [a['owner'] for a in manifest['components']], '组合资格摘要主体不完整')
    for item in summary:
        _fields(item, 'component_key storage_state coverage_typed codec_version')
        require(item['storage_state'] == 'complete' and all(type(item[k]) is str and item[k] for k in ('coverage_typed','codec_version')), '组合存储/覆盖摘要错误')
    validation = manifest['composition_validation']
    _fields(validation, 'validator_version edges_checked typed_digest')
    require(validation['validator_version'] == CONTRACT and type(validation['edges_checked']) is int
            and validation['edges_checked'] == len(manifest['edges']), '组合边验收计数错误')
    for value in (manifest['code_sha256'],validation['typed_digest']):
        require(type(value) is str and len(value) == 64 and all(c in '0123456789abcdef' for c in value), '组合摘要格式错误')
    return admissions


def graph(p, manifest):
    admissions = validate_manifest(manifest)
    from data_pipeline.results.build_manifest import evidence
    evidence(p,manifest)
    require(p.combined is not None, '组合须显式绑定本任务各Admission Runtime')
    validate_mode(manifest['profile'], admissions, p.combined,
                  artificial_input=getattr(p,'combined_input',None),declared=manifest.get('artificial_input'))
    require(not verify_known_roles(manifest), '组合存在未接合角色')
    return Admissions(admissions, p.combined, guard=p.guard)


def validate_mode(profile, admissions, runtimes, *, artificial_input=None, declared=None):
    """真实候选Runtime不等于真实数据；fixture来源不能改名进入真实profile。"""
    require(profile in PROFILES.values(), '未知组合数据种类')
    real = profile['data_kind'] == 'real'
    artificial = profile['data_kind'] == 'artificial'
    candidate_runtime = real or artificial
    if artificial:
        from data_pipeline.results.fixture_metadata import ArtificialInput
        require(type(artificial_input) is ArtificialInput, '人工模式缺原输入合同')
        require(artificial_input.verify(admissions)==declared, '人工输入身份不符')
    else:
        require(artificial_input is None and declared is None, '非人工profile不得携带人工合同')
    require(set(runtimes) == {a['admission_id'] for a in admissions}, '执行模式缺实际Runtime')
    for admission in admissions:
        rt = runtimes[admission['admission_id']]
        require(type(getattr(rt,'fixture_only',None)) is bool and rt.fixture_only is not candidate_runtime, 'profile数据种类与Runtime人工模式不符')
        require(getattr(rt,'execution_profile',None) == ('real-candidate/v1' if candidate_runtime else None), '组合Runtime执行模式不符')
        if admission['owner'] == 'm2':
            b = codec('m2').untyped(admission['owner_binding'])
            origins = [entry['origin_uri'] for entry in b['plan']['manifest']['inputs']]
            require(origins and all(type(uri) is str and bool(uri) for uri in origins), '数据来源身份缺失')
            if real:
                require(not any(uri.startswith('fixture://') for uri in origins), '人工来源不得提升为真实数据')
            if candidate_runtime:
                require(rt.expected_m2_binding == b, '候选Runtime未绑定原完整M2')


@contextmanager
def candidate(p, request):
    """实际全图current后建立旧build候选；失败保留制品并登记failed。

    仅供后续有限prepare适配调用；正常离开未ready也失败关闭。
    不接受外部validator、外部成功Receipt或自报ready。
    """
    request = validate_structure(request)
    require(p.combined is not None, '缺组合Runtime')
    values = request['components'] + [d['admission'] for d in request['dependencies']]
    validate_mode(request['profile'],values,p.combined,
                  artificial_input=getattr(p,'combined_input',None),declared=request.get('artificial_input'))
    require(not verify_known_roles(request), '组合存在未接合角色')
    fixed = Admissions(values,p.combined,guard=p.guard)
    fixed.current()  # 所有原owner实际资格在任何候选写入之前核验。
    build = uuid.uuid4().hex; output = p.root/'q1-builds'/build
    from data_pipeline.results.build_manifest import WriteScope
    scope=WriteScope(p,request,output)
    with closing(psycopg2.connect(p.dsn)) as pg, pg, pg.cursor() as c:
        p._owner(c)
        c.execute('INSERT INTO publication_q1.builds(build_id,state,input_digest) VALUES (%s,%s,%s)',
                  (build,'candidate',digest(request)))
    primary = None
    try:
        scope.check()
        output.mkdir(parents=True,exist_ok=False)
        scope.bind_created()
        yield build,output,fixed,scope
        with closing(psycopg2.connect(p.dsn)) as pg, pg.cursor() as c:
            p._owner(c)
            c.execute('SELECT state FROM publication_q1.builds WHERE build_id=%s',(build,))
            require(c.fetchone() == ('ready',), '组合未完成全部流/领域验收，拒绝成功退出')
    except BaseException as error:
        primary = error
        raise
    finally:
        if primary is not None:
            try:
                with closing(psycopg2.connect(p.dsn)) as pg, pg, pg.cursor() as c:
                    p._owner(c)
                    c.execute("UPDATE publication_q1.builds SET state='failed' WHERE build_id=%s AND state IN ('candidate','ready')",(build,))
            except BaseException as cleanup_error:
                primary.cleanup_errors = (*getattr(primary,'cleanup_errors',()),cleanup_error)


@dataclass
class PublishedSession:
    iterator: object = None
    receipt: object = None
    def __iter__(self): return self
    def __next__(self): return next(self.iterator)


def _published(p, token):
    with closing(psycopg2.connect(p.dsn)) as pg:
        pg.set_session(readonly=True)
        with pg.cursor() as c:
            p._owner(c)
            manifest = p._manifest(c,token,{'published'})
    require(manifest['contract'] == CONTRACT, '令牌不是组合发布')
    return manifest


@contextmanager
def open_component(p, token, owner, request, *, max_rows, max_bytes):
    """固定同P主体读取；旧published可读，不要求仍为head。"""
    manifest = _published(p,token)
    fixed = graph(p,manifest)
    selected = [a for a in manifest['components'] if a['owner'] == owner]
    require(len(selected) == 1, '该P没有请求主体，不允许改用原始依赖')
    session = PublishedSession()
    with fixed.read(selected[0]['admission_id'],request,max_rows=max_rows,max_bytes=max_bytes) as source:
        session.iterator = source
        yield session
    if source.receipt is not None:
        # 新鲜控制查询在数据资源退出和所有owner尾current之后。
        require(_published(p,token) == manifest, '读取期间发布清单改变')
        session.receipt = deepcopy(source.receipt)
