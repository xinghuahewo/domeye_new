"""组合合同的结构检查。领域同源/窗口验收尚由正式owner接合完成。

本模块不登记profile、不生成ready，也不把结构摘要当科学资格。
"""
from copy import deepcopy
import re
from data_pipeline.results.manifest_io import require
from data_pipeline.bgp.archive.value_codec import digest

CONTRACT = 'publication-combined-contract/v1'
BASE_REQUIRED = [
    'resource.values-coverage/v1', 'feature.windows-coverage/v1',
    'detection.six-kinds-details-coverage/v1', 'country.events-qualified/v1',
    'country.result_coverage/v1',
]
PROFILES = {
    'fixture-m3-combined-base/v1': {
        'id': 'fixture-m3-combined-base/v1', 'data_kind': 'fixture',
        'required': BASE_REQUIRED,
    },
    'fixture-m3-combined-country-trend/v1': {
        'id': 'fixture-m3-combined-country-trend/v1', 'data_kind': 'fixture',
        'required': BASE_REQUIRED + ['country.trend-qualified/v1'],
    },
}
for _suffix in ('base','country-trend'):
    _profile = deepcopy(PROFILES['fixture-m3-combined-'+_suffix+'/v1'])
    _profile.update(id='real-m3-combined-'+_suffix+'/v1',data_kind='real')
    PROFILES[_profile['id']] = _profile

for _suffix in ('base','country-trend'):
    _profile = deepcopy(PROFILES['fixture-m3-combined-'+_suffix+'/v1'])
    _profile.update(id='artificial-m3-combined-'+_suffix+'/v1',data_kind='artificial')
    PROFILES[_profile['id']] = _profile


def _fields(value, keys):
    require(type(value) is dict and set(value) == set(keys.split()), '组合合同字段不符')


def validate_structure(request):
    """返回隔离副本；只证明结构闭合，不证明真实Admission、来源或结果能力。"""
    q = deepcopy(request)
    _fields(q, 'contract profile dependency_revisions components dependencies edges role_graph'
            + (' artificial_input' if q.get('profile',{}).get('data_kind')=='artificial' else ''))
    require(q['contract'] == CONTRACT, '未知组合合同')
    profile = q['profile']
    require(type(profile) is dict and profile == PROFILES.get(profile.get('id')), '固定required/profile不符')
    owners = {'resource', 'feature', 'detection', 'country'}
    if profile['id'].endswith('country-trend/v1'): owners.add('trend')
    require(all(type(q[k]) is list for k in ('components', 'dependencies', 'edges', 'role_graph')), '组合列表类型错误')
    components = q['components']
    require([a['owner'] for a in components] == sorted(owners), '组合主体缺失、重复或顺序错误')
    revisions = q['dependency_revisions']
    _fields(revisions, 'canonical resource feature detection country trend')
    required_revisions = owners | {'canonical'}
    for owner, revision in revisions.items():
        require((revision is None and owner not in required_revisions)
                or (type(revision) is str and re.fullmatch(r'[0-9a-f]{40}', revision)), '必需owner固定提交缺失')
    nodes = {a['owner']: a for a in components}
    dependency_keys = []
    for dep in q['dependencies']:
        _fields(dep, 'dependency_key owner admission')
        a = dep['admission']; key = dep['dependency_key']
        require(dep['owner'] in ('m2', 'reference', 'canonical') and a['owner'] == dep['owner'], '原始依赖owner不符')
        require(key == dep['owner'] + ':' + a['admission_id'] and key not in nodes, '依赖键错误或重复')
        nodes[key] = a; dependency_keys.append(key)
    require(dependency_keys == sorted(dependency_keys), '依赖顺序错误')
    ids = [a['admission_id'] for a in nodes.values()]
    for a in nodes.values():
        if a['owner'] in revisions:
            require(a['owner_revision'] == revisions[a['owner']], '固定依赖提交与实际Admission owner_revision不符')
    require(len(set(ids)) == len(ids), '重复Admission身份')
    roles = {}
    for role in q['role_graph']:
        _fields(role, 'consumer_key dependency_key role collector input_binding_typed codec_version selected_sources window_role window_typed reference_role')
        edge_key = (role['consumer_key'], role['dependency_key'], role['role'])
        require(edge_key not in roles, '重复角色边')
        require(role['consumer_key'] in nodes and role['dependency_key'] in nodes, '角色引用不存在')
        require(all(type(role[k]) is str and role[k] for k in ('role', 'collector', 'input_binding_typed', 'codec_version', 'window_typed')), '角色文本缺失')
        require(role['window_role'] in ('initial', 'warmup', 'comparison', 'result', 'reference'), '未知窗口角色')
        sources = role['selected_sources']
        require(type(sources) is list, '来源选择类型错误')
        is_reference = nodes[role['dependency_key']]['owner'] == 'reference'
        if is_reference:
            require(sources == [] and role['window_role'] == 'reference'
                    and type(role['reference_role']) is str and bool(role['reference_role']), '参考CP不得伪装MRT来源')
        else:
            require(role['reference_role'] is None, '非参考边不得声明参考角色')
        seen = set()
        for source in sources:
            _fields(source, 'source_id upstream_rank calculation_role')
            require(type(source['source_id']) is str and source['source_id'] and source['source_id'] not in seen, '重复或空来源')
            require(type(source['upstream_rank']) is int and source['upstream_rank'] >= 0, '原rank类型错误')
            require(type(source['calculation_role']) is str and source['calculation_role'], '计算角色缺失')
            seen.add(source['source_id'])
        roles[edge_key] = role
    edges = set(); dependencies = {key: set() for key in nodes}
    for edge in q['edges']:
        _fields(edge, 'consumer_key dependency_key role expected_admission_id selection_digest')
        key = (edge['consumer_key'], edge['dependency_key'], edge['role'])
        require(key in roles and key not in edges, '边没有唯一完整角色')
        require(edge['expected_admission_id'] == nodes[edge['dependency_key']]['admission_id'], '边Admission错配')
        require(edge['selection_digest'] == digest(roles[key]), '角色选择摘要错配')
        dependencies[edge['consumer_key']].add(edge['dependency_key']); edges.add(key)
    require(edges == set(roles), '角色没有对应边')
    for key, a in nodes.items():
        expected = sorted(nodes[d]['admission_id'] for d in dependencies[key])
        require(a['dependencies'] == expected, '边与原Admission依赖不一致')
    visiting = set(); done = set()
    def visit(key):
        require(key not in visiting, '组合依赖环')
        if key in done: return
        visiting.add(key)
        for dependency in dependencies[key]: visit(dependency)
        visiting.remove(key); done.add(key)
    for key in nodes: visit(key)
    return q
