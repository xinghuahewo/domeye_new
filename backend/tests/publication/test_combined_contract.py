"""未齐依赖的请求必须在结构层拒绝；不构造可准入的假Admission。"""
from copy import deepcopy
import pytest
from data_pipeline.results.manifest_contract import CONTRACT, PROFILES, validate_structure


def incomplete(profile_id):
    return ({'contract': CONTRACT, 'profile': deepcopy(PROFILES[profile_id]),
            'dependency_revisions': {}, 'components': [], 'dependencies': [],
            'edges': [], 'role_graph': []}
            | ({'artificial_input':{}} if PROFILES[profile_id]['data_kind']=='artificial' else {}))


@pytest.mark.parametrize('profile_id', list(PROFILES))
def test_missing_actual_components_cannot_pass(profile_id):
    with pytest.raises(ValueError, match='主体缺失'):
        validate_structure(incomplete(profile_id))


def test_required_cannot_be_reduced_for_available_modules():
    q = incomplete('fixture-m3-combined-base/v1')
    q['profile']['required'] = q['profile']['required'][:3]
    with pytest.raises(ValueError, match='固定required'):
        validate_structure(q)


def test_unknown_request_fields_rejected():
    q = incomplete('fixture-m3-combined-base/v1'); q['allow_missing_country'] = True
    with pytest.raises(ValueError, match='字段'):
        validate_structure(q)
