import json
import re
from pathlib import Path


def _openapi_path(flask_path):
    without_prefix = (
        flask_path.removeprefix('/api/v1')
        if flask_path.startswith('/api/v1/')
        else flask_path
    )
    return re.sub(r'<(?:[^:>]+:)?([^>]+)>', r'{\1}', without_prefix)


def test_openapi_paths_match_runtime_routes(app):
    project_root = Path(__file__).resolve().parents[3]
    contract = json.loads(
        (project_root / 'contracts' / 'openapi.json').read_text(encoding='utf-8')
    )
    runtime_paths = {
        _openapi_path(str(rule))
        for rule in app.url_map.iter_rules()
        if str(rule).startswith(('/api/v1/', '/api/v2/'))
    }

    assert set(contract['paths']) == runtime_paths


def test_openapi_country_outage_general_read_model_is_bounded_and_versioned():
    project_root = Path(__file__).resolve().parents[3]
    contract = json.loads(
        (project_root / 'contracts' / 'openapi.json').read_text(encoding='utf-8')
    )
    schemas = contract['components']['schemas']
    paths = contract['paths']

    expected_variants = {
        '/api/v2/events/resolve': 'CountryOutageGeneralResolutionV1',
        '/api/v2/country-outages/{incident_id}/overview':
            'CountryOutageGeneralOverviewV1',
        '/api/v2/country-outages/{incident_id}/series':
            'CountryOutageGeneralSeriesV1',
        '/api/v2/country-outages/{incident_id}/asns':
            'CountryOutageGeneralAffectedAsPageV1',
        '/api/v2/country-outages/{incident_id}/audit':
            'CountryOutageGeneralAuditV1',
    }
    for path, schema_name in expected_variants.items():
        response_schema = paths[path]['get']['responses']['200'][
            'content'
        ]['application/json']['schema']
        assert {'$ref': f'#/components/schemas/{schema_name}'} in response_schema[
            'oneOf'
        ]
        assert schemas[schema_name]['additionalProperties'] is False

    downstream_path = paths[
        '/api/v2/country-outages/{incident_id}/path-downstreams'
    ]['get']
    assert set(downstream_path['responses']) == {'200', '400', '404', '503'}
    parameters = {item['name']: item for item in downstream_path['parameters']}
    assert parameters['page_size']['schema']['maximum'] == 60
    assert parameters['scope']['schema']['enum'] == ['all', 'concurrent']
    page_schema = schemas['CountryOutageGeneralPathDownstreamPageV1']
    assert page_schema['properties']['items']['maxItems'] == 60
    path_item = schemas['CountryOutageGeneralPathDownstreamItemV1']
    assert path_item['properties']['path_samples']['maxItems'] == 3
    assert path_item['properties']['relationship_semantics']['const'].endswith(
        'not_dependency_or_cause'
    )
    assert schemas['CountryOutageGeneralCapabilitiesV1']['properties'][
        'full_path_evidence'
    ] == {'const': 'audit_only'}


def test_openapi_legacy_country_outage_agent_paths_are_retired():
    project_root = Path(__file__).resolve().parents[3]
    contract = json.loads(
        (project_root / 'contracts' / 'openapi.json').read_text(encoding='utf-8')
    )
    paths = contract['paths']
    retired_fragments = (
        '/api/v2/country-outage/reports',
        '/api/v2/country-outage/runs/',
        '/api/v2/country-outage/capabilities/external-evidence',
        '/api/v2/country-outage/investigations',
    )
    assert not any(
        path.startswith(retired_fragments)
        for path in paths
    )
    assert not any(path.startswith('/api/v2/country-outage/') for path in paths)

def test_openapi_event_count_matches_existing_http_contract():
    project_root = Path(__file__).resolve().parents[3]
    contract = json.loads(
        (project_root / 'contracts' / 'openapi.json').read_text(encoding='utf-8')
    )
    assert (
        contract['components']['schemas']['EventPage']['properties']['record_count']
        == {'type': 'string'}
    )


def test_openapi_feature_list_pages_match_nested_runtime_contracts():
    project_root = Path(__file__).resolve().parents[3]
    contract = json.loads(
        (project_root / 'contracts' / 'openapi.json').read_text(encoding='utf-8')
    )
    countries = contract['paths']['/features/countries']['get']['responses']['200'][
        'content'
    ]['application/json']['schema']
    ases = contract['paths']['/features/ases']['get']['responses']['200']['content'][
        'application/json'
    ]['schema']
    assert countries == {'$ref': '#/components/schemas/CountryFeaturePage'}
    assert ases == {'$ref': '#/components/schemas/AsFeaturePage'}

    schemas = contract['components']['schemas']
    assert schemas['CountryFeaturePage']['properties']['data']['items'] == {
        '$ref': '#/components/schemas/CountryFeatureItem',
    }
    assert schemas['AsFeaturePage']['properties']['data']['items'] == {
        '$ref': '#/components/schemas/AsFeatureItem',
    }
    assert schemas['CountryFeatureItem']['required'] == ['country', 'time_series_data']
    assert schemas['AsFeatureItem']['required'] == [
        'asn', 'as_name', 'country', 'org_name', 'time_series_data',
    ]


def test_openapi_requires_legacy_event_semantic_guardrails():
    project_root = Path(__file__).resolve().parents[3]
    contract = json.loads(
        (project_root / 'contracts' / 'openapi.json').read_text(encoding='utf-8')
    )
    schemas = contract['components']['schemas']
    guardrail_ref = {
        '$ref': '#/components/schemas/LegacyEventSemanticGuardrails',
    }

    assert 'semantic_guardrails' in schemas['EventItem']['required']
    assert schemas['EventItem']['properties']['semantic_guardrails'] == guardrail_ref
    assert 'semantic_guardrails' in schemas['EventDetail']['required']
    assert schemas['EventDetail']['properties']['semantic_guardrails'] == guardrail_ref
    assert 'semantic_guardrails' in schemas['EvidenceBundle']['required']
    assert schemas['EvidenceBundle']['properties']['semantic_guardrails'] == guardrail_ref


def test_openapi_w5_public_contract_is_retired():
    project_root = Path(__file__).resolve().parents[3]
    contract = json.loads(
        (project_root / 'contracts' / 'openapi.json').read_text(encoding='utf-8')
    )
    assert not any(
        path.startswith('/api/v2/country-outage/investigations')
        for path in contract['paths']
    )


def test_openapi_only_exposes_read_only_data_operations():
    project_root = Path(__file__).resolve().parents[3]
    contract = json.loads(
        (project_root / "contracts" / "openapi.json").read_text(encoding="utf-8")
    )
    for path, item in contract["paths"].items():
        assert set(item) <= {"get", "servers"}, path
        assert "get" in item, path
    assert not any(
        name.startswith("CountryOutageInteractive")
        for name in contract["components"]["schemas"]
    )


def test_openapi_retirement_leaves_no_dangling_references():
    project_root = Path(__file__).resolve().parents[3]
    contract = json.loads((project_root / 'contracts/openapi.json').read_text())
    assert not any(path.startswith(('/p0/', '/dashboard/')) for path in contract['paths'])
    assert not any(name.startswith('P0') for name in contract['components']['schemas'])

    def check(value):
        if isinstance(value, dict):
            reference = value.get('$ref', '')
            if reference.startswith('#/'):
                target = contract
                for key in reference[2:].split('/'):
                    target = target[key.replace('~1', '/').replace('~0', '~')]
            for child in value.values():
                check(child)
        elif isinstance(value, list):
            for child in value:
                check(child)

    check(contract)


def test_event_window_options_and_result_scope_are_in_contract():
    project_root = Path(__file__).resolve().parents[3]
    contract = json.loads((project_root / 'contracts/openapi.json').read_text())
    for path in ['/features/ases/overview', '/features/ases/events']:
        parameters = {item.get('name'): item for item in contract['paths'][path]['get']['parameters']}
        assert parameters['event_window']['schema']['type'] == 'boolean'
        assert 'event_reference' in parameters
        assert '503' in contract['paths'][path]['get']['responses']
    assert 'event_window_selected_asn' in contract['components']['schemas']['AsOverview']['properties']['scope_kind']['enum']
