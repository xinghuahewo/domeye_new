"""只读显式Country原元数据；不调用current、不连接PG或签发AD。"""
import json
import os
from pathlib import Path
from copy import deepcopy
import pytest
from data_pipeline.results.country_metadata import metadata
from data_pipeline.results.component_roles import known_role, codec
from data_pipeline.results.component_readers import api, LOCK_OWNERS
from data_pipeline.results.component_streams import requests


@pytest.fixture(params=['real-gap-final-72e7bc1','real-clean-final-72e7bc1'])
def original(request):
    root=os.environ.get('DOMEYE_COMBINED_COUNTRY_METADATA')
    if not root:pytest.skip('须显式提供Country原元数据')
    return json.loads((Path(root)/request.param/'Country正式Admission.json').read_text())


def test_actual_country_edges_and_queries(original):
    upstream,params,mb=metadata(original)
    assert len(upstream['admissions'])==14
    for dep in upstream['admissions']:
        r=known_role('country',original,dep['admission_id'],dep)
        assert r['input_binding_typed']==dep['owner_binding']
        assert [s['source_id'] for s in r['selected_sources']]==([] if dep['owner']=='reference' else mb['ordered_source_ids'])
        bad=deepcopy(dep);bad['inventory_digest']='changed'
        with pytest.raises(ValueError,match='Admission值'):known_role('country',original,dep['admission_id'],bad)
    plan=requests('country',batch_rows=2,batch_bytes=65536,admission=original)
    assert [q['view'] for q in plan]==['events','country_qualification','country_qualified_value','country_coverage']
    assert all(codec('country').untyped(q['scope_typed'])['window_us']==tuple(params['result_window_us']) for q in plan)
    for target in original['lock_targets']:
        assert LOCK_OWNERS[target['namespace']]==('country',target['stage'])
    assert all(callable(getattr(api('country'),name)) for name in ('admit','verify_current','hold_lock','open_reader'))


def test_country_dependency_loss_rejected(original):
    broken=deepcopy(original);broken['dependencies']=broken['dependencies'][:-1]
    with pytest.raises(ValueError,match='依赖集合'):metadata(broken)


def test_unknown_owner_still_unavailable():
    with pytest.raises(ValueError):api('unknown-owner')
    with pytest.raises(ValueError):requests('unknown-owner',batch_rows=1,batch_bytes=1000)
