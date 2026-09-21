"""公开身份、精确数值和运行配置隔离；不需PG。"""
from dataclasses import replace
from fractions import Fraction
import json
import pytest
from data_pipeline.analysis.country_events.snapshot_store import ComponentBinding
from data_pipeline.analysis.country_events.selection_contract import canonical, result_id, contract_json, contract_value, exact_json, exact_value, CountryReadBinding, CountryRuntime, QueryLimits, QueryRequest


def binding():
    return ComponentBinding('123', 42, 'cat', 'abc', 'country_abc', 9,
                            'country-component/v1', 'a'*64, '/固定')


def test_complete_binding_identity_and_json():
    b = binding()
    r = CountryReadBinding(b, result_id(b), 'model', '/index', 'b'*64, None)
    assert contract_value(contract_json(r)) == r
    for name, value in [('database_oid', 43), ('snapshot', 10), ('root', '/other')]:
        assert result_id(replace(b, **{name: value})) != result_id(b)
    assert result_id(b) == result_id(ComponentBinding(**dict(reversed(list(b.__dict__.items())))))
    bad = json.loads(contract_json(r)); bad['fields']['unknown'] = 'bad'
    with pytest.raises(ValueError): contract_value(canonical(bad))
    with pytest.raises(ValueError): contract_json(CountryRuntime('password=not_serializable'))


@pytest.mark.parametrize('value', [None, 0, Fraction(0), Fraction(1,2), Fraction(1,2**80), Fraction(2**128-1,2**80)])
def test_exact_wire(value):
    got = exact_value(exact_json(value))
    assert got == value and type(got) is type(value)


@pytest.mark.parametrize('value', [{'kind':'int','num':'01'}, {'kind':'fraction','num':'2','den':'4'},
                                  {'kind':'fraction','num':'1','den':'-2'}, {'kind':'none','num':'0'}])
def test_reject_noncanonical_exact(value):
    with pytest.raises(ValueError): exact_value(value)


def test_bounded_requests_and_no_float_metadata():
    with pytest.raises(ValueError): QueryRequest('sql')
    with pytest.raises(ValueError): QueryRequest('asns', '{"a": 1}')
    with pytest.raises(ValueError): QueryLimits(max_page_rows=0)
    with pytest.raises(ValueError): canonical({'number': 1.5})
