"""身份 CSV 使用临时参考，覆盖分块、复用与未知降级。"""
from unittest.mock import patch

import pytest

from services import asn_service
from utils import asn_reference


@pytest.fixture(autouse=True)
def empty_loaded_references(monkeypatch):
    monkeypatch.setattr(asn_reference.data_loader, 'as_info', {})


def test_identity_columns_chunking_first_duplicate_and_shared_cache(tmp_path, monkeypatch):
    source = tmp_path / 'identity.csv'
    first = '00064500, 示例名称 ,参考国,Example Org,示例组织,Transit,,不应缓存的联系人\n'
    source.write_text('asn,as_name,as_country_cn,org_name,org_name_cn,type,type_cn,admin_info\n'
                      + first * 4096
                      + '64500,不替换首条,另一国,Other,,Access,,不应缓存\n'
                      + 'AS64501,,,Fallback Org,,,接入,不应缓存\n'
                      + '0,非法,,,,,,\n4294967296,非法,,,,,,\n', encoding='utf-8')
    monkeypatch.setattr(asn_reference, 'AS_INFO_FILE', str(source))
    with patch.object(asn_reference.pd, 'read_csv', wraps=asn_reference.pd.read_csv) as read:
        identity = asn_reference.get_asn_identity('64500')
        assert identity == {'as_name':'示例名称', 'as_country_cn':'参考国', 'org_name':'Example Org',
                            'org_name_cn':'示例组织', 'type':'Transit'}
        identity['as_name'] = '调用方不能修改缓存'
        assert asn_reference.get_asn_identity('64500')['as_name'] == '示例名称'
        assert asn_service._static_profile('64501')['org_name'] == 'Fallback Org'
        assert asn_service._static_profile('64501')['as_type'] == '接入'
        assert asn_reference.get_asn_identity('0') == {}
        assert asn_reference.get_asn_identity('4294967296') == {}
        assert read.call_count == 1
        options = read.call_args.kwargs
        assert options['chunksize'] == 4096
        assert options['usecols']('asn') and options['usecols']('as_name')
        for name in ('admin_info', 'tech_info', 'abuse_info', 'as_info', 'global_rank'):
            assert not options['usecols'](name)


@pytest.mark.parametrize('content', [None, '', 'other\nmissing asn\n', 'asn,as_name\n64500,"unterminated'])
def test_missing_empty_or_corrupt_reference_is_unknown(tmp_path, monkeypatch, content):
    source = tmp_path / 'identity.csv'
    if content is not None:
        source.write_text(content, encoding='utf-8')
    monkeypatch.setattr(asn_reference, 'AS_INFO_FILE', str(source))
    assert asn_reference.get_asn_identity('64500') == {}
    assert asn_service._static_profile('64500')['as_name'] == ''


def test_loaded_reference_preserves_rank_and_avoids_file_read(monkeypatch):
    monkeypatch.setattr(asn_reference.data_loader, 'as_info', {'64500': {
        'as_name':'已加载名称', 'org_name_cn':'中文组织', 'org_name':'English',
        'global_rank':'12', 'country_rank':'3', 'admin_info':'不应返回联系人'}})
    with patch.object(asn_reference.pd, 'read_csv', side_effect=AssertionError('无需重复读取')):
        assert asn_reference.get_asn_identity('64500') == {
            'as_name':'已加载名称', 'org_name_cn':'中文组织', 'org_name':'English'}
        profile = asn_service._static_profile('64500')
        assert (profile['global_rank'], profile['country_rank'], profile['org_name']) == (12, 3, '中文组织')
