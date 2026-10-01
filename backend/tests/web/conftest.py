"""Web 测试不读取运行环境绑定的 AS 身份参考。"""
import pytest

from utils import asn_reference


@pytest.fixture(autouse=True)
def isolate_asn_identity_reference(monkeypatch):
    monkeypatch.setattr(asn_reference, 'AS_INFO_FILE', '')
    asn_reference._read_identities.cache_clear()
    yield
    asn_reference._read_identities.cache_clear()
