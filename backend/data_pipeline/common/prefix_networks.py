"""统计和检测共用的前缀解释；只保存有界、不可变的网络属性。"""
from functools import lru_cache
from ipaddress import ip_network
from typing import NamedTuple


class PrefixFacts(NamedTuple):
    text: str
    version: int
    prefixlen: int


def _describe(value, strict):
    network = ip_network(value, strict=strict)
    return PrefixFacts(str(network), network.version, network.prefixlen)


@lru_cache(maxsize=4096)
def _normalized(text):
    return _describe(text, False)


def network_facts(value, *, strict=True):
    if type(value) is str and len(value) <= 128:
        facts = _normalized(value)
        # 规范文本已经确认无主机位。其他合法写法仍按 ip_network 的严格规则检查，
        # 不能用字符串不相等来拒绝 netmask 写法，也不能让 strict=True 接受主机位。
        if not strict or facts.text == value:
            return facts
    return _describe(value, strict)
