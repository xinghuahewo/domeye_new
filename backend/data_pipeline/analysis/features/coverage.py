"""普通 Feature 的可重建覆盖索引；只记录前缀关联和支持次数，不复制路由负载。"""
from functools import lru_cache
from heapq import nsmallest
from ipaddress import ip_network


class BlockUnion:
    """压缩前缀树上的 /24 或 /48 并集，撤回时恢复仍受支持的子块。"""

    def __init__(self, version):
        import pytricia
        self.version = version
        self.unit = 24 if version == 4 else 48
        self.tree = pytricia.PyTricia(32 if version == 4 else 128)
        self.total = 0

    def _children_size(self, key):
        # PyTricia.children 返回所有后代，只累计没有其他已覆盖父节点的直接子节点。
        return sum(1 << (self.unit - int(child.rsplit('/', 1)[1]))
                   for child in self.tree.children(key) if self.tree.parent(child) == key)

    def add(self, key):
        if self.tree.has_key(key):
            self.tree[key] += 1
            return
        self.tree[key] = 1
        if self.tree.parent(key) is None:
            self.total += (1 << (self.unit - int(key.rsplit('/', 1)[1]))) - self._children_size(key)

    def discard(self, key):
        count = self.tree[key]
        if count > 1:
            self.tree[key] = count - 1
            return
        if self.tree.parent(key) is None:
            self.total -= (1 << (self.unit - int(key.rsplit('/', 1)[1]))) - self._children_size(key)
        del self.tree[key]


@lru_cache(maxsize=8192)
def _facts(prefix):
    from data_pipeline.analysis.features.calculation import prefix_filter
    normalized, reason = prefix_filter(prefix)
    if reason:
        return normalized, reason, None, None
    net = ip_network(normalized)
    unit = 24 if net.version == 4 else 48
    bucket = str(net.supernet(new_prefix=unit)) if net.prefixlen > unit else normalized
    return normalized, None, net.version, bucket


class CoverageScope:
    """计数属于业务关联支持；raw 与规范键分开，集合用于原结果合同。"""

    def __init__(self):
        self.raw = {}
        self.v4 = {}
        self.v6 = {}
        self.blocks4 = BlockUnion(4)
        self.blocks6 = BlockUnion(6)
        self.rejected = {}
        self._sample = None

    def add(self, prefix, facts):
        self.raw[prefix] = self.raw.get(prefix, 0) + 1
        normalized, reason, version, bucket = facts
        if reason:
            self.rejected[prefix] = normalized, reason
            return
        counts, blocks = (self.v4, self.blocks4) if version == 4 else (self.v6, self.blocks6)
        count = counts.get(normalized, 0)
        counts[normalized] = count + 1
        if not count:
            blocks.add(bucket)
            if version == 4 and self._sample is not None:
                self._sample = tuple(sorted((*self._sample, normalized))[:10])

    def discard(self, prefix, facts):
        count = self.raw[prefix]
        if count == 1:
            del self.raw[prefix]
            self.rejected.pop(prefix, None)
        else:
            self.raw[prefix] = count - 1
        normalized, reason, version, bucket = facts
        if reason:
            return
        counts, blocks = (self.v4, self.blocks4) if version == 4 else (self.v6, self.blocks6)
        count = counts[normalized]
        if count == 1:
            del counts[normalized]
            blocks.discard(bucket)
            if version == 4 and self._sample is not None and normalized in self._sample:
                self._sample = None
        else:
            counts[normalized] = count - 1

    def values(self, announcements, withdrawals):
        from data_pipeline.analysis.features.calculation import Values
        count = self.blocks4.total
        return Values(count, self.blocks6.total, count * 256, announcements, withdrawals)

    def warning_sample(self):
        if self._sample is None:
            self._sample = tuple(nsmallest(10, self.v4))
        return self._sample

    def diagnostics(self, country, asn):
        from data_pipeline.analysis.features.calculation import Diagnostic
        seen = set()
        for prefix in sorted(self.rejected):
            normalized, reason = self.rejected[prefix]
            key = reason, prefix if normalized is None else normalized
            if key not in seen:
                seen.add(key)
                yield Diagnostic('aggregate', f'{country}:{asn}', reason, prefix, normalized)


class FeatureCoverage:
    """由已核验的普通 Feature as_prefix 恢复，随后只同步真实关联变化。

    旧窗口仅汇总 feature_dict 中出现过的 ASN，因此路由索引中新出现、却尚未
    产生 Feature 计数的起源不能提前进入国家和 collect 的覆盖集合。
    """

    def __init__(self, reference, projection=None, feature_dict=None):
        self.reference = reference
        self.as_prefix = {}
        self.asns = {}
        self.countries = {}
        self.collect = CoverageScope()
        if projection is not None:
            self.rebuild(projection.as_prefix, feature_dict)

    def rebuild(self, as_prefix, feature_dict):
        self.as_prefix = as_prefix
        self.asns.clear()
        self.countries.clear()
        self.collect = CoverageScope()
        for country, asns in feature_dict.items():
            if country not in self.countries:
                self.countries[country] = CoverageScope()
            for asn in asns:
                self.ensure_asn(asn, country)
        return self

    def ensure_asn(self, asn, country=None):
        if asn in self.asns:
            return self.asns[asn]
        country = self.reference.country(asn) if country is None else country
        scope = self.asns[asn] = CoverageScope()
        target = self.countries.get(country)
        if target is None:
            target = self.countries[country] = CoverageScope()
        for prefix in self.as_prefix.get(asn, ()):
            facts = _facts(prefix)
            scope.add(prefix, facts)
            target.add(prefix, facts)
            self.collect.add(prefix, facts)
        return scope

    def add(self, asn, prefix):
        scope = self.asns.get(asn)
        if scope is None or prefix in scope.raw:
            return
        facts = _facts(prefix)
        scope.add(prefix, facts)
        self.countries[self.reference.country(asn)].add(prefix, facts)
        self.collect.add(prefix, facts)

    def discard(self, asn, prefix):
        scope = self.asns.get(asn)
        if scope is None or prefix not in scope.raw:
            return
        facts = _facts(prefix)
        scope.discard(prefix, facts)
        self.countries[self.reference.country(asn)].discard(prefix, facts)
        self.collect.discard(prefix, facts)

    def sync_prefix(self, prefix, old_origins, new_origins):
        for asn in old_origins - new_origins:
            self.discard(asn, prefix)
        for asn in new_origins - old_origins:
            self.add(asn, prefix)

    def validate_projection(self, projection):
        if projection.as_prefix is not self.as_prefix:
            raise ValueError('覆盖缓存未绑定当前业务关联索引')
