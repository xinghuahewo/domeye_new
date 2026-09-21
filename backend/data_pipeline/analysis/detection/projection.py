"""Detection 专用旧投影；不接触或推进公共规范 RouteState。"""

from copy import deepcopy
from ipaddress import ip_network


def legacy_origin(path):
    fields = path.split(" ")
    for value in reversed(fields):
        if "{" in value:
            return value
        if "_" in value:
            continue
        try:
            number = int(value)
            private = 64512 <= number <= 65535 or 4200000000 <= number <= 4294967294
        except ValueError:
            private = False
        if not private:
            return value
    return fields[-1]


class _Tree:
    """仅提供计算确实使用的精确键和最近父级查找。"""

    def __init__(self, projection, version):
        self.projection, self.version = projection, version

    def has_key(self, prefix):
        return prefix in self.projection.tree_origins

    def get(self, prefix):
        return self.projection.tree_origins.get(prefix)

    def parent(self, prefix):
        net = ip_network(prefix)
        # 逐父层检查，避免每条观察扫描全部前缀。
        while net.prefixlen:
            net = net.supernet()
            if str(net) in self.projection.tree_origins:
                return str(net)
        return None


class DetectionProjection:
    version = "detection-legacy-add-origin-39578fe/v1"

    def __init__(self):
        self.prefix_dict, self.prefix_as, self.as_prefix = {}, {}, {}
        self.vp_set = set()
        self.tree_origins = {}
        self.t = ""
        self.ipv4_tree, self.ipv6_tree = _Tree(self, 4), _Tree(self, 6)

    def apply(self, action, prefix, vp, path):
        if action in ("A", "RIB"):
            origin = legacy_origin(path)
            self.vp_set.add(vp)
            self.prefix_dict.setdefault(prefix, {})[vp] = path
            # D01：覆盖路径仍只添加起源，旧残留不作为新事实。
            self.prefix_as.setdefault(prefix, set()).add(origin)
            self.as_prefix.setdefault(origin, set()).add(prefix)
            self.tree_origins[prefix] = self.prefix_as[prefix].copy()
        elif action == "W" and vp in self.prefix_dict.get(prefix, {}):
            origin = legacy_origin(self.prefix_dict[prefix].pop(vp))
            if not self.prefix_dict[prefix]:
                del self.prefix_dict[prefix]
            present = any(
                legacy_origin(p) == origin
                for p in self.prefix_dict.get(prefix, {}).values()
            )
            if not present:
                for mapping, key, value in (
                    (self.prefix_as, prefix, origin),
                    (self.as_prefix, origin, prefix),
                ):
                    if key in mapping:
                        mapping[key].discard(value)
                        if not mapping[key]:
                            del mapping[key]
            if prefix not in self.prefix_dict:
                self.tree_origins.pop(prefix, None)
            elif not present:
                self.tree_origins[prefix] = self.prefix_as.get(prefix, set()).copy()

    def at(self, prefix):
        return deepcopy(
            {
                "origins": self.prefix_as.get(prefix, set()),
                "vp_paths": self.prefix_dict.get(prefix, {}),
            }
        )

    def export(self):
        return deepcopy(
            {
                k: getattr(self, k)
                for k in (
                    "prefix_dict",
                    "prefix_as",
                    "as_prefix",
                    "vp_set",
                    "t",
                    "tree_origins",
                )
            }
        )
