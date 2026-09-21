# M02 BGPRib 源码逐项登记

来源：`39578fe`；文件 SHA256：`bc0a56a2db8a6c0c90d7f797be27b096a83a0fbf33eb6a77d261f407e94b42ff`。只读 AST 提取，未导入或执行旧代码。

本表配合[主映射](../旧新逻辑状态数据映射.md)中 M02 阅读。目标数据集：`route_observation / route_state / legacy_route_projection`（建议字段，尚未建表）。各字段保持源名称及原始类型，历史结果保留于对应数据集的 legacy 命名空间；工作态写 PostgreSQL，历史结果写 DuckLake 管理的 Parquet。连接、logger、缓存及文件队列为运行元数据，不冒充业务事实。实施阶段与消费者、单位、基线及差异以主表为准。

## 函数与实际调用

| 定位 | 函数/输入 | 直接调用（包含内部和外部） |
|---|---|---|
| [BGPRib.py:21](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:21>) | `normalize_bgp_prefix(prefix)` | `ipaddress.ip_network`; `str`; `str(prefix).strip` |
| [BGPRib.py:29](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:29>) | `get_feature_prefix_skip_reason(version, prefixlen, prefix=None)` |  |
| [BGPRib.py:57](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:57>) | `__init__(self, bgp_info, rib_file, country_filter=None, load_rib=True)` | `dict`; `pytricia.PyTricia`; `self.init_rib`; `set` |
| [BGPRib.py:82](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:82>) | `_is_asn_in_country_filter(self, asn)` | `get_as_country` |
| [BGPRib.py:90](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:90>) | `init_rib(self)` | `datetime.datetime.fromtimestamp`; `datetime.timedelta`; `int`; `len`; `line.strip`; `line.strip().split`; `normalize_bgp_prefix`; `open`; `path.split`; `print`; `self.__get_origin_asn`; `self._is_asn_in_country_filter`; `self.rebuild_trees`; `self.update_dict`; `t.strftime`; `time.time`; `traceback.format_exc` |
| [BGPRib.py:140](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:140>) | `rebuild_trees(self)` | `as_set.copy`; `int`; `normalize_bgp_prefix`; `print`; `pytricia.PyTricia`; `self.ipv4_tree.insert`; `self.ipv6_tree.insert`; `self.prefix_as.items`; `time.time` |
| [BGPRib.py:157](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:157>) | `dump_state(self)` | `as_set.copy`; `prefix_set.copy`; `self.as_prefix.items`; `self.prefix_as.items`; `self.prefix_dict.items`; `self.vp_set.copy`; `vp_path_dict.copy` |
| [BGPRib.py:183](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:183>) | `load_state(self, state)` | `dict`; `self.rebuild_trees`; `set`; `state.get`; `state.get('as_prefix', {}).items`; `state.get('prefix_as', {}).items`; `state.get('prefix_dict', {}).items` |
| [BGPRib.py:203](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:203>) | `has_parent_prefix(self, prefix)` | `ipaddress.ip_network`; `tree.get_key`; `tree.parent` |
| [BGPRib.py:229](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:229>) | `__check_private_as(self, asn)` | `int` |
| [BGPRib.py:242](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:242>) | `__get_origin_asn(self, as_path_fields)` | `len`; `range`; `self.__check_private_as` |
| [BGPRib.py:253](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:253>) | `update_rib(self, flag, prefix, vp, as_path, old_origin_set)` | `as_path.split`; `normalize_bgp_prefix`; `path.split`; `self.__get_origin_asn`; `self._is_asn_in_country_filter`; `self.as_prefix[withdrawn_origin_asn].discard`; `self.ipv4_tree.insert`; `self.ipv6_tree.insert`; `self.prefix_as.get`; `self.prefix_as.get(prefix, set()).copy`; `self.prefix_as[prefix].discard`; `self.prefix_dict[prefix].values`; `self.update_dict`; `set`; `tree.delete`; `tree.insert`; `withdrawn_path.split` |
| [BGPRib.py:336](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:336>) | `update_dict(self, prefix, vp, path, asn)` | `dict`; `normalize_bgp_prefix`; `self.as_prefix.setdefault`; `self.as_prefix.setdefault(asn, set()).add`; `self.prefix_as.setdefault`; `self.prefix_as.setdefault(prefix, set()).add`; `self.vp_set.add`; `set` |

## 字段、字面键与状态写入点

统计单位是写入点/结构键出现点，不是假定运行字段数量。动态索引保留表达式；同名键在不同结构或函数中不合并。每条记录通过所属函数绑定输入和判断分支；对照采用相同输入的前后态及按键集合核验。临时局部结构也登记，不能把本表总数称为数据库字段数。

| 编号 | 定位/函数 | 种类 | 原字段或表达式 → 保存方式 |
|---|---|---|---|
| M02-F0001 | [BGPRib.py:64](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:64>) `__init__` | 属性写入 | `self.bgp_info` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0002 | [BGPRib.py:65](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:65>) `__init__` | 属性写入 | `self.rib_file` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0003 | [BGPRib.py:66](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:66>) `__init__` | 属性写入 | `self.country_filter` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0004 | [BGPRib.py:68](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:68>) `__init__` | 属性写入 | `self.vp_set` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0005 | [BGPRib.py:69](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:69>) `__init__` | 属性写入 | `self.prefix_dict` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0006 | [BGPRib.py:70](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:70>) `__init__` | 属性写入 | `self.prefix_as` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0007 | [BGPRib.py:71](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:71>) `__init__` | 属性写入 | `self.as_prefix` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0008 | [BGPRib.py:72](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:72>) `__init__` | 属性写入 | `self.t` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0009 | [BGPRib.py:75](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:75>) `__init__` | 属性写入 | `self.ipv4_tree` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0010 | [BGPRib.py:76](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:76>) `__init__` | 属性写入 | `self.ipv6_tree` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0011 | [BGPRib.py:115](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:115>) `init_rib` | 属性写入 | `self.t` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0012 | [BGPRib.py:142](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:142>) `rebuild_trees` | 属性写入 | `self.ipv4_tree` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0013 | [BGPRib.py:143](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:143>) `rebuild_trees` | 属性写入 | `self.ipv6_tree` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0014 | [BGPRib.py:167](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:167>) `dump_state` | 结构字面键 | `vp_set` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0015 | [BGPRib.py:168](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:168>) `dump_state` | 结构字面键 | `prefix_dict` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0016 | [BGPRib.py:172](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:172>) `dump_state` | 结构字面键 | `prefix_as` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0017 | [BGPRib.py:176](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:176>) `dump_state` | 结构字面键 | `as_prefix` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0018 | [BGPRib.py:180](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:180>) `dump_state` | 结构字面键 | `t` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0019 | [BGPRib.py:186](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:186>) `load_state` | 属性写入 | `self.vp_set` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0020 | [BGPRib.py:187](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:187>) `load_state` | 属性写入 | `self.prefix_dict` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0021 | [BGPRib.py:191](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:191>) `load_state` | 属性写入 | `self.prefix_as` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0022 | [BGPRib.py:195](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:195>) `load_state` | 属性写入 | `self.as_prefix` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0023 | [BGPRib.py:199](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:199>) `load_state` | 属性写入 | `self.t` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0024 | [BGPRib.py:277](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:277>) `update_rib` | 键删除 | `self.prefix_dict[prefix][vp]` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0025 | [BGPRib.py:280](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:280>) `update_rib` | 键删除 | `self.prefix_dict[prefix]` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0026 | [BGPRib.py:298](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:298>) `update_rib` | 键删除 | `self.prefix_as[prefix]` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0027 | [BGPRib.py:304](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:304>) `update_rib` | 键删除 | `self.as_prefix[withdrawn_origin_asn]` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0028 | [BGPRib.py:348](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:348>) `update_dict` | 键写入 | `self.prefix_dict[prefix]` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M02-F0029 | [BGPRib.py:349](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:349>) `update_dict` | 键写入 | `self.prefix_dict[prefix][vp]` → M02 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |

## 判断与异常分支

每个 if、条件表达式和 except 都有编号，True/False 列给出分支首动作，完整动作由源定位及主映射的生命周期说明限定。布尔短路子条件、循环退出和 return 不是单独的运行覆盖项；此处统计不代表 MC/DC、测试或真实数据分支命中率。`__check_country_outage_legacy_disabled` 的 return 后分支为不可达历史逻辑；其字段仅保存历史解释，不启用。

| 编号 | 定位/函数 | 类别/条件 | 成立或异常时 | 不成立时 |
|---|---|---|---|---|
| M02-B0001 | [BGPRib.py:24](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:24>) `normalize_bgp_prefix` | 异常: `ValueError` | `return (None, None, None)` | `无异常继续` |
| M02-B0002 | [BGPRib.py:30](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:30>) `get_feature_prefix_skip_reason` | if: `prefix in FEATURE_EXCLUDED_PREFIXES` | `return 'manually_excluded_prefix'` | `继续后继语句` |
| M02-B0003 | [BGPRib.py:32](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:32>) `get_feature_prefix_skip_reason` | if: `version == 4 and prefixlen < FEATURE_IPV4_MIN_PREFIXLEN` | `return 'oversized_ipv4_prefix'` | `继续后继语句` |
| M02-B0004 | [BGPRib.py:34](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:34>) `get_feature_prefix_skip_reason` | if: `version == 6 and prefixlen < FEATURE_IPV6_MIN_PREFIXLEN` | `return 'oversized_ipv6_prefix'` | `继续后继语句` |
| M02-B0005 | [BGPRib.py:79](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:79>) `__init__` | if: `load_rib` | `self.init_rib()` | `继续后继语句` |
| M02-B0006 | [BGPRib.py:83](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:83>) `_is_asn_in_country_filter` | if: `self.country_filter is None or asn is None or asn == ''` | `return True` | `继续后继语句` |
| M02-B0007 | [BGPRib.py:100](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:100>) `init_rib` | if: `len(fields) < 7` | `continue` | `继续后继语句` |
| M02-B0008 | [BGPRib.py:107](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:107>) `init_rib` | if: `normalized_prefix is None or normalized_prefix == '0.0.0.0/0' or normalized_prefix == '::/0'` | `continue` | `继续后继语句` |
| M02-B0009 | [BGPRib.py:111](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:111>) `init_rib` | if: `t_flag is False` | `t = datetime.datetime.fromtimestamp(int(timestamp), datetime.timezone.utc) + datetime.timedelta(hours=8)` | `继续后继语句` |
| M02-B0010 | [BGPRib.py:121](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:121>) `init_rib` | if: `asn is None or asn == ''` | `continue` | `继续后继语句` |
| M02-B0011 | [BGPRib.py:124](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:124>) `init_rib` | if: `not self._is_asn_in_country_filter(asn)` | `continue` | `继续后继语句` |
| M02-B0012 | [BGPRib.py:127](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:127>) `init_rib` | 异常: `所有异常` | `print(f'Error reading line {line_count} of RIB file: {self.rib_file}')` | `无异常继续` |
| M02-B0013 | [BGPRib.py:148](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:148>) `rebuild_trees` | if: `normalized_prefix is None` | `continue` | `继续后继语句` |
| M02-B0014 | [BGPRib.py:150](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:150>) `rebuild_trees` | if: `version == 4` | `self.ipv4_tree.insert(normalized_prefix, as_set.copy())` | `进入条件 version == 6` |
| M02-B0015 | [BGPRib.py:152](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:152>) `rebuild_trees` | if: `version == 6` | `self.ipv6_tree.insert(normalized_prefix, as_set.copy())` | `继续后继语句` |
| M02-B0016 | [BGPRib.py:211](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:211>) `has_parent_prefix` | if: `version == 4` | `tree = self.ipv4_tree` | `进入条件 version == 6` |
| M02-B0017 | [BGPRib.py:213](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:213>) `has_parent_prefix` | if: `version == 6` | `tree = self.ipv6_tree` | `继续后继语句` |
| M02-B0018 | [BGPRib.py:215](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:215>) `has_parent_prefix` | if: `prefix in tree` | `parent = tree.parent(prefix)` | `parent = tree.get_key(prefix)` |
| M02-B0019 | [BGPRib.py:221](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:221>) `has_parent_prefix` | if: `parent is not None and parent != prefix and (parent != '0.0.0.0/0') and (parent != '::/0')` | `return parent` | `return False` |
| M02-B0020 | [BGPRib.py:225](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:225>) `has_parent_prefix` | 异常: `所有异常` | `return False` | `无异常继续` |
| M02-B0021 | [BGPRib.py:230](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:230>) `__check_private_as` | if: `'{' in asn` | `return False` | `继续后继语句` |
| M02-B0022 | [BGPRib.py:232](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:232>) `__check_private_as` | if: `'_' in asn` | `return True` | `继续后继语句` |
| M02-B0023 | [BGPRib.py:239](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:239>) `__check_private_as` | 异常: `ValueError` | `return False` | `无异常继续` |
| M02-B0024 | [BGPRib.py:246](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:246>) `__get_origin_asn` | if: `self.__check_private_as(asn)` | `continue` | `return asn` |
| M02-B0025 | [BGPRib.py:263](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:263>) `update_rib` | if: `normalized_prefix is None` | `return` | `继续后继语句` |
| M02-B0026 | [BGPRib.py:267](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:267>) `update_rib` | if: `flag == 'W'` | `进入条件 prefix in self.prefix_dict and vp in self.prefix_dict[prefix]` | `进入条件 flag == 'A'` |
| M02-B0027 | [BGPRib.py:269](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:269>) `update_rib` | if: `prefix in self.prefix_dict and vp in self.prefix_dict[prefix]` | `withdrawn_path = self.prefix_dict[prefix][vp]` | `继续后继语句` |
| M02-B0028 | [BGPRib.py:279](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:279>) `update_rib` | if: `not self.prefix_dict[prefix]` | `del self.prefix_dict[prefix]` | `继续后继语句` |
| M02-B0029 | [BGPRib.py:287](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:287>) `update_rib` | if: `prefix in self.prefix_dict` | `遍历 path in self.prefix_dict[prefix].values()` | `继续后继语句` |
| M02-B0030 | [BGPRib.py:289](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:289>) `update_rib` | if: `self.__get_origin_asn(path.split(' ')) == withdrawn_origin_asn` | `is_origin_still_present = True` | `继续后继语句` |
| M02-B0031 | [BGPRib.py:294](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:294>) `update_rib` | if: `not is_origin_still_present` | `进入条件 withdrawn_origin_asn in self.prefix_as.get(prefix, set())` | `继续后继语句` |
| M02-B0032 | [BGPRib.py:295](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:295>) `update_rib` | if: `withdrawn_origin_asn in self.prefix_as.get(prefix, set())` | `self.prefix_as[prefix].discard(withdrawn_origin_asn)` | `继续后继语句` |
| M02-B0033 | [BGPRib.py:297](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:297>) `update_rib` | if: `not self.prefix_as[prefix]` | `del self.prefix_as[prefix]` | `继续后继语句` |
| M02-B0034 | [BGPRib.py:301](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:301>) `update_rib` | if: `withdrawn_origin_asn in self.as_prefix and prefix in self.as_prefix[withdrawn_origin_asn]` | `self.as_prefix[withdrawn_origin_asn].discard(prefix)` | `继续后继语句` |
| M02-B0035 | [BGPRib.py:303](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:303>) `update_rib` | if: `not self.as_prefix[withdrawn_origin_asn]` | `del self.as_prefix[withdrawn_origin_asn]` | `继续后继语句` |
| M02-B0036 | [BGPRib.py:307](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:307>) `update_rib` | 条件表达式: `version == 4` | `self.ipv4_tree` | `self.ipv6_tree` |
| M02-B0037 | [BGPRib.py:308](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:308>) `update_rib` | if: `prefix not in self.prefix_dict` | `进入条件 prefix in tree` | `进入条件 not is_origin_still_present` |
| M02-B0038 | [BGPRib.py:309](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:309>) `update_rib` | if: `prefix in tree` | `tree.delete(prefix)` | `继续后继语句` |
| M02-B0039 | [BGPRib.py:313](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:313>) `update_rib` | if: `not is_origin_still_present` | `tree.insert(prefix, self.prefix_as.get(prefix, set()).copy())` | `继续后继语句` |
| M02-B0040 | [BGPRib.py:316](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:316>) `update_rib` | if: `flag == 'A'` | `as_path_fields = as_path.split(' ')` | `pass` |
| M02-B0041 | [BGPRib.py:320](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:320>) `update_rib` | if: `not self._is_asn_in_country_filter(asn)` | `return` | `继续后继语句` |
| M02-B0042 | [BGPRib.py:326](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:326>) `update_rib` | if: `version == 4` | `self.ipv4_tree.insert(prefix, self.prefix_as.get(prefix, set()).copy())` | `进入条件 version == 6` |
| M02-B0043 | [BGPRib.py:329](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:329>) `update_rib` | if: `version == 6` | `self.ipv6_tree.insert(prefix, self.prefix_as.get(prefix, set()).copy())` | `继续后继语句` |
| M02-B0044 | [BGPRib.py:339](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:339>) `update_dict` | if: `normalized_prefix is None` | `return` | `继续后继语句` |
| M02-B0045 | [BGPRib.py:344](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:344>) `update_dict` | if: `vp not in self.vp_set` | `self.vp_set.add(vp)` | `继续后继语句` |
| M02-B0046 | [BGPRib.py:347](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:347>) `update_dict` | if: `prefix not in self.prefix_dict` | `self.prefix_dict[prefix] = dict()` | `继续后继语句` |
| M02-B0047 | [BGPRib.py:356](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPRib.py:356>) `<模块>` | if: `__name__ == '__main__'` | `from BGPInfo import BGPInfo` | `继续后继语句` |
