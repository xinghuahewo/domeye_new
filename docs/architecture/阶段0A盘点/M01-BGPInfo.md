# M01 BGPInfo 源码逐项登记

来源：`39578fe`；文件 SHA256：`54fd6173b94d54747a82a8d42d8db33a54cfbbac8fdb9a294692e412593f57be`。只读 AST 提取，未导入或执行旧代码。

本表配合[主映射](../旧新逻辑状态数据映射.md)中 M01 阅读。目标数据集：`reference_snapshot / reference_rows`（建议字段，尚未建表）。各字段保持源名称及原始类型，历史结果保留于对应数据集的 legacy 命名空间；工作态写 PostgreSQL，历史结果写 DuckLake 管理的 Parquet。连接、logger、缓存及文件队列为运行元数据，不冒充业务事实。实施阶段与消费者、单位、基线及差异以主表为准。

## 函数与实际调用

| 定位 | 函数/输入 | 直接调用（包含内部和外部） |
|---|---|---|
| [BGPInfo.py:9](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:9>) | `load_local_env()` | `key.strip`; `line.split`; `line.startswith`; `open`; `os.path.abspath`; `os.path.dirname`; `os.path.exists`; `os.path.join`; `raw_line.strip`; `value.strip` |
| [BGPInfo.py:57](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:57>) | `__init__(self)` | `dict`; `self.load_info` |
| [BGPInfo.py:87](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:87>) | `load_info(self)` | `df.apply`; `df.drop_duplicates`; `df.iterrows`; `df.set_index`; `df_new.index.astype`; `df_new.to_dict`; `df_new['export_as'].apply`; `df_new['import_as'].apply`; `df_new['sibling_as'].apply`; `df_new['v4Peer'].apply`; `df_new['v6Peer'].apply`; `dict`; `eval`; `json.load`; `open`; `pd.concat`; `pd.read_csv`; `pd.read_excel`; `print`; `self.country.setdefault`; `self.triplet_info.setdefault`; `self.triplet_info.setdefault(first_as, dict()).setdefault`; `self.triplet_info.setdefault(first_as, dict()).setdefault(second_as, dict()).setdefault`; `str`; `time.time`; `traceback.format_exc` |
| [BGPInfo.py:159](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:159>) | `init_dict(row)` | `dict`; `self.triplet_info.setdefault`; `self.triplet_info.setdefault(first_as, dict()).setdefault`; `self.triplet_info.setdefault(first_as, dict()).setdefault(second_as, dict()).setdefault`; `str` |
| [BGPInfo.py:175](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:175>) | `get_status(self)` | `len`; `print` |

## 字段、字面键与状态写入点

统计单位是写入点/结构键出现点，不是假定运行字段数量。动态索引保留表达式；同名键在不同结构或函数中不合并。每条记录通过所属函数绑定输入和判断分支；对照采用相同输入的前后态及按键集合核验。临时局部结构也登记，不能把本表总数称为数据库字段数。

| 编号 | 定位/函数 | 种类 | 原字段或表达式 → 保存方式 |
|---|---|---|---|
| M01-F0001 | [BGPInfo.py:30](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:30>) `load_local_env` | 键写入 | `os.environ[key]` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0002 | [BGPInfo.py:59](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:59>) `__init__` | 属性写入 | `self.as_info_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0003 | [BGPInfo.py:60](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:60>) `__init__` | 属性写入 | `self.country_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0004 | [BGPInfo.py:61](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:61>) `__init__` | 属性写入 | `self.import_as_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0005 | [BGPInfo.py:62](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:62>) `__init__` | 属性写入 | `self.ipv4_all_prefix_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0006 | [BGPInfo.py:63](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:63>) `__init__` | 属性写入 | `self.ipv6_all_prefix_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0007 | [BGPInfo.py:64](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:64>) `__init__` | 属性写入 | `self.pfx2as_dict_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0008 | [BGPInfo.py:65](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:65>) `__init__` | 属性写入 | `self.as_rel_dict_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0009 | [BGPInfo.py:66](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:66>) `__init__` | 属性写入 | `self.prefix_info_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0010 | [BGPInfo.py:67](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:67>) `__init__` | 属性写入 | `self.important_domain_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0011 | [BGPInfo.py:68](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:68>) `__init__` | 属性写入 | `self.private_as_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0012 | [BGPInfo.py:69](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:69>) `__init__` | 属性写入 | `self.triplet_file` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0013 | [BGPInfo.py:72](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:72>) `__init__` | 属性写入 | `self.as_info` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0014 | [BGPInfo.py:73](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:73>) `__init__` | 属性写入 | `self.country` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0015 | [BGPInfo.py:74](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:74>) `__init__` | 属性写入 | `self.import_as_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0016 | [BGPInfo.py:75](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:75>) `__init__` | 属性写入 | `self.import_prefix_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0017 | [BGPInfo.py:76](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:76>) `__init__` | 属性写入 | `self.as_prefix_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0018 | [BGPInfo.py:77](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:77>) `__init__` | 属性写入 | `self.as_rel_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0019 | [BGPInfo.py:78](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:78>) `__init__` | 属性写入 | `self.prefix_info` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0020 | [BGPInfo.py:79](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:79>) `__init__` | 属性写入 | `self.important_domain_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0021 | [BGPInfo.py:80](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:80>) `__init__` | 属性写入 | `self.private_as_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0022 | [BGPInfo.py:81](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:81>) `__init__` | 属性写入 | `self.triplet_info` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0023 | [BGPInfo.py:101](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:101>) `load_info` | 属性写入 | `df_new.index` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0024 | [BGPInfo.py:102](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:102>) `load_info` | 键写入 | `df_new['import_as']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0025 | [BGPInfo.py:103](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:103>) `load_info` | 键写入 | `df_new['export_as']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0026 | [BGPInfo.py:104](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:104>) `load_info` | 键写入 | `df_new['v4Peer']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0027 | [BGPInfo.py:105](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:105>) `load_info` | 键写入 | `df_new['v6Peer']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0028 | [BGPInfo.py:106](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:106>) `load_info` | 键写入 | `df_new['sibling_as']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0029 | [BGPInfo.py:107](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:107>) `load_info` | 属性写入 | `self.as_info` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0030 | [BGPInfo.py:113](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:113>) `load_info` | 键写入 | `self.country[two_letter_code]['english_full_name']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0031 | [BGPInfo.py:114](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:114>) `load_info` | 键写入 | `self.country[two_letter_code]['english_short_name']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0032 | [BGPInfo.py:115](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:115>) `load_info` | 键写入 | `self.country[two_letter_code]['chinese_short_name']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0033 | [BGPInfo.py:116](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:116>) `load_info` | 键写入 | `self.country[two_letter_code]['three_letter_code']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0034 | [BGPInfo.py:117](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:117>) `load_info` | 键写入 | `self.country[two_letter_code]['digital_code']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0035 | [BGPInfo.py:118](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:118>) `load_info` | 键写入 | `self.country[two_letter_code]['phone_code']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0036 | [BGPInfo.py:119](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:119>) `load_info` | 键写入 | `self.country[two_letter_code]['jet_lag']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0037 | [BGPInfo.py:120](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:120>) `load_info` | 键写入 | `self.country[two_letter_code]['latitude']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0038 | [BGPInfo.py:121](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:121>) `load_info` | 键写入 | `self.country[two_letter_code]['longitude']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0039 | [BGPInfo.py:127](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:127>) `load_info` | 属性写入 | `self.important_as_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0040 | [BGPInfo.py:135](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:135>) `load_info` | 属性写入 | `self.important_prefix_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0041 | [BGPInfo.py:138](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:138>) `load_info` | 属性写入 | `self.as_prefix_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0042 | [BGPInfo.py:141](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:141>) `load_info` | 属性写入 | `self.as_rel_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0043 | [BGPInfo.py:146](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:146>) `load_info` | 属性写入 | `self.prefix_info` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0044 | [BGPInfo.py:151](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:151>) `load_info` | 属性写入 | `self.important_domain_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0045 | [BGPInfo.py:155](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:155>) `load_info` | 属性写入 | `self.private_as_dict` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M01-F0046 | [BGPInfo.py:162](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:162>) `init_dict` | 键写入 | `self.triplet_info[first_as][second_as][third_as]['stability']` → M01 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |

## 判断与异常分支

每个 if、条件表达式和 except 都有编号，True/False 列给出分支首动作，完整动作由源定位及主映射的生命周期说明限定。布尔短路子条件、循环退出和 return 不是单独的运行覆盖项；此处统计不代表 MC/DC、测试或真实数据分支命中率。`__check_country_outage_legacy_disabled` 的 return 后分支为不可达历史逻辑；其字段仅保存历史解释，不启用。

| 编号 | 定位/函数 | 类别/条件 | 成立或异常时 | 不成立时 |
|---|---|---|---|---|
| M01-B0001 | [BGPInfo.py:18](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:18>) `load_local_env` | if: `not os.path.exists(env_path)` | `continue` | `继续后继语句` |
| M01-B0002 | [BGPInfo.py:23](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:23>) `load_local_env` | if: `not line or line.startswith('#') or '=' not in line` | `continue` | `继续后继语句` |
| M01-B0003 | [BGPInfo.py:28](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:28>) `load_local_env` | if: `not key or key in os.environ` | `continue` | `继续后继语句` |
| M01-B0004 | [BGPInfo.py:102](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:102>) `load_info` | 条件表达式: `x` | `eval(x)` | `[]` |
| M01-B0005 | [BGPInfo.py:103](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:103>) `load_info` | 条件表达式: `x` | `eval(x)` | `[]` |
| M01-B0006 | [BGPInfo.py:104](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:104>) `load_info` | 条件表达式: `x` | `eval(x)` | `[]` |
| M01-B0007 | [BGPInfo.py:105](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:105>) `load_info` | 条件表达式: `x` | `eval(x)` | `[]` |
| M01-B0008 | [BGPInfo.py:106](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:106>) `load_info` | 条件表达式: `x` | `eval(x)` | `[]` |
| M01-B0009 | [BGPInfo.py:167](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:167>) `load_info` | 异常: `Exception` | `print(f'加载实体信息失败: {e}')` | `无异常继续` |
| M01-B0010 | [BGPInfo.py:188](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPInfo.py:188>) `<模块>` | if: `__name__ == '__main__'` | `bgp_info = BGPInfo(AS_INFO_FILE, COUNTRY_INFO_FILE, IMPORTANT_AS_FILE, IPV4_ALL_PREFIX_FILE, IPV6_ALL_PREFIX_FILE, PFX2AS_DICT_FILE, AS_REL_DICT_FILE, PREFIX_INFO_FILE, IMPORTANT_DOMAIN, PRIVATE_AS_FILE)` | `继续后继语句` |
