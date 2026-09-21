# M03 BGPResource 源码逐项登记

来源：`39578fe`；文件 SHA256：`33c472920b41b95b459581405da83b7332197de8f9b7b07427538f635038c520`。只读 AST 提取，未导入或执行旧代码。

本表配合[主映射](../旧新逻辑状态数据映射.md)中 M03 阅读。目标数据集：`resource_snapshot / resource_normal_band / vp_resource / country_topology`（建议字段，尚未建表）。各字段保持源名称及原始类型，历史结果保留于对应数据集的 legacy 命名空间；工作态写 PostgreSQL，历史结果写 DuckLake 管理的 Parquet。连接、logger、缓存及文件队列为运行元数据，不冒充业务事实。实施阶段与消费者、单位、基线及差异以主表为准。

## 函数与实际调用

| 定位 | 函数/输入 | 直接调用（包含内部和外部） |
|---|---|---|
| [BGPResource.py:14](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:14>) | `load_local_env()` | `key.strip`; `line.split`; `line.startswith`; `open`; `os.path.abspath`; `os.path.dirname`; `os.path.exists`; `os.path.join`; `raw_line.strip`; `value.strip` |
| [BGPResource.py:79](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:79>) | `__init__(self, as_info_file, country_file, conn, prefix_count_table)` | `dict` |
| [BGPResource.py:94](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:94>) | `init(self)` | `self.__init_as_info`; `self.__init_info` |
| [BGPResource.py:98](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:98>) | `__init_info(self)` | `prefix_count_logger.info`; `self._ensure_normal_range_bucket` |
| [BGPResource.py:105](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:105>) | `_ensure_normal_range_bucket(self, range_store, bucket: str, items)` | `dict` |
| [BGPResource.py:112](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:112>) | `_ensure_bucket(self, bucket: str, time_key: str)` | `dict`; `set` |
| [BGPResource.py:134](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:134>) | `_ensure_vp_bucket(self, vp_asn: str, time_key: str)` | `dict`; `self._ensure_normal_range_bucket`; `set` |
| [BGPResource.py:147](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:147>) | `_safe_as_rank(self, asn: str)` | `float`; `int`; `self.as_info.get`; `self.as_info.get(asn, {}).get` |
| [BGPResource.py:156](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:156>) | `_build_vp_rows(self, time_key: str)` | `calculate_v6_48_segments_count`; `len`; `rows.append`; `self.__check_vp_outlier`; `self._safe_as_rank`; `self.as_info.get`; `self.as_info.get(vp, {}).get`; `self.vp_prefix_count_dict.items` |
| [BGPResource.py:174](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:174>) | `_cleanup_old_state(self, state_store, bucket: str, file_name: str, keep_days=3)` | `datetime.timedelta`; `file_to_time`; `list`; `state_store.get`; `state_store.get(bucket, {}).keys`; `str` |
| [BGPResource.py:180](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:180>) | `_get_target_buckets(self, observed_vp: str)` | `buckets.append` |
| [BGPResource.py:186](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:186>) | `update_rib(self, rib_file)` | `'Error processing AS path {} in RIB file: {}'.format`; `calculate_c_segments_count`; `calculate_v6_48_segments_count`; `file_to_time`; `int`; `ipaddress.ip_network`; `len`; `line.strip`; `line.strip().split`; `open`; `os.path.basename`; `os.path.getsize`; `path.split`; `prefix_count_insert`; `prefix_count_logger.error`; `prefix_count_logger.info`; `self.__check_outlier`; `self._build_vp_rows`; `self._cleanup_old_state`; `self._ensure_bucket`; `self._ensure_vp_bucket`; `self._get_target_buckets`; `self.prefix_count_dict.items`; `self.prefix_count_dict[bucket][time]['ipv4_prefix'].add`; `self.prefix_count_dict[bucket][time]['ipv6_prefix'].add`; `self.prefix_count_dict[bucket][time]['path'].add`; `self.prefix_count_dict[bucket][time]['private_as'].add`; `self.prefix_count_dict[bucket][time]['public_as'].add`; `self.prefix_count_dict[bucket][time]['vp_set'].add`; `self.vp_prefix_count_dict[peer_asn][time]['ipv4_prefix'].add`; `self.vp_prefix_count_dict[peer_asn][time]['ipv6_prefix'].add`; `str`; `traceback.format_exc`; `upsert_vp_resource_rows` |
| [BGPResource.py:303](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:303>) | `_check_outlier_for_store(self, data_store, range_store, bucket, time_key, items, abnormal_store=None)` | `len`; `normal_list.append`; `range_store[bucket][item].get`; `self.__update_normal_range`; `self._ensure_normal_range_bucket` |
| [BGPResource.py:326](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:326>) | `__check_outlier(self, vp, time)` | `self._check_outlier_for_store` |
| [BGPResource.py:336](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:336>) | `__check_vp_outlier(self, vp, time)` | `self._check_outlier_for_store` |
| [BGPResource.py:345](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:345>) | `__update_normal_range(self, range_store, vp, item, normal_list)` | `'vp {}, item {}, normal_list {}'.format`; `int`; `len`; `np.mean`; `np.std`; `prefix_count_logger.info` |
| [BGPResource.py:357](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:357>) | `__init_country(self)` | `df.iterrows`; `dict`; `pd.read_excel`; `self.country.setdefault` |
| [BGPResource.py:372](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:372>) | `__init_as_info(self)` | `df.drop_duplicates`; `df.set_index`; `df_new.index.astype`; `df_new.to_dict`; `pd.read_csv` |
| [BGPResource.py:379](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:379>) | `get_prefix_count_table(self)` |  |
| [BGPResource.py:382](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:382>) | `set_prefix_count_table(self, prefix_count_table)` |  |
| [BGPResource.py:394](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:394>) | `__init__(self, conn, edge_table: str, snapshot_table: str, as_dict_path: str)` |  |
| [BGPResource.py:401](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:401>) | `init(self)` | `create_country_topology_edge_table`; `create_country_topology_snapshot_table`; `self._load_as_dict` |
| [BGPResource.py:406](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:406>) | `_load_as_dict(self)` | `json.load`; `open` |
| [BGPResource.py:410](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:410>) | `_is_private_asn(self, asn: str)` | `int` |
| [BGPResource.py:426](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:426>) | `_get_country_cn(self, asn: str)` | `self.as_dict.get`; `self.as_dict.get(str(asn), {}).get`; `str` |
| [BGPResource.py:435](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:435>) | `build_and_store_from_rib(self, rib_file: str, build_time)` | `as_path.split`; `country_edges.items`; `country_edges[c1].add`; `defaultdict`; `delete_country_edges`; `insert_country_edges`; `int`; `len`; `line.strip`; `line.strip().split`; `links.append`; `node_set.add`; `open`; `os.path.getsize`; `prefix_count_logger.info`; `self._get_country_cn`; `self._is_private_asn`; `set`; `str`; `upsert_country_snapshot` |
| [BGPResource.py:550](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:550>) | `acquire_resource_task_lock(conn)` | `bool`; `conn.cursor`; `conn.rollback`; `cursor.close`; `cursor.execute`; `cursor.fetchone`; `prefix_count_logger.error` |
| [BGPResource.py:566](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:566>) | `release_resource_task_lock(conn)` | `conn.commit`; `conn.cursor`; `conn.rollback`; `cursor.close`; `cursor.execute`; `getattr`; `prefix_count_logger.warning` |
| [BGPResource.py:582](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:582>) | `process_IN_CLOSE_WRITE(self, event)` | `'检测到新文件 {}, 添加到待处理队列。'.format`; `Rib_File_Queue.append`; `file_name.startswith`; `full_file_path.split`; `prefix_count_logger.info`; `str` |
| [BGPResource.py:599](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:599>) | `main()` | `'Handling file: {}'.format`; `'rib文件{}处理完毕，耗时{}秒'.format`; `'初始化完成，耗时{}秒'.format`; `'检测到新文件 {}, 添加到待处理队列。'.format`; `CountryTopologyBuilder`; `FileEventHandler`; `PrefixCount`; `Rib_File_Queue.append`; `Rib_File_Queue.pop`; `acquire_resource_task_lock`; `conn.close`; `create_prefix_count_table`; `create_vp_resource_table`; `datetime.datetime.now`; `datetime.datetime.utcnow`; `dump_file`; `exit`; `file_to_time`; `get_conn`; `get_data_path`; `get_rib_file`; `get_rib_path`; `if_table_exist`; `int`; `len`; `locals`; `notifier.start`; `notifier.stop`; `os.environ.get`; `os.listdir`; `os.path.basename`; `os.path.exists`; `os.path.getsize`; `os.path.join`; `prefix_count.init`; `prefix_count.update_rib`; `prefix_count_logger.error`; `prefix_count_logger.info`; `print`; `pyinotify.ThreadedNotifier`; `pyinotify.WatchManager`; `r_file.startswith`; `release_resource_task_lock`; `remove_file`; `rib_list.sort`; `sys.exit`; `time.sleep`; `time.time`; `topo_builder.build_and_store_from_rib`; `topo_builder.init`; `traceback.format_exc`; `watch_manager.add_watch` |

## 字段、字面键与状态写入点

统计单位是写入点/结构键出现点，不是假定运行字段数量。动态索引保留表达式；同名键在不同结构或函数中不合并。每条记录通过所属函数绑定输入和判断分支；对照采用相同输入的前后态及按键集合核验。临时局部结构也登记，不能把本表总数称为数据库字段数。

| 编号 | 定位/函数 | 种类 | 原字段或表达式 → 保存方式 |
|---|---|---|---|
| M03-F0001 | [BGPResource.py:35](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:35>) `load_local_env` | 键写入 | `os.environ[key]` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0002 | [BGPResource.py:80](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:80>) `__init__` | 属性写入 | `self.as_info_file` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0003 | [BGPResource.py:81](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:81>) `__init__` | 属性写入 | `self.country_file` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0004 | [BGPResource.py:82](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:82>) `__init__` | 属性写入 | `self.currently_abnormal` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0005 | [BGPResource.py:83](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:83>) `__init__` | 属性写入 | `self.normal_range` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0006 | [BGPResource.py:84](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:84>) `__init__` | 属性写入 | `self.vp_normal_range` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0007 | [BGPResource.py:85](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:85>) `__init__` | 属性写入 | `self.as_info` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0008 | [BGPResource.py:86](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:86>) `__init__` | 属性写入 | `self.country` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0009 | [BGPResource.py:87](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:87>) `__init__` | 属性写入 | `self.prefix_count_dict` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0010 | [BGPResource.py:88](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:88>) `__init__` | 属性写入 | `self.vp_prefix_count_dict` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0011 | [BGPResource.py:89](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:89>) `__init__` | 属性写入 | `self.conn` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0012 | [BGPResource.py:90](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:90>) `__init__` | 属性写入 | `self.prefix_count_table` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0013 | [BGPResource.py:91](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:91>) `__init__` | 属性写入 | `self.vp_table` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0014 | [BGPResource.py:92](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:92>) `__init__` | 属性写入 | `self.rib_count` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0015 | [BGPResource.py:107](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:107>) `_ensure_normal_range_bucket` | 键写入 | `range_store[bucket]` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0016 | [BGPResource.py:110](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:110>) `_ensure_normal_range_bucket` | 结构字面键 | `list_len` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0017 | [BGPResource.py:110](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:110>) `_ensure_normal_range_bucket` | 键写入 | `range_store[bucket][item]` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0018 | [BGPResource.py:113](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:113>) `_ensure_bucket` | 键写入 | `self.currently_abnormal[bucket]` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0019 | [BGPResource.py:115](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:115>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket]` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0020 | [BGPResource.py:117](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:117>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0021 | [BGPResource.py:118](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:118>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['ipv4_prefix']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0022 | [BGPResource.py:119](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:119>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['ipv6_prefix']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0023 | [BGPResource.py:120](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:120>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['vp_set']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0024 | [BGPResource.py:121](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:121>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['private_as']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0025 | [BGPResource.py:122](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:122>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['path']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0026 | [BGPResource.py:123](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:123>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['public_as']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0027 | [BGPResource.py:124](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:124>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['ipv4_prefix_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0028 | [BGPResource.py:125](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:125>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['ipv6_prefix_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0029 | [BGPResource.py:126](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:126>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['ipv4_address_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0030 | [BGPResource.py:127](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:127>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['ipv6_48_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0031 | [BGPResource.py:128](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:128>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['vp_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0032 | [BGPResource.py:129](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:129>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['private_as_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0033 | [BGPResource.py:130](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:130>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['path_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0034 | [BGPResource.py:131](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:131>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['public_as_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0035 | [BGPResource.py:132](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:132>) `_ensure_bucket` | 键写入 | `self.prefix_count_dict[bucket][time_key]['is_outlier']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0036 | [BGPResource.py:137](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:137>) `_ensure_vp_bucket` | 键写入 | `self.vp_prefix_count_dict[vp_asn]` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0037 | [BGPResource.py:139](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:139>) `_ensure_vp_bucket` | 键写入 | `self.vp_prefix_count_dict[vp_asn][time_key]` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0038 | [BGPResource.py:140](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:140>) `_ensure_vp_bucket` | 结构字面键 | `ipv4_prefix` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0039 | [BGPResource.py:141](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:141>) `_ensure_vp_bucket` | 结构字面键 | `ipv6_prefix` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0040 | [BGPResource.py:142](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:142>) `_ensure_vp_bucket` | 结构字面键 | `ipv4_prefix_count` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0041 | [BGPResource.py:143](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:143>) `_ensure_vp_bucket` | 结构字面键 | `ipv6_prefix_count` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0042 | [BGPResource.py:144](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:144>) `_ensure_vp_bucket` | 结构字面键 | `is_outlier` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0043 | [BGPResource.py:160](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:160>) `_build_vp_rows` | 键写入 | `self.vp_prefix_count_dict[vp][time_key]['ipv4_prefix_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0044 | [BGPResource.py:161](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:161>) `_build_vp_rows` | 键写入 | `self.vp_prefix_count_dict[vp][time_key]['ipv6_prefix_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0045 | [BGPResource.py:164](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:164>) `_build_vp_rows` | 结构字面键 | `time` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0046 | [BGPResource.py:165](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:165>) `_build_vp_rows` | 结构字面键 | `asn` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0047 | [BGPResource.py:166](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:166>) `_build_vp_rows` | 结构字面键 | `as_name` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0048 | [BGPResource.py:167](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:167>) `_build_vp_rows` | 结构字面键 | `as_rank` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0049 | [BGPResource.py:168](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:168>) `_build_vp_rows` | 结构字面键 | `ipv4_prefix_count` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0050 | [BGPResource.py:169](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:169>) `_build_vp_rows` | 结构字面键 | `ipv6_prefix_count` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0051 | [BGPResource.py:170](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:170>) `_build_vp_rows` | 结构字面键 | `is_outlier` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0052 | [BGPResource.py:178](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:178>) `_cleanup_old_state` | 键删除 | `state_store[bucket][prev_time]` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0053 | [BGPResource.py:189](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:189>) `update_rib` | 属性写入 | `self.rib_count` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0054 | [BGPResource.py:261](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:261>) `update_rib` | 键写入 | `self.prefix_count_dict[collector][time]['ipv4_prefix_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0055 | [BGPResource.py:264](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:264>) `update_rib` | 键写入 | `self.prefix_count_dict[collector][time]['ipv6_prefix_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0056 | [BGPResource.py:265](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:265>) `update_rib` | 键写入 | `self.prefix_count_dict[collector][time]['ipv4_address_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0057 | [BGPResource.py:266](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:266>) `update_rib` | 键写入 | `self.prefix_count_dict[collector][time]['ipv6_48_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0058 | [BGPResource.py:267](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:267>) `update_rib` | 键写入 | `self.prefix_count_dict[collector][time]['vp_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0059 | [BGPResource.py:268](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:268>) `update_rib` | 键删除 | `self.prefix_count_dict[collector][time]['vp_set']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0060 | [BGPResource.py:269](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:269>) `update_rib` | 键写入 | `self.prefix_count_dict[collector][time]['private_as_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0061 | [BGPResource.py:270](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:270>) `update_rib` | 键写入 | `self.prefix_count_dict[collector][time]['path_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0062 | [BGPResource.py:271](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:271>) `update_rib` | 键删除 | `self.prefix_count_dict[collector][time]['path']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0063 | [BGPResource.py:272](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:272>) `update_rib` | 键写入 | `self.prefix_count_dict[collector][time]['public_as_count']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0064 | [BGPResource.py:322](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:322>) `_check_outlier_for_store` | 键写入 | `data_store[bucket][time_key]['is_outlier']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0065 | [BGPResource.py:324](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:324>) `_check_outlier_for_store` | 键写入 | `abnormal_store[bucket]` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0066 | [BGPResource.py:353](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:353>) `__update_normal_range` | 键写入 | `range_store[vp][item]['list_len']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0067 | [BGPResource.py:354](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:354>) `__update_normal_range` | 键写入 | `range_store[vp][item]['upper_bound']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0068 | [BGPResource.py:355](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:355>) `__update_normal_range` | 键写入 | `range_store[vp][item]['lower_bound']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0069 | [BGPResource.py:362](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:362>) `__init_country` | 键写入 | `self.country[two_letter_code]['english_full_name']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0070 | [BGPResource.py:363](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:363>) `__init_country` | 键写入 | `self.country[two_letter_code]['english_short_name']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0071 | [BGPResource.py:364](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:364>) `__init_country` | 键写入 | `self.country[two_letter_code]['chinese_short_name']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0072 | [BGPResource.py:365](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:365>) `__init_country` | 键写入 | `self.country[two_letter_code]['three_letter_code']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0073 | [BGPResource.py:366](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:366>) `__init_country` | 键写入 | `self.country[two_letter_code]['digital_code']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0074 | [BGPResource.py:367](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:367>) `__init_country` | 键写入 | `self.country[two_letter_code]['phone_code']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0075 | [BGPResource.py:368](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:368>) `__init_country` | 键写入 | `self.country[two_letter_code]['jet_lag']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0076 | [BGPResource.py:369](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:369>) `__init_country` | 键写入 | `self.country[two_letter_code]['latitude']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0077 | [BGPResource.py:370](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:370>) `__init_country` | 键写入 | `self.country[two_letter_code]['longitude']` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0078 | [BGPResource.py:376](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:376>) `__init_as_info` | 属性写入 | `df_new.index` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0079 | [BGPResource.py:377](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:377>) `__init_as_info` | 属性写入 | `self.as_info` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0080 | [BGPResource.py:383](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:383>) `set_prefix_count_table` | 属性写入 | `self.prefix_count_table` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0081 | [BGPResource.py:395](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:395>) `__init__` | 属性写入 | `self.conn` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0082 | [BGPResource.py:396](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:396>) `__init__` | 属性写入 | `self.edge_table` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0083 | [BGPResource.py:397](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:397>) `__init__` | 属性写入 | `self.snapshot_table` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0084 | [BGPResource.py:398](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:398>) `__init__` | 属性写入 | `self.as_dict_path` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0085 | [BGPResource.py:399](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:399>) `__init__` | 属性写入 | `self.as_dict` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0086 | [BGPResource.py:408](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:408>) `_load_as_dict` | 属性写入 | `self.as_dict` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0087 | [BGPResource.py:515](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:515>) `build_and_store_from_rib` | 结构字面键 | `source` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0088 | [BGPResource.py:516](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:516>) `build_and_store_from_rib` | 结构字面键 | `target` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0089 | [BGPResource.py:517](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:517>) `build_and_store_from_rib` | 结构字面键 | `value` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0090 | [BGPResource.py:518](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:518>) `build_and_store_from_rib` | 结构字面键 | `color` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0091 | [BGPResource.py:518](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:518>) `build_and_store_from_rib` | 结构字面键 | `lineStyle` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0092 | [BGPResource.py:520](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:520>) `build_and_store_from_rib` | 结构字面键 | `color` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0093 | [BGPResource.py:520](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:520>) `build_and_store_from_rib` | 结构字面键 | `itemStyle` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0094 | [BGPResource.py:520](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:520>) `build_and_store_from_rib` | 结构字面键 | `name` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0095 | [BGPResource.py:522](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:522>) `build_and_store_from_rib` | 结构字面键 | `country_cn` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0096 | [BGPResource.py:523](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:523>) `build_and_store_from_rib` | 结构字面键 | `build_time` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0097 | [BGPResource.py:524](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:524>) `build_and_store_from_rib` | 结构字面键 | `node_count` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0098 | [BGPResource.py:525](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:525>) `build_and_store_from_rib` | 结构字面键 | `edge_count` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0099 | [BGPResource.py:526](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:526>) `build_and_store_from_rib` | 结构字面键 | `nodes` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |
| M03-F0100 | [BGPResource.py:527](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:527>) `build_and_store_from_rib` | 结构字面键 | `links` → M03 对应历史/工作态的同名字段或可重建索引；动态键按表达式展开，保留空值/集合 |

## 判断与异常分支

每个 if、条件表达式和 except 都有编号，True/False 列给出分支首动作，完整动作由源定位及主映射的生命周期说明限定。布尔短路子条件、循环退出和 return 不是单独的运行覆盖项；此处统计不代表 MC/DC、测试或真实数据分支命中率。`__check_country_outage_legacy_disabled` 的 return 后分支为不可达历史逻辑；其字段仅保存历史解释，不启用。

| 编号 | 定位/函数 | 类别/条件 | 成立或异常时 | 不成立时 |
|---|---|---|---|---|
| M03-B0001 | [BGPResource.py:23](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:23>) `load_local_env` | if: `not os.path.exists(env_path)` | `continue` | `继续后继语句` |
| M03-B0002 | [BGPResource.py:28](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:28>) `load_local_env` | if: `not line or line.startswith('#') or '=' not in line` | `continue` | `继续后继语句` |
| M03-B0003 | [BGPResource.py:33](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:33>) `load_local_env` | if: `not key or key in os.environ` | `continue` | `继续后继语句` |
| M03-B0004 | [BGPResource.py:106](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:106>) `_ensure_normal_range_bucket` | if: `bucket not in range_store` | `range_store[bucket] = dict()` | `继续后继语句` |
| M03-B0005 | [BGPResource.py:109](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:109>) `_ensure_normal_range_bucket` | if: `item not in range_store[bucket]` | `range_store[bucket][item] = {'list_len': 0}` | `继续后继语句` |
| M03-B0006 | [BGPResource.py:114](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:114>) `_ensure_bucket` | if: `bucket not in self.prefix_count_dict` | `self.prefix_count_dict[bucket] = dict()` | `继续后继语句` |
| M03-B0007 | [BGPResource.py:116](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:116>) `_ensure_bucket` | if: `time_key not in self.prefix_count_dict[bucket]` | `self.prefix_count_dict[bucket][time_key] = dict()` | `继续后继语句` |
| M03-B0008 | [BGPResource.py:136](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:136>) `_ensure_vp_bucket` | if: `vp_asn not in self.vp_prefix_count_dict` | `self.vp_prefix_count_dict[vp_asn] = dict()` | `继续后继语句` |
| M03-B0009 | [BGPResource.py:138](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:138>) `_ensure_vp_bucket` | if: `time_key not in self.vp_prefix_count_dict[vp_asn]` | `self.vp_prefix_count_dict[vp_asn][time_key] = {'ipv4_prefix': set(), 'ipv6_prefix': set(), 'ipv4_prefix_count': 0, 'ipv6_prefix_count': 0, 'is_outlier': False}` | `继续后继语句` |
| M03-B0010 | [BGPResource.py:149](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:149>) `_safe_as_rank` | if: `value in ['', None]` | `return None` | `继续后继语句` |
| M03-B0011 | [BGPResource.py:153](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:153>) `_safe_as_rank` | 异常: `(TypeError, ValueError)` | `return None` | `无异常继续` |
| M03-B0012 | [BGPResource.py:177](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:177>) `_cleanup_old_state` | if: `prev_time < cutoff` | `del state_store[bucket][prev_time]` | `继续后继语句` |
| M03-B0013 | [BGPResource.py:182](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:182>) `_get_target_buckets` | if: `observed_vp in self.TRACKED_COLLECTORS` | `buckets.append(observed_vp)` | `继续后继语句` |
| M03-B0014 | [BGPResource.py:193](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:193>) `update_rib` | 异常: `Exception` | `file_size = -1` | `无异常继续` |
| M03-B0015 | [BGPResource.py:204](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:204>) `update_rib` | if: `line_count % 1000000 == 0` | `prefix_count_logger.info(f'[RIB] parsing progress: file={file_name}, lines={line_count}')` | `继续后继语句` |
| M03-B0016 | [BGPResource.py:207](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:207>) `update_rib` | if: `len(fields) < 7` | `continue` | `继续后继语句` |
| M03-B0017 | [BGPResource.py:210](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:210>) `update_rib` | if: `flag == 'STATE' or prefix == '0.0.0.0/0' or prefix == '::/0'` | `continue` | `继续后继语句` |
| M03-B0018 | [BGPResource.py:223](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:223>) `update_rib` | if: `ip_version == 4` | `self.prefix_count_dict[bucket][time]['ipv4_prefix'].add(prefix)` | `self.prefix_count_dict[bucket][time]['ipv6_prefix'].add(prefix)` |
| M03-B0019 | [BGPResource.py:228](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:228>) `update_rib` | if: `ip_version == 4` | `self.vp_prefix_count_dict[peer_asn][time]['ipv4_prefix'].add(prefix)` | `self.vp_prefix_count_dict[peer_asn][time]['ipv6_prefix'].add(prefix)` |
| M03-B0020 | [BGPResource.py:234](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:234>) `update_rib` | if: `'{' in path` | `continue` | `继续后继语句` |
| M03-B0021 | [BGPResource.py:236](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:236>) `update_rib` | if: `int(last_asn) >= 64512 and int(last_asn) <= 65535 or int(last_asn) > 4294967295` | `private_as = last_asn` | `继续后继语句` |
| M03-B0022 | [BGPResource.py:248](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:248>) `update_rib` | if: `(int(last_asn) < 64512 or int(last_asn) > 65535) and int(last_asn) <= 4294967295` | `public_as = last_asn` | `继续后继语句` |
| M03-B0023 | [BGPResource.py:250](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:250>) `update_rib` | if: `public_as != None` | `遍历 bucket in target_buckets` | `继续后继语句` |
| M03-B0024 | [BGPResource.py:253](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:253>) `update_rib` | 异常: `所有异常` | `prefix_count_logger.info('Error processing AS path {} in RIB file: {}'.format(path, rib_file))` | `无异常继续` |
| M03-B0025 | [BGPResource.py:293](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:293>) `update_rib` | if: `vp_rows` | `upsert_vp_resource_rows(self.conn, self.vp_table, vp_rows)` | `继续后继语句` |
| M03-B0026 | [BGPResource.py:299](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:299>) `update_rib` | 异常: `Exception` | `prefix_count_logger.error(f'Error in updating routing table from RIB file {rib_file}: {e}')` | `无异常继续` |
| M03-B0027 | [BGPResource.py:308](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:308>) `_check_outlier_for_store` | if: `data_store[bucket][history_time]['is_outlier'] is False and history_time != time_key` | `normal_list.append(data_store[bucket][history_time][item])` | `继续后继语句` |
| M03-B0028 | [BGPResource.py:311](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:311>) `_check_outlier_for_store` | if: `len(normal_list) > 5` | `self.__update_normal_range(range_store, bucket, item, normal_list)` | `进入条件 self.rib_count < 7` |
| M03-B0029 | [BGPResource.py:313](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:313>) `_check_outlier_for_store` | if: `self.rib_count < 7` | `normal_list.append(data_store[bucket][time_key][item])` | `继续后继语句` |
| M03-B0030 | [BGPResource.py:319](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:319>) `_check_outlier_for_store` | if: `upper_bound is None or lower_bound is None` | `continue` | `继续后继语句` |
| M03-B0031 | [BGPResource.py:321](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:321>) `_check_outlier_for_store` | if: `data_store[bucket][time_key][item] > upper_bound or data_store[bucket][time_key][item] < lower_bound` | `data_store[bucket][time_key]['is_outlier'] = True` | `继续后继语句` |
| M03-B0032 | [BGPResource.py:323](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:323>) `_check_outlier_for_store` | if: `abnormal_store is not None` | `abnormal_store[bucket] = True` | `继续后继语句` |
| M03-B0033 | [BGPResource.py:346](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:346>) `__update_normal_range` | if: `len(normal_list) >= range_store[vp][item]['list_len']` | `prefix_count_logger.info(range_store[vp][item]['list_len'])` | `继续后继语句` |
| M03-B0034 | [BGPResource.py:414](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:414>) `_is_private_asn` | if: `asn is None or asn == ''` | `return True` | `继续后继语句` |
| M03-B0035 | [BGPResource.py:416](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:416>) `_is_private_asn` | if: `'{' in asn` | `return True` | `继续后继语句` |
| M03-B0036 | [BGPResource.py:418](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:418>) `_is_private_asn` | if: `'_' in asn` | `return True` | `继续后继语句` |
| M03-B0037 | [BGPResource.py:423](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:423>) `_is_private_asn` | 异常: `ValueError` | `return True` | `无异常继续` |
| M03-B0038 | [BGPResource.py:432](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:432>) `_get_country_cn` | 异常: `Exception` | `return None` | `无异常继续` |
| M03-B0039 | [BGPResource.py:441](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:441>) `build_and_store_from_rib` | 异常: `Exception` | `file_size = -1` | `无异常继续` |
| M03-B0040 | [BGPResource.py:454](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:454>) `build_and_store_from_rib` | if: `line_count % 2000000 == 0` | `prefix_count_logger.info(f'[TOPO] parsing progress: lines={line_count}')` | `继续后继语句` |
| M03-B0041 | [BGPResource.py:457](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:457>) `build_and_store_from_rib` | if: `len(fields) < 7` | `continue` | `继续后继语句` |
| M03-B0042 | [BGPResource.py:460](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:460>) `build_and_store_from_rib` | if: `not as_path or '{' in as_path` | `continue` | `继续后继语句` |
| M03-B0043 | [BGPResource.py:463](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:463>) `build_and_store_from_rib` | if: `len(path_list) < 2` | `continue` | `继续后继语句` |
| M03-B0044 | [BGPResource.py:468](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:468>) `build_and_store_from_rib` | if: `self._is_private_asn(raw_asn)` | `continue` | `继续后继语句` |
| M03-B0045 | [BGPResource.py:470](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:470>) `build_and_store_from_rib` | if: `prev is None` | `prev = raw_asn` | `继续后继语句` |
| M03-B0046 | [BGPResource.py:473](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:473>) `build_and_store_from_rib` | if: `prev == raw_asn` | `continue` | `继续后继语句` |
| M03-B0047 | [BGPResource.py:478](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:478>) `build_and_store_from_rib` | if: `not c1 or not c2 or c1 != c2` | `prev = raw_asn` | `继续后继语句` |
| M03-B0048 | [BGPResource.py:485](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:485>) `build_and_store_from_rib` | 异常: `Exception` | `prev = raw_asn` | `无异常继续` |
| M03-B0049 | [BGPResource.py:488](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:488>) `build_and_store_from_rib` | if: `a == b` | `prev = raw_asn` | `继续后继语句` |
| M03-B0050 | [BGPResource.py:491](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:491>) `build_and_store_from_rib` | if: `a > b` | `(a, b) = (b, a)` | `继续后继语句` |
| M03-B0051 | [BGPResource.py:508](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:508>) `build_and_store_from_rib` | if: `len(edges) <= COUNTRY_TOPOLOGY_FULL_EDGE_THRESHOLD` | `node_set = set()` | `继续后继语句` |
| M03-B0052 | [BGPResource.py:533](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:533>) `build_and_store_from_rib` | 异常: `Exception` | `prefix_count_logger.info(f'country snapshot build failed for {country_cn}')` | `无异常继续` |
| M03-B0053 | [BGPResource.py:555](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:555>) `acquire_resource_task_lock` | if: `not locked` | `prefix_count_logger.error('another BGPResource instance is already running, exit')` | `继续后继语句` |
| M03-B0054 | [BGPResource.py:558](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:558>) `acquire_resource_task_lock` | 异常: `Exception` | `conn.rollback()` | `无异常继续` |
| M03-B0055 | [BGPResource.py:567](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:567>) `release_resource_task_lock` | if: `conn is None or getattr(conn, 'closed', 1) != 0` | `return` | `继续后继语句` |
| M03-B0056 | [BGPResource.py:574](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:574>) `release_resource_task_lock` | 异常: `Exception` | `conn.rollback()` | `无异常继续` |
| M03-B0057 | [BGPResource.py:591](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:591>) `process_IN_CLOSE_WRITE` | if: `file_name.startswith('rib') or file_name.startswith('bview')` | `进入条件 Last_File_Name == '' or file_name > Last_File_Name` | `继续后继语句` |
| M03-B0058 | [BGPResource.py:593](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:593>) `process_IN_CLOSE_WRITE` | if: `Last_File_Name == '' or file_name > Last_File_Name` | `Last_File_Name = file_name` | `继续后继语句` |
| M03-B0059 | [BGPResource.py:602](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:602>) `main` | if: `MODE == 0` | `rib_file = get_rib_file(BASE_DATA_PATH)` | `rib_file_path = get_rib_path(BASE_DATA_PATH, RIB_HISTORY_FILE)` |
| M03-B0060 | [BGPResource.py:614](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:614>) `main` | if: `len(rib_list) == 0` | `print('没有找到合适的rib文件,等待600秒')` | `遍历 r_file in rib_list` |
| M03-B0061 | [BGPResource.py:619](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:619>) `main` | if: `r_file.startswith('bview')` | `rib_date = file_to_time(r_file)` | `继续后继语句` |
| M03-B0062 | [BGPResource.py:621](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:621>) `main` | if: `rib_date > rib_history_date` | `r_file = os.path.join(rib_file_path, r_file)` | `继续后继语句` |
| M03-B0063 | [BGPResource.py:629](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:629>) `main` | if: `rib_file == ''` | `prefix_count_logger.error('没有找到合适的rib文件, 程序结束')` | `继续后继语句` |
| M03-B0064 | [BGPResource.py:646](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:646>) `main` | if: `not acquire_resource_task_lock(conn)` | `conn.close()` | `继续后继语句` |
| M03-B0065 | [BGPResource.py:651](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:651>) `main` | if: `not if_table_exist(conn, ROUTING_RES_TABLE)` | `create_prefix_count_table(conn=conn, prefix_count_table=ROUTING_RES_TABLE)` | `prefix_count_logger.info(f'routing resource table {ROUTING_RES_TABLE} already exists, skip runtime DDL')` |
| M03-B0066 | [BGPResource.py:656](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:656>) `main` | if: `not if_table_exist(conn, VP_RES_TABLE)` | `create_vp_resource_table(conn=conn, table_name=VP_RES_TABLE)` | `prefix_count_logger.info(f'vp resource table {VP_RES_TABLE} already exists, skip runtime DDL')` |
| M03-B0067 | [BGPResource.py:668](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:668>) `main` | if: `build_country_topology == '1'` | `进入异常保护块` | `继续后继语句` |
| M03-B0068 | [BGPResource.py:679](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:679>) `main` | 异常: `Exception` | `topo_builder = None` | `无异常继续` |
| M03-B0069 | [BGPResource.py:696](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:696>) `main` | if: `os.path.exists(data_path)` | `进入条件 data_path != Data_Path` | `继续后继语句` |
| M03-B0070 | [BGPResource.py:697](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:697>) `main` | if: `data_path != Data_Path` | `进入异常保护块` | `继续后继语句` |
| M03-B0071 | [BGPResource.py:709](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:709>) `main` | 异常: `所有异常` | `prefix_count_logger.error('监测目录更新失败')` | `无异常继续` |
| M03-B0072 | [BGPResource.py:718](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:718>) `main` | 异常: `Exception` | `build_time = datetime.datetime.utcnow()` | `无异常继续` |
| M03-B0073 | [BGPResource.py:724](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:724>) `main` | if: `is_init is False` | `init_start = int(time.time())` | `继续后继语句` |
| M03-B0074 | [BGPResource.py:737](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:737>) `main` | 异常: `Exception` | `dump_size = -1` | `无异常继续` |
| M03-B0075 | [BGPResource.py:746](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:746>) `main` | if: `topo_builder is not None` | `进入异常保护块` | `继续后继语句` |
| M03-B0076 | [BGPResource.py:751](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:751>) `main` | 异常: `Exception` | `prefix_count_logger.error(f'Country topology build failed: {e}')` | `无异常继续` |
| M03-B0077 | [BGPResource.py:762](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:762>) `main` | 异常: `KeyboardInterrupt` | `prefix_count_logger.info('KeyboardInterrupt  Ctrl+C 中断')` | `无异常继续` |
| M03-B0078 | [BGPResource.py:765](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:765>) `main` | 异常: `Exception` | `prefix_count_logger.error(f'Error: {e}')` | `无异常继续` |
| M03-B0079 | [BGPResource.py:771](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:771>) `main` | if: `'notifier' in locals()` | `notifier.stop()` | `继续后继语句` |
| M03-B0080 | [BGPResource.py:773](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:773>) `main` | if: `'conn' in locals()` | `release_resource_task_lock(conn)` | `继续后继语句` |
| M03-B0081 | [BGPResource.py:776](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:776>) `main` | 异常: `所有异常` | `pass` | `无异常继续` |
| M03-B0082 | [BGPResource.py:781](</Users/botongwu/Documents/domeye/project-docs-work/backend/core/BGPResource.py:781>) `<模块>` | if: `__name__ == '__main__'` | `main()` | `继续后继语句` |

## Resource 独立计算迁入（2026-09-13，fixture 已验证，存储接入待实施）

本节是在阶段0A静态登记之上的实施进度，不改写上表的旧源码证据。工作包从 `d105d304ab9207426c72d0c5dc811077dc9e613a` 分出；实现见 [Resource 纯计算模块](../../../backend/data_pipeline/analysis/resources/compute.py)，测试见 [构造输入测试](../../../backend/tests/resources/test_resource_computation.py)。当前仅提供新项目内计算与完整类型化工作态导出，尚未接入阶段1冻结观察、Parquet/DuckLake/PostgreSQL、真实RIB、网页或发布。规则名 `resource-39578fe-v1` 表示明确保留的旧业务投影，不表示标准 origin 归属、旧写库副作用或连续运行等价。

### 输入与接口

- `ResourceComputer(references, topology_enabled=False).compute(context, elements)` 接收一份完整RIB，返回 `ResourceResult`；`export_state()` 返回独立深拷贝。各次时点严格递增、Collector固定，来源不匹配或计算异常不提交部分工作态。没有文件、数据库、监听、锁、环境读取和通知入口；没有自动恢复入口。
- `RibContext` 保存 `source_id/collector/snapshot_time/reference_id/time_basis`，另留 `legacy_time_text/legacy_time_basis/path_rendering_version`。时点必须含时区，不能用本地当前时间回落。真实适配器须提供MRT快照时点和文件名时间的独立依据。
- `RibElement` 保存 `source_id/message_id/ordinal/prefix/path/peer_asn/peer_ip/peer_bgp_id/attributes_ref/afi/safi/action/attributed_origin`。每条 `Decision.observation` 保留输入及原位置，AS_SET、非法或被过滤元素也不消失。原始属性及参考整行由引用指向上游原件，计算不筛掉原件中的未用列。
- 阶段1已确认原始MRT只有路径字节，另提供固定版本渲染文本；接入时必须绑定该版本，并保留AS_PATH/AS4_PATH字节、段类型与完整属性引用。`path` 不是二进制源中不存在的“原始空白文本”。当前fixture文本专门覆盖旧 `split(' ')` 顺序。尚未依赖阶段1未冻结实现。
- `ReferenceAs` 是已投影的AS资料，保留 `as_name/global_rank/country_cn/raw_row_ref`；`reference_id` 绑定完整原件。旧CSV的ASN去重保留首行、键转字符串，由后续参考adapter落实并另测；不能以本模块的字典输入声称已完成真实参考读取。空/非法rank为null，空name与null不混同。

### 类型化字段及旧字段去向

| 旧字段或结构 | 当前类型化去向与单位 | 来源 |
|---|---|---|
| `collector,time` | `ResourceRow.bucket,time`；`dimension=first_path_asn`；实际Collector另在context，不混同 | PrefixCount 112—132、180—184；prefix_count DDL 100—101 |
| `ipv4_prefix_count` | 同名int，去重IPv4前缀条数 | 261 |
| `ipv6_prefix_count,ipv6_48_count` | 同名int，去重覆盖IPv6 /48块数 | 262—266 |
| `ipv4_address_count` | 同名int，IPv4 /24覆盖块并集×256；并非严格地址并集 | 265 |
| `vp_count,private_as_count,path_count,public_as_count` | 同名int，分别是路径首ASN、旧私有尾ASN、渲染路径、旧公开尾ASN集合大小 | 267—272 |
| `ipv4_prefix,ipv6_prefix,vp_set,private_as,path,public_as` | `ResourceRow` 同名 `set[str]`，计数后也保留；可由逐元素位置及规则重建 | 118—123、226—253 |
| 五项 `*_normal_upper/*_normal_lower` | `ResourceState.normal_range[bucket][metric].upper_bound/lower_bound`，nullable int；metric按 `ipv4_prefix_count,ipv6_prefix_count,private_as_count,path_count,public_as_count` 映射；单位与对应计数相同 | 303—355、prefix_count_insert 167—195 |
| `is_outlier,currently_abnormal,rib_count` | `ResourceRow.is_outlier`、state同名字典/整数；历史每时点整体标签保存于两个history字典，不能拆成独立指标历史标签 | 82、92、189、322—324 |
| `normal_range,vp_normal_range,list_len` | 同名字典及 `NormalBand.list_len/upper_bound/lower_bound`；额外保存 `samples(tuple[NormalSample(time,source_id,value)]),mean,population_std`，即使三天清理后仍可核对当前范围来源 | 105—110、303—355 |
| `prefix_count_dict,vp_prefix_count_dict` | state中同名字典，key为bucket/Peer ASN及带时区时点，值为完整ResourceRow；导出附 `last_context,initial_state_basis=cold_start,rule_version` | 87—88、174—178 |
| VP的 `asn,time,as_name,as_rank,ipv4_prefix_count,ipv6_prefix_count,is_outlier` | `vp_rows` 中 `bucket=asn,dimension=peer_asn` 及其他同名字段；rank nullable int，IPv6仍 /48；同ASN不同IP合并，原Peer留于输入引用 | 156—172、vp_resource DDL 26—32 |
| 拓扑 `country_cn,build_time,a_asn,b_asn,weight` | `Topology.country_cn/build_time/edges(tuple[tuple[int,int]])/weight=1`；无向端点升序、边去重 | 478—496、country_topology DDL 45—49 |
| `node_count,edge_count,nodes,links,graph_json` | `Topology.nodes/edges` 为类型化集合，计数由len重建；旧node.name=str(ASN)，link.source/target=str(端点)，value=weight；lineStyle.color=`link_color`，itemStyle.color=`node_color`。不以不透明JSON代替业务结构 | 506—527、snapshot DDL 86—90 |
| `as_info/as_dict`、参考原属性 | `references`、`reference_id/raw_row_ref`；原件/整行所有列须由阶段1参考登记保存，不能只保留以上已使用列 | 372—377、406—408 |
| 未调用 `country` 九字段 | 保留原件及来源：english_full_name/english_short_name/chinese_short_name/three_letter_code/digital_code/phone_code/jet_lag/latitude/longitude；`__init_country` 未被init调用，本轮不擅自启用该投影 | 357—370 |
| 连接、表名、logger、文件队列、锁、清理 | 原映射保留运行用途；不进入纯计算业务态。历史表原行身份及重复行保留由存储迁入负责，本模块不读取旧表 | 89—91、379—383、550以后 |

字段单位还以代码 `METRIC_UNITS` 显式登记。索引可由bucket/time、country/time/端点重建；不存在新库已建索引的声明。

### 规则、差异与验证边界

1. 保留global及9808/4837/4134首路径桶，Peer ASN另聚合。非法prefix之前建桶、默认路由/STATE提前跳过；合法prefix和首ASN先纳入，再遇AS_SET跳过尾ASN和path；非法尾ASN同样保留先前集合。尾ASN私有规则仅64512—65535或大于4294967295，保留0、负值、32位私有ASN等旧公开集合结果，绝不填到 `attributed_origin`。
2. 正常带使用项目锁定numpy的mean与总体std，1.2/0.8及int截断，历史正常点大于5、前6份启动、第7份停止补当前、list_len只增、整体异常剔除、判定后清理均有断言。导出明确cold_start；当前无匹配旧初态，不能称三天暖机足够或真实全天等价。
3. 拓扑仍忽略prefix合法性与默认路由过滤（旧拓扑仅读path），私有/非法token跳过后可连接两侧；去重无向边权1，保留重复token、AS_SET、缺country分支。`topology_enabled` 默认关闭。新返回每时点图与 `computed/no_edges/missing_reference/graph_too_large/not_calculated`；`legacy_replace_edges/legacy_update_snapshot` 明示旧写入条件。新产物不把旧大图前的小图或缺席国家旧边称为当前图。未知参考的全局标记与已知国家边可同时出现，不能称资料完整。
4. **显式差异 R01**：结果排序确定、集合不删、异常原引用保留；不复制旧数据库吞错、写入部分成功和过期快照。异常候选不推进工作态，实存失败门禁待接入。
5. **显式差异 R02**：启动结束后才首次出现的prefix桶可能没有正常带；旧 `prefix_count_insert` 在SQL执行前直接索引缺失bound，可能KeyError并中止后续处理。新结果保留null，并令 `legacy_prefix_insert_ready=False`；这不是声称旧写库成功。后续存储/发布须依据范围可用性，不把该bool误作全链路成功。
6. **显式差异 R03**：无限rank旧转换可能OverflowError；新变null。特殊ASN旧拓扑可能超旧SQL int范围；Python整数保留，后续存储必须采用容纳值的类型并记录旧写入不可表示性，不能截断值。上述差异提请协调审阅，不将其冒充新标准业务事实。
7. **显式差异 R04**：纯计算拒绝无时区、逆序/重复时点和跨Collector工作态；旧文件名/本地时刻不作规范时点。原文件名解释与渲染版本由context另存。无自动checkpoint恢复。

验证命令：`uv run --project backend --frozen pytest backend/tests/resources/test_resource_computation.py -q`，当前 **30 passed**；连同既有 `test_prefix_quantity.py` 共 **34 passed**。fixture包含同ASN多IP、IPv4重叠及不同长度、IPv6 /48、AS_SET纳入先后、公私/特殊ASN边界、rank空与非法、启动/第7份/整体离群连锁/三天清理、缺country、重复/私有中间token、空图、恰50000及50001边。现有prefix_quantity helper与旧参考SHA256相同，复用新项目文件，未导入或运行旧代码。未改依赖锁。

尚待协调分发冻结接口后实施：规范观察adapter与原始格式fixture核验、参考首行去重/整行引用校验、类型化状态与历史结果的真实存储、具体run完成门禁、D内3份RIB及必要前置/旧状态比较。当前实现保留集合、逐元素决策及深拷贝以便核验；全量吞吐和内存尚未验证，真实运行前应以批量引用/存储adapter承接，不能直接把fixture实现性能视为生产GO。本包没有启动真实原件处理、接管试点、发布或部署。

## 阶段1冻结接口的隔离人工集成（2026-09-13，当前开发进度）

已将阶段1冻结提交 `8efd990730acfe8f8770eab90760b4f39604a685` cherry-pick为本分支 `3ed07fb`，未读取或复制903b未提交代码。该上游后来被独立复核指出来源SHA身份、baseline角色、完成回执次序问题，**不是已准入上游**。以下是针对冻结Interface的人工fixture研发结果；最终汇合验收必须由协调分发后续修复再跑，不因下述fixture通过而消除上游问题。前节“存储接入待实施”描述的是 `359c2c4` 首次纯计算交付时的历史边界。

### 公共接口及类型化落点

[观察适配器](../../../backend/data_pipeline/analysis/resources/adapter.py)消费 `scan_observations/scan` 的已完成、同snapshot Arrow批，暂存至Git外私有DuckDB。选择显式RIB来源清单，按声明时点逐份读取来源内 `record,ordinal`，不会按source SHA的字典序推进正常带。UTC快照（首个物理RIB元素epoch）、原文件名时间文本/依据、实际min/max epoch分别保留。Peer原属性、属性/path_key与原元素位置仍引用上游完整观察；未重新解析真实原件。

[Resource存储](../../../backend/data_pipeline/analysis/resources/store.py)未修改阶段1的 `TABLES`、写入或读取核心。使用独立 `resource/v1` schema，每个执行有 `run_id`，内容身份另为 `dataset_id`；绑定 `upstream_run/upstream_snapshot`、规则、schema和源码digest。类型化历史表位于DuckLake管理的Parquet：

| 表 | 主要列及保存含义 |
|---|---|
| `sources` | source/collector/snapshot_time/reference_id/time_basis/legacy_time_text/legacy_time_basis/path_rendering_version/min_epoch/max_epoch/element_count |
| `metrics` | source/dimension/bucket/time、各指标、整体outlier、rank/name/参考行引用、normal_state、membership_ref、独立ipv6_cidr_count；VP不适用的路径/ASN等指标为null，不能补0 |
| `memberships` | source/dimension/bucket/kind/member，kind为ipv4_prefix/ipv6_prefix/vp_set/private_as/public_as/path；path成员为渲染文本SHA256，其他为原集合字符串值 |
| `rendered_paths` | path_digest/path_text，同渲染文本只存一次。不能distinct上游path_key替代旧path_count，因为MED等属性变化可产生多个path_key；原属性沿decision元素引用及上游字典保留 |
| `normal_bands/normal_samples` | 逐RIB、维度、bucket、metric的list_len/上下界/mean/population_std及样本source/time/value；保留原正常带计算顺序 |
| `topology_edges/topology_status` | source/country/整数端点/weight；每时点状态、节点/边计数、旧写入条件及样式；节点集合可由端点并集重建，不存重复的长JSON图。端点用DECIMAL(38,0)容纳旧SQL int不能表示的值，溢出拒绝而不截断 |
| `decision_refs` | source/event_id/status；完整Peer、属性、路径字节及原位置由绑定的上游event定位，不复制进每条结果 |

PostgreSQL的 `resource_runs` 管执行候选/完成/失败及湖snapshot、上游和版本身份；`resource_work_meta` 保存rib_count/最后source/cold_start依据，`resource_work_metrics`保存近三天各桶五项历史采样指标、整体标签及membership_ref，其他历史指标和集合通过source/维度/bucket查已绑定湖表重建；`resource_work_abnormal`保存当前标记；`resource_work_bands/resource_work_samples`保存全部正常带（含未出现桶的空范围）及其样本。最后context由最后source查`sources`还原。没有自动checkpoint恢复、outbox、共享发布或页面改动。

纯计算保留原内存接口，也提供成对 `decision_sink/membership_sink`：逐元素决策流式保存、集合保存取得引用后才清掉内存集合，`decisions_externalized`显式区分已外置与没有决策。历史状态深拷贝不再复制多RIB全部路径/前缀。两种模式在8次状态推进后正常带、数值、标签、成员集合逐项一致；sink失败不提交内存工作态，外部写出仍只是候选。

参考adapter按旧pandas的CSV列类型推断、ASN首行去重和字符串键投影；JSON顶层重复ASN及行内重复属性按最后值兼容，原始重复整行仍由上游保存。未知/空/非法rank保持既有明确处理。参考两个source ID与context必须一致，不能将错误参考标成所声明版本。未启用旧未调用country Excel投影。

### 完成与读取门禁

[离线编排](../../../backend/data_pipeline/analysis/resources/produce.py)只接受上游complete版本和manifest里明确baseline/snapshot来源。独立候选完成前校验：全部声明RIB恰一份、逐元素决策完整且不重复、每份恰一条global、各类成员计数与去重计数一致、渲染路径引用不孤立、工作态已保存、运行期间代码不变。必需execution回执写入并fsync成功后才将PG登记置complete；孤立文件不构成可读版本。`scan_resource`只读取登记的complete+snapshot。完成门禁/写回执失败的候选不会影响已完成版本。

Resource不把缺正常带补零：`normal_state=insufficient_normal_samples`和null范围并存，冷启动依据保留于PG及回执；不声称旧prefix_count_insert成功。资源保护是RSS和可用磁盘检查，正常处理无总运行时限。

### 实际验证与限制

命令使用新建、本任务独占的PG集群：`/Users/botongwu/.codex/outputs/resource-isolated-20260913/pg`，仅Unix socket，端口55489；没有连接共享库、生产服务或旧环境。测试DSN通过 `DOMEYE_RESOURCE_TEST_DSN` 显式传入，没有DSN时真实集成测试明确skip。

- `uv run --project backend --frozen pytest backend/tests/resources/test_resource_adapter.py backend/tests/resources/test_resource_store.py backend/tests/resources/test_resource_computation.py backend/tests/common/test_prefix_quantity.py -q`：**39 passed**，其中包括真实DuckLake/独立PG测试，而非只检查mock SQL。覆盖三RIB完整保存、同路径不同MED计数、参考重复首/末行规则、PG完整工作态引用、候选/失败不可读、回执写失败不可提前完成、外置sink失败不推进、成员与内存模式等价。
- [人工实测脚本](../../../scripts/benchmarks/resource-fixture-benchmark.py)将上游prepare和Resource compute分为独立进程，避免把上游生产RSS算入Resource。构造6份RIB，每份20000元素，总计120000个不同IPv4前缀、60000条不同AS_PATH，每路径另有两个MED值，来源间前缀和路径也不同；不是重复单路径吞吐测试。
- 实测运行 `77e9ec27872b4f8d826cf39e84eb9a2e`：包含完成门禁的总耗时 **3.376秒**，进程峰值RSS **615186432字节（约586.7 MiB）**；门禁前回执计时3.237秒和RSS608714752字节单列，不能冒充完整总耗时。写入6 source、18 metrics、600012 memberships、60000 rendered_paths、72 normal_bands、252 normal_samples、59999 topology_edges、6 topology_status、120000 decision_refs。少一条拓扑边是路径9808→9808的自环按旧规则剔除。独立预期核对各RIB前缀/path计数、路径字典与决策总数通过。
- 完整实测证据：`/Users/botongwu/.codex/outputs/resource-isolated-20260913/benchmark-6x20000/resource-final-measured-measurement.json`；回执及输入绑定同目录Git外保存。该数字仅代表本机、所列人工输入、短路径和本次缓存环境，不能推算真实全天内存、吞吐或宣称生产验收。

当前判定：**隔离fixture集成可审阅；真实全天/阶段2整体仍未验收。** 尚需协调分发阶段1修复冻结提交，重跑适配与集成，并另行对实际获准输入的规模/参考/历史可比较范围验收。历史初态Unknown不阻止本包继续研发，但不转成旧连续运行等价。本次没有运行真实D、发布或部署。

## 新来源身份冻结汇合（2026-09-13，当前隔离集成版本）

按协调分发，已保留Resource自己的提交，只汇入 `8efd990` 之后至 `ecbc03fd226bb0adefa164a9d593230e5028c71d` 的五个冻结提交。对应本分支提交依次为 `f55c58d/5289779/264cac2/b6107ba/d47137e`，无冲突，未复制903b未提交代码。阶段1独立增量复核仍由协调负责；下面结果不代表真实D准入。

当前Resource数据集为 **`resource/v2`**：`sources`新增独立 `content_sha256/origin_uri`，`source_id`使用冻结的 `collector + origin_uri + content_sha256` 身份函数，缓存文件path不进入身份。`RibContext`分别携带这三项；producer使用冻结的 `normalized_inputs` 验证manifest、按source_id选择RIB，并核对Arrow批content_sha256、原始URI及Collector。不接受缺新版身份的旧manifest，不把SHA回填为source_id。参考资料的source_id仍按阶段1参考表合同使用其原件SHA，不能把这点推广到MRT来源身份。

仍只用 `scan_observations/scan` 读取已完成同snapshot数据。Resource不调用原件解析入口；本次手工二进制fixture先经阶段1“保存观察→盘点端点→显式映射→读取已保存观察回放”流程，再由Resource读取已保存观察。各RIB正常带顺序、两个VP维度、路径文本去重、外置成员及旧规则均不变。DuckDB连接复用冻结的 `connect_duckdb`，支持任务独立扩展目录，不修改阶段1核心。

完成回执现在先记 `ready`，PG完成登记是唯一可消费依据；Resource确认提交后返回 `complete`。模拟提交成功但返回确认丢失时，核对PG权威状态，确认complete就不另写failure；权威状态不可取得时保留unknown，不编造失败。这只是候选完成状态核对，不增加checkpoint恢复或自动重跑。

重验命令仍使用本任务独立PG，新增 `test_observation_two_phase.py`；总计 **45 passed**（Resource/adapter/helper/真实Resource存储40项，加阶段1二阶段门禁5项）。本轮新增或重验：

- 三RIB的source_id、内容SHA、原始URI独立落库，source_id不等于SHA；清单内容版本错误或把SHA冒充source_id直接拒绝。
- 同一压缩内容、不同origin_uri保留两个来源；缓存路径别名不重复解析/计数。各来源可以显式独立选择生成Resource结果，内容相同不合并其身份；同一时点的两个来源仍遵循R04，不在一个正常带工作态中隐式合并。
- 原件解析调用计数在Resource执行前后不增加，确认Resource使用保存的观察；同AS_PATH不同MED仍为旧path_count=1。
- 候选/失败不可读取、ready文件不等于complete、回执写失败不可发布、已确认提交不误记失败、终结工作态不可修改。

前节6×20000大样本证据及其旧输入绑定原样保留，属于 `resource/v1` 人工性能证据。按协调要求，本次来源合同与完成门禁变化只重验相关集成，不重复全规模压测，也不声称旧性能证据已变成新版真实D验收。当前完成的是新冻结Interface上的隔离人工集成；真实D、阶段2整体、旧历史连续状态等价及生产发布仍未验收。

## 独立复核两项P2修复（2026-09-13）

针对独立报告 `270388cd9c6489a49d79b0b2c4d130e11668a9ee` 的两项P2实施最小修复，不修改真实D、旧failed制品或运行中上游；不重跑历史v1性能测试。

代码身份不再只拼接本包源码。新增[显式依赖清单](../../../backend/data_pipeline/analysis/resources/identity.py)：Resource全部实现；共享observations Interface及项目内传递依赖rib_origin；计量helper prefix_quantity；pyproject/uv.lock、数据档及观察合同；并记录实际Python、相关计算/解析/存储包与DuckDB已加载扩展版本。清单含相对路径、用途分组和逐文件SHA，采用规范化JSON摘要，避免无文件名拼接；初始清单另存Git外code-identity.json并进入完成回执，摘要继续绑定PG登记及dataset_id。参考解释前固定清单，存储创建及完成前重新核验，依赖或环境变化不能完成。

这是针对当前真实调用链人工审查的有限清单，不是自动依赖发现框架。今后新增跨目录调用、影响结果的配置或运行依赖时，必须同步审查该清单。运行选项中的参考、时点和路径渲染版本仍由sources绑定，拓扑计算状态随dataset身份绑定；内存/磁盘保护不改变旧数值公式。

验证使用临时代码身份副本，未修改项目helper/锁文件：helper、锁、共享store及数据档变更均使代码版本摘要变化；真实隔离PG中在保存工作态后模拟helper源码变化，完成门禁拒绝，运行状态failed且不可读取，没有execution完成回执。该测试证明运行期依赖门禁，不声称在真实运行代码中热替换过helper。

CSV解释移除“一律列数相等”的自定义限制，交给旧参数pandas `read_csv(keep_default_na=False,usecols=...)`决定短列补空及额外列处理；使用带引号重建记录，避免合法空字段被再次解释为空白行，原逻辑record序号不随跳空行或ASN首行去重重排。`[]`空记录可以跳过，`['']`及分隔符空字段记录必须保留。旧v1的单字段ASCII空白缺少引号依据，显式拒绝而非猜测跳过；新版统一词法字段由阶段1提供，Resource不另开原件CSV解析器。

已按协调分发合入统一参考冻结 `726481f5b86482637def7e99cc536c701a3b0b2d`（本分支 `ed17179`），不是修改903b工作树。Resource直接消费 `reference-rows/v2.csv_record_kind`，只跳 `blank_line/whitespace_line`；`record`包括带引号空白、`""`、分隔符空字段、FF/VT/NBSP/EM SPACE及多行引号内容，均交旧pandas参数解释。原逻辑row、物理起止行、原字节偏移/长度/摘要留在统一参考中，Resource引用仍为原source/location/row，未根据筛选后的index重编号。没有另读真实CSV或旧failed候选。

最终受影响验证命令为 `uv run --project backend --frozen pytest backend/tests/resources/test_resource_identity.py backend/tests/resources/test_resource_adapter.py backend/tests/resources/test_resource_store.py -q`，绑定本任务独立 `DOMEYE_RESOURCE_TEST_DSN`：**20 passed，4.23秒**。覆盖5种依赖文件/初始化文件变化、运行中helper身份源变更的真实PG拒绝、v1歧义明确拒绝、实际临时CSV→统一rows→adapter→指标与原引用、v2裸空格/tab和quoted区分、FF/VT/非ASCII空白、短列/extra usecols，以及真实PG保存参考批→Resource指标/引用全路径。旧pandas实际调用作为CSV预期，没有导入旧项目；新词法分类、原字节摘要也经定向断言。初始代码清单和完成回执在真实隔离运行中均已保存。

依赖身份主体先独立提交 `c1b8d5e`；随后补全父包初始化文件并完成CSV适配及本文记录。与 `3c79d54` 的增量分为Resource两P2修复、统一参考冻结的原样cherry-pick两部分。当前状态是**修复与受影响隔离验证完成，待独立增量复核**，不是自行宣布整体GO或真实D准入；此前v1人工性能证据未改写、未重复压测。

## 已加载实现与版本绑定补修（2026-09-13，待独立复核）

增量报告 `35c0cfd4cf0cf4d7b388cfd6ec19fa49d319e187` 确认CSV为GO，但指出磁盘摘要不能证明热进程已加载实现。此次仅修复该边界，CSV代码保持不变。

正式入口为 `uv run --project backend --frozen python scripts/pipeline/resource-frozen-run.py /绝对路径/request.json`。请求JSON包含 `dsn`、`upstream_run`、`contexts`（RibContext各字段，snapshot_time为含时区ISO字符串）、`output`、`csv_source`、`country_source`及现有资源保护/拓扑选项；请求和凭据留在Git外。入口仅复制现有显式清单中的源码、父包、helper、共享Interface、锁和合同，不复制整仓、数据或pyc；校验复制前后身份，文件及目录设为只读。临时快照在子进程结束后清理，逐文件摘要和执行绑定保留在制品中。已有只读归档可通过 `--snapshot /绝对路径/归档` 复用，由归档内入口核验后执行。

新解释器使用 `-I -B`，不使用原工作树当前目录或PYTHONPATH。在任何项目业务导入前绑定快照身份和PID，拒绝残留pyc及预先加载的项目模块，再从快照导入。开始和完成时核对身份、PID、解释器隔离状态，并核验所有已加载data_pipeline/utils模块的实际源码位置在快照显式清单内；完成回执保存模块来源。执行模式、PID、快照路径和摘要进入代码身份，进而绑定PG code_digest与dataset_id。外部包仍按锁文件和实际版本绑定，不引入热更新或自动依赖发现。

直接Python API默认拒绝正式执行。测试必须显式 `fixture_only=True`，producer仅接受fixture://来源；身份和PG登记标注 `synthetic-fixture-api`。该模式的文件摘要仅是测试环境记录，不声称证明已加载实现。默认 `scan_resource` 拒绝fixture结果；人工测试须显式 `allow_fixture=True`。旧登记缺少执行模式不会被当作正式入口完成版本，数据库字段为增量添加；本次只在独立合成PG验证，没有迁移已有真实库。

定向验证覆盖真实旧import→修改临时helper→旧进程正式API拒绝→正式入口新解释器→真实隔离PG完成：旧进程仍返回1，新进程采用两倍helper，IPv4地址指标由512变为1024，完成身份逐文件SHA与新副本一致。另覆盖只读归档复用、默认读取拒绝fixture、既有候选/失败/提交确认门禁、运行期身份变化拒绝。无真实D、旧failed制品或原始真实参考访问；此前v1性能证据原样保留。

最终定向命令：`uv run --project backend --frozen pytest backend/tests/resources/test_resource_identity.py backend/tests/resources/test_resource_store.py -q`，绑定本任务独立PG，**12 passed，8.46秒**。已检查完整差异及 `git diff --check`；benchmark脚本只补显式fixture参数，未重跑性能测试。修复待独立复核，不自行宣布Standards GO或真实D准入。

## Resource多完成来源与独立国家参考（2026-09-13，人工接缝已实现，待独立复核）

本轮以已GO的 `6ca6517` 为保留基线，公共Reader `2fbd7b3`、decoder `2b60b14`、Reader资格复验 `828211e` 原样cherry-pick。只扩展Resource私有调用与参考模块，没有复制Reader分叉；真实D candidate、其他任务PG、旧failed制品与真实参考原件均未访问。参考来源事实仅依据0C `de0b419` 第6节：独立as_dict原件、五字段、country的NaN与字面“未知”；生成链和二月有效性仍Unknown。

### 已实现的消费边界

`produce_bound_resources` 接受同一专用catalog/DSN的显式 `SourceBinding(run_id,snapshot,purpose,context)` 列表。context保留source_id、collector、origin_uri、压缩SHA、首元素时点、文件名时间依据及路径渲染版本。每源必须属于对应complete run的baseline/snapshot，URI/SHA重新计算source_id核对；拒绝重复来源（含缓存别名）、倒序或同点、混合collector、错snapshot、candidate/failed、用途与窗口不符。读取前使用公共ObservationReader核验，完成前再次核验manifest、固定来源回执、CSV回执和独立国家参考身份。

Reader逐指定source下推查询并校验SourceEnd，Resource直接消费真实字段；保留来源全部Peer与前缀，不创建observations.duckdb、不扫描未选择UPDATE、不重解析MRT。源名义时间必须等于首个有元素记录的epoch，另存实际min/max与元素数量。跨catalog、恢复已有Resource工作态、伪造combined上游run均不支持。

`resource/v3.sources` 新增上游run/snapshot、purpose和decoder_version；完整bindings进入PG登记、dataset_id及回执。公共 `mrt-fields-peer-path-et/v1` 标明新字段规则；RIB ADDPATH反例写入decoding_differences，包含真实AS_PATH、path_id、旧固定列推导、复现器版本及历史工具Unknown。旧业务公式未改，不能声称与旧文本错位数值等价。

### 结果窗与预热

正式请求必须显式result_window。D为北京时间 `[2026-02-28 00:00,2026-03-01 00:00)`，UTC `[2026-02-27 16:00,2026-02-28 16:00)`。前9个RIB按UTC 02-24 16:00起每8小时至02-27 08:00列为warmup；D的02-27 16:00、02-28 00:00、08:00为result；右端02-28 16:00不入D。全部12点指标、成员、正常带/样本、状态依据及拓扑保存。

`scan_resource` 默认scope=result，只读result来源的行；路径字典也限定为result成员所引用路径。scope=all是显式审计入口。D正常带的样本引用可以指向预热时点，这是依据，不把这些样本算成D资源点。没有修改或接通页面，后续页面必须用默认结果范围。正常带available仍只表示已有上下界，不能据此断言当前有足够新样本。

前6个RIB的启动规则、>5历史正常样本、list_len只增、整点离群剔除及判定后清理保持旧公式。9份预热不保证每桶/Peer连续出现、正常或足够样本，不保证与旧连续状态相同。先D冷启动再补预热必须另建新Resource版本按12点重算，复用原完成观察；不倒序追加旧state、不覆盖原冷启动版本。

### 独立补充参考合同

私有[references.py](../../../backend/data_pipeline/analysis/resources/references.py)提供 `register_reference`、`read_reference`、`scan_reference_rows` 与 `read_reference_original`。初始a0实现曾以PG BYTEA和PG全行保存主体，因不符ADR-0002已按下节修复；当前原件保存在独立文件目录，完整规范历史在DuckLake，PG仅保存目录/状态/质量与版本引用。每次顶层键出现保存ordinal、原键、字节offset/length、原文entry、完整类型标记树及country投影。内部对象用有序pairs树保留所有字段、重复键和位置；数组/普通值/非标准常量分型。NaN存为 `kind=nonstandard_constant,token=NaN,value_state=non_finite`，不产生标准JSON中的float NaN，不改成null，不推断国家代码；原null仍是scalar/null。

全层重复键均保留，投影显式last-wins；顶层键与内部asn及字段集合差异记质量。country_cn字面“未知”原样保留，兼容相等分组，topology_edges/status标记country_scope=legacy_unknown；缺归属为unknown，其他来源标签为reference_label，并不证明历史有效性。旧五字段不因当前只查country_cn而裁剪，完整审计读取可恢复原始字节切片，整件读取重新验SHA。

生命周期为candidate→validated→complete；SHA/语法/回执失败不得complete，完成回执先fsync后登记，错误后的已提交状态独立确认。Resource必须绑定准确reference_id/dataset_id，candidate/错版/fixture默认不可消费；不往D观察run追加参考。CSV另绑定已complete的run/snapshot/source SHA及Reader所需anchor source，其原行去重/词法解释仍复用已GO adapter。context.reference_id为规范JSON摘要 `identity_digest({'csv':csv_binding,'country':read_reference(...)[1]})`。

### 正式入口与请求

继续使用私有 `scripts/pipeline/resource-frozen-run.py` 的只读快照、新解释器、依赖清单和来源验证；新增文件由现有resources/*.py与observations/*.py显式目录范围纳入身份。共享冻结外形不另做重构。正式Resource请求必须包含sources/result_window；旧contexts入口只保留显式fixture回归。新的参考登记也经该正式新进程入口，不能用热进程API替代。

执行形式仍为新项目锁定Linux环境的 `uv run --project <新项目>/backend --frozen python <新项目>/scripts/pipeline/resource-frozen-run.py <Git外请求JSON> --snapshot <只读归档>`。

参考请求字段：operation=register_reference、dsn、path、output、origin_uri、content_sha256、max_bytes；正式fixture_only=false。真实参考path和SHA待协调后续授权绑定，当前仅人工fixture。

计算请求字段：operation=resource、dsn、sources（每项run_id/snapshot/purpose/context）、result_window（两个含时区ISO字符串）、csv_binding（run_id/snapshot/source_id/anchor_source_id）、country_binding（reference_id/dataset_id）、output、topology_enabled（默认true）、fixture_only=false、max_rss_bytes、min_free_bytes。每个context的字段与前节一致。不会通过topology=false宣称全能力完成。

### 写范围、落盘与保护

当前只支持默认public元数据schema的同一个新迁移专用PG/catalog。上游登记/数据只读；新写resource_<uuid>十张lake表，以及domeye.resource_runs和五张resource_work表；独立参考新增resource_references元数据登记及reference_<uuid>.rows湖历史；a0曾创建的PG完整行表不再作为新版本主体。PG资源登记增量加binding_manifest，不改已有观察表、路径或生产服务。运行角色仍需这些私有表DDL/自身写权限及必要DuckLake catalog元数据写权限；模块schema不等于catalog元数据完全隔离。

Resource Parquet继承catalog根，位于自身schema/table路径；回执storage_layout现场查询并记录catalog_data_path、schema_path、是否相对及本地output。output只控制本地去重暂存和回执，不声称控制整个catalog。没有使用OVERRIDE_DATA_PATH。路径层次与[DuckLake路径说明](https://ducklake.select/docs/stable/duckdb/usage/paths)一致，并经本任务隔离PG/实际Parquet路径验证。

默认RSS保护2GiB、空闲盘保护1GiB可显式配置，不设处理总时长上限。每批/元素检查RSS，检查本地output与catalog数据根所在文件系统；Resource本地DuckDB512MiB/2线程，公共Reader沿用其1GiB DuckDB设置，批次默认512行/4MiB（单记录可能超批预算，仍受RSS检查）。这些是检查点保护，不能保证一次分配不越阈值，也不代替PG服务器自身内存/磁盘保护。参考原件默认最多64MiB（可显式调，最大1GiB），整件保留、逐顶层对象解析和1000行批写；未设总时间deadline。

a0人工双上游fixture曾临时固定DATA_PATH以绕开共享生产者每run输出路径耦合；该历史测试不能证明正式入口。此次已原样合入阶段1的catalog_data_path正式参数（见下节），并删除fixture替换，两个人工完成run通过该参数复用同一精确数据根，不使用OVERRIDE或复制D观察。

### 本轮验证与交付边界

公共合入对应：`2fbd7b3`→本分支`8443366`，`2b60b14`→`e33a4f1`，`828211e`→`5e58ba0`；Reader增量已由协调通知独立GO（报告`d6ccc56`），本轮Resource扩展仍待独立复核。

本任务独立PG/DuckLake中，参考正式fresh登记→两完成上游→12点正式fresh计算→固定版本默认3点读取完整通过；D中额外UPDATE未进入24条RIB决策。含RIB ADDPATH保存真实路径与旧列42差异、12份兼容“未知”拓扑、全部正常带/预热引用、真实Parquet路径、完整原字节恢复及NaN/null分型。负例覆盖错snapshot、candidate/failed、重复来源、倒序、混合collector、运行后上游漂移、参考错版/未完成/SHA失败以及默认fixture拒绝。此前warm-import新进程反例继续通过。普通公式及拓扑缺参考、无边、跳私有桥接、50000阈值及超限分支沿用人工公式测试，本轮不新增真实大样本性能运行。

命令：`uv run --project backend --frozen pytest backend/tests/resources/test_resource_bindings.py backend/tests/resources/test_resource_store.py backend/tests/resources/test_resource_identity.py backend/tests/resources/test_resource_computation.py backend/tests/resources/test_resource_adapter.py backend/tests/observations/test_observation_consumer.py -q`。两个测试DSN环境变量都只绑定本任务既有独立PG（没有接Feature PG）：**64 passed，15.24秒**。未调用外部bgpdump二进制，本轮以已冻结decoder规则和保存RIB人工接缝验证；不替代公共实际工具复现证据。

新Resource读取合同为v3，不声称当前读取器可直接读取旧v1/v2 schema；旧性能证据及已GO提交保留，可用对应旧冻结检查。源码/人工测试已完成，真实预热生产、D准入、Linux实际资源需求、角色授权、页面切换及生产部署均未验收。

最终补强了第12点计算结束后上游变failed的发布前拒绝，以及补充参考回执写失败时原件仍保留、状态failed的断言；仅重跑受影响 `test_resource_bindings.py`，**3 passed，5.74秒**。完整差异和 `git diff --check` 已审查。


## 补充参考分层归属修复（a0后增量，待独立复核）

依据[ADR-0002](../../adr/0002-layered-data-storage.md)修正a0的存储归属，不改变解析、last-wins或拓扑业务规则。保留a0历史冻结，不迁移/删除其已有制品；新参考合同为resource-reference/v2。

SHA核对后以独占创建方式写入本次output/original/as_dict.json，flush/fsync并设为只读，目录同步持久化；保存原origin_uri、输入cache_path、原件路径、字节数、SHA及output。解析、写湖或回执失败后文件仍保留，失败参考不能被公共读取器消费。

所有逐项ordinal、原键、字节坐标、raw_entry、完整有序pairs/全字段/NaN token树、质量与country投影写入reference_<uuid>.rows的DuckLake Parquet，强制关闭小行数内联；数据根沿已存在catalog精确值，只新增私有schema，不OVERRIDE。PG的resource_references仅存身份、状态、计数、质量、manifest和文件/湖快照引用；新运行不创建PG完整行表或写BYTEA。国家查值从固定已发布湖历史重建，不保留第二个独立PG投影主体。

dataset_id绑定文件定位/大小/SHA、解析规则、代码/扩展身份、湖schema/snapshot、规范行完整内容摘要及实际catalog/schema目录。发布前重新核验已持久化原件和精确快照的完整行摘要，随后validated→ready回执fsync/目录fsync→PGcomplete。read_reference、原件读取和全行audit scan均只接受同一complete reference_id/dataset_id，核验登记摘要、文件摘要、目录绑定及全部规范历史内容。行数相同而country投影变化也拒绝，不能只以count通过。审计流先完成全量内容核验再输出固定快照行；读取结束复验完成登记。旧a0参考须用其冻结读取，不能冒充新合同版本。

共享阶段1 `1d00d3bf733360455368b95273d72b05caf56769` 原样cherry-pick为 `834b4ee`。双上游人工准备已使用其正式catalog_data_path参数替代旧fixture workaround；仍只在本任务PG，不连接真实D，也不将共享增量尚待父验收的状态自行宣布GO。

定向验证覆盖原件文件实际恢复、Parquet实际存在、PG不含新BYTEA/完整行主体、9+3两完成run的正式fresh完整链路、参考回执失败保留原件。新增负例对真实Parquet同计数改country投影，并同步容器尺寸元数据以排除单纯页脚损坏，结果因规范内容摘要不符而拒绝；另外原件改写、catalog目录漂移及登记snapshot变化均被拒绝。所有故障注入限本任务人工制品，未修改任何真实参考或观察。


追加闭合独立复核的参考一致性/保护问题：Resource第12点工作态写出后，真实Parquet的country投影被同计数改值（同步容器尺寸，登记摘要不变），完成前重读原件与完整规范历史会拒绝并登记failed，不产生ready回执。没有PG缓存副本，因此不存在仅改PG country而沿用湖历史的第二权威。

有界输入SHA不符时仍保存实际完整原件到output/quarantine，分别记录expected_sha256、actual_sha256、大小、位置与capture_state=complete；它不是预期合格版本。大小超限只读取max_bytes+1用于发现超限，不突破保护继续读/归档，记录read_bytes及not_archived_size_limit、未确认EOF原因；RSS/磁盘保护阻止归档时记录源位置、当前capture_state和失败原因，不声称完整保留。

参考登记新增与Resource一致的默认RSS 2GiB、空闲盘1GiB保护，同时检查output与实际catalog数据根；规范写入默认1000行/4MiB字节批次，单大条完整独占批次并受RSS检查，超限失败不截断。max_bytes默认64MiB仍保留；全部参数进入参考manifest，无处理总时长限制。解析/序列化/Arrow分配周围和验证批次设检查点，不能把它宣称为每次分配的硬内存上限或PG服务器资源保证。

阶段1路径解耦已获父验收两轴GO（b143c7a报告）；本分支834b4ee是1d00d3b原样合入，真实活动D归档未动。

增量验证：仅本任务隔离PG的bindings与fresh入口相关回归先为7 passed/3 deselected（12.10秒）；加入错SHA隔离、大小/RSS/盘保护、单大条完整保存和第12点后同计数参考篡改后，最终 `uv run --project backend --frozen pytest backend/tests/resources/test_resource_bindings.py -q` 为 **4 passed，7.76秒**。未重复无关公式/性能分支。完整差异和diff-check通过；本增量仍待308e独立复核，未运行真实参考注册或真实Resource。

### M3 observation 消费接线（本分支已实现，待独立审查）

从集成 `52f0a1f` 新建隔离分支，新增明确的 M2 `observation_sealed` 输入与 Resource 独立资格版本。旧计算器、正常带和科学表保持；完整输入、实际 RIB/Peer/参考/样本依赖与资格另存。有限入口、16表枚举、原值/主值读取、旧逻辑保留和重算范围统一见 [Resource M3 公开消费合同](../Resource-M3公开消费合同.md)。未执行真实全天、发布或页面接入，不代表产品验收。
