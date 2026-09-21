# Feature 纯计算迁入与适配接缝

状态：纯计算、普通／IR 私有过滤投影 adapter 与人工 Arrow 多文件集成已实现；真实观察读取、DuckLake／PostgreSQL 保存和产品接入尚未完成。

实施冻结基线为 `d105d304ab9207426c72d0c5dc811077dc9e613a`，独立分支 `codex/feature-computation-migration`。旧源码参考仅为 `39578fe` 的 `BGPFeature.py`、`BGPFeature_ir.py`、`BGPRib.py`、`BGPInfo.py` 和国家查询 helper；未导入、执行旧项目或读取其环境、pickle、数据库及真实初态。主映射继续以[旧新映射 M04/M05](../旧新逻辑状态数据映射.md#M04--M05-Feature按文件累计资源快照与稀疏写入)和[M04](M04-BGPFeature.md)、[M05](M05-BGPFeature_ir.md)为权威，本页记录此工作包实际实现范围。

## 已实现的接口

实现为 `backend/data_pipeline/analysis/features/calculation.py`：

- `initialize(mode, projection, reference, execution_id)`：从显式基线 `as_prefix` 建立已知国家／ASN 工作态，资源初值均为 0、dirty 为 False；不产生历史行。
- `calculate_file(state, reference, window, observations, final_projection, ...)`：校验过滤决定，累计文件元素，按末态资源集合计算，返回结果行、稀疏名单、诊断、清零前和清零后状态。函数不改变调用方状态，不写文件或数据库。
- `WorkState.export()`：导出可 JSON 编码的业务态、投影与执行进度；没有加载或恢复入口。

普通和 IR 的 `rule_id` 分别为 `legacy-feature-39578fe/v1`、`legacy-feature-ir-39578fe/v1`，不能互换。`Projection.version` 与窗口 `input_version`、参考版本、执行身份分别保留。结果携带参数、单位、前后投影版本；这些字段尚未构成已准入的公共存储合同或业务发布 ID。

`Observation` 接收来源定位／来源内序号／真实时间、flag、原 prefix、旧 VP、A 新路径、该兼容投影 W 旧路径、前后 legacy origin 集合、投影规则和跳过原因。路径保留旧渲染文本，不用规范 `attributed_origin` 替换。前后 origin 集合为适配取证信息，Feature 计数本身只使用新 A／旧 W 路径；本模块不利用它们推进或纠正规范 RouteState。

`Projection` 保存 `prefix_dict`、`prefix_as`、`as_prefix`、曾见 VP、RIB 来源与原时点、country_filter、覆盖状态。普通模式要求 country_filter=None，IR 要求 IR。直接调用纯计算仍可以使用构造投影；新增 `feature_projection.FeatureAdapter` 则从固定观察行建立投影，并在人工 fixture 验证 RIB 内容筛选和更新。

## 后续适配责任与反例

已按序 cherry-pick 公共 `d105d30..ecbc03f` 六个提交，保留原纯计算 `7ea0fe1`；本分支公共接入终点为 `bcdad28`。本次没有修改公共 observations 代码。Feature adapter 只复用 `observations.state.legacy_origin`，不调用规范 `Replay.consume`；MRT 解码仍在公共阶段一次完成。公共固定渲染不保留原 bgpdump 文本空白，原字节由同版 path_key 保留；不能宣称逐字符复现旧文本。

新接口为 `FeatureAdapter(mode, reference, FeaturePlan, execution_id).consume_source(SourceBinding, Arrow批次迭代器)`。计划固定观察版本、Collector、唯一基线和有序 UPDATE，绑定每源 source_id、content_sha256、窗口、元素回执数量、消息质量引用与状态。适配器校验来源、内容、role/action、来源内 record/ordinal 次序、时间和最终数量；错误不会提交状态或游标。计数器以生成器接收过滤后的前后态信息，末态通过一次回调冻结，不缓存全文件计算输入。

当前公共 `scan_observations` 按 source_id 字典序，并不按 manifest 次序，且只返回元素，缺少 STATE／EOR 零元素消息及接收方向字段。经协调确认，本轮仅使用显式 source→Arrow 流，按计划消费，拒绝错源／倒序；公共按有序 source_ids 的单次 SQL 读取增量另行安排，不改活动真实 D 的冻结。当前没有假装已打通数据库读取，也不全量缓存重排或多次解析 MRT。

STATE 不是 Feature 元素，但它所属消息的 scope 质量不可丢弃。`message_quality_refs` 预留同版消息／EOR／质量记录引用；没有明确消息质量时结果保持 Unknown，即使元素文件声明 complete。fixture 的完整引用为人工构造，不代表真实质量已验证；尚无真实接收方向筛选、零元素消息完整性或连续 Session 验收。

必须按过滤前后顺序对照下列案例：

1. IR 基线 P→ASN1（IR）；下一文件 A P→ASN2（非 IR）被跳过，IR 旧 ASN1 路径继续保留。再下一窗口 W 使用 ASN1 计数，不能使用规范或普通投影的 ASN2。
2. 普通 A ASN1→ASN2 只标新 ASN2 dirty；BGPRib `prefix_as/as_prefix` 可能残留 ASN1。ASN1 本文件无行，而国家资源仍可含该 prefix。
3. 未知 W 的 collect 加 1，找不到该 VP 旧路径时无 ASN 计数；另一个 VP 路径不能替代旧路径。
4. `/7`、IPv6 `/15`、非法 prefix 和 STATE 在 Feature 投影推进前排除；不能先推进通用状态再仅在输出端过滤。RIB 初始化过滤与 UPDATE Feature 过滤并不相同。
5. IR A 先使用 Feature 起源规则限定 IR，随后 BGPRib.update_rib 还按自身起源检查 IR；IR RIB 同样用 BGPRib 起源筛选。例：`2 4200000000` 的 Feature 起源为 4200000000（参考为 IR），BGPRib 起源为 2（参考为 US）：报文计数加一但不更新投影，旧路径保留。`skip` 与 `projection_skip=non_ir_bgprib_origin` 分别保存，不能合并为“整条跳过”。

上述反例现已在 Arrow 观察→私有投影→计算链上验证。`projection_audit` 逐元素记录固定观察/参考版本、来源、event_id、path_key、前路径、前后起源和两层过滤原因；完整原字节仍引用公共观察而不复制。基线审计位于 adapter.last_audit，UPDATE 审计也随 Result 保留。

三组路由索引采用 `FrozenIndex` 的 128 个不可变桶；每文件 `IndexEdit` 仅复制被写入的桶。ASN prefix 成员也采用分片集合，避免给一个大 ASN 每次更新复制整个成员集合。已冻结编辑器不可再次写入，未变的桶／前缀路径对象跨版本共享；`deepcopy` 对只读索引返回自身，旧版本不会混读新时点。改变前缀的 VP 路径小表仍会复制，Feature 五数值状态仍防御性复制，资源汇总仍全量扫描并生成所需结果集合。此实现避免全 RIB 路径深复制，不宣称全部计算增量化或已证明全天性能。

## 字段、状态和旧规则覆盖

| 旧项 | 此实现位置与行为 |
| --- | --- |
| `feature_dict[country][asn]` 五数值与 `is_change` | `AsnValues`；所有已知 ASN 保留，未 dirty 的资源数值可能仍是初始化值或旧值；不能称该数值为本时点完整观测 |
| `feature_collect_dict` 五数值 | `Values`；A/W 按元素累计，结束后保留资源、清零计数 |
| ASN、国家、collect 原 prefix 与资源分母 | 每行 `raw_prefixes/ipv4_prefixes/ipv6_prefixes`；原投影完整 `as_prefix` 另保留，含被过滤的原成员和残留 |
| IPv4 `/24` 等效并集与 ×256 | 复用新项目 `prefix_quantity.calculate_c_segments_count`；`/25` 覆盖一个 `/24` 块，不是去重 CIDR 数，也不是严格地址并集 |
| IPv6 `/48` 等效并集 | 复用新项目区间算法；`/49` 覆盖一个 `/48` 块 |
| 原起源规则 | Feature 从后向前跳 64512—65535、>4294967295；32 位私用范围未排除；末 token 含 `{` 无 Feature ASN；原始文本保持 |
| A、W、dirty | A 新 path，W 投影旧 VP path；重复 A 也计数；只 dirty 的 ASN 重算资源并生成中间行 |
| 行存在性 | 普通 dirty ASN 为 `written`；其他记入 `sparse_asns`，不生成零行、不宣称路径无变化；国家与 collect 每文件生成 |
| IR 写入差异 | dirty ASN 中间行标 `ir_asn_write_disabled`，legacy_table=None；仍计算状态、汇总国家并清零，不能将其当旧版已持久化 ASN 行 |
| 原时间／source／月表 | `FileWindow` 明确实际窗口和文件时点，旧 t 转 Asia/Shanghai 无时区秒文本；默认 source=`r`；BIG_COUNTRY 显式映射到 `feature_XX_YYYYMM`，其余 other；支持关闭月表与显式表名 |
| 大小写／空值 | Python 原五字段大小写保留；旧 SQL 未引用列名实际折为小写，后续存储 adapter 必须另映射；未知 ASN 行 country 保留单空格，国家行保留“未知” |
| 参考重复、空值、归属 | `Reference.from_rows` 保留全部有定位的输入行，按 ASN 首行生效（含空值），来源参考版本不丢；下划线 ASN 查询前段。输入 ASN 为上游已固定渲染文本，本包不声称重现 pandas 类型推断 |
| 跳过／警告 | 返回 update／aggregate scope、原 prefix、规范 prefix、原因、标识。诊断阈值 `/24 >= 2^24`、地址 `>=2^32` 或 `>=10^9`，前缀样本最多 10；不是业务异常事件 |
| 质量与观测零 | 文件覆盖和末态覆盖任一 Unknown 则结果 Unknown；partial 保留 partial。只有完整声明下空资源才标 observed_zero；稀疏 ASN 无此推论 |
| 窗口状态 | `end_state` 为逻辑结束、清零前状态；`next_state` 为资源保留、A/W 清零、dirty 清 False 后状态及完成游标；中间计算无外部可见写入 |

`get_feature_prefix_skip_reason` 的人工排除集合虽在旧源码声明，但实际 Feature 调用未传 prefix；本实现保留这一实际行为，`120.0.0.0/11` 与 `173.0.0.0/11` 不被人工排除。不能将此历史差异当成规范过滤政策。

## checkpoint 与执行信息的去向

| 旧字段 | 保存位置／实施边界 |
| --- | --- |
| `bgp_rib` 的五组核心状态、t、rib_file、country_filter | `WorkState.projection`；树为可重建缓存，不新增事实 |
| feature_dict、feature_collect_dict | 同名类型化工作态 |
| 当前 t、last_completed_update_file | 工作态独立字段；文件边界、完成数量、执行 ID、参考版本另外保存 |
| meta version/status/created_at/snapshot_path/last_completed_update_file/rib_file/rib_time/snapshot_format/feature_name | `LegacyCheckpointMetadata` 可选容器；只提供保存位置，不打开路径、不从元数据推导成功或恢复 |
| `_ensured_feature_asn_tables`、连接、logger | 未迁入计算态；数据库表缓存可重建，连接和日志配置由后续运行层负责 |
| current/previous、pickle/gzip、进度 txt、监听队列 | 保留来源说明，首轮没有 checkpoint 加载、恢复、outbox、监听或自动清理 |

普通与 IR 使用独立工作态，避免沿用旧共用进度文件的相互干扰。旧 feature_asn／feature_country 的 bigint、180 天保留策略是持久化背景；本包无建表和删表行为。`database/feature.py` 的 int／60 天为历史未接入实现，仅保留[字段来源](持久化字段.md#feature)，不启用。

## 显式错误及已知限制

旧路径异常可能在原版逐行吞错继续，旧数据库层也可能吞写入错误。本实现对无效路径 token、过滤不一致、身份/窗口冲突抛 `FeatureCalculationError(quality='failed')`，原传入工作态不变；后续 runner 必须把异常作为失败回执，不发布成功。这是按本次授权改正错误不可见的运行行为，不宣称复刻旧吞错后的数值结果。

IR／普通初始化、跨 VP 覆盖、A 替换残留及 W 移除已在私有 adapter 的人工观察上验证；真实公共读取接缝、PG 工作态、DuckLake 历史、回执与发布均尚未连接。投影版本绑定前版、观察版本、来源身份与内容、Collector、规则、参考、源窗口和限制；执行身份独立。真实历史初态、二月参考有效性、同版旧运行结果仍为 Unknown。旧消息质量缺口和输入窗口缺口不能被一次空文件覆盖为观测零。

## 验证与交付边界

命令：`uv run --project backend pytest backend/tests/features/test_feature_projection.py backend/tests/features/test_feature_calculation.py backend/tests/common/test_prefix_quantity.py backend/tests/observations/test_observation_state.py backend/tests/observations/test_observation_mrt.py -q`。首个输入衔接缺口修复后结果 **67 passed**，没有启动 PG／DuckLake 测试。

当前 fixture 覆盖初始化→两文件→下一窗口、稀疏行、清零前后态、替换残留、未知 W、跨 VP／跨 ASN 重叠集合、`/25`／`/49` 独立预期、私有/AS_SET/未知、过宽/非法/STATE、IR→非 IR 残留后 W、已知国家逐文件输出、月份/时区/秒精度、参考重复/空值/下划线和警告阈值。原集合、数值和状态均单独断言。

新增检查覆盖 IR 两层起源筛选、基线与 UPDATE 过滤差异、同内容不同来源分别计数、源计划不依赖字典序、错源/错摘要/倒序/重复/越窗/错计数/路径异常不提交、旧版本对象共享且不可变、零元素源的消息质量引用与缺口、质量证据改变产生新投影版本、基线实际记录不得晚于首个 UPDATE 切点。一个人工二进制 fixture 用新公共解析器每源解码一次后生成公共字段形状的 Arrow 行，随后普通／IR 两个 adapter 共用行；测试禁止再次调用 MRT 读取函数。这是内存接口集成，不是实际数据库 scan 的验收。

## 人工多文件规模证据

可复跑命令：`uv run --project backend python scripts/benchmarks/benchmark-feature-projection.py --prefixes 16384 --files 8 --updates 8`。输入为 16,384 个 `/24`、64 个 ASN；8 个更新文件各 8 次 A。输出每文件时长、实际复制索引项、实际复制的变更前缀 VP 成员数及 Python 分配峰值，并断言原基线全部路径／成员仍可读取、未变路径对象共享、每窗资源和计数一致。

本机本轮测得：基线约 0.67 秒；8 个文件各约 0.293—0.298 秒；每文件复制索引项 1,572—2,136，变更前缀 VP 成员 8，全路由快照深复制 0；Python tracemalloc 峰值 58,338,298 字节（约 55.6 MiB）。这是一次含结果分配的人工测量，tracemalloc 不是进程 RSS；不推算真实 MRT、PG／DuckLake、全天或更多 Peer 的吞吐。较小 4,096 前缀／4 文件也已运行对照，非生产性能验收。

每源审计列表仍按观察数增长，结果集合也占用内存；真实运行还需有界审计批写及运行资源保护，不能由消除全 RIB 深复制推断全部内存问题已经解决。本次可交付私有 adapter、纯计算、fixture 与规模脚本供独立复核；不代表真实 DuckLake／PG 集成、一天数据、六类异常、阶段 2 整体或页面已完成。后续由协调安排公共只读 API 增量、存储集成和独立复核。

## 独立复核 P2 的最小修复

原冻结 `a747548` 的独立报告 `8c1969c38dbcd105a9e0c3a19ccd3e133a0a2d6b` 判为 REPAIR：基线初始化未设置 `last_window_end`，导致只检查后续 UPDATE 间缺口，遗漏基线至首 UPDATE 的缺口。原冻结保留，新修复将私有投影规则升为 `feature-private-projection/v2`。

首 UPDATE 使用已绑定基线来源窗口的右端作为输入衔接检查点；后续使用前一 UPDATE 的窗口右端。若下一窗口左端更晚，保存类型化 `InputGap(left_source,right_source,start,end,basis)`，完整声明下降为 partial，原 Unknown 保持 Unknown。缺口随投影版本、清零前后工作态及结果参数传递，后续空文件不能洗成 complete 或 observed_zero；旧冻结视图不被修改。

这里核对的是声明输入范围，`basis=declared_input_bounds_not_session_continuity`，不把 RIB 快照实际时点或它的文件范围解释成连续 Session。现有基线实际记录不能晚于首 UPDATE 切点、实际时间必须落在来源窗口内的检查继续有效。

新增 8 个定向组合：普通／IR × 首文件无缺口／缺十分钟 × 基线元素位于窗口首秒／末秒。均使用非空合法默认路由基线，被兼容规则过滤后投影为空；核对首文件和后续空文件的质量、观测零标志、缺口两端来源和时间、状态导出与原基线不可变性。没有将合法基线改判失败，也没有添加恢复或存储能力。此修复尚待协调安排增量独立复核。

## 存储与冻结执行增量（已实现，仅人工输入验证）

`feature_run.run_fixture` 是显式人工库入口：单个完成观察 run:snapshot、有序源及窗口、已保存参考 SHA 与原 CSV 均须匹配。普通/IR 各自读取固定观察流、推进独立私有投影；不重解析 MRT，不调用规范 Replay，不写规范 RouteState。解码字段/方向与差异证据复用[共享读取说明](../观察只读消费接口.md)。SOURCE 的消息数、元素数分别核对后才允许提交 Feature 源回执，LOCAL、STATE、EOR 和质量记录数另存。消息质量仍可为 Unknown，不因处理成功升级完整性。

参考解释使用原 `pd.read_csv(keep_default_na=False, usecols=...)` 整列推断、ASN 保留首行后转字符串；不使用 dtype=str，也不执行旧代码或来源表达式。原 CSV 每行的词法、字节偏移/长度、SHA、空白行和物理行范围均与保存观察核对。仅解释 Feature 实际消费的国家名/代码；其他参考字段保留原词法，不执行旧 BGPInfo 的表达式求值。当前只支持明确绑定的 v2 CSV，XLSX/多 sheet 不在本入口范围。

私有存储分为：

| 表 | 内容 |
|---|---|
| `windows` | 文件时间/半开窗口、普通或 IR、五个数值、资源状态、旧目标表、`written` / `ir_asn_write_disabled` |
| `projection_revisions` | 每个元素前后路径及存在性、前后起源集合、过滤原因、VP、版本、方向；不保存整份 RIB 副本 |
| `resource_members` | 每个输出范围的原始前缀、归一化前缀及跳过原因，支持量值成员解释 |
| `state_deltas` | baseline / window_end / next_window 的受影响 ASN 与 collect 数值、dirty 位、前后投影版本；明确保存清零前后两阶段 |
| `source_receipts` | 来源独立计数、窗口数、输入缺口、质量、参考版本及上游绑定 |
| `reference_rows` | 原参考词法和字节引用、实际 pandas ASN 类型/键、首行选择及国家解释 |
| `decoding_differences` | 独立解码差异记录；原观察身份、固定复现推导与新值，不伪称历史原始 stdout |

每个 Feature run 使用独立 DuckLake metadata schema 和 Parquet 路径，PG 私有 `feature` schema 保存候选登记、类型化当前路径/起源/seen VP/业务状态与源提交。每批历史落盘后更新私有工作态，失败 run 不开放；这不是跨存储原子事务、恢复或 outbox。完成前核对当前 PG 状态和计算状态、必要历史计数、双模式全部源提交；先 fsync ready execution 回执及目录，再将 PG 登记设为 complete。只读查询必须绑定完成 run 与精确 snapshot。

审计和资源成员使用同步 sink，按行数及估计字节限制批写；单条大记录独占批次且不截断，入口默认峰值 RSS 保护为 1 GiB。源回执不再调用整份投影 export。纯函数 API 仍可为小 fixture 保留输出；存储入口关闭输出留存。`reconstruct_phases` 是小规模审计助手，会物化业务状态历史，不是生产恢复入口，也没有声称其空间开销适用于无界历史。

代码身份：`scripts/pipeline/feature-frozen-run.py` 已接入[有限冻结执行接口](../有限冻结执行接口.md)。`feature_identity.py` 显式绑定全部 Feature 实现、参考解释、共享观察/解码/身份、数值 helper、入口及依赖锁；源码快照在业务导入前冻结，新解释器实际模块来源在执行开始及提交前复验。正式 `produce_features` 默认拒绝直接 API；`run_fixture` 明确标记 `synthetic-fixture-api`，`read_table` 默认拒绝此类结果，人工测试必须显式 allow_fixture。回执包含执行模式、文件/包版本、PID、快照摘要及实际模块来源。冻结入口已在人工 PG 真子进程链路验证，但未授权使用真实 D，也未启用恢复、生产部署、前端消费或修改共享服务。

### 人工验证与性能边界

隔离 PG/DuckLake 全链路覆盖：普通/IR 同输入、原 pandas 参考推断、清零前后 typed 状态重建、错误快照拒绝、ready/必要审计/下一窗口状态 sink 失败拒绝完成、同库多 run 不改旧 catalog、大行独占批、LOCAL_ADDPATH 与 ET 修正计算及差异历史。额外参考 fixture 覆盖数字前导零、浮点 ASN、混合下划线/空值、重复首行、带逗号/换行及空白行。共享官方工具证据见共享读取说明。

可复现脚本 `scripts/benchmarks/benchmark-feature-storage.py` 仅从显式 `DOMEYE_FEATURE_TEST_DSN` 新建人工数据库，生成合成 MRT/CSV。以下为接入冻结入口前的人工存储增量实测；相同本机环境，3 VP、8 UPDATE 文件，每文件至多 32 个变化：

| 前缀数 | 上游生产秒 | 双 Feature 计算及持久化秒 | 窗口查询秒 | 进程峰值 RSS MiB | 投影修订 | 资源成员 |
|---|---:|---:|---:|---:|---:|---:|
| 1024 | 0.392 | 1.633 | 0.052 | 339.1 | 6656 | 49782 |
| 16384 | 2.596 | 16.496 | 0.051 | 876.6 | 98816 | 787062 |

两档均输出 60 个窗口、18 个源回执，报告的整路由深拷贝为 0；RSS 是进程高水位，含上游与库开销，不是 Python tracemalloc。较大档接近 1 GiB 保护线，不据此推断真实归档规模可用或承诺线性扩展。原始 benchmark.json、execution.json 与人工库位于本任务临时输出目录，均不进 Git。

### 独立审查后的三项最小修复

历史 seen VP 现在从批内全部通过私有投影过滤的 RIB/A 修订独立累加，路径当前态仍可压缩为批内末修订。同批新 VP 的 A→W 不再丢失历史曾见事实；批大小 1、2、100 的正式 CLI 人工结果相同，末源为空也正常完成。纯计算的 seen 规则、过滤与 IR 行为未变。

请求 collector 必须等于固定观察 manifest 的 collector；源顺序、内容 SHA、role、元素数沿用原有绑定检查。错误 collector 在创建私有输出前拒绝，不能仅通过输出标签冒充另一个观察点。

所有私有源提交及历史输出检查之后、写 ready 回执之前，复验本次实际使用的固定 run/snapshot/manifest、全部选中来源 validated 资格及原消息/元素回执，以及参考 SHA 对应的 validated 资格和开始时绑定的行数。参考预期行数写入规格。代码身份复验和输入资格复验分别执行；任一失败均不进入 complete。这是明确提交点的检查，不宣称跨库并发原子性或恢复能力。

修复测试在最后 IR source_commit 的人工数据库触发器中撤销 run、参考或来源资格，或改变参考/来源计数，证明 SourceEnd 之后发生的失效也会拒绝完成、没有 ready 回执且默认不可读取。反例与修复后正式 CLI stdout/stderr、请求和人工制品分别保存在任务 Git 外的 repair-before / repair-after 目录。

## 多个完成观察视图的有限接缝（已实现，仅人工验证）

`feature_inputs.py` 提供 `SourceView`、`ReferenceView` 和 `FeatureInputs`。当前仅允许一个显式 DSN／专用 catalog；复用公共 ObservationReader，不重解析 MRT、不调用规范 Replay、不导入旧 Feature 工作态。上游同 catalog 新 run 的人工准备使用已 GO 的 `catalog_data_path` 增量，既有 D 运行未改动。

SourceView 明确绑定 run_id、snapshot、source_id、origin_uri、content_sha256、原 source_role、私有 calculation_role、固定 reference_sha256、消息/元素预期数、原 run:snapshot 的 FileWindow 和消息质量声明。URI/SHA/role/collector 均核对保存 manifest，source_id 按已冻结原始身份公式核对；不能只改标签。只允许首位唯一 `initial_rib`，其原角色可为 baseline 或 snapshot；其余只能 calculation_role=update 且原角色必须 update。RIB 元素仍是 rib_snapshot，不能改作 A。

同 source_id 的重复声明只消费第一次选中的视图，全部别名的原 run/snapshot/role/资格仍进入绑定和最终复验。别名必须有相同 URI、SHA、计算用途、时间、参考和双计数；允许上游原 RIB 角色在 baseline/snapshot 间不同，不允许用不同时间窗解释同一源。不同 URI 即使字节 SHA 相同也不是同一来源，不按内容 SHA 合并。保留别名并不声称各 run 的规范计算投影等价。

`feature-fixed-views/v1` 身份绑定有序来源与别名、参考视图和交付区间；多 run 输出的旧单一 observation_run/snapshot 字段为 null，不能伪装成一个 run。source_receipts 增加类型化 upstream_run_id、upstream_snapshot、source_role、calculation_role、origin_uri、content_sha256。投影和业务状态仍按原单一有序私有状态链推进，D 边界不重新初始化。

ReferenceView 可单独引用一个完成 run/snapshot 的原件 SHA、原路径与回执行数，所有 SourceView 要求同一参考 SHA；跨参考解释冲突直接拒绝。全局 as_entity 参考允许来自不同 collector 的独立完成载体，其原 collector 记录为 reference_spec.carrier_collector 并纳入多视图身份，不改写载体 manifest，也不推断参考有效期。实际计算 RIB/UPDATE 的业务 collector 仍必须一致。无需每个观察 run 复制参考。读取复用公共 Reader；参考视图若独立于全部计算来源，公共 Reader 使用该参考 run 的原 baseline 作为读取资格锚点，该依赖显式保存在 `reader_qualification_sources`，不将其路由内容计算进 Feature。参考有效期及旧持续状态仍为 Unknown。

### 计算窗口与交付窗口

显式 result_window、comparison_window 均为带时区半开区间，比较窗必须紧邻结果窗之前。每个 UPDATE 来源必须完整归入 result、comparison 或更早 warmup；跨边界或结果窗之后的来源拒绝，不隐式裁掉元素。初始 RIB 标为 initial。实际消息时间仍由原适配器核对来源窗口；相邻来源的缺口沿既有 InputGap 规则记录，不由多个 run 的 complete 标志推断连续会话。

windows、projection_revisions、resource_members、state_deltas、source_receipts、decoding_differences 增加 window_role。`read_table(..., window_role='comparison')` 与 `window_role='result'` 分别读取 P 对照与 D 结果；默认不筛选，保留全部 P／warmup 历史及清零前后中间态。参考行不支持该筛选。旧输出默认读取方式保留，不为缺少该字段的旧制品猜测 P/D 用途。

规格中的 calculation_window 是实际 UPDATE 计算跨度，initial_rib_time 单列初始时点；交付 result_window、comparison_window 分别保存，prior_persistent_state=Unknown。P 起点RIB→P288 UPDATE→D288 UPDATE 的新状态链有显式初态依据，但不是旧持续历史逐值等价；普通 ASN 稀疏和 IR ASN 禁写语义仍未改变。该能力没有开启真实预热或网页消费。

### 正式入口与兼容

原 `produce_features(reader, plan, reference_sha, reference_path, output)` 和原单 run CLI 请求保留，原角色限制及 run:snapshot 版本不变。新 API 为 `produce_bound_features(FeatureInputs(...), output)`；同样要求冻结新进程，人工显式 fixture_only。

多视图 CLI 请求使用 source_views、reference_view、collector、dsn、output 和可选 result_window/comparison_window；不得混用 sources、observation_run、observation_snapshot、reference_sha/reference_path 等旧字段。每个 source_views 的 window 若省略 input_version/file_ref，分别由该项原 run:snapshot/source_id 得到；必须提供带时区时间与明确消息/元素计数。reference_view 必须提供 expected_rows，未知值不可启动。其他资源参数沿用原入口。

所有实际来源视图、别名和参考读取锚点在开始时与 ready 之前复验原完整资格；跨 run 最后一个 IR 源提交后撤销早先 run、参考或源回执也不能 complete。冻结身份清单的 feature_*.py 自动包含新绑定模块，实际加载来源仍受原机制核验。没有新增恢复、通用调度或跨库写入平台。

人工覆盖同 catalog 两个 complete run、原 snapshot 作初态、P→D 不重置美国路径归属、IR 过滤与禁写、同批新 VP A→W、空源/稀疏、跨视图缺口、错 collector/role/URI/snapshot/参考、最终资格失效。额外覆盖跨 run 同源别名只消费一次、第三完成视图独立参考复用，以及真实冻结子进程多视图 CLI 默认读回。

独立复核后的参考载体修复保留正式 rrc25 第三视图测试，并以局部人工 validator 构造 rrc00 参考载体，验证业务仍为 rrc25、参考原 collector/身份保留、锚点不进入计算、末 IR 提交后参考资格撤销仍失败。正式 Stage1 schema 仍仅支持 rrc25，未修改公共 schema/producer，也没有发生或宣称真实跨 collector 生产。

## 模块诊断历史（已实现，仅人工验证）

新增类型化 Parquet 表 `module_diagnostics`，数据集版本 `feature-module-diagnostics/v1` 写入运行规格。普通与 IR 每个 UPDATE 的 `Result.diagnostics` 均在来源完成前完整保存，保留原生成顺序 sequence、scope、identifier、reason、raw_prefix、normalized_prefix 和有序 sample_prefixes。样本仍由原算法排序后最多取 10 条；保存层不重排、不截断，也不改变计算或稀疏行规则。

诊断同时绑定 mode、source_id/source_rank、来源窗口与 file_time、window_role、输入版本、参考版本、计算规则和投影版本。原来源 run/snapshot/URI/SHA 通过同一固定运行规格的 source_bindings 与 source_receipts 关联。三种 IPv4 数量警告额外保存类型化 metric、threshold、comparison、value、unit：触发值来自同次计算输出行，阈值保持原规则的 2^24、2^32 与 10^9；跳过诊断这些字段为 null，不编造数值。原 Diagnostic 不含的原因文本和其他字段未被假定存在。

来源回执新增 diagnostics 数量和 diagnostics_state；UPDATE 的 saved 且数量为 0 表示本次计算已保存且无诊断，初始 RIB 为 not_applicable（原算法在初始化不产生 Result）。批写与字节保护沿用现有 sink。每个来源落盘计数必须与 Result 的诊断数量一致，最终总数还需与来源预期和全表输出计数一致，才能进入 ready；原代码身份与最终输入资格复验仍在 complete 之前执行。

`read_table(..., 'module_diagnostics')` 只读指定完成 run 的固定 snapshot，支持窗口用途筛选。缺少该数据集版本的旧运行明确拒绝诊断读取并返回 `not_saved`，不把未保存解释成零诊断；其既有表仍沿原固定快照读取。此增量不回填或修改旧制品。

人工验证覆盖 64 个与 256 个 /8 前缀形成的原始 IPv4 警告、RIB /7 聚合跳过与 UPDATE /0 跳过诊断，使用保存观察驱动纯适配器作为逐字段预期，与正式冻结新进程普通/IR 的 Parquet 回读核对；同时验证样本顺序/上限、批大小 1/512 等价、后续运行不改变旧快照、新版无诊断与旧规格 not_saved 区别，以及故意丢失诊断阻止 ready/complete。全 IPv4 的既有 /24 枚举计算会超过默认 1 GiB，相关人工测试显式使用 2 GiB 上限，默认保护和算法未改；不据此宣称真实 D 规模可运行。


M3 新阶段的显式 observation 输入、有序 Gap、独立资格与 raw/主值读取见[Feature有序缺口与独立资格](../Feature有序缺口与独立资格.md)。该新增 profile 的实施状态与验证边界以该文档为准，旧制品及旧 Q1 读取合同不因此升级。
