# M3B 独立 canonical 投影

状态：已实现；独立复核提出的正文一致性与未知规则两项 P2 已修复并做人工回归，待增量复核及集成接受；不代表业务可用或生产验收。

输入使用既有 M2 `ObservationReader(profile='observation')` 与唯一 `ordered(reader)`。canonical 实际选择为原 baseline 加声明 UPDATE 子序；完整 InputBinding 保留所有 snapshot 的原 rank，未选 snapshot 继续供 Resource 独立读取，不在本片充当中途恢复证据。参考通过 M2 固定入口读取并绑定原 checkpoint；不复制八张观察表。

## 执行与固定输入

显式离线入口为 `observations/projection.py:produce_projection(reader, output, ...)`。输出目录必须新建，PG 登记独立 run、`canonical_projection/v1` profile、唯一 `m3_<run_id>` schema、计划、状态与封印；正文写同一 DuckLake catalog 的独立表。旧 M2 表、文件、封印及旧 `Store.finish` 门禁不变。

计划保留完整输入 manifest、InputBinding、实际选择、参考 checkpoint/摘要、代码身份、Replay/映射/ordered/Gap 规则及资源限制。映射或参考读取失败发生在 M3 登记之前，不能描述为已启动 M3；登记后的 `not_run → running → complete` 必须经过落盘、来源引用与计数、全表摘要及输入资格校验。失败转为 failed 并尽可能保留 `failure.json`；PG 不可用等情况仍以未完成处理，不以目录存在推断成功。此实现不提供断点恢复。

完成前除计数和摘要外，还必须从固定 M2 的正规 Reader 使用原 mapping、ordered、canonical Replay 逐行生成预期正文，逐表核对 typed 字段、引用、对象身份和全部值。仅正文、外层引用或声明计划彼此相符仍不足以通过。验证不重新解析 MRT，不写另一套状态表；生产态在验证前释放。

完成封印包含实际 PG system identifier、数据库 oid/name、catalog/backend/data_root、控制目录实体（device/inode）、精确 DuckLake snapshot、13 表行数/摘要及 Parquet 路径/大小/SHA。控制回执 `execution.json` 的文件 SHA 另存 PG；Reader 同时验证两者。输出写连接关闭之后才登记 complete。运行配置、控制回执和数据文件均留在 Git 外。

## 显式 calculation_window（本切片已实现，待独立复核）

`produce_projection(reader, output, calculation_window={"window_start": "2026-02-26T16:00:00Z", "window_end_exclusive": "2026-02-28T16:00:00Z"}, ...)` 可显式约束计算输入资格。两个端点使用规范 UTC 秒级文本，固定半开 `[start,end)`，必须非空且位于原 M2 名义包络内。示例仅说明参数，不证明相应真实来源已通过。

传入时，新 plan 保存完整 `calculation_window` 和固定规则 `canonical-selected-update-window/v1`，并进入原计划/封印的生产身份；原 `input_manifest`、`input_binding`、来源身份和完整 MRT rank 一律保留。`projection_metadata` 同时保存该窗口/规则，`plan_version` 组合原 ReplayPlan 版本、窗口和规则。未传或传 `None` 时完全走旧路径：不新增上述字段，Coverage 仍使用原 manifest 窗，旧计划/绑定/制品不补字段、不改签。这里没有新增真实 execution_profile。

本窗是严格的有限输入资格，不是输出筛选器：

- 完整唯一 baseline 仍按既有角色建立初态，所有原记录和顺序保留，不按 Header 截断；Header 晚于 start 也不因本窗被拒绝。保留 `cutover_assumed` 与 `session_continuity_unknown`，不把 dump Header 当成精确 start 快照或已证切换时点。既有 M2 自身规则不由本窗放宽。
- 每条选中 UPDATE 来源的 `MessageBoundary`，包括 LOCAL、STATE、空元素消息和 Gap，都在推进 Replay/Gap 状态前按既有 `RawTime` 检查。非 ET 为秒精度，ET 使用可信微秒；秒级窗口的 start 包含、end 不含。超窗或不能按 RawTime 判定则整个新候选失败，诊断保留 source_id、完整 source_rank、record、原时间和原因；M2 原观察不删除，也不生成裁掉部分记录的 complete。更早的 ordered 合同错误仍由原门禁拒绝。
- 窗内时间倒序仍按完整来源位置/record/element 顺序处理，不排序修复，不以文件名判断实际时间或连续性。M2 消息级 timestamp_regression/cross_file_time_overlap 质量事实通过原消息引用保留；它们不是 canonical 的来源级 source_quality。Gap、last_known、精确恢复与连续性 Unknown/partial 沿原规则。
- 只选择原 baseline 加原 UPDATE 子序，穿插的 snapshot 保留在完整绑定中，不触发中途初始化。外层 `position.source_rank`、Coverage rank 是完整 M2 rank；既有 `changes.raw.source_rank` 仍是旧 Replay 内部计算序，不可当作上游 rank，两者未被互换或重编号。

显式窗的 `source_coverage.declared_window` 指向计算窗，同时新增 `window_qualification={rule,role,checked_messages}`。baseline 的 role 为 `baseline_complete_cutover_assumed`、checked_messages 为 0（表示不应用 UPDATE 时间门禁，不表示 baseline 没有记录）；UPDATE 为 `update_half_open`，计数覆盖全部实际消息。空 UPDATE 仍有 Coverage 及累计 Gap。该资格只证明所选消息通过输入窗检查，不证明时间连续或整个声明窗无缺口。

生产完成门禁与 `ProjectionReader.audit` 的独立 M2 重消费从实际保存的 plan 读取同一显式窗，重算逐行 typed 预期正文；改 Coverage 标签或改窗但不满足实际输入均拒绝。13 张物理表与 codec 不变，仅以上两种 typed 正文在新显式模式增加有限字段；旧正文继续接受。公共 P1 验证身份包含新增窗口实现，验证版本变化后必须新准入，不能复用旧验证身份冒充已执行该规则。

本片人工验证覆盖：名义包络早于计算窗；baseline Header 晚于 start 仍完整；额外 RIB 穿插且原 rank 保留；UPDATE 起点、终点和最后微秒；窗内倒序/Gap/空来源；错窗及错源拒绝；新生产与独立审计的完整 typed 原序对照；旧未传窗及原健康旧制品只读兼容。真实 577 来源是否符合本窗仍为 Unknown，遇到实际超窗必须据证据处理，不能自动扩窗或改时间。

## 表与资格

所有表的物理列为 `seq, source_id, message_id, event_id, object_key, gap_id, payload`；无适用引用时为 NULL。`payload` 使用 `canonical-typed-json/v1`，保留 tuple/set/bytes/None/0、字典键及空容器；枚举按原值保存。扫描返回解码后的完整正文。

| 表 | 正文职责 |
| --- | --- |
| baseline_mappings | 原映射记录，不提升失败头证据为映射见证 |
| changes | 原 Replay 完整变化字典 `raw`，另存完整来源 `position` |
| invalidations | 真实 STATE 的原失效记录，Gap 不伪造 STATE 或撤回 |
| scope_gap | 唯一 ordered Gap 原证据及引用 |
| qualification_change | 范围及对象资格变化，零命中仍有范围记录 |
| source_coverage | 每个选择来源的原计数、解释计数、派生表计数、声明窗口、首尾观察、累计 Gap 与执行资格 |
| source_quality | 原来源质量记录 |
| current_routes | 完整 scope/current/last_known/last_position 与当前、连续性资格 |
| legacy_state | 原 prefix/VP 路径 |
| legacy_prefixes | 原 legacy_by_prefix、legacy_origins，区分空容器和不存在 |
| legacy_seen_vps | 原已见 VP |
| projection_metadata | 原计划、版本、游标、baseline epoch 及缓存限制 |
| reference_binding | 参考来源/checkpoint/格式/行数及消费摘要，正文仍由 M2 读取 |

`changes.raw.source_rank` 仍是原 Replay 所选计算序；外层 position 使用完整输入源序，两者不能互换。`source_coverage.inherited_gap_ids` 表示截至该来源结束的累计 Gap，包含本来源新 Gap；空来源同样保留这些证据。首尾观察保留原时间与位置，不宣称覆盖整个声明窗口。

精确端点 Gap 覆盖全部 AFI/SAFI、prefix、ADDPATH 槽及未来对象；未映射 RIB 对相同远端保留 possible_overlap；未知端点按 collector 链。LOCAL 保留 Gap，但 canonical received 损失为 not_applicable，不替 Feature/Detection 裁定。新增 A/W 只使精确对象当次 known，before 仍如实 unknown，完整 last_known 和历史 Gap 保留。真实 STATE/EOR 不清除 Gap；额外 RIB 不重置范围。无观察对象返回 unknown，不能补为 absent 或零。

## 公开 Reader

入口为 `ProjectionReader(dsn, ProjectionBinding(run_id, snapshot, seal_digest, profile))`。必须显式绑定固定版本，不读 latest、候选或失败投影。当前 profile 不提供任意历史时刻查询、HTTP 或业务发布。

- `scan(table, batch_rows=2048, max_row_bytes=4194304)`：有界批扫描完整 typed 正文，每行验证有限字段和正文/物理引用，尾部核对行数/摘要，并进行与完成门禁相同的 13 表 M2 正文对照及固定输入和 PG 状态检查。正常耗尽的 `StopIteration.value` 返回含表名、行数、摘要、binding 和 execution 的完成回执；只取前缀没有该回执。调用方早停须 `close()` 或使用上下文清理，释放连接；关闭失败保留原异常，显式早停的关闭失败仍抛出。
- `current(scope)`：查询精确 11 元对象键的投影末态，完整扫描并校验 current 表；未见对象再读取范围 Gap 返回 unknown。此版本没有声称该查询是索引点查。
- `descriptor()`：返回 binding、实际数据库、schema、控制回执 SHA/目录实体、计划、codec、表摘要及文件 SHA，供下游绑定身份；descriptor 验证规则兼容性和封印身份，不代替完整正文 audit。
- `audit()` / `verify()`：校验 M3 文件、依赖的 M2 文件、引用与计数、13 表摘要及从绑定 M2 推导的逐行完整正文，并做首尾资格检查。
- `locked()`：调用方持有的上下文，对 M3 run 和 M2 run/checkpoint 取得 PG 共享行锁，进入与退出均 verify；竞争状态更新受锁约束。它不锁住外部文件或任意 DDL，也不替调用方提交业务事务。Country 后续接线须明确自己的提交边界。

Reader 的 `input_reader` 保留固定 M2 事实入口。路径正文、消息及参考细节仍通过该入口读取，不增设任意平台接口。

## 公共 P1 的显式真实候选 Runtime（本切片已实现，待独立复核）

`observations/projection_publication.py` 的 `Runtime` 支持两种明确模式。原人工调用继续使用 `fixture_only=True` 和原预算默认值；真实候选必须 `fixture_only=False, execution_profile='real-candidate/v1'`，不能用人工标志代替真实模式。本模式名称只限定执行与准入配置，不证明数据已生产完成或符合业务要求。

真实候选逐项显式提供 `dsn`、`allowed_roots`、`scratch_root`、`output_root`、`input_manifest`、`expected_canonical_binding`、完整 `dependency_admissions` 及按 ID 绑定的 `dependency_runtimes`。`expected_canonical_binding` 就是既有 `{binding: ProjectionBinding全字段, descriptor: 原descriptor全字段}`；`input_manifest` 必须等于该原 descriptor.plan.input_manifest，不另造 Canonical MRT manifest。`output_root` 精确绑定原控制回执目录；原 catalog 数据根及文件另由完整 descriptor 和允许根核验。

实际 M2/reference Runtime 必须与本 owner 同模式，并按其已接受合同绑定原 manifest/expected_m2_binding；实际完整 Canonical 计划、显式 calculation_window（若原计划具有该字段）、来源与参考选择由原完整绑定和实际依赖共同核对。旧默认计划仍可在显式真实候选 Runtime 下重新准入，不补窗口字段，不改变科学语义。

真实候选无资源默认值，必须显式给出 9 个严格正整数：`memory_bytes`、`max_temp_bytes`、`max_rss_bytes`、`lock_timeout_ms`、`max_batch_rows`、`max_batch_bytes`、`max_total_rows`、`max_total_bytes`、`min_free_bytes`。`max_total_rows`、`max_total_bytes` 保留配置兼容和正整数校验，不再按全流累计量中止读取；`session.rows/bytes` 继续累计计量。请求的 `batch_rows/batch_bytes` 仍硬限制实际批封装，单行也必须连同封装符合字节上限。合法有限预算可调整；模式、DSN、允许根、输出根、scratch、原 manifest 和完整 expected binding 不可在使用期间漂移。无总处理期限，RSS 与空闲盘保护持续执行。fixture 未传预算沿原默认值，显式 None/NaN/Inf/bool 不能伪装合法预算。

validator.rules_digest 固定执行模式与原绑定范围摘要；Admission 继续保存完整 owner_binding、实际物理/实体和依赖。真实/人工资格不能跨模式 current 或持锁复用；不是将调用者自报模式当作通过科学验证。连接配置与凭据仅保存在 Runtime，不进入 Admission/validator。代码规则变化时对既有原件完整新 admit、新 key，原记录保留，不自动重产 MRT/M2/Canonical。

scratch 必须与原输入、catalog 数据根和控制输出目录隔离。每次使用、临时子目录创建后、DuckDB 配置前及读中重新核实际规范路径和允许根；清理前也校验，不沿同名 symlink 重指向删除其他目录。路径字符串摘要不是实体授权。此处未增加管理员竞态或通用权限框架。

`admit/verify_current/hold_lock/open_reader` 生命周期不变：首次完整验收、当前资格不重放、单目标真实锁、正常耗尽且数据资源成功关闭并通过尾部 current 才返回 Receipt；固定范围或 scratch 漂移、早停、关闭失败均不能产生成功回执。本片仅使用既有人工原件验证双模式；没有核真实 577 来源。

## 规则兼容与重消费影响

当前显式支持以下准确合同，未知、缺失或错值在 Writer 初始化、完成门禁与 Reader 首尾复验时拒绝；物理表名、列名/顺序和类型必须匹配本页 schema。代码文件摘要继续保留为生产身份，支持规则一致的历史代码不因 hash 不同被拒绝。

| 项目 | 支持值 |
| --- | --- |
| 投影 profile / codec | canonical_projection/v1 / canonical-typed-json/v1 |
| Replay / mapping | calculation-replay/v1 / full-input-unique-calculation-endpoint/v1 |
| ordered / Gap | ordered-observation/v1 / payload-gap/v1 |
| M2 profile / schema | observation / observation-checkpoint/v1 |
| M2 interpretation / manifest | mrt-interpretation/v1 / observation-run/v1 |

修复没有改变以上规则、13 表 schema 或科学输出值。旧 `19554d4` 的正确制品可以在原数据库、根目录和快照上通过新验证后继续消费；实际原健康 run 已由新进程完整复读确认，无重产、迁址、改封印。原错误 complete 候选保持原状，新 Reader 拒绝其 audit 或扫描完成回执。

一次 audit/verify 将 13 表共享于一次 Replay 遍历，不对每表独立重放；此外仍有映射、参考及原 SQL 引用/计数查询，遍历行数不等同存储引擎物理 I/O。各入口次数如下：

| 操作 | verify 次数 | Replay 遍历次数 |
| --- | ---: | ---: |
| audit 或 verify | 1 | 1 |
| 空 locked 上下文 | 2（进入/退出） | 2 |
| 单表 scan 正常耗尽 | 0（直接调用同一验证） | 1 |
| 分别耗尽 13 表 scan | 0 | 13 |
| locked 内分别耗尽 13 表 | 2 | 15 |

每次完整验证会重新读取已封存 M2 的所选消息/元素、映射见证和参考，以及固定 M3 的 13 表，使用现有 Replay 的内存状态做直接对照。当前每次 scan 耗尽也执行该验证，连续扫描多表会重复这部分成本；没有引入跨次缓存或廉价索引保证。调用方只读前缀不能称为已验。映射/legacy 的完整容器仍保留，资源超限报失败，不裁剪输出。SQL 对照使用最多 13 个独立游标逐行读取，摘要批量上限 128，正文验证/摘要默认单行 4 MiB；guard 在逐行与参考消费中持续调用，不吞预算异常。原数据重产与验证读取必须区分：M2 无代码变化，不需重新解码 MRT；M3 仅重新验证，无需因本次修复默认重产。若原内容不符，则拒绝该候选，不修改成正确结果。

## 验证与测量边界

人工测试覆盖无 Gap 的完整原 Replay 变化及状态对等、冻结旧回放记录、精确/未知/LOCAL/未映射范围、ADDPATH 0、跨族、零命中未来对象、A/W 局部恢复、历史 last_known、额外 snapshot、空来源、批大小变化和清理失败。

独占无 TCP 的私有 PG 人工 MRT 验证执行 M2 → ordered → M3 writer → 新进程公开 Reader，比较 13 表完整正文；保留 M2 文件 SHA 和封印不变证据。删除 Gap、篡改覆盖计数、来源早停和错封印均拒绝完成读取；共享锁竞争和读取字节上限另有负向检查。测试结束停止私有 PG。测试代码中的私有 PG 环境变量必须指向本任务专用 `/tmp/domeye-m3b-efb1-*` socket。

每次执行记录 mapping/replay/write/validate 独占阶段 wall、行数/正文 bytes、0.1 秒当前进程 RSS 采样、起止当前 RSS、整个进程生命周期峰值、输入压缩 bytes、输出 Parquet bytes 和输出目录临时盘用量。阶段无采样为 Unknown；生命周期峰值可能包含同进程先前 M2/测试，不能当作 M3 单阶段峰值。mapping 输入行数是声明选择消息量，输出量是映射记录；validate 输出零表示不生产正文。`validation_reconsumption` 另列本次验证实际对照的投影行数/正文 bytes、声明所选 M2 消息/元素数和实际参考行数；阶段 validate 的 input_rows 仍指投影行数，不充当物理 I/O 次数。总 wall 与临时盘在最终回执和完成登记前截取，临时盘不包含独立列出的 Parquet。保护检查覆盖临时盘和 catalog 数据盘，并保留 RSS 上限，无总运行截止时间。

人工小样本只能支持上述正确性与资源记录行为，不能外推真实全量性能、Feature/Detection/Resource/Country 业务资格或全国实际网络影响。


## 同组 Peer 引用的稳定枚举

已实现，定点人工验证通过，待独立增量接受：`observations/replay_io.py:build_mapping` 在同一 baseline 的 `(peer_ip, peer_asn)` 组内按已有 `(table_record, index)` 位置枚举 `peer_refs`，与 checkpoint 的 Peer 逻辑键一致。重复内容保留所有位置；不按 BGP ID/IP 值去重或重写字段。该顺序是固定制品引用的稳定表示，不宣称恢复 MRT 原字节顺序，不改变消息/元素/AS_PATH/AS_SET 顺序。

映射规则仍为 `full-input-unique-calculation-endpoint/v1`，`conflicting_baseline_peers`、无端点、多端点及唯一端点的原判定不变。仅生产代码身份更新。原健康单引用或已经按位置枚举的 M3 可在原身份下复验；原多引用若保存为其他排列，新 Reader 的严格正文对照会拒绝，不能静默改写旧 complete/摘要。科学映射未因此改变，但这种旧表示需要另行批准的兼容重准入或新 M3 派生身份；不由本次修复默认重产 MRT/M2 或所有 M3。
