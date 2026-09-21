# Canonical M3B 人工集成增量验收

结论：**GO，仅限本次人工集成片**。现可从原已封存 M2 独立生产 canonical，取得公有 descriptor/audit，并在另一进程完整读取 13 表；错误正文及未知规则拒绝。本片没有提供轻量页面 API，没有接线 Country、Resource 或 P1，不代表全天、真实数据准入或产品验收。

## 固定范围

- 集成起点：`7571c164756c8039f54933ad3f70f6e5745c5d6c`。
- 授权完整链：`52f0a1fe74e7cc1ffbea5a05a83751145d775056` → `0c7c905776f9349b58565d65726138becd6aa53e` → `19554d4c67806f8775f655b13246ae619ff7fefa` → `63cc844e79a361d874f3935ced212c9f3e84dd5e`，相邻唯一父关系已核对。
- 无冲突合并：`0ed5206c8b3bd602f051a3cc938d27749a53a8b8`，准确双亲为 7571c164 和 63cc844e。8 个合入文件、1393 增/12 删的完整差异已读。产品实现与 63cc844 完全一致，集成只新增测试与本报告；没有擅自优化 Reader。
- 原独立报告指定工作树文件已不存在，改从固定 `30db18c811b9437fcc2029766408a91d8b8a675e:docs/reviews/M3B独立canonical实现复核-19554d4.md` 读取全部原文，SHA `317f59def8293abc8ca50086d86c892e617f8f6d7b95b006467167e9b659aab1`。
- 完整阅读 84e5 的 `docs/reviews/M3B两项P2增量复核-63cc844.md`，SHA `d74f66b57019be523477a18ab4eaea5eaf495e9617f3b8fbfb2f06c1d0ffde8a`；作者 `/Users/botongwu/.codex/outputs/domeye-m3b-efb1/M3B两项P2修复交付报告.md`，SHA `adf85306da9b6e72798325bf74d8e597afb42136cdc83e2528c20e28c090a3a7`。两项 P2 已获独立关闭；作者/独立 45、26+9 等结果不计为本任务测试。

## 原 M2 与新输出

证据根：`/tmp/domeye-canonical-m3b-integration-8233`。使用本任务自有 PG `/tmp/domeye-integration-detection-8233/pg`，Unix socket 55483，关闭 TCP。实际 DSN 与输入绑定保存于 Git 外 `reader.json` 及此前 Detection `request.json`。

复用上一片 M2 原库与根目录，不重新解析 MRT，不重产 M2：`3884fdb6cdb7408d92f78a85db70a6af:26`，原 seal `3f26834c1e026ea01a19928f04f602ae282d32c6393ddb950c0145c7fbc9000c`。实际输入为基线 4 消息/9 元素、UPDATE 14 消息/13 元素，共 18 消息/22 元素，压缩 MRT 357 字节；11 份参考共 24 行。正常生产期间将旧 Store.finish 和 M2 producer 替换为抛错哨兵，确认未调用；原 M2 登记、checkpoint、seal 均完全不变，仍为 `observation_sealed / business=not_run`。

新 canonical `c4568efe78c04182900b0999877acd76:51`，seal `e1874d757df7e68cbbd50fe2ac1994a05f3da267eb358368d567ff15d6d7d4f8`，`canonical_projection/v1`，execution complete、business not_assessed。控制根为证据根下 `canonical/`；13 表写在原 catalog 的独立 `m3_c4568efe78c04182900b0999877acd76` schema。新 Parquet 位于 catalog 原 data_root 的新子目录，不能将这些新增文件误称为修改 M2 原表/文件。计划的 25 个代码身份 SHA 与当前实现一致；本入口不是冻结子进程执行，不套用 Detection 的冻结结论。

| 表 | 行数 | 表 | 行数 |
| --- | ---: | --- | ---: |
| baseline_mappings | 3 | changes | 22 |
| invalidations | 0 | scope_gap | 1 |
| qualification_change | 1 | source_coverage | 2 |
| source_quality | 0 | current_routes | 10 |
| legacy_state | 6 | legacy_prefixes | 4 |
| legacy_seen_vps | 3 | projection_metadata | 1 |
| reference_binding | 11 | 合计 | 64 |

## 正文、科学与边界

正常 Writer append 处保存完整生产正文；新独立 Python 进程通过公有 scan 耗尽 13 表，逐表取得 complete 回执。所有 typed 值、bytes/tuple/set/None、空容器和顺序逐项相等，不仅是摘要或计数。两份完整正文文件 SHA 均为 `dfe75137102cd6986dfa98fcf72973254eeae0db6693623a33ff3f99e130974e`。公有 audit 另核物理引用、完整来源/Gap、绑定 M2 推导正文和封印；测试额外逐条核 event/message/元素 ordinal FK 与 position。

从固定 52f 导出原 state.py（SHA `340025bb11b2249da409cb15fee766a64d4d3f168625869398161e264711d354`），用原 SQL 消息转换入口读取同一 M2，未复用新 canonical 转换器。原 Replay 全部 raw 与新 changes/invalidations 相等；current、last_known、legacy_paths、legacy_by_prefix、legacy_origins、seen_vps、baseline_epoch、cursor 八项状态完全相等。完整 Reader 对照中的 Replay 复算不独立证明科学算法，科学证据来自此固定旧代码及下述手写预期。

本主样本最后 1 个 Gap 为 **LOCAL**。canonical scope 资格为 not_applicable，不失效 received 当前态；10 个已见对象当前资格均 known。主样本没有 received Gap、合法 LOCAL 元素、非连续 rank 或未选择 snapshot，不能冒称实际存储链已覆盖这些场景。公有 `current()` 对一个未见前缀返回 current/last_known=None、资格 Unknown，不补 absent/零；该调用耗尽 current_routes 和 scope_gap，各触发一次全验证。

单列手工 typed 流以固定 52f 独立构造 Message 作全 raw/八状态对照，并手写断言：AS_SET `{328405}` 的明确 attribution 为 None；IPv6 path_id_present=True 且 path_id=0 保留；合法 LOCAL 不产生 received changes；精确 W 后真实 STATE 使 current Unknown、last_known 仍为 absent；旧 origins 空集合保留。完整输入 `[baseline,snapshot,update]` 只选择 baseline/update，外层 rank=2 与旧 raw 计算 rank=1 分别保留，没有消费或重置额外 snapshot。

另复用一个手工流测试，验证 received Gap 覆盖 IPv4/IPv6 和不同 ADDPATH 槽、保留其他 peer；后续精确 A/W 只恢复相应对象，before 仍 Unknown、last_known 保留、历史 continuity partial，Gap 不伪造 STATE。这些是手工流科学边界，未伪装为主 M2 实际覆盖或全天证据。

## 两项实际拒绝

1. 经正常 Writer 将第一条 changes 的嵌套 calculation_after.event_id 指向另一条确实存在的末尾事件，外层合法引用不变。不是管理员篡改 PG/seal；正常 emit 可进入落盘，但完成前与绑定 M2 正文对照拒绝，错误为 `投影正文与绑定 M2 推导不符: changes`。失败 run `740ec8e4fe9049c6b63744cad5760997` 保持 failed、seal=NULL，failure.json 与原失败 Parquet 保留。
2. 正常 producer 声明 `unknown-replay/v999`，在 Writer 登记前拒绝，m3_projection.runs 数量不变。合法历史 codehash 支持是已接受源码合同，本任务没有再重跑原完整规则矩阵。

## 实测读取成本

使用包装实际 expected_rows、M2 stream/reference_batches 的计数，不添加缓存、不跳过核验。消息/元素/参考数为实际流返回，bytes 为该流返回内容的 canonical 编码字节数；**不含 mapping 与验证 SQL 内部扫描，不等于总物理 I/O**。

| 本次实际操作 | 完整验证 Replay 次数 | M2 消息/元素/参考行 | 期望投影行/正文 bytes | wall 秒 | 累计进程 RSS 峰 bytes |
| --- | ---: | --- | --- | ---: | ---: |
| descriptor + 单 audit | 1 | 18/22/24 | 64/65672 | 2.488411 | 313376768 |
| 另一进程分别耗尽 13 表 | 13 | 234/286/312 | 832/853736 | 43.859038 | 322076672 |
| 公有未见对象 current | 2 | 36/44/48 | 128/131344 | 2.129179 | 288505856 |

三类公开读取合计 16 次完整验证 Replay。对应消息/元素流编码 bytes 为 55038、715494、110076；参考流编码 bytes 为 11526、149838、23052。上述 RSS 均是各进程生命周期累计峰值，包含该进程此前操作，不含独立 PG；未采样读取阶段独占 RSS，阶段峰值为 Unknown。13 表输出本身共 64 行/65672 编码 bytes；表尾反复验证产生的 832 行是验证重消费量，不是新增产物。

63cc 的准确调用语义：单 audit/verify=1；空 locked 进入/退出=2；13 个 scan=13；locked 内完整 13 表=15。本任务实测了 audit 和独立 13 scan，另有 current 的两次验证；**没有执行空 locked 或 locked 内 13 表**，其次数据源码列出，未虚构为本次实测。不把本 Reader 描述为轻量页面 API；P1 admission 优化不在本片。

## 生产与资源成本

正常生产外层 wall 5.295555 秒；完成登记前内部 wall 5.131464 秒。生产一次 canonical Replay，完成核验再一次 Replay；插桩的 `replays=1` 专指 expected_rows 验证入口，不能把整个生产说成只计算一次。实际流累计 36 消息/44 元素/48 参考行，消息元素编码 110076 bytes、参考编码 23052 bytes。

| 内部阶段 | wall 秒 | 阶段声明输入/输出行 | 输出 bytes | 当前进程采样 RSS 峰 bytes/样本数 |
| --- | ---: | --- | ---: | --- |
| mapping | 2.094587 | 18/3 | 1434 | 235470848/18 |
| replay | 0.257640 | 44/50 | 57501 | 279691264/2 |
| write | 0.447155 | 64/64 | 65672 | 287309824/4 |
| validate | 2.307943 | 64/0 | 0 | 307331072/19 |

mapping 输入是声明消息数、输出是映射行，不等于 SQL I/O；reference 的 11 条绑定计入 write。validate 输出零只表示不再写产物，其实重新读取 18 消息/22 元素/24 参考行并逐项对照 64 行/65672 bytes。0.1 秒采样为当前进程 RSS；生命周期峰值 307494912 bytes 不可替代阶段峰值。11 个健康 Parquet 文件共 39626 bytes；完成前临时目录 12288 bytes，均不含 PG/WAL。正常生产、audit、13 scan、current 之外，固定旧 Replay 对账另有一次 SQL 原消息全读；手工流不读 M2。

正文失败候选另做一次生产 Replay，终检只执行到错误行即停止；失败回执记录 mapping/replay/write/validate wall 4.224551/0.533798/0.771121/4.257239 秒。该负例和未知规则探针没有包实际流计数器，精确重消费行数/bytes 为 Unknown，不加入上表实测总量；未为补指标再跑负例。未知规则在 mapping/参考读取后、Writer 登记前拒绝，没有 canonical 生产遍历。

项目依赖使用 uv.lock；默认 RSS/内存保护保留，人工调用 min_free_bytes=0，不修改生产默认。没有处理总 deadline。小样本与顺序运行受主机负载、缓存影响，不外推性能。

## 测试、保留与停止

本任务 **5 个不同测试通过，无失败、无 skip**。执行记录如下，重叠项只计一次：

- `pytest.log/xml`：3 passed，70.11 秒，包括主联合路径、实际两反例、received Gap 手工流。
- `pure.log/xml`：1 passed、3 deselected，1.33 秒，固定 52f 多类型/rank 手工流。
- `boundary.log/xml`：2 passed、3 deselected，2.80 秒，包括上项在补充缺环境跳过分支后的复查及新增公有未见对象 Unknown。

命令使用 `env -u PYTHONPATH`，`uv run --locked --project backend pytest -q backend/web/tests/test_canonical_m3b_integration.py`；显式绑定 `DOMEYE_CANONICAL_OWN_ROOT` 与 `DOMEYE_CANONICAL_INPUT_REQUEST`，随后仅用 `-k` 执行补充项。完整日志、Junit、正文、指标、拒绝回执均在证据根。没有运行原 45/26+9 矩阵、三锁/kill 或旧 strict 大套。

原 Feature/Detection 及所复用 M2 通过逐文件 SHA 留存核对，267 个路径项经 `/tmp` 与 `/private/tmp` 别名去重为 **247 个唯一原文件**，没有把路径项数当唯一文件数。Feature/Detection 未重产；本次不重复其刚接受的全套读取。原 M2 登记/checkpoint/封印正文不变，新增 canonical 文件独立列出。

`最终文件与权威核对.json` 核 2c8a 迁移计划、ADR-0002、README 三 SHA 与授权基线一致。自有 55483 PG 已 smart stop；55483 与 28763 的 status=3、PID/socket 不存在，见 `PG停止核验.json`。完整差异及 diff --check 已核对，未修改其他任务代码或文件。仅当前人工片，无真实 D/P/H、远端、726、独立 A、HTTP、Issue、push 或部署；交协调者后停止等待下一片授权。
