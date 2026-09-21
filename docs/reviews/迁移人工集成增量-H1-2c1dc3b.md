# H1 / Q3-C.2 人工集成增量

2026-09-13。**GO，仅限授权链的人工离线 H1 集成。** 新规则 CoreToken 已在自有小样本上完成五表、完整详情、分页及窄索引跨进程验证；两项 P2 的真实关闭错误和合法操作登记均闭环。不代表真实 H 迁入、H3 关系闭合、HTTP 或产品验收。

## 固定范围与审查

- 合入链：`57e1f0740e2b321e5e3a6f22150434df3b3aff47` → `23d243d9e71c8f472bcdf30f30bf21c16a4859e8` → `2c1dc3b6669ecd20e00f82a9208b5f8041dec220`；两个唯一父均核实。
- 无冲突 merge：`0b113dddf304782a404c6159452d7929a2491492`。准确双亲为原集成 `d2ff934056aa3e0629495fff62ee6df79bfae631` 与 `2c1dc3b6669ecd20e00f82a9208b5f8041dec220`。
- 完整阅读链差异（10 文件、2286 增/1 删）、七个 H1 产品文件、两份测试及设计增量；本任务只新增集成测试与本报告，不改产品、规则或旧模块。
- 原独立双轴报告 SHA：`5faaec041c7d6e85a8fb57b36e58536373cc4f2f2488a5464ed1d3ccc3c5ffcc`；两项 P2 增量独立复核 SHA：`b7006e4c5c5e5b127b7c794bc0d9b1823df13ae95cc7bea21457e2e642196486`；作者修复说明 SHA：`ed3a14f3b25446f9266b9c44814de65bb71de1ba20132290a4d154c7a5462575`。全文与原作者交付已读，三份 SHA 实测一致。作者 109 项、原独立 37 项及修复 10 项不计入本次结果。

## 一次合法样本与原件边界

证据根：`/tmp/domeye-integration-q3-8233/h1-integration-8233/acceptance-5455ef6903dd43639f106787a2c8d127`。项目锁定依赖、自有 Unix socket 28763、独立新库 `h1_8233_*`；不使用别人的 Core 样本。

原 C.1 Collection `39b6737ffd374e82b8196f4c4de08c78:14` 的 item 刻意是通用重复键 JSON，不是合法 H1 异常记录。保留它的原结构资格与制品，不能为了复用而放宽领域准入。另造一次原生 Core SQLite：19 条、六类记录、重复引用与单一引用、mixed/unknown/AS_SET、一个合格空日和四份不同失败诊断。通过现项目原异常转换器、序列化器、overview 纯规则形成合法记录；增加精确大数、`Decimal('-0.000')`、负一微秒、带偏移微秒时间以及普通 item.extra 重键和数字词法。

只执行一次 freeze、import、project；后续复查复用同一目标与 Token。完成后将自己的 source/freeze 移至 `source-removed` / `freeze-removed`，保留原件，原地址不存在。新进程显式禁止 `DailyIndex._open` 回访，仍完成所有要求。

- Collection：`e686ebb66a60434eb79a2706a94423f4:19`。
- Core：`1f7d8c12e3f043b5b31bca55e5e14851:11`，root_id=0。
- 新规则：`10f545190d3460bd8c351d82e7dc4fedb841c179481c5e0769fc758a54829e07`，实测一致。
- Core ready SHA：`604a06d2b3008c52b8f579d4bc51e0ebd981c3735473e6ecf3e7e75f4c6baf7c`。

本任务没有原 23d CoreToken，**未实测跨规则旧 CoreToken**，也不声称兼容。源码保持旧规则拒绝、合格 Collection 可重新 project 的边界。本次新建合法 Collection 的原因是原 C.1 样本不合法，不是旧制品必须重导。

## 实际验收

五表有序全列结果：days 6、diagnostics 4、records 19、scalars 204、links 38，共 **271 行**。首进程保存完整有序快照，新进程逐表逐字段一致；不是只对 counts 或摘要。19 条原 SQL 检索列、全部 item 精确有序文档、payload、provenance 与根 metadata 均核对。原始文本期望由标准库 `object_pairs_hook` 与数字词法捕获生成，没有用产品 exact/Parser 生成 oracle。scalar 每个成员位置回到原文本树，数值符号、系数、指数、负零、整数便利值、UTC 文本和 UTC 微秒独立对账。普通重键、`1`/`1.0`、`-0`、9007199254740993 保留；H3 链仍标 unresolved_external。

新进程完成 28 次 events 页请求（十组筛选/排序，包括必要末页判空），与固定原 overview 纯规则形成的 items 及独立 Python 筛选排序对账；另完整耗尽引用分页，核 ambiguous 两候选、matched 一候选及 missing。按 day/family 独立核小时 distinct prefix 和总体人口；列表筛选不改变趋势人口。四失败日 query/detail/references 均保留原诊断及三个 null；合格空日为 available/0，缺日 not_retained 且不补零。

真实失败日详情与不存在 occurrence 的详情均进入终端 `operation_scopes`，分别为 validation_failed/unknown 与 not_retained/missing。新进程 `rebuild()` 创建独立 schema `hcq_02c167dd5a26464c84fcc3d2ec22ed3a`，19 行、12 个明确列。第二条真实 PG 连接在 Core 会话退出前读取已提交结果，所有列/行与领域表完全一致；此时 session.receipt 仍为 None。最终回执记录实际 schema、完整 Token、committed、19 行和 core_query_index_only，不能用内部 bulk 登记替代；未复制 item/payload/nodes 正文。

关闭探针实际执行：

1. 原生 DuckDB 先关闭、再抛 RuntimeError：句柄不可用，永久 failed，无 complete。
2. 原生缺表 SQL 产生 CatalogException，同时关闭后抛错：保留原异常对象及回溯，附加 DuckDB cleanup_errors，无 complete。
3. 健康关闭期间，第二条 PG 连接以 100ms lock_timeout 尝试更新本 profile，被真实共享资格锁阻止并回滚。顺序为 DuckDB 已关闭 → 第二连接受阻 → 资格 PG 已关闭 → complete 可见。没有用模拟事件代替锁竞争。

## 旧件与其他链

原 C.1 六表完整有序编码与原 `ordered-typed.json` 字节相等，176 个原文件 SHA 不变。原五个 Q3-A/B Token 在各自原数据库直接 component/scan/bulk 读取，不重导；各表行数分别 `[4,1,0]`、`[6,1]`、`[3,1,0]`、`[5]`、`[3,1,0]`，22 个导入文件 SHA 保持原值。保留原 Token、ready、快照与归档回执，没有新“旧结果”冒充兼容。

Feature/Detection/canonical 三链的 271 个唯一原件 SHA 与上次固定保存清单一致。Resource 仅保存当前 17 文件 SHA，并核执行元数据与原 case 除 state 外所有字段一致；case 返回为 complete，原 execution 写盘为 ready，原区别如实保留。Resource 没有此前逐物理文件 SHA 清单，本次当前摘要不冒充历史逐文件对照。四链都未重新运行全量业务读取，55483 未启动。

## 分阶段实测成本

只适用于本次 19 条人工形态；独占 PG 语句来自已验证随机标记的实际日志，包含 DuckLake 隐式目录访问。Python SHA 包含规则、child、载体、行摘要及游标，不等于 OS 全部 IO，也不能称线上低成本。

| 阶段 | wall 秒 | 实际 PG 语句 | Python SHA 次 / 字节 |
| --- | ---: | ---: | ---: |
| freeze | 0.690105 | 未独立测量 | 未独立测量 |
| import | 1.493459 | 752 | 443 / 11603133 |
| project | 0.440717 | 624 | 670 / 6435208 |
| 五表全读＋19 完整详情＋根 metadata | 0.408280 | 303 | 329 / 3434211 |
| 新进程筛选分页、引用、日门禁及合法详情 | 0.276957 | 616 | 368 / 3382875 |
| 新进程 rebuild＋第二连接全列对账 | 0.137998 | 133 | 487 / 4998421 |

全读输出主体 271 行，回执 typed_rows=291 还包含详情/日定位读取，不能称 291 个事件。project 规范化逻辑写出 109786 bytes；Core Parquet 19366 bytes；Collection Parquet 173136 bytes；底层保留制品 434108 bytes。project 暂存 SQLite 94208 bytes、候选磁盘观测峰 113574 bytes。窄索引 relation_size 49152 bytes，单批写 SQL 2995 bytes。

全读和分页的集合显式资格 hash_calls 均 154 / 1325764 bytes；rebuild 因内部资格路径为 231 / 1988646 bytes。这些不含全部旧 child/代码 SHA，不能与全 Python 计数混称。未做多个分页大小的成本矩阵，不推断本次不同页大小恒定。

project 当前 Python RSS 200507392 bytes，进程生命周期峰 200704000 bytes；全读当前/生命周期峰 165773312 bytes；分页 167772160 bytes；rebuild 当前 168214528、生命周期峰 168640512 bytes。阶段独立峰值、PG 内存与系统整体 IO/临时峰值为 **Unknown**，不是零。成本原 JSON 和原 PG 日志均在证据根。

## 测试、失败保留与封口

本次 **5 个不同集成用例最终通过**，不累加重复运行：首轮 4 passed/1 failed，5.15 秒；修正自有测试后仅重跑全字段/新进程一项，最终 1 passed/4 deselected，1.47 秒。其间两次定向失败日志亦保留（0.58、1.19 秒），没有重新造样本、重导或降低产品门禁。

三处自有测试修正为：负指数 `-0.000` 不应期待整数便利列 0；新进程 cwd 必须是本项目 backend；Python 回执 Token 的 tuple 应与 asdict 原结构比较，不能先把一侧转换为 JSON list。最后一次修正前 rebuild 已真实提交，其独立索引保留，不能把测试断言失败称为产品事务失败。首轮/定向 JUnit、完整 traceback、新进程日志均归档；未发现需产品 REPAIR 的问题。

执行命令使用 `env -u PYTHONPATH Q3_PRIVATE_ROOT=... uv run --locked --project backend pytest backend/web/tests/test_h1_integration.py`；定向复查显式 `H1_INTEGRATION_REUSE=原证据根` 及 `-k full_fields`。没有运行 109/37/10 全矩阵。

完整差异及 `git diff --check` 核验。2c8a 迁移计划、ADR-0002、README 三份 SHA 分别保持 `a7c6fbceda3558557d1bde1404db15afba26d171e105daf5a8013f0c02374610`、`eef76025290ebb6f76ec48c8b9264d1d999376178e29e2bee21ee783b00515af`、`c82f02b8a17276ee8d740d8663b4d969c2a9e095410746b813737c461b9b051f`。

自有 28763 已 smart stop；28763/55483 均复查 status=3、无 PID/Unix socket，证据 `PG停止核验.json`。未操作真实 H/D/P、远端、726、独立 A、HTTP、Issue、push、部署或其他在途候选。标准模式、无 Fast。仅本地提交并交协调者，等待下一步明确授权。
