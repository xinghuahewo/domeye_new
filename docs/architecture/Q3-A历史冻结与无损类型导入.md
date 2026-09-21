# Q3-A 历史冻结与无损类型导入

2026-09-13。**状态：Q3-A人工载体及默认读取成本修复已独立接受；Q3-B 930预算修复已独立Standards GO / Spec GO，并已人工集成；真实历史迁入未实施。**

本片从 `4eeee93937e853b8affc61e9de012e3a15b3db3d` 建立 `codex/q3-a-history-types`，工作树 `762f/domeye-new`，正式任务 ID `01a0977a-0c75-7fd0-b012-ce919a12a2dc`。合同依据是固定 `6106cdb419a32d8e5bcb1a7fa9b7c57d6093f2ba` 的《业务整批发布与历史共享查询接缝设计》第 6 节及 Q3；通过 `git show` 阅读，没有导入其 Q1 产品实现。总计划以父任务指定 `2c8a/domeye-new/docs/architecture/新架构一日全链路迁移计划.md` 为只读权威；不改计划范围。

字段来源沿用已接受 [0A 映射](旧新逻辑状态数据映射.md)（`b74d602c`）与 [0B 映射](一日输入与前端消费映射.md)（`97488d03`）。本片没有访问旧项目运行环境、真实数据库、真实原件或远端；没有 push、Issue、HTTP、前端、业务 head 或部署操作。

## 实现与 Interface

[historical_import](../../backend/data_pipeline/history/database_import/__init__.py) 是独立深模块，应用 codebase-design：调用者只需知道固定源、预算和固定 Token；事务冻结、类型转换、occurrence 身份、候选校验和目录版本细节收在 Implementation 内。没有新增领域术语或重选存储；沿用 [CONTEXT](../../CONTEXT.md) 和[分层存储 ADR](../adr/0002-layered-data-storage.md)，不为本片再立 ADR。

| Interface | 输入与结果 | 必须保留的限制 |
| --- | --- | --- |
| `freeze_sqlite` | 关闭写入的人工独立 SQLite 原件、源版本、可选原附件/可用性/ref → 封存包 manifest 路径 | 有 WAL 则拒绝；复制前后及副本 SHA 相同，从只读事务流式读取；保留原文件、完整原 DDL/schema 对象 |
| `freeze_postgres` | 本任务私有 UTF8 socket PG、显式原 schema/table 集合、源版本 → 封存包 | 同一 repeatable-read/read-only 事务获得目录、约束、总数和所有行；不能把显式子集称为全库 |
| `History.import_package` | 已完成封存包 → 固定 Token | 验证后写候选；所有块/表/typed schema/行内容/occurrence/文件核对并固定回读后，PG 登记 complete |
| `History.component` | Token → 完成资格、原元数据、固定 catalog/schema/snapshot、逐表计数、可用性 | 只交付历史 component；不选择业务发布，不生成 known_empty，不把 complete 当全业务覆盖 |
| `History.scan` | Token、原表序号、after_ordinal、limit → 完整字段与 typed 值的有界行流 | 必须固定版本；按原 occurrence 顺序分页；字段未存在不生成键，字段 NULL 明确保留 |
| `History.resolve_reference` | Token、原 ref → resolved/not_retained/ambiguous_reference/unavailable | 无唯一证据不猜关联；所有目标必须指向本包真实原表行 |
| `History.rebuild` | Token → 独立人工 PG 查询副本及原表映射 | 不再读原 SQLite 或 PG 导出；从历史构造后全字段回读核对；没有业务可见指针 |

Token 固定 `import_id/source_freeze_id/manifest_sha256/snapshot/ready_sha256`。每原表映射为独立 `lake.history.tN`，原列物理名为 `c0…`，防止 DuckDB 的大小写折叠使 PG 的 `A` 和 `a` 冲突；原 schema、表名、列名及顺序完整保存在冻结清单。原 PG OID、typmod、默认表达式、nullable、PK/其他约束、数组声明维数、identity 属性和 collation OID 不覆盖成目标信息。实际 typed Arrow/Parquet 字段另行核对。未知源算法和历史适用性不能用导入规则版本替代。

## 封存、身份与完成资格

封存包使用 `history-freeze/v1`，规则为 `history-lossless/v1`。PG 原值块是规范 JSONL 数组，每个位置对应原列；SQL NULL 是该位置的 JSON null，原 JSON 值是带原类型的文本。SQLite cell 分别保存实际 storage class 与原值编码。清单固定原件/附件 URI、SHA、源版本和用途；附件原字节不解析。附件是独立不可变副本，**不声称与 PG 具有跨源事务一致性**。

PG 冻结记录实际 system_identifier、database OID、快照文本及导出 token、事务时间、服务器/驱动/Python/工具及工具代码 SHA。导出固定 UTF8、UTC、ISO/YMD、ISO 8601 interval、extra_float_digits=3、hex bytea。持有一致事务至所有原表读完；CTID 只用于同快照导出排序，不作跨冻结身份。SQLite 记录原 encoding、SQLite/Python/工具版本与只读事务方式。

每条 occurrence 是 `(source_freeze_id, 原schema, 原table, block, row)`，另有从零开始的整表 ordinal。内容 SHA 用于对账，不能去重；相同内容的两行保留两个 occurrence。每块保存实际/期望行数、字节和 SHA，整表按固定序列累计内容摘要；重数由保留的完整行和独立多重集合验收证明。

同冻结同内容已 complete 的再次消费只返回原 Token；同源身份不同 manifest 摘要拒绝。候选或失败没有自动续跑/追加。新冻结产生新源身份和新 import，旧 Token 不变。`pg_try_advisory_lock` 保证同冻结单写者；PG 唯一键保持身份约束。普通错误留下 failed 候选，显式读取也拒绝。提交响应未知须人工核对，不自动恢复或宣称跨存储原子提交。

历史主体在 DuckLake 管理的 Parquet，PG 只存小型完成登记、DuckLake 元数据和明确离线重建的查询副本。文件定位来自固定 snapshot 的 `ducklake_list_files`；ready 的文件 SHA 是核验回执，不另建取代 DuckLake 的文件目录。文件及相关目录 fsync，封存 source-manifest/ready，之后才提交 PG complete。原件、冻结块、目录元数据和历史文件均不得清理或覆盖；本片不提供清理、备份、恢复平台，也不声称能防御文件拥有者主动改写。

## 无损类型规则与查询映射

普通 PG cell 有 `raw` 与 `value`：前者是数据库规范导出原值，后者是可查询 typed 主体；SQL NULL 为整个 cell 的空值。历史验证同时核对两者，不能只证明 raw 还在。以下没有通过 float 转换 numeric。

| 原类型 | typed 主体与可逆规则 | 独立验收值/含义 |
| --- | --- | --- |
| int2/int4/int8 | int64，检查原类型范围；bigint 在 Python 行流中仍为精确整数 | `9007199254740993`、smallint 最小值、int 最大值；未来 HTTP 序列化须另处理 JS 安全整数 |
| bool | boolean；数据库标量 `true/false` 与数组 `t/f` 输出均明确识别 | true、false、SQL NULL、数组内 NULL 分开 |
| float4/float8 | 分别 float32/float64；raw 保留数据库输出；NaN/±Infinity 明确保留 | float4 `0.1` 的实际值 `0.10000000149011612`、double `-0` 符号位；不承诺数据库导出已丢弃的 NaN payload |
| bytea | binary；raw 为 hex 输出 | `00ff01`、空字节与 SQL NULL 分开 |
| text/varchar/bpchar/name | string；原文本不 eval、不解释成数组/JSON | `text "null"`、空文本、`["{328405}"]` 原样；bpchar 使用 `bpcharout`，避免 `::text` 裁掉原填充空格 |
| numeric(p,s)，p≤38 且目标支持该 scale | struct(state, finite decimal128(p,s))，原 typmod 与文本另存；NaN finite=null 且 state=NaN | `numeric(12,4)=12345678.1250` 使用真正 decimal；非空 NaN 不与 SQL NULL 混同 |
| 无约束/超 38 精度 numeric | 任意精度十进制文本，原 typmod/precision/scale 随列绑定 | `numeric(90,40)` 保留 90 位，不能猜一个有限精度；比较/聚合必须用支持该精度的运算或明确不可计算 |
| interval | struct(months int32, days int32, micros int64) | `10个月+3日−04:05:06.000007` = `(10,3,-14706000007)`；1个月不压成30日或总秒 |
| date | struct(state, finite date32) | 有限日期与 ±infinity 分开 |
| timestamp | struct(state, finite timestamp[us])，无时区 | `2026-03-01 12:34:56.123456`；不补UTC解释 |
| timestamptz | struct(state, finite timestamp[us, UTC]) | 原 `+08` 输入导出为同一 UTC 瞬时；PG 不保存写入时区，不能称恢复原时区 |
| 支持元素类型的 PG 数组 | struct(dimensions int32[], lower_bounds int32[], items typed[])，另存完整原数组文本 | `[0:1][3:4]` 多维、非1下界、空数组、数组内 SQL NULL、文本 `"NULL"` 分别保留；按维度恢复结构 |
| json/jsonb | string typed 原值，另有原类型；只做JSON合法性校验 | SQL NULL、JSON null、text null不同；json空白/重复键保留；jsonb只保数据库可导出值，不恢复写入前键序/字节 |
| SQLite 同列混合类型 | struct(storage_class, integer int64, real_bits binary, real double, bytes binary) | integer/real/text/blob/null 同列共存；real bits与double核对；text bytes为SQLite驱动的UTF8值表示，原磁盘字节由原SQLite文件保留 |

无支持规则的 PG 类型（如 inet/domain/enum/composite）、分区表/视图、SQLite 虚拟/生成/隐藏列、超过 v1 可计算日期范围的时间明确拒绝。时间仅接受数据库冻结格式与最多六位小数；timestamp infinity保留明确状态。字段缺失、重复列、未知storage class、类型/精度不符均不得 complete。原默认表达式和约束作为证据保留，查询副本不执行默认表达式或重建触发器/序列，因此副本不是原数据库写入环境的恢复工具。

PG 人工副本的普通列使用对应 PG 类型，保留可支持 numeric/字符/时间 typmod；SQLite 混合列使用 `sqlite_cell` 复合类型，不强制转成一个 PostgreSQL 标量。查询副本在一个事务中建立并逐行读取回核，失败回滚；原 schema/table/column 合同仍以固定历史清单为准。

## 0A/0B 消费信息保留映射

本片按原表完整列导入，不按页面白名单删字段。下表说明可承载的信息，不声称已经导入真实这些表。

| 原数据需求 | 本片可支持的保留/读取方式 | 仍依赖后续任务 |
| --- | --- | --- |
| Core 55日原行、4隔离日 input/解释/阶段/原因/原ref | SQLite全部列、原文件及原manifest/诊断附件；固定availability与refs；默认business scan及ref解析阻断隔离来源，显式audit可核对原值 | H1真实冻结、逐日完整对账；不能以本片人工数据声称55日已导入 |
| Core scale/origin/path与两端SHA、Peer、路径、private-skip/未归属、interval_change_count=NULL | 按各原SQLite全表或人工封存字段读取，保留NULL与原附件/源版本 | H2、真实时点与比较语义验收 |
| event_table全部列，含state/judge/notify/event_info/detail_url | 原列顺序及完整类型保存；原ref独立定位到occurrence，重复行不折叠 | Q2事件身份/语义、H3总表与六类关联全量核验 |
| prefix_outage | prefix/asn、组织/国家/类型/descr/admin、时间/duration、level、三阶段VP路径、event_info及其余全部原列 | 原归属偏差继续限制解释；不能据原字段判责任 |
| as_outage / country_outage | 分母、峰值、原数组/比例、三阶段路径、原国家名/事件信息、可选incident_id_v2和实际存在的生命周期列 | Q2/H3；无原v2关联不合成cohort或恢复结论 |
| hijack / sub_hijack / leak | 攻击者/受害者旧字段名、原数组、父子prefix、level/info、原路径及实际存在的时间列 | 原表没有结束/duration时保持字段缺失；不替泄漏补生命周期 |
| Feature全部旧月表与前窗字段 | bigint/numeric/时间/text等逐列保存，原表名与所有行保留 | H3实际37表以上/以下的完整集合以冻结清单为准；Q4负责五分钟/前窗等兼容解释 |
| INFO、国家原publication/revision/cohort/轨迹/路径/样本、缺项清单 | 原附件字节及原字段可封存；没有取得的表/文件不能补空成功 | H4/H5/H6、81事件真实核验、国家专用读取及语义；本片不导入真实参考或国家制品 |

业务整批 publication、统一 head 仍由 Q1 的唯一合同负责。本片只提供历史固定 component/全字段行流/资格；没有接入 HTTP、九路由或前端，也没有把 Q2、Q4、H1—H6 从总计划删除。

## 资源、验证与复现

默认批次最多1000行/4MiB，单行最多4MiB；默认总行1000万、每阶段总字节10GiB、元数据4MiB、100原表/512列、RSS峰值2GiB、磁盘余量256MiB。source SQL先检查超大行；导出用命名游标/fetchmany，SQLite用有界cursor，Arrow批按行/字节预算读取，不使用整表fetchall。RSS为进程累计峰值保护，含调用者此前的内存使用；超过预算明确失败。DuckDB内存256MiB、单线程、不允许临时盘溢写，超限报错。读取每页也限制行/字节；分页完全检查后才交付该页。没有总运行时长终止参数或自动重试。

人工验收使用2行/32KiB批、8KiB单行预算，包含多块、无PK重复行、超精度numeric、interval月、无时区/微秒、数组下界/NULL、text/JSON/SQL null、SQLite混合类型、原ref缺失/歧义、隔离详情门禁。同一只读PG快照测试在两表导出之间向源第二表并发插行，冻结仍保留开始时的1行，而新事务能看到2行。

独立预期由手写值/序列及源SQL获得，不只比较两次相同codec。PG全部原列规范输出多重集合与固定历史逐行比较；另外手算 interval 三分量、float4实际值、数组维度/下界/元素、时间瞬时、decimal和二进制；SQLite独立查询storage class及值，检查六行重数。实际Parquet schema核查原生int/decimal/时间/binary。移走源SQLite及两份冻结目录后完成重建，再从PG查询副本读取全部字段核对已保存预期。损坏测试含缺块、坏SHA、类型/缺字段、未封存、同身份异内容、中途回读故障、typed原值不符、已complete的Parquet改字节、越界ref与资源限制。

测试入口：[test_historical_import_q3.py](../../backend/tests/historical/test_historical_import_q3.py)。必须由本任务自己的 `initdb --encoding=UTF8 --locale=C` 启动无TCP socket实例，目录、socket与PG数据目录均位于显式私有根；连接器拒绝其他位置和TCP。示例只适用于人工数据：

```bash
uv sync --project backend --frozen --offline
Q3_PRIVATE_ROOT=/Users/botongwu/.codex/outputs/domeye-q3-a-762f \
  backend/.venv/bin/python -m pytest -q backend/tests/historical/test_historical_import_q3.py -s
```

未设置 `Q3_PRIVATE_ROOT` 时仅运行纯类型检查，人工PG集成明确 skip，不能算运行通过。依赖仍用本项目 `backend/pyproject.toml/uv.lock`，DuckDB 1.4.4、PyArrow 23.0.1、DuckLake扩展 `3f1b372`、postgres_scanner `b9fce43`，没有加载旧项目模块、pickle或环境。

Git外根：`/Users/botongwu/.codex/outputs/domeye-q3-a-762f/`。`tests-*.log` 保留全部尝试，包括初始测试SQL转义、Python3.10时区格式、bool标量输出及故障测试SQL语法修订，不抹掉失败记录。最终证据、精确测试数、资源数与停机结果见该目录 `交付索引.json`；其中源原件/封存包、固定ready、独立预期与副本回执均可追溯。实际实现自测不能替代父任务独立 code-review；本片没有真实55天、37表、81事件或生产验收结论。

## 独立复核四项修复（人工增量自测，待原0C增量复核）

固定候选 `b1c55d2f397f899a7cc34b02e4649f2447440ea3` 被独立报告 `1042d02085703476daa18b0d662473b3a0aaf1db` 判为 **Standards REPAIR / Spec REPAIR**，尚未接受。报告在该提交的 `docs/architecture/Q3-A历史冻结独立复核.md`。原34项通过只证明其覆盖范围，不覆盖已发现的四项缺陷；本次在原候选上修复，没有扩展Q1、真实历史或业务head。

### Standards：硬性 [P2] 消费端单行预算

`scan` 对每条规范 raw 编码加换行执行当前 History 的 `Guard.row`，整页按同一实际字节口径累计；加入缓存前检查单行与页预算。页面全部缓存受行/字节限制，最终资格验证成功后才开始交付。一个健康Token在 `max_row_bytes=1` 下交付0行，同Token正常预算可读；两条重复行各自不过行预算、合计超过页预算的反例同样交付0行。预算是当前消费实例的约束，不复用导入时的大限额。RSS保护仍独立覆盖Python/Arrow与双表示的内存成本，不把raw字节数当RSS。

### Standards：判断性 [P2] 每行一次PG INSERT

重建保留一次入口完整验证、固定有界 `_stream` 和同事务逐字段回读。每行仅在客户端 `mogrify` 得到真实SQL值编码，不访问PG做额外类型检测；把已编码的多行按行数和实际完整语句字节双预算合成一次INSERT，含SQL前缀、逗号、类型转换、引号/反斜线/bytea编码膨胀。实际编码行仍受单行预算，全部实际SQL语句字节也受总预算。超限回滚副本事务，不靠原raw字节估算冒充编码后的大小。

返回 `write_statistics` 记录每表INSERT语句数、行数、实际编码字节及最大批行/语句/单行字节，内存仅保留一批。独立于该计数，在本任务PG开启会话级statement日志，手写9行含引号/反斜线文本：3行预算产生3次INSERT；100行但500字节预算产生5次INSERT。89字节原raw行的SQL编码达到177字节；日志中的完整SQL字节与统计一致。没有做真实规模吞吐实验，该原项仍按判断性成本问题回应。原重复行、NULL、SQLite混合值、默认/触发器不执行、脱离原件重建均回归。

### Spec：[P1] 页交付与副本提交前最终资格

复用同一验证Implementation：实际PG登记必须仍为complete且与Token全部字段一致，核对PG实例/数据库/catalog绑定与固定snapshot存在，再检查ready、source-manifest及相关Parquet SHA。页在交付前重新完整验证；副本在全部表 `_verify_copy` 通过后、事务提交前重新完整验证。不是只复核缓存的component返回值。

最终验证采用同一私有PG的 `READ COMMITTED` 事务，对完成登记行 `SELECT … FOR SHARE`。副本锁持有至其同一事务提交；页交付锁持有至该有界迭代器耗尽或关闭。调用方中途停止必须关闭迭代器，不能长期保留未关闭的页；锁会延后其他事务的完成资格更新。此锁是数据不变的资格读取，需要允许行锁的PG连接权限，不声称已适配Web的只读事务/角色。

复用独立真实撤销位置：`_lake` 打开后另一连接改登记为failed，页交付0行；实际 `_verify_copy` 之后改failed，重建抛错且新副本schema不存在，事务已回滚。页锁和副本最终事务锁另用独立连接实际UPDATE验证：锁内返回SQLSTATE 55P03，页关闭/副本提交后相同撤销成功。另测试页面缓存后ready变化，以及副本回读后ready/manifest/Parquet变化，均在出口拒绝并不交付/不提交。

这是同一PG登记资格的锁语义和最终核验点；不声称跨PG/文件系统原子性、实时撤销服务、抗文件拥有者主动改写或断电恢复。文件仍受封存后不改写的约束。公共人工分页的入口/出口完整SHA扫描、重建的入口/出口完整扫描是本片明确代价；内部流不逐页反复全库SHA，不作全历史性能验收。

### Spec：[P2] 有限float文本越界

先区分显式NaN/±Infinity与有限拼写。float4/float8转换后，有限值成为非有限数，或原十进制非零却舍入为零，均拒绝；float4打包溢出也转成明确类型错误。允许目标原类型正常舍入，不用十进制值与最大有限值作过严比较。没有增加逐行PG校验往返。

使用本任务实际PG cast及 `float4send/float8send` 位表示作独立参照：float4 `3.4028235e38`（含负值）、`1e-45`、0.1、负零；float8 `5e-324`、`1.7976931348623157e308`、可合法舍入至最大有限值的 `1.7976931348623158e308`；以及显式NaN/Infinity。拒绝float4 `1e39/1e-50`、float8 `1e400/1e-400`，对应PG均为22003。错值反例更新所有块/整表SHA、规范编码和新冻结身份后重新封存，确保在类型门禁拒绝且没有complete登记，不以坏SHA代替类型证据。

增量证据仍全部位于本任务Git外根；新源包和typed历史由修复代码重新生成，原b1候选Token另实测保持可读与可重建。修复交付索引为 `/Users/botongwu/.codex/outputs/domeye-q3-a-762f/repair-b1c55d2/交付索引.json`，包含最终测试日志、逐项反例、实际批写SQL日志、浮点PG边界位值、旧Token兼容、完整差异及私有UTF8 socket PG停机回执。原独立报告与原证据不覆盖、不改写；仍须原0C独立增量复核决定是否接受。


## 新判断性P2：默认读取批次成本（独立增量后的小片修复）

固定报告 `f02754ae9ef9dc95b3fe20f8727f61d8dc3de067` 的《Q3-A四项修复独立增量复核》已将上节原四项全部关闭：Spec GO；Standards仅因新增的**判断性P2：默认读取退化为逐行**保持REPAIR。本片从 `4c9850c3fc1f942f520e6f38a55ba0f499196808` 修复该新问题，不重新列原四项为未修，不扩大H族、Q1/head、HTTP或真实数据范围。H1—H6准备已接受但未执行的边界不变；本片未访问或操作真实D进程。

原 `min(batch_rows,batch_bytes//max_row_bytes)` 在默认4MiB/4MiB下永远请求1行，混淆了单行准入上限与实际缓冲规划。现在 `max_row_bytes`、`batch_bytes`、RSS及总量阈值保持原值：

- PG冻结在**同一只读一致事务**中取得原投影各列UTF8字节长度之和的最大值B，再用 `6B+5N+2` 作为规范raw表示的保守读取上界；N为列数，NULL、JSON转义、列间分隔与换行有开销，不能只计非空字段。副本回读在其**同一候选事务**中针对实际回读投影获取上界。SQLite同一只读事务的长度聚合按hex扩张及cell结构计入界限。
- 读取请求为 `min(batch_rows,max(1,batch_bytes//实际上界))`，与允许的最大单行资格分开。source SQL只先拒绝明确超大原值，实际raw仍执行统一行/输出批字节门禁；不再用过大的最坏转义倍率提前排除本来合法的近界单行。
- Arrow验证、内部重建流和有界页在固定DuckLake snapshot内用SQL聚合完整typed行的JSON字节长度，仅返回一个最大宽度。JSON仅用于大小规划，**不作为落盘主体或替换原类型**。当前已支持的标量/struct/list采用 `8×最大JSON字节+256×(原列数+4)` 的保守payload界限，计入offset/NULL位图；随后读取真正的typed Arrow批并核对实际 `num_rows/nbytes`。无需给旧manifest增加字段或重导，也不建立通用缓存。
- **读取缓冲与输出批分别限界。** 普通多行Arrow批的实际payload不得超过batch_bytes；一个合法原值展开为raw+typed后可以大于该输出预算，此时独占读取批，实际Arrow payload不得超过该行的保守展开界限。输出封存块、公开页仍按规范raw的真实字节受原batch_bytes限制；PG INSERT仍按实际SQL编码字节限制。整表只有一条很宽的行时，该保守策略可能令其他行也退回单行读取，不声称优化了所有混合宽度分布。
- 上述是输入/输出**payload**界限，不是Python tuple/dict头、Arrow底层预留容量或RSS的分配前硬保证。DuckDB仍限256MiB且不许临时盘溢写；进程RSS/磁盘/总量与类型门禁保留。尺寸聚合增加一次固定版本SQL扫描，仍可能耗时或因引擎内存预算报错；没有处理总时限。Arrow Reader在耗尽、异常和外层close时显式关闭。

### 默认预算下的真实对比

本任务用相同人工值及原默认Limits，在各自新进程运行固定4c源码快照与修复源码；均使用本任务UTF8 socket PG。造数不计入下表墙钟；阶段覆盖冻结、typed导入、固定读取、重建及独立全字段对账。源码快照来自本项目固定4c，不是旧项目/旧运行环境。输入含重复、NULL、引号、反斜线、中文、空文本及bytea。PG计数来自真实statement日志，Arrow计数来自真实Reader请求/返回批，未用自报统计替代。

| 人工行数 | PG冻结FETCH：4c→修复 | PG副本回读FETCH：4c→修复 | Arrow验证/内部流批数，各自4c→修复 | 墙钟秒：4c→修复 | 峰值RSS MiB：4c→修复 |
| --- | --- | --- | --- | --- | --- |
| 17 | 18→2 | 18→2 | 17→1 | 0.550→0.448 | 169.2→174.0 |
| 65 | 66→2 | 66→2 | 65→1 | 0.414→0.456 | 166.8→174.0 |
| 1025 | 1026→3 | 1026→3 | 1025→2 | 0.673→0.648 | 173.4→178.6 |

FETCH计数含最后一次空返回。17/65行的INSERT仍各1次，1025行为2次；没有通过改写计数字段隐藏逐行操作。三组源输入摘要一致，固定历史完整typed/raw、重复多重集合、输出顺序与重建后实际PG全部字段一致。墙钟为单次小样本，运行顺序和缓存不能视作严格控制；17/65行没有可宣称的加速，1025行差值也不外推吞吐。已证明的是默认批化协议和有界正确性，不是全历史性能验收。

邻接验证包含默认预算下17/65/1025行、33列全NULL的紧字节预算、混合小行与近4MiB单行（原始text 4,194,176字节）、SQL编码膨胀、批边界、空表、读取故障与close释放。大行展开Arrow payload超过4MiB时仍受独立展开界限，原始行资格未被缩小；消费预算收紧仍拒绝大行并零交付。b1和4c时期的本任务旧Token均直接读取、全字段摘要与顺序核对、重建，没有重新导入冒充兼容。原63项范围（含最终资格漂移和四项修复）一起回归，最终70项通过。

新增证据根 `/Users/botongwu/.codex/outputs/domeye-q3-a-762f/read-cost-4c/`：`measure.py`、固定4c源码快照、两组各17/65/1025行的真实PG日志/Arrow统计/全部独立期望、`比较摘要.json`。初始SQL别名错误和测量启动器误解析venv符号链接的失败日志保留；最终均使用本项目锁定venv。最终测试、具体制品/旧Token证明、精确SHA、完整差异和私有PG停止结果见该根 `交付索引.json`。仍交0C独立增量复核，不自行宣布Standards GO。

公共离线页入口/出口全SHA仍只是已接受的人工小规模边界；内部重建流没有循环公共分页或逐页全SHA。此次新增宽度聚合亦不证明真实全历史/在线分页可用，没有添加真实源连接器、恢复、缓存或发布平台。

## Q3-B：显式源绑定与公开批量读取（合同先行，现为人工实现待独立验收）

Q3-A 固定提交 `62669e9115dc122e6d2e2d044ce680599002a39b` 的人工载体已由独立复核 `f606cbc0b2621c2f4390775d83936acbbdb1f5f4` 接受。以下是新分支的实施合同，不代表真实历史迁入、H1—H6执行或在线分页已可用。原 fixture API、私有目标门禁、旧 Token、原冻结及导入身份保持兼容。

### 源与目标

新增不可变 PG 身份绑定（system_identifier、database OID）与源绑定（身份、非凭据 URI、版本、Git 外 DSN）。源连接只使用只读 REPEATABLE READ 事务、原固定文本设置、显式表 ACCESS SHARE 锁和 SELECT/catalog 查询；所有表、目录元数据、快照标识和事务时间在同一事务取得。绑定身份不符、缺 pg_control_system 能力、权限不足或不支持类型明确失败，不申请权限、不生成替代身份，不把 DSN/角色/密码写入制品和模块异常。

archive/catalog/query-copy 目标仍须显式绑定本任务私有 PG。新增 retained-history 导入必须提供独立目标身份，并在任何 catalog/副本写入前验证实际身份，拒绝源目标是同一数据库；不会取消 fixture 门禁来伪装真实源支持。读取与重建只依赖已导入的封存载体，不再次连接原源。SQLite 源须显式绑定原件 SHA、URI、版本及关闭写入声明，并通过原 SHA/WAL 门禁；独立文件冻结不声明与 PG 同一事务。

新增 `retained-history` 合同保留原 publication/ref/availability 描述，Unknown 与隔离不得补成 available；旧字段及原身份不重写。源 DSN 不进入 manifest。Q3-B 只用本任务人工多库和非超级只读角色验证这个非 fixture 合同，不绑定任何真实库或旧项目产物。

### 公开批量 Reader

公开会话固定 Token、表集合、版本和用途，复用内部 typed 解码流。按表索引、原 occurrence 顺序交付有界批；输出 raw 字节、行数、总扫描量、元数据和 RSS 各自受显式预算约束。Arrow 缓冲保留现有独立尺寸界限，不声称 Python/Arrow 分配前硬 RSS 保证。

会话入口、正常耗尽出口各完整核验一次资格和文件，次数不随输出批大小增加。会话内所有数据暂定；正常耗尽后才产生带 Token、声明表集合、完整表计数/摘要、执行代码摘要和资源统计的完成回执。全归档与声明子集分别标明，不能把子集称为全归档完成。提前关闭、异常、资格/文件/执行代码漂移均无完成回执；退出必须显式关闭 Arrow、DuckDB 和 PG/资格锁。出口在最终资格锁内校验执行代码，回执仅证明该核验点，不承担跨 PG/文件系统原子性或实时撤销。

旧 `scan/component` 语义不变；新 Reader 不循环调用 scan，不做通用缓存、恢复、业务分页、JSON/gzip集合解释、HTTP 或 head 发布。验证先覆盖人工多库冻结→导入→新会话完整读取→移除源→重建，完整核对 raw、typed、重复次数、嵌套顺序、occurrence；再测默认17/65/1025行的实际 SQL/Arrow 成本与不同输出批大小下的 SHA/资格次数，以及旧 b1/4c/626 Token、宽行、空表、NULL、精确类型、漂移和提前关闭。最终固定提交与完整差异交父任务独立复核。

### Q3-B实际 Interface

下表是本次实现，不改变上文 Q3-A 的固定 Token 与 fixture 入口。`PGSource.dsn` 必须来自调用方显式提供的 Git 外连接配置，需有 host/port/dbname/user；不允许空串、环境默认源或 service 自动发现。对象 repr 和源连接/权限异常不输出该 DSN。URI 是不含用户名、密码、查询参数或 fragment 的来源标签，不能把连接串当 URI。

| 入口 | 显式输入 | 结果与资格 |
| --- | --- | --- |
| `PGIdentity(system_identifier, database_oid)` | 预先绑定的实例/数据库身份 | 不根据当前连接自行替换预期身份；`read(connection)`仅供已授权绑定核验使用 |
| `PGSource(identity, origin_uri, source_version, dsn)` | 源身份与独立连接 | 只读 REPEATABLE READ；身份探测与后续冻结同事务；无权限则 capability error，不申请授权 |
| `freeze_postgres_source(source, tables, destination, *, target_identity, publication, availability, references=(), attachments=(), limits=Limits())` | 显式表集及独立目标身份，必须给原publication和原可用性 | 新 `retained-history` 包；缺权限、RLS、继承/分区、未知类型、预算、中断均不封 complete |
| `SQLiteSource(origin_uri, source_version, sha256, closed_immutable)` 与 `freeze_sqlite_source(..., binding=..., publication=..., availability=...)` | 原件绑定SHA及关闭写入声明 | 验证原件及副本SHA和WAL；标明 `independent_closed_file`，不声称属于PG事务 |
| `History(target_dsn, private_root, limits, *, target_identity=...)` | 独立私有目标身份 | 每次PG连接及DuckLake attach前核验；retained-history导入/重建拒绝省略绑定或使用源数据库；原fixture构造兼容 |
| `History.bulk(token, *, table_indices=None, purpose='audit', batch_rows=None)` | 固定Token、可选有序非空唯一表索引、用途、输出批行上限 | 返回必须关闭的BulkReader；None表示全部声明表，显式索引即声明子集，哪怕恰好列出所有表也不冒称whole_archive |

publication 是保留的原描述，不赋予业务发布资格；原 freeze/import 标识可随原 publication 原样封存，新的源冻结/导入各自有新身份。本次人工值验证这两个原标识没有被覆盖，同时六个旧 Token 的原 source_freeze_id/import_id/snapshot 全部保持不变。`unknown` 可用性与 ref 在 retained-history 合同中保留原因；fixture 的原状态集合不扩大。原数据隔离仍禁止 business 读取，audit/私有重建不产生业务 head。

BulkReader 是单次迭代会话，推荐 `with history.bulk(token) as reader:`。每个批为 `{qualification: 'provisional', table_index, rows, raw_bytes}`；每行含 `occurrence`（冻结/schema/table/block/row/ordinal）、原行摘要、完整 `values` 与按原列名组织的 `typed`。批只包含一个原表，表索引递增、表内 ordinal 递增，不消除重复；嵌套数组的维度、下界和元素顺序均保留。消费者自行处理暂定数据，不能在读完前据此发布。

`reader.receipt` 在正常耗尽之前为 None；退出/异常会关闭实际Arrow Reader、DuckDB连接以及最终资格PG连接/锁。状态为 pending→provisional→complete，提前close为incomplete，失败为failed。正常耗尽后的回执返回独立副本，包含 `history-bulk/v1`、Token、whole_archive/declared_table_subset、表集合、逐表计数/内容SHA、原可用性、导入时代码与本次执行代码摘要、Limits、批数和资源统计。`receipt_sha256` 是去掉该字段后规范JSON的SHA，用于校验回执内容，并非防伪签名或业务发布许可。

`scan_plan` 显式列出选中表的原值行数/字节、每表一次宽度聚合和一次有序扫描、两次完整资格核验及每次绑定Parquet字节。总量预算约束声明表集合及实际原值消费，在宽度聚合前先检查已封存的总量；它不是全部SQL/文件系统物理IO累计计量器，也没有新增硬总时限。每次资格核验的文件字节另受原max_total_bytes门禁。代码文件在入口/出口各哈希一次；这是本次代码文件集合的版本漂移检查，允许旧Token由新实现读取，不强制等于旧导入代码。资格撤销/文件变化/代码变化/消费预算变化在出口拒绝，先前批仍只能是暂定。

### Q3-B人工验证与成本

最终新检查 **31 passed in 8.17s**，补充300字节输出批分界与会话预算漂移检查 **1 passed, 31 deselected in 0.99s**；相邻原检查 **19 passed, 51 deselected in 4.91s**，覆盖fixture完整typed/raw、重复、固定版本、重建、候选失败、隔离、原页门禁与最终锁。没有再次运行未变化的全算法或前后端套件；未作真实源吞吐、在线业务或产品验收。

新检查使用本任务原UTF8无TCP socket实例下独立源库/目标库和人工非超级角色。源角色只获得显式schema USAGE、表SELECT及pg_control_system EXECUTE；通过撤销该函数的人工授权、普通表SELECT缺失、错身份、源目标混用、RLS/继承/未知类型及中断反例证明拒绝路径。源连接的CREATE TABLE被实际READ ONLY事务拒绝。另在首表冻结后经人工管理员并发插入第二表，冻结仍保留原第二表1行，并查询到真实ACCESS SHARE锁；模块没有执行这些测试设置/并发写操作。

完整闭环将独立人工源库DROP，原冻结包移到保留目录，再以只持目标DSN/身份/Token的新History读取并重建；PG独立SELECT逐字段对账一致，重复次数、schema同名表、NULL、原JSON文本、bytea、精确numeric/time、数组下界/嵌套次序及occurrence保持。独立SQLite原件删除后同样成功，Unknown仍未变available。旧b1/4c/626各PG和SQLite共六个旧Token，以新bulk和原scan完整比较raw/typed/occurrence并实际重建，没有重新导入冒充兼容。

| 原始人工行数 | 源冻结/副本回读FETCH（各，含结束空批） | 实际Arrow请求/返回行数 | 单会话完整载体SHA调用 | 资格核验 | PG实际日志语句数/固定DuckDB读取SQL |
| --- | --- | --- | --- | --- | --- |
| 17 | 2 / 2 | 请求958，返回17 | 6 | 入口+出口，共2次 | 81 / 2 |
| 65 | 2 / 2 | 请求958，返回65 | 6 | 入口+出口，共2次 | 81 / 2 |
| 1025 | 3 / 3 | 请求954，返回954、71 | 8 | 入口+出口，共2次 | 81 / 2 |

每个源都分别使用输出批1、17、1000行；上述SHA、资格、PG语句及DuckDB宽度聚合/有序读次数不随输出批数增长。6/8个载体SHA是两次ready、source-manifest及1/2个Parquet的实际文件哈希，另有本模块6个代码文件各入口/出口一次，共12个小文件哈希；未计入载体列。81条是当前真实PG statement/execute日志计数，含目标预检、目录查询、事务与DuckLake元数据，不能称为只有2次SQL或免费读取。源计数/宽度聚合也仍各有一次全表扫描；这是消除逐输出批全归档重验的证据，不是低成本在线分页。

单进程、同一运行次序的小样本墙钟约0.05—0.08秒/批流会话，RSS受此前测试峰值影响，不外推吞吐。近4MiB text人工行4,194,048字节展开Arrow为8,388,293字节；该表保守退回单行Arrow读取，输出仍按规范raw字节限批。RSS为进程峰值检查、不是分配前硬上限；Python对象、暂定输出批与Arrow缓冲会同时占内存。接收方若自行累积所有批，内存由接收方承担。

证据根为 `/Users/botongwu/.codex/outputs/domeye-q3-a-762f/q3-b/`，本次最终制品在 `acceptance-83470c8157964c60bdaf82c858376d9f/`。其中包括公开完成回执、人工绑定身份（不含源DSN/role）、PG源移除闭环、SQLite独立文件回执、同快照并发写证据、六个旧Token兼容、宽行资源，以及cost_17/65/1025各源/副本/三个输出批的实际PG日志、Arrow请求/返回与逐字段对账。初期断言修正及失败日志单列保留，不替代最终结果。精确提交、完整差异、最终日志SHA、隔离与PG停机回执汇总于该根 `交付索引.json`。Q3-A分支和原证据未覆盖；Q3-B仍交父任务独立复核，不自行宣布Standards/Spec接受，不执行H1—H6。

最终审查将源能力探测的current_setting显式限定为pg_catalog，避免连接配置改变名称查找；随后仅重跑源权限/身份和同快照并发两项（2 passed, 30 deselected），并用最终源码重新导入的人工Token完成公开Reader，回执中的导入/执行代码SHA与当前六个模块文件逐一相等。该增量不重复计入上述32个新增检查。


## Q3-B独立P2响应：预算漂移在读取前拒绝

固定报告 `aba26f4bbbb7f89f766f893bce1414047e8a23c0`（《Q3-B来源隔离与批量Reader独立复核》）对候选 `ec1778f9c746ad58b85a2c27f02baaaa5cc17557` 给出 **Standards GO / Spec REPAIR，仅1项P2**。Q3-A已关闭项及Q3-B其他通过保持。本次只修复该P2，原候选、报告及制品保留，没有推进真实H或下一片。

问题是Python的for先调用下一次取行，原循环体才检查History.limits；而构造后首次启动、表间切换和最后批暂停恢复也缺前置检查。所以“最终无receipt”虽正确，仍不能阻止漂移后已启动的读取。现在把相等性检查集中为纯内存 `_check_budget`：首次代码/组件资格读取前、每个原表流启动前、显式 `next(rows)` 前后、两种批yield恢复后，以及进入最终资格读取前/锁内分别核对。仍按构造时Limits的值相等判断，不添加缓存、重扫、PG查询、总时限或重配置平台；原最终资格、文件与执行代码检查保持。

新增独立测试文件 `backend/tests/historical/test_historical_import_bulk_budget.py` 只绑定本任务自己的健康b1旧PG Token（原表4行、1行、空表），不重新导入、不接审阅者数据库。探针对component、实际宽度SQL、真实Arrow Reader创建/next/close及DuckDB close逐一透传，记录调用次序；不是以模拟返回替代读取。对same_table将原读取批设为1行，确保下一步会跨实际Arrow批。

| 用例 | ec原代码在limits_changed后的真实读取 | 修复后的真实读取/关闭 |
| --- | --- | --- |
| 构造后、首次next前修改 | component_entry→width_0→arrow_open_0→arrow_next_0 | 无调用、零暂定批、无receipt |
| 首表最后批暂停后修改 | width_1→arrow_open_1→arrow_next_1 | 无新调用，原1个暂定批不变，无receipt |
| 同表逐行Arrow批暂停后修改 | arrow_next_0 | 仅arrow_close_0与database_close，无新取批，无receipt |
| 声明单表最后yield后修改 | component_exit | 无最终资格读取，无receipt |

四个反例均为failed且实际Reader/数据库打开数归零。固定起点max_total_rows=1的对照仍允许一次component以取得清单，随后在任何width/Arrow之前按声明总量拒绝；它与“构造后漂移”是不同路径。

正常固定预算以输出批1、2、1000行分别完整读取同一旧Token，原值摘要/ordinal相同并获得完成回执。三种输出批的实际调用轨迹完全相同：入口/出口component各1次、三个原表各一次width，两个非空表各1个Arrow数据批，空表无数据批（next还记录结束空返回）；载体SHA各10次，真实PG statement/execute日志均164条，含目录/事务/读取连接成本，不因输出批数增加。没有把这些元数据往返省略为“只有2条SQL”，也没有做真实规模性能验收。

实际不同测试数为 **8项**（4个漂移反例＋1个固定小预算对照＋3个正常输出批参数）。修复前在固定ec产品代码实跑 **4 failed, 4 passed in 5.27s**；修复后 **8 passed in 3.35s**，补齐正常PG日志探针后的最终同8项为 **8 passed in 4.15s**。这些是同8项的前后执行，不累计成24项；没有重跑完整32/19/70矩阵或把旧结果当本次回归。

证据根 `/Users/botongwu/.codex/outputs/domeye-q3-a-762f/q3-b/budget-ec/`：前后每种情况的逐调用JSON、固定Token、provisional计数/状态/无receipt、真实资源关闭数、三种正常批流回执及实际PG日志。根目录日志为 `q3b-budget-before.log`、`q3b-budget-after.log`、`q3b-budget-final.log`；固定新提交、精确父、完整差异、各文件SHA及本任务PG停止回执汇总在本证据根 `交付索引.json`。代码差异只在reader.py，另有定点测试和本中文响应；修复交原308e/0C独立增量，不自行宣布Spec GO。


Q3-B `930675e` 已由独立增量 `df5cf82bef2855fc85633f00c07ee00e54c2069e` 接受，并经 `dc931b` 中Q3B集成报告确认。后续仅设计的 [Q3-C历史集合与JSON语义迁入](Q3-C历史集合与JSON语义迁入设计.md) 单列待审；未实现、未起PG或执行真实H，旧运行方案未修改。
