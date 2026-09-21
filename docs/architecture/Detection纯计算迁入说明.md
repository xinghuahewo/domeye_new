# Detection 六类纯计算迁入

记录日期：2026-09-13。当前状态：**人工保存观察与参考→冻结子进程→Detection→typed PG/Parquet→固定版本只读重建已实现并通过人工测试，待集成独立复核；真实D、全天吞吐、业务发布和产品验收未实施。** 原纯计算阶段和后续增量分节记录；当前接口以末尾“人工存储集成”一节为准。不改变[阶段1观察实现](阶段1观察与有限状态设计.md#7-独立复核修正与二阶段回放当前实现)或现有 Web。

## 来源与范围

基线 `ecbc03fd226bb0adefa164a9d593230e5028c71d`，独立分支 `codex/detection-computation-migration`。旧参考为 `39578feca69606ab14ee9993be41769aacc6f831` 的 BGPDetection、BGPRib、BGPInfo、BGPOutage、BGPHijack、BGPSubHijack、BGPLeak、country_outage_v2 及纯参考 helper、数据库写入参数定义。读取位置为 `/Users/botongwu/Documents/domeye/project-docs-work`，没有导入、执行旧模块、pickle、环境或旧进程。

设计遵循 `codebase-design`：外部 Interface 是显式输入、文件边界、计算输出和状态导出；规则、阈值、活动事件与兼容索引留在 Module 内。没有通用插件、数据库 Adapter、checkpoint 或发布框架。规则证据来自[旧新映射](旧新逻辑状态数据映射.md)、[0A 逐项盘点](阶段0A盘点/M06-BGPDetection.md)和[参考核对](异常计算参考资料核对.md)，完整源码出处保留在迁入方法的中文 docstring。

## 实际接口与调用顺序

实现目录：`backend/data_pipeline/analysis/detection/`。

| Interface | 必须显式提供或返回的内容 |
| --- | --- |
| `DetectionEngine(DetectionSeed, ReferenceBundle, DetectionScope)` | 新 run/source/collector/input/computation 身份、UTC 半开窗口、RIB 基线观察及引用、旧编号初值、完整参考解释映射/原行/行定位；初始化明确标为 RIB 冷启动，不宣称窗口前无活动异常 |
| `begin_file(FileBoundary)` | 来源版本、文件身份/时间、九种旧月表引用；可附真实 slot_ref，但本计算不认证其连续性 |
| `consume(DetectionInput)` | 观察身份、来源及版本、collector、原记录与真实 Peer 引用、legacy VP、规范前缀/可选原前缀、A/W/STATE、UTC 时间和原路径；可选规范前后态引用，只作为证据，不推进规范态 |
| `finish_file()` | 对当前文件执行 Outage 尾刷新，返回完整变更值或结构化 failed；必须关闭当前文件才可切下一文件 |
| `export_state()` | 全部旧业务态、兼容投影与树、候选/recent/incident/episode、所有记录/修订/判定、参考原件引用、编号身份映射与质量；这是可序列化的导出，不是可恢复 checkpoint |

有效 UPDATE 的次序固定为：保存 Detection 私有前态 → 推进私有兼容投影 → Hijack → SubHijack → Leak → Outage。文件尾单独刷新 Outage。没有 MRT 解析、公共 RouteState 写入或普通/IR Feature 投影借用。

输入过滤保留 STATE、默认路由及含 `{` 的 A 排除；RIB 中的 AS_SET 原文本仍进入旧投影，W 仍按旧规则处理。原观察和真实 Peer 引用独立保存，旧 VP 同 ASN 覆盖不代表真实 Peer 合并。调用返回的 `compatibility_transition` 保留前后起源与 VP 路径；来源身份不能只用内容哈希冒充。

## 计算、状态与中间结果覆盖

| 类型 | 已迁入和导出的行为 |
| --- | --- |
| 前缀中断 | 973 项唯一黑名单；组织过滤；reachable/unreachable VP 集合、总 VP 至少3；不可达比例≥1开始，可达比例≥0.4恢复；开始时归属与完整静态属性、前/中/后路径、等级及原文、旧编号及开始时月表 |
| AS 中断 | normal/outage prefix 集合、严格>0.2开始、严格>0.85恢复；总前缀数/最大中断数/最大比例独立更新；路径取样所选前缀另存 `legacy_selected_prefix`；`if_change` 尾刷新、等级、静态属性与活动态 |
| 国家中断 | 当前可达 country_v2；首次触发时固定 cohort、affected/visible/unknown/dynamic、candidate/recent、incident/episode/milestone/peak、legacy_peak_projection；每次旧触发另存 `country_reduction`，未确认候选也留历史 |
| 前缀劫持 | 旧数量转移及初始化状态；全部 MOAS 集合/原起源/成员静态属性/规则/等级/前中后路径；MOAS 中间结果与成立的 hijack 各有独立修订引用；基线 MOAS 不伪造 onset |
| 子前缀劫持 | 最近父前缀、IPv4差≤8/IPv6差≤3、原父/子集合及旧集合字符串、各自静态属性、过滤原因、等级、开始/结束/月表；新增原集合字段不替换旧首成员文案 |
| 路由泄漏 | A 专用、相邻重复压缩、长度>3、反向多triplet、关系方向与稳定度<0.2；先等级再门禁过滤；低等级现象标记规则未评估，拒绝现象仍设置永久 seen，W 不清除；所有现象及完整三方属性/原路径/VP均导出 |

旧数据库写入调用替换为 `capture` 完整值输出，名称保留用于定位；不是 SQL 执行回执。MOAS 原注释掉的写库现象通过业务修订保留，Leak 在删除临时现象前保存完整记录。旧活动字典照原流程清除，但历史修订不会随之删除。旧通知只生成意图值，代码中不存在发送实现。

新 `incident_id` 绑定 run、source、类型、对象、旧开始时间、起始观察上下文和必要序号；`revision` 单独递增。`legacy_ref` 保存 source、对象、旧编号、开始时间及开始时表引用；`current_legacy_table` 另存旧计算当前选中的表。国家v2每次投影实际重取当前月表，原值留在legacy，不覆盖新引用中的开始表。旧按 month 而非 year 比较的编号规则保留，不拿旧月编号作新唯一身份。

所有新结论统一为“指定观察范围内的启发式异常候选”。旧“攻击者”“劫持”“全国中断”等字段/文字只在 `legacy` 中保留。空结束时间不升级成 ongoing，Leak 明确 `end_state=not_recorded`，其他未闭合记录为 unknown。旧人工 judge/notify 字段属于未来历史库迁移，不凭空为新候选造审核或通知历史。

导出使用 `$set` 保存集合，`$map` 保存含非字符串键的映射，`$datetime` 保存旧 datetime 值；整数键和字符串键不合并。旧列表及旧文案保留其实际次序；稳定集合编码不把任选首成员升级成可信事实。

## 参考解释与质量

`ReferenceBundle` 接收十类内存解释映射以及原行和定位，覆盖 BGPInfo 的11原件用途；两个重要前缀原件在旧重要前缀映射中合并。该接口只接受人工/已明确解释的输入，不重新实现公共参考存储、读取整份实际参考或声称其历史适用有效。

参考 version、原始行/JSON位置与完整未裁剪映射随导出保留；判定记录引用该参考行索引和固定源码函数位置。BGPInfo 五类关系列表及域名文本使用 `ast.literal_eval`，元素原类型保留，失败原文与原因明确，不执行资料。原 helper 的空值、下划线、组织文字和 int/string 键差异照旧保留。原参考有效期默认为 Unknown。

Hijack 保留无效/私有/组织→route扩展→双向import/export→provider/customer→peer→sibling/同组织→历史pfx2as→AntiDDoS→外部peer/sibling的实际先后。SubHijack、Leak和Outage使用自己的旧规则，不统一私有范围。Triplet缺项的旧有效值0附 `measurement_state=missing`，不冒充已观测稳定度0。

计算出错记录失败阶段、异常类型/原因和原观察，状态为 failed/partial，阻止继续输入；不执行原逐行或文件尾吞错后继续。全部输出的 `publication_eligible=false`，成功计算也不是发布成功。构造时非法参考显式抛出包含字段/原文/原因的错误，不创建可运行对象。

## 运行修正与保留差异

| 项目 | 本次处理及依据 |
| --- | --- |
| D09 文件尾 UnboundLocalError | 旧 AS 变化分支在后续 country 循环赋值前访问 country/country_outage_id。迁入后的 AS 分支只判断已绑定的 AS 表引用，输出 AS 详情及总表变化并清 `if_change`。最小 Python 反例和独立字段预期均有 fixture；没有捕获异常后伪称完成 |
| 国家旧总表缺实参 | 旧 BGPOutage 国家开始路径给 `event_start` 少传必需 `attacked_as`，其数据库函数签名无默认值。纯计算只捕获原参数，行和导出 `legacy_projection_quality=partial`，保留完整国家/cohort/受影响集合。后续存储接缝必须显式解决“不适用/缺失”字段语义，不能任选ASN补造；未调用旧函数，不称旧写库成功 |
| D01/D02 私有投影差异 | A仍累加旧origin，W按剩余路径移除；最后路径撤回时树删除，即便prefix_as残留。规范层原事实/未知起源不受该兼容逻辑影响 |
| D11/D12 身份与父变更 | 保留旧月份编号和集合文案；新身份独立。父变化不主动重扫子前缀，持续True不修订；没有悄悄实现新检测规则 |
| D13/D14 国家状态 | 明确 `39578fe-call-count-second-initial-peak-sticky-recovered/v1`；两次ratio>0.03确认，起始peak取第二候选，不验cadence；旧“five_minute”字段是legacy原文。live Prefix×VP unavailable使恢复不可达；合成observed fixture验证fully_recovered后仍留同incident/episode，不宣称支持新一轮恢复协议 |
| 旧不可达国家分支 | 其单次>0.03开始、normal>0.98恢复及独立极值逻辑仅由固定旧源码及0A记录解释，不在本模块激活 |
| 旧持久化形状 | 完整计算字段及调用参数保留，尚未做DDL行构造、decimal舍入或SQL执行；MOAS历史拼写 `eve_vp_pahts` 的落库映射由存储适配处理，不能凭fixture声称旧数据库逐列等价 |

## 验证与下一步边界

测试命令（本项目锁定 Python 3.10 与依赖）：

```bash
cd backend
uv run --locked pytest -q tests/detection/test_detection_computation.py tests/detection/test_detection_country.py tests/detection/test_detection_rules.py
```

本切片 fixture 覆盖：2/3/5VP与恢复边界；AS严格阈值和独立三极值；未知W、AS_SET基线/宣告过滤、旧残留与树删除；跨月/跨年关闭及编号；Hijack 0/1/2/3数量转移与等量换成员；SubHijack双栈长度、最近父级、持续True；Leak长度/压缩/多triplet/稳定度/等级/拒绝后seen；国家高→低候选、Unknown、动态人口、非等间隔和同时间调用、live不能恢复、合成恢复后再发；参考顺序/类型/原文安全与静态无副作用检查。

静态比较迁入方法的 `self` 赋值目标：Hijack 60、SubHijack 40、Leak 46、Outage 112，除连接改为输出对象外未发现遗漏。这个计数不是字段数量、分支全覆盖或数据等价证明；原数据库编号读取、计时、不可达国家方法等不在比较集。最终本组为 **69 passed**；未执行真实数据或数据库集成测试。

尚未适配：真实规范观察→DetectionInput/基线构造、参考整行→本解释映射、运行批次结果/PG工作态/历史湖写入、月表字段映射、事件总表缺项处理、发布、网页、国家增强页面和恢复。当前保留内存审计与活动记录摘要比较，未验收全天规模性能或内存预算；真实集成前须由协调安排有界输出/批量适配，不能将fixture耗时当生产吞吐。

没有新建数据库、读取真实数据、发布、push、操作Issue、启动服务或恢复旧试点。真实全天六类是否出现、旧历史结果等价、业务发布和产品验收均为 Unknown。

## 独立复核后的最小输入合同修正

独立复核 `fa0f4c91319f41134ceec7605eac850416d360ab` 对 `a38b1d0` 的算法/业务状态给出 GO，入口合同提出两项 P2。本次仅修复 `DetectionEngine` 的输入校验，不改六类算法、参考解释、国家既有行为或后置存储接缝。

- `consume` 先完成既有观察身份校验和 A/W/STATE 枚举校验，再应用合法输入的默认路由、AS_SET、STATE 过滤。非法 action 在任意前缀下均产生 input 阶段 failed，兼容投影及业务态不变，后续 consume 被拒绝；文件尾如实标 failed，不转成功。
- `begin_file` 在接纳边界前验证 file_id/source_version 为非空字符串，observed_at 为有效带时区时间，并处于 scope 的既有 UTC 半开窗口。非法边界抛 ValueError，`file_boundary`、月表引用、业务态及审计均不提交；没有可供 consume/finish_file 使用的半初始化文件。调用方可修正后显式打开有效边界。此校验不认证文件真实性、等间隔槽、文件内消息时间一致性或 Session 连续性，也不新增恢复语义。

按已授权公开 Interface 使用 TDD：action 反例首次为 **2 failed / 1 passed**，两栈默认路由均错误返回 filtered；修复后 **3 passed**。文件边界反例首次为 **9 failed / 3 passed**，修复后 **12 passed**。另验证有效 A/W 默认路由、A 的 AS_SET、STATE 正常排除后仍可继续，必需 Peer 引用不能被过滤掩盖，以及合法普通文件、带偏移时间、窗口起点和终点前时点。原跨月业务 fixture 保持通过。

新增 `tests/detection/test_detection_input_contract.py`，与上节原三个测试文件一起执行：**92 passed（原69项＋新增23项）**。本次错误边界采用“提交前拒绝且状态不变”的公开约定，不生成虚假的成功计算回执。尚待原独立复核任务增量确认，未开始 Reader、真实数据、存储或发布。

## 下一阶段：流式计算接缝（已实现，集成尚未完成）

协调在原 `c8d72a8` 双轴 GO 后授权参考解释、人工观察适配及隔离存储集成。已合入公共参考 v2（本分支 `337de43`）和固定快照 Reader（本分支 `785ac4f`，共享原提交 `2fbd7b3`）；其接口见[观察只读消费接口](观察只读消费接口.md)。下面只记录当前已完成的计算接缝，不代表整段人工 PG 集成完成。

新增 `StreamingDetectionEngine` 共用六类算法和原调用顺序，接受同步 sink。输出行先补齐修订、身份、原表引用、质量与通知意图字段，再提交 sink；返回 `EmitReceipt(start,end)`，自身不保留审计列表。单条输出默认限制 8 MiB，超限或 sink 异常后明确失败并禁止继续输出，不截断；sink 必须同步消费或自行提供有界缓冲。原 `DetectionEngine` 仍保留内存输出，供人工对照使用。

事件字典仅跟踪本次被取用或替换的可变记录，按旧类型/对象/编号插入顺序生成修订；不在每次 UPDATE 中附加扫描所有活动事件。取用标记是保守候选，原摘要仍去除无变化修订。嵌套字段继续操作原记录，避免包装嵌套容器改变旧共享引用行为。Leak 等临时记录仍在删除前发出。国家每步 reducer 输出照旧保留。

`StreamingSeed` 接受一次遍历的 RIB 输入及明确计数，消费后只保留基线引用、初值和初始化声明；实际计数不足/过多均拒绝初始化。原内存 seed 仍用于 fixture。此入口的计数检查本身不替代共享 Reader 的 SourceEnd、来源顺序和版本核验。

当前验证：原 92 项加 7 项公开接缝测试，共 **99 passed**。新增人工对照逐条比较输出字段与修订顺序、最终全状态，覆盖前缀/AS/国家、MOAS/劫持、子前缀和删除前 Leak 现象，并检查国家 reducer 记录；另验证输出超限不可续写和一次遍历基线计数失败。尚未证明全天内存上界：兼容投影、旧算法活动状态、编号/去重索引仍会增长，后续运行必须施加 RSS/磁盘保护；`export_state()` 仍是人工对照接口，会复制全状态，不作为正式运行的持久化入口。

待继续：保存参考整行的解释映射、Reader 到 DetectionInput 的版本化适配、typed PG/Parquet 输出及逐项状态导出、持久 ready 回执后准入、固定版本只读重建、隔离人工数据库集成。未读取真实资料或使用其他任务数据库。

## 人工存储集成：当前接口与验收边界

本节取代前文“尚未适配”中已完成的接口项；前文纯计算测试数和范围是其当时冻结记录。原 `c8d72a8` 保留；随后计算流式接缝 `7e2650b` 保留。共同前置已分别固定为：参考 `726481f`（本分支 `337de43`）、Reader `2fbd7b3`（`785ac4f`）、真实字段解码 `2b60b14`（`d7ed1e8`）、冻结接口 `824c4df`（`3dc58c7`）、Reader 资格修复 `828211e`（`990de82`）。公共 Reader 和有限冻结接口的独立 GO 不替代本集成的独立复核。

### 调用与版本绑定

入口为 `scripts/pipeline/detection-frozen-run.py REQUEST.json`。父入口在导入业务之前，经[有限冻结执行接口](有限冻结执行接口.md)复制明确源码组、锁定依赖清单，并以 `-I -B` 启动只读源码快照内的新解释器。`identity.py` 列出 Detection 全部模块、公共 Reader/store/decoder、共享冻结 helper、入口与 Python 锁定依据；开始和所有必需输出完成之后再次核验实际执行来源。未指定或被 `-I` 忽略的 Python hash seed 明记 Unknown，不声称可恢复随机种子。

请求显式给出 observation_dsn、input_run/input_snapshot、ordered_sources、11类参考 source_id/expected_rows、reference_version、scope、逐来源 FileBoundary、独立 detection_dsn、新 output 和资源上限。凭据与请求均在 Git 外，不从环境或旧项目发现配置。一个明确 baseline 后接 UPDATE；每个边界必须匹配源身份和精确快照。scope 的 collector/input_version 与 Reader 清单实核，不能只贴调用者标签。没有跨日活动事件恢复、隐式 carry-in 或自动数据生产。

`ReferenceView` 仅消费 Reader 已保存整行，以新隔离目录重建临时表格并使用本项目锁定 pandas 解释原加载的类型/首末条规则；JSON重复键后值、嵌套对象顺序证据和数组元素类型保留。五类关系列表只用安全字面量解释。11源缺失、来源/版本/计数不符均失败。整行附加列和选中行定位保留，原文通过 source_id+固定snapshot+row/location 回溯。解释映射一次移交 Engine，避免复制整份参考和每次事件复制映射。CSV解释仍需要一个来源的 DataFrame，受进程RSS限制；不是已验收的全量真实参考内存方案。多sheet缺少确定首表次序，以及保存公式/日期尚缺完整旧读取类型合同，当前明确拒绝，不猜缓存值或静默挑表。历史适用性仍 Unknown。

`DecodedDetectionInput` 独立扩展原人工输入，保留 decoder_version、direction_rule、received/sent（RIB为snapshot）、两端点、message/element/路径引用、path_id及存在性、epoch/microsecond/精度、原前缀字节hex、属性/AS4/起源解释质量和旧固定工具解码差异。AS_PATH和Peer取真实MRT字段；LOCAL双向观察继续推进 Detection 私有兼容投影。规范前后态没有提供则为None，不从该兼容投影伪造规范事实。零元素STATE/EOR、消息质量和空源均单独持久化；未压缩为每批末路由状态。公共解码差异的具体证明范围以[观察只读消费接口](观察只读消费接口.md)为准，旧历史工具版本 Unknown。

### Typed 输出及准入

`DetectionStore` 只在调用者新建的独立 PG/输出中使用。PG `detection.runs` 登记 candidate/complete/failed 和固定快照、scope、身份；PG `detection.records` 与湖表 `det_<run>.records` 保存共同记录族：顺序号、record_kind、incident_id/revision、event_kind、subject_type/subject_key、带时区微秒 observed_at、规范角色及规则/原因、完整legacy详情、evidence和其余输出属性。六类、MOAS、Leak现象、country每步reducer和原写入意图共用此结构，未给每类另做发布框架。

PG与湖的 `state_entries` 按 family/attribute/键/容器类型保存工作态，覆盖四个算法模块、兼容前缀/VP/起源/树、编号/修订/摘要、seen、错误与基线/参考身份。顶层集合、列表和映射逐项发出，空容器另有声明；不先 `export_state()` 或复制整套状态。嵌套单个事件或对象仍完整保存，超限明确失败。恢复函数未实现；`reconstruct_state` 是显式只读人工审计，会在调用者内存中重建全状态。

写入顺序为：完整记录的有界同步输出 → 全部来源SourceEnd与计数齐备 → 算法文件尾完成 → 逐项工作态写完 → PG/湖行数核验 → 复验源码执行身份和已绑定上游run/snapshot/source/参考资格及回执 → 必需 `ready.json` 文件和目录fsync → PG candidate改complete。ready只是可提交回执，PG是完成状态权威。提交响应不明时重新查询PG，不把已完成运行改成failed。未做跨数据库事务/持续撤销传播平台。

`read_records`（原记录重建）、`read_revisions`（规范修订）、`read_decisions`（包括无事件的判定）、`reconstruct_state` 都要求明确run和snapshot；错版、candidate、failed拒绝，不追latest。直接API人工模式标 `synthetic-fixture-api`，默认消费拒绝；人工审计须显式 `allow_synthetic=True`。冻结子进程模式绑定实际源码/包与版本后可被正式读取，但人工数据依然只是人工数据，不因此成为真实生产接受结果。

当前默认计算RSS 1GiB、磁盘余量128MiB、单记录/状态项8MiB、同步sink批次256行/4MiB；没有总运行时限。Reader的大记录保留原值，若本阶段输出限制不足则失败并阻止complete，不截断。真实运行前须结合已保存观察及参考的真实最大记录准备限制，不能将8MiB写成真实合法数据上限或将fixture耗时当全天吞吐。旧兼容工作态和精确seen/身份索引仍随输入增长，由资源保护显式中断，不宣称常量总RSS。

### 国家与MOAS角色质量

国家规范主语为 `subject_type=country`、`subject_key=国家码`；攻击/受害ASN角色为not_applicable、两个规范ASN均空。旧 `event_start` 少attacked_as的调用和partial质量仍完整留在legacy证据；不会据此将新的国家业务结果永久标partial。旧v2 incident/episode/cohort/candidate/recent/peak以及每步输出另链保留，不冒充新国家增强的规范矩阵、真实恢复、右截尾或跨日carry-in。

规范角色规则独立为 `roles.py` 的 `detection-moas-role/v2`：原ori_as在MOAS成员对内时保留旧启发式候选角色，仍不证明攻击/责任；ori_as缺失或在pair外时，规范 attacker_asn/victim_asn 均NULL、asn_roles=ambiguous，reason=`original_as_missing_or_outside_moas_pair`，原pair、旧角色、引用和文字不删改。

规范分类规则另为 `classification.py` 的 `detection-moas-classification/v1`。上述off-pair/缺失ori_as条件在每次相关rule_decision、MOAS现象和实际生成的legacy劫持修订上均落 `classification_state=ambiguous`、`classified_hijack=NULL`，reason=`legacy_role_assignment_unanchored_may_change_filter_outcome`。原is_hijack、filter_reason、规则输入/参考/顺序及原输出不变；即使某seed给False未生成劫持事件，判定和MOAS行仍有歧义记录。ori_as在pair内的既有分支标legacy_rule_evaluated并保留其旧结果。消费者不能仅按event_kind=hijack作无条件确定命中计数，应读取规范分类状态；未知不是False或0。本次没有实现方向不敏感的新检测算法。

固定反例：此前未知新前缀第一次A从0→1时旧函数提前return，第二个origin的A从1→2时original_as为空；PYTHONHASHSEED=0/2独立进程会给出相反旧角色。fixture保留两次原输出，同时验证新规范角色均为空。另一个人工参考反例给该prefix.route添加AS4且AS4.import_as含AS2，旧route扩展过滤会随上述方向使is_hijack和filter_reason不同。旧分类过程原样保留；不能将角色或分类差异按无序成员抹平，也不声称整个旧检测跨hash确定。共同六类fixture的ori_as明确在pair内，跨进程只对moas_as1/2非角色成员对按无序比较；所有攻击/受害角色、其他字段、修订顺序及最终状态严格比较。同进程原Engine/StreamingEngine则逐条完整相等。

### 人工验证与未完成事项

本任务新建独立PG集群，仅Unix socket、无TCP监听；测试每次再创建独立UTF8数据库，没有连接其他任务PG、真实D、前端或生产服务。人工完整链由独立构造MRT与11份人工CSV/XLSX/JSON→公共produce→固定Reader→冻结Detection→typed PG/Parquet→只读重建，确实产生前缀/AS/国家中断、MOAS/前缀劫持、子前缀劫持、Leak现象，并保留country每步记录。原内存Engine作对照，检查全部输出和四模块/兼容投影最终状态，不只比较计数。

门禁人工反例包含：错run/snapshot、synthetic默认拒绝、无工作态、必需ready写入失败、末SourceEnd之后的来源资格撤回、参考资格撤回、scope collector/input版本冲突、单记录超限和一次遍历基线计数错误。观察LOCAL/STATE/EOR/空源均有实际保存链路，off-pair角色有两个固定hash新进程原值证据。共享Reader/decoder/冻结接口已有独立复核，本组不重复它们的无关全测。

当前仍待：本提交独立两轴复核；真实资料全量解释、真实D运行/一天吞吐、历史结果数值等价、跨日状态承接、业务总发布和网页验收。没有推送、部署、操作Issue或改写共享服务。

最终本组受影响检查为 **114 passed，0 skipped，0 failed**；Ruff未定义名称检查通过，完整差异检查无空白错误。验证制品位于 Git 外 `/Users/botongwu/.codex/outputs/detection-integration-20260913-01a096cb/`，含JUnit、验证摘要、冻结运行ready/result、实际与原Engine对照记录、两种固定hash原角色/分类差异及人工Parquet。冻结六类运行共130条记录、172个状态项；业务修订为前缀3、AS3、国家2、子前缀2、前缀劫持2、MOAS2、Leak2。这些是该人工fixture的修订数，不是事件实例数、真实一天发生数或完整分支覆盖证明。

## dc17749 独立复核后的 Excel 类型修复

独立报告 `ea6bfa984f2b91d9f0576adca316e26953324398` 对 Standards／Spec 各记一项P2，为同一Excel类型缺陷；其他角色／分类、冻结运行和PG／湖重建结论保持。原显式字符串单元格 `type=s,value="=1+1"` 在临时工作簿重建时被openpyxl自动推断为公式，最终错误变空。本次仅修复参考解释，不改变检测算法或公共保存格式。

重建现在同时使用保存的type和value，在赋值后显式恢复cell.data_type，避免公式文本和错误文本被重新分类。支持s字符串（含合法长度内的公式形文本、错误形文本）、n有限数值或空值、b布尔、e既有Excel错误值；公共读取返回的空inlineStr仅接受None。类型和值不匹配或其他未支持类型明确拒绝，真实公式f和日期d仍拒绝，不新增公式计算、缓存值或日期解释。没有删除合法文本，也没有新增异常填空逻辑。原错误类型e经过旧pandas／country空值处理的结果保持，同时原XLSX类型和值仍保存在原行及重建文件中。

参考解释规则由 `detection-reference-39578fe/v1` 升为 `detection-reference-39578fe/v2`，经ReferenceBundle.row_refs.selection_rule随工作态保存；冻结身份既有的显式源码摘要亦会反映实现变化。原source_id、原文、reference-rows/v2保存版本、外部参考版本标签和其他算法／角色／分类规则未改。

增量人工对照直接创建显式类型的原XLSX，经公共reference-rows/v2读取再交ReferenceView，并比较原文件与重建文件的单元格type/value及原pandas解释结果。覆盖`=1+1`文本、`#N/A`文本、`001`数字文本、原数值、布尔、错误值、空单元格和空字符串；另确认实际公式／日期及未支持类型不能登记或移交完整参考。修复前出现1 failed／5 passed，复现合法文本被改空。受影响参考测试修复后7 passed；未重跑已GO的全部旧算法，未启动PG或访问真实原件／远端／候选／生产。此次仅完成作者修复，等待84e5增量复验。

## 10. 固定 typed 原行公共读取（独立增量，待复核）

`data_pipeline.detection.store.read_stored_rows(dsn, run_id, snapshot, table, *, batch_rows=256, max_row_bytes=8*1024**2, guard=None, allow_synthetic=False)` 公开读取保存层全部 typed 列；table 仅接受 `records`、`state_entries`，分别按原 `sequence`、`ordinal` 排序，保留重复值和原 JSON 文本，不重建业务记录或全状态。旧 read_records/read_revisions/read_decisions/reconstruct_state 的输出结构保持，复用内部流时不施加新增 8 MiB 行限额，避免缩减旧读取能力；新公共入口必须使用正整数行预算。

首次读取固定 PG 的 state/schema_name/snapshot/identity/scope，要求 complete、匹配 schema 和指定 snapshot；正式默认要求既有 `frozen-fresh-process` 身份门禁，人工 synthetic 必须显式允许。湖查询使用指定 AT VERSION，后续 catalog 版本不改变本次版本。全流耗尽后以新 PG 连接复验上述资格元组，最终撤回、失败、身份或范围漂移报错。该检查不是历史撤回日志审计，不能证明曾撤回后又恢复的中间状态。

调用方必须完全耗尽流且没有异常，才取得末尾资格保证；收到最后一行时仍未完成末尾复验。提前退出应使用 `contextlib.closing` 或显式 `close()`，只释放资源，不宣称完整成功。guard 在读取前、批读取前、每行交付前及末尾复验前执行；异常与 close 都关闭湖连接，资格查询连接始终关闭。batch_rows 限制 Arrow 批行数（1–10000），逐行转 Python；max_row_bytes 是完整行以现有 plain/紧凑 UTF-8 JSON 编码后的字节上限，包含所有列和 JSON 文本，超过则不交付该行。它是交付限额，不是数据库解码前的内存硬上限；Arrow 批、单行解码和排序仍占用内存，真实运行须配置进程/磁盘保护。8 MiB 默认值不构成真实容量验收。

本接口仅保证该固定保存表的读取和上述时点资格，不重新验证 MRT、参考源、ready 全 SHA，也不提供 PG 与 DuckLake 跨库事务。Q2 负责最终发布的源/参考核验与预声明 profile；此增量不改变真实 A 输入计划、生产者、算法或写入结构。

作者增量验证：自有 UTF8 PG，项目锁定依赖，人工 MRT＋11 份参考经 produce 保存后，在正式冻结子进程完成 Detection；新接口回读 130 条 records、172 条 state_entries，与湖原表全部字段/JSON文本/sequence或ordinal逐行一致，PG JSONB按语义一致，并对照旧读取和全状态重建。后续 catalog 提交不改变固定读取结果。覆盖非法表、错版、候选/失败、未冻结、末尾 state/schema/snapshot/identity/scope 撤回或漂移、行限额、guard、提前close及旧读取大于8MiB兼容。受影响检查9 passed，0 skipped；自有PG已停止。此次只是人工增量验证，未运行真实D/P、未合入主集成/Q2，等待独立复核。

## 11. M3独立资格片

52f0a1fe基线上的Detection有序缺口/独立资格实施与公共读取见[Detection M3公共资格接口](Detection_M3公共资格接口.md)。该profile保留原科学输出，尚待独立复核，不意味着真实D或Q2已接入。旧typed/v1制品与读取保持。
