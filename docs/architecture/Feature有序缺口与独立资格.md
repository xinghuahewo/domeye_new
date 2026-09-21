# Feature 有序缺口与独立资格

状态：本片已实现并以自有人工输入验证，待独立审查；不代表真实数据、统一发布或网页已接受。基线为 `52f0a1fe74e7cc1ffbea5a05a83751145d775056`，科学合同为 M3 `d4c479a2dcbe22a94620f0f8bd6fc6fb2e35dab05cebf46e20ac1292f7d9327f`。旧科学含义见[Feature纯计算迁入](阶段0A盘点/Feature纯计算迁入.md)。

## 输入与有序接缝

`feature_inputs.SourceView` 与 `ReferenceView` 新增 `profile`，默认仍为 `complete`；新片显式填 `observation`，使用 M2 的固定 run/snapshot Selection、bound_table 和共用 `ordered(reader)`。同次执行的全部来源与参考必须采用同一 profile，暂不支持混合新旧阶段，混合明确失败。旧单 run API 和 CLI 保留；M2 新多视图请求沿既有 `source_views`、`reference_view`，仅在每项加入 profile，不另发明 Gap JSON 输入。

原 baseline/snapshot 均可作唯一私有 initial_rib；后续只能原 UPDATE。别名仍全部绑定并复验、仅消费首选一次。同一 run 的选中子集保留共享 InputBinding 的完整原 source_rank；`source_bindings` 单列 `ordered_binding_ref`、`upstream_source_rank` 和 Feature `sequence`，不改写 Gap 原游标。每个原 Reader 单独产生有序流，多个 run 只在 Feature 已有计算顺序上接续；切到 D 或新 run 不清除私有缺口。

普通/IR 各自保有 Qualification 与原 FeatureAdapter。MessageBoundary 的 Gap 在本消息元素前生效；缓冲中的更早合法元素先进入科学计算，再应用后续 Gap/质量控制；按行数与字节阈值批写，不为每个合法元素单建 Arrow。Element.raw 原字段继续经 `decode_element` 和原计算过滤，未加入 LOCAL 过滤、未改公式、原 origin 残留和清零语义。受限 ET 的时间来自共用 typed boundary/gap；不回填 raw.microsecond。SourceQuality 不捏造 record；真实 STATE/EOR 不作为 Gap 或恢复证据。

参考仍经原 `load_reference` 的固定 CSV SHA、保存行、字节坐标与 pandas 解释规则。参考 Reader 使用同一 M2 固定选择；规格保留本次实际所需参考 SHA/checkpoint/行数、原载体 collector、完整 observation_seals（包括其参考 checkpoint），而非仅凭不含 reference 的 MRT InputBinding 宣称参考齐全。当前 Feature 实际只需既有 as_entity CSV；未要求或宣称本片已消费其他模块的全部参考内容。历史参考有效期仍为 Unknown。

## 资格与恢复边界

新输出 profile 为 `feature-qualified/v1`，schema 为 `feature-qualified-tables/v1`，规则为 `feature-dependency-qualification/v1`。原八张科学/审计表、原计算版本、全部诊断与中间工作态保持原含义。四张新增类型化表：

| 表 | 内容 |
| --- | --- |
| input_gaps | 每 mode 的原 Gap 身份、binding/原rank/Feature rank、record、原始 SHA/offset/length/digest、typed 时间、方向与可信端点、解释摘要/原因、范围与规则版本 |
| source_qualities | 每 mode 的原消息或来源质量；record 可为 null，显式 evidence_id、code/detail |
| qualifications | 每 mode/source 六维资格、qualification_id、绑定窗口、source_end 有效位置、依赖范围与 Gap/质量外键 |
| qualification_receipts | 每 mode/source 的 M2 三种解释计数、Gap/质量/资格数量、原rank映射与资格摘要 |

所有新表带 window_role。Peer ASN 字段始终指 VP 折叠依赖，不是被漏路由的 origin ASN。`unknown_peer_covers_unseen_asns` 保留未知 Peer 扩围语义；`all_future_origin_asns=true` 明确未知 NLRI/origin 可能影响未来或未列的 origin ASN，即使 Peer 已知也不能猜测其国家或假设被 IR 过滤。LOCAL 与 received 可在同 ASN、不同端点、ADDPATH 槽位折叠到同一私有键。保存可信 Peer 范围供审计，当前主值判定保守覆盖可能受影响的全部 origin/国家资源，不宣称每个对象实际上均受损，也未实现精确对象恢复。

| dimension | 本片规则 |
| --- | --- |
| window_counts | 当窗出现 Gap/质量则 partial；后续覆盖声明完整且实际完整耗尽的无新缺口窗口可 complete。是每 mode 接受 A/W 元素数，不是 UPDATE 消息数。 |
| announcement_attribution | A 由本次合法 new_path 归属；当窗有 Gap/质量受限，不能猜被漏 origin；后续完整窗可重新合格。 |
| withdrawal_attribution | 当窗缺口受限；出现可能依赖历史受损 old_vp_path 的 W 时继续 partial，不从旧路径证明归属。无 W 的完整窗可判本窗此维度完整。 |
| resources | 历史 Gap/来源质量保守持续受限；初始覆盖未知也持续未知。单对象 A/W、文件边界不清除 origin 残留或恢复全量资源。 |
| sparse | 不因未 dirty、无 ASN 行、IR 禁写或旧零值解除依赖；历史缺口继续受限。 |
| comparison | 独立保留历史资源/窗口依赖，P/D 换窗不解除；未实现中途 RIB 业务重置。 |

原窗口 coverage/message_quality_state 不完整时，新资格不补 complete。原科学值未因上述资格改算；因此原 `source_receipts.quality`、`windows.resource_status` 或模块 complete 不能替代新六维资格。初态没有 UPDATE 统计，计数/归属/比较维度为 not_applicable。每个 mode/source 都落六维覆盖，包括零元素、无 dirty、无新增 ASN、仅继承前源 Gap；不强制引用不存在的 ASN 科学行。Gap 表给出缺口起始位置，窗口资格为 source_end 的判断，不能用于倒推 Gap 前样本也受影响。对窗口内任意样本或精确事件资格的推导不属于本读取 API。

## 完成门禁与公开接口

SourceEnd 的 Gap 数必须与本 mode 实际接收数量一致。来源完成前核验独立六维枚举、行身份、Gap/质量计数及同 mode、不晚于当前来源的引用；finish 再检查全部 mode/source、全表计数、资格表内容 SHA256、原工作态、代码身份及全部输入选择。ready 回执的 qualification_completion 与同事务写入的 `feature.qualified_results` 完成锚保留相同规格摘要、参考版本、数量及内容摘要。Feature 完成不会修改 M2 的 business=not_run；未引入跨库原子恢复平台。

```python
from data_pipeline.feature_qualified_read import inspect_binding, read_windows, read_coverage

binding = inspect_binding(dsn, feature_run_id, fixed_snapshot)
# 将完整 binding 保存到下游不可变输入清单，不重新选“最新”run。
for item in read_windows(dsn, binding, window_role='result'):
    raw = item['raw']                 # 原完整科学行、原身份、单位沿原规则
    raw_values = item['raw_values']   # 兼容计算值，仅作有明确口径的审计
    values = item['values']           # 主值：依赖不完整时为 None
    qualifications = item['qualifications']  # 六维完整typed资格
for coverage in read_coverage(dsn, binding):
    pass                             # 包含无ASN行/空源窗口，不遗漏稀疏资格
```

公开 binding 包含数据库实例身份、固定 Feature run/snapshot、完整 specification（所选来源/别名、原rank、M2 seals、参考、窗口、代码/算法版本）、reference_version、完成锚摘要与资格摘要。读取开始/耗尽后复验绑定和 M2 资格；全表行数检查与资格内容/枚举/FK验证失败即拒绝。科学行按 Arrow 批流出，资格元数据有界于本次来源及缺口规模，当前会物化资格索引并受 RSS 保护；不是无限规模查询承诺。默认 1 GiB 高水位保护可显式配置，外部 guard 可取消；没有处理总时限。调用者须耗尽或丢弃本次未完成读取，不能将早停结果作为完整发布。

`values` 的资源字段要求 resources complete；collect A/W 要求 window_counts complete，ASN/country A/W 还各自要求 announcement/withdrawal_attribution complete。所有旧 raw 值和 row_presence 保留；没有生成稀疏 ASN 的零行。覆盖读回保留其完整 scope/原因/证据，主值不完整不补零。

底层 `feature_store.read_table(..., profile='feature-qualified/v1')` 明确读取新 profile 的 **raw 表**。不传 profile 的原入口只接受旧结果，因而原 Q1/publication 不会静默接纳新 profile。旧完成制品不回写、不重导；旧缺诊断仍是 not_saved。当前未修改全局 Publication/Country/common；这些 owner 需用新的固定 binding/qualified reader 再接其独立发布与查询合同，不能据新 Feature complete 宣称已统一发布。

## 验证范围

人工测试覆盖实际 M2 封存→共用 ordered→冻结新进程双 Feature→新解释器固定公开读取；无 Gap 同一保存输入与固定接受基线 runner 的全部原八张类型化科学表逐行一致。另覆盖 LOCAL A/W 及遗漏 Gap 对照、完整后窗 ordinary 2/1 与 IR 1/1、IR 禁写、零元素/无 dirty/继承 Gap、snapshot/别名/跨 run 与 P/D、同 ASN 不同端点/ADDPATH 折叠、受限 ET typed 时间、未知 Peer/未来 origin、漏 Gap/漏资格/早停/错映射/末源后撤销输入/完成锚损坏。

原制品兼容检查使用本任务此前保留的 diagnostics-final 私有数据库与原 root，直接读旧固定 run `80bb475f94424f3ab4d71f28942cbe70` / snapshot 21，未重导或重新生产；八张旧表计数与原回执一致，原 execution.json SHA 未变。具体命令、wall/数量/字节/RSS 与临时证据路径随交付回执保存。以上不是业务完整恢复、真实 D/P/H、承载上限或产品验收。

### 公开 Reader 连接关闭修复（已实现，待增量独立复核）

公开 Reader 自行获取的三处 PostgreSQL 连接使用 `contextlib.closing` 包裹原事务上下文。事务退出后立即关闭连接，拒绝完成锚时即使调用者仍持有异常及 traceback，也不依赖垃圾回收释放连接。科学计算、资格规则、表结构、M2 与写入路径未改动，因此已有完成制品无需重导或重算；新执行仍按原机制记录其代码身份，既有制品保留原身份。

定向回归测试通过显式环境变量 `DOMEYE_FEATURE_READER_CONTEXT` 绑定自有人工制品及修复前摘要，未提供时跳过。测试真实提交完成锚损坏/缺失，再保留异常并以独立连接查询 `pg_stat_activity`；另检查不存在的 run、正常读取及真实 DuckDB 的提前关闭和 guard 中断。原新 profile 制品在原数据库和原目录直接回读，完整 binding、12 张原始表摘要、公开窗口/覆盖摘要及原输出文件摘要均一致。该验证不代表生产部署或独立复核通过。

### Feature 公共 P1（人工首片，待独立验收）

`feature_publication` 提供 `admit / verify_current / hold_lock / open_reader`，首片仅接健康 M3 的12表与六维 windows/coverage。旧八表及 Q1 沿原入口，不新增或伪造旧资格；旧 complete 尚无本轮 M2 P1 适配是新增准入缺口，不代表删除历史功能。

Runtime 显式绑定本模块输出DSN、规范输出根、允许根、临时资源预算、人工授权、完整依赖Admission及对应上游Runtime。直接调用上游公共 current、逐目标锁，不复制其权限判断、不将DSN写入Admission。首片仍只支持原Feature同输入库多run计算，不扩充跨输入库算法。

首次 admit 完整读取12表的原typed内容，利用生产与审计共用的 `_calculate` 循环及原行构造方法向私有临时库输出期望行，逐表 `EXCEPT ALL` 双向核对科学值、合法重复、顺序敏感列表、ID、引用、状态链、窗口、参考整列解释及六维资格。没有调用生产入口、初始化FeatureStore或重解析MRT；一次完整审计含ordinary/ir各一次输入流，不把它称为零计算。完成锚、原ready、固定目录、每个实体全文SHA和实际上游依赖也须一致。失败无accepted；独立UUID先于Admission摘要分配，可信登记保存完整Admission与审计记录。

原producer冻结身份保持原值；validator使用自己的明确文件摘要、环境和规则摘要，不要求历史producer HEAD等于当前HEAD。只支持列明的历史科学实现摘要，且必须通过实际完整内容比较；历史诊断排序若造成ID/refs差异将报告run/source/table并拒绝，不删除或排序列表掩盖。创建新登记前检查实现已在当前提交中；上游历史owner_revision不因下游无关提交而改变。

current仅核新鲜PG登记与固定目录、完整可信记录、实际validator、上游current、实体戳，不读取科学正文、重hash实体或调用完整审计。目录范围为该Feature独立catalog，当前不接受delete/inline/分区/mapping布局演进。实体保管沿不可变文件约定，目录与实体戳变化拒绝；不宣称防御有权限篡改可信登记的管理员。

Feature只持一个stage30的run、资格完成锚或自身验收记录。上游stage10交给对应owner，统一排序去重。退出逐项rollback/close，不尾audit。Reader请求仅 `view=windows|coverage`、typed scope `{mode:ordinary|ir|all,window_role:initial|warmup|comparison|result|all}`、原typed codec与行/字节批预算。原raw/main组合由旧Reader与P1共用纯函数，Unknown不补零，零ASN不丢coverage。完整耗尽、关闭全部资源和尾current都成功后才有Receipt；早停、guard、主错误或清理错误均无Receipt。

首批成本限制明确：现文件未按请求source/mode分区，选择windows会先装载全部windows与qualifications表至临时库，coverage先装载全部qualifications；随后逐来源/模式选择及排序。这是选择前置装载，不是重复12表关系验收，不能称为按输出批次限制扫描量。现科学审计的路由状态和参考整列仍驻内存；DuckDB内存/临时盘、进程RSS与guard提供保护，不构成任意规模承诺。成本事件记录显式SQL、实际Parquet行组/字节、临时盘采样及进程累计RSS；不把应用SQL次数冒称内部全部服务器SQL。

P1预算增量修复：`max_temp_bytes` 与 `max_rss_bytes` 仅接受原生int/float、至少1且有限的数值，bool/字符串/NaN/正负Inf拒绝。合法有限值仍可调整。构造、实际预算比较、读取恢复及尾current之后均复验类型/有限性；耗尽后或回调中改成非法值也不能生成Receipt。该修复仅改变Feature validator，不修改上游Runtime或既有科学输出；新规则需新Feature准入key，旧登记及原制品保留。

### 公共P1真实候选Runtime（已实现，待独立验收）

读取现有正式Feature结果须显式使用 `fixture_only=False, execution_profile='real-candidate/v1'`，不能借人工标志绕过。Feature自己的新增范围参数为完整 `expected_feature_binding`：原specification已包含初态、比较窗、结果窗、ordinary/ir规则、原来源/M2 seals和参考身份，不另造Feature MRT manifest或execution_scope。原manifest和完整expected_m2_binding仍交给上游已接受Runtime验证；Feature直接使用其完整Runtime和真实候选Admission，拒绝跨人工/真实模式混接。

真实候选必须显式指定输出DSN、output_root、allowed_roots、隔离scratch_root，以及memory_limit、max_temp_bytes、max_rss_bytes、lock_timeout_ms、min_free_bytes；不为缺失配置填默认值。输出/scratch必须是既有允许目录并彼此隔离。资源仅控制有限内存、临时盘、最低空闲盘和锁等待，没有整个任务总时长上限。构造及运行中拒绝模式、原完整binding、固定输出根或隔离scratch目录漂移；实际PG/目录与binding仍按原完整验收/current规则核验。人工调用沿 `fixture_only=True` 保留原默认预算，不能混入真实模式参数。

execution_profile进入validator规则摘要和新Admission身份。切换模式或validator变化须新准入key，旧producer身份、科学输出和旧登记不修改；不能把新登记描述为重产结果。四操作、单锁无尾audit、有限预算、完整耗尽/全部close/尾current后Receipt及Unknown语义不变。验证仅复用自有人工M3/M2，显式走真实候选控制路径，不是实际全天或生产验收。现/tmp路径兼容由上游owner另修，本片不改共享实体形态或路径策略。

真实scratch实际范围增量：构造时固定scratch的device/inode；运行中重新执行实际路径/允许根/无符号链接检查并核对目录身份和隔离关系。临时目录创建前及DuckDB连接前分别复验，读取中和尾current也经资源guard复验；同名普通目录替换或符号链接重指向拒绝。合法子目录创建不改变根device/inode，因此不冻结根mtime/ctime。人工默认不增加此目录身份门禁。该边界覆盖普通替换及操作边界上的检查，不宣称抵御有权限管理员在检查与系统调用间并发竞态；未建立通用目录权限或TOCTOU平台。
