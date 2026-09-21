# Resource 公共发布接口 P1

状态：已实现、仅人工隔离 fixture 验证；待独立审查与集成。不代表组合 P、业务发布、真实输入或生产可用。

依据协调者已接受的 P0 有限合同；实现基线为 `941f040a838d90771fee102702c20e59c4c47826`。接口位于 `backend/data_pipeline/analysis/resources/publication.py`，不修改旧 Resource 十表、十六表生产与读取接口，也不清理或绑定 `resource_work_*`。

## 四个接缝

- `admit(runtime, owner_binding, guard=...)`：owner_binding 原样传入 `ResourceObservationReader.inputs()` 的完整对象。显式依赖实际 M2/reference Admission。首次核十六表类型、完整 typed 摘要、来源/Peer/历史样本/资格关系，再由固定已封存观察及参考原件只读复算旧算法，对十张科学表全部列及多重性比较。不会重解析 MRT、创建新的科学结果或更改旧 dataset。原 producer 身份与当前 validator 身份分开；复用仅在当前完整资格仍成立时返回原可信记录。
- `verify_current`：新鲜 PG 登记、实际系统/OID/目录、完整可信对象、规则、固定文件 stat 与目录 descriptor、上游公共 current。没有整表 inventory、科学复算或原件全文 hash。它不替代首次审计。
- `hold_lock`：只持一个传入的真实 `FOR SHARE` 目标。Resource run 与可信记录为 stage 30；本模块既有独立国家参考登记 `resource.reference` 为 stage 10。M2/reference 目标由对应 owner 接口持有，调用方按 P0 总序逐个进入；不隐藏依赖锁。入口核对 Runtime 的原完整绑定；正常退出先重核 Runtime 范围及资源，再 rollback/close，异常退出直接清理并保留主错及清理错误。不调用隐藏 current、依赖锁或科学尾审计。
- `open_reader`：view 限 `metrics/normal_bands/topology_status/coverage`；scope_typed 为 `{"scope":"all"}` 或 `{"scope":"result"}` 的 Resource typed 编码。科学行返回原 `raw/qualification/main`，Unknown 主值仍为 NULL。按原来源时点及固定原表键排序。首尾 current，耗尽且数据资源全部关闭成功后才生成 receipt；早停、撤销、关闭失败均没有成功 receipt。

`publication_codec.py` 明确保存 datetime、Decimal、bytes、整数、布尔和 NULL，时间规范为 UTC。Admission/ReadReceipt/RowBatch shape 沿 P0，不增加业务 head 或新恢复协议。国家参考原登记与原件/历史实体直接纳入 Resource 可信审计，不伪装为 M2 的原参考事实 Admission。

## 资源与证据边界

Runtime 显式指定同一旧绑定 catalog 的输入输出 DSN、允许根、scratch、fixture 标记、真实依赖 Admission，以及正有限 rows/bytes/RSS/wall/memory/temp/lock 预算。构造及调用处检查类型，bool、NaN、Inf、非正预算被拒绝；合法有限秒数不取整。累计 typed 审计与读取行/字节仅计量；max_rows/max_bytes 保护批或仍整体驻留的集合，临时 DuckDB 有内存及磁盘限额，生命周期 guard 保留调用方检查。

首次审计包含旧关系检查的内部查询；不能把一个 Python 函数调用当作一次实际 SQL 扫描。事件记录分别声明 inventory、科学比较、复算次数、选中输出批与 PG 包装调用。未观测的内部物理读取量为 Unknown，不能填零。RSS 为当前进程累计高水位、不含 PG；完整成本证据保存在本任务 Git 外交付目录。

不实现崩溃恢复、工作态归档重建、业务副本、通用插件或额外授权系统。必要历史计数、normal 样本、非活跃桶及预置空状态按既有实现保留。当前 public P1 无 work 表读取依赖；未来恢复另按实际边界评估。

## 内层关闭错误修复

实际内层生成器收到 `GeneratorExit` 时，不能仅把关闭次错附着在该信号上：Python 的 `generator.close()` 会吞掉它。Resource 清理函数将真实清理失败抛向外层，外层仍保留调用方原异常对象及原 traceback，并将次错登记在 `cleanup_errors`；没有调用方主错则传播清理失败。该调整不改变科学值、可信登记形状或正常读取回执条件。新增探针实际关闭内层 DuckDB 后注入错误，覆盖调用方主错、主动早停、正常耗尽；不再以外层代理 close 抛错替代内层关闭链验证。

## 显式真实候选 Runtime（待独立复核）

本增量基于 `a4fd730632453d4adf51341bc6cfd0e660f4c072`，包含365 Resource P1、a38内层关闭修复和已接受的M2 real Runtime/8b路径链；不合入Feature或Canonical实现。

人工调用保留 `fixture_only=True` 与原默认预算。真实候选须显式 `execution_profile="real-candidate/v1"`、原完整 `expected_resource_binding`、同一旧catalog的DSN、既有输出根/允许根/独立scratch，以及每个真实M2/reference Admission对应的 `dependency_runtimes`。复用原manifest/plan/seal和来源rank，不修改原声明路径，不凭另一模式的可信记录升级资格。原Resource及独立国家参考只接受原正式frozen-fresh-process版本，人工API生产的版本不可借real Runtime通过；人工输入上的real配置测试不表示真实全天资格。

`inspect_binding(runtime, run_id, snapshot, dataset_id)` 只核固定原完整绑定；不提供自动准入。admit/current/hold_lock/open_reader将模式写入validator规则身份并核相同模式上游。原16表、科学算法、business结果和work状态不改。

真实模式没有数据处理总时长截止：`max_seconds` 必须为None，不继承人工默认300秒，也不新增阶段截止或单读定时器。真实模式必须显式提供正有限 `max_rows/max_bytes/max_rss_bytes/memory_bytes/max_temp_bytes/min_free_bytes/lock_timeout_ms`。RSS、scratch可用空间、临时盘、驻留集合和锁保护保留；实际使用前后重核目录身份、模式、固定范围及预算。scratch与本地输出、Resource湖目录、原M2/CSV文件目录及独立国家参考原件/湖目录隔离，路径重定向或目录替换拒绝。全部接口仍为独立候选控制路径，不启动D/Ppre/H或业务发布。

## M2输入序与Resource处理时点序

Resource依赖接合分别保留两种顺序：M2 Admission选择必须按完整InputBinding的原输入序，Resource的sources仍严格按处理时点递增。InputBinding身份属于完整输入，不因Reader子选择变化。Resource只要求所需RIB被唯一对应的实际M2选择包含，不要求两种顺序相等；原global rank、checkpoint、seal、完整元数据与重复源检查继续执行。参考CP ordinal不当作MRT rank。

此修复只改变新准入的接合门禁，不改变科学计算或原结果。满足原科学/来源证据的旧健康输出可直接在新validator下重新准入，无需因此重产。三点人工非单调rank验证不表示588联合输入的十二时点或全天验收。


## 流式累计量与驻留边界

审核 Account 与公开 ReadSession 的累计行数、逻辑字节只计量，不因跨批总量停止。Resource 请求 batch_bytes 仍是来源批编码上界（且不超过 Runtime max_bytes），返回批与单条均不得越界。P 的 stream_policy 已将普通消费批目标与来源封装分离：在来源封装以内、超过普通消费目标的合法大条由 P 独占消费批。RSS、临时盘、空闲盘和完整尾回执条件保留；超来源封装的巨大条目仍失败，不给成功回执，其表示能力扩展属于后继。

科学审核的期望/实际完整 typed 行及路径行已移至同一 DuckDB 实例的私有临时表，有限批追加，在库内按完整值和重复次数比较；路径期望重复去重，摘要相同而完整值不同仍拒绝。沿用内存、临时盘与 guard 保护，不设置对账总行数上限；私有 cursor 关闭释放临时表，原扫描不受追加影响。Reader 资格通过一次 DuckDB 关系连接按原结果行序和资格全列序增量聚合，只保留当前结果行的完整资格（受单行驻留字节与 RSS 保护），不预装全量资格字典；source-id 覆盖在库内核定。原 receipt 仍必须携带完整 coverage，因此该控制对象继续保留明确驻留保护；原参考加载及科学计算自身状态也未在此增量改造，因此不宣称任意规模完整审核与读取已经可用。业务公式、行结构、顺序、摘要与原科学制品不变；validator 代码身份变化后需新 Resource Admission 及引用它的下游准入，依赖包未改变。
