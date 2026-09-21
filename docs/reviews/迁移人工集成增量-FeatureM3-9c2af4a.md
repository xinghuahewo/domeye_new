# Feature M3 人工增量集成

2026-09-13。**人工模块集成 GO。** 已验证固定M2 observation→共享ordered→冻结新进程ordinary/IR→另一新进程公开绑定、窗口与覆盖读取。缺口后的窗口计数可以恢复，资源、依赖旧路径的W归属、稀疏及比较资格仍受限。真实D/P/H、全天承载、高频读取、其他M3消费者及统一P尚未验收。

## 固定版本与完整差异

从已接受 `812c904b1cab3adf3d61134b5162b41227b97ebb` 合入完整固定链 `52f0a1fe74e7cc1ffbea5a05a83751145d775056`→`26260fbb1e4252e7651900dce9ccdc69c91772bb`→`9c2af4a942f7a96049ed5c8dc1e1bb36258153c1`。merge为 `3c0a9764b4c74f65f32135a35101185f766a490f`，精确双亲依次为812c904b和9c2af4a，无冲突。候选完整差异9文件、965增/26删；已审阅五个产品文件、两个测试文件及两份中文文档。

父接受的两份独立报告全文已读并核SHA：原 `/Users/botongwu/.codex/outputs/feature-m3-review-308e/独立复核报告.md` 为 `87a0f11b474c4e6e65013e2b0f06914a8f961b69df7b598f9a50148ee62bedda`；最终 `reader-close-increment/单项P2增量独立复核.md` 为 `e6b68ccac953c14e0e8f19c356cd74f2fcd2f2816983085fc3ef0c29df14da5e`。原连接P2关闭；原失败证据不改写。候选文档当时“待审”状态以独立报告和本记录补充。

仅集成定向测试与本报告为额外改动，产品实现未修订。旧Feature科学公式、M2、共享ordered、RSS口径、Q1、Detection/Resource、已接受Q3-C.1和C5-S1源码差异为空；未合其他在途M3候选。正式冻结代码34文件的SHA全部与当前源码核对相同，快照摘要 `f9c9498443f7a83f2447593271834c85cd27f45cf2a3da2cf165df892f52cfc0`；正式子进程结束后临时代码副本已移除。完整执行代码身份保留，不需要为Reader连接修复重产旧制品。

## 四项实际定向测试

`backend/web/tests/test_feature_m3_integration.py` **4 passed in 24.51s，首次全部通过**。包含1个原空来源用例和3个新增联合/生命周期/旧制品用例；不叠加作者39、独立11或修复6项。使用本任务PG55483、各自人工数据库与临时目录。

1. 主样本：人工baseline、LOCAL W与LOCAL A后接一个rejected LOCAL Gap，再接完整2A/1W来源。M2共8条消息、6个元素、3条保存参考行，business保持not_run。公开原ObservationReader与ordered输出的全部消息/元素raw字典逐项相等，Gap原record=2、raw/解释摘要、offset/length、原Gap ID核对相同。LOCAL A/W均进入原科学路径，没有新增LOCAL过滤。
2. 普通/IR中缺口窗raw A=1、W=1，但公开主值均None；后一完整窗ordinary A/W=2/1、IR=1/1。六维手写期望依次为window_counts complete、announcement_attribution complete、withdrawal_attribution/resources/sparse/comparison partial。原Gap的未来prefix及未来origin范围仍保留。独立空来源样本无ASN窗口行，末空来源每mode仍有六维资格：窗口计数完整、资源/稀疏/比较保留Gap和partial，未补出ASN零行。
3. 主样本全部12表读取计数与完成回执一致，保存完整正文及逐表全文规范化摘要。另一独立Python进程重新inspect_binding，再完整read_windows/read_coverage，与本进程完整binding、窗口及资格逐字段相等：14行窗口、36行覆盖。底层不传profile读取新结果及Q1实际feature_history路径均明确以profile不匹配拒绝；没有初始化或发布新Q1。
4. 真实提交自有完成锚摘要损坏，保留ValueError及traceback并禁用GC；独立PG观察连接查被测application_name会话为0。之后恢复原锚。只测一次必要坏锚，不重复全部六项关闭矩阵。旧strict兼容与此共享测试文件，详见下节。

四个pytest实例分别是完整科学/资格/新进程、保留异常连接关闭、原strict/Q1兼容和空来源覆盖。上面的科学与资格细分是断言说明，不额外计测试数量。原MessageBoundary/Element全字段比较并非只查消息数；12表摘要用于保留本次完整结果，不能冒称12表所有科学字段都有独立算法oracle。科学独立手算集中在上述LOCAL A/W、后窗统计及六维资格。

## 原strict Feature与Q1直接兼容

直接读取本任务此前已接受的 `/tmp/domeye-m2-repaired-integration-8233/test_resource_feature_share_ob0` 原库/root。Feature原run `4b9599acdde34fd983c07b90a3712093` / snapshot80，八表计数为4、25、16、39、30、10、0、6（依次module_diagnostics/windows/projection_revisions/resource_members/state_deltas/source_receipts/decoding_differences/reference_rows），与原执行回执一致并保存全表摘要。

原Q1 build `a7a4a4ca95d945398ff1907edf418ab9` 的普通/IR全部5个保存页面逐字段相等，包括原科学值、来源与计算依据；旧根368个文件前后SHA一致。没有重导、重生产、修改旧ready或用新造数据替代旧兼容。其余未变模块没有再跑全业务回归；不声称所有历史profile均已验收。

## 本次成本与作用域

| 阶段 | 本次输入/输出及字节 | 实际wall | RSS范围 |
| --- | --- | --- | --- |
| M2与Feature输入绑定 | 压缩MRT合计277字节；8消息、6元素、参考3行 | 1.650430秒 | pytest生命周期高水位前295370752、后300007424字节，包含先执行空来源测试，不是M2独占峰 |
| 正式冻结启动至Feature完成 | 12表119行；23次flush，逻辑68375字节；Parquet216156字节 | 4.294939秒；flush累计0.340023秒 | 正式子进程生命周期峰278200320字节，包含导入和DuckDB，不含PG |
| 新进程inspect＋两次公开完整读取 | 14窗口＋36资格，全文对账 | 4.357918秒（模块导入后开始计时） | 新读者进程峰229195776字节，RSS包含导入 |

12表119行包括科学/审计73行、资格相关46行；各表精确数量与摘要见完整对账与成本.json。M2阶段指标另保留原metrics及upstream-cost.json。ordered单独wall、读取OS实际字节、各阶段独占RSS/全PG资源峰值未单测，记Unknown，不补零。逻辑flush、压缩源和Parquet物理字节不可互换。测试无处理总时限，保留既有RSS/行批/清理机制。

公开inspect每次检查12表计数并全文物化/核验四资格表；read_windows与read_coverage各自入口和耗尽末尾重新inspect，且另读资格表。因此一个联合读者会重复完整资格校验与索引构建，当前不是高频轻量API，不从该小例推算全天性能。未测实际SQL总条数，不把源码调用数充作PG实际计数。

## 证据与收尾

证据根 `/tmp/domeye-feature-m3-integration-8233/`。主目录 `feature-m30/` 内有原消息与元素.json、formal的request/stdout/stderr/output执行回执、绑定请求.json、全部主体.json、本进程公开读取.json、新进程stdout/stderr、完整对账与成本.json、持有异常连接复验.json。旧兼容回执位于test_original_strict_feature_a0/；空来源人工制品位于test_empty_inherited_gap_has_c0/。pytest.log/xml及交付索引统一保存证据SHA、代码身份、两份独立报告SHA与权威文档SHA。

主新Feature run `819846d60bec4af2986e3ca743d49de3`，snapshot36。首次PG启动命令遗漏空host参数，进程未启动；修正引号后仅Unix socket启动，原错误保留于 `/tmp/domeye-integration-detection-8233/feature-m3-pg.log`。完成后smart stop，55483及未启动的28763均no server running，见pg-stop.txt。未触真实D/P/H、远端、旧失败726、生产A、Issue、push或部署。

2c8a三权威SHA不变：计划 `a7c6fbceda3558557d1bde1404db15afba26d171e105daf5a8013f0c02374610`；ADR `eef76025290ebb6f76ec48c8b9264d1d999376178e29e2bee21ee783b00515af`；README `c82f02b8a17276ee8d740d8663b4d969c2a9e095410746b813737c461b9b051f`。本片完成后停止，等待父复核，不自行启动其他候选或全天。
