# Q3-C.1 人工集合结构集成验收

2026-09-13。**人工集成 GO，范围仅 structure_audit_only。** 两个 profile 共用冻结、导入、六表审计读取和文档定位索引重建；H1/H5 领域查询、H2/H4 适配、H6 补缺、真实 H 和完整 P 均未验收。

## 固定身份与审查

从已接受 `52f0a1fe74e7cc1ffbea5a05a83751145d775056` 无冲突合入固定修复 `2bbc4b560be9b0cce545faf52e32543964e1b8a3`（唯一父 `c3a93b268d76c5f3b6d1374348e16f99aedbb6d8`）。合并提交 `fcf9ab175e2f3a719adfde123ccf47d33dc7911b`，精确双亲依次为上述52f和2bbc。候选完整差异11文件、2217增/1删；审阅相邻7个模块、两份测试及两份文档。旧 historical_import 六模块、业务运行链和依赖锁差异为空。集成另加一份定向测试和本报告，未修改候选产品实现。

独立增量报告 `/Users/botongwu/.codex/outputs/q3-a-review-308e/q3c1-fix-review/四项P2增量独立复核.md` SHA256 `37a3fd31064c2ce0a30096959cdbef664a3353fdb7e0f7dc827e79dfcd021865` 已核实；原四项P2失败报告保留。候选文档的“待增量复核”是交付当时状态，以独立报告及本集成记录补充，不重写原证据。冻结前保存全部13个新旧模块代码SHA；导入后相等。

## 本次实际验证

`backend/web/tests/test_q3c1_integration.py` 首次执行 **14 passed in 10.60s**，未有失败重跑。8个参数化修复实例在本任务自己的 fixture/PG 实际执行，另6个集成实例；不将上游18/75项或旧集成计入本次数量。

- 自造17条 records、两事件，core-index/v1 与 general-read-model/v1 同集合实际 freeze→import→newReader。六表行数：files 17、documents 131、nodes 1828、edges 16、identities 203、availability 8，总计2203行。完整typed有序摘要、131份文档独立pairs/数字词法oracle、1828节点span和1320键span核对通过。17条item交替TEXT/BLOB，重键1/1.0、负零、超2^53整数、指数Decimal和数组重复顺序保留；普通extra身份同名重键四个occurrence保留，真实身份重键拒绝。
- 移走自己的sources和freeze后，实际新Python进程仅持目标绑定与CollectionToken，完整六表摘要一致；私有PG重建后实际仅documents表，131行、6个定位字段逐行相等，nodes复制0。
- 空records、空事件集合仍保留四隔离日期；非空集合也有四个validation_failed，不补成功。累计child601+601/1000在正确边界拒绝；201+201/500真实导入使用500/299剩余额度。bulk/nodes/rebuild当前行预算64字节拒绝，真实整页预算失败及span截断后即使恢复原件仍无完成回执。
- 本任务原Q3A两个及Q3B三个不同Token在原库、原root读出，禁止调用重导方法；逐表scan与bulk完整值一致、资格complete，22个原载体文件前后SHA不变。原表行数分别[4,1,0]、[6,1]、[3,1,0]、[5]、[3,1,0]。这是原Token兼容检查；本次未重跑旧来源独立oracle。

主CollectionToken ID `39b6737ffd374e82b8196f4c4de08c78`，snapshot 14；完整Token、目标数据库和代码SHA见证据binding/阶段回执。Q1/Q2/C4使用另一人工根，本次无共享载体目录、无产品运行链差异，未重启其PG或重复旧矩阵。

## 实际成本与资源边界

同一17行/两事件固定输入，冻结0.435秒、导入1.592秒。冻结原件27595字节、解码32226字节、child原始18行（含provenance）、typed2203行；导入typed4406是写入加回读工作量，不是新增源行。集合Parquet 57234字节，保留原/解码制品63684字节，child输出另计13328字节。

| 输出批 | 完整六表秒 | 实际PG语句 | Python SHA调用/字节 | Arrow批 |
| --- | --- | --- | --- | --- |
| 1 | 0.722 | 145 | 214 / 1732318 | 2203 |
| 17 | 0.487 | 145 | 214 / 1732318 | 131 |
| 1000 | 0.479 | 145 | 214 / 1732318 | 9 |

PG数来自独占实例真实log_statement日志；SHA数是透明Python SHA256调用探针，含代码/child/载体及行摘要，不把集合内部计数146误称全部调用。计时含审计日志和校验开销，仅本机人工样本，不推算真实H吞吐。

冻结当前RSS 90472448字节，导入178241536字节；读阶段约190857216—192348160字节，进程生命周期峰值194527232字节。RSS为不同采样接口/时点，生命周期ru_maxrss不是每阶段独立峰值，不含PG。冻结spool实际270336字节、temporary累计595940字节；导入temporary77012字节。累计写入量、候选目录检查值与物理峰值不是同一指标；没有测量全PG磁盘或系统I/O峰值。读块292仅显式Python边界，未测项目不填零。无处理总时限。

## 证据与收尾

本次Git外证据 `/tmp/domeye-integration-q3-8233/q3-c1/integration-3cad2ad5257b40198ed8eebee7247551/`：pytest.log/xml、binding.json、阶段回执.json、全字段oracle.json、ordered-typed.json、expected-digests.json、new-process.json及日志、空集合.json、三批实际成本.json和三份实际PG日志、五原Token兼容.json。负例候选保留于 `/tmp/domeye-q3c1-integration-tests-8233/`。交付索引单列逐文件SHA。

本任务28763仅socket私有PG已smart stop，复查no server running；另一业务人工PG55483也保持停止。关闭证据在上层q3-c1/pg-stop.txt。未接入真实D/P/H、远端、HTTP、共享服务、M3/C5在途工作，未push、发Issue或部署。

2c8a只读权威文件SHA未变：计划 `a7c6fbceda3558557d1bde1404db15afba26d171e105daf5a8013f0c02374610`；ADR-0002 `eef76025290ebb6f76ec48c8b9264d1d999376178e29e2bee21ee783b00515af`；README `c82f02b8a17276ee8d740d8663b4d969c2a9e095410746b813737c461b9b051f`。结论只关闭已授权Q3-C.1人工结构集成片。
