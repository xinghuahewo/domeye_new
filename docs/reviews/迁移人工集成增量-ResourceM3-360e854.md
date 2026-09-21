# Resource M3 人工集成增量验收

结论：**GO，仅限本次人工集成片**。已完成合格 M2 observation→冻结 Resource→新进程 inputs/values/完整 16 表，保留原 Resource 科学行为；三项修复代表均有实际拒绝证据。没有 P1、C5S2、H1 或国家接线，不代表全天、页面、业务发布或真实数据验收。

## 固定基线与审阅

- 起点 `51eee76970eb42a07ff854b3343b6a702d8def4b`。
- 完整授权链 `52f0a1fe74e7cc1ffbea5a05a83751145d775056` → `4a106b4956479806954c092063742e7cb106f6cf` → `11f2f277db40df16b7417ea067a818dd47101c7d` → `360e854a1f44924c51bed51097267f6d8d89d48c`，相邻唯一父已核。
- 无冲突 merge `c80751b72b486f5bee3f5537ae0504fb2f5afdde`；准确双亲为 51eee769 和 360e854a。11 文件、1140 增/16 删完整差异已读。产品与 360e854 一致，仅新增集成测试和本报告，没有改科学计算、M2 或其他消费者。
- 原报告从固定 `f3c326f21a5d5301a0cfd2f21374be2c7b2afcac:docs/reviews/ResourceM3独立实现复核-11f2f27.md` 读取全文，SHA `210a45f4cbe38b0749172e472f890be017925f75515250cbac4e45a93e1d0d5a`。
- 完整阅读 5ec5 的 `docs/reviews/ResourceM3三项P2增量复核-360e854.md`，SHA `bdf9373e0a2238fd96492652941bd773fb867d5bbc00ea5d71122f19b7593661`；亦读 `/Users/botongwu/.codex/outputs/resource-m3-repair/增量交付说明.md`。独立 Standards 2P2/Spec 1P2 已关闭；其 16 反例、5 pytest、原 174 文件等不计入本任务结果。

## 本任务人工输入与冻结产物

证据根 `/tmp/domeye-resource-m3-integration-8233`。仅使用自己的 `/tmp/domeye-integration-detection-8233/pg`，Unix socket 55483、无 TCP，新建独立 Resource M3 人工数据库；不是旧 strict business complete 伪装 observation。现有 M2 没有足够 RIB 历史，故只新造一套 10 个 RIB 的小样本，所有负例复用它。

- M2 run `3d86d4c919d149dfaf2ca962e6efccbd:45`；seal `811c53c1048d1e992ebacc4955db22feb9fce214299c0279020f027037659f87`。M2 总 30 消息/19 元素，去除原路径别名重复后的压缩输入 1042 bytes；两份 M2 参考各 3/2 行，另注册独立 country reference。
- Resource 只选 10 个 RIB，实际 28 消息/18 元素；原 rank 为 `[0,1,3,4,5,6,7,8,9,10]`，没有按 RIB 子集重编号。中间未选 UPDATE 的 1 个 rejected/Gap 仍留在完整 M2 封存；所选 RIB 无 Gap。输入 alias 沿 M2 原归一化，不重复计算。
- 第 9 个 RIB 只有 Peer 表、无元素；其旧 raw 0 被旧异常过滤排除，第 10 个 RIB 非空。参考独立绑定 checkpoint/attempt，不赋 MRT rank。
- 冻结 Resource run `c4b0419f11fc4d6ab7f4abb714ae1078:67`；dataset `08a7ab5f91b969522b4589f66cc77a053b9ea1b34675b4947d434d0b5b0246fd`，`resource-observation/v1` complete。
- 41 个冻结文件 SHA 全部与当前源一致；冻结 digest `1557b375c23418e0d59df0c82323d31ab4509ecf53d71f7194673d7ca4da980b`。实际加载 `data_pipeline.resources.validation` 来自冻结清单 `backend/data_pipeline/resources/validation.py`，SHA `fd70b00383495be6a074907349d562ebe45191db329ca8a76ec6526f0149eb81`；未仅检查文件存在。

运行请求、数据库、完整正文、冻结执行回执、指标均在 Git 外。正常 M2 73 个文件在 Resource 生产前保存 SHA，生产后及负例/撤销恢复后均相等；M2 登记/checkpoint/封印不变，business 仍为 not_run。

## 全文与原科学行为

一次底层读取保存全部 16 表 typed 正文；另一全新 Python 进程通过公有 inputs、三类 values（scope=all）和每表 scan（scope=all）完整耗尽。以保留 datetime/Decimal/bytes 含义的规范编码比较每行全部字段与重复次数，不依赖未承诺的 SQL 行顺序；完整公有输入回执与正式 inventory 相等。

| 表 | 行数 | 表 | 行数 |
| --- | ---: | --- | ---: |
| sources | 10 | metrics | 28 |
| memberships | 108 | rendered_paths | 1 |
| normal_bands | 113 | normal_samples | 544 |
| topology_edges | 9 | topology_status | 10 |
| decoding_differences | 0 | decision_refs | 18 |
| input_receipts | 10 | peer_dependencies | 10 |
| observation_quality | 0 | coverage | 40 |
| qualifications | 459 | qualification_dependencies | 1806 |
| **合计** | **3166** | 逻辑正文 bytes | **928538** |

独立导出固定 52f 原 compute.py，先确认当前科学源逐字节未变，再实际加载旧模块，从公有 M2 原元素及固定参考运行旧 ResourceComputer。复用 Store 的原字段映射但不写库，逐表比较原十张科学表全部 **841 行**：数值、成员、路径、逐元素 decision、normal 全部实际样本与边界、拓扑边和状态。运行归属引用直接绑定同一测试目标 run，未归一化科学差异；最终 PG work_meta 的 rib_count/last_source/initial_state_basis 亦相等。不是只用新资格算法作自身预言机。

另外手写数值断言：每个非空 global 样本 2 个 IPv4 前缀、512 地址、1 条路径，独立数量 main=2；global IPv4 normal 原 mean=2、总体 std=0、upper=2/lower=1。前 6 个冷启动样本的 raw 正常带保留而 main 不可用，第 7/8 个具备六个先前样本而可用。第 9 个空 RIB raw count=0、is_outlier=True，数量 main=None；第 10 个独立非空数量 main=2，但 normal/异常判断主值仍 Unknown。被排除空 RIB 不在第 10 个实际 normal_samples 中，却仍作为 normal_history 依赖保留，未抹掉其历史影响。

每次正常依赖核验均将 Peer 的 source/table_record/index/IP/ASN/BGP ID/presence 与所选 M2 checkpoint/attempt 原行及其固定摘要对照。所选 RIB 的 observation_quality 自然为 0，已明确枚举；本片没有注入人工 quality 正例，也不冒称自然解析错误覆盖。未选 UPDATE 的错误并未因此被隐藏，完整 M2 仍有 1 rejected。

## 三项实际修复代表

- **同计数错 Peer**：显式人工 fixture API 正常完成前，将 10 条 Peer 的 index 加 999、ASN 改为 999999，保留 10 行。完成门禁以 `Peer依赖位置或原属性与实际M2不符` 拒绝。run `244ebe6b05c14bcc9483043fc66e958d` 保持 failed、dataset_id=NULL。
- **未来正常样本**：第 8 个目标 RIB 原有 7 个历史样本，将其中第 7 个换成第 10 个真实未来 RIB 的同值样本；仍有 6 个真实 prior，7 行数量和整体 544 行 normal_samples 不变。完成门禁以 `normal样本不能引用未来RIB` 拒绝。run `3f9ff23612854f04aacc185f0bcbe305` 保持 failed、dataset_id=NULL。不是仅因 prior 数不足而拒绝，也不是降主值后继续 complete。
- **inputs 返回前撤销**：在完整 `_connect` 检查后，由另一已提交连接将 M2 标为 failed，inputs 尾检以 `观察尚未封存` 拒绝，输出连接关闭一次；finally 恢复原 observation_sealed，并核原登记/checkpoint/文件。未把返回后的撤销冒充尾检。

两个完成前反例不是对正式冻结入口的外部攻击，不修改已完成健康回执或重签身份。失败目录和 Parquet 保留；没有把坏候选改成可用结果。没有新增合同外未知 schema 要求或重跑全部原规则/61+52 矩阵。

## 旧 Resource/Q1 与已接受片

直接读取本任务原 `/tmp/domeye-m2-repaired-integration-8233/test_resource_feature_share_ob0`：Resource `0da7110ba4f04310bf152f02d93d879d:53`，dataset `56e489d04a80b69f0935961c51ace99a3b174d6e8ccd58fb2e716c0a5cf657e1`。原十表 1237 行完整公开 scan，计数与原 execution 一致，正文全字段摘要保存在 `旧Resource与Q1.json`。Q1 原 token build `a7a4a4ca95d945398ff1907edf418ab9` 的两页完整 resource 响应与原保存证据完全相等；原 Resource 登记、全部 head、368 个原文件不变。未重导入、未生成一个新“旧结果”伪装兼容。此处正文摘要为本次回读记录，不宣称另有历史全表摘要对照。

刚接受的 canonical/Feature/Detection 仅保存并核对 **271 个唯一原文件 SHA**，包括相应正文/控制制品；没有再次运行 canonical 13 scan 或业务全套。Resource 在另一个人工数据库执行，未修改这些模块的元数据。健康旧同 profile 件无需因本修复默认重产；坏 Resource 如需替换应从合格 M2 另建 run，raw/M2 不重算。本次旧 Resource 属旧公开 profile，不能冒充作者/独立原 11f 同 profile 健康制品兼容证据。

## 实测成本与重复核验

M2 准备 wall 2.054641 秒；冻结 Resource launcher+child wall 6.456753 秒，macOS time -l 峰 RSS 296779776 bytes，不含 PG。完成后健康 Resource Parquet 14 文件/114185 bytes；采样测试根峰 11366991 bytes 包含该根输入/参考/catalog/暂存/输出，不含冻结源码临时目录、PG/WAL，不是严格整机临时峰值。M2 独占阶段 RSS/PG 内存及内部临时空间峰值为 Unknown。

公有新进程整体 wall 35.114874 秒；方法内实际计数如下。inventory 每次扫描完整 16 表，relation validation 每次核 M2 Peer/quality/checkpoint、正常样本与完整资格；它不是重新运行 ResourceComputer，也不是 canonical Replay。

| 新进程方法组合 | 全表 inventory 次数 | relation 完整核验次数 | inventory 累计行/逻辑 bytes | wall 秒 | 进程累计 RSS 峰 bytes |
| --- | ---: | ---: | --- | ---: | ---: |
| inputs | 2 | 1 | 6332/1857076 | 1.659248 | 235765760 |
| 三种 values，全部范围 | 6 | 3 | 18996/5571228 | 5.006983 | 250445824 |
| 16 个 scan，全部范围 | 32 | 16 | 101312/29713216 | 27.449163 | 250478592 |

这 20 个公有方法实测共 **40 次全表 inventory + 20 次 relation 核验**；inventory 累计 126640 行/37141520 逻辑 bytes，是反复扫描量而非新增产物，不包含原值输出扫描、M2/参考与 SQL 内部 I/O，因此不能当总物理读取量。读取 RSS 是新进程生命周期累计高水位，不是阶段独占峰；此处阶段 RSS 为 Unknown。没有为减少上述成本增加缓存或放宽校验，也不称此 Reader 为轻量页面 API或全天性能已验。

同计数 Peer/future 负例 wall 各 3.509106/2.887988 秒；真实 inputs 撤销复查 1.908631 秒，完成 2 inventory/1 relation 后尾检拒绝。原 Resource/Q1 组合只读 1.121470 秒。失败首轮 inputs 测试实际未提交撤销，不计为成功反例；其成本未单列实测，不补零。生产和负例内部重复 SQL 的精确物理读取 bytes 为 Unknown，未为填指标重复生产。所有运行无处理总时限，保留 RSS/内存保护；人工 min_free_bytes=0 不改变产品默认。

## 测试记录、报告与停止

**5 个不同测试最终通过，无 skip。** 首轮 `pytest.log/xml` 为 3 passed、1 failed，55.95 秒。失败是本集成脚本在另一连接 UPDATE 后漏事务提交，关闭连接回滚，故没有实际撤销并报 DID NOT RAISE；产品未修改。保留原日志，修正测试 `with pg` 提交后只重查该项并新增旧兼容：`recheck.log/xml` 为 2 passed、3 deselected，4.25 秒。复用原 M2/Resource，不重产替代，未重复计数。

执行使用项目 `uv run --locked --project backend pytest`、清除 PYTHONPATH，显式 `DOMEYE_RESOURCE_TEST_DSN`/`DOMEYE_RESOURCE_OWN_ROOT`；复查增加 `DOMEYE_RESOURCE_REUSE=1` 与原件目录变量，仅用 `-k 'inputs_revoked or original_resource'`。固定 52f 科学源导出及运行请求均在证据根，测试文件为 `backend/web/tests/test_resource_m3_integration.py`。

最终 `最终封存核对.json` 核实冻结 validation 源/41 文件以及 2c8a 迁移计划、ADR-0002、README 三份授权 SHA 不变。完整差异与 diff --check 已核；自有 55483 PG 已 smart stop，55483/28763 均 status=3、PID/socket 不存在，见 `PG停止核验.json`。未动在途 P1/C5S2/H1、真实 D/P/H、远端、726、独立 A、HTTP、Issue、push 或部署。标准模式、无 Fast，交协调者后停止等待复核。
