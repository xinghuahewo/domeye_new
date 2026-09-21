# Feature 公共 P1 人工集成增量验收

结论：本片有限范围 **GO**。原人工 Feature M3 经公共完整 admit 一次得到可信登记，四接口、两类读者预算、真实关闭失败及原旧八表/Q1兼容均闭合。未重产 Feature/M2，没有新增真实模式、组合 P Token、恢复或生产发布能力。

## 固定基线和完整链

从已接受 Runtime `be534a8031e30235140df84749d2c49592904b9c` 合入完整候选链 `c0804012085284cd019088122231a8510880efbc → 114125611164d4535c086862f78ad9fb40c50618 → 3e9831fbfc5a993abd98c21659774406b6360bbb → 44cd82313ef282f109652aa0e12e88d9e111bbc4 → 33484fe3d56caf5c9a0e73f294a598c887294806 → e58a66d78408475bcebb16cd07f469e17aa61e4b`。合并 `7701126a2129e51e7e3c66b81e44d3534ad6ca04` 无冲突，双亲为 be534a8/e58a66d。完整9文件1165增/107删已核读：三个新P1实现、科学循环/回执行/公开组成值共用提取、两个测试及中文合同。本地只新增专用集成测试与本报告，没有产品代修。

四份固定报告全文与SHA核验：owner报告 `4401c667e7599a3b7544f3f7b10480710adb5ed03475a00249e02ab7f37d35df`；原独立报告 `f94baea7612f87a9a5e881de9d1bb1be275c28700420966243d54a9276d24ea2`；预算修复报告 `dccc82aa514ee681f060bf8101616f19200df96ebdd7cba7398590aa1d0f947c`；最终独立报告 `1f24b06104f700448d29fcbdc1560796586e8e201112f0e7ea9639feb87f1ff5` 与索引 `ea534ecd9b3d43c445d6443bd5e1b6ed7afd72652fe18a3a97aa2a128843129b`。原唯一非有限预算P2已有两轴GO，本轮检验在已接受Runtime上的实际接线。

## 自身原数据与准入

只读原 `/tmp/domeye-feature-m3-integration-8233/feature-m30`，Feature run `819846d60bec4af2986e3ca743d49de3:36`、同库 `feature_m3_8233_4b344c5b5d`。它的实际 M2 是 `d21c4cf77200477fb5648bc32e39e146:29`，原 reference 3行；不是上一片3884。原M2/Feature run、plan、seal、checkpoint、source commits、资格完成锚逐值保持。

本库此前 M2/reference/Feature P1 登记均不存在；使用刚合入的上游 Runtime，正确 fixture_only=True，经实际 inspect/admit/current 新建同原M2/reference的两份依赖，未拿3884登记冒充。本次没有旧Feature P1 Admission，因此不制造历史旧key；原producer代码身份完整保留，新的validator与owner_revision单独记录。

| 对象 | Admission |
|---|---|
| m2 | `0b198c8cf620f9c28d74c74842c4443b35dfbc186210bb1f7e1f603d65cf1724` |
| reference | `d6cd695c71498a47f7b48afeb1dd8ef39f30588ba6e5290e5a87a76b853139e1` |
| feature | `cfc3be8fd02d06a24680c79c728d79bfb14fceaba9e6fbfd6d05965a86bdcecb` |

三份owner_revision均为固定集成合并 `7701126a2129e51e7e3c66b81e44d3534ad6ca04`。Feature显式传完整依赖及其原Runtime，current对每个依赖走真实公共接口。Feature保持fixture-only；256MB临时DuckDB、512MiB临时盘、1GiB进程RSS、2000ms锁预算；上游使用其既有fixture有限默认。没有总处理deadline。

## 六个实际定向场景

`test_feature_p1_integration_8233.py` **6 passed in 12.32s**，首次全部通过，无重跑。测试实例与内部多次预算探针不混加计数，不重跑作者15+12或独立14大矩阵。

1. 完整admit仅一次：原算法ordinary/ir各一次原观察流，在临时期望DuckDB与原12表双向EXCEPT ALL逐typed值/关系/多重比较；包含科学值、必要键、ID、顺序敏感列表、引用、投影/窗口/状态链、参考整列解释及六维资格。它是同规则审计，非独立科学算法oracle；不调用生产入口、不初始化科学run、不重解析MRT。全部119原行另与此前保存的12表全文按完整typed行多重集相等；没有全局归一化，也没有遭遇需放宽的d969排序差异。
2. windows/coverage batch1和17各完整读取，行次序和typed_digest相等；组成值raw/raw_values/values/qualifications与原保存公开证据按完整typed行多重集相等。14窗口/36覆盖；initial仍0窗口、12覆盖，Unknown不补零。复用/current无feature_full_audit、实体全文hash或Parquet正文读取。
3. stage30仅选择Feature自身Admission一个目标，真实第二PG连接50ms更新被阻塞；释放后语句成功并rollback。事件只有一个FOR SHARE，退出不尾audit；没有重跑其他锁矩阵。
4. 两字段max_temp_bytes/max_rss_bytes各拒绝构造NaN/Inf；首批后与耗尽后改NaN/Inf的八次实际读均报ValueError、无receipt。两次合法有限float调整完整读取36覆盖并保持原值，两个有限1对照实际触发资源保护，无receipt。全部路径scratch释放。
5. 真实DuckDB执行close成功后注入OSError，保留错误/traceback；实际close一次、无成功receipt、scratch为空。覆盖新共用资源关闭接线，没有重复全部关闭/坏件矩阵。
6. 原旧Feature `4b9599acdde34fd983c07b90a3712093:80` 经旧read_table全部八表读取，完整表摘要与de027时保存证据相等；原Q1 build `a7a4a4ca95d945398ff1907edf418ab9` 普通/IR共5个保存页面逐字段相等。旧根368文件SHA不变，没有升级旧8表为M3或新造Token。

原12表精确数量：module_diagnostics1、windows14、projection_revisions12、resource_members15、state_deltas22、source_receipts6、decoding_differences0、reference_rows3、input_gaps2、source_qualities2、qualifications36、qualification_receipts6，共119。旧八表数量仍为4/25/16/39/30/10/0/6。

## 本次成本与边界

构造单独计时：上游fixture Runtime 0.000182秒，Feature Runtime 0.000115秒；这两段未开启PG探针，PG数量/RSS为Unknown，不能把事件0冒充无任何IO或内存。后续阶段使用本任务独占PG服务器日志，覆盖上游Selection/DuckLake隐式语句，剔除测量标记；应用显式SQL事件另存，不与实际日志语句数混淆。

| 阶段 | 秒 | PG语句 | Parquet读字节 | 实体hash字节 | 完整Feature审计 |
|---|---:|---:|---:|---:|---:|
| 原M2首次admit | 0.741322 | 1289 | 129334 | 107342 | 0 |
| 原reference首次admit | 0.216652 | 405 | 7916 | 9949 | 0 |
| Feature首次完整admit | 1.547051 | 772 | 271965 | 297468 | 1 |
| Feature复用 | 0.977726 | 345 | 0 | 0 | 0 |
| Featurecurrent | 0.197799 | 172 | 0 | 0 | 0 |
| windows1 | 0.459879 | 361 | 101800 | 0 | 0 |
| windows17 | 0.481425 | 361 | 101800 | 0 | 0 |
| coverage1 | 0.435115 | 361 | 68496 | 0 | 0 |
| coverage17 | 0.447184 | 361 | 68496 | 0 | 0 |

首次Feature准入包含原119行解码、实体hash和一次两模式计算审计；复用/current不重复原规则计算。Parquet计量为被CountedFile拦截的读取，上游原consumer的DuckDB IO未逐字节计入，不冒称全部物理IO或cache miss。

窗口batch1首批0.241549秒，此前已装载全部14 windows和36 qualifications，共50行、19行组、101800字节；batch17同样前置全装载。coverage batch1首批0.227912秒，已装载全部36 qualifications、12行组、68496字节。输出批量有限不等于前置扫描按批有限；按来源/模式筛选发生在装载之后。

Feature首次审计临时盘检查点峰172838字节、进程累计RSS高水位342114304字节；窗口读56919/344571904，coverage读49019/344571904。检查点临时峰不等于关闭刷盘/整机瞬时峰；RSS为整个pytest进程生命周期累计值，含上游准入和此前步骤、不含PG。未发生resource_sample的构造/复用/current内存为Unknown；上游temporary_disk事件与Feature resource_sample不同，前者原事件保留，不把汇总Feature采样0当成上游无暂存。科学审计依旧保存路由状态及参考整列，不能外推真实全天容量、低延迟或性能收益。

## 原锚保护、停止与交付

原Feature/M2联合目录122文件SHA未变，原数据库科学/资格锚逐值相同。另只读复核上一片3884的完整run/CP和M2/reference全部登记，与Runtime最终快照相等。保护清单逐项无变化：Peer原件690、C5S2索引655、Peer索引48、M2P1索引151、H2索引331、DetectionLake索引93、Runtime索引116、2c8a权威3、原本片目录122。清单重叠，不相加为独立文件数；覆盖原H1/C1、Resource/Feature、Canonical、RFD、Detection等前片证据。

证据根 `/tmp/domeye-feature-p1-integration-8233`，含依赖/Feature完整Admission、原锚初末值、12表typed、公开batch1/17全文与receipt、initial覆盖、预算反例、真实关闭、旧8/Q1全值核验、各阶段PG日志/事件/成本、pytest日志/XML、完整差异和历史保护。最终交付索引固定本地提交、报告及全部证据SHA。

自身55483已smart stop，28763仍未启动，PID与socket不存在，scratch为空。标准模式无Fast；未访问他人PG、真实全天/旧试点/726/远端/生产HTTP，未操作Issue、push、部署或扩展恢复。本片待监督核读接受，不宣称完整P或真实业务已可用。
