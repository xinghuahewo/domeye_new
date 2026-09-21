# M2/reference real Runtime 人工集成增量验收

结论：本片有限范围 **GO**。原健康人工 M2 可在 fixture 与 real-candidate/v1 两种模式下经公共 admit 获取模式独立的新登记；real 模式实际八表/reference 全值读取、锁和预算使用点拒绝闭合。只是 real 控制路径使用合成输入，不代表真实全天、D/Ppre、业务 full 或组合 P 发布通过。

## 固定链与范围

从已接受 `5100eba09670119a8653bf1041287f8c73504d45` 合入完整 `c0804012085284cd019088122231a8510880efbc → da8e52affbbb00aab01418da1604b85141be2d4a → cbd6f57dd47e609b2a9fa68a6c76c2d982dfba20 → 4725d05e7df5b9ee6b0f659711729ffc076d8792`。无冲突合并提交 `3b75e5331a399678220f919ec38bd9a95307c7ce`，双亲为 5100eba 与 4725d05。完整候选差异六文件、400 增/25 删已读并核对；本地仅新增定向集成测试与本中文报告，没有产品代修。

前置四报告全文读取并核 SHA：作者 Runtime `799b77fe382afa884b64be2864d77b3685df1e30eab3867bed2288afcd00ada9`；原独立 12b1a6fa 报告 `aa30beada0739814196a45ed13610ebe7d76eaada469eb4c4aa391eb24331b8f`；作者预算修复 `8da1062d4b71a46a685d01e8113fb5269a730a490e20b545c70c19f54f68cafd`；最终独立 53fecb 报告 `1c52c30c5f8898e5f389658ee6e5a4c7eba8ac8ee07cbb78fb8bd218543cc1f2`。原唯一 NaN P2 已由最终独立两轴 GO 关闭，本片验证合入后的自身实际接线。

标准模式，无 Fast、无总处理时长限制。只启动自身 55483/socket PG，无 TCP；没有真实数据、远端、726、旧试点、独立 A、HTTP、Issue、push、发布或 Feature 合入。

## 原件与新准入

原目录 `/tmp/domeye-detection-m3-integration-8233/detection-m30`，原 M2 `3884fdb6cdb7408d92f78a85db70a6af:26`，原库 `det_m3_8233_input_3b92d0d6bd`。完整 expected_m2_binding 取原 `原绑定.json`；manifest 取该绑定原 plan.manifest，未调用 plan_for、parser、M2 producer 或重盖旧生产版本。原 11 reference CP、两个 MRT CP 的 plan/seal/checkpoint 登记逐值不变，147 个原文件 SHA 不变。

real 模式显式五预算：256MB memory_limit、512 MiB max_temp_bytes、2000ms lock_timeout_ms、4 GiB max_rss_bytes、128 MiB min_free_bytes，明确允许原根与本片输出根，scratch 为本片既有独立目录。fixture 与 real 规则摘要、四个独立登记 key 分离；reference 各依赖同模式 M2。

| 模式/对象 | 新 Admission |
|---|---|
| 人工 M2 | `4428f0ffcd94f8e6b30e79db8c2d1cd3e8c59d928f5fd9e5ef6516d84b70081f` |
| 人工 reference | `3df1773e63f0b79cb6545ac727e7a8c71cfeedfc2e67ead094590731e2e012bc` |
| real 合成 M2 | `dd8c54195bda095834b1317bacd84bea414f2c609e5c130cd95154a8825a46b2` |
| real 合成 reference | `3c928ce17579349d803b2ef6dcba2512795e980882e5315cbab28dfcc6c45299` |

四份 owner_revision 均为集成合并 `3b75e5331a399678220f919ec38bd9a95307c7ce`；每份重复 admit 全文复用，current 正常。原 6de M2 Admission `6504f5d217344879b7970935ad0e480145f9755f5b874b29e96e3aebb7a05685` 与 reference `d6011b7d1de719f84fcee5818bd9ee910c497836640dcd1f9fc969c1b714cdbc` 在原 key 全文保留，新规则 current 明确拒绝旧源码版本。登记由各一条增为各三条，无删除/改签。自身没有 cbd 历史登记，不制造或声称验收该历史。

## 实际验证

七个不同 pytest 场景最终全部通过。首次 6 passed/1 failed（8.66 秒）；唯一失败是复用候选测试的顺序反例只反转 inputs[1:]，在本任务两个 MRT 输入上实际未变化，因此不该被拒绝。本地测试改为完整交换两项，确认输入确实改变后再期望拒绝，仅重跑该场景：1 passed/6 deselected（0.97 秒）。原失败日志保留；未改产品、重产输入或重复准入/锁/预算矩阵。

- 模式与范围：默认/未知/人工真实混用拒绝；real 五预算缺项拒绝；原 manifest 的 collector、URI、实际顺序、参考摘要冲突拒绝；错误 run/snapshot/来源序、错误自报完整 binding 与实际 Selection 不符拒绝。正常实际 inspect 全文等于原 binding。空调用方 guard 下实际 RSS/余盘保护仍生效。
- 人工/real 的 current、hold_lock、open_reader 互用拒绝，reference 跨模式依赖拒绝。各自身新登记复用/current 正常。
- real Reader batch=1 的全部八表及选定 reference 与上一片保存的完整 typed 原值逐字同序一致，不作排序归一化。八表行数为 messages18、elements22、paths13、peers3、eor0、associations13、quality2、references24，共95；选定 reference9。重复诊断、Peer 属性/次序、未知值及参考CP含义随全文保留。首 MRT upstream_rank=0 与 reference checkpoint_ordinal=0 分属不同层，MRT 实际 CP ordinal=11/12；reference selected_sources=[]。
- M2/reference 各选择自身一个 Admission 锁目标，实际第二 PG 连接 50ms 更新超时，释放后更新语句可执行并 rollback；各只有一个 Runtime FOR SHARE，不重跑16锁/7-kill矩阵。
- 首批后分别将 RSS、最低余盘、临时盘设 NaN，M2/reference 共六次实际读均交付1行后报“资源参数必须为有限正值”，无 receipt。三项有限超限对照均拒绝，三项合法有限调整均读完18条消息，与原全文一致且获得 complete receipt。不能外推所有参考缓冲都在第二行前停止。
- 耗尽后调用方尾 guard 注入 NaN，尾检拒绝且无 receipt；所有路径 scratch 清空，预算恢复后 M2/reference current 通过。没有重复完整双错/关闭矩阵。

## 成本与边界

real 构造含原参考格式解释，单独实测 0.020572 秒、13 次内部资源检查，累计进程峰值 RSS 121946112 字节。该阶段没有 PG 统计探针，PG 次数为 Unknown；Parquet 事件0不表示参考解释零 IO。输入中11个参考格式解释发生在构造，不能隐藏到后续 current 的“无正文”说法里。

下表为本任务独占 PG 实际日志探针（含 Selection/DuckLake 隐式 PG 语句，剔除测量标记），不是仅 Runtime pg_sql 事件；应用 Parquet read 字节不等于物理磁盘 miss。

| 阶段 | 秒 | PG语句 | Parquet read字节 |
|---|---:|---:|---:|
| real首次M2准入 | 0.873129 | 3700 | 90424 |
| real首次reference准入 | 0.316786 | 497 | 6485 |
| real_m2复用 | 0.089710 | 83 | 0 |
| real_m2current | 0.074778 | 62 | 0 |
| real_reference复用 | 0.254085 | 199 | 0 |
| real_referencecurrent | 0.149186 | 120 | 0 |
| m2_messages全读 | 0.182682 | 124 | 20620 |
| reference_references全读 | 0.334948 | 240 | 6485 |

首次准入包含实际绑定、实体hash、正文验证和登记，不以复用或 pytest 总时长代替。current/复用未触发直接 Parquet 正文枚举，但仍有元数据/索引检查，不能称零 IO。完整八表每阶段日志及事件逐项保留。

M2 消息首批 0.086281 秒、仅交付1行时，已装载首选 CP 的4条消息、读取7967字节；完整18条消息读取20620字节。reference 首批 0.187989 秒、交付1行时已装载该 CP 的9行、读取6485字节。这是所选 CP 表先完整装载、再排序输出的既有边界，不是首批只读一行。阶段保存 first_batch 的原事件序列。

临时盘事件峰值：M2首次准入101821字节、reference首次18975字节；消息全读40171、reference全读17900。属于保护检查点观测，不是严格整机峰值或累计写入。resource_usage 是当前 pytest 进程生命周期累计峰值，含此前阶段、不含 PG，不能作每阶段新增内存；PG RSS、物理 cache miss 与全天容量 Unknown。NaN 路径的耗时仅说明停止位置，不作性能提升结论。

## 保护、停止与交付

历史映射逐项 SHA 无变化：Peer 原件690、C5S2索引655、Peer索引48、M2P1索引151、H2索引331、DetectionLake索引93、2c8a权威文件3、原M2目录147。清单有重叠，不相加冒称独立文件数。原 plan/seal/checkpoint 与两个真正已有 Admission 在初末快照逐值相等；保护涵盖此前 H1/C1、Resource/Feature、Canonical、RFD、Detection 等既有证据。

证据根 `/tmp/domeye-runtime-integration-8233` 保存四新 Admission、原/末登记全文、所有typed与receipt、预算反例、实际锁、阶段成本/PG日志、pytest失败及修订日志、完整差异和历史保护结果。最终 `交付索引.json` 固定本地提交与报告/证据哈希。55483 已 smart stop，28763保持停止，PID与socket不存在；scratch为空。

本片仅交付 Runtime 人工集成，等待主监督接受；Feature 未合入，真实 D/Ppre 与全天/P发布尚不可用。
