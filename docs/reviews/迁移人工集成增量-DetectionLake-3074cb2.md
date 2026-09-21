# Detection lake/v2 人工集成增量验收

结论：本片有限范围 GO。既有人工 M2/reference 可由冻结新进程产出 lake/v2，三张正文表仅保存在 DuckLake/Parquet，完整验收与独立新进程全读通过；原镜像、旧 typed/v1 及原 Q2 保持可读。本次不构成公共 P1、真实数据、恢复或部署验收。

## 固定边界

集成基线 `941f040a838d90771fee102702c20e59c4c47826`；候选链 `98b29b659a1a957a0dc0a8906b61d0519144d837 → 7cb83018e12cb6dbd285255d3c66cd00f43d4f92 → 3074cb2dfefc305e50732418c0cd929a88dbeb49`；无冲突合并 `a8b89e2b789e80f8409f34b5fc5f2c9708a1f90b`，双亲为本基线与最终候选。九个候选文件完整差异已审阅（622 增、43 删）；本地仅新增专用人工集成测试及本报告，未修订产品逻辑。

原实现报告 SHA `0440f7b9719107a68b41c3b5f2e696c930dcb48da10a0ccf08dc3e2fd64aef9c`、修复报告 SHA `eda75942f68d0bd0de9460b049f3afc4f3c6cece0347c18198fde39fbad13455`、最终独立复核报告 SHA `49980dc9f436fa74636627462d702ace24406a3c1cc4383e32b909116a15586d` 及其索引 SHA `c72148325504f28d2d489b33edfe9d7f4f97e956763e79ecef4df5adf4bfae63` 均已全文读取并核验。仅使用自身 55483 PG 及人工制品，未访问作者数据库、远端、726、独立 A 或 HTTP，也未推送、发布、操作 Issue。

## 正式生产与全值比较

复用原 `/tmp/domeye-detection-m3-integration-8233/detection-m30` 的健康 M2 `3884fdb6cdb7408d92f78a85db70a6af:26` 与原 reference，不重解析或重产输入。磁盘下限 128 MiB、RSS 上限 1 GiB，保留批量限制，不设置总处理超时。两次正式生产各一次，冻结清单 48 个文件均逐个匹配当前集成源码。

| 场景 | 原镜像 run:snapshot | 新 lake/v2 run:snapshot | records/state/m3 | 资格 revisions |
|---|---|---|---|---|
| gap | 812500d196c5482b8bcf2592e32d5973:7 | 0774d9ed031d4fb7b25b6ad8a8faff06:7 | 131/178/32 | 16 |
| admitted empty | 356ff5a78e414c4fa3a7c5062b92a427:7 | 9c1a941bf0a840e18f79dca4df2608e8:7 | 10/100/1 | 0 |

两次均有完整 54 个状态头，显式空容器保留；无事件产物仍有一条 source coverage。资格 main 未把不可归入主统计的数据改成成功。固定 schema、逐字段 typed 值、行序、科学索引、终态科学键及最后记录哈希/连续 revision 均由完整三表验收约束；独立新进程以 batch=3 全读、逐行类型编码摘要核对，不是抽样。PG 新库三正文 relation 均不存在，旧镜像三表行数及原全值证据保持一致。PG 仍承担 runs 与 DuckLake 目录写入，不能称为零 PG 写入。

跨旧镜像仅采用既有有限状态 set/MOAS 比较规则、资格 component_run 与派生 entry_id 的运行身份替换，以及主监督明确批准的 source_message 两类元数据规则。旧镜像早于 d969/9d→6de consumer 排序修复：peers、quality 完整 typed 记录以保留重复的多重集比较，同时新 peers 严格按 table_record/index/bgp_id/ip/asn/bgp_id_present NULLS LAST、新 quality 按 source_id/message_id/code/detail NULLS LAST 校验（消息内部 source/message 固定）。其余普通列表、MRT/消息/元素次序、定向角色、incident/revision 严格比较。原始差异保留：gap 逐字段差异 SHA `c60fe3c4e2d740726573294e5e8fa5e0f8c23670fababb7883d5c3caa7a3f974`，empty SHA `78058d5c8dce39f7e3c21ee8f945f067828288bedecd20741fcfffaee9110558`。未改写旧镜像或放宽产品规则。

## 拒绝路径及兼容

两个实际 Writer synthetic 故障各注入一次：移走一个 last_records 科学键、但保持总行数及全部状态头，得到 `终态科学键集合未闭合`；四个非空 attacker_asn 索引改为 999999，得到 `科学typed索引与原正文/固定规则不符：attacker_asn`。两者 PG state=failed、无 ready、读取拒绝；不是正式冻结科学运行。

旧 typed/v1 `c878aa403ab54988b6cac74c61acefc3:5` 完整 records=26/state=97 与历史摘要一致。原 Q2 token build `3aabb69eca52450685295ad4a17e443f` 所有 records 分页与正文全值一致，events 完整响应等于原证据，head 不变，368 个原文件 SHA 不变；新 M3 binding 按预期拒绝旧 profile。自身没有旧 lake/v1 产物，因此不声称历史 lake/v1 实际读取验收，也未伪造旧产物。

## 测试与成本

五个不同场景最终通过：首次 2 个故障通过；有限修订运行旧 typed/Q2 通过；最后正式对照 2 passed（1.91 秒），复用既有正式产物，无重复生产。保留所有失败日志：初次比较遇到上述元数据版本顺序差异及旧错误文案正则；随后本地测试误配了另一旧 run 的路径，修正为历史 c878 根；元数据断言误写 kind 而非 record_kind 已修正。均为本地测试口径/实现修订，没有隐去产品失败或扩大归一化范围。

| 成本 | gap | empty |
|---|---:|---:|
| 生产秒 | 2.0874 | 1.7095 |
| 正常三表读取秒 | 0.1497 | 0.1416 |
| 生产内完整验收秒 | 0.0658 | 0.0491 |
| 生产进程累计峰值 RSS 字节（含此前步骤，不含 PG） | 272236544 | 259768320 |
| 规范 typed 正文字节合计 | 636808 | 36584 |
| Parquet 字节（各 3 文件） | 59593 | 16631 |
| 50ms 采样输出峰值字节 | 145005 | 101367 |
| 采样 staging/temp 峰值字节 | 12288 | 12288 |
| 专有 PG WAL 增量字节 | 542872 | 541600 |
| 正式生产及全读 PG 日志语句数 | 1045 | 1014 |

输出采样可能遗漏短峰，不是累计写入；typed 字节不是 SQL 协议/物理 IO；WAL 含目录与后台写入。完整验收各扫描三张正文表，并验证实际 M2/reference 绑定；身份 integrity 明确 `public_admission=not_performed`。原 M3v2 默认入口保留，本片仅选择 lake/v2；不推断公开准入或恢复能力。

## 制品保护与交付

证据根 `/tmp/domeye-detection-lake-integration-8233`，含 request/result/ready、原新全值、有限排序检查、独立读取摘要、成本、PG 日志、故障产物、全部 pytest 日志及 XML。`交付索引.json` 固定最终本地提交和所有本片证据哈希；自身 PG 55483 已 smart stop，28763 保持停止，PID/套接字不存在。

历史保护清单逐项复核无变化：Peer 原件 690、C5S2 索引 655、Peer 索引 48、M2P1 索引 151、原 M2 目录 147、H2 索引 331、2c8a 权威文件 3、本片原 Detection 目录 147。清单相互重叠，不把数量相加当独立文件数；H1/C1、Resource/Feature、Canonical、RFD 等按既有保护映射保持不变。

当前仅完成 Detection 人工集成，等待主监督核读接受。下一片 Runtime、Feature 不在此次执行范围。
