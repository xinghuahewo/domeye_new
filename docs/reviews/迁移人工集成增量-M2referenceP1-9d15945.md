# M2/reference P1 与 consumer 有限人工集成

结论：**GO，限本报告的既有人工 M2、P1 四项公开操作与 consumer 小型出口，待主监督接受**。首次实际准入、复用、current、单锁及完整/子选择 Reader 已通过。没有重产 MRT、Checkpoint、reference 或其他下游模块；尚未实现或验收组合 P、Feature P1、生产发布和全日迁移。

## 固定版本与范围

- 前置已接受点：`d15011b45b934fceebdcc0d03ad0e2be64e2455b`，本轮不重做 C5。
- 实施链：`7571c164756c8039f54933ad3f70f6e5745c5d6c` → `672f8c641207f38f5a9214592ec36e4136ec34ea` → `d9697ba9104f71acb5fa338f879e8d02dee03f8d` → `9d1594539077385b972efe47cfe0eca568ec1bae`；后三项逐一核对唯一父。`513a621` 未作为实施替代。
- 实际 merge：`c0804012085284cd019088122231a8510880efbc`，双亲为 d15011b 与 9d159453，无冲突。原已集成 Peer 排序仍在，未重新套用。
- 全文读实施代码、测试与中文说明，作者联合报告 SHA `38194bb6a5f796fc0d9bc2c4d77e849d499710cc6df7125538ba63b94376671f`、独立报告 SHA `ad547e6af1c4ad27c456e8cffd1bd0496a89629ece0a0ef310698edf8425e863`、独立索引 SHA `a4a35424f753e22e6bb0896ec582b19708dfc1237a7892df976fb11a63aa2aad` 均匹配监督绑定。
- 本集成提交只增加两个定向测试/测量文件及本报告；产品实现保持 9d 原字节。证据根为 `/tmp/domeye-m2-p1-integration-8233`，不提交真实数据库或制品。

## 实际原对象与新准入

复用自己原 Detection M3 的输入 M2，未使用作者/独立审阅者数据库：

| 项目 | 实际值 |
|---|---|
| PG | `det_m3_8233_input_3b92d0d6bd`，socket `/tmp/domeye-integration-detection-8233/socket`，端口 55483，无 TCP |
| system identifier / database OID | `7684750556297244402` / `39932` |
| 原 M2 | `3884fdb6cdb7408d92f78a85db70a6af:26` |
| 原 seal digest | `3f26834c1e026ea01a19928f04f602ae282d32c6393ddb950c0145c7fbc9000c` |
| 实际 data root | `/private/tmp/domeye-detection-m3-integration-8233/detection-m30/m2/data` |
| 新 M2 Admission | `6504f5d217344879b7970935ad0e480145f9755f5b874b29e96e3aebb7a05685`，35 实体、15 锁目标 |
| 新 reference Admission | `d6011b7d1de719f84fcee5818bd9ee910c497836640dcd1f9fc969c1b714cdbc`，4 实体、16 锁目标，仅依赖上述 M2 |
| 首次 owner_revision | `c0804012085284cd019088122231a8510880efbc`，后续测试/报告提交不改签 |

此前本任务**没有自己的 672 Admission**；首次查询确认 `observation_publication.m2_admissions` 尚不存在。本轮不制造旧 Admission，也不冒称实测旧 672 key 迁移。新 M2/reference 各首次准入一次，再分别复用原 key、current；原 M2 的 run、snapshot、attempt、seal 和 CP 身份没有重建或改签。

原 InputBinding 全文和所选 MRT 原序保留：reference CP ordinal 0 与首个 MRT CP ordinal 1 / upstream rank 0 分开；reference 的 selected_sources=[]。完整绑定、两份 Admission、各 ReadReceipt 和全文 typed 均已保存。

## 全字段与原序对照

以实际固定 Selection 的八表物理字段类型和全行查询作为期望，按原来源顺序逐 CP 读取，和公开 Reader batch 1/17 的完整 typed 输出逐项比较；不把输出重新排序后宣称原序相同。成功 Receipt 只在耗尽且正常退出后取得。

| M2 view | 全行数 | 对照 |
|---|---:|---|
| messages | 18 | batch 1/17 全值同序 |
| elements | 22 | batch 1/17 全值同序 |
| paths | 13 | 10 个唯一 path_key，跨 CP 的 3 项重复仍保留 |
| peers | 3 | 全字段含原索引、身份与存在标志 |
| eor | 0 | 原空表类型与合法空读取；非新增业务成功 |
| associations | 13 | 全字段同序 |
| quality | 2 | 全字段同序 |
| references | 24 | 11 个原参考 CP 全字段同序 |

单独 reference Admission 返回所选原 CP 的全部 9 行。额外实际更新源子选择返回 14 条 messages；空来源选择返回 0 行且 Coverage 的 business_absence 仍为 Unknown。未将参考 CP ordinal 当成 MRT rank，未执行参考投影生产。各表完整摘要见 `全文对照.json`。

## consumer 与小型实际出口

真实原 Reader 直接读固定 M2 的结果，与原 CP 全行临时副本结果逐项一致。临时副本仅改变实际 DuckDB 插入枚举和默认 NULL 顺序，运行实际 ObservationReader.stream，仍执行原来源资格检查；同一组原行的 typed 多重集相等，按源保留 boundary/message/element/quality 各自原序比较，不要求不同 batch 的包装交错完全相同。

原 CP 组有 18 messages、22 elements、2 条消息诊断、3 个 Peer，跨 seed 0/7/2、batch 1/17 及隔离新进程一致。完整 consumer 摘要为 `6e0104f16e7f5474bf2dcba9e57464fd1e0bd6a5dec3dc2594ca8eece195c11d`。

原件的 source quality=0、EOR=0，不能据此声称覆盖重复和所有 NULL。另设**临时副本增强组**：增加每源两个相同的 source quality Unknown 行，并复制 Peer 和增加 BGP ID=None / present=False 条目。重复和 Unknown 在不同枚举下完整保留；它不是合法新 M2、没有准入，也未回写原件。既有 metadata_order 定向测试补充 EOR、同键 Peer 属性、AS_SET/raw bytes 与未知 origin。

两组均送入实际 Feature `Qualification` 和 Detection `Results.event_revision` 小型出口，完整资格/事件行与身份摘要稳定；两组各有真正隔离 Python 新进程对照。该出口使用明确人工窗口和计数上下文，仅验证元数据枚举对出口稳定性，不代表完整科学算法、Feature/Detection 重产或产品验收。

## 三项修复与跨界

1. current：在真实 Selection 连接上注入元数据主错，close 实际释放连接后再抛次错。主异常对象、类型和原回溯保留，次错附 cleanup_errors。既有 `_closing` 定向实例验证多个 owner 逆序逐个尝试，未用 mock 结果代替真实连接释放。
2. 必需 event_id 与 RIB→Peer：从上述真实基线 CP 复制完整八表到私有内存 DuckDB；健康 `_fk` 通过，分别受控 SQL 将 RIB event_id 置 NULL、peer_index 置 9999 均被拒绝，事务回滚后再次通过。这里是实际原行副本上的校验接缝反例，不宣称重新生产了坏 M2 或完成坏 CP 的全公开 admit。八表必需键及合法科学 NULL 的既有定向实例另通过；没有为两项反例重跑 M2。
3. 映射：在自己的实际 PG 固定快照可见 elements 文件目录临时置 mapping_id=999，公开 current 和 admit 均明确拒绝 column mapping；finally 恢复原 NULL，随后真实 current 通过。原 Parquet 字节不改。这是目录注入，**不是原生 schema 演进**。

单一有意义跨界：公开 hold_lock 只取得 M2 Admission 一个 FOR SHARE；另连接以 100ms lock_timeout 更新同记录确实阻塞。持锁期间 reference current 复核 M2 依赖，reference Reader 读首批后早停，无 Receipt，临时目录清理；退出锁没有尾 audit，释放后同更新成功并回滚。未重跑 16 锁、36/47 矩阵或 7 次 kill。

## 首次公开操作实际成本

每阶段用当前自身 PG 日志随机服务端标记确认测量来源，包含隐式 DuckLake PG SQL；探针标记事务排除。表中正文 SHA、Arrow 和文件 read 来自实际事件，不是估算。

| 阶段 | wall 秒 | PG 语句 | 实体 SHA 字节 | 直接 Parquet 行组 | 文件 read 字节 | Arrow 解码字节 |
|---|---:|---:|---:|---:|---:|---:|
| 首次 M2 admit | 1.2846 | 3700 | 89647 | 20 | 90424 | 43531 |
| 首次 reference admit | 0.3313 | 497 | 10976 | 1 | 6485 | 3468 |
| M2 复用 | 0.0912 | 83 | 0 | 0 | 0 | 0 |
| reference 复用 | 0.2732 | 199 | 0 | 0 | 0 | 0 |
| M2 current | 0.0760 | 62 | 0 | 0 | 0 | 0 |
| reference current | 0.1741 | 120 | 0 | 0 | 0 | 0 |
| M2 八 view 完整读取，batch 1 合计 | 1.6205 | 992 | 0 | 20 | 90424 | 44160 |
| M2 八 view 完整读取，batch 17 合计 | 1.4703 | 992 | 0 | 20 | 90424 | 43531 |
| reference 9 行，batch 1 | 0.3868 | 240 | 0 | 1 | 6485 | 3503 |
| reference 9 行，batch 17 | 0.4116 | 240 | 0 | 1 | 6485 | 3468 |
| 更新源 14 行，batch 1 | 0.1856 | 124 | 0 | 1 | 12653 | 14829 |
| 空来源选择 | 0.1825 | 124 | 0 | 0 | 0 | 0 |

单锁/依赖 current/早停/两次另连接更新是一段组合测量，成本在 `单锁跨reference早停.cost.json`，不冒充单独 lock 的成本。其 SQL 片段保留。全八 view 合计包括八次正常入读/尾 current，不能读作一个 SQL 或一个流的成本。

首次 M2 正文第一遍 95 行、typed 75791 字节；reference 9 行、typed 5932 字节。两条全文枚举路径之外另有每表上限、FK 与索引查询，不能称总共只有两次 scan。Replay=0。current/reuse 没有正文 hash、admit body 或直接 Parquet 解码，但仍访问固定选择小索引与目录，不是无 IO。

**首批加载边界实测**：messages batch 1 首次输出约 0.0977 秒，此时首个 CP 的 4 条 messages 已全部解码，输出迭代已取 2 行才推出 pending 的第 1 行。更新源子选择首批约 0.1003 秒，已经解码该 CP 全部 14 行。实现逐所选 CP/表全部装入临时 DuckDB 后排序，再输出批次；输出行/字节预算成立，不能声称输入装载量由 batch 限定。没有扩大成全 seal 每次装载，但大 CP 的首批延迟与临时成本仍需后续容量实证。

运行设置保留 DuckDB memory_limit=256MB、临时盘上限 512MiB、单锁等待限制；没有总处理 deadline。首次 admit 的观测临时盘最大分别 101821/18975 字节，ru_maxrss 为同进程生命周期峰值 233209856/243924992 字节，不能称阶段新增或 PG 内存。补充测试与末次核验增加 4GiB RSS / 512MiB 剩余盘 guard。文件 read 含缓存/重复读取，不是物理磁盘 miss；DuckLake 内部索引 IO 字节未独立测量。

## 验证、原件与停止

9 个不同测试实例通过：本集成 4 项、metadata_order 3 项、repair 2 项。首轮实际集成 3 passed / 8.53s；consumer 与定向补充 6 passed / 5.18s；补直接原 Reader 1 passed / 3.49s；补授权门禁/资源 guard 后定点复核 3 passed / 4.44s，重复不加计。

补门禁时文本替换误及新进程脚本字符串，产生一次收集期 SyntaxError，未执行数据库操作；日志 `资源保护复核.log` 保留。修正仅测试脚本，后续三项通过，没有发现或放宽产品校验。所有测试使用项目 uv 锁定环境；独立新进程显式绑定本项目代码路径。

原 M2 所在完整目录 **147 个文件**的前/后/最终 SHA 相等，原 runs/CP 全登记逐值相等；PG mapping 注入恢复，未改原文件。另对既有固定证据锚实核：其他模块 690 文件、C5 S2 索引 655 文件、Peer 排序索引 48 文件、2c8a 权威 3 文件全部匹配。这些集合有重叠，不相加宣称唯一文件总数；没有重新计算其他模块。

末次 M2/reference current 均通过；本任务 PG 55483 已 smart stop，历史 28763 同样无 server running。停止证据及完整文件索引留在证据根。未执行真实 H/D/P、远端、726、独立 A、HTTP、Issue、push 或部署；本轮不外推全国实际影响和原因。
