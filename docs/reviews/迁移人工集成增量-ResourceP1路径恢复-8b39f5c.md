# Resource P1 路径恢复集成：清理错误传递待修复

结论：**REPAIR，发现1项清理证据丢失问题**。原 `/tmp` 制品已直接新准入，Resource 实际 Runtime 构造、首次完整科学核验、公共四视图、三单锁及其余有界读取检查通过；调用方已有主错时，真实内层 DuckDB 关闭错误被 GeneratorExit 吞掉，未保存在主错的 cleanup_errors 中。因此本片不是 Resource P1 整体验收 GO。

## 固定增量与原件

起点为保留的STOP提交 `8c81e9d5d2437ebc972b63944b0eac7c064ea261`，未回滚已授权 Resource365 候选。完整合入 `4725d05e7df5b9ee6b0f659711729ffc076d8792 → f1bfec2b4db265458977c9a55cebd87d66437e29 → 8b39f5cace8a0d29a014e4ba694ac544e932af0a`；实际 merge `69b92d8e38c497de87c5d85774da1a56446dad93`，双亲为8c81与8b39。没有只摘取最后25行。增量5文件172增/11删，无冲突，无产品代修；所有新Admission的 owner_revision 为完整 merge HEAD。

最终独立报告提交 `2f97b71e883236347375693212829c55991416d4`，报告SHA `9fd20eb09e0b451bda430b44410903a5bd92f03925169cc06555fd37765be869`，索引SHA `be11f1422360446125825930e61819d15342649bf5c726f0efae56fa253a4976`，全文核读且摘要相符。首修独立报告SHA `5d88d7c34a87fb3812f58ae8625f7d9c421f582ab5182030507ac581066e0604`、两份作者报告SHA `48ce92fe95b64627d495e5553f2d491e3e4d6b9a126ce9e2ac988c226f2cec86` 与 `0d11678fbbee3c25d686c1738de599cbb5ba259181e3003617c59dd43b074a6b` 已全文核对。不把其路径子范围与构造GO扩展为本次完整Resource GO。

仍使用本任务原 M2 `3d86d4c919d149dfaf2ca962e6efccbd:45`、其原CSV、独立国家参考 `796f3b91dcda4cc0bb9be74da3aee70c` 和原 Resource `c4b0419f11fc4d6ab7f4abb714ae1078:67`。原 dataset `08a7ab5f91b969522b4589f66cc77a053b9ea1b34675b4947d434d0b5b0246fd` 保持。原根 `/private/tmp/domeye-resource-m3-integration-8233`，实际绑定的原 CSV 声明仍为 `/tmp/domeye-resource-m3-integration-8233/inputs/as.csv`，没有改 plan/seal/path、移动输入、重新解析MRT或重产科学制品。

原M2实体现在同时保存原声明 source_path 和规范物理path，实际八字段；Resource原Runtime构造不含M2私有状态，Resource自身实体仍为七字段。`原tmp已直接准入.json` 固定实际原plan条目与新实体，不是另外构造的别名探针。

## 实跑结果

命令：`DOMEYE_RESOURCE_P1_INTEGRATION=8233 env -u PYTHONPATH uv run --locked --project backend pytest -q backend/web/tests/test_resource_p1_integration_8233.py -k 'not test_07' --junitxml=/tmp/domeye-resource-p1-resumed-8233/恢复首验.xml`。

结果 **5 passed、1 failed、1 deselected，38.98秒**。只运行此前6个setup未执行项；已有旧Resource/Q1通过项未重跑。旧STOP的1 passed/6 setup errors日志与报告保存在原目录及38文件索引中，未覆盖，也不与本次累加为成功数量。另有一次定向只读关闭追踪，不计为新增pytest通过项。

- 首次完整准入：原16表3266行、原字段类型和全部typed值不变；原关系审计1次，从18个原元素及原CSV/国家参考调用旧算法复算1次，对10张科学表全部列与多重性核对。审计 Account 为4883行、1783635 typed字节，包含输入/期望/输出重复计费，不是唯一制品大小。国家参考原件与历史完整核对。
- 四个公共视图：metrics28行、normal_bands113行、topology_status10行、coverage40行，全部typed值和多重性与原保存结果一致，完整读取后Receipt complete。仅顶层 qualification 按完整typed元素排序比较，保留重复；原16表及其他列表没有使用此例外。科学字段、原时点及 Unknown→NULL 保留。
- reuse/current：分别复用同一可信记录及核当前资格，没有 inventory_scan、science_compare 或 entity_hash 事件；完整科学审计只在首次及后述独立准入尝试执行。没有把普通目录/PG检查描述为无I/O。
- 三个实际单目标锁：resource.reference、resource.run、resource.admission 各自仅1条 FOR SHARE；另一连接50ms更新超时，释放后更新可执行，无隐藏current/全审计或尾审计。
- 早停、完整读尾撤销：无成功Receipt，原run状态恢复。bool/NaN/Inf构造及实际读取处拒绝；有限累计行预算拒绝；合法600.5秒预算调整继续完成，完整值与首次读取保持。
- 真实DuckDB关闭后错误：完全耗尽时OSError正常上抛且无Receipt；调用方先抛主错时同一主错保留、Receipt仍null、实际数据连接确已关闭，但清理错误未传回主错，构成本片失败。
- 代表坏件仅1个：临时修改原normal_bands均值为12345，实际Parquet与目录统计、旧inventory/dataset/execution全部重新签齐，新admit仍以“固定原观察科学全值/多重性不符：normal_bands”拒绝，accepted数不增。没有重跑作者48项或独立8项矩阵，也未操作输入别名重指向。

## [P2] GeneratorExit 吞掉内层关闭错误

定位于 `resources/publication.py` 的 `_lake` 清理及 `open_reader` 第372行调用 `source.close()`；正文生成器位于 `publication_validation.selected_rows`。调用方读取首批后抛出 RuntimeError，外层关闭生成器使 `_lake` 收到 GeneratorExit。实际 db.close 已完成后抛出的 OSError 被 `_close` 附加到该 GeneratorExit.cleanup_errors；随后 generator.close 将 GeneratorExit 吞掉，外层 `_close` 没有获得错误，原 RuntimeError 也没有 cleanup_errors。

独立追踪 `实际关闭主错追踪.py` 使用恢复后的现有Admission，只包装真实正文连接的close，并观察原 `_close` 的输入及返回；没有替换关闭控制算法。`实际关闭主错定位.json` 证明真实DuckDB关闭1次、原主错对象保持、Receipt为null，且 OSError确实存在于 GeneratorExit.cleanup_errors，调用方cleanup_errors缺失。scratch为空，不是未关闭连接或错误生成成功回执。

既有作者测试在 selected_rows 代理的外层close直接抛错，能由open_reader收集，未覆盖此次真实内层生成器链。不能删去主错清理信息断言或仅以无Receipt判通过。建议由Resource owner修复生成器内部清理错误向外层主错的传递，并对这一实际关闭路径定向补验；无需重产原件或重跑全部科学矩阵。本任务没有代修。

## 原登记及实体恢复

| 新记录 | Admission ID |
| --- | --- |
| 原M2新准入 | `65c4b84a4c1854a7c3d91bd50464f983359d4c15e5f53ab8397614297c6687c9` |
| 原CSV reference新准入 | `911ea5566b25f77e05b4089b08e77461e0956a358daa712be3f0daf06a31841a` |
| Resource首次准入 | `758f95594afe8705d7cf27f3f6390ef9dbf9270ed734061a5cf12a64e193ba69` |
| 恢复原字节后Resource新实体准入 | `0ee3af80ee42af58b7dd8ed19f64e3eb2a59b95d8474d50b7afc4a5ee35bceb8` |

坏件注入后完整恢复原bytes、目录元数据与原Resource登记。实体stat已变化，因此首次Resource Admission的current正确拒绝；随后对同一原binding新admit取得新key，current通过。首次记录按原键逐值保留，没有改签。两个Resource新登记、一个M2和一个reference最终保留；坏例自身不增加accepted。不将恢复后的实体新版本称为真实源码升级实验。

原16表3266行逐完整typed与先前原结果相等；204个原文件SHA、原Resource/reference/M2登记全值保持。全部前片保护清单再次相符：Peer原件690、C5S2索引655、Peer索引48、M2P1索引151、H2索引331、DetectionLake索引93、Runtime索引116、FeatureP1索引69、H5索引483、2c8a权威3及上次Resource STOP索引38。清单重叠不累计为独立文件数。旧Q1不无故重跑，28763及其他模块业务入口未启动。

## 成本与停止

PG计数来自只对本人55483实例启用的实际statement/execute日志，写入本轮新文件，marker核实绑定且剔除探针连接。包括各阶段内部查询，不能当作单一Python函数调用数。没有其他业务阶段并发；保留原日志片段。各阶段成本如下：

| 阶段 | 墙钟秒 | 实际PG条目 |
| --- | ---: | ---: |
| 原M2新admit | 1.209070 | 4153 |
| 原CSV新admit | 0.343756 | 512 |
| Resource首次完整admit | 4.228669 | 3776 |
| Resource复用 | 1.072847 | 710 |
| Resource current | 0.629113 | 452 |
| metrics完整读取 | 1.351477 | 971 |
| normal_bands完整读取 | 1.283931 | 971 |
| topology_status完整读取 | 1.440543 | 971 |
| coverage完整读取 | 1.403593 | 950 |
| 首批1行后早停 | 0.753285 | 519 |
| 代表normal坏件拒绝 | 3.778900 | 2641 |
| 恢复原字节后新实体admit | 6.133573 | 4228 |

四视图首批依次0.725987、0.673089、0.732382、0.800556秒。首次admit内部完整validate为2.480825秒，与完整admit墙钟分开。早停交付1行时已有2个目标批事件及先前coverage/qualification读取，不称只物理读取1行。坏件和恢复新准入另行发生完整审计，不将整个会话称作只复算一次。

观测到的Python进程生命周期RSS最高354271232字节；不是阶段独占峰值、不含PG。事件分别记录selected批行量/Arrow字节与实体hash等；物理磁盘读取字节、PG峰值、阶段独占峰值和未观测临时盘峰值均Unknown，不用typed/Arrow量代替物理I/O，不作全天容量推断。保留单调用有限预算、内存/临时盘/锁保护，无整个任务处理期限。

私有55483已smart停止；55483与28763均status3、isready2、无PID/socket，scratch为空。原件和全部登记保留。证据根 `/tmp/domeye-resource-p1-resumed-8233`，全量索引 `交付索引.json` 固定本轮日志、脚本、全部typed值/Receipt、前后登记、故障追踪、恢复及停止证据；索引和报告SHA随最终提交回传。

完整产品增量及本地测试差异已审查，diff --check通过。标准模式，无Fast；未接入Canonical P1/window、Detection P1、Country、真实profile、D/P/H生产、726、远端、HTTP、Issue、push或部署。等待清理错误传递修复，当前REPAIR停止。
