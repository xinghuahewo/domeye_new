# Resource真实Runtime与Detection窗口有限组合

结论：**GO，仅本次人工制品有限组合**。Resource原四代表视图、真实锁门禁已实际走通；Detection唯一必要新窗口输出与已集成2e real Runtime实际接合，三表及两结果视图正常完成，尾资源失败无Receipt。没有真实全天、Country或全P验收。

## 固定组合与冲突处理

稳定基线 `8d2c418350cc6f9d9926a91b2fa7217499c7996c` 先完整合入Resource `65b2ae9d77bc33fcecda565dacd7a8c57b53d9e1`（含1901真实模式与锁门禁修复），merge为 `9f59d43c8f6ec3b47973e52953dfecac2b0863d6`；再完整合入Detection `e7239436e7c7a2c5d124228c61e5177ae39730ea`（e42→3092→e723）。实际执行HEAD为 **956723acb1f0d327092309d9c92c09838edaf48d**，双亲9f59/e723，保留原2e真实Runtime链。两个新owner Admission的owner_revision均为该完整HEAD。

Resource无冲突。Detection publication.py及publication_io.py自动合并；逐差异检查确认完整绑定/模式/资源检查与新窗口约束、结果view和Receipt窗口引用同时保留。唯一冲突在接口文档尾部：两侧分别追加真实Runtime和窗口章节，最小处理是保留两段全文，没有改写算法或重新设计接口。两段原候选历史措辞保留，本轮实际接受范围以此报告为准。使用resolving-merge-conflicts技能完成冲突处理与必要检查。

Resource两产品文件逐字节等于65b2；Detection的lake_integrity、m3_runner、m3_store、result_selection、result_window逐字节等于e723。合并的两个公共接口文件另保存实际SHA及叠加差异。完整组合差异已核，diff --check通过；本任务只新增有限测试和报告。

独立报告全文核读及SHA：Resource `7d020dd8cab6776e40e68dccc13e9c18a6503b70107597358e07565ad1eb5c80`；Detection窗口 `a56b9bb4f163ee0a84ca21fc5170e298be93429a481ce7f5df42b9fb3f9ee2c8`。独立29/48项、clean/ET矩阵不计为本轮实测。

## Resource原制品真实模式接合

复用原 `c4b0419f11fc4d6ab7f4abb714ae1078:67`、M2 `3d86d4c919d149dfaf2ca962e6efccbd:45`、原CSV和独立国家参考。完整binding、原frozen执行资格、fixture://来源及Unknown原值保持。仅必要一次M2 real准入、一次CSV real准入及一次Resource real准入，没有生产科学数据或重复16表外部科学对照。

显式完整Resource binding、DSN、输出、允许根、独立scratch和逐依赖Runtime；各资源预算保持，max_seconds为None。inspect等于原完整binding，current/reuse返回同一新Admission，没有inventory_scan/science_compare/entity_hash。首次必要admit自身的完整审计照常执行，没有绕过。

| 公开view | 行数 | 原值对照 |
| --- | ---: | --- |
| metrics | 28 | 原路径恢复片完整typed值 |
| normal_bands | 113 | 同上 |
| topology_status | 10 | 同上 |
| coverage | 40 | 同上 |

四view保留完整raw/main/Unknown及重复；仅顶层qualification按完整typed元素排序以处理原合同未承诺的顺序，其他列表不动。全部Receipt在耗尽、关闭、尾检后为complete。

一个有意义的入口/退出锁接合覆盖：合法Admission配错purpose完整Runtime，入口拒绝且未进入锁体、无FOR SHARE；正常Runtime持有原resource.run锁时，第二连接50ms同值UPDATE确实阻塞，随后将最低空闲盘要求设为2**63-1，正常退出资源门禁拒绝。恢复预算后第二连接同值UPDATE成功，证明释放；所有UPDATE最后rollback，不改业务值。退出片恰一个FOR SHARE，无隐藏current/科学扫描/hash，不重跑旧关闭或三锁矩阵。

Resource新Admission `ff7d22b80aa8cdb6d7c84db342893b09b10c7e9de2b592e6eeb6843468dadd48`。登记Resource Admission3→4、M2 Admission1→2、reference Admission1→2；全部旧记录保留，原run/checkpoint/国家参考行完整不变。

## Detection唯一新窗口输出

原 `0774d9ed031d4fb7b25b6ad8a8faff06:7` 保持为无窗口生产证明的旧锚，不补签、不重产。本次仅从自有原 `3884fdb6cdb7408d92f78a85db70a6af:26` 和11参考，冻结生产一个必要新输出：**d8d33fe8f7a2454cbd0c296ad0d55ebf:7**，独立新库 `det_window_8233_3048cb5ed3df`。新库实际仅1个complete run、1个Admission，无重复或失败生产。使用本项目冻结新解释器入口；旧M2与旧D没有生产或更改。

根据原消息时间选择新计算窗 `[1970-01-01T00:01:40Z, 00:01:54Z)`、结果窗 `[00:01:50Z, 00:01:54Z)`，都在原M2包络中。只为新请求明确计算scope和对应boundary观察时间，原请求不改。实际UPDATE来源包含100–113秒；原处理顺序末尾100秒Gap保留，不按时间重排。

复用98d8已实际取得的real M2/reference全部12份Admission，逐原run/snapshot、来源顺序和11参考source_id核对，实际current全通过；无上游新admit。新完整binding含原scope/identity及窗口生产证明，构造2e真实Runtime，实际inspect/admit/current/reuse通过，current/reuse无正文扫描/hash。

| 新输出公开view | 行数 | 对照/语义 |
| --- | ---: | --- |
| records | 131 | 与同一新输出原Reader完整typed逐值一致 |
| state_entries | 178 | 同上 |
| m3_entries | 32 | 同上 |
| result_revisions | 12 | 10个选中事件全部原修订子序列，无截断窗前链 |
| result_coverage | 2 | 完整等于原m3来源Coverage，窗口引用原样保留 |

三表共341行，保存完整原Reader输出、公开typed输出和inventory。五view各有正常Receipt。12条结果中11条main=null，选择类别实际为possible_unknown/started_in_result；不把当前Gap样本冒充clean carry-in全分支复验。4条选中修订的原开始时间早于结果窗，完整原修订序列仍保留。

另将新输出18条source_message完整plain/typed值与实际M2 `ordered(reader)` 的原消息逐一比较，包含原bytes的类型编码；RawTime、解释与Gap完整边界另外保存。原source ranks0/1不重编号。实际14条计算UPDATE来源消息，结果窗4条；first_raw_time为101秒，last_raw_time为100秒，表示原处理顺序首末，不是最小最大时间。一个scope_gap、Unknown Coverage与main=null保留；earlier_history仍Unknown。本片不另造ET或clean来源。

新Admission `80d355c9576186623b701a2364b66be09b3369b6696570d76c00d8cdd62446f0`。对同一新输出完整result_revisions读取后把RSS预算降到1字节，尾检失败、Receipt=None，随后恢复预算；没有使用假Admission绕过。

## 实际测试与辅助代码修正

本项目 `uv run --locked --project backend pytest`，清除PYTHONPATH，显式开启本任务人工集成环境开关。原日志、XML和各测试版本全部保留，不写成一次全绿：

1. 首轮 **1 passed、1 failed，61.42秒**。Resource完整片通过；Detection生产、准入、三表/两结果view与上述原值/Gap/Unknown检查均通过，随后辅助代码调用不存在的reader.ordered()失败。
2. 仅剩余消息边界及尾失败补验，**1 failed、2 deselected，1.58秒**。改为项目ordered(reader)，原18条消息已完整匹配；提取辅助函数后局部变量ub未传入，rank断言前NameError。
3. 从Runtime实际依赖读取ub，仅继续剩余补验，**1 passed、2 deselected，4.43秒**。消息/rank/窗前链与尾资源检查完成。

两次失败都是本任务测试辅助代码问题，没有产品修复。补验复用已保存新输出、Admission和五view完整结果，不重产、不重新admit、不重跑Resource或已完成视图矩阵。最后只读收口核验通过。

## 实际成本

| 阶段 | 墙钟秒 | 实际PG条目 |
| --- | ---: | ---: |
| Resource M2必要real inspect/admit | 1.295903 | 4157 |
| Resource CSV必要real admit | 0.362411 | 512 |
| Resource inspect | 0.028633 | 12 |
| Resource admit | 9.368098 | 3782 |
| Resource current/reuse | 2.727257 | 1168 |
| Resource metrics | 2.801375 | 980 |
| Resource normal_bands | 2.762300 | 980 |
| Resource topology_status | 2.470541 | 980 |
| Resource coverage | 2.453268 | 959 |
| Resource锁入口错范围（含对照UPDATE） | 0.051331 | 4 |
| Resource锁退出资源（含对照UPDATE） | 0.155375 | 19 |
| Detection原real上游current | 1.831582 | 1382 |
| Detection唯一冻结生产 | 1.910897 | 862 |
| Detection inspect | 0.004926 | 3 |
| Detection admit | 4.352032 | 2956 |
| Detection current/reuse | 5.895141 | 4354 |
| Detection records | 4.293533 | 2927 |
| Detection state_entries | 4.298748 | 2927 |
| Detection m3_entries | 4.006366 | 2927 |
| Detection result_revisions | 3.953586 | 2937 |
| Detection result_coverage | 4.011761 | 2927 |
| Detection结果尾资源失败 | 2.339590 | 1486 |

PG为本次自有55483独立日志实际statement/execute，首尾marker核绑定、排除测量连接；包括内部上游调用及冻结子进程。没有其他共享业务并发。原Reader三表对照各另计29条、约0.05秒，未混入公开读取耗时。消息边界补验未做阶段PG测量，Unknown。

Resource正向检查26919次，进程生命周期峰值RSS347602944字节、最低空闲盘117581832192字节。首轮Python进程测得最高RSS353599488字节，不含PG或冻结子进程，不是阶段独占峰值。Detection原Runtime正向resource_usage未在辅助错误前导出；补验重建Runtime的尾检查前状态不冒充原阶段资源采样。冻结执行自身遥测保存在新identity；物理I/O、PG峰值、临时盘峰值与全天吞吐Unknown。

## 保全与停止

原Resource204文件SHA全部相同，两组上游去重123个实体实际SHA相同。全部历史保护清单、98d8片249文件索引及8d2片59文件索引逐SHA不变；清单有重叠，不相加为唯一原件数。原Detection、Feature、Canonical和其上游登记逐对象不变；Resource旧科学/可信记录保留，仅上述必要新增。新Detection另库登记已固定。

各scratch子目录为空。自有55483已smart stop，28763未启动；两者pg_ctl status=3、pg_isready=2，无PID/socket。证据根 `/tmp/domeye-resource-window-integration-8233`，完整交付索引文件数及SHA随固定提交回传。

标准模式，无Fast、无总处理期限，有限资源保护保持。没有真实输入、共享服务、Country、全P、HTTP、Issue、push或部署。两片有限GO完成，停止等待后续授权。
