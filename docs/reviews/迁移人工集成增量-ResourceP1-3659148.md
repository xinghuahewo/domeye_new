# Resource P1 人工集成：原路径兼容前置阻塞

结论：**STOP，本片未通过 Resource P1 集成验收**。原 Resource/Q1 仍可读，原 Resource 全16表、3266行及全部原件保持；新公共准入在上游 M2 原路径检查处拒绝，尚未取得 M2/reference/Resource Admission。未执行的 Resource P1 科学审计、公共四视图、单锁及失败边界不能计为通过。

## 固定代码与授权

- 接受基线：`7ea04838df2ab93e922bb946bc146a007cd9df46`。
- 本次候选：`365914827e0495b353c7212ff4d62eea8220a5b2`，唯一父 `941f040a838d90771fee102702c20e59c4c47826`。
- 实际合并：`04f87cdf396c1ffd72fb3b9edcb6358fc72407e3`，双亲依次为上述接受基线与候选，无冲突。候选新增5文件829行；产品文件保持候选原字节，没有兼容旁路或产品代修。合并没有改变 observations/publication.py。
- 独立报告提交 `bd2a9673b0751a39c104fa5b11584628ded794d9`，报告 SHA256 `dfcb64362eca304e04f37831ac645727a85ef58f0ef6dde3685435f9e225fd51`；作者报告 SHA256 `4a941b0b2bee50eff35a27c83d28881548815318461ceb1384563aaf9a1c7a9a`。两份全文核读且实算相符；其 GO 与48/8项结果不替代本任务原件的集成结果。
- P0有限合同附录全文核读，SHA256 `2001e9497d0672223d391109320f15b3fe04e9ab14a972036b04d58c03763f91`。仅授权人工首片，同一原数据库/原制品；标准模式，无 Fast，无总处理期限。

协调任务收到拒绝后明确要求保留合并、原件和失败证据，结束本有限片。路径兼容由其另交上游 owner 诊断/修复，独立验收后再补未运行项。

## 本任务实际旧绑定

原根 `/private/tmp/domeye-resource-m3-integration-8233`，自有数据库 `resource_m3_8233_292746b97ff0`，Unix socket55483；未访问作者或独立复核者数据库。

| 对象 | 原身份 |
| --- | --- |
| Resource | `c4b0419f11fc4d6ab7f4abb714ae1078:67` |
| dataset | `08a7ab5f91b969522b4589f66cc77a053b9ea1b34675b4947d434d0b5b0246fd` |
| 唯一实际 M2 | `3d86d4c919d149dfaf2ca962e6efccbd:45`，原10个RIB选择 |
| CSV原事实 | source `844a12afcc08426d0e5642855253efb4045dd89278f273d794a596ec399d2c7d`，属于上述M2 |
| 独立国家参考 | `796f3b91dcda4cc0bb9be74da3aee70c`，dataset `f7f59f20fafab41ff380503bc6012f0c15e67e9ac604966f35fe21d50d212673` |

本任务没有先前 Resource P1 登记，未使用其他人的 af170 原件，也未伪造旧 P1 validator 历史。原完整 Reader.inputs 与先前留存的 typed 对象逐值相等；inspect_binding 已成功取得上述实际 M2 原绑定。

## 前置拒绝与实际执行边界

调用链为 `observations.publication.admit → publication_validation.validate → _entity → Runtime.path`。validate 从原 `Selection.plan.inputs` 取原件路径，首个拒绝路径为 `/tmp/domeye-resource-m3-integration-8233/inputs/as.csv`；其 resolve 结果为 `/private/tmp/domeye-resource-m3-integration-8233/inputs/as.csv`。两者指向同一文件，但当前 `Runtime.path` 要求原字符串与规范路径相等，且拒绝符号链接父目录。因此允许根使用规范路径仍不能使该原绑定准入。

这不是 CSV 内容错误或 Resource 科学值差异的证据，而是原路径词法与当前入口约束不兼容。没有改写原 plan/seal/binding/path，没有复制原件替代，没有改 Runtime.path、伪造 Admission 或重产科学数据。失败前已产生75次实体 hash 事件，不将前置拒绝描述成完全未读原件；M2完整正文审计尚未完成，Resource admit 未调用。

实际首次命令：`DOMEYE_RESOURCE_P1_INTEGRATION=8233 env -u PYTHONPATH uv run --locked --project backend pytest -q backend/web/tests/test_resource_p1_integration_8233.py --junitxml=/tmp/domeye-resource-p1-integration-8233/首次.xml`，输出保存在同目录 `首次.log`。

结果为 **1 passed、6 setup errors，4.17秒**：1项是既有 Resource/Q1 兼容；6项共用的模块前置只执行一次，均被同一 M2 path 拒绝阻断，不计为六个不同产品问题，也没有重复运行这一失败矩阵。新增测试文件保留待补验断言；未执行部分包括首次完整科学复算、四个公共视图、新Admission复用/current、三个单锁、早停/尾撤销/实际close错误、有限预算与代表性科学坏件。未实际注入坏件，因此没有实体stat恢复后新准入事件。

另执行独立收口只读脚本核完整原16表及保护清单，没有调用 admit。首次临时启动该辅助脚本遗漏 backend 导入路径而退出，补全本项目路径后的已保存脚本一次完成；没有借此重跑生产或准入。

## 原件与最小兼容核验

原 Resource 全16表 **3266行** 与先前 `新进程全正文.pickle` 的全部 typed 值和重复次数一致。只忽略表查询未承诺的外层行序，所有行内列表保持原序；没有对16表使用 qualification 排序。公共四视图未运行，授权的顶层 qualification 比较例外也未用于放宽原表核验。

行数依次为：sources10、metrics28、memberships108、rendered_paths1、normal_bands113、normal_samples544、topology_edges9、topology_status10、decoding_differences0、decision_refs18、input_receipts10、peer_dependencies10、observation_quality0、coverage40、qualifications459、qualification_dependencies1806。

原目录204个文件SHA全部一致，包括原M2、CSV/国家参考、Resource、执行回执及旧证据。原 resource_runs、resource_references、M2 runs/checkpoints 全登记逐值相等；新 M2/reference 登记表由 admit 初始化为空，accepted均为0；Resource Admission 表未建立。没有删空表或原登记。旧 Resource/Q1 检查包含原十表行量、原发布 Token 的2个完整页及 head/Resource 登记前后不变。

全部接受锚的保护清单再次逐项一致：Peer原件690、C5S2索引655、Peer索引48、M2P1索引151、H2索引331、DetectionLake索引93、Runtime索引116、FeatureP1索引69、H5索引483、2c8a权威3。清单有重叠，不累计成独立文件数；本片未启动28763重跑H系列业务读取。

## 实测成本与限制

仅自有55483进程临时启用 statement 日志，写入本片新日志。PG数来自经 marker 核实绑定的实际 statement/execute日志条目，测量marker连接排除；不是 Python 调用数。其他模块未并发运行，日志观察包含本阶段调用产生的内部SQL。

| 实际阶段 | 墙钟秒 | 实际PG条目 |
| --- | ---: | ---: |
| 原Reader.inputs完整检查 | 1.793357 | 1660 |
| 原M2 inspect | 0.004852 | 4 |
| 原M2 admit至路径拒绝 | 0.147378 | 126 |
| 旧Resource/Q1兼容 | 0.835422 | 542 |
| 收口原16表全值读取 | 0.114167 | 125 |

各阶段 cost/events/原PG片段分别保留。Resource公共 selected_batch 事件为0只表示未进入公共正文路径，不能据此称原Reader或收口没有读取数据。Python RSS保存进程生命周期高水位，测试进程已观测最高276103168字节，不含PG且不是各阶段独占峰值；PG峰值、物理磁盘读取字节与阶段独占峰值均Unknown。未运行的Resource P1阶段成本Unknown，不填零、不借用作者数据。

## 停止与交付

私有55483已 smart stop；55483和28763均 `pg_ctl status=3`、`pg_isready=2`，无PID/socket。scratch为空，原制品及空登记表保留。未触及真实数据、远端、726、独立A、HTTP、Issue、push、部署、Canonical/D P1、窗口或组合P。

证据根：`/tmp/domeye-resource-p1-integration-8233`；全量索引 `交付索引.json` 包含本片脚本、测试原日志、实际PG片段、前后登记、完整原表typed、原路径定位、保护核验及停止证据。索引/报告SHA随固定提交回传。产品差异及本地测试全文已检查，`git diff --check`通过。本次仅提交合并后的人工尝试及STOP报告，等待上游兼容修复独立验收后补验。
