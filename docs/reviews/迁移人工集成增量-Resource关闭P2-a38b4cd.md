# Resource 实际关闭 P2 集成闭合

结论：**GO，仅本次实际关闭链增量**。调用方主错、早停、耗尽三条真实内层关闭失败路径均正确传播错误、无成功Receipt，正常metrics保持；原P2已闭合。没有重产原件或重跑此前通过的科学/四视图/锁矩阵，不扩展真实profile或组合发布。

## 固定代码与依据

起点 `82142b0ae80cd5fa057a6015b95b63d3be5eb5f0`，完整合入修复 `a38b4cddab5530f16c1abe0a8caec0d79ee90d0c`，实际merge `a4fd730632453d4adf51341bc6cfd0e660f4c072`，双亲依次为上述起点与修复。原M2路径链已在起点中保留，无冲突；候选3文件66行新增，Resource `_close` 对GeneratorExit排除的运行改动仅1行，其余为注释、测试及说明。无产品代修。

独立报告提交 `d7eea752207939654209bd64fa000dbd4150f955`；报告全文核读，SHA256 `c819158c5e5a4d452070a58c47a0674c1c8362bd5e9586a77dc1d54a2792523b`，索引SHA256 `32c97cb54577b9263a95779dd873a8784de13b0dd35ae6b91dfba744d5f4ba4b` 实算相符。独立GO只作本次固定输入，不借用其测试数量。本任务原反例与前两次STOP/REPAIR证据保留不覆盖。

## 原件与必要新准入

直接复用本任务原 Resource `c4b0419f11fc4d6ab7f4abb714ae1078:67`、原 M2 `3d86d4c919d149dfaf2ca962e6efccbd:45`、其原CSV与独立国家参考，数据库和根沿原绑定。原M2/reference Admission在本次代码上current通过，直接复用，没有新增上游准入或更改原/tmp路径。

原Resource Admission `0ee3af80ee42af58b7dd8ed19f64e3eb2a59b95d8474d50b7afc4a5ee35bceb8` 因validator源码变化被拒绝current，执行一次必要的新准入，取得 `7b320cb9109c2ba38a00b77bf8b1015d6fbcb877370defc5a6471d0078aa5efc`，owner_revision为完整 `a4fd730632453d4adf51341bc6cfd0e660f4c072`。原完整owner_binding相同，旧登记未修改或补签。

这一次新准入按接口执行必需的完整审计，不是另跑原科学验证矩阵；后续四个测试均使用同一新Admission，无science_compare或inventory_scan。没有重新解析/生产M2、参考或Resource，没有更改原科学制品。

行数更正：前两份报告正文将16表总量误写为3266；原STOP的停止前核验JSON、原逐表计数及本次实际inventory均为 **3166行**，原值没有少100行或发生变化。本次明确更正文字加总错误，历史报告与原证据仍保留，不回写其固定SHA。当前inventory逻辑字节928538，必要审计Account为4883行/1783635 typed字节（包含重复计费）。

## 本次实际验证

命令：`DOMEYE_RESOURCE_P1_INTEGRATION=8233 DOMEYE_RESOURCE_CLOSE_INTEGRATION=8233 env -u PYTHONPATH uv run --locked --project backend pytest -q backend/web/tests/test_resource_close_integration_8233.py --junitxml=/tmp/domeye-resource-close-integration-8233/首次.xml`。

**首次4 passed、0 skip，8.12秒，无重跑。** 在open_reader首current完成后，仅包装真正正文DuckDB连接与Arrow reader的close，不替换selected_rows或清理控制算法。DuckDB先实际close，随后SELECT 1因连接已关闭被拒绝，再抛出同一注入OSError对象。

| 路径 | 核验结果 |
| --- | --- |
| 首批后调用方RuntimeError | 原主错同一对象；原traceback节点仍在链中；cleanup_errors恰含同一OSError对象 |
| 首批后主动早停 | 同一OSError直接传播，不再由generator.close吞掉 |
| 迭代耗尽 | 同一OSError传播，未生成成功Receipt |
| 正常完整metrics | 28行与本任务上一轮保存值逐完整typed与多重性一致，正常退出后Receipt complete |

三条故障路径分别真实DuckDB关闭1次、三个实际Arrow reader各关闭1次，Receipt均null，scratch为空。正常读取过程中Receipt尚未生成，退出后才complete；仅沿既有授权对顶层qualification列表排序比较，保留全部typed元素与重复，其他列表和科学值不变。没有重跑原四视图、三锁、坏件或旧Q1矩阵。

## 保全与成本

本数据库Resource runs保持3行、国家参考1行、M2 run1行、CP13行，前后全值一致。Resource Admission从2增至3，M2/reference均保持1；所有旧登记按原key完整保留，新准入后的登记与四个测试结束时逐值相同。原204文件SHA全部一致。所有前片保护清单保持，含上次Resource恢复128文件、原STOP38文件及H5等接受锚；清单重叠不相加为唯一文件数。

实际PG数来自本任务55483新日志中的statement/execute条目，marker核绑定并排除测量连接；无其他业务并发。原日志片段与events分别保留。

| 阶段 | 墙钟秒 | 实际PG条目 |
| --- | ---: | ---: |
| 原M2/reference current | 0.282065 | 182 |
| 原Resource规则拒绝 | 0.025218 | 4 |
| 必要Resource新准入 | 3.647463 | 3776 |
| caller真实关闭链 | 0.594163 | 519 |
| early真实关闭链 | 0.597419 | 519 |
| exhausted真实关闭链 | 0.621392 | 519 |
| 正常metrics | 1.195909 | 971 |

Python生命周期峰值RSS最高329105408字节，不含PG、不是阶段独占峰值。PG峰值、阶段独占峰值、磁盘物理读取字节和未观测临时盘峰值均Unknown。没有从小人工输入外推容量；保留单调用预算及资源保护，无总任务期限。

## 收尾

自有55483已smart stop；55483和28763均status3、isready2，无PID/socket，scratch为空。源码及完整增量检查、diff --check通过，原失败现场与全部原件保留。证据根 `/tmp/domeye-resource-close-integration-8233`，全索引 `交付索引.json`；报告与索引SHA随固定提交回传。

标准模式，无Fast；未进入后续Feature/Canonical任务，未扩展真实profile、全天、旧停止试点、D/P/H/726、共享服务、HTTP、Issue、push或部署。本P2闭合后停止，交协调者接受。
