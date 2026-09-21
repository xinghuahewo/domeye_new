# Country C1 固定输入接入人工联合基线

2026-09-13。**人工集成 GO，仅C1输入适配。** 同一人工D观察、Detection及11类参考完成C1固定读取；当前无国家事件，完整枚举后返回 `input_validated / complete_empty`。本片不声称国家计算结果，不包括C2或真实处理。

## 固定父链与完整差异

已接受基线 `bc95f0f157a02ab84640773720520e0715f492e7`，见[Feature诊断集成记录](迁移人工集成增量-Feature诊断-810fb334.md)。无冲突merge `6a198c5b1e47afb5cca9c6a8a17d16a0ce919b20` 的精确双亲：

- `bc95f0f157a02ab84640773720520e0715f492e7`。
- `481cd7ffb8a75c54c33d6cb7c0031fb812a62ed0`。

481父为 `83382b29453de8990d7ce42a0ce3a9a213855515`；833父为 `0cbc02795c2d2728cea5e8be74aef880b87f0f70`。0cbc是显式合并，双亲 `32637e87c6110391dbea353dd5e3a950492946a6` 与已接受Detection `c133b7948f2296adc30b8b16625306e7c9b05750`；32637父为已接受纯算法 `06e0c76d40120450621e1bbc5fde58f8b24bb3d1`。本片按真实merge结果与精确父链核对，不用存在多个merge-base的三点diff代替最终合入范围。

全文读取独立报告 `bfa93bd32914a0af5ee57e841968622fdfc09626`：Standards／Spec均GO，原硬性P2、判断性P2与Spec P1/P2关闭。报告的34项与两国非空独立链不计为本次重跑。

合并16文件、2937增／0删：6个 `country_enhancement` 运行文件、5个测试文件、4份国家说明及原迁移计划6行链接增量。country_enhancement全部文件与481逐字节一致；运行目录和scripts相对bc95的变化仅这6个新增文件。shared、Replay、Feature诊断、Resource、Detection保持bc95字节，未合C2、新Detection typed Reader、Q1.1。原迁移计划仅带入接受分支既有链接段，没有改写历史阶段结论。

本任务额外修改现有联合测试，并新增本文及台账链接。未另造一条与现有三计算无关的国家输入链。

## 最小联合检查

本任务UTF8私有PG，仅Unix socket `/tmp/domeye-integration-detection-8233/socket`、端口55483，无TCP。fixture重新生成人工MRT／CSV／XLSX／JSON和数据库，正式入口生产P/D并运行Resource、Feature、Detection。C1调用已接受适配器；没有国家输出持久化入口或C2计算。

```bash
DOMEYE_FEATURE_TEST_DSN='host=/tmp/domeye-integration-detection-8233/socket port=55483 dbname=postgres' \
uv run --locked --project backend pytest -q backend/web/tests/test_migration_integration.py \
  -k resource_feature --basetemp=/tmp/domeye-integration-country-final2-8233
```

**最终1 passed、0 skipped、1 deselected，24.40秒。** 首轮19.53秒通过后补全六份源回执和Detection typed原表断言；第二轮19.70秒通过后再明确把Resource两run的全表回读放到C1之后，最终重跑上述用例。三个执行轮次是同一项递进验证，不累计成3项，也不借用独立91／34项或纯算法全集。

| 联合检查 | 本次实际结果 |
| --- | --- |
| 同一D输入 | D保存3个结果RIB，其中1 baseline＋2 snapshots；另保留P种子别名snapshot，及非空／空两UPDATE，共6份清单来源。C1与Detection仅选同一baseline＋两UPDATE，未删除额外snapshot |
| 原生产回执 | D正式 `execution.json` 生成后立即固定SHA；全部15表实际行数与原actual_rows相等；六份source_id、原SHA、消息数、元素数与全部Reader starts逐项一致 |
| C1完整枚举 | 2个baseline元素、4 changes、0 invalidations；所选来源5消息／4元素；11参考共21行；Detection 26条记录完整核验，国家修订0，唯一InputCompletion为complete_empty |
| 输入边界 | `boundary_capability=requires_C2_boundary_evaluation`，未计算事件前基线、cohort或国家值；本夹具无国家事件，非空两国证据仅沿用独立C1审查 |
| 反例 | 对本联合绑定传错原SHA返回production_receipt_digest_mismatch；复制回执把changes期望从4改5并绑定副本SHA，返回production_table_count_mismatch:changes，无InputCompletion。未篡改原回执／已保存表；这属于错回执反例，不冒充删行损坏测试 |
| 清理与输出 | 正常及计数失败后scratch为空、workdir清理；完整C1流写入人工证据JSON，bytes以明确hex对象保留，不作为业务publication |
| 观察／Resource | P/D全部15历史表、Resource两run的default／all完整10表在C1后按原类型多重集合相等；外层未承诺行序可忽略，重复与嵌套顺序保留 |
| Feature | ordinary／IR4条诊断、10回执及全部8表旧快照在后续Feature／Resource／Detection／C1后不变；跨P/D资格、空源别名与有效A/W原预期不变 |
| Detection | records及state_entries完整typed湖历史保存前后相等；26记录、6判定、97状态项，重建结果一致。仍是独立catalog固定snapshot5，追加到7后不误读该run |
| 文件保全 | 原观察／Resource／Feature／Detection Parquet、execution／ready字节保持；原生产回执SHA最终复核一致 |

C1在整批固定表上建立索引，统计为一次事件源连接／一次Peer源读取、10个事件索引行／4个Peer索引行，事件查询0、Peer查询1；不是“全部SQL仅扫一次”或规模性能结论。只选择baseline＋updates并不把整批参考、额外RIB或15表完整性检查裁掉。

## 本次身份与Git外证据

| 制品 | 本次固定ID | snapshot |
| --- | --- | --- |
| P观察 | `c859ec6a55b74f278b15e57c4440c745` | 19 |
| D观察/11参考 | `f5e9de05227b45599b0d653e6596bdde` | 38 |
| Resource补充JSON | `ba6a456f4a234542a19aa62af5928b18` | 41 |
| 第一Resource | `c74d0431bb4740a9bac6e6c65e5135c1` | 53 |
| 第一Feature | `357bfe0e110e46dba805abd1d65935e0` | 80 |
| 后续Resource | `42e22531f7f6406793930b70e66d7405` | 65 |
| 后续Feature | `24a277613cb24fd09b1584d542f57b85` | 80 |
| Detection | `09adf053f4a54747873595a9ccf39f2a` | 5 |

C1没有新生产run：绑定上表同一D和Detection。原D生产回执SHA为 `f8b6861b240d11df2a72dfaaa4efa5f02f082deea40a7e0e79cd1865a20be3a0`。Feature两个run各用自身catalog，不能把相同snapshot编号解释为同一历史版本。

最终证据根 `/tmp/domeye-integration-country-final2-8233/`；相邻 `.log` 为pytest日志。`test_resource_feature_share_ob0/` 下保留 `three-module-evidence.json`（沿用既有名称，新增country_input）、`country-binding.json`、`country-input.json`、`country-wrong-receipt.json`、`detection-audit.json`，以及正式请求、stdout／stderr、原件和制品。

附加只读脚本 `/tmp/domeye-integration-country-audit-8233.py` 与同名 `.log` 核对PG资格／固定版本、11参考、Detection错快照拒绝、原回执SHA和保存的C1完整完成流。执行命令为 `PYTHONPATH=backend uv run --locked --project backend python /tmp/domeye-integration-country-audit-8233.py`，通过。

私有PG已停止，回执 `/tmp/domeye-integration-country-pg-stop-8233.log` 为 `server stopped`。完整差异与 `git diff --check` 通过。无真实D/P、远端、共享服务、生产、push或Issue变更；真实D仍726，保持当前人工候选冻结，等待下一片授权。
