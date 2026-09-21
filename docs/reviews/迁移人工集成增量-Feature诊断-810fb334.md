# Feature 诊断持久化接入人工联合基线

2026-09-13。**人工集成 GO**。本片将已接受诊断持久化接入未来候选；ordinary／IR 非空诊断经过正式冻结保存及固定回读，后续 Resource、Detection、Feature 写入后旧八表和文件保持不变。不是业务发布或真实容量验收。

## 固定版本与完整差异

接受基线 `4eeee93937e853b8affc61e9de012e3a15b3db3d`，见[Replay增量](迁移人工集成增量-Replay-7449688.md)。无冲突 merge `9159e78e563149cb5f87b479790874e3ca597e15` 的精确双亲：

- `4eeee93937e853b8affc61e9de012e3a15b3db3d`。
- `810fb33476239f2cc9d16dca07353572158fc639`。

目标父提交为 `f1cf44df84ea4daac8a3a7778037f486d51c051b`；独立报告 `c720b7e56aed8c7802223a3032588a3bbb3a77f5` 全文已读，Standards GO0、Spec GO0。报告中的阈值矩阵、原七表基线对照及10项测试属于此前独立审查，不计为本次执行。

合并4文件191增／4删，运行文件仅 `backend/data_pipeline/feature_run.py`、`feature_store.py`，另为针对性测试及作者中文说明。四文件与810fb334一致；其余运行代码和scripts与4eeee9无差异，包括shared、Replay、Detection、Resource。新增第八表 `module_diagnostics`、来源回执 diagnostics／diagnostics_state 两列及版本／完成门禁；原七表值保持的基线对照沿用已接受独立报告。本片没有重新导出旧代码跑矩阵。

本任务额外只修改现有联合测试及中文记录／台账。联合测试增加P段一条合法默认路由UPDATE，调用现有 `oversized_ipv4_prefix` skip，未改业务阈值；增加诊断字段／计数断言和第二次正式Feature写入，继续使用原类型多重集合函数、嵌套列表顺序与重复值比较，并补收集共享catalog旧Parquet。未合Q1或Country。

## 本次执行与结果

使用本任务UTF8私有PG，仅Unix socket `/tmp/domeye-integration-detection-8233/socket`、端口55483；fixture新建各自人工数据库。全部MRT／CSV／XLSX／JSON由本次夹具生成。正式三计算均经冻结新进程入口。

```bash
DOMEYE_FEATURE_TEST_DSN='host=/tmp/domeye-integration-detection-8233/socket port=55483 dbname=postgres' \
uv run --locked --project backend pytest -q \
  backend/web/tests/test_migration_integration.py backend/web/tests/test_feature_diagnostics.py \
  -k 'resource_feature or saved_empty or missing_diagnostics' \
  --basetemp=/tmp/domeye-integration-diagnostics-final-8233
```

**最终3 passed、0 skipped、3 deselected，19.46秒**：联合三计算、saved空与旧not_saved区分、缺诊断拒绝完成。首轮联合1 passed／1 deselected（18.30秒）后补强默认路由原因及共享文件断言，再执行上述最终检查；首轮不重复累计。未跑64／256个IPv4大范围矩阵、无关全套或真实处理。

| 检查 | 本次证据 |
| --- | --- |
| 非空诊断 | 共4条：ordinary比较段默认路由skip 1；IR比较段non_ir_announcement 1、默认路由skip 1；IR结果段non_ir_announcement 1 |
| 完整保存 | dataset／observation／reference／projection版本、rule、来源及rank有真实绑定；sequence在每mode／source连续；skip数值／阈值／比较符／单位为NULL，样本数组保持空数组 |
| 来源回执 | 每来源diagnostics等于实际行数；baseline为not_applicable；UPDATE为saved，空源为saved且0；不是把旧版本未保存当作零 |
| 固定历史 | 第一Feature全部8表在后续Detection、Resource和第二Feature完成后按完整类型多重集合相等；只忽略外层未承诺行序，嵌套顺序、重复与类型保留 |
| 跨P/D | 保留snapshot初态、P/D别名去重、同SHA不同URI空源、10份来源回执、comparison/result资格；原有效A/W手工预期与IR禁写断言全部通过 |
| Resource／观察 | Resource默认3／全部12个RIB、decision 6／24及完整10表；P/D完整15张观察历史在后续写入后不变 |
| Detection | 26记录、6判定、97状态项；hijack／moas各1修订，typed PG与湖记录全字段相等；固定snapshot5在追加到7后不变且错版拒绝 |
| 文件 | 已有观察、Resource、Feature、Detection Parquet及execution／ready原字节不变，冻结代码清单SHA核对通过 |
| 门禁／版本 | 缺诊断拒绝完成且无execution；新空saved／旧缺版本not_saved区分；补充人工规格future/v9返回unsupported_version，恢复原规格后全部诊断一致 |

第一Feature八表行数：module_diagnostics 4、windows 25、projection_revisions 16、resource_members 39、state_deltas 30、source_receipts 10、decoding_differences 0、reference_rows 6。Feature两次run使用各自私有catalog，snapshot均80，不能据此视为同一快照；Detection也用独立catalog。Resource和Stage1仍共用既有人工catalog。Feature跨P/D连续、Detection仅D冷启动、Resource按RIB时点，各自初态不混同。

## Git外证据与停机

最终根 `/tmp/domeye-integration-diagnostics-final-8233/`，相邻 `.log` 保留最终pytest输出；`test_resource_feature_share_ob0/three-module-evidence.json` 为完整回执，`diagnostics-audit.json` 保留全部诊断字段及版本检查，`detection-audit.json` 为记录计数和参考SHA。正式请求、stdout／stderr、原输入及输出在该子目录。

附加脚本 `/tmp/domeye-integration-diagnostics-audit-8233.py` 及同名 `.log` 核定登记状态、参考资格、Detection固定快照与四条手工诊断预期；只有版本兼容检查临时改本任务人工Feature规格，finally恢复原声明。首次脚本缺PYTHONPATH未导入模块，不计成功；用 `PYTHONPATH=backend uv run --locked --project backend python /tmp/domeye-integration-diagnostics-audit-8233.py` 完成上述审计。

| 制品 | 本次固定ID | snapshot |
| --- | --- | --- |
| P观察 | `49eac657965c4beaa83147d68de47b78` | 19 |
| D观察/11参考 | `f324086dcb7e43beba8e6bdc8cf77e67` | 38 |
| Resource补充JSON | `0c4d466e863a4c2bad5b81d970084ae0` | 41 |
| 第一Resource | `d010f6c517d946c093559b83a3969360` | 53 |
| 第一Feature | `f59fd5e86cad431ebdc08c9d756e862d` | 80 |
| 后续Resource | `2fcb10a400a243e2a70a1f5ac7d8a2c1` | 65 |
| 后续Feature | `cd86b07c73624ac6b95172a9cc868f35` | 80 |
| Detection | `0cfa9b84635c4151b084c323dfc4e782` | 5 |

私有PG已停止：`/tmp/domeye-integration-diagnostics-pg-stop-8233.log` 明确 `server stopped`。完整运行差异、测试差异和文档差异已审，`git diff --check`通过。没有推送、写Issue、部署、真实D726/P读取或处理、共享服务变更；保持未来候选人工GO，等待下一片授权。
