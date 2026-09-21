# Detection 公共 typed Reader 人工联合增量

2026-09-13。**人工集成 GO。** 新公共原行流在现有Resource／Feature诊断／Detection／Country C1同源人工链中通过；两表原列、JSON文本、sequence／ordinal及旧语义不变。仅固定读取增量，不是生产或真实D验收。

## 固定父链及差异

接受基线 `d9fb774fcee3bcdd1766ecc677f32bd040a72e5a`，见[Country C1联合记录](迁移人工集成增量-CountryC1-481cd7f.md)。无冲突merge `3ab1a7acc2b5eda99b84f230c053247113c80c85` 的精确双亲：

- `d9fb774fcee3bcdd1766ecc677f32bd040a72e5a`。
- `0f2d3f6120f72195a41d8ea866717241d0466a01`。

目标0f2父为 `c133b7948f2296adc30b8b16625306e7c9b05750`。全文读取独立报告 `cec1775b8ccc17167726f57bc2dc515b9c8f0fce`，两轴GO0；其中7项实际冻结链、9MiB人工矩阵与其他子审检查均未计入本次。

合并5文件238增／19删：运行仅 `backend/data_pipeline/detection/store.py`，另为2测试及README／迁入说明。store.py与0f2字节一致；Country全目录、shared、Replay、Resource、Feature和scripts与d9fb无差异。未合C2或Q1。README／迁入说明随固定提交保留作者“待复核”时点文字，当前接受和实测范围以本记录及上述独立报告为准。

本任务仅增强现有联合测试，外加本文及台账：公共 `read_stored_rows` 两表对照固定湖直接SELECT、旧 `_read_table` 和PG；增加首行交付后scope漂移三路径拒绝；原前后多重集合／嵌套序／文件保全不放宽。

## 本次实测

自有UTF8 PG、Unix socket `/tmp/domeye-integration-detection-8233/socket` 端口55483，无TCP；重新生成既有人工MRT与11参考，正式冻结三计算后消费C1。无真实D/P或其他任务数据。

```bash
DOMEYE_FEATURE_TEST_DSN='host=/tmp/domeye-integration-detection-8233/socket port=55483 dbname=postgres' \
uv run --locked --project backend pytest -q \
  backend/web/tests/test_migration_integration.py backend/web/tests/test_detection_stored_rows.py \
  -k 'resource_feature or old_reader_does_not_inherit' \
  --basetemp=/tmp/domeye-integration-typed-8233
```

**2 passed、0 skipped、6 deselected，22.28秒，一次执行。** 一项为实际同源联合，一项为旧Reader不继承新默认行预算的mock合同；不将mock算作PG通过。未重跑9MiB矩阵、作者独立全套或无关测试。

| 核对 | 结果 |
| --- | --- |
| 全字段typed | records 26、state_entries 97；公共流batch_rows=3与固定湖直接SELECT原列／原JSON文本／sequence或ordinal顺序逐行相等，同时等于旧内部typed出口 |
| PG／旧语义 | 两表PG JSONB按解码语义逐字段一致；旧read_records等于原JSON解释，read_revisions／read_decisions成员sequence一致；完整状态重建前后相等 |
| 正常耗尽 | 公共两表完整耗尽并复验末尾资格；后续catalog变更及C1读取后全表原类型多重集合仍相等 |
| 错误收尾 | 公共records、公共state_entries、旧read_records首行后分别修改自身PG scope，耗尽均拒绝漂移；finally恢复、close，恢复后两表完整重读一致 |
| 旧接口兼容 | mock确认旧Reader内部max_row_bytes为None，新公共入口默认8MiB；本片不以小行证明9MiB实测，实际大行证据属于独立报告 |
| C1保持 | 同一D／Detection／11参考共21行；整批15表及6源回执核验，C1所选baseline＋2UPDATE为5消息／4元素，26 Detection记录完整枚举；input_validated／complete_empty，国家修订0 |
| 历史及文件 | P/D完整15表、Resource两run default/all10表、首Feature8表、Detection两typed表和重建、旧Parquet／execution／ready在后续写入与C1后保持不变 |

Detection仍6条判定、hijack／moas各1修订；固定snapshot5与后续7不混同。Feature仍4诊断和10来源回执，跨P/D源资格与原A/W预期通过。C1仍需C2边界评估，不将输入complete_empty视为国家计算结果。

## 身份与证据

| 制品 | 本次固定ID | snapshot |
| --- | --- | --- |
| P观察 | `d1a5714fd80b4a5886ed894dcd724cb0` | 19 |
| D观察/11参考 | `f13d8cb786754ed8bbf2b74ef8b1e64e` | 38 |
| 第一Resource | `77c6a2ef1f0342d79609b077eb18b300` | 53 |
| 第一Feature | `4d880167565e4ecd9300aa0a5a2306f1` | 80 |
| 后续Resource | `417fc833ec41454e96dade429f769525` | 65 |
| 后续Feature | `cc762ae9ac3441b0bc053a9aaae220f5` | 80 |
| Detection | `07a5563ec9ef46b8b9bd209db995ecd5` | 5 |

各组件catalog独立关系沿用前片，snapshot不可跨catalog比较。证据根 `/tmp/domeye-integration-typed-8233/`，相邻 `.log` 为pytest日志；`test_resource_feature_share_ob0/` 下 `typed-reader-evidence.json`、`three-module-evidence.json`、`detection-audit.json`、`country-binding.json`、`country-input.json` 以及正式请求／stdout／stderr／输入／输出完整保留。

附加只读 `/tmp/domeye-integration-typed-audit-8233.py` 及同名 `.log` 核定登记版本、参考资格、错snapshot拒绝、C1输出与原生产回执SHA。私有PG已停止，回执 `/tmp/domeye-integration-typed-pg-stop-8233.log` 明确server stopped。

完整差异及 `git diff --check` 通过。收到最后一行不等于完成尾验，必须耗尽且无异常；本次拒绝scope漂移不代表全部上游资格持续传播或跨库原子保证。无真实D/P、远端、共享服务、生产、push或Issue操作，真实D仍726。当前人工候选冻结，等待下一片。
