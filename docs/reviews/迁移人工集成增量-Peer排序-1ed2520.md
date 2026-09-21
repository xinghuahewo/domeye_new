# Peer 引用排序人工集成增量

2026-09-13。**GO，仅限同组 Peer 引用稳定枚举和原人工 canonical 的兼容集成。** 实际 M2 三种查询排列产生相同完整 mapping；原 `c4568efe78c04182900b0999877acd76:51` 在原 DB/root 完成 full audit 和 baseline_mappings 全文对照，未重产。

## 固定链与范围

候选 `1ed25204486ba7fd16d24209e6c5c2a1f4feb2a2`，唯一父 `d2ff934056aa3e0629495fff62ee6df79bfae631`，实核一致。完整差异 3 文件/105 增行：运行仅 `observations/replay_io.py` 的注释及按 `(table_record,index)` 排序两行，测试96行、现有文档7行。完整差异、原规则及调用路径已读；不去重、不改四种状态/唯一端点判定、MAPPING_RULE 或消息/元素顺序。

作者交付报告全文及 SHA `33aaa2e8e7f71e3cd90fcb1051f59bd057ce3069f397721dac69438ba7b6df9b`、独立增量报告全文及 SHA `ce044ec1c8d79d11346078edfdac6191e97e468aa1de59d2fab29d0ccf2d02fd` 均实核。独立 Standards/Spec GO；其结果不计为本次实测。

无冲突 merge `eeb3eb4ca6af2387257a0b9d62b251be989d46ad`，准确双亲 `cec630b700a58e46564c431c4f26c099ffbc8c2d`、`1ed25204486ba7fd16d24209e6c5c2a1f4feb2a2`。本次另增自己的集成测试与本报告；未混入 M2 P1、consumer、C5S2、H2 或其他在途修改。

## 自有一次 M2 定点验证

证据根 `/tmp/domeye-mapping-order-integration-8233`。实际先在原 canonical 的 M2 查询 baseline peers，三组各一引用，不能覆盖同组多引用，因此只新造一次小 MRT→M2，随后三次探针复用同一封印。使用自己的 socket 55483，新库 `mapping_8233_55681070dd`；未连接作者或独立审查者 PG。

新 M2 `a82054756ead46b2819aaea5fd18904c:28`，seal `6ab11be14453315241a07ba8db471dd17911aabdb3bddca560792d230a7fb348`。两份压缩 MRT 共197 bytes，六个原 Peer 位置。同组的三个 refs 为 `[(203.0.113.200,0,0),(192.0.2.1,0,1),(203.0.113.200,2,0)]`，BGP ID 排列特意区别于原位置排序，重复内容的不同位置仍保留。

通过正规 ObservationReader 读取真实固定 M2 metadata，仅在查询返回处将原六行正序、逆序、轮转；Counter 完全相等。**这是主动模拟 SQL 未承诺的行序，没有观察或声称数据库自发交换。** 不改 SQL 原行字段、次数或 M2 表。

三次完整 `(endpoints,baseline_mappings)` typed 编码逐字相等，统一摘要 `19cec33131358f1002df5f4e634261c2957eed9a9623709395b9899ba7ff37b9`。四行分别保持 conflicting_baseline_peers、no_observed_endpoint、ambiguous_local_endpoints、calculation_mapping；唯一端点仍为 `(192.0.2.6,64497,192.0.2.2,12654,0)`。原 MRT、checkpoint 文件共26个 SHA 条目、PG run/checkpoints 和 seal 均不变。

## 原 canonical 直接兼容

直接使用 `/tmp/domeye-canonical-m3b-integration-8233/reader.json` 的原 DB/root：run `c4568efe78c04182900b0999877acd76`、snapshot51、seal `e1874d757df7e68cbbd50fe2ac1994a05f3da267eb358368d567ff15d6d7d4f8`。禁止 ProjectionWriter、checkpoint 生产和 MRT 解析入口。

一次公开 full audit=complete，加一次完整 baseline_mappings scan；三行所有字段与原 `生产完整13表.typed.json` 相等，scan终端回执 complete。原 PG登记、seal、11个 canonical Parquet SHA 不变；未重产、未改签，未重复13 scan。full audit/scan 的内部严格验证各重放一次，完整核验不是只核三行 mapping。

本旧件为三组单引用，因此支持它的原身份兼容。没有自己的旧非规范多 refs complete 样本，不伪造旧件或补造旧摘要。代码仍可能拒绝非规范旧 refs 表示；届时应另行授权兼容重准入或从原合格 M2 创建新 M3 派生身份，不能静默修改旧 complete。此处不推广为所有 canonical 默认重产，更不要求 MRT/M2 重解析。

H1/C.1/原 Feature、Detection、Resource 及原 canonical 仅额外核必要控制文件/原件 SHA，共690个文件条目前后相等；未再全读 H1/C.1/RFD。2c8a 三份权威 SHA 与原指定值一致，详见 `2c8a原SHA.json`。

## 成本与交付

本次项目锁定依赖命令：`env -u PYTHONPATH DOMEYE_MAPPING_INTEGRATION=8233 uv run --locked --project backend pytest backend/web/tests/test_mapping_order_integration.py`。**2 passed，4.35秒，零跳过、零失败。** 不重跑作者/审查者矩阵，也不把三种排列当成三个测试。

| 阶段 | wall 秒 | 实际量 |
| --- | ---: | --- |
| 一次 M2 | 0.701465 | 197压缩 bytes、六个Peer位置 |
| mapping 正序 | 0.063263 | 4行＋唯一endpoint，typed 2069 bytes |
| mapping 逆序 | 0.061324 | 同上，全文相等 |
| mapping 轮转 | 0.061197 | 同上，全文相等 |
| 原 canonical audit＋baseline scan | 2.239628 | 两次内部验证，共128投影行/131344 bytes、36消息/44元素、48参考行 |

metadata 查询使用 fetchall，这不是读批或速度提升测试。mapping 后 Python 生命周期峰 RSS230670336 bytes，原 canonical 回读后284311552 bytes，含前序 M2/测试且不含 PG；阶段独立峰值和 PG RSS 为 Unknown，不补零。保留4GiB RSS和512MiB余盘保护，无总截止时间。没有全量吞吐或性能收益主张。

完整原 typed、metadata排列、M2封印、原canonical回执、计量与 SHA 清单、pytest/JUnit 均保存在上述证据根。本地完整差异及 diff --check 核验。自有55483已smart stop；55483/28763均status3、无PID/socket，证据 `PG停止核验.json`。标准模式、无Fast；没有真实D/H/P、远端、726、HTTP、Issue、push或部署。交协调者后等待下一条精确授权。
