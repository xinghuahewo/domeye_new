# Detection M3 v2 人工集成增量验收

结论：**GO，仅限本次人工集成片**。已具备人工 M2 封存→冻结 Detection M3 v2→新进程完整 raw/coverage/qualified 读取，以及旧 typed/v1/Q2 原制品兼容能力。没有进行真实 D/P/H、HTTP、部署、推送、Issue 修改或其他 M3 合入；本报告不代表真实数据准入或产品验收。

## 基线与差异

- 起点：`de027e31a7e2296953cee0cd545d7b840fa2a5af`。
- 授权链：`52f0a1fe74e7cc1ffbea5a05a83751145d775056` → `34b0f85ea51dd52620c013a2978dc0465342de0e` → `48f11de837f9c969958ad2e9a7d9b27578afc825`，48f11de8 唯一父提交为 34b0f85e。
- 无冲突合并：`fa0f8b37b363ee4f94a2cd6b4f8eab04a0d4068d`，父提交依次为 de027e31 和 48f11de8。
- 已审查合入的 13 个文件完整差异，包括资格目标身份、版本白名单、完成/读取校验、冻结入口及文档。集成仅新增测试和本报告，未修订产品实现。
- 两份独立报告已阅读并核对 SHA：`/Users/botongwu/.codex/outputs/detection-m3-independent-34b0f85e/独立实现审查报告.md` 为 `6ae44cb9b26627d900d822a883fa2d7f27517728436478edd82c08c312c2d5a2`；`/Users/botongwu/.codex/outputs/detection-m3-incremental-48f11de8/增量独立复核报告.md` 为 `103ca902039592f7dbba545beaa86f082d6a45f229db1000e3a3be046427e2b0`。独立审查的 11 项结果没有计入本任务测试数量。

## 自有联合路径

使用 `/tmp/domeye-integration-detection-8233/pg` 的 Unix socket 55483，无 TCP。已有 Feature M2 参考不足 Detection 的 11 份绑定要求，因此新建小型人工 M2；旧 typed/v1 与 Q2 则直接读取原库原制品，没有重新生产替代。

主证据目录：`/tmp/domeye-detection-m3-integration-8233/detection-m30`。`request.json` 保存实际输入/输出数据库绑定，Git 中不保存数据库、请求、正文或运行制品。

- M2：`3884fdb6cdb7408d92f78a85db70a6af:26`。压缩 MRT 357 字节，11 份人工参考文件 15596 字节；参考保存行数 24。基线 4 消息/9 元素；UPDATE 14 消息/13 元素，其中 decoded 13、rejected 1，形成 1 个 Gap。上游 67 个文件 SHA 在正式生产前后不变。
- Detection：`812500d196c5482b8bcf2592e32d5973:7`，complete，`detection-m3/v2`；46 个冻结文件当前 SHA 全部匹配，冻结摘要 `d93503409b7ebd7b97799b9682b4d0d6c9e4456ebd1946838373577411f849bb`。核对的是封存文件清单，不将 module_sources 列表宣称为全部模块导入证明。
- 原表共 341 行：records 131、state_entries 178、m3_entries 32。新 Python 进程完整读取三张表及全部公开资格输出，通过自有 pickle 保留 datetime 等类型，与本进程所有字段、JSON 文本、顺序逐项相等。完整正文另有 JSON 审计副本；比较没有只用计数或摘要。
- 18 条 source_message 与 M2 MessageBatch 的完整公开正文一致；qualified records/state 的 raw 字段逐项等于原表。16 个修订在 Gap 前可用，Gap 后 13 个修订 main 为 None，六维 coverage 全为 unknown；3 个已结束修订没有被 Gap 追溯重新资格化。
- LOCAL A/W 手算测试检查 peer/path 与 origin 集合、MOAS 开始和结束；Gap 前后 export_state 完全一致。空状态 Gap 测试保留原始状态。此处 LOCAL 手算是引擎测试证据，不冒称主冻结样本包含 LOCAL A/W。
- 实际同时修改 PG 与 DuckLake 的同一资格行 typed 目标，保留旧 entry_id/payload，不改变行数或事件集合，读者拒绝。恢复行和原 snapshot 后完整 coverage 相等。未知 store schema `wrong/v999` 被 binding 和 coverage 入口拒绝，再恢复原身份。
- 合法无事件场景复用同一封存 M2 的 baseline 与参考，在独立输出数据库冻结执行：`356ff5a78e414c4fa3a7c5062b92a427:7`。完整读取保留 1 条 source coverage，无 business_revision，qualified revisions 为空。

## 原制品兼容与成本

旧根目录 `/tmp/domeye-m2-repaired-integration-8233/test_resource_feature_share_ob0`：原 Detection `c878aa403ab54988b6cac74c61acefc3:5` 完整读取 records 26、state_entries 97；全部 Q2 records 分页与 raw 逐字段规范编码比较相等，Q2 events 完整响应等于原保存证据。原 token build 为 `3aabb69eca52450685295ad4a17e443f`；所有 head 不变，原 368 个文件 SHA 不变。M3 binding 拒绝旧 typed/v1，旧公开读取继续可用。

Feature M3 仅作必要轻量回读：原 `819846d60bec4af2986e3ca743d49de3:36` 的完整 binding、14 个 windows、36 条 coverage 与保存结果一致，122 个文件不变，耗时 2.975 秒；未重跑原 39 项矩阵。

| 阶段 | 实测成本与口径 |
| --- | --- |
| M2 封存 | 1.998 秒 |
| 正式冻结 Detection 整体 | 2.008 秒，包含冻结/参考/计算/完成 |
| 参考加载 | 0.833 秒；进程启动至参考完成累计 RSS 峰值 199032832 字节，不是独立阶段峰值 |
| seed / compute / finish 前 | 0.164 / 0.047 / 0.246 秒；Detection 完成函数总计 0.292 秒 |
| Detection finish 前累计 RSS | 230244352 字节，不含 PG；当时输出目录 24923 字节，不含 PG/WAL |
| 完成后 Parquet | 6 文件、68418 字节，包含后续负例改写产生的文件，不能等同首次生产输出成本 |
| 新进程完整公开读取 | 0.981 秒，进程累计 RSS 峰值 128679936 字节，不含 PG |

保留资源保护，未设处理总时限。人工测试磁盘余量阈值为 0，并不改变生产默认约束。

## 检查记录与失败保留

使用项目 `uv run --locked --project backend pytest`，清除 PYTHONPATH，以显式环境变量绑定自有 PG。测试文件为 `backend/web/tests/test_detection_m3_integration.py`。6 个不同测试最终均有通过记录，无 skip；不是一次运行 6 passed，也没有把重试重复计数。

1. 首轮：3 passed、2 failed，9.41 秒。失败均为集成脚本：新进程 JSON 输出不能编码 datetime；旧证据 typed_records 是计数 26，却被错误地拿来和行列表比较。未修改产品。日志/XML 为 `/tmp/domeye-detection-m3-integration-8233.log` 和同名 `.xml`。
2. 仅重查两项失败，复用已完成 M2/Detection，不重产：2 passed、3 deselected，4.29 秒。日志/XML 为 `/tmp/domeye-detection-m3-recheck-8233.log` 和 `.xml`。改用类型保真的自有 pickle 完整相等比较，旧数据增加 Q2 全 records 分页与原表完整比较。
3. 无事件补充首轮：1 failed，2.57 秒。同一 DuckLake catalog 已绑定原 DATA_PATH，不能给新输出目录复用；这是测试隔离配置错误，没有使用 OVERRIDE_DATA_PATH 绕过。失败目录、stdout/stderr/request 和 `/tmp/domeye-detection-m3-empty-8233.log`、`.xml` 保留。
4. 无事件补充使用独立人工输出数据库：1 passed、5 deselected，3.43 秒。日志/XML 为 `/tmp/domeye-detection-m3-empty-recheck-8233.log` 和 `.xml`。

正文与类型保真新进程输出、成本、负例恢复、无事件完整 coverage 均在主证据目录。旧兼容证据在 `/tmp/domeye-detection-m3-recheck-8233/test_old_typed_v1_q2_original_0/原typed-v1与Q2.json`；Feature 轻量回读在 `/tmp/domeye-detection-m3-integration-8233/FeatureM3轻量回读.json`。

## 交付边界

`read_binding` 只读 PG 元数据。当前 coverage/qualified 读者核验保存资格和 raw 身份，不重新执行当前 M2/参考准入，也不自动追随上游后来失效；未来 P/Country 消费必须自行落实该责任。新 M3 没有因此接入旧 Q2。保持科学原始态与独立资格边界；观察结果不能推断全国断网、用户影响或原因责任。

三份权威文件（2c8a 的迁移计划、ADR-0002、README）SHA 与给定基线一致，详见 `封存及权威文件核对.json`。未触碰其他任务改动。55483 自有 PG 已 smart stop；28763 自有 PG 确認没有运行。失败与完成制品全部保留，未推送或部署。
