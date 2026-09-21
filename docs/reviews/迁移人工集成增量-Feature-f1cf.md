# Feature 多视图接入人工合流

2026-09-13。**人工合流 GO**：固定 P/D 保存观察支持普通与 IR Feature 连续私有计算，Resource仍独立选取12份RIB。仅为人工数据上的集成结果；真实D、P预热、完整全链、页面及发布未验。

## 固定版本与合并选择

原接受基线 `44e252f7e8de7d81e1caf748b82597c2ba4ed8e6` 保持历史冻结，见[上一片Resource报告](迁移人工集成增量-Resource-d27b.md)。本次merge `a5b08fe7b9cd7f0bb05ce2ddd2b3230a3a5c9e1f` 的实际双亲为：

- `44e252f7e8de7d81e1caf748b82597c2ba4ed8e6`。
- Feature最终冻结 `f1cf44df84ea4daac8a3a7778037f486d51c051b`，包含76a多视图及93d catalog等价移植。

父接受依据为独立报告 `f0993a6e3d6e632372f80f3e6cc61c2d8f8549ff` 末节，两轴GO、原参考载体P2已关闭；该报告历史测试数不计为本次运行。

**无冲突、无产品选择分歧。** 合并相对44e为6文件、520增/52删：`feature_inputs.py`、`feature_run.py`、`feature_store.py`、正式Feature脚本、既有多视图测试、Feature迁入说明。Feature运行文件与f1cf逐字节一致；Resource目录、共享observations目录及Resource正式入口与44e逐字节一致。有限冻结实现及公共schema未修改。后续提交只扩展现有联合测试、中文说明和台账链接。

任务 `01a0971e-47ea-7441-94dd-a9f4b4eb5dfc`，8233工作树，分支 `codex/migration-integration`，标准模式。未复制未提交源码；未合Detection或国家增强，未触D726或启动真实P。

## 联合证据

复用本任务9+3人工RIB场景，两个complete run共用显式catalog；D由正式Stage1 CLI生产。P保存9份RIB及UPDATE/空源，D保存3份RIB、P种子的同来源缓存别名及D UPDATE/空源。Resource只绑定原12个RIB，不消费UPDATE或额外别名，不生成完整观察副本。

Feature将P最后一份原`source_role=snapshot`显式作为唯一`initial_rib`，保留原角色；另绑定其D别名，但只消费一次。P UPDATE把Prefix×VP从起源100（IR）改为101（US），D UPDATE撤回同一对象。D RIB仍含100，用于验证它没有重置Feature。新VP64500在P同批A→W；P/D合法空源gzip内容SHA相同、URI与source_id不同。

同一实际CSV SHA `32da5cd7fff4117d6cde71795f0f3d2460c53aead6c73634a2c2370335b5e8bc` 只由D参考登记承载，P不保存参考副本。Feature ReferenceView和Resource csv_binding均指D固定run/snapshot/SHA；Feature还核对6行回执、规则、载体rrc25与每个SourceView的参考SHA。Resource额外JSON仍由正式入口登记为原件文件/规范湖历史/PG目录，NaN、null及legacy_unknown原文和分型保持。没有另造无关联CSV。

| 检查 | 本次实际结论 |
| --- | --- |
| 连续状态 | D普通ASN101 withdraw=1；IR ASN100 withdraw=1且`ir_asn_write_disabled`。若D RIB重置，普通撤回会错误归到100 |
| 别名与真实身份 | 六项请求绑定归为五个唯一来源；initial保留两个跨run别名；每模式五个回执共10个。同SHA不同URI的两个空源均保留 |
| comparison/result | comparison精确P的两个UPDATE来源，result精确D的两个来源，集合不交；结果空尾无ASN行 |
| typed phase/seen | D `window_end`中US撤回为1，`next_window`清零为0；两模式seen均为64497和64500 |
| Resource回归 | default精确3个D RIB/all12个RIB；decision6/24，global每RIB为2前缀、512地址量、1路径，未知拓扑3/12；仍有明确窗前normal_samples |
| 参考和旧历史 | 后续Resource运行后，第一Resource default/all完整10表逻辑多重集合、Feature完整7表、补充参考全行、两上游elements保持不变 |
| 原文件保全 | 已有观察/Resource/Feature/参考Parquet及ready回执原字节保持；JSON原件字节一致 |
| 实际代码与归属 | Resource模块来源和源码SHA匹配合并工作树；原单run联合回归同时核对Feature显式清单全部SHA，包含新增feature_inputs；Resource/参考写共享catalog各自schema，Feature仍为私有catalog |

Feature计算窗口为`[2026-02-27T15:50Z,16:10Z)`，comparison为前10分钟、result为后10分钟；初始snapshot为08:00Z。Resource结果窗保持该业务日全天范围，人工仅3个RIB采样，不与Feature十分钟结果冒充同一粒度或覆盖。源质量、历史参考效力、旧持续状态仍Unknown。

## 行顺序与验证范围

协调提示另一个消费者复用了44e测试并遇Resource外层返回顺序差异。本次直接核对`scan_resource`为无ORDER BY的固定快照查询，未承诺外层行序，因此测试改用带类型标签的完整行多重集合：保留重复次数、原字符串、路径、角色和嵌套列表顺序，仅忽略外层行排列。不同新run的membership_ref仍先核其自身run/source/dimension/bucket，再转换已验证的run前缀比较；旧run不转换任何字段。原始Parquet/ready字节检查独立保留。没有为测试调整Resource查询或业务。

初次多视图联合2 passed、16.91秒；完善上述外层行序断言后，最终源码完整重跑为 **2 passed，0 skipped，16.38秒**。两个用例分别为原单run两次共享catalog/正式双Feature回归，以及扩展P/D多视图与Resource联合；初次通过不累加为4项，不重跑78/173全集。

命令：

```bash
DOMEYE_FEATURE_TEST_DSN='host=/tmp/domeye-integration-views-8233/socket port=55483 dbname=postgres' \
uv run --locked --project backend pytest -q backend/web/tests/test_migration_integration.py \
  --basetemp=/tmp/domeye-integration-views-8233/final
```

本任务新UTF8 PostgreSQL 14仅监听该Unix socket，Git外根`/tmp/domeye-integration-views-8233`。最终日志`final.log`；`final/test_resource_feature_share_ob0/three-module-evidence.json`包含完整六份回执，目录保留Feature多视图请求及全部正式CLI stdout/stderr/原件。PG已经停止。完整本片差异、合并范围及`git diff --check`已检查。

| 制品 | 固定run或reference | snapshot |
| --- | --- | --- |
| P观察 | `edaf0a8fb75c49078115331ba819ba0c` | 19 |
| D观察/CSV载体 | `990b8464e4634288b0e7999115431ec4` | 38 |
| JSON参考 | `fd357cdb4d5146ddbbb62f58a87b71cd` | 41 |
| 第一Resource | `d0b9a03734484c928b7897c5d0557a4e` | 53 |
| Feature | `28a3be2698cc46c69166938e0f7ea2b8` | 71，私有catalog |
| 后续Resource | `45f4eacca4a443e6b5a4f31f6a6cd46f` | 65 |

snapshot不能跨catalog排序比较。本次正向全部rrc25，没有放宽Stage1 schema来生成rrc00。不同collector参考载体仍仅有已接受独立报告中的消费者人工合同证据，不声称正式生产能力。

## 下一片

Detection c133已由父通知独立GO，但尚未合入，等待下一明确增量；国家增强也不合入。本片不包含真实P/D、前端、公开查询排序、恢复或发布。[原完整迁移计划](../architecture/新架构一日全链路迁移计划.md)继续有效，不改写为三模块已经完成全目标。
