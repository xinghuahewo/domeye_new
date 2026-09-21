# Resource 接入人工合流基线

2026-09-13。**人工三模块合流 GO**：同一 Stage1 共享 catalog 的人工观察可供已冻结 Feature 与 Resource 分别正式计算，来源、参考、窗口和旧结果保全已核对。真实 D、Detection、国家增强、全链发布和前端仍未验；本结论不替代原完整迁移目标。

## 固定提交与差异

原基线 `efb251d17e3cdea0909ebf80cee471e51f164df3` 保持冻结，原[两层人工报告](迁移人工集成基线-1d00-b4f.md)保留历史身份。

本次 merge：`9fadb59f52e9ec45b989a02f5d336063a6130f6b`，双亲分别为：

- `efb251d17e3cdea0909ebf80cee471e51f164df3`。
- Resource 已接受冻结 `d27b7470c93f4ebc25d7d7f3d3d04c1bf92235bd`，包含核心6ca、v3/a0、1d00移植834b及补充参考修复。

独立接受报告 `9258557a37a97138be653d0d57a5260313781a30` 已读取，以末节 d27b 的 GO 为本次接纳依据，原 a0 REPAIR 仍是历史事实。没有从其他工作树或未提交内容复制源码。

合并无冲突。相对efb，新增Resource八个运行模块、两脚本、五个既有测试及M03说明共16文件；Feature与共享观察源码逐字节未变，继续b4版本。Resource运行目录及正式入口与d27b逐字节一致。合并后只扩展现有联合测试并新增本说明及台账链接，没有更改产品行为、合同、HTTP或前端。

任务仍为 `01a0971e-47ea-7441-94dd-a9f4b4eb5dfc`，工作树8233，分支 `codex/migration-integration`。标准模式，无push、Issue、真实输入、远端或共享服务操作。

## 新联合场景与独立预期

复用已冻结的人工RIB字节构造helper，重新在本任务生成12份RIB，每份两个/24、同一渲染路径`9808 100`、不同MED。预热为9点，结果窗口为后3点，8小时一份；结果窗口固定 `[2026-02-27T16:00:00Z, 2026-02-28T16:00:00Z)`。这些日期仅为人工时点，不代表读取真实D。

两次produce均显式复用`shared-catalog`，第二次走新进程正式observations CLI。结果上游还含3条UPDATE（新VP64500的A→W及既有VP的起源101宣告）与合法空尾源。Resource显式选择12个RIB；Feature继续单run，只选择结果上游第一RIB基线、UPDATE及空尾，不使用尚未接受的Feature多run实现。

同一CSV包含两模块实际列，SHA为`32da5cd7fff4117d6cde71795f0f3d2460c53aead6c73634a2c2370335b5e8bc`，两者绑定同一结果上游run/snapshot和CSV SHA。CSV有重复Peer ASN：Resource首行名称`peer-first`、rank12.5按既有规则得到12；Feature按自己的固定列及国家解释消费。没有另造互不关联的CSV来源。

Resource的额外国家JSON经正式冻结入口登记，包含完整五字段、中文、重复内部键、NaN和null；明确保存原件文件、规范湖快照、PG登记。NaN保存为`nonstandard_constant/token=NaN/value_state=non_finite`，null保存为scalar/null，`未知`保留`legacy_unknown`，不补为零或真实国家。逐行原文偏移切片与独立原字节一致。

顺序为：两个Stage1 → 正式补充参考 → 正式Resource第一run → 正式普通/IR Feature → 正式Resource后续run。每次正式执行均使用新解释器和只读显式源码快照。

| 独立核对项 | 本次实际结果 |
| --- | --- |
| Resource默认/all来源 | 默认精确3个结果RIB，all精确12个RIB；来源集合等于手工绑定集合 |
| Resource元素与关键值 | 默认6条/all24条decision；global每RIB为2个IPv4前缀、512地址量、1条渲染路径；Peer名称/整数rank匹配CSV首行 |
| Resource未知拓扑与预热 | 默认3条/all12条`legacy_unknown`边，端点为9808和100；结果normal_samples包含结果窗前样本，global list_len至少9，不声称历史持续状态等价 |
| 不消费UPDATE | 所有Resource decision/source均无两份UPDATE来源；没有`observations.duckdb`观察暂存副本 |
| Feature普通/IR | 第一窗口A/W分别2/1、1/1；空尾均0/0；两模式seen均保留64497与64500；CSV及上游绑定与Resource一致 |
| 后续追加隔离 | 第一Resource默认/all全部10表不变；新Resource除自身run引用外业务内容相同；Feature全部7表、补充参考全部行、两上游elements不变 |
| 文件保全 | 原观察、补充参考、第一Resource与Feature的已有Parquet及execution原字节在后续写入后不变 |
| 实际存储归属 | Resource两run与补充参考Parquet均在共享catalog各自schema目录；JSON原件独立文件；Feature仍使用自己的私有catalog |
| 实际代码 | Resource回执模块来源全部在其显式依赖清单，文件SHA与合并工作树一致；Feature继续原冻结入口 |

首次联合测试在最后的“新旧Resource完整表相等”断言失败。逐字段核对发现唯一差异为`metrics.membership_ref`包含各自run ID，属于预期隔离身份。测试改为先按新run/source/dimension/bucket独立验证引用，再替换已验证的run前缀比较业务内容；第一旧run仍要求原始全表完全不变。产品源码未修改。首次失败保留在`joint.log`，不计入通过数。

## 本次执行与证据

本任务新建UTF8 PostgreSQL 14私有cluster，仅Unix socket `/tmp/domeye-integration-resource-8233/socket`，端口55483，无TCP。依赖使用本工作树已锁定环境；所有数据库、输入、请求、日志和输出均在Git外。验证结束PG已停止。

最终执行：

```bash
DOMEYE_FEATURE_TEST_DSN='host=/tmp/domeye-integration-resource-8233/socket port=55483 dbname=postgres' \
uv run --locked --project backend pytest -q backend/web/tests/test_migration_integration.py \
  --basetemp=/tmp/domeye-integration-resource-8233/verified
```

**2 passed，0 skipped，15.51秒**：一个原两层联合测试、一个新增三模块联合测试。没有把原12项、作者或独立审查者历史数量计入本轮；没有重复大规模性能/真实数据测试。完整增量及`git diff --check`已检查。

证据根 `/tmp/domeye-integration-resource-8233`；最终原日志`verified.log`。`verified/test_resource_feature_share_ob0/three-module-evidence.json`包含六份完整回执，目录同时保留各CLI请求、stdout/stderr、人工原件及输出。精确身份如下：

| 制品 | run或reference ID | 固定snapshot |
| --- | --- | --- |
| 预热观察 | `8998ee59642e4105a97b7881e6b9dfbb` | 19 |
| 结果观察 | `5a92354423864ce5ae20db72a1296c34` | 38 |
| 补充参考 | `49d0ba55b9f44d078cec2eb33a804714` | 41 |
| 第一Resource | `887732271f9d4a55a1ae4b2f7233d73a` | 53 |
| Feature | `adaf45a709794c3f8644e0884ac3ddbc` | 49（私有catalog，不能与共享编号排序比较） |
| 后续Resource | `952c0bb033334509b6b02f216bdeeb07` | 65 |

这些均为临时人工证据，非接纳生产制品。JSON dataset为`8ec680f0d9fde4b0dc3850f7b1a5fd64602dff3a7faf922e456b12bbbd808c72`；各计算身份、规则、输入版本与实际文件归属详见完整回执，不由目录名推断。

## 剩余边界

本次未重读或修改真实D/code726；未接入未审Feature多run、Detection或国家增强；未做真实历史导入、发布、前端替换或恢复。本增量只建立可继续合流的人工基线，[原完整迁移计划](../architecture/新架构一日全链路迁移计划.md)继续有效。父确认下一固定版本后再增量合入。
