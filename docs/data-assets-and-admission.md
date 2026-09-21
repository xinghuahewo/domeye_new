# Domeye 数据制品台账与用途准入说明

最新状态（2026-09-12）：`37e0ea72…`的55个可用日／4个隔离日边界已获接受，C已提交合入服务器main并部署至28471／28473。功能证据见[第48节](#48-三项治理结果进入同版首页)，交付接受见[第51节](#51-用户确认交付边界与本轮收口)，部署和统一GitHub进度见[第52节](#52-服务器合并部署与github进度同步)。以下逐次现场记录保留，不覆盖本段最新状态。

整理日期：2026-09-10。范围：项目整体的历史数据基础，不限于国家中断或 B/C 功能；不涉及持续采集。

本台账回答“有哪些数据、来自哪一份、已知到什么程度、能支持什么用途”。初稿依据仓库文档、合同、读取代码及历史核对记录；首次现场核对时间为 **2026-09-10 04:21—04:23 UTC（北京时间 12:21—12:23）**。第 1—8 节保留这次治理基线；后续 [Issue #13](https://github.com/xinghuahewo/domeye_new/issues/13) 的 **09:03—09:07 UTC** 有界业务核对及用途判断另记于第 9 节。各现场结论只适用于相应时点的 `/home/bgpdata/domeye-new` 实例，不自动适用于其他任务或工作树。

首次核对读取了脱敏进程绑定、服务与挂载元数据、制品清单，并计算四份 INFO 文件的完整摘要；当时没有查询数据库业务行、解压事件业务文件、重读原始 MRT、重放、恢复或修改服务。第 9 节后续核对增加了限定业务行与合同检查，不改写首次检查的范围。上述阶段完成的是治理基线及指定用途调查。**当前进度：第 11 节记录后续授权和来源确认，第 12 节记录本地 C 首页首个真实数据闭环验收；不是全量资产审计或新底层架构已实现。**

## 1．证据与登记口径

当前代码与合同依据为本工作树提交 `983f0d4ac5ed27c60183ce5a46d617a9c4828b4f`；另参考本任务已有的[术语表](../CONTEXT.md)和[底层观测决策及样本记录](adr/0001-routing-observation-evidence-boundaries.md)。决策中涉及较新提交的对照不改变本台账的代码基线，未迁入其他工作树的实现。

登记对象分开处理：

| 证据状态 | 本文含义 | 不能据此声称 |
|---|---|---|
| 历史实物核对 | 已有记录给出了核对日期、对象和有限结果；本轮未重读 | 文件现在仍存在、服务现在仍绑定、整个目录均已验收 |
| 本次现场核对 | 本文注明时间、实例、路径、摘要或检查方法的实际结果 | 未检查的业务内容正确、未来状态不变、上游可以完整重建 |
| 代码读取入口 | 当前源码存在读取该类数据的路径 | 对应实物存在、实例实际消费、质量检查已运行通过 |
| 合同定义 | 当前仓库约束了数据形状和来源绑定 | 数据已经生成、合同中的 `complete` 或数量常量已经得到实物验证 |
| 配置／保留模块线索 | 找到名称、默认路径或旧模块，未证实当前入口消费 | 已启用生产管线，或允许启动旧模块 |
| 文档设计 | 讨论已接受或仍待确定的设计 | 实现、机器合同、测试或真实数据已完成迁移 |

这些是本文的说明标签，不新增机器枚举。台账按数据族登记；某族没有实物证据时，不虚构文件清单或数据量。

**以下共同字段适用于第 2—6 节每一条登记项，除非该条明确给出更具体证据：**

- **未具体举证的实物位置、当前可读性、运行绑定、实际数据版本及上游生产批次：未知。** 第 2.2、3、5、9 节的现场证据覆盖其明确对象与核对时点；其他默认路径仍只是定位线索。合同版本、实现提交、源发布与当前运行数据身份分别登记，不能互相替代。
- **责任归属：未知。** 数据供应、口径确认、保管与恢复、消费配置分别由谁负责，均未获得确认；不以代码作者、目录名或数据库账号推定责任人。
- **除第 3.1 节已定位的归档外，备份副本未知；保留期、恢复验证、访问与再分发授权仍未知。** 本文不批准迁移、清理、开放数据或扩大访问。
- **已验证内容：仅相应来源所明确支持的范围。** 未给出实物统计时，实际行数、覆盖率、重复率、缺槽和质量结果均为未知。
- **允许用途为条件性边界，不是质量通过章或新增授权。** “可用于查询”均要求已有读取授权、明确绑定且完成该用途所需核对；当前尚未满足的条件不能因入账而视作满足。

### 查询窗口、源快照与证据窗口不能混用

| 范围依据 | 已记录的范围 | 含义 |
|---|---|---|
| [项目数据档](../config/data-profile.json) | `feb-mar-2026`，Schema `1`；`[2026-02-01T00:00:00+08:00, 2026-04-01T00:00:00+08:00)`；快照 `2026-03-31T23:59:59+08:00`；`Asia/Shanghai` | 当前仓库配置的查询范围与快照，不证明数据库具有完整覆盖 |
| 数据库来源发布 | `20260717T124354Z`；源 dump 的快照记录为 `2026-07-17T12:50:03Z`；派生实例状态文件记录 2026-08-29 裁剪与核验 | 是来源和处理记录，不是 BGP 观测时间，也不是运行库目前内容的不可变版本；详见第 3.1 节 |
| RRC25 224-310 合同族 | 原始窗口 `[2026-02-24T00:00:00Z, 2026-03-11T00:00:00Z)`；状态点按相应合同的五分钟槽末定义 | 指定历史制品的合同范围，不是全项目数据范围；槽末状态点可以落在原始半开窗口的右端 |
| 已核对 MRT 样本 | RRC25、Peer IP `77.243.32.3`／AS `31027`；`[2026-02-24T00:00:00Z, 2026-02-24T00:15:00Z)` | 本任务的有限样本，不能外推到整窗或其他 Peer |

旧数据库查询还存在不同的端点处理方式；本文没有验证或统一全部查询的窗口实现，不能仅凭数据档宣布所有消费口径已一致。

## 2．已有实物核对记录

### 2.1 原始 MRT：一份 RIB 与三份 UPDATE

- **来源与位置**：历史核对记录指向 `10.99.8.16:/home/bgpdata/data/ripe/rrc25/2026.02/`。原始源为 RIPE RIS RRC25；下载批次、源站传输完整性与本地保管责任未知。
- **版本**：以下压缩原文件 SHA256。文件名和时间不足以替代这些内容身份；这里没有给它们虚构统一数据集版本。
- **粒度与范围**：MRT 物理记录、RIB Peer 表／路由条目及 UPDATE 内路由元素；样本观察范围见上表。

| 文件 | 压缩原文件 SHA256 |
|---|---|
| `bview.20260224.0000.gz` | `800d224173376766725acb0be3cc64b92d31f1ba0fd48beda857711120944248` |
| `updates.20260224.0000.gz` | `468dcc83ac03fca610cbb51024acae8bac7663f52c91d77e4464972a64b70254` |
| `updates.20260224.0005.gz` | `e82b2e4b8cf0fd9b08198a9c42a3fcacc945d5e3a30f34b5c4ca6491713736a5` |
| `updates.20260224.0010.gz` | `3846bf3aeb965cb83f12fe8759f0ada02507a9e41d6a017926e022e2c2d66386` |

- **已验证**：2026-09-09 的只读核对记录记载四文件完整读取及 gzip 校验；选定邻居的 11,749 条 UPDATE 消息展开为 16,341 个路由元素，计数与 `bgpdump 1.6.2` 交叉核对。指定 UPDATE 的 IP＋AS 在该份 RIB Peer 表中唯一匹配条目 5。原始定位、计数和限制详见[已有样本记录](adr/0001-routing-observation-evidence-boundaries.md)。
- **允许用途**：引用已记录样本事实、追溯指定原始观察、讨论指定 RIB 快照和有界身份匹配；后续重读仍按授权边界执行。
- **限制**：未证明源端无遗漏、RIB／UPDATE 切点对齐、稳定身份算法正确或实际 Session 连续。不得因此批准十五分钟乃至整窗连续 RouteState 重建。该核对没有保存新的解析数据集或状态制品；本轮没有验证这些文件当前可读。

### 2.2 通用事件读模型：历史内容核对与本次元数据复核

- **来源与位置**：远端 `/home/bgpdata/Domeye-Dev/data/agent-real-loop-stage1-grm-1d1e0463`。2026-09-09 的任务 `01a08590-eb66-7312-9cd2-fad85113c98b` 曾核对其中指定事件内容；本次确认该根目录正被所查后端进程配置选用，当前业务文件内容未重读。
- **版本与绑定**：`manifest.json`／`COMPLETE.json` 的文件 SHA256 均为 `1d1e0463af18cf1320ce37f918ed47fdfb4e1a4f78a73f198923db50a4fca5b9`，本次重新计算且确认字节一致。事件引用 `country_outage/2026-02-27 09:12:32/IR/1/r`；incident 为 `incident_go_v1_a1de26f854831330c616a72af21597eb`，publication 为 `country_outage_publication_v1_989f698fb6f6c32579eebe7bb2bc833f`，cohort 为 `country_event_cohort_v1_1e04abfc6430776bef20403fac528698`，revision 为 `1`；本次确认这些身份仍在清单中。
- **粒度与范围**：该事件、RRC25、记录窗口 `2026-02-27T00:10:00Z` 至 `2026-03-11T00:00:00Z`；事件结束未知。包含国家时序、整窗 ASN 汇总和有界路径样本；不是每个 ASN／Prefix 的完整逐时状态。
- **已验证**：历史记录中，resolve、ASN、路径请求绑定同一事件版本；查到 AS48715 汇总。限定查找中未取得匹配的上游固定 cohort／ASN 和 Prefix 状态文件，预期 `research-runs` 路径当时不可读；不是所有副本永久丢失的证明。
- **允许用途**：定位当前读模型来源、引用当时已核对的该事件汇总与有界样本，保留核对日期、版本及限制；未以本次元数据核对重新验收所有事件查询。
- **限制**：汇总不能证明固定成员在两个时点的状态；关系级首末时间不能当作路径样本自身时间。历史读模型可读不等于上游可恢复，也不等于当前页或 Agent 可用。该摘要只核对了指定事件，不覆盖根目录全部事件。

**本次根目录检查的具体范围：**

- Schema 为 `country-outage-general-read-model-store/v1`；数据集 `general_read_model_dataset_v1_63be5d12ef847d74824efe5be9892f8a`，批次 `general_read_model_run_v1_63be5d12ef847d74824efe5be9892f8a`，清单声明实现 `git:d1f4d11fa7467dad2612e3776bbb5573c633064b`。清单窗口为 RRC25 的 `[2026-02-24T00:00:00Z, 2026-03-11T00:00:00Z)`；各事件窗口另行绑定。
- 根内容摘要 `f7c7a8797348dfab3766f7514a54022c4d8d5582610b0d102c7e5889bbb030c7` 核对一致；81 个事件条目的内容摘要均核对一致。算法与[读取器](../backend/services/country_outage_general_read_model.py)一致：只移除对象顶层 `content_sha256`，以 UTF-8、键排序、不转义中文、紧凑 JSON 序列化后计算 SHA256。文件摘要与此内容摘要不是同一个值。
- 81 个条目各引用 overview／series／affected_as／path_downstreams，共 324 个引用、324 个不同文件；逐一确认解析后仍在绑定根目录内、为普通文件且大小匹配清单。**没有读取这些文件正文，也未复算其压缩／正文摘要、业务行数或跨文件一致性。**
- 清单声明的 `state_point_count=13488` 与事件条目求和一致，只是事件状态点总计，不能替代整个原始窗口的 4320 个五分钟槽，也不证明无缺测。清单 `status=complete` 是生产方声明，不是本轮全量质量结论。

已读清单给出的上游身份如下。它们由下游声明，**尚未取得并核对对应上游实物**；不能把“知道要找哪一份”写成“已找回”。

| 上游对象 | 清单绑定的数据集／快照 ID | 清单绑定的文件 SHA256 或内容摘要 |
|---|---|---|
| Event Cohort | `event_cohort_dataset_v1_11c18b460a735c1acfa5f925d09c1bd8` | manifest：`3bebb14181912e645e0e1d25439edda9be2e327e059e715655852d102455fef6` |
| Event Metric | `event_metric_dataset_v1_136ef94a1068d83f25f844c0fc85f756` | manifest：`c745153178e7e8a0ccf8ba4e5ac285aa76b66cc3980bdeacb28b40939b0d23d5` |
| 事件 AS 路径制品 | `event_as_path_dataset_v1_027b658b0a3121f9ec41d33da3a01504` | manifest：`b286c8973ac0af139e1f309c6362c77e951961b3691887c77cbdf4365b64bfa5` |
| 生命周期快照 | `event_lifecycle_snapshot_v1_7a76c506bd8641406c0d87ba2fdd98f4` | 内容：`7a76c506bd8641406c0d87ba2fdd98f4bb4ecd5da29116e0ef8ea87e412b3426` |

## 3．数据库与参考数据：当前源码支持的来源

### 3.1 传统数据库数据族

来源依据：[连接配置](../backend/config/database.py)、[受控启动入口](../scripts/run_backend.py)、[事件服务](../backend/services/events_service.py)、[总览服务](../backend/services/dashboard_service.py)、[国家服务](../backend/services/country_service.py)、[ASN 服务](../backend/services/asn_service.py)、[特征服务](../backend/services/features_service.py)。本次从目标后端进程的非敏感环境值与已有 TCP 连接确认数据库目标，未新建数据库连接或查询业务行。`conn_11` 等是兼容连接名，不证明存在多个独立数据库。下面各族共同继承本节已确认的库来源；各表内容身份、真实覆盖和质量仍分别未知。

| 登记项与来源线索 | 版本、粒度与观测范围 | 已验证内容 | 条件性用途与限制 |
|---|---|---|---|
| 异常事件总表与六类详情：`event_table_YYYYMM`，以及 `hijack`、`sub_hijack`、`prefix_outage`、`as_outage`、`country_outage`、`leak_event` 的对应月表 | 实际数据版本／检测实现／上游批次未知；业务事件及详情记录，不是逐路由元素 RouteEvent；查询按项目窗口和 `SOURCE`，真实 Collector 范围未知 | 首次仅核对服务映射与入口；第 9 节补入 2 月 27 日日窗的真实结构、关联与地址族检查，未完成原始追溯 | 支持限定来源下的历史事件探索；正式首页联动仍待第 9 节条件。旧检测标签不是因果或责任证明；前缀归属偏差、AS／国家比例等限制不因关联通过而解除 |
| 国家中断生命周期记录：`country_outage_incident_v2`、`country_outage_episode_v2`、`country_outage_observation_v2` | 表名中的 v2 是结构线索，实际数据版本未知；incident／episode／observation 粒度；覆盖由实际记录决定 | [国家中断查询](../backend/database/country_outage.py)及[仓储定义](../backend/database/country_outage_v2_repository.py)存在；未验证实表或物化生命周期快照 | 有条件读取既有事件阶段与观测；不能把当前表内容自动当成某历史 cohort 所绑定的生命周期快照 |
| 国家／ASN 特征：`FEATURE_COUNTRY_TABLE` 默认 `feature_country`；`FEATURE_OTHER_TABLE` 默认 `feature_other`，ASN 按该表、国家分表及 `_YYYYMM`／旧表规则解析 | 实际版本、采样完整性和生产公式未知；来源×国家或 ASN×时点的计数／资源特征；查询范围受项目数据档约束，真实覆盖未知 | [国家特征](../backend/database/feature_country.py)和[ASN 特征](../backend/database/feature_asn.py)读取及工作台聚合存在 | 可作绑定口径的历史特征展示；不能替代同一 cohort、Publication、Collector 下的逐 Prefix 状态。查询中零值处理与缺失口径未做全量审计，不能声称已全面满足新治理规则 |

数据库读取模型不反向证明其原始 MRT、解析、映射或状态输入已经归档。**已确认来源发布，不等于已确认当前各表的内容版本、检测器实现、跨表一致性或可恢复性。**

#### 已确认的数据库来源与派生关系

本次绑定链为：后端 → `127.0.0.1:31627/bgp_project` → 容器 `domeye_core_dev_pg` → `/home/bgpdata/Domeye-Core-dev-data/overlay/merged` → 只读底层副本及可写 upper 层。容器镜像实际 ID 与状态记录均为 `sha256:618b48fc5342e34e4127402dfe100138de728aa8d8cd6561fd02c9523f456e33`。

- 来源依据 `/home/bgpdata/Domeye-Core-dev-data/state.json`：release `20260717T124354Z`，目录 `/home/bgpdata/Domeye-Core-artifacts/releases/20260717T124354Z`；底层 PGDATA 为 `/home/bgpdata/Domeye-Core-data/work/resume-20260717T124354Z-attempt3/postgres`。状态文件 SHA256 为 `353f1f42bb8108c1452f096afb4665a90529711128b6ecb55d2a2a236bc98fb1`。
- 实际挂载确认 `overlay/lower-readonly` 是上述 PGDATA 的只读绑定，`overlay/merged` 为可写 Overlay，upper／work 位于同一 overlay 目录。因此当前运行库是**派生运行实例，不是不可变原始快照**；只读绑定也不证明底层原目录在所有访问路径均不可写。
- 状态文件记录 `pruned_at=2026-08-29T15:29:51Z`、`verified_at=2026-08-29T15:31:29Z` 和二至三月开发范围。本轮未重跑裁剪／核验 SQL，也未证明此后 upper 层没有变化；`phase=verified` 仅作为历史记录。
- 源数据库清单记录 `prebuilt_full_dump`，源 dump `source-full.pg12.custom.dump` 的 SHA256 为 `092ef641aeb7a88507a0062082a35cde7d162e69880471837db1321d5a212c96`，开始于 `2026-07-17T12:50:03Z`、结束于 `2026-07-17T13:18:53Z`。本次未读取源 dump；该时点不能替代项目查询快照或 BGP 事件时间。

以下元数据文件均在来源发布目录下，本次重新计算的 SHA256 与派生状态文件记录相符；同时确认底层 `PG_VERSION`／`global/pg_control` 的摘要与状态记录相符。这些检查支持派生来源，不是运行库全内容指纹。

| 文件 | 本次 SHA256 |
|---|---|
| `manifest.json` | `413ad7e9b22c70565688c6051bbb700f8c217d4767d5327d1e9c37a731894f63` |
| `database-manifest.json` | `fa9a291528419d08c96be3559d68923d09164d6340873904ec26fb8feb9a6943` |
| `database-inventory.json` | `fbb49483946187537ca160514c5c11b2b3d9d5ce62f81a7e5131fdd4e4b8418a` |

来源目录中 `database.dump.zst`（2,329,091,937 字节）、`info.tar.zst`（143,202,986 字节）、`database-image.tar.zst`（160,548,283 字节）和 `database-schema.sql`（613,468 字节）均为现存普通文件；前两项大小与相应清单一致。**归档文件未全量复算摘要，未做恢复测试**，只能登记恢复线索，不能标为“备份可用”。清单的 109 个表条目是源发布登记数，不是当前裁剪库的实表数量。

### 3.2 参考映射与静态信息

[配置声明](../backend/config/config.py)将 INFO 文件定位到 `INFO_DIR`；[加载器](../backend/utils/data_loader.py)定义读取字段和去重方式。本次进程绑定为 `/home/bgpdata/Domeye-Core-dev-data/api/info`，不是源码默认目录。四份核心文件的身份见本节下表；来源机构、字段历史有效期及质量不由文件身份推定。

| 登记项与来源 | 版本、粒度与适用范围 | 已验证内容 | 条件性用途与限制 |
|---|---|---|---|
| `as_entity.csv`、`important_as.csv` | 发布与文件摘要已确认，见下表；ASN 实体属性、重要 ASN 标记；实际来源机构、属性快照时点、更新历史及有效历史时段未知 | 文件摘要／大小匹配；加载器分别按 `asn`／`aut-num` 读取并保留首条去重；ASN／特征服务引用相应字典 | 展示已绑定的实体属性与筛选条件；不得把静态国别／组织／排名自动回填到事件历史。静默跳过坏行或保留首条不等于质量验证；联系信息的访问／再分发授权未知 |
| `ip_bgp_entity.csv` | 发布与摘要已确认，历史有效期未知；Prefix 的路由／域名等实体属性 | 文件摘要／大小匹配；加载器按 `prefix` 读取并去重，特征服务引用 | 可作绑定版本的 Prefix 补充信息；不是固定 cohort 成员、历史 Origin 或某时刻路由状态的独立证明 |
| `country.xlsx` | 发布与摘要已确认；国家名称／二字码／经纬度映射；适用时间未知 | 文件摘要／大小匹配；加载器读取国家映射字段 | 仅作名称和地图展示等解释性映射；不能把地理点或登记国别解释为流量位置、数据面覆盖或受影响人口 |
| `website_entity.csv`、`domain_cn.csv` | 当前绑定目录下均不存在；其他副本、版本及有效期未知；按 URL 合并的域名／行业／IP／Prefix 信息 | 存在延迟加载函数；当前精简事件服务明确未纳入域名增强能力 | 仅登记保留读取能力，不准入为当前域名数据来源；不得据此声称域名或真实服务受影响 |
| 冻结映射：`mapping_version`、`mapping_compatible_sha256`、`mapping_revised_sha256` | 来源是状态、指标、cohort 合同要求的内容身份；实物文件名／路径／哈希值未知；映射明细粒度与时效待实物确认 | [状态合同](../contracts/data/rrc25-route-state-store.schema.json)、[cohort 合同](../contracts/data/rrc25-event-cohort-store.schema.json)要求绑定这些字段 | 是核对派生血缘所需输入；不能用同名最新 INFO 文件替代，也不能由合同字段存在推定匹配映射已找到 |
| 冻结生命周期与 AS 属性快照 | lifecycle snapshot 按事件生命周期供 cohort 使用；AS 属性快照按 ASN 供路径分析使用；一个下游绑定的生命周期 ID／内容摘要见第 2.2 节；上游实物位置、AS 属性版本与冻结时点未知 | [cohort 合同](../contracts/data/rrc25-event-cohort-store.schema.json)要求生命周期 ID／内容与文件摘要；[路径分析合同](../contracts/data/rrc25-event-as-path-store.schema.json)要求 AS 属性文件摘要、大小及字段／重复策略 | 只作所绑定版本的事件选择与属性解释；当前 DB／CSV 不能自动替代历史冻结输入。生命周期阶段不是原始 BGP Session 状态 |

#### 已确认的 INFO 发布身份

`/home/bgpdata/Domeye-Core-dev-data/api/info-manifest.json` 位于 INFO 目录的**上一级**；它与第 3.1 节来源发布中的 `info-manifest.json` 字节一致，SHA256 为 `ec52811643ea147667d5daca0adb481c5190bb11e61f93ba337ebf853c501400`，声明 release `20260717T124354Z`、创建时间 `2026-07-17T14:23:50Z`。因此不是“找不到 INFO 版本”，而是“发布及文件身份已知，属性历史有效性未知”。

| 当前绑定文件 | 字节数 | 本次完整 SHA256（全部匹配发布清单） |
|---|---:|---|
| `important_as.csv` | 32373 | `c757af2c4a0fece57e4b9aa13540ce4ceca8ca1338ff55045c3d869899249199` |
| `as_entity.csv` | 375961154 | `9ef7bd4dcf07b53d986be392f57e40652e37f352c5e2631d24649cda41ba7da2` |
| `ip_bgp_entity.csv` | 705573429 | `ca35e4e7e09e796cea580235ff1cf91301396aa282fa7fc7e9112d0bfe50bf5c` |
| `country.xlsx` | 34963 | `79ef1febccf9d7eaf234fc537928f61bf9d99533e521b28c05c39b374706c062` |

检查方法为低优先级逐文件流式读取计算 SHA256，前后文件大小、修改时间和 inode 保持一致；未解析 CSV／工作表行，未检查唯一性、空值、关联覆盖或进程内缓存内容。清单的物理行数不能直接作为唯一实体数。

交叉线索：`/home/bgpdata/Domeye-Info-Migration/20260725T131422Z-s1-v5/evidence/S6/static-info-manifest.json` 的文件 SHA256 为 `37d9e0df2a24d5e2284295a299befe34b4a59aa83f12292aba8c077f3c7f3cee`，声明内容 ID `info_v1_400c1e3f74c43cc37088a49b1ad5655f`、24 个文件；其中上述四份文件的摘要也匹配。这证明同内容被该迁移清单登记，**不证明当前实例使用迁移数据库或全部 24 文件**，也不将七月迁移时间当成属性有效时间。

## 4．解析、状态与分析制品：已有合同，实物不作默认存在判断

本节除第 2.2 节的通用读模型实物及其声明的上游身份外，**未核实的实物位置、数据集 ID、manifest／内容哈希值、生成批次与质量结果仍为未知**。知道下游引用的上游 ID 不代表上游实物已找到。下面合同及字段关系是核对依据，不是生成结果。RRC25 224-310 条目的观测窗口沿用第 1 节，不能推广到其他 Collector 或全项目；P0 通用合同的具体范围须来自对应实例。

| 数据族、来源与合同版本 | 粒度与血缘要求 | 条件性用途与明确限制 |
|---|---|---|
| 原始记录旁索引、RouteEvent 分区与 AS_PATH 字典：[单记录](../contracts/data/route-event.schema.json) `route_event_v1`；[制品库](../contracts/data/rrc25-route-event-store.schema.json) `rrc25-route-event-store/v1` | 原始文件→物理记录→路由元素；绑定原始坐标、文件内容、解析／导入实现和选择清单；AS_PATH 字典是内容寻址辅助制品 | 经追溯核对后读取 announce／withdraw／rib_snapshot 观察；历史占位记录不因符合兼容结构成为原始可追溯 RouteEvent。记录数不等于消息数或状态变化次数 |
| Peer 会话状态变化事实：[合同](../contracts/data/rrc25-peer-session-store.schema.json) `rrc25-peer-session-store/v1` | 冻结 UPDATE 中逐条 BGP4MP STATE_CHANGE、端点及原始位置，绑定 RouteEvent 输入；是会话事实记录，不是新设计的完整 Session 生命周期库 | 有条件引用原始会话状态转换；无记录不能解释为会话未断，会话断开不得伪造成 Prefix WITHDRAW |
| RouteState、Checkpoint 与槽账本：[合同](../contracts/data/rrc25-route-state-store.schema.json) `rrc25-route-state-store/v1` | 旧键为 `collector + VP/peer + prefix + address_family`；绑定 RouteEvent、映射及投影实现；合同含槽 0／2160／4320 的检查点 | 取得匹配输入并核对重建条件后用于指定范围状态与恢复；现有样本未使该族获准连续重建。不得把旧 VP 键改称新稳定 Peer Identity／Session，也不由检查点路径推定可恢复 |
| 统一国家／ASN／Collector 指标：[合同](../contracts/data/rrc25-metric-store.schema.json) `rrc25-route-metric-store/v1` | 由同一 RouteState 转移投影到五分钟国家／Collector 序列及 ASN 变化编码；绑定状态版本、映射、公式、质量槽、水位和数据库装载身份 | 同口径历史指标查询与核对；不把派生指标当第二套路由事实，不把活动密度或缺测解释为观察覆盖或零 |
| Event Cohort 固定集合：[合同](../contracts/data/rrc25-event-cohort-store.schema.json) `rrc25-event-cohort-store/v1` | 每事件在首次检测前最后完整五分钟状态点确定成员；绑定 RouteState、会话事实、生命周期快照及冻结映射；前缀按地址族区分，独立方向按 peer ASN 去重 | 证明“这次研究谁”和固定分母的候选依据；需要成员实物与版本匹配。方向的 ASN 归并是分析口径，不改写底层 Peer 身份；不能从 S4 路径经过某 ASN 推定成员归属 |
| Event Metric 事件指标与 ASN／Prefix 状态：[合同](../contracts/data/rrc25-event-metric-store.schema.json) `rrc25-event-metric-store/v1` | 事件×固定集合×五分钟状态，含 ASN、Prefix、新前缀等记录；绑定 cohort、RouteState、RouteEvent、会话及映射 | 有条件回答固定集合随时间的变化；须核对 baseline／change 或其他具体编码，不从无变化行缺省为零。第 2.2 节历史汇总不是本族状态文件的替代品 |
| 事件 AS 属性、路径关系与路径证据：[合同](../contracts/data/rrc25-event-as-path-store.schema.json) `rrc25-event-as-path-store/v1` | 事件内受影响 ASN、路径下游关系和观察样本；绑定 cohort、事件指标、RouteEvent、AS 属性快照 | 描述已观察路径与关系；AS_PATH 不是实际转发路径，关系并发不是因果。若要比较两个时点，必须另有同 Prefix／地址族／Collector／方向的带时点观察，不能用关系汇总首末时间替代 |
| 事件与 Publication：[清单](../contracts/data/rrc25-event-publication-store.schema.json) `rrc25-event-publication-store/v1`；[快照](../contracts/data/rrc25-event-publication.schema.json)含 `rrc25-observation-publication/v1`、`rrc25-analysis-publication/v1` | 每事件的观测／分析发布，绑定指标文件、数据库指纹、装载回执、实现及旧注册引用；修订、截止点、当前指针各有语义 | 支持指定不可变版本的结果引用；存在当前指针不意味着历史引用可随之变化。合同中 `quality=complete`、`gap=none` 是候选约束，不是实际通过证据 |
| 冻结事件／报告快照与 Prefix×VP Evidence：[读模型清单](../contracts/data/rrc25-read-model-store.schema.json) `rrc25-read-model-store/v1`；[快照定义](../contracts/data/rrc25-read-model-snapshot.schema.json) | 从 Publication 与 RouteState 派生事件读模型、紧凑序列、报告快照／指针和有界证据视图；保留源数据库与制品摘要 | 经绑定核对后供只读页面／API 消费；报告或证据视图不是另一套底层真相；合同描述报告／问答表面不等于本项目已具备相关运行功能 |
| 通用国家中断事件读模型：[合同](../contracts/data/country-outage-general-read-model.schema.json) `country-outage-general-read-model-store/v1` | RRC25 224-310 的事件 overview、series、affected_as、path_downstreams；大集合留在上游，路径关系最多三个样本 | 有界时序、稳定分页和样本查询；不是全量路径或完整逐 Prefix 状态仓库。第 2.2 节区分指定事件的历史内容证据和一份当前读模型的元数据核对，不能覆盖本族其他实例 |

**旧阶段编号不能充当资产身份。** 当前合同中的底层 S1／S2 指 RouteEvent／RouteState；另一事件分析链中的 S1／S2／S3／S4 指 cohort／事件指标／路径派生／事件读模型；还存在 S5 读模型称呼。追查时必须同时带上合同名、数据集身份、来源和窗口，不能只说“找到了 S1”。本台账不要求照搬这些旧流水线。

## 5．消费来源与当前实例绑定

本节区分“源码支持从哪里读”“进程配置选了哪一份”和“本次验证了哪些实际请求”。未请求的业务接口不因绑定成立而视为通过验收；文件绑定也不证明进程内缓存与磁盘内容始终一致。

### 5.1 现场绑定快照

本次根据 systemd 状态定位进程，再仅读取非敏感环境字段、进程工作目录及监听／已建立连接；没有打印完整环境或凭据。源代码工作树 HEAD 与本地相同；远端另有文档和 OpenAPI 未提交修改，未将其迁入本地，也未据此认定已加载代码的完整运行版本。

| 对象 | 本次确认 | 能证明与不能证明 |
|---|---|---|
| 后端与前端 | `domeye-new-backend.service`／`domeye-new-frontend.service` 均 active/running、enabled；后端 PID `4174193`，工作目录 `/home/bgpdata/domeye-new/backend` | 当时进程在线，不是业务数据验收或持续在线保证 |
| HTTP 与访问边界 | 后端 `127.0.0.1:28473`，前端实际 `0.0.0.0:28471`；后端及前端代理的 `/api/v1/healthz` 均 200 | 仅证明所查 HTTP 通路；没有核对防火墙或公网可达性，不批准扩大访问 |
| 数据库 | 进程环境 `DB_HOST=127.0.0.1`、`DB_PORT=31627`、`DB_NAME=bgp_project`；已有 TCP 连接指向该端口 | 实际连接目标成立；源发布和可写派生关系见第 3.1 节，不证明各业务表内容正确 |
| 应用只读与窗口 | `PGOPTIONS=-c default_transaction_read_only=on`；`AUTO_INIT_DB=false`、`LOAD_CORE_DATA_ON_STARTUP=false`；窗口、快照与 `TZ=Asia/Shanghai` 与第 1 节数据档一致 | 受控启动器注入默认只读事务；未查询数据库角色权限或会话设置，不称为权限层绝对禁止写入。连接函数本身不补齐该选项，数据库 Overlay 可写是另一层事实 |
| INFO | `INFO_DIR=/home/bgpdata/Domeye-Core-dev-data/api/info` | 四份磁盘文件身份见第 3.2 节；不是历史属性时效、行级质量或当前内存加载验收 |
| 通用事件读模型 | `DOMEYE_COUNTRY_OUTAGE_GENERAL_READ_MODEL=/home/bgpdata/Domeye-Dev/data/agent-real-loop-stage1-grm-1d1e0463` | 元数据与引用核对见第 2.2 节；不代表其他事件来源、趋势或 ASN 特征共享此版本 |

### 5.2 其他消费选择及其边界

| 消费来源与入口 | 版本、粒度和范围 | 已核对的读取要求／条件性用途 | 限制 |
|---|---|---|---|
| 国家事件注册表：[读取器](../backend/services/country_outage_registry.py)；`DOMEYE_COUNTRY_OUTAGE_REGISTRY` | `country_outage_observation_registry_v1`；事件引用／incident／Publication→交付包映射；本次进程未配置，无默认注册文件 | 注册结构、身份字段及绝对路径读取代码存在；配置后用于解析指定事件版本和交付包位置 | 当前不以 registry 证明发布绑定；注册记录即使存在也不是交付包质量证明，不能用 latest 冒充先前版本 |
| 事件研究交付包：[适配器](../backend/services/event_story_service.py)；`DOMEYE_LEGACY_STORY_REPLAY_DIRECTORY` 或注册表绑定 | 实际数据版本未知；incident／cohort、国家快照、ASN 状态、episodes／waves；本次未配置覆盖值或 registry | 读取 `COMPLETE.json`、`QUALITY.json`、`asn-states.jsonl.gz`、`cohort.json`、`country-snapshots.jsonl.gz`、`episodes.json`、`incident.json`、`input-summary.json`、`waves.json`；用于绑定版本的事件叙事投影 | 默认 `/home/bgpdata/Domeye-Core-dev-data/research-runs/iran-rrc25-full-p0/20260723T094940Z-full-p0/state-replay-1805-2300-go-v1` 本次确认不存在；该路径不能准入，不等于所有副本丢失，也不授权重启旧生产引擎 |
| 通用国家事件读模型：[读取器](../backend/services/country_outage_general_read_model.py)；`DOMEYE_COUNTRY_OUTAGE_GENERAL_READ_MODEL` | 第 4 节通用读模型合同版本；事件×Publication×cohort；RRC25 224-310；绑定与具体版本见第 2.2、5.1 节 | 读取 manifest／COMPLETE，并按文件元数据定位 overview、series、affected_as、path_downstreams，检查身份和摘要；用于有界只读查询 | 当前只完成所列元数据检查；下游可读不等于上游可重建，也不证明所有业务文件或页面通过验收 |
| 生产选择清单及其选定读模型：[读取器](../backend/services/data_layer_224_310_runtime.py)；`DOMEYE_DATA_LAYER_224_310_SELECTION` | selection 为 `domeye_data_layer_production_selection_v1`，读模型为 `rrc25-read-model-store/v1`；RRC25 224-310 | 默认定位 `data-layer/PRODUCTION-SELECTION.json`，再绑定 release 内读模型、manifest、production index、紧凑序列与事件证据；源码含目录及文件摘要核对 | 本次进程未配置，远端 `/home/bgpdata/domeye-new/data-layer/PRODUCTION-SELECTION.json` 不存在；不准入为当前选定来源，也不能用旧库冒充其 Publication |
| P0 候选／发布目录及激活指针：[准入服务](../backend/services/p0_data_service.py)；`P0_DATA_RELEASE_DIR`、`P0_DATA_PRODUCTION_ACTIVE` | MetricSeries `metric-series/v1`、质量报告 `data-quality-report/v1`；主体×Collector 范围×五分钟序列；实际窗口、公式版本和发布 ID 未知 | 当前读取布局为 `d2`、`d3`、`metric`、`quality`，可有受闭包约束的旧 `d4`；要求组件 `SHA256SUMS`、指标与质量证据闭合 | 本次两项均未配置；`/api/v1/p0/status` 返回 503、`candidate_repository_unavailable`，消息指明未配置发布目录。当前不可用，不是“指标为零”；不自动配置、生产或激活 |
| 同期参照与确定性趋势结果：[编译器](../backend/services/country_outage_trend_product.py)；`DOMEYE_RRC25_CONTEMPORANEOUS_REFERENCE` | 参照为 `country_outage_contemporaneous_reference_v1`；事件快照上的同期参照、TrendProfile、TrendContext、Evidence Graph；本次未配置外部同期参照 | 根据绑定的事件资源和可选参照编译确定性描述；四份结构见下方合同索引 | 不提供已验证的外部参照，不由此断言所有趋势必然不可用；当前趋势业务响应未检查。参照须匹配快照、窗口、映射及比较口径；相关不是因果，编译输出也不自动算落盘资产 |

趋势合同索引：[同期参照](../contracts/agent/country-outage-contemporaneous-reference-v1.schema.json) `country_outage_contemporaneous_reference_v1`、[TrendProfile](../contracts/agent/country-outage-trend-profile-v1.schema.json) `country_outage_trend_profile_v1`、[TrendContext](../contracts/agent/country-outage-trend-context-v1.schema.json) `country_outage_trend_context_v1`、[Evidence Graph](../contracts/agent/country-outage-evidence-graph-v1.schema.json) `country_outage_evidence_graph_v1`。这些合同及算法版本与实际事件 Publication 版本必须分开引用。

### 5.3 同一页面不等于同一数据版本

按[当前 v2 路由](../backend/web/api/v2/country_outages.py)核对：resolve／overview／series／asns／audit 优先通用读模型，再尝试 224–310 数据层，之后进入 registry／旧事实路径；已选来源校验失败与未配置分开处理。路径关联分页只接通用读模型；趋势先读 224–310，再调用兼容趋势服务，**不经过通用读模型分支**；224–310 的 ASN 分页还存在 `empty_asn_page` 路径，不能据路由存在推定有完整 ASN 矩阵。

总览、六类事件、国家／ASN 档案和传统特征走第 3.1 节数据库，实体补充走 INFO；即使 ASN 特征查询与事件窗口相同，也不证明它绑定了该事件的 cohort、Publication 或固定 Prefix。跨这些入口作比较前，必须单独对齐版本、分母、来源和时间，当前没有项目全局统一 Publication 的证据。

## 6．质量证据、保留线索与非业务资产

| 登记项与来源 | 版本、粒度与范围 | 已验证内容与允许用途 | 限制 |
|---|---|---|---|
| P0 清单、对账与质量证据：[指标合同](../contracts/data/metric-series.schema.json)、[质量合同](../contracts/data/data-quality-report.schema.json)、[质量语义校验](../backend/data_pipeline/common/quality/gate.py) | `metric-series/v1`／`data-quality-report/v1`；每候选、组件、检查及失败项；实际报告版本和覆盖未知 | 源码要求清单、输入闭包、原始验证／RouteEvent／指标对账、复现摘要、执行上下文、数据档及失败明细等；可用于核对来源闭合和相应质量门 | 校验代码存在不等于报告存在或已通过；P0 的旧兼容／原始追溯准入不自动验证新 Peer Identity 或连续 Session |
| 迁移清单、装载／验收回执及回滚绑定：[迁移合同](../contracts/data/rrc25-shadow-migration-manifest.schema.json)、[验收合同](../contracts/data/rrc25-shadow-migration-acceptance.schema.json) | `rrc25-shadow-migration/v1`／`rrc25-data-layer-end-to-end-acceptance/v1`；指定候选、数据库指纹及前后验证；RRC25 224-310 | 合同定义可供将来核对迁移血缘与回滚依据；实际回执、备份位置、保留完成情况未知 | 固定表数、字段数和测试期望不是实际迁移成功或备份可恢复的证据；不据此执行迁移或清理 |
| 未证实当前入口消费的旧数据声明 | 数据版本与有效范围未知；粒度仅能按名称／模块推测，未确认 | [配置](../backend/config/config.py)保留 `as_dict.txt`、`top_nx.csv`、`top_ip.txt`、`ipv4_all_prefix.xls`、`ipv6_all_prefix.xls`、`pfx2as_dict.txt`、`as_rel_dict.txt`、`private_as_dict_new.json`、`triplet_20days.csv`、`as_rank.json`、`org_entity.csv`、`domain_cn_center.txt`；保留 raw 路径／`RIB_HISTORY_FILE` 与 `data/detection`、`data/feature`、`data/prefix_count` 默认输出路径 | 仅供有界定位，不算已生成资产；未证实被当前入口消费不等于无价值或可删除，也不启用检测／采集 |
| 保留数据库模块与资源表线索 | 实际版本、实表粒度及范围未知；模块描述包括 VP 资源、拓扑、前缀计数、AS 信息、MOAS、泄漏现象和登录数据 | 可在 [database 目录](../backend/database)及配置找到 `bgp_vp_resource`、`bgp_routing_resource`、`bgp_boundary`、`bgp_connection`、`country_topology_edge`、`country_topology_snapshot`、`prefix_count_YYYYMM` 等线索 | 本轮未证实当前页面入口消费上述保留族，不将模块存在当成启用证明；登录数据不作 BGP 研究数据，也不读取内容或凭据 |
| 合同样本与测试数据：[fixtures](../contracts/data/fixtures)、[API 测试](../backend/tests) | 仓库版本受上述提交约束；正反例、mock 或临时测试制品，范围以各案例为准 | 可以用于结构／语义回归和说明边界；本轮只确认相关文件与定义，未运行测试 | 不能作为真实覆盖、实际缺失率、生产质量通过或性能验收证据 |
| 运行配置、凭据、日志与缓存 | 独立后端配置定位 `/home/bgpdata/domeye-new-runtime/backend.env`；进程脱敏绑定见第 5.1 节；其他内容版本、日志／缓存保留状态未知 | [README](../README.md)及[启动器](../scripts/run_backend.py)说明配置留在 Git 外；本次仅核对目标进程非敏感绑定，不记录凭据或完整环境 | 配置不是数据发布清单，日志不是唯一业务事实源，缓存不是可恢复权威副本；不批准公开、搬迁或清理 |

## 7．仅计划或尚未验证的底层设计

以下依据[已有决策](adr/0001-routing-observation-evidence-boundaries.md)，单独列出，不计为现有实物资产。

| 设计项 | 当前进度 | 未具备的证据／制品 |
|---|---|---|
| Collector 内稳定 Peer Identity 与可追溯关联 | 指定 RIB Peer 条目首次建号、指定同记录时点 UPDATE 唯一匹配的文档正例已接受；符号 `P` 只是示意 | 未实际建号，未执行关联或验证算法；无已核实的身份登记库、版本化关联制品或全量有效区间 |
| 实际 Session 识别及观测片段关联 | 已接受区分实际会话与证据覆盖、边界可未知 | 识别／冲突规则和实现未定；旧 STATE_CHANGE 制品合同不等于完整 Session 关联已经完成 |
| 新边界下有限 RouteState 与版本化重建 | 已接受存在／不存在／未知、最后已知状态、输入及解释版本分离等原则 | 连续重建准入、RIB／UPDATE 切点、未知 Session 下有限状态规则及机器合同修订未完成；不能把旧状态合同直接标为新设计已落地 |

上述设计本身不是实施授权。本轮不决定存储选型、字段扩展、完整回放或 API 改造。

## 8．用途准入与下一步

### 当前可以作出的判断

1. **观察取证**：第 2.1 节有限原始样本有历史核对依据；只支持所列观察、定位与快照事实，不提升为连续状态能力。
2. **身份、Session、状态分别判断**：文件可读、Peer 原始字段匹配、会话连续、状态可重建是不同问题，不互相替代；旧解析／状态合同不能自动升级为新身份语义。
3. **派生用途依赖匹配输入**：状态、cohort、指标、路径与 Publication 的版本、来源、分母和窗口须匹配。数据缺口、身份冲突、读取失败保留未知／不可用；最后已知路径保持其历史时点，不能默认为当前路径。
4. **汇总不越级**：第 2.2 节允许引用的历史事件汇总，不能代替固定成员证明、逐时状态或两时点路径证据；同理，传统数据库特征不能替代事件版本绑定的状态制品。
5. **消费不反向生产**：页面／只读 API 只消费明确绑定数据；启动和请求不得初始化库、重放、检测或发布。结构校验通过、文件完整、质量门通过和当前功能可用仍需分别举证。
6. **观察不外推因果／影响**：BGP 观测不能直接证明全国实际断网、用户影响、实际转发路径、原因或责任。外部参考数据的来源、有效期及使用许可未经核实，不作为更强结论的补丁。

### 关键用途的当前准入结论

下表是治理判断，不是已部署的统一拦截器。“待核对”不代表业务一定失败，而是本轮证据不足以验收该用途。

| 用途 | 当前可依赖的证据 | 准入边界／暂不准入的原因 |
|---|---|---|
| 原始路由取证与底层身份讨论 | 第 2.1 节历史 MRT 样本；第 7 节文档决策 | 限定样本的原始定位和观察可引用；稳定身份算法、实际 Session 连续性和连续 RouteState 重建未获验证，不准入为已实现能力 |
| 总览、事件列表／详情、国家与 ASN 特征 | 数据库实际目标、源发布和派生挂载已核实；代码入口见第 3.1 节，后续限定事件样本结果见第 9 节 | 允许按现有读取授权做绑定来源的历史探索；单日事件关联核对不验收整体特征，不准入为完整覆盖、统一分母、权威比例或可复现检测结论 |
| 实体名称、国家地图与 Prefix 补充信息 | 四份 INFO 磁盘文件内容身份已确认 | 仅作标明文件版本的参考信息；键质量、关联覆盖和属性历史有效性待核对，不能据此倒填历史 cohort／Origin 或实际影响 |
| 通用国家事件汇总、国家时序和有界路径样本 | 第 2.2 节当前清单与引用检查，加上指定事件的历史内容核对 | 来源可定位；引用历史内容须带当时证据范围。本轮未验收所有事件业务文件／接口；固定 Prefix 逐时变化、两时点路径演化及上游复现暂不准入 |
| 224–310 选定数据层、旧交付包和 registry | 选择配置未设、默认选择文件／交付包路径缺失、registry 未配 | 不准入为当前实例的数据来源；仅记录其他副本仍可能存在，不自动改绑或恢复 |
| 确定性趋势与跨页面比较 | 第 5.3 节不同读取分支及可选参照规则 | 本轮未验收趋势响应；不能从通用读模型存在推出趋势可用，也不能将数据库 ASN 特征拼成同一 cohort／Publication 的状态结果 |
| P0 指标与质量页 | 状态接口实际返回未配置导致的 503 | 当前不可用；不能以旧数据库、零或成功响应代替缺失发布 |
| 研究复现、备份恢复、对外引用与分享 | 已定位若干来源清单和归档；未验证恢复、完整生产身份或分享许可 | 可用于后续有界核对和既有授权内取证，不准入为全链复现／可恢复承诺；不批准数据再分发、清理或更强因果／影响声明 |

### 剩余缺口、优先级与最小下一步

优先级按“何时会影响数据使用”排序，不是宣布已经发生质量事故。未知项缺乏证据，不虚构失败率、业务损失或责任人。

| 优先级与待补项 | 已知风险与影响用途 | 最小下一步与完成证据 | 当前处置 |
|---|---|---|---|
| 高：业务内容与质量口径 | 数据库来源确认不证明实表覆盖；INFO 文件一致不证明键唯一或历史时效；事件文件存在不证明内容一致 | 正式验收某项用途前，限定一个对象／窗口：核对所需表结构、数据内容身份、时间覆盖与缺测／零区别；INFO 核对所用键与关联覆盖；事件核对所需正文摘要、行数和跨文件身份。记录范围、查询／方法和结果，不做全量默认扫描 | 当前只接受上表有限用途；超出现有读取范围先确认授权，不以更大设计补证据 |
| 高：跨来源混用与时间口径 | 相同页面／事件窗口不保证同一 Publication、cohort、分母；不同查询端点处理尚未统一 | 在需要合并展示或比较的具体用例中，逐项核对来源、版本、分母、时区及半开／闭区间；一致才合并，不一致则分开解释。受控启动只读选项不等于数据库权限保障 | 已在第 1、5、8 节明确边界；本轮不改代码、配置或合同 |
| 高（恢复／更换来源前）：保管与责任 | 运行数据库有可写 upper，不能拿原 release 当当前内容身份；没有已核实的保管／恢复承诺 | 由用户或获授权责任方指定数据供应、口径确认、保管恢复、消费配置四项责任，可由同一人承担；确认需要保留的源数据、冻结输入、版本与质量证据。恢复另需隔离目标、权限和实际验证记录 | 四项责任仍未知；归档仅标为存在，不认定可恢复，不清理或覆盖 |
| 中，相关功能准入前必须解决：匹配的历史冻结输入 | 第 2.2 节已知所需 cohort／metric／path／lifecycle 身份，但上游实物未闭合；缺冻结映射、状态和切点证据时不能复现 | 按完整 ID／摘要定位某一被引用版本，取得匹配 manifest、冻结映射／生命周期及所需状态文件，再验证最小对象。不以当前 DB 或最新 INFO 替代，也不直接启动全量重建 | 保留具体身份线索及暂不准入结论；底层规则仍停留在已记录的验证程度 |
| 中，启用前必须解决：缺失的可选来源 | P0、选择清单、交付包或参照不足以形成相应能力 | 只有确定需要启用该能力时，选择获准来源、核对版本闭包和用途证据，再单独批准配置／部署；不能由治理任务自动恢复旧生产管线 | P0 明确不可用；其他分支按第 5 节区分未配置、路径缺失和业务未检查 |
| 中，正式对外使用前必须解决：保留、纠错及使用许可 | 有来源身份不等于有再分发授权；错误结果可能被长期引用 | 确认保留范围／期限和使用许可；若发现错误，记录受影响的事件、窗口、版本及用途限制，新解释采用可区分版本并保留旧引用追溯 | 许可及保留期未知；不公开数据、不静默改写旧版本、不推定可以删除 |

责任证据检查范围：本次读取 `/home/bgpdata/Domeye-Core-governance/approvals`、`reviews` 中的 24 份 JSON，以及 `/home/bgpdata/Domeye-Info-Migration/20260725T131422Z-s1-v5/evidence/` 下 S5 的 `static-info-release-acceptance.json` 和 S6 的 `static-info-closure.json`，递归检查明确的 owner／data_owner／maintainer／custodian／steward／approved_by／approver／reviewer／责任人／维护人／保管人／供应方标量字段，未发现命中。**这不是所有材料均无责任记录的证明**，也不以目录属主、账号、提交作者或泛化“已批准”代替数据责任确认。当前基线允许如实记为未知，尚不需要为了填满台账而指定人选；恢复、变更或作承诺前再确认。

### 本轮完成界限与后续维护

本次治理基线覆盖原始 MRT、解析／会话／状态、参考映射、数据库事件与特征、分析／发布／消费读模型及质量证据；每族有来源证据或明确线索，关键用途有条件性准入或暂不准入理由，未知项与下一步单列。已确认事实回填到原台账，未新增平台、机器枚举、自动质量门或底层实现。

本次修改仅在当前本地工作树的中文台账及 README；未同步到远端工作树，未提交、部署、写数据库或修改远端服务。本文的现场值是带时间的记录：今后变更数据位置、发布、进程绑定或解释规则时，应重新核对对应条目并记录新证据，不覆盖既有引用的版本身份。文档和检查方法可供复核，**不构成已实现持续监控、全量审计、备份恢复或业务验收**。

## 9．Issue #13：核心态势页首切片的制品复用与兼容性

### 9.1 本次结论与核对范围

**有界调查完成；正式“异常列表＋新增中断前缀趋势”接入仍为 REPAIR。** 已存事件记录可作为有限历史探索及后续消费适配的候选；不必为统计已记录事件的开始时间而重建 RouteState。尚缺的观察范围／覆盖依据、可复现输入版本及跨类型地址族规则，不能靠冻结当前查询结果或借用另一份 Publication 补齐。

核对发生于 2026-09-10 09:03—09:07 UTC（北京时间 17:03—17:07）。实际绑定再次确认为 `127.0.0.1:31627/bgp_project`，`SOURCE` 未显式设置，读取代码默认 `r`；通用国家事件读模型仍绑定第 2.2 节目录。SQL 经容器 `domeye_core_dev_pg` 的 `psql -X` 执行，显式 `REPEATABLE READ READ ONLY`，单查询超时 15 秒、锁等待 2 秒。未导入应用、读取凭据或个人判定记录，未执行 HTTP／浏览器验收。

业务范围为 `source=r`、发生时间 `[2026-02-27 00:00:00, 2026-02-28 00:00:00)`，不限制国家或 ASN；仅查询二月事件及其引用的二月详情。时间列不带时区，按数据档解释为 Asia/Shanghai；源发布的快照时区声明不是历史入库时间转换已验证的证据。未遍历其他月份、其他服务器目录或原始 MRT。

本地代码依据仍是本台账基线；远端 Core 官方 SSH `main` 本次解析为 `08e19fee89ee27691694cb32c5f889a2633445f7`，仅按其文档导航读取开发数据库来源说明和相关静态线索，未执行旧生产／管理脚本，也未把当前源码当成历史检测运行版本。本地台账与短规格在远端对应路径均不存在，未同步、覆盖远端文档或修改其他工作树。

### 9.2 来源核对解决了什么，没解决什么

- **已确认的来源记录可复用。** 第 3.1 节 release 的 `manifest.json`、`database-manifest.json`、`database-inventory.json`，以及派生 `state.json` 的 SHA256 本次均与原台账一致。发布根目录仅作一层文件名检查；组件清单将来源止于 `prebuilt_full_dump`，提供原 dump 摘要和处理快照，没有给出这些事件的 Collector、MRT 选择清单或检测批次关联。inventory 里的二月 28,528 行及零孤儿是源发布历史记录，不是本轮全月实测。
- **当前数据内容版本仍不成立。** 开发数据库说明与状态记录支持固定 release → 可写 Overlay 的派生关系；来源清单不是运行库各表当前内容身份。没有读取／恢复大归档，没有新建数据快照，也没有新证据证明历史生产可复现。
- **国家读模型的身份不能借给首页。** 第 2.2 节 manifest／COMPLETE 摘要本次复核一致；清单仍声明 RRC25、81 个事件及既有 cohort／metric／lifecycle 来源。伊朗条目仍绑定自己的 Publication 和窗口。它覆盖国家事件，不覆盖本次六类范围内实际出现的五类、共 2,038 条事件样本；本轮未重读其业务文件。
- **缺失消费选择未被修复。** 本次进程未配置 `DOMEYE_DATA_LAYER_224_310_SELECTION`、`P0_DATA_RELEASE_DIR`，默认 `data-layer/PRODUCTION-SELECTION.json` 仍不存在。不启用或绕过这些入口；第 5 节旧状态接口结果不冒充本次 HTTP 验证。

### 9.3 六类事件的关联与地址族

关联按现有详情查询语义，从总表引用解析类型、来源、对象与编号，在对应二月详情表按键匹配；再独立比较发生时间和等级，以及非泄漏事件的结束时间、时长。不能把自然语言 `affected_prefix` 或带名称的 AS 展示字段当作统一身份。

| 类型 | 当日总表记录／唯一详情匹配 | 地址族依据及本次结果 | 使用边界 |
|---|---:|---|---|
| 前缀中断 | 680／680 | 详情 `prefix`：IPv4 658 条、526 个去重前缀；IPv6 22 条、1 个去重前缀 | 可按记录 Prefix 分类；355 条无结束，不代表持续中 |
| AS 中断 | 50／50 | 50 条 `outage_prefixes` 均为非空数组且成员可解析；28 条仅列 IPv4、22 条仅列 IPv6 | 只说明已存数组的成员；未证明是固定集合、完整受影响集合或整个生命周期的地址族。混合／未知归属策略未冻结 |
| 国家中断 | 1／1 | 详情有 `outage_ases` 数组，无直接 Prefix／地址族字段 | 不能按国家、ASN 或当前 INFO 推定地址族；不强制归入 IPv4／IPv6 |
| 前缀劫持 | 0／不适用 | 详情表结构有 `prefix`，现有读取按其定位；当日详情也无记录 | 仅有结构依据，本窗口无真实配对样本，不外推可用率或检测器能力 |
| 子前缀劫持 | 2／2 | 子、父前缀字段均可解析为 IPv4，地址族冲突 0 | 本次验证这两条记录的归属，不推广为所有起源变化 |
| 路由泄漏 | 1,305／1,305 | 详情 `prefix`：IPv4 1,204 条、IPv6 101 条 | 详情表没有结束／时长字段，本次不作生命周期一致性通过声明 |

样本总表共 **2,038 条**，全部唯一关联，缺失／多重匹配为 0；起始时间、等级、引用内开始时间／来源不一致为 0。非泄漏的 733 条记录，其结束／时长字段不一致为 0。此为“总表记录能定位详情”的单向覆盖检查，不证明所有检测候选均应进入总表，也不验证判定算法、历史原因或原始输入完整性。

前缀中断的小时桶按地址族对账再次通过：IPv4 桶内去重和 653、IPv6 为 20，合计 673；事件数 680、全窗口去重数 527 分别保留。缺失结束仍为 355 条。旧列表两项隐含过滤使前缀中断从 680 剩 601、AS 中断从 50 剩 6；这只是应用“时长不少于 3 分钟或空值＋编号小于 10”后的计数，不是实际 API 返回数，不能解释为数据库缺行。

### 9.4 最小复用与兼容性结果表

| 用途 | 候选来源／版本依据 | 现有口径与缺项 | 所需适配及对既有消费者影响 | 调查结论 |
|---|---|---|---|---|
| 历史异常列表及详情 | 当前绑定事件月表及六类详情；源 release 已知，当前内容版本未知 | 事件引用可定位；旧列表另有时长、编号、其他筛选及闭区间条件 | 保留原表、引用和旧入口；新消费需显式统一筛选、时间、排序及未知处理，不静默改变旧结果 | 记录可复用；正式联动需要消费适配与版本证据 |
| 新增中断前缀趋势 | 同一版本下的前缀中断记录，或逐条对账后的总表前缀中断子集 | 新口径是开始时间入桶后 Prefix 去重；旧曲线是 3 分钟时点并发，另受 INFO 过滤、空结束延续影响 | 需要独立、可辨识的指标语义；不改写旧并发字段，不重跑检测。若持久化，只能作为引用原记录的派生结果 | 需要消费适配；持久化时需要新的派生结果身份 |
| 跨类型 IPv4／IPv6 筛选 | 对应类型的结构化 Prefix／前缀数组 | 前缀类有样本依据；AS 数组的范围限制、混合／未知策略待定；国家类型无法直接判定 | 类型分别解析，不用描述文本猜；不按来源不明的成员补全，不为配合控件隐性丢弃未知 | 部分可适配；国家归属证据不足 |
| 现有 P0 MetricSeries | `metric-series/v1` 合同及指标定义；本次无绑定的 P0 发布 | `prefix_outage_concurrent_count` 是 `max_180s(...)`；`anomaly_incident_count` 是独立 Incident 去重，不是旧总表行数；粒度固定 300 秒 | 不能将新增前缀／旧事件行数写入原指标名，不能直接把原型小时桶套进合同。若未来采用该体系，须单独明确兼容扩展／版本方案 | 现有语义不能原样复用；未批准合同修改 |
| 通用国家事件读模型与 Publication | 第 2.2 节已绑定清单和指定事件身份 | 事件、cohort、观察窗与本次跨类型列表不是已证明的同一输入组合 | 原有用途和引用保持不变；不拿其 Collector／Publication 为传统库补身份，不因同一日期就拼接 | 不纳入此次联动来源；不影响其既有有限用途 |
| RouteState／cohort／统一路由指标合同族 | 第 4 节合同及下游声明的来源 ID；匹配实物未在本轮恢复 | 提供状态／固定分母语义，不是异常记录新增计数的必要输入 | 本任务不创建第二套状态事实，也不要求完成旧 B/C 或全链重建 | 本切片不需要引入；普通路由状态指标仍待验证 |
| INFO 参考信息 | 第 3.2 节历史文件摘要，未重读业务内容 | 可用于有界参考展示，历史有效性／成员覆盖未获新证明 | 不让静态 INFO 决定此次事件是否计数；原有参考消费者不动 | 可保留原用途，不作事件人口或补历史身份依据 |

这张表不冻结新架构。**版本一致性要求是联动结果使用明确、兼容的输入版本组合，不是所有数据族共用一个 Publication。** 新增派生结果不能冒用旧指标名、旧内容身份或旧发布；也不能以读取成功替代来源准入。

### 9.5 真有必要新增时，最小需要什么

若之后获准保存本切片的可复现结果，最低需要保留：实际选用的事件及必要详情字段的内容身份、原表键／事件引用、来源实例与读取时点、查询范围、地址族处理与排除规则、时间桶／去重公式版本、关联检查结果和已知未知项。图表引用这些输入，不反向修改事件事实；原始检测版本无法追溯时，明确这是“已存历史事件记录”的派生解释，而不是重跑检测的同版输出。

这里只确定需要证明的内容，不创建新字段、清单格式、快照、指标或平台。范围证据不能从数据冻结中自动产生；要称为明确 Collector 的正式态势，仍须取得实际来源依据。今后确需修订已引用结果，保留原引用和可区分的新版本，并单独批准实施。

**下一步停止点：** 当前调查已足够把消费适配与数据生产分开。尚需用户确认跨类型混合／未知地址族如何呈现，以及是否接受仅标明限制的历史事件探索作为后续第一步；这不是用降级展示替代正式 C 的准入。来源／版本证据有明确新增位置后再有界核对，无位置不继续无目标扫描。没有启动下一阶段。

### 9.6 方法、来源与验证记录

- 数据结构／关联：本地 [事件详情服务](../backend/services/events_service.py)及六类详情查询；本轮实际表结构与主键交叉核对。列表隐含过滤见[事件查询](../backend/database/event.py)，并发与 INFO 过滤见[特征服务](../backend/services/features_service.py)及[旧统计函数](../backend/utils/get_event.py)。[首页聚合](../backend/database/dashboard.py)当前不含相同的 `source` 条件，不能直接拼接；本轮未改代码。
- 合同冲突：[MetricSeries](../contracts/data/metric-series.schema.json)、[指标定义](../backend/data_pipeline/common/metrics/series.py)、[RRC25 Publication](../contracts/data/rrc25-event-publication.schema.json)。后者约束国家事件及 RRC25，不能包装本次全部异常。
- 来源检查：第 3.1 节三份发布 JSON、派生状态 JSON 和第 2.2 节 manifest／COMPLETE 的完整文件摘要复核；本轮读取发布清单正文，但不读取 dump、INFO 业务行或国家事件正文。官方 Core 固定提交的 `dev/database/README.md` 是派生流程说明，不是新的执行回执。
- 业务核对：09:06:02 UTC 开始的只读事务完成五类关联、六类样本可得性与地址族检查；09:07:25 UTC 开始的另一只读事务复核分桶与过滤。两者分别记录，不宣称它们构成可长期恢复的数据库快照。

<details>
<summary>本次核心关联查询（已执行的只读核对，不是产品实现）</summary>

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout=15000;
SET LOCAL lock_timeout=2000;
WITH e AS (
 SELECT event_type,detail_url,level,s_time,e_time,duration,source,
   split_part(detail_url,'/',1) AS kind,
   replace(split_part(detail_url,'/',3),'-','/') AS obj,
   split_part(detail_url,'/',4) AS id
 FROM event_table_202602
 WHERE source='r' AND s_time>=timestamp '2026-02-27' AND s_time<timestamp '2026-02-28'
), f AS (
 SELECT 'prefix_outage' AS kind,source,prefix AS obj,outage_id::text AS id,
   s_time,e_time,duration,outage_level AS level FROM prefix_outage_202602
 UNION ALL SELECT 'as_outage',source,asn,outage_id::text,s_time,e_time,duration,outage_level FROM as_outage_202602
 UNION ALL SELECT 'country_outage',source,country,outage_id::text,s_time,e_time,duration,outage_level FROM country_outage_202602
 UNION ALL SELECT 'hijack',source,prefix,hijack_eventid::text,s_time,e_time,duration,hijack_level FROM hijack_202602
 UNION ALL SELECT 'sub_hijack',source,prefix,sub_hijack_eventid::text,s_time,e_time,duration,sub_hijack_level FROM sub_hijack_202602
 UNION ALL SELECT 'leak',source,prefix,leak_event_id::text,s_time,NULL::timestamp,NULL::interval,leak_level FROM leak_event_202602
), j AS (
 SELECT e.*,f.id AS fact_id,f.s_time AS fact_start,f.e_time AS fact_end,
   f.duration AS fact_duration,f.level AS fact_level,
   count(f.id) OVER(PARTITION BY e.detail_url) AS matches
 FROM e LEFT JOIN f ON e.source=f.source AND e.kind=f.kind AND e.obj=f.obj AND e.id=f.id
)
SELECT event_type,count(DISTINCT detail_url) AS list_refs,count(*) AS joined_rows,
 count(*) FILTER(WHERE fact_id IS NULL) AS missing_fact,
 count(DISTINCT detail_url) FILTER(WHERE matches>1) AS nonunique_refs,
 count(*) FILTER(WHERE fact_id IS NOT NULL AND s_time IS DISTINCT FROM fact_start) AS start_diff,
 count(*) FILTER(WHERE fact_id IS NOT NULL AND level IS DISTINCT FROM fact_level) AS level_diff,
 count(*) FILTER(WHERE fact_id IS NOT NULL AND kind<>'leak' AND
   (e_time IS DISTINCT FROM fact_end OR duration IS DISTINCT FROM fact_duration)) AS end_duration_diff,
 count(*) FILTER(WHERE split_part(detail_url,'/',2)<>to_char(s_time,'YYYY-MM-DD HH24:MI:SS')
   OR split_part(detail_url,'/',5)<>source) AS encoded_ref_diff
FROM j GROUP BY event_type ORDER BY event_type;
COMMIT;
```

地址族核对对已限定日窗的结构化前缀使用 `family(prefix::inet)`，对 AS 中断的 `jsonb_array_elements_text(outage_prefixes)` 成员取不同地址族；子劫持另比对 `hijacked_prefix`。未对自然语言字段作 CIDR 转换。分桶核对沿用[短规格中的方法](core-overview-v1.md)，此轮同样得到零桶差异；本节区分新的业务核对与旧记录，不把短规格中的首次数字改写为全量结论。

</details>

## 10．首页真实输入验证：三类异常映射与留存

2026-09-10，用户明确授权后续本目标所需的数据读取与留存。本节记录真实输入验证，不代表正式首页或整个数据基础已完成。查询时间为 **12:03—12:08 UTC**；业务范围为 `source=r`、`2026-02-27` 单日、三类异常，不限制国家或 ASN。项目时区、开发窗口和快照仍来自 `config/data-profile.json`，未改配置或首页默认日。

### 10.1 已获得的新证据

- 实际读取 `domeye_core_dev_pg/bgp_project`，容器镜像、回环端口 `31627`、Overlay 挂载与第 3.1 节一致。使用显式 `REPEATABLE READ READ ONLY` 事务，查询超时 15 秒、锁等待 2 秒；只选择业务字段，无 DDL／DML，无联系人或人工判定字段值。
- 重新核对四张月表的实际结构与主键：总表键为 `detail_url`；前缀中断完整键含 ASN；AS 中断及泄漏键与 #14 模型一致。所查起止时间列均为无时区、秒精度；数据库会话为 UTC，不据此改写项目对历史字段的 Asia/Shanghai 解释。
- 先验证每类 10 条引用，再读取同一天三类的全部总表引用；每条取齐其旧查询键对应的候选明细，不以 `LIMIT 1` 或预先过滤时间冲突制造唯一关联。
- 通过 #14 的公开转换、序列化和复读入口处理真实源字段。原引用、完整源键、等级与时间保留；明细与总表逐条对账。转换结果保留 `collector_unknown`、`detector_version_unknown`、`source_content_version_unknown` 和正式准入未建立的限制。

| 类型 | 真实引用数／唯一明细关联 | 未记录结束 | 本次可证明的内容 |
|---|---:|---:|---|
| 前缀中断 | 680／680 | 355 | 本日所选引用的映射、原值与留存复读通过；缺少结束不代表持续中 |
| AS 中断 | 50／50 | 1 | 原前缀数组、数量／比例及路径字段保留；不升级为完整影响集合或权威分母 |
| 路由泄漏 | 1,305／1,305 | 来源不提供 | 原角色和路径保留；不推定持续、结束、攻击或责任 |
| 合计 | 2,035／2,035 | 不合并为生命周期指标 | 零条、多条或冲突关联均为 0，仅限此日三类总表引用 |

前缀中断按开始时间分小时桶、在桶内按规范化 Prefix 去重，并按地址族与同一读取事务中的独立 SQL 结果逐桶比较，差异为 0。结果仍为 **680 条记录、527 个全日去重前缀、673 个桶内去重数之和**。小时桶仅用于本次对账，不因此冻结正式页面粒度，也不改写旧并发指标。

### 10.2 实际留存与复现位置

本地验证目录为 `.local/core-overview-validation/20260910T120154Z/`，由 Git 忽略，不向远端仓库上传真实行。SQL、读取回执、必要业务字段、转换结果、数据档及转换源码副本分别留存；原库、旧发布、旧引用与正式页面未修改。

| 留存项 | 内容与身份 |
|---|---|
| `sample.jsonl` | 30 条引用；40,869 字节；SHA256 `30b6f4790610704423e0513fa9b6ef7b33e04810056e750129d6a428b3727e06` |
| `day.jsonl` | 2,035 条引用；2,151,743 字节；SHA256 `a5dff0b57609375c1e181bc171b99ec588d1e389f141fce0e60cf0e585074d01` |
| `day-records.jsonl` | 实际转换结果；SHA256 `28032feb35a9bed082a55b93c70900730e8253e3260dce3b9d802ce70f88bc03` |
| `extract.sql` | 明确日窗、类型与源字段投影，候选不截断；SHA256 `9edb025e0917f1956c06f38a3af428bd394f631a6a540fdb399a7f8a12785ad7` |
| `metadata.jsonstream`、`provenance.json` | 本轮表结构、读取上下文、容器绑定及来源清单核对；不是历史生产覆盖证明 |
| `sample-audit.json`、`day-audit.json` | 映射、关联、空值、复读及独立桶对账回执；未标为正式准入 |
| `首页真实异常输入复核.ipynb` | 中文离线伴随笔记本；已从头执行，用留存输入重算并与保存结果、独立 SQL 桶值对比，无数据库连接 |

上述文件是当前机器上的 Git 外证据，不是仓库中已发布或远端可下载的制品。字节摘要绑定本次所选字段及读取上下文，不能称为整个运行库的不可变内容版本。生成日窗结果后，先留存的样本字节及转换结果仍匹配其原摘要，且可复读；未追补不存在的历史修订。

### 10.3 仍未解决的准入条件

本轮进一步读取来源 dump 的 `source-full-dump.tsv` 和独立摘要文件，二者摘要与数据库清单一致：分别为 `2ae2f56563e82def7dd9de56f0253bfdd9176a9e56b12840288ca222671aba96`、`8f860cc69defdea1ed154fdddb8877166acfaf7757ac9185e6265af68121eb93`。这些元数据只记录库名、转储时间、版本、大小及摘要，没有给出 Collector、MRT 输入选择或历史检测批次；未读取／恢复大 dump。官方 Core 文档核对绑定 `093bb8331535d48b4b876291406bb72812793c82`，不把其中 Agent 的 RRC25 能力范围借给本批历史异常。

**结论：真实记录映射与选定内容复读已验证；正式 C 态势准入仍为 REPAIR。** 当前可复现输入内容的问题已对这一日窗的三类选定字段取得实证，但观察来源／覆盖、历史检测版本和跨类型混合／未知地址族呈现仍需分别处理。不得把本轮数据授权理解为降低用途准入门槛。

下一步用途选择已向用户提出：是否先在本地 C 页面展示明确标注来源、留存版本和限制的“已存历史异常记录”，同时保持正式 Collector 态势未准入；或先补齐观察来源证据。用户尚未作出该选择前，不以有限历史展示替代原目标验收，不启动正式首页接入。没有重开或改变已完成的 #13／#14，也没有提交、推送或修改共享服务。

### 10.4 本阶段独立审查

- **Standards**：明文规范违反 0、阻断项 0；发现一处轻微分桶逻辑重复，在一次性有界审计中接受，不引入额外框架。审查基于本地 SQL、回执与留存内容，不代表独立重查远端实例或 dump 本体。
- **Spec**：本阶段规格问题 0；独立离线复算确认 30 条样本、2,035 条日窗记录及 44 个地址族小时桶通过核对。没有把记录可复核升级为正式态势准入，也没有把本阶段通过视为整个 goal 完成。
- 审查范围仅为本轮查询、留存与第 10 节；其他 23 个前序文件摘要不变。可离线复现已验证，首页接入与浏览器验收尚未开展，用途选择仍待用户确认。

### 10.5 留存输入的地址族补充复核

同日后续仅使用第 10.2 节已留存内容离线重算，不再次读取数据库。前缀中断为 IPv4 658 条／IPv6 22 条；路由泄漏为 IPv4 1,204 条／IPv6 101 条。事件的规范化 Prefix 与对应原字段逐条一致。50 条 AS 中断的已存前缀数组均非空、可严格解析且无重复成员，其中 28 条仅列 IPv4、22 条仅列 IPv6；数组长度与已存 `max_outage_prefix_num` 在 50 条记录中均相等。

这些结果只证明本批留存字段的一致性：数组长度相等不证明受影响集合完整，仅列某一地址族不代表整个 AS 或事件全生命周期仅涉及该族。本批没有混合、缺失或不可解析数组样本，因此没有实证可据此取消混合／未知分支，也未冻结跨类型筛选策略。正式态势准入与历史记录展示用途的待决状态不变。

复现文件为同一 Git 外目录中的 `首页真实异常输入复核-地址族补充.ipynb`，已从头执行并通过结构校验，包含原五项复核及本次补充检查。原笔记本未覆盖，SHA256 仍为 `a04cd8250b72854a8ea13c89d2e917fe482f82b72cb5a43b7cef3c782881cb29`。本补充没有修改产品代码、原输入、数据档或任何线上服务；不把上一节独立审查自动延伸为本补充的新审查报告。

## 11．用户确认来源及后续治理边界

2026-09-10，用户明确确认：当前仅有 RRC25 一个 Collector，旧记录中的 `source=r` 对应 RRC25；观察覆盖未知；历史检测版本不追补，从今后的检测开始记录。随后要求数据不清楚时自行查询 Domeye 相关目录，找不到即记录，无须再审批。此为本次用户确认的来源依据，不冒称源行直接携带 Collector 或已核实历史运行日志。

- 允许在首页注明 **RRC25／已存异常记录**，展示已留存且可对账的数据。覆盖与旧检测版本继续未知，不再以这两项未知阻断该用途；不能推出全网状态、漏报率、Peer 覆盖率或检测准确率。此前 REPAIR 为当时调查结论，保留不改；当前用途准入按本节执行。
- 后续读取、留存、离线派生所需数据自行有界查询，保留检查位置和未找到项；不据此授权写旧库、覆盖旧制品、部署或修改共享服务。
- 不修改第 10 节已引用的输入或异常记录版本。消费包另外绑定原内容摘要、本次来源确认和新的解释版本；原记录里的 `collector_unknown` 是首次转换时的历史状态，不静默重写。当前解释版本不是历史检测版本。未来实际检测应记录代码／规则版本、参数及输入内容身份，本轮不启动检测或持续采集。
- 地址族采用已存结构化前缀成员：前缀事件按自身 Prefix；AS 中断按已存数组。明确有两族的记录在两种筛选中均可出现；缺失、空数组或不可解析成员保留未知，不猜测。全部包含未知；单族筛选明确排除无法判定的记录，并提供未知筛选。该规则不宣称 AS 的完整影响集合。

当前实施范围仍为 C 首页的三类异常列表、每小时新增中断前缀趋势及同版本详情；先复用第 10 节的留存日窗，不冒称已覆盖配置全窗口或其他三类异常。默认日仍来自项目快照；未留存的日期显示不可用，用户可显式选择已留存日窗。总体规模与普通路由变化继续未知。测试沿用公开只读 HTTP 查询与页面交互，不访问真实数据库。

## 12．C 首页真实数据闭环验收

2026-09-10，同一 goal 继续实施并完成本地验收：**用户可以在已确认 C 结构中查看真实异常、筛选排序、点选小时和复读同版本详情。** [本地入口](http://127.0.0.1:28492/?date=2026-02-27)；启动方式见 [README](../README.md#本地-c-真实记录展示)。仅绑定第 10 节留存输入，不连接旧数据库，不启动检测、发布或持续采集。只重启过本任务自己的隔离后端以检查断连与恢复，未修改共享服务。

### 12.1 留存与版本

消费目录为 `.local/core-overview-inputs/rrc25-20260227-v1/`，在 Git 外。`records.jsonl` 原样复制第 10 节记录字节，SHA256 仍为 `28032feb35a9bed082a55b93c70900730e8253e3260dce3b9d802ce70f88bc03`。原记录里的未知与历史准入状态保留；当前来源声明单独返回，不反写旧记录。

- `manifest.json` SHA256：`839e2fcde03801b0a4e0eb8f14263ffc04e9c2608a66ef7342ccae914a60500b`。
- 消费版本：`overview_v1_839e2fcde03801b0a4e0eb8f14263ffc04e9c2608a66ef7342ccae914a60500b`，绑定上述记录摘要、项目数据档、窗口、三类范围、来源确认和解释版本 `recorded-anomaly-overview/v1`。
- 留存包同时保留 `retainer.py` 及其摘要；[离线命令](../scripts/core_overview/retain-core-overview-input.py)拒绝覆盖已有目录，校验源字节、映射与只读审计回执。摘要不是签名或不可变存储保证，备份／恢复演练仍未验证。
- 本轮消费实现及受影响测试／合同另留存于 `.local/core-overview-runtime/implementation-source.tar.gz`，SHA256 为 `3a34cca4164d023aa39036c2f494a31620cc3fd28ea34aa4a252375b877fd945`；代码基点仍为 `983f0d4ac5ed27c60183ce5a46d617a9c4828b4f`，未提交。这是本次实现证据，不是完整运行环境备份。
- 后续输入或解释规则变化应使用新版本及新目录，不改被引用旧包；解释版本不是历史检测版本，也不是 P0 Publication。当前服务只加载一个明确绑定包；旧包可离线复读，不承诺多版本在线查询。

### 12.2 消费口径与兼容性

[只读服务](../backend/services/core_overview_service.py)提供 `/api/v1/core-overview` 与 `/api/v1/core-overview/record`。默认日期来自配置快照；默认 2026-03-31 未留存时返回 `window_not_retained`，概况、趋势、列表为 null。页面说明留存日并提供显式入口，不暗换默认日期。

日期和地址族共同约束概况、趋势、列表；类型、等级、搜索、小时为列表局部筛选。默认等级优先、发生时间倒序、原引用稳定排序，可切换时间倒序。没有旧的时长、编号或 INFO 成员隐含过滤。地址族规则按第 11 节，未知单列。桶宽为一小时、业务时区、左闭右开；新指标名为 `recorded_prefix_outage_starts_distinct`，不冒用旧中断曲线。

列表、分页、筛选及详情携带同一消费版本，变更返回 409；输入缺失或校验失败返回 503，不转换为空列表。详情保留原引用、记录内容版本和完整源键。跨业务日结束带日期，详情始终显示完整业务日期时间；缺结束仍显示未记录，泄漏仍为来源不提供。

旧 API、P0 指标和原 C 模拟原型未改语义；旧 P0 页面保留于 `/legacy-overview`。本轮隔离服务没有为旧页面绑定数据库／P0 发布目录，实际检查只确认入口保留及正确不可用状态，**不声称旧页面真实数据同时可用**。未修改 #14 最小异常模型、项目配置或既有原型。

### 12.3 实际验收证据

真实 HTTP 对账在 **13:01:42 UTC** 完成，107 次只读请求：21 页遍历核对全部 2,035 个唯一引用及列表字段；全部／IPv4／IPv6 共 72 个小时桶与留存 SQL 结果、列表去重数一致；按类型、地址族、结束状态及跨日抽样的 9 条完整详情与原序列化记录相等。不是逐条 HTTP 检查所有详情，也不是重新验证检测算法。

| 核对量 | 结果 | 解释 |
|---|---:|---|
| 前缀中断／AS 中断／泄漏记录 | 680／50／1,305 | 合计 2,035 条，三类、一个留存日窗 |
| IPv4／IPv6／混合／未知记录 | 1,890／145／0／0 | 当前留存内容；混合及未知分支另用合成数据测试 |
| 全日去重前缀／小时桶去重和 | 527／673 | 均不同于 680 条前缀中断记录 |
| 02:00–03:00 前缀中断 | 63 条／60 个去重前缀 | 浏览器点击联动实测，柱值为 60；上方概况仍为 2,035 |
| 跨业务日结束 | 126 条 | 按 Asia/Shanghai 判断；跨日展示缺陷已修复 |

复现脚本 `.local/core-overview-runtime/verify_http.py`；结果 `http-audit.json` 的 SHA256 为 `b92e848514471ca5887e5240a4e18bb26169704ed669f37740ea80e395c7a387`，其中记录输入／脚本摘要与消费版本。脚本只读本地服务及已留存证据，输出文件已存在时拒绝覆盖。

实际浏览器检查完成：桌面画面、类型与 IPv6／未知筛选、搜索、等级与时间排序、小时联动、翻页、三类详情、完整源键、跨日时间、Escape 关闭及焦点恢复；390×844 移动首屏／列表／详情可读，文档宽度 390 无横向溢出。默认未留存日不会补零；本地 API 断连时清掉旧计数／柱图／列表，恢复后可重新读取。全页拼接截图出现工具重复拼接，未作为页面缺陷或验收图使用，改以真实视口截图核查。

`make api-types` 生成类型且再次生成摘要不变；`make test`：后端 **260** 项、前端 **134** 项通过；`make build`（含类型检查）通过；`git diff --check` 通过。五项旧警告未作为新通过证据消除。自动测试只用临时合成数据／mock；真实 HTTP 与浏览器检查单独执行。

### 12.4 审查、未知及任务收口

- **Standards**：独立静态审查未发现本轮硬规范违反或需要单列的代码异味；不把静态审查等同于真实运行验收。
- **Spec**：独立审查发现 1 项 P2（跨日结束日期省略），已修复、补测试并复查关闭；其他指定范围无确认偏差。新增离线留存成功／拒绝覆盖／摘要损坏／非只读回执检查由主代理补充并运行。
- 来源查找记录复用第 10 节 `provenance.json`：已核对 `/home/bgpdata/Domeye-Core-dev-data/state.json`、对应 release 的清单／数据库清单／inventory 及 work 目录 dump 元数据。追溯止于该层，**未取得该批记录的完整观察覆盖或历史检测版本**；不解释为全目录不存在。Collector 采用第 11 节用户确认，不冒称日志证明；历史检测版本不继续追补。
- 数据供应责任、留存保管期限、备份恢复演练，以及总体可见规模／普通路由变化／其他三类异常准入仍未验证，不补值、不扩大当前任务。不清楚的数据以后自行有界查找并记录，不再请求逐项审批；后续实际检测需记录代码、规则、参数及输入身份，本轮未启动检测。

**原 goal 的首个首页真实展示与支撑治理闭环已完成。** 本节与短规格补充记录作为原任务验收记录，不重开或改写已完成的 #13／#14。没有提交、推送、部署、写旧库或覆盖历史制品；完成不代表全项目所有数据已治理。原输入、映射代码与数据档摘要保持不变；前序改动的 24 文件中，仅 README、短规格、台账按此次授权补充，其余 21 份摘要不变。

## 13．按优先级补齐缺口：日期消费扩展（进行中）

2026-09-10 新总 goal 依次推进日期覆盖、另外三类异常、可见规模、有限普通路由变化、稳定交付与恢复；**本节不是整个总 goal 或日期覆盖已完成的声明**。用户允许跨会话推进；阶段交接保留证据路径、当前状态、验收边界，不复制整段讨论。

### 13.1 实际日期盘点

13:30–13:31 UTC 对同一 `domeye_core_dev_pg/bgp_project` 执行 catalog 查询及两个月事件／明细聚合，事务为只读、可重复读、最终回滚，单语句 15 秒、锁等待 2 秒。Git 外证据：`.local/core-overview-dates/20260910T132944Z/` 的 `catalog.sql/jsonl`、`inventory.sql/jsonl` 和 `read_query.py`。

- 三类总表记录实际分布在 **2026-02-24 至 03-31 共 36 天**；前缀中断 1,068,585、AS 中断 16,458、泄漏 63,281，共 1,148,324 条。已核对两个月所需八张表存在。各日聚合 ref 去重数相等，但这不是行级关联验证。
- 默认日 **03-31** 有前缀中断 12,302、AS 中断 290、泄漏 736，共 **13,328** 条；仍待完整候选关联、留存与小时桶验证，不能直接接入。另三类当天聚合为前缀劫持 562、子前缀劫持 446、国家中断 74，仅记录存在性与数量。
- 02-01 至 02-23 未出现匹配聚合组。这不证明采集无缺口，也尚未形成可供页面消费的成功空日留存。不能直接填零。
- `inventory.jsonl` SHA256：`8da3ba17fd329d8570d0a9dcb76fc8cba1be4dffb8cc43118787020c89e89ef5`；SQL 摘要：`2c7a3e49792e1b48b8beedfce758cc65290d1d32afed76996c86db946b127887`。仍不把可写 Overlay 当成冻结快照。

### 13.2 第二个真实日与按日索引

2 月 28 日有界读取发生于 **13:46:36–13:46:52 UTC**，完整导出三类总表引用及候选明细，不预先过滤时间冲突或限制候选为一条。证据位于 `.local/core-overview-validation/phase1-20260228-v1/`。

| 核对量 | 结果 |
|---|---:|
| 前缀中断／AS 中断／泄漏 | 2,040／216／5,980，合计 8,236 |
| 唯一关联、序列化往返、总表／明细共同字段 | 全部通过；无冲突记录 |
| 全日去重前缀／小时去重和 | 1,916／2,031，与独立 SQL 桶对账一致 |
| 地址族 IPv4／IPv6／混合／未知 | 8,128／107／1／0 |
| 前缀中断／AS 中断结束未记录 | 345／17；未推定持续中 |

原始导出 `day.jsonl` 为 9,050,568 字节，SHA256 `d5e4b415d706ef2727d145a6174b1d542d558cfe3cfc9c0c43183ee3cd86f12d`；转换字节摘要 `f3475fe76cb160f4df02479528ac56151708a567b78fc993c18bc4de23b58cf8`。新单日包 `.local/core-overview-inputs/rrc25-20260228-v1/manifest.json` 摘要为 `4662e557cbbea1a94d0463473d735a872722cbcecb96f8886e6afdb835315e0c`。复用的原始映射保留当时 `formal_admission=REPAIR` 与旧未知，Collector 用户确认仍通过消费来源说明单列，不反写原记录。

离线索引命令 `scripts/core_overview/index-core-overview-inputs.py` 已生成 `.local/core-overview-inputs/rrc25-dates-v1/`，明确选择 02-27 和 02-28 两份单日包。消费版本为 `overview_index_v1_b35810b13c1542a4364aaf2483e2f4fbac9b47cbf96ddf8391cc525eedc50d64`。每个日期独立 SQLite 文件，保留原序列化记录与清单；列表只解析选定分页投影，详情只读指定原记录。不在请求中生成索引，不将百万条整窗 JSONL 塞进每次查询。旧单日包、旧原始输入及内容版本未改动。

日期目录不表示连续覆盖。未选择日期为 `window_not_retained`；日文件缺失或校验失败仍为 HTTP 503，指标全部 null，但已验证目录可供用户切换其他日期；整个目录失败则不返回伪造目录。筛选成功但无匹配的零值与上述状态不同。读取拒绝 WAL／共享内存／日志旁路和 WAL 文件头，用只读单文件模式且前后检查文件实体；摘要缓存不证明不可变保管，也不能保护被恶意同步改写的清单与输入。

### 13.3 本地验收与剩余工作

[当前本地页](http://127.0.0.1:28492/?date=2026-02-28)可在两天间选择。默认 03-31 未暗换，仍显示未留存。只重启本任务的隔离后端 28491；前端 28492 和原型 5179 保留。临时错误验收使用 28493／28494 及独立副本，结束后已停止临时服务；原包未删除或损坏。

- 真实 HTTP 共 **277 次**：分页核对全部 **10,271** 个唯一引用及列表字段；两天共 **144** 个地址族／小时桶与独立 SQL、列表去重数一致；**19** 条按类型／地址族／结束状态／跨日抽样详情完整一致。修复后再次完整复读通过。脚本为 `.local/core-overview-runtime/verify_dates_http_after_review.py`，报告 `http-dates-reviewed-audit-2026-02-27.json` 摘要 `13b24053cbeb32f6144b6f7ba7f7575e3da8176dd1de33481cda3f2424d9bb1f`，02-28 报告摘要 `faa29ffc1815bd92f9c6055034c6d1bf44520886daf9c8da4d506e7ed92444b3`。不把抽样详情称为逐条详情验收，不把这两天的响应速度推广到最大单日规模。
- 实际浏览器：选择 02-28 显示 8,236，切换 02-27 显示 2,035；手动选缺日后清空指标；390×844 宽度无横向溢出，临时视口已恢复。独立副本首次打开缺文件日显示错误且保留日期选择，切换完好日后恢复 2,035；仅测试副本缺文件，不影响原包。
- `make api-types`、`make test`（后端 **274**／前端 **135**）、`make build`、`git diff --check` 通过。测试只用 fixture/mock/临时目录。测试钩子隐式返回 mock 导致误作清理函数的问题已修正，错误断言保留。
- **Standards**：发现 2 项硬缺陷（P1 WAL 旁路、P2 UTC 窗口日期归属），均补回归、修复并由独立审查复核关闭；另有来源校验重复的非阻断建议。**Spec**：发现 1 项 P2（首次日文件失败时丢失日期目录），已修复、复查并实测。审查基点是本阶段开始前 `before/` 文件副本及明确新增文件，不冒用空提交差异；未提交或推送。

**下一步仍优先 03-31 的完整输入核验，然后逐日扩展**。特别大的单日需验证离线读取上限与资源消耗，不能凭索引接口通过就宣称可承载全窗。成功空日留存、另外三类异常、规模／普通路由变化、独立完整备份与恢复仍待推进。独立核验会话创建仅返回待准备标识，至本节核对尚无可用会话 ID 或运行证据，不把创建请求当作已执行；该请求不替代未完成的默认日核验。

## 14．默认日完整核验：等级冲突待处理

### 14.1 已取得的输入及核验结果

2026-09-10 **14:04:37–14:04:57 UTC**，在同一 `domeye_core_dev_pg/bgp_project` 中只读、可重复读导出默认业务日 **2026-03-31（Asia/Shanghai）**的三类全部总表引用及完整明细候选，最终回滚。范围与第 13 节盘点一致；不在关联前丢弃时间冲突，不用单候选截断掩盖重复。没有重跑检测、写源库或修改服务。

Git 外证据位于 `.local/core-overview-validation/phase1-20260331-v1/`：`catalog.sql/jsonl`、`extract.sql`、`capture.py`、`day.jsonl`、`day-receipt.json`、标准核验失败回执 `day-audit-failure.json`、诊断脚本与 `day-diagnostic-audit.json`。导出脚本限制标准输出 64 MiB、错误输出 64 KiB、进程 90 秒，超限失败不作为成功留存；数据库单语句 15 秒、锁等待 2 秒。

| 核对项 | 结果与边界 |
|---|---|
| 三类记录 | 前缀中断 **12,302**、AS 中断 **290**、泄漏 **736**，合计 **13,328** |
| 完整源键与候选 | 13,328 个唯一引用全部唯一关联；无丢失或多候选；全部开始时间在业务日内 |
| 总表／明细共同时间字段 | 开始时间全部一致；两类中断的结束时间、持续时间全部一致；泄漏明细不提供结束语义，不冒称已比较 |
| 等级冲突 | **14 条 AS23860 中断**：总表 `low`，明细 `middle`；占 AS 中断 **4.83%**，占三类全部记录 **0.105%** |
| 前缀小时统计 | 48 个已出现的小时／地址族桶与独立 SQL 一致；前缀记录 12,302、小时去重和 **7,561**、全日去重 **2,729**，三者不能互换 |
| 结束未记录 | AS 中断 **28/290（9.66%）**；前缀中断 **622/12,302（5.06%）**；不据此推定持续中 |
| 转换与复读 | 13,328 条重新转换、序列化往返一致；**这不消除总表／明细冲突** |

`day.jsonl` 为 **16,560,305 字节**，SHA256 `5c8244e8b0c7b149e744a8507e6a2f5d8a8727dcf7d71ec160af26003eb8231c`；查询摘要 `19c78e7ffcd814e692128d584c9398723752ced998a7e17398270b4c26d053a6`。诊断记录摘要 `3508d223571f7eb55c9df319bee4b6132edd8f5b93758fc7a8624b14186a1a17`，诊断报告摘要 `1463d812037c875f26981febd495401178cfb8b9ee2b0b555d9193a7f9412269`。这些绑定只证明本次所选字段及留存字节，不证明原库不可变、历史覆盖或检测正确率。

### 14.2 冲突的解释边界

冲突引用全部保存在诊断报告中，AS23860 对应记录号为 `515,516,517,519,520,521,522,523,524,525,526,528,529,531`。明细峰值比率为 0.325（11 条）或 0.326（3 条），但**没有用当前阈值重算等级来替代历史证据**。

静态检查 `/home/bgpdata/Domeye-Core/backend` 与 `/home/bgpdata/Domeye/backend` 的中断处理、AS 明细和事件总表写入路径。四份现存源码以 `.py.txt` 复制到该证据目录的 `producer-source/`，复制前后摘要相等；具体路径、行号与 SHA256 见其中的 `来源说明.md`。没有导入或执行旧代码。

现存代码的 `as_outage_end` 更新明细峰值和等级，`event_end` 只更新总表结束和持续时间；另有更新总表等级的路径。**总表可能保留较早等级是一个解释线索，不是已验证根因**。未取得与这批记录绑定的历史检测版本／执行日志，不判定哪一边正确，不追补历史版本，也不顺带修复旧生产管线。`r → rrc25` 仍仅采用用户确认，观察覆盖仍未知。

### 14.3 当前状态、待决策及验收要求

标准核验在首个等级冲突处失败；后续诊断完整统计所有冲突，状态仍为 **REPAIR**。诊断文件没有改名成标准成功输出，**未生成 03-31 的正式消费包、未变更其页面等级规则**，已有两日包和版本未改。

本轮再次只读检查实际 API：默认日期仍为 03-31，`available_dates` 仍为 02-27、02-28，默认返回 `window_not_retained` 与空指标。这里是**消费目录未包含该日**，不是原始导出不存在；“已有输入但校验发现冲突”的诊断状态尚未接入消费目录，仍是待补缺口，不以台账记录代替页面交付。

**待用户确认的最小消费规则**：保留全部记录及已验证计数；这 14 条的消费等级显示“等级待核实”，详情并列保留总表／明细原值及完整引用，不静默选低或中，不删除记录，不修改源库。确认后才设计最小消费补充及新版本，并核验总量不变、冲突标签与等级筛选／排序一致、详情原值可追溯、前缀小时对账不变、旧版本仍可读；真实 HTTP 和页面验收后才能称默认日可用。该建议不是已批准或已实施的规则。

中文伴随 notebook 为该证据目录的 **`默认日真实异常输入核验.ipynb`**，使用 `build_notebook.py` 生成并从头到尾执行成功，输入摘要、直接源字段比较、全部转换复读、独立 SQL 桶对账均可复查；摘要 `129f8bcf518f2353c79d11903e5d15dc0f0e6456eddef84c7c38afd13ed2f12e`。使用临时 `uv run --no-project --with nbformat --with nbclient --with ipykernel python .../build_notebook.py` 环境，未修改项目依赖锁；生成器拒绝覆盖已有 notebook，复跑已有 notebook 应在其所在目录按顺序运行，不重跑源查询。数据质量与结论验证结果可带上述限制分享，**核验过程通过不等于数据准入通过，也不是完整独立备份恢复演练**。

本轮仅追加核验文档与 Git 外证据，未修改产品代码、源数据或运行服务。原日期扩展及其余优先级工作仍未完成；整体 goal 保持进行中。

## 15．日期继续扩展：五日可用与三月质量缺口

### 15.1 新增三日已在本地可用

默认日等级规则仍待用户确认；没有把自动 goal 续跑当作该口径已获批准。本轮先完成不依赖它的 **02-24、25、26** 三日，沿用三类历史记录解释、来源、地址族和小时去重规则，不修改页面设计、异常检测或产品代码。

| 业务日 | 前缀中断／AS 中断／泄漏 | 三类合计 | 前缀小时去重和／全日去重 |
|---|---|---:|---:|
| 02-24（新增） | 465／40／2,934 | 3,439 | 400／322 |
| 02-25（新增） | 692／59／3,671 | 4,422 | 678／548 |
| 02-26（新增） | 676／52／9,645 | 10,373 | 668／522 |
| 02-27（保留） | 680／50／1,305 | 2,035 | 673／527 |
| 02-28（保留） | 2,040／216／5,980 | 8,236 | 2,031／1,916 |

新增 **18,234** 条，五日共 **28,505** 条。新增三日的全部引用均唯一关联，开始／结束／持续时间／等级共同字段一致，原字段重新转换及序列化复读一致，独立 SQL 小时桶对账无差异。03-31 冲突行未混入。02-25 有1条混合地址族记录，IPv4／IPv6筛选均可包含它，不把两种筛选计数相加当总量。

三日只读导出分别发生在 2026-09-10 **14:28:12、14:29:09–14:29:10、14:30:07–14:30:12 UTC**，数据库和事务边界沿用第14节。证据分别在 `.local/core-overview-validation/phase1-20260224-v1/`、`phase1-20260225-v1/`、`phase1-20260226-v1/`。`capture.py`／`audit.py` 复用已审阅版本，SQL只调整月份和日窗；不是三个日期在同一历史事务中的原始快照。

| 日期 | 原始导出 SHA256 | 转换记录 SHA256 |
|---|---|---|
| 02-24 | `611408b0c774de0921fa0df30bf3c6b4d1c4515bd56abcc355fea70dde102630` | `2a3ef929fdda8f09bc18b7bf2511ea530724786ad0a66803b6030c189cd9156c` |
| 02-25 | `9ddf9d5c120310300a4e658d94cf4d039070c444005c84d971ac43b926a2bc26` | `64db1ef5845eda8ddb2337ca304b0f66e997909991f5c9a24e54b4da49d48fd7` |
| 02-26 | `301fc958e28a0be7cee50bfd7a7e7a3c1c921266784fa968e1cc28f461d9269a` | `f68d71a68fee9f04096a05dde29606f9cecdb7e3820cd4fcb0e7198d9032a15d` |

三个新单日包为 `.local/core-overview-inputs/rrc25-20260224-v1/` 等；五日索引为 **`.local/core-overview-inputs/rrc25-feb24-28-v1/`**，版本 **`overview_index_v1_e55f564a2fe9e51c907eb47c0b22f5cbcb9faca212d21abd936201473485ec47`**。新索引绑定完整现存索引器摘要 `ec794357da229670d9ce1132bfeb74522e54a1a09fae0ff08236bff7916690f8`。旧两日目录、27/28索引字节及原单日版本未改。只重启本任务隔离后端28491以选择新目录；前端28492与原型5179保留，未部署或修改共享服务。

### 15.2 五日真实验收与审查

- `verify_february_http.py` 执行 **722 次真实 HTTP**，逐页核对五日全部28,505个引用及核心列表字段；**360** 个地址族／小时查询与独立SQL桶及列表去重一致，**49**条按类型、地址族、结束状态与跨日分层抽样详情完整一致。不是逐条详情HTTP验收。五份结果是 `.local/core-overview-runtime/http-february-five-days-audit-2026-02-*.json`，每份绑定原记录、验证脚本及新消费版本摘要。
- 审查后新增 `verify_february_filters_http.py`，再执行 **897 次真实 HTTP**：精确五日目录、两种排序全部分页、四种等级筛选全部分页、每日前缀／ASN／无匹配搜索、120个小时起止标签、局部筛选不改概况／趋势，以及三种未留存日的空指标。结果 `http-february-filter-audit.json` 摘要 **`42b102bf1d72d4bc0ccf00e68f23f2dc12cb38f7d14001aa0a5fd4e1c09916d0`**；脚本摘要 `322b01fd01e9592d57fe283cec78243bc0aa923c4b0fe3d085ce0f36cb9e71d3`。
- 实际浏览器：从旧版本点击重新读取后目录为5天；新三日计数为3,439／4,422／10,373；02-25的AS中断筛选为59而概况仍4,422；详情保留同版本完整源键与内容版本。手动选择03-31后指标和列表为空、不补零，恢复02-26及全部类型后为10,373。过程见 `.local/core-overview-runtime/五日浏览器验收.md`。本次未另作完整移动／宽屏布局回归，不把此前视口检查冒称新证据。
- `make test`：后端274、前端135通过；`make build`、`git diff --check`通过。测试只用fixture/mock/临时目录；真实核对单独运行。本轮产品代码与OpenAPI未变，没有生成新的接口字段。
- **Standards**：按旧两日→新五日制品及明确脚本差异独立审查，未发现硬违反；1项权限建议已处理，新三日审计目录0700、业务文件0600，字节摘要不变。自动生成的缓存子目录保留可遍历权限，不清理文件。
- **Spec**：未发现新增数据错误或越界；指出实际验收需补日期目录、排序、等级／搜索、桶标签和新三日浏览器证据，已补上述897次检查与UI记录，由独立审查复核关闭。没有把整个dirty tree或空git提交差异当成本轮范围，没有提交或推送。

本轮中文伴随 notebook：`.local/core-overview-validation/phase1-calendar-quality-v1/日期扩展与质量缺口核验.ipynb`，从头到尾执行成功，摘要 **`e08c1357567fa7842ce9d3678725f2ce26d58be3f607d51b7da63133cf86925c`**。它冻结的是主会话当时的三月部分预检与新增三日/五日HTTP检查；随后独立会话补齐三月前缀的结果另见15.3，不改写原notebook的历史结果。

### 15.3 三月源字段预检已取得，不等于消费准入

主会话 `.local/core-overview-validation/phase1-calendar-quality-v1/` 完成2月三类按日预检；整月3月三类查询超时。拆分后的 `phase1-calendar-quality-by-type-v1/` 中AS与泄漏查询成功，整月前缀仍超时；两次失败回执保留，未提高15秒语句超时反复硬跑。

独立会话 **Domeye 三月前缀与时间异常只读核验**（`01a08bb9-cf07-7b32-b8b3-214c7067acde`）已完成并停止。它使用完整整数源键与既有索引、按日LATERAL候选查询，不按候选时间或ASN截断；先看EXPLAIN和小日，再查较大日期。35次只读查询含元数据、31日统计及时间异常取证；事务各自闭合回滚，单语句15秒、锁2秒、进程45秒、输出4MiB、事务内work_mem32MiB。最大单日03-30查询回执约7.86秒；只说明这次字段预检耗时，不是完整明细留存或页面性能上限。附带HTML报告仅通过结构验证，未做浏览器视觉验收；数据判断依据为下列原始证据与复算结果。

结果位于 `/Users/botongwu/Documents/Codex/2026-09-10/domeye-march-quality-audit/outputs/` 的 `复核摘要.json`、`逐日质量明细.csv`、`三类逐日对照.csv`、`查询回执索引.csv`、`evidence/queries/`及`evidence/runs/`。主会话已复读SQL、汇总、最大日回执与12条时间异常原值，未重复查询。3月前缀共 **1,064,032** 条全部唯一候选；三类3月合计 **1,119,819** 条，93个日期／类型数量与原盘点一致，03-31也与已留存完整诊断一致。

| 问题 | 事实与处理边界 |
|---|---|
| AS等级冲突 | 3月AS共16,041条，其中 **1,058条（6.60%）**等级不一致，分布03-06—03-31共26天；默认日14条包含其中。只是总表／明细冲突，不自动裁定哪边正确 |
| 时间顺序异常 | **03-03的3条AS中断及9条前缀中断**，两表都记录开始为03-03、结束为03-02，持续时间为对应负值；不是总表/明细不一致，也不是汇总误判。不得交换起止时间、夹成零或宣称已恢复 |
| 前缀其他预检 | 缺/多候选、共同字段差异、未识别等级、引用ID格式问题均为0；结束未记录 **13,366** 条。时间顺序异常9条仍保留，不因其他检查通过就称全部前缀合格 |
| 2月早期空日 | 02-01—02-23成功SQL查询无这三类记录；尚未形成成功空日消费包。不表示观察连续、没有异常或网络正常 |

AS时间原值例：`as_outage/2026-03-03 10:33:13/200436/5/r` 开始 `03-03 10:33:13`、结束 `03-02 09:41:46`、持续秒数 **-89,487**；另外两条为AS61008与AS51897。12条完整源键和原值在 `as-time-0303/result.jsonl` 与 `prefix-time-0303/result.jsonl`。这指出下一步准入检查必须包含**字段自身时间一致性**，仅核对两张表相等还不够；本轮未修源库、未实施时间解释修正或默认日冲突规则。

摘要文件SHA256 `42c7ebfc40948ba4b628cd13f125c6e2311891260b6e83ced3b70ee760b9c7ed`；三月notebook `三月源字段质量核验.ipynb` 已执行，摘要 `075ea1b55f54a7873ae46090125efa38a8122a48caafe29cde8739aa286fcb11`。AS／前缀时间证据摘要分别为 `353c2d49370f43a1feb7107a9c9a879b4553c62ad392c86adb584667e0d1b2a8`、`e3473770a940ae685c017f86512234972b6ee26c950924a52773a4462815e367`。源字段预检通过不代表路径等完整业务字段已留存、观察覆盖已确认或首页已接入3月。

### 15.4 独立会话完成旧两日恢复复读

会话 **Domeye 已验收输入独立恢复核验**（`01a08bb5-e55b-7382-b9d7-7a6074a9b1b5`）已完成并停止。交付目录 `/Users/botongwu/Documents/Codex/2026-09-10/domeye-recovery-audit/outputs/恢复核验-20260910T142854Z/` 含中文 `恢复检查结果.md`、`复制清单.json`、`重跑恢复核验.py`和新运行回执。主会话已复读报告、关键回执并重新核对其文件摘要。

- 48份直接复制与17份归档成员，共65份、137,115,289字节，均非符号链接；复制前后源／目的摘要相等。实际运行时操作系统拒绝读取原工作树、网络访问及写恢复副本，不仅更换环境变量；故障演练仅作用于另建测试副本。
- **02-27／28的10,271条全部记录**从副本复读、重新转换字节一致；原SQLite全部payload与查询列一致。原副本及现存源码重建分别进行764次列表／筛选服务调用、192桶核对；原副本逐条调用10,271次详情服务函数。不把这些离线服务调用称HTTP或浏览器验收。
- 结果支持**同机独立副本的语义恢复**。旧目录绑定的索引器摘要`497c6e…`对应完整历史源码未在指定材料中找到，重建SQLite及目录字节不同；没有改写旧版本来伪造字节再生产。现存完整源码`ec7943…`与本轮新五日索引绑定相符。
- 恢复范围只有原两日，不包含本轮新增三日；不是异机备份、介质独立、原库全备或整套前后端环境迁移。保管责任、期限及异机恢复等仍未验收，不能由本切片通过替代完整交付。

复制清单摘要 `7959b74ce34f32806d2e33b1cc38fdd4bccffe37418f125113fc5a0849cdee70`；复跑脚本摘要 `28cc4b112b2c971d3f14f6789d34f38dc1e175f8f11c7a838e0434e0a4e4213d`；本次结果摘要 `8893cd517740e10a1e53a108b540f8439c6b5f289a36bc4d368b7bbe48c514fc`。两份独立会话通过明确数据范围和各自输出目录交接，不共享产品写入；此前未取得执行ID的默认日创建请求不作为已执行证据。

### 15.5 下一优先项

日期目标仍进行中：补已查证空日的安全留存路径和失败诊断状态；继续逐日完整留存3月无冲突数据，明确拦截自身时间矛盾，不静默抹去问题行。默认日及其他等级冲突的消费口径仍待用户确认；不重复询问可查的数据事实。大日明细的离线大小／内存边界须实际验证，不能将源字段SQL预检速度当作全路径制品可承载性。之后仍需逐类异常、可见规模、有限普通路由变化及与最终实际交付范围匹配的恢复验证；**整体goal未完成，未缩小终点**。

## 16．成功空日准入：二月28天可以查询

### 16.1 当前结果及解释范围

本地首页新增 **02-01—02-23 共23个成功空日**，与原24—28日组成二月28天目录。总记录数仍为 **28,505**，没有生成虚构异常；零仅表示本次选定的前缀中断、AS中断、路由泄漏三类源记录为空，不证明原始观测连续、没有真实异常或网络正常。可见规模与普通路由变化仍未知。

23日分别运行完整日窗的 `REPEATABLE READ READ ONLY` 查询并回滚，读时范围为2026-09-10 **15:01:13.275889—15:02:13.701904 UTC**。各日原始文件依次含上下文、独立三类源计数、独立前缀小时桶、结束回执；均非抽样、三类计数0、无明细行、无桶行。它们不是一个共同历史事务快照。源码、SQL、回执、空转换文件及审计位于 `.local/core-overview-validation/phase1-20260201-v1/` 至 `phase1-20260223-v1/`，未改写之前的源库或制品。

### 16.2 最小实现、测试与审查

只修改离线 `scripts/core_overview/retain-core-overview-input.py` 并新增 `backend/tests/core_overview/test_core_overview_empty_days.py`，复用既有索引、HTTP和页面空态，不修改API、OpenAPI、类型或前端设计。空结果额外要求：完整业务日、同源且闭合的读取回执、非抽样、三类源计数与审计桶汇总严格为整数零，以及非空来源实例。清单额外绑定审计与回执摘要；条件不满足时创建输出目录前拒绝。

按Matt TDD的既有离线CLI／只读HTTP边界先红后绿。真实执行发现Python3.10对PostgreSQL五位小数读取时间的兼容问题，新增回归测试后改用兼容解析，不改原始时间字符串。**Standards**复核无硬问题；测试重复构造建议经复核撤回，保留显式fixture。**Spec**指出“审计桶汇总非零仍放行”“空来源实例仍放行”两项P2，均补拒绝用例及证据摘要断言后修复，由原审查者复核关闭。审查范围固定为本轮修改前副本与新增测试，不混入整个dirty tree。

`make test`：后端 **291**、前端 **135**通过，5项既有警告；`make build`通过。相关空日及原接口测试共61项通过，其中新增17项。测试不访问真实数据库。留存器摘要 `495382f527afdb34bbc723d3d4c778d3921a51b117a5010b861e2049e9d20e49`，新增测试摘要 `dd3b81ce8536097a7447c02ff2adf19252c3c5e40495ca2de10f566864db49ba`。

### 16.3 实际消费版本与复读证据

当前23份空日包使用 `.local/core-overview-inputs/rrc25-202602DD-v3/`（DD为01—23）。新目录为 **`rrc25-february-v2`**，消费版本 **`overview_index_v1_cc39dff334a4bf05108638a099087480c52cea572afac5c05367cf36b748f1f5`**。原五日目录项、SQLite字节及原输入版本逐一核对不变。只切换本任务已确认归属的隔离后端28491；前端28492及原型5179不重启、不部署。中间候选 `02-01-v1`、空日v2及 `rrc25-february-v1` 保留但未用于实际页面，最终包重新绑定修复后的留存器。

本轮可复读证据集中在 `.local/core-overview-validation/empty-days-implementation-v1/`：

| 证据 | 已验证内容 | SHA256 |
|---|---|---|
| `source-proof.json` | 23日原文／查询／回执／审计／消费摘要一致，原五日字节不变 | `162189a859441b89acb84470a98ec78a0c348600d9239362e50562688a65a377` |
| `http-proof.json` | **331次真实HTTP**：299种空日查询组合各核对24桶值与时间标签，23次空日详情404，原五日计数、默认及缺日空指标、旧版本409 | `9fc646a78701bc05e516354ce70e533440bc4acd2c89462f8f9b58887dc097c8` |
| `二月成功空日准入核验.ipynb` | 从头执行4个代码单元，独立复读来源并核对已执行HTTP结果；不重新查询源库或Web | `8a16db55e259342462d680737d9074137266b7e53e44a4eb097256d986a22f8c` |

`verify_empty_days.py`摘要为 `87ff9f58bd00638e815801f34669c45a5eaa2573d355ecc21f5a369e6a4417c6`。实际浏览器确认新目录28天、02-01与02-23为空、IPv6／小时筛选仍为空、03-31不可用不补零、02-24恢复3,439条，最后返回02-26。过程及视觉范围见 `浏览器验收.md`；本轮没有重新逐条HTTP验收原五日明细，也未作完整宽屏／移动端布局回归。

### 16.4 下一步：三月合格输入准入与失败状态

新会话 **Domeye 三月四日完整输入核验**（`01a08bd2-c27c-7b52-9afd-618176c1dd1b`）已完成并停止，产出03-01、02、04、05日完整导出，分别2,566／3,672／1,584／1,785条，合计 **9,607**条。输出在 `/Users/botongwu/Documents/Codex/2026-09-10/domeye-march-four-days-admission/outputs/`。其按日审计及时间复核报告通过、notebook执行回执成功；本阶段结束时主会话已读取这些报告及 `交接.md`，尚未完成对其SQL／检查代码及全部原文的独立准入复核，未运行留存／索引命令或接入首页，不能称三月已经可查。继续验收请使用 `四日完整业务字段离线核验_校正版.ipynb`；原版有字段列数文字错误，校正版已重新执行，原文件未删除。交接摘要为 `db9e762d75e9ba05c379fc22de4766919d3e7e0f2f30e11a66f0570ff09f6537`。后续准入结果见第17节。

下一项先完成四日主验收及不连续日期的实际页面接入，再补源质量失败的诊断状态与自身时间矛盾准入门槛。默认日及其他等级冲突规则继续待用户确认；不会将自动续跑当成接受。后续三类异常、真实可见规模、有限路由变化和最终范围的恢复验收均未完成，整体goal保持进行中。

## 17．三月四日准入：32日真实查询目录

### 17.1 当前可以查询什么

2026-09-10主会话完成03-01、02、04、05四日独立复核、消费留存、索引和实际页面接入；当前共 **32个业务日、38,112条**。沿用前缀中断、AS中断、路由泄漏三类解释；不包括其他异常类型、可见规模或普通路由变化。所有“日期”仍为Asia/Shanghai业务日。

| 三月业务日 | 前缀中断 | AS中断 | 路由泄漏 | 合计 | 小时去重前缀合计／全日去重前缀 |
|---|---:|---:|---:|---:|---:|
| 03-01 | 587 | 104 | 1,875 | 2,566 | 410／174 |
| 03-02 | 872 | 174 | 2,626 | 3,672 | 477／352 |
| 03-04 | 415 | 101 | 1,068 | 1,584 | 200／148 |
| 03-05 | 313 | 63 | 1,409 | 1,785 | 308／220 |

四日9,607条全部唯一关联，逐条保留原字段；独立SQL计数、地址族及163个实际存在的小时／地址族桶与离线结果相符。小时去重不能跨小时相加成全日对象数。434条前缀中断、33条AS中断结束缺失，仍不推为持续中；泄漏源不提供结束信息。独立整数微秒检查通过2,162条具备结束及持续时长的记录，核对总表与明细两侧共4,324次算术关系；当前四日无自身时间矛盾。只证明这些已存记录，不证明历史检测正确率或完整观察覆盖。

四日各自完整只读事务读取发生于 **15:02:24.91154—15:07:29.190086 UTC**，不是共同源库快照。主会话本轮未重查数据库：全文审读SQL、采集／审计／时间检查代码，复读89个冻结文件及全部原文，确认四份SQL除日期外一致、事务只读且回滚回执闭合；失败与冲突记录未混入这些日窗。

### 17.2 兼容修复与旧版本保持

复核发现原冻结转换器在项目Python3.10.20不能解析五位小数 `read_at`。冻结检查器用其可运行的Python3.12.14复读；当前转换器只对**元数据时间的解析副本**补位，不回写原字符串，不修改业务时间、等级或结束解释。Matt TDD实际先4失败／2通过，修复后1—6位小数6例均通过；相关文件58例、全后端297例和前端135例通过，5项既有警告，构建通过。

产品只改 `backend/data_pipeline/common/event_records.py` 的元数据解析与对应测试；API、合同、前端和留存／索引算法本轮未改。原冻结转换器摘要仍为 `d076beee9e22ffa325e0b5646f5cf0f2166a5c03258290bd5206289a2eda394d`，当前修复版为 `3115180e859a4e29f7b75e890986ebf33fb56d064096ef156c8e6fb9566da035`。全部38,112条用当前转换器重新转换，结果及序列化字节一致；未改原审计的旧源码绑定或已引用内容版本。

Matt code-review按本轮改前保留副本双轴审查，不把整个dirty tree算作本切片。**Standards：硬问题0、可选smell0。Spec：缺失、超范围及错实现均无发现。** 主验收另保留实际HTTP和浏览器结果，不将静态审查替代运行验收。

### 17.3 消费版本与运行证据

四日审计副本分别在 `.local/core-overview-validation/phase1-202603DD-v1/`，消费包为 `.local/core-overview-inputs/rrc25-202603DD-v1/`（DD=01、02、04、05）。新32日目录 **`rrc25-february-march-four-v1`**，消费版本 **`overview_index_v1_d962d3ee1333811c0f5f4c0503fb6d4d02e8933e4f75f9d2f2ad6affb232ff22`**。二月28个目录项、SQLite字节及单日输入版本与 `rrc25-february-v2` 逐一一致；旧目录保留。

只切换本任务隔离后端28491的新显式清单，前端28492沿用，原型5179及共享服务未动。本轮证据入口为 `.local/core-overview-validation/march-four-days-admission-v1/`：

| 证据 | 核验范围 | SHA256 |
|---|---|---|
| `parent-proof.json` | 89个冻结文件、四日原文与当前3.10逐条转换一致 | `eacad6f153a5f3396846cd4f0340f1b3f05955566b14b2f31e8cb05102ca3247` |
| `catalog-proof.json` | 32日目录／输入绑定、旧28日字节及全部38,112条兼容复算 | `1ab11651aeddd7180c634a0de18fd2d2b2e883ac5d813af31312374af8bafe8e` |
| `old-days-http-proof.json` | 34次HTTP：旧28日计数、默认／缺日null、旧版本列表／详情409 | `16a0675c5e3df16605c07e6204b2a80c3a69e521a775d959641070613123895e` |
| `三月四日首页准入核验.ipynb` | 从头执行4个代码单元，重做文件复算并复读已执行HTTP证据 | `1f6c6460d3ebadc2659ee7dd0f7346e073ce90581e4cb9fdd370a060512c30a7` |

另在 `.local/core-overview-runtime/` 保留 `http-march-four-days-audit-2026-03-DD.json` 四份及 `http-march-four-filter-audit.json`：四日446次HTTP覆盖9,607条完整列表字段、288个小时／地址族组合及41次分层详情；320次补充HTTP覆盖全部排序分页、等级／对象筛选和桶标签。与上表34次合计 **800次**；详情HTTP按类型、地址族、结束状态及跨日抽样，不宣称9,607次全部详情HTTP检查。

实际浏览器逐一确认四日计数、同版本跨日详情、IPv6／小时／类型联动、空日与默认日切换；局部筛选为空时上方概况保持不变。最后停在03-05全部地址族／全部三类，1,785条。视觉与操作记录见同目录 `浏览器验收.md`，不是完整多视口布局回归。可复跑文件清单及审查结论见 `执行与审查.md`。本轮不是独立副本恢复，不能扩展第15.4节两日恢复证据的适用范围。

### 17.4 剩余缺口与下一切片

03-03的12条自身时间矛盾及03-06—31的等级冲突仍未准入；默认03-31不变，源问题已查明但页面仍仅显示消费日未留存。下一项应将已知失败原因与证据版本接入日期诊断，同时在离线准入处明确拦截自身时间矛盾，不修源值、不删除行、不改变等级。等级冲突采用“待核实”展示的建议仍未获确认，不能因继续goal自动通过。

独立会话 **Domeye 失败日期诊断接口最小方案**（`01a08be8-833c-79b0-aaaa-90e01402c30c`）已提交候选方案并停止，文件 `/Users/botongwu/Documents/Codex/2026-09-10/domeye-failed-date-interface/outputs/首页失败日期诊断候选方案.md`。主会话已阅读，尚未实施或冻结其v2接口；不把方案完成视为失败状态已经可用。后续按优先级推进其他异常类型、真实可见规模、有限路由变化及与最终范围匹配的恢复交付，整体goal保持进行中。

## 18．失败日期诊断及原始时间准入

### 18.1 当前可以做什么

2026-09-11（业务协作时间Asia/Shanghai），完成失败日诊断切片。实际首页的日期目录现在分列 **32个可消费日、27个失败诊断日**；原38,112条消费记录不变。这解决“已经查明失败、页面却只说未留存”的问题，不等于59日数据全部准入。

| 日期 | 显示的源问题 | 当前消费状态 |
|---|---|---|
| 03-03 | AS中断3条、前缀中断9条：结束早于开始或持续时长为负 | 整日503，概况／趋势／列表null；原时间不修正 |
| 03-06—31 | AS总表与明细等级冲突，共1,058条；其中03-31为14条 | 整日503，不选择其中一方等级、不展示冲突事件详情 |
| 原32日 | 二月28日与三月1、2、4、5日 | 仍可查；23个有完整空结果证据的日期可显示零 |

默认日期仍由配置取03-31。失败日的“核验依据与版本”展示源实例、源查询窗及读取时点、原文／回执／SQL／选择清单摘要、单日诊断版本及消费目录版本。明确“源字段预检，不是该日完整准入”；原因条数不是实际用户影响，不能跨原因直接相加。地址族、类型、时间筛选不能绕过整日失败；诊断文件损坏则不显示不可验证的条数。未留存与成功无匹配的语义仍保留。

### 18.2 来源与准入门槛

未重查数据库。复用三月AS逐日质量预检（09-10 **14:26:58.233983—14:26:58.503404 UTC**）及03-03前缀预检（**14:34:46.639731—14:34:46.745017 UTC**）；原始读取不是共同事务。两份结果、SQL及回执的字节副本见 `.local/core-overview-validation/failed-dates-implementation-v1/evidence/`，复制证明为相邻 `evidence-copy-proof.json`。6个源文件摘要一致，AS1,058条及时间3条、前缀时间9条从原结果独立复算；03-31的14条另与第14节全日审计一致。这里只读取字段预检，不冒称已对全部27日做完整导出准入。

离线编译模块只支持两种已核验格式，严格检查原文／SQL／回执／来源／数据档／窗口绑定、完整日历、整数计数及限定失败原因。SQL资产只做字节核验，不执行查询。AS旧格式不含独立rollback_ack行，不为它补造该证据；前缀格式按实际成功及回滚确认检查。底层来源仍为可写Overlay，摘要不是防篡改签名、不可变快照或覆盖证明。

新增原始时间门槛用于留存命令及索引输入：结束早于开始、负持续时长、已知起止与时长不一致、无法解释格式拒绝准入；缺结束或缺时长仍保留未知。对03-03原字段12条共24侧实测拒绝；旧32日38,112条全部通过。未重写已有记录、结束状态、内容ID或等级，也未修改检测代码。

### 18.3 版本、验证与本地运行

当前目录 **`.local/core-overview-inputs/rrc25-dates-diagnostics-v2/manifest.json`**，模式 `core-overview-index/v2`，消费版本 **`overview_index_v2_4d95bf9475a78654d179f726e5980ccdfab8443da805c3ee66a959d9a6150416`**。32日原目录项及SQLite字节与第17节原32日目录逐一相同。新增27个按日诊断JSON，每份有独立摘要版本，Web只读选定日，单份上限64KiB；原查询原文不上Web请求链。中间候选 `rrc25-dates-diagnostics-v1` 未用于运行、保留不覆盖；旧v1消费目录仍可读。

重建选择为README列出的32个原单日包，另加可重复参数 `--diagnostic .local/core-overview-validation/failed-dates-implementation-v1/evidence/as-quality/selection.json` 和对应 `prefix-quality/selection.json`；`--output`必须使用新目录。新目录已存在时勿重复执行或覆盖。仅本任务隔离后端28491切换清单，前端28492沿用；没有数据库、INFO绑定或共享服务变更。最新启动说明见README。

本轮证据根为 `.local/core-overview-validation/failed-dates-implementation-v1/`：

| 文件 | 实际核验范围 | SHA256 |
|---|---|---|
| `source-proof.json` | 原预检／32日字节与38,112条时间门槛／12条矛盾拦截／03-31独立审计交叉检查 | `35fd3910fd46d76ca3da010c3ee96db2dadf648142cdd7eeed2a05372b81a190` |
| `http-proof.json` | **295次真实HTTP**：32日各4组列表／全部小时桶、27次合格详情、27日各4组失败查询及详情拒绝、默认／版本／参数边界 | `445ea49309d5a6e3b7d4cc64baed144316369e93ab0f87187fc989c9564b40e7` |
| `失败日期诊断与时间准入核验.ipynb` | 4个代码单元从头执行，绑定实现摘要、重做来源核验并复读已执行HTTP证据 | `b77b707c8e7d1ee138c0bc9bd08e626468f2186f85657fb66763de1d80c70f78` |

Matt TDD公开行为测试先红后绿；`make test`后端326、前端136通过（后端1项既有警告），`make build`与合同生成类型核对通过。Matt双轴审查：**Standards硬问题0、可选日窗校验重复建议1；Spec问题0**，非阻断整理本轮未做。真实浏览器验证默认失败说明、双原因日、空日、非空日／IPv6详情、旧版本409明确重读及正常清除旧目录。顶部页面与诊断弹窗视觉已看，不外推完整多视口回归；详见 `浏览器验收.md`、`执行与审查.md`。

### 18.4 未解决什么

问题源值没有修复；等级冲突“待核实”展示仍待用户确认，自动续跑不是同意。完整2—3月消费覆盖仍未完成。后续先推进不依赖该决策的前缀劫持输入核验，再按goal顺序考虑子前缀劫持、国家中断、真实可见规模与有限路由变化。最终范围的独立恢复及旧导航验收仍待完成；本轮不是灾备恢复、生产部署或整体goal完成。

已按用户允许拆分会话的安排，创建有界只读取证会话 `01a08c1f-d7f8-7722-a877-eb76a9dbd234`，目标仅为前缀劫持真实输入准入核验。输出目录为 `/Users/botongwu/Documents/Codex/2026-09-11/domeye-hijack-admission-audit/outputs`，不得改主工作树或首页代码；主会话收到交接后仍需独立复核。当前仅已启动，不宣称该类型已核验或接入。

## 19．普通前缀劫持取证与离线映射

记录日期：2026-09-11。本节续接第18节的取证会话：**该会话现已完成并停止，主会话独立复核后完成 hijack 离线字段映射；首页接入仍未完成。** 本轮不改源库、检测规则、共享服务或运行清单，不开始子前缀劫持和国家中断。

### 19.1 查到什么

来源仍为 `10.99.8.16` 的 `domeye_core_dev_pg/bgp_project`、可写Overlay；再次读取的PG系统标识为 `7663836852697006116`、数据库OID16384，容器及state.json与既有绑定一致。独立会话将读取地址写成 `ssh://root@10.99.8.16/docker/domeye_core_dev_pg/bgp_project`，本轮11条离线结果原样保留这一来源字符串；**没有改写旧32日的来源命名空间**。以后与旧输入组合前须显式核对来源绑定，不能仅删前缀当作自动相同。

查询限定二至三月事件总表及普通hijack明细，另查实际列和主键。没有用子前缀劫持充当正例，没有读取旧代码／环境或重跑检测。5次只读事务各自保存SQL原文、结果、完整回执与回滚确认；单语句15秒、锁等待2秒。交易时间和源记录业务时间分开，几次读取不包装为同一源快照。

| 范围 | 总表行数 | 按自身开始时间计数的明细行数 | 本轮可说明什么 |
|---|---:|---:|---|
| 二月 | 14 | 14 | 月份／逐日盘点；02-24为2、02-26为1、02-28为11 |
| 三月 | 11,301 | 11,300 | 月份／逐日盘点，差额定位到03-20；不是全月准入 |
| 02-28完整正样本日 | 11 | 11 | 总表25字段、明细27字段全部留存，引用及完整键各11个，候选均为1 |

实际明细主键是 `(source,prefix,hijack_eventid)`，不含开始时间或ASN。取齐整张对应月表的完整键候选后，独立比较引用／总表／明细时间。**03-20一条引用开始为14:11:03、Prefix为80.244.11.0/24、编号1，而同键明细开始于03-04 19:35:43**；这解释了日计数差额，不是候选缺失。未查明成因、未选取一侧为真、未修源。主会话用这份真实键／时间预检验证转换器返回conflict；没有把部分字段预检称为完整事件准入。

02-28的11条都是IPv4，8条low、3条middle。原角色均为 `hijacked_as=34837`、`hijacker_as=44889`；它们只是源角色。`is_hijack=true`同时有`filter_reason="possible hijack"`，不能省略后者后称为确认攻击。结束、持续时长、end_as及next路径全部null；真实非空起止时长三元组检查为**0组**，不能宣称已验证恢复语义。前／事中路径各451个字符串，不是Peer数，时间键按原值保留。

双侧source、Prefix、时间、等级、组织和国家一致；两个AS展示文本和event_info各11条原字符串不同：本日可精确复构展示格式，但不是全局规范。两表原文分别保留，不覆盖或合并。`r→rrc25`仍依据用户确认，覆盖unknown、历史检测版本null；不会补造检测正确性、历史归属或责任结论。完整取证交接为上述独立会话输出目录的 `前缀劫持准入核验.md`，最终摘要 `e45e6d21c1b2f7aa30ad86b910ccf872bda4dc53d75d8dde88b2ad9925993831`。

### 19.2 实现与证据

`anomaly_records.py`仅在现有类型表中新增hijack字段声明，复用原有完整候选检查、时间解释、未知状态和序列化；保存全部27个明细源字段。没有新增IO、数据库访问或通用模型框架。首次支持的hijack使用既有记录编码与映射版本；原三类的规则不变，实际新旧代码摘要分别绑定在本轮证据中，未对旧记录全量换号或冒称历史检测版本已知。

本轮Git外根为 `.local/core-overview-validation/hijack-mapping-v1/`，含改动前副本、固定新映射、配置、源证据副本、主会话独立复核脚本、11条转换结果和已执行notebook。转换结果不是首页消费包或Publication；没有伪造三类消费清单包住hijack。

| 证据 | 核验／用途 | SHA256 |
|---|---|---|
| `source-evidence/03-day-20260228/result.jsonl` | 完整单日源原文 | `ed6e63dae5296ef3833dea1092967e7e4e4ec76829debb9f1b36b1c44c09859f` |
| 对应 `executed.sql` | 完整键候选及独立计数／字段对账 | `9f8014ddb4998139b1409008b177eb0693bc04bcefdb94ad04c5f07d2bc5f5a6` |
| `anomaly_records_source.py` | 固定本轮离线映射 | `38786c7db41e21dc67af85a8a69949b963646df9a33811533d5987e7a1e8c671` |
| `retained/records.jsonl` | 11条转换／序列化结果 | `abc369e2a4a28fbe2a6663d7c8265b4d40e761ac6741c428a0c4641b2332babd` |
| `retained/proof.json` | 主会话原文复核、真实冲突拦截及旧38,112条重算 | `98e6b9f876bd5bc6e6fad38b4d0259b0ec19b376c0d73ca91538a0bf8016b2e7` |
| `前缀劫持映射与旧版本复读.ipynb` | 3个代码单元从头执行，重做独立复核并比较留存结果 | `866c164007ab1bcdac6ca86f9bf467b4e463de3857386d487816005cba946706` |

`verify_mapping.py`不访问源库，复算所有源回执、11条字段、月计数和冲突；逐条用固定旧／新映射重新转换32日的38,112条原记录，序列化字节与既有SQLite payload全部相同，32份索引摘要也保持一致。先写hijack正例测试，实际失败为类型不支持；增加字段映射后通过，再补完整键／时间冲突、未知标志／空路径、IPv6合成边界回归。`make backend-test`为**337通过，1项既有pandas警告**。合成测试不替代真实IPv6或已结束事件准入。

### 19.3 当前交付边界与下一步

在线仍为第18节目录版本，02-28仍是8,236条三类记录。主会话实际HTTP复查该日结果和版本不变，`kind=hijack`仍400“该异常类型尚未接入”，默认失败日仍503且消费指标为null；没有为本轮做UI修改、重启或浏览器验收，也不把离线测试通过说成页面可用。双轴审查已完成：**Standards问题0、Spec问题0**；准确比较本轮改动前副本，不把既存工作树改动当作本轮。严格Matt模板缺少issue-tracker配置，已告知并以已有goal／短规格做替代审查，未新增流程文件。详见本轮Git外 `执行与审查.md`，HTTP结果见 `runtime-proof.json`。

下一切片继续普通hijack：现有32个可消费日的月盘点共17条，其中11条已核验，其余6条还需完整取证；其余日的零值也需要闭合四类型消费证据，不能仅沿用三类空日声明。完成这组日期／类型准入后再生成新消费版本，验收筛选、详情、时间与地址族、旧记录和中断趋势不变及实际页面。不能用时间或类型筛选绕过既有27个失败日。

03-20的时间冲突、未验证IPv6／已结束样本和其他失败日保留明确边界；等级冲突展示规则仍待用户决策。整个goal尚未完成，当前不跳到下一异常类型、不实施全量状态重建、不部署。

已启动新的独立只读取证会话 `01a08e32-c683-7ee1-a8a0-14c715c899b4`，仅补齐上述32日hijack输入与零日证据，输出 `/Users/botongwu/Documents/Codex/2026-09-11/domeye-hijack-32day-admission/outputs`。主会话确认其状态为active／inProgress，尚未收到交接；不会因观察等待超时重复启动。该任务不写主工作树，不自称完成整体goal。

## 20．普通前缀劫持四类型消费验收

2026-09-11补充：第19节的只读取证任务现已完成并停止，主会话复核其完整SQL、原文、回执和独立核验脚本后，完成普通hijack的有限消费接入。**当前C首页支持四类留存异常；可查询31日、36,544条，另有28个失败日。** 这不是完整2—3月数据准入，整体goal继续。

### 20.1 准入日期和冲突

取证范围为原32个可消费日：02-01—28及03-01、02、04、05。同一只读可重复读事务取得17条总表原文、17条完整月表键候选及17条按明细自身开始日独立提取的原文。无候选时间／ASN／等级／is_hijack过滤，无LIMIT；32日均有独立计数，其中26日明确为零。主会话执行冻结核验脚本，与原审计摘要完全一致。

| 分类 | 日期及记录 | 本次消费判断 |
|---|---|---|
| GO日有hijack | 02-24：2；02-26：1；02-28：11；03-02：1；03-05：1 | 合计16条，保留完整键、27个明细字段及两表原文 |
| GO日无hijack | 其余26个选定日 | 有本次独立零计数，不从旧三类空文件推定 |
| REPAIR | 03-04：1条 | 新四类型版本整日失败，不通过筛选返回部分成功 |

03-04引用 `hijack/2026-03-04 19:35:43/80.244.11.0-24/1/r`，表 `hijack_202603`，键 `(r,80.244.11.0/24,1)`。总表结束03-04 19:49:46、时长843秒；唯一候选明细结束03-20 18:29:00、时长15,477秒，但明细自身起止差1,378,397秒。双侧及自身均矛盾，成因未知，不能择一侧或回写。新版本03-04不提供指标／列表／详情，旧三类版本该日1,584条完整保留。总数变化明确为 **38,112−1,584＋16＝36,544**，不是删除记录或将失败类型补零。

02-24两条已结束正例的双侧时长分别为3天19:11:31和3天19:10:31，算术一致，next路径保留时间键及明确空数组。其余GO日14条结束／时长未知，不称持续中。全部17条均为IPv4、is_hijack=true且filter_reason="possible hijack"；这不是独立确认攻击或责任，真实IPv6及false/null标志正例仍不足。跨月同键 `(r,87.107.105.0/24,1)` 保留各月表定位，不合并成一条。

### 20.2 来源与版本

源实例仍为 `10.99.8.16/docker/domeye_core_dev_pg/bgp_project`，source=r→rrc25来自用户确认；覆盖unknown、旧检测版本null。读取前后容器、PG系统标识`7663836852697006116`、OID16384、state摘要和可写Overlay绑定一致。业务事务UTC **2026-09-11T02:06:14.013732+00:00—02:06:24.201747+00:00**，只读、可重复读、回滚闭合。输出243,602字节／90行，实际流式上限4MiB；独立收口查询确认本任务遗留数据库会话0。第一次BusyBox参数启动失败在psql前发生，原文为空，失败回执也保留，不冒用成功。

原三类与本次hijack来自**不同读取时间**。组合包绑定双方清单／原文／SQL／回执和解释代码，不能称为一个数据库快照；record.read_scope用于选定消费日，实际hijack查询的32日另由SQL、scope和诊断`queried_dates`明确。外包窗口跨过未查询的03-03，不表示连续查询。旧来源字符串及旧已引用内容版本不改写。

当前输入 `.local/core-overview-inputs/rrc25-hijack-v3/index/manifest.json`，目录版本：

`overview_index_v2_71e46fe7b62c6fbd3f613974eb1a3260be248d49834e7f9145450c7a2cdcd667`

可消费日为02-01—28及03-01、02、05；失败日为03-03、04及03-06—31。03-31仍是配置默认日，显示14条AS等级冲突，不暗换日期。原32日三类目录和索引保持原摘要；新包另保留其同磁盘副本，不能据此宣称异地灾备或完整恢复演练通过。

本阶段证据根 `.local/core-overview-validation/hijack-consumption-v1/`：

| 文件／入口 | 作用 | SHA256 |
|---|---|---|
| `source-evidence/evidence/read-32days-v2/result.jsonl` | 全32日原文、计数和候选 | `1aa03fedfca4fe24136c293ca73afbe8004dfaca4510599921c925d9b30a9c25` |
| 对应`executed.sql` | 精确日期与完整键查询 | `0ae020c6e6ecd0bd429a32cd253d6c1360122909e2f5f86681eb7fea165854c7` |
| `anomaly_records_source.py` | 冻结解释；与第19节一致 | `38786c7db41e21dc67af85a8a69949b963646df9a33811533d5987e7a1e8c671` |
| `build_consumption.py` | 保留旧记录、组合日包及索引 | `73cf12ff27d138fdb63cef76fc72b6ce4a7724eaf2ab7b40cb3e029c7081b4ef` |
| `api-live-v3-proof.json` | 518次实际前端代理HTTP核验 | `3b428f5558aacf839896d91ec20373b3f6f7e0dde3bcdb81d371462989639e21` |
| `前缀劫持四类型消费复核.ipynb` | 3个代码单元完整执行，重新复算原文与518次接口行为 | `28a5cc00650275253f2ebd51b4e9634bfabc0be1049145a0c54696f702c056a7` |

`build-proof.json`位于新输入根，摘要`5e4064a0c3d01bf3e9c8b4b8062ef0babde9ec5d8c489db66815e170bc327de3`；本摘要属于v3实际结果，v2候选另保留自己的回执。新诊断编译器摘要为`c922e36e028d277fe25089498e826491ea83698057abf33617bcf2f7db797fb4`。所有摘要仅作完整性绑定，不是签名或不可变存储。

### 20.3 实施、复核及剩余项

复用既有留存格式、按日索引和公开接口，新增四类型声明、hijack两角色数字ASN筛选；列表局部筛选不影响上方统计或前缀中断趋势。日期／类型选项来自实际目录。新增时间冲突诊断仅从完整核验原文派生；可解析的算术矛盾与无法解析分开，后者拒绝生成诊断，不伪造已证实冲突条数。Web仍只读本地文件，不访问源库、不生产索引。

固定阶段基线在`baseline/`；Matt双轴初审发现Standards 1项硬标准＋2项重复代码判断、Spec 2项问题，均通过公开CLI反例、修正及独立复审闭合。最终**Standards残留0，Spec残留0**。严格模板缺issue-tracker，仍以既有goal／短规格和阶段文件快照有限回退，未新增工作流框架。

`make test`为363项后端、136项前端通过；`make build`和生成类型通过，仍有一项既有pandas警告。真实对账与合成测试分开：31日旧记录逐条(item,payload)一致，共36,528条；旧32份文件摘要不变；31×24小时前缀桶一致；16条完整源字段、角色检索、结束信息、地址族、分页、等级／时间排序及版本冲突通过；28失败日均不能借筛选绕过。实际监听后端28491及前端代理28492完成518次HTTP核验。另一518次公开HTTP离线核对在已执行笔记中复跑，不把它冒充监听服务检查。

浏览器实测02-28十一条分页及未知结束详情、02-24跨日结束日期、02-27空hijack结果、03-04失败提示及确切查询日证据、默认03-31十四条冲突。截图`hijack-*-v3.png`及`default-v3.png`已检查，保留C布局和克制用语。过程中原本地前端进程已退出，经监听和句柄核实后恢复；只替换本任务本地28491输入，未改共享服务、5179原型或用户浏览器页。两个中间候选保留且注明未验收：v1末尾回执脚本错误；v2在审查修订前完成接口核对，不作为最终选择。

**后续按优先级继续子前缀劫持准入。** 国家中断、可见前缀／起源AS规模、普通路由变化、完整恢复与旧导航实际验收仍待完成。03-04需可追溯且一致的新证据才能重准入；03-31及其他AS等级冲突的消费取舍仍待用户决定。本轮不改判级、不检测、不状态重建、不提交／推送／部署，整体goal保持active。

收尾复读补充：`verify_old_copy.py`对新输入根中独立保留的旧三类32日副本完成59份日文件摘要核对及59次公开HTTP复读，仍为32可用日／27失败日／38,112条；未切换监听服务。回执`old-copy-restore-proof.json`保留同磁盘和现有环境限制，不替代完整恢复验收。

已按用户允许拆会话的方式新建独立任务“Domeye 子前缀劫持准入核验”，ID `01a08e68-bcfc-73a3-b3f7-2a110b46fb55`，输出 `/Users/botongwu/Documents/Codex/2026-09-11/domeye-sub-hijack-admission/outputs`。已确认active／inProgress，正核对来源与31日取证范围；只交接证据，不改主工作树、当前输入或页面，不代替本goal验收。下一次先检查该具体任务状态，不因等待超时重新启动。

## 21. 子前缀劫持源字段结构复核（2026-09-11，准入进行中）

本节只记录**已取得原文及主任务的有限独立复核**。独立任务仍在完成完整准入检查与交接；未增加首页类型、修改业务代码或切换消费输入，不表示第五类已交付。

### 21.1 查询范围与证据

读取 `10.99.8.16/docker/domeye_core_dev_pg/bgp_project` 的 `event_table_202602/202603` 和 `sub_hijack_202602/202603`，仅选当前四类型目录已可消费的31日：02-01—28、03-01、02、05。未查询其他28个失败日期，不能外推整个2—3月窗口。

按总表自身开始日读取 `source=r` 且“事件类型为子前缀劫持或引用类型为sub_hijack”的并集；候选按引用月份和完整 `(source,prefix,sub_hijack_eventid)` 查找，不以候选时间筛掉冲突；另按事实自身开始日期独立读取。保留SQL、元数据、每日计数、完整源行、双侧检查及回执。

数据库事务UTC为 `2026-09-11T03:05:17.960153+00:00`—`03:05:25.33248+00:00`，只读、可重复读、快照 `4202:4202:`；回执记录exit=0、正常回滚、读取进程已回收。原文92,221字节／72个JSON行，SQL17,581字节；主任务实际复算摘要与回执一致。历史业务时间仍按项目Asia/Shanghai解释，不宣称历史入库时区已独立证明。

证据目录：`/Users/botongwu/Documents/Codex/2026-09-11/domeye-sub-hijack-admission/outputs/evidence/read-31days/`。

| 文件 | SHA256 |
|---|---|
| `result.jsonl` | `766d92cc0a3ca610231f364d1ffc1ee7f83dd905be3a7050b917e037acc9df06` |
| `executed.sql` | `4b9616208966ba5b634ce30c8a16495651fde76bb18e937637f9ae51b322b5f7` |
| `receipt.json` | `3ad909b7bfa76f43513bb8905d063855e605e17780df40d571eb847b137041f2` |

### 21.2 已复核事实与消费风险

- 共11条记录、11个完整键、11条唯一候选；关联事实与独立读取的事实集合逐条一致。正值日为02-25三条、02-26一条、02-27两条、02-28两条、03-02三条；其余26日分别有总表、候选和事实自身日期的零计数，不借用旧四类型空日声明。
- 总表 `affected_prefix` 是“父前缀＋子前缀”的组合文本，不能直接与明细 `prefix` 比较字符串。11条均与明细父、子字段组成的文本相符，且子前缀严格包含于父前缀；全部IPv4，不证明IPv6真实样本准入。
- 每条明细均有24个字段。`prefix` 是子前缀，`hijacked_prefix` 是父前缀；等级说明字段是 `level_info`。角色ASN为 `['44375']` 形式的文本列表，11条均可有限解析为合法ASN。直接复用普通劫持的单数字解析会漏掉筛选对象；原角色、`is_sub_hijack` 和 `filter_reason` 仍不构成独立确认或责任证据。
- 7条有结束原值，4条结束与时长均为null；未知不能显示为持续中。主任务已核对双侧起止、时长和等级原值一致，但本次有限笔记**不检查时长算术**；该项等待完整准入复核，不以原值一致代替。
- 03-02的 `78.157.33.0/24` 在08:15:42有编号1和2两条记录，须分别保留完整引用，不按对象＋时间擅自去重。现有事件身份原则无需为此改动。

高风险是误用旧字段解释造成错误拒绝、父前缀丢失或角色筛选漏项，并非已证实的源数据冲突。当前转换器及C类型列表不支持sub_hijack，故上述风险尚未作为新消费行为发布。

主任务可重跑笔记：`.local/core-overview-validation/sub-hijack-source-review-v1/子前缀劫持源字段结构复核.ipynb`，3个代码单元从头执行通过；SHA256 `589514cd683750712464b5d9ee644dd5b4da3d64662406ba36b0864e1fccc9b1`。同目录 `create_notebook.py` 保存生成及执行代码，读取上述精确摘要绑定的原文；不连接数据库。结论按数据质量skill回填本台账，不另建报告体系。

### 21.3 下一步与未完成条件

先接收独立任务的完整准入结论并复读，再细化最小消费改动：保留父子字段、原角色及24个明细字段，核验结束语义和适用地址族，沿用完整事件身份；只在证据支持时扩展第五类。仍须完成公开行为测试、旧记录／趋势／失败日期兼容对账、同版本HTTP及实际浏览器验收，之后才称首页已接入。新消费结果另建版本，不覆盖四类型输入。

Matt setup仍在等用户确认issue tracker，未写 `docs/agents/` 或修改 `AGENTS.md`；此状态不妨碍已授权的只读取证，但不能将配置或后续正式双轴审查标记完成。整体goal保持active，国家中断、总体规模、普通变化与完整恢复交付等后续范围不缩减。

### 21.4 本轮后续完整离线复读

独立任务的 `evidence/verify_offline.py` 随后可用；主任务已阅读全文，并以不带写入选项的公开命令 `uv run --no-project python /Users/botongwu/Documents/Codex/2026-09-11/domeye-sub-hijack-admission/outputs/evidence/verify_offline.py` 实际复跑，exit=0。复算涵盖元数据、完整原文、来源前后绑定、独立事实集合、父子关系、角色展示、双侧字段、起止／时长算术及读取回收，结果与已留存审计相同。

结论更新为：**31日sub_hijack有限源输入GO，首页消费仍未验收**；11条中7条完整结束的算术检查通过，其中4条跨日，另外4条结束未知；等级为middle六条、low五条。原28个失败日保持未查询与拦截，不因本次GO改变状态。脚本摘要 `a22af62133aeda5f0da9b75ff2d5368f3faf254949fc210d3746e6d74663e6ce`，审计摘要 `d41c0894d43f9345a1275ed3e38e62518056f25a20293c9789dad615d271da71`；均位于独立任务输出的 `evidence/` 中。上方3单元笔记仍只代表其明确写出的有限结构检查，不冒充本段完整复读。

两个月事实表元数据均无路径列；**不能用null、空路径或普通hijack的路径补造sub_hijack路径证据**。该来源的路径用途STOP，不等于全项目不存在其他可用路径制品。目录查找触发30,000项枚举上限（报告访问30,001项），不是穷尽搜索；未据此声称全项目不存在其他数据。

完整源输入检查通过后，下一实施边界为现有转换器、按日消费和C的第五类支持；不是新增检测或状态重建。独立任务尚在整理最终交接，下一次读取其具体句柄，不能因等待超时重复启动。

## 22. 子前缀劫持五类型消费验收

记录日期：2026-09-11。**本地C首页已接入第五类子前缀劫持，有限切片验收通过。** 当前选择31个可查询日、28个失败日，共36,555条；新11条不是新增检测结果，而是第21节源记录的离线消费副本。整体goal未完成。

### 22.1 源交接与本轮实现

第21节独立任务已确认完成，最终交接全文、字段表、查询原文、独立审计及回执已复制至 `.local/core-overview-validation/sub-hijack-consumption-v1/source-evidence/`。主任务重跑完整只读核验，31日GO；其查询范围、Overlay可写性、历史时区和30,000项目录搜索上限等限制仍适用。没有重新查源库、写库、重跑检测或构建RouteState。

只扩展现有转换器、按日输入／索引、只读HTTP合同及C页面：子前缀为对象，父前缀是可显示和搜索的补充字段；角色原列表文本保存，有限解析的ASN供筛选。保持完整引用与24个明细原字段；03-02同秒的两个不同编号不合并，跨月份同键仍带月表身份。严格校验父子同地址族包含关系与原始起止／时长；异常输入不准入。7条结束已记录（其中4条跨日），4条未知；不补持续状态。明细无路径列，不生成null、空路径或借用普通hijack路径。类型名称与角色不构成独立确认、影响或责任证据。

### 22.2 输入、版本和兼容

当前目录为 `.local/core-overview-inputs/rrc25-sub-hijack-v2/index/manifest.json`，消费版本：

`overview_index_v2_9716f375e1626d693ae967790931170037cdcf97a0721a8d28e1b3e43ec546b1`

可查询日期仍为02-01—28及03-01、02、05。子前缀劫持正值日分别为02-25三条、02-26一条、02-27两条、02-28两条、03-02三条；另外26日有独立总表与事实自身日期零计数。旧四类型36,544条与新增11条合计36,555；不将父前缀多计一条异常，不将类型范围不同的计数称为旧版本发生变化。

原四类型 `rrc25-hijack-v3` 清单、59份日文件字节均不变；保留日期内36,544条原记录、内容版本和列表条目逐字节一致，31日前缀小时桶一致。原28失败诊断文件及原因不变：03-03时间矛盾，03-04普通hijack结束／时长冲突，03-06—31等级冲突。筛选不绕过整日失败，概况、趋势和列表为null。默认03-31仍为14条AS等级冲突，不偷偷切换日期或补零。

新目录根下保存31份五类型日包、旧四类型独立副本、完整源交接、三份诊断选择证据、冻结映射及构建脚本。旧四类型与新增子前缀劫持不是共同数据库快照。`rrc25-sub-hijack-v1` 保留为中间候选：其558次对账通过，但脚本审查保护修订后重新生成v2作为最终选择；不覆盖旧候选或已引用版本。

### 22.3 检查、审查和页面证据

主目录：`.local/core-overview-validation/sub-hijack-consumption-v1/`。

| 检查 | 已实际完成的证据 |
|---|---|
| TDD公开边界 | 完整转换／序列化复读、同版本HTTP、父前缀索引检索、7组时间／父子冲突反例；红绿过程记录在同目录执行记录，不测试私有方法 |
| 回归与构建 | `make api-types`、373项后端与136项前端测试、`make build`通过；仍有原pandas FutureWarning，不影响本轮结果 |
| 离线真实对账 | `api-offline-v2-proof.json`，558次公开接口核对；原字段、旧字节、逐日小时桶、排序、逐条分页、等级／角色／父前缀筛选、版本409和失败503 |
| 实际监听服务 | `api-live-v2-proof.json`，经前端28492代理到本地28491的558次实际HTTP；不是仅test client验收 |
| 旧副本独立复读 | `old-copy-restore-proof.json`，仅选择旧副本的59份日文件及59次公开HTTP，仍31日／28失败日／36,544条；同磁盘现有环境，不是异地或空白环境恢复 |
| 可执行笔记 | `子前缀劫持五类型消费复核.ipynb`的3个代码单元从头执行通过，复算源检查与消费对账；不连接源库 |
| 浏览器 | `v2-*.png`：02-25父子列表／跨日结束原字段、03-02同秒不同编号／未知结束、02-24独立零结果、IPv6无匹配、03-04筛选仍失败、默认03-31；桌面与390px检查，无页面横向溢出。已查看截图，不只保存图片 |

关键文件SHA256：

- 构建脚本 `build_consumption.py`：`6800b147b2879c158dc333d33694640fc4798d35ba8c03bae1677ff591d81993`。
- 消费核验 `verify_consumption.py`：`3480fa4d29133dbeeb46f72449f87ba7fae606773df5fd8f48c83b4c9157b145`。
- 已执行笔记：`091c492b8116a10ff70dcc887d5e5c6df129b8c19f911625f7a3d445893a87a5`。
- v2根目录 `build-proof.json`：`dd88e852b8e9b9f8fe5927b656e3441e97cb6cbba37952f208994ad4abde0da6`。

Standards／Spec由两个独立代理审查实施前文件快照与本轮增量；没有虚构Git提交基线，也未提交。Standards初审2项P2：优化模式会跳过断言、输出可能嵌入复制源；均已修复为首次写入前拒绝并经3次公开CLI负例验证、独立复审关闭。Spec无问题。最终未解决项为Standards 0／Spec 0；两轴报告分开保存在执行记录。`runtime-code-v2/`冻结本轮转换、准入、索引、诊断、服务、页面、合同及数据配置，供定位本次解释；不是完整环境镜像。

Matt setup也已完成：沿用 `xinghuahewo/domeye_new` GitHub Issues、五个默认标签和single-context文档布局，配置位于 `docs/agents/`。仅写本地配置，没有创建／修改远端issue或标签。第21节的等待描述是此前时点，不代表当前仍等待。

### 22.4 当前边界与下一项

只替换本任务本地后端输入；前端仍使用已接受C，不改布局和样式。当前监听28491／28492，实际可用不等于部署交付；测试浏览器独立会话已关闭，未操作用户原型标签。源库和共享服务未改，旧数据未删除。

下一项为国家中断的真实输入准入，先查已有模型和数据，明确国家对象、唯一关联、时间／结束、等级及地址族的适用规则，再决定消费切片。不得因已有国家事件详情页可用，就推断首页全日期国家异常已准入。可见前缀／起源AS规模、普通路由变化、完整恢复／导航和剩余日期准入仍未完成。观察覆盖与旧检测版本保持未知；不支持的事实记录查找范围和下一验证条件，不反复询问数据事实。03-31等级冲突的产品取舍仍待明确，其他不依赖它的步骤继续推进。

已按用户允许拆分长会话的安排，新建独立证据任务“Domeye 国家中断准入核验”，ID `01a08eb7-05a6-72c2-931d-bb43c52f99eb`；输出为 `/Users/botongwu/Documents/Codex/2026-09-11/domeye-country-outage-admission/outputs`。实际状态为active／inProgress，已开始读取领域文档和安全读取器。其范围仅同31日的国家异常源准入及完整证据交接，不得改主工作树、当前消费输入或页面。主任务接收后独立复读再实施；下次先检查该具体任务句柄，不因观察超时重复启动。

## 23. 国家中断六类型消费

记录日期：2026-09-11。源任务已经完成并停止，上节active为当时状态。**31日国家异常有限源准入GO；第六类已在本地C首页通过实际HTTP与浏览器验收。** 不代表两个月完整准入或完整goal完成。

### 23.1 来源和适用范围

完整交接保留在 `.local/core-overview-validation/country-outage-consumption-v1/source-evidence/`。主任务独立运行源核验：87个文件绑定一致，3条总表／候选／独立事实唯一关联，29日独立零计数，31日GO。真实国家事实14列、总表25列，保留月表命名空间与完整(source,country,outage_id)，不生成真实表不存在的v2字段。

本次业务只读事务为2026-09-11T04:29:50.208814+00:00至04:29:58.237712+00:00；实例仍为 `10.99.8.16/docker/domeye_core_dev_pg/bgp_project`，Overlay可写。SQL具有超时和输出界限；所有本任务SQL会话与子进程已回收。source=r→rrc25沿用用户确认，观察覆盖与历史检测版本未知。历史无时区时间按配置Asia/Shanghai解释，未独立证明入库时区；新旧类型证据来自不同读取，不是共同数据库快照。目录查找只覆盖交接报告列出的代码根、深度4及5,000项上限，不声称穷尽全项目。

| 业务日／引用尾部 | 原等级 | 结束信息 | 限制 |
|---|---|---|---|
| 02-27 09:12:32／IR/1/r | high | 未知 | 说明时间为02-28 22:34:40，不覆盖结构化开始 |
| 03-02 02:51:36／IR/2/r | middle | 19:25:48，59,652秒 | 说明时间为08:09:21，不覆盖结构化开始 |
| 03-02 08:09:21／IR/1/r | low | 08:12:46，205秒 | 原比例0.031与16/526三位重算0.030不同，不静默修正 |

全部国家记录地址族unknown；国家代码IR、名称伊朗来自原字段。ASN原数组供检索，不是冻结cohort或前缀集合。原聚合不保证同一时点、同一人口，数量／比例不作影响或可见规模；说明文本不参与时间、等级推导。03-02两条区间重叠仍按完整身份分别保留，不合并成一次实际断网。前缀／地址族用途STOP、聚合人口／比例重算用途REPAIR，不阻断上述有限记录用途，也不由记录GO扩大用途。

### 23.2 新旧版本和检查

当前本地选择 `.local/core-overview-inputs/rrc25-country-outage-v1/index/manifest.json`，版本：

`overview_index_v2_cc17a7c5fac10558ef9e04c03f31add378ea6f3cc2eb5acf04c7a29617ce330d`

旧五类型36,555条＋国家3条＝36,558条。仍仅02-01—28及03-01、02、05可查询，其他28日拦截、默认03-31不变。原五类型日文件与副本、原记录及列表条目字节、31日前缀小时桶和28份失败诊断均通过对账。新目录保存旧五类型独立索引副本、31份六类型日包、完整源交接、诊断选择、冻结映射与构建脚本；旧版本不覆盖。

公开转换／序列化、HTTP及离线索引测试覆盖国家身份、原名称／ASN检索、未知地址族，以及4组时间矛盾不能借其他类型筛选绕过。已执行380项后端／136项前端测试、类型生成、构建；原pandas警告仍存在。主验证目录 `.local/core-overview-validation/country-outage-consumption-v1/` 中：`api-offline-v1-proof.json`为766次真实留存数据公开HTTP核对；`old-copy-restore-proof.json`为旧副本59次复读。副本仅同磁盘现有环境，不是异地或空白环境恢复。

最终回执为`api-offline-final-proof.json`和`api-live-final-proof.json`，各766次完整对账；后者实际通过前端28492代理到28491。`api-offline-v1-proof.json`为审查修订前的通过记录，保留但不是最终回执。桌面与390px浏览器已验收国家名称、等级顺序、原字段详情、未知结束、未知地址族、零结果、03-04与默认03-31失败；8张截图均已实际查看，无横向溢出，独立浏览器会话已关闭。

`国家中断六类型消费复核.ipynb`的3个代码单元从头执行通过，不连接源库；`runtime-code/`冻结8份本轮运行解释代码及合同／数据档，非完整环境镜像。Standards初审2项P3（时区独立硬编码、两查询路径重复拼接搜索文本）已修复并独立复审关闭，Spec无问题；最终两轴未解决均0，报告与工具问题分别记录在`执行记录.md`。最终测试和766次对账在修订后重跑通过。

关键SHA256：构建脚本`8c1f4ec44aca867bce37884c944bd61f4b17de9587ef0c5d5c836676f5f8e8f5`；消费核验`692335cb7188bae9be5ab27aa1ca876b32144a15848ef247975358f3caabe006`；构建回执`69e9021da066647b95d16e50dacbd6c33e134bbef994662208f46e99e2bfd924`；已执行笔记`482cd826bea3b628c6bbbc2fcfae5b85f483f690bca2b1277fe3f609359517b5`。只有本任务本地后端更换输入，无共享服务、源库、检测、远端issue、提交或部署变更。旧数据未删除。

下一项为可见前缀数／可见起源AS数的既有制品核验，先确认观察点、快照时点、去重范围与单位。异常涉及对象数、地址覆盖块数不能替代这两项；找不到记明范围与最小补证条件，不擅自重建状态。28失败日（含等级冲突的待决产品取舍）、普通路由变化及完整恢复／导航仍未完成，goal保持进行中。

已按长任务拆分授权启动独立任务“Domeye 可见路由规模制品核验”，ID `01a08ed9-d75b-74f1-be59-139261c8a465`，输出 `/Users/botongwu/Documents/Codex/2026-09-11/domeye-visible-route-scale-admission/outputs`。首次实际观察为active／inProgress，已开始主树约束与来源核对；不是结果已交付。其范围仅只读查证与交接，不改主树、当前输入／页面或源库，不启动重建。主任务下次先读取此句柄，接收结果后独立验证，不重复建任务。

源证据身份：完整原文 `171a42ec703e32dda6e8d5b6f5249f53863ca00469c628fd3ab4a136d88d7738`；实际SQL `0d39318c8181dc16f9d357d0606e60d9f62619769dc1523cb366ce1cee118797`；回执 `0b55f7b91e30a8c2d64b93b4a41f85c1c174373ec85a4bbe0603058fba0acae7`；独立源审计 `c3d68392b6b6dc6d34a946fb0d07feb780fa5cfb861e5fd45c125a2e4960f1af`。源交接的笔记仅为查看入口、未执行；不能冒充主任务消费验证。

## 24. 首页相关导航与旧检索计数显示

2026-09-11。C的变化趋势、路由异常锚点实际可用，保留选定日期；事件检索与旧P0链接可到达对应页面。但当前仅绑定留存首页输入的受控预览中，旧事件GET返回500、旧P0状态GET返回503，不能称旧数据功能已恢复，API健康在线也不证明数据可用。旧检索500根因尚未完成日志确认，下一步应核实其独立运行绑定，不得静默切换数据源。

发现并最小修复 `EventsPage.vue`：加载／查询失败时记录数显示未知，成功空结果才显示0，成功后再次失败不显示上次计数。未改接口、合同、C布局、默认日期、消费输入或等级规则。实际500的DOM检查先红后绿；网络fixture验证成功0／1、撤销fixture后失败及防止计数10／11误通过。加载中分支经过静态审查，未完成受控延迟的浏览器动态验收。

136项前端测试、类型检查和构建通过；Standards 0，Spec初审1项验收脚本数字匹配问题已修复并独立复审关闭，最终两轴0。桌面／390px实际截图已查看，移动端无横向溢出。修正后C实际API仍为第23节版本，03-02国家筛选2条、默认03-31仍14条AS等级冲突；两项规模仍未知。

证据为 `.local/core-overview-validation/navigation-v1/执行记录.md`、改前副本、公开DOM检查脚本及截图。只完成所列首页入口和显示修正，不是所有旧页面／接口兼容或完整恢复验收。未提交、部署、更改共享服务或写源库。可见规模的独立任务仍在取证，完整goal继续。

## 25. 六类型独立副本恢复复读

2026-09-11。当前六类型index已复制到 `.local/core-overview-inputs/rrc25-country-outage-restore-v1/index`；60份文件、约165MiB，摘要及版本与第23节一致，文件不是原实体或硬链接。未切换28491／28492运行输入。

最终证据 `.local/core-overview-validation/restore-six-v1/restore-proof-v2.json`：66次Flask公开路由复读，覆盖31可用日、28失败日、36,558条及3条国家详情，默认03-31仍14条等级冲突。公开查询期间拦截Python文件打开、sqlite3连接对其他消费输入目录的访问及socket.connect，3个专用异常负例通过；不是OS沙箱。首版未覆盖sqlite的回执原样保留，不作为最终隔离依据。

另建临时28493副本后端／28494前端，实际浏览器验证03-02六类总数与国家2条、默认失败、02-01成功零；三张截图已查看。临时服务与独立浏览器均已关闭，原预览未动。Standards 0／Spec 0；完整记录及脚本在同验证目录。v2回执SHA256 `3aea4a6d8ffe54bd990c4352a3c8c38882b4ff0b0c5b1f455cbbe7a773ea247a`，脚本 `4312f95bf809e665345748f20f70fb52558dc7fb1f0841dd3d961c9f02077911`。

本片完成的是同磁盘、现有代码依赖环境下的消费副本恢复复读，不证明异地灾备、空白环境或所有原始来源证据可迁移。未改应用，未重跑全套测试；本轮依据为副本文件检查、公开路由及实际浏览器。旧检索/P0运行绑定仍待核实，不能据此宣布完整goal完成。

## 26. 可见规模既有制品核验

2026-09-11。第23—24节记录的独立任务已完成并停止；主任务复制完整交接并独立复核。**默认2026-03-31 23:59:59 Asia/Shanghai的可见前缀数和可见起源AS数均为REPAIR，仍未交付。** 数据事实无需用户补充，缺口及查找边界已留存。

| 来源 | 本次实际发现 | 对首页的影响 |
|---|---|---|
| feature_country | r/collect末行03-31 23:55，默认精确时点无行；当日282个五分钟时点，07:30—07:55缺6行。旧前缀字段及现存生产代码指向覆盖块口径 | 不作去重Prefix数，不能把末行延续到默认时点；特征行缺口不等于原始观察缺口 |
| ASN特征 | 当前schema不是完整成员清单；现存代码只写is_change行，历史生产版本未绑定 | 不按当时行数、全窗ASN或任意最后值推算Origin AS数 |
| 旧read-model | 实物清单窗口为UTC02-24—03-11，81事件、43国家序列；所引用RouteState实物未取得 | 事件对象和下游绑定不能代替整体规模，不能用于默认03-31 |
| 03-31 .0000 RIB | 压缩256,507,615字节及完整压缩摘要已取得；单次全流复算触发240秒上限，结果为空、未重试 | 完整Prefix／Origin集合、gzip全流验证未完成。样本时间UTC00:00，不是默认UTC15:59:59 |

单RIB摘要 `3dd9417860fccf32dd8ecbb63538aff10bd41f131467d19651c54487108e0e92`。只有物理记录1、2的IPv4样本与独立bgpdump核对：54条路由entry、2个Prefix、11个确定origin。主任务额外验证原始字节和逐Peer分布一致；不扩大为完整RIB、IPv6或首页规模GO。120条Peer表／67个Peer ASN亦不是覆盖数或Origin数。

交接位于 `.local/core-overview-validation/visible-scale-admission-v1/source-evidence/`：127份文件绑定和5个只读事务回收复核通过，完整报告、候选路径／扫描深度与上限、SQL、原文、失败回执均保留。源库仍为已绑定可写Overlay，多次读取不是共同历史快照；观察覆盖和旧生产／检测版本未知。首次父表页数线索不足、启动前压缩大小门限不等于流式硬限等读取限制亦已记录，不隐去失败。

主任务新建 `可见规模主任务独立复核.ipynb` 的3个代码单元已执行通过；原交接笔记仍保持未执行。主笔记SHA256 `f99d7e5d44b2616bd284da9f4d5828b0d9ee9097a2d64788fc71f9db94fc1e09`，证据清单 `7249aed01e2e48bac7e8961a280b2ffb82f2ae9c3f3683f222a9514937254c19`。未重查源库或重跑RIB，未改首页输入／指标合同。

本次来源核验交接经独立Standards／Spec审查，两轴均0项；报告分开保存在主任务复核记录，不以样本通过替代完整规模验收。

最小补证是先完成该单文件自身时点的Prefix／Origin集合、未知origin及实际Peer范围核验，不能直接跳到连续状态。是否允许首页规模区独立标注这一类已验证RIB时点（默认日期及异常窗口不变），已作为产品取舍提出，**尚未收到同意**；现有默认时点规则不变。若只接受配置精确时点，仍需匹配状态制品；新增状态重建范围／规则未确认前不执行。普通路由变化、28失败日准入和旧数据页面运行绑定等剩余项继续按原goal推进。

## 27. 旧事件列表运行绑定：REPAIR

2026-09-11。原28491／28492首页专用运行未绑定源库，旧events请求500的具体原因是本机5432默认角色postgres不存在；不是源表缺失。通过新项目自己的Git外配置，候选本地隧道连到远端31627／bgp_project，复核系统ID7663836852697006116、OID16384、PG12.16及只读设置。源仍为可写Overlay。主代码只允许启动器显式加载首页manifest，新旧配置隔离测试及后端382项通过，未修改检测或接口口径。

候选28493／28494串行8次HTTP及独立只读SQL核对通过：03-24—31共26,063条，高风险5,823，AS中断939，第二页匹配，02-01真实空结果；缺date／窗口外400、P0未配置503均符合原边界。旧总表查询成功不等于首页质量准入，03-31仍按原规则因14条等级冲突失败，六类型manifest未改。

**候选未通过并发验收，未切换原预览。** 浏览器与对账同时读取时出现未结束事务，随后一次200的列表与源不一致；另一个最多4轮的双请求探针在第2轮复现idle in transaction，响应内容正确也不能视为稳定通过。旧共享连接及按进入状态清理事务是下一片须用fixture锁定的修复对象，尚未实现。证据在 `.local/core-overview-validation/legacy-runtime-v1/`，包括来源预检、串行成功、并发失败、诊断脚本及执行记录。

P0本来未配置，未重建或发布；INFO和general read-model未绑定为本机可读制品，不能宣称全站恢复。候选服务、独立浏览器及本轮SSH均关闭；外部候选配置仍以0600保留为 `domeye-new-runtime/legacy-binding-candidate.env`，默认backend.env移走以免误启动。原28491／28492仍为已留存六类型输入，其他数据／产品决策边界不变。下一片先修复旧读取并发与失败语义，再重新验收运行绑定；完整goal保持未完成。

该片新增补丁和证据记录经独立Standards／Spec审查均0项，报告分开保存在执行记录；不抵消运行验收的REPAIR。关闭后核实三个候选端口无监听，本阶段源连接剩余0；原首页默认失败和03-02可用响应的版本均未变。

## 28. 旧事件读取修复与本地共用预览

2026-09-11。本片修复第27节缺陷：Web每个请求有自己的惰性数据库连接，结束时回滚关闭，未用串行服务器绕过并发；事件列表的连接、目录、列表或计数错误及所需月表缺失返回503，不能返回成功零计数或跨月部分结果。成功筛选、分页、时间和源表口径不变。API合同与前端生成类型同步，不改C布局或任何输入／源库记录。

8项公开应用链fixture测试通过；同8项放回改前四份生产文件时全部失败，负向回执保留。后端390项／前端136项及类型检查、构建通过。来源预检再次确认系统ID7663836852697006116、bgp_project／OID16384、PG12.16及事务只读on；角色权限不被称作绝对只读，源仍为可写Overlay。

候选28493／28494完成8次实际代理HTTP与独立只读SQL对账、4轮双请求并发及766次同版本C复读。原列表03-24—31共26,063条，等级／类型筛选与第二页匹配，02-01为真实空结果；并发结束源连接为0。实际页面检查翻页、等级、类型、成功零和错误未知；停止本轮私有隧道时旧列表503而C留存日仍200，恢复后重新读取成功。工具日期填值曾提交缺date请求并被400拒绝，另用真实日期控件完成空日验收，未掩盖该失败。截图原通道超时，改用独立后台标签实际查看成功、空日、失败、恢复与最终主预览，未伪造截图文件。

2026-09-11T06:24:52Z切换本机28491至修复后的受控后端PID13206；28492前端PID87082保持不变。使用独立0600配置 `domeye-new-runtime/backend-local-readonly.env` 及本机31627隧道，启动命令和回执在README／本片记录。切换后原28492再次完成8请求源对账、766次C复读及4轮双请求并发，每轮应用连接为0；未改变manifest `overview_index_v2_cc17a7c5fac10558ef9e04c03f31add378ea6f3cc2eb5acf04c7a29617ce330d`。独立候选服务与临时浏览器已关闭；仅保留所需本地预览和隧道，不涉及共享服务或部署。

本片证据在 `.local/core-overview-validation/legacy-runtime-v2/`：改前实物、正／负测试、来源预检、串行／并发与断链回执、浏览器DOM记录、候选及主预览C复读；内嵌截图已实际查看，未另存PNG。Standards发现核验脚本优化模式可跳过断言的问题，已补入口拒绝并实测不产生回执，复审关闭；Spec无新增问题，报告分轴保存。

完成范围是旧事件列表的本地只读兼容和上述实际验证。C使用已准入留存版本，旧检索读取此刻源总表，两者不是同一版本／质量等级，不能据后者成功放行C的28失败日。默认03-31仍14条等级冲突；P0、INFO／general read-model本机准入、可见规模、普通路由变化及完整恢复范围仍有缺口，整体goal未完成。

## 29. 普通路由变化的既有制品核查

2026-09-11。本轮推进goal第4项的来源验证，不改页面、接口、默认日期、准入规则或生产输入。结论是**尚无已验收的普通变化消费切片**；不能把下述计数填入首页。按照数据质量skill核查粒度、字段、时间与引用，Matt research并行追第一方源码；主记录仍为本台账与短规格，附离线可执行笔记，不另建治理平台。

### 29.1 实际查到什么

| 候选／核查范围 | 本轮事实 | 用途判断与下一条件 |
| --- | --- | --- |
| 已绑定`bgp_project`目录 | 一次repeatable-read只读事务返回49个关系、652个字段，其中public为37个关系；额外关系包括Timescale管理视图，不是新增业务数据。路径字段位于8张异常月表，特征表没有逐Prefix／Peer的前后路径或起源状态 | **不能替代普通变化输入。** 这里只核查结构，不读业务行、完整状态或其他实例；异常前中后样本与通告／撤回量不能当作所有普通变化 |
| 旧S4，伊朗`country_outage/2026-02-27 09:12:32/IR/1/r`完整路径文件 | 文件402,585字节；1,956条关系、5,850条路径样本与manifest计数／压缩SHA一致。样本字段只有Prefix、AF、AS_PATH及其ID、独立Peer ASN集合和观察次数，**0条含自身观察时间**；关系窗口为UTC02-27 00:10—03-11 00:00 | **STOP用于两时点路径演化，高风险误用，证据明确。** 关系首末并发时点不能赋给每条路径；不是证明没有路径变化，也不将AS_PATH经过者当Origin |
| 旧read-model的Prefix×VP终点下钻：IR第一页 | 完整读取1,000行，匹配页SHA／行数；Prefix／AF不合法0行、旧对象键重复0行；文件标记visible为true 918／false 82，quality均为clean。含baseline_origin_asn与终点origin_asn；可见且起源已知的不同值样本0行。仅有last_updated_utc，范围UTC02-24 00:00—03-10 23:56:21，没有基线路径正文或每行两端状态时间 | **REPAIR。** 这些是旧制品自身字段计数，不是新增消失／恢复／起源变化指标；不将其clean标签当作新边界准入。需补匹配Seed与终点RouteState、对象范围、时点与解释规则。IR目录声明385,427行，本轮仅第一页，不外推整个IR／43国／RRC25 |
| `/home/bgpdata/Domeye/data/feature/updates.20260324.0825.gz.data`完整原文 | 1,104字节、6行，时间字段均为UTC03-24 08:25:01；**6／6 Prefix无法解析**，例如`0.189.203.175/125`。SHA256为`1af9a329c5dd6339bf1f5f991c6075a2b5142afc7b7dad2a524429e5d6c09632` | **STOP准入该派生文本，高严重度，字段问题已确认。** 未读取对应原始MRT，不能断言原始数据或整个解析器损坏。该文件也无独立Collector／原输入哈希绑定；保留原文，不覆盖修复、不据此生成变化 |

旧S4文件位于`/home/bgpdata/Domeye-Dev/data/agent-real-loop-stage1-grm-1d1e0463/events/IR/slot-202602270010-be7bb2bc833f/path-downstreams.jsonl.gz`。终点下钻源根为`/home/bgpdata/Domeye-Core-runtime/releases/20260821T160639Z-country-outage-interactive-agent-prod12-backend/data-layer/read-model/`，页为`prefix-vp/pages/IR/page-000001.json.gz`。这些都是远端10.99.8.16路径，不是本机运行输入。

### 29.2 引用与查找限制

六份选定文件的远端摘要与本地留存副本一致，单文件读取上限1MiB；第七份S6血缘清单单独复制核对，SHA256为`4debab7e16fb4002fd7068bcb784ac53dd921338f8d4882dff3646f9085e1227`。初次源端校验只在终端输出，Spec指出离线留证不足；现已追加`lineage-binding-proof.json`，记录2026-09-11T06:56:26Z源端只读核对时间、前后stat及匹配摘要，不冒充初次复制回执。下钻目录和S6均引用RouteState dataset `route_state_dataset_v1_c2f7f7c7c63c824f4e92ed4c90787bcb`，预期manifest SHA为`f810c354b9dd87cdd62ae51b24281f72eac1344552b6616b8cf70b542433b587`；实际指向的`/home/bgpdata/Domeye-Core-dev-data/research-runs/rrc25-route-state-224-310-s2-0a0a322`及上级research-runs均不存在于这次读取位置。

另外一次只读事务查询下游清单指定的两个数据库名`domeye_dl_s3_47e38a7_4523a27b`、`domeye_dl_s4_2338a76_d0f8092f`，在当前绑定实例中均未找到；不声称其他实例、归档或迁移位置不存在。两次事务均绑定系统ID7663836852697006116、bgp_project／OID16384、只读on，并回滚关闭；源仍为可写Overlay。

四个指定目录各最多500项，存在的三个目录均未触及上限：发布data-layer、其lineage及Domeye/data/feature；research-runs报不存在。feature一层为10份文件，仅完整读上述小文本，没有加载8GB级派生BVIEW或扫描整个MRT库。本轮复用第26节13根有界定位作为线索，没有把它说成重新完成全盘搜索或证明永久丢失。

### 29.3 当前结论与最小后续

普通变化继续未知，不以消息量、异常样本、终点可见标志或“起源差异样本0”代填。已有制品复用路径为REPAIR；这不等于整个goal停止或完成。

源码侧查到Go RouteEvent分区／路径字典和Python RouteEvent索引有实际写入普通观察、时点、对象与血缘的代码，不能笼统称旧实现只有异常数据；对应历史实物与运行版本本轮仍未核实。旧异常JSON可带时间标签，但源码丢弃了VP到路径的对应键，不能由数组顺序或事件时间补造每条路径观察。终点下钻源码按seed Origin选国家并匹配两端旧路由键；当前源码存在不证明历史使用了它。第一方文件、完整提交和行号见证据目录的`代码来源核查.md`，没有执行旧代码。

下一项优先按该源码线索定位已生成的RouteEvent索引／分区与路径字典；未取得时，再对ADR已记载的RRC25原始MRT样本进行有界复核：确认同一Prefix／AF和原始Peer端点在两个明确UTC观察时点的路径正文、源文件身份与物理定位。若能够准入，只说明“这两次观察的路径不同”，不能标为完整连续状态、所有变化次数或网络原因；失败则记录失败，不选择性重跑凑正例。此步骤属于已有原始观察取证，不默认批准连续身份区间或RouteState重建。确需新状态重建时，须先确定范围及规则。

本轮证据目录为`.local/core-overview-validation/ordinary-route-change-admission-v1/`：`catalog-proof.json`、`source-binding-proof.json`、七份原文件、SQL／核验脚本、`普通路由变化复核-v2.ipynb`及源码核查。笔记已完整从头执行，数据只在本地复读；临时笔记依赖使用uv隔离环境，不修改项目锁文件，内核显式关闭。早期笔记和生成脚本保留，后续验证以v2为准。没有修改运行服务、源库、旧输入或页面；本片是来源准入调查，不冒称新增页面功能已通过浏览器验收。

Standards未发现硬违反，留1项P3：一次性取源脚本在rollback抛异常时可能跳过close，笔记生成器存在类似清理链。已成功回执未因此失效；保留原脚本与摘要，不改写被引用的执行身份。异常场景尚未验收，后续若复跑须另建版本先修复，不作为生产入口复用。Spec的S6源端留证P2经追加回执与原审查者离线复核已关闭，剩余0项；不代表普通变化准入。两轴报告与处置分开保存在本片执行记录。

## 30. 找到既有 RouteEvent pilot，并验证一对原始路径观察

2026-09-11。本片接续第29节的来源调查，**找到实际可读的RouteEvent索引，不再只有源码线索**。已完成一对原始观察取证；普通路由变化的首页消费仍为REPAIR，没有新增页面、接口、发布输入或状态重建。

### 30.1 实物与查找范围

源位于10.99.8.16的`/home/bgpdata/Domeye-Core-artifacts/releases/20260720T160000Z-p0-legacy/data-quality/candidates/final/route/p0-route-event-pilot.sqlite3`，2,097,152字节，SHA256为`d2bc13045941afc7ff8a2192639919a70989f91ef95d66b0a977f54d7e820c42`。源端以SQLite `mode=ro`、`query_only=ON`读取结构，事务回滚后关闭；未发现该文件的WAL／SHM／journal。留存副本以只读immutable方式复核，未连接其余会话恢复数据库。

本次按第一方源码实际分区／索引命名在13个Domeye根检索。第一次9层、60,000条目、40秒上限的扫描触达条目与部分深度限制；第二次额外排除源码／镜像目录，在同13根看到51,419项、7,334个目录，20个命名命中，未触及条目／时间／深度上限。排除项包括Git、依赖、日志、PostgreSQL、原始MRT、源码镜像等，不跟随符号链接。这里“未触达上限”只适用于所列排除后的范围，不证明整个文件系统只有一个RouteEvent制品。原始MRT另按清单精确路径读取，没有全库扫描；未读取07月AS_PATH CSV及会话数据库。

### 30.2 数据粒度与输入绑定

索引实际有1份输入、1,200条物理记录、2,392条路由观察、661条路径字典、61个原始观察端点；2,248条announce、144条withdraw。观察时间仅为**UTC 2026-03-24 14:55:00—14:55:02（北京时间22:55:00—22:55:02）**。SQLite结构／外键检查通过；逐观察Prefix／AF校验失败0，来源元素键重复0。61是该旧索引的端点数，不是已验证覆盖或新稳定Peer数量。

元数据明确`pilot_only=true`、`production_complete=false`、`selection_coverage_claim=none_pilot_subset`。不得将2,392条观察说成路径变化次数或全天覆盖，也不将旧父清单记录的覆盖率提升为本轮证据。

完整留存六份源文件：索引、原summary、原reconciliation、父MRT manifest、selection、所选原始MRT；均记录读取时点、前后stat与SHA。父清单4,085,620字节、文件SHA`329078407b5b0dbce871ba0f8f440f7d423aec9fe6b3c1904f5c5edacba08edc`，与pilot记载一致；所选对象在父清单唯一，内容与selection／索引artifact相符。manifest／selection语义指纹只核对引用值，未重实现旧指纹算法，未重验其余10,341项输入。

选定原始文件为`/home/bgpdata/data/ripe/rrc25/2026.03/updates.20260324.1455.gz`，30,063字节，SHA`866ad497e45b771a8340e1a54b6723ee8119c26190f7f33dced08e02591581dd`。本轮完整解压190,075字节，1,200条MRT的零基序号、解压偏移、长度、头字段与完整记录SHA逐条匹配索引，差异0；这仍不证明采集无缺口。

### 30.3 已验证的有限对照

用固定SQL按前缀文本、Peer IP、记录顺序选取第一个“跨秒、同对象、不同路径”的IPv4候选；这是结果条件选样，只用于可追溯示例，不估计变化比例。

共同对象：RRC25、原始Peer IP `77.243.32.3`／AS31027、IPv4 unicast前缀`103.109.9.0/24`。本地端点均为`193.0.4.29`／AS12654，原interface index为0。没有为此补Peer BGP ID、稳定Peer Identity或Session ID。

| 观察 | UTC时间 | 原始物理定位（零基；解压偏移） | AS_PATH |
| --- | --- | --- | --- |
| A | 2026-03-24 14:55:00 | record 39；offset 6001；length 150 | `31027 12552 9002 45430 58952 136168 150797` |
| B | 2026-03-24 14:55:01 | record 445；offset 72050；length 166 | `31027 2116 12552 9002 45430 58952 136168 150797` |

两条均为MRT16/4的UPDATE，普通IPv4 announce，无withdraw／MP／AS4_PATH属性，AS_PATH各为单个四字节AS_SEQUENCE。新的一次性有限字节读取器逐字段核对时间、原始端点、NLRI元素位置、路径正文／分段和源记录；另用系统bgpdump对同两份原始帧交叉核对，退出0、stderr为空。二进制SHA为`d73b00a79ae0d284b2c47010c1e2f26d28c5a4ce449b36ff021a41502dcca1ee`，与旧attestation中bgpdump1.6.2绑定相同；字节读取器不调用旧模块。旧producer／adapter版本只按制品原文保留，不等于重验历史执行。

**GO仅限：“这两次原始宣告观察携带不同路径”。** 两条末端AS均150797，不是起源变化；不推出中间只有一次变化、连续Session、完整RouteState、日级变化数、当前可见性、网络原因或真实用户影响。首页三项普通变化仍未知。本片取得现有pilot后没有再读取ADR的02-24候选，也没有启动任何连续状态重建。

### 30.4 复核与后续边界

证据在`.local/core-overview-validation/route-observation-pair-v1/`，包括两次有界扫描清单、源端与留存回执、六份源文件、`pair.sql`、`check_pilot.py`、`pair-proof.json`、`bgpdump-pair-proof.json`、第一方解析边界笔记和完整执行的`RouteEvent样本复核.ipynb`。内存／临时目录负例覆盖非法Prefix、截断、消息长度、超范围subtype／segment、AS4_PATH／MP／重复属性及withdraw；不读真实源。副本保留和校验不等于不可变存储、备份恢复或全量协议正确性证明。

首次结构探查错误假定metadata值列为`value`，只读SQL失败并关闭连接；后按实际`value_json`复核，没有修改数据库。首次探测bgpdump使用了不支持的`-V`，退出1并显示版本用法；不作为样本成功证据，后续两帧对照采用明确`-m -p -v /dev/stdin`并保存真实退出与诊断。未掩盖这两次探查失败。

本片在“已有观察样本可回溯”层收口。可复用此对照讨论最小样本展示，但尚无生产消费契约或页面验收；若要普通变化日级指标，仍需覆盖、比较对象与状态规则，不能通过外推该两秒pilot补齐。默认03-31的14条等级冲突和规模区独立RIB时点选择仍待产品决策，原六类型C输入与本地服务保持不变，整体goal未完成。

本阶段Standards与Spec分别审查完成，均0项发现；各自边界与完整报告在本片执行记录。6单元离线笔记完整执行、8项合成测试通过，实际C只读复读仍为原版本。没有新增页面／接口，因此不伪称新功能浏览器验收。用户尚未确认下一片等级冲突处理选择，本片不改准入规则。

## 31. 独立目录重装依赖后的首页恢复

2026-09-11。推进goal第5项，补第25节“沿用现有代码／依赖”的限制。本片没有修改生产代码、输入版本、默认日期或等级规则；**现已证明当前六类型首页能够从独立目录重新安装依赖、构建并恢复消费**，仍不是全站或灾备承诺。

### 31.1 恢复材料与环境

从当前工作树保留262份源码／配置／锁文件，包含未提交改动；加第25节独立副本的60份消费输入。逐文件校验并生成`bundle-manifest.json`，排除`.git`、`.env*`、日志、venv、node_modules、dist和其他数据。归档`domeye-c-consumer-v1.tar.gz`为9,936,684字节，SHA256 `4b4375745d3ac14f98d2b0019a9cbedbe813153476d2fdeba289e82eff2c5de8`；源码不是HEAD提交内容身份。消费版本仍为`overview_index_v2_cc17a7c5fac10558ef9e04c03f31add378ea6f3cc2eb5acf04c7a29617ce330d`。

在`mkdtemp`创建的独立目录解包，322份文件摘要匹配且不是包目录或原输入硬链接；安装前确认venv和node_modules均不存在。执行`uv sync --frozen --link-mode copy`、`npm ci --no-audit --no-fund`及现有前端构建脚本，全部退出0；包下载缓存可复用，但没有引用原项目依赖目录。实测macOS、Python3.10.20、Node26.5.0、npm11.17.0，锁文件未改变。npm提示两个包的安装脚本尚无审批，本轮未新增审批而构建成功；提示及构建日志原文保留。

恢复后端使用新venv与恢复副本的数据档，65个已导入项目模块均来自新目录。Python文件打开／SQLite连接拦截原工作树及`/home/bgpdata`，另禁止`socket.connect`和`psycopg2.connect`；4个专用负例通过后才构造应用并在本机28495监听。前端28496使用**新构建dist**的Vite preview代理此后端。该Python层检查不是OS沙箱、完整系统调用审计或生产网络隔离保证。

### 31.2 实际恢复验收

- 73组真实前端代理HTTP与原28492完整响应一致：31可用日、28失败日、36,558条，默认03-31失败14条，3条国家详情与各六类筛选。失败日没有消费指标／列表，成功零仍为0，规模未知仍为null。两端消费同一版本，不比较或重写源DB。
- 暂时移走**本次解包副本**的03-02日文件，临时服务返回503、原服务仍200；在finally放回原文件，摘要不变且恢复200。没有删除或扰动原输入，也不由缺文件回退到原数据。
- 在新代码／依赖目录执行现有`make test`：390项后端、136项前端通过；原有4项转义弃用和1项pandas未来行为警告保留。测试使用fixture／mock；真实数据只通过上述独立只读验收。构建成功不单独作为恢复依据。
- 使用agent-browser独立会话实际检查03-02总数3,678、国家筛选2条及同版本详情、02-01成功零、默认失败14条和390px首屏；页面宽度与文档宽度均390，无横向溢出。关键截图已查看。一次猜测的缩小snapshot选择器无命中，随后用实际完整快照定位，不把该失败当作页面缺陷。

验收后关闭独立浏览器，复核PID与命令后仅终止本片后端21077／前端21128，两个执行会话退出143，28495／28496无监听；原28491后端13206和28492前端87082仍运行。没有部署或修改共享服务。

### 31.3 保管与仍缺什么

归档、已执行的恢复入口、中文恢复说明和保管回执另存于工作树外`/Users/botongwu/.codex/recovery-drills/domeye-c-20260911-v1/`，与本片留存摘要一致，文件不是硬链接。这样不再仅依赖原工作树目录；**仍为同一主机同一磁盘**，不防整机／磁盘丢失，不宣称异地、不可变存储或长期责任已确认。临时解包目录约427MB保留供复核，可能被系统清理；恢复材料不依赖它。未做数据清理或覆盖旧版本。

证据目录`.local/core-overview-validation/clean-recovery-v1/`保留归档、bundle逐文件清单、安装／构建日志、`bundle-proof.json`、`setup-proof.json`、`startup-proof.json`、`http-proof.json`、`custody-proof.json`、关键截图、脚本、中文恢复说明及阶段开始的三份文档基线。整套测试输出来自本轮实际终端回执，执行记录标明范围，不伪造未留存的测试日志文件。

GO仅限当前已准入首页消费的上述本机恢复路径。原数据库、INFO／P0、全部上游证据闭包、离线安装包供应和异地恢复仍未验证；日期准入、规模、普通变化功能不因恢复成功而完成，整体goal继续。下一阶段不自动放行28个失败日，也不批准连续状态重建。

审查修正：Spec发现手动说明的安装步骤继承环境，未复现实际安装脚本的环境白名单。保留原v1说明及回执，另建`恢复说明-v2.md`，安装／构建均使用清理环境并在子shell内遇错停止。直接抽取新版命令，在故意设置外部`UV_PROJECT_ENVIRONMENT`的新恢复目录执行，安装／构建退出0、外部依赖目录未创建、322份绑定文件摘要未变；回执为`guide-v2-proof.json`及对应日志。这是安装步骤补验，没有重跑原73组HTTP或整套测试，也没有再启动服务。当前推荐材料另存工作树外`/Users/botongwu/.codex/recovery-drills/domeye-c-20260911-v2/`，由`custody-v2-proof.json`绑定；归档及入口摘要不变，v1仍保留作历史。

Matt双轴审查收口：Standards初审0项；Spec初审1项P2，独立修复复核后未解决0项。完整报告及适用范围在本片执行记录，初审不伪写为零；本阶段未修改生产代码，整体goal仍未完成。

## 32. 单RIB规模补证：发现不完整源文件，核验同日另一份完整快照

2026-09-11。推进goal第3项，修正第26节只能报告“全流超时”的证据状态。没有改默认日期、页面指标、接口或输入manifest；没有重放UPDATE、重建Session／RouteState、重新检测或写源数据。

### 32.1 原00:00 UTC文件不完整，停止完整规模准入

已绑定的`/home/bgpdata/data/ripe/rrc25/2026.03/bview.20260331.0000.gz`仍是256,507,615字节，SHA256 `3dd9417860fccf32dd8ecbb63538aff10bd41f131467d19651c54487108e0e92`。本地完整副本摘要相同；复算66.253秒后报gzip结束标记缺失。本机独立`gzip -t`退出1，源端直接完整解压亦复现EOF，故不是仅本次传输或自编读取包装器的问题。源端失败前已交付2,276,458,496解压字节，不能当作完整文件长度、合法路由数或完整前缀集合。

初次SCP因180秒上限终止，保留220,385,280字节部分文件；确认进程终止后，核对源全文和同长度前缀摘要，只读取余下36,122,335字节，另建完整副本。两者均保留，不覆盖源或掩盖失败。完整副本与源身份匹配不表示内容完整。该源文件如何变成不完整仍未知，不推及全部RIB或采集系统。

### 32.2 08:00 UTC候选完整复核结果

同日08:00与16:00 UTC文件均完整解压至EOF。选择**08:00 UTC／Asia/Shanghai 03-31 16:00**候选；16:00 UTC文件晚于配置默认UTC15:59:59，不用于倒填。候选源为`/home/bgpdata/data/ripe/rrc25/2026.03/bview.20260331.0800.gz`，438,316,014字节，SHA256 `7ab60c80563b22350445b934ed055e44a0ffd98503de73f55f6b7dd20f6fa419`，已另存本地完整原文件。

沿用冻结的有界核查规则，594.959秒内完整解析4,353,737,106解压字节、1,403,384个物理记录，退出0；全部MRT时间为UTC08:00，View Name为rrc25、Collector BGP ID为0.0.0.25。另一个独立程序完整复核物理结构、Prefix集合摘要、逐Peer条目数，结果一致；它没有独立解释全量AS_PATH。

| 文件自身统计 | IPv4 | IPv6 | 双栈口径 |
|---|---:|---:|---|
| 去重Prefix | 1,133,653 | 269,730 | 1,403,383，按地址族区分后相加 |
| RIB条目数 | 46,486,634 | 10,164,442 | 56,651,076，不是前缀数 |
| 原规则下AS_SEQUENCE末端明确ASN去重值 | 78,329 | 36,565 | 并集85,730；不直接称正常网络AS总量 |
| 末端为AS_SET的条目 | 7,278 | 1,151 | 8,429，约占全部条目0.014879% |
| 引用Peer表位置 | 64 | 57 | 并集116；表内总120，不是覆盖率或Session数 |

未见重复Prefix记录、空entry记录或单物理记录重复Peer位置。Originated Time为UTC02-12 16:10:23—03-31 07:59:59，不作为文件快照时间。保留完整原字段和集合摘要，不外推源端无缺测或全球覆盖。

85,730个末端明确ASN取值中有165个私用取值及保留值65535；另有36个AS_SET成员不在该双栈集合中。因此**不能直接把85,730填进“可见起源AS数”并解释为正常网络总量**，也不静默删除、过滤或挑选集合成员。私用／保留分类依据[RFC 6996](https://www.rfc-editor.org/rfc/rfc6996.html)和[RFC 7300](https://www.rfc-editor.org/rfc/rfc7300.html)；保留值的出现不是解析器协议错误，具体来源原因未验证。

### 32.3 复核、保管与页面边界

从完整副本按原偏移复核Peer表及6个IPv4／IPv6物理样本，再与bgpdump独立输出对照146条entry，时间、Prefix集合、Peer IP数值／ASN、明确末端ASN集合与不确定数量均一致。两次早期逐Peer比较因两个等值IPv6地址的压缩写法不同而失败；保留原文后按IP数值比较通过，没有丢弃记录或改源IP。此交叉仅覆盖选定样本，不伪称bgpdump全文件核验，也不构造稳定Peer／Session身份。

证据在`.local/core-overview-validation/rib-scale-stream-v2/`：原文件副本、`retention-complete-proof.json`、`full-failure.json`、`gzip-cli-proof.json`、`same-day-integrity-proof.json`、`candidate-0800-proof.json`、独立`candidate-0800-frames.json`、`candidate-retention-proof.json`、`candidate-samples-v3-proof.json`及执行／失败记录。保留源只读和逐版本绑定；不是不可变或异地保管。12项内存测试通过，不将其当作完整协议验证。

当前准入仅限**该文件自身的前缀集合与有边界的原始起源观察**，尚未接入首页。首页是否允许独立标注这个RIB时点仍待用户选择，起源指标的歧义／私用／保留取值规则也未冻结；默认23:59:59精确状态、28个失败日和普通路由变化仍未交付。不得用完整单RIB证明连续状态或默认时点状态，整体goal保持未完成。

阶段收口：离线笔记6个代码单元已完整执行并逐项读取输出；Standards／Spec独立审查均0项，仅认可本片补证。主任务另执行`final-proof.json`所记集合与HTTP复读，并实际查看原03-02成功页／默认03-31失败页及截图：规模仍待验证、默认14条等级冲突与消费版本不变。独立浏览器已关闭，没有新增服务或页面功能。报告、笔记与最终复核的各自范围见本片执行记录，不将静态审查当作新增首页功能验收。

## 33. 默认日六类型与等级待核实消费

2026-09-11。用户确认规则后，优先落实第14节默认日等级冲突展示；不再将该选择列为待审批。**本片准入2026-03-31的14,410条六类型留存异常，14条AS等级冲突独立标为“等级待核实”**。不改源数据库、原记录或旧消费版本，不重新检测，不扩大为连续状态重建。RRC25覆盖与历史检测版本仍未知。

### 33.1 来源及有限窗口

默认日窗口为Asia/Shanghai的03-31 00:00至04-01 00:00（右开）；项目默认日期与快照配置未改。沿用第14节三类完整留存的13,328条：前缀中断12,302、AS中断290、泄漏736。旧原文SHA256为`5c8244e8b0c7b149e744a8507e6a2f5d8a8727dcf7d71ec160af26003eb8231c`；原转换记录逐字节保留，未因等级冲突重写。

本次对同一明确绑定的bgp_project、OID16384、系统身份7663836852697006116执行有界只读repeatable-read事务，另取得前缀劫持562、子前缀劫持446、国家中断74，共1,082条。总表采用类型标签或引用类型的并集查找，候选按完整身份跨当月关联而不以候选开始时间裁掉冲突；另查事实自身日窗及独立计数。1,082条总表、候选和自身日窗事实集合一致，无重复／多候选；原字段完整保留，双侧身份、对象、起止与时长、等级及消费字段通过。

新增`extra-records.jsonl`为6,249,876字节，SHA256 `018bf93978ba875b230d4fd34f21780fd04013b536d4d77b6a12ff83dfd95c79`，绑定SQL、读取程序、来源、只读设置及回滚回执。语句／事务空闲15秒、连接5秒，客户端90秒、输出64MiB上限。旧三类与新增三类是**不同读取事务的组合，不是共同数据库快照**。这里只说明所选六类留存记录，不证明观察完整或所有路由异常均已检测。

### 33.2 消费规则与原值

14条均为AS23860，总表`level=low`、明细`outage_level=middle`。消费层不选择一方：`level=null`，另加`level_conflict`，保留双侧原等级、总表月表和原文摘要；原引用和记录内容版本不变。只允许这类已核验AS等级冲突例外，非AS冲突、未知等级、身份／时间等错误不借此放行。

新增`recorded-anomaly-overview/v2`兼容解释；旧v1仍可读且不接受冲突注解。列表支持独立`level=conflict`，不混入普通`unknown`筛选；高、中、低后才排列未确定等级，同组仍按时间倒序及原引用排序。详情显示“总表低／明细中”和来源定位。等级筛选仅影响列表，不改全日概况和小时趋势；null不套用低等级颜色。合同、生成类型、前端客户端和两端公开行为测试同步。

### 33.3 审查修复与版本选择

初候选`rrc25-grade-conflict-v1`的审计遗漏sub_hijack总表父子组合对象与明细的双侧核对，Spec提出1项P2。真实数据未查出对象冲突，但反例可误判GO，因此没有直接切换日常预览。保留原脚本、回执、候选及笔记；新增`audit_inputs_v2.py`先执行原全部核验，再精确核对父／子角色与总表文本，失败追加错误并REPAIR。

五项临时fixture测试覆盖正确对象、无关对象、角色颠倒、null及仅有子前缀，红转绿；真实446条补验全部一致、原14,410条字节不变。首次测试的导入别名使mock未命中，误读了已有旧留存文件后因临时回执缺失失败；修正调用接缝后的红绿阶段使用fixture，不把首次装配错误称为业务反例成功。没有查询或写数据库。

最终选择`.local/core-overview-inputs/rrc25-grade-conflict-v2/index/manifest.json`，版本：

`overview_index_v2_b984ac7ea714c1bab5108db64aa0eff669b2ca1223f287edb73ff6e98ad86f73`

此目录另绑定`supplementary-admission.json`，其SHA256为`7834a193fe3b2e977d45cea030f4baa6349b9b6562b60cb55fe5da30c6a4a8cf`，连接补验脚本／报告、源原文、默认记录及原候选身份。v2审计回执SHA256为`53295e40d42b60e9ed8d9b8f7d87ce2c332883dbf37b9057feecf3a10892b911`。日SQLite、旧单日包和被引用版本不改，补验是独立附加依据，不伪装成初次审计已经覆盖。

共32个可消费日、50,968条；旧31日36,558条记录文件与公开字段不变。另27个失败日保持503及null指标，不能用筛选绕过；旧28日诊断仍在旧目录。初候选复制时漏带31份相对引用的旧manifest，已通过`finish_input_refs.py`按声明摘要补齐独立副本并记录回执，没有改已绑定manifest；此补齐不证明全部上游证据闭包。

### 33.4 验证及剩余范围

`make test`通过392项后端、137项前端，原有pandas未来行为警告保留；类型生成、前端typecheck及构建通过。目标公开测试覆盖直接包／索引两条路径的冲突筛选、排序、原详情、非法注解和原时间门禁；额外5项补验fixture测试单独记录，不混入生产测试计数。

初候选与补验后候选各执行987次实际HTTP；最终`candidate-http-v2.json`记录UTC10:04:17复读。旧31日逐页对照原运行版本全部36,558条公开字段／概况／趋势；默认日145页全量14,410条不重不漏、六类计数、14条同版本详情、等级分组、24小时桶按地址族对照独立SQL、27失败日门禁、成功零及旧版本409均通过。不是源数据库重查、恢复或浏览器验收。

实际独立浏览器检查默认14,410条、待核实14条及第二页4条、双侧原等级／来源摘要、未知筛选成功零、02-01成功零、03-03失败null，以及390px页面／详情。窄屏文档宽390、详情宽356，无页面横向溢出，关键截图已查看；v2再次检查默认计数、待核实筛选和同版本详情。工具一次相对截图路径未生效，按返回实际路径留存；一次不支持的语义select命令随后改用真实元素引用，不算页面故障。

证据目录`.local/core-overview-validation/grade-conflict-v1/`保留完整源／失败探查、SQL／回执、初审／复审、补验与提升入口、运行／HTTP回执、截图、阶段基线及`默认日等级冲突消费复核-v4.ipynb`。笔记6个代码单元完整执行，重新复读源关联、旧记录字节和HTTP回执，输出已逐项检查；旧笔记只作历史。Standards补审发现生成器使用assert在优化模式下可跳过门禁，v3改显式抛错但遗漏总数首行，复审继续拒绝关闭；现另建v4补齐并以AST拦截任何残留assert，保留全部旧版。在PYTHONOPTIMIZE=1下完整执行六单元，输出一致；另3项测试直接复用笔记原代码，确认REPAIR与错计数在优化0／1均抛错且无成功输出。数据判断为“可带限制采用”，不是全网状态或治理完成。

早期三类型补查两次因数据库OID文本／整数比较失败；第一次未留存原输出，不作为准入证据，第二次原文保留。后续零计数假设被非零结果否定，复核程序拒绝成功回执；随后完整取得上述1,082条才继续。失败脚本与结果均保留，不把探查成功或数量相符单独当作行级准入。

本片不自动放行03-06—30，仍须逐日完整核验六类；03-03／04时间冲突保持失败。规模区可独立注明已验证RIB实际时点的规则已获确认，但起源AS歧义／私用／保留取值没有凭空补造；规模和普通变化仍未在首页交付。旧第31节恢复包只对应旧`cc17…`版本，不能冒称本片`b984…`恢复验收。整体goal仍进行中。

### 33.5 日常本地预览切换

UTC10:08:59，确认旧28491进程身份后正常停止，使用本片`run_preview_v2.py`启动新后端36515；源身份、只读设置、新首页manifest预检通过并回滚关闭连接。28492此前已无监听，本轮重启前端36542代理28491，不声称旧前端始终运行。`main-startup-v2.json`保留实际预检，Git外环境文件未改；共用预览仅在进程内显式选择新消费包。

主端口`main-http-v2.json`追加203次实际请求通过：默认14,410、14条同版本冲突详情、完整分页、类型／等级／地址族、小时桶、27失败日和成功零；旧31日的全量兼容依据仍是前述候选987次对照，主端口没有重新声称做过旧版比较。实际浏览器打开28492默认页，检查390px宽度、截图，并从首页点击进入旧`/events`，真实列表可读，再返回首页；旧事件入口不是同一留存消费版本，未扩大验证全站。

验收后关闭独立浏览器，核对PID与命令后正常终止本片候选35828／34165，28493／28494无监听；主28491／28492保留。没有删除数据、提交、推送、部署或改共享服务。最终再次运行`make test`，仍为392后端、137前端通过；原警告保留。默认日这一切片可用，下一项是逐日验证并扩大日期目录，不把待核实规则直接套成其他25天已准入。

Matt双轴审查收口：Spec初审1项P2（父子对象检查遗漏），代码、反例及新版本绑定复审后未解决0项；Standards初审0项，补审1项P2（笔记优化模式门禁），v3残留同一问题，v4及两模式反例／AST检查复审后未解决0项。初审与历次复审分别保留，不把发现历史改写为始终零问题。最终笔记为v4，消费版本仍为上述v2；审查与笔记修复没有再改消费记录或源库。

## 34. 剩余三月日期的完整读取、逐日核验和离线候选

2026-09-11。**新增14日、290,403条通过离线准入，尚未接入首页**；当前28492仍是第33节32日、50,968条的`b984…`版本，默认03-31为14,410条。没有将此片的候选索引替换为日常输入，也没有放行身份、时间或集合错误。

### 34.1 本次范围和实际结果

首先对03-06—30的25日做有界只读盘点：六类总表共1,115,204条；盘点原文49,759字节，SHA256为`2a17ebd7c4e2d75912e54cadfe47762554942477c920facce7a0c0707039ccf3`。这只是聚合盘点，不是逐行准入。之后按业务日分别执行read-only／repeatable-read事务，源实例、OID16384和系统身份7663836852697006116与第33节相同。完整留存21日494,710条总表及独立事实日窗原文，共1,250,637,206字节。

| 结果 | 日期（均2026-03） | 本片可以使用到哪里 |
|---|---|---|
| 14日GO | 06、08、12、13、15—19、21—25 | 290,403条六类离线消费记录，含233条等级待核实；不是已上线数据 |
| 7日REPAIR | 07、09、10、11、14、20、26 | 已完整读取204,307条总表记录，但整日不生成消费manifest；不删除坏行后放行其余记录 |
| 4日读取失败 | 27—30 | 初次盘点共620,494条；逐行查询均触发90秒语句上限，仅留327字节上下文及失败回执，不能当作完整原行 |

03-03／04不在本次读取范围，既有时间矛盾仍保持失败。RRC25映射依据仍为用户确认，观察覆盖和历史检测版本未知；各日是不同事务，不是共同数据库快照。数量和一致性只支持所选六类记录消费，不证明检测正确性、全网状态或网络实际连通性。

### 34.2 查询和完整性规则

查询按总表类型标签或引用类型的并集选取；候选按完整引用对象／编号／source在月内关联，**不按候选开始时间过滤掉矛盾**。独立查明细自身日窗及六类计数；完整字段多重集合与候选集合双向核对，另核前缀中断小时桶／地址族去重。逐条核对源字段保留、唯一关联、对象、双侧时间／时长和等级；子前缀劫持精确匹配父／子组合对象。AS等级冲突只加注解，其他错误整日不放行。

早期03-06触发idle15秒，失败原文保留；另建查询使用SET LOCAL statement90秒、lock2秒、idle120秒，不改共享配置。03-07／08无压缩读取约91／110秒；09起SSH传输压缩，解压后容量上限仍2GiB、单行64MiB。最后的capture_day_v4.py以统一截止时间覆盖接收、等待、复读和成功回执，客户端上限180秒；超限不生成成功回执。03-08查询完整结束后才停止旧批次控制器，不中断其事务；完整日回执独立保留。

四个大日的失败是SQL超时，不据此断言数据本身不可解析或业务异常。后续需要更小的有界分段读取与全日闭合核对，本片没有执行该替代方案，也未把上限错误补为零。

### 34.3 已定位的实际源问题

- **身份不可按单ASN解释（高）。** 六日共10条AS中断，其总表引用对象和明细asn原值带花括号，包括`{328405}`和`{36040,211612}`等单／多元素写法；并非本次转换增加。不能去括号、选成员或冒称单一ASN。旧Domeye源码提供路径token传入origin／asn的线索，但没有这些历史记录的检测代码版本或逐条MRT定位，不据此确认成因。
- **国家事实未被总表引用（高）。** 03-10国家总表23条、自身日窗事实24条；额外键为source=r、country空串、outage_id=1、16:24:31，等级high。保留原行，不删空国家后宣称集合一致。
- **劫持引用与明细时间不匹配（高）。** 03-20的`80.244.11.0/24`、编号1，总表开始14:11:03，而唯一月内明细开始03-04 19:35:43；两侧结束均03-20 18:29:00、时长04:17:57。候选不在03-20事实日窗，不能自行修正任一侧或将月内候选裁掉。

精确引用、双侧原值、来源摘要及定位差异保存在`failed-source-locations.json`，查找目录、方法、旧代码位置／摘要与未验证边界在同目录`来源追查.md`。定位程序的身份键差异仅用于找出原行，不替代准入程序的完整原字段多重集合核验。没有运行旧代码、加载旧环境或更改检测器。

### 34.4 流式离线构建和候选版本

core_overview_input保留Web直接读取原包64MiB上限；仅显式离线入口按行校验至多2GiB／100万记录，不把整个日窗正文驻留内存。尾部摘要、实际数量、重复引用、来源、冲突引用闭合或文件身份失败抛错；build_index耗尽生成器、复核输入manifest身份后才完成日事务，全部完成后才写目录manifest。Web仍只读按日SQLite，不在启动或请求中构建。

最终audit_day_v3.py对21日重新全量执行，与初审结果一致。14份GO输入经公开`scripts/core_overview/index-core-overview-inputs.py`生成：

`.local/core-overview-validation/march-dates-admission-v1/added-index-v1/manifest.json`

版本为`overview_index_v1_c3cdb22921549990b117fd1189c69d77eeb1f7591c29071f58f61de572814ad6`。逐日SQLite摘要、完整性和记录数通过。**该索引只含新增14日，不能直接代替现用目录：默认03-31及原32日并不在此候选里。**旧32日50,968条的SQLite及27份旧诊断逐文件摘要不变；旧输入和所有被引用版本保留。

### 34.5 验证、交付物及下一片

`make test`为398项后端／137项前端通过；类型生成、typecheck及构建通过，原pandas警告保留。生产新增6项公开CLI／HTTP测试覆盖大包离线构建、直接Web限制及摘要／数量／尾部重复／冲突引用／容量失败不发布目录。本地审计10项fixture加总期限2项fixture共12项通过，真实源检查不混入测试数量。

Matt Standards生产模块初审0；新增脚本审查发现1项P2（收流后超时漏检）及后续1项P3（布尔耗时误认数字）。分别用公开CLI合成反例观察红，再以capture v4／audit v3修复变绿，旧代码和证据保留，复审关闭。Spec相应范围未发现额外问题；笔记及源定位脚本最终补审两轴均0项。详见`双轴审查.md`，不将静态审查当作行级核验。

本片证据根目录为`.local/core-overview-validation/march-dates-admission-v1/`；包含盘点、SQL、读取成功／失败回执、历版工具、初审、`final-audit/`、候选索引及`final-proof.json`。中文[三月剩余日期准入复核笔记](../.local/core-overview-validation/march-dates-admission-v1/三月剩余日期准入复核.ipynb)的5个代码单元完整执行，重做21日最终审计、区分4日读取失败、建立候选及校验旧文件，输出已逐项查看；证据门禁使用显式require并以AST拒绝assert。笔记SHA256为`be464592a6af142208a82b6326c28da6f0a1ef092af46a9a118650c5e32f4a35`。本片独立notebook-runtime没有更改项目依赖锁文件；内核本地通信警告保留，不隐去执行提示。

`main-unchanged-http.json`的一次实际默认请求确认28492仍为`b984…`、14,410条、32可用／27失败日；两项规模仍null。**本片没有新候选的HTTP、浏览器、组合兼容或恢复验收，没有切换日常输入**。下一片先把14个GO日与原32日按原字节组合，并补准确的身份／集合失败提示，再做完整兼容、HTTP和浏览器验收；4个超时日另走有界读取，不因本片完成而放行。规模、普通变化、新版本恢复及整体goal仍未完成。无提交、推送、部署、源库写入、数据删除或外部消息。

## 35. 新增十四日接入首页与失败原因更新

2026-09-11。**本地28492已切换到46日、341,371条六类型留存异常；默认仍03-31、14,410条。**这是第34节14个GO日的接入验收，不是剩余13日、规模、普通变化或整体goal完成。原32日50,968条的文件字节、公开字段和趋势均未改变。

### 35.1 当前目录及准确边界

| 状态 | 日期 | 当前可用范围 |
|---|---|---|
| 原32日保留 | 02-01—28；03-01、02、05、31 | 原50,968条；包括已核验的成功空日，原记录与内容版本不变 |
| 新14日接入 | 03-06、08、12、13、15—19、21—25 | 290,403条，其中233条AS等级待核实；六类型筛选、小时新增中断前缀和同版本详情 |
| 原2日仍失败 | 03-03、04 | 保留旧诊断原字节；03-03有3条AS／9条前缀时间矛盾，03-04有1条劫持结束／时长矛盾 |
| 新7日源校验失败 | 03-07、09、10、11、14、20、26 | 整日不提供统计、列表或详情，不删坏行后局部放行 |
| 4日读取未完成 | 03-27—30 | 本轮只复读第34节超时证据，未重新查询；不提供完整记录数或成功完成回执 |

源RRC25映射仍依据用户确认；覆盖和历史检测版本未知。不同日为独立源事务，不能当共同数据库快照。源库未改；花括号ASN没有去括号或拆分，国家空代码没有删除，劫持时间没有选边修正。

### 35.2 失败诊断的证据与含义

新增`core-overview-diagnostic/v2`，旧v1不放宽。v2只表达本轮已查证的三种字段原因和一种读取失败：

- `source_identity_unresolved`：AS原身份无法按单一ASN解析，6日共10条，逐日2、1、1、2、1、3条。
- `source_population_mismatch`：03-10国家和03-20普通劫持各1条完整原字段多重集合对称差。范围只是该日总表候选与独立明细；不能称全库孤立或缺失。
- `start_time_conflict`：03-20普通劫持1条，保留总表03-20与明细03-04原开始时间。与同日集合差可能是同一记录，原因数不可相加为影响规模。
- `source_read_timeout`：4个大日均为六类型目标查询未完成；`kind=all`，`count/finished_at/receipt_sha256/audit_sha256=null`，单独绑定部分原文与失败日志摘要。目标日窗不表示已读取整日。

两类失败仍返回HTTP503、`state=unavailable`，overview／trend／events均null；带类型、等级筛选或直查详情不能绕过。页面分别使用“源记录校验失败”和“源数据读取未完成”，超时不显示“0条”或虚构读取完成时点。详情弹窗区分原结果／部分原文、成功回执／失败日志，保留查询、选择清单和审计摘要。

Web仅按需复读≤64KiB单日诊断，校验目录摘要、来源、日窗和格式。Git外专用`compose.py`负责离线编译：核对第34节proof／auditor／原文身份、已核验SQL与回执，流式重新计算失败原始身份／开始时间／完整字段多重集合差；不依赖100条例子上限，不执行SQL。前序通用`--diagnostic`入口仍只支持旧预检格式，不能拿任意v2原因文本当作源证据。

### 35.3 加法组合与本地运行

选定目录：`.local/core-overview-validation/march-home-admission-v1/combined-index-v2/manifest.json`。

消费版本：`overview_index_v2_7e0ff4dfe0432ea39e6c5bce6f526a0e335a4b2eeed5a108d64882cc3792f2b3`。

46份日SQLite逐字节复制并复核摘要；原默认日补验旁证和03-03／04诊断原样保留。`composition.json`绑定新旧manifest、前序proof与编译程序；`selections-v2/`绑定11份新诊断的实际原文、SQL、成功审计或失败日志。权限400与摘要不是不可变存储、签名或独立灾备承诺。

首次组合因Python3.10无法用fromisoformat解析源回执5位小数秒而停止，没有生成manifest；`combined-index-v1/`和`selections/`及`compose-attempt-v1.py`保留为失败尝试，不选用。修复采用明确`strptime`格式解析1—6位小数秒，另建上述v2完整组合，没有覆盖已引用结果。

候选28493／28494完成下述核验后，确认旧PID36515与启动命令，再正常停止该本地主进程；新主后端PID46425、28491，前端PID36542、28492继续复用。启动入口为同阶段`run_preview.py`，对manifest固定版本及源库系统身份7663836852697006116／OID16384／bgp_project做只读预检并回滚关闭，成功后启动。连接5秒、语句／事务空闲15秒，仍是本地开发预览，不是共享服务部署；外部0600配置不变。

`main-runtime.json`记录实际切换于2026-09-11T11:25:10Z。旧`b984…`输入及启动入口保留，可在确认端口后手动回退；本轮只换明确本地进程，没有提交、推送、部署、源库写入、数据删除或外部消息。

### 35.4 实际验证及交付物

- `make test`：412项后端、138项前端通过；合同生成、类型检查及构建通过。原有pandas FutureWarning保留。新增14项公开HTTP fixture测试覆盖两种失败、详情门禁和不一致证据拒绝；新增前端API fixture验证超时不能补零。没有把真实数据查询混入单元测试。
- `verify_http.py`实际执行4,334次本地公开HTTP，完整遍历46日341,371条列表；旧32日50,968条逐页比较仍运行旧版的overview／trend／events／query；新增14日290,403条留存正文逐条与SQLite原payload相等，列表有序人口完整一致。新增日各地址族、六类型、等级筛选及类型详情通过；小时趋势另对源SQL独立桶。13失败日全拦截、02-01成功空日、旧目录版本409均通过。
- 实际浏览器核对：1440px／390px新增03-08（47,007条）、03-10双原因失败、03-27读取未完成及证据弹窗。主28492切换后重新访问默认日、03-08及19条等级待核实筛选，并打开AS23860编号47详情，明确保留low／middle与同一`7e0f…`版本；再查看主超时日。候选中点击“事件检索”实际到旧事件列表并读出记录；不将旧库入口解释为同一留存消费版本。页面无运行时JS错误。
- Matt双轴各自独立审查：Standards 0，Spec 0，无未解决项。固定点为本轮`baseline/`，不是把用户脏工作树整体当作本轮改动。两位审查结果分别保留于`双轴审查.md`。
- 中文[首页新增十四日接入复核笔记](../.local/core-overview-validation/march-home-admission-v1/首页新增十四日接入复核.ipynb)4个代码单元完整执行。重新核对46日文件／SQLite、13份诊断及其原文绑定，核对完整HTTP回执，并实际复查主28492默认、新日、两种失败、旧失败和成功空日。笔记不声称在单元中重新执行4,334次请求；门禁使用显式require并以AST拒绝assert。

本节证据根目录：`.local/core-overview-validation/march-home-admission-v1/`。`candidate-http.json`摘要为`a43dc7e0d40cd43b4d7366dde4bba5eb301d42552c6ce307ff25a372bbbadf68`；笔记摘要为`0dbcd95d0ae038d6dd118d1d9d4a2f4be3a62808659fcb56d1fc29764ed182b5`。截图包括`admitted-desktop.png`、`admitted-mobile.png`、`failed-source-desktop.png`、`read-timeout-mobile.png`、`read-timeout-dialog-mobile.png`、`main-default.png`、`main-conflict-detail.png`和`main-timeout.png`。笔记继续用前序独立runtime，未改项目依赖；本地内核通信／关闭提示保留。

### 35.5 仍需推进的缺口

下一片优先处理03-27—30的大日：在现有日窗／月键语义内有界分段读取，并验证全日无遗漏、无重复及独立计数闭合；不能仅延长上限或选部分行冒充完整日。7个源问题和03-03／04保持失败；仅在需裁定新身份或修正规则时另行决策。可见规模、普通变化和匹配`7e0f…`版本的独立恢复尚未完成。第31节旧`cc17…`恢复不覆盖此版本；本节同盘加法副本也不是恢复验收。

收口补充：主28492于2026-09-11T11:34:03Z完成额外3,797次实际HTTP，重新遍历46日341,371条、新14日290,403条原文及13日门禁，`main-http.json`保留。此次不重做旧服务对照，旧50,968条兼容仍由切换前候选回执证明。后来新增的核验／启动脚本、已执行笔记与文档又经Standards／Spec独立补审，各0项未解决。最终重跑412后端／138前端测试、构建和差异检查通过；正常停止候选PID45949／45988，28493／28494无监听，关闭独立浏览器和审查代理；主28491／28492继续保留，没有删除任何数据。

## 36. 四个大日的同事务分段读取与离线准入

2026-09-11。**03-27—30原来的整日查询超时已通过有界分段读取解决；四日完整读回620,494条六类总表记录及同量独立明细。03-27、29、30共522,337条通过业务准入，03-28仍因原AS身份问题失败。**本节先交付离线证据，不表示首页已经扩展；第35节46日、341,371条的`7e0f…`版本仍被选用。

### 36.1 实际读取范围与结果

| 业务日（Asia/Shanghai） | 完整总表记录 | 读取及协议复核耗时 | 最终业务准入 | AS等级待核实 |
|---|---:|---:|---|---:|
| 03-27 | 176,276 | 265.08秒 | GO | 195 |
| 03-28 | 98,157 | 132.31秒 | REPAIR：1条ASN不可解析 | 97（未准入，不供页面消费） |
| 03-29 | 166,370 | 228.31秒 | GO | 186 |
| 03-30 | 179,691 | 242.85秒 | GO | 196 |

成功三日共577条等级待核实，保留总表与明细原值。失败日审计中的98,156条已转换草稿不是可消费人口；`record_not_admissible=1`与`incomplete_conversion=1`是同一失败导致的两种检查结果，不是两条异常或两个受影响网络。

证据目录为`.local/core-overview-validation/large-days-read-v1/`；四份`reads-v1/<日期>/source.jsonl`合计1,263,595,949字节，保留实际SQL、读取回执、原文摘要、程序摘要及事务标识。实际源读取覆盖2026-09-11T11:43:09Z—11:57:52Z，各日独立事务，不是共同历史快照。源集群仍为7663836852697006116／OID16384／bgp_project，source=r对应RRC25依据用户确认；观察覆盖和旧检测版本仍未知。权限400、内容摘要与事务标识不等于不可变存储、签名、独立灾备或历史检测有效性。

### 36.2 没有改变哪些规则

新查询将同一业务日拆为六个连续四小时段，全部位于**一个只读repeatable-read事务**。各段核对时间边界、同一快照标识、六类声明计数和跨段引用唯一性；另查整日总表／明细计数及小时中断前缀桶，不只把分段计数相加。

候选仍按源月表完整引用键关联，**没有新增候选时间过滤**，避免隐藏跨日矛盾。前缀明细主键含ASN，而旧引用不含ASN；查询保留所有候选，不因主键存在就假定旧引用唯一。总表日窗与明细自身日窗独立读取，后续逐字段、完整字段多重集合双向核对。AS等级例外不放宽身份、时间、父子前缀或其他冲突；未知结束不推断为持续中。

`schema.jsonl`保留本轮只读索引／字段检查。分段SQL由已核验的前序`day-v2.sql`生成，前序摘要固定为`1f71e73c…c86ed`，新SQL摘要为`82bcf58307e64c0f1485983004e3485bdae4614cca82554038efb00af8267691`。这是本轮可行的有界读取调整，不声称已证明旧查询超时的唯一性能原因。单语句仍90秒、锁2秒、事务空闲120秒；客户端完整读取及协议复核600秒，上限2GiB／单行64MiB／100万总表引用。不截断、不改源库索引、不在Web请求中执行。

### 36.3 03-28失败证据与工具审查修复

总表原引用`as_outage/2026-03-28 12:49:47/{64508}/1/r`唯一关联`as_outage_202603`明细：source=`r`、asn=`{64508}`、outage_id=1、s_time=`2026-03-28T12:49:47`，双侧等级low，结束／时长均未知。完整字段集合与独立计数相等，但现有单一ASN身份转换拒绝该文本。没有去括号、拆分、改身份或删行，也未把发现范围扩大成全库问题。整日继续不准入；下一片应以此新证据更新旧“读取超时”提示。

Matt初次双轴审查：Standards发现1项P2——最终空日／清单大小门槛失败之前可能已写GO报告；Spec为0项。原`audit_day.py`与首次27／28日审计保留，最终改用另建的`audit_day_v2.py`：先通过最终门槛再写GO，超限保留完整注解并记REPAIR，不截断。两个公开CLI回归分别经历红／绿；协议与准入共21项合成测试通过。最终四日逐行复核位于`notebook-audit-v1/`，27／28日除审计程序摘要外与首次结果相同；没有回写旧证据。

### 36.4 离线候选与未完成接入

已通过公开离线索引命令建立`added-index-v1/manifest.json`，候选版本为`overview_index_v1_596226787831afc5d3b6f59a29d58d30070d34b565f2257e17b0febe3e89d8da`。仅含三个GO日，不能直接替换现用46日目录。三个SQLite完整性与摘要通过，原清单字节一致，522,337条payload逐条与留存正文完全相同；03-28没有消费manifest。

中文[四大日分段读取与准入复核笔记](../.local/core-overview-validation/large-days-read-v1/四大日分段读取与准入复核.ipynb)四个代码单元从头到尾执行通过，涵盖四日最终逐行审计、候选生成和完整原文复读。摘要为`053988edc8c1acac102a6979dd926a9422274bf36d44b528575335153638d11f`；`final-proof.json`记录2026-09-11T12:04:04Z闭合结果。独立notebook-runtime沿用前序环境，项目锁文件未改；本地内核TCP／退出警告保留，不当作证据门禁失败或源查询结果。

旧46日文件和13份诊断摘要全部不变；五次实际28492 HTTP确认默认03-31仍14,410条、两项规模null，四大日仍返回旧版本读取失败。**这是旧主页面未切换的检查，不是新候选HTTP或浏览器验收。**没有新候选Web服务、组合或恢复验收。412项后端／138项前端测试及类型检查、构建通过，原有pandas警告保留；最终双轴补审另见本轮`双轴审查.md`。

收口补审：Standards确认历史P2已闭合、最终0项未解决；Spec最终0项。两轴检查代码、文档、已执行输出与小制品绑定，不冒充再次全量扫描；真实原文复读由上述笔记完成。21项公开CLI测试、412后端／138前端、构建及差异检查最终重跑通过；主28491／28492原进程继续运行。阶段证明摘要为`a5c96d3d98f8ec8ebf97b1150d00bdeb2ab82c24864c3e5956dfb71d6fcdfdfd`。

下一片只将通过的三日按加法方式组合到现用46日，准确替换03-28的失败原因，再做旧字段／趋势兼容、公开HTTP和实际浏览器验收后切换。旧三日超时制品与旧消费版本保留；原其余9个源失败日不放宽。可见规模、普通变化、与后续新消费版本匹配的独立恢复仍未完成；本节不结束整体goal。无源库写入、检测重跑、持续采集、底层连续状态重建、提交、推送、部署、共享服务修改、数据删除或外部消息。

## 37. 新增三大日接入首页与03-28失败诊断更新

2026-09-11。**本地C首页已从46日341,371条扩展至49日863,708条六类留存异常。**新增03-27的176,276条、03-29的166,370条、03-30的179,691条，共522,337条，其中577条AS等级待核实。默认03-31仍14,410条，不改设计或指标口径。

### 37.1 输入选择与失败边界

本轮目录为`.local/core-overview-validation/large-days-home-admission-v1/`，选用`combined-index-v1/manifest.json`，版本`overview_index_v2_f2d20eb5117e23ed1b39011fa8bfbaec8e05a70592a81170d548580c8a563c99`。`composition.json`绑定旧46日`7e0f…`清单、第36节三日候选、前序证明及组合程序；49份日SQLite均按原字节复制，旧46日及其余9份失败诊断摘要不变。旧版本、四大日超时诊断和源证据保留，不覆盖或删除。

03-28完整读取成功与业务准入失败分开记录：`compile_failure.py`校验六段协议、查询／回执／审计／程序摘要，复扫整日原文，确认唯一失败身份`as_outage/2026-03-28 12:49:47/{64508}/1/r`。原ASN文本`{64508}`不去括号、不拆分、不替换，身份问题计数1；98,156条已转换草稿仍不消费。

新`core-overview-diagnostic/v2`沿用既有字段，阶段为`source_field_validation`，原因`source_identity_unresolved`。原文摘要`e61b97dd9aeb2b005e3fdaaa3099082fbdef9cc32802e2cd15a973c5e662a770`；选择清单摘要`25f431cc96f2e8cdddc90da670422b704c4102291f700a8fb177b06d3eabfa0e`；诊断摘要`f4f7788b022d56f80443e622dbfba3493f6c2ec972b2f8bbc503d666b37239f5`。诊断来自完整原文，不用审计示例条数替代全日重算。

当前10个失败日为03-03、04、07、09、10、11、14、20、26、28。统计、趋势、列表和详情全部阻断，筛选不绕过；成功空日仍独立表示0。最新目录没有读取超时日，不意味着所有日期已准入，更不意味着观察完整。

### 37.2 真实对账与页面验收

`candidate-http.json`记录2026-09-11T12:22:58Z完成的12,226次实际HTTP：完整分页核对863,708条列表，切换前与旧服务比较341,371条的公开字段／查询／趋势；新增522,337条SQLite原记录逐条对照留存正文。三个新日按四种地址族核对独立源小时桶，覆盖六类型、等级冲突、详情同版本、10日失败门禁、成功空日与旧版本409。这里证明留存消费和源字段相符，不验证旧检测正确性。

候选1440px／390px检查新增日、等级待核实、详情与失败证据弹层。03-27筛选195条等级冲突时整体176,276条不变；AS27947／编号315详情保留总表low与明细middle，列表与详情消费版本一致。03-28显示ASN身份问题1条、成功读取证据及摘要，而不是零异常或读取超时。候选03-30窄屏179,691条展示通过。

本地主后端于2026-09-11T12:28:15Z改用上述版本：`main-runtime.json`记录PID54569、28491端口及源库只读预检；原28492前端未重启。主页面默认日、03-27桌面与03-28窄屏实际复查：点击03-27的11—12时柱后列表为7,438条，整体176,276条和趋势不变；带该时段切换到失败日仍阻断。截图保存本目录，`main-march27-hour-desktop.png`是短暂加载态，仅`main-march27-hour-loaded-desktop.png`用于最终展示验收。

主页面“事件检索”导航实际进入`/events`，旧只读列表完成默认查询，显示26,063条源总表结果；再由窄屏菜单返回首页，默认03-31恢复14,410条，浏览器错误检查为空。这里只验证导航与该次旧查询可用；旧列表与留存首页不同窗口／版本，不能对合统计，也不表示所有旧详情与特征页面恢复。

### 37.3 可执行复核、测试与审查

中文[首页新增三日接入复核笔记](../.local/core-overview-validation/large-days-home-admission-v1/首页新增三日接入复核.ipynb)四个代码单元实际执行通过：49份SQLite摘要／完整性／人口；从03-28完整原文重新编译并逐字节核对诊断；检查候选HTTP回执及核验程序；对切换后主28492执行7次HTTP，覆盖默认日、新三日、新旧失败日及成功空日。笔记没有声称重新执行12,226次候选请求，不查询源数据库。笔记摘要`fe76ca4a8ddb31b6f5ae7e6e2504134a52921aa42b5478142bec7cb62aa830ad`；`final-proof.json`摘要`49b4091a75aa9a620016df95592a4375b4053d50a16cf3ae371c13fa5c6d6af8`，时间2026-09-11T12:28:39Z。

新增失败编译器以公开CLI进行红／绿测试：合法诊断及拒绝原文变化、审计计数不符、未知错误、读取程序不符、嵌套输出，共6项合成测试通过。完整412项后端／138项前端测试、构建和差异检查通过；本轮未改变生产API／合同／前端代码。原pandas与本地内核TCP／退出警告保留，不伪装为源或业务失败。

Matt双轴初审相对本轮`baseline/`，不是HEAD全部脏差异，commit list为空：Standards与Spec各0项；只核验静态代码及已有小制品，未冒充重跑HTTP或全量扫描。最终文档／已执行输出另作补审，审查范围见本轮`双轴审查.md`。

### 37.4 剩余缺口

剩余10日源身份／时间／集合问题不通过删行或放宽解释处理。可见前缀数、可见起源AS数及普通路由变化仍未知；下一片优先推进已查明的单RIB实际时点规模，按已确认口径核验后接入，不冒充业务日末或连续RouteState。与本次`f2d2…`消费版本匹配的独立恢复仍未完成，第31节旧`cc17…`演练不能代替。

本轮仅本地预览切换、Git外加法制品及说明更新。没有源库写入、检测重跑、持续采集、Peer／Session／RouteState重构、提交、推送、部署、共享服务修改、数据删除或对外消息；整体goal保持进行中。

收口：Standards与Spec最终补审各0项，已检查新增文档、笔记输出、小制品摘要及已加载截图，不冒充重跑。主任务再次执行6项公开CLI测试、412后端／138前端、构建及差异检查，全部通过。核对进程后正常停止候选52998／53024，关闭独立浏览器与两名审查代理；28493／28494无监听，主28491／28492保留。最后一次主HTTP确认仍为`f2d2…`、49／10目录及默认14,410条，两个规模值仍null，没有删除任何数据。

## 38. 单RIB前缀规模消费包准备

2026-09-11。第32节已核验的03-31 08:00 UTC／16:00北京时间RRC25单RIB，现已通过显式离线入口整理成独立小型消费包。**这是规模接入准备，不是首页指标已交付；现用49日`f2d2…`首页未切换，两个规模指标仍null。**

### 38.1 使用口径与制品

前缀按有路由条目的规范Prefix在该文件内去重，区分地址族、跨本文件Peer位置合并：IPv4为1,133,653，IPv6为269,730，双栈相加1,403,383。对应条目数46,486,634／10,164,442／56,651,076另列；被引用的Peer位置数64／57／并集116，不当作前缀数、覆盖率或Session数。

已确认可以独立注明RIB实际时点，不能把16:00结果延续到配置23:59:59。起源AS取值包含私用／保留及AS_SET歧义；本轮已提出“明确AS_SEQUENCE末端ASN去重、保留原值并标注、不拆AS_SET”的产品口径问题，尚未取得该项答复。消费包暂记`origin_metric_state=pending_definition`、各地址族`visible_origin_ases=null`，不先行冻结该口径，也不把null当作功能完成。

消费包`.local/core-overview-validation/rib-scale-consumption-v1/package-v1/`约920KiB，其中有界`summary.json`与`manifest.json`均小于64KiB，另保留两份报告、读取回执及两份核验程序原字节副本。版本`rib_scale_v1_eb9fe611634867d4ee7d1647b4727e3f045ffed8d46d3012912297b43901e7f5`；summary摘要`b633aff4eac4094b6d6a60c5ec12e053a234c2d453be7ee603fddf38eb1af781`。原438,316,014字节文件沿用第32节本地副本，未重复复制或改变；不是上游证据与独立灾备的完整闭包。

### 38.2 显式离线留存与证据范围

新增公开CLI`scripts/rib/retain-rib-scale-input.py`要求显式指定证据目录、原压缩文件、两份报告SHA和新输出目录。读取源原文件到gzip EOF、核对解压字节及压缩摘要；核对完整/非样本标志、读取成功回执、核验程序摘要、单一UTC时点、项目配置范围、双路Prefix集合摘要及逐Peer条目数。拒绝重复/空Prefix、重复Peer条目、越界或非规范位置、负数/布尔计数、JSON重复键、嵌套或既存输出。时间上限120秒，原文件512MiB、解压12GiB、单报告8MiB、输出摘要/清单64KiB；失败不生成最终manifest。

该入口**消费调用者明确选定的已有完整核验，不是独立MRT协议验证器**，不会执行证据目录中的代码，也不会再解释全量AS_PATH。本轮没有重查远端或源数据库。前序全流报告摘要`45e32b46…97f799`与独立结构报告`78d8582d…fb29f`在本轮完整复核；两者绑定同一原文件`7ab60c80…6fa419`。原始核验的完整范围与146条bgpdump样本对照边界仍见第32节，不扩大为全文件bgpdump复算。

### 38.3 真实执行与未完成项

中文[单RIB规模消费准备复核笔记](../.local/core-overview-validation/rib-scale-consumption-v1/单RIB规模消费准备复核.ipynb)四个代码单元已执行：固定前序证据；通过公开CLI完整复读原文件、生成新包；对副本及前缀/条目/Peer统计作独立字面核对；实际HTTP确认主首页仍为49日／10失败日、默认14,410条且规模null。读取回执时间为2026-09-11T12:45:48Z—12:45:49Z，退出0。

笔记摘要`3804d90cd33d993b1f8832f87a1650e523a279e2b11bbdec8fd4be870ccd1a09`；`final-proof.json`摘要`e8c1feee3b64e2f6daa8eb1b89890534b99e1ae446200a2553bde941e50c43be`。执行入口源码另留`retainer-executed-v1.py`，摘要`3e8d19846db37e6aab04a25877a7dbb0021693931fd2e39fd3e3d37f0f342b14`；后续改动不能冒充此包的原生成程序。

公开CLI的成功路径、截断gzip、6类结构异常、嵌套输出和重复JSON键均经历红／绿，10项临时合成测试通过；完整422后端／138前端测试与构建通过，原pandas和本地内核警告保留。所有单元测试只用fixture/临时目录，真实原文件检查由上述授权离线笔记承担。本轮未改HTTP合同、API、前端或运行选择，不声称进行了新规模页面的浏览器验收。Matt双轴审查另记本轮`双轴审查.md`。

下一片须将已验前缀消费绑定到新的首页版本：日期及地址族决定适用快照，列表小时/类型/等级不能重算该快照；显示实际时点及来源。起源口径待确认后新增版本，原包保留。10日源问题、普通路由变化及同版独立恢复仍未完成，整体goal继续。没有提交、推送、部署、共享服务变更、源库写入或数据删除。

收口审查：Standards发现README入口缺少Python解释器的一项P3；改为`backend/.venv/bin/python scripts/rib/retain-rib-scale-input.py --help`并实测退出0，代理复核闭合，剩余0项。Spec为0项。已执行代码、包和笔记未因说明修复而改变；两名审查代理关闭，主28491/28492保持原运行选择，本轮没有临时Web服务。前缀离线包可继续接入；起源消费仍等待所述产品口径确认，不阻断前缀工作。

## 39. 单RIB前缀规模接入首页

2026-09-11。本地主C页现可查看第38节消费包的前缀规模，实际观察时点为03-31 08:00 UTC／16:00北京时间。默认双栈1,403,383，IPv4 1,133,653，IPv6 269,730；不是配置23:59:59、全天、路由条目总数或异常涉及前缀数。起源AS指标仍null／口径待确认，未默认采用待讨论的AS_SEQUENCE末端口径。

### 39.1 消费版本与兼容

新目录`.local/core-overview-validation/rib-scale-home-v1/combined-index-v1/manifest.json`，版本`overview_index_v2_37b9f13273d73b5c23572f6314d4a03aec327368f375cd3c22931b2d98f762a1`。原49日`f2d2…`异常／诊断按原字节复制，新清单通过可选`scale`绑定独立RIB包；规模版本仍为`rib_scale_v1_eb9fe611634867d4ee7d1647b4727e3f045ffed8d46d3012912297b43901e7f5`，未重新解释MRT。原件和被引用版本保留，49日863,708条六类记录及10失败日不变。

公开离线入口`scripts/core_overview/bind-core-overview-scale.py`接受`--index`、`--scale`及独立新`--output`。验证来源／配置／实际时间／双栈和、原清单、5份证据及日文件SHA；复制完成后才写最终manifest，失败副本不自动删除或假称可消费。拒绝覆盖、嵌套输出和未绑定路径，120秒上限；不连接源库、不执行证据代码、不读取原MRT。该副本约3.9GiB，同盘不同文件实体；不是灾备或独立环境恢复。

### 39.2 HTTP与页面边界

HTTP复用原首页接口，新增可选`metadata.scale`，已有`overview.visible_prefixes`从仅null扩为非负整数／null。旧无绑定版本保持原形状，目录版本变化时旧列表／详情版本请求仍409；合同、生成类型与前端客户端同步。

仅异常日可用且所选业务日等于实际RIB本地日期时提供对应地址族数值。缺快照日为`date_not_retained`，未知地址族为`family_not_supported`，规模清单／摘要校验失败为`unavailable`，数值保持null。小时／类型／等级／检索仅作用异常列表，不重算快照。异常失败日维持503及概况／趋势／列表null，详情不可绕过；RIB不能给该日提供部分成功统计。

请求只读规模manifest与summary（各≤64KiB），校验SHA、来源、时点与计数闭合；五份审计证据只核对绑定声明，其完整字节复核由离线binder承担。**Web成功不是重新完成MRT或审计证据复核的证明。** 文件缺失／损坏，包括循环软链，只使规模不可用，不拖垮原异常查询。页面卡片注明“单RIB · 03-31 16:00（Asia/Shanghai）”，来源弹窗提供原文件SHA、消费／规模版本、文件内Peer位置数及限制。

### 39.3 实际验收与收口

本片证据目录`.local/core-overview-validation/rib-scale-home-v1/`：

- `copy-proof.json`：独立复读59份异常／诊断文件及7份规模文件，共66份；摘要与前序原件相同，863,708条未改变；不包含原MRT的新增复制。
- `http-proof-final.json`：最终代码在候选28493对照原主28491，59日的响应状态、旧公开字段和24小时趋势相同；默认四地址族值、列表筛选不重算、同版详情及旧版本409均通过。实际完成时点2026-09-11T13:10:36Z。这不是全量逐条HTTP重读，新旧日文件字节相同另由上项证明。
- `checks-final.json`及日志：20项新增规模公开HTTP／离线CLI测试包含边界、损坏隔离、循环软链、无审计文件请求与离线审计损坏拒绝；完整442项后端／140项前端测试、构建、差异检查通过。1项原pandas警告保留，不当作业务失败。
- `双轴审查.md`：Standards 0项；Spec初审2项P2（请求读取过多证据、循环软链异常逸出），主任务先复现再修复，代理复审剩余0项。代理只静态核对，不冒充重跑真实检查。
- 实际候选浏览器完成IPv4／IPv6／未知地址族、03-30无快照及03-28失败日切换，桌面截图`candidate-desktop.png`已检查。agent-browser后续截图卡住，未把失败／空白截图当证据；改用独立内置浏览器完成390×844画面核验，实际`scrollWidth=390`、卡片宽351，无横向溢出；图像在本次任务中。临时视口已复原。

候选验收后主28491已于2026-09-11T13:15:03Z启动新版本，运行回执`main-runtime.json`；前端28492原进程沿用。主代理HTTP及独立实际浏览器均确认默认1,403,383前缀、14,410异常与`37b9…`版本，未改用户已有页签的日期。启动仍复用既有明确绑定的只读开发环境和源身份预检，外部配置未改；仅本地预览，不是发布／部署。

尚缺：10日源身份／时间／集合问题、起源AS产品口径与消费、普通路由变化及新版本的独立环境恢复。本轮没有源库写入、检测重跑、持续采集、Peer／Session／RouteState重构、提交、推送、共享服务变更、外发消息或数据删除；整体goal继续，前缀这一个切片完成。

收尾：临时候选后端10999与前端10252正常停止，agent-browser验收会话及临时内置页签关闭；保留用户原页签，未修改其日期选择。主28491（PID11847）／28492（PID36542）继续运行。`final-proof.json`记录主代理四地址族、无快照日、失败日、候选端口关闭及本轮代码／证据SHA；未清理任何数据文件。

## 40. 当前49日与前缀规模的同版独立恢复

2026-09-11。第39节当前`37b9…`消费版本已完成独立目录恢复，补齐旧第31节`cc17…`包不覆盖当前数据的问题。本片不修改生产代码、接口、前端或输入选择；49可用日863,708条六类异常、10失败日不变，默认03-31为14,410条与16:00北京时间单RIB前缀1,403,383。起源AS仍null／口径待确认，普通变化仍不可用。

### 40.1 固定材料与独立环境

阶段目录为`.local/core-overview-validation/current-recovery-v1/`，保留本片短规格、三份文档起始基线、实际脚本和回执。冻结268份实际工作树源码／配置／锁文件及67份消费文件；后者为总manifest、49份日SQLite、10份诊断和7份规模绑定材料。归档含未提交代码，不以HEAD冒充当前源码；不含原依赖、`.env`、凭据、日志、DB／INFO／P0或MRT。包内README保持冻结时点，恢复步骤以包外中文说明为准。

`domeye-c-consumer-current-v1.tar.gz`为329,331,594字节，SHA256 `4a77255b9b4d9e1a6c5834db52362bc187e6e3db61860a6e2f1d404e268c9f11`；bundle清单SHA256 `5ca80a9b323dbe575d289b567f8e58305f214289832c8f1656735fdccf427c6a`，逐文件绑定335份材料。消费版本完整值为`overview_index_v2_37b9f13273d73b5c23572f6314d4a03aec327368f375cd3c22931b2d98f762a1`；规模版本与第39节一致。旧恢复包与当前主输入均不覆盖。

解包至新建临时目录`/private/var/folders/yb/k98mxs2j31g_46bk5x8kd9640000gn/T/domeye-current-recovery-ot3kplbo`，检查安全路径／普通文件、逐文件SHA及独立文件实体。清理环境变量后执行`uv sync --frozen --link-mode copy`、`npm ci --no-audit --no-fund`、`bash scripts/frontend.sh build`及`make test`，新建venv与node_modules；未复制原依赖。四步均退出0，442项后端／140项前端通过；首次导入的4项旧正则转义及1项pandas警告保留。实际安装／构建／测试日志与摘要在`setup-proof.json`中绑定。

复用原字节`serve_guarded.py`（SHA256 `ff61f2331bb0a8b9bf27ba73a157747a95c00a6fae795f206a66da82425bb9fd`），恢复后端28495启动前校验335份文件、4项拒绝访问负例与领域模块路径。拦截Python层原工作树／源目录、其他SQLite、PostgreSQL和外连；不是OS沙箱。前端28496由新构建dist代理独立后端。仍沿用本机uv／Python／Node／npm和下载缓存，不宣称全新OS或断网安装。

### 40.2 运行、故障与页面验收

`http-proof.json`于13:28:56 UTC记录94组代理HTTP与主28492逐响应对账：59日目录、四地址族、六类筛选与详情、等级待核实／高等级、小时、排序、分页、无匹配搜索及失败日详情；另对恢复实例检查错误版本409。200消费结果及失败日列表要求版本字段；失败详情的既有503合同只含状态／说明。首次核验器误要求该503含版本，已修正核验器后完整重跑；没有修改生产合同或因该次误判移动文件。

只暂移新解包副本的文件，并在finally放回：规模摘要缺失时异常仍14,410、规模null／unavailable；03-31日SQLite缺失时整日503、统计null。两次均核对原主服务不受影响，放回后的SHA不变且同版恢复200。对应摘要与公开响应留在HTTP回执，不把故障空值当成功零，不回退读取原工作树。

实际独立浏览器检查默认前缀1,403,383／异常14,410、IPv6前缀269,730／异常8,352、实际16:00与版本详情；03-30异常179,691但无规模快照，03-28源ASN失败且统计／列表不可用。趋势锚点、旧事件导航与返回通过；无凭据恢复实例的旧事件页明确显示查询失败／记录数未知，不声称恢复旧数据库。1280×720与390×844截图目视通过，document滚动宽度分别1280、390。截图在任务内呈现而未另存图片，`browser-proof.json`为人工整理的实际交互记录，不是自动重放证明；独立页签已关闭，用户页签未操作。

### 40.3 工作树外保管与剩余边界

归档、入口、[中文恢复说明](../.local/core-overview-validation/current-recovery-v1/恢复说明.md)、安装日志和验证回执共12份材料，已复制至工作树外私有目录`/Users/botongwu/.codex/recovery-drills/domeye-c-20260911-current-v1/`，各文件0600、目录0700。逐文件摘要和独立inode验证通过；`保管回执.json`另行生成，其SHA256为`8c2af8c63a5125183c6a9c4375874209367caf9c79998a3781941169145b5c21`，本片`custody-proof.json`保存绑定。没有覆盖旧v1／v2材料或删除临时解包文件。

这是同机同磁盘的C消费恢复：不含源DB／INFO／P0、MRT及完整上游闭包，不证明不可变备份、长期保管责任、异地灾备、完整观察覆盖或全站可用。本片没有提交、推送、部署、共享服务变更、源库写入、检测重跑或底层状态重建。剩余10日源问题、起源AS产品口径与普通路由变化仍未完成，整体goal保持进行中。

临时服务收口：核对恢复目录命令后，对后端13116、前端13218发送TERM，两个执行会话均退出143；没有停止或切换主28491／28492。恢复目录及全部材料保留，未删除任何数据。

## 41. 剩余失败日期的原备份对照与裁剪来源追溯

2026-09-11。本轮回到优先级最高的剩余10日，查证旧备份能否提供一致替代证据。结论为**REPAIR：不能通过换回该备份直接准入这些日期**；已查明部分来源差异，不表示日期功能完成。主`37b9…`版本仍49可用／10失败日，默认14,410异常、16:00单RIB前缀1,403,383，起源AS仍null。无生产代码、接口、页面或输入选择变更。

### 41.1 找到并实际读取了什么

只读目录盘点覆盖`/home/bgpdata`下Domeye相关项目，以及Domeye-Core-data／dev-data／artifacts／governance的已发现候选路径；没有把目录存在当作内容可用。找到`Domeye-Core-artifacts/work/20260717T124354Z/source-full.pg12.custom.dump`，3,029,328,735字节，SHA256 `092ef641aeb7a88507a0062082a35cde7d162e69880471837db1321d5a212c96`，与发布元数据及原dump元数据一致；读取前后均完整复算SHA。目录清单确认5张三月表为直接TABLE DATA项，不将无数据的父表当作已读取。

2026-09-11T13:52:28—13:53:17Z，以容器自带`pg_restore --data-only --file=-`只输出文本，完整扫描`as_outage_202603`16,041行、`country_outage_202603`732行、`event_table_202603`1,142,937行、`hijack_202603`11,300行及`prefix_outage_202603`1,064,032行。导出文本流1,454,438,768字节，没有保存整份流或执行其中SQL，只留97条问题键相关字段、COPY行序号／长度／摘要，正文40,028字节。该部分字段投影不能替代完整日原文或全表一致性验收。

备份读取180秒、导出流12GiB／单行64MiB／选中行250条上限；实际48.69秒完成，五表COPY均闭合，进程退出0，备份实体与摘要未变。v1因缺少当前pg_restore要求的`--file=-`失败，失败输出／程序原样保留；无SQL执行或数据变动，修正参数后另建v2成功结果，不覆盖失败证据。

随后按独立SQL在当前`domeye_core_dev_pg/bgp_project`执行一次只读repeatable-read事务，复读同类问题键投影96条，25,280字节。回执确认集群7663836852697006116／OID16384、只读状态和回滚闭合，单语句15秒、锁2秒、客户端60秒；完成于13:54:39Z。原备份与当前库不属同一时点，共同96条**所选字段**完全一致，原备份独有一条、当前独有零条；不外推其他字段或全库。

### 41.2 对首页有何影响

| 问题与粒度 | 新增证据 | 影响与下一步 |
|---|---|---|
| 03-03：3条AS＋9条前缀中断事实的负时间 | 原备份与当前选定字段一致；12条起止差均为负且等于记载时长，不是新消费转换造成 | 高风险、高置信度字段冲突；需要一致且可追溯的时间依据，不能交换起止、夹零或择一侧 |
| 03-04／20：两个总表引用指向同一个劫持明细键 | `(r,80.244.11.0/24,1)`在原备份已是开始03-04、结束03-20、时长04:17:57；实际起止差1,378,397秒而记载15,477秒 | 高风险、高置信度关联／时间冲突；需区分两个引用的原明细证据，不因较早备份存在该值就认定为真 |
| 03-07／09／10／11／14／26／28：11条AS中断花括号身份 | 原备份与当前原文本一致；9条单值写法、2条多值写法，不是新转换器增加 | 高风险、高置信度身份不能按现行单ASN口径消费；单值也不自动去括号，多值不选成员 |
| 03-10：一条空国家总表引用与事实集合差 | 原备份国家总表／事实均24条；当前为23／24。原引用`country_outage/2026-03-10 16:24:31//1/r`已定位，当前事实country仍为空 | 高风险、高置信度来源差；与历史发布裁剪记录相符。但找回引用不等于找回有效国家，不能补入后宣称整日可用 |

这些分组日期有重叠，不能相加成影响天数；97／96是为追踪问题而选择的字段投影，不作为错误率或受影响网络数分母。旧备份已含相同身份／时间异常，只说明问题早于本轮消费重构，不证明检测程序的具体历史版本、成因、责任或真实网络异常。

### 41.3 国家记录裁剪的证据边界

原COPY数据中第166006行（零基、仅此表数据行）保留上述空国家引用，完整COPY行SHA为`d244693431c5153c65008e5670df08e2d25eb008c10097385ef5900cab1bb998`。发布`database-manifest.json`明确记载三月裁剪1条country_outage畸形总表记录；当前事实仍保留同时间、source=r、country空串、outage_id=1。三者一致，支持“发布裁剪留下该集合差”的解释。

另只读保存当下`/home/bgpdata/Domeye-Core/deploy/database/sql/prune.sql`，其现行逻辑包含裁剪空对象引用的条件，SHA为`4b39b462571b42b5ec099eb15448b36990fa1e1447038fa06d1f268fd27c038a`。本轮未执行该SQL，也未确认此文件就是当年执行的确切版本；因此不把机制佐证说成历史逐行操作日志。亦没有重跑裁剪、删除原行或启动恢复数据库。

### 41.4 交付与未决取舍

证据目录`.local/core-overview-validation/remaining-source-repair-v1/`包含原备份TOC、元数据与查询原文、成功／失败读取回执、当前问题键SQL、`analysis.json`及[已执行复核笔记](../.local/core-overview-validation/remaining-source-repair-v1/失败日期来源与原备份对照.ipynb)。笔记4个代码单元重算97／96投影、国家24／24→23／24、12条负时长算术、劫持时间矛盾，并对主页面执行默认及3个失败日HTTP检查。笔记未重新声称执行远端全表导出；本地内核TCP提示保留，项目锁文件未改。

已向用户提出一项产品取舍：是否允许将11条花括号身份记录作为“对象待核实的原始检测记录”消费，原值不拆分、不计入AS数量、不声称单一AS中断。**尚未获确认，未实施**；即使接受，也只是解除身份解释门槛，须另建版本并完成相关日期全量准入与页面验收，时间和国家问题不随之放行。现行单ASN策略下继续阻断，不反复重读同一备份期待问题消失。

本片只完成来源追溯与修复条件收敛，未新增可用日期。原时间、身份、发布与消费版本保留；起源AS统计和普通路由变化仍待解决，整体goal继续。没有提交、推送、部署、共享服务变更、源库写入、持续采集或状态重建。

## 42. AS_SET 代码根因、隔离修复与单案例历史影响

2026-09-12回填。用户要求从代码查根因，不以新增“对象待核实”分类替代修复；第41.4节的分类建议不是已批准的实现方向。独立任务`AS_SET 一致性修复与单案例验收`已完成两个有限切片，父任务现已复读其47份清单材料的字节数和SHA，重新计数目标UPDATE，并检查当前首页。**固定历史版本的代码修复为GO；指定旧记录的处置依据为REPAIR；旧事件精确重算为STOP。** 不表示11条均误报或当前Core已修复。

### 42.1 根因与修复范围

旧`6f01237fa6662a3329e7d938ddd772c6a11bdb0d`的RIB入口接纳末端AS_SET表达，origin辅助函数可以返回`{328405}`；UPDATE分发入口却跳过含花括号的宣告，撤回照常处理。前缀／AS聚合沿用origin，数据库ASN文本列可以保留集合表达。指定两文件56条目标观察在原入口分发为0宣告／16撤回；修复后40宣告／16撤回完整留存。该机制不能证明每条历史记录实际运行的是这一版本。

修复仅位于另一个本地独立工作树`/Users/botongwu/.codex/worktrees/361b/domeye-new/.local/as-set-consistency/repair`，原始末端与跳过私用AS后的归属解释分开；RIB／UPDATE一致保留观察，各检测算法仍按支持范围分流，归属不明不冒充单ASN，不跨失去适用性的区间自动续算旧事件。用户已确认跳过私用AS为主动设计，未将它取消。10项回归及该任务的Standards／Spec复审完成；此次父任务复读不冒称重新执行这些测试。

代码和测试补丁SHA256 `4a75e1b8c5d5a82456beda8a56e533a9af710991ecdfd5ae7d4ead55f3379585`，绑定上述历史基线；尚未移植到当前Domeye-Core，也未改变首页、旧输入、数据库或检测版本。详见[修复交付说明](/Users/botongwu/.codex/worktrees/361b/domeye-new/.local/as-set-consistency/交付说明.md)。

### 42.2 指定案例的历史证据

范围仅RRC25、`45.240.57.0/24`、`as_outage/2026-03-07 05:53:57/{328405}/1/r`。业务时间为Asia/Shanghai，对应UTC 2026-03-06 21:53:57。

- 候选`bview.20260306.0800.gz`，SHA256 `1ed625243c03d0c49cdda1719dd0dd7fb1206cb0308a3201ab1898c1bb2f2460`，含14条目标Peer路径；原事件8条前置路径与其中内容相符，不足以证明历史初态和分母。
- 08:00—22:00 UTC的168份UPDATE读到gzip/MRT framing结束，读取前后SHA相同，合计33,959,118条MRT记录。17:10文件存在5条非目标属性截断，已留存原二进制和独立解码警告；不能将该扫描称为整窗业务有效或采集无遗漏。
- 全窗目标UPDATE为46宣告／16撤回，共62条；其中56条属于前一修复切片，新增6条是更早宣告。14条RIB和62条UPDATE逐条与bgpdump对应，观察数不等于状态变化数。
- 14个IP／AS对唯一匹配候选Peer表，但18条相关会话记录包含14条STATE、3条OPEN、1条NOTIFICATION；两个Peer发生重连，状态链存在5处不接续，不能把整个窗口看作连续Session。
- 进程32082的日志记录168文件开始／完成处理，并记录目标前缀中断开始；未绑定实际代码SHA、有效阈值及当时文件字节。日志是前缀事件，导出是AS事件，相同id不能补足跨表血缘；无时区日志不直接与UTC提交时间比较。

因此只建议保留旧记录供审计，不把`{328405}`去括号变为ASN328405，不填写恢复时间、duration，不声称持续中断或假阳性。此建议尚未改变消费门禁。旧检测版本按本goal不继续追补；现有缺口留档。若另做新版本重建，须先确认单案例的范围、会话缺口和分母规则，不能以“代码已修复”默认授权。

完整证据见[历史核查结论](/Users/botongwu/.codex/worktrees/361b/domeye-new/.local/as-set-consistency/history-impact-v1/核查结论.md)。其`result.json` SHA256 `e51bffea8ba2de1348e80ff387ed5f139d98cad97505c568327fe518a937c94e`；47份制品清单SHA256 `55c539bf4a4f41c533b3c0c2d436a8f8805fb2f890a10afcf26a67ceec384b49`。父任务复核回执位于`.local/core-overview-validation/goal-resume-20260912-3Nj0d8/handoff-verification.json`；本轮未重跑远端扫描。

### 42.3 对首页后续工作的约束

本轮实际HTTP复读：默认03-31仍14,410条异常、1,403,383个前缀、起源AS为null；03-03／07／28仍503且overview为null，消费版本仍`37b9…`。未新增可用日期，不由单例补证放行其他失败日。

用户另已确认首页起源AS统计使用从末端跳过私用AS的明确单ASN归属并集，集合／联盟段不拆分、不越过猜测，原始值另存；验收覆盖公开离线留存命令、只读API和首页。这解除起源指标的产品口径待决策项，但完整RIB复读、消费绑定、API／页面和匹配版本恢复仍需实测。其实现不依赖上述历史事件的精确还原，也不授权连续状态重建。

## 43. 明确归属起源AS的离线留存与首页接入

2026-09-12。用户确认的起源归属口径已跑通公开离线命令、只读API与本地C首页。当前消费版本`overview_index_v2_410cbacafe604372c273532c3872fb7b109495579213ea6155820e0f7e1d97c9`；阶段目录`.local/core-overview-validation/goal-resume-20260912-3Nj0d8/`，实际输入`combined-index-v2/manifest.json`。不是全网规模或持续状态，也不解除10日旧源问题。

### 43.1 来源、归属与完整复读

沿用03-31 08:00UTC／16:00北京时间RRC25 RIB，源SHA256 `7ab60c80563b22350445b934ed055e44a0ffd98503de73f55f6b7dd20f6fa419`，438,316,014压缩字节／4,353,737,106解压字节。公开入口`scripts/rib/retain-rib-origin-input.py`仅接受显式源文件、源SHA及独立新输出；结构、长度、Peer位置、属性段、gzip EOF、运行中源文件与代码变化均校验。TABLE_DUMP_V2 AS_PATH按四字节ASN解码，不运行旧应用或检测器。

从末端AS_SEQUENCE跳过64512—65535及4200000000—4294967294；65535明确为旧规则附带排除的保留值，不错误称作RFC私有AS。0、23456、4294967295停止归属；AS_SET／联盟段不拆分、不越过歧义猜测。路径中较早出现集合，但末端已有明确序列ASN，可以归属末端。原AS_PATH／AS4_PATH字节、原始末端、归属ASN及原因分别保留；AS4_PATH在结构有效但缺乏另行解释规则时不猜测归属。解释版本`rib-attributed-origin/private-skip-v1`。

| 地址族 | 可见前缀 | RIB路由条目 | 明确归属ASN去重 | 未明确归属条目 |
| --- | ---: | ---: | ---: | ---: |
| IPv4 | 1,133,653 | 46,486,634 | 78,236 | 7,278 |
| IPv6 | 269,730 | 10,164,442 | 36,483 | 1,151 |
| 全部 | 1,403,383 | 56,651,076 | 85,565 | 8,429 |

双栈ASN取集合并集，不相加；未归属条目不是缺测ASN数量。本输入未归属原因均为AS_SET；2,780条经私用值跳过获得归属。7,232,160种原路径保存在729,419,776字节`paths.sqlite`，只写新离线制品，不是源数据库。原始末端ASN集合85,730属于不同指标，不直接删私用值冒充归属集合。

首次全内存目录方案触及50万路径上限，明确失败且未产出最终清单；随后改为有界SQLite目录，批缓存3万路径／32MiB，文件4GiB、总运行1200秒等上限。v2完整复读后用独立原路径解释程序遍历全部7,232,160条目录行，对照ASN集合、原因及每族条目；并与既有完整RIB报告核对Prefix集合SHA、逐Peer条目与完整帧数。这不是第二套独立全流MRT解码。修复运行代码绑定、原子最终清单、AS4段损坏及atime误判后，v3再次完整复读，四份数据制品字节与v2一致。

最终起源版本`rib_origin_v1_7b315aedf24fb2fd0eef0eabfae5e352b563a08974a1e23d1abe3eb18d40e5d1`，位于`origin-candidate-v3/`；`origin-independent-verification.json`及`candidate-http-v2.json`记录独立目录核验与两次输出一致性。程序／CLI的实际v2、v3副本另存；旧候选保留。

### 43.2 新版消费与兼容

公开`scripts/core_overview/bind-core-overview-origin.py`给旧`37b9…`前缀版添加可选`origin`绑定，复制原59份日数据／诊断、7份前缀包和5份起源包；不覆盖原输入。固定包根、SHA／字节数、合法ASN成员／排序去重、SQLite分母、同源文件／时点／Prefix集合及执行代码前后摘要均校验。最终清单使用临时名加原子无覆盖链接。审查前v1候选`515a…`未作为主输入，校验修复后另建v2`410c…`；起源数值与数据字节未变。

Web只新增读取origin清单与summary，各≤64KiB；不读取原路径SQLite／ASN成员文件，不访问MRT、不生产数据。日期／地址族决定规模适用性，列表局部筛选不重算。起源损坏只使该指标null／unavailable，前缀与异常保留；异常日失败仍拦截全部统计。OpenAPI、生成类型、前端校验及卡片同步，旧无origin包仍可用。旧前缀包自身保留当时“起源待确认”历史限制，当前起源状态以独立origin版本与字段为准，不回写旧包。

### 43.3 实际验收与剩余边界

- TDD公开CLI／HTTP覆盖已确认归属、双栈并集、原值留存、非法成员、截断、篡改、软链越界、代码变动、原子清单、旧版本409、失败日与日期／地址族门禁。测试临时包曾漏复制`__init__.py`，完整测试cwd下错误加载真实包；已按实证修复测试隔离，不弱化生产校验。最终477项后端／141项前端、构建及差异检查通过，既有pandas警告保留。
- `candidate-http-v2.json`实际对照59日目录、四地址族、六类筛选／详情、等级、小时、排序分页与无匹配，共81组新旧HTTP结果，旧记录、趋势及原详情不变；66份旧文件完整SHA复读、其中59份日文件独立inode通过。审查发现其余7份前缀包实体未核对，随后`verify_prefix_copy_entities.py`实际补读7份，`prefix-copy-inodes.json`补齐SHA与双侧dev/inode；不回写旧回执。不是再次遍历源库全表。
- 独立agent-browser实际检查1440×1000默认页、起源说明的规则／8,429／源SHA和双版本，390×844 IPv6显示36,483；03-30无快照为空值，03-28失败全部空值；滚动宽度390，无页面JS错误。截图`candidate-desktop.png`、`candidate-mobile.png`已目视复核。一次等待使用了不一致的预期文案，后以实际DOM核对“此日无已验证的RIB快照”，不算页面故障。
- Standards／Spec两轴复审0项未解决；已关闭父目录软链、共享复制代码未绑定和非法ASN成员三项P2。审查本身不冒充真实数据重跑。
- 核对本地主进程后正常替换后端，`main-runtime.json`记录2026-09-11T17:12:18Z、PID39638／28491；原前端28492保留。主页实际显示1,403,383前缀、85,565起源、14,410异常。未修改用户原有页签、外部配置或共享服务。

新版本独立恢复尚在验证；第40节旧37b9恢复不能代替本版本。剩余10日源问题、普通路由变化及其有限状态证据仍未交付；整体goal继续。没有源库写入、持续采集、旧检测重跑、底层状态重建、提交／推送／部署或数据删除。

## 44. 起源消费版本的独立恢复与保管

2026-09-12，补齐第43节`410c…`起源消费版的独立恢复。本片不改变该版源代码／业务口径／输入选择；49可用日863,708条、10失败日、默认前缀1,403,383／起源85,565／异常14,410不变。阶段目录为上一节目录下`recovery-v1/`。

冻结273份实际工作树源码／配置／锁文件，含未提交改动；另72份消费文件为总manifest、49日SQLite、10诊断、7前缀材料、5起源材料，逐文件绑定共345份。归档`domeye-c-consumer-origin-v1.tar.gz`为541,500,289字节、SHA256 `0c2ec85a3b81fb826615546898591270d0d71665fe8d9ac45270a5da82131e0c`，包清单SHA256 `34f4896c1afd9253163eda3efa4f75769a3651399a5e2316854e6091cd9a3984`。不包含凭据、原依赖、日志、源DB／INFO／P0或原MRT。包内README为冻结时点，以包外中文恢复说明为准。

实际解包目录`/private/var/folders/yb/k98mxs2j31g_46bk5x8kd9640000gn/T/domeye-origin-recovery-yu28qe8u`，逐文件SHA、安全路径及独立文件实体核对通过。清理继承环境后`uv sync --frozen --link-mode copy`、`npm ci`、build、test四步退出0；477后端／141前端通过，4项旧正则转义与1项pandas警告保留。新venv／node_modules不复制旧依赖；本机工具链和下载缓存仍沿用。

恢复后端28495复用隔离入口原字节`ff61f2331bb0a8b9bf27ba73a157747a95c00a6fae795f206a66da82425bb9fd`，启动前核对345文件、4项拒绝访问负例及领域模块路径。Python层拦截原工作树／源目录、其他SQLite、PostgreSQL与外连；不是OS沙箱。恢复前端28496使用新构建dist，不依赖原工作树Vite。

`http-proof.json`于2026-09-11T17:18:21Z记录95组代理HTTP与当前主28492逐响应对账，包括59日、四地址族、六类详情、局部筛选、失败日详情与错误版本409。仅暂移新恢复目录文件且finally放回：起源摘要缺失只使起源null，前缀与异常可用；前缀摘要缺失使两项规模null但异常可用；03-31日SQLite缺失使全日503。三次均确认主输入不受影响，恢复后同版200、文件SHA不变。

独立agent-browser实际核对1280×800默认85,565和390×844 IPv6 36,483，窄屏滚动宽度390；03-30两项规模为空，03-28全部统计不可用。`desktop.png`、`mobile.png`保存并已目视检查。旧事件检索导航到达，但无数据库实例明确显示“查询失败·记录数未知”，不声称全站有业务数据。`browser-proof.json`为人工整理的真实交互记录，不冒充自动重放。

归档、隔离入口、[中文恢复说明](../.local/core-overview-validation/goal-resume-20260912-3Nj0d8/recovery-v1/恢复说明.md)、安装／构建／测试日志及各项验证回执、截图，共14份材料复制至工作树外私有目录`/Users/botongwu/.codex/recovery-drills/domeye-c-20260912-origin-v1/`。逐文件SHA及独立inode通过，文件0600／目录0700；外部`保管回执.json`SHA256为`07c6ede1a4100c2d4a92943b16b8335cf8cc2a09d8f08b615b0b789a141cd76a`，阶段`custody-proof.json`记录绑定。旧包、恢复目录与原输入均保留。

只完成同机同盘C消费恢复，不证明异地灾备、不可变源快照、长期保管责任、全新OS／断网安装或上游完整生产证据恢复。10日源问题及普通路由变化仍未交付；没有源库写入、检测／状态重建、共享服务变更、提交／推送／部署或数据删除，整体goal继续。

收口：Standards与Spec确认有限切片及7份inode补证无未解决问题；复核范围与实测边界见本轮`双轴审查.md`。`final-proof.json`于2026-09-11T17:26:04Z再次核对266非文档源码、14外部材料与7次主HTTP。核对PID后正常关闭候选39042／38302和恢复40702／40736，28493—28496无监听，主39638／36542继续提供28491／28492。独立浏览器已关闭，恢复目录与旧输入未删除。

## 45. 普通路径比较的候选预检与待确认范围

2026-09-12。起源规模交付后继续goal第4项，只做现有制品查找、压缩完整性及首Peer表预检。**旧00:00 UTC输入继续STOP；新候选为REPAIR，尚未生成路由比较结果。** 阶段目录`.local/core-overview-validation/ordinary-change-next-20260912-7x72fz/`；产品代码、合同、运行输入与服务未变。

### 45.1 状态制品查找

远端13个Domeye相关根下，按RouteState、PeerSession、seed、S1/S2 manifest、RouteEvent和checkpoint命名查找，实际扫描51,419项／7,334目录、6.86秒；深度9、60,000项、40秒、1,200命中上限未触发，无读取错误。排除源码、数据库实体、原始MRT及软链等，完整排除规则在`inventory-proof.json`；**不是全文件系统不存在证明**。

75项命中中68项为同名seed声明，其他为既知RouteEvent pilot／核对摘要和两份源码线索；命名规则下没有RouteState／PeerSession／S1/S2 manifest实物命中。只选读最新所列发布目录中一份`iran-rrc25-seed-spool-attestation-20260227.json`，673字节，SHA256 `2124abe64a3d5e718cec2ee479c3e0dba87bd04b8bfd84930f216ec600727103`。它只有原seed压缩／解压摘要及测量声明，没有状态数据路径或状态时点；不声称其余67份内容相同。原已引用`/home/bgpdata/Domeye-Core-dev-data/research-runs/rrc25-route-state-224-310-s2-0a0a322`及父目录再次明确不存在；没有遍历其他数据库实例或将声明冒充状态实物。

### 45.2 首表可配对不等于输入可用

初检03-31 00:00／08:00 UTC两份文件，各有120条首Peer表记录，原始BGP ID／IP／ASN三元组全部唯一且共同120组。但本轮00:00文件摘要仍为第32节`3dd941…`，与已证实gzip截断的旧源及回执相同。研究核查提示后，主任务关联旧失败实证，停止此文件准入，未重复解压或以其部分内容构造完整分母。

随后另检业务日零点候选，右端保留已交付规模的同一RIB：

| 角色 | 原文件与首MRT时间UTC | 北京时间 | 验证层级 |
| --- | --- | --- | --- |
| 新左端 | `bview.20260330.1600.gz`，03-30 16:00 | 03-31 00:00 | 本轮gzip完整及首Peer表通过；全部MRT／路由语义未验收 |
| 右端 | `bview.20260331.0800.gz`，03-31 08:00 | 03-31 16:00 | 同SHA完整RIB已在第32／43节复读；尚未产生本次逐Peer×Prefix比较输入 |

新左端437,060,440压缩字节，源SHA256 `c7be3c1a978f8373809fea5e5b9e6f6399ec962a9165a2ca77d984632c50e1cd`；本轮32.94秒内完整解压4,325,178,268字节，解压SHA256 `a566b03c0af366100c8533bc7b675956f447bd5c5eb5f9fb6aeb1003c3db78da`。源读取前后摘要、实体／mtime／ctime一致；上限压缩512MiB、解压10GiB、80秒。只保留首Peer帧原文，没有留存整份新RIB或解码路由条目，gzip完整不证明全MRT或观察覆盖完整。

新两端首Peer表均为Collector BGP ID `0.0.0.25`、View `rrc25`，原始三元组共同且唯一120组，单侧／重复组均0；笔记独立解码复核。这只是首表候选键，不是稳定PeerIdentity、连续Session或有效路由分母；仍须核验整个输入内的表作用域及逐条路由。两时点位于配置默认业务日内，不改变默认日期，不把16:00称日末。

### 45.3 最小提案，尚未批准或实施

建议先做“**03-31 00:00与16:00北京时间的RIB路径对照**”，先离线验证，再讨论同版API／首页呈现；不替代期间变化次数、可见性变化或起源变化，不批准连续状态重建。

- 同Collector／View内，按两端唯一原始BGP ID／IP／ASN配对，保留Peer表位置与两侧来源；不创建稳定Peer或Session。仅比较配对组内共同IPv4／IPv6单播Prefix，且每端必须唯一entry；重复、ADD-PATH、多表作用域不明不能任取一条。
- 首片仅比较完整非空AS_SEQUENCE且无AS4_PATH的有序ASN序列，保留私用值、prepend与原始段。只改相邻序列编码分段不计差异，prepend变化计差异。AS_SET／联盟／AS4_PATH等作为本规则不可比项保留原值及原因数量，不删除观察或猜测成员；不改变已确认起源规模口径。
- 分母为可比的`原始Peer属性组×AFI×SAFI×Prefix`对象对，不是前缀去重数。分别报告两端相同／不同、单端列出与不可比数量；零可比对象不补成零变化，单端未列出不推出网络消失，两端相同不推出期间稳定。
- 两文件／解析器／规则绑定新版本，完整核对对象唯一性、原始引用、具名排除量，并通过反例与独立复算。验收后也只能展示两端差异，不能给出中间变化时刻、次数、异常或实际影响。

Matt `research`的[语义核查](../.local/core-overview-validation/ordinary-change-next-20260912-7x72fz/两时点比较语义核查.md)结合项目ADR、[RFC 6396](https://www.rfc-editor.org/rfc/rfc6396.html#section-4.3)与[RFC 4271](https://www.rfc-editor.org/rfc/rfc4271.html#section-4.3)，区分原始字段与比较推论；其末项针对首轮截断文件，新业务日零点候选不是该文件修复版。用途、配对和解释规则仍待用户确认，未自动接入首页。

### 45.4 已执行验证与剩余缺口

[伴随笔记](../.local/core-overview-validation/ordinary-change-next-20260912-7x72fz/普通路径比较候选预检.ipynb)5单元从头执行成功：绑定6份输入，独立解码三张首Peer表、关联旧失败、核验时间／声明及3次主HTTP；`analysis.json`／`notebook-proof.json`保存结果和代码身份。首次笔记误用`address_family`参数被API拒绝，失败生成器保留；改用既有`family`后重跑，未改API迁就核验器。本地内核TCP提示保留，不宣称OS隔离。

主`410c…`仍为49可用／10失败日，默认前缀1,403,383／起源85,565／异常14,410；IPv6 269,730／36,483／8,352；03-28仍503且统计／趋势／列表为空。本轮没有UI改动，HTTP不冒充全套浏览器或产品回归验收。10日旧源问题、普通变化仍未交付，整体goal继续；无源库写入、检测重跑、状态重建、提交／推送／部署或数据删除。

阶段双轴复审均0项未解决：原收口程序的无路径差异检查被两轴指出，现已限定为两份基线文档增量及本片新文件，不让无关旧改动阻断本片。独立审查与主任务实际执行分别记录于本片`双轴审查.md`及`final-proof.json`；审查不代替数据或页面验收。

## 46. 劫持时间串写的当前代码复现

2026-09-12。第45节路径对照的用途／规则尚待用户确认，未开始比较。本轮转向不依赖该决定的失败日期诊断，使用Matt `diagnosing-bugs`先建立可重复的失败信号，再最小化和做单变量对照。**当前代码的串写机制已复现；历史成因仍未闭合，旧数据未修复。** 没有重跑检测、访问真实数据库、执行旧项目代码或修改产品写入函数。

### 46.1 实际驱动的代码和结果

对象为当前工作树`backend/database/hijack.py`，SHA256 `6f9b707ce4bf81ff787f08fd3cc19ada3030a93995f55276c6fa1ae2c03417ec`。调用真实公开`create_hijack_table`／`hijack_start`／`hijack_end`，DDL仅记录，实际写入只在SQLite内存夹具；列和主键从真实DDL取得，字段类型简化为TEXT，驱动只转换占位符。不模拟PostgreSQL类型转换、并发或上游编号生成，不将夹具称为历史运行重放。日志模块替换为内存记录器，真实psycopg2连接显式拒绝，没有加载环境文件。

时间值取自第41节留存的两个总表引用：03-04 19:35:43—19:49:46、时长00:14:03；03-20 14:11:03—18:29:00、时长04:17:57。共同source=r、Prefix `80.244.11.0/24`、编号1。把它们依次送入当前写入函数，是**复现设定的调用顺序**，不是已知历史调用记录。

首次完整复现连续执行两次，每次均RED：第一次完成行随后变成“03-04开始＋03-20结束＋第二次时长”，三个时间字段与留存坏明细完全一致，起止差1,378,397秒而时长15,477秒。第二次start触发主键冲突，函数记录错误并rollback但正常返回None；随后的end仍更新旧行。第一次start／end的返回值也都是None，调用方不能仅凭该返回值区分写入成功。

### 46.2 最小复现与三个解释的对照

固定已存在的第一条完成记录后，仅调用第二次end就能复现；第二次start及其rollback不是必要条件。公开失败命令为本片`repro_v2.py --skip-second-start --output <本片新的JSON路径>`，程序写回执后以退出码1报告旧记录被改动。无夹具初态或无第二次end均不能构成该复现；没有把整个检测器或真实数据库加入测试。

| 场景 | 唯一改变的条件 | 实际结果 |
| --- | --- | --- |
| 原始两次start/end，重复2次 | 无 | 均RED；混合时间与坏明细相同，3次commit／1次rollback |
| 最小场景，重复2次 | 省略第二次start | 均RED；仍混写，3次commit／0次rollback |
| 不同编号 | 只将第二次编号改为2 | GREEN控制组：两行各自正确，旧行不变 |
| 只有冲突插入 | 省略第二次end | GREEN控制组：插入失败，但旧行不变 |

另执行`empty_initial_control.py`去掉旧行初态：同一次end更新0行，不产生混合记录，公开返回仍为None；回执`empty-initial-proof.json`保留。它补齐最小化反例，不把零行更新说成业务成功。

因此，在此夹具内支持“结束更新无法区分同键的两次发生”；不支持“参数错位”或“插入失败／rollback本身改坏旧记录”。实际捕获的end SQL为`WHERE source=… AND prefix=… AND hijack_eventid=…`，没有开始时间或本次发生的绑定；SET参数顺序与第二次输入一致。数据类型／时区转换未在夹具中建模，不据此排除真实历史系统中的其他问题。

### 46.3 对治理和修复意味着什么

未来若修复该写入链，需要共同处理“本次发生与旧行的绑定”和“插入失败后不得继续结束旧行”，并验证调用方能识别失败、零／多行更新及事务边界；不能只隐藏冲突日志、给负时长夹零或给旧记录改编号。本轮只诊断，不冻结新主键／状态模型或改写当前代码。当前首页只读留存输入，修复遗留写入函数也不会自动恢复已混写明细。

此机制与03-04／20的留存字段组合吻合，但未证明历史执行代码、编号为何重用及当时完整调用序列；按goal不追补旧检测版本。总表时间不能恢复第二次完整角色／路径或第一次原结束路径，故两日仍不准入。本片不外推03-03的12条负时长、11条AS_SET身份或空国家问题，更不由局部代码复现推断异常真实发生、原因或影响。

### 46.4 证据与当前状态

目录`.local/core-overview-validation/source-time-code-diagnosis-20260912-w6XS01/`保存原始／最小复现程序、6份早期结果、当前源文件的字节副本和`verify_cases.py`。后者重新执行6次公开调用场景，实际每次0.040—0.044秒，按预期保留4次RED／2个GREEN控制组，回执包含命令、stdout／stderr、退出码、SQL／参数、源／输入／脚本摘要和重复结果一致性。GREEN仅指对照输入没有触发串写，不是代码已修好。完整结果在`verification-proof.json`；没有新增通过的日期。

另做4次只读主HTTP：03-31仍14,410异常／1,403,383前缀／85,565起源AS；03-03／04／20仍503且统计、列表、趋势为空，版本仍`410c…`。本轮没有页面变化或新浏览器验收，也未重跑完整产品测试；整体goal继续。无需为复现添加产品日志，所有夹具保留在明确诊断目录，未删除材料、提交、推送、部署或更改共享服务。

Standards／Spec两轴独立审查均0项，仅认可本诊断范围；源与回执绑定、预期RED及对照结果、两文档增量的最终复读由主任务另行执行，见本片`双轴审查.md`与`final-proof.json`。当前及历史写入代码均未修复，不能把审查通过当作缺陷消失。

## 47. 已确认的两时点RIB离线路径对照

2026-09-12，用户确认第45.3节的用途、配对与解释规则后执行。**限定离线对照GO；尚未接入API或首页。** 没有连续状态重建、可见性／起源变化或期间变化次数。阶段目录`.local/core-overview-validation/rib-path-comparison-20260912-OqlI6z/`，运行中的首页仍为`410c…`起源消费版。

### 47.1 输入与公开离线接口

左端为远端`/home/bgpdata/data/ripe/rrc25/2026.03/bview.20260330.1600.gz`的本次完整副本，源SHA为第45节`c7be3c…`；右端复用第43节完整留存的`bview.20260331.0800.retained.gz`，源SHA仍为`7ab60c…`。两端MRT时点分别为UTC03-30 16:00／03-31 08:00，即北京时间03-31 00:00／16:00；不能称业务日首尾。旧UTC03-31 00:00截断源没有准入或替换。

`scripts/rib/compare-rib-paths.py`提供单一离线命令，入口接收两文件／SHA和新的输出目录，使用当前数据配置。新模块复用本项目`rib_origin`的Peer表／AS段解码，不改变已有起源口径或代码。完整读取两份gzip／MRT、约束单Peer表作用域，按原始三元组配对，同组内共同单播Prefix且两端唯一entry才可能比较。只比较完整非空纯AS_SEQUENCE，无AS4_PATH；私用值、prepend及原始段保留。所有原始属性和Originated Time保留在解压原文，其他属性差异不属于本片指标。

每个结果对象均保留两侧frame／entry零基引用，文件、Peer表、物理记录和解压偏移可追溯。重复、复杂路径或单端对象不从原始记录中删除；多Peer表／作用域不明则整份候选失败，保留原始副本和失败原因，不任意选表。源、代码及其前后身份核对；输入和输出均不得经过软链，输出不得覆盖。说明及资源边界见[离线接口说明](../.local/core-overview-validation/rib-path-comparison-20260912-OqlI6z/离线接口说明.md)。

### 47.2 真实结果与分母

两端各120条原始Peer三元组，表内唯一且全部可配对；这不是120个已确认稳定Peer或Session。左端1,402,807条MRT／56,551,582条RIB entry，右端1,403,384条MRT／56,651,076条RIB entry；MRT计数各含一张Peer表。解压字节分别4,325,178,268／4,353,737,106，左端解压SHA与预检`a566b0…`一致，右端为`31651e85472420f8fc2976ebed1fc3c40ddea4a51a98347a67bcf8ba53cd3d80`。输出1,405,636个AFI/SAFI/Prefix行，每行包含各原始Peer组的对象，不把行数当作比较分母。

| 结果对象数 | IPv4单播 | IPv6单播 | 合计 |
| --- | ---: | ---: | ---: |
| 两端路径相同 | 45,201,408 | 9,748,019 | 54,949,427 |
| 两端路径不同 | 1,180,446 | 321,995 | 1,502,441 |
| 仅左端列出 | 67,303 | 24,013 | 91,316 |
| 仅右端列出 | 97,534 | 93,276 | 190,810 |
| 共同但路径不可比 | 7,246 | 1,152 | 8,398 |

分母为**56,451,868个可比的原始Peer属性组×AFI×SAFI×Prefix对象对**，不同占**2.6614548876%**。不是独立Prefix变化数、日级变化率或区间发生次数。共同不可比的8,398对均涉及AS_SET；全部状态中带AS_SET原因共8,435个对象，另37个是单端项，所以`reason_counts`不能直接当作共同不可比对数。真实两端未出现重复entry／重复三元组、ADD-PATH、联盟或AS4_PATH等其他原因；其失败／不可比分支由合成反例覆盖，不冒充真实正例。

按本次唯一条目输入，共同对象加左单端为56,551,582，另加右单端为56,651,076，两端分母闭合。单端缺项不推出网络消失；同路径不推出期间稳定。`interval_change_count=null`、Session连续性和观察覆盖均unknown。

### 47.3 版本与实际验证

完整版本为`rib_path_comparison_v1_dd4a04f91975f23e867942a946b2df4251e89b97c69ce99e84b684925954d545`，目录`candidate-v2/`。manifest绑定八份制品：两份压缩源、两份原样解压MRT、frame定位SQLite、原始Peer组、全对象引用结果及摘要；SQLite是离线定位索引，不是RouteState或Web数据库。执行模块／复用解码器／CLI的摘要、运行环境和配置均可追溯，实际代码副本另存`executed-code-v2/`。本次399.10秒成功，完整命令与代码前后不变见`run-v2.json`。

- Matt `codebase-design`把实现集中在小的公开离线接口；`tdd`沿该接口验证，不直接测试内部解析函数。实际RED覆盖入口缺失、具名复杂段／身份／ADD-PATH，以及审查发现的提交后清理失败；随后GREEN。其余安全／损坏反例亦实际执行。最终26项新增CLI测试、全后端503项通过，原有pandas警告保留；没有接口或前端改动，不把此测试当作新页面已交付。
- 首轮真实v1启动后，Standards与Spec均指出“最终manifest建立后临时名清理失败，会同时留下成功／失败标志”的P2。按已核对PID终止首轮，45.12秒、子进程-15，尚无最终manifest；原材料及执行代码保留。修复后保留同实体`manifest.pending`别名，预先构造返回值，原子建立`manifest.json`后无清理I/O；公开故障测试先RED后GREEN，双轴复核0项。v2从完整输入重新执行，没有把v1部分结果拼入。
- `independent_verify.py`不导入生产解析器，104.10秒独立复读全部原始frame／entry数量，核对全部1,405,636结果行及条目引用范围、唯一性与穷尽，分地址族及五类结果计数闭合。另按每997行和各状态前10对作确定性分层抽核：1,422个Prefix行、57,444个对象、114,503条原始entry，独立路径判断一致；**这不是第二解析器全量路径语义复算**。`independent-proof.json`保存原始引用与样例，不将抽核推广成观察覆盖证明。

### 47.4 交付边界

数据质量／结论验证可带限制分享：快照时点、对象粒度、分母及排除项均显式，既有两端完整文件可追溯；原始Peer匹配仍只是已接受的有限推断。伴随[核验笔记](../.local/core-overview-validation/rib-path-comparison-20260912-OqlI6z/两时点RIB路径对照核验.ipynb)5个代码单元从头执行成功，`analysis.json`核对回执、分母和样例；4次真实只读HTTP确认03-31仍为14,410异常／1,403,383前缀／85,565起源AS，03-03／04／20仍503、统计／趋势／列表为空，同属旧`410c…`版本。临时uv笔记环境未改项目锁文件，本地内核TCP提示保留，不声称OS隔离或新浏览器验收。两轴最终各0项，最终八制品SHA／两份远端原文件只读身份及限定文档差异复核见本片`final-proof.json`；初次收口脚本误把diff的分段形状当作内容范围，已保留失败程序与日志，改为实际变动行号核验，不改变产品或结果。

不自动将2.66%或1,502,441填入首页“路由变化次数”。同版只读API与首页的“两时点路径对照”如何呈现，需要下一步明确范围；本轮不修改合同／前端、不部署或重启服务。既有10个失败日期、旧检测问题和期间变化证据仍未解决，整体goal未完成。本机Git外离线留存不是异地／不可变保管，也尚未纳入第44节首页恢复包。未写源库、重跑检测、持续采集、提交、推送、部署或删除材料。

## 48. 三项治理结果进入同版首页

2026-09-12。用户授权完成身份原文保留、起源规模口径、两次观察路径对照，并要求第一项与任务“AS_SET 一致性修复与单案例验收”（`01a0910e-42ad-7642-a44a-6352d20aac73`）协作。本轮接收其明确交接，再独立执行全日重验、消费实现、只读API和页面验收；没有运行旧项目代码、重写源字段或把单案例验证扩张成全部异常真实性证明。

当前主入口为本地 `http://127.0.0.1:28492/`，后端28491；完整版本为 `overview_index_v2_37e0ea72f8f1a18a4cf57d8d6be576983db95e5a04be87b78d5f349ac8e9c37d`。制品目录为 `.local/core-overview-validation/three-points-20260912-T53su1/combined-index-v2/`，以下路径均相对于本轮目录。前端保持已确认C结构，不以ASN为首页主轴。

### 48.1 身份：显示原文，不恢复成想象中的单ASN

交接源为361b工作树的 `identity-display-handoff-20260912-v1/review-v2/`。`identity-records.jsonl` SHA256为 `2d74d816e3a7880e39ed5c1594ef7f5cc22d41e2270abcbc014cddd94252efe4`；`day-validation-scopes.json`为 `8c31576140821f320c8f9d008dda1d6dd396fc7edbf57c13b014969a5efb1b7c`。本轮副本在 `handoff/`，授权按完整事件引用及完整来源文件SHA精确匹配，不按集合字符串批量豁免。交接包含11条记录，9个单成员花括号文本、2个多成员文本；均保留一条原检测记录，不能拆分为多个ASN或据此判定误报。

新记录使用 `legacy-anomaly-mapping/v2` 与 `unresolved_asn`，原始文本、引用、结束时间和时长原值不变。列表及详情显示“对象待核实”，`asns=[]`；不创建单ASN跳转。普通数字身份仍是原mapping/v1，序列化字节及内容版本不变。新日采用 `recorded-anomaly-overview/v3`，目录逐日明确绑定规则，旧49日保留v2，不让目录升级隐式重解释历史日。

| 重验业务日 | 完整原事件数 | 集合身份记录数 | 最终处理 |
| --- | ---: | ---: | --- |
| 03-07 | 35,892 | 2 | GO，准入 |
| 03-09 | 35,540 | 1 | GO，准入 |
| 03-10 | 33,703 | 1 | REPAIR，整日继续隔离 |
| 03-11 | 36,036 | 2 | GO，准入 |
| 03-14 | 23,793 | 1 | GO，准入 |
| 03-26 | 27,429 | 3 | GO，准入 |
| 03-28 | 98,157 | 1 | GO，准入 |

最终审计是 **`identity-audits-v2/result.json`** 及其七个日期目录；不要使用文件名相近的 `identity-audits-v2-result.json`，后者只是第一轮v1审计的历史汇总。全日按原完整读取回执、独立事实集合、六类型关联、字段和小时桶重验。六日新增256,847条消费记录，10条待核实身份随之可见；旧49份SQLite逐文件SHA不变，所有此前可转换记录逐条字节一致，见 `identity-merge-proof-v2.json`。

03-10有24条国家事实、23条总表事件，独立集合少1条；修复身份显示不能消除该差异。该日33703条原事件保留于未准入结果，首页统计／趋势／列表为空，详情503；新的诊断仅保留独立 `source_population_mismatch`，不继续把已解决的身份显示问题列为失败原因。整体为55个可用日、1,120,555条记录；03-03、04、10、20四日继续失败，不补零。

### 48.2 起源规模：执行后来明确确认的归属口径

用户本轮引用包含较早的“AS_SEQUENCE原始末端去重”措辞；其后明确确认的是从末端跳过私用ASN、仅对明确归属的单ASN去重，AS_SET／联盟段不拆分或越过猜测，原值另存。本轮保留后者 `rib-attributed-origin/private-skip-v1`，没有切换成原始末端计数。第43节的公开离线命令、源路径目录及摘要继续有效，原始路径／末端未修改；保留值处理和不可归属项仍按已绑定规则，不新增归属推断。

默认03-31 16:00北京时间单RIB：前缀1,403,383，归属起源AS 85,565；IPv4 78,236、IPv6 36,483，并集不是二者之和，未归属路由条目8,429。这是快照规模，不是当天变化、所有网络、稳定Peer或覆盖完整性。独立复制并保留原 `rib_origin_v1_7b315aedf24fb2fd0eef0eabfae5e352b563a08974a1e23d1abe3eb18d40e5d1` 绑定，Web只读有界摘要，不在请求里重算MRT。

### 48.3 路径：两次观察对照已经接入

复用第47节已验证的 `rib_path_comparison_v1_dd4a04f91975f23e867942a946b2df4251e89b97c69ce99e84b684925954d545`。新离线入口 `scripts/core_overview/bind-core-overview-paths.py --index <明确索引清单> --comparison <明确比较清单> --output <新目录>`，完整核验八份源制品SHA／大小、逐行复算全部比较结果计数，并将每族最多5条差异样本定位回原MRT条目。31.77秒成功，命令与结果见 `path-binding-execution-v2.json`。不复制9.8GB原始证据到Web消费包，只复制有界摘要、样本及原清单；独立语义全量／抽核界限仍按47.3节，不把本次绑定冒充第二解析器全量复算。

路径消费版本为 `rib_path_consumption_v1_923285261575c8752b380b5c04a45fa0510ea90354aa55f50f813ec79a637ba9`。页面明确显示北京时间03-31 00:00→16:00，路径不同1,502,441对／可比56,451,868对＝2.66%，相同54,949,427对；不可比8,398、仅左侧91,316、仅右侧190,810。单位是原始Peer属性组×AFI×SAFI×Prefix对象对，不是独立前缀数、期间次数或异常。两端相同不意味着期间稳定；单端缺项不证明撤回或网络消失；Session连续性／覆盖unknown、期间次数null。

“路径样本与依据”显示两源SHA、比较版本、路径消费版本、首页版本和10条原始定位，保留重复ASN及私用ASN。样本按留存顺序选择，不是风险排序或代表性采样。其他日期、未知地址族不给数；路径包损坏只隔离路径，不抹去已验证的异常或前缀规模。列表局部筛选不改变上方概况、趋势及路径分母。可见性变化、起源变化、期间连续状态仍未实现。

### 48.4 验收、失败留痕和交付边界

Matt `codebase-design`和`tdd`将变更限制在公开离线接口及只读消费接口；新增身份反例、路径包损坏、同版详情和目录兼容测试先见RED再GREEN。完整后端534项／前端143项通过，生产构建、合同生成类型及差异空白检查通过；原有pandas警告保留。测试使用夹具，不使用真实数据；真实留存的另行检查明确分开。

`accept_http.py`执行候选141次、主前端代理92次真实只读HTTP，核对全部59日期的成功／失败状态、旧49日统计／趋势／列表一致、全部11身份记录的准入或门禁、地址族、局部筛选不改分母、合同及旧版本409拒绝。结果为 `http-candidate-v2.json`、`http-main-v2.json`。只有完整统计与门禁通过才切本地后端，运行绑定见 `main-runtime-v2.json`，不是共享部署或生产服务。

浏览器实际验证默认路径面板、IPv6切换、集合原文详情及同版证据，390像素布局无页面横向溢出。当前电脑锁屏，截图命令失败，故**没有完成像素级视觉验收**，不能用DOM或旧截图代替。候选第一轮代理端口配置错误已改正；首次浏览器验收脚本误等候不存在的提示措辞，按现有页面实际措辞修正脚本，没有修改产品迁就检查。第一轮原始失败脚本和候选包保留。

两轴审查指出授权文件可能在运行中改变却记录结束SHA：已改为绑定最初实际解析字节、结束前逐字节确认，合成CLI并发变更反例从错误GO改为拒绝；随后从完整源重新执行全部七日，最终消费不沿用旧审计。失败日输入解释版本误回落目录v3的显示也已修正为“尚未取得”，可用旧日显示真实v2。历史中间汇总文件名易混问题通过本节及本轮README明确最终入口，不改动旧证据文件。

已执行的伴随核验笔记、最终版本路径和重启命令统一见本轮 [README](../.local/core-overview-validation/three-points-20260912-T53su1/README.md)。材料留在本机Git外；旧第44节恢复包仍绑定`410c…`，**不声称覆盖本次37e0…版本或上游全证据闭包**。本轮不扩大为异地灾备、持续采集、旧检测修复或四个失败日恢复。未写源库、重跑检测、提交／推送、部署共享服务或删除材料；三项有限消费交付不等于整体数据治理goal完成。

### 48.5 后续视觉补验完成

用户继续执行后，改用独立临时配置的无头Chrome，仅访问本地28492真实首页，不解锁系统或读取用户浏览器资料。通过明确连接设置实际1440／390像素视口，核对页面scrollWidth未超过视口，再生成并实际查看四张图片：桌面首页、窄屏完整首页、身份待核实详情、窄屏路径对照弹窗。数值、原始集合文本、时间／单位／分母、来源版本均完整可读，无可见重叠或水平裁切；长弹窗采用纵向滚动。26条命令、截图时DOM、原工具路径及SHA保存在 `visual-capture-proof.json`，主任务逐图结论见[视觉验收补充](../.local/core-overview-validation/three-points-20260912-T53su1/视觉验收补充.md)。

初轮CLI写出桌面PNG但未退出，超时后停止；只靠启动窗口参数的早期窄屏图存在裁切，没有作为验收。最终`visual-final-*.png`是明确实际视口后的新图，不覆盖初轮回执。48.4节的截图限制是补验前状态；**本轮指定页面状态的视觉检查现已完成**，不外推为跨全部浏览器／设备或完整无障碍审计。数据版本、产品代码、四个失败日、原有起源口径及恢复包边界均不变。最终伴随笔记为 `首页三项治理核验-v2.ipynb`，初轮笔记保留为历史执行记录。

## 49. 当前37e版本的独立恢复与空国家原文复核

2026-09-12。本阶段不改变产品代码、源数据或准入口径。主28491／28492仍选择`overview_index_v2_37e0ea72f8f1a18a4cf57d8d6be576983db95e5a04be87b78d5f349ac8e9c37d`：55可用日、4失败日、1,120,555条留存异常。当前恢复不再引用第44节旧`410c…`的验收结果。

### 49.1 精确空国家键的完整原文

从同一个原逻辑备份 `/home/bgpdata/Domeye-Core-artifacts/work/20260717T124354Z/source-full.pg12.custom.dump` 导出扫描三月总表和国家明细，未向数据库执行SQL。备份3,029,328,735字节，读取前后SHA均为`092ef641aeb7a88507a0062082a35cde7d162e69880471837db1321d5a212c96`。本轮扫描总表1,142,937行、国家明细732行，只留精确引用 `country_outage/2026-03-10 16:24:31//1/r` 的两条完整COPY行；不是全日消费留存。

总表25字段／386字节，行SHA`d244693431c5153c65008e5670df08e2d25eb008c10097385ef5900cab1bb998`，与第41节原行摘要相同；国家明细14字段／551字节，SHA`a895edd86535ed328b81b6185e9308e9a767426eadb33b526e99d09b05c26a80`。原始行base64、列名、原COPY转义文本和null均保留，解码后逐字节摘要及字段数核对通过。

结构化开始均为03-10 16:24:31，结束与时长均null，原等级high；国家代码和中文名称均为空。明细说明文字写04-26 00:24:38，不能覆盖结构化时间或追补检测版本；原数组27项包含集合文本，原分母330也不是已验证的国家人口。**找回总表记录不等于国家身份已恢复**；本轮不将备份记录混入当前库制品、不推断国家、不将集合拆成单ASN。第48节03-10全日集合差诊断及门禁不变。

其他三日沿用第41／46节已复核证据：03-03有12条负时间记录；03-04／20存在同键劫持串写／关联及时间矛盾。相同备份没有提供可信替代值，本轮没有再次声称重建或修复。当前代码机制复现仍不等于历史每次调用顺序或检测正确性。

### 49.2 同版独立安装与公开行为验收

阶段目录：`.local/core-overview-validation/recovery-37e-20260912-2pwMpL/`。冻结280份实际源码／配置／锁文件和75份消费文件，后者共6,238,747,645字节；源码含既有未提交改动，不以HEAD冒充。恢复目录另建新venv和node_modules，按锁安装、构建、534项后端／143项前端测试通过，5项原有警告保留。

独立目录 `/private/var/folders/yb/k98mxs2j31g_46bk5x8kd9640000gn/T/domeye-37e-recovery-80f4hu89` 的28495后端／28496前端实际启动。后端启动前复读355份文件，4个负例确认Python层拒绝原工作树／源目录、其他SQLite、PostgreSQL和外连，领域模块均从恢复源码导入。不是OS沙箱，也不是源DB／INFO／P0恢复。

114组前端代理HTTP与当前主服务逐响应相同：59日期的可用／失败状态、六类详情、各地址族规模、10条集合原文同版详情与1条失败日门禁、路径两时点／分母、局部筛选；另验证错误版本409。四次故障仅暂移新解包文件，并在finally放回及确认SHA：缺路径摘要只使路径不可用；缺起源摘要只使起源null；缺规模摘要使规模null；缺默认日SQLite整日503。原主服务四次均正常，未改原件。

浏览器26条截图命令、57条状态／导航命令完成，9个状态包括两个锚点、IPv6、无快照日、成功空日、失败日、旧事件和P0导航、回到首页。1440桌面、390窄屏、路径／身份弹窗及失败日共五图由主任务逐图检查，无页面横向溢出，长弹窗纵向滚动。无源库恢复实例的旧事件页明确“查询失败 · 记录数未知”，P0明确503不可用；路由可到达不等于这些业务已恢复。主服务旧事件API另用正确完整日期参数复查可读；第一条试探请求缺少规定date参数被正常拒绝，不是回归。

### 49.3 独立保管与使用边界

工作树外保管目录 `/Users/botongwu/.codex/recovery-drills/domeye-c-20260912-37e-v1/` 已存30份材料，逐文件前后SHA相同且inode独立，文件0600、目录0700；不删除或覆盖旧包。归档 `domeye-c-consumer-37e-v1.tar.gz` 为651,804,008字节，SHA`0cc042e69c2293b4d785be0ba1f13f992e133d530d03dfc1de7fd7f61a0421bc`。外部`保管回执.json`的SHA为`c98477aafed6e3886a2cfd5b5508a3b3ba59092138be606466f9d2dc98357c86`。

具体命令与已验范围见[当前恢复说明](../.local/core-overview-validation/recovery-37e-20260912-2pwMpL/恢复说明.md)。三单元[可执行核验笔记](../.local/core-overview-validation/recovery-37e-20260912-2pwMpL/当前恢复与空国家原文核验.ipynb)已从头执行，覆盖原文、包绑定和六次运行HTTP复读。原日志、截图、脚本及回执均留存；不是仅生成一个未执行的笔记。

副本仍为同机同磁盘，工具链和下载缓存沿用本机，不是不可变或异地备份、断网安装或全新OS验收。75文件只覆盖C消费所需数据，不含源DB／INFO／P0、原MRT或上游全部证据；摘要内源文件路径只是来源标识。归档内README属于冻结时点，本包外恢复说明为本轮实际命令；工作树README的当前入口已同步到37e。未提交、推送、部署共享服务、重跑检测或写源库。

阶段结果：当前C消费恢复**GO**，4个日期源数据准入仍为**REPAIR／隔离**，不得混称为全部数据修复。已向用户询问是否以“55日可用、4日明确隔离”作为本轮整体交付边界；答复前不将整体goal标记完成。后续若扩大到四日重建，必须先明确额外规则和证据，不能以页面需要为由猜测修复。

### 49.4 双轴审查与收尾

Matt Standards／Spec两个独立审查已完成，完整分轴报告见[双轴审查](../.local/core-overview-validation/recovery-37e-20260912-2pwMpL/双轴审查.md)。Standards无硬违规，1项P3建议为两份已冻结浏览器脚本的辅助函数重复；本片保留执行原字节，未来复用另建版本。Spec无发现，不将4个失败日或整个goal冒称完成。审查者依据留存证据，不宣称独立重跑了安装或全部数据。

`final-check-proof.json`重新核对355恢复文件及75原消费文件未变，本片仅README和两份项目说明变化；主C与旧事件API均200。`cleanup-proof.json`确认临时28495／28496和隔离浏览器61395已关闭，主28491／28492原进程保留；未删除任何材料。演练不是常驻部署，若重跑笔记的在线部分须先按恢复说明启动新实例。

## 50. 原goal逐项复核与替代源补查

2026-09-12。上一轮恢复完成是实际进展，不作为缩小原goal的依据。本轮主消费版本仍`37e0ea72…`，没有产品、接口、源数据或配置改动；未将整体goal标记完成。

### 50.1 补查范围与结果

实际盘点`/home/bgpdata`下13个已发现的Domeye相关根目录，最大深度10，匹配dump、SQL、SQLite／DB、backup及数据库归档文件名；跳过依赖、Git、缓存和WAL临时目录，不跟随软链。完整声明范围内共1,380个文件路径，命令退出0且无stderr。绝大多数是重复工作树中的建表／迁移SQL文件名，不能当作独立历史数据副本；本轮不执行SQL、不读取旧环境、会话库内容或物理数据库页。

≥1MiB的6个候选中，两项位于会话恢复测试路径且为WAL；三项是已读原始3.03GB逻辑备份、2.33GB发布dump和161MB数据库软件镜像；另一项是2MiB的P0 RouteEvent pilot索引。现读发布元数据明确发布dump来自第41／49节同一原备份，不能当作另一份独立事件来源。pilot现成摘要明确`pilot_only=true`、`production_complete=false`，只包含`rrc25/2026.03/updates.20260324.1455.gz`一份UPDATE，不是03-03／04／10／20原检测事件备份。本轮只验证这些元数据和用途，不冒称重新读取候选全部内容。

结论：**在本次声明范围内，未识别出可以直接替代四个坏日的另一份可信历史异常记录备份**；不外推为整机或所有存储均无证据，不靠重复读取已证实含相同坏值的备份期待恢复。若无新的可信来源，修复四日仍涉及新重建范围／解释规则，不能自行猜时间、国家或对应关系。

### 50.2 原目标与当前证据

本轮执行68次主28492真实只读HTTP：默认日、全部59日期、六类筛选及IPv4／IPv6数值；同时复核第48节最终绑定证据、当前第49节恢复回执与外部保管回执SHA。未重跑源库全表、MRT解析、安装或浏览器；旧回执只证明其原记载范围。

| 原goal要求 | 当前证据与结论 |
| --- | --- |
| 日期覆盖 | 配置默认03-31不变；59日均有明确状态，55可用／4失败，共1,120,555条消费记录。失败日概况／列表／趋势null；四日源数据修复仍未完成 |
| 三种新增异常及旧类型 | 当前六类均可查询、筛选不改变概况和趋势；完整键、字段、时间、角色、结束语义与同版详情沿用第20／22／23／48节已验收证据，不把本轮少量HTTP说成全记录重验 |
| 整体规模 | 同一实际RIB的前缀1,403,383／起源并集85,565，IPv4和IPv6分别对照；不能把异常对象数当规模，也不改为配置日末时点 |
| 普通变化有限切片 | 两次路径观察对照已接入；分母、实际时间、来源版本、单端／不可比和Session未知保留。可见性／起源变化及连续重建不在已批准路径切片内 |
| 稳定交付恢复 | 当前消费版的新依赖、355文件、114组HTTP、四故障、五张图、导航及30份材料保管已于第49节验证；不声称源DB／INFO／P0或全站恢复 |

另核对API中`scale.limits`仍保留旧前缀制品生成时的原始说明，其中“起源待确认”不是当前起源状态。当前起源由`origin_metric_state=available`、独立`origin`版本和已确认private-skip口径解释，首页卡片／起源详情使用后者。此次仅记录溯源作用域，不改写被引用的原前缀摘要或85,565计数；原文不能被当作当前状态覆盖值。

阶段材料位于`.local/core-overview-validation/completion-audit-20260912-RbRJvH/`，包括远端盘点命令／输出／回执、四份元数据及SHA、`completion-audit.json`和[两单元已执行笔记](../.local/core-overview-validation/completion-audit-20260912-RbRJvH/交付逐项复核与替代源盘点.ipynb)。原文、原页、原服务和所有旧输入保留。整体未决项仍是四日源问题及55可用／4隔离能否作为本轮交付边界；不重复询问数据事实，不以隔离代替修复，不标记整体完成。

Matt独立双轴审查完成：Standards 0项、Spec 0项，[分轴报告](../.local/core-overview-validation/completion-audit-20260912-RbRJvH/双轴审查.md)保留各自证据范围。主任务另核对第48节14个关键代码／合同／前端类型文件SHA均不变，28491／28492原进程保留。本片没有产品变化或新浏览器执行，不把第49节浏览器验收冒充本輪重复执行；主HTTP复读另有本轮回执。

## 51. 用户确认交付边界与本轮收口

2026-09-12。用户对“是否接受55天可用、4天明确隔离作为本轮交付边界”明确答复“可以”。据此，本轮首页真实数据展示与有限数据治理目标按调整后的边界完成，不再因这四天未修复而持续运行原goal。第49—50节及短规格中的“待确认／未完成”是答复前的历史状态；原回执、冻结材料和`completion-audit.json`不追改成事后成功证明。

交付仍绑定`overview_index_v2_37e0ea72f8f1a18a4cf57d8d6be576983db95e5a04be87b78d5f349ac8e9c37d`：55个可用日、1,120,555条六类留存异常；身份异常保留原文并显示“对象待核实”，不拆成单ASN。默认03-31可见前缀1,403,383、按已确认private-skip规则明确归属起源AS 85,565；路径只表示两次观察对照，不是全天变化次数。既有离线留存、只读API、首页与本地消费恢复验收范围见第48—50节。

四个隔离日明确保留为未修复：03-03自身负时间，03-04劫持时间矛盾，03-10国家身份缺失及总表／明细集合差，03-20劫持来源集合及时间矛盾。接受交付不等于接受这些源事实有效，不补零、不猜值、不放行部分明细。未来恢复须有新的可信证据或另行明确的重建规则；本次不启动重建、持续采集或后续自动任务。

收口只做小范围现状核对：第48节14个关键代码／合同／前端类型SHA仍一致；主28492默认日及03-15返回同版可用，03-15保留22,745条异常且无该日规模快照，规模值仍为null；四个隔离日均返回`state=unavailable`、概况／列表／趋势null。当前清单仍有55个日期项和4个诊断项；第49节工作树外保管回执SHA仍为`c98477aafed6e3886a2cfd5b5508a3b3ba59092138be606466f9d2dc98357c86`。没有重跑全量数据、测试、浏览器或恢复演练；沿用各自已记录证据，不扩大验收声明。

本次仅更新本台账、短规格和README的交付状态；不修改代码、数据制品、旧证明、用户页面选择或运行服务，不提交／推送／部署。完成范围不包含四日源修复、观察覆盖确认、连续RouteState、全站／源DB恢复或异地灾备。

## 52. 服务器合并部署与GitHub进度同步

2026-09-12。用户在第51节接受边界后，另行明确授权提交、合并、部署及同步所有相关文档与GitHub进度，并确认以原main `983f0d4`为审查基线、现有28471／28473为目标，独立A问答服务不动。本次发布不撤销四日隔离，也不授权重算缺失事实。

### 合并、文档与审查

服务器原有未提交README、AGENTS、CONTEXT、架构／运行手册、Pi选型ADR与合同说明先记录为`259a185`；本地C实现及数据治理文档记录为`e439d98`，合并为`32c4bc2`。六个冲突都是文档，按双方原意合并：CONTEXT只维护领域词汇，架构描述产品与调用链，运行手册负责当前部署，本地既有命令迁入`docs/runbooks/C本地验收与恢复.md`，历史台账和原证据不改写。两份历史0001 ADR保留完整文件名区分；A属于独立分支，不伪装已合入主线。

Matt Standards／Spec独立审查基于`983f0d4…32c4bc2`。Spec未发现缺失、超范围或实现错误，另行fixture 222项通过。Standards发现1项P2：Python3.10循环软链接的`Path.resolve`异常未被日索引／诊断边界接住，HTTP500不符合503合同；另有1项P3重复校验辅助函数建议，保留后续维护，不强行合并不同业务验证。

按已确认的公开API验收入口执行TDD：日SQLite与失败诊断两个循环链接反例先500失败，再在路径读取边界捕获RuntimeError，概况保留已校验目录且统计／列表／趋势null，详情503，未损坏日期仍可用。修复提交`6839516`不改变成功结果、数据口径、消费版本、合同字段或源数据。服务器独立候选按原锁安装，最终536后端／143前端测试通过；前端构建通过，类型已按合并后的合同重新生成。既有警告保留。

### 制品与候选核对

现行`37e0ea72…`消费包复制到项目外新目录，75份选用文件、6,238,747,645字节逐项SHA／大小与本地一致。目录另有26,346字节历史`manifest.pending`，不被选用、不影响当前manifest；未删除它，也不将其纳入75文件。首次新核验脚本错误假设目录只有75文件、使用错误详情参数`reference`，已据原接口和已选清单修正为排除该暂存文件及`ref`，没有修改产品迎合脚本。失败执行保留于任务记录。

`release_check.py`在候选28496→28495完成75次实际HTTP：默认日、55可用日、4失败日、双栈规模、六类型及各一条同版详情、错误版本409；不是全部记录重新检测。确认55／4与1,120,555条、规模与路径口径不变；旧事件相同查询在基线／候选均444条，P0两边仍不可用，不据此声称全站恢复。原数据库／INFO／国家制品选择沿用本项目独立配置，凭据不打印或进入Git。

源文件、运行配置、HTTP原文与截图留在Git外：本机`.local/deploy-20260912-qMWCde/`，服务器`/home/bgpdata/domeye-new-runtime/releases/20260912-c/`；既有消费恢复归档不追改。

### 正式切换与验收完成

Standards对`6839516`的P2修复独立复核通过，新增两项fixture再次通过，未解决硬违反0；P3建议1项保留。Spec结论仍为0项。随后将两份服务覆盖配置纳入`f8be8d6`，服务器main快进至此版本；没有重置仓库或覆盖未提交工作。主前后端一次停止／启动，分别使用发布工作树的独立依赖与新配置；原配置、原工作树及旧消费包保留。

2026-09-12 03:25 UTC的`runtime-receipt.json`确认主后端PID1199423、前端1199424，均active／running／enabled、自动重启计数0；实际消费清单、强制只读事务、关闭初始化／启动加载及127.0.0.1:28473均核对。两个已安装drop-in与Git文件逐字一致。A问答／前端PID473162／473164与切换前一致，未操作这两个服务；共享只读API切换期间有短暂重启，不表示A完成新一轮模型问答验收。

正式28471→28473的`live-proof.json`再次完成同一组75次HTTP及75文件核对；55／4、六类型、默认单RIB规模、两时点路径和409一致。旧事件相同参数仍返回444条；它与C统计口径不同，不混算。P0仍不可用，不重建或伪装成成功。

agent-browser独立浏览器通过候选端口实际验证默认首页、路径依据、失败日、成功空日、IPv6切换及集合身份筛选／详情；主任务逐图检查桌面、390像素全页、路径弹窗、身份弹窗、失败页五张截图，内容可读且窄屏scrollWidth=390。正式内网URL加载默认数据、截图核对并实际导航至旧事件列表、旧P0页面，再返回C首页；P0明确显示不可用，未补零。自动化的首个语义定位失败后改用新快照引用，未修改产品。正式入口与候选验收范围分别记录，没有冒称全面重测所有业务或整机重启。回退版本与依赖已准备，未在正式服务执行完整回退演练。

验收后仅停止本轮候选28495／28496的两个临时服务、关闭本轮28497隧道及独立浏览器；正式主服务与A四个PID不变、自动重启计数仍0。本机28492与用户浏览器未操作，发布工作树、回退工作树、全部证据与数据保留。随后文档状态同步为已部署，纯文档快进不再重启服务。

### 统一任务进度

- [#15：本轮交付与发布](https://github.com/xinghuahewo/domeye_new/issues/15)承接已关闭的调查#13与模型验证#14，集中登记本轮已完成发布的清单与证据。
- [#16：四个隔离日期的源事实修复条件](https://github.com/xinghuahewo/domeye_new/issues/16)继续开放，不把接受有限交付写成源修复。
- [#17：锁定依赖告警与部署加固评估](https://github.com/xinghuahewo/domeye_new/issues/17)记录npm audit的7个节点告警（4高／3中）；并非7个独立已证实可利用漏洞。本轮未改锁或运行强制升级，也未完成可达性审计。
- #2的ASN特征无记录原因、#7的历史cohort／状态输入仍开放，单RIB及有限路径对照不替代其证据。相关任务正文顶部补充当前状态、链接现行成果；已关闭的历史调查及原评论保留原范围。

GitHub仓库当前仅用于Issues；源码提交与合并发生在SSH主仓库，不公开上传源库、凭据、制品或全部源码，也不虚构GitHub文件链接。

## 53. Issue #22 首次真实盘点因监控扫描超时停止

本节保留06:46 UTC的首次失败；随后获准的扫描修复续作见[第54节](#54-issue-22-扫描修复续作与首候选提前停止)，不追改原始失败回执。

2026-09-12，流程结论 **REPAIR**。用户在父任务明确确认了已审阅方案，授权指定窗口的只读盘点及独立目录内的有界离线试点；首次盘点在运行期磁盘统计超时后停止。尚未冻结输入清单，没有进入首候选准备或两批生产，12天实际MRT覆盖仍为 **Unknown**。这不是源文件损坏结论，也不是两批完成或产品验收。

### 执行范围、基线与已完成准备

真实执行任务为 `01a09454-581a-7021-b4be-ac36fda38719`，本地工作树 `/Users/botongwu/.codex/worktrees/63fd/domeye-new`，分支 `codex/issue-22-real-pilot`；父任务负责最终验收及 [Issue #22](https://github.com/xinghuahewo/domeye_new/issues/22) 状态。执行代码固定为 `40f66530643fd2032a2b5f94c976d75524f6f639`。目标为RRC25、Asia/Shanghai业务日02-27至03-10；UTC两批窗口分别是 `[2026-02-26T16:00:00Z,2026-02-28T16:00:00Z)` 与 `[2026-02-28T16:00:00Z,2026-03-10T16:00:00Z)`。

新服务器代码／运行位置及终态见[运行手册](runbooks/运行与维护.md#issue-22-首次真实试点终态)。执行前核对两根未占用、28621／28623无监听；代码bundle SHA256 `b8ab164dd2a24215a2e175f68b30d24b8cdd6d1dfd4e92d01c7311cd0f708eb3`、工具归档SHA256 `21f72d4933f1a3c1533c3c2878614b1b0d85196ca60b00cb6f532333eca40f7e`在传输前后及解包后逐文件一致。Python依赖按本项目uv锁安装（449.020秒），前端按npm锁安装（5.024秒）；结束时跟踪代码、两个锁文件及工具摘要均未变，没有加载旧项目环境。

两次安装编排失败单列保留：解包脚本错误假设归档顶层名为`issue22-tooling`，实际封包为`tooling`，目录断言失败后核对既有解包文件，未改工具或重新解包；npm在实际安装前拒绝user/global配置共用`/dev/null`，改为本次运行根中的两份独立空配置后完成安装。原失败日志仍保留，不作为生产器失败或真实离线重跑证据。

### 临时单元、资源与停止证据

唯一业务步骤为已审查runner的`inventory`，单元 `domeye-issue22-20260912-inventory-initial-5ea0365bca58.service`。单元内的`limits-verified.json`确认`system.slice`和实际`memory.max=8589934592`、`memory.swap.max=0`、`cpu.max=100000 100000`；运行1195秒、停止宽限5秒、Restart=no、KillMode=control-group均符合方案。限额已生效只证明本次单元约束成立，不表示后续业务能够承载。

启动基线统计通过，代码与运行根合计548,968,657字节，可用磁盘2,134,941,577,216字节。随后已放行盘点进程，但第一次运行期`_scan`超过固定3秒预算；worker于06:46:21.784354 UTC保存`state=stopped`、`TimeoutExpired`，记录耗时4.372秒。systemd的主进程单调时钟起止差为4.534427秒，唯一离线步骤累计按该值计；没有开启下一步。

失败触发链位于本次Git外[固定runner](../.local/issue-22/tooling/issue22_runner.py)：`sample()`调用有3秒超时的统计子进程，`worker()`在业务启动后调用它，异常先写`result.json`，再按已绑定的精确unit停止。`supervisor.log`还记录停止命令自身收到SIGTERM，unit最终ExecMainStatus=2；不能把预期退出码125当作已观测结果。终态为failed／failed，MainPID=0、ControlGroup为空，已核对无本次存活业务或预览进程。

`result.json`的`samples=0`，没有`samples.jsonl`，`time.txt`为空。因此最大RSS、cgroup内存峰值、候选／登记／临时空间峰值、运行期间最低可用空间均为 **Unknown**，不能把初始化的`max_sampled_*=0`当成实测零。启动基线不是运行峰值；轮询也不是文件系统硬配额。当前证据只确认统计调用超时，具体性能原因留给后续诊断，未以此认定源坏、磁盘满或OOM。

### 实际声明清单与12日覆盖

只在授权的UTC二月、三月目录中列出36个窗内`bview`文件名声明：第一批6个，第二批30个；第二批包含`2026.02/bview.20260228.1600.gz`。残留`inventory.json`仍为中断前的`running`，36条均无完成SHA256，两个批次manifest均不存在，不能拿它直接驱动批次。摘要、普通文件核验、实际Collector／MRT时点及已读取的原始字节量均未形成完整回执。

| 北京时间业务日 | 批次 | 文件名声明候选 | 已完成摘要 | 实际MRT时点 | 快照计算状态 |
| --- | --- | ---: | ---: | --- | --- |
| 2026-02-27 | 第一批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-02-28 | 第一批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-03-01 | 第二批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-03-02 | 第二批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-03-03 | 第二批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-03-04 | 第二批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-03-05 | 第二批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-03-06 | 第二批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-03-07 | 第二批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-03-08 | 第二批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-03-09 | 第二批 | 3 | 0 | Unknown | `not_calculated` |
| 2026-03-10 | 第二批 | 3 | 0 | Unknown | `not_calculated` |

表中`not_calculated`是本次覆盖汇总，未伪造成生产器已提交报告。每一天都是声明存在但未完成输入核验；没有“已证明缺输入”“校验失败日”或“有效空日”的新判定。prepare、独立原始audit、试登记、两批batch、重跑／故障注入、真实HTTP及实际浏览器均未执行；总体／ASN数值与多Peer、多起源、AS_SET、联盟、AS4等真实类别覆盖仍为Unknown，不以原fixture结果补齐。

### 证据保管、检查与下一修复条件

本地证据根为[`.local/issue-22`](../.local/issue-22/)；[句柄及终态](../.local/issue-22/live-handles.json)、[逐日覆盖](../.local/issue-22/coverage.json)、[21份服务器回执摘要](../.local/issue-22/remote-receipt-checksums.json)与[完整单元回执](../.local/issue-22/remote-receipts/receipts/domeye-issue22-20260912-inventory-initial-5ea0365bca58.service/)均保留。21份回执和盘点残留已在本地与服务器逐文件核对SHA256及字节数一致；原始MRT没有复制进本地或Git。

本次停止后未重跑inventory、未改3秒扫描上限或其他资源边界，未生成snapshot或登记，也未启动28621／28623预览、切换共享服务／main／独立A或操作P0。本轮仅同步文档，无产品代码、接口或依赖锁定变更；检查范围为回执一致性、终态／路径、工具与锁文件摘要、逐日声明统计及文档差异，不声称重新通过全套测试、HTTP或浏览器验收。

下一步先在本地诊断并审查扫描预算适配及中断回执完整性，证明监控在原有资源边界下可用；如修订工具，重新固定摘要并审阅新执行计划。原两个目录已占用且现场必须保留，重新真实执行需要明确的新路径／续作规则和授权范围，不能覆盖现场或沿用36条未冻结声明。当前没有足够测量值计算“全部候选工作量×1.5”的GO门槛，不能据候选数直接批准两批。#22与父规格#18的真实生产／消费验收仍未完成，Issue由父任务统一处理。

## 54. Issue #22 扫描修复续作与首候选提前停止

2026-09-12，阶段结论 **REPAIR**。同一授权范围内的续作完成了36个候选文件摘要冻结；首候选prepare开始真实处理，但按既定整批时长估算门槛被父任务明确要求提前停止。没有完成快照或登记，实际12日覆盖仍为Unknown。第53节的扫描超时现场和文档提交`13f7bc3`保留，不把本次盘点成功改写为首次成功。

### 授权续作与固定材料

父任务完成新runner的本地22项人工输入／mock测试及Standards、Spec独立审查，记录两轴剩余0项，随后明确派发一次同范围续作；本执行任务未重跑这22项，也不把本地基准当成服务器通过。范围继续使用RRC25、北京时间02-27至03-10及原两个UTC窗口，生产代码固定`40f66530643fd2032a2b5f94c976d75524f6f639`，复用本次已安装的独立环境，不重新安装依赖。

新增`issue22_runner_scan_v2.py` SHA256为`cd49d9f6290024e57ddff067a44232735632e63376497df4217c3e2e03bf1434`；其完整遍历仍覆盖CODE＋RUN，保留原计数口径和全部失败现场。3秒扫描、1秒轮询、8GiB内存、Swap0、CPU100%、1195＋5秒、32／208GiB早停、40／200GiB目标及3小时累计限制均未扩大。代码及原`input_inventory.py`、audit和preview脚本未改。

新归档9,707字节、SHA256 `83d2604ddb6cc2448f2eb66c60ffdc54ac12c23536f1f36fcb3ffc5dd3a9172f`，只允许两个普通文件成员：新runner和`plans/inventory-scan-v2.json`，排他创建且两端逐文件核对。新盘点计划SHA256为`0dff5035bb0bf18f80bd2a95f33c6915380ceb3681120b5f4360d7cc239b7c4c`；服务器review与冻结review除随机单元标识外一致。prepare计划也绑定新runner、固定入口及三份冻结盘点JSON，仍只调用正式`rib-snapshot.py prepare`，没有替换生产算法。

### 真实盘点已完成，正文仍待核验

新盘点单元后缀为`inventory-scan-v2-15b9e9f25872`，07:05:45—07:07:20 UTC正常退出，worker为succeeded、业务退出0、systemd退出0。单元内再次核验实际`memory.max=8589934592`、`memory.swap.max=0`、`cpu.max=100000 100000`，57次运行期扫描没有触发3秒上限。输出状态为`inventory_complete_content_unverified`。

| 冻结批次 | 声明／不同SHA数量 | 压缩源总字节 | 清单SHA256 |
| --- | ---: | ---: | --- |
| 第一批，02-27—02-28 | 6／6 | 2,558,482,895 | `8b859ebdbb857f55257f1e47723322dd2491f9b5e3230ba61502d1f836b1b267` |
| 第二批，03-01—03-10 | 30／30 | 13,096,322,733 | `d4157b9a6080c22b185e0286514f3a001ad47800b90a82974f4dd7b5c4543644` |

第二批包含UTC二月目录的`bview.20260228.1600.gz`，未按月目录错误截断。36份均完成普通文件定位／摘要，12日各3份候选；这只冻结文件身份与声明，不证明gzip／MRT完整性、实际Collector／时点或业务覆盖。

### 固定首候选与协调提前停止

按冻结UTC时点＋SHA固定排序，唯一首候选为`2026.02/bview.20260226.1600.gz`，426,823,693字节，SHA256 `ffc7e9e86171bd850243bec3eecc50cae632f3918e47e6b81c8b5149b2442f0f`，声明时点2026-02-26T16:00:00Z。单元后缀为`pilot-prepare-scan-v2-f2fbeb4fa56f`，07:08:40 UTC启动并通过相同实际cgroup核验。

截至07:12:52 UTC，该候选已运行超过4分钟且尚未完成。按本次约定的“全部6候选×完整首候选时长×1.5”估算，保守下界已超过`240×6×1.5=2160`秒，不能进入原1200秒整批门槛。父任务据此明确要求立即停止；这是约定估算规则下的准入结论，不是测得整批实际耗时，也没有把部分结果外推为完整处理速度。

07:15:31.548910 UTC核对Meta／Id／Description／Slice后，通过新runner的精确stop入口终止上述首候选单元，stop CLI退出0。worker记录`InterruptedError: 本次 unit 收到信号 15`及stopped，systemd最终failed／failed、ExecMainStatus=125、MainPID=0、ControlGroup为空。停止决定另存`coordinator-stop-decision.json`，与`stop-request.json`区分调度决定和机械停止动作。该次不是扫描失败、自动耗尽20分钟、生产器完成或源损坏；完整源耗时、最终体积、投影正确性均为Unknown。

最后一条**生产器进度**为解压268,437,637字节、89,091条物理记录、3,742,092条RIB观察条目；没有全流独立核对，不能把进度计数当作已验收数据。停止后仅残留190,406,656字节的`snapshot.sqlite`，没有`manifest.json`，没有任何登记目录或批次输出。残留字节数不等于最终投影大小，不用于计算虚假的完成吞吐。

### 资源实测与累计账本

| 步骤 | systemd实际单调时钟耗时 | 运行期采样 | cgroup内存采样最大值 | CODE＋RUN采样最大值 | 结论 |
| --- | ---: | ---: | ---: | ---: | --- |
| 首次旧runner盘点 | 4.534427秒 | 0 | Unknown | Unknown | 扫描超时，第53节 |
| 新runner盘点 | 95.314181秒 | 57 | 8,582,463,488字节 | 549,222,609字节 | 摘要冻结成功，正文未验证 |
| 首候选prepare | 411.135822秒 | 234 | 422,506,496字节 | 739,850,449字节 | 协调提前停止，未完成 |

累计离线510.984430秒，未达到3小时预算；停止依据为上述批次准入门槛。新盘点业务`time.txt`给出93.67秒与最大RSS 19,832 KiB；首候选`time.txt`为空，业务最大RSS为Unknown。两个新单元均没有内核`memory.peak`数值，表中采样最大值不是连续峰值，也不能替代业务RSS。

首候选采样的暂存目录最大值190,418,944字节，最低可用磁盘2,134,584,799,232字节；全程已保存的memory.events未见OOM或OOM kill。这些只覆盖实际运行时段与采样点，不保证轮询间没有更高瞬时占用，不证明4GiB SQLite或两批8GiB预算最终可满足。

### 12日覆盖与未执行阶段

| 北京时间业务日 | 批次 | 候选／完成摘要 | prepare状态 | 实际完整MRT覆盖 |
| --- | --- | ---: | --- | --- |
| 2026-02-27 | 第一批 | 3／3 | 首候选被协调提前停止 | Unknown |
| 2026-02-28 | 第一批 | 3／3 | 未开始 | Unknown |
| 2026-03-01 | 第二批 | 3／3 | 未开始 | Unknown |
| 2026-03-02 | 第二批 | 3／3 | 未开始 | Unknown |
| 2026-03-03 | 第二批 | 3／3 | 未开始 | Unknown |
| 2026-03-04 | 第二批 | 3／3 | 未开始 | Unknown |
| 2026-03-05 | 第二批 | 3／3 | 未开始 | Unknown |
| 2026-03-06 | 第二批 | 3／3 | 未开始 | Unknown |
| 2026-03-07 | 第二批 | 3／3 | 未开始 | Unknown |
| 2026-03-08 | 第二批 | 3／3 | 未开始 | Unknown |
| 2026-03-09 | 第二批 | 3／3 | 未开始 | Unknown |
| 2026-03-10 | 第二批 | 3／3 | 未开始 | Unknown |

上述12天均未生成可用快照，本次汇总状态为`not_calculated`；该状态没有伪装成生产器已提交批次报告。没有新增缺输入日、输入校验失败日或有效空日判定。独立原始audit、pilot register、两批batch、重跑／故障注入、真实HTTP与实际浏览器均未执行；总体／ASN计数、多Peer、多起源及集合／联盟／AS4等实际类别覆盖尚未验收。

### 保管、核对与后续边界

所有新材料位于[`.local/issue-22/scan-v2`](../.local/issue-22/scan-v2/)；[执行判定](../.local/issue-22/scan-v2/gate-result.json)、[累计时长](../.local/issue-22/scan-v2/budget-ledger.json)、[逐日覆盖](../.local/issue-22/scan-v2/coverage.json)、[完整性终态](../.local/issue-22/scan-v2/final-integrity.json)及[33份新回执摘要](../.local/issue-22/scan-v2/remote-new-receipt-checksums.json)可复核。两个新单元的limits／result／samples／time／unit-status及停止回执均已复制，新清单与计划也保存在本地`remote/`对应路径；33份材料逐文件SHA256及字节数与服务器一致。

终态复核原21份失败回执、新冻结清单、原工具、新runner、处理代码及依赖锁均未变；36份源的实体属性与冻结盘点一致，没有再次散列MRT，不能由metadata推断全文再次复核。三个本次单元均无进程且cgroup目录不存在，成功盘点仅保留active／exited的systemd记录；预览28621／28623未启动。共享服务、服务器main、独立A与P0未切换，原MRT和残留SQLite未复制进本地或Git。

本阶段只同步四份中文文档，未改生产代码、接口、锁文件或正在保管的工具。验证为真实盘点／受限测量、精确停止、清单／回执一致性、源码与工具绑定及文档差异／链接检查；没有重新执行全套产品测试或页面验收。父任务继续人工输入下的本地性能热点诊断，另行审查修复与真实续作条件。本执行任务不扩大预算、不换源或循环重跑，不进入后续消费验收；#22及父规格#18仍未完成，由父任务维护Issue。

## 55. Issue #22 新版首候选在只读挂载门禁处停止

2026-09-12，阶段结论 **STOP，prepare 业务未启动**。父任务完成 #23 优化及新 runner 的本地测试／双轴审查后，明确派发固定首候选的一次新版 prepare；本次在实际挂载只读门禁处拒绝，没有测得新版真实吞吐。第53节扫描超时、第54节盘点成功与旧版 prepare 提前停止的现场及原54份材料均保留。

### 固定版本、输入与一次执行范围

新版活动代码固定为 `6dd2d98d36194c215c756b778270ad9f89170b85`，包含 #23 优化；只排他安装到本次运行根 `code/issue23-first-pilot`，detached HEAD 与跟踪文件状态核对通过。旧代码根保持 `40f66530643fd2032a2b5f94c976d75524f6f639`，依赖锁相同且未安装新依赖。目录权威位置见[运行手册](runbooks/运行与维护.md#issue-22-新版首候选只读门禁终态)。本地文档分支在执行停止后快进到同一 `6dd2d98`，保留 #23 已合并的实现与架构说明。

| 冻结材料 | SHA256 |
| --- | --- |
| `issue22-code-issue23.bundle`，1,115,655字节 | `f1b43b0622112e8083b0b3eeb20cdbbf7b40bba6fa40ae480162104803481135` |
| `issue22-tooling-issue23.tar.gz`，11,716字节 | `48bcaca78d70b5622e4b8ff6639b23ffe11b316bd74f1cd775b43fb00a9caaf6` |
| `tooling/issue22_runner_issue23.py` | `f2f396e463d4be183a753580ebe4bdad17f38571d3712cd09b5a87514097c270` |
| `plans/pilot-prepare-issue23.json` | `2fb2dda0a3a20a5600b180963c89181552f7e90f6324f40935058b5e2cdeb27a` |

代码包唯一 HEAD、工具包两个普通文件成员、传输前后摘要及服务器 review 均与冻结材料一致。首候选仍为第54节的 `bview.20260226.1600.gz`、426,823,693字节、SHA256 `ffc7e9e86171bd850243bec3eecc50cae632f3918e47e6b81c8b5149b2442f0f`，声明时点为2026-02-26T16:00:00Z，Collector为RRC25。计划仅运行正式 `rib-snapshot.py prepare`，输出绑定新的 `pilot-prepared/issue23-first`；没有换源或重新盘点。

资源边界保留3秒完整扫描、1秒采样间隔、8GiB内存、Swap0、CPU100%、1195＋5秒、32／208GiB早停、40／200GiB目标及3小时累计限制；旧 CODE 与整个 RUN 都计入空间。协调器按15秒间隔观察，如超过150秒仍未完成则按授权条件精确停止。实际门禁在0.175165秒内拒绝，未触发150秒协调条件，后续阶段未获本次放行。

### 已观测失败与不能补齐的 worker 证据

唯一新增单元 `domeye-issue22-20260912-pilot-prepare-issue23-a42dfd38b1b5.service` 于07:48:35 UTC启动。worker 的 `readonly_mount_proof()` 对旧 CODE、tooling、新活动代码执行 `statvfs(...).f_flag & ST_RDONLY`，在 `all(proof.values())` 检查处抛出 `ValueError`，记录“旧代码／新代码／工具的实际挂载并非只读，业务未启动”。逐路径映射在异常前未保存，具体哪个路径为false仍为 **Unknown**，不能把合取失败写成三者均可写。

systemd记录 `ProtectSystem=strict`、`ReadWritePaths=RUN`，`ReadOnlyPaths` 显式列出 tooling 与新活动代码，未列旧 CODE。门禁异常发生在完整 `limits-verified.json` 写出之前，`environment-verified.json`、`baseline-disk.json`、业务 stdout／stderr／time 与 `samples.jsonl` 均不存在。配置属性存在不等于已取得 worker 实际隔离及环境核验证据；宿主机通过的 Python／标准库预检也不能代替本轮缺失回执。

worker `result.json` 为stopped、耗时0.011358952秒、样本0；初始化 `max_sampled_*=0` 不是实测零。业务最大RSS、内存／磁盘运行峰值、完整源耗时、最终投影大小与正确性均为Unknown。supervisor记录其停止命令自身收到SIGTERM；最终systemd为failed／failed、退出2、MainPID=0、ControlGroup为空，cgroup目录不存在。本次没有 `stop-request.json` 或协调停止决定文件，不能写成150秒早停、源损坏、OOM或 #23 性能失败。

### 只读平台取证与诊断方向

07:52:59 UTC只读补证确认实际 systemd 为 `249 (249.11-0ubuntu3.22)`，`systemd-detect-virt` 返回none（退出1）；PID1的有效能力包含CAP_SYS_ADMIN，NoNewPrivs=0、Seccomp=0。只读取该unit在07:48:30—07:49:10 UTC的限定日志，共6行启动／退出／停止记录，没有额外namespace、mount或权限失败消息；日志未报告不代表所有隔离步骤已通过。

当前SSH与PID1挂载namespace均为 `mnt:[4026531841]`，旧 CODE、RUN、tooling和新活动代码在该视图命中根ext4挂载、rw，statvfs标志为4096。原worker挂载namespace已退出，以上是宿主机当前读数，不能充当worker内读数，也不能据此认定systemd命名空间未生效。补证未新建unit或写服务器文件。

[systemd v249官方实现](https://github.com/systemd/systemd/blob/v249/src/core/namespace.c#L182-L198)将 `/home` 保留为 `READWRITE_IMPLICIT`，由 `ProtectHome` 单独控制。结合旧 CODE 位于 `/home` 且本次 `ReadOnlyPaths` 未显式列入它，**配置遗漏是待验证的诊断方向**。当前没有原worker逐路径证据，尚不能把该方向认定为全部失败根因或平台失去隔离能力；本轮不修订配置、不降低门禁、不重试。

### 累计时间、覆盖与保管终态

systemd主进程单调时钟起止为3299833837128／3299834012293微秒，本次耗时 **0.175165秒**；加此前三步510.984430秒，累计 **511.159595秒**。观察器等待到15秒才见终态，不将其等待时长计作离线业务耗时。本次停止由只读门禁触发，不是3小时预算耗尽或整批测量结论。

沿用第54节冻结的36份摘要和12日各3份候选，本次结束仅复核源metadata不变，未重新全文散列或解析。新版输出目录不存在；旧partial SQLite仍为190,406,656字节且无manifest。12日完整MRT覆盖、总体／ASN计数及真实类别覆盖继续为Unknown；没有新增缺输入日、校验失败日或有效空日。独立audit、登记、两批、故障注入、HTTP与浏览器均未开始。

新材料位于[`.local/issue-22/issue23-first`](../.local/issue-22/issue23-first/)；[执行判定](../.local/issue-22/issue23-first/gate-result.json)、[逐日覆盖](../.local/issue-22/issue23-first/coverage.json)、[累计时长](../.local/issue-22/issue23-first/budget-ledger.json)、[平台只读证据](../.local/issue-22/issue23-first/platform-readonly.json)、[完整性终态](../.local/issue-22/issue23-first/final-integrity.json)及[11份新服务器材料摘要](../.local/issue-22/issue23-first/remote-new-receipt-checksums.json)可复核。11份包含单元现存回执、安装／终态回执和计划，已逐文件核对本地与服务器字节及SHA256；缺失文件按上文明确列出，没有补造成功回执。

终态确认原54份材料、20份旧小文件／清单、旧新代码与冻结新包均未变，四个本次单元均无进程或cgroup目录，28621／28623未启动。共享服务、服务器main、独立A与P0未修改；原MRT与SQLite未复制到本地或Git。本阶段仅改四份中文文档，检查回执一致性、绑定、时长、缺失证据及文档差异／链接；未重跑父任务测试或进行产品验收。后续修订及真实续作由父任务另行审查，#22／#18尚未完成，GitHub由父任务统一处理。

## 56. Issue #22 只读对照通过与新版首候选按估时门槛停止

2026-09-12，最新阶段 **REPAIR**。父任务先明确派发两个只读诊断单元，核对终态后再单独派发一次新版 prepare。两次对照与 prepare 实际门禁均通过；首候选在150秒仍未完成，按既定整批估时准入规则协调停止。第53—55节及原始失败回执保留；本次没有完成快照、独立audit或真实12日覆盖验收。

### 两个只读单元的实测对照

固定 `readonly_probe.py` 为2,476字节、SHA256 `7e08eab16105a283c23408f9303fa27378cc432796996fb3c55ecf370c3e69d7`；诊断计划SHA256 `cd73b6cdceedb6fb2f1fd68df6a4197e77b045a6a1d7b080b831ad8ae3f27272`。只排他安装新探针，保留原systemd资源／隔离属性，探针内部10秒alarm，不导入业务或读取MRT正文。两步使用现场新nonce，原始映射及终态通过后才进入下一步。

| unit后缀／实际单调耗时 | 旧CODE | tooling | 新活动CODE | RUN |
| --- | --- | --- | --- | --- |
| `ro-probe-baseline-6e15be4d5aa8`／0.115955秒 | false，4098，命中`/home` rw | true，4099 | true，4099 | false，4098 |
| `ro-probe-repaired-0af82d6e6c75`／0.119796秒 | true，4099，命中自身ro挂载 | true，4099 | true，4099 | false，4098 |

表中数值为本unit内实存的statvfs `f_flag`，`ST_RDONLY=1`；对应mountinfo命中行同时保留。两个启动argv规范化身份字段后，唯一差异是 `ReadOnlyPaths` 追加旧CODE。两unit均退出0、active／exited、MainPID=0且cgroup目录不存在，实际限额均为 `memory.max=8589934592`、`memory.swap.max=0`、`cpu.max=100000 100000`。这确认了旧配置遗漏及最小修复在新诊断单元中的作用，不补造第55节原worker缺失映射；先后出现的相同namespace编号不能证明它们曾同时共享namespace。两步后累计511.395346秒。

### 新版一次 prepare 的固定绑定与门禁

父任务验收上述对照后，另行派发新 runner `issue22_runner_issue23_ro.py`（31,001字节，SHA256 `1f1c17789b737e152f59e354e4bf7c656720281cff5a79ea9f8c8f9ecab9a108`）与计划 `pilot-prepare-issue23-ro.json`（1,620字节，SHA256 `c246a0cc51c1b371d4c57c3af4d8c89aea1247cedeeb02c9e64a8f648752e45e`）。两文件排他安装且传输前后摘要一致；服务器review与冻结版本除nonce外完全相同。

生产代码仍固定 `6dd2d98d36194c215c756b778270ad9f89170b85`，旧代码／解释器位置保持 `40f66530643fd2032a2b5f94c976d75524f6f639`。仍仅处理固定首源 `bview.20260226.1600.gz`、426,823,693字节、SHA256 `ffc7e9e86171bd850243bec3eecc50cae632f3918e47e6b81c8b5149b2442f0f`，正式prepare入口及完整源摘要校验规则未绕过。新输出绑定 `pilot-prepared/issue23-first-ro`；没有换源、安装依赖或修改生产代码。

单元 `domeye-issue22-20260912-pilot-prepare-issue23-ro-0519de418823.service` 于08:07:13 UTC启动。`readonly-mounts.json`先保存三路径均4099／true；`limits-verified.json`记录原实际cgroup限额及三路径只读；`environment-verified.json`记录Python3.10.12、`-I -S -B`、新活动代码的四模块来源／摘要、无旧业务模块、固定锁及数据档。这次已取得worker成功门禁证据并启动正式业务，与第55节业务启动前STOP区分。

3秒完整扫描、1秒采样间隔、8GiB／Swap0／一核、1195＋5秒、CODE＋整个RUN空间总账、32／208GiB早停及3小时累计均未放宽。启动前109份固定小文件及六个既有unit终态通过；代码加运行目录747,382,993字节、可用空间2,134,623,440,896字节，新输出尚不存在。

### 150秒协调停止、资源与未完成结果

08:09:43.703377 UTC，协调器按15秒间隔观察到unit实际已运行150.284976秒且仍无manifest。按约定“全部6候选×完整首候选时长×1.5”，估算下界超过 `150×6×1.5=1350` 秒，超出原1200秒整批门槛，随即保存协调决定并用新runner精确stop。决定与机械停止分别保存在 `coordinator-stop-decision.json` 和 `stop-request.json`；这不是1195秒自动超时、已知完整源耗时或源损坏。

systemd主进程单调时钟3300951435011→3301101841002微秒，实际 **150.405991秒**；worker记录stopped、信号15、150.231058秒。终态failed／failed、退出125、MainPID=0、ControlGroup为空且cgroup目录不存在。加前六步511.395346秒，累计 **661.801337秒**，未耗尽3小时预算。

| 已保存资源证据 | 本次值 | 适用边界 |
| --- | ---: | --- |
| 运行期样本 | 84 | 完整保留，未触发3秒扫描失败 |
| cgroup内存采样最大值 | 223,289,344字节 | 非连续内存峰值或业务RSS |
| CODE＋RUN采样最大值 | 847,734,993字节 | 包含原依赖、工具、回执及旧新半成品 |
| 运行期最低可用空间 | 2,134,373,969,920字节 | 仅覆盖已采样时点 |
| 全部`pilot-prepared`目录采样最大值 | 290,611,200字节 | 含旧残留，不能冒充本次单目录最终体积 |
| 业务最大RSS／内核`memory.peak` | Unknown／Unknown | `time.txt`为空，全部样本peak为null |

已保存memory.events未见OOM或OOM kill；采样不能排除轮询间更高瞬时占用。最后一条生产器进度为解压134,217,803字节、44,415条物理记录、1,873,711条RIB观察，未做全流独立audit，不能当作全文计数、完成比例或吞吐测量。

本次只残留100,184,064字节的`snapshot.sqlite`，无manifest；旧partial仍为190,406,656字节且无manifest，第55节输出目录仍不存在。固定版本之间的partial字节或进度不能直接比较为完整源性能收益。12日各3份候选及36份冻结摘要沿用第54节；终态仅复核metadata不变，未重新全文散列。完整源耗时、最终投影大小、总体／ASN数值、实际完整MRT与类别覆盖仍为Unknown；没有新增有效空日、校验失败日或缺输入日判定。

### 回执保管、验证与后续边界

两诊断的[证据目录](../.local/issue-22/readonly-probe-live/)保存18份服务器回执及[对照结果](../.local/issue-22/readonly-probe-live/comparison-result.json)。各诊断的`result.json`只是兼容既有stop入口的非成功初始占位，原始证据用`probe-result.json`，终态用`coordinator-terminal.json`／`final-result.json`。新版prepare的[证据目录](../.local/issue-22/issue23-first-ro/)保存21份新服务器回执／计划及[判定](../.local/issue-22/issue23-first-ro/gate-result.json)、[资源](../.local/issue-22/issue23-first-ro/resources.json)、[累计账本](../.local/issue-22/issue23-first-ro/budget-ledger.json)、[逐日覆盖](../.local/issue-22/issue23-first-ro/coverage.json)、[终态绑定](../.local/issue-22/issue23-first-ro/final-integrity.json)和[文件摘要](../.local/issue-22/issue23-first-ro/remote-receipt-checksums.json)。两组共39份材料均逐文件核对本地与服务器字节／SHA256。

终态111份固定小文件摘要未变，其中83份既有回执／计划／清单保留；36份源metadata不变，七个unit均无残余进程或cgroup目录。原MRT／SQLite未复制到本地或Git，28621／28623未启动，共享服务、main、独立A与P0未改。本阶段只同步四份中文文档，检查原始样本汇总、时间、绑定、回执、完整差异和新增链接，未重跑全套产品测试或进行产品验收。

本轮不自动retry、audit、register、batch、故障注入、HTTP或浏览器验收，也不继续按150秒循环试跑。整批仍REPAIR，父侧评估原限额内的单源完整成本测量，尚未放行；该测量不等于两批准入，也不保证限额内必然完成。#22／#18仍未完成，GitHub由父任务统一维护。

## 57. Issue #22 首源生产完成与验证收口

2026-09-12，固定首源已完成 prepare 与内置完整验证，**单候选 GO**。用户随后明确「没必要为了“审计”再花接近一次完整生产的成本。验证部分就可以了」，额外全量重放审计已精确停止。正式候选保留，收口采用生产阶段已完成的验证及小文件绑定／终态检查；不再重复全流解析或全库核验。该结论不代表独立审计通过、登记完成或36份候选／12日全部生产完成。

### 同源执行与时间决定

沿用第54节冻结首源 `/home/bgpdata/data/ripe/rrc25/2026.02/bview.20260226.1600.gz`，426,823,693字节，SHA256 `ffc7e9e86171bd850243bec3eecc50cae632f3918e47e6b81c8b5149b2442f0f`。观察点为RRC25，时点 `2026-02-26T16:00:00Z`，业务日期为北京时间02-27；没有换源。运行根仍为 `/home/bgpdata/domeye-new-runtime/rib-pilot-20260912-issue22`，下表unit全名统一前缀 `domeye-issue22-20260912-`、后缀 `.service`。

| 本阶段unit／UTC时间 | systemd实际单调耗时 | 结果 |
| --- | ---: | --- |
| `pilot-complete-issue23-ro-7a2af257d918`，08:20:12→08:40:07 | 1,195.208571秒 | 原1195秒运行限额超时；只保留942,014,464字节partial，无manifest |
| `pilot-finish-explicit-time-112316fdded3`，08:57:24→10:53:56 | 6,992.037102秒 | worker succeeded、systemd success、退出0，正式manifest已生成 |
| `pilot-audit-unlimited-00581458b973`，11:32:17→11:47:51 | 934.503313秒 | 用户主动取消，worker stopped、退出125，未生成独立审计报告 |

完整成本测量沿用6dd代码，首行超时来自整个unit的Runtime限制；停止时扫描子进程收到SIGTERM不是再次发生3秒扫描故障。原残留及第53—56节失败现场均保留。成功生产另用新目录和固定 `95de9bbfececdad3554d215f4e9d1b02059a6468`，CLI摘要 `c6bc99105785bd15d365c2fa2be266d57d1285acb8460d4ce9347ad340a83c03`，复用原独立Python3.10.12；原40f／6dd代码、依赖和锁未替换。

成功生产最初显式采用7200秒参数；累计上限先为10,800秒、后获准18,000秒。运行中用户又明确“不要限制时间，跑完数据为止”，撤销同源生产及当时必要验证的单步／累计截止，其他资源与来源边界保留。直接 `systemctl set-property` 修改被系统拒绝且未生效；随后仅为该unit写入 `/run/systemd/system/<unit>.d/90-user-unlimited-time.conf`，重载后实读Runtime为infinity。旧CLI的现场处置仅通过ptrace GETSIGMASK／SETSIGMASK为单线程业务进程增加SIGALRM阻塞位并分离，未修改指令或数据，也没有调用alarm(0)删除内核计时器。原始授权与操作回执保留；实际生产在原7200秒前已完成，不能声称验证了越过原截止的运行行为。

当前源码另已支持原生 `prepare --max-seconds 0`，不是此次已完成候选的生产代码；审计工具使用原生0及systemd infinity。用户最新取消额外重放后即停止，不以取消时限为继续审计的理由。十个离线unit累计 **9,783.550323秒**，仅如实记账，不再按历史上限判定剩余额度。

### 已完成候选、计数和验证

候选目录为运行根 `pilot-prepared/first-explicit-time/`，版本为 `rib_snapshot_v1_64882fe01d19d942450814ace80382ef1676979d26017972d1ecd79a3f50a8c4`。

| 完成文件／身份 | 大小或SHA256 |
| --- | --- |
| `snapshot.sqlite` | 2,553,282,560字节；`ecd8b28d7b79a59eed1ea29f9f212ae3dd8cced80a0b00e35c50c3fc3068ff62` |
| `manifest.json` | 2,192字节；`4e4039ac494557b159bce08b05f2d58ecb1a60f8e477abb3d1241f4eae3ad29d` |
| `execution.json` | 385字节；`dbae44b8296bf6d289fd652413e296ca3ad83a2b84d10e60a5b85c2512e1a1f6` |
| 逻辑摘要 | `de042933304e77f87b5af98aeed080bbd74ad73c3f668c689afdca6309753495` |

生产器完整读取gzip至EOF，解压 **4,339,022,582字节**，共 **1,398,417条MRT物理记录**。按固定实现完成源／数据库摘要、SQLite integrity_check、Prefix与起源关联双向对账、观察总数、分族统计和逻辑摘要核验后，原子建立正式manifest。数据库SHA取自该完整校验及完成清单；收口没有再次对2.55GB数据库或原源全文散列。`execution.finished_at=10:18:36.084249Z`处于内置完整验证之前，不能作为整个prepare完成时间；整步完成以10:53:56的unit成功终态为准。

| 地址族 | 可见Prefix | 明确起源AS | RIB观察条目 | 无明确起源Prefix | 无明确起源观察 |
| --- | ---: | ---: | ---: | ---: | ---: |
| all | 1,398,416 | 85,395 | 55,715,497 | 251 | 8,434 |
| IPv4 | 1,131,541 | 78,104 | 45,462,838 | 215 | 7,358 |
| IPv6 | 266,875 | 36,264 | 10,252,659 | 36 | 1,076 |

all的AS数为双栈并集，不能相加两个分族AS数。上述结果只覆盖这一RIB时点；Session与采集覆盖仍为Unknown，不代表连续RouteState、整日控制面完整性或实际网络／用户影响。12日各3个候选仍沿用原盘点，本次完成其中一个来源，未新增按日成功、有效空日、校验失败日或缺输入日判定。

### 资源、取消记录与收口边界

成功生产保留3,824个样本：cgroup内存采样最大2,883,321,856字节，业务进程最大RSS为233,660 KiB，CODE＋整个RUN采样最大4,355,266,769字节，最低可用空间2,130,193,457,152字节。全部prepared目录采样最大3,785,924,608字节包含旧partial，不是本候选体积。采样峰值与业务RSS口径不同；内核memory.peak为Unknown，已存样本未见OOM／OOM kill。一核、8GiB、Swap0、SQLite4GiB、3秒完整空间扫描、32／208GiB早停及原源／解压／记录数限制保留。

审计取消请求原文与11:47:51.707291 UTC执行记录保存在该unit的 `user-stop-request.json`；随后通过固定runner的精确stop入口终止整个cgroup。systemd failed／退出125是本次协调取消的机械终态，**不表示发现数据校验失败**；独立报告不存在，也不产生独立审计PASS。审计运行521个样本，内存采样最大80,162,816字节，CODE＋RUN采样最大4,355,827,921字节，最低可用空间2,130,125,090,816字节；time文件为空，业务最大RSS与内核peak均Unknown。

终态核对185份固定小文件SHA不变、三个代码HEAD不变、36份源metadata不变；十个unit均MainPID=0、cgroup目录不存在，完整候选及旧partial文件名／大小保持。源metadata不变不冒充再次全文校验。原MRT与SQLite正文仍留服务器，只回收小文件；前次成本测量18份、成功生产21份、审计取消23份服务器小文件各有本地／远端字节和摘要对照。

Git外证据分别位于[完整成本测量](../.local/issue-22/issue23-first-complete/)、[成功生产](../.local/issue-22/first-explicit-time-complete/)及[审计取消与验证收口](../.local/issue-22/audit-unlimited-complete/)。各目录保留原始回执、`remote-receipt-checksums.json`、`final-integrity.json`、`resources.json`、`budget-ledger.json`和`handoff.json`；旧阶段摘要反映各自当时状态，最新用户决定见最后一组，不改写原始证据。

本阶段只同步四份中文文档及Git外结果摘要；验证回执、数量、绑定、终态、完整差异和新增链接，未重复完整生产／核验或产品全套测试。未执行register、batch、新来源、故障注入、HTTP／浏览器或部署；共享服务、服务器main、独立A与P0未修改。#22／#18的两批与消费验收尚未完成，GitHub由父任务统一维护。


## 58. Issue #22 无时限第一批启动

最新状态：第一批真实6候选于2026-09-12 12:16:08 UTC启动，14:58:43 UTC因监控扫描超时停止，**REPAIR，尚未成功登记或验收**；第二批30候选未启动，12日覆盖仍为Unknown。用户取消的额外全量重放审计不再作为门禁，保留第57节首源生产内置完整验证和必要低成本检查。

#24 产品提交`7226ff9c19a9393717fefcfd258a386db7a26326`，CLI SHA256 `a44f15743214352ea5fa363084092f186501c541a32f9e8ca80a1ba82d5f366b`。受影响109项、完整后端657／前端163项、类型检查通过；产品两轴0项。执行器14项fixture通过，唯一Spec边界P2修复并独立复审0剩余，Standards初审和父侧增量0项。它们证明本地实现，不代替真实两批验收。

执行沿用第54节固定6／30来源与三清单摘要，代码新增至原运行根`code/issue24-batch-unlimited`；新工作根为`issue24-batch-unlimited`，输出`executions/batch-1`，登记`registry`。准确unit为`domeye-issue22-20260912-batch-1-10a196d9b5b7.service`，回执在新工作根`receipts/<unit>`。新runner SHA256 `bfe2fe85aab5f738451c63689c184f165b30ee20c5746cbe1fcf6651deaa81c5`，原始首批计划SHA256 `2ec878dc624f0c5eb6676667035e87578bfaef3764989f1e79ad8abfd712b41d`。两批均将绑定这套新代码，不改标95de已完成测量候选。

父侧在用户已授权自主决定并完成本次数据的范围内，按独立复核的磁盘构成明确采用完整CODE+RUN的144GiB提前停止／160GiB保护线，替代本次两批的旧32／40GiB设置；原历史方案和旧工具不改写。单库4096MiB、第一批25600MiB／第二批91136MiB批次预算、空闲208／200GiB、一核、8GiB和Swap0保留。第一次装载要求既有≤16GiB，第二次≤36GiB并包含首批全部产物；两批计划峰值分别≤130／134GiB，均低于136GiB。临时日志16GiB是预留，扫描轮询不等于硬配额，不据此保证必然成功。业务单步／累计时间截止均取消，3秒单次扫描故障保护仍保留。

12:13:42 UTC新鲜检查：完整占用4,355,864,785字节、空闲2,130,130,038,784字节；同设备、三清单SHA、36源stat和十旧unit无PID/cgroup均通过。12:15:57完成排他安装并核对新闭包。12:19:05只读启动检查确认active/running、业务进程处于同一cgroup，参数匹配完整首批；实际Runtime为infinity、Stop5秒、一核／8GiB／Swap0，九处只读／两处可写及实际新模块加载均通过。此处只有启动实证，尚无批次成功结论。

本阶段材料在[本地交付目录](../.local/unified-data-foundation/issue24-batch-unlimited/)：执行决定、冻结清单、两轴报告、fresh-preflight、install-batch、batch-1-submit、startup及后续live-status。原始MRT和SQLite未回收至Git。第一批完整成功后，第二批只填写并冻结实际首批回执与四份小文件SHA，依同一runner核对登记及资源再派发；不减少候选或增加日期常量。共享服务、已发布C55／4、独立A及P0不切换；#22/#18与整体Goal保持开放。

### 第一批停止的实际终态

2026-09-12 15:01:59 UTC只读复核：上述同一unit为failed/failed、MainPID=0、ControlGroup为空、cgroup不存在；worker为stopped，实际运行9753.889434582088秒。直接停止原因是一次`_scan`子进程超过3秒，触发执行器的异常停止；不是业务单步或累计截止。`stop-request.json`不存在，journal记录了14:58:43 UTC停止与退出状态2；不能把systemd退出状态2改写为源数据验证失败。

已成功记录5348次资源采样，采样最大完整CODE+RUN占用9,076,795,601字节，最低空闲2,125,180,407,808字节，最大cgroup内存5,493,297,152字节。最后一份样本的memory.events各项为0；这些记录没有显示磁盘或内存达到停止线，但不声称连续峰值，也不证明扫描变慢的具体系统原因。

首源新7226候选已完成prepare及内置完整验证，manifest为2192字节、SHA256 `639cd4a04d191464b01c155bd55d845b432c306de3c88ec5746e4bad1b7fce5f`；与第57节旧首源的各项计数、来源和数据库声明摘要相同，版本保持各自生产身份。逻辑摘要因CLI实现身份进入summary而不同，已通过源码和小文件绑定核对，无须为此追加全量重放。第二源`8e48479441cd8f1766609244030ee7fed1a08f615206edbd08a958151adad1b5`仅留下2,159,386,624字节SQLite，没有完成manifest；整批没有report、提交回执或成功登记。已完成来源与中断来源均原位保留，没有重启或跳过候选。

终态小回执及摘要位于[本地诊断目录](../.local/unified-data-foundation/issue24-scan-timeout-diagnosis/terminal-20260912T145843Z/)。现有batch不支持断点恢复；fixture已连续两次复现一次扫描超时即停止健康生产的策略，正在核查既有完成制品的安全复用。停止后同一扫描器三次只读耗时为0.358、0.267、0.258秒，未复现持续扫描迟缓；实际超时的系统原因仍为Unknown。恢复尚未实现或执行。原业务资源边界、36个候选和两批完成条件保持，#22/#18与整体Goal开放。

## 迁移人工合流基线（2026-09-13，非真实数据验收）

本地已接受的观察catalog增量与双Feature冻结源码完成独立人工联合验证；具体输入版本、合并差异、12项本次检查及Git外证据见[迁移人工集成记录](reviews/迁移人工集成基线-1d00-b4f.md)。该记录仅为人工合流GO，真实D、其他计算、全链发布与前端仍未验，不改变上述生产数据历史及原迁移计划范围。

上述efb人工基线后，已接受的Resource d27b以固定双亲无冲突合入，并在同一人工观察/CSV上与Feature联合验证。当前增量及2项本次检查见[Resource人工合流记录](reviews/迁移人工集成增量-Resource-d27b.md)；仍不构成真实D、全部模块或页面验收。

44e之后的Feature f1cf多视图人工合流已完成，见[Feature多视图集成记录](reviews/迁移人工集成增量-Feature-f1cf.md)。P/D固定观察上的连续私有状态与Resource共用输入通过2项本次联合测试；真实P/D、Detection集成及全链仍未验。Resource历史内容按未承诺行序的完整多重集合核对，文件与回执字节保全另验。

90e后已接受Detection c133人工同源接入，详见[Detection联合记录](reviews/迁移人工集成增量-Detection-c133.md)。同一D保存11类参考供正式三计算读取，Detection产生非空修订/判定/状态并核对typed PG与固定湖历史；本次2项联合测试通过。Detection单run冷启动与Feature P/D连续初态分别保留，未建立业务publication或真实数据验收。

8126之后仅将已接受Replay表示优化7449688合入未来候选，现有同源三计算人工用例1 passed、0 skipped，细节见[Replay人工增量记录](reviews/迁移人工集成增量-Replay-7449688.md)。运行变动仅state.py，真实D726未替换或停止；本片不提供真实Linux、全天或48GiB容量证明。

4eeee9之后已接受Feature诊断810fb334人工合流，见[Feature诊断集成记录](reviews/迁移人工集成增量-Feature诊断-810fb334.md)。最终3项人工检查通过；非空诊断固定回读及后续三计算写入后的旧八表／文件保全通过，仍不构成真实处理或容量验收。

bc95之后Country C1修复481cd7f完成同源人工输入集成，见[Country C1联合记录](reviews/迁移人工集成增量-CountryC1-481cd7f.md)。最终1项联合检查通过；同一D／Detection／11参考完整枚举为input_validated、complete_empty，旧历史与文件保全通过。仅输入适配，不代表国家计算或真实处理完成。

d9fb之后Detection公共typed Reader 0f2完成人工联合，见[typed Reader集成记录](reviews/迁移人工集成增量-DetectionTyped-0f2d3f6.md)。本次2项检查通过（实际联合1＋mock预算兼容1）；原两表／旧Reader／C1输出保持，末尾scope漂移拒绝，仍非真实处理验收。

ea249之后Q1.1修复1f352完成人工共享发布查询联合，见[Q1.1集成记录](reviews/迁移人工集成增量-Q11-1f352c9.md)。最终1项检查通过；仅fixture Resource／Feature经固定token发布读取，换head旧分页与组件文件保全通过。Detection／C1仍未业务发布，无真实处理或前端验收。

e7a之后Country C2修复83837完成人工联合，见[C2集成记录](reviews/迁移人工集成增量-CountryC2-83837b0.md)。主同源链完整消费为空，独立两国附加链410条C2Row＋Completion通过；两项不同检查及批1/2多重集核验完成，原Q1旧token与文件保全。未业务发布Country或处理真实数据。

49df之后Q3-A62669完成人工历史载体并存验证，见[Q3-A集成记录](reviews/迁移人工集成增量-Q3A-62669e9.md)。最终11项不同检查通过；源移除后五表完整重建、默认实际批读批写和最终门禁通过，现有联合/Q1旧token保持。不是历史业务发布或真实H导入。

aa276之后Q2-b4消息证明修复完成人工联合，见[Q2集成记录](reviews/迁移人工集成增量-Q2-b4bc174.md)。真实旧Q1目录显式迁移v2后七页不变；主同源两事件与独立17消息异常链分别通过，最终5项不同检查通过。仅两个fixture选择器，非完整业务P或历史发布。
