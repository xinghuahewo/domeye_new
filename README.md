# Domeye New

Domeye 的路由观测工作台：Vue 前端、Flask 只读 API，以及独立的数据计算、结果交付与核验工具。首页 C 面向整体控制面态势，不围绕单个 ASN 展开；单观察点的数据不能当作全网或实际用户影响。

## 功能入口

在[路由观测工作台](http://10.99.8.16:28471/)查看态势、事件和国家／ASN 特征，在[问数页面](http://10.99.8.16:28684/)选择数据批次并提问。

| 功能 | 可以查看什么 | 使用说明 |
| --- | --- | --- |
| 核心态势与异常检索 | 所选日期或时间段的事件、筛选数量和小时趋势 | [选择数据与完成查询](docs/usage/guides/query-data.md) |
| 国家中断详情 | 检测时间、事件状态、最后观测时刻、峰值及峰值 AS 名单 | [查询与解释事件](docs/usage/guides/event-analysis.md) |
| 国家同期统计 | BGP 路由活动、IPv4 地址及 /24 块数、IPv6 /48 块数、AS 与前缀中断六张图 | [详情页的六张图](docs/usage/guides/event-analysis.md#详情页的六张图) |
| 国家与 ASN 特征 | 指定窗口的活动、资源状态、关联事件和按既定范围的排名 | [指标目录](docs/usage/metrics/README.md) |
| 国家两窗比较 | 通告与撤回合计、资源末态、并发中断末样本及适用的变化量 | [两窗比较](docs/usage/api/read-api.md#同一国家两窗比较) |
| 快照与资源 | 已登记 RIB 的规模、明确起源 ASN 资源及可用路径对照 | [快照指标](docs/usage/metrics/snapshots.md) |

查询只读消费已经生成的数据。可用对象、日期、实际覆盖与数据版本由响应给出；缺失和未知保留为空值，不能解释为零。指标单位与结果解释见[系统使用文档](docs/usage/README.md)。

## 快速开始

需要 Python 3.10、uv、Node.js 20+ 和 npm。依赖约束与锁分别见[后端清单](backend/pyproject.toml)、[前端清单](frontend/package.json)及相邻锁文件。

```bash
make setup
make test
make api-types
make build
```

命令定义以[Makefile](Makefile)为准。测试仅用 fixture、mock、临时目录；构建不会启动或部署服务。源码包不携带运行配置、凭据、数据库、INFO、MRT 或 C 消费制品。

前台调试使用 `make backend`／`make frontend`，先按[运行手册](docs/runbooks/运行与维护.md)核对端口与配置，不与常驻服务重复启动。

## 查找源码与命令

后端业务在 [`backend/data_pipeline/`](backend/data_pipeline/README.md) 按职责分包，HTTP 入口在 `backend/web/`，测试统一在 `backend/tests/`。脚本按 `native`、`pipeline`、`rib`、`core_overview`、`benchmarks` 分类；`make backend`、`make frontend`、`make backend-test` 与 `scripts/observations.py` 主入口保持原位。

完整目录职责以[源码目录与入口](docs/architecture/系统结构与数据流.md#源码目录与入口)为准；移动后的命令示例见[运行手册](docs/runbooks/运行与维护.md#源码整理后的开发命令)。历史评审保留当时的执行路径和结果。

## 文档导航与权威位置

| 要查找的内容 | 权威位置 |
| --- | --- |
| 系统使用、指标定义、业务查询与结果解释 | [系统使用文档](docs/usage/README.md) |
| 当前实现、入口、数据流与旧能力边界 | [系统结构与数据流](docs/architecture/系统结构与数据流.md) |
| 数据流水线加速改动、历次实测与当前困境 | [数据流水线加速改动与当前困境](docs/architecture/数据流水线加速改动与当前困境.md) |
| 领域术语 | [CONTEXT.md](CONTEXT.md) |
| 协作规则 | [AGENTS.md](AGENTS.md) |
| C 页面使命、口径、有限交付范围 | [C 短规格](docs/core-overview-v1.md) |
| 统一生产与消费底座的规划、首试验收与任务依赖 | [规格 #18 及子任务](https://github.com/xinghuahewo/domeye_new/issues/18) |
| 数据来源、准入、验收证据与历史 | [数据制品台账](docs/data-assets-and-admission.md) |
| 常驻服务、部署、回滚与恢复 | [运行与维护](docs/runbooks/运行与维护.md) |
| 本机预览及既有独立恢复命令 | [C 本地验收与恢复历史](docs/runbooks/C本地验收与恢复.md) |
| 业务查询边界、API 完善与遗留退役 | [业务只读 API 设计与退役](docs/architecture/业务只读API设计与退役.md) |
| HTTP 合同与数据时间配置 | [OpenAPI](contracts/openapi.json)、[数据档](config/data-profile.json) |
| 使用文档的源码映射、核对范围与未决口径 | [系统使用文档维护](docs/maintenance/系统使用文档维护.md) |
| 底层身份与状态的已确认但未验证设计 | [路由观测证据 ADR](docs/adr/0001-routing-observation-evidence-boundaries.md) |
| 新数据存储方案与取舍（部分候选已实现，完整业务性能与恢复未通过） | [分层存储 ADR](docs/adr/0002-layered-data-storage.md) |
| 早期一日全链路迁移规划及历史验收；当前顺序见文首修订 | [一日全链路迁移计划](docs/architecture/新架构一日全链路迁移计划.md) |
| 独立 A 问答选型及其历史范围 | [Pi Runtime ADR](docs/adr/0001-pi-agent-runtime.md)、[Issue #1](https://github.com/xinghuahewo/domeye_new/issues/1) |
| 任务进度和剩余缺口 | [GitHub Issues](https://github.com/xinghuahewo/domeye_new/issues)，按[跟踪配置](docs/agents/issue-tracker.md)维护 |

两份历史 ADR 来自不同分支，保留原路径和编号以维持引用；以完整文件名区分，不将二者视为同一决策。GitHub 当前仅承载任务进度，源码主仓库仍为 SSH origin；不提供不存在的 GitHub 源码链接。台账里的 Git 外证据链接需要对应授权环境，克隆仓库不等于取得这些文件。
