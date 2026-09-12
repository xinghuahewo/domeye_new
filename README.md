# Domeye New

Domeye 的路由观测工作台：Vue 前端、Flask 只读 API，以及显式离线的数据留存与核验工具。首页 C 面向整体控制面态势，不围绕单个 ASN 展开；单观察点的数据不能当作全网或实际用户影响。

## 当前交付

2026-09-12 已接受“55天可用、4天明确隔离”的本轮边界：六类留存异常、前缀与明确归属起源 AS 的单 RIB 规模，以及两次路径观察对照已交付。四个失败日未修复，覆盖仍未知；精确版本、数量和验收证据以[数据台账](docs/data-assets-and-admission.md#51-用户确认交付边界与本轮收口)为准。

本次正在将已验收的本地版本合并到服务器主线并部署。服务器实时版本、入口、回滚与部署结果统一记录于[运行与维护](docs/runbooks/运行与维护.md)。原本机28492预览及独立 A 问答服务不因此合并为同一版本。

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

## 文档导航与权威位置

| 要查找的内容 | 权威位置 |
| --- | --- |
| 当前实现、入口、数据流与旧能力边界 | [系统结构与数据流](docs/architecture/系统结构与数据流.md) |
| 领域术语 | [CONTEXT.md](CONTEXT.md) |
| 协作规则 | [AGENTS.md](AGENTS.md) |
| C 页面使命、口径、有限交付范围 | [C 短规格](docs/core-overview-v1.md) |
| 数据来源、准入、验收证据与历史 | [数据制品台账](docs/data-assets-and-admission.md) |
| 常驻服务、部署、回滚与恢复 | [运行与维护](docs/runbooks/运行与维护.md) |
| 本机预览及既有独立恢复命令 | [C 本地验收与恢复历史](docs/runbooks/C本地验收与恢复.md) |
| HTTP 合同与数据时间配置 | [OpenAPI](contracts/openapi.json)、[数据档](config/data-profile.json) |
| 底层身份与状态的已确认但未验证设计 | [路由观测证据 ADR](docs/adr/0001-routing-observation-evidence-boundaries.md) |
| 独立 A 问答选型及其历史范围 | [Pi Runtime ADR](docs/adr/0001-pi-agent-runtime.md)、[Issue #1](https://github.com/xinghuahewo/domeye_new/issues/1) |
| 任务进度和剩余缺口 | [GitHub Issues](https://github.com/xinghuahewo/domeye_new/issues)，按[跟踪配置](docs/agents/issue-tracker.md)维护 |

两份历史 ADR 来自不同分支，保留原路径和编号以维持引用；以完整文件名区分，不将二者视为同一决策。GitHub 当前仅承载任务进度，源码主仓库仍为 SSH origin；不提供不存在的 GitHub 源码链接。台账里的 Git 外证据链接需要对应授权环境，克隆仓库不等于取得这些文件。
