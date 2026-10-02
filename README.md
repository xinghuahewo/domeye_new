# Domeye New

**面向 BGP 路由观测、异常事件复盘与证据查询的工作台。**

Domeye New 将已交付的路由数据转成可筛选的态势、事件详情、时间趋势和 AS 档案，并提供独立的自然语言问数应用。它帮助使用者回答“某段时间观察到了什么异常”“某个 AS 的路由活动和资源如何变化”“这个结论来自哪份数据”。

本仓库包含 Vue 前端、Flask 只读 API、离线数据处理工具和问数应用。**克隆源码不包含业务数据库、原始 MRT、运行配置或模型密钥**；要查看真实结果，仍需配置获准使用的数据源。

## 可以做什么

| 功能 | 使用方式与范围 |
| --- | --- |
| 核心态势 | 按国家、日期或跨日时间范围筛选，查看异常数量、事件列表和特征趋势；国家入口已整合到首页 |
| 异常事件 | 检索前缀劫持、子前缀劫持、前缀中断、AS 中断、国家中断和路由泄漏，查看已有事实和证据 |
| 国家中断复盘 | 查看事件状态、检测与最后观测时间、峰值 AS 名单，以及同期活动、资源和中断统计 |
| AS 查询与档案 | 查询单个 ASN 在指定窗口的路由活动、资源和关联事件；单 ASN 档案最多支持 45 天，实际可用范围由数据决定 |
| 快照与比较 | 读取已登记 RIB 的规模、起源 ASN 资源、可用路径对照和国家两窗比较 |
| 自然语言问数 | 选择数据批次和已配置模型，连续提问、停止生成、查阅历史；通过文档、API 合同和只读执行工具取得依据 |
| 离线处理 | 提供观测解析、状态与特征计算、异常分析、结果交付及核验工具；需独立配置和显式执行 |

具体操作见[系统使用文档](docs/usage/README.md)，指标解释见[指标目录](docs/usage/metrics/README.md)。独立问数的配置、限制和验证方式见 [apps/query/README.md](apps/query/README.md)。

## 工作方式

```text
原始 BGP / MRT 与参考数据
          ↓ 显式离线计算、核验和交付
已绑定的数据库 / 结果制品
          ↓ 只读 Flask API
     ┌────┴──────────────┐
Vue 路由观测工作台    独立自然语言问数
                     文档检索 → API 发现 → 读取与计算 → 回答
```

页面请求不会触发采集、检测、数据库初始化或结果发布。问数使用 `docs`、`search`、`execute` 三个工具，按会话绑定数据批次并核对读取版本；模型正常完成回答不等于结论已经通过业务验收。

## 数据与能力边界

- BGP 控制面观测限定于具体 collector、对象、版本和时间窗，不能直接推断全国实际断网、真实用户影响、根因或责任。
- 查询窗口、处理覆盖和事件生命周期是不同概念。缺失、Unknown、未配置、校验失败与“确实观测到零”分别表达。
- 资源量、前缀数、/24 与 /48 块数等指标保留各自单位；同一时间窗不自动表示同一来源或可比口径。
- 已有部分真实结果接入和历史验收记录，不代表全部日期、所有检测器、完整生产性能或任意问答均已验收。具体证据见[数据制品台账](docs/data-assets-and-admission.md)。
- 当前服务以开发／内网使用为主；本仓库不是开箱即用的公网多用户部署。问数单进程仅支持一个活动会话，来源校验不等于用户认证。

## 快速开始

### 1. 获取源码与安装依赖

主工作台需要 Python 3.10、uv、Node.js 20+、npm 和 make；独立问数需要 Node.js 22.19+。离线生产及现有服务部署以 Linux 环境为主，具体依赖见各模块清单和锁文件。

```bash
git clone https://github.com/xinghuahewo/domeye_new.git
cd domeye_new
make setup
```

`make setup` 安装后端和主前端依赖，不安装独立问数。Python 使用 `backend/pyproject.toml` 与 `uv.lock`，Node 模块使用各自的 `package-lock.json`。

### 2. 检查与构建

```bash
make test
make build
```

测试使用 fixture、mock 和临时目录，不要求真实业务数据。`make build` 进行前端类型检查并构建静态资源，不启动或部署服务。修改 API 合同时运行 `make api-types` 更新前端原始响应类型。

### 3. 配置数据并启动工作台

先按[运行与维护](docs/runbooks/运行与维护.md)准备项目外的后端配置文件，权限设为 `0600`，绑定获准使用的数据库和结果制品。数据窗口与业务时区以 [config/data-profile.json](config/data-profile.json) 为准。

分别在两个终端运行：

```bash
# 终端一：使用项目外的实际配置路径
DOMEYE_RUNTIME_ENV=/绝对路径/backend.env make backend
```

```bash
# 终端二
make frontend
```

默认本机前端为 `http://127.0.0.1:28471`，后端为 `http://127.0.0.1:28473`。不要与使用相同端口的常驻服务重复启动；无有效数据绑定时，启动成功也不代表能查看业务数据。现有服务器的访问、端口调整和服务恢复统一见运行手册。

### 4. 可选：启动自然语言问数

```bash
cd apps/query
npm ci
```

然后按[问数应用说明](apps/query/README.md)配置项目外的模型凭据、状态目录、只读 API 和 QMD 文档检索，再运行 `npm run web`；默认入口为 `http://127.0.0.1:28684`。应用保留固定的 API／文档来源提交，准备检索资料时需要完整 Git 历史，不建议浅克隆。

## 源码导航

| 路径 | 内容 |
| --- | --- |
| `frontend/` | Vue 3、TypeScript、Vite、ECharts 页面与测试 |
| `backend/web/`、`backend/services/` | Flask 路由、只读业务查询 |
| `backend/data_pipeline/` | 离线观测、计算、存储、结果与质量核验；见[目录说明](backend/data_pipeline/README.md) |
| `backend/tests/` | 后端单元、合同和隔离集成测试 |
| `apps/query/` | Pi 模型运行时、QMD 检索、QuickJS 执行隔离与问数界面 |
| `contracts/`、`config/` | OpenAPI、数据合同、公共数据时间档 |
| `scripts/`、`deploy/` | 显式命令入口与已有服务配置模板 |
| `docs/` | 使用说明、架构、运行手册、验收与历史记录 |

## 此次源码同步

本次 GitHub 版本以 `3cbb3b6`（2026-10-02 的 Core／AS 档案版本）为主，纳入 `abba9b5`（2026-09-30）的独立 `apps/query/` 和问数运行手册，并保留两条来源历史。没有将其他试验分支或未提交工作混入发布。源码同步不改变现有服务器服务、数据绑定或业务验收状态。

## 文档导航与权威位置

| 要查找的内容 | 权威位置 |
| --- | --- |
| 系统使用、指标定义、业务查询与结果解释 | [系统使用文档](docs/usage/README.md) |
| 当前实现、入口、数据流与旧能力边界 | [系统结构与数据流](docs/architecture/系统结构与数据流.md) |
| 数据流水线加速改动、历次实测与当前困境 | [数据流水线加速改动与当前困境](docs/architecture/数据流水线加速改动与当前困境.md) |
| 独立自然语言问数的安装、工具与模型配置 | [问数应用](apps/query/README.md)、[问数运行手册](docs/runbooks/问数服务.md) |
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

两份历史 ADR 来自不同分支，保留原路径和编号以维持引用；以完整文件名区分，不将二者视为同一决策。GitHub 同时提供源码与任务跟踪。台账里的 Git 外证据链接需要对应授权环境，克隆仓库不等于取得这些文件。
