# Domeye New

Domeye New 是从现有系统提取的路由观测工作台，包含 Vue 前端和 Flask 数据 API，使用已有数据库与数据制品。用户能力、可用条件和证据解释范围以 [CONTEXT.md](CONTEXT.md) 为准。

包元数据暂时保留 `domeye-core` 名称；这不表示运行时加载旧项目代码，也不继承旧项目的验收结论。

## 快速开始

需要 Linux、Python 3.10、uv、Node.js 20+ 和 npm。依赖及版本约束以 [后端清单](backend/pyproject.toml)、[前端清单](frontend/package.json) 和各自锁文件为准。

在项目目录执行：

```bash
cd /home/bgpdata/domeye-new
make setup
make test
make build
```

- `make setup` 在本项目安装依赖，生成 `backend/.venv` 和 `frontend/node_modules`。
- `make test` 运行隔离的后端测试与前端测试；后端日志写入 `.local/test-logs`。
- `make build` 执行前端类型检查并生成 `frontend/dist`，不启动或部署服务。
- `make api-types` 根据 OpenAPI 更新前端原始响应类型。

命令定义以 [Makefile](Makefile) 和 [前端脚本清单](frontend/package.json) 为准。测试或构建通过不等于真实数据和页面已完成验收。

## 启动与访问

现有服务器使用 systemd 管理前后端。服务名称、配置文件、SSH 隧道、启停和检查方法统一见 [运行与维护](docs/runbooks/运行与维护.md)。不要在常驻服务占用端口时再启动一份前台进程。

需要前台调试时，先按运行手册准备配置并停止对应常驻服务，再在两个终端分别执行：

```bash
make backend
```

```bash
make frontend
```

前台进程可用 Ctrl+C 停止。重新交回 systemd 管理的方法见运行手册。

## 文档导航与权威位置

| 要查找的知识 | 权威位置 |
|---|---|
| 项目介绍、环境要求、快速开始 | 本文 |
| 产品边界、领域术语、能力范围 | [CONTEXT.md](CONTEXT.md) |
| Codex 和其他 Agent 的仓库规则 | [AGENTS.md](AGENTS.md) |
| 当前模块关系、调用链、数据输入及合同关系 | [系统结构与数据流](docs/architecture/系统结构与数据流.md) |
| 服务管理、部署边界、恢复和排障步骤 | [运行与维护](docs/runbooks/运行与维护.md) |
| HTTP 接口和数据结构 | [OpenAPI](contracts/openapi.json)、[数据 Schema](contracts/data/)；趋势结构的兼容位置见架构文档 |
| 时间范围、快照时点、业务时区的配置值 | [数据档](config/data-profile.json) |
| 已确定架构决策及其理由 | [ADR-0001：采用 Pi Agent Runtime](docs/adr/0001-pi-agent-runtime.md)；后续决策按需记录到 `docs/adr/` |
| 已接受的待实现能力和验收条件 | [Spec Issue #1：国家中断历史复盘 A](https://github.com/xinghuahewo/domeye_new/issues/1)；本轮不另建重复的仓库 Spec |
| 尚未解决的问题 | GitHub Issue；目标仓库和操作方式见 [Issue 跟踪配置](docs/agents/issue-tracker.md) |
| 当前会话进度、临时运行状态和验证结果 | handoff 或当前会话交接记录 |

规格、未解决问题和会话进度按表中约定归位，具体记录按需建立。每个事实只在所属位置维护，其他文档使用链接引用；生成类型和测试样本不成为第二份接口权威。
