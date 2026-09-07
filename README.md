# Domeye New

这是从现有系统中提取的传统 Vue 前端与 Flask 数据 API。它保留总览、事件列表与详情、国家和 ASN 档案、国家中断观测与确定性趋势展示，可继续只读使用 domeye-core 的数据库和已生成数据制品。

交付后服务保持停止。以下命令都由人手动执行；启动命令在前台运行，按 Ctrl+C 停止。

## 安装与检查

需要 Linux、Python 3.10、uv，以及 Node.js 20+ 和 npm。依赖清单和锁文件沿用当前前后端，包元数据中的 domeye-core 名称暂时保留，不会加载旧项目代码。

```bash
cd /home/bgpdata/domeye-new
make setup
make test
make build
```

make setup 在本项目创建 backend/.venv 与 frontend/node_modules；make test 使用隔离配置和测试样本，在 .local/test-logs 生成日志；make build 在 frontend/dist 生成静态页面。这些目录不进入 Git。测试不加载外部 backend.env，不访问真实数据。

前端脚本优先使用当前可用的 Node.js；版本不足时尝试服务器已有的 Node 22。也可以用 DOMEYE_NODE_BIN 指定工具目录。

## 手动启动

后端配置独立保存在 `/home/bgpdata/domeye-new-runtime/backend.env`，权限必须为 0600。启动器只解析允许的 KEY=value，不执行配置内容；不加载旧项目或本项目的 .env。更换环境时可用 DOMEYE_RUNTIME_ENV 指定另一个项目外配置文件。

在两个终端分别运行：

```bash
cd /home/bgpdata/domeye-new
make backend
```

```bash
cd /home/bgpdata/domeye-new
make frontend
```

默认前端监听 127.0.0.1:28471，后端监听 127.0.0.1:28473。前端通过自己的开发服务器代理 `/api/v1` 和 `/api/v2`，可用 DOMEYE_WEB_PORT 与 DOMEYE_API_TARGET 显式覆盖。

从 Windows 访问时，在一个终端建立隧道，再打开浏览器：

```bash
ssh -N -L 28471:127.0.0.1:28471 root@10.99.8.16
```

浏览器打开 [Domeye 本地页面](http://127.0.0.1:28471)。这里没有安装常驻服务、自动重启或 Nginx 站点；make build 也不会启动服务。

## 数据与功能范围

运行入口从 config/data-profile.json 读取时间范围、快照时点和业务时区，并向后端注入一致的窗口。数据库连接强制采用只读事务，Web 不执行初始化、全量数据加载或离线检测。

外部配置绑定数据库、INFO 静态信息和国家中断数据制品；这些文件可以继续留在获准复用的数据目录。源码不复制这些数据，也不引用旧项目 Python 环境、前端依赖目录或运行进程。

保留了当前传统 API 与页面能力，但数据完整性仍取决于实际绑定的数据库及制品。某个制品没有配置或校验失败时，相应功能应显示不可用，不能伪造空数据或成功。P0 指标功能需要单独有效的数据发布目录；本次没有为它重建或发布数据。未完成的 metric-series 实验未纳入这个项目，旧实验与数据仍留在原位置。

原有 Agent 页面、问答 API、代理、Sidecar、模型调用、候选与评测、旧治理计划均未迁入。contracts/agent 下保留的四份 Schema 是当前确定性趋势数据的结构定义，保留路径用于兼容现有合同，不代表包含 Agent 运行能力。后端 data_pipeline 只保留 P0 读取所需的指标定义与质量语义校验。

国家中断数据描述绑定观察点、事件、版本和时间窗内的 BGP 控制面观测，不直接代表全国实际断网或真实用户影响。页面中的“当前”应按固定数据快照理解。

## 开发入口

- frontend：现有 Vue 页面、API 客户端与前端测试。
- backend：只读 Flask API、查询服务与 API 层测试。
- contracts：OpenAPI、数据 Schema 与测试样本。
- config/data-profile.json：唯一数据范围与时区配置。
- scripts：手动前台启动入口。

修改接口后运行 make api-types 更新前端类型，再运行 make test 与 make build。新项目使用独立本地 Git；源码来源和新项目交付状态由实际 Git 记录及运行验证说明，不沿用旧项目的验收结论。
