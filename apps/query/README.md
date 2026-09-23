# Domeye 三工具问数

单用户问数应用，采用 Pi + DeepSeek、QMD 和 `docs`、`search`、`execute`。只查询 Domeye 本项目已生成并交付的结果，旧项目 55 天数据不在能力范围内。页面支持连续追问、切换数据批次、停止与只读历史。

在项目中独立运行，不依赖主前端构建，也不启动业务计算或数据发布。服务器部署、状态目录和回退见[问数服务运行手册](../../docs/runbooks/问数服务.md)。

## 本地启动

需要 Node.js 22.19 以上；服务器使用已有 Node.js 24.12.0。模型配置为权限 0600 的 JSON 文件，包含 `provider: "deepseek"`、`model`、`apiKey`，可选官方 `baseUrl`；不要将配置提交到 Git。

```bash
cd apps/query
npm ci
export DOMEYE_MODEL_CONFIG=/绝对路径/model-config.json
export DOMEYE_QUERY_STATE_DIR=/绝对路径/query-state
# 按下文准备 QMD 后启动
npm run web
```

默认页面为 `http://127.0.0.1:28684/`。`DOMEYE_CHAT_HOST` 改监听地址，非本机监听必须同时明确 `DOMEYE_CHAT_ORIGIN`，例如 `http://10.99.8.16:28684`。Host 和浏览器 Origin 均按此来源严格核对，不信任转发头。Origin 校验不等于用户认证；当前服务沿用项目内网访问范围，单进程只支持一个活动会话。

`DOMEYE_QUERY_STATE_DIR` 默认为仓库 `.local/query`，保存 QMD 依赖、语料、索引、缓存和历史；可用 `DOMEYE_HISTORY_DIR` 单独指定历史目录。切换发布代码不覆盖状态目录。`GET /api/healthz` 仅表示 HTTP 进程存活；QMD、模型和业务数据可用性须分别实际核对。

## 数据与使用

| 新会话选择 | 服务器只读入口 | 来源 |
| --- | --- | --- |
| 2 月 24 日完成结果 | `127.0.0.1:28683` | `iran-business-20260921d` 已交付的完成文件 |
| 三日任务已交付结果 | `127.0.0.1:28572` | `iran-three-days-20260922a` 已完成部分；不宣称三日完整 |

两批分别绑定自己的实际 OpenAPI，查询时发现覆盖和版本；不跨批次借用参数或数据。服务器直接读本机 API；本地使用时须将这两个端口通过 SSH 转发。`DOMEYE_QUERY_API_BASE_URL` 是宿主显式覆盖，CLI 对所选批次生效，页面仅对默认完成文件批次生效，改变前须核对接口合同。

左侧选择只影响下次新会话，顶部显示本会话实际批次。早期未标注批次的历史保持未标注。答案正常结束并保存后显示，复杂问题可能持续数分钟；停止或关闭页面会取消当前模型、检索及在途客户端请求。模型余额不足、请求失败和未知数据分别保留。

```bash
node cli.mjs --dataset completed-files --prompt '现在手里的数据覆盖哪段时间？'
node cli.mjs --dataset three-day --prompt '这批数据完整了吗？'
```

`--questions 文件.json` 可读取问题字符串数组，失败或取消后停止后续问题。交互模式 `/quit` 退出，Ctrl-C 停止。

## 三个工具与边界

| 输入 | 职责 |
| --- | --- |
| `docs({query})` | QMD 检索原文，保留标题、固定提交和行号；无命中不等于业务不支持 |
| `search({code})` | JavaScript 查询已展开引用的 OpenAPI，逐步选出完整接口路径及必要结构；不读业务数据 |
| `execute({code})` | 独立 QuickJS-WASM 中经 `domeye.request()` 读取和计算，返回代码选择的结果 |

`domeye.request({method:"GET",path,query})` 保留原始 `{status,body}`。宿主只允许当前合同的 GET 路径，禁止更换主机、重定向、路径穿越、文件访问及外部模块。模型凭据不进入沙箱。单次代码限制 15 秒、64 MiB 虚拟机内存、16 次请求、4 MiB 结果；每个问题最多 20 次工具调用。

工具发现和收窄遵循 [Cloudflare search](https://github.com/cloudflare/mcp/blob/main/src/tools/search.ts) 与[截断实现](https://github.com/cloudflare/mcp/blob/main/src/truncate.ts)：模型可见结果超过 24000 个 JavaScript 字符时附 TRUNCATED 并提示收窄；原始结果继续保存。宿主注入和执行边界先对照 [Cloudflare execute](https://github.com/cloudflare/mcp/blob/main/src/tools/execute.ts)，业务语义由 Domeye 决定。

Pi 0.87.0、DeepSeek `deepseek-v4-pro`、high 推理、标准请求。会话记录保存问题、文档原文、工具代码、原始 HTTP、答案、数据来源与失败状态，不保存模型思考或密钥。`completed` 只表示正常结束并保存，不是答案正确性认证。没有自动评分、词表或答案检查门槛。

## 准备文档检索

QMD 2.8.3 固定使用 `1e370683cf1233a433c56e498d1f58d34e92484a` 的 20 篇业务原文。项目 Git 的 `query-docs-20260923` 标签保留这份来源，独立于应用发布提交；准备脚本用 `git show` 读取固定提交，不读取源码目录的未提交文档。该标签保全原分支来源，不表示其全部后端修改已合入 main。

```bash
# 以下命令从 apps/query 执行，DOMEYE_QUERY_STATE_DIR 已设置
mkdir -p "$DOMEYE_QUERY_STATE_DIR/qmd-runtime"
cp qmd/package.json qmd/package-lock.json "$DOMEYE_QUERY_STATE_DIR/qmd-runtime/"
npm ci --prefix "$DOMEYE_QUERY_STATE_DIR/qmd-runtime"
node scripts/prepare-docs.mjs snapshot ../..
node scripts/prepare-docs.mjs update
# 预先准备嵌入模型后执行；问答时不会自动下载或重建
node scripts/prepare-docs.mjs embed
node scripts/prepare-docs.mjs status
```

模型为 `hf:Qwen/Qwen3-Embedding-0.6B-GGUF/Qwen3-Embedding-0.6B-Q8_0.gguf`，缓存文件位于状态目录 `qmd-runtime/cache/qmd/models/hf_Qwen_Qwen3-Embedding-0.6B-Q8_0.gguf`，SHA-256 为 `06507c7b42688469c4e7298b0a1e16deff06caf291cf0a5b278c308249c3e439`。QMD 按其官方 SDK 准备本地模型；当前部署复用已核对的模型文件，在 Linux 安装依赖并重建索引，不复制 macOS 原生依赖。

完成文件 API 源码为 `93f8d581457ab06457e78af551954385bd7590bc`。三日 API 含独立运行目录修改，提交保持 null，使用[逐文件源码清单](data/three-day-source.json)和实际 OpenAPI 哈希记录来源。固定说明与实际业务交付状态分别核对。

## 验证

```bash
npm run check
npm run test:contracts
npm run test:executor
npm run test:session
npm run test:integration
npm run test:search
npm run test:web
npm run test:loop
```

程序测试使用固定消息或临时数据验证执行隔离、错误、版本、会话、停止、历史和同源边界。真实问答仍逐项核对理解、说明与接口、数值及时间单位依据、回答自然程度与限制，程序测试不替代问数验收。
