# Domeye 三工具问数

单用户问数应用，采用 Pi + DeepSeek、QMD 和 `docs`、`search`、`execute`。只查询 Domeye 本项目已生成并交付的结果，旧项目 55 天数据不在能力范围内。页面支持连续追问、切换数据批次、停止与只读历史。

在项目中独立运行，不依赖主前端构建，也不启动业务计算或数据发布。服务器部署、状态目录和回退见[问数服务运行手册](../../docs/runbooks/问数服务.md)。

## 本地启动

需要 Node.js 22.19 以上；服务器使用已有 Node.js 24.12.0。模型配置为权限 0600 的 JSON 文件，包含 `provider: "deepseek"`、`model`、`apiKey`，可选官方 `baseUrl` 和 `thinkingLevel: "low" | "high"`，默认 `high`；不要将配置提交到 Git。

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
| 2026 年 2 月 24 日完成结果 | `127.0.0.1:28683` | `iran-business-20260921d` 已交付的完成文件 |
| 三日任务已交付结果 | `127.0.0.1:28572` | `iran-three-days-20260922a` 已完成部分；不宣称三日完整 |

两批分别绑定自己的实际 OpenAPI，查询时发现覆盖和版本；不跨批次借用参数或数据。服务器直接读本机 API；本地使用时须将这两个端口通过 SSH 转发。`DOMEYE_QUERY_API_BASE_URL` 是宿主显式覆盖，CLI 对所选批次生效，页面仅对默认完成文件批次生效，改变前须核对接口合同。

左侧选择只影响下次新会话，顶部显示本会话实际批次。早期未标注批次的历史保持未标注。页面在模型生成正文时逐步展示，并标记尚未完成；正常结束且保存成功后才确认完整答案。工具调用前的中间文字在进入工具或下一轮模型时清除，不与最终正文拼接。失败、取消或断流时清除未完成预览。复杂问题仍可能持续数分钟；停止或关闭页面会取消当前模型、检索及在途客户端请求。模型余额不足、请求失败和未知数据分别保留。CLI 继续在正常结束并保存后输出完整答案。

## 正文流与耗时记录

独立问数 HTTP 合同见 [OpenAPI](openapi.json)，页面事件类型见 [chat-types.d.ts](web/chat-types.d.ts)。该入口不属于主 Flask 业务 API，不更改 `data/openapi*.json` 固定业务合同。

原始页面类型由该合同生成，复用项目已锁定的前端工具。更新合同时从仓库根目录执行 `node frontend/node_modules/openapi-typescript/bin/cli.js apps/query/openapi.json -o apps/query/web/openapi.generated.d.ts`；主 Flask 类型仍按 `make api-types` 生成。

`POST /api/chat` 使用 NDJSON：`start` 确认请求，`answer_start` 标识一次模型消息，`text` 按 `messageId` 和 `contentIndex` 传递公开正文，`answer_end` 区分正文已生成与中间预览应丢弃，`tool` 表示真实工具状态，`done` 才携带最终会话状态。`generated` 不表示已保存或答案正确。思考和工具参数不进入页面。跨分片脱敏仅暂存可能构成完整凭据的尾部前缀，普通短句立即转发。

历史中 `turn.timings` 使用本轮开始后的单调时钟毫秒数：每轮模型请求开始、首个模型增量、首个正文增量、首个脱敏后公开正文、消息结束，以及工具开始/结束、生成结束、保存开始。首个模型增量可能来自思考或工具参数，只记录时间，不保存思考；它不能替代正文首字。`final_message_id` 和 `final_first_text_ms` 只在本轮成功时指向最终回答，前置消息不冒充最终答案。

历史快照写入时无法知道该次写入何时返回，因此本轮快照中的 `save_finished_ms` 保持 null；实际写入尝试结束时间随 `done.timing.agent` 返回，是否成功由 `done.turn.status` 判断。后续轮次保存同一会话时，可以保留以前轮次已测得的保存结束时间。HTTP 层 `done.timing` 单独以请求进入为起点记录首次状态、首次正文、最终正文首字和结束时间，不与 Agent 起点混算。

浏览器用自身单调时钟计时，当前页面文章的 `data-latency` 保存 `firstStatusMs`、`firstTextMs`、`firstTextFrameMs`、`finalFirstTextMs`、`doneMs`。其中 frame 是下一绘制帧机会的近似值，不是硬件像素测量，后台标签页可能没有该值。浏览器计时仅在当前页面内存保留，不写入服务端历史；验收时单独采集。比较时保留模型配置、工具/模型轮次、缓存命中和失败记录，不能把状态提示算作首个有效正文，少量样本不用于宣称稳定分位数。

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

选定接口后，`search` 示例一次返回参数与完整小合同；超过既有输出预算的合同保留字段目录，提示按需选择原始子树。`search` 和 `execute` 回执列出当前已确认版本及适用路径，供模型显式填写；这只是参数提示，不代填版本，也不改变冲突、失败或完整性判断。所需结构已知时，在同次 `execute` 中取数并计算。

同一道问题内，`docs` 对来源标识和原文均完全相同的段落返回 `textReference`，指向此前工具调用中的原文；其他段落、不同来源以及下一道问题仍返回全文。原始检索结果照常保存在证据和工具详情中，只有模型上下文去除重复文本。

请求路径中的参数由调用方逐段编码；`query` 的键和值传原值，宿主使用 URLSearchParams 统一编码一次。提前对查询参数调用 encodeURIComponent 会把编码文本当原值再次编码，改变事件引用等参数的含义。

`domeye.request({method:"GET",path,query})` 保留原始 `{status,body}`，并在实际返回时保留可选 `headers`：`x-domeye-result-state`、`x-domeye-result-version`、`x-domeye-result-start`、`x-domeye-result-end-exclusive`、`x-domeye-result-coverage`。键使用小写，缺失不补齐；Cookie及其他响应头不进入沙箱或记录。这些交付头也独立保存在同次HTTP的范围回执中，模型聚合或省略正文时仍可核对。宿主只允许当前合同的 GET 路径，禁止更换主机、重定向、路径穿越、文件访问及外部模块。模型凭据不进入沙箱。单次代码限制 15 秒、64 MiB 虚拟机内存、16 次宿主调用（`domeye.request` 与 `domeye.readResult` 合计）、4 MiB 结果；被拦截的调用也占次数，每个问题最多 20 次工具调用。时间与调用次数由执行器的同一份默认额度写入模型可见的工具说明，未提高限额。

版本冲突后须发现并按新版本整题重取。Core 已明确声明完成文件来源时，可用 `/api/v1/healthz` 中同一 source_run、collector 和交付格式的 `result_delivery` 重新确认一次；健康响应本身仍无整体交付版本。未知绑定、独立留存或来源不符不能借此恢复，旧版本也不会被静默替换。该恢复只涉及只读请求策略，不触发数据生产或服务重启。

Core 的完整交付绑定已确认时，与完成文件的版本化查询入口共享已确认版本及一次恢复额度；后续跨入口读取也须显式带版本。Core 支持 `completed-file-results/v1` 与 v2；`country-outage-delivery/v1` 解析响应按 `event` 内的同一绑定读取。已声明版本参数的五种中断时序和单国 Feature 时序属于完成文件查询，分别按 `outage-series/v2`、`country-feature-series/v1`、v2 或 v3 的 `metadata.version` 核对；未知结构不递归搜寻同名字段。版本策略与来源回执共用解析入口，回执保留嵌套位置、单位、计量与时间含义、覆盖和已返回的事件生命周期。国家特征的资源时点及文件窗口按原始响应保留；v3 使用 `activity` 的起止区间和 `resources.at`，`source.label` 只用于文件选择与追溯，版本匹配不构成恢复或正常基线判断。

目录发现和健康发现恢复同一来源的版本状态，之前已读的查询仍须整题重取。首次识别共享绑定时若发现版本、source_run 或 collector 不同，保留冲突正文，不将两份读数同时确认为可用。仅从时序开始且尚未取得完整来源绑定时，不能凭同值版本从健康入口恢复；须先保留冲突并重新建立可核对的读取。响应头只保留服务端交付上下文；自动版本策略仍核对正文中合同指定位置的版本，不把通用头升级为所有正文结果的同版保证，也不以头部首末范围替代对象样本窗口。

工具发现和收窄参考 [Cloudflare search](https://github.com/cloudflare/mcp/blob/main/src/tools/search.ts) 与[截断实现](https://github.com/cloudflare/mcp/blob/main/src/truncate.ts)：模型可见结果限制为 24000 个 JavaScript 字符。JSON 排版超过额度时先尝试不改变内容的紧凑表示，能够完整容纳则保留全部字段、数值和时点；字符串原文不压缩。紧凑表示仍超限时明确标记 TRUNCATED，原始结果继续保存。`execute` 另返回 `resultReference.toolCallId`；后续代码用 `await domeye.readResult(id)` 读取该次完整返回，在本题内继续筛选或计算，不再为补齐截断重复 HTTP。其形状仍是原代码的返回值，不自动附加业务字段。引用只存在本题内存，进入新问题或关闭会话时清除；复制和派生结果保留原请求依赖，版本失效或读取故障不能通过复用绕过。`requestControl.reusedResults` 与历史中的 `result_reuse` 记录原工具与 HTTP 回执引用，不伪造新的 HTTP，也不证明任意派生字段的血缘、完整性或可比性。宿主注入和执行边界参考 [Cloudflare execute](https://github.com/cloudflare/mcp/blob/main/src/tools/execute.ts)，业务语义由 Domeye 决定。

Pi 0.87.0、DeepSeek `deepseek-v4-pro`、默认 high 推理、标准请求。Pro 已[正式支持 low](https://api-docs.deepseek.com/updates/)，但锁定 Pi 的模型目录会将 low 提升为 high；应用只在该模型的会话副本中修正映射，不修改依赖或全局目录。选择 low 仍启用思考，输出上限仍为 32768；这不是 Codex 的 Fast mode。会话记录实际选择的档位，效果和回答质量须单独比较，不能由配置生效推定。

会话记录保存问题、文档原文、工具代码、原始 HTTP、答案、数据来源与失败状态，不保存模型思考或密钥。`completed` 只表示正常结束并保存，不是答案正确性认证。没有自动评分、词表或答案检查门槛。

## 准备文档检索

QMD 2.8.3 固定使用 `aa31830e9044a987b331d4bd36859bac0d432a8c` 的 20 篇业务原文。项目 Git 的 `query-docs-20260923-coverage9` 标签保留这份来源，独立于应用发布提交；准备脚本用 `git show` 读取固定提交，不读取源码目录的未提交文档。该标签保全原分支来源，不表示其全部后端修改已合入 main。

docs 复用 QMD 的关键词和向量检索。原查询及按空白、标点分开的词组最多进行 8 次关键词检索；词法命中达到既有分数门槛后优先选入，再用向量候选补足，按文档去重，保留 3 篇主结果和每篇 80 行的原上限。命中末段时向前补足行预算，避免遗漏同篇定义。返回内容仍逐一核对固定文件散列、索引正文和来源行号；向量执行没有候选等底层错误仍明确失败。没有领域词表、标准答案或额外模型。

每个会话复用一个隔离的 QMD 进程，后台预热本地嵌入上下文，与首轮模型请求重叠；检索在进程内串行执行，不缓存答案或跳过原文校验。排队计入检索时限，取消排队项不影响当前查询；当前查询取消、超时、失败或进程崩溃时淘汰进程，后续调用重新冷启动，不自动重试失败请求。关闭或切换会话释放进程，原子替换索引后重新打开连接。QMD 自身默认闲置五分钟后卸载模型，之后可能再次冷启动；HTTP 健康响应不证明预热成功。

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
