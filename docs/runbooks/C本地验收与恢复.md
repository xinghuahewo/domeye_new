本页保存2026-09-12服务器部署前的本机验收与历史命令，原数据及证明不改写。服务器现行入口以[运行与维护](运行与维护.md)为准；这里的28492、Mac路径及“未部署”措辞只描述当时本地状态。

# C 首页本地验收与恢复历史

这是从现有系统中提取的传统 Vue 前端与 Flask 数据 API。它保留总览、事件列表与详情、国家和 ASN 档案、国家中断观测与确定性趋势展示，可继续只读使用 domeye-core 的数据库和已生成数据制品。

以下说明手动前台运行方式，命令由人手动执行，按 Ctrl+C 停止；不能据此判断服务器当前服务是否运行。已核对的实例状态、数据绑定及核对时间见[数据台账](../data-assets-and-admission.md#51-现场绑定快照)。

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

浏览器打开 [Domeye 本地页面](http://127.0.0.1:28471)。上述手动命令和 make build 不负责安装常驻服务；已核对的服务器另有 systemd 服务，运行前应先核对已有进程，避免重复启动。本文不提供修改共享服务的授权。

## 数据与功能范围

受控运行入口从 config/data-profile.json 读取时间范围、快照时点和业务时区，并向后端注入窗口及 `PGOPTIONS=-c default_transaction_read_only=on`。该默认只读事务设置依赖受控启动器，不是连接函数或数据库角色层的绝对禁写保证。Web 不执行初始化、全量数据加载或离线检测；各查询的实际时间边界仍需按用途核对。

外部配置绑定数据库、INFO 静态信息和国家中断数据制品；这些文件可以继续留在获准复用的数据目录。源码不复制这些数据，也不引用旧项目 Python 环境、前端依赖目录或运行进程。

保留了当前传统 API 与页面能力，但数据完整性仍取决于实际绑定的数据库及制品。某个制品没有配置或校验失败时，相应功能应显示不可用，不能伪造空数据或成功。P0 指标功能需要单独有效的数据发布目录；本次没有为它重建或发布数据。未完成的 metric-series 实验未纳入这个项目，旧实验与数据仍留在原位置。

原有 Agent 页面、问答 API、代理、Sidecar、模型调用、候选与评测、旧治理计划均未迁入。contracts/agent 下保留的四份 Schema 是当前确定性趋势数据的结构定义，保留路径用于兼容现有合同，不代表包含 Agent 运行能力。后端 data_pipeline 只保留 P0 读取所需的指标定义与质量语义校验。

国家中断数据描述绑定观察点、事件、版本和时间窗内的 BGP 控制面观测，不直接代表全国实际断网或真实用户影响。页面中的“当前”应按固定数据快照理解。

数据来源、已记录的核对范围与使用限制见[数据制品台账与用途准入说明](../data-assets-and-admission.md)。台账区分带日期的现场核对、历史实物证据、代码／合同线索与未验证设计；来源和文件身份确认不代表业务内容已通过验收。

已确认的首页 C 设计及真实数据切片的验收边界见[核心态势页第一版短规格](../core-overview-v1.md)。本地首页使用 C 结构，通过独立只读接口消费明确绑定的留存异常；原模拟原型保留，旧 P0 页移至 `/legacy-overview`。当前已接入下文列出的六类型留存异常；不表示全部配置日期或总体路由指标已可用。

### 本地 C 真实记录展示

交付状态（2026-09-12）：用户已接受“55天可用、4天明确隔离”的本轮边界，相关首页数据与本地消费恢复交付完成；四天源数据问题仍未修复，详见[确认记录](../data-assets-and-admission.md#51-用户确认交付边界与本轮收口)。

当前选择（2026-09-12）为 `37e0ea72…`：55个可用日、4个失败日、1,120,555条六类型异常；10条集合身份原文以“对象待核实”展示，03-10另一条随独立国家记录问题隔离。默认日规模为1,403,383前缀／85,565明确归属起源AS；路径区为北京时间03-31 00:00与16:00两次观察对照，不是全天变化统计。以下显式启动命令已更新到这一版本；历史构建示例不是当前选择。三项验收见[台账第48节](../data-assets-and-admission.md#48-三项治理结果进入同版首页)，当前恢复说明见[第49节](../data-assets-and-admission.md#49-当前37e版本的独立恢复与空国家原文复核)。

输入必须在 Git 外显式绑定。已有审计目录可用时，用下列离线命令创建新的消费目录；目录存在会拒绝覆盖，不会连接数据库或重跑检测：

```bash
backend/.venv/bin/python scripts/core_overview/retain-core-overview-input.py \
  --audit-dir .local/core-overview-validation/20260910T120154Z \
  --output .local/core-overview-inputs/rrc25-20260227-v1
```

本轮已经创建该目录，不必重复执行。它绑定原记录摘要、来源确认及解释版本；不是历史检测版本或 P0 Publication。换用其他输入需新目录，不能修改被引用的旧包。文件摘要用于完整性核对，不是签名或不可变存储保证。该本地目录不随 Git 分发；不存在时首页应显示不可用。

多日期消费使用按日索引，避免每个请求解析整份记录。以下保留旧五日重建示例，输出已存在，不必重跑。当前选择 `.local/core-overview-validation/three-points-20260912-T53su1/combined-index-v2`（55可用日＋4失败日，六类型，另绑定规模和两次路径对照）；原49日及更早目录保留。新增日期须先完成源关联、原字段及小时桶核验，再选择新输出目录，不覆盖旧版本：

```bash
backend/.venv/bin/python scripts/core_overview/index-core-overview-inputs.py \
  --input .local/core-overview-inputs/rrc25-20260224-v1/manifest.json \
  --input .local/core-overview-inputs/rrc25-20260225-v1/manifest.json \
  --input .local/core-overview-inputs/rrc25-20260226-v1/manifest.json \
  --input .local/core-overview-inputs/rrc25-20260227-v1/manifest.json \
  --input .local/core-overview-inputs/rrc25-20260228-v1/manifest.json \
  --output .local/core-overview-inputs/rrc25-feb24-28-v1
```

每个业务日独立 SQLite 文件，目录清单绑定各日摘要、原单日版本及消费解释；窗口首尾不代表连续覆盖。Web 仅打开已生成的闭合只读索引，拒绝 WAL／日志旁路输入；首读校验摘要，同一文件实体后续复用摘要检查，文件变化或缺失返回不可用。文件权限及摘要不是防恶意篡改的不可变存储承诺。旧单日 JSONL 包仍可显式绑定复读。

较大日包仅由上述显式离线命令流式读取，单包上限2GiB／100万记录，全部校验完成才生成目录；Web直接读原JSONL仍限64MiB，不在请求中构建。2026-09-11新增大日的历史验收见[台账第37节](../data-assets-and-admission.md#37-新增三大日接入首页与03-28失败诊断更新)。当时03-28因ASN身份失败，现已按获准的新解释完成整日重验，不能用历史失败状态覆盖当前55／4目录。

原三类型目录使用 `rrc25-20260201-v3` 至 `rrc25-20260223-v3` 的23份成功空日包、上方原五份非空包，以及 `rrc25-20260301-v1`、`rrc25-20260302-v1`、`rrc25-20260304-v1`、`rrc25-20260305-v1`；全部位于 `.local/core-overview-inputs/`。成功空日必须有完整只读回执、独立三类零计数和一致审计，不能只创建空文件。它只说明所选三类源记录查询为空，不说明没有真实异常或观察连续。二月与三月四日证据分别见[台账第16节](../data-assets-and-admission.md#16成功空日准入二月28天可以查询)和[第17节](../data-assets-and-admission.md#17三月四日准入32日真实查询目录)。旧目录和中间候选版本保留，不覆盖更新。

前序源字段预检可通过离线索引命令的可重复 `--diagnostic` 参数选择：历史两份选择清单位于 `.local/core-overview-validation/failed-dates-implementation-v1/evidence/{as-quality,prefix-quality}/selection.json`，分别绑定原预检结果、SQL及回执。只接受已核验格式，原因与条数从原结果派生，SQL模板不被执行。`core-overview-index/v2` 的 `diagnostics` 与消费 `days` 不能重叠；没有失败原因的预检不能代替完整准入。Web只按需复读有界单日诊断，不提供消费指标或事件详情。历史选择见[台账第18节](../data-assets-and-admission.md#18失败日期诊断及原始时间准入)；本轮v2完整日／读取失败由下方专用离线脚本编译，不是该旧格式CLI的新增参数。

只查看已留存输入可独立启动本地后端，无须绑定旧数据库或 INFO：

```bash
domeye_root="$PWD"
cd backend
env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 \
  FLASK_SKIP_DOTENV=1 FLASK_DEBUG=false FLASK_CONFIG=testing PYTHONDONTWRITEBYTECODE=1 DOMEYE_CORE_SKIP_LOCAL_ENV=true \
  DOMEYE_LOG_DIR="$domeye_root/.local/core-overview-runtime/logs" \
  DOMEYE_CORE_OVERVIEW_MANIFEST="$domeye_root/.local/core-overview-validation/three-points-20260912-T53su1/combined-index-v2/manifest.json" \
  .venv/bin/python -m flask --app web.flask_app:create_flask_app run \
  --host 127.0.0.1 --port 28491 --no-reload
```

另一个终端在项目根目录运行：

```bash
DOMEYE_WEB_PORT=28492 DOMEYE_API_TARGET=http://127.0.0.1:28491 bash scripts/frontend.sh dev
```

打开[默认 C 首页](http://127.0.0.1:28492/)，默认03-31有14,410条六类型异常；14条AS等级冲突标为“等级待核实”，可单独筛选并查看两份原值。**现共55日、1,120,555条**，原49日863,708条字节保留，新增六日256,847条。默认日包括12,302条前缀中断、290条AS中断、736条泄漏、562条前缀劫持、446条子前缀劫持、74条国家中断。原聚合不代表实际影响或可见规模；不同编号不合并，未记录结束不称持续中。

前阶段四类型为36,544条，计算为原三类38,112−03-04的1,584＋普通hijack16；随后追加11条子前缀劫持，本轮再追加3条国家中断。03-04的一条hijack总表／明细结束与时长冲突，继续按既有规则整日失败；该日原三类1,584条仍保留在旧32日版本，并非删除。原五类记录及列表条目字节、31日的前缀小时趋势不变。各批读取不是同一数据库快照。

日期目录另列4个不可消费日：03-03的12条负时间记录；03-04、20的劫持关联／时间矛盾；03-10的空国家事实与总表集合差。03-10缺失总表原文已在原备份找回，但国家代码和名称仍空，未混入当前消费版。03-07、09、11、14、26、28已完成六类全日重验，其中10条集合身份原文不拆分为单ASN。集合差不能解释为全库孤立记录，原因数不能求和成影响规模。失败日的统计、列表及详情均阻断，不提供部分成功。筛选和详情携带同一目录版本，切换后需重新读取；未知不补零。

当前输入为 `.local/core-overview-validation/three-points-20260912-T53su1/combined-index-v2/manifest.json`，版本 `overview_index_v2_37e0ea72f8f1a18a4cf57d8d6be576983db95e5a04be87b78d5f349ac8e9c37d`。它保留原49日异常及规模字节，新增六日和路径绑定；旧`410c…`及更早输入全部保留。旧49日历史验收见台账第37节，起源见第43节，当前三项接入见[第48节](../data-assets-and-admission.md#48-三项治理结果进入同版首页)。

首页现显示03-31 **16:00北京时间单RIB**的可见前缀数：IPv4为1,133,653、IPv6为269,730、全部为1,403,383。可见起源AS数按已确认的末端跳过私用AS归属规则：IPv4为78,236、IPv6为36,483、全部并集85,565；不拆集合／联盟段或越过歧义猜测，8,429条未明确归属的路由条目另存，不是缺测ASN数。只适用于该业务日及相应地址族；其他日期或未知地址族为null，不把16:00延续为整日或日末。小时／类型／等级／搜索只筛选异常列表，不重算快照。来源、实际时点、规模及消费版本可点击卡片查看；观察覆盖仍未知。

规模包原件`.local/core-overview-validation/rib-scale-consumption-v1/package-v1/`保留，不能单独替换异常manifest。显式离线入口为`backend/.venv/bin/python scripts/core_overview/bind-core-overview-scale.py --help`：指定`--index`旧日期目录、`--scale`规模清单和独立新`--output`，复制并校验所有绑定字节，最后生成新清单，不覆盖旧目录。Web只读规模清单与摘要（各≤64KiB），不读取MRT、不重跑检测或完整审计；规模摘要损坏只使规模不可用，异常失败日仍阻断全部统计。完整核验证据见[台账第38节](../data-assets-and-admission.md#38-单rib前缀规模消费包准备)，接入边界见[第39节](../data-assets-and-admission.md#39-单rib前缀规模接入首页)。

起源入口为 `backend/.venv/bin/python scripts/rib/retain-rib-origin-input.py --help`：显式指定单RIB、源SHA及新目录，完整复读后保存原路径目录、原始末端、归属结果及集合；只供离线运行。用 `backend/.venv/bin/python scripts/core_overview/bind-core-overview-origin.py --help` 选择带前缀的旧索引、起源manifest和独立新输出，校验同源同一时点、成员与分母后另建消费版本。Web只读取新增的两个64KiB以内摘要，不读取路径SQLite／ASN集合或生产数据；起源损坏不影响原前缀与异常。

路径区展示两次RIB观察中共同可比Peer／Prefix项：路径不同1,502,441／可比56,451,868；另列单端和不可比项，最多10个非代表性样本。观察覆盖、Session连续性未知，不把16小时内的变化次数或单端缺失推成撤回。离线绑定入口为 `scripts/core_overview/bind-core-overview-paths.py`，实际完整证据及规则见台账第47—48节；Web只读有界摘要。可见性／起源变化未接入，不在本片扩建状态重构。

旧五类型 `.local/core-overview-inputs/rrc25-sub-hijack-v2/`、四类型 `rrc25-hijack-v3/` 及旧三类型目录原样保留，可显式切回；对应证据见台账第20、22节。剩余4日仍未通过准入。上方无数据库模式仅提供留存首页；本机共用预览见下节。

此前六类型恢复副本 `.local/core-overview-inputs/rrc25-country-outage-restore-v1/index/manifest.json` 仍保留，60份文件摘要与66次公开路由复读通过；这是旧 `cc17…` 版本，**不是本次默认日准入版本**。可在确认端口后显式选择旧包回退，不覆盖原件；历史临时服务已关闭。它仍在同磁盘、使用现有依赖，不是完整灾备，见[台账第25节](../data-assets-and-admission.md#25-六类型独立副本恢复复读)。

### 当前本机共用预览：留存首页与旧事件列表

2026-09-11已修复旧读取的请求连接隔离／回收及失败伪装成功空结果的问题，历史验收见[台账第28节](../data-assets-and-admission.md#28-旧事件读取修复与本地共用预览)。本次共用预览选择上述55日`37e…`版本；[旧事件列表](http://127.0.0.1:28492/events)仍查询明确绑定的只读源。C留存包与此刻源总表查询不是同一不可变版本，不混算统计。P0未配置，INFO／旧general read-model未在本机准入；不是全站恢复或生产部署。

当前共用运行需要Git外的 `domeye-new-runtime/backend-local-readonly.env`（0600）、31627隧道、留存输入及 `.local/core-overview-validation/three-points-20260912-T53su1/run_preview.py`。该小入口复用`large-days-home-admission-v1/run_preview.py`的已验收只读启动／预检流程，明确核对清单版本；外部配置文件未改。连接5秒、语句及事务空闲15秒上限，仅为受控只读开发预览，不是常驻服务或空白环境安装器。旧入口仍保留；先确认端口无冲突，回执另选新路径。

以下仅为手动重启说明，**先检查端口，不要重复启动现有进程**。在一个终端维持本机隧道：

```bash
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=2 \
  -L 127.0.0.1:31627:127.0.0.1:31627 root@10.99.8.16
```

在本工作树根目录另一个终端运行后端，回执路径每次换成尚不存在的新文件，不能覆盖既有证明：

```bash
backend/.venv/bin/python -B .local/core-overview-validation/three-points-20260912-T53su1/run_preview.py \
  --manifest .local/core-overview-validation/three-points-20260912-T53su1/combined-index-v2/manifest.json \
  --version overview_index_v2_37e0ea72f8f1a18a4cf57d8d6be576983db95e5a04be87b78d5f349ac8e9c37d \
  --port 28491 \
  --proof .local/core-overview-validation/three-points-20260912-T53su1/restart-proof-001.json
```

前端沿用上方28492→28491命令；各前台进程用Ctrl+C停止。只恢复首页时仍可用上方无数据库的独立模式，先停止冲突端口；旧输入不删除。旧候选配置与失败回执保留，不作为当前运行选择；这次没有安装常驻服务。

### 当前37e版本独立恢复

当前55可用日／4失败日、规模和路径对照已经独立安装与恢复验证：280份源码和75份消费文件，534后端／143前端测试、114组HTTP同版对照及4种缺文件隔离通过。桌面／390px、两个详情和失败日图片已目视核对。无源库恢复实例不含旧事件／P0业务，不能称全站恢复。

30份材料已另存 `/Users/botongwu/.codex/recovery-drills/domeye-c-20260912-37e-v1/`。归档651,804,008字节，SHA256 `0cc042e69c2293b4d785be0ba1f13f992e133d530d03dfc1de7fd7f61a0421bc`；逐文件身份见该目录`保管回执.json`，命令见[当前恢复说明](../../.local/core-overview-validation/recovery-37e-20260912-2pwMpL/恢复说明.md)，范围见[台账第49节](../data-assets-and-admission.md#49-当前37e版本的独立恢复与空国家原文复核)。这是同机同磁盘消费恢复，不是异地或不可变备份；包内README为冻结时点，以包外说明为准。

### 历史独立恢复包

2026-09-12，新起源`410c…`版本已在独立目录重装依赖、构建测试，并在禁止原工作树／源库回读的Python隔离入口下恢复。273份实际源码及72份消费文件全部绑定；477后端／141前端通过，95组真实代理HTTP与主服务一致，含缺起源摘要、缺前缀摘要、缺日文件三种隔离恢复。桌面／390px及失败日期页面通过，不含旧数据库与全站恢复。

新包另存 `/Users/botongwu/.codex/recovery-drills/domeye-c-20260912-origin-v1/`，归档541,500,289字节，SHA256 `0c2ec85a3b81fb826615546898591270d0d71665fe8d9ac45270a5da82131e0c`。14份材料由`保管回执.json`绑定，命令见[起源版恢复说明](../../.local/core-overview-validation/goal-resume-20260912-3Nj0d8/recovery-v1/恢复说明.md)，边界见[台账第44节](../data-assets-and-admission.md#44-起源消费版本的独立恢复与保管)。同机同磁盘不是异地或不可变备份；旧包保留，不混用。

2026-09-11已在新目录按锁重装依赖、构建并恢复**旧前缀 `37b9…` 版本**：49可用日863,708条六类异常、10失败日及03-31 16:00的1,403,383前缀。94组实际代理HTTP与当时主页面一致，缺规模摘要只使规模不可用，缺日文件则整日503；放回新解包副本后同版恢复，原主输入未动。442项后端／140项前端测试、桌面与390px页面通过。该包不含新起源统计。

旧前缀版恢复材料另存工作树外 `/Users/botongwu/.codex/recovery-drills/domeye-c-20260911-current-v1/`：329,331,594字节归档、已执行入口、中文说明、验证回执与日志，由`保管回执.json`绑定。归档SHA256为`4a77255b9b4d9e1a6c5834db52362bc187e6e3db61860a6e2f1d404e268c9f11`；包含268份实际源码／配置／锁文件和67份消费文件，源码含未提交改动，不冒充HEAD。命令见[旧前缀版恢复说明](../../.local/core-overview-validation/current-recovery-v1/恢复说明.md)，范围见[台账第40节](../data-assets-and-admission.md#40-当前49日与前缀规模的同版独立恢复)。包内README属于归档冻结时点，以对应包外恢复说明为准；这些Git外材料不会随Git克隆取得。

材料只恢复C消费首页，不含凭据、原数据库／INFO／P0、MRT或上游完整证据。在无数据库的恢复实例中，旧事件检索明确显示查询失败、记录数未知；不声称全站恢复。后端使用Python层访问拦截，不是OS沙箱；工具链和下载缓存沿用本机，不是全新操作系统或断网验收，外部副本仍在同一磁盘。

旧 `cc17…` 恢复包及说明仍保留于 `/Users/botongwu/.codex/recovery-drills/domeye-c-20260911-v2/`，约10MB归档SHA256为`4b4375745d3ac14f98d2b0019a9cbedbe813153476d2fdeba289e82eff2c5de8`；当时31可用／28失败日且规模未知。历史73组HTTP及390／136项测试见[台账第31节](../data-assets-and-admission.md#31-独立目录重装依赖后的首页恢复)。旧包不代表当前数据，不能与当前入口材料混用；没有删除或覆盖它。

## 开发入口

- frontend：现有 Vue 页面、API 客户端与前端测试。
- backend：只读 Flask API、查询服务与 API 层测试。
- contracts：OpenAPI、数据 Schema 与测试样本。
- config/data-profile.json：唯一数据范围与时区配置。
- scripts：手动前台启动入口。

修改接口后运行 make api-types 更新前端类型，再运行 make test 与 make build。新项目使用独立本地 Git；源码来源和新项目交付状态由实际 Git 记录及运行验证说明，不沿用旧项目的验收结论。
