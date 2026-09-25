# 只读业务 API 导航

本页把业务问题对应到已经实现的读取入口。现有 32 个入口均为 GET，其中 6 个标记 deprecated；路径存在不保证已经绑定可读数据。精确响应结构与正式参数见 [OpenAPI](../../../contracts/openapi.json)。

以下路径均从站点根开始。`/api/v1` 与 `/api/v2` 分别是两组实际前缀，不要将 `/api/v1` 再加到完整 v2 路径前。花括号为需替换并正确 URL 编码的参数。

## 核心态势与共享快照

| GET 路径 | 用途 | 主要参数与注意点 |
| --- | --- | --- |
| `/api/v1/healthz` | 服务健康 | 不能由服务健康推断业务数据完整 |
| `/api/v1/core-overview` | 日期目录、所选日概况、筛选列表与小时趋势 | `date`、`family`、`kind`、`level`、`hour`、`q`、`sort`、`page`、`page_size`、`version`；以响应实际筛选范围解释总数 |
| `/api/v1/core-overview/record` | 留存异常详情 | 必须使用列表取得的 `ref` 和 `version` |
| `/api/v1/resources` | 窗口内独立 RIB 的总体资源统计点 | 必须提供 `start_time`、`end_time`；半开窗口最多 24 小时，固定八项 global 指标，不支持任意指标或 ASN 筛选 |
| `/api/v1/rib-snapshots` | 发现已登记共享快照 | `date` 或 `latest`；未配置时明确返回相应状态 |
| `/api/v1/rib-snapshots/{version}` | 同一快照总体规模 | `family`；保持发现取得的版本 |
| `/api/v1/rib-snapshots/{version}/asns/{asn}` | 同一快照中指定明确起源 ASN 的资源 | `family`；数量与返回样本列表长度分别解释 |
| `/api/v1/rib-snapshots/{version}/observations` | 该快照的有界原始观察分页 | `page`、`page_size`；用于审计读取，不是所有历史 UPDATE 的查询入口 |

Core 的规模与路径对照位于其响应相应部分；无需编造一个尚不存在的“路径变化次数 API”。共享快照的同版查询说明见[快照指标](../metrics/snapshots.md)。

完成文件交付模式中，Core 额外提供 `event_trends`：六类事件分别按开始时间计数，只受日期和地址族影响，列表的类型、等级、搜索与分页不改变趋势。`metadata.result_delivery.intervals` 给出实际覆盖；每个小时只返回与覆盖相交的片段，窗口外和中间缺口不补零。投影失败时趋势明确不可用。旧 `trend` 仍是每小时前缀中断对象去重数，两者含义不同。

同一模式下，`metadata.rib_statistics` 提供所选日、同一采集器、实际交付时段内最新独立 RIB 的规模及时点；Core 的两个规模主值来自这里，列表筛选不改变快照选择。资源失败不抹去独立事件结果。`/resources` 返回按时点排序的 `points`，没有点为 `not_calculated`，未启用交付为 `not_configured`，读取失败为 HTTP 503；点内只使用 `main`，并检查 `qualification` 和单位。一个点不代表连续曲线。该入口时间支持秒级本地格式，也支持带 Z 或偏移的 ISO 时间；无时区时按 Asia/Shanghai，重复和未知参数返回 400。

## 历史事件与事实读取

| GET 路径 | 用途 | 主要参数与注意点 |
| --- | --- | --- |
| `/api/v1/events` | 历史事件列表与数量 | `date`、`event_type`、`level`、`country`、`event_info`、`sort_mode`、`page_num`、`page_size`；筛选值遵循此入口，不能直接照搬 Core 的 `kind` |
| `/api/v1/events/top` | 置顶／近期事件 | `event_type`；不是任意指定窗口的全量事件目录 |
| `/api/v1/{event_type}/{start_time}/{problem}/{event_id}/{source}` | 一条历史事件详情 | 身份各部分沿用列表记录；不能只凭对象名称生成 |
| `/api/v1/events/evidence-bundle/{event_type}/{start_time}/{problem}/{event_id}/{source}` | 业务事实记录与其限制 | 保留语义约束；不是根因结论 |
| `/api/v1/events/story/{event_type}/{start_time}/{problem}/{event_id}/{source}` | 已建立的事件研究叙事 | 只适用已配置事件；未配置不应伪装为通用生成能力 |
| `/api/v1/events/observations/{event_type}/{start_time}/{problem}/{event_id}/{source}` | 兼容旧引用的国家观测读取（deprecated） | 新调用优先沿明确国家事件解析流程选择可用能力 |

历史事件 `date` 常用 `YYYY-MM-DD_YYYY-MM-DD`，日期右边界按该日结束处理。返回 `record_count` 当前为字符串；程序需要按接口类型解析，不能用当前页长度代替总量。时区与窗口规则见[时间说明](../rules/time-and-aggregation.md)。

## 国家和 ASN 的活动、资源与档案

| GET 路径 | 用途 | 主要参数与注意点 |
| --- | --- | --- |
| `/api/v1/features/top` | 单个目标的时序 | `target`、`start_time`、`end_time`；目标可为该入口支持的国家名、ASN 或采集范围标记 |
| `/api/v1/features/countries` | 国家对象及其时序分页 | `country`、`page_num`、`page_size`、`start_time`、`end_time` |
| `/api/v1/features/countries/overview` | 国家档案与候选排名 | `country`、`limit`、`start_time`、`end_time`；普通窗口最多 24 小时 |
| `/api/v1/features/ases` | ASN 对象及其时序分页 | `asn`、`country`、`page_num`、`page_size`、`start_time`、`end_time`；默认候选不代表全部 ASN |
| `/api/v1/features/ases/overview` | ASN 档案及运营候选排名 | `asn`、`limit`、`start_time`、`end_time`；普通窗口最多 24 小时；事件窗口扩展见文末 |
| `/api/v1/features/ases/events` | 指定 ASN 的精确关联事件 | `asn`、`page_size`、`start_time`、`end_time`；事件窗口扩展见文末 |

时间参数按当前实现使用 `YYYY-MM-DD HH:MM:SS`，不假定所有旧入口都接受带时区 ISO 字符串。国家参数沿用该入口列表返回的名称，不能默认把中文名与国家代码互换。除单目标识别明确支持外，ASN 参数使用相应接口要求的数字形式。

国家和 ASN 档案、ASN 关联事件均使用 `[start_time,end_time)`；档案响应以 `window_boundary` 明示。Feature 列表的页数按对象划分，不是时间覆盖证明。档案保留缺失值，零分母比例返回 null；派生字段的单位及条件见[活动指标](../metrics/activity.md)和[资源指标](../metrics/resources.md)。

## 国家事件观测

| GET 路径 | 用途 | 主要参数与注意点 |
| --- | --- | --- |
| `/api/v2/events/resolve` | 按所选来源解析国家事件 | `ref`；完成文件模式返回共用详情和数据版本，可选 `version` 校验一致性；历史响应保持发布绑定 |
| `/api/v2/country-outages/{incident_id}/overview` | 该事件观测概况 | `publication_id`，沿用解析结果 |
| `/api/v2/country-outages/{incident_id}/series` | 该事件窗口曲线与指标说明 | `publication_id`；区分轨道型与列表型结果 |
| `/api/v2/country-outages/{incident_id}/asns` | 已提供的 ASN 名单 | `publication_id`、`page`、`page_size`、`query` 及该结果家族支持的分类／地址族／排序筛选 |
| `/api/v2/country-outages/{incident_id}/path-downstreams` | 指定事件的路径下游关联 | `publication_id`、`affected_asn`、`scope`、`query`、`page`、`page_size` |
| `/api/v2/country-outages/{incident_id}/trend` | 已有确定性趋势产品 | `publication_id`；是否有结果依赖实际绑定 |
| `/api/v2/country-outages/{incident_id}/audit` | 内部审计信息 | `publication_id`；通常不进入业务回答正文 |

候选已实现、尚未部署：完成文件模式返回 `schema_version=country-outage-delivery/v1`，`event` 复用 Core 详情。首次解析可省略版本，响应自带实际版本和覆盖；后续携带版本若已变化则返回 409。此家族没有 `publication_id`，不应再拼接上表的历史发布接口。已有峰值、名单和生命周期直接读取 `event.item`；普通统计沿用国家 Feature 与下方中断曲线 API。它们仍需检查各自数据覆盖、单位和同次阅读的版本。专用增强能力是否可用由实际绑定决定，解析成功不宣称路径关联等均可用。

不同国家响应家族对 `asns` 筛选与能力的支持不完全相同。先按返回能力选择，再按该家族校验参数；不把所有可见参数视为每个事件都支持。历史 ASN 窗口名单不当作某个时点的成员集合。

## 单国路由活动与资源曲线（候选已实现，尚未部署）

`GET /api/v1/features/countries/series` 要求 `country`、`start_time`、`end_time`，可选 `version`。国家名沿用数据库名称，`collect` 不作为国家；时间按北京时间 `[start_time,end_time)`，单次最多 24 小时。该入口只读指定国家的既有五分钟样本，不提供国家排名和前窗比较；总览仍使用 `/features/countries/overview`。

三个国家曲线入口（本接口、`outages/country-as`、`outages/country-prefix`）共用当前结果源的国家目录。先使用 `start_time/end_time` 且不传 `country` 查询 `/api/v1/core-overview`，从响应 `metadata.countries` 选择名称；仅传 `date` 的模式不返回该目录。三个入口的 `country` 须精确匹配目录值；目录之外的代码、别名或拼写返回 400，不能把未识别对象解释为零中断或空样本。目录读取失败返回 503。已识别国家在覆盖内没有中断记录时仍可返回观测值 0，缺少 Feature 样本仍保留空数组；对象识别与数值可用性分别判断。

候选响应 `country-feature-series/v3` 尚未部署。`query` 保留按 `source.label` 文件标签选择的查询范围，`metadata` 给出版本、采集器、处理覆盖、单位与实际计量方式。每个样本的 `source` 保留已有交付文件身份和标签；`activity` 的活动次数直接绑定实际 `start/end_exclusive` 区间，`resources` 的资源数量直接绑定文件末态时点 `at`。没有共用的测量时间字段，文件标签不代替资源时点。非对齐查询可能选中超出请求端点的完整文件，不得把标签范围当作精确活动区间。处理覆盖不证明样本齐全；字段未知为 null，缺样本不补零。活动、覆盖块数与地址折算的定义分别见[活动](../metrics/activity.md)和[资源](../metrics/resources.md)，不能仅从字段名推断计量对象。

前端按同版结果补空槽和处理缺口的断线。版本冲突 409，查询无效 400，读取失败、重复时点或无效数值 503。此候选需要完成文件来源；没有覆盖与版本的历史源不冒充已核对曲线。

## 中断曲线（候选已迭代，尚未部署）

以下五个入口在候选实现中共用已交付事件与覆盖查询，OpenAPI 已同步移除 deprecated。现网旧实现与新响应不能混用；统计定义和逐点状态以[中断时序覆盖合同](../metrics/events.md#中断时序的覆盖合同候选实现尚未部署)为准。

| GET 路径 | 统计对象 | 必需参数 |
| --- | --- | --- |
| `/api/v1/features/outages/country-as` | 国家检测记录中的去重 AS | `country` 与起止时间 |
| `/api/v1/features/outages/country-prefix` | 国家粗路由集合中的去重前缀 | `country` 与起止时间 |
| `/api/v1/features/outages/as-prefix` | 单 ASN 粗路由集合中的去重前缀 | `asn` 与起止时间 |
| `/api/v1/features/outages/global-as` | 当前采集范围的去重 AS | 起止时间；global 不表示全互联网 |
| `/api/v1/features/outages/global-prefix` | 当前采集范围粗路由集合的去重前缀 | 起止时间 |

五个入口统一返回 `query / metadata / data`，不再返回无覆盖元数据的裸数组；前端类型及适配随合同更新。可选 `version` 用于拒绝不同交付版本，400 表示参数错误、409 表示版本已变化、503 表示无法可靠读取。GET 仅查询现有表，不生成额外制品。

## 调用结束时检查什么

上述单国 Feature 与五个中断时序入口均可选 `summary_seconds`，从原样本得到首末量、极值、均值、端点差和适用的活动合计。读取 `summary` 时一并保留 `query` 与 `metadata`；时间和缺口规则见[分桶统计](../rules/time-and-aggregation.md#时序的分桶统计候选已实现尚未部署)。该候选接口扩展尚未部署。

同时检查 HTTP 状态和响应正文；部分兼容接口以 `status:false` 表示业务失败。状态正常后，再判断数值是否适用、是否还有分页、是否包含当前问题所需的对象与时间。详见[查询步骤](../guides/query-data.md)与[结果状态](../rules/result-states.md)。

## ASN 事件窗口参数

`/api/v1/features/ases/overview` 和 `/api/v1/features/ases/events` 正式支持 `event_window`、`event_reference`。布尔参数只能为 `true` 或 `false`，默认 `false`，不接受重复参数。`true` 时必须给出单个数字 ASN 和事件引用，起止时间与已解析国家事件原窗口严格相同，最多 45 天；普通模式最多 24 小时，不能附带非空事件引用。档案响应此时使用 `scope_kind=event_window_selected_asn`，不查询前窗，前窗活动与环比为 null。

必需参数缺失、格式或窗口无效返回 HTTP 400；事件窗口无法核对时返回已有 503 状态。Feature 单目标读取失败返回结构化 500，不返回原始异常文本。部分旧事件入口仍需检查 HTTP 200 正文中的 `status:false`。

## 已退役入口

P0 与旧 dashboard 的六个入口已从源码和合同移除，返回 404。它们不自动重定向到不同来源或不同统计范围。退役路径、当前迁移选择和源代码／运行服务的区别见[业务 API 设计与退役](../../architecture/业务只读API设计与退役.md#退役与保留清单)。
