# 遗留 P0 与 Dashboard 指标

本页是退役记录，不属于当前系统使用文档或默认业务检索范围。2026-09-21 的源码整理移除了 P0 与旧 dashboard 的六个 HTTP 入口、专用服务及合同，运行服务是否已切换需另行核对。当前替代查询及迁移边界见[业务 API 设计与退役](../architecture/业务只读API设计与退役.md)。

旧 P0 曾读取既有候选制品，有自己的十项指标名称、五分钟时间槽和缺失状态。它不是所有 Domeye 数据的统一入口，不能把它的名称和返回单位直接套用到档案、Core 或新国家结果。

## 历史 P0 十项指标

下表保留旧制品中 `metric_name` 的含义，供解释历史结果，不表示对应 HTTP 查询仍然存在。

| `metric_name` | 含义与单位 | 汇总和限制 |
| --- | --- | --- |
| `bgp_announce_record_count` | 来自 Feature 的宣告活动计数，单位次 | 名称中的 record 不把它变成 BGP 消息数；活动语义见[路由活动](../usage/metrics/activity.md) |
| `bgp_withdraw_record_count` | 来自 Feature 的撤回活动计数，单位次 | 同义、不重叠且覆盖适用的窗口可求和 |
| `bgp_update_record_count` | 宣告与撤回活动之和，单位次 | 不与前两项再次相加 |
| `bgp_withdraw_ratio` | 撤回／宣告与撤回总量，单位为 0–1 比例 | 与档案返回的百分数不同；分母零时为不可计算，不补零 |
| `ipv4_24_equivalent_count` | 相应 Feature 资源末值，实际按 /24 覆盖块计量 | 名称包含 equivalent 不改变其覆盖折算含义；见[资源](../usage/metrics/resources.md) |
| `ipv6_48_equivalent_count` | 相应 Feature 资源末值，实际按 /48 覆盖块计量 | 不是新国家计算的精确 /48 分数等价量 |
| `ipv4_equivalent_address_count` | Feature /24 覆盖块折算的 IPv4 地址量 | 不是任意前缀集合的精确地址并集 |
| `anomaly_incident_count` | 槽内按事件身份去重的异常数，单位次 | 跨槽相加前还需确认事件是否重复纳入 |
| `prefix_outage_concurrent_count` | 槽内已有 180 秒采样中的最大唯一中断前缀数，单位条 | 是采样最大并发量，不是开始事件数，也不是连续时间峰值 |
| `as_outage_concurrent_count` | 槽内已有 180 秒采样中的最大唯一中断 ASN 数，单位个 | 历史生命周期不足时保持 `legacy_unknown` |

## 时间与缺失

这组 MetricSeries 使用 300 秒时间槽，窗口为 `[start,end)`。资源取对应槽内提供的末观察值；没有适用资源行时不自动延用前值。并发指标内部采用已有 180 秒采样，不能把两种间隔当成同一个定义。

`value_state` 区分 `observed_zero`、`observed_nonzero`、`source_unavailable`、`parse_failed`、`processing_gap`、`not_observed`、`not_applicable` 和受限使用的 `legacy_unknown`。未知点的 `value` 保持 null，具体原因随结果保留。

ASN 稀疏无行仅在来源与稀疏条件已被明确验证、且属于前三个活动计数时才可能解释为零。它不适用于资源、比例或并发。并发采样数组为空表示缺少可复核采样，不证明零中断。

`source_coverage_ratio` 是来源可用槽／预期槽，`metric_coverage_ratio` 是指标已观测槽／预期槽，`subject_activity_density` 是对象显式有行槽／来源可用槽，三者都是 0–1 比例。对象有行不等于所有派生值可计算；槽分类完整也不等于所有槽都有有效数据。

## 历史 Dashboard 派生数量

| 旧字段 | 当时定义 | 解释限制 |
| --- | --- | --- |
| `event_count` | 按开始时间纳入的六类历史事件记录总量 | 不是所有与窗口重叠的事件 |
| `event_change_rate` | 当前与前一等长窗口记录数的相对变化，单位 % | 前窗为零时不可计算；两窗范围须可比 |
| `active_event_count` | 当前窗口内开始、未记录结束的非泄漏事件记录数 | 缺少结束不证明持续；不是某时点全部仍发生的事件 |
| `affected_asn_count`、`affected_country_count` | 历史记录关联字段中解析并去重的对象数 | 不等于真实受影响对象总量，也不是国家增强曲线的同名量 |

当前国家／ASN 档案仍有 `anomaly_count`、`high_risk_count`，旧中断采样仍有 `outage_count`，这些定义继续维护在[事件指标](../usage/metrics/events.md)，不因旧 dashboard 退役而删除。

## 退役范围

移除 `/api/v1/p0/status`、`/api/v1/p0/metrics/{metric_name}`、`/api/v1/p0/quality` 及 `/api/v1/dashboard/counts/total`、`/api/v1/dashboard/counts/type`、`/api/v1/dashboard/overview`，在新源码中均返回 404。P0 的旧运行配置键只为配置迁移而接受，加载后丢弃，不能恢复这些接口。

未删除原始数据、历史制品、离线 MetricSeries Schema 和共用质量计算。新查询依照当前业务导航选取数据家族，不用重定向把旧窗口数量替换成另一来源或另一统计范围。
