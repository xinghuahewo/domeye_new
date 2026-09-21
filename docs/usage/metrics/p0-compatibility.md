# 旧 P0 指标时序

P0 是既有候选制品的读取能力，有自己的十项指标名称、五分钟时间槽和缺失状态。它不是所有 Domeye 数据的统一入口，不能把它的名称和返回单位直接套用到档案、Core 或新国家结果。

## 十项兼容指标

下表说明已有 `metric_name` 的含义。合法名称也可能未出现在当前已准入候选中；先确认状态和实际可读结果。

| `metric_name` | 含义与单位 | 汇总和限制 |
| --- | --- | --- |
| `bgp_announce_record_count` | 来自 Feature 的宣告活动计数，单位次 | 名称中的 record 不把它变成 BGP 消息数；活动语义见[路由活动](activity.md) |
| `bgp_withdraw_record_count` | 来自 Feature 的撤回活动计数，单位次 | 同义、不重叠且覆盖适用的窗口可求和 |
| `bgp_update_record_count` | 宣告与撤回活动之和，单位次 | 不与前两项再次相加 |
| `bgp_withdraw_ratio` | 撤回／宣告与撤回总量，单位为 0–1 比例 | 与档案返回的百分数不同；分母零时为不可计算，不补零 |
| `ipv4_24_equivalent_count` | 相应 Feature 资源末值，实际按 /24 覆盖块计量 | 名称包含 equivalent 不改变其覆盖折算含义；见[资源](resources.md) |
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

## 如何取得

从 `/api/v1/p0/status` 了解当前候选状态，经 `/api/v1/p0/metrics/{metric_name}` 读取已准入指标，质量信息由 `/api/v1/p0/quality` 提供。它读取已经绑定的制品，不为任意对象和时间现场计算新序列；问题范围必须与返回的对象、窗口和限制一致。详见 [API 导航](../api/read-api.md#兼容与辅助入口)。
