# 指标目录

按结果家族选择指标说明。同名字段在不同家族中可能具有不同单位或统计总体；每篇说明均给出适用范围、主要字段、汇总方法和结果限制。

| 结果家族 | 主要问题与字段 | 说明 |
| --- | --- | --- |
| 原始观察与路由状态 | 消息数、元素数、存在／不存在／未知、最后已知路径 | [观察与状态](observations.md) |
| 单 RIB 规模与路径对照 | `visible_prefixes`、`visible_origin_ases`、`prefix_count`、`different_fraction` | [快照与路径对照](snapshots.md) |
| 路由活动和档案聚合 | `announce`、`withdraw`、`update_total`、`withdraw_rate`、`volatility` | [路由活动](activity.md) |
| Feature 与独立 Resource | `v4Prefix_num`、`ipv4_prefixes`、`ipv6_prefix_count`、参考范围与拓扑 | [资源数量](resources.md) |
| 异常及事件查询 | 六类事件、`record_count`、`event_count`、`active_event_count`、小时趋势 | [事件数量与生命周期](events.md) |
| 已发布国家事件 | 现有国家接口的曲线、ASN 窗口名单、路径关联 | [已发布国家观测](country-published.md) |
| 新国家计算 | 前缀状态、端点方向、互斥 ASN 分类、精确地址并集 | [新国家计算结果](country-calculation.md) |
| 趋势计算 | 极值、下降、反弹、累计缺口、阶段、双栈差异及同期参照 | [趋势分析](trends.md) |
| 旧 P0 指标时序 | 十项兼容指标、五分钟槽、0–1 比例、并发采样与覆盖率 | [旧 P0 指标](p0-compatibility.md) |

## 根据常见说法选择

| 用户说法 | 需要明确的业务含义 |
| --- | --- |
| “多少前缀” | 唯一 CIDR 条数，还是 IPv4 /24、IPv6 /48 覆盖块数 |
| “多少更新” | 宣告与撤回的路由元素总量，还是原始消息数量 |
| “发生多少次中断” | 开始的事件数、开始中断的唯一对象数，还是某时点的并发数量 |
| “影响多少 AS” | 哪个数据家族、哪个时点、哪种分类，还是整个窗口的名单 |
| “下降最多” | 明确指标与单位、两个时点、带符号的下降量及排序对象范围 |
| “恢复了多少” | 指标从谷值反弹多少，还是事件生命周期确认恢复；两者分别回答 |

共同的运算条件见[时间、汇总与比较规则](../rules/time-and-aggregation.md)。某项指标的计算实现存在时，也可能暂时没有业务查询接口；各篇的“如何取得”说明这一差别。
