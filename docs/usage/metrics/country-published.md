# 已发布国家事件观测

已有国家事件接口可返回固定窗口的观测曲线、ASN 窗口名单和路径关联。读取时以该结果自带的指标说明和可用能力为准，不能把新计算的同名字段自动套到历史结果。

适用范围：通过事件解析取得的已发布国家观测。当前存在不同响应家族：有的返回 `track_definitions` 与 `tracks`，有的返回 `metric_definitions` 与 `series`。结果家族由程序识别，业务用户通常只需看到具体指标及其限制。

## 读取曲线

先读取事件绑定的窗口、数据截止和能力，再读取曲线。对于轨道型结果，按 `timestamps` 与对应 `tracks` 的位置关联样本，字段含义由同一响应的 `track_definitions` 给出。列表型结果使用自身的定义与样本结构。

以下列出已核对的历史轨道说明，不代表所有事件一定提供全部轨道。

| 轨道字段 | 含义与单位 | 使用条件 |
| --- | --- | --- |
| `interrupted_prefix_count` | 部分与完全中断的固定唯一前缀合计，单位条 | 同一时点的状态量 |
| `completely_interrupted_prefix_count` | 预期独立 peer ASN 方向均不可见的固定唯一前缀，单位条 | 属于中断前缀的一部分，不再与总中断量相加 |
| `invisible_direction_count` | 已核对历史定义为按 peer ASN 去重的不可见方向 | 不按新计算的端点×前缀解释 |
| `affected_asn_count` | 历史说明为至少一个固定前缀异常、且无未知前缀的 AS，单位个 | 与下一分类的互斥关系尚未独立确认 |
| `route_interrupted_asn_count` | 所有固定前缀完全中断、且无未知前缀的 AS，单位个 | 不直接与受影响类相加求唯一总数 |
| `fixed_visible_ipv4_address_count` | 固定前缀中可见 IPv4 网络的唯一地址并集，单位地址 | 不与 Feature 的 /24 覆盖折算量拼接 |
| `fixed_visible_ipv6_slash48_count` | 固定前缀可见 IPv6 /48 等价并集，单位 /48 等价量 | 按该结果定义与精度解释；不能仅按字段后缀推定算法 |
| `new_visible_ipv4_prefix_count`、`new_visible_ipv6_prefix_count` | 固定集合建立后纳入、当前可见的新前缀数，单位条 | IPv4、IPv6 分别统计 |
| `new_cumulative_ipv4_prefix_count`、`new_cumulative_ipv6_prefix_count` | 窗口内累计纳入的新前缀数，单位条 | 累计值取末值，不逐采样相加 |
| `new_visible_ipv4_address_count` | 当前可见新 IPv4 前缀的唯一地址并集 | 与固定集合可能有地址空间重叠 |
| `new_cumulative_ipv4_address_count` | 累计新 IPv4 前缀的唯一地址并集 | 累计前缀增加不等于同样多的独立新增地址 |
| `new_visible_ipv6_slash48_count`、`new_cumulative_ipv6_slash48_count` | 新 IPv6 前缀的当前可见／累计 /48 等价并集 | 当前量与累计量分别解释，精度按该结果保留 |

“新”指相对本次固定集合和窗口首次纳入，不是全球首次发布。固定与新增地址量相加不保证得到总唯一地址量；需要成员并集。

## 历史 ASN 分类的未决关系

现有文字说明不足以独立证明 `affected_asn_count` 与 `route_interrupted_asn_count` 的完整互斥、优先级和未知处理规则。这里保留 Unknown。可以分别报告返回数量，不能套用[新国家计算](country-calculation.md)的互斥分类去改写历史结果。

`event_classification` 是窗口分类。`fixed_prefix_count` 是对应固定前缀数量；`peak_partial_prefix_count`、`peak_complete_prefix_count`、`peak_invisible_direction_count` 是各指标的窗口峰值。不同指标的峰值未必同时发生。窗口名单长度不等于某一时点的受影响 ASN 数，缺少逐 ASN 时点数据时也不能推出分类转换时间。

## 路径关联

| 字段或结果 | 解释 |
| --- | --- |
| `observed_path_count` | 指定关联涉及的不同路径数，按该结果的路径身份去重 |
| `route_observation_count` | 原始路由观察引用去重数 |
| `independent_direction_count` | 按该数据家族方向定义得到的去重方向数 |
| `associated_prefix_count`／`associated_fixed_prefix_count` | 关联涉及的固定前缀数量 |
| `concurrent_sample_count`／`concurrent_state_point_count` | 关联与指定异常状态同时满足的采样点数 |
| 首次／末次并发时点 | 最早、最晚满足条件的位置；中间可能不连续 |
| `peak_concurrent_prefix_count` 及地址量峰值 | 各自的样本峰值，不保证在同一时点取得 |

有序 AS_PATH 的下游关联不要求直接相邻，也不等于网络依赖、原因或用户影响。历史路径样本用于分析时，其原观察时间不能改写成采样时间。完整路径如果只允许审计读取，不作为普通业务查询能力开放。

## 如何取得

按[事件查询流程](../guides/event-analysis.md)先解析事件、保持同一次读取的内部绑定，再调用概况、曲线、ASN 和路径关联接口。某项能力未提供时说明不可用，不用窗口名单或其他曲线替代。
