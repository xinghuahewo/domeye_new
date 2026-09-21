# 新国家计算结果

新国家计算在固定集合、相同时点下定义前缀状态、端点×前缀方向和互斥 ASN 分类。地址量使用精确地址并集；IPv6 /48 等价量可为分数。

适用范围：新国家计算已经实现的结果与相应资格化读取。这里定义计算含义，不宣称这些结果都已接入现有国家 Web 接口。[已发布历史国家结果](country-published.md)单独解释。

## 固定集合、方向与状态

固定集合在事件前的适用基线上建立，后续新出现前缀另行统计。国家纳入以相应前缀组规则选择；不能进一步假定组内所有路径和关联 ASN 都具有目标国家属性。

一个观察方向是一个**端点×前缀**组合。同一端点观察多个前缀会贡献多个方向，不能把方向数称为端点个数。ADD-PATH 的多条路径在同方向内按存在性归并，不增加方向分母。

前缀状态：预期方向全部可见为 normal；全部明确不可见为 complete；部分可见、部分明确不可见为 partial；存在无法判断方向或基线不适用时保留 unknown。采样采用该时刻之前的状态。

## 19 项基础指标

同一数值绑定固定集合和采样时点。下表数量均为时点量，标明“累计”的除外。

| 字段 | 定义 | 单位 |
| --- | --- | --- |
| `normal_prefix_count` | 固定集合中 normal 前缀数 | 唯一前缀条数 |
| `partially_interrupted_prefix_count` | 固定集合中 partial 前缀数 | 唯一前缀条数 |
| `completely_interrupted_prefix_count` | 固定集合中 complete 前缀数 | 唯一前缀条数 |
| `interrupted_prefix_count` | partial 与 complete 前缀合计 | 唯一前缀条数 |
| `invisible_direction_count` | 固定集合中明确不可见方向数 | 端点×前缀组合数 |
| `visible_direction_count` | 固定集合中明确可见方向数 | 端点×前缀组合数 |
| `normal_asn_count` | 所有固定前缀均 normal 的 ASN 数 | ASN 个数 |
| `affected_asn_count` | 有异常、未达到全部 complete 且无未知前缀的 ASN 数 | ASN 个数 |
| `route_interrupted_asn_count` | 全部固定前缀 complete 且无未知前缀的 ASN 数 | ASN 个数 |
| `fixed_visible_ipv4_address_count` | 固定前缀中仍可见 IPv4 网络的精确并集 | 地址个数 |
| `fixed_visible_ipv6_slash48_equivalent` | 固定前缀可见 IPv6 地址并集÷2^80 | /48 等价量 |
| `new_visible_ipv4_address_count` | 新前缀中当前可见 IPv4 地址并集 | 地址个数 |
| `new_cumulative_ipv4_address_count` | 累计新前缀的 IPv4 地址并集 | 地址个数，累计 |
| `new_visible_ipv4_prefix_count` | 当前可见的新 IPv4 前缀数 | 唯一前缀条数 |
| `new_cumulative_ipv4_prefix_count` | 到当前已纳入的新 IPv4 前缀数 | 唯一前缀条数，累计 |
| `new_visible_ipv6_slash48_equivalent` | 新前缀中当前可见 IPv6 地址并集÷2^80 | /48 等价量 |
| `new_cumulative_ipv6_slash48_equivalent` | 累计新前缀的 IPv6 地址并集÷2^80 | /48 等价量，累计 |
| `new_visible_ipv6_prefix_count` | 当前可见的新 IPv6 前缀数 | 唯一前缀条数 |
| `new_cumulative_ipv6_prefix_count` | 到当前已纳入的新 IPv6 前缀数 | 唯一前缀条数，累计 |

同一时点的 normal、affected、route_interrupted 三类 ASN **互斥**，另保留 unknown。全部前缀完全中断的 ASN 不再次计入 affected。这个规则不用于补写历史分类。

单位示例：一个 /64 的精确 IPv6 地址量为 2^64，折成 /48 为 `2^64/2^80=1/65536`；它不是一个 /48 覆盖块。示例不是实测。

## 缺失、分母和汇总

当未知状态使精确总数无法确定时，保留未知主值和 `known_lower_bound`。已知下界只能表述为“至少”，不能冒充精确总数或精确分母。

前缀比例、方向比例、ASN 比例分别使用对应固定分母。分母未知或为零时不可计算；同名的“比例”不一定采用同一总体。

当前可见量按时点比较，累计量取末值或成员集合差。固定与新前缀的地址空间可能重叠，两项地址量相加不必等于总唯一地址量。跨 ASN、窗口或国家的唯一数需要按成员去重。

## 窗口分类、采样和关联结果

`fixed_prefix_count` 表示对应集合或 ASN 的固定前缀分母。`WindowClass`／`event_classification` 根据整个窗口中出现的情况和优先级归类，不能代替每个采样时点的分类。

`observed_slots`、`unknown_slots` 是样本数量，不是连续持续时间。`Peak` 保存已知样本极值、首次时点和出现次数；完整窗口极值需要相应覆盖条件。

路径、原始观察、独立方向、关联前缀、并发采样以及并发前缀／地址峰值，分别按自己的对象身份统计。与历史接口有近似字段名时仍先确认定义，通用解释见[路径关联](country-published.md#路径关联)。

该计算的工程入口与消费状态见[国家公开接口说明](../../architecture/国家C4公开接口.md)。普通问数只在已经完成并允许读取的结果范围内使用这些定义。
