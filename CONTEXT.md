# Domeye 底层路由数据术语

本术语表描述项目共用的 `Collector → Peer → Session → RouteEvent → RouteState` 观测链。涉及身份关联、时间、缺口和状态解释时，参阅[底层路由观测与证据边界决策](docs/adr/0001-routing-observation-evidence-boundaries.md)。

## 术语

**Collector／采集器**：接收并记录远端 BGP 邻居信息的观测节点，限定一份路由观察的来源范围；本地解析机器或处理批次不构成新的 Collector。
_避免_：将单个 Collector 的视图称为全网状态。

**Peer／远端 BGP 邻居**：Peer 是 Collector 所观测的远端 BGP peer；Domeye 保留 MRT 提供的 Peer BGP ID、Peer IP、Peer AS 等原始身份属性，再定义自己的稳定 Peer Identity。该稳定身份的作用域限定在单个 Collector 内，原始属性、Peer ASN 和既有 VP 标识不直接等同于 Peer Identity。
_避免_：用单一 ASN 代替 Peer，或把既有 VP 直接改称新的 Peer Identity。

**Session／BGP 会话**：Collector 与远端 Peer 之间的一次实际 BGP 会话，其身份和起止边界可能只能被部分确定。会话观测片段描述已有证据的范围，不能直接等同于一次真实 Session。
_避免_：把首末观察时间当作会话起止，或把数据缺口当作重连。

**RouteEvent／路由观察记录**：来自原始记录的单个路由元素观察，区分 `announce`、`withdraw` 与 `rib_snapshot`，不保证路由状态发生变化。
_避免_：将 RouteEvent 等同于业务异常事件、状态变化次数，或将快照条目计作新宣告。

**RouteState／路由观测状态**：在指定观测范围、查询时点和数据版本下，依据已有证据重建的路由状态；存在性判断区分存在、不存在和未知。最后已知状态是带时间和来源的历史证据，不等同于查询时点的确定状态。
_避免_：把未知补成零、把旧路径当作当前路径，或把观测状态解释为全网转发状态。
