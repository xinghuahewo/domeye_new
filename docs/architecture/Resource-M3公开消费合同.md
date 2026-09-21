# Resource M3 公开消费合同

状态：已授权实施，尚未验证；基线 `52f0a1fe74e7cc1ffbea5a05a83751145d775056`。本片只消费固定 M2 观察，不改变旧 Resource 算法、旧 complete 入口及旧结果读取。

## 有限入口

新增 `resources.observation.produce_observation_resources`，正式运行经 `scripts/pipeline/resource-frozen-run.py` 的 `operation=resource_observation` 在业务导入前冻结完整代码。输入沿用 `SourceBinding(run_id,snapshot,purpose,context)`、半开 `result_window`、CSV 绑定与独立 country_reference；显式使用 observation profile。RIB 按实际计算时点递增，每份通过公共 `ordered(reader)` 单源选择，保留所属封存完整 MRT 清单中的原 rank；参考无 MRT rank。重复已选 RIB 拒绝；上游合法路径别名只沿用 M2 归一化。

输入回执绑定实际 PG system_identifier、database OID/name、M2 plan/seal/snapshot、完整 InputBinding、所选 checkpoint/attempt、原始 Peer 表引用、MRT 时点范围与原始质量、CSV checkpoint 和原始定位、country 原件及规范历史版本。选择和参考在完成前再次校验。其他 UPDATE 的观察质量保留在完整绑定/checkpoint 中，不将其 Gap 自动传播给独立 RIB，也不声明整源或全链无 Gap。

## 结果与资格

新增独立 `resource-observation/v1` 结果 profile、`resource-qualification/v1` 资格规则。旧十张科学表原字段/算法不变；新增有限 typed 表保存输入回执、Peer 依赖、覆盖、逐结果资格和 normal 依赖。历史主体仍在 DuckLake/Parquet，PG 保存身份、状态与现有必要工作态。

资格状态只允许 `qualified`、`unknown`、`not_applicable`。覆盖范围只代表 `independent_rib` 或 `declared_sample_set`，不代表全网、连续历史或恢复。非空完整 RIB 的原始数值可在其独立观察范围合格；零元素仍保留计算原值与覆盖。资格依据是时点和完整范围证据，不是行数本身；本版 M2 没有可证明空快照闭集的独立凭据，MRT EOF/Peer 表/SourceEnd 不构成该证明，因此零元素案例主值 Unknown。正常带保留旧冷启动自含当前样本算法；可用正常带另外要求至少六个先前样本、实际全部样本和其 RIB 依赖合格，且只对声明样本集合有效。该门槛来自 [ResourceComputer.compute](../../backend/data_pipeline/analysis/resources/compute.py) 的 `len(samples) > 5` 先前样本分支；`candidate.rib_count < 7` 时把当前值纳入样本的冷启动回退继续原样计算，只不将该回退提升为已证实的先前正常基线。未证实的旧连续历史等价和参考历史适用性保持 Unknown。参考版本内的标签/拓扑与历史事实解释分开；缺参考标签或未计算拓扑不能成为合格主值。

公开 `ResourceObservationReader(dsn,run_id,expected_snapshot,expected_dataset_id)` 只读已完成新 profile，提供 `inputs()`、`coverage()`、`scan(table,scope='result')` 和 `values()`。`scan` 是原值审计；`values` 返回原值、资格及 nullable 主值，不改写原零值。默认仅结果窗；`scope='all'` 才含前置样本。无业务行也有独立覆盖。

完成回执枚举全部表、行数和内容摘要，校验依赖引用与实际计数；新 Reader 校验固定 PG/目录/完成身份、完整表枚举和摘要，并在读取尾部复验状态。candidate/failed、错 seal、漏依赖、伪计数或内容漂移不可读。该有限离线 Reader 不实现业务发布、页面或兼容旧发布 full 门禁。

## 影响范围

只新增消费已有 M2，不需要重算观察。Resource 新候选按新代码/资格版本重新生产到新身份和目录，不覆盖旧结果。Feature、Detection、canonical、Country、Publication、前端和真实全天运行均不在本片。

## 本分支接线与表枚举（已实现，待独立审查）

输入接口实现见 [observation.py](../../backend/data_pipeline/analysis/resources/observation.py)，旧 [produce.py](../../backend/data_pipeline/analysis/resources/produce.py) 仍沿用 complete。`SourceBinding` 的全部字段和 `RibContext` 保留；CSV 参数为 `run_id/snapshot/anchor_source_id/source_id`，country 参数为 `reference_id/dataset_id`。`anchor_source_id` 只用于公共 Reader 构造，不给 reference 分配 MRT rank。所有依赖要求同一实际 PG 系统与库；其他库不能只靠相同 run 文本混入。

`binding_manifest.observation_runs` 按公共 `binding_id` 保存一次完整 InputBinding、seal 和规范 manifest；`observation_inputs` 保存实际所选 source 的原 rank、binding 引用和完整 checkpoint。逐 RIB 回执引用具体 attempt、Peer 表位置/索引、实际首末元素 epoch。M2 当前拒绝跨 epoch RIB；Resource 仍防御性核对首末元素 epoch 与声明时点一致，不将 EOF 当作时点闭集证明。

[qualification.py](../../backend/data_pipeline/analysis/resources/qualification.py) 是完整列结构与资格枚举的权威位置：

| 表 | 数据责任 |
|---|---|
| sources、metrics、memberships、rendered_paths | 原始 Resource 来源、数值、集合与路径文本 |
| normal_bands、normal_samples | 旧正常边界、实际样本/时点/来源/值 |
| topology_edges、topology_status | 旧国家参考投影内的边、状态、阈值与样式 |
| decoding_differences、decision_refs | 原解码差异及每个元素的旧处理决定 |
| input_receipts | 实际所选 RIB 的绑定、rank、attempt、结束计数、Peer/质量计数和时点 |
| peer_dependencies | 所依赖原始 Peer 表位置与原属性；不是稳定 Session/Peer Identity |
| observation_quality | 所选 RIB 的原始源级/消息级质量记录；消息位置可空，不制造 Gap |
| coverage | 每 RIB 独立时点、前置样本清单、连续历史和参考历史的覆盖；无 Peer/业务行也保存 |
| qualifications | 每个指标单元、正常带或国家拓扑的独立资格、原因、范围和版本 |
| qualification_dependencies | 资格对当前 RIB、实际 normal 样本、影响筛选/保留状态的全部前置 RIB 或固定 reference 的引用 |

PG `resource_runs.observation_receipt` 保存全表行数、逻辑内容 SHA256、输入/代码/目录身份；新 dataset_id 绑定该回执、运行和固定快照。来源和资格历史不写进 PG 工作表。写入完成前核对样本值与它所依赖的原始科学行、样本数量/唯一性、Peer 和质量计数，再核对所有上游及冻住代码。物理表枚举也必须恰好等于这16张表。

公开 Reader 位于 [observation_reader.py](../../backend/data_pipeline/analysis/resources/observation_reader.py)。`values(target='metrics'|'normal_bands'|'topology_status', scope='result'|'all')` 返回 `{raw, qualification, main}`；normal 主值为上下界/均值/总体标准差，topology 主值为状态/节点数/边数。`scan('topology_edges')` 等接口只提供原值审计，调用方必须联结同版资格，不可把 raw 当自动可发布结果。读取前检查全表内容与依赖，完整迭代后再次检查内容/状态/依赖；中途放弃迭代不是完成读取回执。

本版是有限离线核验 Reader：每次读方法会执行全表核验，安全优先但成本高，不宣称适合高频页面请求或全天容量。没有快照恢复/自动重试/发布副本；不为减少核验成本放宽资格。

## 旧逻辑保留与重算范围

| 旧行为 | 本片处理 |
|---|---|
| 全局/9808/4837/4134 首 AS 与按 Peer ASN 统计、各单位 | `compute.py`、数量 helper 不改动 |
| AS_SET 先保留 prefix、私有尾 AS、默认路由等决定 | 原 Decision 与 raw 数值、成员继续写原表 |
| 全量集合外置、路径字典、逐元素决策 | 沿用原 ResourceStore，新增 profile 只扩展表集和完成身份 |
| 正常带冷启动、自含当前样本、非减 list_len、总体标准差、异常样本过滤、3天清理 | 原算法、raw 边界、sample 和工作态不改；资格单独保守判断 |
| 国家同参考标签拓扑、路径跳过/桥接、5万边阈值、旧渲染样式 | 原值及状态保留；未知/缺参考或未算不升为主值 |
| 原 source.complete、旧 result/all 读取 | 旧入口与旧资格保持；新 profile 必须走新 Reader，避免绕过新增资格 |

无需重算 M2；本片没有修改任何观察生产/解释/选择模块。已有 Resource 结果不增加默认资格、不覆盖；要取得新资格须从固定 M2 输入生产新的 Resource 候选。旧 v1 的现存人工结果只读审计，不冒充新 profile；旧 v3 文件已不存在的测试记录不重新导入伪装为兼容证明。

正常带资格还核验声明链中所有前置 RIB 的时点依赖，包括已被旧异常过滤排除的点；这些点仍能影响样本选择/保留状态。前置点未获时点资格时，后续正常带/异常判断主值 Unknown，独立当前 RIB 数量仍可合格。原样本选择、raw 边界和 is_outlier 不变。

## 11f2f27 独立复核后的定向修复（已实现，待增量复核）

保持 `resource-observation/v1`、`resource-qualification/v1`、16表、字段、资格生成与 dataset_id 算法不变，仅加强既有依赖合同校验：

- [validation.py](../../backend/data_pipeline/analysis/resources/validation.py) 经公共 Reader 的固定封存选择读取所选 checkpoint/attempt 的 Peer、quality；先按 M2 原摘要算法核实际原行，再逐行对照输出的原 key 与全部属性、质量位置/code/detail。即使两边同计数、或两边被同样改错，未改变的 checkpoint 摘要也不能放行。
- 正常样本只验证原算法的历史筛选/保留关系：前六次可含当前样本，后续阈值、非减 list_len、按桶比较后的3天清理和旧异常过滤不变。保留的 band 可继续引用已退出当前历史字典的旧样本；未来样本或非实际筛选/保留成员一律拒绝完成，不改 raw、不用降资格掩盖错误关系。
- `inputs/scan/values` 共用返回/结束前的内容、输出状态及输入/参考资格尾检；失败路径关闭连接。

本次是同格式校验修复。原健康11f候选已在原DB/root通过加强后的公开Reader，16表typed摘要与原回执相同，run/snapshot/dataset_id不变，无需重产。已封存错误依赖的候选必须拒绝消费；修复其结果需从原固定M2另产Resource候选，不能改旧行/旧身份。M2/raw不重算；新生产自然记录新的完整代码摘要。`compute.py`和M2实现没有改动。
