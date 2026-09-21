# Detection M3 有序缺口与公共资格接口

状态：原M3 v2修复48f11de已进入指定集成98b29b6；湖单写7cb8301独立复核REPAIR；本次v2定向修复为候选，待增量独立审查，未合入集成、未执行真实D/P/H。基线为52f0a1fe74e7cc1ffbea5a05a83751145d775056，科学语义沿用已接受的d4c479a2合同。此文只约定Detection，不更改国家/Feature/Q2。

## 输入与科学边界

`scripts/pipeline/detection-frozen-run.py`请求显式指定`input_profile="observation"`才进入M3；省略继续旧complete路径。M3唯一输入为`ordered(reader)`，完整MessageBoundary在该消息Element前处理，保留全部source_message、来源质量、原Element字段与实际SourceEnd。拒绝载荷不重新解释，不猜NLRI、origin或伪造STATE/W。基线seed也消费边界；后续只能UPDATE，不能用snapshot重置。

合法Element继续原adapt_element→DetectionProjection→六类。LOCAL双向消费保持，peer_asn折叠legacy_vp；Gap范围包含未见prefix/ASN、所有族/路径槽位及unmapped可能重叠。资格层不清seen、改起止、改分类或制造revision。Leak的prefix_path_dict是前缀级长期抑制，不由一次路径观察恢复。

科学raw与资格分离。当前有限实现对受限消息之后无法排除的聚合依赖保守Unknown，不尝试自动恢复分母/峰值、anchor、候选/生命周期或事件缺席。`qualify_vp_state`只允许按可信legacy_vp说明精确VP依赖不相交，不能用其给origin、事件或国家聚合放行。无Gap的complete只指固定冷启动和缺口依赖满足，原D前史、Session、分类歧义、参考历史适用性Unknown均不升级。

## 固定身份与表

正式M3输出identity包含`output_profile=detection-m3/v2`、`store_schema_version=detection-typed-m3/v2`、完整InputBinding及binding_id、实际selected_sources、11份reference_checkpoints/解释版本/原参考来源、算法/解码/角色/分类/Gap/资格版本及冻结代码身份。MRT的rank来自完整manifest；所选子集不重编号。reference不混入MRT rank。生产末尾复验M2 selection和参考checkpoint，并按原逻辑验证冻结文件及模块来源；没有声明完整loaded-module身份返回值首尾逐项全等；observation_sealed不等于Detection complete。

保留原records、state_entries字段/值，M3另有`m3_entries`，列为：

| 列 | 含义 |
| --- | --- |
| ordinal / entry_id | 连续原序号／内容绑定身份；entry_id以资格规则v2为域，覆盖component run、ordinal、kind、incident_id、revision、source_id、gap_id和完整payload |
| kind | scope_gap、event_qualification、source_coverage |
| incident_id / revision | 仅已有科学revision引用；无事件不捏造 |
| source_id / gap_id | 所选来源；scope_gap的原Gap自然ID |
| payload_json | 完整原JSON文本；含component/run、InputBinding、资格规则及各类payload |

所有payload另保存target_ref（component/run/source/incident/revision/gap），逐项等于typed目标列。scope_gap保存共享Gap及私有依赖范围。event_qualification保存独立qualification身份（entry_id）、原incident/revision、六个dimension_coverage、Gap引用、有效position和window。Gap前已有资格不被改写；同一revision可追加Gap后资格，后续修订继承未解决历史维度。已结束修订不被之后Gap追溯改写。source_coverage在每个源处理和尾部后保存，含原有序End/parse_counts、窗口与所有继承Gap；零元素、无事件、无raw变化也存在。

六个维度为vp_origin_current、denominator_peak、origin_anchor、prefix_leak_suppression、candidate_lifecycle、event_absence。本片对受影响聚合各维度保守Unknown，不声明已实现独立自动恢复器。state_entries另保存m3资格状态，不替代原模块和projection状态。

emit、finish与完整公共读取共用qualification_contract.validate_entry：校验完整列集合、全部语义列与payload的内容身份、target_ref逐项一致、kind必填/不适用目标、所选source，以及Gap原ID/source/binding和source_coverage原回执绑定。finish对PG与湖两侧逐行校验并比较有序内容身份摘要，再核连续序号、源覆盖有序枚举、每源Gap/End计数、Gap引用、全部科学revision外键覆盖及PG/湖计数核对；ready与PG identity固定qualification_counts。缺覆盖或错误引用不能完成。元数据表与湖不是跨库原子事务；完成门禁和只读首尾资格复验仍必要。

## 公共读取

原`store.read_stored_rows`新增白名单`m3_entries`，仅受支持M3 v2 profile可读该表；旧records/state_entries及旧语义Reader输出不变。原typed/v1制品无需新表，不重导。34b0f85e的弱M3 v1候选不作为v2兼容输入，不能仅改标签冒充；新Reader明确拒绝。旧Q2只接受detection-typed/v1，不能把M3未经新资格接线自动发布。

公共函数位于`data_pipeline.detection.qualified_reader`：

- `read_binding(dsn, run_id, snapshot)`：只查询Detection PG元数据，要求固定complete、正式冻结及M3 v2版本白名单，返回identity/scope及原run/snapshot。这不是全表校验或上游当前仍准入的证明。下游取得input_binding_id后仍必须显式固定，不能取latest。
- `read_coverage(..., expected_binding_id, **limits)`：流式输出全部m3_entries原typed列/JSON；验证连续序号、内容身份、Gap/revision引用、全部源/事件覆盖和末尾绑定。无事件也必须完全消费此流。
- `read_qualified_revisions(..., expected_binding_id, at_position, **limits)`：四元position为(rank,record,phase,ordinal)，不得超出实际最后消费位置，返回raw、main、qualification及as_of_position；main在资格Unknown时None，raw完整保留。查询位置之前尚未生成的revision不返回；已生成revision按该时点独立资格选择，不倒改Gap前样本。
- `read_qualified_records(..., expected_binding_id, at_position, **limits)`：保留全部原记录；能证明原触发位置、查询时点及缺口依赖满足的非事件记录才有main；无法定位/固定事件修订的main为None（事件用上项）。适用于包括False的原rule_decision、country_reduction等。raw导出不是主值许可。
- `read_qualified_state_entries(..., expected_binding_id, **limits)`：实际保存终态的raw/主值与保守聚合资格，只表示identity内qualification_as_of_position；不把终态冒充历史前态。存在Gap时聚合main为None。原科学值仍在raw。
- `qualify_vp_state(gap_payloads, legacy_vp, at_position)`：仅精确legacy_vp依赖的范围相交辅助，不是公共数据Reader或新的Gap解释器。

limits沿用batch_rows、max_row_bytes、guard；新查询不接受synthetic作为正式资格。validate_identity在写入初始化/finish以及实际Reader开始/末尾共用白名单：profile、store schema、qualification/Gap/ordered/解码/方向/角色/分类/投影/算法及输入/参考解释版本必须受支持，缺失或错误拒绝。完整耗尽且末尾无异常才得到Detection输出读取保证，提前退出必须close或contextlib.closing。查询保留资格/事件键索引，内存随这些数量增长；不全量reconstruct_state。Arrow批读取后的字节限额不是分配前RSS硬限制。Detection公共Reader只复核Detection输出，不连接M2/ref核当前准入；生产末尾的M2/ref核验不等于永久准入。P/国家/发布者必须另核绑定的原M2/ref当前资格并完全消费对应输出Reader，不能把read_binding元数据当成全表或上游证明。此次不更改Publication。

国家C1/C2应绑定此固定profile、原incident/revision与对应有效区间的独立coverage，携带原reference版本；不因原值存在推断cohort/轨迹完整。没有事件也消费source_coverage。实际国家/CountryRevision接线和发布profile策略由其owner后续实现，本片不修改国家代码。

## 验证和资源

人工MRT＋11参考经真实M2封存，再运行正式冻结Detection；无Gap六类详情/规则记录/模块与projection状态及revision逐字段对原算法，保留既有跨hash MOAS成员无序对照边界。另有末尾LOCAL受限帧→无元素→EOF，验证raw保留及独立coverage，新进程全表/引用读取。手算覆盖LOCAL A导致MOAS 1→2、LOCAL W导致2→1；Gap本身不改变科学状态或造revision，未知端点/同ASN/不相交VP、后续revision与Gap前位置分别检查。实际PG撤回、错版、错绑定和漏coverage阻止完成/完整读取；早停关闭链路。

另读取本任务原有typed/v1数据库和root（run b823d88bcb0c40f9a48794c30dd6aeac、snapshot 5），不重导：130 records/172 state_entries与原ready一致，旧Reader仍可用，M3入口拒绝冒版。

遥测记录参考加载wall/行数、PID及进程启动至参考加载结束累计ru_maxrss、seed及UPDATE wall、finish前量/本地输出字节、子进程峰值和总wall；PG/WAL不在子进程RSS和本地输出字节内，明确不作整机容量结论。保留RSS/磁盘/行保护，不设处理总deadline；本次仅人工规模，不是实际生产承载验收。

34b0f85e原候选的历史作者验证：受影响集合14 passed／0 skipped，之后1项是定点重复复验，不累计为15项。独立审查仍发现上述三项REPAIR。固定人工输出131 records、178 state_entries、32条资格（1 Gap／2来源／16个科学revision覆盖）。原DB回读及PG停止回执在Git外验证目录。本片等待独立复核，未合入统一集成。

## 本次修复与重消费影响

独立报告SHA256 6ae44cb9b26627d900d822a883fa2d7f27517728436478edd82c08c312c2d5a2指出资格typed目标未入摘要、错误schema被接受及累计RSS误标阶段峰值。本修复仅关闭这三项及相邻完整性/兼容检查。

- M2封存及11参考不改、不重产；是否仍可使用必须重新核当前准入。
- 新M3资格采用detection-gap-qualification/v2、输出profile/schema v2，必须新建Detection输出运行，不能就地改旧v1资格ID/标签。旧候选、审查和失败证据保留。
- 科学算法、原record/revision身份规则和旧制品不改。下游须重消费新M3绑定及资格；原typed/v1健康制品和既有Q2边界不变。
- reference_stage的process_peak_rss_bytes明确为进程启动至参考结束累计峰值，附PID和RUSAGE_SELF作用域，不是独立参考阶段峰值，不以峰值差相减。原wall/来源/行数和输出字节度量保留。
- 对本任务新人工制品的实际上游撤回探针仍可读取已完成Detection；这是上述责任边界证据，不声称新增上游自动失效或跨库事务。

本修复作者验证：最终受影响集合14 passed／0 skipped；修复前/后各以本任务新人工制品执行实际PG+湖新snapshot探针，ordinal18在总集合/计数及旧payload/entry_id不变时改指已有revision、source_id、gap_id，修复前接受32行、修复后拒绝。独立脚本不计入pytest用例数。原typed/v1原DB/root未重导，全typed/旧Reader/状态重建摘要一致；最终冻结代码files与待提交代码一致，自有PG停止并确认no server。等待父安排增量独立复核。


## 湖单写第一片（7cb8301历史候选，已拒绝，公共P1未接入）

以下记录7cb8301原实现，不再作为当前合格输入。原显式输出profile为`detection-m3-lake/v1`，store schema为`detection-typed-m3-lake/v1`，资格语义仍为`detection-gap-qualification/v2`。冻结请求在`input_profile=observation`时显式填写`output_profile`选择新布局；省略仍走原M3 v2双写。不支持的输出profile拒绝。旧typed/v1、M3 v2与旧Q2不改签、不删除，弱M3 v1仍拒绝。旧Q2不接受这个新profile。

`DetectionStore`按固定profile选择正文镜像策略，`M3Store`沿用同一选择：新运行的`records`、`state_entries`、`m3_entries`完整正文只批写DuckLake/Parquet，不创建或写入三张PG正文表；`DATA_INLINING_ROW_LIMIT=0`保留。PG仍有DuckLake目录与`detection.runs`的运行身份、scope、快照、完成/失败状态、有限完整性报告。已有PG正文副本原样保留，不清理。此片没有公共P1可信登记，没有新增活动异常、恢复游标或复杂恢复状态表。终态是历史完整快照，不冒充已实现的恢复入口。

原`emit`的角色/分类和完整JSON、`save_state`的容器/键/顺序、科学算法、事件/revision身份、资格Unknown/Gap与原来源均保留。新模块进入原`identity_builder`的Detection源码组，正式新计算固定新代码身份，不冒充原生产执行。

新finish顺序为：flush三表→关闭Writer→固定湖snapshot→实际复核原M2选择和参考checkpoint→独立只读连接执行`lake_integrity.validate_snapshot`→再次核上游→记录完整性报告→原ready/PG complete完成路径。全表正文验收仅扫描三张固定表各一次；旧双写finish保留原PG/湖对账。失败由原runner标failed，无成功输出。普通进程异常处理不构成跨库原子事务或断电恢复保证。

验收比较生产端完整typed行流摘要与独立固定湖读取：列名/类型、全部字段、原JSON文本、行数、序号、每个科学revision的完整详情与索引一致性、终态容器头/条目/键结构、资格内容身份及科学/Gap外键、全部来源与revision覆盖。TIMESTAMPTZ按同一UTC时刻编码；其余原字符串不重新解释后写回。来源start/end与实际InputBinding计数/内容身份/角色核对；资格End/parse_counts与实际来源核对；参考角色与实际已选metadata及checkpoint逐项一致，不另硬编码来源数量。原ReferenceView自身的有限角色合同不变。生产摘要不能单独证明科学算法正确，本片人工对照另验证原值和状态一致。

`storage_integrity.state=verified`只表示这次生产完整性检查；`public_admission=not_performed`明确尚未获得公共P1 Admission。公开旧Reader仍从固定湖读取并核运行首尾状态；不会自动全量复核历史正文或当前M2/ref准入。新完整验收函数保留为后续admit复用点，当前没有实现admit/current/lock/Receipt，也不向Publication或Country自动接线。

健康旧湖可继续按原入口消费；此片不执行重新准入、转存或更改物理绑定。之后P1须等待已接受的M2/reference实现及实际Admission、首个Feature试点，再另行固定实现，不能给旧ready文件补一个可信标签。

人工验证比较一组含六类/完整详情/Gap的旧双写与新单写，以及一个零事件但有来源Coverage的样本。每次新运行按实际完整typed序列验收；不同冻结进程的旧set枚举次序可能不同，跨运行仅对明确set容器成员和既有MOAS成对成员及其派生last_records摘要作语义对照（先逐运行核该摘要确实等于原正文摘要），不修改保存序号/原值、不全局删除identity。资格只逐项映射本次component_run与其target_ref，再按原规则重算预期entry_id比较。新鲜只读进程复读三表摘要；同计数正文篡改、重算资格ID后的坏科学FK、终态容器损坏均须finish失败且无可用结果。

测量分开记录生产/完整验收/读取wall、三表typed行数及规范UTF8字节、新运行PG正文INSERT行数为0、PG目录与运行登记关系字节、Parquet文件字节、输出与staging/temp采样峰值，以及本任务专有PG的WAL位置差。规范typed字节不是SQL协议流量，文件占用不是累计设备写入，WAL包含目录/后台成本；PG总写入不为0。RSS附PID与累计作用域，不表示独立阶段峰值；人工结果不外推全天吞吐。

最终作者验证：受影响集合13 passed／0 skipped；重复复验不累计。原typed/v1原DB/root全typed流、旧Reader及重建状态的旧/新代码摘要一致；原M3 v2原固定快照可回读，未重导。原Q2 Token不在本任务，协调者安排原Q2 owner使用固定提交在其自己的原DB补验，本片不提前声称通过。完整验收保留状态键、事件与Gap索引，内存随其规模增长，RSS/磁盘保护仍生效；尚无全天承载结论。


## 湖单写v2定向修复（候选，等待增量独立审查）

7cb8301独立报告SHA256 `463b255f3effb02c72fab437ee21a49fe648fc7fe389a3caab7743f4e4f3269d`发现两个Writer同错可签complete/ready：output族整体改名但数量未变，以及attacker索引错误但原有向legacy不变。原13项通过没有覆盖这两个缺口；历史报告与坏制品保留。

当前只接受`detection-m3-lake/v2` / `detection-typed-m3-lake/v2`，完整验收为`detection-lake-integrity/v2`；资格内容规则仍是`detection-gap-qualification/v2`。新Reader的既有profile/schema白名单拒绝所有lake/v1，包括7cb签出的正常候选和坏候选；不自动改标签或补一个完整性报告。原typed/v1和原M3 v2路径保持，弱M3 v1仍拒绝。没有新公共P1、trusted登记或恢复框架。

`lake_contract.STATE_HEADERS`独立于Writer实例、state_attributes和写入流，按四个现有模块、projection、output、run、m3固定全部必需族/属性/容器及头顺序。缺项、多项、改名、错容器拒绝；空字典也必须有头。output.last_records和output.revisions必须是dict，键集合分别恰等于完整科学incident集合；值分别核最后原正文摘要和连续最终revision，revision必须为int，不能用bool代替。没有对普通list或有向角色排序。

科学记录的全部非JSON索引（除单独校验的连续sequence）由原attributes/legacy/evidence独立复算，包含kind、incident/revision、event_kind、subject、observed_at，以及每个角色/分类值与规则字段。验收直接引用固定roles/classification原函数，不引用store中可被Writer故障影响的函数别名或已写出的错误索引。仅为原分类规则的成员判断还原既有codec在指定rule_decision入参中的显式$set；不修改保存的JSON、科学算法、原角色方向或Unknown。

自有人工M2/ref上重放两种原Writer故障：62行output改名、4行非空attacker索引改变；修复前均complete/ready，修复后failed、无ready。另有同count将单个last_records条目移至已有其他字典的探针，必需头仍在，最终键集合闭合检查拒绝。正常六类/详情/Gap、零事件且非空来源Coverage的正式旧/新策略对照及新解释器回读通过；跨冻结进程仅声明既有set/MOAS有限语义相等，不声明逐字相同。原五种损坏路径和新增单键缺失均纳入同一个小人工回归。

本修复不实现就地重新验证登记或转存。所有lake/v1候选保留；要取得v2合格输出，使用仍健康且当前检查通过的原M2/ref，正式冻结新代码后重产Detection的records/state_entries/m3_entries和运行完成证据，生成新run/snapshot。无需重解析MRT或重产健康M2/ref；坏Detection正文不能直接复用为正确结果。逐run处置清单在本次Git外交付报告，未连接审查者数据库。

本次最终受影响集合11 passed／0 skipped；Writer探针属于其中的helper与独立脚本，不另累加为pytest数量。旧typed/v1、原M3 v2原DB/root完整typed、旧Reader和重建摘要与原证据一致。Q2的79项原Token补验是9ebf针对7cb的已接受证据，不是本任务重跑；本修复未改变store/qualified_reader的旧路径或Q2代码，仅替换湖单写版本白名单。当前complete仍不是公共Admission或发布许可。

## P1 公共四接口（已实现候选，待独立接受）

本节接续以上历史切片。`detection.publication` 新增 `admit(runtime, owner_binding, *, guard)`、`verify_current(runtime, admission, *, guard)`、`hold_lock(runtime, admission, lock_target, *, guard)`、`open_reader(runtime, admission, request, *, guard)`；只允许明确人工 fixture。没有接入 Web、Publication 或 Country，没有发布许可。原 typed/v1、原 M3/v2 的旧 API 仍可独立使用。

`inspect_binding` 返回原 `{run_id,snapshot,identity,scope}`。新准入接受健康 lake/v2 和原 M3/v2；旧 typed/v1 不升级为 M3，lake/v1 拒绝。原 producer identity 留在完整绑定中，新 validator 使用实际源码与运行依赖摘要及固定提交身份；健康旧制品重新准入不重产三表、不重解析 MRT。原 run scope 全值保留，不等同后续 D 结果窗，不过滤窗口之前开始的事件；本片没有实现新的计算窗、结果窗或 carry-in 规则。

Runtime 明确输出 DSN、原 output_root、允许根、scratch_root、上游真实 Admission 和逐 ID 的实际 M2/reference Runtime，禁止从 Admission 推导连接权限。内存、临时空间、进程 RSS、锁等待预算均为正有限整数，在构造和使用时重新检查。没有总处理时限。RSS 使用当前进程启动以来的累计峰值，不能解释为独立阶段峰值，也不包含 PG 服务。

首次准入真实检查固定目录/完整 schema、所有实体 SHA 与 stat、原 ready、真实 M2/reference 当前准入；三表各完整扫描一次，复用 lake_integrity 的独立科学索引、所有状态族/键、资格内容身份及科学/Gap FK、来源顺序与完整覆盖。lake/v2 同时对照原生产完整枚举；旧 M3/v2 没有该新枚举，因此重新计算全量 typed inventory 并执行相同语义检查，不伪造历史生产摘要。所有原 JSON 字符串、普通列表和有向角色保留。PG 只新增 `detection.publication_admissions`，保存完整 Admission 与有限验收报告，不写三表正文或清理旧镜像。

同绑定有效登记复用原 UUID key；首次并发收口在本 Detection 登记事务内串行，保留历史记录。current 每次重新核 trusted 完整登记、原 run、真实 PG 目录、固定表定义、实体 stat、新 validator 与实际上游 current；不重新哈希正文或扫描三表。Admission 含两个本 owner stage30 目标：`detection.run` 和 `detection.admission`。`hold_lock` 每次只取得传入真实行的 `FOR SHARE`，锁后比对原值，不嵌套取得上游锁、不尾 audit；组合调用者自行收集依赖 stage10 并全局排序。

请求为 P0 的 `{view,scope_typed,codec_version,batch_rows,batch_bytes}`。codec 为 `detection-publication-typed/v1`，显式标记标量、字典、列表、bytes、带时区时间；TIMESTAMPTZ 规范到同一 UTC 时刻。view 有限为 `records`、`state_entries`、`m3_entries`、`revisions`、`decisions`、`qualified_revisions`。scope 固定 `{start,stop,key,at_position}`：start 包含、stop 不包含，stop 可 null；state 的 key 是 family，其余是 incident_id，可 null。原序号不重编。只有 qualified_revisions 接受必填四元 at_position，其余必须 null。资格修订在固定快照选择该位置前最后资格，逐条返回原 raw、资格及 main，Unknown 对应 main=null；不据全局标签改写各维度资格。原资格全表仍提供来源 Coverage 和 Gap，包括零事件来源。

RowBatch 的 UTF8 编码字节和行数在加入批次前检查；大于整批预算的单行拒绝。开始 current 后只查询请求视图所需表，不调用旧 Reader 的每页完整 Coverage 扫描。只有完全耗尽、Arrow/DuckDB/临时资源真实关闭成功并通过末尾 current 才产生完整 ReadReceipt；早停、读取错误、关闭失败、末尾撤销均无回执，清理失败保留主异常。coverage_ref 绑定请求、原 scope、资格表/计数及位置，不把无业务记录推断为业务成功或全国无异常。

### P1 独立复核两项修复（候选，待同一增量复核）

b80bb51 独立复核为 REPAIR：单目标锁正常退出多执行 commit，以及未来目录结束时间影响固定旧快照 current。修复后 hold_lock 使用可执行 FOR SHARE 的普通事务，退出仅 rollback/close，保留主异常及 cleanup_errors，不改共享 Runtime。

目录身份按固定 snapshot 选择实际可见 schema、三张表、column、data_file，以及引用可见文件的 delete_file；先执行 begin/end 可见性判断，再将仍晚于固定 snapshot 的 end_snapshot 规范为 null。结束时间等于或早于该 snapshot 仍会移除该行并使 current 拒绝，不能无条件忽略结束字段。mapping 仅检查可见文件引用，不把未来文件 mapping 当作当前内容；inlining 按固定 snapshot 的 schema_version 判断并拒绝可见声明。当前文件元数据、类型、映射或内联正文变化的拒绝保留。

本增量只重新准入和复读原健康人工制品，不重产三表或解析 MRT；原锚和原验收保留。未来 end_snapshot 是原独立报告的真实 PG 实证，未来 mapping 原为静态风险；本增量另行对未来文件带 mapping 引用做目录探针，不冒充已生产未来科学数据。没有追加窗口/carry-in 实现，也不重新跑整套生产矩阵。

## real-candidate Runtime 增量候选（待独立接受）

本增量从已接受的共享 PG Runtime 兼容修复继续，尚未合入计算窗/结果窗候选。沿用 `inspect_binding / admit / verify_current / hold_lock / open_reader`，不生产或改写原三表、科学身份、检测算法与活动事件链。

`fixture_only=True` 保持人工默认预算；真实模式须显式传 `execution_profile='real-candidate/v1'`、非空 DSN、允许根、既有输出目录、独立既有 scratch、`expected_detection_binding`（完整原 `run_id/snapshot/identity/scope`）、同模式真实 M2 及全部参考 Admission/Runtime，以及正有限整数 `memory_bytes/max_temp_bytes/max_rss_bytes/lock_timeout_ms/min_free_bytes`。完整绑定包含 identity 内任何已有窗口字段，不能删改后重新解释；真实模式是受限调用方式，不能将人工数据描述为真实观察。

使用时重验模式、DSN/允许根/输出/scratch/完整绑定范围，scratch 规范实际路径、目录 inode 和输入/制品目录隔离；保留进程 RSS、临时盘、最低空闲盘及锁等待保护，不设批量处理总时长或阶段截止。验证规则包含执行模式，跨模式 Admission 与依赖拒绝；资源失败不得签发完成 Receipt。`resource_usage` 记录本进程启动以来累计峰值 RSS（不含 PG）、实际最低空闲盘与检查次数，不表示独立阶段内存峰值。窗口组合的实际接口适配与验证仍待另片收口。

## 计算窗、结果窗与原生命周期选择（已实现候选，待独立接受）

前述 e42c9c5 P1 修复已由父任务确认独立接受；本节是其后的独立切片，不代表真实联合 profile、Country 或组合 P 已实现。

冻结入口顶层可传 result_window，字段恰为 window_start/window_end_exclusive，显式值使用规范 UTC 秒级字符串。未传时等于原 scope.window_start/window_end 的计算窗；scope 保留原值，不添加结果窗字段，不改变科学记录或事件身份算法。结果窗必须是计算窗内非空半开区间，计算窗必须包含于实际 M2 plan.manifest 原包络，不替换 M2 身份。旧制品、旧 scope/identity 不补字段或改签，原全表入口继续可用；新结果视图需要新的结果窗生产证明，旧制品不被静默升级。

新 identity 增加 result_window、result_window_rule（detection-result-window/v1）和 window_coverage。生产在共用 MessageBoundary 按真实 RawTime 核查所有 UPDATE 源消息，包括 LOCAL、STATE、EOR/无元素和 Gap；epoch/ET 微秒未知或时间越界即失败，不仅检查进入算法的元素。baseline 完整消费原角色，标为 complete_selected_baseline_cutover_assumed，不逐条要求其 Header 时点早于计算起点；更早历史始终 Unknown。所选源必须按原 global rank 保持一个 baseline 后接 UPDATE，额外 snapshot/RIB 不读作重置。

window_coverage 包含 update_messages/first_raw_time/last_raw_time、result_messages/result_first_raw_time/result_last_raw_time、逐源 source_result_messages，以及带原 upstream_rank/role 的 selected_source_ranks。first/last 是处理顺序首末，不是最小/最大时刻。首次 P1 准入从完整原 source_message 重算并对照这些数值，同时核 scope、原 M2 包络及真实依赖；结果窗不是仅配置标签。整个计算窗的 records、revisions、seen/抑制状态、活动状态和 qualification 全部保留。

### 下游实际调用与返回结构

沿用 detection.publication 的 inspect_binding(runtime,run_id,snapshot) → admit(runtime,binding,guard=...) → open_reader(runtime,admission,request,guard=...)。Runtime 明确绑定实际 M2/ref Admission 与各自 Runtime。新增 view 为 result_revisions、result_coverage；codec 仍为 detection-publication-typed/v1。request 仍为 view/scope_typed/codec_version/batch_rows/batch_bytes。

完整 D 选择的 scope 使用 start=0、stop=null、key=null、at_position=null。result_revisions 的 key 可指定 incident_id，start/stop 仍指原 records sequence；需要整条前史时不得裁掉它们。result_coverage 必须完整 scope。结果窗由 Admission 原绑定固定，读请求不可临时覆盖或拿过去位置配当前终态。

result_revisions 每行返回：

- raw：完整原 records 行；被选 incident 的计算窗内全部修订，包括 D 前修订，按原 sequence 返回。
- main：同 raw 或 null；原修订资格不完整或结果关系未知时为 null。
- qualification：原独立资格 payload，加 qualification_id，各维度值不改。
- lifecycle：first_sequence、last_sequence、revisions、final_state_ordinal、final_qualification_id、original_start、original_end、start_utc、end_utc、active_in_final_saved_state、selection、result_window、calculation_window、anchor_coverage、end_coverage、earlier_history、interpretation。
- as_of_position：原最终四元处理位置，资格及终态适用位置明确。

selection 为 carry_in、started_in_result 或 possible_unknown。选择读取完整修订首末端点、最后资格和实际保存的模块活动条目；仍活动需要最后完整记录与活动状态成员实际相等，不能只看 e_time=null。已知生命周期与结果窗相交则选中；结果窗开始仍活动但窗内无新 revision 也保留。已知结束恰等于或早于结果起点则不纳入。更早锚、Gap 或缺结束链导致不能确定时保留 possible_unknown，不用 event_start 筛掉，也不把结果起点写成新事件开始。原 legacy 时间和值保留，仅区间判断按原北京时间语义转换；普通列表及有向角色不重写。

泄漏原算法没有可信结束链，不能凭“不在终态”断言已结束或凭 e_time=null 断言仍活动，结果关系保持 Unknown。这是观察范围内原启发式候选链，不是物理网络事件起止。final_state_ordinal 可在原 state_entries 核对；原记录和资格全表仍可读。

result_coverage 每个真实 source_coverage 返回 raw/windows/window_coverage/earlier_history/business_absence；windows 含 calculation_window/result_window，raw 是原资格行。零事件不造 incident，仍有真实来源 Coverage；缺历史或缺观察不因零事件升级为业务无异常。Country 应消费实际选择行、原生命周期和独立资格，不能只靠配置判断 carry-in。

RowBatch/Receipt 生命周期不变：耗尽、资源成功关闭、末尾 current 成功后才生成 Receipt。新 coverage_ref 额外绑定 result_window、window_coverage 和 selection_rule。

人工 A/B 使用一个必要新造的跨日 M2，按原 ranks 0/2 跳过 snapshot 1；验证跨窗结束、无新修订持续、窗内新发生、窗前已结束。Gap/零事件复用已有健康 M2。跨冻结进程仅按既有 MOAS 成对成员规则做有限语义对照，不声称全部字节相同；每份实际制品三表与原 Reader 逐值核对。不重跑旧六类算法/P1 大矩阵，不运行真实 577 源 profile。

窗口候选 ET 修复增量（待独立复核）：完成验收与首次准入从完整保留的 source_message 和原 InputBinding 重建共用 `ordered._boundary`，沿用生产入口相同的可信 RawTime 解释。可信 ET 头下的已隔离载荷失败可保留原 `microsecond=null`，派生时间仍使用获准的头微秒；不补零、不跳过 Gap，也不另设 fallback。只有这类旧失败 Detection 需要新输出；原 M2/参考及健康 Detection 不重产，验证器变化仅作必要新准入。
