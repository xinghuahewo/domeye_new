# Q3-C 历史集合与 JSON 语义迁入设计

2026-09-13。**第1—8节是已接受的固定设计原文；第9—10节保留Q3-C.1候选与四P2修复记录；修复已独立两轴GO并于57e1f074集成、由父任务接受。第11节保留Q3-C.2 H1原候选；独立复核提出两项P2，第12节保留已获父接受和独立双轴GO的最小修复记录；第13节保留Q3-C.3 H2原人工候选，独立双轴各发现一项P2；第14节为两项P2有限修复，已获独立双轴GO和父任务接受；第15节保留Q3-C.4 H5原人工候选，独立Standards一项P2 REPAIR、Spec GO；第16节为构造清理定向修复，待独立增量复核。未执行真实迁入。** 基础为 Q3-B `930675e1fae00063167478463764871949b70835`，已获独立 `df5cf82bef2855fc85633f00c07ee00e54c2069e` Standards GO / Spec GO，并经 `dc931b` 中的《迁移人工集成增量-Q3B-930675e》人工集成确认。本设计不改 Q3-A/B 的已接受事实，也不改变 H1—H6 全部历史范围。

设计形成阶段只读本地方案、当前项目源码和测试中的格式样例，没有读真实业务行、扫描远端、启动 PG、加载旧运行模块或修改生产。0B 的 `/Users/botongwu/.codex/outputs/domeye-history-h1-h6-plan-20260913/运行方案.md` 第1—5节仅作来源线索，不改原件；其中 Q3-A/B 未通过、默认逐行读取等措辞已过时。该方案的数量是旧声明，不能当本次重计。解析/观察 checkpoint 与恢复一致验证由903b负责且优先，本片不接其实现、不扩恢复框架；计划顺序由父任务/8233维护。

## 1. 缺口与拟交付范围

Q3-B 可以冻结独立 SQLite/显式 PG 表并读取 typed 行，附件只保证原字节。它没有 manifest 集合依赖关系、JSON 成员语义或跨文件业务查询。Q3-C 拟增加**有限 profile 的集合冻结、无损 JSON 结构与领域投影**，复用既有历史载体；仅存每个 JSON 文档的 bytes、JSONB 或文本不算完成语义迁入。以下新增 Interface、格式与验收均待批准，不是当前能力。

| 族/格式 profile | 已核对的当前调用与文件形态 | 本片必须覆盖的查询信息 |
| --- | --- | --- |
| H1 `core-index/v1` | [DailyIndex](../../backend/data_pipeline/overview/index.py)读取`core-overview-index/v1/v2`的days/diagnostics；日SQLite含records和provenance。records的item是JSON TEXT，payload是[anomaly-record序列化](../../backend/data_pipeline/common/event_records.py)BLOB，provenance.manifest也是JSON BLOB | 日期、原input_version/解释版本、window/source/data_profile；reference、kind/object/start_time/hour/family/level/severity/search；item/payload完整字段、原引用与来源证据；诊断stage/reasons/count/evidence及门禁 |
| H2 `core-rib-consumption/v1` | [scale](../../backend/data_pipeline/overview/scale.py)、[paths](../../backend/data_pipeline/overview/paths.py)消费小JSON清单/摘要；[origin生产格式](../../backend/data_pipeline/bgp/snapshots/origin.py)含origins.json、peers.json、paths.sqlite | 单观察时点、family、visible_prefixes/rib_entries/visible_origin_ases/unattributed_entries、归属规则与原身份；origins各family的有序ASN成员、Peer原属性、path_key/as4_key原BLOB、已存归属/原因；端点五分类及有界示例 |
| H2 `rib-comparison-jsonl/v1` | [比较格式](../../backend/data_pipeline/bgp/snapshots/path_comparison.py)输出comparisons.jsonl.gz，每行有afi/safi/prefix、left/right_frames以及objects数组；另有frames.sqlite、peer-groups、左右压缩源/MRT、summary/source-manifest | 每个objects occurrence的group/status/左右refs/reasons；frames与refs的零基位置关联，按原Peer属性组和前缀查询。原MRT才有的完整路径不得由summary推造 |
| H5 `general-read-model/v1` | [General读取器](../../backend/services/country_outage_general_read_model.py)读取根manifest/COMPLETE及每事件overview.json.gz、series.json.gz、affected-as.jsonl.gz、path-downstreams.jsonl.gz；当前会整体read_bytes/decompress/json.loads，不能直接复用为有界迁入解析器 | resolution/overview/series、AS分类/搜索/分页、路径关联/样本/分页及audit的所有原字段和身份；见第5节 |

页面依据是 [Core客户端](../../frontend/src/api/coreOverview.ts)、[General客户端](../../frontend/src/api/events.ts)及[General页面](../../frontend/src/components/CountryOutageGeneralPage.vue)。页面只支持有限数值与已知状态，不等于源任意JSON均有同样语义。本片只定义可从新载体执行的离线查询及对账，不修改这些页面、HTTP合同或发布 head。

0B线索：H1为55个日SQLite、4个诊断日，旧声明1,120,555条；H2为15个已有摘要文件及另一清单声明的8个依赖，两者按URI/内容身份区分，不能相加当已取得23个独立文件；H5为81个事件、324个事件文件，旧声明13,488个state points、2,112个AS、12,447个relations、37,205个path samples。它们是未来核验的期望/冲突线索，不是本设计读取或验收所得。

## 2. 完整集合与冻结身份

新增`history-collection-freeze/v1`，与既有`history-freeze/v1`分开分派。调用者提供固定且有序的根清单URI、原版本及预期SHA、允许的本地根目录、profile及其版本、显式外部URI→本地只读文件绑定、独立输出绑定与全部资源预算。不得从环境默认路径、目录猜测或URL自动下载依赖，也不读取清单里的任意绝对路径。真实绑定本设计中均为Unknown，设计中的格式清单不是可执行配置。

### 2.1 依赖闭包规则

- 按profile定义的字段遍历，而不是对任意字符串递归找路径。H1从days/diagnostics以及已存在的scale/origin/path_comparison、composition/补验声明进入；读取日SQLite内provenance清单并登记其records/evidence_refs依赖。已声明的嵌入JSON列也属于解析输入，不能只保留SQLite外壳。
- H2消费包的summary/evidence/source-manifest进入各自命名空间，继续跟随源manifest.files。两个summary同名也不能覆盖。H5从events的四种角色取得全部文件，同时保留根source_event_cohort/metric/as_path/lifecycle的dataset、content/manifest SHA等身份引用。
- 每条依赖边保存`parent_file/node_occurrence, edge_ordinal, original_reference, role, expected_size/hash, relation_kind, resolution_state, target_file_or_identity`。同一物理文件被多个位置引用可共用一个文件实体，但边不能去重；同字节不同文件或不同JSON记录仍是不同 occurrence。
- 区分**声明的文件依赖**和**仅声明身份的上游引用**。前者即使缺失也必须进入闭包失败账，不能改为可选附件；后者没有路径时保留unresolved_external，不能捏造文件或源快照。profile的必需/仅引用角色在冻结前固定，运行时不得降级来取得complete。H5读模型完整不证明S1/S2/S3/lifecycle原件齐全，H1本地完整不证明H3详情关联闭合。
- 缺必需文件、摘要/角色冲突、未知清单版本、依赖环、路径越界、链接替换或清单必需字段重键，均不签发集合COMPLETE。外部MRT若尚未获准绑定，只登记`not_authorized/unresolved`；不访问该源，也不宣称H2完整。只能把已有独立子件交付为其原有有限范围。
- 根/中间清单遍历结束后核对节点/边总数、声明文件集合、实际取得集合和重复边；对profile声明的封闭目录作有界文件名清点，额外未声明候选文件列为冲突，不自动吸收。仅检查获准目录，不重新全盘扫描。H1/H2/H5的“全部”分别由自己的根声明和闭包回执限定。

### 2.2 文件前后身份与封存

1. 先拒绝符号链接/非普通文件/越界路径，以不跟随链接的打开方式取得原实体；记录原URI、解析后的受准路径、device/inode、size、mtime_ns/ctime_ns。大小和时间戳不能代替SHA。
2. 根清单先按预绑定SHA复制并解释；每个依赖按流复制，记录原压缩/原文件SHA及字节数。SQLite复用Q3-B关闭原件、WAL和前后SHA门禁；JSON在已复制件上解释。不给源建账号、改ACL、删WAL或暂停写进程。
3. 每件在复制前后核对实体与SHA；所有依赖完成后，再核验全部源清单/子件身份、源SHA、封闭目录集合和全部封存件SHA。变更/丢失/替换时失败，不能拿前一次成功哈希继续签发。
4. 必须由调用方声明源集合已封闭且不可变。前后核验不等于跨文件原子快照，也不能证明对抗性ABA修改从未发生；若源仍在变化，应等待另行授权的一致封存方式。本片不发明跨文件事务，不把各SQLite/文件冻结标成同一PG快照。
5. 原`COMPLETE`与新集合`COMPLETE`分开：H5当前合同要求原manifest与原COMPLETE**原字节相同**，且需继续验证事件、实际子文件SHA/解码/人口与身份；其他profile按各自合同验证，不能套用同一种标记规则。原complete只是被检查的声明。

新集合身份由新的collection_freeze_id、profile规则版本和完整清单摘要绑定；原publication/revision/cohort/incident/metric/path/read-model/dataset/run/source/interpretation等逐字段保存，不重命名成新业务P。新import_id、DuckLake snapshot与原业务身份分别存储。同一冻结身份不同内容拒绝；同内容的新冻结也不复用旧occurrence身份冒充同一输入。

## 3. JSON无损结构与有限typed合同

拟用固定的`history-json-tokens/v1`，仅作为上述profile的结构保全层，不提供通用文档数据库、用户任意SQL/JSONPath或动态插件。需新增受限流式词法/结构读取器，不能用普通dict或jsonb作为权威原值。选库/自有实现尚未决定；准入标准是第8节边界fixture，不因库声称“streaming”而跳过词法/深度/数值验证。

| 表/字段 | 语义及可逆性 |
| --- | --- |
| `files` | 有序file_ordinal、原URI/角色/profile、原字节/解码字节SHA及长度、实体前后证据、压缩成员数、原完成声明；SHA是内容校验，不作为唯一行身份 |
| `documents` | `(file_ordinal, document_ordinal)`；JSON对象文件为文档0，JSONL为原物理行对应的文档序号；保留行号、换行样式、解码流byte_start/end、节点数。SQLite嵌入JSON另绑定原child Token/表/行/列occurrence，不能失去原SQL storage class |
| `nodes` | 每文档前序node_ordinal、parent_ordinal、child/member_ordinal、kind（object/array/string/number/bool/null）、key的解码值及原字节span、值span/子节点数。对象成员按原顺序排列，数组按原索引排列；重复key有独立member_ordinal，不以key当PK |
| 数字 | 原number词法原样保留；精确表示为sign、coefficient_digits、exponent10、negative_zero。可精确落int64/decimal128的另给原生数值，否则保持有类型的精确十进制结构；受预算的相等/大小比较按符号、有效位和指数实现，不经float、不得按普通文本词典序比较 |
| 字符串/空值 | 原UTF8字节与转义形式由span+原/解码文件保留，解码文本供查询；不Unicode归一化。JSON null有kind=null的节点；缺字段无该节点，投影另有presence标记；与SQL NULL、空串、文字`null`、空数组/对象分开 |

例如`{"a":1,"a":1.0,"b":[null,"null",1e0]}`保留两个a成员以及三项数组顺序；三个数的数学比较可相等但词法与位置不能合并。`-0`保留负零标记；`1.2300e+02`的词法不改写为123。严格JSON拒绝NaN/Infinity、注释、非法UTF8/无法无损解释的孤立代理项；超数字位数/指数/深度/长度不转为字符串成功或截断。原件可留失败证据，但不能得到JSON typed完成资格。

结构层保存所有成员，包括profile暂不认识的非必需附加字段。profile读取的必需字段/依赖/身份路径必须唯一；重键或对象/数组类型不符时，该profile拒绝语义准入，不能照旧json.loads的最后键覆盖规则默默选值。未知schema/profile保留原字节与明确拒绝原因，不凭通用节点可读宣称领域语义通过。

H1的`$anomaly_scalar`只在已匹配的anomaly-record序列化profile解释为decimal/datetime/timedelta_us，原tag对象、词法和位置仍保留；其他JSON里的同名key不自动解释。时间保留原文本/偏移/精度及独立解析值，无时区仅按原绑定数据档规则处理。大于int64/decimal128的JSON数仍能以精确结构查询；若某领域列只支持有界整数，则该领域投影明确拒绝，不能悄悄改用浮点数。

同时保留四种不同摘要：原压缩bytes SHA、解码bytes SHA、源合同的content_sha256及算法版本、新typed结构/表摘要。General的源content hash使用去掉content_sha256后的原规范规则，与文件meta中的解码SHA不是同一件事。旧规范验证器单独固定实现，不能把其可能有损的解码结果用于新typed投影；重键、数字或未知旧规范使原摘要不可可靠复核时记录Unknown/冲突并拒绝对应profile的validated资格，不能修改旧hash或新造旧publication来绕过。

## 4. gzip与资源门禁

profile首版只接受一个gzip成员；成员头/可选头字段也受长度预算，逐块读取和受限解压。必须读至该成员真正结束、校验CRC32和ISIZE，并确认底层压缩文件物理EOF；ISIZE仅是模2^32值，另累计64位实际解码量。截断、坏CRC/尾部长度、第二成员、尾随垃圾（含首版不支持的padding）均拒绝，不能看到第一个eof标记就complete。多成员如确有历史需要另版本适配，不能当单成员静默丢尾。

JSON对象文件只允许一个完整根值加合法空白；JSONL按字节识别LF/CRLF，禁止空白行与一行多值，允许最后一个完整值无末尾换行并保留此事实；每行必须是该profile要求的对象。字符串内转义换行不分行，跨解压块的UTF8、数字、escape及行边界均须正确续读。H5的series.timestamps/tracks必须流式解析和按索引归并，不把整事件文档或整个track保留在Python列表。

新增`CollectionLimits`与既有Limits同时固定。人工最小fixture拟用：64KiB读取/解压块、4MiB元数据清单/JSONL单行、1MiB单token、深度64、数字有效位4096和指数绝对值100000、单文档100万节点/64MiB解码量、1024个文件/4096条依赖边、累计1000万节点、单成员、单文件512MiB压缩及解码上限、总解码2GiB；批仍1000行/4MiB并受当前RSS/磁盘余量门禁。各计数均为显式预算，实际H资源需另绑定，不以这些数字授权真实执行；324文件作为文件集合行处理，不放宽旧附件或max_tables来硬塞。

集合预算不因换文件、child、文档或track而重置；物理文件字节、逻辑文档/节点与profile行分别累计并限制，别名引用次数另计。解压、词法、节点、profile行和写批各阶段在继续读取/分配下一有界块前检查累计量；节点展开量及原值重复存储另计，不能只限制压缩大小。长tracks按index写有界暂存/查询表并按document/track/index归并，不能依赖JSON key出现顺序。暂存磁盘计入预算，失败留失败目录，不自动恢复。继承930的构造/每源启动/next/yield恢复/最终资格前预算不漂移检查。RSS仍是运行保护，不宣称所有底层分配前硬上限；**不设置处理总时限**。

原压缩件与必要的解码流各保留一份，后者让node span可从归档有界定位，不为每次字段查询重新解压整文件。SQLite原件复用child冻结包的唯一原件位置，避免无意再复制一套；输入/解码/typed/暂存/副本的实际字节和峰值分别计量。

## 5. 必须落地的领域投影与查询

所有投影行带原文件/文档/节点或SQLite列occurrence；不得只给一个document_bytes列。“可查询”至少要求以下独立列、类型、过滤和关系可从新载体执行，同时能返回字段缺失/null/原值及来源。新增未知字段仍可从结构层审计，不能靠丢弃来固定schema。

### H1/H2

- `core_days/core_diagnostics/core_records`：保留按日input_version、根/日interpretation分别绑定；SQLite原records的检索列与item/payload/provenance分别可读并相互核对。kind/level/family/hour/search、severity或time排序按当前DailyIndex规则；family=ipv4/ipv6含mixed，unknown排除数另列；趋势按小时distinct prefix对象计数，不能被列表kind/level/q筛选意外重定义。缺失日期/失败日期不执行空成功查询。
- 四隔离日2026-03-03/04/10/20始终绑定自己的诊断原件；stage/reasons/count/evidence原样迁入，不能把03-03原因复制到其余日。overview/trend/events为null、直达详情门禁保留；有证据的available空日才可给0。层级表`day_availability`不允许被集合complete覆盖。
- `scale_family/origin_family/origin_members/peers/origin_paths/path_endpoint_metrics`：字段来源按第1节文件；保留原family交并口径、私用排除/不可归属规则、Peer原属性和AS_SET/AS4原件，不重新跑检测或改归属。03-31 08Z单时点与03-30 16Z→03-31 08Z两个端点分开；coverage/session_continuity仍unknown、interval_change_count仍null。different_fraction只在原same+different非零时有值。
- `comparison_objects/comparison_refs`：将objects位置数组显式映射为group/status/left_refs/right_refs/reasons，保留每项ordinal；不把整objects串当字段完成迁入。refs解析为side/frame_position/entry_index，和left/right_frames、frames.sqlite、peer-groups按原零基规则核对。原完整AS_PATH在MRT时，需对**已冻结且已授权**MRT复用本项目只读定位/解码规则核验，不新建观察生产或checkpoint。未完成该适配只可报告JSON引用可查、完整路径证据Unknown，H2全族不能关闭；仅offset落在文件长度内不算路径引用验证。

### H5

| 投影 | 必需语义字段与查询 |
| --- | --- |
| `general_store/general_events/overview` | 根run_id/dataset_id/implementation_id、窗口/分页/路径证据/因果边界声明；原publication_id/revision/state、incident、legacy_reference、country、event_read_model/cohort/event_metric/event_as_path及所有source dataset/hash/lifecycle身份；window/data_through、event_end/duration、is_final、quality/observation/missing_slot/capabilities/semantic_boundary；cohort各分母、final_values、peaks值及峰时按有序指标行保存 |
| `general_series_points/track_definitions` | event、track原key及ordinal、point_index、timestamp原文/解析时点、精确value/presence、单位/定义；验证每track长度=point_count=timestamps长度，event声明的state_point_count一致。按原point_index读取，不自动去重/排序/补时间槽；额外track保留且可枚举，不能只存页面当前4条 |
| `general_affected_as` | 原行ordinal、rank/asn/name/organization/nature及各state、event_classification、fixed_prefix_count、三类peak计数、path/concurrent_downstream_asn_count；支持原classification、搜索、default/asn_asc排序和最大60项分页 |
| `general_path_relations` | 原relation ordinal、affected/downstream ASN及名称/性质state、observed_path/associated_fixed_prefix/independent_direction/route_observation/concurrent_state_point计数、first/last_concurrent时点、三类peak值及原relationship_semantics；按affected_asn、scope=all/concurrent、搜索分页 |
| `general_path_samples/sample_peer_members` | relation与sample ordinal、prefix/family/as_path_id/as_path_canonical、route_observation_count、independent_peer_asns逐成员及顺序；同路径重复样本保留。样本只是原声明的bounded evidence，不代替完整S3审计制品 |

当前General的default分页保留文件行序，asn_asc是稳定排序；新副本用`(asn, original_row_ordinal)`明确同值次序。relations保持原行序，samples保持数组序。cohort/incident不是唯一性去重键；旧读取器会以dict登记事件/ref，Q3-C不复制覆盖行为，身份冲突须返回完整候选并隔离对应查询。

`event_end_unknown/is_final=false`与event_end/duration null原样保留，不能根据末个series点补结束。当前服务中有常量quality/observation/missing_slot输出；迁入必须区分源实际字段与旧服务推导常量，不能倒灌为源事实。缺源字段时以presence/Unknown表示，原语义profile若要求它则拒绝相应查询准入。

## 6. 与Q3目录/Token的复用及新增Interface

以下是建议合同名，不是已存在的函数。保持`Token`五字段及旧freeze/import/component/scan/bulk/rebuild语义不变；不把JSON伪装成PG/SQLite引擎，不取消private_pg、retained-history或旧版本门禁。

1. `freeze_collection(binding, destination, profiles, limits)`产出上述新集合清单、file/edge inventory、原/解码件、profile映射回执；SQLite子件调用既有freeze_sqlite_source。文件依赖和身份只引用其真实封存位置，不改其旧清单/hash。集合失败不赋予新complete，已单独验证的child仍只代表child范围。
2. `History.import_collection(manifest)`需独立私有目标绑定。SQLite子件复用既有import_package得到原形Token；JSON结构和领域表新增明确的codec/Arrow schema及验证分派（建议`history-json-profile/v1`），复用Q3批写、固定snapshot、typed回读、文件SHA与候选登记。必须扩展合同实现及反例测试，930本身不能读取该新引擎。H5按profile表装行，不每文件建库/表。
3. 新`CollectionToken`固定集合身份、清单SHA、profile规则SHA、目录ready SHA及有序child/typed Token集合；不将它塞进旧Token字段。私有PG只增加窄集合登记，children仍用既有history登记/DuckLake目录。子件全合格、必需闭包及各声明profile状态对账后最后登记集合；按稳定ID顺序持子件资格共享锁至集合登记事务提交，不发明跨PG/文件系统原子恢复。
4. 新`History.collection(token)`返回显式上下文会话，提供`bulk(dataset)`、有限`query(profile, parameters, cursor)`及离线`rebuild()`。bulk复用内部已解码有界流；入口/正常耗尽出口核验固定集合及全部依赖Token/文件/代码，不对每个输出批循环公开scan或重验全集。任一子件资格/文件变更，最终不出完成回执。集合级全档/子集的scope、原available/unknown与profile_admission分别返回。
5. query仅接受本节列出的领域查询，不暴露任意SQL。游标固定CollectionToken、profile、规范查询参数及完整排序键（含occurrence）；版本/筛选变化则拒绝续页。索引只在离线新副本建立；分页查询前/后资格验证及实际扫描/返回预算必需。完整全页对账使用同一只读审计会话及固定快照，不靠逐页重复全文件SHA；独立零散页如仍采用全SHA，应如实计价，不声称已获得低成本线上分页。

集合manifest限制其自身元数据大小；大量file/edge/node/profile行以有界表/块存储，不塞进ready JSON。未来目录仅用全新输出根与import ID，source连接和目标目录绝不混用。此处只定义单次候选/完成的最小合成，失败后不自动追加、重试或恢复，不触及903b工作。

新增集合合同用`integrity_state`表达物理完整性，并用`availability_by_scope`逐日/事件/profile保留可用性；根不能把“有一个available子件”聚合为全域available，也不能因四个隔离日把55个正常日抹成无数据。JSON/诊断结构的通用audit可读不授权business旁路，领域查询仍先执行对应scope的门禁。该映射是新集合合同，不能偷偷扩展旧Token的availability枚举。

集合冻结complete、typed导入complete、某profile查询准入、H族全范围验收、业务发布是五件事。四隔离日、未知源、缺失外部身份、未适配H4/H6不能因第一件成功而消失；不创建新业务publication、不更新P/head。重建只依赖固定载体及其已封存原件，不能临时查旧目录补字段。

## 7. 最小实施片与不缩减的后续范围

| 小片（均须另行实现授权） | 最小交付与关闭条件 | 明确不能声称的结果 |
| --- | --- | --- |
| Q3-C.1 集合/结构基础 | 人工根清单→依赖闭包、单gzip严格解码、JSON/JSONL/嵌入JSON无损节点、固定typed表、缺失/失败账；至少两个不同profile共享同一路径 | 仅结构可查不能称H1/H2/H5领域语义完成 |
| Q3-C.2 H1 | 人工多日SQLite+4隔离日型诊断+provenance/补验依赖，core领域投影、源移除后筛选/趋势/详情/引用全页核对 | H3源表、真实55日及其旧详情未闭合前不能称H1/H3已迁完 |
| Q3-C.3 H2 | summary/origins/peers/SQLite及comparison JSONL两类profile；完整物理依赖、位置数组展开、原分类/原路径引用核验。MRT定位适配单列子项，未通过不能关闭H2 | 两端点不是连续观察；摘要样例不能代替全部comparison/原MRT证据 |
| Q3-C.4 H5 | 人工至少两事件×四文件（含跨页/同值/未知生命周期），全事件/轨道/AS/关系/样本及原身份图全部对账；324件模拟规模只在完整基础后测 | 真实81事件未逐件解码/计数/引用验收之前，不能用样本通过关闭H5 |
| H3延续 | Q3-B显式PG只读源及完整表/前置窗另行绑定；H1→H3、各详情关系全量核验 | 不以已定位37表替代完整实际闭包，也不把仅D/source=r视为全范围 |
| H4后续适配 | CSV需编码/BOM、分隔/引号/嵌入换行、物理span与语义记录、重键/空单元格/首行优先版本；XLSX需ZIP依赖闭包/解压限额、工作簿/表顺序、cell类型、shared strings、公式与缓存值分开、日期体系/空白缺失。保留INFO原publication/archive/manifest | 四原件不能替代另一份24文件清单；历史有效期Unknown，名称/组织/首行规则未适配时对应H4查询不可用 |
| H6持续缺失账 | P0/trend/224–310/story/evidence/country lifecycle/Resource历史状态/MOAS-leak中间件逐项保存已有定位、观察日期、缺失原因、依赖查询影响和待补材料 | 当时404/目录未定位不证明全域不存在；缺失登记完成不等于历史数据迁完；禁止空成功release |

上述四片只拆分Q3-C自身，不与C1/C2等既有计算任务同名，也不改变父计划优先级。真实H执行仍需具体源/输出绑定、资源确认及授权；D终态或本设计接受都不是运行授权。

## 8. 可执行验收矩阵（均待实施）

| 检查组 | 人工构造/执行 | 必须核对的结果 |
| --- | --- | --- |
| 闭包 | 两根共享子件、同名不同URI/内容、重复边、缺子件、环、越界/链接、清单变化、子件复制后变化、额外未声明文件 | 原文件实体与边不丢；失败无集合COMPLETE；每个缺口有定位，不能按SHA合并不同occurrence |
| 原完成标记 | H5 manifest/COMPLETE同/不同原bytes，完整标记但子件丢失/坏SHA/人口错，源仅身份引用无本地文件 | 逐层状态与实际子件一致，明确unresolved_external与必需文件缺失的区别，不能以文件存在判complete |
| JSON词法 | 超2^53整数、超decimal128数、指数/负零、null/缺字段/文字null、重复key、转义等价key、空容器、同内容多行、对象和嵌套数组乱序、tag对象 | 原bytes全SHA；全节点按occurrence/key/value/类型/父子序比较；精确值比较和原词法分别通过；重键身份profile拒绝 |
| gzip/资源 | 每个块边界截断、坏CRC/ISIZE、第二成员、尾垃圾、无末换行、CRLF、跨块UTF8/转义、压缩炸弹、超行/节点/深度/数字/文件数；construct/yield间改预算 | 必须到物理EOF才完成；超限/漂移前置拒绝且关闭文件/解压器/Arrow/PG，无部分完成回执，无处理总时限 |
| H1 | 人工available非空/空日、四隔离诊断、根v3/日v2、混合family、等级冲突、AS_SET、同ref多候选、item/payload/tag/provenance不一致 | 所有筛选/排序/小时趋势与当前代码规则逐项对账；失败日概览/趋势/列表/直达详情均不可旁路；原标识不变 |
| H2 | 两family、原origin交并/私用/未归属、五分类、重复Peer/entry、frame/ref越界或指错、AS_SET/AS4原件、分母0 | JSON投影总量与summary/SQLite/冻结MRT来源全对账；null与non-continuous边界不变；未适配原路径不能给verified |
| H5全图 | 两事件同country/不同incident、同ASN/rank及重复关系、>60项分页、全部轨道/peaks、refs跨publication/revision或event、sample计数错、lifecycle未知 | 每个原字段和所有事件/子件计数全量相等，跨表/跨页不漏不重；冲突完整列matched/missing/ambiguous/unknown，不任取首条 |
| 源移除 | 封存、导入、退出进程；移走自己的人工源根，启动只持新CollectionToken/目标绑定的新进程，禁止原路径连接 | 领域查询、节点定位、解码span原字节、引用解析和私有副本重建均可验证；分页1/17/60及不同筛选的全集与独立期望相等 |
| Token/最终资格 | 原b1/4c/626/930健康人工Token不重导；集合任一child撤销、ready/Parquet/规则代码漂移、早停、子集完成 | 旧接口不退化；漂移无新读取/无错误终端回执；子集不冒充全集合；正常完成绑定所有固定身份 |
| 实际成本 | 同输入17/65/1025条、多文件及324件人工形态，输出批1/17/1000；流式独立对账，不在验收程序list全档 | 分阶段compressed/decoded/raw/node/profile/Parquet/副本字节与行数、open/read/解压块、PG实际语句/事务/批写、Arrow批、SHA次数/字节、wall、当前/峰值RSS及临时磁盘；变输出批不新增全闭包SHA或逐行PG往返 |

完整对账不能只比set或抽样：逐文件原bytes+解码bytes、逐文档有序结构、逐profile完整typed多重集、occurrence序列、全部引用候选、全页拼接与筛选交叉都需独立期望。当前读取器对小型、唯一键、正常精度fixture可作兼容对照；重键/大数等不能用其有损结果作唯一oracle，需手写精确期望和结构/字节重构。旧页面未提供的未知数据不补造来让对照通过。

性能不设虚构吞吐门槛；门槛是预算不失效、成本可归因、正常批化且不随输出批数反复扫全闭包。节点数可能远大于文档/业务行数，宽行或排序仍可能拒绝；实际H的时间、空间、字段变体和可用性目前Unknown。最终分别交源码/人工测试/历史族验收/产品可消费结论，不能跨级宣称成功。

## 9. Q3-C.1 原人工集合结构候选（2026-09-13，独立复核REPAIR）

上文固定设计 `b902a110` 已由308e独立 Standards GO / Spec GO（P1/P2均0），父任务据此明确授权本片。设计接受不等于实现接受。本节描述从该提交建立的 `codex/q3-c1-collection-structure` 候选；测试结果与成本回执见本节末尾。H1—H6全部真实范围仍未迁入，H1/H5领域投影、H2/H4适配和H6补缺均未完成。

### 实际入口与首版有限合同

新增相邻的 [historical_collection](../../backend/data_pipeline/history/event_collection/__init__.py) 深模块，`History`继承原历史载体入口；原`historical_import`六个文件及旧Token五字段不改。新入口为`freeze_collection(Binding(...), destination)`、`History.import_collection(manifest_path)`和`with History.collection(CollectionToken) as session`。不在旧模块注入插件、替换旧引擎或改写原ready/code绑定。

本片只接受`data_kind=fixture`，显式有序`Root(profile, path, origin_uri, source_version, sha256)`、封闭源目录与`external_files`绑定；冻结拒绝目录重叠、符号链接、非普通文件、越界、未知角色/schema、缺必需文件、依赖环、额外未声明文件及原件前后实体/SHA变化。受准目录作有界闭包清点，不能借此扫描其他目录。外部依赖只有明确文件绑定才读取，不访问URL、环境默认位置或旧代码。

| 首批profile | 已实现结构角色 | 尚不可用或拒绝范围 |
| --- | --- | --- |
| `core-index/v1` | 根`core-overview-index/v1/v2`的days/diagnostics；日SQLite完整旧child载体；records.item/payload与provenance.manifest按旧child的表/行occurrence解析；provenance的records JSONL和固定record/source/evidence_refs路径；diagnostic v1/v2 | 根另声明scale/origin/path_comparison/composition时明确拒绝，不能忽略依赖取得完整；完整anomaly-record领域校验、scalar标签解释和首页查询仍属于Q3-C.2 |
| `general-read-model/v1` | store v1、event v1、overview/series/affected-as/path-downstream的四种现行schema；原manifest/COMPLETE同字节，全部事件文件；根/事件/文件行数、series轨道长度、样本总数和原read-model/publication绑定 | 不执行旧业务canonical content hash算法、不解释quality等旧服务常量；保留原摘要节点/身份且不授予`profile validated`或领域查询资格；完整General投影属于Q3-C.4 |

`evidence_refs`显式file/path/uri均是必需文件角色，不能因未绑定而自动变为identity-only。General根现行cohort/metric/as_path的dataset/content/manifest身份及lifecycle snapshot身份字段固定记为`identity_only/unresolved_external`，有无本地绑定不改变其角色。根、子件和scope分别保存；四诊断日逐日保留原节点、stage/reasons/evidence和`validation_failed`，General事件结构可读也不宣称业务available。未知附加字段保留所有节点；实际使用的身份/依赖路径遇到重键则拒绝，普通字段重键保留全部occurrence。

### 固定结构、文件与资源

新typed载体包含files、documents、nodes、edges、identities和availability六张固定schema表。节点有原文档/前序编号、父节点、成员/数组位置、kind、解码key、原key/value span、子节点数以及独立字符串、bool、精确数字列。JSON null有自己的节点；缺字段无节点，结构成员定位返回`present=false`；两者与SQL NULL、文字null和空容器不混合。旧SQLite child继续保存原storage class及完整表，嵌入JSON文档另外绑定原表/行/列，不重新排序来源行。

数字保留原词法、sign/coefficient_digits/exponent10/negative_zero和有界精确比较。便利列包含int64及固定`decimal128(38,18)`：只有精确可表示时填值，其余保留精确十进制结构，不舍入、不转float、不按普通文本排序。该固定scale是新codec的便利投影范围，不限制原精确数的保留；不是每个数字都强制落入decimal128。严格UTF8、孤立代理项、数字位数/指数、深度、单token/行/文档和累计节点预算都在解析路径检查。

gzip采用标准库zlib的有界输出接口，检查CRC/ISIZE、成员EOF和压缩文件物理EOF；第二成员及尾随字节拒绝。JSONL保留物理行、LF/CRLF、无末尾换行、文档位置；原压缩与解码实体分别封存并哈希。原始字节、结构暂存、子件块、解码量与节点展开均单独累计；SQLite候选暂存按实际页数加其他累计写入量检查磁盘上界，RSS为进程运行保护，不宣称分配前硬上限或PG全系统内存峰值。无处理总时长上限。

冻结阶段`structure.sqlite`只是候选暂存，完成导入不将它复制到新载体；主体是DuckLake管理的原生typed Parquet。私有PG仅集合窄登记、DuckLake目录以及显式重建的文档定位索引。原/解码件仍保留，重建不能临时回访原source或freeze目录。失败只留失败账/候选目录，不自动续跑、恢复或追加，不接903b/M2/M3/C4。

### 结构读取与限定重建

`session.bulk(dataset, batch_rows=...)`逐表提供有界provisional批，正常耗尽核对该表完整有序摘要。`session.nodes(document_id, parent=..., key=..., cursor=..., limit=...)`只提供固定文档结构定位，返回全部重复成员occurrence；游标绑定Token、文档、父节点和key，页最大60项。`session.span(document_id, node_ordinal)`只在返回字节预算内读取已封存原span；不开放任意SQL/JSONPath。所有这些都是离线audit，不提供business入口。

集合会话入口与正常退出各做一次全资格校验，固定所有child/Parquet/原件/规则身份；退出持共享资格锁到完成回执生成。批及页仍是provisional，早停或漂移没有完成回执，已耗尽哪些表由`datasets_exhausted`逐项列明，不把子集叫作全集。页本身不重复全闭包SHA；bulk每表固定快照有一次行宽聚合，成本单列，不能称为免扫描查询。`session.rebuild()`只在自己的私有PG创建documents定位索引（document/file/entity/span/node_count），不复制nodes、JSON或领域明细；同一事务末尾再核验源资格。


### 最终验证与实际成本

最终产品源码主矩阵为 **60 passed in 143.47s**，其中包含四种必要成本形态；随后对同版产品补做分页成本，**1 passed, 60 deselected in 1.04s**。合计61个不同测试，不把较早重复运行累计成新覆盖。主日志为本任务Git外根`q3-c1-verified-final.log`，分页日志为`q3-c1-pages-final.log`；各自输出固定人工证据目录。全部输入是自造fixture，未用旧业务行作测试。

验证包括：两个profile同路径；全部有序节点与独立标准库pairs/精确数字词法oracle比较；全部原文件SHA、文档span和依赖引用序列；源实体替换、链接、环、未绑定必需引用、额外文件、原SQLite WAL、身份重键、空值/大数/代理项、gzip所有截断位置/CRC/ISIZE/第二成员/尾字节，以及累计资源/会话预算漂移。原SQLite文件本身的WAL在复制前和最终核验拒绝，不能通过把主文件改名复制来绕过旧门禁。

新进程测试先移走本任务人工source目录及冻结输入包，再以仅目标绑定和固定CollectionToken启动真实新Python进程。其全表有序typed摘要、结构定位和原span回读通过；重建PG表逐行对账，只含documents索引，nodes复制数为0。旧b1/4c/626共六个不同Token均在各自原库重新读取，不重导、不改旧ready及数据；930复用其中的b1 PG Token。原测试执行了七次读取，但不能称为七个不同Token；第10节修复候选已去重证明。旧模块和依赖锁没有变更。

| fixture行 / 事件 | 文档 / 节点 | 冻结 / 导入秒 | 全表读取秒（输出批1 / 17 / 1000） | 每次读取实际PG语句 | 每次读取Python SHA256调用 |
| --- | --- | --- | --- | --- | --- |
| 17 / 2 | 131 / 1755 | 0.426 / 0.595 | 0.509 / 0.282 / 0.269 | 145 | 210 |
| 65 / 2 | 467 / 5835 | 0.976 / 1.187 | 1.480 / 0.735 / 0.692 | 145 | 410 |
| 1025 / 2 | 7187 / 87435 | 14.661 / 18.149 | 21.365 / 9.657 / 8.990 | 145 | 4450 |
| 1 / 81 | 335 / 11455 | 2.272 / 2.782 | 3.519 / 1.781 / 1.464 | 145 | 1434 |

最后一行是81个事件×4文件的324文件人工形态；包含Core、根清单、COMPLETE后整个集合实际333个文件，不能把333误称324。计时包含显式校验及PG `log_statement=all`审计开销，不包括fixture造数和目标数据库创建；是本机人工形态，不能推算真实H吞吐。各形态换输出批后，服务端实际PG语句、Python SHA256调用及字节均保持不变；Arrow批数会变化，逐阶段raw/decoded/typed/Parquet/副本字节、节点膨胀、读块及完整PG日志另见`实际成本.json`。

分页另有实际代价：同一人工文档25个节点，页大小1/17/60分别执行26/2/1页（含必要的末页判空）、333/93/81条PG日志语句。集合显式文件SHA调用均84次；此数**不含**另计的旧child和代码身份SHA，不冒充整个进程所有SHA调用。分页不重扫全闭包，但DuckLake目录查询仍随页数增加；没有声称低成本线上分页。

内存回执同时记录当前Python RSS与该进程生命周期`ru_maxrss`；后运行形态可能沿用前面较高的生命周期峰值，不能称为本阶段独立峰值，也不包含独立PostgreSQL进程。`read_blocks`仅统计集合明确的Python读边界，不冒充SQLite/DuckDB/PG内部OS读次数；PG调用来自本任务独占实例的真实日志。所有指标均明确作用域，未测项不补零。

仅本任务28763私有socket PG被启动，使用新增q3c1/q3cc目标数据库；完成后smart stop，状态复查`no server running`，回执为`q3-c1/pg-stop.txt`。无真实H/D/P、远端、共享服务、HTTP/前端、Issue/push或业务发布。固定提交、精确父、完整差异、每文件SHA、测试/成本/兼容/新进程证据及上述关闭回执由本任务`q3-c1/交付索引.json`统一索引，先交父任务，再按父任务安排独立复核。


## 10. Q3-C.1 四项P2最小修复候选（2026-09-13，待增量独立复核）

固定独立报告为308e的`q3c1-review/独立复核报告.md`，SHA256为`876af4df9b810801cb617854592c87b205eec325e0083984af3a0dddff61bf39`。原候选`c3a93b268d76c5f3b6d1374348e16f99aedbb6d8`的结果为Standards REPAIR（2项P2）、Spec REPAIR（2项P2）；第1—8节设计GO不受影响。本修复从该候选建立`codex/q3-c1-four-p2`，它是唯一父提交。未改原候选或独立失败证据，仍只交付`structure_audit_only`，不是领域语义或真实H迁入验收。

| 固定问题 | 最小修正及实际反例 |
| --- | --- |
| Standards：child累计门禁迟到 | 相邻`children.py`计算集合剩余原始行、原件加块字节及临时目录额度，向旧child冻结/导入传递收紧后的Limits；冻结前只读预检复制SQLite的完整表行数，导入整包验证先按共享额度预检全部有界child清单，再允许读取正文。601+601行、总限1000时，冻结仅首child完成；导入在child正文验证和导入调用前拒绝，不登记集合。201+201行、总限500的正常冻结与导入均实际传500、299。 |
| Standards：当前单行字节上限缺失 | bulk、nodes及经bulk执行的rebuild共用当前会话`row_bytes`精确编码检查，交付前拒绝超限行；64字节额度反例拒绝且无回执，恰好等于行宽的边界通过。 |
| Spec：普通重键被误判身份 | 身份登记与唯一性检查限定在两个profile实际使用的路径；普通`extra.source_note`、`extra.publication_id`、`extra.input_version`均保留两个有序occurrence；既有真实身份/依赖重键拒绝测试继续通过。 |
| Spec：捕获实际错误后仍签complete | 实际SQL/行页资源/span读取/最终资格失败统一永久标记会话失败并关闭连接。单行分别合法而合页超限，以及有效span实际截断后恢复原bytes，均不能继续完成；无效参数和不存在节点仍可纠正后继续正常读取。 |

原始child人口、typed节点及重复验证工作量分开计量：每遍必要整包验证共享一份源人口/字节额度，实际生产也按child消费剩余额度，不把重复资格验证当成新增源行，也不把typed节点混入原始行计数。源原件加块字节另有跨child反例；临时额度不足在启动child前拒绝。child元数据保守预留两份`max_metadata_bytes`，候选SQLite暂存使用页数替换逻辑计量；原生DuckLake写入在有界执行前后检查当前候选目录实际大小，计入已完成child输出。此保护可能保守拒绝，不声称文件系统硬配额、单次原生写入分配前硬上限或跨存储原子性，也不计为PostgreSQL全实例磁盘/内存峰值。没有新增全闭包逐批SHA或处理总时限。

旧`historical_import`六模块、五字段Token、依赖锁和旧ready/code绑定均不改；新增相邻子类仅为本次集合调用传剩余额度及检查当前输出目录。六个真正旧Token在自己的原库读取，未重导；证明为六条，第一条aliases包含b1与930。

验证日志保存在本任务Git外`q3-c1-four-p2/`：修复前`before.log`为9 failed，保留原产品反例。中间一次附加字段oracle误选到day.input_version，修正测试限定extra父节点后通过，失败日志保留且不算产品缺陷。最终源码`verified.log`为18 passed（2.46秒）；`regression-verified.log`为74 passed、5 deselected（6.91秒），包括同18项修复用例及原56项，不能将重复执行相加。另`cost-verified.log`为1 passed（2.12秒），合计75个不同用例；本次不重跑其余三种大成本形态与完整gzip逐截断矩阵，也不将原候选结果当作修复版复测。

正常回归覆盖两个profile、全部有序节点/精确数、真实身份重键、共享依赖、源和冻结包移除后的真实新进程、固定Token、文档索引重建、分页/预算漂移/早停/末尾资格以及六个旧Token。最终成本仅17条Core、两事件人工形态：冻结0.271秒、导入0.730秒；全六表输出批1/17/1000分别0.472/0.275/0.264秒，每次实际私有PG日志语句145、Python SHA256调用214、处理字节1,701,738，三次不随输出批改变。统计含代码/child/载体/行摘要及PG审计开销，不能推断真实H吞吐；当前RSS与生命周期峰值、显式Python读边界沿用第9节的限定。

最终自有PG已smart stop并复查`no server running`，新回执为`q3-c1-four-p2/pg-stop.txt`。固定修复提交、完整差异、逐文件SHA、最终版证据及报告原件SHA在同目录`交付索引.json`；先交父任务，再按其安排由308e增量复核。无真实H/D/P、远端、共享服务、HTTP/前端、Issue/push、M2/M3/C4或业务发布；原候选、旧制品及独立报告保留。


## 11. Q3-C.2 H1人工领域投影与离线查询候选（2026-09-13，待独立复核）

本片按已接受第7节与父任务明确授权，从集成`57e1f0740e2b321e5e3a6f22150434df3b3aff47`建立`codex/q3-c2-h1-profile`，不改变第1—8节设计。新增相邻[historical_core](../../backend/data_pipeline/history/event_index/__init__.py)模块；`historical_collection`七产品文件、`historical_import`六文件及五字段Token、原Core服务／DailyIndex／异常记录序列化器均不改。新H1资格与原`structure_audit_only`分开；没有真实55日、H3全关系、HTTP／前端／发布验收。

### 有限公开合同与资格

- `History.project_core(CollectionToken, root_id=...) -> CoreToken`：只在显式自有离线目标执行；从已完成集合的固定typed结构定位、旧child typed行和封存原／解码件投影。无需重新冻结或重导原集合，不回访其source/freeze目录。一次调用只选一个明确Core根；混合集合中的其他profile仍绑定在完整CollectionToken中并参加入口／末尾资格检查。
- `CoreToken(profile_id, collection, root_id, rule_sha256, snapshot, ready_sha256)`：`collection`保留完整底层CollectionToken及所有旧child Token；新`profile_id`不重命名旧reference、publication或input_version。规则`history-core-domain/v1`绑定本模块、所用现项目Core纯规则、数据档及底层集合规则的文件SHA。只有全投影有序回读、原集合／child／实体末尾复验和私有目录事务完成，才登记`profile_validated`；这仅是人工Core查询一致性资格。
- `with History.core(CoreToken) as session`：`metadata()`返回完整根清单的精确有序视图及原index版本；`query(day, family='all', kind='all', level='all', hour=None, q='', sort='severity', limit=60, cursor=None)`返回选定日的概况、独立小时趋势和记录页；`references(day, reference, limit=60, cursor=None)`返回按原引用的完整候选分页；`detail(day, occurrence)`只读取该日明确occurrence，不能用原引用默取第一行。`bulk(dataset, batch_rows=...)`支持下表五个固定领域表；`rebuild()`显式创建独立私有PG窄查询索引，无业务head。
- 页、详情、索引结果均是`provisional`。入口／正常退出对领域及完整来源做资格复验，末尾持共享资格锁至回执生成；实际SQL、原span、行／页预算或资格失败永久关闭会话且无complete。非法参数、错游标、未留存节点不等于真实读取失败；可纠正参数继续。合法分页未耗尽或bulk早停时无终端回执。
- 游标绑定完整CoreToken、规则、日、所有筛选／排序／页大小参数，以及`severity/start_time/reference/occurrence`四项完整排序键。每个会话按已交付游标连续读，不能跳页取得全集回执；新会话从合法中间游标继续时，回执明确`query_suffix`和起始键，只有从头正常耗尽才记`full_query`。完成操作与所有查询scope逐项登记，不将表子集、后缀或失败日门禁冒充全H1查询完成。

`family`允许all/ipv4/ipv6/unknown，ipv4／ipv6均包含mixed，另报未归属族排除数；`kind`为根已绑定六类之一或all；`level`为all/high/middle/low/unknown/conflict；hour为0—23整数或None，q为trim后的至多120字符，sort为severity/time。列表按原SQLite文本规则`severity ASC, start_time DESC, reference ASC`或后两项，再追加occurrence稳定排序；不把原文本排序暗换成新的时间排序语义。页最大60且受当前行／批资源预算约束。

### 领域表、行身份与原字段保留映射

历史主体继续是DuckLake管理的原生typed Parquet；各表都处于CoreToken固定snapshot。以下是逻辑唯一键／FK，生产构造与独立有序对账检查它们，不声称DuckLake替本模块强制SQL外键。

| 表与必要性 | 行身份／FK | 实际保存及来源规则 |
| --- | --- | --- |
| `core_days` | PK为本profile内day；root_document与document_id指向底层documents，file_id指向files。正常日document_id为provenance，失败日为自己的诊断 | state、原input_version、root_interpretation与input_interpretation分列、原日window、已知记录数；根source/data_profile/kinds/index窗口及所有额外原字段由metadata精确根文档保留。available空日count=0；validation_failed／not_retained不补零 |
| `core_diagnostics` | PK `(day,reason_ordinal)`；day→core_days，document_id→该日诊断文档 | 每个原reason的stage、kind、code和可空count；完整evidence、原schema、源及时间仍在同一诊断精确文档，不把四个日原因复制成一种 |
| `core_records` | PK occurrence，按日及原SQLite行序赋值；`(file_id,source_ordinal)`指向原child表occurrence；day→core_days；item_document／payload_document→底层documents | 原reference/kind/object/start_time/hour/family/level/severity/search/content_version为独立typed列；item与payload的封存path/span、原TEXT/BLOB storage class分别保存。完整item以已知字段及item_exact返回，完整payload／provenance以精确有序文档返回；原额外SQL列继续在未改的child载体 |
| `core_scalars` | PK `(occurrence,scalar_ordinal)`；occurrence→core_records，document_id必须等于其payload_document；member_path从原payload根开始，以对象成员键及原member ordinal／数组index定位 | 必须从通用nodes提升为明确anomaly-record tag解释，故新增此表。每个tag occurrence保存kind、原value文本；decimal／timedelta另有sign/coefficient_digits/exponent10/negative_zero及可精确落入时的int64便利值；datetime另有UTC文本、精确UTC微秒及无偏移时实际使用的数据档时区。原tag、词法、偏移、重复位置和精度保留，不用float替代原值；同值多位置不合成一条，也不当作新事件人口 |
| `core_links` | PK link_ordinal；occurrence→core_records，document_id→该payload；resolved的target_file→同一底层集合files | 必须区分本地候选关联与H3完整详情，故新增此表。逐记录保留实际payload evidence依赖边，以及原reference的H3_original_detail／unresolved_external边；无目标不捏造FK。其余根、provenance、补验JSONL及诊断依赖完整保留在底层edges，不声称此表替代整集合闭包 |

Root/day/input_version核对复用当前`DailyIndex`的纯构造规则及按日解释选择；不会调用其原文件读取作为新查询后门。provenance原bytes的overview_v1摘要、根／日来源和数据档、按实际时刻比较的窗口、原records数及补验JSONL的完整多重集逐项核对。SQLite检索列与item、payload共同字段／明细再次交叉核验；对matched来源调用当前纯转换规则复核共同字段／明细／关联／原身份／限制。非matched payload不能得到本有限领域准入；同reference的多个合法原行仍分别保留并由references报告ambiguous，不沿用旧fetchone行为。

旧逻辑保留位置：`core_overview_input.overview_item/overview_level_filter/overview_search_text`继续决定AS_SET对象待核实、六类对象与角色、mixed／unknown、等级冲突、父前缀／国家名搜索等；`validate_record_time/validate_level_conflicts`仍执行原时间、duration和等级证据检查。原kind／族总体数与distinct-prefix小时趋势仅受day/family控制；列表kind/level/hour/q不反向改变趋势。查询SQL与现项目DailyIndex在正常唯一键fixture上逐项对照；同severity/time/reference的额外occurrence键只解决完整多候选稳定翻页。

JSON解释先使用独立精确树保留有序members、重复成员、数字词法和缺失/null。实际item必需／已解释字段重键拒绝；普通item.extra重键仍完整可读。payload自身的旧content_version需要唯一规范对象，payload重键导致该领域profile拒绝，底层结构仍保留，不能用最后键覆盖获得准入。只有在已精确拒绝payload重键后，才调用旧deserialize_record核验**旧codec自身的content_version**；其dict／float结果立即丢弃，不用作领域投影或数字oracle。投影和全部字段比对仍使用精确树、显式tag解码及严格bool/int区分。Decimal超便利列范围保留独立精确结构；原生datetime原本没有文本小数精度证明时仍标unknown，不伪造原精度。

四隔离日03-03／04／10／20各自返回原diagnostic，overview/trend/events均为null，detail与references执行同一日门禁。缺日为not_retained；合格空日available且已知0。H3原关系与未适配H2不由本地record存在推定闭合；本片不访问H3表，不删除依赖来授予H族完整。根声明scale/origin/path_comparison/composition仍沿未改C.1拒绝规则；H2/H5/H4/H6没有借此扩展。

### 执行、资源与限定索引

投影使用候选目录中有界SQLite仅暂存结构定位和领域写批，逐原record处理及按补验payload摘要索引核对完整多重集；不在Python装下整个源人口。文档解释／原span返回受当前单文档和行字节限制，source行、定位输入行、profile行、typed回读行、实际字节分别计量，不把tag展开行与旧records行混合。暂存按SQLite页数记账，native写批前后检查候选目录，上报候选峰值；完成载体移除本次派生projection.sqlite，JSON正文／nodes不再复制到PG。元数据保守预留、当前RSS／磁盘余量及无总处理时限继承底层合同；不声称分配前硬配额或跨文件原子快照。

领域读取会话资格扫描是入口／末尾固定成本，页内只查固定表、缓存已绑定day/family趋势与query统计，返回记录的原文档定位已在同批取得，不为每条item额外访问PG。分页仍有DuckLake目录开销；不得称线上低成本。`rebuild`仅在自有PG新schema建立day/occurrence/reference/kind/family/level/hour/severity/start_time/search/item_document/payload_document窄表及lookup/order索引；无item/payload/JSON/nodes主体，索引可从固定领域表完整重建。每个有界写批后测relation_size，末尾共享资格校验后事务提交，返回仍为provisional；PG关系大小不包含WAL、整个实例临时空间或全系统磁盘峰值。


### 固定候选验证与实际成本

最终H1矩阵为**36 passed，34.10秒**（Git外`q3-c2-fixed.log`）；兼容矩阵为**73 passed、6 deselected，13.83秒**（`q3-c2-compatibility.log`），合计109个不同用例，不累计中间重复运行。兼容检查使用未改的C.1／旧模块，覆盖两个结构profile、原四P2、旧六个唯一Token原库不重导及旧ready不变；930是其中b1的别名。未选六项为旧四成本形态、旧完整gzip逐截断矩阵及其旧分页成本，本次不拿旧结果冒充新实测。

H1使用自己的非空65条日、合格空日、四具体失败诊断，另有17条唯一键DailyIndex对照和小型正反例。逐原SQL检索列、全部item/payload/provenance、全部有序投影行、多候选全页、mixed/AS_SET/unknown等级和族、tag大精度/微秒/偏移与原位置、小时distinct prefix、同severity/time/ref的稳定翻页均对账。完整文档的期望由独立标准库pairs/数字词法oracle生成，不调用新exact/Parser；大数字、负零与scalar独立数值结构另用手写期望，不能只比集合或counts。

验证包括原检索列／item／payload／provenance矛盾、tag冲突、真实语义重键、普通item.extra重复保留、未适配根拒绝、投影累计行、原span总字节打开前门禁、当前单行与**包含metadata/trend的整页**字节门禁、真实span截断恢复后永久failed、参数纠错／错游标、入口／yield／末尾预算漂移、profile／collection／child／ready／规则／源实体撤回。新进程实际移走自己的source与freeze后，只凭目标／CoreToken完成五表全有序比较、筛选／排序／小时趋势／空日／失败日／完整详情／引用候选及PG索引重建；禁止回访旧DailyIndex。索引逐行与领域表核对，无JSON/nodes正文；65行及两索引实际relation_size为98,304字节。

初期SQL固定快照别名语法错误与JSON序列化tuple/list绑定比较错误各有一次失败，随后修复；原日志及候选均保留。中间证据曾复用产品exact生成文档期望，最终已替换为独立oracle并完整重跑；中间结果不作为独立无损证明。资源末检又补齐规范化全部列后的写前字节计量及完整响应包计量，均在最终36项中覆盖。

实际成本只测自己的65条Core／1个非空日＋1个合格空日＋4诊断形态，不外推真实55日；17条用于规则对照，未追加1025条吞吐结论。

| 阶段／输出形态 | wall秒 | 独占PG实际日志语句 | 全Python SHA256调用／字节 |
| --- | --- | --- | --- |
| 集合freeze | 2.606 | 0 | 383 / 12,528,144 |
| 集合import | 4.251 | 824 | 927 / 32,131,476 |
| H1领域project | 1.370 | 624 | 1,313 / 12,900,337 |
| 五表bulk，批1 | 0.431 | 145 | 533 / 5,852,454 |
| 五表bulk，批17 | 0.301 | 145 | 533 / 5,852,454 |
| 五表bulk，批1000 | 0.245 | 145 | 533 / 5,852,454 |
| events页1（66次，含末页判空） | 0.394 | 700 | 594 / 5,682,552 |
| events页17（4次） | 0.184 | 142 | 532 / 5,600,220 |
| events页60（2次） | 0.191 | 120 | 530 / 5,597,562 |

分页三次的集合显式资格hash_calls均362；此数不含另计的旧child/代码SHA。全Python计数还含每页小游标身份SHA，因此不能声称分页总哈希次数恒定；变化部分不是逐页重扫全闭包。PG来自本任务独占socket实例的log_statement实际日志，包含DuckLake隐式目录访问；不等于底层所有OS I/O，也不是无审计开销吞吐。

65条原records投影为days 6、diagnostics 4、records 65、scalar occurrence 437、links 130，共642行；原child总67行另外含两日provenance，不能混报事件数。领域写前规范化逻辑字节256,908；新领域Parquet 26,183字节，底层集合Parquet 478,525字节，底层原／解码／child制品1,217,901字节。领域候选磁盘观测峰值235,079字节、暂存SQLite 208,896字节；完成前正文26,183字节，不含稍后ready元数据。project当前Python RSS 185,794,560字节，进程生命周期ru_maxrss 222,937,088字节，后者不是本阶段独立峰值且不包含PG。完整分阶段计数、资源、native写批、原物理／解码字节及PG原日志保留在`实际成本.json`及同目录文件；H1没有新压缩解码，未测的系统级峰值不补零。

所有数据库和输入均是自己的fixture／私有PG；最终smart stop后复查`no server running`，回执`q3-c2/pg-stop-final.txt`。固定提交、精确父、完整差异、逐文件SHA、原逻辑保留映射、旧Token证明、失败候选、独立可复算期望及新进程／成本证据统一见本任务Git外`q3-c2/交付索引.json`。先交父任务安排独立复核，不自行集成或宣称GO；未改2c8a、M2/M3、其他计算／Publication，未操作真实H/D/P、旧业务failedcandidate、远端、生产、pilot、HTTP／前端、Issue／push／部署。


## 12. Q3-C.2 两项 P2 定向修复候选（待独立增量复核）

以固定原候选 `23d243d9e71c8f472bcdf30f30bf21c16a4859e8` 为唯一父，在独立分支 `codex/q3-c2-session-cleanup` 修复；第11节原候选、109项作者结果、独立37项及原反例分别保留，均不计为本次新增测试。独立完整报告为Standards／Spec各一项P2；静态暂存预算疑点没有合法可达反例，未列阻断，本次不扩资源或领域重构。

**终端资格与清理。** 完成回执先只存局部候选，在末尾资格校验、DuckDB真实关闭、本地预算末检及资格PG真实关闭全部成功后才对调用者可见。只读资格事务的共享锁保持到DuckDB释放和本地末检之后；不向已关闭DuckDB执行检查。关闭错误导致永久failed、没有complete。已有读取或资格主异常保持原类型、对象和回溯，附加`cleanup_errors`及异常note；逐项尝试剩余owner清理。Arrow流、资格连接和重建owner沿相同释放规则；GeneratorExit／bulk早停仍不能获得完成回执。

**合法操作登记。** 失败日和不存在occurrence的详情仍合法返回provisional；失败日overview/trend/events三字段保持null，缺详情保持not_retained／missing。所有详情在完成后以`detail:day:occurrence`登记，并在新增`operation_scopes`记录实际day、occurrence、state、resolution（门禁unknown／缺失missing／存在matched）和completed状态。重建在真实事务提交且其PG owner成功关闭后，另以`rebuild:实际schema`登记实际schema、固定CoreToken、core_query_index_only范围、committed状态和实际行数，内部bulk不能替代此登记。上述登记须经会话终端资格及全部清理成功才能成为complete回执；分页full_query／query_suffix及未耗尽规则保留。新增scope受原数量及元数据字节预算限制。此处仅离线Python返回结构，不涉及HTTP／OpenAPI。

**规则身份与重算边界。** 没有删除代码身份门禁，也没有跨规则CoreToken兼容层：本修复读取器明确拒绝旧23d CoreToken。允许从其既有且仍合格的原CollectionToken重新调用project_core，得到新规则身份的CoreToken；不需要重新freeze、重新import、重读MRT或重导历史源。旧Token／ready／领域载体保持不变。是否在将来真实迁入前采用此策略，由父任务评审，本片不改真实绑定。

**本次实际验证。** 自己的原65条人工小库上先用23d完整源码和原CoreToken重新执行三条操作，实际重现两个详情completed_operations为空、真实已提交索引仅登记bulk。修复后执行`test_historical_core_cleanup.py`共**10个不同用例，10 passed，4.59秒**，不累加中间7项重跑：真实DuckDB close后抛错；真实SQL CatalogException加close错误；PG实际close后抛错；末尾资格主异常加两个owner关闭错误；健康关闭时实际PG共享锁仍持有及参数纠正；bulk早停；原Collection重投影五表642行和全部65条独立期望详情一致；两个合法详情及实际提交索引登记各一项。实际SQL主异常对象／回溯、所有实际句柄关闭、失败无回执、成功回执时序均保存。正常回执不代表H3闭合或实际H族迁入。

证据位于作者Git外`q3-c2-cleanup/`，最终10项目录为`acceptance-bf3ca8823cf94024aba15bf8c1a6997c`；旧候选和原交付索引保持不变。仅使用自己的fixture与私有PG。交付含精确提交／父、完整差异、原／新Token规则身份、原ready未变、原反例与修复后记录、PG smart停止证明。两项修复仍须由同一审查者增量复核及父任务接受，当时不宣称GO、集成或生产验收；该H1修复随后获父任务接受及独立双轴GO，本片记录保留。

## 13. Q3-C.3 H2 原人工候选（独立双轴 REPAIR，原记录保留）

本片从固定`2c1dc3b6669ecd20e00f82a9208b5f8041dec220`建立`codex/q3-c3-h2-profile`。H1两项P2已获父任务接受、独立双轴GO；原分支及制品不改。以下具体范围经父任务确认后完成本片源码及人工验证；尚非独立GO、集成或真实H2验收。

相邻`historical_rib`提供小型离线Interface：`freeze_collection(Binding, destination)`、`History.import_collection`、`History.project_rib(CollectionToken)`、`with History.rib(RibToken)`内的有界`bulk(table)`、固定筛选／occurrence游标`query(table, ...)`、文档原值和MRT定位`detail`。不开放任意SQL／JSONPath。原集合六张结构表及SQLite child导入、预算、原字节保管和DuckLake批写继续复用；H2仅增加有限冻结角色与领域适配，不修改旧C.1／H1代码身份。H2冻结适配器代码单独绑定到源清单，领域规则同时绑定适配器、共享设施和本项目旧科学规则；C.1本身只承担结构审计，不借其complete声称H2准入。

`core-rib-consumption/v1`的原生根可为scale manifest、origin manifest或path consumption package；`rib-comparison-jsonl/v1`根为原comparison manifest。各根按原schema分派，summary/evidence/source-manifest与manifest.files依赖逐一跟随；源MRT必须显式本地绑定且SHA一致。消费summary与源summary各按原URI／file occurrence保存，不能按同名或同值覆盖。根之外没有另造业务清单；未声明文件、漏依赖、错摘要／大小、身份重键均拒绝。SQLite paths和frames完整沿既有child保存，包括storage class及BLOB。

领域表限定于`scale_family`、`origin_family`、`origin_members`、`peers`、`origin_paths`、`path_endpoint_metrics`、`comparison_objects`、`comparison_frames`、`comparison_refs`、`comparison_reasons`及`mrt_observations`／文档定位。所有表绑定原root/file/document或物理record/entry occurrence；数组原位置单列，原文扩展仍由集合节点和span保留。path_key/as4_key保持原BLOB，NULL／缺属性前导0／空属性前导1不合并。PG只目录和必要薄索引；主体为原生typed湖表和封存原件。

MRT适配仅离线只读自己的冻结原件：压缩EOF、压缩／解压SHA全核验，按完整MRT物理记录和原entry位置保留时间、AFI/SAFI/Prefix、原Peer属性、Originated Time、AS_PATH/AS4_PATH原字节；复用当前rib_origin和rib_path_comparison已接受段解释／分类规则。逐条核对frames.sqlite、JSON左右frames/ref occurrence、Peer组、全部refs及分类reason，必须比同计数更强，不能只校验offset在文件内。单RIB路径目录原首次位置和计数／原origin成员与原MRT全量对照；不能核对的原路径标Unknown／未验证且不能授予该完整H2范围profile_validated。单时点与两个端点分别标scope，Session为unknown，interval_change_count为null，不解释连续变化或H1→H3闭合。

验收只用自造双family／五分类／重复内容与Peer/entry／AS_SET及AS4的人工输入；原文本／二进制构造期望与固定旧科学规则作独立oracle。封存导入后移走自己的source和freeze，新进程仅固定Token／目标，逐表全部有序原值、全refs及MRT原字节／位置逐项比较；不同输出批和页大小不改变全集。必须拒绝同计数错frame/ref、错Peer/路径、漏依赖、伪合格、最终资格／清理失败和预算漂移。报告分阶段wall、行与字节、实际PG SQL、全hash调用及字节、暂存磁盘／当前RSS与进程生命周期峰；无处理总时限。只做必要旧Collection/H1原Token兼容检查，不重跑109／37旧矩阵；共享身份若实际变化，明确列出重投影范围且不改原Token。


### 本片实际接口、复用与限定

实际入口为相邻`historical_rib`七文件，原`historical_collection`七模块、`historical_import`六模块、`historical_core`七模块与Core／RIB原科学规则均未修改。冻结复用C.1的关闭源绑定、路径／复制／最终源验证、严格单成员gzip、JSON/JSONL节点／数字、Spool及SQLite child；本片分派原生H2根和其固定角色，沿同一六表导入路径进入DuckLake。H2自身角色代码在`h2_freezer_code`单独绑定，`RibToken`再绑定完整领域代码、C.1/H1共享设施及旧RIB规则；构造不发布、不自动选源。

消费包内`source-manifest.json`的来源summary与消费summary即使都叫`summary.json`也不能按目录相对名混用。显式`external_files`可用`父清单的file URI + '/' + 原ref`作为清单限定的绑定键（仅为绑定键，不当文件路径访问），绑定键优先于一般相对URI。文件表仍保存实际绑定原文件URI，边保存原ref和父文档节点；两份summary和两份同字节manifest不合并物理occurrence。已声明必需角色缺失或失败记失败边，冻结失败账不能成为完成标记。

完成领域载体为15张原生typed表、384行：documents 48、scale_family 3、origin_family 3、origin_members 4、peers 10、origin_paths 4、path_endpoint_metrics 6、comparison_objects 36、comparison_frames 60、comparison_refs 62、comparison_reasons 9、peer_groups 2、peer_group_members 6、mrt_frames 65、mrt_observations 66。SQL BLOB和路径原字节保留；路径便利解释不能替代原AS_PATH／AS4_PATH。实际MRT范围沿当前单播解析及端点规则限定，不建立稳定Peer Identity或Session，不执行UPDATE状态重放。

`session.query(table, file_id/family/status, limit, cursor)`只接受固定表和支持的筛选；页最多60条，游标绑定完整RibToken／筛选／页大小及含原occurrence的完整排序键。AFI表支持ipv4／ipv6筛选。`detail(document_id)`返回原span bytes及精确有序树；`reference(document_id, object_ordinal, side, ref_ordinal)`返回固定原引用、Peer／路径原列及封存MRT真实header+body bytes。普通多页只查询固定湖快照，不逐页重做闭包SHA；bulk逐表完整有序摘要与页scope分别登记。未耗尽无complete，新会话从中间游标继续只签query_suffix。

投影和查询所有结果均受终端资格约束。写DuckDB成功关闭之后才提交领域登记；目录writer真实close失败时不返回Token，并实际尝试将本次登记撤销为failed，保留FAILED账及主异常。查询末尾保持资格共享锁至DuckDB释放／末检，最后PG释放成功才给complete。两者不是跨PG／文件系统断电原子恢复承诺；撤销本身遇外部故障时保留异常说明，不能把未完成撤销说成成功。JSON主体／MRT正文／领域表不复制到PG，PG仅旧目录及新增八列rib_profiles登记；本片未实现另一个PG业务副本或HTTP索引。

### 实际验证与资源（仅自己的人工小库）

固定交付运行`q3-c3-delivery.log`及`q3-c3-tests-delivery.xml`为**26个不同用例，26 passed，14.12秒**。不累计此前1／10／18／20／23／25项中间重复执行，不重跑H1作者109／独立37矩阵。最早外部绑定键不是URI、两种summary绑定命名空间冲突各有一次真实失败，原日志／候选保留；修正后未用旁路门禁获得通过。

正例包含两family、全部五分类、相同路径的重复entry／重复Peer属性、AS_SET未知、AS4原件与private-skip；另有零分母样例保留null。独立struct解包逐字段核对全部66条MRT观察、65个物理frame、原Peer、路径字节、Originated Time／位置，科学解释调用固定原规则而非新Projector；原JSON逐数组位置独立核对全部objects／frames／refs／reasons及origin_members，原paths.sqlite逐列核对BLOB和首次位置。原文档用独立标准库pairs／数字词法oracle对所有48份精确树和原字节核对。

将自己的source、freeze分别移为source-removed、freeze-removed之后，真实新Python进程只用固定Token／目标读取全部15表、全部原文档及62条引用返回，逐项比较已独立核对的全字段、路径、原帧bytes及原位置，不只比计数或首行。页1／17／60的全集、族与分类筛选及scope通过。自洽重算清单SHA后，同计数错frame、重复frame代替另一frame、错ref、错Peer、错原路径首次位置、伪连续资格均拒绝；缺必需MRT冻结失败。实际SQL错误+真实close后注错保留主异常；入口后预算漂移、bulk早停、末尾资格错误、真实DuckDB／PG close后注错及末尾错误+两个owner关闭失败均没有complete。投影目录writer真实关闭后注错也实际撤销登记为failed、所有实际句柄关闭且没有返回Token。

| 阶段／输出形态 | wall秒 | 实际PG日志语句 | 全Python SHA256调用／字节 |
| --- | --- | --- | --- |
| freeze | 0.216 | 0 | 241 / 1,572,130 |
| import_collection | 0.886 | 654 | 429 / 3,612,438 |
| project_rib | 0.483 | 808 | 499 / 3,884,885 |
| 全15表bulk，批1／17／1000 | 0.161 / 0.137 / 0.135 | 各264 | 各347 / 2,996,374 |
| 36个比较对象页1／17／60 | 0.153 / 0.099 / 0.093 | 380 / 108 / 90 | 369 / 2,963,264；335 / 2,921,515；333 / 2,919,057 |

PG来自自己独占socket实例log_statement的实际分阶段片段，不用显式Python调用数冒充数据库往返；freeze实际为文件操作，PG日志为空。不同bulk输出批不增加全闭包hash次数／字节；分页全Python差异含每页小游标摘要，集合显式资格hash固定，不能说所有hash次数不变。成本包含审计和资格检查，不外推全天／真实15+8文件、吞吐上限或在线时延。

领域Parquet30,075字节；投影写前规范化78,162字节；候选实测盘峰140,667字节，主SQLite暂存110,592字节，原规则复验SQLite峰8,192字节。完成元数据生成前候选当前占用30,075字节。project当前Python RSS193,757,184字节，进程生命周期ru_maxrss同值；不是独立阶段峰，也不含PG／系统峰。collection／freeze自己的物理、逻辑累计及RSS另保留，未测全系统峰为Unknown。无处理总时限；磁盘、当前RSS、typed／MRT行字节、单文档／页及冻结资源门禁保留。原规则解析的prefix集合仍受既有有限范围和RSS保护，不声称无内存索引或无限承载。

旧合法Collection与H1原Token在各自原目标、原ready直接全表回读，通过且SHA未变；没有freeze／import／project重算。因为此次未改C.1／H1源文件，旧规则身份保持不变。新H2的冻结角色身份与领域规则身份分开；将来仅领域查询规则变化时，可由仍合格的原H2 Collection重新project_rib，不静默更新Token；冻结角色变化则另行评估其原始闭包身份，不能冒称任意旧集合都可跨规则复用。

自己的PG已smart停止，`q3-c3/pg-stop.txt`成功、`pg-status.txt`为no server running。最终证据目录为`q3-c3/acceptance-8bb569e285ba4d32a7482036627f653f`，固定提交／唯一父、完整差异／源码SHA／载体及失败证据索引另交父任务安排独立复核。旧准备材料中15件摘要与另一8依赖仍仅是来源线索，未打开远端真实内容；本片没有真实H2／H3—H6／D／P迁入、HTTP／前端、726、Issue／push、部署或集成。


## 14. Q3-C.3 两项 P2 有限修复（已获独立双轴 GO 和父任务接受）

唯一父为原候选`89999697545844f30344cc03873fc21312b26a88`，修复分支`codex/q3-c3-two-p2`。308e独立报告的Standards／Spec各有一项硬P2；报告SHA256为`7a4c8418eac57d36fcbbc72d047fa2c3b9334d3b20543a1250322a91f21420ef`。第13节原26项作者验证、独立25通过／1排除及原成本保持历史口径，不计入本次新增用例。继承H1的metadata／references／rebuild接口问题不在本次范围。

**累计预算。** SQLite child预检和实际执行均继续使用剩余额度；仅将已累计child_rows的比较上限改为全局max_total_rows。实际40＋30行、总限100时，两次预检和执行收到100、60，累计70通过；40＋61在第二次预检拒绝，不进入其冻结执行。没有增大或移除行、字节、磁盘、RSS门禁。

**原生消费资格。** 从封存集合读取两个原字节JSON文件，在候选的有界临时目录中恢复旧Reader要求的路径，直接调用未修改的core_overview_paths.read_comparison；不重编码JSON、不改全局读取函数、不回访原source。原Reader的唯一性、总数≤10、每族≤min(5,different)、同业务日、原引用精确字段、路径长度和类型等门禁全部沿原调用执行。随后每个示例必须唯一绑定已核验comparison_objects中的different对象，并逐侧与其唯一comparison_refs原位置一致；不删除或去重示例掩盖错误。临时目录计入既有磁盘检查并在调用后清理；每份文件最多65,536字节。

**固定身份和旧集合。** 只额外接受精确8999969冻结器文件SHA`7ce7c5fe729c73e5ed78ba307e5bba78698423d1fe13c5c3cfbcb23c71942b9d`，全部C.1依赖仍须与当前逐项相同。这一旧冻结器只过早误拒累计预算内的child，没有改变原生角色、封存结构或来源准入；其仍合格Collection可重新完整领域投影。完整H2领域规则身份因修复变化，旧RibToken直接读取仍拒绝，不能改写原Token或ready。自己的原合法8999969 Collection重投影后15表全字段与全部62条引用／原帧字节一致，Collection未变且未重新freeze/import/MRT封存；同一旧坏Collection重新投影被原Reader门禁拒绝，没有新RibToken或ready。C.1和H1原Token在各自原目标直接读取仍通过。

**实际前后反例及定向验证。** Git外独立归档8999969源码，以自己的人工四根闭包重现：8个合法示例增加重复项成为9个，旧Reader拒绝，但8999969仍签RibToken并完成全15表读取。原坏Token／Collection／ready保持不变。另以真实SQLite child重现40＋30在原实现第二次错误拒绝。修复测试`test_historical_rib_repair.py`本次共**14个不同用例，14 passed，17.18秒**：两组真实child预算；同一旧坏Collection拒绝；同一旧合法Collection重投影；七种原Reader分支反例；跨业务日反例；原Reader字段检查可通过但实际对象为not_comparable的示例绑定反例；一个新健康闭包。最后一项包含完整原文档／MRT／SQLite列／数组occurrence独立核对、实际移走source与freeze后新Python进程全15表和全refs回读，以及必要旧C.1/H1原目标兼容；这些检查不另累加为不同pytest用例。未重跑旧26／25／109／37矩阵。

本次新健康投影ready中记录384行、规范化78,162字节、领域Parquet 30,075字节、暂存SQLite 110,592字节、候选观测磁盘峰140,667字节；该记录写入前elapsed为0.470秒、当前Python RSS 167,428,096字节、进程生命周期峰220,741,632字节。它不是含最终资格检查和返回的完整调用wall，也不是系统／PG峰；其中hash与sql_calls仅该Budget内部计数，不能替代第13节的全Python hash和PG日志分阶段测量。本次未重测完整性能矩阵，新增原Reader仅两份有界原字节临时副本，未测性能为Unknown，不外推真实业务规模。

证据根为作者Git外`q3-c3-repair/`，本次测试目录`acceptance-865ded30b3ad42ebb08d86eff8699c06`；原候选及308e证据保持不变。最终交付另附固定提交／父、完整差异、逐文件SHA、前后反例、失败候选、旧合法重投影、新进程证明及本次私有PG smart停止回执。仅自己的人工输入和私有PG；先交父任务安排同一审查者有限增量复核，不自行宣称GO、集成或真实迁入。


## 15. Q3-C.4 H5 原人工候选（Standards REPAIR，Spec GO）

H2固定修复`d57f3a997024c7a63e9d5879a720576da044f8bf`已获308e独立增量GO及父任务接受，随后8233实际人工集成`941f040a838d90771fee102702c20e59c4c47826`也获父任务接受；第13—14节的原报告、反例和当时状态记录保留。本片按已接受第1—8节和父任务另行授权，从固定d57f3a9新建`codex/q3-c4-h5-profile`，不修改8233工作，不表示真实H5或产品目标关闭。

### 有限入口与复用

相邻`historical_general`提供`freeze_collection`（直接复用C.1）、`History.import_collection`（既有C.1路径）、`History.project_general(CollectionToken)`及`with History.general(GeneralToken)`。H5仅接受人工`general-read-model/v1`闭包；原根manifest/COMPLETE同bytes、四角色文件、压缩/解码SHA、全部节点/人口/轨道长度/样本计数仍由不变C.1负责。领域阶段从固定C.1六表的有界bulk读取，复用其Spool和Node，在自己的临时SQLite按原节点定位进行投影；不另建JSON词法引擎、通用catalog或原业务Publication。

General会话显式复用已接受的预算、失败、owner释放及操作回执设施，未继承H1/RIB业务会话，因此不暴露metadata/references/rebuild等不适用方法。领域写入继续使用既有DuckLake／Arrow批路径、固定snapshot／原件资格及真实owner关闭规则。PG只增加八列general_profiles窄登记及既有DuckLake目录，JSON正文／typed明细不进入PG；本片没有另建业务查询索引，需定位时使用新湖表和C.1既有结构，不建立恢复/重试框架。

### 原生表与定位键

| 表 | 固定完整排序键／作用 |
| --- | --- |
| documents | document_id；沿C.1保留file、物理行／文档ordinal、原entity和byte span |
| general_stores / general_events | store_id / event_id；原根、根内event_ordinal及事件node，原dataset/run/implementation和publication/revision/incident/reference/cohort/metric/as_path等分别绑定，保留admission |
| event_files | event_id, role, row_ordinal；根声明的四文件与其每个原文档occurrence，不按同内容去重 |
| scalar_fields | event_id, document_id, node_ordinal, owner, owner_ordinal, member_ordinal, name；根/事件/overview/AS/关系/样本/轨道定义的有序scalar原字段，显式kind/presence、字符串/bool/number原词法及精确十进制结构 |
| overview_metrics | event_id, section, metric_ordinal；cohort各分母、final_values及全部peaks名称、原值node与峰时node，不限页面当前指标 |
| track_definitions / series_points | event_id, track_ordinal / event_id, track_ordinal, point_index；全部轨道/额外定义、原timestamp/node/解析UTC、精确原值及null；定义存在但无轨道单列，缺定义标missing |
| affected_as | event_id, row_ordinal；原AS/rank/classification及旧搜索字段，原完整列由scalar_fields及文档保留 |
| path_relations | event_id, relation_ordinal；原affected/downstream ASN、concurrent计数和搜索列；全部计数、峰值、时点和relationship_semantics由scalar_fields及原文档保留 |
| path_samples / sample_peer_members | event_id, relation_ordinal, sample_ordinal / 再加member_ordinal；全部prefix/family/as_path原ID/原canonical、route_observation_count及Peer数组成员，不合并重复样本／成员 |
| identity_references | event_id, reference_ordinal；原文档/node/字段、actual/expected、matched/missing/ambiguous/Unknown、目标event/store及关系种类，保留全部原候选 |

完整原文档／额外容器、原对象成员和精确数字仍可由`token.collection`调用既有结构接口读取；并非固定便利列之外的字段被删除。来源缺失没有scalar节点，JSON null有present/kind=null；旧服务常量quality/observation/missing_slot/data_mode仅放在`overview`返回的`legacy_derived`，不倒灌`source`。

### 领域资格、身份和查询

源content SHA沿固定旧General规范单独复验：按C.1节点、对象排序键及数组原序逐块送hash，不重建整事件对象或track列表。仅这个旧hash兼容通道复现旧json数字规范，领域typed数值始终取原节点；无法可靠复现的旧规范数值（如转非有限float）拒绝H5资格，不修改旧SHA。General原科学代码不变。

series timestamps和每条track以两个SQLite节点游标按原member/index有界归并，逐点保留原时间文本与UTC、原数值词法；不排序、去重或补槽。overview计数与原event逐项一致；未知生命周期要求event_end/duration原null及is_final=False，不由末尾timestamp推结束。overview各分母、final和peaks是原声明的完整保留，不把它们重新解释成真实全国影响。

同一根的原身份冲突沿C.1冻结门禁拒绝；跨根同incident/publication/read_model/canonical_reference或dataset身份的多个候选全部保留，受影响event标identity_conflict。相同上游dataset/snapshot的content或manifest声明冲突也隔离其事件。事件子件显式身份/revision/cohort与所属event不符记missing（指所属事件的预期绑定不匹配），源未声明的可选身份记Unknown；不任取最后项。根上游仅身份引用继续是identity_only/Unknown，store_declaration_candidate的matched只表示声明可关联，不能证明上游S1/S2/S3/lifecycle原件已存在。一个根的complete、领域策略验证和某事件business available分别表达。

会话`resolve(incident_id/reference, publication_id/revision, cursor)`返回所有原事件候选及解析状态；`query(table, event_id, track_ordinal/relation_ordinal, classification/search/sort/affected_asn/scope, limit, cursor)`只开放固定表和相应筛选。业务表必须绑定具体event occurrence并通过admission；audit表及原文档可以读取隔离事实。AS default保留原行序，asn_asc用(asn, row_ordinal)稳定排序；关系保持原序，scope=concurrent沿原非零规则；搜索沿旧trim/lower/removeprefix('as')及旧字段组合。`affected_asns`与`path_downstreams`提供旧字段兼容页，`overview`返回源字段及分离的旧常量；series用全部track/point页，不提供整事件大列表捷径。

所有页最大60项，游标绑定完整GeneralToken、表、全部参数及含occurrence的完整排序键；同会话连续页登记full_query，新会话接中间页只登记query_suffix，未耗尽没有完成回执。原文档`document(document_id, offset, length)`按原span分块返回，回执只声明实际byte范围；完整原文由调用者逐块对账。普通页不重复全闭包SHA；入口和出口仍完整核验固定身份，最终共享资格锁保持至DuckDB真实释放及预算末检，PG关闭成功后才给complete。

### 资源与人工验收边界

前置C.1结构读入、临时SQLite实际页、typed写前行/字节、单行/返回页/RSS/磁盘余量继续受既有预算；源/冻结/导入/投影为分阶段口径。固定DuckDB 1.4.4会话关闭late_materialization_max_rows并在每次读取检查配置未漂移，防止小LIMIT的双扫描优化；分页按所涉及固定表的全表行数和规范逻辑字节**保守累计扫描上界**，达到既有累计预算即拒绝；DuckDB可能剪枝，因此这个计数不是引擎实际扫描量。实际执行计划另作一次人工观测，不以输出批有界推断前置扫描有界。查询没有持久PG业务索引，也不声称无全表扫描或无限承载。

本次只用两个同国不同incident的完整人工事件，各含四文件、65个AS与关系、同ASN/rank、重复关系/样本/Peer成员、六条全部轨道及额外定义、完整peaks、负零/超decimal128额外原值和未知生命周期。独立原字节/标准库pairs与数字词法oracle核对结构及领域定位，旧General仅用于合法有限字段/分类/搜索/稳定排序的全页对照。完整基础之后才测17／1025条×两事件和81×四文件的人工形态，不将324文件说成真实81事件。

最终测试数量、阶段实际PG日志/全Python hash、读批、compressed/decoded/raw/node/profile/Parquet字节、wall/RSS作用域、暂存盘、原Token兼容及源移除新进程全值证据统一见本任务Git外固定交付报告和索引；不累计中间重跑，不重跑C.1的61套闭包词法或H1/H2旧矩阵。自己的PG交付前停止。仅作者人工候选，仍需独立审查和父任务接受；真实H5/H3—H6/H/D/P、HTTP/前端、Issue/push、生产/部署/集成均不在本片。


### 本片最终实际结果

最终固定源码运行`single-scan.log`及`single-scan-tests.xml`为**26个不同用例，26 passed，160.95秒**；不把此前1／18／23／26项中间重复运行累加。证据目录`q3-c4/acceptance-ead737f1cc9141308779ed0891565286`。主基础仍实际freeze/import/project两个65项事件；最后三种形态明确复用前次**同一C.1 Collection、同原DB/root**，来源清单SHA相同，C.1代码未变，重新执行H5 project与三种bulk；freeze/import成本沿前次真实日志，不称作本次再次执行。

基础有10个原文件（根＋COMPLETE＋两事件×四文件）、266份原文档、9,576个结构节点；13张H5表共13,146行：stores 1、events 2、documents 266、event_files文档关联264、scalar_fields 6,747、overview_metrics 34、track_definitions 14、series_points 780、affected_as 130、path_relations 130、path_samples 260、sample_peer_members 780、identity_references 3,738。引用行包含matched和Unknown声明，不是取得3,738份上游原件。原文件共19,125字节，其中gzip 8,335字节、gzip解码258,250字节；H5 Parquet 130,347字节。独立核对源bytes、全节点词法/顺序及全部typed定位；移走基础source/freeze后，新进程只用固定Token/目标比对全13表、全部原文档bytes、全部引用行和跨页所有字段。

| 人工形态 | H5行数 | project wall秒／实际PG语句／全Python SHA次数 | H5 Parquet字节 | 投影SQLite／候选实际磁盘峰字节 |
| --- | --- | --- | --- | --- |
| 65项×2事件 | 13,146 | 4.348／792／426 | 130,347 | 2,469,888／2,600,235 |
| 17项×2事件 | 3,738 | 1.572／750／384 | 60,372 | 782,336／842,708 |
| 1025项×2事件 | 201,306 | 64.124／1,932／1,390 | 1,884,362 | 37,281,792／39,166,154 |
| 1项×81事件、324事件文件 | 23,117 | 10.387／852／3,271 | 276,204 | 5,312,512／5,588,716 |

各阶段完整compressed/decoded/raw/node/profile/Parquet字节、Arrow与read块、PG实际日志及全Python hash见`实际资源归因.json`、`phases.json`和各`shape-report.json`。三种形态的bulk 1/17/1000逐行摘要全等，实际PG各224语句；各形态全Python SHA次数/字节在三批大小间完全相同。成本没有外推真实业务规模；PG业务正文/业务索引没有新增，独占DuckLake catalog的实际表/TOAST与索引字节另测，窄登记表为同DB全部候选合计而非单profile占用。

作者资源复查实际发现并修正两点，修正前源码和真实证据均保留。第一，Spool逻辑累计与H5实际磁盘混用导致旧中间计数负上界；最终用同固定limits的独立Spool Budget，保留input_spool_resources，实际disk另计，不把负数截为0。基础输入暂存界1,474,560字节，1025形态21,729,280字节，81事件3,489,792字节，实际断言均非负且≥对应SQLite物理占用。第二，旧配置小LIMIT实际触发两个DUCKLAKE_SCAN、各130行（合计260）；关闭late materialization后实际计划一个扫描130行，配置漂移拒绝，保守计费不能被双扫描绕过。实际OS字节和系统峰仍Unknown，不把计划中的零bytes当零I/O。

65项×2事件：当前Python RSS 195,772,416字节、进程生命周期峰195,772,416字节，非独立阶段/系统/PG峰；该profile独占DuckLake catalog总434,176字节，其中索引73,728字节。

1025项×2事件：当前Python RSS 235,536,384字节、进程生命周期峰255,082,496字节，非独立阶段/系统/PG峰；该profile独占DuckLake catalog总860,160字节，其中索引73,728字节。

1项×81事件、324事件文件：当前Python RSS 240,877,568字节、进程生命周期峰255,082,496字节，非独立阶段/系统/PG峰；该profile独占DuckLake catalog总442,368字节，其中索引73,728字节。

原合法C.1/H1/H2 Token在各自原目标直接回读，ready SHA不变；未重导替身或重新投影旧H2。自己的PG已smart停止，`q3-c4/pg-stop.txt`为server stopped，`pg-status.txt`为no server running。本片交固定提交/唯一父、完整差异、源码与报告SHA、全部负例/成本/新进程证据，由父任务安排独立复核；没有自行集成或宣称真实H5完成。


## 16. Q3-C.4 H5 构造清理 P2 定向修复（待独立增量复核）

唯一父为`5d533927272d5ed3a3840ed0001d1c0fd3c75dee`，新分支`codex/q3-c4-constructor-cleanup`。84e5固定独立报告提交`a1127e8e59fec8ec7821d1ff1daab2df8cbb9617`、报告SHA`df7723290c00686438ee18b2d1bc5dc792b478e17a8bdc297bc7e2bf3f105af2`：Standards仅一项P2 REPAIR，Spec GO；原报告、作者26项及独立22项结果均保留，不计入本次新增验证。FAILED写入次错的非阻断建议明确不在本片。

`Projector`成功取得Spool后，构造期间的H5表/索引初始化现在置于局部异常释放范围。任意构造异常即调用已接受的release释放Spool，再原样raise；外层owner此时尚未进入，不依赖它清理未构造成功的对象。清理自身失败只附加cleanup_errors，不替换主异常对象/回溯。正常构造及外层owner退出的关闭路径保持不变；没有修改C.1 Spool、共享cleanup或其他业务行为。

自己的原健康Collection与真实SQLite authorizer先在5d533代码重现：拒绝取得Spool之后的CREATE，History.project_general抛not authorized，产生FAILED且无ready/Token，但连接仍可SELECT 1；探针随后手动关闭，未留下泄漏。修复后`test_historical_general_constructor.py`共**3个不同用例，3 passed，7.50秒**：真实CREATE拒绝后连接实际关闭；真实关闭后再抛清理错误仍保留原SQLite异常对象与原回溯、仅一次close、FAILED且无ready/Token；正常构造一次关闭，并从同一原Collection重新投影，全13表所有原字段/有序摘要（含全部引用行）及266份原文档bytes与原已独立核验期望一致。未重跑22/26套或三种shape。

H5完整规则SHA随源码变化，由原`9fc30466ca27befe2c67b074848f244f40c50b70687912cc344a9bfda0ad7c15`变为`7a8cb8cd39942939e07328b2b2c6b4c1d27ab2dd9a4df3fac1ab3f21a17195be`。实际新读取器拒绝旧H5 Token；同一仍合格C.1 Collection在原DB/root重新project得到新Token，不需要freeze/import或回访原源。旧Token/ready保持原SHA，未补签旧记录。C.1/H1/H2源文件和规则身份均未改，本次不重复其旧矩阵。

证据根`q3-c4-cleanup/`，最终目录`acceptance-25a53d52927a4344a03e2c9bd8c68615`，保留5d533完整源码归档、自己的前后真实反例、双错/正常关闭、原Collection新旧Token、全值摘要及原ready未变证明。自己的PG已smart停止并确认no server running；成本仅本次3项用例7.50秒及正常投影ready内的有限资源回执，未重测性能矩阵，不沿用原成本冒充本次。提交固定补丁及中文证据，由父任务交同一84e5有限增量复核；不自行集成或启动真实/远端/生产。
