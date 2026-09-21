# 三模块共享公共 Runtime 有限集成

结论：**GO，仅本次人工原件上的公共 Runtime 接合**。Feature、Canonical 的 fixture/real-candidate 同模式准入及读取已实际走通；Detection 原Gap制品通过当前M2接口准入与读取。没有重解析或重产科学数据，没有全P/真实全天、窗口业务或部署验收。

## 固定组合

从接受基线 `d95905d959bc4cc4c2b889d638f3f438343a83cd` 依次完整合入三条已接受链，无冲突：

| owner | 候选末端 | 实际merge |
| --- | --- | --- |
| Feature | `4865bf33bf11fac58e6d7112cbced016b8c4d252`，含7ea→9045→778→4865 | `27fe882b19e1f5346961559f660bb381f6c72d00` |
| Canonical | `3a08f654645477eeb419e86ad7a350c44f39cb1e`，含公共P1/601、0b7窗口与d3→05ff→670→3a | `56233df0b83c6688428e15ee611659ac017677b2` |
| Detection | `00e7fe206b318f64bff6ffb60280bbd85a688c88`，含511双亲7ea/e42与M2 PG兼容 | `f19f285358b09ecc706af1792dd5804722538382` |

实际组合HEAD为 **f19f285358b09ecc706af1792dd5804722538382**，所有新Admission的owner_revision均为该完整提交。各owner涉及的产品文件与对应候选逐字节相同，共享M2三文件保持8b39f5c，未代修产品。未混入未接受3092窗口或Resource real候选；原Resource关闭修复仍在组合基线上。

三份指定独立报告均全文核读并实算SHA相符：Feature `ec8091c53581a4f40415d7ceed311367f611c1a65feeaff9118df64f0a0e9bd3`；Canonical `e68cbe461c3d9cdc45ff3cbfc08d0aba97b074ff1f1411870be3f94712ee2966`；Detection `fbd5025c5cc1ad14a3824f335b058105a57af54bf36ed8f12db79f32ba8d1362`。e42完整P1增量报告另全文读。它们是候选依据，不将其测试数量算成本次实测。

## 原件和模式

本片仅使用自有55483原数据库。运行配置将同一Unix socket的 `/tmp` 拼写解析为 `/private/tmp` 后显式传给各Runtime，实际PG/库身份相同；原manifest、plan、seal、制品路径和原绑定没有改写。Canonical fixture的允许根和socket规范路径检查仍启用。

| 模块 | 原固定对象与依赖 | 本次完整旧输出对照 |
| --- | --- | --- |
| Feature | `819846d60bec4af2986e3ca743d49de3:36`，原M2 `d21c4cf77200477fb5648bc32e39e146:29` 与原CSV | real模式windows14行、coverage36行及原12表全部typed/多重性；fixture完整coverage36行 |
| Canonical | 默认原投影 `c4568efe78c04182900b0999877acd76:51`，实际原M2 `3884fdb6cdb7408d92f78a85db70a6af:26` 与11份参考 | real模式原13表64行逐完整typed与原顺序一致；fixture完整source_coverage2行 |
| Detection | Gap `0774d9ed031d4fb7b25b6ad8a8faff06:7`，同一原3884 M2与11份参考 | records131、state_entries178、m3_entries32，共341行；qualified_revisions16行 |

Feature/Canonical各自两模式均实际准入、current、复用和open_reader；Canonical另显式核对inspect，Feature使用原完整binding；real-candidate明确提供原完整binding、manifest（适用时）、输出根及有限预算，依赖也是实际同模式M2/reference Admission。这里是人工制品走real-candidate控制路径，不是实际真实数据验收。Canonical只消费默认原投影，没有新造或复产显式窗口投影。

Canonical与Detection实际选择相同的原3884/来源序列/参考集合，因此Detection复用本轮Canonical fixture已产生的完整真实依赖；逐项核run/snapshot、原来源顺序和参考source_id，不根据名称猜配。Feature依赖仍独立，不错用3884。没有借用其他任务Admission。

## 必要准入与公开读取

Feature两模式各一次必要full audit；Canonical两模式各一次完整13表audit/一次Replay；Detection一次必要三表完整审计。新M2/reference也是按当前8b规则和实际模式正常准入。没有重解析MRT、运行生产Writer或重产科学制品。

已完成后的reuse/current无Feature full audit、Canonical full audit或entity_hash；Detection current无full_table_scan、body_query或entity_hash。普通读取使用新公开接口，不调用旧Canonical每表重Replay入口。每个正常读取都在资源关闭和尾current通过后才得到complete Receipt，迭代中Receipt为null。

Feature原12表对照保存的全typed原值，不将缺失或Unknown补零；两视图的原时间、窗口、资格与科学值保持。Canonical13表为baseline_mappings3、changes22、invalidations0、scope_gap1、source_quality0、source_coverage2、qualification_change1、reference_binding11、current_routes10、legacy_state6、legacy_prefixes4、legacy_seen_vps3、projection_metadata1，共64行，含原bytes/tuple/set/NULL及原顺序。

Detection首先与原固定三表逐字段对照；原普通JSON证据曾用default=str保存时间，仅按原SQL schema将observed_at恢复为datetime，再沿公开codec规范到同一UTC时刻。其他字符串（含原JSON正文）和列表不解析、排序或改写。另使用原独立fresh-reader的完整typed inventory交叉验证，三表rows/typed_bytes/SHA均完全相同，排除把字符串/时间差异误当科学变化。qualified结果与原qualified读取接口16条全值一致；旧结果窗view尚未接入。

有限拒绝覆盖Feature/Canonical两个方向的跨模式current、错误完整绑定以及读取耗尽后模式漂移；Detection错误snapshot和完整读取后收紧RSS预算均拒绝，无成功Receipt。没有重复已有锁、清理、scratch攻击或科学坏件矩阵。

## 实际执行与测试配置修正

使用 `env -u PYTHONPATH uv run --locked --project backend pytest`，显式 `DOMEYE_PUBLIC_RUNTIME_INTEGRATION=8233`，测试为 `backend/web/tests/test_public_runtime_integration_8233.py`。所有原始脚本版本、日志及XML保存，不把下面几轮写成一次全绿：

1. 首次全片 **3 failed，168.01秒**。Feature/Canonical的两模式正向准入/读取和完整原表对照均已执行并通过断言，之后跨模式在PG前正确拒绝；沿用的测量探针误要求至少1条业务SQL而报measurement_no_statements。Detection定位原ready时误少一层business目录，尚未进入其admit。
2. 仅补跨模式/尾边界及Detection，**2 passed、1 failed、2 deselected，6.05秒**。两模块全部有限拒绝通过；Detection脚本误找独立命名的依赖文件，而首轮实际已从缓存复用Canonical同一组依赖。改为读取该原保存对象，严格核其完整选择，不新准入。
3. 仅Detection补验，**1 failed、4 deselected，63.76秒**。inspect/admit/current及records完整读取和Receipt均已通过，比较旧普通JSON时把其时间字符串当成真实str类型而失败。按原schema恢复datetime，并用先前typed inventory交叉核验，不修改产品或原证据。
4. 仅补Detection未完成项，**1 passed、4 deselected，14.52秒**。复用上一轮Admission和已保存records完整读取，补完另外两表、qualified和有限拒绝；没有再次admit、重读已完成records对照或重跑其他模块。

前置拒绝的实际PG数可以为0；修正后的探针仍由首尾marker证明日志连接绑定，只去掉错误的“至少1条业务SQL”要求，未用空文件猜0。所有测量和定位修改仅在本任务测试。最终必要正向及负向证据已闭合，未发现新的产品阻断。

## 新登记与原件保持

| owner/模式 | 新Admission ID |
| --- | --- |
| Feature fixture | `791dd54273295a1f551c43c3f57da5e6f1a3b785a86203af3ac7297427f82e1c` |
| Feature real | `03eb86f081bf5b07c6e4ef714824b4883b2a50208dc3660f372439ba6c259d75` |
| Canonical fixture | `34ce6a5fe13082af9fb3221ee5724a5552f738b586fbbe831b28d0a2d7f9887e` |
| Canonical real | `7f12a9bdb63dab13e013deff1404f114f6d5cf3f97831c7ac77003d5b753db99` |
| Detection fixture | `828e71dc66889036dac6ec8c7beae08b223a40127dae530103b38bf3c23bb22d` |

各原科学登记全值不变；原可信记录按完整typed多重集合保留。Feature库M2/reference/Feature Admission各1→3；3884库M23→5、reference3→25、Canonical0→2；Detection0→1。增加的仅为必要新准入，两模式和11参考的数量区别保留；所有对象全文在证据目录，不覆签旧记录。

全部历史保护清单逐项相同：Peer原件690、C5S2索引655、Peer索引48、M2P1索引151、H2索引331、DetectionLake索引93、Runtime索引116、FeatureP1索引69、H5索引483、2c8a权威3、Resource STOP38、路径恢复128、关闭P2索引44。清单有重叠，不相加为唯一原件数；本片不重启28763或重跑H系列。前次Resource3166行的文字更正不回写旧报告。

## 成本与收尾

实际PG条目来自自有55483本片新日志的statement/execute，marker核绑定且剔除探针连接；包括owner和内部DuckDB/M2/reference调用，不是Python hook数，也不是只计输出库。调用顺序串行，无其他业务并发。若跨模式在访问PG前拒绝，实测0条。每阶段原日志和事件完整保存。

| 阶段 | 墙钟秒 | 实际PG条目 |
| --- | ---: | ---: |
| Feature fixture admit | 1.555685 | 772 |
| Feature real admit | 2.140831 | 772 |
| Feature real两视图合计 | 1.726846 | 722 |
| Canonical fixture admit | 6.322002 | 4378 |
| Canonical real admit | 6.137800 | 4378 |
| Canonical real13表读取合计 | 111.956178 | 52181 |
| Detection admit | 6.020407 | 2956 |
| Detection current | 2.547867 | 1451 |
| Detection records | 5.359145 | 2927 |
| Detection state_entries | 3.945257 | 2927 |
| Detection m3_entries | 3.752058 | 2927 |
| Detection qualified | 3.747893 | 2933 |

上游inspect/admit/reference准备单独保存，未混入表中owner准入。Canonical每个视图仍多次检查全部11份参考，导致此次小样本读取开销明显，不能以无全量Replay误称低成本。Python生命周期峰值RSS最高397803520字节，不含PG、不是各阶段独占峰值；PG峰值、磁盘物理读取、阶段独占峰值和未测临时盘峰值Unknown，不拿typed量替代物理I/O。

自有55483已smart stop，55483与28763均status3、isready2，无PID/socket，各scratch子目录为空。证据根 `/tmp/domeye-public-runtime-integration-8233`，完整索引 `交付索引.json`；报告和索引SHA随固定提交回传。完整差异、owner源码保持及diff --check已核。

标准模式，无Fast、无总处理时限，有限资源保护保留。没有真实全天/共享服务、停止试点、D/P/H/726、HTTP、Issue、push或部署。仅本公共Runtime接合GO，完成后停止等待下一授权。
