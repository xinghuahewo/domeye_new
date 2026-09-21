# Country C2 固定修复人工联合增量

2026-09-13。**人工集成 GO，限C2人工计算范围。** 原同源P/D联合链追加C2完整消费，结果complete_empty；另建正式人工两国链确认非空410条C2Row＋完成包络。两条链输入不同，非空国家结果不属于主联合的Resource／Feature输入，更未业务发布。

## 固定父链与完整差异

已接受基线 `e7a818e93f3b6d40449685fe07da38a145cbcdd0`，见[Q1.1联合记录](迁移人工集成增量-Q11-1f352c9.md)。无冲突merge `3edcd8c8532602bed1e56106efec28b92d6f2ed8` 的精确双亲：

- `e7a818e93f3b6d40449685fe07da38a145cbcdd0`。
- `83837b0e05a03568685f018ffe38de623cc2b914`。

838父 `7c849bdda9ae604b96207ee2041682e2fda27bd0`，7c父 `481cd7ffb8a75c54c33d6cb7c0031fb812a62ed0`。已读原报告 `d45edf7357a5191288f134cdcbe7759af0e25adb`、修复独立报告 `9b64240146a45bf27ac971e69164ca66da6d279c`及作者中文修复说明；原两P2关闭，两轴GO。作者148／51项及独立27项不计本次执行。

合并10文件1429增／23删。运行差异仅country_enhancement的c2.py、c2_stage.py、compute.py、incremental_types.py及Detection reference_view.py；这些文件与838一致。C1、Detection.store公共typed、shared producer／Replay／Resource／Feature及scripts与e7字节不变。参考解释selected_sink接缝保留，未由旧依赖覆盖。未合Q2／Q3／C3。

新增运行代码之外，合入2项测试文件与3份中文说明。作者说明的待复核时点原样保留；本次接受范围以独立报告及本文为准。本任务另外只增强原联合测试、新增非空链测试并记录中文报告／台账。

## 实际检查

自有UTF8 PG仅Unix socket `/tmp/domeye-integration-detection-8233/socket` 端口55483，无TCP。所有数据库、原件及暂存都由本任务新建；未连接作者或独立审阅者数据库。非空MRT生成代码复用已接受独立手写方案，正式build_pipeline接缝使用合并后本项目实现，不导入外部旧代码。

```bash
DOMEYE_FEATURE_TEST_DSN='host=/tmp/domeye-integration-detection-8233/socket port=55483 dbname=postgres' \
uv run --locked --project backend pytest -q backend/web/tests/test_migration_integration.py \
  backend/web/tests/test_migration_c2_integration.py -k 'resource_feature or formal_two_country' \
  --basetemp=/tmp/domeye-integration-c2-8233
```

首轮 **2 passed、0 skipped、1 deselected，33.38秒**。随后只增强非空测试为批1／2完整repr多重集合比较及暂存清理、选中8行断言，执行同一锁定环境 `pytest -q backend/web/tests/test_migration_c2_integration.py --basetemp=/tmp/domeye-integration-c2-nonempty-final-8233`：**1 passed、0 skipped，3.97秒**。最终为**2项不同检查通过**，不把复跑累计为3项；未运行批256或无关矩阵，无处理总时限。

## 主联合P/D：空事件但完整消费

- 原Resource两run、Feature双模式八表／4诊断、Detection新公共typed两表和旧Reader／重建、C1全部原断言保留。Detection仍26 records／6判定／97 state_entries，typed原全部列及JSON文本直接对湖／PG，尾部漂移拒绝。
- C2用同一个CountrySavedInput固定D／Detection／11参考，完成input_kind=saved_C1，原InputCompletion逐字段一致、complete_empty、event_count=0。4changes→4units，6条C2Row＋完成，无EventStatus。
- 追加只读审计逐条对4个ObservationFact核对SavedDelta的object、after presence／path／origin、时间／cursor与fact_after_presence；没有从原action重新推测状态。参考解释v2、原as_info6行、选中4行，与本次原绑定一致；原全部11参考21行仍由C1核验。
- Q1仍只ResourceFeature module_scoped fixture，generation2和第一token全部7页旧响应不变；alias资格漂移拒绝／旧head保全保留。P/D15表、Resource两run10表、首Feature8表、Detection两typed历史与重建、旧文件字节在C2／Q1后不变。
- 主链C2暂存32行、SQLite写43、最终盘201959字节、约0.639秒；输出与完整成本保留。不是国家事件零值发布。

## 独立附加两国链：实际非空

手写12个RIB元素：4个/24，每个3方向，origin1/2为ZZ、3/4为YY；101—112秒逐方向撤回。随后含announce、两个STATE、LOCAL、ET 123456微秒／EOR、时间回退及空源。经正式producer→fresh Detection→C1→C2，无Resource／Feature结果参与此链。

| 手算与实际 | 结果 |
| --- | --- |
| 独立pre-onset | ZZ onset106严格之前仅172.16.1.0/24的Peer102；YY onset112严格之前仅172.16.3.0/24的Peer102；两者fixed_prefix_count／direction_count均1／1 |
| 采样 | ZZ在107秒、YY在113秒相应PrefixPoint.state为complete，未把两个事件共用起点 |
| 全量行 | 原25changes／9invalidations合成27units（25变化＋2STATE）；完整410 C2Row＋1 C2Completion，output_rows=410不含包络 |
| 完整比较 | 批1／2全部C2Row的repr多重集合相等，保留类型、嵌套顺序、重复、Fraction表示及新参考字段；完成包络含运行成本，不把成本时间纳入业务行比较 |
| 参考locator | 固定解释选中8行，ZZ origin2和YY origin4的EventStatus locator分别指向原第2／4行；固定prefix／方向正确。没有重跑重复末行矩阵，重复首行语义沿用独立修复证据 |
| 原事实与清理 | LOCAL原事实恰1条；正常完整消费后C1／C2暂存目录均清理 |

成本：两批均state_assignments34、activation_member_rows6、related_changes_sent7、reference locator查询2、预留／释放／峰值成员各7、staged_rows136、staged_bytes181712、SQLite写210、最终盘468434字节；C1峰值暂存41512字节。含C1构造墙钟约0.831／0.802秒；process RSS高水位339345408／353337344字节，为整个测试进程累计峰值，不是C2独占或真实容量。VM steps为采样下界。参考仍有全列DataFrame解释瞬时成本，不称常量内存。

## 身份、证据与停机

主链D `93fe8716ec094a898e88aa72dd1b1a6c`／snapshot38，Detection `db32b7a75d394dd284ce6425f65a4e6c`／snapshot5。Q1第一token `q1_3a38d90bdc580aa49b511b79bad2eb26f5ec9052a6fe0e976ac7e059662a62d9`；当前第二token和全部固定分页保留于q1-evidence.json。

非空链观察 `f1c5c4c38b024028890cc90d71d0b178`／snapshot60，Detection `5bb38c1153a04a82b2b39da2ed472389`／snapshot5，与主链明确不同。

主证据根 `/tmp/domeye-integration-c2-8233/`及相邻.log，子目录test_resource_feature_share_ob0下有c2-empty-rows.txt（完整repr）、c2-empty-evidence.json、c2-fact-audit.json、原typed／C1／Q1证据。非空最终根 `/tmp/domeye-integration-c2-nonempty-final-8233/`及相邻.log，test_formal_two_country_c2_int0下含原MRT／11参考、manifest、request、binding、fresh Detection制品、c2-full-rows.txt（完整repr含locator）、independent-evidence.json（沿用方案文件名，为本任务本次执行）。

附加 `/tmp/domeye-integration-c2-audit-8233.py`及.log只读复核组件资格、原回执SHA、Q1第二head／第一token全部页，以及C2原事实和解释。私有PG停止回执 `/tmp/domeye-integration-c2-pg-stop-8233.log` 明确server stopped。Q1按合同封存本任务文件权限，只承诺原正文SHA／字节保全，不声称ctime或权限未变。

完整差异及git diff --check通过。无业务Country发布/head、HTTP／前端、真实D/P/H、远端、生产、Issue或push；既有Q1人工head仅属于ResourceFeature。真实D214MRT任务仍独立，不因本片合并更改。当前人工候选冻结，等待下一片。
