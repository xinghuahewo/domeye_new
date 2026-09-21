# Detection 接入同源人工联合链

2026-09-13。**人工合流 GO**：实际保存的同组观察与参考可供 Resource、普通/IR Feature、Detection 正式计算并固定读回。Detection取得非空修订、判定及状态，旧结果保全通过。真实D/P、全部真实数据、全链发布及前端未验。

## 固定提交与合并依据

原接受基线 `90e7dfe3c0229d62dc239043e01b6cf4af1910d8` 保留，见[Feature多视图人工报告](迁移人工集成增量-Feature-f1cf.md)。本次merge `84c5fdd2a8f59028136bb8e61d3908c59b94b332` 双亲为：

- `90e7dfe3c0229d62dc239043e01b6cf4af1910d8`。
- Detection最终冻结 `c133b7948f2296adc30b8b16625306e7c9b05750`。

独立接受报告为 `4e4e7543d57da1f344a7b9ce0df0b176718460c7` 末节：Excel类型P2已关闭、解释规则v2、两轴GO。原六类覆盖、角色/分类和Excel类型全组验证仍引用该独立报告，不以历史114项充当本次通过数。

两处冲突：`test_observation_two_phase.py`的共享catalog测试与阶段1设计说明末节。逐文件双方差异证明c133只缺已有1d增量，没有相反的新测试/设计，因此完整保留90e的catalog测试与说明。共享store自动合并后与90e无差异。没有按时间戳选择版本，也没有丢弃Detection内容。

合并相对90e新增Detection运行目录、测试、说明及正式入口共31文件、9575行。Detection目录及入口与c133逐字节一致；observations、frozen、Resource及Feature运行源码与90e逐字节一致。合并后仅最小扩展现有人工联合测试及本文/台账，没有业务算法、阈值、公共合同、HTTP或页面改动。

任务 `01a0971e-47ea-7441-94dd-a9f4b4eb5dfc`，8233，`codex/migration-integration`，标准模式。没有真实输入、远端、push、Issue或共享服务操作；未合国家增强或Q1。

## 同源输入与计算差异

两个Stage1 complete人工run共用显式catalog，P含9份RIB及P UPDATE，D含3份RIB、P initial的同源缓存别名、D UPDATE和合法空尾；第二produce使用正式CLI。原12份RIB每份2个/24、路径`9808 100`保持。

共享CSV增加实际组织、类型及关系字段，三类计算使用同一D固定run/snapshot上的SHA `41c973026a624a376111b1dae6dcb9f1ddb63cf56fade3732a7bd95945683050`。Feature/Resource所用原列仍在；Detection所需关系集合按明确字符串保存，解释时恢复类型。另十类参考也通过同一D Stage1真实保存，不由Detection凭空注入映射。

| Detection参考角色 | 格式 | 保存行/对象数 |
| --- | --- | --- |
| as_info（共同CSV） | CSV | 6 |
| important_as_dict | CSV | 1 |
| prefix_info | CSV | 2 |
| triplet_info | CSV | 1 |
| country | XLSX | 2 |
| important_prefix_v4 | XLSX | 1 |
| important_prefix_v6 | XLSX | 1 |
| as_prefix_dict | JSON | 1 |
| as_rel_dict | JSON | 4 |
| important_domain_dict | JSON | 1 |
| private_as_dict | JSON | 1 |

合计11类、21个保存行/对象，CSV/XLSX计数包含表头；只有表头的人工表明确为空集合，不冒充真实资料覆盖。人工生成helper来自冻结测试，全部字节在本任务生成；共同CSV单独合并实际列并绑定相同SHA。每种原件的完整字段、词法/单元格类型和来源定位经公共reference-rows/v2保存，Detection解释规则为`detection-reference-39578fe/v2`；原历史适用性Unknown不改。Resource补充国家JSON仍另行正式登记为原件文件、湖历史及PG目录，保持其NaN/null/legacy_unknown解释，未与Detection的country XLSX混作同一版本。

计算范围严格区分：

- Resource仍只读取原12份RIB，default返回D的3份、all返回12份，不消费UPDATE、额外别名或Feature状态。
- Feature仍从P snapshot initial_rib跨P→D连续计算，P comparison与D result分开；D RIB不重置它。别名只消费一次，不同URI同SHA空源分别保留，IR禁写与seen保持。
- Detection只绑定D第一份baseline及D UPDATE/空源，明确冷启动，窗口`[2026-02-27T16:00Z,16:10Z)`；不从P继承状态、不消费D其他RIB或P别名。它看到的初态仍为100，不能与Feature的P更新后初态101混称一致。

为使联合结果非空，在原D撤回之外增加1条`10.0.1.0/24`、Peer64498、路径`9808 101`宣告；D baseline对此前缀已有起源100。未改规则或阈值，触发实际MOAS及旧规则hijack修订。此为人工控制面检测输出，不是已证实攻击/责任。

## 独立核对与旧结果保全

正式链顺序：Stage1 P/D → Resource补充JSON登记 → Resource第一run → Feature双模式 → Detection → Resource后续run。每个计算均实际进入冻结新解释器，读取固定complete观察。

- Detection共26条共同记录：6条rule_decision、2条compatibility_transition、2条business_revision（hijack/moas各1），以及来源/文件等记录；97个typed状态项。修订主语明确含`10.0.1.0/24`，不是空结果通过。
- 全部Detection records与state_entries逐字段对比typed PG和固定Parquet，JSON字段按完整值对照；重建状态明确保存参考解释v2。ready绑定全部11个来源/SHA/行数、D run/snapshot、所选三来源及实际冻结源码SHA。
- Feature普通D窗口A/W为1/1，IR为0/1；普通撤回仍归101，IR归100且禁写；D空尾无ASN行。普通seen增加此次宣告的64498，IR仍保留64497和64500。
- Resource仍default3/all12来源、decision6/24，global为2前缀/512地址量/1路径，参考Peer首行及未知拓扑符合原人工预期。
- 后续追加后，两份Stage1全部15表、第一Resource default/all全部10表、Feature全部7表逻辑多重集合不变；保留重复、类型和嵌套顺序，仅忽略无承诺的外层行序。Resource新run引用先核自身归属再对照。
- 已有观察、参考、Resource、Feature、Detection Parquet和各execution/ready原字节不变。补充参考全行及原件字节不变。另只读审计所有组件PG登记snapshot/state及D的11项参考validated/行数仍与原回执一致。

Detection c133使用独立数据库/catalog，DATA_PATH固定在其本run `business/parquet`。本片没有添加共享catalog多run能力；在其catalog创建无关人工表并写入值17，snapshot从5增加到7，随后旧snapshot5完整records与重建状态不变，使用7冒充该run固定snapshot被拒绝。这是追加后的快照隔离证据，不是第二Detection run复用能力。

## 执行记录与文件归属

本任务新建UTF8 PostgreSQL14，仅Unix socket `/tmp/domeye-integration-detection-8233/socket`、端口55483，无TCP；Stage1/Resource/Feature在一个人工数据库，Detection使用同cluster另一人工数据库。所有配置/输入/输出在Git外；PG已停止。

最终 **2 passed，0 skipped，20.79秒**：原单run回归与扩展三计算联合。首次同片2 passed、20.68秒后，为落实完整上游保全将elements检查扩到全部15表，再执行最终两项；不累加为4项。没有重跑114全集。另`final-audit.py`只读核对组件PG登记、11类参考的21行/对象计数、Detection真实DATA_PATH和错snapshot拒绝。

```bash
DOMEYE_FEATURE_TEST_DSN='host=/tmp/domeye-integration-detection-8233/socket port=55483 dbname=postgres' \
uv run --locked --project backend pytest -q backend/web/tests/test_migration_integration.py \
  --basetemp=/tmp/domeye-integration-detection-8233/final
```

证据根 `/tmp/domeye-integration-detection-8233`，最终日志`final.log`；`final/test_resource_feature_share_ob0/three-module-evidence.json`包含全部计算回执，`detection-audit.json`包含精确类别、计数、参考SHA、目录；各正式CLI请求/stdout/stderr及原件同目录。完整代码身份在各ready/回执，所有实际运行依赖来自新项目锁定环境。完整本片差异及`git diff --check`已检查。

| 制品 | 固定ID | snapshot/实际文件归属 |
| --- | --- | --- |
| P观察 | `5628c552f4f24f148d890f26795179c1` | 19；shared-catalog/r_run |
| D观察及11参考 | `3a5261bb6f7f4b84aef529077639b437` | 38；shared-catalog/r_run |
| Resource补充JSON | `5cef7a1a177b4c45944658057ab7a6cd` | 41；shared-catalog/reference_ID，原件在supplement-output/original |
| 第一Resource | `5955bbd77848436d96e4b7453cd5294f` | 53；shared-catalog/resource_run |
| Feature | `1db46d3916164cb188ba36786e96b7ab` | 75；私有feature/parquet与fl_run目录 |
| Detection | `68f643467af048da8395ffa6db7a5aed` | 5；另一数据库，detection/business/parquet |
| 后续Resource | `605268fe331b420cb6203759123fa20a` | 65；shared-catalog/resource_run |

跨catalog的snapshot数字不能互相排序。临时制品不是业务publication，也没有把各模块不同初态/窗口合成同publication。

## 保留边界

国家增强与Q1未合；Detection冻结内既有country_v2兼容逻辑随原作者目录保留，不表示新国家增强接入。真实11参考、真实P/D、全日吞吐、全部六类实数覆盖、跨日恢复、公开查询及页面/发布均未验。[原完整迁移计划](../architecture/新架构一日全链路迁移计划.md)保持全目标，本片只交付人工同源合流基线。
