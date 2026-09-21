# Detection 真实候选 Runtime 有限集成

结论：**GO，仅原人工 Gap 制品上的 real-candidate 公共 Runtime 接合**。完整 inspect→admit→current/reuse→三表公开读取实际通过，341行完整 typed 与原独立 inventory 一致。没有重产科学数据，不代表真实数据、窗口或全P验收。

## 固定组合和范围

接受基线 `98d8d50c9b791b27e4351ad01093ebbb2405bba5` 完整合入 `2e1be72656781a42b09b19b42bbba907ca6116f0`（00e7fe..2e1三提交）。实际merge/执行HEAD为 **bd16cacdf09746a8632813aa860d5b4eec906ea2**，双亲为98d8、2e1，无冲突。五文件257增行/23删行，涉及Detection Runtime、IO、两个测试及接口文档；本任务仅追加集成测试与报告，没有代修产品。四个Detection产品文件逐字节等于2e1固定候选，完整差异已审查，diff --check通过。

独立报告 `/Users/botongwu/.codex/outputs/detection-real-independent-2e1be72/独立Runtime复核报告.md` 全文读取并实算SHA256为 `d05e0040609d34c9888efc9188a9b0e779f32d524d306c716b1dea22f79b49af`。它的测试和29项拒绝不算本片实测。本片未混入3092/e723窗口或Resource在途修复。

仅使用自有55483：原Gap `0774d9ed031d4fb7b25b6ad8a8faff06:7`，原M2 `3884fdb6cdb7408d92f78a85db70a6af:26` 与原11份参考。原binding、scope、identity和科学执行模式不改写。沿用规范化Unix socket拼写，同一物理PG与库身份不变。

## 实际接合结果

复用98d8片已取得的Canonical fixture/real两组M2及reference完整Admission，逐run/snapshot、来源顺序、参考source_id核对，并实际全部current通过。没有调用上游admit，没有新造上游登记。

Detection real Runtime显式传入原完整binding、DSN、允许根、原输出根、隔离scratch，以及256MiB内存、512MiB临时盘、2GiB进程RSS、128MiB最低空闲盘和2000ms锁等待预算。实际inspect完全等于原绑定；一次必要完整admit后current与再次admit返回同一Admission，current/reuse事件均无full_table_scan、body_query、entity_hash或admit_body_batch。

新Detection Admission为 `9d355200df494eb2ff3b89b3d75ceec20b31d6d54543f5cbb3b41ee6d59b9492`，owner_revision为完整执行HEAD `bd16cacdf09746a8632813aa860d5b4eec906ea2`。原accepted记录保留，Detection登记仅1→2；Feature、M2、reference、Canonical及全部原科学登记逐对象不变。

三张表均使用公开open_reader：records131、state_entries178、m3_entries32，共341行。逐完整typed文本等于98d8固定原值；再与原Gap独立fresh-reader inventory比较rows、typed_bytes及SHA，三表全部相同。没有将时间字符串误作真实类型、排序或改写科学值。每个正常读取在迭代耗尽时Receipt仍为空，关闭和尾current成功后为complete，行数对应。

fixture兼容保留98d8已通过的完整证据，仅做必要current确认：旧fixture Admission实际被当前验证器身份变化拒绝。2e1引入模式相关规则及代码SHA变化，旧记录没有被覆签为当前有效。fixture上游current仍通过；未追加fixture Detection准入或重跑双模式全链，因此本片不声称新代码下fixture成功读取已再次实测。

有限负向仅两项：fixture Runtime消费本轮real Admission的实际current因验证器模式身份不符拒绝；real完整m3_entries读取后收紧RSS预算至1字节，尾部拒绝且Receipt=None，预算随后恢复。没有重跑29项、锁、路径或科学坏件矩阵，也未以无效旧记录代替本轮跨模式拒绝。

## 实测与成本

执行 `DOMEYE_PUBLIC_RUNTIME_INTEGRATION=8233 DOMEYE_DETECTION_REAL_INTEGRATION=8233 env -u PYTHONPATH uv run --locked --project backend pytest -q backend/web/tests/test_detection_real_integration_8233.py`：**1 passed，36.09秒**，一次完成，无失败补验。测试复用本项目已有有限测量/typed读取辅助函数，未加载其他任务或旧项目代码。

| 阶段 | 墙钟秒 | 实际PG条目 |
| --- | ---: | ---: |
| fixture上游全部current | 2.017685 | 1382 |
| real上游全部current | 1.759633 | 1382 |
| fixture旧Admission current拒绝 | 0.014609 | 6 |
| real inspect | 0.004453 | 3 |
| real admit | 4.408785 | 2956 |
| real current及reuse合计 | 7.199044 | 4354 |
| real records完整读取 | 5.386170 | 2927 |
| real state_entries完整读取 | 5.526451 | 2927 |
| real m3_entries完整读取 | 5.355718 | 2927 |
| 实际跨模式current拒绝 | 0.010773 | 6 |
| real尾资源拒绝 | 2.675065 | 1476 |

PG条目是本次独立日志中实际statement/execute，首尾marker证明绑定并排除测量连接，包含内部M2/reference访问。自有PG串行运行，没有共享业务混入；不是Python hook计数。每阶段完整日志、事件、成本及首批时间均保存。

正向资源记录1152次检查，Python进程生命周期累计RSS峰值323616768字节（不含PG），最低空闲盘117667278848字节；该记录在负向探针前保存。阶段独占内存、PG峰值、物理设备IO、DuckLake原生扫描总量及临时盘峰值Unknown。依赖current仍有元数据开销，不把无正文扫描描述成零成本。

## 原件和收尾

原保护清单及98d8片249文件索引全部逐项SHA相同，清单有重叠，不合计为唯一文件数。包括Peer原件690、C5S2 655、Peer48、M2P1 151、H2 331、DetectionLake93、Runtime116、FeatureP1 69、H5 483、2c8a权威3、Resource STOP38/路径恢复128/关闭44。原科学及旧可信登记全文保持，scratch各子目录为空。

自有55483已smart stop，28763未启动；两者pg_ctl status=3、pg_isready=2，无PID/socket。证据位于 `/tmp/domeye-detection-real-integration-8233`，完整 `交付索引.json` 的SHA和文件数随固定提交回传。

标准模式，无Fast、无总处理时限，资源保护保留。没有真实数据检查或生产、共享服务、HTTP、Issue、push或部署。完成后停止，等待窗口及Resource独立接受后的下一片授权。
