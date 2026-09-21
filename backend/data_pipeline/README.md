# 数据流水线：从哪里开始看

先看 **[bgp/pipeline.py](bgp/pipeline.py)**：这是原生有序批次直接进入共享路由状态、普通 Feature 和 Detection 的主入口。它对应 `scripts/observations.py route-pipeline`；参数和运行边界见[运行手册](../../docs/runbooks/运行与维护.md)。目录中保存的是程序，原始数据、数据库和运行输出在 Git 外。

## 七个目录

| 目录 | 中文含义 | 到这里找什么 |
| --- | --- | --- |
| `bgp/` | BGP 输入与路由状态 | MRT 解析、归档、有序批次、共享状态、恢复与离线回放 |
| `analysis/` | 业务计算 | 特征统计、异常检测、资源统计、国家事件与趋势 |
| `results/` | 结果校验、发布与读取 | 组合结果清单、来源绑定、发布选择和读取；不是输出文件存放目录 |
| `history/` | 历史导入 | 历史数据库保留、事件集合、查询索引、RIB 索引、国家事件导入 |
| `jobs/` | 离线任务编排 | 输入计划、迁移步骤、下游计算、候选重新核验 |
| `overview/` | 首页数据准备 | 已留存异常索引、诊断、规模和路径摘要读取 |
| `common/` | 公共工具 | 文件核验、前缀解释、执行版本冻结、资源计量和准入锁 |

## RIB／UPDATE 主线

```text
原始 MRT
  → bgp/input/              解析为有序批次
  → bgp/pipeline.py        一个顺序计算入口
      ├─ bgp/archive/      保存原始观察
      └─ bgp/state/        更新共享状态并执行 analysis/ 的业务计算
  → 文件完整核验
  → bgp/state/checkpoint.py 保存状态、结果引用和恢复位置
```

| 要修改的事情 | 先看的文件 |
| --- | --- |
| 整体处理顺序、归档与计算衔接 | [bgp/pipeline.py](bgp/pipeline.py) |
| 原生 libbgpdump 调用及批次输入 | [bgp/input/native_parser.py](bgp/input/native_parser.py)、[native_ingest.py](bgp/input/native_ingest.py)、`bgp/input/native/` |
| RIB 批量建立状态 | [bgp/state/rib_columns.py](bgp/state/rib_columns.py) |
| UPDATE 批次与逐条业务字段 | [bgp/state/update_columns.py](bgp/state/update_columns.py)、[update_fields.py](bgp/state/update_fields.py) |
| `prefix_dict` 等共享路由状态 | [bgp/state/routes.py](bgp/state/routes.py) |
| 普通 Feature、Detection 顺序调用 | [bgp/state/business.py](bgp/state/business.py) |
| 状态增量保存、文件级恢复 | [bgp/state/checkpoint.py](bgp/state/checkpoint.py)、[dirty_keys.py](bgp/state/dirty_keys.py) |
| 已归档观察读取、核验及准入 | `bgp/archive/` |
| 已归档数据的离线重放与独立规范投影 | `bgp/replay/`；不是当前直接批次主入口 |
| 独立 RIB 快照留存、登记、路径比较 | `bgp/snapshots/`；不轮流覆盖 UPDATE 当前状态 |

业务配置可用 `feature_modes: ["ir"]` 与 `detection_enabled: false` 单独运行伊朗特征；原始输入与规范状态仍完整处理，检测回执为未运行。

归档与计算仍使用原有文件级确认规则。移动目录没有降低校验要求，也不表示真实 RIB＋8 小时 UPDATE 已通过一小时验收。

## 业务计算怎么找

- `analysis/features/`：Feature 计算、覆盖量、兼容投影与窗口结果。
- `analysis/detection/`：异常算法、检测入口、活动事件及结果读取。
- `analysis/resources/`：各份 RIB 的独立资源统计。
- `analysis/country_events/`：国家事件的固定观察集合、聚合、来源和结果资格。
- `analysis/country_trends/`：国家趋势计算、背景信息、快照与流式结果。

## 文件名怎么理解

文件名说明职责，不使用 `m3_`、`s2_`、`s3_`、`q1` 等开发阶段编号。例如：

| 现在的名字 | 做什么 |
| --- | --- |
| `country_events/event_aggregation.py` | 按事件归约已保存状态 |
| `country_events/route_history.py` | 按原处理位置读取路由历史 |
| `country_events/qualified_reader.py` | 读取带可用性核验的固定结果 |
| `country_trends/snapshot_store.py` | 保存既有独立趋势快照 |
| `country_trends/stream_store.py` | 生产通过流式输入建立的趋势候选 |
| `country_trends/result_admission.py` | 核验趋势候选是否满足读取准入条件 |
| `country_trends/part_manifest.py` | 核对物理分片清单 |

上表路径相对于 `analysis/`。`snapshot_*` 和 `stream_*` 表示仍有调用者的不同处理路径；`qualified_*` 表示独立的结果可用性核验。内部类名、数据库表名、数据合同版本和历史摘要保留原值，不能把源码改名当成数据迁移。历史评审中的旧路径只对应当时版本。

后续文件按实际职责加入现有目录。业务逻辑不进入 `scripts/`，不再新建阶段编号目录；确需保留两种处理方式时，用输入或职责区分，并写明入口。
